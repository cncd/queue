package redis

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis"
	"sync"
	"time"

	"github.com/cncd/queue"
)

const POP_TIMEOUT = 0 // 0 == Blocking forever

var ErrDeadLine = errors.New("queue: deadline received")

type entry struct {
	item     *queue.Task
	done     chan bool
	retry    int
	error    error
	deadline time.Time
}

type conn struct {
	sync.Mutex

	opts      *Options
	client    *redis.Client
	running   map[string]*entry
	extension time.Duration
}

func New(opts ...Option) (queue.Queue, error) {
	conn := new(conn)
	// init running
	conn.running = map[string]*entry{}
	conn.extension = time.Minute * 10
	// conn.extension = time.Second * 10

	conn.opts = new(Options)
	conn.opts.queueName = "task-queue"
	for _, opt := range opts {
		opt(conn.opts)
	}

	conn.client = redis.NewClient(&redis.Options{
		Addr:     conn.opts.addr,
		Password: conn.opts.password, // no password set
		DB:       conn.opts.db,       // use default DB
	})

	return conn, nil
}

// Push pushes an task to the tail of this queue.
// 1. Task => Undone list(Redis)
func (c *conn) Push(ctx context.Context, task *queue.Task) error {
	taskRaw, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = c.client.LPush(c.opts.queueName, taskRaw).Err()
	if err != nil {
		return err
	}
	go c.tracking()
	return nil
}

// 2. Undone list(Redis) => Task
func (c *conn) Poll(ctx context.Context, f queue.Filter) (*queue.Task, error) {
	result, err := c.client.BLPop(POP_TIMEOUT, c.opts.queueName).Result()
	if err != nil {
		return nil, err
	}
	taskRawData := result[1]

	task := new(queue.Task)
	err = json.Unmarshal([]byte(taskRawData), task)
	if err != nil {
		return nil, err
	}
	c.running[task.ID] = &entry{
		item:     task,
		done:     make(chan bool),
		deadline: time.Now().Add(c.extension),
	}

	go c.tracking()
	return task, nil
}

// Extend extends the deadline for a task.
func (c *conn) Extend(ctx context.Context, id string) error {
	c.Lock()
	defer c.Unlock()

	task, ok := c.running[id]
	if ok {
		task.deadline = time.Now().Add(c.extension)
		return nil
	}
	return queue.ErrNotFound
}

// Done signals the task is complete.
func (c *conn) Done(ctx context.Context, id string) error {
	return c.Error(ctx, id, nil)
}

// Error signals the task is complete with errors.
func (c *conn) Error(ctx context.Context, id string, err error) error {
	c.Lock()
	task, ok := c.running[id]
	if ok {
		task.error = err
		close(task.done)
		delete(c.running, id)
	}
	c.Unlock()
	return nil
}

// Evict removes a pending task from the queue.
func (c *conn) Evict(ctx context.Context, id string) error {
	return nil
}

// Wait waits until the task is complete.
// 3. Return error when task is done
func (c *conn) Wait(ctx context.Context, id string) error {
	c.Lock()
	task, ok := c.running[id]
	c.Unlock()
	if ok {
		select {
		case <-ctx.Done():
		case <-task.done:
			return task.error
		}
	}
	return nil
}

// Info returns internal queue information.
func (c *conn) Info(ctx context.Context) queue.InfoT {
	c.Lock()
	stats := queue.InfoT{}
	stats.Stats.Running = len(c.running)
	for _, entry := range c.running {
		stats.Running = append(stats.Running, entry.item)
	}
	c.Unlock()
	return stats
}

// every call this method will checking if task.deadline is arrived.
func (c *conn) tracking() {
	c.Lock()
	defer c.Unlock()

	// TODO(bradrydzewski) move this to a helper function
	// push items to the front of the queue if the item expires.
	for id, task := range c.running {
		if time.Now().After(task.deadline) {
			task.error = ErrDeadLine
			delete(c.running, id)
			close(task.done)
		}
	}
}
