package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"sync"
	"time"

	"github.com/cncd/queue"
)

const POP_TIMEOUT = 0 // 0 == Blocking forever

var ErrDeadLine = errors.New("queue: deadline received")

type entry struct {
	Item     *queue.Task `json:"item"`
	done     chan bool   `json:"done"`
	Retry    int         `json:"retry"`
	Error    error       `json:"error"`
	Deadline time.Time   `json:"deadline"`
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

	conn.opts = new(Options)
	conn.opts.hostIdentity = "host01"
	conn.opts.penddingQueueName = "pendding-queue"
	for _, opt := range opts {
		opt(conn.opts)
	}

	conn.client = redis.NewClient(&redis.Options{
		Addr:     conn.opts.addr,
		Password: conn.opts.password,
		DB:       conn.opts.db,
	})

	runningQueueKey := fmt.Sprintf("%s:running:queue", conn.opts.hostIdentity)

	allRunning, err := conn.client.HGetAll(runningQueueKey).Result()
	if err != nil {
		return nil, err
	}
	for _, taskRaw := range allRunning {
		runningTask := new(entry)
		err = json.Unmarshal([]byte(taskRaw), runningTask)
		if err != nil {
			return nil, err
		}
		runningTask.done = make(chan bool)
		conn.running[runningTask.Item.ID] = runningTask
	}
	return conn, nil
}

// Push pushes an task to the tail of this queue.
// 1. Task => Undone list(Redis)
func (c *conn) Push(ctx context.Context, task *queue.Task) error {
	taskRaw, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = c.client.LPush(c.opts.penddingQueueName, taskRaw).Err()
	if err != nil {
		return err
	}
	go c.tracking()
	return nil
}

// 2. Undone list(Redis) => Task
func (c *conn) Poll(ctx context.Context, f queue.Filter) (*queue.Task, error) {
	result, err := c.client.BRPop(POP_TIMEOUT, c.opts.penddingQueueName).Result()
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
		Item:     task,
		done:     make(chan bool),
		Deadline: time.Now().Add(c.extension),
	}

	taskRaw, err := json.Marshal(c.running[task.ID])
	if err != nil {
		return nil, err
	}
	runningQueueKey := fmt.Sprintf("%s:running:queue", c.opts.hostIdentity)
	runningTaskKey := fmt.Sprintf("running:task:%s", task.ID)
	err = c.client.HSet(runningQueueKey, runningTaskKey, taskRaw).Err()
	if err != nil {
		return nil, err
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
		task.Deadline = time.Now().Add(c.extension)
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
		task.Error = err
		close(task.done)
		delete(c.running, id)
		c.deleteTaskFromRunningQueue(id)
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
			return task.Error
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
		stats.Running = append(stats.Running, entry.Item)
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
		if time.Now().After(task.Deadline) {
			task.Error = ErrDeadLine
			close(task.done)
			delete(c.running, id)
			c.deleteTaskFromRunningQueue(id)
		}
	}
}

func (c *conn) deleteTaskFromRunningQueue(taskID string) {
	runningQueueKey := fmt.Sprintf("%s:running:queue", c.opts.hostIdentity)
	runningTaskKey := fmt.Sprintf("running:task:%s", taskID)
	err := c.client.HDel(runningQueueKey, runningTaskKey).Err()
	if err != nil {
		log.Printf("queue: delete %s key %s error: %v\n", runningQueueKey, runningTaskKey, err)
	}
}
