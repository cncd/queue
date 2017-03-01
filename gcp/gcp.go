package gcp

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/cncd/queue"
	"github.com/cncd/queue/gcp/internal"
)

type subscription struct {
	done  chan bool
	error error
}

type conn struct {
	sync.Mutex

	// base   queue.Queue
	opts   *Options
	client *internal.Client
	subs   map[string]*subscription
}

// New returns a task queue backed by Google Cloud pubusb.
func New(opts ...Option) (queue.Queue, error) {
	conn := new(conn)
	conn.subs = map[string]*subscription{}

	// conn.base = queue.New()
	conn.opts = new(Options)
	conn.opts.topic = "queue"
	conn.opts.subscription = "default"
	for _, opt := range opts {
		opt(conn.opts)
	}

	jsonToken, err := ioutil.ReadFile(conn.opts.tokenpath)
	if err != nil {
		return nil, err
	}
	jwt, err := google.JWTConfigFromJSON(jsonToken, "https://www.googleapis.com/auth/pubsub")
	if err != nil {
		return nil, err
	}
	src := jwt.TokenSource(oauth2.NoContext)
	cli := oauth2.NewClient(oauth2.NoContext, src)
	conn.client = internal.NewClient(cli)

	go conn.pollWait()
	return conn, nil
}

// Push pushes an task to the tail of this queue.
func (c *conn) Push(ctx context.Context, task *queue.Task) error {
	message := internal.Message{
		MessageID:  task.ID,
		Attributes: task.Labels,
		Data:       task.Data,
	}
	_, err := c.client.Publish(ctx, c.opts.project, c.opts.topic, message)
	if err != nil {
		return err
	}
	return nil
}

// Poll retrieves and removes a task head of this queue.
func (c *conn) Poll(ctx context.Context, f queue.Filter) (*queue.Task, error) {
	message, err := c.poll(ctx)
	if err != nil {
		return nil, err
	}
	task := new(queue.Task)
	task.ID = message.MessageID
	task.Data = message.Data
	task.Labels = message.Attributes
	// TODO store AckID somewhere for the modifyAckDeadline procedure
	return task, nil
}

// Extend extends the deadline for a task.
func (c *conn) Extend(ctx context.Context, id string) error {
	return c.client.Extend(ctx, c.opts.project, c.opts.topic, id, 600)
}

// Done signals the task is complete.
func (c *conn) Done(ctx context.Context, id string) error {
	return c.Error(ctx, id, nil)
}

// Error signals the task is complete with errors.
func (c *conn) Error(ctx context.Context, id string, err error) error {
	var data []byte
	labels := map[string]string{"error": "false"}
	if err != nil {
		labels["error"] = "true"
		data = []byte(err.Error())
	}
	message := internal.Message{
		MessageID:  id,
		Attributes: labels,
		Data:       data,
	}
	_, err = c.client.Publish(ctx, c.opts.project, c.opts.topicDone, message)
	if err != nil {
		return err
	}
	c.Lock()
	s, ok := c.subs[id]
	if !ok {
		s = &subscription{
			done: make(chan bool),
		}
		c.subs[id] = s
	}
	c.Unlock()
	return nil
}

// Wait waits until the task is complete.
func (c *conn) Wait(ctx context.Context, id string) error {
	c.Lock()
	s, ok := c.subs[id]
	c.Unlock()
	if !ok {
		return queue.ErrNotFound
	}
	select {
	case <-ctx.Done():
	case <-s.done:
	}
	c.Lock()
	delete(c.subs, id)
	c.Unlock()
	return s.error
}

// Info returns internal queue information.
func (c *conn) Info(ctx context.Context) queue.InfoT {
	// TODO this will be different for gcp
	return queue.InfoT{}
}

func (c *conn) poll(ctx context.Context) (internal.Message, error) {
	log.Println("queue: pull: polling for messages")

	var message internal.Message
	messages, err := c.client.Pull(
		context.Background(),
		c.opts.project,
		c.opts.subscription,
		1,
	)
	if err != nil {
		return message, err
	}
	if len(messages) == 0 {
		return message, nil
	}
	message = messages[0]

	err = c.client.Ack(
		context.Background(),
		c.opts.project,
		c.opts.subscription,
		message.AckID,
	)
	if err != nil {
		log.Printf("queue: ack: error: %s", err)
	}
	return message, nil
}

func (c *conn) pollWait() {
	for {
		log.Println("queue: pull: polling for done messages")

		messages, err := c.client.Pull(
			context.Background(),
			c.opts.project,
			c.opts.subscriptionDone,
			100,
		)
		if err != nil {
			log.Printf("pubsub: ack: error: %s", err)
			continue
		}

		var ackIDs []string
		for _, message := range messages {
			ackIDs = append(ackIDs, message.AckID)
		}

		err = c.client.Ack(
			context.Background(),
			c.opts.project,
			c.opts.subscriptionDone,
			ackIDs...,
		)
		if err != nil {
			log.Printf("pubsub: ack: error: %s", err)
			continue
		}

		c.Lock()
		for _, message := range messages {
			sub, ok := c.subs[message.MessageID]
			if ok {
				if len(message.Data) != 0 {
					sub.error = errors.New(string(message.Data))
				}
				close(sub.done)
			}
		}
		c.Unlock()
	}
}
