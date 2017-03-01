package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const (
	endpointPush        = "https://pubsub.googleapis.com/v1/projects/%s/topics/%s:publish"
	endpointPull        = "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:pull"
	endpointAck         = "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:acknowledge"
	endpointAckDeadline = "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:modifyAckDeadline"
)

const (
	methodGet    = "GET"
	methodPut    = "PUT"
	methodPost   = "POST"
	methodDelete = "DELETE"
)

// Client is a Google Pub/Sub client.
type Client struct {
	client *http.Client
}

// NewClient creates a new PubSub client.
func NewClient(httpClient *http.Client) *Client {
	return &Client{
		client: httpClient,
	}
}

// Publish publishes the supplied Messages to the topic.
func (c *Client) Publish(ctx context.Context, project, topic string, messages ...Message) ([]string, error) {
	uri := fmt.Sprintf(endpointPush, project, topic)
	res := new(pushResponse)
	req := new(pushRequest)
	req.Messages = messages
	err := c.do(ctx, uri, methodPost, req, res)
	return res.MessageIds, err
}

// Pull pulls a maximum number of N Messages for the subscription.
func (c *Client) Pull(ctx context.Context, project, subscription string, max int) ([]Message, error) {
	uri := fmt.Sprintf(endpointPull, project, subscription)
	res := new(pullResponse)
	req := new(pullRequest)
	req.MaxMessages = max
	err := c.do(ctx, uri, methodPost, req, res)
	var messages []Message
	for _, message := range res.Messages {
		message.Message.AckID = message.AckID
		messages = append(messages, message.Message)
	}
	return messages, err
}

// Ack acknowledges message receipt.
func (c *Client) Ack(ctx context.Context, project, subscription string, ids ...string) error {
	uri := fmt.Sprintf(endpointAck, project, subscription)
	req := new(ackRequest)
	req.AckIDs = ids
	return c.do(ctx, uri, methodPost, req, nil)
}

// Extend extends the acknowledgement deadline for a specific message.
func (c *Client) Extend(ctx context.Context, project, subscription string, id string, deadline int) error {
	uri := fmt.Sprintf(endpointAckDeadline, project, subscription)
	req := new(ackDeadlineRequest)
	req.AckIDs = []string{id}
	req.Deadline = deadline
	return c.do(ctx, uri, methodPost, req, nil)
}

func (c *Client) do(ctx context.Context, rawurl, method string, in, out interface{}) error {
	uri, uerr := url.Parse(rawurl)
	if uerr != nil {
		return uerr
	}

	// if we are posting or putting data, we need to
	// write it to the body of the request.
	var buf io.ReadWriter
	if in != nil {
		buf = new(bytes.Buffer)
		json.NewEncoder(buf).Encode(in)
	}

	// creates a new http request to bitbucket.
	req, err := http.NewRequest(method, uri.String(), buf)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// if an error is encountered, parse and return the
	// error response.
	if resp.StatusCode > http.StatusPartialContent {
		err := new(errorResponse)
		json.NewDecoder(resp.Body).Decode(err)
		return err.Error
	}

	// if a json response is expected, parse and return
	// the json response.
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}

	return nil
}
