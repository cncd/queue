package internal

import "time"

type (
	pushRequest struct {
		Messages []Message `json:"messages"`
	}

	pushResponse struct {
		MessageIds []string `json:"messageIds"`
	}

	pullRequest struct {
		MaxMessages int `json:"maxMessages"`
	}

	pullResponse struct {
		Messages []struct {
			AckID   string  `json:"ackId"`
			Message Message `json:"message"`
		} `json:"receivedMessages"`
	}

	ackRequest struct {
		AckIDs []string `json:"ackIds"`
	}

	ackDeadlineRequest struct {
		AckIDs   []string `json:"ackIds"`
		Deadline int      `json:"ackDeadlineSeconds"`
	}

	errorResponse struct {
		Error *Error `json:"error"`
	}

	// Message represents a pubsub message.
	Message struct {
		MessageID   string            `json:"messageId"`
		AckID       string            `json:"-"`
		Data        []byte            `json:"data"`
		PublishTime time.Time         `json:"publishTime"`
		Attributes  map[string]string `json:"attributes"`
	}

	// Error represents a pubsub error message.
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	}
)

// Error returns the error message in string format.
func (e *Error) Error() string {
	return e.Message
}
