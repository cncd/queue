package gcp

// Options defines Google Cloud Pubsub options.
type Options struct {
	project          string
	topic            string
	topicDone        string
	subscription     string
	subscriptionDone string
	tokenpath        string
}

// Option configures the Google Cloud pubsub client.
type Option func(*Options)

// WithProject configures the Pubsub client with the named project.
func WithProject(project string) Option {
	return func(opts *Options) {
		opts.project = project
	}
}

// WithTopic configures the Pubsub client with the named topic.
func WithTopic(topicChan, topicDone string) Option {
	return func(opts *Options) {
		opts.topic = topicChan
		opts.topicDone = topicDone
	}
}

// WithSubscription configures the Pubsub client with the named subscription.
func WithSubscription(subscriptionChan, subscriptionDone string) Option {
	return func(opts *Options) {
		opts.subscription = subscriptionChan
		opts.subscriptionDone = subscriptionDone
	}
}

// WithServiceAccountToken configures the Pubsub client with the service
// account token file for authentication and authorization.
func WithServiceAccountToken(path string) Option {
	return func(opts *Options) {
		opts.tokenpath = path
	}
}
