package redis

// Options defines Google Cloud Pubsub options.
type Options struct {
	addr      string
	password  string
	db        int
	queueName string
}

// Option configures the Google Cloud pubsub client.
type Option func(*Options)

// WithProject configures the Pubsub client with the named project.
func WithRedisAddr(addr string) Option {
	return func(opts *Options) {
		opts.addr = addr
	}
}

func WithRedisPassword(password string) Option {
	return func(opts *Options) {
		opts.password = password
	}
}

func WithRedisDB(db int) Option {
	return func(opts *Options) {
		opts.db = db
	}
}

func WithRedisQueueName(queueName string) Option {
	return func(opts *Options) {
		opts.queueName = queueName
	}
}
