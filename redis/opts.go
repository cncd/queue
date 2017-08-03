package redis

// Options defines Redis queue options.
type Options struct {
	addr              string
	password          string
	db                int
	penddingQueueName string
	hostIdentity      string
}

// Option configures the Redis client.
type Option func(*Options)

// WithAddr configures Redis address.
func WithAddr(addr string) Option {
	return func(opts *Options) {
		opts.addr = addr
	}
}

// WithPassword configures Redis password.
func WithPassword(password string) Option {
	return func(opts *Options) {
		opts.password = password
	}
}

// WithDB configures Redis DB.
func WithDB(db int) Option {
	return func(opts *Options) {
		opts.db = db
	}
}

// WithPenddingQueueName configures Redis key where saveing pendding tasks.
func WithPenddingQueueName(queueName string) Option {
	return func(opts *Options) {
		opts.penddingQueueName = queueName
	}
}

// WithHostIdentity saving multiple-server running task in different redis key.
func WithHostIdentity(identity string) Option {
	return func(opts *Options) {
		opts.hostIdentity = identity
	}
}
