package eventsource

import "time"

// Config is the eventsource section of the public reflow.Config. Empty
// Sources disables the manager entirely (no goroutines, no factories,
// no metrics registered).
type Config struct {
	Sources []SourceConfig `koanf:"sources"`
}

// SourceConfig is one broker→handler binding.
type SourceConfig struct {
	Name        string          `koanf:"name"`
	Type        string          `koanf:"type"`
	Topic       string          `koanf:"topic"`
	Service     string          `koanf:"service"`
	Handler     string          `koanf:"handler"`
	ObjectKey   ExtractorConfig `koanf:"object_key"`
	Idempotency ExtractorConfig `koanf:"idempotency"`
	Retry       RetryConfig     `koanf:"retry"`
	DLQ         DLQConfig       `koanf:"dlq"`
	Backend     BackendConfig   `koanf:"backend"`
}

// ExtractorConfig selects how a per-message string is pulled out of an
// inbound broker message. From: uuid | const | header | json.
type ExtractorConfig struct {
	From  string `koanf:"from"`
	Value string `koanf:"value"`
}

// RetryConfig tunes the exponential-backoff retry middleware.
type RetryConfig struct {
	MaxRetries      int           `koanf:"max_retries"`
	InitialInterval time.Duration `koanf:"initial_interval"`
	MaxInterval     time.Duration `koanf:"max_interval"`
	Multiplier      float64       `koanf:"multiplier"`
}

// DLQConfig wires the PoisonQueue middleware. Topic empty disables DLQ.
type DLQConfig struct {
	Topic    string         `koanf:"topic"`
	Requeuer RequeuerConfig `koanf:"requeuer"`
}

// RequeuerConfig configures the optional components/requeuer.
type RequeuerConfig struct {
	Enabled  bool          `koanf:"enabled"`
	Delay    time.Duration `koanf:"delay"`
	MaxTries int           `koanf:"max_tries"`
}

// BackendConfig is the untyped settings bag passed to the factory.
type BackendConfig struct {
	Settings map[string]string `koanf:"settings"`
}
