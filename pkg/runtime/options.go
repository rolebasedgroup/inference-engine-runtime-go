package runtime

import (
	"log/slog"
	"time"
)

// BackoffConfig controls retry spacing for failed callbacks and failed
// confirmations. Delays grow exponentially from Initial to Max; Jitter is
// the maximum additional random fraction of each delay.
type BackoffConfig struct {
	Initial time.Duration
	Max     time.Duration
	Jitter  float64
}

// runtimeConfig is the fully resolved option set. It is deliberately
// package-private: embedding programs map their own flags and environment
// onto Option values.
type runtimeConfig struct {
	source          SignalSource
	logger          *slog.Logger
	observer        Observer
	defaultTimeout  time.Duration
	callbackTimeout map[string]time.Duration
	startBudget     time.Duration
	shutdownBudget  time.Duration
	backoff         BackoffConfig
}

// Option configures a Runtime.
type Option func(*runtimeConfig)

// WithSource attaches the single signal source used for hook bindings.
// Hook bindings require a source; start and end work without one.
func WithSource(source SignalSource) Option {
	return func(cfg *runtimeConfig) { cfg.source = source }
}

// WithLogger sets the structured logger used for diagnostics. The default
// is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *runtimeConfig) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

// WithObserver attaches metrics and readiness instrumentation. The default
// is a no-op observer.
func WithObserver(observer Observer) Option {
	return func(cfg *runtimeConfig) {
		if observer != nil {
			cfg.observer = observer
		}
	}
}

// WithDefaultCallbackTimeout sets the execution deadline for callbacks
// without a name-specific override.
func WithDefaultCallbackTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) { cfg.defaultTimeout = timeout }
}

// WithCallbackTimeout overrides the execution deadline for one registered
// callback name. Invalid values are reported by New.
func WithCallbackTimeout(name string, timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if cfg.callbackTimeout == nil {
			cfg.callbackTimeout = map[string]time.Duration{}
		}
		cfg.callbackTimeout[name] = timeout
	}
}

// WithStartBudget sets the total budget for all start callbacks.
func WithStartBudget(budget time.Duration) Option {
	return func(cfg *runtimeConfig) { cfg.startBudget = budget }
}

// WithShutdownBudget sets the budget shared by waiting for a cancelled
// callback to return and by executing end callbacks.
func WithShutdownBudget(budget time.Duration) Option {
	return func(cfg *runtimeConfig) { cfg.shutdownBudget = budget }
}

// WithBackoff configures callback and acknowledgement retry spacing.
func WithBackoff(config BackoffConfig) Option {
	return func(cfg *runtimeConfig) { cfg.backoff = config }
}

func defaultRuntimeConfig() runtimeConfig {
	return runtimeConfig{
		logger:         slog.Default(),
		observer:       noopObserver{},
		defaultTimeout: 30 * time.Second,
		startBudget:    5 * time.Minute,
		shutdownBudget: 30 * time.Second,
		backoff: BackoffConfig{
			Initial: 1 * time.Second,
			Max:     30 * time.Second,
			Jitter:  0.2,
		},
	}
}

func (cfg runtimeConfig) validate() error {
	if cfg.defaultTimeout <= 0 {
		return errString("default callback timeout must be positive")
	}
	if cfg.startBudget <= 0 {
		return errString("start budget must be positive")
	}
	if cfg.shutdownBudget <= 0 {
		return errString("shutdown budget must be positive")
	}
	if cfg.backoff.Initial <= 0 {
		return errString("backoff initial delay must be positive")
	}
	if cfg.backoff.Max < cfg.backoff.Initial {
		return errString("backoff max delay must not be smaller than the initial delay")
	}
	if cfg.backoff.Jitter < 0 || cfg.backoff.Jitter >= 1 {
		return errString("backoff jitter must be in [0,1)")
	}
	for name, timeout := range cfg.callbackTimeout {
		if name == "" {
			return errString("callback timeout override requires a callback name")
		}
		if timeout <= 0 {
			return errString("callback timeout override must be positive")
		}
	}
	return nil
}

func (cfg runtimeConfig) timeoutFor(name string) time.Duration {
	if timeout, ok := cfg.callbackTimeout[name]; ok {
		return timeout
	}
	return cfg.defaultTimeout
}
