package runtime

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
)

// Callback invocation results, used as low-cardinality metric labels.
const (
	ResultSuccess  = "success"
	ResultError    = "error"
	ResultPanic    = "panic"
	ResultCanceled = "canceled"
	ResultTimeout  = "timeout"
)

// Observer receives runtime events for metrics and readiness reporting.
// Implementations must be safe for concurrent use.
type Observer interface {
	// CallbackInvoked records one completed callback invocation.
	CallbackInvoked(name string, result string, duration time.Duration)
	// CallbackRetried records that a failed callback will be retried.
	CallbackRetried(name string)
	// AckCompleted records one acknowledgement attempt result:
	// "acknowledged", "stale", or "error".
	AckCompleted(result string)
	// SourceError records a failed source observation.
	SourceError()
	// SetPendingSignals records the number of signals in the last snapshot.
	SetPendingSignals(count int)
	// SetRuntimeState records lifecycle state: initializing, syncing,
	// starting, running, or shutting-down.
	SetRuntimeState(state string)
}

// Runtime lifecycle states reported through Observer.SetRuntimeState.
const (
	StateInitializing = "initializing"
	StateSyncing      = "syncing"
	StateStarting     = "starting"
	StateRunning      = "running"
	StateShuttingDown = "shutting-down"
)

type noopObserver struct{}

func (noopObserver) CallbackInvoked(string, string, time.Duration) {}
func (noopObserver) CallbackRetried(string)                        {}
func (noopObserver) AckCompleted(string)                           {}
func (noopObserver) SourceError()                                  {}
func (noopObserver) SetPendingSignals(int)                         {}
func (noopObserver) SetRuntimeState(string)                        {}

func errString(message string) error {
	return errors.New("patio: " + message)
}

// backoff produces exponentially growing delays with optional jitter.
type backoff struct {
	config  BackoffConfig
	current time.Duration
}

func newBackoff(config BackoffConfig) *backoff {
	return &backoff{config: config, current: config.Initial}
}

// next returns the next delay and advances the sequence.
func (b *backoff) next() time.Duration {
	delay := b.current
	b.current = min(2*b.current, b.config.Max)
	if b.config.Jitter > 0 {
		delay += time.Duration(rand.Float64() * b.config.Jitter * float64(delay))
	}
	return delay
}

func (b *backoff) reset() {
	b.current = b.config.Initial
}

// invoke runs one callback attempt, converting panics into errors and
// refusing to accept success after the attempt context is done.
func (r *Runtime) invoke(ctx context.Context, name string, action callback.Callback) (err error) {
	started := time.Now()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("callback %q panicked: %v", name, recovered)
			r.observer.CallbackInvoked(name, ResultPanic, time.Since(started))
		}
	}()

	err = action(ctx)
	if err == nil && ctx.Err() != nil {
		err = fmt.Errorf("callback %q returned nil after its context was done: %w", name, ctx.Err())
	}

	result := ResultSuccess
	if err != nil {
		result = ResultError
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			result = ResultTimeout
		case errors.Is(ctx.Err(), context.Canceled):
			result = ResultCanceled
		}
	}
	r.observer.CallbackInvoked(name, result, time.Since(started))
	return err
}
