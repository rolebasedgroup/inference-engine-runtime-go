package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
)

// Runtime executes registered callbacks for one logical target: start
// callbacks once at process startup, hook callbacks whenever their signal is
// currently executable, and end callbacks once during shutdown. At most one
// callback runs at a time.
type Runtime struct {
	events    event.Events
	hooks     []hookBinding
	callbacks map[string]callback.Callback
	source    SignalSource
	cfg       runtimeConfig
	logger    *slog.Logger
	observer  Observer

	shutdownOnce sync.Once
	shutdownAt   time.Time
}

// hookBinding is a validated hook binding with its signal parsed once.
type hookBinding struct {
	binding event.HookBinding
	key     string
	value   string
}

// New validates events against registry and returns a ready Runtime. All
// configuration must be complete before this call: unknown callbacks,
// duplicate bindings, invalid options, and hook bindings without a source
// are rejected here, never partially at run time. The resolved callbacks
// are copied, so later registrations in the original registry do not affect
// the returned Runtime.
func New(events event.Events, registry *callback.Registry, opts ...Option) (*Runtime, error) {
	if registry == nil {
		return nil, errString("registry must not be nil")
	}
	cfg := defaultRuntimeConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.observer == nil {
		cfg.observer = noopObserver{}
	}

	if err := event.Validate(events); err != nil {
		return nil, fmt.Errorf("runtime: invalid events: %w", err)
	}

	callbacks := make(map[string]callback.Callback)
	resolve := func(name, phase string) error {
		if _, ok := callbacks[name]; ok {
			return nil
		}
		resolved, ok := registry.Lookup(name)
		if !ok {
			return fmt.Errorf("runtime: unknown %s callback %q", phase, name)
		}
		callbacks[name] = resolved
		return nil
	}
	for _, name := range events.Start {
		if err := resolve(name, "start"); err != nil {
			return nil, err
		}
	}
	for _, name := range events.End {
		if err := resolve(name, "end"); err != nil {
			return nil, err
		}
	}
	for _, binding := range events.Hook {
		if err := resolve(binding.Callback, "hook"); err != nil {
			return nil, err
		}
	}

	hooks := make([]hookBinding, 0, len(events.Hook))
	for i, binding := range events.Hook {
		key, value, err := event.ParseSignal(binding.Signal)
		if err != nil {
			return nil, fmt.Errorf("runtime: invalid events: hook binding %d: %w", i, err)
		}
		hooks = append(hooks, hookBinding{binding: binding, key: key, value: value})
	}
	if len(hooks) > 0 && cfg.source == nil {
		return nil, errString("hook bindings require a signal source (WithSource)")
	}

	return &Runtime{
		events: event.Events{
			Start: append([]string(nil), events.Start...),
			Hook:  append([]event.HookBinding(nil), events.Hook...),
			End:   append([]string(nil), events.End...),
		},
		hooks:     hooks,
		callbacks: callbacks,
		source:    cfg.source,
		cfg:       cfg,
		logger:    cfg.logger,
		observer:  cfg.observer,
	}, nil
}

// Run executes the full lifecycle until ctx is canceled, shutdown is
// triggered by the embedding program, or a fatal error occurs.
//
// If a callback refuses to return after cancellation within the shutdown
// budget, Run returns a fatal error without running end callbacks: Go
// cannot kill uncooperative goroutines, and returning an error does not
// mean such code has stopped. Otherwise Run runs end callbacks with a
// fresh, bounded cleanup context and returns nil on graceful shutdown.
func (r *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.observer.SetRuntimeState(StateInitializing)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	notify := make(chan struct{}, 1)

	if r.source != nil {
		go func() {
			if err := r.source.Run(runCtx, notify); err != nil && runCtx.Err() == nil {
				r.logger.Error("signal source stopped unexpectedly", "error", err)
			}
		}()

		r.observer.SetRuntimeState(StateSyncing)
		if err := r.waitInitialSync(runCtx); err != nil {
			// The only failure mode is shutdown being requested.
			return r.finish(ctx)
		}
	}

	r.observer.SetRuntimeState(StateStarting)
	if err := r.runStart(runCtx); err != nil {
		if runCtx.Err() != nil {
			return r.finish(ctx)
		}
		// Startup actions already ran; attempt end cleanup, then fail.
		endErr := r.runEnd(ctx, r.cfg.shutdownBudget)
		return errors.Join(fmt.Errorf("startup failed: %w", err), endErr)
	}

	r.observer.SetRuntimeState(StateRunning)
	if r.source != nil {
		if err := r.hookLoop(runCtx, notify); err != nil {
			return fmt.Errorf("fatal shutdown: %w", err)
		}
	} else {
		// No source means there is nothing to observe; stay alive until
		// shutdown is requested, then run end callbacks.
		<-runCtx.Done()
	}

	return r.finish(ctx)
}

// finish runs the shutdown sequence: end callbacks with a fresh context
// and the remaining shutdown budget.
func (r *Runtime) finish(ctx context.Context) error {
	r.observer.SetRuntimeState(StateShuttingDown)
	r.beginShutdown()
	return r.runEnd(ctx, r.shutdownRemaining())
}

func (r *Runtime) beginShutdown() time.Time {
	r.shutdownOnce.Do(func() {
		r.shutdownAt = time.Now().Add(r.cfg.shutdownBudget)
	})
	return r.shutdownAt
}

func (r *Runtime) shutdownRemaining() time.Duration {
	if r.shutdownAt.IsZero() {
		return r.cfg.shutdownBudget
	}
	if remaining := time.Until(r.shutdownAt); remaining > 0 {
		return remaining
	}
	return 0
}

// waitInitialSync retries Snapshot until it succeeds or shutdown begins.
func (r *Runtime) waitInitialSync(ctx context.Context) error {
	retry := newBackoff(r.cfg.backoff)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := r.source.Snapshot(ctx); err == nil {
			return nil
		} else {
			r.observer.SourceError()
			r.logger.Warn("initial signal snapshot failed", "error", err)
		}
		proceed, _ := r.waitRetry(ctx, retry, nil, Signal{})
		if !proceed {
			return ctx.Err()
		}
	}
}

// runStart executes start callbacks in order. A failed item is retried
// until it succeeds or the start budget is exhausted; already successful
// items are never re-executed.
func (r *Runtime) runStart(ctx context.Context) error {
	if len(r.events.Start) == 0 {
		return nil
	}
	deadline := time.Now().Add(r.cfg.startBudget)
	retry := newBackoff(r.cfg.backoff)

	for _, name := range r.events.Start {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("start budget exhausted before callback %q completed", name)
			}

			result, err := r.runCallbackAttempt(ctx, nil, name, min(r.cfg.timeoutFor(name), remaining), Signal{})
			if result == attemptStuck {
				return err
			}
			if result == attemptCompleted && err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.observer.CallbackRetried(name)
			r.logger.Warn("start callback failed; retrying", "callback", name, "error", err)

			if time.Now().After(deadline) {
				return fmt.Errorf("start budget exhausted before callback %q completed", name)
			}
			proceed, _ := r.waitRetry(ctx, retry, nil, Signal{})
			if !proceed {
				return ctx.Err()
			}
		}
	}
	return nil
}

// signalOutcome describes how processing of one signal ended.
type signalOutcome int

const (
	signalAcknowledged signalOutcome = iota
	signalDropped
	signalShutdown
)

// hookLoop repeatedly snapshots the source, selects the configured-order
// first matching binding, and processes it to completion before
// re-snapshotting. Snapshot errors are retried and never silently ignored.
func (r *Runtime) hookLoop(ctx context.Context, notify <-chan struct{}) error {
	snapshotRetry := newBackoff(r.cfg.backoff)
	for ctx.Err() == nil {
		snapshot, err := r.source.Snapshot(ctx)
		if err != nil {
			r.observer.SourceError()
			r.logger.Warn("signal snapshot failed", "error", err)
			proceed, _ := r.waitRetry(ctx, snapshotRetry, nil, Signal{})
			if !proceed {
				return nil
			}
			continue
		}
		snapshotRetry.reset()
		r.observer.SetPendingSignals(len(snapshot.Signals))

		binding := matchHookBinding(r.hooks, snapshot)
		if binding == nil {
			select {
			case <-notify:
			case <-ctx.Done():
			}
			continue
		}

		outcome, err := r.processSignal(ctx, notify, *binding, snapshot)
		if err != nil {
			return err
		}
		switch outcome {
		case signalAcknowledged:
			r.logger.Info("hook signal completed", "callback", binding.binding.Callback, "signal", binding.binding.Signal)
		case signalDropped:
			r.logger.Info("hook signal no longer valid", "callback", binding.binding.Callback, "signal", binding.binding.Signal)
		case signalShutdown:
			return nil
		}
	}
	return nil
}

// matchHookBinding returns the first binding, in configuration order, whose
// exact signal is present in the snapshot.
func matchHookBinding(bindings []hookBinding, snapshot SourceSnapshot) *hookBinding {
	for i := range bindings {
		if snapshot.Has(bindings[i].key, bindings[i].value) {
			return &bindings[i]
		}
	}
	return nil
}

// processSignal drives one hook binding from execution through
// acknowledgement. The callback is never re-executed after success while
// acknowledgement is pending; temporary acknowledgement failures retry only
// the confirmation. If the signal becomes invalid, retry state and the
// completion record are dropped and the newer signal is never confirmed by
// this path.
func (r *Runtime) processSignal(ctx context.Context, notify <-chan struct{}, binding hookBinding, initial SourceSnapshot) (signalOutcome, error) {
	name := binding.binding.Callback
	signal := Signal{Key: binding.key, Value: binding.value}
	callbackRetry := newBackoff(r.cfg.backoff)
	ackRetry := newBackoff(r.cfg.backoff)
	snapshot := initial
	completed := false

	for {
		if ctx.Err() != nil {
			return signalShutdown, nil
		}

		if !completed {
			// Re-read the execution basis before every attempt; a signal
			// that is no longer valid must not be executed.
			fresh, err := r.source.Snapshot(ctx)
			if err != nil {
				r.observer.SourceError()
				r.logger.Warn("signal snapshot failed before callback execution", "callback", name, "error", err)
				proceed, _ := r.waitRetry(ctx, callbackRetry, notify, signal)
				if !proceed {
					return signalShutdown, nil
				}
				continue
			}
			snapshot = fresh
			r.observer.SetPendingSignals(len(fresh.Signals))
			if !fresh.Has(signal.Key, signal.Value) {
				return signalDropped, nil
			}

			result, err := r.runCallbackAttempt(ctx, notify, name, r.cfg.timeoutFor(name), signal)
			switch result {
			case attemptCompleted:
				if err == nil {
					completed = true
					continue
				}
				if ctx.Err() != nil {
					return signalShutdown, nil
				}
				r.observer.CallbackRetried(name)
				r.logger.Warn("hook callback failed; retrying", "callback", name, "signal", binding.binding.Signal, "error", err)
				proceed, dropped := r.waitRetry(ctx, callbackRetry, notify, signal)
				if dropped {
					return signalDropped, nil
				}
				if !proceed {
					return signalShutdown, nil
				}
				continue
			case attemptDropped:
				return signalDropped, nil
			case attemptShutdown:
				return signalShutdown, nil
			case attemptStuck:
				return signalShutdown, err
			}
		}

		// Acknowledge the completed callback against the observation used
		// for the successful attempt. Temporary failures retry only the
		// confirmation, never the business action.
		ackCtx, cancel := context.WithTimeout(ctx, r.cfg.timeoutFor(name))
		status, ackErr := r.source.Ack(ackCtx, snapshot, signal)
		cancel()
		if ackErr == nil {
			switch status {
			case Acknowledged:
				r.observer.AckCompleted(Acknowledged.String())
				return signalAcknowledged, nil
			case Stale:
				r.observer.AckCompleted(Stale.String())
				return signalDropped, nil
			default:
				ackErr = fmt.Errorf("unexpected ack status %d", status)
			}
		}
		r.observer.AckCompleted("error")
		if ctx.Err() != nil {
			return signalShutdown, nil
		}
		r.logger.Warn("signal acknowledgement failed; retrying", "callback", name, "signal", binding.binding.Signal, "error", ackErr)
		proceed, _ := r.waitRetry(ctx, ackRetry, nil, signal)
		if !proceed {
			return signalShutdown, nil
		}
	}
}

// attemptResult describes the end of one callback attempt.
type attemptResult int

const (
	attemptCompleted attemptResult = iota
	attemptDropped
	attemptShutdown
	attemptStuck
)

// runCallbackAttempt starts one callback execution and handles, while it is
// running: the attempt deadline, shutdown, and (for hook signals)
// invalidation of the observed signal. Invalidation cancels the callback
// and waits for it to return before the next action runs.
func (r *Runtime) runCallbackAttempt(
	ctx context.Context,
	notify <-chan struct{},
	name string,
	timeout time.Duration,
	signal Signal,
) (attemptResult, error) {
	callback := r.callbacks[name]
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.invoke(attemptCtx, name, callback)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			return attemptCompleted, err
		case <-timer.C:
			// Deadline exceeded: cancel and wait for the callback to
			// return. Waiting preserves the one-callback-at-a-time
			// guarantee; an unresponsive callback is only surfaced as a
			// fatal error during shutdown.
			return r.cancelAndWait(ctx, cancel, done, name)
		case <-notify:
			fresh, err := r.source.Snapshot(ctx)
			if err != nil {
				r.observer.SourceError()
				r.logger.Warn("refreshing signal snapshot failed during callback execution", "callback", name, "error", err)
				continue
			}
			if fresh.Has(signal.Key, signal.Value) {
				// Irrelevant notification: keep waiting without touching
				// any retry or timer state.
				continue
			}
			r.logger.Info("signal invalidated during callback execution; cancelling", "callback", name, "signal", signal.Key+"="+signal.Value)
			result, err := r.cancelAndWait(ctx, cancel, done, name)
			if result == attemptCompleted {
				return attemptDropped, nil
			}
			return result, err
		case <-ctx.Done():
			result, err := r.cancelAndWait(ctx, cancel, done, name)
			if result == attemptCompleted {
				return attemptShutdown, err
			}
			return result, err
		}
	}
}

// cancelAndWait cancels an in-flight callback and waits for it to return.
// If shutdown has begun, the wait is bounded by the remaining shutdown
// budget; exceeding it is a fatal error.
func (r *Runtime) cancelAndWait(ctx context.Context, cancel context.CancelFunc, done <-chan error, name string) (attemptResult, error) {
	cancel()
	select {
	case err := <-done:
		return attemptCompleted, err
	case <-ctx.Done():
		remaining := time.Until(r.beginShutdown())
		if remaining <= 0 {
			return attemptStuck, fmt.Errorf("callback %q did not return within the shutdown budget after cancellation", name)
		}
		select {
		case err := <-done:
			return attemptCompleted, err
		case <-time.After(remaining):
			return attemptStuck, fmt.Errorf("callback %q did not return within the shutdown budget after cancellation", name)
		}
	}
}

// waitRetry waits for the next retry while watching for shutdown and, when
// notify is not nil, for invalidation of signal. Notifications that leave
// the signal valid never reset or bypass the backoff timer.
func (r *Runtime) waitRetry(ctx context.Context, retry *backoff, notify <-chan struct{}, signal Signal) (proceed bool, dropped bool) {
	timer := time.NewTimer(retry.next())
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return true, false
		case <-ctx.Done():
			return false, false
		case <-notify:
			fresh, err := r.source.Snapshot(ctx)
			if err != nil {
				r.observer.SourceError()
				continue
			}
			if !fresh.Has(signal.Key, signal.Value) {
				return false, true
			}
			// Irrelevant notification.
		}
	}
}

// runEnd executes end callbacks in order with a fresh, bounded cleanup
// context that does not inherit the canceled run context.
func (r *Runtime) runEnd(ctx context.Context, budget time.Duration) error {
	if len(r.events.End) == 0 {
		return nil
	}
	if budget <= 0 {
		return fmt.Errorf("shutdown budget exhausted before end callbacks could run")
	}
	deadline := time.Now().Add(budget)
	base := context.WithoutCancel(ctx)
	retry := newBackoff(r.cfg.backoff)

	for _, name := range r.events.End {
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("shutdown budget exhausted before end callback %q completed", name)
			}
			timeout := min(r.cfg.timeoutFor(name), remaining)
			endCtx, cancel := context.WithTimeout(base, timeout)
			result, err := r.runEndAttempt(endCtx, cancel, name, deadline)
			if result == attemptStuck {
				return err
			}
			if result == attemptCompleted && err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("shutdown budget exhausted before end callback %q completed", name)
			}
			r.observer.CallbackRetried(name)
			r.logger.Warn("end callback failed; retrying", "callback", name, "error", err)
			timer := time.NewTimer(retry.next())
			select {
			case <-timer.C:
			case <-time.After(time.Until(deadline)):
				timer.Stop()
				return fmt.Errorf("shutdown budget exhausted before end callback %q completed", name)
			}
		}
	}
	return nil
}

// runEndAttempt runs one end callback attempt, bounding the wait for an
// unresponsive callback by the overall end deadline.
func (r *Runtime) runEndAttempt(ctx context.Context, cancel context.CancelFunc, name string, deadline time.Time) (attemptResult, error) {
	callback := r.callbacks[name]
	done := make(chan error, 1)
	go func() {
		done <- r.invoke(ctx, name, callback)
	}()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case err := <-done:
		cancel()
		return attemptCompleted, err
	case <-timer.C:
		return r.cancelAndWaitEnd(cancel, done, name, deadline)
	case <-ctx.Done():
		return r.cancelAndWaitEnd(cancel, done, name, deadline)
	}
}

func (r *Runtime) cancelAndWaitEnd(cancel context.CancelFunc, done <-chan error, name string, deadline time.Time) (attemptResult, error) {
	cancel()
	select {
	case err := <-done:
		return attemptCompleted, err
	case <-time.After(time.Until(deadline)):
		return attemptStuck, fmt.Errorf("end callback %q did not return within the shutdown budget", name)
	}
}
