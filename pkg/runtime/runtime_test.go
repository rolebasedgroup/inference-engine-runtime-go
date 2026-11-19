package runtime_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
)

// fakeSource is an in-memory runtime.SignalSource for exercising the runtime.
type fakeSource struct {
	mu         sync.Mutex
	signals    []runtime.Signal
	clearOnAck bool
	ackStatus  runtime.AckStatus
	ackErr     error
	acked      []runtime.Signal
	ackCalls   int
	notify     chan<- struct{}
}

func newFakeSource(signals ...runtime.Signal) *fakeSource {
	return &fakeSource{
		signals:    append([]runtime.Signal(nil), signals...),
		ackStatus:  runtime.Acknowledged,
		clearOnAck: true,
	}
}

func (f *fakeSource) Run(ctx context.Context, notify chan<- struct{}) error {
	f.mu.Lock()
	f.notify = notify
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeSource) setSignals(signals ...runtime.Signal) {
	f.mu.Lock()
	f.signals = append([]runtime.Signal(nil), signals...)
	f.mu.Unlock()
	f.notifyRuntime()
}

func (f *fakeSource) notifyRuntime() {
	f.mu.Lock()
	notify := f.notify
	f.mu.Unlock()
	if notify == nil {
		return
	}
	select {
	case notify <- struct{}{}:
	default:
	}
}

func (f *fakeSource) setAck(status runtime.AckStatus, err error) {
	f.mu.Lock()
	f.ackStatus, f.ackErr = status, err
	f.mu.Unlock()
}

func (f *fakeSource) Snapshot(ctx context.Context) (runtime.SourceSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return runtime.SourceSnapshot{
		Signals: append([]runtime.Signal(nil), f.signals...),
		Token:   len(f.signals),
	}, nil
}

func (f *fakeSource) Ack(ctx context.Context, snapshot runtime.SourceSnapshot, signal runtime.Signal) (runtime.AckStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls++
	f.acked = append(f.acked, signal)
	if f.ackErr != nil {
		return 0, f.ackErr
	}
	if f.ackStatus == runtime.Acknowledged && f.clearOnAck {
		f.signals = removeSignal(f.signals, signal)
	}
	return f.ackStatus, nil
}

func (f *fakeSource) ackedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acked)
}

func removeSignal(signals []runtime.Signal, target runtime.Signal) []runtime.Signal {
	kept := signals[:0:0]
	for _, signal := range signals {
		if signal != target {
			kept = append(kept, signal)
		}
	}
	return kept
}

type recorder struct {
	mu      sync.Mutex
	entries []string
}

func (rec *recorder) record(entry string) {
	rec.mu.Lock()
	rec.entries = append(rec.entries, entry)
	rec.mu.Unlock()
}

func (rec *recorder) snapshot() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.entries...)
}

func testOptions(source runtime.SignalSource) []runtime.Option {
	opts := []runtime.Option{
		runtime.WithLogger(slog.New(slog.DiscardHandler)),
		runtime.WithBackoff(runtime.BackoffConfig{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}),
		runtime.WithDefaultCallbackTimeout(100 * time.Millisecond),
		runtime.WithStartBudget(2 * time.Second),
		runtime.WithShutdownBudget(300 * time.Millisecond),
	}
	if source != nil {
		opts = append(opts, runtime.WithSource(source))
	}
	return opts
}

func TestNewValidatesRegistryAndSource(t *testing.T) {
	registry := runtime.NewDefaultRegistry()
	if err := registry.Register("action", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	if _, err := runtime.New(event.Events{Start: []string{"missing"}}, registry); err == nil {
		t.Fatal("unknown start callback must be rejected")
	}
	if _, err := runtime.New(event.Events{End: []string{"missing"}}, registry); err == nil {
		t.Fatal("unknown end callback must be rejected")
	}
	hookEvents := event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "action"}}}
	if _, err := runtime.New(hookEvents, registry); err == nil {
		t.Fatal("hook bindings without a source must be rejected")
	}
	if _, err := runtime.New(event.Events{}, nil); err == nil {
		t.Fatal("nil registry must be rejected")
	}
}

func TestNewDefaultRegistriesAreIndependent(t *testing.T) {
	registry := runtime.NewDefaultRegistry()
	if _, ok := registry.Lookup("noop"); !ok {
		t.Fatal("default registry must contain noop")
	}
	other := runtime.NewDefaultRegistry()
	if err := other.Register("user", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("register user callback: %v", err)
	}
	if _, ok := registry.Lookup("user"); ok {
		t.Fatal("registering on one default registry polluted the other")
	}
}

func TestRuntimeSnapshotIgnoresLaterRegistrations(t *testing.T) {
	registry := callback.NewRegistry()
	if err := registry.Register("known", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	events := event.Events{Start: []string{"known"}}
	if _, err := runtime.New(events, registry); err != nil {
		t.Fatalf("New before later registration: %v", err)
	}

	// A second runtime on the same registry only sees callbacks that
	// existed when it was constructed.
	late := event.Events{Start: []string{"late"}}
	if _, err := runtime.New(late, registry); err == nil {
		t.Fatal("late callback must not be visible to a new runtime")
	}
}

func TestRunExecutesStartHookEndInOrder(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "example.com/update", Value: "true"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	endDone := make(chan struct{})

	mustRegister(t, registry, "prepare", func(ctx context.Context) error {
		rec.record("start:prepare")
		return nil
	})
	mustRegister(t, registry, "update", func(ctx context.Context) error {
		rec.record("hook:update")
		return nil
	})
	mustRegister(t, registry, "cleanup", func(ctx context.Context) error {
		if ctx.Err() != nil {
			rec.record("end:cleanup:canceled-ctx")
		} else {
			rec.record("end:cleanup")
		}
		close(endDone)
		return nil
	})

	events := event.Events{
		Start: []string{"prepare"},
		Hook:  []event.HookBinding{{Signal: "example.com/update=true", Callback: "update"}},
		End:   []string{"cleanup"},
	}
	rt, err := runtime.New(events, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for strings.Join(rec.snapshot(), ",") != "start:prepare,hook:update" {
		if time.Now().After(deadline) {
			t.Fatalf("entries = %v", rec.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-endDone:
	case <-time.After(2 * time.Second):
		t.Fatal("end callback did not run")
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	entries := rec.snapshot()
	want := []string{"start:prepare", "hook:update", "end:cleanup"}
	if strings.Join(entries, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", entries, want)
	}
	if source.ackedCount() != 1 {
		t.Fatalf("ack calls = %d, want 1", source.ackedCount())
	}
}

func TestRunWithoutSourceWaitsForShutdown(t *testing.T) {
	rec := &recorder{}
	registry := callback.NewRegistry()
	mustRegister(t, registry, "prepare", func(context.Context) error {
		rec.record("start")
		return nil
	})
	mustRegister(t, registry, "cleanup", func(context.Context) error {
		rec.record("end")
		return nil
	})

	rt, err := runtime.New(event.Events{Start: []string{"prepare"}, End: []string{"cleanup"}}, registry, testOptions(nil)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	select {
	case <-done:
		t.Fatal("Run returned before shutdown was requested")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Join(rec.snapshot(), ","); got != "start,end" {
		t.Fatalf("entries = %q", got)
	}
}

func TestStartRetriesFailedItemWithoutRerunningSuccessfulItems(t *testing.T) {
	rec := &recorder{}
	registry := callback.NewRegistry()
	failures := 0
	var mu sync.Mutex
	mustRegister(t, registry, "first", func(context.Context) error {
		rec.record("first")
		return nil
	})
	mustRegister(t, registry, "second", func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		failures++
		if failures < 3 {
			return errors.New("not ready")
		}
		rec.record("second")
		return nil
	})

	rt, err := runtime.New(event.Events{Start: []string{"first", "second"}}, registry, testOptions(nil)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for strings.Join(rec.snapshot(), ",") != "first,second" {
		if time.Now().After(deadline) {
			t.Fatalf("entries = %q, want first,second", rec.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestStartupFailureRunsEndCleanupAndReturnsError(t *testing.T) {
	rec := &recorder{}
	registry := callback.NewRegistry()
	mustRegister(t, registry, "broken", func(context.Context) error { return errors.New("boom") })
	mustRegister(t, registry, "cleanup", func(context.Context) error {
		rec.record("end")
		return nil
	})

	rt, err := runtime.New(event.Events{Start: []string{"broken"}, End: []string{"cleanup"}}, registry,
		runtime.WithBackoff(runtime.BackoffConfig{Initial: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0}),
		runtime.WithDefaultCallbackTimeout(50*time.Millisecond),
		runtime.WithStartBudget(120*time.Millisecond),
		runtime.WithShutdownBudget(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = rt.Run(ctx)
	if err == nil {
		t.Fatal("startup failure must return an error")
	}
	if !strings.Contains(err.Error(), "startup failed") {
		t.Fatalf("error = %v", err)
	}
	if got := strings.Join(rec.snapshot(), ","); got != "end" {
		t.Fatalf("entries = %q, want end", got)
	}
}

func TestHookAckFailureRetriesAckWithoutRerunningCallback(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	executed := make(chan struct{}, 1)
	release := make(chan struct{})
	mustRegister(t, registry, "work", func(context.Context) error {
		rec.record("work")
		executed <- struct{}{}
		<-release
		return nil
	})

	rt, err := runtime.New(event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "work"}}}, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// The callback succeeds; make acknowledgement fail once, then succeed
	// (which also clears the signal like a real source would).
	source.setAck(runtime.Acknowledged, errors.New("temporary"))
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not execute")
	}
	close(release)
	waitForCondition(t, func() bool { return source.ackedCount() >= 2 })
	source.setAck(runtime.Acknowledged, nil)
	waitForCondition(t, func() bool { return source.ackedCount() >= 3 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Join(rec.snapshot(), ","); got != "work" {
		t.Fatalf("callback executions = %q, want exactly one", got)
	}
}

func TestHookStaleAckDropsCompletionRecord(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	executed := make(chan struct{}, 1)
	release := make(chan struct{})
	calls := 0
	var mu sync.Mutex
	mustRegister(t, registry, "work", func(ctx context.Context) error {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()
		if call == 1 {
			rec.record("work")
			executed <- struct{}{}
			<-release
			return nil
		}
		// If the signal were wrongly re-selected after the stale drop, do
		// not record it; hold until shutdown cancels the attempt.
		<-ctx.Done()
		return ctx.Err()
	})

	rt, err := runtime.New(event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "work"}}}, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// The single callback execution is held until the source is prepared
	// to report the original observation as stale.
	source.setAck(runtime.Stale, nil)
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not execute")
	}
	close(release)
	waitForCondition(t, func() bool { return source.ackedCount() >= 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Join(rec.snapshot(), ","); got != "work" {
		t.Fatalf("callback executions = %q, want exactly one", got)
	}
}

func TestHookPanicIsFailureAndRetried(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	attempts := 0
	var mu sync.Mutex
	mustRegister(t, registry, "work", func(context.Context) error {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			panic("exploded")
		}
		rec.record("work")
		return nil
	})

	rt, err := runtime.New(event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "work"}}}, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.snapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("callback did not recover from panic; attempts = %d", attempts)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHookCallbackTimeoutIsFailureAndRetried(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	registry := callback.NewRegistry()
	attempts := 0
	var mu sync.Mutex
	mustRegister(t, registry, "work", func(ctx context.Context) error {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})

	rt, err := runtime.New(event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "work"}}}, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		current := attempts
		mu.Unlock()
		if current >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout was not retried; attempts = %d", current)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestSignalInvalidationCancelsRunningCallbackAndWaits(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	returned := make(chan struct{})

	mustRegister(t, registry, "work", func(ctx context.Context) error {
		rec.record("work:start")
		<-ctx.Done()
		rec.record("work:canceled")
		close(returned)
		return ctx.Err()
	})
	mustRegister(t, registry, "next", func(context.Context) error {
		rec.record("next:start")
		return nil
	})

	events := event.Events{Hook: []event.HookBinding{
		{Signal: "a=1", Callback: "work"},
		{Signal: "b=1", Callback: "next"},
	}}
	rt, err := runtime.New(events, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.snapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("callback did not start")
		}
		time.Sleep(time.Millisecond)
	}
	// Invalidate the signal; the next signal only becomes valid when the
	// old callback has returned.
	source.setSignals(runtime.Signal{Key: "b", Value: "1"})
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("callback was not cancelled after invalidation")
	}
	deadline = time.Now().Add(2 * time.Second)
	for strings.Join(rec.snapshot(), ",") != "work:start,work:canceled,next:start" {
		if time.Now().After(deadline) {
			t.Fatalf("entries = %v", rec.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestShutdownWithUnresponsiveCallbackIsFatalAndSkipsEnd(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	rec := &recorder{}
	registry := callback.NewRegistry()
	mustRegister(t, registry, "stuck", func(ctx context.Context) error {
		rec.record("stuck:start")
		<-make(chan struct{}) // ignores context cancellation forever
		return nil
	})
	mustRegister(t, registry, "cleanup", func(context.Context) error {
		rec.record("end")
		return nil
	})

	rt, err := runtime.New(
		event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "stuck"}}, End: []string{"cleanup"}},
		registry,
		runtime.WithSource(source),
		runtime.WithBackoff(runtime.BackoffConfig{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}),
		runtime.WithDefaultCallbackTimeout(10*time.Minute),
		runtime.WithShutdownBudget(80*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(rec.snapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("callback did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not return") {
			t.Fatalf("Run error = %v, want fatal callback-stuck error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after shutdown budget")
	}
	for _, entry := range rec.snapshot() {
		if entry == "end" {
			t.Fatal("end callbacks must not run after a fatal stuck callback")
		}
	}
}

func TestHookBindingsRunInConfiguredOrder(t *testing.T) {
	source := newFakeSource(
		runtime.Signal{Key: "a", Value: "1"},
		runtime.Signal{Key: "b", Value: "1"},
	)
	rec := &recorder{}
	registry := callback.NewRegistry()
	mustRegister(t, registry, "first", func(context.Context) error {
		rec.record("first")
		return nil
	})
	mustRegister(t, registry, "second", func(context.Context) error {
		rec.record("second")
		return nil
	})

	events := event.Events{Hook: []event.HookBinding{
		{Signal: "b=1", Callback: "second"},
		{Signal: "a=1", Callback: "first"},
	}}
	rt, err := runtime.New(events, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for strings.Join(rec.snapshot(), ",") != "second,first" {
		if time.Now().After(deadline) {
			t.Fatalf("entries = %v", rec.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCallbackReturningNilAfterCancelIsFailure(t *testing.T) {
	source := newFakeSource(runtime.Signal{Key: "a", Value: "1"})
	registry := callback.NewRegistry()
	attempts := 0
	var mu sync.Mutex
	mustRegister(t, registry, "work", func(ctx context.Context) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		<-ctx.Done()
		return nil // must not be accepted as success
	})

	rt, err := runtime.New(event.Events{Hook: []event.HookBinding{{Signal: "a=1", Callback: "work"}}}, registry, testOptions(source)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	time.Sleep(120 * time.Millisecond) // let the timeout expire
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The callback kept reporting success after cancellation; the runtime
	// must still be retrying rather than acknowledging it.
	mu.Lock()
	finalAttempts := attempts
	mu.Unlock()
	if finalAttempts < 2 {
		t.Fatalf("attempts = %d; nil-after-cancel must be treated as failure", finalAttempts)
	}
}

func mustRegister(t *testing.T, registry *callback.Registry, name string, action callback.Callback) {
	t.Helper()
	if err := registry.Register(name, action); err != nil {
		t.Fatalf("register %q: %v", name, err)
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
