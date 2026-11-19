// Command custom-runtime shows how an embedding program registers its own
// Go callbacks and changes only events.yaml to rebind them. Run it from the
// repository root:
//
//	go run ./examples/custom-runtime --config examples/custom-runtime/events.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
)

func main() {
	configPath := flag.String("config", "events.yaml", "path to events.yaml")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Business dependencies are constructed here and captured by closures;
	// the framework does not inject anything into callbacks.
	registry := runtime.NewDefaultRegistry()
	if err := registry.Register("prepare-update", logCallback(logger, "prepare-update")); err != nil {
		exitWithError(err)
	}
	if err := registry.Register("activate", logCallback(logger, "activate")); err != nil {
		exitWithError(err)
	}
	if err := registry.Register("cleanup", logCallback(logger, "cleanup")); err != nil {
		exitWithError(err)
	}

	events, err := loadEvents(*configPath)
	if err != nil {
		exitWithError(err)
	}

	// Every error is checked before the runtime starts; no callback runs if
	// configuration is invalid.
	rt, err := runtime.New(events, registry,
		runtime.WithLogger(logger),
		runtime.WithDefaultCallbackTimeout(30*time.Second),
		runtime.WithStartBudget(5*time.Minute),
		runtime.WithShutdownBudget(30*time.Second),
	)
	if err != nil {
		exitWithError(err)
	}

	// SIGTERM/SIGINT (or embedding-program cancellation) triggers the
	// shutdown sequence: cancel the current callback, wait for it, run end.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := rt.Run(ctx); err != nil {
		exitWithError(err)
	}
}

func loadEvents(path string) (event.Events, error) {
	file, err := os.Open(path)
	if err != nil {
		return event.Events{}, fmt.Errorf("open events config: %w", err)
	}
	defer file.Close()
	return event.LoadEvents(file)
}

func logCallback(logger *slog.Logger, name string) callback.Callback {
	return func(ctx context.Context) error {
		logger.Info("callback executed", "callback", name)
		// Real programs would call business libraries here and must honor
		// ctx cancellation and be idempotent.
		return nil
	}
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, "custom-runtime:", err)
	os.Exit(1)
}
