// Command patio is the standard assembly of the callback runtime: it uses
// the default callback registry and maps process flags and environment
// variables onto patio options. It intentionally ships no business actions
// beyond the noop callback used for smoke testing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rolebasedgroup/inference-engine-runtime-go/internal/diagnostics"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/source/rbg"
)

const defaultConfigPath = "/etc/patio/events.yaml"

type cliConfig struct {
	configPath      string
	sourceKind      string
	riNamespace     string
	riName          string
	kubeconfig      string
	labelFile       string
	labelPollEvery  time.Duration
	callbackTimeout time.Duration
	startBudget     time.Duration
	shutdownBudget  time.Duration
	backoffInitial  time.Duration
	backoffMax      time.Duration
	jitter          float64
	listenAddress   string
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func addCommonFlags(flags *flag.FlagSet, config *cliConfig) {
	flags.StringVar(&config.configPath, "config", envOr("PATIO_CONFIG", defaultConfigPath), "path to events.yaml")
	flags.StringVar(&config.sourceKind, "source", "none", "signal source: none or rbg (experimental)")
	flags.StringVar(&config.riNamespace, "ri-namespace", "", "RoleInstance namespace (required for --source=rbg)")
	flags.StringVar(&config.riName, "ri-name", "", "RoleInstance name (required for --source=rbg)")
	flags.StringVar(&config.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig (defaults to in-cluster configuration)")
	flags.StringVar(&config.labelFile, "label-file", envOr("PATIO_LABEL_FILE", ""), "downwardAPI label file used for wake-up instead of watching the API server (requires --source=rbg)")
}

func addRunFlags(flags *flag.FlagSet, config *cliConfig) {
	addCommonFlags(flags, config)
	flags.DurationVar(&config.callbackTimeout, "callback-timeout", 30*time.Second, "default callback execution timeout")
	flags.DurationVar(&config.startBudget, "start-budget", 5*time.Minute, "total start budget")
	flags.DurationVar(&config.shutdownBudget, "shutdown-budget", 30*time.Second, "budget shared by shutdown and end callbacks")
	flags.DurationVar(&config.backoffInitial, "backoff-initial", time.Second, "initial retry backoff")
	flags.DurationVar(&config.backoffMax, "backoff-max", 30*time.Second, "maximum retry backoff")
	flags.Float64Var(&config.jitter, "jitter", 0.2, "retry backoff jitter fraction in [0,1)")
	flags.StringVar(&config.listenAddress, "listen-address", envOr("PATIO_LISTEN_ADDRESS", ":9090"), "diagnostics listen address (empty disables HTTP endpoints)")
	flags.DurationVar(&config.labelPollEvery, "label-poll-interval", time.Second, "label file polling interval")
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "run":
		flags := flag.NewFlagSet("patio run", flag.ExitOnError)
		config := &cliConfig{}
		addRunFlags(flags, config)
		_ = flags.Parse(os.Args[2:])
		err = run(logger, config)
	case "validate":
		flags := flag.NewFlagSet("patio validate", flag.ExitOnError)
		config := &cliConfig{}
		addCommonFlags(flags, config)
		_ = flags.Parse(os.Args[2:])
		err = validate(logger, config)
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		logger.Error("patio failed", "error", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  patio run     [flags]      run the runtime until SIGTERM/SIGINT
  patio validate [flags]     strictly validate events.yaml and bindings

common flags:
  -config PATH          events.yaml path (env PATIO_CONFIG, default /etc/patio/events.yaml)
  -source none|rbg      signal source (default none; rbg is experimental)
  -ri-namespace NS      RoleInstance namespace (source=rbg)
  -ri-name NAME         RoleInstance name (source=rbg)
  -kubeconfig PATH      kubeconfig path (env KUBECONFIG)
  -label-file PATH      downwardAPI label file for wake-up instead of watch (env PATIO_LABEL_FILE, requires source=rbg)

run-only flags control callback timeout, start/shutdown budgets, backoff,
the label-file polling interval, and the diagnostics listen address
(env PATIO_LISTEN_ADDRESS, default :9090).
`)
}

func loadEventsFile(path string) (event.Events, error) {
	file, err := os.Open(path)
	if err != nil {
		return event.Events{}, fmt.Errorf("open events config %s: %w", path, err)
	}
	defer file.Close()
	events, err := event.LoadEvents(file)
	if err != nil {
		return event.Events{}, fmt.Errorf("load events config %s: %w", path, err)
	}
	return events, nil
}

func validateSourceConfig(config *cliConfig) error {
	switch strings.ToLower(config.sourceKind) {
	case "none":
		return nil
	case "rbg":
		if config.riNamespace == "" || config.riName == "" {
			return errors.New("--source=rbg requires --ri-namespace and --ri-name")
		}
		if config.labelFile != "" && config.labelPollEvery <= 0 {
			return errors.New("--label-file requires a positive --label-poll-interval")
		}
		return nil
	default:
		return fmt.Errorf("unknown --source %q (want none or rbg)", config.sourceKind)
	}
}

func validate(logger *slog.Logger, config *cliConfig) error {
	if err := validateSourceConfig(config); err != nil {
		return err
	}
	events, err := loadEventsFile(config.configPath)
	if err != nil {
		return err
	}
	// New performs the full startup validation, including unknown callback
	// names and the hook-without-source rule. Diagnostics are not started
	// and no callback runs.
	_, err = runtime.New(events, runtime.NewDefaultRegistry())
	if err != nil {
		return err
	}
	logger.Info("events configuration is valid", "path", config.configPath)
	return nil
}

func run(logger *slog.Logger, config *cliConfig) error {
	if err := validateSourceConfig(config); err != nil {
		return err
	}
	events, err := loadEventsFile(config.configPath)
	if err != nil {
		// Configuration validation happens before any callback runs.
		return err
	}

	registry := runtime.NewDefaultRegistry()
	opts := []runtime.Option{
		runtime.WithLogger(logger),
		runtime.WithDefaultCallbackTimeout(config.callbackTimeout),
		runtime.WithStartBudget(config.startBudget),
		runtime.WithShutdownBudget(config.shutdownBudget),
		runtime.WithBackoff(runtime.BackoffConfig{
			Initial: config.backoffInitial,
			Max:     config.backoffMax,
			Jitter:  config.jitter,
		}),
	}

	if strings.ToLower(config.sourceKind) == "rbg" {
		source, err := newRBGSource(logger, config)
		if err != nil {
			return err
		}
		opts = append(opts, runtime.WithSource(source))
	}

	server, err := diagnostics.New(logger, config.listenAddress, nil)
	if err != nil {
		return err
	}
	opts = append(opts, runtime.WithObserver(server))

	rt, err := runtime.New(events, registry, opts...)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if address := server.Address(); address != "" {
		logger.Info("diagnostics listening", "address", address)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Start(ctx) }()

	runErr := rt.Run(ctx)
	stop()
	if err := <-serverDone; err != nil {
		logger.Warn("diagnostics server stopped with error", "error", err)
	}
	return runErr
}

func newRBGSource(logger *slog.Logger, config *cliConfig) (runtime.SignalSource, error) {
	restConfig, err := loadRESTConfig(config.kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}
	sourceOptions := []rbg.Option{rbg.WithLogger(logger.With("source", "rbg"))}
	if config.labelFile != "" {
		sourceOptions = append(sourceOptions, rbg.WithLabelFile(config.labelFile))
		if config.labelPollEvery > 0 {
			sourceOptions = append(sourceOptions, rbg.WithPollInterval(config.labelPollEvery))
		}
	}
	source, err := rbg.New(client, config.riNamespace, config.riName, sourceOptions...)
	if err != nil {
		return nil, err
	}
	return source, nil
}

func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	return loader.ClientConfig()
}
