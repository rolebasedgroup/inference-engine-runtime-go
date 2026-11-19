package diagnostics

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
)

// Server serves /healthz, /readyz, and /metrics. /healthz always reflects
// process liveness. /readyz becomes ready only after initialization, the
// first source synchronization, and start completion, and stops being ready
// once shutdown begins. Runtime readiness never claims engine readiness.
type Server struct {
	logger  *slog.Logger
	metrics *Metrics
	ready   atomic.Bool

	listener net.Listener
	server   *http.Server
}

// New creates a diagnostics server. An empty address disables the HTTP
// endpoints while keeping the observer usable.
func New(logger *slog.Logger, address string, metrics *Metrics) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	server := &Server{logger: logger, metrics: metrics}
	if address == "" {
		return server, nil
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("diagnostics: listen on %s: %w", address, err)
	}
	server.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if server.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))

	server.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server, nil
}

// Metrics returns the metrics observer.
func (s *Server) Metrics() *Metrics {
	return s.metrics
}

// Address returns the bound address, or an empty string when disabled.
func (s *Server) Address() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Start serves diagnostics endpoints until ctx is done. It returns nil when
// endpoints are disabled.
func (s *Server) Start(ctx context.Context) error {
	if s.server == nil {
		<-ctx.Done()
		return nil
	}
	errCh := make(chan error, 1)
	go func() {
		if err := s.server.Serve(s.listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	}
}

// SetRuntimeState implements runtime.Observer by forwarding metrics and
// deriving readiness: only the running state is ready.
func (s *Server) SetRuntimeState(state string) {
	s.metrics.SetRuntimeState(state)
	switch state {
	case runtime.StateRunning:
		s.ready.Store(true)
	default:
		s.ready.Store(false)
	}
}

// CallbackInvoked implements runtime.Observer.
func (s *Server) CallbackInvoked(name string, result string, duration time.Duration) {
	s.metrics.CallbackInvoked(name, result, duration)
}

// CallbackRetried implements runtime.Observer.
func (s *Server) CallbackRetried(name string) {
	s.metrics.CallbackRetried(name)
}

// AckCompleted implements runtime.Observer.
func (s *Server) AckCompleted(result string) {
	s.metrics.AckCompleted(result)
}

// SourceError implements runtime.Observer.
func (s *Server) SourceError() {
	s.metrics.SourceError()
}

// SetPendingSignals implements runtime.Observer.
func (s *Server) SetPendingSignals(count int) {
	s.metrics.SetPendingSignals(count)
}

var _ runtime.Observer = (*Server)(nil)
