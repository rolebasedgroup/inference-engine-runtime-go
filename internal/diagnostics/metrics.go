// Package diagnostics provides structured logging, Prometheus metrics, and
// health endpoints for the standard patio process. It implements the patio
// Observer interface; embedding programs may supply their own.
package diagnostics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
)

// Metrics implements runtime.Observer with Prometheus metrics. Labels are
// restricted to callback names and fixed result values; signal values and
// RoleInstance UIDs are deliberately never used as labels.
type Metrics struct {
	registry *prometheus.Registry

	callbacksInvoked *prometheus.CounterVec
	callbackDuration *prometheus.HistogramVec
	callbackRetried  *prometheus.CounterVec
	acks             *prometheus.CounterVec
	sourceErrors     prometheus.Counter
	pendingSignals   prometheus.Gauge
}

// NewMetrics creates a private Prometheus registry with patio metrics.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		callbacksInvoked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "patio_callbacks_invoked_total",
			Help: "Callback invocations by callback name and result.",
		}, []string{"callback", "result"}),
		callbackDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "patio_callback_duration_seconds",
			Help:    "Callback execution duration by callback name.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"callback"}),
		callbackRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "patio_callback_retries_total",
			Help: "Scheduled callback retries by callback name.",
		}, []string{"callback"}),
		acks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "patio_ack_total",
			Help: "Signal acknowledgement attempts by result.",
		}, []string{"result"}),
		sourceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "patio_source_errors_total",
			Help: "Signal source observation errors.",
		}),
		pendingSignals: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "patio_pending_signals",
			Help: "Signals present in the latest source snapshot.",
		}),
	}
	registry.MustRegister(
		metrics.callbacksInvoked,
		metrics.callbackDuration,
		metrics.callbackRetried,
		metrics.acks,
		metrics.sourceErrors,
		metrics.pendingSignals,
	)
	return metrics
}

// Registry returns the private Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// CallbackInvoked implements runtime.Observer.
func (m *Metrics) CallbackInvoked(name string, result string, duration time.Duration) {
	m.callbacksInvoked.WithLabelValues(name, result).Inc()
	m.callbackDuration.WithLabelValues(name).Observe(duration.Seconds())
}

// CallbackRetried implements runtime.Observer.
func (m *Metrics) CallbackRetried(name string) {
	m.callbackRetried.WithLabelValues(name).Inc()
}

// AckCompleted implements runtime.Observer.
func (m *Metrics) AckCompleted(result string) {
	m.acks.WithLabelValues(result).Inc()
}

// SourceError implements runtime.Observer.
func (m *Metrics) SourceError() {
	m.sourceErrors.Inc()
}

// SetPendingSignals implements runtime.Observer.
func (m *Metrics) SetPendingSignals(count int) {
	m.pendingSignals.Set(float64(count))
}

// SetRuntimeState implements runtime.Observer.
func (m *Metrics) SetRuntimeState(state string) {
	// Readiness is derived from the state; no state metric is exported to
	// avoid duplicating labels with cardinality concerns.
}

var _ runtime.Observer = (*Metrics)(nil)
