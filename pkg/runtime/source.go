package runtime

import "context"

// Signal is one executable signal observation, expressed as an exact
// key/value pair.
type Signal struct {
	Key   string
	Value string
}

// SourceSnapshot is the result of one source observation. Signals lists the
// currently executable signals. Token is the source-provided observation
// credential for this snapshot; the core never inspects it and only returns
// it to the source when acknowledging a completed signal.
type SourceSnapshot struct {
	Signals []Signal
	Token   any
}

// Has reports whether the snapshot contains the exact signal key/value.
func (s SourceSnapshot) Has(key, value string) bool {
	for _, signal := range s.Signals {
		if signal.Key == key && signal.Value == value {
			return true
		}
	}
	return false
}

// AckStatus classifies an Ack result.
type AckStatus int

const (
	// Acknowledged means the source recorded completion for the signal.
	Acknowledged AckStatus = iota + 1
	// Stale means the original observation is no longer valid; the runtime
	// must drop its in-memory completion record and must not confirm the
	// newer signal.
	Stale
)

// String returns a stable, low-cardinality label for metrics.
func (s AckStatus) String() string {
	switch s {
	case Acknowledged:
		return "acknowledged"
	case Stale:
		return "stale"
	default:
		return "unknown"
	}
}

// SignalSource adapts one observation protocol to the runtime. It is a
// protocol adapter, not business execution: the source validates its own
// protocol, provides the execution basis, and writes completion back
// safely. A Runtime uses at most one source.
type SignalSource interface {
	// Run observes until ctx is done. Change notifications are sent to
	// notify and may be coalesced; sending must never block the source.
	// Run returns nil when ctx is canceled.
	Run(ctx context.Context, notify chan<- struct{}) error
	// Snapshot returns the currently executable signals together with an
	// observation credential for this call. It must fail rather than
	// return an unsafe best-effort snapshot.
	Snapshot(ctx context.Context) (SourceSnapshot, error)
	// Ack confirms that the callback for signal, originally observed in
	// snapshot, completed. It distinguishes Acknowledged, Stale, and (via
	// error) temporary failure. It must never write based on an outdated
	// observation.
	Ack(ctx context.Context, snapshot SourceSnapshot, signal Signal) (AckStatus, error)
}
