// Package callback defines the action model of the runtime: a Callback is a
// plain Go function, and a Registry maps names to callbacks. Business
// actions are always code compiled into the binary; configuration only
// selects registered names.
package callback

import "context"

// Callback is a single application action executed by the runtime.
//
// Implementations must be idempotent and must honor context cancellation.
// Returning nil means the action is complete; returning an error means it is
// not complete and may be retried. A panic is converted into a failure.
//
// Dependencies are captured by the embedding program via closures or method
// receivers before registration; the framework does not provide dependency
// injection and never passes signal or RoleInstance state to callbacks.
type Callback func(ctx context.Context) error
