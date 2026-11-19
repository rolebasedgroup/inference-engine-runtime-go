// Package noop is the first built-in plugin of the runtime: a callback that
// does nothing. Plugins are compiled into the binary and registered
// explicitly; this package does not use Go's runtime plugin loading.
//
// noop exists only for explicit wiring and smoke tests. It must never be
// bound implicitly to production signals.
package noop

import (
	"context"
	"errors"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
)

// Callback completes immediately without doing anything.
func Callback(context.Context) error { return nil }

// AddToRegistry registers the noop callback in registry, following the same
// convention user callback packages may adopt. A name conflict is rejected
// without partial registration; there is no override mechanism.
func AddToRegistry(registry *callback.Registry) error {
	if registry == nil {
		return errors.New("noop: AddToRegistry called with a nil Registry")
	}
	return registry.Register("noop", Callback)
}
