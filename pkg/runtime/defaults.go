package runtime

import (
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/plugin/noop"
)

// NewDefaultRegistry returns a new registry preloaded with the standard
// built-in plugin callbacks (currently noop). Every call returns an
// independent registry; there is no process-level mutable default registry,
// and two default registries never share state. "Default registration" does
// not mean "default binding": only event bindings that explicitly reference
// a name execute.
func NewDefaultRegistry() *callback.Registry {
	registry := callback.NewRegistry()
	// AddToRegistry only fails on a fresh registry if the built-in set is
	// internally inconsistent, which is a programming error.
	if err := noop.AddToRegistry(registry); err != nil {
		panic("runtime: inconsistent built-in callback set: " + err.Error())
	}
	return registry
}
