package callback

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Registry is an instance-level, mutable collection of named callbacks.
// Registries are not global: every embedding program creates and populates
// its own registry. The registry is frozen only indirectly: the runtime
// copies the callbacks it resolves at construction time, so later
// registrations cannot affect a running Runtime.
type Registry struct {
	mu        sync.RWMutex
	callbacks map[string]Callback
}

// NewRegistry returns an empty registry for fully custom action sets.
func NewRegistry() *Registry {
	return &Registry{callbacks: make(map[string]Callback)}
}

// Register adds callback under name. Empty names, nil callbacks, and
// duplicate names are rejected; registration never silently overwrites an
// existing entry.
func (r *Registry) Register(name string, callback Callback) error {
	if r == nil {
		return errors.New("callback: Register called on a nil Registry")
	}
	if name == "" {
		return errors.New("callback: callback name must not be empty")
	}
	if callback == nil {
		return fmt.Errorf("callback: callback %q must not be nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.callbacks[name]; exists {
		return fmt.Errorf("callback: callback %q is already registered", name)
	}
	r.callbacks[name] = callback
	return nil
}

// Lookup returns the callback registered under name.
func (r *Registry) Lookup(name string) (Callback, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	callback, ok := r.callbacks[name]
	return callback, ok
}

// Names returns the registered callback names in sorted order.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.callbacks))
	for name := range r.callbacks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
