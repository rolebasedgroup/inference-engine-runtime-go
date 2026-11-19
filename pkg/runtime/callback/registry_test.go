package callback_test

import (
	"context"
	"testing"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
)

func TestNewRegistryIsEmpty(t *testing.T) {
	registry := callback.NewRegistry()
	if names := registry.Names(); len(names) != 0 {
		t.Fatalf("empty registry has callbacks: %v", names)
	}
	if _, ok := registry.Lookup("noop"); ok {
		t.Fatal("empty registry must not contain the noop callback")
	}
}

func TestRegistryRegisterValidation(t *testing.T) {
	registry := callback.NewRegistry()
	action := func(context.Context) error { return nil }

	if err := registry.Register("", action); err == nil {
		t.Fatal("empty name must be rejected")
	}
	if err := registry.Register("name", nil); err == nil {
		t.Fatal("nil callback must be rejected")
	}
	if err := registry.Register("name", action); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
	if err := registry.Register("name", action); err == nil {
		t.Fatal("duplicate name must be rejected")
	}
}

func TestRegistryConcurrentRegisterAndLookup(t *testing.T) {
	registry := callback.NewRegistry()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_, _ = registry.Lookup("name")
			_ = registry.Names()
		}
	}()
	for i := 0; i < 100; i++ {
		if err := registry.Register("name", func(context.Context) error { return nil }); err != nil {
			// Only the first registration succeeds.
			_ = err
		}
	}
	<-done
}
