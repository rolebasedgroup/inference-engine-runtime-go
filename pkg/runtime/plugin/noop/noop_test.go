package noop_test

import (
	"context"
	"testing"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/callback"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/plugin/noop"
)

func TestAddToRegistryRegistersNoop(t *testing.T) {
	registry := callback.NewRegistry()
	if err := noop.AddToRegistry(registry); err != nil {
		t.Fatalf("AddToRegistry: %v", err)
	}
	registered, ok := registry.Lookup("noop")
	if !ok || registered == nil {
		t.Fatal("noop must be registered under its own name")
	}
	if err := registered(context.Background()); err != nil {
		t.Fatalf("noop callback returned error: %v", err)
	}
}

func TestAddToRegistryConflictIsAtomic(t *testing.T) {
	registry := callback.NewRegistry()
	conflict := func(context.Context) error { return nil }
	if err := registry.Register("noop", conflict); err != nil {
		t.Fatalf("seed conflict: %v", err)
	}
	if err := noop.AddToRegistry(registry); err == nil {
		t.Fatal("conflicting built-in name must be rejected")
	}
	if callback, ok := registry.Lookup("noop"); !ok || callback == nil {
		t.Fatal("conflict must not remove the existing callback")
	}
}

func TestAddToRegistryNilRegistry(t *testing.T) {
	if err := noop.AddToRegistry(nil); err == nil {
		t.Fatal("nil registry must be rejected")
	}
}
