//go:build integration

// Package integration verifies the experimental RBG source against a real
// API server using envtest and the test CRD fixture. Run with:
//
//	go test -tags integration ./test/integration/...
//
// The test is skipped when envtest binaries are not available
// (KUBEBUILDER_ASSETS is unset and setup-envtest has not been used).
package integration

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/source/rbg"
)

var riGVK = schema.GroupVersionKind{Group: rbg.Group, Version: rbg.Version, Kind: "RoleInstance"}

func TestRBGSourceLifecycle(t *testing.T) {
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("fixtures")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	if err != nil {
		t.Skipf("envtest unavailable (skipping): %v", err)
	}
	defer func() {
		_ = environment.Stop()
	}()

	client, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "apps"}}
	if _, err := kubeClient.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	ri := newRI("apps", "ri-integration", map[string]string{
		rbg.HookStateLabel:     rbg.HookStatePreUpdate,
		"example.com/update":   "true",
		"example.com/consumer": "true",
	})
	setLifecycle(ri, "preUpdate", map[string]string{"example.com/update": "true"})
	created, err := client.Resource(rbg.RoleInstanceGVR).Namespace("apps").Create(ctx, ri, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create RoleInstance: %v", err)
	}

	source, err := rbg.New(client, "apps", "ri-integration", rbg.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}

	notify := make(chan struct{}, 1)
	sourceDone := make(chan error, 1)
	go func() { sourceDone <- source.Run(ctx, notify) }()

	// Snapshot reads the watched object; retry until the informer has
	// synced, exactly like the runtime's initial synchronization.
	var snapshot runtime.SourceSnapshot
	for deadline := time.Now().Add(10 * time.Second); ; {
		snapshot, err = source.Snapshot(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !snapshot.Has("example.com/update", "true") {
		t.Fatalf("snapshot signals = %v", snapshot.Signals)
	}
	if snapshot.Has("example.com/consumer", "true") {
		t.Fatal("undeclared label must not become a signal")
	}

	// Acknowledge: only the executed label is removed.
	status, err := source.Ack(ctx, snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if status != runtime.Acknowledged {
		t.Fatalf("ack status = %v", status)
	}
	fresh, err := client.Resource(rbg.RoleInstanceGVR).Namespace("apps").Get(ctx, "ri-integration", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := fresh.GetLabels()["example.com/update"]; present {
		t.Fatal("executed signal label must be removed")
	}
	if fresh.GetLabels()["example.com/consumer"] != "true" {
		t.Fatal("other consumer's signal must be preserved")
	}
	if fresh.GetLabels()[rbg.HookStateLabel] != rbg.HookStatePreUpdate {
		t.Fatal("ack must not change hook-state")
	}
	_ = created

	// Advance the controller state: further acks against the old snapshot
	// must be stale, and the new state is valid but not executable.
	staleRI := fresh.DeepCopy()
	staleRI.SetLabels(withLabel(staleRI, rbg.HookStateLabel, rbg.HookStateUpdating))
	if _, err := client.Resource(rbg.RoleInstanceGVR).Namespace("apps").Update(ctx, staleRI, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("advance hook-state: %v", err)
	}
	status, err = source.Ack(ctx, snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	if status != runtime.Stale {
		t.Fatalf("stale ack status = %v", status)
	}

	snapshot, err = source.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after update: %v", err)
	}
	if len(snapshot.Signals) != 0 {
		t.Fatalf("Updating state must not be executable: %v", snapshot.Signals)
	}

	cancel()
	if err := <-sourceDone; err != nil {
		t.Fatalf("source Run: %v", err)
	}
}

func newRI(namespace, name string, labels map[string]string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(riGVK)
	object.SetNamespace(namespace)
	object.SetName(name)
	object.SetLabels(labels)
	return object
}

func setLifecycle(object *unstructured.Unstructured, hook string, signalLabels map[string]string) {
	value := map[string]any{}
	for key, signalValue := range signalLabels {
		value[key] = signalValue
	}
	_ = unstructured.SetNestedMap(object.Object, map[string]any{
		hook: map[string]any{"signalLabels": value},
	}, "spec", "lifecycle")
}

func withLabel(object *unstructured.Unstructured, key, value string) map[string]string {
	labels := map[string]string{}
	for existingKey, existingValue := range object.GetLabels() {
		labels[existingKey] = existingValue
	}
	labels[key] = value
	return labels
}
