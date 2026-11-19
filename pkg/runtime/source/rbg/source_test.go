package rbg

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
)

var riGVK = schema.GroupVersionKind{Group: Group, Version: Version, Kind: "RoleInstance"}

func roleInstance(namespace, name string, uid string, labels map[string]string, lifecycle map[string]any) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(riGVK)
	object.SetNamespace(namespace)
	object.SetName(name)
	object.SetUID(types.UID(uid))
	object.SetResourceVersion("10")
	object.SetLabels(labels)
	if lifecycle != nil {
		_ = unstructured.SetNestedMap(object.Object, lifecycle, "spec", "lifecycle")
	}
	return object
}

func hookConfig(signalLabels map[string]string) map[string]any {
	return map[string]any{
		"preUpdate": map[string]any{"signalLabels": toAnyMap(signalLabels)},
		"postReady": map[string]any{"signalLabels": map[string]any{"other.example/ready": "true"}},
	}
}

func toAnyMap(input map[string]string) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func newTestClient(t *testing.T, objects ...k8sruntime.Object) *fake.FakeDynamicClient {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		RoleInstanceGVR: "RoleInstanceList",
	}, objects...)
	return client
}

// startSource runs the source in API mode and waits until observation has
// synced, so Snapshot reads the informer store.
func startSource(t *testing.T, source *Source) {
	t.Helper()
	notify := make(chan struct{}, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- source.Run(ctx, notify) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("source Run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("source Run did not stop")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := source.Snapshot(context.Background())
		// Fail-closed protocol errors are permanent; the subtest asserts
		// them. Only the not-yet-synced state is worth waiting for.
		if err == nil || !errors.Is(err, errNotSynced) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("source observation did not sync: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForSnapshot(t *testing.T, source *Source, condition func(runtime.SourceSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := source.Snapshot(context.Background())
		if err == nil && condition(snapshot) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot condition not met: err=%v snapshot=%+v", err, snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSnapshotMatching(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	tests := []struct {
		description string
		labels      map[string]string
		lifecycle   map[string]any
		wantSignals []runtime.Signal
		wantErr     string
	}{
		{
			description: "executable signal with matching label",
			labels: map[string]string{
				HookStateLabel:       HookStatePreUpdate,
				"example.com/update": "true",
				"example.com/other":  "false",
			},
			lifecycle:   hookConfig(map[string]string{"example.com/update": "true"}),
			wantSignals: []runtime.Signal{{Key: "example.com/update", Value: "true"}},
		},
		{
			description: "missing label does not match",
			labels:      map[string]string{HookStateLabel: HookStatePreUpdate},
			lifecycle:   hookConfig(map[string]string{"example.com/update": "true"}),
		},
		{
			description: "different label value does not match",
			labels: map[string]string{
				HookStateLabel:       HookStatePreUpdate,
				"example.com/update": "false",
			},
			lifecycle: hookConfig(map[string]string{"example.com/update": "true"}),
		},
		{
			description: "empty declared value matches present empty label but not missing label",
			labels: map[string]string{
				HookStateLabel:       HookStatePreUpdate,
				"example.com/update": "",
			},
			lifecycle:   hookConfig(map[string]string{"example.com/update": ""}),
			wantSignals: []runtime.Signal{{Key: "example.com/update", Value: ""}},
		},
		{
			description: "no hook state means no executable signal",
			labels:      map[string]string{"example.com/update": "true"},
			lifecycle:   hookConfig(map[string]string{"example.com/update": "true"}),
		},
		{
			description: "in-progress state is valid but not executable",
			labels: map[string]string{
				HookStateLabel:       HookStateUpdating,
				"example.com/update": "true",
			},
			lifecycle: hookConfig(map[string]string{"example.com/update": "true"}),
		},
		{
			description: "unknown hook state fails closed",
			labels: map[string]string{
				HookStateLabel:       "PreUp",
				"example.com/update": "true",
			},
			lifecycle: hookConfig(map[string]string{"example.com/update": "true"}),
			wantErr:   "unknown hook-state",
		},
		{
			description: "duplicate signal key across hooks fails closed",
			labels: map[string]string{
				HookStateLabel:       HookStatePreUpdate,
				"example.com/update": "true",
			},
			lifecycle: map[string]any{
				"preUpdate": map[string]any{"signalLabels": map[string]any{"example.com/update": "true"}},
				"preDelete": map[string]any{"signalLabels": map[string]any{"example.com/update": "true"}},
			},
			wantErr: "declared by both",
		},
		{
			description: "unknown lifecycle field fails closed",
			labels: map[string]string{
				HookStateLabel:       HookStatePreUpdate,
				"example.com/update": "true",
			},
			lifecycle: map[string]any{
				"preUpdate": map[string]any{"signalLabels": map[string]any{"example.com/update": "true"}, "extra": "x"},
			},
			wantErr: "invalid spec.lifecycle",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			client := newTestClient(t, roleInstance(ns, name, uid, test.labels, test.lifecycle))
			source, err := New(client, ns, name, nil)
			if err != nil {
				t.Fatal(err)
			}
			startSource(t, source)
			snapshot, err := source.Snapshot(context.Background())
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if len(snapshot.Signals) != len(test.wantSignals) {
				t.Fatalf("signals = %v, want %v", snapshot.Signals, test.wantSignals)
			}
			for i, want := range test.wantSignals {
				if snapshot.Signals[i] != want {
					t.Fatalf("signals = %v, want %v", snapshot.Signals, test.wantSignals)
				}
			}
			if len(test.wantSignals) > 0 && !snapshot.Has(test.wantSignals[0].Key, test.wantSignals[0].Value) {
				t.Fatalf("snapshot does not contain expected signal")
			}
		})
	}
}

func TestAckRemovesOnlyOwnLabel(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:         HookStatePreUpdate,
		"example.com/update":   "true",
		"example.com/consumer": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)
	source, err := New(client, ns, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	startSource(t, source)

	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if status != runtime.Acknowledged {
		t.Fatalf("status = %v, want acknowledged", status)
	}

	updated, err := client.Resource(RoleInstanceGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	labels := updated.GetLabels()
	if _, present := labels["example.com/update"]; present {
		t.Fatal("acknowledged signal label must be removed")
	}
	if labels["example.com/consumer"] != "true" {
		t.Fatal("other consumers' signals must be preserved")
	}
	if labels[HookStateLabel] != HookStatePreUpdate {
		t.Fatal("ack must not modify hook-state")
	}
}

func TestAckStaleRejections(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	baseLabels := func() map[string]string {
		return map[string]string{
			HookStateLabel:       HookStatePreUpdate,
			"example.com/update": "true",
		}
	}
	baseLifecycle := hookConfig(map[string]string{"example.com/update": "true"})

	tests := []struct {
		description string
		mutate      func(object *unstructured.Unstructured)
	}{
		{
			description: "uid replacement",
			mutate:      func(object *unstructured.Unstructured) { object.SetUID("uid-2") },
		},
		{
			description: "hook state advanced",
			mutate: func(object *unstructured.Unstructured) {
				labels := withLabels(object, HookStateLabel, HookStateUpdating)
				object.SetLabels(labels)
			},
		},
		{
			description: "hook state removed",
			mutate: func(object *unstructured.Unstructured) {
				labels := withoutLabels(object, HookStateLabel)
				object.SetLabels(labels)
			},
		},
		{
			description: "signal label already removed",
			mutate: func(object *unstructured.Unstructured) {
				object.SetLabels(withoutLabels(object, "example.com/update"))
			},
		},
		{
			description: "signal label value changed",
			mutate: func(object *unstructured.Unstructured) {
				object.SetLabels(withLabels(object, "example.com/update", "false"))
			},
		},
		{
			description: "declaration removed",
			mutate: func(object *unstructured.Unstructured) {
				lifecycle := hookConfig(nil)
				delete(lifecycle, "preUpdate")
				_ = unstructured.SetNestedMap(object.Object, lifecycle, "spec", "lifecycle")
			},
		},
		{
			description: "declaration value changed",
			mutate: func(object *unstructured.Unstructured) {
				lifecycle := hookConfig(map[string]string{"example.com/update": "changed"})
				_ = unstructured.SetNestedMap(object.Object, lifecycle, "spec", "lifecycle")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			object := roleInstance(ns, name, uid, baseLabels(), baseLifecycle)
			client := newTestClient(t, object)
			source, err := New(client, ns, name, nil)
			if err != nil {
				t.Fatal(err)
			}
			startSource(t, source)
			snapshot, err := source.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			test.mutate(object)
			// Re-registering the mutated object changes the stored state.
			if err := client.Tracker().Update(RoleInstanceGVR, object, ns); err != nil {
				t.Fatalf("update tracker: %v", err)
			}
			_, stillHadLabel := object.GetLabels()["example.com/update"]

			status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
			if err != nil {
				t.Fatalf("Ack: %v", err)
			}
			if status != runtime.Stale {
				t.Fatalf("status = %v, want stale", status)
			}
			updated, err := client.Resource(RoleInstanceGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, present := updated.GetLabels()["example.com/update"]; !present && stillHadLabel {
				t.Fatal("stale acknowledgement must not remove any label")
			}
		})
	}

	_ = baseLabels
}

func withLabels(object *unstructured.Unstructured, key, value string) map[string]string {
	labels := map[string]string{}
	for existingKey, existingValue := range object.GetLabels() {
		labels[existingKey] = existingValue
	}
	labels[key] = value
	return labels
}

func withoutLabels(object *unstructured.Unstructured, keys ...string) map[string]string {
	labels := map[string]string{}
	for existingKey, existingValue := range object.GetLabels() {
		labels[existingKey] = existingValue
	}
	for _, key := range keys {
		delete(labels, key)
	}
	return labels
}

func TestNewValidatesTarget(t *testing.T) {
	if _, err := New(nil, "ns", "name", nil); err == nil {
		t.Fatal("nil client must be rejected")
	}
	if _, err := New(newTestClient(t), "", "name", nil); err == nil {
		t.Fatal("empty namespace must be rejected")
	}
	if _, err := New(newTestClient(t), "ns", "", nil); err == nil {
		t.Fatal("empty name must be rejected")
	}
	if _, err := New(newTestClient(t), "ns", "name", WithLabelFile("/tmp/labels"), WithPollInterval(0)); err == nil {
		t.Fatal("non-positive poll interval must be rejected with a label file")
	}
}

func TestRunLabelFileWakesUpPeriodically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "labels")
	client := newTestClient(t)
	source, err := New(client, "ns", "ri",
		WithLogger(slog.New(slog.DiscardHandler)),
		WithLabelFile(path),
		WithPollInterval(2*time.Millisecond),
		WithReplaySuppressTTL(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	notify := make(chan struct{}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- source.Run(ctx, notify) }()
	defer cancel()

	// Every polling tick is a wake-up: the snapshot itself reads the file,
	// so change detection is unnecessary.
	expectNotify(t, notify)
	expectNotify(t, notify)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func writeLabelFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write label file: %v", err)
	}
}

func expectNotify(t *testing.T, notify <-chan struct{}) {
	t.Helper()
	select {
	case <-notify:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a wake-up notification")
	}
}

func TestParseDownwardAPILabels(t *testing.T) {
	labels, malformed := parseDownwardAPILabels(
		"example.com/update=\"true\"\n" +
			"example.com/empty=\"\"\n" +
			"example.com/plain=true\n" +
			"\n" +
			"malformed-line\n")
	if len(malformed) != 1 || malformed[0] != "malformed-line" {
		t.Fatalf("malformed lines = %v", malformed)
	}
	want := map[string]string{
		"example.com/update": "true",
		"example.com/empty":  "",
		"example.com/plain":  "true",
	}
	if len(labels) != len(want) {
		t.Fatalf("labels = %#v", labels)
	}
	for key, value := range want {
		if labels[key] != value {
			t.Fatalf("labels[%q] = %q, want %q", key, labels[key], value)
		}
	}
}

func TestFileSnapshotAndReplaySuppression(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:         HookStatePreUpdate,
		"example.com/update":   "true",
		"example.com/consumer": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)

	path := filepath.Join(t.TempDir(), "labels")
	writeLabelFile(t, path, "example.com/update=\"true\"\nexample.com/consumer=\"true\"\n")
	source, err := New(client, ns, name,
		WithLogger(slog.New(slog.DiscardHandler)),
		WithLabelFile(path),
		WithReplaySuppressTTL(60*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snapshot.Has("example.com/update", "true") {
		t.Fatalf("file snapshot signals = %v", snapshot.Signals)
	}
	// Labels that are not bound are visible to the runtime but ignored by
	// its binding filter; the source cannot know the bindings.
	if !snapshot.Has("example.com/consumer", "true") {
		t.Fatal("file snapshot must expose every pod label")
	}

	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if status != runtime.Acknowledged {
		t.Fatalf("status = %v", err)
	}

	// The label is gone from the RoleInstance but still in the file; the
	// replay cache must hide it while the mirror propagates.
	after, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Has("example.com/update", "true") {
		t.Fatal("recently acknowledged signal must be suppressed")
	}
	if !after.Has("example.com/consumer", "true") {
		t.Fatal("suppression must be per signal")
	}

	updated, err := client.Resource(RoleInstanceGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := updated.GetLabels()["example.com/update"]; present {
		t.Fatal("acknowledged label must be removed from the RoleInstance")
	}
}

func TestFileSnapshotStaleAckSuppressesReplay(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:       HookStatePreUpdate,
		"example.com/update": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)

	path := filepath.Join(t.TempDir(), "labels")
	writeLabelFile(t, path, "example.com/update=\"true\"\n")
	source, err := New(client, ns, name,
		WithLogger(slog.New(slog.DiscardHandler)),
		WithLabelFile(path),
		WithReplaySuppressTTL(60*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// The controller removed the label (for example a config change)
	// before acknowledgement; the file still shows it.
	object.SetLabels(withoutLabels(object, "example.com/update"))
	if err := client.Tracker().Update(RoleInstanceGVR, object, ns); err != nil {
		t.Fatalf("update tracker: %v", err)
	}

	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if status != runtime.Stale {
		t.Fatalf("status = %v, want stale", status)
	}

	after, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Has("example.com/update", "true") {
		t.Fatal("stale signal must be suppressed while the file lags")
	}
}

func TestFileSnapshotMissingFileAndTTLExpiry(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:       HookStatePreUpdate,
		"example.com/update": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)

	path := filepath.Join(t.TempDir(), "labels")
	source, err := New(client, ns, name,
		WithLogger(slog.New(slog.DiscardHandler)),
		WithLabelFile(path),
		WithReplaySuppressTTL(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Missing file is an empty, valid observation.
	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snapshot.Signals) != 0 {
		t.Fatalf("missing file must yield no signals: %v", snapshot.Signals)
	}

	// Suppression expires with the TTL and the signal becomes visible
	// again.
	writeLabelFile(t, path, "example.com/update=\"true\"\n")
	snapshot, err = source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil || status != runtime.Acknowledged {
		t.Fatalf("ack = %v, %v", status, err)
	}
	if snapshot, err = source.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snapshot.Has("example.com/update", "true") {
		t.Fatal("signal must be suppressed immediately after acknowledgement")
	}
	time.Sleep(15 * time.Millisecond)
	if snapshot, err = source.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Has("example.com/update", "true") {
		t.Fatal("signal must be visible again after the TTL expires")
	}
}

func TestFileSnapshotValidation(t *testing.T) {
	if _, err := New(newTestClient(t), "ns", "name", WithReplaySuppressTTL(0)); err == nil {
		t.Fatal("non-positive suppression TTL must be rejected")
	}
}

func TestAckRetriesMergePatchConflict(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:       HookStatePreUpdate,
		"example.com/update": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)
	source, err := New(client, ns, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	startSource(t, source)
	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	conflicts := 0
	client.PrependReactor("patch", Resource, func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		if conflicts == 0 {
			conflicts++
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: Group, Resource: Resource}, name,
				errors.New("stale resourceVersion"))
		}
		return false, nil, nil
	})

	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if status != runtime.Acknowledged {
		t.Fatalf("status = %v, want acknowledged", status)
	}
	if conflicts != 1 {
		t.Fatalf("conflict reactor invocations = %d, want 1", conflicts)
	}
	updated, err := client.Resource(RoleInstanceGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := updated.GetLabels()["example.com/update"]; present {
		t.Fatal("signal label must be removed after conflict retry")
	}
}

func TestAPISnapshotFollowsWatchAndDeletion(t *testing.T) {
	const ns, name, uid = "apps", "ri-0", "uid-1"
	object := roleInstance(ns, name, uid, map[string]string{
		HookStateLabel:       HookStatePreUpdate,
		"example.com/update": "true",
	}, hookConfig(map[string]string{"example.com/update": "true"}))
	client := newTestClient(t, object)
	source, err := New(client, ns, name,
		WithLogger(slog.New(slog.DiscardHandler)),
		WithReplaySuppressTTL(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	startSource(t, source)

	// The snapshot comes from the watched object: no GET is issued.
	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Has("example.com/update", "true") {
		t.Fatalf("signals = %v", snapshot.Signals)
	}

	// Controller-side label changes are observed through the watch.
	object.SetLabels(withoutLabels(object, "example.com/update"))
	if err := client.Tracker().Update(RoleInstanceGVR, object, ns); err != nil {
		t.Fatalf("update tracker: %v", err)
	}
	waitForSnapshot(t, source, func(s runtime.SourceSnapshot) bool {
		return !s.Has("example.com/update", "true")
	})

	// Deleting the RoleInstance makes the observation empty, and an
	// acknowledgement against the old observation is stale, not a
	// retryable error.
	if err := client.Tracker().Delete(RoleInstanceGVR, ns, name); err != nil {
		t.Fatalf("delete tracker: %v", err)
	}
	waitForSnapshot(t, source, func(s runtime.SourceSnapshot) bool {
		return len(s.Signals) == 0
	})

	status, err := source.Ack(context.Background(), snapshot, runtime.Signal{Key: "example.com/update", Value: "true"})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if status != runtime.Stale {
		t.Fatalf("status = %v, want stale after deletion", status)
	}
}
