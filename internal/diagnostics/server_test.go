package diagnostics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
)

func TestServerHealthAndReadiness(t *testing.T) {
	server, err := New(nil, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()

	base := "http://" + server.Address()

	get := func(path string) (int, string) {
		response, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}

	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz code = %d", code)
	}
	if code, _ := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before running = %d", code)
	}

	server.SetRuntimeState(runtime.StateSyncing)
	if code, _ := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while syncing = %d", code)
	}
	server.SetRuntimeState(runtime.StateRunning)
	if code, body := get("/readyz"); code != http.StatusOK || !strings.Contains(body, "ready") {
		t.Fatalf("/readyz while running = %d %q", code, body)
	}
	server.SetRuntimeState(runtime.StateShuttingDown)
	if code, _ := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while shutting down = %d", code)
	}
	server.CallbackInvoked("noop", runtime.ResultSuccess, time.Millisecond)

	code, body := get("/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics code = %d", code)
	}
	if !strings.Contains(body, "patio_callbacks_invoked_total") {
		t.Fatalf("/metrics is missing patio metrics")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestObserverRecordsMetrics(t *testing.T) {
	server, err := New(nil, "", NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	server.CallbackInvoked("noop", runtime.ResultSuccess, 10*time.Millisecond)
	server.CallbackRetried("noop")
	server.AckCompleted(runtime.Acknowledged.String())
	server.SourceError()
	server.SetPendingSignals(2)
}
