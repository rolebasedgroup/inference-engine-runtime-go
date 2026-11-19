package event_test

import (
	"strings"
	"testing"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
)

func TestLoadEventsValid(t *testing.T) {
	document := `
event_callbacks:
  start: []
  hook:
    - signal: "example.com/update-block=true"
      callback: prepare-update
    - signal: "example.com/empty="
      callback: prepare-update
  end:
    - cleanup
`
	events, err := event.LoadEvents(strings.NewReader(document))
	if err != nil {
		t.Fatalf("LoadEvents: %v", err)
	}
	if len(events.Hook) != 2 {
		t.Fatalf("hook bindings: %d", len(events.Hook))
	}
	key, value, err := events.Hook[1].SignalParts()
	if err != nil {
		t.Fatalf("SignalParts: %v", err)
	}
	if key != "example.com/empty" || value != "" {
		t.Fatalf("empty value signal parsed as key=%q value=%q", key, value)
	}
}

func TestLoadEventsEmptyListsValid(t *testing.T) {
	events, err := event.LoadEvents(strings.NewReader("event_callbacks:\n  start: []\n  hook: []\n  end: []\n"))
	if err != nil {
		t.Fatalf("empty lists must be valid: %v", err)
	}
	if len(events.Start) != 0 || len(events.Hook) != 0 || len(events.End) != 0 {
		t.Fatalf("unexpected non-empty lists: %+v", events)
	}
}

func TestParseSignal(t *testing.T) {
	if _, _, err := event.ParseSignal("novalue"); err == nil {
		t.Fatal("signal without = must be rejected")
	}
	if _, _, err := event.ParseSignal("=1"); err == nil {
		t.Fatal("empty key must be rejected")
	}
	key, value, err := event.ParseSignal("a=b=c")
	if err != nil {
		t.Fatal(err)
	}
	if key != "a" || value != "b=c" {
		t.Fatalf("split at first =: key=%q value=%q", key, value)
	}
}

func TestLoadEventsStrictRejections(t *testing.T) {
	tests := []struct {
		name     string
		document string
	}{
		{"unknown field", "event_callbacks:\n  start: []\n  unknown: true\n"},
		{"duplicate yaml key", "event_callbacks:\n  start: []\n  start: []\n"},
		{"duplicate signal", "event_callbacks:\n  hook:\n    - signal: \"a=1\"\n      callback: x\n    - signal: \"a=1\"\n      callback: y\n"},
		{"signal without separator", "event_callbacks:\n  hook:\n    - signal: \"a\"\n      callback: x\n"},
		{"empty signal key", "event_callbacks:\n  hook:\n    - signal: \"=1\"\n      callback: x\n"},
		{"empty callback name", "event_callbacks:\n  end:\n    - \"\"\n"},
		{"duplicate start entry", "event_callbacks:\n  start: [a, a]\n"},
		{"empty document", ""},
		{"multiple documents", "event_callbacks:\n  start: []\n---\nevent_callbacks:\n  start: []\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := event.LoadEvents(strings.NewReader(test.document)); err == nil {
				t.Fatal("invalid document must be rejected")
			}
		})
	}
}

func TestValidateRejectsGoConstructedEvents(t *testing.T) {
	events := event.Events{Hook: []event.HookBinding{{Signal: "bad", Callback: "x"}}}
	if err := event.Validate(events); err == nil {
		t.Fatal("invalid Go-constructed events must be rejected")
	}
}
