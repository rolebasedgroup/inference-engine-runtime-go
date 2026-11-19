package event

import (
	"fmt"
	"strings"
)

// Events describes when registered callbacks run. It mirrors the
// event_callbacks structure of events.yaml and can also be constructed
// directly in Go.
type Events struct {
	// Start lists callback names executed once, in order, after
	// initialization and before any hook processing.
	Start []string `yaml:"start"`
	// Hook binds exact signal key=value pairs to callback names. The
	// configured order selects which matching signal runs first.
	Hook []HookBinding `yaml:"hook"`
	// End lists callback names executed once, in order, during shutdown.
	End []string `yaml:"end"`
}

// HookBinding binds one exact signal to one registered callback.
type HookBinding struct {
	// Signal is an exact "key=value" string, split at the first "=".
	// The key must be non-empty; the value may be empty.
	Signal string `yaml:"signal"`
	// Callback is the registered callback name.
	Callback string `yaml:"callback"`
}

// SignalParts parses and returns the signal key and value.
func (b HookBinding) SignalParts() (key string, value string, err error) {
	return ParseSignal(b.Signal)
}

// ParseSignal splits a signal at the first "=". The key must be non-empty;
// the value may be empty.
func ParseSignal(signal string) (key, value string, err error) {
	index := strings.Index(signal, "=")
	if index < 0 {
		return "", "", fmt.Errorf("invalid signal %q: missing \"=\" separator", signal)
	}
	key, value = signal[:index], signal[index+1:]
	if key == "" {
		return "", "", fmt.Errorf("invalid signal %q: empty key", signal)
	}
	return key, value, nil
}

// cloneEvents returns a copy, so that loading never aliases the caller's
// Events value.
func cloneEvents(events Events) Events {
	return Events{
		Start: append([]string(nil), events.Start...),
		Hook:  append([]HookBinding(nil), events.Hook...),
		End:   append([]string(nil), events.End...),
	}
}

// Validate checks binding structure: non-empty callback names, no duplicate
// start/end entries, parseable signals, and no duplicate full signal
// bindings. It does not check callback existence; that requires a registry
// and is done by the runtime.
func Validate(events Events) error {
	for phase, names := range map[string][]string{"start": events.Start, "end": events.End} {
		seen := make(map[string]bool, len(names))
		for i, name := range names {
			if name == "" {
				return fmt.Errorf("%s callback %d: name must not be empty", phase, i)
			}
			if seen[name] {
				return fmt.Errorf("%s callbacks contain duplicate entry %q", phase, name)
			}
			seen[name] = true
		}
	}

	seenSignals := make(map[string]bool, len(events.Hook))
	for i, binding := range events.Hook {
		if binding.Callback == "" {
			return fmt.Errorf("hook binding %d: callback name must not be empty", i)
		}
		if _, _, err := ParseSignal(binding.Signal); err != nil {
			return fmt.Errorf("hook binding %d: %w", i, err)
		}
		if seenSignals[binding.Signal] {
			return fmt.Errorf("hook bindings contain duplicate signal %q", binding.Signal)
		}
		seenSignals[binding.Signal] = true
	}
	return nil
}
