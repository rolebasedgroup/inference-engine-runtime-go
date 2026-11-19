package event

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// eventsFile is the top-level events.yaml document. Only the
// event_callbacks key is accepted.
type eventsFile struct {
	EventCallbacks Events `yaml:"event_callbacks"`
}

// LoadEvents reads and strictly parses an events.yaml document from reader.
// Unknown fields, duplicate YAML keys, multiple documents, malformed
// signals, and duplicate bindings are rejected before any callback runs.
// Empty or missing start/hook/end lists are valid.
func LoadEvents(reader io.Reader) (Events, error) {
	if reader == nil {
		return Events{}, errors.New("event: LoadEvents called with a nil reader")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return Events{}, fmt.Errorf("event: read events: %w", err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var file eventsFile
	if err := decoder.Decode(&file); err != nil {
		if errors.Is(err, io.EOF) {
			return Events{}, errors.New("event: events document is empty")
		}
		return Events{}, fmt.Errorf("event: parse events: %w", err)
	}
	if err := decoder.Decode(&yaml.Node{}); !errors.Is(err, io.EOF) {
		return Events{}, errors.New("event: events document must contain exactly one YAML document")
	}
	events := cloneEvents(file.EventCallbacks)
	if err := Validate(events); err != nil {
		return Events{}, fmt.Errorf("event: invalid events: %w", err)
	}
	return events, nil
}
