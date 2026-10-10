package frontend

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	nexuspb "go.temporal.io/api/nexus/v1"
)

// maxNexusProgressMetadataBytes bounds the metadata of one progress delivery, so progress stays a
// notification and the data travels on the read path.
const maxNexusProgressMetadataBytes = 2 * 1024

// maxNexusProgressPositionBytes bounds the position, which History copies onto the caller's
// Workflow Task scheduled event.
const maxNexusProgressPositionBytes = 1024

// nexusProgress is the body of a Nexus progress delivery, the OperationProgress object of the
// Nexus HTTP spec.
type nexusProgress struct {
	Position string
	Counter  int64
	Metadata map[string]string
}

// parseNexusProgress decodes and checks a progress body. Every error it returns is the caller's
// fault, so the frontend answers it with a 400, which tells the handler to stop sending progress.
func parseNexusProgress(body []byte) (nexusProgress, error) {
	var wire struct {
		Position string            `json:"position"`
		Counter  json.RawMessage   `json:"counter"`
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return nexusProgress{}, fmt.Errorf(
			"progress body is not an OperationProgress object: %w", err)
	}
	counter, err := parseNexusProgressCounter(wire.Counter)
	if err != nil {
		return nexusProgress{}, err
	}
	if len(wire.Position) > maxNexusProgressPositionBytes {
		return nexusProgress{}, fmt.Errorf(
			"progress position is %d bytes, more than the %d allowed",
			len(wire.Position), maxNexusProgressPositionBytes)
	}
	size := 0
	for key, value := range wire.Metadata {
		size += len(key) + len(value)
	}
	if size > maxNexusProgressMetadataBytes {
		return nexusProgress{}, fmt.Errorf(
			"progress metadata is %d bytes, more than the %d allowed",
			size, maxNexusProgressMetadataBytes)
	}
	return nexusProgress{Position: wire.Position, Counter: counter, Metadata: wire.Metadata}, nil
}

// parseNexusProgressCounter reads the counter as a JSON number or a decimal string, since JSON
// encoders commonly write 64-bit integers as strings. History keeps it as an int64.
func parseNexusProgressCounter(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, errors.New("progress counter is required")
	}
	text := string(raw)
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, fmt.Errorf("progress counter is not a string: %w", err)
		}
	}
	// The spec says decimal, so a sign is refused even though ParseInt takes one.
	counter, err := strconv.ParseInt(text, 10, 64)
	if err != nil || counter <= 0 || text[0] == '+' {
		return 0, fmt.Errorf("progress counter must be a positive integer, got %s", raw)
	}
	return counter, nil
}

// nexusProgressProto is the progress as History carries it.
func nexusProgressProto(progress nexusProgress) *nexuspb.NexusOperationProgress {
	return &nexuspb.NexusOperationProgress{
		Position: progress.Position,
		Counter:  progress.Counter,
		Metadata: progress.Metadata,
	}
}
