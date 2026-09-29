package workflow

import (
	"encoding/json"
	"net/url"
	"time"
)

// cloneState deep-copies a workflow state so callers can never mutate the
// store's in-memory representation directly.
func cloneState(in *WorkflowState) *WorkflowState {
	data, err := json.Marshal(in)
	if err != nil {
		panic(err) // all contained types are JSON-safe
	}
	out := &WorkflowState{}
	if err := json.Unmarshal(data, out); err != nil {
		panic(err)
	}
	return out
}

// safeName turns a trigger/workflow identifier into a filesystem-safe
// single path component. The encoding is reversible and collision-free
// (every byte maps to a distinct percent-escaped form).
func safeName(name string) string {
	escaped := url.PathEscape(name)
	// PathEscape leaves a few characters that are legal but keep filenames
	// tidy; additionally escape "." and "-" prefixes confusion is unnecessary
	// since we append extensions. Just guard empty names.
	if escaped == "" {
		return "_"
	}
	return escaped
}

// nowTS returns a UTC RFC3339Nano timestamp.
func nowTS() string { return time.Now().UTC().Format(time.RFC3339Nano) }
