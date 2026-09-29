// Package workflow implements a small, local workflow orchestration engine.
//
// It supports:
//   - Declarative DAG-like definitions (serial / parallel steps).
//   - Persistent state with resume across process restarts.
//   - At-most-once execution of completed steps (deduplicated by triggerID).
//   - Retries for retryable failures, per-attempt timeout and terminal states.
//   - Full execution traces (inputs, outputs, failures) for every attempt.
package workflow

import (
	"context"
	"time"
)

// Status is the state of a step run, a workflow instance, or a trigger.
type Status int

const (
	StatusPending   Status = iota // not started
	StatusRunning                 // at least one attempt in flight
	StatusSucceeded               // finished successfully
	StatusFailed                  // terminal failure
	StatusSkipped                 // never run because an upstream step failed
)

func (s Status) String() string { return statusName[s] }

var statusName = [...]string{
	StatusPending:   "pending",
	StatusRunning:   "running",
	StatusSucceeded: "succeeded",
	StatusFailed:    "failed",
	StatusSkipped:   "skipped",
}

// Terminal reports whether the status is a final status.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusSkipped
}

// StepDef declares one workflow step.
//
// Parallelism is expressed through DependsOn: steps whose dependency sets
// are disjoint are executed in parallel. Steps sharing no dependency relation
// at all are independent roots and also run in parallel.
type StepDef struct {
	// Name is the unique step identifier within a definition.
	Name string

	// Activity is the name under which the executable handler is registered.
	Activity string

	// Input is the static input payload handed to the activity.
	Input map[string]any

	// DependsOn lists step names that must succeed before this step runs.
	// If any dependency terminally fails, this step is Skipped.
	DependsOn []string

	// MaxAttempts is the number of attempts including the first one.
	// Zero falls back to the engine default.
	MaxAttempts int

	// Timeout bounds a single attempt. Zero falls back to the engine default.
	Timeout Duration

	// InitialBackoff is the delay before the second attempt; later attempts
	// use exponential backoff. Zero falls back to the engine default.
	InitialBackoff Duration
}

// Definition is a declarative workflow definition.
type Definition struct {
	Name  string
	Steps []StepDef
}

// Attempt records one execution try of a step.
type Attempt struct {
	AttemptNo int            `json:"attempt_no"`
	StartedAt string         `json:"started_at"`
	EndedAt   string         `json:"ended_at"`
	Input     map[string]any `json:"input"`
	Output    map[string]any `json:"output,omitempty"`
	Err       string         `json:"error,omitempty"`
	Retryable bool           `json:"retryable"`
	TimedOut  bool           `json:"timed_out"`
}

// StepState is the persisted state of one step inside a workflow instance.
type StepState struct {
	Name      string    `json:"name"`
	Activity  string    `json:"activity"`
	Status    Status    `json:"status"`
	Attempts  []Attempt `json:"attempts"`
	StartedAt string    `json:"started_at,omitempty"`
	EndedAt   string    `json:"ended_at,omitempty"`
}

// WorkflowState is the persisted state of one workflow instance.
// One instance corresponds to one (WorkflowName, TriggerID) pair.
type WorkflowState struct {
	WorkflowName string                `json:"workflow_name"`
	TriggerID    string                `json:"trigger_id"`
	Status       Status                `json:"status"`
	Steps        map[string]*StepState `json:"steps"`
	CreatedAt    string                `json:"created_at"`
	UpdatedAt    string                `json:"updated_at"`
	// Version is incremented on every persisted update; used for optimistic
	// concurrency control between processes.
	Version int `json:"version"`
	// Err records the terminal failure reason of the workflow.
	Err string `json:"err,omitempty"`
}

// RunResult is returned by Engine.Run and Engine.Get.
type RunResult struct {
	State *WorkflowState
	// Adopted is true when this call found an existing instance for the
	// triggerID (duplicate delivery) instead of creating a new one.
	Adopted bool
}

// Store persists workflow states.
type Store interface {
	// CreateIfAbsent atomically creates the state if triggerID is unknown.
	// It returns the existing state and true when the trigger already existed.
	CreateIfAbsent(ctx context.Context, state *WorkflowState) (existing *WorkflowState, adopted bool, err error)

	// Get loads a state.
	Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error)

	// CompareAndSwap persists state only when the stored Version equals
	// wantVersion. It returns ErrConflict on a version mismatch.
	CompareAndSwap(ctx context.Context, state *WorkflowState, wantVersion int) error

	// List returns all instances of a workflow (for resume-on-startup).
	List(ctx context.Context, workflowName string) ([]*WorkflowState, error)
}

// Duration is an alias of time.Duration for use in definitions.
type Duration = time.Duration
