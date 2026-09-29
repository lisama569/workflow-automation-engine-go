package workflow

import (
	"context"
	"log"
	"time"
)

// Config holds engine defaults.
type Config struct {
	// DefaultMaxAttempts is used when StepDef.MaxAttempts is zero.
	DefaultMaxAttempts int
	// DefaultTimeout bounds a single attempt when the step does not set one.
	DefaultTimeout time.Duration
	// DefaultInitialBackoff is the base retry delay (doubled each retry).
	DefaultInitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff.
	MaxBackoff time.Duration
}

// DefaultConfig returns sensible local defaults.
func DefaultConfig() Config {
	return Config{
		DefaultMaxAttempts:    3,
		DefaultTimeout:        5 * time.Second,
		DefaultInitialBackoff: 50 * time.Millisecond,
		MaxBackoff:            2 * time.Second,
	}
}

// Engine executes definitions against a store and an activity registry.
type Engine struct {
	cfg      Config
	store    Store
	registry *Registry
	logger   *log.Logger
}

// NewEngine constructs an engine.
func NewEngine(store Store, registry *Registry, cfg Config, logger *log.Logger) *Engine {
	return &Engine{cfg: cfg, store: store, registry: registry, logger: logger}
}

// Run triggers a workflow.
//
// Semantics:
//   - First triggerID: a new instance is created and executed to a terminal
//     state (or until ctx is canceled).
//   - Duplicate triggerID (same workflow name): the existing instance is
//     adopted; if it is unfinished it is resumed, otherwise its terminal
//     state is returned. Exactly one instance ever exists per triggerID.
func (e *Engine) Run(ctx context.Context, def Definition, triggerID string) (*RunResult, error) {
	return nil, nil
}

// ResumeAll resumes every non-terminal instance of def in the store.
// Intended to be called once at process startup.
func (e *Engine) ResumeAll(ctx context.Context, def Definition) error {
	return nil
}

// Get returns the current state of an instance.
func (e *Engine) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	return e.store.Get(ctx, workflowName, triggerID)
}
