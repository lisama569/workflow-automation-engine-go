package workflow

import (
	"context"
	"errors"
	"fmt"
)

// ActivityContext carries per-execution information to an activity handler.
type ActivityContext struct {
	WorkflowName string
	TriggerID    string
	StepName     string
	Attempt      int
}

// ActivityFunc is the handler executed for one step attempt.
//
// Activities SHOULD be idempotent: after a crash between activity completion
// and state persistence, the engine may invoke the handler once more.
// Return a *RetryableError to ask the engine for another attempt.
type ActivityFunc func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error)

// RetryableError marks an error as retryable.
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps err so the engine retries the step.
func Retryable(err error) error { return &RetryableError{Err: err} }

// NonRetryable wraps err so the engine fails the step immediately.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{Err: err}
}

type nonRetryableError struct{ Err error }

func (e *nonRetryableError) Error() string { return e.Err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.Err }

// IsRetryable reports whether err asks for a retry.
// Unknown errors are treated as non-retryable: handlers must opt in
// explicitly, which keeps failure semantics predictable.
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}

// Registry maps activity names to handlers.
type Registry struct{ funcs map[string]ActivityFunc }

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{funcs: map[string]ActivityFunc{}} }

// Register adds a handler under name.
func (r *Registry) Register(name string, f ActivityFunc) { r.funcs[name] = f }

func (r *Registry) lookup(name string) (ActivityFunc, error) {
	f, ok := r.funcs[name]
	if !ok {
		return nil, fmt.Errorf("activity %q is not registered", name)
	}
	return f, nil
}
