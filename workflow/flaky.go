package workflow

import (
	"context"
	"fmt"
	"sync"
)

// FlakeCounter is the counter abstraction the flaky activity needs.
// *Ledger satisfies it with persistence; *InMemoryFlakeCounter is used in
// tests that do not touch the filesystem.
type FlakeCounter interface {
	IncrementCounter(ctx context.Context, name string, delta int) (int, error)
}

// InMemoryFlakeCounter is a process-local FlakeCounter.
type InMemoryFlakeCounter struct {
	mu       sync.Mutex
	counters map[string]int
}

// NewInMemoryFlakeCounter creates an empty in-memory counter.
func NewInMemoryFlakeCounter() *InMemoryFlakeCounter {
	return &InMemoryFlakeCounter{counters: map[string]int{}}
}

// IncrementCounter adds delta and returns the new value.
func (c *InMemoryFlakeCounter) IncrementCounter(_ context.Context, name string, delta int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name] += delta
	return c.counters[name], nil
}

// registerFlaky registers the "flaky" demo activity.
//
// It fails with a *retryable* error for the first fail_times invocations of
// each (trigger, step) and succeeds on the invocation after. Failure counts
// are persisted through FlakeCounter, so even if the process dies between
// retries, progress is retained and the retried execution eventually succeeds
// rather than failing forever. The successful effect is ledger-deduplicated.
func registerFlaky(reg *Registry, ledger EffectLedger, counter FlakeCounter) {
	// Tests register flaky with an in-memory counter; file ledger also
	// implements FlakeCounter.
	idempotentSuccess := IdempotentActivity(ledger, "", func(input map[string]any) (map[string]any, error) {
		return map[string]any{"result": "flaky succeeded"}, nil
	})

	reg.Register("flaky", func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error) {
		failTimes := inputInt(input, "fail_times", 1)
		counterKey := "flakes|wf=" + ac.WorkflowName + "|trigger=" + ac.TriggerID + "|step=" + ac.StepName
		seen, err := counter.IncrementCounter(ctx, counterKey, 1)
		if err != nil {
			return nil, NonRetryable(err)
		}
		if seen <= failTimes {
			return nil, Retryable(fmt.Errorf("flaky transient failure %d/%d (trigger=%s step=%s)",
				seen, failTimes, ac.TriggerID, ac.StepName))
		}
		return idempotentSuccess(ctx, ac, input)
	})
}
