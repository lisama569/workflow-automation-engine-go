package workflow

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// effectKey builds the deterministic business key used by the ledger.
// It is scoped by trigger and step, and optionally by an input field, so the
// same logical operation always maps to the same key no matter how many times
// the step is attempted or the trigger redelivered.
func effectKey(ac ActivityContext, keyInputField string, input map[string]any) string {
	key := "wf=" + ac.WorkflowName + "|trigger=" + ac.TriggerID + "|step=" + ac.StepName
	if keyInputField != "" {
		if v, ok := input[keyInputField]; ok {
			key += "|" + keyInputField + "=" + fmt.Sprintf("%v", v)
		}
	}
	return key
}

// IdempotentActivity wraps an effectful function with ledger-based
// deduplication. The business key is derived deterministically from the
// trigger id, step name and (optionally) an input field, so retries and
// duplicate deliveries can never apply the effect twice.
//
// The supplied fn is what performs the (simulated) external call. It runs at
// most once per key, ever, across retries AND process restarts. When a stored
// result exists it is returned directly ("reused" is surfaced in the output)
// so callers can distinguish a real effect from a deduplicated one.
func IdempotentActivity(ledger EffectLedger, keyInputField string, fn func(input map[string]any) (map[string]any, error)) ActivityFunc {
	return func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error) {
		key := effectKey(ac, keyInputField, input)
		res, reused, err := ledger.Apply(ctx, key, func() (map[string]any, error) {
			return fn(input)
		})
		if err != nil {
			return nil, err
		}
		out := map[string]any{}
		for k, v := range res.Output {
			out[k] = v
		}
		out["_effect_reused"] = reused
		out["_effect_key"] = key
		out["_attempt"] = ac.Attempt
		return out, nil
	}
}

// inputInt reads an int from an activity input, accepting float64 (JSON),
// int and string forms.
func inputInt(input map[string]any, key string, def int) int {
	v, ok := input[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		if i, err := strconv.Atoi(n); err == nil {
			return i
		}
	}
	return def
}

// RegisterDemoActivities registers a small set of activities used by the
// built-in demo and the CLI:
//
//   - noop      : applies a ledger-keyed no-op effect, echoes its input.
//   - flaky     : fails with a retryable error fail_times times (per trigger
//   - step, counted through the ledger-adjacent flake counter file), then
//     succeeds. The flake count is itself persisted, so the behavior stays
//     deterministic across restarts: the engine retries see progress.
//   - alwaysfail: fails with a non-retryable error every time.
//   - slow      : sleeps sleep_ms milliseconds (used to demo attempt timeout
//     and parallel scheduling); honors context cancellation.
//
// RegisterDemoActivities registers the built-in demo activities.
// counter is used by the flaky activity to persist failure progress; pass a
// *Ledger (which implements FlakeCounter) for durable behavior or an
// InMemoryFlakeCounter for tests. When nil, an in-memory counter is used.
func RegisterDemoActivities(reg *Registry, ledger EffectLedger, counter FlakeCounter) {
	if counter == nil {
		counter = NewInMemoryFlakeCounter()
	}
	reg.Register("noop", IdempotentActivity(ledger, "", func(input map[string]any) (map[string]any, error) {
		echo := map[string]any{"echo": map[string]any(input)}
		return echo, nil
	}))

	reg.Register("alwaysfail", func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error) {
		return nil, NonRetryable(fmt.Errorf("alwaysfail activity never succeeds (trigger=%s step=%s attempt=%d)",
			ac.TriggerID, ac.StepName, ac.Attempt))
	})

	reg.Register("slow", func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error) {
		ms := inputInt(input, "sleep_ms", 100)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return map[string]any{"slept_ms": ms}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	registerFlaky(reg, ledger, counter)
}
