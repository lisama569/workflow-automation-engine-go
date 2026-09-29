package workflow

import (
	"context"
	"fmt"
)

// IdempotentActivity wraps an effectful function with ledger-based
// deduplication. The business key is derived deterministically from the
// trigger id, step name and the input's "ledger_key" field, so retries and
// duplicate deliveries can never apply the effect twice.
//
// The supplied effect fn is what actually performs the (simulated) external
// call. It runs at most once per key, ever, across retries AND restarts.
func IdempotentActivity(ledger *Ledger, keyInputField string, fn func(input map[string]any) (map[string]any, error)) ActivityFunc {
	return func(ctx context.Context, ac ActivityContext, input map[string]any) (map[string]any, error) {
		return nil, nil
	}
}

// RegisterDemoActivities registers a small set of activities used by the
// built-in demo and the CLI:
//
//   - noop      : echoes its input
//   - flaky     : fails with a retryable error until input["fail_times"]
//                 consecutive failures for this trigger have been observed,
//                 then succeeds (deterministic, persisted in the ledger)
//   - alwaysfail: fails with a non-retryable error
//   - slow      : sleeps input["sleep_ms"] milliseconds (timeout demo)
func RegisterDemoActivities(reg *Registry, ledger *Ledger) {
	reg.Register("noop", nil)
	reg.Register("flaky", nil)
	reg.Register("alwaysfail", nil)
	reg.Register("slow", nil)
}

var errActivityPlaceholder = fmt.Errorf("activity not implemented")
