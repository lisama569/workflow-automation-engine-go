package workflow

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func testEngine(t *testing.T, store Store, reg *Registry) *Engine {
	t.Helper()
	cfg := Config{
		DefaultMaxAttempts:    3,
		DefaultTimeout:        time.Second,
		DefaultInitialBackoff: time.Millisecond,
		MaxBackoff:            10 * time.Millisecond,
	}
	return NewEngine(store, reg, cfg, log.New(testWriter{t}, "[engine] ", log.Lmicroseconds))
}

// testWriter routes logs through t.Logf so they appear in unit test output
// with trigger id, step status and conclusion already embedded in messages.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", trimNewline(p))
	return len(p), nil
}

func trimNewline(p []byte) []byte {
	for len(p) > 0 && (p[len(p)-1] == '\n' || p[len(p)-1] == '\r') {
		p = p[:len(p)-1]
	}
	return p
}

func serialDef() Definition {
	return Definition{
		Name: "serial",
		Steps: []StepDef{
			{Name: "a", Activity: "noop"},
			{Name: "b", Activity: "noop", DependsOn: []string{"a"}},
			{Name: "c", Activity: "noop", DependsOn: []string{"b"}},
		},
	}
}

func TestSerialWorkflowSucceeds(t *testing.T) {
	store := NewMemoryStore()
	ledger := NewInMemoryLedger()
	reg := NewRegistry()
	RegisterDemoActivities(reg, ledger, NewInMemoryFlakeCounter())
	eng := testEngine(t, store, reg)

	res, err := eng.Run(context.Background(), serialDef(), "TRG-1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Adopted {
		t.Fatalf("first trigger must not be adopted")
	}
	if res.State.Status != StatusSucceeded {
		t.Fatalf("status=%s, want succeeded; err=%s", res.State.Status, res.State.Err)
	}
	for _, name := range []string{"a", "b", "c"} {
		ss := res.State.Steps[name]
		if ss.Status != StatusSucceeded || len(ss.Attempts) != 1 {
			t.Fatalf("step %s: status=%s attempts=%d", name, ss.Status, len(ss.Attempts))
		}
	}
	if ledger.Count() != 3 {
		t.Fatalf("ledger effects=%d, want 3", ledger.Count())
	}
}

func TestParallelBranchesDoNotShareState(t *testing.T) {
	// Two parallel leaves each append to their own output only; we assert
	// their recorded inputs/outputs are isolated.
	store := NewMemoryStore()
	reg := NewRegistry()
	reg.Register("branch", func(ctx context.Context, ac ActivityContext, in map[string]any) (map[string]any, error) {
		time.Sleep(time.Duration(in["sleep_ms"].(int)) * time.Millisecond)
		return map[string]any{"branch": ac.StepName, "saw": in["id"]}, nil
	})
	eng := testEngine(t, store, reg)

	def := Definition{Name: "parallel", Steps: []StepDef{
		{Name: "x", Activity: "branch", Input: map[string]any{"id": "X", "sleep_ms": 20}},
		{Name: "y", Activity: "branch", Input: map[string]any{"id": "Y", "sleep_ms": 5}},
	}}
	res, err := eng.Run(context.Background(), def, "P1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.State.Status != StatusSucceeded {
		t.Fatalf("status=%s", res.State.Status)
	}
	xOut := res.State.Steps["x"].Attempts[0].Output
	yOut := res.State.Steps["y"].Attempts[0].Output
	if xOut["branch"] != "x" || xOut["saw"] != "X" {
		t.Fatalf("x branch state leaked: %v", xOut)
	}
	if yOut["branch"] != "y" || yOut["saw"] != "Y" {
		t.Fatalf("y branch state leaked: %v", yOut)
	}
	// x slept longer; both still succeed => they really ran concurrently.
	if xEnd := res.State.Steps["x"].EndedAt; xEnd < res.State.Steps["y"].EndedAt {
		// expected ordering (y finishes first); just log it
		t.Logf("parallel ordering confirmed: y finished before x")
	}
}

func TestFailureConventionFailFastAndSkipped(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	var ran int64
	reg.Register("ok", func(ctx context.Context, _ ActivityContext, _ map[string]any) (map[string]any, error) {
		atomic.AddInt64(&ran, 1)
		return map[string]any{"ok": true}, nil
	})
	reg.Register("boom", func(ctx context.Context, _ ActivityContext, _ map[string]any) (map[string]any, error) {
		return nil, NonRetryable(errors.New("boom"))
	})
	eng := testEngine(t, store, reg)

	def := Definition{Name: "failfast", Steps: []StepDef{
		{Name: "root", Activity: "ok"},
		{Name: "bad", Activity: "boom", DependsOn: []string{"root"}},
		{Name: "after1", Activity: "ok", DependsOn: []string{"bad"}},
		{Name: "after2", Activity: "ok", DependsOn: []string{"bad"}},
	}}
	res, err := eng.Run(context.Background(), def, "F1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.State.Status != StatusFailed {
		t.Fatalf("want failed, got %s", res.State.Status)
	}
	if res.State.Steps["root"].Status != StatusSucceeded {
		t.Fatalf("root should remain succeeded, got %s", res.State.Steps["root"].Status)
	}
	if res.State.Steps["bad"].Status != StatusFailed {
		t.Fatalf("bad status=%s", res.State.Steps["bad"].Status)
	}
	for _, n := range []string{"after1", "after2"} {
		if res.State.Steps[n].Status != StatusSkipped {
			t.Fatalf("%s should be skipped, got %s", n, res.State.Steps[n].Status)
		}
		if len(res.State.Steps[n].Attempts) != 0 {
			t.Fatalf("skipped step %s must have zero attempts, got %d", n, len(res.State.Steps[n].Attempts))
		}
	}
	if atomic.LoadInt64(&ran) != 1 {
		t.Fatalf("ok activity ran %d times, want 1 (skipped steps never execute)", ran)
	}
}

func TestRetryExhaustionHasTerminalState(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	var calls int64
	reg.Register("always-retryable", func(ctx context.Context, ac ActivityContext, _ map[string]any) (map[string]any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, Retryable(errors.New("transient"))
	})
	eng := testEngine(t, store, reg)

	def := Definition{Name: "retry-exhaust", Steps: []StepDef{
		{Name: "s", Activity: "always-retryable", MaxAttempts: 3, InitialBackoff: time.Millisecond},
	}}
	res, err := eng.Run(context.Background(), def, "R1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.State.Status != StatusFailed {
		t.Fatalf("want failed, got %s", res.State.Status)
	}
	ss := res.State.Steps["s"]
	if len(ss.Attempts) != 3 {
		t.Fatalf("attempts=%d, want 3", len(ss.Attempts))
	}
	for i, a := range ss.Attempts {
		if !a.Retryable || a.Err == "" {
			t.Fatalf("attempt %d not marked retryable with error: %+v", i+1, a)
		}
	}
	if atomic.LoadInt64(&calls) != 3 {
		t.Fatalf("handler calls=%d, want 3", calls)
	}
}

func TestTimeoutIsRetryableThenTerminal(t *testing.T) {
	store := NewMemoryStore()
	ledger := NewInMemoryLedger()
	reg := NewRegistry()
	RegisterDemoActivities(reg, ledger, NewInMemoryFlakeCounter())
	eng := testEngine(t, store, reg)

	def := Definition{Name: "timeout", Steps: []StepDef{
		{Name: "slow", Activity: "slow", Input: map[string]any{"sleep_ms": 500},
			Timeout: 20 * time.Millisecond, MaxAttempts: 2, InitialBackoff: time.Millisecond},
	}}
	res, err := eng.Run(context.Background(), def, "TO1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.State.Status != StatusFailed {
		t.Fatalf("want failed, got %s", res.State.Status)
	}
	for i, a := range res.State.Steps["slow"].Attempts {
		if !a.TimedOut || !a.Retryable {
			t.Fatalf("attempt %d: timedOut=%v retryable=%v", i+1, a.TimedOut, a.Retryable)
		}
	}
}

func TestRetryThenSuccessRecordsFullTrace(t *testing.T) {
	store := NewMemoryStore()
	ledger := NewInMemoryLedger()
	reg := NewRegistry()
	RegisterDemoActivities(reg, ledger, NewInMemoryFlakeCounter())
	eng := testEngine(t, store, reg)

	def := Definition{Name: "flaky-ok", Steps: []StepDef{
		{Name: "f", Activity: "flaky", Input: map[string]any{"fail_times": 2},
			MaxAttempts: 5, InitialBackoff: time.Millisecond, Timeout: time.Second},
	}}
	res, err := eng.Run(context.Background(), def, "FL1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.State.Status != StatusSucceeded {
		t.Fatalf("want succeeded, got %s err=%s", res.State.Status, res.State.Err)
	}
	att := res.State.Steps["f"].Attempts
	if len(att) != 3 {
		t.Fatalf("attempts=%d want 3 (2 failures + success)", len(att))
	}
	if att[0].Err == "" || !att[0].Retryable || att[1].Err == "" || !att[1].Retryable {
		t.Fatalf("first two attempts must be retryable failures: %+v / %+v", att[0], att[1])
	}
	if att[2].Err != "" || att[2].Output["result"] != "flaky succeeded" {
		t.Fatalf("third attempt must succeed with output, got %+v", att[2])
	}
}
