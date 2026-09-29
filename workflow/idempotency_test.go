package workflow

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentTriggersSameTriggerID fires the SAME trigger id from many
// goroutines at once. Requirements under test:
//   - the activity of every step executes its (simulated) effect exactly once;
//   - all callers observe the SAME terminal succeeded state;
//   - all but one call report adoption (duplicate delivery identified).
func TestConcurrentTriggersSameTriggerID(t *testing.T) {
	store := NewMemoryStore()
	ledger := NewInMemoryLedger()
	reg := NewRegistry()

	var rawCalls int64 // handler invocations, including deduped replays
	reg.Register("counted", IdempotentActivity(ledger, "", func(in map[string]any) (map[string]any, error) {
		atomic.AddInt64(&rawCalls, 1)
		time.Sleep(5 * time.Millisecond) // widen the race window
		return map[string]any{"applied": true}, nil
	}))
	eng := testEngine(t, store, reg)

	def := Definition{Name: "concurrent", Steps: []StepDef{
		{Name: "s1", Activity: "counted"},
		{Name: "s2", Activity: "counted", DependsOn: []string{"s1"}},
	}}

	const n = 20
	var wg sync.WaitGroup
	results := make([]*RunResult, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			t.Logf("DELIVERY trigger=DUP-1 goroutine=%d fired", i)
			results[i], errs[i] = eng.Run(context.Background(), def, "DUP-1")
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	adopted, fresh := 0, 0
	var first *WorkflowState
	for i, r := range results {
		if r == nil {
			t.Fatalf("caller %d nil result", i)
		}
		if r.Adopted {
			adopted++
		} else {
			fresh++
		}
		if r.State.Status != StatusSucceeded {
			t.Fatalf("caller %d sees status=%s err=%s", i, r.State.Status, r.State.Err)
		}
		if first == nil {
			first = r.State
		} else if r.State.Version != first.Version {
			t.Fatalf("caller %d version=%d differs from %d", i, r.State.Version, first.Version)
		}
	}
	t.Logf("CONCLUSION trigger=DUP-1 callers=%d fresh=%d adopted(duplicate)=%d final=%s version=%d",
		n, fresh, adopted, first.Status, first.Version)
	if fresh != 1 || adopted != n-1 {
		t.Fatalf("want exactly 1 fresh and %d adopted, got fresh=%d adopted=%d", n-1, fresh, adopted)
	}

	// The ledger is the source of truth for side effects: 2 steps, 1 effect
	// each, regardless of 20 concurrent deliveries.
	if got := ledger.Count(); got != 2 {
		t.Fatalf("ledger effects=%d, want exactly 2 (no duplicated side effects)", got)
	}
	if raw := atomic.LoadInt64(&rawCalls); raw != 2 {
		t.Fatalf("raw effect fn executions=%d, want 2", raw)
	}
	for _, step := range []string{"s1", "s2"} {
		ss := first.Steps[step]
		if len(ss.Attempts) != 1 {
			t.Fatalf("step %s attempts=%d, want 1 (completed step never re-executed)", step, len(ss.Attempts))
		}
	}
}

// TestDistinctTriggersRunInParallel shows different trigger ids are isolated:
// each gets its own instance, steps and ledger keys.
func TestDistinctTriggersAreIsolated(t *testing.T) {
	store := NewMemoryStore()
	ledger := NewInMemoryLedger()
	reg := NewRegistry()
	reg.Register("noop", IdempotentActivity(ledger, "", func(in map[string]any) (map[string]any, error) {
		return map[string]any{"order": in["order"]}, nil
	}))
	eng := testEngine(t, store, reg)

	def := Definition{Name: "orders", Steps: []StepDef{{Name: "create", Activity: "noop",
		Input: map[string]any{}}}}
	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			trg := fmt.Sprintf("ORD-%d", i)
			res, err := eng.Run(context.Background(), def, trg)
			if err != nil {
				t.Errorf("trigger %s: %v", trg, err)
				return
			}
			if res.State.Status != StatusSucceeded {
				t.Errorf("trigger %s status=%s", trg, res.State.Status)
			}
		}(i)
	}
	wg.Wait()
	if got := ledger.Count(); got != n {
		t.Fatalf("ledger effects=%d, want %d", got, n)
	}
}

// TestDuplicateDeliveryAfterCompletion calls Run twice sequentially with the
// same trigger and proves the second call executes no step at all.
func TestDuplicateDeliveryAfterCompletion(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	var calls int64
	reg.Register("once", func(ctx context.Context, _ ActivityContext, _ map[string]any) (map[string]any, error) {
		atomic.AddInt64(&calls, 1)
		return map[string]any{}, nil
	})
	eng := testEngine(t, store, reg)
	def := serialThreeSteps("once")

	r1, err := eng.Run(context.Background(), def, "SEQ-DUP")
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	r2, err := eng.Run(context.Background(), def, "SEQ-DUP")
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if !r2.Adopted {
		t.Fatalf("second delivery must be adopted")
	}
	if r2.State.Version != r1.State.Version {
		t.Fatalf("version changed on duplicate: %d -> %d", r1.State.Version, r2.State.Version)
	}
	if atomic.LoadInt64(&calls) != 3 {
		t.Fatalf("handler executed %d times across both deliveries, want 3", calls)
	}
}

// TestDuplicateDeliveryWhileRunning starts a long step, fires the same
// trigger concurrently, and ensures only one execution happens and the waiter
// sees the final succeeded state.
func TestDuplicateDeliveryWhileRunning(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	var calls int64
	release := make(chan struct{})
	reg.Register("blocking", func(ctx context.Context, ac ActivityContext, _ map[string]any) (map[string]any, error) {
		atomic.AddInt64(&calls, 1)
		t.Logf("STEP trigger=%s step=%s attempt=%d entered, blocking...", ac.TriggerID, ac.StepName, ac.Attempt)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return map[string]any{"done": true}, nil
	})
	eng := testEngine(t, store, reg)
	def := Definition{Name: "blocking-wf", Steps: []StepDef{
		{Name: "slow-step", Activity: "blocking", Timeout: 5 * time.Second},
	}}

	firstDone := make(chan *RunResult, 1)
	go func() {
		r, err := eng.Run(context.Background(), def, "LONG-1")
		if err != nil {
			t.Errorf("first run: %v", err)
		}
		firstDone <- r
	}()

	// Give the first run time to enter the activity, then deliver twice more.
	time.Sleep(30 * time.Millisecond)
	const dup = 5
	var wg sync.WaitGroup
	dups := make([]*RunResult, dup)
	for i := 0; i < dup; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t.Logf("DELIVERY trigger=LONG-1 duplicate=%d fired while running", i)
			r, err := eng.Run(context.Background(), def, "LONG-1")
			if err != nil {
				t.Errorf("dup %d: %v", i, err)
			}
			dups[i] = r
		}(i)
	}
	time.Sleep(30 * time.Millisecond)
	t.Logf("releasing blocked step; all %d deliveries must converge on success", dup)
	close(release)

	r1 := <-firstDone
	wg.Wait()
	if r1.State.Status != StatusSucceeded {
		t.Fatalf("first run status=%s", r1.State.Status)
	}
	for i, r := range dups {
		if !r.Adopted {
			t.Fatalf("dup %d not adopted", i)
		}
		if r.State.Status != StatusSucceeded {
			t.Fatalf("dup %d status=%s", i, r.State.Status)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("blocking step executed %d times, want exactly 1", got)
	}
	t.Logf("CONCLUSION trigger=LONG-1 single execution=%d, %d duplicates converged, final=%s",
		calls, dup, r1.State.Status)
}

// silence unused import guard if test file is compiled selectively
var _ = log.New
