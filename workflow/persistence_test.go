package workflow

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newFileEngine(t *testing.T, dir string, reg *Registry) *Engine {
	t.Helper()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("file store: %v", err)
	}
	cfg := Config{
		DefaultMaxAttempts:    3,
		DefaultTimeout:        2 * time.Second,
		DefaultInitialBackoff: time.Millisecond,
		MaxBackoff:            10 * time.Millisecond,
	}
	return NewEngine(store, reg, cfg, log.New(testWriter{t}, "[engine] ", log.Lmicroseconds))
}

// TestFileStoreRoundTrip persists a state through one store instance and
// reads it back through a brand new one (simulating a restart).
func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := newState(serialDef(), "PERSIST-1")
	st.Status = StatusRunning
	st.Steps["a"].Status = StatusSucceeded
	st.Steps["a"].Attempts = []Attempt{{AttemptNo: 1, Output: map[string]any{"x": 1}}}
	st.Steps["b"].Status = StatusRunning
	got, adopted, err := s1.CreateIfAbsent(context.Background(), st)
	if err != nil || adopted {
		t.Fatalf("create adopted=%v err=%v", adopted, err)
	}
	if err := s1.CompareAndSwap(context.Background(), got, got.Version); err != nil {
		t.Fatalf("cas: %v", err)
	}

	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	again, adopted2, err := s2.CreateIfAbsent(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if !adopted2 {
		t.Fatalf("restarted store must detect existing trigger")
	}
	if again.Version != 1 || again.Steps["a"].Status != StatusSucceeded {
		t.Fatalf("state lost across reopen: %+v", again)
	}

	// Stale CAS is rejected.
	if err := s2.CompareAndSwap(context.Background(), again, 999); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}

	// List works from disk.
	list, err := s2.List(context.Background(), "serial")
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%v err=%v", list, err)
	}
}

// TestResumeAcrossRestart simulates a crash: process 1 completes step a and
// dies mid-workflow; process 2 (a fresh engine on the same directory) resumes
// by trigger id and must NOT re-run a, continuing from b.
func TestResumeAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ledger, err := NewLedger(dir)
	if err != nil {
		t.Fatal(err)
	}

	var executions int64
	reg := NewRegistry()
	reg.Register("counted", IdempotentActivity(ledger, "", func(in map[string]any) (map[string]any, error) {
		atomic.AddInt64(&executions, 1)
		return map[string]any{"ok": true}, nil
	}))
	def := serialThreeSteps("counted")

	// --- process 1: create the instance and mark a done manually via engine
	eng1 := newFileEngine(t, dir, reg)
	st0 := newState(def, "RESTART-1")
	st, adopted, err := eng1.store.CreateIfAbsent(context.Background(), st0)
	if err != nil || adopted {
		t.Fatalf("create adopted=%v err=%v", adopted, err)
	}
	st.Status = StatusRunning
	st.Steps["a"].Status = StatusSucceeded
	st.Steps["a"].Attempts = []Attempt{{AttemptNo: 1, Output: map[string]any{"ok": true}}}
	// Simulate that a's effect definitely happened.
	if _, _, err := ledger.Apply(context.Background(), "wf=serial-counted|trigger=RESTART-1|step=a", func() (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng1.store.CompareAndSwap(context.Background(), st, st.Version); err != nil {
		t.Fatalf("seed cas: %v", err)
	}

	// --- process 2: fresh engine, resumes by delivering the same trigger
	eng2 := newFileEngine(t, dir, reg)
	res, err := eng2.Run(context.Background(), def, "RESTART-1")
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if !res.Adopted {
		t.Fatalf("restarted trigger must be adopted")
	}
	if res.State.Status != StatusSucceeded {
		t.Fatalf("resumed workflow status=%s err=%s", res.State.Status, res.State.Err)
	}
	if len(res.State.Steps["a"].Attempts) != 1 {
		t.Fatalf("completed step a was re-executed: attempts=%d", len(res.State.Steps["a"].Attempts))
	}
	if len(res.State.Steps["b"].Attempts) != 1 || len(res.State.Steps["c"].Attempts) != 1 {
		t.Fatalf("b/c should each have one attempt: b=%d c=%d",
			len(res.State.Steps["b"].Attempts), len(res.State.Steps["c"].Attempts))
	}
	if n, _ := ledger.Count(); n != 3 {
		t.Fatalf("ledger effects=%d want 3", n)
	}
	t.Logf("CONCLUSION trigger=RESTART-1 resumed across restart: a preserved, b+c continued, effects=%d", mustCount(ledger))
}

// TestResumeAllOnStartup leaves an unfinished instance on disk and verifies
// Engine.ResumeAll drives it to completion (startup recovery path).
func TestResumeAllOnStartup(t *testing.T) {
	dir := t.TempDir()
	ledger, err := NewLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	RegisterDemoActivities(reg, ledger, ledger)
	def := Definition{Name: "startup", Steps: []StepDef{
		{Name: "one", Activity: "noop"},
		{Name: "two", Activity: "flaky", Input: map[string]any{"fail_times": 1},
			DependsOn: []string{"one"}, MaxAttempts: 4, InitialBackoff: time.Millisecond},
	}}
	// seed a pending instance, as if a previous process crashed before running
	eng0 := newFileEngine(t, dir, reg)
	if _, _, err := eng0.store.CreateIfAbsent(context.Background(), newState(def, "BOOT-1")); err != nil {
		t.Fatal(err)
	}

	eng1 := newFileEngine(t, dir, reg)
	if err := eng1.ResumeAll(context.Background(), def); err != nil {
		t.Fatalf("resume all: %v", err)
	}
	final, err := eng1.Get(context.Background(), "startup", "BOOT-1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != StatusSucceeded {
		t.Fatalf("startup resume status=%s", final.Status)
	}
	if len(final.Steps["two"].Attempts) != 2 {
		t.Fatalf("flaky step attempts=%d want 2", len(final.Steps["two"].Attempts))
	}
}

// TestCrossProcessDuplicateDelivery runs two engines sharing one directory
// (two "processes" simulated by two engine instances over the same files)
// and delivers the same trigger concurrently. Exactly one must execute; both
// must observe the identical terminal state and one set of effects.
func TestCrossProcessDuplicateDelivery(t *testing.T) {
	dir := t.TempDir()

	makeReg := func() (*Registry, *Ledger) {
		ledger, err := NewLedger(dir)
		if err != nil {
			t.Fatal(err)
		}
		reg := NewRegistry()
		reg.Register("slow-once", IdempotentActivity(ledger, "", func(in map[string]any) (map[string]any, error) {
			time.Sleep(40 * time.Millisecond)
			return map[string]any{"applied": true}, nil
		}))
		return reg, ledger
	}
	reg1, _ := makeReg()
	reg2, ledger2 := makeReg()
	eng1 := newFileEngine(t, dir, reg1)
	eng2 := newFileEngine(t, dir, reg2)
	def := Definition{Name: "xproc", Steps: []StepDef{{Name: "s", Activity: "slow-once", Timeout: 5 * time.Second}}}

	type outcome struct {
		r   *RunResult
		err error
	}
	ch := make(chan outcome, 2)
	go func() { r, e := eng1.Run(context.Background(), def, "X-1"); ch <- outcome{r, e} }()
	time.Sleep(5 * time.Millisecond)
	go func() { r, e := eng2.Run(context.Background(), def, "X-1"); ch <- outcome{r, e} }()

	var results []outcome
	for i := 0; i < 2; i++ {
		results = append(results, <-ch)
	}
	for _, o := range results {
		if o.err != nil {
			t.Fatalf("run: %v", o.err)
		}
		if o.r.State.Status != StatusSucceeded {
			t.Fatalf("status=%s err=%s", o.r.State.Status, o.r.State.Err)
		}
	}
	n, err := ledger2.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ledger effects=%d, want 1 across two processes", n)
	}
	adoptedCount := 0
	for _, o := range results {
		if o.r.Adopted {
			adoptedCount++
		}
	}
	if adoptedCount != 1 {
		t.Fatalf("want exactly one adopter, got %d", adoptedCount)
	}
	t.Logf("CONCLUSION trigger=X-1 cross-process adopters=%d effects=%d final=%s",
		adoptedCount, n, results[0].r.State.Status)

	// nothing else left around
	if infos, _ := os.ReadDir(filepath.Join(dir, "workflows")); len(infos) == 0 {
		t.Fatalf("workflow dir missing")
	}
}

func mustCount(l *Ledger) int {
	n, err := l.Count()
	if err != nil {
		panic(err)
	}
	return n
}
