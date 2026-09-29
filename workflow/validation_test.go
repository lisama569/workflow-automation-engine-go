package workflow

import (
	"context"
	"testing"
)

func TestValidateRejectsCyclesAndDuplicates(t *testing.T) {
	bad := []Definition{
		{Name: "dup", Steps: []StepDef{{Name: "a"}, {Name: "a"}}},
		{Name: "missingdep", Steps: []StepDef{{Name: "a", DependsOn: []string{"ghost"}}}},
		{Name: "cycle", Steps: []StepDef{
			{Name: "a", DependsOn: []string{"b"}},
			{Name: "b", DependsOn: []string{"a"}},
		}},
		{Name: "self", Steps: []StepDef{{Name: "a", DependsOn: []string{"a"}}}},
		{Name: "", Steps: []StepDef{{Name: "a"}}},
	}
	for _, d := range bad {
		if err := validateDef(d); err == nil {
			t.Fatalf("expected validation error for %+v", d)
		}
		if _, err := NewEngine(NewMemoryStore(), NewRegistry(), DefaultConfig(), nil).
			Run(context.Background(), d, "x"); err == nil {
			t.Fatalf("Run with invalid def %q must fail", d.Name)
		}
	}
}

func TestLedgerEffectAppliesExactlyOnce(t *testing.T) {
	l := NewInMemoryLedger()
	var effects int
	fn := func() (map[string]any, error) {
		effects++
		return map[string]any{"n": effects}, nil
	}
	for i := 0; i < 5; i++ {
		r, reused, err := l.Apply(context.Background(), "K1", fn)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && reused {
			t.Fatalf("first apply must not be reused")
		}
		if i > 0 && (!reused || r.Output["n"].(int) != 1) {
			t.Fatalf("apply %d should be deduped to first result, got reused=%v out=%v", i, reused, r.Output)
		}
	}
	if effects != 1 || l.Count() != 1 {
		t.Fatalf("effects=%d count=%d, both must be 1", effects, l.Count())
	}
}

func TestLedgerFailureIsNotRecorded(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	flaky := func() (map[string]any, error) {
		calls++
		if calls == 1 {
			return nil, errSentinelTest
		}
		return map[string]any{"ok": true}, nil
	}
	if _, _, err := l.Apply(context.Background(), "K", flaky); err == nil {
		t.Fatal("first call should fail")
	}
	r, reused, err := l.Apply(context.Background(), "K", flaky)
	if err != nil || reused {
		t.Fatalf("failed effect must be retryable: reused=%v err=%v", reused, err)
	}
	if n, _ := l.Count(); n != 1 || r.Output["ok"] != true {
		t.Fatalf("effect not recorded after recovery: n=%d r=%v", n, r)
	}
}

var errSentinelTest = &testErr{"sentinel"}

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }
