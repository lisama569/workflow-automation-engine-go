package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// TestRetryableFailureSucceedsWithinBudget：瞬时故障在重试预算内恢复，
// 活动记录 3 次尝试，前 2 次标记为 retryable，最终成功；下游副作用只发生一次。
func TestRetryableFailureSucceedsWithinBudget(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-retry-ok",
		Steps: []workflow.Step{
			{Name: "flaky", Kind: workflow.KindActivity, Activity: "flaky",
				Timeout: workflow.Duration(time.Second), RetryPolicy: retry3()},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-retry-ok", "trigger-retry-ok", nil)
	if err != nil || st.Status != workflow.RunSucceeded {
		t.Fatalf("status=%s err=%v reason=%s", st.Status, err, st.FailureReason)
	}
	step := findStep(st.Steps, "/0")
	if len(step.Attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(step.Attempts))
	}
	if !step.Attempts[0].Retryable || !step.Attempts[1].Retryable {
		t.Errorf("first two attempts must be marked retryable: %+v", step.Attempts)
	}
	if step.Attempts[2].Error != "" {
		t.Errorf("last attempt should succeed, got error %q", step.Attempts[2].Error)
	}
	if got := f.svc.SideEffectCount("trigger-retry-ok:/0"); got != 1 {
		t.Errorf("downstream side effects=%d want 1", got)
	}
	printConclusion(t, "trigger-retry-ok", st)
}

// TestNonRetryableFailsImmediately：不可重试错误不重试，一步失败、后续步骤 skipped，
// run 进入确定的 failed 终态，失败原因可追溯。
func TestNonRetryableFailsImmediately(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-fatal",
		Steps: []workflow.Step{
			{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
			{Name: "fatal", Kind: workflow.KindActivity, Activity: "fatal", RetryPolicy: retry3()},
			{Name: "c", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-fatal", "trigger-fatal-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Status != workflow.RunFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	fatal := findStep(st.Steps, "/1")
	if fatal.Status != workflow.StepFailed {
		t.Errorf("fatal step status=%s want failed", fatal.Status)
	}
	if len(fatal.Attempts) != 1 {
		t.Errorf("non-retryable step attempts=%d want 1", len(fatal.Attempts))
	}
	if c := findStep(st.Steps, "/2"); c.Status != workflow.StepSkipped {
		t.Errorf("downstream step status=%s want skipped", c.Status)
	}
	if st.FailureReason == "" {
		t.Errorf("run failure reason must be recorded")
	}
	printConclusion(t, "trigger-fatal-1", st)
}

// TestExhaustRetryBudgetFails：持续可重试失败耗尽预算后进入失败终态。
func TestExhaustRetryBudgetFails(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-retry-exhaust",
		Steps: []workflow.Step{
			{Name: "a", Kind: workflow.KindActivity, Activity: "always_retryable_fail",
				RetryPolicy: retry3()},
			{Name: "b", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-retry-exhaust", "trigger-retry-exhaust", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Status != workflow.RunFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	a := findStep(st.Steps, "/0")
	if a.Status != workflow.StepFailed || len(a.Attempts) != 3 {
		t.Fatalf("step a status=%s attempts=%d want failed/3", a.Status, len(a.Attempts))
	}
	if b := findStep(st.Steps, "/1"); b.Status != workflow.StepSkipped {
		t.Errorf("step b status=%s want skipped", b.Status)
	}
	printConclusion(t, "trigger-retry-exhaust", st)
}

// TestTimeoutIsTerminalAndRecorded：活动超过单步超时后重试，耗尽预算进入 timed_out 终态。
func TestTimeoutIsTerminalAndRecorded(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-timeout",
		Steps: []workflow.Step{
			{Name: "slow", Kind: workflow.KindActivity, Activity: "sleep",
				Timeout: workflow.Duration(20 * time.Millisecond), RetryPolicy: retry3()},
			{Name: "after", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-timeout", "trigger-timeout-1",
		map[string]any{"sleep_ms": 500})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Status != workflow.RunFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	slow := findStep(st.Steps, "/0")
	if slow.Status != workflow.StepTimedOut {
		t.Errorf("slow step status=%s want timed_out", slow.Status)
	}
	if len(slow.Attempts) != 3 {
		t.Errorf("timeout retries=%d want 3 attempts", len(slow.Attempts))
	}
	for _, a := range slow.Attempts {
		if !a.TimedOut || !a.Retryable {
			t.Errorf("attempt must be flagged timed_out+retryable: %+v", a)
		}
	}
	if after := findStep(st.Steps, "/1"); after.Status != workflow.StepSkipped {
		t.Errorf("after step status=%s want skipped", after.Status)
	}
	printConclusion(t, "trigger-timeout-1", st)
}

// TestParallelFailureConvention：并行分支一个不可重试失败，另一个照常跑完，
// 容器与 run 为 failed；被跳过约定只影响串行后续，不影响同层其他分支。
func TestParallelFailureConvention(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-par-fail",
		Steps: []workflow.Step{
			{
				Name: "fan", Kind: workflow.KindParallel,
				Children: []workflow.Step{
					{Name: "good", Kind: workflow.KindActivity, Activity: "ok"},
					{Name: "bad", Kind: workflow.KindActivity, Activity: "fatal"},
				},
			},
			{Name: "tail", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-par-fail", "trigger-par-fail", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Status != workflow.RunFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	fan := findStep(st.Steps, "/1")
	if fan == nil {
		fan = findStep(st.Steps, "/0")
	}
	if good := findStep(st.Steps, "/0/0"); good.Status != workflow.StepSucceeded {
		t.Errorf("good parallel branch status=%s want succeeded (branches run to end)", good.Status)
	}
	if bad := findStep(st.Steps, "/0/1"); bad.Status != workflow.StepFailed {
		t.Errorf("bad parallel branch status=%s want failed", bad.Status)
	}
	if tail := findStep(st.Steps, "/1"); tail == nil || tail.Status != workflow.StepSkipped {
		t.Errorf("serial tail after failed fan status=%v want skipped", tail)
	}
	// 失败分支仍完整跑过（没有被提前取消）。
	if got := f.cnt.get("/0/0"); got != 1 {
		t.Errorf("good branch executions=%d want 1", got)
	}
	printConclusion(t, "trigger-par-fail", st)
}

// TestParallelBranchesDoNotPolluteEachOther：分支对数据视图的写入相互隔离，
// 且只能看到上游（/0）的输出；成功后容器输出按稳定 path 合并给后续步骤。
func TestParallelBranchesDoNotPolluteEachOther(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()

	// 泄漏哨兵：若 A 分支的视图里出现了 B 分支的输出，则视为污染，直接报错。
	f.reg.MustRegister("branch_a", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		if _, leak := ac.Data["/1/1"]; leak {
			return nil, &workflow.NonRetryableError{Err: errLeak("branch A saw branch B output")}
		}
		return map[string]any{"a_only": "A"}, nil
	})
	f.reg.MustRegister("branch_b", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		if _, leak := ac.Data["/1/0"]; leak {
			return nil, &workflow.NonRetryableError{Err: errLeak("branch B saw branch A output")}
		}
		return map[string]any{"b_only": "B"}, nil
	})
	f.reg.MustRegister("merge_reader", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		a, aOK := ac.Data["/1/0"].(map[string]any)["a_only"]
		b, bOK := ac.Data["/1/1"].(map[string]any)["b_only"]
		if !aOK || !bOK {
			return nil, &workflow.NonRetryableError{Err: errLeak("merged outputs missing after parallel")}
		}
		return map[string]any{"a": a, "b": b}, nil
	})

	def := workflow.Definition{
		Name: "wf-par-isolation",
		Steps: []workflow.Step{
			{Name: "source", Kind: workflow.KindActivity, Activity: "ok"},
			{
				Name: "fan", Kind: workflow.KindParallel,
				Children: []workflow.Step{
					{Name: "a", Kind: workflow.KindActivity, Activity: "branch_a"},
					{Name: "b", Kind: workflow.KindActivity, Activity: "branch_b"},
				},
			},
			{Name: "merge", Kind: workflow.KindActivity, Activity: "merge_reader"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, err := eng.Trigger(context.Background(), "wf-par-isolation", "trigger-par-isolation", nil)
	if err != nil || st.Status != workflow.RunSucceeded {
		t.Fatalf("status=%s err=%v reason=%s", st.Status, err, st.FailureReason)
	}
	merge := findStep(st.Steps, "/2")
	if merge.Output["a"] != "A" || merge.Output["b"] != "B" {
		t.Errorf("merged output wrong: %+v", merge.Output)
	}
	printConclusion(t, "trigger-par-isolation", st)
}

// TestAttemptTraceability：每一步的输入、每次尝试的错误/输出都持久化可追溯。
func TestAttemptTraceability(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-trace",
		Steps: []workflow.Step{
			{Name: "flaky", Kind: workflow.KindActivity, Activity: "flaky", RetryPolicy: retry3()},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	st, _ := eng.Trigger(context.Background(), "wf-trace", "trigger-trace-1",
		map[string]any{"order_id": "O-77"})
	step := findStep(st.Steps, "/0")
	if step.Attempts[0].Input["trigger_input"] == nil {
		t.Errorf("attempt input snapshot missing")
	}
	if step.Attempts[0].Error == "" {
		t.Errorf("first failing attempt must record the error reason")
	}
	if step.Attempts[2].Output["ok"] != true {
		t.Errorf("successful attempt output missing: %+v", step.Attempts[2])
	}
	if !step.Attempts[0].StartedAt.Before(step.Attempts[2].EndedAt) {
		t.Errorf("attempt timestamps must be ordered")
	}
	printConclusion(t, "trigger-trace-1", st)
}

type errLeak string

func (e errLeak) Error() string { return string(e) }
