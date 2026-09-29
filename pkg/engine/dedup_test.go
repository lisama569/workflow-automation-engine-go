package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lisama569/workflow-automation-engine-go/pkg/engine"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// TestConcurrentTriggersExecuteOnce 是核心场景：
// N 个 goroutine 用同一个 triggerID 并发触发，工作流只能产生一次执行，
// 每个活动步骤只跑一次，所有调用方拿到同一个成功终态。
func TestConcurrentTriggersExecuteOnce(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()

	def := workflow.Definition{
		Name: "wf-concurrent",
		Steps: []workflow.Step{
			{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
			{
				Name: "fan", Kind: workflow.KindParallel,
				Children: []workflow.Step{
					{Name: "b1", Kind: workflow.KindActivity, Activity: "ok"},
					{Name: "b2", Kind: workflow.KindActivity, Activity: "ok"},
				},
			},
			{Name: "c", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	if err := eng.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([]*workflow.RunState, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = eng.Trigger(context.Background(), "wf-concurrent",
				"trigger-concurrent-1", map[string]any{"order_id": "O1"})
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("trigger %d returned error: %v", i, err)
		}
		if results[i].Status != workflow.RunSucceeded {
			t.Fatalf("trigger %d status=%s want succeeded", i, results[i].Status)
		}
	}
	// 每个活动步骤的真实执行次数必须恰好为 1。
	for _, path := range []string{"/0", "/1/0", "/1/1", "/2"} {
		if got := f.cnt.get(path); got != 1 {
			t.Errorf("step %s executed %d times, want exactly 1", path, got)
		}
	}
	printConclusion(t, "trigger-concurrent-1", results[0])
}

// TestRedeliveryAfterTerminal 验证终态之后的重复投递：
// 直接识别为已完成并回放结果，不再执行任何步骤。
func TestRedeliveryAfterTerminal(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-redeliver",
		Steps: []workflow.Step{
			{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
			{Name: "b", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}
	eng := f.newEngine()
	if err := eng.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}

	first, err := eng.Trigger(context.Background(), "wf-redeliver", "trigger-redeliver-1", nil)
	if err != nil || first.Status != workflow.RunSucceeded {
		t.Fatalf("first trigger: status=%s err=%v", first.Status, err)
	}

	// 用全新的引擎实例重放「投递方重试/进程重启后的补投」。
	eng2 := f.newEngine()
	if err := eng2.Register(def); err != nil {
		t.Fatalf("register on new engine: %v", err)
	}
	second, err := eng2.Trigger(context.Background(), "wf-redeliver", "trigger-redeliver-1", nil)
	if err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if second.Status != workflow.RunSucceeded {
		t.Fatalf("redelivered status=%s", second.Status)
	}
	for _, path := range []string{"/0", "/1"} {
		if got := f.cnt.get(path); got != 1 {
			t.Errorf("step %s executed %d times after redelivery, want 1", path, got)
		}
	}
	printConclusion(t, "trigger-redeliver-1", second)
}

// TestResumeAfterSimulatedCrash 验证断点续跑：
// 第一个「进程」在第二个步骤执行中崩溃（无终态落盘），
// 用同一个持久化目录启动第二个引擎，已完成的 /0 不重跑，/1 从中断处继续并成功。
func TestResumeAfterSimulatedCrash(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def := workflow.Definition{
		Name: "wf-resume",
		Steps: []workflow.Step{
			{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
			{Name: "b", Kind: workflow.KindActivity, Activity: "crash_once"},
			{Name: "c", Kind: workflow.KindActivity, Activity: "ok"},
		},
	}

	eng1 := f.newEngine()
	if err := eng1.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	st1, err := eng1.Trigger(context.Background(), "wf-resume", "trigger-resume-1", nil)
	if !errors.Is(err, workflow.ErrSimulatedCrash) {
		t.Fatalf("want ErrSimulatedCrash, got err=%v status=%s", err, st1.Status)
	}
	if st1.Status == workflow.RunSucceeded || st1.Status == workflow.RunFailed {
		t.Fatalf("crashed run must not have a terminal status, got %s", st1.Status)
	}
	if got := f.cnt.get("/0"); got != 1 {
		t.Fatalf("step /0 ran %d times before crash, want 1", got)
	}
	t.Logf("CRASH trigger-resume-1 | state persisted without terminal: status=%s", st1.Status)
	printConclusion(t, "trigger-resume-1", st1)

	// 模拟进程重启：新引擎、同一份磁盘状态。
	eng2 := f.newEngine()
	if err := eng2.Register(def); err != nil {
		t.Fatalf("register on restarted engine: %v", err)
	}
	st2, err := eng2.Trigger(context.Background(), "wf-resume", "trigger-resume-1", nil)
	if err != nil {
		t.Fatalf("resume trigger: %v", err)
	}
	if st2.Status != workflow.RunSucceeded {
		t.Fatalf("resumed run status=%s reason=%s", st2.Status, st2.FailureReason)
	}
	if got := f.cnt.get("/0"); got != 1 {
		t.Errorf("completed step /0 re-executed on resume: ran %d times, want 1", got)
	}
	if got := f.cnt.get("/1"); got != 2 {
		t.Errorf("crashed step /1 attempts=%d, want 2 (crash attempt + 1 resume)", got)
	}
	if got := f.cnt.get("/2"); got != 1 {
		t.Errorf("pending step /2 ran %d times, want 1", got)
	}
	// 崩溃后恢复运行的步骤输出可追溯。
	if b := findStep(st2.Steps, "/1"); b == nil || b.Output["recovered"] != true {
		t.Errorf("resumed step output missing recovery marker: %+v", b)
	}
	printConclusion(t, "trigger-resume-1", st2)
}

var _ = engine.New
