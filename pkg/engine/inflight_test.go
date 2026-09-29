package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// TestDuplicateTriggerWhileInFlight：首个触发仍在执行时，重复投递必须等待终态，
// 等待方不执行任何步骤（活动进入次数始终为 1），最终双方拿到同一终态。
func TestDuplicateTriggerWhileInFlight(t *testing.T) {
	f := newFixture(t)

	var entries int64
	var mu sync.Mutex
	entered := make(chan struct{})
	release := make(chan struct{})
	f.reg.MustRegister("slow_ok", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		mu.Lock()
		entries++
		n := entries
		mu.Unlock()
		if n == 1 {
			close(entered)
		}
		<-release // 测试控制释放，制造确定的「执行中」窗口
		return map[string]any{"done": true, "entry": n}, nil
	})

	def := workflow.Definition{
		Name: "wf-inflight",
		Steps: []workflow.Step{
			{Name: "slow", Kind: workflow.KindActivity, Activity: "slow_ok"},
		},
	}
	eng := f.newEngine()
	_ = eng.Register(def)

	firstDone := make(chan *workflow.RunState, 1)
	go func() {
		st, err := eng.Trigger(context.Background(), "wf-inflight", "trigger-inflight-1", nil)
		if err != nil {
			t.Errorf("first trigger err: %v", err)
		}
		firstDone <- st
	}()

	<-entered
	t.Logf("IN-FLIGHT trigger-inflight-1 | first execution inside activity, firing duplicate now")

	dupDone := make(chan *workflow.RunState, 1)
	go func() {
		st, err := eng.Trigger(context.Background(), "wf-inflight", "trigger-inflight-1", nil)
		if err != nil {
			t.Errorf("duplicate trigger err: %v", err)
		}
		dupDone <- st
	}()

	// 给重复投递 80ms：它必须一直等待，且活动不能被二次进入。
	select {
	case st := <-dupDone:
		t.Fatalf("duplicate returned before first execution finished: %+v", st)
	case <-time.After(80 * time.Millisecond):
		t.Logf("IN-FLIGHT trigger-inflight-1 | duplicate is waiting, no second activity entry")
	}
	mu.Lock()
	if entries != 1 {
		t.Fatalf("activity entered %d times during in-flight duplicate, want 1", entries)
	}
	mu.Unlock()

	close(release)
	first := <-firstDone
	dup := <-dupDone
	if first.Status != workflow.RunSucceeded || dup.Status != workflow.RunSucceeded {
		t.Fatalf("statuses first=%s dup=%s want both succeeded", first.Status, dup.Status)
	}
	if first.Epoch != dup.Epoch {
		t.Errorf("callers observed different persisted states: epoch %d vs %d", first.Epoch, dup.Epoch)
	}
	mu.Lock()
	if entries != 1 {
		t.Errorf("activity entered %d times total, want exactly 1", entries)
	}
	mu.Unlock()
	printConclusion(t, "trigger-inflight-1", dup)
}

// TestTriggerIDBoundToSingleWorkflow：同一 triggerID 绑定不同工作流名必须报错。
func TestTriggerIDBoundToSingleWorkflow(t *testing.T) {
	f := newFixture(t)
	f.registerStandard()
	def1 := workflow.Definition{Name: "wf-a", Steps: []workflow.Step{
		{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
	}}
	def2 := workflow.Definition{Name: "wf-b", Steps: []workflow.Step{
		{Name: "a", Kind: workflow.KindActivity, Activity: "ok"},
	}}
	eng := f.newEngine()
	_ = eng.Register(def1)
	_ = eng.Register(def2)

	if _, err := eng.Trigger(context.Background(), "wf-a", "trigger-bound", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Trigger(context.Background(), "wf-b", "trigger-bound", nil); err == nil {
		t.Fatal("expected binding conflict error")
	}
}
