// 本地演示：构建一个「校验 -> [并行: 风控/库存] -> 下单」的工作流，
// 其中库存步骤前 2 次调用返回瞬时失败以验证重试，下单步骤重复投递只生效一次。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/engine"
	"github.com/lisama569/workflow-automation-engine-go/pkg/mockservice"
	"github.com/lisama569/workflow-automation-engine-go/pkg/store"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dir := filepath.Join(os.TempDir(), "wf-engine-demo")
	_ = os.RemoveAll(dir)
	st, err := store.NewFileStore(dir)
	if err != nil {
		panic(err)
	}

	paymentSvc := mockservice.New()   // 下游：幂等下单，永不重复扣款
	inventorySvc := mockservice.New() // 下游：库存预留，前两次瞬时失败

	reg := activity.NewRegistry()
	reg.MustRegister("validate", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		return map[string]any{"valid": true, "order_id": ac.Input["order_id"]}, nil
	})
	reg.MustRegister("risk_check", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		return map[string]any{"risk": "low"}, nil
	})
	reg.MustRegister("reserve_inventory", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		res, err := inventorySvc.Call(ac.IdempotencyKey, mockservice.CallOptions{FailTimes: 2})
		if err != nil {
			return nil, &workflow.RetryableError{Err: err}
		}
		return map[string]any{"reservation": res.OrderID}, nil
	})
	reg.MustRegister("place_order", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		res, err := paymentSvc.Call(ac.IdempotencyKey, mockservice.CallOptions{})
		if err != nil {
			return nil, err
		}
		return map[string]any{"order": res.OrderID, "effect": res.Effect}, nil
	})

	def := workflow.Definition{
		Name: "order-pipeline",
		Steps: []workflow.Step{
			{Name: "validate", Kind: workflow.KindActivity, Activity: "validate"},
			{
				Name: "fanout",
				Kind: workflow.KindParallel,
				Children: []workflow.Step{
					{Name: "risk_check", Kind: workflow.KindActivity, Activity: "risk_check"},
					{
						Name:     "reserve_inventory",
						Kind:     workflow.KindActivity,
						Activity: "reserve_inventory",
						Timeout:  workflow.Duration(5 * time.Second),
						RetryPolicy: &workflow.RetryPolicy{
							MaxAttempts:       3,
							InitialBackoff:    workflow.Duration(20 * time.Millisecond),
							MaxBackoff:        workflow.Duration(100 * time.Millisecond),
							BackoffMultiplier: 2,
						},
					},
				},
			},
			{Name: "place_order", Kind: workflow.KindActivity, Activity: "place_order"},
		},
	}

	eng := engine.New(st, reg, engine.WithLogger(logger))
	if err := eng.Register(def); err != nil {
		panic(err)
	}

	ctx := context.Background()
	input := map[string]any{"order_id": "ORD-42", "sku": "SKU-1"}

	// 同一 triggerID 并发重复投递 3 次。
	results := make(chan *workflow.RunState, 3)
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			r, e := eng.Trigger(ctx, "order-pipeline", "trigger-001", input)
			results <- r
			errs <- e
		}()
	}
	var final *workflow.RunState
	for i := 0; i < 3; i++ {
		final = <-results
		if e := <-errs; e != nil {
			panic(e)
		}
	}

	fmt.Println("\n=== final ===")
	printSummary(final)
	fmt.Printf("payment side effects: %d (expect 1)\n", paymentSvc.SideEffectCount("trigger-001:/2"))
}

func printSummary(st *workflow.RunState) {
	fmt.Printf("trigger=%s workflow=%s status=%s epoch=%d\n",
		st.TriggerID, st.WorkflowName, st.Status, st.Epoch)
	var walk func(prefix string, steps []workflow.StepState)
	walk = func(prefix string, steps []workflow.StepState) {
		for _, s := range steps {
			fmt.Printf("%s- %s [%s] %s attempts=%d %s\n",
				prefix, s.Name, s.Path, s.Status, len(s.Attempts), s.FailureReason)
			if len(s.Children) > 0 {
				walk(prefix+"    ", s.Children)
			}
		}
	}
	walk("  ", st.Steps)
	if st.FailureReason != "" {
		fmt.Printf("failure: %s\n", st.FailureReason)
	}
}
