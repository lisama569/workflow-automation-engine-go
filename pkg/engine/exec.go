package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// 失败处理约定（全引擎前后一致）：
//  1. 串行容器：子步骤严格按序执行；任一步骤失败/超时后，剩余兄弟步骤标记 skipped，
//     容器结论为 failed。
//  2. 并行容器：所有分支一律并发跑到结束（不做提前取消）；每个分支拿到独立深拷贝的
//     上游数据视图，分支之间互不污染；全部结束后合并输出，只要有一个分支失败，
//     容器结论即为 failed。
//  3. 活动（叶子）：仅 RetryableError 与超时会重试；NonRetryableError 立即终态失败；
//     超过 MaxAttempts 后失败，最后一次是超时则终态为 timed_out。
//  4. 任一步骤失败后，其所在串行链路上的外层后续步骤同样 skipped，run 进入 failed 终态。
//  5. 已处于终态的步骤在恢复执行时直接跳过，其输出从持久化状态重建进数据视图。

// commit 在同一次加锁内完成「修改状态树 + 落盘」。
// 并行分支的活动*执行*在锁外并发进行，但状态记账与整树序列化在这里串行化，
// 从而避免对共享状态树的数据竞争。
func (rt *runRuntime) commit(ctx context.Context, mut func()) error {
	rt.persistMu.Lock()
	defer rt.persistMu.Unlock()
	if mut != nil {
		mut()
	}
	rt.state.Epoch++
	rt.state.UpdatedAt = time.Now().UTC()
	return rt.engine.store.Save(ctx, rt.state)
}

// execLoop 对一次触发执行（或恢复执行）直到终态，每一步状态变化都会落盘。
func (e *Engine) execLoop(ctx context.Context, rt *runRuntime) error {
	rt.log.Info("workflow execution started",
		"trigger_id", rt.state.TriggerID, "workflow", rt.def.Name,
		"resume", rt.state.Status != workflow.RunPending)

	if rt.state.Status == workflow.RunPending {
		if err := rt.commit(ctx, func() {
			rt.state.Status = workflow.RunRunning
		}); err != nil {
			return err
		}
	}

	// 恢复场景：从持久化的成功步骤重建数据视图，已完成步骤不会被重复执行。
	data := map[string]any{}
	rt.commit(ctx, func() { collectOutputs(rt.state.Steps, data) })

	for i := range rt.def.Steps {
		def := &rt.def.Steps[i]
		st := &rt.state.Steps[i]
		status, stepErr := rt.execStep(ctx, def, st, data)
		if errors.Is(stepErr, workflow.ErrSimulatedCrash) {
			rt.log.Info("simulated crash: execution aborted without terminal state",
				"trigger_id", rt.state.TriggerID)
			return stepErr
		}
		if stepErr != nil {
			return stepErr
		}
		if !statusOK(status) {
			reason := fmt.Sprintf("step %q (%s) ended with %s: %s",
				st.Name, st.Path, status, firstFailureReason(st))
			rt.commit(ctx, func() {
				skipPendingOneSlice(rt.state.Steps[i+1:],
					fmt.Sprintf("upstream step %q did not succeed (%s)", st.Name, status))
				now := time.Now().UTC()
				rt.state.Status = workflow.RunFailed
				rt.state.FailureReason = reason
				rt.state.TerminatedAt = &now
			})
			rt.log.Info("workflow execution failed",
				"trigger_id", rt.state.TriggerID, "reason", reason)
			return nil
		}
	}

	err := rt.commit(ctx, func() {
		now := time.Now().UTC()
		rt.state.Status = workflow.RunSucceeded
		rt.state.FailureReason = ""
		rt.state.TerminatedAt = &now
	})
	if err != nil {
		return err
	}
	rt.log.Info("workflow execution succeeded", "trigger_id", rt.state.TriggerID)
	return nil
}

// execStep 执行单个步骤，返回其最终状态；data 为该步骤可见的上游输出视图。
// 返回 ErrSimulatedCrash 表示模拟进程崩溃中止，状态停留在崩溃前一刻（running）。
func (rt *runRuntime) execStep(ctx context.Context, def *workflow.Step, st *workflow.StepState, data map[string]any) (workflow.StepStatus, error) {
	if st.Status.Terminal() {
		rt.log.Info("resume: step already in terminal state, skip execution",
			"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name, "status", st.Status)
		return st.Status, nil
	}
	switch def.Kind {
	case workflow.KindSerial:
		return rt.execSerial(ctx, def, st, data)
	case workflow.KindParallel:
		return rt.execParallel(ctx, def, st, data)
	default:
		if err := rt.runActivity(ctx, def, st, data); err != nil {
			return st.Status, err
		}
		return st.Status, nil
	}
}

func (rt *runRuntime) execSerial(ctx context.Context, def *workflow.Step, st *workflow.StepState, data map[string]any) (workflow.StepStatus, error) {
	if err := rt.commit(ctx, func() { enterContainer(st) }); err != nil {
		rt.log.Error("persist container state", "err", err)
	}
	for i := range def.Children {
		childDef := &def.Children[i]
		childSt := &st.Children[i]
		status, stepErr := rt.execStep(ctx, childDef, childSt, data)
		if stepErr != nil {
			return st.Status, stepErr
		}
		if !statusOK(status) {
			reason := fmt.Sprintf("child %q %s: %s", childSt.Name, status, firstFailureReason(childSt))
			rt.commit(ctx, func() {
				skipPendingOneSlice(st.Children[i+1:],
					fmt.Sprintf("previous serial step %q ended with %s", childSt.Name, status))
				now := time.Now().UTC()
				st.Status = workflow.StepFailed
				st.FailureReason = reason
				st.EndedAt = &now
			})
			return workflow.StepFailed, nil
		}
	}
	rt.commit(ctx, func() {
		now := time.Now().UTC()
		st.Status = workflow.StepSucceeded
		st.EndedAt = &now
	})
	return workflow.StepSucceeded, nil
}

func (rt *runRuntime) execParallel(ctx context.Context, def *workflow.Step, st *workflow.StepState, data map[string]any) (workflow.StepStatus, error) {
	if err := rt.commit(ctx, func() { enterContainer(st) }); err != nil {
		rt.log.Error("persist container state", "err", err)
	}

	n := len(def.Children)
	statuses := make([]workflow.StepStatus, n)
	branchViews := make([]map[string]any, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		childDef := &def.Children[i]
		childSt := &st.Children[i]
		// 每个分支拿到上游数据的独立深拷贝：分支内写入绝不污染其他分支。
		branchViews[i] = cloneMap(data)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					now := time.Now().UTC()
					rt.commit(ctx, func() {
						childSt.Status = workflow.StepFailed
						childSt.FailureReason = fmt.Sprintf("branch panicked: %v", r)
						childSt.EndedAt = &now
					})
					statuses[i] = workflow.StepFailed
				}
			}()
			statuses[i], errs[i] = rt.execStep(ctx, childDef, childSt, branchViews[i])
		}()
	}
	wg.Wait()

	for _, branchErr := range errs {
		if branchErr != nil {
			return st.Status, branchErr
		}
	}

	// 所有分支结束后才合并输出（key 为全局稳定 path，分支间不会冲突）。
	var allOK = true
	var failedIdx = -1
	for i := 0; i < n; i++ {
		for k, v := range branchViews[i] {
			data[k] = v
		}
		if !statusOK(statuses[i]) {
			allOK = false
			if failedIdx < 0 {
				failedIdx = i
			}
		}
	}
	rt.commit(ctx, func() {
		now := time.Now().UTC()
		st.EndedAt = &now
		if allOK {
			st.Status = workflow.StepSucceeded
		} else {
			fc := &st.Children[failedIdx]
			st.Status = workflow.StepFailed
			st.FailureReason = fmt.Sprintf("parallel branch %q %s: %s",
				fc.Name, fc.Status, firstFailureReason(fc))
		}
	})
	if allOK {
		return workflow.StepSucceeded, nil
	}
	return workflow.StepFailed, nil
}

func enterContainer(st *workflow.StepState) {
	now := time.Now().UTC()
	if st.StartedAt == nil {
		st.StartedAt = &now
	}
	st.Status = workflow.StepRunning
}

// runActivity 执行叶子活动，带逐次尝试记录、重试与单步超时。
func (rt *runRuntime) runActivity(ctx context.Context, def *workflow.Step, st *workflow.StepState, data map[string]any) error {
	policy := def.RetryPolicy
	if policy == nil {
		policy = &workflow.RetryPolicy{MaxAttempts: 1}
	}
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	rt.commit(ctx, func() {
		now := time.Now().UTC()
		if st.StartedAt == nil {
			st.StartedAt = &now
		}
		st.Status = workflow.StepRunning
	})

	// 幂等键在同一 triggerID + 同一步骤路径下永远稳定，重试/恢复/重复投递都一致。
	idempotencyKey := rt.state.TriggerID + ":" + st.Path
	backoff := 0 * time.Second
	if policy.InitialBackoff > 0 {
		backoff = policy.InitialBackoff.Std()
	}

	fn, ok := rt.engine.reg.Lookup(def.Activity)
	if !ok {
		rt.commit(ctx, func() {
			now := time.Now().UTC()
			st.Status = workflow.StepFailed
			st.FailureReason = fmt.Sprintf("activity %q not registered", def.Activity)
			st.EndedAt = &now
		})
		return nil
	}

	var lastErr string
	lastTimedOut := false
	for attemptNo := 1; attemptNo <= maxAttempts; attemptNo++ {
		if attemptNo > 1 {
			rt.log.Info("retrying activity step",
				"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name,
				"attempt", attemptNo, "backoff", backoff.String())
			time.Sleep(backoff)
			if policy.BackoffMultiplier > 1 {
				backoff = time.Duration(float64(backoff) * policy.BackoffMultiplier)
				if policy.MaxBackoff > 0 && backoff > policy.MaxBackoff.Std() {
					backoff = policy.MaxBackoff.Std()
				}
			}
		}

		attemptCtx, cancel := context.WithCancel(ctx)
		if def.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, def.Timeout.Std())
		}

		// 输入快照在锁外拷贝即可（data 由所在分支独占）。
		ac := activity.Context{
			TriggerID:      rt.state.TriggerID,
			StepPath:       st.Path,
			Input:          cloneMap(rt.state.Input),
			Data:           cloneMap(data),
			IdempotencyKey: idempotencyKey,
		}
		started := time.Now().UTC()

		rt.log.Info("executing activity step",
			"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name,
			"activity", def.Activity, "attempt", attemptNo, "idempotency_key", idempotencyKey)

		// 活动函数在锁外执行，并行分支因此真正并发。
		out, err := fn(attemptCtx, ac)
		cancel()
		ended := time.Now().UTC()

		if errors.Is(err, workflow.ErrSimulatedCrash) {
			// 模拟进程被杀：不记录本次尝试、不写任何终态，步骤停留在 running。
			rt.log.Info("simulated crash inside activity, abort run immediately",
				"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name)
			return err
		}

		if err == nil {
			normalized := normalizeMap(out)
			status := workflow.StepSucceeded
			rt.commit(ctx, func() {
				st.Attempts = append(st.Attempts, workflow.Attempt{
					AttemptNo: attemptNo,
					StartedAt: started,
					EndedAt:   ended,
					Input: map[string]any{
						"trigger_input":     cloneMap(rt.state.Input),
						"available_outputs": cloneMap(data),
					},
					Output: normalized,
				})
				st.Output = normalized
				st.Status = status
				st.FailureReason = ""
				st.EndedAt = &ended
				data[st.Path] = normalized
			})
			rt.log.Info("activity step succeeded",
				"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name, "attempt", attemptNo)
			return nil
		}

		timedOut := errors.Is(err, context.DeadlineExceeded) || attemptCtx.Err() == context.DeadlineExceeded
		var nonRetry *workflow.NonRetryableError
		// 超时按可重试处理；显式 NonRetryableError 立即终态；其余错误默认可重试。
		retryable := true
		if errors.As(err, &nonRetry) {
			retryable = false
		}
		lastErr = err.Error()
		lastTimedOut = timedOut
		errMsg := lastErr

		rt.commit(ctx, func() {
			st.Attempts = append(st.Attempts, workflow.Attempt{
				AttemptNo: attemptNo,
				StartedAt: started,
				EndedAt:   ended,
				Input: map[string]any{
					"trigger_input":     cloneMap(rt.state.Input),
					"available_outputs": cloneMap(data),
				},
				Error:     errMsg,
				Retryable: retryable,
				TimedOut:  timedOut,
			})
		})

		rt.log.Info("activity step attempt failed",
			"trigger_id", rt.state.TriggerID, "step", st.Path, "name", st.Name,
			"attempt", attemptNo, "retryable", retryable, "timed_out", timedOut, "error", lastErr)

		if !retryable || attemptNo == maxAttempts {
			break
		}
	}

	terminal := workflow.StepFailed
	if lastTimedOut {
		terminal = workflow.StepTimedOut
	}
	rt.commit(ctx, func() {
		now := time.Now().UTC()
		st.EndedAt = &now
		st.FailureReason = lastErr
		st.Status = terminal
	})
	return nil
}

func statusOK(s workflow.StepStatus) bool { return s == workflow.StepSucceeded }

// skipPendingOneSlice 把尚未开始的步骤（递归）标记为 skipped。
func skipPendingOneSlice(states []workflow.StepState, reason string) {
	for i := range states {
		skipPendingOne(&states[i], reason)
	}
}

func skipPendingOne(st *workflow.StepState, reason string) {
	if st.Status.Terminal() {
		return
	}
	now := time.Now().UTC()
	st.Status = workflow.StepSkipped
	st.FailureReason = reason
	st.EndedAt = &now
	for i := range st.Children {
		skipPendingOne(&st.Children[i], reason)
	}
}

// collectOutputs 从（恢复的）状态树中提取所有成功活动的输出，重建数据视图。
func collectOutputs(states []workflow.StepState, data map[string]any) {
	for i := range states {
		st := &states[i]
		if st.Kind == workflow.KindActivity && st.Status == workflow.StepSucceeded && st.Output != nil {
			data[st.Path] = st.Output
		}
		collectOutputs(st.Children, data)
	}
}

// firstFailureReason 在子树中找到第一个确定的失败原因。
func firstFailureReason(st *workflow.StepState) string {
	if st.FailureReason != "" {
		return st.FailureReason
	}
	for i := range st.Children {
		if r := firstFailureReason(&st.Children[i]); r != "" {
			return r
		}
	}
	return ""
}

func normalizeMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	b, err := json.Marshal(in)
	if err != nil {
		return in
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return in
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}
