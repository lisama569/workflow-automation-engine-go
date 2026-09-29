package engine

import (
	"context"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// finalizePanic 在执行链路 panic 时把运行态推进到确定的失败终态。
func (e *Engine) finalizePanic(st *workflow.RunState, reason string) {
	if st.Status.Terminal() {
		return
	}
	now := time.Now().UTC()
	st.Status = workflow.RunFailed
	st.FailureReason = reason
	st.TerminatedAt = &now
	st.UpdatedAt = now
	st.Epoch++
	if err := e.store.Save(context.Background(), st); err != nil {
		e.log.Error("persist panic terminal state", "trigger_id", st.TriggerID, "err", err)
	}
}
