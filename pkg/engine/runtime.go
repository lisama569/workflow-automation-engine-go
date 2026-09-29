package engine

import (
	"log/slog"
	"sync"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// runRuntime 承载单次执行的内存态，不参与持久化。
type runRuntime struct {
	engine    *Engine
	def       workflow.Definition
	state     *workflow.RunState
	log       *slog.Logger
	persistMu sync.Mutex // 串行化并行分支对同一份 RunState 的落盘
}
