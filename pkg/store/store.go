package store

import (
	"context"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// Store 是工作流运行态的持久化抽象（本地用文件实现）。
type Store interface {
	// Load 读取一次触发对应的运行态；不存在返回 (nil, nil)。
	Load(ctx context.Context, triggerID string) (*workflow.RunState, error)
	// CreateIfAbsent 仅当 triggerID 不存在时写入，返回当前已存在的状态（若有）。
	// 用于并发/重复投递下的去重闸门。
	CreateIfAbsent(ctx context.Context, state *workflow.RunState) (existing *workflow.RunState, err error)
	// Save 全量覆盖保存运行态。
	Save(ctx context.Context, state *workflow.RunState) error
}
