package activity

import (
	"context"
	"fmt"
	"sync"
)

// Context 是活动执行时拿到的上下文。
// 并行分支之间拿到的 data 是深拷贝，互不污染。
type Context struct {
	TriggerID string
	StepPath  string
	Input     map[string]any // 触发工作流时的原始输入
	Data      map[string]any // 已完成步骤的输出视图： stepPath -> output
	// IdempotencyKey 由引擎保证稳定：同一 triggerID + 同一步骤，任何重试都相同。
	// 活动侧可用它对外部副作用做去重。
	IdempotencyKey string
}

// Func 是一个活动实现。
type Func func(ctx context.Context, ac Context) (map[string]any, error)

// Registry 保存活动名到实现的映射。
type Registry struct {
	mu         sync.RWMutex
	activities map[string]Func
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{activities: map[string]Func{}}
}

// Register 注册一个活动，重名返回错误。
func (r *Registry) Register(name string, f Func) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.activities[name]; ok {
		return fmt.Errorf("activity %q already registered", name)
	}
	r.activities[name] = f
	return nil
}

// MustRegister 注册活动，失败时 panic（用于进程启动期装配）。
func (r *Registry) MustRegister(name string, f Func) {
	if err := r.Register(name, f); err != nil {
		panic(err)
	}
}

// Lookup 查找活动。
func (r *Registry) Lookup(name string) (Func, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.activities[name]
	return f, ok
}
