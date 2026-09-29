package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/store"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// Engine 负责工作流的触发去重、断点续跑、重试/超时与串并行编排。
type Engine struct {
	mu    sync.RWMutex
	defs  map[string]workflow.Definition
	store store.Store
	reg   *activity.Registry
	log   *slog.Logger

	coordMu sync.Mutex
	coords  map[string]*runCoord
}

// runCoord 保证「同一 triggerID 进程内只有一个执行者」，重复投递方等待终态。
type runCoord struct {
	mu       sync.Mutex
	cond     *sync.Cond
	running  bool
	terminal bool
}

// Option 配置引擎。
type Option func(*Engine)

// WithLogger 注入日志器（测试可注入输出到 stdout 的 logger 打印触发与步骤状态）。
func WithLogger(log *slog.Logger) Option {
	return func(e *Engine) { e.log = log }
}

// New 创建引擎。
func New(st store.Store, reg *activity.Registry, opts ...Option) *Engine {
	e := &Engine{
		defs:   map[string]workflow.Definition{},
		store:  st,
		reg:    reg,
		log:    slog.Default(),
		coords: map[string]*runCoord{},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Register 注册一个声明式工作流定义并做静态校验。
func (e *Engine) Register(def workflow.Definition) error {
	if def.Name == "" {
		return errors.New("workflow definition requires a name")
	}
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow %q requires at least one step", def.Name)
	}
	if err := validateSteps(def.Steps, e.reg); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.defs[def.Name]; exists {
		return fmt.Errorf("workflow %q already registered", def.Name)
	}
	e.defs[def.Name] = def
	return nil
}

func validateSteps(steps []workflow.Step, reg *activity.Registry) error {
	for i := range steps {
		s := &steps[i]
		if s.Name == "" {
			return fmt.Errorf("step at index %d requires a name", i)
		}
		switch s.Kind {
		case workflow.KindActivity:
			if s.Activity == "" {
				return fmt.Errorf("activity step %q requires an activity name", s.Name)
			}
			if _, ok := reg.Lookup(s.Activity); !ok {
				return fmt.Errorf("step %q references unregistered activity %q", s.Name, s.Activity)
			}
		case workflow.KindSerial, workflow.KindParallel:
			if len(s.Children) == 0 {
				return fmt.Errorf("container step %q requires children", s.Name)
			}
			if err := validateSteps(s.Children, reg); err != nil {
				return err
			}
		default:
			return fmt.Errorf("step %q has unknown kind %q", s.Name, s.Kind)
		}
	}
	return nil
}

// Get 读取某次触发的持久化状态。
func (e *Engine) Get(ctx context.Context, triggerID string) (*workflow.RunState, error) {
	return e.store.Load(ctx, triggerID)
}

// Trigger 触发一次工作流。
// 同一个 triggerID 的重复投递（含并发、进程重启后的补投）只会产生一次执行：
// 首个调用成为执行者并把每一步状态落盘；重复调用识别出已有执行，
// 等待并返回同一个终态，绝不重复执行已完成步骤或重复产生副作用。
// 执行者使用独立于调用方 ctx 的上下文执行，等待方取消不会中断工作流。
func (e *Engine) Trigger(ctx context.Context, name, triggerID string, input map[string]any) (*workflow.RunState, error) {
	if triggerID == "" {
		return nil, errors.New("triggerID is required for idempotent delivery")
	}
	e.mu.RLock()
	def, ok := e.defs[name]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("workflow %q is not registered", name)
	}

	now := time.Now().UTC()
	initial := &workflow.RunState{
		TriggerID:    triggerID,
		WorkflowName: name,
		Status:       workflow.RunPending,
		Input:        cloneMap(input),
		Steps:        buildStepStates(def.Steps, ""),
		CreatedAt:    now,
		UpdatedAt:    now,
		Epoch:        1,
	}

	// 去重闸门：并发的多个 Trigger 只有一个 CreateIfAbsent 成功。
	existing, err := e.store.CreateIfAbsent(ctx, initial)
	if err != nil {
		return nil, err
	}
	st := initial
	if existing != nil {
		if existing.WorkflowName != name {
			return nil, fmt.Errorf("triggerID %q already bound to workflow %q", triggerID, existing.WorkflowName)
		}
		st = existing
	}
	return e.joinOrAdopt(ctx, def, st)
}

// joinOrAdopt 返回已有执行的终态，或在本进程没有存活执行者时接管恢复执行。
func (e *Engine) joinOrAdopt(waitCtx context.Context, def workflow.Definition, st *workflow.RunState) (*workflow.RunState, error) {
	for {
		if st.Status.Terminal() {
			// 重复投递命中终态：直接返回，什么都不执行。
			e.log.Info("trigger already terminal, replay result",
				"trigger_id", st.TriggerID, "status", st.Status)
			return st, nil
		}

		coord := e.getCoord(st.TriggerID)
		coord.mu.Lock()
		if coord.terminal {
			coord.mu.Unlock()
			loaded, err := e.store.Load(waitCtx, st.TriggerID)
			if err != nil {
				return nil, err
			}
			return loaded, nil
		}
		if coord.running {
			coord.mu.Unlock()
			// 重复投递：已有执行者，等待其终态，期间不执行任何步骤。
			e.log.Info("duplicate trigger detected, waiting for in-flight execution",
				"trigger_id", st.TriggerID)
			select {
			case <-waitCtx.Done():
				return nil, waitCtx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			loaded, err := e.store.Load(waitCtx, st.TriggerID)
			if err != nil {
				return nil, err
			}
			st = loaded
			continue
		}
		// 本进程无存活执行者：接管执行（覆盖首次执行与进程崩溃后的恢复）。
		coord.running = true
		coord.terminal = false
		coord.mu.Unlock()

		runErr := e.runSafely(def, st)

		coord.mu.Lock()
		coord.running = false
		// 模拟进程崩溃时运行态未进入终态，不标记 terminal，允许后续投递接管恢复。
		coord.terminal = !errors.Is(runErr, workflow.ErrSimulatedCrash)
		coord.cond.Broadcast()
		coord.mu.Unlock()

		final, loadErr := e.store.Load(context.Background(), st.TriggerID)
		if loadErr != nil {
			return nil, loadErr
		}
		if runErr != nil {
			return final, runErr
		}
		return final, nil
	}
}

// runSafely 执行工作流并兜底 panic，保证 run 一定进入确定终态。
func (e *Engine) runSafely(def workflow.Definition, st *workflow.RunState) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("workflow execution panicked: %v", r)
			e.finalizePanic(st, fmt.Sprintf("internal panic: %v", r))
		}
	}()
	rt := &runRuntime{engine: e, def: def, state: st, log: e.log}
	return e.execLoop(context.Background(), rt)
}

func (e *Engine) getCoord(triggerID string) *runCoord {
	e.coordMu.Lock()
	defer e.coordMu.Unlock()
	c, ok := e.coords[triggerID]
	if !ok {
		c = &runCoord{}
		c.cond = sync.NewCond(&c.mu)
		e.coords[triggerID] = c
	}
	return c
}
