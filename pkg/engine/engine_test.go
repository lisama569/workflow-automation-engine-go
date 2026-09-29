package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/activity"
	"github.com/lisama569/workflow-automation-engine-go/pkg/engine"
	"github.com/lisama569/workflow-automation-engine-go/pkg/mockservice"
	"github.com/lisama569/workflow-automation-engine-go/pkg/store"
	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// ---- 测试夹具 --------------------------------------------------------------

type counters struct {
	mu  sync.Mutex
	run map[string]int
}

func newCounters() *counters { return &counters{run: map[string]int{}} }

func (c *counters) inc(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.run[path]++
	return c.run[path]
}

func (c *counters) get(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.run[path]
}

type fixture struct {
	t          *testing.T
	dir        string
	reg        *activity.Registry
	svc        *mockservice.Service
	cnt        *counters
	registered map[string]bool
}

// testLogger 把触发标识、步骤状态等结构化日志打到 stdout，满足单测日志要求。
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, testWriter{t}), &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{
		t:          t,
		dir:        dir,
		reg:        activity.NewRegistry(),
		svc:        mockservice.New(),
		cnt:        newCounters(),
		registered: map[string]bool{},
	}
	return f
}

func (f *fixture) newEngine() *engine.Engine {
	st, err := store.NewFileStore(filepath.Join(f.dir, "store"))
	if err != nil {
		f.t.Fatalf("new store: %v", err)
	}
	return engine.New(st, f.reg, engine.WithLogger(testLogger(f.t)))
}

// registerStandard 注册一组可复用的标准活动。
func (f *fixture) registerStandard() {
	f.reg.MustRegister("ok", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		n := f.cnt.inc(ac.StepPath)
		return map[string]any{"ran": n, "by": ac.IdempotencyKey}, nil
	})
	f.reg.MustRegister("read_upstream", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		v := ac.Data["/0"]
		return map[string]any{"saw_upstream": fmt.Sprint(v)}, nil
	})
	f.reg.MustRegister("branch_marker", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		return map[string]any{"branch": ac.StepPath}, nil
	})
	f.reg.MustRegister("flaky", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		_, err := f.svc.Call(ac.IdempotencyKey, mockservice.CallOptions{FailTimes: 2})
		if err != nil {
			return nil, &workflow.RetryableError{Err: err}
		}
		return map[string]any{"ok": true}, nil
	})
	f.reg.MustRegister("fatal", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		_, err := f.svc.Call(ac.IdempotencyKey, mockservice.CallOptions{Fatal: true})
		return nil, &workflow.NonRetryableError{Err: err}
	})
	f.reg.MustRegister("sleep", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		ms := 100
		if v, ok := ac.Input["sleep_ms"].(float64); ok {
			ms = int(v)
		}
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return map[string]any{"slept_ms": ms}, nil
		case <-ctx.Done():
			return nil, ctx.Err() // DeadlineExceeded -> 可重试
		}
	})
	f.reg.MustRegister("always_retryable_fail", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		return nil, &workflow.RetryableError{Err: errors.New("boom")}
	})
	f.reg.MustRegister("crash_once", func(ctx context.Context, ac activity.Context) (map[string]any, error) {
		n := f.cnt.inc(ac.StepPath)
		if n == 1 {
			// 模拟进程在活动执行中被杀：直接返回崩溃信号，不产生副作用。
			return nil, workflow.ErrSimulatedCrash
		}
		return map[string]any{"ran": n, "recovered": true}, nil
	})
}

func retry3() *workflow.RetryPolicy {
	return &workflow.RetryPolicy{
		MaxAttempts:       3,
		InitialBackoff:    workflow.Duration(time.Millisecond),
		MaxBackoff:        workflow.Duration(5 * time.Millisecond),
		BackoffMultiplier: 2,
	}
}

func printConclusion(t *testing.T, triggerID string, st *workflow.RunState) {
	t.Helper()
	var walk func(prefix string, steps []workflow.StepState)
	walk = func(prefix string, steps []workflow.StepState) {
		for _, s := range steps {
			t.Logf("CONCLUSION %s | %s%s [%s] status=%s attempts=%d %s",
				triggerID, prefix, s.Name, s.Path, s.Status, len(s.Attempts), s.FailureReason)
			if len(s.Children) > 0 {
				walk(prefix+"  ", s.Children)
			}
		}
	}
	t.Logf("CONCLUSION %s | RUN status=%s reason=%q", triggerID, st.Status, st.FailureReason)
	walk("", st.Steps)
}

func findStep(states []workflow.StepState, path string) *workflow.StepState {
	for i := range states {
		if states[i].Path == path {
			return &states[i]
		}
		if found := findStep(states[i].Children, path); found != nil {
			return found
		}
	}
	return nil
}
