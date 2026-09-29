package engine

import (
	"strconv"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// cloneMap 对 JSON 友好的 map/slice 做深拷贝，保证并行分支数据互不污染。
func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := cloneValue(in)
	if out == nil {
		return nil
	}
	return out.(map[string]any)
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, val := range t {
			cp[k] = cloneValue(val)
		}
		return cp
	case []any:
		cp := make([]any, len(t))
		for i, val := range t {
			cp[i] = cloneValue(val)
		}
		return cp
	default:
		// 基础类型不可变，直接返回。
		return v
	}
}

// buildStepStates 依据定义构建与结构一一对应的初始步骤状态树。
// path 形如 "/0"，容器子节点形如 "/1/0"，是步骤在定义中的稳定坐标，重启后仍可对齐。
func buildStepStates(steps []workflow.Step, parentPath string) []workflow.StepState {
	states := make([]workflow.StepState, 0, len(steps))
	for i := range steps {
		s := &steps[i]
		path := parentPath + "/" + strconv.Itoa(i)
		st := workflow.StepState{
			Path:   path,
			Name:   s.Name,
			Kind:   s.Kind,
			Status: workflow.StepPending,
		}
		if s.Kind != workflow.KindActivity {
			st.Children = buildStepStates(s.Children, path)
		}
		states = append(states, st)
	}
	return states
}
