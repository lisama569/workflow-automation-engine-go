package workflow

import (
	"encoding/json"
	"time"
)

// StepKind 描述步骤的执行拓扑。
type StepKind string

const (
	// KindSerial 串行容器：子步骤按顺序执行，前一个失败则跳过后续。
	KindSerial StepKind = "serial"
	// KindParallel 并行容器：所有子步骤并发执行，全部结束后汇总结论。
	KindParallel StepKind = "parallel"
	// KindActivity 叶子活动：调用注册的活动函数。
	KindActivity StepKind = "activity"
)

// Duration 是 time.Duration 的 JSON 友好包装，支持 "500ms"、"2s" 等写法。
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// RetryPolicy 描述叶子活动的失败重试策略。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次）。0 表示不重试。
	MaxAttempts int `json:"max_attempts"`
	// InitialBackoff 首次重试前的基础等待。
	InitialBackoff Duration `json:"initial_backoff"`
	// MaxBackoff 退避上限。
	MaxBackoff Duration `json:"max_backoff"`
	// BackoffMultiplier 每次重试退避乘数。
	BackoffMultiplier float64 `json:"backoff_multiplier"`
}

// Step 是声明式工作流的节点：容器节点用 Children，叶子节点用 Activity。
type Step struct {
	Name        string       `json:"name"`
	Kind        StepKind     `json:"kind"`
	Activity    string       `json:"activity,omitempty"`
	Timeout     Duration     `json:"timeout,omitempty"`
	RetryPolicy *RetryPolicy `json:"retry_policy,omitempty"`
	Children    []Step       `json:"children,omitempty"`
}

// Definition 是一个可被触发的工作流定义。
type Definition struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// StepStatus 步骤状态机。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped" // 前置步骤失败后被跳过
	StepTimedOut  StepStatus = "timed_out"
)

// Terminal 是否为不再变化的终态。
func (s StepStatus) Terminal() bool {
	switch s {
	case StepSucceeded, StepFailed, StepSkipped, StepTimedOut:
		return true
	}
	return false
}

// Attempt 记录一次活动尝试，构成可追溯的执行链路。
type Attempt struct {
	AttemptNo int            `json:"attempt_no"`
	StartedAt time.Time      `json:"started_at"`
	EndedAt   time.Time      `json:"ended_at"`
	Input     map[string]any `json:"input,omitempty"`
	Output    map[string]any `json:"output,omitempty"`
	Error     string         `json:"error,omitempty"`
	Retryable bool           `json:"retryable"`
	TimedOut  bool           `json:"timed_out"`
}

// StepState 是单个步骤（含容器）的持久化运行态。
type StepState struct {
	Path          string         `json:"path"` // 形如 "root/2/parallel/0" 的稳定路径
	Name          string         `json:"name"`
	Kind          StepKind       `json:"kind"`
	Status        StepStatus     `json:"status"`
	Attempts      []Attempt      `json:"attempts,omitempty"`
	Output        map[string]any `json:"output,omitempty"`
	FailureReason string         `json:"failure_reason,omitempty"`
	StartedAt     *time.Time     `json:"started_at,omitempty"`
	EndedAt       *time.Time     `json:"ended_at,omitempty"`
	Children      []StepState    `json:"children,omitempty"`
}

// RunStatus 工作流实例状态机。
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// RunState 是一次触发（triggerId）对应的完整持久化状态。
type RunState struct {
	TriggerID     string         `json:"trigger_id"`
	WorkflowName  string         `json:"workflow_name"`
	Status        RunStatus      `json:"status"`
	Input         map[string]any `json:"input,omitempty"`
	Steps         []StepState    `json:"steps"`
	FailureReason string         `json:"failure_reason,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	TerminatedAt  *time.Time     `json:"terminated_at,omitempty"`
	Epoch         int            `json:"epoch"` // 持久化版本号，每次保存 +1
}

// Terminal 是否为不再变化的终态。
func (s RunStatus) Terminal() bool {
	return s == RunSucceeded || s == RunFailed
}
