package workflow

// RetryableError 标记一次活动失败是可重试的。
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error { return e.Err }

// NonRetryableError 标记一次活动失败是不可重试的（直接进入失败终态）。
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable error"
	}
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error { return e.Err }

// ErrSimulatedCrash 供测试模拟「进程在活动执行中被杀掉」：
// 引擎收到该错误后立即停止本次执行、不写入任何终态（步骤停留在 running），
// 随后可用同一个持久化目录启动新引擎来验证断点续跑。
var ErrSimulatedCrash = newSentinel("simulated process crash during activity")

type sentinel string

func newSentinel(msg string) error { return sentinel(msg) }

func (s sentinel) Error() string { return string(s) }
