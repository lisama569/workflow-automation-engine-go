// Package mockservice 用进程内存储模拟一个带幂等语义的外部服务：
// 相同幂等键的重复请求只会生效一次，且可被配置为前几次失败以验证重试。
package mockservice

import (
	"fmt"
	"sync"
)

// Service 模拟外部依赖（如支付、通知、库存等）。
type Service struct {
	mu sync.Mutex
	// records 记录每个幂等键的调用结果。
	records map[string]record
}

type record struct {
	result        CallResult
	fatal         bool
	failRemaining int // 还需返回多少次可重试失败
	attempts      int // 真实进入服务的次数（含失败）
	effects       int // 真实产生副作用的次数
}

// New 创建模拟服务。
func New() *Service {
	return &Service{records: map[string]record{}}
}

// CallOptions 控制一次模拟调用的行为。
type CallOptions struct {
	// FailTimes 该幂等键前 FailTimes 次调用返回可重试错误（模拟瞬时故障）。
	FailTimes int
	// Fatal 为 true 时返回不可重试错误（模拟业务拒绝）。
	Fatal bool
}

// CallResult 是模拟调用的返回。
type CallResult struct {
	OrderID string `json:"order_id"`
	Effect  string `json:"effect"`
}

// Call 以幂等键调用模拟服务；重复键返回首次结果，不重复产生副作用。
func (s *Service) Call(idempotencyKey string, opts CallOptions) (CallResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, seen := s.records[idempotencyKey]
	if !seen {
		// 首次见到该幂等键，登记其预期行为。
		rec = record{failRemaining: opts.FailTimes, fatal: opts.Fatal}
	}
	rec.attempts++

	if rec.effects > 0 {
		// 已经成功过：直接重放首次结果，绝不重复产生副作用。
		s.records[idempotencyKey] = rec
		return rec.result, nil
	}
	if rec.fatal {
		s.records[idempotencyKey] = rec
		return CallResult{}, fmt.Errorf("fatal rejection for %s", idempotencyKey)
	}
	if rec.failRemaining > 0 {
		rec.failRemaining--
		s.records[idempotencyKey] = rec
		return CallResult{}, fmt.Errorf("transient failure, %d flaky attempt(s) left for %s",
			rec.failRemaining, idempotencyKey)
	}

	// 真正生效，副作用只发生一次。
	rec.effects++
	rec.result = CallResult{OrderID: "order-" + idempotencyKey, Effect: "created"}
	s.records[idempotencyKey] = rec
	return rec.result, nil
}

// Attempts 返回某幂等键真实进入服务的次数。
func (s *Service) Attempts(idempotencyKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[idempotencyKey].attempts
}

// SideEffectCount 返回某幂等键真实产生副作用的次数（正常永远 <= 1）。
func (s *Service) SideEffectCount(idempotencyKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[idempotencyKey].effects
}
