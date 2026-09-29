# 本地工作流编排引擎（Go）

一个纯本地运行、零外部中间件依赖的工作流编排引擎。外部依赖（持久化存储、
幂等下游服务）全部用本地文件 + 进程内结构模拟，目标是让**同一个工作流在
并发触发、重复投递、失败重试、进程崩溃恢复下都产生确定、可解释的结果**。

## 1. 能力概览

- 声明式定义工作流：`serial`（串行容器）/ `parallel`（并行容器）/ `activity`（叶子活动），可任意嵌套。
- 触发去重：以调用方提供的 `triggerID` 为幂等键；并发触发、执行中重复投递、
  终态后补投、进程重启后补投，都只会产生**一次**实际执行。
- 持久化与断点续跑：每一次状态变化都立即落盘（本地 JSON 文件，临时文件 + rename 原子写）；
  重启后已完成步骤不重跑，未完成步骤从稳定路径（step path）对应的位置继续。
- 失败分级与重试：区分 `RetryableError` 与 `NonRetryableError`，支持指数退避、
  单步超时；超过重试上限或超时后进入确定终态（`failed` / `timed_out`）。
- 全链路可追溯：每个步骤的每次尝试都记录输入快照、输出、错误原因、
  是否可重试、是否超时、开始/结束时间，随运行态一起持久化。
- 并行隔离：并行分支各自拿到上游数据的**深拷贝**视图，分支之间无法互相污染；
  全部结束后按稳定 path 合并输出。

## 2. 包结构

| 包 | 职责 |
| --- | --- |
| `pkg/workflow` | 声明式定义、运行态模型、可重试/不可重试错误类型 |
| `pkg/activity` | 活动函数签名与注册表；活动上下文携带稳定幂等键 |
| `pkg/store` | 运行态持久化接口与 `FileStore`（JSON 文件原子写） |
| `pkg/engine` | 编排引擎：去重闸门、断点续跑、串并行调度、重试/超时 |
| `pkg/mockservice` | 模拟带幂等语义的外部下游服务，可统计真实副作用次数 |
| `cmd/demo` | 本地端到端演示 |
| `examples/order-pipeline.json` | 声明式工作流 JSON 样例 |

## 3. 如何定义一个工作流

### 3.1 Go 代码方式

```go
def := workflow.Definition{
    Name: "order-pipeline",
    Steps: []workflow.Step{
        {Name: "validate", Kind: workflow.KindActivity, Activity: "validate"},
        {
            Name: "fanout",
            Kind: workflow.KindParallel, // 两个分支并发
            Children: []workflow.Step{
                {Name: "risk_check", Kind: workflow.KindActivity, Activity: "risk_check"},
                {
                    Name: "reserve_inventory", Kind: workflow.KindActivity,
                    Activity: "reserve_inventory",
                    Timeout:  workflow.Duration(5 * time.Second),
                    RetryPolicy: &workflow.RetryPolicy{
                        MaxAttempts:       3,               // 含首次，共最多 3 次
                        InitialBackoff:    workflow.Duration(20 * time.Millisecond),
                        MaxBackoff:        workflow.Duration(100 * time.Millisecond),
                        BackoffMultiplier: 2,               // 20ms -> 40ms -> 80ms（封顶 100ms）
                    },
                },
            },
        },
        {
            Name: "serial-cleanup", Kind: workflow.KindSerial,
            Children: []workflow.Step{ /* 按顺序执行的子步骤 */ },
        },
    },
}
```

### 3.2 JSON 声明方式

时长使用 `"20ms"` / `"5s"` 等字符串。完整样例见
[`examples/order-pipeline.json`](examples/order-pipeline.json)，其解析由
`workflow.Duration` 的 `UnmarshalJSON` 支持。

### 3.3 步骤稳定路径（step path）

定义中的每个步骤都有一个由索引构成的稳定坐标，如 `/0`、`/1/1`、`/2/0`。
它的用途：

1. 重启后把磁盘上的步骤状态与定义重新对齐，决定「从哪里继续」；
2. 作为活动输出在数据视图中的 key（`/0` -> 输出），并行合并时不冲突；
3. 与 `triggerID` 一起组成活动的幂等键。

> 因此同一个 `triggerID` 的工作流定义应当保持不变；变更定义请使用新的 trigger 命名空间。

## 4. 活动与幂等规则

活动签名：

```go
type Func func(ctx context.Context, ac activity.Context) (map[string]any, error)
```

`activity.Context` 提供：

- `Input`：本次触发的原始输入（深拷贝）；
- `Data`：此前已成功步骤的输出视图，key 为步骤 path（并行分支内是隔离副本）；
- `IdempotencyKey`：`triggerID + ":" + stepPath`，**在重试、恢复、重复投递下永远不变**。

### 4.1 幂等约定

- 引擎保证：同一个 `(triggerID, stepPath)` 的活动**至多成功落账一次**；
  已成功的步骤在重试/恢复/重复投递中不会再被调用。
- 活动对真正的外部副作用（扣款、发货等）必须用 `ac.IdempotencyKey` 做下游去重：
  因为「请求已到达下游、但响应在返回前丢失」时，引擎会重试，
  下游必须能识别同一个幂等键并回放首次结果。
- `pkg/mockservice.Service` 就是这种下游的参考实现：相同 key 重复调用只产生一次副作用，
  并提供 `SideEffectCount(key)` 供测试断言。

### 4.2 触发去重规则

- `triggerID` 是必传的投递幂等键（建议由上游用业务单号 + 操作类型生成）。
- 并发触发同一个 `triggerID`：持久化层 `CreateIfAbsent` 是去重闸门，只有一个成为执行者，
  其余调用在进程内等待终态，然后返回同一份结果。
- 执行中重复投递：等待者不执行任何步骤。
- 终态后重复投递（含进程重启后的补投）：直接识别并回放终态，不触碰任何活动。
- 同一 `triggerID` 绑定了不同工作流名：返回冲突错误。

## 5. 失败、重试与超时的确定约定

| 失败来源 | 是否重试 | 步骤终态 |
| --- | --- | --- |
| `workflow.NonRetryableError` | 否，立即结束 | `failed` |
| `workflow.RetryableError` | 是，直到 `MaxAttempts` | 预算内成功则 `succeeded`，否则 `failed` |
| 普通 error（未包装） | 默认按可重试处理 | 同上 |
| `context.DeadlineExceeded`（单步超时） | 是，超时计入尝试次数 | 耗尽预算后为 `timed_out` |
| `panic` | 否 | 该步骤 `failed`，run 兜底为 `failed` 终态 |

步骤失败后的传播约定（全引擎一致）：

1. **串行容器**：某子步骤失败/超时后，其后所有尚未开始的兄弟步骤标记为 `skipped`
   （`FailureReason` 记录跳过原因），容器结论为 `failed`。
2. **并行容器**：不做提前取消，所有分支都跑到各自结束；只要有一个分支失败，
   容器结论为 `failed`，但成功分支的结果仍然保留。
3. 顶层任一步骤失败后，后续顶层步骤 `skipped`，run 进入 `failed` 终态；
   `RunState.FailureReason` 与失败步骤的 `Attempts` 共同给出完整原因链。

超时是**单次尝试**级别的（每次重试重新计时）。若需要「整个工作流」级别的截止时间，
可在活动内基于输入时间戳判断，或在外层触发时控制。

## 6. 持久化与断点续跑

- 存储位置：`FileStore` 目录下每个 `triggerID` 一个 JSON 文件，写入采用
  「写临时文件 + fsync + rename」，崩溃不会留下半截文件。
- 落盘时机：run/容器/活动每一次状态迁移（running、每次 attempt、终态）都立即保存，
  并推进 `Epoch` 版本号。
- 恢复算法：重新触发同一个 `triggerID` 时，引擎加载状态树——
  - `succeeded/failed/skipped/timed_out` 的步骤直接采用持久化结论，不再执行；
  - 崩溃时停留在 `running` 的步骤视为「上次未确认」，重新执行一次
    （这正是必须依赖下游幂等键的原因）；
  - `pending` 步骤按定义继续。
- 测试中用 `workflow.ErrSimulatedCrash` 确定性模拟「进程在活动执行中被杀」，
  见 `TestResumeAfterSimulatedCrash`。

## 7. 本地验证方法

要求 Go 1.25+。

```bash
# 端到端演示（含并发重复投递、瞬时失败重试，打印结构化日志与最终结论）
go run ./cmd/demo

# 全量单测（-race 必开，并发去重是核心场景）
go test -race -count=1 ./...

# 只看引擎测试的详细日志（触发标识、步骤状态、最终结论都在日志里）
go test -race -count=1 -v ./pkg/engine/

# 压几遍确认并发稳定性
go test -race -count=5 ./...
```

演示与测试的日志统一打印 `trigger_id`、`step`（稳定路径）、`name`、
`attempt`、`retryable`、`timed_out`、步骤 `status` 与 run 的最终 `status`。

## 8. 关键测试场景索引

| 测试 | 验证内容 |
| --- | --- |
| `TestConcurrentTriggersExecuteOnce` | 8 个 goroutine 同 triggerID 并发触发，每个活动恰好执行 1 次 |
| `TestDuplicateTriggerWhileInFlight` | 执行进行中重复投递：等待终态，活动不被二次进入 |
| `TestRedeliveryAfterTerminal` | 终态后（含新引擎实例）补投：直接回放，零重复执行 |
| `TestResumeAfterSimulatedCrash` | 模拟进程崩溃后重启：已完成步骤不重跑，中断步骤继续 |
| `TestRetryableFailureSucceedsWithinBudget` | 前 2 次瞬时失败、第 3 次成功；下游副作用恰好 1 次 |
| `TestNonRetryableFailsImmediately` | 不可重试错误不重试，后续步骤 skipped |
| `TestExhaustRetryBudgetFails` | 重试耗尽进入确定 failed 终态 |
| `TestTimeoutIsTerminalAndRecorded` | 超时可重试，耗尽后为 timed_out，尝试记录带 timed_out 标记 |
| `TestParallelFailureConvention` | 并行分支失败不取消其他分支，容器与 run 为 failed |
| `TestParallelBranchesDoNotPolluteEachOther` | 并行分支数据视图深拷贝隔离，结束后正确合并 |
| `TestAttemptTraceability` | 每次尝试的输入/输出/错误/时间戳完整可追溯 |
| `TestTriggerIDBoundToSingleWorkflow` | triggerID 与工作流名一一绑定 |
