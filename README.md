# 本地工作流编排引擎（Go）

一个完全在本地运行、用文件存储模拟外部依赖的工作流编排引擎。目标是让**同一个工作流**在
**并发触发、消息重复投递、进程崩溃重启、步骤失败重试、超时**这些混乱条件下，仍然得到
**可预期、可解释**的结果：

- 已完成的步骤**永不重复执行**；
- 重复投递被识别为同一个实例，而不是产生新的副作用；
- 可重试 / 不可重试失败界限明确，超过重试预算或超时后有确定终态；
- 每一次尝试的输入、输出、失败原因都持久化，整条链路可追溯。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `workflow/workflow.go` | 核心类型：定义、状态、尝试记录、`Store` 接口 |
| `workflow/engine.go` | 调度引擎：DAG 波次、并行、重试退避、超时、CAS 持久化、去重锁 |
| `workflow/store.go` | `MemoryStore`（测试用）与 `FileStore`（JSON + 原子改名 + `flock`） |
| `workflow/ledger.go` | 模拟外部依赖的副作用台账（业务键精确一次）与持久化计数器 |
| `workflow/activity.go` | 活动处理器注册表、`Retryable` / `NonRetryable` 错误分类 |
| `workflow/activities.go` / `flaky.go` | 内置演示活动 `noop` `flaky` `alwaysfail` `slow` |
| `cmd/wf/main.go` | 本地命令行 demo |

## 定义工作流

工作流用声明式的 `workflow.Definition` 表达：每个步骤声明自己依赖哪些步骤。
**依赖集合互不相交的步骤会并行执行**，有依赖边的步骤严格串行。

```go
def := workflow.Definition{
    Name: "order-demo",
    Steps: []workflow.StepDef{
        {Name: "validate", Activity: "noop", Input: map[string]any{"order": "A-1001"}},
        {Name: "prepare", Activity: "flaky",
            DependsOn: []string{"validate"},      // 等 validate 成功
            Input:      map[string]any{"fail_times": 2},
            MaxAttempts: 4,                       // 含首次在内最多尝试 4 次
            Timeout:     2 * time.Second,         // 单次尝试超时
            InitialBackoff: 20 * time.Millisecond}, // 首次重试退避，之后指数翻倍
        {Name: "reserve", Activity: "noop", DependsOn: []string{"validate"}}, // 与 prepare 并行
        {Name: "charge",  Activity: "noop", DependsOn: []string{"prepare", "reserve"}},
        {Name: "receipt", Activity: "noop", DependsOn: []string{"charge"}},
    },
}
```

上面的图是：`validate → (prepare ∥ reserve) → charge → receipt`。
并行分支之间**不共享任何可变状态**，每个活动只拿到自己那份输入的副本。

定义会被校验：步骤名唯一、依赖必须存在、依赖图不允许成环（包括自环）。

### 执行模型与失败约定（串行/并行完全一致）

调度按"波次"推进，每个波次把所有依赖已满足的步骤**并发**执行，并在执行任何副作用
之前先把步骤状态（`running` / `skipped`）持久化：

1. **可重试失败**（`workflow.Retryable(err)` 或尝试超时）：步骤保持 `running`，
   按指数退避后发起下一次尝试，直到成功或用完 `MaxAttempts`。
2. **不可重试失败**（`workflow.NonRetryable(err)`，以及未分类错误一律视为不可重试）：
   步骤立即进入终态 `failed`。
3. 任一步骤进入终态 `failed` 时遵循 **fail-fast（快速失败）** 约定：
   - 同波次仍在飞行的其它步骤通过 `context` 取消；
   - 尚未执行的下游步骤标记为 `skipped`（终态，尝试次数为 0，从不执行）；
   - 已经 `succeeded` 的步骤**保留结果、不回滚、不重跑**；
   - 整个工作流实例进入确定终态 `failed`，`state.Err` 记录首个失败步骤。
4. 所有步骤终态且没有失败步骤时，工作流为 `succeeded`。

终态只有三种：`succeeded` / `failed` / `skipped`（步骤级）；
工作流级终态为 `succeeded` / `failed`。

## 重试与超时规则

| 情况 | 行为 |
| --- | --- |
| 活动返回 `Retryable(err)` | 退避后重试，计入 `MaxAttempts` |
| 活动返回普通 error 或 `NonRetryable(err)` | **不**重试，立即 `failed`（必须显式选择可重试，避免把未知错误当瞬态） |
| 单次尝试超过 `Timeout` | 记为超时失败，**按可重试处理**；每次超时都尝试满预算后才终态 `failed` |
| 并行兄弟失败导致 `ctx` 取消 | 当前尝试记录为可重试取消，但 fail-fast 会直接把未完成步骤置 `skipped`/`failed` |
| 退避 | `InitialBackoff` 起步，每次失败翻倍，上限 `Config.MaxBackoff`；可被 ctx 中断 |

每次尝试都会落一条 `Attempt`：尝试序号、起止时间、**输入快照**、输出、错误文案、
是否可重试、是否超时。它们随状态一起持久化，是完整的执行轨迹。

## 幂等与重复投递规则

触发标识 `triggerID` 是一次投递的幂等键。`(workflowName, triggerID)` 唯一对应一个
工作流实例。

1. **重复投递识别**：`Engine.Run` 对同一 triggerID 先做 `CreateIfAbsent`（进程内还有
   一把按 triggerID 的执行锁）。已存在实例时调用方拿到 `Adopted=true`：
   - 实例已终态：直接返回同一终态，**不执行任何步骤**；
   - 实例未终态：等待正在执行的一方完成并读取同一终态。
2. **并发触发同一 triggerID**：同一进程内只有一个 goroutine 成为执行者，其余等待；
   不同进程之间由 `FileStore` 的 `flock` + 乐观版本号（CAS）仲裁，竞争失败者转为等待
   终态，不重复执行。
3. **副作用精确一次**：活动处理器在"完成但崩溃在落盘之前"这一瞬间可能被再次调用
   （at-least-once 调用），因此有外部副作用的活动必须用 `IdempotentActivity` 包装。
   副作用台账以确定性业务键
   `wf=<工作流>|trigger=<触发标识>|step=<步骤>[|输入字段]` 去重：
   效果函数每个键**有且仅有一次**真正执行，重试/重复投递/重启都只取回已存结果
   （输出中的 `_effect_reused=true` 表示这次是去重回放）。
4. **失败的副作用不留痕**：台账只记录成功结果，活动失败后重试会再次真正调用效果函数。

## 持久化与断点续跑

`FileStore` 把每个实例存成一个 JSON 文件：

- 写入走"临时文件 + `fsync` + 原子 rename"，崩溃不会留下半截状态；
- 每次更新都让 `Version` 加一，配合 CAS 检测并发写入；
- 每个实例另有一个 `.lock` 文件做 `flock`，跨进程串行化创建与更新。

续跑规则（`Engine.Run` 对已存在实例、或启动时 `Engine.ResumeAll`）：

- `succeeded` 步骤：跳过，绝不重跑，历史尝试原样保留；
- `pending` 且依赖已满足的步骤：正常开始；
- 上次崩溃时停留在 `running` 的步骤：其上次尝试结果未知，**追加一次新尝试**继续
  （配合幂等活动，副作用仍然只有一次）；
- 下游步骤等待依赖，从正确位置继续。

本地数据目录布局（默认 `.wfdata/`）：

```
.wfdata/
├── workflows/<workflow>/<trigger>.json   # 实例状态（含全部 Attempt）
├── workflows/<workflow>/<trigger>.lock   # 实例文件锁
└── ledger/
    ├── effects.json    # 已生效副作用（业务键 -> 结果）
    ├── counters.json   # 演示活动的持久化失败计数
    └── *.lock
```

## 本地验证

### 跑测试

```bash
go test ./... -race -v          # 全部用例，含并发与重复投递
go test ./... -race             # 只看结果
```

重点用例（单测日志通过 `t.Logf` 打印**触发标识、步骤状态、最终结论**）：

| 用例 | 验证点 |
| --- | --- |
| `TestConcurrentTriggersSameTriggerID` | 20 个 goroutine 同 triggerID 并发：1 次新建、19 次识别重复，效果恰好 2 个，终态版本号一致 |
| `TestDuplicateDeliveryWhileRunning` | 步骤执行期间重复投递 5 次，步骤只执行 1 次，等待方都看到成功 |
| `TestDuplicateDeliveryAfterCompletion` | 完成后再次投递，版本号不变、零步骤重跑 |
| `TestCrossProcessDuplicateDelivery` | 两个共享目录的引擎并发同 trigger，恰好一方执行、效果只有 1 个 |
| `TestResumeAcrossRestart` | 模拟"进程 1 完成 a 后崩溃"，新引擎从 b 继续，a 不重跑 |
| `TestResumeAllOnStartup` | 启动时扫描并恢复所有未终态实例 |
| `TestFailureConventionFailFastAndSkipped` | 不可重试失败后下游 `skipped`、已成功步骤保留 |
| `TestRetryExhaustionHasTerminalState` / `TestTimeoutIsRetryableThenTerminal` | 重试预算耗尽 / 超时的确定终态与轨迹 |
| `TestLedgerEffectAppliesExactlyOnce` | 业务键去重，效果函数恰好执行一次 |

### 跑命令行 demo

```bash
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-1 --mode=ok        # 全成功
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-2 --mode=retry     # flaky 失败 2 次后成功
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-3 --mode=fail      # 不可重试失败 + 下游 skipped
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-4 --mode=timeout   # 每次尝试超时，预算耗尽后 failed
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-5 --mode=parallel  # 4 个独立步骤并行
go run ./cmd/wf list --data-dir=.wfdata                                  # 查看实例清单
```

演示重复投递（第二条命令不会执行任何步骤，输出 `adopted=true`）：

```bash
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-1 --mode=ok
go run ./cmd/wf demo --data-dir=.wfdata --trigger=ORD-1 --mode=ok
```

演示断点续跑：工作流跑到一半时 `Ctrl+C`（或直接删掉一个未终态实例后重启），再次用
**同一个 triggerID** 触发；或新进程启动时自动 `ResumeAll`，可以看到已完成步骤被跳过、
从断点继续。

查看完整执行轨迹：

```bash
cat .wfdata/workflows/order-demo-retry/ORD-2.json   # 每个步骤的 attempts/input/output/error
cat .wfdata/ledger/effects.json                     # 真正生效过的副作用
```

## 设计取舍说明

- **状态先于副作用落盘**：步骤进入 `running` 先持久化，再执行活动。因此最坏情况是
  活动被多调用一次（at-least-once），由幂等台账收敛为"效果一次"，而不会静默丢步骤。
- **未分类错误默认不可重试**：把"是否瞬态"的判断权交给活动作者，避免对未知错误反复
  放大副作用。
- **本地存储仅模拟外部依赖**：FileStore/台账是教学级实现（单机、flock、JSON），语义
  等价于"带唯一约束的状态表 + 幂等键表"，可直接映射到真实数据库方案。
