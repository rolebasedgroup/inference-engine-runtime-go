# Inference Engine Runtime (Go)

Inference Engine Runtime 是一个伴随单个推理实例运行的 Go 进程（下文称 runtime 进程），也可以作为库嵌入你自己的进程。它做三件事：

1. 观察编排系统（负责创建、更新、删除推理实例的控制器，例如 RBG）发出的信号；
2. 执行你用 Go 写好、编译进二进制的回调；
3. 上报本进程的健康状态与指标。

本文面向为推理服务编写生命周期动作的 Go 开发者。除“实验性 RoleInstance 信号源”一节外，不需要 Kubernetes 背景。

## 解决什么问题

推理服务在更新、恢复、就绪、删除前后需要执行业务动作：把实例从路由摘除并等待在途请求排空、就绪后注册路由、删除前交出需要存活引擎的资源。把这些动作写进编排系统，业务逻辑会进入控制器；用脚本临时执行，则没有重试和完成确认。

本项目把动作留在业务侧的 Go 代码里，由信号触发。先约定四个名词：

- **推理引擎**：SGLang、vLLM 这类提供推理服务的进程。本项目不是推理引擎，首版也不操作它们。
- **RoleInstance**：RBG（Role-Based Group）项目的自定义资源，表示一个推理实例。
- **信号**：RoleInstance 上的一个 `key=value` 标签，含义是“编排系统在这个阶段等待外部完成某事”。
- **回调**：编译进二进制的 Go 函数，签名 `func(context.Context) error`；返回 nil 表示动作完成，返回 error 表示未完成、会被重试。

一个端到端的例子（使用实验性的 RoleInstance 信号源）：

1. 编排系统准备更新某个推理实例，给它的 RoleInstance 打上标签 `example.com/update-block=true`（信号）；
2. 你部署的 runtime 进程观察到信号，运行注册的 `prepare-update` 回调：调用 router API 摘除实例、等待在途请求排空；
3. 回调成功后，runtime 进程清除这个标签；编排系统看到标签消失，才开始真正的更新；
4. runtime 进程收到 SIGTERM 时，先取消并等待当前回调返回，再依次运行 end 回调（例如恢复路由），然后退出。

当前版本交付的是这套执行机制：回调注册与绑定、串行执行、超时与重试、退出流程、诊断端点，以及实验性的 RoleInstance 信号源。引擎探测、指标转换等能力不在当前范围。

## 工作方式

```text
编排系统 (RBG RoleInstance)
    │  信号（实验性信号源）
    ▼
runtime 进程
    信号源 → 回调引擎 → 注册的回调（业务动作）
    start / hook / end 三类绑定，串行执行，超时 / 重试 / 取消
    ▲ SIGTERM 或 context 取消 → 退出流程 + end 回调
    └─ /healthz  /readyz  /metrics
```

三类绑定：

- **start 回调**：进程启动且信号源完成首次同步（读到目标对象的第一份状态）后，按配置顺序各执行一次；
- **hook 回调**：信号出现时执行；成功后由 runtime 进程确认（清除信号）；失败则按退避持续重试，不自动跳过；
- **end 回调**：退出时按配置顺序执行，使用独立的限时 context，不与已取消的运行 context 混用。

同一时刻最多运行一个回调；回调超时或被取消不算成功，panic 转为失败。

## 快速开始

```go
import (
    "github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
    "github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime/event"
)

registry := runtime.NewDefaultRegistry()                  // 内置 noop
err := registry.Register("prepare-update", prepareUpdate) // 你的函数

events, err := event.LoadEvents(reader) // events.yaml，见下

rt, err := runtime.New(events, registry) // 启动前完成全部校验
err = rt.Run(ctx)                        // 阻塞直到 ctx 取消；执行 start/hook/end
```

完整示例见 `examples/custom-runtime`。

## events.yaml

```yaml
event_callbacks:
  start: []
  hook:
    - signal: "example.com/update-block=true"
      callback: prepare-update
  end:
    - cleanup
```

加载使用严格模式：未知字段、重复 YAML key、重复 signal、未注册的回调名都会在启动前报错，不会执行到一半才发现配置问题。

## 标准进程

```sh
go build ./cmd/patio
./patio validate --config examples/events.yaml  # 只校验配置，不执行回调
./patio run --config /etc/patio/events.yaml
```

诊断端点：`/healthz` 反映进程存活；`/readyz` 在初始化、信号源首次同步、start 回调都完成后返回 200，它不代表推理引擎就绪；Prometheus 指标在 `/metrics`。

内置动作目前只有 `noop`（空操作，用于联调验证）。没有编译进二进制的名字在启动时报错——events.yaml 只能选择已注册的动作，不能凭空创建。

## 包组织

```text
pkg/runtime/            # 运行时主包：Runtime、生命周期、执行策略、信号源契约
├── callback/           # Callback 类型与注册表
├── event/              # events.yaml 绑定模型与加载
├── plugin/noop/        # 内置 noop 回调（编译期注册）
└── source/rbg/         # 实验性 RoleInstance 信号源
cmd/patio/              # 标准二进制入口
internal/diagnostics/   # 本仓库私有：日志、指标、健康端点
examples/  deploy/  test/integration/
```

## 实验性 RoleInstance 信号源

`pkg/runtime/source/rbg` 把 RoleInstance lifecycle-hook 协议草案（KEP-527；KEP 指 Kubernetes Enhancement Proposal，见 sgl-project/rbg PR #546）适配为信号源：

协议草案中与本节相关的两个名词：

- **hook-state 标签**：RoleInstance 的标签 `rbg.workloads.x-k8s.io/role-instance-hook-state`，取值 PreUpdate、Updating、PreRecover、Recovering、PostReady、PreDelete、Terminating，表示当前所处的阶段；
- **`spec.lifecycle`**：RoleInstance 的字段，声明每个阶段使用的信号标签（`key=value` 集合）。

适配器的行为：

- 观察即快照，两种模式：API 模式（默认）用限定单对象的 informer watch（LIST/WATCH 带 `metadata.name` field selector）维护观察结果，Snapshot 读取 watch 缓存，不做逐次 GET；文件模式（`--label-file`）轮询 downwardAPI 投影的 Pod label 文件（controller 会把信号 label 从 RoleInstance 镜像到成员 Pod），Snapshot 就是文件内容，确认前不访问 API server，RBAC 收窄为 get/patch。快照是最终一致的：watch 延迟、轮询间隔或镜像传播；
- API 模式的信号可执行需同时满足：hook-state 标签为 PreUpdate / PreRecover / PostReady / PreDelete 之一，`spec.lifecycle` 声明了该 `key=value`，且 RoleInstance 上实际存在这个标签；文件模式退化为“Pod label 存在且值匹配”——文件里没有 hook-state 和声明，配置变更窗口内可能迟到执行，由回调幂等契约覆盖；
- 确认是唯一的强一致步骤：重新 GET，核对 UID（对象的唯一标识）、hook-state、声明都未变化后，用带 resourceVersion 的 merge patch 只删除自己负责的标签；写冲突（409）时重读重试；任何不匹配都放弃写回，不清除别人的信号；对象已被删除时返回 stale，不无限重试。确认或失效后的信号在重放缓存 TTL（默认 30 秒）内不再出现在快照里，覆盖观察滞后造成的重放窗口。

注意：该协议还没有任何已发布的 RBG controller 实现。这个适配器只通过了 `test/integration` 中测试 CRD fixture 的验证，不能视为与真实集群版本兼容。RBAC 与部署示例见 `deploy/`。

## 开发

```sh
go test ./...                                     # 单元测试（内存信号源）
go test -race ./...
go test -tags integration ./test/integration/... # envtest 集成测试，需要 KUBEBUILDER_ASSETS
```

Go 基线版本：1.25。
