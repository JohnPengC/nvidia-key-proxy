# NVIDIA API Key 负载均衡代理

一个用 Rust 编写的轻量级高性能代理，管理多个 NVIDIA API Key，提供按 Key 限流、健康检查和 Prometheus 可观测性。

## 功能特性

- **多 Key 负载均衡** — 轮换使用多个 Key，规避单 Key 速率限制。
- **自动限流检测** — 429/403（带 `Retry-After`）冷却该 Key；401（无 `Retry-After`）触发换 Key 重试，连续失败则禁用该 Key。
- **健康检查** — 启动时探测 + 定期恢复检查，保持 Key 池健康。
- **可观测性** — Prometheus 指标 + 结构化 tracing 日志。
- **安全** — 真实 Key 不出进程；日志中 Key 以 `XXXX***XXXX` 格式脱敏。
- **并发限制** — 逐模型信号量限制并发上游请求（防止 503 "Worker local total request limit reached"）；队列超时返回 503 + `Retry-After`。
- **空壳检测与重试** — 检测 NVIDIA 静默 200 软失败（`usage:null`），自动 clamp `max_tokens`、指数退避重试（默认 2 次、500ms 起步）、可选上游健康探针、逐模型熔断。
- **网络韧性** — 可配置连接池（空闲 90s、连接超时 5s）、重试抖动（默认 100ms）、网络错误计入指标。
- **流式超时** — 独立的 `request_timeout_streaming_seconds` 支持长生成任务。
- **请求归一化** — 合并非法 `max_tokens`/`reasoning_effort`/`tools` 字段而非硬 400；重映射模型别名；注入 `stream_options.include_usage`。
- **配置校验** — 启动即校验占位 `base_url`/Key，过短超时告警。
- **max_tokens 校验** — 丢弃非法 `max_tokens`（≤0、>100k、非数字、负数字符串），不传给上游。
- **Empty-shell 重试** — 检测 NVIDIA 静默软失败（`choices:[]`、`usage:null`），自动 clamp `max_tokens`，指数退避重试（默认 2 次，500ms 起步），可选上游健康探针，逐模型熔断。
- **并发限制** — 逐模型信号量防止上游 worker 过载（如 nemotron-3-ultra 32 worker 限制）；队列超时返回 503 + `Retry-After`。
- **网络韧性** — 可配置连接池（空闲 90s、连接 5s）、重试抖动（默认 100ms）、网络错误指标。
- **请求归一化** — 合并无效 `max_tokens`/`reasoning_effort`/`tools` 字段而非硬 400；重映射模型别名；注入 `stream_options.include_usage`。
- **配置校验** — 启动时拦截占位 `base_url`/Key，过短超时告警。
- **流式超时** — 独立的 `request_timeout_streaming_seconds` 供长生成使用。
- **max_tokens 校验** — 提前丢弃无效 `max_tokens`（≤0、>100k、非数字、负数字符串）。

## 快速开始

1. **克隆并编译**
   ```bash
   git clone <仓库地址>
   cd nvidia-key-proxy
   cargo build --release
   ```

2. **配置** （项目根目录 `config.toml`）：
   ```toml
   listen_addr = "0.0.0.0:8888"
   base_url    = "https://integrate.api.nvidia.com"

   # API Key — 也可通过 NVIDIA_KEYS 环境变量设置（逗号分隔）
   keys = [
       "nvapi-xxxxxxxxxxxxxxxx1",
       "nvapi-xxxxxxxxxxxxxxxx2",
   ]

   [rate_limiter]
   qps_per_key = 0.65   # 每 Key 每秒请求数
   burst       = 8      # 令牌桶大小

   [cooldown]
   base_seconds = 60    # 初始冷却秒数
   max_seconds  = 600   # 冷却上限（指数退避封顶）

   [health_check]
   interval_seconds = 30            # 坏 Key 恢复检查间隔
   timeout_seconds  = 5             # 探测超时
   max_consecutive_failures = 3     # 连续失败多少次后冷却

   # 请求超时（秒）。设为 0 关闭（流式建议关闭）。
   # 非流式请求使用此超时进行端到端控制。流式请求完全绕过此超时
   # （长 token 生成可能超过 60s）。默认调大至 600s 以支持长上下文非流式补全。
   request_timeout_seconds = 600

   # 可选：流式连接单独的超时（连接 + 空闲读取）。
   # > 0 时用于流式路径，替代 request_timeout_seconds。
   # 0 = 复用 request_timeout_seconds（兼容旧版）。默认 0（关闭）。
   # request_timeout_streaming_seconds = 0

   # max_tokens 校验上限。请求 max_tokens 超过此值会直接丢弃该字段（使用模型默认值）。
   # 默认 100000。可用 MAX_TOKENS_MAX 环境变量覆盖。
   # max_tokens_max = 100000

   # NVIDIA "empty-shell" 软失败重试：上游返回 200 但 body 为
   # `{"choices":[],"id":"","usage":null,...}`（未真正跑推理；常见于
   # `/compact` 且 max_tokens 超过模型剩余上下文预算）。
   # 重试一次，将 max_tokens 夹紧到 `empty_shell_retry_max_tokens`，失败则
   # 抛出 502。没有此机制，Claude Code 的 `/compact` 会误报
   # "summarization produced empty response"。默认开启；设
   # NVIDIA_RETRY_ON_EMPTY_SHELL=false 关闭。
   retry_on_empty_shell = true

   # 重试时使用的 max_tokens（原请求原样转发，仅重试受影响）。
   # 8192 在 build.integrate 的 GLM 系模型上较保守。
   # 环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS。
   empty_shell_retry_max_tokens = 8192

   # 可选：empty-shell 软失败的重试次数。
   # 1 = 一次重试（原始 + 一次重试）。0 = 不重试（直接 502）。
   # 默认 1。环境变量：NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS。
   # empty_shell_retry_attempts = 1

   # 可选：重试前先探测 /v1/models，避免向已退化的上游重试。
   # 默认 true。环境变量：NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY。
   # empty_shell_probe_before_retry = true

   # 可选：熔断阈值。同一 key/model 连续 N 次 empty-shell/504 后，
   # 该 key 冷却 cooldown.base_seconds。0 = 禁用。默认 3。
   # 环境变量：NVIDIA_CIRCUIT_BREAKER_THRESHOLD。
   # circuit_breaker_threshold = 3

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"
   ```

3. **运行**
   ```bash
   ./target/release/nvidia-key-proxy
   ```

4. **接入客户端** — 将 `base_url` 指向 `http://localhost:8888`。`api_key` 字段可填任意值，代理会用池中的真实 Key 替换。

## 配置说明

### 顶层字段

| 字段 | 说明 | 默认值 |
|---|---|---|
| `listen_addr` | 监听地址 | 必填 |
| `base_url` | NVIDIA API 地址 | 必填 |
| `keys` | NVIDIA API Key 列表 | 必填（或 `NVIDIA_KEYS` 环境变量） |
| `request_timeout_seconds` | 非流式请求超时；0 关闭（流式始终绕过） | `600` |
| `request_timeout_streaming_seconds` | 流式请求超时；0 = 复用非流式超时或关闭 | `0` |
| `retry_on_empty_shell` | NVIDIA empty-shell 软失败时重试 | `true` |
| `empty_shell_retry_max_tokens` | empty-shell 重试时的 max_tokens 夹紧值 | `8192` |
| `empty_shell_retry_attempts` | empty-shell 重试次数（0 = 不重试） | `1` |
| `empty_shell_probe_before_retry` | 重试前探测 /v1/models | `true` |
| `circuit_breaker_threshold` | 连续 shell/504 多少次后冷却（0 = 禁用） | `3` |
| `max_tokens_max` | max_tokens 校验上限；超出则丢弃字段 | `100_000` |
| `allow_local_base_url` | 允许 loopback/localhost base_url（仅测试） | `false` |
| `auth` | 前门+管理鉴权配置（见下） | 禁用 |

### `[rate_limiter]` 速率限制

| 字段 | 说明 |
|---|---|
| `qps_per_key` | 每 Key 每秒查询数 |
| `burst` | 令牌桶容量 |

### `[cooldown]` 冷却

| 字段 | 说明 |
|---|---|
| `base_seconds` | 初始冷却时长 |
| `max_seconds` | 冷却上限（指数退避封顶） |

### `[health_check]` 健康检查

| 字段 | 说明 |
|---|---|
| `interval_seconds` | 坏 Key 恢复检查间隔 |
| `timeout_seconds` | 探测超时 |
| `max_consecutive_failures` | 连续失败多少次后进入冷却 |

### `[observability]` 可观测性

| 字段 | 说明 |
|---|---|
| `enable_metrics` | 是否暴露 Prometheus 指标端点 |
| `metrics_path` | 指标端点路径（默认 `/metrics`） |
| `log_level` | tracing 日志级别（也可通过 `RUST_LOG` 覆盖） |

### `[retry]` 重试

| 字段 | 说明 | 默认值 |
|---|---|---|
| `max_retries` | 可重试错误的最大重试次数 | `3` |
| `backoff_base_ms` | 初始退避毫秒 | `200` |
| `backoff_max_ms` | 退避上限毫秒 | `2000` |
| `auth_fail_threshold` | 连续鉴权失败多少次后禁用 Key | `3` |

## 配置校验

代理在启动时会对加载的配置进行校验，若发现明显错误会立即退出，避免在占位配置下静默产生 401/404 失败：

- `base_url` 不能是 `example.com`、`localhost` 或 `127.0.0.1`，且必须以 `http://` 或 `https://` 开头。
- API Key 长度至少为 8，且不能包含 `fake`、`dummy`、`test`、`example`、`placeholder`。
- 若 `request_timeout_seconds` 小于 `5`，会打印警告，建议使用更大的值（推荐 30s+ 非流式）。

## 基于 failure.log 的可靠性增强 (2026‑07‑23)

分析 `failure.log` 后针对四大错误类别实施修复：

| 错误类别 | 占比 | 根因 | 修复 |
|---|---|---|---|
| `empty_choices_shell_first` | 41.9% | NVIDIA 返回 200 但 `choices:[]`、`usage:null`、`id:""` —— 未跑推理。 | **重试并夹紧 `max_tokens`**（配置 `empty_shell_retry_max_tokens`，默认 8192）。重试前可选探测上游 `/v1/models`（`empty_shell_probe_before_retry`）。可配置重试次数（`empty_shell_retry_attempts`，默认 1）。返回可行动的 502，而非误导性的 "summarization produced empty response"。 |
| `timeout` | 37.2% | 300s 硬超时对长上下文非流式太短。 | **默认 `request_timeout_seconds` 提升至 600s**。流式请求完全绕过。新增 `request_timeout_streaming_seconds` 单独配置流式超时。警告阈值提至 30s。 |
| `upstream_5xx` (504) | 14.0% | 流式网关超时。 | Client `connect_timeout` (5s) 不变；请求级超时仅对非流式生效。流式路径默认无界。 |
| `upstream_4xx` (400 `max_tokens < 1`) | 2.3% | 客户端计算 `max_tokens` 下溢（大上下文 + 预留 token）。 | **代理丢弃无效 `max_tokens`**（≤0、>100k、非数值）。上限可配置 `max_tokens_max`（默认 100_000）或 `MAX_TOKENS_MAX` 环境变量。 |

## 请求校验

在转发 chat-completion 请求前，代理会校验已知会导致上游 400 的请求体字段：

- `max_tokens`：若存在，必须是大于等于 1 的整数。
- `reasoning_effort`：若存在，必须是 `"low"`、`"medium"`、`"high"` 之一。

非法请求会立即以 400 返回，不会进入上游 Key 池，避免浪费重试。

## Prometheus 指标

当 `observability.enable_metrics` 为 `true` 时，指标暴露在 `observability.metrics_path`（默认 `/metrics`）。

| 指标名 | 说明 |
|---|---|
| `proxy_requests_total` | 总请求数 |
| `proxy_requests_success_total` | 2xx 请求数 |
| `proxy_requests_errors_total` | 非 2xx 请求数 |
| `proxy_request_duration_seconds` | 延迟直方图 |
| `proxy_key_active` | 活跃 Key 数 |
| `proxy_key_cooldown` | 冷却中 Key 数 |
| `proxy_key_disabled` | 禁用 Key 数 |
| `proxy_tokens_input_total` | 累计输入 token 数（prompt tokens） |
| `proxy_tokens_output_total` | 累计输出 token 数（completion tokens） |
| `proxy_tokens_total` | 累计 token 总数（prompt + completion） |

## API 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `{metrics_path}` | Prometheus 指标 |
| GET | `/health` | 健康检查（返回 204） |
| * | `/*` | 透传到 NVIDIA API（注入 Key + 限流） |

## 架构

代理按职责拆分为 `src/lib.rs` 下的多个模块：

- **`config`** — 加载 `config.toml` / 环境变量；`Debug` 实现对 API Key 脱敏。
- **`key_pool`** — 维护每个 Key 的状态（active / cooling / disabled），加权随机选择。
- **`proxy`** — warp handler：获取 Key → 限流 → 转发 → 更新 Key 状态 → 流式回传响应。
- **`health`** — 启动探测 + 定期坏 Key 恢复。
- **`metrics`** — Prometheus 计数器和直方图。
- **`failure_log`** — 结构化 JSONL 失败日志（`failure.log`）。

`AppState` 把共享运行时对象（metrics、key_pool、proxy、health_checker、stop_signal、config）打包，`build_routes` 构建 warp 过滤器，使集成测试无需进入 `main` 即可测试整条链路。

## 实现重构 (2026-07-11)

- 引入 **`AppState`** 将共享运行时对象统一管理。
- 将 tracing 初始化抽离为 **`init_tracing`**，确保启动顺序清晰。
- 将路由构建抽象为 **`build_routes`**，`main` 只负责 orchestration。
- 使用单一 `Notify` 实现优雅关闭，关机时直接 `drop` 背景任务句柄，避免死锁。
- 在 `tests/` 新增集成测试，覆盖健康接口、指标端点（启用/禁用）以及未知路径 404。

## 校验与加固 (2026-07-16)

- 新增 **配置校验**（`Config::validate`），在启动时快速识别占位 `base_url`/Key，并对过短的请求超时发出警告。
- 在代理层新增 **请求体校验**，对 `max_tokens` 和 `reasoning_effort` 进行前置检查，避免非法 OpenAI 风格请求到达 NVIDIA。
- 补充单元测试覆盖上述两层校验。

## 测试

```bash
cargo test --release
```

单元测试覆盖 Key 池状态转换、令牌桶正确性、代理重试/超时行为。集成测试覆盖健康接口、指标端点（启用/禁用）以及未知路径 404。

## 许可证

MIT

## 技术栈

Tokio、Warp、reqwest、serde、tracing。