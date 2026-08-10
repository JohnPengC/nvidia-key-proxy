# NVIDIA API Key 负载均衡代理（Rust）

## 概览

一个用 Rust 编写的轻量、高性能代理，在多个 NVIDIA API key 前做一层。
Client（如 OpenAI 兼容 SDK）用惯常的 `base_url`、`api_key`、`model` 参数和代理对话。
代理从池子里挑一个健康 key、做 per-key 限流、把请求转发到真正的 NVIDIA 端点，并把响应原样返回（含流式 SSE）。

核心目标

* **负载均衡**：跨多个 key 分发，规避单 key 限流。
* **自动限流检测** —— 429/403 触发对应 key 的冷却。
* **健康检查** —— 后台探测让池子感知坏 key。
* **可观测性** —— Prometheus 指标 + 结构化 tracing。
* **透明使用** —— 下游 client 无需改代码。
* **安全** —— 真实 key 不出进程；日志脱敏。
* **并发限制** —— per-model 信号量给并发上游请求数封顶（避免 503 "Worker local total request limit reached"）；排队超时返回 503 带 `Retry-After`。
* **空壳检测与重试** —— 检测 NVIDIA 带着静默 200 + `usage:null` 的软失败；用钳住的 `max_tokens` + 指数 backoff + 可选上游健康探测重试；per-model 断路器。
* **网络韧性** —— 可配置连接池（idle 超时 90s、connect 超时 5s）、重试抖动（默认 100ms）；网络错误进指标。
* **流式超时** —— 独立的 `request_timeout_streaming_seconds` 应对长 generation。
* **max_tokens 校验** —— 在打到 NVIDIA 之前丢掉非法 `max_tokens`（≤0、>100k、非数字、负数字符串）。

## 架构

```
+----------------+    HTTP    +------------------+    HTTP    +-------------------+
|  Client (SDK)  |  <---->  |  Rust Proxy      |  <---->  |  NVIDIA API       |
| base_url,key,  |          |  (KeyPool,RL,    |          |  integrate.api.   |
| model)         |          |   Health,Metrics)|          |  nvidia.com       |
+----------------+          +------------------+          +-------------------+
        ^                        ^   ^   ^                     ^
        |                        |   |   |                     |
        |   Config (Key list)    |   |   |   Response (stream) |
        +------------------------+   +---+---------------------+
                                    |
                                    +-- KeyPool (state per key)
                                    |
                                    +-- TokenBucket per key (rate limit)
                                    |
                                    +-- HealthChecker (startup + bad-key recovery)
                                    |
                                    +-- FailureLog (structured JSONL)
                                    |
                                    +-- Metrics / Tracing
```

### 模块

| 模块 | 职责 | 技术 |
|--------|----------------|------|
| **Config** | 加载 `config.toml` / 环境变量 / CLI。自定义 `Debug` 把 API key 在日志里脱敏。 | `serde` + `toml` + `dotenvy` |
| **KeyPool** | 维护 `KeyState`（active、cooling、disabled）。提供 `acquire()`、`mark_good()`、`mark_bad()`。Per-model 断路器计数。 | `DashMap`<br>`DashMap<KeyId, KeyState>` |
| **TokenBucket** | Per-key 限流器（QPS + burst）。 | `async-tokenbucket` 或用 `tokio::time::Interval` 自实现 |
| **Proxy Handler** | 接 HTTP 请求 → 拿 key → 等 token → 转发 → 更新 key 状态 → 返回响应（流式 SSE 透传）。空壳检测、预钳、带 backoff 重试、per-model 断路器。 | 服务端 `warp`，client 端 `reqwest` |
| **HealthChecker** | 启动探测 + 周期性坏 key 恢复。更新状态。 | `tokio::task::spawn` 循环 |
| **Observability** | Prometheus endpoint 在 `metrics_path`（默认 `/metrics`）；tracing 用 `tracing`+`tracing-subscriber`。 | `prometheus`、`tracing` |
| **Graceful Shutdown** | 监听 SIGINT/SIGTERM，停收新连接、排空在途请求。 | `tokio::signal`、warp 的 graceful shutdown |

### Key 选取算法

1. 在 `KeyPool` 里筛 `State::Active`。
2. 对每个 active key 算一个分数：
   `score = (available_tokens / capacity) − in_flight × 0.15 / weight`
   weight 从 1 起、失败减半（下限 1）、成功重置为 1。
3. 取分最高的 key；平局随机破。
4. 返回选中的 key；调用方完事必须调 `dec_in_flight()`。

### 限流

* Token 桶以 `qps_per_key` 每秒补充，桶容量 = `burst`。
* 转发前 `bucket.consume_with_timeout(1, deadline)` 最多等 `token_wait_ms`（默认 200 ms）。超时则释放该 key、试次优 key（受 `max_key_switches` 限制）。

### 冷却与健康

* 429/403 带 `Retry-After`：冷却该 key `min(Retry-After, cooldown.max_seconds)`。
  403 "Authorization failed"：无视该头、立即冷却。
* 网络/超时错误：标记 key 为坏（冷却 `cooldown.base_seconds`），换一个 key 重试一次。
* 健康检查器每 `health_interval` 秒跑一轮：
  * 用 `/v1/models` 探测每个 key；成功 → `Active`、失败计数清零。
  * 连续探测失败达 `max_consecutive_failures` → 强制 `Disabled`，直到下次探测成功或冷却到期。

### 断路器（per key & model）

连续的空壳（200 带 `usage:null`）或 504 响应递增该 key 上 per-model 计数。达到 `circuit_breaker_threshold`（默认 3）即冷却该 key。经由 `circuit_breaker_per_model`（默认 true）开启/关闭。

### 校验

两层校验挡掉本可避免的上游失败、并降低 `failure.log` 噪声：

1. **配置校验**（`Config::validate`，在 `Config::load` 应用环境变量覆盖后调用）：
   * 拒绝占位 `base_url`（`example.com`、`localhost`、`127.0.0.1`）与无 scheme 的 URL。
   * 拒绝明显是假/太短的 API key（含 `fake`、`dummy`、`test`、`example`、`placeholder`，或短于 8 字符）。
   * `request_timeout_seconds` 低于 5 秒时告警，因为过激的值在正常 LLM 延迟下会误超时。

2. 在代理 handler 里、消费 key 之前的**请求体规范化**：
   * `max_tokens`：bool/null/字符串/≤0/>100k → 丢弃；数字字符串强转为 int。上下文窗口算术产生的负值（如 `-14758`）被拦下。
   * `reasoning_effort`：`true`→"high"，`false`/null/非法→丢弃。
   * `model` 若在 `model_aliases` 里则重映射。
   * `tools` / `function.description` / `function.name`：null/缺失 → 空串（NVIDIA 拒绝 `null`）。
   * 非法请求立即返回 400、不拿 key、不打 NVIDIA。

### 指标（Prometheus）

* `proxy_requests_total{status="success|error|throttled"}`
* `proxy_key_state_total{state="active|cooling|disabled"}`
* `proxy_key_cooling_seconds_remaining{key_id="..."} (gauge)`
* `proxy_request_latency_seconds` (histogram)

### 配置示例（config.toml）

```toml
listen_addr = "0.0.0.0:8888"
base_url    = "https://integrate.api.nvidia.com"

# API keys —— 也可通过 NVIDIA_KEYS 环境变量设置（逗号分隔）
keys = [
    "nvapi-xxxxxxxxxxxxxxxx1",
    "nvapi-xxxxxxxxxxxxxxxx2",
    "nvapi-xxxxxxxxxxxxxxxx3",
]

[rate_limiter]
qps_per_key = 0.65
burst       = 8
# 在一个 key 的 token 桶上最多等多少毫秒，超时就切到另一个 key
#（默认 200）。0 = 关闭切换——保持既有无限等待行为。
token_wait_ms = 200

[cooldown]
base_seconds = 60
max_seconds  = 600

[health_check]
interval_seconds = 30
timeout_seconds  = 5
max_consecutive_failures = 3

# 请求超时（秒）。设为 0 可禁用（流式建议设 0）。
# 非流式请求端到端使用此超时。流式请求完全绕过此超时
#（长 generation 可能超过 60s）。
# 默认上调到 600s 以适配长上下文非流式补全。
request_timeout_seconds = 600

# 可选：流式连接专用超时（connect + idle read）。
# 设为 > 0 时由它接管流式路径，而非 request_timeout_seconds。
# 0 = 流式与非流式都用 request_timeout_seconds（旧行为）。默认 0（关闭）。
# request_timeout_streaming_seconds = 0

# NVIDIA "空壳" 软失败 —— 一个 200，body 为
# `{"choices":[],"id":"","usage":null,...}`（没真正跑推理；常见于 /compact
# 且 max_tokens 超出模型剩余上下文预算）—— 把 max_tokens 钳到
# `empty_shell_retry_max_tokens` 重试，仍失败再返回 502。否则 Claude Code 的
# /compact 会打印误导性 "summarization produced empty response"。默认 true；
# 关闭请设 NVIDIA_RETRY_ON_EMPTY_SHELL=false。
retry_on_empty_shell = true

# 重试空壳时使用的 max_tokens。第一次上游调用本身可能已被预钳
#（见下 `empty_shell_pre_clamp_max_tokens`），让预算刚好放得下，省掉一次
# 注定空壳的往返。8192 在 build.integrate 上对 GLM 系模型保守可用。
# 环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS。
empty_shell_retry_max_tokens = 8192

# 空壳软失败的重试次数。
# 2 = 原始 + 两次重试。0 = 不重试（回退到 502）。
# 默认 2。环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS。
empty_shell_retry_attempts = 2

# 空壳重试之间指数 backoff 的基线延迟（毫秒）。
# 默认 500。环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_BASE_MS。
empty_shell_retry_backoff_base_ms = 500

# 单次空壳重试 backoff 睡眠上限（毫秒）。
# 默认 5000。环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_MAX_MS。
empty_shell_retry_backoff_max_ms = 5000

# 在*第一次*上游调用时预钳的 per-model max_tokens 上限。
# 当请求命中的 model 在此 map 里、且 max_tokens 超过上限时，代理在请求发出
# 前就把它钳住——对每个请求（含流式）生效。NVIDIA 的 GLM 系模型在
# `prompt + max_tokens` 超出剩余上下文预算时返回一个 HTTP-200 空壳 body
#（choices:[]、usage:null）；入口就钳住预算，让推理真正跑起来，而不是烧一次
# 注定空壳的往返。默认含 z-ai/glm-5.2 → 32000。
# 覆盖方式：NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS（JSON map）。
empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 32000 }

# 是否对 500 Internal Server Error 重试。GLM 系模型偶发性返回
# {"message":"Failed to generate completions","type":"Internal Server Error","code":500}
# 是一种瞬时 per-request worker 失败——同一 prompt 秒后往往在另一个 key 上
# 成功。开启后 500 用与 401/503 相同的 backoff/重试预算在另一个 key 上重试。
# 默认 true。环境变量覆盖：NVIDIA_RETRY_ON_500。
retry_on_500 = true

# 可选：重试空壳失败前，先对 /v1/models 做一次轻量健康探测，
# 避免往已降级的上游重试。默认 true。环境变量覆盖：
# NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY。
empty_shell_probe_before_retry = true

# 断路器阈值。某 (key, model) 上连续 N 次空壳或 504 即把该 key 冷却
# cooldown.base_seconds 秒。0 = 关闭。默认 3。环境变量覆盖：
# NVIDIA_CIRCUIT_BREAKER_THRESHOLD。
circuit_breaker_threshold = 3

# 断路器是否按 (key, model) 对追踪失败，而非全局 per key。默认 true。
# 环境变量覆盖：NVIDIA_CIRCUIT_BREAKER_PER_MODEL。
circuit_breaker_per_model = true

# 可选：max_tokens 校验上限。任何 max_tokens 超过此值的请求会把该字段丢弃
#（用模型默认）。默认 100000。覆盖方式：MAX_TOKENS_MAX 环境变量。
# max_tokens_max = 100000

[observability]
enable_metrics = true
metrics_path   = "/metrics"
log_level      = "info"
```

### 构建与运行

```bash
# 克隆 / 建项目
git clone <repo-url> nvidia-key-proxy
cd nvidia-key-proxy

# 构建
cargo build --release

# 运行（用 ./config.toml 或环境变量）
./target/release/nvidia-key-proxy
```

Dockerfile（可选）：

```dockerfile
FROM rust:1.78-slim AS builder
WORKDIR /app
COPY . .
RUN cargo install --release --path .

FROM debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/nvidia-key-proxy /usr/local/bin/
EXPOSE 8888
ENTRYPOINT ["/usr/local/bin/nvidia-key-proxy"]
```

### 测试策略

* **单元测试** —— `KeyPool` 状态转换、token 桶正确性、代理重试/超时行为，以及请求校验（`max_tokens`、`reasoning_effort`、空壳重试逻辑、上游健康探测）。
* **集成测试** —— 起一个 mock NVIDIA 服务（用 `warp`），能返回 200、429、500、空壳 200；断言代理轮换 key、尊重限流、尊重冷却、对空壳用钳住的 `max_tokens` 重试、重试前探测上游健康。另有测试覆盖 health 端点、metrics（开/关）、未知路径 404 处理、配置校验。
* **压测** —— 用 `wrk` 或 `hey` 打代理；验证指标显示负载被分摊、没有单 key 超其 QPS。
* **安全审计** —— 确保日志绝不出现完整 API key（`Config` 的自定义 `Debug` 把 key 脱敏成 `XXXX***XXXX`）。

### 基于 failure.log 的可靠性改造（2026‑07‑23）

分析 `failure.log` 揭示四类高频错误。当前实现均已处理：

| 类别 | 计数 | 根因 | 修复 |
|---|---|---|---|
| `empty_choices_shell_first` | ~30% | NVIDIA 返回 200 带 `choices:[]`、`usage:null`、`id:""` —— 没跑推理。 | **在第一次上游调用时预钳 `max_tokens`**（per-model，`empty_shell_pre_clamp_max_tokens`，默认 GLM‑5.2 → 32000；含流式），让 `prompt + max_tokens` 预算放得下。仍返回空壳时**用 `max_tokens` 钳到 `empty_shell_retry_max_tokens`**（默认 8192）重试——次数可配（`empty_shell_retry_attempts`，默认 2）、指数 backoff（`empty_shell_retry_backoff_base_ms` / `..._max_ms`）、重试前可选上游健康探测（`empty_shell_probe_before_retry`）。Per-model 断路器（`circuit_breaker_per_model`，默认 true）。最终用一个可操作的 502 替代误导性的 "summarization produced empty response"。 |
| `timeout` | ~15% | 300s 硬超时对长上下文非流式太短。 | **`request_timeout_seconds` 默认上调到 600s**。流式请求完全绕过。新增 `request_timeout_streaming_seconds` 给流式单独的超时。告警阈值上调到 30s。 |
| `upstream_5xx`（504/503/500） | ~10% | 网关超时 / 资源耗尽 / 瞬时 `{"message":"Failed to generate completions"}` 500。 | client `connect_timeout`（5s）不变；per-request 超时只作用于非流式。流式默认不设上限。**503** 换另一个 key 带 backoff 重试（`retry_on_503`，默认 true，可选 `probe_before_503_retry`）。**500** 用同一 backoff/重试预算换另一个 key 重试（`retry_on_500`，默认 true）—— GLM 的 500 是瞬时 per-request 失败，不是确定性封禁。 |
| `upstream_4xx`（400 `max_tokens < 1`） | ~2% | client 算出的 `max_tokens` 下溢（大上下文 + 预留 token）。 | **代理丢弃非法 `max_tokens`**（≤0、>100k、非数字）。上限可配 `max_tokens_max`（默认 100_000）或 `MAX_TOKENS_MAX` 环境变量。 |

### 未来增强

* 经一个受保护的管理端点动态增删 key。
* Per-model 加权或成本感知调度。
* 支持多个上游 base（区域故障转移）。
* 若 NVIDIA 开放 gRPC/WebSocket 接口则转发之。

---

*设计文档结束。*
