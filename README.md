# NVIDIA API Key 负载均衡代理

一个用 Rust 编写的轻量、高性能代理，在多个 NVIDIA API key 前做一层负载均衡，附带 per-key 限流、健康检查与 Prometheus 可观测性。

## 特性

- **负载均衡**：跨多个 key 分发请求，规避单 key 限流。
- **自动限流检测** —— 429/403（带 `Retry-After`）冷却对应 key；不带 `Retry-After` 的 401 触发向另一个健康 key 的重试，连续认证失败则禁用该 key。
- **健康检查** —— 启动探测 + 周期性坏 key 恢复，让池子感知失效 key。
- **可观测性** —— Prometheus 指标暴露在 `metrics_path`（默认 `/metrics`），附带结构化 tracing 日志。
- **分 4 级的 tracing 日志**（`debug`/`info`/`warn`/`error`，`observability.log_level` 可配、默认 `info`、`RUST_LOG` 覆盖）—— 每请求分流细节降到 debug、生命周期事件 info、软失败 warn、不可恢复 error；主 tracing 流经 `tracing_appender::non_blocking` 异步写出，不阻塞请求路径。**最全的失败证据另入 `failure.log`**（结构化 JSONL，独立于 `log_level`、永远全量记录；受 `FAILURE_LOG_PATH`/`FAILURE_LOG_MAX_BYTES`/`FAILURE_LOG_DISABLE` 控制），供定期评审项目优化点。
- **启动期 `/health` 503**（S5）—— 启动健康检查（`HealthChecker::run_startup`）完成前置 503，让 LB/探针在完成首轮 key 池清点前摘掉本实例；就绪后回 204，且 `/health` 始终豁免前置鉴权。
- **413 prompt-token 硬上限**（`request_max_prompt_tokens`，默认 90000）—— estimated prompt 大于等于此值时直接以 413 拒绝（零上游往返、零客户端重试风暴），要求 client 自行压缩上下文。它刻意与空壳不重试软限（`empty_shell_no_retry_prompt_tokens`，默认同样 90000）**相等**——两者之间不留灰带（两轮实测表明任何灰带只会产出注定空壳 + 误导性 502）。
- **安全** —— 真实 key 不出进程；日志用 `XXXX***XXXX` 脱敏。
- **空壳重试** —— 检测 NVIDIA 的静默 200 软失败（`choices:[]`、`usage:null`），自动钳住 `max_tokens`，按指数 backoff 重试（默认 2 次重试、500ms 基线、5000ms 上限），重试前可选上游健康探测，per-(key,model) 断路器（已实现并在 `proxy_circuit_breaker_triggered_total` 计数，非“预留”）。
- **并发限制** —— per-model 信号量防止压垮上游 worker（例如 nemotron-3-ultra 的 32 worker 上限）；`concurrency_queue_timeout_ms` 排队超时返回 503 带 `Retry-After`。
- **网络韧性** —— 可配置上游连接池（idle 超时 90s、connect 超时 5s）、重试抖动（默认 100ms）、网络错误指标。
- **请求规范化** —— 把非法 `max_tokens`/`reasoning_effort`/`tools` 字段做合并修正而非硬 400；重映射 model alias；注入 `stream_options.include_usage`。
- **多模态透传** —— 把 OpenAI 风格的 content-block 数组（`image_url`、`input_audio`）原样转发给 NVIDIA NIM 视觉/音频模型，并按粗粒度的 per-part 成本（而非原始字节数）估算其 prompt 大小，确保内联 base64 图片不会撑爆 413 上限，URL 图片也不会被低估。不支持视频（NIM 没有标准化视频输入）。
- **配置校验** —— 占位 `base_url`/key 会快速失败，过短超时会告警，`request_max_prompt_tokens > empty_shell_no_retry_prompt_tokens`（灰带复活）会告警。
- **流式超时** —— 独立的 `request_timeout_streaming_seconds` 应对长 generation。
- **max_tokens 校验** —— 在打到 NVIDIA 之前丢掉非法 `max_tokens`（≤0、>100k、非数字、负数字符串）。
- **管理 API** —— 受 `admin_token` 保护的 `/admin/keys`（GET 列状态 / POST 增 / DELETE 删），未设 token 时管理面整体藏起来返回 404，无法被探测。

## 快速开始

1. **克隆并构建**
   ```bash
   git clone <repository-url>
   cd nvidia-key-proxy
   cargo build --release
   ```

2. **配置**（项目根目录的 `config.toml`）：
   ```toml
   listen_addr = "0.0.0.0:8888"
   base_url    = "https://integrate.api.nvidia.com"

   # API keys —— 也可通过 NVIDIA_KEYS 环境变量设置（逗号分隔）
   keys = [
       "nvapi-xxxxxxxxxxxxxxxx1",
       "nvapi-xxxxxxxxxxxxxxxx2",
   ]

   [rate_limiter]
   qps_per_key = 0.65
   burst       = 8

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
   # `{"choices":[],"id":"","usage":null,...}`（没有真正跑推理；常见于 `/compact`
   # 且 max_tokens 超出模型剩余上下文预算）—— 出现时先把 max_tokens 钳到
   # `empty_shell_retry_max_tokens` 重试一次，仍失败再返回 502。否则 Claude Code 的
   # `/compact` 会打印误导性 "summarization produced empty response"。默认 true；
   # 关闭请设 NVIDIA_RETRY_ON_EMPTY_SHELL=false。
   retry_on_empty_shell = true

   # 重试空壳时使用的 max_tokens。第一次上游调用本身可能已被预钳
   #（见下 `empty_shell_pre_clamp_max_tokens`），让预算刚好放得下，省掉一次
   # 注定空壳的往返。8192 在 build.integrate 上对 GLM 系模型保守可用。
   # 环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS。
   empty_shell_retry_max_tokens = 8192

   # 可选：空壳软失败的重试次数。
   # 2 = 原始 + 两次重试。0 = 不重试（直接 502）。
   # 默认 2。环境变量覆盖：NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS。
   # empty_shell_retry_attempts = 2

   # 可选：重试空壳失败前，先对 /v1/models 做一次轻量健康探测，
   # 避免往已降级的上游重试。默认 true。环境变量覆盖：
   # NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY。
   # empty_shell_probe_before_retry = true

   # 可选：per-model max_tokens 上限，让*第一次*上游调用（含流式）就被钳住。
   # NVIDIA 的 GLM 系模型在 `prompt + max_tokens` 超出剩余上下文预算时返回 200
   # 空壳 body；入口就钳住预算，让推理真正跑起来，省掉一次注定空壳的往返。
   # 默认 z-ai/glm-5.2 → 8192（不是 32000——两轮实测把上限调低以更稳地放得下）。
   # 覆盖方式：NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS（JSON map）。
   # empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 8192 }

   # 可选：断路器阈值。某 (key, model) 上连续 N 次空壳或 504 即把该 key 冷却
   # cooldown.base_seconds 秒。0 = 关闭。默认 3。环境变量覆盖：
   # NVIDIA_CIRCUIT_BREAKER_THRESHOLD。
   # circuit_breaker_threshold = 3

   # 可选：413 prompt-token 硬上限。estimated prompt >= 此值时直接以 413 拒绝
   #（零上游往返、零客户端重试风暴），要求 client 自行压缩上下文。请把它设成
   # 与 `empty_shell_no_retry_prompt_tokens` *相等*（两者默认都 90000）——
   # 之间不留灰带。0 = 关闭。覆盖方式：NVIDIA_REQUEST_MAX_PROMPT_TOKENS。
   # request_max_prompt_tokens = 90000

   # 是否对 500 Internal Server Error 重试。GLM 系模型偶发性返回
   # {"message":"Failed to generate completions",...}，是一种瞬时 per-request
   # worker 失败（同一 prompt 秒后在另一个 key 上能成功）。开启后 500 会带
   # backoff 在另一个 key 上重试。默认 true。环境变量覆盖：NVIDIA_RETRY_ON_500。
   # retry_on_500 = true

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"

   # client 可见的 model 名 → NVIDIA 模型 id 的映射。
   # 当 chat-completion 请求的 `model` 命中此处某 key 时，代理在转发上游前把该
   # 字段改写为映射值。未命中的名字原样转发。默认为空。
   #
   # 当你的 client（cc-switch、Cursor、OpenAI SDK……）发送的是 NVIDIA NIM 上不
   # 存在的 OpenAI 风格 model 名时用它，例如：
   #
   #   [model_aliases]
   #   "claude-opus-4-8"        = "z-ai/glm-5.2"   # 同名映射，保留以示清晰
   #   "gpt-4o"              = "meta/llama-3.1-405b-instruct"
   #   "gpt-4o-mini"         = "meta/llama-3.1-70b-instruct"
   ```

3. **运行**
   ```bash
   ./target/release/nvidia-key-proxy
   ```

4. **接上你的 client** —— 把 `base_url` 指向 `http://localhost:8888`。`api_key` 字段填任意字符串即可；代理会用池子里的真实 key 替换它。

## 配置

### 顶层字段

| 字段 | 说明 | 默认值 |
|---|---|---|
| `listen_addr` | 监听地址 | 必填 |
| `base_url` | NVIDIA API base URL | 必填 |
| `keys` | NVIDIA API key 列表 | 必填（或 `NVIDIA_KEYS` 环境变量） |
| `request_timeout_seconds` | 非流式请求超时；0 关闭（流式始终绕过） | `600` |
| `request_timeout_streaming_seconds` | 流式请求超时；0 = 用非流式超时或关闭 | `0` |
| `max_tokens_max` | `max_tokens` 校验上限；超过则丢弃该字段 | `100_000` |
| `retry_on_empty_shell` | 对 NVIDIA 空壳软失败（200 带 `choices:[]`、`usage:null`）做重试 | `true` |
| `empty_shell_retry_max_tokens` | 空壳重试时钳到的 `max_tokens` | `8192` |
| `empty_shell_retry_attempts` | 空壳重试次数（0 = 不重试） | `2` |
| `empty_shell_probe_before_retry` | 空壳重试前先探测 `/v1/models` | `true` |
| `empty_shell_retry_backoff_base_ms` | 空壳重试间指数 backoff 的基线延迟 | `500` |
| `empty_shell_retry_backoff_max_ms` | 单次空壳重试 backoff 睡眠上限 | `5000` |
| `empty_shell_pre_clamp_max_tokens` | per-model `max_tokens` 上限，在*第一次*上游调用（含流式）时钳住 | `{"z-ai/glm-5.2": 8192}` |
| `empty_shell_no_retry_prompt_tokens` | 估算 prompt 超过此值的空壳不再重试，直接 502（0 = 永远重试） | `90000` |
| `request_max_prompt_tokens` | 413 硬上限：estimated prompt ≥ 此值直接拒绝（0 = 关闭） | `90000` |
| `circuit_breaker_threshold` | 触发冷却所需的连续空壳/504 次数（0 = 关闭） | `3` |
| `circuit_breaker_per_model` | 断路器按 (key, model) 维度而非全局 per key 统计 | `true` |
| `retry_on_503` | 对 503（Worker resource exhausted）换另一个 key 重试 | `true` |
| `probe_before_503_retry` | 503 重试前先探测 `/v1/models` | `true` |
| `retry_on_500` | 对 500 `Failed to generate completions` 换另一个 key 重试 | `true` |
| `concurrency_limit_per_model` | per-model 最大并发上游请求数；默认空（无限制） | `{}` |
| `concurrency_queue_timeout_ms` | 等并发槽位的最长时间（超时回 503 带 `Retry-After`） | `5000` |
| `upstream_connect_timeout_secs` | 上游连接超时 | `5` |
| `upstream_pool_idle_timeout_secs` | 上游连接池空闲超时 | `90` |
| `retry_jitter_ms` | 重试抖动，叠加到指数 backoff 之上 | `100` |
| `est_image_token_cost` | prompt 大小估算里每个 `image_url` block 的固定 token 成本（URL 与内联 base64 同价） | `1500` |
| `est_audio_token_per_kb` | 每个 `input_audio` block 里每 KiB base64 的 token 成本 | `16` |
| `est_audio_token_cap` | 每个 audio block 估算值的硬上限 | `10000` |
| `est_unknown_part_token_cap` | 任何未识别 content-block 类型序列化长度的上限 | `2000` |
| `inject_stream_usage` | 向流式 chat 注入 `stream_options.include_usage: true` | `true` |
| `synthesize_empty_choices` | 上游返回 2xx 非 SSE 且 `choices:[]` 时合成一个占位 choice | `true` |
| `allow_local_base_url` | 允许 `base_url` 指向回环/localhost（仅用于测试） | `false` |
| `auth` | 前置 + 管理 API 鉴权配置（见下） | 关闭 |
| `model_aliases` | 客户端可见模型名 → NVIDIA 适用模型 id 的映射 | 空 |

### `[rate_limiter]`

| 字段 | 说明 |
|---|---|
| `qps_per_key` | 每个 key 每秒查询数 |
| `burst` | token 桶容量 |
| `token_wait_ms` | 在一个 key 的 token 桶上最多等多少毫秒，超时切另一个 key（0 = 关闭切换） |

### `[cooldown]`

| 字段 | 说明 |
|---|---|
| `base_seconds` | 初始冷却时长 |
| `max_seconds` | 最大冷却（指数 backoff 上限） |
| `scan_interval_seconds` | `cool_down_task` 扫描"冷却是否到期"的独立周期（0 = 回退到 `health_check.interval_seconds`） |

### `[health_check]`

| 字段 | 说明 |
|---|---|
| `interval_seconds` | 坏 key 恢复检查间隔 |
| `timeout_seconds` | 探测超时 |
| `max_consecutive_failures` | 多少次失败后把 key 移入冷却 |

### `[observability]`

| 字段 | 说明 |
|---|---|
| `enable_metrics` | 是否启用 Prometheus 指标 endpoint |
| `metrics_path` | 指标暴露路径（默认 `/metrics`） |
| `log_level` | tracing 日志级别（也可用 `RUST_LOG` 覆盖） |

### `[retry]`

| 字段 | 说明 | 默认值 |
|---|---|---|
| `max_retries` | 可重试错误的最大重试次数 | `3` |
| `backoff_base_ms` | 首次 backoff 毫秒数 | `200` |
| `backoff_max_ms` | 单次 backoff 上限毫秒数 | `2000` |
| `auth_fail_threshold` | 连续认证失败多少次后禁用 key | `3` |
| `max_key_switches` | 单个请求在 token 饥饿时最多切几次 key（与 `max_retries` 解耦） | `3` |

### `[auth]`

| 字段 | 说明 | 默认值 |
|---|---|---|
| `enabled` | 是否要求 client 在 `/v1/*` 上带 `Authorization: Bearer <bearer_token>` | `false` |
| `bearer_token` | client 的 bearer token（也用 `NVIDIA_AUTH_BEARER_TOKEN`） | 空 |
| `admin_token` | 保护 `/admin/*` 的 token；为空时管理 API 完全禁用 | 空 |

## 配置校验

启动时代理会校验加载到的配置，发现明显的误配就提前退出，避免静默失败（例如占位值导致的 401/404）：

- `base_url` 不能是 `example.com`、`localhost` 或 `127.0.0.1`，且必须以 `http://` 或 `https://` 开头。
- API key 至少 8 个字符，且不能包含 `fake`、`dummy`、`test`、`example`、`placeholder`。
- `request_timeout_seconds` 低于 `5` 秒会告警，建议调大（正常 LLM 延迟会误超时）。
- `request_timeout_streaming_seconds` 大于 0 且低于 `5` 秒也会告警。
- `request_max_prompt_tokens` 严格大于 `empty_shell_no_retry_prompt_tokens`（即硬上限 > 软不重试阈值、两者间留灰带）会告警——这会复活"打一发就过但注定空壳 + 误导 502"的灰带（默认二者相等 90000）。不阻断启动。

## 基于 failure.log 的可靠性改造（2026‑07‑23）

分析了 `failure.log`，处理了四类高频错误：

| 类别 | 占比 | 根因 | 修复 |
|---|---|---|---|
| `empty_choices_shell_first` | 41.9% | NVIDIA 返回 200 带 `choices:[]`、`usage:null`、`id:""` —— 没跑推理。 | **在第一次上游调用时预钳 `max_tokens`**（per-model `empty_shell_pre_clamp_max_tokens`，默认 GLM-5.2 → 8192；含流式），让 `prompt + max_tokens` 预算放得下。仍返回空壳时**用钳过的 `max_tokens` 重试**（配置项 `empty_shell_retry_max_tokens`，默认 8192），重试前可选上游健康探测（`empty_shell_probe_before_retry`），次数可配（`empty_shell_retry_attempts`，默认 2）。最终用一个可操作的 502 替代误导性的 "summarization produced empty response"。 |
| `timeout` | 37.2% | 300s 硬超时对长上下文非流式太短。 | **`request_timeout_seconds` 默认上调到 600s**。流式请求完全绕过。新增 `request_timeout_streaming_seconds` 给流式单独的超时。告警阈值上调到 30s。 |
| `upstream_5xx`（503/504/500） | 14.0% | 流式网关超时 / 资源耗尽 / 瞬时 `{"message":"Failed to generate completions"}` 500。 | client `connect_timeout`（5s）不变；per-request 超时只作用于非流式。流式默认不设上限。**503** 换另一个 key 带 backoff 重试（`retry_on_503`，默认 true，可选 `probe_before_503_retry`）。**500** 用同一 backoff/重试预算换另一个 key 重试（`retry_on_500`，默认 true）—— GLM 的 500 是瞬时 per-request 失败，不是确定性封禁。 |
| `upstream_4xx`（400 `max_tokens < 1`） | 2.3% | client 算出的 `max_tokens` 下溢（大上下文 + 预留 token）。 | **代理丢弃非法 `max_tokens`**（≤0、>100k、非数字）。上限可配 `max_tokens_max`（默认 100_000）或 `MAX_TOKENS_MAX` 环境变量。 |

### 新增配置项

```toml
# config.toml
request_timeout_seconds = 600                # 非流式超时（0 = 关闭）
request_timeout_streaming_seconds = 0        # 流式超时（0 = 用非流式或关闭）

# 空壳软失败重试
retry_on_empty_shell = true
empty_shell_retry_max_tokens = 8192          # 重试时钳到的预算
empty_shell_retry_attempts = 2               # 空壳后的重试次数（默认已上调到 2）
empty_shell_probe_before_retry = true        # 重试前探测 /v1/models
empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 8192 }  # 首次调用预钳（含流式；默认从 32000 调到 8192）

# 对瞬时 5xx 换另一个 key 重试
retry_on_503 = true
probe_before_503_retry = true
retry_on_500 = true

# 断路器（已实现；触发计数见 proxy_circuit_breaker_triggered_total）
circuit_breaker_threshold = 3                # 触发冷却所需的连续空壳/504 次数
circuit_breaker_per_model = true             # 按 (key, model) 维度统计

# 413 prompt-token 硬上限（与空壳不重试软限相等，不留灰带）
empty_shell_no_retry_prompt_tokens = 90000   # 超过此值的空壳不再重试、直接 502
request_max_prompt_tokens = 90000            # estimated prompt >= 此值直接 413 拒绝

# max_tokens 校验上限
max_tokens_max = 100_000                     # 超过则丢弃该字段
```

所有选项都可用 `NVIDIA_*` 环境变量覆盖（例如 `NVIDIA_REQUEST_TIMEOUT_SECONDS`、`NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS`）。

### 断路器（per (key, model)）

`circuit_breaker_threshold`（默认 3）已**生效**：某 (key, model) 上连续 N 次空壳或 504 即把该 key 冷却 `cooldown.base_seconds`。经由 `circuit_breaker_per_model`（默认 true）开启按 (key, model) 维度统计。触发计数在 `proxy_circuit_breaker_triggered_total` 暴露。`max_tokens_max` 也在请求校验路径上生效（>此值的 `max_tokens` 直接丢弃）。

## 请求规范化

转发 chat-completion 请求前，代理会对一些 NVIDIA NIM 会以 400 拒绝、而 client（cc-switch、Cursor、OpenAI SDK）常发送成那副样子的请求体字段做规范化——救回请求而非弹回：

- `max_tokens`：bool / null / 非正 / 非数字字符串值会被丢弃（NVIDIA 用其默认）或强转为 int。
- `reasoning_effort`：bool（`true`→`"high"`）或未识别字符串会被改写或丢弃；`low`/`medium`/`high` 原样放行。
- `model`：配置了 `[model_aliases]` 时按其重映射。

这些是尽力而为的兼容性修正，不是严格校验——这些字段不再走硬 400 路径（见*行为与兼容性*）。

## 指标

`observability.enable_metrics` 为 `true` 时，Prometheus 指标暴露在 `observability.metrics_path`（默认 `/metrics`）。

| 指标 | 说明 |
|---|---|
| `proxy_requests_total{status="success\|error\|throttled"}` | 处理过的 HTTP 请求总数 |
| `proxy_requests_success_total` | 2xx 请求数 |
| `proxy_requests_errors_total` | 非 2xx 请求数（含 413 拒绝） |
| `proxy_request_duration_seconds` | 延迟直方图 |
| `proxy_tokens_input_total` | 累计输入 token 数（prompt tokens） |
| `proxy_tokens_output_total` | 累计输出 token 数（completion tokens） |
| `proxy_tokens_total` | 累计总 token 数（prompt + completion），从流式 + 缓冲响应里真实 `usage` 对象统计 |
| `proxy_retry_attempt_failures_total` | 经内部重试的 per-attempt 上游失败数（对 client 不可见） |
| `proxy_key_active` | 活跃 key 数量（gauge） |
| `proxy_key_cooldown` | 冷却中的 key 数量（gauge） |
| `proxy_key_disabled` | 被禁用的 key 数量（gauge） |
| `proxy_key_switch_token_starvation_total` | 当前 key 的 token 桶饿空后切到另一个 key 的总次数 |
| `proxy_key_403_auth_cooldown_total` | 因 403 `Authorization failed` 触发的冷却总次数 |
| `proxy_circuit_breaker_triggered_total` | per-(key,model) 断路器触发的总次数 |
| `proxy_empty_shell_total` | 上游侧 NVIDIA 空壳软失败总数 |
| `proxy_empty_shell_retry_attempts_total` | 空壳重试尝试总次数 |
| `proxy_prompt_rejected_total` | 代理侧 prompt 拒绝总数（413 prompt-token 超硬上限） |
| `proxy_concurrency_current{model="..."}` | 当前每模型在途并发数（gauge） |
| `proxy_concurrency_acquire_total{model="..."}` | 每模型并发槽位获取总次数 |
| `proxy_network_errors_total` | 网络层错误总次数 |

## 日志与可观测性

tracing 日志按 4 级分流（`observability.log_level` 可配、默认 `info`、`RUST_LOG` env var 覆盖）：

| 级别 | 记录什么 |
|---|---|
| `error` | 不可恢复内部错误（约 5 处） |
| `warn` | 软失败 / 跳过 / 限流 / 冷却 / 惩罚等异常但可恢复事件（默认生产基线） |
| `info` | 生命周期事件：监听绑定、配置加载、启动健康检查、key add/remove、冷却恢复（默认值） |
| `debug` | 每请求分流细节：收到请求、代理请求至、上游尝试开始/上游响应、请求成功完成、模型别名重映射、预 clamp max_tokens、空壳重试响应、key mark-good（调查误指/重试/限流时临时调到 debug） |

主 tracing 流经 `tracing_appender::non_blocking` 异步写出：格式化与 `write` syscall 由一个后台 worker 完成，业务协程只 `try_send` 即返回——不被慢消费方（`docker logs` / journald / 管道拥塞）拖累 IO 抖动。其 `WorkerGuard` 绑在 `async fn main` 的栈变量上活到进程末——**Rust 进程正常退出时不 drop 进程级 static 变量**，把它放进 static 会吞掉关停尾部日志（已实测修过；guard 必须是有确定生命周期的栈变量）。

最全的失败证据另入 `failure.log`（结构化 JSONL：完整 body 节选、重试原因、上游 URL、脱敏 key），它独立于 `log_level`、永远全量记录失败，供定期评审项目优化点；与异步主 tracing 流不同，`failure.log` 保持同步写入（`std::sync::Mutex` + `writeln!` + `flush`），不向异步 channel 投递——是诊断事实库，不能冒任何丢失风险。

`failure.log` 三个独立 env var：

| env var | 作用 |
|---|---|
| `FAILURE_LOG_PATH` | 文件路径，默认 `./failure.log` |
| `FAILURE_LOG_MAX_BYTES` | 达到该大小则轮转（rollover 到 `failure.log.<unix-secs>`）；`0` / 不设 = 无限 append |
| `FAILURE_LOG_DISABLE` | 设了就完全跳过写入（CI / 测试用） |

归档文件代理不清理、不压缩；需要保留期限时配合外部 `logrotate`（如 `copytruncate` + `rotate N` + compress）。

## 架构

代理按职责拆成 `src/lib.rs` 下的若干模块：

- **`config`** —— 加载 `config.toml` / 环境变量 / CLI；自定义 `Debug` 对 API key 脱敏。
- **`key_pool`** —— 维护 per-key 状态（active、cooling、disabled），加权随机选取。
- **`proxy`** —— warp handler：拿 key → 限流 → 转发 → 更新 key 状态 → 把响应流回 client。
- **`health`** —— 启动探测 + 周期性坏 key 恢复。
- **`metrics`** —— Prometheus counter 与 histogram。
- **`failure_log`** —— 结构化 JSONL 失败日志（`failure.log`，同步写入、不可丢；详见上节"日志与可观测性"）。

`AppState` 把共享运行时对象（metrics、key pool、proxy、health checker、stop signal、config）打包到一起，`build_routes` 构造 warp filter，这样集成测试可以不动 `main` 就跑全栈。

## 行为与兼容性

本代理**不是透明透传**。除了核心职责——把请求分发到多个 NVIDIA key 上、per-key 限流、故障转移——它还做了一组尽力而为的兼容性规范化，使 OpenAI 风格的 client（cc-switch、Cursor、OpenAI SDK……）无需改请求体就能工作。这些改写每一项都是有意的"不纯转发"偏离，逐一列出以使行为可预测：

- **请求体规范化**（仅 `/v1/chat/completions`）：作为 bool / null / 非正 / 非数字字符串的 `max_tokens` 会被丢弃或强转；作为 bool（`true`→`"high"`）或未识别字符串的 `reasoning_effort` 会被改写或丢弃；非法值是被救回而非弹回 400。
- **`/health` 启动期 503**：启动健康检查（`HealthChecker::run_startup`）完成前置 `GET /health` 回 503，让 LB/探针在完成首轮 key 池清点前摘除本实例；就绪后回 204。始终豁免前置鉴权。
- **Model alias**：client 发来的 `model` 命中 `[model_aliases]` 时，转发前改写为映射的 NVIDIA id。
- **`/v1/responses` 与 `/v1/messages` 被拒绝**：NVIDIA 只提供 OpenAI Chat Completions。OpenAI Responses API（`/v1/responses`）与 Anthropic Messages API（`/v1/messages`）不暴露；代理返回 `501 Not Implemented` 带 typed `not_supported` 错误，而不是默默转发一个这些端点解析不了的 body。
- **`/v1/models` 透传给 NVIDIA**：代理把 `GET /v1/models` 透传给 NVIDIA（其无需鉴权即可应答）、缓存结果，在 NVIDIA 不可达时回退到配置的 `[model_aliases]` 列表。因此 client 看到的是 NVIDIA 真实完整目录，而非写死的子集。
- **流式 usage 注入**：对流式 chat 请求，代理注入 `stream_options.include_usage: true`（除非 client 已设），让 NVIDIA 在流末返回 token 用量，从而填上代理的 token 指标。用 `inject_stream_usage = false` 或 `NVIDIA_INJECT_STREAM_USAGE=false` 关闭。
- **`Accept` 头被强改**：client 请求 `stream` 时强制为 `text/event-stream`（否则 `application/json`），让 NVIDIA 与 client 在传输形态上对齐。
- **`error.code` 被强转为字符串**：缓冲与 SSE 错误响应都一样，因为 NVIDIA 返回数字 `code`，而 OpenAI 兼容 client 期望字符串。
- **多模态 content 原样透传**：消息里的 `content` 数组（OpenAI 风格 block：`{type:"text",…}`、`{type:"image_url",…}`、`{type:"input_audio",…}`）原样到达 NVIDIA —— body 规范化改写只动顶层标量字段（`max_tokens`、`reasoning_effort`、`model`、`stream_options`），从不碰 `messages[].content`。NVIDIA NIM 公开 API 只文档化纯文本 `content`；视觉/音频模型是 OpenAI 兼容的、能接受这些 block 数组，但确切 schema 与哪些模型接受上游未验证 —— 代理不做任何格式翻译、不做任何校验声明。**不支持视频**（NIM 没有标准化视频输入）。
- **多模态 prompt-token 估算用粗粒度 per-part 成本**：413 硬上限（`request_max_prompt_tokens`）与空壳不重试软限（`empty_shell_no_retry_prompt_tokens`）都与 `estimate_prompt_tokens` 比较，后者把 `content` 数组按 `text=chars/4`、`image_url=<固定成本>`、`input_audio=<每 KiB 成本、封顶>`、unknown=`<chars/4、封顶>` 计大小。这样内联 base64 图片（几万字符）不会撑爆估算、误 413 一个合法多模态请求，而只有 URL 的图片（几十字符）也按固定图片成本计费、不能免费钻过上限。failure-log 指纹上报同一个粗粒度估算值和 part 普查（text/image/audio 计数）而非倾倒 base64 payload。可用 `est_image_token_cost`、`est_audio_token_per_kb`/`est_audio_token_cap`、`est_unknown_part_token_cap` 调。**注意（2026-08-01，实测验证）：** *上游*那侧受 key 权限门控 —— 对 22 个测试 key，只要请求的 `content` 是 OpenAI 风格数组（纯文本数组、只有 URL 的图片、内联 base64 图片、视觉*或*文本模型上），NVIDIA 一律返回 HTTP **403 `Authorization failed`**，而同样 key 上纯字符串 `content` 返回 200。代理把数组原样透传（并把 403 转发给 client，因 4xx 不重试），但真正能跑多模态推理需要一个 NVIDIA 套餐带视觉权限的 key。

如果你需要严格透明，需要留意的就是 body / alias / 响应路径的改写部分；它们都被限制在 chat-completion 路径内。

## API 端点

| 方法 | 路径 | 鉴权 | 说明 |
|---|---|---|---|
| GET | `{metrics_path}` | — | Prometheus 指标 |
| GET | `/health` | — | 启动健康检查完成后回 204；启动检查未完成前回 503（让 LB/探针摘除）。始终豁免前置鉴权 |
| * | `/*` | 前置* | 透传到 NVIDIA（注入 key + 限流） |
| GET | `/admin/keys` | admin | 列出每个 key 的状态（脱敏）与实时统计 |
| POST | `/admin/keys` | admin | 运行时加 key：JSON `["k1", "k2"]` 或 `{"keys":[...]}` |
| DELETE | `/admin/keys/{key}` | admin | 运行时删一个 key |

\* 前置鉴权仅在 `[auth].enabled = true` 时生效。`[auth].admin_token` 未设时管理 API 完全禁用（404）。

## 实现重构（2026‑07‑11）

- 引入 **`AppState`** 把共享运行时对象（metrics、key pool、proxy、health checker、stop signal、config）打包到一起。
- 把 tracing 初始化抽成 **`init_tracing`**，启动顺序更清晰。
- 把路由构造挪进 **`build_routes`**，让 `main` 只管编排。
- 优雅关停改用单个 `Notify` 并 drop 后台 task handle，避免死锁。
- 在 **`tests/`** 下加集成测试，覆盖 health、metrics（开/关）、未知路径处理。

## 校验与加固（2026‑07‑16）

- 加了 **配置校验**（`Config::validate`），对占位 `base_url`/key 快速失败，对过短请求超时告警。
- 在代理里加了针对 `max_tokens` 与 `reasoning_effort` 的**请求体校验**，在非法 OpenAI 风格 chat 请求打到 NVIDIA 前短路。
- 扩充了覆盖这两层校验的单元测试。

## 日志分级 + 异步主 tracing 流 + 配置补全 + `/health` 启动期 503（2026‑08‑10）

- **tracing 日志按 4 级分流**（`debug`/`info`/`warn`/`error`，`observability.log_level` 可配、默认 `info`、`RUST_LOG` 覆盖）—— 每请求分流细节降到 debug，生命周期事件 info，软失败 warn，不可恢复 error。详见上节"日志与可观测性"的级别表。
- **主 tracing 流改异步**：经 `tracing_appender::non_blocking` 写出，格式化与 `write` 由后台 worker 完成，业务协程非阻塞，不被 stdout 慢消费拖累。其 `WorkerGuard` 绑在 `async fn main` 栈上活到函数末（**不能放进进程级 static**——Rust 进程正常退出不 drop statics，会吞掉关停尾部日志）；`init_tracing(&Config) -> Option<WorkerGuard>` 把 guard 还给调用方，由 main 持住。`failure.log` 刻意保持同步（不可丢的诊治事实库）。
- **`/health` 返回 503（启动健康检查完成前）**（S5）：让 LB/探针在完成首轮 key 池清点前摘除本实例，避免把流量打到未初始化的池上。
- **断路器已实现并计数**（之前 README 标"预留"是陈旧的）—— `proxy_circuit_breaker_triggered_total` 暴露 per-(key,model) 断路器触发次数；`circuit_breaker_per_model`（默认 true）控制按 (key, model) 还是全局 per key 统计。
- **多模态 prompt-token 估算 pass-through**（U1/U2）：`stream:false` / 非流式 SSE 合成路径现在都把多模态 part 成本并入 `estimate_prompt_tokens`，413 硬上限与空壳不重试软限都用它判，内联 base64 不再撑爆估算。
- **流式跨块 SSE 重组装**（S4）：`CoerceSseTap` 流包装在上游 SSE chunk 横跨 `Bytes` 边界时按 `\n` 重组装后再逐行 coerce，避免双发 / 漏行。
- **`body_summary` 记录真实值**（U3）：413 / 失败日志的 `body_summary` 现在记录 `max_tokens`、`reasoning_effort` 等标量的真实取值而非 `<present>` 标记（null 仍标 `<present:null>`），便于评审真实触发条件。
- **TokenBucket approx_tokens 原子化**（P1）：key 选取的可用 token 比较走 `AtomicU64` 快路径，不再在 hot loop 里获取 async 锁。
- **`mark_failure` 单锁化**（P2）：原来在 mark_failure 里取 4 把异步锁的路径合并为 `record_failure_and_maybe_disable` 单写锁，避免 key 失败路径的锁抖动。
- **配置模板补全**：`config.toml.example` 现在覆盖 `Config` 结构体全部 48 个字段，每项标注 `NVIDIA_*` 环境变量名；**修复一个潜伏 typo**：`config.toml` 里 `[model_aliase]` → `[model_aliases]`，原来三条 Claude→GLM 别名因 serde 忽略未识别表而静默未生效。
- 全部 55 条旧 clippy 告警清零；101 lib + 11 integration 测试通过。

## 测试

```bash
cargo test --release
```

单元测试覆盖 key-pool 状态转换、token 桶正确性、代理重试/超时行为。集成测试覆盖 health、metrics（开/关）、未知路径处理。

## License

MIT

## 致谢

- 基于 Tokio、Warp、reqwest、serde 构建。
