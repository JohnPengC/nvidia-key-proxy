# NVIDIA API Key 负载均衡代理

一个用 Rust 编写的轻量级高性能代理，管理多个 NVIDIA API Key，内置 Codex CLI 集成。

## 功能特性

- **多 Key 负载均衡** — 轮换使用多个 Key，规避单 Key 速率限制。
- **自动限流检测** — 401（无 `Retry-After`）触发换 Key 重试，连续失败则禁用该 Key；429/403 属于确定性上游决策，带 `Retry-After` 才冷却该 Key，否则透传不重试。
- **健康检查** — 启动时探测 + 定期恢复检查，保持 Key 池健康。
- **Responses API ↔ Chat Completions 协议翻译** — Codex CLI（`wire_api = "responses"`）可直接对接 NVIDIA 的 `/v1/chat/completions` 后端，代理自动双向翻译。
- **Codex CLI 自动配置** — 启动时自动生成 `~/.codex/config.toml` 和模型目录，无需 cc-switch 中转。
- **可观测性** — Prometheus 指标 + 结构化 tracing 日志。
- **安全** — 真实 Key 不出进程；日志中 Key 以 `XXXX***XXXX` 格式脱敏。

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

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"

   [codex]
   enabled                  = true
   default_model            = "z-ai/glm-5.2"
   model_reasoning_effort   = "high"
   disable_response_storage = true
   bearer_token             = "proxy-managed"
   # models = [
   #   "z-ai/glm-5.2:GLM 5.2:1000000",
   #   "moonshotai/kimi-k2.6:Kimi K2.6:256000",
   # ]
   ```

3. **运行**
   ```bash
   ./target/release/nvidia-key-proxy
   ```

4. **接入客户端** — 将 `base_url` 指向 `http://localhost:8888`。`api_key` 字段可填任意值，代理会用池中的真实 Key 替换。

## Codex CLI 集成

代理在启动时自动管理 `~/.codex/config.toml`，省去 cc-switch 作为中间层。

### 工作原理

1. **生成 `~/.codex/nvidia-proxy-model-catalog.json`** — 模型目录，优先从 Codex 的 `gpt-5.5` 模板（`models_cache.json`）衍生，否则使用最小化 fallback。
2. **增量更新 `~/.codex/config.toml`** — 设置 `model_provider`、`model`、`model_reasoning_effort`、`disable_response_storage`、`model_catalog_json`（绝对路径）和 `[model_providers.nvidia-proxy]` section。用户自定义的 section（`notify`、`mcp_servers` 等）原样保留。
3. **清理冲突 Provider** — cc-switch 残留的 `[model_providers.custom]` section 会被自动移除。

### 为什么 `model_catalog_json` 必须是绝对路径

Codex CLI 要求 `model_catalog_json` 使用绝对路径。相对路径会在较新版本的 Codex 上触发 `AbsolutePathBuf deserialized without a base path` 错误。代理始终写入绝对路径（如 `/Users/<用户>/.codex/nvidia-proxy-model-catalog.json`）。

### 默认模型列表

| 模型 ID | 显示名称 | 上下文窗口 |
|---|---|---|
| `z-ai/glm-5.2` | GLM 5.2 | 1,000,000 |
| `moonshotai/kimi-k2.6` | Kimi K2.6 | 256,000 |
| `deepseek-ai/deepseek-v4-pro` | DeepSeek V4 Pro | 1,000,000 |
| `deepseek-ai/deepseek-v4-flash` | DeepSeek V4 Flash | 1,000,000 |
| `qwen/qwen3.5-397b-a17b` | Qwen 3.5 397B | 256,000 |
| `meta/llama-3.3-70b-instruct` | Llama 3.3 70B | 128,000 |

通过 `[codex] models` 自定义（格式：`"model_id:显示名称:上下文窗口"`）。

## Responses API 协议翻译

Codex CLI 使用 `wire_api = "responses"`（OpenAI Responses API），但 NVIDIA 只实现 `/v1/chat/completions`。代理在中间做双向翻译：

```
Codex CLI                    代理                       NVIDIA API
   |                           |                            |
   |-- POST /v1/responses -->  |                            |
   |   (Responses 格式)        |                            |
   |                           |-- 翻译请求体 -->             |
   |                           |-- POST /v1/chat/completions -->
   |                           |                            |
   |                           |<-- SSE (chat completions) --|
   |<-- SSE (responses)  <----|                            |
```

- **请求翻译**：`input` → `messages`，`instructions` → 系统消息，展开嵌套内容，`max_output_tokens` → `max_tokens`。
- **响应翻译（非流式）**：Chat Completions 响应 → Responses API JSON（`response.created`、`response.completed` 包装）。
- **响应翻译（流式 SSE）**：Chat Completions SSE 片段 → Responses API 事件（`response.created`、`response.output_text.delta`、`response.output_text.done`、`response.completed`）。
- **触发条件**：路径为 `/v1/responses`，或请求体含 `input` 字段但无 `messages`（cc-switch 转发到 `/v1/chat/completions` 时不翻译请求体）。

## 配置说明

### 顶层字段

| 字段 | 说明 | 默认值 |
|---|---|---|
| `listen_addr` | 监听地址 | 必填 |
| `base_url` | NVIDIA API 地址 | 必填 |
| `keys` | NVIDIA API Key 列表 | 必填（或 `NVIDIA_KEYS` 环境变量） |

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

### `[codex]` Codex 集成

| 字段 | 说明 | 默认值 |
|---|---|---|
| `enabled` | 启动时是否自动管理 Codex 配置 | `true` |
| `default_model` | Codex 默认模型 | `z-ai/glm-5.2` |
| `model_reasoning_effort` | 推理强度：none/minimal/low/medium/high | `high` |
| `disable_response_storage` | 禁用响应存储（非 OpenAI 后端必填） | `true` |
| `bearer_token` | `experimental_bearer_token` 的占位值 | `proxy-managed` |
| `models` | 自定义模型列表（覆盖默认） | `[]` |

## Prometheus 指标

当 `observability.enable_metrics` 为 `true` 时，指标暴露在根路径（`/`）。

| 指标名 | 说明 |
|---|---|
| `proxy_requests_total` | 总请求数 |
| `proxy_requests_success_total` | 2xx 请求数 |
| `proxy_requests_errors_total` | 非 2xx 请求数 |
| `proxy_request_duration_seconds` | 延迟直方图 |
| `proxy_key_active` | 活跃 Key 数 |
| `proxy_key_cooldown` | 冷却中 Key 数 |
| `proxy_key_disabled` | 禁用 Key 数 |

## API 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | Prometheus 指标 |
| GET | `/v1/models` | 模型目录（从生成的 catalog JSON 返回） |
| GET | `/models` | 同上（无 `/v1` 前缀） |
| GET | `/health` | 健康检查（返回 204） |
| * | `/*` | 透传到 NVIDIA API（注入 Key + 限流） |

## 测试

```bash
cargo test --release
```

当前测试覆盖 `codex_config` 模块：
- 删除 `[model_providers.custom]` section（有后续 section、在文件末尾、不存在三种场景）
- 替换 `[model_providers.nvidia-proxy]` section
- 验证 Rust `regex` crate 不支持 lookahead `(?=...)`，改用手动 `find` 逻辑

## 许可证

MIT

## 技术栈

Tokio、Warp、reqwest、regex、serde、tracing。
