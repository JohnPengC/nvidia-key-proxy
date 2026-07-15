# NVIDIA API Key 负载均衡代理

一个用 Rust 编写的轻量级高性能代理，管理多个 NVIDIA API Key，提供按 Key 限流、健康检查和 Prometheus 可观测性。

## 功能特性

- **多 Key 负载均衡** — 轮换使用多个 Key，规避单 Key 速率限制。
- **自动限流检测** — 429/403（带 `Retry-After`）冷却该 Key；401（无 `Retry-After`）触发换 Key 重试，连续失败则禁用该 Key。
- **健康检查** — 启动时探测 + 定期恢复检查，保持 Key 池健康。
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

   # 请求超时（秒）。设为 0 关闭（流式建议关闭）。
   # 流式请求始终绕过此超时。
   request_timeout_seconds = 60
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
| `request_timeout_seconds` | 请求超时；0 关闭（流式始终绕过） | `60` |

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

## 测试

```bash
cargo test --release
```

单元测试覆盖 Key 池状态转换、令牌桶正确性、代理重试/超时行为。集成测试覆盖健康接口、指标端点（启用/禁用）以及未知路径 404。

## 许可证

MIT

## 技术栈

Tokio、Warp、reqwest、serde、tracing。