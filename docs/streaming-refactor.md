# 流式响应改造规划

## 1. 背景与问题

当前代理在处理 Responses API（`/v1/responses`，或 body 带 `input` 无 `messages`）
请求时，虽然把上游返回的 SSE 流逐 chunk 翻译成 Responses SSE 事件，但最后是
把所有事件 `Vec<String>` `join("")` 成一个 `Bytes`，通过
`warp::http::Response<Bytes>` 一次性返回给客户端。

用户反馈："信息不是流式加载，而是等一会后突然全部加载"——确认该现象为真。

passthrough 路径（`/v1/chat/completions` 直通）同样问题：`is_streaming` 写死
`false`，走 `resp.bytes().await` 整块读完再回，所以直连 chat completions 也会
"突然全出"。

### 根因定位

`src/proxy.rs` `handle_streaming_response` 中：

```rust
// Collect all translated SSE events, then return as a single body.
let mut all_events = Vec::new();
...
let body = all_events.join("");
Ok(response_builder.body(Bytes::from(body))...)
```

Header 虽然写了 `Content-Type: text/event-stream`，但 body 是整块下发的，
客户端只能等整段完成才解析渲染。链路在代理这一层被缓冲。

passthrough 分支：`is_streaming = false` 写死（`src/proxy.rs:109`），无论客户端
是否传 `stream:true`，都走 `resp.bytes().await` 整块返回。

## 2. 改造目标

让两条路径都真正边读上游、边下发，客户端能看到 token 逐段到达：
- **Responses 路径**（`is_responses && is_sse && status.is_success()`）：边读
  上游 SSE、边翻译、边下发。
- **passthrough 路径**：从 body 解析 `stream` 字段，若是流式则用
  `hyper::Body::wrap_stream` 直接把上游 `bytes_stream()` 透出去，不翻译、不缓冲。

## 3. 技术方案

### 3.1 依赖

**不引入新依赖**。原计划用 `async-stream` 的 `stream!` 宏，但当前构建环境无法
联网拉取（sandbox 无网）。改为复用已有的 `futures-util`：用
`futures_util::stream::poll_fn` 手写状态机即可覆盖逐事件 yield 的需求，无新
crate。

已确认环境：
- warp 0.3.7 默认拉了 hyper 0.14 的 `stream` feature（已查 warp 的 Cargo.toml）。
- `warp::hyper::Body::wrap_stream` 现成可用，无需额外给本 crate 加 hyper 依赖
  或开 feature。
- `futures-util = "0.3"` 本来就在依赖里，`poll_fn` 与 `StreamExt::poll_next_unpin`
  均可用，无版本风险。

### 3.2 返回类型调整

- `handle_request` 的 `Result<impl Reply, Rejection>` 保留不变（`Reply` 对
  `Response<hyper::Body>` 也成立，warp 不要求两条分支类型统一）。
- 两条流式分支都构造 `warp::http::Response<warp::hyper::Body>`，body 用
  `warp::hyper::Body::wrap_stream(stream)` 包装一个
  `Stream<Item = Result<Bytes, std::io::Error>>`。
- 非流式分支保持返回 `Response<Bytes>`。

### 3.3 Responses 路径流式构造

用 `async_stream::stream!` 包裹现有 `while let Some(chunk_result)` 循环：

```rust
use async_stream::stream;
use futures_util::StreamExt;

let s = stream! {
    let mut stream_state = responses_compat::StreamState::new();
    let mut buffer = String::new();
    let mut upstream = resp.bytes_stream();
    let mut aborted = false;

    while let Some(chunk_result) = upstream.next().await {
        match chunk_result {
            Ok(chunk) => {
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buffer.find("\n\n") {
                    let event_block = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();
                    if let Some(data_line) = event_block
                        .lines()
                        .find(|l| l.starts_with("data: "))
                    {
                        let data = &data_line[6..];
                        let payload = if data == "[DONE]" { b"null" } else { data.as_bytes() };
                        for ev in responses_compat::translate_stream_chunk(
                            payload, &mut stream_state, "",
                        ) {
                            yield Ok::<_, std::io::Error>(Bytes::from(ev));
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Stream read error: {}", e);
                aborted = true;
                break;
            }
        }
    }

    // 收尾：仅上游正常结束时补发 response.completed；中途断流不补发
    if !aborted && !stream_state.emitted_completed {
        for ev in responses_compat::translate_stream_chunk(
            b"null", &mut stream_state, "",
        ) {
            yield Ok(Bytes::from(ev));
        }
    }
};

let body = warp::hyper::Body::wrap_stream(s);
```

要点：
- `translate_stream_chunk` 签名不变，它本来就是无副作用翻译。
- `StreamState` 在闭包内持有 `&mut`，事件按生成顺序 yield，天然保序。
- 不再 `all_events.join("")`，事件翻译完即下发。
- `aborted` 标志：上游 chunk 读失败时置位，收尾时不补发 `response.completed`，
  让客户端识别为未完成并可重试。

### 3.4 passthrough 路径流式化

passthrough 分支原本走 `resp.bytes().await` 整块。改为：
- 在判定 `is_responses` 之前/同时，对 passthrough body 解析 `stream` 字段
  （`serde_json` 解析 `stream` 取 bool，无法解析视为非流式）。
- 若是流式且 `status.is_success()`：构造 `warp::hyper::Body`，body 用
  `warp::hyper::Body::wrap_stream(resp.bytes_stream().map_ok(Bytes::from))`，
  不翻译、不缓冲，纯透传。头部透传逻辑同现状。
- 若非流式或非成功：维持 `resp.bytes().await` 整块返回（与现状一致）。

### 3.5 头部处理

两条流式分支头部仍需在发 body 前一次性写好：
- 透传 `resp_headers` 里除 hop-by-hop 之外的头（逻辑同现状）。
- Responses 翻译分支：主动 `insert`
  `Content-Type: text/event-stream`、`Cache-Control: no-cache`；过滤掉上游的
  `content-type`/`cache-control`。
- passthrough 流式分支：不加自定 content-type，沿用上游的（已经是
  `text/event-stream`），只需过滤 hop-by-hop。
- 全部头部设置完毕后 `.body(wrap_stream(s))`。

### 3.6 错误语义

- 上游 4xx/5xx 不进入流式分支（`status.is_success()` 已过滤），仍走非流式
  错误透传，能 `Err(Rejection)` 转 502。
- 流中途上游 `chunk_result == Err`：Responses 路径设 `aborted` 不补发
  `response.completed`；passthrough 路径 stream 直接 break 结束（hyper 会给
  客户端一个截断的 body，客户端按 SSE 协议识别未完成）。
- "首个 chunk 读取失败"在 send 阶段已能判断（请求 send 失败 → 现有 BadGateway
  分支）。
- HTTP/1.1 chunked：hyper 在 body 是 stream 时自动用 chunked transfer-encoding，
  客户端按 SSE 边读边解析。

### 3.7 不变项

- `responses_compat.rs` 完全不改。
- `key_pool.rs` / `metrics.rs` / `health.rs` / `codex_config.rs` 不改。
- `main.rs` 路由层不改（`handle_request` 签名不变）。
- 非 SSE 成功响应（`is_responses && !is_sse`，即上游非流式）不改，维持整块
  翻译返回。

## 4. 修改清单

1. `Cargo.toml`：`[dependencies]` 增加 `async-stream = "0.3"`。
2. `src/proxy.rs`：
   - `handle_streaming_response` 返回类型改
     `Result<warp::http::Response<warp::hyper::Body>, Rejection>`。
   - 用 `async_stream::stream!` 重写事件循环，逐事件 `yield`。
   - 头部构建逻辑保留，body 改为 `wrap_stream`。
   - 加 `aborted` 标志：断流时不补发 `response.completed`。
   - passthrough 分支：从 body 解析 `stream`；流式成功时用
     `wrap_stream(resp.bytes_stream().map_ok(Bytes::from))` 透传。
   - 顶部 `use` 复核：新增 `warp::hyper::Body` 路径、`async_stream`。
3. 无其它文件改动。

## 5. 风险与缓解

**R1 错误语义退化（高风险）**
流式后头已发，中途上游断流只能截断。缓解：Responses 路径 `aborted` 标志，
断流时不补发 `response.completed`，让客户端识别为未完成并可重试。passthrough
路径让 hyper 自然截断 body。需在自测中验证 Codex 行为（是否会把"无
response.completed 的流结束"当错误）。

**R2 背压/连接 hang（中风险）**
客户端读得慢时 hyper channel 会被动背压，上游 read 暂停。这是预期行为，不会
爆内存。缓解：保留现有 `pool_idle_timeout`。

**R3 async-stream 状态机 panic 传染（低风险）**
`stream!` 生成的状态机若 panic 影响当前连接 task。缓解：翻译函数纯逻辑无
unwinding 风险点，`String::from_utf8_lossy` 不 panic，保持无 `unwrap`。

**R4 warp/hyper 版本匹配（低风险）**
warp 0.3.7 默认依赖 hyper 0.14 且带 `stream` feature，`wrap_stream` 现成可用。
已通过 Cargo.lock 和 warp Cargo.toml 确认，无升级风险。

**R5 SSE 边界解析回归（中风险）**
现有 `\n\n` 分块逻辑搬到 `stream!` 内，行为需与原 buffer 处理一致。缓解：
逐字搬移，不重构解析；自测覆盖跨 chunk 事件。

**R6 passthrough 透传破坏非流式行为（低风险）**
仅当 `stream:true` 且上游 SSE 时走流式，其它维持 `bytes().await`。判定条件
复用现有 `is_sse`（上游 content-type），不会误伤非流式调用。

**R7 passthrough 也无 status 修正窗口（低风险）**
passthrough 流式分支头已发，中途上游 5xx 已无修正窗口，但上游 5xx 在
`status.is_success()` 阶段就会走非流式返回，流中途断流等同 Responses 路径
处理。一致、无新增风险。

## 6. 验证计划

1. `cargo build --release` 通过。
2. `cargo clippy -- -D warnings` 通过（如项目已用）。
3. 启动代理 + Codex CLI 实跑推理，观察是否逐字渲染（不再"突然全出"）。
4. 实跑 `/v1/chat/completions` 直连带 `stream:true`，观察逐字渲染。
5. 制造上游中途断流（手动 kill 上游或 curl 模拟截断），验证代理不补发
   `response.completed`，Codex 不把截断当成功。
6. 跨 chunk SSE：暂用 mitmproxy/socat 模拟拆包，验证翻译不丢事件。
7. 非 Responses 非流式回归：`curl /v1/chat/completions` 不带 `stream`，仍
   走整块返回，行为无变化。

## 7. 不在本期范围

- 流式错误增加标准 `response.failed` / `response.error` 事件（Codex 协议未
  明确支持，先靠"不发 completed"信号）。
- 上游首 chunk 嗅探做更细致的 200/SSE 判定后再发头（当前 send 阶段已能判
  status；payload 级异常暂不处理）。


## 8. 实现状态（与最新代码同步）

最终落地结果与上面规划一致，但**不引入新依赖**，改为手写
`futures_util::stream::poll_fn` 状态机：

- `handle_streaming_response`（Responses 路径）：用一个 `St` 结构持有上游
  `bytes_stream()`（一次性 `Box::pin`）、`StreamState`、buffer、pending
  `VecDeque<Bytes>`、`upstream_done` 标志。每次 poll 先 drain pending、再切 buffer
  里的完整 `\n\n` 事件翻译入队、必要时拉下一个上游 chunk 翻译并 yield。上游
  `Err` 时直接 `return Ready(None)`（原本设的 `aborted` 已被简化为死分支，现在直
  接结束流），**不**补发 `response.completed`；上游 `None` 时若
  `!emitted_completed` 补发后再结束。
- `model` 字段透传：从请求 body 提取 `model`（字符串值），传入
  `handle_streaming_response` 与 `translate_response`；旧实现传空字符串，导致翻译
  出来的 `response.created` / `response.completed` 里 `model` 为空，本次修复。
- `handle_passthrough_stream`（passthrough 路径）：`resp.bytes_stream()` 直接
  `map(|r| r.map_err(...))` 后 `wrap_stream` 透传，零翻译零缓冲。
- passthrough 的 `is_streaming` 从 body 解析 `stream` 字段，不再写死 `false`。
- 头部构建、错误码处理（4xx/5xx 仍走非流式整块透传）与规划一致。

### 验证

- `cargo build --release`：通过。
- `cargo test --release`：7 个测试全过，其中新增 3 个行为测试覆盖
  「正常流含 created/delta/completed」「aborted 流不含 completed」
  「model 字段透传到 created/completed」。
- `cargo clippy --release`：**无任何 warning**。本次顺手清掉了仓库里遗留的
  3 个 clippy 告警：
  + `src/key_pool.rs` 中 `s as u64` 冗余 cast（retry_after 已是 `u64`），顺道把
    `.map(|s| s).unwrap_or(base).min(max)` 简化为
    `retry_after.unwrap_or(base).min(max)`；
  + `src/main.rs` 中 `let _ = cooldown_task;`/`let _ = bad_key_task;`
    `clippy::let_underscore_future`：改为显式 `drop(...)`；
  + `src/codex_config.rs` 测试模块中未使用的 `use super::*;`，直接删除。

### 受环境限制未能覆盖

- 端到端实跑：sandbox 无网、没有绑定端口权限，无法本地起代理 + mock 上游做
  端到端逐 token 渲染验证。建议在本机 `./target/release/nvidia-key-proxy`
  启动后用 Codex 实跑一次确认「逐字渲染」效果。
- 代理启动时 `system-configuration` crate 在 sandbox 内 panic（macOS 系统代理
  动态库不可访问），与本次改造无关：这是 reqwest 读 macOS 系统代理的既有行为
  在受限环境下触发的，本机正常环境不会出现。

### 受影响文件清单（本次改造）

- `src/proxy.rs`：真流式实现主体；新增 `model` 透传参数。
- `src/responses_compat.rs`：追加 `#[cfg(test)] mod streaming_behavior_tests`
  与 3 个行为测试；翻译逻辑本身未改。
- `src/main.rs`：清掉 `let _ =` future 告警，改为显式 `drop`。
- `src/key_pool.rs`：清掉冗余 `as u64` cast，简化 `mark_bad` cooldown 计算。
- `src/codex_config.rs`：删除未使用的 `use super::*;`。
- `docs/streaming-refactor.md`：本规划。


## 9. 401 重试与失效 key 处理

### 背景

用户反馈偶发 `unexpected status 403 Forbidden: Authorization failed`。诊断：
403 是 NVIDIA 上游对某把 key 偶发拒绝（账号级限制 / 区域 / key 失效）。
最终的实现里，重试只覆盖 401（最清晰的"这把 key 本身被拒"信号）；
429/403 属于确定性上游决策（限流 / 禁止），重试只会把整个池子都烧光，
因此透传给客户端、不重试。

### 实际策略

`handle_request` 的上游请求段包进 `attempt loop`：

- 上游 **401 且无 `Retry-After`** → `mark_failure(key)` 记一次失败，
  换一把健康 key 重发同一请求，sleep 指数退避
  (`backoff_base_ms * 2^(attempt-1)`，cap `backoff_max_ms`)；
- 每把 key 维护 `fail_count`，连续达 `auth_fail_threshold` →
  `KeyState::Disabled`，acquire 时跳过；
- **401 带 `Retry-After`**：显式冷却信号，`mark_bad` + 透传，不重试；
- **429 / 403**（带不带 `Retry-After`）：不重试。带 `Retry-After` 时 `mark_bad`
  冷却该 key；不带则不冷却，直接透传（确定性上游决策，不该烧整个池子）；
- 网络层错误 / timeout 也走另一把 key（不算 auth 失败计数，但 `mark_bad`
  短冷却）；
- 达 `max_retries` 仍失败，把最后一次上游响应透传给客户端。

### 探测复活

`HealthChecker::check_bad_keys` 现在同时探测 `Disabled` 的 key；探测成功
走 `recover_disabled`（即 `mark_good`），失败时保持 `Disabled`，下一轮再试。

### 新增配置（`[retry]`）

| 字段 | 默认 | 含义 |
|---|---|---|
| `max_retries` | 3 | 单个客户端请求最多额外重试次数（不含首次） |
| `backoff_base_ms` | 200 | 首次重试基础等待 ms |
| `backoff_max_ms` | 2000 | 单次退避上限 |
| `auth_fail_threshold` | 3 | 连续 auth 失败达此值则 key 被置 Disabled |

env override：`NVIDIA_RETRY_MAX_RETRIES` / `NVIDIA_RETRY_BACKOFF_BASE_MS` /
`NVIDIA_RETRY_BACKOFF_MAX_MS` / `NVIDIA_RETRY_AUTH_FAIL_THRESHOLD`。

### 测试

`key_pool::retry_state_machine_tests`：

- `mark_failure` 阈值以下不禁用；
- 达阈值进 `Disabled` 且 `acquire` 跳过；
- `recover_disabled` 把 Disabled key 复活回 `Active`；
- `get_disabled_keys` 仅列出 Disabled。

### 相关文件

- `src/config.rs`：新增 `RetryConfig`。
- `src/key_pool.rs`：新增 `mark_failure` / `get_disabled_keys` /
  `recover_disabled`、`KeyStateHandle::fail_count`。
- `src/proxy.rs`：`handle_request` 重写为重试循环，新增 `build_upstream_request`
  与 `backoff_sleep` 辅助方法。
- `src/health.rs`：`check_bad_keys` 同时探测 Disabled key。
- `config.toml.example`：新增 `[retry]` 段示例。


## 10. 代码评审第二轮修复

针对评审发现的 7 个问题逐条修复：

**#1 重试抽回同一把 key**：`mark_failure` 在阈值未达前也 `add_weight(-w/2)`
降权，下轮加权随机几乎不会再选中刚失败的 key。已加单测
`mark_failure_halves_weight_even_below_threshold`。

**#2 Disabled key 缺自动兜底复活**：`KeyState::Disabled` 加 `ready_at` 字段，

`cool_down_task` 现在既复活过期的 `Cooling`，也复活过期的 `Disabled`。复活后
`weight=1`、`fail_count=0`。`ready_at` 用 `cooldown.max_seconds` 作为兜底时长
（默认 600s），避免探活端点不可用时 pool 永久缩水。

**#3 Cargo.lock 倒退**：手动修正 `rustix` 与 `tempfile` 节里的
`windows-sys 0.52.0` → `0.61.2`，恢复与 HEAD 一致。`ring` 节里的 0.52.0 是
它真实依赖,不动。

**#4 指标虚高**：新增 `proxy_retry_attempt_failures_total` 计数器，重试过程中
的 attempt 失败用 `inc_retry_attempt_failure()`；只有 budget 耗尽/无其它 key
而透传给客户端时才 `inc_error()`。`proxy_requests_errors_total` 现在反映的是
"客户端可见失败"。

**#5 重试范围收窄**：仅 401 重试（最明确的"该 key 被拒"信号）。
403 / 429 不重试——它们是 NVIDIA 的确定性拒绝（forbidden / 限流），换 key
也只会烧 pool。带 `Retry-After` 的 401 走 `mark_bad` + 透传。

**#6 最后一次重试前不再退避**：`backoff_sleep` 只在 `will_retry_again` 为
真时调用，最后一次 attempt 立即触发，减少无意义延迟。

**#7 acquire 读写路径优化**：`acquire` / `get_bad_keys` / `get_disabled_keys`
不再两次进入 `handle.state()` + `handle.weight()`，改成对每把 key 只取一次
`inner.read()` 然后一次性读出 `state` 与 `weight`。

### 验证

- `cargo build --release` 通过。
- `cargo test --release` 12 个测试全过，含新增
  `mark_failure_halves_weight_even_below_threshold`。
- `cargo clippy --release` 无 warning。
