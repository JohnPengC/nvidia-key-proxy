use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use tracing::{debug, error, warn};
use warp::{self, Rejection, Reply};

use crate::failure_log;
use crate::key_pool::{mask_key, KeyPool, KeyStateHandle};
use crate::metrics::Metrics;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_util::{Stream, StreamExt};
use reqwest::header::{self as req_header, HeaderValue as ReqHeaderValue};
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

/// 进程级单调计数器，用于为每次失败生成 `req_id`。
/// 单调递增、永不复位——为同一逻辑客户端请求的重试链提供稳定关联 id
///（同一请求的每次重试都会拿到下一个计数器值，因此它们在排序时会排在一起）。
static REQ_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_req_id() -> u64 {
    REQ_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// 每个模型并发槽的 RAII 持有者。`Drop` 会释放 `OwnedSemaphorePermit`，
/// *并*刷新 `proxy_concurrency_current`，使 gauge 在请求结束时下降——而不是
/// 在下一个请求开始时才下降。
///
/// 该槽在每个客户端请求时获取一次（在上游尝试循环之前），并跨所有重试尝试持有；
/// 只有最终处置会释放它。对于流式成功，guard 会被**移入响应 body 流**
///（见 `GuardedStream`），因此槽在客户端排空完 body 后才释放，而不是在
/// `handle_request` 返回时；否则 NVIDIA 的上游 worker 在代理释放 permit 后仍
/// 在流式输出 token，而刚获取下一个槽的请求就会击穿该 limiter 本要防止的
/// 每模型 worker 上限（`503 ResourceExhausted`）。
struct ConcurrencyGuard {
    /// 仅为持有其 `Drop`（把槽归还给信号量）。从不直接读取——
    /// guard 的职责就是持有该 permit 的生命周期。
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    model: String,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        // `OwnedSemaphorePermit` 自身的 Drop 会释放槽；在确认要释放后再读取
        // `available_permits`。我们无法观察自身 drop 的顺序，因此直接计算释放后的值：
        // 当前可用数 + 1（我们归还的这个槽）。`min(limit)` 限制的是边缘情况——
        // 即一个并发释放已经把我们恢复到满额时。
        let post = (self.semaphore.available_permits() as i64).saturating_add(1);
        self.metrics.set_concurrency_current(&self.model, post);
        // `self.permit` 在此处 drop → 信号量真正获得该 permit。gauge 被设为
        // 一个假定该释放已发生的值；并发的 acquire 会自行再次减去它，因此在
        // 竞争下 gauge 可能瞬时多报 1。可接受：一个槽的瞬时偏差对一个用于
        // 显示大致并发压力的 gauge 而言无伤大雅。
    }
}

/// 用一个 `ConcurrencyGuard` 包装 pinned 流，使 guard（及其信号量 permit）
/// 的生命周期延续到客户端仍在读取 body 之时。drop `GuardedStream` 会同时 drop
/// 内部流和 guard。
struct GuardedStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    _guard: ConcurrencyGuard,
}

impl Stream for GuardedStream {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// key 的 `in_flight` 计数器的 RAII guard。`acquire_*` 路径对每个获取到的
/// `KeyStateHandle` 恰好 bump 一次 `in_flight`；`handle_request` 重试循环里
/// 过去每个显式返回路径都要手工调用 `key_handle.dec_in_flight()`——但当 warp
/// 在 `await` 中途 drop handler future（客户端断连）时，那些 `match result` 分支
/// 根本不会执行，`in_flight` 因此泄漏 +1，永久压低该 key 的获取分数
///（`in_flight * IN_FLIGHT_PENALTY`），直到进程重启。
///
/// `InFlightGuard` 通过持有该 handle 并在 `Drop` 中恰好 decrement 一次来修复
/// 此问题——除非 `release()`/`swap()` 已经做过。显式 `dec` 位置会提前调用
/// `release()`，使该 key 的分数能立即看到退款（而非等到帧拆解）；`swap()` 支持
/// 重试切换（`key_handle = next.clone()`）位置。幂等——调用两次 `release()`、
/// 或 `release()` 后再 `Drop` 都是安全的。
///
/// token 饥饿探针循环（在 guard 生命周期之上，仍使用裸 `KeyStateHandle`）未改动：
/// 那些候选尚未被包装，且自身已一对一平衡 inc/dec。
struct InFlightGuard {
    handle: KeyStateHandle,
    released: bool,
}

impl InFlightGuard {
    /// 包装一个刚获取的 handle。该 handle 已被 `acquire_excluding` 做过
    /// `inc_in_flight`——guard 承担恰好一次匹配 `dec` 的职责。
    fn new(handle: KeyStateHandle) -> Self {
        Self {
            handle,
            released: false,
        }
    }

    /// 幂等地 decrement `in_flight`。可多次调用，且 `Drop` 跟在后面也安全——
    /// 只有第一次调用真正 decrement。
    fn release(&mut self) {
        if !self.released {
            self.handle.dec_in_flight();
            self.released = true;
        }
    }

    /// 借用被包装的 handle，用于 `key().await` 日志 key 提取、ptr-hash 身份
    /// 等。（`process_upstream_attempt` 接收 `&KeyStateHandle`，因此该 guard
    /// 对它透明。）
    fn handle(&self) -> &KeyStateHandle {
        &self.handle
    }

    /// 把被包装的 handle 切换为 `new`：释放旧的（提前退款，让旧 key 的分数在
    /// 下一轮获取前恢复），然后取走 `new` 的所有权（它已被 `acquire_retry_key`
    /// `inc_in_flight`），并把 `released` 标志复位，使稍后的 `release()` / `Drop`
    /// 恰好 decrement 一次*新* handle。
    fn swap(&mut self, new: KeyStateHandle) {
        self.release();
        self.handle = new;
        self.released = false;
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.release();
    }
}


/// 我们会把模型名原样带入 Prometheus 标签的最大长度。任何超出该长度（无论
/// 如何到达）的名称都会被塌缩为 `"_unknown"`——即使是运营者配置的、意外超长
/// 的别名——这样单个 `model` 就无法膨胀 /metrics 输出。
const METRICS_MODEL_MAX_LEN: usize = 64;

/// 当某 `model` 不在运营者白名单（见 `Proxy::known_models`）中、或为空 / 超
/// 长时使用的哨兵标签值。把所有这类值都塌缩到一个桶里，可让 metrics 标签维度
/// 维持有界，无论客户端发什么。
const METRICS_UNKNOWN_MODEL: &str = "_unknown";

/// Proxy 负责从池中取一个 key，把请求转发给 NVIDIA API。
#[derive(Clone)]
pub struct Proxy {
    base_url: String,
    key_pool: Arc<KeyPool>,
    metrics: Arc<Metrics>,
    client: reqwest::Client,
    /// 单请求超时（秒）。0 表示禁用；流式请求总是绕过它（长时间生成 token
    /// 可能超过 60s）。
    request_timeout_secs: u64,
    /// 流式请求的独立超时。> 0 时流式调用用它替代 request_timeout_secs；
    /// 0 = 使用 request_timeout_secs。
    request_timeout_streaming_secs: u64,
    /// 重试 / 上游错误策略（指数退避 + key 禁用阈值）。
    retry_config: crate::config::RetryConfig,
    /// 在单个 key 的 token bucket 上等待的最大毫秒数，超时则切换到另一个 key。
    /// 0 表示完全禁用该超时。
    token_wait_ms: u64,
    /// 客户端可见模型名 → NVIDIA 就绪模型 id 的映射表。当请求带有的 `model`
    /// 命中此处某个 key 时，代理会在转发上游前把该字段改写为映射值。未命中
    /// 的名称原样转发。
    model_aliases: std::collections::HashMap<String, String>,
    /// 冻结的白名单：允许作为 Prometheus 标签出现、而不被塌缩为 `"_unknown"`
    /// 的模型名。在构造时由 `model_aliases` 的 keys+values、
    /// `empty_shell_pre_clamp_max_tokens` 的 keys 与 `concurrency_limit_per_model`
    /// 的 keys 取并集构建——即运营者在配置中显式点名的每个模型。客户端发来的、
    /// 不在此集合中（或超过 `LABEL_MODEL_MAX_LEN`）的 `model`，在 metrics 中
    /// 被上报为 `"_unknown"`，使攻击者无法通过每次请求发一个新 `model` 来铸
    /// 无界的标签基数（基数 DoS）。
    known_models: std::collections::HashSet<String>,
    /// 上游 `/v1/models` 目录的缓存。NVIDIA 不需鉴权即可应答该端点，因此我们
    /// 透传并对结果做 memoize，避免在每个客户端启动时都重新探上游。
    models_cache: Arc<Mutex<Option<serde_json::Value>>>,
    /// 在流式 chat 请求中注入 `stream_options.include_usage: true`，使 NVIDIA
    /// 在流结束时返回 token 用量。NVIDIA/OpenAI 仅在该标志置位时才 emit usage；
    /// 不注入的话我们的 token 指标对（常见的）流式场景恒为 0。
    inject_stream_usage: bool,
    /// 当非流式 2xx body 到达但 `choices` 数组为空时，合成一个占位 `choices`
    /// 条目，使下游消费者（尤其是 cc-switch 的 `openai_to_anthropic`）不会在
    /// 空数组上硬报错。完整理由见 `Config::synthesize_empty_choices`。仅非流式
    /// 非 SSE 成功路径受影响，且只 inspect 一个有界的前缀。
    synthesize_empty_choices: bool,
    /// 当 NVIDIA 返回空壳 2xx 软失败（`choices:[]`、`usage` 为 null 且 `id` 为
    /// 空）时，用 clamp 后的 `max_tokens` 重试一次。完整理由见
    /// `Config::retry_on_empty_shell`。
    retry_on_empty_shell: bool,
    /// 用于那次 post-shell 重试的 `max_tokens` 值。仅重试尝试受影响；原始请求
    /// 原样转发。
    empty_shell_retry_max_tokens: u64,
    /// 空壳软失败的重试次数。
    empty_shell_retry_attempts: u32,
    /// 空壳重试之间指数退避的基础延迟（毫秒）。
    empty_shell_retry_backoff_base_ms: u64,
    /// 单次空壳重试退避睡眠的上限（毫秒）。
    empty_shell_retry_backoff_max_ms: u64,
    /// 重试空壳失败前，先探上游 /v1/models。
    empty_shell_probe_before_retry: bool,
    /// 首次尝试时预 clamp 的、按模型的 max_tokens 上限。
    empty_shell_pre_clamp_max_tokens: std::collections::HashMap<String, u64>,
    /// 高于该 prompt token 阈值时跳过空壳重试（直接外抛 502）。见
    /// Config::empty_shell_no_retry_prompt_tokens。
    empty_shell_no_retry_prompt_tokens: u64,
    /// 估算 prompt token 数的硬上限；达到/超过时在任何上游调用前就以 413 拒绝。
    /// 见 Config::request_max_prompt_tokens。
    request_max_prompt_tokens: u64,
    /// 估算 **多模态** 请求（image_url / input_audio 内容块）prompt 大小时用到
    /// 的、按部分类型的粗粒度 token 成本，使 413 硬上限和空壳不重试软限制不被
    /// 内联 base64（会把估算吹爆从而误拒合法请求）或仅 URL 引用（会少算）欺骗。
    /// 派生自 `Config::est_*` 字段。见 `EstPromptCosts`。
    est_costs: EstPromptCosts,
    /// 连续空壳 / 504 / 503 响应的断路器阈值。
    circuit_breaker_threshold: u32,
    /// 断路器是否按 (key, model) 对跟踪失败。
    circuit_breaker_per_model: bool,
    /// 是否在 503 Service Unavailable（NVIDIA Worker 本地请求上限达到）时重试。
    /// 启用后，503 响应会用指数退避在另一个 key 上重试（与 401 重试共用同一
    /// 重试预算）。
    retry_on_503: bool,
    /// 重试 503 失败前，先探上游 /v1/models。
    probe_before_503_retry: bool,
    /// 是否在 500 Internal Server Error（GLM "Failed to generate completions"
    /// 一类瞬态 per-request worker 失败）时重试。镜像 `retry_on_503`：当预算
    /// 仍有时在另一个 key 上带退避重试。
    retry_on_500: bool,
    /// 校验用 max_tokens 上限。请求中 max_tokens 大于此值时该字段被丢弃。
    max_tokens_max: u64,
    /// 每模型最大并发上游请求数。
    concurrency_limit_per_model: std::collections::HashMap<String, u32>,
    /// 等待一个并发信号量槽的最长时间（毫秒）。
    concurrency_queue_timeout_ms: u64,
    /// 重试抖动（毫秒）。加到指数退避之上。
    retry_jitter_ms: u64,
}

/// 单次上游尝试的结果。`Retry` 仅用于不带 `Retry-After` 头且调用方仍有重试
/// 预算时的 401。
enum AttemptResponse {
    Done(warp::reply::Response),
    Retry,
}

/// 2xx 非 SSE ChatCompletions 响应在 empty-choices peek 之后的处置。`Response`
/// 是正常透传（当 body 本身就有空的 `choices` + 真实 `usage` 时为合成的占位，
/// 或零所有权透传）。`ShellFailure` 是 NVIDIA 的"空壳"软失败：`choices:[]`、
/// `usage` 为 NONE、`id` 为空——没有真正的推理发生。调用方
///（`process_upstream_attempt`）决定是否用 clamp 后的 `max_tokens` 重试一次
///（见 `Config::retry_on_empty_shell`），还是外抛 502——而不是外抛那个让
/// Claude Code 的 `/compact` 打印 "summarization produced empty response" 的、
/// 具有误导性的空内容占位。
enum SuccessOutcome {
    Response(warp::reply::Response),
    /// 上游返回了一个空壳软失败。携带解析后的 body，使调用方在自己选择不（或
    /// 无法）重试时可从中提取字段构造 502 detail 消息。
    ShellFailure { body: serde_json::Value },
}

/// 把一个 empty-`choices` 2xx body 判定为 NVIDIA 空壳软失败。
///
/// NVIDIA 的软失败空壳（在 GLM 类模型上观察到，特见于大 `max_tokens` 的
/// `/compact`）长这样：
///   `{"choices":[],"id":"","model":"","created":0,"object":"chat.completion",
///     "service_tier":null,"system_fingerprint":null,"usage":null}`
/// 每个有意义的字段都是 null/空，因为没有推理真正发生——模型被静默 drop，
/// 而非运行。tell 是 `usage:null`：一个*合法的*压缩空 choices body
///（模型运行了、产出 0 token）仍会带一个真实 `usage` 对象。因此本函数仅在
/// `choices` 为空且 `usage` 为 null/缺省时返回 true。我们还要求 `id` 接近空
/// 且 `created` 为 0 作为额外的佐证信号。
fn is_empty_shell_failure(body: &serde_json::Value) -> bool {
    // choices 必须存在且为空。
    let choices_empty = body
        .get("choices")
        .and_then(|c| c.as_array())
        .map(|a| a.is_empty())
        .unwrap_or(false);
    if !choices_empty {
        return false;
    }
    // usage 为 null / 缺省 / 空对象 → 没有推理发生。
    let usage_null = match body.get("usage") {
        None | Some(serde_json::Value::Null) => true,
        Some(u) if u.as_object().map(|o| o.is_empty()).unwrap_or(false) => true,
        Some(_) => false,
    };
    if !usage_null {
        return false;
    }
    // 空 id 作为第二个佐证信号。
    let id_empty = match body.get("id") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(s)) => s.is_empty(),
        Some(_) => false,
    };
    if !id_empty {
        return false;
    }
    // `created == 0` 是一道**硬闸门**，而不只是第三个信号：NVIDIA 的空壳
    // 软失败会 emit `created: 0`（没有推理发生 → 没有服务器时间戳），而我们
    // 刻意要求它，以免把一个真实的、`choices` 恰好为空且 `usage` 为 null
    // 的 `stream:false` 完成误判为空壳（对某些 no-op / tool-only 路径是合法的）。
    // 在没有"实测证据表明 NVIDIA 在空壳上会带真实时间戳"之前放开这道闸门，
    // 会冒着把真实完成判成空壳、对健康响应外抛 502 的风险。见
    // memory/tune-from-data-not-assumption。
    let created_zero = match body.get("created") {
        Some(serde_json::Value::Number(n)) => n.as_u64() == Some(0),
        _ => false,
    };
    created_zero
}

/// 单次上游尝试中可能发生的错误。
#[derive(Debug)]
enum AttemptError {
    Network(reqwest::Error),
    Timeout,
    BodyRead(reqwest::Error),
    /// 不可重试的 rejection，应原样返回给 client。
    Fatal(Rejection),
}
impl warp::reject::Reject for AttemptError {}

/// 把 JSON 值里任何数值型的 `code` 字段递归地强转为字符串。NVIDIA 返回
/// `{"error":{"code":503,...}}`，其中 `code` 是数字，但 OpenAI 的 schema
///（以及 Cursor 这类 Go 客户端）期望 `code` 是字符串。本函数原地遍历对象与
/// 数组。
fn coerce_error_code_json(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            if let Some(code) = map.get_mut("code") {
                if code.is_i64() || code.is_u64() {
                    let n = code.as_i64().or_else(|| code.as_u64().map(|x| x as i64));
                    if let Some(n) = n {
                        *code = serde_json::Value::String(n.to_string());
                    } else if let Some(f) = code.as_f64() {
                        *code = serde_json::Value::String(f.to_string());
                    }
                }
            }
            for (_, child) in map.iter_mut() {
                coerce_error_code_json(child);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr.iter_mut() {
                coerce_error_code_json(item);
            }
        }
        _ => {}
    }
}

/// 原地强转一整个 JSON body。返回一个新的 Bytes，其中 `error.code`（若存在
/// 且为数值）已被转为字符串；若 body 不是合法 JSON 或重新序列化失败，则原样
/// 返回原始字节。
fn coerce_json_body(body: &Bytes) -> Bytes {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(mut v) => {
            coerce_error_code_json(&mut v);
            serde_json::to_vec(&v).map(Bytes::from).unwrap_or_else(|_| body.clone())
        }
        Err(_) => body.clone(),
    }
}

/// 强转 SSE chunk 里的 `error.code`。SSE 事件是按行划分的
///（`data: <json>\n\n`）；我们处理每个 `data:` 行：解析其 JSON payload、强转、
/// 再重新序列化。不是 `data:` 行、或 payload 不是合法 JSON 的行原样透传。
///
/// 本函数对单个 chunk 操作、不带跨 chunk 状态。跨 chunk 边界拆分的 `data:` 行
/// 会让那一段 payload 的 JSON 解析失败并被原样转发；实践上 NVIDIA 的错误事件
/// 很短、单一 chunk 到达。
fn coerce_sse_chunk(chunk: &Bytes) -> Bytes {
    // 快路径：绝大多数流式 chunk 是 token delta、没有 `code` 字段。当该 chunk
    // 确定不含 `"code"` 子串时，直接跳过逐行 JSON 解析。
    let text = match std::str::from_utf8(chunk) {
        Ok(s) => s,
        Err(_) => return chunk.clone(),
    };
    if !text.contains("\"code\"") {
        return chunk.clone();
    }
    let mut changed = false;
    let mut out = String::with_capacity(chunk.len());

    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches('\n');
        let trimmed = trimmed.trim_end_matches('\r');
        if let Some(payload) = trimmed.strip_prefix("data:") {
            let json_str = payload.trim_start();
            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(json_str) {
                let before = serde_json::to_string(&v).unwrap_or_default();
                coerce_error_code_json(&mut v);
                let after = serde_json::to_string(&v).unwrap_or_default();
                if before != after {
                    out.push_str("data: ");
                    out.push_str(&after);
                    if line.ends_with('\n') {
                        out.push('\n');
                    }
                    changed = true;
                    continue;
                }
            }
        }
        out.push_str(line);
    }

    if changed {
        Bytes::from(out)
    } else {
        chunk.clone()
    }
}

/// 构造一个带指定状态码与 OpenAI 风格 `{"error":{"message":..., "type":...}}`
/// body 的 JSON 错误响应。用于不支持的 API 形态（Responses / Anthropic
/// Messages），使客户端拿到一个明确的类型化信号、而非一个困惑的上游 404。
fn json_error(status: warp::http::StatusCode, message: &str) -> warp::reply::Response {
    let body = serde_json::json!({ "error": { "message": message, "type": "not_supported" } })
        .to_string();
    warp::http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(warp::hyper::Body::from(body))
        .unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(warp::hyper::Body::empty())
                .unwrap()
        })
}

/// 用任意 `serde_json::Value`（例如上游 `/v1/models` 目录）构造一个 JSON
/// 响应，在原样透传 NVIDIA 真实模型列表时使用。
fn json_value_response(status: warp::http::StatusCode, v: &serde_json::Value) -> warp::reply::Response {
    let body = serde_json::to_vec(v).unwrap_or_default();
    warp::http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(warp::hyper::Body::from(body))
        .unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(warp::hyper::Body::empty())
                .unwrap()
        })
}

/// 为日志构造一个紧凑、稳定的请求 body 摘要。捕获最可能与上游失败相关联的
/// 字段：model、message 数量、是否流式，以及 reasoning_effort / temperature /
/// max_tokens / top_p 的**真实值**（而非仅存在性）。摘要是 JSON 字符串，便于
/// `jq` 直接过滤。
///
/// 真值优先级：若字段是标量（数/串/布），原样记入；否则（对象/数组/JSON null）
/// 记一个 `"<present>"` 字符串占位，保留「该键存在」这一信号，便于 failure.log
/// 里区分「未设」与「设了但取不到标量」。
fn body_summary(parsed_body: &Option<serde_json::Value>) -> String {
    fn finalize(val: &serde_json::Value) -> serde_json::Value {
        match val {
            serde_json::Value::Number(_) | serde_json::Value::String(_) | serde_json::Value::Bool(_) => {
                val.clone()
            }
            serde_json::Value::Null => serde_json::Value::String("<present:null>".to_string()),
            // 对象/数组取不到标量，但「该键存在」对诊断仍是有用信号。
            _ => serde_json::Value::String("<present>".to_string()),
        }
    }
    let mut summary = serde_json::Map::new();
    if let Some(v) = parsed_body {
        if let Some(m) = v.get("model").and_then(|x| x.as_str()) {
            summary.insert("model".to_string(), serde_json::Value::String(m.to_string()));
        }
        if let Some(messages) = v.get("messages").and_then(|x| x.as_array()) {
            summary.insert(
                "messages".to_string(),
                serde_json::Value::Number(serde_json::Number::from(messages.len())),
            );
        }
        if let Some(stream) = v.get("stream").and_then(|x| x.as_bool()) {
            summary.insert("stream".to_string(), serde_json::Value::Bool(stream));
        }
        for field in ["reasoning_effort", "temperature", "max_tokens", "top_p"] {
            if let Some(val) = v.get(field) {
                summary.insert(field.to_string(), finalize(val));
            }
        }
    }
    serde_json::Value::Object(summary).to_string()
}

/// 当某个上游 2xx ChatCompletions 响应被识别为 NVIDIA 空壳软失败
///（`choices:[]`、`usage:null`、空 `id`——没有真正推理发生）时，emit 一条
/// 结构化 `failure.log` 记录。该记录携带足够证据，使根因可在 failure.log 内
/// 直接定位、无需再探 NVIDIA：
///   * `body_excerpt` —— 完整的上游响应 body（很小，封顶 2048）。其中
///     `usage:null` 与 `usage:{...}` 之别正是区分空壳与合法零 token 压缩响应
///     的 tell。
///   * `detail` —— 一个结构化指纹：请求侧（`max_tokens`、`stream`、`messages`
///     数、**粗略 prompt-token 估算**、`temperature`、`top_p`、
///     `reasoning_effort`、`tools`/`tool_choice`）、shell 侧（`id`、`usage`、
///     `created`、`choiceslen`）、重试处置（clamp 后的 `max_tokens`、尝试索引、
///     重试原因），以及当调用方提供时的每尝试上游 elapsed-ms。
///
/// 请求 messages 永不全量记录（其内可能含大段对话）；只记粗略 token 估算 +
/// 首/末 message 的预览。
///
/// `category` 区分首次尝试的空壳与重试空壳，便于 jq group-by 区分两者。
/// `retry_attempt` 是产生本空壳的尝试索引（0 → 原始请求），`retry_reason`
/// 为随后发生重试的原因（或 None 表示未重试）。`elapsed_ms` 是产生本空壳的
/// 尝试的上游往返耗时（当调用方持有时），未知时为 None。
#[allow(clippy::too_many_arguments)]
fn write_empty_shell_failure_log(
    upstream_body: &serde_json::Value,
    request_body: &bytes::Bytes,
    client_req_id: &str,
    model: &str,
    key_str: &str,
    upstream_url: &str,
    body_summary: &str,
    category: &str,
    retry_attempt: u32,
    retry_reason: Option<&str>,
    elapsed_ms: Option<u128>,
) {
    // 完整上游响应 body —— 空壳 body 很小（无生成），所以开销低。封顶只是以防
    // 上游膨胀。
    let body_excerpt = upstream_body
        .to_string()
        .chars()
        .take(2048)
        .collect::<String>();

    let req_fp = request_body_fingerprint(request_body);
    let shell_fp = shell_body_fingerprint(upstream_body);
    let detail = build_empty_shell_detail(
        &req_fp,
        &shell_fp,
        retry_attempt,
        retry_reason,
        elapsed_ms,
    );

    failure_log::write_failure(
        "POST",
        "/v1/chat/completions",
        upstream_url,
        model,
        key_str,
        Some(200),
        category,
        &detail,
        &body_excerpt,
        body_summary,
        retry_attempt,
        retry_reason,
        next_req_id(),
        client_req_id,
        Some("empty_choices"),
    );
}

/// chat-completions **请求** body 的紧凑、结构化指纹，用于 failure-log /
/// tracing 上下文。提取最常影响 NVIDIA 对 GLM 类模型软拒绝的字段：`max_tokens`
///（`/compact` 上的首要嫌疑）、`stream`、`messages` 数、一个粗略 prompt-token
/// 估算（Σ content chars / 4——粗，但足以发现 context-window 溢出）、
/// `temperature` / `top_p` / `reasoning_effort`，以及任何 `tools` / `tool_choice`。
/// 不可解析时原样返回请求。永不记录全文 message——此处只有标量 / 预览维度。
fn request_body_fingerprint(body: &bytes::Bytes) -> String {
    let Ok(s) = std::str::from_utf8(body) else {
        return "<unparseable request body>".to_string();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
        return "<request body not JSON>".to_string();
    };

    let scalar = |field: &str| match v.get(field) {
        None => "<absent>".to_string(),
        Some(other) => other.to_string(),
    };

    // messages 数量 + 粗略 prompt-token 估算 + 首/末 role 预览。
    let messages = v.get("messages").and_then(|m| m.as_array());
    let n = messages.map(|a| a.len()).unwrap_or(0);
    // system/tool 文本也计入 prompt；无论何种 role 都汇总每条 message 的 content
    // 长度。使用共享的 `message_content_tokens` 辅助，使指纹的 `est_prompt_tokens`
    // 与 `estimate_prompt_tokens`（以及使用它的 413 / 不重试 guard）保持同步——并
    // 使多模态 `content` 数组不会把其 base64 图像/音频按原始字符计，那样会相对
    // guard 的判定吹爆（内联 base64）或少算（仅 URL）。默认成本与静态辅助一致。
    let costs = EstPromptCosts::default_costs();
    let est_tokens: u64 = messages
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m.get("content"))
                .map(|c| message_content_tokens(c, &costs))
                .sum()
        })
        .unwrap_or(0);

    // 首/末 message content 的预览。纯字符串则如前显示前 120 字符。对多模态
    // `content` *数组*，我们**不** dump 序列化 JSON（单张内联 base64 图像就是
    // 上万字符的 base64 噪声），而是打印一个紧凑的 part census：按块类型计数 +
    // 前导文本片段，有封顶。
    let prev = |idx: usize| -> Option<String> {
        let arr = messages?;
        let m = arr.get(idx)?;
        let role = m
            .get("role")
            .and_then(|r| r.as_str())
            .unwrap_or("?")
            .to_string();
        let preview = match m.get("content") {
            Some(serde_json::Value::String(s)) => {
                let c = s.chars().take(120).collect::<String>();
                format!("content[..120]={c:?}")
            }
            Some(serde_json::Value::Array(parts)) => {
                let mut text = 0u64;
                let mut image = 0u64;
                let mut audio = 0u64;
                let mut other = 0u64;
                let mut first_text: Option<String> = None;
                for p in parts {
                    let ty = p.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    match ty {
                        "text" => {
                            text += 1;
                            if first_text.is_none() {
                                first_text = p
                                    .get("text")
                                    .and_then(|t| t.as_str())
                                    .map(|s| s.chars().take(60).collect());
                            }
                        }
                        "image_url" => image += 1,
                        "input_audio" => audio += 1,
                        _ => other += 1,
                    }
                }
                let snippet = first_text
                    .map(|t| format!(" text[..60]={t:?}"))
                    .unwrap_or_default();
                format!(
                    "content[parts={} (text={text}+image={image}+audio={audio}+other={other}){snippet}]",
                    parts.len()
                )
            }
            Some(other) => {
                // 非字符串、非数组的标量——短，预览安全。
                let c = other.to_string().chars().take(120).collect::<String>();
                format!("content[..120]={c:?}")
            }
            None => "content=<absent>".to_string(),
        };
        Some(format!("role={role} {preview}"))
    };
    let first = prev(0).unwrap_or_else(|| "<none>".to_string());
    let last = prev(n.saturating_sub(1)).unwrap_or_else(|| "<none>".to_string());

    let tools_n = v
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|a| a.len())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "<absent>".to_string());

    format!(
        "max_tokens={} stream={} messages={} est_prompt_tokens=~{} temperature={} top_p={} \
         reasoning_effort={} tools={} tool_choice={} first=[{first}] last=[{last}]",
        scalar("max_tokens"),
        scalar("stream"),
        n,
        est_tokens,
        scalar("temperature"),
        scalar("top_p"),
        scalar("reasoning_effort"),
        tools_n,
        scalar("tool_choice"),
    )
}

/// 估算 **多模态** chat-completion 请求 prompt 大小时用到的、按类型的粗粒度
/// token 成本。NVIDIA NIM 的公开 API 只文档化了纯文本 `content`；视觉 / 音频
/// 模型是 OpenAI 兼容的，接受 OpenAI 风格的内容块数组
///（`{type:"image_url",…}`、`{type:"input_audio",…}`）。代理原样透传这些，但其
/// prompt 大小 guard（413 硬上限、空壳不重试软限制、failure-log 指纹）绝不能
/// 被它们欺骗：
///
/// - 一张内联 base64 图像有数万字符。把它按 `chars/4`（文本启发式）计数会把估算
///   吹爆数个数量级，并以 413 拒掉一个完全合法的多模态请求。
/// - 一张仅 URL 引用的图像只有几十字符，会*少算*，让过长 prompt 漏过上限。
///
/// 因此每个非文本 part 被替换为一个粗略的固定 / 按量成本，而非其原始字节长度。
/// 这些是启发式旋钮，可配置，使运营者能针对特定模型的真实 token 计费进行调节。
/// 全置 0 会禁用专门处理并回退为按 part 序列化长度计数（有上限）——仅作为逃生
/// 舱有用。
#[derive(Clone, Copy, Debug)]
pub struct EstPromptCosts {
    /// 赋给每个 `image_url` 块的固定 token 成本，无论它是远端 `https://` URL 还是
    /// 内联 `data:…;base64,` payload。规避 base64 吹爆与 URL 少算两端问题。
    /// 0 → 走未知 part 回退。
    pub image_cost: u64,
    /// `input_audio` 块中每 *KiB* base64 音频 payload 的 token 成本，以
    /// `audio_token_cap` 为上限。0 → 走未知 part 回退。
    pub audio_token_per_kb: u64,
    /// 每块音频估算的硬上限，使单个超大片段不会把估算吹爆。
    pub audio_token_cap: u64,
    /// 对我们不识别的内容 part（未知 `type`、形状未建模的对象/数组）施加的上限。
    /// 防止意外字段里乱窜的 base64 blob 把估算吹爆，但仍计入*某些*量。置 0 则
    /// 完全忽略未知 part。
    pub unknown_part_cap: u64,
}

impl EstPromptCosts {
    /// 保守默认：图像 ≈ 1500 token（覆盖低细节 OpenAI tile + 安全余量），音频
    /// ≈ 16 token/KiB base64、上限约 10k，未知 part 上限 2000。可通过 config/env 调节。
    pub fn default_costs() -> Self {
        Self {
            image_cost: 1500,
            audio_token_per_kb: 16,
            audio_token_cap: 10_000,
            unknown_part_cap: 2000,
        }
    }

    /// 单个内容块 `part`（`content` 数组的一个元素）的粗粒度 token 成本。对
    /// 无法计量贡献的 part（例如无可识别字段的对象）返回 0。
    fn part_cost(&self, part: &serde_json::Value) -> u64 {
        let ty = part.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match ty {
            // `{type:"text", text:"…"}` —— 唯一可精确计量的 part。
            "text" => part
                .get("text")
                .and_then(|t| t.as_str())
                .map(|s| s.chars().count() as u64 / 4)
                .unwrap_or(0),
            // `{type:"image_url", image_url:{url:"…"}}` —— URL 与内联 base64 同
            // 固定成本。仅尊重显式 `detail` 至 NVIDIA 忽略它为止；此处不建模
            // 逐 tile 缩放。
            "image_url" if self.image_cost > 0 => self.image_cost,
            // `{type:"input_audio", input_audio:{data:"<base64>", format:"…"}}` ——
            // base64 长度是片段大小的粗代理；设上限使单个超大片段不会独大。3 字节
            // base64 ≈ 3 字符，因此字符数 / 1024 给出约 KiB 计数。
            "input_audio" if self.audio_token_per_kb > 0 => {
                let chars = part
                    .get("input_audio")
                    .and_then(|a| a.get("data"))
                    .and_then(|d| d.as_str())
                    .map(|s| s.chars().count() as u64)
                    .unwrap_or(0);
                // chars/1024 ≈ KiB base64；× 每 KiB 成本；clamp 到上限。
                let est = (chars / 1024).saturating_mul(self.audio_token_per_kb);
                est.min(self.audio_token_cap)
            }
            // 其它任何情况（未知 type，或已知 type 但其专用成本被禁用）：计入序列化
            // 长度但有上限，使意外形状里乱窜的 base64 blob 不把估算吹爆，但仍计入
            // *某些*量。0 → 完全忽略。
            _ => {
                if self.unknown_part_cap == 0 {
                    return 0;
                }
                let raw = part.to_string().chars().count() as u64 / 4;
                raw.min(self.unknown_part_cap)
            }
        }
    }
}

/// 单条 message `content` 的粗略 token 估算，content 可能是纯字符串（仅文本
/// 请求）或类型化 part 数组（OpenAI 风格多模态）。文本按 `chars/4`；每个非文本
/// part 用其 `EstPromptCosts::part_cost` 替代，使内联 base64 图像/音频不吹爆
/// 估算、URL-only 图像不少算。
fn message_content_tokens(content: &serde_json::Value, costs: &EstPromptCosts) -> u64 {
    match content {
        serde_json::Value::String(s) => s.chars().count() as u64 / 4,
        serde_json::Value::Array(parts) => {
            parts.iter().map(|p| costs.part_cost(p)).sum::<u64>()
        }
        // 一个非字符串、非数组的标量（number/bool/null）——按其序列化字符 / 4
        // 计数，有上限。罕见，主要是防御性回退。
        other => {
            if costs.unknown_part_cap == 0 {
                return 0;
            }
            (other.to_string().chars().count() as u64 / 4).min(costs.unknown_part_cap)
        }
    }
}

/// 从序列化的 chat-completions 请求 body 解析出的粗略 prompt-token 估算
///（文本按 Σ message content chars / 4，图像/音频块按粗略 per-part 成本）。
/// 以纯数字暴露，使 413 硬上限和空壳不重试软限制能与之比较。当 body 不是合法
/// JSON 或无 `messages` 数组时返回 None。设计上刻意粗略——它只需可靠地把
/// failure.log 中见到的 107k+ 空壳与正常请求区分开；阈值（90k 软 / 120k 硬）承
/// 担安全余量。
///
/// 多模态 `content` 数组用 `costs` 计量，使内联 base64 图像（数万字符）不会被
/// 除以 4 而误拒合法请求，远端 URL 图像（几十字符）不会少算漏过上限。纯文本
/// `content` 沿用原 `chars/4` 启发式不变。持有 config 的调用方（`Proxy` 上的两
/// 个 prompt 大小 guard）传 `self.est_costs`；failure-log 指纹传
/// `EstPromptCosts::default_costs()`，因为它是一个无法访问 config 的自由函数。
fn estimate_prompt_tokens(body: &bytes::Bytes, costs: &EstPromptCosts) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let arr = v.get("messages").and_then(|m| m.as_array())?;
    let est: u64 = arr
        .iter()
        .filter_map(|m| m.get("content"))
        .map(|c| message_content_tokens(c, costs))
        .sum();
    Some(est)
}

/// NVIDIA 空壳响应 body 的紧凑指纹——确认它是空壳（相对于合法零 token 完成）
/// 的字段：`id`、`model`、`created`、`usage`、以及 `choices` 数组长度（应为 0）。
/// 此处 `usage:null` 正是没有推理发生的铁证。
fn shell_body_fingerprint(body: &serde_json::Value) -> String {
    let id = body
        .get("id")
        .map(|v| if v.is_null() { "null".to_string() } else { v.to_string() })
        .unwrap_or_else(|| "<absent>".to_string());
    let model = body
        .get("model")
        .map(|v| if v.is_null() { "null".to_string() } else { v.to_string() })
        .unwrap_or_else(|| "<absent>".to_string());
    let created = body.get("created").map(|v| v.to_string()).unwrap_or_else(|| "<absent>".to_string());
    let usage = body
        .get("usage")
        .map(|v| if v.is_null() { "null".to_string() } else { v.to_string() })
        .unwrap_or_else(|| "<absent>".to_string());
    let choiceslen = body
        .get("choices")
        .and_then(|c| c.as_array())
        .map(|a| a.len())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "<absent>".to_string());
    let object = body
        .get("object")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "<absent>".to_string());
    format!(
        "object={object} id={id} model={model} created={created} choiceslen={choiceslen} usage={usage}",
    )
}

/// 组合一条空壳失败记录的 `detail` 字符串：请求指纹 + 空壳指纹 + 重试处置 +
/// 计时。硬换行（`\\n`）让 `jq -r .detail` 输出可读，而不是一长串逗号行；无论
/// 如何 failure.log 都把它作为单条 JSON 字符串存储。
fn build_empty_shell_detail(
    req_fp: &str,
    shell_fp: &str,
    retry_attempt: u32,
    retry_reason: Option<&str>,
    elapsed_ms: Option<u128>,
) -> String {
    let timing = match elapsed_ms {
        Some(ms) => format!("upstream_elapsed_ms={ms}"),
        None => "upstream_elapsed_ms=<unknown>".to_string(),
    };
    let attempt_line = match retry_reason {
        Some(r) => format!("attempts_up_to_this_shell={retry_attempt} retry_reason={r}"),
        None => format!("attempts_up_to_this_shell={retry_attempt} retry_reason=<none>"),
    };
    format!(
        "NVIDIA empty-shell soft failure (200, choices:[], usage:null — no inference ran).\
         \n  {timing}\
         \n  {attempt_line}\
         \n  request[{req_fp}]\
         \n  upstream_shell[{shell_fp}]",
    )
}

/// 构造一个 502 Bad Gateway 响应 body，把 NVIDIA 空壳软失败外抛给客户端——
/// 而非那个让 Claude Code 的 `/compact` 打印 "summarization produced empty
/// response" 的合成空内容 choice。内含一个结构化 OpenAI 形态的错误，使下游
/// 消费者（cc-switch → Anthropic）把它归类为硬失败、而非零 token 完成。
///
/// `primary` 是驱动该 502 的 body（重试的空壳，或未重试时的原始空壳）。
/// `original` 是首次尝试的空壳，即便 `primary` 是重试结果也保留下来用于日志
/// detail。`retry_max` 是重试时使用的 clamp 后的 `max_tokens`（None 表示重试
/// 被禁用 / 预算耗尽）。
/// 构造一个 502 Bad Gateway 响应 body，把 NVIDIA 空壳软失败外抛给客户端——而非
/// 那个让 Claude Code 的 `/compact` 打印 "summarization produced empty response"
/// 的合成空内容 choice。内含一个结构化 OpenAI 形态的错误，使下游消费者（cc-switch
/// → Anthropic）把它归类为硬失败、而非零 token 完成。
///
/// `request_body` 是最后一次发给上游的 body（未重试时为原始，重试时为 clamp 后
/// 的重试 body）。`shell_body` 是驱动本 502 的上游空壳 body（重试的空壳，或原始
/// 空壳）。`retry` 为 `Some((clamped_max_tokens, attempt_index, outcome))` 表示
/// 已尝试过一次 clamp 重试；`outcome` 字符串命名其结局。
fn empty_shell_bad_gateway(
    request_body: &bytes::Bytes,
    shell_body: &serde_json::Value,
    client_req_id: &str,
    retry: Option<(u64, u32, &'static str)>,
) -> warp::reply::Response {
    let request_fp = request_body_fingerprint(request_body);
    let shell_fp = shell_body_fingerprint(shell_body);
    let (retry_json, detail) = match retry {
        Some((m, attempt, outcome)) => {
            let j = serde_json::json!({
                "attempted": true,
                "clamped_max_tokens": m,
                "attempt_index": attempt,
                "outcome": outcome,
            });
            let d = format!(
                "upstream empty-shell 2xx (choices:[], usage:null — no inference ran); \
                 retried once with max_tokens={m} (attempt={attempt}) and failed ({outcome}); \
                 client_req_id={client_req_id}"
            );
            (j, d)
        }
        None => {
            let j = serde_json::json!({
                "attempted": false,
                "clamped_max_tokens": null,
                "attempt_index": null,
                "outcome": "not_attempted",
            });
            let d = format!(
                "upstream empty-shell 2xx (choices:[], usage:null — no inference ran); \
                 not retried (retry disabled or budget exhausted); client_req_id={client_req_id}"
            );
            (j, d)
        }
    };
    let body = serde_json::json!({
        "error": {
            "message": "upstream soft-failure: empty chat completion (no inference produced). \
                        This typically happens on long-context /compact with max_tokens larger \
                        than the model's remaining context budget.",
            "type": "upstream_empty_shell",
            "code": "empty_completion",
            "client_req_id": client_req_id,
            "retry": retry_json,
            "request_fingerprint": request_fp,
            "upstream_shell": {
                "status": 200,
                "fingerprint": shell_fp,
                "body": shell_body,
            },
            "detail": detail,
        }
    });
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    warp::http::Response::builder()
        .status(warp::http::StatusCode::BAD_GATEWAY)
        .header("content-type", "application/json")
        .body(warp::hyper::Body::from(bytes))
        .unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(warp::http::StatusCode::BAD_GATEWAY)
                .body(warp::hyper::Body::empty())
                .unwrap()
        })
}

/// 解析一个 chat-completions 请求 body，把 `max_tokens` clamp 到 `target`，并重
/// 新序列化。空壳重试用它给 NVIDIA 一个更小的输出预算，使推理真正发生。若 body
/// 不是合法 JSON 对象、或没有数值 `max_tokens`，原样返回原始字节。
fn clamp_max_tokens(body: &bytes::Bytes, target: u64) -> Vec<u8> {
    let Ok(s) = std::str::from_utf8(body) else {
        return body.to_vec();
    };
    let Ok(mut v) = serde_json::from_str::<serde_json::Value>(s) else {
        return body.to_vec();
    };
    let Some(obj) = v.as_object_mut() else {
        return body.to_vec();
    };
    // 仅当原始 max_tokens 超过 target 时才 clamp——否则原样不动（且根本无
    // max_tokens 时什么也不设，免得新增字段惊到客户端）。
    let clamp_needed = match obj.get("max_tokens") {
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .map(|x| x > target)
            .unwrap_or(false),
        Some(serde_json::Value::String(s)) => s.parse::<u64>().ok().map(|x| x > target).unwrap_or(false),
        _ => false,
    };
    if !clamp_needed {
        return body.to_vec();
    }
    obj.insert(
        "max_tokens".to_string(),
        serde_json::Value::Number(serde_json::Number::from(target)),
    );
    serde_json::to_vec(&v).unwrap_or_else(|_| body.to_vec())
}


/// 从上游 `usage` 对象提取的 token 用量。任一分量在省略时（如最终 chunk 之前的
/// 流式 chunk，或无 usage 响应）都可能为 `None`。
#[derive(Debug, Default, Clone, Copy)]
struct Usage {
    prompt: Option<u64>,
    completion: Option<u64>,
    total: Option<u64>,
}

/// 从解析后的 JSON 值中提取 `Usage`。当无 `usage` 对象时返回 `None`，使调用
/// 方得以保留其看到的最近一个。
fn parse_usage(v: &serde_json::Value) -> Option<Usage> {
    let u = v.get("usage")?;
    Some(Usage {
        prompt: u
            .get("prompt_tokens")
            .and_then(|x| x.as_i64())
            .map(|x| x.max(0) as u64),
        completion: u
            .get("completion_tokens")
            .and_then(|x| x.as_i64())
            .map(|x| x.max(0) as u64),
        total: u
            .get("total_tokens")
            .and_then(|x| x.as_i64())
            .map(|x| x.max(0) as u64),
    })
}

/// `coerce_sse_chunk` 的有状态流包装：把跨 chunk 边界拆分的 `data:` 行重拼
/// 后再强转其数值型 `error.code`，治愈原逐-chunk 实现的盲区。
///
/// 旧的 `coerce_sse_chunk(chunk)` 对单个 chunk 操作、不带跨 chunk 状态：当一个
/// `data: {"error":{"code":400}}\n\n` 事件被上游的 `bytes_stream()` 拆到两个
/// chunk 边界上时，每一半的 JSON 解析都失败、原样转发 → 数值 `code` 漏过做强转，
/// 客户端仍可能拿到数值 `code` 触发 cc-switch 422。NVIDIA 的错误事件虽短、大多
/// 单 chunk 到达，但流式 body 在上面这种小事件之外也可能被任意切分。
///
/// 本 tap 维护一个原始字节累加器 `feed_buf`（与 `UsageTap` 同样的 `valid_up_to`
/// 策略处理跨 chunk 的多字节 UTF-8 码点）和一个待处理行 carry。每次 feed 一 chunk：
/// 累加字节 → 剥离最长有效 UTF-8 前缀 → 处理所有以 `\n` 结尾的完整行（每行跑一遍
/// `coerce_sse_chunk` 的逐行强转逻辑）→ 把不完整的尾行留在 carry 里等下一 chunk。
/// 没有发生任何强转时，poll_next 直接转发原始 chunk 字节；发生强转时转发改写后的
/// 字节。hold-back 的代价仅是一个不完整尾行的延迟（下一 chunk 到来即放行）。
///
/// 注意：本结构体只持有重组 + 强转所需状态；不读 usage、不发 metric——那些由下游
/// 的 `UsageTap` 负责。两层 tap 因此正交、可独立演进。
struct CoerceSseTap<S> {
    inner: S,
    /// 原始字节累加器，处理跨 chunk 的多字节 UTF-8 与跨 chunk 的 `\n` 行边界。
    feed_buf: Vec<u8>,
    /// 已确认 UTF-8 完整、等待按 `\n` 切行的待处理文本。
    carry: String,
}

impl<S> CoerceSseTap<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send,
{
    /// feed 一个 chunk：累加字节、剥离 UTF-8、按完整 `\n` 行强转、保留尾残行。
    /// 返回本 chunk 处理后应当向下游 emit 的字节（可能等于原 chunk 前 `<=len`
    /// 部分，也可能是改写后的、可能更短/更长）。未到 `\n` 的尾行不在此 emit、
    /// 留 carry 等下一 chunk；故 emit 的字节数严格等于「本 chunk 中截至最后一个
    /// `\n`（含）的字节数」，除非 carry 里前一段不完整尾行也在本 chunk 被补全——
    /// 那时 emit 里会带上那前段的强转结果，但本 chunk 的对应字节只 emit 一次。
    fn feed(&mut self, chunk: &Bytes) -> Bytes {
        self.feed_buf.extend_from_slice(chunk);
        // 剥离最长有效 UTF-8 前缀；非法尾字节（部分多字节码点）留在 feed_buf。
        let (text, consumed) = match std::str::from_utf8(&self.feed_buf) {
            Ok(s) => (s, self.feed_buf.len()),
            Err(e) => {
                let up = e.valid_up_to();
                if up == 0 {
                    // 整段至今非 UTF-8（例如上游 gzipped body）：不常驻全 body，
                    // 清空缓冲、原样转发本 chunk（强转不可能在这些字节上成立）。
                    self.feed_buf.clear();
                    return chunk.clone();
                }
                let t = std::str::from_utf8(&self.feed_buf[..up])
                    .expect("valid_up_to bytes are valid UTF-8");
                (t, up)
            }
        };
        let appended = text.to_string();
        self.feed_buf.drain(..consumed);
        // carry = 上一 chunk 留下的不完整尾行（无 `\n`）；先接上本次的新文本，
        // 形成一个连续文本块，再按 `\n` 切完整行强转、把尾残行留回 carry。
        let mut block = std::mem::take(&mut self.carry);
        block.push_str(&appended);
        // 找最后一个 `\n`：它之前（含）是完整行（含 carry 前缀被本次补全的），
        // 之后是无 `\n` 的尾残行，留回 carry 等下一 chunk。若根本没有 `\n`，
        // 整段块都是尾残行 → 留 carry、emit 空（不向下游 emit 本 chunk 任何字节；
        // 它会在下一 chunk 把这行补全后一起放行）。
        let split = match block.rfind('\n') {
            Some(i) => i + 1, // 完整行截止于 `\n` 之后一字节
            None => {
                // 整段都是不完整尾行：留 carry，本 chunk 不 emit 任何字节。
                self.carry = block;
                return Bytes::new();
            }
        };
        let complete: String = block.drain(..split).collect();
        // block 里剩的（若有）即新的尾残行。
        if !block.is_empty() {
            self.carry = block;
        }
        // 对完整行段跑逐行强转。逐行跑 coerce_sse_chunk（其内部对单行做
        // `data:` JSON 解析 + code 强转 + 重序列化）。
        let mut changed = false;
        let mut out = String::with_capacity(complete.len() + 64);
        for line in complete.split_inclusive('\n') {
            let coerced_line = coerce_sse_chunk(&Bytes::from(line.to_string()));
            if coerced_line.len() != line.len() || coerced_line.as_ref() != line.as_bytes() {
                changed = true;
                out.push_str(std::str::from_utf8(&coerced_line).unwrap_or(line));
            } else {
                out.push_str(line);
            }
        }
        if !changed {
            // 无强转：emit 完整行段的原样字节。注意 split 切的是 carry+appended
            // 的连接结果，而 emit 应当保持字节身份稳定——这里直接返回 complete
            // 的 String（与原始字节语义等价，因 carry 已在本 chunk 把上次的尾残
            // 行补全且未做任何改动）。客户端最终看到的字节流与上游逐字节一致。
            return Bytes::from(complete);
        }
        Bytes::from(out)
    }

    /// 流结束：把 carry 里的尾残行（无最终 `\n`）也强转一次再放行。若无残行
    /// 或强转未改动则返回 None。
    fn flush_trailing(&mut self) -> Option<Bytes> {
        // 先把 feed_buf 里残留的任何字节（最终部分码点）尽力转成 lossy 字符串。
        if !self.feed_buf.is_empty() {
            let rest = String::from_utf8_lossy(&self.feed_buf).into_owned();
            self.carry.push_str(&rest);
            self.feed_buf.clear();
        }
        if self.carry.is_empty() {
            return None;
        }
        let line = std::mem::take(&mut self.carry);
        let coerced = coerce_sse_chunk(&Bytes::from(line.clone()));
        if coerced.len() == line.len() && coerced.as_ref() == line.as_bytes() {
            // 未改动：仍需把原尾行放行（否则丢字节）。
            Some(Bytes::from(line))
        } else {
            Some(coerced)
        }
    }
}

impl<S> Stream for CoerceSseTap<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        // 单次 poll：每个分支都直接 return，因此不需要外层 loop——上游流每个
        // wake 对应一次 poll_next，我们的状态推进只在「拿到一个 chunk / 流结束」
        // 这两个事件上发生，无需自旋。
        match Pin::new(&mut self.inner).poll_next(cx) {
            std::task::Poll::Ready(None) => {
                // 上游流结束：把 carry 里尾残行强转放行（若有）。
                if let Some(b) = self.flush_trailing() {
                    return std::task::Poll::Ready(Some(Ok(b)));
                }
                // 到 None 时 carry/feed_buf 已被 flush_trailing 清空（含 lossy 收尾）。
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                std::task::Poll::Ready(Some(Err(e)))
            }
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                let out = self.feed(&chunk);
                std::task::Poll::Ready(Some(Ok(out)))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// 一个流包装器，扫描每个转发的 chunk 中的 `usage`（处理 SSE `data:` 行
/// 和裸 JSON，带一个 carry 缓冲以应对跨 chunk 拆分的行），并在上游流结束后把
/// 最近一次 usage 记到 Prometheus 计数器。正是它使
/// `proxy_tokens_input/output/total` 真正反映真实流量——没有它，成功响应作为流
/// 转发、从未被检视。
///
/// 它还嗅探流上的*空* `choices` 数组。回顾：cc-switch 把上游 ChatCompletions
/// body 翻译回 Anthropic 格式，其非流式转换器 `openai_to_anthropic` 在 body 解析
/// 为 `{"choices": [], ...}` 时硬报错 "格式转换错误: Empty choices array"
///（对 Claude Code 客户端体现为 422）——这发生在 cc-switch 聚合到的、一个走
/// genuine OpenAI/NVIDIA usage-only 最终 SSE chunk
/// `data: {"choices":[],"usage":{...}}` 上，或一个 body 确实带 `choices: []` 的
/// 非流式 200 上。我们在此看不见 cc-switch，但当上游流上确实带一个空 `choices`
/// 数组的 JSON 对象时，把它（连同状态）记下，可在生产中精准定位触发源，而非猜测。
///
/// 该诊断只读（绝不改动流），并有 guard 守卫：仅当确解析出带 `choices: []` 的
/// JSON 对象、且之后从未见过非空 `choices` 时才触发；text/plain body、SSE
/// `[DONE]`、以及我们无法解析的 chunk 都保持安静，于是该警告不会在每个非流式
/// 或非 JSON 响应上误报。
struct UsageTap<S> {
    inner: S,
    metrics: Arc<Metrics>,
    carry: String,
    /// 跨 chunk UTF-8 重组用的原始字节累加器。上游 `bytes_stream()` 可把一个多
    /// 字节 UTF-8 码点拆到两个 chunk 里（罕见——NVIDIA SSE 多为 ASCII，但
    /// `data:` payload 内的用户可见内容 / 工具名可能带多字节字符）。`from_utf8(chunk)`
    /// 会在部分 chunk 上失败，`feed` 会 `return`，把整个 chunk 从 `carry` 丢
    /// 掉——于是跨边界的行永远到不了 `classify_json_line`，本流的 usage /
    /// `saw_choices` 标志会被少算。我们改为在这里累加原始字节，并剥离最长*有效*
    /// UTF-8 前缀（`Utf8Error::valid_up_to`），把任何部分尾码点留在缓冲里等下
    /// 一次 feed。这仅用于 metric / 诊断恢复——`poll_next` 仍把原始 chunk 逐字
    /// 转发给客户端，故客户端字节从不被影响。
    feed_buf: Vec<u8>,
    usage: Option<Usage>,
    /// 本流上是否出现过带非空 `choices` 数组的已解析 JSON 对象？是则在流结束
    /// 时响应有真实内容，不需警告。
    saw_choices: bool,
    /// 是否出现过带*空* `choices: []` 数组的已解析 JSON 对象？
    saw_empty_choices: bool,
    /// 是否成功解析过任意 JSON 对象？守护流结束诊断：一个 text/plain body 或无
    /// 法解析的 chunk 不应被误判为带空 choices 的 JSON 响应。
    saw_any_json: bool,
    /// 本 tap 包装的上游响应状态，用于日志行。
    status: Option<u16>,
    client_req_id: String,
}

impl<S> UsageTap<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send,
{
    fn feed(&mut self, chunk: &Bytes) {
        self.feed_buf.extend_from_slice(chunk);
        // 从原始累加器剥离最长有效 UTF-8 前缀；任何尾部残码点留在
        // `feed_buf` 里等下一 chunk 补全。（`from_utf8` 的 err 带 `valid_up_to`，
        // 我们据此免费拿到切分点；字节切片情形仅在非法序列触发，而非 EOF 处的
        // 部分尾——后者由 `flush_trailing` 处理。）
        let (text, consumed) = match std::str::from_utf8(&self.feed_buf) {
            Ok(s) => (s, self.feed_buf.len()),
            Err(e) => {
                let up = e.valid_up_to();
                if up == 0 {
                    // 整个缓冲至今都是非 UTF-8 序列（例如上游 gzipped body）。
                    // 不再持续累加；否则若整流都看不见合法 UTF-8，会把整个 body
                    // 都常驻内存。清空。
                    self.feed_buf.clear();
                    return;
                }
                let text = std::str::from_utf8(&self.feed_buf[..up])
                    .expect("valid_up_to bytes are valid UTF-8");
                (text, up)
            }
        };
        self.carry.push_str(text);
        // 把剩余的部分尾留在字节缓冲里；本函数余下部分只 inspect `carry` 的完整
        // 行，故尾部残字节天然在此等待。
        self.feed_buf.drain(..consumed);
        // 处理每个完整行（以 '\n' 结尾）；把尾部的不完整行（若有）留在 `carry`
        // 里等下一 chunk。
        while let Some(nl) = self.carry.find('\n') {
            let line: String = self.carry.drain(..=nl).collect();
            self.classify_json_line(&line);
        }
    }

    /// 把单行（带或不带尾换行）作为 JSON payload 解析，剥离 SSE `data:` 前缀。
    /// `feed`（处理完整行）与 `flush_trailing`（处理从未收到换行的最终行，如
    /// 一个单行非流式 ChatCompletions body）都用它。一个单独的裸 JSON 对象——
    /// 无 `data:` 前缀、无 SSE 框架——正是非流式 OpenAI 响应的样子，因此此处不
    /// 要求 SSE 框架。
    fn classify_json_line(&mut self, line: &str) {
        let line = line.trim_end_matches(['\r', '\n']);
        let json_str = match line.strip_prefix("data:") {
            Some(rest) => rest.trim_start(),
            None => line.trim(),
        };
        if json_str.is_empty() {
            return;
        }
        // `[DONE]` 这类 SSE 终止符不是 JSON；安静跳过。
        if json_str == "[DONE]" {
            return;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return;
        };
        if let Some(u) = parse_usage(&v) {
            self.usage = Some(u);
        }
        // 记录我们至少解析出过任意 JSON 对象，使流结束诊断能区分"我们从未解析
        // 出 JSON"（text/plain 或解析失败——非我们关注）与"我们解析出 JSON 但它
        // 从未带非空 choices 数组"（cc-switch 422 触发源）。
        self.saw_any_json = true;
        if let Some(choices) = v.get("choices") {
            match choices.as_array() {
                Some(arr) if !arr.is_empty() => self.saw_choices = true,
                Some(_) => self.saw_empty_choices = true,
                None => {}
            }
        }
    }

    /// 流结束时把 `carry` 中剩余的排干。非流式 OpenAI 响应是单个无尾换行的
    /// JSON 对象；若从不 flush 它，`saw_choices` 会一直 false，诊断会每个非流
    /// 式请求都误报。
    fn flush_trailing(&mut self) {
        // UTF-8 重组缓冲里残留的任何字节（一个最终的部分码点，或一个 EOF 而无
        // 最终换行）也需要成为候选行——尽力而为用 U+FFFD 替换非法字节而非丢弃。
        // true EOF 处的部分码点对 `classify_json_line` 无害（JSON 反正也解析
        // 失败），但一个缓冲中的非流式 body 其最终 chunk 结束于码点中间，若不
        // 处理就会丢失。
        if !self.feed_buf.is_empty() {
            let rest = String::from_utf8_lossy(&self.feed_buf).into_owned();
            self.carry.push_str(&rest);
            self.feed_buf.clear();
        }
        if !self.carry.is_empty() {
            let rest: String = std::mem::take(&mut self.carry);
            self.classify_json_line(&rest);
        }
    }
}

impl<S> Stream for UsageTap<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        match Pin::new(&mut me.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                me.feed(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                // 排干从未获得 '\n' 的任何尾部行。非流式 OpenAI 响应是单个无
                // 尾换行的 JSON 对象；不 flush 它会一直留在 `carry` 未读，使
                // 空 choices 诊断误报。
                me.flush_trailing();
                if let Some(u) = me.usage.take() {
                    me.metrics.record_usage(u.prompt, u.completion, u.total);
                }
                // 空 choices 诊断：仅在我们解析出带空 `choices: []` 数组、且此
                // 后从未见过非空数组的 JSON 对象时才 warn。要求 `saw_empty_choices`
                //（而非仅 `!saw_choices`）使 text/plain body、单行非流式响应、
                // 以及我们解析失败的 chunk 不误报——它们本就没有空 choices 的
                // JSON 可言。
                if let Some(status) = me.status {
                    if me.saw_empty_choices && !me.saw_choices {
                        warn!(
                            client_req_id = %me.client_req_id,
                            status = status,
                            "上游 2xx 返回空 choices 数组 —— cc-switch 可能在把它翻译成 Anthropic 时硬报错 (格式转换错误: Empty \
                             choices array → 422)"
                        );
                    }
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 在一个非流式成功 body 中，我们会缓冲多少字节以寻找空 `choices` 数组来做
/// 合成。真正带空 `choices`（无生成 token）的 body 都很小——远低于此上限——因
/// 此我们从不把一个大完成体留在内存里，任何真正大的非流式 body 在这多字节之后
/// 便直接落入流式透传。
const EMPTY_CHOICES_PEEK_BYTES: usize = 64 * 1024;

/// 剥离我们要重新计算的逐跳与长度相关 header，然后把 `resp_headers` 的其余
/// 部分拷到 `builder` 上。流式透传与空 choices 合成路径共用，使客户端看到的转发
/// header 集合与响应走哪条路径无关、保持一致。
fn apply_forwarded_response_headers(
    builder: &mut warp::http::response::Builder,
    resp_headers: Vec<(String, Vec<u8>)>,
) {
    for (key, value) in resp_headers {
        match key.as_str() {
            "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length" => continue,
            _ => {}
        }
        if let Ok(hv) = warp::http::HeaderValue::from_bytes(&value) {
            if let Some(headers) = builder.headers_mut() {
                let key_bytes = key.as_bytes().to_vec();
                let hname = warp::http::HeaderName::from_bytes(&key_bytes)
                    .unwrap_or_else(|_| warp::http::HeaderName::from_bytes(b"x-unknown").unwrap());
                headers.append(hname, hv);
            }
        }
    }
}

impl Proxy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: String,
        key_pool: Arc<KeyPool>,
        metrics: Arc<Metrics>,
        request_timeout_secs: u64,
        request_timeout_streaming_secs: u64,
        retry_config: crate::config::RetryConfig,
        token_wait_ms: u64,
        model_aliases: std::collections::HashMap<String, String>,
        inject_stream_usage: bool,
        synthesize_empty_choices: bool,
        retry_on_empty_shell: bool,
        empty_shell_retry_max_tokens: u64,
        empty_shell_retry_attempts: u32,
        empty_shell_probe_before_retry: bool,
        empty_shell_retry_backoff_base_ms: u64,
        empty_shell_retry_backoff_max_ms: u64,
        empty_shell_pre_clamp_max_tokens: std::collections::HashMap<String, u64>,
        empty_shell_no_retry_prompt_tokens: u64,
        request_max_prompt_tokens: u64,
        est_costs: EstPromptCosts,
        circuit_breaker_threshold: u32,
        circuit_breaker_per_model: bool,
        retry_on_503: bool,
        probe_before_503_retry: bool,
        retry_on_500: bool,
        max_tokens_max: u64,
        concurrency_limit_per_model: std::collections::HashMap<String, u32>,
        concurrency_queue_timeout_ms: u64,
        upstream_connect_timeout_secs: u64,
        upstream_pool_idle_timeout_secs: u64,
        retry_jitter_ms: u64,
    ) -> Self {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(upstream_pool_idle_timeout_secs))
            .connect_timeout(Duration::from_secs(upstream_connect_timeout_secs))
            .build()
            .expect("构建 reqwest 客户端失败");
        // 为 metrics 标签基数收容而构建模型白名单：运营者在配置中显式点名的每个
        // 模型。客户端发来、不在此集合里的 `model` 会被上报为 "_unknown"
        //（见 metrics_model）。
        let mut known_models: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for k in model_aliases.keys() {
            known_models.insert(k.clone());
        }
        for v in model_aliases.values() {
            known_models.insert(v.clone());
        }
        for k in empty_shell_pre_clamp_max_tokens.keys() {
            known_models.insert(k.clone());
        }
        for k in concurrency_limit_per_model.keys() {
            known_models.insert(k.clone());
        }
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            key_pool,
            metrics,
            client,
            request_timeout_secs,
            request_timeout_streaming_secs,
            retry_config,
            token_wait_ms,
            model_aliases,
            known_models,
            models_cache: Arc::new(Mutex::new(None)),
            inject_stream_usage,
            synthesize_empty_choices,
            retry_on_empty_shell,
            empty_shell_retry_max_tokens,
            empty_shell_retry_attempts,
            empty_shell_probe_before_retry,
            empty_shell_retry_backoff_base_ms,
            empty_shell_retry_backoff_max_ms,
            empty_shell_pre_clamp_max_tokens,
            empty_shell_no_retry_prompt_tokens,
            request_max_prompt_tokens,
            est_costs,
            circuit_breaker_threshold,
            circuit_breaker_per_model,
            retry_on_503,
            probe_before_503_retry,
            retry_on_500,
            max_tokens_max,
            concurrency_limit_per_model,
            concurrency_queue_timeout_ms,
            retry_jitter_ms,
        }
    }

    /// 把一个（可能由客户端提供的）模型字符串塌缩为一个作为 Prometheus 维度
    /// 发出安全 label 的值。仅当值非空、不超过 `METRICS_MODEL_MAX_LEN` 字节、且
    /// 存在于运营者 `known_models` 白名单中时才透传；其余一律成为 `"_unknown"`。
    /// 这收容标签基数：非特权客户端无法通过每请求发一个新 `model` 来膨胀注册表。
    fn metrics_model<'a>(&'a self, model: &'a str) -> &'a str {
        if !model.is_empty()
            && model.len() <= METRICS_MODEL_MAX_LEN
            && self.known_models.contains(model)
        {
            model
        } else {
            METRICS_UNKNOWN_MODEL
        }
    }

    /// 仅据已配置的 `model_aliases` 构造一个最小的 `/v1/models` 响应。这是在
    /// 上游 `/v1/models` 目录取不到时的回退；当上游可达时由 `handle_models`
    /// 提供完整真实目录。
    ///
    /// 我们列出别名 key（客户端可见名）而非映射后的 NVIDIA id——调用方按那个
    /// 名字已知它们，并会以之在 `model` 中发送。我们刻意不硬编码任何 NVIDIA
    /// 模型名：真实目录通过 `handle_models` 提供，硬编码列表会与现实漂移、并
    /// 可能把客户端指向不存在的模型。
    fn local_models_response(&self) -> warp::reply::Response {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut entries: Vec<serde_json::Value> = Vec::new();

        for name in self.model_aliases.keys() {
            if seen.insert(name.clone()) {
                entries.push(serde_json::json!({
                    "id": name,
                    "object": "model",
                    "created": 0,
                    "owned_by": "nvidia",
                }));
            }
        }

        let payload = serde_json::json!({
            "object": "list",
            "data": entries,
        });
        let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
        warp::http::Response::builder()
            .status(warp::http::StatusCode::OK)
            .header("content-type", "application/json")
            .body(warp::hyper::Body::from(body))
            .unwrap_or_else(|_| {
                warp::http::Response::builder()
                    .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(warp::hyper::Body::empty())
                    .unwrap()
            })
    }

    /// 用 NVIDIA 的真实目录应答 `GET /v1/models`。该上游端点无需鉴权，返回完整
    /// OpenAI 格式列表，因此我们透传并对结果缓存，避免在每个客户端启动时都重新
    /// 探上游（cc-switch / Cursor 在启动时都会触发）。任何失败时回退到仅
    /// `model_aliases` 列表，使该端点仍返回 200。
    async fn handle_models(&self) -> warp::reply::Response {
        // 若已拉取过上游目录则从缓存提供。
        if let Some(cached) = self.models_cache.lock().await.clone() {
            return json_value_response(warp::http::StatusCode::OK, &cached);
        }

        let result = self
            .client
            .get(format!("{}/v1/models", self.base_url))
            .send()
            .await;

        if let Ok(resp) = result {
            if resp.status().is_success() {
                if let Ok(v) = resp.json::<serde_json::Value>().await {
                    if v.get("data").is_some() {
                        *self.models_cache.lock().await = Some(v.clone());
                        return json_value_response(warp::http::StatusCode::OK, &v);
                    }
                }
            }
        }

        // 上游不可达 / 形态错误：回退到已配置的 `model_aliases` 列表。我们不再
        // 硬编码 NVIDIA 模型名。
        self.local_models_response()
    }

    /// 为给定的客户端 path/method/body 构造一个上游 reqwest 请求，并用 `key_str`
    /// 鉴权。逐跳与客户端提供的 auth header 被剥离。返回的 builder 已准备好
    /// `.send()`。
    ///
    /// `is_streaming` 控制一个显式 `Accept` 覆盖：NVIDIA（与多数 OpenAI 兼容服
    /// 务器）据 `Accept` 头而非仅 body 的 `stream` 字段来选择 SSE vs 缓冲 JSON。
    /// 一些客户端发 `stream:true` 但不带 `Accept: text/event-stream`，导致 NVIDIA
    /// 对一个客户端随后按 SSE 解析的请求返回缓冲 JSON——并使代理缓冲一个长 body，
    /// 触发 worker 背压（503 ResourceExhausted）。把 Accept 头与 `stream` 字段
    /// 同步强制使两端在传输形态上保持一致。
    #[allow(clippy::too_many_arguments)]
    fn build_upstream_request(
        &self,
        url: &str,
        method: &warp::http::Method,
        headers: &warp::http::HeaderMap,
        body: &bytes::Bytes,
        key_str: &str,
        is_streaming: bool,
        body_is_json: bool,
    ) -> reqwest::RequestBuilder {
        let mut req_builder = self.client.request(method.clone(), url);
        req_builder = req_builder
            .header(req_header::AUTHORIZATION, format!("Bearer {}", key_str))
            .header(req_header::USER_AGENT, "nvidia-key-proxy/0.1");
        // 当 body 被改写为规范 JSON 时，强制 Content-Type 为 application/json 以
        // 匹配我们实际上游发送的内容。客户端可能合法地以非 JSON Content-Type
        // 发一个 JSON payload（cc-switch 时好用 text/plain）；否则我们会把改写后
        // 的 JSON 字节标成 form-urlencoded/text 转发，NVIDIA 网关会拒/误解析——即使
        // body 内容合法也是协议错配。当 body 原样转发（解析失败）时，我们保留客户端
        // 的原始 Content-Type，使非 JSON 透传保持字节/标签忠实。
        // 把 Accept 与 stream 同步强制。这是"客户端是否要 SSE"的唯一真值源，
        // 且与 cc-switch /  Nvswitch / OpenAI SDK 直连 NVIDIA 时所做一致。
        //
        // H1（header 白名单）：此前这里是黑名单——逐跳/鉴权 header 被剥，但
        // 其它**任意**客户端 header 原样转发上游。NVIDIA 网关（及许多
        // OpenAI 兼容端）也认 `X-Api-Key` / `api-key` / `X-Goog-Api-Key` /
        // `OpenAI-Organization` / `Cookie` 等：一个不可信客户端能注入这些，用
        // 自己的上游凭证/组织覆盖代理注入的 `Bearer`，把代理变成打别人配额的
        // 开放 relay，或改变上游计费路由。改成**显式白名单**：只转发 W3C trace
        // 传播头与 request-id 系列；其余一概剥。`content-type` 在 body 未重序列
        // 化为 JSON 时按原值保留（非 JSON 透传的忠实性），由下方的强制设置覆盖
        // JSON 情形。`authorization`/`accept`/`user-agent` 由代理自己设，不转发。
        let strip_content_type = body_is_json;
        for (hkey, hvalue) in headers.iter() {
            let key_name = hkey.as_str();
            let allow = matches!(
                key_name,
                "x-request-id" | "request-id" | "traceparent" | "tracestate"
            ) || (key_name == "content-type" && !strip_content_type);
            if !allow {
                continue;
            }
            if let Ok(req_val) = ReqHeaderValue::from_bytes(hvalue.as_bytes()) {
                req_builder = req_builder.header(key_name, req_val);
            }
        }
        // 把 Accept 与 `stream` 同步强制。这是"客户端是否要 SSE"的唯一真值源，
        // 且与 cc-switch / OpenAI SDK 直连 NVIDIA 时所做一致。
        let accept_val = if is_streaming {
            "text/event-stream"
        } else {
            "application/json"
        };
        req_builder = req_builder.header(req_header::ACCEPT, accept_val);
        // body 被重序列化为 JSON 时强制 Content-Type。
        if body_is_json {
            req_builder = req_builder.header("content-type", "application/json");
        }
        if !body.is_empty() {
            req_builder = req_builder.body(body.clone());
        }
        req_builder
    }

    /// warp filter 调用的主入口。
    pub async fn handle_request(
        &self,
        path: String,
        method: warp::http::Method,
        headers: warp::http::HeaderMap,
        body: bytes::Bytes,
        _remote: Option<std::net::SocketAddr>,
    ) -> Result<impl Reply, Rejection> {
        let start = std::time::Instant::now();
        self.metrics.inc_request();

        // ── 显式拒绝不支持的 API 形态 ────────────────────────────────────
        //
        // NVIDIA 仅服务 OpenAI Chat Completions。它不暴露 OpenAI Responses API
        //（`/v1/responses`）也不暴露 Anthropic Messages API（`/v1/messages`）——两者
        // 上游都会 404。与其静默转发这些端点无法解析的 body（只会得到一个困惑的上游
        // 4xx、毫无对客户端的提示），不如在获取 key 之前就明确拒绝。（对其中任一端点
        // 发一个无效 key 也会在网关 404，因此我们无法据"端点缺失"与"鉴权失败"区分——
        // 只按 path 严格拒绝。）
        if path.starts_with("/v1/responses") {
            return Ok(json_error(
                warp::http::StatusCode::NOT_IMPLEMENTED,
                "NVIDIA does not expose OpenAI's Responses API (/v1/responses). \
                 Send OpenAI Chat Completions requests to /v1/chat/completions.",
            ));
        }
        if path.starts_with("/v1/messages") {
            return Ok(json_error(
                warp::http::StatusCode::NOT_IMPLEMENTED,
                "NVIDIA only serves OpenAI Chat Completions format. \
                 The Anthropic Messages API (/v1/messages) is not supported.",
            ));
        }

        // ── 元数据端点的本地应答 ─────────────────────────────────────────
        //
        // 来自 OpenAI 风格客户端（cc-switch、Cursor 等）的 `GET /v1/models` 发现探测
        // 不应消费我们池中的 key、也不应烧一个限流 token。NVIDIA 无需鉴权即应答该端点
        // 并返回其完整真实目录，因此我们透传并对结果缓存（见 `handle_models`）。我们不在
        // 该路径上转发 chat 流量。
        if method == warp::http::Method::GET
            && (path == "/v1/models" || path == "/v1/models/")
        {
            return Ok(self.handle_models().await);
        }

        // ── 用 token 饥饿切换循环获取一个 key ──────────────────────────

        // ── 用 token 饥饿切换循环获取一个 key ──────────────────────────
        let max_switches = self.retry_config.max_key_switches as usize;
        let token_timeout = self.token_wait_ms;
        let mut tried_key_hashes: Vec<u64> = Vec::new();
        let mut key_handle: Option<crate::key_pool::KeyStateHandle> = None;
        let mut switch_count: u32 = 0;
        // 按模型并发槽，在本函数稍后 `model` 已知时获取。放这里以跨所有重试尝试
        // 存活（每客户端请求一个槽），要么在非流式退出路径上于本帧 drop、要么移入
        // 流式响应 body 使其生命周期延续到客户端排空 body 之时。
        let mut concurrency_guard: Option<ConcurrencyGuard> = None;

        for _attempt in 0..=max_switches {
            // 按 score 取 top-1 获取，跳过本请求已试过的 key，使刚失败（token 饥饿）
            // 的 key 不会被再次选中。
            let candidate = match self.key_pool.acquire_excluding(&tried_key_hashes).await {
                Some(k) => k,
                None => {
                    // 返回 SERVICE_UNAVAILABLE 前，释放此前持有 key 的 in_flight。
                    if let Some(prev) = key_handle.take() {
                        prev.dec_in_flight();
                    }
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(ServiceUnavailable {
                        msg: "No available API keys".into(),
                    }));
                }
            };

            // 记录该 key，使后续重试跳过它。
            let key_hash = candidate.identity();
            tried_key_hashes.push(key_hash);

            // 取新 handle 前 drop 任何旧的。
            if let Some(prev) = key_handle.take() {
                prev.dec_in_flight();
            }

            // 在截止时间前尝试消费一个 token。
            let got_token = candidate.try_consume_token(token_timeout).await;
            if !got_token {
                // token 饥饿——切到另一个 key。
                candidate.dec_in_flight();
                self.metrics.inc_key_switch_token_starvation();
                switch_count += 1;
                if switch_count <= max_switches as u32 {
                    tracing::warn!(
                        switch = switch_count,
                        "当前 key token 饥饿；切换到另一个"
                    );
                }
                continue; // 循环试下一个 key
            }

            key_handle = Some(candidate);
            break;
        }

        let key_handle = match key_handle {
            Some(k) => k,
            None => {
                self.metrics.inc_error();
                return Err(warp::reject::custom(ServiceUnavailable {
                    msg: "No available API keys (all tried keys were token-starved)".into(),
                }));
            }
        };
        // 把获取到的 handle 包进 `InFlightGuard`，使 `acquire_excluding` 所做的
        // `inc_in_flight` 即便在 warp 中途 `await` drop 本 future（客户端断连）时也
        // 恰好 decrement 一次——见上方 guard 文档。从此处起，所有显式 `dec_in_flight()`
        // / 重试切换位置都用 `key_guard.release()` / `swap()`。
        let mut key_guard = InFlightGuard::new(key_handle);

        // 为本客户端请求生成一个唯一关联 id。所有上游尝试（含重试）将共享此 id，
        // 使我们能在 failure.log 与 tracing 中追到重试链。
        let client_req_id = format!("r{}", next_req_id());

        // 一次性解析 body 以做校验与元数据提取。`body_value` 是我们即将上游转发的
        //（可能被改写过的）JSON；`body_bytes` 是由其派生的序列化字节。当客户端原始
        // body 是合法 JSON 时，我们可能改写那些 NVIDIA 拒绝其 OpenAI 风格拼法的字段
        //（`reasoning_effort`/`max_tokens` 里的布尔的值、客户端模型别名等），而非让请
        // 求失败——这正是让那些直连 NVIDIA 能工作的 cc-switch 客户端、经过代理也无需
        // 在它们一侧改 body 的关键。
        let parsed_body: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
        let body_summary = body_summary(&parsed_body);

        // ── body 规范化（原地改写解析后的 JSON）────────────────────────────
        //
        // 对 chat-completion 风格请求，我们改写少量 cc-switch / OpenAI 风格客户端发送、
        // 而 NVIDIA NIM 会以 400 拒绝的字段，而非把请求退回调用方。意图是："若它直连
        // NVIDIA 能工作，那经过我们也能工作。"
        //
        // 具体地：
        //   * `max_tokens`
        //       - 布尔 → 移除（让 NVIDIA 选模型默认）
        //       - null   → 移除
        //       - 字符串 → 解析为 int；若不是正整数则移除
        //       - 数值 <= 0 → 移除
        //   * `reasoning_effort`
        //       - `true`  → "high"
        //       - `false` / null → 移除
        //       - 不在 {low,medium,high} 中的字符串 → 移除
        //         （过去在此会 400，但有些客户端发 "auto" / "max" 作为偏好；丢掉比
        //         失败更安全）
        //   * `model` 在配置时通过 `model_aliases` 重映射。
        //
        // 现在只有一种我们无法挽救的情况留给硬 400：chat-completion 路径上、调用方
        // 明显意图是 JSON、但顶层 body 不是对象。
        let mut body_value: Option<serde_json::Value> = parsed_body.clone();
        let mut model_rewrite_log: Option<String> = None;
        // 原始客户端可见模型名（在任意别名重映射前）。保留它以使按模型并发上限与断路器
        // 可按客户端可见别名*源*（X）或别名*目标*（Y）之一来索引——运营者自然地按其中任
        // 一为这些 map 作 key。若只保留 `model_rewrite_log`（重映射时 == 目标），那按
        // 源 X 作索引的上限将悄悄永不生效（按目标查找 → None）。
        let mut model_lookup_source: Option<String> = None;
        if let Some(obj) = body_value.as_mut().and_then(|v| v.as_object_mut()) {
            // max_tokens 规范化，带下溢/上溢守护。当计算出的预算下溢（长上下文 + 预留
            // token）时 NVIDIA 会 400 "max_tokens must be at least 1, got -14758"。
            // 我们丢弃任何 <= 0 或解析失败的 max_tokens，让模型用其默认。同时把荒谬的大
            // 值 clamp 到一个安全上限（可经 `max_tokens_max` 配置）。
            if let Some(mt) = obj.get("max_tokens").cloned() {
                let max_allowed = self.max_tokens_max;
                let keep = match &mt {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            i >= 1 && (i as u64) <= max_allowed
                        } else if let Some(u) = n.as_u64() {
                            u >= 1 && u <= max_allowed
                        } else if let Some(f) = n.as_f64() {
                            f >= 1.0 && f <= max_allowed as f64
                        } else {
                            false
                        }
                    }
                    serde_json::Value::String(s) => {
                        s.parse::<i64>().is_ok_and(|x| x >= 1 && (x as u64) <= max_allowed)
                    }
                    _ => false,
                };
                if !keep {
                    warn!(
                        client_req_id = %client_req_id,
                        "丢弃无效 max_tokens（必须在 1..={} 范围）: {}", max_allowed, mt
                    );
                    obj.remove("max_tokens");
                } else if let serde_json::Value::String(s) = &mt {
                    // 把数值字符串 coerce 成数字，使 NVIDIA 看到的是 int。
                    if let Ok(n) = s.parse::<i64>() {
                        obj.insert(
                            "max_tokens".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(n)),
                        );
                    }
                }
            }

            // reasoning_effort 规范化：布尔 / 非规范 → 改写或丢弃
            if let Some(re) = obj.get("reasoning_effort").cloned() {
                let replacement: Option<serde_json::Value> = match &re {
                    serde_json::Value::Bool(b) => {
                        if *b {
                            Some(serde_json::Value::String("high".to_string()))
                        } else {
                            None
                        }
                    }
                    serde_json::Value::Null => None,
                    serde_json::Value::String(s) => match s.as_str() {
                        "low" | "medium" | "high" => Some(re.clone()),
                        _ => {
                            warn!(
                                client_req_id = %client_req_id,
                                "丢弃无法识别的 reasoning_effort 值: {}", s
                            );
                            None
                        }
                    },
                    // 数字 / 数组 / 对象：静默丢弃
                    _ => None,
                };
                match replacement {
                    Some(v) => {
                        obj.insert("reasoning_effort".to_string(), v);
                    }
                    None => {
                        obj.remove("reasoning_effort");
                    }
                }
            }

            // model 别名重映射：客户端名 → NVIDIA 就绪名
            if let Some(m) = obj.get("model").and_then(|x| x.as_str()).map(|s| s.to_string()) {
                // 在任意改写前记录客户端可见源，使上限/ 断路器查找优先用它（运营者
                // 可按 X 或 Y 之一来索引这些 map）。
                model_lookup_source = Some(m.clone());
                if let Some(target) = self.model_aliases.get(&m).cloned() {
                    if target != m {
                        debug!(
                            client_req_id = %client_req_id,
                            "重映射模型别名 '{}' -> '{}'", m, target
                        );
                        obj.insert(
                            "model".to_string(),
                            serde_json::Value::String(target.clone()),
                        );
                        model_rewrite_log = Some(target);
                    }
                }
            }

            // 对 `empty_shell_pre_clamp_max_tokens` 中列出的模型，在*首次*上游尝
            // 试前预 clamp max_tokens。当 `prompt + max_tokens` 超过模型剩余上下文
            // 预算时，NVIDIA 的 GLM 类模型会返回一个 HTTP 200 "空壳" body
            //（choices:[]、usage:null——没有推理发生），而不是一个干净的 400。在请
            // 求发出前 clamp `max_tokens` 使预算放得下、推理真正发生，而不是烧一个往
            // 返到必然的空壳再重试。
            //
            // 这对*每个*请求都运行——含流式。failure.log 中看到的流式空壳与 400
            // "context length exceeded" 都来自 max_tokens（64000+）在 ~100K token 的
            // prompt 被计入后没给输出留出空间的流式请求；入口处预 clamp 关闭了那条
            // 路径。post-shell 重试 clamp（`handle_empty_shell_failure`）仍作为兜底，
            // 针对尽管如此仍然空壳的请求。
            if let Some(target) = obj
                .get("model")
                .and_then(|m| m.as_str())
                .and_then(|m| self.empty_shell_pre_clamp_max_tokens.get(m).copied())
            {
                let current = obj.get("max_tokens").and_then(|n| match n {
                    serde_json::Value::Number(num) => num.as_u64(),
                    serde_json::Value::String(s) => s.parse::<u64>().ok(),
                    _ => None,
                });
                if let Some(cur) = current {
                    // `>=`（而非 `>`）：当 cur == target 时请求仍在上限，对 GLM 类
                    // 模型该上限等于 NVIDIA 在算入大 prompt 后剩下的剩余上下文预算
                    //——因此即便 cur==target 也无输出空间并空壳。在达到上限或以上
                    // 时 clamp，使主导情形（客户端发刚好等于上限的值，如
                    // max_tokens=32000）在首次尝试即被捕获、而不发出去空壳。
                    // failure.log 显示 216/216 首次尝试空壳都带 max_tokens == 已配置
                    // 的 32000 target，即严格 `>` 在它们身上从不触发。
                    if cur >= target {
                        debug!(
                            client_req_id = %client_req_id,
                            max_tokens_before = cur,
                            max_tokens_after = target,
                            "预 clamp max_tokens 以适配模型上下文预算",
                        );
                        obj.insert(
                            "max_tokens".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(target)),
                        );
                    }
                }
            }

            // stream_options.include_usage 注入（流式 token 用量）：NVIDIA/OpenAI 带
            // 请求里 `stream_options.include_usage: true` 时才在 SSE 流末尾 emit
            // `usage`。注入它使我们的流式请求 token 指标被填充，但绝不覆盖客户端已
            // 设的值（true 或 false）——仅当 `include_usage` 缺省时才注入。
            if self.inject_stream_usage {
                let is_stream = obj
                    .get("stream")
                    .and_then(|s| s.as_bool())
                    .unwrap_or(false);
                if is_stream {
                    let client_set = obj
                        .get("stream_options")
                        .and_then(|so| so.get("include_usage"))
                        .is_some();
                    if !client_set {
                        let mut so = obj
                            .get("stream_options")
                            .and_then(|v| v.as_object().cloned())
                            .unwrap_or_default();
                        so.insert(
                            "include_usage".to_string(),
                            serde_json::Value::Bool(true),
                        );
                        obj.insert(
                            "stream_options".to_string(),
                            serde_json::Value::Object(so),
                        );
                    }
                }
            }

            // tools 规范化：NVIDIA 的 ToolDescription 要求 `description`
            // 与（当嵌在 `function` 下时）`function.description` 是 *string*——它
            // 以 400 拒绝 `null`（pydantic "Input should be a valid string,
            // input_value=None"）。cc-switch / Claude Code 发出的 tool 定义中有些
            // tool 不带 description（渲染成 `null`），它们被原样透传到 NVIDIA 并 400
            //（见 failure.log）。把缺失或为 null 的 `description` coerce 成空串；对
            // max_tokens/reasoning_effort 已用的空 type 标注在此也适用。我们还防御性地
            // coerce `name`（NVIDIA 要求非 null），但保留 `parameters` 不动。
            //
            // 处理客户端可能发的两种形态：
            //   * flat：  {type, name, description, parameters}
            //   * nested（OpenAI 规范）：{type:"function", function:{name, description, parameters}}
            if let Some(arr) = obj.get_mut("tools").and_then(|t| t.as_array_mut()) {
                let mut fixed = 0u32;
                for tool in arr.iter_mut() {
                    // 目标对象：有嵌套 `function` 时取它，否则取 tool 自身（flat 形态）。
                    let target: Option<&mut serde_json::Map<String, serde_json::Value>> =
                        if let Some(func) = tool.get_mut("function") {
                            func.as_object_mut()
                        } else {
                            tool.as_object_mut()
                        };
                    let Some(tobj) = target else { continue };

                    // 当字段缺省或显式为 null 时需要修；一个真实字符串（哪怕是先前工
                    // 具链路经我们往返后留下的空 ""）保持原样。
                    let need_desc = tobj
                        .get("description")
                        .is_none_or(serde_json::Value::is_null);
                    if need_desc {
                        tobj.insert(
                            "description".to_string(),
                            serde_json::Value::String(String::new()),
                        );
                        fixed += 1;
                    }
                    let need_name = tobj.get("name").is_none_or(serde_json::Value::is_null);
                    if need_name {
                        tobj.insert(
                            "name".to_string(),
                            serde_json::Value::String(String::new()),
                        );
                        fixed += 1;
                    }
                }
                if fixed > 0 {
                    warn!(
                        client_req_id = %client_req_id,
                        tool_fields_fixed = fixed,
                        "把 null/缺失的 tool description/name coerce 成空串（NVIDIA 拒绝 null）"
                    );
                }
            }
        }

        // 重新序列化（可能改写过的）body。若原始 body 非 JSON，回退到原始字节不动——
        // 此处非 JSON body 罕见，原样转发。
        let body_bytes: bytes::Bytes = match &body_value {
            Some(v) => serde_json::to_vec(v)
                .map(bytes::Bytes::from)
                .unwrap_or_else(|_| body.clone()),
            None => body.clone(),
        };

        // 提取 `model` 用于日志。优先取（可能改写过的）值，使日志显示 NVIDIA 实际看到
        // 的名字。
        let model = model_rewrite_log
            .clone()
            .or_else(|| {
                body_value
                    .as_ref()
                    .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string()))
            })
            .unwrap_or_default();

        // 转发（可能改写过的）body。尊重客户端的 `stream` 字段，使要流式产出的
        // chat-completions 客户端拿到流式 body（无 `bytes().await` 缓冲）。
        let request_body = body_bytes.clone();
        let is_streaming = body_value
            .as_ref()
            .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
            .unwrap_or(false);

        // ── 硬 prompt 大小上限：在消费 key/槽 之前就拒绝 ────────────────
        // failure.log 显示那些注定失败请求在先前上限之下就已空壳：第 1 轮 77 个空壳落在
        // [98288, 109231]（在从不触发的 120000 上限之下），第 2 轮 32 个落在
        // [92488, 93445]（在一个 [90k, 95k) 的"一次性"灰区里，0% 成功并产生一个客户端
        // 重试约 10 分钟的误导 502）。估算约 92k 时，`prompt + any max_tokens` 已超过
        // glm-5.2 的 ~128k 窗（`chars/4` 估算少算真实 token）。与其为一个不可能成功的请
        // 求烧一个 key 槽与一次上游往返，不如预先以 413 拒绝并让客户端自行压缩其上下
        // 文。我们*不*改写客户端 messages——客户端拥有其对话历史（Claude Code 的
        // /compact 由它跑）；代理的职责是大声失败、而非静默改动。该上限（默认 90000）
        // 与软不重试上限设为相等，故无灰区：注定失败请求立即命中清晰 413、零上游往返，
        // 而非一个客户端会重试的 502 "empty chat completion"。`request_max_prompt_tokens
        // == 0` 时禁用。
        if self.request_max_prompt_tokens > 0 {
            if let Some(est) = estimate_prompt_tokens(&body_bytes, &self.est_costs) {
                if est >= self.request_max_prompt_tokens {
                    warn!(
                        client_req_id = %client_req_id,
                        est_prompt_tokens = est,
                        limit = self.request_max_prompt_tokens,
                        model = %model,
                        "拒绝请求：估算 prompt token 超过硬上限；客户端须压缩其上下文"
                    );
                    self.metrics.inc_error();
                    self.metrics
                        .inc_prompt_rejected_total(self.metrics_model(model.as_str()), "413");
                    let err = serde_json::json!({
                        "error": {
                            "message": format!(
                                "Estimated prompt tokens (~{}) reach or exceed the proxy's context \
                                 ceiling ({}) for this model; the request cannot fit the model's \
                                 context window and would be rejected upstream. Compress your \
                                 conversation context (e.g. clear history / run /compact) and retry.",
                                est, self.request_max_prompt_tokens
                            ),
                            "type": "request_too_large",
                            "code": "prompt_tokens_exceeds_ceiling",
                            "client_req_id": client_req_id,
                            "est_prompt_tokens": est,
                            "limit": self.request_max_prompt_tokens,
                        }
                    });
                    return Ok(json_value_response(
                        warp::http::StatusCode::PAYLOAD_TOO_LARGE,
                        &err,
                    ));
                }
            }
        }

        // 构造上游 URL
        let mut url = self.base_url.clone();
        url.push_str(&path);

        debug!(client_req_id = %client_req_id, body_summary = %body_summary, "代理请求至: {} {}", method.as_str(), url);

        // ---- 并发信号量（Priority 2）----
        // 在我们试图取 key 之前，先为本模型并发上限获取一个槽。这避免压垮上游
        // worker（如 nemotron-3-ultra 的 32-worker 上限）。
        //
        // 先按客户端可见源名查找（运营者自然按别名源 X 来为
        // `concurrency_limit_per_model` 作 key），回退到重映射后的目标 Y——于是按
        // 任一形态作 key 的上限都适用。见上文 `model_lookup_source`。
        let cap_key: Option<String> =
            model_lookup_source.clone().or_else(|| Some(model.clone()));
        let cap_lookup = cap_key
            .as_ref()
            .and_then(|k| self.concurrency_limit_per_model.get(k))
            .or_else(|| self.concurrency_limit_per_model.get(&model));
        if let Some(&limit) = cap_lookup {
            if limit > 0 {
                // 按模型惰性初始化信号量（用 StdMutex 做内部可变性、线程安全）
                static SEMAPHORES: OnceLock<StdMutex<HashMap<String, Arc<Semaphore>>>> = OnceLock::new();
                let semaphores = SEMAPHORES.get_or_init(|| StdMutex::new(HashMap::new()));

                // 取或创建本模型信号量
                let semaphore = {
                    let mut map = semaphores.lock().unwrap();
                    map.entry(model.clone())
                        .or_insert_with(|| Arc::new(Semaphore::new(limit as usize)))
                        .clone()
                };

                let queue_timeout = Duration::from_millis(self.concurrency_queue_timeout_ms);
                let permit = match tokio::time::timeout(queue_timeout, semaphore.clone().acquire_owned()).await {
                    Ok(Ok(permit)) => {
                        self.metrics.inc_concurrency_acquire_total(&model, "ok");
                        permit
                    }
                    Ok(Err(_)) => {
                        self.metrics.inc_concurrency_acquire_total(&model, "closed");
                        return Err(warp::reject::custom(ServiceUnavailable {
                            msg: format!("Concurrency semaphore closed for model {}", model),
                        }));
                    }
                    Err(_) => {
                        self.metrics.inc_concurrency_acquire_total(&model, "timeout");
                        let resp = warp::http::Response::builder()
                            .status(warp::http::StatusCode::SERVICE_UNAVAILABLE)
                            .header("Retry-After", (self.concurrency_queue_timeout_ms / 1000).max(1).to_string())
                            .body(warp::hyper::Body::from(format!("Model {} concurrency limit reached, queue timeout", model)))
                            .unwrap();
                        return Ok(resp);
                    }
                };

                // 获取时记录当前并发（取走 permit 后）。
                self.metrics.set_concurrency_current(&model, semaphore.available_permits() as i64);

                // guard 持有 permit 并在 drop 时刷新 gauge。流式成功 body 它被移入
                // 响应流（`GuardedStream`），故仅当客户端排空完 body 时槽才归还信号
                // 量——与 NVIDIA 上游 worker 实际释放之时一致——而非 `handle_request`
                // 返回之时（可能远早于流结束）。对其它每个退出路径，它在 `handle_request`
                // 本帧 drop，permit 被回收。
                let guard = ConcurrencyGuard {
                    permit,
                    semaphore: semaphore.clone(),
                    metrics: self.metrics.clone(),
                    model: model.clone(),
                };
                concurrency_guard = Some(guard);
            }
        }

        // --- 带指数退避重试的上游尝试循环 ---
        //
        // 仅 401 *不带* `Retry-After` 才触发对另一个健康 key 的重试，带指数退避
        //（backoff_base * 2^attempt，以 backoff_max 为上限）睡眠。每次这种失败都会
        // 递增该 key 的 fail_count；一旦超过 `auth_fail_threshold` 即把该 key 标记为
        // Disabled。
        //
        // 401 *带* `Retry-After`，以及任何情况下的 429/403，都*不*重试：它们是确定性
        // 上游决策，在另一个 key 上重试只会烧掉整个池。
        let mut attempt: u32 = 0;
        loop {
            let is_last_attempt = attempt >= self.retry_config.max_retries;
            let attempt_fut = self.process_upstream_attempt(
                key_guard.handle(),
                &method,
                &url,
                &headers,
                &request_body,
                is_streaming,
                &model,
                &body_summary,
                &client_req_id,
                is_last_attempt,
                attempt,
                body_value.is_some(),
                &mut concurrency_guard,
            );

            // 对流式请求，我们有显式流式超时（>0）；未设（0）时继承
            // `request_timeout_secs`——这与 config 文档字符串一致（"0 = use
            // request_timeout_seconds"），故一个停摆的上游 worker 不会在持有健康池流量
            // 人质的同时永久钉住一个 key + 并发槽。该超时只 bounds connect + send +
            // 接收 headers + 有界 peek 前缀（TTFB-ish），而非流的总排空：长 token 生成
            // 不被中止。
            let timeout_secs = if is_streaming {
                if self.request_timeout_streaming_secs > 0 {
                    self.request_timeout_streaming_secs
                } else {
                    self.request_timeout_secs
                }
            } else {
                self.request_timeout_secs
            };
            let result = if timeout_secs == 0 {
                attempt_fut.await
            } else {
                match tokio::time::timeout(
                    Duration::from_secs(timeout_secs),
                    attempt_fut,
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => {
                        let k = key_guard.handle().key().await;
                        warn!(key = %mask_key(&k), timeout_secs, "上游请求超时");
                        self.key_pool.mark_bad(&k, None).await;
                        Err(AttemptError::Timeout)
                    }
                }
            };

            match result {
                Ok(AttemptResponse::Done(resp)) => {
                    key_guard.release();
                    self.metrics.observe_latency(start.elapsed().as_secs_f64());
                    debug!(
                        client_req_id = %client_req_id,
                        total_attempts = attempt + 1,
                        elapsed_ms = start.elapsed().as_millis(),
                        "请求成功完成"
                    );
                    return Ok(resp);
                }
                Ok(AttemptResponse::Retry) => {
                    // 可重试上游状态（不带 Retry-After 的 401、503 或 500）——调用方仍有
                    // 重试预算。
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.acquire_retry_key(&mut tried_key_hashes).await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            // 切换前释放当前 handle。
                            key_guard.release();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!("重试 key token 饥饿；放弃");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream retry failed".into(),
                                }));
                            }
                            // `acquire_retry_key` 已对 `next` inc；把它移入 guard
                            //（上方旧 key 已释放，故 `swap` 内部的 `release()` 是 no-op）。
                            key_guard.swap(next);
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "用另一个 key 重试上游尝试"
                            );
                            continue;
                        }
                        warn!(client_req_id = %client_req_id, attempt = attempt, "无其他可用 key；外抛可重试错误");
                    }
                    // 防御性回退（helper 在最后一次尝试时应已返回 Done）。
                    key_guard.release();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream retryable error".into(),
                    }));
                }
                Err(AttemptError::Network(e)) => {
                    // 网络层错误：冷却 key，且若仍有重试次数则换另一个 key、不把它计为
                    // 鉴权失败。
                    let k = key_guard.handle().key().await;
                    warn!(key = %mask_key(&k), "请求发送错误: {}", e);
                    failure_log::write_failure(
                        method.as_str(),
                        &url,
                        &url,
                        &model,
                        &k,
                        None,
                        "network_error",
                        &e.to_string(),
                        "",
                        &body_summary,
                        attempt,
                        Some("network"),
                        next_req_id(),
                        &client_req_id,
                        Some("network"),
                    );
                    self.key_pool.mark_bad(&k, None).await;
                    // 网络错误常跨 key 相关（NVIDIA 维护、DNS 抖动、本地网络中断）：在
                    // 不同 key 上重试 `max_retries`（默认 3）次只会让已经收了 ~10s 连接
                    // 超时的客户端更痛苦。把网络错误重试上限封为 1 次额外尝试。
                    let network_retry_cap: u32 = 1;
                    if attempt < network_retry_cap && attempt < self.retry_config.max_retries {
                        if let Some(next) = self.acquire_retry_key(&mut tried_key_hashes).await {
                            let will_retry_again = false; // 网络上限 = 1 → 无进一步重试
                            self.metrics.inc_retry_attempt_failure();
                            self.metrics.inc_network_errors_total(self.metrics_model(&model), "connection");
                            key_guard.release();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!(client_req_id = %client_req_id, attempt = attempt, "网络错误后重试 key token 饥饿");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream request failed".into(),
                                }));
                            }
                            key_guard.swap(next);
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "用另一个 key 重试上游发送"
                            );
                            continue;
                        }
                    }
                    key_guard.release();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream request failed".into(),
                    }));
                }
                Err(AttemptError::Timeout) => {
                    let k = key_guard.handle().key().await;
                    warn!(key = %mask_key(&k), "上游请求超时");
                    failure_log::write_failure(
                        method.as_str(),
                        &url,
                        &url,
                        &model,
                        &k,
                        None,
                        "timeout",
                        &format!(
                            "exceeded request_timeout_secs={}",
                            self.request_timeout_secs
                        ),
                        "",
                        &body_summary,
                        attempt,
                        Some("timeout"),
                        next_req_id(),
                        &client_req_id,
                        Some("timeout"),
                    );
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.acquire_retry_key(&mut tried_key_hashes).await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            key_guard.release();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!(client_req_id = %client_req_id, attempt = attempt, "超时后重试 key token 饥饿");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream request timed out".into(),
                                }));
                            }
                            key_guard.swap(next);
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "上游超时后用另一个 key 重试"
                            );
                            continue;
                        }
                    }
                    key_guard.release();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream request timed out".into(),
                    }));
                }
                Err(AttemptError::BodyRead(e)) => {
                    let k = key_guard.handle().key().await;
                    error!(key = %mask_key(&k), "读取响应 body 失败: {}", e);
                    failure_log::write_failure(
                        method.as_str(),
                        &url,
                        &url,
                        &model,
                        &k,
                        None,
                        "body_read_error",
                        &e.to_string(),
                        "",
                        &body_summary,
                        attempt,
                        Some("body_read"),
                        next_req_id(),
                        &client_req_id,
                        Some("body_read"),
                    );
                    key_guard.release();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Failed to read response body".into(),
                    }));
                }
                Err(AttemptError::Fatal(r)) => {
                    key_guard.release();
                    return Err(r);
                }
            }
        }
    }

    /// 带抖动的指数退避睡眠：`base * 2^(attempt-1) + jitter`，以 `max` 为上限。
    /// 为一次重试获取一个 key，优先选本请求尚未用过的。把所选 key 的 identity 记录
    /// 进 `tried_key_hashes`。
    ///
    /// 当所有活跃 key 本请求都已试过时回退到 `acquire()`（允许重试已用过的
    /// key）——例如单 key 部署。在那里，对同一 key 在 401/503 上重试比外抛 5xx 更
    /// 可取，因为 NVIDIA 侧瞬态失败常在第二次尝试时清除，而原始重试预算恰为此存在。
    async fn acquire_retry_key(
        &self,
        tried_key_hashes: &mut Vec<u64>,
    ) -> Option<crate::key_pool::KeyStateHandle> {
        if let Some(k) = self.key_pool.acquire_excluding(tried_key_hashes).await {
            tried_key_hashes.push(k.identity());
            return Some(k);
        }
        if let Some(k) = self.key_pool.acquire().await {
            let h = k.identity();
            if !tried_key_hashes.contains(&h) {
                tried_key_hashes.push(h);
            }
            Some(k)
        } else {
            None
        }
    }

    async fn backoff_sleep(&self, attempt: u32) {
        let base = self.retry_config.backoff_base_ms;
        let max = self.retry_config.backoff_max_ms;
        let jitter = self.retry_jitter_ms;
        // 首次重试 attempt 从 1 开始 → 延迟 = base
        let exp = if attempt == 0 { 0 } else { attempt - 1 };
        let mut delay = base.saturating_mul(1u64 << exp.min(20));
        delay = delay.min(max);
        // 加抖动：[0, jitter_ms] 区间的随机值
        if jitter > 0 {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            delay = delay.saturating_add(rng.gen_range(0..=jitter));
        }
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    /// 发送一次上游请求、分类响应，并构造回复。
    ///
    /// 仅当 `is_last_attempt` 为 false 时不带 `Retry-After` 的 401 返回 `Retry`。
    /// 其它所有结果要么是已完成响应、要么是不可重试错误。
    #[allow(clippy::too_many_arguments)]
    async fn process_upstream_attempt(
        &self,
        key_handle: &crate::key_pool::KeyStateHandle,
        method: &warp::http::Method,
        url: &str,
        headers: &warp::http::HeaderMap,
        request_body: &bytes::Bytes,
        is_streaming: bool,
        model: &str,
        body_summary: &str,
        client_req_id: &str,
        is_last_attempt: bool,
        attempt: u32,
        request_body_is_json: bool,
        concurrency_guard: &mut Option<ConcurrencyGuard>,
    ) -> Result<AttemptResponse, AttemptError> {
        let key_str = key_handle.key().await;
        let key_hash = {
            let addr = key_handle as *const crate::key_pool::KeyStateHandle as usize;
            addr as u64
        };
        debug!(
            client_req_id = %client_req_id,
            attempt = attempt,
            key = %mask_key(&key_str),
            key_hash = key_hash,
            body_summary = %body_summary,
            "上游尝试开始"
        );
        let req_builder = self.build_upstream_request(url, method, headers, request_body, &key_str, is_streaming, request_body_is_json);
        let resp = req_builder.send().await.map_err(AttemptError::Network)?;

        let status = resp.status();
        let content_type = resp
            .headers()
            .get(req_header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        debug!(
            client_req_id = %client_req_id,
            attempt = attempt,
            status = %status,
            content_type = %content_type,
            url = %url,
            "上游响应"
        );

        let is_sse = content_type.contains("text/event-stream");

        // 在消费 body 前克隆 headers
        let resp_headers: Vec<(String, Vec<u8>)> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
            .collect();

        // 透传流式：当客户端要 `stream`、且上游以 SSE 响应时，原样把上游流转发
        // 而不缓冲。
        if is_streaming && is_sse && status.is_success() {
            let resp = self
                .handle_passthrough_stream(resp, resp_headers, status, client_req_id, concurrency_guard.take())
                .await
                .map_err(AttemptError::Fatal)?;
            return Ok(AttemptResponse::Done(resp));
        }

        // 成功的非 SSE 响应：把上游 bytes_stream 转发给客户端、不缓冲。NVIDIA
        // worker 期望下游排空 body 以释放推理资源；`resp.bytes().await` 会把整个
        // body 驻内存直至（可能慢的）客户端读完，触发 503 `ResourceExhausted:
        // Worker local total request limit reached`。在此标记 key 为 good，然后
        // 流式产出。
        //
        // 仅当启用空 choices 合成时才有界地 peek body：把 body 读到
        // `EMPTY_CHOICES_PEEK_BYTES` 为止，若解析为带空 `choices` 数组的 JSON 对象，
        // 则改写为一个合法的单 choice 响应（使 cc-switch 的非流式转换器不在其上 422）。
        // 任何其它 body——非 JSON、大完成体、或非空 `choices`——回到与从前相同的零
        // 所有权透传，并把已缓冲前缀 prepend 到流上。
        if status.is_success() {
            // 推迟 `mark_good` / `inc_success` 直到我们知道这个 200 究竟是真实完成
            // 还是 NVIDIA 空壳软失败（200、`usage:null`、无推理）。空壳不是成功：把它当
            // 成成功既会虚增成功指标，又会通过 `mark_good` →
            // `reset_all_circuit_breakers` 清掉空壳路径（下方
            // `handle_empty_shell_failure`）正在累积的每模型 cb 计数器，使空壳断路器
            // 在默认阈值下几乎不可达。对遗留路径（合成禁用）我们无法检视 body，因此
            // 如前那样早早标 good。
            let outcome = if self.synthesize_empty_choices {
                self.handle_success_maybe_synthesize(resp, resp_headers, status, client_req_id, concurrency_guard)
                    .await
            } else {
                // 合成禁用——空壳重试路径无法检视 body，故原样透传（遗留行为）；
                // 此为真实成功。
                self.key_pool.mark_good(&key_str).await;
                self.metrics.inc_success();
                self.handle_passthrough_stream(resp, resp_headers, status, client_req_id, concurrency_guard.take())
                    .await
                    .map(SuccessOutcome::Response)
            }
            .map_err(AttemptError::Fatal)?;
            return Ok(match outcome {
                SuccessOutcome::Response(resp) => {
                    // 真实完成（合成路径已确认 choices，或遗留透传上方已标 good）。
                    // 合成路径仅在此——body 通过空壳检查后——才标 good。
                    if self.synthesize_empty_choices {
                        self.key_pool.mark_good(&key_str).await;
                        self.metrics.inc_success();
                    }
                    AttemptResponse::Done(resp)
                }
                SuccessOutcome::ShellFailure { body } => {
                    // NVIDIA 空壳软失败——无推理发生。外抛真实原因、而非误导性的空
                    // body。可选地用 clamp 后的 `max_tokens` 重试一次。刻意在这里
                    // *不* 标 good / inc_success：空壳是软失败，cb 计数器必须保留，使
                    // 重复空壳能击穿断路器。
                    self.handle_empty_shell_failure(
                        body,
                        method,
                        url,
                        headers,
                        request_body,
                        model,
                        &key_str,
                        body_summary,
                        client_req_id,
                        attempt,
                    )
                    .await?
                }
            });
        }

        // 非成功路径仍缓冲 body——我们需要字节做分类（Retry-After 解析、
        // error.code coerce、token 统计、failure.log）。错误是短 JSON，故缓冲其开销
        // 低、且不延长 worker 存活（错误会尽快释放）。
        let body_bytes = resp.bytes().await.map_err(AttemptError::BodyRead)?;

        // `resp.headers()` 在 `resp.bytes()` 消费 `resp` 后已不可访问；此处从克隆的
        // `resp_headers` 快照一次性解析 Retry-After，以供所有 401/429/403 分支复用。
        let retry_after: Option<u64> = resp_headers
            .iter()
            .find(|(k, _)| k == req_header::RETRY_AFTER.as_str())
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok());

        // 从此起 status 为非成功（成功路径已在上方返回）。
        if status == reqwest::StatusCode::UNAUTHORIZED {
            // 401 是我们唯一重试的状态：它是最清晰的"该特定 key 被拒"信号。
            if let Some(ra) = retry_after {
                warn!(
                    key = %mask_key(&key_str),
                    retry_after = ra,
                    "上游 401 带 Retry-After；冷却 key，不重试"
                );
                self.key_pool.mark_bad(&key_str, Some(ra)).await;
                self.metrics.inc_error();
                // 继续下去：直接交付此响应。
            } else {
                // 记录失败（同时把该 key 权重减半，使下一次获取不太会再选它）。
                self.key_pool
                    .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                    .await;
                if !is_last_attempt {
                    return Ok(AttemptResponse::Retry);
                }
                warn!(
                    key = %mask_key(&key_str),
                    attempt_limit = self.retry_config.max_retries,
                    "重试预算耗尽；透传 401"
                );
                self.metrics.inc_error();
                // 继续下去：直接交付此响应。
            }
        } else if status == reqwest::StatusCode::INTERNAL_SERVER_ERROR {
            // 来自 NVIDIA/GLM 类模型的 500（`{"message":"Failed to generate
            // completions",...}`）是一个瞬态 per-request worker 失败——failure.log 中
            // 见于 z-ai/glm-5.2、同一 prompt 数秒后在另一个 key 上成功。当剩余预算、且
            // retry_on_500 启用时，在另一个 key 上重试（镜像 503 路径）。此处不尊重
            // Retry-After，因为 NVIDIA 的 500 不带它；把 key 标为降权而非 bad。
            if !is_last_attempt && self.retry_on_500 {
                self.key_pool
                    .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                    .await;
                warn!(
                    client_req_id = %client_req_id,
                    key = %mask_key(&key_str),
                    status = %status,
                    "上游 500（Failed to generate completions）；在另一个 key 上重试"
                );
                self.metrics.inc_error();
                return Ok(AttemptResponse::Retry);
            }
            warn!(
                key = %mask_key(&key_str),
                attempt_limit = self.retry_config.max_retries,
                "500 重试预算耗尽或重试被禁用；透传"
            );
            self.metrics.inc_error();
            // 继续下去：直接交付此响应。
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::FORBIDDEN
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            // 429、403 与 503 是确定性上游决策。
            // 429/403：若带 Retry-After 则尊重之、否则原样外抛。
            // 503（Worker local total request limit reached）：瞬态容量问题——若有
            // 重试预算则换另一个 key。
            // 仅 403 "Authorization failed" 是账户级封禁（立即冷却）。
            let is_503 = status == reqwest::StatusCode::SERVICE_UNAVAILABLE;

            if let Some(ra) = retry_after {
                warn!(
                    key = %mask_key(&key_str),
                    status = %status,
                    retry_after = ra,
                    "上游 {} 带 Retry-After；冷却 key",
                    status.as_u16()
                );
                self.key_pool.mark_bad(&key_str, Some(ra)).await;
            } else if is_503 {
                // 不带 Retry-After 的 503：瞬态容量耗尽。若有重试预算则换另一个 key。
                if !is_last_attempt && self.retry_on_503 {
                    // 重试前可选的健康探测
                    if self.probe_before_503_retry {
                        let probe_ok = self.probe_upstream_health().await;
                        if !probe_ok {
                            warn!(
                                client_req_id = %client_req_id,
                                "503 重试前上游健康探测失败；透传 503"
                            );
                            self.metrics.inc_error();
                            // 继续下去交付 503 响应
                        } else {
                            // 探测成功，继续重试
                            self.key_pool
                                .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                                .await;
                            return Ok(AttemptResponse::Retry);
                        }
                    } else {
                        self.key_pool
                            .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                            .await;
                        return Ok(AttemptResponse::Retry);
                    }
                }
                warn!(
                    key = %mask_key(&key_str),
                    attempt_limit = self.retry_config.max_retries,
                    "503 重试预算耗尽或重试被禁用；透传"
                );
                self.metrics.inc_error();
                // 继续下去：直接交付此响应。
            } else {
                // 不带 Retry-After 的 429/403
                let body_str = std::str::from_utf8(&body_bytes).unwrap_or("");
                let is_auth_ban = status == reqwest::StatusCode::FORBIDDEN
                    && body_str.contains("Authorization failed");
                if is_auth_ban {
                    warn!(
                        key = %mask_key(&key_str),
                        "上游 403 Authorization failed；立即冷却"
                    );
                    self.key_pool.mark_bad(&key_str, None).await;
                    self.metrics.inc_key_403_auth_cooldown();
                } else {
                    // 不带 Retry-After 的 429（或非 auth-ban 的 403）属于上游的
                    // 确定性限流 / 区域-账号门控，*不是*认证失败。改用短路冷却
                    //（`mark_bad(None) → cooldown.base_seconds`），让 cool_down_task
                    // 在 60s 后把它无探测转回 active，或健康探测在 ready_at 后
                    // 探活恢复。刻意**不**走 `mark_failure`：那条路径会累加
                    // `fail_count` 并在达到 `auth_fail_threshold`（默认 3）时把
                    // 该 key 置入 `Disabled`（兜底 `cooldown.max_seconds`=600s，
                    // 且探测对 429 也救不回来）。一个被 NVIDIA 限流的合法 key
                    // 累计 3 次这样的 429 就会被关进 600s 禁用，池在真实流量下
                    // 会自我收缩到零。这里只降权（避免本轮重试再次命中它）+ 短冷却。
                    warn!(
                        key = %mask_key(&key_str),
                        status = %status,
                        "上游 {} 不带 Retry-After；短冷却（base_seconds），不计入禁用阈值",
                        status.as_u16()
                    );
                    self.key_pool.mark_bad(&key_str, None).await;
                }
            }
            self.metrics.inc_error();
        } else {
            // 其它 4xx/5xx：原样外抛、不对 key 惩罚。
            self.metrics.inc_error();
        }

        // ── 针对 empty-shell/504/503 响应的断路器 ────────────────────────
        // 跟踪每 key 连续空壳软失败（200、usage:null）与 504 网关超时 / 503 不可用。
        // 当达到阈值则冷却该 key，防止对降级端点反复重试。
        if status == reqwest::StatusCode::OK {
            // 检查这是否为空壳响应（已在前文 handle_empty_shell_failure 里记录）。
            // failure.log 已捕获它，故这里仅通过判断响应是否为空壳来跟踪。
            // 注意：我们只对那些未被成功路径处理的 2xx 响应到达这里（例如经
            // handle_empty_shell_failure 返回 502 的空壳）。
        } else if status == reqwest::StatusCode::GATEWAY_TIMEOUT
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            // 504（Gateway Timeout）或 503（Service Unavailable: Worker local
            // total request limit reached）——计入断路器（若配置则按模型）
            if self.circuit_breaker_threshold > 0 {
                let cb_model = if self.circuit_breaker_per_model { model } else { "global" };
                self.key_pool.increment_circuit_breaker(&key_str, cb_model, self.circuit_breaker_threshold).await;
                if self.key_pool.check_circuit_breaker(&key_str, cb_model, self.circuit_breaker_threshold).await {
                    warn!(
                        key = %mask_key(&key_str),
                        model = %model,
                        threshold = self.circuit_breaker_threshold,
                        status = %status.as_u16(),
                        "断路器触发：连续过多 503/504/空壳响应；冷却 key"
                    );
                    self.key_pool.mark_bad(&key_str, None).await;
                    self.metrics.inc_circuit_breaker_triggered();
                }
            }
        }

        // 成功 Chat Completion 响应的 token 统计在已缓冲 body 时收集——即仅下方非
        // 成功路径上、而这些路径从无 usage。成功响应以流转发、我们不缓冲 body，故此
        // 处无可记录。日后需要流上 usage 时，用一个解析最终 chunk 的 tap 包住流。

        // ── 结构化失败日志（failure.log JSONL）──────────────────────────
        // 单一 hook 覆盖所有非 2xx 上游结果。我们要浮现的诊断要点：status、model
        //（在存在时）、脱敏 key、上游 URL、重试尝试，以及一个短 body 节选——让我们
        // 能分类 NVIDIA 的 "Function 'uuid': Not found for account 'xxxx'" 404、
        // 限流 JSON、或空 / 错误 body。
        if !status.is_success() {
            let body_excerpt = {
                let s = std::str::from_utf8(&body_bytes).unwrap_or("");
                // 取前 512 字符；body 是短错误 JSON。
                s.chars().take(512).collect::<String>()
            };
            let (function_id, account_id) =
                failure_log::extract_nvidia_function_detail(&body_excerpt);
            let category = if status.is_client_error() {
                "upstream_4xx"
            } else if status.is_server_error() {
                "upstream_5xx"
            } else {
                "upstream_other"
            };
            // detail：当存在 function_id / account_id 时纳入，使 jq 查询可按它们分组
            // 而无需重新解析 body。
            let detail = if !function_id.is_empty() || !account_id.is_empty() {
                format!("function_id={} account_id={}", function_id, account_id)
            } else {
                String::new()
            };
            // endpoint = 上游 URL 的 path 部分（在 base_url 之后）
            let endpoint = url
                .find("//")
                .and_then(|i| url[i + 2..].find('/').map(|j| i + 2 + j))
                .map(|i| &url[i..])
                .unwrap_or(url);
            failure_log::write_failure(
                method.as_str(),
                endpoint,
                url,
                model,
                &key_str,
                Some(status.as_u16()),
                category,
                &detail,
                &body_excerpt,
                body_summary,
                attempt,
                None,
                next_req_id(),
                client_req_id,
                None,
            );
        }

        // 把数值型 `error.code` 规范成字符串。NVIDIA 返回
        // {"error":{"code":503,...}}（数字），但 OpenAI 的 schema 与 Cursor 这类 Go
        // 客户端期望 `code` 为字符串。无条件应用到每个缓冲 body——无 `code` 字段的
        // 成功响应为 no-op。
        let final_body = coerce_json_body(&body_bytes);

        let mut response_builder = warp::http::Response::builder().status(status);

        for (key, value) in resp_headers {
            match key.as_str() {
                "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailers"
                | "transfer-encoding"
                | "upgrade"
                | "content-length" => continue,
                _ => {}
            }
            if let Ok(hv) = warp::http::HeaderValue::from_bytes(&value) {
                if let Some(headers) = response_builder.headers_mut() {
                    let key_bytes = key.as_bytes().to_vec();
                    let hname =
                        warp::http::HeaderName::from_bytes(&key_bytes).unwrap_or_else(|_| {
                            warp::http::HeaderName::from_bytes(b"x-unknown").unwrap()
                        });
                    headers.append(hname, hv);
                }
            }
        }

        let body = warp::hyper::Body::from(final_body);
        let resp = response_builder.body(body).map_err(|e| {
            AttemptError::Fatal(warp::reject::custom(BadGateway {
                msg: format!("Failed to build response: {}", e),
            }))
        })?;
        Ok(AttemptResponse::Done(resp))
    }

/// 处理透传流式：把上游 SSE / 字节流转发给客户端。每个 chunk 经过
/// `coerce_sse_chunk`，后者把 `data:` 行里数值型 `error.code` 改写为字符串；非
/// `data:` 字节与不含 `code` 字段的 chunk 原样转发。一个 `UsageTap` 包住流以计入
/// token 用量，并在（成功路径流上）响应没有真实 `choices` 内容时记录一条空 choices
/// 诊断。
async fn handle_passthrough_stream(
    &self,
    resp: reqwest::Response,
    resp_headers: Vec<(String, Vec<u8>)>,
    status: warp::http::StatusCode,
    client_req_id: &str,
    concurrency_guard: Option<ConcurrencyGuard>,
) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
    let upstream = resp.bytes_stream();
    // 先把上游 reqwest 错误映射为 std::io::Error，再用 CoerceSseTap 包住：把跨
    // chunk 边界拆分的 `data:` 行重拼后强转数值 `error.code` 为字符串（治愈原逐-chunk
    // coerce_sse_chunk 的盲区）。再由下游 UsageTap 计 usage / 诊断。两层 tap 正交。
    let upstream = upstream.map(|r| r.map_err(|e| {
        warn!("透传流读取错误: {}", e);
        std::io::Error::other(e.to_string())
    }));
    let forwarded: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        Box::pin(CoerceSseTap {
            inner: upstream,
            feed_buf: Vec::new(),
            carry: String::new(),
        });
    let tapped: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        Box::pin(UsageTap {
            inner: forwarded,
            metrics: self.metrics.clone(),
            carry: String::new(),
            feed_buf: Vec::new(),
            usage: None,
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status: Some(status.as_u16()),
            client_req_id: client_req_id.to_string(),
        });

        let mut response_builder = warp::http::Response::builder().status(status);
        apply_forwarded_response_headers(&mut response_builder, resp_headers);

        // 当已获取了按模型并发槽时，把 guard 移入 body 流，使该槽在客户端排空完
        // body 之时（NVIDIA worker 在流结束时释放）才归还、而非 `handle_request`
        // 返回之时——后者可能只是响应 headers 到达、远早于 token 生成完成。
        let body = if let Some(guard) = concurrency_guard {
            warp::hyper::Body::wrap_stream(Box::pin(GuardedStream {
                inner: tapped,
                _guard: guard,
            }))
        } else {
            warp::hyper::Body::wrap_stream(tapped)
        };
        Ok(response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        }))
    }

    /// 尝试在未缓冲整段上游的情况下改写空 `choices` 成功 body——一旦我们确定
    /// 它不是空 choices 情形，热点成功路径仍保持零所有权。
    ///
    /// 策略：把流式 body 最多读到 [`EMPTY_CHOICES_PEEK_BYTES`] 字节进缓冲。每读到
    /// 一个 chunk 后尝试把*整个*缓冲作为 JSON 对象解析：
    ///   * 解析成功、且顶层 `choices` 数组为**空** → 这就是 cc-switch 422 触发源；
    ///     合成一个单占位 `choice`（空 assistant 消息、`finish_reason: "stop"`）并保留
    ///     `id` / `model` / `usage`，返回缓冲 JSON 响应。带空 `choices` 的 body 总是
    ///     很小（无生成），故截断不可能藏起一个。
    ///   * 解析成功、且 `choices` 非空（或作为键缺失而非空数组）→ 正常完成：停止
    ///     peek、落入下方流式透传，并把已读字节 prepend。
    ///   * 尚未构成完整 JSON 对象 → 继续读，直到要么解析成功、要么撞到 peek 上限、
    ///     要么看到非 UTF8 / 明显非 JSON 字节。
    ///
    /// 对任何不可解析 / 超上限 / 合成被禁用情形，我们把缓冲前缀与剩余上游流拼接转
    /// 发，使客户端看到与 `handle_passthrough_stream`完全一致的字节。
    async fn handle_success_maybe_synthesize(
        &self,
        resp: reqwest::Response,
        resp_headers: Vec<(String, Vec<u8>)>,
        status: warp::http::StatusCode,
        client_req_id: &str,
        concurrency_guard: &mut Option<ConcurrencyGuard>,
    ) -> Result<SuccessOutcome, Rejection> {
        let mut upstream = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();

        while let Some(chunk_res) = upstream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(e) => {
                    warn!(client_req_id = %client_req_id, "成功 peek 读取错误: {}", e);
                    // 转发现有内容再加一个错误尾；客户端会看到一条截断流，
                    // 与透传一样。
                    break;
                }
            };
            buf.extend_from_slice(&chunk);
            // 仅当缓冲是合法 UTF-8 时才尝试 JSON 解析。一个 gzipped / 二进制
            // body 在此失败并直接落到透传。
            let candidate = match std::str::from_utf8(&buf) {
                Ok(s) => s,
                Err(_) => break,
            };
            let parsed = serde_json::from_str::<serde_json::Value>(candidate);
            if let Ok(v) = &parsed {
                if let Some(arr) = v.get("choices").and_then(|c| c.as_array()) {
                    if arr.is_empty() {
                        // 空 choices。两种不同上游形态都落在这里：
                        //  (a) 一个*合法的*压缩空 choices body——模型跑了但产出 0 token，
                        //      仍带一个真实 `usage` 对象。合成占位 choice 使 cc-switch 不
                        //      422。
                        //  (b) NVIDIA 的空壳软失败——`usage:null`、`id:"` 因为没有推理跑
                        //      过（常是 `max_tokens` 对剩余上下文过大的 `/compact`）。在此
                        //      合成一个空内容 choice 就是让 Claude Code 的 `/compact` 打印
                        //      "summarization produced empty response" 的元凶。所以改为把
                        //      空壳交给调用方，它会用 clamp 后的 `max_tokens` 重试一次或外抛
                        //      502——绝不给一个误导性空 body。
                        if is_empty_shell_failure(v) {
                            return Ok(SuccessOutcome::ShellFailure { body: v.clone() });
                        }
                        // 确认是 cc-switch 422 触发源（形态 (a)）。合成并交付一个正常
                        //（缓冲）JSON body——上游已结束，无更多流可转发。
                        let resp = self.respond_synthesized_empty_choices(
                            v,
                            resp_headers,
                            client_req_id,
                        )?;
                        return Ok(SuccessOutcome::Response(resp));
                    }
                    // 非空 choices：普通完成。停止 peek，把已读前缀 + 剩余部分透传。
                    break;
                }
                // 解析出 JSON 但无 `choices` 键：非我们的情形（按上游契约 2xx
                // ChatCompletions body 总有该键），故落入流式。把前缀切片掉没必要——
                // 我们转发读到的确切字节。
                break;
            }
            if buf.len() >= EMPTY_CHOICES_PEEK_BYTES {
                // 真正大的非流式 body（除非 JSON 巨大否则非压缩所致）：不再继续缓冲，
                // 流式透传。空 choices body 不可能这么大。
                break;
            }
        }

        // 落入透传：把缓冲前缀 prepend 到剩余上游流。这与 `handle_passthrough_stream`
        // 保持逐字节一致（未发生合成），*包括*把前缀也做强转——否则一个落在 peek
        // 前缀内的 `data: {"error":{"code":400}}` 事件会在此被未 coerce 转发，而纯
        // 透传路径会 coerce 每行。流的前几个 chunk 恰是 NVIDIA 易于外抛错误事件之处，
        // 跳过强转会重新引入该 coerce 存在所防的数值 `code` 422。
        //
        // 把前缀作为 CoerceSseTap 的首个 chunk 喂入，从而让「前缀尾残行 ↔ rest 首
        // chunk 头」的跨边界 `data:` 行也被重拼强转——这正是 S4 修复点。统一走 tap
        // 后，纯透传路径与 peek 落入路径的强转语义完全一致。
        let prefix = Bytes::from(buf);
        let req_id_owned = client_req_id.to_string();
        let rest_mapped = upstream.map(move |r| r.map_err(|e| {
            warn!(client_req_id = %req_id_owned, "透传流读取错误: {}", e);
            std::io::Error::other(e.to_string())
        }));
        let prefixed: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> = {
            let head = futures_util::stream::iter(vec![Ok(prefix)]);
            Box::pin(head.chain(rest_mapped))
        };
        let pref: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(CoerceSseTap {
                inner: prefixed,
                feed_buf: Vec::new(),
                carry: String::new(),
            });

        let tapped: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(UsageTap {
                inner: pref,
                metrics: self.metrics.clone(),
                carry: String::new(),
                feed_buf: Vec::new(),
                usage: None,
                saw_choices: false,
                saw_empty_choices: false,
                saw_any_json: false,
                status: Some(status.as_u16()),
                client_req_id: client_req_id.to_string(),
            });

        let mut response_builder = warp::http::Response::builder().status(status);
        apply_forwarded_response_headers(&mut response_builder, resp_headers);
        // 响应现为已知长度、带缓冲前缀的流；我们略去 Content-Length（用 chunked）
        // 以保持字节忠实、而无需为前缀 + 动态剩余重算它。
        //
        // 把并发 guard 移入 body 流以覆盖流式 fallthrough（上方）——上游 worker 仍在
        // 生成。合成缓冲路径（更早返回）不取该 guard；调用方帧持有它并在返回时 drop，
        // 这没问题因该 body 已全缓冲（无更多上游工作）。
        let body = if let Some(guard) = concurrency_guard.take() {
            warp::hyper::Body::wrap_stream(Box::pin(GuardedStream {
                inner: tapped,
                _guard: guard,
            }))
        } else {
            warp::hyper::Body::wrap_stream(tapped)
        };
        let resp = response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        });
        Ok(SuccessOutcome::Response(resp))
    }

    /// 处置 NVIDIA 空壳软失败（`is_empty_shell_failure`）。完整理由见
    /// `Config::retry_on_empty_shell`。每空壳重试上限为
    /// `empty_shell_retry_attempts` 次额外尝试：若所有重试都空壳、或重试被禁用，我们
    /// 外抛一个携带真实原因的 502，使 Claude Code 的 `/compact` 不再打印误导性的
    /// "summarization produced empty response"，而是显示一个可操作的代理/网关错误。
    #[allow(clippy::too_many_arguments)]
    async fn handle_empty_shell_failure(
        &self,
        shell_body: serde_json::Value,
        method: &warp::http::Method,
        url: &str,
        headers: &warp::http::HeaderMap,
        request_body: &bytes::Bytes,
        model: &str,
        key_str: &str,
        body_summary: &str,
        client_req_id: &str,
        retry_attempt_already_done: u32,
    ) -> Result<AttemptResponse, AttemptError> {
        // 把重试限制在 empty_shell_retry_attempts 次额外尝试内，且仅当调用方仍有
        // 重试预算（尝试计数尚未耗尽 max_retries）时。
        // `retry_attempt_already_done` 是*产生*本空壳的尝试索引；首次重试是
        // attempt+1。
        let can_retry = self.retry_on_empty_shell
            && self.empty_shell_retry_attempts > 0
            && retry_attempt_already_done < self.empty_shell_retry_attempts
            && retry_attempt_already_done + 1 < self.retry_config.max_retries;

        // 长上下文短路：当请求估算 prompt token 达到或超过
        // `empty_shell_no_retry_prompt_tokens` 时，跳过 clamp 重试、立即外抛 502。
        // failure.log 显示在 107k+ prompt 上 clamp 重试恢复率为 0%
        //（49/49 再空壳）：那里的空壳意味着 `prompt + any max_tokens` 已超过模型
        // 上下文窗口，故缩小输出预算无济于事——重试只烧一次上游往返与一个 key 槽。
        // 我们只对*首次*空壳短路（retry_attempt_already_done == 0）：日后以 >0
        // attempt 到来的递归重试已承诺要重试。
        let mut prompt_too_large = false;
        if can_retry
            && retry_attempt_already_done == 0
            && self.empty_shell_no_retry_prompt_tokens > 0
        {
            if let Some(est) = estimate_prompt_tokens(request_body, &self.est_costs) {
                if est >= self.empty_shell_no_retry_prompt_tokens {
                    prompt_too_large = true;
                    warn!(
                        client_req_id = %client_req_id,
                        est_prompt_tokens = est,
                        threshold = self.empty_shell_no_retry_prompt_tokens,
                        "跳过空壳重试：prompt 超过不重试阈值"
                    );
                }
            }
        }
        let can_retry = can_retry && !prompt_too_large;

        // 预 clamp 应用于首次上游调用（在 `forward_upstream` 中），不在此处的重试
        // 上。重试为何用 `empty_shell_retry_max_tokens` 见下方重试预算注释。

        // 可选：重试前探上游健康，避免重试到降级端点。
        if can_retry && self.empty_shell_probe_before_retry {
            let probe_ok = self.probe_upstream_health().await;
            if !probe_ok {
                warn!(
                    client_req_id = %client_req_id,
                    "空壳重试前上游健康探测失败；外抛 502"
                );
                write_empty_shell_failure_log(
                    &shell_body,
                    request_body,
                    client_req_id,
                    model,
                    key_str,
                    url,
                    body_summary,
                    "empty_choices_shell_first",
                    retry_attempt_already_done,
                    Some("empty_shell_probe_failed"),
                    None,
                );
                return Ok(AttemptResponse::Done(empty_shell_bad_gateway(
                    request_body,
                    &shell_body,
                    client_req_id,
                    Some((
                        self.empty_shell_retry_max_tokens,
                        retry_attempt_already_done + 1,
                        "retry_skipped_probe_failed",
                    )),
                )));
            }
        }

        // 把该空壳记入 failure.log，含完整请求+响应证据——无论是否重试。这是
        // 为 jq 诊断浮现根因的路径。我们在决定 `can_retry` 之*后*记录，使
        // `retry_reason` 反映实际后续的处置。
        let req_fp = request_body_fingerprint(request_body);
        let shell_fp = shell_body_fingerprint(&shell_body);
        warn!(
            client_req_id = %client_req_id,
            attempt = retry_attempt_already_done,
            will_retry = can_retry,
            retry_max_tokens = if can_retry { Some(self.empty_shell_retry_max_tokens) } else { None },
            shell = %shell_fp,
            req = %req_fp,
            "NVIDIA 空壳软失败（200、usage:null、无推理）"
        );
        write_empty_shell_failure_log(
            &shell_body,
            request_body,
            client_req_id,
            model,
            key_str,
            url,
            body_summary,
            "empty_choices_shell_first",
            retry_attempt_already_done,
            if can_retry {
                Some("empty_shell_clamped_retry")
            } else if prompt_too_large {
                Some("empty_shell_no_retry_prompt_too_large")
            } else {
                Some("empty_shell_no_retry")
            },
            None,
        );

        // 跟踪空壳响应的断路器（若配置则按模型）
        if self.circuit_breaker_threshold > 0 {
            let cb_model = if self.circuit_breaker_per_model { model } else { "global" };
            self.key_pool.increment_circuit_breaker(key_str, cb_model, self.circuit_breaker_threshold).await;
            if self.key_pool.check_circuit_breaker(key_str, cb_model, self.circuit_breaker_threshold).await {
                warn!(
                    key = %mask_key(key_str),
                    model = %model,
                    threshold = self.circuit_breaker_threshold,
                    "断路器触发：连续过多空壳响应；冷却 key"
                );
                self.key_pool.mark_bad(key_str, None).await;
                self.metrics.inc_circuit_breaker_triggered();
            }
        }

        // 空壳指标
        let outcome = if can_retry { "first_shell" } else { "502" };
        self.metrics.inc_empty_shell_total(self.metrics_model(model), outcome);

        if can_retry {
            // 重试之间指数退避
            if retry_attempt_already_done > 0 {
                let backoff = self.empty_shell_retry_backoff_base_ms
                    .saturating_mul(1u64 << retry_attempt_already_done.min(10))
                    .min(self.empty_shell_retry_backoff_max_ms);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }

            // 重试尝试指标
            self.metrics.inc_empty_shell_retry_attempts_total(self.metrics_model(model));

            // 决定本次重试的 max_tokens。首次上游调用已应用预 clamp（见
            // `forward_upstream`），故当它仍空壳时每次重试都直降到已配置的
            // `empty_shell_retry_max_tokens`（8192）——再应用同一预 clamp 上限只会复现
            // 已失败的空壳。当该模型未配置预 clamp 时原始 max_tokens 是客户端所发，
            // 故这首次 clamp 才引入预算缩减。
            let retry_max_tokens = self.empty_shell_retry_max_tokens;

            let clamped = clamp_max_tokens(request_body, retry_max_tokens);
            let retry_body: bytes::Bytes = clamped.into();
            warn!(
                client_req_id = %client_req_id,
                attempt = retry_attempt_already_done + 1,
                max_tokens = retry_max_tokens,
                req = %request_body_fingerprint(&retry_body),
                "空壳软失败；以 clamp 后的 max_tokens 重试",
            );
            // S3：空壳重试前从该 key 的限流桶消费一个令牌，使重试不绕过
            // per-key QPS。此前该路径直接 `send()`，从不消费令牌——一个被
            // 上游限流的 key 能借空壳重试超出其配额、加剧 429。消费失败（桶
            // 空且 token_wait_ms 内未补上）说明该 key 已撞限流天花板，继续打
            // 它只会再收 429——跳过本次重试，直接外抛 502（fallback 与 budget
            // 耗尽的既有分支一致）。
            let got_token = self
                .key_pool
                .try_consume_token_for_key(key_str, self.token_wait_ms)
                .await;
            if !got_token {
                warn!(
                    client_req_id = %client_req_id,
                    key = %mask_key(key_str),
                    "空壳重试前 key 令牌桶空（限流天花板）；跳过重试、外抛 502"
                );
                self.metrics.inc_error();
                return Err(AttemptError::Fatal(warp::reject::custom(BadGateway {
                    msg: "Upstream empty-shell failure; key rate-limited, retry skipped"
                        .into(),
                })));
            }
            let rebuild = self.build_upstream_request(
                url,
                method,
                headers,
                &retry_body,
                key_str,
                false, // shell responses are non-streaming compaction requests
                true,  // retry_body is the JSON we re-serialized above + clamped
            );
            match rebuild.send().await {
                Ok(resp) => {
                    let resp_status = resp.status();
                    let content_type = resp
                        .headers()
                        .get(req_header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let resp_headers: Vec<(String, Vec<u8>)> = resp
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
                        .collect();
                    debug!(
                        client_req_id = %client_req_id,
                        attempt = retry_attempt_already_done + 1,
                        status = %resp_status,
                        content_type = %content_type,
                        url = %url,
                        "空壳重试上游响应"
                    );
                    if resp_status.is_success() && self.synthesize_empty_choices {
                        // 并发 guard 由 `handle_request` 帧所有；我们不把它穿进空壳
                        // 重试递归（后者可能跨 pinned future 自借）。空壳失败重试指向
                        // 同一模型、`max_tokens` 有界，故 worker 很快完成；若该 body 流
                        // 式产出，槽在 `handle_request` 返回时释放、而非流末——一个已知
                        // 轻微放宽，仅影响带已配置按模型上限的流式空壳重试（罕见：空壳是
                        // GLM 行为、而该上限是为 nemotron-3-ultra 设的）。
                        let mut no_guard: Option<ConcurrencyGuard> = None;
                        let outcome = self
                            .handle_success_maybe_synthesize(
                                resp,
                                resp_headers,
                                resp_status,
                                client_req_id,
                                &mut no_guard,
                            )
                            .await
                            .map_err(AttemptError::Fatal)?;
                        return Ok(match outcome {
                            SuccessOutcome::Response(resp) => {
                                // 空壳重试成功：窄恢复——只清本 model 的熔断计数，
                                // 不清其它 model 的（S4）。若用 `mark_good` 会清掉
                                // 该 key 在别的 model 上正在累积的 CB 计数，使那条
                                // per-(key,model) 断路器永远触不到阈值。
                                self.key_pool
                                    .mark_good_for_empty_shell_retry(key_str, model)
                                    .await;
                                // 记录空壳重试成功指标
                                self.metrics.inc_empty_shell_total(self.metrics_model(model), "retry_ok");
                                AttemptResponse::Done(resp)
                            }
                            // 重试仍空壳——或许再重试、或许外抛 502。
                            SuccessOutcome::ShellFailure { body } => {
                                let _retry_shell_fp = shell_body_fingerprint(&body);
                                let next_attempt = retry_attempt_already_done + 1;
                                // 若还有更多空壳重试次数则递归。否则以
                                // "retry_also_empty_shell" 外抛 502。
                                if next_attempt < self.empty_shell_retry_attempts
                                    && next_attempt + 1 < self.retry_config.max_retries
                                {
                                    return Box::pin(self.handle_empty_shell_failure(
                                        body, method, url, headers, &retry_body,
                                        model, key_str, body_summary, client_req_id,
                                        next_attempt,
                                    )).await;
                                }
                                // 最后一次重试仍空壳——以正确 outcome 外抛 502。
                                write_empty_shell_failure_log(
                                    &body,
                                    &retry_body,
                                    client_req_id,
                                    model,
                                    key_str,
                                    url,
                                    body_summary,
                                    "empty_choices_shell_retry",
                                    next_attempt,
                                    Some("empty_shell_retry_exhausted"),
                                    None,
                                );
                                // 记录最终空壳指标
                                self.metrics.inc_empty_shell_total(self.metrics_model(model), "retry_shell");
                                return Ok(AttemptResponse::Done(empty_shell_bad_gateway(
                                    request_body,  // 用于 fingerprint 的原始请求 body
                                    &body,         // 重试空壳 body
                                    client_req_id,
                                    Some((
                                        self.empty_shell_retry_max_tokens,
                                        next_attempt,
                                        "retry_also_empty_shell",
                                    )),
                                )));
                            }
                        });
                    }
                    // 非 2xx 重试或合成被禁用：外抛 502。
                    return Ok(AttemptResponse::Done(empty_shell_bad_gateway(
                        request_body,
                        &shell_body,
                        client_req_id,
                        Some((
                            self.empty_shell_retry_max_tokens,
                            retry_attempt_already_done + 1,
                            "retry_non_2xx_or_passthrough",
                        )),
                    )));
                }
                Err(e) => {
                    warn!(client_req_id = %client_req_id, "空壳重试发送错误: {}", e);
                    // 重试上网络错误：或许再重试、或许外抛 502。
                    let next_attempt = retry_attempt_already_done + 1;
                    if next_attempt < self.empty_shell_retry_attempts
                        && next_attempt + 1 < self.retry_config.max_retries
                    {
                        return Box::pin(self.handle_empty_shell_failure(
                            shell_body, method, url, headers, request_body,
                            model, key_str, body_summary, client_req_id,
                            next_attempt,
                        )).await;
                    }
                    // 最后一次重试或无更多预算：外抛 502。
                    return Ok(AttemptResponse::Done(empty_shell_bad_gateway(
                        request_body,
                        &shell_body,
                        client_req_id,
                        Some((
                            self.empty_shell_retry_max_tokens,
                            next_attempt,
                            "retry_network_error",
                        )),
                    )));
                }
            }
        }

        // 无重试预算 / 重试被禁用：以真实原因外抛 502。`prompt_too_large` 已记入
        // failure.log（retry_reason = `empty_shell_no_retry_prompt_too_large`）及上方
        // warn!；HTTP body 的 `retry` 字段保留为 not-attempted，因未运行重试。
        warn!(
            client_req_id = %client_req_id,
            "空壳软失败；外抛 502（重试被禁用或预算耗尽）",
        );
        Ok(AttemptResponse::Done(empty_shell_bad_gateway(
            request_body,
            &shell_body,
            client_req_id,
            None,
        )))
    }

    /// 对上游 /v1/models 的轻量健康探测。上游在超时内以 2xx 响应则返回 true。
    async fn probe_upstream_health(&self) -> bool {
        let probe_url = format!("{}/v1/models", self.base_url);
        let timeout = Duration::from_secs(5);
        match tokio::time::timeout(timeout, self.client.get(&probe_url).send()).await {
            Ok(Ok(resp)) => resp.status().is_success(),
            Ok(Err(_)) | Err(_) => false,
        }
    }

    /// 构造并返回合成响应：取一个带空 `choices` 数组的解析 body，放入一个空
    /// assistant 消息 choice，保留 `id` / `model` / `usage` 及任何兄弟字段，重新
    /// 序列化，作为带订正 Content-Length 的缓冲 JSON 响应交出。
    fn respond_synthesized_empty_choices(
        &self,
        body: &serde_json::Value,
        resp_headers: Vec<(String, Vec<u8>)>,
        client_req_id: &str,
    ) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
        warn!(
            client_req_id = %client_req_id,
            "为空 choices 2xx body 合成占位 choice \
             （避免下游 \"Empty choices array\" 422）",
        );

        let mut obj = match body {
            serde_json::Value::Object(m) => m.clone(),
            // 调用方仅在 `body.choices` 解析为空数组后到达此处，这要求 JSON 对象；
            // 其它任何情况都是 parser guard 的 bug，故安全失败到原始透传路径。
            _ => return self.handle_passthrough_empty_synthesize_fallback(),
        };
        obj.insert("choices".to_string(), serde_json::json!([{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": ""
            },
            "finish_reason": "stop"
        }]));
        let out = serde_json::to_vec(&serde_json::Value::Object(obj))
            .unwrap_or_else(|_| b"{}".to_vec());

        let mut response_builder = warp::http::Response::builder().status(
            warp::http::StatusCode::OK,
        );
        apply_forwarded_response_headers(&mut response_builder, resp_headers);
        // 丢弃上游 Content-Length / transfer-encoding，设我们自己的，使客户端确切知道
        // 合成 body 在何处结束。
        if let Some(headers) = response_builder.headers_mut() {
            headers.remove("content-length");
            headers.insert(
                warp::http::header::CONTENT_LENGTH,
                warp::http::HeaderValue::from(out.len()),
            );
            headers.insert(
                warp::http::header::CONTENT_TYPE,
                warp::http::HeaderValue::from_static("application/json"),
            );
        }
        Ok(response_builder
            .body(warp::hyper::Body::from(out))
            .unwrap_or_else(|_| {
                warp::http::Response::builder()
                    .status(warp::http::StatusCode::OK)
                    .body(warp::hyper::Body::empty())
                    .unwrap()
            }))
    }

    /// 在 object guard 下实践中不可达；提供以便合成 helper 能返回*某种*响应而非
    /// `unwrap`。
    fn handle_passthrough_empty_synthesize_fallback(
        &self,
    ) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
        let mut response_builder =
            warp::http::Response::builder().status(warp::http::StatusCode::OK);
        if let Some(headers) = response_builder.headers_mut() {
            headers.insert(
                warp::http::header::CONTENT_TYPE,
                warp::http::HeaderValue::from_static("application/json"),
            );
        }
        Ok(response_builder
            .body(warp::hyper::Body::from(b"{}" as &[u8]))
            .unwrap_or_else(|_| {
                warp::http::Response::builder()
                    .status(warp::http::StatusCode::OK)
                    .body(warp::hyper::Body::empty())
                    .unwrap()
            }))
    }
}

// 自定义 rejection
#[derive(Debug, Clone)]
struct ServiceUnavailable {
    msg: String,
}
#[derive(Debug, Clone)]
struct BadGateway {
    msg: String,
}

impl warp::reject::Reject for ServiceUnavailable {}
impl warp::reject::Reject for BadGateway {}

impl Reply for ServiceUnavailable {
    fn into_response(self) -> warp::reply::Response {
        warp::reply::with_status(self.msg, warp::http::StatusCode::SERVICE_UNAVAILABLE)
            .into_response()
    }
}
impl Reply for BadGateway {
    fn into_response(self) -> warp::reply::Response {
        warp::reply::with_status(self.msg, warp::http::StatusCode::BAD_GATEWAY).into_response()
    }
}
// NOTE：此处曾有一个用于硬 400 校验路径（`max_tokens` / `reasoning_effort`）的
// `BadRequest` reject 类型。这些路径在 2026-07 改为原地 body 规范化，故 handler 不再
// 发射该 reject 类型。如将来又需硬 400 情形，可重新引入
// `struct BadRequest { msg: String }`，并实现 `impl Reject` + `impl Reply`
//（映射到 BAD_REQUEST）。

#[cfg(test)]
mod proxy_integration_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use warp::{Filter, Rejection, Reply};
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use crate::config::{
        AuthConfig, Config, CooldownConfig, HealthCheckConfig, ObservabilityConfig,
        RateLimiterConfig, RetryConfig,
    };
    use crate::key_pool::KeyPool;
    use crate::metrics::Metrics;
    use crate::proxy::Proxy;
    use crate::proxy::InFlightGuard;
use crate::config::default_empty_shell_pre_clamp_max_tokens;
use crate::config::default_request_max_prompt_tokens;

    // 进程级 guard：置一次，使本模块无任何集成测试写入用于生产诊断的磁盘
    // failure.log。
    static DISABLE_FAILURE_LOG: std::sync::Once = std::sync::Once::new();

    /// 回归：`InFlightGuard` 被 drop 而未显式 `release()` 时必须正好 `dec_in_flight`
    /// 一次——这是 cancel-drop 路径（client 断连时 warp 在 `await` 中途 drop 掉
    /// handler future，于是没任何 `match result` 分支会跑显式 `release()`）。
    /// 加 guard 之前，每条取消的请求漏 +1，永久压低该 key 的 acquire 分
    ///（`in_flight * IN_FLIGHT_PENALTY`）。这里 acquire（inc）、包进 guard、
    /// *不 release 直接 drop*，断言计数器回到 0。
    #[tokio::test]
    async fn in_flight_guard_drops_dec_on_unreleased_drop() {
        let cfg = test_config("https://example.test", 3, 60);
        let metrics = Arc::new(Metrics::new());
        let pool = Arc::new(KeyPool::new(&cfg, metrics));
        // acquire 把 `in_flight` 提到 1（acquire_excluding 行里的 `inc_in_flight`）。
        let handle = pool.acquire().await.expect("active key must be available");
        let h_probe = handle.clone();
        assert_eq!(
            h_probe.in_flight(),
            1,
            "acquire must bump in_flight to 1"
        );
        // 把 acquire 到的 handle 包进 guard。不调 release() 直接 drop——
        // 模拟 client 取消时把 future 在 await 中途 drop。
        drop(InFlightGuard::new(handle));
        assert_eq!(
            h_probe.in_flight(),
            0,
            "InFlightGuard Drop must dec in_flight to 0 (cancel-drop path); \
             found {} — would mean +1 leaked per cancelled request",
            h_probe.in_flight()
        );
    }

    /// `InFlightGuard` 幂等性：先 `release()` 再 `Drop` 必须正好减一次（不是
    /// 两次——会少计 `in_flight`，最终向 u32::MAX 翻卷）。且第二次 `release()`
    /// 是 no-op。
    #[tokio::test]
    async fn in_flight_guard_release_then_drop_decrements_once() {
        let cfg = test_config("https://example.test", 3, 60);
        let metrics = Arc::new(Metrics::new());
        let pool = Arc::new(KeyPool::new(&cfg, metrics));
        for _ in 0..5 {
            let handle = pool.acquire().await.expect("active key available");
            let h_probe = handle.clone();
            assert_eq!(h_probe.in_flight(), 1);
            let mut guard = InFlightGuard::new(handle);
            guard.release(); // happy/retry 路径上的显式 release
            guard.release(); // 幂等——第二次不该再减
            assert_eq!(
                h_probe.in_flight(),
                0,
                "after release() in_flight should be 0; double-release must not undercount"
            );
            drop(guard); // 紧跟 release 的 Drop 必须是 no-op
            assert_eq!(
                h_probe.in_flight(),
                0,
                "Drop following release() must not further decrement; found {}",
                h_probe.in_flight()
            );
        }
        // sanity：N 次 acquire/release/drop 循环后计数器恰为 0，而非负/未翻卷
        //（翻卷会表现为极大的 in_flight）。
        let final_handle = pool.acquire().await.expect("still available");
        let final_probe = final_handle.clone();
        assert_eq!(
            final_probe.in_flight(),
            1,
            "after acquire again in_flight=1, no accumulation from idempotency test"
        );
        drop(InFlightGuard::new(final_handle));
        assert_eq!(final_probe.in_flight(), 0);
    }

    /// `InFlightGuard::swap`：切到一个新 handle 必须 release 旧的、adopt 新的
    ///（新的 acquire 已经撞过自己的计数器）。这兜住重试切换点
    ///（`key_guard.swap(next)`）。
    #[tokio::test]
    async fn in_flight_guard_swap_releases_old_and_adopts_new() {
        let cfg = test_config("https://example.test", 3, 60);
        let metrics = Arc::new(Metrics::new());
        let pool = Arc::new(KeyPool::new(&cfg, metrics));
        // 两个 handle 共享同一 key 的 in_flight Arc（都是 "fake-key" 那个
        // KeyStateHandle 的 clone）；每次 acquire +1。
        let h1 = pool.acquire().await.unwrap();
        let h1_probe = h1.clone();
        let h2 = pool.acquire().await.unwrap();
        let h2_probe = h2.clone();
        assert_eq!(h1_probe.in_flight(), 2, "two acquires → in_flight=2");
        let mut guard = InFlightGuard::new(h1); // guard 拥有第一个 inc
        // swap：release 旧的（in_flight 2→1），adopt 新的（其 inc 已算过）
        guard.swap(h2);
        assert_eq!(
            h1_probe.in_flight(),
            1,
            "swap must release the old handle (2→1)"
        );
        // 不显式 release 直接 Drop：Drop adopt 新 handle 的 dec
        drop(guard);
        assert_eq!(
            h1_probe.in_flight(),
            0,
            "Drop of the swapped-in guard must decrement the new handle → 0"
        );
        // 两个 probe 读的是同一个 Arc 计数器
        assert_eq!(h2_probe.in_flight(), 0);
    }

    fn test_config(base_url: &str, max_retries: u32, request_timeout: u64) -> Config {
        DISABLE_FAILURE_LOG.call_once(|| {
            // SAFETY：本模块的测试在单进程内运行；该环境变量是进程级
            // 全局的，仅用于守护 failure-log 写入。
            unsafe {
                std::env::set_var("FAILURE_LOG_DISABLE", "1");
            }
        });
        Config {
            listen_addr: "127.0.0.1:0".to_string(),
            base_url: base_url.to_string(),
            keys: vec!["fake-key".to_string()],
            rate_limiter: RateLimiterConfig {
                qps_per_key: 1000.0,
                burst: 1000,
                token_wait_ms: 0,
            },
            cooldown: CooldownConfig {
                base_seconds: 1,
                max_seconds: 2,
                scan_interval_seconds: 5,
            },
            health_check: HealthCheckConfig {
                interval_seconds: 30,
                timeout_seconds: 5,
                max_consecutive_failures: 3,
            },
            observability: ObservabilityConfig {
                enable_metrics: false,
                metrics_path: "/metrics".to_string(),
                log_level: "info".to_string(),
            },
            request_timeout_seconds: request_timeout,
            request_timeout_streaming_seconds: 0,
            retry: RetryConfig {
                max_retries,
                backoff_base_ms: 100,
                backoff_max_ms: 2000,
                auth_fail_threshold: 3,
                max_key_switches: 3,
            },
            model_aliases: std::collections::HashMap::new(),
            allow_local_base_url: false,
            auth: AuthConfig::default(),
            inject_stream_usage: true,
            synthesize_empty_choices: true,
            retry_on_empty_shell: true,
            empty_shell_retry_max_tokens: 8192,
            empty_shell_retry_attempts: 1,
            empty_shell_probe_before_retry: false,  // Disable probe for tests
            circuit_breaker_threshold: 3,
            circuit_breaker_per_model: true,
            retry_on_503: true,
            probe_before_503_retry: false,  // Disable probe for tests
            retry_on_500: true,
            empty_shell_retry_backoff_base_ms: 500,
            empty_shell_retry_backoff_max_ms: 5000,
            empty_shell_pre_clamp_max_tokens: default_empty_shell_pre_clamp_max_tokens(),
            empty_shell_no_retry_prompt_tokens: 90000,
            request_max_prompt_tokens: default_request_max_prompt_tokens(),
            est_image_token_cost: 1500,
            est_audio_token_per_kb: 16,
            est_audio_token_cap: 10000,
            est_unknown_part_token_cap: 2000,
            max_tokens_max: 100_000,
            concurrency_limit_per_model: std::collections::HashMap::new(),
            concurrency_queue_timeout_ms: 5000,
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
        }
    }

    async fn start_proxy(config: Config) -> SocketAddr {
        let metrics = Arc::new(Metrics::new());
        let key_pool = Arc::new(KeyPool::new(&config, metrics.clone()));
        let proxy = Proxy::new(
            config.base_url.clone(),
            key_pool,
            metrics,
            config.request_timeout_seconds,
            config.request_timeout_streaming_seconds,
            config.retry.clone(),
            config.rate_limiter.token_wait_ms,
            config.model_aliases.clone(),
            config.inject_stream_usage,
            config.synthesize_empty_choices,
            config.retry_on_empty_shell,
            config.empty_shell_retry_max_tokens,
            config.empty_shell_retry_attempts,
            config.empty_shell_probe_before_retry,
            config.empty_shell_retry_backoff_base_ms,
            config.empty_shell_retry_backoff_max_ms,
            config.empty_shell_pre_clamp_max_tokens.clone(),
            config.empty_shell_no_retry_prompt_tokens,
            config.request_max_prompt_tokens,
            crate::proxy::EstPromptCosts {
                image_cost: config.est_image_token_cost,
                audio_token_per_kb: config.est_audio_token_per_kb,
                audio_token_cap: config.est_audio_token_cap,
                unknown_part_cap: config.est_unknown_part_token_cap,
            },
            config.circuit_breaker_threshold,
            config.circuit_breaker_per_model,
            config.retry_on_503,
            config.probe_before_503_retry,
            config.retry_on_500,
            config.max_tokens_max,
            config.concurrency_limit_per_model.clone(),
            config.concurrency_queue_timeout_ms,
            config.upstream_connect_timeout_secs,
            config.upstream_pool_idle_timeout_secs,
            config.retry_jitter_ms,
        );

        let proxy_route = warp::path::full()
            .map(|p: warp::path::FullPath| p.as_str().to_string())
            .and(warp::method())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .and(warp::addr::remote())
            .and_then({
                let proxy = proxy.clone();
                move |path: String, method, headers, body, remote| {
                    let proxy = proxy.clone();
                    async move {
                        proxy
                            .handle_request(path, method, headers, body, remote)
                            .await
                    }
                }
            })
            .recover(recover_rejection);

        let (addr, server) =
            warp::serve(proxy_route).bind_ephemeral("127.0.0.1:0".parse::<SocketAddr>().unwrap());
        tokio::spawn(server);
        addr
    }

    #[tokio::test]
    async fn max_retries_zero_passes_401_through() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 401);
    }

    // ── 不支持的 API 形态拒绝（mod ①+②）────────────────────────────────

    #[tokio::test]
    async fn responses_endpoint_rejected_with_501() {
        let mock_server = MockServer::start().await;
        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/responses", addr))
            .body(r#"{"model":"m","input":"hi"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 501);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["type"], "not_supported");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("/v1/chat/completions"));
    }

    #[tokio::test]
    async fn messages_endpoint_rejected_with_501() {
        let mock_server = MockServer::start().await;
        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 501);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["type"], "not_supported");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("/v1/messages"));
    }

    // ── /v1/models 透传 + 缓存（mod ③）─────────────────────────────────

    #[tokio::test]
    async fn models_passthrough_returns_upstream_catalog_and_caches() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("GET"))
            .and(matchers::path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": "list",
                "data": [
                    {"id": "custom/model-a", "object": "model", "owned_by": "custom"}
                ]
            })))
            // 仅一次上游调用：第 2 个客户端请求必须命中缓存。
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();

        let resp1 = client
            .get(format!("http://{}/v1/models", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.status(), 200);
        let body1: serde_json::Value = resp1.json().await.unwrap();
        assert_eq!(body1["data"][0]["id"], "custom/model-a");

        let resp2 = client
            .get(format!("http://{}/v1/models", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.status(), 200);
        let body2: serde_json::Value = resp2.json().await.unwrap();
        assert_eq!(body2["data"][0]["id"], "custom/model-a");
    }

    // ── stream_options.include_usage 注入（mod ④）──────────────────────

    #[tokio::test]
    async fn stream_options_injected_for_streaming() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let _resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .json(&serde_json::json!({"model": "m", "messages": [], "stream": true}))
            .send()
            .await
            .unwrap();

        let requests = mock_server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[tokio::test]
    async fn stream_options_not_overridden_when_client_disables() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let _resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .json(&serde_json::json!({
                "model": "m",
                "messages": [],
                "stream": true,
                "stream_options": {"include_usage": false}
            }))
            .send()
            .await
            .unwrap();

        let requests = mock_server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["stream_options"]["include_usage"], false);
    }

    #[tokio::test]
    async fn max_retries_one_retries_once() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401))
            .expect(2)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 1, 5)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 401);
    }

    /// 上游返回 500 "Failed to generate completions" 时应被重试（当
    /// `retry_on_500` 打开时），并在重试返回正常 completion 后成功。镜像 401
    /// 重试测试，但断言的是 NVIDIA 偶发 GLM 500 的「重试后恢复」路径。
    #[tokio::test]
    async fn internal_500_retries_then_succeeds() {
        let mock_server = MockServer::start().await;
        let shell_seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shell_seen_c = shell_seen.clone();
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(move |_req: &wiremock::Request| {
                let n = shell_seen_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    // 第一次尝试：瞬时 500。
                    ResponseTemplate::new(500).set_body_json(serde_json::json!({
                        "message": "Failed to generate completions",
                        "type": "Internal Server Error",
                        "code": 500,
                    }))
                } else {
                    // 重试：正常的非空 completion。
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "id": "cc-500",
                        "object": "chat.completion",
                        "model": "z-ai/glm-5.2",
                        "created": 1,
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
                    }))
                }
            })
            .expect(2)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 3, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","messages":[{"role":"user","content":"hi"}],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            200,
            "500 should have been retried to a successful completion"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["id"].as_str(), Some("cc-500"));
        assert_eq!(
            shell_seen.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "exactly two upstream calls (initial 500 + retry 200)"
        );
    }

    /// 对处于 `empty_shell_pre_clamp_max_tokens` 中的模型发起流式请求，且
    /// `max_tokens` 达到或超过上限时，必须在**首次上游调用之前**就被钳住——
    /// 证明 pre-clamp 路径如今覆盖了流式，而不只是 post-shell 重试。我们断言
    /// 上游 body 携带的是钳住后的上限值（8192），而非客户端的 64000。`>=`
    /// 守卫在 `cur == target` 时同样钳住（见 failure.log：216/216 个 shell 都
    /// 带着 max_tokens == 配置的上限，旧的严格 `>` 漏掉了这种情况）。
    #[tokio::test]
    async fn pre_clamp_applies_to_streaming_first_call() {
        let mock_server = MockServer::start().await;
        let seen_mt = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let seen_mt_c = seen_mt.clone();
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(move |req: &wiremock::Request| {
                // 解析被投递的 body，记录 NVIDIA 实际看到的 max_tokens。
                let raw = std::str::from_utf8(&req.body).unwrap_or("");
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
                    if let Some(mt) = v.get("max_tokens").and_then(|x| x.as_u64()) {
                        seen_mt_c.lock().unwrap().push(mt);
                    }
                }
                // 一段正常的流式响应，让请求正常收尾。
                ResponseTemplate::new(200).insert_header("content-type", "text/event-stream")
                    .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
            })
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 3, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[{"role":"user","content":"hi"}],"stream":true}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        // 把 SSE 流排干，以释放上游 worker。
        use futures_util::StreamExt;
        let mut s = resp.bytes_stream();
        while let Some(_c) = s.next().await {}

        let observed = seen_mt.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![8192],
            "streaming first call max_tokens should be pre-clamped 64000 -> 8192, got {:?}",
            observed
        );
    }

    /// 长上下文旁路：当请求的估算 prompt token 达到或超过
    /// `empty_shell_no_retry_prompt_tokens`、且第一次上游调用空壳时，代理必须
    /// **不重试**——它立刻返回 502、outcome 为 `retry_skipped_prompt_too_large`、
    /// 只发一次上游调用。failure.log 显示 107k+ prompt 上做钳住重试恢复率为 0%；
    /// 这条旁路省掉那次注定浪费的往返。我们把阈值设得很小（2），发一条
    /// est_prompt_tokens（chars/4）已超过它的消息，让逻辑在没有 MB 级 body 的
    /// 情况下触发。
    #[tokio::test]
    async fn empty_shell_skips_retry_when_prompt_exceeds_no_retry_threshold() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(move |_req: &wiremock::Request| {
                ResponseTemplate::new(200).set_body_json(shell.clone())
            })
            .expect(1) // exactly ONE call — the bypass must suppress the retry
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        // 极小阈值 → 任何非平凡 prompt 都会触发 bypass
        cfg.empty_shell_no_retry_prompt_tokens = 2;
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        // 12 字符内容 → est_prompt_tokens = 12/4 = 3 ≥ 2（阈值）
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[{"role":"user","content":"hello world!"}],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            502,
            "long-context empty shell must surface 502 immediately, got: {:?}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["type"].as_str(), Some("upstream_empty_shell"));
        // 没有发生重试：body 必须把 retry 报为未尝试，且 retry-clamp
        // detail（`max_tokens=8192`）必须缺席。（上面那次单调用断言才是 bypass
        // 抑制了重试的真正证据。）
        assert_eq!(
            body["error"]["retry"]["attempted"].as_bool(),
            Some(false),
            "retry must be reported as not attempted, got: {body}"
        );
        assert!(
            !body["error"]["detail"].as_str().unwrap_or_default().contains("max_tokens=8192"),
            "retry-clamp detail must be absent when retry is skipped, got: {body}"
        );
    }

    /// 硬上限：当估算 prompt token 达到代理的 `request_max_prompt_tokens` 上限
    /// 时，请求在*任何上游调用之前*就被以 413 拒绝（不占 key 槽、不往返），
    /// 且 body 带 `type: request_too_large` + est/limit，让 client 可据此行动。
    /// failure.log 显示 107k+ prompt 一律空壳；413 把这事实前置暴露、要 client
    /// 自己压缩上下文。我们把上限设得很小（5），发一条 est_prompt_tokens
    ///（chars/4）已超过它的消息，让触发条件在没有 MB 级 body 的情况下命中。
    #[tokio::test]
    async fn oversized_prompt_request_rejected_with_413_before_upstream() {
        let mock_server = MockServer::start().await;
        // 期望零次上游调用——bypass 必须在 send() 之前触发。
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.request_max_prompt_tokens = 5; // tiny ceiling
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        // 24 字符内容 → est_prompt_tokens = 24/4 = 6 ≥ 5（上限）
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":32000,"messages":[{"role":"user","content":"twenty four chars here!!"}],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            413,
            "oversized prompt must be rejected with 413 Payload Too Large, got: {:?}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["error"]["type"].as_str(),
            Some("request_too_large"),
            "error type should classify as request_too_large, got: {body}"
        );
        assert_eq!(
            body["error"]["code"].as_str(),
            Some("prompt_tokens_exceeds_ceiling"),
            "error code should name the ceiling breach, got: {body}"
        );
        assert_eq!(
            body["error"]["limit"].as_u64(),
            Some(5),
            "body should expose the configured ceiling, got: {body}"
        );
        assert!(
            body["error"]["est_prompt_tokens"].as_u64().unwrap_or(0) >= 5,
            "body should expose the estimated prompt tokens at/above the ceiling, got: {body}"
        );
    }

    /// 多模态回归：带内联 base64 `image_url`（50_000 字符 payload）的请求不
    /// 应被 prompt 上限 413。在旧的 chars/4 启发式下，光 base64 估算就
    /// ~12_500 token；多模态成本把它收拢成 `image_cost`（默认 1500）。我们
    /// 把上限设成 3000，让旧启发式会拒（12500 ≥ 3000）而新的放过
    ///（1500 < 3000）——且恰好一次上游调用。
    #[tokio::test]
    async fn multimodal_inline_image_not_rejected_by_prompt_ceiling() {
        let mock_server = MockServer::start().await;
        // 恰好一次上游调用：请求通过 413 校验并被转发。
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"a cat"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1500,"completion_tokens":3,"total_tokens":1503}}"#,
                ),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.request_max_prompt_tokens = 3000; // old heuristic (12500) would 413; new (1500) passes
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let big_b64 = "A".repeat(50_000);
        let body = serde_json::json!({
            "model": "nvidia/neva-13b-vision-instruct",
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"text","text":"what is this?"},
                    {"type":"image_url","image_url":{"url": format!("data:image/png;base64,{big_b64}")}}
                ]
            }],
            "stream": false
        });
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            200,
            "multimodal request with inline base64 image must NOT be 413'd, got: {:?}",
            resp.status()
        );
    }

    /// 多模态守卫仍咬得住：即便带一张 base64 图片，若仅*文本*就超过上限，
    /// 仍要 413——图片成本的豁免不能变成放超长文本过去的洞。期望零次上游调用。
    #[tokio::test]
    async fn multimodal_with_long_text_still_rejected_by_prompt_ceiling() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0) // 413 fires before any upstream call
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.request_max_prompt_tokens = 3000;
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let big_b64 = "A".repeat(50_000);
        // 20_000 字符文本 → 仅文本就 5000 token，≥ 3000 上限。
        let long_text = "x".repeat(20_000);
        let body = serde_json::json!({
            "model": "nvidia/neva-13b-vision-instruct",
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"text","text": long_text},
                    {"type":"image_url","image_url":{"url": format!("data:image/png;base64,{big_b64}")}}
                ]
            }],
            "stream": false
        });
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            413,
            "multimodal request whose text exceeds the ceiling must still be 413'd, got: {:?}",
            resp.status()
        );
        let err: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            err["error"]["type"].as_str(),
            Some("request_too_large"),
            "should still classify as request_too_large, got: {err}"
        );
    }

    /// Body 规范化必须逐字保留多模态 `content` 数组：model alias /
    /// max_tokens / reasoning_effort 的改写只动顶层标量字段，绝不碰
    /// `messages[].content`。我们从 mock 把 body 原样回传，从而可以断言
    /// content 数组保持不变。
    #[tokio::test]
    async fn multimodal_content_array_preserved_through_normalization() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
                ),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        // 把客户端可见名别名为一个 NVIDIA id，使规范化路径（重写顶层字段）
        // 真正跑起来。
        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.model_aliases.insert(
            "gpt-4o".to_string(),
            "meta/llama-3.2-90b-vision-instruct".to_string(),
        );
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();

        let content_arr = serde_json::json!([
            {"type":"text","text":"what is this?"},
            {"type":"image_url","image_url":{"url":"https://example.com/cat.png","detail":"high"}},
            {"type":"input_audio","input_audio":{"data":"QkFTRTY0","format":"wav"}}
        ]);
        let body = serde_json::json!({
            "model": "gpt-4o",
            "max_tokens": 256,
            "messages": [{"role":"user","content": content_arr.clone()}]
        });
        let _resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();

        // 从 mock 的日志读出代理实际转发给上游的内容。
        let received = &mock_server.received_requests().await.expect("recorded")[0];
        let fwd: serde_json::Value = serde_json::from_slice(&received.body).unwrap();
        // 模型别名已被重写（证明规范化跑过）…
        assert_eq!(
            fwd["model"].as_str(),
            Some("meta/llama-3.2-90b-vision-instruct"),
            "alias should have been rewritten before forwarding, got: {fwd}"
        );
        // …但 content 数组与客户端所发逐字节一致。
        assert_eq!(
            fwd["messages"][0]["content"],
            content_arr,
            "multimodal content array must be preserved verbatim through normalization, got: {fwd}"
        );
    }

    #[tokio::test]
    async fn last_attempt_does_not_backoff() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401))
            .expect(2)
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 1, 5);
        cfg.retry.backoff_base_ms = 500;
        let addr = start_proxy(cfg).await;

        let client = reqwest::Client::new();
        let start = Instant::now();
        let _resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // 只有初次尝试 + 1 次重试，最后一次尝试前不退避。
        // 给一个宽裕的余量以避免 flaky。
        assert!(
            elapsed < Duration::from_millis(450),
            "expected no backoff before last attempt, elapsed {:?}",
            elapsed
        );
    }

    async fn recover_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
        if let Some(e) = err.find::<super::BadGateway>().cloned() {
            return Ok(e.into_response());
        }
        if let Some(e) = err.find::<super::ServiceUnavailable>().cloned() {
            return Ok(e.into_response());
        }
        Err(err)
    }

    #[tokio::test]
    async fn request_timeout_covers_response_delay() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
            .expect(1..)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 1)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 502);
    }

    /// 上游返回 503 且 `error.code` 为数字（NVIDIA 真实形态），非流式。
    /// 验证代理出口 body 里 `error.code` 被转成字符串。
    #[tokio::test]
    async fn numeric_error_code_coerced_non_streaming() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(503).set_body_json(serde_json::json!({
                    "error": {
                        "message": "ResourceExhausted: All workers are busy, please retry later",
                        "type": "Service Unavailable",
                        "code": 503
                    }
                })),
            )
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","stream":false,"messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        let code = body
            .get("error")
            .and_then(|e| e.get("code"))
            .expect("error.code present");
        assert!(
            code.is_string(),
            "error.code should be a string after coercion, got: {:?}",
            code
        );
        assert_eq!(code.as_str(), Some("503"));
    }

    /// 上游返回 503 + 数字 code，但客户端请求 `stream:true`。
    /// 上游错误不是 SSE 格式，代理走缓冲路径，coerce 必须生效。
    #[tokio::test]
    async fn numeric_error_code_coerced_streaming_non_sse_error() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(503)
                    .insert_header("content-type", "application/json")
                    .set_body_json(serde_json::json!({
                        "error": {
                            "message": "ResourceExhausted",
                            "type": "Service Unavailable",
                            "code": 503
                        }
                    })),
            )
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","stream":true,"messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        let code = body
            .get("error")
            .and_then(|e| e.get("code"))
            .expect("error.code present");
        assert!(
            code.is_string(),
            "streaming + non-SSE error: error.code should be string, got: {:?}",
            code
        );
        assert_eq!(code.as_str(), Some("503"));
    }

    /// 客户端请求 `stream:true`，上游返回 200 + SSE，但在流中途发 error event，
    /// error event 的 data 里 `error.code` 是数字。
    /// 这条路径走 `handle_passthrough_stream` → `coerce_sse_chunk`，
    /// 数字 `code` 必须被逐行 coerce 成字符串。
    #[test]
    fn coerce_sse_chunk_turns_numeric_code_into_string() {
        let sse_error = "data: {\"error\":{\"message\":\"x\",\"code\":400}}\n\n";
        let coerced = super::coerce_sse_chunk(
            &bytes::Bytes::from_static(sse_error.as_bytes()),
        );
        let coerced_str = std::str::from_utf8(&coerced).unwrap();
        assert!(
            coerced_str.contains("\"code\":\"400\""),
            "coerce_sse_chunk should coerce numeric code to string, got: {}",
            coerced_str
        );
        assert!(
            !coerced_str.contains("\"code\":400"),
            "no raw numeric code should remain, got: {}",
            coerced_str
        );
    }

    #[tokio::test]
    async fn coerce_sse_tap_reassembles_data_line_split_across_chunks() {
        // S4：一个数值 `error.code` 事件被拆到两个 chunk 边界上——前半截缺尾 `\n`，
        // 后半截补全事件尾。旧逐-chunk coerce_sse_chunk 会在每半失败 JSON 解析、
        // 原样透传 → 数值 code 漏过。CoerceSseTap 必须把整行重拼后再强转。
        let whole = "data: {\"error\":{\"message\":\"x\",\"code\":400}}\n\n".to_string();
        let half = whole.len() / 2;
        let (a, b) = whole.split_at(half);
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
            Ok(bytes::Bytes::from(a.to_string())),
            Ok(bytes::Bytes::from(b.to_string())),
        ];
        let stream = futures_util::stream::iter(chunks);
        let mut tap = super::CoerceSseTap {
            inner: stream,
            feed_buf: Vec::new(),
            carry: String::new(),
        };
        let mut collected = Vec::new();
        use futures_util::StreamExt;
        while let Some(r) = tap.next().await {
            collected.extend_from_slice(&r.unwrap());
        }
        let out = String::from_utf8(collected).unwrap();
        assert!(
            out.contains("\"code\":\"400\""),
            "split-chunk numeric code must be coerced to string, got: {}",
            out
        );
        assert!(
            !out.contains("\"code\":400"),
            "no raw numeric code should remain after reassembly, got: {}",
            out
        );
    }

    #[tokio::test]
    async fn coerce_sse_tap_preserves_bytes_when_no_coercion_applies() {
        // 非 `code` 的普通 delta chunk：CoerceSseTap 不得改动任何字节，且整段
        //（chunk 以 `\n\n` 结束）必须一次性 emit，无 hold-back。
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_string();
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> =
            vec![Ok(bytes::Bytes::from(body.clone()))];
        let stream = futures_util::stream::iter(chunks);
        let mut tap = super::CoerceSseTap {
            inner: stream,
            feed_buf: Vec::new(),
            carry: String::new(),
        };
        let mut collected = Vec::new();
        use futures_util::StreamExt;
        while let Some(r) = tap.next().await {
            collected.extend_from_slice(&r.unwrap());
        }
        assert_eq!(
            String::from_utf8(collected).unwrap(),
            body,
            "non-coerced SSE body must pass through byte-identical"
        );
    }

    #[tokio::test]
    async fn coerce_sse_tap_never_loses_trailing_fragment_on_stream_end() {
        // 一个无最终 `\n` 的尾残行（合法 SSE 不会这样，但防御）：流结束时
        // flush_trailing 必须把它放行，字节不丢。
        let body = "data: {\"error\":{\"message\":\"x\",\"code\":401}}".to_string(); // 无尾 \n\n
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> =
            vec![Ok(bytes::Bytes::from(body.clone()))];
        let stream = futures_util::stream::iter(chunks);
        let mut tap = super::CoerceSseTap {
            inner: stream,
            feed_buf: Vec::new(),
            carry: String::new(),
        };
        let mut collected = Vec::new();
        use futures_util::StreamExt;
        while let Some(r) = tap.next().await {
            collected.extend_from_slice(&r.unwrap());
        }
        let out = String::from_utf8(collected).unwrap();
        assert!(
            out.contains("\"code\":\"401\""),
            "trailing fragment must be flushed & coerced, got: {}",
            out
        );
    }

    #[tokio::test]
    async fn numeric_error_code_in_sse_stream_is_coerced() {
        let sse_error = "data: {\"error\":{\"message\":\"x\",\"code\":400}}\n\n";
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(sse_error, "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","stream":true,"messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("\"code\":\"400\""),
            "SSE error event should have numeric code coerced to string, got: {}",
            body
        );
        assert!(
            !body.contains("\"code\":400"),
            "no raw numeric code should remain in SSE output, got: {}",
            body
        );
    }

    /// 回归：`handle_success_maybe_synthesize` 的 fallthrough 路径把 peek
    /// 前缀缓冲进 `buf` 并把它 prepend 到流式剩余部分之前。修复前，该缓冲
    /// 前缀逐字发出、**不**经过 `coerce_sse_chunk`，于是落在前缀内的带数字
    /// `code` 的 `data:` error event 会原样到达 client——重新触发那个 coerce
    /// 本来就是为了防止的 cc-switch 422。纯 passthrough 路径
    /// （`handle_passthrough_stream`）会对每个 chunk coerce，所以两条路径
    /// 不一致。本测试通过发送一个以 error event（无 `choices`）开头、随后跟
    /// 非空 choices event 的 SSE body 来触发 synthesis fallthrough；第一个
    /// event 整个落在缓冲前缀之内。
    #[tokio::test]
    async fn numeric_error_code_in_fallthrough_prefix_is_coerced() {
        let sse_body = concat!(
            "data: {\"error\":{\"message\":\"x\",\"code\":400}}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );

        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(sse_body, "text/event-stream"),
            )
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","stream":true,"messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("\"code\":\"400\""),
            "error event in buffered prefix should have numeric code coerced to string, got: {}",
            body
        );
        assert!(
            !body.contains("\"code\":400"),
            "no raw numeric code should remain in fallthrough SSE output, got: {}",
            body
        );
        // fallthrough 仍必须转发同一缓冲前缀里紧随错误事件的 *非错误* SSE 事件——
        // 把 choices 事件合成掉会是 body 保真度上的回归。
        assert!(
            body.contains("\"choices\":[{\"delta\":{\"content\":\"hi\"}}]"),
            "non-error choices event after the error event must still be forwarded, got: {}",
            body
        );
    }

    /// 回归：per-model 并发上限查找必须以*客户端可见的* alias 源（X）
    /// 为键，而不仅是 remap 之后的 target（Y）。修复前，
    /// `concurrency_limit_per_model.get(&model)` 在 alias 改写之后按
    /// `model == target` 查找 → 以源为键的上限（`{ "client-a": 1 }`）
    /// 静默地从不命中，limiter 形同虚设，任由 upstream worker 超额。
    /// 这里挂一个慢（300ms）的 mock，并以 cap=1、50 ms 队列超时发起两个
    /// 并发 POST。若查找按源为键（正确），第二个请求排队 50 ms 并返回
    /// 503 `concurrency limit reached`；若只按 target 为键（bug），第二个
    /// 请求立即 acquire，两个都返回 200。
    ///
    /// 注：本测试对时序敏感（两个并发请求、300 ms mock 延迟、50 ms 队列
    /// 超时），但余量（mock 延迟是队列超时的 6 倍）足够宽，在 CI 上稳定
    /// 不 flaky。
    #[tokio::test]
    async fn concurrency_cap_keyed_by_alias_source_is_enforced() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::header("content-type", "application/json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(300))
                    .set_body_json(serde_json::json!({
                        "id": "x", "object": "chat.completion", "model": "glm-5.2",
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}]
                    })),
            )
            // 两个请求都匹配；我们不 pin 期望计数，因为其中一个会由
            // 代理自身作答（503）、根本不会到上游。
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 0, 60);
        cfg.concurrency_limit_per_model
            .insert("client-a".to_string(), 1);
        cfg.concurrency_queue_timeout_ms = 50;
        cfg.model_aliases
            .insert("client-a".to_string(), "glm-5.2".to_string());

        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let body = r#"{"model":"client-a","messages":[{"role":"user","content":"hi"}]}"#;
        // 并发发起两个请求。
        let (r1, r2) = tokio::join!(
            client
                .post(format!("http://{}/v1/chat/completions", addr))
                .header("content-type", "application/json")
                .body(body)
                .send(),
            client
                .post(format!("http://{}/v1/chat/completions", addr))
                .header("content-type", "application/json")
                .body(body)
                .send(),
        );
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();
        // 恰好一个必须成功（200），另一个必须撞到队列超时（503 带
        // 并发上限消息）。顺序是 racy 的，所以只断言状态的 *集合* 是 {200, 503}。
        let s1 = r1.status();
        let s2 = r2.status();
        let body2 = r2.text().await.unwrap_or_default();
        assert!(
            (s1 == 200 && s2 == 503) || (s1 == 503 && s2 == 200),
            "expected one 200 and one 503 (cap enforced via alias source key), got {} and {}; r2 body={:?}",
            s1, s2, body2
        );
    }

    #[tokio::test]
    async fn rewritten_json_body_forces_application_json_content_type_upstream() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}]
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            // 故意使用非 JSON 的 Content-Type，但带一个合法 JSON body。
            .header("content-type", "text/plain")
            .body(r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            200,
            "rewritten JSON body should reach upstream as content-type=application/json (got {}, body={:?})",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    /// 非规范的 `reasoning_effort` 字符串（如 "max"）被静默 drop 后转发——
    /// NVIDIA 会对它们返回 400，所以 drop 让请求成功，而不必让 client 改写
    /// 其 body。
    #[tokio::test]
    async fn noncanonical_reasoning_effort_max_is_dropped() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m", "choices": []
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"reasoning_effort":"max"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "non-canonical reasoning_effort should be dropped, not 400");
    }

    /// 布尔 `reasoning_effort: true` 被改写成 "high" 后转发，而不是被拒。
    /// cc-switch / OpenAI o1 风格的 client 会发 `true`/`false`，期望代理
    /// 接受它们。
    #[tokio::test]
    async fn reasoning_effort_true_is_rewritten_to_high() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m", "choices": []
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"reasoning_effort":true}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "reasoning_effort=true should be rewritten to 'high' and forwarded");
    }

    /// 合法的 `reasoning_effort` 值正常转发。
    #[tokio::test]
    async fn valid_reasoning_effort_is_forwarded() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "model": "m",
                "choices": []
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"reasoning_effort":"high"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
    }

    // ── tools 规范化（mod：ToolDescription.description null→empty）────

    /// 扁平形状的 tool 带 `description: null` 时，转发前必须被 coerce 成 `""`，
    /// 且请求仍要成功。NVIDIA 的 pydantic ToolDescription 对 null
    /// description 返回 400；failure.log 显示这会级联成 cc-switch 的
    /// "Empty choices array" 422。
    #[tokio::test]
    async fn tool_flat_null_description_normalized_to_empty() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m",
                "choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"tools":[{"type":"function","name":"t","description":null,"parameters":{}}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "flat tool with null description should be coerced, not 400");

        let requests = mock_server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        let desc = body["tools"][0]["description"]
            .as_str()
            .expect("flat description should be a string after coercion");
        assert_eq!(desc, "", "null description should become empty string, got {:?}", body["tools"][0]["description"]);
    }

    /// 嵌套的 OpenAI 规范形状 `{type:function, function:{...}}` 若缺失
    /// `function.description`，必须回填 `""`。
    #[tokio::test]
    async fn tool_nested_missing_description_backfilled_to_empty() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m",
                "choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"tools":[{"type":"function","function":{"name":"t","parameters":{}}}],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "nested tool with missing description should be backfilled, not 400");

        let requests = mock_server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        let desc = body["tools"][0]["function"]["description"]
            .as_str()
            .expect("nested function.description should be a string after backfill");
        assert_eq!(desc, "", "missing description should become empty string");
        // 健全性检查：非 null 的 description 原样保留。
        let name = body["tools"][0]["function"]["name"].as_str().unwrap();
        assert_eq!(name, "t");
    }

    /// 带真实非空 `description` 的 tool 必须原样通过（normalizer 在字段
    /// 已合法时是 no-op）。
    #[tokio::test]
    async fn tool_nonempty_description_preserved() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "x", "object": "chat.completion", "model": "m",
                "choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"tools":[{"type":"function","function":{"name":"t","description":"real desc","parameters":{}}}],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let requests = mock_server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["tools"][0]["function"]["description"].as_str(), Some("real desc"));
    }

    // ── 空 choices 合成（cc-switch 422 缓解，不改 cc-switch）────────────

    /// 非流式 `stream:false` 的 2xx body 带一个空 `choices` 数组——正是让
    /// cc-switch 的 `openai_to_anthropic` 抛 "Empty choices array" 的那种
    /// shape（→ 对 Claude Code 422，例如 context compaction 时）。代理必须
    /// 注入单个占位 choice 并保留 `id` / `model` / `usage`，使下游 converter
    /// 看到一个合法的 ChatCompletions 响应而非报错。
    #[tokio::test]
    async fn non_stream_empty_choices_is_synthesized_to_legal_body() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "cc-42", "object": "chat.completion", "model": "z-ai/glm-5.2",
                "choices": [],
                "usage": {"prompt_tokens": 7, "completion_tokens": 0, "total_tokens": 7}
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "synthesized response must be 200");
        let body: serde_json::Value = resp.json().await.unwrap();
        let choices = body["choices"].as_array().expect("choices present");
        assert_eq!(choices.len(), 1, "a placeholder choice is injected");
        assert_eq!(choices[0]["index"].as_u64(), Some(0));
        assert_eq!(choices[0]["message"]["role"].as_str(), Some("assistant"));
        assert_eq!(choices[0]["message"]["content"].as_str(), Some(""));
        assert_eq!(choices[0]["finish_reason"].as_str(), Some("stop"));
        // id/model/usage 必须保留，使 token 统计与元数据得以存续。
        assert_eq!(body["id"].as_str(), Some("cc-42"));
        assert_eq!(body["model"].as_str(), Some("z-ai/glm-5.2"));
        assert_eq!(body["usage"]["prompt_tokens"].as_u64(), Some(7));
        assert_eq!(body["usage"]["total_tokens"].as_u64(), Some(7));
    }

    /// NVIDIA 的 empty-shell 软失败：200、`choices:[]` 且 `usage:null` 且
    /// 空 `id`（没跑过推理）。代理必须返回 502，而不是合成一个空 content
    /// 占位，让 Claude Code 的 `/compact` 汇报一个可操作的上游错误，而非
    /// "summarization produced empty response"。这里重试也 shell，所以 client
    /// 拿到 502，且恰好发了两次上游调用（首次 + 一次钳住重试）。
    #[tokio::test]
    async fn empty_shell_soft_failure_surfaces_502_after_one_retry() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let mock_server = MockServer::start().await;
        // 用计数器跟踪调用，两次都返回空壳。
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(move |_req: &wiremock::Request| {
                let _n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(shell.clone())
            })
            .expect(2)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 3, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            502,
            "empty-shell soft failure must surface as 502, not a synthesized empty body"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["error"]["type"].as_str(),
            Some("upstream_empty_shell"),
            "error body should classify the failure as upstream_empty_shell, got: {body}"
        );
        // clamp 发生了：重试请求 body 带了 `max_tokens=8192`。
        assert!(
            body["error"]["detail"].as_str().unwrap_or_default().contains("max_tokens=8192"),
            "detail should record the clamped retry budget, got: {body}"
        );
        // 结构化 retry 字段（新的诊断面）。
        assert_eq!(body["error"]["retry"]["attempted"].as_bool(), Some(true));
        assert_eq!(
            body["error"]["retry"]["clamped_max_tokens"].as_u64(),
            Some(8192),
            "retry.clamped_max_tokens should be 8192, got: {body}"
        );
        assert_eq!(
            body["error"]["retry"]["outcome"].as_str(),
            Some("retry_also_empty_shell"),
            "retry.outcome should report the shell, got: {body}"
        );
        // 请求指纹与上游空壳以结构化形式置于 error 中。
        // 第一次上游调用在发出前先 clamp（glm-5.2 → 8192），所以指纹记录的是
        // 被 clamp 后的 8192，而非客户端原始的 64000——pre-clamp 的全部意义
        // 就是避免空壳发生；即便仍发生，重试也停在 8192。
        let req_fp = body["error"]["request_fingerprint"]
            .as_str()
            .unwrap_or("");
        assert!(
            req_fp.contains("max_tokens=8192") && req_fp.contains("messages=0"),
            "request_fingerprint should capture the pre-clamped max_tokens + message count, got: {req_fp}"
        );
        assert_eq!(
            body["error"]["upstream_shell"]["status"].as_u64(),
            Some(200),
            "upstream_shell.status should be 200, got: {body}"
        );
        assert_eq!(
            body["error"]["upstream_shell"]["body"]["choices"]
                .as_array()
                .map(|a| a.len()),
            Some(0),
            "upstream_shell.body should carry the shell's empty choices, got: {body}"
        );
    }

    /// Empty-shell 软失败、但那一次钳住重试**成功**的情形：代理必须返回 200
    /// 并带重试后的正常 choices，而不是 502。只重试一次（即便重试成功也不
    /// 循环）。
    #[tokio::test]
    async fn empty_shell_soft_failure_retries_and_recovers() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let good = serde_json::json!({
            "id": "cc-ok", "object": "chat.completion", "model": "z-ai/glm-5.2",
            "choices": [{"index":0,"message":{"role":"assistant","content":"summary"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 9, "completion_tokens": 1, "total_tokens": 10},
        });
        let mock_server = MockServer::start().await;
        // 用计数器第一次返回空壳、之后返回正常响应。
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(move |_req: &wiremock::Request| {
                let _n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if _n == 0 {
                    ResponseTemplate::new(200).set_body_json(shell.clone())
                } else {
                    ResponseTemplate::new(200).set_body_json(good.clone())
                }
            })
            .expect(2) // exactly two calls: initial + retry
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 3, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "the clamped retry should succeed as 200");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["choices"][0]["message"]["content"].as_str(),
            Some("summary"),
            "retry's real completion should be forwarded, got: {body}"
        );
    }

    /// 当 `retry_on_empty_shell = false` 时，shell 以 502 暴露，无上游重试
    ///（恰好一次上游调用）。
    #[tokio::test]
    async fn empty_shell_surfaces_502_when_retry_disabled() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(shell))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.retry_on_empty_shell = false;
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 502);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(
            body["error"]["detail"].as_str().unwrap_or_default().contains("not retried"),
            "detail should say the shell was not retried, got: {body}"
        );
        assert_eq!(
            body["error"]["retry"]["attempted"].as_bool(),
            Some(false),
            "retry.attempted should be false, got: {body}"
        );
        assert_eq!(
            body["error"]["retry"]["outcome"].as_str(),
            Some("not_attempted"),
            "retry.outcome should be not_attempted, got: {body}"
        );
    }

    /// 回归：per-(key, model) 的 empty-shell 熔断器必须在反复 shell 之后*真的
    /// 触发*。修复前，`mark_good` 在每个 200 上、于 `handle_success_maybe_synthesize`
    /// 的 empty-shell 分类*之前*就跑了；`mark_good` 会调
    /// `reset_all_circuit_breakers`，于是每条 shell 响应都把 shell 路径刚
    /// 加上去的 cb 计数器清零——熔断器永远攒不到阈值、`inc_circuit_breaker_triggered`
    /// 永不触发。把 mark_good 延后到分类之后，连续两次 shell
    /// （threshold=2、retry 关闭所以每次一条上游调用）就必须 trip 熔断器并
    /// 递增 triggered 计数器。
    #[tokio::test]
    async fn empty_shell_circuit_breaker_triggers_on_repeated_shells() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(shell))
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 3, 60);
        cfg.retry_on_empty_shell = false;
        cfg.circuit_breaker_threshold = 2;
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let url = format!("http://{}/v1/chat/completions", addr);
        let body = r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"messages":[],"stream":false}"#;

        let cb_before = crate::metrics::circuit_breaker_triggered().get();
        // 连续两次空壳；第二次必须触发断路器（threshold=2 处 check-后-
        // increment）。全局计数的 metrics 以 delta 方式读取，以在其他并行
        // 测试下保持稳健。
        for _ in 0..2 {
            let resp = client
                .post(&url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 502, "shell must surface as 502");
        }
        let cb_after = crate::metrics::circuit_breaker_triggered().get();
        assert!(
            cb_after > cb_before,
            "circuit_breaker_triggered counter must increment after two shells with threshold=2 \
             (before={cb_before}, after={cb_after}); if mark_good resets the cb counter on the \
             shell path the breaker never reaches threshold"
        );
    }

    /// 正常的非空 choices `stream:false` 响应必须原样通过 bounded-peek
    /// 路径——合成严格是「加性的」，只动空 choices 这一种情况。
    #[tokio::test]
    async fn non_stream_normal_response_passes_through_unchanged() {
        let mock_body = serde_json::json!({
            "id": "cc-7", "object": "chat.completion", "model": "m",
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        });
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_body.clone()))
            .expect(1)
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 60)).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        // 逐字节保真：choices/content 不变，无任何注入。
        assert_eq!(body["choices"][0]["message"]["content"].as_str(), Some("hi"));
        assert_eq!(body["id"].as_str(), Some("cc-7"));
        assert_eq!(body["usage"]["total_tokens"].as_u64(), Some(2));
    }

    /// 当 `synthesize_empty_choices = false` 时，代理不得合成：一个空
    /// choices 的 200 body 原样转发（缓解前的行为）。这就是配置字段上记载的
    /// 逃生口。
    #[tokio::test]
    async fn synthesize_disabled_forwards_empty_choices_unchanged() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "cc-1", "object": "chat.completion", "model": "m",
                "choices": [],
                "usage": {"prompt_tokens": 3, "completion_tokens": 0, "total_tokens": 3}
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut cfg = test_config(&mock_server.uri(), 0, 60);
        cfg.synthesize_empty_choices = false;
        let addr = start_proxy(cfg).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"m","messages":[],"stream":false}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        // 不变：choices 仍为空——合成是关闭的。
        assert!(body["choices"].as_array().map(|a| a.is_empty()).unwrap_or(false));
        assert_eq!(body["id"].as_str(), Some("cc-1"));
    }

    /// H1 回归：客户端发来的危险 header（`x-api-key`/`api-key`/`x-goog-api-key`/
    /// `openai-organization`/`cookie`/`x-forwarded-for`）必须被白名单剥掉，
    /// 不得透传到 NVIDIA 上游——否则一个不可信调用方能用自家上游凭证把代理
    /// 变成开放 relay、或改变上游计费路由。同时断言白名单内的 `traceparent`
    /// 仍被转发（W3C trace 传播）。
    #[tokio::test]
    async fn h1_dangerous_client_headers_not_forwarded_to_upstream() {
        let mock_server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .mount(&mock_server)
            .await;

        let addr = start_proxy(test_config(&mock_server.uri(), 0, 5)).await;
        let client = reqwest::Client::new();
        let _resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "attacker-upstream-key")
            .header("api-key", "attacker-upstream-key-2")
            .header("x-goog-api-key", "attacker-gcp-key")
            .header("openai-organization", "attacker-org")
            .header("cookie", "session=attacker-session")
            .header("x-forwarded-for", "1.2.3.4")
            .header("traceparent", "00-0af7651916cd43dd8448123456789abcdef-0123456789abcdef-01")
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        let requests = mock_server.received_requests().await.unwrap();
        assert!(!requests.is_empty());
        let upstream_headers = &requests[0].headers;
        // wiremock 的 Request.headers 是 HashMap<HeaderName, HeaderValues>；按
        // HeaderName.as_str() 收一把小写键，避免在两个 `http` 版本的 HeaderName
        // 之间做不安全的跨类型转换。
        let keys: std::collections::HashSet<&str> =
            upstream_headers.keys().map(|k| k.as_str()).collect();
        for banned in [
            "x-api-key",
            "api-key",
            "x-goog-api-key",
            "openai-organization",
            "cookie",
            "x-forwarded-for",
        ] {
            assert!(
                !keys.contains(banned),
                "危险 header {banned} 不应被转发到上游，但上游收到了"
            );
        }
        // 代理注入的鉴权头必须由代理掌控，而非来自客户端。
        let auth = upstream_headers
            .iter()
            .find(|(k, _)| k.as_str() == "authorization")
            .map(|(_, v)| v.iter().next().map(|h| h.as_str()).unwrap_or(""))
            .unwrap();
        assert_eq!(auth, "Bearer fake-key");
        // 白名单内的 W3C trace 头仍应转发。
        assert!(
            keys.contains("traceparent"),
            "traceparent 应被转发（白名单），但未出现在上游请求中"
        );
    }
}

#[cfg(test)]
mod usage_accounting_tests {
    use super::*;

    // ==================== 多模态 prompt-token 估算 ====================
    //
    // `estimate_prompt_tokens` / `message_content_tokens` / `EstPromptCosts`
    // 必须按粗粒度的每种 part 成本（image、audio）来计量多模态 `content` 数组，
    // 而不是序列化后的原始长度——否则一张内联 base64 图像（上万个字符）会
    // 把估算吹爆，假命中 413 硬上限 / 空壳不重试软限；而一张仅 URL 的图像
    // （几十字符）又会少算、漏过判定。

    /// 纯文本请求维持原有 `chars/4` 启发式不变，使既有 413 / no-retry 行为
    /// 对文本流量不回归。
    #[test]
    fn estimate_prompt_tokens_text_unchanged_chars_div_4() {
        let costs = EstPromptCosts::default_costs();
        let body = bytes::Bytes::from(
            r#"{"messages":[{"role":"user","content":"twenty four chars here!!"}]}"#,
        );
        // 24 字符 / 4 = 6
        assert_eq!(
            estimate_prompt_tokens(&body, &costs),
            Some(6),
            "text content must stay chars/4"
        );
    }

    /// 内联 base64 `image_url`（50_000 字符 payload）不得被记成
    /// 50_000/4 = 12_500 token——它收拢成扁平的 image cost（默认 1500），
    /// 使一个合法的多模态请求不至于被 413。
    #[test]
    fn estimate_prompt_tokens_inline_base64_image_collapses_to_image_cost() {
        let costs = EstPromptCosts::default_costs();
        let big_b64 = "A".repeat(50_000);
        let body = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"text","text":"what is this?"},
                    {"type":"image_url","image_url":{"url": format!("data:image/png;base64,{big_b64}")}}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let est = estimate_prompt_tokens(&bytes, &costs).expect("valid body");
        // 文本 "what is this?" = 14 字符 / 4 = 3；+ image_cost 1500 = 1503。
        // 50k 字符的 base64 载荷绝不可把这吹到 ~12500。
        assert_eq!(
            est, 1503,
            "base64 image must collapse to image_cost, not chars/4; got {est}"
        );
    }

    /// 仅 URL 的 `image_url`（几十字符）不得少算——它仍花费相同的固定
    /// `image_cost`，所以一个塞了许多远程图片的请求不能白蹭、漏过上限。
    #[test]
    fn estimate_prompt_tokens_url_image_costs_image_cost_not_chars() {
        let costs = EstPromptCosts::default_costs();
        let body = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"text","text":"hi"},
                    {"type":"image_url","image_url":{"url":"https://x.example/img.png"}},
                    {"type":"image_url","image_url":{"url":"https://x.example/img2.png"}}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let est = estimate_prompt_tokens(&bytes, &costs).expect("valid body");
        // "hi"/4 = 0（2 字符）；两张图 × 1500 = 恰好 3000。
        assert_eq!(
            est, 3000,
            "two URL images should cost exactly 3000 (2× image_cost), not undercount by URL length; got {est}"
        );
    }

    /// `input_audio` 块按 base64 长度（KiB 级）× per-KiB 成本计、并封顶——
    /// 所以一段适中的 clip 花费有界，再大也不会无界地主导估算。
    #[test]
    fn estimate_prompt_tokens_input_audio_sized_and_capped() {
        let costs = EstPromptCosts::default_costs(); // 16 per KiB, cap 10000
        // 8192 字符 ≈ 8 "KiB"（chars/1024=8）× 16 = 128 token——远低于封顶。
        let mid_b64 = "B".repeat(8192);
        let body = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"input_audio","input_audio":{"data": mid_b64, "format":"wav"}}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let est = estimate_prompt_tokens(&bytes, &costs).expect("valid body");
        assert_eq!(est, 128, "8KiB base64 audio @ 16/KiB = 128 tokens, got {est}");

        // 1_000_000 字符 ≈ 976 "KiB" × 16 = 15616 → 封顶到 10000。
        let huge_b64 = "C".repeat(1_000_000);
        let body = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"input_audio","input_audio":{"data": huge_b64, "format":"wav"}}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let est = estimate_prompt_tokens(&bytes, &costs).expect("valid body");
        assert_eq!(est, costs.audio_token_cap, "huge audio clip must hit the cap, got {est}");
    }

    /// 未知 content-block 类型按其序列化长度计、并封顶于
    /// `unknown_part_cap`，所以意外形状里一坨散落的 base64 blob 不会让
    /// 估算失控膨胀，但仍会记入*某值*。
    #[test]
    fn estimate_prompt_tokens_unknown_part_capped() {
        let costs = EstPromptCosts::default_costs(); // unknown cap = 2000
        let big = "Z".repeat(50_000);
        let body = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"video/embed","url": format!("data:video/mp4;base64,{big}")}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let est = estimate_prompt_tokens(&bytes, &costs).expect("valid body");
        assert_eq!(
            est, costs.unknown_part_cap,
            "unknown part must be capped, not chars/4 (~12500); got {est}"
        );
    }

    /// `EstPromptCosts::part_cost` 对一个空/无文本的 text block 返回 0，且
    /// 对缺失预期子字段的 part 不会 panic。
    #[test]
    fn part_cost_handles_missing_fields_without_panic() {
        let costs = EstPromptCosts::default_costs();
        // 无 `type` 的 text 块 → 0
        assert_eq!(costs.part_cost(&serde_json::json!({"type":"text"})), 0);
        // 即使无内部对象，image_url 仍按定额成本计
        assert_eq!(
            costs.part_cost(&serde_json::json!({"type":"image_url"})),
            costs.image_cost
        );
        // 无 `data` 的 input_audio → 0 字符 → 0 token
        assert_eq!(
            costs.part_cost(&serde_json::json!({"type":"input_audio","input_audio":{"format":"wav"}})),
            0
        );
        // 完全没有 `type` → unknown 路径，封顶（且绝不 panic）
        let unknown_cost = costs.part_cost(&serde_json::json!({"foo":"bar"}));
        assert!(
            unknown_cost <= costs.unknown_part_cap,
            "unknown part must not exceed unknown_part_cap, got {unknown_cost}"
        );
    }

    /// 多模态请求的请求指纹必须露出 part 普查（text/image/audio 计数）、
    /// 且**绝不**倾倒 base64 payload —— 否则一个 40k 字符的图会把 failure.log
    /// 灌爆。
    #[test]
    fn request_body_fingerprint_multimodal_no_base64_dump() {
        let big_b64 = "Q".repeat(40_000);
        let body = serde_json::json!({
            "model": "nvidia/neva-13b-vision-instruct",
            "messages": [{
                "role": "user",
                "content": [
                    {"type":"text","text":"describe this"},
                    {"type":"image_url","image_url":{"url": format!("data:image/png;base64,{big_b64}")}},
                    {"type":"input_audio","input_audio":{"data": big_b64.clone(), "format":"wav"}}
                ]
            }]
        });
        let bytes = bytes::Bytes::from(body.to_string());
        let fp = request_body_fingerprint(&bytes);
        // 各 part 普查计数已存在
        assert!(fp.contains("text=1"), "missing text census in: {fp}");
        assert!(fp.contains("image=1"), "missing image census in: {fp}");
        assert!(fp.contains("audio=1"), "missing audio census in: {fp}");
        // 无 base64 载荷泄漏
        assert!(!fp.contains("QQQQQ"), "base64 payload must not leak into fp: {fp}");
        // 仍带 role + 一段有封顶的文本片段
        assert!(fp.contains("role=user"), "missing role in: {fp}");
        // est_prompt_tokens 必须是多模态粗粒度估算：image 1500 +
        // audio ((40000/1024)*16 = 608，< 封顶 10000) = 2108 + 一点 text/4。
        // 而不是按 ~80k 字符 body 算 chars/4（~20000）。
        let est = fp
            .split("est_prompt_tokens=~")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        assert!(
            est < 5000,
            "fingerprint est must use multimodal costs, not chars/4 (would be ~20000); got {est}"
        );
    }

    #[test]
    fn parse_usage_pulls_components() {
        let v = serde_json::json!({
            "id": "x",
            "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
        });
        let u = parse_usage(&v).expect("usage present");
        assert_eq!(u.prompt, Some(10));
        assert_eq!(u.completion, Some(5));
        assert_eq!(u.total, Some(15));
    }

    #[test]
    fn parse_usage_absent_returns_none() {
        let v = serde_json::json!({"id": "x", "choices": []});
        assert!(parse_usage(&v).is_none());
    }

    #[test]
    fn parse_usage_negative_clamped_to_zero() {
        let v = serde_json::json!({"usage": {"prompt_tokens": -3, "total_tokens": -3}});
        let u = parse_usage(&v).expect("usage present");
        assert_eq!(u.prompt, Some(0));
        assert_eq!(u.total, Some(0));
    }

    /// 直接驱动 `UsageTap::feed`，确认它抽出最后见到的 `usage`，包括 `data:`
    /// 行被拆在两个 chunk 里的情况。
    #[test]
    fn usage_tap_feed_captures_usage_across_chunks() {
        let mut tap = UsageTap {
            inner: futures_util::stream::empty(),
            metrics: Arc::new(Metrics::new()),
            carry: String::new(),
            feed_buf: Vec::new(),
            usage: None,
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status: None,
            client_req_id: String::new(),
        };
        // 第一 chunk：一个 token delta（无 usage）+ 一行不完整的 data 行。
        tap.feed(&Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":7,",
        ));
        assert!(tap.usage.is_none(), "usage not yet complete");
        // 第二 chunk 补全那行被拆开的 data 行，再接一个最终 usage 事件。
        tap.feed(&Bytes::from_static(
            b"\"completion_tokens\":3,\"total_tokens\":10}}\n\n",
        ));
        let u = tap.usage.expect("usage captured");
        assert_eq!(u.prompt, Some(7));
        assert_eq!(u.completion, Some(3));
        assert_eq!(u.total, Some(10));
    }

    /// 回归：`bytes_stream()` 可能把一个多字节 UTF-8 码点拆进两个 chunk。
    /// 在跨 chunk UTF-8 重组修复之前，`feed` 做 `from_utf8(chunk)`，遇到 `Err`
    /// 就返回，把整个第一个 chunk 从 `carry` 丢弃——于是跨在边界上的 data 行
    /// 永远到不了 `classify_json_line`，`usage` / `saw_choices` 被悄悄少计。
    /// 这只是指标层的丢失（client 字节始终原样转发），但 tap 是本请求唯一的
    /// usage 记录，对可观测性重要。我们把 `"café"` 的多字节 `é`
    ///（U+00E9、两个 UTF-8 字节 0xC3 0xA9）拆进两次 feed、合起来承载一条完整
    /// 的带 usage 事件的 `data:` 行——tap 必须仍能解析并记录它。
    #[test]
    fn usage_tap_handles_multibyte_utf8_split_across_chunks() {
        let mut tap = UsageTap {
            inner: futures_util::stream::empty(),
            metrics: Arc::new(Metrics::new()),
            carry: String::new(),
            feed_buf: Vec::new(),
            usage: None,
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status: None,
            client_req_id: String::new(),
        };
        // 第一 chunk 在码点中段截断：café = 'c','a','f',0xC3,0xA9。
        // 我们在 0xC3 之后切，使下次 feed 的首字节（0xA9）补全 é。
        // 该 chunk 还带一行完整的首条 data: 行 + 一个未闭合的 JSON
        // 对象，其字符串值正好在被截断的 é 中段结束。
        let mut full1: Vec<u8> =
            b"data: {\"choices\":[{\"delta\":{\"content\":\"caf".to_vec();
        full1.push(0xC3);
        let mut chunk2: Vec<u8> = vec![0xA9];
        chunk2.extend_from_slice(b"\"}}]}\n\n");
        chunk2.extend_from_slice(
            b"data: {\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}\n\n",
        );
        tap.feed(&Bytes::from(full1.clone()));
        assert!(tap.usage.is_none(), "first chunk is a partial line + partial UTF-8");
        tap.feed(&Bytes::from(chunk2.clone()));
        let u = tap.usage.expect("usage captured across multibyte UTF-8 split");
        assert_eq!(u.prompt, Some(4));
        assert_eq!(u.completion, Some(1));
        assert_eq!(u.total, Some(5));
        // 而那条跨越边界的非空 choices 行也必须被分类——否则
        // 空 choices 诊断会假命中。
        assert!(tap.saw_choices, "choices event spanning UTF-8 split must be parsed");
    }

    // ── 空 choices 诊断：分类但不误判 ──

    /// 辅助：把一个 body 喂过一个 `UsageTap`（内层用 `empty()` 流，所以
    /// 真正被测的只有 `feed`/`flush_trailing`），再跑流结束逻辑，回报空
    /// choices 警告*是否*会触发。镜像 `Stream` 里的 `Poll::Ready(None)` 分支。
    fn empty_choices_would_warn(body: &[u8], status: Option<u16>) -> bool {
        let mut tap = UsageTap {
            inner: futures_util::stream::empty(),
            metrics: Arc::new(Metrics::new()),
            carry: String::new(),
            feed_buf: Vec::new(),
            usage: None,
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status,
            client_req_id: String::from("t"),
        };
        // 把整段 body 当作一块喂入；`feed` 处理完整行，
        // `flush_trailing` 排干 body 里没有结尾换行的剩余内容。
        tap.feed(&Bytes::copy_from_slice(body));
        tap.flush_trailing();
        // 与 poll_next 的 Ready(None) 分支同 guard。
        tap.saw_empty_choices && !tap.saw_choices
    }

    /// 真正病态的情形：一个 2xx、只用 usage 的 SSE 终末 chunk 带
    /// `choices: []`（且有 usage）。这正是 cc-switch 聚合后 422 的那种——
    /// 诊断必须触发。
    #[test]
    fn empty_choices_diagnostic_fires_on_empty_choices_json() {
        let body = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":0,\"total_tokens\":1}}\n";
        assert!(
            empty_choices_would_warn(body, Some(200)),
            "an empty-choices JSON object on a 2xx stream must trigger the diagnostic",
        );
    }

    /// 非流式 ChatCompletions 响应是单行 JSON、无尾换行。即便缺 `'\n'`，
    /// `flush_trailing` 也必须读到它非空的 `choices`，让诊断**不**误报。
    #[test]
    fn empty_choices_diagnostic_quiet_on_nonstream_json_without_newline() {
        let body = b"{\"id\":\"x\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "a normal non-streaming response (single JSON line, non-empty choices) must not warn",
        );
    }

    /// 一个流式响应：先有内容 delta，后跟一个只用 usage 的终末 chunk
    ///（`choices:[]`）。更早的非空 `choices` 让诊断保持安静——这是常见的
    /// OpenAI 流式收尾形态。
    #[test]
    fn empty_choices_diagnostic_quiet_when_real_choice_preceded_usage_tail() {
        let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "a real choice earlier in the stream suppresses the empty-tail warning",
        );
    }

    /// 非 JSON 的成功 body（text/plain 上游，或任何我们无法按 JSON 解析的
    /// body）绝不能触发诊断——没有可空的 choices 数组可言，于是 cc-switch 那条
    /// 触发器不适用。
    #[test]
    fn empty_choices_diagnostic_quiet_on_text_body() {
        let body = b"hello world this is not json at all\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "text/non-JSON bodies must not false-positive the empty-choices diagnostic",
        );
    }

    /// 错误形态的 body（`{"error":...}`）刻意按 JSON 解析、根本没有 `choices`
    /// 键——这不会置 `saw_empty_choices`，于是诊断保持安静。
    ///（cc-switch 的 "No choices in response" 是另一条上游处理路径。）
    #[test]
    fn empty_choices_diagnostic_quiet_on_error_body_no_choices_key() {
        let body = b"{\"error\":{\"message\":\"bad\",\"type\":\"BadRequestError\",\"code\":400}}\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "an error body with no choices key is not the empty-choices case",
        );
    }

    /// 空壳分类器必须只对 NVIDIA 的空壳形态触发：空 `choices` + null/缺失的
    /// `usage` + 空样 `id`。一个*合法*的零 token 压缩 body（空 choices 但有
    /// 真实 `usage`）**不得**被分类为空壳——那条路径仍要合成占位项。
    #[test]
    fn is_empty_shell_failure_classifies_only_null_usage_shell() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        assert!(is_empty_shell_failure(&shell), "real NVIDIA shell should classify");

        let legit = serde_json::json!({
            "choices": [], "id": "cc-1", "model": "z-ai/glm-5.2",
            "usage": {"prompt_tokens": 7, "completion_tokens": 0, "total_tokens": 7},
        });
        assert!(
            !is_empty_shell_failure(&legit),
            "legit zero-token completion (has usage) must NOT classify as a shell",
        );

        let real_completion = serde_json::json!({
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "id": "cc-2", "model": "z-ai/glm-5.2",
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        });
        assert!(
            !is_empty_shell_failure(&real_completion),
            "a normal completion is never a shell",
        );

        // 非空 usage 但空 id+空 choices 在契约上不可能；仍不应判为空壳
        //（usage 是决定性信号）。
        let weird = serde_json::json!({
            "choices": [], "id": "", "usage": {"prompt_tokens": 3},
        });
        assert!(!is_empty_shell_failure(&weird), "non-null usage wins over empty id");
    }

    /// 请求指纹必须呈现会影响 NVIDIA 对 GLM 类模型软拒绝的那些字段：
    /// `max_tokens`、`stream`、消息条数、粗略 prompt-token 估算、
    /// `temperature`/`top_p`/`reasoning_effort`、以及 `tools`——外加首/末
    /// message 的 role 预览。绝不记录完整 transcript；预览封顶 120 字符。
    #[test]
    fn request_body_fingerprint_captures_key_fields() {
        let body = bytes::Bytes::from(
            r#"{"model":"z-ai/glm-5.2","max_tokens":64000,"stream":false,"temperature":0.3,"top_p":0.9,"reasoning_effort":"high","tools":[{"type":"function"}],"tool_choice":"auto","messages":[{"role":"system","content":"You are Claude Code."},{"role":"user","content":"summarize this long transcript"}]}"#,
        );
        let fp = request_body_fingerprint(&body);
        assert!(fp.contains("max_tokens=64000"), "missing max_tokens in: {fp}");
        assert!(fp.contains("stream=false"), "missing stream in: {fp}");
        assert!(fp.contains("messages=2"), "missing message count in: {fp}");
        assert!(fp.contains("temperature=0.3"), "missing temperature in: {fp}");
        assert!(fp.contains("top_p=0.9"), "missing top_p in: {fp}");
        assert!(fp.contains("reasoning_effort=\"high\""), "missing reasoning_effort in: {fp}");
        assert!(fp.contains("tools=1"), "missing tools count in: {fp}");
        assert!(fp.contains("tool_choice=\"auto\""), "missing tool_choice in: {fp}");
        // 粗估：两条短 message ≈ 一小撮 token，绝非巨大数字——
        // 健全性检查它在场且大致非零。
        assert!(fp.contains("est_prompt_tokens="), "missing token estimate in: {fp}");
    }

    #[test]
    fn body_summary_records_real_scalar_values_not_presence_flags() {
        // U3：max_tokens / temperature / top_p / reasoning_effort 必须记入真实标量，
        // 而非旧的 `Bool(true)` 存在性标志。
        let parsed: serde_json::Value = serde_json::json!({
            "model": "z-ai/glm-5.2",
            "messages": [{"role":"user","content":"hi"}],
            "stream": true,
            "max_tokens": 64000,
            "temperature": 0.3,
            "top_p": 0.9,
            "reasoning_effort": "high",
        });
        let s = super::body_summary(&Some(parsed.clone()));
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["max_tokens"], 64000, "max_tokens must be real int, got: {v}");
        assert_eq!(v["temperature"], 0.3, "temperature must be real float, got: {v}");
        assert_eq!(v["top_p"], 0.9, "top_p must be real float, got: {v}");
        assert_eq!(v["reasoning_effort"], "high", "reasoning_effort must be real string, got: {v}");
        assert_eq!(v["model"], "z-ai/glm-5.2");
        assert_eq!(v["messages"], 1);
        assert_eq!(v["stream"], true);
    }

    #[test]
    fn body_summary_non_scalar_value_keeps_presence_marker() {
        // U3：当一个参数取了对象/数组（非标量），我们无法精确记录真值，但仍要
        // 保留「该键存在」这一诊断信号 —— 用 `<present>` 占位，而非静默丢失。
        let parsed: serde_json::Value = serde_json::json!({
            "model": "m",
            "messages": [],
            "max_tokens": {"weird": true},
        });
        let s = super::body_summary(&Some(parsed));
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["max_tokens"], "<present>", "non-scalar must mark presence, got: {v}");
    }

    #[test]
    fn body_summary_omits_absent_fields() {
        let parsed: serde_json::Value = serde_json::json!({"model": "m", "messages": []});
        let s = super::body_summary(&Some(parsed));
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert!(v.get("max_tokens").is_none(), "absent fields must not appear, got: {v}");
        assert!(v.get("temperature").is_none());
    }

    #[test]
    fn request_body_fingerprint_handles_minimal_body() {
        let body = bytes::Bytes::from(r#"{"model":"m","messages":[]}"#);
        let fp = request_body_fingerprint(&body);
        assert!(fp.contains("messages=0"), "no messages should show 0: {fp}");
        assert!(fp.contains("max_tokens=<absent>"), "absent max_tokens should be visible: {fp}");
        assert!(fp.contains("stream=<absent>"), "absent stream should be visible: {fp}");
    }

    #[test]
    fn shell_body_fingerprint_summarizes_shell_fields() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let fp = shell_body_fingerprint(&shell);
        assert!(fp.contains("id=\"\""), "id should be empty-string, got: {fp}");
        assert!(fp.contains("model=\"\""), "model should be empty-string, got: {fp}");
        assert!(fp.contains("choiceslen=0"), "choiceslen should be 0, got: {fp}");
        assert!(fp.contains("usage=null"), "usage should be null, got: {fp}");
        assert!(fp.contains("created=0"), "created should be 0, got: {fp}");
    }
}
