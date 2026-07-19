use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use tracing::{error, info, warn};
use warp::{self, Rejection, Reply};

use crate::failure_log;
use crate::key_pool::{mask_key, KeyPool};
use crate::metrics::Metrics;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_util::{Stream, StreamExt};
use reqwest::header::{self as req_header, HeaderValue as ReqHeaderValue};

/// Process-wide monotonic counter used to mint per-failure `req_id`s.
/// Monotonic, never resets — gives us a stable correlation id for retries
/// belonging to the same logical client request (every retry for the same
/// request gets the next counter value, so they sort together).
static REQ_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_req_id() -> u64 {
    REQ_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Proxy handles forwarding requests to NVIDIA API using a key from the pool.
#[derive(Clone)]
pub struct Proxy {
    base_url: String,
    key_pool: Arc<KeyPool>,
    metrics: Arc<Metrics>,
    client: reqwest::Client,
    /// Per-request timeout in seconds. 0 disables it; streaming requests
    /// always bypass it (long running token generation can exceed 60s).
    request_timeout_secs: u64,
    /// Retry/upstream-error policy (exponential backoff + key-disable threshold).
    retry_config: crate::config::RetryConfig,
    /// Max milliseconds to wait on a single key's token bucket before
    /// switching to another key. 0 disables the timeout entirely.
    token_wait_ms: u64,
    /// Client-facing model name → NVIDIA-ready model id map. When a request
    /// carries `model` matching a key here, the proxy rewrites the field to
    /// the mapped value before forwarding upstream. Unmatched names are
    /// forwarded unchanged.
    model_aliases: std::collections::HashMap<String, String>,
    /// Cache of the upstream `/v1/models` catalog. NVIDIA answers this
    /// endpoint without auth, so we forward it through and memoize the
    /// result to avoid re-probing upstream on every client startup.
    models_cache: Arc<Mutex<Option<serde_json::Value>>>,
    /// Inject `stream_options.include_usage: true` into streaming chat
    /// requests so NVIDIA returns token usage at stream end. NVIDIA/OpenAI
    /// only emit usage when this flag is set; without it our token metrics
    /// are zero for the (common) streaming case.
    inject_stream_usage: bool,
}

/// Outcome of a single upstream attempt. `Retry` is used only for 401 without
/// a `Retry-After` header when the caller still has retry budget left.
enum AttemptResponse {
    Done(warp::reply::Response),
    Retry,
}

/// Errors that can occur during a single upstream attempt.
#[derive(Debug)]
enum AttemptError {
    Network(reqwest::Error),
    Timeout,
    BodyRead(reqwest::Error),
    /// Non-retryable rejection that should be surfaced to the client as-is.
    Fatal(Rejection),
}
impl warp::reject::Reject for AttemptError {}

/// Recursively coerce any numeric `code` field inside a JSON value to a
/// string. NVIDIA returns `{"error":{"code":503,...}}` where `code` is a
/// number, but OpenAI's schema (and Go clients like Cursor) expect
/// `code` to be a string. This walks objects and arrays in place.
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

/// Coerce a complete JSON body in place. Returns a new Bytes whose
/// `error.code` (if present and numeric) has been converted to a string;
/// if the body isn't valid JSON or re-serialization fails, the original
/// bytes are returned untouched.
fn coerce_json_body(body: &Bytes) -> Bytes {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(mut v) => {
            coerce_error_code_json(&mut v);
            serde_json::to_vec(&v).map(Bytes::from).unwrap_or_else(|_| body.clone())
        }
        Err(_) => body.clone(),
    }
}

/// Coerce `error.code` inside an SSE chunk. SSE events are line-oriented
/// (`data: <json>\n\n`); we process each `data:` line by parsing its JSON
/// payload, coercing, and re-serializing. Lines that aren't `data:` lines,
/// or whose payload isn't valid JSON, are passed through unmodified.
///
/// This operates on a single chunk without cross-chunk state. A `data:`
/// line split across chunk boundaries will fail JSON parse for the partial
/// payload and be forwarded as-is; in practice NVIDIA's error events are
/// short and arrive in a single chunk.
fn coerce_sse_chunk(chunk: &Bytes) -> Bytes {
    // Fast path: the vast majority of streaming chunks are token deltas
    // without a `code` field. Skip the per-line JSON parse entirely when
    // the chunk provably has no `"code"` substring.
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

/// Build a JSON error response with the given status and OpenAI-style
/// `{"error":{"message":..., "type":...}}` body. Used for unsupported API
/// shapes (Responses / Anthropic Messages) so clients get a clear, typed
/// signal instead of a confusing upstream 404.
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

/// Build a JSON response from an arbitrary `serde_json::Value` (e.g. the
/// upstream `/v1/models` catalog), used when forwarding NVIDIA's real model
/// list through unchanged.
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

/// Build a compact, stable summary of the request body for logging.
/// Captures the fields most likely to correlate with upstream failures:
/// model, number of messages, whether streaming, and the presence of
/// parameters like reasoning_effort / temperature / max_tokens. The summary
/// is a JSON string so `jq` can filter on it directly.
fn body_summary(parsed_body: &Option<serde_json::Value>) -> String {
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
            if v.get(field).is_some() {
                summary.insert(field.to_string(), serde_json::Value::Bool(true));
            }
        }
    }
    serde_json::Value::Object(summary).to_string()
}

/// Token usage extracted from an upstream `usage` object. Any component may
/// be `None` when the upstream omits it (e.g. streaming chunks before the
/// final one, or usage-less responses).
#[derive(Debug, Default, Clone, Copy)]
struct Usage {
    prompt: Option<u64>,
    completion: Option<u64>,
    total: Option<u64>,
}

/// Pull `Usage` out of a parsed JSON value. Returns `None` when there is no
/// `usage` object, so callers can keep the most recent one they've seen.
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

/// A stream wrapper that scans each forwarded chunk for `usage` (handling
/// SSE `data:` lines and bare JSON, with a carry buffer for lines split
/// across chunk boundaries) and, once the upstream stream ends, records the
/// most recent usage to the Prometheus counters. This is what makes
/// `proxy_tokens_input/output/total` actually reflect real traffic — without
/// it, successful responses were forwarded as a stream and never inspected.
struct UsageTap<S> {
    inner: S,
    metrics: Arc<Metrics>,
    carry: String,
    usage: Option<Usage>,
}

impl<S> UsageTap<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send,
{
    fn feed(&mut self, chunk: &Bytes) {
        let s = match std::str::from_utf8(chunk) {
            Ok(s) => s,
            Err(_) => return,
        };
        self.carry.push_str(s);
        // Process every complete line (one ending in '\n'); keep the trailing
        // partial line (if any) in `carry` for the next chunk.
        while let Some(nl) = self.carry.find('\n') {
            let line: String = self.carry.drain(..=nl).collect();
            let line = line.trim_end_matches(['\r', '\n']);
            let json_str = match line.strip_prefix("data:") {
                Some(rest) => rest.trim_start(),
                None => line.trim(),
            };
            if json_str.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(u) = parse_usage(&v) {
                    self.usage = Some(u);
                }
            }
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
                if let Some(u) = me.usage.take() {
                    me.metrics.record_usage(u.prompt, u.completion, u.total);
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Proxy {
    pub fn new(
        base_url: String,
        key_pool: Arc<KeyPool>,
        metrics: Arc<Metrics>,
        request_timeout_secs: u64,
        retry_config: crate::config::RetryConfig,
        token_wait_ms: u64,
        model_aliases: std::collections::HashMap<String, String>,
        inject_stream_usage: bool,
    ) -> Self {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            // Connection-level backstop so a stalled/hung upstream can't pin
            // a proxy connection indefinitely. This bounds the *connection*
            // phase and the read-quiet period between bytes; streaming bodies
            // (the common case here) are NOT bound by it because each SSE
            // chunk resets the read-quiet timer. The per-request
            // `request_timeout_secs` wrapper still governs non-streaming
            // responses end-to-end.
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build reqwest client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            key_pool,
            metrics,
            client,
            request_timeout_secs,
            retry_config,
            token_wait_ms,
            model_aliases,
            models_cache: Arc::new(Mutex::new(None)),
            inject_stream_usage,
        }
    }

    /// Build a minimal `/v1/models` response from the configured
    /// `model_aliases` only. This is the fallback used when the upstream
    /// `/v1/models` catalog can't be fetched; the full real catalog is
    /// served via `handle_models` whenever upstream is reachable.
    ///
    /// We list alias keys (the client-facing names) rather than the
    /// mapped NVIDIA ids — the caller already knows them by that name
    /// and will send them in `model`. We deliberately do NOT hardcode any
    /// NVIDIA model names: the real catalog is served through
    /// `handle_models`, and a hardcoded list drifts from reality and can
    /// point clients at models that don't exist.
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

    /// Answer `GET /v1/models` with NVIDIA's real catalog. The upstream
    /// endpoint is unauthenticated and returns the full OpenAI-format list,
    /// so we forward it through and cache the result to avoid re-probing
    /// upstream on every client startup (cc-switch / Cursor fire this on
    /// launch). On any failure we fall back to the `model_aliases`-only
    /// list so the endpoint still returns 200.
    async fn handle_models(&self) -> warp::reply::Response {
        // Serve from cache if we've already fetched the upstream catalog.
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

        // Upstream unreachable / malformed: fall back to the configured
        // `model_aliases` list. We no longer hardcode NVIDIA model names.
        self.local_models_response()
    }

    /// Build an upstream reqwest request for the given client path/method/body
    /// and authorize with `key_str`. Hop-by-hop and client-supplied auth
    /// headers are stripped. The returned builder is ready to `.send()`.
    ///
    /// `is_streaming` controls an explicit `Accept` override: NVIDIA (and
    /// most OpenAI-compatible servers) select SSE vs buffered JSON based on
    /// the `Accept` header, not just the body's `stream` field. Some clients
    /// send `stream:true` without `Accept: text/event-stream`, which leads
    /// NVIDIA to return buffered JSON for a request the client then tries to
    /// parse as SSE — and to the proxy buffering a long body, which trips
    /// worker back-pressure (503 ResourceExhausted). Forcing the Accept
    /// header in lockstep with the `stream` field keeps both sides agreeing
    /// on the transfer shape.
    fn build_upstream_request(
        &self,
        url: &str,
        method: &warp::http::Method,
        headers: &warp::http::HeaderMap,
        body: &bytes::Bytes,
        key_str: &str,
        is_streaming: bool,
    ) -> reqwest::RequestBuilder {
        let mut req_builder = self.client.request(method.clone(), url);
        req_builder = req_builder
            .header(req_header::AUTHORIZATION, format!("Bearer {}", key_str))
            .header(req_header::USER_AGENT, "nvidia-key-proxy/0.1");
        for (hkey, hvalue) in headers.iter() {
            let key_name = hkey.as_str();
            match key_name {
                "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailers"
                | "transfer-encoding"
                | "upgrade"
                | "content-length"
                | "host"
                // `accept` is dropped here and replaced explicitly below so
                // the proxy controls which response shape NVIDIA picks.
                | "accept"
                | "authorization" => continue,
                _ => {}
            }
            if let Ok(req_val) = ReqHeaderValue::from_bytes(hvalue.as_bytes()) {
                req_builder = req_builder.header(key_name, req_val);
            }
        }
        // Force Accept in lockstep with `stream`. This is the single
        // source of truth for "does the client want SSE", and matches what
        // cc-switch / OpenAI SDKs do when talking to NVIDIA directly.
        let accept_val = if is_streaming {
            "text/event-stream"
        } else {
            "application/json"
        };
        req_builder = req_builder.header(req_header::ACCEPT, accept_val);
        if !body.is_empty() {
            req_builder = req_builder.body(body.clone());
        }
        req_builder
    }

    /// Main entry point called by warp filter.
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

        // ── Explicitly reject unsupported API shapes ─────────────────────
        //
        // NVIDIA only serves OpenAI Chat Completions. It does not expose the
        // OpenAI Responses API (`/v1/responses`) nor the Anthropic Messages
        // API (`/v1/messages`) — both return 404 upstream. Rather than
        // silently forwarding a body these endpoints can't parse (which
        // yields a confusing upstream 4xx with no hint to the client), reject
        // clearly up front, before acquiring a key. (A request to any of
        // these with an invalid key also 404s at the gateway, so we can't
        // distinguish "endpoint missing" from "auth failed" that way — we
        // reject strictly by path.)
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

        // ── Local answers for metadata endpoints ─────────────────────────
        //
        // `GET /v1/models` discovery probes from OpenAI-style clients
        // (cc-switch, Cursor, …) should not consume a key from our pool nor
        // burn a rate-limit token. NVIDIA answers this endpoint without auth
        // and returns its full real catalog, so we forward it through and
        // cache the result (see `handle_models`). We don't forward chat
        // traffic on this path.
        if method == warp::http::Method::GET
            && (path == "/v1/models" || path == "/v1/models/")
        {
            return Ok(self.handle_models().await);
        }

        // ── Acquire a key with token-starvation switch loop ─────────────

        // ── Acquire a key with token-starvation switch loop ─────────────
        let max_switches = self.retry_config.max_key_switches as usize;
        let token_timeout = self.token_wait_ms;
        let mut tried_key_hashes: Vec<u64> = Vec::new();
        let mut key_handle: Option<crate::key_pool::KeyStateHandle> = None;
        let mut switch_count: u32 = 0;

        for _attempt in 0..=max_switches {
            // Acquire (score-based top-1)
            let candidate = match self.key_pool.acquire().await {
                Some(k) => k,
                None => {
                    // Release any previously held key's in_flight before
                    // returning SERVICE_UNAVAILABLE.
                    if let Some(prev) = key_handle.take() {
                        prev.dec_in_flight();
                    }
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(ServiceUnavailable {
                        msg: "No available API keys".into(),
                    }));
                }
            };

            // Avoid re-trying the same key (identified by pointer hash).
            let key_hash = {
                let addr = &candidate as *const crate::key_pool::KeyStateHandle as usize;
                addr as u64
            };
            if tried_key_hashes.contains(&key_hash) {
                // This key was already tried; release it and loop for next.
                candidate.dec_in_flight();
                continue;
            }
            tried_key_hashes.push(key_hash);

            // Drop any previous handle before taking the new one.
            if let Some(prev) = key_handle.take() {
                prev.dec_in_flight();
            }

            // Try to consume a token with deadline.
            let got_token = candidate.try_consume_token(token_timeout).await;
            if !got_token {
                // Token starved — switch to another key.
                candidate.dec_in_flight();
                self.metrics.inc_key_switch_token_starvation();
                switch_count += 1;
                if switch_count <= max_switches as u32 {
                    tracing::warn!(
                        switch = switch_count,
                        "Token starved on current key; switching to another"
                    );
                }
                continue; // loop to try next key
            }

            key_handle = Some(candidate);
            break;
        }

        let mut key_handle = match key_handle {
            Some(k) => k,
            None => {
                self.metrics.inc_error();
                return Err(warp::reject::custom(ServiceUnavailable {
                    msg: "No available API keys (all tried keys were token-starved)".into(),
                }));
            }
        };

        // Generate a single correlation id for this client request. All
        // upstream attempts (including retries) will share this id so we can
        // follow the retry chain in failure.log and tracing.
        let client_req_id = format!("r{}", next_req_id());

        // Parse the body once for validation and metadata extraction.
        // `body_value` is the (possibly rewritten) JSON we will forward
        // upstream; `body_bytes` is the serialized bytes derived from it.
        // When the client's original body is valid JSON we may rewrite
        // fields whose OpenAI-style spellings NVIDIA rejects (booleans in
        // `reasoning_effort`/`max_tokens`, client-side model aliases, …)
        // rather than failing the request — this is what lets cc-switch
        // clients that work against NVIDIA directly keep working through
        // the proxy without body edits on their side.
        let parsed_body: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
        let body_summary = body_summary(&parsed_body);

        // ── Body normalization (in-place rewrite of the parsed JSON) ──────
        //
        // For chat-completion-style requests we rewrite a handful of
        // fields that cc-switch / OpenAI-style clients send in a form
        // NVIDIA NIM rejects with a 400, instead of bouncing the request
        // back to the caller. The intent is: "if it would work against
        // NVIDIA directly, it should work through us."
        //
        // Specifically we:
        //   * `max_tokens`
        //       - boolean → removed (let NVIDIA pick the model default)
        //       - null    → removed
        //       - string  → parsed to int; if not a positive int, removed
        //       - number <= 0 → removed
        //   * `reasoning_effort`
        //       - `true`  → "high"
        //       - `false` / null → removed
        //       - string not in {low,medium,high} → removed
        //         (we used to 400 here, but some clients send "auto" /
        //         "max" as a preference; safer to drop than to fail)
        //   * `model` is remapped through `model_aliases` if configured.
        //
        // A hard 400 is now reserved for the one case we cannot salvage:
        // a non-object top-level body for a chat-completion path that the
        // caller clearly intended to be JSON.
        let mut body_value: Option<serde_json::Value> = parsed_body.clone();
        let mut model_rewrite_log: Option<String> = None;
        if let Some(obj) = body_value.as_mut().and_then(|v| v.as_object_mut()) {
            // max_tokens normalization
            if let Some(mt) = obj.get("max_tokens").cloned() {
                let keep = match &mt {
                    serde_json::Value::Number(n) => n.as_i64().is_some_and(|x| x >= 1),
                    serde_json::Value::String(s) => s.parse::<i64>().is_ok_and(|x| x >= 1),
                    _ => false,
                };
                if !keep {
                    warn!(
                        client_req_id = %client_req_id,
                        "Dropping non-positive/non-numeric max_tokens: {}", mt
                    );
                    obj.remove("max_tokens");
                } else if let serde_json::Value::String(s) = &mt {
                    // Coerce numeric strings to numbers so NVIDIA sees an int.
                    if let Ok(n) = s.parse::<i64>() {
                        obj.insert(
                            "max_tokens".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(n)),
                        );
                    }
                }
            }

            // reasoning_effort normalization: bool/non-canonical → rewrite/drop
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
                                "Dropping unrecognized reasoning_effort value: {}", s
                            );
                            None
                        }
                    },
                    // numbers/arrays/objects: drop silently
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

            // model alias remap: client name → NVIDIA-ready name
            if let Some(m) = obj.get("model").and_then(|x| x.as_str()).map(|s| s.to_string()) {
                if let Some(target) = self.model_aliases.get(&m).cloned() {
                    if target != m {
                        info!(
                            client_req_id = %client_req_id,
                            "Remapping model alias '{}' -> '{}'", m, target
                        );
                        obj.insert(
                            "model".to_string(),
                            serde_json::Value::String(target.clone()),
                        );
                        model_rewrite_log = Some(target);
                    }
                }
            }

            // stream_options.include_usage injection (streaming token usage):
            // NVIDIA/OpenAI only emit `usage` at the end of an SSE stream when
            // the request carries `stream_options.include_usage: true`. Inject
            // it so our token metrics are populated for streaming requests,
            // but never override a value the client already set (true OR
            // false) — only inject when `include_usage` is absent.
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
        }

        // Re-serialize the (possibly rewritten) body. If the original
        // body wasn't JSON, fall back to the original bytes untouched —
        // non-JSON bodies are rare here and forwarded as-is.
        let body_bytes: bytes::Bytes = match &body_value {
            Some(v) => serde_json::to_vec(v)
                .map(bytes::Bytes::from)
                .unwrap_or_else(|_| body.clone()),
            None => body.clone(),
        };

        // Extract `model` for logging. Prefer the (possibly rewritten)
        // value so logs show the name NVIDIA will actually see.
        let model = model_rewrite_log
            .clone()
            .or_else(|| {
                body_value
                    .as_ref()
                    .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string()))
            })
            .unwrap_or_default();

        // Forward the (possibly rewritten) body. Honor the client's `stream`
        // field so chat-completions clients that ask for streaming get a
        // streamed body (no `bytes().await` buffering).
        let request_body = body_bytes.clone();
        let is_streaming = body_value
            .as_ref()
            .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
            .unwrap_or(false);

        // Build upstream URL
        let mut url = self.base_url.clone();
        url.push_str(&path);

        info!(client_req_id = %client_req_id, body_summary = %body_summary, "Proxying request to: {} {}", method.as_str(), url);

        // --- Upstream attempt loop with exponential backoff retry ---
        //
        // Only 401 *without* `Retry-After` triggers a retry against another
        // healthy key, sleeping with exponential backoff
        // (backoff_base * 2^attempt, capped at backoff_max). Each such
        // failure increments the offending key's fail_count; once it crosses
        // `auth_fail_threshold` we mark the key Disabled.
        //
        // 401 *with* `Retry-After`, and 429/403 in either case, are NOT retried:
        // they are deterministic upstream decisions and retrying on another key
        // would only burn the whole pool.
        let mut attempt: u32 = 0;
        loop {
            let is_last_attempt = attempt >= self.retry_config.max_retries;
            let attempt_fut = self.process_upstream_attempt(
                &key_handle,
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
            );

            // For streaming requests we never wrap in a hard timeout
            // (long-running token generation can exceed 60s). For non-streaming
            // requests we honor request_timeout_secs (0 disables the timeout).
            let result = if is_streaming || self.request_timeout_secs == 0 {
                attempt_fut.await
            } else {
                match tokio::time::timeout(
                    Duration::from_secs(self.request_timeout_secs),
                    attempt_fut,
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => {
                        let k = key_handle.key().await;
                        warn!(key = %mask_key(&k), "Upstream request timeout");
                        self.key_pool.mark_bad(&k, None).await;
                        Err(AttemptError::Timeout)
                    }
                }
            };

            match result {
                Ok(AttemptResponse::Done(resp)) => {
                    key_handle.dec_in_flight();
                    self.metrics.observe_latency(start.elapsed().as_secs_f64());
                    info!(
                        client_req_id = %client_req_id,
                        total_attempts = attempt + 1,
                        elapsed_ms = start.elapsed().as_millis(),
                        "Request completed successfully"
                    );
                    return Ok(resp);
                }
                Ok(AttemptResponse::Retry) => {
                    // 401 without Retry-After: caller still has retry budget.
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            // Release the current handle before switching.
                            key_handle.dec_in_flight();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            key_handle = next.clone();
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!("Token starved on retry key; giving up");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream 401".into(),
                                }));
                            }
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "Retrying after upstream 401 with a different key"
                            );
                            continue;
                        }
                        warn!(client_req_id = %client_req_id, attempt = attempt, "No other keys available; passing 401 through");
                    }
                    // Defensive fallback (helper should have returned Done on last attempt).
                    key_handle.dec_in_flight();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream 401".into(),
                    }));
                }
                Err(AttemptError::Network(e)) => {
                    // Network-level error: cool the key and, if we still have
                    // retries left, try another key without counting this as
                    // an auth failure.
                    let k = key_handle.key().await;
                    warn!(key = %mask_key(&k), "Request send error: {}", e);
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
                    // Network errors are often correlated (NVIDIA maintenance,
                    // DNS hiccup, local network outage) across keys: retrying
                    // `max_retries` times (default 3) against different keys
                    // just multiplies the pain for the client, which has
                    // already received ~10s of connection timeouts. Cap
                    // network-error retries at 1 additional attempt.
                    let network_retry_cap: u32 = 1;
                    if attempt < network_retry_cap && attempt < self.retry_config.max_retries {
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = false; // network cap = 1 → no further retry
                            self.metrics.inc_retry_attempt_failure();
                            key_handle.dec_in_flight();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            key_handle = next.clone();
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!(client_req_id = %client_req_id, attempt = attempt, "Token starved on retry key after network error");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream request failed".into(),
                                }));
                            }
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "Retrying upstream send with a different key"
                            );
                            continue;
                        }
                    }
                    key_handle.dec_in_flight();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream request failed".into(),
                    }));
                }
                Err(AttemptError::Timeout) => {
                    let k = key_handle.key().await;
                    warn!(key = %mask_key(&k), "Upstream request timeout");
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
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            key_handle.dec_in_flight();
                            let got_backup = next.try_consume_token(self.token_wait_ms).await;
                            key_handle = next.clone();
                            let got_token = got_backup;
                            if !got_token {
                                next.dec_in_flight();
                                self.metrics.inc_key_switch_token_starvation();
                                warn!(client_req_id = %client_req_id, attempt = attempt, "Token starved on retry key after timeout");
                                self.metrics.inc_error();
                                return Err(warp::reject::custom(BadGateway {
                                    msg: "Upstream request timed out".into(),
                                }));
                            }
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                client_req_id = %client_req_id,
                                attempt = attempt,
                                "Retrying after upstream timeout with a different key"
                            );
                            continue;
                        }
                    }
                    key_handle.dec_in_flight();
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream request timed out".into(),
                    }));
                }
                Err(AttemptError::BodyRead(e)) => {
                    let k = key_handle.key().await;
                    error!(key = %mask_key(&k), "Failed to read response body: {}", e);
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
                    key_handle.dec_in_flight();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Failed to read response body".into(),
                    }));
                }
                Err(AttemptError::Fatal(r)) => {
                    key_handle.dec_in_flight();
                    return Err(r);
                }
            }
        }
    }

    /// Exponential backoff sleep: `base * 2^(attempt-1)` capped at `max`.
    async fn backoff_sleep(&self, attempt: u32) {
        let base = self.retry_config.backoff_base_ms;
        let max = self.retry_config.backoff_max_ms;
        // attempt starts at 1 for the first retry → delay = base
        let exp = if attempt == 0 { 0 } else { attempt - 1 };
        let mut delay = base.saturating_mul(1u64 << exp.min(20));
        delay = delay.min(max);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    /// Send one upstream request, classify the response, and build a reply.
    ///
    /// Returns `Retry` only for 401 without `Retry-After` when
    /// `is_last_attempt` is false. All other outcomes are either a finished
    /// response or a non-retryable error.
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
    ) -> Result<AttemptResponse, AttemptError> {
        let key_str = key_handle.key().await;
        let key_hash = {
            let addr = key_handle as *const crate::key_pool::KeyStateHandle as usize;
            addr as u64
        };
        info!(
            client_req_id = %client_req_id,
            attempt = attempt,
            key = %mask_key(&key_str),
            key_hash = key_hash,
            body_summary = %body_summary,
            "upstream attempt start"
        );
        let req_builder = self.build_upstream_request(url, method, headers, request_body, &key_str, is_streaming);
        let resp = req_builder.send().await.map_err(AttemptError::Network)?;

        let status = resp.status();
        let content_type = resp
            .headers()
            .get(req_header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        info!(
            client_req_id = %client_req_id,
            attempt = attempt,
            status = %status,
            content_type = %content_type,
            url = %url,
            "Upstream response"
        );

        let is_sse = content_type.contains("text/event-stream");

        // Clone headers before consuming the body
        let resp_headers: Vec<(String, Vec<u8>)> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
            .collect();

        // Passthrough streaming: when the client asked for `stream` and
        // upstream responded with SSE, forward the upstream stream
        // verbatim without buffering.
        if is_streaming && is_sse && status.is_success() {
            let resp = self
                .handle_passthrough_stream(resp, resp_headers, status)
                .await
                .map_err(AttemptError::Fatal)?;
            return Ok(AttemptResponse::Done(resp));
        }

        // Successful non-SSE responses: forward the upstream bytes_stream
        // to the client without buffering. NVIDIA workers expect the
        // downstream to drain the body so they can release inference
        // resources; `resp.bytes().await` would hold the whole body in
        // memory until the (potentially slow) client finishes reading,
        // which triggers 503 `ResourceExhausted: Worker local total
        // request limit reached`. Mark the key good here, then stream.
        if status.is_success() {
            self.key_pool.mark_good(&key_str).await;
            self.metrics.inc_success();
            let resp = self
                .handle_passthrough_stream(resp, resp_headers, status)
                .await
                .map_err(AttemptError::Fatal)?;
            return Ok(AttemptResponse::Done(resp));
        }

        // Non-success paths still buffer the body — we need the bytes for
        // classification (Retry-After parsing, error.code coercion, token
        // stats, failure.log). Errors are short JSON, so buffering them is
        // cheap and doesn't keep workers alive (errors release eagerly).
        let body_bytes = resp.bytes().await.map_err(AttemptError::BodyRead)?;

        // `resp.headers()` is no longer accessible after `resp.bytes()`
        // consumed `resp`; resolve Retry-After from the cloned `resp_headers`
        // snapshot once, here, for all 401/429/403 branches to reuse.
        let retry_after: Option<u64> = resp_headers
            .iter()
            .find(|(k, _)| k == req_header::RETRY_AFTER.as_str())
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok());

        // From here on, status is non-success (success paths returned above).
        if status == reqwest::StatusCode::UNAUTHORIZED {
            // 401 is the only status we retry: it's the clearest
            // "this specific key is rejected" signal.
            if let Some(ra) = retry_after {
                warn!(
                    key = %mask_key(&key_str),
                    retry_after = ra,
                    "Upstream 401 with Retry-After; cooling key, no retry"
                );
                self.key_pool.mark_bad(&key_str, Some(ra)).await;
                self.metrics.inc_error();
                // Fall through: deliver this response.
            } else {
                // Record failure (also halves this key's weight so the next
                // acquire is unlikely to re-pick it).
                self.key_pool
                    .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                    .await;
                if !is_last_attempt {
                    return Ok(AttemptResponse::Retry);
                }
                warn!(
                    key = %mask_key(&key_str),
                    attempt_limit = self.retry_config.max_retries,
                    "Retry budget exhausted; passing 401 through"
                );
                self.metrics.inc_error();
                // Fall through: deliver this response.
            }
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::FORBIDDEN
        {
            // 429 and 403 are deterministic upstream decisions
            // (rate limit / forbidden). Honor Retry-After if present, else
            // surface the response as-is.
            if let Some(ra) = retry_after {
                warn!(
                    key = %mask_key(&key_str),
                    status = %status,
                    retry_after = ra,
                    "Upstream {} with Retry-After; cooling key",
                    status.as_u16()
                );
                self.key_pool.mark_bad(&key_str, Some(ra)).await;
            } else {
                // 403 "Authorization failed" is an account/region-level ban,
                // not a transient hiccup — cool the key immediately rather
                // than waiting for `auth_fail_threshold` failures to accrue.
                // 429 without Retry-After, and other 403 variants, still use
                // `mark_failure` (decrease weight, disable after threshold).
                let body_str = std::str::from_utf8(&body_bytes).unwrap_or("");
                let is_auth_ban = status == reqwest::StatusCode::FORBIDDEN
                    && body_str.contains("Authorization failed");
                if is_auth_ban {
                    warn!(
                        key = %mask_key(&key_str),
                        "Upstream 403 Authorization failed; immediate cooldown"
                    );
                    self.key_pool.mark_bad(&key_str, None).await;
                    self.metrics.inc_key_403_auth_cooldown();
                } else {
                    warn!(
                        key = %mask_key(&key_str),
                        status = %status,
                        "Upstream {} without Retry-After; decreasing weight, may disable after repeated failures",
                        status.as_u16()
                    );
                    // Record failure and potentially disable the key after threshold.
                    self.key_pool
                        .mark_failure(&key_str, self.retry_config.auth_fail_threshold)
                        .await;
                }
            }
            self.metrics.inc_error();
        } else {
            // Other 4xx/5xx: surface as-is, no key penalty.
            self.metrics.inc_error();
        }

        // Token statistics for successful Chat Completion responses are
        // collected when we already buffered the body — i.e. only on the
        // non-success paths below, which never carry usage. Successful
        // responses are forwarded as a stream and we don't buffer the body,
        // so nothing to record here. If usage-on-stream is needed later,
        // wrap the stream in a tap that parses the final chunk.

        // ── Structured failure log (failure.log JSONL) ───────────────────
        // Single hook covering all non-2xx upstream outcomes. Key facts we
        // want surfaced for diagnosis: status, model (when present), masked
        // key, upstream URL, retry attempt, and a short body excerpt that
        // lets us classify NVIDIA's "Function 'uuid': Not found for account
        // 'xxxx'" 404s, rate-limit JSON, or empty/error bodies.
        if !status.is_success() {
            let body_excerpt = {
                let s = std::str::from_utf8(&body_bytes).unwrap_or("");
                // Take first 512 chars; bodies are short error JSON.
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
            // detail: include function_id / account_id when present so jq
            // queries can group by them without re-parsing the body.
            let detail = if !function_id.is_empty() || !account_id.is_empty() {
                format!("function_id={} account_id={}", function_id, account_id)
            } else {
                String::new()
            };
            // endpoint = path portion of upstream URL (after base_url)
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

        // Normalize numeric `error.code` to a string. NVIDIA returns
        // {"error":{"code":503,...}} (number), but OpenAI's schema and Go
        // clients like Cursor expect `code` as a string. Applied
        // unconditionally to every buffered body — success responses
        // without a `code` field are a no-op.
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

/// Handle passthrough streaming: forward the upstream SSE/byte stream
/// to the client. Each chunk passes through `coerce_sse_chunk` which
/// rewrites numeric `error.code` values to strings in `data:` lines;
/// non-`data:` bytes and chunks without a `code` field are forwarded
/// verbatim. A `UsageTap` wraps the stream so token usage is accounted for.
async fn handle_passthrough_stream(
    &self,
    resp: reqwest::Response,
    resp_headers: Vec<(String, Vec<u8>)>,
    status: warp::http::StatusCode,
) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
    let upstream = resp.bytes_stream();
    let forwarded: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        Box::pin(upstream.map(|r| {
            r.map(|chunk| coerce_sse_chunk(&chunk)).map_err(|e| {
                warn!("Passthrough stream read error: {}", e);
                std::io::Error::other(e.to_string())
            })
        }));
    let tapped: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        Box::pin(UsageTap {
            inner: forwarded,
            metrics: self.metrics.clone(),
            carry: String::new(),
            usage: None,
        });

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

        let body = warp::hyper::Body::wrap_stream(tapped);
        Ok(response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        }))
    }
}

// Custom rejections
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
// NOTE: A `BadRequest` reject type used to live here for the hard-400
// validation paths (`max_tokens` / `reasoning_effort`). Those paths were
// converted to in-place body normalization in 2026-07, so the reject
// type is no longer emitted by the handler. If a future hard-400 case
// is needed, re-introduce `struct BadRequest { msg: String }` with
// `impl Reject` + `impl Reply` (mapping to BAD_REQUEST).

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

    // Process-global guard: set once so no integration test in this module
    // writes to the on-disk failure.log used for production diagnosis.
    static DISABLE_FAILURE_LOG: std::sync::Once = std::sync::Once::new();

    fn test_config(base_url: &str, max_retries: u32, request_timeout: u64) -> Config {
        DISABLE_FAILURE_LOG.call_once(|| {
            // SAFETY: tests in this module run in a single process; the env
            // var is process-global and only guards failure-log writes.
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
            config.retry.clone(),
            config.rate_limiter.token_wait_ms,
            config.model_aliases.clone(),
            true,
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

    // ── Unsupported API shape rejection (mod ①+②) ────────────────────────

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

    // ── /v1/models passthrough + cache (mod ③) ───────────────────────────

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
            // Only one upstream call: the 2nd client request must hit the cache.
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

    // ── stream_options.include_usage injection (mod ④) ───────────────────

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

        // Only the initial attempt + 1 retry, with no backoff before the final
        // attempt. Give a generous margin to avoid flakiness.
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

    /// Non-canonical `reasoning_effort` strings (e.g. "max") are silently
    /// dropped and forwarded — NVIDIA would 400 on them, so dropping lets the
    /// request succeed without making the client rewrite their body.
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

    /// Boolean `reasoning_effort: true` is rewritten to "high" and forwarded,
    /// rather than being rejected. cc-switch / OpenAI o1-style clients send
    /// `true`/`false` and expect the proxy to accept them.
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

    /// Valid `reasoning_effort` values are forwarded normally.
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
}

#[cfg(test)]
mod usage_accounting_tests {
    use super::*;

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

    /// Drive `UsageTap::feed` directly and confirm it extracts the last `usage`
    /// seen, including when the `data:` line is split across two chunks.
    #[test]
    fn usage_tap_feed_captures_usage_across_chunks() {
        let mut tap = UsageTap {
            inner: futures_util::stream::empty(),
            metrics: Arc::new(Metrics::new()),
            carry: String::new(),
            usage: None,
        };
        // First chunk: a token delta (no usage) + a partial data line.
        tap.feed(&Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":7,",
        ));
        assert!(tap.usage.is_none(), "usage not yet complete");
        // Second chunk completes the split data line, then a final usage event.
        tap.feed(&Bytes::from_static(
            b"\"completion_tokens\":3,\"total_tokens\":10}}\n\n",
        ));
        let u = tap.usage.expect("usage captured");
        assert_eq!(u.prompt, Some(7));
        assert_eq!(u.completion, Some(3));
        assert_eq!(u.total, Some(10));
    }
}
