use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};
use warp::{self, Rejection, Reply};

use crate::failure_log;
use crate::key_pool::{mask_key, KeyPool};
use crate::metrics::Metrics;
use futures_util::{Stream, StreamExt};
use reqwest::header::{self as req_header, HeaderValue as ReqHeaderValue};
use std::pin::Pin;

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

impl Proxy {
    pub fn new(
        base_url: String,
        key_pool: Arc<KeyPool>,
        metrics: Arc<Metrics>,
        request_timeout_secs: u64,
        retry_config: crate::config::RetryConfig,
        token_wait_ms: u64,
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
            .connect_timeout(Duration::from_secs(10))
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
        }
    }

    /// Build an upstream reqwest request for the given client path/method/body
    /// and authorize with `key_str`. Hop-by-hop and client-supplied auth
    /// headers are stripped. The returned builder is ready to `.send()`.
    fn build_upstream_request(
        &self,
        url: &str,
        method: &warp::http::Method,
        headers: &warp::http::HeaderMap,
        body: &bytes::Bytes,
        key_str: &str,
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
                | "authorization" => continue,
                _ => {}
            }
            if let Ok(req_val) = ReqHeaderValue::from_bytes(hvalue.as_bytes()) {
                req_builder = req_builder.header(key_name, req_val);
            }
        }
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

        // ── Acquire a key with token-starvation switch loop ─────────────
        let max_switches = self.retry_config.max_retries as usize;
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
        let parsed_body: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
        let body_summary = body_summary(&parsed_body);

        // Validate `max_tokens` for chat completions: must be a positive
        // integer when supplied. NVIDIA rejects non-positive values with a
        // 400 which wastes a request, so we short-circuit here.
        if let Some(v) = &parsed_body {
            if let Some(max_val) = v.get("max_tokens") {
                let is_valid = match max_val {
                    serde_json::Value::Number(num) => num.as_i64().is_some_and(|n| n >= 1),
                    serde_json::Value::String(s) => s.parse::<i64>().is_ok_and(|n| n >= 1),
                    _ => false,
                };
                if !is_valid {
                    warn!(client_req_id = %client_req_id, "Invalid max_tokens value: {}", max_val);
                    self.metrics.inc_error();
                    key_handle.dec_in_flight();
                    return Err(warp::reject::custom(BadRequest {
                        msg: "Invalid max_tokens (must be >= 1)".into(),
                    }));
                }
            }
        }

        // Extract `model` once from the client's request body for logging.
        let model = parsed_body
            .as_ref()
            .and_then(|v| {
                v.get("model")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();

        // Passthrough: use original path and body. Honor the client's
        // `stream` field so chat-completions clients that ask for streaming
        // get a streamed body (no `bytes().await` buffering).
        let upstream_path = path.clone();
        let request_body = body.clone();
        let is_streaming = parsed_body
            .as_ref()
            .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
            .unwrap_or(false);

        // Build upstream URL
        let mut url = self.base_url.clone();
        url.push_str(&upstream_path);

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
        let req_builder = self.build_upstream_request(url, method, headers, request_body, &key_str);
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

        // Buffer the body once. All non-SSE paths (success, 4xx, 5xx) use
        // this owned `body_bytes` for classification, failure logging, and
        // the final response body.
        let body_bytes = resp.bytes().await.map_err(AttemptError::BodyRead)?;

        // `resp.headers()` is no longer accessible after `resp.bytes()`
        // consumed `resp`; resolve Retry-After from the cloned `resp_headers`
        // snapshot once, here, for all 401/429/403 branches to reuse.
        let retry_after: Option<u64> = resp_headers
            .iter()
            .find(|(k, _)| k == req_header::RETRY_AFTER.as_str())
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok());

        if status.is_success() {
            self.key_pool.mark_good(&key_str).await;
            self.metrics.inc_success();
        } else if status == reqwest::StatusCode::UNAUTHORIZED {
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

        // Token statistics (prompt/completion/total) for successful Chat Completion responses.
        if status.is_success() {
            if let Ok(json_body) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                if let Some(usage) = json_body.get("usage") {
                    if let Some(prompt) = usage.get("prompt_tokens").and_then(|v| v.as_u64()) {
                        crate::metrics::token_input().inc_by(prompt as f64);
                    }
                    if let Some(completion) =
                        usage.get("completion_tokens").and_then(|v| v.as_u64())
                    {
                        crate::metrics::token_output().inc_by(completion as f64);
                    }
                    if let (Some(p), Some(c)) = (
                        usage.get("prompt_tokens").and_then(|v| v.as_u64()),
                        usage.get("completion_tokens").and_then(|v| v.as_u64()),
                    ) {
                        crate::metrics::token_total().inc_by((p + c) as f64);
                    }
                }
            }
        }

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
    /// verbatim.
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

        let body = warp::hyper::Body::wrap_stream(forwarded);
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
#[derive(Debug, Clone)]
struct BadRequest {
    msg: String,
}

impl warp::reject::Reject for ServiceUnavailable {}
impl warp::reject::Reject for BadGateway {}
impl warp::reject::Reject for BadRequest {}

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
impl Reply for BadRequest {
    fn into_response(self) -> warp::reply::Response {
        warp::reply::with_status(self.msg, warp::http::StatusCode::BAD_REQUEST).into_response()
    }
}

#[cfg(test)]
mod proxy_integration_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use warp::{Filter, Rejection, Reply};
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use crate::config::{
        Config, CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
        RetryConfig,
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
            },
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
        if let Some(e) = err.find::<super::BadRequest>().cloned() {
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
}
