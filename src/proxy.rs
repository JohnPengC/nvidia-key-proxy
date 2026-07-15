use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};
use warp::{self, Rejection, Reply};

use crate::failure_log;
use crate::key_pool::{mask_key, KeyPool};
use crate::metrics::Metrics;
use crate::responses_compat;
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

impl Proxy {
    pub fn new(
        base_url: String,
        key_pool: Arc<KeyPool>,
        metrics: Arc<Metrics>,
        request_timeout_secs: u64,
        retry_config: crate::config::RetryConfig,
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

        // Acquire a key
        let mut key_handle = match self.key_pool.acquire().await {
            Some(k) => k,
            None => {
                self.metrics.inc_error();
                return Err(warp::reject::custom(ServiceUnavailable {
                    msg: "No available API keys".into(),
                }));
            }
        };

        // Consume a token (rate limit)
        key_handle.consume_token().await;

        // Detect whether the request needs Responses → Chat Completions
        // translation. We translate when EITHER:
        //   1. The path hits /v1/responses (direct Responses API call), OR
        //   2. The body is a Responses-style request (has `input`, no
        //      `messages`) even when the client forwards to /v1/chat/completions
        //      without translating the body (cc-switch issue #2806).
        // In both cases we translate the request *and* the response, so the
        // symmetry of translation is preserved.
        let parsed_body: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
        let is_responses_path = responses_compat::is_responses_path(&path);
        let body_is_responses_format = parsed_body
            .as_ref()
            .map(|v| v.get("input").is_some() && v.get("messages").is_none())
            .unwrap_or(false);
        let is_responses = is_responses_path || body_is_responses_format;
        // Validation for chat completions (non‑Responses) requests: ensure
        // `max_tokens` is a positive integer when supplied.
        if !is_responses {
            if let Some(v) = &parsed_body {
                if let Some(max_val) = v.get("max_tokens") {
                    let is_valid = match max_val {
                        serde_json::Value::Number(num) => num.as_i64().map_or(false, |n| n >= 1),
                        serde_json::Value::String(s) => s.parse::<i64>().map_or(false, |n| n >= 1),
                        _ => false,
                    };
                    if !is_valid {
                        warn!("Invalid max_tokens value: {}", max_val);
                        self.metrics.inc_error();
                        return Err(warp::reject::custom(BadRequest {
                            msg: "Invalid max_tokens (must be >= 1)".into(),
                        }));
                    }
                }
            }
        }

        // Extract `model` once from the client's request body so it can be
        // propagated into translated Responses events (response.created /
        // response.completed).
        let model = parsed_body
            .as_ref()
            .and_then(|v| {
                v.get("model")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();

        // Determine upstream path and request body.
        let (upstream_path, request_body, is_streaming);

        if is_responses {
            let (_translated_path, translated_body) =
                match responses_compat::translate_request(&body) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("Failed to translate Responses request: {}", e);
                        self.metrics.inc_error();
                        return Err(warp::reject::custom(BadRequest {
                            msg: "Invalid Responses request body".into(),
                        }));
                    }
                };
            upstream_path = "/v1/chat/completions".to_string();
            request_body = Bytes::from(translated_body);
            // Codex's Responses API defaults to streaming.
            is_streaming = parsed_body
                .as_ref()
                .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
                .unwrap_or(true);
        } else {
            // Passthrough: use original path and body. Honor the client's
            // `stream` field so chat-completions clients that ask for streaming
            // get a streamed body (no `bytes().await` buffering).
            upstream_path = path.clone();
            request_body = body.clone();
            is_streaming = parsed_body
                .as_ref()
                .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
                .unwrap_or(false);
        }

        // Build upstream URL
        let mut url = self.base_url.clone();
        url.push_str(&upstream_path);

        if is_responses {
            info!(
                "Responses API → Chat Completions: {} {}",
                method.as_str(),
                url
            );
        } else {
            info!("Proxying request to: {} {}", method.as_str(), url);
        }

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
                is_responses,
                is_streaming,
                &model,
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
                    self.metrics.observe_latency(start.elapsed().as_secs_f64());
                    return Ok(resp);
                }
                Ok(AttemptResponse::Retry) => {
                    // 401 without Retry-After: caller still has retry budget.
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            key_handle = next;
                            key_handle.consume_token().await;
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(attempt, "Retrying after upstream 401 with a different key");
                            continue;
                        }
                        warn!("No other keys available; passing 401 through");
                    }
                    // Defensive fallback (helper should have returned Done on last attempt).
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
                        attempt,
                        next_req_id(),
                        Some("network"),
                    );
                    self.key_pool.mark_bad(&k, None).await;
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            key_handle = next;
                            key_handle.consume_token().await;
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(attempt, "Retrying upstream send with a different key");
                            continue;
                        }
                    }
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
                        attempt,
                        next_req_id(),
                        Some("timeout"),
                    );
                    if attempt < self.retry_config.max_retries {
                        if let Some(next) = self.key_pool.acquire().await {
                            let will_retry_again = attempt + 1 < self.retry_config.max_retries;
                            self.metrics.inc_retry_attempt_failure();
                            key_handle = next;
                            key_handle.consume_token().await;
                            attempt += 1;
                            if will_retry_again {
                                self.backoff_sleep(attempt).await;
                            }
                            warn!(
                                attempt,
                                "Retrying after upstream timeout with a different key"
                            );
                            continue;
                        }
                    }
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
                        attempt,
                        next_req_id(),
                        Some("body_read"),
                    );
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Failed to read response body".into(),
                    }));
                }
                Err(AttemptError::Fatal(r)) => return Err(r),
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
        is_responses: bool,
        is_streaming: bool,
        model: &str,
        is_last_attempt: bool,
        attempt: u32,
    ) -> Result<AttemptResponse, AttemptError> {
        let key_str = key_handle.key().await;
        let req_builder = self.build_upstream_request(url, method, headers, request_body, &key_str);
        let resp = req_builder.send().await.map_err(AttemptError::Network)?;

        let status = resp.status();
        info!(status = %status, url = %url, "Upstream response");

        let content_type = resp
            .headers()
            .get(req_header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let is_sse = content_type.contains("text/event-stream");

        if status.is_success() {
            self.key_pool.mark_good(&key_str).await;
            self.metrics.inc_success();
        } else if status == reqwest::StatusCode::UNAUTHORIZED {
            // 401 is the only status we retry: it's the clearest
            // "this specific key is rejected" signal.
            let retry_after = resp
                .headers()
                .get(req_header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
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
            let retry_after = resp
                .headers()
                .get(req_header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
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
            self.metrics.inc_error();
        } else {
            // Other 4xx/5xx: surface as-is, no key penalty.
            self.metrics.inc_error();
        }

        // Clone headers before consuming the body
        let resp_headers: Vec<(String, Vec<u8>)> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
            .collect();

        // Streaming SSE translation: only when we are translating a
        // Responses API request (request + response).
        if is_responses && is_sse && status.is_success() {
            let resp = self
                .handle_streaming_response(resp, resp_headers, status, model)
                .await
                .map_err(AttemptError::Fatal)?;
            return Ok(AttemptResponse::Done(resp));
        }

        // Passthrough streaming: when the client asked for `stream` and
        // upstream responded with SSE, forward the upstream stream
        // verbatim without buffering.
        if !is_responses && is_streaming && is_sse && status.is_success() {
            let resp = self
                .handle_passthrough_stream(resp, resp_headers, status)
                .await
                .map_err(AttemptError::Fatal)?;
            return Ok(AttemptResponse::Done(resp));
        }

        let body_bytes = resp.bytes().await.map_err(AttemptError::BodyRead)?;

        // Token statistics (prompt/completion/total) for successful Chat Completion responses.
        // We only count when the request is a plain Chat Completion (not a Responses↔Chat translation).
        if status.is_success() && !is_responses {
            if let Ok(json_body) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                if let Some(usage) = json_body.get("usage") {
                    if let Some(prompt) = usage.get("prompt_tokens").and_then(|v| v.as_u64()) {
                        crate::metrics::token_input().inc_by(prompt as f64);
                    }
                    if let Some(completion) = usage.get("completion_tokens").and_then(|v| v.as_u64()) {
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
                format!(
                    "function_id={} account_id={}",
                    function_id, account_id
                )
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
                attempt,
                next_req_id(),
                None,
            );
        }

        let final_body = if is_responses && status.is_success() {
            let translated = responses_compat::translate_response(&body_bytes, model);
            Bytes::from(translated)
        } else {
            // For non-success responses, ensure error JSON fields have expected types.
            // NVIDIA may return {"error":{"code":403,...}} where `code` is a number.
            // Some clients (e.g., Go SDK) expect `code` as a string.
            // We coerce numeric codes to strings to improve compatibility.
            if let Ok(mut err_json) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                if let Some(error_obj) = err_json.get_mut("error") {
                    if let Some(code_val) = error_obj.get_mut("code") {
                        if code_val.is_number() {
                            if let Some(num) = code_val.as_i64() {
                                *code_val = serde_json::Value::String(num.to_string());
                            } else if let Some(u) = code_val.as_u64() {
                                *code_val = serde_json::Value::String(u.to_string());
                            }
                        }
                    }
                    if let Ok(new_bytes) = serde_json::to_vec(&err_json) {
                        Bytes::from(new_bytes)
                    } else {
                        body_bytes
                    }
                } else {
                    body_bytes
                }
            } else {
                body_bytes
            }
        };

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
                | "upgrade" => continue,
                _ => {}
            }
            if is_responses && status.is_success() && key == "content-type" {
                continue;
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

        if is_responses && status.is_success() {
            if let Some(headers) = response_builder.headers_mut() {
                headers.insert(
                    warp::http::header::CONTENT_TYPE,
                    warp::http::HeaderValue::from_static("application/json"),
                );
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

    /// Handle streaming SSE response: read NVIDIA chat completions SSE,
    /// translate to Responses API SSE events, and stream them back to the
    /// client as they are produced (no buffering).
    ///
    /// Returns a hyper::Body backed by `Body::wrap_stream`. If the upstream
    /// stream errors mid-way, the response is truncated and we deliberately
    /// do NOT emit `response.completed`, so Codex sees an unfinished response
    /// rather than a silent success.
    async fn handle_streaming_response(
        &self,
        resp: reqwest::Response,
        resp_headers: Vec<(String, Vec<u8>)>,
        status: warp::http::StatusCode,
        model: &str,
    ) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
        use std::collections::VecDeque;
        use std::task::{Context, Poll};

        // Build the upstream chunk stream once (bytes_stream() consumes the
        // reqwest::Response by value) and store it inside our poll state.
        let upstream = resp.bytes_stream();
        let upstream: std::pin::Pin<
            Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>,
        > = Box::pin(upstream);

        // `model` is captured by reference into the poll closure; clone it to a
        // String so the stream is `'static` and `Send` without borrowing `&str`
        // from the enclosing stack frame.
        let model = model.to_string();

        // Streaming state machine shared across polls.
        struct St {
            upstream: std::pin::Pin<
                Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>,
            >,
            state: responses_compat::StreamState,
            buffer: String,
            pending: std::collections::VecDeque<Bytes>,
            upstream_done: bool,
        }

        let mut st = St {
            upstream,
            state: responses_compat::StreamState::new(),
            buffer: String::new(),
            pending: VecDeque::new(),
            upstream_done: false,
        };

        // poll_fn captures `st` and `model` by move; each poll drives the
        // state machine and returns one translated event at a time.
        let translated: std::pin::Pin<
            Box<dyn futures_util::stream::Stream<Item = Result<Bytes, std::io::Error>> + Send>,
        > = Box::pin(futures_util::stream::poll_fn(
            move |cx: &mut Context<'_>| -> Poll<Option<Result<Bytes, std::io::Error>>> {
                loop {
                    // 1. Drain already-translated events.
                    if let Some(ev) = st.pending.pop_front() {
                        return Poll::Ready(Some(Ok(ev)));
                    }

                    // 2. Extract any complete SSE event from the buffer.
                    while let Some(pos) = st.buffer.find("\n\n") {
                        let event_block = st.buffer[..pos].to_string();
                        st.buffer = st.buffer[pos + 2..].to_string();
                        if let Some(data_line) =
                            event_block.lines().find(|l| l.starts_with("data: "))
                        {
                            let data = &data_line[6..];
                            let payload: &[u8] = if data == "[DONE]" {
                                b"null"
                            } else {
                                data.as_bytes()
                            };
                            let events = responses_compat::translate_stream_chunk(
                                payload,
                                &mut st.state,
                                &model,
                            );
                            for ev in events {
                                st.pending.push_back(Bytes::from(ev));
                            }
                        }
                        // Re-loop to drain the newly enqueued events.
                        continue;
                    }

                    // 3. Need more bytes from upstream.
                    if st.upstream_done {
                        return Poll::Ready(None);
                    }
                    match st.upstream.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => {
                            // Upstream ended cleanly. Emit response.completed
                            // if we haven't already, then signal end of stream.
                            st.upstream_done = true;
                            if !st.state.emitted_completed {
                                let events = responses_compat::translate_stream_chunk(
                                    b"null",
                                    &mut st.state,
                                    &model,
                                );
                                for ev in events {
                                    st.pending.push_back(Bytes::from(ev));
                                }
                                continue;
                            }
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(Ok(chunk))) => {
                            st.buffer.push_str(&String::from_utf8_lossy(&chunk));
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            warn!("Stream read error: {}", e);
                            // Upstream error mid-stream: truncate the response.
                            // We deliberately do NOT emit `response.completed`
                            // so the client can recognize this as an unfinished
                            // response rather than a silent success.
                            return Poll::Ready(None);
                        }
                    }
                }
            },
        ));

        // Build headers, then wrap the stream into a hyper body.
        let mut response_builder = warp::http::Response::builder().status(status);
        if let Some(headers) = response_builder.headers_mut() {
            headers.insert(
                warp::http::header::CONTENT_TYPE,
                warp::http::HeaderValue::from_static("text/event-stream"),
            );
            headers.insert(
                warp::http::header::CACHE_CONTROL,
                warp::http::HeaderValue::from_static("no-cache"),
            );
        }
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
                | "content-type"
                | "cache-control" => continue,
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

        let body = warp::hyper::Body::wrap_stream(translated);
        Ok(response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        }))
    }

    /// Handle passthrough streaming: forward the upstream SSE/byte stream
    /// verbatim to the client. No translation, no buffering.
    async fn handle_passthrough_stream(
        &self,
        resp: reqwest::Response,
        resp_headers: Vec<(String, Vec<u8>)>,
        status: warp::http::StatusCode,
    ) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
        let upstream = resp.bytes_stream();
        let forwarded: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(upstream.map(|r| {
                r.map_err(|e| {
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
                | "upgrade" => continue,
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

    use crate::codex_config::CodexConfig;
    use crate::config::{
        Config, CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
        RetryConfig,
    };
    use crate::key_pool::KeyPool;
    use crate::metrics::Metrics;
    use crate::proxy::Proxy;

    fn test_config(base_url: &str, max_retries: u32, request_timeout: u64) -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(),
            base_url: base_url.to_string(),
            keys: vec!["fake-key".to_string()],
            rate_limiter: RateLimiterConfig {
                qps_per_key: 1000.0,
                burst: 1000,
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
            codex: CodexConfig::default(),
            request_timeout_seconds: request_timeout,
            cursor: None,
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
}
