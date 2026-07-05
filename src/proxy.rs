use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use warp::{self, Rejection, Reply};

use crate::key_pool::{mask_key, KeyPool};
use crate::metrics::Metrics;
use crate::responses_compat;
use futures_util::{Stream, StreamExt};
use reqwest::header::{self as req_header, HeaderValue as ReqHeaderValue};
use std::pin::Pin;

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
            let (_translated_path, translated_body) = responses_compat::translate_request(&body);
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
                start,
                is_last_attempt,
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
                Ok(AttemptResponse::Done(resp)) => return Ok(resp),
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
        start: std::time::Instant,
        is_last_attempt: bool,
    ) -> Result<AttemptResponse, AttemptError> {
        let key_str = key_handle.key().await;
        let req_builder = self.build_upstream_request(url, method, headers, request_body, &key_str);
        let resp = req_builder.send().await.map_err(AttemptError::Network)?;

        let duration = start.elapsed();
        self.metrics.observe_latency(duration.as_secs_f64());

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
                    "Upstream {} without Retry-After; not cooling key, passing through",
                    status.as_u16()
                );
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
        let final_body = if is_responses && status.is_success() {
            let translated = responses_compat::translate_response(&body_bytes, model);
            Bytes::from(translated)
        } else {
            body_bytes
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
#[derive(Debug)]
struct ServiceUnavailable {
    msg: String,
}
#[derive(Debug)]
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
