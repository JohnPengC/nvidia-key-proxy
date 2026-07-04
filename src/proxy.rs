use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use warp::{self, Rejection, Reply};
use tracing::{error, warn, info};

use crate::key_pool::{KeyPool, mask_key};
use crate::metrics::Metrics;
use crate::responses_compat;
use reqwest::header::{self as req_header, HeaderValue as ReqHeaderValue};

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
}

impl Proxy {
    pub fn new(
        base_url: String,
        key_pool: Arc<KeyPool>,
        metrics: Arc<Metrics>,
        request_timeout_secs: u64,
    ) -> Self {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .expect("Failed to build reqwest client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            key_pool,
            metrics,
            client,
            request_timeout_secs,
        }
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
        let key_handle = match self.key_pool.acquire().await {
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
        // symmetry of translation is preserved (see review concern #10).
        let is_responses_path = responses_compat::is_responses_path(&path);
        let body_is_responses_format = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .map(|v| v.get("input").is_some() && v.get("messages").is_none())
            .unwrap_or(false);
        let is_responses = is_responses_path || body_is_responses_format;

        // Determine upstream path and request body.
        let upstream_path;
        let request_body;
        let is_streaming;

        if is_responses {
            let (_translated_path, translated_body) =
                responses_compat::translate_request(&body);
            upstream_path = "/v1/chat/completions".to_string();
            request_body = Bytes::from(translated_body);
            // Codex's Responses API defaults to streaming.
            is_streaming = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
                .unwrap_or(true);
        } else {
            // Passthrough: use original path and body, no streaming assumption.
            upstream_path = path.clone();
            request_body = body.clone();
            is_streaming = false;
        }

        // Build upstream URL
        let mut url = self.base_url.clone();
        let path_str = upstream_path.as_str();
        if !path_str.is_empty() {
            url.push_str(path_str);
        }

        if is_responses {
            info!("Responses API → Chat Completions: {} {}", method.as_str(), url);
        } else {
            info!("Proxying request to: {} {}", method.as_str(), url);
        }

        // Build request with headers from client (excluding hop-by-hop and auth)
        let req_method = method;
        let mut req_builder = self.client.request(req_method, &url);

        let key_str = key_handle.key().await;
        req_builder = req_builder
            .header(req_header::AUTHORIZATION, format!("Bearer {}", key_str))
            .header(req_header::USER_AGENT, "nvidia-key-proxy/0.1");

        for (key, value) in headers.iter() {
            let key_name = key.as_str();
            match key_name {
                "connection" | "keep-alive" | "proxy-authenticate"
                | "proxy-authorization" | "te" | "trailers" | "transfer-encoding"
                | "upgrade" | "content-length" | "host" | "authorization" => continue,
                _ => {}
            }
            if let Ok(req_val) = ReqHeaderValue::from_bytes(value.as_bytes()) {
                req_builder = req_builder.header(key_name, req_val);
            }
        }

        if !request_body.is_empty() {
            req_builder = req_builder.body(request_body);
        }

        // Send request. For streaming requests we never wrap in a hard timeout
        // — long-running token generation can exceed 60s. For non-streaming
        // requests we honor request_timeout_secs (0 disables the timeout).
        let send_future = req_builder.send();
        let resp_result = if is_streaming || self.request_timeout_secs == 0 {
            send_future.await
        } else {
            match tokio::time::timeout(
                Duration::from_secs(self.request_timeout_secs),
                send_future,
            ).await {
                Ok(r) => r,
                Err(_) => {
                    let k = key_handle.key().await;
                    warn!(key = %mask_key(&k), "Upstream request timeout");
                    self.key_pool.mark_bad(&k, None).await;
                    self.metrics.inc_error();
                    return Err(warp::reject::custom(BadGateway {
                        msg: "Upstream request timed out".into(),
                    }));
                }
            }
        };

        let duration = start.elapsed();
        self.metrics.observe_latency(duration.as_secs_f64());

        match resp_result {
            Err(e) => {
                let k = key_handle.key().await;
                warn!(key = %mask_key(&k), "Request send error: {}", e);
                self.key_pool.mark_bad(&k, None).await;
                self.metrics.inc_error();
                Err(warp::reject::custom(BadGateway {
                    msg: "Upstream request failed".into(),
                }))
            }
            Ok(resp) => {
                let status = resp.status();
                info!("Upstream response: {} for URL: {}", status, url);

                let content_type = resp
                    .headers()
                    .get(req_header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let is_sse = content_type.contains("text/event-stream");

                if status.is_success() {
                    let k = key_handle.key().await;
                    self.key_pool.mark_good(&k).await;
                    self.metrics.inc_success();
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::FORBIDDEN
                    || status == reqwest::StatusCode::UNAUTHORIZED
                {
                    let retry_after = resp
                        .headers()
                        .get(req_header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok());
                    let k = key_handle.key().await;
                    self.key_pool
                        .mark_bad(&k, retry_after)
                        .await;
                    self.metrics.inc_error();
                } else {
                    // treat other errors as transient, not penalizing key
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
                    return self.handle_streaming_response(resp, resp_headers, status).await;
                }

                match resp.bytes().await {
                    Ok(body_bytes) => {
                        // Translate chat completions → Responses API whenever
                        // we translated the request. This keeps request and
                        // response translation symmetric (concern #10).
                        let final_body = if is_responses && status.is_success() {
                            let translated = responses_compat::translate_response(
                                &body_bytes,
                                "",
                            );
                            Bytes::from(translated)
                        } else {
                            body_bytes
                        };

                        let mut response_builder = warp::http::Response::builder()
                            .status(status);

                        for (key, value) in resp_headers {
                            match key.as_str() {
                                "connection" | "keep-alive" | "proxy-authenticate"
                                | "proxy-authorization" | "te" | "trailers" | "transfer-encoding"
                                | "upgrade" => continue,
                                _ => {}
                            }
                            // For translated Responses responses, override content-type.
                            if is_responses && status.is_success() && key == "content-type" {
                                continue;
                            }
                            if let Ok(hv) = warp::http::HeaderValue::from_bytes(&value) {
                                if let Some(headers) = response_builder.headers_mut() {
                                    let key_bytes = key.as_bytes().to_vec();
                                    let hname = warp::http::HeaderName::from_bytes(&key_bytes).unwrap_or(
                                        warp::http::HeaderName::from_bytes(b"x-unknown").unwrap()
                                    );
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

                        Ok(response_builder
                            .body(final_body)
                            .unwrap_or_else(|_| warp::http::Response::new(Bytes::new())))
                    }
                    Err(e) => {
                        let k = key_handle.key().await;
                        error!(key = %mask_key(&k), "Failed to read response body: {}", e);
                        Err(warp::reject::custom(BadGateway {
                            msg: "Failed to read response body".into(),
                        }))
                    }
                }
            }
        }
    }

    /// Handle streaming SSE response: read NVIDIA chat completions SSE,
    /// translate to Responses API SSE events, stream back to client.
    async fn handle_streaming_response(
        &self,
        resp: reqwest::Response,
        resp_headers: Vec<(String, Vec<u8>)>,
        status: warp::http::StatusCode,
    ) -> Result<warp::http::Response<Bytes>, Rejection> {
        use futures_util::StreamExt;

        let mut stream_state = responses_compat::StreamState::new();

        // Collect all translated SSE events, then return as a single body.
        // (A true streaming response would use hyper::Body::wrap_stream, but
        // warp's Reply type makes that complex. For now we buffer and return.)
        let mut all_events = Vec::new();

        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    buffer.push_str(&String::from_utf8_lossy(&chunk));

                    // Process complete SSE events (separated by \n\n)
                    while let Some(pos) = buffer.find("\n\n") {
                        let event_block = buffer[..pos].to_string();
                        buffer = buffer[pos + 2..].to_string();

                        // Parse "data: ..." line
                        if let Some(data_line) = event_block
                            .lines()
                            .find(|l| l.starts_with("data: "))
                        {
                            let data = &data_line[6..];

                            if data == "[DONE]" {
                                let events =
                                    responses_compat::translate_stream_chunk(
                                        b"null",
                                        &mut stream_state,
                                        "",
                                    );
                                all_events.extend(events);
                            } else {
                                let events =
                                    responses_compat::translate_stream_chunk(
                                        data.as_bytes(),
                                        &mut stream_state,
                                        "",
                                    );
                                all_events.extend(events);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Stream read error: {}", e);
                    break;
                }
            }
        }

        // Emit response.completed if not already done
        if !stream_state.emitted_completed {
            let events = responses_compat::translate_stream_chunk(
                b"null",
                &mut stream_state,
                "",
            );
            all_events.extend(events);
        }

        let body = all_events.join("");
        info!(
            "Streaming translation complete: {} events, {} bytes",
            all_events.len(),
            body.len()
        );

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
                "connection" | "keep-alive" | "proxy-authenticate"
                | "proxy-authorization" | "te" | "trailers" | "transfer-encoding"
                | "upgrade" | "content-type" | "cache-control" => continue,
                _ => {}
            }
            if let Ok(hv) = warp::http::HeaderValue::from_bytes(&value) {
                if let Some(headers) = response_builder.headers_mut() {
                    let key_bytes = key.as_bytes().to_vec();
                    let hname = warp::http::HeaderName::from_bytes(&key_bytes)
                        .unwrap_or_else(|_| {
                            warp::http::HeaderName::from_bytes(b"x-unknown").unwrap()
                        });
                    headers.append(hname, hv);
                }
            }
        }

        Ok(response_builder
            .body(Bytes::from(body))
            .unwrap_or_else(|_| warp::http::Response::new(Bytes::new())))
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
        warp::reply::with_status(
            self.msg,
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        ).into_response()
    }
}
impl Reply for BadGateway {
    fn into_response(self) -> warp::reply::Response {
        warp::reply::with_status(
            self.msg,
            warp::http::StatusCode::BAD_GATEWAY,
        ).into_response()
    }
}
