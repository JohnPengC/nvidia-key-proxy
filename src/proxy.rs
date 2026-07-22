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
    /// Synthesize a placeholder `choices` entry when a non-stream 2xx body
    /// arrives with an empty `choices` array, so downstream consumers (notably
    /// cc-switch's `openai_to_anthropic`) don't hard-error on the empty
    /// array. See `Config::synthesize_empty_choices` for the full rationale.
    /// Only the non-streaming non-SSE success path is affected, and only a
    /// bounded prefix is ever inspected.
    synthesize_empty_choices: bool,
    /// Retry once with a clamped `max_tokens` when NVIDIA returns an empty-shell
    /// 2xx soft failure (`choices:[]` with a null `usage` and empty `id`).
    /// See `Config::retry_on_empty_shell` for the full rationale.
    retry_on_empty_shell: bool,
    /// `max_tokens` value used for that single post-shell retry. Only the retry
    /// attempt is affected; the original request is forwarded unchanged.
    empty_shell_retry_max_tokens: u64,
}

/// Outcome of a single upstream attempt. `Retry` is used only for 401 without
/// a `Retry-After` header when the caller still has retry budget left.
enum AttemptResponse {
    Done(warp::reply::Response),
    Retry,
}

/// Disposition of a 2xx non-SSE ChatCompletions response after the
/// empty-choices peek. `Response` is the normal carry (a synthesized
/// placeholder when the body legitimately had empty `choices` + real `usage`,
/// or zero-ownership passthrough). `ShellFailure` is the NVIDIA "empty-shell"
/// soft failure: `choices:[]` with a NONE `usage` and empty `id` — no real
/// inference ran. The caller (`process_upstream_attempt`) decides whether to
/// retry once with a clamped `max_tokens` (see `Config::retry_on_empty_shell`)
/// or surface a 502 instead of the misleading empty-content placeholder that
/// made Claude Code's `/compact` print "summarization produced empty response".
enum SuccessOutcome {
    Response(warp::reply::Response),
    /// Upstream returned an empty-shell soft failure. Carries the parsed body
    /// so the caller can extract fields for the 502 detail message if it
    /// chooses not to (or cannot) retry.
    ShellFailure { body: serde_json::Value },
}

/// Classify an empty-`choices` 2xx body as an NVIDIA empty-shell soft failure.
///
/// NVIDIA's soft-failure shell (observed on GLM-class models at
/// build.integrate, notably for `/compact` with large `max_tokens`) looks like:
///   `{"choices":[],"id":"","model":"","created":0,"object":"chat.completion",
///     "service_tier":null,"system_fingerprint":null,"usage":null}`
/// Every meaningful field is null/empty because no inference actually ran —
/// the model was silently dropped, not run. The tell is `usage:null`: a
/// *legitimate* compaction empty-choices body (model ran, produced zero
/// tokens) still carries a real `usage` object. So this returns true only when
/// `choices` is empty AND `usage` is null/absent. We also require `id` to be
/// empty-ish as a second corroborating signal, which keeps a hypothetical
/// long-running empty-body usage-less 200 (not in NVIDIA's contract) from
/// being misread as a shell.
fn is_empty_shell_failure(body: &serde_json::Value) -> bool {
    // choices must be present and empty.
    let choices_empty = body
        .get("choices")
        .and_then(|c| c.as_array())
        .map(|a| a.is_empty())
        .unwrap_or(false);
    if !choices_empty {
        return false;
    }
    // usage null/absent/empty object → no inference ran.
    let usage_null = match body.get("usage") {
        None | Some(serde_json::Value::Null) => true,
        Some(u) if u.as_object().map(|o| o.is_empty()).unwrap_or(false) => true,
        Some(_) => false,
    };
    if !usage_null {
        return false;
    }
    // Empty-id as a second corroborating signal.
    match body.get("id") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(s)) => s.is_empty(),
        Some(_) => false,
    }
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

/// Emit a structured `failure.log` record when an upstream 2xx ChatCompletions
/// response was identified as an NVIDIA empty-shell soft failure (`choices:[]`
/// with `usage:null` and empty `id` — no real inference ran). The record carries
/// enough evidence to pin the root cause from failure.log alone, without
/// re-poking NVIDIA:
///   * `body_excerpt` — the full upstream response body (tiny; capped at 2048).
///     Its `usage:null` vs `usage:{...}` is the tell that distinguishes a shell
///     from a legitimate zero-token compaction response.
///   * `detail` — a structured fingerprint: request side (`max_tokens`, `stream`,
///     `messages` count, **rough prompt-token estimate**, `temperature`, `top_p`,
///     `reasoning_effort`, `tools`/`tool_choice`), the shell side (`id`,
///     `usage`, `created`, `choiceslen`), the retry disposition (clamped
///     `max_tokens`, attempt index, retry reason), and per-attempt upstream
///     elapsed-ms when supplied by the caller.
///
/// Request messages are NEVER logged in full (they may hold large transcript);
/// only the rough token estimate + first/last message previews.
///
/// `category` distinguishes the first-attempt shell from a retry shell so a jq
/// group-by can tell the two apart. `retry_attempt` is the attempt index that
/// produced this shell (0 → original request), and `retry_reason` is set to
/// the reason a retry followed (or None when none followed). `elapsed_ms` is the
/// upstream round-trip time for the attempt that produced this shell when the
/// caller has it (None when unknown).
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
    // Full upstream response body — the empty-shell body is tiny (no
    // generation), so this is cheap. Cap defensively in case upstream bloats.
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

/// Compact, structured fingerprint of a chat-completions **request** body, for
/// failure-log/tracing context. Pulls the fields that most commonly influence
/// NVIDIA's soft-rejection of GLM-class models: `max_tokens` (the prime suspect
/// on `/compact`), `stream`, `messages` count, a rough prompt-token estimate
/// (Σ content chars / 4 — coarse but enough to spot a context-window overflow),
/// `temperature` / `top_p` / `reasoning_effort`, and any `tools`/`tool_choice`.
/// Returns the request untouched when unparseable. Never logs full message
/// text — only the scalar/preview facets here.
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

    // messages count + rough prompt-token estimate + first/last role previews.
    let messages = v.get("messages").and_then(|m| m.as_array());
    let n = messages.map(|a| a.len()).unwrap_or(0);
    let mut est_chars: usize = 0;
    // System/tool text also counts toward the prompt; sum every message's
    // content length regardless of role.
    if let Some(arr) = messages {
        for m in arr {
            match m.get("content") {
                Some(serde_json::Value::String(s)) => est_chars += s.chars().count(),
                Some(other) => est_chars += other.to_string().chars().count(),
                None => {}
            }
        }
    }
    let est_tokens = est_chars / 4;

    let prev = |idx: usize| -> Option<String> {
        let arr = messages?;
        let m = arr.get(idx)?;
        let role = m
            .get("role")
            .and_then(|r| r.as_str())
            .unwrap_or("?")
            .to_string();
        let content_str = match m.get("content") {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        let c = content_str.chars().take(120).collect::<String>();
        Some(format!("role={role} content[..120]={c:?}"))
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

/// Compact fingerprint of an NVIDIA empty-shell response body — the fields that
/// confirm it's a shell (vs. a legitimate zero-token completion): `id`, `model`,
/// `created`, `usage`, and the length of the `choices` array (should be 0).
/// `usage:null` here is the smoking gun that no inference ran.
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

/// Compose the `detail` string for an empty-shell failure record: request
/// fingerprint + shell fingerprint + retry disposition + timing. Hard line-breaks
/// (`\\n`) make the `jq -r .detail` output readable rather than one long comma'd
/// run; failure.log stores it as a single JSON string either way.
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

/// Build a 502 Bad Gateway response body that surfaces an NVIDIA empty-shell
/// soft failure to the client — instead of the synthesized empty-content
/// choice that made Claude Code's `/compact` print "summarization produced
/// empty response". Carries a structured OpenAI-shaped error so downstream
/// consumers (cc-switch → Anthropic) classify it as a hard failure rather than
/// a zero-token completion.
///
/// `primary` is the body that drove the 502 (the retry's shell, or the
/// original shell when no retry). `original` is the first-attempt shell, kept
/// for the log detail even when `primary` is a retry outcome. `retry_max`
/// is the clamped `max_tokens` used when a retry was attempted (None means
/// retry was disabled/budget exhausted).
/// Build a 502 Bad Gateway response body that surfaces an NVIDIA empty-shell
/// soft failure to the client — instead of the synthesized empty-content choice
/// that made Claude Code's `/compact` print "summarization produced empty
/// response". Carries a structured OpenAI-shaped error so downstream consumers
/// (cc-switch → Anthropic) classify it as a hard failure rather than a
/// zero-token completion.
///
/// `request_body` is the body that last went upstream (original when no retry,
/// clamped retry body when one was made). `shell_body` is the upstream
/// empty-shell body that drove this 502 (the retry's shell, or the original).
/// `retry` is `Some((clamped_max_tokens, attempt_index, outcome))` when a
/// clamped retry was attempted; the outcome string names how it ended.
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

/// Parse a chat-completions request body, clamp `max_tokens` to `target`, and
/// re-serialize. Used by the empty-shell retry to give NVIDIA a smaller output
/// budget so the inference actually runs. If the body isn't a valid JSON object
/// or has no numeric `max_tokens`, the original bytes are returned unchanged.
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
    // Only clamp when the original max_tokens exceeds the target — otherwise
    // leave the request untouched (and when there's no max_tokens at all, set
    // nothing rather than surprise clients with a new field).
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
///
/// It also sniffs for an *empty* `choices` array on the stream. Recall:
/// cc-switch translates the upstream ChatCompletions body back to Anthropic
/// format, and its non-streaming converter `openai_to_anthropic` hard-errors
/// with "格式转换错误: Empty choices array" (surfaced as 422 to the Claude
/// Code client) when the body parses to `{"choices": [], ...}` — which
/// happens on a genuine OpenAI/NVIDIA usage-only final SSE chunk
/// `data: {"choices":[],"usage":{...}}` aggregated by cc-switch, or on a
/// non-stream 200 whose body really did carry `choices: []`. We can't see
/// cc-switch from here, but when the upstream stream actually contained a JSON
/// object with an empty `choices` array, logging it (with status) lets us
/// pinpoint the trigger in production rather than guessing.
///
/// The diagnostic is *read-only* (never mutates the stream) and guarded so it
/// only fires when we genuinely parsed a JSON object carrying `choices: []`
/// and never afterward saw a non-empty `choices` — text/plain bodies, SSE
/// `[DONE]`, and chunks we failed to parse all stay quiet, so the warning
/// doesn't false-positive on every non-streaming or non-JSON response.
struct UsageTap<S> {
    inner: S,
    metrics: Arc<Metrics>,
    carry: String,
    usage: Option<Usage>,
    /// Did any parsed JSON object on this stream carry a non-empty `choices`
    /// array? If yes by stream end, the response had real content and we
    /// don't need to warn.
    saw_choices: bool,
    /// Did any parsed JSON object carry an *empty* `choices: []` array?
    saw_empty_choices: bool,
    /// Did we successfully parse *any* JSON object at all? Guards the
    /// end-of-stream diagnostic: a text/plain body or a chunk we couldn't
    /// parse should not be mistaken for a JSON response with empty choices.
    saw_any_json: bool,
    /// Status of the upstream response this tap wraps, for the log line.
    status: Option<u16>,
    client_req_id: String,
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
            self.classify_json_line(&line);
        }
    }

    /// Parse a single line (with or without a trailing newline) as a JSON
    /// payload, after stripping an SSE `data:` prefix. Used both by `feed`
    /// (for complete lines) and by `flush_trailing` (for the final line that
    /// never received a newline, e.g. a single-line non-streaming
    /// ChatCompletions body). A single bare JSON object on its own — no
    /// `data:` prefix, no surrounding SSE framing — is what a non-streaming
    /// OpenAI response looks like, so don't require SSE framing here.
    fn classify_json_line(&mut self, line: &str) {
        let line = line.trim_end_matches(['\r', '\n']);
        let json_str = match line.strip_prefix("data:") {
            Some(rest) => rest.trim_start(),
            None => line.trim(),
        };
        if json_str.is_empty() {
            return;
        }
        // `[DONE]` and similar SSE terminators aren't JSON; skip quietly.
        if json_str == "[DONE]" {
            return;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return;
        };
        if let Some(u) = parse_usage(&v) {
            self.usage = Some(u);
        }
        // Track that we saw *any* JSON object at all, so the end-of-stream
        // diagnostic can distinguish "we never parsed JSON" (text/plain or a
        // parse failure — not our concern) from "we parsed JSON but it never
        // carried a non-empty choices array" (the cc-switch 422 trigger).
        self.saw_any_json = true;
        if let Some(choices) = v.get("choices") {
            match choices.as_array() {
                Some(arr) if !arr.is_empty() => self.saw_choices = true,
                Some(_) => self.saw_empty_choices = true,
                None => {}
            }
        }
    }

    /// Drain whatever is left in `carry` when the stream ends. A non-streaming
    /// OpenAI response is a single JSON object with no trailing newline; if we
    /// never flushed it, `saw_choices` would stay false and the diagnostic
    /// would false-positive on every non-streaming request.
    fn flush_trailing(&mut self) {
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
                // Drain any trailing line that never got a '\n'. A non-streaming
                // OpenAI response is a single JSON object with no trailing
                // newline; without this flush it'd sit unread in `carry` and
                // the empty-choices diagnostic would false-positive.
                me.flush_trailing();
                if let Some(u) = me.usage.take() {
                    me.metrics.record_usage(u.prompt, u.completion, u.total);
                }
                // Empty-choices diagnostic: warn *only* when we parsed a JSON
                // object that carried an empty `choices: []` array and never
                // subsequently saw a non-empty one. Requiring `saw_empty_choices`
                // (not merely `!saw_choices`) keeps text/plain bodies, single-line
                // non-streaming responses, and chunks we failed to parse from
                // false-positiving — those never had an empty-choices JSON to
                // begin with.
                if let Some(status) = me.status {
                    if me.saw_empty_choices && !me.saw_choices {
                        warn!(
                            client_req_id = %me.client_req_id,
                            status = status,
                            "Upstream 2xx returned an empty choices array — cc-switch may \
                             hard-error translating it to Anthropic (格式转换错误: Empty \
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

/// How much of a non-stream success body we'll buffer while looking for an
/// empty `choices` array to synthesize around. Bodies that actually have an
/// empty `choices` (no generated tokens) are tiny — far under this cap — so we
/// never hold a large completion in memory, and any genuinely large non-stream
/// body falls straight through to streaming passthrough after this many bytes.
const EMPTY_CHOICES_PEEK_BYTES: usize = 64 * 1024;

/// Strip hop-by-hop and length headers we'll recompute, then copy the rest of
/// `resp_headers` onto `builder`. Shared by the streaming-passthrough and
/// empty-choices synthesis paths so the client sees a consistent forwarded
/// header set regardless of which path a response takes.
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
        retry_config: crate::config::RetryConfig,
        token_wait_ms: u64,
        model_aliases: std::collections::HashMap<String, String>,
        inject_stream_usage: bool,
        synthesize_empty_choices: bool,
        retry_on_empty_shell: bool,
        empty_shell_retry_max_tokens: u64,
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
            synthesize_empty_choices,
            retry_on_empty_shell,
            empty_shell_retry_max_tokens,
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

            // tools normalization: NVIDIA's ToolDescription requires `description`
            // and (when nested under `function`) `function.description` to be a
            // *string* — it rejects `null` with a 400 (pydantic
            // "Input should be a valid string, input_value=None"). cc-switch /
            // Claude Code emit tool definitions where some tools carry no
            // description (rendered as `null`), which were passing straight
            // through to NVIDIA and 400-ing (see failure.log). Coerce a missing
            // or null `description` to an empty string; the empty type
            // annotation already used for max_tokens/reasoning_effort applies
            // here too. We also coerce `name` defensively (NVIDIA requires
            // non-null), but leave `parameters` untouched.
            //
            // Handles both shapes a client may send:
            //   * flat:  {type, name, description, parameters}
            //   * nested (OpenAI canonical): {type:"function", function:{name, description, parameters}}
            if let Some(arr) = obj.get_mut("tools").and_then(|t| t.as_array_mut()) {
                let mut fixed = 0u32;
                for tool in arr.iter_mut() {
                    // Target object: the nested `function` when present, else
                    // the tool itself (flat shape).
                    let target: Option<&mut serde_json::Map<String, serde_json::Value>> =
                        if let Some(func) = tool.get_mut("function") {
                            func.as_object_mut()
                        } else {
                            tool.as_object_mut()
                        };
                    let Some(tobj) = target else { continue };

                    // Need a fix when the field is absent or explicitly null;
                    // a real string (even empty "" left by a prior tooling run
                    // round-tripping through us) is left as-is.
                    let need_desc = tobj
                        .get("description")
                        .map_or(true, serde_json::Value::is_null);
                    if need_desc {
                        tobj.insert(
                            "description".to_string(),
                            serde_json::Value::String(String::new()),
                        );
                        fixed += 1;
                    }
                    let need_name = tobj.get("name").map_or(true, serde_json::Value::is_null);
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
                        "Coerced null/missing tool description/name to empty string (NVIDIA rejects null)"
                    );
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
                .handle_passthrough_stream(resp, resp_headers, status, client_req_id)
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
        //
        // We do a bounded peek *only* when the empty-choices synthesis is
        // enabled: the body is read up to `EMPTY_CHOICES_PEEK_BYTES` and, if it
        // parses to a JSON object with an empty `choices` array, rewritten to a
        // legal single-choice response (so cc-switch's non-streaming converter
        // doesn't 422 on it). Any other body — non-JSON, large completion, or a
        // non-empty `choices` — falls back to the same zero-ownership passthrough
        // as before, with the already-buffered prefix prepended to the stream.
        if status.is_success() {
            self.key_pool.mark_good(&key_str).await;
            self.metrics.inc_success();
            let outcome = if self.synthesize_empty_choices {
                self.handle_success_maybe_synthesize(resp, resp_headers, status, client_req_id)
                    .await
            } else {
                // Synthesis disabled — the empty-shell retry path can't
                // inspect the body, so passthrough verbatim (legacy behavior).
                self.handle_passthrough_stream(resp, resp_headers, status, client_req_id)
                    .await
                    .map(SuccessOutcome::Response)
            }
            .map_err(AttemptError::Fatal)?;
            return Ok(match outcome {
                SuccessOutcome::Response(resp) => AttemptResponse::Done(resp),
                SuccessOutcome::ShellFailure { body } => {
                    // NVIDIA empty-shell soft failure — no inference ran.
                    // Surface the real cause instead of a misleading empty
                    // body. Optionally retry once with a clamped `max_tokens`.
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
/// verbatim. A `UsageTap` wraps the stream so token usage is accounted for
/// and (on success-path streams) an empty-choices diagnostic is logged
/// when the response carried no real `choices` content.
async fn handle_passthrough_stream(
    &self,
    resp: reqwest::Response,
    resp_headers: Vec<(String, Vec<u8>)>,
    status: warp::http::StatusCode,
    client_req_id: &str,
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
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status: Some(status.as_u16()),
            client_req_id: client_req_id.to_string(),
        });

        let mut response_builder = warp::http::Response::builder().status(status);
        apply_forwarded_response_headers(&mut response_builder, resp_headers);

        let body = warp::hyper::Body::wrap_stream(tapped);
        Ok(response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        }))
    }

    /// Try to rewrite an empty-`choices` success body without buffering the
    /// whole upstream — the hot success path stays zero-ownership once we've
    /// decided it isn't the empty-choices case.
    ///
    /// Strategy: read up to [`EMPTY_CHOICES_PEEK_BYTES`] of the streaming body
    /// into a buffer. After each chunk attempt to parse the *entire* buffer as
    /// a JSON object:
    ///   * parsed, and the top-level `choices` array is **empty** → this is the
    ///     cc-switch 422 trigger; synthesize a single placeholder `choice` (an
    ///     empty assistant message, `finish_reason: "stop"`) while preserving
    ///     `id` / `model` / `usage`, and return a buffered JSON response.
    ///     Bodies with an empty `choices` are always tiny (no generation), so
    ///     truncation can't hide one.
    ///   * parsed, and `choices` is non-empty (or absent as a key that isn't an
    ///     empty array) → a normal completion: stop peeking and fall through to
    ///     streaming passthrough below, prepending what we already read.
    ///   * not yet a complete JSON object → keep reading until we either parse,
    ///     hit the peek cap, or see non-UTF8 / clearly non-JSON bytes.
    ///
    /// On any unparseable / over-cap / synthesis-disabled case we forward the
    /// buffered prefix concatenated with the remaining upstream stream, so the
    /// client sees the exact bytes it would have under `handle_passthrough_stream`.
    async fn handle_success_maybe_synthesize(
        &self,
        resp: reqwest::Response,
        resp_headers: Vec<(String, Vec<u8>)>,
        status: warp::http::StatusCode,
        client_req_id: &str,
    ) -> Result<SuccessOutcome, Rejection> {
        let mut upstream = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();

        while let Some(chunk_res) = upstream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(e) => {
                    warn!(client_req_id = %client_req_id, "Success peek read error: {}", e);
                    // Forward what we have, plus an error tail; the client will
                    // see a truncated stream, same as passthrough would.
                    break;
                }
            };
            buf.extend_from_slice(&chunk);
            // Only attempt JSON decomposition when the buffer is valid UTF-8.
            // A gzipped / binary body fails here and falls straight through.
            let candidate = match std::str::from_utf8(&buf) {
                Ok(s) => s,
                Err(_) => break,
            };
            let parsed = serde_json::from_str::<serde_json::Value>(candidate);
            if let Ok(v) = &parsed {
                if let Some(arr) = v.get("choices").and_then(|c| c.as_array()) {
                    if arr.is_empty() {
                        // Empty choices. Two distinct upstream shapes land here:
                        //  (a) a *legitimate* compaction empty-choices body — the
                        //      model ran but produced zero tokens, which still
                        //      carries a real `usage` object. Synthesize a
                        //      placeholder choice so cc-switch doesn't 422.
                        //  (b) NVIDIA's empty-shell soft failure — `usage:null`
                        //      and `id:"` because no inference ran at all (often
                        //      a `/compact` with `max_tokens` too large for the
                        //      remaining context). Synthesizing an empty-content
                        //      choice here is what made Claude Code's `/compact`
                        //      print "summarization produced empty response". So
                        //      instead hand the shell to the caller, which will
                        //      retry once with a clamped `max_tokens` or surface
                        //      a 502 — never a misleading empty body.
                        if is_empty_shell_failure(v) {
                            return Ok(SuccessOutcome::ShellFailure { body: v.clone() });
                        }
                        // Confirmed the cc-switch 422 trigger (shape (a)).
                        // Synthesize and serve a normal (buffered) JSON body —
                        // the upstream finished, so there's no more stream to
                        // forward.
                        let resp = self.respond_synthesized_empty_choices(
                            v,
                            resp_headers,
                            client_req_id,
                        )?;
                        return Ok(SuccessOutcome::Response(resp));
                    }
                    // Non-empty choices: ordinary completion. Stop peeking and
                    // passthrough the already-read prefix + the rest.
                    break;
                }
                // Parsed JSON but no `choices` key: not our case (and per the
                // upstream contract a 2xx ChatCompletions body always has one),
                // so fall through to streaming. Slicing the prefix off isn't
                // needed — we forward the exact bytes we read.
                break;
            }
            if buf.len() >= EMPTY_CHOICES_PEEK_BYTES {
                // Genuinely large non-streaming body (no compression to blame
                // unless the JSON is huge): don't keep buffering; stream it
                // through. An empty-choices body can't be this big.
                break;
            }
        }

        // Fallthrough: passthrough with the buffered prefix prepended to the
        // remaining upstream stream. This preserves byte-for-byte fidelity with
        // `handle_passthrough_stream` (no synthesis happened).
        let prefix = Bytes::from(buf);
        let req_id_owned = client_req_id.to_string();
        let rest = upstream.map(move |r| {
            r.map(|chunk| coerce_sse_chunk(&chunk)).map_err(|e| {
                warn!(client_req_id = %req_id_owned, "Passthrough stream read error: {}", e);
                std::io::Error::other(e.to_string())
            })
        });
        let pref: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            if prefix.is_empty() {
                Box::pin(rest)
            } else {
                let head = futures_util::stream::iter(vec![Ok(prefix)]);
                Box::pin(head.chain(rest))
            };

        let tapped: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(UsageTap {
                inner: pref,
                metrics: self.metrics.clone(),
                carry: String::new(),
                usage: None,
                saw_choices: false,
                saw_empty_choices: false,
                saw_any_json: false,
                status: Some(status.as_u16()),
                client_req_id: client_req_id.to_string(),
            });

        let mut response_builder = warp::http::Response::builder().status(status);
        apply_forwarded_response_headers(&mut response_builder, resp_headers);
        // The response is now a known-length buffered-prefix stream; we leave
        // Content-Length off (chunked) to stay byte-faithful without having to
        // recompute it for the prefix + dynamic remainder.
        let body = warp::hyper::Body::wrap_stream(tapped);
        let resp = response_builder.body(body).unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(status)
                .body(warp::hyper::Body::empty())
                .unwrap()
        });
        Ok(SuccessOutcome::Response(resp))
    }

    /// Disposition of an NVIDIA empty-shell soft failure (`is_empty_shell_failure`).
    /// See `Config::retry_on_empty_shell` for the rationale. Per-shell retry
    /// is bounded at ONE additional attempt: if the retry itself shells, or if
    /// retry is disabled, we surface a 502 carrying the real cause so Claude
    /// Code's `/compact` stops printing the misleading "summarization produced
    /// empty response" and instead shows an actionable proxy/gateway error.
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
        // Bound the retry to one extra attempt, and only when the caller still
        // has retry budget (the attempt counter hasn't exhausted max_retries).
        // `retry_attempt_already_done` is the attempt index that *produced* this
        // shell; the retry is attempt+1.
        let can_retry = self.retry_on_empty_shell
            && retry_attempt_already_done + 1 < self.retry_config.max_retries;

        // Record the shell in failure.log with the full request+response
        // evidence regardless of whether we retry — this is the path that
        // surfaces the root cause for jq triage. We log *after* deciding
        // `can_retry` so `retry_reason` reflects the disposition that actually
        // followed.
        let req_fp = request_body_fingerprint(request_body);
        let shell_fp = shell_body_fingerprint(&shell_body);
        warn!(
            client_req_id = %client_req_id,
            attempt = retry_attempt_already_done,
            will_retry = can_retry,
            retry_max_tokens = if can_retry { Some(self.empty_shell_retry_max_tokens) } else { None },
            shell = %shell_fp,
            req = %req_fp,
            "NVIDIA empty-shell soft failure (200, usage:null, no inference)"
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
            } else {
                Some("empty_shell_no_retry")
            },
            None,
        );

        if can_retry {
            let clamped = clamp_max_tokens(request_body, self.empty_shell_retry_max_tokens);
            let retry_body: bytes::Bytes = clamped.into();
            warn!(
                client_req_id = %client_req_id,
                attempt = retry_attempt_already_done + 1,
                max_tokens = self.empty_shell_retry_max_tokens,
                req = %request_body_fingerprint(&retry_body),
                "Empty-shell soft failure; retrying once with clamped max_tokens",
            );
            let rebuild = self.build_upstream_request(
                url,
                method,
                headers,
                &retry_body,
                key_str,
                false, // shell responses are non-streaming compaction requests
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
                    info!(
                        client_req_id = %client_req_id,
                        attempt = retry_attempt_already_done + 1,
                        status = %resp_status,
                        content_type = %content_type,
                        url = %url,
                        "Empty-shell retry upstream response"
                    );
                    if resp_status.is_success() && self.synthesize_empty_choices {
                        let outcome = self
                            .handle_success_maybe_synthesize(
                                resp,
                                resp_headers,
                                resp_status,
                                client_req_id,
                            )
                            .await
                            .map_err(AttemptError::Fatal)?;
                        return Ok(match outcome {
                            SuccessOutcome::Response(resp) => {
                                self.key_pool.mark_good(key_str).await;
                                AttemptResponse::Done(resp)
                            }
                            // Retry also shelled — surface 502, don't loop.
                            SuccessOutcome::ShellFailure { body } => {
                                let retry_shell_fp = shell_body_fingerprint(&body);
                                warn!(
                                    client_req_id = %client_req_id,
                                    attempt = retry_attempt_already_done + 1,
                                    orig_shell = %shell_fp,
                                    retry_shell = %retry_shell_fp,
                                    "Empty-shell retry also soft-failed; surfacing 502",
                                );
                                write_empty_shell_failure_log(
                                    &body,
                                    &retry_body,
                                    client_req_id,
                                    model,
                                    key_str,
                                    url,
                                    body_summary,
                                    "empty_choices_shell_retry",
                                    retry_attempt_already_done + 1,
                                    Some("empty_shell_retry_exhausted"),
                                    None,
                                );
                                AttemptResponse::Done(empty_shell_bad_gateway(
                                    request_body,
                                    &body,
                                    client_req_id,
                                    Some((
                                        self.empty_shell_retry_max_tokens,
                                        retry_attempt_already_done + 1,
                                        "retry_also_empty_shell",
                                    )),
                                ))
                            }
                        });
                    }
                    // Non-2xx retry or synthesis disabled: classify the error
                    // its own way would need a whole second pass; for this rare
                    // self-retried path, surface a 502 so the client sees a clear
                    // failure instead of a half-handled body.
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
                    warn!(client_req_id = %client_req_id, "Empty-shell retry send error: {}", e);
                    // Network error on the retry: surface 502 rather than the
                    // original shell (we'd rather show a hard failure than a
                    // misleading empty completion).
                    return Ok(AttemptResponse::Done(empty_shell_bad_gateway(
                        request_body,
                        &shell_body,
                        client_req_id,
                        Some((
                            self.empty_shell_retry_max_tokens,
                            retry_attempt_already_done + 1,
                            "retry_network_error",
                        )),
                    )));
                }
            }
        }

        // No retry budget / retry disabled: surface 502 with the real cause.
        warn!(
            client_req_id = %client_req_id,
            "Empty-shell soft failure; surfacing 502 (retry disabled or budget exhausted)",
        );
        Ok(AttemptResponse::Done(empty_shell_bad_gateway(
            request_body,
            &shell_body,
            client_req_id,
            None,
        )))
    }

    /// Build and return the synthesized response: take a parsed body with an
    /// empty `choices` array, drop in a single empty-assistant-message choice,
    /// preserve `id` / `model` / `usage` / any sibling fields, re-serialize,
    /// and serve it as a buffered JSON response with a corrected Content-Length.
    fn respond_synthesized_empty_choices(
        &self,
        body: &serde_json::Value,
        resp_headers: Vec<(String, Vec<u8>)>,
        client_req_id: &str,
    ) -> Result<warp::http::Response<warp::hyper::Body>, Rejection> {
        warn!(
            client_req_id = %client_req_id,
            "Synthesizing placeholder choice for empty-choices 2xx body \
             (prevents downstream \"Empty choices array\" 422)",
        );

        let mut obj = match body {
            serde_json::Value::Object(m) => m.clone(),
            // The caller only reaches here after `body.choices` resolved to an
            // empty array, which requires a JSON object; anything else is a bug
            // in the parser guard, so fail safe to the raw passthrough path.
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
        // Drop any upstream Content-Length/transfer-encoding and set our own so
        // the client knows exactly where the synthesized body ends.
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

    /// Unreachable in practice under the object guard; provided so the
    /// synthesis helper can return *some* response rather than `unwrap`.
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
            synthesize_empty_choices: true,
            retry_on_empty_shell: true,
            empty_shell_retry_max_tokens: 8192,
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
            config.inject_stream_usage,
            config.synthesize_empty_choices,
            config.retry_on_empty_shell,
            config.empty_shell_retry_max_tokens,
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

    // ── tools normalization (mod: ToolDescription.description null→empty) ──

    /// Flat-shape tool with `description: null` must be coerced to `""`
    /// before forwarding, and the request must still succeed. NVIDIA's
    /// pydantic ToolDescription rejects null description with a 400; the
    /// failure.log showed this cascading into cc-switch's
    /// "Empty choices array" 422.
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

    /// Nested OpenAI-canonical shape `{type:function, function:{...}}` with
    /// a missing `function.description` must be backfilled with `""`.
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
        // Sanity: non-null description is preserved untouched.
        let name = body["tools"][0]["function"]["name"].as_str().unwrap();
        assert_eq!(name, "t");
    }

    /// A tool with a real non-empty `description` must pass through
    /// unchanged (the normalizer is a no-op when the field is already valid).
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

    // ── empty-choices synthesis (cc-switch 422 mitigation, no cc-switch change) ──

    /// A non-stream `stream:false` 2xx body with an empty `choices` array —
    /// the exact shape that makes cc-switch's `openai_to_anthropic` throw
    /// "Empty choices array" (→ 422 to Claude Code, e.g. on context
    /// compaction). The proxy must inject a single placeholder choice and
    /// preserve `id` / `model` / `usage`, so the downstream converter sees a
    /// legal ChatCompletions response instead of erroring.
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
        // id/model/usage must be preserved so token accounting & metadata survive.
        assert_eq!(body["id"].as_str(), Some("cc-42"));
        assert_eq!(body["model"].as_str(), Some("z-ai/glm-5.2"));
        assert_eq!(body["usage"]["prompt_tokens"].as_u64(), Some(7));
        assert_eq!(body["usage"]["total_tokens"].as_u64(), Some(7));
    }

    /// NVIDIA's empty-shell soft failure: 200 with `choices:[]` AND `usage:null`
    /// AND empty `id` (no inference ran). The proxy MUST surface a 502, not a
    /// synthesized empty-content placeholder, so Claude Code's `/compact`
    /// reports an actionable upstream error instead of "summarization produced
    /// empty response". Here the retry also shells, so the client gets 502 and
    /// exactly two upstream calls were made (initial + one clamped retry).
    #[tokio::test]
    async fn empty_shell_soft_failure_surfaces_502_after_one_retry() {
        let shell = serde_json::json!({
            "choices": [], "id": "", "model": "", "created": 0,
            "object": "chat.completion", "service_tier": null,
            "system_fingerprint": null, "usage": null,
        });
        let mock_server = MockServer::start().await;
        // First (initial, max_tokens=64000) -> shell.
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::body_string_contains("\"max_tokens\":64000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(shell.clone()))
            .expect(1)
            .mount(&mock_server)
            .await;
        // Retry (clamped to 8192) -> also shell.
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::body_string_contains("\"max_tokens\":8192"))
            .respond_with(ResponseTemplate::new(200).set_body_json(shell))
            .expect(1)
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
        // clamp happened: the retry request body carried max_tokens=8192.
        assert!(
            body["error"]["detail"].as_str().unwrap_or_default().contains("max_tokens=8192"),
            "detail should record the clamped retry budget, got: {body}"
        );
        // Structured retry fields (new diagnostic surface).
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
        // Request fingerprint and upstream shell structured in the error.
        let req_fp = body["error"]["request_fingerprint"]
            .as_str()
            .unwrap_or("");
        assert!(
            req_fp.contains("max_tokens=64000") && req_fp.contains("messages=0"),
            "request_fingerprint should capture the request's max_tokens + message count, got: {req_fp}"
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

    /// Empty-shell soft failure where the one clamped retry *succeeds*: the
    /// proxy must return 200 with the retry's normal choices, not 502. Only one
    /// retry is attempted (no loop even if retry succeeds).
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
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::body_string_contains("\"max_tokens\":64000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(shell))
            .expect(1)
            .mount(&mock_server)
            .await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::body_string_contains("\"max_tokens\":8192"))
            .respond_with(ResponseTemplate::new(200).set_body_json(good.clone()))
            .expect(1)
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

    /// With `retry_on_empty_shell = false`, the shell surfaces as 502 with no
    /// upstream retry (exactly one upstream call).
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

    /// A normal non-empty-choices `stream:false` response must pass through the
    /// bounded-peek path unchanged — the synthesis is strictly additive and
    /// only touches the empty-choices case.
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
        // Byte-faithful: choices/content unchanged, no injection.
        assert_eq!(body["choices"][0]["message"]["content"].as_str(), Some("hi"));
        assert_eq!(body["id"].as_str(), Some("cc-7"));
        assert_eq!(body["usage"]["total_tokens"].as_u64(), Some(2));
    }

    /// With `synthesize_empty_choices = false` the proxy must NOT synthesize:
    /// an empty-choices 200 body is forwarded as-is (the pre-mitigation
    /// behaviour). This is the escape hatch documented on the config field.
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
        // Unchanged: still empty choices — synthesis was off.
        assert!(body["choices"].as_array().map(|a| a.is_empty()).unwrap_or(false));
        assert_eq!(body["id"].as_str(), Some("cc-1"));
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
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status: None,
            client_req_id: String::new(),
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

    // ── empty-choices diagnostic: classify without false-positiving ──

    /// Helper: feed a body through a `UsageTap` (with an inner `empty()` stream
    /// so the only thing being exercised is `feed`/`flush_trailing`), then run
    /// the end-of-stream logic and report whether the empty-choices warning
    /// *would* fire. Mirrors the `Poll::Ready(None)` branch in `Stream`.
    fn empty_choices_would_warn(body: &[u8], status: Option<u16>) -> bool {
        let mut tap = UsageTap {
            inner: futures_util::stream::empty(),
            metrics: Arc::new(Metrics::new()),
            carry: String::new(),
            usage: None,
            saw_choices: false,
            saw_empty_choices: false,
            saw_any_json: false,
            status,
            client_req_id: String::from("t"),
        };
        // Feed the whole body as if it arrived in one chunk; `feed` processes
        // complete lines, `flush_trailing` drains whatever the body left
        // without a trailing newline.
        tap.feed(&Bytes::copy_from_slice(body));
        tap.flush_trailing();
        // Same guard as poll_next's Ready(None) branch.
        tap.saw_empty_choices && !tap.saw_choices
    }

    /// The genuinely-pathological case: a 2xx usage-only final SSE chunk with
    /// `choices: []` (and usage). This is what cc-switch aggregates and then
    /// 422's on — the diagnostic must fire.
    #[test]
    fn empty_choices_diagnostic_fires_on_empty_choices_json() {
        let body = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":0,\"total_tokens\":1}}\n";
        assert!(
            empty_choices_would_warn(body, Some(200)),
            "an empty-choices JSON object on a 2xx stream must trigger the diagnostic",
        );
    }

    /// A non-streaming ChatCompletions response is a single JSON line with no
    /// trailing newline. Despite the missing '\\n', `flush_trailing` must read
    /// its non-empty `choices` so the diagnostic does *not* false-positive.
    #[test]
    fn empty_choices_diagnostic_quiet_on_nonstream_json_without_newline() {
        let body = b"{\"id\":\"x\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "a normal non-streaming response (single JSON line, non-empty choices) must not warn",
        );
    }

    /// A streaming response with content deltas followed by a usage-only final
    /// chunk (`choices:[]`). The earlier non-empty `choices` makes the
    /// diagnostic quiet — this is the common OpenAI streaming tail.
    #[test]
    fn empty_choices_diagnostic_quiet_when_real_choice_preceded_usage_tail() {
        let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "a real choice earlier in the stream suppresses the empty-tail warning",
        );
    }

    /// A non-JSON success body (text/plain upstream, or any body we can't
    /// parse as JSON) must never fire the diagnostic — there's no choices
    /// array to be empty, so the cc-switch trigger doesn't apply.
    #[test]
    fn empty_choices_diagnostic_quiet_on_text_body() {
        let body = b"hello world this is not json at all\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "text/non-JSON bodies must not false-positive the empty-choices diagnostic",
        );
    }

    /// Error-shaped bodies (`{"error":...}`) intentionally parse as JSON with
    /// no `choices` key at all — that does not set `saw_empty_choices`, so the
    /// diagnostic stays quiet. (cc-switch's "No choices in response" is a
    /// separate upstream-handles path.)
    #[test]
    fn empty_choices_diagnostic_quiet_on_error_body_no_choices_key() {
        let body = b"{\"error\":{\"message\":\"bad\",\"type\":\"BadRequestError\",\"code\":400}}\n";
        assert!(
            !empty_choices_would_warn(body, Some(200)),
            "an error body with no choices key is not the empty-choices case",
        );
    }

    /// The empty-shell classifier must fire only for the NVIDIA shell shape:
    /// empty `choices` + null/absent `usage` + empty-ish `id`. A *legitimate*
    /// zero-token compaction body (empty choices but real `usage`) must NOT
    /// classify as a shell — that path still synthesizes a placeholder.
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

        // Non-null usage but empty id+empty choices is impossible per contract;
        // still shouldn't classify (usage is the deciding signal).
        let weird = serde_json::json!({
            "choices": [], "id": "", "usage": {"prompt_tokens": 3},
        });
        assert!(!is_empty_shell_failure(&weird), "non-null usage wins over empty id");
    }

    /// The request fingerprint must surface the fields that influence NVIDIA's
    /// soft-rejection of GLM-class models: `max_tokens`, `stream`, message count,
    /// a rough prompt-token estimate, `temperature`/`top_p`/`reasoning_effort`,
    /// and `tools` — plus first/last message role previews. Never log the full
    /// transcript; previews cap at 120 chars.
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
        // Rough estimate: 2 short messages ≈ a small handful of tokens, never a
        // huge number — sanity bound it's present and non-zero-ish.
        assert!(fp.contains("est_prompt_tokens="), "missing token estimate in: {fp}");
    }

    /// Absent fields surface as `<absent>`/`0` rather than panicking or silently
    /// mutating the request shape — keeps the fingerprint readable for minimal
    /// requests like the integration tests send.
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
