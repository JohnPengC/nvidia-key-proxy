//! Structured failure logging — mirrors non-2xx upstream outcomes and
//! internal proxy errors to a dedicated JSONL file (`failure.log` next to
//! the binary's CWD by default, overridable via `FAILURE_LOG_PATH` env var).
//!
//! Each line is one JSON object. Designed for `jq` group-by queries so the
//! user can quickly answer "how many 404 / 403 hit which models on which
//! keys in the last hour". The main tracing log stream stays unchanged;
//! this is a separate, failure-only channel.
//!
//! Fields kept stable across versions:
//!   ts, req_id, endpoint, method, model, key, status, category,
//!   detail, body_excerpt, upstream_url, retry_attempt, error_kind

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

use chrono::Local;
use serde_json::{json, Value};

use crate::key_pool::mask_key;

/// One global file handle guarded by a Mutex. Writes are short (single JSON
/// line) so contention is negligible; we never hold the lock across await.
static FAILURE_FILE: Mutex<Option<File>> = Mutex::new(None);

fn log_path() -> String {
    std::env::var("FAILURE_LOG_PATH").unwrap_or_else(|_| "failure.log".to_string())
}

fn ensure_open() {
    let mut guard = FAILURE_FILE.lock().unwrap();
    if guard.is_none() {
        let path = log_path();
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => *guard = Some(f),
            Err(e) => {
                eprintln!("failure_log: cannot open {}: {}", path, e);
                // Leave None — write_failure becomes a no-op.
            }
        }
    }
}

/// Generate a short, monotonic-ish request id: unix seconds + a per-process
/// counter. Not globally unique, but unique enough to correlate retries
/// belonging to the same client request within one proxy lifetime.
fn gen_req_id(counter: u64) -> String {
    let secs = Local::now().timestamp();
    format!("r{:x}_{:x}", secs, counter)
}

/// Internal entry point. `category` is a short string tag the caller picks
/// from the set below; everything else is derived from the call site.
///
/// Categories (kept stable):
///   upstream_4xx         — upstream returned 4xx (404/403/401/400/…)
///   upstream_5xx         — upstream returned 5xx
///   upstream_other       — upstream returned an unexpected non-2xx status
///   network_error        — reqwest send failed (DNS / connect / TLS / …)
///   timeout              — upstream request exceeded request_timeout_secs
///   body_read_error      — resp.bytes() failed mid-response
///   bad_request          — client sent a body we couldn't translate
///   bad_gateway_internal — we couldn't build a response / internal panic
pub fn write_failure(
    method: &str,
    endpoint: &str,
    upstream_url: &str,
    model: &str,
    raw_key: &str,
    status: Option<u16>,
    category: &str,
    detail: &str,
    body_excerpt: &str,
    retry_attempt: u32,
    req_counter: u64,
    error_kind: Option<&str>,
) {
    ensure_open();
    let mut guard = FAILURE_FILE.lock().unwrap();
    let file = match guard.as_mut() {
        Some(f) => f,
        None => return,
    };

    let now = Local::now();
    let ts = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let error_time = ts.clone();

    let mut entry = json!({
        "ts": ts,
        "error_time": error_time,
        "req_id": gen_req_id(req_counter),
        "method": method,
        "endpoint": endpoint,
        "upstream_url": upstream_url,
        "model": model,
        "key": mask_key(raw_key),
        "status": status,
        "category": category,
        "detail": detail,
        "retry_attempt": retry_attempt,
    });
    if !body_excerpt.is_empty() {
        entry["body_excerpt"] = Value::String(body_excerpt.to_string());
    }
    if let Some(k) = error_kind {
        entry["error_kind"] = Value::String(k.to_string());
    }

    // Serialize then append a newline. serde_json::to_string never panics
    // on our Value shape, but we guard with a terse stderr on the (impossible)
    // failure path.
    match serde_json::to_string(&entry) {
        Ok(line) => {
            let _ = writeln!(file, "{}", line);
            let _ = file.flush();
        }
        Err(e) => {
            eprintln!("failure_log: serialize failed: {}", e);
        }
    }
}

/// Extract the `Function '<uuid>': Not found for account '<acct>'` pattern
/// from an upstream error body. Returns (function_id, account_id) when found,
/// both empty strings otherwise. We avoid regex to keep this crate-free;
/// simple substring matching is enough for the known NVIDIA error shape.
pub fn extract_nvidia_function_detail(body: &str) -> (String, String) {
    let mut function_id = String::new();
    let mut account_id = String::new();
    if let Some(start) = body.find("Function '") {
        let rest = &body[start + "Function '".len()..];
        if let Some(end) = rest.find('\'') {
            function_id = rest[..end].to_string();
        }
    }
    if let Some(start) = body.find("account '") {
        let rest = &body[start + "account '".len()..];
        if let Some(end) = rest.find('\'') {
            account_id = rest[..end].to_string();
        }
    }
    (function_id, account_id)
}
