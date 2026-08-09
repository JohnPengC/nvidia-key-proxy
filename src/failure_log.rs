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
//!   ts, req_id, client_req_id, endpoint, method, model, key, status,
//!   category, detail, body_excerpt, body_summary, upstream_url,
//!   retry_attempt, retry_reason, error_kind
//!
//! Rotation: the file grows unbounded unless `FAILURE_LOG_MAX_BYTES` is set
//! (a byte ceiling on the *current* file). When a written line would push
//! the current file past the limit, the existing file is rolled to
//! `<path>.<unix-secs>` and a fresh file is opened before the line is
//! written. 0/disabled (the default) keeps the append-only behavior the
//! tuning history in README assumes. For a no-restart rotation policy,
//! pair `FAILURE_LOG_MAX_BYTES` with an external `logrotate` using
//! `copytruncate` (the in-process handle flushes per line and survives
//! truncation), or rely on the built-in roll above. Archived files are
//! not compressed or pruned by this process — manage retention externally.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

use chrono::Local;
use serde_json::{json, Value};

use crate::key_pool::mask_key;

/// One global file handle guarded by a Mutex, plus a byte counter for the
/// current file so we can rotate at `FAILURE_LOG_MAX_BYTES`. Writes are short
/// (single JSON line) so contention is negligible; we never hold the lock
/// across await.
struct RotationState {
    file: File,
    /// Bytes written to the *current* open file. Reset on rotation.
    written: u64,
    /// Parsed `FAILURE_LOG_MAX_BYTES`. Captured at ensure_open time so a
    /// mid-process env change doesn't take effect (config snapshot).
    max_bytes: u64,
}
static FAILURE_FILE: Mutex<Option<RotationState>> = Mutex::new(None);

fn log_path() -> String {
    std::env::var("FAILURE_LOG_PATH").unwrap_or_else(|_| "failure.log".to_string())
}

/// `FAILURE_LOG_MAX_BYTES` ceiling on the current file. 0 = disabled
/// (append unbounded — legacy behavior). Read once at open time.
fn max_bytes_setting() -> u64 {
    std::env::var("FAILURE_LOG_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn ensure_open() {
    // `unwrap_or_else(|p| p.into_inner())` survives a poisoned mutex — if a
    // prior write panicked (it never should on this path, but a panic during
    // `writeln!`/`flush` from a broken fd would do it), we still want the
    // failure-log channel to keep writing rather than cascade-panic every
    // subsequent failure record.
    let mut guard = FAILURE_FILE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if guard.is_none() {
        let path = log_path();
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                // Start the counter at the current file size (only when
                // rotation is enabled) so an already-large file rotates on the
                // very next write rather than ignoring existing bytes.
                let written = match f.metadata().map(|m| m.len()) {
                    Ok(len) => len,
                    Err(_) => 0,
                };
                *guard = Some(RotationState {
                    file: f,
                    written,
                    max_bytes: max_bytes_setting(),
                });
            }
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
/// Kept as positional arguments on purpose: each call site already has all
/// fields in scope and a struct would just move the parameter list elsewhere.
#[allow(clippy::too_many_arguments)]
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
    body_summary: &str,
    retry_attempt: u32,
    retry_reason: Option<&str>,
    req_counter: u64,
    client_req_id: &str,
    error_kind: Option<&str>,
) {
    // Test/CI opt-out: when `FAILURE_LOG_DISABLE` is set (any value), writes
    // are skipped so integration tests don't pollute the on-disk failure.log
    // used for production diagnosis. Production leaves this unset.
    if std::env::var("FAILURE_LOG_DISABLE").is_ok() {
        return;
    }
    ensure_open();
    let mut guard = FAILURE_FILE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let rot = match guard.as_mut() {
        Some(r) => r,
        None => return,
    };

    let now = Local::now();
    let ts = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let error_time = ts.clone();

    let mut entry = json!({
        "ts": ts,
        "error_time": error_time,
        "req_id": gen_req_id(req_counter),
        "client_req_id": client_req_id,
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
    if !body_summary.is_empty() {
        entry["body_summary"] = Value::String(body_summary.to_string());
    }
    if let Some(k) = retry_reason {
        entry["retry_reason"] = Value::String(k.to_string());
    }
    if let Some(k) = error_kind {
        entry["error_kind"] = Value::String(k.to_string());
    }

    // Serialize then append a newline. serde_json::to_string never panics
    // on our Value shape, but we guard with a terse stderr on the (impossible)
    // failure path.
    match serde_json::to_string(&entry) {
        Ok(line) => {
            // Rotate before writing this line if it would push the current
            // file past the ceiling. `max_bytes == 0` disables rotation.
            if rot.max_bytes > 0 && rot.written + (line.len() as u64) + 1 > rot.max_bytes {
                let path = log_path();
                // Flush + close the current file, roll it to a timestamped
                // name, and open a fresh one. All best-effort: a failed roll
                // falls back to continuing on the same handle so we never
                // drop the failure record.
                let _ = rot.file.flush();
                let stamp = now.timestamp();
                if let Err(e) = std::fs::rename(&path, format!("{}.{}", path, stamp)) {
                    eprintln!("failure_log: rename for rotation failed ({}): {}", path, e);
                    // Could not roll; keep appending to the same handle and
                    // update `written` so we don't re-attempt every line.
                } else {
                    match OpenOptions::new().create(true).append(true).open(&path) {
                        Ok(new) => {
                            rot.file = new;
                            rot.written = 0;
                        }
                        Err(e) => {
                            eprintln!("failure_log: reopen after rotation failed ({}): {}", path, e);
                            // `rot.file` still holds the old handle; rename
                            // of an open file on Unix leaves the fd writing to
                            // the moved inode, so data is preserved.
                        }
                    }
                }
            }
            let _ = writeln!(rot.file, "{}", line);
            let _ = rot.file.flush();
            rot.written += (line.len() as u64) + 1;
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
