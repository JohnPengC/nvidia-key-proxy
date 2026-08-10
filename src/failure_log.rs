//! 结构化失败日志——把非 2xx 上游结果与代理内部错误镜像到一份专门的
//! JSONL 文件（默认在二进制 CWD 旁的 `failure.log`，可用 `FAILURE_LOG_PATH`
//! 环境变量覆盖）。
//!
//! 每行一个 JSON 对象。设计上配合 `jq` 做 group-by 查询，让人能快速回答
//! "过去一小时里有多少 404 / 403 落到哪些 key 的哪些 model 上"。主 tracing
//! 日志流保持不变；这是一条独立的、只记失败的旁路。
//!
//! 跨版本保持稳定的字段：
//!   ts, req_id, client_req_id, endpoint, method, model, key, status,
//!   category, detail, body_excerpt, body_summary, upstream_url,
//!   retry_attempt, retry_reason, error_kind
//!
//! 轮转：文件默认无限增长，除非设了 `FAILURE_LOG_MAX_BYTES`（对*当前*
//! 文件的字节上限）。当某行写入会让当前文件超过上限时，先把现有文件 rollover
//! 到 `<path>.<unix-secs>`，再开新文件写入该行。0/不设（默认）则保持
//! README 里调优历史所依赖的 append-only 旧行为。要做不停机轮转策略，
//! 把 `FAILURE_LOG_MAX_BYTES` 配合外部 `logrotate` 用 `copytruncate`
//!（进程内 handle 每行 flush、能扛截断），或靠上面的内置 roll。归档文件
//! 本进程不压缩、不清理——保留期限请外部管理。

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

use chrono::Local;
use serde_json::{json, Value};

use crate::key_pool::mask_key;

/// 一个由 Mutex 守护的全局文件 handle，外加当前文件的字节计数，用于在
/// `FAILURE_LOG_MAX_BYTES` 处轮转。写入很短（单行 JSON），竞争可忽略；
/// 从不跨 await 持锁。
struct RotationState {
    file: File,
    /// 写到*当前*打开文件的字节数。轮转时重置。
    written: u64,
    /// 解析后的 `FAILURE_LOG_MAX_BYTES`。在 ensure_open 时抓取快照，避免进
    /// 程中途改环境变量生效（配置快照）。
    max_bytes: u64,
}
static FAILURE_FILE: Mutex<Option<RotationState>> = Mutex::new(None);

fn log_path() -> String {
    std::env::var("FAILURE_LOG_PATH").unwrap_or_else(|_| "failure.log".to_string())
}

/// `FAILURE_LOG_MAX_BYTES` 对当前文件的上限。0 = 关闭（无限 append——旧行
/// 为）。打开时读取一次。
fn max_bytes_setting() -> u64 {
    std::env::var("FAILURE_LOG_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn ensure_open() {
    // `unwrap_or_else(|p| p.into_inner())` 能扛中毒的 mutex——如果之前某次
    // 写入 panic（这条路径本不该 panic，但 `writeln!`/`flush` 在 fd 损坏时
    // 确实可能 panic），我们仍希望失败日志通道继续写，而不是把之后每条
    // 失败记录都级联 panic 掉。
    let mut guard = FAILURE_FILE
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if guard.is_none() {
        let path = log_path();
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                // 把计数器起点设为当前文件大小（仅当开启轮转时），这样一个
                // 本就很大的文件会在下一次写入就轮转，而不是忽略已有字节。
                let written = f.metadata().map(|m| m.len()).unwrap_or_default();
                *guard = Some(RotationState {
                    file: f,
                    written,
                    max_bytes: max_bytes_setting(),
                });
            }
            Err(e) => {
                eprintln!("failure_log: 无法打开 {}：{}", path, e);
                // 留 None——write_failure 变成 no-op。
            }
        }
    }
}

/// 生成一个短的、近单调的请求 id：unix 秒数 + per-process 计数。不是全局
/// 唯一，但在一个代理生命周期内足以把同一 client 请求的重试关联起来。
fn gen_req_id(counter: u64) -> String {
    let secs = Local::now().timestamp();
    format!("r{:x}_{:x}", secs, counter)
}

/// 内部入口。`category` 是个短字符串 tag，由调用方从下面那组里挑；其余字段
/// 都从调用点派生。
///
/// 故意保留位置参数：每个调用点都已经把所有字段放在作用域里，套个 struct
/// 只是把参数列表挪个地方。
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
    // 测试/CI 退出开关：设了 `FAILURE_LOG_DISABLE`（任意值）就跳过写入，
    // 避免集成测试污染用于生产诊断的 on-disk failure.log。生产环境保持不设。
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

    // 序列化后追加换行。serde_json::to_string 在我们这个 Value 形态下不会
    // panic，但失败路径（不可能发生）上用一句简短 stderr 兜底。
    match serde_json::to_string(&entry) {
        Ok(line) => {
            // 若这行会让当前文件超过上限，则在写之前先轮转。`max_bytes == 0`
            // 关闭轮转。
            if rot.max_bytes > 0 && rot.written + (line.len() as u64) + 1 > rot.max_bytes {
                let path = log_path();
                // flush + 关闭当前文件，roll 到带时间戳的名字，再开新文件。
                // 全程尽力而为：roll 失败就回退到继续用同一 handle，绝不丢
                // 失败记录。
                let _ = rot.file.flush();
                let stamp = now.timestamp();
                if let Err(e) = std::fs::rename(&path, format!("{}.{}", path, stamp)) {
                    eprintln!("failure_log: 轮转 rename 失败（{}）：{}", path, e);
                    // 没能 roll；继续往同一 handle append，并更新 `written`，
                    // 避免每行都重试。
                } else {
                    match OpenOptions::new().create(true).append(true).open(&path) {
                        Ok(new) => {
                            rot.file = new;
                            rot.written = 0;
                        }
                        Err(e) => {
                            eprintln!("failure_log: 轮转后 reopen 失败（{}）：{}", path, e);
                            // `rot.file` 仍持有旧 handle；Unix 上对已打开文件
                            // rename 会让 fd 继续写到被移走的 inode，数据不丢。
                            // **关键**：即使 reopen 失败也要把 `written` 归零，
                            // 否则下一行 `written + len > max_bytes` 仍为真，会
                            // 每行都重复 rename+reopen，对坏掉的文件系统形成一个
                            // spin-on-every-line 的活锁——数据已安全落在被 roll
                            // 走的旧 inode 上，没必要每行再试一遍。后续写入会继
                            // 续在旧 handle（roll 走的文件）上累积，`written` 从 0
                            // 起计，max_bytes 后再尝试一次 reopen（可能届时已恢复）。
                            rot.written = 0;
                        }
                    }
                }
            }
            let _ = writeln!(rot.file, "{}", line);
            let _ = rot.file.flush();
            rot.written += (line.len() as u64) + 1;
        }
        Err(e) => {
            eprintln!("failure_log: 序列化失败：{}", e);
        }
    }
}

/// 从上游错误 body 里抽 `Function '<uuid>': Not found for account '<acct>'`
/// 模式。命中则返回 (function_id, account_id)，否则都返回空串。为保持 crate
/// 依赖零额外，不用 regex；对 NVIDIA 已知的错误形态，简单子串匹配就够。
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
