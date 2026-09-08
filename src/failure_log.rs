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

/// 把序列化好的单行 JSON + 换行，写入目标文件。`file` 已就绪。
fn write_line(file: &mut File, line: &str) -> std::io::Result<()> {
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")
}

/// 失败日志的异步写入后台。收到序列化行 → 写入 + flush，独占地轮转/回滚。
/// 使用 `std::sync::Mutex`（而非持锁跨 await）：整个 write_failure 体现在
/// channel send 上，是无锁操作，对业务协程透明——不会阻塞 tokio worker
///（刷盘/轮转在专用线程上）。此前写 failure.log 的实现把 `writeln!` +
/// `flush()` 跑在 async 热路径上，一旦上游 brownout（正是写入狂热时刻）
/// 所有请求任务串行阻塞在一个 fd 互斥体上。
static WRITER: Mutex<Option<WriterHandle>> = Mutex::new(None);

struct WriterHandle {
    tx: std::sync::mpsc::Sender<LogEntry>,
}

struct LogEntry {
    line: String,
}

/// 单个后台 writer 任务，串行刷盘。
fn spawn_writer_thread() {
    let (tx, rx) = std::sync::mpsc::channel::<LogEntry>();
    std::thread::Builder::new()
        .name("failure-log-writer".to_string())
        .spawn(move || {
            // 失败日志聚合账号/上游错误体片段，应受进程文件权限保护。
            let mut state = open_rotation_state();
            for entry in rx.iter() {
                let path = log_path();
                let line_len = entry.line.len() as u64;
                state.before_write_rotate(&path, line_len);
                if let Some(st) = &mut state.inner {
                    let _ = write_line(&mut st.file, &entry.line);
                    // flush 以防进程崩溡丢掉末位日志——失败日志用于事后
                    // 诊断，丢尾必然。刷盘成本摊摊在专用线程上，不挂热路径。
                    let _ = st.file.flush();
                    st.written += line_len + 1;
                }
            }
        })
        .expect("failure-log-writer thread spawn");
    let mut g = WRITER.lock().unwrap_or_else(|p| p.into_inner());
    *g = Some(WriterHandle { tx });
}

/// 确保 writer 线程已就绪。
fn ensure_writer() {
    // 快速路径：已挂则直接返回。
    {
        let g = WRITER.lock().unwrap_or_else(|p| p.into_inner());
        if g.is_some() {
            return;
        }
    }
    spawn_writer_thread();
}

/// 持有打开文件 handle + 轮转状态。
struct RotationState {
    inner: Option<RotationFile>,
    max_bytes: u64,
}
struct RotationFile {
    file: File,
    written: u64,
}

impl RotationState {
    /// 在写入 `line` 前，若它会让当前文件超过 `max_bytes`，则轮转旧文件到
    /// 带时间戳归档名并开新文件。全程尽力而为：roll/reopen 失败就回退到
    /// 继续用旧 handle，绝不丢日志。
    fn before_write_rotate(&mut self, path: &str, line_len: u64) {
        if self.max_bytes == 0 {
            return;
        }
        let Some(st) = self.inner.as_mut() else {
            return;
        };
        if st.written + line_len + 1 > self.max_bytes {
            let _ = st.file.flush();
            let stamp = Local::now().timestamp();
            match std::fs::rename(path, format!("{}.{}", path, stamp)) {
                Ok(_) => match OpenOptions::new().create(true).append(true).open(path) {
                    Ok(new) => {
                        set_private_perms(path, &new);
                        st.file = new;
                        st.written = 0;
                    }
                    Err(e) => {
                        eprintln!("failure_log: 轮转 reopen 失败（{}）：{}", path, e);
                        // roll 走的旧 inode fd 仍可写；归零 written 避免活锁。
                        st.written = 0;
                    }
                },
                Err(e) => {
                    eprintln!("failure_log: 轮转 rename 失败（{}）：{}", path, e);
                    st.written = 0;
                }
            }
        }
    }
}

fn open_rotation_state() -> RotationState {
    RotationState {
        inner: open_failure_file(),
        max_bytes: max_bytes_setting(),
    }
}

/// 在创建时设置 `0600` 权限（Unix），防其它本地用户读含 key/账号 id 的日志。
fn open_failure_file() -> Option<RotationFile> {
    let path = log_path();
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .ok()?;
    set_private_perms(&path, &f);
    let written = f.metadata().map(|m| m.len()).unwrap_or_default();
    Some(RotationFile { file: f, written })
}

/// 尽力把日志文件权限收到 `0600`（owner-only）。跨平台 no-op 在 Windows。
fn set_private_perms(_path: &str, file: &File) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = file.metadata() {
            // 仅当当前权限放得比 0600 宽松时才 tighten，免干扰运维预设
            // 更高隔离的部署。
            let cur = meta.permissions().mode() & 0o777;
            if cur & 0o077 != 0 {
                let _ = file.set_permissions(std::fs::Permissions::from_mode(cur & !0o077));
            }
        }
    }
    let _ = file;
}

/// `FAILURE_LOG_MAX_BYTES` 对当前文件的上限。0 = 关闭（无限 append——旧行为）。
fn max_bytes_setting() -> u64 {
    std::env::var("FAILURE_LOG_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn log_path() -> String {
    std::env::var("FAILURE_LOG_PATH").unwrap_or_else(|_| "failure.log".to_string())
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
    ensure_writer();
    let tx = {
        let g = WRITER.lock().unwrap_or_else(|p| p.into_inner());
        g.as_ref().map(|h| h.tx.clone())
    };
    let Some(tx) = tx else { return };

    let now = Local::now();
    let ts = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

    let mut entry = json!({
        "ts": ts,
        "error_time": ts.clone(),
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

    // 序列化后追加换行。失败时回退 stderr，绝不 panic 阻塞热路径。
    match serde_json::to_string(&entry) {
        Ok(line) => {
            // 单行日志跨 await 压回 channel（背压可无限——INFO 量未见上限）。
            let _ = tx.send(LogEntry { line });
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
