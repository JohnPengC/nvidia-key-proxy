//! 库门面：重新导出所有模块，并承载应用状态与路由构造器，以便集成测试
//! 可以在不深入二进制 crate（`src/main.rs`）的情况下跑全栈。

pub mod config;
pub mod failure_log;
pub mod health;
pub mod key_pool;
pub mod metrics;
pub mod proxy;

use std::sync::Arc;

use chrono::Local;
use tracing::error;
use tracing_subscriber::EnvFilter;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

pub use crate::config::*;
use crate::health::HealthChecker;
use crate::key_pool::KeyPool;
use crate::metrics::Metrics;
use crate::proxy::Proxy;

struct LocalTimer;
impl tracing_subscriber::fmt::time::FormatTime for LocalTimer {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        let now = Local::now();
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S%.3f %:z"))
    }
}

/// 把所有运行时共享状态打包到一起。`Clone` 让各路由构造器各自持有一份
/// 共享 `Arc` 字段的 handle，互不争夺借用规则。
#[derive(Clone)]
pub struct AppState {
    pub metrics: Arc<Metrics>,
    pub key_pool: Arc<KeyPool>,
    pub proxy: Proxy,
    pub health_checker: Arc<HealthChecker>,
    pub stop_signal: Arc<tokio::sync::Notify>,
    pub config: Config,
    /// 启动健康检查完成标志；`/health` 在其为 false 时回 503。
    pub health_ready: Arc<std::sync::atomic::AtomicBool>,
    /// 最近一次 `count_states` 的活跃 key 计数缓存，供 `/health` 同步读以判断
    /// 池子是否仍有可用 key（S6：0 个活跃 key → 503，让 LB 摘流）。由
    /// `KeyPool::refresh_metrics` 在每个扫描周期刷新。至多落后一个
    /// `cool_down_task`/健康扫描周期。
    pub active_keys_cached: Arc<std::sync::atomic::AtomicI64>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        // tracing 由调用方（`main` / 测试入口）先于本函数调 `init_tracing` 完成；
        // 这里不再自调，避免在每个 `AppState::new`（main 一次、集成测试 N 次）里
        // 重试装 subscriber 并误关 log channel guard。详见 `init_tracing` 文档。
        tracing::info!("已加载配置：{:?}", config);
        eprintln!(
            "[config-load] request_timeout_seconds={}",
            config.request_timeout_seconds
        );
        let config_path = std::env::var("NVIDIA_CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
        eprintln!("[config-load] config_path={}", config_path);
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            eprintln!("[config-load] config_file_len={}", content.len());
        } else {
            eprintln!("[config-load] config_file_read=FAILED");
        }

        let metrics = Arc::new(Metrics::new());
        let active_keys_cached = Arc::new(std::sync::atomic::AtomicI64::new(
            config.keys.len() as i64,
        ));
        let mut key_pool_unwrapped = KeyPool::new(&config, metrics.clone());
        // S6：挂 `/health` 活跃计数缓存（在包 Arc 之前 mutate，绕开 Arc 不可变借用）。
        key_pool_unwrapped.set_active_keys_cache(active_keys_cached.clone());
        let key_pool = Arc::new(key_pool_unwrapped);
            let request_timeout = config.request_timeout_seconds;
        let request_timeout_streaming = config.request_timeout_streaming_seconds;
        let retry_config = config.retry.clone();
        let model_aliases = config.model_aliases.clone();
        let proxy = Proxy::new(
            config.base_url.clone(),
            key_pool.clone(),
            metrics.clone(),
            request_timeout,
            request_timeout_streaming,
            retry_config,
            config.rate_limiter.token_wait_ms,
            model_aliases,
            config.inject_stream_usage,
            config.synthesize_empty_choices,
            config.retry_on_empty_shell,
            config.empty_shell_retry_max_tokens,
            config.empty_shell_retry_attempts,
            config.empty_shell_probe_before_retry,
            config.empty_shell_retry_backoff_base_ms,
            config.empty_shell_retry_backoff_max_ms,
            config.empty_shell_pre_clamp_max_tokens.clone(),
            config.empty_shell_no_retry_prompt_tokens,
            config.request_max_prompt_tokens,
            crate::proxy::EstPromptCosts {
                image_cost: config.est_image_token_cost,
                audio_token_per_kb: config.est_audio_token_per_kb,
                audio_token_cap: config.est_audio_token_cap,
                unknown_part_cap: config.est_unknown_part_token_cap,
            },
            config.circuit_breaker_threshold,
            config.circuit_breaker_per_model,
            config.retry_on_503,
            config.probe_before_503_retry,
            config.retry_on_500,
            config.max_tokens_max,
            config.concurrency_limit_per_model.clone(),
            config.concurrency_queue_timeout_ms,
            config.upstream_connect_timeout_secs,
            config.upstream_pool_idle_timeout_secs,
            config.retry_jitter_ms,
        );
        let health_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let health_checker = Arc::new(HealthChecker::new(
            key_pool.clone(),
            config.base_url.clone(),
            config.health_check.interval_seconds,
            config.health_check.timeout_seconds,
            config.health_check.max_consecutive_failures,
            health_ready.clone(),
        ));
        let stop_signal = Arc::new(tokio::sync::Notify::new());

        Self {
            metrics,
            key_pool,
            proxy,
            health_checker,
            stop_signal,
            config,
            health_ready,
            active_keys_cached,
        }
    }
}

/// 基于环境变量**和**配置文件初始化 tracing。
///
/// **异步输出**：subscriber 写到 `tracing_appender::non_blocking` 包装的
/// stdout，日志的格式化与 `write` syscall 由一个专用后台 worker 挪走，业务
/// 协程只 `try_send` 到 channel 即返回——不再因 stdout 被慢消费方（`docker logs`
/// 抓取慢、journald 限速、管道拥塞）占用而把 IO 抖动落在请求路径上。
///
/// 返回 `Some(WorkerGuard)` 当且仅这一次真的把 subscriber 装上了（首次调用）。
/// Guard drop 时把 channel 残留事件排空后关 worker——这是不可丢尾部日志的关
/// 键：**调用方必须把返回的 guard 持到进程退出之前**。`main` 把它绑在栈上，
/// `async fn main` return 时栈解、guard drop、worker flush 完最后几行再退。
///
/// **不要**把 guard 放进进程级 `static`：Rust 进程正常退出时不 drop static
///（不像 C++ thread-local atexit 那种确定性析构），尾部日志会被吞。guard 必须
/// 是有**确定生命周期**的栈变量。`try_init` 在 subscriber 已存在时返回 Err，
/// 本函数返回 None——第二次调用者拿不到 guard（subscriber 只装一次）。集成测
/// 试不调用 `init_tracing`：它们没有 tracing subscriber，宏变 no-op，测试不读
/// stdout，但也不会因 guard 生命周期问题丢东西。
pub fn init_tracing(config: &Config) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.observability.log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    // non_blocking(stdout)：写挪到后台线程，业务协程非阻塞。无界 channel——
    // info 级日志量远未到压垮内存，无界保证不丢日志，符合"日志不能丢"的运维
    // 预期；debug 级 verbose 通常临时排障、不是常驻，无需为它上背压。
    let (nb, guard) = tracing_appender::non_blocking(std::io::stdout());
    let installed = tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_writer(nb)
        .with_env_filter(filter)
        .try_init()
        .is_ok();
    if installed {
        Some(guard)
    } else {
        // subscriber 已存在：drop guard 让这个 channel flush 后关，避免泄漏。
        // 调用方拿到 None，不持这个 guard。
        drop(guard);
        None
    }
}

/// 用共享 `AppState` 构造所有 warp 路由。
pub fn build_routes(
    state: &AppState,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    // 最外层请求日志——在*任何*路径匹配或 guard 之前触发，所以 warp 处理的
    // 每条连接都会记一行 DEBUG。这是"client 到底有没有够到我们"的基准信号，
    // 用来诊断误指代理的 cc-switch / Cursor / IDE 配置。没有它，guard 拒绝
    //（404）和 handler 拒绝（503）落在既有 `Proxying request to ...` 日志行
    // 之前，就看不见了。默认 INFO 级下不可见——调查连接/误指问题时把
    // `observability.log_level = "debug"` 临时调高即可。
    let entry_log = warp::path::full()
        .and(warp::method())
        .and(warp::header::headers_cloned())
        .map(|path: warp::path::FullPath, method: warp::http::Method, headers: warp::http::HeaderMap| {
            let host = headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let ua = headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            tracing::debug!(
                target: "proxy::entry",
                method = %method,
                path = %path.as_str(),
                host = %host,
                user_agent = %ua,
                "收到请求"
            );
            
        })
        .untuple_one();

    let health_ready = state.health_ready.clone();
    let active_keys_cached = state.active_keys_cached.clone();
    let health_route = warp::path!("health")
        .and(warp::get())
        .map(move || {
            // S5：启动健康检查（`HealthChecker::run_startup`）完成前回 503，使
            // 负载均衡器/探针在此期间摘掉本实例，避免把流量打到尚未对池做完一次
            // 初始清点的代理上。`run_startup` 在退出前把 `health_ready` 置 true。
            //
            // S6：启动就绪**之后**仍要反映池子活性——若所有 key 都漂到
            // Cooling/Disabled（凭证轮换全失效、或冷却/禁用 bug 叠加），回 204
            // 会让 LB 继续打流量、全 503。这里读一个由 `cool_down_task`/健康 task
            // 周期刷新的 `AtomicI64` 缓存（同步读，无需 async——`count_states` 是
            // async 不能在 `map` 里 await）。缓存至多落后一个扫描周期，对探针
            // 频率而言可接受。始终豁免前门鉴权。
            if !health_ready.load(std::sync::atomic::Ordering::Relaxed) {
                return warp::http::Response::builder().status(503).body("");
            }
            if active_keys_cached.load(std::sync::atomic::Ordering::Relaxed) <= 0 {
                return warp::http::Response::builder().status(503).body("");
            }
            warp::http::Response::builder().status(204).body("")
        });

    let proxy_route = warp::path::full()
        .and_then(|p: warp::path::FullPath| {
            let path = p.as_str().to_string();
            async move {
                if path.starts_with("/v1/") || path == "/v1" {
                    Ok(path)
                } else {
                    Err(warp::reject::not_found())
                }
            }
        })
        .and(warp::method())
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and(warp::addr::remote())
        .and_then({
            let proxy = state.proxy.clone();
            let auth = state.config.auth.clone();
            move |path: String, method, headers: warp::http::HeaderMap, body, remote| {
                let proxy = proxy.clone();
                let auth = auth.clone();
                async move {
                    // 前置鉴权：开启后，转发前要求带匹配的
                    // `Authorization: Bearer <bearer_token>`。
                    if auth.enabled {
                        let expected = format!("Bearer {}", auth.bearer_token);
                        match headers.get("authorization").and_then(|v| v.to_str().ok()) {
                            Some(v) if constant_time_eq(v.as_bytes(), expected.as_bytes()) => {}
                            _ => {
                                return Err::<_, Rejection>(warp::reject::custom(Unauthorized));
                            }
                        }
                    }
                    proxy
                        .handle_request(path, method, headers, body, remote)
                        .await
                }
            }
        });

    let metrics_path = state.config.observability.metrics_path.clone();
    let enable_metrics = state.config.observability.enable_metrics;
    let metrics = state.metrics.clone();
    // `auth.enabled` 时 `/metrics` 也必须过前门 bearer 校验——它暴露 per-model
    // 流量、token 数、key 池基数与断路器触发，是可信运维面，不该对任意 client
    // 开放。`/admin/*` 有它自己的 `admin_token` 独立门（见 `check_admin_token`），
    // 故不叠加前门。`/health` 始终豁免（见下）。此前 metrics 路由排在 `.or()`
    // 链中 proxy 的 auth 块之前，绕过了前门——这是 S1 的根因。
    let front_door_auth = state.config.auth.clone();
    let metrics_route = warp::path::full()
        .and_then(move |p: warp::path::FullPath| {
            let metrics_path = metrics_path.clone();
            async move {
                if p.as_str() == metrics_path {
                    Ok(())
                } else {
                    Err(warp::reject::not_found())
                }
            }
        })
        .untuple_one()
        .and(warp::get())
        .and(warp::header::headers_cloned())
        .and_then(move |headers: warp::http::HeaderMap| {
            let metrics = metrics.clone();
            let front_door_auth = front_door_auth.clone();
            async move {
                // 复用前门 bearer 校验：开启时要求 `Authorization: Bearer <token>`。
                if front_door_auth.enabled {
                    let expected = format!("Bearer {}", front_door_auth.bearer_token);
                    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
                        Some(v) if constant_time_eq(v.as_bytes(), expected.as_bytes()) => {}
                        _ => {
                            return Err::<_, Rejection>(warp::reject::custom(Unauthorized));
                        }
                    }
                }
                if !enable_metrics {
                    return Err::<_, Rejection>(warp::reject::not_found());
                }
                // 显式声明 `charset=utf-8`：HTTP/1.1 规定未声明 charset 的
                // `text/*` 默认按 `iso-8859-1` 解码（RFC 7231 §3.1.1.1），
                // 这里的 help 文本是中文（UTF-8），不声明就会让严格遵守规约
                // 的抓取端按 latin1 处理字节，并在后续 GBK/latin1↔UTF-8 转码
                // 链路里被弄成 mojibake。Prometheus parser 本身能吃 UTF-8。
                Ok(warp::reply::with_header(
                    metrics.render(),
                    "Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8",
                ))
            }
        });

    let admin = admin_api(state.clone());

    entry_log
        .and(
            metrics_route
                .or(health_route)
                .or(admin)
                .or(proxy_route),
        )
        .recover(handle_rejection)
}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    if err.find::<Unauthorized>().is_some() {
        return Ok(warp::reply::with_status(
            "Authentication required".to_string(),
            StatusCode::UNAUTHORIZED,
        ));
    } else if err.find::<AdminDisabled>().is_some() {
        return Ok(warp::reply::with_status(
            "Not found".to_string(),
            StatusCode::NOT_FOUND,
        ));
    }
    if err.find::<warp::reject::InvalidQuery>().is_some() {
        error!("非法 query");
        Ok(warp::reply::with_status(
            "Invalid query".to_string(),
            StatusCode::BAD_REQUEST,
        ))
    } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
        Ok(warp::reply::with_status(
            "Payload too large".to_string(),
            StatusCode::PAYLOAD_TOO_LARGE,
        ))
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        Ok(warp::reply::with_status(
            "Method not allowed".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ))
    } else if err.is_not_found() {
        Ok(warp::reply::with_status(
            "Not found".to_string(),
            StatusCode::NOT_FOUND,
        ))
    } else {
        error!("未处理的 rejection：{:?}", err);
        Ok(warp::reply::with_status(
            "Internal server error".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

// ── 前置鉴权与管理 API ────────────────────────────────────────────────────

/// 代理自身前置鉴权（或管理 API 鉴权）失败时返回的 rejection。对 client
/// 表现为 401。
#[derive(Debug, Clone)]
struct Unauthorized;
impl warp::reject::Reject for Unauthorized {}

/// 查询管理 API 但 `auth.admin_token` 未设时返回的 rejection。表现为 404，
/// 让管理面无法被探测。故意**不**用 `not_found()`——warp 的 `.or()` 会对
/// `not_found` 落空，那样一个 `/admin/keys` 的 GET 会落空到 POST/DELETE
/// 路由、报 405 而不是 404。
#[derive(Debug, Clone)]
struct AdminDisabled;
impl warp::reject::Reject for AdminDisabled {}

/// 常数时间字节比较，用于 bearer-token 校验，避免远端调用者通过测量 `==`
/// 的耗时还原 token（或其长度）。仅长度不一致就直接返回 false（与
/// `subtle::ConstantTimeCompare`/Go 的 `crypto/subtle.ConstantTimeCompare`
/// 一致；显然的长度侧信道被接受，因为格式良好的
/// `Authorization: Bearer <token>` 头几乎总带固定 `Bearer ` 前缀）。逐字节
/// 循环总是扫完整个公共长度、把差异累计进一个累加器，不短路。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 从请求头里校验管理 bearer token。`admin_token` 为空时整个管理面被藏
/// 起来（404）、无法被探测；否则调用方必须提供
/// `Authorization: Bearer <admin_token>`。
fn check_admin_token(
    admin_token: &str,
    headers: &warp::http::HeaderMap,
) -> Result<(), Rejection> {
    if admin_token.is_empty() {
        return Err(warp::reject::custom(AdminDisabled));
    }
    let expected = format!("Bearer {}", admin_token);
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some(v) if constant_time_eq(v.as_bytes(), expected.as_bytes()) => Ok(()),
        _ => Err(warp::reject::custom(Unauthorized)),
    }
}

/// 把 `val` 序列化成给定状态码的 JSON `Response`。
fn json_reply<T: serde::Serialize>(status: u16, val: &T) -> warp::reply::Response {
    let body = serde_json::to_vec(val).unwrap_or_default();
    warp::http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(warp::hyper::Body::from(body))
        .unwrap_or_else(|_| {
            warp::http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(warp::hyper::Body::empty())
                .unwrap()
        })
}

/// 从管理「加 key」的 JSON body 里抽 key 字符串。接受裸字符串数组、单个
/// 字符串，或对象 `{ "keys": [...] }`。
fn extract_keys(body: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(arr) = body.as_array() {
        for v in arr {
            if let Some(s) = v.as_str() {
                out.push(s.to_string());
            }
        }
    } else if let Some(s) = body.as_str() {
        out.push(s.to_string());
    } else if let Some(arr) = body.get("keys").and_then(|x| x.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                out.push(s.to_string());
            }
        }
    }
    out
}

/// 挂在 `/admin` 下的管理 API。每个 handler 都用 `auth.admin_token` 校验
/// 调用方（经 `check_admin_token`）；该 token 未设时路由返回 404，使管理面
/// 无法被探测。
fn admin_api(state: AppState) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let list = warp::path!("admin" / "keys")
        .and(warp::get())
        .and(warp::header::headers_cloned())
        .and_then({
            let s = state.clone();
            move |headers: warp::http::HeaderMap| {
                let s = s.clone();
                async move {
                    check_admin_token(&s.config.auth.admin_token, &headers)?;
                    let snap = s.key_pool.snapshot().await;
                    Ok::<_, Rejection>(json_reply(200, &snap))
                }
            }
        });

    // 给管理「加 key」body 设上限。`warp::body::json()` 默认无大小限制，会在
    // `extract_keys` 跑起来之前就把整个 body 全缓冲进内存——一个粘了巨型 key
    // 文件的已认证调用方（或持 admin token 的恶意一方）能把进程 OOM，或在
    // `add_keys_batch` 里制造病态锁抖动。1 MiB 对任何现实的 add-keys 负载都
    // 富余（一千个 80 字节 key + JSON 框架约 100 KiB），并以 413 直接挡掉 OOM
    // 这一类。
    let add = warp::path!("admin" / "keys")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::json())
        .and(warp::header::headers_cloned())
        .and_then({
            let s = state.clone();
            move |body: serde_json::Value, headers: warp::http::HeaderMap| {
                let s = s.clone();
                async move {
                    check_admin_token(&s.config.auth.admin_token, &headers)?;
                    let keys = extract_keys(&body);
                    let (added, already_present, invalid) =
                        s.key_pool.add_keys_batch(keys).await;
                    Ok::<_, Rejection>(json_reply(
                        200,
                        &serde_json::json!({
                            "added": added,
                            "already_present": already_present,
                            "invalid": invalid,
                        }),
                    ))
                }
            }
        });

    let del = warp::path!("admin" / "keys" / String)
        .and(warp::delete())
        .and(warp::header::headers_cloned())
        .and_then({
            let s = state.clone();
            move |key: String, headers: warp::http::HeaderMap| {
                let s = s.clone();
                async move {
                    check_admin_token(&s.config.auth.admin_token, &headers)?;
                    let removed = s.key_pool.remove_key(&key).await;
                    if removed {
                        Ok::<_, Rejection>(json_reply(
                            200,
                            &serde_json::json!({ "removed": true, "key": key }),
                        ))
                    } else {
                        Ok::<_, Rejection>(json_reply(
                            404,
                            &serde_json::json!({ "removed": false, "key": key }),
                        ))
                    }
                }
            }
        });

    list.or(add).or(del)
}
