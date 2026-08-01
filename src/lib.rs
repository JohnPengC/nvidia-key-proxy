//! Library façade: re-exports all modules and hosts the application state
//! and route builder so that integration tests can exercise them without
//! reaching into the binary crate (`src/main.rs`).

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

/// Bundles all runtime‑shared state. `Clone` so route builders can each hold
/// their own handle to the shared `Arc` fields without fighting borrow rules.
#[derive(Clone)]
pub struct AppState {
    pub metrics: Arc<Metrics>,
    pub key_pool: Arc<KeyPool>,
    pub proxy: Proxy,
    pub health_checker: Arc<HealthChecker>,
    pub stop_signal: Arc<tokio::sync::Notify>,
    pub config: Config,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        // Tracing must be initialized before we emit any logs.
        init_tracing(&config);
        tracing::info!("Loaded configuration: {:?}", config);
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
        let key_pool = Arc::new(KeyPool::new(&config, metrics.clone()));

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
        let health_checker = Arc::new(HealthChecker::new(
            key_pool.clone(),
            config.base_url.clone(),
            config.health_check.interval_seconds,
            config.health_check.timeout_seconds,
            config.health_check.max_consecutive_failures,
        ));
        let stop_signal = Arc::new(tokio::sync::Notify::new());

        Self {
            metrics,
            key_pool,
            proxy,
            health_checker,
            stop_signal,
            config,
        }
    }
}

/// Initialise tracing based on env vars **and** the config file.
///
/// Idempotent: subsequent calls (e.g. when multiple `AppState`s are built in
/// tests) are silently ignored once a global default subscriber is installed.
pub fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.observability.log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    // `try_init` returns Err when a global default subscriber already exists;
    // in that case there is nothing to do.
    let _ = tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_env_filter(filter)
        .try_init();
}

/// Build all warp routes using the shared `AppState`.
pub fn build_routes(
    state: &AppState,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    // Outermost request logger — fires *before* any path matching or guard,
    // so every connection warp processes shows up in stdout. This is the
    // ground-truth "did the client reach us at all?" signal, useful for
    // diagnosing cc-switch / Cursor / IDE configurations that mis-target
    // the proxy. Without this, guard rejections (404) and handler rejections
    // (503) land before the existing `Proxying request to ...` log line and
    // are invisible.
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
            tracing::info!(
                target: "proxy::entry",
                method = %method,
                path = %path.as_str(),
                host = %host,
                user_agent = %ua,
                "incoming request"
            );
            ()
        })
        .untuple_one();

    let health_route = warp::path!("health")
        .and(warp::get())
        .map(|| warp::http::Response::builder().status(204).body(""));

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
                    // Front-door auth: when enabled, require a matching
                    // `Authorization: Bearer <bearer_token>` before forwarding.
                    if auth.enabled {
                        let expected = format!("Bearer {}", auth.bearer_token);
                        match headers.get("authorization").and_then(|v| v.to_str().ok()) {
                            Some(v) if v == expected => {}
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
        .and_then(move || {
            let metrics = metrics.clone();
            async move {
                if !enable_metrics {
                    return Err::<_, Rejection>(warp::reject::not_found());
                }
                Ok(warp::reply::with_header(
                    metrics.render(),
                    "Content-Type",
                    "text/plain; version=0.0.4",
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
    }    if err.find::<AdminDisabled>().is_some() {
        return Ok(warp::reply::with_status(
            "Not found".to_string(),
            StatusCode::NOT_FOUND,
        ));
    }
    if err.find::<warp::reject::InvalidQuery>().is_some() {
        error!("Invalid query");
        Ok(warp::reply::with_status(
            "Invalid query".to_string(),
            StatusCode::BAD_REQUEST,
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
        error!("Unhandled rejection: {:?}", err);
        Ok(warp::reply::with_status(
            "Internal server error".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

// ── Front-door auth & admin API ───────────────────────────────────────────

/// Rejection returned when the proxy's own front-door auth (or the admin API
/// auth) fails. Surfaced to the client as 401.
#[derive(Debug, Clone)]
struct Unauthorized;
impl warp::reject::Reject for Unauthorized {}

/// Rejection returned when the admin API is queried but `auth.admin_token`
/// is unset. Surfaced as 404 so the admin surface can't be probed. Deliberately
/// NOT `not_found()` — warp's `.or()` falls through on `not_found`, which would
/// let a `/admin/keys` GET fall through to the POST/DELETE routes and report
/// 405 instead of 404.
#[derive(Debug, Clone)]
struct AdminDisabled;
impl warp::reject::Reject for AdminDisabled {}

/// Check the admin bearer token from the request headers. When `admin_token`
/// is empty the whole admin surface is hidden (404), so it can't be probed;
/// otherwise the caller must present `Authorization: Bearer <admin_token>`.
fn check_admin_token(
    admin_token: &str,
    headers: &warp::http::HeaderMap,
) -> Result<(), Rejection> {
    if admin_token.is_empty() {
        return Err(warp::reject::custom(AdminDisabled));
    }
    let expected = format!("Bearer {}", admin_token);
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some(v) if v == expected => Ok(()),
        _ => Err(warp::reject::custom(Unauthorized)),
    }
}

/// Serialize `val` into a JSON `Response` with the given status.
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

/// Extract key strings from an admin add-keys JSON body. Accepts a bare array
/// of strings, a single string, or an object `{ "keys": [...] }`.
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

/// Admin management API mounted under `/admin`. Each handler authenticates
/// the caller against `auth.admin_token` (via `check_admin_token`); when that
/// token is unset the route 404s so the surface can't be probed.
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

    let add = warp::path!("admin" / "keys")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::header::headers_cloned())
        .and_then({
            let s = state.clone();
            move |body: serde_json::Value, headers: warp::http::HeaderMap| {
                let s = s.clone();
                async move {
                    check_admin_token(&s.config.auth.admin_token, &headers)?;
                    let keys = extract_keys(&body);
                    let mut added = Vec::new();
                    let mut already_present = Vec::new();
                    let mut invalid = Vec::new();
                    for k in keys {
                        if k.len() < 8 {
                            invalid.push(k);
                            continue;
                        }
                        if s.key_pool.add_key(k.clone()).await {
                            added.push(k);
                        } else {
                            already_present.push(k);
                        }
                    }
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
