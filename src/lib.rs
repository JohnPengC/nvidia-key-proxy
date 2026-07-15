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

use tracing::error;
use tracing_subscriber::EnvFilter;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

pub use crate::config::*;
use crate::health::HealthChecker;
use crate::key_pool::KeyPool;
use crate::metrics::Metrics;
use crate::proxy::Proxy;

/// Bundles all runtime‑shared state.
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

        let metrics = Arc::new(Metrics::new());
        let key_pool = Arc::new(KeyPool::new(&config, metrics.clone()));

        let request_timeout = config.request_timeout_seconds;
        let retry_config = config.retry.clone();
        let proxy = Proxy::new(
            config.base_url.clone(),
            key_pool.clone(),
            metrics.clone(),
            request_timeout,
            retry_config,
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
        .with_timer(tracing_subscriber::fmt::time::SystemTime)
        .with_env_filter(filter)
        .try_init();
}

/// Build all warp routes using the shared `AppState`.
pub fn build_routes(state: &AppState) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let health_route = warp::path!("health")
        .and(warp::get())
        .map(|| warp::http::Response::builder().status(204).body(""));

    let proxy_route = warp::path::full()
        .map(|p: warp::path::FullPath| p.as_str().to_string())
        .and(warp::method())
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and(warp::addr::remote())
        .and_then({
            let proxy = state.proxy.clone();
            move |path: String, method, headers, body, remote| {
                let proxy = proxy.clone();
                async move { proxy.handle_request(path, method, headers, body, remote).await }
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

    metrics_route
        .or(health_route)
        .or(proxy_route)
        .recover(handle_rejection)
}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
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