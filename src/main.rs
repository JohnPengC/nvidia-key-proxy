use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

use crate::config::Config;
use crate::health::HealthChecker;
use crate::key_pool::KeyPool;
use crate::metrics::Metrics;
use crate::proxy::Proxy;

mod codex_config;
mod config;
mod failure_log;
mod health;
mod key_pool;
mod metrics;
mod proxy;
mod responses_compat;
mod cursor_config;

// -----------------------------------------------------------------------------
// Helper structs / functions – split responsibilities from `main` for readability
// -----------------------------------------------------------------------------

/// Bundles all runtime‑shared state.
struct AppState {
    metrics: Arc<Metrics>,
    key_pool: Arc<KeyPool>,
    proxy: Proxy,
    health_checker: Arc<HealthChecker>,
    stop_signal: Arc<tokio::sync::Notify>,
    config: Config,
}

impl AppState {
    fn new(config: Config) -> Self {
        // Tracing must be initialized before we emit any logs.
        init_tracing(&config);
        info!("Loaded configuration: {:?}", config);

        // Apply optional Cursor configuration.
        if let Some(ref cursor_cfg) = config.cursor {
            cursor_config::apply(cursor_cfg);
        }
        // Auto‑configure Codex CLI.
        codex_config::auto_configure(&config.codex, &config.listen_addr);

        // Shared components.
        let metrics = Arc::new(Metrics::new());
        let key_pool = Arc::new(KeyPool::new(&config, metrics.clone()));
        // Populate initial key metrics (awaited later in `main`).

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
fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.observability.log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_timer(tracing_subscriber::fmt::time::SystemTime)
        .with_env_filter(filter)
        .init();
}

/// Build all warp routes using the shared `AppState`.
fn build_routes(state: &AppState) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    // Health endpoint
    let health_route = warp::path!("health")
        .and(warp::get())
        .map(|| warp::http::Response::builder().status(204).body(""));

    // Model catalog (Codex compatibility)
    let models_route = warp::path!("v1" / "models").and(warp::get()).map(|| {
        let catalog = crate::codex_config::get_catalog();
        warp::reply::with_header(
            warp::reply::json(&catalog),
            "Content-Type",
            "application/json",
        )
    });
    let models_route_alt = warp::path!("models").and(warp::get()).map(|| {
        let catalog = crate::codex_config::get_catalog();
        warp::reply::with_header(
            warp::reply::json(&catalog),
            "Content-Type",
            "application/json",
        )
    });

    // Proxy all other paths – clone the proxy for the closure.
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

    // Metrics endpoint – honour the config flag.
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

    // Compose all routes and attach a common rejection handler.
    metrics_route
        .or(health_route)
        .or(models_route)
        .or(models_route_alt)
        .or(proxy_route)
        .recover(handle_rejection)
}

#[tokio::main]
async fn main() {
    // Load configuration (may exit on error).
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Failed to load configuration: {}", e);
        std::process::exit(1);
    });

    // Build shared application state – this also initialises tracing.
    let state = AppState::new(config);
    // Ensure metrics reflect the initial key pool state.
    state.key_pool.refresh_metrics().await;

    // Spawn background tasks.
    let cooldown_handle = {
        let key_pool = state.key_pool.clone();
        let stop = state.stop_signal.clone();
        tokio::spawn(async move { key_pool.cool_down_task(stop).await })
    };
    // One‑off startup health check.
    {
        let checker = state.health_checker.clone();
        tokio::spawn(async move { checker.run_startup().await });
    }
    // Periodic bad‑key recovery.
    let bad_key_handle = {
        let checker = state.health_checker.clone();
        let stop = state.stop_signal.clone();
        tokio::spawn(async move { checker.run_bad_key_checker(stop).await })
    };

    // Build warp routes.
    let routes = build_routes(&state);
    let addr: SocketAddr = state
        .config
        .listen_addr
        .parse()
        .expect("Invalid listen address");
    info!(
        "Listening on {}, metrics at {}",
        addr,
        state.config.observability.metrics_path
    );

    // Serve with graceful shutdown support.
    let (_, server_future) = warp::serve(routes).bind_with_graceful_shutdown(addr, async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received, draining in‑flight requests...");
    });

    // Wait for either the server to finish or a Ctrl+C.
    tokio::select! {
        _ = server_future => {
            info!("Server future completed");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, initiating graceful shutdown");
        }
    }

    // Notify background tasks to stop and drop their handles.
    state.stop_signal.notify_waiters();
    drop(cooldown_handle);
    drop(bad_key_handle);
    info!("Shutdown complete");
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
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