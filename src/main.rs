use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{info, error};
use tracing_subscriber::EnvFilter;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

use crate::config::Config;
use crate::key_pool::KeyPool;
use crate::proxy::Proxy;
use crate::health::HealthChecker;
use crate::metrics::Metrics;

mod config;
mod key_pool;
mod proxy;
mod health;
mod metrics;
mod responses_compat;
mod codex_config;

#[tokio::main]
async fn main() {
    // Initialize tracing. Honor NVIDIA_LOG_LEVEL before falling back to "info".
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    // Load configuration
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Failed to load configuration: {}", e);
        std::process::exit(1);
    });
    info!("Loaded configuration: {:?}", config);

    // Auto-configure Codex CLI (~/.codex/config.toml + model catalog)
    codex_config::auto_configure(&config.codex, &config.listen_addr);

    // Shared state
    let metrics = Arc::new(Metrics::new());
    let key_pool = Arc::new(KeyPool::new(&config, metrics.clone()));
    // Push initial key-state counts to Prometheus.
    key_pool.refresh_metrics().await;

    let request_timeout = config.request_timeout_seconds;
    let proxy = Proxy::new(
        config.base_url.clone(),
        key_pool.clone(),
        metrics.clone(),
        request_timeout,
    );
    let health_checker = Arc::new(HealthChecker::new(
        key_pool.clone(),
        config.base_url.clone(),
        config.health_check.interval_seconds,
        config.health_check.timeout_seconds,
        config.health_check.max_consecutive_failures,
    ));

    // Graceful shutdown signal shared by all background tasks.
    let stop_signal = Arc::new(tokio::sync::Notify::new());

    // Background cooldown sweep
    let cooldown_stop = stop_signal.clone();
    let key_pool_for_task = key_pool.clone();
    let cooldown_task = tokio::spawn(async move {
        key_pool_for_task.cool_down_task(cooldown_stop).await;
    });

    // Startup health check: run once (detached)
    let health_checker_startup = health_checker.clone();
    tokio::spawn(async move {
        health_checker_startup.run_startup().await;
    });

    // Bad key recovery: periodically check bad keys
    let health_checker_bad = health_checker.clone();
    let bad_key_stop = stop_signal.clone();
    let bad_key_task = tokio::spawn(async move {
        health_checker_bad.run_bad_key_checker(bad_key_stop).await;
    });

    // Metrics endpoint exposed at the configured `metrics_path` (default /metrics).
    let metrics_path = config.observability.metrics_path.clone();
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
        .and_then({
            let metrics = metrics.clone();
            move || {
                let metrics = metrics.clone();
                async move {
                    Ok::<_, Rejection>(warp::reply::with_header(
                        metrics.render(),
                        "Content-Type",
                        "text/plain; version=0.0.4",
                    ))
                }
            }
        });

    // Health endpoint
    let health_route = warp::path!("health")
        .and(warp::get())
        .map(|| warp::http::Response::builder().status(204).body(""));

    // Codex reachability check: GET /v1/models → return our catalog
    let models_route = warp::path!("v1" / "models")
        .and(warp::get())
        .map(|| {
            let catalog = crate::codex_config::get_catalog();
            warp::reply::with_header(
                warp::reply::json(&catalog),
                "Content-Type",
                "application/json",
            )
        });

    // Also handle GET /models (no v1 prefix)
    let models_route_alt = warp::path!("models")
        .and(warp::get())
        .map(|| {
            let catalog = crate::codex_config::get_catalog();
            warp::reply::with_header(
                warp::reply::json(&catalog),
                "Content-Type",
                "application/json",
            )
        });

    // Proxy all paths: need to capture headers
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
                    proxy.handle_request(path, method, headers, body, remote).await
                }
            }
        });

    let routes = metrics_route
        .or(health_route)
        .or(models_route)
        .or(models_route_alt)
        .or(proxy_route)
        .recover(handle_rejection);

    let addr: SocketAddr = config.listen_addr.parse().expect("Invalid listen address");
    info!("Listening on {}, metrics at {}", addr, config.observability.metrics_path);

    let (_, server_future) = warp::serve(routes).bind_with_graceful_shutdown(addr, async move {
        // Wait for Ctrl+C, then signal warp to stop accepting new connections.
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received, draining in-flight requests...");
    });

    // Run the server and a Ctrl+C watcher concurrently. The first to finish
    // triggers a coordinated shutdown (notify background tasks + await server).
    tokio::select! {
        _ = server_future => {
            info!("Server future completed");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, initiating graceful shutdown");
        }
    }

    // Stop background tasks.
    stop_signal.notify_waiters();
    let _ = cooldown_task;
    let _ = bad_key_task;
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
    } else {
        error!("Unhandled rejection: {:?}", err);
        Ok(warp::reply::with_status(
            "Internal server error".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
