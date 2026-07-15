use std::net::SocketAddr;

use tracing::info;

use nvidia_key_proxy::{build_routes, AppState, Config};

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
        addr, state.config.observability.metrics_path
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