use std::net::SocketAddr;

use tracing::info;

use nvidia_key_proxy::{build_routes, AppState, Config, init_tracing};

#[tokio::main]
async fn main() {
    // 关闭代理环境变量，强制直连
    std::env::remove_var("ALL_PROXY");
    std::env::remove_var("all_proxy");
    std::env::remove_var("HTTP_PROXY");
    std::env::remove_var("http_proxy");
    std::env::remove_var("HTTPS_PROXY");
    std::env::remove_var("https_proxy");
    // 加载配置（出错时可能直接退出）。
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Failed to load configuration: {}", e);
        std::process::exit(1);
    });

    // 异步 tracing：guard 绑在 main 栈上活到函数末——`async fn main` return 时
    // guard drop、后台 worker 排空尾部事件再退，不丢尾部日志。必须排在
    // `AppState::new`（发出第一条 `info!("已加载配置")`）之前。
    let _log_guard = init_tracing(&config);

    // 构建共享应用状态（不再自调 init_tracing）。
    let state = AppState::new(config);
    // 让指标反映 key pool 的初始状态。
    state.key_pool.refresh_metrics().await;

    // 启动后台 task。
    let cooldown_handle = {
        let key_pool = state.key_pool.clone();
        let stop = state.stop_signal.clone();
        tokio::spawn(async move { key_pool.cool_down_task(stop).await })
    };
    // 一次性的启动健康检查。
    {
        let checker = state.health_checker.clone();
        tokio::spawn(async move { checker.run_startup().await });
    }
    // 周期性坏 key 恢复。
    let bad_key_handle = {
        let checker = state.health_checker.clone();
        let stop = state.stop_signal.clone();
        tokio::spawn(async move { checker.run_bad_key_checker(stop).await })
    };

    // 构造 warp 路由。
    let routes = build_routes(&state);
    let addr: SocketAddr = state
        .config
        .listen_addr
        .parse()
        .expect("Invalid listen address");

    // 当绑定到 IPv4 通配地址（0.0.0.0）时，也绑定 IPv6 通配地址，
    // 这样把 `localhost` 解析到 `::1` 的 client 也能连上（macOS 把
    // ::1 排在最前）。否则 `http://localhost:<端口>` 会连不上，哪怕
    // `http://127.0.0.1:<端口>` 是好的。
    let mut addrs = vec![addr];
    if addr.ip().is_unspecified() && addr.is_ipv4() {
        if let Ok(v6) = format!("[::]:{}", addr.port()).parse::<SocketAddr>() {
            addrs.push(v6);
        }
    }

    let mut server_handles = Vec::new();
    for a in addrs {
        info!(
            "监听于 {}，指标路径 {}",
            a, state.config.observability.metrics_path
        );
        let (_, fut) = warp::serve(routes.clone()).bind_with_graceful_shutdown(
            a,
            async {
                tokio::signal::ctrl_c().await.ok();
                info!("收到关停信号，排空在途请求……");
            },
        );
        server_handles.push(tokio::spawn(fut));
    }

    // 等所有监听者关停（每个都在 Ctrl+C 时停）。
    for h in server_handles {
        let _ = h.await;
    }

    // 通知后台 task 停下并 drop 它们的 handle。
    state.stop_signal.notify_waiters();
    drop(cooldown_handle);
    drop(bad_key_handle);
    info!("关停完成");
}
