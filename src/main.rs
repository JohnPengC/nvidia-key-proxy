use std::net::SocketAddr;

use tracing::{info, warn};

use nvidia_key_proxy::{build_routes, init_tracing, AppState, Config};

/// 优雅关停的硬上限。`bind_with_graceful_shutdown` 会在收到信号后排空在途请求，
/// 但**不设上限**——一个不配合的流式调用方（一直不读 body、TCP 半开）能让单个
/// 监听 future 永不退出，进程像僵在那。我们给整个关停流程一个硬墙：超时后强杀，
/// 让 orchestrator（k8s/k8s preStop / systemd）的 `terminationGracePeriodSeconds`
/// 真正生效——超时不如直接让上游重启我们，而不是被一个坏 client 卡死。
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// 等待关停信号：Ctrl+C（SIGINT）或 SIGTERM。容器/k8s/systemd 停 pod 默认发
/// SIGTERM，只听 Ctrl+C 会把 SIGTERM 当未捕获信号走默认动作（直接退出），
/// 跳过整段在途排空——in-flight 请求被切、`failure.log` 尾记录丢、worker
/// guard 不 drop。二者听其一即触发。
async fn await_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        } else {
            // 退化为永远不触发，合并 future 等价于只听 Ctrl+C。
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }
}

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

    // 当绑定到 IPv4 通配地址（0.0.0.0）时，同时绑定 IPv6 通配地址，使把
    // `localhost` 解析到 `::1` 的 client 也能连上（macOS 把 ::1 排在最前）。
    // 但在 Linux（默认 `net.ipv6.bindv6only=0`）下，`[::]:port` 已经覆盖
    // IPv4-mapped 空间，再绑 `0.0.0.0:port` 就会 `EADDRINUSE` 让
    // `bind_with_graceful_shutdown` panic——生产 Linux 部署刚启动就崩。此处用
    // try_bind 跳过不可用的额外地址（仅在双栈冲突时丢弃 IPv6 bind；macOS 会
    // 得到两个监听 socket，不影响）。
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
        let server = warp::serve(routes.clone());
        let (_, fut) = match server.try_bind_with_graceful_shutdown(a, await_shutdown_signal()) {
            Ok(x) => x,
            Err(e) => {
                // 多半是 Linux 上 [::] 与 0.0.0.0 的 EADDRINUSE 冲突；
                // 跳过该地址而非 panic 启动。
                warn!("监听 {} 失败，跳过：{}", a, e);
                continue;
            }
        };
        server_handles.push(tokio::spawn(fut));
    }

    // 一个独立 daemon：等关停信号一到，**立刻** `notify_waiters` 通知后台 task
    // （坏 key 扫描、冷却计时）停。这样它们的收尾与 warp 在途请求排空**并行**，
    // 而不是先等 drain 全部完成、再串行通知它们走关停分支，缩短整体关闭时延。
    // `notify_waiters` 不累加，被等者只会被叫一次——daemon 触发一次即够。
    let bg_stop = state.stop_signal.clone();
    let bg_stop_handle = tokio::spawn(async move {
        await_shutdown_signal().await;
        bg_stop.notify_waiters();
        info!("已通知后台 task 关停；等待在途请求排空……");
    });

    // 先等关停信号，再给在途请求一个硬上限排空时间。
    await_shutdown_signal().await;
    info!("收到关停信号，开始排空在途请求……");

    // 等所有监听者关停，或超时硬墙到——后者强退，避免坏 client 把进程卡死。
    let drain = async {
        for h in server_handles {
            let _ = h.await;
        }
    };
    tokio::select! {
        _ = drain => {
            info!("在途请求排空完成，关停完成");
        }
        _ = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT) => {
            warn!(
                "关停排空超时（{}s），仍存在不配合的在途请求/坏流式调用方；强退",
                SHUTDOWN_DRAIN_TIMEOUT.as_secs()
            );
        }
    }

    // drop background-task handle（已由 stop_signal 通知停）。
    drop(cooldown_handle);
    drop(bad_key_handle);
    drop(bg_stop_handle);
}
