use nvidia_app::*; // assuming crate name is nvidia_app
use warp::test::request;

fn test_config() -> Config {
    Config {
        listen_addr: "127.0.0.1:0".to_string(),
        base_url: "http://example.com".to_string(),
        keys: vec!["dummy_key".to_string()],
        rate_limiter: RateLimiterConfig { qps_per_key: 1.0, burst: 1 },
        cooldown: CooldownConfig { base_seconds: 1, max_seconds: 10 },
        health_check: HealthCheckConfig { interval_seconds: 30, timeout_seconds: 5, max_consecutive_failures: 3 },
        observability: ObservabilityConfig { enable_metrics: true, metrics_path: "/metrics".to_string(), log_level: "info".to_string() },
        codex: CodexConfig::default(),
        cursor: None,
        request_timeout_seconds: 60,
        retry: RetryConfig::default(),
    }
}

#[tokio::test]
async fn health_endpoint_returns_204() {
    let state = AppState::new(test_config());
    let filter = build_routes(&state);
    let resp = request().method("GET").path("/health").reply(&filter).await;
    assert_eq!(resp.status(), 204);
}

#[tokio::test]
async fn metrics_enabled_returns_200() {
    let mut cfg = test_config();
    cfg.observability.enable_metrics = true;
    let state = AppState::new(cfg);
    state.metrics.inc_request();
    let filter = build_routes(&state);
    let resp = request().method("GET").path(state.config.observability.metrics_path.as_str()).reply(&filter).await;
    assert_eq!(resp.status(), 200);
    let body = std::str::from_utf8(resp.body()).unwrap();
    assert!(body.contains("proxy_requests_total"));
}

#[tokio::test]
async fn metrics_disabled_returns_404() {
    let mut cfg = test_config();
    cfg.observability.enable_metrics = false;
    let state = AppState::new(cfg);
    let filter = build_routes(&state);
    let resp = request().method("GET").path(state.config.observability.metrics_path.as_str()).reply(&filter).await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let state = AppState::new(test_config());
    let filter = build_routes(&state);
    let resp = request().method("GET").path("/nonexistent").reply(&filter).await;
    assert_eq!(resp.status(), 404);
}