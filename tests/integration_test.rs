use nvidia_key_proxy::*;
use warp::test::request;

// Ensure integration tests never write to the on-disk failure.log used for
// production diagnosis. `std::env::set_var` is process-global and safe here
// because tests run in one process; the Once guard avoids redundant writes.
static DISABLE_FAILURE_LOG: std::sync::Once = std::sync::Once::new();

fn test_config() -> Config {
    DISABLE_FAILURE_LOG.call_once(|| {
        // SAFETY: tests run in one process; the env var is process-global
        // and only guards failure-log writes.
        unsafe {
            std::env::set_var("FAILURE_LOG_DISABLE", "1");
        }
    });
    Config {
        listen_addr: "127.0.0.1:0".to_string(),
        base_url: "http://example.com".to_string(),
        keys: vec!["dummy_key".to_string()],
        rate_limiter: RateLimiterConfig {
            qps_per_key: 1.0,
            burst: 1,
            token_wait_ms: 0,
        },
        cooldown: CooldownConfig {
            base_seconds: 1,
            max_seconds: 10,
        },
        health_check: HealthCheckConfig {
            interval_seconds: 30,
            timeout_seconds: 5,
            max_consecutive_failures: 3,
        },
        observability: ObservabilityConfig {
            enable_metrics: true,
            metrics_path: "/metrics".to_string(),
            log_level: "info".to_string(),
        },
        request_timeout_seconds: 60,
        retry: RetryConfig::default(),
    }
}

fn test_config_with_base(base_url: &str) -> Config {
    let mut cfg = test_config();
    cfg.base_url = base_url.to_string();
    cfg
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
    let resp = request()
        .method("GET")
        .path(state.config.observability.metrics_path.as_str())
        .reply(&filter)
        .await;
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
    let resp = request()
        .method("GET")
        .path(state.config.observability.metrics_path.as_str())
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let state = AppState::new(test_config());
    let filter = build_routes(&state);
    let resp = request()
        .method("GET")
        .path("/nonexistent")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn non_v1_path_returns_404_not_forwarded() {
    // Paths outside the /v1/ namespace must be rejected by the route guard
    // and never forwarded upstream.
    let state = AppState::new(test_config());
    let filter = build_routes(&state);
    let resp = request()
        .method("GET")
        .path("/api/foo")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn v1_path_is_forwarded_to_upstream() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"ok":true}"#))
        .mount(&upstream)
        .await;

    let state = AppState::new(test_config_with_base(&upstream.uri()));
    let filter = build_routes(&state);
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200, "v1 path must reach upstream, not 404");
}
