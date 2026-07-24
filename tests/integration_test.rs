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
        model_aliases: std::collections::HashMap::new(),
        allow_local_base_url: false,
        auth: AuthConfig::default(),
        inject_stream_usage: true,
        synthesize_empty_choices: true,
        retry_on_empty_shell: true,
        empty_shell_retry_max_tokens: 8192,
        empty_shell_retry_attempts: 2,
        empty_shell_probe_before_retry: true,
        empty_shell_retry_backoff_base_ms: 500,
        empty_shell_retry_backoff_max_ms: 5000,
        empty_shell_pre_clamp_max_tokens: default_empty_shell_pre_clamp_max_tokens(),
        circuit_breaker_threshold: 3,
        circuit_breaker_per_model: true,
        retry_on_503: true,
        probe_before_503_retry: true,
        retry_on_500: true,
        max_tokens_max: 100_000,
        concurrency_limit_per_model: std::collections::HashMap::new(),
        concurrency_queue_timeout_ms: 5000,
        request_timeout_streaming_seconds: 0,
        upstream_connect_timeout_secs: 5,
        upstream_pool_idle_timeout_secs: 90,
        retry_jitter_ms: 100,
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

fn test_config_with_auth(enabled: bool, bearer: &str, admin: &str) -> Config {
    let mut cfg = test_config();
    // Point at a refused local port so the forwarded attempt fails fast
    // instead of hanging on a real DNS lookup. `allow_local_base_url`
    // bypasses the localhost validation for this test only.
    cfg.allow_local_base_url = true;
    cfg.base_url = "http://127.0.0.1:1".to_string();
    cfg.auth.enabled = enabled;
    cfg.auth.bearer_token = bearer.to_string();
    cfg.auth.admin_token = admin.to_string();
    cfg
}

#[tokio::test]
async fn admin_api_hidden_when_token_unset() {
    // Default test_config has no admin_token → admin surface must 404.
    let state = AppState::new(test_config());
    let filter = build_routes(&state);
    let resp = request()
        .method("GET")
        .path("/admin/keys")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn admin_keys_list_and_add_and_remove() {
    let mut cfg = test_config();
    cfg.auth.admin_token = "admin-secret".to_string();
    let state = AppState::new(cfg);
    let filter = build_routes(&state);

    // List (initially one key from test_config: "dummy_key").
    let resp = request()
        .method("GET")
        .path("/admin/keys")
        .header("authorization", "Bearer admin-secret")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert!(list.as_array().unwrap().len() >= 1);

    // Add a new key.
    let resp = request()
        .method("POST")
        .path("/admin/keys")
        .header("authorization", "Bearer admin-secret")
        .json(&vec!["nvapi-newkey-12345678"])
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["added"].as_array().unwrap().len(), 1);

    // Remove it.
    let resp = request()
        .method("DELETE")
        .path("/admin/keys/nvapi-newkey-12345678")
        .header("authorization", "Bearer admin-secret")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(v["removed"].as_bool(), Some(true));

    // Admin auth required: missing token → 401.
    let resp = request()
        .method("GET")
        .path("/admin/keys")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);
}


#[tokio::test]
async fn front_door_auth_enforced() {
    let cfg = test_config_with_auth(true, "letmein", "admin-secret");
    let state = AppState::new(cfg);
    let filter = build_routes(&state);

    // No token → 401.
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);

    // Wrong token → 401.
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("authorization", "Bearer wrong")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);

    // Correct token → passes through the auth gate (then 404 from upstream
    // since there is no mock; the point is the proxy gate accepted it).
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("authorization", "Bearer letmein")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_ne!(resp.status(), 401);
}
