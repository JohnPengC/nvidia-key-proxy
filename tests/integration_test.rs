use nvidia_key_proxy::*;
use warp::test::request;

// 确保集成测试绝不写入用于生产诊断的 on-disk failure.log。
// `std::env::set_var` 是进程全局的、这里安全，因为测试跑在单进程里；
// Once guard 避免重复写入。
static DISABLE_FAILURE_LOG: std::sync::Once = std::sync::Once::new();

fn test_config() -> Config {
    DISABLE_FAILURE_LOG.call_once(|| {
        // SAFETY：测试跑在单进程里；该环境变量是进程全局的，且只守护
        // failure-log 写入。
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
                scan_interval_seconds: 5,
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
        empty_shell_no_retry_prompt_tokens: 90000,
        request_max_prompt_tokens: default_request_max_prompt_tokens(),
        est_image_token_cost: 1500,
        est_audio_token_per_kb: 16,
        est_audio_token_cap: 10000,
        est_unknown_part_token_cap: 2000,
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
async fn health_endpoint_returns_204_when_ready() {
    let state = AppState::new(test_config());
    // S5：模拟启动健康检查已完成（`run_startup` 在退出前置此 flag）。
    state
        .health_ready
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let filter = build_routes(&state);
    let resp = request().method("GET").path("/health").reply(&filter).await;
    assert_eq!(resp.status(), 204);
}

#[tokio::test]
async fn health_endpoint_returns_503_before_startup_check_completes() {
    // S5：AppState 构造后 `health_ready` 默认 false —— `/health` 在 `run_startup`
    // 跑完前回 503，让负载均衡器在初始清点期间摘掉本实例。
    let state = AppState::new(test_config());
    assert!(
        !state
            .health_ready
            .load(std::sync::atomic::Ordering::Relaxed),
        "ready flag must start false"
    );
    let filter = build_routes(&state);
    let resp = request().method("GET").path("/health").reply(&filter).await;
    assert_eq!(resp.status(), 503);
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

/// S1 回归：`auth.enabled = true` 时 `/metrics` 必须过前门 bearer 校验——
/// 此前 metrics 路由排在 `.or()` 链的 proxy auth 块之前，绕过了前门。
/// 无 bearer 应被 401，带正确 bearer 应放行 200。
#[tokio::test]
async fn metrics_denied_by_front_door_auth_when_enabled() {
    let cfg = test_config_with_auth(true, "secret-bearer", "");
    let state = AppState::new(cfg);
    let filter = build_routes(&state);

    // 无 bearer → 401
    let resp = request()
        .method("GET")
        .path("/metrics")
        .reply(&filter)
        .await;
    assert_eq!(
        resp.status(),
        401,
        "auth.enabled 时 /metrics 无 bearer 必须被拒"
    );

    // 带正确 bearer → 200
    let resp = request()
        .method("GET")
        .path("/metrics")
        .header("authorization", "Bearer secret-bearer")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);

    // 带错误 bearer → 401
    let resp = request()
        .method("GET")
        .path("/metrics")
        .header("authorization", "Bearer wrong-token")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);
}

/// S1：`auth.enabled = false`（默认）时 `/metrics` 仍无需 bearer——不破坏
/// 开放部署的现状。
#[tokio::test]
async fn metrics_open_when_front_door_auth_disabled() {
    let cfg = test_config_with_auth(false, "secret-bearer", "");
    let state = AppState::new(cfg);
    let filter = build_routes(&state);
    let resp = request().method("GET").path("/metrics").reply(&filter).await;
    assert_eq!(resp.status(), 200);
}

/// S2 回归：开启前门鉴权却没配 bearer_token，`Config::validate` 必须
/// fail-fast，而不是放任"Bearer "（空）通过校验把代理开放。
#[tokio::test]
async fn validate_rejects_auth_enabled_with_empty_bearer_token() {
    let mut cfg = test_config_with_auth(true, "", "");
    // 给一个看起来合法的 key，避免被前置的 fake/placeholder key 校验先行拒下，
    // 从而让本测试确实是在断言 *auth* 校验分支。
    cfg.keys = vec!["nvapi-validkey-1234".to_string()];
    let err = cfg.validate().expect_err("空 bearer + auth.enabled 必须被校验拒绝");
    let msg = err.to_string();
    assert!(
        msg.contains("bearer_token"),
        "错误信息应指明 bearer_token 缺失，实际：{}",
        msg
    );
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
    // /v1/ 命名空间之外的路径必须被路由 guard 拒绝，绝不透传到上游。
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
    // 指向一个被拒的本地端口，让转发尝试快速失败，而不是卡在真实 DNS 查询
    // 上。`allow_local_base_url` 仅在本测试里绕过 localhost 校验。
    cfg.allow_local_base_url = true;
    cfg.base_url = "http://127.0.0.1:1".to_string();
    cfg.auth.enabled = enabled;
    cfg.auth.bearer_token = bearer.to_string();
    cfg.auth.admin_token = admin.to_string();
    cfg
}

#[tokio::test]
async fn admin_api_hidden_when_token_unset() {
    // 默认 test_config 没有 admin_token → 管理面必须 404。
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

    // 列出（初始有一个来自 test_config 的 key："dummy_key"）。
    let resp = request()
        .method("GET")
        .path("/admin/keys")
        .header("authorization", "Bearer admin-secret")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert!(!list.as_array().unwrap().is_empty());

    // 加一个新 key。
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

    // 删掉它。
    let resp = request()
        .method("DELETE")
        .path("/admin/keys/nvapi-newkey-12345678")
        .header("authorization", "Bearer admin-secret")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(v["removed"].as_bool(), Some(true));

    // 需要 admin 鉴权：缺 token → 401。
    let resp = request()
        .method("GET")
        .path("/admin/keys")
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);
}

/// 回归：管理 POST /admin/keys 必须以 413 拒绝超大的 body。
/// `warp::body::json()` 默认无大小限制，所以路由上若没有显式
/// `content_length_limit(1 MiB)`，一个粘了巨型 key 文件的已认证调用方就能
/// 把进程 OOM、或在 `add_keys_batch` 里制造病态锁抖动。有了上限，4 MiB 的
/// body 根本到不了 json()。
#[tokio::test]
async fn admin_keys_add_rejects_oversized_body() {
    let mut cfg = test_config();
    cfg.auth.admin_token = "admin-secret".to_string();
    let state = AppState::new(cfg);
    let filter = build_routes(&state);

    // 一个 4 MiB body：单个约 4 MiB 合法 JSON 字符的 JSON 串（每个 key 短到能
    // 过 add_keys_batch 的长度校验——但我们到不了那；content_length_limit 先
    // 拒）。
    let huge = "x".repeat(4 * 1024 * 1024);
    let body = format!(r#"["{}"]"#, huge);
    let resp = request()
        .method("POST")
        .path("/admin/keys")
        .header("authorization", "Bearer admin-secret")
        .header("content-type", "application/json")
        .body(body)
        .reply(&filter)
        .await;
    assert_eq!(
        resp.status(),
        413,
        "oversized admin add-keys body should be rejected with 413, got {} body={:?}",
        resp.status(),
        std::str::from_utf8(resp.body()).unwrap_or("")
    );
}


#[tokio::test]
async fn front_door_auth_enforced() {
    let cfg = test_config_with_auth(true, "letmein", "admin-secret");
    let state = AppState::new(cfg);
    let filter = build_routes(&state);

    // 无 token → 401。
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);

    // 错 token → 401。
    let resp = request()
        .method("POST")
        .path("/v1/chat/completions")
        .header("authorization", "Bearer wrong")
        .header("content-type", "application/json")
        .body(r#"{"model":"x","stream":false}"#)
        .reply(&filter)
        .await;
    assert_eq!(resp.status(), 401);

    // 正确 token → 过鉴权门（之后因无 mock 会从上游拿 404；重点是代理门
    // 放行了它）。
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
