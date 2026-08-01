use dotenvy::dotenv;
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use tracing::{info, warn};

use crate::key_pool::mask_key;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub base_url: String,
    pub keys: Vec<String>,
    pub rate_limiter: RateLimiterConfig,
    pub cooldown: CooldownConfig,
    pub health_check: HealthCheckConfig,
    pub observability: ObservabilityConfig,
    /// Optional request timeout in seconds. 0 disables the timeout entirely
    /// (recommended when proxying long-running streaming requests).
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    /// Retry/upstream-error policy (exponential backoff, key-disable threshold).
    #[serde(default)]
    pub retry: RetryConfig,
    /// Client-facing model name → NVIDIA-ready model id. Empty by default.
    /// When a chat-completion request carries `model` matching a key here,
    /// the proxy rewrites the field to the mapped value before forwarding.
    /// Unmatched names are forwarded unchanged.
    #[serde(default)]
    pub model_aliases: std::collections::HashMap<String, String>,
    /// Allow `base_url` to point at a loopback / localhost address. Off by
    /// default because production should never target example.com/localhost,
    /// but turning it on is handy for pointing the proxy at a local
    /// NVIDIA-compatible mock during development / integration testing.
    #[serde(default)]
    pub allow_local_base_url: bool,
    /// Front-door authentication for the proxy itself. The pool's NVIDIA
    /// quota is exposed to anyone who can reach `listen_addr`; enabling this
    /// requires clients to present `Authorization: Bearer <bearer_token>` on
    /// `/v1/*` requests. Disabled by default to preserve the "any api_key
    /// works" behaviour existing clients rely on. `/health` is always exempt.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Inject `stream_options.include_usage: true` into streaming chat
    /// requests so upstream returns token usage at stream end. NVIDIA/OpenAI
    /// only emit usage when this flag is set; without it the proxy's token
    /// metrics are zero for the (common) streaming case. Defaults to true;
    /// disable via `inject_stream_usage = false` or `NVIDIA_INJECT_STREAM_USAGE=false`.
    #[serde(default = "default_inject_stream_usage")]
    pub inject_stream_usage: bool,
    /// Synthesize a placeholder `choices` entry when upstream returns a 2xx
    /// non-SSE body whose `choices` array is empty (`choices: []`). cc-switch's
    /// non-streaming converter `openai_to_anthropic` hard-errors on an empty
    /// `choices` ("格式转换错误: Empty choices array" → 422 to Claude Code);
    /// this commonly happens on context compaction (`stream:false`) where the
    /// model produced no tokens but still returned `usage`. Instead of relying
    /// on cc-switch being patched, the proxy rewrites that body in-place to a
    /// legal single-choice response (empty assistant message, `finish_reason:
    /// stop`, preserving `id`/`model`/`usage`) so cc-switch succeeds.
    ///
    /// Only the non-streaming, non-SSE success branch is affected — and there
    /// only a bounded ≤64 KiB prefix is buffered to decide, so large streaming
    /// completions keep their zero-copy passthrough. Defaults to true; disable
    /// via `synthesize_empty_choices = false` or
    /// `NVIDIA_SYNTHESIZE_EMPTY_CHOICES=false`.
    #[serde(default = "default_synthesize_empty_choices")]
    pub synthesize_empty_choices: bool,
    /// Retry a query once with a reduced `max_tokens` when NVIDIA returns a
    /// "shell" 2xx — a body shaped `{"choices":[], "id":"", "usage":null, ...}`
    /// where every field is null/empty. This is NVIDIA's silent soft-failure for
    /// GLM-class models on build.integrate (no real inference ran — hence
    /// `usage:null`, vs. the legitimate compaction empty-choices body which
    /// DOES carry `usage`). A 198-message `/compact` with `max_tokens=64000`
    /// commonly trips this: NVIDIA drops the whole request into an empty shell
    /// rather than truncating `max_tokens`. The retry re-sends the same body
    /// with `max_tokens` clamped to `empty_shell_retry_max_tokens` (8192 by
    /// default), which usually lets the inference actually run. Defaults to
    /// true; disable via `retry_on_empty_shell = false` or
    /// `NVIDIA_RETRY_ON_EMPTY_SHELL=false`.
    #[serde(default = "default_retry_on_empty_shell")]
    pub retry_on_empty_shell: bool,
    /// `max_tokens` value used when retried after an empty-shell soft failure.
    /// Only affects the single retry attempt; the original request is forwarded
    /// unchanged first. 8192 is conservative across GLM-family models on
    /// build.integrate. Override via the `empty_shell_retry_max_tokens` config
    /// key or `NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS` env var.
    #[serde(default = "default_empty_shell_retry_max_tokens")]
    pub empty_shell_retry_max_tokens: u64,
    /// Before retrying an empty-shell failure, do a lightweight health probe
    /// against /v1/models to avoid retrying into a degraded upstream.
    /// Default true. Override via `NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY`.
    #[serde(default = "default_empty_shell_probe_before_retry")]
    pub empty_shell_probe_before_retry: bool,
    /// Circuit breaker threshold. After N consecutive empty-shell or 504
    /// responses on a given key/model, the key is cooled for
    /// cooldown.base_seconds. 0 = disabled. Default 3. Override via
    /// `NVIDIA_CIRCUIT_BREAKER_THRESHOLD`.
    #[serde(default = "default_circuit_breaker_threshold")]
    pub circuit_breaker_threshold: u32,

    /// Whether circuit breaker tracks failures per (key, model) pair
    /// instead of globally per key. Default true. Override via
    /// `NVIDIA_CIRCUIT_BREAKER_PER_MODEL`.
    #[serde(default = "default_circuit_breaker_per_model")]
    pub circuit_breaker_per_model: bool,

    /// Retry on 503 Service Unavailable (NVIDIA Worker local request limit reached).
    /// When enabled, 503 responses trigger a retry against another key with
    /// exponential backoff (uses same retry budget as 401 retries).
    /// Default true. Override via `NVIDIA_RETRY_ON_503`.
    #[serde(default = "default_retry_on_503")]
    pub retry_on_503: bool,

    /// Before retrying a 503 failure, do a lightweight health probe
    /// against /v1/models to avoid retrying into a degraded upstream.
    /// Default true. Override via `NVIDIA_PROBE_BEFORE_503_RETRY`.
    #[serde(default = "default_probe_before_503_retry")]
    pub probe_before_503_retry: bool,

    /// Retry on 500 Internal Server Error. NVIDIA/GLM-class models
    /// intermittently return `{"message":"Failed to generate
    /// completions","type":"Internal Server Error","code":500}` — a
    /// transient per-request worker failure (same prompt succeeds on the
    /// next key seconds later). When enabled, 500 responses trigger a
    /// retry against another key using the same retry budget as 401/503.
    /// Default true. Override via `NVIDIA_RETRY_ON_500`.
    #[serde(default = "default_retry_on_500")]
    pub retry_on_500: bool,

    /// Number of retry attempts for empty-shell soft failures.
    /// 2 = original + two retries. 0 = no retry (falls back to 502).
    /// Default 2. Override via `NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS`.
    #[serde(default = "default_empty_shell_retry_attempts")]
    pub empty_shell_retry_attempts: u32,

    /// Base delay for exponential backoff between empty-shell retries (ms).
    /// Default 500. Override via `NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_BASE_MS`.
    #[serde(default = "default_empty_shell_retry_backoff_base_ms")]
    pub empty_shell_retry_backoff_base_ms: u64,

    /// Cap for a single empty-shell retry backoff sleep (ms).
    /// Default 5000. Override via `NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_MAX_MS`.
    #[serde(default = "default_empty_shell_retry_backoff_max_ms")]
    pub empty_shell_retry_backoff_max_ms: u64,

    /// Per-model max_tokens ceiling for pre-clamping on first attempt.
    /// When a request targets a model in this map and max_tokens is at or above
    /// the ceiling, the proxy clamps it before the first upstream call.
    /// Reduces empty-shell occurrences for known problematic models.
    /// Default includes z-ai/glm-5.2 → 8192.
    /// Override via `NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS` (JSON map).
    #[serde(default = "default_empty_shell_pre_clamp_max_tokens")]
    pub empty_shell_pre_clamp_max_tokens: std::collections::HashMap<String, u64>,

    /// Prompt-token threshold above which an empty-shell soft failure is NOT
    /// retried with a clamped max_tokens. failure.log showed clamp-retry on
    /// very long contexts (est_prompt_tokens 107k+) recovered 0% (49/49
    /// re-shelled): the shell there isn't caused by an oversized output budget
    /// but by `prompt + any max_tokens` exceeding the model's context window, so
    /// shrinking max_tokens can't help. Above this threshold the proxy skips
    /// the retry round-trip and surfaces 502 immediately with reason
    /// `empty_shell_no_retry_prompt_too_large`, avoiding wasted upstream calls.
    /// 0 disables this bypass (always retry per the existing policy).
    /// Default 90000 (well below the 107k failure-log floor for shells).
    /// Override via `NVIDIA_EMPTY_SHELL_NO_RETRY_PROMPT_TOKENS`.
    #[serde(default = "default_empty_shell_no_retry_prompt_tokens")]
    pub empty_shell_no_retry_prompt_tokens: u64,

    /// Optional ceiling for max_tokens validation. Any request with max_tokens
    /// > this value will have the field dropped (model default used). Default
    /// 100000. Override via `MAX_TOKENS_MAX` env var.
    #[serde(default = "default_max_tokens_max")]
    pub max_tokens_max: u64,

    // ==================== Concurrency limiting (Priority 2) ====================
    /// Per-model maximum concurrent upstream requests.
    /// When a model's concurrency reaches this limit, new requests are queued
    /// (up to `concurrency_queue_timeout_ms`) or rejected with 503.
    /// Default: empty (no limit). Override via `NVIDIA_CONCURRENCY_LIMIT_PER_MODEL` (JSON map).
    #[serde(default)]
    pub concurrency_limit_per_model: std::collections::HashMap<String, u32>,

    /// Maximum time (ms) to wait for a concurrency semaphore slot.
    /// If the queue is full for this long, return 503 with Retry-After.
    /// Default 5000 (5s). Override via `NVIDIA_CONCURRENCY_QUEUE_TIMEOUT_MS`.
    #[serde(default = "default_concurrency_queue_timeout_ms")]
    pub concurrency_queue_timeout_ms: u64,

    // ==================== Timeout tuning (Priority 3) ====================
    /// Request timeout for streaming connections (Priority 3).
    /// When set > 0, used instead of request_timeout_seconds for streaming calls.
    /// 0 = use request_timeout_seconds. Default 0 (disabled).
    /// Override via NVIDIA_REQUEST_TIMEOUT_STREAMING_SECONDS.
    #[serde(default)]
    pub request_timeout_streaming_seconds: u64,

    // ==================== Network resilience (Priority 4) ====================
    /// Upstream connection timeout (seconds). Default 5.
    /// Override via NVIDIA_UPSTREAM_CONNECT_TIMEOUT_SECONDS.
    #[serde(default = "default_upstream_connect_timeout_secs")]
    pub upstream_connect_timeout_secs: u64,

    /// Upstream connection pool idle timeout (seconds). Default 90.
    /// Override via NVIDIA_UPSTREAM_POOL_IDLE_TIMEOUT_SECONDS.
    #[serde(default = "default_upstream_pool_idle_timeout_secs")]
    pub upstream_pool_idle_timeout_secs: u64,

    /// Retry jitter (ms). Added to exponential backoff. Default 100.
    /// Override via NVIDIA_RETRY_JITTER_MS.
    #[serde(default = "default_retry_jitter_ms")]
    pub retry_jitter_ms: u64,
}

/// Public wrapper to get the default pre-clamp map (used in tests).
pub fn default_empty_shell_pre_clamp_max_tokens() -> std::collections::HashMap<String, u64> {
    let mut m = std::collections::HashMap::new();
    // 8192, not 32000: GLM-class models (~128k context) return an HTTP-200
    // empty shell when `prompt + max_tokens` exceeds their remaining budget.
    // failure.log showed all 216 first-attempt shells carried max_tokens=32000
    // (the empty output budget once a ~107k+ prompt was counted in). Clamping
    // to 8192 leaves room for output on prompts up to ~120k.
    m.insert("z-ai/glm-5.2".to_string(), 8192);
    m
}

fn default_request_timeout_seconds() -> u64 {
    60
}

fn default_request_timeout_streaming_seconds() -> u64 {
    0
}

fn default_inject_stream_usage() -> bool {
    true
}

fn default_synthesize_empty_choices() -> bool {
    true
}

fn default_retry_on_empty_shell() -> bool {
    true
}

fn default_empty_shell_retry_max_tokens() -> u64 {
    8192
}

fn default_empty_shell_retry_attempts() -> u32 {
    2
}

fn default_empty_shell_probe_before_retry() -> bool {
    true
}

fn default_empty_shell_retry_backoff_base_ms() -> u64 {
    500
}

fn default_empty_shell_retry_backoff_max_ms() -> u64 {
    5000
}

fn default_empty_shell_no_retry_prompt_tokens() -> u64 {
    90000
}

fn default_circuit_breaker_threshold() -> u32 {
    3
}

fn default_circuit_breaker_per_model() -> bool {
    true
}

fn default_retry_on_503() -> bool {
    true
}

fn default_probe_before_503_retry() -> bool {
    true
}

fn default_retry_on_500() -> bool {
    true
}

fn default_concurrency_queue_timeout_ms() -> u64 {
    5000
}

fn default_upstream_connect_timeout_secs() -> u64 {
    5
}

fn default_upstream_pool_idle_timeout_secs() -> u64 {
    90
}

fn default_retry_jitter_ms() -> u64 {
    100
}

fn default_max_tokens_max() -> u64 {
    100_000
}

/// Front-door auth config. See `Config::auth` for the rationale.
#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// Require clients to authenticate before the proxy forwards their
    /// request. When false, requests are forwarded without checking a token.
    #[serde(default)]
    pub enabled: bool,
    /// Bearer token clients must present. Also overridable via
    /// `NVIDIA_AUTH_BEARER_TOKEN`. Empty when not set.
    #[serde(default)]
    pub bearer_token: String,
    /// Bearer token required by the `/admin/*` management API (key add/remove
    /// / status). When empty (and `NVIDIA_AUTH_ADMIN_TOKEN` unset), the admin
    /// API is disabled entirely. Also overridable via `NVIDIA_AUTH_ADMIN_TOKEN`.
    #[serde(default)]
    pub admin_token: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bearer_token: String::new(),
            admin_token: String::new(),
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let masked_keys: Vec<String> = self.keys.iter().map(|k| mask_key(k)).collect();
        f.debug_struct("Config")
            .field("listen_addr", &self.listen_addr)
            .field("base_url", &self.base_url)
            .field(
                "keys",
                &format!("<{} keys: {:?}>", self.keys.len(), masked_keys),
            )
            .field("rate_limiter", &self.rate_limiter)
            .field("cooldown", &self.cooldown)
            .field("health_check", &self.health_check)
            .field("observability", &self.observability)
            .field("request_timeout_seconds", &self.request_timeout_seconds)
            .field("request_timeout_streaming_seconds", &self.request_timeout_streaming_seconds)
            .field("retry", &self.retry)
            .field("model_aliases", &self.model_aliases)
            .field("allow_local_base_url", &self.allow_local_base_url)
            .field("inject_stream_usage", &self.inject_stream_usage)
            .field("synthesize_empty_choices", &self.synthesize_empty_choices)
            .field("retry_on_empty_shell", &self.retry_on_empty_shell)
            .field(
                "empty_shell_retry_max_tokens",
                &self.empty_shell_retry_max_tokens,
            )
            .field(
                "empty_shell_retry_attempts",
                &self.empty_shell_retry_attempts,
            )
            .field(
                "empty_shell_probe_before_retry",
                &self.empty_shell_probe_before_retry,
            )
            .field(
                "empty_shell_retry_backoff_base_ms",
                &self.empty_shell_retry_backoff_base_ms,
            )
            .field(
                "empty_shell_retry_backoff_max_ms",
                &self.empty_shell_retry_backoff_max_ms,
            )
            .field(
                "empty_shell_pre_clamp_max_tokens",
                &self.empty_shell_pre_clamp_max_tokens,
            )
            .field(
                "empty_shell_no_retry_prompt_tokens",
                &self.empty_shell_no_retry_prompt_tokens,
            )
            .field(
                "circuit_breaker_threshold",
                &self.circuit_breaker_threshold,
            )
            .field(
                "circuit_breaker_per_model",
                &self.circuit_breaker_per_model,
            )
            .field("max_tokens_max", &self.max_tokens_max)
            .field("concurrency_limit_per_model", &self.concurrency_limit_per_model)
            .field("concurrency_queue_timeout_ms", &self.concurrency_queue_timeout_ms)
            .field("request_timeout_streaming_seconds", &self.request_timeout_streaming_seconds)
            .field("upstream_connect_timeout_secs", &self.upstream_connect_timeout_secs)
            .field("upstream_pool_idle_timeout_secs", &self.upstream_pool_idle_timeout_secs)
            .field("retry_jitter_ms", &self.retry_jitter_ms)
            .field(
                "auth",
                &format!(
                    "AuthConfig {{ enabled: {}, bearer_token: {}, admin_token: {} }}",
                    self.auth.enabled,
                    if self.auth.bearer_token.is_empty() {
                        "<none>"
                    } else {
                        "<set>"
                    },
                    if self.auth.admin_token.is_empty() {
                        "<none>"
                    } else {
                        "<set>"
                    }
                ),
            )
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimiterConfig {
    pub qps_per_key: f64,
    pub burst: u64,
    /// Maximum milliseconds to wait on a single key's token bucket before
    /// switching to another key (default 200). 0 disables switching — the
    /// existing unbounded-wait behaviour is preserved.
    #[serde(default = "default_token_wait_ms")]
    pub token_wait_ms: u64,
}

fn default_token_wait_ms() -> u64 {
    200
}

#[derive(Debug, Deserialize, Clone)]
pub struct CooldownConfig {
    pub base_seconds: u64,
    pub max_seconds: u64,
}

/// Retry/upstream-error policy for proxy → NVIDIA.
///
/// Only a 401 *without* `Retry-After` triggers a retry against another healthy
/// key with exponential backoff. Each such attempt that fails increments the
/// offending key's `fail_count`; once `auth_fail_threshold` is reached, the key
/// is moved to `KeyState::Disabled` and is no longer acquired. A background
/// health probe must see a success before the key is reactivated.
///
/// 401 with `Retry-After`, and 429/403 regardless of `Retry-After`, are NOT
/// retried: they are deterministic upstream decisions (per-key throttle /
/// region / account gating) and retrying on another key would only burn the
/// whole pool. A `Retry-After` value cools the offending key; otherwise the
/// response is surfaced to the client unchanged.
#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    /// Maximum retry attempts per client request (excluding the initial
    /// attempt). Default 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Base delay for exponential backoff in milliseconds. Default 200.
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    /// Cap for a single backoff sleep. Default 2000.
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    /// Per-key consecutive upstream auth/forbidden failures after which the
    /// key is Disabled (until a health probe recovers it). Default 3.
    #[serde(default = "default_auth_fail_threshold")]
    pub auth_fail_threshold: u32,
    /// Max times a single client request may switch to another key because
    /// the current key's token bucket was starved. Decoupled from
    /// `max_retries` (the upstream 401 retry budget) so key-switching and
    /// retry-on-auth-failure are tuned independently. Defaults to `max_retries`.
    #[serde(default = "default_max_key_switches")]
    pub max_key_switches: u32,
}

fn default_max_retries() -> u32 {
    3
}
fn default_backoff_base_ms() -> u64 {
    200
}
fn default_backoff_max_ms() -> u64 {
    2000
}
fn default_auth_fail_threshold() -> u32 {
    3
}
fn default_max_key_switches() -> u32 {
    default_max_retries()
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            auth_fail_threshold: default_auth_fail_threshold(),
            max_key_switches: default_max_key_switches(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthCheckConfig {
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub max_consecutive_failures: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    pub enable_metrics: bool,
    pub metrics_path: String,
    pub log_level: String,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        // Load .env file if present
        let _ = dotenv();

        // Default configuration. base_url aligns with README / example value.
        let mut config = Config {
            listen_addr: "0.0.0.0:8080".to_string(),
            base_url: "https://integrate.api.nvidia.com".to_string(),
            keys: Vec::new(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: 0.65,
                burst: 8,
                token_wait_ms: 0,
            },
            cooldown: CooldownConfig {
                base_seconds: 60,
                max_seconds: 600,
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
            request_timeout_seconds: default_request_timeout_seconds(),
            retry: RetryConfig::default(),
            model_aliases: std::collections::HashMap::new(),
            allow_local_base_url: false,
            auth: AuthConfig::default(),
            inject_stream_usage: default_inject_stream_usage(),
            synthesize_empty_choices: default_synthesize_empty_choices(),
            retry_on_empty_shell: default_retry_on_empty_shell(),
            empty_shell_retry_max_tokens: default_empty_shell_retry_max_tokens(),
            empty_shell_retry_attempts: default_empty_shell_retry_attempts(),
            empty_shell_probe_before_retry: default_empty_shell_probe_before_retry(),
            empty_shell_retry_backoff_base_ms: default_empty_shell_retry_backoff_base_ms(),
            empty_shell_retry_backoff_max_ms: default_empty_shell_retry_backoff_max_ms(),
            empty_shell_pre_clamp_max_tokens: default_empty_shell_pre_clamp_max_tokens(),
            empty_shell_no_retry_prompt_tokens: default_empty_shell_no_retry_prompt_tokens(),
            circuit_breaker_threshold: default_circuit_breaker_threshold(),
            circuit_breaker_per_model: default_circuit_breaker_per_model(),
            retry_on_503: default_retry_on_503(),
            probe_before_503_retry: default_probe_before_503_retry(),
            retry_on_500: default_retry_on_500(),
            max_tokens_max: default_max_tokens_max(),
            concurrency_limit_per_model: std::collections::HashMap::new(),
            concurrency_queue_timeout_ms: default_concurrency_queue_timeout_ms(),
            request_timeout_streaming_seconds: default_request_timeout_streaming_seconds(),
            upstream_connect_timeout_secs: default_upstream_connect_timeout_secs(),
            upstream_pool_idle_timeout_secs: default_upstream_pool_idle_timeout_secs(),
            retry_jitter_ms: default_retry_jitter_ms(),
        };

        // Prefer NVIDIA_CONFIG_PATH, fall back to ./config.toml
        let config_path =
            std::env::var("NVIDIA_CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

        if let Ok(content) = fs::read_to_string(&config_path) {
            let toml_config: Config = toml::from_str(&content)?;
            config.listen_addr = toml_config.listen_addr;
            config.base_url = toml_config.base_url;
            if !toml_config.keys.is_empty() {
                config.keys = toml_config.keys;
            }
            config.rate_limiter = toml_config.rate_limiter;
            config.cooldown = toml_config.cooldown;
            config.health_check = toml_config.health_check;
            config.observability = toml_config.observability;
            config.request_timeout_seconds = toml_config.request_timeout_seconds;
            config.retry = toml_config.retry;
            config.allow_local_base_url = toml_config.allow_local_base_url;
            config.auth = toml_config.auth;
            // Merge model_aliases: TOML-defined aliases take precedence, but
            // we *add* (don't replace) the default set so callers can extend
            // rather than override in the common case.
            for (k, v) in toml_config.model_aliases.into_iter() {
                config.model_aliases.insert(k, v);
            }
        }

        // Override from environment variables (NVIDIA_*)
        Self::apply_env_overrides(&mut config);

        config.validate()?;

        if config.keys.is_empty() {
            return Err(
                "No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into(),
            );
        }

        Ok(config)
    }

    /// Validate configuration values after all overrides have been applied.
    /// Catches placeholder/fake values that would otherwise manifest as 401/404
    /// failures at runtime.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        // base_url must be a real endpoint, not the default example domain or
        // a loopback address. Using example.com/localhost in production means
        // every upstream request will 404/401 and burn retry budget.
        if self.base_url.is_empty() {
            return Err("base_url must not be empty".into());
        }
        let base_lower = self.base_url.to_ascii_lowercase();
        if !self.allow_local_base_url
            && (base_lower.contains("example.com")
                || base_lower.contains("localhost")
                || base_lower.contains("127.0.0.1"))
        {
            return Err(format!(
                "base_url '{}' looks like a placeholder/development endpoint; \
                 set a real NVIDIA API base URL",
                self.base_url
            )
            .into());
        }
        if !base_lower.starts_with("http://") && !base_lower.starts_with("https://") {
            return Err(format!(
                "base_url '{}' must start with http:// or https://",
                self.base_url
            )
            .into());
        }

        // Reject obviously fake/test keys before startup. The log showed
        // fake***-key / dumm***_key / cursor-dummy-key causing 401s; failing
        // fast here prevents silent operation on a broken key pool.
        if self.keys.is_empty() {
            return Err(
                "No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into(),
            );
        }
        for key in &self.keys {
            if key.len() < 8 {
                return Err(format!(
                    "API key '{}' is too short (must be at least 8 characters)",
                    mask_key(key)
                )
                .into());
            }
            let key_lower = key.to_ascii_lowercase();
            if key_lower.contains("fake")
                || key_lower.contains("dummy")
                || key_lower.contains("test")
                || key_lower.contains("example")
                || key_lower.contains("placeholder")
            {
                return Err(format!(
                    "API key '{}' looks like a placeholder/fake key; \
                     provide real NVIDIA API keys",
                    mask_key(key)
                )
                .into());
            }
        }

        // Warn if the per-request timeout is so short that it will trip on
        // normal LLM response latency. The failure.log showed timeout=1s on
        // chat completions, which is almost certainly a misconfiguration.
        if self.request_timeout_seconds > 0 && self.request_timeout_seconds < 5 {
            warn!(
                "request_timeout_seconds={} is very short; consider 30+ seconds for non-streaming requests",
                self.request_timeout_seconds
            );
        }
        // Warn if streaming timeout is configured but very short
        if self.request_timeout_streaming_seconds > 0 && self.request_timeout_streaming_seconds < 5 {
            warn!(
                "request_timeout_streaming_seconds={} is very short; consider 60+ seconds for streaming",
                self.request_timeout_streaming_seconds
            );
        }

        info!("Configuration validation passed");
        Ok(())
    }

    fn apply_env_overrides(config: &mut Config) {
        for (key, value) in std::env::vars() {
            if !key.starts_with("NVIDIA_") {
                continue;
            }
            let key = &key["NVIDIA_".len()..];
            let key_lower = key.to_ascii_lowercase();
            match key_lower.as_str() {
                "listen_addr" => {
                    if let Ok(addr) = value.parse::<SocketAddr>() {
                        config.listen_addr = addr.to_string();
                    }
                }
                "base_url" => config.base_url = value,
                // keys: comma-separated list
                "keys" => {
                    config.keys = value
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                "rate_limiter_qps_per_key" => {
                    if let Ok(v) = value.parse::<f64>() {
                        config.rate_limiter.qps_per_key = v;
                    }
                }
                "rate_limiter_burst" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.rate_limiter.burst = v;
                    }
                }
                "rate_limiter_token_wait_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.rate_limiter.token_wait_ms = v;
                    }
                }
                "cooldown_base_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.cooldown.base_seconds = v;
                    }
                }
                "cooldown_max_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.cooldown.max_seconds = v;
                    }
                }
                "health_check_interval_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.health_check.interval_seconds = v;
                    }
                }
                "health_check_timeout_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.health_check.timeout_seconds = v;
                    }
                }
                "health_check_max_consecutive_failures" => {
                    if let Ok(v) = value.parse::<usize>() {
                        config.health_check.max_consecutive_failures = v;
                    }
                }
                "observability_enable_metrics" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.observability.enable_metrics = v;
                    }
                }
                "observability_metrics_path" => {
                    config.observability.metrics_path = value;
                }
                "observability_log_level" => {
                    config.observability.log_level = value;
                }
                "request_timeout_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.request_timeout_seconds = v;
                    }
                }
                "retry_max_retries" => {
                    if let Ok(v) = value.parse::<u32>() {
                        config.retry.max_retries = v;
                    }
                }
                "retry_backoff_base_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.retry.backoff_base_ms = v;
                    }
                }
                "retry_backoff_max_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.retry.backoff_max_ms = v;
                    }
                }
                "retry_auth_fail_threshold" => {
                    if let Ok(v) = value.parse::<u32>() {
                        config.retry.auth_fail_threshold = v;
                    }
                }
                "retry_max_key_switches" => {
                    if let Ok(v) = value.parse::<u32>() {
                        config.retry.max_key_switches = v;
                    }
                }
                "allow_local_base_url" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.allow_local_base_url = v;
                    }
                }
                "auth_enabled" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.auth.enabled = v;
                    }
                }
                "auth_bearer_token" => {
                    config.auth.bearer_token = value;
                }
                "auth_admin_token" => {
                    config.auth.admin_token = value;
                }
                "inject_stream_usage" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.inject_stream_usage = v;
                    }
                }
                "synthesize_empty_choices" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.synthesize_empty_choices = v;
                    }
                }
                "retry_on_empty_shell" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.retry_on_empty_shell = v;
                    }
                }
                "empty_shell_retry_max_tokens" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.empty_shell_retry_max_tokens = v;
                    }
                }
                "empty_shell_retry_attempts" => {
                    if let Ok(v) = value.parse::<u32>() {
                        config.empty_shell_retry_attempts = v;
                    }
                }
                "empty_shell_probe_before_retry" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.empty_shell_probe_before_retry = v;
                    }
                }
                "circuit_breaker_threshold" => {
                    if let Ok(v) = value.parse::<u32>() {
                        config.circuit_breaker_threshold = v;
                    }
                }
                "circuit_breaker_per_model" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.circuit_breaker_per_model = v;
                    }
                }
                "empty_shell_retry_backoff_base_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.empty_shell_retry_backoff_base_ms = v;
                    }
                }
                "empty_shell_retry_backoff_max_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.empty_shell_retry_backoff_max_ms = v;
                    }
                }
                "empty_shell_pre_clamp_max_tokens" => {
                    // Parse as JSON object: {"model": max_tokens}
                    if let Ok(map) = serde_json::from_str::<std::collections::HashMap<String, u64>>(&value) {
                        config.empty_shell_pre_clamp_max_tokens = map;
                    }
                }
                "empty_shell_no_retry_prompt_tokens" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.empty_shell_no_retry_prompt_tokens = v;
                    }
                }
                "max_tokens_max" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.max_tokens_max = v;
                    }
                }
                "retry_on_503" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.retry_on_503 = v;
                    }
                }
                "probe_before_503_retry" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.probe_before_503_retry = v;
                    }
                }
                "retry_on_500" => {
                    if let Ok(v) = value.parse::<bool>() {
                        config.retry_on_500 = v;
                    }
                }
                "request_timeout_streaming_seconds" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.request_timeout_streaming_seconds = v;
                    }
                }
                "concurrency_limit_per_model" => {
                    // Parse as JSON object: {"model": limit}
                    if let Ok(map) = serde_json::from_str::<std::collections::HashMap<String, u32>>(&value) {
                        config.concurrency_limit_per_model = map;
                    }
                }
                "concurrency_queue_timeout_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.concurrency_queue_timeout_ms = v;
                    }
                }
                "upstream_connect_timeout_secs" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.upstream_connect_timeout_secs = v;
                    }
                }
                "upstream_pool_idle_timeout_secs" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.upstream_pool_idle_timeout_secs = v;
                    }
                }
                "retry_jitter_ms" => {
                    if let Ok(v) = value.parse::<u64>() {
                        config.retry_jitter_ms = v;
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod config_validation_tests {
    use super::*;

    fn base_valid_config() -> Config {
        Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://integrate.api.nvidia.com".to_string(),
            keys: vec!["nvapi-realvalidkey1234567890".to_string()],
            rate_limiter: RateLimiterConfig {
                qps_per_key: 0.65,
                burst: 8,
                token_wait_ms: 200,
            },
            cooldown: CooldownConfig {
                base_seconds: 60,
                max_seconds: 600,
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
            request_timeout_streaming_seconds: 0,
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
            empty_shell_no_retry_prompt_tokens: default_empty_shell_no_retry_prompt_tokens(),
            circuit_breaker_threshold: 3,
            circuit_breaker_per_model: true,
            retry_on_503: true,
            probe_before_503_retry: true,
            retry_on_500: true,
            max_tokens_max: 100_000,
            concurrency_limit_per_model: std::collections::HashMap::new(),
            concurrency_queue_timeout_ms: 5000,
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
        }
    }

    #[test]
    fn valid_config_passes() {
        let cfg = base_valid_config();
        assert!(cfg.validate().is_ok(), "valid config should pass");
    }

    #[test]
    fn empty_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn example_com_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "http://example.com".to_string();
        let err = cfg.validate().expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("placeholder"), "error should mention placeholder: {}", msg);
    }

    #[test]
    fn localhost_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "http://127.0.0.1:8080".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn missing_scheme_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "integrate.api.nvidia.com".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn fake_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["fake-key-1234567890".to_string()];
        let err = cfg.validate().expect_err("should fail");
        assert!(err.to_string().contains("placeholder"));
    }

    #[test]
    fn dummy_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["cursor-dummy-key".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn short_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["nvapi-1".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_keys_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec![];
        assert!(cfg.validate().is_err());
    }
}
