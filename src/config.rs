use dotenvy::dotenv;
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::net::SocketAddr;

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
}

fn default_request_timeout_seconds() -> u64 {
    60
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
            .field("retry", &self.retry)
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimiterConfig {
    pub qps_per_key: f64,
    pub burst: u64,
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

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            auth_fail_threshold: default_auth_fail_threshold(),
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
        }

        // Override from environment variables (NVIDIA_*)
        Self::apply_env_overrides(&mut config);

        if config.keys.is_empty() {
            return Err(
                "No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into(),
            );
        }

        Ok(config)
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
                _ => {}
            }
        }
    }
}
