use serde::Deserialize;
use dotenvy::dotenv;
use std::fmt;
use std::fs;
use std::net::SocketAddr;

use crate::codex_config::CodexConfig;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub base_url: String,
    pub keys: Vec<String>,
    pub rate_limiter: RateLimiterConfig,
    pub cooldown: CooldownConfig,
    pub health_check: HealthCheckConfig,
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub codex: CodexConfig,
    /// Optional request timeout in seconds. 0 disables the timeout entirely
    /// (recommended when proxying long-running streaming requests).
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
}

fn default_request_timeout_seconds() -> u64 {
    60
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let masked_keys: Vec<String> = self.keys.iter()
            .map(|k| {
                if k.len() <= 8 {
                    "***".to_string()
                } else {
                    format!("{}***{}", &k[..4], &k[k.len()-4..])
                }
            })
            .collect();
        f.debug_struct("Config")
            .field("listen_addr", &self.listen_addr)
            .field("base_url", &self.base_url)
            .field("keys", &format!("<{} keys: {:?}>", self.keys.len(), masked_keys))
            .field("rate_limiter", &self.rate_limiter)
            .field("cooldown", &self.cooldown)
            .field("health_check", &self.health_check)
            .field("observability", &self.observability)
            .field("codex", &self.codex.enabled)
            .field("request_timeout_seconds", &self.request_timeout_seconds)
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
                max_seconds: 300,
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
            codex: crate::codex_config::CodexConfig::default(),
            request_timeout_seconds: default_request_timeout_seconds(),
        };

        // Prefer NVIDIA_CONFIG_PATH, fall back to ./config.toml
        let config_path = std::env::var("NVIDIA_CONFIG_PATH")
            .unwrap_or_else(|_| "config.toml".to_string());

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
            config.codex = toml_config.codex;
            config.request_timeout_seconds = toml_config.request_timeout_seconds;
        }

        // Override from environment variables (NVIDIA_*)
        Self::apply_env_overrides(&mut config);

        if config.keys.is_empty() {
            return Err("No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into());
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
                _ => {}
            }
        }
    }
}
