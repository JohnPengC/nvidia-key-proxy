use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::key_pool::{mask_key, KeyPool};

#[allow(dead_code)]
pub struct HealthChecker {
    key_pool: Arc<KeyPool>,
    base_url: String,
    interval: Duration,
    timeout: Duration,
    max_failures: usize,
    client: reqwest::Client,
}

impl HealthChecker {
    pub fn new(
        key_pool: Arc<KeyPool>,
        base_url: String,
        interval_seconds: u64,
        timeout_seconds: u64,
        max_consecutive_failures: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()
            .expect("Failed to build reqwest client");
        Self {
            key_pool,
            base_url: base_url.trim_end_matches('/').to_string(),
            interval: Duration::from_secs(interval_seconds),
            timeout: Duration::from_secs(timeout_seconds),
            max_failures: max_consecutive_failures,
            client,
        }
    }

    /// Run once at startup to check all keys.
    pub async fn run_startup(&self) {
        info!("Running startup health check for all keys");
        if let Err(e) = self.check_all_keys().await {
            error!("Startup health check failed: {}", e);
        }
    }

    /// Periodically check only bad (cooling) keys to see if they've recovered.
    /// `stop` allows graceful shutdown of this background loop.
    pub async fn run_bad_key_checker(&self, stop: Arc<tokio::sync::Notify>) {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.check_bad_keys().await {
                        error!("Bad key check round failed: {}", e);
                    }
                }
                _ = stop.notified() => {
                    info!("Bad key checker stopping");
                    break;
                }
            }
        }
    }

    async fn check_all_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<String> = {
            let read_guard = self.key_pool.inner.read().await;
            read_guard.keys().cloned().collect()
        };

        for key_str in keys {
            match self.probe_key(&key_str).await {
                Ok(status) if status.is_success() => {
                    self.key_pool.mark_good(&key_str).await;
                }
                Ok(status) => {
                    // Don't punish the key: NVIDIA's /v1/models can return
                    // 403/401 on a perfectly valid key (account/region gating).
                    // Cool-down here would drain the pool. Only log and leave
                    // the key's state untouched.
                    warn!(key = %mask_key(&key_str), status = %status, "Startup probe non-2xx; leaving key state untouched");
                }
                Err(e) => {
                    warn!(key = %mask_key(&key_str), error = %e, "Startup probe error; leaving key state untouched");
                }
            }
        }
        Ok(())
    }

    /// Check only keys currently in Cooling state. A key whose cooldown has not
    /// yet elapsed is skipped — we only probe keys whose ready_at has passed,
    /// so cooldown timing is respected and we avoid probing a key too early.
    async fn check_bad_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bad_keys = self.key_pool.get_bad_keys().await;
        // Also probe Disabled keys: those were marked bad after repeated
        // auth failures (see Proxy::handle_request + KeyPool::mark_failure).
        // A success here reactivates them; a non-success leaves them Disabled.
        let disabled_keys = self.key_pool.get_disabled_keys().await;
        if bad_keys.is_empty() && disabled_keys.is_empty() {
            return Ok(());
        }
        info!(
            bad = bad_keys.len(),
            disabled = disabled_keys.len(),
            "Recovery probe round"
        );
        for key_str in &bad_keys {
            match self.probe_key(key_str).await {
                Ok(status) if status.is_success() => {
                    info!(key = %mask_key(key_str), "Bad key recovered via probe; marking as good");
                    self.key_pool.mark_good(key_str).await;
                }
                Ok(status) => {
                    // /v1/models returning non-2xx is not decisive evidence
                    // the key is bad. Defer to cooldown timing for recovery
                    // (cool_down_task reactivates once ready_at passes).
                    warn!(key = %mask_key(key_str), status = %status, "Recovery probe non-2xx; deferring to cooldown timer");
                }
                Err(e) => {
                    warn!(key = %mask_key(key_str), error = %e, "Recovery probe error; deferring to cooldown timer");
                }
            }
        }
        for key_str in &disabled_keys {
            match self.probe_key(key_str).await {
                Ok(status) if status.is_success() => {
                    info!(key = %mask_key(key_str), "Disabled key recovered via probe; reactivating");
                    self.key_pool.recover_disabled(key_str).await;
                }
                Ok(status) => {
                    warn!(key = %mask_key(key_str), status = %status, "Disabled key still failing probe; staying Disabled");
                }
                Err(e) => {
                    warn!(key = %mask_key(key_str), error = %e, "Disabled key probe error; staying Disabled");
                }
            }
        }
        Ok(())
    }

    async fn probe_key(
        &self,
        key: &str,
    ) -> Result<StatusCode, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/v1/models", self.base_url);
        let resp = self
            .client
            .get(&url)
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", key))
            .send()
            .await?;
        Ok(resp.status())
    }
}
