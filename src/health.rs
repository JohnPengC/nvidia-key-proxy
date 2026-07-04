use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use reqwest::StatusCode;

use crate::key_pool::{KeyPool, mask_key};

#[allow(dead_code)]
pub struct HealthChecker {
    key_pool: Arc<KeyPool>,
    base_url: String,
    interval: Duration,
    timeout: Duration,
    max_failures: usize,
    client: reqwest::Client,
    stop_flag: Arc<tokio::sync::Notify>,
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
            stop_flag: Arc::new(tokio::sync::Notify::new()),
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

    #[allow(dead_code)]
    pub fn stop(&self) {
        self.stop_flag.notify_one();
    }

    async fn check_all_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<String> = {
            let read_guard = self.key_pool.inner.read().await;
            read_guard.keys().cloned().collect()
        };

        for key_str in keys {
            let result = self.probe_key(&key_str).await;
            match result {
                Ok(status) if status.is_success() => {
                    self.key_pool.mark_good(&key_str).await;
                }
                Ok(status) => {
                    warn!(key = %mask_key(&key_str), status = %status, "Health check failed");
                    self.key_pool.mark_bad(&key_str, None).await;
                }
                Err(e) => {
                    warn!(key = %mask_key(&key_str), error = %e, "Health check error");
                    self.key_pool.mark_bad(&key_str, None).await;
                }
            }
            let _ = self.timeout; // timeout already applied via client builder
        }
        Ok(())
    }

    /// Check only keys currently in Cooling state. A key whose cooldown has not
    /// yet elapsed is skipped — we only probe keys whose ready_at has passed,
    /// so cooldown timing is respected and we avoid probing a key too early.
    async fn check_bad_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bad_keys = self.key_pool.get_bad_keys().await;
        if bad_keys.is_empty() {
            return Ok(());
        }
        info!("Checking {} bad key(s) for recovery", bad_keys.len());
        for key_str in bad_keys {
            let result = self.probe_key(&key_str).await;
            match result {
                Ok(status) if status.is_success() => {
                    info!(key = %mask_key(&key_str), "Bad key recovered, marking as good");
                    self.key_pool.mark_good(&key_str).await;
                }
                Ok(status) => {
                    warn!(key = %mask_key(&key_str), status = %status, "Bad key still not healthy");
                }
                Err(e) => {
                    warn!(key = %mask_key(&key_str), error = %e, "Bad key probe error");
                }
            }
        }
        Ok(())
    }

    async fn probe_key(&self, key: &str) -> Result<StatusCode, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/v1/models", self.base_url);
        let resp = self.client
            .get(&url)
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", key))
            .send()
            .await?;
        Ok(resp.status())
    }
}
