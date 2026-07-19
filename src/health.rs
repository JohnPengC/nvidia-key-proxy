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
    /// Per-key count of *consecutive probe errors* (network/probe failures,
    /// not non-2xx). When a key's count reaches `max_failures`, the health
    /// checker force-disables it (see `recover_one`) instead of forever
    /// deferring to the cooldown timer. Reset to 0 on a successful probe.
    probe_failures: std::sync::Mutex<std::collections::HashMap<String, usize>>,
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
            probe_failures: std::sync::Mutex::new(std::collections::HashMap::new()),
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
            self.recover_one(key_str, false).await;
        }
        for key_str in &disabled_keys {
            self.recover_one(key_str, true).await;
        }
        Ok(())
    }

    /// Probe a single bad/disabled key and react accordingly.
    ///
    /// * Success → recover the key (`mark_good` / `recover_disabled`) and reset
    ///   its consecutive-probe-error counter.
    /// * Non-2xx → not decisive (NVIDIA's `/v1/models` can 403/401 on a
    ///   perfectly valid key), so defer to the cooldown timer; the error
    ///   counter is left untouched.
    /// * Probe *error* (network/timeout) → increment the consecutive-error
    ///   counter. Once it reaches `max_failures` the key is force-disabled so a
    ///   persistently-unreachable key can't keep cycling through cooldown
    ///   forever. A `max_failures` of 0 disables this behaviour.
    async fn recover_one(&self, key_str: &str, was_disabled: bool) {
        match self.probe_key(key_str).await {
            Ok(status) if status.is_success() => {
                self.reset_probe_failures(key_str);
                if was_disabled {
                    info!(key = %mask_key(key_str), "Disabled key recovered via probe; reactivating");
                    self.key_pool.recover_disabled(key_str).await;
                } else {
                    info!(key = %mask_key(key_str), "Bad key recovered via probe; marking as good");
                    self.key_pool.mark_good(key_str).await;
                }
            }
            Ok(status) => {
                warn!(key = %mask_key(key_str), status = %status, "Recovery probe non-2xx; deferring to cooldown timer");
            }
            Err(e) => {
                let count = {
                    let mut map = self.probe_failures.lock().unwrap();
                    let c = map.entry(key_str.to_string()).or_insert(0);
                    *c += 1;
                    *c
                };
                warn!(key = %mask_key(key_str), error = %e, probe_errors = count, max_failures = self.max_failures, "Recovery probe error");
                if self.max_failures > 0 && count >= self.max_failures {
                    warn!(key = %mask_key(key_str), "Consecutive probe failures reached threshold; force-disabling key");
                    self.key_pool.disable_key(key_str).await;
                    self.reset_probe_failures(key_str);
                }
            }
        }
    }

    fn reset_probe_failures(&self, key_str: &str) {
        self.probe_failures.lock().unwrap().remove(key_str);
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
