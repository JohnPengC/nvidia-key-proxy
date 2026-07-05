use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::config::Config;
use crate::metrics::Metrics;

/// Mask an API key for display: first 4 + "***" + last 4 chars.
/// If key is shorter than 8 chars, returns "".
pub fn mask_key(key: &str) -> String {
    if key.len() < 8 {
        return "***".to_string();
    }
    let first = &key[..4];
    let last = &key[key.len() - 4..];
    format!("{}***{}", first, last)
}

/// Unique identifier for a key (we keep the string in an Arc to avoid cloning).
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KeyId(pub Arc<String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum KeyState {
    Active,
    Cooling {
        ready_at: Instant,
    },
    /// Disabled after repeated auth failures. `ready_at` is a fallback expiry:
    /// if a health probe never recovers the key (probe endpoint itself is
    /// unavailable, or probes keep failing), `cool_down_task` will reactivate
    /// the key once `ready_at` passes — so the pool can't permanently shrink
    /// to zero when probes fail.
    Disabled {
        ready_at: Instant,
    },
}

/// Internal state stored per key.
#[derive(Debug)]
pub struct InnerKeyState {
    pub key: Arc<String>,
    pub state: KeyState,
    pub weight: u32, // used for weighted random selection
    pub fail_count: u32,
    pub last_success: Option<Instant>,
    pub bucket: TokenBucket,
}

/// Public handle to a key's state (cloneable, cheap).
#[derive(Debug, Clone)]
pub struct KeyStateHandle {
    pub(crate) inner: Arc<RwLock<InnerKeyState>>,
}

impl KeyStateHandle {
    /// Returns the key as an owned String.
    ///
    /// Uses the async `read()` path rather than `blocking_read()`, which
    /// panics when called from inside a tokio runtime worker thread (the
    /// exact panic we hit before this fix).  Tokio's `RwLock` is never
    /// poisoned, so `read().await` is both safe and correct here.
    pub async fn key(&self) -> String {
        let guard = self.inner.read().await;
        (*guard.key).clone()
    }

    #[allow(dead_code)]
    pub async fn key_masked(&self) -> String {
        mask_key(&self.key().await)
    }

    pub async fn state(&self) -> KeyState {
        self.inner.read().await.state
    }

    /// Current consecutive auth/forbidden failure counter.
    pub async fn fail_count(&self) -> u32 {
        self.inner.read().await.fail_count
    }

    pub async fn set_state(&self, state: KeyState) {
        let mut guard = self.inner.write().await;
        guard.state = state;
    }

    pub async fn weight(&self) -> u32 {
        self.inner.read().await.weight
    }

    pub async fn add_weight(&self, delta: i32) {
        let mut guard = self.inner.write().await;
        let w = guard.weight as i32 + delta;
        guard.weight = if w > 0 { w as u32 } else { 1 };
    }

    pub async fn reset_weight(&self) {
        let mut guard = self.inner.write().await;
        guard.weight = 1;
    }

    pub async fn increment_fail(&self) {
        let mut guard = self.inner.write().await;
        guard.fail_count += 1;
    }

    pub async fn reset_fail(&self) {
        let mut guard = self.inner.write().await;
        guard.fail_count = 0;
    }

    pub async fn set_last_success(&self) {
        let mut guard = self.inner.write().await;
        guard.last_success = Some(Instant::now());
    }

    /// Consume one token from the key's bucket; waits until a token is available.
    ///
    /// The bucket lives inside `InnerKeyState` and is accessed through its own
    /// internal `tokio::Mutex`. We hold the outer read lock for the duration of
    /// the await — this is safe because no other path takes the outer read
    /// lock and then writes the *bucket*, and the bucket's own Mutex serializes
    /// access. The outer RwLock is never poisoned by tokio.
    pub async fn consume_token(&self) {
        let guard = self.inner.read().await;
        let _ = guard.bucket.consume(1.0).await;
    }
}

/// Simple token bucket implementation. Not Clone — cloning would silently
/// reset `tokens` and `last_refill`, bypassing the rate limit. Each key owns
/// exactly one bucket for its lifetime.
pub struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        let cap = capacity as f64;
        Self {
            capacity: cap,
            refill_per_sec,
            tokens: Mutex::new(cap), // start full
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Attempt to consume `tokens` tokens, waiting until enough are available.
    pub async fn consume(&self, tokens: f64) -> Result<(), ()> {
        if tokens <= 0.0 {
            return Ok(());
        }
        if self.refill_per_sec <= 0.0 {
            // No refill configured — never block forever. Return success since
            // the bucket starts full and we have no way to replenish.
            return Ok(());
        }
        loop {
            let mut toks = self.tokens.lock().await;
            let mut last = self.last_refill.lock().await;
            let now = Instant::now();
            let elapsed = now.duration_since(*last).as_secs_f64();
            let added = elapsed * self.refill_per_sec;
            if added > 0.0 {
                *toks = (*toks + added).min(self.capacity);
                *last = now;
            }
            if *toks >= tokens {
                *toks -= tokens;
                return Ok(());
            }
            // Not enough tokens; release locks and sleep until we expect to have enough.
            let need = tokens - *toks;
            drop((toks, last));
            let wait_secs = need / self.refill_per_sec;
            if wait_secs > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(wait_secs)).await;
            }
            // loop again
        }
    }
}

impl std::fmt::Debug for TokenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TokenBucket {{ capacity: {}, refill_per_sec: {} }}",
            self.capacity, self.refill_per_sec
        )
    }
}

pub struct KeyPool {
    /// map from key string to handle
    pub(crate) inner: RwLock<HashMap<String, KeyStateHandle>>,
    config: Config,
    metrics: Arc<Metrics>,
    /// For broadcasting state changes (optional).
    pub_notifier: tokio::sync::broadcast::Sender<KeyStateHandle>,
}

impl KeyPool {
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(16);
        let mut inner = HashMap::new();
        // Initialize keys
        for key_str in &config.keys {
            let key_id = KeyId(Arc::new(key_str.clone()));
            let bucket = TokenBucket::new(
                config.rate_limiter.burst as u32,
                if config.rate_limiter.qps_per_key > 0.0 {
                    config.rate_limiter.qps_per_key
                } else {
                    1.0
                },
            );
            let handle = KeyStateHandle {
                inner: Arc::new(RwLock::new(InnerKeyState {
                    key: key_id.0.clone(),
                    state: KeyState::Active,
                    weight: 1,
                    fail_count: 0,
                    last_success: None,
                    bucket,
                })),
            };
            inner.insert(key_str.clone(), handle);
        }
        Self {
            inner: RwLock::new(inner),
            config: config.clone(),
            metrics,
            pub_notifier: tx,
        }
    }

    /// Returns a handle to a randomly selected active key based on weight.
    /// Returns None if no active keys.
    pub async fn acquire(&self) -> Option<KeyStateHandle> {
        // Sample active keys and their weights in a single read lock per key
        // (rather than once for `state()` and again for `weight()`).
        let mut active: Vec<(KeyStateHandle, u32)> = vec![];
        let read_guard = self.inner.read().await;
        for (_key_str, handle) in read_guard.iter() {
            let guard = handle.inner.read().await;
            let is_active = matches!(guard.state, KeyState::Active);
            let w = guard.weight;
            drop(guard);
            if is_active && w > 0 {
                active.push((handle.clone(), w));
            }
        }
        drop(read_guard);
        if active.is_empty() {
            None
        } else {
            // Weighted random selection
            let total_weight: u32 = active.iter().map(|(_, w)| w).sum();
            if total_weight == 0 {
                return active.first().map(|(h, _)| h.clone());
            }
            let mut rand_val: u32 = fastrand::u32(..total_weight);
            let mut result: Option<KeyStateHandle> = None;
            for (handle, weight) in &active {
                if rand_val < *weight {
                    result = Some(handle.clone());
                    break;
                }
                rand_val -= *weight;
            }
            result.or_else(|| active.first().map(|(h, _)| h.clone()))
        }
    }

    /// Get all keys currently in Cooling state (for bad key recovery).
    pub async fn get_bad_keys(&self) -> Vec<String> {
        // Snapshot the handles first so we don't hold the outer lock while
        // awaiting each inner lock.
        let handles: Vec<(String, KeyStateHandle)> = {
            let read_guard = self.inner.read().await;
            read_guard
                .iter()
                .map(|(k, h)| (k.clone(), h.clone()))
                .collect()
        };
        let mut bad = Vec::new();
        for (key_str, handle) in handles {
            let guard = handle.inner.read().await;
            if matches!(guard.state, KeyState::Cooling { .. }) {
                bad.push(key_str);
            }
        }
        bad
    }

    /// Get all keys currently in Disabled state (for health-probe recovery).
    pub async fn get_disabled_keys(&self) -> Vec<String> {
        let handles: Vec<(String, KeyStateHandle)> = {
            let read_guard = self.inner.read().await;
            read_guard
                .iter()
                .map(|(k, h)| (k.clone(), h.clone()))
                .collect()
        };
        let mut disabled = Vec::new();
        for (key_str, handle) in handles {
            let guard = handle.inner.read().await;
            if matches!(guard.state, KeyState::Disabled { .. }) {
                disabled.push(key_str);
            }
        }
        disabled
    }

    /// Atomically record a non-throttling upstream auth/forbidden failure for
    /// `key_str` and, if the consecutive failure count crosses the configured
    /// threshold, transition the key to `KeyState::Disabled` until a health
    /// probe recovers it. Returns `true` if the key was just disabled.
    pub async fn mark_failure(&self, key_str: &str, threshold: u32) -> bool {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            handle.increment_fail().await;
            let fail_count = handle.fail_count().await;
            let already_disabled = matches!(handle.state().await, KeyState::Disabled { .. });
            if already_disabled {
                // Already disabled; nothing more to do (weight is already 1).
                self.refresh_metrics().await;
                return true;
            }
            // Halve the weight even below threshold so the next weighted-random
            // `acquire()` is unlikely to re-pick this same key within one retry
            // sequence. Fix for "retry wastes an attempt on the same bad key".
            {
                let w = handle.weight().await as i32;
                let delta = -(w.max(1) / 2);
                handle.add_weight(delta).await;
            }
            if fail_count >= threshold {
                let ready_at =
                    Instant::now() + Duration::from_secs(self.config.cooldown.max_seconds);
                handle.set_state(KeyState::Disabled { ready_at }).await;
                warn!(
                    key = %mask_key(key_str),
                    fail_count,
                    threshold,
                    "Key disabled after {} consecutive auth failures; awaiting health probe or expiry recovery",
                    fail_count
                );
                let _ = self.pub_notifier.send(handle);
                self.refresh_metrics().await;
                return true;
            }
            warn!(
                key = %mask_key(key_str),
                fail_count,
                threshold,
                "Recorded non-throttling auth failure (no disable yet)",
            );
        }
        self.refresh_metrics().await;
        false
    }

    /// Reactivate a previously Disabled key after a successful health probe.
    pub async fn recover_disabled(&self, key_str: &str) {
        self.mark_good(key_str).await;
    }

    /// Count keys by state — for Prometheus gauges.
    pub async fn count_states(&self) -> (i64, i64, i64) {
        let handles: Vec<KeyStateHandle> = {
            let read_guard = self.inner.read().await;
            read_guard.values().cloned().collect()
        };
        let mut active = 0i64;
        let mut cooling = 0i64;
        let mut disabled = 0i64;
        for handle in handles {
            let guard = handle.inner.read().await;
            match guard.state {
                KeyState::Active => active += 1,
                KeyState::Cooling { .. } => cooling += 1,
                KeyState::Disabled { .. } => disabled += 1,
            }
        }
        (active, cooling, disabled)
    }

    /// Refresh the Prometheus key-state gauges if metrics are attached.
    pub async fn refresh_metrics(&self) {
        let (a, c, d) = self.count_states().await;
        self.metrics.set_key_counts(a, c, d);
    }

    /// Mark a key as good (successful request)
    pub async fn mark_good(&self, key_str: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            handle.set_state(KeyState::Active).await;
            handle.reset_weight().await;
            handle.reset_fail().await;
            handle.set_last_success().await;
            info!(key = %mask_key(key_str), "Marked key as good (active)");
            let _ = self.pub_notifier.send(handle);
        }
        self.refresh_metrics().await;
    }

    /// Mark a key as bad (got 429/403) and put it into cooldown.
    pub async fn mark_bad(&self, key_str: &str, retry_after: Option<u64>) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            let base = self.config.cooldown.base_seconds;
            let max = self.config.cooldown.max_seconds;
            let delay = retry_after.unwrap_or(base).min(max);
            let ready_at = Instant::now() + Duration::from_secs(delay);
            handle.set_state(KeyState::Cooling { ready_at }).await;
            let w = handle.weight().await as i32;
            let delta = -(w.max(1) / 2);
            handle.add_weight(delta).await;
            handle.increment_fail().await;
            warn!(
                key = %mask_key(key_str),
                delay = delay,
                "Marked key as bad, cooling for {} seconds",
                delay
            );
            let _ = self.pub_notifier.send(handle);
        }
        self.refresh_metrics().await;
    }

    /// Background task to move cooling keys back to active when ready.
    /// `stop` is a Notify that signals graceful shutdown.
    pub async fn cool_down_task(self: Arc<Self>, stop: Arc<tokio::sync::Notify>) {
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.config.health_check.interval_seconds,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = stop.notified() => {
                    info!("Cooldown task stopping");
                    break;
                }
            }
            let now = Instant::now();
            let read_guard = self.inner.read().await;
            let mut to_update = Vec::new();
            for (key_str, handle) in read_guard.iter() {
                match handle.state().await {
                    KeyState::Cooling { ready_at } if now >= ready_at => {
                        let handle_clone = handle.clone();
                        to_update.push((key_str.clone(), handle_clone));
                    }
                    KeyState::Disabled { ready_at } if now >= ready_at => {
                        // Fallback recovery: a health probe is the primary
                        // recovery path, but if probes keep failing (or the
                        // probe endpoint itself is unavailable) we must not
                        // let the pool permanently shrink. Let the key retry.
                        let handle_clone = handle.clone();
                        to_update.push((key_str.clone(), handle_clone));
                    }
                    _ => {}
                }
            }
            drop(read_guard);
            for (key_str, handle) in to_update {
                let mut guard = handle.inner.write().await;
                let was_disabled = matches!(guard.state, KeyState::Disabled { .. });
                guard.state = KeyState::Active;
                guard.weight = 1;
                guard.fail_count = 0;
                if was_disabled {
                    info!(key = %mask_key(&key_str), "Disabled key reactivated by cooldown expiry (probes likely failing)");
                } else {
                    info!(key = %mask_key(&key_str), "Cooldown finished, reactivating key (no probe)");
                }
            }
            self.refresh_metrics().await;
        }
    }
}

#[cfg(test)]
mod retry_state_machine_tests {
    use super::*;
    use crate::config::{
        CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
    };

    fn build_pool(keys: &[&str]) -> KeyPool {
        let cfg = Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://example.test".to_string(),
            keys: keys.iter().map(|s| s.to_string()).collect(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: 100.0,
                burst: 100,
            },
            cooldown: CooldownConfig {
                base_seconds: 1,
                max_seconds: 2,
            },
            health_check: HealthCheckConfig {
                interval_seconds: 1,
                timeout_seconds: 1,
                max_consecutive_failures: 3,
            },
            observability: ObservabilityConfig {
                enable_metrics: false,
                metrics_path: "/metrics".to_string(),
                log_level: "info".to_string(),
            },
            codex: crate::codex_config::CodexConfig::default(),
            request_timeout_seconds: 0,
            retry: crate::config::RetryConfig::default(),
        };
        let metrics = Arc::new(Metrics::new());
        KeyPool::new(&cfg, metrics)
    }

    #[tokio::test]
    async fn mark_failure_below_threshold_keeps_active() {
        let pool = build_pool(&["k1", "k2"]);
        // threshold = 3; two failures must NOT disable the key.
        assert!(!pool.mark_failure("k1", 3).await);
        assert!(!pool.mark_failure("k1", 3).await);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert_eq!(handle.state().await, KeyState::Active);
        assert_eq!(handle.fail_count().await, 2);
    }

    #[tokio::test]
    async fn mark_failure_at_threshold_disables_key() {
        let pool = build_pool(&["k1", "k2"]);
        pool.mark_failure("k1", 3).await;
        pool.mark_failure("k1", 3).await;
        assert!(
            pool.mark_failure("k1", 3).await,
            "third failure should disable the key"
        );
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert!(matches!(handle.state().await, KeyState::Disabled { .. }));
        // A Disabled key is no longer acquired.
        let acquired = pool.acquire().await.expect("another key still active");
        assert_ne!(acquired.key().await, "k1");
    }

    #[tokio::test]
    async fn recover_disabled_reactivates_key() {
        let pool = build_pool(&["k1", "k2"]);
        for _ in 0..3 {
            pool.mark_failure("k1", 3).await;
        }
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert!(matches!(handle.state().await, KeyState::Disabled { .. }));
        pool.recover_disabled("k1").await;
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert_eq!(handle.state().await, KeyState::Active);
    }

    #[tokio::test]
    async fn get_disabled_keys_lists_only_disabled() {
        let pool = build_pool(&["k1", "k2", "k3"]);
        for _ in 0..3 {
            pool.mark_failure("k1", 3).await;
        }
        for _ in 0..3 {
            pool.mark_failure("k2", 3).await;
        }
        let disabled = pool.get_disabled_keys().await;
        assert_eq!(disabled.len(), 2);
        assert!(disabled.contains(&"k1".to_string()));
        assert!(disabled.contains(&"k2".to_string()));
        assert!(!disabled.contains(&"k3".to_string()));
    }

    #[tokio::test]
    async fn mark_failure_halves_weight_even_below_threshold() {
        // Even before the disable threshold is reached, mark_failure must
        // reduce the key's weight so a subsequent weighted-random acquire()
        // is unlikely to re-select it within the same retry sequence.
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        // Default weight starts at 1 — to make the "halving" observable, bump
        // it to a known higher value first.
        handle.add_weight(7).await; // weight = 1 + 7 = 8
        assert_eq!(handle.weight().await, 8);
        let disabled = pool.mark_failure("k1", 100).await; // threshold high → no disable
        assert!(!disabled);
        let w = handle.weight().await;
        assert_eq!(
            w, 4,
            "weight should be halved (8 → 4) after one mark_failure"
        );
        let disabled2 = pool.mark_failure("k1", 100).await;
        assert!(!disabled2);
        let w2 = handle.weight().await;
        assert_eq!(w2, 2, "weight should halve again (4 → 2)");
    }
}
