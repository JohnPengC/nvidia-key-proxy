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
    Cooling { ready_at: Instant },
    Disabled,
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
        write!(f, "TokenBucket {{ capacity: {}, refill_per_sec: {} }}", self.capacity, self.refill_per_sec)
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
            let bucket = TokenBucket::new(config.rate_limiter.burst as u32, if config.rate_limiter.qps_per_key > 0.0 { config.rate_limiter.qps_per_key } else { 1.0 });
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
        // First collect active keys and their weights
        let mut active: Vec<(KeyStateHandle, u32)> = vec![];
        let read_guard = self.inner.read().await;
        for (_key_str, handle) in read_guard.iter() {
            if matches!(handle.state().await, KeyState::Active) {
                let w = handle.weight().await;
                if w > 0 {
                    let handle_clone = handle.clone();
                    active.push((handle_clone, w));
                }
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
        let read_guard = self.inner.read().await;
        let mut bad = Vec::new();
        for (key_str, handle) in read_guard.iter() {
            if matches!(handle.state().await, KeyState::Cooling { .. }) {
                bad.push(key_str.clone());
            }
        }
        bad
    }

    /// Count keys by state — for Prometheus gauges.
    pub async fn count_states(&self) -> (i64, i64, i64) {
        let read_guard = self.inner.read().await;
        let mut active = 0i64;
        let mut cooling = 0i64;
        let mut disabled = 0i64;
        for (_, handle) in read_guard.iter() {
            match handle.state().await {
                KeyState::Active => active += 1,
                KeyState::Cooling { .. } => cooling += 1,
                KeyState::Disabled => disabled += 1,
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
            let delay = retry_after
                .map(|s| s as u64)
                .unwrap_or(base)
                .min(max);
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
        let mut interval = tokio::time::interval(
            Duration::from_secs(self.config.health_check.interval_seconds),
        );
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
                if let KeyState::Cooling { ready_at } = handle.state().await {
                    if now >= ready_at {
                        let handle_clone = handle.clone();
                        to_update.push((key_str.clone(), handle_clone));
                    }
                }
            }
            drop(read_guard);
            for (key_str, handle) in to_update {
                match handle.inner.try_write() {
                    Ok(mut guard) => {
                        guard.state = KeyState::Active;
                        guard.weight = 1;
                        guard.fail_count = 0;
                        info!(key = %mask_key(&key_str), "Cooldown finished, reactivating key (no probe)");
                    }
                    Err(_) => {
                        warn!(key = %mask_key(&key_str), "Failed to acquire write lock for cooldown reactivation");
                    }
                }
            }
            self.refresh_metrics().await;
        }
    }

}
