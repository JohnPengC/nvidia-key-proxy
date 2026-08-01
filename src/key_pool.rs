use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::config::Config;
use crate::metrics::Metrics;

/// Mask an API key for display: first 4 chars + "***" + last 4 chars.
/// If the key has fewer than 8 characters, returns "***".
pub fn mask_key(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() < 8 {
        return "***".to_string();
    }
    let first: String = chars.iter().take(4).collect();
    let last: String = chars.iter().skip(chars.len() - 4).collect();
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
    pub weight: u32, // penalty multiplier for score-based selection
    pub fail_count: u32,
    pub last_success: Option<Instant>,
    pub bucket: TokenBucket,
    /// Consecutive empty-shell or 504 responses for circuit breaker.
    /// If circuit_breaker_per_model is true, tracks per-model counts.
    /// Otherwise uses "global" key.
    pub circuit_breaker_failures: std::collections::HashMap<String, u32>,
}

/// Public handle to a key's state (cloneable, cheap).
#[derive(Debug, Clone)]
pub struct KeyStateHandle {
    pub(crate) inner: Arc<RwLock<InnerKeyState>>,
    /// How many requests currently hold this key (acquired but not yet
    /// returned / released). Kept outside `inner` so we can update it
    /// synchronously without ever locking the async RwLock.
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU32>,
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
        // Reset circuit breaker on success
        guard.circuit_breaker_failures.clear();
    }

    /// Increment circuit breaker failure count for a specific model.
    /// Returns true if threshold is exceeded.
    pub async fn increment_circuit_breaker(&self, model: &str, threshold: u32) -> bool {
        let mut guard = self.inner.write().await;
        let count = guard.circuit_breaker_failures.entry(model.to_string()).or_insert(0);
        *count += 1;
        *count >= threshold
    }

    /// Check if circuit breaker threshold is exceeded for a model.
    pub async fn check_circuit_breaker(&self, model: &str, threshold: u32) -> bool {
        let guard = self.inner.read().await;
        guard.circuit_breaker_failures.get(model).map_or(false, |c| *c >= threshold)
    }

    /// Reset circuit breaker failure count for a model.
    pub async fn reset_circuit_breaker(&self, model: &str) {
        let mut guard = self.inner.write().await;
        guard.circuit_breaker_failures.remove(model);
    }

    /// Reset all circuit breaker failure counts.
    pub async fn reset_all_circuit_breakers(&self) {
        let mut guard = self.inner.write().await;
        guard.circuit_breaker_failures.clear();
    }

    // ── in-flight tracking ──────────────────────────────────────────

    pub fn inc_in_flight(&self) {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn dec_in_flight(&self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn in_flight(&self) -> u32 {
        self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Try to consume one token from the key's bucket with a deadline.
    /// Returns `true` when the token was consumed before the deadline,
    /// `false` when the deadline elapsed without success. The caller should
    /// drop this key handle and acquire a different one on `false`.
    pub async fn try_consume_token(&self, timeout_ms: u64) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        let guard = self.inner.read().await;
        guard
            .bucket
            .consume_with_timeout(1.0, deadline)
            .await
            .is_ok()
    }

    /// Available tokens in this key's bucket (for score calculation).
    pub async fn available_tokens(&self) -> f64 {
        let guard = self.inner.read().await;
        guard.bucket.available_tokens().await
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

/// Outcome of a non-blocking consume attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeError {
    /// Bucket has fewer tokens than requested right now.
    NotEnough,
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

    /// Refill the bucket proportionally to elapsed time. Must be called with
    /// both mutexes held.
    fn refill_locked(&self, toks: &mut f64, last: &mut Instant) {
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed > 0.0 && self.refill_per_sec > 0.0 {
            *toks = (*toks + elapsed * self.refill_per_sec).min(self.capacity);
            *last = now;
        }
    }

    /// Non-blocking snapshot of the current available tokens (after a refill
    /// pass). Returns `capacity` if the refill rate is zero (bucket stays full).
    pub async fn available_tokens(&self) -> f64 {
        if self.refill_per_sec <= 0.0 {
            return self.capacity;
        }
        let mut toks = self.tokens.lock().await;
        let mut last = self.last_refill.lock().await;
        self.refill_locked(&mut toks, &mut last);
        *toks
    }

    /// Try to consume `tokens` **without blocking**. Returns `Ok(())` if the
    /// bucket had enough tokens (they are deducted atomically with the refill
    /// pass). Returns `Err(NotEnough)` otherwise — caller should either wait
    /// or switch to another key.
    pub async fn try_consume(&self, tokens: f64) -> Result<(), ConsumeError> {
        if tokens <= 0.0 {
            return Ok(());
        }
        if self.refill_per_sec <= 0.0 {
            // No refill — permanent bucket; consume from the initial capacity.
        }
        let mut toks = self.tokens.lock().await;
        let mut last = self.last_refill.lock().await;
        self.refill_locked(&mut toks, &mut last);
        if *toks >= tokens {
            *toks -= tokens;
            return Ok(());
        }
        Err(ConsumeError::NotEnough)
    }

    /// Consume `tokens`, waiting at most until `deadline`. Returns `Ok(())`
    /// on success, `Err(ConsumeError::NotEnough)` if the deadline elapsed
    /// before enough tokens accumulated.
    pub async fn consume_with_timeout(
        &self,
        tokens: f64,
        deadline: Instant,
    ) -> Result<(), ConsumeError> {
        if tokens <= 0.0 {
            return Ok(());
        }
        if self.refill_per_sec <= 0.0 {
            // No refill: try once and bail.
            return self.try_consume(tokens).await;
        }
        loop {
            // Fast path: refill + check.
            {
                let mut toks = self.tokens.lock().await;
                let mut last = self.last_refill.lock().await;
                self.refill_locked(&mut toks, &mut last);
                if *toks >= tokens {
                    *toks -= tokens;
                    return Ok(());
                }
                let need = tokens - *toks;
                drop((toks, last));
                // How long until we'd have enough if nobody else consumes?
                let wait_secs = need / self.refill_per_sec;
                let now = Instant::now();
                let target = now + Duration::from_secs_f64(wait_secs);
                if target > deadline {
                    // Would exceed the deadline — don't bother sleeping the
                    // full interval; sleep until deadline then do a final
                    // check (another consumer may have left tokens).
                    if deadline > now {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    }
                    // Final attempt.
                    let mut toks = self.tokens.lock().await;
                    let mut last = self.last_refill.lock().await;
                    self.refill_locked(&mut toks, &mut last);
                    if *toks >= tokens {
                        *toks -= tokens;
                        return Ok(());
                    }
                    return Err(ConsumeError::NotEnough);
                }
                // Sleep until we expect enough, then loop to re-check under lock.
                tokio::time::sleep_until(tokio::time::Instant::from_std(target)).await;
            }
        }
    }

    /// Attempt to consume `tokens` tokens, waiting until enough are available.
    /// Retained for backwards compatibility with tests; production code should
    /// prefer `try_consume` / `consume_with_timeout` to avoid unbounded waits.
    #[deprecated = "使用 try_consume 或 consume_with_timeout 以支持超时换 key"]
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

/// Build a fresh `KeyStateHandle` for `key_str` with a full token bucket and
/// `Active` state. Shared by `KeyPool::new` (startup init) and `add_key`
/// (runtime key injection) so both paths construct identical handles.
fn make_handle(config: &Config, key_str: &str) -> KeyStateHandle {
    let bucket = TokenBucket::new(
        config.rate_limiter.burst as u32,
        if config.rate_limiter.qps_per_key > 0.0 {
            config.rate_limiter.qps_per_key
        } else {
            1.0
        },
    );
    KeyStateHandle {
        inner: Arc::new(RwLock::new(InnerKeyState {
            key: Arc::new(key_str.to_string()),
            state: KeyState::Active,
            weight: 1,
            fail_count: 0,
            last_success: None,
            bucket,
            circuit_breaker_failures: std::collections::HashMap::new(),
        })),
        in_flight: Arc::new(std::sync::atomic::AtomicU32::new(0)),
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
            let handle = make_handle(config, key_str);
            inner.insert(key_str.clone(), handle);
        }
        Self {
            inner: RwLock::new(inner),
            config: config.clone(),
            metrics,
            pub_notifier: tx,
        }
    }

    /// Acquires the best key via a score-based selection. After acquiring,
    /// the caller **must** call `handle.dec_in_flight()` when the request
    /// is complete (success or failure).
    ///
    /// The score favours keys with more available tokens and fewer in-flight
    /// requests. Multi-key ties are broken randomly. Keys with `weight` < 1
    /// (penalised via `mark_failure`) see a proportional score penalty.
    ///
    /// Returns `None` when no active key exists.
    pub async fn acquire(&self) -> Option<KeyStateHandle> {
        let handles: Vec<KeyStateHandle> = {
            let read_guard = self.inner.read().await;
            read_guard.values().cloned().collect()
        };

        // ── Collect active keys with computed scores ────────────────────
        struct Candidate {
            handle: KeyStateHandle,
            score: f64,
        }
        const IN_FLIGHT_PENALTY: f64 = 0.15;
        let mut candidates: Vec<Candidate> = Vec::new();
        for handle in handles {
            let guard = handle.inner.read().await;
            let is_active = matches!(guard.state, KeyState::Active);
            if !is_active {
                continue;
            }
            // Compute score inside the read guard to minimise lock contention.
            let available = guard.bucket.available_tokens().await;
            let capacity = guard.bucket.capacity;
            let in_flight = handle.in_flight.load(std::sync::atomic::Ordering::Relaxed) as f64;
            let w = guard.weight as f64;
            drop(guard);

            let token_ratio = if capacity > 0.0 {
                available / capacity
            } else {
                0.0
            };
            let score = token_ratio - in_flight * IN_FLIGHT_PENALTY * (1.0 / w.max(1.0));
            candidates.push(Candidate { handle, score });
        }
        if candidates.is_empty() {
            return None;
        }

        // ── Top-1 with random tie-break ─────────────────────────────────
        candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
        let best_score = candidates[0].score;
        let top_n = candidates
            .iter()
            .take_while(|c| c.score == best_score)
            .count();
        let idx = if top_n > 1 {
            fastrand::usize(..top_n)
        } else {
            0
        };
        let chosen = &candidates[idx].handle;
        chosen.inc_in_flight();
        Some(chosen.clone())
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

    /// Force a key into `Disabled` state (used by the health checker when a
    /// key's recovery probe errors `max_consecutive_failures` times in a row).
    /// Mirrors the `Disabled` transition inside `mark_failure`.
    pub async fn disable_key(&self, key_str: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            let ready_at = Instant::now() + Duration::from_secs(self.config.cooldown.max_seconds);
            handle.set_state(KeyState::Disabled { ready_at }).await;
            warn!(
                key = %mask_key(key_str),
                "Key force-disabled by health probe (consecutive probe failures)"
            );
            let _ = self.pub_notifier.send(handle);
        }
        self.refresh_metrics().await;
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
            handle.reset_all_circuit_breakers().await; // Also reset circuit breaker on success
            info!(key = %mask_key(key_str), "Marked key as good (active)");
            let _ = self.pub_notifier.send(handle);
        }
        self.refresh_metrics().await;
    }

    /// Mark a key as bad (got 429/403) and put it into cooldown.
    ///
    /// If the upstream asks for a Retry-After longer than `cooldown.max_seconds`,
    /// we cap it to `max_seconds` to prevent a single key from draining the
    /// pool for an unbounded time. This is an intentional policy choice.
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

    /// Increment circuit breaker failure count for a key and model (for empty-shell/504 responses).
    /// Returns true if the threshold is exceeded.
    pub async fn increment_circuit_breaker(&self, key_str: &str, model: &str, threshold: u32) -> bool {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            handle.increment_circuit_breaker(model, threshold).await
        } else {
            false
        }
    }

    /// Check if a key has exceeded the circuit breaker threshold for a model.
    pub async fn check_circuit_breaker(&self, key_str: &str, model: &str, threshold: u32) -> bool {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            handle.check_circuit_breaker(model, threshold).await
        } else {
            false
        }
    }

    /// Inject a key into the pool at runtime. Returns `true` if the key was
    /// newly added, `false` if it already existed (its existing handle is left
    /// untouched). The new key starts `Active` with a full token bucket and is
    /// immediately eligible for `acquire()`.
    pub async fn add_key(&self, key_str: String) -> bool {
        if key_str.len() < 8 || key_str.trim().is_empty() {
            warn!(key = %mask_key(&key_str), "Refusing to add key: too short or blank");
            return false;
        }
        let mut write_guard = self.inner.write().await;
        if write_guard.contains_key(&key_str) {
            return false;
        }
        let handle = make_handle(&self.config, &key_str);
        write_guard.insert(key_str.clone(), handle);
        info!(key = %mask_key(&key_str), "Key added to pool at runtime");
        drop(write_guard);
        self.refresh_metrics().await;
        true
    }

    /// Remove a key from the pool at runtime. Returns `true` if the key was
    /// present (and removed), `false` otherwise. In-flight requests holding the
    /// key continue to completion; `acquire()` will simply never pick it again.
    pub async fn remove_key(&self, key_str: &str) -> bool {
        let mut write_guard = self.inner.write().await;
        let removed = write_guard.remove(key_str).is_some();
        drop(write_guard);
        if removed {
            info!(key = %mask_key(key_str), "Key removed from pool at runtime");
            self.refresh_metrics().await;
        }
        removed
    }

    /// Snapshot every key's current state for the admin `/admin/keys`
    /// endpoint / observability. `Instant`-based fields are surfaced as
    /// human-friendly derived values (state name, seconds-until-ready,
    /// seconds-since-last-success) because `Instant` isn't serializable.
    pub async fn snapshot(&self) -> Vec<serde_json::Value> {
        let handles: Vec<(String, KeyStateHandle)> = {
            let read_guard = self.inner.read().await;
            read_guard
                .iter()
                .map(|(k, h)| (k.clone(), h.clone()))
                .collect()
        };
        let now = Instant::now();
        let mut out = Vec::with_capacity(handles.len());
        for (key_str, handle) in handles {
            let guard = handle.inner.read().await;
            let (state_name, ready_in_secs) = match guard.state {
                KeyState::Active => ("active".to_string(), None),
                KeyState::Cooling { ready_at } => (
                    "cooling".to_string(),
                    Some(ready_at.saturating_duration_since(now).as_secs()),
                ),
                KeyState::Disabled { ready_at } => (
                    "disabled".to_string(),
                    Some(ready_at.saturating_duration_since(now).as_secs()),
                ),
            };
            let last_success_secs = guard.last_success.map(|t| {
                // `Instant` has no wall-clock; expose elapsed-since as a proxy.
                now.saturating_duration_since(t).as_secs()
            });
            out.push(serde_json::json!({
                "key": mask_key(&key_str),
                "state": state_name,
                "ready_in_secs": ready_in_secs,
                "weight": guard.weight,
                "fail_count": guard.fail_count,
                "in_flight": handle.in_flight.load(std::sync::atomic::Ordering::Relaxed),
                "available_tokens": guard.bucket.available_tokens().await,
                "last_success_secs_ago": last_success_secs,
                "circuit_breaker_failures": guard.circuit_breaker_failures,
            }));
        }
        out
    }

    /// Background task to move cooling keys back to active when ready.
    /// `stop` is a Notify that signals graceful shutdown.
    ///
    /// This task currently reuses `health_check.interval_seconds` as its sweep
    /// period. If the two intervals need to diverge in the future, add a
    /// dedicated `cooldown.interval_seconds` field.
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
            // Snapshot handles first to avoid holding the outer read lock across
            // multiple inner-lock awaits.
            let handles: Vec<(String, KeyStateHandle)> = {
                let read_guard = self.inner.read().await;
                read_guard
                    .iter()
                    .map(|(k, h)| (k.clone(), h.clone()))
                    .collect()
            };
            let mut to_update = Vec::new();
            for (key_str, handle) in handles {
                let expired = {
                    let guard = handle.inner.read().await;
                    match guard.state {
                        KeyState::Cooling { ready_at } if now >= ready_at => true,
                        KeyState::Disabled { ready_at } if now >= ready_at => true,
                        _ => false,
                    }
                };
                if expired {
                    to_update.push((key_str, handle));
                }
            }
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
        AuthConfig, CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
        default_empty_shell_pre_clamp_max_tokens,
    };

    fn build_pool(keys: &[&str]) -> KeyPool {
        let cfg = Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://example.test".to_string(),
            keys: keys.iter().map(|s| s.to_string()).collect(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: 100.0,
                burst: 100,
                token_wait_ms: 0,
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
            request_timeout_seconds: 0,
            request_timeout_streaming_seconds: 0,
            retry: crate::config::RetryConfig::default(),
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

#[cfg(test)]
mod mask_key_tests {
    use super::mask_key;

    #[test]
    fn short_key_returns_masked_placeholder() {
        assert_eq!(mask_key("abc"), "***");
        assert_eq!(mask_key("abcdefg"), "***");
    }

    #[test]
    fn ascii_key_reveals_first_and_last_four() {
        assert_eq!(mask_key("nvapi-1234567890abcd"), "nvap***abcd");
    }

    #[test]
    fn unicode_key_does_not_panic() {
        // 8+ Unicode characters: should not panic and should produce some mask.
        let key = "ключ-для-теста-12345";
        let masked = mask_key(key);
        assert!(masked.contains("***"), "masked={}", masked);
    }
}

#[cfg(test)]
mod score_selection_tests {
    use super::*;
    use crate::config::{
        AuthConfig, CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
        default_empty_shell_pre_clamp_max_tokens,
    };

    fn build_pool_with_keys(keys: &[&str], qps: f64, burst: u64) -> KeyPool {
        let cfg = Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://example.test".to_string(),
            keys: keys.iter().map(|s| s.to_string()).collect(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: qps,
                burst,
                token_wait_ms: 0,
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
            request_timeout_seconds: 0,
            request_timeout_streaming_seconds: 0,
            retry: crate::config::RetryConfig::default(),
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
        };
        let metrics = Arc::new(Metrics::new());
        KeyPool::new(&cfg, metrics)
    }

    #[tokio::test]
    async fn acquire_returns_some_when_active_key_exists() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 100);
        let h = pool.acquire().await;
        assert!(h.is_some(), "active key must be acquired");
        if let Some(h) = h {
            h.dec_in_flight();
        }
    }

    #[tokio::test]
    async fn acquire_returns_none_when_all_keys_disabled() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 100);
        pool.mark_failure("k1", 1).await; // single failure with threshold 1 → disable
        let h = pool.acquire().await;
        assert!(h.is_none(), "no active key should yield None");
    }

    #[tokio::test]
    async fn acquire_increments_in_flight_and_release_decrements() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 100);
        let handle = pool.acquire().await.expect("key acquired");
        let handle_ref = &handle;
        let pool_ref = &pool;
        let h_inner = pool_ref.inner.read().await.get("k1").cloned().unwrap();
        assert_eq!(h_inner.in_flight(), 1, "acquire must inc in_flight");
        handle_ref.dec_in_flight();
        assert_eq!(h_inner.in_flight(), 0, "dec_in_flight must restore");
    }

    #[tokio::test]
    async fn try_consume_token_succeeds_on_full_bucket() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        let handle = pool.acquire().await.expect("key acquired");
        // bucket starts full (burst=10), one consume must succeed immediately.
        let got = handle.try_consume_token(50).await;
        assert!(got, "token must be consumed from a full bucket");
        handle.dec_in_flight();
    }

    #[tokio::test]
    async fn try_consume_token_times_out_when_bucket_empty_and_zero_qps() {
        // qps=0 means no refill — after draining, no token can arrive.
        let pool = build_pool_with_keys(&["k1"], 0.0, 1);
        let handle = pool.acquire().await.expect("key acquired");
        // Drain the one token.
        assert!(handle.try_consume_token(0).await, "first consume ok");
        // Second consume with short timeout must return false (no refill).
        let got = handle.try_consume_token(10).await;
        assert!(!got, "must time out when bucket stays empty");
        handle.dec_in_flight();
    }

    /// Score-based selection: a key with a fuller bucket must be picked over
    /// one whose bucket has been drained. We verify by draining k2 first,
    /// then calling `acquire` repeatedly and asserting k1 is chosen.
    #[tokio::test]
    async fn acquire_prefers_key_with_more_tokens() {
        let pool = build_pool_with_keys(&["k1", "k2"], 0.0, 10);
        // Drain k2 to zero so its score is strictly lower than k1's.
        let h2 = pool
            .inner
            .read()
            .await
            .get("k2")
            .cloned()
            .expect("k2 exists");
        for _ in 0..10 {
            // Directly consume tokens without going through inc_in_flight.
            let got = h2.try_consume_token(0).await;
            assert!(got, "drain k2");
        }
        // Now k2 has 0 tokens, k1 has 10. acquire must pick k1.
        let h = pool.acquire().await.expect("active key exists");
        let k = h.key().await;
        assert_eq!(
            k, "k1",
            "score-based selection must prefer k1 (fuller bucket)"
        );
        h.dec_in_flight();
    }

    /// `mark_bad` must transition a key from Active to Cooling, so subsequent
    /// `acquire()` skips it. This mirrors the 403 "Authorization failed"
    /// cooldown path implemented in `process_upstream_attempt`.
    #[tokio::test]
    async fn mark_bad_puts_key_into_cooling() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        pool.mark_bad("k1", None).await;

        let h = pool.inner.read().await.get("k1").cloned().unwrap();
        let g = h.inner.read().await;
        assert!(
            matches!(g.state, KeyState::Cooling { .. }),
            "mark_bad must put key into Cooling state"
        );
        drop(g);

        // With only one key now in Cooling, acquire must return None.
        let acquire = pool.acquire().await;
        assert!(acquire.is_none(), "Cooling key must not be acquireable");
    }
}

#[cfg(test)]
mod runtime_key_tests {
    use super::*;
    use crate::config::{
        AuthConfig, CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
        default_empty_shell_pre_clamp_max_tokens,
    };

    fn pool(keys: &[&str]) -> KeyPool {
        let cfg = Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://example.test".to_string(),
            keys: keys.iter().map(|s| s.to_string()).collect(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: 100.0,
                burst: 100,
                token_wait_ms: 0,
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
            request_timeout_seconds: 0,
            request_timeout_streaming_seconds: 0,
            retry: crate::config::RetryConfig::default(),
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
        };
        KeyPool::new(&cfg, Arc::new(Metrics::new()))
    }

    #[tokio::test]
    async fn add_then_remove_key_changes_pool() {
        let pool = pool(&["k1"]);
        assert!(pool.add_key("k2-new-key".to_string()).await);
        // Duplicate add returns false.
        assert!(!pool.add_key("k2-new-key".to_string()).await);
        // Too-short key is rejected.
        assert!(!pool.add_key("short".to_string()).await);

        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 2);
        let maskeds: Vec<&str> = snap
            .iter()
            .map(|v| v.get("key").and_then(|x| x.as_str()).unwrap())
            .collect();
        assert!(maskeds.iter().any(|m| m.contains("k2")));

        assert!(pool.remove_key("k2-new-key").await);
        assert!(!pool.remove_key("k2-new-key").await); // already gone
        assert_eq!(pool.snapshot().await.len(), 1);
    }

    #[tokio::test]
    async fn snapshot_reports_state_and_in_flight() {
        let pool = pool(&["k1", "k2"]);
        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 2);
        for v in &snap {
            assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("active"));
            assert_eq!(v.get("in_flight").and_then(|x| x.as_u64()), Some(0));
            assert!(v.get("available_tokens").is_some());
        }
    }
}
