use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::metrics::Metrics;

/// 对 API key 做掩码以便展示：前 4 个字符 + "***" + 后 4 个字符。
/// 若 key 少于 8 个字符，则返回 "***"。
pub fn mask_key(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() < 8 {
        return "***".to_string();
    }
    let first: String = chars.iter().take(4).collect();
    let last: String = chars.iter().skip(chars.len() - 4).collect();
    format!("{}***{}", first, last)
}

/// 与 `Config::validate` 一致的 placeholder/fake key 判定。运行时 admin
/// 加 key 的门槛不能只查长度——否则启动门禁拦下的 dummy key 可以经
/// admin API 绕回来。
pub fn looks_like_placeholder_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("fake")
        || k.contains("dummy")
        || k.contains("test")
        || k.contains("example")
        || k.contains("placeholder")
}

/// Key 的唯一标识（字符串放在 Arc 里以避免克隆）。
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KeyId(pub Arc<String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum KeyState {
    Active,
    Cooling {
        ready_at: Instant,
    },
    /// 多次鉴权失败后被禁用。`ready_at` 是一个兜底过期时间：
    /// 如果健康探测一直没能恢复该 key（探测端点本身不可用，或探测持续失败），
    /// `cool_down_task` 会在 `ready_at` 到期后重新激活该 key —— 这样在探测
    /// 全部失败时，池也不会永久收缩到零。
    Disabled {
        ready_at: Instant,
    },
    /// 健康探测连续多次（`health_check.max_consecutive_failures`）拿到 401 —
    /// 确诊 key 已失效/过期。与 `Disabled` 不同：**永久**排除——不参与冷却
    /// 计时、不再被探测、也不会被 `cool_down_task` 自动恢复；只能由用户从
    /// 配置中删除后重启、或经 admin API 显式加回来。`acquire` 只选 `Active`，
    /// 天然不会选中它。被置为该状态时会打 warn 日志（含掩码 key），提示
    /// 用户从 `config.toml` 清理。
    Expired,
}

/// 每个 key 存储的内部状态。
#[derive(Debug)]
pub struct InnerKeyState {
    pub key: Arc<String>,
    pub state: KeyState,
    pub weight: u32, // 打分选择时的惩罚倍数
    pub fail_count: u32,
    pub last_success: Option<Instant>,
    pub bucket: TokenBucket,
    /// 熔断器使用的连续空壳/504 响应计数。
    /// 若 circuit_breaker_per_model 为 true，按 model 维度分别计数；
    /// 否则使用 "global" 这个 key。
    pub circuit_breaker_failures: std::collections::HashMap<String, u32>,
}

/// 全局单调自增的句柄 ID 分配器。不能复用 Arc 指针作身份：运行时删/加 key
/// 后内存可能被复用，新 key 的句柄会撞上 `tried_key_hashes` 里的陈旧指针
/// 哈希而被误排除（ABA）。
static NEXT_HANDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// 指向某个 key 状态的公开句柄（可低成本克隆）。
#[derive(Debug, Clone)]
pub struct KeyStateHandle {
    pub(crate) inner: Arc<RwLock<InnerKeyState>>,
    /// 稳定身份标识；见 `identity()`。
    id: u64,
    /// 当前持有该 key 的请求数（已获取但尚未归还）。放在 `inner` 之外，这样
    /// 我们可以同步更新它，而不必去锁那个 async RwLock。
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU32>,
}

impl KeyStateHandle {
    /// 以 owned String 返回该 key。
    ///
    /// 走 async 的 `read()` 路径而非 `blocking_read()`——后者在 tokio
    /// runtime worker 线程里调用会 panic（正是我们此前踩过的那个 panic）。
    /// tokio 的 `RwLock` 永不中毒，所以这里 `read().await` 既安全又正确。
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

    /// 当前连续的鉴权/403 失败计数。
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

    /// 单次写锁内完成 `mark_failure` 的全部状态变更：递增失败计数、（若未到
    /// 阈值且非 Disabled）权重减半、在达到阈值时转 `Disabled`。返回
    /// `(new_fail_count, already_disabled, disabled_now)`，让调用方在锁外
    /// 发布 notifier / 做日志，避免在热路径上串行 4 次 async 锁获取。
    ///
    /// `disable_after` 即 `Disabled { ready_at }` 的相对时长；由 `KeyPool`
    /// 从 `config.cooldown.max_seconds` 传入，使本方法不依赖配置句柄。
    pub async fn record_failure_and_maybe_disable(
        &self,
        threshold: u32,
        disable_after: Duration,
    ) -> (u32, bool, bool) {
        let mut guard = self.inner.write().await;
        guard.fail_count += 1;
        let fail_count = guard.fail_count;
        if matches!(guard.state, KeyState::Disabled { .. }) {
            return (fail_count, true, false);
        }
        // 即便没到阈值也把权重减半，这样重试序列里下次 acquire 不太可能再
        // 选中同一个坏 key。`w.max(1)/2` 保持与旧 `add_weight` 路径一致的
        // 整数除法语义（w=1 时 delta=0，权重不下穿到 0）。
        let w = guard.weight as i32;
        let delta = -(w.max(1) / 2);
        let nw = w + delta;
        guard.weight = if nw > 0 { nw as u32 } else { 1 };
        if fail_count >= threshold {
            guard.state = KeyState::Disabled {
                ready_at: Instant::now() + disable_after,
            };
            return (fail_count, false, true);
        }
        (fail_count, false, false)
    }

    pub async fn reset_fail(&self) {
        let mut guard = self.inner.write().await;
        guard.fail_count = 0;
    }

    pub async fn set_last_success(&self) {
        let mut guard = self.inner.write().await;
        guard.last_success = Some(Instant::now());
        // 成功时重置熔断器
        guard.circuit_breaker_failures.clear();
    }

    /// 增加某 model 的熔断失败计数。超过阈值时返回 true。
    pub async fn increment_circuit_breaker(&self, model: &str, threshold: u32) -> bool {
        let mut guard = self.inner.write().await;
        let count = guard
            .circuit_breaker_failures
            .entry(model.to_string())
            .or_insert(0);
        *count += 1;
        *count >= threshold
    }

    /// 检查某 model 是否已超过熔断阈值。
    pub async fn check_circuit_breaker(&self, model: &str, threshold: u32) -> bool {
        let guard = self.inner.read().await;
        guard
            .circuit_breaker_failures
            .get(model)
            .is_some_and(|c| *c >= threshold)
    }

    /// 重置某 model 的熔断失败计数。
    pub async fn reset_circuit_breaker(&self, model: &str) {
        let mut guard = self.inner.write().await;
        guard.circuit_breaker_failures.remove(model);
    }

    /// 重置所有 model 的熔断失败计数。
    pub async fn reset_all_circuit_breakers(&self) {
        let mut guard = self.inner.write().await;
        guard.circuit_breaker_failures.clear();
    }

    // ── 在途计数 ────────────────────────────────────────────────────

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

    /// 该句柄的稳定身份标识：创建时分配的单调自增 ID。两个句柄指向同一个
    /// key 槽位（克隆而来）时相等；与内存地址无关，因此不受运行时删/加 key
    /// 造成的 Arc 指针复用（ABA）影响。
    pub fn identity(&self) -> u64 {
        self.id
    }

    /// 在截止时间前尝试从该 key 的桶里消费一个令牌。
    /// 在截止时间前消费成功返回 `true`；截止时间到仍未成功返回 `false`。
    /// 调用方在收到 `false` 时应丢弃该 key 句柄并去获取另一个。
    pub async fn try_consume_token(&self, timeout_ms: u64) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        let guard = self.inner.read().await;
        guard
            .bucket
            .consume_with_timeout(1.0, deadline)
            .await
            .is_ok()
    }

    /// 该 key 桶中当前可用令牌数（用于打分计算）。
    pub async fn available_tokens(&self) -> f64 {
        let guard = self.inner.read().await;
        guard.bucket.available_tokens().await
    }
}

/// 简单的令牌桶实现。不可 Clone —— 克隆会静默重置 `tokens` 和
/// `last_refill`，从而绕过限流。每个 key 在其生命周期内只持有一个桶。
pub struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    /// 令牌数与上次补充时间总是一起读改，合并进同一把锁，消费路径锁获取减半。
    state: Mutex<(f64, Instant)>,
    /// 打分热路径用的"近似"可用令牌快照——以 `f64::to_bits()` 存进一个
    /// `AtomicU64`。每次我们在 `tokens` Mutex 锁内观察/改动桶的真实令牌数
    /// 之后，顺手 `store` 一次此值；`approx_available_tokens()` 只做一次
    /// relaxed atomic load、**不取那两把 async 锁**。
    ///
    /// 动机：`acquire_excluding` 在每个上游请求都会对 N 个候选 key 各走一次
    /// `available_tokens()`，后者内部要先取桶的 async 锁（一次 await 调度点）。
    /// 在大池（admin 批量灌上千 key）下，每请求 N×2 把锁 + N 个跨线程 await
    /// 是选择阶段的瓶颈。打分只需要"哪个 key 更满"的粗排，不需要到实时精度，
    /// 因此给打分一条 relaxed-load 旁路。
    ///
    /// 近似值的陈旧性是有界的：它与真实值偏差最多"自上次某条 holder 改动桶
    /// 起累积的 refill"，且恒夹在 `[0, capacity]`；单调反映"谁更满"，对打分
    /// 足矣。`try_consume` / `consume_with_timeout` 仍走精确锁路径并原子地扣，
    /// 限流正确性不依赖此近似值。
    approx_tokens: std::sync::atomic::AtomicU64,
}

/// 非阻塞消费尝试的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeError {
    /// 当前桶中令牌数少于请求数量。
    NotEnough,
}

impl TokenBucket {
    fn store_approx(&self, v: f64) {
        // 夹到容量上限——refill 路径在锁内已夹过，但守住并发写下的数值边界，
        // 也避免 NaN 的 bits（NaN 在 load 端会还原成 NaN，破坏排序）。
        let clamped = if v.is_finite() {
            v.min(self.capacity).max(0.0)
        } else {
            self.capacity
        };
        self.approx_tokens
            .store(clamped.to_bits(), std::sync::atomic::Ordering::Relaxed);
    }

    /// 近似可用令牌数——纯 relaxed atomic load，不取任何 async 锁。供打分
    /// 热路径使用。值为"最近一次某条持有者观察桶时所留下的快照"，故可能略微
    /// 滞后于真实 refill 累积值，但恒在 `[0, capacity]`。详细说明见结构体注释。
    pub fn approx_available_tokens(&self) -> f64 {
        let bits = self
            .approx_tokens
            .load(std::sync::atomic::Ordering::Relaxed);
        f64::from_bits(bits)
    }

    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        let cap = capacity as f64;
        Self {
            capacity: cap,
            refill_per_sec,
            state: Mutex::new((cap, Instant::now())), // 起始即满
            approx_tokens: std::sync::atomic::AtomicU64::new(cap.to_bits()),
        }
    }

    /// 按经过时间比例补充令牌。调用时必须同时持有两把锁。
    fn refill_locked(&self, toks: &mut f64, last: &mut Instant) {
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed > 0.0 && self.refill_per_sec > 0.0 {
            *toks = (*toks + elapsed * self.refill_per_sec).min(self.capacity);
            *last = now;
        }
    }

    /// 当前可用令牌的非阻塞快照（在刷新补充之后）。若补充速率为零（桶保持
    /// 全满）则返回 `capacity`。
    pub async fn available_tokens(&self) -> f64 {
        if self.refill_per_sec <= 0.0 {
            return self.capacity;
        }
        let mut st = self.state.lock().await;
        { let (toks, last) = &mut *st; self.refill_locked(toks, last); }
        self.store_approx(st.0);
        st.0
    }

    /// 尝试**不阻塞**地消费 `tokens` 个令牌。桶中令牌足够时返回 `Ok(())`
    /// （随刷新补充一起原子地扣除令牌）；否则返回 `Err(NotEnough)` —— 调用方
    /// 应当等待或切换到另一个 key。
    pub async fn try_consume(&self, tokens: f64) -> Result<(), ConsumeError> {
        if tokens <= 0.0 {
            return Ok(());
        }
        let mut st = self.state.lock().await;
        // refill_locked 在 refill_per_sec == 0 时是空操作（内部有判断），
        // 所以一个永久（不补充）的桶在这里就是从初始容量里往下扣 —— 效果与
        // 显式提前返回的 consume() 一样，只是不走那条特判分支。
        { let (toks, last) = &mut *st; self.refill_locked(toks, last); }
        if st.0 >= tokens {
            st.0 -= tokens;
            self.store_approx(st.0);
            return Ok(());
        }
        Err(ConsumeError::NotEnough)
    }

    /// 消费 `tokens` 个令牌，最多等待到 `deadline`。成功返回 `Ok(())`，
    /// 截止时间前仍未攒够令牌则返回 `Err(ConsumeError::NotEnough)`。
    pub async fn consume_with_timeout(
        &self,
        tokens: f64,
        deadline: Instant,
    ) -> Result<(), ConsumeError> {
        if tokens <= 0.0 {
            return Ok(());
        }
        if self.refill_per_sec <= 0.0 {
            // 不补充：尝试一次就放弃。
            return self.try_consume(tokens).await;
        }
        loop {
            // 快速路径：刷新补充 + 检查。
            {
                let mut st = self.state.lock().await;
                { let (toks, last) = &mut *st; self.refill_locked(toks, last); }
                if st.0 >= tokens {
                    st.0 -= tokens;
                    self.store_approx(st.0);
                    return Ok(());
                }
                let need = tokens - st.0;
                drop(st);
                // 假如没人再消费，还要等多久才能凑够？
                let wait_secs = need / self.refill_per_sec;
                let now = Instant::now();
                let target = now + Duration::from_secs_f64(wait_secs);
                if target > deadline {
                    // 会超过截止时间 —— 不必睡满整个间隔，直接睡到截止时间
                    // 再做一次最终检查（可能有别的消费者留下了令牌）。
                    if deadline > now {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    }
                    // 最终尝试。
                    let mut st = self.state.lock().await;
                    { let (toks, last) = &mut *st; self.refill_locked(toks, last); }
                    if st.0 >= tokens {
                        st.0 -= tokens;
                        self.store_approx(st.0);
                        return Ok(());
                    }
                    return Err(ConsumeError::NotEnough);
                }
                // 睡到预计凑够的时刻，然后在锁下重新检查再循环。
                tokio::time::sleep_until(tokio::time::Instant::from_std(target)).await;
            }
        }
    }

    /// 尝试消费 `tokens` 个令牌，等待直到令牌足够。仅为兼容旧测试而保留；
    /// 生产代码应优先使用 `try_consume` / `consume_with_timeout`，避免无限等待。
    #[deprecated = "使用 try_consume 或 consume_with_timeout 以支持超时换 key"]
    pub async fn consume(&self, tokens: f64) -> Result<(), ()> {
        if tokens <= 0.0 {
            return Ok(());
        }
        if self.refill_per_sec <= 0.0 {
            // 没配置补充 —— 绝不永久阻塞。桶初始即满且无从补充，直接返回成功。
            return Ok(());
        }
        loop {
            let mut st = self.state.lock().await;
            let now = Instant::now();
            let elapsed = now.duration_since(st.1).as_secs_f64();
            let added = elapsed * self.refill_per_sec;
            if added > 0.0 {
                st.0 = (st.0 + added).min(self.capacity);
                st.1 = now;
                self.store_approx(st.0);
            }
            if st.0 >= tokens {
                st.0 -= tokens;
                self.store_approx(st.0);
                return Ok(());
            }
            // 令牌不足；释放锁，睡到预计凑够的时刻。
            let need = tokens - st.0;
            drop(st);
            let wait_secs = need / self.refill_per_sec;
            if wait_secs > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(wait_secs)).await;
            }
            // 再循环一次
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

/// 为 `key_str` 构造一个全新的 `KeyStateHandle`，带一个满的令牌桶和
/// `Active` 状态。`KeyPool::new`（启动初始化）和 `add_key`（运行时注入 key）
/// 都共用它，两条路径构造出的句柄完全一致。
fn make_handle(config: &Config, key_str: &str) -> KeyStateHandle {
    // `qps_per_key = 0` 的语义是**关闭 per-key 限流**（运维常见直觉：0 即不限制），
    // 不是偷偷换成 1 QPS（这是此前版本的 footgun —— 配 `qps_per_key=0` "想不限流"
    // 的部署实际被夹成 1 QPS，高并发下静默节流）。我们用"桶容量极大 + 补充速率为
    // 正无穷"显式实现无限通行：`refill_locked` 里 `(toks + elapsed * INF).min(cap)`
    // 把 toks 恒拉满 cap，任何 `try_consume(1)` 必得 Ok。`burst` 在此模式下一并
    // 抬为 `u32::MAX` 容量以免起跑前桶就空着。finite 且 >0 走原 token 桶。
    let (burst_cap, refill) = if config.rate_limiter.qps_per_key <= 0.0 {
        (u32::MAX, f64::INFINITY)
    } else {
        (
            if config.rate_limiter.burst == 0 {
                1u32
            } else {
                config.rate_limiter.burst as u32
            },
            config.rate_limiter.qps_per_key,
        )
    };
    let bucket = TokenBucket::new(burst_cap, refill);
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
        id: NEXT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        in_flight: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    }
}

pub struct KeyPool {
    /// key 字符串到句柄的映射
    pub(crate) inner: RwLock<HashMap<String, KeyStateHandle>>,
    config: Config,
    metrics: Arc<Metrics>,
    /// 用于广播状态变化（可选）。
    pub_notifier: tokio::sync::broadcast::Sender<KeyStateHandle>,
    /// `/health` 用来判断"池子是否还有活跃 key"的同步缓存。由
    /// `refresh_metrics` 每个扫描周期写入（S6）。`None` 表示未挂载（如
    /// 单元测试构造的 KeyPool），`/health` 路径不会查它。
    active_keys_cached: Option<std::sync::Arc<std::sync::atomic::AtomicI64>>,
}

impl KeyPool {
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(16);
        let mut inner = HashMap::new();
        // 初始化 keys
        for key_str in &config.keys {
            let handle = make_handle(config, key_str);
            inner.insert(key_str.clone(), handle);
        }
        Self {
            inner: RwLock::new(inner),
            config: config.clone(),
            metrics,
            pub_notifier: tx,
            active_keys_cached: None,
        }
    }

    /// 挂载 `/health` 用的活跃 key 计数缓存。`refresh_metrics` 会在每个扫描
    /// 周期把它刷成最新 active 计数。AppState 在构造后立即调用一次。
    pub fn set_active_keys_cache(&mut self, cache: std::sync::Arc<std::sync::atomic::AtomicI64>) {
        self.active_keys_cached = Some(cache);
    }

    /// 通过打分选择获取最佳 key。获取后，调用方在请求完成时（无论成功还是
    /// 失败）**必须**调用 `handle.dec_in_flight()`。
    ///
    /// 打分优先选择可用令牌更多、在途请求更少的 key。多个 key 同分时随机打破
    /// 平局。`weight` < 1 的 key（经 `mark_failure` 惩罚过）会按比例扣分。
    ///
    /// 没有活跃 key 时返回 `None`。
    pub async fn acquire(&self) -> Option<KeyStateHandle> {
        self.acquire_excluding(&[]).await
    }

    /// 与 `acquire` 类似，但绝不会返回指针哈希落在 `exclude_hashes` 中的 key。
    /// 重试/令牌饥饿循环用它避免在本请求中再次选中刚刚失败的 key：否则刚失败的
    /// key 在下一轮循环里可能被再次选中（`mark_failure` 只把它的权重减半，
    /// 而非归零），白白浪费一个槽位和一次令牌消费尝试。没有活跃且未被排除的
    /// key 时返回 `None`。
    pub async fn acquire_excluding(&self, exclude_hashes: &[u64]) -> Option<KeyStateHandle> {
        let handles: Vec<KeyStateHandle> = {
            let read_guard = self.inner.read().await;
            read_guard.values().cloned().collect()
        };

        // ── 收集活跃 key 并计算打分 ────────────────────────────────────
        struct Candidate {
            handle: KeyStateHandle,
            score: f64,
        }
        const IN_FLIGHT_PENALTY: f64 = 0.15;
        let mut candidates: Vec<Candidate> = Vec::new();
        for handle in handles {
            // 跳过调用方显式排除的 key（本请求已试过）。identity 是共享的
            // `inner` Arc 指针，在句柄克隆之间保持稳定。
            if !exclude_hashes.is_empty() && exclude_hashes.contains(&handle.identity()) {
                continue;
            }
            let guard = handle.inner.read().await;
            let is_active = matches!(guard.state, KeyState::Active);
            if !is_active {
                continue;
            }
            // 在读锁内计算打分，以尽量减少锁竞争。
            //
            // 打分用近似的可用令牌（`approx_available_tokens`，一次 relaxed
            // atomic load），**不**取 `TokenBucket` 内部那两把 async 锁。打分
            // 只需要"哪个 key 更满"的粗排，近似快照足矣；精确令牌消费路径
            //（`try_consume_token`）独立保证限流正确性。在大池下这把每请求的
            // N×2 把锁降到 N 次 relaxed load。
            let available = guard.bucket.approx_available_tokens();
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

        // ── 取最高分，同分随机打破 ─────────────────────────────────────
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

    /// 获取当前处于 Cooling 状态的所有 key（用于坏 key 恢复）。
    pub async fn get_bad_keys(&self) -> Vec<String> {
        // 先快照句柄，这样在逐个等待内部锁时不会持有外层锁。
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

    /// 获取当前处于 Disabled 状态的所有 key（用于健康探测恢复）。
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

    /// 原子地记录 `key_str` 的一次非限流上游鉴权/403 失败；若连续失败计数
    /// 超过配置阈值，则将该 key 切换到 `KeyState::Disabled`，直到健康探测恢复
    /// 它。若该 key 正是该次被禁用的则返回 `true`。
    /// 原子地记录 `key_str` 的一次非限流上游鉴权/403 失败；若连续失败计数
    /// 超过配置阈值，则将该 key 切换到 `KeyState::Disabled`，直到健康探测恢复
    /// 它。若该 key 正是该次被禁用的则返回 `true`。
    ///
    /// 这条路径在每一个上游响应（401/500/429 且无 Retry-After）上都会执行。
    /// 我们刻意**不**在这里调用 `refresh_metrics()`：那会在热路径上做一次
    /// O(N) 的全 key 状态清点（每个 key 每次失败都要一次 async 读锁）。Prometheus
    /// 的 key 状态 gauge 由 `cool_down_task` 周期性的 `refresh_metrics()`（每
    /// `health_check.interval_seconds` 一次）保持新鲜，所以 gauge 最多滞后一个
    /// 周期 —— 这是我们为不在每次错误响应上串行 N 次锁获取而做的有意权衡。
    /// `main.rs` 在启动时也会调用一次 `refresh_metrics()`。
    pub async fn mark_failure(&self, key_str: &str, threshold: u32) -> bool {
        let handle = {
            let read_guard = self.inner.read().await;
            read_guard.get(key_str).cloned()
        };
        let Some(handle) = handle else { return false };
        // 单次写锁内完成 increment + halve-weight + 按 threshold 转 Disabled。
        let disable_after = Duration::from_secs(self.config.cooldown.max_seconds);
        let (fail_count, already_disabled, disabled_now) = handle
            .record_failure_and_maybe_disable(threshold, disable_after)
            .await;
        if already_disabled {
            // 已禁用；无需再做什么（权重已是 1）。
            return true;
        }
        if disabled_now {
            let _ = self.pub_notifier.send(handle);
            warn!(
                key = %mask_key(key_str),
                fail_count,
                threshold,
                "连续 {} 次鉴权失败后禁用该 key；等待健康探测或过期恢复",
                fail_count
            );
            return true;
        }
        warn!(
            key = %mask_key(key_str),
            fail_count,
            threshold,
            "记录一次非限流鉴权失败（尚未禁用）",
        );
        false
    }

    /// 瞬态上游失败（500 `Failed to generate completions`、不带 Retry-After 的
    /// 503 容量耗尽）专用：**只降权、不冷却、不累加禁用计数**。
    ///
    /// 为什么不直接用 `mark_failure(threshold=u32::MAX)`：那条路径会累加
    /// `fail_count`，长时段瞬态暴雨里 u32 溢出（debug 下 panic / release 下
    /// wrap→ `fail_count >= threshold` 误判→ 把好 key 置 Disabled）。为什么
    /// 不用 `mark_bad`：那是 Cooling + `cool_down_task` 的 ready_at 安全网，
    /// 单 key 池里会让本次重试找不到任何 active key→ 502。瞬态失败希望的是
    /// 「multi-key 池优先换 key、单 key 池仍能续试」——降权重恰好满足：acquire
    /// 的加权随机按权重抽，单 key 时它的权重是唯一选项仍被选中；多 key 时
    /// 半权大幅降低再撞它的概率。`fail_count` 不动→ `mark_good`/`reactivate`
    /// 路径无需特殊清理。`circuit_breaker_failures` 也不动——那是 empty-shell /
    /// 504 的独立计数器，500/503 走 `mark_bad`/CB 另有路径。
    pub async fn mark_transient_failure(&self, key_str: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str).cloned() {
            drop(read_guard);
            // 在该 handle 的写锁内完成单次权重减半。状态不动（仍 Active）。
            {
                let mut guard = handle.inner.write().await;
                // 已 Disabled/Cooling 的 key 不再画蛇添足。
                if !matches!(guard.state, KeyState::Active) {
                    return;
                }
                let w = guard.weight as i32;
                let delta = -(w.max(1) / 2);
                let nw = w + delta;
                guard.weight = if nw > 0 { nw as u32 } else { 1 };
            }
            warn!(
                key = %mask_key(key_str),
                "瞬态上游失败；降权一半（不冷却、不计禁用阈值）",
            );
        }
    }

    /// 强制将一个 key 置于 `Disabled` 状态（当健康检查发现某 key 的恢复探测
    /// 连续 `max_consecutive_failures` 次报错时使用）。与 `mark_failure` 中的
    /// `Disabled` 转换逻辑一致。
    ///
    /// **S7**：若该 key 已是 Disabled，保留其**更长**的 ready_at 而非无条件盖
    /// `now + max_seconds`。否则一个持续不可达的 `/v1/models` 探针会让
    /// `recover_one` 每轮都把它推远一个 max_seconds、计数又清零、cool_down_task
    /// 的到期安全网永不触发，key 永久卡 Disabled、池子自我收缩。镜像 `mark_bad`
    /// 的"永不缩短"规则。
    pub async fn disable_key(&self, key_str: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            let base = self.config.cooldown.max_seconds;
            let new_ready_at = Instant::now() + Duration::from_secs(base);
            // 保留更长的既有 Disabled ready_at，避免反复探测失败时无脑后推。
            // 「取更长」的读-比较-写在同一把写锁内完成（并发 mark_bad/disable
            // 竞态下不会被较短的 ready_at 覆盖）。
            {
                let mut guard = handle.inner.write().await;
                let ready_at = match guard.state {
                    KeyState::Disabled { ready_at } if ready_at > new_ready_at => ready_at,
                    _ => new_ready_at,
                };
                guard.state = KeyState::Disabled { ready_at };
            }
            warn!(
                key = %mask_key(key_str),
                "健康探测强制禁用该 key（连续探测失败）"
            );
            let _ = self.pub_notifier.send(handle);
        }
        // gauge 刷新推迟到 cool_down_task（参见 mark_failure 的说明）。
    }

    /// 成功的健康探测后，重新激活一个此前被禁用的 key。
    pub async fn recover_disabled(&self, key_str: &str) {
        self.mark_good(key_str).await;
    }

    /// 把一个 key 永久标记为 `Expired`（健康探测连续 401 确诊，即 key 已
    /// 失效/过期）。key 存在且此前不是 `Expired` 时返回 `true`。
    ///
    /// 与 `disable_key` 不同：不设 `ready_at`、`cool_down_task` 永不恢复它、
    /// 健康探测也不再扫它——过期 key 没有"等一会儿可能就活"这回事。用户
    /// 应从 `config.toml` 删除对应 key；运行时若误标，可通过 admin API 移除
    /// 再 `add_key` 加回（`add_key` 对不存在的 key 才会新建 Active 句柄，
    /// 故需先 `remove_key`）。
    pub async fn mark_expired(&self, key_str: &str) -> bool {
        let handle = {
            let read_guard = self.inner.read().await;
            match read_guard.get(key_str) {
                Some(h) => h.clone(),
                None => return false,
            }
        };
        {
            let mut guard = handle.inner.write().await;
            if matches!(guard.state, KeyState::Expired) {
                return false;
            }
            guard.state = KeyState::Expired;
        }
        let _ = self.pub_notifier.send(handle);
        // Expired 是引导用户行动的运维信号，立刻刷 gauge 而不等下个扫描周期。
        self.refresh_metrics().await;
        true
    }

    /// 按状态统计 key 数量 —— 用于 Prometheus gauge。
    /// 返回 `(active, cooling, disabled, expired)`。
    pub async fn count_states(&self) -> (i64, i64, i64, i64) {
        let handles: Vec<KeyStateHandle> = {
            let read_guard = self.inner.read().await;
            read_guard.values().cloned().collect()
        };
        let mut active = 0i64;
        let mut cooling = 0i64;
        let mut disabled = 0i64;
        let mut expired = 0i64;
        for handle in handles {
            let guard = handle.inner.read().await;
            match guard.state {
                KeyState::Active => active += 1,
                KeyState::Cooling { .. } => cooling += 1,
                KeyState::Disabled { .. } => disabled += 1,
                KeyState::Expired => expired += 1,
            }
        }
        (active, cooling, disabled, expired)
    }

    /// 若已挂载 metrics 则刷新 Prometheus 的 key 状态 gauge。
    pub async fn refresh_metrics(&self) {
        let (a, c, d, e) = self.count_states().await;
        self.metrics.set_key_counts(a, c, d, e);
        // S6：同步刷新 `/health` 用的活跃 key 缓存，使 0-Active 时 `/health`
        // 回 503、LB 摘流。未挂载（单测）则跳过。
        if let Some(cache) = &self.active_keys_cached {
            cache.store(a, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// 将 key 标记为正常（请求成功）。所有复位在**一把写锁**内完成，
    /// 避免热路径上 4 次串行 async 写锁获取。
    pub async fn mark_good(&self, key_str: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            {
                let mut guard = handle.inner.write().await;
                guard.state = KeyState::Active;
                guard.weight = 1;
                guard.fail_count = 0;
                guard.circuit_breaker_failures.clear();
            }
            debug!(key = %mask_key(key_str), "将 key 标记为正常（active）");
            let _ = self.pub_notifier.send(handle);
        }
        // gauge 刷新推迟到 cool_down_task（参见 mark_failure 的说明）。
    }

    /// 空壳重试成功后的窄恢复：把 key 置 Active、重置 weight/fail，但**只清
    /// 本 model 的熔断计数**，不清其它 model 的。
    ///
    /// 之所以和 `mark_good` 分开：空壳路径在 `increment_circuit_breaker` 时只
    /// 累加**本 model** 的计数（见 `proxy.rs` 的 `cb_model`）；若恢复后用
    /// `mark_good` → `reset_all_circuit_breakers()`，会把**别的 model** 的计数
    /// 一并清掉，于是该 key 在 model A 上的 CB（默认阈值 3 的 per-(key,model)
    /// 断路器）会因为一次 model B 的空壳重试成功而被抹平、永远触不到阈值——
    /// 这是 S4 的根因。这里只抹本次重试对应的 model。
    pub async fn mark_good_for_empty_shell_retry(&self, key_str: &str, model: &str) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            {
                let mut guard = handle.inner.write().await;
                guard.state = KeyState::Active;
                guard.weight = 1;
                guard.fail_count = 0;
                guard.circuit_breaker_failures.remove(model); // 只清本 model 的熔断计数
            }
            debug!(
                key = %mask_key(key_str),
                model = %model,
                "空壳重试成功，窄恢复（只清本 model 的熔断计数）"
            );
            let _ = self.pub_notifier.send(handle);
        }
    }

    /// 将 key 标记为坏（收到 429/403）并进入冷却。
    ///
    /// 若上游要求的 Retry-After 长于 `cooldown.max_seconds`，我们把它截断到
    /// `max_seconds`，以避免单个 key 把池抽干到无上限的时间。这是有意的
    /// 策略选择。
    ///
    /// 若该 key 已在 Cooling，只有当新的 `ready_at` 会**延长**既有冷却时
    /// 才应用它 —— 绝不缩短。并发的 429 带着长 Retry-After，可能在数秒后
    /// 紧接着又来一个带更短（或没有）Retry-After；没有这条取最大值规则，
    /// 后到的调用会用更短的值覆盖既有的长冷却，违背上游的退避请求。非
    /// Cooling 的先前状态（Active / Disabled）则照旧无条件覆盖。
    pub async fn mark_bad(&self, key_str: &str, retry_after: Option<u64>) {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            let base = self.config.cooldown.base_seconds;
            let max = self.config.cooldown.max_seconds;
            let delay = retry_after.unwrap_or(base).min(max);
            let new_ready_at = Instant::now() + Duration::from_secs(delay);
            // 「永不缩短」的读-比较-写必须落在**同一把写锁**内：此前先读锁读
            // 旧 ready_at、放锁、再写锁写入，两个并发 mark_bad（429 带长
            // Retry-After 与 403 无 Retry-After 竞争）都可能读到同一个旧值，
            // 较短冷却被后写、违背"绝不缩短"不变式。
            let applied_delay = {
                let mut guard = handle.inner.write().await;
                let prev_ready_at = match guard.state {
                    KeyState::Cooling { ready_at } => Some(ready_at),
                    _ => None,
                };
                let ready_at = match prev_ready_at {
                    Some(prev) if prev > new_ready_at => prev,
                    _ => new_ready_at,
                };
                let applied_delay = ready_at
                    .checked_duration_since(Instant::now())
                    .map(|d| d.as_secs())
                    .filter(|&s| s > 0)
                    .unwrap_or(delay);
                // `filter(s > 0)` 守住亚秒边界：当我们保留了更长的 `prev`
                // ready_at 但只剩几百毫秒时，原始 `as_secs()` 会截断为 0，日志会
                // 误读成"冷却 0 秒"。回退到新的 `delay` 让日志反映一个正的冷却预算。
                guard.state = KeyState::Cooling { ready_at };
                let w = guard.weight as i32;
                let nw = w - w.max(1) / 2;
                guard.weight = if nw > 0 { nw as u32 } else { 1 };
                guard.fail_count += 1;
                applied_delay
            };
            warn!(
                key = %mask_key(key_str),
                delay = applied_delay,
                "将 key 标记为坏，冷却 {} 秒",
                applied_delay
            );
            let _ = self.pub_notifier.send(handle);
        }
        // gauge 刷新推迟到 cool_down_task（参见 mark_failure 的说明）。
    }

    /// 对某个 key 的限流桶消费一个令牌。供空壳重试路径在重试**前**调用，
    /// 以保证空壳重试不绕过 per-key QPS（S3）——此前 `handle_empty_shell_failure`
    /// 直接 `build_upstream_request(...).send()`，从不消费令牌，使一个被限流
    /// 的 key 能借空壳重试超出其 QPS 配额。返回 `false` 表示该 key 的桶空了、
    /// 在 `timeout_ms` 内未补上；调用方可据此不重试或换 key。
    pub async fn try_consume_token_for_key(&self, key_str: &str, timeout_ms: u64) -> bool {
        let handle = {
            let read_guard = self.inner.read().await;
            read_guard.get(key_str).cloned()
        };
        match handle {
            Some(h) => h.try_consume_token(timeout_ms).await,
            None => false,
        }
    }

    /// 增加某 key、某 model 的熔断失败计数（针对空壳/504 响应）。超过阈值
    /// 返回 true。
    pub async fn increment_circuit_breaker(
        &self,
        key_str: &str,
        model: &str,
        threshold: u32,
    ) -> bool {
        let read_guard = self.inner.read().await;
        if let Some(handle) = read_guard.get(key_str) {
            let handle = handle.clone();
            drop(read_guard);
            handle.increment_circuit_breaker(model, threshold).await
        } else {
            false
        }
    }

    /// 检查某 key 对某 model 是否已超过熔断阈值。
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

    /// 在运行时向池中注入一个 key。key 是新加入的返回 `true`，已存在则
    /// 返回 `false`（其既有句柄保持不动）。新 key 起始为 `Active` 且桶是满的，
    /// 立即可以被 `acquire()` 选中。
    pub async fn add_key(&self, key_str: String) -> bool {
        if key_str.len() < 8 || key_str.trim().is_empty() {
            warn!(key = %mask_key(&key_str), "拒绝添加 key：过短或为空");
            return false;
        }
        if looks_like_placeholder_key(&key_str) {
            warn!(key = %mask_key(&key_str), "拒绝添加 key：疑似 placeholder/fake");
            return false;
        }
        let mut write_guard = self.inner.write().await;
        if write_guard.contains_key(&key_str) {
            return false;
        }
        let handle = make_handle(&self.config, &key_str);
        write_guard.insert(key_str.clone(), handle);
        info!(key = %mask_key(&key_str), "运行时向池中添加 key");
        drop(write_guard);
        self.refresh_metrics().await;
        true
    }

    /// 一次性持有写锁批量添加 keys，然后只刷新一次 metrics。这是 admin
    /// POST `/admin/keys` 路径 —— 调用方可能一次粘贴成百上千个 key，而走
    /// 每次 `add_key`（每次都要获取写锁并跑一次 O(N) 状态扫描）会让它退化
    /// 成 O(N²) 的锁抖动，卡住代理的热路径。这里我们只持一次写锁做完所有
    /// 插入。
    ///
    /// 返回 `(added, already_present, invalid)`，这样 admin 调用方可以汇报
    /// 每个 key 的处置结果而无需再扫一遍池。`invalid` 存放过短或为空的 key
    /// （即 `add_key` 应用的同样门槛判定为无效的那些）。
    pub async fn add_keys_batch(
        &self,
        keys: Vec<String>,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut added = Vec::new();
        let mut already_present = Vec::new();
        let mut invalid = Vec::new();
        // 第一遍：在无锁状态下分区 —— 长度/空判都是纯计算，不碰共享状态。
        // 这样写锁只在 HashMap 插入时持有。
        let mut to_insert: Vec<String> = Vec::new();
        for k in keys {
            if k.len() < 8 || k.trim().is_empty() || looks_like_placeholder_key(&k) {
                invalid.push(k);
                continue;
            }
            to_insert.push(k);
        }
        if !to_insert.is_empty() {
            let mut write_guard = self.inner.write().await;
            for k in to_insert {
                if write_guard.contains_key(&k) {
                    already_present.push(k);
                } else {
                    let handle = make_handle(&self.config, &k);
                    write_guard.insert(k.clone(), handle);
                    info!(key = %mask_key(&k), "运行时向池中添加 key");
                    added.push(k);
                }
            }
            drop(write_guard);
            // 整批只刷新一次 —— 不论批量多大都很便宜。
            self.refresh_metrics().await;
        }
        (added, already_present, invalid)
    }

    /// 在运行时从池中移除一个 key。key 存在（并被移除）返回 `true`，否则
    /// 返回 `false`。持有该 key 的在途请求会继续完成；`acquire()` 只是再也不会
    /// 选中它。
    pub async fn remove_key(&self, key_str: &str) -> bool {
        let mut write_guard = self.inner.write().await;
        let removed = write_guard.remove(key_str).is_some();
        drop(write_guard);
        if removed {
            info!(key = %mask_key(key_str), "运行时从池中移除 key");
            self.refresh_metrics().await;
        }
        removed
    }

    /// 为 admin `/admin/keys` 端点 / 可观测性快照所有 key 的当前状态。
    /// `Instant` 字段以人类友好的派生值呈现（状态名、距就绪秒数、距上次
    /// 成功秒数），因为 `Instant` 无法序列化。
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
                KeyState::Expired => ("expired".to_string(), None),
            };
            let last_success_secs = guard.last_success.map(|t| {
                // `Instant` 没有墙上时钟；用"距今已过多久"作为替代呈现。
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

    /// 把冷却中的 key 在到期时重新转回 active 的后台任务。`stop` 是一个
    /// 用于优雅停机信号的 Notify。
    ///
    /// 扫描周期取 `cooldown.scan_interval_seconds`（默认 5s），使其与
    /// `health_check.interval_seconds`（默认 30s）解耦——冷却的 ready_at 已过
    /// 后最多多这一个扫描周期才被发现，独立于健康检查周期。该值为 0 时回退到
    /// 旧行为：用 `health_check.interval_seconds` 作扫描周期。
    pub async fn cool_down_task(self: Arc<Self>, stop: Arc<tokio::sync::Notify>) {
        let scan_secs = if self.config.cooldown.scan_interval_seconds > 0 {
            self.config.cooldown.scan_interval_seconds
        } else {
            self.config.health_check.interval_seconds
        };
        let mut interval = tokio::time::interval(Duration::from_secs(scan_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = stop.notified() => {
                    info!("冷却任务停止");
                    break;
                }
            }
            let now = Instant::now();
            // 先快照句柄，避免在多个内部锁 await 期间一直持有外层读锁。
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
                let was_disabled = self.reactivate_after_cooldown(&handle).await;
                if was_disabled {
                    info!(key = %mask_key(&key_str), "冷却到期重新激活被禁用的 key（探测可能正在失败）");
                } else {
                    info!(key = %mask_key(&key_str), "冷却结束，重新激活该 key（无探测）");
                }
            }
            self.refresh_metrics().await;
        }
    }

    /// 冷却到期重新激活：把一个 Cooling/Disabled 的 key 转回 `Active`，
    /// 重置打分/权重/失败计数，并**清掉过期的熔断计数**。由 `cool_down_task`
    /// 在某个 key 的 `ready_at` 已到（无探测）时调用。返回该 key 此前是否为
    /// Disabled（仅用于日志）。
    ///
    /// 拆成 `pub(crate)` 辅助函数是为了让这些不变式（状态 → Active、cb 计数
    /// 清空）可以脱离扫描任务的墙上时钟做单测。一个刚冷却完的 key 应当拿到
    /// 一张干净的熔断清白 —— 否则恢复后的第一个空壳/504 会立刻再次触发熔断
    /// 断路器，key 就会在 cool→shell→cool 之间永远振荡。
    ///
    /// **S5 重检**：`cool_down_task` 在读锁下快照"now >= ready_at"后**释放**
    /// 读锁、再去取每个 key 的写锁。在释放到取写锁之间，一个在途请求可能
    /// `mark_bad(K, Some(long_retry_after))` 把 K 的 ready_at 推得更远（按
    /// "永不缩短"规则）。若不重检，机械驱动的 reactivate 会无条件置 Active、
    /// 清计数，使 K 在仍应 honoring Retry-After 时被激活，刚被限流的请求立刻
    /// 再打回同 key、再 429。这里在写锁内再校验一次 state 仍是 Cooling/Disabled
    /// **且** ready_at 已过；否则什么都不做，把该 key 留在它的冷却里。
    pub(crate) async fn reactivate_after_cooldown(&self, handle: &KeyStateHandle) -> bool {
        let mut guard = handle.inner.write().await;
        let now = Instant::now();
        match guard.state {
            KeyState::Cooling { ready_at } if now >= ready_at => {}
            KeyState::Disabled { ready_at } if now >= ready_at => {}
            // 状态已被并发改动：已被提前恢复（health 探测 mark_good）、或
            // ready_at 被并发 mark_bad 推远。保持现状，不动。
            _ => return matches!(guard.state, KeyState::Disabled { .. }),
        }
        let was_disabled = matches!(guard.state, KeyState::Disabled { .. });
        guard.state = KeyState::Active;
        guard.weight = 1;
        guard.fail_count = 0;
        guard.circuit_breaker_failures.clear();
        was_disabled
    }
}

#[cfg(test)]
mod retry_state_machine_tests {
    use super::*;
    use crate::config::{
        default_empty_shell_pre_clamp_max_tokens, default_request_max_prompt_tokens, AuthConfig,
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
                token_wait_ms: 0,
            },
            cooldown: CooldownConfig {
                base_seconds: 1,
                max_seconds: 2,
                scan_interval_seconds: 5,
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
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
            request_body_max_bytes: crate::config::default_request_body_max_bytes(),
        };
        let metrics = Arc::new(Metrics::new());
        KeyPool::new(&cfg, metrics)
    }

    #[tokio::test]
    async fn mark_failure_below_threshold_keeps_active() {
        let pool = build_pool(&["k1", "k2"]);
        // 阈值 = 3；两次失败绝不能禁用该 key。
        assert!(!pool.mark_failure("k1", 3).await);
        assert!(!pool.mark_failure("k1", 3).await);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert_eq!(handle.state().await, KeyState::Active);
        assert_eq!(handle.fail_count().await, 2);
    }

    #[tokio::test]
    async fn record_failure_and_maybe_disable_halves_weight_floor_one() {
        // P2：合并写锁路径下，weight 不会减穿到 0，且重复 mark_failure（高阈值）
        // 反复减半最终稳定在 1。
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        // 先把权重抬到 8，模拟 acquire 加分路径的累积，便于观察减半。
        handle.add_weight(7).await;
        assert_eq!(handle.weight().await, 8);
        // 5 次失败、阈值 100 → 永不禁用，仅减半：8→4→2→1→1→1。
        for _ in 0..5 {
            let _ = pool.mark_failure("k1", 100).await;
        }
        assert_eq!(
            handle.weight().await,
            1,
            "weight must floor at 1 after repeated halving"
        );
        assert!(matches!(handle.state().await, KeyState::Active));
        assert_eq!(handle.fail_count().await, 5);
    }

    #[tokio::test]
    async fn record_failure_and_maybe_disable_already_disabled_is_idempotent() {
        // 已 Disabled 的 key 再次 mark_failure：计数仍递增，但权重/状态不动，
        // 且返回 true（语义 = “该次仍落在被禁用 key 上”）。
        let pool = build_pool(&["k1", "k2"]);
        for _ in 0..3 {
            pool.mark_failure("k1", 3).await;
        }
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert!(matches!(handle.state().await, KeyState::Disabled { .. }));
        assert_eq!(handle.fail_count().await, 3);
        // 再来一次：返回 true、fail_count++、状态不变。
        let again = pool.mark_failure("k1", 3).await;
        assert!(again, "already-disabled key must report true");
        assert_eq!(handle.fail_count().await, 4);
        assert!(matches!(handle.state().await, KeyState::Disabled { .. }));
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
        // Disabled 的 key 不会再被获取。
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
        // 即便未到禁用阈值，mark_failure 也必须减小该 key 的权重，这样
        // 随后的加权随机 acquire() 在同一个重试序列内不太可能再次选中它。
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        // 默认权重起始为 1 —— 为了让"减半"可见，先把权重提到一个已知更高值。
        handle.add_weight(7).await; // weight = 1 + 7 = 8
        assert_eq!(handle.weight().await, 8);
        let disabled = pool.mark_failure("k1", 100).await; // 阈值很高 → 不会禁用
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

    #[tokio::test]
    async fn mark_expired_permanently_excludes_key() {
        let pool = build_pool(&["k1", "k2"]);
        assert!(pool.mark_expired("k1").await);
        // 幂等：重复标记返回 false
        assert!(!pool.mark_expired("k1").await);
        // 未知 key 不影响
        assert!(!pool.mark_expired("nope").await);

        // acquire 永不选中 Expired key
        for _ in 0..20 {
            let h = pool.acquire().await.expect("k2 still active");
            assert_eq!(h.key().await, "k2");
        }

        // cool_down 恢复路径对它无效：reactivate_after_cooldown 返回 false
        // 且状态保持 Expired。
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        assert!(!pool.reactivate_after_cooldown(&handle).await);
        assert!(matches!(handle.state().await, KeyState::Expired));

        // 状态计数：1 active + 1 expired
        let (a, c, d, e) = pool.count_states().await;
        assert_eq!((a, c, d, e), (1, 0, 0, 1));

        // snapshot 呈现 "expired"
        let snap = pool.snapshot().await;
        let states: Vec<&str> = snap
            .iter()
            .map(|v| v["state"].as_str().unwrap())
            .collect();
        assert!(states.contains(&"expired"));
        assert!(states.contains(&"active"));
    }

    #[tokio::test]
    async fn mark_transient_failure_halves_weight_and_keeps_active() {
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        handle.add_weight(7).await; // weight = 1 + 7 = 8
        assert_eq!(handle.weight().await, 8);
        pool.mark_transient_failure("k1").await;
        assert_eq!(handle.weight().await, 4, "瞬态失败应把权重减半（8 → 4）");
        assert_eq!(
            handle.state().await,
            KeyState::Active,
            "状态应保持 Active（不冷却）"
        );
        assert_eq!(
            handle.fail_count().await,
            0,
            "瞬态失败不应累加 fail_count（不计禁用阈值）"
        );
    }

    #[tokio::test]
    async fn mark_transient_failure_repeated_never_disables_and_saturates_at_min_weight() {
        // 模拟长时段瞬态暴雨：连续 mark_transient_failure 不该把 key 变 Disabled，
        // 也不该让权重下穿到 0（floor 1，保证单 key 池仍能续试）。
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        for _ in 0..40 {
            pool.mark_transient_failure("k1").await;
        }
        assert!(
            matches!(handle.state().await, KeyState::Active),
            "反复瞬态失败不得置 Disabled"
        );
        assert!(
            handle.weight().await >= 1,
            "权重不得下穿 1（保证单 key 池续试）"
        );
        assert_eq!(handle.fail_count().await, 0, "fail_count 仍为 0");
    }

    #[tokio::test]
    async fn mark_transient_failure_no_ops_on_non_active() {
        // 已 Disabled/Cooling 的 key 调用此方法应 no-op，不死减权重/不复活。
        let pool = build_pool(&["k1"]);
        let handle = pool.inner.read().await.get("k1").cloned().unwrap();
        // 先把它置 Disabled（3 次鉴权失败）。
        for _ in 0..3 {
            pool.mark_failure("k1", 3).await;
        }
        let w_before = handle.weight().await;
        pool.mark_transient_failure("k1").await;
        assert_eq!(
            handle.weight().await,
            w_before,
            "Disabled key 上调用瞬态失败为 no-op"
        );
        assert!(matches!(handle.state().await, KeyState::Disabled { .. }));
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
        // 8 个以上的 Unicode 字符：不应 panic 且应产生某种掩码。
        let key = "ключ-для-теста-12345";
        let masked = mask_key(key);
        assert!(masked.contains("***"), "masked={}", masked);
    }
}

#[cfg(test)]
mod score_selection_tests {
    use super::*;
    use crate::config::{
        default_empty_shell_pre_clamp_max_tokens, default_request_max_prompt_tokens, AuthConfig,
        CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
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
                scan_interval_seconds: 5,
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
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
            request_body_max_bytes: crate::config::default_request_body_max_bytes(),
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
        // 桶初始即满（burst=10），一次消费必须立刻成功。
        let got = handle.try_consume_token(50).await;
        assert!(got, "token must be consumed from a full bucket");
        handle.dec_in_flight();
    }

    #[tokio::test]
    async fn try_consume_token_succeeds_when_qps_zero_means_no_limit() {
        // M4：qps=0 的语义是**关闭 per-key 限流**（不再偷偷换成 1 QPS）。
        // 一个有限桶（burst=N）即使被抽干后，因为这桶的 refill 率被设成正无穷、
        // 容量被抬到 u32::MAX，几乎"瞬间"补满，故**连续**消费必须永远 Ok。
        let pool = build_pool_with_keys(&["k1"], 0.0, 1);
        let handle = pool.acquire().await.expect("key acquired");
        // 连续抽干 100 次；零超时；应次次成功——证明 qps=0 ≠ 节流。
        for i in 0..100 {
            assert!(
                handle.try_consume_token(0).await,
                "consume #{} must succeed when qps=0 means unlimited",
                i + 1
            );
        }
        handle.dec_in_flight();
    }

    /// 近似可用令牌（`approx_available_tokens`）必须在每次成功消费后立刻下降，
    /// 且不必取 `TokenBucket` 内部那两把 async 锁（一次 relaxed atomic load 即可）。
    /// 这是 P1 给打分热路径开的旁路；此测试锁定其不变式：① 初始 == capacity（`new`
    /// store 了 cap 的 bits）；② 消费两次后严格下降；③ 与精确 `available_tokens()`
    /// 在同一读锁快照下一致（近似值正是每个精确路径在锁内 store 的那个值）。
    /// 用有限 qps（10 QPS）以便桶容量 / 消费可观察地下降；qps=0 现在表示无限、
    /// 桶被构造成正无穷补充，因而无法观察"消费后下降"。
    #[tokio::test]
    async fn approx_tokens_track_consumption_without_async_locks() {
        let pool = build_pool_with_keys(&["k1"], 10.0, 10);
        let handle = pool.acquire().await.expect("key acquired");
        let h_inner = pool.inner.read().await.get("k1").cloned().unwrap();
        // 近似初始 == capacity（new 里 store 了 cap 的 bits）。
        let approx0 = {
            let g = h_inner.inner.read().await;
            g.bucket.approx_available_tokens()
        };
        assert!(
            (approx0 - 10.0).abs() < 1e-9,
            "approx starts at capacity: {}",
            approx0
        );

        // 消费两个令牌（try_consume_token 硬编码消费 1.0；循环两次）。
        assert!(handle.try_consume_token(0).await, "consume 1");
        assert!(handle.try_consume_token(0).await, "consume 2");
        let approx1 = {
            let g = h_inner.inner.read().await;
            g.bucket.approx_available_tokens()
        };
        assert!(
            approx1 < approx0,
            "approx must strictly decrease after consumption: {} -> {}",
            approx0,
            approx1
        );
        // 近似值必须落在合理区间内：消费了 2 个令牌，桶 1/s 补充，耗时 < 0.1s 间
        // 不会补超过 0.1，所以近似应 < 9（充分宽松，只锁单调性 + 数量级）。
        assert!(
            approx1 < 9.0 && approx1 > 6.0,
            "approx should be ~8 after consuming 2 of 10: got {}",
            approx1
        );
        // 精确 `available_tokens()`（会做同步补充 + store 近似）在本次读取后应与
        // 重新 load 的近似一致。
        let h_inner2 = pool.inner.read().await.get("k1").cloned().unwrap();
        let exact = {
            let g = h_inner2.inner.read().await;
            g.bucket.available_tokens().await
        };
        let approx_after_exact = {
            let g = h_inner2.inner.read().await;
            g.bucket.approx_available_tokens()
        };
        assert!(
            (exact - approx_after_exact).abs() < 1e-9,
            "approx must equal last store(newest exact after available_tokens): exact={}, approx={}",
            exact, approx_after_exact
        );
        handle.dec_in_flight();
    }

    /// token 越多的桶打分越高：先抽干 k2，再 `acquire` 应选中 k1。
    ///
    /// 用有限 qps（10/s，burst=10）构桶，这样消费是真正扣 token 的——抽干
    /// k2 后它的 `token_ratio` 严格低于 k1，打分胜负来自真实 token 差，而非
    /// qps=0 无限桶（`{u32::MAX, f64::INFINITY}`）下「cap 与 cap-10」那种
    /// ~1e-9 的浮点尾差。
    #[tokio::test]
    async fn acquire_prefers_key_with_more_tokens() {
        let pool = build_pool_with_keys(&["k1", "k2"], 10.0, 10);
        // 把 k2 抽干到 0，使其 `token_ratio` 严格低于 k1（k1 桶满）。
        let h2 = pool
            .inner
            .read()
            .await
            .get("k2")
            .cloned()
            .expect("k2 exists");
        for _ in 0..10 {
            // 不走 inc_in_flight，直接消费令牌。
            let got = h2.try_consume_token(0).await;
            assert!(got, "drain k2");
        }
        // 现在 k2 桶空、k1 桶满（10 个）。acquire 必须选 k1。
        let h = pool.acquire().await.expect("active key exists");
        let k = h.key().await;
        assert_eq!(
            k, "k1",
            "score-based selection must prefer k1 (fuller bucket)"
        );
        h.dec_in_flight();
    }

    /// `mark_bad` 必须把一个 key 从 Active 切换到 Cooling，使随后的
    /// `acquire()` 跳过它。这与 `process_upstream_attempt` 中实现的 403
    /// "Authorization failed" 冷却路径对应。
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

        // 现在唯一的 key 处于 Cooling，acquire 必须返回 None。
        let acquire = pool.acquire().await;
        assert!(acquire.is_none(), "Cooling key must not be acquireable");
    }

    /// 回归：一个更短的后续 `mark_bad` 不得缩短既有 Cooling key 的
    /// `ready_at`。没有取最大值规则时，第一个长 Retry-After 之后紧接着
    /// 来的第二个并发 429（带短 Retry-After 或 `None` → base）会覆盖掉
    /// 那条长冷却，让上游在仍被限流时提前再次猛打该 key。
    #[tokio::test]
    async fn mark_bad_preserves_longer_existing_cooldown() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        // 第一次标记：长 Retry-After，被截断到 max_seconds（2s）。
        pool.mark_bad("k1", Some(60)).await;
        let ready_at_1 = {
            let h = pool.inner.read().await.get("k1").cloned().unwrap();
            let g = h.inner.read().await;
            match g.state {
                KeyState::Cooling { ready_at } => ready_at,
                _ => panic!("must be Cooling after first mark_bad"),
            }
        };
        // 第二次标记：短冷却（None → base=1s），严格早于第一次设置的值。
        // 不得把 ready_at 提前。
        pool.mark_bad("k1", None).await;
        let ready_at_2 = {
            let h = pool.inner.read().await.get("k1").cloned().unwrap();
            let g = h.inner.read().await;
            match g.state {
                KeyState::Cooling { ready_at } => ready_at,
                _ => panic!("must remain Cooling after second mark_bad"),
            }
        };
        assert_eq!(
            ready_at_2, ready_at_1,
            "second mark_bad with a shorter cooldown must not shorten the existing one"
        );
    }

    /// 回归：冷却到期重新激活（`cool_down_task` 的自动路径，不是 `mark_good`）
    /// 必须清掉过期的按 model 维度的熔断计数。修复前，`cool_down_task` 直接
    /// 设置 `state = Active` 却不清 `circuit_breaker_failures`，于是被熔断器
    /// 冷却下来的 key 回来时计数仍停在阈值 → 恢复后的第一个空壳立刻再次触发
    /// 熔断器，key 就在 Cool→shell→Cool 之间永远振荡。
    #[tokio::test]
    async fn reactivate_after_cooldown_clears_circuit_breaker_counters() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        // 用一个很小的 Retry-After 冷却该 key，使 ready_at 在测试检查状态时已
        // 在过去；我们通过抽出来的 `reactivate_after_cooldown` 辅助函数
        // （`cool_down_task` 调用的同一个）手工驱动状态转换，以避开墙上时钟
        // 的扫描间隔。
        pool.mark_bad("k1", Some(0)).await;
        // 给该 key + model 放一些熔断失败（仿佛之前出现过空壳）。
        pool.increment_circuit_breaker("k1", "z-ai/glm-5.2", 3)
            .await;
        pool.increment_circuit_breaker("k1", "z-ai/glm-5.2", 3)
            .await;
        {
            let h = pool.inner.read().await.get("k1").cloned().unwrap();
            let g = h.inner.read().await;
            assert!(matches!(g.state, KeyState::Cooling { .. }));
            assert_eq!(
                g.circuit_breaker_failures.get("z-ai/glm-5.2"),
                Some(&2),
                "precondition: cb counter must be populated"
            );
        }
        // 显式驱动冷却到期的状态转换。
        let h = pool.inner.read().await.get("k1").cloned().unwrap();
        let was_disabled = pool.reactivate_after_cooldown(&h).await;
        assert!(
            !was_disabled,
            "a Cooling key reactivates as not-was-disabled"
        );

        let g = h.inner.read().await;
        assert!(matches!(g.state, KeyState::Active), "must be Active");
        assert!(
            g.circuit_breaker_failures.is_empty(),
            "cb counters must be cleared on cooldown-expiry reactivation, found: {:?}",
            g.circuit_breaker_failures
        );
    }

    /// S4 回归：空壳重试成功只清**本 model** 的熔断计数，不清其它 model 的。
    /// 此前该路径调的是 `mark_good`（→ `reset_all_circuit_breakers`），会把该
    /// key 在别的 model 上正在累积的 CB 计数一并抹平——于是 per-(key,model)
    /// 断路器对"间歇空壳但重试能恢复"的 key 永远触不到阈值。
    #[tokio::test]
    async fn mark_good_for_empty_shell_retry_clears_only_that_model_cb() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        // 两个 model 各累两次 CB（达到默认阈值 3 的 2/3）。
        pool.increment_circuit_breaker("k1", "z-ai/glm-5.2", 3)
            .await;
        pool.increment_circuit_breaker("k1", "z-ai/glm-5.2", 3)
            .await;
        pool.increment_circuit_breaker("k1", "meta/llama-3.1-405b", 3)
            .await;
        pool.increment_circuit_breaker("k1", "meta/llama-3.1-405b", 3)
            .await;
        {
            let h = pool.inner.read().await.get("k1").cloned().unwrap();
            let g = h.inner.read().await;
            assert_eq!(g.circuit_breaker_failures.get("z-ai/glm-5.2"), Some(&2));
            assert_eq!(
                g.circuit_breaker_failures.get("meta/llama-3.1-405b"),
                Some(&2)
            );
        }
        // 空壳重试成功——只清本 model（glm）。
        pool.mark_good_for_empty_shell_retry("k1", "z-ai/glm-5.2")
            .await;
        let h = pool.inner.read().await.get("k1").cloned().unwrap();
        let g = h.inner.read().await;
        assert!(
            !g.circuit_breaker_failures.contains_key("z-ai/glm-5.2"),
            "glm cb must be cleared; got {:?}",
            g.circuit_breaker_failures
        );
        assert_eq!(
            g.circuit_breaker_failures.get("meta/llama-3.1-405b"),
            Some(&2),
            "llama cb must be untouched; got {:?}",
            g.circuit_breaker_failures
        );
        assert!(matches!(g.state, KeyState::Active));
    }

    /// S5 回归：`reactivate_after_cooldown` 必须在写锁内重检 ready_at。`cool_down_task`
    /// 在读锁下快照"expired"后会释放读锁、再取写锁；该窗口里 `mark_bad` 可能
    /// 把 ready_at 推到未来。机械的无条件 reactivate 会把一个仍应冷却的 key 置
    /// Active、清计数，使其在仍应 honoring Retry-After 时被复用。这里手工模拟：
    /// 先让它 expired、拿 handle、再用长 Retry-After 推远 ready_at，然后调用
    /// reactivate 验证它**不**改状态。
    #[tokio::test]
    async fn reactivate_after_cooldown_respects_a_concurrent_reextension() {
        let pool = build_pool_with_keys(&["k1"], 100.0, 10);
        // 让它进入 Cooling 且 ready_at 在过去。
        pool.mark_bad("k1", Some(0)).await;
        // 拿到 handle（模拟 cool_down_task 快照阶段）。
        let h = pool.inner.read().await.get("k1").cloned().unwrap();
        {
            let g = h.inner.read().await;
            assert!(matches!(g.state, KeyState::Cooling { .. }));
        }
        // 模拟"取写锁窗口里 mark_bad 推远 ready_at"：用大 Retry-After（受
        // max_seconds=2 限幅，故推到 ~now+2s）。注意不要在读锁持有期调写操作。
        pool.mark_bad("k1", Some(10_000)).await;
        {
            let g = h.inner.read().await;
            assert!(
                matches!(g.state, KeyState::Cooling { ready_at } if ready_at > Instant::now() + Duration::from_secs(1)),
                "after re-mark_bad with long retry_after (clamped to max_seconds=2), ready_at must be ~2s out, got {:?}",
                g.state
            );
        }
        // 现在 reactivate 不该把它置 Active——ready_at 还没到。
        let was_disabled = pool.reactivate_after_cooldown(&h).await;
        assert!(
            !was_disabled,
            "must not report was_disabled for Cooling key"
        );
        let g = h.inner.read().await;
        assert!(
            matches!(g.state, KeyState::Cooling { .. }),
            "must remain Cooling (ready_at not yet reached), got {:?}",
            g.state
        );
    }
}

#[cfg(test)]
mod runtime_key_tests {
    use super::*;
    use crate::config::{
        default_empty_shell_pre_clamp_max_tokens, default_request_max_prompt_tokens, AuthConfig,
        CooldownConfig, HealthCheckConfig, ObservabilityConfig, RateLimiterConfig,
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
                scan_interval_seconds: 5,
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
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
            request_body_max_bytes: crate::config::default_request_body_max_bytes(),
        };
        KeyPool::new(&cfg, Arc::new(Metrics::new()))
    }

    #[tokio::test]
    async fn add_then_remove_key_changes_pool() {
        let pool = pool(&["k1"]);
        assert!(pool.add_key("k2-new-key".to_string()).await);
        // 重复添加返回 false。
        assert!(!pool.add_key("k2-new-key".to_string()).await);
        // 过短的 key 被拒绝。
        assert!(!pool.add_key("short".to_string()).await);

        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 2);
        let maskeds: Vec<&str> = snap
            .iter()
            .map(|v| v.get("key").and_then(|x| x.as_str()).unwrap())
            .collect();
        assert!(maskeds.iter().any(|m| m.contains("k2")));

        assert!(pool.remove_key("k2-new-key").await);
        assert!(!pool.remove_key("k2-new-key").await); // 已经不在池里
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
