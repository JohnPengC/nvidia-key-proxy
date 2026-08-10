use reqwest::StatusCode;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// 该 key 的*连续探测错误*计数（网络/探测失败，而非非 2xx）。当某 key 的
    /// 计数达到 `max_failures` 时，健康检查器强制禁用它（见 `recover_one`），
    /// 而不是永远等待冷却计时器。成功探测后重置为 0。
    probe_failures: std::sync::Mutex<std::collections::HashMap<String, usize>>,
    client: reqwest::Client,
    /// 启动健康检查已完成的读门。`run_startup` 在退出前（无论成功与否）把它
    /// 置 true；`/health` 路由在它为 false 时返回 503、为 true 后返回 204。这让
    /// 负载均衡器/探针能在代理尚未对池做完一次初始清点时把它摘掉，而不是在
    /// 启动期把流量打到尚未验证的 key 上。
    ready: Arc<AtomicBool>,
}

impl HealthChecker {
    pub fn new(
        key_pool: Arc<KeyPool>,
        base_url: String,
        interval_seconds: u64,
        timeout_seconds: u64,
        max_consecutive_failures: usize,
        ready: Arc<AtomicBool>,
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
            ready,
        }
    }

    /// `/health` 路由查询此值：启动健康检查完成前为 false。
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// 启动时跑一次，检查所有 key。
    pub async fn run_startup(&self) {
        info!("正在对所有 key 执行启动健康检查");
        if let Err(e) = self.check_all_keys().await {
            error!("启动健康检查失败：{}", e);
        }
        // 无论本轮成功还是出错，启动清点都已结束 —— 让 `/health` 放行。出错时
        // 不阻塞就绪：`check_all_keys` 自己的错误已记日志、每个 key 的状态保持
        // 不变（见其分支），池子仍可正常服务，只是没拿到启动期的探测信号。
        self.ready.store(true, Ordering::Relaxed);
    }

    /// 周期性地只检查坏（冷却中）的 key，看它们是否已恢复。
    /// `stop` 用于优雅关停这个后台循环。
    pub async fn run_bad_key_checker(&self, stop: Arc<tokio::sync::Notify>) {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.check_bad_keys().await {
                        error!("坏 key 检查轮失败：{}", e);
                    }
                }
                _ = stop.notified() => {
                    info!("坏 key 检查器正在停止");
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
                    // 不要惩罚 key：NVIDIA 的 /v1/models 对一个完全有效的 key
                    // 也可能返回 403/401（账户/区域门控）。这里冷却会把池子抽干。
                    // 只记日志、不动 key 的状态。
                    warn!(key = %mask_key(&key_str), status = %status, "启动探测非 2xx；保持 key 状态不变");
                }
                Err(e) => {
                    warn!(key = %mask_key(&key_str), error = %e, "启动探测出错；保持 key 状态不变");
                }
            }
        }
        Ok(())
    }

    /// 只检查当前处于 Cooling 状态的 key。未到冷却时间的 key 跳过——
    /// 我们只探测 ready_at 已过的 key，以尊重冷却计时、避免过早探测。
    async fn check_bad_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bad_keys = self.key_pool.get_bad_keys().await;
        // 也探测 Disabled 的 key：那些是反复认证失败后被标记为坏的
        //（见 Proxy::handle_request + KeyPool::mark_failure）。此处成功则
        // 重新激活；非成功则保持 Disabled。
        let disabled_keys = self.key_pool.get_disabled_keys().await;
        if bad_keys.is_empty() && disabled_keys.is_empty() {
            return Ok(());
        }
        info!(
            bad = bad_keys.len(),
            disabled = disabled_keys.len(),
            "恢复探测轮"
        );
        for key_str in &bad_keys {
            self.recover_one(key_str, false).await;
        }
        for key_str in &disabled_keys {
            self.recover_one(key_str, true).await;
        }
        Ok(())
    }

    /// 探测单个坏/禁用 key 并据此反应。
    ///
    /// * 成功 → 恢复 key（`mark_good` / `recover_disabled`）并重置其连续探测错误计数。
    /// * 非 2xx → 不算定性（NVIDIA 的 `/v1/models` 对一个完全有效的 key 也可能
    ///   403/401），交给冷却计时器；错误计数不动。
    ///   **注**：一个因 429（不带 Retry-After）被短冷却的 key 也走这条路径
    ///   （见 `Proxy::process_upstream_attempt` 的不重试分支）：探测期若上游仍在
    ///   限流，`/v1/models` 也回 429 → 非 2xx → 不重置也不递增。但它不依赖探测
    ///   回来——`cool_down_task` 会在其 `ready_at`（`cooldown.base_seconds`）到期
    ///   后无探测 `reactivate_after_cooldown` 把它转回 active。探测一旦成功则提前
    ///   `mark_good`。两条自愈路径都不会卡死。
    /// * 探测*出错*（网络/超时）→ 递增连续错误计数。一旦达到 `max_failures`
    ///   就强制禁用 key，避免一个长期不可达的 key 永远在冷却里循环。
    ///   `max_failures` 为 0 关闭此行为。
    async fn recover_one(&self, key_str: &str, was_disabled: bool) {
        match self.probe_key(key_str).await {
            Ok(status) if status.is_success() => {
                self.reset_probe_failures(key_str);
                if was_disabled {
                    info!(key = %mask_key(key_str), "禁用 key 经探测恢复；重新激活");
                    self.key_pool.recover_disabled(key_str).await;
                } else {
                    info!(key = %mask_key(key_str), "坏 key 经探测恢复；标记为好");
                    self.key_pool.mark_good(key_str).await;
                }
            }
            Ok(status) => {
                warn!(key = %mask_key(key_str), status = %status, "恢复探测非 2xx；交给冷却计时器");
            }
            Err(e) => {
                let count = {
                    let mut map = self.probe_failures.lock().unwrap();
                    let c = map.entry(key_str.to_string()).or_insert(0);
                    *c += 1;
                    *c
                };
                warn!(key = %mask_key(key_str), error = %e, probe_errors = count, max_failures = self.max_failures, "恢复探测出错");
                if self.max_failures > 0 && count >= self.max_failures {
                    // S7：对**已 Disabled** 的 key，不要再调 `disable_key`——它
                    // 进来时就是 Disabled，反复探测失败时若每次都 `disable_key`
                    // 并 reset 计数，旧版本会把 ready_at 无脑后推一个 max_seconds、
                    // cool_down_task 的到期安全网永不触发，key 永久卡 Disabled、
                    // 池子自我收缩。`disable_key` 现已保留更长 ready_at，故这里对
                    // 已禁用的 key 只 reset 计数（避免下一轮立即又触阈值）、保持其
                    // Disabled 状态与既有 ready_at 即可。
                    if was_disabled {
                        warn!(
                            key = %mask_key(key_str),
                            "连续探测失败已阈值，但 key 已禁用；保持其既有 ready_at（不重推）"
                        );
                    } else {
                        warn!(key = %mask_key(key_str), "连续探测失败达阈值；强制禁用 key");
                        self.key_pool.disable_key(key_str).await;
                    }
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
