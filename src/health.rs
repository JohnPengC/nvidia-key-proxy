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
    /// 该 key 的*连续探测错误*计数（网络/探测失败，而非非 2xx）。当某 key 的
    /// 计数达到 `max_failures` 时，健康检查器强制禁用它（见 `recover_one`），
    /// 而不是永远等待冷却计时器。成功探测后重置为 0。
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

    /// 启动时跑一次，检查所有 key。
    pub async fn run_startup(&self) {
        info!("正在对所有 key 执行启动健康检查");
        if let Err(e) = self.check_all_keys().await {
            error!("启动健康检查失败：{}", e);
        }
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
                    warn!(key = %mask_key(key_str), "连续探测失败达阈值；强制禁用 key");
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
