use std::sync::OnceLock;

use prometheus::{register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram, Counter, CounterVec, Gauge, GaugeVec, Histogram};

fn request_count() -> &'static Counter {
    static REQUEST_COUNT: OnceLock<Counter> = OnceLock::new();
    REQUEST_COUNT.get_or_init(|| {
        register_counter!(
            "proxy_requests_total",
            "处理过的 HTTP 请求总数"
        )
        .unwrap()
    })
}

fn request_success() -> &'static Counter {
    static REQUEST_SUCCESS: OnceLock<Counter> = OnceLock::new();
    REQUEST_SUCCESS.get_or_init(|| {
        register_counter!(
            "proxy_requests_success_total",
            "成功请求数（状态 2xx）"
        )
        .unwrap()
    })
}

fn request_error() -> &'static Counter {
    static REQUEST_ERROR: OnceLock<Counter> = OnceLock::new();
    REQUEST_ERROR.get_or_init(|| {
        register_counter!(
            "proxy_requests_errors_total",
            "结果为错误的请求数（非 2xx）"
        )
        .unwrap()
    })
}

pub fn token_input() -> &'static Counter {
    static TOKEN_INPUT: OnceLock<Counter> = OnceLock::new();
    TOKEN_INPUT.get_or_init(|| {
        register_counter!(
            "proxy_tokens_input_total",
            "累计输入 token 数 (prompt_tokens)"
        )
        .unwrap()
    })
}

pub fn token_output() -> &'static Counter {
    static TOKEN_OUTPUT: OnceLock<Counter> = OnceLock::new();
    TOKEN_OUTPUT.get_or_init(|| {
        register_counter!(
            "proxy_tokens_output_total",
            "累计输出 token 数 (completion_tokens)"
        )
        .unwrap()
    })
}

pub fn token_total() -> &'static Counter {
    static TOKEN_TOTAL: OnceLock<Counter> = OnceLock::new();
    TOKEN_TOTAL.get_or_init(|| {
        register_counter!("proxy_tokens_total", "累计 token 数 (prompt + completion)").unwrap()
    })
}

fn request_latency() -> &'static Histogram {
    static REQUEST_LATENCY: OnceLock<Histogram> = OnceLock::new();
    REQUEST_LATENCY.get_or_init(|| {
        // 16 个桶：0.005s 起 ×2 上限 ≈ 0.005 * 2^15 ≈ 163.84s。此前是 12 个桶
        //（止于 10.24s），任何 ≥10s 的请求（流式生成、长 prompt 思考）都落入
        // `+Inf` 而失去 bisect 下游延迟的能力——长尾全部糊在 +Inf 桶里读不出
        // 分布。16 桶把 +Inf 推到 ~164s，覆盖典型 `request_timeout_seconds`
        //（60s）与流式默认超时，使 p99/p999 在真实分布上仍可读。`+Inf` 兜底
        // 仍存在， blijven 在 >164s 才丢线性度。
        register_histogram!(
            "proxy_request_duration_seconds",
            "HTTP 请求延迟（秒）",
            prometheus::exponential_buckets(0.005, 2.0, 16).unwrap()
        )
        .unwrap()
    })
}

fn retry_attempt_failure() -> &'static Counter {
    static RETRY_ATTEMPT_FAILURE: OnceLock<Counter> = OnceLock::new();
    RETRY_ATTEMPT_FAILURE.get_or_init(|| {
        register_counter!(
            "proxy_retry_attempt_failures_total",
            "经内部重试的 per-attempt 上游失败数（对 client 不可见）。与 proxy_requests_errors_total（计 client 可见失败）不同。"
        )
        .unwrap()
    })
}

fn key_active() -> &'static Gauge {
    static KEY_ACTIVE: OnceLock<Gauge> = OnceLock::new();
    KEY_ACTIVE.get_or_init(|| {
        register_gauge!(
            "proxy_key_active",
            "当前处于 active 状态的 API key 数量"
        )
        .unwrap()
    })
}

fn key_cooldown() -> &'static Gauge {
    static KEY_COOLDOWN: OnceLock<Gauge> = OnceLock::new();
    KEY_COOLDOWN.get_or_init(|| {
        register_gauge!(
            "proxy_key_cooldown",
            "当前处于冷却中的 API key 数量"
        )
        .unwrap()
    })
}

fn key_disabled() -> &'static Gauge {
    static KEY_DISABLED: OnceLock<Gauge> = OnceLock::new();
    KEY_DISABLED.get_or_init(|| {
        register_gauge!(
            "proxy_key_disabled",
            "当前被禁用的 API key 数量"
        )
        .unwrap()
    })
}

// ── token 饥饿切换与 403 即时冷却的新指标 ──

pub fn key_switch_token_starvation() -> &'static Counter {
    static KSW: OnceLock<Counter> = OnceLock::new();
    KSW.get_or_init(|| {
        register_counter!(
            "proxy_key_switch_token_starvation_total",
            "因首个 key 的 token 桶已空而把请求切到另一个 key 的次数"
        )
        .unwrap()
    })
}

pub fn key_403_auth_cooldown() -> &'static Counter {
    static K403: OnceLock<Counter> = OnceLock::new();
    K403.get_or_init(|| {
        register_counter!(
            "proxy_key_403_auth_cooldown_total",
            "403 Authorization failed 触发即时 key 冷却的次数"
        )
        .unwrap()
    })
}

pub fn circuit_breaker_triggered() -> &'static Counter {
    static CBT: OnceLock<Counter> = OnceLock::new();
    CBT.get_or_init(|| {
        register_counter!(
            "proxy_circuit_breaker_triggered_total",
            "因连续空壳/504 响应触发断路器的次数"
        )
        .unwrap()
    })
}

/// 空壳软失败检测总数，按 model 和 outcome 分。
pub fn empty_shell_total() -> &'static CounterVec {
    static EST: OnceLock<CounterVec> = OnceLock::new();
    EST.get_or_init(|| {
        register_counter_vec!(
            "proxy_empty_shell_total",
            "检测到的空壳软失败总数，按 model 和 outcome 分",
            &["model", "outcome"]
        )
        .unwrap()
    })
}

/// 被硬 prompt-token 上限（HTTP 413，`prompt_tokens_exceeds_ceiling`）在
/// 入口处直接拒绝的请求总数，按 model 和 outcome 分。与
/// `proxy_empty_shell_total`（计上游侧软失败）对称：这里计 **代理侧** 的
/// 前置拒绝。outcome 目前恒为 `"413"`；保留为 label，这样日后更细的拆分
///（例如区分硬上限与其他前门拒绝）可以无缝接入而无需改 metric 名。没有
/// 它，413 拒绝在 `proxy_requests_errors_total` 里和其它非 2xx 混在一起，
/// 运维无法判断上限是真在拦截流量还是闲置。
pub fn prompt_rejected_total() -> &'static CounterVec {
    static PRT: OnceLock<CounterVec> = OnceLock::new();
    PRT.get_or_init(|| {
        register_counter_vec!(
            "proxy_prompt_rejected_total",
            "被硬 prompt-token 上限（413）拒绝的请求总数，按 model 和 outcome 分",
            &["model", "outcome"]
        )
        .unwrap()
    })
}

/// 空壳重试尝试总数，按 model 分。
pub fn empty_shell_retry_attempts_total() -> &'static CounterVec {
    static ESR: OnceLock<CounterVec> = OnceLock::new();
    ESR.get_or_init(|| {
        register_counter_vec!(
            "proxy_empty_shell_retry_attempts_total",
            "空壳重试尝试总数，按 model 分",
            &["model"]
        )
        .unwrap()
    })
}

/// 某 model 当前并发数（在途请求）。
pub fn concurrency_current() -> &'static GaugeVec {
    static CC: OnceLock<GaugeVec> = OnceLock::new();
    CC.get_or_init(|| {
        register_gauge_vec!(
            "proxy_concurrency_current",
            "每个 model 当前并发的上游请求数",
            &["model"]
        )
        .unwrap()
    })
}

/// 并发获取尝试总数，按 model 和 result 分。
pub fn concurrency_acquire_total() -> &'static CounterVec {
    static CA: OnceLock<CounterVec> = OnceLock::new();
    CA.get_or_init(|| {
        register_counter_vec!(
            "proxy_concurrency_acquire_total",
            "并发信号量获取尝试总数，按 model 和 result 分",
            &["model", "result"]
        )
        .unwrap()
    })
}

/// 网络错误总数，按 model 和 error type 分。
pub fn network_errors_total() -> &'static CounterVec {
    static NET: OnceLock<CounterVec> = OnceLock::new();
    NET.get_or_init(|| {
        register_counter_vec!(
            "proxy_network_errors_total",
            "网络错误总数，按 model 和 error type 分",
            &["model", "error_type"]
        )
        .unwrap()
    })
}

#[derive(Clone)]
pub struct Metrics {
    _private: (),
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self { _private: () }
    }

    pub fn inc_request(&self) {
        request_count().inc();
    }

    pub fn inc_success(&self) {
        request_success().inc();
    }

    pub fn inc_retry_attempt_failure(&self) {
        retry_attempt_failure().inc();
    }

    pub fn inc_error(&self) {
        request_error().inc();
    }

    pub fn observe_latency(&self, seconds: f64) {
        request_latency().observe(seconds);
    }

    pub fn inc_key_switch_token_starvation(&self) {
        key_switch_token_starvation().inc();
    }

    pub fn inc_key_403_auth_cooldown(&self) {
        key_403_auth_cooldown().inc();
    }

    pub fn inc_circuit_breaker_triggered(&self) {
        circuit_breaker_triggered().inc();
    }

    pub fn inc_empty_shell_total(&self, model: &str, outcome: &str) {
        empty_shell_total().with_label_values(&[model, outcome]).inc();
    }

    /// 递增硬上限 413 拒绝计数。见 `prompt_rejected_total`。
    pub fn inc_prompt_rejected_total(&self, model: &str, outcome: &str) {
        prompt_rejected_total().with_label_values(&[model, outcome]).inc();
    }

    pub fn inc_empty_shell_retry_attempts_total(&self, model: &str) {
        empty_shell_retry_attempts_total().with_label_values(&[model]).inc();
    }

    pub fn set_concurrency_current(&self, model: &str, current: i64) {
        concurrency_current().with_label_values(&[model]).set(current as f64);
    }

    pub fn inc_concurrency_acquire_total(&self, model: &str, result: &str) {
        concurrency_acquire_total().with_label_values(&[model, result]).inc();
    }

    pub fn inc_network_errors_total(&self, model: &str, error_type: &str) {
        network_errors_total().with_label_values(&[model, error_type]).inc();
    }

    /// 记录从上游响应抽出的 token 用量。每个请求调一次（非流式 body 读完后，
    /// 或流结束时），传入 `usage` 对象的 prompt/completion/total 计数。任意
    /// `None`/`0` 分量直接跳过，避免部分 usage 对象少计数。
    pub fn record_usage(&self, prompt: Option<u64>, completion: Option<u64>, total: Option<u64>) {
        if let Some(p) = prompt {
            token_input().inc_by(p as f64);
        }
        if let Some(c) = completion {
            token_output().inc_by(c as f64);
        }
        if let Some(t) = total {
            token_total().inc_by(t as f64);
        }
    }

    /// 以 Prometheus 文本格式导出指标。
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// 更新 key 状态 gauge。每次状态转换后由 `KeyPool::refresh_metrics` 调用。
    pub fn set_key_counts(&self, active: i64, cooldown: i64, disabled: i64) {
        key_active().set(active as f64);
        key_cooldown().set(cooldown as f64);
        key_disabled().set(disabled as f64);
    }
}
