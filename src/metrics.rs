use std::sync::OnceLock;

use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};

fn request_count() -> &'static Counter {
    static REQUEST_COUNT: OnceLock<Counter> = OnceLock::new();
    REQUEST_COUNT.get_or_init(|| {
        register_counter!(
            "proxy_requests_total",
            "Total number of HTTP requests processed"
        )
        .unwrap()
    })
}

fn request_success() -> &'static Counter {
    static REQUEST_SUCCESS: OnceLock<Counter> = OnceLock::new();
    REQUEST_SUCCESS.get_or_init(|| {
        register_counter!(
            "proxy_requests_success_total",
            "Number of successful requests (status 2xx)"
        )
        .unwrap()
    })
}

fn request_error() -> &'static Counter {
    static REQUEST_ERROR: OnceLock<Counter> = OnceLock::new();
    REQUEST_ERROR.get_or_init(|| {
        register_counter!(
            "proxy_requests_errors_total",
            "Number of requests that result in error (non-2xx)"
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
        register_histogram!(
            "proxy_request_duration_seconds",
            "HTTP request latency in seconds",
            prometheus::exponential_buckets(0.005, 2.0, 12).unwrap()
        )
        .unwrap()
    })
}

fn retry_attempt_failure() -> &'static Counter {
    static RETRY_ATTEMPT_FAILURE: OnceLock<Counter> = OnceLock::new();
    RETRY_ATTEMPT_FAILURE.get_or_init(|| {
        register_counter!(
            "proxy_retry_attempt_failures_total",
            "Per-attempt upstream failures retried internally (not visible to client). Distinct from proxy_requests_errors_total which counts client-visible failures."
        )
        .unwrap()
    })
}

fn key_active() -> &'static Gauge {
    static KEY_ACTIVE: OnceLock<Gauge> = OnceLock::new();
    KEY_ACTIVE.get_or_init(|| {
        register_gauge!(
            "proxy_key_active",
            "Number of API keys currently in active state"
        )
        .unwrap()
    })
}

fn key_cooldown() -> &'static Gauge {
    static KEY_COOLDOWN: OnceLock<Gauge> = OnceLock::new();
    KEY_COOLDOWN.get_or_init(|| {
        register_gauge!(
            "proxy_key_cooldown",
            "Number of API keys currently in cooldown"
        )
        .unwrap()
    })
}

fn key_disabled() -> &'static Gauge {
    static KEY_DISABLED: OnceLock<Gauge> = OnceLock::new();
    KEY_DISABLED.get_or_init(|| {
        register_gauge!(
            "proxy_key_disabled",
            "Number of API keys currently disabled"
        )
        .unwrap()
    })
}

// ── New metrics for token-starvation switch and 403 immediate cooldown ──

pub fn key_switch_token_starvation() -> &'static Counter {
    static KSW: OnceLock<Counter> = OnceLock::new();
    KSW.get_or_init(|| {
        register_counter!(
            "proxy_key_switch_token_starvation_total",
            "Number of times a request switched to another key because the first key's token bucket was empty"
        )
        .unwrap()
    })
}

pub fn key_403_auth_cooldown() -> &'static Counter {
    static K403: OnceLock<Counter> = OnceLock::new();
    K403.get_or_init(|| {
        register_counter!(
            "proxy_key_403_auth_cooldown_total",
            "Number of times a 403 Authorization failed triggered immediate key cooldown"
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

    /// Record token usage extracted from an upstream response. Called once per
    /// request (after a non-streaming body is read, or when a stream ends) with
    /// the `usage` object's prompt/completion/total counts. Any `None`/`0`
    /// component is simply skipped so partial usage objects don't under-count.
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

    /// Expose metrics in Prometheus text format.
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Update key-state gauges. Called by `KeyPool::refresh_metrics` after
    /// every state transition.
    pub fn set_key_counts(&self, active: i64, cooldown: i64, disabled: i64) {
        key_active().set(active as f64);
        key_cooldown().set(cooldown as f64);
        key_disabled().set(disabled as f64);
    }
}
