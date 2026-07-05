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

#[derive(Clone)]
pub struct Metrics {
    _private: (),
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
