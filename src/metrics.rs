use lazy_static::lazy_static;

use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};

lazy_static! {
    static ref REQUEST_COUNT: Counter = register_counter!(
        "proxy_requests_total",
        "Total number of HTTP requests processed"
    )
    .unwrap();
    static ref REQUEST_SUCCESS: Counter = register_counter!(
        "proxy_requests_success_total",
        "Number of successful requests (status 2xx)"
    )
    .unwrap();
    static ref REQUEST_ERROR: Counter = register_counter!(
        "proxy_requests_errors_total",
        "Number of requests that result in error (non-2xx)"
    )
    .unwrap();
    static ref REQUEST_LATENCY: Histogram = register_histogram!(
        "proxy_request_duration_seconds",
        "HTTP request latency in seconds",
        prometheus::exponential_buckets(0.005, 2.0, 12).unwrap()
    )
    .unwrap();
    static ref RETRY_ATTEMPT_FAILURE: Counter = register_counter!(
        "proxy_retry_attempt_failures_total",
        "Per-attempt upstream failures retried internally (not visible to client). Distinct from proxy_requests_errors_total which counts client-visible failures."
    )
    .unwrap();
    static ref KEY_ACTIVE: Gauge = register_gauge!(
        "proxy_key_active",
        "Number of API keys currently in active state"
    )
    .unwrap();
    static ref KEY_COOLDOWN: Gauge = register_gauge!(
        "proxy_key_cooldown",
        "Number of API keys currently in cooldown"
    )
    .unwrap();
    static ref KEY_DISABLED: Gauge = register_gauge!(
        "proxy_key_disabled",
        "Number of API keys currently disabled"
    )
    .unwrap();
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
        REQUEST_COUNT.inc();
    }

    pub fn inc_success(&self) {
        REQUEST_SUCCESS.inc();
    }

    pub fn inc_retry_attempt_failure(&self) {
        RETRY_ATTEMPT_FAILURE.inc();
    }

    pub fn inc_error(&self) {
        REQUEST_ERROR.inc();
    }

    pub fn observe_latency(&self, seconds: f64) {
        REQUEST_LATENCY.observe(seconds);
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
        KEY_ACTIVE.set(active as f64);
        KEY_COOLDOWN.set(cooldown as f64);
        KEY_DISABLED.set(disabled as f64);
    }
}
