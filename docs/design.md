# NVIDIA API Key Load‑Balancing Proxy (Rust)

## Overview

A lightweight, high‑performance proxy written in Rust that fronts multiple NVIDIA API keys.  
Clients (e.g. OpenAI‑compatible SDKs) talk to the proxy using the usual `base_url`, `api_key` and `model` parameters.  
The proxy picks a healthy key from a pool, applies per‑key rate limiting, forwards the request to the real NVIDIA endpoint, and returns the response unchanged (including streaming SSE).

Key goals  

* **Load‑balance** many keys to avoid per‑key rate limits.  
* **Automatic throttling detection** – 429/403 triggers a cool‑down for the offending key.  
* **Health checking** – background probes keep the pool aware of dead keys.  
* **Observability** – Prometheus metrics and structured tracing.  
* **Transparent usage** – downstream clients need no code changes.  
* **Secure** – real keys never leave the process; logs are desensitized.  
* **Concurrency limiting** – per-model semaphore caps concurrent upstream requests (prevents 503 "Worker local total request limit reached"); queue timeout returns 503 with `Retry-After`.  
* **Empty-shell detection & retry** – NVIDIA's silent 200 with `usage:null` is detected; retries with clamped `max_tokens` + exponential backoff + optional upstream health probe; per-model circuit breaker.  
* **Network resilience** – configurable connection pool (idle timeout 90s, connect timeout 5s), retry jitter (default 100ms); network errors tracked in metrics.  
* **Streaming timeout** – separate `request_timeout_streaming_seconds` for long generations.  
* **max_tokens validation** – drops invalid `max_tokens` (≤0, >100k, non‑numeric, negative strings) before they hit NVIDIA.

## Architecture

```
+----------------+    HTTP    +------------------+    HTTP    +-------------------+
|  Client (SDK)  |  <---->  |  Rust Proxy      |  <---->  |  NVIDIA API       |
| base_url,key,  |          |  (KeyPool,RL,    |          |  integrate.api.   |
| model)         |          |   Health,Metrics)|          |  nvidia.com       |
+----------------+          +------------------+          +-------------------+
        ^                        ^   ^   ^                     ^
        |                        |   |   |                     |
        |   Config (Key list)    |   |   |   Response (stream) |
        +------------------------+   +---+---------------------+
                                    |
                                    +-- KeyPool (state per key)
                                    |
                                    +-- TokenBucket per key (rate limit)
                                    |
                                    +-- HealthChecker (startup + bad-key recovery)
                                    |
                                    +-- FailureLog (structured JSONL)
                                    |
                                    +-- Metrics / Tracing
```

### Modules

| Module | Responsibility | Tech |
|--------|----------------|------|
| **Config** | Load `config.toml` / env / CLI. Implements custom `Debug` to mask API keys in logs. | `serde` + `toml` + `dotenvy` |
| **KeyPool** | Hold `KeyState` (active, cooling, disabled). Provide `acquire()`, `mark_good()`, `mark_bad()`. Per‑model circuit‑breaker counters. | `DashMap`<br>`DashMap<KeyId, KeyState>` |
| **TokenBucket** | Per‑key rate limiter (QPS + burst). | `async-tokenbucket` or custom using `tokio::time::Interval` |
| **Proxy Handler** | Accept HTTP request → acquire key → wait for token → forward → update key state → return response (streaming SSE passthrough). Empty‑shell detection, pre‑clamp, retry with backoff, per‑model circuit breaker. | `warp` for server, `reqwest` client |
| **HealthChecker** | Startup probe + periodic bad-key recovery. Update state. | `tokio::task::spawn` loop |
| **Observability** | Prometheus endpoint at `metrics_path` (default `/metrics`); tracing with `tracing`+`tracing-subscriber`. | `prometheus`, `tracing` |
| **Graceful Shutdown** | Listen for SIGINT/SIGTERM, stop accepting new connections, drain ongoing requests. | `tokio::signal`, warp’s graceful shutdown |

### Key Acquisition Algorithm

1. Filter `KeyPool` for `State::Active`.
2. For each active key, compute a score:
   `score = (available_tokens / capacity) − in_flight × 0.15 / weight`
   Weight starts at 1, halves on failure (min 1), resets to 1 on success.
3. Pick the key with the highest score; tie‑break randomly.
4. Return the selected key; the caller must call `dec_in_flight()` on completion.

### Rate Limiting

* Token bucket refills at `qps_per_key` tokens per second, bucket size = `burst`.
* Before forwarding, `bucket.consume_with_timeout(1, deadline)` waits up to `token_wait_ms` (default 200 ms). On timeout the key is released and the next‑best key is tried (bounded by `max_key_switches`).

### Cool‑down & Health

* On 429/403 with `Retry-After`: cool the key for `min(Retry-After, cooldown.max_seconds)`.  
  On 403 "Authorization failed": immediate cooldown regardless of header.
* On network/timeout error: mark key bad (cool for `cooldown.base_seconds`), retry once with a different key.
* Health checker runs every `health_interval` seconds:
  * Probe each key via `/v1/models`; success → `Active`, fail count reset.
  * Consecutive probe failures (`max_consecutive_failures`) → force `Disabled` until next probe succeeds or cooldown expires.

### Circuit Breaker (per key & model)

Consecutive empty‑shell (200 with `usage:null`) or 504 responses increment a per‑model counter on the key. When the counter reaches `circuit_breaker_threshold` (default 3), the key is cooled. Enabled/disabled via `circuit_breaker_per_model` (default true).

### Validation

Two validation layers prevent avoidable upstream failures and reduce noise in `failure.log`:

1. **Configuration validation** (`Config::validate`, called from `Config::load` after env overrides):
   * Rejects placeholder `base_url` values (`example.com`, `localhost`, `127.0.0.1`) and URLs without a scheme.
   * Rejects obviously fake/short API keys (`fake`, `dummy`, `test`, `example`, `placeholder`, or shorter than 8 characters).
   * Warns when `request_timeout_seconds` is set below 5 seconds, because such aggressive values cause false timeouts on normal LLM latency.

2. **Request-body normalization** in the proxy handler before a key is consumed:
   * `max_tokens`: bool/null/string/≤0/>100k → dropped; numeric strings coerced to int. Negative results from context‑window math (e.g. `-14758`) are caught.
   * `reasoning_effort`: `true`→"high", `false`/null/invalid→dropped.
   * `model` remapped via `model_aliases` if present.
   * `tools` / `function.description` / `function.name`: null/missing → empty string (NVIDIA rejects `null`).
   * Invalid requests are returned as 400 immediately, without acquiring a key or contacting NVIDIA.

### Metrics (Prometheus)

* `proxy_requests_total{status="success|error|throttled"}`
* `proxy_key_state_total{state="active|cooling|disabled"}`
* `proxy_key_cooling_seconds_remaining{key_id="..."} (gauge)`
* `proxy_request_latency_seconds` (histogram)

### Configuration Example (config.toml)

```toml
listen_addr = "0.0.0.0:8888"
base_url    = "https://integrate.api.nvidia.com"

# API keys – can also be set via NVIDIA_KEYS env var (comma‑separated)
keys = [
    "nvapi-xxxxxxxxxxxxxxxx1",
    "nvapi-xxxxxxxxxxxxxxxx2",
    "nvapi-xxxxxxxxxxxxxxxx3",
]

[rate_limiter]
qps_per_key = 0.65
burst       = 8
# Maximum milliseconds to wait on a single key's token bucket before
# switching to another key (default 200). 0 disables switching — the
# existing unbounded-wait behaviour is preserved.
token_wait_ms = 200

[cooldown]
base_seconds = 60
max_seconds  = 600

[health_check]
interval_seconds = 30
timeout_seconds  = 5
max_consecutive_failures = 3

# Request timeout in seconds. Set to 0 to disable (recommended for streaming).
# Non-streaming requests use this timeout end-to-end. Streaming requests
# bypass this timeout entirely (long-running token generation can exceed 60s).
# Default increased to 600s for long-context non-streaming completions.
request_timeout_seconds = 600

# Optional: separate timeout for streaming connections (connect + idle read).
# When set > 0, this governs the streaming path instead of request_timeout_seconds.
# 0 = use request_timeout_seconds for both (legacy). Default 0 (disabled).
# request_timeout_streaming_seconds = 0

# On NVIDIA's "empty-shell" soft failure — a 200 whose body is
# `{"choices":[],"id":"","usage":null,...}` (no real inference ran; common on
# `/compact` with max_tokens larger than the model's remaining context budget) —
# retry with max_tokens clamped to `empty_shell_retry_max_tokens` before
# giving up and surfacing a 502. Without this, Claude Code's `/compact` prints
# the misleading "summarization produced empty response". Defaults to true;
# disable via NVIDIA_RETRY_ON_EMPTY_SHELL=false.
retry_on_empty_shell = true

# max_tokens used for the post-shell retry. The first upstream call may itself
# be pre-clamped (see `empty_shell_pre_clamp_max_tokens` below) so the budget
# fits without burning a round-trip into a guaranteed shell. 8192 is
# conservative across GLM-family models on build.integrate.
# Env override: NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS.
empty_shell_retry_max_tokens = 8192

# Number of retry attempts for empty-shell soft failures.
# 2 = original + two retries. 0 = no retry (falls back to 502).
# Default 2. Env override: NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS.
empty_shell_retry_attempts = 2

# Base delay for exponential backoff between empty-shell retries (ms).
# Default 500. Env override: NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_BASE_MS.
empty_shell_retry_backoff_base_ms = 500

# Cap for a single empty-shell retry backoff sleep (ms).
# Default 5000. Env override: NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_MAX_MS.
empty_shell_retry_backoff_max_ms = 5000

# Per-model max_tokens ceiling for pre-clamping on the *first* upstream call.
# When a request targets a model in this map and max_tokens exceeds the
# ceiling, the proxy clamps it before the request goes out — for every
# request, streaming included. NVIDIA's GLM-class models return an HTTP-200
# "empty-shell" body (choices:[], usage:null) when `prompt + max_tokens`
# exceeds the remaining context budget; clamping at the entrance makes the
# budget fit so inference actually runs, instead of burning a round-trip into
# a guaranteed shell. Default includes z-ai/glm-5.2 → 32000.
# Override via NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS (JSON map).
empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 32000 }

# Retry on 500 Internal Server Error. GLM-class models intermittently return
# {"message":"Failed to generate completions","type":"Internal Server Error","code":500}
# as a transient per-request worker failure — the same prompt typically succeeds
# on another key seconds later. When enabled, 500 triggers a retry against
# another key using the same backoff/retry budget as 401/503. Default true.
# Env override: NVIDIA_RETRY_ON_500.
retry_on_500 = true

# Optional: before retrying an empty-shell failure, do a lightweight health
# probe against /v1/models to avoid retrying into a degraded upstream.
# Default true. Env override: NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY.
empty_shell_probe_before_retry = true

# Circuit breaker threshold. After N consecutive empty-shell or 504
# responses on a given key/model, the key is cooled for cooldown.base_seconds.
# 0 = disabled. Default 3. Env override: NVIDIA_CIRCUIT_BREAKER_THRESHOLD.
circuit_breaker_threshold = 3

# Whether circuit breaker tracks failures per (key, model) pair
# instead of globally per key. Default true. Env override:
# NVIDIA_CIRCUIT_BREAKER_PER_MODEL.
circuit_breaker_per_model = true

# Optional ceiling for max_tokens validation. Any request with max_tokens
# > this value will have the field dropped (model default used). Default
# 100000. Override via MAX_TOKENS_MAX env var.
# max_tokens_max = 100000

[observability]
enable_metrics = true
metrics_path   = "/metrics"
log_level      = "info"
```

### Building & Running

```bash
# Clone / create project
git clone <repo-url> nvidia-key-proxy
cd nvidia-key-proxy

# Build
cargo build --release

# Run (uses ./config.toml or env)
./target/release/nvidia-key-proxy
```

Dockerfile (optional):

```dockerfile
FROM rust:1.78-slim AS builder
WORKDIR /app
COPY . .
RUN cargo install --release --path .

FROM debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/nvidia-key-proxy /usr/local/bin/
EXPOSE 8888
ENTRYPOINT ["/usr/local/bin/nvidia-key-proxy"]
```

### Testing Strategy

* **Unit tests** – `KeyPool` state transitions, token‑bucket correctness, proxy retry / timeout behaviour, and request validation (`max_tokens`, `reasoning_effort`, empty-shell retry logic, upstream health probe).
* **Integration tests** – spin up a mock NVIDIA server (using `warp`) that can return 200, 429, 500, empty-shell 200; assert proxy rotates keys, honors rate limits, honors cool‑down, retries on empty-shell with clamped `max_tokens`, probes upstream health before retry. Additional tests cover the health endpoint, metrics (enabled/disabled), unknown-path 404 handling, and configuration validation.
* **Load test** – use `wrk` or `hey` against the proxy; verify metrics show distributed load and that no single key exceeds its QPS.
* **Security audit** – ensure logs never contain full API keys (custom `Debug` impl on `Config` masks keys as `XXXX***XXXX`).

### Failure-Log-Driven Reliability (2026‑07‑23)

Analysis of `failure.log` revealed four dominant error classes. All are addressed in the current implementation:

| Category | Count | Root Cause | Fix |
|---|---|---|---|
| `empty_choices_shell_first` | ~30% | NVIDIA returns 200 with `choices:[]`, `usage:null`, `id:""` — no inference ran. | **Pre-clamp `max_tokens` on the first upstream call** (per-model, `empty_shell_pre_clamp_max_tokens`, default GLM‑5.2 → 32000; covers streaming) so the `prompt + max_tokens` budget fits. If a shell still returns, **retry with `max_tokens` clamped to `empty_shell_retry_max_tokens`** (default 8192) — configurable attempts (`empty_shell_retry_attempts`, default 2), exponential backoff (`empty_shell_retry_backoff_base_ms` / `..._max_ms`), optional upstream health probe before retry (`empty_shell_probe_before_retry`). Per‑model circuit breaker (`circuit_breaker_per_model`, default true). Surfaces actionable 502 instead of misleading "summarization produced empty response". |
| `timeout` | ~15% | 300s hard timeout too short for long-context non-streaming. | **Default `request_timeout_seconds` raised to 600s**. Streaming requests bypass this entirely. New `request_timeout_streaming_seconds` for separate streaming timeout. Warn threshold raised to 30s. |
| `upstream_5xx` (504/503/500) | ~10% | Gateway timeout / resource exhausted / transient `{"message":"Failed to generate completions"}` 500. | Client `connect_timeout` (5s) unchanged; per-request timeout now only for non-streaming. Streaming path unbounded by default. **503 retries** another key with backoff (`retry_on_503`, default true, optional `probe_before_503_retry`). **500 retries** another key with the same backoff/retry budget (`retry_on_500`, default true) — GLM 500s are transient per-request failures, not deterministic bans. |
| `upstream_4xx` (400 `max_tokens < 1`) | ~2% | Client-computed `max_tokens` underflow (large context + reserved tokens). | **Proxy drops invalid `max_tokens`** (≤0, >100k, non-numeric). Ceiling configurable via `max_tokens_max` (default 100_000) or `MAX_TOKENS_MAX` env. |

### Future Enhancements

* Dynamic key addition/removal via a protected admin endpoint.
* Per‑model weighting or cost‑aware scheduling.
* Support for multiple upstream bases (region fail‑over).
* gRPC/WebSocket forwarding if NVIDIA adds such interfaces.

---  

*End of design document.*