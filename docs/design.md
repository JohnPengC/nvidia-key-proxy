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
| **KeyPool** | Hold `KeyState` (active, cooling, disabled). Provide `acquire()`, `mark_good()`, `mark_bad()`. | `DashMap`<br>`DashMap<KeyId, KeyState>` |
| **TokenBucket** | Per‑key rate limiter (QPS + burst). | `async-tokenbucket` or custom using `tokio::time::Interval` |
| **Proxy Handler** | Accept HTTP request → acquire key → wait for token → forward → update key state → return response (streaming SSE passthrough). | `warp` for server, `reqwest` client |
| **HealthChecker** | Startup probe + periodic bad-key recovery. Update state. | `tokio::task::spawn` loop |
| **Observability** | Prometheus endpoint at `metrics_path` (default `/metrics`); tracing with `tracing`+`tracing-subscriber`. | `prometheus`, `tracing` |
| **Graceful Shutdown** | Listen for SIGINT/SIGTERM, stop accepting new connections, drain ongoing requests. | `tokio::signal`, warp’s graceful shutdown |

### Key Acquisition Algorithm

1. Filter `KeyPool` for `State::Active`.
2. Each active key has a weight (starts at 1, increases on success, halves on throttling, min 1).
3. Perform weighted random selection.
4. Return the selected key and its associated `TokenBucket`.

### Rate Limiting

* Token bucket refills at `qps_per_key` tokens per second, bucket size = `burst`.
* Before forwarding, `bucket.acquire_one().await` waits if no token is available.

### Cool‑down & Health

* On 429/403: read `retry-after` header (seconds) or use `base_cooldown`; set state to `Cooling { ready_at: Instant::now() + Duration::from_secs(secs) }`.
* Health checker runs every `health_interval` seconds:
  * Probe each key; on success → reset fail count, ensure `Active`.
  * On failure → increment fail count; after `max_consecutive_failures` → move to `Cooling` (or `Disabled` if desired).

### Validation

Two validation layers prevent avoidable upstream failures and reduce noise in `failure.log`:

1. **Configuration validation** (`Config::validate`, called from `Config::load` after env overrides):
   * Rejects placeholder `base_url` values (`example.com`, `localhost`, `127.0.0.1`) and URLs without a scheme.
   * Rejects obviously fake/short API keys (`fake`, `dummy`, `test`, `example`, `placeholder`, or shorter than 8 characters).
   * Warns when `request_timeout_seconds` is set below 5 seconds, because such aggressive values cause false timeouts on normal LLM latency.

2. **Request-body validation** in the proxy handler before a key is consumed:
   * `max_tokens` must be an integer >= 1.
   * `reasoning_effort` must be `"low"`, `"medium"`, or `"high"`.
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

[cooldown]
base_seconds = 60
max_seconds  = 600

[health_check]
interval_seconds = 30
timeout_seconds  = 5
max_consecutive_failures = 3

[observability]
enable_metrics = true
metrics_path   = "/metrics"
log_level      = "info"

# Request timeout in seconds. Set to 0 to disable (recommended for streaming).
# Streaming requests bypass this timeout regardless.
request_timeout_seconds = 60
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

* **Unit tests** – `KeyPool` state transitions, token‑bucket correctness, proxy retry / timeout behaviour, and request validation (`max_tokens`, `reasoning_effort`).
* **Integration tests** – spin up a mock NVIDIA server (using `warp`) that can return 200, 429, 500; assert proxy rotates keys, honors rate limits, honors cool‑down. Additional tests cover the health endpoint, metrics (enabled/disabled), unknown-path 404 handling, and configuration validation.
* **Load test** – use `wrk` or `hey` against the proxy; verify metrics show distributed load and that no single key exceeds its QPS.
* **Security audit** – ensure logs never contain full API keys (custom `Debug` impl on `Config` masks keys as `XXXX***XXXX`).

### Future Enhancements

* Dynamic key addition/removal via a protected admin endpoint.
* Per‑model weighting or cost‑aware scheduling.
* Support for multiple upstream bases (region fail‑over).
* gRPC/WebSocket forwarding if NVIDIA adds such interfaces.

---  

*End of design document.*