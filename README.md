# NVIDIA API Key Load‑Balancing Proxy

A lightweight, high‑performance proxy written in Rust that fronts multiple NVIDIA API keys with per‑key rate limiting, health checking, and Prometheus observability.

## Features

- **Load‑balancing** across many keys to avoid per‑key rate limits.
- **Automatic throttling detection** – 429/403 (with `Retry-After`) cools the offending key; 401 without `Retry-After` triggers a retry on another healthy key, and repeated auth failures disable the key.
- **Health checking** – startup probe + periodic bad-key recovery keep the pool aware of dead keys.
- **Observability** – Prometheus metrics at `metrics_path` (default `/metrics`) and structured tracing.
- **Secure** – real keys never leave the process; logs use `XXXX***XXXX` masking.

## Quick Start

1. **Clone and build**
   ```bash
   git clone <repository-url>
   cd nvidia-key-proxy
   cargo build --release
   ```

2. **Configure** (`config.toml` in project root):
   ```toml
   listen_addr = "0.0.0.0:8888"
   base_url    = "https://integrate.api.nvidia.com"

   # API keys – can also be set via NVIDIA_KEYS env var (comma‑separated)
   keys = [
       "nvapi-xxxxxxxxxxxxxxxx1",
       "nvapi-xxxxxxxxxxxxxxxx2",
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

   # Request timeout in seconds. Set to 0 to disable (recommended for streaming).
   # Streaming requests bypass this timeout regardless.
   request_timeout_seconds = 300

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"
   ```

3. **Run**
   ```bash
   ./target/release/nvidia-key-proxy
   ```

4. **Use with your client** – point `base_url` to `http://localhost:8888`. The `api_key` field can be any string; the proxy replaces it with a real key from the pool.

## Configuration

### Top-level fields

| Field | Description | Default |
|---|---|---|
| `listen_addr` | Address to listen on | Required |
| `base_url` | NVIDIA API base URL | Required |
| `keys` | List of NVIDIA API keys | Required (or `NVIDIA_KEYS` env) |
| `request_timeout_seconds` | Request timeout; 0 disables (streaming always bypasses) | `60` |
| `allow_local_base_url` | Permit a loopback/localhost `base_url` (testing only) | `false` |
| `auth` | Front-door + admin auth config (see below) | disabled |

### `[rate_limiter]`

| Field | Description |
|---|---|
| `qps_per_key` | Queries per second per key |
| `burst` | Token bucket size |

### `[cooldown]`

| Field | Description |
|---|---|
| `base_seconds` | Initial cool-down duration |
| `max_seconds` | Maximum cool-down (exponential backoff cap) |

### `[health_check]`

| Field | Description |
|---|---|
| `interval_seconds` | Bad-key recovery check interval |
| `timeout_seconds` | Probe timeout |
| `max_consecutive_failures` | Failures before moving a key to cool-down |

### `[observability]`

| Field | Description |
|---|---|
| `enable_metrics` | Enable Prometheus metrics endpoint |
| `metrics_path` | Path where metrics are exposed (default `/metrics`) |
| `log_level` | Tracing log level (also overridable via `RUST_LOG`) |

### `[retry]`

| Field | Description | Default |
|---|---|---|
| `max_retries` | Maximum retry attempts on retryable errors | `3` |
| `backoff_base_ms` | Initial backoff in milliseconds | `200` |
| `backoff_max_ms` | Maximum backoff in milliseconds | `2000` |
| `auth_fail_threshold` | Consecutive auth failures before a key is disabled | `3` |
| `max_key_switches` | Max key switches on token-starvation per request (decoupled from `max_retries`) | `3` |

### `[auth]`

| Field | Description | Default |
|---|---|---|
| `enabled` | Require clients to send `Authorization: Bearer <bearer_token>` on `/v1/*` | `false` |
| `bearer_token` | Client bearer token (also `NVIDIA_AUTH_BEARER_TOKEN`) | empty |
| `admin_token` | Token protecting `/admin/*`; when empty the admin API is disabled | empty |

### `[health_check]`

## Configuration Validation

At startup the proxy validates the loaded configuration and exits early if it looks misconfigured, preventing silent failures (e.g. 401/404 caused by placeholder values):

- `base_url` must not be `example.com`, `localhost`, or `127.0.0.1`, and must start with `http://` or `https://`.
- API keys must be at least 8 characters long and cannot contain `fake`, `dummy`, `test`, `example`, or `placeholder`.
- A `request_timeout_seconds` value below `5` logs a warning suggesting a larger value.

## Request Normalization

Before forwarding chat-completion requests, the proxy normalizes request-body
fields that clients (cc-switch, Cursor, OpenAI SDK) often send in a shape
NVIDIA NIM rejects with a 400 — salvaging the request instead of bouncing it:

- `max_tokens`: a bool / null / non-positive / non-numeric-string value is
  dropped (NVIDIA picks its default) or coerced to an int.
- `reasoning_effort`: a bool (`true`→`"high"`) or an unrecognized string is
  rewritten or dropped; `low`/`medium`/`high` pass through unchanged.
- `model`: remapped through `[model_aliases]` when configured.

These are best-effort compatibility fixes, not strict validation — there is no
longer a hard 400 path for these fields (see *Behavior & Compatibility*).

## Metrics

Prometheus metrics are exposed at `observability.metrics_path` (default `/metrics`) when `observability.enable_metrics` is `true`.

| Metric | Description |
|---|---|
| `proxy_requests_total` | Total HTTP requests processed |
| `proxy_requests_success_total` | 2xx requests |
| `proxy_requests_errors_total` | Non-2xx requests |
| `proxy_request_duration_seconds` | Latency histogram |
| `proxy_key_active` | Number of active keys |
| `proxy_key_cooldown` | Number of keys in cool-down |
| `proxy_key_disabled` | Number of disabled keys |
| `proxy_tokens_input_total` | Cumulative input token count (prompt tokens) |
| `proxy_tokens_output_total` | Cumulative output token count (completion tokens) |
| `proxy_tokens_total` | Cumulative total tokens (prompt + completion), counted from real `usage` objects in streamed + buffered responses |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `{metrics_path}` | Prometheus metrics |
| GET | `/health` | Health check (returns 204) |
| * | `/*` | Proxied to NVIDIA API (with key injection + rate limiting) |

## Architecture

The proxy is organised into focused modules under `src/lib.rs`:

- **`config`** – loads `config.toml` / env / CLI; custom `Debug` masks API keys.
- **`key_pool`** – holds per-key state (active, cooling, disabled) with weighted random selection.
- **`proxy`** – warp handler: acquire key → rate-limit → forward → update key state → stream response back.
- **`health`** – startup probe + periodic bad-key recovery.
- **`metrics`** – Prometheus counters and histograms.
- **`failure_log`** – structured JSONL failure log (`failure.log`).

`AppState` bundles the shared runtime objects (metrics, key pool, proxy, health checker, stop signal, config) and `build_routes` constructs the warp filters, so integration tests can exercise the full stack without reaching into `main`.

## Behavior & Compatibility

This proxy is **not a transparent pass-through**. On top of the core job —
distributing requests across NVIDIA keys with per-key rate limiting and
failover — it applies a set of best-effort compatibility normalizations so
that OpenAI-style clients (cc-switch, Cursor, the OpenAI SDK, …) work without
editing their request bodies. Each of these rewrites is a deliberate deviation
from "pure forwarding" and is listed here so the behavior is predictable:

- **Request-body normalization** (`/v1/chat/completions` only): `max_tokens`
  given as a bool / null / non-positive / non-numeric string is dropped or
  coerced; `reasoning_effort` given as a bool (`true`→`"high"`) or an
  unrecognized string is rewritten or dropped; invalid values are salvaged
  rather than bounced with a 400.
- **Model aliases**: a client-sent `model` matching `[model_aliases]` is
  rewritten to the mapped NVIDIA id before forwarding.
- **`/v1/responses` and `/v1/messages` are rejected**: NVIDIA only serves
  OpenAI Chat Completions. The OpenAI Responses API (`/v1/responses`) and the
  Anthropic Messages API (`/v1/messages`) are not exposed; the proxy returns
  `501 Not Implemented` with a typed `not_supported` error instead of silently
  forwarding a body these endpoints can't parse.
- **`/v1/models` is forwarded to NVIDIA**: the proxy forwards `GET /v1/models`
  to NVIDIA (which answers it without auth), caches the result, and falls back
  to the configured `[model_aliases]` list if NVIDIA is unreachable. Clients
  therefore see NVIDIA's full, real catalog rather than a hardcoded subset.
- **Streaming usage is injected**: for streaming chat requests, the proxy adds
  `stream_options.include_usage: true` (unless the client already set it) so
  NVIDIA returns token usage at stream end and the proxy's token metrics are
  populated. Disable with `inject_stream_usage = false` or
  `NVIDIA_INJECT_STREAM_USAGE=false`.
- **`Accept` header is forced** to `text/event-stream` when the client asks
  for `stream` (and `application/json` otherwise), so NVIDIA and the client
  agree on the transfer shape.
- **`error.code` is coerced to a string** in both buffered and SSE error
  responses, because NVIDIA returns a numeric `code` while OpenAI-compatible
  clients expect a string.

If you need strict transparency, the body/alias/response-path rewrites are the
parts to be aware of; they are confined to chat-completion paths.

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `{metrics_path}` | — | Prometheus metrics |
| GET | `/health` | — | Health check (204); always exempt from front-door auth |
| * | `/*` | front-door* | Proxied to NVIDIA (key injection + rate limiting) |
| GET | `/admin/keys` | admin | List every key's state (masked) and live stats |
| POST | `/admin/keys` | admin | Add key(s): JSON `["k1", "k2"]` or `{"keys":[...]}` |
| DELETE | `/admin/keys/{key}` | admin | Remove a key at runtime |

\* Front-door auth applies only when `[auth].enabled = true`. The admin API
is entirely disabled (404) unless `[auth].admin_token` is set.

## Implementation Refactor (2026‑07‑11)

- Introduced **`AppState`** to group shared runtime objects (metrics, key pool, proxy, health checker, stop signal, config).
- Extracted tracing initialization into **`init_tracing`** for clearer start‑up ordering.
- Moved route construction into **`build_routes`**, keeping `main` focused on orchestration.
- Graceful shutdown now uses a single `Notify` and drops background task handles, avoiding deadlocks.
- Added integration tests under **`tests/`** verifying health, metrics (enabled/disabled), and unknown‑path handling.

## Validation & Hardening (2026‑07‑16)

- Added **configuration validation** (`Config::validate`) to fail fast on placeholder `base_url`/keys and warn on overly short request timeouts.
- Added **request-body validation** in the proxy for `max_tokens` and `reasoning_effort`, short-circuiting invalid OpenAI-style chat requests before they reach NVIDIA.
- Expanded unit tests covering both validation layers.

## Testing

```bash
cargo test --release
```

Unit tests cover key-pool state transitions, token-bucket correctness, and proxy retry/timeout behaviour. Integration tests verify health, metrics (enabled/disabled), and unknown‑path handling.

## License

MIT

## Acknowledgments

- Built with Tokio, Warp, reqwest, and serde.