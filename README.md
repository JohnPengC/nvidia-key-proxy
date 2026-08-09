# NVIDIA API Key Load‑Balancing Proxy

A lightweight, high‑performance proxy written in Rust that fronts multiple NVIDIA API keys with per‑key rate limiting, health checking, and Prometheus observability.

## Features

- **Load‑balancing** across many keys to avoid per‑key rate limits.
- **Automatic throttling detection** – 429/403 (with `Retry-After`) cools the offending key; 401 without `Retry-After` triggers a retry on another healthy key, and repeated auth failures disable the key.
- **Health checking** – startup probe + periodic bad-key recovery keep the pool aware of dead keys.
- **Observability** – Prometheus metrics at `metrics_path` (default `/metrics`) and structured tracing.
- **Secure** – real keys never leave the process; logs use `XXXX***XXXX` masking.
- **Empty-shell retry** – detects NVIDIA's silent 200 soft-failure (`choices:[]`, `usage:null`), auto-clamps `max_tokens`, retries with exponential backoff (default 2 retries, 500ms base), optional upstream health probe, per-model circuit breaker.
- **Concurrency limiting** – per-model semaphore prevents overwhelming upstream workers (e.g., nemotron-3-ultra's 32-worker limit); queue timeout returns 503 with `Retry-After`.
- **Network resilience** – configurable connection pool (idle timeout 90s, connect timeout 5s), retry jitter (default 100ms), network error metrics.
- **Request normalization** – coalesces invalid `max_tokens`/`reasoning_effort`/`tools` fields instead of hard 400; remaps model aliases; injects `stream_options.include_usage`.
- **Multimodal pass-through** – forwards OpenAI-style content-block arrays (`image_url`, `input_audio`) to NVIDIA NIM vision/audio models unchanged, and estimates their prompt size by coarse per-part costs (not raw byte length) so an inline base64 image doesn't trip the 413 ceiling and a URL image doesn't undercount. Video is not supported (NIM has no standardized video input).
- **Configuration validation** – fails fast on placeholder `base_url`/keys, warns on overly short timeouts.
- **Streaming timeout** – separate `request_timeout_streaming_seconds` for long generations.
- **max_tokens validation** – drops invalid `max_tokens` (≤0, >100k, non‑numeric, negative strings) before they hit NVIDIA.

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
   # retry once with max_tokens clamped to `empty_shell_retry_max_tokens` before
   # giving up and surfacing a 502. Without this, Claude Code's `/compact` prints
   # the misleading "summarization produced empty response". Defaults to true;
   # disable via NVIDIA_RETRY_ON_EMPTY_SHELL=false.
   retry_on_empty_shell = true

   # max_tokens used for the post-shell retry. The first upstream call may
   # itself be pre-clamped (see `empty_shell_pre_clamp_max_tokens` below) so
   # the budget fits without burning a round-trip into a guaranteed shell.
   # 8192 is conservative across GLM-family models on build.integrate.
   # Env override: NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS.
   empty_shell_retry_max_tokens = 8192

   # Optional: number of retry attempts for empty-shell soft failures.
   # 1 = one retry (original + one retry). 0 = no retry (falls back to 502).
   # Default 1. Env override: NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS.
   # empty_shell_retry_attempts = 1

   # Optional: before retrying an empty-shell failure, do a lightweight health
   # probe against /v1/models to avoid retrying into a degraded upstream.
   # Default true. Env override: NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY.
   # empty_shell_probe_before_retry = true

   # Optional: per-model max_tokens ceiling so the *first* upstream call is
   # clamped before it goes out (streaming included). NVIDIA's GLM-class models
   # return a 200 "empty-shell" when `prompt + max_tokens` exceeds the remaining
   # context budget; clamping at the entrance makes the budget fit instead of
   # burning a round-trip into a guaranteed shell. Default GLM-5.2 → 32000.
   # Override via NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS (JSON map).
   # empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 32000 }

   # Optional: circuit breaker threshold. After N consecutive empty-shell or 504
   # responses on a given key/model, the key is cooled for cooldown.base_seconds.
   # 0 = disabled. Default 3. Env override: NVIDIA_CIRCUIT_BREAKER_THRESHOLD.
   # circuit_breaker_threshold = 3

   # Retry on 500 Internal Server Error. GLM-class models intermittently return
   # {"message":"Failed to generate completions",...} as a transient per-request
   # worker failure (same prompt succeeds on another key seconds later). When
   # enabled, 500 retries against another key with backoff. Default true.
   # Env override: NVIDIA_RETRY_ON_500.
   # retry_on_500 = true

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"

   # Client-facing model name → NVIDIA-ready model id mappings.
   # When a chat-completion request carries `model` matching a key here, the
   # proxy rewrites the field to the mapped value before forwarding upstream.
   # Unmatched names are forwarded unchanged. Empty by default.
   #
   # Use this when your client (cc-switch, Cursor, OpenAI SDK, …) sends
   # OpenAI-style model names that don't exist on NVIDIA NIM, e.g.:
   #
   #   [model_aliases]
   #   "claude-opus-4-8"        = "z-ai/glm-5.2"   # identity mapping kept for clarity
   #   "gpt-4o"              = "meta/llama-3.1-405b-instruct"
   #   "gpt-4o-mini"         = "meta/llama-3.1-70b-instruct"
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
| `request_timeout_seconds` | Non-streaming request timeout; 0 disables (streaming always bypasses) | `600` |
| `request_timeout_streaming_seconds` | Streaming request timeout; 0 = use non-streaming timeout or disabled | `0` |
| `retry_on_empty_shell` | Retry on NVIDIA empty-shell soft failure (200 with `choices:[]`, `usage:null`) | `true` |
| `empty_shell_retry_max_tokens` | Clamped `max_tokens` for empty-shell retry | `8192` |
| `empty_shell_retry_attempts` | Number of empty-shell retries (0 = no retry) | `1` |
| `empty_shell_probe_before_retry` | Probe `/v1/models` before empty-shell retry | `true` |
| `empty_shell_pre_clamp_max_tokens` | Per-model `max_tokens` ceiling clamped on the *first* upstream call (streaming included) | `{"z-ai/glm-5.2": 32000, "nvidia/nemotron-3-ultra-550b-a55b": 8192}` |
| `circuit_breaker_threshold` | Consecutive shell/504 before cooldown (0 = disabled) | `3` |
| `max_tokens_max` | Ceiling for `max_tokens` validation; drop field if exceeded | `100_000` |
| `est_image_token_cost` | Flat token cost per `image_url` block in the prompt-size estimate (URL or inline base64 alike) | `1500` |
| `est_audio_token_per_kb` | Token cost per KiB of base64 in an `input_audio` block | `16` |
| `est_audio_token_cap` | Hard cap on the per-block audio estimate | `10000` |
| `est_unknown_part_token_cap` | Cap on any unrecognized content-block type's serialized length | `2000` |
| `retry_on_503` | Retry 503 (Worker resource exhausted) against another key | `true` |
| `retry_on_500` | Retry 500 `Failed to generate completions` against another key | `true` |
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

## Failure-Log-Driven Reliability (2026‑07‑23)

Analyzed `failure.log` and addressed the four dominant error classes:

| Category | Count | Root Cause | Fix |
|---|---|---|---|
| `empty_choices_shell_first` | 41.9% | NVIDIA returns 200 with `choices:[]`, `usage:null`, `id:""` — no inference ran. | **Pre-clamp `max_tokens` on the first upstream call** (per-model `empty_shell_pre_clamp_max_tokens`, default GLM-5.2 → 32000; covers streaming) so the `prompt + max_tokens` budget fits. If a shell still returns, **retry with clamped `max_tokens`** (config `empty_shell_retry_max_tokens`, default 8192), optional upstream health probe before retry (`empty_shell_probe_before_retry`), configurable attempts (`empty_shell_retry_attempts`, default 1). Surfaces actionable 502 instead of misleading "summarization produced empty response". |
| `timeout` | 37.2% | 300s hard timeout too short for long-context non-streaming. | **Default `request_timeout_seconds` raised to 600s**. Streaming requests bypass this entirely. New `request_timeout_streaming_seconds` for separate streaming timeout. Warn threshold raised to 30s. |
| `upstream_5xx` (503/504/500) | 14.0% | Gateway timeout on streaming / resource exhausted / transient `{"message":"Failed to generate completions"}` 500. | Client `connect_timeout` (5s) unchanged; per-request timeout now only for non-streaming. Streaming path unbounded by default. **503 retries** another key with backoff (`retry_on_503`, default true, optional `probe_before_503_retry`). **500 retries** another key with the same backoff/retry budget (`retry_on_500`, default true) — GLM 500s are transient per-request failures, not deterministic bans. |
| `upstream_4xx` (400 `max_tokens < 1`) | 2.3% | Client-computed `max_tokens` underflow (large context + reserved tokens). | **Proxy drops invalid `max_tokens`** (≤0, >100k, non-numeric). Ceiling configurable via `max_tokens_max` (default 100_000) or `MAX_TOKENS_MAX` env. |

### New Configuration Options

```toml
# config.toml
request_timeout_seconds = 600                # Non-streaming timeout (0 = disabled)
request_timeout_streaming_seconds = 0        # Streaming timeout (0 = use non-streaming or disabled)

# Empty-shell soft failure retry
retry_on_empty_shell = true
empty_shell_retry_max_tokens = 8192          # Clamped budget for retry
empty_shell_retry_attempts = 1               # Number of retries after shell
empty_shell_probe_before_retry = true        # Probe /v1/models before retry
empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 32000 }  # Pre-clamp first call (streaming too)

# Retry transient 5xx against another key
retry_on_503 = true
retry_on_500 = true

# Circuit breaker (pre-reserved, logic pending)
circuit_breaker_threshold = 3                # Consecutive shell/504 before cooldown

# max_tokens validation ceiling
max_tokens_max = 100_000                     # Drop field if exceeded
```

All options overrideable via `NVIDIA_*` env vars (e.g. `NVIDIA_REQUEST_TIMEOUT_SECONDS`, `NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS`).

### Circuit Breaker Fields (Pre-reserved)

`circuit_breaker_threshold` and `max_tokens_max` are now in `Config` and `Proxy` structs; the circuit breaker logic will be implemented in a follow-up. `max_tokens_max` is already active for request validation.

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
- **`failure_log`** – structured JSONL failure log (`failure.log`). Path override and rotation via `FAILURE_LOG_PATH` / `FAILURE_LOG_MAX_BYTES` (see below); `FAILURE_LOG_DISABLE` silently no-ops writes (CI).

#### `failure.log` rotation

The log appends unbounded unless `FAILURE_LOG_MAX_BYTES` is set (a byte ceiling on the *current* file — set it to e.g. `10485760` for 10 MiB). When a new line would push the current file past the ceiling, the existing file is rolled to `failure.log.<unix-secs>` and a fresh file is opened before the line is written, all under one mutex — no entries are dropped. `0` / unset keeps the legacy append-only behavior the tuning history above was built on. Archived files are not pruned or compressed by the proxy: pair with an external `logrotate` (e.g. `copytruncate` + `rotate N` + compress) when you need retention limits.

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
- **Multimodal content is passed through verbatim**: a message `content` array
  of OpenAI-style blocks (`{type:"text",…}`, `{type:"image_url",…}`,
  `{type:"input_audio",…}`) reaches NVIDIA unchanged — the body-normalization
  rewrites touch only top-level scalar fields (`max_tokens`,
  `reasoning_effort`, `model`, `stream_options`), never `messages[].content`.
  NVIDIA NIM's public API only documents plain-text `content`; vision/audio
  models are OpenAI-compatible and accept these block arrays, but the exact
  schema and which models accept it is unverified upstream — the proxy does no
  format translation and makes no validation claims. **Video is not supported**
  (NIM has no standardized video input).
- **Multimodal prompt-token estimates use coarse per-part costs**: the 413 hard
  ceiling (`request_max_prompt_tokens`) and the empty-shell no-retry soft limit
  (`empty_shell_no_retry_prompt_tokens`) compare against `estimate_prompt_tokens`,
  which sizes a `content` array as `text=chars/4`, `image_url=<flat cost>`,
  `input_audio=<per-KiB cost, capped>`, unknown=`<chars/4, capped>`. This keeps
  an inline base64 image (tens of thousands of chars) from ballooning the
  estimate and falsely 413'ing a legal multimodal request, while a URL-only image
  (a few dozen chars) still costs the flat image cost and can't slip past the
  ceiling for free. The failure-log fingerprint reports this same coarse est
  and a part census (text/image/audio counts) instead of dumping the base64
  payload. Tunable via `est_image_token_cost`,
  `est_audio_token_per_kb`/`est_audio_token_cap`, `est_unknown_part_token_cap`.
  **Note (2026-08-01, live-verified):** the *upstream* side is gated by key
  permissions — against a test pool of 22 keys, NVIDIA returned HTTP **403
  `Authorization failed`** for any request whose `content` is an OpenAI-style
  array (text-only array, URL-only image, inline base64 image, on a vision
  *or* text model), while plain-string `content` on the same keys returned 200.
  The proxy passes the array through unchanged (and forwards the 403 to the
  client, since 4xx is not retried), but actual multimodal inference requires a
  key whose NVIDIA plan has vision permission; see `FAILURE_ANALYSIS.md` §九.

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