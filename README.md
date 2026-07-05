# NVIDIA API Key Load‑Balancing Proxy

A lightweight, high‑performance proxy written in Rust that fronts multiple NVIDIA API keys with built-in Codex CLI integration.

## Features

- **Load‑balancing** across many keys to avoid per‑key rate limits.
- **Automatic throttling detection** – 429/403 (with `Retry-After`) cools the offending key; 401 without `Retry-After` triggers a retry on another healthy key, and repeated auth failures disable the key.
- **Health checking** – startup probe + periodic bad-key recovery keep the pool aware of dead keys.
- **Responses API ↔ Chat Completions translation** – Codex CLI (`wire_api = "responses"`) can use NVIDIA's `/v1/chat/completions` backend transparently.
- **Codex CLI auto‑configuration** – on startup, generates `~/.codex/config.toml` and model catalog, replacing cc-switch for the Codex use case.
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

   [observability]
   enable_metrics = true
   metrics_path   = "/metrics"
   log_level      = "info"

   [codex]
   enabled                  = true
   default_model            = "z-ai/glm-5.2"
   model_reasoning_effort   = "high"
   disable_response_storage = true
   bearer_token             = "proxy-managed"
   # models = [
   #   "z-ai/glm-5.2:GLM 5.2:1000000",
   #   "moonshotai/kimi-k2.6:Kimi K2.6:256000",
   # ]
   ```

3. **Run**
   ```bash
   ./target/release/nvidia-key-proxy
   ```

4. **Use with your client** – point `base_url` to `http://localhost:8888`. The `api_key` field can be any string; the proxy replaces it with a real key from the pool.

## Codex CLI Integration

The proxy auto-manages `~/.codex/config.toml` on startup, eliminating the need for cc-switch as an intermediary.

### What it does

1. **Generates `~/.codex/nvidia-proxy-model-catalog.json`** – model catalog with entries derived from Codex's `gpt-5.5` template (if available in `models_cache.json`) or a minimal fallback.
2. **Updates `~/.codex/config.toml` incrementally** – sets `model_provider`, `model`, `model_reasoning_effort`, `disable_response_storage`, `model_catalog_json` (as an absolute path), and the `[model_providers.nvidia-proxy]` section. User-defined sections (`notify`, `mcp_servers`, etc.) are preserved.
3. **Removes conflicting providers** – any `[model_providers.custom]` section left by cc-switch is cleaned up.

### Why `model_catalog_json` must be absolute

Codex CLI requires `model_catalog_json` to be an absolute path. A relative path triggers `AbsolutePathBuf deserialized without a base path` on newer Codex versions. The proxy always writes the absolute path (e.g., `/Users/<user>/.codex/nvidia-proxy-model-catalog.json`).

### Default models

| Model ID | Display Name | Context Window |
|---|---|---|
| `z-ai/glm-5.2` | GLM 5.2 | 1,000,000 |
| `moonshotai/kimi-k2.6` | Kimi K2.6 | 256,000 |
| `deepseek-ai/deepseek-v4-pro` | DeepSeek V4 Pro | 1,000,000 |
| `deepseek-ai/deepseek-v4-flash` | DeepSeek V4 Flash | 1,000,000 |
| `qwen/qwen3.5-397b-a17b` | Qwen 3.5 397B | 256,000 |
| `meta/llama-3.3-70b-instruct` | Llama 3.3 70B | 128,000 |

Override via `[codex] models` in `config.toml` (format: `"model_id:display_name:context_window"`).

## Responses API Translation

Codex CLI uses `wire_api = "responses"` (OpenAI Responses API), but NVIDIA only implements `/v1/chat/completions`. The proxy bridges this gap:

```
Codex CLI                    Proxy                      NVIDIA API
   |                           |                            |
   |-- POST /v1/responses -->  |                            |
   |   (Responses format)      |                            |
   |                           |-- translate body -->        |
   |                           |-- POST /v1/chat/completions -->
   |                           |                            |
   |                           |<-- SSE (chat completions) --|
   |<-- SSE (responses)  <----|                            |
   |                           |                            |
```

- **Request translation**: `input` → `messages`, `instructions` → system message, flattens nested content, maps `max_output_tokens` → `max_tokens`.
- **Response translation (non-streaming)**: Chat completions response → Responses API JSON (`response.created`, `response.completed` wrapper).
- **Response translation (streaming SSE)**: Chat completions SSE chunks → Responses API events (`response.created`, `response.output_text.delta`, `response.output_text.done`, `response.completed`).
- **Detection**: Triggered when path is `/v1/responses` OR when body has `input` field without `messages` (cc-switch forwards to `/v1/chat/completions` without translating the body).

## Configuration

### Top-level fields

| Field | Description | Default |
|---|---|---|
| `listen_addr` | Address to listen on | Required |
| `base_url` | NVIDIA API base URL | Required |
| `keys` | List of NVIDIA API keys | Required (or `NVIDIA_KEYS` env) |

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

### `[codex]`

| Field | Description | Default |
|---|---|---|
| `enabled` | Auto-manage Codex config on startup | `true` |
| `default_model` | Default model for Codex | `z-ai/glm-5.2` |
| `model_reasoning_effort` | Reasoning effort: none/minimal/low/medium/high | `high` |
| `disable_response_storage` | Required for non-OpenAI backends | `true` |
| `bearer_token` | Dummy token for `experimental_bearer_token` | `proxy-managed` |
| `models` | Custom model list (overrides defaults) | `[]` |

## Metrics

Prometheus metrics are exposed at `observability.metrics_path` (default `/metrics`) when `observability.enable_metrics` is `true`.

- `proxy_requests_total` – total HTTP requests processed
- `proxy_requests_success_total` – 2xx requests
- `proxy_requests_errors_total` – non-2xx requests
- `proxy_request_duration_seconds` – latency histogram
- `proxy_key_active` – number of active keys
- `proxy_key_cooldown` – number of keys in cool-down
- `proxy_key_disabled` – number of disabled keys

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `{metrics_path}` | Prometheus metrics |
| GET | `/v1/models` | Model catalog (from generated catalog JSON) |
| GET | `/models` | Same as above (no `/v1` prefix) |
| GET | `/health` | Health check (returns 204) |
| * | `/*` | Proxied to NVIDIA API (with key injection + rate limiting) |

## Testing

```bash
cargo test --release
```

Current tests cover the `codex_config` module:
- Removing `[model_providers.custom]` sections (with following section, at end of string, not present)
- Replacing `[model_providers.nvidia-proxy]` sections
- Verified that Rust's `regex` crate does not support lookahead (`(?=...)`), so manual `find` logic is used instead

## License

MIT

## Acknowledgments

- Built with Tokio, Warp, reqwest, and regex.
- Responses API translation inspired by OpenAI's Responses API specification.
