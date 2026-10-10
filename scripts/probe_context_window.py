#!/usr/bin/env python3
"""Probe the real context-window boundary of a model on integrate.api.nvidia.com.

Bypasses the local proxy entirely (talks to upstream directly with a raw key) so
the measurement is the *model's* limit, not the proxy's est-space ceiling.

Strategy: send single-message prompts of increasing synthetic length
(repeated ASCII filler, ~4 chars/token under the proxy's estimator, but what
matters here is the upstream's own tokenizer), with max_tokens=1 so each probe
costs ~1 output token. Watch for the first non-200 (4xx "context length", empty
shell, etc.) vs 200.

Cost control: lengths ascend; stop at the first definitive failure. Each probe
is one upstream call; input tokens are billed but unavoidable for the
measurement — keep the ladder short and stop early.

Usage:
  python3 scripts/probe_context_window.py z-ai/glm-5.3-flash
  python3 scripts/probe_context_window.py z-ai/glm-5.3-flash 150000 250000 350000
"""

import json
import sys
import time
import urllib.request
import urllib.error

BASE = "https://integrate.api.nvidia.com/v1/chat/completions"


def load_key() -> str:
    # Read the first key from config.toml without a TOML parser dependency:
    # keys are listed as quoted strings under `keys = [ ... ]`.
    with open("config.toml") as f:
        text = f.read()
    start = text.index("keys = [")
    end = text.index("]", start)
    seg = text[start:end]
    first = seg.split('"')[1]  # first quoted string after `keys = [`
    return first


def probe(key: str, model: str, approx_tokens: int) -> tuple[int, str]:
    # ASCII filler at ~4 chars/token → approx_tokens*4 chars. The upstream
    # tokenizer decides the real count; we label probes by our approximation
    # and record the API's own usage.prompt_tokens in the result.
    filler = "x" * (approx_tokens * 4)
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": filler}],
            "max_tokens": 1,
            "stream": False,
        }
    ).encode()
    req = urllib.request.Request(
        BASE,
        data=body,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            payload = json.loads(resp.read())
            usage = payload.get("usage") or {}
            pt = usage.get("prompt_tokens")
            # Detect the empty-shell degenerate case: HTTP 200 but no choices.
            n_choices = len(payload.get("choices") or [])
            tag = "OK" if n_choices else "EMPTY_SHELL"
            return resp.status, f"{tag} prompt_tokens={pt} choices={n_choices}"
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:200]
        return e.code, f"HTTPError: {detail}"
    except Exception as e:
        return -1, f"error: {e}"
    finally:
        print(f"  ({time.time() - t0:.1f}s)", file=sys.stderr)


def main() -> int:
    model = sys.argv[1] if len(sys.argv) > 1 else "z-ai/glm-5.3-flash"
    ladder = (
        [int(x) for x in sys.argv[2:]]
        if len(sys.argv) > 2
        else [150_000, 250_000, 350_000, 500_000, 750_000, 1_000_000]
    )
    key = load_key()
    print(f"probing model={model} ladder={ladder} max_tokens=1 (direct to upstream)")
    for n in ladder:
        status, info = probe(key, model, n)
        print(f"approx_tokens={n:>9,}  ->  status={status}  {info}")
        # Stop conditions: definitive rejection (4xx/5xx) or empty shell.
        # 429 = rate limited → do not treat as a boundary; wait and continue.
        if status == 200 and info.startswith("OK"):
            continue
        if status == 429:
            print("  rate-limited; sleeping 30s and retrying this rung once")
            time.sleep(30)
            status, info = probe(key, model, n)
            print(f"approx_tokens={n:>9,}  ->  status={status}  {info}")
        if status == 200 and info.startswith("OK"):
            continue
        print("  boundary reached; stopping ladder")
        return 0
    print("ladder exhausted without a boundary hit; model accepts >= top rung")
    return 0


if __name__ == "__main__":
    sys.exit(main())
