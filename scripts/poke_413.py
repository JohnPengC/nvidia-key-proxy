#!/usr/bin/env python3
import json, sys, urllib.request, urllib.error

# Send a chat-completions request whose estimated prompt size exceeds the
# proxy's hard ceiling (request_max_prompt_tokens, default 90000) and assert the
# proxy returns HTTP 413 with the expected error body and does NOT call upstream.
#
# The estimate is Σ content chars / 4 (see message_content_tokens in proxy.rs),
# so ~360k chars content ⇒ ~90k est ⇒ 413. We send a comfy margin so `/4`
# rounding can't land under the threshold.
#
# No NVIDIA quota is burned: 413 is returned before any upstream call.

URL = "http://0.0.0.0:8888/v1/chat/completions"
TARGET_EST = 95000                       # well above the 90000 ceiling
CHARS = TARGET_EST * 4 + 4096           # one user message, plain string

body = json.dumps({
    "model": "z-ai/glm-5.2",
    "messages": [{"role": "user", "content": "x" * CHARS}],
}).encode()

print(f"sending content_chars={CHARS} (est ~= {CHARS // 4}) -> {URL}")
req = urllib.request.Request(URL, data=body, headers={"content-type": "application/json"}, method="POST")
try:
    resp = urllib.request.urlopen(req, timeout=10)
    print(f"UNEXPECTED: 2xx status={resp.status}")
    print(resp.read().decode()[:400])
    sys.exit(1)
except urllib.error.HTTPError as e:
    body_bytes = e.read()
    print(f"status: {e.code} (expect 413)")
    print(f"body : {body_bytes.decode()[:600]}")
    j = json.loads(body_bytes)
    err = j.get("error", {})
    checks = {
        "http_status_is_413": e.code == 413,
        "type=request_too_large": err.get("type") == "request_too_large",
        "code=prompt_tokens_exceeds_ceiling": err.get("code") == "prompt_tokens_exceeds_ceiling",
        "est_prompt_tokens_present": "est_prompt_tokens" in err,
        "limit_present": "limit" in err,
    }
    print("--- checks ---")
    ok = True
    for k, v in checks.items():
        print(f"  {('ok' if v else 'FAIL')}: {k}")
        ok = ok and v
    sys.exit(0 if ok else 1)
except Exception as e:
    print(f"ERROR connecting to proxy at {URL}: {e}")
    sys.exit(2)
