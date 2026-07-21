# cc-switch "格式转换错误: Empty choices array" 422 — 根因与补丁

## 根因（已用源码定位）

链路：`Claude Code CLI → cc-switch (Anthropic↔OpenAI 转换) → nvidia-app (8888, 透传) → NVIDIA`。

`Empty choices array` 这条错误**只**来自 cc-switch 的 `src-tauri/src/proxy/providers/transform.rs` 里 `openai_to_anthropic` 的非流式批量转换分支：

```rust
pub fn openai_to_anthropic(body: Value) -> Result<Value, ProxyError> {
    let choices = body
        .get("choices")
        .and_then(|c| c.as_array())
        .ok_or_else(|| ProxyError::TransformError("No choices in response".to_string()))?;
    let choice = choices
        .first()
        .ok_or_else(|| ProxyError::TransformError("Empty choices array".to_string()))?;
    ...
}
```

由此可分辨三种情况：

| 上游 NVIDIA 返回 | cc-switch 抛的错 | 含义 |
|---|---|---|
| `{"error":...}`（400 等，无 `choices` 键）| `No choices in response` | 上游错误体被当成正常响应去解析（`body.get("choices")` 为 None）|
| `{"choices":[]}`（200，数组为空）| **`Empty choices array`** ← 本次问题 | 数组存在但为空 |
| `{"choices":[{...}]}` 正常 | 不抛错 | 正常 |

判定走哪条转换路径的依据（`handlers.rs`）：

```rust
fn should_use_claude_transform_streaming(requested_streaming, upstream_is_sse, api_format, is_codex_oauth)
    -> bool {
    requested_streaming || upstream_is_sse || (is_codex_oauth && api_format == "openai_responses")
}
```

`openai_to_anthropic`（批量转换，会抛 `Empty choices array`）只在 `use_streaming == false` 时被调用；即 `use_streaming == false` 需要 **客户端发了 `stream:false` 且上游不是 SSE**。

Claude Code 普通对话都走 `stream:true`；**手动压缩上下文（context compaction）** 在多个版本里用 `stream:false` 发 summarize 请求——这与用户报告"手动压缩上下文也会报错"完全吻合。此刻 NVIDIA（`z-ai/glm-5.2`）对这次 compaction 返回 `200` 且 body 里 `choices: []`（典型的 usage-only/模型未产出 token），cc-switch 的批量转换 `choices.first() == None` → 抛 `Empty choices array` → 变成 422 透回 Claude Code。

## 佐证

- `~/.cc-switch/logs/cc-switch.log` 最近一次 422 在 `2026-07-21 15:02:06`，恰是那次会话里 Claude Code 自己被打断的时间点；紧邻的转发日志都是 `[Claude] >>> 请求 URL: http://127.0.0.1:8888/v1/chat/completions (model=z-ai/glm-5.2)`。
- `failure.log` 里**没有**一条上游 200 + `choices:[]` 的记录（代理只在失败路径写 failure.log），说明这是正常但空的 200，不是上游错误——符合"模型对 compaction prompt 未产出 token"。
- 来源仓库：`https://github.com/farion1231/cc-switch`（MIT，默认分支 `main`，Rust 后端在 `src-tauri/`）。本机只装了编译产物 `/Applications/CC Switch.app`，源码未克隆，因此下面这份补丁需要在用户自己 clone 的 cc-switch 里落地、`cargo build --release`、替换 app 里的二进制。

## 与 nvidia-app 代理的关系

- nvidia-app 是纯透传 OpenAI→OpenAI，**不**改写响应；本次 `choices:[]` 的 200 直接源自 NVIDIA，不是代理造的。
- nvidia-app 的 `stream_options.include_usage` 注入已正确按 `is_stream` 门控（`src/proxy.rs` 中 `if is_stream { ... }`），非流式请求**不会**被注入 usage 选项，所以代理不是 empty choices 的成因。
- nvidia-app 已在成功路径加了 empty-choices 诊断（`UsageTap`），会在"上游 200 但 choices 为空 JSON"时 `warn!`，便于以后抓证据。（本会话已修掉它的假阳性，见 commit。）

## 建议补丁（cc-switch，`src-tauri/src/proxy/providers/transform.rs`）

目的：让 `openai_to_anthropic` 不再在空 choices 上硬抛错——空 choices 且带 usage 时，返回一个空的 assistant message（保留 usage），不再置终端于 422。

```rust
pub fn openai_to_anthropic(body: Value) -> Result<Value, ProxyError> {
    // 新增：上游错误体（无 choices 键）直接透传成 Anthropic 错误，
    // 不要硬塞进 choices 解析路径。
    if body.get("choices").is_none() {
        if let Some(err) = body.get("error") {
            let message = err.get("message").and_then(|m| m.as_str())
                .unwrap_or("upstream error").to_string();
            return Err(ProxyError::TransformError(message));
        }
    }

    let choices = body
        .get("choices")
        .and_then(|c| c.as_array())
        .ok_or_else(|| ProxyError::TransformError("No choices in response".to_string()))?;

    // 修改：choices 为空但带 usage → 返回空 assistant message（保留 usage），
    // 不再抛 "Empty choices array"。这是 compaction 触发 422 的根因路径。
    let choice = if let Some(first) = choices.first() {
        first
    } else if body.get("usage").is_some() {
        // usage-only 响应：无 token 产出，但计费/上下文需要它成功返回。
        return Ok(serde_json::json!({
            "id": body.get("id").cloned().unwrap_or(serde_json::Value::Null),
            "type": "message",
            "role": "assistant",
            "model": body.get("model").cloned().unwrap_or(serde_json::Value::Null),
            "content": [],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": body["usage"]
                    .get("prompt_tokens").and_then(|t| t.as_u64()).unwrap_or(0),
                "output_tokens": body["usage"]
                    .get("completion_tokens").and_then(|t| t.as_u64()).unwrap_or(0),
            },
        }));
    } else {
        return Err(ProxyError::TransformError("Empty choices array".to_string()));
    };

    let message = choice
        .get("message")
        .ok_or_else(|| ProxyError::TransformError("No message in choice".to_string()))?;
    // ……原有转换逻辑保持不变……
}
```

要点：
1. **错误体短路**：`choices` 键缺失且存在 `error` 时，把上游错误透传成转换错误，便于排查，而不是泛泛的 `No choices in response`。
2. **usage-only 空返回兜底**：`choices:[]` 且有 `usage` → 返回一个空的 assistant message（`content: []`、`stop_reason: "end_turn"`），保留 `input_tokens`/`output_tokens`。这是消除 422 的关键。
3. 只在批处理（非流式）路径；流式路径（`create_anthropic_sse_stream`）已按 chunk 增量处理，不受影响。

> 注意：`serde_json::json!` 宏是否在 cc-switch 该模块可用、字段名是否与 cc-switch 的 Anthropic 输出结构完全一致，需对照其 `AnthropicResponse`/`Message` 结构体微调字段（例如是否有 `usage.input_tokens` vs `prompt_tokens` 这种别名）。补丁仅是结构性示意，落地时以仓库实际类型为准。

## 落地步骤

1. `git clone https://github.com/farion1231/cc-switch.git` 到本机开发目录。
2. 在 `src-tauri/src/proxy/providers/transform.rs` 应用上述补丁。
3. 加 1～2 个单元测试：构造 `{"choices":[],"usage":{...}}` 输入，断言 `openai_to_anthropic` 返回空 message 而非 `Err`；构造 `{"error":...}` 断言透传为转换错误。
4. `cd src-tauri && cargo build --release && cargo test`。
5. 用产物替换 `/Applications/CC Switch.app/Contents/MacOS/cc-switch`（保留原二进制为备份），或直接用 `pnpm tauri dev` 在本地跑一个 dev 版验证后再发布。
6. 重启 Claude Code 与 cc-switch，重跑触发过 422 的对话 + 手动压缩上下文动作，确认 `~/.cc-switch/logs/cc-switch.log` 不再出现 `Empty choices array`。

## 已落地：代理侧限距窥探改写（无需改 cc-switch）

经实测设计后**已在 nvidia-app 实现**，无需改动 cc-switch 即可消除 422。见 `src/proxy.rs::Proxy::handle_success_maybe_synthesize` + `respond_synthesized_empty_choices`，配置开关 `synthesize_empty_choices`（默认开，env `NVIDIA_SYNTHESIZE_EMPTY_CHOICES=false` 关闭）。

机制（只碰非流式成功分支，流式零拷贝不动）：

```
非流式 200  →  缓冲 ≤ EMPTY_CHOICES_PEEK_BYTES(64KiB)  →  逐块解析 JSON
   ├─ 解析成 JSON 且 choices 为空数组：
   │     注入 choices:[{index:0,message:{role:"assistant",content:""},finish_reason:"stop"}]
   │     保留 id/model/usage → 作为带 Content-Length 的缓冲 JSON 返回（warn 一行）
   └─ 其他：把已读字节 + 剩余流 串接 → 透传（与原 handle_passthrough_stream 字节一致，UsageTap 照常）
```

关键风险点都用机制收住：
- **503 ResourceExhausted**：原避免机制是「不缓冲成功体」「下游慢、worker 被占住」会 503。本改把**最多 64KiB** 掐死，空 choices 体一律远小于此；正常/大响应在遇到非空 choices 或超 64KiB 时立刻转透传，前缀字节立刻转发，不再 hold。
- **gzip/二进制体**：`std::str::from_utf8` 失败 → 直接转透传，不误判、不误改。
- **伪造产出**：`content:""` 只在「上游本就没产出 token（choices:[]）」时注入，语义上等价于「模型未产出」，与上游 cc-switch 建议补丁产出的结构一致，不算无中生有。
- cc-switch 转换只读 `choices / message / id / model / usage`，注入体满足其全部读取，会产出一个 `content:[]`、`stop_reason: end_turn`、保留 usage 的合法 Anthropic message，不再抛 `Empty choices array`。

测试（`cargo test` 全绿）：`non_stream_empty_choices_is_synthesized_to_legal_body` / `non_stream_normal_response_passes_through_unchanged` / `synthesize_disabled_forwards_empty_choices_unchanged`。

## 仍走 cc-switch 补丁（可选，长期更干净）

下面是 cc-switch 侧 `transform::openai_to_anthropic` 的结构性补丁，作为「长期正解」留存——代理侧改写属于代理替 cc-switch 兜一跳，上游打补丁能省掉这一跳、也更符合"谁抛错谁兜底"。仓库 `https://github.com/farion1231/cc-switch`（MIT）。


