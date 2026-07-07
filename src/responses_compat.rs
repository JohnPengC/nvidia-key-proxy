//! Responses API ↔ Chat Completions API compatibility layer.
//!
//! Codex CLI only supports `wire_api = "responses"`, but NVIDIA's API
//! only implements `/v1/chat/completions`. This module translates
//! between the two formats so the proxy can serve as a Codex backend.

use bytes::Bytes;
use serde_json::{json, Value};
use tracing::debug;

/// True if the incoming path should be handled as a Responses API request.
pub fn is_responses_path(path: &str) -> bool {
    // Match /v1/responses exactly (no trailing segment).
    path == "/v1/responses" || path == "/responses"
}

/// Convert a Responses API request body to a Chat Completions request body.
///
/// Returns the translated JSON body and the upstream path to send it to.
pub fn translate_request(body: &Bytes) -> Result<(String, Vec<u8>), String> {
    let req: Value = serde_json::from_slice(body)
        .map_err(|e| format!("responses_compat: failed to parse request body: {}", e))?;

    let mut chat_req = json!({
        "stream": req.get("stream").cloned().unwrap_or(Value::Bool(true)),
    });

    // model
    if let Some(model) = req.get("model") {
        chat_req["model"] = model.clone();
    }

    // instructions → system message
    // input (string or array) → messages
    let mut messages: Vec<Value> = Vec::new();

    // Top-level instructions → system message
    if let Some(instructions) = req.get("instructions") {
        if !instructions.is_null() {
            let instr_text = match instructions {
                Value::String(s) => s.clone(),
                _ => instructions.to_string(),
            };
            if !instr_text.is_empty() {
                messages.push(json!({
                    "role": "system",
                    "content": instr_text,
                }));
            }
        }
    }

    // input can be a string or an array of items
    match req.get("input") {
        Some(Value::String(s)) => {
            messages.push(json!({
                "role": "user",
                "content": s,
            }));
        }
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(item_type) = item.get("type").and_then(|t| t.as_str()) {
                    match item_type {
                        "message" => {
                            // {type:"message", role:"user"/"assistant"/"system", content: ...}
                            let role = item.get("role").and_then(|r| r.as_str()).unwrap_or("user");
                            let content = item.get("content").cloned().unwrap_or(Value::Null);
                            // content can be a string or an array of content blocks
                            let content_text = match &content {
                                Value::String(s) => s.clone(),
                                Value::Array(blocks) => {
                                    // Extract text from content blocks
                                    let mut texts = Vec::new();
                                    for block in blocks {
                                        if let Some(t) = block.get("text").and_then(|t| t.as_str())
                                        {
                                            texts.push(t.to_string());
                                        } else if let Some(t) =
                                            block.get("content").and_then(|t| t.as_str())
                                        {
                                            texts.push(t.to_string());
                                        }
                                    }
                                    texts.join("\n")
                                }
                                _ => content.to_string(),
                            };
                            messages.push(json!({
                                "role": role,
                                "content": content_text,
                            }));
                        }
                        "function_call" => {
                            // Previous function call from the assistant → assistant message with tool_calls
                            let name = item.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let arguments = item
                                .get("arguments")
                                .and_then(|a| a.as_str())
                                .unwrap_or("{}");
                            let call_id =
                                item.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
                            messages.push(json!({
                                "role": "assistant",
                                "content": Value::Null,
                                "tool_calls": [{
                                    "id": call_id,
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": arguments,
                                    }
                                }]
                            }));
                        }
                        "function_call_output" => {
                            // Tool result → tool message
                            let call_id =
                                item.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
                            let output = item
                                .get("output")
                                .cloned()
                                .unwrap_or(Value::String(String::new()));
                            let output_text = match &output {
                                Value::String(s) => s.clone(),
                                _ => output.to_string(),
                            };
                            messages.push(json!({
                                "role": "tool",
                                "tool_call_id": call_id,
                                "content": output_text,
                            }));
                        }
                        "reasoning" => {
                            // Skip reasoning items — not relevant for chat completions
                        }
                        _ => {
                            // Unknown item type, skip
                            debug!("responses_compat: unknown input item type: {}", item_type);
                        }
                    }
                } else {
                    // No type field — treat as a message if it has role/content
                    if item.get("role").is_some() {
                        messages.push(item.clone());
                    }
                }
            }
        }
        None => {
            // No input field — empty messages
        }
        _ => {}
    }

    chat_req["messages"] = Value::Array(messages);

    // max_output_tokens → max_tokens
    if let Some(max_tokens) = req.get("max_output_tokens") {
        chat_req["max_tokens"] = max_tokens.clone();
    }

    // temperature, top_p, stop — passthrough
    for key in ["temperature", "top_p", "stop"] {
        if let Some(v) = req.get(key) {
            chat_req[key] = v.clone();
        }
    }

    // tools (flat) → tools (nested under function key)
    if let Some(tools) = req.get("tools") {
        if let Some(tools_arr) = tools.as_array() {
            let chat_tools: Vec<Value> = tools_arr
                .iter()
                .filter_map(|tool| {
                    let tool_type = tool
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("function");
                    if tool_type == "function" {
                        // Already might have a name/description/parameters or a nested function object
                        if let Some(func) = tool.get("function") {
                            return Some(json!({"type": "function", "function": func}));
                        }
                        // Flat: name, description, parameters at top level
                        let name = tool
                            .get("name")
                            .cloned()
                            .unwrap_or(Value::String(String::new()));
                        let description = tool
                            .get("description")
                            .cloned()
                            .unwrap_or(Value::String(String::new()));
                        let parameters = tool.get("parameters").cloned().unwrap_or(json!({}));
                        return Some(json!({
                            "type": "function",
                            "function": {
                                "name": name,
                                "description": description,
                                "parameters": parameters,
                            }
                        }));
                    }
                    None // Skip non-function tools (web_search, etc.) — not supported by NVIDIA
                })
                .collect();
            if !chat_tools.is_empty() {
                chat_req["tools"] = Value::Array(chat_tools);
            }
        }
    }

    // tool_choice — passthrough
    if let Some(tc) = req.get("tool_choice") {
        chat_req["tool_choice"] = tc.clone();
    }

    // reasoning — drop (NVIDIA models don't support reasoning config in chat completions)

    let body_bytes = serde_json::to_vec(&chat_req).unwrap_or_default();
    Ok(("/v1/chat/completions".to_string(), body_bytes))
}

/// Convert a non-streaming Chat Completions response to Responses API format.
pub fn translate_response(body: &[u8], model: &str) -> Vec<u8> {
    let chat_resp: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            debug!("responses_compat: failed to parse chat response: {}", e);
            return body.to_vec();
        }
    };

    let mut output: Vec<Value> = Vec::new();

    if let Some(choices) = chat_resp.get("choices").and_then(|c| c.as_array()) {
        if let Some(choice) = choices.first() {
            // Handle message content → output message
            if let Some(message) = choice.get("message") {
                let role = message
                    .get("role")
                    .and_then(|r| r.as_str())
                    .unwrap_or("assistant");
                let content = message.get("content");

                // Handle tool calls first (if any)
                if let Some(tool_calls) = message.get("tool_calls").and_then(|tc| tc.as_array()) {
                    for tc in tool_calls {
                        let id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("");
                        let func = tc.get("function").unwrap_or(&Value::Null);
                        let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("");
                        let arguments = func
                            .get("arguments")
                            .and_then(|a| a.as_str())
                            .unwrap_or("{}");
                        output.push(json!({
                            "type": "function_call",
                            "id": id,
                            "call_id": id,
                            "name": name,
                            "arguments": arguments,
                        }));
                    }
                }

                // Handle content
                if let Some(content_val) = content {
                    if !content_val.is_null() {
                        let text = match content_val {
                            Value::String(s) => s.clone(),
                            _ => content_val.to_string(),
                        };
                        if !text.is_empty() {
                            output.push(json!({
                                "type": "message",
                                "role": role,
                                "content": [{
                                    "type": "output_text",
                                    "text": text,
                                }]
                            }));
                        }
                    }
                }

                // Handle refusal
                if let Some(refusal) = message.get("refusal").and_then(|r| r.as_str()) {
                    output.push(json!({
                        "type": "refusal",
                        "refusal": refusal,
                    }));
                }
            }

            // Handle finish_reason
            // (not strictly needed for the output array, but could map to status)
        }
    }

    // Build usage
    let mut usage = json!({});
    if let Some(u) = chat_resp.get("usage") {
        if let Some(pt) = u.get("prompt_tokens") {
            usage["input_tokens"] = pt.clone();
        }
        if let Some(ct) = u.get("completion_tokens") {
            usage["output_tokens"] = ct.clone();
        }
        if let Some(tt) = u.get("total_tokens") {
            usage["total_tokens"] = tt.clone();
        }
    }

    let resp_id = format!("resp_{}", chrono_like_id());
    let created = chat_resp
        .get("created")
        .and_then(|c| c.as_i64())
        .unwrap_or(0);

    let response = json!({
        "id": resp_id,
        "object": "response",
        "created_at": created,
        "model": model,
        "output": output,
        "status": "completed",
        "usage": usage,
    });

    serde_json::to_vec(&response).unwrap_or_else(|_| body.to_vec())
}

/// Convert a streaming Chat Completions SSE chunk to Responses API SSE events.
///
/// Returns a list of SSE event strings to emit. Each is a complete
/// "data: {...}\n\n" block. The caller is responsible for writing them
/// to the SSE stream.
pub fn translate_stream_chunk(chunk: &[u8], state: &mut StreamState, model: &str) -> Vec<String> {
    // chunk is a single SSE "data: {...}" line without the "data: " prefix
    let chat_chunk: Value = match serde_json::from_slice(chunk) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let mut events = Vec::new();

    // Check for [DONE]
    if chat_chunk.is_null() {
        // Emit response.completed
        if !state.emitted_completed {
            let completed = json!({
                "type": "response.completed",
                "response": {
                    "id": state.response_id,
                    "object": "response",
                    "model": model,
                    "status": "completed",
                    "output": state.accumulated_output.clone(),
                }
            });
            events.push(format!("data: {}\n\n", completed));
            state.emitted_completed = true;
        }
        return events;
    }

    // First chunk: emit response.created + response.output_item.added
    if !state.emitted_created {
        let created = json!({
            "type": "response.created",
            "response": {
                "id": state.response_id.clone(),
                "object": "response",
                "status": "in_progress",
                "model": model,
            }
        });
        events.push(format!("data: {}\n\n", created));

        // Emit output_item.added so Codex creates an active item
        // This is required before sending any output_text.delta events.
        let item_added = json!({
            "type": "response.output_item.added",
            "item": {
                "type": "message",
                "id": state.message_item_id,
                "role": "assistant",
                "status": "in_progress",
                "content": [],
            }
        });
        events.push(format!("data: {}\n\n", item_added));

        state.emitted_created = true;
    }

    // Process choices
    if let Some(choices) = chat_chunk.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            let delta = match choice.get("delta") {
                Some(d) => d,
                None => continue,
            };

            // Text content delta
            if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    state.accumulated_text.push_str(content);
                    let event = json!({
                        "type": "response.output_text.delta",
                        "delta": content,
                    });
                    events.push(format!("data: {}\n\n", event));
                }
            }

            // Tool call delta
            if let Some(tool_calls) = delta.get("tool_calls").and_then(|tc| tc.as_array()) {
                for tc in tool_calls {
                    let index = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;
                    let func = tc.get("function").unwrap_or(&Value::Null);
                    let arguments_delta =
                        func.get("arguments").and_then(|a| a.as_str()).unwrap_or("");

                    // Accumulate tool call
                    if index >= state.tool_calls.len() {
                        state.tool_calls.push(ToolCallAccum {
                            id: tc
                                .get("id")
                                .and_then(|i| i.as_str())
                                .unwrap_or("")
                                .to_string(),
                            name: func
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string(),
                            arguments: String::new(),
                        });
                    }

                    if !arguments_delta.is_empty() {
                        state.tool_calls[index].arguments.push_str(arguments_delta);
                    }

                    // Emit function_call_arguments.delta
                    let event = json!({
                        "type": "response.function_call_arguments.delta",
                        "item_id": state.tool_calls[index].id,
                        "delta": arguments_delta,
                    });
                    events.push(format!("data: {}\n\n", event));
                }
            }

            // Finish reason
            let finish_reason = choice.get("finish_reason").and_then(|f| f.as_str());
            if let Some(reason) = finish_reason {
                if reason == "tool_calls" {
                    // Emit function_call items for each accumulated tool call
                    for tc in &state.tool_calls {
                        let event = json!({
                            "type": "response.output_item.done",
                            "item": {
                                "type": "function_call",
                                "id": tc.id,
                                "call_id": tc.id,
                                "name": tc.name,
                                "arguments": tc.arguments,
                                "status": "completed",
                            }
                        });
                        events.push(format!("data: {}\n\n", event));
                        state.accumulated_output.push(json!({
                            "type": "function_call",
                            "id": tc.id,
                            "call_id": tc.id,
                            "name": tc.name,
                            "arguments": tc.arguments,
                        }));
                    }
                } else if reason == "stop" && !state.accumulated_text.is_empty() {
                    // Emit output_text.done
                    let event = json!({
                        "type": "response.output_text.done",
                        "text": state.accumulated_text.clone(),
                    });
                    events.push(format!("data: {}\n\n", event));

                    // Emit output_item.done to close the message item
                    let item_done = json!({
                        "type": "response.output_item.done",
                        "item": {
                            "type": "message",
                            "id": state.message_item_id,
                            "role": "assistant",
                            "status": "completed",
                            "content": [{
                                "type": "output_text",
                                "text": state.accumulated_text.clone(),
                            }]
                        }
                    });
                    events.push(format!("data: {}\n\n", item_done));

                    // Add message item to accumulated output
                    state.accumulated_output.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": [{
                            "type": "output_text",
                            "text": state.accumulated_text.clone(),
                        }]
                    }));
                }
            }
        }
    }

    events
}

/// State for streaming translation, held across chunks.
#[derive(Default)]
pub struct StreamState {
    pub response_id: String,
    pub message_item_id: String,
    pub emitted_created: bool,
    pub emitted_completed: bool,
    pub accumulated_text: String,
    pub accumulated_output: Vec<Value>,
    pub tool_calls: Vec<ToolCallAccum>,
}

impl StreamState {
    pub fn new() -> Self {
        let mut state = StreamState::default();
        let ts = chrono_like_id();
        state.response_id = format!("resp_{}", ts);
        state.message_item_id = format!("msg_{}", ts);
        state
    }
}

#[derive(Clone)]
pub struct ToolCallAccum {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Generate a timestamp-like ID for response objects.
fn chrono_like_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}", now)
}

#[cfg(test)]
mod streaming_behavior_tests {
    // Streaming behavior tests: emits created/delta/completed on normal stream, no completed on abort.

    use super::{translate_stream_chunk, StreamState};
    use serde_json::Value;

    fn drive(upstream_chunks: &[String], upstream_aborted: bool, model: &str) -> Vec<Value> {
        let mut state = StreamState::new();
        let mut buffer = String::new();
        let mut out: Vec<Value> = Vec::new();
        for chunk in upstream_chunks {
            buffer.push_str(chunk);
            while let Some(pos) = buffer.find("\n\n") {
                let block = buffer[..pos].to_string();
                buffer = buffer[pos + 2..].to_string();
                if let Some(line) = block.lines().find(|l| l.starts_with("data: ")) {
                    let data = &line[6..];
                    let payload: &[u8] = if data == "[DONE]" {
                        b"null"
                    } else {
                        data.as_bytes()
                    };
                    for ev in translate_stream_chunk(payload, &mut state, model) {
                        if let Some(json_part) = ev
                            .strip_prefix("data: ")
                            .and_then(|s| s.strip_suffix("\n\n"))
                        {
                            if let Ok(v) = serde_json::from_str::<Value>(json_part) {
                                out.push(v);
                            }
                        }
                    }
                }
            }
        }
        if !upstream_aborted && !state.emitted_completed {
            for ev in translate_stream_chunk(b"null", &mut state, model) {
                if let Some(json_part) = ev
                    .strip_prefix("data: ")
                    .and_then(|s| s.strip_suffix("\n\n"))
                {
                    if let Ok(v) = serde_json::from_str::<Value>(json_part) {
                        out.push(v);
                    }
                }
            }
        }
        out
    }

    fn types_of(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .filter_map(|v| v.get("type").and_then(|t| t.as_str()).map(String::from))
            .collect()
    }

    fn data(s: &str) -> String {
        format!("data: {}\n\n", s)
    }

    #[test]
    fn normal_stream_emits_created_deltas_and_completed() {
        let chunks = vec![
            data(r#"{"choices":[{"delta":{"content":"Hel"}}]}"#),
            "data: ".to_string(),
            r#"{"choices":[{"delta":{"content":"lo"}}]}"#.to_string(),
            "\n\n".to_string(),
            data(r#"{"choices":[{"delta":{"content":" world"}}]}"#),
            data("[DONE]"),
        ];

        let events = drive(&chunks, false, "");
        let types = types_of(&events);

        assert!(
            types.iter().any(|t| *t == "response.created"),
            "{:?}",
            types
        );
        assert!(
            types
                .iter()
                .filter(|t| **t == *"response.output_text.delta")
                .count()
                >= 3,
            "{:?}",
            types
        );
        assert!(
            types.iter().any(|t| *t == "response.completed"),
            "{:?}",
            types
        );
    }

    #[test]
    fn aborted_stream_does_not_emit_completed() {
        let chunks = vec![data(r#"{"choices":[{"delta":{"content":"partial"}}]}"#)];
        let events = drive(&chunks, true, "");
        let types = types_of(&events);

        assert!(
            !types.iter().any(|t| *t == "response.completed"),
            "aborted stream must NOT emit response.completed; got {:?}",
            types
        );
        assert!(
            types.iter().any(|t| *t == "response.created"),
            "{:?}",
            types
        );
        assert!(
            types.iter().any(|t| *t == "response.output_text.delta"),
            "{:?}",
            types
        );
    }

    #[test]
    fn model_is_propagated_into_created_and_completed_events() {
        // drive with a non-empty model and assert the created/completed events
        // carry that model string (regression guard for the previously-empty
        // `model` field in translated Responses events).
        let chunks = vec![
            data(r#"{"choices":[{"delta":{"content":"hi"}}]}"#),
            data("[DONE]"),
        ];
        let events = drive(&chunks, false, "z-ai/glm-5.2");

        let created = events
            .iter()
            .find(|v| v.get("type").and_then(|t| t.as_str()) == Some("response.created"));
        let completed = events
            .iter()
            .find(|v| v.get("type").and_then(|t| t.as_str()) == Some("response.completed"));

        let created_model = created
            .and_then(|v| v.get("response"))
            .and_then(|r| r.get("model"))
            .and_then(|m| m.as_str());
        let completed_model = completed
            .and_then(|v| v.get("response"))
            .and_then(|r| r.get("model"))
            .and_then(|m| m.as_str());

        assert_eq!(created_model, Some("z-ai/glm-5.2"), "created model missing");
        assert_eq!(
            completed_model,
            Some("z-ai/glm-5.2"),
            "completed model missing"
        );
    }
}

#[cfg(test)]
mod translate_request_tests {
    use super::translate_request;
    use bytes::Bytes;

    #[test]
    fn invalid_json_returns_error() {
        let body = Bytes::from_static(b"not json");
        assert!(translate_request(&body).is_err());
    }

    #[test]
    fn valid_responses_request_returns_chat_completions() {
        let body =
            Bytes::from_static(br#"{"model":"z-ai/glm-5.2","input":"hello","stream":false}"#);
        let (path, bytes) = translate_request(&body).expect("valid JSON should translate");
        assert_eq!(path, "/v1/chat/completions");
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["model"], "z-ai/glm-5.2");
        assert_eq!(json["stream"], false);
        assert!(json["messages"].is_array());
    }
}
