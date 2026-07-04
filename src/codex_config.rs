//! Codex CLI configuration auto-management.
//!
//! Replaces cc-switch's role for the Codex use case: on proxy startup,
//! generates `~/.codex/config.toml` and `~/.codex/model-catalog.json` so
//! Codex CLI can use this proxy as its backend without any intermediary.
//!
//! Key difference from cc-switch: `model_catalog_json` is always written as
//! an **absolute path**, fixing the `AbsolutePathBuf deserialized without a
//! base path` error that cc-switch's relative-path approach triggers on
//! newer Codex versions.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Default models to expose to Codex (curated for coding).
/// Users can override via [codex] models in config.toml.
const DEFAULT_CODEX_MODELS: &[(&str, &str, u64)] = &[
    // (model_id, display_name, context_window)
    ("z-ai/glm-5.2", "GLM 5.2", 1_000_000),
    ("moonshotai/kimi-k2.6", "Kimi K2.6", 256_000),
    ("deepseek-ai/deepseek-v4-pro", "DeepSeek V4 Pro", 1_000_000),
    ("deepseek-ai/deepseek-v4-flash", "DeepSeek V4 Flash", 1_000_000),
    ("qwen/qwen3.5-397b-a17b", "Qwen 3.5 397B", 256_000),
    ("meta/llama-3.3-70b-instruct", "Llama 3.3 70B", 128_000),
];

/// Provider ID used in Codex config.toml [model_providers] section.
const PROVIDER_ID: &str = "nvidia-proxy";

/// Catalog filename (lives in ~/.codex/).
const CATALOG_FILENAME: &str = "nvidia-proxy-model-catalog.json";

/// Configuration for the Codex integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexConfig {
    /// Enable auto-management of ~/.codex/config.toml on startup.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Default model to use when Codex starts.
    #[serde(default = "default_model")]
    pub default_model: String,

    /// Model reasoning effort: none, minimal, low, medium, high.
    #[serde(default = "default_reasoning_effort")]
    pub model_reasoning_effort: String,

    /// Disable response storage (required for non-OpenAI backends).
    #[serde(default = "default_true")]
    pub disable_response_storage: bool,

    /// Models to expose in the catalog.
    /// Each entry: "model_id:display_name:context_window"
    /// If empty, uses DEFAULT_CODEX_MODELS.
    #[serde(default)]
    pub models: Vec<String>,

    /// Auth token to put in experimental_bearer_token.
    /// Codex requires this field but we set it to a dummy value
    /// since the proxy injects real keys.
    #[serde(default = "default_bearer_token")]
    pub bearer_token: String,
}

fn default_true() -> bool {
    true
}
fn default_model() -> String {
    "z-ai/glm-5.2".to_string()
}
fn default_reasoning_effort() -> String {
    "high".to_string()
}
fn default_bearer_token() -> String {
    "proxy-managed".to_string()
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_model: default_model(),
            model_reasoning_effort: default_reasoning_effort(),
            disable_response_storage: true,
            models: Vec::new(),
            bearer_token: default_bearer_token(),
        }
    }
}

/// Get the ~/.codex directory path.
fn codex_config_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".codex")
    } else {
        PathBuf::from(".codex")
    }
}

/// Get the catalog file path.
fn catalog_path() -> PathBuf {
    codex_config_dir().join(CATALOG_FILENAME)
}

/// Get the config.toml path.
fn config_toml_path() -> PathBuf {
    codex_config_dir().join("config.toml")
}

/// Parse user-provided model strings.
/// Format: "model_id:display_name:context_window" or just "model_id".
fn parse_user_models(models: &[String]) -> Vec<(String, String, u64)> {
    let mut result = Vec::new();
    for entry in models {
        let parts: Vec<&str> = entry.splitn(3, ':').collect();
        let model_id = parts[0].trim().to_string();
        if model_id.is_empty() {
            continue;
        }
        let display_name = parts
            .get(1)
            .filter(|s| !s.is_trim_empty())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| model_id.clone());
        let context_window = parts
            .get(2)
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(128_000);
        result.push((model_id, display_name, context_window));
    }
    result
}

trait StrTrimExt {
    fn is_trim_empty(&self) -> bool;
}
impl StrTrimExt for str {
    fn is_trim_empty(&self) -> bool {
        self.trim().is_empty()
    }
}

/// Build the list of models to put in the catalog.
fn build_model_list(config: &CodexConfig) -> Vec<(String, String, u64)> {
    let user_models = parse_user_models(&config.models);
    if !user_models.is_empty() {
        user_models
    } else {
        DEFAULT_CODEX_MODELS
            .iter()
            .map(|(m, d, c)| (m.to_string(), d.to_string(), *c))
            .collect()
    }
}

/// Load the gpt-5.5 template from Codex's models_cache.json.
/// This gives us the full base_instructions and all required catalog fields.
fn load_template_from_cache() -> Option<Value> {
    let cache_path = codex_config_dir().join("models_cache.json");
    if !cache_path.exists() {
        return None;
    }
    let text = fs::read_to_string(&cache_path).ok()?;
    let cache: Value = serde_json::from_str(&text).ok()?;
    cache
        .get("models")
        .and_then(|m| m.as_array())
        .and_then(|models| {
            models
                .iter()
                .find(|m| m.get("slug").and_then(|s| s.as_str()) == Some("gpt-5.5"))
        })
        .cloned()
}

/// Generate a model catalog entry from the gpt-5.5 template, customizing
/// slug/display_name/context_window.
fn make_catalog_entry(template: &Value, model_id: &str, display_name: &str, context_window: u64, priority: usize) -> Value {
    let mut entry = template.clone();
    let obj = entry.as_object_mut().expect("template must be object");

    // Override identity fields
    obj.insert("slug".to_string(), json!(model_id));
    obj.insert("model".to_string(), json!(model_id));
    obj.insert("display_name".to_string(), json!(display_name));
    obj.insert("description".to_string(), json!(display_name));

    // Override context window
    obj.insert("context_window".to_string(), json!(context_window));
    obj.insert("max_context_window".to_string(), json!(context_window));

    // Set priority (1000+ to sort below built-in models)
    obj.insert("priority".to_string(), json!(1000 + priority));

    // Clear speed tiers
    obj.insert("additional_speed_tiers".to_string(), json!([]));
    obj.insert("service_tiers".to_string(), json!([]));
    obj.insert("availability_nux".to_string(), Value::Null);
    obj.insert("upgrade".to_string(), Value::Null);

    entry
}

/// Generate the full model catalog JSON.
fn generate_catalog(config: &CodexConfig) -> Value {
    let models = build_model_list(config);
    let template = load_template_from_cache();

    let entries: Vec<Value> = models
        .iter()
        .enumerate()
        .map(|(i, (model_id, display_name, ctx))| {
            if let Some(ref tpl) = template {
                make_catalog_entry(tpl, model_id, display_name, *ctx, i)
            } else {
                // Fallback: minimal entry without gpt-5.5 template
                json!({
                    "slug": model_id,
                    "model": model_id,
                    "display_name": display_name,
                    "description": display_name,
                    "context_window": ctx,
                    "max_context_window": ctx,
                    "priority": 1000 + i,
                    "additional_speed_tiers": [],
                    "service_tiers": [],
                    "availability_nux": null,
                    "upgrade": null,
                    "default_reasoning_level": "medium",
                    "default_reasoning_summary": "none",
                    "default_verbosity": "low",
                    "input_modalities": ["text"],
                    "apply_patch_tool_type": "freeform",
                    "shell_type": "shell_command",
                    "base_instructions": "",
                })
            }
        })
        .collect();

    json!({ "models": entries })
}

/// Generate the Codex config.toml content, preserving user sections.
///
/// Strategy: read existing config.toml, then use regex to update only the
/// fields we manage (model_provider, model, model_reasoning_effort,
/// disable_response_storage, model_catalog_json, and the [model_providers.X]
/// section). User-defined sections (notify, mcp_servers, marketplaces,
/// features, projects, plugins) are preserved untouched.
fn generate_config_toml(config: &CodexConfig, proxy_port: u16) -> String {
    let catalog = catalog_path();
    let catalog_str = catalog.to_string_lossy();
    let existing = fs::read_to_string(config_toml_path()).unwrap_or_default();

    // Our managed provider section
    let provider_section = format!(
        r#"[model_providers.{PROVIDER_ID}]
name = "NVIDIA Proxy"
base_url = "http://localhost:{port}/v1"
wire_api = "responses"
requires_openai_auth = true
experimental_bearer_token = "{token}"
"#,
        port = proxy_port,
        token = config.bearer_token,
    );

    // Check if existing config has our provider section
    let _has_provider = existing.contains(&format!("[model_providers.{PROVIDER_ID}]"));

    // Check if existing config has a [model_providers] section at all
    let _has_model_providers_header = existing.contains("[model_providers]")
        || existing.contains("[model_providers.");

    // Build the managed fields map
    let managed_fields = vec![
        ("model_provider", format!("\"{}\"", PROVIDER_ID)),
        ("model", format!("\"{}\"", config.default_model)),
        (
            "model_reasoning_effort",
            format!("\"{}\"", config.model_reasoning_effort),
        ),
        (
            "disable_response_storage",
            if config.disable_response_storage {
                "true".to_string()
            } else {
                "false".to_string()
            },
        ),
        (
            "model_catalog_json",
            format!("\"{}\"", catalog_str),
        ),
    ];

    let mut result = existing;

    // Update or insert each managed top-level field
    for (key, value) in &managed_fields {
        let pattern = format!(r"(?m)^{}\s*=.*$", regex_escape(key));
        let replacement = format!("{} = {}", key, value);
        if let Ok(re) = regex::Regex::new(&pattern) {
            if re.is_match(&result) {
                result = re.replace(&result, replacement.as_str()).to_string();
            } else {
                // Insert at the beginning
                result = format!("{} = {}\n{}", key, value, result);
            }
        }
    }

    // Remove any cc-switch managed provider sections (custom, etc.)
    // that would conflict with our provider.
    // NOTE: Rust's `regex` crate does NOT support lookahead (?=...),
    // so we match the section up to and including its trailing newline,
    // then also any bare [model_providers] header line.
    let remove_patterns: &[&str] = &[
        // Bare [model_providers] header line (Toml allows it but it's noise)
        r"(?m)^\[model_providers\]\s*\n",
        // [model_providers.custom] section — match from the header to the
        // *content* before the next section. Since we can't use lookahead,
        // we match greedily to end-of-string then backtrack by finding the
        // next \n[ manually after the match.
    ];
    for pat in remove_patterns {
        if let Ok(re) = regex::Regex::new(pat) {
            result = re.replace(&result, "").to_string();
        }
    }
    // Manual removal of [model_providers.custom] section (no lookahead):
    if let Some(start) = result.find("[model_providers.custom]") {
        // Find the next section header after this one
        let after = &result[start..];
        let next_section = after[1..] // skip the [ we just found
            .find("\n[")
            .map(|pos| start + 1 + pos + 1); // +1 for the [ we skipped, +1 for \n
        let end = next_section.unwrap_or(result.len());
        result = format!("{}{}", &result[..start], &result[end..]);
    }

    // Re-check after removal
    let has_provider = result.contains(&format!("[model_providers.{PROVIDER_ID}]"));
    let has_model_providers_header =
        result.contains("[model_providers]") || result.contains("[model_providers.");

    if has_provider {
        // Replace existing provider section (no lookahead — find end manually)
        let marker = format!("[model_providers.{PROVIDER_ID}]");
        let start = result.find(&marker).unwrap_or(0);
        let after = &result[start..];
        let end = after[1..]
            .find("\n[")
            .map(|pos| start + 1 + pos + 1)
            .unwrap_or(result.len());
        result = format!("{}{}{}", &result[..start], provider_section, &result[end..]);
    } else if has_model_providers_header {
        // Insert before the first [model_providers] section
        if let Some(pos) = result.find("[model_providers") {
            result = format!(
                "{}\n{}\n{}",
                &result[..pos].trim_end_matches('\n'),
                provider_section,
                &result[pos..]
            );
        }
    } else {
        // Append at the end
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }
        result = format!("{}\n{}", result, provider_section);
    }

    // Clean up excessive blank lines (more than 2 consecutive)
    while result.contains("\n\n\n") {
        result = result.replace("\n\n\n", "\n\n");
    }

    result
}

/// Escape special regex characters in a string.
fn regex_escape(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c.to_string()
            } else {
                format!("\\{}", c)
            }
        })
        .collect()
}

/// Extract the port from a listen address like "0.0.0.0:8888".
fn extract_port(listen_addr: &str) -> u16 {
    listen_addr
        .rsplit(':')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888)
}

/// Atomically write file (write to temp then rename).
fn atomic_write(path: &Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Main entry point: called on proxy startup to auto-manage Codex config.
///
/// 1. Generates `~/.codex/nvidia-proxy-model-catalog.json`
/// 2. Updates `~/.codex/config.toml` incrementally (preserves user sections)
/// 3. Logs what was done
pub fn auto_configure(config: &CodexConfig, listen_addr: &str) {
    if !config.enabled {
        info!("Codex auto-config disabled, skipping");
        return;
    }

    let codex_dir = codex_config_dir();
    if let Err(e) = fs::create_dir_all(&codex_dir) {
        warn!("Failed to create ~/.codex directory: {}", e);
        return;
    }

    // Step 1: Generate catalog
    let cat_path = catalog_path();
    let catalog = generate_catalog(config);
    let catalog_str = serde_json::to_string_pretty(&catalog).unwrap_or_default();
    match atomic_write(&cat_path, &catalog_str) {
        Ok(()) => info!(
            "Wrote Codex model catalog: {} ({} models)",
            cat_path.display(),
            catalog
                .get("models")
                .and_then(|m| m.as_array())
                .map(|a| a.len())
                .unwrap_or(0)
        ),
        Err(e) => {
            warn!("Failed to write catalog {}: {}", cat_path.display(), e);
            return;
        }
    }

    // Step 2: Update config.toml incrementally (preserves user sections)
    let port = extract_port(listen_addr);
    let config_text = generate_config_toml(config, port);
    let config_path = config_toml_path();

    match atomic_write(&config_path, &config_text) {
        Ok(()) => info!(
            "Updated Codex config.toml: {} -> provider={}, model={}, port={}",
            config_path.display(),
            PROVIDER_ID,
            config.default_model,
            port
        ),
        Err(e) => warn!("Failed to write config.toml: {}", e),
    }
}

/// Get the catalog as a JSON value (for serving GET /v1/models).
/// Reads from the catalog file we generated.
pub fn get_catalog() -> Value {
    let path = catalog_path();
    if path.exists() {
        if let Ok(text) = fs::read_to_string(&path) {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                return v;
            }
        }
    }
    // Fallback: return empty
    json!({ "models": [] })
}

#[cfg(test)]
mod tests {
    /// Manual removal of [model_providers.custom] section.
    /// Mirrors the logic in `generate_config_toml` — Rust's regex crate
    /// does not support lookahead, so we find the section header then
    /// cut everything up to the next `\n[` or end of string.
    fn remove_custom_provider_section(mut result: String) -> String {
        if let Some(start) = result.find("[model_providers.custom]") {
            let after = &result[start..];
            let next_section = after[1..]
                .find("\n[")
                .map(|pos| start + 1 + pos + 1);
            let end = next_section.unwrap_or(result.len());
            result = format!("{}{}", &result[..start], &result[end..]);
        }
        result
    }

    #[test]
    fn test_remove_custom_provider_with_following_section() {
        let text = r#"model_provider = "nvidia-proxy"
[model_providers.nvidia-proxy]
name = "NVIDIA Proxy"
base_url = "http://localhost:8888/v1"
[model_providers.custom]
name = "Nvidia proxy"
base_url = "http://localhost:8888/v1"
wire_api = "responses"

[mcp_servers]
[mcp_servers.node_repl]
"#;
        let result = remove_custom_provider_section(text.to_string());
        assert!(!result.contains("model_providers.custom"), "custom should be removed, got: {}", result);
        // Following sections must survive
        assert!(result.contains("[mcp_servers]"), "[mcp_servers] should survive, got: {}", result);
        assert!(result.contains("[mcp_servers.node_repl]"), "[mcp_servers.node_repl] should survive, got: {}", result);
        // Our provider section must survive
        assert!(result.contains("[model_providers.nvidia-proxy]"), "nvidia-proxy should survive, got: {}", result);
    }

    #[test]
    fn test_remove_custom_provider_at_end_of_string() {
        let text = r#"[model_providers.custom]
name = "Nvidia proxy"
base_url = "http://localhost:8888/v1"
wire_api = "responses"
"#;
        let result = remove_custom_provider_section(text.to_string());
        assert!(!result.contains("model_providers.custom"), "custom should be removed, got: {}", result);
        assert!(result.trim().is_empty(), "result should be empty, got: {:?}", result);
    }

    #[test]
    fn test_remove_custom_provider_not_present() {
        let text = r#"model = "z-ai/glm-5.2"
[model_providers.nvidia-proxy]
name = "NVIDIA Proxy"
"#;
        let result = remove_custom_provider_section(text.to_string());
        // No custom section → text unchanged
        assert_eq!(result, text);
    }

    #[test]
    fn test_replace_nvidia_proxy_section() {
        // Simulate the provider-replacement logic from generate_config_toml
        let text = r#"[model_providers.nvidia-proxy]
name = "Old Name"
base_url = "http://old:9999/v1"
wire_api = "chat"

[mcp_servers]
"#;
        let marker = "[model_providers.nvidia-proxy]";
        let provider_section = r#"[model_providers.nvidia-proxy]
name = "NVIDIA Proxy"
base_url = "http://localhost:8888/v1"
wire_api = "responses"
requires_openai_auth = true
experimental_bearer_token = "proxy-managed"
"#;

        let mut result = text.to_string();
        let start = result.find(marker).unwrap();
        let after = &result[start..];
        let end = after[1..]
            .find("\n[")
            .map(|pos| start + 1 + pos + 1)
            .unwrap_or(result.len());
        result = format!("{}{}{}", &result[..start], provider_section, &result[end..]);

        assert!(result.contains("localhost:8888"), "new base_url should be present, got: {}", result);
        assert!(!result.contains("old:9999"), "old base_url should be gone, got: {}", result);
        assert!(result.contains("[mcp_servers]"), "[mcp_servers] should survive, got: {}", result);
    }
}
