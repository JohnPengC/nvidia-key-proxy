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
    (
        "deepseek-ai/deepseek-v4-flash",
        "DeepSeek V4 Flash",
        1_000_000,
    ),
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
fn make_catalog_entry(
    template: &Value,
    model_id: &str,
    display_name: &str,
    context_window: u64,
    priority: usize,
) -> Value {
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
/// Strategy: parse existing config.toml as an editable TOML document, then
/// update only the root-level fields we manage and the
/// `[model_providers.nvidia-proxy]` section. User-defined sections and
/// comments outside of those fields are preserved.
fn generate_config_toml_with_existing(
    config: &CodexConfig,
    proxy_port: u16,
    existing: &str,
    catalog_path: &str,
) -> String {
    let mut doc = existing
        .parse::<toml_edit::DocumentMut>()
        .unwrap_or_else(|_| toml_edit::DocumentMut::new());

    // Managed root-level fields.
    doc["model_provider"] = toml_edit::value(PROVIDER_ID);
    doc["model"] = toml_edit::value(config.default_model.as_str());
    doc["model_reasoning_effort"] = toml_edit::value(config.model_reasoning_effort.as_str());
    doc["disable_response_storage"] = toml_edit::value(config.disable_response_storage);
    doc["model_catalog_json"] = toml_edit::value(catalog_path);

    // Remove any conflicting cc-switch provider section.
    if let Some(model_providers) = doc
        .get_mut("model_providers")
        .and_then(|v| v.as_table_mut())
    {
        model_providers.remove("custom");
    }

    // Ensure the [model_providers] table exists and set our managed section.
    {
        let model_providers =
            doc["model_providers"].or_insert(toml_edit::Item::Table(toml_edit::Table::new()));
        let model_providers = model_providers
            .as_table_mut()
            .expect("model_providers must be a table");
        let nvidia = model_providers
            .entry(PROVIDER_ID)
            .or_insert(toml_edit::Item::Table(toml_edit::Table::new()));
        let nvidia = nvidia.as_table_mut().expect("nvidia-proxy must be a table");
        nvidia["name"] = toml_edit::value("NVIDIA Proxy");
        nvidia["base_url"] = toml_edit::value(format!("http://localhost:{}/v1", proxy_port));
        nvidia["wire_api"] = toml_edit::value("responses");
        nvidia["requires_openai_auth"] = toml_edit::value(true);
        nvidia["experimental_bearer_token"] = toml_edit::value(config.bearer_token.as_str());
    }

    doc.to_string()
}

fn generate_config_toml(config: &CodexConfig, proxy_port: u16) -> String {
    let existing = fs::read_to_string(config_toml_path()).unwrap_or_default();
    let catalog = catalog_path();
    generate_config_toml_with_existing(config, proxy_port, &existing, &catalog.to_string_lossy())
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
    use super::*;

    fn default_config() -> CodexConfig {
        CodexConfig::default()
    }

    #[test]
    fn test_update_root_fields_and_provider() {
        let existing = r#"model = "old-model"
[model_providers.nvidia-proxy]
name = "Old Name"
base_url = "http://old:9999/v1"
wire_api = "chat"

[mcp_servers]
"#;
        let out = generate_config_toml_with_existing(
            &default_config(),
            8888,
            existing,
            "/tmp/catalog.json",
        );
        assert!(
            out.contains(r#"model = "z-ai/glm-5.2""#),
            "model updated: {}",
            out
        );
        assert!(
            out.contains(r#"base_url = "http://localhost:8888/v1""#),
            "base_url updated: {}",
            out
        );
        assert!(
            out.contains(r#"wire_api = "responses""#),
            "wire_api updated: {}",
            out
        );
        assert!(
            out.contains("[mcp_servers]"),
            "mcp_servers preserved: {}",
            out
        );
        assert!(!out.contains("old:9999"), "old base_url removed: {}", out);
        assert!(!out.contains("old-model"), "old model removed: {}", out);
    }

    #[test]
    fn test_remove_custom_provider_and_preserve_others() {
        let existing = r#"[model_providers.custom]
name = "cc-switch"
base_url = "http://localhost:8888/v1"
wire_api = "responses"

[model_providers.nvidia-proxy]
name = "Old Name"
base_url = "http://old:9999/v1"

[mcp_servers]
"#;
        let out = generate_config_toml_with_existing(
            &default_config(),
            8888,
            existing,
            "/tmp/catalog.json",
        );
        assert!(
            !out.contains("[model_providers.custom]"),
            "custom removed: {}",
            out
        );
        assert!(
            !out.contains("cc-switch"),
            "custom content removed: {}",
            out
        );
        assert!(
            out.contains("[model_providers.nvidia-proxy]"),
            "nvidia-proxy preserved: {}",
            out
        );
        assert!(
            out.contains("[mcp_servers]"),
            "mcp_servers preserved: {}",
            out
        );
        assert!(
            out.contains(r#"base_url = "http://localhost:8888/v1""#),
            "nvidia-proxy updated: {}",
            out
        );
    }

    #[test]
    fn test_does_not_touch_nested_model_key() {
        let existing = r#"model = "old-model"
[some_plugin]
model = "plugin-internal-model"
"#;
        let out = generate_config_toml_with_existing(
            &default_config(),
            8888,
            existing,
            "/tmp/catalog.json",
        );
        assert!(
            out.contains(r#"model = "z-ai/glm-5.2""#),
            "root model updated: {}",
            out
        );
        assert!(
            out.contains(r#"[some_plugin]"#),
            "plugin table preserved: {}",
            out
        );
        assert!(
            out.contains(r#"model = "plugin-internal-model""#),
            "nested model untouched: {}",
            out
        );
    }

    #[test]
    fn test_insert_into_empty_config() {
        let out =
            generate_config_toml_with_existing(&default_config(), 1234, "", "/tmp/catalog.json");
        assert!(
            out.contains(r#"model_provider = "nvidia-proxy""#),
            "model_provider inserted: {}",
            out
        );
        assert!(
            out.contains(r#"base_url = "http://localhost:1234/v1""#),
            "base_url inserted: {}",
            out
        );
        assert!(
            out.contains(r#"model_catalog_json = "/tmp/catalog.json""#),
            "catalog inserted: {}",
            out
        );
    }
}
