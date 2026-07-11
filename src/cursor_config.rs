//! Cursor configuration helper – writes/updates ~/.cursor/config.json
//!
//! The Cursor "assistant" (Cursor助手) reads OpenAI‑compatible settings from
//! `openai.baseUrl` and `openai.apiKey` inside the JSON file located at
//! `$HOME/.cursor/config.json` (macOS/Linux) or `%APPDATA%/Cursor/User/settings.json`
//! (Windows).  This module ensures the file exists and sets those two fields
//! according to the proxy configuration.
//!
//! The logic is deliberately idempotent – every call overwrites (or inserts) the two
//! keys while preserving any other user‑defined entries.
//!
//! Usage (called once during startup, after loading `config.toml`):
//! ```rust
//! let cfg = Config::load().unwrap();
//! cursor_config::apply(&cfg);
//! ```
//!
//! The implementation mirrors the style used in `codex_config.rs` (atomic write
//! via a temporary file, JSON pretty‑print, error handling via `log` macros).

use std::{fs, path::PathBuf};
use serde_json::{json, Value};
use tracing::{error, info};


/// Sub‑structure representing the optional `cursor` block in `config.toml`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CursorConfig {
    /// "local" → use the local proxy, "official" → use the official OpenAI endpoint.
    pub mode: String,
    /// The port on which the local proxy listens (defaults to the same port as
    /// `listen_addr`).
    pub proxy_port: u16,
    /// Dummy API key – Cursor complains if the field is missing. The real key is
    /// injected by the proxy when forwarding requests.
    pub api_key: String,
    /// Full path to the target JSON file. `~` is expanded to the user home dir.
    pub settings_path: String,
}

/// Resolve the `~` in a path to the absolute home directory.
fn expand_home(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~") {
        let home = std::env::var("HOME").ok().map(|p| std::path::PathBuf::from(p));
        if let Some(home) = home {
            return home.join(stripped.trim_start_matches('/'));
        }
    }
    PathBuf::from(path)
}

/// Write (or update) the Cursor config file according to the supplied `CursorConfig`.
///
/// The function is tolerant to missing files – it creates a fresh JSON object if
/// the target does not exist. Existing keys other than the two we manage are left
/// untouched.
pub fn apply(cursor_cfg: &CursorConfig) {

    let target_path = expand_home(&cursor_cfg.settings_path);
    let base_url = if cursor_cfg.mode == "local" {
        format!("http://127.0.0.1:{}/v1", cursor_cfg.proxy_port)
    } else {
        // Official OpenAI endpoint – can be empty to let Cursor fall back to its
        // built‑in default, but we set it explicitly for clarity.
        "https://api.openai.com/v1".to_string()
    };

    // Load existing JSON if present.
    let mut json_root: Value = match fs::read_to_string(&target_path) {
        Ok(s) => match serde_json::from_str(&s) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to parse existing Cursor config at {:?}: {}", target_path, e);
                json!({})
            }
        },
        Err(_) => json!({}), // file missing – start with empty object
    };

    // Ensure the root is an object.
    // Ensure the root is an object; if not, reset to empty object.
    if !json_root.is_object() {
        error!("Cursor config at {:?} is not an object; resetting", target_path);
        json_root = json!({});
    }
    let obj = json_root.as_object_mut().expect("json_root should be object");

    obj.insert("openai.baseUrl".to_string(), json!(base_url));
    obj.insert("openai.apiKey".to_string(), json!(cursor_cfg.api_key.clone()));

    // Write out atomically.
    if let Some(parent) = target_path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            error!("Unable to create directory for Cursor config {:?}: {}", parent, e);
            return;
        }
    }
    let tmp_path = target_path.with_extension("tmp");
    match fs::write(&tmp_path, serde_json::to_string_pretty(&json_root).unwrap()) {
        Ok(_) => {
            if let Err(e) = fs::rename(&tmp_path, &target_path) {
                error!("Failed to rename temporary Cursor config {:?} -> {:?}: {}", tmp_path, target_path, e);
            } else {
                info!("Cursor config written to {:?}", target_path);
            }
        }
        Err(e) => error!("Failed to write temporary Cursor config {:?}: {}", tmp_path, e),
    }
}