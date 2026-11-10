//! Where the config files live. The environment arrives as a map argument; this
//! module never reads the process environment.

use super::error::ConfigError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const USER_DIR: &str = "remote-task-runner";
pub const USER_FILE: &str = "config.toml";
pub const PROJECT_FILE: &str = ".remote-task-runner.toml";
pub const LEGACY_FILE: &str = ".local-ci.toml";

/// `$XDG_CONFIG_HOME/remote-task-runner/config.toml`, else
/// `$HOME/.config/remote-task-runner/config.toml`. As the XDG spec says, an
/// empty or relative `XDG_CONFIG_HOME` is ignored.
pub fn user_config_path(env: &BTreeMap<String, String>) -> Result<PathBuf, ConfigError> {
    let base = match env.get("XDG_CONFIG_HOME") {
        Some(x) if Path::new(x).is_absolute() => PathBuf::from(x),
        _ => match env.get("HOME") {
            Some(h) if !h.is_empty() => Path::new(h).join(".config"),
            _ => return Err(ConfigError::NoConfigHome),
        },
    };
    Ok(base.join(USER_DIR).join(USER_FILE))
}

pub fn project_config_path(repo_root: &Path) -> PathBuf {
    repo_root.join(PROJECT_FILE)
}

pub fn legacy_config_path(repo_root: &Path) -> PathBuf {
    repo_root.join(LEGACY_FILE)
}
