//! Text to typed config. Unknown keys fail here, with the reader's line and key.

use super::error::ConfigError;
use super::model::{ProjectConfig, UserConfig, SCHEMA_VERSION};

fn check_version(path: &str, v: u32) -> Result<(), ConfigError> {
    if v == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ConfigError::Parse {
            path: path.to_string(),
            message: format!("schema_version {v} is not supported (expected {SCHEMA_VERSION})"),
        })
    }
}

fn syntax(path: &str, e: toml::de::Error) -> ConfigError {
    ConfigError::Parse {
        path: path.to_string(),
        message: e.to_string(),
    }
}

pub fn parse_user(path: &str, text: &str) -> Result<UserConfig, ConfigError> {
    let c: UserConfig = toml::from_str(text).map_err(|e| syntax(path, e))?;
    check_version(path, c.schema_version)?;
    Ok(c)
}

pub fn parse_project(path: &str, text: &str) -> Result<ProjectConfig, ConfigError> {
    let c: ProjectConfig = toml::from_str(text).map_err(|e| syntax(path, e))?;
    check_version(path, c.schema_version)?;
    Ok(c)
}
