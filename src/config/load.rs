//! Reading the files. The only part of `config` that touches the file system.

use super::error::ConfigError;
use super::legacy;
use super::parse::{parse_project, parse_user};
use super::paths::{legacy_config_path, project_config_path, user_config_path};
use super::resolve::{resolve, Inputs, Loaded, Overlay, ResolvedConfig};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;

fn read_optional(path: &Path) -> Result<Option<String>, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(t) => Ok(Some(t)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ConfigError::Io {
            path: path.display().to_string(),
            message: e.to_string(),
        }),
    }
}

/// Reads the user config, the project config and the legacy `.local-ci.toml`
/// (each may be absent) and merges them with `overlay`.
///
/// `env` is the environment as a map (the caller decides what it contains);
/// `repo_root` is `None` outside a repository, which leaves out both project
/// files.
pub fn load(
    env: &BTreeMap<String, String>,
    repo_root: Option<&Path>,
    overlay: &Overlay,
) -> Result<ResolvedConfig, ConfigError> {
    let user_path = user_config_path(env)?;
    let user = match read_optional(&user_path)? {
        Some(t) => {
            let p = user_path.display().to_string();
            Some(Loaded {
                value: parse_user(&p, &t)?,
                path: p,
            })
        }
        None => None,
    };
    let mut project = None;
    let mut fallback = None;
    if let Some(root) = repo_root {
        let pp = project_config_path(root);
        if let Some(t) = read_optional(&pp)? {
            let p = pp.display().to_string();
            project = Some(Loaded {
                value: parse_project(&p, &t)?,
                path: p,
            });
        }
        let lp = legacy_config_path(root);
        if let Some(t) = read_optional(&lp)? {
            fallback = Some(legacy::from_text(&lp.display().to_string(), &t)?);
        }
    }
    resolve(&Inputs {
        overlay,
        project: project.as_ref(),
        legacy: fallback.as_ref(),
        user: user.as_ref(),
    })
}
