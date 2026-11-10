//! `.local-ci.toml` (local-ci 0.9) as a fallback for the keys both tools share.
//!
//! | `.local-ci.toml`          | `.remote-task-runner.toml`  |
//! | ------------------------- | --------------------------- |
//! | `workflows-dirs`          | `[ci] workflows_dirs`       |
//! | `[source] include`        | `[source] include`          |
//! | `[source] exclude`        | `[source] exclude`          |
//! | `[source] secret_files`   | `[source] secret_files`     |
//!
//! `no-cache`, `link-node-modules` and `[setup] ignore-env` have no counterpart
//! (see [`UNMAPPED`]); they stay with local-ci. The file is validated by
//! local-ci's own parser, so a typo is rejected exactly as local-ci rejects it.
//! An empty list in the legacy file counts as "not set" (local-ci cannot tell
//! the two apart either).

use super::error::ConfigError;
use super::model::{SecretFile, SecretSpec};

/// One shared key: its name in each file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mapping {
    pub local_ci: &'static str,
    pub rtr: &'static str,
}

pub const MAPPING: &[Mapping] = &[
    Mapping {
        local_ci: "workflows-dirs",
        rtr: "[ci] workflows_dirs",
    },
    Mapping {
        local_ci: "[source] include",
        rtr: "[source] include",
    },
    Mapping {
        local_ci: "[source] exclude",
        rtr: "[source] exclude",
    },
    Mapping {
        local_ci: "[source] secret_files",
        rtr: "[source] secret_files",
    },
];

/// local-ci keys with no counterpart here.
pub const UNMAPPED: &[&str] = &["no-cache", "link-node-modules", "[setup] ignore-env"];

/// The mapped values of a `.local-ci.toml`. `None` means the key is absent
/// (or an empty list).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Fallback {
    pub path: String,
    pub workflows_dirs: Option<Vec<String>>,
    pub include: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub secret_files: Option<Vec<SecretSpec>>,
}

fn non_empty<T>(v: Vec<T>) -> Option<Vec<T>> {
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

pub fn from_text(path: &str, text: &str) -> Result<Fallback, ConfigError> {
    let c = local_ci::config::parse(text).map_err(|e| ConfigError::Parse {
        path: path.to_string(),
        message: format!("{e:#}"),
    })?;
    let secrets = c
        .source
        .secret_files
        .iter()
        .map(|s| SecretSpec {
            glob: s.glob.clone(),
            required: s.required,
        })
        .collect();
    Ok(Fallback {
        path: path.to_string(),
        workflows_dirs: c.workflows_dirs.and_then(non_empty),
        include: non_empty(c.source.include),
        exclude: non_empty(c.source.exclude),
        secret_files: non_empty(secrets),
    })
}

/// A `secret_files` entry of the project file as a spec.
pub fn secret_spec(s: &SecretFile) -> SecretSpec {
    match s {
        SecretFile::Glob(g) => SecretSpec {
            glob: g.clone(),
            required: false,
        },
        SecretFile::Full { glob, required } => SecretSpec {
            glob: glob.clone(),
            required: *required,
        },
    }
}

/// How a spec reads in a message: `glob` or `glob (required)`.
pub fn secret_label(s: &SecretSpec) -> String {
    if s.required {
        format!("{} (required)", s.glob)
    } else {
        s.glob.clone()
    }
}
