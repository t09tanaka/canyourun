//! Serde types for the two config files. Every table rejects unknown keys.

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

pub const SCHEMA_VERSION: u32 = 1;

// ---- user config -----------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub schema_version: u32,
    pub coordinator: Option<Coordinator>,
    pub scheduler: Option<Scheduler>,
    /// Values a project may override: the lowest file layer.
    pub defaults: Option<Defaults>,
    #[serde(default)]
    pub workers: Vec<Worker>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coordinator {
    pub local_worker: Option<bool>,
    pub heartbeat_seconds: Option<u64>,
    pub stale_seconds: Option<u64>,
    pub lease_seconds: Option<u64>,
    pub term_grace_seconds: Option<u64>,
    pub max_active_homework: Option<u32>,
    pub log_spool_limit_bytes: Option<u64>,
    pub retention_days: Option<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scheduler {
    pub strategy: Option<Strategy>,
    pub forecast_ttl_ms: Option<u64>,
    pub prefetch_per_worker: Option<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    pub timeout_seconds: Option<u64>,
    pub toolchain_match: Option<ToolchainMatch>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub id: String,
    pub ssh: String,
    pub service_mode: Option<ServiceMode>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub ephemeral: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    EarliestFinish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceMode {
    User,
    SystemUser,
}

// ---- project config --------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub schema_version: u32,
    pub source: Option<Source>,
    pub execution: Option<Execution>,
    pub toolchain: Option<Toolchain>,
    pub ci: Option<Ci>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    pub prepare: Option<Prepare>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub mode: Option<SourceMode>,
    pub include: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub secret_files: Option<Vec<SecretFile>>,
}

/// `"glob"` or `{ glob = "...", required = true }`, as in `.local-ci.toml`.
///
/// Deserialize is written by hand: with `#[serde(untagged)]` a typo inside the
/// table would only say "did not match any variant", losing the key name.
#[derive(Debug, Clone, PartialEq)]
pub enum SecretFile {
    Glob(String),
    Full(SecretFileFull),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretFileFull {
    pub glob: String,
    #[serde(default)]
    pub required: bool,
}

struct SecretFileVisitor;

impl<'de> Visitor<'de> for SecretFileVisitor {
    type Value = SecretFile;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a glob string or a table { glob = \"...\", required = true }")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<SecretFile, E> {
        Ok(SecretFile::Glob(v.to_string()))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<SecretFile, A::Error> {
        SecretFileFull::deserialize(de::value::MapAccessDeserializer::new(map))
            .map(SecretFile::Full)
    }
}

impl<'de> Deserialize<'de> for SecretFile {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(SecretFileVisitor)
    }
}

/// A `secret_files` entry, normalised.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SecretSpec {
    pub glob: String,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMode {
    Worktree,
    Commit,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    pub profile: Option<String>,
    pub requires: Option<Vec<String>>,
    pub pass_env: Option<Vec<String>>,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Toolchain {
    #[serde(rename = "match")]
    pub match_: Option<ToolchainMatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolchainMatch {
    Exact,
    Minor,
    Major,
    Any,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ci {
    pub workflows_dirs: Option<Vec<String>>,
    /// Workflows (`ci.yml`) or jobs (`ci.yml:e2e`) whose results are never
    /// reused. Same format as `no-cache` in `.local-ci.toml`.
    pub no_cache: Option<Vec<String>>,
    pub status_context: Option<String>,
    pub unsupported: Option<Unsupported>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unsupported {
    Error,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub name: String,
    pub paths: Vec<String>,
    pub required: Option<bool>,
    pub when: Option<OutputWhen>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputWhen {
    Always,
    Success,
    Failure,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub commands: Vec<Vec<String>>,
}
