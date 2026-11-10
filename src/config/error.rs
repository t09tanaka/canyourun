//! Errors of the configuration layer. Kept inside `config` so the crate-wide
//! error type can adopt it later.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    /// A file did not parse (syntax, unknown key, wrong type). The message
    /// carries the line and the key name from the TOML reader.
    #[error("{path}: {message}")]
    Parse { path: String, message: String },
    /// Values that are individually valid but wrong together, or wrong alone.
    /// One line per problem.
    #[error("invalid configuration:\n{}", .problems.iter().map(|p| format!("  - {p}")).collect::<Vec<_>>().join("\n"))]
    Invalid { problems: Vec<String> },
    #[error("cannot read {path}: {message}")]
    Io { path: String, message: String },
    #[error("cannot locate the user config directory: set XDG_CONFIG_HOME or HOME")]
    NoConfigHome,
}
