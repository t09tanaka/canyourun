//! User and project configuration (spec chapter 5).
//!
//! Layout: `model` holds the serde types, `parse` turns text into them,
//! `legacy` maps a `.local-ci.toml` onto the keys it shares, `paths` finds the
//! files from an env map, `resolve` merges the layers (CLI, project, legacy
//! fallback, user, default) and keeps where every value came from, and `load`
//! reads the configuration files. `runtime` composes validated settings into
//! the coordinator and setup-enabled worker construction paths.

pub mod error;
pub mod legacy;
pub mod load;
pub mod model;
pub mod parse;
pub mod paths;
pub mod resolve;
pub mod runtime;

pub use error::ConfigError;
pub use resolve::{resolve, Inputs, Origin, Overlay, ResolvedConfig, Value};
