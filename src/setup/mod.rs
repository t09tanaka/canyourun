//! Setup templates (spec chapter 8): what a task's install steps made, kept
//! once per worker and copied into the workspaces that need it.
//!
//! - [`policy`]: the decisions, free of I/O (coverage and mutation gates)

pub mod policy;
