//! Worker agent: accepts attempts over the protocol and hands each to its own
//! supervisor (spec chapters 11 and 12).
//!
//! - [`policy`]: the decisions, free of I/O (coverage and mutation gates)
//! - [`agent`]: the operations over the worker database, the startup
//!   sequence, and the thread that takes an attempt to its supervisor
//! - [`blob_policy`]: the decisions of the blob operations, free of I/O
//! - [`blobs`]: receiving a source (manifests, files, bundle) into the store,
//!   and serving an attempt's artifacts
//! - [`host`]: the machine around the agent (sweeps, workspace, secrets)
//! - [`machine`]: the real host (source store, fast-worktree, secret files)
//! - [`serve`]: the tokio side (connections, housekeeping)

pub mod agent;
pub mod blob_policy;
pub mod blobs;
pub mod host;
pub mod machine;
pub mod policy;
pub mod serve;

pub use agent::{Agent, AgentConfig, Probe, ProbePoint, Session, StartupReport};
pub use host::Host;
