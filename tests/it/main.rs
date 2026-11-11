//! All integration tests in one binary: one link step instead of one per file.
//! Each former test file is a module; `common` and `proto_common` are shared
//! helpers.

mod common;
mod proto_common;

mod bridge;
mod config;
mod coordinator;
mod forecast;
mod not_implemented;
mod protocol;
mod secrets;
mod source_capture;
mod stop;
mod supervisor;
mod worker_agent;
mod worker_blobs;
mod workspace;
