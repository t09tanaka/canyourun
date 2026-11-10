//! Secret files (spec 22.3): ignored files a run needs, such as a test `.env`.
//!
//! The flow, and where each step lives:
//!
//! 1. *Which files*: the declared `[source] secret_files` globs plus the
//!    ignored files `.worktreeinclude` matches ([`worktreeinclude`]).
//! 2. *Capture*: local-ci reads them with its usual verification and retries
//!    (see [`crate::source::CaptureParams`]) into a bundle that is not in the
//!    object store, and records path and keyed HMAC only. The key is the
//!    cluster key ([`key`]).
//! 3. *Hold on the parent*: one 0600 file per run ([`vault`]), deleted when the
//!    run is terminal. A run whose bundle is gone is `blocked: secret_unavailable`.
//! 4. *Send*: `secret_bundle_put` requests, every file with its HMAC, a few
//!    files per request ([`send`]; the agent connection itself is S10).
//! 5. *Place on the worker*: HMAC verified first, then 0600 files in the
//!    attempt workspace, without following symlinks ([`place`]); erased when
//!    the attempt is terminal and again by the reconcile at worker start.
//! 6. *Mask*: values of `KEY=VALUE` lines are replaced in logs ([`envfile`],
//!    [`mask`]).
//!
//! Secret values never appear in an error, a `Debug` output or a log line of
//! this module. The pure decisions ([`plan`], [`envfile`], [`mask`]) have no
//! I/O and are held to the coverage and mutation gates.

pub mod envfile;
pub mod key;
pub mod mask;
pub mod place;
pub mod plan;
pub mod send;
pub mod vault;
pub mod worktreeinclude;

use crate::error::{ApiError, BlockedReason, ErrorCode};
use plan::{BadPath, Limit};
use std::path::PathBuf;
use thiserror::Error;

/// Why a secret operation failed. Messages name paths, counts and sizes, never
/// a value.
#[derive(Debug, Error)]
pub enum SecretError {
    #[error("{0}")]
    Limit(Limit),
    #[error("{0}")]
    BadPath(BadPath),
    #[error("secret file path {0:?} is not valid UTF-8")]
    NonUtf8Path(String),
    /// A file's HMAC does not match: it was changed, or another key made it.
    #[error("secret file {path} does not match its HMAC")]
    Tampered { path: String },
    #[error("the secret bundle does not hold the files the manifest names")]
    PathSetMismatch,
    /// The bundle is not there: the parent lost it, or never had it.
    #[error("the secret bundle of {what} is unavailable")]
    Unavailable { what: String },
    #[error("{what}: {path} must be a regular file readable by its owner only", path = path.display())]
    Insecure { what: &'static str, path: PathBuf },
    #[error("{0}")]
    Placement(String),
    #[error("{what}: {source}")]
    Io {
        what: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    Format(String),
}

impl SecretError {
    pub(crate) fn io(what: impl Into<String>, source: std::io::Error) -> Self {
        SecretError::Io {
            what: what.into(),
            source,
        }
    }

    /// The error as the API reports it.
    pub fn to_api(&self) -> ApiError {
        match self {
            SecretError::Unavailable { .. } => {
                ApiError::blocked(BlockedReason::SecretUnavailable, self.to_string())
            }
            SecretError::Limit(_)
            | SecretError::BadPath(_)
            | SecretError::NonUtf8Path(_)
            | SecretError::PathSetMismatch => ApiError::new(ErrorCode::InvalidInput, self.to_string()),
            SecretError::Tampered { .. }
            | SecretError::Insecure { .. }
            | SecretError::Placement(_)
            | SecretError::Io { .. }
            | SecretError::Format(_) => ApiError::new(ErrorCode::Internal, self.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, SecretError>;
