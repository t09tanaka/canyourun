//! Durable state (spec chapter 10, 11, 12, 13, 18).
//!
//! The coordinator and every worker keep a local SQLite database (WAL,
//! `synchronous = FULL`) as the source of truth. A [`Db`] owns one connection
//! on a dedicated thread; the repositories ([`CoordinatorStore`],
//! [`WorkerStore`]) are synchronous, so async code calls them through
//! `tokio::task::spawn_blocking`. Every operation that must survive a crash is
//! one transaction, committed before the method returns.
//!
//! The state machine lives in `crate::state`; storage only applies its
//! verdicts, guarded by conditional updates. The decisions that are pure
//! (schema compatibility, idempotent submit, launch claims, lease renewal, GC
//! selection) live in [`policy`].

mod common;
mod coordinator;
mod db;
mod lock;
pub mod policy;
mod schema;
#[cfg(test)]
mod tests;
mod worker;

pub use common::{BlobKind, BlobRef, Event, GcReport, NewBlobRef};
pub use coordinator::{
    ArtifactReceipt, AttemptRecord, CoordinatorStore, NewAttempt, Permit, Placement,
};
pub use db::Db;
pub use lock::ProcessLock;
pub use worker::{
    AcceptedAttempt, CancelOutcome, LaunchClaim, LeaseOutcome, LogPosition, NewAcceptance,
    ProcessIdentity, SubmitOutcome, WorkerStore,
};

use crate::error::ErrorCode;
use crate::ids::{AttemptId, CoordinatorEpoch};
use crate::state::{AttemptState, Termination, TransitionError};
use policy::Incompat;
use std::path::PathBuf;
use thiserror::Error;

/// Which of the two databases a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Coordinator,
    Worker,
}

impl DbKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DbKind::Coordinator => "coordinator",
            DbKind::Worker => "worker",
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("database is not usable by this binary: {0:?}")]
    Incompatible(Incompat),
    #[error("database holds a {found} schema, expected {expected}")]
    KindMismatch { expected: String, found: String },
    #[error("another process holds {}", .0.display())]
    Locked(PathBuf),
    #[error("{what} not found")]
    NotFound { what: String },
    #[error("attempt {0} was submitted with a different payload")]
    PayloadConflict(AttemptId),
    #[error("attempt {0} is retired")]
    Retired(AttemptId),
    #[error("coordinator epoch {got} is older than {current}")]
    StaleEpoch {
        got: CoordinatorEpoch,
        current: CoordinatorEpoch,
    },
    #[error("homework already has a live attempt ({0})")]
    LiveAttemptExists(String),
    #[error("lease for {0} cannot be renewed")]
    LeaseNotRenewable(AttemptId),
    #[error(transparent)]
    Transition(#[from] TransitionError),
    #[error("attempt {attempt} is {state:?}: {why}")]
    IllegalState {
        attempt: String,
        state: AttemptState,
        why: &'static str,
    },
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("already fixed and different: {0}")]
    Immutable(String),
    #[error("stored data is corrupt: {0}")]
    Corrupt(String),
    #[error("database thread is gone")]
    Closed,
    #[error("a storage operation panicked")]
    Panicked,
}

impl StorageError {
    /// The wire error code this maps to.
    pub fn code(&self) -> ErrorCode {
        match self {
            StorageError::Incompatible(_) | StorageError::KindMismatch { .. } => {
                ErrorCode::ProtocolIncompatible
            }
            StorageError::Locked(_)
            | StorageError::PayloadConflict(_)
            | StorageError::Immutable(_)
            | StorageError::LiveAttemptExists(_) => ErrorCode::Conflict,
            StorageError::NotFound { .. } => ErrorCode::NotFound,
            StorageError::Retired(_) => ErrorCode::Retired,
            StorageError::StaleEpoch { .. } => ErrorCode::StaleEpoch,
            StorageError::LeaseNotRenewable(_) => ErrorCode::LeaseNotRenewable,
            StorageError::Invalid(_) | StorageError::Transition(_) => ErrorCode::InvalidInput,
            StorageError::IllegalState { .. } => ErrorCode::Conflict,
            StorageError::Sqlite(_)
            | StorageError::Io(_)
            | StorageError::Corrupt(_)
            | StorageError::Closed
            | StorageError::Panicked => ErrorCode::Internal,
        }
    }
}

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Whether a terminal write won or found an earlier result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finalized {
    /// This call fixed the termination.
    Won(Termination),
    /// Somebody fixed it first; this is theirs.
    AlreadyTerminal(Termination),
}

impl Finalized {
    pub fn termination(&self) -> Termination {
        match self {
            Finalized::Won(t) | Finalized::AlreadyTerminal(t) => *t,
        }
    }

    pub fn won(&self) -> bool {
        matches!(self, Finalized::Won(_))
    }
}

pub(crate) fn to_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| StorageError::Invalid(format!("{v} does not fit in i64")))
}

pub(crate) fn to_u64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| StorageError::Corrupt(format!("negative value {v}")))
}
