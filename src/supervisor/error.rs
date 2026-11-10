//! Supervisor errors.

use crate::storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A failure reported by the task-traffic-controller library.
    #[error("task-traffic-controller: {0}")]
    Traffic(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The supervisor spec is not usable.
    #[error("invalid supervisor spec: {0}")]
    Spec(String),
    /// The attempt is not in a state this supervisor can act on.
    #[error("{0}")]
    State(String),
    /// Another supervisor is alive for this attempt.
    #[error("another supervisor holds attempt {0}")]
    Alive(String),
}

impl SupervisorError {
    pub fn traffic(e: impl std::fmt::Display) -> Self {
        SupervisorError::Traffic(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SupervisorError>;
