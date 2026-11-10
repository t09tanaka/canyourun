//! What the agent asks of the machine around it: the startup sweeps and the
//! preparation of an attempt's workspace. A trait, so the agent's own rules
//! (acceptance, recovery, order of the startup steps) are tested without Git,
//! fast-worktree or process-wide settings.
//!
//! All of it is blocking; the agent calls it from its own threads.

use crate::error::ApiError;
use crate::ids::AttemptId;
use crate::storage::AcceptedAttempt;
use crate::wire::{AttemptPayload, SecretFile};
use crate::workspace::Workspace;

pub trait Host: Send + Sync {
    /// Once, before any secret is read (process-wide).
    fn disable_core_dumps(&self) -> Result<(), ApiError>;
    /// Plaintext secret bundles a crashed capture left in the source store.
    fn sweep_store_copies(&self) -> Result<(), ApiError>;
    /// Stale files of the bundle vault.
    fn sweep_vault(&self) -> Result<(), ApiError>;
    /// Erase the placed secret files of every attempt `should_erase` names
    /// (terminal or unknown ones).
    fn reconcile_secrets(&self, should_erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError>;
    /// Remove workspaces and scratch directories no live attempt owns. Runs
    /// before any [`Host::prepare`].
    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError>;
    /// Make the attempt's workspace. Safe to call again for the same attempt
    /// (an agent that died half-way).
    fn prepare(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
    ) -> Result<Workspace, ApiError>;
    /// Place the files of a `secret_bundle_put` in the attempt's workspace.
    /// Returns how many are in place.
    fn place_secrets(
        &self,
        attempt: &AcceptedAttempt,
        workspace: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError>;
    /// Before the commands start: `Ok(true)` when every secret file the
    /// attempt's source names is in place, `Ok(false)` when some have not
    /// arrived yet, an error when what is there is wrong.
    fn secrets_ready(&self, attempt: &AcceptedAttempt) -> Result<bool, ApiError>;
    /// The attempt ended: erase its secret files and remove its workspace.
    /// Idempotent.
    fn cleanup(&self, attempt: &AttemptId, workspace: Option<&Workspace>) -> Result<(), ApiError>;
}
