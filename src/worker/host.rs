//! What the agent asks of the machine around it: the startup sweeps and the
//! preparation of an attempt's workspace. A trait, so the agent's own rules
//! (acceptance, recovery, order of the startup steps) are tested without Git,
//! fast-worktree or process-wide settings.
//!
//! All of it is blocking; the agent calls it from its own threads.

use super::blobs::BlobSink;
use crate::error::ApiError;
use crate::ids::AttemptId;
use crate::secrets::place::Placed;
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
    /// The paths (relative to the workspace root) of the secret files of the
    /// source the payload names: the declared ones and what
    /// `.worktreeinclude` matched. `None` while that source is not in this
    /// worker's store.
    fn secret_paths(&self, payload: &AttemptPayload) -> Result<Option<Vec<String>>, ApiError>;
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
    /// The secret values to mask in the output of the attempt's commands,
    /// from the secret files in place, and which files those are (what no
    /// artifact may be, under any name). Called after [`Host::secrets_ready`]
    /// said `true` and before the commands start; an error keeps them from
    /// starting.
    fn placed_secrets(&self, attempt: &AcceptedAttempt) -> Result<Placed, ApiError>;
    /// The attempt ended: erase its secret files and remove its workspace.
    /// Idempotent.
    fn cleanup(&self, attempt: &AttemptId, workspace: Option<&Workspace>) -> Result<(), ApiError>;
    /// Where the sources the coordinator sends are taken in (`blob_put`,
    /// `blob_verify`). `None`: this host does not receive sources.
    fn blobs(&self) -> Option<&dyn BlobSink> {
        None
    }
}
