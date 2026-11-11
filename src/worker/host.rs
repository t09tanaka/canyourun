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
use crate::setup::builder::SetupPoll;
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
    /// Where the setup of the attempt stands (spec chapter 8): its steps
    /// are prepared once per worker by a builder and copied into the
    /// attempt's workspace, which the agent calls this about, again and
    /// again, until it is not `Pending`. `Done` names the ids of the setup
    /// steps whose result is in the workspace; the agent leaves those
    /// commands out and runs every other one. The default has no setup.
    fn setup_poll(
        &self,
        _attempt: &AcceptedAttempt,
        _payload: &AttemptPayload,
        _workspace: &Workspace,
    ) -> Result<SetupPoll, ApiError> {
        Ok(SetupPoll::Done {
            skip: Default::default(),
            note: None,
        })
    }
    /// The attempt does not wait for its setup any more (it is stopped, or
    /// gave up waiting). Its reference to the builder goes; the last one
    /// stops the builder. Idempotent.
    fn setup_release(&self, _attempt: &AttemptId) {}
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
