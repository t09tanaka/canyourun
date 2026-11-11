//! The real [`Host`]: the source store, fast-worktree workspaces and the
//! secret files of this worker's state directory.
//!
//! The source an attempt names has to be in the worker's store before the
//! attempt is prepared; it gets there through the blob operations of the
//! protocol ([`super::blobs`]).

use super::blobs::{BlobSink, Incoming};
use super::host::Host;
use crate::error::{ApiError, ErrorCode};
use crate::ids::AttemptId;
use crate::platform;
use crate::secrets::capture::sweep_store_copies;
use crate::secrets::envfile::Values;
use crate::secrets::key;
use crate::secrets::place::WorkerSecrets;
use crate::secrets::vault::Vault;
use crate::secrets::SecretError;
use crate::source::classify::ci_id_of;
use crate::source::SourceStore;
use crate::storage::AcceptedAttempt;
use crate::wire::{AttemptPayload, SecretFile};
use crate::workspace::{CliFastWorktree, Workspace, Workspaces};
use local_ci::source::SourceManifest as CiManifest;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Machine {
    state_dir: PathBuf,
    store: SourceStore,
    workspaces: Workspaces,
    secrets: WorkerSecrets,
    key_path: PathBuf,
    incoming: Incoming,
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

impl Machine {
    /// `env` is what Git, fast-worktree and the project's hooks run with
    /// (`HOME` and `PATH` at least); `key_path` is the cluster key file.
    pub fn open(
        state_dir: &Path,
        env: BTreeMap<String, String>,
        key_path: PathBuf,
    ) -> Result<Self, ApiError> {
        let store = SourceStore::open(state_dir).map_err(|e| internal("source store", e))?;
        let secrets = WorkerSecrets::open(state_dir).map_err(|e| e.to_api())?;
        let fw = Arc::new(CliFastWorktree::new(env.clone()));
        let incoming = Incoming::new(
            state_dir,
            store.clone(),
            Workspaces::new(state_dir.to_path_buf(), env.clone(), fw.clone()),
            env.clone(),
        );
        Ok(Machine {
            incoming,
            state_dir: state_dir.to_path_buf(),
            store,
            workspaces: Workspaces::new(state_dir.to_path_buf(), env, fw),
            secrets,
            key_path,
        })
    }

    /// The manifest of the source the attempt's payload names, and only if
    /// it is the manifest the payload names.
    fn manifest(&self, attempt: &AcceptedAttempt) -> Result<CiManifest, ApiError> {
        let payload: AttemptPayload = serde_json::from_value(attempt.spec.clone())
            .map_err(|e| internal("stored payload", e))?;
        let missing = || {
            ApiError::new(
                ErrorCode::NotFound,
                format!("source {} is not in this worker's store", payload.source_id),
            )
        };
        let source = ci_id_of(&payload.source_id).ok_or_else(missing)?;
        let m = self
            .store
            .manifest(&source)
            .map_err(|e| internal("source store", format!("{e:#}")))?
            .ok_or_else(missing)?;
        let want = payload.manifest_digest.as_str();
        if m.manifest_digest != want && format!("sha256:{}", m.manifest_digest) != want {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "source {} has another manifest than the attempt names",
                    payload.source_id
                ),
            ));
        }
        Ok(m)
    }
}

impl Host for Machine {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        platform::disable_core_dumps().map_err(|e| internal("core dumps", e))
    }

    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        sweep_store_copies(&self.store, |_| false).map(|_| ())
    }

    fn sweep_vault(&self) -> Result<(), ApiError> {
        // The agent does not know which runs ended (the coordinator does):
        // no bundle is erased here, only the vault's stale temporary files.
        let vault = Vault::open(&self.state_dir).map_err(|e| e.to_api())?;
        vault.sweep(|_| false).map(|_| ()).map_err(|e| e.to_api())
    }

    fn reconcile_secrets(&self, should_erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        let report = self
            .secrets
            .reconcile(should_erase)
            .map_err(|e| e.to_api())?;
        // Their secret files cannot be proven gone one by one: the whole
        // workspace goes, then the record. This runs at startup, before any
        // workspace is being made, which the sweep requires.
        let required = &report.workspace_required;
        if !required.is_empty() {
            let swept = self.workspaces.sweep_orphans(&|a| !required.contains(a))?;
            if let Some((path, why)) = swept.failed.first() {
                return Err(internal(
                    "workspace with secret files",
                    format!("{}: {why}", path.display()),
                ));
            }
            for attempt in required {
                self.secrets.forget(attempt).map_err(|e| e.to_api())?;
            }
        }
        if let Some((name, why)) = report.failed.first() {
            return Err(internal("secret files", format!("{name}: {why}")));
        }
        Ok(())
    }

    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        let report = self.workspaces.sweep_orphans(has_record)?;
        match report.failed.first() {
            Some((path, why)) => Err(internal(
                "orphan workspace",
                format!("{}: {why}", path.display()),
            )),
            None => Ok(()),
        }
    }

    fn prepare(
        &self,
        attempt: &AcceptedAttempt,
        _: &AttemptPayload,
    ) -> Result<Workspace, ApiError> {
        let m = self.manifest(attempt)?;
        let repo = match (&m.execution_commit, &m.repository_id) {
            (Some(_), Some(id)) => Some(self.workspaces.ensure_clone(id, None)?),
            _ => None,
        };
        self.workspaces
            .create(&attempt.attempt_id, &self.store, &m, repo.as_ref())
    }

    fn place_secrets(
        &self,
        attempt: &AcceptedAttempt,
        workspace: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        let m = self.manifest(attempt)?;
        let key = key::load_in_state(&self.state_dir, &self.key_path).map_err(|e| e.to_api())?;
        self.secrets
            .place(
                &key,
                &attempt.attempt_id,
                &workspace.root,
                files,
                &m.secret_files,
            )
            .map_err(|e| e.to_api())
    }

    fn secrets_ready(&self, attempt: &AcceptedAttempt) -> Result<bool, ApiError> {
        let m = self.manifest(attempt)?;
        match self
            .secrets
            .ensure_placed(&attempt.attempt_id, &m.secret_files)
        {
            Ok(()) => Ok(true),
            // Not all of them arrived yet.
            Err(SecretError::PathSetMismatch) => Ok(false),
            Err(e) => Err(e.to_api()),
        }
    }

    fn mask_values(&self, attempt: &AcceptedAttempt) -> Result<Values, ApiError> {
        self.secrets
            .mask_values(&attempt.attempt_id)
            .map_err(|e| e.to_api())
    }

    fn blobs(&self) -> Option<&dyn BlobSink> {
        Some(&self.incoming)
    }

    fn cleanup(&self, attempt: &AttemptId, workspace: Option<&Workspace>) -> Result<(), ApiError> {
        match self.secrets.erase(attempt) {
            Ok(_) => {}
            Err(e @ SecretError::WorkspaceRequired { .. }) => {
                // Without the workspace record nothing can be removed safely
                // here; the next startup's reconcile does it.
                let Some(ws) = workspace else {
                    return Err(e.to_api());
                };
                self.workspaces.remove(ws)?;
                self.secrets.forget(attempt).map_err(|e| e.to_api())?;
                return Ok(());
            }
            Err(e) => return Err(e.to_api()),
        }
        match workspace {
            Some(ws) if ws.root.exists() => self.workspaces.remove(ws),
            _ => Ok(()),
        }
    }
}
