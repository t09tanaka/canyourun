//! Input snapshots (working tree, commit, directory) and transfer (spec
//! chapter 6).
//!
//! The capture itself is local-ci's [`capture_source`] / [`capture_directory`]
//! (verified copy into a private content-addressed store, at most three
//! passes, atomic manifest publication, an original that is never written).
//! This module puts them under the runner's state directory, binds a capture
//! to a run (one capture per run), maps refusals to the runner's error codes,
//! and adds the receiving side: difference planning, verified blob intake,
//! and path-safe unpacking.
//!
//! All of it is blocking (Git, file I/O): call it from a dedicated thread or
//! `spawn_blocking`.

pub mod classify;
mod commit;
pub mod diff;
pub mod manifest;
pub mod safety;
pub mod store;

use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use local_ci::source::{
    capture_directory, capture_source, CaptureOptions, Hook, SecretSpec, SourceKind,
};
use local_ci::ExecContext;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use manifest::SourceManifest;
pub use store::{
    Inventory, ObjectsRepo, Peer, RequestSummary, RunRecord, SourceStore, TransferPlan,
};

/// What to capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRequest {
    /// The repository containing `dir`, as it is now (the default).
    Worktree { dir: PathBuf },
    /// The commit `rev` of the repository containing `dir`, as committed.
    Commit { dir: PathBuf, rev: String },
    /// A directory that is not a Git repository.
    Directory { dir: PathBuf },
}

impl SourceRequest {
    pub fn kind(&self) -> SourceKind {
        match self {
            SourceRequest::Worktree { .. } => SourceKind::Worktree,
            SourceRequest::Commit { .. } => SourceKind::Commit,
            SourceRequest::Directory { .. } => SourceKind::Directory,
        }
    }
}

#[derive(Clone, Default)]
pub struct CaptureParams {
    /// The base the logical context (`base`, `merge_base`) is taken against.
    pub base: Option<String>,
    /// Ignored, untracked files to take along (globs, repo-root relative).
    pub include: Vec<String>,
    /// Files to leave out (globs).
    pub exclude: Vec<String>,
    /// Passes before `SOURCE_BUSY`; clamped to `1..=3`.
    pub max_attempts: Option<u32>,
    /// A test seam, called between the steps of a capture (see
    /// [`local_ci::source::Stage`]).
    pub hook: Option<Hook>,
    /// Ignored files to send as secrets (chapter 22.3); built by
    /// [`crate::secrets::capture`], which adds what `.worktreeinclude` matches.
    pub secret_files: Vec<SecretSpec>,
    /// The cluster key the secret files' HMACs are made with.
    pub secret_key: Vec<u8>,
    /// Where local-ci writes the secret bundle: a path of the run, so that two
    /// runs of the same source never share a file. Created, never replaced
    /// (the directory must exist). `None`: the store's default, keyed by
    /// source.
    pub secret_bundle: Option<PathBuf>,
}

impl std::fmt::Debug for CaptureParams {
    /// The key is not shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureParams")
            .field("base", &self.base)
            .field("include", &self.include)
            .field("exclude", &self.exclude)
            .field("max_attempts", &self.max_attempts)
            .field("secret_files", &self.secret_files)
            .field("secret_bundle", &self.secret_bundle)
            .field("secret_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// A source that is fixed and published.
#[derive(Debug, Clone)]
pub struct Captured {
    pub manifest: SourceManifest,
    /// Where local-ci put the secret files of this capture (0600, in the
    /// source store, keyed by source). `None` when there are none, and for a
    /// run whose capture was made before: its bundle is already with the run.
    pub secrets: Option<PathBuf>,
    /// Things worth saying that did not stop the capture.
    pub warnings: Vec<String>,
}

/// Capture `request` into `store`. Never writes the original. When the input
/// kept changing for the whole attempt budget the error is `SOURCE_BUSY`, and
/// nothing is published.
pub fn capture(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    let ctx = ExecContext::new(env.clone());
    let opts = CaptureOptions {
        store: Some(store.root().to_path_buf()),
        base: params.base.clone(),
        include: params.include.clone(),
        exclude: params.exclude.clone(),
        secret_files: params.secret_files.clone(),
        secret_key: params.secret_key.clone(),
        secret_root: None,
        secret_bundle: params.secret_bundle.clone(),
        max_attempts: classify::attempt_budget(params.max_attempts),
        hook: params.hook.clone(),
    };
    let ci_err = |e: anyhow::Error| classify::api_error_of(&e);
    let snap = match request {
        SourceRequest::Worktree { dir } => capture_source(&ctx, dir, &opts).map_err(ci_err)?,
        SourceRequest::Commit { dir, rev } => {
            let co = commit::checkout(
                env,
                &store.root().join("tmp"),
                dir,
                rev,
                params.base.as_deref(),
            )
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, format!("{e:#}")))?;
            // the scratch repository has the base as a commit, not as a ref
            // the clean checkout has no ignored files: the secret files are
            // chosen and read in the worktree the commit came from
            let o = CaptureOptions {
                base: co.base.clone(),
                secret_root: Some(dir.clone()),
                ..opts
            };
            capture_source(&ctx, &co.cwd, &o).map_err(ci_err)?
        }
        SourceRequest::Directory { dir } => capture_directory(&ctx, dir, &opts).map_err(ci_err)?,
    };
    // from here on the store holds a plaintext secret bundle: every way out
    // that does not hand it to the caller removes it
    let checked = refuse_uninitialized_submodules(store, &snap.manifest).and_then(|()| {
        SourceManifest::from_ci(&snap.manifest, request.kind()).ok_or_else(|| {
            ApiError::new(
                ErrorCode::Internal,
                format!("unexpected source id {}", snap.manifest.source_id),
            )
        })
    });
    let manifest = match checked {
        Ok(m) => m,
        Err(e) => {
            remove_secret_copy(snap.secrets.as_deref());
            return Err(e);
        }
    };
    Ok(Captured {
        manifest,
        secrets: snap.secrets,
        warnings: snap.warnings,
    })
}

/// Remove the store's plaintext secret bundle, if there is one. Best effort:
/// it is called on the way out of an error, which it must not replace; a copy
/// that stays is found by the sweep at start
/// ([`crate::secrets::capture::sweep_store_copies`]).
fn remove_secret_copy(path: Option<&Path>) {
    if let Some(p) = path {
        let _ = std::fs::remove_file(p);
    }
}

/// A submodule that was not initialized in the original has no files to
/// capture, only a revision: a source made so would not hold what the
/// repository holds, so it is refused, with the path. (The refused capture
/// leaves its manifest in the store, unreferenced; nothing is bound to it.)
fn refuse_uninitialized_submodules(
    store: &SourceStore,
    m: &local_ci::source::SourceManifest,
) -> Result<(), ApiError> {
    let internal = |e: anyhow::Error| classify::api_error_of(&e);
    for s in store.closure(m).map_err(internal)? {
        if let Some(sub) = s.submodules.iter().find(|x| x.source_id.is_none()) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!(
                    "submodule {} is not initialized (run `git submodule update --init` there): a source needs its files, not only its revision",
                    sub.path.display()
                ),
            ));
        }
    }
    Ok(())
}

/// The part of a request that decides what is captured, for the run record.
fn summarize(request: &SourceRequest, params: &CaptureParams) -> RequestSummary {
    RequestSummary {
        kind: request.kind(),
        rev: match request {
            SourceRequest::Commit { rev, .. } => Some(rev.clone()),
            _ => None,
        },
        base: params.base.clone(),
        include: params.include.clone(),
        exclude: params.exclude.clone(),
    }
}

/// The capture of a run: made once, then the same source for every later
/// call (a re-dispatch, a late worker, a retry after a crash). The source is
/// bound to the run after it is published, so a record always names a source
/// that can be loaded.
pub fn capture_for_run(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    run: &RunId,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    let internal = |e: anyhow::Error| classify::api_error_of(&e);
    let from_ci = |m: &local_ci::source::SourceManifest| {
        SourceManifest::from_ci(m, request.kind()).ok_or_else(|| {
            ApiError::new(
                ErrorCode::Internal,
                format!("bad source id {}", m.source_id),
            )
        })
    };
    // one capture of a run at a time, in this process and in others
    let _lock = store.lock_run(run).map_err(internal)?;
    let wanted = RunRecord {
        ci_source_id: String::new(),
        source_kind: request.kind(),
        request: summarize(request, params),
    };
    if let Some(rec) = store.run_record(run).map_err(internal)? {
        if rec.source_kind != wanted.source_kind || rec.request != wanted.request {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "run {run} already has a source ({}) captured for {:?}; this request is {:?}",
                    rec.ci_source_id, rec.request, wanted.request
                ),
            ));
        }
        // the record names the source: a manifest that is gone is damage, not
        // a reason to capture again (the working tree may have moved on)
        let m = store
            .manifest(&rec.ci_source_id)
            .map_err(internal)?
            .ok_or_else(|| {
                ApiError::new(
                    ErrorCode::Internal,
                    format!(
                        "run {run} is bound to {}, whose manifest is missing from the store",
                        rec.ci_source_id
                    ),
                )
            })?;
        return Ok(Captured {
            manifest: from_ci(&m)?,
            secrets: None,
            warnings: vec![],
        });
    }
    // no record binds the run yet, so a bundle at its path is the leftover of
    // a capture that did not get to bind: local-ci creates the file and never
    // replaces one
    if let Some(p) = &params.secret_bundle {
        match std::fs::remove_file(p) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(ApiError::new(
                    ErrorCode::Internal,
                    format!("secret bundle {}: {e}", p.display()),
                ))
            }
            _ => {}
        }
    }
    let got = capture(env, store, request, params)?;
    let record = RunRecord {
        ci_source_id: got.manifest.ci_source_id.clone(),
        ..wanted
    };
    let bound = match store.bind_run(run, &record) {
        Ok(b) => b,
        Err(e) => {
            remove_secret_copy(got.secrets.as_deref());
            return Err(internal(e));
        }
    };
    if bound != record {
        remove_secret_copy(got.secrets.as_deref());
        return Err(ApiError::new(
            ErrorCode::Internal,
            format!(
                "run {run} was bound to {} while its capture was held",
                bound.ci_source_id
            ),
        ));
    }
    Ok(got)
}
