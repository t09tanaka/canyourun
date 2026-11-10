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
pub mod diff;
pub mod manifest;
pub mod safety;
pub mod store;

use crate::error::ApiError;
use crate::ids::RunId;
use local_ci::source::{capture_directory, capture_source, CaptureOptions, Hook, SourceKind};
use local_ci::ExecContext;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub use manifest::SourceManifest;
pub use store::{Inventory, SourceStore, TransferPlan};

/// What to capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRequest {
    /// The repository containing `dir`, as it is now (the default).
    Worktree { dir: PathBuf },
    /// A directory that is not a Git repository.
    Directory { dir: PathBuf },
}

impl SourceRequest {
    pub fn kind(&self) -> SourceKind {
        match self {
            SourceRequest::Worktree { .. } => SourceKind::Worktree,
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
}

/// A source that is fixed and published.
#[derive(Debug, Clone)]
pub struct Captured {
    pub manifest: SourceManifest,
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
        // the secret bundle is captured by the secret path (chapter 22.3)
        secret_files: vec![],
        secret_key: vec![],
        max_attempts: classify::attempt_budget(params.max_attempts),
        hook: params.hook.clone(),
    };
    let snap = match request {
        SourceRequest::Worktree { dir } => capture_source(&ctx, dir, &opts),
        SourceRequest::Directory { dir } => capture_directory(&ctx, dir, &opts),
    }
    .map_err(|e| classify::api_error_of(&e))?;
    let manifest = SourceManifest::from_ci(&snap.manifest, request.kind()).ok_or_else(|| {
        ApiError::new(
            crate::error::ErrorCode::Internal,
            format!("unexpected source id {}", snap.manifest.source_id),
        )
    })?;
    Ok(Captured {
        manifest,
        warnings: snap.warnings,
    })
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
    if let Some(id) = store.source_of_run(run).map_err(internal)? {
        if let Some(m) = store.manifest(&id).map_err(internal)? {
            let manifest = SourceManifest::from_ci(&m, request.kind()).ok_or_else(|| {
                ApiError::new(
                    crate::error::ErrorCode::Internal,
                    format!("bad source id {id}"),
                )
            })?;
            return Ok(Captured {
                manifest,
                warnings: vec![],
            });
        }
    }
    let got = capture(env, store, request, params)?;
    let bound = store
        .bind_run(run, &got.manifest.ci_source_id)
        .map_err(internal)?;
    if bound != got.manifest.ci_source_id {
        // another caller bound this run first: its source is the run's
        let m = store.manifest(&bound).map_err(internal)?.ok_or_else(|| {
            ApiError::new(crate::error::ErrorCode::Internal, "bound source is missing")
        })?;
        let manifest = SourceManifest::from_ci(&m, request.kind()).ok_or_else(|| {
            ApiError::new(
                crate::error::ErrorCode::Internal,
                format!("bad source id {bound}"),
            )
        })?;
        return Ok(Captured {
            manifest,
            warnings: got.warnings,
        });
    }
    Ok(got)
}
