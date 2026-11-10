//! Capturing a run's source together with its secret files (spec 22.3).
//!
//! Which requests may carry secret files:
//!
//! - `Worktree`: the declared globs plus what `.worktreeinclude` matches are
//!   read from the working tree.
//! - `Commit`: the same, read from the original worktree the commit came from
//!   (local-ci's `secret_root`), never from the clean checkout the commit is
//!   captured in.
//! - `Directory`: the declared globs (local-ci separates them from the
//!   snapshot as it does for a worktree). A `.worktreeinclude` that names a
//!   pattern is still refused: its patterns are Git's, and there is no
//!   repository to evaluate them in, so which files it means cannot be told.
//!
//! The store's plaintext bundle (`<store>/secrets/<run>.json`, a path of the
//! run, created by local-ci and never replaced) lives from
//! the moment local-ci writes it until the vault has adopted it. Every error
//! after that moment removes it, a later call for a run whose capture was
//! already bound adopts a copy that is still there, and
//! [`sweep_store_copies`] removes what a crash left (the supervisor calls it
//! at start, S10). One lock ([`SourceStore::lock_secrets`]) covers the
//! capture and the adoption, because the file is named by source and two runs
//! of the same source would otherwise share it.

use super::key::ClusterKey;
use super::plan::{check_declared, merge_specs};
use super::vault::Vault;
use super::worktreeinclude::{declares_patterns, ignored_matches, preflight};
use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use crate::source::{capture_for_run, CaptureParams, Captured, SourceRequest, SourceStore};
use local_ci::source::SecretSpec;
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;

/// What decides the secret files of a capture.
pub struct SecretInputs<'a> {
    pub key: &'a ClusterKey,
    /// `[source] secret_files` of the project.
    pub declared: &'a [SecretSpec],
}

fn worktreeinclude_in_directory() -> ApiError {
    ApiError::new(
        ErrorCode::InvalidInput,
        "a directory capture: .worktreeinclude names patterns, which are Git's and cannot be evaluated in a directory that is not a repository; list the files in [source] secret_files, or run from a repository".to_string(),
    )
}

/// Where local-ci writes the plaintext bundle of `run` in `store`.
pub fn run_bundle_path(store: &SourceStore, run: &RunId) -> PathBuf {
    store.root().join("secrets").join(format!("{run}.json"))
}

/// The leftover of a bundle write that was killed: local-ci's temporary file
/// (`.secrets.tmp-*`) holds the secret files in plain.
fn is_bundle_tmp(name: &str) -> bool {
    name.starts_with(".secrets.tmp-")
}

/// `capture_for_run` with secret files: the declared globs plus what
/// `.worktreeinclude` matches (taken from the working tree for a worktree
/// capture, from the original worktree for a commit capture, and for a
/// directory capture the declared globs only), read by local-ci under the
/// cluster key, and the bundle moved into the run's own hold in `vault`.
/// Blocking, like the rest of capture.
pub fn capture_run(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    vault: &Vault,
    inputs: &SecretInputs<'_>,
    run: &RunId,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    let (key, declared) = (inputs.key, inputs.declared);
    check_declared(declared).map_err(|l| ApiError::new(ErrorCode::InvalidInput, l.to_string()))?;
    let included = match request {
        SourceRequest::Worktree { dir } | SourceRequest::Commit { dir, .. } => {
            ignored_matches(env, dir).map_err(|e| e.to_api())?
        }
        SourceRequest::Directory { dir } => {
            if declares_patterns(dir).map_err(|e| e.to_api())? {
                return Err(worktreeinclude_in_directory());
            }
            vec![]
        }
    };
    if let SourceRequest::Worktree { dir } | SourceRequest::Commit { dir, .. } = request {
        // fail on a file that cannot be sent before local-ci reads anything
        preflight(env, dir, declared, &included).map_err(|e| e.to_api())?;
    }
    let specs = merge_specs(declared, &included);
    // no secret file, nothing to adopt, nothing to serialize
    let _lock = if specs.is_empty() {
        None
    } else {
        Some(store.lock_secrets().map_err(|e| {
            ApiError::new(ErrorCode::Internal, format!("secret capture lock: {e:#}"))
        })?)
    };
    let copy = run_bundle_path(store, run);
    if !specs.is_empty() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(store.root().join("secrets"))
            .map_err(|e| {
                ApiError::new(ErrorCode::Internal, format!("secret store directory: {e}"))
            })?;
    }
    let params = CaptureParams {
        secret_bundle: (!specs.is_empty()).then(|| copy.clone()),
        secret_files: specs,
        secret_key: key.as_bytes().to_vec(),
        ..params.clone()
    };
    let got = capture_for_run(env, store, run, request, &params)?;
    let entries = &got.manifest.secret_files;
    // a fresh capture hands the path over; a repeated call finds the copy an
    // earlier call could not adopt, if it is still there
    let pending = match &got.secrets {
        Some(path) => Some(path.clone()),
        None if !entries.is_empty() && !vault.has(run) && fs::symlink_metadata(&copy).is_ok() => {
            Some(copy)
        }
        None => None,
    };
    if let Some(path) = pending {
        vault
            .adopt(run, &path, entries, key)
            .map_err(|e| e.to_api())?;
    }
    Ok(got)
}

/// Remove the plaintext bundles in the store that no run is going to adopt:
/// every `<store>/secrets/<name>.json` for which `keep(name)` is false (`name`
/// is the run id of a run's own bundle, the source id of a default one), and
/// the `.secrets.tmp-*` files a killed bundle write left. `keep` is true for
/// the runs that were captured but whose bundle is not in the vault yet; a call at supervisor start with such a `keep`
/// (or `|_| false`) is what clears the leftovers of a crash (S10). It takes
/// the same lock as [`capture_run`], so it never removes a bundle that a
/// capture in this or another process is about to adopt. Returns how many
/// were removed.
pub fn sweep_store_copies(
    store: &SourceStore,
    keep: impl Fn(&str) -> bool,
) -> Result<usize, ApiError> {
    let internal = |what: &str, e: &dyn std::fmt::Display| {
        ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
    };
    let _lock = store
        .lock_secrets()
        .map_err(|e| internal("secret capture lock", &format!("{e:#}")))?;
    let dir = store.root().join("secrets");
    let rd = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(internal("secret store directory", &e)),
    };
    let mut removed = 0;
    for entry in rd {
        let entry = entry.map_err(|e| internal("secret store directory", &e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_bundle_tmp(&name) {
            // under the lock of every capture that writes one: not in use
            match fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(internal("secret store file", &e)),
            }
            continue;
        }
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        if keep(stem) {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(internal("secret store file", &e)),
        }
    }
    Ok(removed)
}
