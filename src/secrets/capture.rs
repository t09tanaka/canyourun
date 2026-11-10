//! Capturing a run's source together with its secret files (spec 22.3).
//!
//! Which requests may carry secret files, for now (a stopgap until local-ci
//! can do the rest, see below):
//!
//! - `Worktree`: yes. The declared globs plus what `.worktreeinclude` matches
//!   are read from the working tree.
//! - `Commit`: refused (`INVALID_INPUT`) when secret files are declared or
//!   `.worktreeinclude` matches an ignored file. The spec's answer is to read
//!   them from the original worktree and send them with the commit, which
//!   needs local-ci to take a secret root separate from the clean clone the
//!   commit is checked out in. Until then a commit run is not silently sent
//!   without its secrets.
//! - `Directory`: refused likewise when secret files are declared or a
//!   `.worktreeinclude` names a pattern. local-ci's directory capture has no
//!   way to keep a secret file out of the snapshot, so its plaintext would
//!   become a blob in the object store.
//!
//! Both refusals are lifted when local-ci's secret root and secret separation
//! for a directory capture exist (the L1c work).
//!
//! The store's plaintext bundle (`<store>/secrets/<source>.json`) lives from
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

/// What decides the secret files of a capture.
pub struct SecretInputs<'a> {
    pub key: &'a ClusterKey,
    /// `[source] secret_files` of the project.
    pub declared: &'a [SecretSpec],
}

fn unsupported(what: &str) -> ApiError {
    ApiError::new(
        ErrorCode::InvalidInput,
        format!(
            "{what}: secret files are not supported for this kind of source yet (they are for a worktree); remove them from [source] secret_files and .worktreeinclude, or run from the working tree. Support follows with the next local-ci release"
        ),
    )
}

/// `capture_for_run` with secret files: the declared globs plus what
/// `.worktreeinclude` matches (taken from the working tree, so for a worktree
/// capture only), read by local-ci under the cluster key, and the bundle moved
/// into the run's own hold in `vault`. Blocking, like the rest of capture.
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
        SourceRequest::Worktree { dir } => ignored_matches(env, dir).map_err(|e| e.to_api())?,
        SourceRequest::Commit { dir, .. } => {
            if !declared.is_empty()
                || !ignored_matches(env, dir)
                    .map_err(|e| e.to_api())?
                    .is_empty()
            {
                return Err(unsupported("a commit capture"));
            }
            vec![]
        }
        SourceRequest::Directory { dir } => {
            if !declared.is_empty() || declares_patterns(dir).map_err(|e| e.to_api())? {
                return Err(unsupported("a directory capture"));
            }
            vec![]
        }
    };
    if let SourceRequest::Worktree { dir } = request {
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
    let params = CaptureParams {
        secret_files: specs,
        secret_key: key.as_bytes().to_vec(),
        ..params.clone()
    };
    let got = capture_for_run(env, store, run, request, &params)?;
    let entries = &got.manifest.secret_files;
    let copy = store.secrets_path(&got.manifest.ci_source_id);
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
/// every `<store>/secrets/<source>.json` for which `keep(source)` is false.
/// `keep` is true for the sources of runs that were captured but whose bundle
/// is not in the vault yet; a call at supervisor start with such a `keep`
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
