//! Capturing a run's source together with its secret files (spec 22.3).

use super::key::ClusterKey;
use super::plan::merge_specs;
use super::vault::Vault;
use super::worktreeinclude::ignored_matches;
use crate::error::ApiError;
use crate::ids::RunId;
use crate::source::{capture_for_run, CaptureParams, Captured, SourceRequest, SourceStore};
use local_ci::source::SecretSpec;
use std::collections::BTreeMap;

/// What decides the secret files of a capture.
pub struct SecretInputs<'a> {
    pub key: &'a ClusterKey,
    /// `[source] secret_files` of the project.
    pub declared: &'a [SecretSpec],
}

/// `capture_for_run` with secret files: the declared globs plus what
/// `.worktreeinclude` matches (taken from the working tree, so for a worktree
/// capture only: a commit capture runs through a clean clone, which holds no
/// ignored file, and a plain directory has no Git to ask), read by local-ci
/// under the cluster key, and the bundle moved into the run's own hold in
/// `vault`. Blocking, like the rest of capture.
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
    let included = match request {
        SourceRequest::Worktree { dir } => ignored_matches(env, dir).map_err(|e| e.to_api())?,
        _ => vec![],
    };
    let params = CaptureParams {
        secret_files: merge_specs(declared, &included),
        secret_key: key.as_bytes().to_vec(),
        ..params.clone()
    };
    let got = capture_for_run(env, store, run, request, &params)?;
    if let Some(path) = &got.secrets {
        vault.adopt(run, path).map_err(|e| e.to_api())?;
    }
    Ok(got)
}
