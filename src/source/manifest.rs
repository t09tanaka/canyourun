//! The `SourceManifest` of spec chapter 6, as remote-task-runner sees it.
//!
//! The content, and with it the identity, belong to local-ci's sealed
//! [`CiManifest`] (schema version 2): its `manifest_digest` covers every
//! field, and `source_id` is the first 128 bits of that digest. This type is
//! the view the runner passes around; it does not compute a digest of its own.
//!
//! | spec field            | here                                   | local-ci field        |
//! | --------------------- | -------------------------------------- | --------------------- |
//! | source_id             | `source_id` (`src_<ULID>`, one to one) | `source_id` (`src-<hex>`, kept as `ci_source_id`) |
//! | manifest_digest       | `manifest_digest`                      | same                  |
//! | repository_id         | `repository_id`                        | same                  |
//! | source_kind           | `source_kind`                          | same, but the *requested* kind (a commit capture runs through a clean clone, which local-ci calls a worktree) |
//! | original_head / index_tree / worktree_tree | same names       | same                  |
//! | execution_commit      | `execution_commit`                     | same                  |
//! | base / merge_base / branch | same names                        | same                  |
//! | cwd_relative          | `cwd_relative`                         | same                  |
//! | object_manifest       | `object_manifest` (entries, tree digest, `git_tips`) | same    |
//! | submodules / lfs      | `submodules`, `lfs`                    | same                  |
//! | secret_files          | `secret_files` (path + HMAC only)      | same                  |
//!
//! `canonical` (the settings that make Git's blobs out of the captured bytes)
//! is not in the spec's table but a worker needs it, so it is carried too.

use super::classify::source_id_of_ci;
use crate::ids::SourceId;
use local_ci::source::{
    CanonicalConfig, LfsRef, ObjectManifest, RelPath, SecretEntry, SourceKind,
    SourceManifest as CiManifest, SubmoduleRef,
};
use serde::{Deserialize, Serialize};

/// What was asked for. A commit capture is reported as `Commit` here even
/// though local-ci sees a checkout of it.
pub type Kind = SourceKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceManifest {
    pub source_id: SourceId,
    /// local-ci's name of the same source, the key of its store.
    pub ci_source_id: String,
    pub manifest_digest: String,
    pub repository_id: Option<String>,
    pub source_kind: Kind,
    pub original_head: Option<String>,
    pub index_tree: Option<String>,
    pub worktree_tree: Option<String>,
    pub execution_commit: Option<String>,
    pub base: Option<String>,
    pub merge_base: Option<String>,
    pub branch: Option<String>,
    pub cwd_relative: RelPath,
    pub object_manifest: ObjectManifest,
    pub submodules: Vec<SubmoduleRef>,
    pub lfs: Vec<LfsRef>,
    pub canonical: CanonicalConfig,
    /// HMAC digests and paths; never content (chapter 22.3).
    pub secret_files: Vec<SecretEntry>,
}

impl SourceManifest {
    /// The view of a sealed local-ci manifest. `None` when its `source_id`
    /// is not one that local-ci's seal produces.
    pub fn from_ci(m: &CiManifest, requested: Kind) -> Option<Self> {
        Some(SourceManifest {
            source_id: source_id_of_ci(&m.source_id)?,
            ci_source_id: m.source_id.clone(),
            manifest_digest: m.manifest_digest.clone(),
            repository_id: m.repository_id.clone(),
            source_kind: requested,
            original_head: m.original_head.clone(),
            index_tree: m.index_tree.clone(),
            worktree_tree: m.worktree_tree.clone(),
            execution_commit: m.execution_commit.clone(),
            base: m.base.clone(),
            merge_base: m.merge_base.clone(),
            branch: m.branch.clone(),
            cwd_relative: m.cwd_relative.clone(),
            object_manifest: m.object_manifest.clone(),
            submodules: m.submodules.clone(),
            lfs: m.lfs.clone(),
            canonical: m.canonical.clone(),
            secret_files: m.secret_files.clone(),
        })
    }
}
