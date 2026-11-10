//! The worker's workspaces (spec 22.5, chapter 6).
//!
//! A repository has one canonical clone on the worker (where bundles are
//! received); every attempt of a Git source gets a worktree of it made by
//! `fast-worktree` at the source's execution commit (the project's
//! `.fast-worktree.toml` prepares the dependencies), then the verified files of
//! the source are laid over it. A directory snapshot gets a plain directory.
//! `exec --in-place` uses the user's own worktree and never removes it.
//!
//! All of it is blocking (Git, file I/O, child processes): call it from a
//! dedicated thread or `spawn_blocking`.

mod fastwt;
mod git;
mod overlay;
pub mod plan;

pub use fastwt::{CliFastWorktree, FastWorktree};
pub use plan::FwFailure;

use crate::error::{ApiError, ErrorCode};
use crate::ids::AttemptId;
use crate::source::classify::api_error_of;
use crate::source::{ObjectsRepo, SourceStore};
use git::Env;
use local_ci::source::SourceManifest as CiManifest;
use plan::Flavor;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What kind of workspace this is, and so how it goes away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceKind {
    /// A worktree of the canonical clone `clone`, on `branch`.
    Worktree { clone: PathBuf, branch: String },
    /// A plain directory the runner made.
    Directory,
    /// The user's own worktree: never removed, never copied.
    InPlace,
}

/// A prepared workspace: where the run's files are, and where it starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workspace {
    pub root: PathBuf,
    pub cwd: PathBuf,
    pub kind: WorkspaceKind,
}

/// The worker's workspaces under its state directory: `clones/` holds the
/// canonical clones, `workspaces/` one directory per attempt.
pub struct Workspaces {
    state_dir: PathBuf,
    env: Env,
    fw: Arc<dyn FastWorktree>,
}

fn internal(e: anyhow::Error) -> ApiError {
    api_error_of(&e)
}

fn io_err(what: &str, e: std::io::Error) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

impl Workspaces {
    pub fn new(state_dir: PathBuf, env: Env, fw: Arc<dyn FastWorktree>) -> Self {
        Workspaces { state_dir, env, fw }
    }

    pub fn workspaces_root(&self) -> PathBuf {
        self.state_dir.join("workspaces")
    }

    pub fn clones_root(&self) -> PathBuf {
        self.state_dir.join("clones")
    }

    /// The canonical clone of `repository_id`, made when it is not there yet:
    /// cloned from `seed` (a path or URL of the repository; the merge base of
    /// later bundles has to be in it), or empty. Safe to call from several
    /// threads and processes at once.
    pub fn ensure_clone(
        &self,
        repository_id: &str,
        seed: Option<&str>,
    ) -> Result<ObjectsRepo, ApiError> {
        let root = self.clones_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the clones directory", e))?;
        let name = plan::clone_dir_name(repository_id);
        let dir = root.join(&name);
        let lock = open_lock(&root.join(format!("{name}.lock")))?;
        lock.lock()
            .map_err(|e| io_err("cannot lock the clone", e))?;
        if !dir.join(".git").exists() {
            // build beside, move into place: a crash leaves no half clone
            let tmp = tempfile::Builder::new()
                .prefix(".clone.")
                .tempdir_in(&root)
                .map_err(|e| io_err("cannot make a scratch directory", e))?;
            let work = tmp.path().join("c");
            match seed {
                Some(s) => {
                    let w = work.to_string_lossy();
                    git::run(&self.env, tmp.path(), &["clone", "-q", "--", s, &w])
                        .map_err(internal)?
                }
                None => {
                    std::fs::create_dir(&work).map_err(|e| io_err("cannot create the clone", e))?;
                    git::run(&self.env, &work, &["init", "-q"]).map_err(internal)?
                }
            };
            // a leftover directory without `.git` is ours (we hold the lock)
            if dir.exists() {
                std::fs::remove_dir_all(&dir)
                    .map_err(|e| io_err("cannot clear a broken clone", e))?;
            }
            std::fs::rename(&work, &dir).map_err(|e| io_err("cannot place the clone", e))?;
        }
        Ok(ObjectsRepo::new(self.env.clone(), dir))
    }

    /// Bring the clone up to date with its `origin`, when it has one.
    pub fn update_clone(&self, clone: &ObjectsRepo) -> Result<(), ApiError> {
        let has_origin = git::run(&self.env, clone.dir(), &["remote"])
            .map_err(internal)?
            .lines()
            .any(|l| l == "origin");
        if has_origin {
            git::run(
                &self.env,
                clone.dir(),
                &["fetch", "-q", "--no-tags", "origin"],
            )
            .map_err(internal)?;
        }
        Ok(())
    }

    /// The workspace of `attempt` for the source `m`. A Git source needs its
    /// canonical clone to hold the execution commit already (received by
    /// bundle); `repo` is that clone. On any failure nothing of the workspace
    /// is left behind.
    pub fn create(
        &self,
        attempt: &AttemptId,
        store: &SourceStore,
        m: &CiManifest,
        repo: Option<&ObjectsRepo>,
    ) -> Result<Workspace, ApiError> {
        let flavor = plan::flavor_of(m.execution_commit.as_deref(), m.repository_id.as_deref())?;
        let cwd_rel = m.cwd_relative.to_path();
        plan::check_cwd_relative(&cwd_rel)?;
        let root = self.workspaces_root();
        std::fs::create_dir_all(&root)
            .map_err(|e| io_err("cannot create the workspaces directory", e))?;
        let dest = plan::workspace_dir(&root, attempt);
        match flavor {
            Flavor::Directory => {
                std::fs::create_dir(&dest).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        ApiError::new(
                            ErrorCode::Conflict,
                            format!("the workspace of {attempt} already exists"),
                        )
                    } else {
                        io_err("cannot create the workspace", e)
                    }
                })?;
                let made = store
                    .materialize(m, &dest)
                    .map_err(internal)
                    .map(|()| Workspace {
                        cwd: dest.join(&cwd_rel),
                        root: dest.clone(),
                        kind: WorkspaceKind::Directory,
                    });
                if made.is_err() {
                    let _ = std::fs::remove_dir_all(&dest);
                }
                made
            }
            Flavor::Git { commit, .. } => {
                let repo = repo.ok_or_else(|| {
                    ApiError::new(
                        ErrorCode::InvalidInput,
                        "a Git source needs the repository's canonical clone",
                    )
                })?;
                self.create_git(attempt, store, m, repo, &commit, &dest)
            }
        }
    }

    fn create_git(
        &self,
        attempt: &AttemptId,
        store: &SourceStore,
        m: &CiManifest,
        repo: &ObjectsRepo,
        commit: &str,
        dest: &Path,
    ) -> Result<Workspace, ApiError> {
        if !repo.has_commit(commit) {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                format!("the execution commit {commit} is not in the canonical clone"),
            ));
        }
        if std::fs::symlink_metadata(dest).is_ok() {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("the workspace of {attempt} already exists"),
            ));
        }
        let clone = repo.dir();
        let branch = plan::branch_name(attempt);
        // a branch left by a create that died: cleared before anything else
        self.clear_stale(clone, &branch)?;
        // `fast-worktree` reads the project's configuration from the clone's
        // own checkout: it has to be at the commit the attempt runs. Adds at
        // the commit it is at share the clone; moving it waits for them.
        let lock = open_lock(&clone.join(".git").join("rtr-workspace.lock"))?;
        lock.lock_shared()
            .map_err(|e| io_err("cannot lock the clone", e))?;
        let at = git::run(&self.env, clone, &["rev-parse", "HEAD"]).ok();
        let _held = if at.as_deref() == Some(commit) {
            lock
        } else {
            lock.unlock()
                .map_err(|e| io_err("cannot unlock the clone", e))?;
            lock.lock()
                .map_err(|e| io_err("cannot lock the clone", e))?;
            git::run(
                &self.env,
                clone,
                &["checkout", "-q", "-f", "--detach", commit],
            )
            .map_err(internal)?;
            lock
        };
        self.fw
            .add(clone, dest, &branch, commit)
            .map_err(|f| plan::fw_error("add", &f))
            .and_then(|()| {
                self.lay_source(store, m, dest).map(|()| Workspace {
                    cwd: dest.join(m.cwd_relative.to_path()),
                    root: dest.to_path_buf(),
                    kind: WorkspaceKind::Worktree {
                        clone: clone.to_path_buf(),
                        branch: branch.clone(),
                    },
                })
            })
            .inspect_err(|_| {
                // best effort: the original error is the one to report
                let _ = self.teardown_worktree(clone, dest, &branch, true);
            })
    }

    /// A branch left by a create that died before it could clean up. Pruning
    /// the clone's worktree list would also drop the registration of a
    /// worktree another attempt is removing right now, so it is done only when
    /// there is a stale branch, and under the exclusive lock (removals hold the
    /// shared one).
    fn clear_stale(&self, clone: &Path, branch: &str) -> Result<(), ApiError> {
        let exists = git::run(
            &self.env,
            clone,
            &[
                "rev-parse",
                "--verify",
                "-q",
                &format!("refs/heads/{branch}"),
            ],
        )
        .is_ok();
        if !exists {
            return Ok(());
        }
        let lock = open_lock(&clone.join(".git").join("rtr-workspace.lock"))?;
        lock.lock()
            .map_err(|e| io_err("cannot lock the clone", e))?;
        let _ = git::run(&self.env, clone, &["worktree", "prune"]);
        let _ = git::run(&self.env, clone, &["branch", "-D", "-q", branch]);
        Ok(())
    }

    /// Unpack the source into a scratch directory (verified there, working
    /// directory included), then lay it over the worktree and check the
    /// working directory again.
    fn lay_source(&self, store: &SourceStore, m: &CiManifest, dest: &Path) -> Result<(), ApiError> {
        let staging = tempfile::Builder::new()
            .prefix(".staging.")
            .tempdir_in(self.workspaces_root())
            .map_err(|e| io_err("cannot make a scratch directory", e))?;
        store.materialize(m, staging.path()).map_err(internal)?;
        overlay::overlay(staging.path(), dest).map_err(internal)?;
        let cwd = dest.join(m.cwd_relative.to_path());
        match std::fs::symlink_metadata(&cwd) {
            Ok(meta) if meta.is_dir() => Ok(()),
            _ => Err(ApiError::new(
                ErrorCode::Internal,
                format!("the working directory {cwd:?} is not a directory in the workspace"),
            )),
        }
    }

    /// The user's own worktree as the workspace of an `--in-place` run. Nothing
    /// is copied and nothing is removed later; the supervisor keeps
    /// [`plan::IN_PLACE_PROTECTED`] out of the run's reach.
    pub fn open_in_place(
        &self,
        dir: &Path,
        cwd_relative: &Path,
        parent_local: bool,
    ) -> Result<Workspace, ApiError> {
        let real = std::fs::canonicalize(dir)
            .map_err(|e| ApiError::new(ErrorCode::NotFound, format!("{dir:?}: {e}")))?;
        if !real.is_dir() {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!("{real:?} is not a directory"),
            ));
        }
        // the state directory may not exist yet: compare against what is there
        let state = std::fs::canonicalize(&self.state_dir).unwrap_or(self.state_dir.clone());
        plan::check_in_place(&real, cwd_relative, &state, parent_local)?;
        let cwd = real.join(cwd_relative);
        if !cwd.is_dir() {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                format!("the working directory {cwd:?} does not exist"),
            ));
        }
        Ok(Workspace {
            root: real,
            cwd,
            kind: WorkspaceKind::InPlace,
        })
    }

    /// Remove a workspace made by [`Workspaces::create`] (idempotent). An
    /// in-place workspace is never removed.
    pub fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
        if ws.kind == WorkspaceKind::InPlace {
            return Ok(());
        }
        plan::check_owned(&self.workspaces_root(), &ws.root)?;
        match &ws.kind {
            WorkspaceKind::InPlace => Ok(()),
            WorkspaceKind::Directory => match std::fs::symlink_metadata(&ws.root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_err("cannot inspect the workspace", e)),
                Ok(m) if m.is_dir() => std::fs::remove_dir_all(&ws.root)
                    .map_err(|e| io_err("cannot remove the workspace", e)),
                Ok(_) => Err(ApiError::new(
                    ErrorCode::Internal,
                    format!("{:?} is not a directory", ws.root),
                )),
            },
            WorkspaceKind::Worktree { clone, branch } => {
                self.teardown_worktree(clone, &ws.root, branch, false)
            }
        }
    }

    /// The attempt's own worktree goes whatever the run left in it: tracked
    /// changes and untracked files are reset first (ignored files, the
    /// dependencies, are `fast-worktree`'s to delete), because it refuses a
    /// dirty one.
    fn teardown_worktree(
        &self,
        clone: &Path,
        dir: &Path,
        branch: &str,
        holding_lock: bool,
    ) -> Result<(), ApiError> {
        if std::fs::symlink_metadata(dir).is_ok() {
            // the worktree may be half made: these can fail without mattering
            let _ = git::run(&self.env, dir, &["reset", "-q", "--hard"]);
            let _ = git::run(&self.env, dir, &["clean", "-q", "-fd"]);
            // shared: a prune of the clone's worktree list waits for us
            let _shared = if holding_lock {
                None
            } else {
                let l = open_lock(&clone.join(".git").join("rtr-workspace.lock"))?;
                l.lock_shared()
                    .map_err(|e| io_err("cannot lock the clone", e))?;
                Some(l)
            };
            self.fw
                .remove(clone, dir)
                .map_err(|f| plan::fw_error("remove", &f))?;
        }
        // (a registration whose directory is gone is the next create's to prune)
        let _ = git::run(&self.env, clone, &["branch", "-D", "-q", branch]);
        Ok(())
    }
}

fn open_lock(path: &Path) -> Result<File, ApiError> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(|e| io_err("cannot open a lock file", e))
}
