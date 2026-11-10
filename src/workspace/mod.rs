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
//!
//! Locks (per canonical clone, in `<clone>/.git`): adds of one commit share
//! the clone; moving its checkout to another commit is exclusive. A
//! writer-priority `intent` lock keeps a stream of adds of one commit from
//! starving an add of another: whoever wants the main lock passes through the
//! intent lock first, and a writer holds it while it waits for the readers to
//! drain. Removals and their branch cleanup use the same two locks.
//!
//! A repository that has a worker crash mid-way is repaired lazily: a create
//! of the same attempt clears what the dead one left, and
//! [`Workspaces::sweep_orphans`] (the worker's startup, S10) clears the
//! directories no record names.

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
    /// `git diff` and `git diff --cached` in the workspace show what the
    /// original's did (spec 6): the branch is at the original's HEAD and the
    /// index is the original's. `false` when the clone lacked the object to
    /// do it (see `warnings`); `true` when nothing was lost, which is also
    /// what a directory or in-place workspace says.
    pub index_reproduced: bool,
    /// What the supervisor reports as events: things that did not stop the
    /// workspace from being made but a user should know.
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl Workspace {
    fn plain(root: PathBuf, cwd: PathBuf, kind: WorkspaceKind) -> Self {
        Workspace {
            root,
            cwd,
            kind,
            index_reproduced: true,
            warnings: Vec::new(),
        }
    }
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
    /// later bundles has to be in it), or empty. An existing clone must have
    /// been made from the same `seed` (`Conflict` otherwise: its objects and
    /// its `origin` would be another repository's). Safe to call from several
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
        if dir.join(".git").exists() {
            let have = git::run(&self.env, &dir, &["remote", "get-url", "origin"]).ok();
            let same = match (seed, have.as_deref()) {
                (None, None) => true,
                (Some(s), Some(h)) => same_location(s, h),
                _ => false,
            };
            if !same {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "the canonical clone of {repository_id} was made from {have:?}, not from {seed:?}"
                    ),
                ));
            }
        } else {
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
            if std::fs::symlink_metadata(&dir).is_ok() {
                remove_tree(&dir).map_err(|e| io_err("cannot clear a broken clone", e))?;
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
                let made = store.materialize(m, &dest).map_err(internal).map(|()| {
                    Workspace::plain(dest.clone(), dest.join(&cwd_rel), WorkspaceKind::Directory)
                });
                if made.is_err() {
                    let _ = remove_tree(&dest);
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
        let clone = repo.dir();
        let branch = plan::branch_name(attempt);
        // what a create of this attempt that died left is cleared first
        self.reclaim(clone, dest, attempt)?;
        let added = {
            // Held for the whole `fast-worktree add`, not only for the
            // checkout: the project's `[[clone]]` rules copy dependencies out
            // of the clone's main checkout, which must stay at `commit` until
            // that is done.
            let _held = self.hold_clone_at(clone, commit)?;
            self.fw.add(clone, dest, &branch, commit)
        };
        let made = added
            .map_err(|f| plan::fw_error("add", &f))
            .and_then(|()| self.lay_source(store, m, dest))
            .and_then(|()| self.reproduce_head(repo, dest, m, commit))
            .map(|(index_reproduced, warnings)| Workspace {
                cwd: dest.join(m.cwd_relative.to_path()),
                root: dest.to_path_buf(),
                kind: WorkspaceKind::Worktree {
                    clone: clone.to_path_buf(),
                    branch: branch.clone(),
                },
                index_reproduced,
                warnings,
            });
        if made.is_err() {
            // best effort: the original error is the one to report
            let _ = self.teardown_worktree(clone, attempt);
        }
        made
    }

    /// The clone's lock, held, with its main checkout at `commit`. `fast-worktree`
    /// reads the project's configuration from that checkout, so it has to be
    /// at the commit the attempt runs. At the right commit already, adds
    /// share the clone (shared lock); moving it is exclusive, and waits for
    /// the adds in flight.
    fn hold_clone_at(&self, clone: &Path, commit: &str) -> Result<File, ApiError> {
        let shared = lock_shared(clone)?;
        if self.head_of(clone).as_deref() == Some(commit) {
            return Ok(shared);
        }
        // Releasing the shared lock and taking the exclusive one is not
        // atomic: another writer may move HEAD in between, so what was read
        // above is not trusted and HEAD is read again under the exclusive lock.
        drop(shared);
        let exclusive = lock_exclusive(clone)?;
        if self.head_of(clone).as_deref() != Some(commit) {
            git::run(
                &self.env,
                clone,
                &["checkout", "-q", "-f", "--detach", commit],
            )
            .map_err(internal)?;
        }
        Ok(exclusive)
    }

    fn head_of(&self, clone: &Path) -> Option<String> {
        git::run(&self.env, clone, &["rev-parse", "HEAD"]).ok()
    }

    /// What a create of `attempt` that died (a crash, a cleaned disk) left: a
    /// directory, the branch, a worktree registration. They are ours by their
    /// names. A directory with none of the others is not a leftover of ours
    /// and is a conflict.
    fn reclaim(&self, clone: &Path, dest: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir_exists = std::fs::symlink_metadata(dest).is_ok();
        if !dir_exists && !self.branch_exists(clone, attempt) && !self.registered(clone, attempt) {
            return Ok(());
        }
        if dir_exists && !self.branch_exists(clone, attempt) && !self.registered(clone, attempt) {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("the workspace of {attempt} already exists"),
            ));
        }
        self.teardown_worktree(clone, attempt)
    }

    fn branch_exists(&self, clone: &Path, attempt: &AttemptId) -> bool {
        let r = format!("refs/heads/{}", plan::branch_name(attempt));
        git::ok(&self.env, clone, &["rev-parse", "--verify", "-q", &r])
    }

    fn registered(&self, clone: &Path, attempt: &AttemptId) -> bool {
        let listing =
            git::run(&self.env, clone, &["worktree", "list", "--porcelain"]).unwrap_or_default();
        let mut roots = vec![self.workspaces_root()];
        roots.extend(std::fs::canonicalize(self.workspaces_root()).ok());
        plan::registered_for(&listing, &roots, attempt)
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

    /// The worktree was made at the execution commit (for a worktree capture,
    /// a synthetic commit on top of the original's HEAD whose tree is the
    /// captured files). Make `git diff` and `git diff --cached` show what the
    /// original's did: the branch goes back to the original's HEAD (the
    /// files stay), and the index becomes the original's `index_tree`. When
    /// the clone has no such tree yet, the index is the original HEAD's tree
    /// instead and the result says so (`false`, with a warning). The bundle
    /// carries `index_tree` once local-ci's source bundles do, after which this
    /// is always `true`.
    fn reproduce_head(
        &self,
        repo: &ObjectsRepo,
        dest: &Path,
        m: &CiManifest,
        commit: &str,
    ) -> Result<(bool, Vec<String>), ApiError> {
        let Some(head) = m.original_head.as_deref() else {
            return Ok((
                false,
                vec!["the source has no original HEAD: HEAD and the index are the execution commit's".into()],
            ));
        };
        if head == commit {
            // a commit capture: HEAD and the index are the commit's already
            return Ok((true, Vec::new()));
        }
        for id in [Some(head), m.index_tree.as_deref()].into_iter().flatten() {
            if !plan::is_commit_id(id) {
                return Err(ApiError::new(
                    ErrorCode::InvalidInput,
                    format!("{id:?} in the manifest is not an object id"),
                ));
            }
        }
        if !repo.has_commit(head) {
            return Ok((
                false,
                vec![format!(
                    "the original HEAD {head} is not in the canonical clone: HEAD and the index are the execution commit's"
                )],
            ));
        }
        let ceiling = self.workspaces_root();
        let git = |args: &[&str]| {
            git::run_in_worktree(&self.env, dest, &ceiling, args)
                .map(|_| ())
                .map_err(internal)
        };
        let tree = m.index_tree.as_deref().filter(|t| {
            git::ok(
                &self.env,
                repo.dir(),
                &["cat-file", "-e", &format!("{t}^{{tree}}")],
            )
        });
        match tree {
            Some(t) => {
                git(&["reset", "-q", "--soft", head])?;
                git(&["read-tree", t])?;
                Ok((true, Vec::new()))
            }
            None => {
                git(&["reset", "-q", head])?;
                Ok((
                    false,
                    vec![format!(
                        "the original's index tree is not in the canonical clone: the index is the original HEAD's tree ({head}), so `git diff --cached` shows nothing and `git diff` shows all the changes"
                    )],
                ))
            }
        }
    }

    /// The user's own worktree as the workspace of an `--in-place` run. Nothing
    /// is copied and nothing is removed later; the supervisor keeps
    /// [`plan::IN_PLACE_PROTECTED`] out of the run's reach. The directory has
    /// to be the top of a Git worktree (not the home directory, not the file
    /// system root), and the working directory, resolved, has to stay inside it.
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
        let home = self
            .env
            .get("HOME")
            .and_then(|h| std::fs::canonicalize(h).ok());
        plan::check_in_place(&real, cwd_relative, &state, home.as_deref(), parent_local)?;
        let top = git::run(&self.env, &real, &["rev-parse", "--show-toplevel"])
            .ok()
            .and_then(|t| std::fs::canonicalize(t).ok());
        if top.as_deref() != Some(real.as_path()) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!(
                    "--in-place needs the top directory of a Git worktree, {real:?} is not one"
                ),
            ));
        }
        let cwd = std::fs::canonicalize(real.join(cwd_relative)).map_err(|_| {
            ApiError::new(
                ErrorCode::NotFound,
                format!("the working directory {cwd_relative:?} does not exist in {real:?}"),
            )
        })?;
        if !cwd.starts_with(&real) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!("the working directory {cwd_relative:?} leaves {real:?}"),
            ));
        }
        if !cwd.is_dir() {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                format!("the working directory {cwd:?} is not a directory"),
            ));
        }
        Ok(Workspace::plain(real, cwd, WorkspaceKind::InPlace))
    }

    /// Remove a workspace made by [`Workspaces::create`] (idempotent). An
    /// in-place workspace is never removed. The record is checked before it is
    /// used to run anything: the directory has to be an attempt's under the
    /// workspaces root and, for a worktree, the clone and branch the ones
    /// `create` makes.
    ///
    /// The clone's `pre_remove` hook is read by `fast-worktree` from the
    /// clone's main checkout, which is at whichever commit the last add moved
    /// it to, not necessarily this attempt's. Aligning it would make every
    /// removal exclusive (serialized with all adds and removals of the
    /// repository, and flipping the checkout under adds in flight), so it is
    /// not done: a project's `pre_remove` hooks must not depend on the commit.
    pub fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
        if ws.kind == WorkspaceKind::InPlace {
            return Ok(());
        }
        let attempt = plan::check_owned(&self.workspaces_root(), &ws.root)?;
        match &ws.kind {
            WorkspaceKind::InPlace => Ok(()),
            WorkspaceKind::Directory => match std::fs::symlink_metadata(&ws.root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_err("cannot inspect the workspace", e)),
                Ok(m) if m.is_dir() => {
                    remove_tree(&ws.root).map_err(|e| io_err("cannot remove the workspace", e))
                }
                Ok(_) => Err(ApiError::new(
                    ErrorCode::Internal,
                    format!("{:?} is not a directory", ws.root),
                )),
            },
            WorkspaceKind::Worktree { clone, branch } => {
                plan::check_worktree_record(&self.clones_root(), clone, branch, &attempt)?;
                self.teardown_worktree(clone, &attempt)
            }
        }
    }

    /// The attempt's own worktree goes whatever the run left in it: tracked
    /// changes and untracked files are reset first (ignored files, the
    /// dependencies, are `fast-worktree`'s to delete), because it refuses a
    /// dirty one. That is done only to a directory that really is a worktree
    /// of this clone (a plain directory, or a `.git` that points elsewhere, is
    /// removed as a plain directory without running Git in it), and Git is
    /// given the worktree explicitly (`GIT_DIR`, `GIT_WORK_TREE`) so it cannot
    /// wander into a repository above. A symlink at the workspace's place is
    /// removed itself, never followed. Then the registration and the branch
    /// are cleared, whether or not the directory was still there.
    fn teardown_worktree(&self, clone: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir = plan::workspace_dir(&self.workspaces_root(), attempt);
        match std::fs::symlink_metadata(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("cannot inspect the workspace", e)),
            Ok(m) if m.is_dir() => {
                if self.owned_worktree(clone, &dir) && self.clean_worktree(&dir) {
                    // shared: a prune of the clone's worktree list waits for us
                    let _shared = lock_shared(clone)?;
                    self.fw
                        .remove(clone, &dir)
                        .map_err(|f| plan::fw_error("remove", &f))?;
                } else {
                    remove_tree(&dir).map_err(|e| io_err("cannot remove the workspace", e))?;
                }
            }
            Ok(_) => std::fs::remove_file(&dir)
                .map_err(|e| io_err("cannot remove what is at the workspace's place", e))?,
        }
        self.settle(clone, attempt)
    }

    /// Is `dir` a real directory (not a link) whose `.git` is a file that
    /// names a worktree of `clone`?
    fn owned_worktree(&self, clone: &Path, dir: &Path) -> bool {
        use std::io::Read;
        let real_dir = std::fs::symlink_metadata(dir).is_ok_and(|m| m.is_dir());
        let gitfile = dir.join(".git");
        let real_file = std::fs::symlink_metadata(&gitfile).is_ok_and(|m| m.is_file());
        if !real_dir || !real_file {
            return false;
        }
        let mut content = String::new();
        let read =
            std::fs::File::open(&gitfile).and_then(|f| f.take(4096).read_to_string(&mut content));
        if read.is_err() {
            return false;
        }
        let named = plan::gitdir_clone(&content).and_then(|c| std::fs::canonicalize(c).ok());
        named.is_some() && named == std::fs::canonicalize(clone).ok()
    }

    /// Reset and clean the worktree `dir` (already found to be a worktree of
    /// the clone), checking that Git succeeded.
    fn clean_worktree(&self, dir: &Path) -> bool {
        let ceiling = self.workspaces_root();
        ["reset", "clean"].iter().all(|c| {
            let args: &[&str] = if *c == "reset" {
                &["reset", "-q", "--hard"]
            } else {
                &["clean", "-q", "-fd"]
            };
            git::run_in_worktree(&self.env, dir, &ceiling, args).is_ok()
        })
    }

    /// Drop the attempt's branch and registration. A branch whose worktree is
    /// gone cannot be deleted until the registration is pruned, and pruning
    /// also drops the registration of a worktree another attempt is removing
    /// right now, so it is done only when it is needed, under the exclusive
    /// lock (removals hold the shared one).
    fn settle(&self, clone: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let branch = plan::branch_name(attempt);
        let clear = !self.registered(clone, attempt)
            && (!self.branch_exists(clone, attempt)
                || git::ok(&self.env, clone, &["branch", "-D", "-q", &branch]));
        if clear {
            return Ok(());
        }
        let _exclusive = lock_exclusive(clone)?;
        let _ = git::run(&self.env, clone, &["worktree", "prune"]);
        if self.branch_exists(clone, attempt) {
            git::run(&self.env, clone, &["branch", "-D", "-q", &branch]).map_err(internal)?;
        }
        Ok(())
    }

    /// Clear what workers that died left under the state directory: scratch
    /// directories (`workspaces/.staging.*`, `clones/.clone.*`) and attempt
    /// workspaces `has_record` does not know. Meant for the worker's startup
    /// (S10), before any create runs: a create's own scratch directory would
    /// look like a leftover. A leftover worktree is removed like any other
    /// (its clone is read from its `.git` file); anything that cannot be
    /// cleared is reported in `failed` and the sweep goes on.
    pub fn sweep_orphans(
        &self,
        has_record: &dyn Fn(&AttemptId) -> bool,
    ) -> Result<SweepReport, ApiError> {
        let mut report = SweepReport::default();
        let clones = canonical_or(&self.clones_root());
        for (root, scratch_only) in [(self.workspaces_root(), false), (self.clones_root(), true)] {
            let entries = match std::fs::read_dir(&root) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err("cannot read the state directory", e)),
            };
            for ent in entries.flatten() {
                let path = ent.path();
                let name = ent.file_name().to_string_lossy().into_owned();
                let done = match plan::classify_entry(&name) {
                    plan::Entry::Foreign => continue,
                    plan::Entry::Attempt(_) if scratch_only => continue,
                    plan::Entry::Attempt(a) if has_record(&a) => continue,
                    plan::Entry::Attempt(a) => self.sweep_attempt(&clones, &a),
                    plan::Entry::Scratch => remove_any(&path)
                        .map_err(|e| io_err("cannot remove a scratch directory", e)),
                };
                match done {
                    Ok(()) => report.removed.push(path),
                    Err(e) => report.failed.push((path, e.message)),
                }
            }
        }
        Ok(report)
    }

    fn sweep_attempt(&self, clones: &Path, attempt: &AttemptId) -> Result<(), ApiError> {
        let dir = plan::workspace_dir(&self.workspaces_root(), attempt);
        let named = std::fs::read_to_string(dir.join(".git"))
            .ok()
            .and_then(|c| plan::gitdir_clone(&c))
            .filter(|c| c.parent() == Some(clones));
        match named {
            Some(clone) => self.teardown_worktree(&clone, attempt),
            None => remove_any(&dir).map_err(|e| io_err("cannot remove a leftover workspace", e)),
        }
    }
}

/// What [`Workspaces::sweep_orphans`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub removed: Vec<PathBuf>,
    /// Entries that could not be cleared, with why.
    pub failed: Vec<(PathBuf, String)>,
}

fn canonical_or(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// The same place: written the same, or both existing paths that resolve to
/// one (a clone seeded by a URL is compared by the string).
fn same_location(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let (pa, pb) = (Path::new(a), Path::new(b));
    pa.is_absolute()
        && pb.is_absolute()
        && matches!(
            (std::fs::canonicalize(pa), std::fs::canonicalize(pb)),
            (Ok(x), Ok(y)) if x == y
        )
}

/// A directory tree removed even where the run left directories it cannot
/// read (a mode of 000): their permissions are given back first. Links are
/// not followed.
fn remove_tree(path: &Path) -> std::io::Result<()> {
    fn open_up(p: &Path) {
        if !std::fs::symlink_metadata(p).is_ok_and(|m| m.is_dir()) {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700));
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                open_up(&e.path());
            }
        }
    }
    open_up(path);
    std::fs::remove_dir_all(path)
}

/// A directory tree, or a file or link itself.
fn remove_any(path: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(path)?.is_dir() {
        remove_tree(path)
    } else {
        std::fs::remove_file(path)
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

/// The clone's main lock, shared. A writer that is waiting for it holds the
/// intent lock, so this queues behind the writer instead of joining the
/// readers that keep it from ever getting in.
fn lock_shared(clone: &Path) -> Result<File, ApiError> {
    lock_main(clone, File::lock_shared)
}

/// The clone's main lock, exclusive (writer priority: see [`lock_shared`]).
fn lock_exclusive(clone: &Path) -> Result<File, ApiError> {
    lock_main(clone, File::lock)
}

fn lock_main(clone: &Path, take: fn(&File) -> std::io::Result<()>) -> Result<File, ApiError> {
    let git = clone.join(".git");
    let intent = open_lock(&git.join("rtr-workspace.intent"))?;
    let main = open_lock(&git.join("rtr-workspace.lock"))?;
    let err = |e| io_err("cannot lock the clone", e);
    intent.lock().map_err(err)?;
    // the intent lock is released when `intent` is dropped, on any return
    take(&main).map_err(err)?;
    Ok(main)
}
