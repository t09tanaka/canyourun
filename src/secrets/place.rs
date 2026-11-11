//! Secret files on the worker (spec 22.3, "workerでの配置" and "消去").
//!
//! Placement checks everything before it writes anything: the limits, every
//! path, that each file is one the attempt's source manifest names (path and
//! HMAC), and every file's HMAC under the cluster key. Files are then written
//! into the attempt workspace as 0600. The path is walked one component at a
//! time with `openat(O_NOFOLLOW | O_DIRECTORY)` from a descriptor of the
//! workspace, so a link swapped in while the walk runs is refused, not
//! followed; directories this makes are 0700; the file itself is created with
//! `O_CREAT | O_EXCL | O_NOFOLLOW`. Nothing is written outside the workspace,
//! and nothing is kept anywhere else: not in the object store, a cache, a
//! template, a log or an artifact.
//!
//! Which files were placed is recorded first, in `<state>/secret-index/
//! <attempt>.json` (0600; paths only, never a content), so that the erase
//! after the attempt and the reconcile at worker start know what to remove
//! even after a crash in between. Erasing runs after the attempt's own
//! commands have run in that workspace, so it does not trust the tree: it
//! walks the same way, removes the file itself (a symlink standing in for it
//! is removed, never its target) and gives up on a path whose parent is no
//! longer a plain directory. Such a path, or an index that cannot be read,
//! means the secret file cannot be proven gone: the erase fails, the record
//! stays, and the whole workspace must be deleted
//! ([`SecretError::WorkspaceRequired`]; [`ReconcileReport::workspace_required`]).
//!
//! Before an attempt's commands run, the agent (S10) calls
//! [`WorkerSecrets::ensure_placed`]: the files in place must be exactly the
//! ones the source manifest names. A bundle of another run is refused at
//! [`WorkerSecrets::place`] already, since its files are not in this
//! attempt's manifest.

use super::envfile::{secret_values, Values};
use super::key::ClusterKey;
use super::plan::{
    check_limits, check_paths, ct_eq, to_wire_digest, Limit, MAX_FILES, MAX_FILE_BYTES,
};
use super::vault::{read_private, sweep_stale_tmp, write_private, STALE_TMP_AGE};
use super::{Result, SecretError};
use crate::ids::AttemptId;
use crate::wire::SecretFile;
use local_ci::source::{secret_digest, RelPath, SecretEntry};
use rustix::fs::{AtFlags, FileType, Mode, OFlags, CWD};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Record {
    attempt_id: String,
    workspace: String,
    paths: Vec<String>,
}

/// What an erase did. An erase that could not prove a file gone is an error,
/// not a report.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EraseReport {
    pub removed: Vec<String>,
    /// Already absent.
    pub gone: Vec<String>,
}

/// What a reconcile pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub erased: Vec<AttemptId>,
    pub kept: Vec<AttemptId>,
    /// Attempts whose secret files cannot be proven gone one by one (the index
    /// is unreadable, or a path is no longer reachable): the caller deletes
    /// the whole workspace, then calls [`WorkerSecrets::forget`]. The records
    /// stay until then.
    pub workspace_required: Vec<AttemptId>,
    /// Records that could not be handled: the file name and why (no values).
    pub failed: Vec<(String, String)>,
    /// Stale temporary files removed from the index directory.
    pub stale_tmp_removed: usize,
}

pub struct WorkerSecrets {
    index_dir: PathBuf,
}

/// A file as the file system knows it, whatever name leads to it: another
/// spelling the file system folds to the same entry, or a hard link, is the
/// same file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
}

impl FileIdentity {
    pub fn of(meta: &fs::Metadata) -> Self {
        FileIdentity {
            dev: meta.dev(),
            ino: meta.ino(),
        }
    }
}

/// What [`WorkerSecrets::placed`] reads from the files in place: the values
/// to mask, and which files they are (one per placed file, taken from the
/// handle the values were read through).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Placed {
    pub values: Values,
    pub files: Vec<FileIdentity>,
}

fn placement(msg: impl Into<String>) -> SecretError {
    SecretError::Placement(msg.into())
}

enum Step {
    Removed,
    Gone,
    Unreachable,
}

const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

fn errno_io(e: Errno) -> std::io::Error {
    e.into()
}

/// Not a directory, or a link standing where one should be.
fn is_not_dir(e: Errno) -> bool {
    e == Errno::LOOP || e == Errno::NOTDIR
}

enum Walk {
    Dir(OwnedFd),
    Gone,
    Unreachable,
}

/// The descriptor of the directory holding `rel`'s last component, walked
/// from `root` without following a link. Nothing is created.
fn walk_existing(root: &Path, parents: &[&str], rel: &str) -> Result<Walk> {
    let mut dir = match rustix::fs::openat(CWD, root, DIR_FLAGS, Mode::empty()) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(Walk::Gone),
        Err(e) if is_not_dir(e) => return Ok(Walk::Unreachable),
        Err(e) => return Err(SecretError::io(format!("workspace for {rel}"), errno_io(e))),
    };
    for c in parents {
        dir = match rustix::fs::openat(dir.as_fd(), *c, DIR_FLAGS, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return Ok(Walk::Gone),
            Err(e) if is_not_dir(e) => return Ok(Walk::Unreachable),
            Err(e) => return Err(SecretError::io(format!("directory of {rel}"), errno_io(e))),
        };
    }
    Ok(Walk::Dir(dir))
}

fn split_rel(rel: &str) -> Result<(&str, Vec<&str>)> {
    let mut comps: Vec<&str> = rel.split('/').collect();
    let last = comps
        .pop()
        .filter(|l| !l.is_empty())
        .ok_or_else(|| placement("empty secret file path"))?;
    Ok((last, comps))
}

impl WorkerSecrets {
    /// `<state_dir>/secret-index`, made 0700 when missing; a link or a
    /// directory others can read is refused.
    pub fn open(state_dir: &Path) -> Result<Self> {
        let dir = state_dir.join("secret-index");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        let meta = fs::symlink_metadata(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        if !meta.file_type().is_dir() || meta.mode() & 0o077 != 0 {
            return Err(SecretError::Insecure {
                what: "secret index directory",
                path: dir,
            });
        }
        Ok(WorkerSecrets { index_dir: dir })
    }

    fn record_path(&self, attempt: &AttemptId) -> PathBuf {
        self.index_dir.join(format!("{attempt}.json"))
    }

    fn load_record(&self, attempt: &AttemptId) -> Result<Option<Record>> {
        match read_private(&self.record_path(attempt), "secret index") {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| SecretError::Format("the secret index is not readable".into())),
            Err(SecretError::Unavailable { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn save_record(&self, attempt: &AttemptId, rec: &Record) -> Result<()> {
        let bytes = serde_json::to_vec(rec)
            .map_err(|_| SecretError::Format("cannot write the secret index".into()))?;
        write_private(&self.record_path(attempt), &bytes)
    }

    fn remove_record(&self, attempt: &AttemptId) -> Result<()> {
        match fs::remove_file(self.record_path(attempt)) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                Err(SecretError::io("secret index".to_string(), e))
            }
            _ => Ok(()),
        }
    }

    /// Place `files` (one request's worth; more may follow for the same
    /// attempt) in `workspace`. `expected` is `secret_files` of the attempt's
    /// source manifest: a file that is not in it, or whose HMAC differs from
    /// the manifest's, is refused (a bundle of another run, or of another
    /// capture). Returns how many are in place. A failure leaves none of this
    /// call's new files behind.
    pub fn place(
        &self,
        key: &ClusterKey,
        attempt: &AttemptId,
        workspace: &Path,
        files: &[SecretFile],
        expected: &[SecretEntry],
    ) -> Result<u32> {
        // 1. everything is checked before anything is written
        let sizes: Vec<(usize, usize)> = files
            .iter()
            .map(|f| (f.path.len(), f.content.0.len()))
            .collect();
        check_limits(&sizes).map_err(SecretError::Limit)?;
        check_paths(files.iter().map(|f| f.path.as_str())).map_err(SecretError::BadPath)?;
        for f in files {
            let rel = RelPath(f.path.clone().into_bytes());
            let entry = expected.iter().find(|e| e.path == rel).ok_or_else(|| {
                placement(format!(
                    "{} is not a secret file of this attempt's source",
                    f.path
                ))
            })?;
            if !ct_eq(entry.hmac.as_bytes(), f.hmac_digest.as_str().as_bytes()) {
                return Err(SecretError::Tampered {
                    path: f.path.clone(),
                });
            }
            let want = secret_digest(key.as_bytes(), &rel, &f.content.0);
            let want = to_wire_digest(&want)
                .ok_or_else(|| SecretError::Format("unexpected digest form".into()))?;
            if !ct_eq(want.as_str().as_bytes(), f.hmac_digest.as_str().as_bytes()) {
                return Err(SecretError::Tampered {
                    path: f.path.clone(),
                });
            }
        }
        // 2. the workspace is a real directory
        let meta = fs::symlink_metadata(workspace)
            .map_err(|e| SecretError::io(format!("workspace {}", workspace.display()), e))?;
        if !meta.file_type().is_dir() {
            return Err(placement("the workspace is not a plain directory"));
        }
        let root = fs::canonicalize(workspace)
            .map_err(|e| SecretError::io(format!("workspace {}", workspace.display()), e))?;
        let root_text = root
            .to_str()
            .ok_or_else(|| placement("the workspace path is not valid UTF-8"))?
            .to_string();
        // 3. the record, before the first file
        let mut rec = self.load_record(attempt)?.unwrap_or(Record {
            attempt_id: attempt.to_string(),
            workspace: root_text.clone(),
            paths: vec![],
        });
        if rec.workspace != root_text {
            return Err(placement(
                "this attempt already has secret files in another workspace",
            ));
        }
        let prior: BTreeSet<String> = rec.paths.iter().cloned().collect();
        let mut all = prior.clone();
        all.extend(files.iter().map(|f| f.path.clone()));
        if all.len() > MAX_FILES {
            return Err(SecretError::Limit(Limit::TooManyFiles { count: all.len() }));
        }
        rec.paths = all.into_iter().collect();
        self.save_record(attempt, &rec)?;
        // 4. place; undo this call's files on failure
        let mut created: Vec<&str> = Vec::new();
        for f in files {
            match place_one(&root, &f.path, &f.content.0, prior.contains(&f.path)) {
                Ok(true) => created.push(&f.path),
                Ok(false) => {}
                Err(e) => {
                    for p in created {
                        let _ = erase_one(&root, p);
                    }
                    return Err(e);
                }
            }
        }
        Ok(files.len() as u32)
    }

    /// Before the attempt's commands run: the files in place are exactly the
    /// ones `expected` (`secret_files` of the attempt's source manifest)
    /// names, each a regular 0600 file. A manifest with no secret file needs
    /// none placed. The agent (S10) calls this once every
    /// `secret_bundle_put` has been answered and before it starts the
    /// attempt; a missing, extra or replaced file stops the attempt.
    pub fn ensure_placed(&self, attempt: &AttemptId, expected: &[SecretEntry]) -> Result<()> {
        let want: BTreeSet<&[u8]> = expected.iter().map(|e| e.path.0.as_slice()).collect();
        let rec = self.load_record(attempt)?;
        let have: BTreeSet<&[u8]> = rec
            .iter()
            .flat_map(|r| r.paths.iter().map(|p| p.as_bytes()))
            .collect();
        if want != have {
            return Err(SecretError::PathSetMismatch);
        }
        let Some(rec) = rec else {
            return Ok(());
        };
        let root = PathBuf::from(&rec.workspace);
        for p in &rec.paths {
            let (last, parents) = split_rel(p)?;
            let Walk::Dir(dir) = walk_existing(&root, &parents, p)? else {
                return Err(placement(format!("{p} is not in the workspace")));
            };
            let st =
                rustix::fs::statat(dir.as_fd(), last, AtFlags::SYMLINK_NOFOLLOW).map_err(|e| {
                    if e == Errno::NOENT {
                        placement(format!("{p} is not in the workspace"))
                    } else {
                        SecretError::io(format!("secret file {p}"), errno_io(e))
                    }
                })?;
            let raw = st.st_mode as rustix::fs::RawMode;
            let regular = FileType::from_raw_mode(raw) == FileType::RegularFile;
            if !regular || Mode::from_raw_mode(raw).bits() & 0o777 != 0o600 {
                return Err(placement(format!(
                    "{p} is not a regular file with mode 0600"
                )));
            }
        }
        Ok(())
    }

    /// The strings to mask in what the attempt's commands print: the
    /// `KEY=VALUE` values of every file placed for it (the same rule as the
    /// coordinator's [`super::vault::HeldBundle::mask_values`]), read from the
    /// files as they are in the workspace now. Read from the files and not
    /// kept in memory, so an agent that restarted after the bundle arrived
    /// masks the same values. Call it after [`WorkerSecrets::ensure_placed`]
    /// and before the commands start. A file that is missing, not a regular
    /// 0600 file, reached through a link or larger than a placed file can be
    /// is an error: the attempt is not started with something unmasked.
    pub fn mask_values(&self, attempt: &AttemptId) -> Result<Values> {
        Ok(self.placed(attempt)?.values)
    }

    /// [`WorkerSecrets::mask_values`], and with them the identity of every
    /// file they were read from: what the supervisor refuses to keep as an
    /// artifact under any name.
    pub fn placed(&self, attempt: &AttemptId) -> Result<Placed> {
        let mut all = Values::default();
        let mut files = Vec::new();
        let Some(rec) = self.load_record(attempt)? else {
            return Ok(Placed::default());
        };
        let root = PathBuf::from(&rec.workspace);
        let mut skipped = BTreeSet::new();
        for p in &rec.paths {
            let gone = || placement(format!("{p} is not in the workspace"));
            let (last, parents) = split_rel(p)?;
            let Walk::Dir(dir) = walk_existing(&root, &parents, p)? else {
                return Err(gone());
            };
            let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
            let fd = rustix::fs::openat(dir.as_fd(), last, flags, Mode::empty()).map_err(|e| {
                if e == Errno::NOENT || is_not_dir(e) {
                    gone()
                } else {
                    SecretError::io(format!("secret file {p}"), errno_io(e))
                }
            })?;
            let file = File::from(fd);
            let meta = file
                .metadata()
                .map_err(|e| SecretError::io(format!("secret file {p}"), e))?;
            if !meta.is_file() || meta.mode() & 0o777 != 0o600 {
                return Err(placement(format!(
                    "{p} is not a regular file with mode 0600"
                )));
            }
            files.push(FileIdentity::of(&meta));
            let mut content = Vec::new();
            file.take(MAX_FILE_BYTES as u64 + 1)
                .read_to_end(&mut content)
                .map_err(|e| SecretError::io(format!("secret file {p}"), e))?;
            if content.len() > MAX_FILE_BYTES {
                return Err(placement(format!("{p} grew since it was placed")));
            }
            let v = secret_values(&content);
            for x in v.values {
                if !all.values.contains(&x) {
                    all.values.push(x);
                }
            }
            skipped.extend(v.skipped_keys);
        }
        all.skipped_keys = skipped.into_iter().collect();
        Ok(Placed { values: all, files })
    }

    /// Remove the files placed for `attempt` and forget them. Idempotent.
    /// When a path cannot be reached (a parent became a link or a file) or the
    /// index cannot be read, nothing is claimed: the record stays and the
    /// error is [`SecretError::WorkspaceRequired`].
    pub fn erase(&self, attempt: &AttemptId) -> Result<EraseReport> {
        let mut report = EraseReport::default();
        let rec = match self.load_record(attempt) {
            Ok(Some(rec)) => rec,
            Ok(None) => return Ok(report),
            Err(SecretError::Format(_) | SecretError::Insecure { .. }) => {
                return Err(SecretError::WorkspaceRequired {
                    attempt: attempt.to_string(),
                })
            }
            Err(e) => return Err(e),
        };
        let root = PathBuf::from(&rec.workspace);
        let mut first_error: Option<SecretError> = None;
        let mut unreachable = false;
        for p in &rec.paths {
            match erase_one(&root, p) {
                Ok(Step::Removed) => report.removed.push(p.clone()),
                Ok(Step::Gone) => report.gone.push(p.clone()),
                Ok(Step::Unreachable) => unreachable = true,
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_error {
            // the record stays, so a later pass tries again
            return Err(e);
        }
        if unreachable {
            return Err(SecretError::WorkspaceRequired {
                attempt: attempt.to_string(),
            });
        }
        self.remove_record(attempt)?;
        Ok(report)
    }

    /// Drop the record of `attempt` once its whole workspace was deleted (the
    /// way out of [`SecretError::WorkspaceRequired`]). Idempotent.
    pub fn forget(&self, attempt: &AttemptId) -> Result<()> {
        self.remove_record(attempt)
    }

    /// At worker start: erase the secret files of every attempt `should_erase`
    /// says is terminal (or unknown). Attempts that are still to run keep
    /// theirs. A pass never stops at the first failure. Stale temporary files
    /// of the index directory are removed as well.
    pub fn reconcile(&self, should_erase: impl Fn(&AttemptId) -> bool) -> Result<ReconcileReport> {
        let mut report = ReconcileReport {
            stale_tmp_removed: sweep_stale_tmp(&self.index_dir, STALE_TMP_AGE)?,
            ..ReconcileReport::default()
        };
        let rd = fs::read_dir(&self.index_dir)
            .map_err(|e| SecretError::io(format!("directory {}", self.index_dir.display()), e))?;
        let mut names: Vec<String> = Vec::new();
        for entry in rd {
            let entry = entry.map_err(|e| {
                SecretError::io(format!("directory {}", self.index_dir.display()), e)
            })?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        for name in names {
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(attempt) = AttemptId::parse(stem) else {
                report
                    .failed
                    .push((name.clone(), "not an attempt's record".into()));
                continue;
            };
            if !should_erase(&attempt) {
                report.kept.push(attempt);
                continue;
            }
            match self.erase(&attempt) {
                Ok(_) => report.erased.push(attempt),
                Err(SecretError::WorkspaceRequired { .. }) => {
                    report.workspace_required.push(attempt)
                }
                Err(e) => report.failed.push((name.clone(), e.to_string())),
            }
        }
        Ok(report)
    }
}

/// The directories above the last component of `rel`, created (0700) when
/// missing, each opened without following a link; then the file.
fn place_one(root: &Path, rel: &str, content: &[u8], ours_already: bool) -> Result<bool> {
    let (last, parents) = split_rel(rel)?;
    let not_dir = || {
        placement(format!(
            "{rel}: a parent directory is a link or not a directory"
        ))
    };
    let dir_io = |e: Errno| SecretError::io(format!("directory for {rel}"), errno_io(e));
    let mut dir = match rustix::fs::openat(CWD, root, DIR_FLAGS, Mode::empty()) {
        Ok(fd) => fd,
        Err(e) if is_not_dir(e) => return Err(placement("the workspace is not a plain directory")),
        Err(e) => return Err(dir_io(e)),
    };
    for c in parents {
        dir = match rustix::fs::openat(dir.as_fd(), c, DIR_FLAGS, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => {
                match rustix::fs::mkdirat(dir.as_fd(), c, Mode::RWXU) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(e) => return Err(dir_io(e)),
                }
                // whatever stands there now, if it is not a directory it is refused
                match rustix::fs::openat(dir.as_fd(), c, DIR_FLAGS, Mode::empty()) {
                    Ok(fd) => fd,
                    Err(e) if is_not_dir(e) => return Err(not_dir()),
                    Err(e) => return Err(dir_io(e)),
                }
            }
            Err(e) if is_not_dir(e) => return Err(not_dir()),
            Err(e) => return Err(dir_io(e)),
        };
    }
    let create = OFlags::WRONLY
        .union(OFlags::CREATE)
        .union(OFlags::EXCL)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    match rustix::fs::openat(dir.as_fd(), last, create, Mode::RUSR | Mode::WUSR) {
        Ok(fd) => {
            let mut f = File::from(fd);
            let r = f
                .write_all(content)
                .and_then(|()| {
                    f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
                })
                .and_then(|()| f.sync_all());
            drop(f);
            if let Err(e) = r {
                let _ = rustix::fs::unlinkat(dir.as_fd(), last, AtFlags::empty());
                return Err(SecretError::io(format!("secret file {rel}"), e));
            }
            Ok(true)
        }
        Err(Errno::EXIST) => {
            // a re-sent request after a restart: the same file, by us, is fine
            let read = OFlags::RDONLY
                .union(OFlags::NOFOLLOW)
                .union(OFlags::NONBLOCK)
                .union(OFlags::CLOEXEC);
            let fd = rustix::fs::openat(dir.as_fd(), last, read, Mode::empty()).map_err(|e| {
                if is_not_dir(e) {
                    placement(format!(
                        "{rel} already exists in the workspace; it is not overwritten"
                    ))
                } else {
                    SecretError::io(format!("secret file {rel}"), errno_io(e))
                }
            })?;
            let mut f = File::from(fd);
            let meta = f
                .metadata()
                .map_err(|e| SecretError::io(format!("secret file {rel}"), e))?;
            let mut same = false;
            if ours_already && meta.file_type().is_file() && meta.mode() & 0o777 == 0o600 {
                let mut have = Vec::new();
                same = f.read_to_end(&mut have).is_ok() && have == content;
            }
            if same {
                Ok(false)
            } else {
                Err(placement(format!(
                    "{rel} already exists in the workspace; it is not overwritten"
                )))
            }
        }
        Err(e) if is_not_dir(e) => Err(placement(format!(
            "{rel} cannot be created in the workspace"
        ))),
        Err(e) => Err(SecretError::io(format!("secret file {rel}"), errno_io(e))),
    }
}

/// Remove `rel` under `root` without following a link on the way.
fn erase_one(root: &Path, rel: &str) -> Result<Step> {
    let (last, parents) = split_rel(rel)?;
    let dir = match walk_existing(root, &parents, rel)? {
        Walk::Dir(fd) => fd,
        Walk::Gone => return Ok(Step::Gone),
        Walk::Unreachable => return Ok(Step::Unreachable),
    };
    let io = |e: Errno| SecretError::io(format!("secret file {rel}"), errno_io(e));
    match rustix::fs::statat(dir.as_fd(), last, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT) => Ok(Step::Gone),
        Err(e) => Err(io(e)),
        Ok(st)
            if FileType::from_raw_mode(st.st_mode as rustix::fs::RawMode)
                == FileType::Directory =>
        {
            Ok(Step::Unreachable)
        }
        Ok(_) => match rustix::fs::unlinkat(dir.as_fd(), last, AtFlags::empty()) {
            Ok(()) => Ok(Step::Removed),
            Err(Errno::NOENT) => Ok(Step::Gone),
            Err(e) => Err(io(e)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Bytes;
    use std::os::unix::fs::{symlink, PermissionsExt};

    /// `place` with the manifest built from the files themselves.
    trait Put {
        fn put(
            &self,
            key: &ClusterKey,
            a: &AttemptId,
            ws: &Path,
            files: &[SecretFile],
        ) -> Result<u32>;
    }
    fn entries(files: &[SecretFile]) -> Vec<SecretEntry> {
        files
            .iter()
            .map(|f| SecretEntry {
                path: RelPath(f.path.clone().into_bytes()),
                hmac: f.hmac_digest.to_string(),
            })
            .collect()
    }
    impl Put for WorkerSecrets {
        fn put(
            &self,
            key: &ClusterKey,
            a: &AttemptId,
            ws: &Path,
            files: &[SecretFile],
        ) -> Result<u32> {
            self.place(key, a, ws, files, &entries(files))
        }
    }

    struct Env {
        _tmp: tempfile::TempDir,
        state: PathBuf,
        ws: PathBuf,
        outside: PathBuf,
        key: ClusterKey,
        ws_secrets: WorkerSecrets,
    }

    fn env() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let ws = tmp.path().join("ws");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&ws).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let ws_secrets = WorkerSecrets::open(&state).unwrap();
        Env {
            _tmp: tmp,
            state,
            ws,
            outside,
            key: ClusterKey::from_bytes(vec![7; 32]).unwrap(),
            ws_secrets,
        }
    }

    fn file(key: &ClusterKey, path: &str, content: &str) -> SecretFile {
        let d = secret_digest(
            key.as_bytes(),
            &RelPath(path.as_bytes().to_vec()),
            content.as_bytes(),
        );
        SecretFile {
            path: path.to_string(),
            hmac_digest: to_wire_digest(&d).unwrap(),
            content: Bytes(content.as_bytes().to_vec()),
        }
    }

    fn mode(p: &Path) -> u32 {
        fs::metadata(p).unwrap().mode() & 0o777
    }

    #[test]
    fn files_are_placed_as_0600_with_their_content_and_parents() {
        let e = env();
        let a = AttemptId::generate();
        let files = [
            file(&e.key, ".env", "TOKEN=abc-123456\n"),
            file(&e.key, "pkg/a/.env.test", "X=yyyy-9999\n"),
        ];
        assert_eq!(e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap(), 2);
        assert_eq!(
            fs::read_to_string(e.ws.join(".env")).unwrap(),
            "TOKEN=abc-123456\n"
        );
        assert_eq!(mode(&e.ws.join(".env")), 0o600);
        assert_eq!(mode(&e.ws.join("pkg/a/.env.test")), 0o600);
        // the directories placement made are private too
        assert_eq!(mode(&e.ws.join("pkg")), 0o700);
        assert_eq!(mode(&e.ws.join("pkg/a")), 0o700);
        // the index lists paths, holds no content, and is private
        let idx = e.state.join("secret-index").join(format!("{a}.json"));
        assert_eq!(mode(&idx), 0o600);
        let text = fs::read_to_string(&idx).unwrap();
        assert!(text.contains(".env.test"));
        assert!(!text.contains("abc-123456") && !text.contains("yyyy-9999"));
        assert_eq!(mode(&e.state.join("secret-index")), 0o700);
    }

    #[test]
    fn a_wrong_digest_writes_nothing_at_all() {
        let e = env();
        let a = AttemptId::generate();
        let mut bad = file(&e.key, "b.env", "B=bbbb-2222");
        bad.content = Bytes(b"B=changed-22".to_vec());
        let files = [file(&e.key, "a.env", "A=aaaa-1111"), bad];
        let err = e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap_err();
        assert!(matches!(err, SecretError::Tampered { ref path } if path == "b.env"));
        assert!(!err.to_string().contains("changed"));
        assert_eq!(fs::read_dir(&e.ws).unwrap().count(), 0);
        assert_eq!(
            fs::read_dir(e.state.join("secret-index")).unwrap().count(),
            0
        );
        // another cluster's key is the same refusal
        let other = ClusterKey::from_bytes(vec![9; 32]).unwrap();
        let good = [file(&e.key, "a.env", "A=aaaa-1111")];
        assert!(matches!(
            e.ws_secrets.put(&other, &a, &e.ws, &good),
            Err(SecretError::Tampered { .. })
        ));
        // a path is part of the digest: the same content under another path
        let mut moved = file(&e.key, "a.env", "A=aaaa-1111");
        moved.path = "c.env".into();
        assert!(matches!(
            e.ws_secrets.put(&e.key, &a, &e.ws, &[moved]),
            Err(SecretError::Tampered { .. })
        ));
    }

    #[test]
    fn paths_that_leave_the_workspace_or_repeat_are_refused_before_writing() {
        let e = env();
        let a = AttemptId::generate();
        for bad in [
            "../x.env",
            "/abs.env",
            "a/../../x",
            ".git/hooks/pre-commit",
            "",
        ] {
            let f = file(&e.key, bad, "V=vvvv-0000");
            assert!(
                matches!(
                    e.ws_secrets.put(&e.key, &a, &e.ws, &[f]),
                    Err(SecretError::BadPath(_))
                ),
                "{bad}"
            );
        }
        let dup = [
            file(&e.key, "d.env", "V=1111-1111"),
            file(&e.key, "d.env", "V=1111-1111"),
        ];
        assert!(matches!(
            e.ws_secrets.put(&e.key, &a, &e.ws, &dup),
            Err(SecretError::BadPath(_))
        ));
        let big = file(
            &e.key,
            "big.env",
            &"x".repeat(super::super::plan::MAX_FILE_BYTES + 1),
        );
        assert!(matches!(
            e.ws_secrets.put(&e.key, &a, &e.ws, &[big]),
            Err(SecretError::Limit(_))
        ));
        assert_eq!(fs::read_dir(&e.ws).unwrap().count(), 0);
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
    }

    #[test]
    fn a_symlinked_parent_is_not_followed() {
        let e = env();
        let a = AttemptId::generate();
        symlink(&e.outside, e.ws.join("linked")).unwrap();
        let err = e
            .ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "linked/.env", "S=ssss-0000")],
            )
            .unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)), "{err}");
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
        // a parent that is a file
        fs::write(e.ws.join("plain"), b"x").unwrap();
        assert!(e
            .ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "plain/.env", "S=ssss-0000")]
            )
            .is_err());
        // a symlinked directory deeper in the path
        fs::create_dir(e.ws.join("d")).unwrap();
        symlink(&e.outside, e.ws.join("d/deep")).unwrap();
        assert!(e
            .ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "d/deep/.env", "S=ssss-0000")]
            )
            .is_err());
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
    }

    #[test]
    fn a_symlink_standing_where_the_file_goes_is_not_written_through() {
        let e = env();
        let a = AttemptId::generate();
        let victim = e.outside.join("victim");
        fs::write(&victim, b"keep").unwrap();
        symlink(&victim, e.ws.join(".env")).unwrap();
        let err = e
            .ws_secrets
            .put(&e.key, &a, &e.ws, &[file(&e.key, ".env", "S=ssss-0000")])
            .unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)));
        assert_eq!(fs::read(&victim).unwrap(), b"keep");
        // a dangling link too
        symlink(e.outside.join("nothing"), e.ws.join(".env2")).unwrap();
        assert!(e
            .ws_secrets
            .put(&e.key, &a, &e.ws, &[file(&e.key, ".env2", "S=ssss-0000")])
            .is_err());
        assert!(!e.outside.join("nothing").exists());
    }

    #[test]
    fn a_workspace_that_is_a_link_or_missing_is_refused() {
        let e = env();
        let a = AttemptId::generate();
        let link = e.state.join("wslink");
        symlink(&e.ws, &link).unwrap();
        let f = [file(&e.key, ".env", "S=ssss-0000")];
        assert!(matches!(
            e.ws_secrets.put(&e.key, &a, &link, &f),
            Err(SecretError::Placement(_))
        ));
        assert!(matches!(
            e.ws_secrets.put(&e.key, &a, &e.state.join("none"), &f),
            Err(SecretError::Io { .. })
        ));
        assert!(!e.ws.join(".env").exists());
    }

    #[test]
    fn the_values_to_mask_are_read_from_the_files_in_place() {
        let e = env();
        let a = AttemptId::generate();
        // Nothing placed: nothing to mask.
        assert_eq!(e.ws_secrets.mask_values(&a).unwrap(), Values::default());
        let files = [
            file(&e.key, ".env", "TOKEN=aaaa-1111-bbbb\nPORT=80\n"),
            file(
                &e.key,
                "conf/db.env",
                "PASSWORD=cccc-2222-dddd\nTOKEN=aaaa-1111-bbbb\n",
            ),
        ];
        e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap();
        let v = e.ws_secrets.mask_values(&a).unwrap();
        let mut values = v.values.clone();
        values.sort();
        // Every file's values, each once; the short one is named, not masked.
        assert_eq!(
            values,
            [b"aaaa-1111-bbbb".to_vec(), b"cccc-2222-dddd".to_vec()]
        );
        assert_eq!(v.skipped_keys, ["PORT"]);
        // With the values, which files they were read from: every placed
        // file, as the file system knows it.
        let placed = e.ws_secrets.placed(&a).unwrap();
        assert_eq!(placed.values, v);
        let id = |p: &str| FileIdentity::of(&fs::symlink_metadata(e.ws.join(p)).unwrap());
        let mut ids = placed.files.clone();
        ids.sort();
        let mut want = vec![id(".env"), id("conf/db.env")];
        want.sort();
        assert_eq!(ids, want);
        assert_ne!(want[0], want[1]);
        assert_eq!(
            e.ws_secrets.placed(&AttemptId::generate()).unwrap(),
            Placed::default()
        );
        // A second instance (an agent that restarted) reads the same.
        let again = WorkerSecrets::open(&e.state).unwrap();
        assert_eq!(again.mask_values(&a).unwrap(), v);
        // Another attempt has none of them.
        assert_eq!(
            e.ws_secrets.mask_values(&AttemptId::generate()).unwrap(),
            Values::default()
        );
    }

    #[test]
    fn a_placed_file_that_is_not_as_placed_gives_no_values_but_an_error() {
        let placed = |e: &Env| {
            let a = AttemptId::generate();
            let files = [file(&e.key, "conf/.env", "TOKEN=aaaa-1111-bbbb")];
            e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap();
            a
        };
        let target = |e: &Env| e.ws.join("conf/.env");
        // Gone.
        let e = env();
        let a = placed(&e);
        fs::remove_file(target(&e)).unwrap();
        assert!(matches!(
            e.ws_secrets.mask_values(&a),
            Err(SecretError::Placement(_))
        ));
        // Replaced by a link to another file.
        let e = env();
        let a = placed(&e);
        fs::remove_file(target(&e)).unwrap();
        fs::write(e.state.join("elsewhere"), b"TOKEN=zzzz-9999-yyyy").unwrap();
        symlink(e.state.join("elsewhere"), target(&e)).unwrap();
        assert!(e.ws_secrets.mask_values(&a).is_err());
        // Its directory replaced by a link.
        let e = env();
        let a = placed(&e);
        fs::rename(e.ws.join("conf"), e.state.join("moved")).unwrap();
        symlink(e.state.join("moved"), e.ws.join("conf")).unwrap();
        assert!(matches!(
            e.ws_secrets.mask_values(&a),
            Err(SecretError::Placement(_))
        ));
        // Opened to others.
        let e = env();
        let a = placed(&e);
        fs::set_permissions(target(&e), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            e.ws_secrets.mask_values(&a),
            Err(SecretError::Placement(_))
        ));
        // Grown beyond what can be placed.
        let e = env();
        let a = placed(&e);
        fs::write(target(&e), vec![b'x'; MAX_FILE_BYTES + 1]).unwrap();
        assert!(matches!(
            e.ws_secrets.mask_values(&a),
            Err(SecretError::Placement(_))
        ));
        // Exactly the largest is read.
        let e = env();
        let a = placed(&e);
        fs::write(target(&e), vec![b'x'; MAX_FILE_BYTES]).unwrap();
        assert!(e.ws_secrets.mask_values(&a).is_ok());
    }

    #[test]
    fn an_existing_file_is_never_overwritten_and_the_call_leaves_nothing() {
        let e = env();
        let a = AttemptId::generate();
        fs::write(e.ws.join("b.env"), b"mine").unwrap();
        let files = [
            file(&e.key, "a.env", "A=aaaa-1111"),
            file(&e.key, "b.env", "B=bbbb-2222"),
        ];
        let err = e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)));
        assert_eq!(fs::read(e.ws.join("b.env")).unwrap(), b"mine");
        assert!(!e.ws.join("a.env").exists(), "the first file was undone");
    }

    #[test]
    fn a_resent_request_is_idempotent_but_changed_content_is_not_accepted() {
        let e = env();
        let a = AttemptId::generate();
        let files = [file(&e.key, ".env", "A=aaaa-1111")];
        assert_eq!(e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap(), 1);
        assert_eq!(e.ws_secrets.put(&e.key, &a, &e.ws, &files).unwrap(), 1);
        // the file was changed by something else: not silently accepted
        fs::write(e.ws.join(".env"), b"tampered").unwrap();
        assert!(e.ws_secrets.put(&e.key, &a, &e.ws, &files).is_err());
        // widened permissions: not "ours" any more
        fs::write(e.ws.join(".env"), b"A=aaaa-1111").unwrap();
        fs::set_permissions(e.ws.join(".env"), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(e.ws_secrets.put(&e.key, &a, &e.ws, &files).is_err());
        // another attempt does not own it
        let b = AttemptId::generate();
        fs::set_permissions(e.ws.join(".env"), fs::Permissions::from_mode(0o600)).unwrap();
        assert!(e.ws_secrets.put(&e.key, &b, &e.ws, &files).is_err());
    }

    #[test]
    fn batches_for_one_attempt_add_up_and_stay_in_one_workspace() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .put(&e.key, &a, &e.ws, &[file(&e.key, "1.env", "A=aaaa-1111")])
            .unwrap();
        e.ws_secrets
            .put(&e.key, &a, &e.ws, &[file(&e.key, "2.env", "B=bbbb-2222")])
            .unwrap();
        let other = e.state.join("ws2");
        fs::create_dir(&other).unwrap();
        assert!(matches!(
            e.ws_secrets
                .put(&e.key, &a, &other, &[file(&e.key, "3.env", "C=cccc-3333")]),
            Err(SecretError::Placement(_))
        ));
        let r = e.ws_secrets.erase(&a).unwrap();
        assert_eq!(r.removed, ["1.env", "2.env"]);
        assert!(!e.ws.join("1.env").exists() && !e.ws.join("2.env").exists());
    }

    #[test]
    fn erase_removes_files_and_the_index_and_can_be_repeated() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[
                    file(&e.key, "x/.env", "A=aaaa-1111"),
                    file(&e.key, ".env", "B=bbbb-2222"),
                ],
            )
            .unwrap();
        let r = e.ws_secrets.erase(&a).unwrap();
        assert_eq!(r.removed.len(), 2);
        assert!(r.gone.is_empty());
        assert!(!e.ws.join(".env").exists() && !e.ws.join("x/.env").exists());
        assert!(!e
            .state
            .join("secret-index")
            .join(format!("{a}.json"))
            .exists());
        assert_eq!(e.ws_secrets.erase(&a).unwrap(), EraseReport::default());
    }

    #[test]
    fn erase_after_the_workspace_was_tampered_with_follows_no_link() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[
                    file(&e.key, "d/.env", "A=aaaa-1111"),
                    file(&e.key, ".env", "B=bbbb-2222"),
                    file(&e.key, "gone.env", "C=cccc-3333"),
                ],
            )
            .unwrap();
        // the parent directory is swapped for a link to a directory outside
        let victim = e.outside.join(".env");
        fs::write(&victim, b"precious").unwrap();
        fs::remove_dir_all(e.ws.join("d")).unwrap();
        symlink(&e.outside, e.ws.join("d")).unwrap();
        // the file itself is swapped for a link to a file outside
        fs::remove_file(e.ws.join(".env")).unwrap();
        let precious2 = e.outside.join("p2");
        fs::write(&precious2, b"precious2").unwrap();
        symlink(&precious2, e.ws.join(".env")).unwrap();
        fs::remove_file(e.ws.join("gone.env")).unwrap();
        // d/.env cannot be reached: nothing is claimed, the record stays, the
        // workspace has to go; what could be removed was removed
        let err = e.ws_secrets.erase(&a).unwrap_err();
        assert!(
            matches!(err, SecretError::WorkspaceRequired { .. }),
            "{err}"
        );
        assert!(e
            .state
            .join("secret-index")
            .join(format!("{a}.json"))
            .exists());
        assert_eq!(fs::read(&victim).unwrap(), b"precious");
        assert_eq!(fs::read(&precious2).unwrap(), b"precious2");
        assert!(fs::symlink_metadata(e.ws.join(".env")).is_err());
        // after the workspace is deleted as a whole, the record is dropped
        e.ws_secrets.forget(&a).unwrap();
        assert!(!e
            .state
            .join("secret-index")
            .join(format!("{a}.json"))
            .exists());
        e.ws_secrets.forget(&a).unwrap();
        assert_eq!(e.ws_secrets.erase(&a).unwrap(), EraseReport::default());
    }

    #[test]
    fn erase_copes_with_a_missing_or_replaced_workspace_and_a_directory_in_the_way() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .put(&e.key, &a, &e.ws, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        fs::remove_dir_all(&e.ws).unwrap();
        assert_eq!(e.ws_secrets.erase(&a).unwrap().gone, [".env"]);
        // the workspace path became a link
        let b = AttemptId::generate();
        fs::create_dir(&e.ws).unwrap();
        e.ws_secrets
            .put(&e.key, &b, &e.ws, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        let moved = e.state.join("moved");
        fs::rename(&e.ws, &moved).unwrap();
        symlink(&moved, &e.ws).unwrap();
        assert!(matches!(
            e.ws_secrets.erase(&b),
            Err(SecretError::WorkspaceRequired { .. })
        ));
        assert!(moved.join(".env").exists(), "the link was not followed");
        // a directory where the file was
        let c = AttemptId::generate();
        let ws3 = e.state.join("ws3");
        fs::create_dir(&ws3).unwrap();
        e.ws_secrets
            .put(&e.key, &c, &ws3, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        fs::remove_file(ws3.join(".env")).unwrap();
        fs::create_dir(ws3.join(".env")).unwrap();
        assert!(matches!(
            e.ws_secrets.erase(&c),
            Err(SecretError::WorkspaceRequired { .. })
        ));
    }

    #[test]
    fn reconcile_erases_terminal_attempts_and_keeps_the_others() {
        let e = env();
        let (t, r, u) = (
            AttemptId::generate(),
            AttemptId::generate(),
            AttemptId::generate(),
        );
        for (i, a) in [&t, &r, &u].into_iter().enumerate() {
            let ws = e.state.join(format!("ws{i}"));
            fs::create_dir(&ws).unwrap();
            e.ws_secrets
                .put(&e.key, a, &ws, &[file(&e.key, ".env", "A=aaaa-1111")])
                .unwrap();
        }
        // a file that is not an attempt's record, and one that is not a record
        fs::write(e.state.join("secret-index/junk.json"), b"{}").unwrap();
        fs::write(e.state.join("secret-index/notes.txt"), b"x").unwrap();
        let running = r.clone();
        let rep = e.ws_secrets.reconcile(|a| *a != running).unwrap();
        let mut erased = rep.erased.clone();
        erased.sort();
        let mut want = vec![t.clone(), u.clone()];
        want.sort();
        assert_eq!(erased, want);
        assert_eq!(rep.kept, vec![r.clone()]);
        assert_eq!(rep.failed.len(), 1);
        assert!(rep.failed[0].0 == "junk.json");
        assert!(!e.state.join("ws0/.env").exists() && !e.state.join("ws2/.env").exists());
        assert!(e.state.join("ws1/.env").exists());
        // a second pass has nothing left to erase
        let again = e.ws_secrets.reconcile(|_| true).unwrap();
        assert_eq!(again.erased, vec![r]);
    }

    #[test]
    fn a_damaged_index_is_reported_and_kept_for_a_later_pass() {
        let e = env();
        let a = AttemptId::generate();
        let idx = e.state.join("secret-index").join(format!("{a}.json"));
        fs::write(&idx, b"not json").unwrap();
        fs::set_permissions(&idx, fs::Permissions::from_mode(0o600)).unwrap();
        // which workspace it named is unknown: the whole workspace has to go
        assert!(matches!(
            e.ws_secrets.erase(&a),
            Err(SecretError::WorkspaceRequired { .. })
        ));
        let rep = e.ws_secrets.reconcile(|_| true).unwrap();
        assert_eq!(rep.workspace_required, vec![a.clone()]);
        assert!(rep.failed.is_empty() && rep.erased.is_empty());
        assert!(idx.exists());
        // an index others can read is the same: it cannot be trusted
        fs::set_permissions(&idx, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            e.ws_secrets.erase(&a),
            Err(SecretError::WorkspaceRequired { .. })
        ));
        e.ws_secrets.forget(&a).unwrap();
        assert!(!idx.exists());
    }

    #[test]
    fn the_index_directory_must_be_private() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("secret-index");
        fs::create_dir(&d).unwrap();
        fs::set_permissions(&d, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            WorkerSecrets::open(tmp.path()),
            Err(SecretError::Insecure { .. })
        ));
    }

    #[test]
    fn too_many_files_over_several_requests_are_refused() {
        let e = env();
        let a = AttemptId::generate();
        let first: Vec<SecretFile> = (0..MAX_FILES)
            .map(|i| file(&e.key, &format!("f{i}"), "V=vvvv-0000"))
            .collect();
        e.ws_secrets.put(&e.key, &a, &e.ws, &first).unwrap();
        let err = e
            .ws_secrets
            .put(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "one-more", "V=vvvv-0000")],
            )
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::Limit(Limit::TooManyFiles { .. })
        ));
        assert!(!e.ws.join("one-more").exists());
    }
}
