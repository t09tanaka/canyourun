//! Recording what an attempt produced: its artifacts, its result, and the
//! files GC may delete (spec chapter 13).
//!
//! Files live under the spool root, per attempt (`<attempt>/artifacts/...`,
//! `<attempt>/logs`), so a new attempt never overwrites an old one's result.
//! The result itself is the worker database's `result_json` (canonical, write
//! once). Every file is referenced from the database (`blob_ref`) *before* the
//! attempt turns terminal, so GC can see it; GC drops the reference and queues
//! the locator in `pending_delete`, and [`drain_pending_deletes`] is the only
//! thing that removes a file, followed by `complete_delete`.
//!
//! The spool root is the supervisor's: the commands it runs cannot write
//! there (their workspace is elsewhere, and artifacts are copied from it by
//! the supervisor once the command has exited). Inside the root every path is
//! still checked against symlinks, and a source path is only taken from below
//! the workspace root and never through a symlink, so a command cannot make the
//! supervisor copy a file from outside its workspace.

use super::spool::{AttemptSpool, SpoolError, StreamStatus};
use super::spool_plan::{
    artifact_locator, check_artifact_names, is_tmp_name, logs_locator, relative_path,
    resolve_locator, result_has_artifacts, result_json, sha256_digest, source_under,
    ArtifactSetError, StreamSummary,
};
use crate::ids::{AttemptId, Digest, PlanDigest, SourceId};
use crate::state::Termination;
use crate::storage::{BlobKind, LogPosition, NewBlobRef, StorageError, WorkerStore};
use crate::wire::ArtifactRecord;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum OutcomeError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Closing the log spool failed (an earlier write or the final sync
    /// failed, e.g. the disk is full): the stored output is incomplete and
    /// nothing was recorded for the attempt. What that means for the attempt's
    /// result (typically a failure) is the supervisor's decision (S9a); this
    /// module records no result in that case.
    #[error("the log spool could not be completed: {0}")]
    SpoolFailed(SpoolError),
    #[error(transparent)]
    Spool(#[from] SpoolError),
    /// A directory or file in the managed area is a symlink or is not what it
    /// should be; nothing outside the area is read or written through it.
    #[error("{} in the managed area is a symlink or not a plain directory/file", .0.display())]
    Symlink(PathBuf),
    #[error("artifact name {0:?} is not a plain relative path")]
    BadName(String),
    /// The source is not below the workspace root, or a directory on the way
    /// is a symlink.
    #[error("artifact source {} is outside the workspace or goes through a symlink", .0.display())]
    BadSource(PathBuf),
    #[error("artifact source {} is not a regular file", .0.display())]
    NotRegular(PathBuf),
    #[error("artifact names clash: {0:?}")]
    NameClash(String),
    /// A recorded artifact is missing or is not what its record says.
    #[error("artifact {0} is missing or differs from its record")]
    ArtifactChanged(String),
    #[error("artifact {name} is larger than {max} bytes")]
    TooLarge { name: String, max: u64 },
    #[error("artifact {0} already exists with different content")]
    Conflict(String),
}

pub type Result<T, E = OutcomeError> = std::result::Result<T, E>;

/// An artifact the command produced, to be copied into the attempt's store.
#[derive(Debug, Clone)]
pub struct ArtifactInput<'a> {
    /// Name and relative path inside the attempt's artifacts.
    pub name: &'a str,
    pub source: &'a Path,
    pub source_id: SourceId,
    pub plan_digest: PlanDigest,
    pub profile_fingerprint: Digest,
}

fn artifacts_root(root: &Path, attempt: &AttemptId) -> PathBuf {
    root.join(attempt.as_str()).join("artifacts")
}

/// The source file below `workspace`: every directory from the workspace root
/// down is looked at with `symlink_metadata` and must be a real directory (a
/// symlink in the middle would lead out of the workspace), and the file itself
/// must be a regular file, not a link. `workspace` itself is trusted (it may be
/// reached through a symlink, like a temp directory). Returns the path and its
/// metadata.
fn checked_source(workspace: &Path, source: &Path) -> Result<(PathBuf, fs::Metadata)> {
    let comps = source_under(workspace, source)
        .ok_or_else(|| OutcomeError::BadSource(source.to_path_buf()))?;
    let (last, dirs) = comps.split_last().expect("source_under is never empty");
    let mut cur = workspace.to_path_buf();
    for c in dirs {
        cur.push(c);
        if !fs::symlink_metadata(&cur)?.is_dir() {
            return Err(OutcomeError::BadSource(source.to_path_buf()));
        }
    }
    cur.push(last);
    let meta = fs::symlink_metadata(&cur)?;
    if !meta.file_type().is_file() {
        return Err(OutcomeError::NotRegular(source.to_path_buf()));
    }
    Ok((cur, meta))
}

/// Copy a produced file into `<attempt>/artifacts/<name>`: hashed while
/// copied, through a uniquely named temporary file, fsynced, then moved into
/// place without replacing anything. The source (`input.source`, absolute below
/// `workspace` or relative to it) is checked from the workspace root down,
/// directory by directory, and may not go through a symlink; a file over
/// `max_bytes`, a non-regular source (a symlink, a device) or a name that is not
/// a plain relative path is refused before anything is kept. The same name with
/// the same content is a replay (the existing file is kept); with other content
/// it is a conflict. Call it after the command has exited: a directory swapped
/// for a symlink between the check and the open is not defended against.
pub fn store_artifact(
    root: &Path,
    attempt: &AttemptId,
    workspace: &Path,
    input: &ArtifactInput<'_>,
    max_bytes: u64,
) -> Result<ArtifactRecord> {
    let rel =
        relative_path(input.name).ok_or_else(|| OutcomeError::BadName(input.name.to_string()))?;
    let (source, meta) = checked_source(workspace, input.source)?;
    let mut src = File::open(&source)?;
    let opened = src.metadata()?;
    if !opened.is_file() || (opened.dev(), opened.ino()) != (meta.dev(), meta.ino()) {
        return Err(OutcomeError::NotRegular(input.source.to_path_buf()));
    }
    let dest = artifacts_root(root, attempt).join(&rel);
    let parent = dest.parent().expect("a joined path has a parent");
    let comps: Vec<&std::ffi::OsStr> = Path::new(attempt.as_str())
        .iter()
        .chain(Path::new("artifacts").iter())
        .chain(rel.parent().into_iter().flat_map(Path::iter))
        .collect();
    ensure_real_dirs(root, &comps)?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".tmp")
        .tempfile_in(parent)?;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        if size > max_bytes {
            return Err(OutcomeError::TooLarge {
                name: input.name.to_string(),
                max: max_bytes,
            });
        }
        hasher.update(&buf[..n]);
        tmp.write_all(&buf[..n])?;
    }
    tmp.as_file().sync_all()?;
    let digest = sha256_digest(&hasher.finalize()).expect("sha256 hex is a digest");
    match tmp.persist_noclobber(&dest) {
        Ok(_) => {}
        Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
            if file_digest(&dest)?.0 != digest {
                return Err(OutcomeError::Conflict(input.name.to_string()));
            }
        }
        Err(e) => return Err(e.error.into()),
    }
    File::open(parent)?.sync_all()?;
    Ok(ArtifactRecord {
        name: input.name.to_string(),
        path: input.name.to_string(),
        size,
        digest,
        source_id: input.source_id.clone(),
        plan_digest: input.plan_digest.clone(),
        attempt_id: attempt.clone(),
        profile_fingerprint: input.profile_fingerprint.clone(),
    })
}

/// Walk `comps` below `base`, creating missing directories (0700) and refusing
/// any component that is a symlink or not a directory: no path of the managed
/// area leads outside it. `base` itself is the configured root and is trusted.
/// (A swap between the check and the use needs a writer inside the area, which
/// only the supervisor's own user has.)
fn ensure_real_dirs(base: &Path, comps: &[&std::ffi::OsStr]) -> Result<()> {
    let mut cur = base.to_path_buf();
    for c in comps {
        cur.push(c);
        loop {
            match fs::symlink_metadata(&cur) {
                Ok(m) if m.is_dir() => break,
                Ok(_) => return Err(OutcomeError::Symlink(cur)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    match fs::DirBuilder::new().mode(0o700).create(&cur) {
                        Ok(()) => {}
                        // Raced with another creator: look again.
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

/// Digest and size of a regular file (not a link) at `path`.
fn file_digest(path: &Path) -> Result<(Digest, u64)> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_file() {
        return Err(OutcomeError::Symlink(path.to_path_buf()));
    }
    let mut f = OpenOptions::new().read(true).open(path)?;
    let opened = f.metadata()?;
    if (opened.dev(), opened.ino()) != (meta.dev(), meta.ino()) {
        return Err(OutcomeError::Symlink(path.to_path_buf()));
    }
    let mut h = Sha256::new();
    let mut size = 0u64;
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        size += n as u64;
    }
    Ok((
        sha256_digest(&h.finalize()).expect("sha256 hex is a digest"),
        size,
    ))
}

/// The stored result and whether this call stored it.
#[derive(Debug, Clone, PartialEq)]
pub struct Recorded {
    pub result: Value,
    pub fresh: bool,
}

fn summary(s: StreamStatus) -> StreamSummary {
    StreamSummary {
        bytes: s.end,
        dropped_bytes: s.dropped,
        last_seq: s.seq,
    }
}

/// The reference with this id is already there (a replay of the same call):
/// only a primary-key clash counts, not any other constraint.
pub(super) fn is_duplicate(e: &StorageError) -> bool {
    matches!(e, StorageError::Sqlite(r)
        if matches!(r, rusqlite::Error::SqliteFailure(f, _)
            if f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY))
}

fn add_ref(
    store: &WorkerStore,
    attempt: &AttemptId,
    ref_id: String,
    kind: BlobKind,
    locator: String,
    at_ms: u64,
) -> Result<()> {
    let r = NewBlobRef {
        ref_id,
        attempt_id: attempt.to_string(),
        kind,
        locator,
    };
    match store.add_blob_ref(&r, at_ms) {
        Ok(()) => Ok(()),
        Err(e) if is_duplicate(&e) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Remove the leftovers of a crashed [`store_artifact`] (`.tmp*` files, which
/// no artifact name can have) from `<attempt>/artifacts`, below real
/// directories only. Call it when no `store_artifact` of this attempt runs.
pub fn sweep_tmp(root: &Path, attempt: &AttemptId) -> Result<u32> {
    fn walk(dir: &Path, depth: usize, n: &mut u32) -> io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let meta = fs::symlink_metadata(&path)?;
            let name = entry.file_name();
            if meta.is_dir() {
                if depth < 32 {
                    walk(&path, depth + 1, n)?;
                }
            } else if is_tmp_name(&name.to_string_lossy()) && meta.is_file() {
                fs::remove_file(&path)?;
                *n += 1;
            }
        }
        Ok(())
    }
    let dir = artifacts_root(root, attempt);
    match fs::symlink_metadata(&dir) {
        Ok(m) if m.is_dir() => {}
        // Nothing there, or not a directory (a symlink is never followed).
        Ok(_) => return Ok(0),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    }
    let mut n = 0;
    walk(&dir, 0, &mut n)?;
    Ok(n)
}

/// The artifact as the record says: present below real directories, with the
/// recorded size and digest.
fn verify_artifact(root: &Path, attempt: &AttemptId, a: &ArtifactRecord) -> Result<()> {
    let rel = relative_path(&a.path).ok_or_else(|| OutcomeError::BadName(a.path.clone()))?;
    let changed = || OutcomeError::ArtifactChanged(a.path.clone());
    let mut cur = artifacts_root(root, attempt);
    let comps: Vec<_> = rel.iter().collect();
    let (_, dirs) = comps.split_last().expect("a relative path has a component");
    for c in dirs {
        cur.push(c);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.is_dir() => {}
            Ok(_) => return Err(OutcomeError::Symlink(cur)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(changed()),
            Err(e) => return Err(e.into()),
        }
    }
    cur.push(comps[comps.len() - 1]);
    let (digest, size) = match file_digest(&cur) {
        Ok(x) => x,
        Err(OutcomeError::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Err(changed()),
        Err(e) => return Err(e),
    };
    if digest != a.digest || size != a.size {
        return Err(changed());
    }
    Ok(())
}

/// Everything about `artifacts` that can be decided before anything is
/// recorded: they belong to this attempt, their names are plain and do not
/// clash (also on a file system that folds case and Unicode form), and the
/// files are there as recorded.
fn check_artifacts(root: &Path, attempt: &AttemptId, artifacts: &[ArtifactRecord]) -> Result<()> {
    for a in artifacts {
        if &a.attempt_id != attempt || a.name != a.path || relative_path(&a.path).is_none() {
            return Err(OutcomeError::BadName(a.path.clone()));
        }
    }
    let names: Vec<&str> = artifacts.iter().map(|a| a.path.as_str()).collect();
    check_artifact_names(&names).map_err(|e| match e {
        ArtifactSetError::Duplicate(n) | ArtifactSetError::Nested(n) => OutcomeError::NameClash(n),
    })?;
    sweep_tmp(root, attempt)?;
    for a in artifacts {
        verify_artifact(root, attempt, a)?;
    }
    Ok(())
}

/// Called on every heartbeat of the supervisor (S9a): make what the streams
/// hold durable and published ([`AttemptSpool::sync`]), then store their
/// positions in the worker database, where they only move forward (so a replay
/// or an older call changes nothing). The remaining log budget of a later
/// attempt of the same task is computed from the positions of the earlier
/// ones, so a position never stored here counts as nothing there.
/// `finish_attempt` stores the final ones. Returns `(stdout, stderr)` as the
/// database holds them.
pub fn sync_log_position(
    store: &WorkerStore,
    attempt: &AttemptId,
    spool: &AttemptSpool,
) -> Result<(LogPosition, LogPosition)> {
    let (out, err) = spool.sync()?;
    Ok((
        store.update_log_position(attempt, "stdout", out.into())?,
        store.update_log_position(attempt, "stderr", err.into())?,
    ))
}

/// Close the attempt's spool, reference its files, fix its termination and
/// store its result: in that order, so a crash at any point leaves a state
/// that calling this again completes. The termination in the result is the
/// one that won (a cancel may have fixed it first). `artifacts` are checked
/// first (names, clashes, and the files against their records under `root`).
/// A result that is already stored is returned as is (`fresh == false`) after
/// confirming that the spool is closed and repairing the journal; `artifacts`
/// that differ from the stored ones are [`StorageError::Immutable`], returned
/// before anything is referenced, never a second result. Sizes and drop counts
/// of both streams go in the result, and `log_truncated` is also written to the
/// event journal for each stream that dropped bytes, exactly once: the stored
/// result is the truth and the events are made to match it on every call.
///
/// If the spool cannot be completed (an earlier write failed), this returns
/// [`OutcomeError::SpoolFailed`] before recording anything: the attempt's
/// result is then S9a's (the supervisor's) decision, not this module's.
pub fn finish_attempt(
    store: &WorkerStore,
    root: &Path,
    attempt: &AttemptId,
    spool: &AttemptSpool,
    termination: Termination,
    artifacts: &[ArtifactRecord],
    at_ms: u64,
) -> Result<Recorded> {
    if let Some(existing) = store.get_result(attempt)? {
        if !result_has_artifacts(&existing, artifacts) {
            return Err(StorageError::Immutable(format!("artifacts of {attempt}")).into());
        }
        spool.finish().map_err(OutcomeError::SpoolFailed)?;
        ensure_truncation_events(store, attempt, &existing, at_ms)?;
        return Ok(Recorded {
            result: existing,
            fresh: false,
        });
    }
    check_artifacts(root, attempt, artifacts)?;
    let (out, err) = spool.finish().map_err(OutcomeError::SpoolFailed)?;
    store.update_log_position(attempt, "stdout", out.into())?;
    store.update_log_position(attempt, "stderr", err.into())?;
    add_ref(
        store,
        attempt,
        format!("{attempt}:logs"),
        BlobKind::Log,
        logs_locator(attempt),
        at_ms,
    )?;
    for a in artifacts {
        add_ref(
            store,
            attempt,
            format!("{attempt}:artifact:{}", a.path),
            BlobKind::Result,
            artifact_locator(attempt, &a.path),
            at_ms,
        )?;
    }
    let fixed = store.finalize(attempt, termination, at_ms)?.termination();
    let result = result_json(&fixed, artifacts, summary(out), summary(err));
    let fresh = store.record_result(attempt, &result)?;
    // The stored result is the truth, whichever call stored it: a crash
    // between storing it and the events is repaired by the next call, which
    // writes the events the result implies and the journal lacks.
    ensure_truncation_events(store, attempt, &result, at_ms)?;
    Ok(Recorded { result, fresh })
}

/// Write `log_truncated` for each stream the result says dropped bytes, unless
/// the journal already has it for this attempt and stream (idempotent). The
/// check and the writes are one transaction, and the events carry the
/// attempt's run, like every other event of the worker.
fn ensure_truncation_events(
    store: &WorkerStore,
    attempt: &AttemptId,
    result: &Value,
    at_ms: u64,
) -> Result<()> {
    let wanted: Vec<(String, Value)> = ["stdout", "stderr"]
        .into_iter()
        .filter_map(|name| {
            let l = &result["logs"][name];
            (l["log_truncated"] == true).then(|| {
                (
                    name.to_string(),
                    json!({
                        "stream": name,
                        "dropped_bytes": l["dropped_bytes"].as_u64().unwrap_or(0),
                        "stored_bytes": l["bytes"].as_u64().unwrap_or(0),
                    }),
                )
            })
        })
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let run = store
        .get(attempt)?
        .ok_or_else(|| StorageError::NotFound {
            what: format!("attempt {attempt}"),
        })?
        .run_id;
    store.append_events_unseen(
        Some(&run),
        attempt,
        "log_truncated",
        "stream",
        &wanted,
        at_ms,
    )?;
    Ok(())
}

/// What a deletion pass did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DrainReport {
    pub deleted: u32,
    /// Queued locators that are not files of this store: left alone, still
    /// queued.
    pub foreign: Vec<String>,
    /// Locators whose deletion failed (`locator`, the I/O error): still queued,
    /// to be retried by the next pass. One failure does not stop the others.
    pub failed: Vec<(String, String)>,
}

/// After a file went: remove the directories above it that are now empty, up
/// to (not including) the root, sweeping `.tmp*` leftovers of crashed copies
/// out of them first. Stops at the first directory that is not empty.
fn prune_empty_dirs(root: &Path, rel: &Path) {
    let mut cur = rel.to_path_buf();
    while cur.pop() && !cur.as_os_str().is_empty() {
        let dir = root.join(&cur);
        if let Ok(rd) = fs::read_dir(&dir) {
            for e in rd.flatten() {
                let leftover = e.file_type().is_ok_and(|t| t.is_file())
                    && is_tmp_name(&e.file_name().to_string_lossy());
                if leftover {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        match fs::remove_dir(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => break,
        }
    }
}

/// Delete the files GC queued in `pending_delete`, then tell the database.
/// A file already gone counts as deleted (a crash between the two steps).
/// The directories left empty (`<attempt>/artifacts/...`, the attempt's own)
/// go too. A failing I/O operation is reported in `failed` and the pass goes on;
/// a database error stops it.
pub fn drain_pending_deletes(store: &WorkerStore, root: &Path) -> Result<DrainReport> {
    let mut report = DrainReport::default();
    for locator in store.pending_deletes()? {
        let Some(rel) = resolve_locator(&locator) else {
            report.foreign.push(locator);
            continue;
        };
        let path = root.join(&rel);
        let comps: Vec<&std::ffi::OsStr> = rel.parent().into_iter().flat_map(Path::iter).collect();
        // A planted symlink on the way would make the deletion leave the area.
        if comps
            .iter()
            .try_fold(root.to_path_buf(), |mut cur, c| {
                cur.push(c);
                fs::symlink_metadata(&cur)
                    .map_or(true, |m| m.is_dir())
                    .then_some(cur)
            })
            .is_none()
        {
            report.foreign.push(locator);
            continue;
        }
        let removed = match fs::symlink_metadata(&path) {
            Ok(m) if m.is_dir() => fs::remove_dir_all(&path),
            Ok(_) => fs::remove_file(&path),
            Err(e) => Err(e),
        };
        match removed {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                report.failed.push((locator, e.to_string()));
                continue;
            }
        }
        prune_empty_dirs(root, &rel);
        store.complete_delete(&locator)?;
        report.deleted += 1;
    }
    Ok(report)
}
