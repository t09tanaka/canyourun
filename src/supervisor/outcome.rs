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

use super::spool::{AttemptSpool, SpoolError, StreamStatus};
use super::spool_plan::{
    artifact_locator, logs_locator, relative_path, resolve_locator, result_json, sha256_digest,
    StreamSummary,
};
use crate::ids::{AttemptId, Digest, PlanDigest, SourceId};
use crate::state::Termination;
use crate::storage::{BlobKind, NewBlobRef, StorageError, WorkerStore};
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
    #[error("artifact source {} is not a regular file", .0.display())]
    NotRegular(PathBuf),
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

/// Copy a produced file into `<attempt>/artifacts/<name>`: hashed while
/// copied, through a uniquely named temporary file, fsynced, then moved into
/// place without replacing anything. A file over `max_bytes`, a non-regular
/// source (a symlink, a device) or a name that is not a plain relative path is
/// refused before anything is kept. The same name with the same content is a
/// replay (the existing file is kept); with other content it is a conflict.
pub fn store_artifact(
    root: &Path,
    attempt: &AttemptId,
    input: &ArtifactInput<'_>,
    max_bytes: u64,
) -> Result<ArtifactRecord> {
    let rel =
        relative_path(input.name).ok_or_else(|| OutcomeError::BadName(input.name.to_string()))?;
    let meta = fs::symlink_metadata(input.source)?;
    if !meta.file_type().is_file() {
        return Err(OutcomeError::NotRegular(input.source.to_path_buf()));
    }
    let mut src = File::open(input.source)?;
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
            if file_digest(&dest)? != digest {
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

fn file_digest(path: &Path) -> Result<Digest> {
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
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(sha256_digest(&h.finalize()).expect("sha256 hex is a digest"))
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

fn is_duplicate(e: &StorageError) -> bool {
    matches!(e, StorageError::Sqlite(r)
        if r.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation))
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
        // A replay of the same call: the reference is already there.
        Err(e) if is_duplicate(&e) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Close the attempt's spool, reference its files, fix its termination and
/// store its result: in that order, so a crash at any point leaves a state
/// that calling this again completes. The termination in the result is the
/// one that won (a cancel may have fixed it first). A result that is already
/// stored is returned as is (`fresh == false`); stored artifacts that differ
/// from it are an error, never a second result. Sizes and drop counts of both
/// streams go in the result, and `log_truncated` is also written to the event
/// journal for each stream that dropped bytes, exactly once: the stored result
/// is the truth and the events are made to match it on every call.
///
/// If the spool cannot be completed (an earlier write failed), this returns
/// [`OutcomeError::SpoolFailed`] before recording anything: the attempt's
/// result is then S9a's (the supervisor's) decision, not this module's.
pub fn finish_attempt(
    store: &WorkerStore,
    attempt: &AttemptId,
    spool: &AttemptSpool,
    termination: Termination,
    artifacts: &[ArtifactRecord],
    at_ms: u64,
) -> Result<Recorded> {
    let existing = store.get_result(attempt)?;
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
        if &a.attempt_id != attempt || relative_path(&a.path).is_none() {
            return Err(OutcomeError::BadName(a.path.clone()));
        }
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
    store.record_result(attempt, &result)?;
    // The stored result is the truth, whichever call stored it: a crash
    // between storing it and the events is repaired by the next call, which
    // writes the events the result implies and the journal lacks.
    let stored = existing.clone().unwrap_or_else(|| result.clone());
    ensure_truncation_events(store, attempt, &stored, at_ms)?;
    Ok(match existing {
        Some(result) => Recorded {
            result,
            fresh: false,
        },
        None => Recorded {
            result,
            fresh: true,
        },
    })
}

/// Write `log_truncated` for each stream the result says dropped bytes, unless
/// the journal already has it for this attempt and stream (idempotent).
fn ensure_truncation_events(
    store: &WorkerStore,
    attempt: &AttemptId,
    result: &Value,
    at_ms: u64,
) -> Result<()> {
    let wanted: Vec<(&str, u64, u64)> = ["stdout", "stderr"]
        .into_iter()
        .filter_map(|name| {
            let l = &result["logs"][name];
            (l["log_truncated"] == true).then(|| {
                (
                    name,
                    l["dropped_bytes"].as_u64().unwrap_or(0),
                    l["bytes"].as_u64().unwrap_or(0),
                )
            })
        })
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let mut seen: Vec<String> = Vec::new();
    let mut after = 0;
    loop {
        let page = store.events_after(after, 500)?;
        let Some(last) = page.last() else { break };
        after = last.seq;
        for e in &page {
            if e.kind == "log_truncated" && e.attempt_id.as_deref() == Some(attempt.as_str()) {
                if let Some(st) = e.payload["stream"].as_str() {
                    seen.push(st.to_string());
                }
            }
        }
    }
    for (name, dropped, stored) in wanted {
        if !seen.iter().any(|s| s == name) {
            store.append_event(
                None,
                Some(attempt),
                "log_truncated",
                &json!({"stream": name, "dropped_bytes": dropped, "stored_bytes": stored}),
                at_ms,
            )?;
        }
    }
    Ok(())
}

/// What a deletion pass did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DrainReport {
    pub deleted: u32,
    /// Queued locators that are not files of this store: left alone, still
    /// queued.
    pub foreign: Vec<String>,
}

/// Delete the files GC queued in `pending_delete`, then tell the database.
/// A file already gone counts as deleted (a crash between the two steps).
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
            Err(e) => return Err(e.into()),
        }
        store.complete_delete(&locator)?;
        report.deleted += 1;
    }
    Ok(report)
}
