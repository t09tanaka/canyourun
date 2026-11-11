//! The parent's files of collected results (spec chapter 13).
//!
//! Layout under the root (the `results` directory of the parent's state):
//!
//! ```text
//! <attempt>/result.json        the result the worker answered
//! <attempt>/logs/<stream>.log  the collected log of a stream
//! <attempt>/artifacts/<name>   a verified artifact
//! <attempt>/tmp/<sha256>.part  an artifact that is still arriving
//! ```
//!
//! An artifact appears under `artifacts/` only whole: its bytes arrive in a
//! `.part` file, which is hashed, compared with the recorded digest and
//! size, synced and then renamed. A `.part` file is named by the digest it
//! must have, so a collection that is resumed (by this process or the next)
//! continues it; every chunk is synced before the next is asked for. A log
//! grows in place: whether it is complete is recorded in the database, not
//! by the file.
//!
//! One process works here at a time: the parent holds its process lock, and
//! one collection runs at a time.

use crate::ids::{AttemptId, Digest};
use crate::supervisor::spool_plan::{relative_path, sha256_digest};
use crate::wire::LogStream;
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// The prefix of the locators of this store in the GC references.
pub const LOCATOR_PREFIX: &str = "results/";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("{0} is not a name the result store can use")]
    BadName(String),
    /// The file is not as long as the caller thinks: nothing was written.
    #[error("the file holds {have} bytes, not {want}")]
    Offset { want: u64, have: u64 },
    /// Something else than the recorded artifact is published under its name.
    #[error("artifact {0} is published with another content")]
    Conflict(String),
}

type Result<T, E = StoreError> = std::result::Result<T, E>;

/// How far an artifact is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactState {
    /// It is published and is what the record says.
    Published,
    /// This many of its bytes are here.
    Partial(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Publish {
    Published,
    /// The bytes that arrived do not have the recorded digest or size: they
    /// were discarded.
    Mismatch,
}

#[derive(Debug, Clone)]
pub struct ResultStore {
    root: PathBuf,
}

fn stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn dir_all(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn len_of(path: &Path) -> io::Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

/// The digest and size of a regular file; `None` when there is no file.
fn digest_of(path: &Path) -> io::Result<Option<(Digest, u64)>> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if !meta.file_type().is_file() {
        return Err(io::Error::other(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
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
    let digest = sha256_digest(&h.finalize()).expect("sha256 hex is a digest");
    Ok(Some((digest, size)))
}

/// Append `data` to the file at `path`, which must hold exactly `offset`
/// bytes, and sync it. Returns the new length.
fn append_at(path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
    if let Some(parent) = path.parent() {
        dir_all(parent)?;
    }
    let mut f = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let have = f.metadata()?.len();
    if have != offset {
        return Err(StoreError::Offset { want: offset, have });
    }
    f.write_all(data)?;
    f.sync_all()?;
    Ok(offset + data.len() as u64)
}

impl ResultStore {
    /// Open (and make) the store at `root`.
    pub fn open(root: &Path) -> Result<ResultStore> {
        dir_all(root)?;
        Ok(ResultStore {
            root: root.to_path_buf(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn attempt_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.root.join(attempt.as_str())
    }

    pub fn result_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("result.json")
    }

    pub fn log_path(&self, attempt: &AttemptId, stream: LogStream) -> PathBuf {
        self.attempt_dir(attempt)
            .join("logs")
            .join(format!("{}.log", stream_name(stream)))
    }

    /// Where the artifact `name` of an attempt is published.
    pub fn artifact_path(&self, attempt: &AttemptId, name: &str) -> Result<PathBuf> {
        let rel = relative_path(name).ok_or_else(|| StoreError::BadName(name.to_string()))?;
        Ok(self.attempt_dir(attempt).join("artifacts").join(rel))
    }

    fn part_path(&self, attempt: &AttemptId, digest: &Digest) -> PathBuf {
        let hex = digest.as_str().trim_start_matches(Digest::PREFIX);
        self.attempt_dir(attempt)
            .join("tmp")
            .join(format!("{hex}.part"))
    }

    pub fn result_locator(attempt: &AttemptId) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/result.json")
    }

    pub fn logs_locator(attempt: &AttemptId) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/logs")
    }

    pub fn artifact_locator(attempt: &AttemptId, name: &str) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/artifacts/{name}")
    }

    /// Write the result of an attempt: whole or not at all. The same bytes
    /// again, or the bytes of a newer result, replace it the same way.
    pub fn write_result(&self, attempt: &AttemptId, bytes: &[u8]) -> Result<()> {
        let dir = self.attempt_dir(attempt);
        dir_all(&dir)?;
        let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
        tmp.write_all(bytes)?;
        tmp.as_file().sync_all()?;
        tmp.persist(self.result_path(attempt))
            .map_err(|e| StoreError::Io(e.error))?;
        sync_dir(&dir)?;
        Ok(())
    }

    pub fn read_result(&self, attempt: &AttemptId) -> Result<Option<Vec<u8>>> {
        match fs::read(self.result_path(attempt)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// How many bytes of a stream are here.
    pub fn log_len(&self, attempt: &AttemptId, stream: LogStream) -> Result<u64> {
        Ok(len_of(&self.log_path(attempt, stream))?)
    }

    /// Append to a stream at `offset` (its length) and sync.
    pub fn append_log(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
        offset: u64,
        data: &[u8],
    ) -> Result<u64> {
        append_at(&self.log_path(attempt, stream), offset, data)
    }

    pub fn read_log(&self, attempt: &AttemptId, stream: LogStream) -> Result<Vec<u8>> {
        match fs::read(self.log_path(attempt, stream)) {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    /// How far the artifact `name` (of `size` bytes, with `digest`) is. A
    /// published file is hashed again: it counts only if it is the recorded
    /// one. Bytes past the recorded size are discarded.
    pub fn artifact_state(
        &self,
        attempt: &AttemptId,
        name: &str,
        digest: &Digest,
        size: u64,
    ) -> Result<ArtifactState> {
        let path = self.artifact_path(attempt, name)?;
        if let Some((d, s)) = digest_of(&path)? {
            if &d == digest && s == size {
                return Ok(ArtifactState::Published);
            }
            return Err(StoreError::Conflict(name.to_string()));
        }
        let part = self.part_path(attempt, digest);
        let have = len_of(&part)?;
        if have > size {
            fs::remove_file(&part)?;
            return Ok(ArtifactState::Partial(0));
        }
        Ok(ArtifactState::Partial(have))
    }

    /// Append a chunk of the blob with `digest` at `offset` and sync.
    pub fn append_part(
        &self,
        attempt: &AttemptId,
        digest: &Digest,
        offset: u64,
        data: &[u8],
    ) -> Result<u64> {
        append_at(&self.part_path(attempt, digest), offset, data)
    }

    /// Publish the arrived bytes as the artifact `name` if they are `size`
    /// bytes with `digest`; otherwise discard them. Publishing what is
    /// published already changes nothing.
    pub fn publish_artifact(
        &self,
        attempt: &AttemptId,
        name: &str,
        digest: &Digest,
        size: u64,
    ) -> Result<Publish> {
        let path = self.artifact_path(attempt, name)?;
        let part = self.part_path(attempt, digest);
        if let Some((d, s)) = digest_of(&path)? {
            if &d != digest || s != size {
                return Err(StoreError::Conflict(name.to_string()));
            }
            match fs::remove_file(&part) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                _ => return Ok(Publish::Published),
            }
        }
        // An empty artifact has no chunk that would have made the file.
        append_at(&part, len_of(&part)?, &[])?;
        match digest_of(&part)? {
            Some((d, s)) if &d == digest && s == size => {}
            _ => {
                fs::remove_file(&part)?;
                return Ok(Publish::Mismatch);
            }
        }
        let parent = path.parent().expect("an artifact path has a parent");
        dir_all(parent)?;
        fs::rename(&part, &path)?;
        sync_dir(parent)?;
        Ok(Publish::Published)
    }

    /// Whether `locator` names a file of this store.
    pub fn owns(locator: &str) -> bool {
        locator
            .strip_prefix(LOCATOR_PREFIX)
            .is_some_and(|rest| relative_path(rest).is_some())
    }

    /// Delete what `locator` names (a file or a directory). Deleting what
    /// is gone already is fine. The attempt's directory goes with its last
    /// entry.
    pub fn remove(&self, locator: &str) -> Result<()> {
        let rel = locator
            .strip_prefix(LOCATOR_PREFIX)
            .and_then(relative_path)
            .ok_or_else(|| StoreError::BadName(locator.to_string()))?;
        let path = self.root.join(&rel);
        let gone = match fs::symlink_metadata(&path) {
            Ok(m) if m.is_dir() => fs::remove_dir_all(&path),
            Ok(_) => fs::remove_file(&path),
            Err(e) => Err(e),
        };
        match gone {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        // Empty directories above it, up to the attempt's own.
        let mut dir = path.parent();
        while let Some(d) = dir.filter(|d| *d != self.root && d.starts_with(&self.root)) {
            if d.parent() == Some(self.root.as_path()) {
                // What is left of an attempt without files is its `tmp`.
                let _ = fs::remove_dir(d.join("tmp"));
            }
            if fs::remove_dir(d).is_err() {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }
}
