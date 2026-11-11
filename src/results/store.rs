//! The parent's files of collected results (spec chapter 13).
//!
//! Layout under the root (the `results` directory of the parent's state):
//!
//! ```text
//! <attempt>/result.json         the result the worker answered
//! <attempt>/logs/<stream>.log   the collected log of a stream
//! <attempt>/artifacts/<sha256>  a verified artifact, named by its digest
//! <attempt>/tmp/<sha256>.part   an artifact that is still arriving
//! ```
//!
//! No name a worker chose is ever a path here: an artifact is stored under
//! its digest, and which declared name has which digest is in the result and
//! in the receipts of the parent database. Names that are one path on this
//! file system and two on the worker's (case, Unicode form), a name that is
//! a directory of another, and a name that is too long for a path of the
//! parent therefore cannot collide or fail.
//!
//! An artifact appears under `artifacts/` only whole: its bytes arrive in a
//! `.part` file, which is hashed, compared with the recorded digest and
//! size, synced and then renamed. A `.part` file is named by the digest it
//! must have, so a collection that is resumed (by this process or the next)
//! continues it; every chunk is synced before the next is asked for. A log
//! grows in place: whether it is complete is recorded in the database, not
//! by the file.
//!
//! Every directory entry that is made is made durable: the parent of a new
//! directory or file is synced, at every level. [`ResultStore::verify_log`],
//! [`ResultStore::verify_artifact`] and [`ResultStore::seal`] read back what
//! the database records and sync it once more; the collection sends its
//! acknowledgement only after them.
//!
//! One process works here at a time: the parent holds its process lock, and
//! one collection runs at a time.

use crate::ids::{AttemptId, Digest};
use crate::supervisor::spool_plan::{relative_path, sha256_digest};
use crate::wire::LogStream;
use sha2::{Digest as _, Sha256};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// The prefix of the locators of this store in the GC references.
pub const LOCATOR_PREFIX: &str = "results/";

/// A point right after a file operation of the store, before its caller
/// records it in the database. A test asks the store to stop there once
/// ([`ResultStore::crash_once_at`]): what a parent that died at that point
/// leaves behind is then on disk, and the next one continues from it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Crash {
    ResultWritten,
    LogAppended,
    ChunkAppended,
    ArtifactPublished,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("{0} is not a name the result store can use")]
    BadName(String),
    /// The file is not as long as the caller thinks: nothing was written.
    #[error("the file holds {have} bytes, not {want}")]
    Offset { want: u64, have: u64 },
    /// A test asked the store to stop here.
    #[error("stopped after {0:?}")]
    Crash(Crash),
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
    /// were discarded. `whole`: this process wrote every one of them, from
    /// the first; otherwise the file was begun by a parent that is gone and
    /// may have been torn.
    Mismatch {
        whole: bool,
    },
}

#[derive(Debug, Clone)]
pub struct ResultStore {
    root: PathBuf,
    /// The `.part` files this process began at their first byte.
    begun: Arc<Mutex<HashSet<PathBuf>>>,
    crash: Arc<Mutex<Option<Crash>>>,
}

fn stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn parent_of(path: &Path) -> Option<&Path> {
    path.parent().filter(|p| !p.as_os_str().is_empty())
}

/// Make `path` and every level above it that is missing, and make each new
/// entry durable: the parent of every directory that was made is synced.
fn dir_all(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut at = Some(path);
    while let Some(dir) = at {
        match fs::metadata(dir) {
            Ok(m) if m.is_dir() => break,
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "{} is not a directory",
                    dir.display()
                )))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                missing.push(dir);
                at = parent_of(dir);
            }
            Err(e) => return Err(e),
        }
    }
    for dir in missing.into_iter().rev() {
        match fs::DirBuilder::new().mode(0o700).create(dir) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
            _ => {}
        }
        if let Some(parent) = parent_of(dir) {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

fn len_of(path: &Path) -> io::Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

fn remove_if_there(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// The digest and size of a regular file, which is synced on the way;
/// `None` when there is no file.
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
    f.sync_all()?;
    let digest = sha256_digest(&h.finalize()).expect("sha256 hex is a digest");
    Ok(Some((digest, size)))
}

/// Append `data` to the file at `path`, which must hold exactly `offset`
/// bytes, and sync it; a file that was made by this is made durable in its
/// directory. Returns the new length.
fn append_at(path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
    let parent = path.parent().expect("a file of the store has a parent");
    dir_all(parent)?;
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
    if have == 0 {
        // New, or as good as: its entry may not be durable yet.
        sync_dir(parent)?;
    }
    Ok(offset + data.len() as u64)
}

fn hex(digest: &Digest) -> &str {
    digest.as_str().trim_start_matches(Digest::PREFIX)
}

impl ResultStore {
    /// Open (and make) the store at `root`. One instance (and its clones)
    /// is to serve a parent process for as long as it lives: which `.part`
    /// files this process began itself is remembered in it, and bytes
    /// without their digest count against the worker only then
    /// ([`Publish::Mismatch`]). A store that is opened anew for every pass
    /// never holds a mismatch of a transfer that spans passes against the
    /// worker.
    pub fn open(root: &Path) -> Result<ResultStore> {
        dir_all(root)?;
        Ok(ResultStore {
            root: root.to_path_buf(),
            begun: Arc::default(),
            crash: Arc::default(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stop with [`StoreError::Crash`] the next time `point` is passed.
    #[doc(hidden)]
    pub fn crash_once_at(&self, point: Crash) {
        *self.crash.lock().unwrap_or_else(|e| e.into_inner()) = Some(point);
    }

    fn passed(&self, point: Crash) -> Result<()> {
        let mut asked = self.crash.lock().unwrap_or_else(|e| e.into_inner());
        if *asked == Some(point) {
            *asked = None;
            return Err(StoreError::Crash(point));
        }
        Ok(())
    }

    fn attempt_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.root.join(attempt.as_str())
    }

    fn tmp_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("tmp")
    }

    pub fn result_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("result.json")
    }

    pub fn log_path(&self, attempt: &AttemptId, stream: LogStream) -> PathBuf {
        self.attempt_dir(attempt)
            .join("logs")
            .join(format!("{}.log", stream_name(stream)))
    }

    /// Where the artifact with `digest` of an attempt is published.
    pub fn artifact_path(&self, attempt: &AttemptId, digest: &Digest) -> PathBuf {
        self.attempt_dir(attempt)
            .join("artifacts")
            .join(hex(digest))
    }

    fn part_path(&self, attempt: &AttemptId, digest: &Digest) -> PathBuf {
        self.tmp_dir(attempt).join(format!("{}.part", hex(digest)))
    }

    pub fn result_locator(attempt: &AttemptId) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/result.json")
    }

    pub fn logs_locator(attempt: &AttemptId) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/logs")
    }

    pub fn artifact_locator(attempt: &AttemptId, digest: &Digest) -> String {
        format!("{LOCATOR_PREFIX}{attempt}/artifacts/{}", hex(digest))
    }

    /// Write the result of an attempt: whole or not at all, and durable
    /// when this returns. The same bytes again, or the bytes of a newer
    /// result, replace it the same way.
    pub fn write_result(&self, attempt: &AttemptId, bytes: &[u8]) -> Result<()> {
        let dir = self.attempt_dir(attempt);
        let tmp_dir = self.tmp_dir(attempt);
        dir_all(&tmp_dir)?;
        let mut tmp = tempfile::NamedTempFile::new_in(&tmp_dir)?;
        tmp.write_all(bytes)?;
        tmp.as_file().sync_all()?;
        tmp.persist(self.result_path(attempt))
            .map_err(|e| StoreError::Io(e.error))?;
        sync_dir(&dir)?;
        self.passed(Crash::ResultWritten)
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
        let len = append_at(&self.log_path(attempt, stream), offset, data)?;
        self.passed(Crash::LogAppended)?;
        Ok(len)
    }

    pub fn read_log(&self, attempt: &AttemptId, stream: LogStream) -> Result<Vec<u8>> {
        match fs::read(self.log_path(attempt, stream)) {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    /// The digest and length of what is here of a stream (of nothing, when
    /// there is no file).
    pub fn log_digest(&self, attempt: &AttemptId, stream: LogStream) -> Result<(Digest, u64)> {
        Ok(digest_of(&self.log_path(attempt, stream))?
            .unwrap_or_else(|| (Digest::of_bytes(b""), 0)))
    }

    /// Whether the stream is here, durable, with exactly the recorded
    /// `bytes` and `digest`.
    pub fn verify_log(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
        digest: &Digest,
        bytes: u64,
    ) -> Result<bool> {
        let path = self.log_path(attempt, stream);
        match digest_of(&path)? {
            Some((d, s)) if &d == digest && s == bytes => {
                sync_dir(path.parent().expect("a log has a directory"))?;
                Ok(true)
            }
            // A stream that never had a byte has no file.
            None => Ok(bytes == 0 && digest == &Digest::of_bytes(b"")),
            Some(_) => Ok(false),
        }
    }

    /// Drop what is here of a stream: it is read again from its start.
    pub fn discard_log(&self, attempt: &AttemptId, stream: LogStream) -> Result<()> {
        Ok(remove_if_there(&self.log_path(attempt, stream))?)
    }

    /// How far the artifact with `digest` (of `size` bytes) is. A published
    /// file is hashed again: it counts only if it is the recorded one, and
    /// one that is not (it was damaged where it lay) is removed. Arrived
    /// bytes past the recorded size are discarded.
    pub fn artifact_state(
        &self,
        attempt: &AttemptId,
        digest: &Digest,
        size: u64,
    ) -> Result<ArtifactState> {
        let path = self.artifact_path(attempt, digest);
        if let Some((d, s)) = digest_of(&path)? {
            if &d == digest && s == size {
                return Ok(ArtifactState::Published);
            }
            fs::remove_file(&path)?;
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
        let part = self.part_path(attempt, digest);
        let len = append_at(&part, offset, data)?;
        if offset == 0 {
            self.begun().insert(part);
        }
        self.passed(Crash::ChunkAppended)?;
        Ok(len)
    }

    fn begun(&self) -> std::sync::MutexGuard<'_, HashSet<PathBuf>> {
        self.begun.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Publish the arrived bytes of the blob with `digest` if they are
    /// `size` bytes with that digest; otherwise discard them. Publishing
    /// what is published already changes nothing.
    pub fn publish_artifact(
        &self,
        attempt: &AttemptId,
        digest: &Digest,
        size: u64,
    ) -> Result<Publish> {
        let path = self.artifact_path(attempt, digest);
        let part = self.part_path(attempt, digest);
        if self.artifact_state(attempt, digest, size)? == ArtifactState::Published {
            remove_if_there(&part)?;
            self.begun().remove(&part);
            return Ok(Publish::Published);
        }
        // An empty artifact has no chunk that would have made the file.
        let have = len_of(&part)?;
        if have == 0 {
            self.append_part(attempt, digest, 0, &[])?;
        }
        let whole = self.begun().remove(&part);
        match digest_of(&part)? {
            Some((d, s)) if &d == digest && s == size => {}
            _ => {
                fs::remove_file(&part)?;
                return Ok(Publish::Mismatch { whole });
            }
        }
        let parent = path.parent().expect("an artifact path has a parent");
        dir_all(parent)?;
        fs::rename(&part, &path)?;
        sync_dir(parent)?;
        sync_dir(&self.tmp_dir(attempt))?;
        self.passed(Crash::ArtifactPublished)?;
        Ok(Publish::Published)
    }

    /// Whether the artifact with `digest` is published, durable, and is
    /// `size` bytes with that digest.
    pub fn verify_artifact(&self, attempt: &AttemptId, digest: &Digest, size: u64) -> Result<bool> {
        let path = self.artifact_path(attempt, digest);
        match digest_of(&path)? {
            Some((d, s)) if &d == digest && s == size => {
                sync_dir(path.parent().expect("an artifact path has a parent"))?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Make the directory of an attempt, and its entry in the store,
    /// durable. The last file operation before an acknowledgement.
    pub fn seal(&self, attempt: &AttemptId) -> Result<()> {
        let dir = self.attempt_dir(attempt);
        dir_all(&dir)?;
        sync_dir(&dir)?;
        sync_dir(&self.root)?;
        Ok(())
    }

    /// Remove what is left of unfinished transfers of an attempt.
    pub fn clear_tmp(&self, attempt: &AttemptId) -> Result<()> {
        let tmp = self.tmp_dir(attempt);
        self.begun().retain(|p| !p.starts_with(&tmp));
        match fs::remove_dir_all(&tmp) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
            Err(_) => Ok(()),
            Ok(()) => Ok(sync_dir(&self.attempt_dir(attempt))?),
        }
    }

    /// Whether `locator` names a file of this store.
    pub fn owns(locator: &str) -> bool {
        locator
            .strip_prefix(LOCATOR_PREFIX)
            .is_some_and(|rest| relative_path(rest).is_some())
    }

    /// Delete what `locator` names (a file or a directory). Deleting what
    /// is gone already is fine. The attempt's directory goes with its last
    /// entry, and with it what unfinished transfers left behind.
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
                let only_tmp = fs::read_dir(d)
                    .is_ok_and(|entries| entries.flatten().all(|e| e.file_name() == "tmp"));
                if only_tmp {
                    let _ = fs::remove_dir_all(d.join("tmp"));
                }
            }
            if fs::remove_dir(d).is_err() {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const ALPHA: &[u8] = b"0123456789";

    struct Fx {
        dir: TempDir,
        a: AttemptId,
    }

    fn fx() -> Fx {
        Fx {
            dir: tempfile::tempdir().unwrap(),
            a: AttemptId::generate(),
        }
    }

    impl Fx {
        /// The store of this process, or of the next after a crash.
        fn open(&self) -> ResultStore {
            ResultStore::open(&self.dir.path().join("state").join("results")).unwrap()
        }
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .map(|d| {
                d.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn every_missing_level_is_made_private_and_a_file_in_the_way_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a").join("b").join("c");
        dir_all(&deep).unwrap();
        dir_all(&deep).unwrap();
        for level in [&deep, deep.parent().unwrap(), &dir.path().join("a")] {
            assert!(level.is_dir());
            assert_eq!(mode(level), 0o700);
        }
        fs::write(dir.path().join("file"), b"x").unwrap();
        let e = dir_all(&dir.path().join("file")).unwrap_err();
        assert!(e.to_string().contains("is not a directory"), "{e}");
        assert!(dir_all(&dir.path().join("file").join("below")).is_err());
        // A relative path of one level has no parent to make.
        assert_eq!(parent_of(Path::new("one")), None);
        assert_eq!(parent_of(Path::new("one/two")), Some(Path::new("one")));
    }

    #[test]
    fn the_paths_and_locators_never_hold_a_name_of_the_worker() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        let h = hex(&d);
        assert_eq!(h.len(), 64);
        assert!(s.root().ends_with("state/results") && s.root().is_dir());
        assert_eq!(
            s.result_path(a),
            s.root().join(a.as_str()).join("result.json")
        );
        assert_eq!(
            s.log_path(a, LogStream::Stdout),
            s.root().join(a.as_str()).join("logs").join("stdout.log")
        );
        assert_eq!(
            s.log_path(a, LogStream::Stderr),
            s.root().join(a.as_str()).join("logs").join("stderr.log")
        );
        assert_eq!(
            s.artifact_path(a, &d),
            s.root().join(a.as_str()).join("artifacts").join(h)
        );
        assert_eq!(
            s.part_path(a, &d),
            s.root()
                .join(a.as_str())
                .join("tmp")
                .join(format!("{h}.part"))
        );
        assert_eq!(
            ResultStore::result_locator(a),
            format!("results/{a}/result.json")
        );
        assert_eq!(ResultStore::logs_locator(a), format!("results/{a}/logs"));
        assert_eq!(
            ResultStore::artifact_locator(a, &d),
            format!("results/{a}/artifacts/{h}")
        );
        for locator in [
            ResultStore::result_locator(a),
            ResultStore::logs_locator(a),
            ResultStore::artifact_locator(a, &d),
        ] {
            assert!(ResultStore::owns(&locator), "{locator}");
        }
        assert!(!ResultStore::owns("blobs/x"));
        assert!(!ResultStore::owns("results/../x"));
        assert!(!ResultStore::owns("results/"));
    }

    #[test]
    fn a_result_is_written_whole_replaced_whole_and_leaves_no_temporary_file() {
        let f = fx();
        let s = f.open();
        assert_eq!(s.read_result(&f.a).unwrap(), None);
        s.write_result(&f.a, b"{\"one\":1}").unwrap();
        assert_eq!(s.read_result(&f.a).unwrap().unwrap(), b"{\"one\":1}");
        s.write_result(&f.a, b"{\"one\":1}").unwrap();
        s.write_result(&f.a, b"{}").unwrap();
        assert_eq!(s.read_result(&f.a).unwrap().unwrap(), b"{}");
        let dir = s.root().join(f.a.as_str());
        assert_eq!(names(&dir), vec!["result.json", "tmp"]);
        assert_eq!(names(&dir.join("tmp")), Vec::<String>::new());
        assert_eq!(mode(&s.result_path(&f.a)), 0o600);
        // Something that is not a file where the result goes is an error.
        let other = AttemptId::generate();
        fs::create_dir_all(s.result_path(&other)).unwrap();
        assert!(matches!(s.read_result(&other), Err(StoreError::Io(_))));
        assert!(matches!(
            s.write_result(&other, b"x"),
            Err(StoreError::Io(_))
        ));
    }

    #[test]
    fn a_parent_that_dies_after_writing_the_result_finds_it_and_writes_it_again() {
        let f = fx();
        let s = f.open();
        s.crash_once_at(Crash::ResultWritten);
        assert!(matches!(
            s.write_result(&f.a, b"first"),
            Err(StoreError::Crash(Crash::ResultWritten))
        ));
        drop(s);
        let s = f.open();
        assert_eq!(s.read_result(&f.a).unwrap().unwrap(), b"first");
        s.write_result(&f.a, b"second").unwrap();
        assert_eq!(s.read_result(&f.a).unwrap().unwrap(), b"second");
        // The stop is asked for once, and only at its own point.
        s.crash_once_at(Crash::LogAppended);
        s.write_result(&f.a, b"third").unwrap();
        assert!(s.append_log(&f.a, LogStream::Stdout, 0, b"x").is_err());
        assert_eq!(s.append_log(&f.a, LogStream::Stdout, 1, b"y").unwrap(), 2);
    }

    #[test]
    fn a_log_grows_only_at_its_end_and_is_verified_by_length_and_digest() {
        let f = fx();
        let s = f.open();
        let (a, out) = (&f.a, LogStream::Stdout);
        assert_eq!(s.log_len(a, out).unwrap(), 0);
        assert_eq!(s.read_log(a, out).unwrap(), b"");
        let empty = Digest::of_bytes(b"");
        assert_eq!(s.log_digest(a, out).unwrap(), (empty.clone(), 0));
        // A stream without a byte has no file, and that is what was recorded.
        assert!(s.verify_log(a, out, &empty, 0).unwrap());
        assert!(!s.verify_log(a, out, &empty, 1).unwrap());
        assert!(!s.verify_log(a, out, &Digest::of_bytes(b"x"), 0).unwrap());

        assert_eq!(s.append_log(a, out, 0, b"hello ").unwrap(), 6);
        assert_eq!(s.append_log(a, out, 6, b"log\n").unwrap(), 10);
        assert_eq!(s.append_log(a, out, 10, b"").unwrap(), 10);
        // Not at its end: nothing is written.
        for wrong in [0, 9, 11] {
            assert!(matches!(
                s.append_log(a, out, wrong, b"again"),
                Err(StoreError::Offset { want, have: 10 }) if want == wrong
            ));
        }
        assert_eq!(s.read_log(a, out).unwrap(), b"hello log\n");
        assert_eq!(s.log_len(a, out).unwrap(), 10);
        assert_eq!(s.log_len(a, LogStream::Stderr).unwrap(), 0);
        assert_eq!(mode(&s.log_path(a, out)), 0o600);

        let d = Digest::of_bytes(b"hello log\n");
        assert_eq!(s.log_digest(a, out).unwrap(), (d.clone(), 10));
        assert!(s.verify_log(a, out, &d, 10).unwrap());
        assert!(!s.verify_log(a, out, &d, 9).unwrap());
        assert!(!s.verify_log(a, out, &empty, 10).unwrap());
        assert!(!s.verify_log(a, out, &empty, 0).unwrap());
        // Damaged where it lay: same length, other bytes.
        fs::write(s.log_path(a, out), b"HELLO log\n").unwrap();
        assert!(!s.verify_log(a, out, &d, 10).unwrap());
        s.discard_log(a, out).unwrap();
        s.discard_log(a, out).unwrap();
        assert_eq!(s.log_len(a, out).unwrap(), 0);
        assert_eq!(s.append_log(a, out, 0, b"again").unwrap(), 5);
    }

    #[test]
    fn a_log_that_was_cut_off_by_a_crash_is_continued_from_its_length() {
        let f = fx();
        let s = f.open();
        s.append_log(&f.a, LogStream::Stderr, 0, b"abc").unwrap();
        s.crash_once_at(Crash::LogAppended);
        assert!(matches!(
            s.append_log(&f.a, LogStream::Stderr, 3, b"def"),
            Err(StoreError::Crash(Crash::LogAppended))
        ));
        drop(s);
        let s = f.open();
        assert_eq!(s.log_len(&f.a, LogStream::Stderr).unwrap(), 6);
        assert_eq!(s.append_log(&f.a, LogStream::Stderr, 6, b"g").unwrap(), 7);
        assert_eq!(s.read_log(&f.a, LogStream::Stderr).unwrap(), b"abcdefg");
    }

    #[test]
    fn an_artifact_arrives_in_parts_and_is_published_whole_under_its_digest() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(0)
        );
        assert!(!s.verify_artifact(a, &d, 10).unwrap());
        assert_eq!(s.append_part(a, &d, 0, &ALPHA[..4]).unwrap(), 4);
        assert!(matches!(
            s.append_part(a, &d, 0, &ALPHA[..4]),
            Err(StoreError::Offset { want: 0, have: 4 })
        ));
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(4)
        );
        assert_eq!(s.append_part(a, &d, 4, &ALPHA[4..]).unwrap(), 10);
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(10)
        );
        assert!(!s.artifact_path(a, &d).exists());

        assert_eq!(s.publish_artifact(a, &d, 10).unwrap(), Publish::Published);
        assert_eq!(fs::read(s.artifact_path(a, &d)).unwrap(), ALPHA);
        assert_eq!(mode(&s.artifact_path(a, &d)), 0o600);
        assert!(!s.part_path(a, &d).exists());
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Published
        );
        assert!(s.verify_artifact(a, &d, 10).unwrap());
        assert!(!s.verify_artifact(a, &d, 9).unwrap());
        // Again changes nothing, and takes a part that came back with it.
        s.append_part(a, &d, 0, b"01").unwrap();
        assert_eq!(s.publish_artifact(a, &d, 10).unwrap(), Publish::Published);
        assert!(!s.part_path(a, &d).exists());
        assert_eq!(fs::read(s.artifact_path(a, &d)).unwrap(), ALPHA);
        s.seal(a).unwrap();

        // An empty artifact has no chunk and is published all the same.
        let none = Digest::of_bytes(b"");
        assert_eq!(s.publish_artifact(a, &none, 0).unwrap(), Publish::Published);
        assert_eq!(fs::read(s.artifact_path(a, &none)).unwrap(), b"");
        assert!(s.verify_artifact(a, &none, 0).unwrap());
    }

    #[test]
    fn bytes_without_their_digest_are_never_published_and_say_who_wrote_them() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        // Written from the first byte by this process: the worker's fault.
        s.append_part(a, &d, 0, b"01234").unwrap();
        s.append_part(a, &d, 5, b"5678X").unwrap();
        assert_eq!(
            s.publish_artifact(a, &d, 10).unwrap(),
            Publish::Mismatch { whole: true }
        );
        assert!(!s.part_path(a, &d).exists() && !s.artifact_path(a, &d).exists());
        // Too short is no better, and neither is the right content of the
        // wrong recorded size.
        s.append_part(a, &d, 0, b"01234").unwrap();
        assert_eq!(
            s.publish_artifact(a, &d, 10).unwrap(),
            Publish::Mismatch { whole: true }
        );
        s.append_part(a, &d, 0, ALPHA).unwrap();
        assert_eq!(
            s.publish_artifact(a, &d, 11).unwrap(),
            Publish::Mismatch { whole: true }
        );
        // More than the recorded size is dropped when it is looked at.
        s.append_part(a, &d, 0, b"0123456789AB").unwrap();
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(0)
        );
        assert!(!s.part_path(a, &d).exists());

        // Begun by a parent that is gone (its tail may be torn): not known
        // to be what the worker served.
        s.append_part(a, &d, 0, b"01234").unwrap();
        drop(s);
        let s = f.open();
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(5)
        );
        s.append_part(a, &d, 5, b"5678X").unwrap();
        assert_eq!(
            s.publish_artifact(a, &d, 10).unwrap(),
            Publish::Mismatch { whole: false }
        );
        assert!(!s.part_path(a, &d).exists());
        // Fetched again from the start it is this process's own.
        s.append_part(a, &d, 0, ALPHA).unwrap();
        assert_eq!(s.publish_artifact(a, &d, 10).unwrap(), Publish::Published);
    }

    #[test]
    fn a_parent_that_dies_between_a_file_operation_and_its_record_continues() {
        let f = fx();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        // After a chunk: the next parent continues at the length.
        let s = f.open();
        s.append_part(a, &d, 0, &ALPHA[..4]).unwrap();
        s.crash_once_at(Crash::ChunkAppended);
        assert!(matches!(
            s.append_part(a, &d, 4, &ALPHA[4..8]),
            Err(StoreError::Crash(Crash::ChunkAppended))
        ));
        drop(s);
        let s = f.open();
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(8)
        );
        s.append_part(a, &d, 8, &ALPHA[8..]).unwrap();
        // After the rename, before the receipt: the next parent finds it
        // published, verifies it again and has nothing to fetch.
        s.crash_once_at(Crash::ArtifactPublished);
        assert!(matches!(
            s.publish_artifact(a, &d, 10),
            Err(StoreError::Crash(Crash::ArtifactPublished))
        ));
        drop(s);
        let s = f.open();
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Published
        );
        assert_eq!(s.publish_artifact(a, &d, 10).unwrap(), Publish::Published);
        assert!(s.verify_artifact(a, &d, 10).unwrap());
    }

    #[test]
    fn a_published_file_that_was_damaged_is_removed_and_fetched_again() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        s.append_part(a, &d, 0, ALPHA).unwrap();
        s.publish_artifact(a, &d, 10).unwrap();
        fs::write(s.artifact_path(a, &d), b"0123456789 and more").unwrap();
        assert!(!s.verify_artifact(a, &d, 10).unwrap());
        assert_eq!(
            s.artifact_state(a, &d, 10).unwrap(),
            ArtifactState::Partial(0)
        );
        assert!(!s.artifact_path(a, &d).exists());
        s.append_part(a, &d, 0, ALPHA).unwrap();
        assert_eq!(s.publish_artifact(a, &d, 10).unwrap(), Publish::Published);
        // A link where the file goes is not followed.
        let other = Digest::of_bytes(b"other");
        std::os::unix::fs::symlink(s.artifact_path(a, &d), s.artifact_path(a, &other)).unwrap();
        assert!(matches!(
            s.verify_artifact(a, &other, 10),
            Err(StoreError::Io(_))
        ));
        assert!(matches!(
            s.artifact_state(a, &other, 10),
            Err(StoreError::Io(_))
        ));
        fs::remove_file(s.artifact_path(a, &other)).unwrap();
        std::os::unix::fs::symlink(s.artifact_path(a, &d), s.part_path(a, &other)).unwrap();
        assert!(matches!(
            s.append_part(a, &other, 10, b"x"),
            Err(StoreError::Io(_))
        ));
        assert_eq!(fs::read(s.artifact_path(a, &d)).unwrap(), ALPHA);
    }

    #[test]
    fn what_unfinished_transfers_left_is_cleared_and_forgotten() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let d = Digest::of_bytes(ALPHA);
        s.clear_tmp(a).unwrap();
        s.write_result(a, b"{}").unwrap();
        s.append_part(a, &d, 0, b"0123X").unwrap();
        s.clear_tmp(a).unwrap();
        s.clear_tmp(a).unwrap();
        assert_eq!(names(&s.root().join(a.as_str())), vec!["result.json"]);
        // The part that is begun now is a new one.
        s.append_part(a, &d, 0, b"X").unwrap();
        assert_eq!(
            s.publish_artifact(a, &d, 10).unwrap(),
            Publish::Mismatch { whole: true }
        );
    }

    #[test]
    fn a_locator_is_removed_with_the_directories_it_leaves_empty() {
        let f = fx();
        let s = f.open();
        let a = &f.a;
        let (d, e) = (Digest::of_bytes(ALPHA), Digest::of_bytes(b"other"));
        s.write_result(a, b"{}").unwrap();
        s.append_log(a, LogStream::Stdout, 0, b"out").unwrap();
        s.append_log(a, LogStream::Stderr, 0, b"err").unwrap();
        s.append_part(a, &d, 0, ALPHA).unwrap();
        s.publish_artifact(a, &d, 10).unwrap();
        // A transfer that never finished.
        s.append_part(a, &e, 0, b"oth").unwrap();
        let dir = s.root().join(a.as_str());

        assert!(matches!(s.remove("blobs/x"), Err(StoreError::BadName(_))));
        assert!(matches!(
            s.remove("results/../x"),
            Err(StoreError::BadName(_))
        ));
        s.remove(&ResultStore::artifact_locator(a, &d)).unwrap();
        assert_eq!(names(&dir), vec!["logs", "result.json", "tmp"]);
        s.remove(&ResultStore::artifact_locator(a, &d)).unwrap();
        s.remove(&ResultStore::logs_locator(a)).unwrap();
        assert_eq!(names(&dir), vec!["result.json", "tmp"]);
        s.remove(&ResultStore::result_locator(a)).unwrap();
        assert!(!dir.exists(), "{:?}", names(&dir));
        assert!(s.root().is_dir());
        s.remove(&ResultStore::result_locator(a)).unwrap();
        assert!(s.root().is_dir());
    }
}
