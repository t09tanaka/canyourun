//! The log spool: stdout and stderr of an attempt on the worker's disk.
//!
//! One supervisor writes ([`AttemptSpool`]); the worker agent, a different
//! process, reads ([`SpoolReader`]). Layout under the spool root:
//! `<attempt>/logs/{stdout,stderr}.{log,idx}`, `writer.lock`, and `closed`
//! once the attempt's output is complete. See [`super::spool_plan`] for the
//! file formats.
//!
//! Offsets. A stream's bytes are stored contiguously from offset 0 up to the
//! limit (both streams share it). The spec's limit is per homework, so the
//! limit given to [`AttemptSpool::open`] is the homework's limit less what its
//! other attempts stored ([`super::spool_plan::attempt_limit`]); the caller
//! knows the attempts and sums their `LogPosition`s in the worker database.
//! Past it, bytes are dropped, not
//! stored: the offset space does not move, `end` stays where the limit cut it
//! and `dropped_bytes` counts what was lost, so a reader at `end` that sees
//! `log_truncated` knows nothing more will come there. The sequence of a
//! stream is its number of index records; with the offset it is what the
//! protocol's cursors dedupe on. A record is written when
//! [`MIN_RECORD_BYTES`](super::spool_plan::MIN_RECORD_BYTES) of stored bytes
//! wait for one, when drops are worth recording, and on `sync` and `finish`, so
//! the index stays small whatever the size of the producer's writes. The log
//! bytes themselves are written at once; a reader sees them when their record
//! is written, so a small tail becomes visible at the next `sync` (the
//! supervisor's heartbeat) at the latest.
//!
//! Durability (the fsync policy). An append is `write(2)` of the bytes, then of
//! one index record, both through the kernel: a crash of the supervisor
//! process (the case that matters, as it is a separate process) loses nothing
//! that was appended, and the agent sees the bytes at once. `fsync` of the log
//! and then the index happens every [`SYNC_BYTES`] of stored data, on
//! [`StreamWriter::sync`] (the supervisor calls it on its heartbeat) and always
//! at [`AttemptSpool::finish`]. Only an OS crash or power loss can lose the
//! bytes after the last sync; recovery then keeps the valid prefix of the
//! index and never reuses an offset for different bytes on this machine. A
//! reader that had already seen offsets past the last sync gets a smaller
//! `end` after such a crash, which the protocol reports as ordinary
//! resumption, not as a gap.
//!
//! Resuming after a crash ([`AttemptSpool::open`]) cuts the index to its valid
//! prefix, adds a record for log bytes written after the last record, and
//! continues from the log's real length: the offset never goes back and no
//! byte is stored twice. Bytes the producer wrote while no supervisor was
//! alive are not in the spool, and their count is not known.

use super::spool_plan::{
    admit, read_window, record_needed, IndexScan, Mark, Recovery, MAX_READ_BYTES, RECORD_LEN,
};
use crate::ids::AttemptId;
use crate::storage::LogPosition;
use crate::wire::LogStream;
use std::fs::{DirBuilder, File, OpenOptions, TryLockError};
use std::io::{self, BufReader, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Stored bytes appended between two fsyncs.
pub const SYNC_BYTES: u64 = 1 << 20;

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("spool io: {0}")]
    Io(#[from] io::Error),
    /// Another supervisor is writing this attempt's spool.
    #[error("another process writes the spool at {}", .0.display())]
    Locked(PathBuf),
    /// The output is complete; nothing more can be appended.
    #[error("the spool is finished")]
    Finished,
    /// An earlier write failed (disk full, ...). Nothing was hidden: the
    /// caller saw the error; later appends, `sync` and `finish` are refused so
    /// the index never disagrees with the log. The spool's output is then
    /// incomplete, and what that means for the attempt is S9a's decision (the
    /// supervisor), not the spool's: see `OutcomeError::SpoolFailed`.
    #[error("an earlier spool write failed")]
    Failed,
    #[error("no spool at {}", .0.display())]
    NotFound(PathBuf),
}

pub type Result<T, E = SpoolError> = std::result::Result<T, E>;

/// Where one stream stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamStatus {
    /// Bytes stored, from offset 0.
    pub end: u64,
    /// Bytes that did not fit under the limit (`log_truncated` when > 0).
    pub dropped: u64,
    /// Sequence number of the last append.
    pub seq: u64,
}

impl StreamStatus {
    pub fn truncated(&self) -> bool {
        self.dropped > 0
    }
}

impl From<StreamStatus> for LogPosition {
    fn from(s: StreamStatus) -> Self {
        LogPosition {
            written_bytes: s.end,
            dropped_bytes: s.dropped,
            last_seq: s.seq,
        }
    }
}

fn stem(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

pub fn logs_dir(root: &Path, attempt: &AttemptId) -> PathBuf {
    root.join(attempt.as_str()).join("logs")
}

fn open_rw(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .mode(0o600)
        .open(path)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// The limit is shared by the attempt's two streams, which are written by two
/// threads.
#[derive(Debug)]
struct Budget {
    limit: u64,
    used: AtomicU64,
}

impl Budget {
    fn reserve(&self, len: u64) -> super::spool_plan::Admission {
        let prev = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |u| {
                Some(u + admit(u, self.limit, len).store)
            })
            .expect("closure always returns Some");
        admit(prev, self.limit, len)
    }
}

#[derive(Debug)]
struct Inner {
    data: File,
    idx: File,
    /// The last record written (what a reader can see).
    mark: Mark,
    /// Bytes in the log file, which may be ahead of `mark.end` by less than
    /// [`MIN_RECORD_BYTES`](super::spool_plan::MIN_RECORD_BYTES).
    data_end: u64,
    /// Dropped bytes counted so far (the record may lag by a batch).
    dropped: u64,
    seq: u64,
    unsynced: u64,
    failed: bool,
    finished: bool,
}

/// The writing end of one stream.
#[derive(Debug)]
pub struct StreamWriter {
    inner: Mutex<Inner>,
    budget: Arc<Budget>,
}

impl StreamWriter {
    /// Store as much of `bytes` as the limit allows and count the rest as
    /// dropped. An `Err` means the disk refused a write; the writer is then
    /// failed and the caller must report it, not carry on as if stored.
    pub fn append(&self, bytes: &[u8]) -> Result<StreamStatus> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.failed {
            return Err(SpoolError::Failed);
        }
        if g.finished {
            return Err(SpoolError::Finished);
        }
        if bytes.is_empty() {
            return Ok(status_of(&g));
        }
        let adm = self.budget.reserve(bytes.len() as u64);
        let r = (|| -> io::Result<()> {
            let store = adm.store as usize;
            g.data.write_all(&bytes[..store])?;
            g.dropped += adm.drop;
            g.data_end += adm.store;
            if record_needed(g.data_end - g.mark.end, g.mark.dropped, g.dropped) {
                write_record(&mut g)?;
            }
            g.unsynced += adm.store;
            if g.unsynced >= SYNC_BYTES {
                sync_inner(&mut g)?;
            }
            Ok(())
        })();
        match r {
            Ok(()) => Ok(status_of(&g)),
            Err(e) => {
                g.failed = true;
                Err(e.into())
            }
        }
    }

    /// Publish everything appended so far to readers (the pending index
    /// record) and make it durable (log first, then index). The supervisor
    /// calls this on its heartbeat, which bounds how long a small tail stays
    /// invisible to the agent.
    pub fn sync(&self) -> Result<()> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.failed {
            return Err(SpoolError::Failed);
        }
        write_record_if_pending(&mut g)
            .and_then(|()| sync_inner(&mut g))
            .map_err(|e| {
                g.failed = true;
                e.into()
            })
    }

    /// Test hook: behave as after a failed disk write.
    #[cfg(test)]
    pub(super) fn poison(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).failed = true;
    }

    pub fn status(&self) -> StreamStatus {
        status_of(&self.inner.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn finish(&self) -> Result<StreamStatus> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.failed {
            return Err(SpoolError::Failed);
        }
        let r = (|| -> io::Result<()> {
            write_record_if_pending(&mut g)?;
            sync_inner(&mut g)
        })();
        if let Err(e) = r {
            g.failed = true;
            return Err(e.into());
        }
        g.finished = true;
        Ok(status_of(&g))
    }
}

/// Publish the log's current end and the drop count as one index record.
fn write_record(g: &mut Inner) -> io::Result<()> {
    let m = Mark {
        end: g.data_end,
        dropped: g.dropped,
    };
    g.idx.write_all(&m.encode())?;
    g.mark = m;
    g.seq += 1;
    Ok(())
}

fn write_record_if_pending(g: &mut Inner) -> io::Result<()> {
    if g.data_end != g.mark.end || g.dropped != g.mark.dropped {
        write_record(g)?;
    }
    Ok(())
}

fn status_of(g: &Inner) -> StreamStatus {
    StreamStatus {
        end: g.data_end,
        dropped: g.dropped,
        seq: g.seq,
    }
}

fn sync_inner(g: &mut Inner) -> io::Result<()> {
    g.data.sync_data()?;
    g.idx.sync_data()?;
    g.unsynced = 0;
    Ok(())
}

/// Open one stream for writing, recovering what a crash left.
fn open_stream(dir: &Path, stream: LogStream, budget: &Arc<Budget>) -> io::Result<StreamWriter> {
    let data = open_rw(&dir.join(format!("{}.log", stem(stream))))?;
    let idx = open_rw(&dir.join(format!("{}.idx", stem(stream))))?;
    let data_len = data.metadata()?.len();
    let mut scan = IndexScan::new(data_len);
    {
        let mut r = BufReader::new(&idx);
        let mut rec = [0u8; RECORD_LEN];
        while r.read_exact(&mut rec).is_ok() && scan.feed(&rec) {}
    }
    let Recovery {
        valid_records,
        append,
        mark,
        seq,
    } = scan.finish();
    let mut repaired = false;
    if idx.metadata()?.len() != valid_records * RECORD_LEN as u64 {
        idx.set_len(valid_records * RECORD_LEN as u64)?;
        repaired = true;
    }
    if let Some(m) = append {
        (&idx).write_all(&m.encode())?;
        repaired = true;
    }
    let mut inner = Inner {
        data,
        idx,
        mark,
        data_end: mark.end,
        dropped: mark.dropped,
        seq,
        unsynced: 0,
        failed: false,
        finished: false,
    };
    if repaired {
        sync_inner(&mut inner)?;
    }
    budget.used.fetch_add(mark.end, Ordering::SeqCst);
    Ok(StreamWriter {
        inner: Mutex::new(inner),
        budget: Arc::clone(budget),
    })
}

/// The writing end of an attempt's spool: both streams, one shared limit.
#[derive(Debug)]
pub struct AttemptSpool {
    dir: PathBuf,
    stdout: StreamWriter,
    stderr: StreamWriter,
    _lock: File,
}

impl AttemptSpool {
    /// Create the spool of `attempt` under `root`, or resume it after a crash.
    /// `limit` is the cap on bytes this attempt may store (both streams
    /// together): the homework limit less its other attempts' bytes.
    /// A second writer, in this or another process, is [`SpoolError::Locked`];
    /// a spool already finished is [`SpoolError::Finished`].
    pub fn open(root: &Path, attempt: &AttemptId, limit: u64) -> Result<Self> {
        let dir = logs_dir(root, attempt);
        DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
        let lock = open_rw(&dir.join("writer.lock"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(SpoolError::Locked(dir)),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        if dir.join("closed").exists() {
            return Err(SpoolError::Finished);
        }
        let budget = Arc::new(Budget {
            limit,
            used: AtomicU64::new(0),
        });
        let stdout = open_stream(&dir, LogStream::Stdout, &budget)?;
        let stderr = open_stream(&dir, LogStream::Stderr, &budget)?;
        sync_dir(&dir)?;
        if let Some(parent) = dir.parent() {
            sync_dir(parent)?;
        }
        Ok(AttemptSpool {
            dir,
            stdout,
            stderr,
            _lock: lock,
        })
    }

    pub fn writer(&self, stream: LogStream) -> &StreamWriter {
        match stream {
            LogStream::Stdout => &self.stdout,
            LogStream::Stderr => &self.stderr,
        }
    }

    /// The output is complete: write the exact drop counts, fsync, and mark
    /// the spool `closed` so readers can report eof. Idempotent. Returns
    /// `(stdout, stderr)`.
    pub fn finish(&self) -> Result<(StreamStatus, StreamStatus)> {
        let out = self.stdout.finish()?;
        let err = self.stderr.finish()?;
        let marker = self.dir.join("closed");
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(&marker)?;
        f.sync_all()?;
        sync_dir(&self.dir)?;
        Ok((out, err))
    }
}

/// One answer of [`SpoolReader::read`]; the fields of `ReadLogResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadChunk {
    pub offset: u64,
    pub data: Vec<u8>,
    pub next_offset: u64,
    pub eof: bool,
    pub log_truncated: bool,
    pub dropped_bytes: u64,
    pub seq: u64,
}

/// The reading end, used by another process than the writer. It takes no lock
/// and trusts only what the index publishes.
#[derive(Debug, Clone)]
pub struct SpoolReader {
    dir: PathBuf,
}

impl SpoolReader {
    pub fn open(root: &Path, attempt: &AttemptId) -> Result<Self> {
        let dir = logs_dir(root, attempt);
        if !dir.is_dir() {
            return Err(SpoolError::NotFound(dir));
        }
        Ok(SpoolReader { dir })
    }

    /// Whether the supervisor marked the output complete.
    pub fn finished(&self) -> bool {
        self.dir.join("closed").exists()
    }

    /// What the index publishes for `stream`: its last whole record, with
    /// `end` held to the bytes the log file really has.
    pub fn status(&self, stream: LogStream) -> Result<StreamStatus> {
        let idx = File::open(self.dir.join(format!("{}.idx", stem(stream))))?;
        let data = File::open(self.dir.join(format!("{}.log", stem(stream))))?;
        let n = idx.metadata()?.len() / RECORD_LEN as u64;
        if n == 0 {
            return Ok(StreamStatus::default());
        }
        let mut rec = [0u8; RECORD_LEN];
        idx.read_exact_at(&mut rec, (n - 1) * RECORD_LEN as u64)?;
        let m = Mark::decode(&rec);
        Ok(StreamStatus {
            end: m.end.min(data.metadata()?.len()),
            dropped: m.dropped,
            seq: n,
        })
    }

    /// Up to `max_bytes` of `stream` from `offset`. Bytes are returned as
    /// stored (any bytes, not text).
    pub fn read(&self, stream: LogStream, offset: u64, max_bytes: u32) -> Result<ReadChunk> {
        // Ask for completeness first: if it is set, the status read next is final.
        let finished = self.finished();
        let st = self.status(stream)?;
        let (start, len) = read_window(st.end, offset, u64::from(max_bytes));
        debug_assert!(len <= MAX_READ_BYTES);
        let data = File::open(self.dir.join(format!("{}.log", stem(stream))))?;
        let mut buf = vec![0u8; len as usize];
        let mut got = 0usize;
        while got < buf.len() {
            let n = data.read_at(&mut buf[got..], start + got as u64)?;
            if n == 0 {
                break;
            }
            got += n;
        }
        buf.truncate(got);
        let next = start + got as u64;
        Ok(ReadChunk {
            offset: start,
            data: buf,
            next_offset: next,
            eof: finished && next >= st.end,
            log_truncated: st.truncated(),
            dropped_bytes: st.dropped,
            seq: st.seq,
        })
    }
}
