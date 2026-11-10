//! Carrying a command's output into the log spool.
//!
//! task-traffic-controller starts the command and can only append its stdout
//! and stderr to a path, so the path it is given is a FIFO the supervisor
//! made and reads: the bytes never rest in a file of their own, and what the
//! spool drops at its limit is dropped, not kept somewhere else. One thread
//! per running command reads the FIFO and appends to the spool. It never
//! stops reading while the command may still write (also after the spool
//! failed or reached its limit), so the command is never blocked on its
//! output by anything but the local disk.
//!
//! Secret values are replaced before the bytes reach the spool
//! ([`StreamMasker`]): a value cut by a read is still found, and what the
//! masker holds back is stored when the command is over.
//!
//! Both of the command's streams arrive through the one descriptor, so they
//! are stored as one stream, in the order they were written.

use super::spool::StreamWriter;
use crate::ids::AttemptId;
use crate::secrets::mask::StreamMasker;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const IDLE: Duration = Duration::from_millis(5);
/// How long the last drain may go on once the command is known to be over: a
/// process that escaped the stop and keeps writing must not hold the
/// supervisor.
const LAST_DRAIN: Duration = Duration::from_secs(2);
const CHUNK: usize = 64 * 1024;

/// The reading end of a command's output pipe. The FIFO is removed when this
/// is dropped: it is the supervisor's own, made for one command.
#[derive(Debug)]
pub(crate) struct Pipe {
    file: File,
    path: PathBuf,
}

impl Pipe {
    /// Where the FIFO of node `index` of `attempt` lives: beside its spool.
    pub fn path_for(root: &Path, attempt: &AttemptId, index: usize) -> PathBuf {
        super::spool::logs_dir(root, attempt).join(format!("node-{index}.pipe"))
    }

    /// Make the FIFO (one that a crash left is used again; anything else at
    /// the path is refused) and open its reading end without waiting for a
    /// writer. The directory exists: the spool was opened first.
    pub fn open(path: &Path) -> io::Result<Self> {
        let c = CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: `c` is a valid NUL-terminated path for the call's duration.
        if unsafe { libc::mkfifo(c.as_ptr(), 0o600) } != 0 {
            let e = io::Error::last_os_error();
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(e);
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(path)?;
        if !file.metadata()?.file_type().is_fifo() {
            return Err(io::Error::other(format!(
                "{} is not a FIFO",
                path.display()
            )));
        }
        Ok(Pipe {
            file,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Read `pipe` into `out` until `done` is set and the pipe is empty (or
/// [`LAST_DRAIN`] has passed since). The first failure to store goes to
/// `error`, where the supervisor's heartbeat finds it; reading goes on.
/// Everything passes through `mask` on its way to `out`.
pub(crate) fn pump(
    pipe: &Pipe,
    out: &StreamWriter,
    mut mask: StreamMasker,
    done: &AtomicBool,
    error: &Mutex<Option<String>>,
) {
    let mut buf = vec![0u8; CHUNK];
    let mut failed = false;
    let fail = |why: String| {
        error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(why);
    };
    let mut store = |bytes: Vec<u8>| {
        if !failed {
            if let Err(e) = out.append(&bytes) {
                failed = true;
                fail(e.to_string());
            }
        }
    };
    let mut deadline: Option<Instant> = None;
    loop {
        // Looked at before the read: after `done`, one more read is always
        // made, so nothing written before the command ended is left behind.
        let finishing = done.load(Ordering::SeqCst);
        if finishing && deadline.is_none() {
            deadline = Some(Instant::now() + LAST_DRAIN);
        }
        match (&pipe.file).read(&mut buf) {
            Ok(n) if n > 0 => {
                store(mask.push(&buf[..n]));
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
            }
            // No writer (yet, or any more), or nothing to read right now.
            Ok(_) => {
                if finishing {
                    break;
                }
                std::thread::sleep(IDLE);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                if finishing {
                    break;
                }
                std::thread::sleep(IDLE);
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => {
                fail(format!("reading the command's output: {e}"));
                break;
            }
        }
    }
    // The end of this command's output: what was held back for a secret
    // that might have continued.
    store(mask.finish());
}
