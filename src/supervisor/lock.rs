//! One supervisor per attempt: an exclusive lock held for the supervisor's
//! whole life. The operating system drops it when the process dies, however
//! it dies, so a lock that can be taken proves that the supervisor is gone
//! (recovery relies on it before it stops anything).

use super::error::Result;
use crate::ids::AttemptId;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SupervisorLock {
    _file: File,
}

pub fn lock_path(state_dir: &Path, attempt: &AttemptId) -> PathBuf {
    state_dir
        .join("supervisors")
        .join(format!("{attempt}.lock"))
}

impl SupervisorLock {
    /// Take the lock, or `None` if a live supervisor holds it.
    pub fn try_acquire(state_dir: &Path, attempt: &AttemptId) -> Result<Option<SupervisorLock>> {
        let path = lock_path(state_dir, attempt);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        // SAFETY: the fd is valid for the call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(Some(SupervisorLock { _file: file }));
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(None)
        } else {
            Err(err.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let a = AttemptId::generate();
        let first = SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .unwrap();
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_none());
        // Another attempt is independent.
        let b = AttemptId::generate();
        assert!(SupervisorLock::try_acquire(dir.path(), &b)
            .unwrap()
            .is_some());
        drop(first);
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_some());
    }
}
