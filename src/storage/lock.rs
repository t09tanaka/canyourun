//! The coordinator's process lock: one coordinator per state directory.

use super::{Result, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

/// An exclusive advisory lock (`flock`) on a file in the state directory. It
/// is released when this value is dropped or the process dies, so a crashed
/// coordinator never leaves a stale lock behind.
#[derive(Debug)]
pub struct ProcessLock {
    _file: File,
    path: PathBuf,
}

impl ProcessLock {
    /// Take the lock, creating the directory and the file if needed. A second
    /// holder (in this or another process) fails with [`StorageError::Locked`].
    pub fn acquire(dir: &Path, name: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(StorageError::Locked(path)),
            Err(TryLockError::Error(e)) => Err(e.into()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_fails_until_the_first_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let first = ProcessLock::acquire(dir.path(), "x.lock").unwrap();
        let err = ProcessLock::acquire(dir.path(), "x.lock").unwrap_err();
        assert!(matches!(err, StorageError::Locked(_)), "{err:?}");
        assert_eq!(err.code(), crate::error::ErrorCode::Conflict);
        // A different name is a different lock.
        ProcessLock::acquire(dir.path(), "y.lock").unwrap();
        assert!(first.path().ends_with("x.lock"));
        drop(first);
        ProcessLock::acquire(dir.path(), "x.lock").unwrap();
    }
}
