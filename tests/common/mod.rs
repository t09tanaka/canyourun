//! Shared by the integration tests.

#![allow(dead_code)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// A temporary directory that can be removed even though the content-addressed
/// stores under it are read-only by design (`tempfile` would leak it).
pub struct Tmp(tempfile::TempDir);

pub fn tempdir() -> std::io::Result<Tmp> {
    tempfile::tempdir().map(Tmp)
}

impl Tmp {
    pub fn path(&self) -> &Path {
        self.0.path()
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let mut todo = vec![self.0.path().to_path_buf()];
        while let Some(dir) = todo.pop() {
            let Ok(meta) = std::fs::symlink_metadata(&dir) else {
                continue;
            };
            if !meta.is_dir() {
                continue;
            }
            let mut mode = meta.permissions();
            mode.set_mode(mode.mode() | 0o700);
            let _ = std::fs::set_permissions(&dir, mode);
            if let Ok(rd) = std::fs::read_dir(&dir) {
                todo.extend(rd.flatten().map(|e| e.path()));
            }
        }
    }
}
