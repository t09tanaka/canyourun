//! Laying a materialized source over a fresh worktree, without following a
//! symlink anywhere and without touching what already matches (so the
//! worktree stays clean for `fast-worktree remove`).

use anyhow::{bail, Context, Result};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// Copy everything under `src` onto `dest`. A symlink in `dest` is never
/// followed: where the source has a directory, a symlink there is an error.
pub(super) fn overlay(src: &Path, dest: &Path) -> Result<()> {
    for ent in fs::read_dir(src).with_context(|| format!("cannot read {}", src.display()))? {
        let ent = ent?;
        let from = ent.path();
        let to = dest.join(ent.file_name());
        let meta = fs::symlink_metadata(&from)?;
        let existing = fs::symlink_metadata(&to).ok();
        if meta.is_dir() {
            match existing {
                None => fs::create_dir(&to)?,
                Some(m) if m.is_dir() => {}
                Some(_) => bail!("{} is not a directory in the worktree", to.display()),
            }
            overlay(&from, &to)?;
        } else if meta.file_type().is_symlink() {
            let target = fs::read_link(&from)?;
            if let Some(m) = existing {
                if m.file_type().is_symlink() && fs::read_link(&to)? == target {
                    continue;
                }
                remove_leaf(&to, &m)?;
            }
            std::os::unix::fs::symlink(&target, &to)?;
        } else if meta.is_file() {
            let bytes = fs::read(&from)?;
            let mode = meta.permissions().mode() & 0o777;
            if let Some(m) = existing {
                if m.is_file() && m.permissions().mode() & 0o777 == mode && fs::read(&to)? == bytes
                {
                    continue;
                }
                remove_leaf(&to, &m)?;
            }
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(mode)
                .open(&to)?;
            f.write_all(&bytes)?;
            drop(f);
            fs::set_permissions(&to, fs::Permissions::from_mode(mode))?;
        } else {
            bail!("{} is not a file, directory or symlink", from.display());
        }
    }
    Ok(())
}

fn remove_leaf(path: &Path, meta: &fs::Metadata) -> Result<()> {
    if meta.is_dir() {
        bail!("{} is a directory in the worktree", path.display());
    }
    fs::remove_file(path)?;
    Ok(())
}
