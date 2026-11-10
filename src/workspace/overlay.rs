//! Laying a materialized source over a fresh worktree, without following a
//! symlink anywhere and without touching what already matches (so the
//! worktree stays clean for `fast-worktree remove`).
//!
//! What is reproduced: file contents, the executable bits (the lower nine
//! mode bits), symlinks. What is not: directories' modes and empty
//! directories (Git records neither), ownership and times.

use anyhow::{bail, Context, Result};
use std::fs;
use std::io::Read;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// Copy everything under `src` onto `dest`. A symlink in `dest` is never
/// followed: where the source has a directory, a symlink there is an error,
/// and so is a directory where the source has a file or a link (a whole tree
/// is never deleted to make room).
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
            let mode = meta.permissions().mode() & 0o777;
            if let Some(m) = existing {
                if m.is_file()
                    && m.permissions().mode() & 0o777 == mode
                    && m.len() == meta.len()
                    && same_content(&from, &to)?
                {
                    continue;
                }
                remove_leaf(&to, &m)?;
            }
            let mut input = fs::File::open(&from)?;
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(mode)
                .open(&to)?;
            std::io::copy(&mut input, &mut f)?;
            drop(f);
            fs::set_permissions(&to, fs::Permissions::from_mode(mode))?;
        } else {
            bail!("{} is not a file, directory or symlink", from.display());
        }
    }
    Ok(())
}

/// Equal bytes, compared a chunk at a time (a file is never held whole).
fn same_content(a: &Path, b: &Path) -> Result<bool> {
    let (mut fa, mut fb) = (fs::File::open(a)?, fs::File::open(b)?);
    let (mut ba, mut bb) = (vec![0u8; 64 * 1024], vec![0u8; 64 * 1024]);
    loop {
        let n = read_full(&mut fa, &mut ba)?;
        let m = read_full(&mut fb, &mut bb)?;
        if n != m || ba[..n] != bb[..m] {
            return Ok(false);
        }
        if n == 0 {
            return Ok(true);
        }
    }
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut at = 0;
    while at < buf.len() {
        match r.read(&mut buf[at..])? {
            0 => break,
            n => at += n,
        }
    }
    Ok(at)
}

fn remove_leaf(path: &Path, meta: &fs::Metadata) -> Result<()> {
    if meta.is_dir() {
        bail!("{} is a directory in the worktree", path.display());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn dirs() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let t = tempfile::tempdir().unwrap();
        let (s, d) = (t.path().join("src"), t.path().join("dest"));
        fs::create_dir(&s).unwrap();
        fs::create_dir(&d).unwrap();
        (t, s, d)
    }

    fn mode(p: &Path) -> u32 {
        fs::symlink_metadata(p).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn new_files_and_directories_arrive_with_their_modes() {
        let (_t, s, d) = dirs();
        fs::create_dir_all(s.join("a/b")).unwrap();
        fs::write(s.join("a/b/x.sh"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(s.join("a/b/x.sh"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(s.join("y"), "y").unwrap();
        symlink("y", s.join("l")).unwrap();
        overlay(&s, &d).unwrap();
        assert_eq!(
            fs::read_to_string(d.join("a/b/x.sh")).unwrap(),
            "#!/bin/sh\n"
        );
        assert_eq!(mode(&d.join("a/b/x.sh")), 0o755);
        assert_eq!(mode(&d.join("y")), 0o644);
        assert_eq!(fs::read_link(d.join("l")).unwrap(), Path::new("y"));
    }

    #[test]
    fn a_mode_change_is_applied_and_an_equal_file_is_left_alone() {
        let (_t, s, d) = dirs();
        fs::write(s.join("f"), "same").unwrap();
        fs::set_permissions(s.join("f"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(d.join("f"), "same").unwrap();
        fs::set_permissions(d.join("f"), fs::Permissions::from_mode(0o644)).unwrap();
        overlay(&s, &d).unwrap();
        assert_eq!(mode(&d.join("f")), 0o755);
        // equal in content and mode: not rewritten (same inode)
        use std::os::unix::fs::MetadataExt;
        let ino = fs::metadata(d.join("f")).unwrap().ino();
        overlay(&s, &d).unwrap();
        assert_eq!(fs::metadata(d.join("f")).unwrap().ino(), ino);
    }

    #[test]
    fn different_content_is_replaced_including_a_tail_difference() {
        let (_t, s, d) = dirs();
        let mut big = vec![7u8; 200_000];
        fs::write(d.join("big"), &big).unwrap();
        *big.last_mut().unwrap() = 8;
        fs::write(s.join("big"), &big).unwrap();
        fs::write(s.join("short"), "ab").unwrap();
        fs::write(d.join("short"), "abc").unwrap();
        fs::write(s.join("diff"), "one").unwrap();
        fs::write(d.join("diff"), "two").unwrap();
        overlay(&s, &d).unwrap();
        assert_eq!(fs::read(d.join("big")).unwrap(), big);
        assert_eq!(fs::read(d.join("short")).unwrap(), b"ab");
        assert_eq!(fs::read(d.join("diff")).unwrap(), b"one");
    }

    #[test]
    fn a_symlink_replaces_a_file_and_a_different_link_and_vice_versa() {
        let (_t, s, d) = dirs();
        symlink("t1", s.join("a")).unwrap();
        fs::write(d.join("a"), "was a file").unwrap();
        symlink("t2", s.join("b")).unwrap();
        symlink("old", d.join("b")).unwrap();
        symlink("same", s.join("c")).unwrap();
        symlink("same", d.join("c")).unwrap();
        fs::write(s.join("e"), "now a file").unwrap();
        symlink("/somewhere", d.join("e")).unwrap();
        overlay(&s, &d).unwrap();
        assert_eq!(fs::read_link(d.join("a")).unwrap(), Path::new("t1"));
        assert_eq!(fs::read_link(d.join("b")).unwrap(), Path::new("t2"));
        assert_eq!(fs::read_link(d.join("c")).unwrap(), Path::new("same"));
        let m = fs::symlink_metadata(d.join("e")).unwrap();
        assert!(m.is_file());
        assert_eq!(fs::read_to_string(d.join("e")).unwrap(), "now a file");
    }

    #[test]
    fn writing_never_goes_through_a_symlink() {
        let (t, s, d) = dirs();
        let outside = t.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("victim"), "keep").unwrap();
        // the source wants a file where the worktree has a link to a file
        fs::write(s.join("link"), "new").unwrap();
        symlink(outside.join("victim"), d.join("link")).unwrap();
        overlay(&s, &d).unwrap();
        assert_eq!(fs::read_to_string(outside.join("victim")).unwrap(), "keep");
        assert!(fs::symlink_metadata(d.join("link")).unwrap().is_file());
        // and a link to a directory is not entered
        fs::create_dir(s.join("dir")).unwrap();
        fs::write(s.join("dir/f"), "x").unwrap();
        symlink(&outside, d.join("dir")).unwrap();
        let e = overlay(&s, &d).unwrap_err();
        assert!(e.to_string().contains("is not a directory"), "{e}");
        assert!(!outside.join("f").exists());
    }

    #[test]
    fn a_leaf_type_clash_with_a_directory_is_an_error() {
        // the source has a file or a link, the worktree a directory
        let (_t, s, d) = dirs();
        fs::write(s.join("n"), "file").unwrap();
        fs::create_dir(d.join("n")).unwrap();
        fs::write(d.join("n/keep"), "k").unwrap();
        let e = overlay(&s, &d).unwrap_err();
        assert!(e.to_string().contains("is a directory"), "{e}");
        assert!(d.join("n/keep").exists());
        fs::remove_file(s.join("n")).unwrap();
        symlink("x", s.join("n")).unwrap();
        let e = overlay(&s, &d).unwrap_err();
        assert!(e.to_string().contains("is a directory"), "{e}");
        // the source has a directory, the worktree a file
        let (_t2, s2, d2) = dirs();
        fs::create_dir(s2.join("n")).unwrap();
        fs::write(d2.join("n"), "file").unwrap();
        let e = overlay(&s2, &d2).unwrap_err();
        assert!(e.to_string().contains("is not a directory"), "{e}");
        assert_eq!(fs::read_to_string(d2.join("n")).unwrap(), "file");
    }

    #[test]
    fn something_that_is_not_a_file_is_refused() {
        let (_t, s, d) = dirs();
        let st = std::process::Command::new("mkfifo")
            .arg(s.join("pipe"))
            .status()
            .unwrap();
        assert!(st.success());
        let e = overlay(&s, &d).unwrap_err();
        assert!(
            e.to_string().contains("not a file, directory or symlink"),
            "{e}"
        );
        let e = overlay(&s.join("missing"), &d).unwrap_err();
        assert!(e.to_string().contains("cannot read"), "{e}");
    }
}
