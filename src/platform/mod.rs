//! OS-dependent primitives (spec chapter 15): a monotonic clock that counts
//! suspend, the boot ID, process identity, and sleep inhibition.
//!
//! macOS and Linux each have an implementation behind the same functions, so
//! the supervisor and the worker agent keep one contract on both.

pub mod boot;
pub mod clock;
pub mod inhibit;
pub mod process;
pub mod process_read;

pub use boot::boot_id;
pub use clock::{monotonic_ms, wall_ms};
pub use inhibit::{Inhibit, SleepGuard};
pub use process::{
    process_group_of, process_start_identity, read_process, signal_pid, signal_process_group,
    GroupSignal, ProcessRead, StartIdentity,
};

/// Stop this process from leaving a core dump (and, on Linux, from being
/// attached to by another process of the same user): a dump of a process that
/// holds secret files, or the cluster key, would write them to disk.
/// `RLIMIT_CORE` is set to 0 (soft and hard, so it cannot be raised again) on
/// macOS and Linux; Linux also clears `PR_SET_DUMPABLE`.
///
/// This changes the whole process, so it is for the supervisor and the worker
/// agent to call once at start (S10), not for a library function or a test
/// that shares a process.
pub fn disable_core_dumps() -> std::io::Result<()> {
    use rustix::process::{setrlimit, Resource, Rlimit};
    let zero = Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    setrlimit(Resource::Core, zero)?;
    #[cfg(target_os = "linux")]
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)?;
    Ok(())
}

/// The bytes of the file system that holds `path` an unprivileged process
/// may still write (the same call on macOS and Linux).
pub fn free_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    let s = rustix::fs::statvfs(path)?;
    Ok(s.f_bavail.saturating_mul(s.f_frsize))
}

/// Open the file `rel` below the directory `base` for reading, without
/// following a symbolic link at any step: `base` and every directory of
/// `rel` are opened one by one (`O_DIRECTORY | O_NOFOLLOW`, each relative to
/// the one before), so what is opened is what was looked at, whatever is
/// renamed meanwhile. `rel` must be made of plain names.
pub fn open_beneath(
    base: &std::path::Path,
    rel: &std::path::Path,
) -> std::io::Result<std::fs::File> {
    use rustix::fs::{open, openat, Mode, OFlags};
    use std::path::Component;
    let mut names = vec![];
    for c in rel.components() {
        match c {
            Component::Normal(n) => names.push(n),
            _ => return Err(std::io::ErrorKind::InvalidInput.into()),
        }
    }
    let Some((last, dirs)) = names.split_last() else {
        return Err(std::io::ErrorKind::InvalidInput.into());
    };
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut cur = open(base, dir_flags, Mode::empty())?;
    for d in dirs {
        cur = openat(&cur, *d, dir_flags, Mode::empty())?;
    }
    // non-blocking: a FIFO under that name must not hold the caller
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
    Ok(std::fs::File::from(openat(
        &cur,
        *last,
        flags,
        Mode::empty(),
    )?))
}

/// The soft and hard `RLIMIT_CORE` of this process.
pub fn core_limit() -> (Option<u64>, Option<u64>) {
    let l = rustix::process::getrlimit(rustix::process::Resource::Core);
    (l.current, l.maximum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn the_free_space_of_a_disk_is_read() {
        let dir = tempfile::tempdir().unwrap();
        assert!(free_bytes(dir.path()).unwrap() > 0);
        assert!(free_bytes(&dir.path().join("nothing")).is_err());
    }

    #[test]
    fn a_file_is_opened_beneath_a_directory_through_no_link() {
        use std::io::Read;
        use std::os::unix::fs::symlink;
        use std::path::Path;
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("base");
        std::fs::create_dir_all(base.join("a/b")).unwrap();
        std::fs::write(base.join("a/b/f.txt"), "inside").unwrap();
        std::fs::write(base.join("top.txt"), "top").unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(outside.join("b")).unwrap();
        std::fs::write(outside.join("b/f.txt"), "outside").unwrap();
        std::fs::write(outside.join("f.txt"), "outside").unwrap();
        let read = |base: &Path, rel: &str| {
            open_beneath(base, Path::new(rel)).map(|mut f| {
                let mut s = String::new();
                f.read_to_string(&mut s).unwrap();
                s
            })
        };
        assert_eq!(read(&base, "a/b/f.txt").unwrap(), "inside");
        assert_eq!(read(&base, "top.txt").unwrap(), "top");
        assert_eq!(
            read(&base, "a/none.txt").unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        // not a plain relative path
        for rel in [
            "",
            "/etc/hosts",
            "../outside/f.txt",
            "a/../top.txt",
            "./top.txt",
        ] {
            assert!(read(&base, rel).is_err(), "{rel}");
        }
        // a link in the place of the file, of a directory on the way, of the
        // first directory, and of the base itself
        symlink(outside.join("f.txt"), base.join("a/b/link.txt")).unwrap();
        assert!(read(&base, "a/b/link.txt").is_err());
        symlink(outside.join("b"), base.join("a/l")).unwrap();
        assert!(read(&base, "a/l/f.txt").is_err());
        symlink(&outside, base.join("o")).unwrap();
        assert!(read(&base, "o/b/f.txt").is_err());
        assert!(read(&base, "o/f.txt").is_err());
        let linked = tmp.path().join("linked");
        symlink(&base, &linked).unwrap();
        assert!(read(&linked, "top.txt").is_err());
    }

    /// Runs in a child process only (it changes the process): see
    /// `core_dumps_are_disabled_for_the_process_that_asks`.
    #[test]
    #[ignore = "runs in a child process"]
    fn child_disables_core_dumps() {
        disable_core_dumps().unwrap();
        assert_eq!(core_limit(), (Some(0), Some(0)));
        // idempotent, and the limit cannot be raised again
        disable_core_dumps().unwrap();
        assert!(rustix::process::setrlimit(
            rustix::process::Resource::Core,
            rustix::process::Rlimit {
                current: Some(1),
                maximum: Some(1)
            }
        )
        .is_err());
        #[cfg(target_os = "linux")]
        assert_eq!(
            rustix::process::dumpable_behavior().unwrap(),
            rustix::process::DumpableBehavior::NotDumpable
        );
    }

    #[test]
    fn core_dumps_are_disabled_for_the_process_that_asks() {
        let exe = std::env::current_exe().unwrap();
        let out = Command::new(exe)
            .args([
                "--ignored",
                "--exact",
                "platform::tests::child_disables_core_dumps",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
