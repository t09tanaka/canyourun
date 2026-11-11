//! Shared by the integration tests.

#![allow(dead_code)]

use remote_task_runner::platform::{
    process_group_of, process_start_identity, signal_pid, StartIdentity,
};
use remote_task_runner::supervisor::identity::Leader;
use remote_task_runner::supervisor::stop::{
    stop_group, Members, NoEvidence, StopTarget, StopTimes,
};
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

/// Whether a process with this pid exists (signal 0 through the guarded helper).
pub fn alive(pid: i32) -> bool {
    u32::try_from(pid).is_ok_and(|p| signal_pid(p, 0))
}

/// The start identity of `pid`, for a process this test just started and
/// still holds: what a later cleanup compares before it signals.
pub fn identity_of(pid: u32) -> Option<String> {
    process_start_identity(pid).identity().map(str::to_string)
}

/// SIGKILL `pid` alone, only while it still is the process that had the
/// start identity `start`. A PID that was reused, or whose identity cannot
/// be read, is left alone.
pub fn kill_recorded(pid: u32, start: &str) {
    if process_start_identity(pid) == StartIdentity::Identity(start.to_string()) {
        signal_pid(pid, libc::SIGKILL);
    }
}

/// The command line of `pid`, as `ps` shows it.
fn command_line(pid: u32) -> Option<String> {
    let out = std::process::Command::new("ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// SIGKILL `pid` alone, for a PID that a search of the process table gave
/// (`pgrep`): only when the process whose identity is read first has
/// `pattern` in its command line, and is still that process right before
/// the signal. A number that was reused since the search, or whose identity
/// cannot be read, is left alone.
pub fn kill_if_command(pid: i32, pattern: &str) {
    let Ok(pid) = u32::try_from(pid) else {
        return;
    };
    let Some(start) = identity_of(pid) else {
        return;
    };
    if command_line(pid).is_some_and(|c| c.contains(pattern)) {
        kill_recorded(pid, &start);
    }
}

/// Processes a test started itself and read while it held them (PID and
/// start identity): the evidence that a group whose leader has ended still
/// holds one of them.
pub struct RecordedMembers(pub Vec<(u32, String)>);

impl Members for RecordedMembers {
    fn proven(&self, pgid: i32) -> bool {
        self.0.iter().any(|(pid, start)| {
            let same = || process_start_identity(*pid) == StartIdentity::Identity(start.clone());
            // Read again after the group: the group read was of that process.
            same() && process_group_of(*pid) == Some(pgid) && same()
        })
    }
}

/// Stop the group led by `pid`, through the same gate the supervisor uses:
/// the leader must still have the start identity `start`. A reused PID or
/// an unreadable identity is never signalled, and neither is what a leader
/// that is gone left under its number (there is no evidence here of whose
/// that is).
pub fn kill_recorded_group(pid: u32, start: &str) {
    let Ok(pgid) = i32::try_from(pid) else {
        return;
    };
    let target = StopTarget {
        pid,
        pgid,
        leader: Leader::Known(start.to_string()),
    };
    let times = StopTimes {
        grace_ms: 0,
        kill_wait_ms: 2_000,
        poll_ms: 10,
    };
    let _ = stop_group(&target, times, &NoEvidence);
}

/// The pid written in a file, if it holds one.
pub fn read_pid(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Write `text` to `path` and make it executable, so that it can be run at
/// once by a test that shares its process with other tests.
///
/// A file this process holds open for writing cannot be executed (ETXTBSY),
/// and another thread that forks at that moment hands the descriptor to its
/// child until the child execs. So the file is written by a short-lived child
/// instead: no descriptor of it ever exists in this process.
pub fn write_executable(path: &Path, text: &str) {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg("cat > \"$0\" && chmod 755 \"$0\"")
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
    assert!(child.wait().unwrap().success(), "writing {path:?}");
}
