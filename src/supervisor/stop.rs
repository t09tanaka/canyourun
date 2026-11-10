//! Stopping a command's process group and confirming it is gone.
//!
//! The signals and the probing are here; the decisions (when to send TERM,
//! when to escalate to KILL, when to give up) are in [`super::escalation`].

use super::escalation::{Escalation, StopStep};
use crate::platform::{self, GroupSignal};
use std::time::Duration;

/// A recorded command: its leader and process group, and the start identity
/// that tells the leader from a later process with the same PID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopTarget {
    pub pid: u32,
    pub pgid: i32,
    pub start_identity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopTimes {
    /// TERM to KILL.
    pub grace_ms: u64,
    /// After KILL, how long to keep checking before giving up.
    pub kill_wait_ms: u64,
    /// How often to look.
    pub poll_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopResult {
    /// The group has no members (stopped, or was already gone).
    Confirmed,
    /// Members were still there after KILL and the confirmation window.
    Unconfirmed,
}

/// Whether the target group still has members that are ours.
///
/// A leader whose start identity differs from the recorded one is a
/// different process that reused the PID: the group is not ours, and nothing
/// of it may be signalled. With the leader gone, members that remain belong
/// to the group (a group number is not handed out again while it has members).
pub fn group_alive(t: &StopTarget) -> bool {
    match (platform::process_start_identity(t.pid), &t.start_identity) {
        (Some(now), Some(rec)) if now != *rec => return false,
        (Some(_), _) => return true,
        (None, _) => {}
    }
    platform::signal_process_group(t.pgid, 0) != GroupSignal::Empty
}

fn send(t: &StopTarget, sig: i32) {
    // Check again right before signalling: never signal a reused PID.
    if group_alive(t) {
        let _ = platform::signal_process_group(t.pgid, sig);
    }
}

/// TERM the group, KILL it after the grace, and report whether it is gone.
pub fn stop_group(t: &StopTarget, times: StopTimes) -> StopResult {
    let mut esc = Escalation::new(times.grace_ms, times.kill_wait_ms);
    loop {
        match esc.step(platform::monotonic_ms(), group_alive(t)) {
            StopStep::Term => send(t, libc::SIGTERM),
            StopStep::Kill => send(t, libc::SIGKILL),
            StopStep::Wait => {}
            StopStep::Confirmed => return StopResult::Confirmed,
            StopStep::Unconfirmed => return StopResult::Unconfirmed,
        }
        std::thread::sleep(Duration::from_millis(times.poll_ms.max(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    fn times(grace: u64) -> StopTimes {
        StopTimes {
            grace_ms: grace,
            kill_wait_ms: 3_000,
            poll_ms: 20,
        }
    }

    /// A shell that leads its own group, ignores TERM when asked to, and has
    /// a background child in the same group.
    fn spawn_group(ignore_term: bool) -> (std::process::Child, StopTarget) {
        let ready = tempfile::NamedTempFile::new().unwrap();
        let ready_path = ready.path().to_path_buf();
        std::fs::remove_file(&ready_path).unwrap();
        let body = if ignore_term {
            "trap '' TERM; sleep 30 & touch \"$1\"; while :; do sleep 1; done"
        } else {
            "sleep 30 & touch \"$1\"; wait"
        };
        let child = Command::new("sh")
            .arg("-c")
            .arg(body)
            .arg("sh")
            .arg(&ready_path)
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = std::time::Instant::now() + Duration::from_secs(10);
        while !ready_path.exists() {
            assert!(std::time::Instant::now() < until, "the group did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        let pid = child.id();
        let target = StopTarget {
            pid,
            pgid: pid as i32,
            start_identity: platform::process_start_identity(pid),
        };
        (child, target)
    }

    #[test]
    fn term_stops_a_cooperative_group_without_kill() {
        let (mut child, t) = spawn_group(false);
        assert!(group_alive(&t));
        let started = std::time::Instant::now();
        let reaper = std::thread::spawn(move || child.wait().unwrap());
        assert_eq!(stop_group(&t, times(10_000)), StopResult::Confirmed);
        let status = reaper.join().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGTERM), "{status:?}");
        assert!(started.elapsed() < Duration::from_secs(8));
        assert!(!group_alive(&t));
    }

    #[test]
    fn kill_follows_the_grace_for_a_group_that_ignores_term() {
        let (mut child, t) = spawn_group(true);
        let started = std::time::Instant::now();
        let reaper = std::thread::spawn(move || child.wait().unwrap());
        assert_eq!(stop_group(&t, times(400)), StopResult::Confirmed);
        let status = reaper.join().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");
        // TERM was ignored, so the stop took at least the grace.
        assert!(started.elapsed() >= Duration::from_millis(350));
        assert!(
            !group_alive(&t),
            "the whole group, background child included"
        );
    }

    #[test]
    fn descendants_in_the_group_are_stopped_with_the_leader() {
        let (mut child, t) = spawn_group(false);
        // Reap the leader first: the background `sleep` keeps the group alive.
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(group_alive(&t), "a member remains after the leader is gone");
        assert_eq!(stop_group(&t, times(5_000)), StopResult::Confirmed);
        assert!(!group_alive(&t));
    }

    #[test]
    fn a_reused_pid_is_not_signalled() {
        let (mut child, mut t) = spawn_group(false);
        t.start_identity = Some("linux:0-someone-else".into());
        assert!(!group_alive(&t), "a different start identity is not ours");
        assert_eq!(stop_group(&t, times(100)), StopResult::Confirmed);
        // The real process was left alone.
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
        let _ = platform::signal_process_group(t.pgid, libc::SIGKILL);
    }

    #[test]
    fn a_group_that_is_already_gone_is_confirmed_at_once() {
        let (mut child, t) = spawn_group(false);
        let _ = platform::signal_process_group(t.pgid, libc::SIGKILL);
        child.wait().unwrap();
        let started = std::time::Instant::now();
        while group_alive(&t) && started.elapsed() < Duration::from_secs(3) {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(stop_group(&t, times(10_000)), StopResult::Confirmed);
    }
}
