//! Stopping process groups, against real processes. Kept out of the library's
//! unit tests: forking from there races with other tests' lock files.

use remote_task_runner::platform::{self, StartIdentity};
use remote_task_runner::supervisor::identity::Leader;
use remote_task_runner::supervisor::stop::*;

use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::Duration;

fn times(grace: u64) -> StopTimes {
    StopTimes {
        grace_ms: grace,
        kill_wait_ms: 3_000,
        poll_ms: 20,
    }
}

/// The group has members that are the command's (and that is known).
fn alive(t: &StopTarget) -> bool {
    group_state(t) == GroupState::Alive
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
    // Leave nothing behind in the temp directory.
    let _ = std::fs::remove_file(&ready_path);
    let pid = child.id();
    let target = StopTarget {
        pid,
        pgid: pid as i32,
        leader: Leader::from_read(platform::process_start_identity(pid)),
    };
    (child, target)
}

#[test]
fn term_stops_a_cooperative_group_without_kill() {
    let (mut child, t) = spawn_group(false);
    assert!(alive(&t));
    let started = std::time::Instant::now();
    let reaper = std::thread::spawn(move || child.wait().unwrap());
    assert_eq!(stop_group(&t, times(10_000)), StopResult::Confirmed);
    let status = reaper.join().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGTERM), "{status:?}");
    assert!(started.elapsed() < Duration::from_secs(8));
    assert!(!alive(&t));
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
    assert!(!alive(&t), "the whole group, background child included");
}

#[test]
fn descendants_in_the_group_are_stopped_with_the_leader() {
    let (mut child, t) = spawn_group(false);
    // Reap the leader first: the background `sleep` keeps the group alive.
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(alive(&t), "a member remains after the leader is gone");
    assert_eq!(stop_group(&t, times(5_000)), StopResult::Confirmed);
    assert!(!alive(&t));
}

#[test]
fn a_reused_pid_is_not_signalled() {
    let (mut child, mut t) = spawn_group(false);
    t.leader = Leader::Known("linux:0-someone-else".into());
    assert_eq!(
        group_state(&t),
        GroupState::Gone,
        "a different start identity is not ours"
    );
    assert_eq!(stop_group(&t, times(100)), StopResult::Confirmed);
    // The real process was left alone.
    assert!(child.try_wait().unwrap().is_none());
    child.kill().unwrap();
    child.wait().unwrap();
    // What it left in its group, through the gate: the leader has ended.
    t.leader = Leader::Ended;
    assert_eq!(stop_group(&t, times(0)), StopResult::Confirmed);
}

#[test]
fn a_leader_that_was_never_read_is_not_signalled_while_it_lives() {
    let (mut child, mut t) = spawn_group(false);
    t.leader = Leader::Unread;
    // Something has the PID and there is nothing to compare it with: the
    // group is not ours to signal, and it is not taken to be gone either.
    assert_eq!(group_state(&t), GroupState::Unknown);
    assert_eq!(
        stop_group_retrying(&t, times(100), 2),
        StopResult::IdentityUnknown
    );
    assert!(child.try_wait().unwrap().is_none(), "left alone");
    child.kill().unwrap();
    child.wait().unwrap();
    // The leader is gone but its background child remains.
    assert!(alive(&t));
    assert_eq!(
        stop_group_retrying(&t, times(5_000), 2),
        StopResult::Confirmed
    );
    assert!(!alive(&t));
}

#[test]
fn a_group_that_is_already_gone_is_confirmed_at_once() {
    let (mut child, t) = spawn_group(false);
    let _ = platform::signal_process_group(t.pgid, libc::SIGKILL);
    child.wait().unwrap();
    let started = std::time::Instant::now();
    while alive(&t) && started.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(stop_group(&t, times(10_000)), StopResult::Confirmed);
}

#[test]
fn a_leader_known_to_have_ended_is_not_signalled_when_its_pid_is_taken() {
    let (mut child, mut t) = spawn_group(false);
    // As if the command had been reaped and this process got its PID.
    t.leader = Leader::Ended;
    assert_eq!(group_state(&t), GroupState::Gone);
    assert_eq!(stop_group(&t, times(100)), StopResult::Confirmed);
    assert!(child.try_wait().unwrap().is_none(), "left alone");
    child.kill().unwrap();
    child.wait().unwrap();
    // Now it has ended: what it left in its group is stopped.
    assert!(alive(&t));
    assert_eq!(stop_group(&t, times(5_000)), StopResult::Confirmed);
    assert!(!alive(&t));
}

#[test]
fn another_process_differs_and_a_dead_pid_is_no_process() {
    let me = platform::process_start_identity(std::process::id());
    assert!(me.identity().is_some(), "{me:?}");
    // The start time has a resolution of one clock tick: let one pass.
    std::thread::sleep(Duration::from_millis(50));
    let mut child = Command::new("sleep").arg("5").spawn().unwrap();
    let theirs = platform::process_start_identity(child.id());
    assert!(theirs.identity().is_some(), "{theirs:?}");
    assert_ne!(theirs, me);
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        platform::process_start_identity(child.id()),
        StartIdentity::NoProcess
    );
}
