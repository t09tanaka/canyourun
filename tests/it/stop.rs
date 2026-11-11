//! Stopping process groups, against real processes. Kept out of the library's
//! unit tests: forking from there races with other tests' lock files.

use crate::common::RecordedMembers;
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

/// A group started here: its target, and its background member as the
/// evidence of whose the group is once the leader has ended.
struct Group {
    target: StopTarget,
    members: RecordedMembers,
}

/// The group has members that are the command's (and that is known).
fn alive(g: &Group) -> bool {
    group_state(&g.target, &g.members) == GroupState::Alive
}

fn stop(g: &Group, times: StopTimes) -> StopResult {
    stop_group(&g.target, times, &g.members)
}

/// A shell that leads its own group, ignores TERM when asked to, and has
/// a background child in the same group.
fn spawn_group(ignore_term: bool) -> (std::process::Child, Group) {
    let ready = tempfile::NamedTempFile::new().unwrap();
    let ready_path = ready.path().to_path_buf();
    std::fs::remove_file(&ready_path).unwrap();
    let body = if ignore_term {
        "trap '' TERM; sleep 30 & echo $! > \"$1.tmp\"; mv \"$1.tmp\" \"$1\"; \
         while :; do sleep 1; done"
    } else {
        "sleep 30 & echo $! > \"$1.tmp\"; mv \"$1.tmp\" \"$1\"; wait"
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
    let member: u32 = std::fs::read_to_string(&ready_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // Leave nothing behind in the temp directory.
    let _ = std::fs::remove_file(&ready_path);
    let pid = child.id();
    let target = StopTarget {
        pid,
        pgid: pid as i32,
        leader: Leader::from_read(&platform::read_process(pid), std::process::id()),
    };
    assert!(matches!(target.leader, Leader::Known(_)), "{target:?}");
    // The shell's background child, read while the shell (our child, just
    // identified) is alive: it is the process the shell named.
    let start = platform::process_start_identity(member);
    let start = start
        .identity()
        .expect("the member is readable")
        .to_string();
    let members = RecordedMembers(vec![(member, start)]);
    (child, Group { target, members })
}

#[test]
fn term_stops_a_cooperative_group_without_kill() {
    let (mut child, t) = spawn_group(false);
    assert!(alive(&t));
    let started = std::time::Instant::now();
    let reaper = std::thread::spawn(move || child.wait().unwrap());
    assert_eq!(stop(&t, times(10_000)), StopResult::Confirmed);
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
    assert_eq!(stop(&t, times(400)), StopResult::Confirmed);
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
    assert_eq!(stop(&t, times(5_000)), StopResult::Confirmed);
    assert!(!alive(&t));
}

#[test]
fn what_a_gone_leader_left_is_not_signalled_without_evidence_of_a_member() {
    let (mut child, t) = spawn_group(false);
    child.kill().unwrap();
    child.wait().unwrap();
    // Members are left under the number, and nothing says whose they are:
    // by now the number could be the group of another process.
    assert_eq!(group_state(&t.target, &NoEvidence), GroupState::Unproven);
    assert_eq!(
        stop_group_retrying(&t.target, times(0), 2, &NoEvidence),
        StopResult::IdentityUnknown
    );
    assert!(alive(&t), "the member was left alone");
    // A member in another group, or one that is another process by now,
    // proves nothing about this group.
    let (pid, start) = t.members.0[0].clone();
    for wrong in [
        RecordedMembers(vec![(std::process::id(), start.clone())]),
        RecordedMembers(vec![(pid, "linux:0-someone-else".into())]),
        RecordedMembers(vec![]),
    ] {
        assert_eq!(group_state(&t.target, &wrong), GroupState::Unproven);
    }
    // With the member the test recorded, it is the command's group.
    assert_eq!(stop(&t, times(5_000)), StopResult::Confirmed);
    assert!(!alive(&t));
}

#[test]
fn a_reused_pid_is_not_signalled() {
    let (mut child, mut t) = spawn_group(false);
    t.target.leader = Leader::Known("linux:0-someone-else".into());
    assert_eq!(
        group_state(&t.target, &t.members),
        GroupState::Gone,
        "a different start identity is not ours"
    );
    assert_eq!(stop(&t, times(100)), StopResult::Confirmed);
    // The real process was left alone.
    assert!(child.try_wait().unwrap().is_none());
    child.kill().unwrap();
    child.wait().unwrap();
    // What it left in its group, through the gate: the leader has ended.
    t.target.leader = Leader::Ended;
    assert_eq!(stop(&t, times(0)), StopResult::Confirmed);
}

#[test]
fn a_leader_that_was_never_read_is_not_signalled_while_it_lives() {
    let (mut child, mut t) = spawn_group(false);
    t.target.leader = Leader::Unread;
    // Something has the PID and there is nothing to compare it with: the
    // group is not ours to signal, and it is not taken to be gone either.
    assert_eq!(group_state(&t.target, &t.members), GroupState::Unknown);
    assert_eq!(
        stop_group_retrying(&t.target, times(100), 2, &t.members),
        StopResult::IdentityUnknown
    );
    assert!(child.try_wait().unwrap().is_none(), "left alone");
    child.kill().unwrap();
    child.wait().unwrap();
    // The leader is gone but its background child remains.
    assert!(alive(&t));
    assert_eq!(
        stop_group_retrying(&t.target, times(5_000), 2, &t.members),
        StopResult::Confirmed
    );
    assert!(!alive(&t));
}

#[test]
fn a_group_that_is_already_gone_is_confirmed_at_once() {
    let (mut child, t) = spawn_group(false);
    let _ = platform::signal_process_group(t.target.pgid, libc::SIGKILL);
    child.wait().unwrap();
    let started = std::time::Instant::now();
    while alive(&t) && started.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(stop(&t, times(10_000)), StopResult::Confirmed);
}

#[test]
fn a_leader_known_to_have_ended_is_not_signalled_when_its_pid_is_taken() {
    let (mut child, mut t) = spawn_group(false);
    // As if the command had been reaped and this process got its PID.
    t.target.leader = Leader::Ended;
    assert_eq!(group_state(&t.target, &t.members), GroupState::Gone);
    assert_eq!(stop(&t, times(100)), StopResult::Confirmed);
    assert!(child.try_wait().unwrap().is_none(), "left alone");
    child.kill().unwrap();
    child.wait().unwrap();
    // Now it has ended: what it left in its group is stopped.
    assert!(alive(&t));
    assert_eq!(stop(&t, times(5_000)), StopResult::Confirmed);
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

#[test]
fn a_zombie_keeps_the_identity_it_had_alive_until_it_is_reaped() {
    // It ends by itself: nothing is signalled here.
    let mut child = Command::new("sleep").arg("0.5").spawn().unwrap();
    let pid = child.id();
    let live = platform::read_process(pid);
    let Some(start) = live.start.identity().map(str::to_string) else {
        panic!("the child could not be read: {live:?}");
    };
    assert_eq!(live.ppid, Some(std::process::id()));
    let ended = std::time::Instant::now();
    let zombie = loop {
        let now = platform::read_process(pid);
        if now != live || ended.elapsed() > Duration::from_secs(20) {
            break now;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(zombie.start, StartIdentity::Zombie(start.clone()));
    assert_eq!(zombie.ppid, Some(std::process::id()));
    // Still our child and still the same process: its identity is known.
    assert_eq!(
        Leader::from_read(&zombie, std::process::id()),
        Leader::Known(start)
    );
    child.wait().unwrap();
    assert_eq!(
        platform::process_start_identity(pid),
        StartIdentity::NoProcess
    );
}
