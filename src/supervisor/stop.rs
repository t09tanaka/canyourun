//! Stopping a command's process group and confirming it is gone.
//!
//! The signals and the probing are here; the decisions (when to send TERM,
//! when to escalate to KILL, when to give up) are in [`super::escalation`].

use super::escalation::{Escalation, StopStep};
use crate::platform::{self, GroupSignal};
use std::time::Duration;

/// A recorded command: its leader and process group, and the start identity
/// that tells the leader from a later process with the same PID. Without a
/// start identity the leader is taken to be gone (it ended before its
/// identity could be read, or was reaped): only the group is looked at.
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
    if let Some(rec) = &t.start_identity {
        match platform::process_start_identity(t.pid) {
            Some(now) if now != *rec => return false,
            Some(_) => return true,
            None => {}
        }
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

/// How many more times a stop that could not be confirmed is tried (each try
/// goes straight to KILL and waits `kill_wait_ms`), so that the whole
/// retrying stays within [`RETRY_BUDGET_MS`].
pub fn retries_for(kill_wait_ms: u64) -> u32 {
    (RETRY_BUDGET_MS / kill_wait_ms.max(1)).min(3) as u32
}

/// The most time the retries of one stop may take in total.
pub const RETRY_BUDGET_MS: u64 = 30_000;

/// [`stop_group`], tried again (KILL at once) when the group could not be
/// confirmed gone, up to `retries` more times.
pub fn stop_group_retrying(t: &StopTarget, times: StopTimes, retries: u32) -> StopResult {
    retry(retries, |i| {
        let times = if i == 0 {
            times
        } else {
            StopTimes {
                grace_ms: 0,
                ..times
            }
        };
        stop_group(t, times)
    })
}

fn retry(retries: u32, mut f: impl FnMut(u32) -> StopResult) -> StopResult {
    let mut last = StopResult::Unconfirmed;
    for i in 0..=retries {
        last = f(i);
        if last == StopResult::Confirmed {
            break;
        }
    }
    last
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_stop_at_the_first_confirmation_and_report_the_last_failure() {
        let mut calls = Vec::new();
        let r = retry(3, |i| {
            calls.push(i);
            if i == 2 {
                StopResult::Confirmed
            } else {
                StopResult::Unconfirmed
            }
        });
        assert_eq!((r, calls), (StopResult::Confirmed, vec![0, 1, 2]));
        let mut n = 0;
        let r = retry(2, |_| {
            n += 1;
            StopResult::Unconfirmed
        });
        assert_eq!((r, n), (StopResult::Unconfirmed, 3));
        let mut n = 0;
        let r = retry(0, |_| {
            n += 1;
            StopResult::Unconfirmed
        });
        assert_eq!((r, n), (StopResult::Unconfirmed, 1));
    }

    #[test]
    fn the_retries_fit_the_budget() {
        assert_eq!(retries_for(5_000), 3);
        assert_eq!(retries_for(10_000), 3);
        assert_eq!(retries_for(15_000), 2);
        assert_eq!(retries_for(30_000), 1);
        assert_eq!(retries_for(60_000), 0);
        assert_eq!(retries_for(0), 3);
    }
}
