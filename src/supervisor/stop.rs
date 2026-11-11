//! Stopping a command's process group and confirming it is gone.
//!
//! The signals and the probing are here; the decisions (when to send TERM,
//! when to escalate to KILL, when to give up) are in [`super::escalation`].

use super::escalation::{Escalation, StopStep};
use super::identity::{group_owner, GroupOwner, Leader};
use crate::platform::{self, GroupSignal, StartIdentity};
use std::time::Duration;

/// A recorded command: its leader and process group, and what is known of
/// the leader ([`Leader`]): that is what tells it from a later process with
/// the same PID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopTarget {
    pub pid: u32,
    pub pgid: i32,
    pub leader: Leader,
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
    /// Whose the group is could not be told (the leader's identity could
    /// not be read): nothing was signalled from then on and nothing is
    /// known. The stop is to be tried again later.
    IdentityUnknown,
}

/// Whether the target group still has members that are ours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// It has members that are the command's.
    Alive,
    /// It has none: they ended, or the PID belongs to another process now
    /// (which is never signalled).
    Gone,
    /// It cannot be told. Never signal, never record a stop.
    Unknown,
}

/// The system calls a stop makes. A test replaces them, so that no test ever
/// sends a real signal to a made-up PID.
pub(crate) trait Os {
    fn start_identity(&self, pid: u32) -> StartIdentity;
    fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal;
    fn monotonic_ms(&self) -> u64;
    fn pause(&self, ms: u64);
}

/// The real system, through the guarded helpers of [`crate::platform`].
pub(crate) struct System;

impl Os for System {
    fn start_identity(&self, pid: u32) -> StartIdentity {
        platform::process_start_identity(pid)
    }
    fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal {
        platform::signal_process_group(pgid, sig)
    }
    fn monotonic_ms(&self) -> u64 {
        platform::monotonic_ms()
    }
    fn pause(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

/// Whether the target group still has members that are ours.
///
/// The leader's PID is read now and judged by [`group_owner`]: only a
/// leader with the recorded identity, or a leader that is definitely gone
/// with members left in its group, makes the group alive. A read that fails
/// is `Unknown`, never gone.
pub fn group_state(t: &StopTarget) -> GroupState {
    group_state_on(&System, t)
}

pub(crate) fn group_state_on(os: &dyn Os, t: &StopTarget) -> GroupState {
    let leads = i32::try_from(t.pid) == Ok(t.pgid);
    match group_owner(&t.leader, &os.start_identity(t.pid), leads) {
        GroupOwner::Ours => GroupState::Alive,
        GroupOwner::Foreign => GroupState::Gone,
        GroupOwner::Unknown => GroupState::Unknown,
        GroupOwner::Orphaned => {
            if os.signal_group(t.pgid, 0) == GroupSignal::Empty {
                GroupState::Gone
            } else {
                GroupState::Alive
            }
        }
    }
}

fn send(os: &dyn Os, t: &StopTarget, sig: i32) {
    // Judged again right before signalling: never signal a reused PID or a
    // group whose owner cannot be told.
    if group_state_on(os, t) == GroupState::Alive {
        let _ = os.signal_group(t.pgid, sig);
    }
}

/// TERM the group, KILL it after the grace, and report whether it is gone.
/// The moment the group's owner cannot be told, the stop ends as
/// [`StopResult::IdentityUnknown`] without another signal.
pub fn stop_group(t: &StopTarget, times: StopTimes) -> StopResult {
    stop_group_on(&System, t, times)
}

pub(crate) fn stop_group_on(os: &dyn Os, t: &StopTarget, times: StopTimes) -> StopResult {
    let mut esc = Escalation::new(times.grace_ms, times.kill_wait_ms);
    loop {
        let alive = match group_state_on(os, t) {
            GroupState::Unknown => return StopResult::IdentityUnknown,
            state => state == GroupState::Alive,
        };
        match esc.step(os.monotonic_ms(), alive) {
            StopStep::Term => send(os, t, libc::SIGTERM),
            StopStep::Kill => send(os, t, libc::SIGKILL),
            StopStep::Wait => {}
            StopStep::Confirmed => return StopResult::Confirmed,
            StopStep::Unconfirmed => return StopResult::Unconfirmed,
        }
        os.pause(times.poll_ms.max(1));
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
/// confirmed gone, up to `retries` more times. An unknown identity is not
/// retried here: the caller looks again later.
pub fn stop_group_retrying(t: &StopTarget, times: StopTimes, retries: u32) -> StopResult {
    stop_group_retrying_on(&System, t, times, retries)
}

pub(crate) fn stop_group_retrying_on(
    os: &dyn Os,
    t: &StopTarget,
    times: StopTimes,
    retries: u32,
) -> StopResult {
    retry(retries, |i| {
        let times = if i == 0 {
            times
        } else {
            StopTimes {
                grace_ms: 0,
                ..times
            }
        };
        stop_group_on(os, t, times)
    })
}

fn retry(retries: u32, mut f: impl FnMut(u32) -> StopResult) -> StopResult {
    let mut last = StopResult::Unconfirmed;
    for i in 0..=retries {
        last = f(i);
        if last != StopResult::Unconfirmed {
            break;
        }
    }
    last
}

/// A system that only exists in a test: it answers identity reads from a
/// script and records the signals instead of sending them, so a made-up PID
/// can never reach a real `kill`.
#[cfg(test)]
pub(crate) mod fake {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    pub(crate) struct FakeOs {
        /// Answers to the identity reads, in order; the last one repeats.
        reads: RefCell<VecDeque<StartIdentity>>,
        /// Whether the group has members (what a probe with signal 0 finds).
        pub members: Cell<bool>,
        /// A TERM or KILL empties the group.
        pub dies_when_signalled: bool,
        /// Every `(pgid, signal)` asked for, probes included.
        pub signals: RefCell<Vec<(i32, i32)>>,
        pub now: Cell<u64>,
        pub read_count: Cell<u32>,
    }

    impl FakeOs {
        pub(crate) fn new(reads: impl IntoIterator<Item = StartIdentity>) -> Self {
            FakeOs {
                reads: RefCell::new(reads.into_iter().collect()),
                members: Cell::new(true),
                dies_when_signalled: true,
                signals: RefCell::default(),
                now: Cell::new(0),
                read_count: Cell::new(0),
            }
        }

        /// The signals that would have reached a process (not the probes).
        pub(crate) fn sent(&self) -> Vec<(i32, i32)> {
            let all = self.signals.borrow();
            all.iter().copied().filter(|(_, sig)| *sig != 0).collect()
        }
    }

    impl Os for FakeOs {
        fn start_identity(&self, _pid: u32) -> StartIdentity {
            self.read_count.set(self.read_count.get() + 1);
            let mut reads = self.reads.borrow_mut();
            if reads.len() > 1 {
                reads.pop_front().expect("not empty")
            } else {
                reads.front().cloned().expect("a scripted read")
            }
        }
        fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal {
            self.signals.borrow_mut().push((pgid, sig));
            if !self.members.get() {
                return GroupSignal::Empty;
            }
            if sig != 0 && self.dies_when_signalled {
                self.members.set(false);
            }
            GroupSignal::Delivered
        }
        fn monotonic_ms(&self) -> u64 {
            self.now.get()
        }
        fn pause(&self, ms: u64) {
            self.now.set(self.now.get() + ms);
        }
    }

    /// A PID no system hands out (above every `pid_max`), for records that
    /// only ever meet a [`FakeOs`].
    pub(crate) const NO_SUCH_PID: u32 = 0x7fff_fff0;

    pub(crate) fn id(s: &str) -> StartIdentity {
        StartIdentity::Identity(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::fake::{id, FakeOs, NO_SUCH_PID};
    use super::*;

    const PGID: i32 = NO_SUCH_PID as i32;

    fn target(leader: Leader) -> StopTarget {
        StopTarget {
            pid: NO_SUCH_PID,
            pgid: PGID,
            leader,
        }
    }

    fn known() -> StopTarget {
        target(Leader::Known("linux:1000".into()))
    }

    fn times() -> StopTimes {
        StopTimes {
            grace_ms: 100,
            kill_wait_ms: 100,
            poll_ms: 10,
        }
    }

    #[test]
    fn the_recorded_leader_makes_the_group_alive_without_a_probe() {
        let os = FakeOs::new([id("linux:1000")]);
        assert_eq!(group_state_on(&os, &known()), GroupState::Alive);
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn another_process_with_the_pid_is_gone_and_is_not_even_probed() {
        let os = FakeOs::new([id("linux:2000")]);
        assert_eq!(group_state_on(&os, &known()), GroupState::Gone);
        assert_eq!(
            stop_group_retrying_on(&os, &known(), times(), 3),
            StopResult::Confirmed
        );
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn a_leader_that_is_gone_leaves_the_group_to_a_probe() {
        let os = FakeOs::new([StartIdentity::NoProcess]);
        assert_eq!(group_state_on(&os, &known()), GroupState::Alive);
        os.members.set(false);
        assert_eq!(group_state_on(&os, &known()), GroupState::Gone);
        assert_eq!(*os.signals.borrow(), vec![(PGID, 0), (PGID, 0)]);
    }

    #[test]
    fn an_unreadable_identity_is_unknown_and_nothing_is_probed_or_signalled() {
        for leader in [
            Leader::Known("linux:1000".into()),
            Leader::Ended,
            Leader::Unread,
        ] {
            let os = FakeOs::new([StartIdentity::Unknown]);
            let t = target(leader);
            assert_eq!(group_state_on(&os, &t), GroupState::Unknown);
            assert_eq!(stop_group_on(&os, &t, times()), StopResult::IdentityUnknown);
            assert!(os.signals.borrow().is_empty(), "{t:?}");
            assert_eq!(os.now.get(), 0, "it does not wait either");
        }
    }

    #[test]
    fn a_leader_that_was_never_read_is_not_signalled_while_something_has_its_pid() {
        let os = FakeOs::new([id("linux:1000")]);
        let t = target(Leader::Unread);
        assert_eq!(stop_group_on(&os, &t, times()), StopResult::IdentityUnknown);
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn a_group_that_the_leader_does_not_lead_is_never_signalled() {
        let os = FakeOs::new([id("linux:1000")]);
        let t = StopTarget {
            pgid: PGID - 1,
            ..known()
        };
        assert_eq!(group_state_on(&os, &t), GroupState::Unknown);
        assert_eq!(stop_group_on(&os, &t, times()), StopResult::IdentityUnknown);
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn an_unknown_identity_is_not_retried_in_place() {
        let os = FakeOs::new([StartIdentity::Unknown]);
        assert_eq!(
            stop_group_retrying_on(&os, &known(), times(), 3),
            StopResult::IdentityUnknown
        );
        // One look, then back to the caller: no spinning on a failing read.
        assert_eq!(os.read_count.get(), 1);
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn a_stop_ends_without_another_signal_once_the_identity_is_unreadable() {
        // Alive for the first look and the look before TERM, then unreadable.
        let mut os = FakeOs::new([id("linux:1000"), id("linux:1000"), StartIdentity::Unknown]);
        os.dies_when_signalled = false;
        assert_eq!(
            stop_group_on(&os, &known(), times()),
            StopResult::IdentityUnknown
        );
        assert_eq!(os.sent(), vec![(PGID, libc::SIGTERM)], "no KILL followed");
    }

    #[test]
    fn the_identity_is_read_again_between_the_decision_and_the_signal() {
        // The look says alive; by the time TERM is due the read fails.
        let os = FakeOs::new([id("linux:1000"), StartIdentity::Unknown]);
        assert_eq!(
            stop_group_on(&os, &known(), times()),
            StopResult::IdentityUnknown
        );
        assert!(os.sent().is_empty());
        // The same, with the PID taken by another process in between.
        let os = FakeOs::new([id("linux:1000"), id("linux:2000")]);
        assert_eq!(stop_group_on(&os, &known(), times()), StopResult::Confirmed);
        assert!(os.sent().is_empty());
    }

    #[test]
    fn the_same_process_is_termed_and_confirmed() {
        // TERM empties the group; the leader is gone on the next look.
        let os = FakeOs::new([id("linux:1000"), id("linux:1000"), StartIdentity::NoProcess]);
        assert_eq!(stop_group_on(&os, &known(), times()), StopResult::Confirmed);
        assert_eq!(os.sent(), vec![(PGID, libc::SIGTERM)]);
    }

    #[test]
    fn a_group_that_ignores_everything_is_killed_and_then_unconfirmed() {
        let mut os = FakeOs::new([id("linux:1000")]);
        os.dies_when_signalled = false;
        assert_eq!(
            stop_group_on(&os, &known(), times()),
            StopResult::Unconfirmed
        );
        let sent = os.sent();
        assert_eq!(sent[0], (PGID, libc::SIGTERM));
        assert!(sent[1..].iter().all(|s| *s == (PGID, libc::SIGKILL)));
        assert!(sent.len() > 2);
        assert!(os.now.get() >= 200);
    }

    #[test]
    fn what_a_gone_leader_left_is_stopped_through_the_group() {
        let os = FakeOs::new([StartIdentity::NoProcess]);
        assert_eq!(
            stop_group_on(&os, &target(Leader::Ended), times()),
            StopResult::Confirmed
        );
        assert_eq!(os.sent(), vec![(PGID, libc::SIGTERM)]);
    }

    #[test]
    fn the_real_system_reads_and_refuses_as_the_platform_does() {
        // No signal is sent here: group 0 is refused by the guarded helper.
        assert_eq!(System.signal_group(0, 0), GroupSignal::Empty);
        assert_eq!(
            System.start_identity(std::process::id()),
            platform::process_start_identity(std::process::id())
        );
        let before = System.monotonic_ms();
        System.pause(1);
        assert!(System.monotonic_ms() >= before);
    }

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
        let mut n = 0;
        let r = retry(3, |_| {
            n += 1;
            StopResult::IdentityUnknown
        });
        assert_eq!((r, n), (StopResult::IdentityUnknown, 1));
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
