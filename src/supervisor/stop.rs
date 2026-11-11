//! Stopping a command's process group and confirming it is gone.
//!
//! The signals and the probing are here; the decisions (when to send TERM,
//! when to escalate to KILL, when to give up) are in [`super::escalation`].

use super::escalation::{Escalation, StopStep};
pub use super::identity::GroupState;
use super::identity::{self, group_owner, Leader};
use crate::platform::{self, GroupSignal, ProcessRead};
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
    /// not be read, or the leader has ended and no member left under its
    /// group number is known to be the command's): nothing was signalled
    /// from then on and nothing is known. The stop is to be tried again
    /// later.
    IdentityUnknown,
}

/// Evidence about the members of a process group whose leader has ended.
///
/// A group number is the PID of its leader, and a PID is reused: once the
/// group was empty, the number can be another group's. So what is left under
/// the number of a leader that is gone is signalled only when a member is
/// positively known to be the command's.
pub trait Members {
    /// True only when a process known to be the command's (by more than its
    /// PID) is in the group `pgid` right now.
    fn proven(&self, pgid: i32) -> bool;
}

/// No evidence about any group: what a gone leader left is never signalled.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEvidence;

impl Members for NoEvidence {
    fn proven(&self, _pgid: i32) -> bool {
        false
    }
}

/// The system calls a stop makes. A test replaces them, so that no test ever
/// sends a real signal to a made-up PID.
pub(crate) trait Os {
    fn read(&self, pid: u32) -> ProcessRead;
    fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal;
    /// Whether a member of the group is known to be the command's.
    fn member_proven(&self, pgid: i32) -> bool;
    fn monotonic_ms(&self) -> u64;
    fn pause(&self, ms: u64);
}

/// The real system, through the guarded helpers of [`crate::platform`], with
/// the evidence about the members of the command's group.
pub(crate) struct System<'a>(pub &'a dyn Members);

impl Os for System<'_> {
    fn read(&self, pid: u32) -> ProcessRead {
        platform::read_process(pid)
    }
    fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal {
        platform::signal_process_group(pgid, sig)
    }
    fn member_proven(&self, pgid: i32) -> bool {
        self.0.proven(pgid)
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
/// The leader's PID is read now and judged by [`group_owner`]: a leader with
/// the recorded identity makes the group alive. When the leader has ended,
/// the group is alive only if it has members and `members` proves that one
/// of them is the command's ([`identity::group_state`]). A read that fails
/// is `Unknown`, never gone.
pub fn group_state(t: &StopTarget, members: &dyn Members) -> GroupState {
    group_state_on(&System(members), t)
}

pub(crate) fn group_state_on(os: &dyn Os, t: &StopTarget) -> GroupState {
    let leads = i32::try_from(t.pid) == Ok(t.pgid);
    let owner = group_owner(&t.leader, &os.read(t.pid).start, leads);
    identity::group_state(
        owner,
        || os.signal_group(t.pgid, 0),
        || os.member_proven(t.pgid),
    )
}

/// Signal the group, when it is (still) the command's. True when a signal
/// was sent.
fn send(os: &dyn Os, t: &StopTarget, sig: i32) -> bool {
    // Judged again right before signalling: never signal a reused PID or a
    // group whose owner cannot be told.
    let ours = group_state_on(os, t) == GroupState::Alive;
    if ours {
        let _ = os.signal_group(t.pgid, sig);
    }
    ours
}

/// TERM the group, KILL it after the grace, and report whether it is gone.
/// The moment the group's owner cannot be told, the stop ends as
/// [`StopResult::IdentityUnknown`] without another signal.
///
/// Members that are left under the number of a leader that has ended, and
/// that are not known to be the command's, are never signalled either. When
/// this stop has signalled the group before, they may be what is still
/// ending of it, so the stop waits for them (within the same windows)
/// instead of ending at once; if they stay, it ends as `IdentityUnknown`.
pub fn stop_group(t: &StopTarget, times: StopTimes, members: &dyn Members) -> StopResult {
    stop_group_on(&System(members), t, times)
}

pub(crate) fn stop_group_on(os: &dyn Os, t: &StopTarget, times: StopTimes) -> StopResult {
    let mut esc = Escalation::new(times.grace_ms, times.kill_wait_ms);
    let mut signalled = false;
    loop {
        let state = group_state_on(os, t);
        if identity::waits_for(state, signalled) == Some(false) {
            return StopResult::IdentityUnknown;
        }
        match esc.step(os.monotonic_ms(), state != GroupState::Gone) {
            StopStep::Term => signalled |= send(os, t, libc::SIGTERM),
            StopStep::Kill => signalled |= send(os, t, libc::SIGKILL),
            StopStep::Wait => {}
            StopStep::Confirmed => return StopResult::Confirmed,
            StopStep::Unconfirmed if identity::stop_was_ours(state) => {
                return StopResult::Unconfirmed
            }
            StopStep::Unconfirmed => return StopResult::IdentityUnknown,
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
pub fn stop_group_retrying(
    t: &StopTarget,
    times: StopTimes,
    retries: u32,
    members: &dyn Members,
) -> StopResult {
    stop_group_retrying_on(&System(members), t, times, retries)
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
    use crate::platform::StartIdentity;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    pub(crate) struct FakeOs {
        /// Answers to the identity reads, in order; the last one repeats.
        reads: RefCell<VecDeque<StartIdentity>>,
        /// Whether the group has members (what a probe with signal 0 finds).
        pub members: Cell<bool>,
        /// Whether a member is known to be the command's.
        pub proven: Cell<bool>,
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
                proven: Cell::new(true),
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
        fn read(&self, _pid: u32) -> ProcessRead {
            self.read_count.set(self.read_count.get() + 1);
            let mut reads = self.reads.borrow_mut();
            ProcessRead::of(if reads.len() > 1 {
                reads.pop_front().expect("not empty")
            } else {
                reads.front().cloned().expect("a scripted read")
            })
        }
        fn member_proven(&self, _pgid: i32) -> bool {
            self.proven.get()
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
    use crate::platform::StartIdentity;

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
    fn what_a_gone_leader_left_is_not_signalled_without_evidence_of_a_member() {
        // Members under the number, none known to be the command's: the
        // number may be another group's by now.
        for leader in [Leader::Known("linux:1000".into()), Leader::Ended] {
            let os = FakeOs::new([StartIdentity::NoProcess]);
            os.proven.set(false);
            let t = target(leader);
            assert_eq!(group_state_on(&os, &t), GroupState::Unproven);
            assert_eq!(
                stop_group_retrying_on(&os, &t, times(), 3),
                StopResult::IdentityUnknown
            );
            assert!(os.sent().is_empty(), "{t:?}");
            assert_eq!(os.now.get(), 0, "it does not wait either");
        }
    }

    #[test]
    fn the_zombie_of_the_recorded_leader_leaves_the_group_to_the_evidence() {
        let z = || StartIdentity::Zombie("linux:1000".into());
        let os = FakeOs::new([z()]);
        assert_eq!(group_state_on(&os, &known()), GroupState::Alive);
        os.proven.set(false);
        assert_eq!(group_state_on(&os, &known()), GroupState::Unproven);
        // A zombie of another process, or any zombie once ours was reaped.
        let os = FakeOs::new([StartIdentity::Zombie("linux:2000".into())]);
        assert_eq!(group_state_on(&os, &known()), GroupState::Gone);
        let os = FakeOs::new([z()]);
        assert_eq!(
            group_state_on(&os, &target(Leader::Ended)),
            GroupState::Gone
        );
        assert!(os.signals.borrow().is_empty());
    }

    #[test]
    fn unproven_members_after_a_signal_are_waited_for_and_never_signalled() {
        // TERM reaches the leader; what is then left under the number (the
        // leader not reaped yet, members still ending) is not known to be
        // ours any more. It is waited for, not signalled.
        let mut os = FakeOs::new([id("linux:1000"), id("linux:1000"), StartIdentity::NoProcess]);
        os.dies_when_signalled = false;
        os.proven.set(false);
        assert_eq!(
            stop_group_on(&os, &known(), times()),
            StopResult::IdentityUnknown
        );
        assert_eq!(os.sent(), vec![(PGID, libc::SIGTERM)], "no KILL followed");
        assert!(os.now.get() >= 200, "both windows were waited out");

        // The same, and they end during the wait: confirmed.
        struct Ends(FakeOs);
        impl Os for Ends {
            fn read(&self, pid: u32) -> ProcessRead {
                self.0.read(pid)
            }
            fn signal_group(&self, pgid: i32, sig: i32) -> GroupSignal {
                self.0.signal_group(pgid, sig)
            }
            fn member_proven(&self, pgid: i32) -> bool {
                self.0.member_proven(pgid)
            }
            fn monotonic_ms(&self) -> u64 {
                self.0.monotonic_ms()
            }
            fn pause(&self, ms: u64) {
                self.0.pause(ms);
                if self.0.now.get() >= 50 {
                    self.0.members.set(false);
                }
            }
        }
        let mut os = FakeOs::new([id("linux:1000"), id("linux:1000"), StartIdentity::NoProcess]);
        os.dies_when_signalled = false;
        os.proven.set(false);
        let os = Ends(os);
        assert_eq!(stop_group_on(&os, &known(), times()), StopResult::Confirmed);
        assert_eq!(os.0.sent(), vec![(PGID, libc::SIGTERM)]);
        assert!(os.0.now.get() < 100);
    }

    #[test]
    fn the_real_system_reads_and_refuses_as_the_platform_does() {
        let os = System(&NoEvidence);
        // No signal is sent here: group 0 is refused by the guarded helper.
        assert_eq!(os.signal_group(0, 0), GroupSignal::Empty);
        assert_eq!(
            os.read(std::process::id()),
            platform::read_process(std::process::id())
        );
        assert!(!os.member_proven(i32::MAX - 1));
        let before = os.monotonic_ms();
        os.pause(1);
        assert!(os.monotonic_ms() >= before);
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
