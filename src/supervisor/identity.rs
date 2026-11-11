//! Telling whether a recorded process is still the same process (spec
//! chapter 12, 15). A PID is not an identity: it is reused, and a reboot
//! recycles every one of them. A recorded process is `(boot ID, pid, start
//! identity)`, and this module compares that record with what the system
//! says now.
//!
//! The system's answer about a PID ([`crate::platform::StartIdentity`]) is
//! an identity (of a live process or of a zombie), a definite "no such
//! process", or "could not be read". The last one is never folded into
//! any of the others: a process whose identity is unknown is never
//! signalled, and is never taken to be gone.

use crate::platform::process_read::{GroupSignal, ProcessRead, StartIdentity};
use crate::storage::ProcessIdentity;

/// What the recorded launch looks like now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A launch intent exists but the process was never recorded: whether a
    /// child was started is unknown. Never respawn; investigate.
    NotRecorded,
    /// The machine restarted since the launch: the process is gone.
    Rebooted,
    /// The same boot and the PID is free, or the recorded process has ended
    /// and only waits to be reaped: the process is gone.
    Gone,
    /// The same boot and the PID now belongs to a different process: the
    /// recorded one is gone, and the new one must not be signalled.
    PidReused,
    /// The same process is still there.
    Same,
    /// The boot ID could not be read now, so nothing can be concluded.
    CannotTell,
    /// The same boot, but what has the PID now could not be read: nothing
    /// can be concluded and nothing may be signalled. Look again later.
    Unknown,
}

impl Verdict {
    /// True when the recorded process is known not to exist.
    pub fn is_gone(self) -> bool {
        matches!(self, Verdict::Rebooted | Verdict::Gone | Verdict::PidReused)
    }
}

/// Compare the `recorded` identity with the current boot and with what the
/// system says has that PID now.
pub fn verdict(
    recorded: Option<&ProcessIdentity>,
    boot_now: Option<&str>,
    start_now: &StartIdentity,
) -> Verdict {
    let Some(rec) = recorded else {
        return Verdict::NotRecorded;
    };
    let Some(boot) = boot_now else {
        return Verdict::CannotTell;
    };
    if rec.boot_id.as_str() != boot {
        return Verdict::Rebooted;
    }
    match start_now {
        StartIdentity::NoProcess => Verdict::Gone,
        StartIdentity::Unknown => Verdict::Unknown,
        StartIdentity::Identity(start) if *start == rec.start_identity => Verdict::Same,
        // The recorded process itself has ended and waits to be reaped.
        StartIdentity::Zombie(start) if *start == rec.start_identity => Verdict::Gone,
        StartIdentity::Identity(_) | StartIdentity::Zombie(_) => Verdict::PidReused,
    }
}

/// What is known about the leader of a process group that may be stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leader {
    /// Its start identity, read while it was known to be the command.
    Known(String),
    /// It was the command and has ended (it was reaped); its identity was
    /// never read.
    Ended,
    /// Its identity could not be read, and it is not known to have ended.
    Unread,
}

impl Leader {
    /// What a read of the command's PID tells about the leader, when the
    /// command was started by the process `parent` (this one).
    ///
    /// The command is reaped by another thread, so by the time of the read
    /// its PID may already belong to another process. An identity is taken
    /// only from a process whose parent is `parent`: a process that got the
    /// PID after the command was reaped is not a child of ours. Anything
    /// else that has the PID leaves the leader unread.
    pub fn from_read(read: &ProcessRead, parent: u32) -> Leader {
        match &read.start {
            StartIdentity::Identity(start) | StartIdentity::Zombie(start)
                if read.ppid == Some(parent) =>
            {
                Leader::Known(start.clone())
            }
            StartIdentity::NoProcess => Leader::Ended,
            StartIdentity::Identity(_) | StartIdentity::Zombie(_) | StartIdentity::Unknown => {
                Leader::Unread
            }
        }
    }
}

/// Whose a process group is, as far as can be told right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOwner {
    /// The leader is the recorded process: the group is the command's and
    /// may be signalled.
    Ours,
    /// The leader has ended: nothing has its PID, or it is the recorded
    /// process waiting to be reaped. The number alone does not say whose the
    /// group is now (the PID may have been taken and left again by a process
    /// that started a group of its own), so the group may be probed, and is
    /// signalled only with evidence that a member is the command's
    /// ([`group_state`]).
    Orphaned,
    /// Another process has the leader's PID, so the command's group has no
    /// members left. Never signal.
    Foreign,
    /// It cannot be told. Never signal; look again later.
    Unknown,
}

/// Judge the group from what is known about its `leader` and what the system
/// says `now` about the leader's PID. `leads` is whether the group number is
/// the leader's PID; when it is not, the leader says nothing about the group.
///
/// This is the first gate in front of a signal to a recorded group, and it
/// is taken again immediately before every signal.
pub fn group_owner(leader: &Leader, now: &StartIdentity, leads: bool) -> GroupOwner {
    if !leads {
        return GroupOwner::Unknown;
    }
    match (now, leader) {
        (StartIdentity::Unknown, _) => GroupOwner::Unknown,
        (StartIdentity::NoProcess, _) => GroupOwner::Orphaned,
        (StartIdentity::Identity(start), Leader::Known(rec)) if start == rec => GroupOwner::Ours,
        (StartIdentity::Zombie(start), Leader::Known(rec)) if start == rec => GroupOwner::Orphaned,
        // A leader that was reaped cannot be the zombie that has its PID.
        (
            StartIdentity::Identity(_) | StartIdentity::Zombie(_),
            Leader::Known(_) | Leader::Ended,
        ) => GroupOwner::Foreign,
        // Something has the PID, and there is nothing to compare it with.
        (StartIdentity::Identity(_) | StartIdentity::Zombie(_), Leader::Unread) => {
            GroupOwner::Unknown
        }
    }
}

/// Whether a group still has members that are the command's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// It has members that are the command's.
    Alive,
    /// It has none: they ended, or the PID belongs to another process now
    /// (which is never signalled).
    Gone,
    /// The leader has ended and the group number has members, but none is
    /// known to be the command's. Never signal, never record a stop. (Right
    /// after a signal of ours these may be members that are still ending.)
    Unproven,
    /// It cannot be told. Never signal, never record a stop.
    Unknown,
}

/// The state of a group from its owner. A group whose leader has ended is
/// probed (`probe`: what signal 0 reaches); with members left it is the
/// command's only when `proven` says that one of them is. Without that
/// evidence it cannot be told, and is not signalled: a number is not an
/// identity.
pub fn group_state(
    owner: GroupOwner,
    probe: impl FnOnce() -> GroupSignal,
    proven: impl FnOnce() -> bool,
) -> GroupState {
    match owner {
        GroupOwner::Ours => GroupState::Alive,
        GroupOwner::Foreign => GroupState::Gone,
        GroupOwner::Unknown => GroupState::Unknown,
        GroupOwner::Orphaned => match probe() {
            GroupSignal::Empty => GroupState::Gone,
            GroupSignal::Delivered if proven() => GroupState::Alive,
            GroupSignal::Delivered => GroupState::Unproven,
            // Members that we may not signal at all are not ours to judge.
            GroupSignal::Denied => GroupState::Unknown,
        },
    }
}

/// For a group that cannot be told to be the command's: whether a stop goes
/// on looking (`Some(true)`: unproven members after a signal of that stop,
/// which may be what is still ending of the group) or ends at once
/// (`Some(false)`). `None` for a group that can be told.
pub fn waits_for(state: GroupState, signalled: bool) -> Option<bool> {
    match state {
        GroupState::Alive | GroupState::Gone => None,
        GroupState::Unproven => Some(signalled),
        GroupState::Unknown => Some(false),
    }
}

/// Whether a stop whose windows have passed leaves members that are known
/// to be the command's (it could not be confirmed), judged by the last
/// `state` seen. Otherwise what is left cannot be told to be the command's.
pub fn stop_was_ours(state: GroupState) -> bool {
    state == GroupState::Alive
}

/// Whether a process seen earlier as the command's (`recorded`: its start
/// time then) proves that the group `pgid` holds a process of the command:
/// it is in that group now (`group_now`) and, read after that, still has
/// the same start time (`start_after`), so the group read was of the same
/// process. A process whose start time was never learned proves nothing.
pub fn member_is_ours<T: PartialEq>(
    recorded: Option<&T>,
    group_now: Option<i32>,
    start_after: Option<&T>,
    pgid: i32,
) -> bool {
    pgid > 1 && recorded.is_some() && group_now == Some(pgid) && start_after == recorded
}

/// Whether an attempt is unresolved for no other reason than identities
/// that could not be told: `reasons` says of each reason whether it is one.
pub fn only_identity_unknown(reasons: impl IntoIterator<Item = bool>) -> bool {
    let mut any = false;
    for by_identity in reasons {
        if !by_identity {
            return false;
        }
        any = true;
    }
    any
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::WorkerBootId;

    fn rec() -> ProcessIdentity {
        ProcessIdentity {
            boot_id: WorkerBootId::parse("boot-1").unwrap(),
            pid: 4242,
            start_identity: "linux:1000".into(),
        }
    }

    fn id(s: &str) -> StartIdentity {
        StartIdentity::Identity(s.into())
    }

    fn reads() -> [StartIdentity; 3] {
        [
            id("linux:1000"),
            StartIdentity::NoProcess,
            StartIdentity::Unknown,
        ]
    }

    #[test]
    fn no_record_means_the_launch_is_unknown() {
        for now in reads() {
            assert_eq!(verdict(None, Some("boot-1"), &now), Verdict::NotRecorded);
        }
    }

    #[test]
    fn same_boot_same_start_is_the_same_process() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), &id("linux:1000")),
            Verdict::Same
        );
    }

    #[test]
    fn a_different_start_identity_is_a_reused_pid() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), &id("linux:2000")),
            Verdict::PidReused
        );
    }

    #[test]
    fn no_process_with_the_pid_means_gone() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), &StartIdentity::NoProcess),
            Verdict::Gone
        );
    }

    #[test]
    fn an_unreadable_identity_is_neither_gone_nor_the_same() {
        let r = rec();
        let v = verdict(Some(&r), Some("boot-1"), &StartIdentity::Unknown);
        assert_eq!(v, Verdict::Unknown);
        assert!(!v.is_gone());
    }

    #[test]
    fn another_boot_means_rebooted_whatever_the_pid_is_now() {
        let r = rec();
        // Even an identical start identity after a reboot is not the process,
        // and a read that failed does not matter: the boot settles it.
        for now in reads() {
            assert_eq!(verdict(Some(&r), Some("boot-2"), &now), Verdict::Rebooted);
        }
    }

    #[test]
    fn an_unreadable_boot_id_concludes_nothing() {
        let r = rec();
        for now in reads() {
            assert_eq!(verdict(Some(&r), None, &now), Verdict::CannotTell);
        }
        assert!(!Verdict::CannotTell.is_gone());
    }

    #[test]
    fn which_verdicts_are_gone() {
        assert!(Verdict::Rebooted.is_gone());
        assert!(Verdict::Gone.is_gone());
        assert!(Verdict::PidReused.is_gone());
        assert!(!Verdict::Same.is_gone());
        assert!(!Verdict::NotRecorded.is_gone());
        assert!(!Verdict::Unknown.is_gone());
    }

    fn zombie(s: &str) -> StartIdentity {
        StartIdentity::Zombie(s.into())
    }

    #[test]
    fn a_zombie_with_the_recorded_identity_is_gone_and_another_one_is_a_reused_pid() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), &zombie("linux:1000")),
            Verdict::Gone
        );
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), &zombie("linux:2000")),
            Verdict::PidReused
        );
    }

    const ME: u32 = 77;

    fn read(start: StartIdentity, ppid: Option<u32>) -> ProcessRead {
        ProcessRead { start, ppid }
    }

    #[test]
    fn a_read_of_the_command_says_what_its_leader_is_only_for_a_child_of_ours() {
        let known = Leader::Known("mac:1.000002".into());
        for start in [id("mac:1.000002"), zombie("mac:1.000002")] {
            assert_eq!(Leader::from_read(&read(start.clone(), Some(ME)), ME), known);
            // Another process's child, or one whose parent was not read, got
            // the PID after the command was reaped: its identity is not the
            // command's.
            for ppid in [Some(ME + 1), Some(1), None] {
                assert_eq!(
                    Leader::from_read(&read(start.clone(), ppid), ME),
                    Leader::Unread
                );
            }
        }
        for ppid in [Some(ME), None] {
            assert_eq!(
                Leader::from_read(&read(StartIdentity::NoProcess, ppid), ME),
                Leader::Ended
            );
            assert_eq!(
                Leader::from_read(&read(StartIdentity::Unknown, ppid), ME),
                Leader::Unread
            );
        }
    }

    fn owner(leader: &Leader, now: StartIdentity) -> GroupOwner {
        group_owner(leader, &now, true)
    }

    #[test]
    fn a_group_is_ours_only_while_its_leader_has_the_recorded_identity() {
        let known = Leader::Known("linux:1000".into());
        assert_eq!(owner(&known, id("linux:1000")), GroupOwner::Ours);
        assert_eq!(owner(&known, id("linux:2000")), GroupOwner::Foreign);
        assert_eq!(
            owner(&known, StartIdentity::NoProcess),
            GroupOwner::Orphaned
        );
        assert_eq!(owner(&known, StartIdentity::Unknown), GroupOwner::Unknown);
    }

    #[test]
    fn a_zombie_leader_is_ours_only_with_the_recorded_identity() {
        let known = Leader::Known("linux:1000".into());
        // The recorded leader, ended and not reaped: what it left is judged
        // like the group of a leader that is gone.
        assert_eq!(owner(&known, zombie("linux:1000")), GroupOwner::Orphaned);
        // A zombie of another process has the PID: the group is not ours.
        assert_eq!(owner(&known, zombie("linux:2000")), GroupOwner::Foreign);
        // Our leader was reaped, so a zombie with its PID is someone else's.
        assert_eq!(
            owner(&Leader::Ended, zombie("linux:1000")),
            GroupOwner::Foreign
        );
        assert_eq!(
            owner(&Leader::Unread, zombie("linux:1000")),
            GroupOwner::Unknown
        );
    }

    #[test]
    fn after_the_leader_ended_any_process_with_its_pid_is_another_one() {
        let ended = Leader::Ended;
        assert_eq!(owner(&ended, id("linux:1000")), GroupOwner::Foreign);
        assert_eq!(
            owner(&ended, StartIdentity::NoProcess),
            GroupOwner::Orphaned
        );
        assert_eq!(owner(&ended, StartIdentity::Unknown), GroupOwner::Unknown);
    }

    #[test]
    fn a_leader_that_was_never_read_is_judged_only_by_a_definite_absence() {
        let unread = Leader::Unread;
        assert_eq!(owner(&unread, id("linux:1000")), GroupOwner::Unknown);
        assert_eq!(
            owner(&unread, StartIdentity::NoProcess),
            GroupOwner::Orphaned
        );
        assert_eq!(owner(&unread, StartIdentity::Unknown), GroupOwner::Unknown);
    }

    #[test]
    fn a_group_the_leader_does_not_lead_is_never_judged_by_the_leader() {
        let known = Leader::Known("linux:1000".into());
        for leader in [&known, &Leader::Ended, &Leader::Unread] {
            for now in reads().into_iter().chain([zombie("linux:1000")]) {
                assert_eq!(group_owner(leader, &now, false), GroupOwner::Unknown);
            }
        }
    }

    /// The state, and whether the group was probed and evidence was asked.
    fn state(owner: GroupOwner, probe: GroupSignal, proven: bool) -> (GroupState, bool, bool) {
        let (probed, asked) = (std::cell::Cell::new(false), std::cell::Cell::new(false));
        let s = group_state(
            owner,
            || {
                probed.set(true);
                probe
            },
            || {
                asked.set(true);
                proven
            },
        );
        (s, probed.get(), asked.get())
    }

    #[test]
    fn a_group_with_a_known_owner_is_neither_probed_nor_asked_for_evidence() {
        for probe in [
            GroupSignal::Delivered,
            GroupSignal::Empty,
            GroupSignal::Denied,
        ] {
            for proven in [true, false] {
                assert_eq!(
                    state(GroupOwner::Ours, probe, proven),
                    (GroupState::Alive, false, false)
                );
                assert_eq!(
                    state(GroupOwner::Foreign, probe, proven),
                    (GroupState::Gone, false, false)
                );
                assert_eq!(
                    state(GroupOwner::Unknown, probe, proven),
                    (GroupState::Unknown, false, false)
                );
            }
        }
    }

    #[test]
    fn an_orphaned_group_is_alive_only_with_evidence_that_a_member_is_ours() {
        let orphaned = |probe, proven| state(GroupOwner::Orphaned, probe, proven);
        assert_eq!(
            orphaned(GroupSignal::Delivered, true),
            (GroupState::Alive, true, true)
        );
        // Members, but none known to be the command's: the number may be
        // another group's by now. Not signalled, and not gone.
        assert_eq!(
            orphaned(GroupSignal::Delivered, false),
            (GroupState::Unproven, true, true)
        );
        // No members: gone, whatever the evidence would say (it is not asked).
        for proven in [true, false] {
            assert_eq!(
                orphaned(GroupSignal::Empty, proven),
                (GroupState::Gone, true, false)
            );
            // Members we may not signal are not ours to judge.
            assert_eq!(
                orphaned(GroupSignal::Denied, proven),
                (GroupState::Unknown, true, false)
            );
        }
    }

    #[test]
    fn a_stop_waits_only_for_unproven_members_it_may_have_signalled() {
        assert_eq!(waits_for(GroupState::Alive, false), None);
        assert_eq!(waits_for(GroupState::Alive, true), None);
        assert_eq!(waits_for(GroupState::Gone, false), None);
        assert_eq!(waits_for(GroupState::Gone, true), None);
        assert_eq!(waits_for(GroupState::Unproven, true), Some(true));
        assert_eq!(waits_for(GroupState::Unproven, false), Some(false));
        assert_eq!(waits_for(GroupState::Unknown, true), Some(false));
        assert_eq!(waits_for(GroupState::Unknown, false), Some(false));
        assert!(stop_was_ours(GroupState::Alive));
        assert!(!stop_was_ours(GroupState::Unproven));
        assert!(!stop_was_ours(GroupState::Unknown));
        assert!(!stop_was_ours(GroupState::Gone));
    }

    #[test]
    fn a_member_proves_the_group_only_when_it_is_the_recorded_process_in_that_group() {
        let (then, other) = (Some(&1000_u64), Some(&2000_u64));
        assert!(member_is_ours(then, Some(4242), then, 4242));
        // In another group, or its group could not be read.
        assert!(!member_is_ours(then, Some(4243), then, 4242));
        assert!(!member_is_ours(then, None, then, 4242));
        // The PID belongs to another process by now, or to none.
        assert!(!member_is_ours(then, Some(4242), other, 4242));
        assert!(!member_is_ours(then, Some(4242), None, 4242));
        // A start time that was never learned proves nothing.
        assert!(!member_is_ours(None::<&u64>, Some(4242), None, 4242));
        assert!(!member_is_ours(None, Some(4242), then, 4242));
        // Never our own group, init, or every process.
        for pgid in [1, 0, -1] {
            assert!(!member_is_ours(then, Some(pgid), then, pgid));
        }
        assert!(member_is_ours(then, Some(2), then, 2));
    }

    #[test]
    fn only_reasons_that_are_all_about_identity_make_an_attempt_identity_unknown() {
        assert!(only_identity_unknown([true]));
        assert!(only_identity_unknown([true, true]));
        assert!(!only_identity_unknown([true, false]));
        assert!(!only_identity_unknown([false, true]));
        assert!(!only_identity_unknown([false]));
        assert!(!only_identity_unknown([]));
    }
}
