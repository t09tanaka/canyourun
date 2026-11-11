//! Telling whether a recorded process is still the same process (spec
//! chapter 12, 15). A PID is not an identity: it is reused, and a reboot
//! recycles every one of them. A recorded process is `(boot ID, pid, start
//! identity)`, and this module compares that record with what the system
//! says now.

//!
//! The system's answer about a PID is three-way
//! ([`crate::platform::StartIdentity`]): an identity, a definite "no such
//! process", or "could not be read". The last one is never folded into
//! either of the others: a process whose identity is unknown is never
//! signalled, and is never taken to be gone.

use crate::platform::StartIdentity;
use crate::storage::ProcessIdentity;

/// What the recorded launch looks like now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A launch intent exists but the process was never recorded: whether a
    /// child was started is unknown. Never respawn; investigate.
    NotRecorded,
    /// The machine restarted since the launch: the process is gone.
    Rebooted,
    /// The same boot and the PID is free: the process is gone.
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
        StartIdentity::Identity(_) => Verdict::PidReused,
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
    /// What a read of the command's PID, taken while that PID could only be
    /// the command, tells about the leader.
    pub fn from_read(read: StartIdentity) -> Leader {
        match read {
            StartIdentity::Identity(start) => Leader::Known(start),
            StartIdentity::NoProcess => Leader::Ended,
            StartIdentity::Unknown => Leader::Unread,
        }
    }
}

/// Whose a process group is, as far as can be told right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOwner {
    /// The leader is the recorded process: the group is the command's and
    /// may be signalled.
    Ours,
    /// The leader has ended and nothing else has its PID. A group number is
    /// not handed out again while the group has members, so members that
    /// remain are what the command left: the group may be probed, and
    /// signalled when it has members.
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
/// This is the only gate in front of a signal to a recorded group, and it is
/// taken again immediately before every signal.
pub fn group_owner(leader: &Leader, now: &StartIdentity, leads: bool) -> GroupOwner {
    if !leads {
        return GroupOwner::Unknown;
    }
    match (now, leader) {
        (StartIdentity::Unknown, _) => GroupOwner::Unknown,
        (StartIdentity::NoProcess, _) => GroupOwner::Orphaned,
        (StartIdentity::Identity(start), Leader::Known(rec)) if start == rec => GroupOwner::Ours,
        (StartIdentity::Identity(_), Leader::Known(_) | Leader::Ended) => GroupOwner::Foreign,
        // Something has the PID, and there is nothing to compare it with.
        (StartIdentity::Identity(_), Leader::Unread) => GroupOwner::Unknown,
    }
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

    #[test]
    fn a_read_of_the_command_says_what_its_leader_is() {
        assert_eq!(
            Leader::from_read(id("mac:1.000002")),
            Leader::Known("mac:1.000002".into())
        );
        assert_eq!(Leader::from_read(StartIdentity::NoProcess), Leader::Ended);
        assert_eq!(Leader::from_read(StartIdentity::Unknown), Leader::Unread);
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
            for now in reads() {
                assert_eq!(group_owner(leader, &now, false), GroupOwner::Unknown);
            }
        }
    }
}
