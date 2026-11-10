//! Telling whether a recorded process is still the same process (spec
//! chapter 12, 15). A PID is not an identity: it is reused, and a reboot
//! recycles every one of them. A recorded process is `(boot ID, pid, start
//! identity)`, and this module compares that record with what the system
//! says now.

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
}

impl Verdict {
    /// True when the recorded process is known not to exist.
    pub fn is_gone(self) -> bool {
        matches!(self, Verdict::Rebooted | Verdict::Gone | Verdict::PidReused)
    }
}

/// Compare the `recorded` identity with the current boot and with the start
/// identity of whatever process has that PID now (`None`: no live process).
pub fn verdict(
    recorded: Option<&ProcessIdentity>,
    boot_now: Option<&str>,
    start_now: Option<&str>,
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
        None => Verdict::Gone,
        Some(start) if start == rec.start_identity => Verdict::Same,
        Some(_) => Verdict::PidReused,
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

    #[test]
    fn no_record_means_the_launch_is_unknown() {
        assert_eq!(
            verdict(None, Some("boot-1"), Some("linux:1000")),
            Verdict::NotRecorded
        );
    }

    #[test]
    fn same_boot_same_start_is_the_same_process() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), Some("linux:1000")),
            Verdict::Same
        );
    }

    #[test]
    fn a_different_start_identity_is_a_reused_pid() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), Some("boot-1"), Some("linux:2000")),
            Verdict::PidReused
        );
    }

    #[test]
    fn no_process_with_the_pid_means_gone() {
        let r = rec();
        assert_eq!(verdict(Some(&r), Some("boot-1"), None), Verdict::Gone);
    }

    #[test]
    fn another_boot_means_rebooted_whatever_the_pid_is_now() {
        let r = rec();
        // Even an identical start identity after a reboot is not the process.
        assert_eq!(
            verdict(Some(&r), Some("boot-2"), Some("linux:1000")),
            Verdict::Rebooted
        );
        assert_eq!(verdict(Some(&r), Some("boot-2"), None), Verdict::Rebooted);
    }

    #[test]
    fn an_unreadable_boot_id_concludes_nothing() {
        let r = rec();
        assert_eq!(
            verdict(Some(&r), None, Some("linux:1000")),
            Verdict::CannotTell
        );
        assert!(!Verdict::CannotTell.is_gone());
    }

    #[test]
    fn which_verdicts_are_gone() {
        assert!(Verdict::Rebooted.is_gone());
        assert!(Verdict::Gone.is_gone());
        assert!(Verdict::PidReused.is_gone());
        assert!(!Verdict::Same.is_gone());
        assert!(!Verdict::NotRecorded.is_gone());
    }
}
