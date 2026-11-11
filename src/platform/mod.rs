//! OS-dependent primitives (spec chapter 15): a monotonic clock that counts
//! suspend, the boot ID, process identity, and sleep inhibition.
//!
//! macOS and Linux each have an implementation behind the same functions, so
//! the supervisor and the worker agent keep one contract on both.

pub mod boot;
pub mod clock;
pub mod inhibit;
pub mod process;

pub use boot::boot_id;
pub use clock::{monotonic_ms, wall_ms};
pub use inhibit::{Inhibit, SleepGuard};
pub use process::{
    process_start_identity, signal_pid, signal_process_group, GroupSignal, StartIdentity,
};

/// Stop this process from leaving a core dump (and, on Linux, from being
/// attached to by another process of the same user): a dump of a process that
/// holds secret files, or the cluster key, would write them to disk.
/// `RLIMIT_CORE` is set to 0 (soft and hard, so it cannot be raised again) on
/// macOS and Linux; Linux also clears `PR_SET_DUMPABLE`.
///
/// This changes the whole process, so it is for the supervisor and the worker
/// agent to call once at start (S10), not for a library function or a test
/// that shares a process.
pub fn disable_core_dumps() -> std::io::Result<()> {
    use rustix::process::{setrlimit, Resource, Rlimit};
    let zero = Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    setrlimit(Resource::Core, zero)?;
    #[cfg(target_os = "linux")]
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)?;
    Ok(())
}

/// The soft and hard `RLIMIT_CORE` of this process.
pub fn core_limit() -> (Option<u64>, Option<u64>) {
    let l = rustix::process::getrlimit(rustix::process::Resource::Core);
    (l.current, l.maximum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    /// Runs in a child process only (it changes the process): see
    /// `core_dumps_are_disabled_for_the_process_that_asks`.
    #[test]
    #[ignore = "runs in a child process"]
    fn child_disables_core_dumps() {
        disable_core_dumps().unwrap();
        assert_eq!(core_limit(), (Some(0), Some(0)));
        // idempotent, and the limit cannot be raised again
        disable_core_dumps().unwrap();
        assert!(rustix::process::setrlimit(
            rustix::process::Resource::Core,
            rustix::process::Rlimit {
                current: Some(1),
                maximum: Some(1)
            }
        )
        .is_err());
        #[cfg(target_os = "linux")]
        assert_eq!(
            rustix::process::dumpable_behavior().unwrap(),
            rustix::process::DumpableBehavior::NotDumpable
        );
    }

    #[test]
    fn core_dumps_are_disabled_for_the_process_that_asks() {
        let exe = std::env::current_exe().unwrap();
        let out = Command::new(exe)
            .args([
                "--ignored",
                "--exact",
                "platform::tests::child_disables_core_dumps",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
