//! Process identity and signalling.
//!
//! A PID alone is reused, so a recorded process is `(boot ID, pid, start
//! identity)`. The start identity is an opaque string that is equal for the
//! same process and (practically) never equal for a later process that got
//! the same PID.
//!
//! This file makes the system calls; what their answers mean is decided in
//! [`super::process_read`].

pub use super::process_read::{GroupSignal, ProcessRead, StartIdentity};

/// The start identity of `pid` ([`read_process`] without the parent).
pub fn process_start_identity(pid: u32) -> StartIdentity {
    read_process(pid).start
}

/// One read of `pid`: its start identity and its parent.
///
/// Linux: the `starttime` and `ppid` fields of `/proc/<pid>/stat` (clock
/// ticks since boot). macOS: `sysctl(KERN_PROC_PID)` start seconds and
/// microseconds and the parent. Both report a zombie with its start time.
pub fn read_process(pid: u32) -> ProcessRead {
    if libc::pid_t::try_from(pid).is_err() {
        // Not a PID the system hands out; say nothing about it.
        return ProcessRead::of(StartIdentity::Unknown);
    }
    #[cfg(target_os = "linux")]
    {
        let read = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        super::process_read::classify_proc_stat(
            read.as_deref().map_err(std::io::Error::raw_os_error),
            &|| std::fs::metadata("/proc/self/stat").is_ok(),
        )
    }
    #[cfg(target_os = "macos")]
    {
        let mut record = [0u8; super::process_read::KINFO_PROC_SIZE];
        let mut len = record.len();
        let mut mib = [
            libc::CTL_KERN,
            libc::KERN_PROC,
            libc::KERN_PROC_PID,
            pid as libc::c_int,
        ];
        // SAFETY: `record` is a valid buffer of `len` bytes and `mib` has
        // the four names passed.
        let rc = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as libc::c_uint,
                record.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        super::process_read::classify_kinfo(pid, rc == 0, len, &record)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        ProcessRead::of(StartIdentity::Unknown)
    }
}

/// The process group `pid` is in now, when the system says. Asks only; no
/// signal is involved.
pub fn process_group_of(pid: u32) -> Option<i32> {
    let p = libc::pid_t::try_from(pid).ok().filter(|p| *p > 1)?;
    // SAFETY: plain syscall with an integer argument.
    let pgid = unsafe { libc::getpgid(p) };
    (pgid > 1).then_some(pgid)
}

/// Send `sig` to `pid` alone. True when the signal was delivered (or the
/// process is a zombie); false when there is no such process or no right.
#[allow(clippy::disallowed_methods)] // the one guarded way to kill()
pub fn signal_pid(pid: u32, sig: i32) -> bool {
    // pid 0 is our own group, 1 is init, and a pid above i32::MAX wraps to a
    // negative pid_t: -1 would reach every process of the user. Never.
    let Ok(p) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if p <= 1 {
        return false;
    }
    // SAFETY: plain syscall with integer arguments.
    unsafe { libc::kill(p, sig) == 0 }
}

/// Send `sig` to every member of the group `pgid`. Signal 0 only probes.
#[allow(clippy::disallowed_methods)] // the one guarded way to killpg()
pub fn signal_process_group(pgid: i32, sig: i32) -> GroupSignal {
    if pgid <= 1 {
        // Group 0/1 would hit our own group or init: never.
        return GroupSignal::Empty;
    }
    // SAFETY: plain syscall with integer arguments.
    if unsafe { libc::killpg(pgid, sig) } == 0 {
        return GroupSignal::Delivered;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::EPERM) => GroupSignal::Denied,
        _ => GroupSignal::Empty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_identity_is_stable_and_an_impossible_pid_is_unknown() {
        let me = std::process::id();
        let a = read_process(me);
        assert!(a.start.identity().is_some(), "{a:?}");
        assert_eq!(read_process(me), a);
        assert_eq!(process_start_identity(me), a.start);
        // SAFETY: plain syscall without arguments.
        assert_eq!(a.ppid, Some(unsafe { libc::getppid() } as u32));
        assert_eq!(process_start_identity(u32::MAX), StartIdentity::Unknown);
        assert_eq!(
            process_start_identity(i32::MAX as u32 + 1),
            StartIdentity::Unknown
        );
        // A PID that can be asked about and that nothing has.
        assert_eq!(
            process_start_identity(i32::MAX as u32 - 1),
            StartIdentity::NoProcess
        );
    }

    /// The identity recorded by earlier versions came from `proc_pidinfo`:
    /// the record read now must give the same one, or every recorded
    /// process would look like a reused PID.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_start_identity_is_the_one_proc_pidinfo_gives() {
        let me = std::process::id();
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
        // SAFETY: `info` is a valid buffer of `size` bytes.
        let got = unsafe {
            libc::proc_pidinfo(
                me as libc::c_int,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size,
            )
        };
        assert_eq!(got, size);
        let read = read_process(me);
        assert_eq!(
            read.start,
            StartIdentity::Identity(format!(
                "mac:{}.{:06}",
                info.pbi_start_tvsec, info.pbi_start_tvusec
            ))
        );
        assert_eq!(read.ppid, Some(info.pbi_ppid));
    }

    #[test]
    fn the_group_of_a_process_is_asked_only_for_a_pid_that_can_be_one() {
        // SAFETY: plain syscall without arguments.
        let mine = unsafe { libc::getpgrp() };
        assert_eq!(process_group_of(std::process::id()), Some(mine));
        assert_eq!(process_group_of(0), None);
        assert_eq!(process_group_of(1), None);
        assert_eq!(process_group_of(u32::MAX), None);
        assert_eq!(process_group_of(i32::MAX as u32 - 1), None);
    }

    #[test]
    fn group_signals_report_what_they_reached() {
        assert_eq!(signal_process_group(0, 0), GroupSignal::Empty);
        assert_eq!(signal_process_group(1, 0), GroupSignal::Empty);
        assert_eq!(signal_process_group(i32::MAX - 1, 0), GroupSignal::Empty);
        // Never our own group, init, or a pid that wraps to kill(-1).
        assert!(!signal_pid(0, 0));
        assert!(!signal_pid(1, 0));
        assert!(!signal_pid(u32::MAX, 0));
        assert!(!signal_pid(i32::MAX as u32 + 1, 0));
        assert!(signal_pid(std::process::id(), 0));
    }
}
