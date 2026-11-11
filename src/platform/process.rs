//! Process identity and signalling.
//!
//! A PID alone is reused, so a recorded process is `(boot ID, pid, start
//! identity)`. The start identity is an opaque string that is equal for the
//! same process and (practically) never equal for a later process that got
//! the same PID.

/// What the system says about the process that has a PID now.
///
/// The three outcomes are kept apart on purpose. `NoProcess` is only a
/// definite "there is no such process"; every read that failed for another
/// reason is `Unknown`, and a caller that may signal must treat `Unknown` as
/// "cannot tell": never as gone and never as the recorded process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartIdentity {
    /// A live process has the PID, and this is its start identity.
    Identity(String),
    /// The system said that no live process has the PID (it never existed,
    /// has ended, or is a zombie that only waits to be reaped).
    NoProcess,
    /// The read failed or made no sense: nothing is known.
    Unknown,
}

impl StartIdentity {
    /// The identity, when one was read. Not for deciding whether a process
    /// is gone: `None` is both `NoProcess` and `Unknown`.
    pub fn identity(&self) -> Option<&str> {
        match self {
            StartIdentity::Identity(s) => Some(s),
            StartIdentity::NoProcess | StartIdentity::Unknown => None,
        }
    }
}

/// The start identity of `pid`.
///
/// Linux: the `starttime` field of `/proc/<pid>/stat` (clock ticks since
/// boot). macOS: `proc_pidinfo(PROC_PIDTBSDINFO)` start seconds and
/// microseconds.
pub fn process_start_identity(pid: u32) -> StartIdentity {
    if libc::pid_t::try_from(pid).is_err() {
        // Not a PID the system hands out; say nothing about it.
        return StartIdentity::Unknown;
    }
    #[cfg(target_os = "linux")]
    {
        let read = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        classify_proc_stat(
            read.as_deref().map_err(std::io::Error::raw_os_error),
            || std::fs::metadata("/proc/self/stat").is_ok(),
        )
    }
    #[cfg(target_os = "macos")]
    {
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
        // SAFETY: `info` is a valid buffer of `size` bytes.
        let got = unsafe {
            libc::proc_pidinfo(
                pid as libc::c_int,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size,
            )
        };
        // Read at once: the next call may change it.
        let errno = std::io::Error::last_os_error().raw_os_error();
        classify_pidinfo(
            got,
            size,
            errno,
            info.pbi_status == libc::SZOMB,
            info.pbi_start_tvsec,
            info.pbi_start_tvusec,
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        StartIdentity::Unknown
    }
}

/// What a read of `/proc/<pid>/stat` says: its text, or the OS error number
/// of the failed read. `proc_readable` tells whether `/proc` itself can be
/// read: a missing file means "no such process" only then (without a mounted
/// `/proc` every PID would look gone).
pub fn classify_proc_stat(
    read: Result<&str, Option<i32>>,
    proc_readable: impl Fn() -> bool,
) -> StartIdentity {
    match read {
        Ok(text) => parse_proc_stat(text),
        // ESRCH: the process ended between the open and the read.
        Err(Some(libc::ENOENT | libc::ESRCH)) if proc_readable() => StartIdentity::NoProcess,
        Err(_) => StartIdentity::Unknown,
    }
}

/// The start identity out of the text of `/proc/<pid>/stat`. `NoProcess` for
/// a zombie (it has exited), `Unknown` for a line that does not parse.
///
/// The command name is in parentheses and may itself contain spaces and
/// parentheses, so fields are counted from the last `)`.
pub fn parse_proc_stat(text: &str) -> StartIdentity {
    let Some(rest) = text.rfind(')').and_then(|i| text.get(i + 1..)) else {
        return StartIdentity::Unknown;
    };
    let mut fields = rest.split_whitespace();
    match fields.next() {
        Some("Z" | "X") => return StartIdentity::NoProcess,
        Some(_) => {}
        None => return StartIdentity::Unknown,
    }
    // After the state: ppid pgrp session tty_nr tpgid flags minflt cminflt
    // majflt cmajflt utime stime cutime cstime priority nice num_threads
    // itrealvalue, then starttime (field 22 overall, the 19th after state).
    match fields.nth(18) {
        Some(start) if start.parse::<u64>().is_ok() => {
            StartIdentity::Identity(format!("linux:{start}"))
        }
        _ => StartIdentity::Unknown,
    }
}

/// What `proc_pidinfo(PROC_PIDTBSDINFO)` says: `got` is its return value
/// (`size` when the record was filled), `errno` the error number read right
/// after it. Only `ESRCH` is "no such process"; a short record, another
/// error or a start time of zero is `Unknown`.
pub fn classify_pidinfo(
    got: i32,
    size: i32,
    errno: Option<i32>,
    zombie: bool,
    start_sec: u64,
    start_usec: u64,
) -> StartIdentity {
    if got != size {
        return if got <= 0 && errno == Some(libc::ESRCH) {
            StartIdentity::NoProcess
        } else {
            StartIdentity::Unknown
        };
    }
    if zombie {
        return StartIdentity::NoProcess;
    }
    if start_sec == 0 {
        return StartIdentity::Unknown;
    }
    StartIdentity::Identity(format!("mac:{start_sec}.{start_usec:06}"))
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

/// What a signal to a process group reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSignal {
    /// At least one member got it (`sig == 0`: the group has members).
    Delivered,
    /// The group has no members.
    Empty,
    /// The group exists but belongs to someone we cannot signal.
    Denied,
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

    const LIVE: &str = "42 (we ird) name) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 18446744073709551615";

    fn id(s: &str) -> StartIdentity {
        StartIdentity::Identity(s.into())
    }

    #[test]
    fn proc_stat_start_time_is_counted_from_the_last_paren() {
        assert_eq!(parse_proc_stat(LIVE), id("linux:987654"));
    }

    #[test]
    fn a_zombie_is_no_process_and_garbage_is_unknown() {
        let z = "42 (x) Z 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(z), StartIdentity::NoProcess);
        let dead = "42 (x) X 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(dead), StartIdentity::NoProcess);
        // A line that does not parse says nothing: never "gone".
        assert_eq!(parse_proc_stat("no parens"), StartIdentity::Unknown);
        assert_eq!(parse_proc_stat("42 (x)"), StartIdentity::Unknown);
        assert_eq!(parse_proc_stat("42 (x) S 1 2"), StartIdentity::Unknown);
        let nan = "42 (x) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 abc 1000 10 0";
        assert_eq!(parse_proc_stat(nan), StartIdentity::Unknown);
    }

    #[test]
    fn a_failed_proc_read_is_no_process_only_for_a_missing_entry_under_a_readable_proc() {
        let yes = || true;
        let no = || false;
        assert_eq!(classify_proc_stat(Ok(LIVE), no), id("linux:987654"));
        for gone in [libc::ENOENT, libc::ESRCH] {
            assert_eq!(
                classify_proc_stat(Err(Some(gone)), yes),
                StartIdentity::NoProcess
            );
            // `/proc` itself is not there: every PID would look gone.
            assert_eq!(
                classify_proc_stat(Err(Some(gone)), no),
                StartIdentity::Unknown
            );
        }
        for other in [
            libc::EPERM,
            libc::EACCES,
            libc::EMFILE,
            libc::ENFILE,
            libc::EIO,
        ] {
            assert_eq!(
                classify_proc_stat(Err(Some(other)), yes),
                StartIdentity::Unknown
            );
        }
        assert_eq!(classify_proc_stat(Err(None), yes), StartIdentity::Unknown);
    }

    #[test]
    fn pidinfo_is_no_process_only_for_esrch_or_a_zombie() {
        let ok = |z, sec| classify_pidinfo(136, 136, None, z, sec, 42);
        assert_eq!(ok(false, 1_700_000_000), id("mac:1700000000.000042"));
        assert_eq!(ok(true, 1_700_000_000), StartIdentity::NoProcess);
        assert_eq!(ok(false, 0), StartIdentity::Unknown);
        let failed = |got, errno| classify_pidinfo(got, 136, errno, false, 0, 0);
        assert_eq!(failed(0, Some(libc::ESRCH)), StartIdentity::NoProcess);
        assert_eq!(failed(-1, Some(libc::ESRCH)), StartIdentity::NoProcess);
        assert_eq!(failed(0, Some(libc::EPERM)), StartIdentity::Unknown);
        assert_eq!(failed(0, Some(libc::EMFILE)), StartIdentity::Unknown);
        assert_eq!(failed(0, None), StartIdentity::Unknown);
        // A short record is not an answer, whatever errno was left behind.
        assert_eq!(failed(64, Some(libc::ESRCH)), StartIdentity::Unknown);
        assert_eq!(failed(64, None), StartIdentity::Unknown);
    }

    #[test]
    fn own_identity_is_stable_and_an_impossible_pid_is_unknown() {
        let me = std::process::id();
        let a = process_start_identity(me);
        assert!(a.identity().is_some(), "{a:?}");
        assert_eq!(process_start_identity(me), a);
        assert_eq!(StartIdentity::NoProcess.identity(), None);
        assert_eq!(StartIdentity::Unknown.identity(), None);
        assert_eq!(process_start_identity(u32::MAX), StartIdentity::Unknown);
        assert_eq!(
            process_start_identity(i32::MAX as u32 + 1),
            StartIdentity::Unknown
        );
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
