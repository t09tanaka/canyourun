//! Process identity and signalling.
//!
//! A PID alone is reused, so a recorded process is `(boot ID, pid, start
//! identity)`. The start identity is an opaque string that is equal for the
//! same process and (practically) never equal for a later process that got
//! the same PID.

/// The start identity of `pid`, or `None` when no live, non-zombie process
/// has that PID (or the OS does not tell).
///
/// Linux: the `starttime` field of `/proc/<pid>/stat` (clock ticks since
/// boot). macOS: `proc_pidinfo(PROC_PIDTBSDINFO)` start seconds and
/// microseconds.
pub fn process_start_identity(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        parse_proc_stat(&text)
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
        if got != size || info.pbi_status == libc::SZOMB || info.pbi_start_tvsec == 0 {
            return None;
        }
        Some(format!(
            "mac:{}.{:06}",
            info.pbi_start_tvsec, info.pbi_start_tvusec
        ))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        None
    }
}

/// The start identity out of the text of `/proc/<pid>/stat`. `None` for a
/// zombie (it has exited) or a line that does not parse.
///
/// The command name is in parentheses and may itself contain spaces and
/// parentheses, so fields are counted from the last `)`.
pub fn parse_proc_stat(text: &str) -> Option<String> {
    let rest = text.get(text.rfind(')')? + 1..)?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?;
    if state == "Z" || state == "X" {
        return None;
    }
    // After the state: ppid pgrp session tty_nr tpgid flags minflt cminflt
    // majflt cmajflt utime stime cutime cstime priority nice num_threads
    // itrealvalue, then starttime (field 22 overall, the 19th after state).
    let start = fields.nth(18)?;
    start.parse::<u64>().ok()?;
    Some(format!("linux:{start}"))
}

/// Send `sig` to `pid` alone. True when the signal was delivered (or the
/// process is a zombie); false when there is no such process or no right.
pub fn signal_pid(pid: u32, sig: i32) -> bool {
    // SAFETY: plain syscall with integer arguments.
    unsafe { libc::kill(pid as libc::pid_t, sig) == 0 }
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
    fn proc_stat_start_time_is_counted_from_the_last_paren() {
        let line = "42 (we ird) name) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 18446744073709551615";
        assert_eq!(parse_proc_stat(line).as_deref(), Some("linux:987654"));
    }

    #[test]
    fn proc_stat_zombie_and_garbage_have_no_identity() {
        let z = "42 (x) Z 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(z), None);
        let dead = "42 (x) X 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(dead), None);
        assert_eq!(parse_proc_stat("no parens"), None);
        assert_eq!(parse_proc_stat("42 (x) S 1 2"), None);
        let nan = "42 (x) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 abc 1000 10 0";
        assert_eq!(parse_proc_stat(nan), None);
    }

    #[test]
    fn own_identity_is_stable_and_a_dead_pid_has_none() {
        let me = std::process::id();
        let a = process_start_identity(me).expect("this process has an identity");
        assert_eq!(process_start_identity(me).unwrap(), a);
        // Another live process differs from ours.
        let mut child = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let theirs = process_start_identity(child.id()).expect("child identity");
        assert_ne!(theirs, a);
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(process_start_identity(child.id()), None);
    }

    #[test]
    fn group_signals_report_what_they_reached() {
        assert_eq!(signal_process_group(0, 0), GroupSignal::Empty);
        assert_eq!(signal_process_group(1, 0), GroupSignal::Empty);
        assert_eq!(signal_process_group(i32::MAX - 1, 0), GroupSignal::Empty);
        assert!(signal_pid(std::process::id(), 0));
    }
}
