//! What a read of a PID says, decided from what the system returned.
//!
//! Nothing here asks the system anything: [`super::process`] makes the calls
//! and hands the raw answers to these functions, so the decisions can be
//! tested (and mutation-tested) on every platform without a process.

/// What the system says about the process that has a PID now.
///
/// The outcomes are kept apart on purpose. `NoProcess` is only a definite
/// "there is no such process"; every read that failed for another reason is
/// `Unknown`, and a caller that may signal must treat `Unknown` as "cannot
/// tell": never as gone and never as the recorded process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartIdentity {
    /// A live process has the PID, and this is its start identity.
    Identity(String),
    /// A process that has ended but was not reaped yet has the PID, and this
    /// is its start identity. The PID is still taken: it is not free, and
    /// whose it is can be told from the identity like that of a live one.
    Zombie(String),
    /// The system said that no process has the PID (it never existed, or
    /// has ended and was reaped).
    NoProcess,
    /// The read failed or made no sense: nothing is known.
    Unknown,
}

impl StartIdentity {
    /// The identity of a live process, when one was read. Not for deciding
    /// whether a process is gone: `None` is a zombie, `NoProcess` and
    /// `Unknown` alike.
    pub fn identity(&self) -> Option<&str> {
        match self {
            StartIdentity::Identity(s) => Some(s),
            StartIdentity::Zombie(_) | StartIdentity::NoProcess | StartIdentity::Unknown => None,
        }
    }
}

/// One read of a PID: its start identity and, from the same read, its
/// parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRead {
    pub start: StartIdentity,
    /// The parent's PID, when the read gave an identity.
    pub ppid: Option<u32>,
}

impl ProcessRead {
    /// A read that gave no parent.
    pub fn of(start: StartIdentity) -> ProcessRead {
        ProcessRead { start, ppid: None }
    }
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

/// What a read of `/proc/<pid>/stat` says: its text, or the OS error number
/// of the failed read. `proc_readable` tells whether `/proc` itself can be
/// read: a missing file means "no such process" only then (without a mounted
/// `/proc` every PID would look gone).
pub fn classify_proc_stat(
    read: Result<&str, Option<i32>>,
    proc_readable: &dyn Fn() -> bool,
) -> ProcessRead {
    match read {
        Ok(text) => parse_proc_stat(text),
        // ESRCH: the process ended between the open and the read.
        Err(Some(libc::ENOENT | libc::ESRCH)) if proc_readable() => {
            ProcessRead::of(StartIdentity::NoProcess)
        }
        Err(_) => ProcessRead::of(StartIdentity::Unknown),
    }
}

/// The start identity and the parent out of the text of `/proc/<pid>/stat`.
/// A zombie (`Z`) keeps its start time and its parent; a process that is
/// being removed (`X`) is `NoProcess`; a line that does not parse is
/// `Unknown`.
///
/// The command name is in parentheses and may itself contain spaces and
/// parentheses, so fields are counted from the last `)`.
pub fn parse_proc_stat(text: &str) -> ProcessRead {
    let unknown = ProcessRead::of(StartIdentity::Unknown);
    let Some(rest) = text.rfind(')').and_then(|i| text.get(i + 1..)) else {
        return unknown;
    };
    let mut fields = rest.split_whitespace();
    let zombie = match fields.next() {
        Some("X") => return ProcessRead::of(StartIdentity::NoProcess),
        Some(state) => state == "Z",
        None => return unknown,
    };
    let ppid = fields.next().and_then(|p| p.parse::<u32>().ok());
    // After the parent: pgrp session tty_nr tpgid flags minflt cminflt
    // majflt cmajflt utime stime cutime cstime priority nice num_threads
    // itrealvalue, then starttime (field 22 overall, the 18th after ppid).
    let start = match fields.nth(17) {
        Some(start) if start.parse::<u64>().is_ok() => format!("linux:{start}"),
        _ => return unknown,
    };
    if ppid.is_none() {
        return unknown;
    }
    let start = if zombie {
        StartIdentity::Zombie(start)
    } else {
        StartIdentity::Identity(start)
    };
    ProcessRead { start, ppid }
}

/// The size of the record `sysctl(KERN_PROC_PID)` fills on macOS (`struct
/// kinfo_proc` of a 64-bit system; the layout is part of the system's ABI).
pub const KINFO_PROC_SIZE: usize = 648;
/// `kp_proc.p_starttime.tv_sec` (64 bits), then `tv_usec` (32 bits).
const KINFO_START_SEC: usize = 0;
const KINFO_START_USEC: usize = 8;
/// `kp_proc.p_stat`.
const KINFO_STAT: usize = 36;
/// `kp_proc.p_pid`.
const KINFO_PID: usize = 40;
/// `kp_eproc.e_ppid`.
const KINFO_PPID: usize = 560;
/// `p_stat` of a process that has ended and waits to be reaped.
const SZOMB: u8 = 5;

fn i32_at(record: &[u8; KINFO_PROC_SIZE], at: usize) -> i32 {
    i32::from_ne_bytes(*record[at..].first_chunk().expect("the field is inside"))
}

/// What `sysctl(KERN_PROC_PID)` says about `pid`: `ok` is whether the call
/// succeeded, `len` the length it reported and `record` the buffer it
/// filled. A successful call that returned nothing is the system's "no such
/// process". A record of another length, of another PID, or without a start
/// time is `Unknown`. A zombie is reported with its start time (unlike
/// `proc_pidinfo`, which says "no such process" for one).
pub fn classify_kinfo(
    pid: u32,
    ok: bool,
    len: usize,
    record: &[u8; KINFO_PROC_SIZE],
) -> ProcessRead {
    let unknown = ProcessRead::of(StartIdentity::Unknown);
    if !ok {
        return unknown;
    }
    if len == 0 {
        return ProcessRead::of(StartIdentity::NoProcess);
    }
    if len != KINFO_PROC_SIZE || u32::try_from(i32_at(record, KINFO_PID)) != Ok(pid) {
        return unknown;
    }
    let sec = i64::from_ne_bytes(
        *record[KINFO_START_SEC..]
            .first_chunk()
            .expect("the field is inside"),
    );
    let usec = i32_at(record, KINFO_START_USEC);
    let ppid = u32::try_from(i32_at(record, KINFO_PPID)).ok();
    if sec <= 0 || usec < 0 || ppid.is_none() {
        return unknown;
    }
    let start = format!("mac:{sec}.{usec:06}");
    let start = if record[KINFO_STAT] == SZOMB {
        StartIdentity::Zombie(start)
    } else {
        StartIdentity::Identity(start)
    };
    ProcessRead { start, ppid }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVE: &str = "42 (we ird) name) S 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 18446744073709551615";

    fn id(s: &str) -> StartIdentity {
        StartIdentity::Identity(s.into())
    }

    fn zombie(s: &str) -> StartIdentity {
        StartIdentity::Zombie(s.into())
    }

    fn unknown() -> ProcessRead {
        ProcessRead::of(StartIdentity::Unknown)
    }

    fn none() -> ProcessRead {
        ProcessRead::of(StartIdentity::NoProcess)
    }

    fn read(start: StartIdentity, ppid: u32) -> ProcessRead {
        ProcessRead {
            start,
            ppid: Some(ppid),
        }
    }

    #[test]
    fn only_a_live_process_has_an_identity_to_compare() {
        assert_eq!(id("a").identity(), Some("a"));
        assert_eq!(zombie("a").identity(), None);
        assert_eq!(StartIdentity::NoProcess.identity(), None);
        assert_eq!(StartIdentity::Unknown.identity(), None);
        assert_eq!(ProcessRead::of(id("a")).ppid, None);
        assert_eq!(ProcessRead::of(id("a")).start, id("a"));
    }

    #[test]
    fn proc_stat_fields_are_counted_from_the_last_paren() {
        assert_eq!(parse_proc_stat(LIVE), read(id("linux:987654"), 7));
    }

    #[test]
    fn a_zombie_keeps_its_start_time_and_its_parent() {
        let z = "42 (x) Z 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(z), read(zombie("linux:987654"), 7));
        let dead = "42 (x) X 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10 0";
        assert_eq!(parse_proc_stat(dead), none());
    }

    #[test]
    fn a_stat_line_that_does_not_parse_is_unknown() {
        // A line that does not parse says nothing: never "gone".
        assert_eq!(parse_proc_stat("no parens"), unknown());
        assert_eq!(parse_proc_stat("42 (x)"), unknown());
        assert_eq!(parse_proc_stat("42 (x) S 1 2"), unknown());
        let nan = "42 (x) S 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 abc 1000 10 0";
        assert_eq!(parse_proc_stat(nan), unknown());
        let no_parent = "42 (x) S ? 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000";
        assert_eq!(parse_proc_stat(no_parent), unknown());
        // The field before the start time is not it.
        let short = "42 (x) S 7 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0";
        assert_eq!(parse_proc_stat(short), unknown());
    }

    #[test]
    fn a_failed_proc_read_is_no_process_only_for_a_missing_entry_under_a_readable_proc() {
        let yes: &dyn Fn() -> bool = &|| true;
        let no: &dyn Fn() -> bool = &|| false;
        assert_eq!(
            classify_proc_stat(Ok(LIVE), no),
            read(id("linux:987654"), 7)
        );
        for gone in [libc::ENOENT, libc::ESRCH] {
            assert_eq!(classify_proc_stat(Err(Some(gone)), yes), none());
            // `/proc` itself is not there: every PID would look gone.
            assert_eq!(classify_proc_stat(Err(Some(gone)), no), unknown());
        }
        for other in [
            libc::EPERM,
            libc::EACCES,
            libc::EMFILE,
            libc::ENFILE,
            libc::EIO,
        ] {
            assert_eq!(classify_proc_stat(Err(Some(other)), yes), unknown());
        }
        assert_eq!(classify_proc_stat(Err(None), yes), unknown());
    }

    const PID: u32 = 0x0102_0304;
    const PPID: u32 = 0x0a0b_0c0d;
    const SEC: i64 = 0x1122_3344_5566;
    const USEC: i32 = 0x0001_0203;

    /// A record as the system fills it, with every other byte set so that a
    /// field read from the wrong place is not the expected value.
    fn record(pid: u32, ppid: i32, sec: i64, usec: i32, stat: u8) -> [u8; KINFO_PROC_SIZE] {
        let mut r = [0xee_u8; KINFO_PROC_SIZE];
        r[0..8].copy_from_slice(&sec.to_ne_bytes());
        r[8..12].copy_from_slice(&usec.to_ne_bytes());
        r[36] = stat;
        r[40..44].copy_from_slice(&(pid as i32).to_ne_bytes());
        r[560..564].copy_from_slice(&ppid.to_ne_bytes());
        r
    }

    fn kinfo(r: &[u8; KINFO_PROC_SIZE]) -> ProcessRead {
        classify_kinfo(PID, true, KINFO_PROC_SIZE, r)
    }

    #[test]
    fn a_kinfo_record_gives_the_start_time_and_the_parent() {
        let live = record(PID, PPID as i32, SEC, USEC, 2);
        assert_eq!(kinfo(&live), read(id("mac:18838586676582.066051"), PPID));
        // Microseconds are padded, and zero of them is a time like any other.
        let padded = record(PID, PPID as i32, 1_700_000_000, 42, 2);
        assert_eq!(kinfo(&padded), read(id("mac:1700000000.000042"), PPID));
        let whole = record(PID, 0, 1, 0, 2);
        assert_eq!(kinfo(&whole), read(id("mac:1.000000"), 0));
    }

    #[test]
    fn a_kinfo_zombie_keeps_its_start_time_and_its_parent() {
        let z = record(PID, PPID as i32, 1_700_000_000, 42, 5);
        assert_eq!(kinfo(&z), read(zombie("mac:1700000000.000042"), PPID));
        // Only the zombie state is one: every other state is a live process.
        for stat in [0, 1, 2, 3, 4, 6] {
            let live = record(PID, PPID as i32, 1_700_000_000, 42, stat);
            assert_eq!(kinfo(&live), read(id("mac:1700000000.000042"), PPID));
        }
    }

    #[test]
    fn a_kinfo_answer_is_no_process_only_when_the_call_returned_nothing() {
        let live = record(PID, PPID as i32, SEC, USEC, 2);
        assert_eq!(classify_kinfo(PID, true, 0, &live), none());
        // A failed call says nothing, whatever length it left behind.
        assert_eq!(classify_kinfo(PID, false, 0, &live), unknown());
        assert_eq!(
            classify_kinfo(PID, false, KINFO_PROC_SIZE, &live),
            unknown()
        );
        // A record of another length is not an answer.
        assert_eq!(classify_kinfo(PID, true, 1, &live), unknown());
        assert_eq!(
            classify_kinfo(PID, true, KINFO_PROC_SIZE - 1, &live),
            unknown()
        );
        assert_eq!(
            classify_kinfo(PID, true, KINFO_PROC_SIZE + 1, &live),
            unknown()
        );
    }

    #[test]
    fn a_kinfo_record_that_makes_no_sense_is_unknown() {
        // Of another PID, or of none.
        let live = record(PID, PPID as i32, SEC, USEC, 2);
        assert_eq!(
            classify_kinfo(PID + 1, true, KINFO_PROC_SIZE, &live),
            unknown()
        );
        assert_eq!(
            kinfo(&record(u32::MAX, PPID as i32, SEC, USEC, 2)),
            unknown()
        );
        // Without a start time, or with one that cannot be.
        assert_eq!(kinfo(&record(PID, PPID as i32, 0, USEC, 2)), unknown());
        assert_eq!(kinfo(&record(PID, PPID as i32, -1, USEC, 2)), unknown());
        assert_eq!(kinfo(&record(PID, PPID as i32, SEC, -1, 2)), unknown());
        // Without a parent that can be one.
        assert_eq!(kinfo(&record(PID, -1, SEC, USEC, 2)), unknown());
    }
}
