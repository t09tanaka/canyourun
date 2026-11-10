//! The decisions of the log spool that need no I/O (spec chapter 13, A29).
//!
//! A stream is two files: `<stream>.log` holds the raw bytes, `<stream>.idx`
//! is an append-only list of 16-byte records, one per append. A record is the
//! stream's state after that append: `end` (bytes stored, contiguous from 0)
//! and `dropped` (bytes that did not fit under the limit). The number of
//! records is the stream's sequence number. The last record is what readers
//! trust; bytes of the log file past it are not yet published.

use crate::ids::{AttemptId, Digest};
use crate::state::Termination;
use crate::wire::ArtifactRecord;
use serde_json::{json, Value};
use std::path::PathBuf;

pub const RECORD_LEN: usize = 16;
/// The largest answer to one read, whatever the caller asked for.
pub const MAX_READ_BYTES: u64 = 8 << 20;
/// Unrecorded drops are written out when they reach this many bytes, so a
/// flood after the limit does not grow the index without bound.
pub const DROP_RECORD_STEP: u64 = 1 << 20;
/// Stored bytes that may wait without an index record. The log bytes are
/// written at once; only the record that publishes them to a reader is held
/// back until this much has accumulated (or the writer syncs or finishes), so
/// the index stays at most `RECORD_LEN` per `MIN_RECORD_BYTES` of log however
/// small the producer's writes are.
pub const MIN_RECORD_BYTES: u64 = 4096;
const MAX_NAME_BYTES: usize = 1024;
const MAX_COMPONENT_BYTES: usize = 255;
const MAX_DEPTH: usize = 16;

/// The state of one stream after an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Mark {
    pub end: u64,
    pub dropped: u64,
}

impl Mark {
    pub fn encode(self) -> [u8; RECORD_LEN] {
        let mut b = [0u8; RECORD_LEN];
        b[..8].copy_from_slice(&self.end.to_le_bytes());
        b[8..].copy_from_slice(&self.dropped.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8; RECORD_LEN]) -> Self {
        let (e, d) = b.split_at(8);
        Mark {
            end: u64::from_le_bytes(e.try_into().expect("8 bytes")),
            dropped: u64::from_le_bytes(d.try_into().expect("8 bytes")),
        }
    }
}

/// How much of an incoming chunk is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admission {
    pub store: u64,
    pub drop: u64,
}

/// `used` bytes are already stored (both streams together) under `limit`:
/// keep what fits and drop the rest. The pipe is always drained by the caller.
pub fn admit(used: u64, limit: u64, len: u64) -> Admission {
    let store = len.min(limit.saturating_sub(used));
    Admission {
        store,
        drop: len - store,
    }
}

/// Whether the drops counted since the last record are worth a record now:
/// the first drop always is (it makes `log_truncated` durable), later ones
/// are batched.
pub fn drop_needs_record(recorded_dropped: u64, dropped: u64) -> bool {
    recorded_dropped == 0 && dropped > 0
        || dropped.saturating_sub(recorded_dropped) >= DROP_RECORD_STEP
}

/// Whether an append must publish a record now: enough stored bytes wait
/// (`unrecorded`), or the drops are worth one.
pub fn record_needed(unrecorded: u64, recorded_dropped: u64, dropped: u64) -> bool {
    unrecorded >= MIN_RECORD_BYTES || drop_needs_record(recorded_dropped, dropped)
}

/// The cap on what one attempt may store (both streams): the homework's limit
/// less what its other attempts already stored. The limit of the spec is per
/// homework, so the caller (which knows the homework's attempts and their
/// `LogPosition`s) passes this to the spool as its limit.
pub fn attempt_limit(homework_limit: u64, stored_by_other_attempts: u64) -> u64 {
    homework_limit.saturating_sub(stored_by_other_attempts)
}

/// What a crash left behind in one stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    /// The index is cut to this many records.
    pub valid_records: u64,
    /// A record to add for bytes the log holds beyond the last record.
    pub append: Option<Mark>,
    /// The stream's state once both are applied.
    pub mark: Mark,
    /// Total records once both are applied: the sequence number.
    pub seq: u64,
}

/// Reads an index record by record (the caller supplies whole records, so the
/// scan runs in constant memory). A record is valid when it does not go
/// backwards and does not claim bytes the log file lacks; the first invalid one
/// ends the valid prefix.
#[derive(Debug)]
pub struct IndexScan {
    data_len: u64,
    prev: Mark,
    valid: u64,
    stopped: bool,
}

impl IndexScan {
    pub fn new(data_len: u64) -> Self {
        IndexScan {
            data_len,
            prev: Mark::default(),
            valid: 0,
            stopped: false,
        }
    }

    /// Returns whether the scan should go on.
    pub fn feed(&mut self, rec: &[u8; RECORD_LEN]) -> bool {
        if self.stopped {
            return false;
        }
        let m = Mark::decode(rec);
        if m.end < self.prev.end || m.dropped < self.prev.dropped || m.end > self.data_len {
            self.stopped = true;
            return false;
        }
        self.prev = m;
        self.valid += 1;
        true
    }

    pub fn finish(self) -> Recovery {
        let append = (self.prev.end < self.data_len).then_some(Mark {
            end: self.data_len,
            dropped: self.prev.dropped,
        });
        Recovery {
            valid_records: self.valid,
            append,
            mark: append.unwrap_or(self.prev),
            seq: self.valid + u64::from(append.is_some()),
        }
    }
}

/// The part of a stream a read returns: `(start, len)`. An offset past the
/// published end is answered at the end (nothing to give yet).
pub fn read_window(end: u64, offset: u64, max_bytes: u64) -> (u64, u64) {
    let start = offset.min(end);
    (start, max_bytes.min(MAX_READ_BYTES).min(end - start))
}

/// A path the spool or the result store may use under its root: relative,
/// plain components only. `name` is an artifact name or a path inside it.
pub fn relative_path(name: &str) -> Option<PathBuf> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return None;
    }
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() > MAX_DEPTH {
        return None;
    }
    let ok = parts.iter().all(|p| {
        !p.is_empty()
            && p.len() <= MAX_COMPONENT_BYTES
            && *p != "."
            && *p != ".."
            && !p.starts_with(".tmp")
            && !p.chars().any(|c| c.is_control() || c == '\\')
    });
    ok.then(|| parts.iter().collect())
}

pub fn logs_locator(attempt: &AttemptId) -> String {
    format!("{attempt}/logs")
}

pub fn artifact_locator(attempt: &AttemptId, rel: &str) -> String {
    format!("{attempt}/artifacts/{rel}")
}

/// A locator that GC may delete: `<attempt>/logs` or `<attempt>/artifacts/<path>`.
/// Anything else is not ours and is never touched.
pub fn resolve_locator(locator: &str) -> Option<PathBuf> {
    let (attempt, rest) = locator.split_once('/')?;
    // `parse` re-encodes the ULID, so only the canonical spelling passes.
    if AttemptId::parse(attempt).ok()?.as_str() != attempt {
        return None;
    }
    let ok = rest == "logs"
        || rest
            .strip_prefix("artifacts/")
            .is_some_and(|p| relative_path(p).is_some());
    ok.then(|| PathBuf::from(attempt).join(rest))
}

/// One stream in the stored result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSummary {
    pub bytes: u64,
    pub dropped_bytes: u64,
    pub last_seq: u64,
}

/// The stored result: termination, artifacts (by name, so the same result is
/// the same bytes) and, per stream, what the spool kept and dropped.
pub fn result_json(
    termination: &Termination,
    artifacts: &[ArtifactRecord],
    stdout: StreamSummary,
    stderr: StreamSummary,
) -> Value {
    let mut sorted: Vec<&ArtifactRecord> = artifacts.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let stream = |s: StreamSummary| {
        json!({
            "bytes": s.bytes,
            "dropped_bytes": s.dropped_bytes,
            "last_seq": s.last_seq,
            "log_truncated": s.dropped_bytes > 0,
        })
    };
    json!({
        "termination": termination,
        "artifacts": sorted,
        "logs": { "stdout": stream(stdout), "stderr": stream(stderr) },
    })
}

/// Hex of a finished SHA-256, as the digest type wants it.
pub fn sha256_digest(hash: &[u8]) -> Option<Digest> {
    let mut s = String::from(Digest::PREFIX);
    for b in hash {
        s.push_str(&format!("{b:02x}"));
    }
    Digest::parse(&s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SourceId;
    use crate::state::Outcome;

    fn rec(end: u64, dropped: u64) -> [u8; RECORD_LEN] {
        Mark { end, dropped }.encode()
    }

    #[test]
    fn a_record_waits_for_enough_bytes_or_a_drop_worth_recording() {
        assert!(!record_needed(0, 0, 0));
        assert!(!record_needed(MIN_RECORD_BYTES - 1, 0, 0));
        assert!(record_needed(MIN_RECORD_BYTES, 0, 0));
        assert!(record_needed(0, 0, 1));
        assert!(!record_needed(1, 3, 4));
        assert!(record_needed(1, 3, 3 + DROP_RECORD_STEP));
    }

    #[test]
    fn the_attempt_limit_is_what_the_other_attempts_left() {
        assert_eq!(attempt_limit(100, 0), 100);
        assert_eq!(attempt_limit(100, 30), 70);
        assert_eq!(attempt_limit(100, 100), 0);
        assert_eq!(attempt_limit(100, 500), 0);
    }

    #[test]
    fn mark_round_trips() {
        for m in [
            Mark::default(),
            Mark { end: 1, dropped: 2 },
            Mark {
                end: u64::MAX,
                dropped: u64::MAX - 1,
            },
        ] {
            assert_eq!(Mark::decode(&m.encode()), m);
        }
        let b = Mark { end: 1, dropped: 2 }.encode();
        assert_eq!((b[0], b[8]), (1, 2));
    }

    #[test]
    fn admit_keeps_what_fits() {
        let a = |u, l, n| {
            let x = admit(u, l, n);
            (x.store, x.drop)
        };
        assert_eq!(a(0, 10, 4), (4, 0));
        assert_eq!(a(8, 10, 4), (2, 2));
        assert_eq!(a(10, 10, 4), (0, 4));
        assert_eq!(a(12, 10, 4), (0, 4));
        assert_eq!(a(0, 10, 10), (10, 0));
        assert_eq!(a(0, 0, 0), (0, 0));
    }

    #[test]
    fn first_drop_is_recorded_later_ones_in_batches() {
        assert!(!drop_needs_record(0, 0));
        assert!(drop_needs_record(0, 1));
        assert!(!drop_needs_record(1, 2));
        assert!(!drop_needs_record(1, DROP_RECORD_STEP));
        assert!(drop_needs_record(1, DROP_RECORD_STEP + 1));
        assert!(drop_needs_record(5, 5 + DROP_RECORD_STEP));
    }

    fn scan(records: &[[u8; RECORD_LEN]], data_len: u64) -> Recovery {
        let mut s = IndexScan::new(data_len);
        for r in records {
            if !s.feed(r) {
                break;
            }
        }
        s.finish()
    }

    #[test]
    fn scan_of_an_empty_index() {
        let r = scan(&[], 0);
        assert_eq!((r.valid_records, r.append, r.seq), (0, None, 0));
        assert_eq!(r.mark, Mark::default());
        // Bytes with no record yet become one.
        let r = scan(&[], 7);
        assert_eq!(r.append, Some(Mark { end: 7, dropped: 0 }));
        assert_eq!((r.valid_records, r.seq, r.mark.end), (0, 1, 7));
    }

    #[test]
    fn scan_keeps_the_valid_prefix() {
        let r = scan(&[rec(3, 0), rec(3, 2), rec(9, 2)], 9);
        assert_eq!((r.valid_records, r.append, r.seq), (3, None, 3));
        assert_eq!(r.mark, Mark { end: 9, dropped: 2 });
        // Going backwards ends the prefix, in either field.
        assert_eq!(scan(&[rec(5, 0), rec(4, 0), rec(9, 0)], 9).valid_records, 1);
        assert_eq!(scan(&[rec(5, 3), rec(5, 2), rec(9, 3)], 9).valid_records, 1);
        // So does a record that claims bytes the log lacks (power loss).
        let r = scan(&[rec(5, 0), rec(9, 0)], 6);
        assert_eq!(r.valid_records, 1);
        assert_eq!(r.append, Some(Mark { end: 6, dropped: 0 }));
        assert_eq!(r.seq, 2);
        // Equal records are fine (a drop-only record keeps `end`).
        assert_eq!(scan(&[rec(5, 0), rec(5, 0)], 5).valid_records, 2);
    }

    #[test]
    fn scan_stops_for_good_after_the_first_bad_record() {
        let mut s = IndexScan::new(10);
        assert!(s.feed(&rec(5, 0)));
        assert!(!s.feed(&rec(4, 0)));
        // A later record that would be valid on its own is not taken.
        assert!(!s.feed(&rec(8, 0)));
        assert_eq!(s.finish().valid_records, 1);
    }

    #[test]
    fn read_window_clamps() {
        assert_eq!(read_window(10, 0, 4), (0, 4));
        assert_eq!(read_window(10, 8, 4), (8, 2));
        assert_eq!(read_window(10, 10, 4), (10, 0));
        assert_eq!(read_window(10, 99, 4), (10, 0));
        assert_eq!(read_window(10, 3, 0), (3, 0));
        assert_eq!(read_window(u64::MAX, 0, u64::MAX), (0, MAX_READ_BYTES));
    }

    #[test]
    fn relative_paths_are_plain() {
        assert_eq!(relative_path("a.txt"), Some(PathBuf::from("a.txt")));
        assert_eq!(relative_path("d/e/f.bin"), Some(PathBuf::from("d/e/f.bin")));
        for bad in [
            "", "/abs", "a//b", "a/", "./a", "a/./b", "../a", "a/../b", "a\\b", ".tmpx", "a/.tmp1",
            "a\nb", "a\0b",
        ] {
            assert_eq!(relative_path(bad), None, "{bad:?}");
        }
        assert!(relative_path(&"x".repeat(255)).is_some());
        assert!(relative_path(&"x".repeat(256)).is_none());
        let deep = |n: usize| vec!["d"; n].join("/");
        assert!(relative_path(&deep(MAX_DEPTH)).is_some());
        assert!(relative_path(&deep(MAX_DEPTH + 1)).is_none());
        // Total length: 1024 bytes pass, 1025 do not.
        let part = |n: usize| "a".repeat(n);
        let exact = format!(
            "{}/{}/{}/{}/{}/{}",
            part(200),
            part(200),
            part(200),
            part(200),
            part(200),
            part(19)
        );
        assert_eq!(exact.len(), MAX_NAME_BYTES);
        assert!(relative_path(&exact).is_some());
        assert!(relative_path(&format!("{exact}a")).is_none());
    }

    #[test]
    fn locators_resolve_only_to_our_files() {
        let a = AttemptId::generate();
        assert_eq!(
            resolve_locator(&logs_locator(&a)),
            Some(PathBuf::from(a.as_str()).join("logs"))
        );
        assert_eq!(
            resolve_locator(&artifact_locator(&a, "d/x.bin")),
            Some(PathBuf::from(a.as_str()).join("artifacts/d/x.bin"))
        );
        for bad in [
            "".to_string(),
            "logs".to_string(),
            format!("{a}"),
            format!("{a}/other"),
            format!("{a}/logs/x"),
            format!("{a}/artifacts"),
            format!("{a}/artifacts/"),
            format!("{a}/artifacts/../x"),
            format!("{a}/artifacts/a//b"),
            format!("/{a}/logs"),
            format!("../{a}/logs"),
            "att_nope/logs".to_string(),
            format!("{}/logs", a.as_str().to_lowercase()),
        ] {
            assert_eq!(resolve_locator(&bad), None, "{bad:?}");
        }
    }

    #[test]
    fn result_is_sorted_and_flags_truncation() {
        let t = Termination::new(Outcome::Cancelled, None, None).unwrap();
        let s = |b, d, q| StreamSummary {
            bytes: b,
            dropped_bytes: d,
            last_seq: q,
        };
        let a = AttemptId::generate();
        let art = |n: &str| ArtifactRecord {
            name: n.into(),
            path: n.into(),
            size: 1,
            digest: Digest::of_bytes(b"x"),
            source_id: SourceId::generate(),
            plan_digest: Digest::of_bytes(b"p"),
            attempt_id: a.clone(),
            profile_fingerprint: Digest::of_bytes(b"f"),
        };
        let v = result_json(&t, &[art("b"), art("a")], s(5, 0, 2), s(6, 9, 3));
        let names: Vec<&str> = v["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(v["termination"]["outcome"], "cancelled");
        assert_eq!(v["logs"]["stdout"]["log_truncated"], false);
        assert_eq!(v["logs"]["stderr"]["log_truncated"], true);
        assert_eq!(v["logs"]["stderr"]["dropped_bytes"], 9);
        assert_eq!(v["logs"]["stdout"]["last_seq"], 2);
        assert_eq!(v["logs"]["stdout"]["bytes"], 5);
    }

    #[test]
    fn digest_of_a_hash() {
        use sha2::{Digest as _, Sha256};
        let h = Sha256::digest(b"abc");
        assert_eq!(sha256_digest(&h), Some(Digest::of_bytes(b"abc")));
        assert_eq!(sha256_digest(&[1, 2, 3]), None);
    }
}
