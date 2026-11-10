//! Offset and digest rules for blob and log transfer (spec chapters 11, 13).
//!
//! Nothing here stores bytes. Each type tells the caller which bytes are new
//! and when a transfer is complete, so the same rules serve a spool file, a blob
//! store or a test buffer.

use crate::ids::{AttemptId, Digest};
use crate::wire::AttemptEvent;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Largest payload of one chunk (256 KiB). One frame is the unit a heartbeat can
/// be delayed by, so this also bounds that delay.
pub const MAX_CHUNK_BYTES: usize = 262_144;

/// The chunk size to use for a peer's `max_bytes`, never above
/// [`MAX_CHUNK_BYTES`] and never zero.
pub fn clamp_chunk(max_bytes: u32) -> usize {
    (max_bytes as usize).clamp(1, MAX_CHUNK_BYTES)
}

/// The bytes of `data` that start at `from`, at most `max` of them; empty once
/// `from` is at or past the end.
pub fn chunk_at(data: &[u8], from: u64, max: usize) -> &[u8] {
    let from = usize::try_from(from).unwrap_or(usize::MAX).min(data.len());
    let to = from.saturating_add(max).min(data.len());
    &data[from..to]
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChunkError {
    /// The chunk starts after the bytes received so far; resend from `received`.
    #[error("chunk starts past the received bytes (received {received})")]
    Gap { received: u64 },
    /// The chunk ends past the announced total size.
    #[error("chunk runs past the announced size {total}")]
    Overflow { total: u64 },
    /// All bytes arrived but their digest is not the announced one. The partial
    /// blob is discarded; the sender starts over.
    #[error("digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: Digest, actual: Digest },
}

/// A chunk that was accepted.
#[derive(Debug, PartialEq, Eq)]
pub struct Accepted<'a> {
    /// Contiguous bytes held now; the next chunk starts here.
    pub received: u64,
    /// The part of the chunk that was new. Empty for a duplicate.
    pub fresh: &'a [u8],
    /// Every byte arrived and the digest matches.
    pub complete: bool,
}

fn digest_of(hasher: &Sha256) -> Digest {
    let hash = hasher.clone().finalize();
    let mut s = String::from(Digest::PREFIX);
    for b in hash {
        s.push_str(&format!("{b:02x}"));
    }
    Digest::parse(&s).expect("a sha256 renders as a valid digest")
}

/// The receiving side of one blob.
#[derive(Debug, Clone)]
pub struct ChunkReceiver {
    expected: Digest,
    total: u64,
    received: u64,
    hasher: Sha256,
}

impl ChunkReceiver {
    pub fn new(expected: Digest, total: u64) -> Self {
        ChunkReceiver {
            expected,
            total,
            received: 0,
            hasher: Sha256::new(),
        }
    }

    /// The announced size of the whole blob.
    pub fn total_size(&self) -> u64 {
        self.total
    }

    pub fn received(&self) -> u64 {
        self.received
    }

    /// Take a chunk. Bytes already held are skipped, so a repeated or
    /// overlapping chunk is harmless; a chunk that leaves a gap is refused. The
    /// blob only counts as complete after its digest is checked.
    pub fn accept<'a>(&mut self, offset: u64, data: &'a [u8]) -> Result<Accepted<'a>, ChunkError> {
        let end = offset
            .checked_add(data.len() as u64)
            .filter(|end| *end <= self.total)
            .ok_or(ChunkError::Overflow { total: self.total })?;
        if offset > self.received {
            return Err(ChunkError::Gap {
                received: self.received,
            });
        }
        let skip = (self.received - offset) as usize;
        let fresh = data.get(skip..).unwrap_or(&[]);
        self.hasher.update(fresh);
        self.received = self.received.max(end);
        let complete = self.received == self.total;
        if complete {
            let actual = digest_of(&self.hasher);
            if actual != self.expected {
                self.received = 0;
                self.hasher = Sha256::new();
                return Err(ChunkError::DigestMismatch {
                    expected: self.expected.clone(),
                    actual,
                });
            }
        }
        Ok(Accepted {
            received: self.received,
            fresh,
            complete,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LogError {
    #[error("log chunk starts at {got} but {expected} was expected")]
    Gap { expected: u64, got: u64 },
    #[error("log chunk at {offset} with {len} bytes does not end at next_offset {next_offset}")]
    Inconsistent {
        offset: u64,
        len: u64,
        next_offset: u64,
    },
}

/// Where a reader of one log stream stands. Offsets count bytes of the original
/// output; a worker that dropped bytes (spool limit) says so with
/// `log_truncated`, and only then may a chunk start past the cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogCursor {
    next_offset: u64,
    skipped: u64,
}

impl LogCursor {
    pub fn new(next_offset: u64) -> Self {
        LogCursor {
            next_offset,
            skipped: 0,
        }
    }

    /// The offset to ask for next.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Bytes the worker reported as dropped and the reader skipped over.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// Apply one `read_log` answer; return only the bytes not seen before.
    pub fn apply<'a>(
        &mut self,
        offset: u64,
        data: &'a [u8],
        next_offset: u64,
        log_truncated: bool,
    ) -> Result<&'a [u8], LogError> {
        let len = data.len() as u64;
        if offset.checked_add(len) != Some(next_offset) {
            return Err(LogError::Inconsistent {
                offset,
                len,
                next_offset,
            });
        }
        if offset > self.next_offset {
            if !log_truncated {
                return Err(LogError::Gap {
                    expected: self.next_offset,
                    got: offset,
                });
            }
            self.skipped += offset - self.next_offset;
            self.next_offset = offset;
        }
        let skip = (self.next_offset - offset) as usize;
        let fresh = data.get(skip..).unwrap_or(&[]);
        self.next_offset = self.next_offset.max(next_offset);
        Ok(fresh)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventError {
    /// An event skips a `seq` of its attempt: events were lost.
    #[error("event stream has a gap: expected seq {expected}, got {got}")]
    Gap { expected: u64, got: u64 },
    /// Within one answer the `seq` went down.
    #[error("event seq went backwards inside one answer: {after} after {before}")]
    Descending { before: u64, after: u64 },
    /// An event of another attempt came back from a watch of this one.
    #[error("event belongs to attempt {got}, not {expected}")]
    ForeignAttempt { expected: AttemptId, got: AttemptId },
}

/// Where a reader of one attempt's event stream stands (`watch_events`).
///
/// `seq` is a per-attempt sequence that starts at 1 and has no holes, so one
/// `after_seq` follows one attempt and a cursor belongs to exactly one attempt;
/// the identity of an event is `(attempt_id, seq)`. A repeated or older event is
/// dropped, a missing one is an error (like a gap in a log), a `seq` that goes
/// down inside one answer is an error, and so is an event of another attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCursor {
    attempt_id: AttemptId,
    after_seq: u64,
}

impl EventCursor {
    /// A cursor over `attempt_id` that has seen everything up to `after_seq`
    /// (0 for a fresh one: the first event is seq 1).
    pub fn new(attempt_id: AttemptId, after_seq: u64) -> Self {
        EventCursor {
            attempt_id,
            after_seq,
        }
    }

    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// The `after_seq` to ask for next.
    pub fn after_seq(&self) -> u64 {
        self.after_seq
    }

    /// Events not seen before, in order. Nothing is applied when the answer is
    /// inconsistent. The cursor moves only over events actually delivered:
    /// `next_seq` is the worker's claim of where to continue (the `after_seq` for
    /// the next ask), and it can never be believed past the last delivered
    /// `seq`, so it is not used: a wrong or hostile value cannot make the reader
    /// skip events it has not been given. `u64::MAX` is a valid `seq`; nothing
    /// can follow it.
    pub fn fresh<'a>(
        &mut self,
        events: &'a [AttemptEvent],
        _next_seq: u64,
    ) -> Result<Vec<&'a AttemptEvent>, EventError> {
        let mut last = self.after_seq;
        let mut previous: Option<u64> = None;
        let mut out = Vec::new();
        for e in events {
            if e.attempt_id != self.attempt_id {
                return Err(EventError::ForeignAttempt {
                    expected: self.attempt_id.clone(),
                    got: e.attempt_id.clone(),
                });
            }
            if let Some(before) = previous {
                if e.seq < before {
                    return Err(EventError::Descending {
                        before,
                        after: e.seq,
                    });
                }
            }
            previous = Some(e.seq);
            if e.seq <= last {
                continue;
            }
            // `last + 1` cannot overflow: `e.seq > last` means `last < u64::MAX`.
            let expected = last + 1;
            if e.seq != expected {
                return Err(EventError::Gap {
                    expected,
                    got: e.seq,
                });
            }
            last = e.seq;
            out.push(e);
        }
        self.after_seq = last;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::AttemptId;
    use crate::wire::EventKind;

    fn blob(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 % 251) as u8).collect()
    }

    fn receiver(data: &[u8]) -> ChunkReceiver {
        ChunkReceiver::new(Digest::of_bytes(data), data.len() as u64)
    }

    #[test]
    fn clamp_and_slice_helpers() {
        assert_eq!(clamp_chunk(0), 1);
        assert_eq!(clamp_chunk(10), 10);
        assert_eq!(clamp_chunk(u32::MAX), MAX_CHUNK_BYTES);
        let d = [1u8, 2, 3, 4, 5];
        assert_eq!(chunk_at(&d, 0, 2), &[1, 2]);
        assert_eq!(chunk_at(&d, 3, 10), &[4, 5]);
        assert_eq!(chunk_at(&d, 5, 3), &[] as &[u8]);
        assert_eq!(chunk_at(&d, 99, 3), &[] as &[u8]);
        assert_eq!(chunk_at(&d, u64::MAX, 3), &[] as &[u8]);
        assert_eq!(chunk_at(&d, 1, usize::MAX), &[2, 3, 4, 5]);
    }

    #[test]
    fn in_order_chunks_complete_only_at_the_end() {
        let data = blob(10);
        let mut r = receiver(&data);
        assert_eq!(r.total_size(), 10);
        let a = r.accept(0, &data[..4]).unwrap();
        assert_eq!((a.received, a.fresh, a.complete), (4, &data[..4], false));
        let a = r.accept(4, &data[4..9]).unwrap();
        assert_eq!((a.received, a.complete), (9, false));
        let a = r.accept(9, &data[9..]).unwrap();
        assert_eq!((a.received, a.fresh, a.complete), (10, &data[9..], true));
        assert_eq!(r.received(), 10);
    }

    #[test]
    fn duplicate_and_overlapping_chunks_are_harmless() {
        let data = blob(10);
        let mut r = receiver(&data);
        r.accept(0, &data[..6]).unwrap();
        let dup = r.accept(0, &data[..6]).unwrap();
        assert_eq!((dup.received, dup.fresh.len(), dup.complete), (6, 0, false));
        let inside = r.accept(2, &data[2..4]).unwrap();
        assert_eq!((inside.received, inside.fresh.len()), (6, 0));
        let overlap = r.accept(4, &data[4..]).unwrap();
        assert_eq!(
            (overlap.received, overlap.fresh, overlap.complete),
            (10, &data[6..], true)
        );
        // A late duplicate after completion is still complete and adds nothing.
        let late = r.accept(0, &data[..3]).unwrap();
        assert_eq!(
            (late.received, late.fresh.len(), late.complete),
            (10, 0, true)
        );
    }

    #[test]
    fn a_gap_is_refused_with_the_resume_offset() {
        let data = blob(10);
        let mut r = receiver(&data);
        r.accept(0, &data[..3]).unwrap();
        assert_eq!(
            r.accept(5, &data[5..]),
            Err(ChunkError::Gap { received: 3 })
        );
        assert_eq!(r.received(), 3);
        // The offset equal to what is held is not a gap.
        assert!(r.accept(3, &data[3..5]).is_ok());
    }

    #[test]
    fn a_chunk_past_the_total_is_refused() {
        let data = blob(4);
        let mut r = receiver(&data);
        assert_eq!(
            r.accept(0, &blob(5)),
            Err(ChunkError::Overflow { total: 4 })
        );
        assert_eq!(
            r.accept(3, &blob(2)),
            Err(ChunkError::Overflow { total: 4 })
        );
        assert_eq!(
            r.accept(u64::MAX, &blob(2)),
            Err(ChunkError::Overflow { total: 4 })
        );
        assert_eq!(r.received(), 0);
        // Exactly up to the total is fine.
        assert!(r.accept(0, &data).unwrap().complete);
    }

    #[test]
    fn a_wrong_digest_is_refused_and_the_blob_starts_over() {
        let data = blob(8);
        let mut r = ChunkReceiver::new(Digest::of_bytes(b"something else"), 8);
        r.accept(0, &data[..5]).unwrap();
        assert_eq!(
            r.accept(5, &data[5..]),
            Err(ChunkError::DigestMismatch {
                expected: Digest::of_bytes(b"something else"),
                actual: Digest::of_bytes(&data),
            })
        );
        assert_eq!(r.received(), 0);
        // The retry hashes from scratch: wrong again, same error, not a stale hash.
        r.accept(0, &data[..5]).unwrap();
        assert!(matches!(
            r.accept(5, &data[5..]),
            Err(ChunkError::DigestMismatch { .. })
        ));
        let mut good = receiver(&data);
        assert!(good.accept(0, &data).unwrap().complete);
    }

    #[test]
    fn an_empty_blob_completes_on_an_empty_probe() {
        let mut r = receiver(b"");
        let a = r.accept(0, b"").unwrap();
        assert_eq!((a.received, a.fresh.len(), a.complete), (0, 0, true));
        let mut r = ChunkReceiver::new(Digest::of_bytes(b"x"), 0);
        assert!(matches!(
            r.accept(0, b""),
            Err(ChunkError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn non_utf8_bytes_pass_through() {
        let data = vec![0xff, 0xfe, 0x00, 0x80, 0xc3, 0x28];
        let mut r = receiver(&data);
        let a = r.accept(0, &data).unwrap();
        assert_eq!(a.fresh, &data[..]);
        assert!(a.complete);
    }

    #[test]
    fn log_cursor_returns_each_byte_once() {
        let mut c = LogCursor::new(0);
        assert_eq!(c.apply(0, b"abc", 3, false).unwrap(), b"abc");
        assert_eq!(c.next_offset(), 3);
        // The same answer again is a duplicate.
        assert_eq!(c.apply(0, b"abc", 3, false).unwrap(), b"");
        // An overlapping answer yields only the tail.
        assert_eq!(c.apply(1, b"bcde", 5, false).unwrap(), b"de");
        assert_eq!(c.next_offset(), 5);
        // An empty answer at the cursor is fine.
        assert_eq!(c.apply(5, b"", 5, false).unwrap(), b"");
        assert_eq!(c.next_offset(), 5);
        // An older empty answer does not move the cursor back.
        assert_eq!(c.apply(2, b"", 2, false).unwrap(), b"");
        assert_eq!(c.next_offset(), 5);
        assert_eq!(c.skipped(), 0);
    }

    #[test]
    fn log_gaps_need_a_truncation_report() {
        let mut c = LogCursor::new(2);
        assert_eq!(
            c.apply(5, b"xy", 7, false),
            Err(LogError::Gap {
                expected: 2,
                got: 5
            })
        );
        assert_eq!(c.next_offset(), 2);
        assert_eq!(c.apply(5, b"xy", 7, true).unwrap(), b"xy");
        assert_eq!((c.next_offset(), c.skipped()), (7, 3));
    }

    #[test]
    fn log_answers_must_be_self_consistent() {
        let mut c = LogCursor::new(0);
        assert_eq!(
            c.apply(0, b"ab", 5, false),
            Err(LogError::Inconsistent {
                offset: 0,
                len: 2,
                next_offset: 5
            })
        );
        assert!(c.apply(u64::MAX, b"ab", 1, true).is_err());
        assert_eq!(c.next_offset(), 0);
        assert!(LogError::Gap {
            expected: 1,
            got: 2
        }
        .to_string()
        .contains("expected"));
    }

    fn event_of(attempt: &AttemptId, seq: u64) -> AttemptEvent {
        AttemptEvent {
            seq,
            attempt_id: attempt.clone(),
            kind: EventKind::StateChanged,
            state: None,
            receipt: None,
            node_id: None,
            step: None,
        }
    }

    fn seqs(v: Vec<&AttemptEvent>) -> Vec<u64> {
        v.iter().map(|e| e.seq).collect()
    }

    #[test]
    fn event_cursor_drops_repeats_and_follows_the_stream() {
        let a = AttemptId::generate();
        let mut c = EventCursor::new(a.clone(), 0);
        assert_eq!(c.attempt_id(), &a);
        let first = [event_of(&a, 1), event_of(&a, 2), event_of(&a, 3)];
        assert_eq!(seqs(c.fresh(&first, 3).unwrap()), vec![1, 2, 3]);
        assert_eq!(c.after_seq(), 3);
        // The same page again, and an older one: nothing new.
        assert!(c.fresh(&first, 3).unwrap().is_empty());
        let mixed = [
            event_of(&a, 2),
            event_of(&a, 4),
            event_of(&a, 4),
            event_of(&a, 5),
        ];
        assert_eq!(seqs(c.fresh(&mixed, 5).unwrap()), vec![4, 5]);
        assert_eq!(c.after_seq(), 5);
        // A cursor resumed in the middle expects the next seq, not 1.
        let mut resumed = EventCursor::new(a.clone(), 5);
        assert_eq!(seqs(resumed.fresh(&[event_of(&a, 6)], 6).unwrap()), vec![6]);
    }

    #[test]
    fn a_missing_seq_is_an_error_and_nothing_is_applied() {
        let a = AttemptId::generate();
        let mut c = EventCursor::new(a.clone(), 0);
        c.fresh(&[event_of(&a, 1)], 1).unwrap();
        let before = c.clone();
        assert_eq!(
            c.fresh(&[event_of(&a, 2), event_of(&a, 4)], 4),
            Err(EventError::Gap {
                expected: 3,
                got: 4
            })
        );
        // The good first event of the bad answer was not kept either.
        assert_eq!(c, before);
        // A first event that is not 1 is a gap too (events 1 and 2 are missing).
        let mut fresh = EventCursor::new(a.clone(), 0);
        let err = fresh.fresh(&[event_of(&a, 3)], 3).unwrap_err();
        assert_eq!(
            err,
            EventError::Gap {
                expected: 1,
                got: 3
            }
        );
        assert!(err.to_string().contains("expected seq 1"));
    }

    #[test]
    fn a_descending_answer_is_an_error() {
        let a = AttemptId::generate();
        let mut c = EventCursor::new(a.clone(), 0);
        let err = c
            .fresh(&[event_of(&a, 1), event_of(&a, 2), event_of(&a, 1)], 2)
            .unwrap_err();
        assert_eq!(
            err,
            EventError::Descending {
                before: 2,
                after: 1
            }
        );
        assert!(err.to_string().contains("backwards"));
        assert_eq!(c.after_seq(), 0);
    }

    #[test]
    fn an_event_of_another_attempt_is_an_error() {
        let (a, b) = (AttemptId::generate(), AttemptId::generate());
        let mut c = EventCursor::new(a.clone(), 0);
        let err = c.fresh(&[event_of(&a, 1), event_of(&b, 1)], 1).unwrap_err();
        assert_eq!(
            err,
            EventError::ForeignAttempt {
                expected: a.clone(),
                got: b.clone()
            }
        );
        assert!(err.to_string().contains(&b.to_string()));
        assert_eq!(c.after_seq(), 0);
    }

    #[test]
    fn next_seq_is_believed_only_up_to_what_was_delivered() {
        let a = AttemptId::generate();
        let mut c = EventCursor::new(a.clone(), 0);
        // With nothing delivered, next_seq cannot move the cursor.
        assert!(c.fresh(&[], u64::MAX).unwrap().is_empty());
        assert_eq!(c.after_seq(), 0);
        // Beyond the last event it is capped at that event.
        c.fresh(&[event_of(&a, 1), event_of(&a, 2)], u64::MAX)
            .unwrap();
        assert_eq!(c.after_seq(), 2);
        // Below it, it does not pull the cursor back.
        assert!(c.fresh(&[], 1).unwrap().is_empty());
        assert_eq!(c.after_seq(), 2);
        c.fresh(&[event_of(&a, 3)], 0).unwrap();
        assert_eq!(c.after_seq(), 3);
    }

    #[test]
    fn the_largest_seq_is_handled_without_overflow() {
        let a = AttemptId::generate();
        let mut c = EventCursor::new(a.clone(), u64::MAX - 1);
        assert_eq!(
            seqs(c.fresh(&[event_of(&a, u64::MAX)], u64::MAX).unwrap()),
            vec![u64::MAX]
        );
        assert_eq!(c.after_seq(), u64::MAX);
        // Nothing can come after it; repeats are dropped, no overflow.
        assert!(c
            .fresh(&[event_of(&a, u64::MAX)], u64::MAX)
            .unwrap()
            .is_empty());
        let mut at_max = EventCursor::new(a, u64::MAX);
        assert!(at_max.fresh(&[], u64::MAX).unwrap().is_empty());
        assert_eq!(at_max.after_seq(), u64::MAX);
    }
}
