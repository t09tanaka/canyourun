//! The decisions of result collection (spec chapter 13), without I/O.
//!
//! Everything here judges durable facts of the parent or an answer of a
//! worker, which is another host and is not trusted: sizes, offsets, names
//! and counts are checked before anything is written.

use crate::error::ErrorCode;
use crate::state::Outcome;
use crate::wire::NotStoredReason;

/// How many faults that are not a lost connection one collection survives.
pub const MAX_STRIKES: u32 = 3;

/// The most artifacts one result may list (the worker refuses longer lists).
pub const MAX_ARTIFACTS: usize = 1024;

/// Inputs, logs and results are kept this long after the end by default.
pub const DEFAULT_RETENTION_DAYS: u32 = 7;

/// The retention in milliseconds.
pub fn retention_ms(days: u32) -> u64 {
    u64::from(days) * 86_400_000
}

// ------------------------------------------------------------- the steps

/// What is durable in the parent about one collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// The result was read and recorded with its digest.
    pub has_result: bool,
    /// The logs were read to their end.
    pub logs_done: bool,
    /// Stored, declared artifacts that are not yet published here.
    pub artifacts_left: u32,
    /// The worker's acknowledgement was recorded (or nothing is asked of it).
    pub closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    FetchResult,
    FetchLogs,
    FetchArtifact,
    /// Everything is verified and durable: only now the worker is told.
    Ack,
    Done,
}

/// The one step that follows. The acknowledgement comes last, whatever
/// else is known.
pub fn next_step(p: Progress) -> Step {
    if p.closed {
        Step::Done
    } else if !p.has_result {
        Step::FetchResult
    } else if !p.logs_done {
        Step::FetchLogs
    } else if p.artifacts_left > 0 {
        Step::FetchArtifact
    } else {
        Step::Ack
    }
}

// ------------------------------------------------------------- the faults

/// What went wrong with one request of a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// No answer.
    Unreachable,
    /// The worker answered with an error.
    Remote(ErrorCode),
    /// An answer that is not about what was asked, or contradicts itself.
    BadAnswer,
    /// The bytes that arrived are not the recorded ones.
    Corrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultVerdict {
    /// Not a fault of the collection: ask again later, as often as it takes.
    Later,
    /// Counts toward giving up.
    Strike,
    /// A newer coordinator took over.
    Fenced,
}

/// A lost connection and a busy worker are never a reason to give up: the
/// computed result stays where it is until it can be fetched.
pub fn judge_fault(fault: Fault) -> FaultVerdict {
    match fault {
        Fault::Unreachable | Fault::Remote(ErrorCode::Overloaded) => FaultVerdict::Later,
        Fault::Remote(ErrorCode::StaleEpoch) => FaultVerdict::Fenced,
        Fault::Remote(_) | Fault::BadAnswer | Fault::Corrupt => FaultVerdict::Strike,
    }
}

/// Whether a collection with this many strikes is given up.
pub fn gives_up(strikes: u32) -> bool {
    strikes >= MAX_STRIKES
}

// ------------------------------------------------------------- the result

/// One artifact record of a result, as far as its form goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordFacts {
    /// The record names the attempt that is collected.
    pub same_attempt: bool,
    /// Its path is its name (where the worker keeps it).
    pub path_is_name: bool,
    /// The name is a plain relative path.
    pub name_ok: bool,
    pub size: u64,
    pub max_size: u64,
}

pub fn check_record(f: RecordFacts) -> Result<(), &'static str> {
    if !f.same_attempt {
        Err("an artifact of another attempt")
    } else if !f.name_ok {
        Err("an artifact name that is not a plain relative path")
    } else if !f.path_is_name {
        Err("an artifact whose path is not its name")
    } else if f.size > f.max_size {
        Err("an artifact above the size limit")
    } else {
        Ok(())
    }
}

/// The lists of a result: how many entries, and whether a name is used
/// twice (among the stored ones, the unstored ones, or across both).
pub fn check_lists(stored: usize, not_stored: usize, distinct: usize) -> Result<(), &'static str> {
    let all = stored.saturating_add(not_stored);
    if all > MAX_ARTIFACTS {
        Err("more artifacts than a result may list")
    } else if distinct != all {
        Err("an artifact name that is listed twice")
    } else {
        Ok(())
    }
}

/// Where one declared artifact stands according to the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    Stored,
    /// The commands left nothing at the declared path.
    Missing,
    /// Something was there and the worker did not keep it. Final: asking
    /// again never changes it.
    Refused,
    /// The result says nothing about it.
    Unreported,
}

pub fn standing(stored: bool, not_stored: Option<NotStoredReason>) -> Standing {
    match (stored, not_stored) {
        (true, _) => Standing::Stored,
        (false, Some(NotStoredReason::Missing)) => Standing::Missing,
        (false, Some(NotStoredReason::Refused)) => Standing::Refused,
        (false, None) => Standing::Unreported,
    }
}

/// What the artifacts make of the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Every required artifact is here.
    Complete,
    /// A required artifact does not exist on the worker and never will.
    RequiredMissing,
    /// The collection was given up.
    GaveUp,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Complete => "complete",
            Verdict::RequiredMissing => "required_missing",
            Verdict::GaveUp => "gave_up",
        }
    }

    pub fn parse(s: &str) -> Option<Verdict> {
        match s {
            "complete" => Some(Verdict::Complete),
            "required_missing" => Some(Verdict::RequiredMissing),
            "gave_up" => Some(Verdict::GaveUp),
            _ => None,
        }
    }
}

/// A missing `optional` artifact changes nothing; a `required` one that is
/// not stored (missing, refused or not even reported) is never complete.
pub fn judge_artifacts(declared: &[(bool, Standing)]) -> Verdict {
    let lacking = declared
        .iter()
        .any(|(required, s)| *required && *s != Standing::Stored);
    if lacking {
        Verdict::RequiredMissing
    } else {
        Verdict::Complete
    }
}

// ------------------------------------------------------------- the chunks

/// One `blob_get` answer against what was asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkFacts {
    /// The answer names the digest that was asked for.
    pub same_digest: bool,
    pub want_offset: u64,
    /// The size the result records.
    pub size: u64,
    /// The most bytes that were asked for.
    pub max: u64,
    pub got_offset: u64,
    pub got_total: u64,
    pub got_len: u64,
    pub got_eof: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkVerdict {
    /// Append the bytes; `complete` when they reach the recorded size.
    Append {
        next: u64,
        complete: bool,
    },
    Bad(&'static str),
}

pub fn check_chunk(f: ChunkFacts) -> ChunkVerdict {
    if !f.same_digest {
        return ChunkVerdict::Bad("a chunk of another blob");
    }
    if f.got_offset != f.want_offset {
        return ChunkVerdict::Bad("a chunk at another offset");
    }
    if f.got_total != f.size {
        return ChunkVerdict::Bad("a blob of another size than recorded");
    }
    if f.got_len > f.max {
        return ChunkVerdict::Bad("a chunk larger than asked for");
    }
    let Some(next) = f
        .want_offset
        .checked_add(f.got_len)
        .filter(|n| *n <= f.size)
    else {
        return ChunkVerdict::Bad("a chunk past the end of the blob");
    };
    let complete = next == f.size;
    if f.got_eof != complete {
        return ChunkVerdict::Bad("a chunk whose end mark contradicts its size");
    }
    if f.got_len == 0 && !complete {
        return ChunkVerdict::Bad("an empty chunk before the end");
    }
    ChunkVerdict::Append { next, complete }
}

/// One `read_log` answer against what was asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogFacts {
    pub want_offset: u64,
    pub max: u64,
    /// The most bytes of one stream the parent keeps.
    pub limit: u64,
    pub got_offset: u64,
    pub got_len: u64,
    pub got_next: u64,
    pub got_eof: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogVerdict {
    /// Append the bytes; at `end` the stream is complete: the spool is
    /// closed and everything up to its end was read.
    Append {
        next: u64,
        end: bool,
    },
    /// Nothing new and no end yet.
    Wait,
    Bad(&'static str),
}

pub fn check_log(f: LogFacts) -> LogVerdict {
    if f.got_offset != f.want_offset {
        return LogVerdict::Bad("log bytes at another offset");
    }
    if f.got_len > f.max {
        return LogVerdict::Bad("more log bytes than asked for");
    }
    let Some(next) = f
        .want_offset
        .checked_add(f.got_len)
        .filter(|n| *n <= f.limit)
    else {
        return LogVerdict::Bad("a log above the size limit");
    };
    if f.got_next != next {
        return LogVerdict::Bad("a next offset that is not the end of the bytes");
    }
    if f.got_len == 0 && !f.got_eof {
        return LogVerdict::Wait;
    }
    LogVerdict::Append {
        next,
        end: f.got_eof,
    }
}

/// What to make of a stream that has no end mark and nothing new.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogWait {
    /// The end mark is still to come: the logs are not complete.
    Retry,
    /// The attempt ended without its spool being closed and has no result
    /// that describes the output: what was read is all there is to take.
    TakeIncomplete,
}

/// A stored result is written only after the spool is closed, so with a
/// result the end mark always comes. The `log_truncated` event of the
/// journal is not consulted: it is written after the attempt is terminal.
pub fn log_wait(has_stored_result: bool) -> LogWait {
    if has_stored_result {
        LogWait::Retry
    } else {
        LogWait::TakeIncomplete
    }
}

// ------------------------------------------------------------- the ack

/// After a refused acknowledgement the result was read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reread {
    /// Another result: collect it from its start.
    Restart,
    /// The same result, and still refused: the worker contradicts itself.
    Contradiction,
}

pub fn judge_reread(same_digest: bool) -> Reread {
    if same_digest {
        Reread::Contradiction
    } else {
        Reread::Restart
    }
}

// ------------------------------------------------------------- the run

/// Where one task of a run stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStanding {
    /// Not ended, or ended and not yet collected.
    Pending,
    Succeeded,
    Failed,
    Cancelled,
    /// It ended, and a required artifact is not there.
    RequiredArtifactMissing,
    /// Its outputs could not be collected.
    CollectionFailed,
}

/// `outcome`: how the task's current attempt ended, if it did; `verdict`:
/// what its closed collection found. A cancelled task without an attempt
/// is cancelled.
pub fn task_standing(
    outcome: Option<Outcome>,
    verdict: Option<Verdict>,
    cancelled_unplaced: bool,
) -> TaskStanding {
    match (outcome, verdict) {
        (None, _) if cancelled_unplaced => TaskStanding::Cancelled,
        (None, _) | (Some(_), None) => TaskStanding::Pending,
        (Some(_), Some(Verdict::GaveUp)) => TaskStanding::CollectionFailed,
        (Some(Outcome::Cancelled), Some(_)) => TaskStanding::Cancelled,
        (Some(Outcome::Succeeded), Some(Verdict::RequiredMissing)) => {
            TaskStanding::RequiredArtifactMissing
        }
        (Some(Outcome::Succeeded), Some(Verdict::Complete)) => TaskStanding::Succeeded,
        (Some(_), Some(_)) => TaskStanding::Failed,
    }
}

/// What the results of a run add up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunVerdict {
    /// Nothing was to run. Not a success of any test.
    NoWork,
    /// Fewer results than expected tasks.
    Missing,
    /// A task has not ended or is not collected.
    Collecting,
    Passed,
    Failed,
}

/// An empty or short set of results is never a success: zero expected
/// tasks is `NoWork`, fewer tasks than expected is `Missing`.
pub fn run_verdict(expected: u32, tasks: &[TaskStanding]) -> RunVerdict {
    if expected == 0 && tasks.is_empty() {
        RunVerdict::NoWork
    } else if (tasks.len() as u64) < u64::from(expected) {
        RunVerdict::Missing
    } else if tasks.contains(&TaskStanding::Pending) {
        RunVerdict::Collecting
    } else if tasks.iter().all(|t| *t == TaskStanding::Succeeded) {
        RunVerdict::Passed
    } else {
        RunVerdict::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_constants_are_what_the_spec_says() {
        assert_eq!(MAX_STRIKES, 3);
        assert_eq!(MAX_ARTIFACTS, 1024);
        assert_eq!(DEFAULT_RETENTION_DAYS, 7);
        assert_eq!(retention_ms(7), 604_800_000);
        assert_eq!(retention_ms(0), 0);
        assert_eq!(retention_ms(u32::MAX), 371_085_174_288_000_000);
    }

    fn p(has_result: bool, logs_done: bool, artifacts_left: u32, closed: bool) -> Progress {
        Progress {
            has_result,
            logs_done,
            artifacts_left,
            closed,
        }
    }

    #[test]
    fn the_acknowledgement_comes_after_everything_else() {
        assert_eq!(next_step(p(false, false, 0, false)), Step::FetchResult);
        assert_eq!(next_step(p(false, true, 0, false)), Step::FetchResult);
        assert_eq!(next_step(p(true, false, 0, false)), Step::FetchLogs);
        assert_eq!(next_step(p(true, false, 2, false)), Step::FetchLogs);
        assert_eq!(next_step(p(true, true, 2, false)), Step::FetchArtifact);
        assert_eq!(next_step(p(true, true, 1, false)), Step::FetchArtifact);
        assert_eq!(next_step(p(true, true, 0, false)), Step::Ack);
        for (r, l, a) in [(false, false, 3), (true, true, 0), (true, false, 1)] {
            assert_eq!(next_step(p(r, l, a, true)), Step::Done);
        }
    }

    #[test]
    fn a_lost_connection_or_a_busy_worker_is_never_a_strike() {
        use ErrorCode::*;
        assert_eq!(judge_fault(Fault::Unreachable), FaultVerdict::Later);
        assert_eq!(judge_fault(Fault::Remote(Overloaded)), FaultVerdict::Later);
        assert_eq!(judge_fault(Fault::Remote(StaleEpoch)), FaultVerdict::Fenced);
        for code in [NotFound, Internal, Conflict, InvalidInput, Retired] {
            assert_eq!(judge_fault(Fault::Remote(code)), FaultVerdict::Strike);
        }
        assert_eq!(judge_fault(Fault::BadAnswer), FaultVerdict::Strike);
        assert_eq!(judge_fault(Fault::Corrupt), FaultVerdict::Strike);
        assert!(!gives_up(0));
        assert!(!gives_up(2));
        assert!(gives_up(3));
        assert!(gives_up(4));
    }

    fn record() -> RecordFacts {
        RecordFacts {
            same_attempt: true,
            path_is_name: true,
            name_ok: true,
            size: 10,
            max_size: 10,
        }
    }

    #[test]
    fn a_record_is_checked_before_a_byte_is_fetched() {
        assert_eq!(check_record(record()), Ok(()));
        let e = |f: RecordFacts| check_record(f).unwrap_err();
        assert_eq!(
            e(RecordFacts {
                same_attempt: false,
                ..record()
            }),
            "an artifact of another attempt"
        );
        assert_eq!(
            e(RecordFacts {
                name_ok: false,
                ..record()
            }),
            "an artifact name that is not a plain relative path"
        );
        assert_eq!(
            e(RecordFacts {
                path_is_name: false,
                ..record()
            }),
            "an artifact whose path is not its name"
        );
        assert_eq!(
            e(RecordFacts {
                size: 11,
                ..record()
            }),
            "an artifact above the size limit"
        );
    }

    #[test]
    fn the_lists_of_a_result_are_bounded_and_name_nothing_twice() {
        assert_eq!(check_lists(0, 0, 0), Ok(()));
        assert_eq!(check_lists(1000, 24, 1024), Ok(()));
        assert_eq!(
            check_lists(1000, 25, 1025),
            Err("more artifacts than a result may list")
        );
        assert_eq!(
            check_lists(usize::MAX, 2, 3),
            Err("more artifacts than a result may list")
        );
        assert_eq!(
            check_lists(2, 1, 2),
            Err("an artifact name that is listed twice")
        );
        assert_eq!(
            check_lists(1, 1, 3),
            Err("an artifact name that is listed twice")
        );
    }

    #[test]
    fn a_required_artifact_that_is_not_stored_is_never_complete() {
        use NotStoredReason as R;
        assert_eq!(standing(true, None), Standing::Stored);
        assert_eq!(standing(true, Some(R::Refused)), Standing::Stored);
        assert_eq!(standing(false, Some(R::Missing)), Standing::Missing);
        assert_eq!(standing(false, Some(R::Refused)), Standing::Refused);
        assert_eq!(standing(false, None), Standing::Unreported);

        assert_eq!(judge_artifacts(&[]), Verdict::Complete);
        assert_eq!(
            judge_artifacts(&[(true, Standing::Stored), (false, Standing::Stored)]),
            Verdict::Complete
        );
        for s in [Standing::Missing, Standing::Refused, Standing::Unreported] {
            assert_eq!(
                judge_artifacts(&[(false, s), (true, Standing::Stored)]),
                Verdict::Complete
            );
            assert_eq!(
                judge_artifacts(&[(true, Standing::Stored), (true, s)]),
                Verdict::RequiredMissing
            );
        }
        for v in [Verdict::Complete, Verdict::RequiredMissing, Verdict::GaveUp] {
            assert_eq!(Verdict::parse(v.as_str()), Some(v));
        }
        assert_eq!(Verdict::Complete.as_str(), "complete");
        assert_eq!(Verdict::RequiredMissing.as_str(), "required_missing");
        assert_eq!(Verdict::GaveUp.as_str(), "gave_up");
        assert_eq!(Verdict::parse("done"), None);
    }

    fn chunk() -> ChunkFacts {
        ChunkFacts {
            same_digest: true,
            want_offset: 4,
            size: 10,
            max: 4,
            got_offset: 4,
            got_total: 10,
            got_len: 4,
            got_eof: false,
        }
    }

    #[test]
    fn a_chunk_is_appended_only_where_it_was_asked_for() {
        assert_eq!(
            check_chunk(chunk()),
            ChunkVerdict::Append {
                next: 8,
                complete: false
            }
        );
        let last = ChunkFacts {
            want_offset: 8,
            got_offset: 8,
            got_len: 2,
            got_eof: true,
            ..chunk()
        };
        assert_eq!(
            check_chunk(last),
            ChunkVerdict::Append {
                next: 10,
                complete: true
            }
        );
        // An empty blob is complete with its first, empty chunk.
        let empty = ChunkFacts {
            want_offset: 0,
            size: 0,
            got_offset: 0,
            got_total: 0,
            got_len: 0,
            got_eof: true,
            ..chunk()
        };
        assert_eq!(
            check_chunk(empty),
            ChunkVerdict::Append {
                next: 0,
                complete: true
            }
        );
        let bad = check_chunk;
        assert_eq!(
            bad(ChunkFacts {
                same_digest: false,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk of another blob")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_offset: 0,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk at another offset")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_total: 11,
                ..chunk()
            }),
            ChunkVerdict::Bad("a blob of another size than recorded")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_len: 5,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk larger than asked for")
        );
        assert_eq!(
            bad(ChunkFacts {
                max: 8,
                got_len: 7,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk past the end of the blob")
        );
        assert_eq!(
            bad(ChunkFacts {
                want_offset: u64::MAX,
                got_offset: u64::MAX,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk past the end of the blob")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_eof: true,
                ..chunk()
            }),
            ChunkVerdict::Bad("a chunk whose end mark contradicts its size")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_eof: false,
                ..last
            }),
            ChunkVerdict::Bad("a chunk whose end mark contradicts its size")
        );
        assert_eq!(
            bad(ChunkFacts {
                got_len: 0,
                ..chunk()
            }),
            ChunkVerdict::Bad("an empty chunk before the end")
        );
    }

    fn log() -> LogFacts {
        LogFacts {
            want_offset: 4,
            max: 4,
            limit: 8,
            got_offset: 4,
            got_len: 4,
            got_next: 8,
            got_eof: false,
        }
    }

    #[test]
    fn a_log_is_complete_only_at_its_end_mark() {
        assert_eq!(
            check_log(log()),
            LogVerdict::Append {
                next: 8,
                end: false
            }
        );
        assert_eq!(
            check_log(LogFacts {
                got_eof: true,
                ..log()
            }),
            LogVerdict::Append { next: 8, end: true }
        );
        let nothing = LogFacts {
            got_len: 0,
            got_next: 4,
            ..log()
        };
        assert_eq!(check_log(nothing), LogVerdict::Wait);
        assert_eq!(
            check_log(LogFacts {
                got_eof: true,
                ..nothing
            }),
            LogVerdict::Append { next: 4, end: true }
        );
        let bad = check_log;
        assert_eq!(
            bad(LogFacts {
                got_offset: 3,
                ..log()
            }),
            LogVerdict::Bad("log bytes at another offset")
        );
        assert_eq!(
            bad(LogFacts {
                got_len: 5,
                got_next: 9,
                ..log()
            }),
            LogVerdict::Bad("more log bytes than asked for")
        );
        assert_eq!(
            bad(LogFacts { limit: 7, ..log() }),
            LogVerdict::Bad("a log above the size limit")
        );
        assert_eq!(
            bad(LogFacts {
                want_offset: u64::MAX,
                got_offset: u64::MAX,
                limit: u64::MAX,
                ..log()
            }),
            LogVerdict::Bad("a log above the size limit")
        );
        assert_eq!(
            bad(LogFacts {
                got_next: 9,
                ..log()
            }),
            LogVerdict::Bad("a next offset that is not the end of the bytes")
        );
        assert_eq!(log_wait(true), LogWait::Retry);
        assert_eq!(log_wait(false), LogWait::TakeIncomplete);
    }

    #[test]
    fn a_refused_acknowledgement_restarts_only_for_another_result() {
        assert_eq!(judge_reread(false), Reread::Restart);
        assert_eq!(judge_reread(true), Reread::Contradiction);
    }

    #[test]
    fn a_task_stands_by_its_outcome_and_its_collection() {
        use Outcome::*;
        use TaskStanding as T;
        let t = task_standing;
        assert_eq!(t(None, None, false), T::Pending);
        assert_eq!(t(None, None, true), T::Cancelled);
        assert_eq!(t(None, Some(Verdict::Complete), false), T::Pending);
        // An attempt that ended is judged by itself, cancel request or not.
        assert_eq!(t(Some(Succeeded), None, true), T::Pending);
        assert_eq!(t(Some(Succeeded), None, false), T::Pending);
        assert_eq!(
            t(Some(Succeeded), Some(Verdict::Complete), false),
            T::Succeeded
        );
        assert_eq!(
            t(Some(Succeeded), Some(Verdict::RequiredMissing), false),
            T::RequiredArtifactMissing
        );
        for o in [Succeeded, Failed, Cancelled, TimedOut, Interrupted] {
            assert_eq!(
                t(Some(o), Some(Verdict::GaveUp), false),
                T::CollectionFailed
            );
        }
        for v in [Verdict::Complete, Verdict::RequiredMissing] {
            assert_eq!(t(Some(Cancelled), Some(v), false), T::Cancelled);
            for o in [Failed, TimedOut, Interrupted] {
                assert_eq!(t(Some(o), Some(v), false), T::Failed);
            }
        }
    }

    #[test]
    fn an_empty_or_short_set_of_results_is_never_a_success() {
        use TaskStanding as T;
        assert_eq!(run_verdict(0, &[]), RunVerdict::NoWork);
        assert_eq!(run_verdict(1, &[]), RunVerdict::Missing);
        assert_eq!(run_verdict(2, &[T::Succeeded]), RunVerdict::Missing);
        assert_eq!(run_verdict(0, &[T::Succeeded]), RunVerdict::Passed);
        assert_eq!(run_verdict(1, &[T::Succeeded]), RunVerdict::Passed);
        assert_eq!(
            run_verdict(2, &[T::Succeeded, T::Succeeded]),
            RunVerdict::Passed
        );
        assert_eq!(
            run_verdict(2, &[T::Failed, T::Pending]),
            RunVerdict::Collecting
        );
        for bad in [
            T::Failed,
            T::Cancelled,
            T::RequiredArtifactMissing,
            T::CollectionFailed,
        ] {
            assert_eq!(run_verdict(2, &[T::Succeeded, bad]), RunVerdict::Failed);
        }
    }
}
