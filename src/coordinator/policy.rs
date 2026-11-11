//! Pure decisions of the coordinator: whether a worker has room, and what to
//! do next about an attempt given what the parent recorded and what its
//! worker answered.
//!
//! Nothing here touches the database, the clock or a connection. The rules
//! (spec chapters 9 to 12) in one place:
//!
//! * no answer is not an answer: the attempt becomes `unknown` and is kept;
//! * an attempt is sent again only to its own worker, and only while that
//!   worker never acknowledged it;
//! * a cancel or a retire is delivered before any resend;
//! * a task is free for another worker only after the retire was confirmed
//!   by the worker that held it.

use crate::error::ErrorCode;
use crate::state::{AttemptState, Outcome};
use crate::storage::WorkerLoad;
use crate::wire::RetireStatus;

/// How many tasks a worker may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Tasks whose command may be running (`starting` onward).
    pub max_active_task: u32,
    /// Tasks placed beyond the active limit, waiting to start.
    pub prefetch_per_worker: u32,
}

impl Limits {
    /// The stricter of the coordinator's setting and the worker's own.
    pub fn stricter(self, other: Limits) -> Limits {
        Limits {
            max_active_task: self.max_active_task.min(other.max_active_task),
            prefetch_per_worker: self.prefetch_per_worker.min(other.prefetch_per_worker),
        }
    }
}

/// Whether one more task may be placed on a worker with this load: the
/// tasks it holds, started or not, stay below the active limit plus the
/// prefetch. So tasks that have not started fill the free active slots and
/// the prefetch, and a worker never holds more than both together.
pub fn has_room(load: WorkerLoad, limits: Limits) -> bool {
    let held = u64::from(load.active) + u64::from(load.pending);
    held < u64::from(limits.max_active_task) + u64::from(limits.prefetch_per_worker)
}

/// What the parent recorded about an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    /// The worker's acceptance was recorded.
    pub acked: bool,
    pub cancel_requested: bool,
    /// A retire was asked for and is not confirmed.
    pub retire_requested: bool,
}

/// What the worker answered when asked for the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The worker holds the attempt.
    Status(AttemptState),
    /// The worker has neither the attempt nor a tombstone for it.
    NotFound,
    /// A tombstone without an attempt behind it: it never arrived and can
    /// never start there.
    Tombstone,
    /// No answer. Nothing is known.
    Unreachable,
}

/// The next thing to do about an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Keep it as `unknown`: not sent again, not placed elsewhere, not failed.
    KeepUnknown,
    /// Ask the worker to retire it; only its answer frees the task.
    Retire,
    /// Deliver the cancel.
    Cancel,
    /// It was cancelled before it ever arrived: the parent fixes `cancelled`.
    SettleCancelled,
    /// Send the same attempt to the same worker (again).
    Submit,
    /// It lives on the worker: mirror its state and renew its lease.
    Follow,
    /// It ended on the worker: mirror the result.
    Settle,
}

/// Decide the next step. Retire comes before cancel, and both before a
/// resend.
pub fn next_step(facts: Facts, answer: Answer) -> Step {
    match answer {
        Answer::Unreachable => Step::KeepUnknown,
        _ if facts.retire_requested => Step::Retire,
        Answer::Status(AttemptState::Terminal) => Step::Settle,
        Answer::Status(state) => {
            if facts.cancel_requested && !cancel_delivered(state) {
                Step::Cancel
            } else {
                Step::Follow
            }
        }
        Answer::Tombstone => {
            if facts.cancel_requested {
                Step::SettleCancelled
            } else {
                Step::KeepUnknown
            }
        }
        Answer::NotFound => {
            if facts.cancel_requested {
                Step::Cancel
            } else if facts.acked {
                Step::KeepUnknown
            } else {
                Step::Submit
            }
        }
    }
}

/// A worker in one of these states is already acting on a cancel.
fn cancel_delivered(state: AttemptState) -> bool {
    matches!(state, AttemptState::Cancelling | AttemptState::Finalizing)
}

/// What a submit that was answered with an error means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Not taken now, and nothing says it never will be: the same attempt is
    /// sent to the same worker again later.
    Later,
    /// A tombstone answered: the next look at the attempt settles it.
    Tombstoned,
    /// A newer coordinator exists. This one stops.
    Fenced,
    /// The worker will never take this attempt: retire it there, then fail
    /// it here.
    Never,
}

/// Read the error a worker answered a submit with.
///
/// Only an answer that is about the attempt itself is final: its input is
/// invalid, or the worker holds the same attempt with another payload
/// (`payload_mismatch` says so by its typed reason). Every other code, a
/// fault of the worker, a conflict with something else it holds, a code
/// this version does not know, is no verdict on the attempt and never fails
/// the task.
pub fn submit_refusal(code: ErrorCode, payload_mismatch: bool) -> Refusal {
    match code {
        ErrorCode::Retired => Refusal::Tombstoned,
        ErrorCode::StaleEpoch => Refusal::Fenced,
        ErrorCode::InvalidInput => Refusal::Never,
        ErrorCode::Conflict if payload_mismatch => Refusal::Never,
        _ => Refusal::Later,
    }
}

/// How the attempt stands on its worker after a retire was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The worker holds only the tombstone: the attempt never arrived.
    Never,
    /// The attempt is terminal there, with this outcome.
    With(Outcome),
    /// No answer, or one that says neither of the above.
    Unknown,
}

/// What the answer to a retire means for the parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireVerdict {
    /// Never started, or stopped without completing: the retire is
    /// confirmed and the task may be placed again.
    Confirmed,
    /// The command ran to an end of its own, before the retire or while it
    /// was carried out: its result stands and the task is not placed again.
    Adopt,
    /// The worker has not stopped it yet. The attempt keeps its task and
    /// its slot.
    Wait,
    /// The worker says it is over, and how it ended is not known: the
    /// attempt is kept as `unknown`.
    Unknown,
}

/// The attempt is judged by how it ended, not by which word the retire was
/// answered with. The command may end on its own between the worker's look
/// and its stop (answered `stopped`), and a retire whose first answer was
/// lost is answered `already_terminal` the second time: all of these must
/// come to the conclusion the outcome gives.
pub fn retire_verdict(status: RetireStatus, ended: Ended) -> RetireVerdict {
    match (status, ended) {
        (RetireStatus::Unconfirmed, _) => RetireVerdict::Wait,
        (_, Ended::Unknown) => RetireVerdict::Unknown,
        (_, Ended::With(outcome)) => {
            if ended_on_its_own(outcome) {
                RetireVerdict::Adopt
            } else {
                RetireVerdict::Confirmed
            }
        }
        (RetireStatus::NotStarted, Ended::Never) => RetireVerdict::Confirmed,
        // Stopped or ended, and yet not there: the two answers contradict.
        (RetireStatus::Stopped | RetireStatus::AlreadyTerminal, Ended::Never) => {
            RetireVerdict::Unknown
        }
    }
}

/// The command ran to an end of its own (`cancelled` and `interrupted` are
/// stops from outside).
pub fn ended_on_its_own(outcome: Outcome) -> bool {
    matches!(
        outcome,
        Outcome::Succeeded | Outcome::Failed | Outcome::TimedOut
    )
}

/// Whether an attempt that was just placed may be sent: not when a cancel or
/// a retire for it was recorded in the meantime (those are delivered first,
/// by the next look at it).
pub fn may_send(facts: Facts) -> bool {
    !facts.cancel_requested && !facts.retire_requested
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(active: u32, pending: u32) -> WorkerLoad {
        WorkerLoad { active, pending }
    }

    fn limits(max_active_task: u32, prefetch_per_worker: u32) -> Limits {
        Limits {
            max_active_task,
            prefetch_per_worker,
        }
    }

    #[test]
    fn room_is_the_free_active_slots_plus_the_prefetch() {
        let l = limits(2, 1);
        assert!(has_room(load(0, 0), l));
        assert!(has_room(load(0, 2), l));
        assert!(!has_room(load(0, 3), l));
        assert!(has_room(load(1, 1), l));
        assert!(!has_room(load(1, 2), l));
        assert!(has_room(load(2, 0), l));
        assert!(!has_room(load(2, 1), l));
        assert!(!has_room(load(3, 0), l));
        assert!(!has_room(load(5, 0), l), "over the limit is not room");
        // No prefetch: only free active slots.
        assert!(has_room(load(1, 0), limits(2, 0)));
        assert!(!has_room(load(2, 0), limits(2, 0)));
        assert!(!has_room(load(0, 0), limits(0, 0)));
        assert!(has_room(load(0, 0), limits(0, 1)));
        // Neither sum wraps.
        assert!(!has_room(
            load(u32::MAX, u32::MAX),
            limits(u32::MAX, u32::MAX)
        ));
        assert!(has_room(load(u32::MAX, 0), limits(u32::MAX, 1)));
        assert!(has_room(load(1, u32::MAX), limits(u32::MAX, 2)));
        assert!(!has_room(load(1, u32::MAX), limits(u32::MAX, 1)));
    }

    #[test]
    fn the_stricter_limit_wins_field_by_field() {
        assert_eq!(limits(2, 1).stricter(limits(1, 3)), limits(1, 1));
        assert_eq!(limits(1, 3).stricter(limits(2, 1)), limits(1, 1));
        assert_eq!(limits(4, 0).stricter(limits(4, 0)), limits(4, 0));
    }

    fn facts(acked: bool, cancel_requested: bool, retire_requested: bool) -> Facts {
        Facts {
            acked,
            cancel_requested,
            retire_requested,
        }
    }

    const ANSWERS: [Answer; 5] = [
        Answer::Status(AttemptState::Active),
        Answer::Status(AttemptState::Terminal),
        Answer::NotFound,
        Answer::Tombstone,
        Answer::Unreachable,
    ];

    #[test]
    fn no_answer_keeps_the_attempt_unknown_whatever_was_asked_for() {
        for acked in [false, true] {
            for cancel in [false, true] {
                for retire in [false, true] {
                    let f = facts(acked, cancel, retire);
                    assert_eq!(next_step(f, Answer::Unreachable), Step::KeepUnknown);
                }
            }
        }
    }

    #[test]
    fn a_pending_retire_goes_first_on_every_answer() {
        for answer in ANSWERS {
            if answer == Answer::Unreachable {
                continue;
            }
            for acked in [false, true] {
                for cancel in [false, true] {
                    assert_eq!(
                        next_step(facts(acked, cancel, true), answer),
                        Step::Retire,
                        "{answer:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_attempt_the_worker_holds_is_followed_cancelled_or_settled() {
        for acked in [false, true] {
            for state in AttemptState::ALL {
                let plain = next_step(facts(acked, false, false), Answer::Status(state));
                let cancelled = next_step(facts(acked, true, false), Answer::Status(state));
                match state {
                    AttemptState::Terminal => {
                        assert_eq!(plain, Step::Settle);
                        assert_eq!(cancelled, Step::Settle);
                    }
                    AttemptState::Cancelling | AttemptState::Finalizing => {
                        assert_eq!(plain, Step::Follow);
                        assert_eq!(cancelled, Step::Follow, "{state:?} already stops");
                    }
                    _ => {
                        assert_eq!(plain, Step::Follow);
                        assert_eq!(cancelled, Step::Cancel, "{state:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn an_attempt_the_worker_does_not_hold_is_resent_only_while_unacknowledged() {
        assert_eq!(
            next_step(facts(false, false, false), Answer::NotFound),
            Step::Submit
        );
        assert_eq!(
            next_step(facts(true, false, false), Answer::NotFound),
            Step::KeepUnknown,
            "an acknowledged attempt that is gone is not started again"
        );
        // A cancel is delivered instead of a resend, then settled by the
        // tombstone it leaves.
        for acked in [false, true] {
            assert_eq!(
                next_step(facts(acked, true, false), Answer::NotFound),
                Step::Cancel
            );
            assert_eq!(
                next_step(facts(acked, true, false), Answer::Tombstone),
                Step::SettleCancelled
            );
            assert_eq!(
                next_step(facts(acked, false, false), Answer::Tombstone),
                Step::KeepUnknown
            );
        }
    }

    #[test]
    fn only_an_answer_about_the_attempt_itself_refuses_a_submit_for_good() {
        for code in ErrorCode::ALL {
            for mismatch in [false, true] {
                let want = match (code, mismatch) {
                    (ErrorCode::Retired, _) => Refusal::Tombstoned,
                    (ErrorCode::StaleEpoch, _) => Refusal::Fenced,
                    (ErrorCode::InvalidInput, _) => Refusal::Never,
                    (ErrorCode::Conflict, true) => Refusal::Never,
                    _ => Refusal::Later,
                };
                assert_eq!(submit_refusal(code, mismatch), want, "{code:?} {mismatch}");
            }
        }
        // A fault of the worker, or a conflict with something else it holds
        // (also one whose reason an older or newer peer did not name), is
        // not a verdict on the attempt.
        for code in [
            ErrorCode::Internal,
            ErrorCode::CommunicationUnknown,
            ErrorCode::Blocked,
            ErrorCode::NotImplemented,
            ErrorCode::ProtocolIncompatible,
            ErrorCode::NotFound,
            ErrorCode::Overloaded,
            ErrorCode::Conflict,
        ] {
            assert_eq!(submit_refusal(code, false), Refusal::Later, "{code:?}");
        }
        assert_eq!(submit_refusal(ErrorCode::Conflict, true), Refusal::Never);
        assert_eq!(submit_refusal(ErrorCode::Internal, true), Refusal::Later);
    }

    #[test]
    fn a_retire_is_judged_by_how_the_attempt_ended_whatever_the_answer_was() {
        use Outcome::*;
        use RetireStatus::*;
        use RetireVerdict::{Adopt, Confirmed, Wait};
        let all = [Succeeded, Failed, Cancelled, TimedOut, Interrupted];
        // Not stopped yet: nothing else matters.
        assert_eq!(retire_verdict(Unconfirmed, Ended::Never), Wait);
        assert_eq!(retire_verdict(Unconfirmed, Ended::Unknown), Wait);
        for o in all {
            assert_eq!(retire_verdict(Unconfirmed, Ended::With(o)), Wait);
        }
        for status in [NotStarted, Stopped, AlreadyTerminal] {
            // How it ended is not known: never a reason to place it again.
            assert_eq!(
                retire_verdict(status, Ended::Unknown),
                RetireVerdict::Unknown
            );
            // The same conclusion for every word, also for `stopped` when
            // the command ended on its own while it was being stopped.
            assert_eq!(retire_verdict(status, Ended::With(Succeeded)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(Failed)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(TimedOut)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(Cancelled)), Confirmed);
            assert_eq!(retire_verdict(status, Ended::With(Interrupted)), Confirmed);
        }
        // Only a tombstone: it never arrived. Any other word contradicts.
        assert_eq!(retire_verdict(NotStarted, Ended::Never), Confirmed);
        assert_eq!(
            retire_verdict(Stopped, Ended::Never),
            RetireVerdict::Unknown
        );
        assert_eq!(
            retire_verdict(AlreadyTerminal, Ended::Never),
            RetireVerdict::Unknown
        );
        assert!(ended_on_its_own(Succeeded));
        assert!(ended_on_its_own(Failed));
        assert!(ended_on_its_own(TimedOut));
        assert!(!ended_on_its_own(Cancelled));
        assert!(!ended_on_its_own(Interrupted));
    }

    #[test]
    fn a_placed_attempt_is_sent_only_without_a_cancel_or_a_retire() {
        for acked in [false, true] {
            assert!(may_send(facts(acked, false, false)));
            assert!(!may_send(facts(acked, true, false)));
            assert!(!may_send(facts(acked, false, true)));
            assert!(!may_send(facts(acked, true, true)));
        }
    }
}
