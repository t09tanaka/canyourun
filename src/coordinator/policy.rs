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
    /// Nothing was stored; the same attempt is sent again later.
    Later,
    /// A tombstone answered: the next look at the attempt settles it.
    Tombstoned,
    /// A newer coordinator exists. This one stops.
    Fenced,
    /// The worker will never take this attempt: retire it there, then fail
    /// it here.
    Never,
}

pub fn submit_refusal(code: ErrorCode) -> Refusal {
    match code {
        ErrorCode::Overloaded => Refusal::Later,
        ErrorCode::Retired => Refusal::Tombstoned,
        ErrorCode::StaleEpoch => Refusal::Fenced,
        _ => Refusal::Never,
    }
}

/// What the answer to a retire means for the parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireVerdict {
    /// Never started, or stopped without completing: the retire is
    /// confirmed and the task may be placed again.
    Confirmed,
    /// The attempt had ended on its own before the retire: its result
    /// stands and the task is not placed again.
    Adopt,
    /// Not confirmed. The attempt keeps its task and its slot.
    Wait,
}

/// `ended` is the outcome the worker reports for the attempt after the
/// retire, when it is terminal there.
///
/// An attempt that was already terminal is told apart by its outcome, not
/// by which answer said so: a retire whose first answer was lost is answered
/// `already_terminal` the second time, and must free the task exactly as the
/// first answer (`stopped`) would have.
pub fn retire_verdict(status: RetireStatus, ended: Option<Outcome>) -> RetireVerdict {
    match (status, ended) {
        (RetireStatus::NotStarted | RetireStatus::Stopped, _) => RetireVerdict::Confirmed,
        (RetireStatus::Unconfirmed, _) | (RetireStatus::AlreadyTerminal, None) => {
            RetireVerdict::Wait
        }
        (RetireStatus::AlreadyTerminal, Some(outcome)) => {
            if ended_on_its_own(outcome) {
                RetireVerdict::Adopt
            } else {
                RetireVerdict::Confirmed
            }
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
    fn a_refused_submit_is_read_by_its_code() {
        for code in ErrorCode::ALL {
            let want = match code {
                ErrorCode::Overloaded => Refusal::Later,
                ErrorCode::Retired => Refusal::Tombstoned,
                ErrorCode::StaleEpoch => Refusal::Fenced,
                _ => Refusal::Never,
            };
            assert_eq!(submit_refusal(code), want, "{code:?}");
        }
        assert_eq!(submit_refusal(ErrorCode::InvalidInput), Refusal::Never);
        assert_eq!(submit_refusal(ErrorCode::Conflict), Refusal::Never);
    }

    #[test]
    fn a_retire_is_confirmed_by_a_stop_and_not_by_an_end_of_its_own() {
        use Outcome::*;
        use RetireStatus::*;
        use RetireVerdict::*;
        let outcomes = [
            None,
            Some(Succeeded),
            Some(Failed),
            Some(Cancelled),
            Some(TimedOut),
            Some(Interrupted),
        ];
        for ended in outcomes {
            assert_eq!(retire_verdict(NotStarted, ended), Confirmed);
            assert_eq!(retire_verdict(Stopped, ended), Confirmed);
            assert_eq!(retire_verdict(Unconfirmed, ended), Wait);
        }
        assert_eq!(retire_verdict(AlreadyTerminal, None), Wait);
        assert_eq!(retire_verdict(AlreadyTerminal, Some(Succeeded)), Adopt);
        assert_eq!(retire_verdict(AlreadyTerminal, Some(Failed)), Adopt);
        assert_eq!(retire_verdict(AlreadyTerminal, Some(TimedOut)), Adopt);
        // What a lost first answer (`stopped`) looks like the second time.
        assert_eq!(retire_verdict(AlreadyTerminal, Some(Cancelled)), Confirmed);
        assert_eq!(
            retire_verdict(AlreadyTerminal, Some(Interrupted)),
            Confirmed
        );
        assert!(ended_on_its_own(Succeeded));
        assert!(ended_on_its_own(Failed));
        assert!(ended_on_its_own(TimedOut));
        assert!(!ended_on_its_own(Cancelled));
        assert!(!ended_on_its_own(Interrupted));
    }
}
