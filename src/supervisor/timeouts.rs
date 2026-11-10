//! The three timeouts (spec chapter 12). They are separate budgets and never
//! share a counter:
//!
//! - command timeout: the command's own run time, counted from its start.
//!   Queue wait is not part of it.
//! - resource-wait timeout: time spent waiting for admission.
//! - deadline: the whole task, counted from the attempt's start, across
//!   every phase; the record says which phase it fired in.
//!
//! Everything is on the monotonic clock that includes suspend, and the
//! decision is a pure function of the current time.

use crate::state::{TimeoutInfo, TimeoutKind, TimeoutPhase};

/// The configured limits. `None` means no limit of that kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeoutPolicy {
    pub command_ms: Option<u64>,
    pub resource_wait_ms: Option<u64>,
    pub deadline_ms: Option<u64>,
}

/// What the attempt is doing now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Waiting for admission since `since_ms`.
    Waiting { since_ms: u64 },
    /// The command has been running since `since_ms`.
    Running { since_ms: u64 },
}

impl Stage {
    fn phase(self) -> TimeoutPhase {
        match self {
            Stage::Waiting { .. } => TimeoutPhase::Waiting,
            Stage::Running { .. } => TimeoutPhase::Running,
        }
    }
}

/// Which timeout, if any, has fired at `now_ms`. `attempt_started_ms` is when
/// the deadline began to count. When several are due, the one whose due time
/// came first is reported; a tie goes to the narrower budget (command or
/// resource wait) over the deadline.
pub fn fired(
    policy: &TimeoutPolicy,
    attempt_started_ms: u64,
    stage: Stage,
    now_ms: u64,
) -> Option<TimeoutInfo> {
    let specific = match stage {
        Stage::Running { since_ms } => policy
            .command_ms
            .map(|limit| (since_ms.saturating_add(limit), TimeoutKind::Command)),
        Stage::Waiting { since_ms } => policy
            .resource_wait_ms
            .map(|limit| (since_ms.saturating_add(limit), TimeoutKind::ResourceWait)),
    };
    let deadline = policy.deadline_ms.map(|limit| {
        (
            attempt_started_ms.saturating_add(limit),
            TimeoutKind::Deadline,
        )
    });
    let mut due: Vec<(u64, u8, TimeoutKind)> = Vec::new();
    if let Some((at, kind)) = specific {
        due.push((at, 0, kind));
    }
    if let Some((at, kind)) = deadline {
        due.push((at, 1, kind));
    }
    due.into_iter()
        .filter(|(at, _, _)| now_ms >= *at)
        .min_by_key(|(at, rank, _)| (*at, *rank))
        .map(|(_, _, kind)| TimeoutInfo {
            kind,
            phase: stage.phase(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(c: Option<u64>, r: Option<u64>, d: Option<u64>) -> TimeoutPolicy {
        TimeoutPolicy {
            command_ms: c,
            resource_wait_ms: r,
            deadline_ms: d,
        }
    }

    fn info(kind: TimeoutKind, phase: TimeoutPhase) -> Option<TimeoutInfo> {
        Some(TimeoutInfo { kind, phase })
    }

    #[test]
    fn no_limits_never_fire() {
        let p = TimeoutPolicy::default();
        assert_eq!(fired(&p, 0, Stage::Running { since_ms: 0 }, u64::MAX), None);
        assert_eq!(fired(&p, 0, Stage::Waiting { since_ms: 0 }, u64::MAX), None);
    }

    #[test]
    fn the_command_timeout_counts_from_the_run_start_not_the_queue() {
        let p = policy(Some(1_000), None, None);
        // Queued for a long time, started at 50_000: the budget starts there.
        let s = Stage::Running { since_ms: 50_000 };
        assert_eq!(fired(&p, 0, s, 50_999), None);
        assert_eq!(
            fired(&p, 0, s, 51_000),
            info(TimeoutKind::Command, TimeoutPhase::Running)
        );
    }

    #[test]
    fn the_command_timeout_does_not_apply_while_waiting() {
        let p = policy(Some(1), None, None);
        assert_eq!(fired(&p, 0, Stage::Waiting { since_ms: 0 }, 99_999), None);
    }

    #[test]
    fn the_resource_wait_timeout_counts_the_wait_only() {
        let p = policy(None, Some(2_000), None);
        let s = Stage::Waiting { since_ms: 10_000 };
        assert_eq!(fired(&p, 0, s, 11_999), None);
        assert_eq!(
            fired(&p, 0, s, 12_000),
            info(TimeoutKind::ResourceWait, TimeoutPhase::Waiting)
        );
        // It does not apply once running.
        assert_eq!(
            fired(&p, 0, Stage::Running { since_ms: 10_000 }, 99_999),
            None
        );
    }

    #[test]
    fn the_deadline_counts_from_the_attempt_start_in_either_phase() {
        let p = policy(None, None, Some(5_000));
        assert_eq!(
            fired(&p, 1_000, Stage::Waiting { since_ms: 1_000 }, 5_999),
            None
        );
        assert_eq!(
            fired(&p, 1_000, Stage::Waiting { since_ms: 1_000 }, 6_000),
            info(TimeoutKind::Deadline, TimeoutPhase::Waiting)
        );
        assert_eq!(
            fired(&p, 1_000, Stage::Running { since_ms: 3_000 }, 6_000),
            info(TimeoutKind::Deadline, TimeoutPhase::Running)
        );
    }

    #[test]
    fn the_earlier_due_time_is_reported() {
        let p = policy(Some(10_000), None, Some(4_000));
        // Deadline due at 4_000, command due at 11_000.
        let s = Stage::Running { since_ms: 1_000 };
        assert_eq!(
            fired(&p, 0, s, 20_000),
            info(TimeoutKind::Deadline, TimeoutPhase::Running)
        );
        let p = policy(Some(1_000), None, Some(9_000));
        assert_eq!(
            fired(&p, 0, s, 20_000),
            info(TimeoutKind::Command, TimeoutPhase::Running)
        );
    }

    #[test]
    fn a_tie_goes_to_the_narrower_budget() {
        let p = policy(Some(2_000), None, Some(3_000));
        let s = Stage::Running { since_ms: 1_000 };
        assert_eq!(
            fired(&p, 0, s, 3_000),
            info(TimeoutKind::Command, TimeoutPhase::Running)
        );
        let p = policy(None, Some(2_000), Some(3_000));
        let s = Stage::Waiting { since_ms: 1_000 };
        assert_eq!(
            fired(&p, 0, s, 3_000),
            info(TimeoutKind::ResourceWait, TimeoutPhase::Waiting)
        );
    }

    #[test]
    fn only_due_timeouts_are_candidates() {
        // The deadline is earlier-configured but the command is the one due.
        let p = policy(Some(100), None, Some(1_000_000));
        let s = Stage::Running { since_ms: 0 };
        assert_eq!(
            fired(&p, 0, s, 100),
            info(TimeoutKind::Command, TimeoutPhase::Running)
        );
    }

    #[test]
    fn huge_limits_do_not_overflow() {
        let p = policy(Some(u64::MAX), None, Some(u64::MAX));
        assert_eq!(
            fired(
                &p,
                u64::MAX - 1,
                Stage::Running {
                    since_ms: u64::MAX - 1
                },
                u64::MAX - 1
            ),
            None
        );
    }
}
