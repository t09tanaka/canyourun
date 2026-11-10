//! How an attempt's result follows from why it stopped and how the command
//! ended (spec chapter 12). Pure.

use crate::state::{Exit, Outcome, Termination, TimeoutInfo};

/// Why the supervisor stopped the command (or refused to start it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The user asked for cancellation.
    Cancel,
    /// The execution lease ran out.
    LeaseExpired,
    /// One of the three timeouts fired.
    Timeout(TimeoutInfo),
}

/// What made the supervisor act, from the facts seen in one look. Explicit
/// cancellation wins over everything, an expired lease over a timeout (the
/// worker is acting without its parent then), and a timeout comes last.
pub fn pick_reason(
    cancel_requested: bool,
    lease_expired: bool,
    timeout: Option<TimeoutInfo>,
) -> Option<StopReason> {
    if cancel_requested {
        Some(StopReason::Cancel)
    } else if lease_expired {
        Some(StopReason::LeaseExpired)
    } else {
        timeout.map(StopReason::Timeout)
    }
}

/// The attempt's result. `reason` is `Some` only when the supervisor sent a
/// stop (or refused the start); a command that ended by itself first keeps
/// its own result even if a cancel was requested at the same moment (one
/// terminal, decided by whichever commits first). `exit` is `None` when no
/// command ran or could not be reaped.
pub fn termination(reason: Option<StopReason>, exit: Option<Exit>) -> Termination {
    let (outcome, timeout) = match reason {
        Some(StopReason::Cancel) => (Outcome::Cancelled, None),
        Some(StopReason::LeaseExpired) => (Outcome::Interrupted, None),
        Some(StopReason::Timeout(t)) => (Outcome::TimedOut, Some(t)),
        None => match exit {
            Some(Exit::Code(0)) => (Outcome::Succeeded, None),
            Some(_) => (Outcome::Failed, None),
            // The command was lost: neither success nor an ordinary failure.
            None => (Outcome::Interrupted, None),
        },
    };
    Termination::new(outcome, exit, timeout)
        .unwrap_or_else(|_| Termination::new(Outcome::Interrupted, None, None).expect("valid"))
}

/// True when the next node of the attempt may run after a node ended this way.
pub fn may_continue(reason: Option<StopReason>, exit: Option<Exit>) -> bool {
    reason.is_none() && exit == Some(Exit::Code(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{TimeoutKind, TimeoutPhase};

    fn t(kind: TimeoutKind, phase: TimeoutPhase) -> TimeoutInfo {
        TimeoutInfo { kind, phase }
    }

    #[test]
    fn the_reason_priority_is_cancel_then_lease_then_timeout() {
        let info = t(TimeoutKind::Command, TimeoutPhase::Running);
        let to = Some(info);
        assert_eq!(pick_reason(true, true, to), Some(StopReason::Cancel));
        assert_eq!(pick_reason(false, true, to), Some(StopReason::LeaseExpired));
        assert_eq!(
            pick_reason(false, false, to),
            Some(StopReason::Timeout(info))
        );
        assert_eq!(pick_reason(false, false, None), None);
        assert_eq!(pick_reason(true, false, None), Some(StopReason::Cancel));
    }

    #[test]
    fn a_natural_end_keeps_its_own_result() {
        let ok = termination(None, Some(Exit::Code(0)));
        assert_eq!(ok.outcome(), Outcome::Succeeded);
        assert_eq!(ok.exit(), Some(Exit::Code(0)));
        let bad = termination(None, Some(Exit::Code(3)));
        assert_eq!(bad.outcome(), Outcome::Failed);
        assert_eq!(bad.exit(), Some(Exit::Code(3)));
        let sig = termination(None, Some(Exit::Signal(9)));
        assert_eq!(sig.outcome(), Outcome::Failed);
        assert_eq!(sig.exit(), Some(Exit::Signal(9)));
    }

    #[test]
    fn a_stop_decides_the_outcome_and_keeps_how_the_command_ended() {
        let c = termination(Some(StopReason::Cancel), Some(Exit::Signal(15)));
        assert_eq!(c.outcome(), Outcome::Cancelled);
        assert_eq!(c.exit(), Some(Exit::Signal(15)));
        // Even a clean exit during the stop is a cancel.
        let c = termination(Some(StopReason::Cancel), Some(Exit::Code(0)));
        assert_eq!(c.outcome(), Outcome::Cancelled);
        let l = termination(Some(StopReason::LeaseExpired), Some(Exit::Signal(9)));
        assert_eq!(l.outcome(), Outcome::Interrupted);
        assert_eq!(l.exit(), Some(Exit::Signal(9)));
        let info = t(TimeoutKind::Deadline, TimeoutPhase::Waiting);
        let d = termination(Some(StopReason::Timeout(info)), None);
        assert_eq!(d.outcome(), Outcome::TimedOut);
        assert_eq!(d.timeout(), Some(info));
        assert_eq!(d.exit(), None);
    }

    #[test]
    fn no_exit_and_no_reason_is_interrupted_not_a_success() {
        let x = termination(None, None);
        assert_eq!(x.outcome(), Outcome::Interrupted);
        assert_eq!(x.exit(), None);
    }

    #[test]
    fn an_impossible_exit_falls_back_to_interrupted() {
        let x = termination(None, Some(Exit::Code(999)));
        assert_eq!(x.outcome(), Outcome::Interrupted);
        assert_eq!(x.exit(), None);
        // An inconsistent timeout info (command timeout while waiting) too.
        let bad = t(TimeoutKind::Command, TimeoutPhase::Waiting);
        let y = termination(Some(StopReason::Timeout(bad)), None);
        assert_eq!(y.outcome(), Outcome::Interrupted);
    }

    #[test]
    fn only_a_clean_natural_end_lets_the_next_node_run() {
        assert!(may_continue(None, Some(Exit::Code(0))));
        assert!(!may_continue(None, Some(Exit::Code(1))));
        assert!(!may_continue(None, Some(Exit::Signal(9))));
        assert!(!may_continue(None, None));
        assert!(!may_continue(Some(StopReason::Cancel), Some(Exit::Code(0))));
    }
}
