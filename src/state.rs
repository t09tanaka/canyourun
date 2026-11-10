//! The attempt state machine and the status vocabularies around it (spec chapter 10).

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Execution state of one attempt.
///
/// `Accepted -> Preparing -> Ready -> Starting -> Active -> Finalizing -> Terminal`.
/// Every non-final stage can move to `Cancelling`, and `Cancelling` always goes
/// through `Finalizing`. Preparation and start failures also finalize through
/// `Finalizing` (spec: "各段階の準備失敗・起動失敗等もfinalizingを通して確定する").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Accepted,
    Preparing,
    Ready,
    Starting,
    Active,
    Cancelling,
    Finalizing,
    Terminal,
}

/// A transition the state machine does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("illegal attempt transition {from:?} -> {to:?}")]
pub struct TransitionError {
    pub from: AttemptState,
    pub to: AttemptState,
}

impl AttemptState {
    /// Every state, in lifecycle order.
    pub const ALL: [AttemptState; 8] = [
        AttemptState::Accepted,
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Cancelling,
        AttemptState::Finalizing,
        AttemptState::Terminal,
    ];

    /// The states reachable in one step.
    pub fn successors(self) -> &'static [AttemptState] {
        use AttemptState::*;
        match self {
            Accepted => &[Preparing, Cancelling, Finalizing],
            Preparing => &[Ready, Cancelling, Finalizing],
            Ready => &[Starting, Cancelling, Finalizing],
            Starting => &[Active, Cancelling, Finalizing],
            Active => &[Finalizing, Cancelling],
            Cancelling => &[Finalizing],
            Finalizing => &[Terminal],
            Terminal => &[],
        }
    }

    pub fn can_transition(self, to: AttemptState) -> bool {
        self.successors().contains(&to)
    }

    /// Move to `to`, or fail without changing anything.
    pub fn transition(self, to: AttemptState) -> Result<AttemptState, TransitionError> {
        if self.can_transition(to) {
            Ok(to)
        } else {
            Err(TransitionError { from: self, to })
        }
    }

    pub fn is_terminal(self) -> bool {
        self == AttemptState::Terminal
    }

    /// True while a new user command must not be spawned.
    pub fn spawn_forbidden(self) -> bool {
        matches!(
            self,
            AttemptState::Cancelling | AttemptState::Finalizing | AttemptState::Terminal
        )
    }
}

/// Final result of an attempt, fixed once in `Finalizing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

/// Communication state, kept apart from the execution state. A disconnect is
/// neither a failure nor "not run".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Observation {
    Known,
    Reconciling,
    Unknown,
}

/// Step state inside an `Active` attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    WaitingResources,
    Running,
}

/// How far an attempt's acceptance has been confirmed (spec "受理の段階").
/// Ordered: a later receipt implies the earlier ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Receipt {
    CoordinatorAccepted,
    WorkerAccepted,
    CommandRegistered,
    Running,
    Terminal,
    Collected,
}

impl Receipt {
    /// The next stage, if any.
    pub fn next(self) -> Option<Receipt> {
        match self {
            Receipt::CoordinatorAccepted => Some(Receipt::WorkerAccepted),
            Receipt::WorkerAccepted => Some(Receipt::CommandRegistered),
            Receipt::CommandRegistered => Some(Receipt::Running),
            Receipt::Running => Some(Receipt::Terminal),
            Receipt::Terminal => Some(Receipt::Collected),
            Receipt::Collected => None,
        }
    }
}

/// What a user sees for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunDisplay {
    Queued,
    Preparing,
    Running,
    Cancelling,
    /// The worker finished but required outputs are still being fetched.
    Collecting,
    /// Communication is unconfirmed; execution state cannot be asserted.
    Unknown,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl RunDisplay {
    /// Derive the display state from the attempt state, its outcome, the
    /// observation state, and whether required outputs were collected.
    pub fn derive(
        state: AttemptState,
        outcome: Option<Outcome>,
        observation: Observation,
        collected: bool,
    ) -> RunDisplay {
        if observation == Observation::Unknown {
            return RunDisplay::Unknown;
        }
        match (state, outcome) {
            (AttemptState::Terminal, Some(outcome)) if collected => match outcome {
                Outcome::Succeeded => RunDisplay::Succeeded,
                Outcome::Failed => RunDisplay::Failed,
                Outcome::Cancelled => RunDisplay::Cancelled,
                Outcome::TimedOut => RunDisplay::TimedOut,
                Outcome::Interrupted => RunDisplay::Interrupted,
            },
            (AttemptState::Terminal, _) | (AttemptState::Finalizing, _) => RunDisplay::Collecting,
            (AttemptState::Cancelling, _) => RunDisplay::Cancelling,
            (AttemptState::Accepted, _) | (AttemptState::Ready, _) => RunDisplay::Queued,
            (AttemptState::Preparing, _) => RunDisplay::Preparing,
            (AttemptState::Starting, _) | (AttemptState::Active, _) => RunDisplay::Running,
        }
    }
}

/// Completeness of a result set. An empty or partial set is never success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    /// Every expected unit is terminal or explicitly skipped.
    Complete,
    /// A filter selected zero units. Distinct from "tests passed".
    NoWork,
    /// Results are missing or do not match the expectation.
    Incomplete,
}

impl Completeness {
    /// `expected`: units the plan requires; `accounted`: units that are
    /// terminal or carry an explicit skip reason; `filtered_to_zero`: a filter
    /// removed every unit on purpose.
    pub fn classify(expected: usize, accounted: usize, filtered_to_zero: bool) -> Completeness {
        if expected == 0 {
            if filtered_to_zero {
                Completeness::NoWork
            } else {
                Completeness::Incomplete
            }
        } else if accounted == expected {
            Completeness::Complete
        } else {
            Completeness::Incomplete
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use AttemptState::*;

    #[test]
    fn transition_table_is_exact() {
        let allowed: &[(AttemptState, AttemptState)] = &[
            (Accepted, Preparing),
            (Accepted, Cancelling),
            (Accepted, Finalizing),
            (Preparing, Ready),
            (Preparing, Cancelling),
            (Preparing, Finalizing),
            (Ready, Starting),
            (Ready, Cancelling),
            (Ready, Finalizing),
            (Starting, Active),
            (Starting, Cancelling),
            (Starting, Finalizing),
            (Active, Finalizing),
            (Active, Cancelling),
            (Cancelling, Finalizing),
            (Finalizing, Terminal),
        ];
        for from in AttemptState::ALL {
            for to in AttemptState::ALL {
                let expect = allowed.contains(&(from, to));
                assert_eq!(from.can_transition(to), expect, "{from:?} -> {to:?}");
                match from.transition(to) {
                    Ok(s) => assert!(expect && s == to),
                    Err(e) => {
                        assert!(!expect);
                        assert_eq!(e, TransitionError { from, to });
                        assert!(e.to_string().contains("illegal"));
                    }
                }
            }
        }
    }

    #[test]
    fn happy_path_and_cancel_path_reach_terminal() {
        let mut s = Accepted;
        for next in [Preparing, Ready, Starting, Active, Finalizing, Terminal] {
            s = s.transition(next).unwrap();
        }
        assert!(s.is_terminal());
        let s = Active
            .transition(Cancelling)
            .unwrap()
            .transition(Finalizing)
            .unwrap();
        assert_eq!(s.transition(Terminal).unwrap(), Terminal);
        assert!(!Active.is_terminal());
        assert!(Terminal.successors().is_empty());
    }

    #[test]
    fn spawn_is_forbidden_once_cancelling_or_later() {
        for s in AttemptState::ALL {
            let expect = matches!(s, Cancelling | Finalizing | Terminal);
            assert_eq!(s.spawn_forbidden(), expect, "{s:?}");
        }
    }

    #[test]
    fn enums_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&AttemptState::Cancelling).unwrap(),
            "\"cancelling\""
        );
        assert_eq!(
            serde_json::to_string(&Outcome::TimedOut).unwrap(),
            "\"timed_out\""
        );
        assert_eq!(
            serde_json::to_string(&Observation::Reconciling).unwrap(),
            "\"reconciling\""
        );
        assert_eq!(
            serde_json::to_string(&StepState::WaitingResources).unwrap(),
            "\"waiting_resources\""
        );
        assert_eq!(
            serde_json::to_string(&Receipt::CommandRegistered).unwrap(),
            "\"command_registered\""
        );
        assert_eq!(
            serde_json::to_string(&RunDisplay::Collecting).unwrap(),
            "\"collecting\""
        );
        assert_eq!(
            serde_json::to_string(&Completeness::NoWork).unwrap(),
            "\"no_work\""
        );
        for s in AttemptState::ALL {
            let j = serde_json::to_string(&s).unwrap();
            assert_eq!(serde_json::from_str::<AttemptState>(&j).unwrap(), s);
        }
        for o in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::Cancelled,
            Outcome::Interrupted,
        ] {
            let j = serde_json::to_string(&o).unwrap();
            assert_eq!(serde_json::from_str::<Outcome>(&j).unwrap(), o);
        }
        for o in [Observation::Known, Observation::Unknown] {
            let j = serde_json::to_string(&o).unwrap();
            assert_eq!(serde_json::from_str::<Observation>(&j).unwrap(), o);
        }
        assert_eq!(
            serde_json::to_string(&StepState::Running).unwrap(),
            "\"running\""
        );
    }

    #[test]
    fn receipts_are_ordered_and_chain_to_collected() {
        let mut r = Receipt::CoordinatorAccepted;
        let mut seen = vec![r];
        while let Some(n) = r.next() {
            assert!(n > r);
            seen.push(n);
            r = n;
        }
        assert_eq!(seen.len(), 6);
        assert_eq!(r, Receipt::Collected);
        assert_eq!(Receipt::Collected.next(), None);
    }

    #[test]
    fn display_unknown_observation_wins() {
        for s in AttemptState::ALL {
            assert_eq!(
                RunDisplay::derive(s, Some(Outcome::Succeeded), Observation::Unknown, true),
                RunDisplay::Unknown
            );
        }
        // reconciling is not unknown
        assert_eq!(
            RunDisplay::derive(Active, None, Observation::Reconciling, false),
            RunDisplay::Running
        );
    }

    #[test]
    fn display_by_state() {
        let d = |s, o, c| RunDisplay::derive(s, o, Observation::Known, c);
        assert_eq!(d(Accepted, None, false), RunDisplay::Queued);
        assert_eq!(d(Ready, None, false), RunDisplay::Queued);
        assert_eq!(d(Preparing, None, false), RunDisplay::Preparing);
        assert_eq!(d(Starting, None, false), RunDisplay::Running);
        assert_eq!(d(Active, None, false), RunDisplay::Running);
        assert_eq!(d(Cancelling, None, false), RunDisplay::Cancelling);
        assert_eq!(d(Finalizing, None, false), RunDisplay::Collecting);
    }

    #[test]
    fn display_terminal_waits_for_collection() {
        let d = |o, c| RunDisplay::derive(Terminal, o, Observation::Known, c);
        for o in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::Cancelled,
            Outcome::TimedOut,
            Outcome::Interrupted,
        ] {
            assert_eq!(d(Some(o), false), RunDisplay::Collecting);
        }
        assert_eq!(d(Some(Outcome::Succeeded), true), RunDisplay::Succeeded);
        assert_eq!(d(Some(Outcome::Failed), true), RunDisplay::Failed);
        assert_eq!(d(Some(Outcome::Cancelled), true), RunDisplay::Cancelled);
        assert_eq!(d(Some(Outcome::TimedOut), true), RunDisplay::TimedOut);
        assert_eq!(d(Some(Outcome::Interrupted), true), RunDisplay::Interrupted);
        // terminal without a recorded outcome is never shown as finished
        assert_eq!(d(None, true), RunDisplay::Collecting);
    }

    #[test]
    fn completeness_separates_no_work_from_incomplete() {
        assert_eq!(Completeness::classify(3, 3, false), Completeness::Complete);
        assert_eq!(
            Completeness::classify(3, 2, false),
            Completeness::Incomplete
        );
        assert_eq!(
            Completeness::classify(3, 4, false),
            Completeness::Incomplete
        );
        assert_eq!(Completeness::classify(3, 3, true), Completeness::Complete);
        assert_eq!(Completeness::classify(0, 0, true), Completeness::NoWork);
        assert_eq!(
            Completeness::classify(0, 0, false),
            Completeness::Incomplete
        );
    }
}
