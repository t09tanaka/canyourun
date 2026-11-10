//! The attempt state machine and the status vocabularies around it (spec chapter 10).

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::{MAX_EXIT_CODE, MAX_SIGNAL};

/// Execution state of one attempt.
///
/// `Accepted -> Preparing -> Ready -> Starting -> Active -> Finalizing -> Terminal`.
/// Every non-final stage can move to `Cancelling`, and `Cancelling` always goes
/// through `Finalizing`. Preparation and start failures also finalize through
/// `Finalizing` (spec: "各段階の準備失敗・起動失敗等もfinalizingを通して確定する").
///
/// `Finalizing -> Terminal` is the one transition that writes the result, so it
/// does not go through [`AttemptState::transition`]: only
/// [`AttemptState::finalize`] takes it, and it needs a [`Termination`]. A
/// terminal attempt therefore always has exactly one termination, written once.
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
pub enum TransitionError {
    #[error("illegal attempt transition {from:?} -> {to:?}")]
    Illegal {
        from: AttemptState,
        to: AttemptState,
    },
    /// `Finalizing -> Terminal` must carry the result.
    #[error("Finalizing -> Terminal only happens through finalize(termination)")]
    FinalizeRequired,
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

    /// Whether the table has an edge `self -> to`. `Finalizing -> Terminal` is
    /// an edge, but only [`AttemptState::finalize`] may take it.
    pub fn can_transition(self, to: AttemptState) -> bool {
        self.successors().contains(&to)
    }

    /// Move to `to`, or fail without changing anything. Never reaches
    /// `Terminal`; use [`AttemptState::finalize`] for that.
    pub fn transition(self, to: AttemptState) -> Result<AttemptState, TransitionError> {
        if self == AttemptState::Finalizing && to == AttemptState::Terminal {
            Err(TransitionError::FinalizeRequired)
        } else if self.can_transition(to) {
            Ok(to)
        } else {
            Err(TransitionError::Illegal { from: self, to })
        }
    }

    /// `Finalizing -> Terminal`, fixing the result. Returns the new state and
    /// the termination to persist with it, in the same commit.
    pub fn finalize(
        self,
        termination: Termination,
    ) -> Result<(AttemptState, Termination), TransitionError> {
        if self == AttemptState::Finalizing {
            Ok((AttemptState::Terminal, termination))
        } else {
            Err(TransitionError::Illegal {
                from: self,
                to: AttemptState::Terminal,
            })
        }
    }

    pub fn is_terminal(self) -> bool {
        self == AttemptState::Terminal
    }

    /// True when the commit `from -> to` is the one that grants spawn
    /// authority. The authority to start the user command is taken exactly
    /// once, by the conditional commit `Ready -> Starting`, before the spawn.
    /// No other transition grants it, and nothing later can take it again.
    pub fn grants_spawn(from: AttemptState, to: AttemptState) -> bool {
        from == AttemptState::Ready && to == AttemptState::Starting
    }

    /// True when recovery from this state must never spawn the user command
    /// again. From `Starting` onward the spawn authority was either consumed
    /// or withdrawn (`Cancelling`, `Finalizing`, `Terminal`), and whether the
    /// process was actually launched may be unknown. Recovery then checks the
    /// recorded process identity (boot ID and start identity), and
    /// investigates, stops, and settles `interrupted`; it does not run the
    /// command again. Only `Accepted`, `Preparing` and `Ready` have not yet
    /// consumed the authority.
    pub fn recovery_must_not_respawn(self) -> bool {
        !matches!(
            self,
            AttemptState::Accepted | AttemptState::Preparing | AttemptState::Ready
        )
    }
}

/// Final result of an attempt, fixed once by [`AttemptState::finalize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

/// How the user command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exit {
    /// Exited with this code (`0..=255`).
    Code(i32),
    /// Killed by this signal (`1..=64`).
    Signal(i32),
}

impl Exit {
    fn in_range(self) -> bool {
        match self {
            Exit::Code(c) => (0..=MAX_EXIT_CODE).contains(&c),
            Exit::Signal(n) => (1..=MAX_SIGNAL).contains(&n),
        }
    }
}

/// Which timeout fired. They are different budgets and never share a counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutKind {
    /// The command's own execution time. Queue wait is not part of it.
    Command,
    /// The homework-wide deadline across every phase.
    Deadline,
    /// Waiting for resources (admission) took too long.
    ResourceWait,
}

/// Where the attempt was when a timeout fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutPhase {
    Preparing,
    Waiting,
    Running,
    Collecting,
}

/// Details of a `timed_out` termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeoutInfo {
    pub kind: TimeoutKind,
    pub phase: TimeoutPhase,
}

impl TimeoutInfo {
    /// A command timeout happens while running, a resource-wait timeout while
    /// waiting; only the homework deadline can fire in any phase.
    pub fn is_consistent(self) -> bool {
        match self.kind {
            TimeoutKind::Command => self.phase == TimeoutPhase::Running,
            TimeoutKind::ResourceWait => self.phase == TimeoutPhase::Waiting,
            TimeoutKind::Deadline => true,
        }
    }
}

/// A termination that contradicts itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TerminationError {
    #[error("exit {0:?} is outside the valid range (code 0..=255, signal 1..=64)")]
    ExitOutOfRange(Exit),
    #[error("exit {exit:?} contradicts outcome {outcome:?}")]
    ExitContradictsOutcome { outcome: Outcome, exit: Exit },
    #[error("outcome {0:?}: timed_out needs timeout info and no other outcome carries it")]
    TimeoutInfoMismatch(Outcome),
    #[error("timeout kind and phase do not belong together: {0:?}")]
    TimeoutPhaseMismatch(TimeoutInfo),
}

/// The write-once result of an attempt: the outcome, how the command ended,
/// and for `timed_out` which timeout fired where. Built only through
/// [`Termination::new`] (deserialization goes through it too), so a value of
/// this type is always self-consistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "TerminationRaw")]
pub struct Termination {
    outcome: Outcome,
    exit: Option<Exit>,
    timeout: Option<TimeoutInfo>,
}

#[derive(Deserialize)]
struct TerminationRaw {
    outcome: Outcome,
    #[serde(default)]
    exit: Option<Exit>,
    #[serde(default)]
    timeout: Option<TimeoutInfo>,
}

impl TryFrom<TerminationRaw> for Termination {
    type Error = TerminationError;
    fn try_from(raw: TerminationRaw) -> Result<Self, TerminationError> {
        Termination::new(raw.outcome, raw.exit, raw.timeout)
    }
}

impl Termination {
    /// `exit` is `None` when no command ran (preparation failure, cancel before
    /// start). `timeout` is required for `timed_out` and forbidden otherwise.
    pub fn new(
        outcome: Outcome,
        exit: Option<Exit>,
        timeout: Option<TimeoutInfo>,
    ) -> Result<Self, TerminationError> {
        if let Some(e) = exit {
            if !e.in_range() {
                return Err(TerminationError::ExitOutOfRange(e));
            }
            let contradiction = match outcome {
                Outcome::Succeeded => e != Exit::Code(0),
                Outcome::Failed => e == Exit::Code(0),
                _ => false,
            };
            if contradiction {
                return Err(TerminationError::ExitContradictsOutcome { outcome, exit: e });
            }
        }
        match (outcome == Outcome::TimedOut, timeout) {
            (true, Some(t)) if !t.is_consistent() => {
                return Err(TerminationError::TimeoutPhaseMismatch(t));
            }
            (true, Some(_)) | (false, None) => {}
            _ => return Err(TerminationError::TimeoutInfoMismatch(outcome)),
        }
        Ok(Termination {
            outcome,
            exit,
            timeout,
        })
    }

    pub fn outcome(&self) -> Outcome {
        self.outcome
    }

    pub fn exit(&self) -> Option<Exit> {
        self.exit
    }

    pub fn timeout(&self) -> Option<TimeoutInfo> {
        self.timeout
    }
}

/// `state` and `termination` disagree: a terminal attempt must have exactly one
/// termination, and a non-terminal one must have none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TerminationStateError {
    #[error("state {0:?} requires a termination")]
    Missing(AttemptState),
    #[error("state {0:?} must not carry a termination")]
    Unexpected(AttemptState),
}

/// Check the invariant "state is `Terminal` iff a termination exists".
pub fn check_termination(
    state: AttemptState,
    termination: Option<&Termination>,
) -> Result<(), TerminationStateError> {
    match (state.is_terminal(), termination.is_some()) {
        (true, false) => Err(TerminationStateError::Missing(state)),
        (false, true) => Err(TerminationStateError::Unexpected(state)),
        _ => Ok(()),
    }
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

/// One stage of acceptance (spec "受理の段階"). There is no order between
/// stages: each is reached or not, independently (a failed preparation reaches
/// `Terminal` without ever registering a command). In particular, "the user
/// command was started" is not a receipt comparison; it is
/// `AttemptStatus::spawned_at_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Every stage, in the order they are normally reached.
    pub const ALL: [Receipt; 6] = [
        Receipt::CoordinatorAccepted,
        Receipt::WorkerAccepted,
        Receipt::CommandRegistered,
        Receipt::Running,
        Receipt::Terminal,
        Receipt::Collected,
    ];
}

/// The receipts an attempt has reached, each with the UTC unix time (ms) it was
/// reached. The times are for display and diagnosis only; ordering decisions use
/// monotonic clocks on the host that owns them. `collected` is only ever set by
/// the coordinator; a worker reports the other five.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Receipts {
    #[serde(default)]
    pub coordinator_accepted_at_ms: Option<u64>,
    #[serde(default)]
    pub worker_accepted_at_ms: Option<u64>,
    #[serde(default)]
    pub command_registered_at_ms: Option<u64>,
    #[serde(default)]
    pub running_at_ms: Option<u64>,
    #[serde(default)]
    pub terminal_at_ms: Option<u64>,
    #[serde(default)]
    pub collected_at_ms: Option<u64>,
}

impl Receipts {
    fn slot(&self, receipt: Receipt) -> &Option<u64> {
        match receipt {
            Receipt::CoordinatorAccepted => &self.coordinator_accepted_at_ms,
            Receipt::WorkerAccepted => &self.worker_accepted_at_ms,
            Receipt::CommandRegistered => &self.command_registered_at_ms,
            Receipt::Running => &self.running_at_ms,
            Receipt::Terminal => &self.terminal_at_ms,
            Receipt::Collected => &self.collected_at_ms,
        }
    }

    fn slot_mut(&mut self, receipt: Receipt) -> &mut Option<u64> {
        match receipt {
            Receipt::CoordinatorAccepted => &mut self.coordinator_accepted_at_ms,
            Receipt::WorkerAccepted => &mut self.worker_accepted_at_ms,
            Receipt::CommandRegistered => &mut self.command_registered_at_ms,
            Receipt::Running => &mut self.running_at_ms,
            Receipt::Terminal => &mut self.terminal_at_ms,
            Receipt::Collected => &mut self.collected_at_ms,
        }
    }

    /// When `receipt` was reached, if it was.
    pub fn at_ms(&self, receipt: Receipt) -> Option<u64> {
        *self.slot(receipt)
    }

    pub fn reached(&self, receipt: Receipt) -> bool {
        self.slot(receipt).is_some()
    }

    /// Record `receipt` as reached at `at_ms`. The first time wins; returns
    /// whether this call recorded it.
    pub fn mark(&mut self, receipt: Receipt, at_ms: u64) -> bool {
        let slot = self.slot_mut(receipt);
        if slot.is_some() {
            false
        } else {
            *slot = Some(at_ms);
            true
        }
    }
}

/// Whether the coordinator holds the verified, durably saved required outputs.
///
/// "Collected" is derived, never guessed: an attempt with no required
/// artifacts is collected the moment its terminal is confirmed, so nothing is
/// left to wait for. Before terminal nothing is collected (`Pending`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Collection {
    Pending,
    Collected,
    /// Collection gave up; the computed result is kept, nothing is re-run.
    Failed,
}

impl Collection {
    /// `required`: required artifacts; `persisted`: those verified and saved;
    /// `failed`: collection has given up.
    pub fn derive(state: AttemptState, required: u32, persisted: u32, failed: bool) -> Collection {
        if !state.is_terminal() {
            Collection::Pending
        } else if required == 0 || persisted >= required {
            Collection::Collected
        } else if failed {
            Collection::Failed
        } else {
            Collection::Pending
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
    /// The worker finished but required outputs could not be collected.
    CollectionFailed,
    /// Communication is unconfirmed; execution state cannot be asserted.
    Unknown,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl RunDisplay {
    /// Derive the display state from the attempt state, its termination, the
    /// observation state, and the collection state.
    ///
    /// A run that is terminal with its required outputs durably collected is
    /// settled: a later `Unknown` observation does not overwrite it. Every
    /// other run shows `Unknown` while communication is unconfirmed, including
    /// one whose collection failed.
    pub fn derive(
        state: AttemptState,
        termination: Option<&Termination>,
        observation: Observation,
        collection: Collection,
    ) -> RunDisplay {
        let settled =
            state.is_terminal() && termination.is_some() && collection == Collection::Collected;
        if observation == Observation::Unknown && !settled {
            return RunDisplay::Unknown;
        }
        match (state, termination) {
            (AttemptState::Terminal, Some(t)) => match collection {
                Collection::Collected => match t.outcome() {
                    Outcome::Succeeded => RunDisplay::Succeeded,
                    Outcome::Failed => RunDisplay::Failed,
                    Outcome::Cancelled => RunDisplay::Cancelled,
                    Outcome::TimedOut => RunDisplay::TimedOut,
                    Outcome::Interrupted => RunDisplay::Interrupted,
                },
                Collection::Failed => RunDisplay::CollectionFailed,
                Collection::Pending => RunDisplay::Collecting,
            },
            (AttemptState::Terminal, None) | (AttemptState::Finalizing, _) => {
                RunDisplay::Collecting
            }
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

    fn term(o: Outcome) -> Termination {
        let timeout = (o == Outcome::TimedOut).then_some(TimeoutInfo {
            kind: TimeoutKind::Deadline,
            phase: TimeoutPhase::Preparing,
        });
        Termination::new(o, None, timeout).unwrap()
    }

    const OUTCOMES: [Outcome; 5] = [
        Outcome::Succeeded,
        Outcome::Failed,
        Outcome::Cancelled,
        Outcome::TimedOut,
        Outcome::Interrupted,
    ];

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
                    Ok(s) => assert!(expect && s == to && to != Terminal),
                    Err(TransitionError::FinalizeRequired) => {
                        assert_eq!((from, to), (Finalizing, Terminal));
                    }
                    Err(e) => {
                        assert!(!expect);
                        assert_eq!(e, TransitionError::Illegal { from, to });
                        assert!(e.to_string().contains("illegal"));
                    }
                }
            }
        }
        assert!(TransitionError::FinalizeRequired
            .to_string()
            .contains("finalize"));
    }

    #[test]
    fn terminal_is_reached_only_through_finalize() {
        for o in OUTCOMES {
            let (s, t) = Finalizing.finalize(term(o)).unwrap();
            assert_eq!(s, Terminal);
            assert_eq!(t.outcome(), o);
        }
        for from in AttemptState::ALL {
            if from != Finalizing {
                assert_eq!(
                    from.finalize(term(Outcome::Failed)),
                    Err(TransitionError::Illegal { from, to: Terminal })
                );
            }
        }
        // plain transition cannot write the result
        assert_eq!(
            Finalizing.transition(Terminal),
            Err(TransitionError::FinalizeRequired)
        );
    }

    #[test]
    fn happy_path_and_cancel_path_reach_terminal() {
        let mut s = Accepted;
        for next in [Preparing, Ready, Starting, Active, Finalizing] {
            s = s.transition(next).unwrap();
        }
        let (s, _) = s.finalize(term(Outcome::Succeeded)).unwrap();
        assert!(s.is_terminal());
        let s = Active
            .transition(Cancelling)
            .unwrap()
            .transition(Finalizing)
            .unwrap();
        assert_eq!(s.finalize(term(Outcome::Cancelled)).unwrap().0, Terminal);
        assert!(!Active.is_terminal());
        assert!(Terminal.successors().is_empty());
    }

    #[test]
    fn spawn_authority_is_taken_only_by_the_ready_to_starting_commit() {
        for from in AttemptState::ALL {
            for to in AttemptState::ALL {
                assert_eq!(
                    AttemptState::grants_spawn(from, to),
                    (from, to) == (Ready, Starting),
                    "{from:?} -> {to:?}"
                );
            }
        }
        // the only edge that grants it is a real edge
        assert!(Ready.can_transition(Starting));
    }

    #[test]
    fn recovery_never_respawns_once_the_authority_was_taken() {
        for s in AttemptState::ALL {
            let expect = matches!(s, Starting | Active | Cancelling | Finalizing | Terminal);
            assert_eq!(s.recovery_must_not_respawn(), expect, "{s:?}");
        }
        // a state reachable only after the spawn commit never allows a respawn
        for s in AttemptState::ALL {
            if AttemptState::ALL
                .iter()
                .any(|&from| AttemptState::grants_spawn(from, s))
            {
                assert!(s.recovery_must_not_respawn(), "{s:?}");
            }
        }
    }

    #[test]
    fn termination_accepts_consistent_results() {
        let t = Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None).unwrap();
        assert_eq!(
            (t.outcome(), t.exit(), t.timeout()),
            (Outcome::Succeeded, Some(Exit::Code(0)), None)
        );
        assert!(Termination::new(Outcome::Succeeded, None, None).is_ok());
        assert!(Termination::new(Outcome::Failed, Some(Exit::Code(1)), None).is_ok());
        assert!(Termination::new(Outcome::Failed, Some(Exit::Code(255)), None).is_ok());
        assert!(Termination::new(Outcome::Failed, Some(Exit::Signal(9)), None).is_ok());
        assert!(Termination::new(Outcome::Failed, None, None).is_ok());
        assert!(Termination::new(Outcome::Cancelled, Some(Exit::Signal(15)), None).is_ok());
        assert!(Termination::new(Outcome::Cancelled, Some(Exit::Code(0)), None).is_ok());
        assert!(Termination::new(Outcome::Interrupted, None, None).is_ok());
        let info = TimeoutInfo {
            kind: TimeoutKind::Command,
            phase: TimeoutPhase::Running,
        };
        let t = Termination::new(Outcome::TimedOut, Some(Exit::Signal(9)), Some(info)).unwrap();
        assert_eq!(t.timeout(), Some(info));
    }

    #[test]
    fn termination_rejects_contradictions() {
        // range
        for bad in [
            Exit::Code(-1),
            Exit::Code(256),
            Exit::Signal(0),
            Exit::Signal(65),
        ] {
            let e = Termination::new(Outcome::Cancelled, Some(bad), None).unwrap_err();
            assert_eq!(e, TerminationError::ExitOutOfRange(bad));
            assert!(e.to_string().contains("range"));
        }
        for ok in [
            Exit::Code(0),
            Exit::Code(255),
            Exit::Signal(1),
            Exit::Signal(64),
        ] {
            assert!(Termination::new(Outcome::Cancelled, Some(ok), None).is_ok());
        }
        // exit versus outcome
        for bad in [Exit::Code(1), Exit::Signal(9)] {
            let e = Termination::new(Outcome::Succeeded, Some(bad), None).unwrap_err();
            assert_eq!(
                e,
                TerminationError::ExitContradictsOutcome {
                    outcome: Outcome::Succeeded,
                    exit: bad
                }
            );
            assert!(e.to_string().contains("contradicts"));
        }
        assert!(matches!(
            Termination::new(Outcome::Failed, Some(Exit::Code(0)), None),
            Err(TerminationError::ExitContradictsOutcome { .. })
        ));
        // timeout info
        let info = TimeoutInfo {
            kind: TimeoutKind::Deadline,
            phase: TimeoutPhase::Collecting,
        };
        for o in OUTCOMES {
            let with = Termination::new(o, None, Some(info));
            let without = Termination::new(o, None, None);
            if o == Outcome::TimedOut {
                assert!(with.is_ok());
                let e = without.unwrap_err();
                assert_eq!(e, TerminationError::TimeoutInfoMismatch(o));
                assert!(e.to_string().contains("timeout"));
            } else {
                assert_eq!(with, Err(TerminationError::TimeoutInfoMismatch(o)));
                assert!(without.is_ok());
            }
        }
        let bad = TimeoutInfo {
            kind: TimeoutKind::Command,
            phase: TimeoutPhase::Waiting,
        };
        let e = Termination::new(Outcome::TimedOut, None, Some(bad)).unwrap_err();
        assert_eq!(e, TerminationError::TimeoutPhaseMismatch(bad));
        assert!(e.to_string().contains("phase"));
    }

    #[test]
    fn timeout_info_kinds_and_phases_belong_together() {
        use TimeoutKind::*;
        use TimeoutPhase::*;
        for kind in [Command, Deadline, ResourceWait] {
            for phase in [Preparing, Waiting, Running, Collecting] {
                let expect = match kind {
                    Command => phase == Running,
                    ResourceWait => phase == Waiting,
                    Deadline => true,
                };
                assert_eq!(TimeoutInfo { kind, phase }.is_consistent(), expect);
            }
        }
    }

    #[test]
    fn termination_json_roundtrips_and_revalidates() {
        let t = Termination::new(Outcome::Failed, Some(Exit::Code(2)), None).unwrap();
        let j = serde_json::to_value(t).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"outcome":"failed","exit":{"code":2},"timeout":null})
        );
        assert_eq!(serde_json::from_value::<Termination>(j).unwrap(), t);
        let s = Termination::new(Outcome::Cancelled, Some(Exit::Signal(15)), None).unwrap();
        assert_eq!(
            serde_json::to_value(s).unwrap()["exit"],
            serde_json::json!({"signal":15})
        );
        // defaults
        let min: Termination = serde_json::from_str(r#"{"outcome":"interrupted"}"#).unwrap();
        assert_eq!(min.exit(), None);
        // a hand-written contradiction is refused at the boundary
        assert!(serde_json::from_str::<Termination>(
            r#"{"outcome":"succeeded","exit":{"code":3}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Termination>(r#"{"outcome":"timed_out"}"#).is_err());
        assert!(
            serde_json::from_str::<Termination>(r#"{"outcome":"failed","exit":{"code":999}}"#)
                .is_err()
        );
    }

    #[test]
    fn terminal_iff_termination() {
        let t = term(Outcome::Failed);
        for s in AttemptState::ALL {
            let with = check_termination(s, Some(&t));
            let without = check_termination(s, None);
            if s == Terminal {
                assert_eq!(with, Ok(()));
                assert_eq!(without, Err(TerminationStateError::Missing(Terminal)));
                assert!(without.unwrap_err().to_string().contains("requires"));
            } else {
                assert_eq!(without, Ok(()));
                assert_eq!(with, Err(TerminationStateError::Unexpected(s)));
                assert!(with.unwrap_err().to_string().contains("must not"));
            }
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
            serde_json::to_string(&RunDisplay::CollectionFailed).unwrap(),
            "\"collection_failed\""
        );
        assert_eq!(
            serde_json::to_string(&Collection::Failed).unwrap(),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&TimeoutKind::ResourceWait).unwrap(),
            "\"resource_wait\""
        );
        assert_eq!(
            serde_json::to_string(&TimeoutPhase::Collecting).unwrap(),
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
        for o in OUTCOMES {
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
    fn receipts_are_independent_and_first_mark_wins() {
        let mut r = Receipts::default();
        for k in Receipt::ALL {
            assert!(!r.reached(k));
            assert_eq!(r.at_ms(k), None);
        }
        // reaching a later stage implies nothing about the earlier ones
        assert!(r.mark(Receipt::Terminal, 50));
        for k in Receipt::ALL {
            assert_eq!(r.reached(k), k == Receipt::Terminal, "{k:?}");
        }
        for (i, k) in Receipt::ALL.into_iter().enumerate() {
            let at = 100 + i as u64;
            let first = r.mark(k, at);
            assert_eq!(first, k != Receipt::Terminal, "{k:?}");
            assert!(r.reached(k));
            // a second mark neither succeeds nor moves the time
            assert!(!r.mark(k, 999));
            assert_eq!(
                r.at_ms(k),
                Some(if k == Receipt::Terminal { 50 } else { at })
            );
        }
        let j = serde_json::to_value(r).unwrap();
        assert_eq!(j["worker_accepted_at_ms"], 101);
        assert_eq!(serde_json::from_value::<Receipts>(j).unwrap(), r);
        let empty: Receipts = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, Receipts::default());
    }

    #[test]
    fn collection_is_derived_from_required_outputs() {
        let d = Collection::derive;
        // nothing is collected before terminal, even with zero required outputs
        for s in AttemptState::ALL.into_iter().filter(|s| *s != Terminal) {
            assert_eq!(d(s, 0, 0, false), Collection::Pending, "{s:?}");
            assert_eq!(d(s, 2, 2, false), Collection::Pending, "{s:?}");
            assert_eq!(d(s, 2, 0, true), Collection::Pending, "{s:?}");
        }
        // zero required outputs: collected at the terminal confirmation
        assert_eq!(d(Terminal, 0, 0, false), Collection::Collected);
        assert_eq!(d(Terminal, 2, 2, false), Collection::Collected);
        assert_eq!(d(Terminal, 2, 3, false), Collection::Collected);
        assert_eq!(d(Terminal, 2, 1, false), Collection::Pending);
        assert_eq!(d(Terminal, 2, 1, true), Collection::Failed);
        // everything persisted beats a stale failure flag
        assert_eq!(d(Terminal, 2, 2, true), Collection::Collected);
    }

    fn known(s: AttemptState, t: Option<&Termination>, c: Collection) -> RunDisplay {
        RunDisplay::derive(s, t, Observation::Known, c)
    }

    #[test]
    fn display_unknown_observation_wins_unless_settled() {
        let ok = term(Outcome::Succeeded);
        for s in AttemptState::ALL {
            for t in [None, Some(&ok)] {
                for c in [
                    Collection::Pending,
                    Collection::Failed,
                    Collection::Collected,
                ] {
                    let settled = s == Terminal && t.is_some() && c == Collection::Collected;
                    let got = RunDisplay::derive(s, t, Observation::Unknown, c);
                    if settled {
                        assert_eq!(got, RunDisplay::Succeeded);
                    } else {
                        assert_eq!(got, RunDisplay::Unknown, "{s:?} {t:?} {c:?}");
                    }
                }
            }
        }
        // zero required outputs do not make a live run "settled"
        let c = Collection::derive(Active, 0, 0, false);
        assert_eq!(
            RunDisplay::derive(Active, None, Observation::Unknown, c),
            RunDisplay::Unknown
        );
        // reconciling is not unknown
        assert_eq!(
            RunDisplay::derive(Active, None, Observation::Reconciling, Collection::Pending),
            RunDisplay::Running
        );
    }

    #[test]
    fn display_by_state() {
        let d = |s| known(s, None, Collection::Pending);
        assert_eq!(d(Accepted), RunDisplay::Queued);
        assert_eq!(d(Ready), RunDisplay::Queued);
        assert_eq!(d(Preparing), RunDisplay::Preparing);
        assert_eq!(d(Starting), RunDisplay::Running);
        assert_eq!(d(Active), RunDisplay::Running);
        assert_eq!(d(Cancelling), RunDisplay::Cancelling);
        assert_eq!(d(Finalizing), RunDisplay::Collecting);
    }

    #[test]
    fn display_terminal_waits_for_collection() {
        for o in OUTCOMES {
            let t = term(o);
            assert_eq!(
                known(Terminal, Some(&t), Collection::Pending),
                RunDisplay::Collecting
            );
            assert_eq!(
                known(Terminal, Some(&t), Collection::Failed),
                RunDisplay::CollectionFailed
            );
        }
        let at = |o| known(Terminal, Some(&term(o)), Collection::Collected);
        assert_eq!(at(Outcome::Succeeded), RunDisplay::Succeeded);
        assert_eq!(at(Outcome::Failed), RunDisplay::Failed);
        assert_eq!(at(Outcome::Cancelled), RunDisplay::Cancelled);
        assert_eq!(at(Outcome::TimedOut), RunDisplay::TimedOut);
        assert_eq!(at(Outcome::Interrupted), RunDisplay::Interrupted);
        // terminal without a recorded termination is never shown as finished
        assert_eq!(
            known(Terminal, None, Collection::Collected),
            RunDisplay::Collecting
        );
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
