//! Which worker a task goes to, and why: the whole judgement, without I/O.
//!
//! The caller gathers the facts (who answered, what each worker forecast,
//! how old that answer is on a monotonic clock, what the parent holds on
//! each worker) and gets back a [`Decision`] that names the winner and
//! keeps everything that was compared, so the placement can be explained
//! later from the record alone.
//!
//! The rules (spec chapter 9):
//!
//! * A worker's predicted completion is its forecast for this task on top
//!   of its queue (transfer and setup are given to the worker as the delay
//!   before the task is ready, so they overlap the queue) plus collection.
//! * The compared value adds the priority correction.
//! * A worker without a number is `unknown`. It is never read as free: it
//!   is chosen only when no worker has a number.
//! * Values within the tie window of the smallest are "about the same";
//!   among those the order is priority, less waiting, less load held by
//!   the parent, warm cache, less to transfer, affinity, worker id.

use crate::error::{BlockedReason, ErrorCode};
use crate::forecast::plan::{validity, Validity};
use crate::ids::{AgentEpoch, PlanDigest, WorkerBootId, WorkerId};
use crate::wire::{ForecastField, ForecastResponse, RuntimeSource, Uncertainty};
use serde::{Deserialize, Serialize};

/// The numbers the judgement is tuned by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rules {
    /// What one step of priority is worth (`priority_step_seconds`).
    pub priority_step_ms: u64,
    /// Compared values this close to the smallest are about the same.
    pub tie_window_ms: u64,
    /// The parent's own limit on the age of a forecast; a worker's longer
    /// `ttl_ms` does not extend it.
    pub forecast_ttl_ms: u64,
}

/// Index of the transfer estimate in [`Costs::ms`].
pub const TRANSFER: usize = 0;
/// Index of the setup estimate in [`Costs::ms`].
pub const SETUP: usize = 1;
/// Index of the collection estimate in [`Costs::ms`].
pub const COLLECTION: usize = 2;

/// What the parent knows about putting this task on this worker, besides
/// the worker's own forecast. `None` is unknown, never zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Costs {
    /// Transfer, setup and collection, in that order.
    pub ms: [Option<u64>; 3],
    /// The part was unknown for this worker and known for another: it was
    /// given the worst known figure (see [`fill_costs`]).
    pub assumed: [bool; 3],
    pub transfer_bytes: Option<u64>,
    /// Whether the prepared state the task needs is there already.
    pub cache_warm: Option<bool>,
    /// The worker already holds work of the same run.
    pub affinity: bool,
}

/// Give every unknown part the worst figure any candidate has for it, so
/// that a worker the parent knows nothing about is not compared as if the
/// part were free. A part nobody knows stays unknown for all, and so does
/// not move the comparison.
pub fn fill_costs(all: &mut [Costs]) {
    for part in [TRANSFER, SETUP, COLLECTION] {
        let Some(worst) = all.iter().filter_map(|c| c.ms[part]).max() else {
            continue;
        };
        for c in all.iter_mut() {
            if c.ms[part].is_none() {
                c.ms[part] = Some(worst);
                c.assumed[part] = true;
            }
        }
    }
}

/// The delay before the task is ready on the worker: transfer and setup.
/// Unknown only when both are.
pub fn ready_delay_ms(c: &Costs) -> Option<u64> {
    match (c.ms[TRANSFER], c.ms[SETUP]) {
        (None, None) => None,
        (t, s) => Some(t.unwrap_or(0).saturating_add(s.unwrap_or(0))),
    }
}

/// Why a forecast answer is not used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unusable {
    /// Older than the worker's `ttl_ms` or the parent's own limit.
    Expired,
    /// The machine restarted since the worker was looked at.
    BootChanged,
    /// The agent restarted since the worker was looked at.
    EpochChanged,
    /// It is about another worker or another plan than was asked.
    NotAsked,
    /// The worker answered the question with an error.
    Refused,
}

/// What a worker's forecast says about this task, reduced to what the
/// judgement reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outlook {
    /// A time the worker stands behind.
    pub finish_ms: Option<u64>,
    /// A fallback figure: for ordering only.
    pub ranking_ms: Option<u64>,
    pub ready_ms: Option<u64>,
    pub wait_ms: Option<u64>,
    pub execution_ms: Option<u64>,
    pub source: RuntimeSource,
    pub conditional: bool,
    /// Why the worker gave no finish time.
    pub null_reason: Option<Uncertainty>,
    pub unusable: Option<Unusable>,
}

impl Outlook {
    /// An answer that says nothing.
    pub fn unusable(why: Unusable) -> Outlook {
        Outlook {
            finish_ms: None,
            ranking_ms: None,
            ready_ms: None,
            wait_ms: None,
            execution_ms: None,
            source: RuntimeSource::Unknown,
            conditional: false,
            null_reason: None,
            unusable: Some(why),
        }
    }
}

/// What a forecast was asked of: the worker as this tick's `inspect` saw
/// it, and the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked<'a> {
    pub worker_id: &'a WorkerId,
    pub plan_digest: &'a PlanDigest,
    pub boot_id: &'a WorkerBootId,
    pub agent_epoch: AgentEpoch,
}

/// Read a forecast answer. `age_ms` is how long ago the question was sent,
/// on a monotonic clock. An answer that is not about what was asked, is too
/// old, or comes from a worker that restarted since it was looked at is not
/// used at all.
pub fn outlook_of(
    asked: &Asked<'_>,
    answer: &ForecastResponse,
    age_ms: u64,
    own_ttl_ms: u64,
) -> Outlook {
    if &answer.worker_id != asked.worker_id || &answer.target.plan_digest != asked.plan_digest {
        return Outlook::unusable(Unusable::NotAsked);
    }
    if age_ms >= own_ttl_ms {
        return Outlook::unusable(Unusable::Expired);
    }
    match validity(answer, age_ms, asked.agent_epoch, asked.boot_id) {
        Validity::Fresh => {}
        Validity::Expired => return Outlook::unusable(Unusable::Expired),
        Validity::BootChanged => return Outlook::unusable(Unusable::BootChanged),
        Validity::EpochChanged => return Outlook::unusable(Unusable::EpochChanged),
    }
    let t = &answer.target;
    Outlook {
        finish_ms: t.finish_in_ms,
        ranking_ms: t.ranking.and_then(|r| r.finish_in_ms),
        ready_ms: t.ready_in_ms,
        wait_ms: t.queue_wait_after_ready_ms,
        execution_ms: t.execution_ms,
        source: t.runtime_source,
        conditional: t.conditional,
        null_reason: t.null_reasons.get(&ForecastField::FinishInMs).copied(),
        unusable: None,
    }
}

/// A worker that may be given the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub worker_id: WorkerId,
    pub priority: i64,
    /// The tasks the parent holds on the worker now.
    pub load: u32,
    pub costs: Costs,
    pub outlook: Outlook,
}

/// Why a worker was not compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exclusion {
    /// It did not answer `inspect` in this tick (or used up its time).
    Unreached,
    Draining,
    /// The parent holds as many tasks there as the limits allow.
    Full,
    /// It keeps refusing this task for a reason that will not pass there.
    RefusedTask,
    /// It did not answer the forecast in time.
    NoForecast,
    /// It did not take a submit earlier in this tick.
    NotTaking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Excluded {
    pub worker_id: WorkerId,
    pub why: Exclusion,
}

/// What kind of number a worker was compared by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Label {
    /// A time from recorded runs.
    Measured,
    /// A figure from the configured fallback: an order, not a time.
    Fallback,
    /// No number. Not free: only chosen when nobody has a number.
    Unknown,
}

/// The rule that separated the winner from the next of those about the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rule {
    Priority,
    LessWaiting,
    Load,
    Cache,
    Transfer,
    Affinity,
    WorkerId,
}

const RULES: [Rule; 6] = [
    Rule::Priority,
    Rule::LessWaiting,
    Rule::Load,
    Rule::Cache,
    Rule::Transfer,
    Rule::Affinity,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "rule")]
pub enum Why {
    NoCandidate,
    OnlyCandidate,
    /// Its compared value is the smallest, and no other is about the same.
    Earliest,
    /// Others were about the same; this rule chose.
    TieBreak(Rule),
}

/// One compared worker with everything the comparison used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compared {
    pub worker_id: WorkerId,
    pub priority: i64,
    pub load: u32,
    pub costs: Costs,
    pub outlook: Outlook,
    pub label: Label,
    /// The priority correction added to the predicted completion.
    pub penalty_ms: u64,
    /// The forecast's number plus collection. For a `fallback` label this
    /// orders workers; it is not a time to show.
    pub predicted_ms: Option<u64>,
    /// `predicted_ms` plus `penalty_ms`.
    pub compare_ms: Option<u64>,
    /// About the same as the smallest.
    pub tied: bool,
}

/// A placement decision, as recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub winner: Option<WorkerId>,
    pub why: Why,
    /// In the order of preference: the winner first.
    pub compared: Vec<Compared>,
    pub excluded: Vec<Excluded>,
    pub rules: Rules,
}

fn word<T: Serialize>(t: &T) -> String {
    let value = serde_json::to_value(t).ok();
    let text = value.as_ref().and_then(|v| v.as_str());
    text.unwrap_or_default().to_string()
}

impl Decision {
    /// One line for a person. A measured worker shows its compared value as
    /// a time; a fallback figure is shown as a rank, never as a time.
    pub fn summary(&self) -> String {
        let head = match (&self.winner, self.why) {
            (Some(w), Why::OnlyCandidate) => format!("{w}: the only candidate"),
            (Some(w), Why::Earliest) => format!("{w}: earliest predicted completion"),
            (Some(w), Why::TieBreak(rule)) => {
                format!("{w}: about the same as others, chosen by {}", word(&rule))
            }
            _ => "no worker can take it".to_string(),
        };
        let compared: Vec<String> = self
            .compared
            .iter()
            .map(|c| match (c.compare_ms, c.label) {
                (Some(ms), Label::Measured) => format!("{}={}ms measured", c.worker_id, ms),
                (Some(rank), _) => format!("{}=rank {} fallback", c.worker_id, rank),
                (None, _) => format!("{}=unknown (not counted as free)", c.worker_id),
            })
            .collect();
        let excluded: Vec<String> = self
            .excluded
            .iter()
            .map(|e| format!("{} {}", e.worker_id, word(&e.why)))
            .collect();
        format!(
            "{head}; compared: [{}]; excluded: [{}]",
            compared.join(", "),
            excluded.join(", ")
        )
    }
}

fn label_of(o: &Outlook) -> Label {
    match (o.finish_ms, o.ranking_ms) {
        (Some(_), _) if o.source == RuntimeSource::Measured => Label::Measured,
        (None, None) => Label::Unknown,
        _ => Label::Fallback,
    }
}

/// The order among workers that are about the same, smaller first.
fn key(c: &Compared) -> [u64; 6] {
    let priority = i128::from(i64::MAX) - i128::from(c.priority);
    let cache = match c.costs.cache_warm {
        Some(true) => 0,
        None => 1,
        Some(false) => 2,
    };
    [
        u64::try_from(priority).unwrap_or(u64::MAX),
        c.outlook.wait_ms.unwrap_or(u64::MAX),
        u64::from(c.load),
        cache,
        c.costs.transfer_bytes.unwrap_or(u64::MAX),
        u64::from(!c.costs.affinity),
    ]
}

/// Choose a worker for one task.
pub fn decide(candidates: Vec<Candidate>, mut excluded: Vec<Excluded>, rules: Rules) -> Decision {
    excluded.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    let top = candidates.iter().map(|c| c.priority).max().unwrap_or(0);
    let mut compared: Vec<Compared> = candidates
        .into_iter()
        .map(|c| {
            let steps = u64::try_from(i128::from(top) - i128::from(c.priority)).unwrap_or(0);
            let penalty_ms = steps.saturating_mul(rules.priority_step_ms);
            let number = c.outlook.finish_ms.or(c.outlook.ranking_ms);
            let predicted_ms =
                number.map(|n| n.saturating_add(c.costs.ms[COLLECTION].unwrap_or(0)));
            Compared {
                label: label_of(&c.outlook),
                penalty_ms,
                predicted_ms,
                compare_ms: predicted_ms.map(|p| p.saturating_add(penalty_ms)),
                tied: false,
                worker_id: c.worker_id,
                priority: c.priority,
                load: c.load,
                costs: c.costs,
                outlook: c.outlook,
            }
        })
        .collect();
    let smallest = compared.iter().filter_map(|c| c.compare_ms).min();
    for c in compared.iter_mut() {
        c.tied = match (smallest, c.compare_ms) {
            (Some(least), Some(ms)) => ms <= least.saturating_add(rules.tie_window_ms),
            // Nobody has a number: all are equally unknown.
            (None, _) => true,
            (Some(_), None) => false,
        };
    }
    // The tied first, by the tie rules; then by number; unknown last.
    compared.sort_by(|a, b| {
        let rank = |c: &Compared| {
            if c.tied {
                (false, key(c), 0)
            } else {
                (true, [0; 6], c.compare_ms.unwrap_or(u64::MAX))
            }
        };
        (rank(a), &a.worker_id).cmp(&(rank(b), &b.worker_id))
    });
    let tied = compared.iter().filter(|c| c.tied).count();
    let why = match compared.as_slice() {
        [] => Why::NoCandidate,
        [_] => Why::OnlyCandidate,
        [_, ..] if tied == 1 => Why::Earliest,
        [first, second, ..] => {
            let (a, b) = (key(first), key(second));
            let at = (0..RULES.len()).find(|i| a[*i] != b[*i]);
            Why::TieBreak(at.map_or(Rule::WorkerId, |i| RULES[i]))
        }
    };
    Decision {
        winner: compared.first().map(|c| c.worker_id.clone()),
        why,
        compared,
        excluded,
        rules,
    }
}

/// Whether an attempt whose submit a worker deferred should be taken off
/// that worker and placed elsewhere. `refused_before` says the worker gave
/// the same answer (the same [`refusal_key`]) for this attempt before: one
/// such answer may pass, the second means the worker keeps refusing.
///
/// Only reasons the worker itself named as its own count: it does not speak
/// the protocol, or it blocks the task for what it is (environment,
/// capability). A secret that is unavailable is missing on every worker
/// alike, a `BLOCKED` without a reason says nothing about the worker, and an
/// overloaded or faulty worker will take the same attempt later.
pub fn moves_elsewhere(
    code: ErrorCode,
    blocked: Option<BlockedReason>,
    refused_before: bool,
) -> bool {
    let lies_with_worker = match (code, blocked) {
        (ErrorCode::ProtocolIncompatible, _) => true,
        (ErrorCode::Blocked, Some(reason)) => match reason {
            BlockedReason::EnvironmentMismatch | BlockedReason::NoCompatibleWorker => true,
            BlockedReason::SecretUnavailable => false,
        },
        _ => false,
    };
    refused_before && lies_with_worker
}

/// What makes two refusals of a submit the same answer: the code and, for
/// `BLOCKED`, its reason. `BLOCKED` for a missing secret followed by
/// `BLOCKED` for the environment is two first answers, not one repeated.
pub fn refusal_key(code: ErrorCode, blocked: Option<BlockedReason>) -> String {
    match blocked {
        Some(reason) => format!("{code}/{}", word(&reason)),
        None => code.to_string(),
    }
}

impl Exclusion {
    /// Whether the exclusion holds until something is done about it, as
    /// opposed to one that describes a single tick (a worker that was full,
    /// slow or silent just now).
    pub fn lasts(self) -> bool {
        match self {
            Exclusion::RefusedTask | Exclusion::Draining => true,
            Exclusion::Unreached
            | Exclusion::Full
            | Exclusion::NoForecast
            | Exclusion::NotTaking => false,
        }
    }
}

/// Whether two decisions that placed nothing say the same about why the task
/// waits: the same outcome and the same lasting exclusions. What changes
/// from tick to tick (who was full, who did not answer, the numbers) is not
/// news.
pub fn same_wait(a: &Decision, b: &Decision) -> bool {
    fn lasting(d: &Decision) -> Vec<&Excluded> {
        d.excluded.iter().filter(|e| e.why.lasts()).collect()
    }
    a.why == b.why && lasting(a) == lasting(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Digest;
    use crate::wire::{Confidence, ForecastRanking, ForecastTarget, SchemaVersion};
    use std::collections::BTreeMap;

    const RULES_1S: Rules = Rules {
        priority_step_ms: 60_000,
        tie_window_ms: 1000,
        forecast_ttl_ms: 2000,
    };

    fn w(name: &str) -> WorkerId {
        WorkerId::parse(name).unwrap()
    }

    fn measured(finish: u64) -> Outlook {
        Outlook {
            finish_ms: Some(finish),
            ranking_ms: None,
            ready_ms: Some(0),
            wait_ms: Some(0),
            execution_ms: Some(finish),
            source: RuntimeSource::Measured,
            conditional: false,
            null_reason: None,
            unusable: None,
        }
    }

    fn fallback(rank: u64) -> Outlook {
        Outlook {
            finish_ms: None,
            ranking_ms: Some(rank),
            execution_ms: None,
            source: RuntimeSource::Fallback,
            null_reason: Some(Uncertainty::NoHistory),
            ..measured(0)
        }
    }

    fn unknown() -> Outlook {
        Outlook {
            finish_ms: None,
            wait_ms: None,
            execution_ms: None,
            null_reason: Some(Uncertainty::Overrun),
            ..measured(0)
        }
    }

    fn cand(name: &str, outlook: Outlook) -> Candidate {
        Candidate {
            worker_id: w(name),
            priority: 0,
            load: 0,
            costs: Costs::default(),
            outlook,
        }
    }

    fn order(d: &Decision) -> Vec<&str> {
        d.compared.iter().map(|c| c.worker_id.as_str()).collect()
    }

    fn run(candidates: Vec<Candidate>) -> Decision {
        decide(candidates, vec![], RULES_1S)
    }

    #[test]
    fn the_smallest_predicted_completion_wins_whatever_the_order_given() {
        let all = vec![
            cand("w-a", measured(80_000)),
            cand("w-b", measured(50_000)),
            cand("w-c", measured(51_001)),
        ];
        let mut turned = all.clone();
        turned.reverse();
        let d = run(all);
        assert_eq!(d.winner, Some(w("w-b")));
        assert_eq!(d.why, Why::Earliest);
        assert_eq!(order(&d), ["w-b", "w-c", "w-a"]);
        assert_eq!(d.compared[0].predicted_ms, Some(50_000));
        assert_eq!(d.compared[0].compare_ms, Some(50_000));
        assert_eq!(d.compared[0].label, Label::Measured);
        assert!(d.compared[0].tied && !d.compared[1].tied && !d.compared[2].tied);
        assert_eq!(d.rules, RULES_1S);
        // The same facts in another order give the same decision.
        assert_eq!(run(turned), d);
    }

    #[test]
    fn nobody_and_one_alone_are_said_as_such() {
        let d = run(vec![]);
        assert_eq!((d.winner.clone(), d.why), (None, Why::NoCandidate));
        assert!(d.compared.is_empty());
        let d = run(vec![cand("w-a", unknown())]);
        assert_eq!((d.winner, d.why), (Some(w("w-a")), Why::OnlyCandidate));
    }

    #[test]
    fn collection_is_added_to_the_forecast_and_the_priority_correction_to_that() {
        let mut a = cand("w-a", measured(10_000));
        a.costs.ms[COLLECTION] = Some(5000);
        a.priority = -1;
        let mut b = cand("w-b", measured(74_999));
        b.priority = 1;
        let d = run(vec![a.clone(), b.clone()]);
        // w-a: 10 s + 5 s collection + 2 steps of 60 s = 135 s.
        let of = |d: &Decision, name: &str| {
            let c = d.compared.iter().find(|c| c.worker_id == w(name)).unwrap();
            (c.predicted_ms, c.penalty_ms, c.compare_ms)
        };
        assert_eq!(of(&d, "w-a"), (Some(15_000), 120_000, Some(135_000)));
        assert_eq!(of(&d, "w-b"), (Some(74_999), 0, Some(74_999)));
        assert_eq!((d.winner, d.why), (Some(w("w-b")), Why::Earliest));
        // More than the correction faster: the lower priority wins.
        b.outlook = measured(136_001);
        let d = run(vec![a, b]);
        assert_eq!((d.winner, d.why), (Some(w("w-a")), Why::Earliest));
        // Equal priorities correct nothing.
        let d = run(vec![cand("w-a", measured(1)), cand("w-b", measured(2))]);
        assert!(d.compared.iter().all(|c| c.penalty_ms == 0));
    }

    #[test]
    fn the_numbers_saturate_instead_of_wrapping() {
        let mut a = cand("w-a", measured(u64::MAX - 1));
        a.costs.ms[COLLECTION] = Some(10);
        a.priority = i64::MIN;
        let mut b = cand("w-b", measured(5));
        b.priority = i64::MAX;
        let rules = Rules {
            tie_window_ms: u64::MAX,
            ..RULES_1S
        };
        let d = decide(vec![a, b], vec![], rules);
        let last = &d.compared[1];
        assert_eq!(last.worker_id, w("w-a"));
        assert_eq!(last.predicted_ms, Some(u64::MAX));
        assert_eq!(last.penalty_ms, u64::MAX);
        assert_eq!(last.compare_ms, Some(u64::MAX));
        // The window saturates too: everything is about the same.
        assert!(d.compared.iter().all(|c| c.tied));
        assert_eq!(d.why, Why::TieBreak(Rule::Priority));
    }

    #[test]
    fn a_fallback_figure_orders_workers_and_is_labelled() {
        let d = run(vec![
            cand("w-a", fallback(90_000)),
            cand("w-b", fallback(30_000)),
            cand("w-c", measured(60_000)),
        ]);
        assert_eq!(order(&d), ["w-b", "w-c", "w-a"]);
        let labels: Vec<Label> = d.compared.iter().map(|c| c.label).collect();
        assert_eq!(labels, [Label::Fallback, Label::Measured, Label::Fallback]);
        assert_eq!(d.compared[0].compare_ms, Some(30_000));
        // A finish time on a runtime that was not measured is no measured one.
        let mut odd = measured(10);
        odd.source = RuntimeSource::Fallback;
        assert_eq!(label_of(&odd), Label::Fallback);
        // The time is preferred to the ranking when a worker gives both.
        let mut both = measured(10);
        both.ranking_ms = Some(99);
        assert_eq!(
            run(vec![cand("w-a", both)]).compared[0].predicted_ms,
            Some(10)
        );
    }

    #[test]
    fn a_worker_without_a_number_is_unknown_and_never_beats_one_with_a_number() {
        let d = run(vec![
            cand("w-a", unknown()),
            cand("w-b", measured(3_600_000)),
            cand("w-c", Outlook::unusable(Unusable::Expired)),
        ]);
        assert_eq!(d.winner, Some(w("w-b")));
        assert_eq!(d.why, Why::Earliest);
        assert_eq!(order(&d), ["w-b", "w-a", "w-c"]);
        for c in &d.compared[1..] {
            assert_eq!(c.label, Label::Unknown);
            assert_eq!((c.predicted_ms, c.compare_ms, c.tied), (None, None, false));
        }
        assert_eq!(
            d.compared[1].outlook.null_reason,
            Some(Uncertainty::Overrun)
        );
        assert_eq!(d.compared[2].outlook.unusable, Some(Unusable::Expired));
    }

    #[test]
    fn when_nobody_has_a_number_the_tie_rules_choose_and_say_so() {
        let mut a = cand("w-a", unknown());
        a.load = 2;
        let mut b = cand("w-b", unknown());
        b.load = 1;
        let d = run(vec![a, b, cand("w-c", unknown())]);
        assert_eq!(order(&d), ["w-c", "w-b", "w-a"]);
        assert_eq!(d.why, Why::TieBreak(Rule::Load));
        assert!(d.compared.iter().all(|c| c.tied && c.compare_ms.is_none()));
    }

    /// Two workers that are about the same, differing in one fact.
    fn pair(f: impl FnOnce(&mut Candidate, &mut Candidate)) -> Decision {
        let mut a = cand("w-a", measured(10_000));
        let mut b = cand("w-b", measured(10_000));
        f(&mut a, &mut b);
        run(vec![a, b])
    }

    #[test]
    fn the_tie_rules_apply_in_their_order() {
        // Each rule alone prefers w-b; the worker id would prefer w-a.
        type Tweak = Box<dyn Fn(&mut Candidate, &mut Candidate)>;
        let rules: Vec<(Rule, Tweak)> = vec![
            (Rule::Priority, Box::new(|a, _| a.priority = -1)),
            (
                Rule::LessWaiting,
                Box::new(|a, _| a.outlook.wait_ms = Some(1)),
            ),
            (Rule::Load, Box::new(|a, _| a.load = 1)),
            (
                Rule::Cache,
                Box::new(|_, b| b.costs.cache_warm = Some(true)),
            ),
            (
                Rule::Transfer,
                Box::new(|a, b| {
                    a.costs.transfer_bytes = Some(2);
                    b.costs.transfer_bytes = Some(1);
                }),
            ),
            (Rule::Affinity, Box::new(|_, b| b.costs.affinity = true)),
        ];
        // The priority correction is switched off so that priority is a
        // tie rule only.
        let flat = Rules {
            priority_step_ms: 0,
            ..RULES_1S
        };
        for at in 0..rules.len() {
            let mut a = cand("w-a", measured(10_000));
            let mut b = cand("w-b", measured(10_000));
            (rules[at].1)(&mut a, &mut b);
            // Every later rule points the other way and must not matter.
            for later in &rules[at + 1..] {
                (later.1)(&mut b, &mut a);
            }
            let d = decide(vec![a, b], vec![], flat);
            assert_eq!(d.winner, Some(w("w-b")));
            assert_eq!(d.why, Why::TieBreak(rules[at].0));
        }
        let d = pair(|_, _| {});
        assert_eq!(
            (d.winner, d.why),
            (Some(w("w-a")), Why::TieBreak(Rule::WorkerId))
        );
    }

    #[test]
    fn unknown_facts_come_after_known_ones_in_a_tie() {
        // An unknown wait is after any known wait.
        let d = pair(|a, b| {
            a.outlook.wait_ms = None;
            b.outlook.wait_ms = Some(u64::MAX - 1);
        });
        assert_eq!(d.winner, Some(w("w-b")));
        // warm, then unknown, then cold.
        let d = pair(|a, _| a.costs.cache_warm = Some(false));
        assert_eq!(
            (d.winner, d.why),
            (Some(w("w-b")), Why::TieBreak(Rule::Cache))
        );
        let d = pair(|a, b| {
            a.costs.cache_warm = Some(false);
            b.costs.cache_warm = Some(true);
        });
        assert_eq!(d.winner, Some(w("w-b")));
        // An unknown amount to transfer is after any known one.
        let d = pair(|_, b| b.costs.transfer_bytes = Some(u64::MAX - 1));
        assert_eq!(
            (d.winner, d.why),
            (Some(w("w-b")), Why::TieBreak(Rule::Transfer))
        );
        // The extremes of priority keep their order.
        let d = pair(|a, b| {
            a.priority = i64::MIN;
            b.priority = i64::MAX;
            a.outlook = unknown();
            b.outlook = unknown();
        });
        assert_eq!(
            (d.winner, d.why),
            (Some(w("w-b")), Why::TieBreak(Rule::Priority))
        );
        let d = pair(|a, b| {
            a.priority = i64::MAX - 1;
            b.priority = i64::MAX;
            a.outlook = unknown();
            b.outlook = unknown();
        });
        assert_eq!(d.winner, Some(w("w-b")));
    }

    #[test]
    fn about_the_same_is_within_the_window_of_the_smallest_inclusive() {
        let three = |last: u64| {
            let mut a = cand("w-a", measured(last));
            a.costs.cache_warm = Some(true);
            run(vec![
                a,
                cand("w-b", measured(10_000)),
                cand("w-c", measured(10_500)),
            ])
        };
        // 11 000 is within 1 s of 10 000: the warm cache decides.
        let d = three(11_000);
        assert_eq!(
            (d.winner.clone(), d.why),
            (Some(w("w-a")), Why::TieBreak(Rule::Cache))
        );
        assert!(d.compared.iter().all(|c| c.tied));
        // 11 001 is not. The two left are about the same, and the id decides.
        let d = three(11_001);
        assert_eq!(order(&d), ["w-b", "w-c", "w-a"]);
        assert_eq!(d.why, Why::TieBreak(Rule::WorkerId));
        assert!(!d.compared[2].tied);
        // Among those about the same the smaller number is no rule.
        let d = run(vec![
            cand("w-b", measured(10_500)),
            cand("w-c", measured(10_000)),
        ]);
        assert_eq!(order(&d), ["w-b", "w-c"]);
        // Those outside the window are in the order of their numbers, then ids.
        let d = run(vec![
            cand("w-a", measured(1)),
            cand("w-d", measured(30_000)),
            cand("w-c", measured(30_000)),
            cand("w-b", measured(40_000)),
            cand("w-e", measured(20_000)),
        ]);
        assert_eq!(order(&d), ["w-a", "w-e", "w-c", "w-d", "w-b"]);
        // The tie rules do not reorder those outside the window.
        let mut far = cand("w-z", measured(50_000));
        far.load = 0;
        let mut near = cand("w-y", measured(40_000));
        near.load = 9;
        let d = run(vec![cand("w-a", measured(1)), far, near]);
        assert_eq!(order(&d), ["w-a", "w-y", "w-z"]);
    }

    #[test]
    fn the_excluded_are_kept_in_the_order_of_their_ids() {
        let ex = |name: &str, why| Excluded {
            worker_id: w(name),
            why,
        };
        let d = decide(
            vec![],
            vec![ex("w-b", Exclusion::Full), ex("w-a", Exclusion::Draining)],
            RULES_1S,
        );
        assert_eq!(
            d.excluded,
            vec![ex("w-a", Exclusion::Draining), ex("w-b", Exclusion::Full)]
        );
        assert_eq!(
            d.summary(),
            "no worker can take it; compared: []; excluded: [w-a draining, w-b full]"
        );
    }

    #[test]
    fn the_summary_names_the_winner_the_rule_and_every_number() {
        let d = run(vec![
            cand("w-a", measured(80_000)),
            cand("w-b", fallback(50_000)),
            cand("w-c", unknown()),
        ]);
        assert_eq!(
            d.summary(),
            "w-b: earliest predicted completion; compared: [w-b=rank 50000 fallback, \
             w-a=80000ms measured, w-c=unknown (not counted as free)]; excluded: []"
        );
        let d = pair(|a, _| a.load = 1);
        assert_eq!(
            d.summary(),
            "w-b: about the same as others, chosen by load; compared: [w-b=10000ms measured, \
             w-a=10000ms measured]; excluded: []"
        );
        let d = run(vec![cand("w-a", measured(1))]);
        assert_eq!(
            d.summary(),
            "w-a: the only candidate; compared: [w-a=1ms measured]; excluded: []"
        );
        // A value that is no plain word has no word.
        assert_eq!(word(&3), "");
        assert_eq!(word(&Rule::LessWaiting), "less_waiting");
    }

    #[test]
    fn a_decision_reads_back_from_its_record() {
        let mut a = cand("w-a", measured(10));
        a.costs.ms = [Some(1), None, Some(3)];
        let d = decide(
            vec![a, cand("w-b", unknown())],
            vec![Excluded {
                worker_id: w("w-c"),
                why: Exclusion::RefusedTask,
            }],
            RULES_1S,
        );
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["why"], serde_json::json!({"kind": "earliest"}));
        assert_eq!(v["excluded"][0]["why"], "refused_task");
        assert_eq!(v["compared"][1]["outlook"]["null_reason"], "overrun");
        assert_eq!(serde_json::from_value::<Decision>(v).unwrap(), d);
        let tie = serde_json::to_value(Why::TieBreak(Rule::Affinity)).unwrap();
        assert_eq!(
            tie,
            serde_json::json!({"kind": "tie_break", "rule": "affinity"})
        );
    }

    #[test]
    fn an_unknown_cost_gets_the_worst_known_figure_and_is_marked() {
        let c = |t: Option<u64>, s: Option<u64>, k: Option<u64>| Costs {
            ms: [t, s, k],
            ..Costs::default()
        };
        let mut all = vec![
            c(Some(5), None, None),
            c(None, Some(30), None),
            c(Some(9), Some(0), None),
        ];
        fill_costs(&mut all);
        assert_eq!(all[0].ms, [Some(5), Some(30), None]);
        assert_eq!(all[0].assumed, [false, true, false]);
        assert_eq!(all[1].ms, [Some(9), Some(30), None]);
        assert_eq!(all[1].assumed, [true, false, false]);
        assert_eq!(all[2].ms, [Some(9), Some(0), None]);
        assert_eq!(all[2].assumed, [false; 3]);
        // Collection is filled like the others.
        let mut all = vec![c(None, None, Some(7)), c(None, None, None)];
        fill_costs(&mut all);
        assert_eq!(all[1].ms, [None, None, Some(7)]);
        assert_eq!(all[1].assumed, [false, false, true]);
        fill_costs(&mut []);
    }

    #[test]
    fn the_ready_delay_is_transfer_plus_setup_and_unknown_only_when_both_are() {
        let c = |t: Option<u64>, s: Option<u64>| Costs {
            ms: [t, s, Some(1000)],
            ..Costs::default()
        };
        assert_eq!(ready_delay_ms(&c(None, None)), None);
        assert_eq!(ready_delay_ms(&c(Some(10), None)), Some(10));
        assert_eq!(ready_delay_ms(&c(None, Some(30))), Some(30));
        assert_eq!(ready_delay_ms(&c(Some(10), Some(30))), Some(40));
        assert_eq!(ready_delay_ms(&c(Some(u64::MAX), Some(1))), Some(u64::MAX));
    }

    fn answer(finish: Option<u64>, ranking: Option<ForecastRanking>) -> ForecastResponse {
        let mut null_reasons = BTreeMap::new();
        null_reasons.insert(ForecastField::ExecutionMs, Uncertainty::NoHistory);
        if finish.is_none() {
            null_reasons.insert(ForecastField::FinishInMs, Uncertainty::Overrun);
        }
        ForecastResponse {
            schema_version: SchemaVersion::CURRENT,
            worker_id: w("w-a"),
            worker_boot_id: WorkerBootId::parse("b1").unwrap(),
            agent_epoch: AgentEpoch(3),
            snapshot_digest: Digest::of_bytes(b"s"),
            policy_digest: Digest::of_bytes(b"p"),
            ttl_ms: 2000,
            coverage: vec![],
            target: ForecastTarget {
                plan_digest: Digest::of_bytes(b"plan"),
                ready_in_ms: Some(1),
                queue_wait_after_ready_ms: Some(2),
                execution_ms: Some(3),
                finish_in_ms: finish,
                confidence: Confidence::High,
                runtime_source: RuntimeSource::Fallback,
                history_count: 0,
                conditional: true,
                overrun: false,
                wait_reason: None,
                assumptions: vec![],
                null_reasons,
                ranking,
            },
        }
    }

    #[test]
    fn a_forecast_is_read_only_when_it_is_fresh_and_about_what_was_asked() {
        let (worker, plan) = (w("w-a"), Digest::of_bytes(b"plan"));
        let boot = WorkerBootId::parse("b1").unwrap();
        let asked = Asked {
            worker_id: &worker,
            plan_digest: &plan,
            boot_id: &boot,
            agent_epoch: AgentEpoch(3),
        };
        let a = answer(Some(6), None);
        let o = outlook_of(&asked, &a, 1999, 2000);
        assert_eq!(
            o,
            Outlook {
                finish_ms: Some(6),
                ranking_ms: None,
                ready_ms: Some(1),
                wait_ms: Some(2),
                execution_ms: Some(3),
                source: RuntimeSource::Fallback,
                conditional: true,
                null_reason: None,
                unusable: None,
            }
        );
        // The ranking and the reason for the missing time are carried.
        let ranked = answer(
            None,
            Some(ForecastRanking {
                start_in_ms: Some(1),
                finish_in_ms: Some(61),
            }),
        );
        let o = outlook_of(&asked, &ranked, 0, 2000);
        assert_eq!((o.finish_ms, o.ranking_ms), (None, Some(61)));
        assert_eq!(o.null_reason, Some(Uncertainty::Overrun));
        let no_finish = answer(
            None,
            Some(ForecastRanking {
                start_in_ms: Some(1),
                finish_in_ms: None,
            }),
        );
        assert_eq!(outlook_of(&asked, &no_finish, 0, 2000).ranking_ms, None);

        let unusable = |o: Outlook| {
            assert_eq!(o, Outlook::unusable(o.unusable.unwrap()));
            o.unusable.unwrap()
        };
        // The worker's ttl.
        assert_eq!(
            unusable(outlook_of(&asked, &a, 2000, 9000)),
            Unusable::Expired
        );
        // The parent's own limit, whatever the worker says.
        let mut long = a.clone();
        long.ttl_ms = 60_000;
        assert_eq!(
            unusable(outlook_of(&asked, &long, 2000, 2000)),
            Unusable::Expired
        );
        assert_eq!(outlook_of(&asked, &long, 1999, 2000).unusable, None);
        // The worker restarted since `inspect`.
        let other_boot = WorkerBootId::parse("b2").unwrap();
        let rebooted = Asked {
            boot_id: &other_boot,
            ..asked.clone()
        };
        assert_eq!(
            unusable(outlook_of(&rebooted, &a, 0, 2000)),
            Unusable::BootChanged
        );
        let restarted = Asked {
            agent_epoch: AgentEpoch(4),
            ..asked.clone()
        };
        assert_eq!(
            unusable(outlook_of(&restarted, &a, 0, 2000)),
            Unusable::EpochChanged
        );
        // Another worker, or another plan.
        let mut other = a.clone();
        other.worker_id = w("w-b");
        assert_eq!(
            unusable(outlook_of(&asked, &other, 0, 2000)),
            Unusable::NotAsked
        );
        let mut other = a.clone();
        other.target.plan_digest = Digest::of_bytes(b"other");
        assert_eq!(
            unusable(outlook_of(&asked, &other, 0, 2000)),
            Unusable::NotAsked
        );
        // An answer that is not about what was asked is that before it is old.
        assert_eq!(
            unusable(outlook_of(&asked, &other, 9999, 2000)),
            Unusable::NotAsked
        );
        assert_eq!(
            Outlook::unusable(Unusable::Refused).source,
            RuntimeSource::Unknown
        );
    }

    #[test]
    fn an_attempt_moves_only_when_its_worker_keeps_refusing_for_its_own_reasons() {
        use BlockedReason::*;
        use ErrorCode::*;
        let moves = [
            (ProtocolIncompatible, None),
            (Blocked, Some(EnvironmentMismatch)),
            (Blocked, Some(NoCompatibleWorker)),
        ];
        for (code, reason) in moves {
            assert!(moves_elsewhere(code, reason, true));
            // The first such answer may pass.
            assert!(!moves_elsewhere(code, reason, false));
        }
        let stays = [
            (Blocked, Some(SecretUnavailable)),
            // No reason given: nothing says it lies with the worker.
            (Blocked, None),
            // A reason on another code is not read.
            (Overloaded, Some(EnvironmentMismatch)),
            (Overloaded, None),
            (Internal, None),
            (CommunicationUnknown, None),
            (NotImplemented, None),
            (NotFound, None),
            (Conflict, None),
        ];
        for (code, reason) in stays {
            assert!(!moves_elsewhere(code, reason, true));
        }
    }

    #[test]
    fn two_refusals_are_the_same_answer_only_with_the_same_code_and_reason() {
        use BlockedReason::*;
        use ErrorCode::*;
        assert_eq!(refusal_key(Blocked, None), "BLOCKED");
        assert_eq!(
            refusal_key(Blocked, Some(SecretUnavailable)),
            "BLOCKED/secret_unavailable"
        );
        assert_eq!(
            refusal_key(Blocked, Some(EnvironmentMismatch)),
            "BLOCKED/environment_mismatch"
        );
        assert_eq!(
            refusal_key(Blocked, Some(NoCompatibleWorker)),
            "BLOCKED/no_compatible_worker"
        );
        assert_eq!(
            refusal_key(ProtocolIncompatible, None),
            "PROTOCOL_INCOMPATIBLE"
        );
    }

    #[test]
    fn only_exclusions_that_outlive_a_tick_last() {
        use Exclusion::*;
        let lasting = [RefusedTask, Draining];
        for e in [
            Unreached,
            Draining,
            Full,
            RefusedTask,
            NoForecast,
            NotTaking,
        ] {
            assert_eq!(e.lasts(), lasting.contains(&e), "{e:?}");
        }
    }

    #[test]
    fn a_wait_is_the_same_while_its_outcome_and_lasting_exclusions_are() {
        fn waits(excluded: &[(&str, Exclusion)]) -> Decision {
            let excluded = excluded
                .iter()
                .map(|(w, why)| Excluded {
                    worker_id: WorkerId::parse(w).unwrap(),
                    why: *why,
                })
                .collect();
            decide(Vec::new(), excluded, RULES_1S)
        }
        use Exclusion::*;
        let base = waits(&[("w-a", RefusedTask), ("w-b", Full)]);
        // What describes one tick may come, go and change.
        for other in [
            waits(&[("w-a", RefusedTask), ("w-b", NoForecast)]),
            waits(&[("w-a", RefusedTask), ("w-b", NotTaking)]),
            waits(&[("w-a", RefusedTask), ("w-b", Unreached)]),
            waits(&[("w-a", RefusedTask)]),
            waits(&[("w-a", RefusedTask), ("w-b", Full), ("w-c", Full)]),
        ] {
            assert!(same_wait(&base, &other), "{other:?}");
            assert!(same_wait(&other, &base), "{other:?}");
        }
        // A lasting one that comes, goes, changes or names another worker.
        for other in [
            waits(&[("w-a", RefusedTask), ("w-b", RefusedTask)]),
            waits(&[("w-a", RefusedTask), ("w-b", Draining)]),
            waits(&[("w-b", Full)]),
            waits(&[("w-a", Draining), ("w-b", Full)]),
            waits(&[("w-c", RefusedTask), ("w-b", Full)]),
        ] {
            assert!(!same_wait(&base, &other), "{other:?}");
        }
        // Another outcome with the same exclusions.
        let mut placed = base.clone();
        placed.why = Why::OnlyCandidate;
        assert!(!same_wait(&base, &placed));
    }
}
