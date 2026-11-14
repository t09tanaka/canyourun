//! The decisions of the worker forecast, free of I/O: what a request may
//! contain, how a wire workload becomes a task-traffic-controller plan, which
//! demands the overlay holds (and that none is counted twice or dropped), how
//! the simulation's answer becomes the wire answer with a label and a reason
//! for every missing number, and when an answer stops being valid (spec
//! chapter 9 "需要の二重計上と取りこぼしを防ぐ", chapter 11).

use crate::error::{ApiError, ErrorCode};
use crate::ids::{
    AgentEpoch, AttemptId, Digest, IdError, NodeId, PlanDigest, PolicyDigest, SnapshotDigest,
    WorkerBootId, WorkerId,
};
use crate::wire::{
    AttemptPayload, Confidence, Coverage, ForecastField, ForecastRanking, ForecastRequest,
    ForecastResponse, ForecastTarget, RuntimeSource, SchemaVersion, Uncertainty, WaitReason,
    WorkloadNode, WorkloadPlan,
};
use std::collections::{BTreeMap, BTreeSet};
use task_traffic_controller::admit::State;
use task_traffic_controller::forecast as ttc;
use task_traffic_controller::inspect::Inspection;

/// Plans a request may carry as accepted-but-unacknowledged work.
pub const MAX_PENDING_PLANS: usize = 64;
/// Nodes of the workload and of every pending plan together.
pub const MAX_NODES: usize = 2048;

/// Why a request was refused. Nothing is allocated for a request that fails
/// these checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    TooManyPending(usize),
    TooManyNodes(usize),
    EmptyWorkload,
    /// A node whose correlation does not name its own plan and node.
    BadCorrelation(NodeId),
}

impl RequestError {
    pub fn into_api(self) -> ApiError {
        ApiError::new(ErrorCode::InvalidInput, self.to_string())
    }
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::TooManyPending(n) => write!(
                f,
                "{n} pending plans; at most {MAX_PENDING_PLANS} are accepted"
            ),
            RequestError::TooManyNodes(n) => {
                write!(f, "{n} nodes; at most {MAX_NODES} are accepted")
            }
            RequestError::EmptyWorkload => f.write_str("the workload has no node"),
            RequestError::BadCorrelation(n) => write!(
                f,
                "node `{n}` correlation does not match its plan or node id"
            ),
        }
    }
}

/// Check a request before anything is built from it.
pub fn check_request(req: &ForecastRequest) -> Result<(), RequestError> {
    if req.pending.len() > MAX_PENDING_PLANS {
        return Err(RequestError::TooManyPending(req.pending.len()));
    }
    let nodes = req.pending.iter().fold(req.workload.nodes.len(), |n, p| {
        n.saturating_add(p.nodes.len())
    });
    if nodes > MAX_NODES {
        return Err(RequestError::TooManyNodes(nodes));
    }
    if req.workload.nodes.is_empty() {
        return Err(RequestError::EmptyWorkload);
    }
    for plan in std::iter::once(&req.workload).chain(&req.pending) {
        plan.check()
            .map_err(|e| RequestError::BadCorrelation(e.node_id))?;
    }
    Ok(())
}

/// Numbers the worker fills in where a request gives none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Defaults {
    /// The task-traffic-controller `default_weight`.
    pub weight: u32,
}

/// What the mapping had to assume. `soft` marks the answer conditional.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Notes {
    pub assumptions: Vec<String>,
    pub soft: bool,
}

impl Notes {
    /// An assumption that does not make the answer conditional.
    pub fn note(&mut self, text: String) {
        if !self.assumptions.contains(&text) {
            self.assumptions.push(text);
        }
    }

    fn add(&mut self, text: String) {
        if !self.assumptions.contains(&text) {
            self.assumptions.push(text);
        }
        self.soft = true;
    }
}

fn runtime_of(ms: Option<u64>) -> ttc::Runtime {
    match ms {
        // The coordinator's figure comes from recorded runs.
        Some(ms) => ttc::Runtime::measured(ms as f64 / 1000.0, 0),
        None => ttc::Runtime::fallback(),
    }
}

fn priority_of(p: i32) -> ttc::Priority {
    if p > 0 {
        ttc::Priority::Front
    } else {
        ttc::Priority::Normal
    }
}

fn node_notes(n: &WorkloadNode, notes: &mut Notes, id: &str) {
    if n.predicted_rss_bytes.is_none() {
        notes.add(format!("node {id}: predicted memory unknown"));
    }
    if n.weight.is_none() {
        notes.add(format!("node {id}: weight not given; the default is used"));
    }
    if n.ready_in_ms.is_none() {
        notes.add(format!(
            "node {id}: ready_in_ms not given; assumed ready now"
        ));
    }
    if n.concurrency.max_waiting.is_some() || n.concurrency.max_running.is_some() {
        notes.add(format!(
            "node {id}: max_waiting and max_running are not modelled; only max_registered is"
        ));
    }
}

/// The plan to simulate: the requested workload. `max_registered` of the
/// nodes (the smallest one) becomes the plan's in-flight cap.
pub fn target_plan(w: &WorkloadPlan, d: Defaults) -> (ttc::WorkloadPlan, Notes) {
    let mut notes = Notes::default();
    let nodes = w
        .nodes
        .iter()
        .map(|n| {
            node_notes(n, &mut notes, n.node_id.as_str());
            ttc::CommandNode {
                node_id: n.node_id.to_string(),
                identity: n.node_id.to_string(),
                label: n.node_id.to_string(),
                weight: n.weight.unwrap_or(d.weight).max(1),
                predicted_rss: n.predicted_rss_bytes.unwrap_or(0),
                runtime: runtime_of(n.predicted_runtime_ms),
                priority: priority_of(n.priority),
                depends_on: n.depends_on.iter().map(|d| d.to_string()).collect(),
                ready_in_ms: n.ready_in_ms.unwrap_or(0),
                external_delay_ms: n.external_delay_ms.unwrap_or(0),
                ledger_entry: n.correlation.ledger_entry.clone(),
                shared_id: n.shared_builder.as_ref().map(|b| b.to_string()),
            }
        })
        .collect();
    let cap = w
        .nodes
        .iter()
        .filter_map(|n| n.concurrency.max_registered)
        .min()
        .map(|c| c.max(1) as usize);
    (
        ttc::WorkloadPlan {
            attempt_id: w.attempt_id.to_string(),
            plan_digest: w.plan_digest.to_string(),
            expansion_seq: w.expansion_seq,
            max_in_flight: cap,
            nodes,
        },
        notes,
    )
}

/// One node of an attempt the worker accepted and has not finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedNode {
    pub attempt_id: AttemptId,
    pub plan_digest: PlanDigest,
    pub node_id: NodeId,
    /// The id the node is (or will be) registered with.
    pub command_id: String,
    /// The node finished: it is no demand any more.
    pub terminal: bool,
}

/// The nodes of an accepted attempt with the command id each is registered
/// under. The supervisor numbers nodes in execution order, not in the order
/// of the payload, so this uses the same order.
pub fn accepted_nodes(
    attempt: &AttemptId,
    payload: &AttemptPayload,
) -> Result<Vec<AcceptedNode>, &'static str> {
    let order = crate::worker::policy::execution_order(&payload.commands)?;
    Ok(order
        .into_iter()
        .enumerate()
        .map(|(position, i)| AcceptedNode {
            attempt_id: attempt.clone(),
            plan_digest: payload.plan_digest.clone(),
            node_id: payload.commands[i].node_id.clone(),
            command_id: crate::supervisor::spec::command_id(attempt, position),
            terminal: false,
        })
        .collect())
}

fn demand_of_accepted(a: &AcceptedNode, d: Defaults) -> ttc::FutureDemand {
    ttc::FutureDemand {
        attempt_id: a.attempt_id.to_string(),
        plan_digest: a.plan_digest.to_string(),
        expansion_seq: 0,
        node_id: a.node_id.to_string(),
        weight: d.weight.max(1),
        predicted_rss: 0,
        runtime: ttc::Runtime::fallback(),
        priority: ttc::Priority::Normal,
        ready_in_ms: 0,
        ledger_entry: Some(a.command_id.clone()),
        shared_id: None,
        terminal: a.terminal,
    }
}

fn demand_of_pending(plan: &WorkloadPlan, n: &WorkloadNode, d: Defaults) -> ttc::FutureDemand {
    ttc::FutureDemand {
        attempt_id: plan.attempt_id.to_string(),
        plan_digest: plan.plan_digest.to_string(),
        expansion_seq: plan.expansion_seq,
        node_id: n.node_id.to_string(),
        weight: n.weight.unwrap_or(d.weight).max(1),
        predicted_rss: n.predicted_rss_bytes.unwrap_or(0),
        runtime: runtime_of(n.predicted_runtime_ms),
        priority: priority_of(n.priority),
        ready_in_ms: n.ready_in_ms.unwrap_or(0),
        ledger_entry: n.correlation.ledger_entry.clone(),
        shared_id: n.shared_builder.as_ref().map(|b| b.to_string()),
        terminal: false,
    }
}

/// The demands that are not in the ledger: what this worker accepted, and
/// what the coordinator is sending. A node is one demand however many
/// places name it. `attempt_id / node_id` decides, not the plan digest or
/// the expansion number, because an accepted attempt does not carry the
/// latter. The requested workload itself is never also overlay demand.
///
/// For a node both lists name, the coordinator's numbers are used (they
/// come from recorded runs) and the worker's facts win where it knows them:
/// the ledger id the node registers as, and that it finished.
pub fn merge_overlay(
    target: &WorkloadPlan,
    pending: &[WorkloadPlan],
    accepted: &[AcceptedNode],
    d: Defaults,
) -> Vec<ttc::FutureDemand> {
    let in_target: BTreeSet<(&str, &str)> = target
        .nodes
        .iter()
        .map(|n| (target.attempt_id.as_str(), n.node_id.as_str()))
        .collect();
    let known: BTreeMap<(&str, &str), &AcceptedNode> = accepted
        .iter()
        .map(|a| ((a.attempt_id.as_str(), a.node_id.as_str()), a))
        .collect();
    let mut taken: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out: Vec<ttc::FutureDemand> = Vec::new();
    for plan in pending {
        for n in &plan.nodes {
            let key = (plan.attempt_id.as_str(), n.node_id.as_str());
            if in_target.contains(&key) || !taken.insert((key.0.to_string(), key.1.to_string())) {
                continue;
            }
            let mut demand = demand_of_pending(plan, n, d);
            if let Some(a) = known.get(&key) {
                demand.ledger_entry = Some(a.command_id.clone());
                demand.terminal = a.terminal;
            }
            out.push(demand);
        }
    }
    for a in accepted {
        let key = (a.attempt_id.as_str(), a.node_id.as_str());
        if in_target.contains(&key) || !taken.insert((key.0.to_string(), key.1.to_string())) {
            continue;
        }
        out.push(demand_of_accepted(a, d));
    }
    out.sort_by(|a, b| {
        (&a.attempt_id, &a.node_id, a.expansion_seq).cmp(&(
            &b.attempt_id,
            &b.node_id,
            b.expansion_seq,
        ))
    });
    out
}

/// The digest of what the answer rests on: the ledger and the histories it
/// refers to, the policy, and the overlay (spec chapter 16). It changes when
/// any of them does.
pub fn snapshot_digest(
    ledger_digest: &str,
    policy_digest: &str,
    overlay: &[ttc::FutureDemand],
) -> Digest {
    let doc = serde_json::json!({
        "ledger": ledger_digest,
        "policy": policy_digest,
        "overlay": overlay,
    });
    // Serializing a `Value` cannot fail.
    Digest::of_bytes(doc.to_string().as_bytes())
}

/// Running ledger entries whose remaining time is unknown: past their
/// history, without a history, without a start time, or without a lease.
pub fn overrunning(insp: &Inspection) -> usize {
    insp.entries
        .iter()
        .filter(|e| e.entry.state == State::Running)
        .filter(|e| {
            let elapsed_ms = e
                .entry
                .started_at
                .map(|s| (insp.taken_at - s).num_milliseconds().max(0));
            match (e.entry.expected_secs, elapsed_ms) {
                (Some(d), Some(el)) => e.suspect_dead || (d * 1000.0) as i64 <= el,
                _ => true,
            }
        })
        .count()
}

/// The facts a missing number is explained from. They come from the plan and
/// the snapshot, not from the simulation's wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    pub unmeasured: bool,
    pub overrun: bool,
    pub memory_blocked: bool,
}

fn reason_for(field: ForecastField, f: Facts) -> Uncertainty {
    let own_runtime = matches!(
        field,
        ForecastField::ExecutionMs | ForecastField::FinishInMs
    );
    if own_runtime && f.unmeasured {
        Uncertainty::NoHistory
    } else if f.overrun {
        Uncertainty::Overrun
    } else if f.unmeasured {
        Uncertainty::NoHistory
    } else if f.memory_blocked {
        Uncertainty::MemoryBlocked
    } else {
        Uncertainty::Conditional
    }
}

/// The worst source among runtimes: one unknown makes the whole unknown,
/// one fallback makes it a fallback.
fn source_of<'a>(runtimes: impl IntoIterator<Item = &'a ttc::Runtime>) -> RuntimeSource {
    let mut worst = RuntimeSource::Measured;
    for r in runtimes {
        match r.source {
            ttc::RuntimeSource::Unknown => return RuntimeSource::Unknown,
            ttc::RuntimeSource::Fallback => worst = RuntimeSource::Fallback,
            ttc::RuntimeSource::Measured => {}
        }
    }
    worst
}

/// The source of what the answer rests on: the target's nodes and the overlay
/// demands that were applied ahead of it. An accepted node that is not
/// registered yet runs on the fallback runtime, and the target's start
/// depends on it, so a measured target behind it is no measured answer.
fn answer_source(
    plan: &ttc::WorkloadPlan,
    overlay: &[ttc::FutureDemand],
    applied: &[String],
) -> RuntimeSource {
    let ahead = overlay
        .iter()
        .filter(|d| applied.contains(&format!("{}/{}", d.attempt_id, d.node_id)))
        .map(|d| &d.runtime);
    source_of(plan.nodes.iter().map(|n| &n.runtime).chain(ahead))
}

fn confidence_of(c: ttc::Confidence) -> Confidence {
    match c {
        ttc::Confidence::High => Confidence::High,
        ttc::Confidence::Medium => Confidence::Medium,
        ttc::Confidence::Low | ttc::Confidence::None => Confidence::Low,
    }
}

fn wait_reason_of(kind: &str) -> Option<WaitReason> {
    match kind {
        "queue" | "reserved" => Some(WaitReason::QueueAhead),
        "budget" | "cpu" | "overload" => Some(WaitReason::ConcurrencyLimit),
        "memory" => Some(WaitReason::MemoryBlocked),
        "not_ready" => Some(WaitReason::ExternalDelay),
        _ => None,
    }
}

/// The worker answering.
#[derive(Debug, Clone)]
pub struct Ident {
    pub worker_id: WorkerId,
    pub boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    /// How long an answer stays usable (`forecast_ttl_ms`; the spec default
    /// is 2000). It replaces the simulation's own constant on the wire.
    pub forecast_ttl_ms: u64,
}

/// A simulation answer the wire cannot carry (an id it does not parse as).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertError(pub String);

impl From<IdError> for ConvertError {
    fn from(e: IdError) -> Self {
        ConvertError(e.to_string())
    }
}

/// Everything [`to_wire`] needs besides the simulation's answer.
pub struct Wiring<'a> {
    pub ident: &'a Ident,
    pub plan: &'a ttc::WorkloadPlan,
    pub target_digest: &'a PlanDigest,
    pub insp: &'a Inspection,
    pub overlay: &'a [ttc::FutureDemand],
    pub notes: &'a Notes,
}

/// The wire answer. A value is `null` exactly when it has a reason; a
/// fallback figure is never returned as a time (it is for ranking only).
pub fn to_wire(fc: &ttc::Forecast, w: &Wiring) -> Result<ForecastResponse, ConvertError> {
    let t = &fc.target.timing;
    let multi = fc.target.nodes.len() >= 2;
    // Several nodes may run side by side: their sum is no time on the clock,
    // so a plan reports what its critical path spends.
    let (wait, exec) = if multi {
        (
            fc.target.critical_path_wait_ms,
            fc.target.critical_path_execution_ms,
        )
    } else {
        (t.queue_wait_after_ready_ms, t.execution_ms)
    };
    let overrun_entries = overrunning(w.insp);
    let waits = t.wait_reason.as_ref().and_then(|r| wait_reason_of(&r.kind));
    let source = answer_source(w.plan, w.overlay, &fc.overlay.applied);
    let facts = Facts {
        unmeasured: source != RuntimeSource::Measured,
        overrun: overrun_entries > 0 && t.start_in_ms.is_none(),
        memory_blocked: waits == Some(WaitReason::MemoryBlocked),
    };
    let mut null_reasons = BTreeMap::new();
    for (field, value) in [
        (ForecastField::ReadyInMs, t.ready_in_ms),
        (ForecastField::QueueWaitAfterReadyMs, wait),
        (ForecastField::ExecutionMs, exec),
        (ForecastField::FinishInMs, t.finish_in_ms),
    ] {
        if value.is_none() {
            null_reasons.insert(field, reason_for(field, facts));
        }
    }
    let mut assumptions = t.assumptions.clone();
    if source != RuntimeSource::Measured
        && source_of(w.plan.nodes.iter().map(|n| &n.runtime)) == RuntimeSource::Measured
    {
        assumptions.push(
            "work queued ahead of the target has no recorded runtime; its fallback runtime is used for ordering only"
                .to_string(),
        );
    }
    for a in &w.notes.assumptions {
        if !assumptions.contains(a) {
            assumptions.push(a.clone());
        }
    }
    let mut coverage = Vec::with_capacity(fc.coverage.len());
    for c in &fc.coverage {
        coverage.push(Coverage {
            attempt_id: AttemptId::parse(&c.attempt_id)?,
            plan_digest: PlanDigest::parse(&c.plan_digest)?,
            expansion_seq: c.expansion_seq,
            node_ids: c
                .node_ids
                .iter()
                .map(|n| NodeId::parse(n))
                .collect::<Result<_, _>>()?,
        });
    }
    let ledger = Digest::parse(&fc.snapshot_digest)?;
    let policy: PolicyDigest = Digest::parse(&fc.policy_digest)?;
    let snapshot: SnapshotDigest = snapshot_digest(ledger.as_str(), policy.as_str(), w.overlay);
    Ok(ForecastResponse {
        schema_version: SchemaVersion::CURRENT,
        worker_id: w.ident.worker_id.clone(),
        worker_boot_id: w.ident.boot_id.clone(),
        agent_epoch: w.ident.agent_epoch,
        snapshot_digest: snapshot,
        policy_digest: policy,
        ttl_ms: w.ident.forecast_ttl_ms,
        coverage,
        target: ForecastTarget {
            plan_digest: w.target_digest.clone(),
            ready_in_ms: t.ready_in_ms,
            queue_wait_after_ready_ms: wait,
            execution_ms: exec,
            finish_in_ms: t.finish_in_ms,
            confidence: confidence_of(t.confidence),
            runtime_source: source,
            history_count: u32::try_from(t.history_count).unwrap_or(u32::MAX),
            conditional: t.conditional || w.notes.soft,
            overrun: facts.overrun,
            wait_reason: waits,
            assumptions,
            null_reasons,
            ranking: t.ranking.as_ref().map(|r| ForecastRanking {
                start_in_ms: r.start_in_ms,
                finish_in_ms: r.finish_in_ms,
            }),
        },
    })
}

/// Whether a forecast may still be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    Fresh,
    /// Older than its `ttl_ms`.
    Expired,
    /// The agent restarted since: its queue is not the one that was read.
    EpochChanged,
    /// The machine rebooted since.
    BootChanged,
}

/// A forecast is a reading, not a reservation. It is stale once `ttl_ms` has
/// passed since it was taken, and at once when the agent or the machine
/// started again, whichever the age.
///
/// `age_ms` is supplied by the caller and must come from a monotonic clock
/// (time since the answer arrived). A caller that only has the wall clock
/// must saturate a backwards step to 0 (`now.saturating_sub(taken)`), which
/// reads as fresh rather than as an enormous age.
pub fn validity(
    answer: &ForecastResponse,
    age_ms: u64,
    current_epoch: AgentEpoch,
    current_boot: &WorkerBootId,
) -> Validity {
    if &answer.worker_boot_id != current_boot {
        Validity::BootChanged
    } else if answer.agent_epoch != current_epoch {
        Validity::EpochChanged
    } else if age_ms >= answer.ttl_ms {
        Validity::Expired
    } else {
        Validity::Fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BuilderId, CoordinatorEpoch, RequestId};
    use crate::wire::{CommandSpec, ConcurrencyLimits, Correlation};
    use chrono::{DateTime, Duration as Cd, Utc};
    use task_traffic_controller::admit::Limits;
    use task_traffic_controller::inspect::InspectedEntry;
    use task_traffic_controller::ledger::Entry;

    const D: Defaults = Defaults { weight: 2 };

    #[test]
    fn repeated_assumptions_are_deduplicated_but_can_make_the_answer_conditional() {
        let mut notes = Notes::default();
        notes.note("runtime was assumed".into());
        notes.note("runtime was assumed".into());
        assert_eq!(notes.assumptions, ["runtime was assumed"]);
        assert!(!notes.soft);

        // A conditional occurrence still changes confidence when the same
        // wording was already recorded as an unconditional assumption.
        notes.add("runtime was assumed".into());
        notes.add("runtime was assumed".into());
        assert_eq!(notes.assumptions, ["runtime was assumed"]);
        assert!(notes.soft);
    }

    fn t0() -> DateTime<Utc> {
        "2030-01-01T00:00:00Z".parse().unwrap()
    }

    fn node(attempt: &AttemptId, id: &str) -> WorkloadNode {
        WorkloadNode {
            node_id: NodeId::parse(id).unwrap(),
            depends_on: vec![],
            priority: 0,
            weight: Some(3),
            predicted_rss_bytes: Some(10),
            predicted_runtime_ms: Some(1500),
            external_delay_ms: Some(7),
            ready_in_ms: Some(9),
            concurrency: ConcurrencyLimits::default(),
            correlation: Correlation {
                attempt_id: attempt.clone(),
                node_id: NodeId::parse(id).unwrap(),
                ledger_entry: None,
            },
            shared_builder: None,
        }
    }

    fn plan(attempt: &AttemptId, nodes: Vec<WorkloadNode>) -> WorkloadPlan {
        WorkloadPlan {
            attempt_id: attempt.clone(),
            plan_digest: Digest::of_bytes(b"p"),
            expansion_seq: 4,
            nodes,
        }
    }

    fn request(w: WorkloadPlan, pending: Vec<WorkloadPlan>) -> ForecastRequest {
        ForecastRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            workload: w,
            pending,
        }
    }

    fn acc(attempt: &AttemptId, id: &str, i: usize, terminal: bool) -> AcceptedNode {
        AcceptedNode {
            attempt_id: attempt.clone(),
            plan_digest: Digest::of_bytes(b"acc"),
            node_id: NodeId::parse(id).unwrap(),
            command_id: format!("cmd-{i}"),
            terminal,
        }
    }

    #[test]
    fn a_request_is_checked_before_anything_is_built() {
        let a = AttemptId::generate();
        let ok = request(plan(&a, vec![node(&a, "x")]), vec![]);
        assert_eq!(check_request(&ok), Ok(()));
        // The limits are exact: the cap itself passes, one more does not.
        let many = |n| (0..n).map(|_| plan(&a, vec![node(&a, "x")])).collect();
        assert_eq!(
            check_request(&request(
                plan(&a, vec![node(&a, "x")]),
                many(MAX_PENDING_PLANS)
            )),
            Ok(())
        );
        assert_eq!(
            check_request(&request(
                plan(&a, vec![node(&a, "x")]),
                many(MAX_PENDING_PLANS + 1)
            )),
            Err(RequestError::TooManyPending(MAX_PENDING_PLANS + 1))
        );
        let nodes = |n: usize| {
            (0..n)
                .map(|i| node(&a, &format!("n{i}")))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            check_request(&request(plan(&a, nodes(MAX_NODES)), vec![])),
            Ok(())
        );
        assert_eq!(
            check_request(&request(
                plan(&a, nodes(MAX_NODES)),
                vec![plan(&a, nodes(1))]
            )),
            Err(RequestError::TooManyNodes(MAX_NODES + 1))
        );
        assert_eq!(
            check_request(&request(plan(&a, vec![]), vec![])),
            Err(RequestError::EmptyWorkload)
        );
        let mut bad = node(&a, "x");
        bad.correlation.node_id = NodeId::parse("other").unwrap();
        assert_eq!(
            check_request(&request(plan(&a, vec![bad.clone()]), vec![])),
            Err(RequestError::BadCorrelation(NodeId::parse("x").unwrap()))
        );
        // A pending plan is checked too.
        assert!(check_request(&request(
            plan(&a, vec![node(&a, "x")]),
            vec![plan(&a, vec![bad])]
        ))
        .is_err());
    }

    #[test]
    fn a_refusal_is_invalid_input_and_says_why() {
        for e in [
            RequestError::TooManyPending(65),
            RequestError::TooManyNodes(2049),
            RequestError::EmptyWorkload,
            RequestError::BadCorrelation(NodeId::parse("x").unwrap()),
        ] {
            let api = e.clone().into_api();
            assert_eq!(api.code, ErrorCode::InvalidInput);
            assert_eq!(api.message, e.to_string());
            assert!(!api.message.is_empty());
        }
        assert!(RequestError::TooManyPending(65).to_string().contains("65"));
        assert!(RequestError::TooManyNodes(2049)
            .to_string()
            .contains("2049"));
    }

    #[test]
    fn the_target_plan_keeps_every_number_the_request_gave() {
        let a = AttemptId::generate();
        let builder = BuilderId::generate();
        let mut n = node(&a, "x");
        n.depends_on = vec![NodeId::parse("w").unwrap()];
        n.priority = 1;
        n.concurrency.max_registered = Some(3);
        n.shared_builder = Some(builder.clone());
        n.correlation.ledger_entry = Some("led".into());
        let mut m = node(&a, "w");
        m.concurrency.max_registered = Some(2);
        let (p, notes) = target_plan(&plan(&a, vec![n, m]), D);
        assert_eq!(p.attempt_id, a.to_string());
        assert_eq!(p.expansion_seq, 4);
        assert_eq!(p.max_in_flight, Some(2));
        let x = &p.nodes[0];
        assert_eq!(x.node_id, "x");
        assert_eq!(x.weight, 3);
        assert_eq!(x.predicted_rss, 10);
        assert_eq!(x.runtime.secs, Some(1.5));
        assert_eq!(x.runtime.source, ttc::RuntimeSource::Measured);
        assert_eq!(x.priority, ttc::Priority::Front);
        assert_eq!(x.depends_on, vec!["w".to_string()]);
        assert_eq!(x.ready_in_ms, 9);
        assert_eq!(x.external_delay_ms, 7);
        assert_eq!(x.ledger_entry.as_deref(), Some("led"));
        assert_eq!(x.shared_id, Some(builder.to_string()));
        assert_eq!(p.nodes[1].priority, ttc::Priority::Normal);
        assert!(!notes.soft);
        assert!(notes.assumptions.is_empty());
    }

    #[test]
    fn what_a_request_leaves_out_is_assumed_and_said() {
        let a = AttemptId::generate();
        let mut n = node(&a, "x");
        n.weight = None;
        n.predicted_rss_bytes = None;
        n.predicted_runtime_ms = None;
        n.ready_in_ms = None;
        n.external_delay_ms = None;
        n.concurrency.max_running = Some(1);
        let (p, notes) = target_plan(
            &plan(
                &a,
                vec![n.clone(), {
                    let mut m = n.clone();
                    m.node_id = NodeId::parse("y").unwrap();
                    m.correlation.node_id = m.node_id.clone();
                    m
                }],
            ),
            D,
        );
        let x = &p.nodes[0];
        assert_eq!(x.weight, 2);
        assert_eq!(x.predicted_rss, 0);
        assert_eq!(x.runtime.source, ttc::RuntimeSource::Fallback);
        assert_eq!(x.ready_in_ms, 0);
        assert_eq!(x.external_delay_ms, 0);
        assert_eq!(p.max_in_flight, None);
        assert!(notes.soft);
        // One note per node and kind, and no repeats.
        assert!(notes
            .assumptions
            .iter()
            .any(|t| t.contains("node x: predicted memory unknown")));
        assert!(notes
            .assumptions
            .iter()
            .any(|t| t.contains("node y: weight not given")));
        assert!(notes
            .assumptions
            .iter()
            .any(|t| t.contains("ready_in_ms not given")));
        assert!(notes
            .assumptions
            .iter()
            .any(|t| t.contains("max_waiting and max_running")));
        assert_eq!(notes.assumptions.len(), 8);
        // A zero weight and a zero cap are raised to one, never read as free.
        let mut z = node(&a, "z");
        z.weight = Some(0);
        z.concurrency.max_registered = Some(0);
        let (p, _) = target_plan(&plan(&a, vec![z]), Defaults { weight: 0 });
        assert_eq!(p.nodes[0].weight, 1);
        assert_eq!(p.max_in_flight, Some(1));
        let mut z = node(&a, "z");
        z.weight = None;
        assert_eq!(
            target_plan(&plan(&a, vec![z]), Defaults { weight: 0 })
                .0
                .nodes[0]
                .weight,
            1
        );
    }

    #[test]
    fn accepted_nodes_are_numbered_in_execution_order() {
        let a = AttemptId::generate();
        let cmd = |name: &str, deps: &[&str]| CommandSpec {
            setup_step: None,
            node_id: NodeId::parse(name).unwrap(),
            argv: vec!["true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: deps.iter().map(|d| NodeId::parse(d).unwrap()).collect(),
            timeout_ms: None,
        };
        // `test` is listed first but runs after `setup`.
        let mut payload = payload_of(vec![cmd("test", &["setup"]), cmd("setup", &[])]);
        let nodes = accepted_nodes(&a, &payload).unwrap();
        assert_eq!(nodes[0].node_id.as_str(), "setup");
        assert_eq!(
            nodes[0].command_id,
            crate::supervisor::spec::command_id(&a, 0)
        );
        assert_eq!(nodes[1].node_id.as_str(), "test");
        assert_eq!(
            nodes[1].command_id,
            crate::supervisor::spec::command_id(&a, 1)
        );
        assert!(nodes.iter().all(|n| !n.terminal && n.attempt_id == a));
        assert_eq!(nodes[0].plan_digest, payload.plan_digest);
        payload.commands[1].depends_on = vec![NodeId::parse("test").unwrap()];
        assert!(accepted_nodes(&a, &payload).is_err());
    }

    fn payload_of(commands: Vec<CommandSpec>) -> AttemptPayload {
        let d = Digest::of_bytes(b"x");
        AttemptPayload {
            setup: None,
            run_id: crate::ids::RunId::generate(),
            task_id: crate::ids::TaskId::generate(),
            generation: crate::ids::Generation(1),
            source_id: crate::ids::SourceId::generate(),
            plan_digest: d.clone(),
            manifest_digest: d,
            permit_id: None,
            commands,
            lease_ms: 1,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn the_overlay_holds_each_node_once_whoever_names_it() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let c = AttemptId::generate();
        let target = plan(&a, vec![node(&a, "main")]);
        // `b/one` is pending and accepted, `b/two` only accepted, `c/x`
        // only pending (twice, in two plans), and the target's own node is
        // pending and accepted too.
        let pend = vec![
            plan(&b, vec![node(&b, "one")]),
            plan(&c, vec![node(&c, "x")]),
            plan(&c, vec![node(&c, "x")]),
            plan(&a, vec![node(&a, "main")]),
        ];
        let accepted = vec![
            acc(&b, "one", 0, false),
            acc(&b, "two", 1, false),
            acc(&a, "main", 2, false),
        ];
        let o = merge_overlay(&target, &pend, &accepted, D);
        let keys: Vec<(String, String)> = o
            .iter()
            .map(|d| (d.attempt_id.clone(), d.node_id.clone()))
            .collect();
        let mut want = vec![
            (b.to_string(), "one".to_string()),
            (b.to_string(), "two".to_string()),
            (c.to_string(), "x".to_string()),
        ];
        want.sort();
        assert_eq!(keys, want);
        // For `b/one` the coordinator's numbers and the worker's ledger id.
        let one = o.iter().find(|d| d.node_id == "one").unwrap();
        assert_eq!(one.weight, 3);
        assert_eq!(one.runtime.source, ttc::RuntimeSource::Measured);
        assert_eq!(one.ledger_entry.as_deref(), Some("cmd-0"));
        assert_eq!(one.expansion_seq, 4);
        assert!(!one.terminal);
        // For `b/two` only what the worker knows: the default weight, a
        // fallback runtime, and no claim about memory.
        let two = o.iter().find(|d| d.node_id == "two").unwrap();
        assert_eq!((two.weight, two.predicted_rss), (2, 0));
        assert_eq!(two.runtime.source, ttc::RuntimeSource::Fallback);
        assert_eq!(two.ledger_entry.as_deref(), Some("cmd-1"));
        assert_eq!(two.expansion_seq, 0);
        assert_eq!(two.priority, ttc::Priority::Normal);
        assert_eq!(two.ready_in_ms, 0);
        // A pending node nobody accepted keeps its own correlation.
        let x = o.iter().find(|d| d.node_id == "x").unwrap();
        assert_eq!(x.ledger_entry, None);
        assert_eq!(x.ready_in_ms, 9);
    }

    #[test]
    fn a_finished_node_is_marked_so_whichever_list_names_it() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let target = plan(&a, vec![node(&a, "main")]);
        let pend = vec![plan(&b, vec![node(&b, "one")])];
        let o = merge_overlay(
            &target,
            &pend,
            &[acc(&b, "one", 0, true), acc(&b, "two", 1, true)],
            D,
        );
        assert_eq!(o.len(), 2);
        assert!(o.iter().all(|d| d.terminal));
    }

    #[test]
    fn a_pending_front_node_stays_front() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let mut n = node(&b, "one");
        n.priority = 5;
        n.shared_builder = Some(BuilderId::generate());
        let o = merge_overlay(
            &plan(&a, vec![node(&a, "main")]),
            &[plan(&b, vec![n])],
            &[],
            D,
        );
        assert_eq!(o[0].priority, ttc::Priority::Front);
        assert!(o[0].shared_id.is_some());
        let mut n = node(&b, "one");
        n.priority = 0;
        let o = merge_overlay(
            &plan(&a, vec![node(&a, "main")]),
            &[plan(&b, vec![n.clone()])],
            &[],
            D,
        );
        assert_eq!(o[0].priority, ttc::Priority::Normal);
        n.priority = -1;
        let o = merge_overlay(
            &plan(&a, vec![node(&a, "main")]),
            &[plan(&b, vec![n])],
            &[],
            D,
        );
        assert_eq!(o[0].priority, ttc::Priority::Normal);
    }

    #[test]
    fn the_snapshot_digest_moves_with_each_of_its_parts() {
        let a = AttemptId::generate();
        let demand = demand_of_accepted(&acc(&a, "x", 0, false), D);
        let base = snapshot_digest("l", "p", &[]);
        assert_eq!(base, snapshot_digest("l", "p", &[]));
        assert_ne!(base, snapshot_digest("l2", "p", &[]));
        assert_ne!(base, snapshot_digest("l", "p2", &[]));
        assert_ne!(
            base,
            snapshot_digest("l", "p", std::slice::from_ref(&demand))
        );
        let mut finished = demand.clone();
        finished.terminal = true;
        assert_ne!(
            snapshot_digest("l", "p", std::slice::from_ref(&demand)),
            snapshot_digest("l", "p", &[finished])
        );
        // The three parts are not interchangeable.
        assert_ne!(
            snapshot_digest("a", "b", &[]),
            snapshot_digest("b", "a", &[])
        );
    }

    fn entry(
        state: State,
        expected: Option<f64>,
        started: Option<i64>,
        dead: bool,
    ) -> InspectedEntry {
        InspectedEntry {
            entry: Entry {
                label: None,
                id: "e".into(),
                pid: 1,
                state,
                front: false,
                weight: 1,
                predicted_rss: 0,
                expected_secs: expected,
                repo: String::new(),
                subdir: String::new(),
                argv: "e".into(),
                key: "e".into(),
                seq: 1,
                enqueued_at: t0(),
                started_at: started.map(|s| t0() - Cd::seconds(s)),
                using: None,
                backfilled: false,
            },
            suspect_dead: dead,
        }
    }

    fn insp_of(entries: Vec<InspectedEntry>) -> Inspection {
        Inspection {
            schema_version: 1,
            taken_at: t0(),
            snapshot_digest: Digest::of_bytes(b"l").to_string(),
            policy_digest: Digest::of_bytes(b"p").to_string(),
            limits: Limits {
                budget: 1,
                memory_reserve: 0,
                cpu_threshold: 0.9,
                cpu_guard_max_wait_secs: 1.0,
                front_overload_load: 0.0,
                front_overload_max_wait_secs: 1.0,
                backfill_max_job_secs: 0.0,
                backfill_max_wait_secs: 1.0,
            },
            live: None,
            ledger_locked: true,
            entries,
            history: vec![],
            history_quality: vec![],
        }
    }

    #[test]
    fn overrun_counts_only_running_entries_whose_remaining_time_is_unknown() {
        let n = |e| overrunning(&insp_of(vec![e]));
        // Time left: not overrun. Exactly out of time, past it, no history,
        // no start time, no lease: unknown.
        assert_eq!(n(entry(State::Running, Some(10.0), Some(5), false)), 0);
        assert_eq!(n(entry(State::Running, Some(10.0), Some(10), false)), 1);
        assert_eq!(n(entry(State::Running, Some(10.0), Some(11), false)), 1);
        assert_eq!(n(entry(State::Running, None, Some(1), false)), 1);
        assert_eq!(n(entry(State::Running, Some(10.0), None, false)), 1);
        assert_eq!(n(entry(State::Running, Some(10.0), Some(5), true)), 1);
        // A waiting entry has no remaining time to be unknown.
        assert_eq!(n(entry(State::Waiting, None, None, false)), 0);
        assert_eq!(
            overrunning(&insp_of(vec![
                entry(State::Running, None, None, false),
                entry(State::Running, Some(10.0), Some(1), false),
                entry(State::Running, Some(10.0), Some(99), false),
            ])),
            2
        );
    }

    #[test]
    fn a_missing_number_is_explained_from_the_facts_in_a_fixed_order() {
        let f = |unmeasured, overrun, memory_blocked| Facts {
            unmeasured,
            overrun,
            memory_blocked,
        };
        use ForecastField::*;
        use Uncertainty::*;
        // Own runtime unknown wins for the numbers that need it.
        assert_eq!(reason_for(ExecutionMs, f(true, true, true)), NoHistory);
        assert_eq!(reason_for(FinishInMs, f(true, true, true)), NoHistory);
        // For the others an overrun comes first, then the missing history.
        assert_eq!(reason_for(ReadyInMs, f(true, true, true)), Overrun);
        assert_eq!(
            reason_for(QueueWaitAfterReadyMs, f(true, false, true)),
            NoHistory
        );
        assert_eq!(
            reason_for(QueueWaitAfterReadyMs, f(false, true, true)),
            Overrun
        );
        assert_eq!(reason_for(FinishInMs, f(false, true, true)), Overrun);
        assert_eq!(reason_for(FinishInMs, f(false, false, true)), MemoryBlocked);
        assert_eq!(reason_for(ReadyInMs, f(false, false, false)), Conditional);
    }

    #[test]
    fn labels_follow_the_plan_not_the_simulation() {
        let mk = |s: ttc::RuntimeSource| ttc::CommandNode {
            node_id: "n".into(),
            identity: "n".into(),
            label: "n".into(),
            weight: 1,
            predicted_rss: 0,
            runtime: ttc::Runtime {
                secs: None,
                source: s,
                history_count: 0,
                latest_sample: None,
            },
            priority: ttc::Priority::Normal,
            depends_on: vec![],
            ready_in_ms: 0,
            external_delay_ms: 0,
            ledger_entry: None,
            shared_id: None,
        };
        let p = |v: Vec<ttc::RuntimeSource>| ttc::WorkloadPlan {
            attempt_id: "a".into(),
            plan_digest: "d".into(),
            expansion_seq: 0,
            max_in_flight: None,
            nodes: v.into_iter().map(mk).collect(),
        };
        use ttc::RuntimeSource as S;
        assert_eq!(
            answer_source(&p(vec![S::Measured, S::Measured]), &[], &[]),
            RuntimeSource::Measured
        );
        assert_eq!(
            answer_source(&p(vec![S::Measured, S::Fallback]), &[], &[]),
            RuntimeSource::Fallback
        );
        assert_eq!(
            answer_source(&p(vec![S::Fallback, S::Unknown, S::Measured]), &[], &[]),
            RuntimeSource::Unknown
        );
        assert_eq!(
            answer_source(&p(vec![S::Unknown, S::Fallback]), &[], &[]),
            RuntimeSource::Unknown
        );
        // Demand applied ahead of the target counts; demand that was
        // replaced by the ledger, dropped or removed does not.
        let demand = |node: &str, r: ttc::Runtime| ttc::FutureDemand {
            attempt_id: "o".into(),
            plan_digest: "d".into(),
            expansion_seq: 0,
            node_id: node.into(),
            weight: 1,
            predicted_rss: 0,
            runtime: r,
            priority: ttc::Priority::Normal,
            ready_in_ms: 0,
            ledger_entry: None,
            shared_id: None,
            terminal: false,
        };
        let target = p(vec![S::Measured]);
        let overlay = [
            demand("fb", ttc::Runtime::fallback()),
            demand("ms", ttc::Runtime::measured(1.0, 1)),
            demand("un", ttc::Runtime::unknown()),
        ];
        let applied =
            |names: &[&str]| -> Vec<String> { names.iter().map(|n| format!("o/{n}")).collect() };
        assert_eq!(
            answer_source(&target, &overlay, &applied(&["ms"])),
            RuntimeSource::Measured
        );
        assert_eq!(
            answer_source(&target, &overlay, &applied(&["ms", "fb"])),
            RuntimeSource::Fallback
        );
        assert_eq!(
            answer_source(&target, &overlay, &applied(&["fb", "un"])),
            RuntimeSource::Unknown
        );
        assert_eq!(
            answer_source(&target, &overlay, &[]),
            RuntimeSource::Measured,
            "an overlay demand that was not applied is not ahead of the target"
        );
        assert_eq!(confidence_of(ttc::Confidence::High), Confidence::High);
        assert_eq!(confidence_of(ttc::Confidence::Medium), Confidence::Medium);
        assert_eq!(confidence_of(ttc::Confidence::Low), Confidence::Low);
        assert_eq!(confidence_of(ttc::Confidence::None), Confidence::Low);
        for (kind, want) in [
            ("queue", Some(WaitReason::QueueAhead)),
            ("reserved", Some(WaitReason::QueueAhead)),
            ("budget", Some(WaitReason::ConcurrencyLimit)),
            ("cpu", Some(WaitReason::ConcurrencyLimit)),
            ("overload", Some(WaitReason::ConcurrencyLimit)),
            ("memory", Some(WaitReason::MemoryBlocked)),
            ("not_ready", Some(WaitReason::ExternalDelay)),
            ("something else", None),
        ] {
            assert_eq!(wait_reason_of(kind), want, "{kind}");
        }
    }

    fn answer(epoch: u64, boot: &str, ttl: u64) -> ForecastResponse {
        ForecastResponse {
            schema_version: SchemaVersion::CURRENT,
            worker_id: WorkerId::parse("w").unwrap(),
            worker_boot_id: WorkerBootId::parse(boot).unwrap(),
            agent_epoch: AgentEpoch(epoch),
            snapshot_digest: Digest::of_bytes(b"s"),
            policy_digest: Digest::of_bytes(b"p"),
            ttl_ms: ttl,
            coverage: vec![],
            target: ForecastTarget {
                plan_digest: Digest::of_bytes(b"t"),
                ready_in_ms: Some(0),
                queue_wait_after_ready_ms: Some(0),
                execution_ms: Some(0),
                finish_in_ms: Some(0),
                confidence: Confidence::High,
                runtime_source: RuntimeSource::Measured,
                history_count: 0,
                conditional: false,
                overrun: false,
                wait_reason: None,
                assumptions: vec![],
                null_reasons: BTreeMap::new(),
                ranking: None,
            },
        }
    }

    #[test]
    fn a_forecast_expires_with_its_ttl_and_at_once_when_the_agent_or_machine_restarts() {
        let boot = WorkerBootId::parse("b1").unwrap();
        let a = answer(3, "b1", 2000);
        assert_eq!(validity(&a, 0, AgentEpoch(3), &boot), Validity::Fresh);
        assert_eq!(validity(&a, 1999, AgentEpoch(3), &boot), Validity::Fresh);
        assert_eq!(validity(&a, 2000, AgentEpoch(3), &boot), Validity::Expired);
        assert_eq!(
            validity(&a, 60_000, AgentEpoch(3), &boot),
            Validity::Expired
        );
        // A restart is stale however young the answer is.
        assert_eq!(
            validity(&a, 0, AgentEpoch(4), &boot),
            Validity::EpochChanged
        );
        assert_eq!(
            validity(&a, 0, AgentEpoch(2), &boot),
            Validity::EpochChanged
        );
        assert_eq!(
            validity(&a, 0, AgentEpoch(3), &WorkerBootId::parse("b2").unwrap()),
            Validity::BootChanged
        );
        // A reboot also moves the epoch: the reboot is what is reported.
        assert_eq!(
            validity(
                &a,
                99_999,
                AgentEpoch(9),
                &WorkerBootId::parse("b2").unwrap()
            ),
            Validity::BootChanged
        );
        // A zero ttl is never fresh.
        assert_eq!(
            validity(&answer(3, "b1", 0), 0, AgentEpoch(3), &boot),
            Validity::Expired
        );
    }

    fn timing() -> ttc::Timing {
        ttc::Timing {
            ready_in_ms: Some(0),
            start_in_ms: Some(0),
            queue_wait_after_ready_ms: Some(0),
            execution_ms: Some(1),
            finish_in_ms: Some(1),
            null_reasons: BTreeMap::new(),
            ranking: None,
            confidence: ttc::Confidence::High,
            runtime_source: ttc::RuntimeSource::Measured,
            history_count: usize::MAX,
            conditional: false,
            assumptions: vec!["no new external load".into()],
            wait_reason: None,
        }
    }

    fn forecast_of(snapshot: &str, coverage: Vec<ttc::CoverageItem>) -> ttc::Forecast {
        ttc::Forecast {
            schema_version: 1,
            snapshot_digest: snapshot.into(),
            policy_digest: Digest::of_bytes(b"p").to_string(),
            ttl_ms: 2000,
            coverage,
            overlay: ttc::OverlayReport::default(),
            target: ttc::PlanForecast {
                attempt_id: "a".into(),
                plan_digest: Digest::of_bytes(b"t").to_string(),
                expansion_seq: 0,
                timing: timing(),
                critical_path: vec![],
                critical_path_wait_ms: None,
                critical_path_execution_ms: None,
                nodes: vec![],
            },
        }
    }

    fn convert(fc: &ttc::Forecast, notes: &Notes) -> Result<ForecastResponse, ConvertError> {
        convert_on(fc, notes, &insp_of(vec![]))
    }

    fn convert_on(
        fc: &ttc::Forecast,
        notes: &Notes,
        insp: &Inspection,
    ) -> Result<ForecastResponse, ConvertError> {
        let ident = Ident {
            worker_id: WorkerId::parse("w").unwrap(),
            boot_id: WorkerBootId::parse("b").unwrap(),
            agent_epoch: AgentEpoch(1),
            forecast_ttl_ms: 2000,
        };
        let target = ttc::WorkloadPlan {
            attempt_id: "a".into(),
            plan_digest: "d".into(),
            expansion_seq: 0,
            max_in_flight: None,
            nodes: vec![],
        };
        to_wire(
            fc,
            &Wiring {
                ident: &ident,
                plan: &target,
                target_digest: &Digest::of_bytes(b"t"),
                insp,
                overlay: &[],
                notes,
            },
        )
    }

    #[test]
    fn the_conversion_keeps_the_numbers_the_notes_and_the_counts() {
        let a = AttemptId::generate();
        let cover = ttc::CoverageItem {
            attempt_id: a.to_string(),
            plan_digest: Digest::of_bytes(b"c").to_string(),
            expansion_seq: 2,
            node_ids: vec!["x".into(), "y z".into()],
        };
        let mut notes = Notes::default();
        notes.add("extra".into());
        let r = convert(
            &forecast_of(&Digest::of_bytes(b"l").to_string(), vec![cover]),
            &notes,
        )
        .unwrap();
        assert_eq!(r.coverage[0].attempt_id, a);
        assert_eq!(r.coverage[0].expansion_seq, 2);
        assert_eq!(r.coverage[0].node_ids[1].as_str(), "y z");
        assert_eq!(r.target.finish_in_ms, Some(1));
        assert_eq!(r.target.confidence, Confidence::High);
        // The count saturates instead of wrapping.
        assert_eq!(r.target.history_count, u32::MAX);
        // The soft note makes the answer conditional and is listed once
        // after what the simulation said.
        assert!(r.target.conditional);
        assert_eq!(
            r.target.assumptions,
            vec!["no new external load".to_string(), "extra".to_string()]
        );
        assert_eq!(r.ttl_ms, 2000);
        // A measured answer carries no ranking.
        assert_eq!(r.target.ranking, None);
        let plain = convert(
            &forecast_of(&Digest::of_bytes(b"l").to_string(), vec![]),
            &Notes::default(),
        )
        .unwrap();
        assert!(!plain.target.conditional);
        // A note the simulation already made is not repeated.
        let mut same = Notes::default();
        same.assumptions.push("no new external load".into());
        let r = convert(
            &forecast_of(&Digest::of_bytes(b"l").to_string(), vec![]),
            &same,
        )
        .unwrap();
        assert_eq!(r.target.assumptions.len(), 1);
    }

    #[test]
    fn the_fallback_ranking_is_carried_as_it_is_and_never_as_a_time() {
        let mut fc = forecast_of(&Digest::of_bytes(b"l").to_string(), vec![]);
        let t = &mut fc.target.timing;
        t.execution_ms = None;
        t.finish_in_ms = None;
        t.ranking = Some(ttc::Ranking {
            start_in_ms: Some(7),
            finish_in_ms: Some(67),
            note: "fallback".into(),
        });
        let r = convert(&fc, &Notes::default()).unwrap();
        assert_eq!(
            r.target.ranking,
            Some(ForecastRanking {
                start_in_ms: Some(7),
                finish_in_ms: Some(67),
            })
        );
        assert_eq!(r.target.finish_in_ms, None);
        fc.target.timing.ranking = Some(ttc::Ranking {
            start_in_ms: None,
            finish_in_ms: None,
            note: String::new(),
        });
        let r = convert(&fc, &Notes::default()).unwrap();
        assert_eq!(
            r.target.ranking,
            Some(ForecastRanking {
                start_in_ms: None,
                finish_in_ms: None,
            })
        );
    }

    #[test]
    fn overrun_and_memory_block_come_from_the_snapshot_and_the_wait() {
        let overrunning_entry = entry(State::Running, Some(10.0), Some(20), false);
        // The answer has no finish and no start; the wait is as given.
        let missing = |wait: Option<&str>| {
            let mut fc = forecast_of(&Digest::of_bytes(b"l").to_string(), vec![]);
            let t = &mut fc.target.timing;
            t.start_in_ms = None;
            t.queue_wait_after_ready_ms = None;
            t.execution_ms = None;
            t.finish_in_ms = None;
            t.wait_reason = wait.map(|k| ttc::WaitReason {
                kind: k.into(),
                detail: String::new(),
            });
            fc
        };
        let reason = |r: &ForecastResponse| r.target.null_reasons[&ForecastField::FinishInMs];
        // A running entry past its history, and no start: overrun.
        let r = convert_on(
            &missing(None),
            &Notes::default(),
            &insp_of(vec![overrunning_entry.clone()]),
        )
        .unwrap();
        assert!(r.target.overrun);
        assert_eq!(reason(&r), Uncertainty::Overrun);
        // No such entry: not an overrun, whatever else is missing.
        let r = convert_on(&missing(None), &Notes::default(), &insp_of(vec![])).unwrap();
        assert!(!r.target.overrun);
        assert_eq!(reason(&r), Uncertainty::Conditional);
        // A start the simulation could compute: the overrun entry is not why
        // anything is missing.
        let mut fc = missing(None);
        fc.target.timing.start_in_ms = Some(0);
        let r = convert_on(&fc, &Notes::default(), &insp_of(vec![overrunning_entry])).unwrap();
        assert!(!r.target.overrun);
        // The wait kind decides memory-blocked, and only that kind.
        let r = convert_on(
            &missing(Some("memory")),
            &Notes::default(),
            &insp_of(vec![]),
        )
        .unwrap();
        assert_eq!(r.target.wait_reason, Some(WaitReason::MemoryBlocked));
        assert_eq!(reason(&r), Uncertainty::MemoryBlocked);
        let r = convert_on(&missing(Some("queue")), &Notes::default(), &insp_of(vec![])).unwrap();
        assert_eq!(r.target.wait_reason, Some(WaitReason::QueueAhead));
        assert_eq!(reason(&r), Uncertainty::Conditional);
    }

    #[test]
    fn the_ttl_on_the_wire_is_the_workers_not_the_simulations() {
        let mut fc = forecast_of(&Digest::of_bytes(b"l").to_string(), vec![]);
        fc.ttl_ms = 5;
        // `convert` sets the worker's 2000.
        assert_eq!(convert(&fc, &Notes::default()).unwrap().ttl_ms, 2000);
    }

    #[test]
    fn an_id_the_simulation_returns_that_is_not_one_is_an_error() {
        let bad_cover = |attempt: &str, digest: &str, node: &str| ttc::CoverageItem {
            attempt_id: attempt.into(),
            plan_digest: digest.into(),
            expansion_seq: 0,
            node_ids: vec![node.into()],
        };
        let ok_digest = Digest::of_bytes(b"c").to_string();
        let a = AttemptId::generate().to_string();
        let snap = Digest::of_bytes(b"l").to_string();
        for cover in [
            bad_cover("not an attempt", &ok_digest, "x"),
            bad_cover(&a, "not a digest", "x"),
            bad_cover(&a, &ok_digest, ""),
        ] {
            assert!(convert(&forecast_of(&snap, vec![cover]), &Notes::default()).is_err());
        }
        assert!(convert(&forecast_of("not a digest", vec![]), &Notes::default()).is_err());
        let mut fc = forecast_of(&snap, vec![]);
        fc.policy_digest = "not a digest".into();
        assert!(convert(&fc, &Notes::default()).is_err());
    }

    #[test]
    fn an_answer_the_wire_cannot_carry_is_an_error_not_a_guess() {
        let e: ConvertError = Digest::parse("nope").unwrap_err().into();
        assert!(e.0.contains("nope"));
    }
}
