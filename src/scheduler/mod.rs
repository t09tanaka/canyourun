//! Placement decisions.
//!
//! [`decide`] holds the judgement (pure). This module holds what the
//! coordinator's actor needs around it: the settings, the estimates the
//! parent itself brings to a comparison, the forecast question for a task,
//! and the record of each decision.
//!
//! One actor decides: the coordinator calls into here only while it holds
//! its actor lock, so two decisions never see the same free slot. The
//! forecast questions of one decision go out side by side; the commit that
//! follows is one transaction, which counts the worker's load again.

pub mod decide;

use crate::forecast::plan::MAX_PENDING_PLANS;
use crate::ids::{AttemptId, PlanDigest, TaskId, WorkerId};
use crate::storage::{CoordinatorStore, Placement, QueuedTask};
use crate::wire::{
    AttemptPayload, CommandSpec, ConcurrencyLimits, Correlation, WorkloadNode, WorkloadPlan,
};
use decide::{Costs, Decision, Rules};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The reason recorded for a retire the coordinator starts because the
/// worker keeps refusing the attempt for a reason of its own. Once it is
/// confirmed the task is free again, and that worker is left out for it.
pub const REASON_WORKER_CANNOT_RUN: &str = "worker_cannot_run";

/// The event on an attempt that says why its worker is one the task is not
/// placed on again. Payload: `{"worker", "code", "reason"}`. It is written
/// with the tombstone, in one transaction; what excludes the worker is the
/// tombstone's reason ([`REASON_WORKER_CANNOT_RUN`]), not this event.
pub const EVENT_WORKER_EXCLUDED: &str = "worker_excluded";

/// The event on a run for a task no worker could be chosen for. Payload:
/// `{"task", "decision"}`; written again only when the outcome or a lasting
/// exclusion changes ([`decide::same_wait`]).
pub const EVENT_TASK_WAITING: &str = "task_waiting";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub rules: Rules,
    /// How long one decision waits for a worker's forecast. A worker that
    /// takes longer is left out for the rest of the tick. It must be below
    /// `rules.forecast_ttl_ms` ([`SchedulerConfig::check`]): the answers of
    /// the quick workers must still be fresh when the slowest one is given
    /// up. Not a key of the configuration file: the default is used.
    pub forecast_wait_ms: u64,
}

impl SchedulerConfig {
    /// Settings no decision could be made with are refused: a wait as long
    /// as an answer lives would make every answer too old to use.
    pub fn check(&self) -> Result<(), String> {
        if self.forecast_wait_ms < self.rules.forecast_ttl_ms {
            Ok(())
        } else {
            Err(format!(
                "forecast_wait_ms ({}) must be below forecast_ttl_ms ({})",
                self.forecast_wait_ms, self.rules.forecast_ttl_ms
            ))
        }
    }
}

/// The range of a worker's configured priority (spec chapter 9).
pub const PRIORITY_MIN: i64 = -100;
pub const PRIORITY_MAX: i64 = 100;

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            rules: Rules {
                priority_step_ms: 60_000,
                tie_window_ms: 1000,
                forecast_ttl_ms: 2000,
            },
            forecast_wait_ms: 1000,
        }
    }
}

/// What the parent itself estimates for a task on a worker: transfer,
/// setup, collection, cache state, affinity. The worker's forecast covers
/// the queue and the run.
pub trait CostModel: Send + Sync {
    fn costs(&self, worker_id: &WorkerId, task: &QueuedTask) -> Costs;
}

/// Nothing is known: every part is unknown for every worker, which leaves
/// the comparison to the workers' forecasts.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnknownCosts;

impl CostModel for UnknownCosts {
    fn costs(&self, _worker_id: &WorkerId, _task: &QueuedTask) -> Costs {
        Costs::default()
    }
}

/// The scheduler's state inside a coordinator.
pub struct Scheduler {
    pub cfg: SchedulerConfig,
    costs: Arc<dyn CostModel>,
    priorities: Mutex<BTreeMap<WorkerId, i64>>,
    seq: AtomicU64,
    /// The last decision for each task that waits, so that a task that
    /// waits for the same reason is recorded once.
    waiting: Mutex<BTreeMap<TaskId, Decision>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler::unchecked(SchedulerConfig::default(), Arc::new(UnknownCosts))
    }
}

impl Scheduler {
    /// Fails for settings [`SchedulerConfig::check`] refuses.
    pub fn new(cfg: SchedulerConfig, costs: Arc<dyn CostModel>) -> Result<Scheduler, String> {
        cfg.check()?;
        Ok(Scheduler::unchecked(cfg, costs))
    }

    fn unchecked(cfg: SchedulerConfig, costs: Arc<dyn CostModel>) -> Scheduler {
        Scheduler {
            cfg,
            costs,
            priorities: Mutex::default(),
            seq: AtomicU64::new(0),
            waiting: Mutex::default(),
        }
    }

    pub fn costs(&self, worker_id: &WorkerId, task: &QueuedTask) -> Costs {
        self.costs.costs(worker_id, task)
    }

    /// A priority outside the range is taken as the nearest end of it.
    pub fn set_priority(&self, worker_id: &WorkerId, priority: i64) {
        let mut all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.insert(
            worker_id.clone(),
            priority.clamp(PRIORITY_MIN, PRIORITY_MAX),
        );
    }

    /// The configured priority; 0 for a worker that was given none.
    pub fn priority_of(&self, worker_id: &WorkerId) -> i64 {
        let all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.get(worker_id).copied().unwrap_or(0)
    }

    /// The number of the next decision of this process, counting from 1. It
    /// is local to the process: a restarted coordinator counts from 1 again,
    /// and a decision whose commit placed nothing uses one up. It orders the
    /// decisions of one process in a record; it identifies nothing in the
    /// database (the placement and the attempt have their own ids).
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Remember why a task waits. Returns whether this is news: the first
    /// reason, or another one than before. What differs only in what one
    /// tick saw (a worker that was full or silent) is kept, and is not news.
    pub fn note_waiting(&self, task_id: &TaskId, decision: &Decision) -> bool {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        let news = all
            .get(task_id)
            .is_none_or(|before| !decide::same_wait(before, decision));
        all.insert(task_id.clone(), decision.clone());
        news
    }

    /// Forget every waiting task that is not among `queued`: it was
    /// cancelled, its run was closed, or it went another way.
    pub fn keep_waiting(&self, queued: &BTreeSet<TaskId>) {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.retain(|task_id, _| queued.contains(task_id));
    }

    /// The task was placed (or is gone): it waits no more.
    pub fn clear_waiting(&self, task_id: &TaskId) {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.remove(task_id);
    }

    /// Why the task was not placed when it was last looked at, if it was
    /// looked at by this process and not placed since.
    pub fn waiting_reason(&self, task_id: &TaskId) -> Option<Decision> {
        let all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.get(task_id).cloned()
    }
}

/// The plan a worker is asked to forecast for an attempt: its commands, with
/// what the parent knows about the time before it is ready (transfer and
/// setup) on the commands that wait for no other. The parent has no runtime
/// history of its own, so the runtimes are left to the worker.
pub fn workload_of(
    attempt_id: &AttemptId,
    plan_digest: &PlanDigest,
    commands: &[CommandSpec],
    ready_delay_ms: Option<u64>,
) -> WorkloadPlan {
    let nodes = commands
        .iter()
        .map(|c| WorkloadNode {
            node_id: c.node_id.clone(),
            depends_on: c.depends_on.clone(),
            priority: 0,
            weight: None,
            predicted_rss_bytes: None,
            predicted_runtime_ms: None,
            external_delay_ms: ready_delay_ms.filter(|_| c.depends_on.is_empty()),
            ready_in_ms: None,
            concurrency: ConcurrencyLimits::default(),
            correlation: Correlation {
                attempt_id: attempt_id.clone(),
                node_id: c.node_id.clone(),
                ledger_entry: None,
            },
            shared_builder: None,
        })
        .collect();
    WorkloadPlan {
        attempt_id: attempt_id.clone(),
        plan_digest: plan_digest.clone(),
        expansion_seq: 0,
        nodes,
    }
}

/// The work the parent placed on a worker that the worker has not
/// acknowledged: it is not in the worker's own state yet, so the forecast
/// question carries it (spec chapter 9, step 6). The oldest first, and no
/// more than a worker accepts in one question.
pub fn pending_of(unacked: &[Placement], worker_id: &WorkerId) -> Vec<WorkloadPlan> {
    unacked
        .iter()
        .filter(|p| &p.worker_id == worker_id)
        .filter_map(|p| {
            let attempt_id = p.attempt_id.as_ref()?;
            let payload: AttemptPayload =
                serde_json::from_value(p.demand.get("payload")?.clone()).ok()?;
            Some(workload_of(
                attempt_id,
                &payload.plan_digest,
                &payload.commands,
                None,
            ))
        })
        .take(MAX_PENDING_PLANS)
        .collect()
}

/// The decision that placed an attempt, as it was committed with the
/// placement. `None` for an attempt that was placed before decisions were
/// recorded, or whose record is gone.
pub fn reason_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<Decision>> {
    let demand = store.placement_demand(attempt_id)?;
    Ok(demand
        .and_then(|d| d.get("decision").cloned())
        .and_then(|d| serde_json::from_value(d).ok()))
}

/// The number of the decision that placed an attempt, within the process
/// that made it ([`Scheduler::next_seq`]): not unique across restarts.
pub fn decision_seq_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<u64>> {
    let demand = store.placement_demand(attempt_id)?;
    Ok(demand.and_then(|d| d.get("decision_seq").and_then(|s| s.as_u64())))
}
