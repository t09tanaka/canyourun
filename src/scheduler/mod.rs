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
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The reason recorded for a retire the coordinator starts because the
/// worker keeps refusing the attempt for a reason of its own. Once it is
/// confirmed the task is free again, and that worker is left out for it.
pub const REASON_WORKER_CANNOT_RUN: &str = "worker_cannot_run";

/// The event on an attempt that marks its worker as one the task is not
/// placed on again. Payload: `{"worker", "code"}`.
pub const EVENT_WORKER_EXCLUDED: &str = "worker_excluded";

/// The event on a run for a task no worker could be chosen for. Payload:
/// `{"task", "decision"}`; written again only when the reason changes.
pub const EVENT_TASK_WAITING: &str = "task_waiting";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub rules: Rules,
    /// How long one decision waits for a worker's forecast. A worker that
    /// takes longer is left out for the rest of the tick. Keep it below
    /// `rules.forecast_ttl_ms`: the answers of the quick workers must still
    /// be fresh when the slowest one is given up.
    pub forecast_wait_ms: u64,
}

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
    /// The last reason recorded for each task that waits, so that a task
    /// that waits for the same reason is recorded once.
    waiting: Mutex<BTreeMap<TaskId, Decision>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler::new(SchedulerConfig::default(), Arc::new(UnknownCosts))
    }
}

impl Scheduler {
    pub fn new(cfg: SchedulerConfig, costs: Arc<dyn CostModel>) -> Scheduler {
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

    pub fn set_priority(&self, worker_id: &WorkerId, priority: i64) {
        let mut all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.insert(worker_id.clone(), priority);
    }

    /// The configured priority; 0 for a worker that was given none.
    pub fn priority_of(&self, worker_id: &WorkerId) -> i64 {
        let all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.get(worker_id).copied().unwrap_or(0)
    }

    /// The number of the next decision of this process, counting from 1.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Remember why a task waits. Returns whether this is news (the first
    /// reason, or another one than before).
    pub fn note_waiting(&self, task_id: &TaskId, decision: &Decision) -> bool {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        if all.get(task_id) == Some(decision) {
            return false;
        }
        all.insert(task_id.clone(), decision.clone());
        true
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
/// that made it.
pub fn decision_seq_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<u64>> {
    let demand = store.placement_demand(attempt_id)?;
    Ok(demand.and_then(|d| d.get("decision_seq").and_then(|s| s.as_u64())))
}
