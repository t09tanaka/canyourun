//! Parent process: acceptance, placement and state.
//!
//! The [`Coordinator`] owns the durable queue (runs, tasks, attempts) in the
//! parent database and talks to workers through [`WorkerLink`]s. One
//! [`Coordinator::tick`] at a time is the single actor that commits
//! placements (spec chapter 9). A tick first looks at every attempt that
//! still needs attention on each attached worker, then places queued tasks.
//!
//! The workers are looked at side by side, each within the same time budget
//! ([`CoordinatorConfig::worker_budget_ms`]): a worker that stalls costs the
//! tick that budget once, not one timeout per attempt, so the leases on the
//! other workers are renewed in time. Placing stays one step after the
//! other, by this one actor.
//!
//! Every step is decided from what is durable in the parent database and
//! what the worker answers now ([`policy`]), so a tick can be repeated after
//! a crash at any point: a placement that was committed and never sent, a
//! submit whose acknowledgement was lost and a retire whose answer was lost
//! are all picked up by the next tick, in this process or the next.
//!
//! Which worker gets a task is the simplest deterministic choice here (the
//! first attached worker, by id, that has room); choosing by predicted
//! completion belongs to the scheduler.

mod collect;
mod link;
pub mod policy;

pub use link::{ClientLink, LinkError, WorkerLink};

use crate::error::{ApiError, ConflictReason, ErrorCode};
use crate::ids::{
    AttemptId, CoordinatorEpoch, Generation, ManifestDigest, PlanDigest, RequestId, RunId,
    SourceId, TaskId, WorkerId,
};
use crate::platform::wall_ms;
use crate::state::{AttemptState, Observation, Outcome, Termination};
use crate::storage::{
    AttemptRecord, CoordinatorStore, LiveAttempt, PlaceInput, PlaceOutcome, QueuedTask, RetireEnd,
    Seen, StorageError,
};
use crate::wire::{
    payload_hash, ArtifactDecl, AttemptPayload, AttemptStatus, CancelAttemptRequest, CommandSpec,
    GetAttemptRequest, InspectRequest, InspectResponse, RenewLeaseRequest, Request, Response,
    RetireAttemptRequest, RetireStatus, SchemaVersion, SubmitAttemptRequest,
};
use link::Budgeted;
use policy::{Answer, Ended, Facts, Limits, Refusal, RetireVerdict, Step};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// The reason recorded for a retire the coordinator starts by itself after a
/// worker refused the attempt for good. Such an attempt is failed, not
/// placed again, once the retire is confirmed.
pub const REASON_SUBMIT_REFUSED: &str = "submit_refused";

/// The most queued tasks one tick looks at.
pub const DISPATCH_SCAN: u32 = 256;

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A worker has seen a newer coordinator epoch: this process is not the
    /// coordinator any more. It places and controls nothing from here on.
    #[error("a newer coordinator took over; this one (epoch {0}) stopped")]
    Fenced(CoordinatorEpoch),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("a storage call was aborted")]
    Aborted,
    /// The parent's result store could not be read or written.
    #[error("result store: {0}")]
    Results(String),
}

type Result<T, E = CoordinatorError> = std::result::Result<T, E>;

/// What a task runs, as it is queued. The payload of each attempt is built
/// from this by [`build_payload`] and nowhere else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub plan_digest: PlanDigest,
    pub manifest_digest: ManifestDigest,
    pub commands: Vec<CommandSpec>,
    #[serde(default)]
    pub command_timeout_ms: Option<u64>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub resource_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    pub has_secret_bundle: bool,
    /// The files to keep as artifacts; none when the task declares none (and
    /// then not written, like in the payload).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactDecl>,
}

/// The one place an [`AttemptPayload`] is made.
pub fn build_payload(
    input: &PlaceInput,
    spec: &TaskSpec,
    lease_ms: u64,
) -> Result<AttemptPayload, StorageError> {
    let source_id = input
        .source_id
        .clone()
        .ok_or_else(|| StorageError::Invalid(format!("run {} has no source", input.run_id)))?;
    Ok(AttemptPayload {
        run_id: input.run_id.clone(),
        task_id: input.task_id.clone(),
        generation: input.generation,
        source_id,
        plan_digest: spec.plan_digest.clone(),
        manifest_digest: spec.manifest_digest.clone(),
        permit_id: None,
        commands: spec.commands.clone(),
        lease_ms,
        command_timeout_ms: spec.command_timeout_ms,
        deadline_ms: spec.deadline_ms,
        resource_wait_timeout_ms: spec.resource_wait_timeout_ms,
        has_secret_bundle: spec.has_secret_bundle,
        artifacts: spec.artifacts.clone(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    /// The coordinator's limits per worker; a worker's own stricter limits
    /// (from `inspect`) win.
    pub limits: Limits,
    /// The execution lease granted with each submit and renewal.
    pub lease_ms: u64,
    /// How long one tick waits for one worker, over all its attempts; then
    /// again for the submits of that tick, over all workers. What was not
    /// answered by then is `unknown` and is asked again by the next tick.
    /// Keep twice this well below `lease_ms`: a tick takes no longer, and
    /// every tick renews the leases.
    pub worker_budget_ms: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        CoordinatorConfig {
            limits: Limits {
                max_active_task: 2,
                prefetch_per_worker: 1,
            },
            lease_ms: 60_000,
            worker_budget_ms: 15_000,
        }
    }
}

/// What one tick did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Attempts placed (and sent) by this tick.
    pub placed: Vec<AttemptId>,
    /// Attempts left `unknown` because their worker gave no answer.
    pub unknown: Vec<AttemptId>,
    /// Attempts whose retire was confirmed: their tasks are queued again.
    pub retired: Vec<AttemptId>,
    /// Attempts that became terminal on the parent.
    pub settled: Vec<AttemptId>,
}

/// A worker that answered `inspect` in this tick.
struct Reached {
    worker_id: WorkerId,
    link: Arc<dyn WorkerLink>,
    limits: Limits,
    draining: bool,
}

/// What became of an attempt that was just placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// The worker holds it.
    Taken,
    /// The worker did not take it (now).
    NotTaken,
    /// It was not sent: a cancel or a retire for it was recorded first.
    Withheld,
}

type Followed = Result<(Option<Reached>, TickReport)>;

/// Drive every future to its end, side by side on this task.
async fn join_all<'a, T>(mut all: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>) -> Vec<T> {
    let mut done: Vec<Option<T>> = all.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, slot) in all.iter_mut().zip(done.iter_mut()) {
            if slot.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(out) => *slot = Some(out),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    done.into_iter().flatten().collect()
}

pub struct Coordinator {
    store: Arc<CoordinatorStore>,
    cfg: CoordinatorConfig,
    workers: Mutex<BTreeMap<WorkerId, Arc<dyn WorkerLink>>>,
    actor: tokio::sync::Mutex<()>,
    /// One pass of result collection at a time.
    collecting: tokio::sync::Mutex<()>,
    lease_seq: AtomicU64,
    fenced: AtomicBool,
    /// The queued tasks no payload could be made for, as this process
    /// already recorded.
    unplaceable: Mutex<BTreeSet<TaskId>>,
}

fn cancelled() -> Termination {
    Termination::new(Outcome::Cancelled, None, None).expect("cancelled without an exit is valid")
}

fn seen_of(status: &AttemptStatus) -> Seen {
    Seen {
        state: status.state,
        termination: status.termination,
        receipts: status.receipts,
    }
}

impl Coordinator {
    /// Take over the state in `store` (opening it started a new coordinator
    /// epoch). Nothing is known first-hand after a start: every attempt that
    /// still needs attention is `reconciling` until its worker was asked.
    pub fn start(store: CoordinatorStore, cfg: CoordinatorConfig) -> Result<Coordinator> {
        store.mark_reconciling()?;
        Ok(Coordinator {
            store: Arc::new(store),
            cfg,
            workers: Mutex::default(),
            actor: tokio::sync::Mutex::new(()),
            collecting: tokio::sync::Mutex::new(()),
            lease_seq: AtomicU64::new(0),
            fenced: AtomicBool::new(false),
            unplaceable: Mutex::default(),
        })
    }

    pub fn store(&self) -> &CoordinatorStore {
        &self.store
    }

    pub fn epoch(&self) -> CoordinatorEpoch {
        self.store.epoch()
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    async fn db<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&CoordinatorStore) -> crate::storage::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || f(&store)).await {
            Ok(out) => Ok(out?),
            Err(_) => Err(CoordinatorError::Aborted),
        }
    }

    fn fence(&self) -> CoordinatorError {
        self.fenced.store(true, Ordering::SeqCst);
        CoordinatorError::Fenced(self.epoch())
    }

    fn check_fenced(&self) -> Result<()> {
        if self.is_fenced() {
            Err(CoordinatorError::Fenced(self.epoch()))
        } else {
            Ok(())
        }
    }

    /// A worker is reachable through `link` from now on (its `hello` was
    /// checked by whoever connected). Replaces an earlier link.
    pub fn attach(&self, worker_id: WorkerId, link: Arc<dyn WorkerLink>) {
        let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        workers.insert(worker_id, link);
    }

    /// The link is gone. The worker's attempts are `unknown` from here on;
    /// they are kept, and followed again when it is attached again.
    pub async fn detach(&self, worker_id: &WorkerId) -> Result<()> {
        {
            let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers.remove(worker_id);
        }
        let _actor = self.actor.lock().await;
        let w = worker_id.clone();
        let live = self.db(move |s| s.live_attempts(Some(&w))).await?;
        for l in live {
            self.observe(&l.record.attempt_id, Observation::Unknown)
                .await?;
        }
        Ok(())
    }

    /// Accept a run and its tasks durably (`coordinator accepted`).
    pub async fn enqueue(
        &self,
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    ) -> Result<()> {
        self.check_fenced()?;
        let mut specs = Vec::with_capacity(tasks.len());
        for (task, spec) in tasks {
            if spec.commands.is_empty() {
                return Err(CoordinatorError::Invalid(format!(
                    "task {task} has no command"
                )));
            }
            let value = serde_json::to_value(&spec)
                .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
            check_spec(&run_id, &source_id, &task, &value, self.cfg.lease_ms)?;
            specs.push((task, value));
        }
        self.db(move |s| s.enqueue(&run_id, Some(&source_id), &json!({}), &specs, wall_ms()))
            .await
    }

    /// Cancel a task: durable before anything is sent. A queued task is
    /// never placed; a placed one gets the cancel on the next tick, and on
    /// every later one until its worker answered.
    pub async fn cancel_task(&self, task_id: &TaskId) -> Result<Option<AttemptRecord>> {
        let task = task_id.clone();
        self.db(move |s| s.cancel_task(&task, wall_ms())).await
    }

    /// Ask for an attempt to be taken off its worker so that its task can be
    /// placed again. The tombstone is durable here first; the task is free
    /// only after the worker confirmed that the attempt never started or has
    /// stopped.
    pub async fn request_retire(&self, attempt_id: &AttemptId, reason: &str) -> Result<()> {
        let (a, reason) = (attempt_id.clone(), reason.to_string());
        self.db(move |s| s.begin_retire(&a, &reason, wall_ms()))
            .await
    }

    /// One pass of the actor: follow every attempt on every attached worker,
    /// then place queued tasks. Fails only for a storage fault or when this
    /// coordinator was fenced; a worker that does not answer is not an error.
    pub async fn tick(&self) -> Result<TickReport> {
        let _actor = self.actor.lock().await;
        self.check_fenced()?;
        let mut report = TickReport::default();
        let attached: Vec<(WorkerId, Arc<dyn WorkerLink>)> = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers
                .iter()
                .map(|(w, l)| (w.clone(), Arc::clone(l)))
                .collect()
        };
        let deadline = self.deadline();
        let follows: Vec<Pin<Box<dyn Future<Output = Followed> + Send + '_>>> = attached
            .into_iter()
            .map(|(worker_id, link)| {
                Box::pin(self.follow(worker_id, link, deadline))
                    as Pin<Box<dyn Future<Output = Followed> + Send + '_>>
            })
            .collect();
        let mut reached = Vec::new();
        let mut failed = None;
        // In the order of the worker ids, whichever answered first.
        for followed in join_all(follows).await {
            match followed {
                Ok((worker, part)) => {
                    report.placed.extend(part.placed);
                    report.unknown.extend(part.unknown);
                    report.retired.extend(part.retired);
                    report.settled.extend(part.settled);
                    reached.extend(worker);
                }
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }
        self.dispatch(reached, &mut report).await?;
        Ok(report)
    }

    fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.cfg.worker_budget_ms)
    }

    /// Look at one worker and at every attempt on it that needs attention.
    /// Nothing waits for the worker past `deadline`.
    async fn follow(
        &self,
        worker_id: WorkerId,
        link: Arc<dyn WorkerLink>,
        deadline: Instant,
    ) -> Followed {
        let budgeted = Budgeted {
            inner: Arc::clone(&link),
            deadline,
        };
        let mut report = TickReport::default();
        let inspected = self.inspect(&worker_id, &budgeted).await;
        let w = worker_id.clone();
        let live = self.db(move |s| s.live_attempts(Some(&w))).await?;
        let Some(inspected) = inspected else {
            for l in live {
                self.unknown(&l.record.attempt_id, &mut report).await?;
            }
            return Ok((None, report));
        };
        for l in live {
            // Another worker may have said that this coordinator is replaced.
            self.check_fenced()?;
            self.drive(&budgeted, l, &mut report).await?;
        }
        if Instant::now() >= deadline {
            // It used up its time: nothing new goes there in this tick.
            return Ok((None, report));
        }
        let reached = Reached {
            worker_id,
            link,
            limits: self.cfg.limits.stricter(Limits {
                max_active_task: inspected.max_active_task,
                prefetch_per_worker: inspected.prefetch_per_worker,
            }),
            draining: inspected.draining,
        };
        Ok((Some(reached), report))
    }

    async fn inspect(
        &self,
        worker_id: &WorkerId,
        link: &dyn WorkerLink,
    ) -> Option<InspectResponse> {
        let request = Request::Inspect(InspectRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
        });
        match link.call(request).await {
            // Another identity behind the link is not this worker.
            Ok(Response::Inspect(r)) if &r.worker_id == worker_id => Some(r),
            _ => None,
        }
    }

    async fn observe(&self, attempt_id: &AttemptId, o: Observation) -> Result<AttemptRecord> {
        let a = attempt_id.clone();
        self.db(move |s| s.record_seen(&a, false, None, o, wall_ms()))
            .await
    }

    async fn note(&self, attempt_id: &AttemptId, kind: &'static str, payload: Value) -> Result<()> {
        let a = attempt_id.clone();
        self.db(move |s| {
            // Once per attempt: a stuck attempt is looked at on every tick.
            if s.events_of(&a, kind)?.is_empty() {
                s.append_event(None, Some(&a), kind, &payload, wall_ms())?;
            }
            Ok(())
        })
        .await
    }

    /// Record why a worker did not take an attempt, once per error code, so
    /// that an attempt which waits on its worker shows what it waits for.
    async fn note_refusal(
        &self,
        attempt_id: &AttemptId,
        kind: &'static str,
        e: &ApiError,
    ) -> Result<()> {
        let a = attempt_id.clone();
        let code = e.code.to_string();
        let payload = json!({"code": code, "message": e.message});
        self.db(move |s| {
            s.append_events_unseen(None, &a, kind, "code", &[(code, payload)], wall_ms())
        })
        .await?;
        Ok(())
    }

    /// Ask the worker for the attempt. A status that is not about this
    /// attempt and generation, or contradicts itself, is no answer.
    async fn ask(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
    ) -> (Answer, Option<AttemptStatus>) {
        let request = Request::GetAttempt(GetAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.attempt_id.clone(),
        });
        match link.call(request).await {
            Ok(Response::GetAttempt(s))
                if s.attempt_id == a.attempt_id
                    && s.generation == a.generation
                    && s.task_id == a.task_id
                    && s.check().is_ok() =>
            {
                (Answer::Status(s.state), Some(s))
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::NotFound => (Answer::NotFound, None),
            Err(LinkError::Remote(e)) if e.code == ErrorCode::Retired => (Answer::Tombstone, None),
            _ => (Answer::Unreachable, None),
        }
    }

    /// The worker holds the attempt: acknowledge its demand and mirror what
    /// it reported.
    async fn mirror(&self, status: &AttemptStatus, report: &mut TickReport) -> Result<()> {
        let (a, seen) = (status.attempt_id.clone(), seen_of(status));
        let rec = self
            .db(move |s| s.record_seen(&a, true, Some(seen), Observation::Known, wall_ms()))
            .await?;
        if rec.state == AttemptState::Terminal {
            report.settled.push(rec.attempt_id);
        }
        Ok(())
    }

    /// The parent fixes the result of an attempt no worker will ever report.
    async fn settle_here(
        &self,
        attempt_id: &AttemptId,
        termination: Termination,
        report: &mut TickReport,
    ) -> Result<()> {
        let a = attempt_id.clone();
        let seen = Seen {
            state: AttemptState::Terminal,
            termination: Some(termination),
            receipts: Default::default(),
        };
        self.db(move |s| s.record_seen(&a, false, Some(seen), Observation::Known, wall_ms()))
            .await?;
        report.settled.push(attempt_id.clone());
        Ok(())
    }

    async fn unknown(&self, attempt_id: &AttemptId, report: &mut TickReport) -> Result<()> {
        self.observe(attempt_id, Observation::Unknown).await?;
        report.unknown.push(attempt_id.clone());
        Ok(())
    }

    /// One look at one attempt, and the one step that follows from it.
    async fn drive(
        &self,
        link: &dyn WorkerLink,
        live: LiveAttempt,
        report: &mut TickReport,
    ) -> Result<()> {
        let a = &live.record;
        let facts = Facts {
            acked: live
                .placement
                .as_ref()
                .is_none_or(|p| p.acked_at_ms.is_some()),
            cancel_requested: a.cancel_requested_at_ms.is_some(),
            retire_requested: live.retire_reason.is_some(),
        };
        let (answer, status) = self.ask(link, a).await;
        match (policy::next_step(facts, answer), status) {
            (Step::KeepUnknown, _) => self.unknown(&a.attempt_id, report).await,
            (Step::Retire, _) => self.retire(link, &live, report).await,
            (Step::Cancel, status) => self.cancel(link, a, status, report).await,
            (Step::SettleCancelled, _) => {
                self.settle_here(&a.attempt_id, cancelled(), report).await
            }
            (Step::Submit, _) => self.submit(link, &live, report).await.map(|_| ()),
            (Step::Settle, Some(status)) => self.mirror(&status, report).await,
            (Step::Follow, Some(status)) => {
                self.mirror(&status, report).await?;
                self.renew(link, a, status.state, report).await
            }
            // A step that needs a status always comes with one; without it
            // nothing is known.
            (Step::Settle | Step::Follow, None) => self.unknown(&a.attempt_id, report).await,
        }
    }

    /// Send the attempt as it was recorded when it was placed: the stored
    /// payload, which must still hash to the stored hash. Returns whether
    /// the worker holds the attempt now.
    async fn submit(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        report: &mut TickReport,
    ) -> Result<bool> {
        let a = &live.record;
        let payload: Option<AttemptPayload> = live
            .placement
            .as_ref()
            .and_then(|p| serde_json::from_value(p.demand["payload"].clone()).ok());
        let request = payload.and_then(|p| {
            SubmitAttemptRequest::new(RequestId::generate(), self.epoch(), a.attempt_id.clone(), p)
                .ok()
        });
        let Some(request) = request.filter(|r| r.payload_hash == a.payload_hash) else {
            // Fail closed: what would be sent is not what was decided.
            self.note(&a.attempt_id, "submit_payload_mismatch", Value::Null)
                .await?;
            self.unknown(&a.attempt_id, report).await?;
            return Ok(false);
        };
        match link.call(Request::SubmitAttempt(request)).await {
            Ok(Response::SubmitAttempt(r))
                if r.attempt_id == a.attempt_id && r.generation == a.generation =>
            {
                // A resend may find the attempt terminal already; its result
                // comes with the next look, not with this answer.
                let seen = (r.state != AttemptState::Terminal).then_some(Seen {
                    state: r.state,
                    termination: None,
                    receipts: r.receipts,
                });
                let id = a.attempt_id.clone();
                self.db(move |s| s.record_seen(&id, true, seen, Observation::Known, wall_ms()))
                    .await?;
                Ok(true)
            }
            Err(LinkError::Remote(e)) => {
                let mismatch = e.conflict_reason == Some(ConflictReason::PayloadMismatch);
                match policy::submit_refusal(e.code, mismatch) {
                    Refusal::Fenced => Err(self.fence()),
                    Refusal::Tombstoned => {
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(false)
                    }
                    Refusal::Later => {
                        self.note_refusal(&a.attempt_id, "submit_deferred", &e)
                            .await?;
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(false)
                    }
                    Refusal::Never => {
                        self.note_refusal(&a.attempt_id, "submit_refused", &e)
                            .await?;
                        self.request_retire(&a.attempt_id, REASON_SUBMIT_REFUSED)
                            .await?;
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(false)
                    }
                }
            }
            // Sent or not, accepted or not: nothing is known.
            _ => {
                self.unknown(&a.attempt_id, report).await?;
                Ok(false)
            }
        }
    }

    async fn cancel(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        status: Option<AttemptStatus>,
        report: &mut TickReport,
    ) -> Result<()> {
        if let Some(status) = &status {
            self.mirror(status, report).await?;
        }
        let request = Request::CancelAttempt(CancelAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
            reason: None,
        });
        match link.call(request).await {
            Ok(Response::CancelAttempt(_)) => {
                self.observe(&a.attempt_id, Observation::Known).await?;
                Ok(())
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => Err(self.fence()),
            Err(LinkError::Remote(e)) => {
                let payload = json!({"code": e.code.to_string()});
                self.note(&a.attempt_id, "cancel_refused", payload).await
            }
            _ => self.unknown(&a.attempt_id, report).await,
        }
    }

    async fn retire(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        report: &mut TickReport,
    ) -> Result<()> {
        let a = &live.record;
        let request = Request::RetireAttempt(RetireAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
        });
        let status = match link.call(request).await {
            Ok(Response::RetireAttempt(r)) if r.attempt_id == a.attempt_id && r.tombstone => {
                r.status
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => {
                return Err(self.fence())
            }
            Err(LinkError::Remote(e)) => {
                let payload = json!({"code": e.code.to_string()});
                return self.note(&a.attempt_id, "retire_refused", payload).await;
            }
            _ => return self.unknown(&a.attempt_id, report).await,
        };
        // A worker that says the attempt is over is asked how it ended: the
        // word of the answer alone never frees the task.
        let (ended, seen) = if status == RetireStatus::Unconfirmed {
            (Ended::Unknown, None)
        } else {
            self.ended(link, a).await
        };
        match (policy::retire_verdict(status, ended), seen) {
            (RetireVerdict::Confirmed, _) => {
                // What the confirmation means (free the task, or settle the
                // attempt because the task was cancelled or refused) is read
                // in the commit: a cancel may have come since `live` was read.
                let id = a.attempt_id.clone();
                let end = self
                    .db(move |s| s.finish_retire(&id, REASON_SUBMIT_REFUSED, wall_ms()))
                    .await?;
                match end {
                    RetireEnd::Freed => report.retired.push(a.attempt_id.clone()),
                    RetireEnd::Settled(_) => report.settled.push(a.attempt_id.clone()),
                }
                Ok(())
            }
            (RetireVerdict::Adopt, Some(status)) => self.mirror(&status, report).await,
            (RetireVerdict::Wait, _) => {
                self.observe(&a.attempt_id, Observation::Known).await?;
                Ok(())
            }
            (RetireVerdict::Unknown, _) | (RetireVerdict::Adopt, None) => {
                self.unknown(&a.attempt_id, report).await
            }
        }
    }

    /// How a retired attempt stands on its worker now.
    async fn ended(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
    ) -> (Ended, Option<AttemptStatus>) {
        match self.ask(link, a).await {
            (Answer::Tombstone, _) => (Ended::Never, None),
            (Answer::Status(AttemptState::Terminal), Some(status)) if status.retired => {
                match status.termination {
                    Some(t) => (Ended::With(t.outcome()), Some(status)),
                    None => (Ended::Unknown, None),
                }
            }
            _ => (Ended::Unknown, None),
        }
    }

    async fn renew(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        state: AttemptState,
        report: &mut TickReport,
    ) -> Result<()> {
        if matches!(state, AttemptState::Cancelling | AttemptState::Finalizing) {
            return Ok(());
        }
        let request = Request::RenewLease(RenewLeaseRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
            lease_seq: self.lease_seq.fetch_add(1, Ordering::SeqCst) + 1,
            lease_ms: self.cfg.lease_ms,
        });
        match link.call(request).await {
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => Err(self.fence()),
            // Renewed, or not renewable any more: the next look says why.
            Ok(_) | Err(LinkError::Remote(_)) => Ok(()),
            Err(LinkError::Unconfirmed) => self.unknown(&a.attempt_id, report).await,
        }
    }

    /// Place queued tasks, oldest first, each on the first worker (by id)
    /// that answered in this tick, is not draining and has room. The commit
    /// counts the worker's load again; the submit follows the commit, and a
    /// worker that does not take it is left alone for the rest of the tick.
    async fn dispatch(&self, reached: Vec<Reached>, report: &mut TickReport) -> Result<()> {
        let deadline = self.deadline();
        let mut open: Vec<Reached> = reached
            .into_iter()
            .filter(|w| !w.draining)
            .map(|mut w| {
                w.link = Arc::new(Budgeted {
                    inner: w.link,
                    deadline,
                });
                w
            })
            .collect();
        if open.is_empty() {
            return Ok(());
        }
        let queued = self.db(|s| s.queued_tasks(DISPATCH_SCAN)).await?;
        for task in queued {
            while let Some(worker) = open.first() {
                let attempt_id = AttemptId::generate();
                let (t, w, a) = (
                    task.task_id.clone(),
                    worker.worker_id.clone(),
                    attempt_id.clone(),
                );
                let (limits, lease_ms) = (worker.limits, self.cfg.lease_ms);
                let outcome = self
                    .db(move |s| {
                        s.place(
                            &t,
                            &w,
                            &a,
                            move |load| policy::has_room(load, limits),
                            move |input| demand_of(input, lease_ms),
                            wall_ms(),
                        )
                    })
                    .await?;
                match outcome {
                    PlaceOutcome::WorkerFull(_) => {
                        open.remove(0);
                    }
                    PlaceOutcome::NotQueued => break,
                    PlaceOutcome::Unbuildable(why) => {
                        // This task only; the ones behind it are placed.
                        self.note_unplaceable(&task, why).await?;
                        break;
                    }
                    PlaceOutcome::Placed(_) => {
                        report.placed.push(attempt_id.clone());
                        let sent = self
                            .send_placed(&worker.worker_id, &*worker.link, &attempt_id, report)
                            .await?;
                        if sent == Sent::NotTaken {
                            // A worker that did not take this one gets no
                            // more in this tick: what is placed there cannot
                            // move until it answers.
                            open.remove(0);
                        }
                        break;
                    }
                }
            }
            if open.is_empty() {
                break;
            }
        }
        Ok(())
    }

    /// Read back what the placement committed, and send that. A cancel or a
    /// retire that was recorded since goes first: then nothing is sent, and
    /// the next look at the attempt delivers it.
    async fn send_placed(
        &self,
        worker_id: &WorkerId,
        link: &dyn WorkerLink,
        attempt_id: &AttemptId,
        report: &mut TickReport,
    ) -> Result<Sent> {
        let w = worker_id.clone();
        let live = self.db(move |s| s.live_attempts(Some(&w))).await?;
        let Some(placed) = live
            .into_iter()
            .find(|l| &l.record.attempt_id == attempt_id)
        else {
            return Ok(Sent::NotTaken);
        };
        let facts = Facts {
            acked: false,
            cancel_requested: placed.record.cancel_requested_at_ms.is_some(),
            retire_requested: placed.retire_reason.is_some(),
        };
        if !policy::may_send(facts) {
            return Ok(Sent::Withheld);
        }
        if self.submit(link, &placed, report).await? {
            Ok(Sent::Taken)
        } else {
            Ok(Sent::NotTaken)
        }
    }

    /// A queued task no payload can be made for stays queued and is passed
    /// over; why is recorded once (per process) on its run.
    async fn note_unplaceable(&self, task: &QueuedTask, why: String) -> Result<()> {
        let fresh = {
            let mut seen = self.unplaceable.lock().unwrap_or_else(|e| e.into_inner());
            seen.insert(task.task_id.clone())
        };
        if !fresh {
            return Ok(());
        }
        let run = task.run_id.clone();
        let payload = json!({"task": task.task_id.to_string(), "error": why});
        self.db(move |s| s.append_event(Some(&run), None, "task_unplaceable", &payload, wall_ms()))
            .await?;
        Ok(())
    }
}

/// Whether a payload can be made for a task as it is about to be queued: the
/// construction every placement of it will use.
fn check_spec(
    run_id: &RunId,
    source_id: &SourceId,
    task_id: &TaskId,
    spec: &Value,
    lease_ms: u64,
) -> Result<()> {
    let input = PlaceInput {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        generation: Generation(1),
        source_id: Some(source_id.clone()),
        spec: spec.clone(),
    };
    match demand_of(&input, lease_ms) {
        Ok(_) => Ok(()),
        Err(e) => Err(CoordinatorError::Invalid(format!("task {task_id}: {e}"))),
    }
}

/// The payload hash and the demand recorded with a placement: the payload
/// itself, so that exactly what was decided can be sent, again if needed.
fn demand_of(
    input: &PlaceInput,
    lease_ms: u64,
) -> crate::storage::Result<(crate::ids::PayloadHash, Value)> {
    let spec: TaskSpec = serde_json::from_value(input.spec.clone())
        .map_err(|e| StorageError::Invalid(format!("task spec: {e}")))?;
    let payload = build_payload(input, &spec, lease_ms)?;
    let hash = payload_hash(&payload).map_err(|e| StorageError::Invalid(e.to_string()))?;
    let stored =
        serde_json::to_value(&payload).map_err(|e| StorageError::Invalid(e.to_string()))?;
    // What is recorded is what will be read back and sent.
    let back: AttemptPayload = serde_json::from_value(stored.clone())
        .map_err(|e| StorageError::Invalid(format!("payload: {e}")))?;
    if back != payload {
        return Err(StorageError::Invalid(
            "the payload does not read back as it was written".into(),
        ));
    }
    Ok((hash, json!({ "payload": stored })))
}

#[cfg(test)]
mod tests;
