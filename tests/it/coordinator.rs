//! The coordinator against workers that keep the worker side of the
//! contract (spec chapter 11) in memory: A19 (lost ACK, retire before a new
//! placement), A21 (a link that is gone), A22 (the parent stopped for long),
//! A23 (a restarted parent adopts), with a crash of the parent at each
//! boundary of a placement: recorded and not sent, sent and the ACK lost,
//! retire sent and not confirmed.
//!
//! No process is started here. A "crash" is the coordinator dropped where it
//! stands and its database opened again.

use crate::common::{alive, read_pid};
use crate::worker_agent;
use remote_task_runner::coordinator::policy::Limits;
use remote_task_runner::coordinator::ClientLink;
use remote_task_runner::coordinator::{
    Coordinator, CoordinatorConfig, CoordinatorError, LinkError, TaskSpec, WorkerLink,
};
use remote_task_runner::error::{ApiError, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{connect, ClientConfig};
use remote_task_runner::state::{
    AttemptState, Observation, Outcome, Receipt, Receipts, Termination,
};
use remote_task_runner::storage::{AttemptRecord, CoordinatorStore};
use remote_task_runner::wire::*;
use remote_task_runner::worker::policy::{load_counts, retire_status};
use remote_task_runner::worker::serve::serve_connection;
use remote_task_runner::worker::Agent;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Semaphore;

// ------------------------------------------------------------ the workers

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Inspect,
    Get,
    Submit,
    Cancel,
    Retire,
    Renew,
}

/// Where a request is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lost {
    /// It never arrives.
    Request,
    /// It is carried out and its answer never arrives.
    Answer,
}

#[derive(Debug, Clone)]
struct Held {
    payload: AttemptPayload,
    hash: PayloadHash,
    state: AttemptState,
    termination: Option<Termination>,
    receipts: Receipts,
    retired: bool,
    launches: u32,
    lease: Option<LeaseKey>,
    renewals: u32,
}

/// Which worker runs which task now, over all workers of a test: a task
/// that runs in two places at once is the failure everything here prevents.
#[derive(Default)]
struct Cluster {
    running: Mutex<BTreeMap<TaskId, String>>,
    starts: Mutex<Vec<(TaskId, AttemptId, String)>>,
}

impl Cluster {
    fn starts_of(&self, task: &TaskId) -> Vec<String> {
        let starts = self.starts.lock().unwrap();
        starts
            .iter()
            .filter(|(t, _, _)| t == task)
            .map(|(_, _, w)| w.clone())
            .collect()
    }
}

struct State {
    max_epoch: u64,
    held: BTreeMap<AttemptId, Held>,
    tombstones: BTreeSet<AttemptId>,
    offline: bool,
    lose: Vec<(Op, Lost)>,
    overloaded: bool,
    refuse: bool,
    slow_stop: bool,
    draining: bool,
    limits: (u32, u32),
    /// The requests that were carried out, in order.
    log: Vec<(Op, AttemptId)>,
    /// What `inspect` says its id is.
    says: WorkerId,
}

struct FakeWorker {
    id: WorkerId,
    cluster: Arc<Cluster>,
    state: Mutex<State>,
}

fn err(code: ErrorCode) -> ApiError {
    ApiError::new(code, format!("{code}"))
}

fn ended(outcome: Outcome) -> Termination {
    Termination::new(outcome, None, None).unwrap()
}

impl FakeWorker {
    fn new(name: &str, cluster: &Arc<Cluster>, limits: (u32, u32)) -> Arc<FakeWorker> {
        let id = WorkerId::parse(name).unwrap();
        Arc::new(FakeWorker {
            id: id.clone(),
            cluster: Arc::clone(cluster),
            state: Mutex::new(State {
                max_epoch: 0,
                held: BTreeMap::new(),
                tombstones: BTreeSet::new(),
                offline: false,
                lose: Vec::new(),
                overloaded: false,
                refuse: false,
                slow_stop: false,
                draining: false,
                limits,
                log: Vec::new(),
                says: id,
            }),
        })
    }

    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    fn held(&self, a: &AttemptId) -> Option<Held> {
        self.state.lock().unwrap().held.get(a).cloned()
    }

    fn holds(&self) -> Vec<AttemptId> {
        self.state.lock().unwrap().held.keys().cloned().collect()
    }

    fn count(&self, op: Op) -> usize {
        let s = self.state.lock().unwrap();
        s.log.iter().filter(|(o, _)| *o == op).count()
    }

    /// The worker starts the command of an accepted attempt.
    fn start(&self, a: &AttemptId) {
        let mut s = self.state.lock().unwrap();
        let h = s.held.get_mut(a).expect("the worker holds the attempt");
        assert_eq!(
            h.state,
            AttemptState::Accepted,
            "started once, from accepted"
        );
        assert!(!h.retired);
        h.state = AttemptState::Active;
        h.launches += 1;
        h.receipts.mark(Receipt::CommandRegistered, 1);
        h.receipts.mark(Receipt::Running, 1);
        let task = h.payload.task_id.clone();
        let name = self.id.to_string();
        let other = self
            .cluster
            .running
            .lock()
            .unwrap()
            .insert(task.clone(), name.clone());
        assert_eq!(other, None, "task {task} already runs on another worker");
        self.cluster
            .starts
            .lock()
            .unwrap()
            .push((task, a.clone(), name));
    }

    fn stop(&self, h: &mut Held, outcome: Outcome) {
        if h.state == AttemptState::Active {
            self.cluster
                .running
                .lock()
                .unwrap()
                .remove(&h.payload.task_id);
        }
        h.state = AttemptState::Terminal;
        h.termination = Some(ended(outcome));
    }

    /// The command ends, or the worker stops it by itself (lease expiry).
    fn finish(&self, a: &AttemptId, outcome: Outcome) {
        let mut s = self.state.lock().unwrap();
        let mut h = s.held.remove(a).expect("the worker holds the attempt");
        self.stop(&mut h, outcome);
        s.held.insert(a.clone(), h);
    }

    fn status(&self, a: &AttemptId, h: &Held) -> AttemptStatus {
        AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: h.payload.run_id.clone(),
            task_id: h.payload.task_id.clone(),
            generation: h.payload.generation,
            worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(1),
            state: h.state,
            receipts: h.receipts,
            spawned_at_ms: None,
            observation: Observation::Known,
            termination: h.termination,
            nodes: vec![],
            steps: vec![],
            retired: h.retired,
            log_truncated: false,
            log_dropped_bytes: 0,
        }
    }

    fn epoch(s: &mut State, epoch: CoordinatorEpoch) -> Result<(), ApiError> {
        if epoch.0 < s.max_epoch {
            return Err(err(ErrorCode::StaleEpoch));
        }
        s.max_epoch = epoch.0;
        Ok(())
    }

    /// Carry one request out as a worker would.
    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        let mut s = self.state.lock().unwrap();
        match request {
            Request::Inspect(_) => {
                let (active, pending) = load_counts(s.held.values().map(|h| h.state));
                Ok(Response::Inspect(InspectResponse {
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: s.says.clone(),
                    worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
                    agent_epoch: AgentEpoch(1),
                    snapshot_digest: Digest::of_bytes(b"s"),
                    policy_digest: Digest::of_bytes(b"p"),
                    active_task: active,
                    prefetch_pending: pending,
                    max_active_task: s.limits.0,
                    prefetch_per_worker: s.limits.1,
                    disk_free_bytes: 1,
                    draining: s.draining,
                }))
            }
            Request::GetAttempt(r) => match s.held.get(&r.attempt_id) {
                Some(h) => Ok(Response::GetAttempt(self.status(&r.attempt_id, h))),
                None if s.tombstones.contains(&r.attempt_id) => Err(err(ErrorCode::Retired)),
                None => Err(err(ErrorCode::NotFound)),
            },
            Request::SubmitAttempt(r) => {
                if !r.hash_matches().unwrap() {
                    return Err(err(ErrorCode::InvalidInput));
                }
                Self::epoch(&mut s, r.coordinator_epoch)?;
                let retired = s.held.get(&r.attempt_id).is_some_and(|h| h.retired);
                if retired || s.tombstones.contains(&r.attempt_id) {
                    return Err(err(ErrorCode::Retired));
                }
                if let Some(h) = s.held.get(&r.attempt_id) {
                    if h.hash != r.payload_hash {
                        return Err(err(ErrorCode::Conflict));
                    }
                    return Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        generation: h.payload.generation,
                        state: h.state,
                        receipts: h.receipts,
                        duplicate: true,
                    }));
                }
                if s.refuse {
                    return Err(err(ErrorCode::InvalidInput));
                }
                if s.overloaded {
                    return Err(err(ErrorCode::Overloaded));
                }
                let mut receipts = Receipts::default();
                receipts.mark(Receipt::WorkerAccepted, 1);
                let generation = r.payload.generation;
                s.log.push((Op::Submit, r.attempt_id.clone()));
                s.held.insert(
                    r.attempt_id.clone(),
                    Held {
                        payload: r.payload,
                        hash: r.payload_hash,
                        state: AttemptState::Accepted,
                        termination: None,
                        receipts,
                        retired: false,
                        launches: 0,
                        lease: None,
                        renewals: 0,
                    },
                );
                Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    generation,
                    state: AttemptState::Accepted,
                    receipts,
                    duplicate: false,
                }))
            }
            Request::CancelAttempt(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                s.log.push((Op::Cancel, r.attempt_id.clone()));
                let Some(mut h) = s.held.remove(&r.attempt_id) else {
                    s.tombstones.insert(r.attempt_id.clone());
                    return Ok(Response::CancelAttempt(CancelAttemptResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        phase: CancelPhase::Accepted,
                        state: None,
                        tombstone: true,
                    }));
                };
                if h.state != AttemptState::Terminal {
                    self.stop(&mut h, Outcome::Cancelled);
                }
                s.held.insert(r.attempt_id.clone(), h);
                Ok(Response::CancelAttempt(CancelAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    phase: CancelPhase::Completed,
                    state: Some(AttemptState::Terminal),
                    tombstone: false,
                }))
            }
            Request::RetireAttempt(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                s.log.push((Op::Retire, r.attempt_id.clone()));
                s.tombstones.insert(r.attempt_id.clone());
                let slow = s.slow_stop;
                let (known, was, now, launched) = match s.held.remove(&r.attempt_id) {
                    None => (false, false, false, false),
                    Some(mut h) => {
                        let was = h.state == AttemptState::Terminal;
                        h.retired = true;
                        if !was && !slow {
                            self.stop(&mut h, Outcome::Cancelled);
                        }
                        let out = (true, was, h.state == AttemptState::Terminal, h.launches > 0);
                        s.held.insert(r.attempt_id.clone(), h);
                        out
                    }
                };
                Ok(Response::RetireAttempt(RetireAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    tombstone: true,
                    status: retire_status(known, was, now, launched),
                }))
            }
            Request::RenewLease(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                let key = r.key();
                let Some(h) = s.held.get_mut(&r.attempt_id) else {
                    return Err(err(ErrorCode::NotFound));
                };
                if h.retired || h.state == AttemptState::Terminal {
                    return Err(err(ErrorCode::LeaseNotRenewable));
                }
                let extended = key.extends(h.lease);
                if extended {
                    h.lease = Some(key);
                    h.renewals += 1;
                }
                let state = h.state;
                s.log.push((Op::Renew, r.attempt_id.clone()));
                Ok(Response::RenewLease(RenewLeaseResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    lease_seq: r.lease_seq,
                    extended,
                    lease_remaining_ms: r.lease_ms,
                    state,
                }))
            }
            other => panic!("the coordinator core does not send {other:?}"),
        }
    }
}

fn op_of(request: &Request) -> Op {
    match request {
        Request::Inspect(_) => Op::Inspect,
        Request::GetAttempt(_) => Op::Get,
        Request::SubmitAttempt(_) => Op::Submit,
        Request::CancelAttempt(_) => Op::Cancel,
        Request::RetireAttempt(_) => Op::Retire,
        Request::RenewLease(_) => Op::Renew,
        other => panic!("{other:?}"),
    }
}

impl WorkerLink for FakeWorker {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let lost = {
                let mut s = self.state.lock().unwrap();
                if s.offline {
                    Some(Lost::Request)
                } else {
                    let op = op_of(&request);
                    let at = s.lose.iter().position(|(o, _)| *o == op);
                    at.map(|i| s.lose.remove(i).1)
                }
            };
            match lost {
                Some(Lost::Request) => Err(LinkError::Unconfirmed),
                Some(Lost::Answer) => {
                    let _ = self.handle(request);
                    Err(LinkError::Unconfirmed)
                }
                None => self.handle(request).map_err(LinkError::Remote),
            }
        })
    }
}

// ------------------------------------------------------------ the fixture

struct Fx {
    dir: TempDir,
    cluster: Arc<Cluster>,
    cfg: CoordinatorConfig,
}

fn fx() -> Fx {
    Fx {
        dir: tempfile::tempdir().unwrap(),
        cluster: Arc::default(),
        cfg: CoordinatorConfig {
            limits: Limits {
                max_active_task: 2,
                prefetch_per_worker: 1,
            },
            lease_ms: 60_000,
        },
    }
}

impl Fx {
    /// Start a coordinator on the state directory: the first, or the next
    /// after the one before was dropped.
    fn open(&self, workers: &[&Arc<FakeWorker>]) -> Coordinator {
        let store = CoordinatorStore::open(self.dir.path(), 1).unwrap();
        let c = Coordinator::start(store, self.cfg).unwrap();
        for w in workers {
            c.attach(w.id.clone(), Arc::clone(w) as Arc<dyn WorkerLink>);
        }
        c
    }

    fn worker(&self, name: &str, limits: (u32, u32)) -> Arc<FakeWorker> {
        FakeWorker::new(name, &self.cluster, limits)
    }
}

fn spec() -> TaskSpec {
    TaskSpec {
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::of_bytes(b"manifest"),
        commands: vec![CommandSpec {
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
    }
}

/// A source the parent holds (a run refers to one).
fn source(c: &Coordinator) -> SourceId {
    let id = SourceId::generate();
    c.store()
        .put_source_ref(&id, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    id
}

async fn enqueue(c: &Coordinator, n: usize) -> Vec<TaskId> {
    let tasks: Vec<TaskId> = (0..n).map(|_| TaskId::generate()).collect();
    let specs = tasks.iter().map(|t| (t.clone(), spec())).collect();
    c.enqueue(RunId::generate(), source(c), specs)
        .await
        .unwrap();
    tasks
}

fn attempts(c: &Coordinator, task: &TaskId) -> Vec<AttemptRecord> {
    c.store().attempts_of_task(task).unwrap()
}

/// The one attempt of a task.
fn only(c: &Coordinator, task: &TaskId) -> AttemptRecord {
    let mut all = attempts(c, task);
    assert_eq!(all.len(), 1, "{all:?}");
    all.remove(0)
}

fn queued(c: &Coordinator) -> Vec<TaskId> {
    let q = c.store().queued_tasks(100).unwrap();
    q.into_iter().map(|t| t.task_id).collect()
}

// --------------------------------------------------------------- dispatch

#[tokio::test]
async fn dispatch_fills_the_free_active_slots_and_the_prefetch_and_no_more() {
    let f = fx();
    // The worker's own limits (1 active, 1 prefetch) are stricter than the
    // coordinator's (2, 1).
    let a = f.worker("w-a", (1, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 5).await;

    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2, "one to run and one prefetched");
    assert_eq!(a.holds().len(), 2);
    let first = only(&c, &tasks[0]);
    let second = only(&c, &tasks[1]);
    assert_eq!(
        report.placed,
        vec![first.attempt_id.clone(), second.attempt_id.clone()]
    );
    assert_eq!(queued(&c), tasks[2..].to_vec(), "oldest first");
    assert!(first.receipts.reached(Receipt::WorkerAccepted));
    assert!(c.store().unacked_demand().unwrap().is_empty());

    // Nothing more while both slots are held, started or not.
    assert!(c.tick().await.unwrap().placed.is_empty());
    a.start(&first.attempt_id);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(only(&c, &tasks[0]).state, AttemptState::Active);
    assert!(only(&c, &tasks[0]).receipts.reached(Receipt::Running));

    // The end of one frees one.
    a.finish(&first.attempt_id, Outcome::Succeeded);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert_eq!(report.placed.len(), 1);
    assert_eq!(only(&c, &tasks[2]).worker_id, a.id);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Succeeded
    );
    assert_eq!(queued(&c), tasks[3..].to_vec());

    // A second worker takes what the first has no room for; a draining one
    // takes nothing.
    let b = f.worker("w-b", (9, 9));
    let d = f.worker("w-d", (9, 9));
    d.set(|s| s.draining = true);
    c.attach(b.id.clone(), b.clone());
    c.attach(d.id.clone(), d.clone());
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2);
    assert_eq!(only(&c, &tasks[3]).worker_id, b.id);
    assert_eq!(only(&c, &tasks[4]).worker_id, b.id);
    assert!(d.holds().is_empty());
    assert!(queued(&c).is_empty());
}

#[tokio::test]
async fn the_coordinators_own_limits_hold_when_the_worker_allows_more() {
    let f = fx();
    let a = f.worker("w-a", (50, 50));
    let c = f.open(&[&a]);
    enqueue(&c, 6).await;
    assert_eq!(
        c.tick().await.unwrap().placed.len(),
        3,
        "2 active + 1 prefetch"
    );
    assert!(c.tick().await.unwrap().placed.is_empty());
    for id in a.holds() {
        a.start(&id);
    }
    // Started or not, three is all it ever holds.
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(a.holds().len(), 3);
}

#[tokio::test]
async fn a_worker_that_is_not_who_it_should_be_gets_nothing() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    a.set(|s| s.says = WorkerId::parse("w-other").unwrap());
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(a.holds().is_empty());
    assert_eq!(queued(&c), tasks);
}

// ------------------------------------ recorded, and the parent dies unsent

#[tokio::test]
async fn a_placement_that_was_recorded_and_never_sent_is_sent_by_the_next_parent() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 2).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));

    let report = c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(report.placed, vec![first.attempt_id.clone()]);
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(a.holds().is_empty(), "the submit never arrived");
    // The decision and the demand are durable together, before any send.
    assert_eq!(first.observation, Observation::Unknown);
    assert_eq!(first.state, AttemptState::Accepted);
    let demand = c.store().unacked_demand().unwrap();
    assert_eq!(demand.len(), 1);
    assert_eq!(demand[0].attempt_id, Some(first.attempt_id.clone()));
    assert_eq!(demand[0].worker_id, a.id);
    let recorded: AttemptPayload =
        serde_json::from_value(demand[0].demand["payload"].clone()).unwrap();
    assert_eq!(payload_hash(&recorded).unwrap(), first.payload_hash);
    assert_eq!(recorded.task_id, tasks[0]);
    assert_eq!(recorded.generation, Generation(1));
    assert_eq!(recorded.lease_ms, 60_000);

    // The parent dies here. The next one finds the demand, counts it against
    // the worker (the second task does not go there) and sends the same
    // attempt to the same worker, not to the idle one.
    drop(c);
    let c = f.open(&[&a, &b]);
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Reconciling);
    let report = c.tick().await.unwrap();
    assert_eq!(a.holds(), vec![first.attempt_id.clone()]);
    let held = a.held(&first.attempt_id).unwrap();
    assert_eq!(held.hash, first.payload_hash);
    assert_eq!(held.payload, recorded);
    let now = only(&c, &tasks[0]);
    assert_eq!(now.attempt_id, first.attempt_id);
    assert_eq!(now.observation, Observation::Known);
    assert_eq!(
        now.coordinator_epoch,
        CoordinatorEpoch(1),
        "the same attempt"
    );
    assert!(c.store().unacked_demand().unwrap().is_empty());
    // The second task went to the other worker, and only that.
    assert_eq!(report.placed.len(), 1);
    assert_eq!(only(&c, &tasks[1]).worker_id, b.id);
    assert_eq!(b.holds().len(), 1);
}

// --------------------------------------------------- A19: the ACK is lost

#[tokio::test]
async fn a19_a_lost_ack_is_asked_on_the_same_worker_and_the_task_runs_nowhere_else() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Answer)));

    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(first.observation, Observation::Unknown);
    assert!(!first.receipts.reached(Receipt::WorkerAccepted));
    assert_eq!(c.store().unacked_demand().unwrap().len(), 1);
    // The worker took it and starts it; the parent does not know.
    a.start(&first.attempt_id);
    a.set(|s| s.offline = true);

    // However long the worker is silent, with an idle worker next to it:
    // not placed again, not failed, not forgotten.
    for _ in 0..20 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty());
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    }
    assert!(b.holds().is_empty());
    assert!(queued(&c).is_empty());
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Accepted);
    assert_eq!(kept.observation, Observation::Unknown);

    // A parent restart in the middle changes nothing about that.
    drop(c);
    let c = f.open(&[&a, &b]);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());

    // The worker is back: the same attempt is found there, running.
    a.set(|s| s.offline = false);
    let report = c.tick().await.unwrap();
    assert!(report.placed.is_empty() && report.unknown.is_empty());
    let found = only(&c, &tasks[0]);
    assert_eq!(found.attempt_id, first.attempt_id);
    assert_eq!(found.state, AttemptState::Active);
    assert_eq!(found.observation, Observation::Known);
    assert!(found.receipts.reached(Receipt::WorkerAccepted));
    assert!(c.store().unacked_demand().unwrap().is_empty());
    assert_eq!(a.count(Op::Submit), 1, "accepted once, never sent anew");
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
}

#[tokio::test]
async fn a19_the_task_moves_only_after_the_retire_and_the_late_submit_is_refused() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    // The submit is on its way for a long time: it has not arrived.
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    let demand = c.store().unacked_demand().unwrap().remove(0);
    let late = SubmitAttemptRequest::new(
        RequestId::generate(),
        CoordinatorEpoch(1),
        first.attempt_id.clone(),
        serde_json::from_value(demand.demand["payload"].clone()).unwrap(),
    )
    .unwrap();

    // Someone wants the task elsewhere. While the worker cannot confirm the
    // retire, the task stays where it is.
    a.set(|s| s.offline = true);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.retired.is_empty());
    }
    assert!(b.holds().is_empty());
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Unknown);
    assert!(!only(&c, &tasks[0]).retired);

    // The worker answers: tombstone written, never started. Only now is the
    // next generation placed (the first worker is draining by then).
    a.set(|s| {
        s.offline = false;
        s.draining = true;
    });
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    let all = attempts(&c, &tasks[0]);
    assert_eq!(all.len(), 2);
    assert!(all[0].retired);
    assert_eq!(all[1].generation, Generation(2));
    assert_eq!(all[1].worker_id, b.id);
    assert_eq!(report.placed, vec![all[1].attempt_id.clone()]);
    let (_, confirmed) = c
        .store()
        .retire_tombstone(&first.attempt_id)
        .unwrap()
        .unwrap();
    assert!(confirmed.is_some());
    assert!(c.store().unacked_demand().unwrap().is_empty());

    // The old submit arrives at last, and again: refused, never started.
    for _ in 0..3 {
        let e = a.handle(Request::SubmitAttempt(late.clone())).unwrap_err();
        assert_eq!(e.code, ErrorCode::Retired);
    }
    assert!(a.holds().is_empty());
    b.start(&all[1].attempt_id);
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-b"]);
    // Nothing more is placed for the task.
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(attempts(&c, &tasks[0]).len(), 2);
}

// ----------------------------------- retire sent, and its answer is lost

#[tokio::test]
async fn a_retire_whose_answer_was_lost_frees_the_task_exactly_once_after_a_restart() {
    for lose_answer in [false, true] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a]);
        let tasks = enqueue(&c, 1).await;
        c.tick().await.unwrap();
        let first = only(&c, &tasks[0]);
        a.start(&first.attempt_id);
        c.tick().await.unwrap();

        c.request_retire(&first.attempt_id, "move").await.unwrap();
        let c = if lose_answer {
            // The worker stops the command and writes the tombstone; the
            // parent never hears it, and dies.
            a.set(|s| s.lose.push((Op::Retire, Lost::Answer)));
            let report = c.tick().await.unwrap();
            assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
            assert!(report.retired.is_empty() && report.placed.is_empty());
            let held = a.held(&first.attempt_id).unwrap();
            assert!(held.retired);
            assert_eq!(held.termination, Some(ended(Outcome::Cancelled)));
            assert!(queued(&c).is_empty(), "not confirmed: the task stays");
            drop(c);
            f.open(&[&a])
        } else {
            c
        };
        a.set(|s| s.draining = true);
        c.attach(b.id.clone(), b.clone());

        // The retire is sent (again). What the worker did is the same in
        // both runs, and so is what the parent concludes.
        let report = c.tick().await.unwrap();
        assert_eq!(
            report.retired,
            vec![first.attempt_id.clone()],
            "{lose_answer}"
        );
        assert!(report.settled.is_empty());
        let all = attempts(&c, &tasks[0]);
        assert_eq!(all.len(), 2, "{lose_answer}");
        assert!(all[0].retired && all[0].termination.is_none());
        assert_eq!(all[1].generation, Generation(2));
        assert_eq!(all[1].worker_id, b.id);
        b.start(&all[1].attempt_id);
        for _ in 0..3 {
            assert!(c.tick().await.unwrap().placed.is_empty());
        }
        assert_eq!(
            attempts(&c, &tasks[0]).len(),
            2,
            "one new generation, not more"
        );
        assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a", "w-b"]);
        assert_eq!(a.count(Op::Retire), if lose_answer { 2 } else { 1 });
    }
}

#[tokio::test]
async fn an_attempt_that_had_ended_by_itself_keeps_its_result_and_is_not_placed_again() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();
    // It succeeds, and before the parent sees that, a retire is asked for.
    a.finish(&first.attempt_id, Outcome::Succeeded);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    let report = c.tick().await.unwrap();
    assert!(report.retired.is_empty() && report.placed.is_empty());
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Succeeded);
    assert!(!rec.retired);
    assert!(queued(&c).is_empty());
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());
}

#[tokio::test]
async fn a_retire_the_worker_cannot_confirm_yet_keeps_the_task_and_its_slot() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 2).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(only(&c, &tasks[1]).worker_id, b.id);
    a.start(&first.attempt_id);
    a.set(|s| s.slow_stop = true);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    for _ in 0..3 {
        let report = c.tick().await.unwrap();
        assert!(report.retired.is_empty() && report.placed.is_empty());
    }
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Known);
    // It stops at last.
    a.set(|s| s.slow_stop = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    // The freed task goes back to the first worker with room: the same one.
    assert_eq!(attempts(&c, &tasks[0])[1].worker_id, a.id);
    assert_eq!(a.holds().len(), 2);
}

// ------------------------------------------- A21: the link is gone for long

#[tokio::test]
async fn a21_an_attempt_whose_link_is_gone_is_kept_and_followed_again_on_return() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    let renewed = a.held(&first.attempt_id).unwrap();
    assert_eq!(renewed.renewals, 2, "each look renews the lease");
    assert_eq!(
        renewed.lease.unwrap().coordinator_epoch,
        CoordinatorEpoch(1)
    );

    // The connection is gone. The worker keeps running within its lease.
    c.detach(&a.id).await.unwrap();
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Unknown);
    for _ in 0..30 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.settled.is_empty());
    }
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Active, "a cut link ends nothing");
    assert_eq!(kept.observation, Observation::Unknown);
    assert_eq!(kept.termination, None);
    assert!(b.holds().is_empty());
    assert_eq!(a.held(&first.attempt_id).unwrap().renewals, 2);

    // Reconnected: the same attempt, known again, its lease renewed with a
    // higher key.
    c.attach(a.id.clone(), a.clone());
    let report = c.tick().await.unwrap();
    assert!(report.unknown.is_empty());
    let back = only(&c, &tasks[0]);
    assert_eq!(back.attempt_id, first.attempt_id);
    assert_eq!(back.observation, Observation::Known);
    let held = a.held(&first.attempt_id).unwrap();
    assert_eq!(held.renewals, 3);
    assert!(held.lease.unwrap() > renewed.lease.unwrap());
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
    assert_eq!(a.count(Op::Submit), 1);
}

// ------------------------------------ A22: the parent is stopped for long

#[tokio::test]
async fn a22_after_a_long_stop_the_parent_keeps_unknown_until_the_worker_says_it_stopped() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();

    // The parent is gone for longer than the lease. The worker stops the
    // command by itself.
    drop(c);
    a.finish(&first.attempt_id, Outcome::Interrupted);
    a.set(|s| s.offline = true);

    // The parent returns and cannot reach the worker. The lease has surely
    // run out, and an idle worker is there: still nothing is placed and
    // nothing is concluded.
    let c = f.open(&[&b]);
    for _ in 0..5 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Reconciling);
    c.attach(a.id.clone(), a.clone());
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
        assert!(report.placed.is_empty() && report.settled.is_empty());
    }
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Active);
    assert_eq!(kept.observation, Observation::Unknown);
    assert!(b.holds().is_empty() && queued(&c).is_empty());

    // The worker's word that it stopped is what ends the attempt.
    a.set(|s| s.offline = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Interrupted);
    assert_eq!(rec.observation, Observation::Known);
    // Interrupted is a result; a retry is the user's to ask for.
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
}

// ------------------------------------------- A23: the parent is restarted

#[tokio::test]
async fn a23_a_restarted_parent_adopts_what_runs_and_starts_nothing_again() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    let before: Vec<AttemptRecord> = tasks.iter().map(|t| only(&c, t)).collect();
    a.start(&before[0].attempt_id);
    a.start(&before[1].attempt_id);
    c.tick().await.unwrap();
    assert_eq!(a.count(Op::Submit), 3);

    drop(c);
    let c = f.open(&[&a]);
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    for t in &tasks {
        assert_eq!(only(&c, t).observation, Observation::Reconciling);
    }
    let report = c.tick().await.unwrap();
    assert!(report.placed.is_empty() && report.unknown.is_empty());
    for (t, was) in tasks.iter().zip(&before) {
        let now = only(&c, t);
        assert_eq!(now.attempt_id, was.attempt_id, "the same attempt, adopted");
        assert_eq!(now.generation, Generation(1));
        assert_eq!(now.observation, Observation::Known);
        assert_eq!(now.coordinator_epoch, CoordinatorEpoch(1));
    }
    assert_eq!(only(&c, &tasks[0]).state, AttemptState::Active);
    assert_eq!(only(&c, &tasks[2]).state, AttemptState::Accepted);
    assert_eq!(a.count(Op::Submit), 3, "nothing was submitted again");
    assert_eq!(a.holds().len(), 3);
    // The new epoch controls the old attempts.
    let lease = a.held(&before[0].attempt_id).unwrap().lease.unwrap();
    assert_eq!(lease.coordinator_epoch, CoordinatorEpoch(2));
    assert_eq!(a.state.lock().unwrap().max_epoch, 2);

    // A result of an attempt made under the old epoch is taken.
    a.finish(&before[0].attempt_id, Outcome::Succeeded);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![before[0].attempt_id.clone()]);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Succeeded
    );
    for t in &tasks {
        assert_eq!(f.cluster.starts_of(t).len(), usize::from(t != &tasks[2]));
    }
}

#[tokio::test]
async fn a_parent_that_a_newer_one_replaced_stops_for_good() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 2).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    // The worker has heard from a newer coordinator.
    a.set(|s| s.max_epoch = 7);
    let e = c.tick().await.unwrap_err();
    assert!(
        matches!(e, CoordinatorError::Fenced(CoordinatorEpoch(1))),
        "{e:?}"
    );
    assert!(c.is_fenced());
    let renewals = a.held(&first.attempt_id).unwrap().renewals;
    let calls = a.count(Op::Renew) + a.count(Op::Submit) + a.count(Op::Cancel);
    assert!(matches!(c.tick().await, Err(CoordinatorError::Fenced(_))));
    let more = c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(TaskId::generate(), spec())],
    );
    assert!(matches!(more.await, Err(CoordinatorError::Fenced(_))));
    assert_eq!(a.held(&first.attempt_id).unwrap().renewals, renewals);
    assert_eq!(
        a.count(Op::Renew) + a.count(Op::Submit) + a.count(Op::Cancel),
        calls
    );
    // What it knew stays as it was: nothing is failed by the fencing.
    assert_eq!(only(&c, &tasks[0]).termination, None);
}

// ----------------------------------------------------------------- cancel

#[tokio::test]
async fn a_cancel_is_durable_first_and_reaches_the_worker_also_after_a_restart() {
    let f = fx();
    let a = f.worker("w-a", (1, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    let running = only(&c, &tasks[0]);
    a.start(&running.attempt_id);
    c.tick().await.unwrap();

    // A queued task: never placed. A running one: the worker is offline, so
    // the request waits in the database, across a restart.
    assert_eq!(c.cancel_task(&tasks[2]).await.unwrap(), None);
    a.set(|s| s.offline = true);
    let hit = c.cancel_task(&tasks[0]).await.unwrap().unwrap();
    assert_eq!(hit.attempt_id, running.attempt_id);
    c.tick().await.unwrap();
    assert_eq!(a.count(Op::Cancel), 0);
    drop(c);
    let c = f.open(&[&a]);
    a.set(|s| s.offline = false);

    let report = c.tick().await.unwrap();
    assert_eq!(a.count(Op::Cancel), 1);
    assert_eq!(
        a.held(&running.attempt_id).unwrap().termination,
        Some(ended(Outcome::Cancelled))
    );
    assert!(report.settled.is_empty(), "accepted is not completed");
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![running.attempt_id.clone()]);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Cancelled
    );
    assert_eq!(
        a.count(Op::Cancel),
        1,
        "a settled attempt is not cancelled again"
    );
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    assert!(
        attempts(&c, &tasks[2]).is_empty(),
        "the cancelled task was never placed"
    );
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
}

#[tokio::test]
async fn a_cancel_of_an_attempt_that_never_arrived_leaves_a_tombstone_and_is_settled_here() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    let demand = c.store().unacked_demand().unwrap().remove(0);
    c.cancel_task(&tasks[0]).await.unwrap();

    // The cancel goes instead of a resend, and the worker remembers it.
    let report = c.tick().await.unwrap();
    assert!(report.settled.is_empty());
    assert_eq!(a.count(Op::Cancel), 1);
    assert_eq!(a.count(Op::Submit), 0);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(c.store().unacked_demand().unwrap().is_empty());
    // The submit that was on its way cannot start anything.
    let late = SubmitAttemptRequest::new(
        RequestId::generate(),
        CoordinatorEpoch(1),
        first.attempt_id.clone(),
        serde_json::from_value(demand.demand["payload"].clone()).unwrap(),
    )
    .unwrap();
    let e = a.handle(Request::SubmitAttempt(late)).unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    assert!(f.cluster.starts_of(&tasks[0]).is_empty());
    assert!(c.tick().await.unwrap().placed.is_empty());
}

// ----------------------------------------------------------- refused submits

#[tokio::test]
async fn an_overloaded_worker_gets_the_same_attempt_again_later() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.overloaded = true);
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(first.observation, Observation::Known);
    assert!(a.holds().is_empty() && b.holds().is_empty());
    assert_eq!(c.store().unacked_demand().unwrap().len(), 1);
    a.set(|s| s.overloaded = false);
    c.tick().await.unwrap();
    assert_eq!(a.holds(), vec![first.attempt_id.clone()]);
    assert!(c.store().unacked_demand().unwrap().is_empty());
}

#[tokio::test]
async fn an_attempt_a_worker_will_never_take_is_retired_there_and_failed_here() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.refuse = true);
    let report = c.tick().await.unwrap();
    assert!(report.settled.is_empty());
    let first = only(&c, &tasks[0]);
    assert_eq!(
        c.store()
            .events_of(&first.attempt_id, "submit_refused")
            .unwrap()
            .len(),
        1
    );
    // The tombstone goes to the worker first; then the parent fixes failed.
    let report = c.tick().await.unwrap();
    assert_eq!(a.count(Op::Retire), 1);
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert!(report.retired.is_empty());
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Failed);
    assert!(c.store().unacked_demand().unwrap().is_empty());
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert!(
        b.holds().is_empty(),
        "a refusal is not a reason to try elsewhere"
    );
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
}

#[tokio::test]
async fn a_status_about_another_generation_is_no_answer() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.set(|s| {
        let h = s.held.get_mut(&first.attempt_id).unwrap();
        h.payload.generation = Generation(9);
        h.state = AttemptState::Terminal;
        h.termination = Some(ended(Outcome::Succeeded));
    });
    let report = c.tick().await.unwrap();
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(report.settled.is_empty());
    assert_eq!(only(&c, &tasks[0]).termination, None);
}

#[tokio::test]
async fn a_run_without_a_command_is_refused_whole() {
    let f = fx();
    let c = f.open(&[]);
    let mut empty = spec();
    empty.commands.clear();
    let tasks = vec![(TaskId::generate(), spec()), (TaskId::generate(), empty)];
    let e = c.enqueue(RunId::generate(), source(&c), tasks).await;
    assert!(matches!(e, Err(CoordinatorError::Invalid(_))), "{e:?}");
    assert!(queued(&c).is_empty());
}

// ------------------------------------------------- against a real worker

/// A link that carries one retire out and loses its answer.
struct LoseRetireAnswer {
    inner: ClientLink,
    armed: AtomicBool,
}

impl WorkerLink for LoseRetireAnswer {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let lose = matches!(request, Request::RetireAttempt(_))
                && self.armed.swap(false, Ordering::SeqCst);
            let answer = self.inner.call(request).await;
            if lose {
                Err(LinkError::Unconfirmed)
            } else {
                answer
            }
        })
    }
}

/// A connection of this coordinator (its cluster, id and epoch) to the agent.
async fn link_to(c: &Coordinator, agent: &Arc<Agent>, lose_retire: bool) -> Arc<dyn WorkerLink> {
    let (near, far) = duplex_pair(64 * 1024);
    let cluster = c.store().cluster_id().clone();
    let permits = Arc::new(Semaphore::new(64));
    tokio::spawn(serve_connection(
        agent.clone(),
        far,
        cluster.clone(),
        permits,
    ));
    let cfg = ClientConfig::new(cluster, c.store().coordinator_id().clone(), c.epoch());
    let client = connect(near, cfg).await.unwrap().client;
    Arc::new(LoseRetireAnswer {
        inner: ClientLink::new(client, Duration::from_secs(30)),
        armed: AtomicBool::new(lose_retire),
    })
}

fn count_lines(p: &std::path::Path) -> usize {
    std::fs::read_to_string(p).map_or(0, |s| s.lines().count())
}

/// The attempt of a task that is not retired.
fn only_live(c: &Coordinator, task: &TaskId) -> AttemptRecord {
    let mut live: Vec<AttemptRecord> = attempts(c, task)
        .into_iter()
        .filter(|a| !a.retired)
        .collect();
    assert_eq!(live.len(), 1);
    live.remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_real_worker_stops_the_retired_run_and_the_resent_retire_frees_the_task_once() {
    let mut w = worker_agent::fx();
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        Coordinator::start(store, CoordinatorConfig::default()).unwrap()
    };
    let c = open();
    w.cluster = c.store().cluster_id().clone();
    let agent = w.start("boot-a");
    let id = WorkerId::parse("w-test").unwrap();
    c.attach(id.clone(), link_to(&c, &agent, true).await);

    let (script, marker, pidfile) = worker_agent::stay(&w);
    let mut task_spec = spec();
    task_spec.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
    task_spec.commands[0]
        .env
        .insert("PATH".into(), "/usr/bin:/bin".into());
    let task = TaskId::generate();
    c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(task.clone(), task_spec)],
    )
    .await
    .unwrap();
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    let first = only(&c, &task);
    worker_agent::wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();
    c.tick().await.unwrap();
    assert!(only(&c, &task).receipts.reached(Receipt::WorkerAccepted));

    // The retire reaches the worker, which stops the command; the answer
    // is lost and the parent dies not knowing.
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(report.retired.is_empty() && report.placed.is_empty());
    assert!(!alive(pid), "the worker stopped the command");
    assert!(queued(&c).is_empty());
    drop(c);

    // The next parent, in a new epoch, sends the retire again. The worker
    // says the attempt had ended already; that it was stopped, not finished,
    // is what frees the task. It is placed once more, and only once.
    let c = open();
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    c.attach(id.clone(), link_to(&c, &agent, false).await);
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert_eq!(report.placed.len(), 1);
    let all = attempts(&c, &task);
    assert_eq!(all.len(), 2);
    assert!(all[0].retired);
    assert_eq!(all[1].generation, Generation(2));
    worker_agent::wait_for("the second run", || count_lines(&marker) == 2);
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(attempts(&c, &task).len(), 2);
    assert_eq!(only_live(&c, &task).state, AttemptState::Active);

    // Cancel what still runs, and see it end.
    c.cancel_task(&task).await.unwrap();
    let second = all[1].attempt_id.clone();
    let mut ended = None;
    for _ in 0..200 {
        c.tick().await.unwrap();
        ended = c.store().get_attempt(&second).unwrap().unwrap().termination;
        if ended.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ended.map(|t| t.outcome()), Some(Outcome::Cancelled));
    assert_eq!(count_lines(&marker), 2, "one start per generation");
}
