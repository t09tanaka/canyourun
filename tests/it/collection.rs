//! Result collection (spec chapter 13) against a worker that keeps the
//! worker side of the contract in memory: A30 (a connection lost while
//! collecting: `collecting` is kept and only the collection is continued),
//! A31 (a missing or empty result is not a success), a crash of the parent
//! at each boundary of a collection, corrupted bytes, and the order of the
//! acknowledgement.
//!
//! No process is started here. A "crash" is the coordinator dropped where
//! it stands and its database and result store opened again.

use remote_task_runner::coordinator::policy::Limits;
use remote_task_runner::coordinator::{
    Coordinator, CoordinatorConfig, LinkError, TaskSpec, WorkerLink,
};
use remote_task_runner::error::{ApiError, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::platform::wall_ms;
use remote_task_runner::results::plan::{RunVerdict, TaskStanding, Verdict};
use remote_task_runner::results::retention::sweep;
use remote_task_runner::results::{CollectConfig, CollectReport, ResultStore};
use remote_task_runner::state::{
    AttemptState, Collection, Observation, Outcome, Receipt, Receipts, RunDisplay, Termination,
};
use remote_task_runner::storage::{CoordinatorStore, Seen, StorageError};
use remote_task_runner::wire::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

const DAY: u64 = 86_400_000;

// ------------------------------------------------------------ the worker

/// Where a request is lost. The worker is gone from then on, until it is
/// brought back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lost {
    /// It never arrives.
    Request,
    /// It is carried out and its answer never arrives.
    Answer,
}

/// What an ended attempt left on the worker.
#[derive(Debug, Clone, Default)]
struct Out {
    /// A result is stored (otherwise the attempt has its termination only).
    has_result: bool,
    /// The records of the result, each with the bytes the worker serves.
    artifacts: Vec<(ArtifactRecord, Vec<u8>)>,
    not_stored: Vec<ArtifactNotStored>,
    log: Vec<u8>,
    /// The spool is closed: the log has its end mark.
    log_closed: bool,
    dropped: u64,
}

struct Job {
    payload: AttemptPayload,
    state: AttemptState,
    termination: Option<Termination>,
    out: Out,
    /// The digests that were acknowledged, in order.
    acks: Vec<Digest>,
}

#[derive(Default)]
struct State {
    jobs: BTreeMap<AttemptId, Job>,
    offline: bool,
    /// Lose the n-th request (from 1) of a kind, and be gone after it.
    trip: Option<(&'static str, usize, Lost)>,
    /// How many of the next chunks arrive with a flipped byte.
    corrupt_chunks: u32,
    /// The next chunk says it is from one byte further.
    shift_chunk: bool,
    /// What was left behind for an attempt that has no result yet is stored
    /// as its result the moment an acknowledgement arrives.
    result_appears_on_ack: bool,
    /// Answer the next requests of a kind with an error: the kind, the
    /// code, and how many times.
    fail: Option<(&'static str, ErrorCode, u32)>,
    /// Every request that arrived: its kind and (for bytes) its offset.
    ops: Vec<(&'static str, u64)>,
}

struct Worker {
    id: WorkerId,
    state: Mutex<State>,
}

fn err(code: ErrorCode) -> ApiError {
    ApiError::new(code, format!("{code}"))
}

fn ended(outcome: Outcome) -> Termination {
    Termination::new(outcome, None, None).unwrap()
}

fn digest_of(job: &Job) -> Digest {
    let t = job.termination.unwrap();
    if job.out.has_result {
        let records: Vec<&ArtifactRecord> = job.out.artifacts.iter().map(|(r, _)| r).collect();
        payload_hash(&json!({
            "termination": t,
            "artifacts": records,
            "artifacts_not_stored": job.out.not_stored,
            "logs": job.out.log.len(),
        }))
        .unwrap()
    } else {
        payload_hash(&json!({ "termination": t, "artifacts": [] })).unwrap()
    }
}

impl Worker {
    fn new(name: &str) -> Arc<Worker> {
        Arc::new(Worker {
            id: WorkerId::parse(name).unwrap(),
            state: Mutex::default(),
        })
    }

    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    fn count(&self, kind: &str) -> usize {
        let s = self.state.lock().unwrap();
        s.ops.iter().filter(|(k, _)| *k == kind).count()
    }

    fn offsets(&self, kind: &str) -> Vec<u64> {
        let s = self.state.lock().unwrap();
        let of_kind = s.ops.iter().filter(|(k, _)| *k == kind);
        of_kind.map(|(_, o)| *o).collect()
    }

    fn acks(&self, a: &AttemptId) -> Vec<Digest> {
        self.state.lock().unwrap().jobs[a].acks.clone()
    }

    fn digest(&self, a: &AttemptId) -> Digest {
        digest_of(&self.state.lock().unwrap().jobs[a])
    }

    fn payload(&self, a: &AttemptId) -> AttemptPayload {
        self.state.lock().unwrap().jobs[a].payload.clone()
    }

    /// The attempt ends and leaves `out`.
    fn finish(&self, a: &AttemptId, outcome: Outcome, out: Out) {
        let mut s = self.state.lock().unwrap();
        let job = s.jobs.get_mut(a).unwrap();
        job.state = AttemptState::Terminal;
        job.termination = Some(ended(outcome));
        job.out = out;
    }

    fn status(a: &AttemptId, job: &Job) -> AttemptStatus {
        AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: job.payload.run_id.clone(),
            task_id: job.payload.task_id.clone(),
            generation: job.payload.generation,
            worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(1),
            state: job.state,
            receipts: Receipts::default(),
            spawned_at_ms: None,
            observation: Observation::Known,
            termination: job.termination,
            nodes: vec![],
            steps: vec![],
            retired: false,
            log_truncated: false,
            log_dropped_bytes: 0,
        }
    }

    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        let mut s = self.state.lock().unwrap();
        let job_of = |s: &mut State, a: &AttemptId| -> Result<(), ApiError> {
            match s.jobs.get(a) {
                Some(j) if j.state == AttemptState::Terminal => Ok(()),
                Some(_) => Err(err(ErrorCode::Conflict)),
                None => Err(err(ErrorCode::NotFound)),
            }
        };
        match request {
            Request::Inspect(_) => Ok(Response::Inspect(InspectResponse {
                schema_version: SchemaVersion::CURRENT,
                worker_id: self.id.clone(),
                worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
                agent_epoch: AgentEpoch(1),
                snapshot_digest: Digest::of_bytes(b"s"),
                policy_digest: Digest::of_bytes(b"p"),
                active_task: 0,
                prefetch_pending: 0,
                max_active_task: 8,
                prefetch_per_worker: 8,
                disk_free_bytes: 1,
                draining: false,
            })),
            Request::GetAttempt(r) => match s.jobs.get(&r.attempt_id) {
                Some(job) => Ok(Response::GetAttempt(Self::status(&r.attempt_id, job))),
                None => Err(err(ErrorCode::NotFound)),
            },
            Request::SubmitAttempt(r) => {
                let generation = r.payload.generation;
                let job = s.jobs.entry(r.attempt_id.clone()).or_insert(Job {
                    payload: r.payload,
                    state: AttemptState::Accepted,
                    termination: None,
                    out: Out::default(),
                    acks: Vec::new(),
                });
                Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    generation,
                    state: job.state,
                    receipts: Receipts::default(),
                    duplicate: false,
                }))
            }
            Request::RenewLease(r) => Ok(Response::RenewLease(RenewLeaseResponse {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: r.attempt_id.clone(),
                lease_seq: r.lease_seq,
                extended: true,
                lease_remaining_ms: r.lease_ms,
                state: s.jobs[&r.attempt_id].state,
            })),
            Request::GetResult(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let job = &s.jobs[&r.attempt_id];
                let stored = job.out.has_result;
                Ok(Response::GetResult(GetResultResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id.clone(),
                    generation: r.generation,
                    termination: job.termination.unwrap(),
                    nodes: vec![],
                    artifacts: if stored {
                        job.out.artifacts.iter().map(|(r, _)| r.clone()).collect()
                    } else {
                        vec![]
                    },
                    artifacts_not_stored: if stored {
                        job.out.not_stored.clone()
                    } else {
                        vec![]
                    },
                    result_digest: digest_of(job),
                }))
            }
            Request::ReadLog(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let out = &s.jobs[&r.attempt_id].out;
                let log: &[u8] = match r.stream {
                    LogStream::Stdout => &out.log,
                    LogStream::Stderr => &[],
                };
                let from = (r.offset as usize).min(log.len());
                let to = (from + r.max_bytes.max(1) as usize).min(log.len());
                Ok(Response::ReadLog(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: r.offset,
                    data: Bytes(log[from..to].to_vec()),
                    next_offset: to as u64,
                    eof: out.log_closed && to == log.len(),
                    log_truncated: out.log_closed && out.dropped > 0,
                    dropped_bytes: if out.log_closed { out.dropped } else { 0 },
                }))
            }
            Request::BlobGet(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let flip = s.corrupt_chunks > 0;
                if flip && s.corrupt_chunks != u32::MAX {
                    s.corrupt_chunks -= 1;
                }
                let shift = std::mem::take(&mut s.shift_chunk);
                let out = &s.jobs[&r.attempt_id].out;
                let found = out.artifacts.iter().find(|(a, _)| a.digest == r.digest);
                let Some((record, bytes)) = found.filter(|_| out.has_result) else {
                    return Err(err(ErrorCode::NotFound));
                };
                if r.offset > record.size {
                    return Err(err(ErrorCode::InvalidInput));
                }
                let from = r.offset as usize;
                let to = (from + r.max_bytes.max(1) as usize).min(bytes.len());
                let mut data = bytes[from..to].to_vec();
                if flip && !data.is_empty() {
                    data[0] ^= 0xff;
                }
                Ok(Response::BlobGet(BlobGetResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    offset: r.offset + u64::from(shift),
                    total_size: record.size,
                    data: Bytes(data),
                    eof: to == bytes.len(),
                }))
            }
            Request::AckCollected(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let appears = s.result_appears_on_ack;
                let job = s.jobs.get_mut(&r.attempt_id).unwrap();
                if appears && !job.out.has_result {
                    job.out.has_result = true;
                    job.out.log_closed = true;
                }
                if digest_of(job) != r.result_digest {
                    return Err(err(ErrorCode::Conflict));
                }
                job.acks.push(r.result_digest);
                Ok(Response::AckCollected(AckCollectedResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    released: true,
                }))
            }
            // It has no estimate to give: the scheduler then places by what
            // the parent itself holds.
            Request::Forecast(_) => Err(err(ErrorCode::Internal)),
            other => panic!("not sent to this worker: {other:?}"),
        }
    }
}

fn kind_of(request: &Request) -> (&'static str, u64) {
    match request {
        Request::Inspect(_) => ("inspect", 0),
        Request::GetAttempt(_) => ("get", 0),
        Request::SubmitAttempt(_) => ("submit", 0),
        Request::RenewLease(_) => ("renew", 0),
        Request::GetResult(_) => ("result", 0),
        Request::ReadLog(r) => ("log", r.offset),
        Request::BlobGet(r) => ("blob", r.offset),
        Request::AckCollected(_) => ("ack", 0),
        Request::Forecast(_) => ("forecast", 0),
        other => panic!("{other:?}"),
    }
}

impl WorkerLink for Worker {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let op = kind_of(&request);
            let lost = {
                let mut s = self.state.lock().unwrap();
                if s.offline {
                    Some(Lost::Request)
                } else {
                    s.ops.push(op);
                    let seen = s.ops.iter().filter(|(k, _)| *k == op.0).count();
                    match s.trip {
                        Some((kind, nth, lost)) if kind == op.0 && nth == seen => {
                            s.trip = None;
                            s.offline = true;
                            Some(lost)
                        }
                        _ => None,
                    }
                }
            };
            match lost {
                Some(Lost::Request) => Err(LinkError::Unconfirmed),
                Some(Lost::Answer) => {
                    let _ = self.handle(request);
                    Err(LinkError::Unconfirmed)
                }
                None => {
                    let refused = {
                        let mut s = self.state.lock().unwrap();
                        match s.fail {
                            Some((kind, code, left)) if kind == op.0 && left > 0 => {
                                s.fail = Some((kind, code, left - 1));
                                Some(code)
                            }
                            _ => None,
                        }
                    };
                    match refused {
                        Some(code) => Err(LinkError::Remote(err(code))),
                        None => self.handle(request).map_err(LinkError::Remote),
                    }
                }
            }
        })
    }
}

// ------------------------------------------------------------ the fixture

struct Fx {
    dir: TempDir,
    w: Arc<Worker>,
}

fn fx() -> Fx {
    Fx {
        dir: tempfile::tempdir().unwrap(),
        w: Worker::new("w-a"),
    }
}

/// Small chunks, so that every artifact and log takes several requests.
fn cfg() -> CollectConfig {
    CollectConfig {
        chunk_bytes: 4,
        budget_ms: 10_000,
        // The passes of a test follow each other at once.
        backoff_ms: 0,
        ..CollectConfig::default()
    }
}

impl Fx {
    /// Start a coordinator and open the result store on the state directory:
    /// the first, or the next after the one before was dropped.
    fn open(&self) -> (Coordinator, Arc<ResultStore>) {
        let started = std::time::Instant::now();
        let store = loop {
            match CoordinatorStore::open(self.dir.path(), 1) {
                Err(StorageError::Locked(_)) if started.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                other => break other.unwrap(),
            }
        };
        let cfg = CoordinatorConfig {
            limits: Limits {
                max_active_task: 8,
                prefetch_per_worker: 8,
            },
            lease_ms: 60_000,
            worker_budget_ms: 10_000,
        };
        let c = Coordinator::start(store, cfg).unwrap();
        c.attach(
            self.w.id.clone(),
            Arc::clone(&self.w) as Arc<dyn WorkerLink>,
        );
        let files = ResultStore::open(&self.dir.path().join("results")).unwrap();
        (c, Arc::new(files))
    }
}

fn decl(name: &str, if_missing: MissingPolicy) -> ArtifactDecl {
    ArtifactDecl {
        name: name.into(),
        path: format!("out/{name}"),
        if_missing,
    }
}

fn spec(artifacts: Vec<ArtifactDecl>) -> TaskSpec {
    TaskSpec {
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::of_bytes(b"manifest"),
        commands: vec![CommandSpec {
            setup_step: None,
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
        artifacts,
    }
}

/// A stored artifact of an attempt, as the worker records it.
fn stored(
    a: &AttemptId,
    p: &AttemptPayload,
    name: &str,
    bytes: &[u8],
) -> (ArtifactRecord, Vec<u8>) {
    let record = ArtifactRecord {
        name: name.into(),
        path: name.into(),
        size: bytes.len() as u64,
        digest: Digest::of_bytes(bytes),
        source_id: p.source_id.clone(),
        plan_digest: p.plan_digest.clone(),
        attempt_id: a.clone(),
        profile_fingerprint: Digest::of_bytes(b"profile"),
    };
    (record, bytes.to_vec())
}

fn not_stored(d: &ArtifactDecl, reason: NotStoredReason) -> ArtifactNotStored {
    ArtifactNotStored {
        name: d.name.clone(),
        path: d.path.clone(),
        if_missing: d.if_missing,
        reason,
    }
}

/// Queue one task, let the worker take it, and return its attempt (which
/// still runs).
async fn placed(
    fx: &Fx,
    c: &Coordinator,
    artifacts: Vec<ArtifactDecl>,
) -> (RunId, TaskId, AttemptId) {
    let (run, task) = (RunId::generate(), TaskId::generate());
    let source = SourceId::generate();
    c.store()
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    c.enqueue(run.clone(), source, vec![(task.clone(), spec(artifacts))])
        .await
        .unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    assert!(fx.w.count("submit") >= 1);
    (run, task, report.placed[0].clone())
}

/// The attempt ends on the worker with `out` and the parent sees it.
async fn end(fx: &Fx, c: &Coordinator, a: &AttemptId, outcome: Outcome, out: Out) {
    fx.w.finish(a, outcome, out);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![a.clone()]);
}

const ALPHA: &[u8] = b"0123456789";
const BETA: &[u8] = b"abcdef";
const LOG: &[u8] = b"hello log\n";

fn two() -> Vec<ArtifactDecl> {
    vec![
        decl("alpha.txt", MissingPolicy::Required),
        decl("reports/beta.bin", MissingPolicy::Required),
    ]
}

/// A result with both artifacts of [`two`] and a closed log.
fn full(fx: &Fx, a: &AttemptId) -> Out {
    let p = fx.w.payload(a);
    Out {
        has_result: true,
        artifacts: vec![
            stored(a, &p, "alpha.txt", ALPHA),
            stored(a, &p, "reports/beta.bin", BETA),
        ],
        not_stored: vec![],
        log: LOG.to_vec(),
        log_closed: true,
        dropped: 0,
    }
}

async fn collect(c: &Coordinator, files: &Arc<ResultStore>) -> CollectReport {
    c.collect(files, cfg()).await.unwrap()
}

fn is_collected(c: &Coordinator, a: &AttemptId) -> bool {
    let rec = c.store().get_attempt(a).unwrap().unwrap();
    rec.receipts.reached(Receipt::Collected)
}

/// The published bytes of the artifact `name`: the result that was collected
/// says which digest the name has, and the file is stored under the digest.
fn published(files: &ResultStore, a: &AttemptId, name: &str) -> Option<Vec<u8>> {
    let result: GetResultResponse =
        serde_json::from_slice(&files.read_result(a).unwrap()?).unwrap();
    let record = result.artifacts.iter().find(|r| r.name == name)?;
    std::fs::read(files.artifact_path(a, &record.digest)).ok()
}

/// The names of the artifacts the parent recorded as verified and published.
fn persisted(c: &Coordinator, a: &AttemptId) -> Vec<String> {
    let all = c.store().persisted_artifacts(a).unwrap();
    all.into_iter().map(|(name, _)| name).collect()
}

fn hex(bytes: &[u8]) -> String {
    let digest = Digest::of_bytes(bytes);
    digest
        .as_str()
        .trim_start_matches(Digest::PREFIX)
        .to_string()
}

fn strikes(c: &Coordinator, a: &AttemptId) -> u32 {
    c.store().collection(a).unwrap().map_or(0, |r| r.strikes)
}

/// Change the parent's database behind its back.
fn sql(fx: &Fx, statement: &str) {
    let conn = rusqlite::Connection::open(fx.dir.path().join("coordinator.db")).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(conn.execute(statement, []).unwrap(), 1, "{statement}");
}

/// Everything of [`full`] is in the parent, verified, and the worker was
/// told so for the result it holds.
fn assert_all_here(fx: &Fx, c: &Coordinator, files: &ResultStore, a: &AttemptId) {
    assert_eq!(published(files, a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(
        published(files, a, "reports/beta.bin").as_deref(),
        Some(BETA)
    );
    assert_eq!(files.read_log(a, LogStream::Stdout).unwrap(), LOG);
    assert!(files.read_result(a).unwrap().is_some());
    assert_eq!(
        persisted(c, a),
        vec!["alpha.txt".to_string(), "reports/beta.bin".to_string()]
    );
    // Durable as recorded: the logs with their length and digest.
    let logs = c.store().collection(a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["sha256"], Digest::of_bytes(LOG).as_str());
    assert_eq!(logs["stderr"]["sha256"], Digest::of_bytes(b"").as_str());
    let dir = files.root().join(a.as_str()).join("artifacts");
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 2);
    let row = c.store().collection(a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    assert!(row.acked_at_ms.is_some() && row.closed_at_ms.is_some());
    assert_eq!(row.result_digest, Some(fx.w.digest(a)));
    assert!(is_collected(c, a));
    assert!(fx.w.acks(a).iter().all(|d| d == &fx.w.digest(a)));
    assert!(!fx.w.acks(a).is_empty());
    // Nothing of an unfinished transfer is left behind.
    let tmp = files.root().join(a.as_str()).join("tmp");
    let left = std::fs::read_dir(tmp).map_or(0, |d| d.count());
    assert_eq!(left, 0);
}

// ------------------------------------------------------------ the path

#[tokio::test]
async fn the_outputs_are_fetched_verified_published_and_acknowledged_last() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    // While it runs there is nothing to collect.
    assert_eq!(collect(&c, &files).await, CollectReport::default());
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;

    // Ended on the worker and not collected: the run shows `collecting`.
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    let display = RunDisplay::derive(
        rec.state,
        rec.termination.as_ref(),
        rec.observation,
        Collection::Pending,
    );
    assert_eq!(display, RunDisplay::Collecting);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Collecting);
    assert_eq!(standing.tasks, vec![(task.clone(), TaskStanding::Pending)]);

    let report = collect(&c, &files).await;
    assert_eq!(report.collected, vec![a.clone()]);
    assert!(report.pending.is_empty() && report.gave_up.is_empty());
    assert_all_here(&fx, &c, &files, &a);

    // One read of the result; the bytes in chunks from where they were; the
    // acknowledgement once, after everything else.
    let ops: Vec<&str> = {
        let s = fx.w.state.lock().unwrap();
        let after = s.ops.iter().skip_while(|(k, _)| *k != "result");
        after.map(|(k, _)| *k).collect()
    };
    assert_eq!(ops.first(), Some(&"result"));
    assert_eq!(ops.last(), Some(&"ack"));
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.count("ack"), 1);
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4]);
    assert_eq!(fx.w.offsets("log"), vec![0, 4, 8, 0]);
    let row = c.store().collection(&a).unwrap().unwrap();
    let logs = row.logs.unwrap();
    assert_eq!(logs["stdout"]["bytes"], 10);
    assert_eq!(logs["stdout"]["complete"], true);
    assert_eq!(logs["stdout"]["log_truncated"], false);
    assert_eq!(logs["stderr"]["bytes"], 0);

    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Passed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Succeeded)]);

    // A closed collection asks the worker for nothing more.
    let before = fx.w.state.lock().unwrap().ops.len();
    assert_eq!(collect(&c, &files).await, CollectReport::default());
    assert_eq!(fx.w.state.lock().unwrap().ops.len(), before);
    assert_eq!(fx.w.count("submit"), 1);
}

#[tokio::test]
async fn a30_a_worker_that_disappears_while_collecting_is_continued_where_it_stopped() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // The second chunk of the first artifact never arrives.
    fx.w.set(|s| s.trip = Some(("blob", 2, Lost::Request)));

    for _ in 0..3 {
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()]);
        assert!(report.collected.is_empty() && report.gave_up.is_empty());
    }
    // Collecting is kept; nothing partial is visible; nothing was given up.
    assert!(!is_collected(&c, &a));
    assert_eq!(published(&files, &a, "alpha.txt"), None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.strikes, row.closed_at_ms), (0, None));
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Collecting
    );
    // A tick in between changes nothing about the ended attempt.
    assert_eq!(c.tick().await.unwrap().placed, vec![]);

    fx.w.set(|s| s.offline = false);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // The first artifact continued at byte 4; the result was read once and
    // the command ran once.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 4, 8, 0, 4]);
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.count("submit"), 1);
    assert_eq!(fx.w.acks(&a).len(), 1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    ResultFetched,
    SomeArtifactsPublished,
    AllPublishedNotAcked,
    AckedNotRecorded,
}

#[tokio::test]
async fn a_parent_that_crashes_at_any_boundary_continues_the_collection_and_runs_nothing() {
    use Boundary::*;
    for boundary in [
        ResultFetched,
        SomeArtifactsPublished,
        AllPublishedNotAcked,
        AckedNotRecorded,
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        let trip = match boundary {
            ResultFetched => ("log", 1, Lost::Request),
            // The first artifact takes three chunks.
            SomeArtifactsPublished => ("blob", 4, Lost::Request),
            AllPublishedNotAcked => ("ack", 1, Lost::Request),
            AckedNotRecorded => ("ack", 1, Lost::Answer),
        };
        fx.w.set(|s| s.trip = Some(trip));
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()], "{boundary:?}");

        // What is durable at the boundary.
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.result_digest, Some(fx.w.digest(&a)), "{boundary:?}");
        assert_eq!(row.closed_at_ms, None, "{boundary:?}");
        assert!(!is_collected(&c, &a), "{boundary:?}");
        let names = persisted(&c, &a);
        let acks = fx.w.acks(&a).len();
        match boundary {
            ResultFetched => {
                assert!(files.read_result(&a).unwrap().is_some());
                assert_eq!((names.len(), acks), (0, 0));
            }
            SomeArtifactsPublished => {
                assert_eq!(names, vec!["alpha.txt".to_string()]);
                assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
                assert_eq!(published(&files, &a, "reports/beta.bin"), None);
                assert_eq!(acks, 0);
            }
            AllPublishedNotAcked => assert_eq!((names.len(), acks), (2, 0)),
            AckedNotRecorded => assert_eq!((names.len(), acks), (2, 1)),
        }

        // The parent is gone; the next one finds the worker again.
        drop(c);
        drop(files);
        fx.w.set(|s| s.offline = false);
        let (c, files) = fx.open();
        assert_eq!(
            collect(&c, &files).await.collected,
            vec![a.clone()],
            "{boundary:?}"
        );
        assert_all_here(&fx, &c, &files, &a);
        // Only the collection was continued: the result is not read again,
        // no byte is fetched twice, and no command ran again.
        assert_eq!(fx.w.count("result"), 1, "{boundary:?}");
        // (The request the worker was lost on is the one that is repeated.)
        let blobs = match boundary {
            SomeArtifactsPublished => vec![0, 4, 8, 0, 0, 4],
            _ => vec![0, 4, 8, 0, 4],
        };
        assert_eq!(fx.w.offsets("blob"), blobs, "{boundary:?}");
        assert_eq!(fx.w.count("submit"), 1, "{boundary:?}");
        // An acknowledgement whose answer was lost is sent again, and the
        // worker takes the duplicate.
        let twice = boundary == AckedNotRecorded;
        assert_eq!(
            fx.w.acks(&a).len(),
            if twice { 2 } else { 1 },
            "{boundary:?}"
        );
        // And once more changes nothing.
        assert_eq!(collect(&c, &files).await, CollectReport::default());
    }
}

#[tokio::test]
async fn a_corrupted_chunk_is_never_published_and_the_artifact_is_fetched_again() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.corrupt_chunks = 1);

    let report = collect(&c, &files).await;
    assert_eq!(report.pending, vec![a.clone()]);
    // The bytes did not have the recorded digest: nothing is visible, the
    // worker was not told anything, and the fault is counted.
    assert_eq!(published(&files, &a, "alpha.txt"), None);
    assert!(persisted(&c, &a).is_empty());
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 1);

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // The whole artifact again, from its first byte.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4, 8, 0, 4]);
}

#[tokio::test]
async fn a_chunk_from_another_offset_is_not_written() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| {
        s.trip = Some(("blob", 2, Lost::Request));
    });
    collect(&c, &files).await;
    // Four bytes are here. The next chunk claims to be from byte 5.
    fx.w.set(|s| {
        s.offline = false;
        s.shift_chunk = true;
    });
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 1);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // Byte 4 was asked for again: what the bad chunk carried was dropped.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 4, 4, 8, 0, 4]);
}

#[tokio::test]
async fn bytes_that_never_have_their_digest_end_the_collection_and_run_nothing_again() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    let mut out = full(&fx, &a);
    // The worker serves other bytes than it recorded.
    out.artifacts[1].1 = b"ABCDEF".to_vec();
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let report = collect(&c, &files).await;
    assert_eq!(report.gave_up, vec![a.clone()]);

    // The artifact that verified is kept; the other never appeared.
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(published(&files, &a, "reports/beta.bin"), None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.verdict, row.strikes), (Some(Verdict::GaveUp), 3));
    assert_eq!(row.acked_at_ms, None);
    // The worker keeps what it has: it was not acknowledged.
    assert!(fx.w.acks(&a).is_empty());
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    assert!(rec.collection_failed_at_ms.is_some());
    assert!(!rec.receipts.reached(Receipt::Collected));
    // Not a success, and not a reason to run the command again.
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Failed);
    assert_eq!(
        standing.tasks,
        vec![(task.clone(), TaskStanding::CollectionFailed)]
    );
    assert_eq!(c.tick().await.unwrap().placed, vec![]);
    assert_eq!(c.store().attempts_of_task(&task).unwrap().len(), 1);
    assert_eq!(fx.w.count("submit"), 1);
    assert_eq!(collect(&c, &files).await, CollectReport::default());
}

// ------------------------------------------------------------ the order

#[tokio::test]
async fn a_result_that_is_stored_after_it_was_read_is_collected_before_it_is_acknowledged() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    // The attempt is terminal and has no result yet: its termination only,
    // and a spool that is not closed.
    let mut out = full(&fx, &a);
    out.has_result = false;
    out.log_closed = false;
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    let bare = fx.w.digest(&a);
    // The result is stored the moment the acknowledgement arrives.
    fx.w.set(|s| s.result_appears_on_ack = true);

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    // The result that was read first was never acknowledged; the one the
    // worker holds was collected whole and acknowledged.
    let real = fx.w.digest(&a);
    assert_ne!(bare, real);
    assert_eq!(fx.w.acks(&a), vec![real]);
    assert_eq!(fx.w.count("ack"), 2);
    assert_eq!(fx.w.count("result"), 2);
    assert_all_here(&fx, &c, &files, &a);
    let logs = c.store().collection(&a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["complete"], true);
}

#[tokio::test]
async fn an_attempt_without_a_result_is_closed_with_what_there_is_and_is_no_success() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    let out = Out {
        log: LOG.to_vec(),
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Interrupted, out).await;

    assert_eq!(collect(&c, &files).await.required_missing, vec![a.clone()]);
    // The log is taken as far as it goes and marked incomplete.
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), LOG);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.logs.unwrap()["stdout"]["complete"], false);
    assert_eq!(row.verdict, Some(Verdict::RequiredMissing));
    assert_eq!(fx.w.acks(&a), vec![fx.w.digest(&a)]);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Failed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Failed)]);
}

#[tokio::test]
async fn the_logs_are_complete_only_at_their_end_mark_which_carries_the_truncation() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    // Terminal, with a result, and the end of the log is not there yet.
    let out = Out {
        has_result: true,
        log: LOG.to_vec(),
        log_closed: false,
        dropped: 7,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    for _ in 0..4 {
        assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    }
    // Every byte is here, and still nothing is called complete: no
    // acknowledgement, no strike, no closed collection.
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), LOG);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.logs, row.strikes, row.closed_at_ms), (None, 0, None));
    assert!(fx.w.acks(&a).is_empty());

    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.log_closed = true);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    let logs = c.store().collection(&a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["complete"], true);
    assert_eq!(logs["stdout"]["log_truncated"], true);
    assert_eq!(logs["stdout"]["dropped_bytes"], 7);
    assert_eq!(logs["stdout"]["bytes"], 10);
    assert_eq!(fx.w.acks(&a).len(), 1);
}

// ------------------------------------------------------- required, optional

#[tokio::test]
async fn an_optional_artifact_that_is_missing_or_refused_changes_nothing_and_is_not_asked_for() {
    let fx = fx();
    let (c, files) = fx.open();
    let decls = vec![
        decl("alpha.txt", MissingPolicy::Required),
        decl("gone.txt", MissingPolicy::Optional),
        decl("secret.txt", MissingPolicy::Optional),
    ];
    let (run, task, a) = placed(&fx, &c, decls.clone()).await;
    let p = fx.w.payload(&a);
    let out = Out {
        has_result: true,
        artifacts: vec![stored(&a, &p, "alpha.txt", ALPHA)],
        not_stored: vec![
            not_stored(&decls[1], NotStoredReason::Missing),
            not_stored(&decls[2], NotStoredReason::Refused),
        ],
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 0);
    // Only the stored artifact was fetched: three chunks, nothing else.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8]);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Passed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Succeeded)]);
}

#[tokio::test]
async fn a_required_artifact_that_is_missing_or_refused_fails_the_task_once_and_for_all() {
    for reason in [NotStoredReason::Missing, NotStoredReason::Refused] {
        let fx = fx();
        let (c, files) = fx.open();
        let (run, task, a) = placed(&fx, &c, two()).await;
        let p = fx.w.payload(&a);
        let out = Out {
            has_result: true,
            artifacts: vec![stored(&a, &p, "alpha.txt", ALPHA)],
            not_stored: vec![not_stored(&two()[1], reason)],
            log_closed: true,
            ..Out::default()
        };
        end(&fx, &c, &a, Outcome::Succeeded, out).await;

        let report = collect(&c, &files).await;
        assert_eq!(report.required_missing, vec![a.clone()], "{reason:?}");
        assert!(report.collected.is_empty() && report.gave_up.is_empty());
        // What exists was collected and the worker was told so: there is
        // nothing more to wait for, and nothing is asked for again.
        assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
        assert_eq!(fx.w.acks(&a).len(), 1, "{reason:?}");
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.verdict, Some(Verdict::RequiredMissing));
        assert_eq!(row.strikes, 0, "{reason:?}");
        // The command succeeded; the task did not.
        let rec = c.store().get_attempt(&a).unwrap().unwrap();
        assert!(!rec.receipts.reached(Receipt::Collected));
        assert!(rec.collection_failed_at_ms.is_some());
        let standing = c.store().run_standing(&run, None).unwrap();
        assert_eq!(standing.verdict, RunVerdict::Failed);
        assert_eq!(
            standing.tasks,
            vec![(task.clone(), TaskStanding::RequiredArtifactMissing)]
        );
        let ops = fx.w.state.lock().unwrap().ops.len();
        assert_eq!(collect(&c, &files).await, CollectReport::default());
        assert_eq!(fx.w.state.lock().unwrap().ops.len(), ops, "{reason:?}");
        assert_eq!(c.store().attempts_of_task(&task).unwrap().len(), 1);
    }
}

// ------------------------------------------------------- what is not trusted

#[tokio::test]
async fn a_result_that_names_a_path_outside_the_store_is_refused_whole() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    let p = fx.w.payload(&a);
    let mut out = full(&fx, &a);
    let (mut record, bytes) = stored(&a, &p, "alpha.txt", b"x");
    record.name = "../../escaped".into();
    record.path = record.name.clone();
    out.artifacts.push((record, bytes));
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    // Not a byte was asked for or written.
    assert_eq!(fx.w.count("blob"), 0);
    assert_eq!(fx.w.count("log"), 0);
    assert!(!fx.dir.path().join("escaped").exists());
    assert!(!files.root().join(a.as_str()).exists());
    assert!(fx.w.acks(&a).is_empty());
}

#[tokio::test]
async fn an_artifact_the_parent_did_not_declare_is_not_fetched() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![decl("alpha.txt", MissingPolicy::Required)]).await;
    let p = fx.w.payload(&a);
    let out = Out {
        has_result: true,
        artifacts: vec![
            stored(&a, &p, "alpha.txt", ALPHA),
            stored(&a, &p, "extra.bin", BETA),
        ],
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(published(&files, &a, "extra.bin"), None);
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8]);
}

#[tokio::test]
async fn a_result_of_another_termination_than_the_attempts_is_not_recorded() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    let out = Out {
        has_result: true,
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    // The worker now says the attempt failed.
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().termination = Some(ended(Outcome::Failed)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.result_digest, row.strikes), (None, 1));
    assert!(fx.w.acks(&a).is_empty());
}

// ------------------------------------------------------- without a worker

#[tokio::test]
async fn an_attempt_the_parent_settled_itself_is_closed_without_asking_a_worker() {
    let fx = fx();
    let (c, files) = fx.open();
    // The submit never arrives, and the worker is gone.
    fx.w.set(|s| s.trip = Some(("submit", 1, Lost::Request)));
    let (run, task) = (RunId::generate(), TaskId::generate());
    let source = SourceId::generate();
    c.store()
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    let optional = vec![decl("alpha.txt", MissingPolicy::Optional)];
    c.enqueue(run.clone(), source, vec![(task.clone(), spec(optional))])
        .await
        .unwrap();
    c.tick().await.unwrap();
    let a = c.store().attempts_of_task(&task).unwrap()[0]
        .attempt_id
        .clone();
    let seen = Seen {
        state: AttemptState::Terminal,
        termination: Some(ended(Outcome::Cancelled)),
        receipts: Receipts::default(),
    };
    c.store()
        .record_seen(&a, false, Some(seen), Observation::Known, wall_ms())
        .unwrap();

    let asked = fx.w.state.lock().unwrap().ops.len();
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(fx.w.state.lock().unwrap().ops.len(), asked);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    assert_eq!(row.acked_at_ms, None);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Cancelled)]);
    assert_eq!(standing.verdict, RunVerdict::Failed);
}

// ------------------------------------------------------- A31

#[tokio::test]
async fn a31_an_empty_or_short_set_of_results_is_told_apart_and_never_a_success() {
    let fx = fx();
    let (c, files) = fx.open();
    // A run whose filter left nothing to do.
    let empty = RunId::generate();
    c.store()
        .create_run(&empty, None, None, &json!({}), 1)
        .unwrap();
    let standing = c.store().run_standing(&empty, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::NoWork);
    assert!(standing.tasks.is_empty());
    // The same run when a task was expected: its result is missing.
    assert_eq!(
        c.store().run_standing(&empty, Some(1)).unwrap().verdict,
        RunVerdict::Missing
    );

    let (run, _, a) = placed(&fx, &c, vec![]).await;
    let out = Out {
        has_result: true,
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a]);
    let s = c.store();
    assert_eq!(
        s.run_standing(&run, None).unwrap().verdict,
        RunVerdict::Passed
    );
    assert_eq!(
        s.run_standing(&run, Some(1)).unwrap().verdict,
        RunVerdict::Passed
    );
    // Two were expected and one is there: missing, whatever the one says.
    assert_eq!(
        s.run_standing(&run, Some(2)).unwrap().verdict,
        RunVerdict::Missing
    );
    assert!(matches!(
        s.run_standing(&RunId::generate(), None),
        Err(StorageError::NotFound { .. })
    ));
}

// ------------------------------------------------------- retention

#[tokio::test]
async fn collected_results_are_kept_for_the_retention_and_then_deleted() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // A second attempt whose collection is cut off after its result.
    let (_, _, b) = placed(&fx, &c, two()).await;
    // Collected a moment after it ended.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
    fx.w.set(|s| s.trip = Some(("log", 5, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![b.clone()]);

    // The retention runs from the later of the end and the collection.
    let terminal = c.store().get_attempt(&a).unwrap().unwrap();
    let terminal = terminal.terminal_at_ms.unwrap();
    let ended = c.store().collection(&a).unwrap().unwrap();
    let ended = ended.closed_at_ms.unwrap();
    assert!(ended > terminal);
    let report = sweep(c.store(), &files, ended + 7 * DAY - 1, 7).unwrap();
    assert!(report.deleted.is_empty());
    assert_all_here(&fx, &c, &files, &a);

    let report = sweep(c.store(), &files, ended + 7 * DAY, 7).unwrap();
    let mut deleted = report.deleted.clone();
    deleted.sort();
    assert_eq!(
        deleted,
        vec![
            format!("results/{a}/artifacts/{}", hex(ALPHA)),
            format!("results/{a}/artifacts/{}", hex(BETA)),
            format!("results/{a}/logs"),
            format!("results/{a}/result.json"),
        ]
    );
    assert!(!files.root().join(a.as_str()).exists());
    assert!(c.store().pending_deletes().unwrap().is_empty());
    // What still waits for its collection is not touched, however old.
    assert!(files.read_result(&b).unwrap().is_some());
    // The verdict outlives the files.
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    // A sweep with nothing to do does nothing.
    let again = sweep(c.store(), &files, ended + 31 * DAY, 7).unwrap();
    assert!(again.deleted.is_empty());

    fx.w.set(|s| s.offline = false);
    assert_eq!(collect(&c, &files).await.collected, vec![b.clone()]);
    assert_all_here(&fx, &c, &files, &b);
}

// ------------------------------------------- nothing is acknowledged early

#[tokio::test]
async fn a_parent_that_dies_between_a_file_operation_and_its_record_continues_from_the_files() {
    use remote_task_runner::results::store::Crash;
    for point in [
        Crash::ResultWritten,
        Crash::LogAppended,
        Crash::ChunkAppended,
        Crash::ArtifactPublished,
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        files.crash_once_at(point);
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()], "{point:?}");
        assert_eq!(report.faults.len(), 1, "{point:?}");
        assert_eq!(report.faults[0].0, a, "{point:?}");
        // The file operation happened, its record did not, and the worker
        // was told nothing.
        assert!(fx.w.acks(&a).is_empty(), "{point:?}");
        assert!(!is_collected(&c, &a), "{point:?}");
        assert_eq!(strikes(&c, &a), 0, "{point:?}");
        match point {
            Crash::ResultWritten => {
                assert!(files.read_result(&a).unwrap().is_some());
                assert_eq!(c.store().collection(&a).unwrap(), None);
            }
            Crash::ArtifactPublished => {
                assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
                assert!(persisted(&c, &a).is_empty());
            }
            _ => assert!(persisted(&c, &a).is_empty()),
        }

        drop(c);
        drop(files);
        let (c, files) = fx.open();
        assert_eq!(
            collect(&c, &files).await.collected,
            vec![a.clone()],
            "{point:?}"
        );
        assert_all_here(&fx, &c, &files, &a);
        assert_eq!(fx.w.acks(&a).len(), 1, "{point:?}");
        assert_eq!(fx.w.count("submit"), 1, "{point:?}");
        if point == Crash::ArtifactPublished {
            // The file that was published is verified again, not fetched.
            assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4], "{point:?}");
        }
    }
}

#[tokio::test]
async fn what_is_not_on_disk_as_recorded_is_collected_again_before_the_acknowledgement() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(persisted(&c, &a).len(), 2);

    // Everything is recorded as collected; then the disk loses the result
    // and one artifact, and damages the log and the other artifact.
    let dir = files.root().join(a.as_str());
    std::fs::remove_file(dir.join("result.json")).unwrap();
    std::fs::write(dir.join("logs").join("stdout.log"), b"HELLO log\n").unwrap();
    std::fs::remove_file(dir.join("artifacts").join(hex(ALPHA))).unwrap();
    std::fs::write(dir.join("artifacts").join(hex(BETA)), b"abcdeX").unwrap();
    fx.w.set(|s| s.offline = false);
    let (blobs, logs) = (fx.w.count("blob"), fx.w.count("log"));

    // The acknowledgement is not sent for what is not there: it is fetched
    // again first.
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(fx.w.count("blob"), blobs + 5);
    assert!(fx.w.count("log") > logs);
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.acks(&a).len(), 1);
    assert_eq!(strikes(&c, &a), 0);
    let events = c.store().events_of(&a, "collection_recollect").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["logs"], true);
    assert_eq!(
        events[0].payload["artifacts"],
        json!(["alpha.txt", "reports/beta.bin"])
    );
}

#[tokio::test]
async fn an_attempt_whose_payload_cannot_be_read_is_never_acknowledged_and_keeps_nothing_else_back()
{
    for broken in [
        "json_remove(demand_json, '$.payload')",
        "json_set(demand_json, '$.payload', 'junk')",
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        // The older attempt is the broken one: it comes first in every pass.
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        let (_, _, b) = placed(&fx, &c, two()).await;
        end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
        sql(
            &fx,
            &format!("UPDATE placement SET demand_json = {broken} WHERE attempt_id = '{a}'"),
        );
        for pass in 0..4 {
            let report = collect(&c, &files).await;
            assert_eq!(report.pending, vec![a.clone()], "{broken}");
            assert_eq!(report.faults.len(), 1, "{broken}");
            assert!(report.faults[0].1.contains("payload"), "{report:?}");
            let others = if pass == 0 { vec![b.clone()] } else { vec![] };
            assert_eq!(report.collected, others, "{broken}");
        }
        assert_all_here(&fx, &c, &files, &b);
        // "Nothing was declared" would have released the worker's
        // artifacts unfetched: nothing was asked, nothing was told.
        assert!(fx.w.acks(&a).is_empty(), "{broken}");
        assert_eq!(c.store().collection(&a).unwrap(), None, "{broken}");
        assert!(!files.root().join(a.as_str()).exists(), "{broken}");
        assert!(!is_collected(&c, &a), "{broken}");
    }
}

#[tokio::test]
async fn an_error_inside_the_worker_is_waited_out_however_often_it_comes() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    for (kind, code) in [
        ("result", ErrorCode::Internal),
        ("log", ErrorCode::Internal),
        ("blob", ErrorCode::Internal),
        ("blob", ErrorCode::InvalidInput),
        ("blob", ErrorCode::Conflict),
        // A supervisor of a result-less attempt may still store its result.
        ("ack", ErrorCode::Overloaded),
        ("ack", ErrorCode::Internal),
    ] {
        fx.w.set(|s| s.fail = Some((kind, code, 4)));
        for _ in 0..4 {
            let report = collect(&c, &files).await;
            assert_eq!(report.pending, vec![a.clone()], "{kind} {code:?}");
            assert!(report.faults.is_empty(), "{kind} {code:?}");
        }
        assert_eq!(strikes(&c, &a), 0, "{kind} {code:?}");
        assert!(fx.w.acks(&a).is_empty(), "{kind} {code:?}");
        let row = c.store().collection(&a).unwrap();
        assert!(row.is_none_or(|r| r.closed_at_ms.is_none()));
    }
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
}

#[tokio::test]
async fn a_worker_that_answered_with_an_error_is_given_time_and_one_that_was_away_is_not() {
    let fx = fx();
    let (c, files) = fx.open();
    let slow = CollectConfig {
        backoff_ms: 60_000,
        ..cfg()
    };
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    let ops = |fx: &Fx| fx.w.state.lock().unwrap().ops.len();
    // Away: asked again by the very next pass.
    fx.w.set(|s| s.trip = Some(("blob", 2, Lost::Request)));
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    fx.w.set(|s| {
        s.offline = false;
        s.fail = Some(("blob", ErrorCode::Internal, 1));
    });
    let before = ops(&fx);
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    assert!(ops(&fx) > before);
    // It answered with an error: the next pass leaves it alone.
    let before = ops(&fx);
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    assert_eq!(ops(&fx), before);
    // The wait is not durable: a parent that starts again asks at once.
    drop(c);
    drop(files);
    let (c, files) = fx.open();
    assert_eq!(
        c.collect(&files, slow).await.unwrap().collected,
        vec![a.clone()]
    );
    assert_all_here(&fx, &c, &files, &a);
}

#[tokio::test]
async fn strikes_start_over_with_verified_progress_and_a_worker_that_forgot_closes_the_collection()
{
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.shift_chunk = true);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 1);
    // An artifact is verified and published: what went wrong before it is
    // not held against the rest.
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Answer)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 0);
    assert_eq!(fx.w.acks(&a).len(), 1);

    // The worker took the acknowledgement, released, and has deleted the
    // attempt by the time the acknowledgement is sent again.
    for code in [ErrorCode::NotFound, ErrorCode::Retired] {
        let (c, files, a) = if code == ErrorCode::NotFound {
            (&c, &files, a.clone())
        } else {
            let (_, _, b) = placed(&fx, &c, two()).await;
            end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
            (&c, &files, b)
        };
        fx.w.set(|s| {
            s.offline = false;
            s.fail = Some(("ack", code, 1));
        });
        assert_eq!(collect(c, files).await.collected, vec![a.clone()]);
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.verdict, Some(Verdict::Complete), "{code:?}");
        assert!(row.closed_at_ms.is_some() && row.acked_at_ms.is_none());
        assert_eq!(row.strikes, 0, "{code:?}");
        assert!(is_collected(c, &a), "{code:?}");
        assert_eq!(published(files, &a, "alpha.txt").as_deref(), Some(ALPHA));
        assert_eq!(collect(c, files).await, CollectReport::default());
    }
}

#[tokio::test]
async fn a_part_a_dead_parent_left_torn_is_fetched_again_and_is_no_strike() {
    use remote_task_runner::results::store::Crash;
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    files.crash_once_at(Crash::ChunkAppended);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let part = files.root().join(a.as_str()).join("tmp");
    let part = part.join(format!("{}.part", hex(ALPHA)));
    assert_eq!(std::fs::read(&part).unwrap(), &ALPHA[..4]);
    drop(c);
    drop(files);
    // The power went with the parent: the tail of the file is not what was
    // written.
    std::fs::write(&part, b"01\0\0").unwrap();

    let (c, files) = fx.open();
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(strikes(&c, &a), 0);
    assert!(c
        .store()
        .events_of(&a, "collection_fault")
        .unwrap()
        .is_empty());
    // Continued at its length, found wrong when whole, fetched once more.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4, 8, 0, 4]);
}

#[tokio::test]
async fn a_collection_that_was_given_up_is_opened_again_while_the_worker_has_the_result() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, two()).await;
    let mut out = full(&fx, &a);
    out.artifacts[1].1 = b"ABCDEF".to_vec();
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    // An open collection is not one to reopen.
    assert!(!c.store().reopen_collection(&a, 1).unwrap());
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert!(!c.store().reopen_collection(&a, 1).unwrap());
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    // Nothing of the transfers that never finished is left.
    assert!(!files.root().join(a.as_str()).join("tmp").exists());
    assert!(!c
        .store()
        .reopen_collection(&AttemptId::generate(), 1)
        .unwrap());

    // The worker was not acknowledged, so it still has the result (for its
    // `ack_wait_ms`); now it serves the bytes it recorded.
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.artifacts[1].1 = BETA.to_vec());
    assert!(c.store().reopen_collection(&a, wall_ms()).unwrap());
    assert!(!c.store().reopen_collection(&a, wall_ms()).unwrap());
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    assert_eq!(rec.collection_failed_at_ms, None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(
        (
            row.closed_at_ms,
            row.verdict,
            row.strikes,
            row.result_digest
        ),
        (None, None, 0, None)
    );
    assert!(persisted(&c, &a).is_empty());
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Collecting
    );
    // The retention of what is here does not run while it is open.
    let report = sweep(c.store(), &files, wall_ms() + 30 * DAY, 7).unwrap();
    assert!(report.deleted.is_empty());

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(fx.w.count("result"), 2);
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Passed
    );
    // What the worker acknowledged is closed for good.
    assert!(!c.store().reopen_collection(&a, wall_ms()).unwrap());
    assert_eq!(
        c.store()
            .events_of(&a, "collection_reopened")
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn names_that_are_one_path_on_the_parent_are_each_collected() {
    let fx = fx();
    let (c, files) = fx.open();
    // The same name but for its case, a name that is a directory of
    // another, and two names with the same bytes: fine on the worker.
    let all: [(&str, &[u8]); 6] = [
        ("Report.txt", b"upper"),
        ("report.txt", b"lower"),
        ("a", b"a file"),
        ("a/b", b"below a"),
        ("same-1", b"same"),
        ("same-2", b"same"),
    ];
    let decls = all.iter().map(|(n, _)| decl(n, MissingPolicy::Required));
    let (_, _, a) = placed(&fx, &c, decls.collect()).await;
    let p = fx.w.payload(&a);
    let out = Out {
        has_result: true,
        artifacts: all.iter().map(|(n, b)| stored(&a, &p, n, b)).collect(),
        not_stored: vec![],
        log: LOG.to_vec(),
        log_closed: true,
        dropped: 0,
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    for (name, bytes) in all {
        assert_eq!(
            published(&files, &a, name).as_deref(),
            Some(bytes),
            "{name}"
        );
    }
    assert_eq!(persisted(&c, &a).len(), 6);
    assert!(is_collected(&c, &a));
    // One file per content, none under a name of the worker.
    let dir = files.root().join(a.as_str()).join("artifacts");
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut want: Vec<String> = ["upper", "lower", "a file", "below a", "same"]
        .iter()
        .map(|b| hex(b.as_bytes()))
        .collect();
    want.sort();
    assert_eq!(names, want);
    // And all of it goes with the retention.
    let closed = c.store().collection(&a).unwrap().unwrap();
    let closed = closed.closed_at_ms.unwrap();
    let report = sweep(c.store(), &files, closed + 7 * DAY, 7).unwrap();
    assert_eq!(report.deleted.len(), 7);
    assert!(!files.root().join(a.as_str()).exists());
    assert!(c.store().pending_deletes().unwrap().is_empty());
}

#[tokio::test]
async fn a_worker_that_no_longer_has_the_blob_ends_the_collection_after_three_answers() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // Asking again does not bring back what the worker deleted.
    fx.w.set(|s| s.fail = Some(("blob", ErrorCode::NotFound, 3)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.verdict, row.strikes), (Some(Verdict::GaveUp), 3));
    assert!(fx.w.acks(&a).is_empty());
    assert!(persisted(&c, &a).is_empty());
    assert_eq!(fx.w.count("blob"), 3);
}

#[tokio::test]
async fn a_name_that_is_published_is_not_fetched_over_with_other_bytes() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    // The receipt says `alpha.txt` was published with another digest than
    // the result records: an artifact never changes, so this is no reason
    // to fetch, and no state to acknowledge.
    let other = Digest::of_bytes(b"other");
    sql(
        &fx,
        &format!(
            "UPDATE artifact_receipt SET digest = '{}' WHERE attempt_id = '{a}' AND name = 'alpha.txt'",
            other.as_str()
        ),
    );
    fx.w.set(|s| s.offline = false);
    let blobs = fx.w.count("blob");
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 1);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    assert_eq!(fx.w.count("blob"), blobs);
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert!(!files.artifact_path(&a, &other).exists());
}
