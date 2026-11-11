//! Collecting the outputs of ended attempts from their workers (spec
//! chapter 13).
//!
//! For each terminal attempt whose collection is not closed, one pass takes
//! the steps [`plan::next_step`] names, each from what is durable in the
//! parent: read the result and record it with its digest, read the logs to
//! their end mark, fetch every stored artifact the parent declared, verify
//! and publish it, and only then send `ack_collected` for the recorded
//! digest. The collection is closed (and the attempt `collected`) in the
//! commit that records the worker's acknowledgement.
//!
//! Nothing here runs a command. A pass that is cut off anywhere (the parent
//! crashed, the link is gone, the time is up) is continued by the next pass
//! from the same durable facts: the logs from the bytes that are here, an
//! artifact from the bytes of its `.part` file.
//!
//! The acknowledgement is the last step and is never early: it releases the
//! worker's copy for good. Right before it, everything the collection
//! recorded is read back from the result store (the result is written
//! again from its record, each log and each artifact is hashed against its
//! recorded length and digest) and synced; what is not there as recorded is
//! collected again instead.
//!
//! What goes wrong with one attempt stays with that attempt: the pass goes
//! on with the others. Only what asking again cannot change counts toward
//! giving up ([`plan::judge_fault`]); everything else waits, a little
//! longer each time.
//!
//! The acknowledgement is by digest. A worker that holds another result
//! than the one that was read (an attempt that was settled without a result
//! can get one later) refuses it; the result is then read again and
//! collected from its start, so a result that was read while incomplete is
//! never acknowledged.

use super::plan::{
    self, AckRefusal, ChunkFacts, ChunkVerdict, Fault, FaultVerdict, LogFacts, LogVerdict, LogWait,
    Mismatch, Progress, RecordFacts, Reread, Standing, Step, Verdict,
};
use super::store::{ArtifactState, Publish, ResultStore, StoreError};
use crate::coordinator::{LinkError, WorkerLink};
use crate::ids::{AttemptId, Digest, NodeId, RequestId, WorkerId};
use crate::platform::wall_ms;
use crate::state::{Receipt, Termination};
use crate::storage::{
    ArtifactReceipt, AttemptRecord, BlobKind, Collectable, CoordinatorStore, ResultRef,
    StorageError,
};
use crate::supervisor::spool_plan::relative_path;
use crate::wire::{
    payload_hash, AckCollectedRequest, ArtifactRecord, AttemptPayload, BlobGetRequest,
    GetResultRequest, GetResultResponse, LogStream, MissingPolicy, ReadLogRequest, Request,
    Response, SchemaVersion,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectConfig {
    /// The most bytes asked for with one `blob_get` or `read_log`.
    pub chunk_bytes: u32,
    /// An artifact above this is not fetched.
    pub max_artifact_bytes: u64,
    /// The most bytes of one log stream that are kept.
    pub max_log_bytes: u64,
    /// The most attempts of one worker one pass looks at.
    pub scan: u32,
    /// How long one pass may take per worker.
    pub budget_ms: u64,
    /// How long a collection whose worker answered with an error (or whose
    /// own files failed) waits before its next try; doubled with every wait
    /// in a row, up to 64 times this.
    pub backoff_ms: u64,
}

impl Default for CollectConfig {
    fn default() -> Self {
        CollectConfig {
            chunk_bytes: 262_144,
            max_artifact_bytes: 1_073_741_824,
            max_log_bytes: 1_073_741_824,
            scan: 256,
            budget_ms: 10_000,
            backoff_ms: 1_000,
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectReport {
    /// Closed with every required output here.
    pub collected: Vec<AttemptId>,
    /// Closed, and a required artifact does not exist.
    pub required_missing: Vec<AttemptId>,
    /// Given up.
    pub gave_up: Vec<AttemptId>,
    /// Still to be continued.
    pub pending: Vec<AttemptId>,
    /// Of the pending ones: what the parent itself could not do (read the
    /// payload it sent, write its files or its records). They wait, are
    /// never acknowledged as they are, and keep nothing else from being
    /// collected.
    pub faults: Vec<(AttemptId, String)>,
}

impl CollectReport {
    pub fn merge(&mut self, other: CollectReport) {
        self.collected.extend(other.collected);
        self.required_missing.extend(other.required_missing);
        self.gave_up.extend(other.gave_up);
        self.pending.extend(other.pending);
        self.faults.extend(other.faults);
    }

    fn closed(&mut self, attempt: &AttemptId, verdict: Verdict) {
        match verdict {
            Verdict::Complete => self.collected.push(attempt.clone()),
            Verdict::RequiredMissing => self.required_missing.push(attempt.clone()),
            Verdict::GaveUp => self.gave_up.push(attempt.clone()),
        }
    }
}

#[derive(Debug, Error)]
pub enum CollectError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Files(#[from] StoreError),
    /// A worker has seen a newer coordinator epoch.
    #[error("a newer coordinator took over")]
    Fenced,
    #[error("a blocking call was aborted")]
    Aborted,
}

type Result<T, E = CollectError> = std::result::Result<T, E>;

/// Where one pass left a collection.
enum End {
    Closed(Verdict),
    /// To be continued by the next pass.
    Pending,
    /// To be continued, after this went wrong.
    Waits(Fault, String),
}

/// Where one step left the collection.
enum Flow {
    /// A step was taken; look again.
    Again,
    Closed(Verdict),
    /// Nothing more can be done now.
    Later,
    Fault(Fault, String),
}

/// The artifacts the parent declared, and the node whose log is read.
struct Declared {
    artifacts: Vec<(String, bool)>,
    node: Option<NodeId>,
    payload: AttemptPayload,
}

/// What the parent declared is what it sent. Without it nothing is known
/// about what has to be fetched, and "nothing was declared" would release
/// the worker's artifacts unfetched: a payload that is not recorded or
/// cannot be read is a fault, never an empty declaration.
fn declared_of(payload: &Option<Value>) -> std::result::Result<Declared, &'static str> {
    let Some(payload) = payload else {
        return Err("the payload that was sent is not recorded");
    };
    let Ok(payload) = serde_json::from_value::<AttemptPayload>(payload.clone()) else {
        return Err("the payload that was sent cannot be read");
    };
    Ok(Declared {
        artifacts: payload
            .artifacts
            .iter()
            .map(|d| (d.name.clone(), d.if_missing == MissingPolicy::Required))
            .collect(),
        node: payload.commands.first().map(|c| c.node_id.clone()),
        payload,
    })
}

/// The collections that wait before their next try, and until when. In
/// memory: a parent that starts again tries each once.
#[derive(Debug, Default)]
pub struct Waits(Mutex<BTreeMap<AttemptId, (u32, Instant)>>);

impl Waits {
    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<AttemptId, (u32, Instant)>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn waiting(&self, attempt: &AttemptId) -> bool {
        self.map()
            .get(attempt)
            .is_some_and(|(_, until)| Instant::now() < *until)
    }

    fn put_off(&self, attempt: &AttemptId, base_ms: u64) {
        let mut map = self.map();
        let entry = map.entry(attempt.clone()).or_insert((0, Instant::now()));
        entry.1 = Instant::now() + Duration::from_millis(plan::backoff_ms(base_ms, entry.0));
        entry.0 = entry.0.saturating_add(1);
    }

    fn clear(&self, attempt: &AttemptId) {
        self.map().remove(attempt);
    }
}

/// The result of an attempt that has none stored: its termination only.
fn bare_result(termination: &Termination) -> Value {
    json!({ "termination": termination, "artifacts": [] })
}

fn fault_of(e: LinkError) -> Fault {
    match e {
        LinkError::Remote(e) => Fault::Remote(e.code),
        LinkError::Unconfirmed => Fault::Unreachable,
    }
}

/// What the result says about each declared artifact.
fn verdict_of(declared: &[(String, bool)], result: &GetResultResponse) -> Verdict {
    let standings: Vec<(bool, Standing)> = declared
        .iter()
        .map(|(name, required)| {
            let stored = result.artifacts.iter().any(|a| &a.name == name);
            let not_stored = result
                .artifacts_not_stored
                .iter()
                .find(|n| &n.name == name)
                .map(|n| n.reason);
            (*required, plan::standing(stored, not_stored))
        })
        .collect();
    plan::judge_artifacts(&standings)
}

/// The stored artifacts of a result that the parent declared, in the order
/// of the result. What a worker lists beyond the declaration is not fetched.
fn wanted<'a>(
    declared: &[(String, bool)],
    result: &'a GetResultResponse,
) -> Vec<&'a ArtifactRecord> {
    result
        .artifacts
        .iter()
        .filter(|a| declared.iter().any(|(name, _)| name == &a.name))
        .collect()
}

/// One pass over the collections of one worker.
pub struct Collector {
    store: Arc<CoordinatorStore>,
    files: Arc<ResultStore>,
    cfg: CollectConfig,
    waits: Arc<Waits>,
}

impl Collector {
    pub fn new(
        store: Arc<CoordinatorStore>,
        files: Arc<ResultStore>,
        cfg: CollectConfig,
    ) -> Collector {
        Collector {
            store,
            files,
            cfg,
            waits: Arc::default(),
        }
    }

    /// Use the waits of the passes before this one.
    pub fn with_waits(mut self, waits: Arc<Waits>) -> Collector {
        self.waits = waits;
        self
    }

    pub fn config(&self) -> CollectConfig {
        self.cfg
    }

    async fn db<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&CoordinatorStore) -> crate::storage::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || f(&store)).await {
            Ok(out) => Ok(out?),
            Err(_) => Err(CollectError::Aborted),
        }
    }

    async fn fs<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&ResultStore) -> std::result::Result<T, StoreError> + Send + 'static,
    {
        let files = Arc::clone(&self.files);
        match tokio::task::spawn_blocking(move || f(&files)).await {
            Ok(out) => Ok(out?),
            Err(_) => Err(CollectError::Aborted),
        }
    }

    /// Continue every open collection of `worker` until it is closed, has
    /// to wait, or `deadline` passed. `link` is `None` for a worker that is
    /// not connected: only what needs no worker is closed then. What fails
    /// for one attempt is reported for it and the others are continued; the
    /// pass itself fails only when a newer coordinator took over or the
    /// open collections cannot be listed.
    pub async fn collect_worker(
        &self,
        worker: &WorkerId,
        link: Option<&dyn WorkerLink>,
        deadline: Instant,
    ) -> Result<CollectReport> {
        let (w, scan) = (worker.clone(), self.cfg.scan);
        let open = self.db(move |s| s.collectable(Some(&w), scan)).await?;
        let mut report = CollectReport::default();
        for c in open {
            let a = c.record.attempt_id.clone();
            if self.waits.waiting(&a) {
                report.pending.push(a);
                continue;
            }
            let (fault, why) = match self.one(link, &c, deadline).await {
                Ok(End::Closed(verdict)) => {
                    self.waits.clear(&a);
                    report.closed(&a, verdict);
                    continue;
                }
                Ok(End::Pending) => {
                    report.pending.push(a);
                    continue;
                }
                Ok(End::Waits(fault, why)) => (fault, why),
                Err(e @ (CollectError::Fenced | CollectError::Aborted)) => return Err(e),
                Err(e) => (Fault::Local, e.to_string()),
            };
            if plan::backs_off(fault) {
                self.waits.put_off(&a, self.cfg.backoff_ms);
            }
            if fault == Fault::Local {
                report.faults.push((a.clone(), why));
            }
            report.pending.push(a);
        }
        Ok(report)
    }

    /// Work on one collection. An `Err` is about this attempt only, unless
    /// it is `Fenced` or `Aborted`.
    async fn one(
        &self,
        link: Option<&dyn WorkerLink>,
        c: &Collectable,
        deadline: Instant,
    ) -> Result<End> {
        let a = c.record.attempt_id.clone();
        let declared = match declared_of(&c.payload) {
            Ok(declared) => declared,
            Err(why) => return Ok(End::Waits(Fault::Local, why.to_string())),
        };
        if c.settled_here || !c.record.receipts.reached(Receipt::WorkerAccepted) {
            // No worker holds a result of this attempt: the one the parent
            // fixed is all there is.
            let verdict = self.close_local(&c.record, &declared).await?;
            return Ok(End::Closed(verdict));
        }
        let Some(link) = link else {
            return Ok(End::Pending);
        };
        loop {
            if Instant::now() >= deadline {
                return Ok(End::Pending);
            }
            let (fault, why) = match self.step(link, &c.record, &declared, deadline).await? {
                Flow::Again => {
                    self.waits.clear(&a);
                    continue;
                }
                Flow::Closed(verdict) => {
                    self.tidy(&a).await;
                    return Ok(End::Closed(verdict));
                }
                Flow::Later => return Ok(End::Pending),
                Flow::Fault(fault, why) => (fault, why),
            };
            return match plan::judge_fault(fault) {
                FaultVerdict::Later => Ok(End::Waits(fault, why)),
                FaultVerdict::Fenced => Err(CollectError::Fenced),
                FaultVerdict::Strike => {
                    let (id, text) = (a.clone(), why.clone());
                    let strikes = self
                        .db(move |s| s.strike_collection(&id, &text, wall_ms()))
                        .await?;
                    if !plan::gives_up(strikes) {
                        return Ok(End::Waits(fault, why));
                    }
                    let id = a.clone();
                    self.db(move |s| {
                        s.close_collection(&id, None, Verdict::GaveUp, false, wall_ms())
                    })
                    .await?;
                    self.tidy(&a).await;
                    Ok(End::Closed(Verdict::GaveUp))
                }
            };
        }
    }

    /// Remove what unfinished transfers of a closed collection left. What
    /// cannot be removed now goes with the files of the attempt.
    async fn tidy(&self, attempt: &AttemptId) {
        let a = attempt.clone();
        let _ = self.fs(move |f| f.clear_tmp(&a)).await;
    }

    /// Close the collection of an attempt no worker has a result of.
    async fn close_local(&self, rec: &AttemptRecord, declared: &Declared) -> Result<Verdict> {
        let Some(termination) = rec.termination else {
            return Err(StorageError::Corrupt(format!(
                "terminal attempt {} has no termination",
                rec.attempt_id
            ))
            .into());
        };
        let result = bare_result(&termination);
        let digest = payload_hash(&result)
            .map_err(|e| StorageError::Invalid(format!("result digest: {e}")))?;
        let standings: Vec<(bool, Standing)> = declared
            .artifacts
            .iter()
            .map(|(_, required)| (*required, Standing::Unreported))
            .collect();
        let verdict = plan::judge_artifacts(&standings);
        let (a, names) = (rec.attempt_id.clone(), declared.artifacts.clone());
        self.db(move |s| {
            let at = wall_ms();
            s.begin_collection(&a, &digest, &result, &names, &[], at)?;
            s.set_collection_logs(&a, &digest, &json!({}), at)?;
            s.close_collection(&a, Some(&digest), verdict, false, at)?;
            Ok(())
        })
        .await?;
        Ok(verdict)
    }

    async fn step(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        deadline: Instant,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        let (row, published) = self
            .db(move |s| Ok((s.collection(&a)?, s.persisted_artifacts(&a)?)))
            .await?;
        let closed = row.as_ref().and_then(|r| r.closed_at_ms.and(r.verdict));
        let recorded: Option<(Digest, Value)> = row
            .as_ref()
            .and_then(|r| Some((r.result_digest.clone()?, r.result.clone()?)));
        let current: Option<(Digest, Value, GetResultResponse)> =
            recorded.and_then(|(digest, value)| {
                let result = serde_json::from_value(value.clone()).ok()?;
                Some((digest, value, result))
            });
        let all: Vec<ArtifactRecord> = current
            .iter()
            .flat_map(|(_, _, result)| wanted(&declared.artifacts, result))
            .cloned()
            .collect();
        // Published is what has this name and this digest. A name that is
        // published with other bytes than the result records now is not
        // fetched over it: an artifact never changes.
        let here = |w: &ArtifactRecord| {
            published
                .iter()
                .any(|(n, d)| n == &w.name && d == &w.digest)
        };
        let changed = all.iter().any(|w| {
            published
                .iter()
                .any(|(n, d)| n == &w.name && d != &w.digest)
        });
        let left: Vec<&ArtifactRecord> = all.iter().filter(|w| !here(w)).collect();
        let progress = Progress {
            has_result: current.is_some(),
            logs_done: row.as_ref().is_some_and(|r| r.logs.is_some()),
            artifacts_left: u32::try_from(left.len()).unwrap_or(u32::MAX),
            closed: closed.is_some(),
        };
        match (plan::next_step(progress), current, closed) {
            (Step::Done, _, Some(verdict)) => Ok(Flow::Closed(verdict)),
            (Step::FetchLogs, Some((digest, _, result)), _) => {
                self.logs(link, rec, declared, &digest, &result, deadline)
                    .await
            }
            (Step::FetchArtifact, Some(_), _) if changed => {
                Ok(bad("an artifact that changed after it was published"))
            }
            (Step::FetchArtifact, Some(_), _) => match left.first() {
                Some(artifact) => self.artifact(link, rec, artifact, deadline).await,
                None => Ok(Flow::Again),
            },
            (Step::Ack, Some((digest, value, result)), _) => {
                let logs = row.and_then(|r| r.logs).unwrap_or(Value::Null);
                if !self.sealed(rec, &value, &logs, &all).await? {
                    return Ok(Flow::Again);
                }
                self.ack(link, rec, declared, &digest, &result).await
            }
            // Without a recorded result nothing else is known.
            _ => match self.read_result(link, rec, declared).await? {
                Ok(_) => Ok(Flow::Again),
                Err(flow) => Ok(flow),
            },
        }
    }

    /// Whether everything the collection recorded is in the result store,
    /// durable, as recorded: the last thing before the acknowledgement,
    /// which is final. The result is written again from its record; each
    /// log and each artifact is read back and compared with its recorded
    /// length and digest. What is not as recorded is taken back from the
    /// record (`false`), and the collection goes on from there.
    async fn sealed(
        &self,
        rec: &AttemptRecord,
        result: &Value,
        logs: &Value,
        artifacts: &[ArtifactRecord],
    ) -> Result<bool> {
        let a = rec.attempt_id.clone();
        let streams: Vec<(LogStream, Option<(Digest, u64)>)> =
            [(LogStream::Stdout, "stdout"), (LogStream::Stderr, "stderr")]
                .into_iter()
                .map(|(stream, name)| {
                    let digest = logs[name]["sha256"]
                        .as_str()
                        .and_then(|d| Digest::parse(d).ok());
                    (stream, digest.zip(logs[name]["bytes"].as_u64()))
                })
                .collect();
        let (bytes, wanted) = (result.to_string().into_bytes(), artifacts.to_vec());
        let id = a.clone();
        let (logs_ok, lost) = self
            .fs(move |f| {
                f.write_result(&id, &bytes)?;
                let mut logs_ok = true;
                for (stream, recorded) in &streams {
                    logs_ok &= match recorded {
                        Some((digest, bytes)) => f.verify_log(&id, *stream, digest, *bytes)?,
                        None => false,
                    };
                }
                if !logs_ok {
                    for (stream, _) in &streams {
                        f.discard_log(&id, *stream)?;
                    }
                }
                let mut lost = Vec::new();
                for w in &wanted {
                    if !f.verify_artifact(&id, &w.digest, w.size)? {
                        lost.push(w.name.clone());
                    }
                }
                if logs_ok && lost.is_empty() {
                    f.seal(&id)?;
                }
                Ok((logs_ok, lost))
            })
            .await?;
        if logs_ok && lost.is_empty() {
            return Ok(true);
        }
        self.db(move |s| s.recollect(&a, !logs_ok, &lost, wall_ms()))
            .await?;
        Ok(false)
    }

    /// Read the result, check it and record it as the one that is
    /// collected. Returns its digest.
    async fn read_result(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
    ) -> Result<std::result::Result<Digest, Flow>> {
        let request = Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: rec.attempt_id.clone(),
            generation: rec.generation,
        });
        let result = match link.call(request).await {
            Ok(Response::GetResult(r)) => r,
            Ok(_) => return Ok(Err(bad("another answer than a result"))),
            Err(e) => return Ok(Err(Flow::Fault(fault_of(e), "get_result".into()))),
        };
        if let Err(why) = self.check_result(rec, declared, &result) {
            return Ok(Err(bad(why)));
        }
        let Ok(value) = serde_json::to_value(&result) else {
            return Ok(Err(bad("a result that cannot be recorded")));
        };
        // The file first, whole and durable; then the record and the GC
        // references of the files in one commit. A parent that dies in
        // between has a file without a record and reads the result again.
        let bytes = value.to_string().into_bytes();
        let a = rec.attempt_id.clone();
        self.fs(move |f| f.write_result(&a, &bytes)).await?;
        let (a, digest, names) = (
            rec.attempt_id.clone(),
            result.result_digest.clone(),
            declared.artifacts.clone(),
        );
        self.db(move |s| {
            let refs = [
                ResultRef {
                    ref_id: format!("{a}:result"),
                    kind: BlobKind::Result,
                    locator: ResultStore::result_locator(&a),
                },
                ResultRef {
                    ref_id: format!("{a}:logs"),
                    kind: BlobKind::Log,
                    locator: ResultStore::logs_locator(&a),
                },
            ];
            s.begin_collection(&a, &digest, &value, &names, &refs, wall_ms())
        })
        .await?;
        Ok(Ok(result.result_digest))
    }

    /// A result is about this attempt, ended as the parent knows it, and
    /// lists artifacts that can be kept here.
    fn check_result(
        &self,
        rec: &AttemptRecord,
        declared: &Declared,
        result: &GetResultResponse,
    ) -> std::result::Result<(), &'static str> {
        if result.attempt_id != rec.attempt_id || result.generation != rec.generation {
            return Err("a result of another attempt");
        }
        if Some(result.termination) != rec.termination {
            return Err("a result with another termination than the attempt's");
        }
        let names: BTreeSet<&str> = result
            .artifacts
            .iter()
            .map(|a| a.name.as_str())
            .chain(result.artifacts_not_stored.iter().map(|n| n.name.as_str()))
            .collect();
        plan::check_lists(
            result.artifacts.len(),
            result.artifacts_not_stored.len(),
            names.len(),
        )?;
        for artifact in &result.artifacts {
            let of_payload = declared.payload.source_id == artifact.source_id
                && declared.payload.plan_digest == artifact.plan_digest;
            plan::check_record(RecordFacts {
                same_attempt: artifact.attempt_id == rec.attempt_id && of_payload,
                path_is_name: artifact.path == artifact.name,
                name_ok: relative_path(&artifact.name).is_some(),
                size: artifact.size,
                max_size: self.cfg.max_artifact_bytes,
            })?;
        }
        Ok(())
    }

    /// Read both streams to their end and record what they hold.
    async fn logs(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        digest: &Digest,
        result: &GetResultResponse,
        deadline: Instant,
    ) -> Result<Flow> {
        let has_stored_result = payload_hash(&bare_result(&result.termination))
            .map(|bare| &bare != digest)
            .unwrap_or(true);
        let Some(node) = declared.node.clone().or_else(|| NodeId::parse("log").ok()) else {
            return Ok(bad("no node to read the log of"));
        };
        let mut summary = serde_json::Map::new();
        for (stream, name) in [(LogStream::Stdout, "stdout"), (LogStream::Stderr, "stderr")] {
            let a = rec.attempt_id.clone();
            let mut offset = self.fs(move |f| f.log_len(&a, stream)).await?;
            let info = loop {
                if Instant::now() >= deadline {
                    return Ok(Flow::Later);
                }
                let request = Request::ReadLog(ReadLogRequest {
                    schema_version: SchemaVersion::CURRENT,
                    request_id: RequestId::generate(),
                    attempt_id: rec.attempt_id.clone(),
                    node_id: node.clone(),
                    stream,
                    offset,
                    max_bytes: self.cfg.chunk_bytes,
                });
                let r = match link.call(request).await {
                    Ok(Response::ReadLog(r)) => r,
                    Ok(_) => return Ok(bad("another answer than log bytes")),
                    Err(e) => return Ok(Flow::Fault(fault_of(e), "read_log".into())),
                };
                let verdict = plan::check_log(LogFacts {
                    want_offset: offset,
                    max: u64::from(self.cfg.chunk_bytes),
                    limit: self.cfg.max_log_bytes,
                    got_offset: r.offset,
                    got_len: r.data.0.len() as u64,
                    got_next: r.next_offset,
                    got_eof: r.eof,
                });
                let complete = match verdict {
                    LogVerdict::Bad(why) => return Ok(bad(why)),
                    LogVerdict::Wait => match plan::log_wait(has_stored_result) {
                        LogWait::Retry => return Ok(Flow::Later),
                        LogWait::TakeIncomplete => false,
                    },
                    LogVerdict::Append { next, end } => {
                        let (a, at, data) = (rec.attempt_id.clone(), offset, r.data.0);
                        self.fs(move |f| f.append_log(&a, stream, at, &data))
                            .await?;
                        offset = next;
                        if !end {
                            continue;
                        }
                        true
                    }
                };
                // What is here now is what the acknowledgement vouches for:
                // its length and digest are recorded and compared again
                // before it is sent.
                let a = rec.attempt_id.clone();
                let (sha256, here) = self.fs(move |f| f.log_digest(&a, stream)).await?;
                if here != offset {
                    return Err(StoreError::Offset {
                        want: offset,
                        have: here,
                    }
                    .into());
                }
                break json!({
                    "bytes": offset,
                    "sha256": sha256.as_str(),
                    "complete": complete,
                    "log_truncated": r.log_truncated,
                    "dropped_bytes": r.dropped_bytes,
                });
            };
            summary.insert(name.to_string(), info);
        }
        let (a, digest, logs) = (
            rec.attempt_id.clone(),
            digest.clone(),
            Value::Object(summary),
        );
        self.db(move |s| s.set_collection_logs(&a, &digest, &logs, wall_ms()))
            .await?;
        Ok(Flow::Again)
    }

    /// Fetch what is missing of one artifact, verify it and publish it.
    async fn artifact(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        artifact: &ArtifactRecord,
        deadline: Instant,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        loop {
            let (id, digest, size) = (a.clone(), artifact.digest.clone(), artifact.size);
            let state = self
                .fs(move |f| f.artifact_state(&id, &digest, size))
                .await?;
            let offset = match state {
                ArtifactState::Published => break,
                ArtifactState::Partial(offset) => offset,
            };
            if offset == artifact.size {
                let (id, digest, size) = (a.clone(), artifact.digest.clone(), artifact.size);
                let published = self
                    .fs(move |f| f.publish_artifact(&id, &digest, size))
                    .await?;
                let whole = match published {
                    Publish::Published => break,
                    Publish::Mismatch { whole } => whole,
                };
                match plan::judge_mismatch(whole) {
                    Mismatch::Strike => {
                        return Ok(Flow::Fault(
                            Fault::Corrupt,
                            format!("artifact {} does not have its digest", artifact.name),
                        ))
                    }
                    // Bytes a parent before this one left, perhaps torn:
                    // they are gone now, and the artifact is fetched from
                    // its start.
                    Mismatch::Refetch => continue,
                }
            }
            if Instant::now() >= deadline {
                return Ok(Flow::Later);
            }
            let request = Request::BlobGet(BlobGetRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: a.clone(),
                digest: artifact.digest.clone(),
                offset,
                max_bytes: self.cfg.chunk_bytes,
            });
            let r = match link.call(request).await {
                Ok(Response::BlobGet(r)) => r,
                Ok(_) => return Ok(bad("another answer than a chunk")),
                Err(e) => return Ok(Flow::Fault(fault_of(e), "blob_get".into())),
            };
            let verdict = plan::check_chunk(ChunkFacts {
                same_digest: r.digest == artifact.digest,
                want_offset: offset,
                size: artifact.size,
                max: u64::from(self.cfg.chunk_bytes),
                got_offset: r.offset,
                got_total: r.total_size,
                got_len: r.data.0.len() as u64,
                got_eof: r.eof,
            });
            match verdict {
                ChunkVerdict::Bad(why) => return Ok(bad(why)),
                ChunkVerdict::Append { .. } => {
                    let (id, digest, data) = (a.clone(), artifact.digest.clone(), r.data.0);
                    self.fs(move |f| f.append_part(&id, &digest, offset, &data))
                        .await?;
                }
            }
        }
        let receipt = ArtifactReceipt {
            attempt_id: a.clone(),
            name: artifact.name.clone(),
            rel_path: artifact.path.clone(),
            size: artifact.size,
            digest: artifact.digest.clone(),
            source_id: artifact.source_id.clone(),
            plan_digest: artifact.plan_digest.clone(),
            profile_fingerprint: artifact.profile_fingerprint.to_string(),
        };
        self.db(move |s| {
            let file = ResultRef {
                ref_id: format!("{a}:artifact:{}", receipt.name),
                kind: BlobKind::Result,
                locator: ResultStore::artifact_locator(&a, &receipt.digest),
            };
            s.record_collected_artifact(&receipt, &file, wall_ms())
        })
        .await?;
        Ok(Flow::Again)
    }

    /// Everything of the recorded result is verified and durable here: tell
    /// the worker, by digest, and close on its answer.
    async fn ack(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        digest: &Digest,
        result: &GetResultResponse,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        let request = Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: rec.generation,
            result_digest: digest.clone(),
        });
        match link.call(request).await {
            Ok(Response::AckCollected(r)) if r.attempt_id == a && r.released => {
                let verdict = verdict_of(&declared.artifacts, result);
                let digest = digest.clone();
                self.db(move |s| s.close_collection(&a, Some(&digest), verdict, true, wall_ms()))
                    .await?;
                Ok(Flow::Closed(verdict))
            }
            Ok(_) => Ok(bad("an acknowledgement that released nothing")),
            Err(LinkError::Remote(e)) => match plan::ack_refusal(e.code) {
                // The worker holds another result than the one that was read.
                AckRefusal::OtherResult => match self.read_result(link, rec, declared).await? {
                    Err(flow) => Ok(flow),
                    Ok(now) => match plan::judge_reread(&now == digest) {
                        Reread::Restart => Ok(Flow::Again),
                        Reread::Contradiction => {
                            Ok(bad("an acknowledgement refused for the result it gave"))
                        }
                    },
                },
                // The worker has nothing of the attempt any more (an
                // acknowledgement whose answer was lost, sent again after
                // the worker deleted what it released). Everything is here,
                // verified: the collection is closed, as not acknowledged.
                AckRefusal::Gone => {
                    let verdict = verdict_of(&declared.artifacts, result);
                    let digest = digest.clone();
                    self.db(move |s| {
                        s.close_collection(&a, Some(&digest), verdict, false, wall_ms())
                    })
                    .await?;
                    Ok(Flow::Closed(verdict))
                }
                AckRefusal::Fault => Ok(Flow::Fault(Fault::Remote(e.code), "ack_collected".into())),
            },
            Err(e) => Ok(Flow::Fault(fault_of(e), "ack_collected".into())),
        }
    }
}

fn bad(why: &str) -> Flow {
    Flow::Fault(Fault::BadAnswer, why.to_string())
}
