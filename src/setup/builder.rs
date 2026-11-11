//! The worker's setup builders: one per template key at a time (singleflight),
//! each a supervised process of its own (spec chapters 8 and 12).
//!
//! A consumer (an attempt whose payload carries a [`SetupSpec`]) asks with
//! [`Setups::poll`], which never blocks: the template is there, or a builder
//! for it is working and the consumer holds a reference to it, or the builder
//! could not make it and the consumer is told why (and runs the steps
//! itself). The builder is an internal attempt in a database of its own
//! (`<state>/setup/builders/<builder_id>/worker.db`), run by the same
//! `supervise` process as any attempt: its steps are admitted by
//! task-traffic-controller, its processes are tracked and stopped by the
//! supervisor, and its lease is renewed here only while at least one
//! consumer still needs the result. When the last reference goes the lease is
//! not renewed and the builder is cancelled; the supervisor stops it.
//!
//! The builder works in a workspace of its own, made from the same source as
//! the consumers'. What its steps added to it (the entries that were not
//! there before) becomes the template, published by rename. Nothing of the
//! builder is ever visible under a template key before it is complete.

use super::policy::{self, BuildEnd, KeyContext, Leave};
use super::store::{digest_of, remove_tree, StoreError, Template, TemplateStore};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{
    AttemptId, BuilderId, CoordinatorEpoch, Digest, Generation, NodeId, RunId, TaskId,
};
use crate::platform;
use crate::state::AttemptState;
use crate::storage::{NewAcceptance, WorkerStore};
use crate::supervisor::lock::SupervisorLock;
use crate::supervisor::process::{code, spawn_detached, supervise};
use crate::supervisor::spec::{MaskValues, NodeSpec, SuperviseSpec};
use crate::wire::{AttemptPayload, LeaseKey, PreparedArtifact, SetupSpec};
use crate::workspace::Workspace;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// How often, and how far apart, a builder that could not be settled is
/// looked at again.
const REAP_TRIES: u32 = 60;
const REAP_PAUSE_MS: u64 = 1_000;

/// How the builders get their workspaces: the same way the consumers do.
pub trait BuilderWorkspaces: Send + Sync {
    fn create(&self, attempt: &AttemptId, payload: &AttemptPayload) -> Result<Workspace, ApiError>;
    fn remove(&self, ws: &Workspace) -> Result<(), ApiError>;
}

#[derive(Debug, Clone)]
pub struct SetupConfig {
    /// The worker's state directory; everything of the setup is under
    /// `<state_dir>/setup`.
    pub state_dir: PathBuf,
    pub worker_id: crate::ids::WorkerId,
    /// The binary whose `supervise --spec <file>` runs a builder.
    pub supervisor_exe: PathBuf,
    pub traffic_state_dir: PathBuf,
    pub traffic_config: Option<PathBuf>,
    /// Part of every template key: what the tools of this worker are.
    pub profile: String,
    /// The environment the steps start from (`PATH`, `HOME`, ...). Nothing
    /// of this process's own environment is inherited.
    pub env: BTreeMap<String, String>,
    pub poll_ms: u64,
    pub lease_ms: u64,
    pub term_grace_ms: u64,
    pub kill_wait_ms: u64,
    /// The whole builder, from its start to its end.
    pub deadline_ms: u64,
    pub inhibit_sleep: bool,
}

impl SetupConfig {
    pub fn new(
        state_dir: PathBuf,
        worker_id: crate::ids::WorkerId,
        supervisor_exe: PathBuf,
    ) -> Self {
        SetupConfig {
            traffic_state_dir: state_dir.join("traffic"),
            state_dir,
            worker_id,
            supervisor_exe,
            traffic_config: None,
            profile: "default".into(),
            env: BTreeMap::new(),
            poll_ms: 200,
            lease_ms: crate::supervisor::spec::DEFAULT_LEASE_MS,
            term_grace_ms: crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 10_000,
            deadline_ms: 30 * 60 * 1000,
            inhibit_sleep: false,
        }
    }

    fn root(&self) -> PathBuf {
        self.state_dir.join("setup")
    }

    fn builders(&self) -> PathBuf {
        self.root().join("builders")
    }
}

/// What a consumer is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupPoll {
    /// A builder is working (or is about to start). Ask again.
    Pending,
    /// Over: `skip` are the ids of the setup steps whose result is in the
    /// consumer's workspace now. `note` says why there is nothing to skip,
    /// when a template was wanted and could not be had.
    Done {
        skip: BTreeSet<String>,
        note: Option<String>,
    },
}

impl SetupPoll {
    fn nothing() -> Self {
        SetupPoll::Done {
            skip: BTreeSet::new(),
            note: None,
        }
    }
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    builder_id: BuilderId,
    attempt_id: AttemptId,
    key: String,
}

struct Flight {
    key: String,
    attempt_id: AttemptId,
    /// The attempts that hold a reference.
    refs: BTreeSet<String>,
    /// Set once, when the builder has ended.
    end: Option<BuildEnd>,
    /// The last reference went: no new consumer joins, and the builder is
    /// cancelled.
    stop: bool,
}

#[derive(Clone)]
enum Resolved {
    Skip(BTreeSet<String>),
    Unavailable(String),
}

#[derive(Default)]
struct Consumer {
    builder: Option<BuilderId>,
    resolved: Option<Resolved>,
}

#[derive(Default)]
struct Inner {
    /// The builder working for a key (none once it ended or was stopped).
    by_key: HashMap<String, BuilderId>,
    flights: HashMap<BuilderId, Flight>,
    consumers: HashMap<String, Consumer>,
}

pub struct Setups {
    cfg: SetupConfig,
    store: TemplateStore,
    ws: Arc<dyn BuilderWorkspaces>,
    inner: Mutex<Inner>,
    started: AtomicU64,
    /// Builders left from before this start that could not be settled.
    leftover: Mutex<BTreeSet<AttemptId>>,
}

impl Setups {
    /// Open the setup area of the worker and clean up after a crash: builders
    /// of an earlier run are stopped (through the supervisor's own recovery
    /// when one had started a command) and removed, scratch directories and
    /// unfinished templates are removed. Runs before the worker's workspace
    /// sweep and before any consumer asks.
    pub fn open(cfg: SetupConfig, ws: Arc<dyn BuilderWorkspaces>) -> Result<Arc<Setups>, ApiError> {
        private_dir(&cfg.root()).map_err(|e| internal("setup directory", e))?;
        private_dir(&cfg.builders()).map_err(|e| internal("setup directory", e))?;
        let store = TemplateStore::open(cfg.root().join("templates"))
            .map_err(|e| internal("template store", e))?;
        store.sweep().map_err(|e| internal("template store", e))?;
        let s = Setups {
            cfg,
            store,
            ws,
            inner: Mutex::new(Inner::default()),
            started: AtomicU64::new(0),
            leftover: Mutex::new(BTreeSet::new()),
        };
        s.recover();
        Ok(Arc::new(s))
    }

    /// How many builders were started by this process.
    pub fn builds_started(&self) -> u64 {
        self.started.load(Ordering::SeqCst)
    }

    /// The workspaces that belong to a builder (a running one, or one that
    /// could not be settled at the start): the worker's sweep of workspaces
    /// no attempt owns must leave them.
    pub fn owns_workspace(&self, attempt: &AttemptId) -> bool {
        lock(&self.leftover).contains(attempt)
            || lock(&self.inner)
                .flights
                .values()
                .any(|f| &f.attempt_id == attempt)
    }

    /// Builders found at the start that are still not settled (the processes
    /// could not be confirmed stopped). They are tried again at the next
    /// start.
    pub fn unsettled(&self) -> Vec<AttemptId> {
        lock(&self.leftover).iter().cloned().collect()
    }

    fn recover(&self) {
        let Ok(rd) = std::fs::read_dir(self.cfg.builders()) else {
            return;
        };
        for ent in rd.flatten() {
            let dir = ent.path();
            let meta = std::fs::read(dir.join("meta.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Meta>(&b).ok());
            let Some(meta) = meta else {
                // made but not described: nothing ran
                let _ = remove_tree(&dir);
                continue;
            };
            if !self.settle_old(&dir, &meta) {
                lock(&self.leftover).insert(meta.attempt_id);
            }
        }
    }

    /// Settle one builder of an earlier run. `true` when nothing of it is
    /// left (directory and workspace removed).
    fn settle_old(&self, dir: &Path, meta: &Meta) -> bool {
        let attempt = &meta.attempt_id;
        let held = match SupervisorLock::try_acquire(dir, attempt) {
            Ok(Some(lock)) => lock,
            // a supervisor is still alive: it is asked to stop (nobody
            // renews its lease, so it would stop by itself anyway)
            Ok(None) => {
                if let Ok(store) = WorkerStore::open(dir, &self.cfg.worker_id) {
                    let _ = store.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms());
                }
                return false;
            }
            Err(_) => return false,
        };
        // nothing was accepted, or nothing was started: nothing runs
        let launched =
            WorkerStore::open(dir, &self.cfg.worker_id).and_then(|s| match s.get(attempt)? {
                None => Ok(false),
                Some(_) => Ok(!s.launches(attempt)?.is_empty()),
            });
        let launched = match launched {
            Ok(l) => l,
            Err(_) => return false,
        };
        drop(held);
        if launched {
            // something may still run: the supervisor's recovery stops what
            // it recorded, through the guarded path, and settles the attempt
            if !self.settle_in_process(dir, attempt) {
                return false;
            }
        }
        if let Some(ws) = std::fs::read(dir.join("workspace.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Workspace>(&b).ok())
        {
            if self.ws.remove(&ws).is_err() {
                return false;
            }
        }
        remove_tree(dir).is_ok()
    }

    /// Where a consumer stands. Never blocks for a build: a template that is
    /// there is copied into `ws` (which can take a while), a builder that is
    /// working makes the answer `Pending`.
    pub fn poll(
        self: &Arc<Self>,
        attempt: &AttemptId,
        payload: &AttemptPayload,
        ws: &Workspace,
    ) -> Result<SetupPoll, ApiError> {
        let Some(spec) = &payload.setup else {
            return Ok(SetupPoll::nothing());
        };
        policy::validate(spec).map_err(invalid)?;
        if policy::hoisted(spec).is_empty() {
            return Ok(SetupPoll::nothing());
        }
        let a = attempt.to_string();
        let resolved = lock(&self.inner)
            .consumers
            .get(&a)
            .and_then(|c| c.resolved.clone());
        match resolved {
            Some(Resolved::Skip(skip)) => return Ok(SetupPoll::Done { skip, note: None }),
            Some(Resolved::Unavailable(why)) => {
                return Ok(SetupPoll::Done {
                    skip: BTreeSet::new(),
                    note: Some(why),
                })
            }
            None => {}
        }
        let ctx = KeyContext {
            source_digest: payload.manifest_digest.to_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            profile: self.cfg.profile.clone(),
            run_id: payload.run_id.to_string(),
        };
        let key = policy::template_key(spec, &ctx);
        let now = platform::wall_ms();
        let mut inner = lock(&self.inner);
        let joined = inner.consumers.get(&a).and_then(|c| c.builder.clone());
        let ready: Option<Template> = match joined {
            Some(id) => match inner.flights.get(&id).map(|f| f.end.clone()) {
                Some(None) => return Ok(SetupPoll::Pending),
                None => {
                    drop(inner);
                    let why = "the builder it waited for is not known any more".to_string();
                    return Ok(self.resolve(&a, Resolved::Unavailable(why)));
                }
                Some(Some(end)) if !end.is_built() => {
                    let why = end.describe();
                    drop(inner);
                    return Ok(self.resolve(&a, Resolved::Unavailable(why)));
                }
                Some(Some(_)) => {
                    drop(inner);
                    self.store
                        .get(&key, &ctx.source_digest, spec, now)
                        .map_err(|e| internal("template store", e))?
                }
            },
            None => match inner.by_key.get(&key).cloned() {
                Some(id) => {
                    let f = inner.flights.get_mut(&id).expect("a flight is listed");
                    policy::join(&mut f.refs, &a);
                    inner.consumers.entry(a.clone()).or_default().builder = Some(id);
                    return Ok(SetupPoll::Pending);
                }
                None => {
                    // no builder: a template that is there already, or a new
                    // builder; the store is looked at under the lock so that
                    // a builder that just published is not built again
                    let found = self
                        .store
                        .get(&key, &ctx.source_digest, spec, now)
                        .map_err(|e| internal("template store", e))?;
                    if found.is_none() {
                        let id = BuilderId::generate();
                        let attempt_id = AttemptId::generate();
                        let mut refs = BTreeSet::new();
                        policy::join(&mut refs, &a);
                        inner.flights.insert(
                            id.clone(),
                            Flight {
                                key: key.clone(),
                                attempt_id: attempt_id.clone(),
                                refs,
                                end: None,
                                stop: false,
                            },
                        );
                        inner.by_key.insert(key.clone(), id.clone());
                        inner.consumers.entry(a.clone()).or_default().builder = Some(id.clone());
                        drop(inner);
                        self.spawn_builder(id, attempt_id, key, ctx.source_digest, payload.clone());
                        return Ok(SetupPoll::Pending);
                    }
                    drop(inner);
                    found
                }
            },
        };
        let Some(template) = ready else {
            // it was built but is gone or unusable now
            return Ok(self.resolve(
                &a,
                Resolved::Unavailable("the template made for it is not usable".into()),
            ));
        };
        let resolved = match self.store.materialize(&template, &ws.root) {
            Ok(()) => Resolved::Skip(policy::skip_set(&template.receipt)),
            Err(StoreError::Corrupt(why)) => {
                // not what its receipt says: it is never used again
                let _ = self.store.discard(&key);
                Resolved::Unavailable(format!("the template did not match its receipt: {why}"))
            }
            Err(StoreError::Conflict(path)) => Resolved::Unavailable(format!(
                "the workspace already holds `{path}`, which the template would write"
            )),
            Err(e) => return Err(internal("template", e)),
        };
        Ok(self.resolve(&a, resolved))
    }

    /// Record how a consumer ended and give up its reference.
    fn resolve(&self, consumer: &str, r: Resolved) -> SetupPoll {
        let mut inner = lock(&self.inner);
        let c = inner.consumers.entry(consumer.to_string()).or_default();
        c.resolved = Some(r.clone());
        let builder = c.builder.clone();
        if let Some(id) = builder {
            Self::drop_ref(&mut inner, &id, consumer);
        }
        match r {
            Resolved::Skip(skip) => SetupPoll::Done { skip, note: None },
            Resolved::Unavailable(why) => SetupPoll::Done {
                skip: BTreeSet::new(),
                note: Some(why),
            },
        }
    }

    /// The consumer is gone (cancelled, ended): its reference goes. The last
    /// reference of a builder that is still working stops the builder.
    pub fn release(&self, attempt: &AttemptId) {
        let a = attempt.to_string();
        let mut inner = lock(&self.inner);
        let Some(c) = inner.consumers.remove(&a) else {
            return;
        };
        if let Some(id) = c.builder {
            Self::drop_ref(&mut inner, &id, &a);
        }
    }

    fn drop_ref(inner: &mut Inner, id: &BuilderId, consumer: &str) {
        let Some(f) = inner.flights.get_mut(id) else {
            return;
        };
        let building = f.end.is_none() && !f.stop;
        if policy::leave(&mut f.refs, consumer, building) == Leave::StopBuilder {
            f.stop = true;
            let key = f.key.clone();
            if inner.by_key.get(&key) == Some(id) {
                inner.by_key.remove(&key);
            }
        }
        if let Some(f) = inner.flights.get(id) {
            if f.end.is_some() && f.refs.is_empty() {
                inner.flights.remove(id);
            }
        }
    }

    fn spawn_builder(
        self: &Arc<Self>,
        id: BuilderId,
        attempt: AttemptId,
        key: String,
        source: String,
        payload: AttemptPayload,
    ) {
        self.started.fetch_add(1, Ordering::SeqCst);
        let me = self.clone();
        let spawned = std::thread::Builder::new()
            .name("setup-builder".into())
            .spawn({
                let (id, attempt, key) = (id.clone(), attempt.clone(), key.clone());
                move || {
                    let end = catch_unwind(AssertUnwindSafe(|| {
                        me.run_builder(&id, &attempt, &key, &source, &payload)
                    }))
                    .unwrap_or_else(|_| BuildEnd::Crashed("the builder thread panicked".into()));
                    // the consumers are told first; then a builder whose
                    // processes could not be confirmed stopped is looked at
                    // again, as the agent does for an attempt
                    me.finish(&id, end);
                    me.reap(&me.cfg.builders().join(id.as_str()), &attempt);
                }
            });
        if spawned.is_err() {
            self.finish(&id, BuildEnd::Crashed("no thread for the builder".into()));
        }
    }

    /// Settle a builder that was left unsettled (its supervisor died and
    /// what it started could not be told from what is not its own, or the
    /// supervisor itself could not confirm the stop): the supervisor's
    /// recovery is run again, with a pause, as the agent does for an attempt,
    /// until it settles, up to a limit. What is still unsettled then is
    /// settled at the next start.
    fn reap(&self, dir: &Path, attempt: &AttemptId) {
        for _ in 0..REAP_TRIES {
            if !lock(&self.leftover).contains(attempt) {
                return;
            }
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.max(REAP_PAUSE_MS)));
            if !self.settle_in_process(dir, attempt) {
                continue;
            }
            if let Some(ws) = std::fs::read(dir.join("workspace.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Workspace>(&b).ok())
            {
                if self.ws.remove(&ws).is_err() {
                    continue;
                }
            }
            if remove_tree(dir).is_ok() {
                lock(&self.leftover).remove(attempt);
                return;
            }
        }
    }

    /// Settle a builder whose supervisor is gone, in this process, through
    /// the supervisor's own recovery. The builder is cancelled first: a
    /// supervisor that died before it recorded a launch left the attempt
    /// `Ready` with nothing started, and `supervise` would then *run* the
    /// steps here, in the agent, with nobody renewing the lease. With the
    /// cancel recorded it starts nothing and only stops what was recorded.
    /// `true` when the attempt is settled and nothing of it can still run.
    fn settle_in_process(&self, dir: &Path, attempt: &AttemptId) -> bool {
        let Ok(store) = WorkerStore::open(dir, &self.cfg.worker_id) else {
            return false;
        };
        match store.get(attempt) {
            Ok(None) => return true,
            Ok(Some(_)) => {}
            Err(_) => return false,
        }
        if store
            .cancel(attempt, CoordinatorEpoch(1), platform::wall_ms())
            .is_err()
        {
            return false;
        }
        drop(store);
        SuperviseSpec::from_file(&dir.join("spec.json"))
            .ok()
            .and_then(|s| supervise(&s).ok())
            .is_some_and(|r| r.unresolved.is_none())
    }

    fn finish(&self, id: &BuilderId, end: BuildEnd) {
        let mut inner = lock(&self.inner);
        let Some(f) = inner.flights.get_mut(id) else {
            return;
        };
        f.end = Some(end);
        let (key, empty) = (f.key.clone(), f.refs.is_empty());
        if inner.by_key.get(&key) == Some(id) {
            inner.by_key.remove(&key);
        }
        if empty {
            inner.flights.remove(id);
        }
    }

    fn wanted(&self, id: &BuilderId) -> (usize, bool) {
        lock(&self.inner)
            .flights
            .get(id)
            .map_or((0, true), |f| (f.refs.len(), f.stop))
    }

    /// The references of a builder are kept in its directory (written
    /// whole, atomically, whenever they change), so that what a crash cut
    /// short can be told afterwards.
    fn keep_refs(&self, id: &BuilderId, dir: &Path, last: &mut Option<BTreeSet<String>>) {
        let now = lock(&self.inner).flights.get(id).map(|f| f.refs.clone());
        if let Some(now) = now.filter(|n| last.as_ref() != Some(n)) {
            let bytes = serde_json::to_vec(&now).expect("strings serialize");
            let _ = write_private(&dir.join("consumers.json"), &bytes);
            *last = Some(now);
        }
    }

    /// The builder: its workspace, its supervisor, its lease, its template.
    /// Everything it made is gone when it returns, except the template.
    fn run_builder(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        source: &str,
        payload: &AttemptPayload,
    ) -> BuildEnd {
        let spec = payload.setup.as_ref().expect("a builder is for a setup");
        let dir = self.cfg.builders().join(id.as_str());
        let mut made: Option<Workspace> = None;
        let end = self.build(id, attempt, key, source, payload, spec, &dir, &mut made);
        // the supervisor has ended or never started: nothing runs in them
        if let Some(ws) = &made {
            let _ = self.ws.remove(ws);
        }
        // unless its processes could not be confirmed stopped: then the
        // next start's recovery needs what it recorded
        if !lock(&self.leftover).contains(attempt) {
            let _ = remove_tree(&dir);
        }
        end
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        source: &str,
        payload: &AttemptPayload,
        spec: &SetupSpec,
        dir: &Path,
        made: &mut Option<Workspace>,
    ) -> BuildEnd {
        let crashed =
            |what: &str, e: &dyn std::fmt::Display| BuildEnd::Crashed(format!("{what}: {e}"));
        if let Err(e) = private_dir(dir) {
            return crashed("builder directory", &e);
        }
        let meta = Meta {
            builder_id: id.clone(),
            attempt_id: attempt.clone(),
            key: key.to_string(),
        };
        if let Err(e) = write_private(&dir.join("meta.json"), &serde_json::to_vec(&meta).unwrap()) {
            return crashed("builder record", &e);
        }
        if self.wanted(id).0 == 0 {
            return BuildEnd::Cancelled;
        }
        let ws = match self.ws.create(attempt, payload) {
            Ok(ws) => ws,
            Err(e) => return crashed("builder workspace", &e.message),
        };
        *made = Some(ws.clone());
        let record = serde_json::to_vec(&ws).expect("a workspace is plain data");
        if let Err(e) = write_private(&dir.join("workspace.json"), &record) {
            return crashed("builder record", &e);
        }
        let hoisted = policy::hoisted(spec);
        let dirs: BTreeSet<String> = hoisted.iter().map(|s| s.dir.clone()).collect();
        let before = top_level_entries(&ws.root, &dirs);
        let store = match self.accept(dir, attempt) {
            Ok(s) => s,
            Err(e) => return crashed("builder state", &e),
        };
        if let Err(e) = self.write_spec(dir, attempt, spec, &ws) {
            return crashed("builder spec", &e);
        }
        if self.wanted(id).0 == 0 {
            return BuildEnd::Cancelled;
        }
        let mut child = match spawn_detached(&self.cfg.supervisor_exe, &dir.join("spec.json")) {
            Ok(c) => c,
            Err(e) => return crashed("supervisor", &e),
        };
        // the lease: renewed while somebody needs the result
        let every = (self.cfg.lease_ms / 3).max(self.cfg.poll_ms);
        let mut last = platform::monotonic_ms();
        let mut seq = 0u64;
        let mut cancelled = false;
        let mut kept = None;
        let status = loop {
            self.keep_refs(id, dir, &mut kept);
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) => {}
                Err(e) => {
                    let _ = store.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms());
                    return crashed("supervisor", &e);
                }
            }
            let (refs, stop) = self.wanted(id);
            let now = platform::monotonic_ms();
            if policy::renew_lease(refs, stop) {
                if now.saturating_sub(last) >= every {
                    seq += 1;
                    let key = LeaseKey {
                        coordinator_epoch: CoordinatorEpoch(1),
                        lease_seq: seq,
                    };
                    let _ = store.renew_lease(attempt, key, platform::wall_ms());
                    last = now;
                }
            } else if !cancelled {
                cancelled = true;
                let _ = store.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms());
            }
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.clamp(1, 100)));
        };
        match status.code() {
            Some(code::SETTLED) => {}
            Some(code::UNRESOLVED) | Some(code::IDENTITY_UNKNOWN) => {
                // the processes could not be confirmed stopped: the builder
                // and its workspace are kept for the next start's recovery
                *made = None;
                lock(&self.leftover).insert(attempt.clone());
                return BuildEnd::Crashed("its processes could not be confirmed stopped".into());
            }
            other => {
                // The supervisor died or failed: what it started is stopped
                // by the supervisor's own recovery, which settles the
                // attempt. Only a builder that could not be settled is kept.
                if !self.settle_in_process(dir, attempt) {
                    *made = None;
                    lock(&self.leftover).insert(attempt.clone());
                }
                return BuildEnd::Crashed(format!("the supervisor ended with {other:?}"));
            }
        }
        let end = policy::end_of(
            store
                .get(attempt)
                .ok()
                .flatten()
                .and_then(|r| r.termination),
        );
        if !end.is_built() {
            return end;
        }
        let after = top_level_entries(&ws.root, &dirs);
        let mut artifacts = Vec::new();
        for path in policy::added_entries(&before, &after) {
            match digest_of(&ws.root.join(&path)) {
                Ok(digest) => artifacts.push(PreparedArtifact { path, digest }),
                Err(e) => return crashed("what the setup made", &e),
            }
        }
        let receipt = policy::seal_receipt(key, source, id, spec, artifacts, platform::wall_ms());
        match self.store.publish(key, &receipt, &ws.root) {
            Ok(_) => BuildEnd::Built,
            Err(e) => crashed("the template", &e),
        }
    }

    /// The builder's own database: one attempt, `Ready`.
    fn accept(&self, dir: &Path, attempt: &AttemptId) -> Result<WorkerStore, String> {
        let store = WorkerStore::open(dir, &self.cfg.worker_id).map_err(|e| e.to_string())?;
        let now = platform::wall_ms();
        store
            .submit(
                NewAcceptance {
                    attempt_id: attempt.clone(),
                    run_id: RunId::generate(),
                    task_id: TaskId::generate(),
                    generation: Generation(1),
                    payload_hash: Digest::of_bytes(attempt.as_str().as_bytes()),
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::json!({ "setup_builder": true }),
                },
                now,
            )
            .map_err(|e| e.to_string())?;
        for to in [AttemptState::Preparing, AttemptState::Ready] {
            store
                .transition(attempt, to, now)
                .map_err(|e| e.to_string())?;
        }
        Ok(store)
    }

    fn write_spec(
        &self,
        dir: &Path,
        attempt: &AttemptId,
        spec: &SetupSpec,
        ws: &Workspace,
    ) -> Result<(), String> {
        let mut nodes = Vec::new();
        for (i, s) in policy::hoisted(spec).iter().enumerate() {
            let argv = policy::shell_argv(s.shell.as_deref(), &s.run)
                .ok_or_else(|| format!("step {}: unsupported shell", s.id))?;
            let mut env = self.cfg.env.clone();
            env.extend(s.env.clone());
            nodes.push(NodeSpec {
                node_id: NodeId::parse(&format!("setup-{i}")).map_err(|e| e.to_string())?,
                argv,
                cwd: ws.root.join(&s.dir),
                env,
                weight: None,
                timeout_ms: spec.step_timeout_ms,
            });
        }
        let sup = SuperviseSpec {
            state_dir: dir.to_path_buf(),
            worker_id: self.cfg.worker_id.clone(),
            attempt_id: attempt.clone(),
            traffic_state_dir: self.cfg.traffic_state_dir.clone(),
            traffic_config: self.cfg.traffic_config.clone(),
            nodes,
            lease_ms: self.cfg.lease_ms,
            term_grace_ms: self.cfg.term_grace_ms,
            kill_wait_ms: self.cfg.kill_wait_ms,
            poll_ms: self.cfg.poll_ms,
            command_timeout_ms: None,
            resource_wait_timeout_ms: None,
            deadline_ms: Some(self.cfg.deadline_ms),
            lease_seed: None,
            inhibit_sleep: self.cfg.inhibit_sleep,
            spool_root: None,
            log_limit_bytes: crate::supervisor::spec::DEFAULT_LOG_LIMIT_BYTES,
            workspace_warnings: ws.warnings.clone(),
            // no secret reaches a builder; an explicit empty list says so
            mask: MaskValues(Vec::new()),
            artifacts: None,
        };
        sup.validate().map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec(&sup).map_err(|e| e.to_string())?;
        write_private(&dir.join("spec.json"), &bytes).map_err(|e| e.to_string())
    }
}

fn private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("a file has a parent");
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// The entries directly under each of `dirs` (relative to `root`), as paths
/// relative to `root`; Git's own are not listed.
fn top_level_entries(root: &Path, dirs: &BTreeSet<String>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for d in dirs {
        let Ok(rd) = std::fs::read_dir(root.join(d)) else {
            continue;
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == ".git" {
                continue;
            }
            out.insert(if d.is_empty() {
                name
            } else {
                format!("{d}/{name}")
            });
        }
    }
    out
}
