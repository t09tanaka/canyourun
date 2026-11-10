//! The worker agent: every worker-side operation of spec chapter 11 over the
//! worker database, and the life of an accepted attempt up to its supervisor.
//!
//! The agent never runs a user command. It persists the acceptance, prepares
//! the workspace, writes the supervisor's spec and starts `supervise` as a
//! detached process; from then on the supervisor owns the attempt and the
//! agent only watches. One thread per attempt (`drive`) does this, and the
//! same loop is what a restarted agent runs for the attempts it finds: a
//! living supervisor is left alone (adopted), a dead one is replaced by a
//! `supervise` that recovers and never spawns the command again.
//!
//! Everything here blocks (SQLite, files, child processes): [`super::serve`]
//! calls it through `spawn_blocking`.

use super::host::Host;
use super::policy::{self, AttemptFacts, Recovery, Settle, Step};
use crate::error::{ApiError, ErrorCode};
use crate::ids::*;
use crate::platform;
use crate::state::{AttemptState, Observation, Outcome, Receipt, Termination};
use crate::storage::{
    AcceptedAttempt, CancelOutcome, GcReport, LeaseOutcome, NewAcceptance, ProcessLock,
    StorageError, SubmitOutcome, WorkerStore,
};
use crate::supervisor::lock::SupervisorLock;
use crate::supervisor::outcome::drain_pending_deletes;
use crate::supervisor::process::spawn_detached;
use crate::supervisor::spec::{NodeSpec, SuperviseSpec};
use crate::supervisor::spool::{SpoolError, SpoolReader};
use crate::supervisor::termination::{termination, StopReason};
use crate::wire::*;
use crate::workspace::plan::check_cwd_relative;
use crate::workspace::Workspace;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// What an agent is started with. Nothing is read from the process
/// environment: the boot ID and the supervisor binary are inputs.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// `worker.db`, the attempts and the locks live here (mode 0700).
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    /// This boot of the machine (`platform::boot_id()` in production).
    pub boot_id: WorkerBootId,
    /// The binary whose `supervise --spec <file>` runs an attempt.
    pub supervisor_exe: PathBuf,
    pub traffic_state_dir: PathBuf,
    pub traffic_config: Option<PathBuf>,
    pub profile: String,
    pub profile_fingerprint: Digest,
    pub max_active_task: u32,
    pub prefetch_per_worker: u32,
    pub term_grace_ms: u64,
    pub kill_wait_ms: u64,
    /// How often the supervisors and the agent's own loops look again.
    pub poll_ms: u64,
    pub inhibit_sleep: bool,
    /// How long `retire_attempt` waits for the stop before it answers
    /// `unconfirmed` (the coordinator asks again).
    pub retire_wait_ms: u64,
    /// How long a prepared attempt waits for its secret bundle.
    pub secret_wait_ms: u64,
    /// Pause before a supervisor that could not settle is started again.
    pub retry_ms: u64,
    /// How long a start waits for the agent lock. A process that was forked
    /// by the previous agent holds the lock's descriptor until its `exec`,
    /// so the lock of an agent that just went away can look held for a
    /// moment; a living agent holds it for good and the start is refused.
    pub lock_wait_ms: u64,
    /// Files of ended attempts are kept this long (spec 13: seven days).
    pub retention_ms: u64,
    /// Without `ack_collected`, the protection ends this long after the end.
    pub ack_wait_ms: u64,
}

impl AgentConfig {
    pub fn new(
        state_dir: PathBuf,
        worker_id: WorkerId,
        boot_id: WorkerBootId,
        supervisor_exe: PathBuf,
    ) -> Self {
        AgentConfig {
            traffic_state_dir: state_dir.join("traffic"),
            state_dir,
            worker_id,
            boot_id,
            supervisor_exe,
            traffic_config: None,
            profile: "default".into(),
            profile_fingerprint: Digest::of_bytes(b""),
            max_active_task: 1,
            prefetch_per_worker: 1,
            term_grace_ms: crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 5_000,
            poll_ms: 200,
            inhibit_sleep: true,
            retire_wait_ms: 2_000,
            secret_wait_ms: 60_000,
            retry_ms: 5_000,
            lock_wait_ms: 2_000,
            retention_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
            ack_wait_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
        }
    }

    /// Where each attempt keeps its files: `<root>/<attempt>/` holds the
    /// supervisor spec, the workspace record, `logs/` and `artifacts/`.
    pub fn attempts_root(&self) -> PathBuf {
        self.state_dir.join("attempts")
    }

    /// The agent's Unix socket.
    pub fn socket_path(&self) -> PathBuf {
        self.state_dir.join("agent.sock")
    }
}

/// What the connection's `hello` established.
#[derive(Debug, Clone, Copy)]
pub struct Session {
    pub coordinator_epoch: CoordinatorEpoch,
}

/// What a startup found and did, for the log and the tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupReport {
    pub steps: Vec<Step>,
    pub boot_changed: bool,
    /// Attempts fixed as interrupted or cancelled without a supervisor.
    pub settled: Vec<AttemptId>,
    /// Attempts handed to a watcher thread (adopt, recover or resume).
    pub watched: Vec<AttemptId>,
    /// Attempts that may have run and cannot be recovered.
    pub unresolved: Vec<AttemptId>,
}

pub struct Agent {
    cfg: AgentConfig,
    store: WorkerStore,
    host: Arc<dyn Host>,
    epoch: AgentEpoch,
    lock: Mutex<Option<ProcessLock>>,
    driving: Mutex<BTreeSet<String>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    stopping: AtomicBool,
    report: StartupReport,
}

fn now() -> u64 {
    platform::wall_ms()
}

fn api(e: StorageError) -> ApiError {
    // A busy database committed nothing: the same request may be sent again.
    let code = if e.is_retryable() {
        ErrorCode::Overloaded
    } else {
        e.code()
    };
    ApiError::new(code, e.to_string())
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("a file has a parent");
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// `<dir>` exists, is a directory of this user's, and nobody else can enter.
fn private_dir(dir: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

impl Agent {
    /// Run the startup steps in [`policy::STARTUP_ORDER`] and return the
    /// running agent. Any failing step refuses the start.
    pub fn start(cfg: AgentConfig, host: Arc<dyn Host>) -> Result<Arc<Agent>, ApiError> {
        let mut lock = None;
        let mut opened: Option<(WorkerStore, AgentEpoch)> = None;
        let mut report = StartupReport::default();
        let mut todo: Vec<AttemptId> = Vec::new();
        let boot_file = cfg.state_dir.join("boot_id");
        for step in policy::STARTUP_ORDER {
            let store = opened.as_ref().map(|o| &o.0);
            match step {
                Step::StateDir => {
                    private_dir(&cfg.state_dir).map_err(|e| internal("state directory", e))?;
                    private_dir(&cfg.attempts_root())
                        .map_err(|e| internal("attempts directory", e))?;
                }
                Step::CoreDumps => host.disable_core_dumps()?,
                Step::Lock => lock = Some(acquire_lock(&cfg)?),
                Step::Store => {
                    let s = WorkerStore::open(&cfg.state_dir, &cfg.worker_id).map_err(api)?;
                    let e = s.start_agent(now()).map_err(api)?;
                    let recorded = match fs::read_to_string(&boot_file) {
                        Ok(t) => Some(t),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                        Err(e) => return Err(internal("boot id record", e)),
                    };
                    report.boot_changed =
                        policy::boot_changed(recorded.as_deref(), cfg.boot_id.as_str());
                    opened = Some((s, e));
                }
                Step::StoreSecretCopies => host.sweep_store_copies()?,
                Step::Vault => host.sweep_vault()?,
                Step::PlacedSecrets => {
                    let s = store.expect("the store is open");
                    // Unknown or unreadable counts as ended: fail closed.
                    host.reconcile_secrets(
                        &|a| !matches!(s.get(a), Ok(Some(r)) if r.state != AttemptState::Terminal),
                    )?
                }
                Step::Workspaces => {
                    let s = store.expect("the store is open");
                    // An unreadable record keeps the workspace.
                    host.sweep_workspaces(&|a| {
                        !matches!(s.get(a), Ok(None))
                            && !matches!(s.get(a), Ok(Some(r)) if r.state == AttemptState::Terminal)
                    })?
                }
                Step::Attempts => {
                    let s = store.expect("the store is open");
                    for rec in s.list_nonterminal().map_err(api)? {
                        let a = rec.attempt_id.clone();
                        match recovery_of(&cfg, s, &rec, report.boot_changed)? {
                            Recovery::Settle(how) => {
                                if settle(&cfg, s, &a, how_termination(how), how)? {
                                    report.settled.push(a.clone());
                                }
                                // Settled or held by a living supervisor:
                                // the watcher cleans up or adopts.
                                todo.push(a);
                            }
                            Recovery::Unresolved => {
                                note(s, &rec, "agent_unresolved", json!({"why": "no spec"}));
                                report.unresolved.push(a);
                            }
                            _ => todo.push(a),
                        }
                    }
                }
                Step::BootRecord => write_atomic(&boot_file, cfg.boot_id.as_str().as_bytes())
                    .map_err(|e| internal("boot id record", e))?,
            }
            report.steps.push(step);
        }
        let (store, epoch) = opened.expect("the store step ran");
        report.watched = todo.clone();
        let agent = Arc::new(Agent {
            cfg,
            store,
            host,
            epoch,
            lock: Mutex::new(lock),
            driving: Mutex::default(),
            threads: Mutex::default(),
            stopping: AtomicBool::new(false),
            report,
        });
        for a in todo {
            agent.kick(&a);
        }
        // Attempts that ended while no agent ran still hold a workspace.
        agent.clean_ended();
        Ok(agent)
    }

    pub fn config(&self) -> &AgentConfig {
        &self.cfg
    }

    pub fn store(&self) -> &WorkerStore {
        &self.store
    }

    pub fn agent_epoch(&self) -> AgentEpoch {
        self.epoch
    }

    pub fn startup_report(&self) -> &StartupReport {
        &self.report
    }

    /// Stop watching and release the agent lock. Supervisors keep running:
    /// they do not depend on the agent (spec chapter 12).
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let threads = std::mem::take(&mut *self.threads.lock().expect("threads"));
        for t in threads {
            let _ = t.join();
        }
        self.lock.lock().expect("lock").take();
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn pause(&self, ms: u64) {
        let step = self.cfg.poll_ms.clamp(1, 100);
        let mut left = ms;
        while left > 0 && !self.stopping() {
            let d = left.min(step);
            std::thread::sleep(Duration::from_millis(d));
            left -= d;
        }
    }

    // ------------------------------------------------------------ attempts

    /// Make sure a thread watches `attempt`. Idempotent.
    pub fn kick(self: &Arc<Self>, attempt: &AttemptId) {
        if self.stopping() {
            return;
        }
        if !self
            .driving
            .lock()
            .expect("driving")
            .insert(attempt.to_string())
        {
            return;
        }
        let (me, a) = (self.clone(), attempt.clone());
        let t = std::thread::spawn(move || {
            me.drive(&a);
            me.driving.lock().expect("driving").remove(a.as_str());
        });
        let mut threads = self.threads.lock().expect("threads");
        threads.retain(|t| !t.is_finished());
        threads.push(t);
    }

    fn drive(&self, attempt: &AttemptId) {
        let mut child: Option<Child> = None;
        let mut noted_exit = false;
        while !self.stopping() {
            let rec = match self.store.get(attempt) {
                Ok(Some(r)) => r,
                Ok(None) => return,
                Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            };
            if rec.state == AttemptState::Terminal {
                if let Some(c) = child.as_mut() {
                    // Reap it; it is about to exit.
                    let _ = c.try_wait();
                }
                self.clean(attempt);
                return;
            }
            // A supervisor we started and that still runs owns the attempt.
            if let Some(c) = child.as_mut() {
                match c.try_wait() {
                    Ok(None) => {
                        self.pause(self.cfg.poll_ms);
                        continue;
                    }
                    Ok(Some(status)) => {
                        let code = status.code();
                        child = None;
                        if code != Some(0) && code != Some(4) && !noted_exit {
                            noted_exit = true;
                            note(
                                &self.store,
                                &rec,
                                "agent_supervisor_exit",
                                json!({ "code": code }),
                            );
                        }
                        if code != Some(0) && code != Some(4) {
                            self.pause(self.cfg.retry_ms);
                        }
                        continue;
                    }
                    Err(_) => child = None,
                }
            }
            // The lock tells whether any supervisor (ours or an adopted one)
            // lives. Only this thread starts supervisors for the attempt, so
            // taking it for a moment cannot make a new one give up.
            match SupervisorLock::try_acquire(&self.cfg.state_dir, attempt) {
                Ok(Some(lock)) => drop(lock),
                Ok(None) | Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            }
            let step = recovery_of(&self.cfg, &self.store, &rec, false);
            match step {
                Ok(Recovery::Nothing) | Err(_) => self.pause(self.cfg.poll_ms),
                Ok(Recovery::Unresolved) => return,
                Ok(Recovery::Settle(how)) => {
                    let _ = settle(&self.cfg, &self.store, attempt, how_termination(how), how);
                }
                Ok(Recovery::Supervise) => child = self.spawn(&rec),
                Ok(Recovery::Resume) => {
                    if rec.state == AttemptState::Ready && self.spec_path(attempt).is_file() {
                        child = self.spawn(&rec);
                    } else if let Err(e) = self.prepare(&rec) {
                        note(
                            &self.store,
                            &rec,
                            "agent_prepare_failed",
                            json!({ "code": e.code, "message": e.message }),
                        );
                        let failed = Termination::new(Outcome::Failed, None, None)
                            .expect("failed without an exit is valid");
                        let _ =
                            settle(&self.cfg, &self.store, attempt, failed, Settle::Interrupted);
                    }
                }
            }
        }
    }

    fn spawn(&self, rec: &AcceptedAttempt) -> Option<Child> {
        match spawn_detached(&self.cfg.supervisor_exe, &self.spec_path(&rec.attempt_id)) {
            Ok(c) => Some(c),
            Err(e) => {
                note(
                    &self.store,
                    rec,
                    "agent_spawn_failed",
                    json!({ "error": e.to_string() }),
                );
                self.pause(self.cfg.retry_ms);
                None
            }
        }
    }

    fn attempt_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.cfg.attempts_root().join(attempt.as_str())
    }

    fn spec_path(&self, attempt: &AttemptId) -> PathBuf {
        spec_path(&self.cfg, attempt)
    }

    fn workspace_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("workspace.json")
    }

    fn workspace_of(&self, attempt: &AttemptId) -> Option<Workspace> {
        let text = fs::read_to_string(self.workspace_path(attempt)).ok()?;
        serde_json::from_str(&text).ok()
    }

    /// `Accepted | Preparing -> Ready`: the workspace, the secret files, the
    /// supervisor's spec. No command is started here.
    fn prepare(&self, rec: &AcceptedAttempt) -> Result<(), ApiError> {
        let attempt = &rec.attempt_id;
        let payload: AttemptPayload =
            serde_json::from_value(rec.spec.clone()).map_err(|e| internal("stored payload", e))?;
        if rec.state == AttemptState::Accepted {
            self.store
                .transition(attempt, AttemptState::Preparing, now())
                .map_err(api)?;
        }
        let dir = self.attempt_dir(attempt);
        private_dir(&dir).map_err(|e| internal("attempt directory", e))?;
        // An agent that died after making the workspace left its record.
        let ws = match self.workspace_of(attempt) {
            Some(ws) if ws.root.is_dir() => ws,
            _ => self.host.prepare(rec, &payload)?,
        };
        let ws_json = serde_json::to_vec(&ws).map_err(|e| internal("workspace record", e))?;
        write_atomic(&self.workspace_path(attempt), &ws_json)
            .map_err(|e| internal("workspace record", e))?;

        // The secret bundle arrives by its own request once the workspace is
        // there; the commands do not start before the set is complete.
        let mut waited = 0;
        while !self.host.secrets_ready(rec)? {
            if self.stopping() || self.stop_requested(attempt) {
                return Ok(());
            }
            if waited >= self.cfg.secret_wait_ms {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::SecretUnavailable,
                    "the secret bundle did not arrive",
                ));
            }
            self.pause(self.cfg.poll_ms);
            waited += self.cfg.poll_ms;
        }

        let mut nodes = Vec::with_capacity(payload.commands.len());
        for c in &payload.commands {
            let rel = Path::new(&c.cwd);
            check_cwd_relative(rel)?;
            nodes.push(NodeSpec {
                node_id: c.node_id.clone(),
                argv: c.argv.clone(),
                cwd: ws.cwd.join(rel),
                env: c.env.clone(),
                stdout_log: None,
                weight: None,
            });
        }
        let spec = SuperviseSpec {
            state_dir: self.cfg.state_dir.clone(),
            worker_id: self.cfg.worker_id.clone(),
            attempt_id: attempt.clone(),
            traffic_state_dir: self.cfg.traffic_state_dir.clone(),
            traffic_config: self.cfg.traffic_config.clone(),
            nodes,
            lease_ms: payload.lease_ms,
            term_grace_ms: self.cfg.term_grace_ms,
            kill_wait_ms: self.cfg.kill_wait_ms,
            poll_ms: self.cfg.poll_ms,
            command_timeout_ms: payload.command_timeout_ms,
            resource_wait_timeout_ms: payload.resource_wait_timeout_ms,
            deadline_ms: payload.deadline_ms,
            inhibit_sleep: self.cfg.inhibit_sleep,
        };
        spec.validate().map_err(|e| invalid(e.to_string()))?;
        let bytes = serde_json::to_vec(&spec).map_err(|e| internal("supervisor spec", e))?;
        write_atomic(&self.spec_path(attempt), &bytes)
            .map_err(|e| internal("supervisor spec", e))?;
        if self.stop_requested(attempt) {
            // The loop settles it as cancelled; nothing is started.
            return Ok(());
        }
        self.store
            .transition(attempt, AttemptState::Ready, now())
            .map_err(api)?;
        Ok(())
    }

    fn stop_requested(&self, attempt: &AttemptId) -> bool {
        match self.store.get(attempt) {
            Ok(Some(r)) => r.cancel_requested_at_ms.is_some() || r.retired,
            // Unknown is not a licence to start.
            _ => true,
        }
    }

    /// After the end: secret files and the workspace go at once (spec 22.3
    /// does not wait for the logs to be collected); logs, artifacts and the
    /// result stay until GC.
    fn clean(&self, attempt: &AttemptId) {
        let ws = self.workspace_of(attempt);
        let marker = self.workspace_path(attempt);
        if ws.is_none() && marker.exists() {
            // An unreadable record: keep it, a person has to look.
            return;
        }
        match self.host.cleanup(attempt, ws.as_ref()) {
            Ok(()) => {
                let _ = fs::remove_file(marker);
                let _ = fs::remove_file(self.spec_path(attempt));
                let _ = fs::remove_dir(self.attempt_dir(attempt));
            }
            Err(e) => {
                if let Ok(Some(rec)) = self.store.get(attempt) {
                    note(
                        &self.store,
                        &rec,
                        "agent_cleanup_failed",
                        json!({ "code": e.code, "message": e.message }),
                    );
                }
            }
        }
    }

    /// Clean every ended attempt that still has a workspace record.
    pub fn clean_ended(&self) {
        let Ok(rd) = fs::read_dir(self.cfg.attempts_root()) else {
            return;
        };
        for ent in rd.flatten() {
            let Ok(a) = AttemptId::parse(&ent.file_name().to_string_lossy()) else {
                continue;
            };
            if !self.workspace_path(&a).exists() {
                continue;
            }
            if matches!(self.store.get(&a), Ok(Some(r)) if r.state == AttemptState::Terminal) {
                self.clean(&a);
            }
        }
    }

    /// One GC pass: drop what the store says may go (never an attempt that
    /// runs or waits for `ack_collected`), then delete the files.
    pub fn gc(&self) -> Result<GcReport, ApiError> {
        let report = self
            .store
            .gc(now(), self.cfg.retention_ms, self.cfg.ack_wait_ms)
            .map_err(api)?;
        drain_pending_deletes(&self.store, &self.cfg.attempts_root())
            .map_err(|e| internal("gc", e))?;
        Ok(report)
    }

    /// Periodic work: watch attempts nobody watches, clean, collect garbage.
    pub fn tick(self: &Arc<Self>) {
        if let Ok(list) = self.store.list_nonterminal() {
            for rec in list {
                self.kick(&rec.attempt_id);
            }
        }
        self.clean_ended();
        let _ = self.gc();
    }

    // ---------------------------------------------------------- operations

    /// Answer one request. Mutations are committed before this returns.
    pub fn handle(
        self: &Arc<Self>,
        session: Session,
        request: Request,
    ) -> Result<Response, ApiError> {
        request
            .ensure_current_schema()
            .map_err(|e| ApiError::new(ErrorCode::ProtocolIncompatible, e.to_string()))?;
        match request {
            Request::Hello(_) => Err(invalid("hello is the first request of a connection")),
            Request::Inspect(_) => self.inspect().map(Response::Inspect),
            Request::SubmitAttempt(r) => self.submit(r).map(Response::SubmitAttempt),
            Request::GetAttempt(r) => {
                let rec = self.known(&r.attempt_id)?;
                self.status(&rec).map(Response::GetAttempt)
            }
            Request::ListAttempts(r) => self.list(r).map(Response::ListAttempts),
            Request::RenewLease(r) => self.renew(r).map(Response::RenewLease),
            Request::CancelAttempt(r) => self.cancel(r).map(Response::CancelAttempt),
            Request::RetireAttempt(r) => self.retire(r).map(Response::RetireAttempt),
            Request::WatchEvents(r) => self.watch(r).map(Response::WatchEvents),
            Request::ReadLog(r) => self.read_log(r).map(Response::ReadLog),
            Request::GetResult(r) => self.result(r).map(Response::GetResult),
            Request::AckCollected(r) => self.ack(session, r).map(Response::AckCollected),
            Request::SecretBundlePut(r) => self.secrets(r).map(Response::SecretBundlePut),
            Request::Forecast(_)
            | Request::BlobPut(_)
            | Request::BlobGet(_)
            | Request::BlobVerify(_)
            | Request::EnvReport(_)
            | Request::EnvSync(_) => Err(ApiError::new(
                ErrorCode::NotImplemented,
                "this operation is not implemented by the worker agent yet",
            )),
        }
    }

    /// The record, or why there is none: `RETIRED` for a tombstone without an
    /// attempt, else `NOT_FOUND`.
    fn known(&self, attempt: &AttemptId) -> Result<AcceptedAttempt, ApiError> {
        if let Some(rec) = self.store.get(attempt).map_err(api)? {
            return Ok(rec);
        }
        if self.store.tombstone(attempt).map_err(api)?.is_some() {
            return Err(ApiError::new(
                ErrorCode::Retired,
                format!("attempt {attempt} has a tombstone and was never accepted"),
            ));
        }
        Err(ApiError::new(
            ErrorCode::NotFound,
            format!("attempt {attempt} is unknown"),
        ))
    }

    fn generation(rec: &AcceptedAttempt, got: Generation) -> Result<(), ApiError> {
        if rec.generation == got {
            Ok(())
        } else {
            Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "attempt {} is generation {}, not {}",
                    rec.attempt_id, rec.generation.0, got.0
                ),
            ))
        }
    }

    fn submit(
        self: &Arc<Self>,
        r: SubmitAttemptRequest,
    ) -> Result<SubmitAttemptResponse, ApiError> {
        // The claimed hash is the sender's word: recompute it.
        if !r.hash_matches().map_err(|e| invalid(e.to_string()))? {
            return Err(invalid("payload_hash does not match the payload"));
        }
        policy::check_payload(&r.payload).map_err(invalid)?;
        for c in &r.payload.commands {
            check_cwd_relative(Path::new(&c.cwd))?;
        }
        let spec = serde_json::to_value(&r.payload).map_err(|e| internal("payload", e))?;
        let outcome = self
            .store
            .submit(
                NewAcceptance {
                    attempt_id: r.attempt_id.clone(),
                    run_id: r.payload.run_id.clone(),
                    task_id: r.payload.task_id.clone(),
                    generation: r.payload.generation,
                    payload_hash: r.payload_hash,
                    coordinator_epoch: r.coordinator_epoch,
                    spec,
                },
                now(),
            )
            .map_err(api)?;
        // The acceptance is committed; only now may anything start.
        let (rec, duplicate) = match outcome {
            SubmitOutcome::Accepted(rec) => (rec, false),
            SubmitOutcome::Existing(rec) => (rec, true),
        };
        if rec.state != AttemptState::Terminal {
            self.kick(&rec.attempt_id);
        }
        Ok(SubmitAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: rec.attempt_id,
            generation: rec.generation,
            state: rec.state,
            receipts: rec.receipts,
            duplicate,
        })
    }

    fn status(&self, rec: &AcceptedAttempt) -> Result<AttemptStatus, ApiError> {
        let a = &rec.attempt_id;
        let out = self.store.log_position(a, "stdout").map_err(api)?;
        let err = self.store.log_position(a, "stderr").map_err(api)?;
        let dropped = out.dropped_bytes.saturating_add(err.dropped_bytes);
        Ok(AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: rec.run_id.clone(),
            task_id: rec.task_id.clone(),
            generation: rec.generation,
            worker_boot_id: self.cfg.boot_id.clone(),
            agent_epoch: self.epoch,
            state: rec.state,
            receipts: rec.receipts,
            spawned_at_ms: rec.receipts.at_ms(Receipt::Running),
            observation: Observation::Known,
            termination: rec.termination,
            nodes: Vec::new(),
            steps: Vec::new(),
            retired: rec.retired,
            log_truncated: dropped > 0,
            log_dropped_bytes: dropped,
        })
    }

    fn list(&self, r: ListAttemptsRequest) -> Result<ListAttemptsResponse, ApiError> {
        let recs = self
            .store
            .list_attempts(r.include_terminal, policy::MAX_LIST_ATTEMPTS)
            .map_err(api)?;
        let mut attempts = Vec::new();
        for rec in recs {
            if r.states.as_ref().is_none_or(|s| s.contains(&rec.state)) {
                attempts.push(self.status(&rec)?);
            }
        }
        Ok(ListAttemptsResponse {
            schema_version: SchemaVersion::CURRENT,
            attempts,
        })
    }

    fn renew(&self, r: RenewLeaseRequest) -> Result<RenewLeaseResponse, ApiError> {
        let rec = self.known(&r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        let outcome = self
            .store
            .renew_lease(&r.attempt_id, r.key(), now())
            .map_err(api)?;
        let extended = outcome == LeaseOutcome::Extended;
        let lease_ms = serde_json::from_value::<AttemptPayload>(rec.spec)
            .map(|p| p.lease_ms)
            .unwrap_or(0);
        Ok(RenewLeaseResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            lease_seq: r.lease_seq,
            extended,
            // The supervisor counts the lease on its own monotonic clock; a
            // renewal that extended starts a full period there.
            lease_remaining_ms: if extended { lease_ms } else { 0 },
            state: rec.state,
        })
    }

    fn cancel(
        self: &Arc<Self>,
        r: CancelAttemptRequest,
    ) -> Result<CancelAttemptResponse, ApiError> {
        if let Some(rec) = self.store.get(&r.attempt_id).map_err(api)? {
            Self::generation(&rec, r.generation)?;
        }
        // Persisted first; the supervisor (or the watcher, before a
        // supervisor exists) acts on it.
        let outcome = self
            .store
            .cancel(&r.attempt_id, r.coordinator_epoch, now())
            .map_err(api)?;
        let (state, tombstone) = match outcome {
            CancelOutcome::Requested(rec) | CancelOutcome::AlreadyRequested(rec) => {
                if rec.state != AttemptState::Terminal {
                    self.kick(&rec.attempt_id);
                }
                (Some(rec.state), false)
            }
            CancelOutcome::Tombstoned => (None, true),
        };
        Ok(CancelAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            phase: policy::cancel_phase(state),
            state,
            tombstone,
        })
    }

    fn retire(
        self: &Arc<Self>,
        r: RetireAttemptRequest,
    ) -> Result<RetireAttemptResponse, ApiError> {
        let a = &r.attempt_id;
        let before = self.store.get(a).map_err(api)?;
        if let Some(rec) = &before {
            Self::generation(rec, r.generation)?;
        }
        let was_terminal = before
            .as_ref()
            .is_some_and(|b| b.state == AttemptState::Terminal);
        // The tombstone is durable before anything else: a submit that
        // arrives late is refused from here on.
        let known = self
            .store
            .retire(a, r.coordinator_epoch, now())
            .map_err(api)?
            .is_some();
        let mut now_terminal = was_terminal;
        if known && !was_terminal {
            self.kick(a);
            let mut waited = 0;
            loop {
                now_terminal = matches!(
                    self.store.get(a).map_err(api)?,
                    Some(rec) if rec.state == AttemptState::Terminal
                );
                if now_terminal || waited >= self.cfg.retire_wait_ms || self.stopping() {
                    break;
                }
                let step = self.cfg.poll_ms.clamp(1, 50);
                std::thread::sleep(Duration::from_millis(step));
                waited += step;
            }
        }
        let had_launches = known && !self.store.launches(a).map_err(api)?.is_empty();
        Ok(RetireAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            tombstone: true,
            status: policy::retire_status(known, was_terminal, now_terminal, had_launches),
        })
    }

    fn watch(&self, r: WatchEventsRequest) -> Result<WatchEventsResponse, ApiError> {
        // The wire `seq` is per attempt, so one cursor follows one attempt.
        let Some(a) = r.attempt_id else {
            return Err(invalid("watch_events needs an attempt_id"));
        };
        self.known(&a)?;
        let journal = self
            .store
            .events_of_attempt(&a, policy::MAX_EVENT_SCAN)
            .map_err(api)?;
        let wire: Vec<_> = journal
            .iter()
            .map(|e| policy::wire_event(&e.kind, &e.payload))
            .collect();
        let is_wire: Vec<bool> = wire.iter().map(Option::is_some).collect();
        let mut next_seq = r.after_seq;
        let mut events = Vec::new();
        for (seq, i) in policy::select_events(&is_wire, r.after_seq, r.limit) {
            let w = wire[i].expect("selected entries are wire events");
            next_seq = seq;
            events.push(AttemptEvent {
                seq,
                attempt_id: a.clone(),
                kind: w.kind,
                state: w.state,
                receipt: w.receipt,
                node_id: None,
                step: None,
            });
        }
        Ok(WatchEventsResponse {
            schema_version: SchemaVersion::CURRENT,
            events,
            next_seq,
        })
    }

    fn read_log(&self, r: ReadLogRequest) -> Result<ReadLogResponse, ApiError> {
        let rec = self.known(&r.attempt_id)?;
        let reader = match SpoolReader::open(&self.cfg.attempts_root(), &r.attempt_id) {
            Ok(reader) => reader,
            // Nothing was written yet. Only an ended attempt has no more to
            // come.
            Err(SpoolError::NotFound(_)) => {
                return Ok(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: r.offset,
                    data: Bytes(Vec::new()),
                    next_offset: r.offset,
                    eof: rec.state == AttemptState::Terminal,
                    log_truncated: false,
                    dropped_bytes: 0,
                })
            }
            Err(e) => return Err(internal("log spool", e)),
        };
        let chunk = reader
            .read(r.stream, r.offset, r.max_bytes)
            .map_err(|e| internal("log spool", e))?;
        Ok(ReadLogResponse {
            schema_version: SchemaVersion::CURRENT,
            offset: chunk.offset,
            data: Bytes(chunk.data),
            next_offset: chunk.next_offset,
            eof: chunk.eof,
            log_truncated: chunk.log_truncated,
            dropped_bytes: chunk.dropped_bytes,
        })
    }

    /// The ended attempt's result and its digest. The stored result (written
    /// by the supervisor) is the truth when there is one; an attempt that
    /// ended without one (settled by the agent or by recovery) has its
    /// termination and nothing else.
    fn ended(
        &self,
        attempt: &AttemptId,
        generation: Generation,
    ) -> Result<(Termination, Vec<ArtifactRecord>, Digest), ApiError> {
        let rec = self.known(attempt)?;
        Self::generation(&rec, generation)?;
        let Some(t) = rec.termination else {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("attempt {attempt} has not ended"),
            ));
        };
        let stored = self.store.get_result(attempt).map_err(api)?;
        let artifacts: Vec<ArtifactRecord> = match &stored {
            Some(v) => serde_json::from_value(v["artifacts"].clone())
                .map_err(|e| internal("stored artifacts", e))?,
            None => Vec::new(),
        };
        let value = stored.unwrap_or_else(|| json!({ "termination": t, "artifacts": [] }));
        let digest = payload_hash(&value).map_err(|e| internal("result digest", e))?;
        Ok((t, artifacts, digest))
    }

    fn result(&self, r: GetResultRequest) -> Result<GetResultResponse, ApiError> {
        let (termination, artifacts, result_digest) = self.ended(&r.attempt_id, r.generation)?;
        Ok(GetResultResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            generation: r.generation,
            termination,
            nodes: Vec::new(),
            artifacts,
            result_digest,
        })
    }

    fn ack(
        &self,
        session: Session,
        r: AckCollectedRequest,
    ) -> Result<AckCollectedResponse, ApiError> {
        let (_, _, digest) = self.ended(&r.attempt_id, r.generation)?;
        if digest != r.result_digest {
            // The parent stored something else than what is here: the
            // protection stays.
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "result_digest is not the digest of this attempt's result",
            ));
        }
        self.store
            .ack_collected(&r.attempt_id, session.coordinator_epoch, now())
            .map_err(api)?;
        Ok(AckCollectedResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            released: true,
        })
    }

    fn secrets(&self, r: SecretBundlePutRequest) -> Result<SecretBundlePutResponse, ApiError> {
        let rec = self.known(&r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        if rec.retired || rec.cancel_requested_at_ms.is_some() {
            return Err(ApiError::new(
                ErrorCode::Retired,
                "the attempt is being stopped",
            ));
        }
        if !matches!(rec.state, AttemptState::Accepted | AttemptState::Preparing) {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "attempt {} is {:?}: secrets go in before the start",
                    rec.attempt_id, rec.state
                ),
            ));
        }
        let Some(ws) = self.workspace_of(&r.attempt_id) else {
            return Err(ApiError::new(
                ErrorCode::Overloaded,
                "the workspace is not prepared yet; send the bundle again",
            ));
        };
        let placed = self.host.place_secrets(&rec, &ws, &r.files)?;
        Ok(SecretBundlePutResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            verified: true,
            placed,
        })
    }

    fn inspect(&self) -> Result<InspectResponse, ApiError> {
        let live = self.store.list_nonterminal().map_err(api)?;
        let (active, pending) = policy::load_counts(live.iter().map(|r| r.state));
        let policy_bytes = self
            .cfg
            .traffic_config
            .as_ref()
            .and_then(|p| fs::read(p).ok())
            .unwrap_or_default();
        let disk = rustix::fs::statvfs(&self.cfg.state_dir)
            .map(|s| s.f_bavail.saturating_mul(s.f_frsize))
            .unwrap_or(0);
        Ok(InspectResponse {
            schema_version: SchemaVersion::CURRENT,
            worker_id: self.cfg.worker_id.clone(),
            worker_boot_id: self.cfg.boot_id.clone(),
            agent_epoch: self.epoch,
            snapshot_digest: Digest::of_bytes(
                format!("{}/{active}/{pending}", self.epoch.0).as_bytes(),
            ),
            policy_digest: Digest::of_bytes(&policy_bytes),
            active_task: active,
            prefetch_pending: pending,
            max_active_task: self.cfg.max_active_task,
            prefetch_per_worker: self.cfg.prefetch_per_worker,
            disk_free_bytes: disk,
            draining: false,
        })
    }
}

fn acquire_lock(cfg: &AgentConfig) -> Result<ProcessLock, ApiError> {
    let mut waited = 0;
    loop {
        match ProcessLock::acquire(&cfg.state_dir, "agent.lock") {
            Err(StorageError::Locked(_)) if waited < cfg.lock_wait_ms => {
                std::thread::sleep(Duration::from_millis(50));
                waited += 50;
            }
            other => return other.map_err(api),
        }
    }
}

fn spec_path(cfg: &AgentConfig, attempt: &AttemptId) -> PathBuf {
    cfg.attempts_root()
        .join(attempt.as_str())
        .join("supervise.json")
}

fn recovery_of(
    cfg: &AgentConfig,
    store: &WorkerStore,
    rec: &AcceptedAttempt,
    boot_changed: bool,
) -> Result<Recovery, ApiError> {
    let facts = AttemptFacts {
        state: rec.state,
        has_launches: !store.launches(&rec.attempt_id).map_err(api)?.is_empty(),
        stop_requested: rec.cancel_requested_at_ms.is_some() || rec.retired,
        has_spec: spec_path(cfg, &rec.attempt_id).is_file(),
    };
    Ok(policy::decide_recovery(boot_changed, facts))
}

fn how_termination(how: Settle) -> Termination {
    match how {
        Settle::Cancelled => termination(Some(StopReason::Cancel), None),
        Settle::Interrupted => termination(None, None),
    }
}

/// Fix the result of an attempt whose command was never started, holding the
/// supervisor lock so no supervisor can be starting it meanwhile. `false`
/// when a supervisor lives (it owns the attempt) or a launch appeared.
fn settle(
    cfg: &AgentConfig,
    store: &WorkerStore,
    attempt: &AttemptId,
    t: Termination,
    how: Settle,
) -> Result<bool, ApiError> {
    let lock = SupervisorLock::try_acquire(&cfg.state_dir, attempt)
        .map_err(|e| internal("supervisor lock", e))?;
    let Some(_lock) = lock else {
        return Ok(false);
    };
    if !store.launches(attempt).map_err(api)?.is_empty() {
        return Ok(false);
    }
    if how == Settle::Cancelled {
        // Already there, or past it: both are fine.
        let _ = store.transition(attempt, AttemptState::Cancelling, now());
    }
    crate::supervisor::run::finalize(store, attempt, t)
        .map_err(|e| internal("settling the attempt", e))?;
    Ok(true)
}

fn note(store: &WorkerStore, rec: &AcceptedAttempt, kind: &str, payload: Value) {
    let _ = store.append_event(
        Some(&rec.run_id),
        Some(&rec.attempt_id),
        kind,
        &payload,
        now(),
    );
}
