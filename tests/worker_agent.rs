//! The worker agent against real supervisors (separate processes) and real
//! connections: A18 (a resent attempt starts once), A19 (lost ACK, retire
//! tombstone), A23 (an agent restart adopts the supervisor), A24 (a changed
//! boot ID, simulated), GC protection until `ack_collected`, and the order of
//! the startup steps.

mod proto_common;

use proto_common::client_cfg;
use remote_task_runner::error::{ApiError, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{connect, CallError, Client};
use remote_task_runner::state::{AttemptState, Outcome};
use remote_task_runner::storage::{AcceptedAttempt, BlobKind, NewBlobRef, WorkerStore};
use remote_task_runner::wire::*;
use remote_task_runner::worker::policy::{Step, STARTUP_ORDER};
use remote_task_runner::worker::serve::serve_connection;
use remote_task_runner::worker::{Agent, AgentConfig, Host};
use remote_task_runner::workspace::{Workspace, WorkspaceKind};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Semaphore;

const T: Duration = Duration::from_secs(20);

/// The machine, reduced to plain directories, with a record of every call.
struct DirHost {
    base: PathBuf,
    calls: Mutex<Vec<String>>,
    secrets_ready: AtomicBool,
    fail_core_dumps: AtomicBool,
}

impl DirHost {
    fn log(&self, what: impl Into<String>) {
        self.calls.lock().unwrap().push(what.into());
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn count(&self, what: &str) -> usize {
        self.calls().iter().filter(|c| c.as_str() == what).count()
    }
    fn ws_dir(&self, a: &AttemptId) -> PathBuf {
        self.base.join("ws").join(a.as_str())
    }
}

impl Host for DirHost {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        self.log("core_dumps");
        if self.fail_core_dumps.load(Ordering::SeqCst) {
            return Err(ApiError::new(ErrorCode::Internal, "no rlimit"));
        }
        Ok(())
    }
    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        self.log("store_copies");
        Ok(())
    }
    fn sweep_vault(&self) -> Result<(), ApiError> {
        self.log("vault");
        Ok(())
    }
    fn reconcile_secrets(&self, erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        // An attempt nobody knows is erased.
        assert!(erase(&AttemptId::generate()));
        self.log("secrets");
        Ok(())
    }
    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        assert!(!has_record(&AttemptId::generate()));
        self.log("workspaces");
        Ok(())
    }
    fn prepare(&self, a: &AcceptedAttempt, _: &AttemptPayload) -> Result<Workspace, ApiError> {
        self.log(format!("prepare {}", a.attempt_id));
        let dir = self.ws_dir(&a.attempt_id);
        std::fs::create_dir_all(&dir).unwrap();
        Ok(Workspace {
            root: dir.clone(),
            cwd: dir,
            kind: WorkspaceKind::Directory,
            index_reproduced: true,
            warnings: Vec::new(),
        })
    }
    fn place_secrets(
        &self,
        a: &AcceptedAttempt,
        ws: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        self.log(format!("place {}", a.attempt_id));
        for f in files {
            std::fs::write(ws.root.join(&f.path), &f.content.0).unwrap();
        }
        self.secrets_ready.store(true, Ordering::SeqCst);
        Ok(files.len() as u32)
    }
    fn secrets_ready(&self, _: &AcceptedAttempt) -> Result<bool, ApiError> {
        Ok(self.secrets_ready.load(Ordering::SeqCst))
    }
    fn cleanup(&self, a: &AttemptId, ws: Option<&Workspace>) -> Result<(), ApiError> {
        self.log(format!("cleanup {a}"));
        if let Some(ws) = ws {
            let _ = std::fs::remove_dir_all(&ws.root);
        }
        Ok(())
    }
}

struct Fx {
    dir: TempDir,
    host: Arc<DirHost>,
    cluster: ClusterId,
    agents: Mutex<Vec<Arc<Agent>>>,
}

fn fx() -> Fx {
    // Not under the system temp directory: a supervisor a failed test leaves
    // behind must not write into a directory a test runner clears.
    let dir = tempfile::Builder::new()
        .prefix("rtr-agent-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let host = Arc::new(DirHost {
        base: dir.path().to_path_buf(),
        calls: Mutex::default(),
        secrets_ready: AtomicBool::new(true),
        fail_core_dumps: AtomicBool::new(false),
    });
    Fx {
        dir,
        host,
        cluster: ClusterId::generate(),
        agents: Mutex::default(),
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        for a in self.agents.lock().unwrap().iter() {
            a.shutdown();
        }
        // Whatever still runs for this fixture: supervisors (their spec path
        // is under the directory) and the commands they started.
        let state = self.state();
        if let Ok(store) = WorkerStore::open_read_only(&state) {
            for rec in store.list_attempts(true, 500).unwrap_or_default() {
                for (_, _, ident) in store.launches(&rec.attempt_id).unwrap_or_default() {
                    if let Some(i) = ident {
                        unsafe { libc::kill(-(i.pid as i32), libc::SIGKILL) };
                    }
                }
            }
        }
        for pid in pgrep(&state.to_string_lossy()) {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}

fn pgrep(pattern: &str) -> Vec<i32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", pattern])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn wait_for(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + T;
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Fx {
    fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
    fn cfg(&self, boot: &str) -> AgentConfig {
        let mut cfg = AgentConfig::new(
            self.state(),
            WorkerId::parse("w-test").unwrap(),
            WorkerBootId::parse(boot).unwrap(),
            PathBuf::from(env!("CARGO_BIN_EXE_remote-task-runner")),
        );
        cfg.poll_ms = 25;
        cfg.inhibit_sleep = false;
        cfg.retry_ms = 200;
        cfg.retire_wait_ms = 15_000;
        cfg.secret_wait_ms = 15_000;
        cfg.term_grace_ms = 2_000;
        cfg.kill_wait_ms = 2_000;
        cfg
    }
    fn start_with(&self, cfg: AgentConfig) -> Arc<Agent> {
        let agent = Agent::start(cfg, self.host.clone()).unwrap();
        self.agents.lock().unwrap().push(agent.clone());
        agent
    }
    fn start(&self, boot: &str) -> Arc<Agent> {
        self.start_with(self.cfg(boot))
    }
    /// A new connection to the agent: its own stream and its own `hello`.
    async fn connect(&self, agent: &Arc<Agent>) -> Client {
        let (a, b) = duplex_pair(64 * 1024);
        let permits = Arc::new(Semaphore::new(64));
        tokio::spawn(serve_connection(
            agent.clone(),
            b,
            self.cluster.clone(),
            permits,
        ));
        connect(a, client_cfg(&self.cluster)).await.unwrap().client
    }
    fn store(&self) -> WorkerStore {
        WorkerStore::open_read_only(&self.state()).unwrap()
    }
    fn rec(&self, a: &AttemptId) -> AcceptedAttempt {
        self.store().get(a).unwrap().unwrap()
    }
    fn wait_terminal(&self, a: &AttemptId) -> Outcome {
        let store = self.store();
        wait_for("the attempt to end", || {
            store.get(a).unwrap().unwrap().state == AttemptState::Terminal
        });
        self.rec(a).termination.unwrap().outcome()
    }
    fn supervisors(&self, a: &AttemptId) -> Vec<i32> {
        pgrep(&format!("{}/supervise.json", a.as_str()))
    }
}

fn payload(script: &str) -> AttemptPayload {
    let d = Digest::of_bytes(b"x");
    let mut env = BTreeMap::new();
    env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    AttemptPayload {
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: SourceId::generate(),
        plan_digest: d.clone(),
        manifest_digest: d,
        permit_id: None,
        commands: vec![CommandSpec {
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            cwd: String::new(),
            env,
            depends_on: vec![],
            timeout_ms: None,
        }],
        lease_ms: 60_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
    }
}

fn submit_req(a: &AttemptId, p: &AttemptPayload) -> Request {
    Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            p.clone(),
        )
        .unwrap(),
    )
}

async fn call(c: &Client, r: Request) -> Result<Response, ApiError> {
    match c.call(r, T).await {
        Ok(r) => Ok(r),
        Err(CallError::Remote(e)) => Err(e),
        Err(e) => panic!("{e:?}"),
    }
}

async fn submit(c: &Client, a: &AttemptId, p: &AttemptPayload) -> SubmitAttemptResponse {
    match call(c, submit_req(a, p)).await.unwrap() {
        Response::SubmitAttempt(r) => r,
        other => panic!("{other:?}"),
    }
}

fn cancel_req(a: &AttemptId) -> Request {
    Request::CancelAttempt(CancelAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        reason: None,
    })
}

fn retire_req(a: &AttemptId) -> Request {
    Request::RetireAttempt(RetireAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
    })
}

fn lines(p: &Path) -> usize {
    std::fs::read_to_string(p)
        .map(|t| t.lines().count())
        .unwrap_or(0)
}

/// A command that records each start and its PID, then stays.
fn stay(f: &Fx) -> (String, PathBuf, PathBuf) {
    let (marker, pidfile) = (f.path("starts"), f.path("pid"));
    let script = format!(
        "echo start >> {}; echo $$ > {}; exec sleep 300",
        marker.display(),
        pidfile.display()
    );
    (script, marker, pidfile)
}

fn read_pid(p: &Path) -> Option<i32> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

// ---------------------------------------------------------------- A18

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a18_an_attempt_resent_100_times_starts_its_command_once() {
    let f = fx();
    let agent = f.start("boot-a");
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}; sleep 0.3", marker.display()));
    let a = AttemptId::generate();
    let mut tasks = Vec::new();
    // Four connections, 25 submits each, all at once.
    for _ in 0..4 {
        let c = f.connect(&agent).await;
        let (a, p) = (a.clone(), p.clone());
        tasks.push(tokio::spawn(async move {
            let mut fresh = 0;
            for _ in 0..25 {
                let r = submit(&c, &a, &p).await;
                assert_eq!(r.attempt_id, a);
                fresh += usize::from(!r.duplicate);
            }
            fresh
        }));
    }
    let mut fresh = 0;
    for t in tasks {
        fresh += t.await.unwrap();
    }
    assert_eq!(fresh, 1, "exactly one submit is the acceptance");
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // Resends after the end change nothing either.
    let c = f.connect(&agent).await;
    let r = submit(&c, &a, &p).await;
    assert!(r.duplicate);
    assert_eq!(r.state, AttemptState::Terminal);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(lines(&marker), 1, "the command started once");
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
    assert_eq!(f.host.count(&format!("prepare {a}")), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_attempt_with_another_payload_is_a_conflict() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();
    assert!(!submit(&c, &a, &p).await.duplicate);
    assert!(submit(&c, &a, &p).await.duplicate);

    // Another payload under the same attempt id, with its own correct hash.
    let mut other = p.clone();
    other.commands[0].argv[2] = "echo other".into();
    let e = call(&c, submit_req(&a, &other)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);

    // A hash that is not the payload's is refused before anything is stored.
    let fresh = AttemptId::generate();
    let Request::SubmitAttempt(mut bad) = submit_req(&fresh, &p) else {
        unreachable!()
    };
    bad.payload.lease_ms += 1;
    let e = call(&c, Request::SubmitAttempt(bad)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(f.store().get(&fresh).unwrap().is_none());

    // A working directory that leaves the workspace is refused too.
    let mut escape = p.clone();
    escape.commands[0].cwd = "../x".into();
    let e = call(&c, submit_req(&AttemptId::generate(), &escape))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);

    // The stored attempt is still the first payload.
    let stored: AttemptPayload = serde_json::from_value(f.rec(&a).spec).unwrap();
    assert_eq!(stored, p);
    assert_eq!(lines(&marker), 0);
}

// ---------------------------------------------------------------- A19

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_submit_that_arrives_after_the_retire_is_refused() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();

    // The coordinator gave up on a submit it never saw answered and retires
    // the attempt before placing the task elsewhere.
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.status, RetireStatus::NotStarted);

    // The delayed submit arrives, on another connection, again and again.
    let late = f.connect(&agent).await;
    for _ in 0..3 {
        let e = call(&late, submit_req(&a, &p)).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Retired);
    }
    // The tombstone survives an agent restart.
    agent.shutdown();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    let e = call(
        &c,
        Request::GetAttempt(GetAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired, "a tombstone is not NOT_FOUND");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(lines(&marker), 0);
    assert_eq!(f.host.count(&format!("prepare {a}")), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_lost_ack_is_answered_by_the_resend_and_retire_stops_the_run() {
    let f = fx();
    let agent = f.start("boot-a");
    let (script, marker, pidfile) = stay(&f);
    let p = payload(&script);
    let a = AttemptId::generate();

    // The first submit is accepted, but its connection dies before the answer
    // is read: the coordinator does not know.
    {
        let c = f.connect(&agent).await;
        let _ = tokio::time::timeout(Duration::from_millis(1), c.call(submit_req(&a, &p), T)).await;
    }
    wait_for("the acceptance", || f.store().get(&a).unwrap().is_some());
    // It asks the same worker again and gets the existing attempt.
    let c = f.connect(&agent).await;
    let r = submit(&c, &a, &p).await;
    assert!(r.duplicate);
    wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();

    // Retire: tombstone first, then the stop is confirmed.
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.status, RetireStatus::Stopped);
    assert!(!alive(pid), "the command is gone when retire says stopped");
    assert_eq!(f.rec(&a).termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(f.rec(&a).retired);

    // Old submits and a second retire.
    let e = call(&c, submit_req(&a, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    let Response::RetireAttempt(r) = call(&c, retire_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.status, RetireStatus::AlreadyTerminal);
    assert_eq!(lines(&marker), 1);
}

// ---------------------------------------------------------------- A23

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_restarted_agent_adopts_the_living_supervisor() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    let pid = read_pid(&pidfile).unwrap();
    let sup = f.supervisors(&a);
    assert_eq!(sup.len(), 1);
    let epoch = agent.agent_epoch();

    // The agent goes away; the supervisor and the command do not.
    agent.shutdown();
    drop(c);
    assert!(alive(pid));

    let agent = f.start("boot-a");
    assert!(agent.agent_epoch() > epoch);
    let report = agent.startup_report();
    assert!(!report.boot_changed);
    assert_eq!(report.watched, vec![a.clone()]);
    assert!(report.settled.is_empty());
    // Give the watcher time to do something wrong.
    std::thread::sleep(Duration::from_millis(500));
    assert!(alive(pid), "the same command still runs");
    assert_eq!(f.supervisors(&a), sup, "no second supervisor");
    assert_eq!(lines(&marker), 1, "the command was not started again");
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
    assert_eq!(f.host.count(&format!("prepare {a}")), 1);
    assert_eq!(f.rec(&a).state, AttemptState::Active);

    // The adopted attempt obeys the new agent.
    let c = f.connect(&agent).await;
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.phase, CancelPhase::Accepted);
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    assert!(!alive(pid));
    wait_for("the cleanup", || !f.host.ws_dir(&a).exists());
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.phase, CancelPhase::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a23_a_dead_supervisor_is_recovered_and_the_command_is_not_started_again() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });
    let pid = read_pid(&pidfile).unwrap();
    agent.shutdown();
    for s in f.supervisors(&a) {
        unsafe { libc::kill(s, libc::SIGKILL) };
    }
    wait_for("the supervisor to die", || f.supervisors(&a).is_empty());
    assert!(alive(pid), "the command outlives its supervisor");

    // The database still says active. The agent must not run it again: the
    // new supervisor stops what is left and fixes the result.
    assert_eq!(f.rec(&a).state, AttemptState::Active);
    let _agent = f.start("boot-a");
    assert_eq!(f.wait_terminal(&a), Outcome::Interrupted);
    assert!(!alive(pid));
    assert_eq!(lines(&marker), 1);
    assert_eq!(f.store().launches(&a).unwrap().len(), 1);
}

// ---------------------------------------------------------------- A24

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a24_a_changed_boot_id_interrupts_everything_and_a_retry_runs() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, marker, pidfile) = stay(&f);
    let running = AttemptId::generate();
    let p_running = payload(&script);
    submit(&c, &running, &p_running).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();

    // A second attempt is accepted but still waits for its secret bundle.
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let waiting = AttemptId::generate();
    let waiting_marker = f.path("waiting-starts");
    let p_waiting = payload(&format!("echo start >> {}", waiting_marker.display()));
    submit(&c, &waiting, &p_waiting).await;
    wait_for("the preparation", || {
        f.rec(&waiting).state == AttemptState::Preparing
            && f.host.count(&format!("prepare {waiting}")) == 1
    });

    // The machine restarts: nothing survives. (Simulated: the processes are
    // killed and the next agent is told another boot ID.)
    agent.shutdown();
    for s in f.supervisors(&running) {
        unsafe { libc::kill(s, libc::SIGKILL) };
    }
    unsafe { libc::kill(-pid, libc::SIGKILL) };
    wait_for("the processes to die", || {
        f.supervisors(&running).is_empty() && !alive(pid)
    });
    f.host.secrets_ready.store(true, Ordering::SeqCst);

    let agent = f.start("boot-b");
    let report = agent.startup_report().clone();
    assert!(report.boot_changed);
    // The one that never started is fixed before the agent serves anything.
    assert_eq!(report.settled, vec![waiting.clone()]);
    assert_eq!(
        f.rec(&waiting).termination.unwrap().outcome(),
        Outcome::Interrupted
    );
    assert_eq!(f.wait_terminal(&running), Outcome::Interrupted);
    wait_for("the cleanup", || {
        !f.host.ws_dir(&running).exists() && !f.host.ws_dir(&waiting).exists()
    });
    assert_eq!(lines(&marker), 1);
    assert_eq!(
        lines(&waiting_marker),
        0,
        "it is not started after a reboot"
    );
    assert_eq!(f.host.count(&format!("prepare {waiting}")), 1);

    // After `interrupted` the same task can be tried again as a new attempt.
    let c = f.connect(&agent).await;
    let mut again = p_waiting.clone();
    again.generation = Generation(2);
    let retry = AttemptId::generate();
    assert!(!submit(&c, &retry, &again).await.duplicate);
    assert_eq!(f.wait_terminal(&retry), Outcome::Succeeded);
    assert_eq!(lines(&waiting_marker), 1);

    // The next start under the same boot ID is an ordinary restart.
    agent.shutdown();
    let agent = f.start("boot-b");
    assert!(!agent.startup_report().boot_changed);
}

// ------------------------------------------------------- cancel, secrets

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_before_the_start_means_the_command_never_starts() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let marker = f.path("starts");
    let p = payload(&format!("echo start >> {}", marker.display()));
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace", || f.host.ws_dir(&a).exists());
    let Response::CancelAttempt(r) = call(&c, cancel_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!((r.phase, r.tombstone), (CancelPhase::Accepted, false));
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    wait_for("the cleanup", || !f.host.ws_dir(&a).exists());
    assert!(f.supervisors(&a).is_empty());
    assert_eq!(lines(&marker), 0);
    assert!(f.store().launches(&a).unwrap().is_empty());

    // A cancel for an attempt that has not arrived keeps a tombstone.
    let unknown = AttemptId::generate();
    let Response::CancelAttempt(r) = call(&c, cancel_req(&unknown)).await.unwrap() else {
        panic!()
    };
    assert!(r.tombstone);
    assert_eq!(r.state, None);
    let e = call(&c, submit_req(&unknown, &p)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_command_waits_for_its_secret_bundle() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    f.host.secrets_ready.store(false, Ordering::SeqCst);
    let mut p = payload("cat .env > out; test \"$(cat out)\" = token");
    p.has_secret_bundle = true;
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    wait_for("the workspace record", || {
        f.state()
            .join("attempts")
            .join(a.as_str())
            .join("workspace.json")
            .exists()
    });
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(f.rec(&a).state, AttemptState::Preparing);
    let put = Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: a.clone(),
        generation: Generation(1),
        files: vec![SecretFile {
            path: ".env".into(),
            hmac_digest: HmacDigest::parse(&format!("hmac-sha256:{}", "a".repeat(64))).unwrap(),
            content: Bytes(b"token".to_vec()),
        }],
    });
    let Response::SecretBundlePut(r) = call(&c, put.clone()).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.placed, 1);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    // Once it started, nothing more is placed.
    let e = call(&c, put).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
}

// ------------------------------------------------- results, events, GC

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_collected_as_garbage_before_ack_collected() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    // Keep nothing longer than necessary: only the missing ACK protects.
    cfg.retention_ms = 0;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("true")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);

    // A log file of the attempt, referenced the way the supervisor does.
    let logs = f.state().join("attempts").join(a.as_str()).join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(logs.join("stdout.log"), b"output").unwrap();
    agent
        .store()
        .add_blob_ref(
            &NewBlobRef {
                ref_id: format!("{a}/logs"),
                attempt_id: a.to_string(),
                kind: BlobKind::Log,
                locator: format!("{a}/logs"),
            },
            1,
        )
        .unwrap();

    std::thread::sleep(Duration::from_millis(5));
    for _ in 0..3 {
        let report = agent.gc().unwrap();
        assert!(report.removed.is_empty(), "{report:?}");
        agent.tick();
        assert!(logs.join("stdout.log").exists());
    }

    let get = |generation| {
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(generation),
        })
    };
    let e = call(&c, get(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    let Response::GetResult(result) = call(&c, get(1)).await.unwrap() else {
        panic!()
    };
    assert_eq!(result.termination.outcome(), Outcome::Succeeded);
    let ack = |digest: Digest| {
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
            result_digest: digest,
        })
    };
    // An ACK for something else than this result releases nothing.
    let e = call(&c, ack(Digest::of_bytes(b"other"))).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert!(agent.gc().unwrap().removed.is_empty());
    assert!(logs.join("stdout.log").exists());

    for _ in 0..2 {
        let Response::AckCollected(r) = call(&c, ack(result.result_digest.clone())).await.unwrap()
        else {
            panic!()
        };
        assert!(r.released);
    }
    assert!(f.rec(&a).acked_at_ms.is_some());
    let report = agent.gc().unwrap();
    assert_eq!(report.removed.len(), 1);
    assert!(!logs.exists());
    // The attempt's record and tombstone-level facts stay: a resend is still
    // the same attempt, never a new run.
    assert!(
        submit(&c, &a, &serde_json::from_value(f.rec(&a).spec).unwrap())
            .await
            .duplicate
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_events_logs_lease_and_inspect_answer_from_the_store() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let (script, _marker, pidfile) = stay(&f);
    let a = AttemptId::generate();
    submit(&c, &a, &payload(&script)).await;
    wait_for("the command", || read_pid(&pidfile).is_some());
    wait_for("the running state", || {
        f.rec(&a).state == AttemptState::Active
    });

    let renew = |seq| {
        Request::RenewLease(RenewLeaseRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            attempt_id: a.clone(),
            generation: Generation(1),
            lease_seq: seq,
            lease_ms: 60_000,
        })
    };
    let Response::RenewLease(r) = call(&c, renew(1)).await.unwrap() else {
        panic!()
    };
    assert!(r.extended);
    assert_eq!(r.lease_remaining_ms, 60_000);
    let Response::RenewLease(r) = call(&c, renew(1)).await.unwrap() else {
        panic!()
    };
    assert!(!r.extended, "a replay does not extend");

    let Response::Inspect(i) = call(
        &c,
        Request::Inspect(InspectRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!((i.active_task, i.prefetch_pending), (1, 0));
    assert_eq!(i.agent_epoch, agent.agent_epoch());
    assert!(i.disk_free_bytes > 0);

    let list = |include_terminal, states| {
        Request::ListAttempts(ListAttemptsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            states,
            include_terminal,
        })
    };
    let Response::ListAttempts(l) = call(&c, list(false, None)).await.unwrap() else {
        panic!()
    };
    assert_eq!(l.attempts.len(), 1);
    assert_eq!(l.attempts[0].state, AttemptState::Active);
    assert_eq!(l.attempts[0].worker_boot_id.as_str(), "boot-a");
    let Response::ListAttempts(l) = call(&c, list(false, Some(vec![AttemptState::Ready])))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(l.attempts.is_empty());

    call(&c, cancel_req(&a)).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Cancelled);
    let e = call(&c, renew(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::LeaseNotRenewable);
    let Response::ListAttempts(l) = call(&c, list(false, None)).await.unwrap() else {
        panic!()
    };
    assert!(l.attempts.is_empty());
    let Response::ListAttempts(l) = call(&c, list(true, None)).await.unwrap() else {
        panic!()
    };
    assert_eq!(l.attempts.len(), 1);
    assert!(l.attempts[0].termination.is_some());

    // Events: a per-attempt sequence without holes, resumable.
    let watch = |attempt: Option<AttemptId>, after_seq, limit| {
        Request::WatchEvents(WatchEventsRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: attempt,
            after_seq,
            limit,
        })
    };
    let Response::WatchEvents(w) = call(&c, watch(Some(a.clone()), 0, 500)).await.unwrap() else {
        panic!()
    };
    let seqs: Vec<u64> = w.events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert_eq!(w.next_seq, seqs.len() as u64);
    assert_eq!(w.events[0].kind, EventKind::ReceiptReached);
    assert_eq!(w.events.last().unwrap().state, Some(AttemptState::Terminal));
    let mut cursor = remote_task_runner::protocol::chunk::EventCursor::new(a.clone(), 0);
    assert_eq!(
        cursor.fresh(&w.events, w.next_seq).unwrap().len(),
        seqs.len()
    );
    let Response::WatchEvents(page) = call(&c, watch(Some(a.clone()), 2, 2)).await.unwrap() else {
        panic!()
    };
    assert_eq!(page.events, w.events[2..4].to_vec());
    assert_eq!(page.next_seq, 4);
    let Response::WatchEvents(end) = call(&c, watch(Some(a.clone()), w.next_seq, 10))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(end.events.is_empty());
    assert_eq!(end.next_seq, w.next_seq);
    let e = call(&c, watch(None, 0, 10)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let e = call(&c, watch(Some(AttemptId::generate()), 0, 10))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);

    // No spool was written: an ended attempt has an empty, complete log.
    let Response::ReadLog(log) = call(
        &c,
        Request::ReadLog(ReadLogRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            node_id: NodeId::parse("main").unwrap(),
            stream: LogStream::Stdout,
            offset: 0,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(log.eof && log.data.0.is_empty());

    // Operations of other rows say so instead of pretending.
    let e = call(
        &c,
        Request::BlobVerify(BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            digests: vec![],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotImplemented);
}

// ---------------------------------------------------------------- startup

#[test]
fn startup_runs_every_sweep_in_order_before_any_attempt_is_prepared() {
    let f = fx();
    // An attempt accepted by an agent that died before preparing it.
    let state = f.state();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    let a = AttemptId::generate();
    {
        let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
        let p = payload("true");
        let req = SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            p.clone(),
        )
        .unwrap();
        store
            .submit(
                remote_task_runner::storage::NewAcceptance {
                    attempt_id: a.clone(),
                    run_id: p.run_id.clone(),
                    task_id: p.task_id.clone(),
                    generation: p.generation,
                    payload_hash: req.payload_hash,
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::to_value(&p).unwrap(),
                },
                1,
            )
            .unwrap();
    }
    let agent = f.start("boot-a");
    assert_eq!(agent.startup_report().steps, STARTUP_ORDER.to_vec());
    assert_eq!(agent.startup_report().watched, vec![a.clone()]);
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let calls = f.host.calls();
    assert_eq!(
        calls[..6],
        [
            "core_dumps".to_string(),
            "store_copies".into(),
            "vault".into(),
            "secrets".into(),
            "workspaces".into(),
            format!("prepare {a}"),
        ]
    );
    // The state directory is private whatever it was before.
    let mode = std::fs::metadata(&state).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert_eq!(
        std::fs::read_to_string(state.join("boot_id")).unwrap(),
        "boot-a"
    );

    // One agent per state directory.
    let mut cfg = f.cfg("boot-a");
    cfg.lock_wait_ms = 300;
    let second = Agent::start(cfg, f.host.clone());
    assert_eq!(second.err().unwrap().code, ErrorCode::Conflict);
    // The refused start changed nothing: the sweeps did not run again.
    assert_eq!(f.host.count("store_copies"), 1);
}

#[test]
fn a_failing_startup_step_refuses_the_start_and_runs_nothing_after_it() {
    let f = fx();
    f.host.fail_core_dumps.store(true, Ordering::SeqCst);
    let e = Agent::start(f.cfg("boot-a"), f.host.clone()).err().unwrap();
    assert_eq!(e.code, ErrorCode::Internal);
    assert_eq!(f.host.calls(), vec!["core_dumps".to_string()]);
    assert!(!f.state().join("worker.db").exists());
    assert!(!f.state().join("boot_id").exists());
    assert_eq!(STARTUP_ORDER[1], Step::CoreDumps);
}

// ------------------------------------------------------------ real host

/// The real host, except for the process-wide core dump limit (this test
/// shares its process with the others).
struct RealButCore(remote_task_runner::worker::machine::Machine);

impl Host for RealButCore {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        Ok(())
    }
    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        self.0.sweep_store_copies()
    }
    fn sweep_vault(&self) -> Result<(), ApiError> {
        self.0.sweep_vault()
    }
    fn reconcile_secrets(&self, e: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.reconcile_secrets(e)
    }
    fn sweep_workspaces(&self, h: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.sweep_workspaces(h)
    }
    fn prepare(&self, a: &AcceptedAttempt, p: &AttemptPayload) -> Result<Workspace, ApiError> {
        self.0.prepare(a, p)
    }
    fn place_secrets(
        &self,
        a: &AcceptedAttempt,
        ws: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        self.0.place_secrets(a, ws, files)
    }
    fn secrets_ready(&self, a: &AcceptedAttempt) -> Result<bool, ApiError> {
        self.0.secrets_ready(a)
    }
    fn cleanup(&self, a: &AttemptId, ws: Option<&Workspace>) -> Result<(), ApiError> {
        self.0.cleanup(a, ws)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_real_host_runs_the_command_in_the_snapshot_of_the_run_and_removes_it() {
    use remote_task_runner::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
    let f = fx();
    let state = f.state();
    let env: BTreeMap<String, String> = [
        ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
        ("HOME", f.path("home").to_str().unwrap()),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    std::fs::create_dir_all(f.path("home")).unwrap();
    let plain = f.path("plain");
    std::fs::create_dir_all(plain.join("sub")).unwrap();
    std::fs::write(plain.join("sub/x.txt"), "from the snapshot\n").unwrap();

    // The source of the run is in the worker's store, bound to the run.
    let run = RunId::generate();
    let store = SourceStore::open(&state).unwrap();
    capture_for_run(
        &env,
        &store,
        &run,
        &SourceRequest::Directory { dir: plain.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    // An orphan of an attempt nobody knows: the startup sweep takes it.
    let orphan = state
        .join("workspaces")
        .join(AttemptId::generate().as_str());
    std::fs::create_dir_all(&orphan).unwrap();

    let machine =
        remote_task_runner::worker::machine::Machine::open(&state, env, state.join("cluster.key"))
            .unwrap();
    let agent = Agent::start(f.cfg("boot-a"), Arc::new(RealButCore(machine))).unwrap();
    f.agents.lock().unwrap().push(agent.clone());
    assert!(!orphan.exists());

    let c = f.connect(&agent).await;
    let out = f.path("out");
    let mut p = payload(&format!("pwd > {0}; cat x.txt >> {0}", out.display()));
    p.run_id = run;
    p.commands[0].cwd = "sub".into();
    let a = AttemptId::generate();
    submit(&c, &a, &p).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let text = std::fs::read_to_string(&out).unwrap();
    let mut it = text.lines();
    let cwd = PathBuf::from(it.next().unwrap());
    assert!(cwd.ends_with(format!("workspaces/{a}/sub")), "{cwd:?}");
    assert_eq!(it.next(), Some("from the snapshot"));
    // The original is untouched and the workspace goes at the end.
    wait_for("the workspace to go", || !cwd.exists());
    assert!(plain.join("sub/x.txt").exists());

    // A run whose source never arrived fails as such; nothing is started.
    let b = AttemptId::generate();
    let mut q = payload("true");
    q.task_id = TaskId::generate();
    submit(&c, &b, &q).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Failed);
    assert!(f.store().launches(&b).unwrap().is_empty());
}
