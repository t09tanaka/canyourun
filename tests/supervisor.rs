//! The supervisor against real processes: lease, cancel, timeouts, stop,
//! recovery, and survival of the agent's death.

use remote_task_runner::ids::*;
use remote_task_runner::platform;
use remote_task_runner::state::{AttemptState, Exit, Outcome, Receipt, TimeoutKind, TimeoutPhase};
use remote_task_runner::storage::{LeaseOutcome, NewAcceptance, StorageError, WorkerStore};
use remote_task_runner::supervisor::process::{code, spawn_detached, supervise};
use remote_task_runner::supervisor::recover::recover;
use remote_task_runner::supervisor::run::{run, Report};
use remote_task_runner::supervisor::spec::{NodeSpec, SuperviseSpec};
use remote_task_runner::wire::LeaseKey;
use std::collections::BTreeMap;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Fx {
    dir: TempDir,
    store: WorkerStore,
    spec: SuperviseSpec,
}

fn node(name: &str, script: &str) -> NodeSpec {
    let mut env = BTreeMap::new();
    env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    NodeSpec {
        node_id: NodeId::parse(name).unwrap(),
        argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
        cwd: std::env::temp_dir(),
        env,
        stdout_log: None,
        weight: Some(1),
    }
}

fn fx(nodes: Vec<NodeSpec>) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let traffic = dir.path().join("traffic");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&traffic).unwrap();
    let worker = WorkerId::parse("w-test").unwrap();
    let store = WorkerStore::open(&state, &worker).unwrap();
    let attempt = AttemptId::generate();
    store
        .submit(
            NewAcceptance {
                attempt_id: attempt.clone(),
                run_id: RunId::generate(),
                task_id: TaskId::generate(),
                generation: Generation(1),
                payload_hash: PayloadHash::parse(&format!("sha256:{}", "a".repeat(64))).unwrap(),
                coordinator_epoch: CoordinatorEpoch(1),
                spec: serde_json::json!({}),
            },
            1,
        )
        .unwrap();
    store
        .transition(&attempt, AttemptState::Preparing, 2)
        .unwrap();
    store.transition(&attempt, AttemptState::Ready, 3).unwrap();
    let spec = SuperviseSpec {
        state_dir: state,
        worker_id: worker,
        attempt_id: attempt,
        traffic_state_dir: traffic,
        traffic_config: None,
        nodes,
        lease_ms: 60_000,
        term_grace_ms: 10_000,
        kill_wait_ms: 5_000,
        poll_ms: 25,
        command_timeout_ms: None,
        resource_wait_timeout_ms: None,
        deadline_ms: None,
        inhibit_sleep: false,
    };
    Fx { dir, store, spec }
}

impl Fx {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
    fn attempt(&self) -> &AttemptId {
        &self.spec.attempt_id
    }
    fn open_second(&self) -> WorkerStore {
        WorkerStore::open(&self.spec.state_dir, &self.spec.worker_id).unwrap()
    }
    fn state(&self) -> AttemptState {
        self.store.get(self.attempt()).unwrap().unwrap().state
    }
    fn term(&self) -> remote_task_runner::state::Termination {
        self.store
            .get(self.attempt())
            .unwrap()
            .unwrap()
            .termination
            .expect("terminal")
    }
}

fn key(seq: u64) -> LeaseKey {
    LeaseKey {
        coordinator_epoch: CoordinatorEpoch(1),
        lease_seq: seq,
    }
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn read_pid(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn wait_for<F: Fn() -> bool>(what: &str, f: F) {
    let until = Instant::now() + Duration::from_secs(20);
    while !f() {
        assert!(Instant::now() < until, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn settled(r: &Report) -> remote_task_runner::state::Termination {
    assert_eq!(r.unresolved, None);
    r.termination.expect("settled")
}

// ------------------------------------------------------------- the run

#[test]
fn a_successful_node_is_recorded_with_its_process_identity() {
    let f = fx(vec![node("build", "sleep 0.6; exit 0")]);
    let r = run(&f.spec).unwrap();
    let t = settled(&r);
    assert!(r.finalized_here);
    assert_eq!(t.outcome(), Outcome::Succeeded);
    assert_eq!(t.exit(), Some(Exit::Code(0)));
    assert_eq!(f.state(), AttemptState::Terminal);
    let launches = f.store.launches(f.attempt()).unwrap();
    assert_eq!(launches.len(), 1);
    let ident = launches[0].2.clone().expect("identity recorded");
    assert_eq!(ident.boot_id.as_str(), platform::boot_id().unwrap());
    assert!(ident.pid > 1);
    assert!(ident.start_identity.len() > 3);
    let a = f.store.get(f.attempt()).unwrap().unwrap();
    assert!(a.receipts.reached(Receipt::CommandRegistered));
    assert!(a.receipts.reached(Receipt::Running));
    assert!(a.receipts.reached(Receipt::Terminal));
}

#[test]
fn a_failing_node_stops_the_attempt_and_later_nodes_never_start() {
    let f = fx(vec![
        node("one", "exit 3"),
        node("two", &format!("touch {}", "/nonexistent-dir/never")),
    ]);
    let marker = f.path("two-ran");
    let mut spec = f.spec.clone();
    spec.nodes[1] = node("two", &format!("touch {}", marker.display()));
    let r = run(&spec).unwrap();
    let t = settled(&r);
    assert_eq!(t.outcome(), Outcome::Failed);
    assert_eq!(t.exit(), Some(Exit::Code(3)));
    assert!(!marker.exists());
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
}

#[test]
fn nodes_run_in_order_with_an_isolated_environment_and_a_log_file() {
    let f = fx(vec![]);
    let log = f.path("out.log");
    let mut spec = f.spec.clone();
    let mut n1 = node("one", "echo first:$RTR_VAR:${HOME:-nohome}; echo err >&2");
    n1.env.insert("RTR_VAR".into(), "x".into());
    n1.stdout_log = Some(log.clone());
    let mut n2 = node("two", "echo second");
    n2.stdout_log = Some(log.clone());
    spec.nodes = vec![n1, n2];
    let t = settled(&run(&spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let text = std::fs::read_to_string(&log).unwrap();
    // The supervisor's own HOME is not inherited.
    assert!(text.contains("first:x:nohome"), "{text}");
    assert!(text.contains("err"), "{text}");
    assert!(text.contains("second"), "{text}");
    assert!(text.find("first").unwrap() < text.find("second").unwrap());
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 2);
}

#[test]
fn a_finished_attempt_is_not_run_again() {
    let f = fx(vec![node("n", "exit 0")]);
    let first = run(&f.spec).unwrap();
    assert!(first.finalized_here);
    let again = run(&f.spec).unwrap();
    assert!(!again.finalized_here);
    assert_eq!(again.termination, first.termination);
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
}

// ------------------------------------------------------------- A25 cancel

const IGNORES_TERM: &str = "trap '' TERM; sleep 30 & echo $! > \"$BGPID\"; echo $$ > \"$LEADER\"; while :; do sleep 1; done";

fn with_pidfiles(f: &Fx, script: &str) -> (NodeSpec, PathBuf, PathBuf) {
    let bg = f.path("bg.pid");
    let leader = f.path("leader.pid");
    let mut n = node("n", script);
    n.env.insert("BGPID".into(), bg.display().to_string());
    n.env.insert("LEADER".into(), leader.display().to_string());
    (n, bg, leader)
}

#[test]
fn cancel_terms_then_kills_the_group_and_records_only_after_it_is_gone() {
    let mut f = fx(vec![]);
    let (n, bg, leader) = with_pidfiles(&f, IGNORES_TERM);
    f.spec.nodes = vec![n];
    f.spec.term_grace_ms = 600;
    let second = f.open_second();
    let attempt = f.attempt().clone();
    let (bgp, ldp) = (bg.clone(), leader.clone());
    let canceller = std::thread::spawn(move || {
        wait_for("the command to start", || {
            read_pid(&bgp).is_some() && read_pid(&ldp).is_some()
        });
        second.cancel(&attempt, CoordinatorEpoch(1), 100).unwrap();
    });
    let started = Instant::now();
    let r = run(&f.spec).unwrap();
    canceller.join().unwrap();
    let t = settled(&r);
    assert_eq!(t.outcome(), Outcome::Cancelled);
    // TERM was ignored, so the group was killed after the grace.
    assert_eq!(t.exit(), Some(Exit::Signal(9)));
    assert!(started.elapsed() >= Duration::from_millis(600));
    // The leader and the background child are both gone when it is recorded.
    assert!(!alive(read_pid(&leader).unwrap()));
    assert!(!alive(read_pid(&bg).unwrap()));
    assert_eq!(f.state(), AttemptState::Terminal);
}

#[test]
fn a_cooperative_command_stops_on_term_within_the_grace() {
    let mut f = fx(vec![]);
    let (n, bg, leader) = with_pidfiles(
        &f,
        "sleep 30 & echo $! > \"$BGPID\"; echo $$ > \"$LEADER\"; wait",
    );
    f.spec.nodes = vec![n];
    let second = f.open_second();
    let attempt = f.attempt().clone();
    let bgp = bg.clone();
    let canceller = std::thread::spawn(move || {
        wait_for("the command", || read_pid(&bgp).is_some());
        second.cancel(&attempt, CoordinatorEpoch(1), 100).unwrap();
    });
    let started = Instant::now();
    let t = settled(&run(&f.spec).unwrap());
    canceller.join().unwrap();
    assert_eq!(t.outcome(), Outcome::Cancelled);
    assert_eq!(t.exit(), Some(Exit::Signal(15)));
    assert!(started.elapsed() < Duration::from_secs(8));
    assert!(!alive(read_pid(&bg).unwrap()));
    assert!(!alive(read_pid(&leader).unwrap()));
}

#[test]
fn a_cancel_before_the_start_means_no_command_is_ever_started() {
    let f = fx(vec![node("n", "echo started > /nonexistent-dir/x")]);
    let marker = f.path("started");
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &format!("touch {}", marker.display()))];
    f.store
        .cancel(f.attempt(), CoordinatorEpoch(1), 50)
        .unwrap();
    let t = settled(&run(&spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Cancelled);
    assert_eq!(t.exit(), None);
    assert!(!marker.exists());
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
}

#[test]
fn natural_end_and_cancel_racing_still_give_one_terminal() {
    let f = fx(vec![node("n", "exit 0")]);
    let r = run(&f.spec).unwrap();
    let first = settled(&r);
    // A cancel after the end is accepted but changes nothing.
    f.store
        .cancel(f.attempt(), CoordinatorEpoch(1), 9_999)
        .unwrap();
    assert_eq!(f.term(), first);
    assert_eq!(first.outcome(), Outcome::Succeeded);
}

// ----------------------------------------------------- A22 lease, A26 clock

#[test]
fn lease_expiry_stops_the_command_and_a_late_renewal_cannot_revive_it() {
    let mut f = fx(vec![]);
    let (n, bg, _) = with_pidfiles(&f, IGNORES_TERM);
    f.spec.nodes = vec![n];
    f.spec.lease_ms = 700;
    f.spec.term_grace_ms = 300;
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(t.exit().is_some(), "the command did run and was stopped");
    assert!(!alive(read_pid(&bg).unwrap()));
    let (_, expired) = f.store.lease(f.attempt()).unwrap().unwrap();
    assert!(expired);
    let err = f
        .store
        .renew_lease(f.attempt(), key(1), platform::wall_ms())
        .unwrap_err();
    assert!(matches!(err, StorageError::LeaseNotRenewable(_)), "{err:?}");
}

#[test]
fn renewals_keep_the_attempt_alive_past_one_lease_period() {
    let mut f = fx(vec![node("n", "sleep 1.6")]);
    f.spec.lease_ms = 800;
    let second = f.open_second();
    let attempt = f.attempt().clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = stop.clone();
    let renewer = std::thread::spawn(move || {
        let mut seq = 1;
        while !flag.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = second.renew_lease(&attempt, key(seq), platform::wall_ms());
            seq += 1;
            std::thread::sleep(Duration::from_millis(150));
        }
    });
    let t = settled(&run(&f.spec).unwrap());
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    renewer.join().unwrap();
    assert_eq!(t.outcome(), Outcome::Succeeded);
}

#[test]
fn a_replayed_renewal_does_not_extend_the_lease() {
    let mut f = fx(vec![node("n", "sleep 30")]);
    f.spec.lease_ms = 900;
    f.spec.term_grace_ms = 300;
    let second = f.open_second();
    let attempt = f.attempt().clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = stop.clone();
    let replayer = std::thread::spawn(move || {
        let mut extended = 0;
        while !flag.load(std::sync::atomic::Ordering::SeqCst) {
            // The same request again and again.
            if let Ok(LeaseOutcome::Extended) =
                second.renew_lease(&attempt, key(1), platform::wall_ms())
            {
                extended += 1;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        extended
    });
    let started = Instant::now();
    let t = settled(&run(&f.spec).unwrap());
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let extended = replayer.join().unwrap();
    assert_eq!(extended, 1, "only the first delivery extends");
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn no_new_node_starts_once_the_lease_has_expired() {
    let f = fx(vec![node("n", "exit 0")]);
    let marker = f.path("started");
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &format!("touch {}", marker.display()))];
    f.store
        .expire_lease(f.attempt(), platform::wall_ms())
        .unwrap();
    let t = settled(&run(&spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert_eq!(t.exit(), None);
    assert!(!marker.exists());
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
}

#[test]
fn a_node_after_the_lease_ran_out_is_not_started() {
    let mut f = fx(vec![]);
    let marker = f.path("second-started");
    f.spec.nodes = vec![
        node("first", "sleep 0.1"),
        node("second", &format!("touch {}", marker.display())),
    ];
    // Short enough to expire between the two nodes' admission loops.
    f.spec.lease_ms = 60;
    let t = settled(&run(&f.spec).unwrap());
    assert!(!marker.exists(), "the second node must not start");
    assert_eq!(t.outcome(), Outcome::Interrupted);
}

// ------------------------------------------------------------ timeouts

#[test]
fn the_command_timeout_stops_a_running_command() {
    let mut f = fx(vec![node("n", "sleep 30")]);
    f.spec.command_timeout_ms = Some(500);
    f.spec.term_grace_ms = 2_000;
    let started = Instant::now();
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::TimedOut);
    let info = t.timeout().unwrap();
    assert_eq!(info.kind, TimeoutKind::Command);
    assert_eq!(info.phase, TimeoutPhase::Running);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn the_deadline_counts_the_whole_attempt() {
    let mut f = fx(vec![node("n", "sleep 30")]);
    f.spec.deadline_ms = Some(500);
    f.spec.command_timeout_ms = Some(60_000);
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::TimedOut);
    let info = t.timeout().unwrap();
    assert_eq!(info.kind, TimeoutKind::Deadline);
    assert_eq!(info.phase, TimeoutPhase::Running);
}

// ------------------------------------------------- A20 recovery and identity

fn spawn_sleeper() -> (std::process::Child, ProcessIdentityParts) {
    let child = Command::new("sleep")
        .arg("60")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let parts = ProcessIdentityParts {
        pid: child.id(),
        start: platform::process_start_identity(child.id()).unwrap(),
    };
    (child, parts)
}

struct ProcessIdentityParts {
    pid: u32,
    start: String,
}

fn claim(f: &Fx, node: &str, ident: Option<(&str, &ProcessIdentityParts)>) {
    let n = NodeId::parse(node).unwrap();
    f.store.claim_launch(f.attempt(), &n, 10).unwrap();
    if let Some((boot, p)) = ident {
        f.store
            .record_process_identity(
                f.attempt(),
                &n,
                &remote_task_runner::storage::ProcessIdentity {
                    boot_id: WorkerBootId::parse(boot).unwrap(),
                    pid: p.pid,
                    start_identity: p.start.clone(),
                },
                11,
            )
            .unwrap();
    }
}

fn recover_spec(f: &Fx) -> SuperviseSpec {
    let mut s = f.spec.clone();
    s.term_grace_ms = 500;
    s
}

#[test]
fn after_a_supervisor_crash_the_same_process_is_stopped_and_never_respawned() {
    let f = fx(vec![node("n", "touch /nonexistent-dir/never")]);
    let (mut child, p) = spawn_sleeper();
    claim(&f, "n", Some((&platform::boot_id().unwrap(), &p)));
    // An orphan is reaped by init; here the test is the parent and reaps it.
    let reaper = std::thread::spawn(move || child.wait().unwrap());
    let r = recover(&recover_spec(&f), &f.store).unwrap();
    let t = settled(&r);
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert_eq!(t.exit(), None);
    // The orphan was stopped: it ended on a signal.
    let status = reaper.join().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert!(status.signal().is_some(), "{status:?}");
    assert_eq!(f.state(), AttemptState::Terminal);
    // `run` on the same attempt goes the recovery way and starts nothing.
    let again = run(&f.spec).unwrap();
    assert_eq!(again.termination, Some(t));
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
}

#[test]
fn after_a_reboot_the_recorded_pid_is_not_touched() {
    let f = fx(vec![node("n", "true")]);
    let (mut child, p) = spawn_sleeper();
    // Recorded under another boot: the PID now belongs to someone else.
    claim(&f, "n", Some(("some-earlier-boot", &p)));
    let t = settled(&recover(&recover_spec(&f), &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(child.try_wait().unwrap().is_none(), "must still be running");
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn a_reused_pid_with_another_start_identity_is_not_touched() {
    let f = fx(vec![node("n", "true")]);
    let (mut child, mut p) = spawn_sleeper();
    p.start = "not-the-same-start".into();
    claim(&f, "n", Some((&platform::boot_id().unwrap(), &p)));
    let t = settled(&recover(&recover_spec(&f), &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(child.try_wait().unwrap().is_none(), "must still be running");
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn a_dead_leader_with_a_surviving_descendant_is_stopped_by_recovery() {
    let f = fx(vec![node("n", "true")]);
    let bg = f.path("bg.pid");
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 60 & echo $! > \"$1\"; wait")
        .arg("sh")
        .arg(&bg)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for("the descendant", || read_pid(&bg).is_some());
    let p = ProcessIdentityParts {
        pid: child.id(),
        start: platform::process_start_identity(child.id()).unwrap(),
    };
    claim(&f, "n", Some((&platform::boot_id().unwrap(), &p)));
    // The leader dies (and is reaped); its background child stays.
    child.kill().unwrap();
    child.wait().unwrap();
    let bgp = read_pid(&bg).unwrap();
    assert!(alive(bgp));
    let t = settled(&recover(&recover_spec(&f), &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    wait_for("the descendant to be gone", || !alive(bgp));
}

#[test]
fn a_launch_without_an_identity_is_investigated_not_respawned() {
    let f = fx(vec![node("n", "touch /nonexistent-dir/never")]);
    claim(&f, "n", None);
    let t = settled(&recover(&f.spec, &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
}

#[test]
fn recovering_a_cancelled_attempt_that_never_launched_settles_it_as_cancelled() {
    let f = fx(vec![node("n", "true")]);
    f.store.cancel(f.attempt(), CoordinatorEpoch(1), 5).unwrap();
    let t = settled(&recover(&f.spec, &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Cancelled);
}

// ------------------------------------------- A21 the agent dies, the supervisor does not

fn exe() -> PathBuf {
    assert_cmd::cargo::cargo_bin("remote-task-runner")
}

fn write_spec(f: &Fx, spec: &SuperviseSpec) -> PathBuf {
    let p = f.path("spec.json");
    std::fs::write(&p, serde_json::to_string(spec).unwrap()).unwrap();
    p
}

#[test]
fn the_supervisor_finishes_the_attempt_after_its_agent_is_killed() {
    let f = fx(vec![]);
    let done = f.path("done");
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &format!("sleep 1.5; touch {}", done.display()))];
    let spec_path = write_spec(&f, &spec);
    // The "agent": a process that starts the supervisor in its own group.
    let agent = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "'{}' supervise --spec '{}' & wait",
            exe().display(),
            spec_path.display()
        ))
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let agent_pid = agent.id() as i32;
    wait_for("the command to be running", || {
        f.store.get(f.attempt()).unwrap().unwrap().state == AttemptState::Active
    });
    // The agent and everything left in its group die.
    unsafe { libc::killpg(agent_pid, libc::SIGKILL) };
    let mut agent = agent;
    agent.wait().unwrap();
    wait_for("the supervisor to settle the attempt", || {
        f.store.get(f.attempt()).unwrap().unwrap().state == AttemptState::Terminal
    });
    assert!(done.exists(), "the command ran to its end");
    assert_eq!(f.term().outcome(), Outcome::Succeeded);
}

#[test]
fn a_detached_supervisor_runs_without_its_parent_waiting() {
    let f = fx(vec![node("n", "exit 7")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut child = spawn_detached(&exe(), &spec_path).unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(code::SETTLED));
    assert_eq!(f.term().outcome(), Outcome::Failed);
    assert_eq!(f.term().exit(), Some(Exit::Code(7)));
}

#[test]
fn a_killed_supervisor_leaves_the_command_for_recovery_to_stop() {
    let f = fx(vec![]);
    let (n, bg, leader) = with_pidfiles(&f, IGNORES_TERM);
    let mut spec = f.spec.clone();
    spec.nodes = vec![n];
    spec.term_grace_ms = 300;
    let spec_path = write_spec(&f, &spec);
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    wait_for("the command to be running", || {
        read_pid(&leader).is_some() && read_pid(&bg).is_some() && f.state() == AttemptState::Active
    });
    // The supervisor is killed hard; the command (its own session) survives.
    sup.kill().unwrap();
    sup.wait().unwrap();
    let leader_pid = read_pid(&leader).unwrap();
    assert!(alive(leader_pid), "the command outlives its supervisor");
    // A new attempt of the supervisor never starts the command again: it
    // recovers, stops what is left, and settles as interrupted.
    let report = supervise(&spec).unwrap();
    let t = settled(&report);
    assert_eq!(t.outcome(), Outcome::Interrupted);
    wait_for("the orphan to be gone", || !alive(leader_pid));
    assert!(!alive(read_pid(&bg).unwrap()));
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
}

#[test]
fn a_descendant_left_behind_by_an_early_exit_is_stopped_before_the_result_is_fixed() {
    let mut f = fx(vec![]);
    let (n, bg, _leader) = with_pidfiles(&f, "sleep 1000 & echo $! > \"$BGPID\"; exit 0");
    f.spec.nodes = vec![n];
    f.spec.term_grace_ms = 500;
    let spec = f.spec.clone();
    let runner = std::thread::spawn(move || run(&spec));
    while !runner.is_finished() {
        // Read the state first: if it is terminal, the descendant must
        // already be gone when it is looked at afterwards.
        let terminal = f.state() == AttemptState::Terminal;
        let survivor = read_pid(&bg).is_some_and(alive);
        assert!(!(terminal && survivor), "settled while a descendant lived");
        std::thread::sleep(Duration::from_millis(5));
    }
    let t = settled(&runner.join().unwrap().unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    assert!(!alive(read_pid(&bg).unwrap()), "the descendant was stopped");
    // Either the identity was recorded, or the log says why it was not. (The
    // command is gone almost at once, so which one depends on timing.)
    let recorded = f.store.launches(f.attempt()).unwrap()[0].2.is_some();
    let logged = f
        .store
        .events_after(0, 1000)
        .unwrap()
        .iter()
        .any(|e| e.kind == "supervisor_identity_not_recorded");
    assert!(recorded || logged);
}

#[test]
fn the_command_keeps_the_default_sighup_disposition() {
    let f = fx(vec![node("n", "kill -HUP $$; sleep 1; exit 0")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut child = spawn_detached(&exe(), &spec_path).unwrap();
    child.wait().unwrap();
    // Killed by the hang-up (the default), not ignoring it.
    assert_eq!(f.term().exit(), Some(Exit::Signal(libc::SIGHUP)));
}

#[test]
fn the_supervisor_survives_a_hangup_and_finishes_the_attempt() {
    let f = fx(vec![node("n", "sleep 1.5; exit 0")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    wait_for("the command to be running", || {
        f.state() == AttemptState::Active
    });
    unsafe { libc::kill(sup.id() as i32, libc::SIGHUP) };
    let status = sup.wait().unwrap();
    assert_eq!(status.code(), Some(code::SETTLED), "{status:?}");
    assert_eq!(f.term().outcome(), Outcome::Succeeded);
}

#[test]
fn only_one_supervisor_may_hold_an_attempt() {
    let f = fx(vec![node("n", "sleep 2")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut first = spawn_detached(&exe(), &spec_path).unwrap();
    wait_for("the first supervisor to run", || {
        f.state() == AttemptState::Active
    });
    let second = Command::new(exe())
        .arg("supervise")
        .arg("--spec")
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(second.status.code(), Some(code::ALIVE));
    first.wait().unwrap();
    assert_eq!(f.term().outcome(), Outcome::Succeeded);
}

#[test]
fn a_bad_spec_is_refused_with_its_own_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("spec.json");
    std::fs::write(&p, "{\"nope\": 1}").unwrap();
    let out = Command::new(exe())
        .arg("supervise")
        .arg("--spec")
        .arg(&p)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code::BAD_SPEC));
}
