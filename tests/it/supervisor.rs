//! The supervisor against real processes: lease, cancel, timeouts, stop,
//! recovery, and survival of the agent's death.

use crate::common::{alive, identity_of, kill_recorded, kill_recorded_group, read_pid};
use remote_task_runner::ids::*;
use remote_task_runner::platform;
use remote_task_runner::platform::{signal_pid, signal_process_group};
use remote_task_runner::state::{AttemptState, Exit, Outcome, Receipt, TimeoutKind, TimeoutPhase};
use remote_task_runner::storage::{LeaseOutcome, NewAcceptance, StorageError, WorkerStore};
use remote_task_runner::supervisor::lease::LeaseSeed;
use remote_task_runner::supervisor::outcome::drain_pending_deletes;
use remote_task_runner::supervisor::process::{code, spawn_detached, supervise};
use remote_task_runner::supervisor::recover::recover;
use remote_task_runner::supervisor::run::{run, Report};
use remote_task_runner::supervisor::spec::{
    ArtifactFile, ArtifactsSpec, MaskValues, NodeSpec, SuperviseSpec,
};
use remote_task_runner::supervisor::spool::{AttemptSpool, SpoolReader};
use remote_task_runner::wire::LeaseKey;
use remote_task_runner::wire::LogStream;
use remote_task_runner::wire::MissingPolicy;
use std::collections::BTreeMap;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Fx {
    /// Processes this test started that must not outlive it (they would write
    /// into the directory while it is being removed): (pid, the start
    /// identity read when it was started, whole group).
    strays: std::sync::Mutex<Vec<(u32, String, bool)>>,
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
        weight: Some(1),
        timeout_ms: None,
    }
}

fn fx(nodes: Vec<NodeSpec>) -> Fx {
    // Not under the system temp directory: a supervisor left running by a
    // killed test (setsid keeps it alive) must not write into a directory that
    // a test runner clears between runs.
    let dir = tempfile::Builder::new()
        .prefix("rtr-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
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
        lease_seed: None,
        inhibit_sleep: false,
        spool_root: Some(dir.path().join("spool")),
        log_limit_bytes: 1 << 30,
        workspace_warnings: vec![],
        mask: Default::default(),
        artifacts: None,
        prepared_only: false,
    };
    Fx {
        strays: Default::default(),
        dir,
        store,
        spec,
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        let mut all = self.strays.lock().unwrap().clone();
        let launched = self
            .store
            .launches(&self.spec.attempt_id)
            .unwrap_or_default();
        for (_, _, ident) in launched {
            if let Some(i) = ident {
                all.push((i.pid, i.start_identity, true));
            }
        }
        for (pid, start, group) in all {
            // Never by the number alone: the PID may be another process's by
            // now. Only what still has the recorded start identity (or, for
            // a group, what a leader that is definitely gone left behind).
            if group {
                kill_recorded_group(pid, &start);
            } else {
                kill_recorded(pid, &start);
            }
        }
    }
}

impl Fx {
    fn track(&self, pid: u32) {
        // Without an identity it has ended already: nothing to clean up, and
        // nothing that could be told from a later process with its PID.
        if let Some(start) = identity_of(pid) {
            self.strays.lock().unwrap().push((pid, start, false));
        }
    }
    fn track_group(&self, pid: u32) {
        if let Some(start) = identity_of(pid) {
            self.strays.lock().unwrap().push((pid, start, true));
        }
    }
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
    /// What a reader in another process sees of the attempt's output.
    fn output_of(
        &self,
        attempt: &AttemptId,
    ) -> (Vec<u8>, remote_task_runner::supervisor::spool::ReadChunk) {
        let r = SpoolReader::open(&self.spec.spool_dir(), attempt).unwrap();
        let (mut out, mut off) = (Vec::new(), 0);
        loop {
            let c = r.read(LogStream::Stdout, off, 1 << 20).unwrap();
            out.extend_from_slice(&c.data);
            if c.data.is_empty() {
                return (out, c);
            }
            // A read that returns bytes must move the offset forward, or this
            // would loop forever instead of failing.
            assert!(
                c.next_offset > off,
                "a read returned bytes but did not advance"
            );
            off = c.next_offset;
        }
    }
    fn output(&self) -> String {
        String::from_utf8(self.output_of(self.attempt()).0).unwrap()
    }
    fn events(&self, attempt: &AttemptId, kind: &str) -> Vec<serde_json::Value> {
        let all = self.store.events_of(attempt, kind).unwrap();
        all.into_iter().map(|e| e.payload).collect()
    }
    /// Accept another attempt of the same task (the first one is over) and
    /// return the spec that supervises it.
    fn next_attempt(&self) -> SuperviseSpec {
        let first = self.store.get(self.attempt()).unwrap().unwrap();
        let attempt = AttemptId::generate();
        self.store
            .submit(
                NewAcceptance {
                    attempt_id: attempt.clone(),
                    run_id: first.run_id,
                    task_id: first.task_id,
                    generation: Generation(first.generation.0 + 1),
                    payload_hash: first.payload_hash,
                    coordinator_epoch: first.coordinator_epoch,
                    spec: serde_json::json!({}),
                },
                100,
            )
            .unwrap();
        for (to, at) in [(AttemptState::Preparing, 101), (AttemptState::Ready, 102)] {
            self.store.transition(&attempt, to, at).unwrap();
        }
        let mut spec = self.spec.clone();
        spec.attempt_id = attempt;
        spec
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

// Measure the stop from a command's durable start marker. Registration,
// admission and earlier nodes are not part of the running-command budget.
fn run_from_command_start(f: &Fx, marker: &Path) -> remote_task_runner::state::Termination {
    std::thread::scope(|scope| {
        let runner = scope.spawn(|| run(&f.spec).unwrap());
        wait_for("the timed command to start", || marker.exists());
        let started = Instant::now();
        let report = runner.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        let t = settled(&report);
        assert_eq!(t.timeout().unwrap().phase, TimeoutPhase::Running);
        assert!(f
            .store
            .get(f.attempt())
            .unwrap()
            .unwrap()
            .receipts
            .reached(Receipt::Running));
        for (_, _, identity) in f.store.launches(f.attempt()).unwrap() {
            if let Some(identity) = identity {
                assert!(
                    !signal_pid(identity.pid, 0),
                    "a settled command must be stopped"
                );
            }
        }
        t
    })
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
fn nodes_run_in_order_with_an_isolated_environment_and_their_output_in_the_spool() {
    let f = fx(vec![]);
    let mut spec = f.spec.clone();
    let mut n1 = node("one", "echo first:$RTR_VAR:${HOME:-nohome}; echo err >&2");
    n1.env.insert("RTR_VAR".into(), "x".into());
    let n2 = node("two", "echo second");
    spec.nodes = vec![n1, n2];
    let t = settled(&run(&spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let text = f.output();
    // The supervisor's own HOME is not inherited.
    assert!(text.contains("first:x:nohome"), "{text}");
    assert!(text.contains("err"), "{text}");
    assert!(text.contains("second"), "{text}");
    assert!(text.find("first").unwrap() < text.find("second").unwrap());
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 2);
    // The spool is closed, its position is stored and the result describes it.
    let (bytes, last) = f.output_of(f.attempt());
    assert!(last.eof && !last.log_truncated);
    let pos = f.store.log_position(f.attempt(), "stdout").unwrap();
    assert_eq!(pos.written_bytes, bytes.len() as u64);
    let result = f.store.get_result(f.attempt()).unwrap().expect("a result");
    assert_eq!(result["termination"]["outcome"], "succeeded");
    assert_eq!(result["logs"]["stdout"]["bytes"], bytes.len() as u64);
    // The pipes the output came through are gone.
    let left: Vec<_> =
        std::fs::read_dir(spec.spool_dir().join(f.attempt().to_string()).join("logs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".pipe"))
            .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn output_larger_than_a_pipe_is_stored_whole_and_in_order() {
    // Far more than a pipe holds, not UTF-8, written faster than it is read.
    let f = fx(vec![node(
        "big",
        "i=0; while [ $i -lt 40 ]; do head -c 65536 /dev/zero | tr '\\000' '\\377'; i=$((i+1)); done; printf end",
    )]);
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let (bytes, last) = f.output_of(f.attempt());
    assert_eq!(bytes.len(), 40 * 65536 + 3);
    assert!(bytes[..40 * 65536].iter().all(|b| *b == 0xff));
    assert_eq!(&bytes[40 * 65536..], b"end");
    assert!(last.eof);
}

#[test]
fn the_task_limit_counts_what_its_other_attempts_stored() {
    let mut f = fx(vec![node("n", "printf %060d 0; exit 1")]);
    f.spec.log_limit_bytes = 100;
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Failed);
    assert_eq!(f.output().len(), 60);
    assert!(f.events(f.attempt(), "log_truncated").is_empty());
    // The retry is another attempt of the same task, supervised by another
    // process: it may store only what the first one left of the limit.
    let second = f.next_attempt();
    assert_eq!(
        f.store
            .log_bytes_of_other_attempts(&second.attempt_id)
            .unwrap(),
        60
    );
    assert_eq!(f.store.log_bytes_of_other_attempts(f.attempt()).unwrap(), 0);
    let mut spec = second.clone();
    spec.nodes = vec![node("n", "printf %070d 7")];
    let spec_path = write_spec(&f, &spec);
    let mut child = spawn_detached(&exe(), &spec_path).unwrap();
    f.track(child.id());
    assert_eq!(child.wait().unwrap().code(), Some(code::SETTLED));
    let a2 = &second.attempt_id;
    let (bytes, last) = f.output_of(a2);
    assert_eq!(bytes, "0".repeat(40).into_bytes());
    assert!(last.eof && last.log_truncated);
    assert_eq!(last.dropped_bytes, 30);
    let pos = f.store.log_position(a2, "stdout").unwrap();
    assert_eq!((pos.written_bytes, pos.dropped_bytes), (40, 30));
    // The command itself was not disturbed by the limit.
    let rec = f.store.get(a2).unwrap().unwrap();
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Succeeded);
    let ev = f.events(a2, "log_truncated");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["dropped_bytes"], 30);
    // The first attempt's output is untouched, and it now counts 40 more
    // against any later attempt.
    assert_eq!(f.output().len(), 60);
    assert_eq!(
        f.store.log_bytes_of_other_attempts(f.attempt()).unwrap(),
        40
    );
}

#[test]
fn each_heartbeat_publishes_the_output_and_advances_the_stored_position() {
    let f = fx(vec![]);
    let (go, end) = (f.path("go"), f.path("end"));
    let script = format!(
        "printf one; while [ ! -e '{}' ]; do sleep 0.05; done; printf two; \
         while [ ! -e '{}' ]; do sleep 0.05; done",
        go.display(),
        end.display()
    );
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &script)];
    let spec_path = write_spec(&f, &spec);
    // The supervisor is another process; this one reads like the agent does,
    // through its own database connection and the spool's reader.
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    f.track(sup.id());
    let reader = f.open_second();
    let written = || {
        reader
            .log_position(f.attempt(), "stdout")
            .unwrap()
            .written_bytes
    };
    wait_for("the first output to be stored", || written() == 3);
    wait_for("the command to be shown as running", || {
        f.state() == AttemptState::Active
    });
    assert_eq!(f.output(), "one");
    let seq = reader.log_position(f.attempt(), "stdout").unwrap().last_seq;
    std::fs::write(&go, b"").unwrap();
    wait_for("the position to advance", || written() == 6);
    assert_eq!(f.state(), AttemptState::Active, "still running");
    assert_eq!(f.output(), "onetwo");
    assert!(reader.log_position(f.attempt(), "stdout").unwrap().last_seq > seq);
    assert!(!f.output_of(f.attempt()).1.eof, "the spool is still open");
    std::fs::write(&end, b"").unwrap();
    assert_eq!(sup.wait().unwrap().code(), Some(code::SETTLED));
    assert_eq!(f.term().outcome(), Outcome::Succeeded);
    assert!(f.output_of(f.attempt()).1.eof);
}

#[test]
fn a_spool_that_cannot_be_opened_interrupts_the_attempt_and_nothing_is_started() {
    let f = fx(vec![]);
    let marker = f.path("ran");
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &format!("touch '{}'", marker.display()))];
    // The spool root is a file: no directory can be made below it.
    std::fs::write(spec.spool_dir(), b"in the way").unwrap();
    let spec_path = write_spec(&f, &spec);
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    f.track(sup.id());
    assert_eq!(sup.wait().unwrap().code(), Some(code::SETTLED));
    assert_eq!(f.term().outcome(), Outcome::Interrupted);
    assert_eq!(f.term().exit(), None);
    assert!(
        !marker.exists(),
        "no command without a place for its output"
    );
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
    let ev = f.events(f.attempt(), "spool_failed");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["stage"], "open");
    assert!(!ev[0]["error"].as_str().unwrap().is_empty());
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    assert_eq!(std::fs::read(spec.spool_dir()).unwrap(), b"in the way");
}

#[test]
fn a_cancelled_attempt_whose_spool_cannot_be_opened_stays_cancelled() {
    let f = fx(vec![node("n", "true")]);
    std::fs::write(f.spec.spool_dir(), b"").unwrap();
    f.store.cancel(f.attempt(), CoordinatorEpoch(1), 5).unwrap();
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Cancelled);
    assert_eq!(f.events(f.attempt(), "spool_failed").len(), 1);
    // Settled without its spool: no result, and a later start leaves it so.
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    let again = run(&f.spec).unwrap();
    assert_eq!((again.termination, again.finalized_here), (Some(t), false));
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    assert_eq!(f.events(f.attempt(), "spool_failed").len(), 1);
    assert_eq!(std::fs::read(f.spec.spool_dir()).unwrap(), b"");
}

#[test]
fn an_output_pipe_that_cannot_be_made_interrupts_the_attempt() {
    let f = fx(vec![]);
    let marker = f.path("ran");
    let mut spec = f.spec.clone();
    spec.nodes = vec![node("n", &format!("touch '{}'", marker.display()))];
    // Something that is not a pipe sits where the node's pipe belongs.
    let logs = spec.spool_dir().join(f.attempt().to_string()).join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(logs.join("node-0.pipe"), b"not a pipe").unwrap();
    let t = settled(&run(&spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(!marker.exists());
    let ev = f.events(f.attempt(), "spool_failed");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["stage"], "pipe");
    // What was in the way is not removed or written through.
    assert_eq!(
        std::fs::read(logs.join("node-0.pipe")).unwrap(),
        b"not a pipe"
    );
}

#[test]
fn a_secret_the_command_prints_never_reaches_the_spool_or_the_journal() {
    let secret = "hunter2-value";
    // Printed whole, on stderr, and in two writes with a pause between.
    let mut f = fx(vec![node(
        "n",
        "echo \"token=$TOKEN\"; echo \"err $TOKEN\" >&2; printf hunter2-; sleep 0.3; echo value; echo done",
    )]);
    f.spec.nodes[0].env.insert("TOKEN".into(), secret.into());
    f.spec.mask = MaskValues(vec![secret.into()]);
    f.spec.workspace_warnings = vec![format!("git said: {secret} is not a ref")];
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let text = f.output();
    assert!(text.contains("token=***"), "{text}");
    assert!(text.contains("err ***"), "{text}");
    assert!(text.ends_with("***\ndone\n"), "{text}");
    // Not in any file of the spool, whatever its format.
    fn files(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                files(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    let mut all = Vec::new();
    files(&f.spec.spool_dir(), &mut all);
    assert!(!all.is_empty());
    for p in all {
        let bytes = std::fs::read(&p).unwrap();
        let found = bytes.windows(secret.len()).any(|w| w == secret.as_bytes());
        assert!(!found, "{} holds the secret", p.display());
    }
    // Nor in the journal or the stored result.
    let ev = f.store.events_after(0, 1000).unwrap();
    assert!(ev.iter().any(|e| e.kind == "workspace_warning"));
    for e in ev {
        assert!(!e.payload.to_string().contains(secret), "{:?}", e.kind);
    }
    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    assert!(!result.to_string().contains(secret));
}

#[test]
fn workspace_warnings_become_events_once_and_bounded() {
    let mut f = fx(vec![node("n", "true")]);
    let long = "x".repeat(10_000);
    let mut warnings = vec!["index not reproduced".to_string(), long];
    warnings.extend((0..70).map(|i| format!("warning {i}")));
    f.spec.workspace_warnings = warnings;
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let ev = f.events(f.attempt(), "workspace_warning");
    assert_eq!(ev.len(), 65, "64 warnings and the count of the rest");
    assert_eq!(ev[0]["message"], "index not reproduced");
    assert_eq!(ev[1]["message"].as_str().unwrap().len(), 4096);
    assert_eq!(ev[64]["message"], "8 more warnings omitted");
    assert_eq!(ev[0]["index"], "0");
    assert_eq!(ev[64]["index"], "64");
    // A second start (the attempt is over) writes nothing more.
    run(&f.spec).unwrap();
    assert_eq!(f.events(f.attempt(), "workspace_warning").len(), 65);
    // No warnings, no events.
    let quiet = fx(vec![node("n", "true")]);
    run(&quiet.spec).unwrap();
    assert!(quiet
        .events(quiet.attempt(), "workspace_warning")
        .is_empty());
}

#[test]
fn a_secret_across_the_cut_of_a_long_warning_leaves_no_part_of_itself() {
    let secret = "hunter2-value";
    let mut f = fx(vec![node("n", "true")]);
    f.spec.mask = MaskValues(vec![secret.into()]);
    // The secret starts 5 bytes before the 4096-byte cut.
    let long = format!("{}{secret} and more", "x".repeat(4091));
    f.spec.workspace_warnings = vec![
        long,
        // Two different warnings that read the same once masked: both stay.
        format!("token {secret}"),
        "token ***".to_string(),
    ];
    run(&f.spec).unwrap();
    let ev = f.events(f.attempt(), "workspace_warning");
    assert_eq!(ev.len(), 3, "{ev:?}");
    let cut = ev[0]["message"].as_str().unwrap();
    // Masked whole, then cut at 4096 bytes: the mask and two bytes after it.
    assert_eq!(cut.len(), 4096);
    assert_eq!(cut, format!("{}*** a", "x".repeat(4091)));
    assert_eq!(ev[1]["message"], "token ***");
    assert_eq!(ev[2]["message"], "token ***");
    assert!(!format!("{ev:?}").contains("hunt"));
    run(&f.spec).unwrap();
    assert_eq!(f.events(f.attempt(), "workspace_warning").len(), 3);
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
    let (bgp, ldp) = (bg.clone(), leader.clone());
    let canceller = std::thread::spawn(move || {
        // Both files: the script writes the leader's after the background
        // child's, and a cancel in between would leave it never written.
        wait_for("the command", || {
            read_pid(&bgp).is_some() && read_pid(&ldp).is_some()
        });
        let started = Instant::now();
        second.cancel(&attempt, CoordinatorEpoch(1), 100).unwrap();
        started
    });
    let t = settled(&run(&f.spec).unwrap());
    let started = canceller.join().unwrap();
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
fn a_node_s_own_timeout_replaces_the_default_for_that_node_only() {
    // The first node outlives the default and may: its own limit is longer.
    // The second has a shorter one of its own and is stopped by it.
    let mut f = fx(vec![]);
    let marker = f.path("stuck-started");
    f.spec.nodes = vec![
        node("slow", "sleep 1.2; echo slow-done"),
        node(
            "stuck",
            &format!("touch {}; sleep 30; echo stuck-done", marker.display()),
        ),
    ];
    f.spec.command_timeout_ms = Some(400);
    f.spec.nodes[0].timeout_ms = Some(20_000);
    f.spec.nodes[1].timeout_ms = Some(300);
    f.spec.term_grace_ms = 2_000;
    let t = run_from_command_start(&f, &marker);
    assert!(f.output().contains("slow-done"), "{}", f.output());
    assert_eq!(t.outcome(), Outcome::TimedOut);
    let info = t.timeout().unwrap();
    assert_eq!(info.kind, TimeoutKind::Command);
    assert_eq!(info.phase, TimeoutPhase::Running);
    assert!(!f.output().contains("stuck-done"));
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 2);
}

#[test]
fn a_node_without_its_own_timeout_keeps_the_default() {
    let mut f = fx(vec![]);
    let marker = f.path("stuck-started");
    f.spec.nodes = vec![
        node("quick", "true"),
        node(
            "stuck",
            &format!("touch {}; sleep 30; echo stuck-done", marker.display()),
        ),
    ];
    f.spec.command_timeout_ms = Some(400);
    // The limit of the node before it does not stay in force.
    f.spec.nodes[0].timeout_ms = Some(60_000);
    f.spec.term_grace_ms = 2_000;
    let t = run_from_command_start(&f, &marker);
    assert_eq!(t.outcome(), Outcome::TimedOut);
    assert_eq!(t.timeout().unwrap().kind, TimeoutKind::Command);
    assert!(!f.output().contains("stuck-done"));
}

#[test]
fn a_node_timeout_of_zero_is_refused() {
    let mut f = fx(vec![node("n", "true")]);
    f.spec.nodes[0].timeout_ms = Some(0);
    assert!(run(&f.spec).is_err());
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
}

#[test]
fn a_seeded_lease_ends_when_the_grant_made_before_the_start_ends() {
    // A 30 s lease of which 700 ms are left when the supervisor starts.
    let mut f = fx(vec![node("n", "sleep 30")]);
    f.spec.lease_ms = 30_000;
    f.spec.term_grace_ms = 300;
    f.spec.lease_seed = Some(LeaseSeed {
        ends_mono_ms: platform::monotonic_ms() + 700,
        key: None,
    });
    let started = Instant::now();
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(matches!(
        f.store.lease(f.attempt()).unwrap(),
        Some((_, true))
    ));
    // Said once in the journal, by whoever saw it first.
    assert_eq!(f.events(f.attempt(), "lease_expired").len(), 1);
}

#[test]
fn a_seed_measured_before_a_renewal_does_not_shorten_that_renewal() {
    // The agent measured against no key; a renewal was applied before the
    // supervisor started. The seed (already run out) says nothing about it.
    let mut f = fx(vec![node("n", "sleep 0.8; echo done")]);
    f.spec.lease_ms = 30_000;
    f.store
        .renew_lease(f.attempt(), key(1), platform::wall_ms())
        .unwrap();
    f.spec.lease_seed = Some(LeaseSeed {
        ends_mono_ms: platform::monotonic_ms(),
        key: None,
    });
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    assert!(f.events(f.attempt(), "lease_expired").is_empty());
}

// ------------------------------------------------------------ artifacts

fn artifacts(f: &Fx, files: &[(&str, &str)]) -> ArtifactsSpec {
    let ws = f.path("ws");
    std::fs::create_dir_all(&ws).unwrap();
    ArtifactsSpec {
        workspace: ws,
        source_id: SourceId::generate(),
        plan_digest: Digest::of_bytes(b"plan"),
        profile_fingerprint: Digest::of_bytes(b"profile"),
        max_bytes: 1 << 20,
        files: files
            .iter()
            .map(|(name, source)| ArtifactFile {
                name: (*name).to_string(),
                source: PathBuf::from(source),
                if_missing: MissingPolicy::Required,
            })
            .collect(),
        secret_files: Vec::new(),
    }
}

fn in_ws(f: &Fx, script: &str) -> NodeSpec {
    let mut n = node("n", script);
    n.cwd = f.path("ws");
    n
}

#[test]
fn artifacts_are_stored_before_the_attempt_is_settled_and_the_result_names_them() {
    let mut f = fx(vec![]);
    let plan = artifacts(
        &f,
        &[
            ("report.xml", "out/report.xml"),
            ("missing.txt", "out/never-written.txt"),
            ("link.txt", "out/link"),
        ],
    );
    f.spec.nodes = vec![in_ws(
        &f,
        "mkdir out; printf '<ok/>' > out/report.xml; ln -s /etc/hosts out/link",
    )];
    f.spec.artifacts = Some(plan.clone());
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);

    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    let listed = result["artifacts"].as_array().unwrap();
    assert_eq!(listed.len(), 1, "{result}");
    assert_eq!(listed[0]["name"], "report.xml");
    assert_eq!(listed[0]["size"], 5);
    assert_eq!(listed[0]["attempt_id"], f.attempt().as_str());
    assert_eq!(listed[0]["source_id"], plan.source_id.as_str());
    assert_eq!(
        listed[0]["digest"].as_str().unwrap(),
        Digest::of_bytes(b"<ok/>").to_string()
    );
    let kept = f
        .spec
        .spool_dir()
        .join(f.attempt().as_str())
        .join("artifacts");
    assert_eq!(std::fs::read(kept.join("report.xml")).unwrap(), b"<ok/>");
    // What could not be kept is said, by name, and is in no result: a
    // missing file, and a link that leads out of the workspace.
    assert!(!kept.join("missing.txt").exists() && !kept.join("link.txt").exists());
    let mut refused: Vec<String> = f
        .events(f.attempt(), "artifact_not_stored")
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    refused.sort();
    assert_eq!(refused, ["link.txt", "missing.txt"]);
    // The result says which declared artifacts are not in it and why: a
    // file that is not there is `missing`, anything else `refused`.
    assert_eq!(
        result["artifacts_not_stored"],
        serde_json::json!([
            {"name": "link.txt", "path": "out/link", "if_missing": "required", "reason": "refused"},
            {"name": "missing.txt", "path": "out/never-written.txt", "if_missing": "required", "reason": "missing"},
        ])
    );

    // The list was fixed in the journal before the termination was.
    let all = f.store.events_of_attempt(f.attempt(), 1000).unwrap();
    let at = |kind: &str| all.iter().position(|e| e.kind == kind).unwrap();
    assert!(at("artifacts_stored") < at("attempt_terminal"));
    assert_eq!(f.events(f.attempt(), "artifacts_stored").len(), 1);

    // A second start changes nothing: the result is the same bytes.
    let again = run(&f.spec).unwrap();
    assert_eq!(again.termination, Some(t));
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), Some(result));
    assert_eq!(f.events(f.attempt(), "artifacts_stored").len(), 1);
}

#[test]
fn a_secret_file_and_a_copy_of_a_secret_value_are_not_kept_whatever_they_are_called() {
    use remote_task_runner::secrets::place::FileIdentity;
    const VALUE: &str = "hunter2-hunter2-hunter2";
    let mut f = fx(vec![]);
    // `ſecrets.env` is the secret file itself where the file system folds
    // case, and a hard link the command makes where it does not.
    let mut plan = artifacts(
        &f,
        &[
            ("alias.env", "\u{17f}ecrets.env"),
            ("link.txt", "out/link.txt"),
            ("copy.txt", "out/copy.txt"),
            ("fine.txt", "out/fine.txt"),
        ],
    );
    // The secret file as the agent placed it before the commands started.
    let secret = f.path("ws/secrets.env");
    std::fs::write(&secret, format!("TOKEN={VALUE}\n")).unwrap();
    plan.secret_files = vec![FileIdentity::of(
        &std::fs::symlink_metadata(&secret).unwrap(),
    )];
    f.spec.mask = remote_task_runner::supervisor::spec::MaskValues(vec![VALUE.to_string()]);
    f.spec.nodes = vec![in_ws(
        &f,
        "mkdir out && ln secrets.env out/link.txt && cp secrets.env out/copy.txt && \
         { [ -e \u{17f}ecrets.env ] || ln secrets.env \u{17f}ecrets.env; } && \
         printf fine > out/fine.txt",
    )];
    f.spec.artifacts = Some(plan);
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);

    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    let listed = result["artifacts"].as_array().unwrap();
    assert_eq!(listed.len(), 1, "{result}");
    assert_eq!(listed[0]["name"], "fine.txt");
    let mut not_stored: Vec<(String, String)> = result["artifacts_not_stored"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| {
            (
                n["name"].as_str().unwrap().to_string(),
                n["reason"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    not_stored.sort();
    let refused = |n: &str| (n.to_string(), "refused".to_string());
    assert_eq!(
        not_stored,
        [
            refused("alias.env"),
            refused("copy.txt"),
            refused("link.txt")
        ]
    );
    // Nothing of the secret is in the attempt's store or in what the journal
    // says about the refusals.
    let kept = f
        .spec
        .spool_dir()
        .join(f.attempt().as_str())
        .join("artifacts");
    let names: Vec<_> = std::fs::read_dir(&kept)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["fine.txt"]);
    let said = f.events(f.attempt(), "artifact_not_stored");
    assert_eq!(said.len(), 3);
    // Each for its own reason: the file by what it is (its content would
    // give it away too, which is not what refuses it), the copy by what it
    // holds.
    for e in &said {
        let why = e["why"].as_str().unwrap();
        let want = match e["name"].as_str().unwrap() {
            "copy.txt" => "contains a secret value",
            _ => "is one of the attempt's secret files",
        };
        assert!(why.contains(want), "{e}");
    }
    assert!(!format!("{said:?}{result}").contains("hunter2"));
}

#[test]
fn a_failed_command_keeps_what_it_left_and_a_cancel_before_the_start_keeps_nothing() {
    let mut f = fx(vec![]);
    f.spec.artifacts = Some(artifacts(&f, &[("log.txt", "log.txt")]));
    f.spec.nodes = vec![in_ws(&f, "echo why > log.txt; exit 3")];
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Failed);
    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    assert_eq!(result["artifacts"][0]["name"], "log.txt");

    // Cancelled before anything started: no command wrote anything, nothing
    // is looked for and nothing is reported missing.
    let mut g = fx(vec![]);
    g.spec.artifacts = Some(artifacts(&g, &[("log.txt", "log.txt")]));
    g.spec.nodes = vec![in_ws(&g, "echo x > log.txt")];
    g.store
        .cancel(g.attempt(), CoordinatorEpoch(1), platform::wall_ms())
        .unwrap();
    let t = settled(&run(&g.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Cancelled);
    assert!(g.events(g.attempt(), "artifacts_stored").is_empty());
    assert!(g.events(g.attempt(), "artifact_not_stored").is_empty());
    let result = g.store.get_result(g.attempt()).unwrap().unwrap();
    assert_eq!(result["artifacts"], serde_json::json!([]));
}

#[test]
fn a_missing_required_artifact_and_a_missing_optional_one_differ_in_the_result() {
    let mut f = fx(vec![]);
    let mut plan = artifacts(
        &f,
        &[
            ("kept.txt", "kept.txt"),
            ("needed.txt", "needed.txt"),
            ("nice.txt", "deep/er/nice.txt"),
            ("big.bin", "big.bin"),
        ],
    );
    plan.files[2].if_missing = MissingPolicy::Optional;
    plan.files[3].if_missing = MissingPolicy::Optional;
    plan.max_bytes = 4;
    f.spec.nodes = vec![in_ws(&f, "printf abc > kept.txt; printf 12345 > big.bin")];
    f.spec.artifacts = Some(plan);
    // The policy travels in the spec file the supervisor reads.
    let spec: serde_json::Value = serde_json::to_value(&f.spec).unwrap();
    assert_eq!(spec["artifacts"]["files"][0]["if_missing"], "required");
    assert_eq!(spec["artifacts"]["files"][2]["if_missing"], "optional");

    // Neither changes how the attempt ended: judging it is the parent's.
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::Succeeded);
    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    assert_eq!(result["artifacts"].as_array().unwrap().len(), 1);
    assert_eq!(result["artifacts"][0]["name"], "kept.txt");
    assert_eq!(
        result["artifacts_not_stored"],
        serde_json::json!([
            {"name": "big.bin", "path": "big.bin", "if_missing": "optional", "reason": "refused"},
            {"name": "needed.txt", "path": "needed.txt", "if_missing": "required", "reason": "missing"},
            {"name": "nice.txt", "path": "deep/er/nice.txt", "if_missing": "optional", "reason": "missing"},
        ])
    );
    // The journal entry that fixed the stored ones fixed these too.
    let fixed = f.events(f.attempt(), "artifacts_stored");
    assert_eq!(fixed.len(), 1);
    assert_eq!(fixed[0]["not_stored"], result["artifacts_not_stored"]);

    // A second start stores nothing else.
    run(&f.spec).unwrap();
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), Some(result));

    // An attempt that stored everything has no such list in its result.
    let mut g = fx(vec![]);
    g.spec.nodes = vec![in_ws(&g, "printf abc > kept.txt")];
    g.spec.artifacts = Some(artifacts(&g, &[("kept.txt", "kept.txt")]));
    settled(&run(&g.spec).unwrap());
    let result = g.store.get_result(g.attempt()).unwrap().unwrap();
    assert!(result.get("artifacts_not_stored").is_none(), "{result}");
}

#[test]
fn a_crash_after_the_artifacts_were_fixed_is_settled_with_the_same_list_without_the_workspace() {
    let mut f = fx(vec![]);
    f.spec.artifacts = Some(artifacts(&f, &[("a.txt", "a.txt")]));
    f.spec.nodes = vec![in_ws(&f, "printf kept > a.txt")];
    let t = settled(&run(&f.spec).unwrap());
    let first = f.store.get_result(f.attempt()).unwrap().unwrap();
    assert_eq!(first["artifacts"][0]["name"], "a.txt");

    // The same journal, in an attempt whose supervisor died right after
    // fixing the termination: no result yet, and the workspace is gone.
    let spec = f.next_attempt();
    let b = &spec.attempt_id;
    let from = f.spec.spool_dir().join(f.attempt().as_str());
    let to = spec.spool_dir().join(b.as_str());
    std::fs::create_dir_all(to.join("artifacts")).unwrap();
    std::fs::copy(from.join("artifacts/a.txt"), to.join("artifacts/a.txt")).unwrap();
    // A file copied by a supervisor that died before the list was fixed.
    std::fs::write(to.join("artifacts/orphan.bin"), b"left over").unwrap();
    let mut record = first["artifacts"][0].clone();
    record["attempt_id"] = serde_json::json!(b.as_str());
    let run_id = f.store.get(b).unwrap().unwrap().run_id;
    f.store
        .append_event(
            Some(&run_id),
            Some(b),
            "artifacts_stored",
            &serde_json::json!({ "set": "attempt", "artifacts": [record] }),
            40,
        )
        .unwrap();
    AttemptSpool::open(&spec.spool_dir(), b, 1 << 20)
        .unwrap()
        .finish()
        .unwrap();
    f.store.transition(b, AttemptState::Finalizing, 51).unwrap();
    f.store.finalize(b, t, 52).unwrap();
    std::fs::remove_dir_all(f.path("ws")).unwrap();

    let r = run(&spec).unwrap();
    assert_eq!((r.termination, r.finalized_here), (Some(t), false));
    let result = f.store.get_result(b).unwrap().unwrap();
    assert_eq!(result["artifacts"], serde_json::json!([record]));
    assert!(to.join("artifacts/a.txt").exists());
    assert!(!to.join("artifacts/orphan.bin").exists());

    // An entry that cannot be read is never taken for "no artifacts".
    let spec = f.next_attempt();
    let c = &spec.attempt_id;
    f.store
        .append_event(
            Some(&run_id),
            Some(c),
            "artifacts_stored",
            &serde_json::json!({ "set": "attempt", "artifacts": "garbled" }),
            60,
        )
        .unwrap();
    AttemptSpool::open(&spec.spool_dir(), c, 1 << 20)
        .unwrap()
        .finish()
        .unwrap();
    f.store.transition(c, AttemptState::Finalizing, 61).unwrap();
    f.store.finalize(c, t, 62).unwrap();
    assert!(run(&spec).is_err());
    assert!(f.store.get_result(c).unwrap().is_none());
}

#[test]
fn a_declared_artifact_that_leaves_the_workspace_refuses_the_spec() {
    for (name, source) in [
        ("a.txt", "../a.txt"),
        ("a.txt", "/etc/hosts"),
        ("../a.txt", "a.txt"),
        (".tmp-x", "a.txt"),
    ] {
        let mut f = fx(vec![node("n", "true")]);
        f.spec.artifacts = Some(artifacts(&f, &[(name, source)]));
        assert!(run(&f.spec).is_err(), "{name} {source}");
        assert!(f.store.launches(f.attempt()).unwrap().is_empty());
    }
    let mut f = fx(vec![node("n", "true")]);
    let mut plan = artifacts(&f, &[("a.txt", "a.txt")]);
    plan.workspace = PathBuf::from("relative/ws");
    f.spec.artifacts = Some(plan);
    assert!(run(&f.spec).is_err());
}

#[test]
fn the_deadline_counts_the_whole_attempt() {
    let mut f = fx(vec![]);
    let marker = f.path("deadline-command-started");
    f.spec.nodes = vec![node(
        "n",
        &format!("touch {}; sleep 30; echo deadline-missed", marker.display()),
    )];
    f.spec.deadline_ms = Some(500);
    f.spec.command_timeout_ms = Some(60_000);
    let t = settled(&run(&f.spec).unwrap());
    assert_eq!(t.outcome(), Outcome::TimedOut);
    let info = t.timeout().unwrap();
    assert_eq!(info.kind, TimeoutKind::Deadline);
    // The deadline includes registration and admission. If those consume
    // the budget, no launch may occur. The durable launch claim is made
    // after the final Waiting look and before entering the Running loop;
    // a child/Running receipt may still be pending at that boundary.
    let launches = f.store.launches(f.attempt()).unwrap();
    let running = f
        .store
        .get(f.attempt())
        .unwrap()
        .unwrap()
        .receipts
        .reached(Receipt::Running);
    if launches.is_empty() {
        assert_eq!(info.phase, TimeoutPhase::Waiting);
        assert!(!running);
        assert!(!marker.exists());
    } else {
        assert_eq!(launches.len(), 1);
        assert_eq!(info.phase, TimeoutPhase::Running);
        use task_traffic_controller::{managed, receipt::Phase, Context, Paths};
        let ctx = Context::new(Paths::new(
            &f.spec.traffic_state_dir,
            f.spec.traffic_config_path(),
        ));
        let id = remote_task_runner::supervisor::spec::command_id(f.attempt(), 0);
        let managed::Loaded::Ok(sidecar) = managed::load_sidecar(&ctx, &id) else {
            panic!("the claimed launch must have a durable terminal sidecar");
        };
        assert_eq!(sidecar.phase, Phase::Terminal);
        assert!(sidecar.launch_intent_at.is_some());
        assert!(sidecar.terminal.is_some());
        if let Some(child) = sidecar.child {
            assert!(!signal_pid(child.pid, 0));
        }
        if let Some(identity) = &launches[0].2 {
            assert!(!signal_pid(identity.pid, 0));
        }
    }
    assert!(!f.output().contains("deadline-missed"));
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
        start: platform::process_start_identity(child.id())
            .identity()
            .expect("the identity of a live child")
            .to_string(),
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
        start: platform::process_start_identity(child.id())
            .identity()
            .expect("the identity of a live child")
            .to_string(),
    };
    claim(&f, "n", Some((&platform::boot_id().unwrap(), &p)));
    // The leader dies (and is reaped); its background child stays.
    child.kill().unwrap();
    child.wait().unwrap();
    let bgp = read_pid(&bg).unwrap();
    assert!(alive(bgp));
    // Nothing says that what is left under the dead leader's number is the
    // command's: the number may be another group's by now. Nothing is
    // signalled, and the attempt stays unresolved for that reason alone.
    let r = recover(&recover_spec(&f), &f.store).unwrap();
    assert!(r.unresolved.is_some() && r.identity_unknown, "{r:?}");
    assert_eq!(r.termination, None);
    assert!(alive(bgp), "left alone");
    // The supervisor process says so with its own exit code, which its
    // parent does not count as a failure.
    let spec_path = write_spec(&f, &recover_spec(&f));
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    assert_eq!(
        sup.wait().unwrap().code(),
        Some(code::IDENTITY_UNKNOWN),
        "unresolved by identity alone"
    );
    assert!(alive(bgp), "left alone");
    // With the descendant recorded while the command ran (as the tracker of
    // a managed command does: PID and start time), it is the command's.
    record_descendant(&f, 0, bgp as u32);
    let t = settled(&recover(&recover_spec(&f), &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    wait_for("the descendant to be gone", || !alive(bgp));
}

/// Write the record a managed command's tracker keeps of a descendant of
/// node `index`: its PID and start time, read here from the live process.
fn record_descendant(f: &Fx, index: usize, pid: u32) {
    use task_traffic_controller::{managed, process, Context, Paths};
    let id = remote_task_runner::supervisor::spec::command_id(f.attempt(), index);
    let start = process::start_time(pid).expect("the descendant is running");
    let sidecar: managed::Sidecar = serde_json::from_value(serde_json::json!({
        "version": 1,
        "command_id": id,
        "spec_digest": "test",
        // Settled for the controller: only the stop is looked at here.
        "phase": "terminal",
        "boot_id": null,
        "registered_at": "2026-01-01T00:00:00Z",
        "owner_pid": std::process::id(),
        "owner_start_time": null,
        "descendants": [{ "pid": pid, "start_time": start }],
    }))
    .unwrap();
    let paths = Paths::new(&f.spec.traffic_state_dir, f.spec.traffic_config_path());
    managed::write_sidecar(&Context::new(paths), &sidecar).unwrap();
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

// --------------------------------------- a supervisor that died while settling

/// The attempt's spool as a supervisor left it that died right after closing
/// it: `bytes` on stdout, the `closed` marker, nothing in the database.
fn closed_spool(f: &Fx, bytes: &[u8]) {
    let s = AttemptSpool::open(&f.spec.spool_dir(), f.attempt(), 1 << 20).unwrap();
    s.writer(LogStream::Stdout).append(bytes).unwrap();
    s.finish().unwrap();
}

fn log_ref(f: &Fx) -> remote_task_runner::storage::NewBlobRef {
    remote_task_runner::storage::NewBlobRef {
        ref_id: format!("{}:logs", f.attempt()),
        attempt_id: f.attempt().to_string(),
        kind: remote_task_runner::storage::BlobKind::Log,
        locator: format!("{}/logs", f.attempt()),
    }
}

/// The attempt has its one log reference (a pin needs the reference).
fn has_log_ref(f: &Fx) -> bool {
    let id = format!("{}:logs", f.attempt());
    let there = f.store.pin_blob(&id).is_ok();
    if there {
        f.store.unpin_blob(&id).unwrap();
    }
    there
}

fn assert_settled_with_output(f: &Fx, bytes: u64) {
    let result = f.store.get_result(f.attempt()).unwrap().expect("a result");
    assert_eq!(result["logs"]["stdout"]["bytes"], bytes);
    assert_eq!(
        f.store
            .log_position(f.attempt(), "stdout")
            .unwrap()
            .written_bytes,
        bytes
    );
    assert!(has_log_ref(f));
    assert!(f.events(f.attempt(), "spool_failed").is_empty());
    assert_eq!(f.state(), AttemptState::Terminal);
    let (out, last) = f.output_of(f.attempt());
    assert_eq!(out.len() as u64, bytes);
    assert!(last.eof);
}

#[test]
fn a_crash_right_after_closing_the_spool_is_settled_from_the_closed_spool() {
    let f = fx(vec![node("n", "true")]);
    // A launch without a process identity: no made-up pid is ever stored
    // (the fixture and recovery signal what the store names).
    claim(&f, "n", None);
    closed_spool(&f, b"what the command wrote");
    // `run` takes the recovery way (a launch is recorded) and finds the
    // spool closed: that is not a spool failure.
    let r = run(&f.spec).unwrap();
    assert!(r.finalized_here);
    assert_eq!(settled(&r).outcome(), Outcome::Interrupted);
    assert_settled_with_output(&f, 22);
    // Again, and from the recovery entry itself: nothing changes.
    let result = f.store.get_result(f.attempt()).unwrap();
    let again = run(&f.spec).unwrap();
    assert_eq!(
        (again.termination, again.finalized_here),
        (r.termination, false)
    );
    let again = recover(&f.spec, &f.store).unwrap();
    assert_eq!(
        (again.termination, again.finalized_here),
        (r.termination, false)
    );
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), result);
}

#[test]
fn a_crash_after_the_log_reference_is_settled_with_that_one_reference() {
    let f = fx(vec![node("n", "true")]);
    closed_spool(&f, b"12345");
    let pos = remote_task_runner::storage::LogPosition {
        written_bytes: 5,
        dropped_bytes: 0,
        last_seq: 1,
    };
    f.store
        .update_log_position(f.attempt(), "stdout", pos)
        .unwrap();
    f.store.add_blob_ref(&log_ref(&f), 50).unwrap();
    let r = recover(&f.spec, &f.store).unwrap();
    assert!(r.finalized_here);
    assert_eq!(settled(&r).outcome(), Outcome::Interrupted);
    assert_settled_with_output(&f, 5);
    // One reference: once it is collected the files are queued, once.
    f.store
        .ack_collected(f.attempt(), CoordinatorEpoch(1), platform::wall_ms())
        .unwrap();
    let gc = f.store.gc(u64::MAX / 4, 0, 0).unwrap();
    assert_eq!(gc.queued, [format!("{}/logs", f.attempt())]);
    // Collected and deleted: a later start completes nothing a second time
    // and does not bring the files (or a reference to them) back.
    let root = f.spec.spool_dir();
    let result = f.store.get_result(f.attempt()).unwrap();
    assert_eq!(drain_pending_deletes(&f.store, &root).unwrap().deleted, 1);
    let again = run(&f.spec).unwrap();
    assert_eq!(
        (again.termination, again.finalized_here),
        (r.termination, false)
    );
    assert!(!root.join(f.attempt().to_string()).exists());
    assert!(!has_log_ref(&f));
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), result);
}

#[test]
fn a_crash_between_the_termination_and_the_result_is_completed_by_the_next_start() {
    let f = fx(vec![node("n", "touch /nonexistent-dir/never")]);
    closed_spool(&f, b"done");
    f.store.add_blob_ref(&log_ref(&f), 50).unwrap();
    f.store
        .transition(f.attempt(), AttemptState::Finalizing, 51)
        .unwrap();
    let ok =
        remote_task_runner::state::Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None)
            .unwrap();
    f.store.finalize(f.attempt(), ok, 52).unwrap();
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    // The termination that was fixed holds, and gets its result.
    let r = run(&f.spec).unwrap();
    assert_eq!((r.termination, r.finalized_here), (Some(ok), false));
    assert_settled_with_output(&f, 4);
    let result = f.store.get_result(f.attempt()).unwrap().unwrap();
    assert_eq!(result["termination"]["outcome"], "succeeded");
    // Nothing was started for it.
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
    let again = run(&f.spec).unwrap();
    assert_eq!(again.termination, Some(ok));
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), Some(result));
}

#[test]
fn a_crash_before_the_spool_was_closed_is_completed_too() {
    // Terminal (a competing writer fixed it), the spool still open on disk.
    let f = fx(vec![node("n", "true")]);
    {
        let s = AttemptSpool::open(&f.spec.spool_dir(), f.attempt(), 1 << 20).unwrap();
        s.writer(LogStream::Stdout).append(b"abc").unwrap();
        s.sync().unwrap();
    }
    f.store
        .transition(f.attempt(), AttemptState::Finalizing, 5)
        .unwrap();
    let cancelled =
        remote_task_runner::state::Termination::new(Outcome::Cancelled, None, None).unwrap();
    f.store.finalize(f.attempt(), cancelled, 6).unwrap();
    let r = recover(&f.spec, &f.store).unwrap();
    assert_eq!((r.termination, r.finalized_here), (Some(cancelled), false));
    assert_settled_with_output(&f, 3);
}

#[test]
fn a_crash_while_settling_after_a_write_failure_is_completed_too() {
    // The reader failed (`spool_failed`, stage `write`), the supervisor
    // settled through the spool all the same and died between fixing the
    // termination and storing the result.
    let f = fx(vec![node("n", "true")]);
    let run_id = f.store.get(f.attempt()).unwrap().unwrap().run_id;
    f.store
        .append_event(
            Some(&run_id),
            Some(f.attempt()),
            "spool_failed",
            &serde_json::json!({ "stage": "write", "error": "read failed" }),
            40,
        )
        .unwrap();
    closed_spool(&f, b"partial");
    f.store.add_blob_ref(&log_ref(&f), 50).unwrap();
    f.store
        .transition(f.attempt(), AttemptState::Finalizing, 51)
        .unwrap();
    let interrupted = remote_task_runner::state::Termination::new(
        Outcome::Interrupted,
        Some(Exit::Code(0)),
        None,
    )
    .unwrap();
    f.store.finalize(f.attempt(), interrupted, 52).unwrap();
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    // That event does not say "settled without its spool": the result is
    // stored by the next start.
    let r = run(&f.spec).unwrap();
    assert_eq!(
        (r.termination, r.finalized_here),
        (Some(interrupted), false)
    );
    let result = f.store.get_result(f.attempt()).unwrap().expect("a result");
    assert_eq!(result["termination"]["outcome"], "interrupted");
    assert_eq!(result["logs"]["stdout"]["bytes"], 7);
    assert!(has_log_ref(&f));
    assert_eq!(f.events(f.attempt(), "spool_failed").len(), 1);
    let again = recover(&f.spec, &f.store).unwrap();
    assert_eq!(again.termination, Some(interrupted));
    assert_eq!(f.store.get_result(f.attempt()).unwrap(), Some(result));
}

#[test]
fn a_closed_spool_of_an_attempt_that_never_launched_is_settled_through_it() {
    // Cancelled before the start; the supervisor closed the (empty) spool and
    // died before the reference, the termination and the result.
    let f = fx(vec![node("n", "touch /nonexistent-dir/never")]);
    f.store.cancel(f.attempt(), CoordinatorEpoch(1), 5).unwrap();
    closed_spool(&f, b"");
    let r = run(&f.spec).unwrap();
    assert!(r.finalized_here);
    assert_eq!(settled(&r).outcome(), Outcome::Cancelled);
    assert_settled_with_output(&f, 0);
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
    let again = run(&f.spec).unwrap();
    assert_eq!(
        (again.termination, again.finalized_here),
        (r.termination, false)
    );

    // The same without a cancel: nothing ran, so it is interrupted, and
    // nothing is started for it now.
    let f = fx(vec![node("n", "touch /nonexistent-dir/never")]);
    closed_spool(&f, b"");
    let r = run(&f.spec).unwrap();
    assert!(r.finalized_here);
    assert_eq!(settled(&r).outcome(), Outcome::Interrupted);
    assert_settled_with_output(&f, 0);
    assert!(f.store.launches(f.attempt()).unwrap().is_empty());
}

#[test]
fn a_terminal_attempt_loses_the_pipes_its_supervisor_left() {
    let f = fx(vec![node("n", "true")]);
    run(&f.spec).unwrap();
    let logs = f
        .spec
        .spool_dir()
        .join(f.attempt().to_string())
        .join("logs");
    let fifo = logs.join("node-0.pipe");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    recover(&f.spec, &f.store).unwrap();
    assert!(!fifo.exists());
}

#[test]
fn an_attempt_settled_without_its_spool_keeps_its_files_accounted_for() {
    let f = fx(vec![node("a", "true"), node("b", "true")]);
    let root = f.spec.spool_dir();
    // Another writer holds the spool: this supervisor cannot resume it.
    let held = AttemptSpool::open(&root, f.attempt(), 1 << 20).unwrap();
    held.writer(LogStream::Stdout)
        .append(b"0123456789")
        .unwrap();
    held.sync().unwrap();
    // What the lost supervisor left beside the spool: the pipe of a node,
    // and something that only has a pipe's name.
    let logs = root.join(f.attempt().to_string()).join("logs");
    let fifo = logs.join("node-0.pipe");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    std::fs::write(logs.join("node-1.pipe"), b"not a pipe").unwrap();
    let t = settled(&recover(&f.spec, &f.store).unwrap());
    assert_eq!(t.outcome(), Outcome::Interrupted);
    let ev = f.events(f.attempt(), "spool_failed");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["stage"], "open");
    assert!(f.store.get_result(f.attempt()).unwrap().is_none());
    // The pipe is gone, the other file is not ours to remove.
    assert!(!fifo.exists());
    assert_eq!(
        std::fs::read(logs.join("node-1.pipe")).unwrap(),
        b"not a pipe"
    );
    // The last position the spool published is stored, and counts against
    // the task's other attempts while the files are there.
    assert_eq!(
        f.store
            .log_position(f.attempt(), "stdout")
            .unwrap()
            .written_bytes,
        10
    );
    let second = f.next_attempt();
    let others = |a: &AttemptId| f.store.log_bytes_of_other_attempts(a).unwrap();
    assert_eq!(others(&second.attempt_id), 10);
    // The directory is referenced, so GC collects it: nothing is left that
    // no one can reach.
    assert!(has_log_ref(&f));
    drop(held);
    f.store
        .ack_collected(f.attempt(), CoordinatorEpoch(1), platform::wall_ms())
        .unwrap();
    let gc = f.store.gc(u64::MAX / 4, 0, 0).unwrap();
    assert_eq!(gc.queued, [format!("{}/logs", f.attempt())]);
    // Collected bytes no longer count against the task's limit, also
    // before the files themselves are deleted.
    assert_eq!(others(&second.attempt_id), 0);
    let drained = drain_pending_deletes(&f.store, &root).unwrap();
    assert_eq!(drained.deleted, 1);
    assert!(!root.join(f.attempt().to_string()).exists());
    assert_eq!(others(&second.attempt_id), 0);
    // A later start of the supervisor does not bring the directory back.
    let again = run(&f.spec).unwrap();
    assert_eq!(again.termination, Some(t));
    assert!(!root.join(f.attempt().to_string()).exists());
}

// ------------------------------------------- A21 the agent dies, the supervisor does not

fn exe() -> PathBuf {
    assert_cmd::cargo::cargo_bin("remote-task-runner")
}

/// The spec file as the agent must write it: its user's alone.
fn write_private(p: &Path, text: &str) {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let _ = std::fs::remove_file(p);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(p)
        .unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

fn write_spec(f: &Fx, spec: &SuperviseSpec) -> PathBuf {
    let p = f.path("spec.json");
    write_private(&p, &serde_json::to_string(spec).unwrap());
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
    f.track_group(agent.id());
    wait_for("the command to be running", || {
        f.store.get(f.attempt()).unwrap().unwrap().state == AttemptState::Active
    });
    // The agent and everything left in its group die.
    signal_process_group(agent_pid, libc::SIGKILL);
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
    f.track(child.id());
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
    f.track(sup.id());
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
    let until = Instant::now() + Duration::from_secs(20);
    let mut report = supervise(&spec).unwrap();
    while report.unresolved.is_some() {
        // A fresh hidden process elsewhere on the host may temporarily make
        // the safe stop inconclusive. Like the agent, look again; do not turn
        // uncertainty into a termination or restart the command.
        assert_eq!(report.termination, None);
        assert_ne!(f.state(), AttemptState::Terminal);
        assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
        assert!(
            Instant::now() < until,
            "recovery stayed unresolved: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(spec.poll_ms));
        report = supervise(&spec).unwrap();
    }
    let t = settled(&report);
    assert_eq!(t.outcome(), Outcome::Interrupted);
    wait_for("the orphan to be gone", || !alive(leader_pid));
    assert!(!alive(read_pid(&bg).unwrap()));
    assert_eq!(f.store.launches(f.attempt()).unwrap().len(), 1);
    // Recovery resumed the dead supervisor's spool and closed it with the
    // attempt: readers reach its end and the result is stored.
    assert!(f.output_of(f.attempt()).1.eof);
    let result = f.store.get_result(f.attempt()).unwrap().expect("a result");
    assert_eq!(result["termination"]["outcome"], "interrupted");
    assert!(f.events(f.attempt(), "spool_failed").is_empty());
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
    f.track(child.id());
    child.wait().unwrap();
    // Killed by the hang-up (the default), not ignoring it.
    assert_eq!(f.term().exit(), Some(Exit::Signal(libc::SIGHUP)));
}

#[test]
fn the_supervisor_survives_a_hangup_and_finishes_the_attempt() {
    let f = fx(vec![node("n", "sleep 1.5; exit 0")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut sup = spawn_detached(&exe(), &spec_path).unwrap();
    f.track(sup.id());
    wait_for("the command to be running", || {
        f.state() == AttemptState::Active
    });
    signal_pid(sup.id(), libc::SIGHUP);
    let status = sup.wait().unwrap();
    assert_eq!(status.code(), Some(code::SETTLED), "{status:?}");
    assert_eq!(f.term().outcome(), Outcome::Succeeded);
}

#[test]
fn only_one_supervisor_may_hold_an_attempt() {
    let f = fx(vec![node("n", "sleep 2")]);
    let spec_path = write_spec(&f, &f.spec);
    let mut first = spawn_detached(&exe(), &spec_path).unwrap();
    f.track(first.id());
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
    write_private(&p, "{\"nope\": 1}");
    let out = Command::new(exe())
        .arg("supervise")
        .arg("--spec")
        .arg(&p)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code::BAD_SPEC));
}
