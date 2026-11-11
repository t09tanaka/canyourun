//! The worker forecast against real files: A06 (asking changes nothing),
//! A08 (an accepted node finishing, registered or not, is counted once) and
//! the stale coordinator. No process is started: a command is registered in
//! the task-traffic-controller ledger without running.

use remote_task_runner::error::ErrorCode;
use remote_task_runner::forecast::answer_with;
use remote_task_runner::forecast::plan::Ident;
use remote_task_runner::ids::*;
use remote_task_runner::storage::{AcceptedAttempt, NewAcceptance, WorkerStore};
use remote_task_runner::wire::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use task_traffic_controller::admit::LiveSnapshot;
use task_traffic_controller::config::Config;
use task_traffic_controller::context::{Context, FixedLive};
use task_traffic_controller::exec::Opts;
use task_traffic_controller::inspect::inspect_with;
use task_traffic_controller::managed::{self, CommandSpec as TtcSpec, Registration, TerminalKind};
use task_traffic_controller::paths::Paths;
use tempfile::TempDir;

struct Fx {
    dir: TempDir,
}

fn fx() -> Fx {
    Fx {
        dir: tempfile::Builder::new()
            .prefix("rtr-forecast-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap(),
    }
}

impl Fx {
    fn traffic(&self) -> PathBuf {
        self.dir.path().join("traffic")
    }
    fn ctx(&self) -> Context {
        Context::new(Paths::new(
            self.traffic(),
            self.traffic().join("config.toml"),
        ))
        .with_live(Arc::new(FixedLive(Some(LiveSnapshot {
            available_memory: 1 << 40,
            cpu_busy: 0.1,
            load_avg: 0.0,
        }))))
    }
}

fn cfg() -> Config {
    Config {
        budget: 1,
        ..Config::default()
    }
}

fn ident() -> Ident {
    Ident {
        worker_id: WorkerId::parse("w-test").unwrap(),
        boot_id: WorkerBootId::parse("boot-1").unwrap(),
        agent_epoch: AgentEpoch(1),
        forecast_ttl_ms: 2000,
    }
}

fn node(attempt: &AttemptId, id: &str, runtime_ms: u64) -> WorkloadNode {
    WorkloadNode {
        node_id: NodeId::parse(id).unwrap(),
        depends_on: vec![],
        priority: 0,
        weight: Some(1),
        predicted_rss_bytes: Some(1),
        predicted_runtime_ms: Some(runtime_ms),
        external_delay_ms: None,
        ready_in_ms: Some(0),
        concurrency: ConcurrencyLimits::default(),
        correlation: Correlation {
            attempt_id: attempt.clone(),
            node_id: NodeId::parse(id).unwrap(),
            ledger_entry: None,
        },
        shared_builder: None,
    }
}

fn plan(attempt: &AttemptId, nodes: Vec<WorkloadNode>) -> WorkloadPlan {
    WorkloadPlan {
        attempt_id: attempt.clone(),
        plan_digest: Digest::of_bytes(attempt.as_str().as_bytes()),
        expansion_seq: 0,
        nodes,
    }
}

fn request(target: WorkloadPlan, pending: Vec<WorkloadPlan>, epoch: u64) -> ForecastRequest {
    ForecastRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(epoch),
        workload: target,
        pending,
    }
}

fn accept(store: &WorkerStore, attempt: &AttemptId, node: &str) -> AcceptedAttempt {
    let d = Digest::of_bytes(attempt.as_str().as_bytes());
    let payload = AttemptPayload {
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: SourceId::generate(),
        plan_digest: d.clone(),
        manifest_digest: d,
        permit_id: None,
        commands: vec![remote_task_runner::wire::CommandSpec {
            node_id: NodeId::parse(node).unwrap(),
            argv: vec!["true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        lease_ms: 60_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: Vec::new(),
    };
    store
        .submit(
            NewAcceptance {
                attempt_id: attempt.clone(),
                run_id: payload.run_id.clone(),
                task_id: payload.task_id.clone(),
                generation: payload.generation,
                payload_hash: payload_hash(&payload).unwrap(),
                coordinator_epoch: CoordinatorEpoch(1),
                spec: serde_json::to_value(&payload).unwrap(),
            },
            1,
        )
        .unwrap();
    store.get(attempt).unwrap().unwrap()
}

/// Register the attempt's only node in the ledger, as its supervisor would.
fn register(ctx: &Context, attempt: &AttemptId, work: &Path) -> managed::OwnerGuard {
    let id = remote_task_runner::supervisor::spec::command_id(attempt, 0);
    let spec = TtcSpec {
        command_id: id,
        spec_digest: "digest".into(),
        opts: Opts {
            weight: Some(1),
            front: false,
            key: None,
            argv: vec!["true".into()],
            cwd: work.to_path_buf(),
            env: None,
            stdout_log: None,
        },
        attempt: None,
        services: vec![],
    };
    match managed::register(ctx, &cfg(), &spec).unwrap() {
        Registration::Registered(_, guard) => guard,
        Registration::AlreadyTerminal(_) => panic!("fresh id"),
    }
}

/// Every file under `dir` with its bytes.
fn tree(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.insert(p.clone(), b"<dir>".to_vec());
                stack.push(p);
            } else {
                out.insert(p.clone(), std::fs::read(&p).unwrap());
            }
        }
    }
    out
}

fn counts(store: &WorkerStore) -> (usize, usize, u64) {
    (
        store.list_attempts(None, true, 1000).unwrap().len(),
        store.events_after(0, 10_000).unwrap().len(),
        store.count_launches().unwrap(),
    )
}

#[test]
fn a06_a_hundred_forecasts_add_no_entry_lease_job_or_reservation() {
    let f = fx();
    let state = f.dir.path().join("state");
    let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
    store.start_agent(1).unwrap();
    let a = AttemptId::generate();
    let b = AttemptId::generate();
    let rec = accept(&store, &b, "work");
    let ctx = f.ctx();
    let work = f.dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let _guard = register(&ctx, &b, &work);
    let attempts = store.list_nonterminal().unwrap();
    assert_eq!(attempts, vec![rec]);

    let traffic_before = tree(&f.traffic());
    let state_before = counts(&store);
    let ledger_before = inspect_with(&ctx, &cfg()).unwrap();
    assert_eq!(ledger_before.entries.len(), 1);
    let req = request(
        plan(&a, vec![node(&a, "main", 10_000)]),
        vec![plan(&b, vec![node(&b, "work", 20_000)])],
        1,
    );
    let first = answer_with(&ctx, &cfg(), &ident(), &attempts, CoordinatorEpoch(1), &req).unwrap();
    for _ in 0..100 {
        let again =
            answer_with(&ctx, &cfg(), &ident(), &attempts, CoordinatorEpoch(1), &req).unwrap();
        assert_eq!(again.snapshot_digest, first.snapshot_digest);
    }
    assert_eq!(
        tree(&f.traffic()),
        traffic_before,
        "the traffic state changed"
    );
    assert_eq!(counts(&store), state_before, "the worker store changed");
    let after = inspect_with(&ctx, &cfg()).unwrap();
    assert_eq!(after.entries, ledger_before.entries);
    assert_eq!(after.snapshot_digest, ledger_before.snapshot_digest);
}

#[test]
fn a06_a_state_directory_that_does_not_exist_stays_absent() {
    let f = fx();
    let a = AttemptId::generate();
    let req = request(plan(&a, vec![node(&a, "main", 10_000)]), vec![], 1);
    let ans = answer_with(&f.ctx(), &cfg(), &ident(), &[], CoordinatorEpoch(1), &req).unwrap();
    assert_eq!(ans.target.finish_in_ms, Some(10_000));
    assert!(!f.traffic().exists());
}

#[test]
fn a08_a_node_is_one_demand_before_it_registers_after_and_once_finished() {
    let f = fx();
    let state = f.dir.path().join("state");
    let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
    store.start_agent(1).unwrap();
    let a = AttemptId::generate();
    let b = AttemptId::generate();
    accept(&store, &b, "work");
    let attempts = store.list_nonterminal().unwrap();
    let ctx = f.ctx();
    // `b/work` is pending at the coordinator too, with a recorded 20 s;
    // the target is 10 s on the one slot.
    let req = request(
        plan(&a, vec![node(&a, "main", 10_000)]),
        vec![plan(&b, vec![node(&b, "work", 20_000)])],
        1,
    );
    let ask = |attempts: &[AcceptedAttempt]| {
        answer_with(&ctx, &cfg(), &ident(), attempts, CoordinatorEpoch(1), &req).unwrap()
    };
    // Accepted, not registered: its 20 s are ahead of the target.
    assert_eq!(ask(&attempts).target.finish_in_ms, Some(30_000));

    // Registered (waiting for the slot): the ledger's entry stands for it.
    // It carries the 20 s its own history recorded, so the finish is one
    // ledger entry plus the target: 30 s, the same as when it was only
    // pending. Counted twice it would be 50 s.
    let work = f.dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let argv = vec!["true".to_string()];
    let key = task_traffic_controller::exec::command_key(&ctx, &work, None, &argv).unwrap();
    task_traffic_controller::history::append(
        ctx.paths.history_file(),
        &task_traffic_controller::history::Record {
            key: key.hash.clone(),
            repo: key.repo.clone(),
            subdir: key.subdir.clone(),
            argv: key.argv.clone(),
            key_argv: String::new(),
            started_at: "2030-01-01T00:00:00Z".parse().unwrap(),
            wall_secs: 20.0,
            cpu_secs: 1.0,
            max_rss_bytes: 1,
            exit_code: 0,
            weight_used: 1,
            contended: false,
            measurement_id: None,
        },
    )
    .unwrap();
    let guard = register(&ctx, &b, &work);
    let registered = ask(&attempts);
    assert_eq!(registered.target.finish_in_ms, Some(30_000));
    let covered: usize = registered
        .coverage
        .iter()
        .filter(|c| c.attempt_id == b)
        .map(|c| c.node_ids.iter().filter(|n| n.as_str() == "work").count())
        .sum();
    assert_eq!(covered, 1);
    // The simulation's own report: the node is replaced by its ledger entry
    // and nothing of it is applied as overlay demand.
    let insp = inspect_with(&ctx, &cfg()).unwrap();
    let nodes = remote_task_runner::forecast::plan::accepted_nodes(
        &b,
        &serde_json::from_value(attempts[0].spec.clone()).unwrap(),
    )
    .unwrap();
    let (_, report) =
        remote_task_runner::forecast::compute(&ctx, &insp, 1, &ident(), &req, &nodes).unwrap();
    assert_eq!(report.replaced_by_ledger, [format!("{b}/work")]);
    assert!(report.applied.is_empty());

    // Finished: the ledger entry is gone, the sidecar says so, and the
    // attempt is still not terminal in the worker's store. No phantom demand.
    managed::finish(&ctx, guard, TerminalKind::Cancelled).unwrap();
    assert_eq!(ask(&attempts).target.finish_in_ms, Some(10_000));
}

#[test]
fn a_request_from_a_stale_coordinator_is_refused() {
    let f = fx();
    let a = AttemptId::generate();
    let req = request(plan(&a, vec![node(&a, "main", 1)]), vec![], 1);
    let e = answer_with(&f.ctx(), &cfg(), &ident(), &[], CoordinatorEpoch(2), &req).unwrap_err();
    assert_eq!(e.code, ErrorCode::StaleEpoch);
    // The same epoch and a newer one are served.
    assert!(answer_with(&f.ctx(), &cfg(), &ident(), &[], CoordinatorEpoch(1), &req).is_ok());
    let newer = request(plan(&a, vec![node(&a, "main", 1)]), vec![], 9);
    assert!(answer_with(&f.ctx(), &cfg(), &ident(), &[], CoordinatorEpoch(2), &newer).is_ok());
}

#[test]
fn an_unreadable_attempt_record_is_an_error_never_a_free_worker() {
    let f = fx();
    let state = f.dir.path().join("state");
    let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
    store.start_agent(1).unwrap();
    let b = AttemptId::generate();
    let mut rec = accept(&store, &b, "work");
    rec.spec = serde_json::json!({"not": "a payload"});
    let a = AttemptId::generate();
    let req = request(plan(&a, vec![node(&a, "main", 1)]), vec![], 1);
    let e = answer_with(
        &f.ctx(),
        &cfg(),
        &ident(),
        &[rec],
        CoordinatorEpoch(1),
        &req,
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::Internal);
}

#[test]
fn an_oversized_request_is_refused_before_any_file_is_read() {
    let f = fx();
    let a = AttemptId::generate();
    let pending = (0..65).map(|_| plan(&a, vec![node(&a, "n", 1)])).collect();
    let req = request(plan(&a, vec![node(&a, "main", 1)]), pending, 1);
    let e = answer_with(&f.ctx(), &cfg(), &ident(), &[], CoordinatorEpoch(1), &req).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(!f.traffic().exists());
}
