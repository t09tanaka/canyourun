use remote_task_runner::config::resolve::WorkerSpec;
use remote_task_runner::config::runtime::{WorkerInputs, WorkerTarget};
use remote_task_runner::config::{resolve, Inputs, Overlay, ResolvedConfig};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{accept, connect, ClientConfig, Connected, ServerConfig};
use remote_task_runner::scheduler::UnknownCosts;
use remote_task_runner::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::sync::Arc;

fn config() -> ResolvedConfig {
    resolve(&Inputs {
        overlay: &Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap()
}
fn inputs(root: &std::path::Path) -> WorkerInputs {
    WorkerInputs {
        state_dir: root.join("worker"),
        worker_id: WorkerId::parse("local").unwrap(),
        boot_id: WorkerBootId::parse("boot-1").unwrap(),
        supervisor_exe: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        cluster_key_path: root.join("cluster.key"),
        env: BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), root.display().to_string()),
        ]),
    }
}

#[test]
fn runtime_factory_enables_setup_and_starts_agent() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_active_task.value = 3;
    cfg.prefetch_per_worker.value = 2;
    cfg.log_spool_limit_bytes.value = 8192;
    cfg.term_grace_seconds.value = 4;
    cfg.profile.value = Some("custom".into());
    let input = inputs(root.path());
    let runtime = cfg.worker_runtime(input.clone()).unwrap();
    assert_eq!(runtime.agent_config().state_dir, input.state_dir);
    assert_eq!(
        runtime.setup_config().state_dir,
        runtime.agent_config().state_dir
    );
    assert_eq!(
        runtime.setup_config().worker_id,
        runtime.agent_config().worker_id
    );
    assert_eq!(
        runtime.setup_config().supervisor_exe,
        runtime.agent_config().supervisor_exe
    );
    assert_eq!(runtime.setup_config().env, input.env);
    assert_eq!(runtime.setup_config().profile, "custom");
    assert_eq!(
        runtime.setup_config().traffic_state_dir,
        runtime.agent_config().traffic_state_dir
    );
    assert_eq!(
        runtime.setup_config().traffic_config,
        runtime.agent_config().traffic_config
    );
    assert_eq!(runtime.setup_config().term_grace_ms, 4000);
    assert_eq!(runtime.setup_config().log_limit_bytes, 8192);
    assert_eq!(runtime.setup_config().lease_ms, 60_000);
    assert_eq!(
        runtime.setup_config().poll_ms,
        runtime.agent_config().poll_ms
    );
    assert_eq!(
        runtime.setup_config().kill_wait_ms,
        runtime.agent_config().kill_wait_ms
    );
    assert_eq!(
        runtime.setup_config().inhibit_sleep,
        runtime.agent_config().inhibit_sleep
    );
    assert_eq!(runtime.agent_config().retention_ms, 7 * 86400 * 1000);
    assert!(runtime.open_host().unwrap().setups().is_some());
    let agent = runtime.start().unwrap();
    assert_eq!(agent.config().max_active_task, 3);
    assert_eq!(agent.config().prefetch_per_worker, 2);
    assert_eq!(agent.config().log_limit_bytes, 8192);
    assert_eq!(agent.config().forecast_ttl_ms, 2000);
    agent.shutdown();
}

#[test]
fn runtime_factory_preserves_git_preflight_error() {
    let root = tempfile::tempdir().unwrap();
    let mut input = inputs(root.path());
    input.env.insert(
        "PATH".into(),
        root.path().join("missing").display().to_string(),
    );
    let err = config()
        .worker_runtime(input)
        .unwrap()
        .open_host()
        .err()
        .unwrap();
    assert_eq!(err.code, remote_task_runner::error::ErrorCode::Blocked);
    assert!(err.message.contains("Git") || err.message.contains("git"));
}

async fn connected(c: &remote_task_runner::coordinator::Coordinator, name: &str) -> Connected {
    let (near, far) = duplex_pair(65536);
    let cluster = c.store().cluster_id().clone();
    let client = ClientConfig::new(
        cluster.clone(),
        c.store().coordinator_id().clone(),
        c.epoch(),
    );
    let server = ServerConfig::new(cluster, super::proto_common::identity(name));
    let (connection, accepted) = tokio::join!(connect(near, client), accept(far, server));
    // Keep the server side alive for the lifetime of the test.
    tokio::spawn(async move {
        let mut incoming = accepted.unwrap().incoming;
        while let Some(_request) = incoming.next().await {}
    });
    connection.unwrap()
}

#[tokio::test]
async fn runtime_attach_applies_only_verified_identity_priorities() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.local_worker_priority.value = -9;
    let mut priority = cfg.local_worker_priority.clone();
    priority.value = 21;
    cfg.workers.push(WorkerSpec {
        id: "remote".into(),
        ssh: "host".into(),
        service_mode: None,
        labels: vec![],
        ephemeral: true,
        priority,
    });
    let settings = cfg.runtime_settings().unwrap();
    let c = settings
        .start_coordinator(
            CoordinatorStore::open(root.path(), 1).unwrap(),
            Arc::new(UnknownCosts),
        )
        .unwrap();
    let local = WorkerId::parse("local").unwrap();
    let remote = WorkerId::parse("remote").unwrap();
    cfg.attach_worker(
        &c,
        WorkerTarget::Local(&local),
        connected(&c, "local").await,
    )
    .unwrap();
    cfg.attach_worker(
        &c,
        WorkerTarget::Remote("remote"),
        connected(&c, "remote").await,
    )
    .unwrap();
    assert_eq!(c.scheduler().priority_of(&local), -9);
    assert_eq!(c.scheduler().priority_of(&remote), 21);
    assert_eq!(c.scheduler().cfg, settings.scheduler);
    let other = WorkerId::parse("other").unwrap();
    for target in [
        WorkerTarget::Local(&local),
        WorkerTarget::Remote("remote"),
        WorkerTarget::Remote("missing"),
    ] {
        assert!(cfg
            .attach_worker(&c, target, connected(&c, "other").await)
            .is_err());
    }
    assert_eq!(c.scheduler().priority_of(&other), 0);
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = settings
        .start_coordinator(
            CoordinatorStore::open(foreign_root.path(), 1).unwrap(),
            Arc::new(UnknownCosts),
        )
        .unwrap();
    assert!(cfg
        .attach_worker(
            &c,
            WorkerTarget::Local(&local),
            connected(&foreign, "local").await
        )
        .is_err());
    cfg.local_worker.value = false;
    assert!(cfg
        .attach_worker(
            &c,
            WorkerTarget::Local(&local),
            connected(&c, "local").await
        )
        .is_err());
    assert_eq!(c.scheduler().priority_of(&local), -9);
}
