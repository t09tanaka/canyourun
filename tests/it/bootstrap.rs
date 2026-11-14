//! Real owner processes, all under private temporary state and environment.
use remote_task_runner::ids::{ClusterId, WorkerId};
use remote_task_runner::service::bootstrap::{ensure, Spec};
use remote_task_runner::service::resident::{Client, Operation, Reply};
use remote_task_runner::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, Spec) {
    let root = tempfile::Builder::new()
        .prefix("rtr-boot-")
        .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
        .unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = root.path().join("parent");
    let worker = root.path().join("worker");
    for dir in [&state, &worker] {
        std::fs::create_dir(dir).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let key = root.path().join("key");
    remote_task_runner::secrets::key::init(&key).unwrap();
    let spec = Spec {
        state_dir: state,
        worker_state_dir: worker,
        worker_id: WorkerId::parse("persistent-local").unwrap(),
        cluster_key_path: key,
        executable: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        env: BTreeMap::from([
            ("HOME".into(), root.path().display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]),
        repo_root: None,
        expected_cluster: None,
    };
    (root, spec)
}
async fn shutdown(client: &Client) {
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.socket_path().exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn load(spec: &Spec) -> Client {
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    Client::load(&spec.state_dir, store.cluster_id().clone()).unwrap()
}

#[test]
#[ignore = "invoked as a dedicated launcher subprocess by the process regression"]
fn launcher_process() {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes).unwrap();
    let spec: Spec = serde_json::from_slice(&bytes).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(ensure(spec)).unwrap();
    // Runtime teardown and launcher exit must leave the owner running.
}

#[tokio::test]
async fn owner_survives_launcher_exit_and_rotation_reconnects() {
    let (_root, spec) = fixture();
    let mut launcher = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bootstrap::launcher_process",
            "--ignored",
            "--nocapture",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    launcher
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&spec).unwrap())
        .unwrap();
    let output = tokio::task::spawn_blocking(move || launcher.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let client = load(&spec);
    let epoch = match client.request(Operation::Ping).await.unwrap() {
        Reply::Ready { epoch } => epoch,
        other => panic!("{other:?}"),
    };
    shutdown(&client).await;
    let mut again = spec.clone();
    again.expected_cluster = Some(client.cluster_id().clone());
    let fresh = ensure(again).await.unwrap();
    assert!(
        matches!(fresh.request(Operation::Ping).await.unwrap(), Reply::Ready { epoch: e } if e != epoch)
    );
    assert!(client.request(Operation::Ping).await.is_err());
    assert_eq!(fresh.cluster_id(), client.cluster_id());
    shutdown(&fresh).await;
}

#[tokio::test]
async fn concurrent_startup_has_one_owner_and_foreign_cluster_is_refused() {
    let (_root, spec) = fixture();
    let mut joins = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let copy = spec.clone();
        joins.spawn(ensure(copy));
    }
    let mut clients = Vec::new();
    while let Some(result) = joins.join_next().await {
        clients.push(result.unwrap().unwrap());
    }
    let first = clients.first().unwrap();
    let before = first.request(Operation::Ping).await.unwrap();
    for client in &clients {
        assert_eq!(client.cluster_id(), first.cluster_id());
        assert_eq!(
            serde_json::to_value(client.request(Operation::Ping).await.unwrap()).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    let mut foreign = spec.clone();
    foreign.expected_cluster = Some(ClusterId::generate());
    assert!(ensure(foreign)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("foreign"));
    shutdown(first).await;
}

#[tokio::test]
async fn unsafe_keys_and_tokens_are_rejected_without_changing_files() {
    let (_root, spec) = fixture();
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(ensure(spec.clone()).await.is_err());
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let key = std::fs::read(&spec.cluster_key_path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&spec.cluster_key_path)
        .unwrap()
        .set_len(1 << 34)
        .unwrap();
    assert!(ensure(spec.clone())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("exceeds"));
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::write(&spec.cluster_key_path, key).unwrap();
    let client = ensure(spec.clone()).await.unwrap();
    let token = client.socket_path().parent().unwrap().join("owner.token");
    let original = std::fs::read(&token).unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let alias = token.with_extension("alias");
    std::fs::hard_link(&token, &alias).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(alias).unwrap();
    let saved = token.with_extension("saved");
    std::fs::rename(&token, &saved).unwrap();
    std::os::unix::fs::symlink(&saved, &token).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(&token).unwrap();
    std::fs::rename(saved, &token).unwrap();
    std::fs::write(&token, "bad-token").unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::write(&token, original).unwrap();
    shutdown(&client).await;
}

#[tokio::test]
async fn retained_input_is_private_idempotent_and_restarts_through_shared_argv() {
    use remote_task_runner::service::bootstrap::{daemon_argv, write_input};
    let (root, spec) = fixture();
    let input = root.path().join("resident.json");
    let managed = write_input(&input, &spec).unwrap();
    let same = write_input(&input, &spec).unwrap();
    assert_eq!(
        (managed.device, managed.inode, &managed.sha256),
        (same.device, same.inode, &same.sha256)
    );
    assert_eq!(
        std::fs::metadata(&input).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut changed = spec.clone();
    changed.worker_id = WorkerId::parse("different").unwrap();
    assert!(write_input(&input, &changed).is_err());
    let mut previous = None;
    for _ in 0..2 {
        let mut child = tokio::process::Command::new(&spec.executable)
            .args(daemon_argv(&input).unwrap())
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(child.try_wait().unwrap().is_none(), "daemon exited early");
                if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                    if let Ok(client) = Client::load(&spec.state_dir, store.cluster_id().clone()) {
                        if matches!(
                            client.request(Operation::Ping).await,
                            Ok(Reply::Ready { .. })
                        ) {
                            break client;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(spec.worker_state_dir.join("boot_id")).unwrap(),
            remote_task_runner::platform::boot_id().unwrap()
        );
        let worker =
            remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                .unwrap();
        assert_eq!(worker.worker_id(), &spec.worker_id);
        let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
        if let Some(epoch) = previous {
            assert_ne!(store.epoch(), epoch);
        }
        previous = Some(store.epoch());
        shutdown(&client).await;
        assert!(child.wait().await.unwrap().success());
        assert!(input.exists());
    }
    std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
    let failed = tokio::process::Command::new(&spec.executable)
        .args(daemon_argv(&input).unwrap())
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!failed.status.success());
    assert!(write_input(&input, &spec).is_err());
}

#[tokio::test]
async fn failed_child_and_interrupted_startup_do_not_keep_startup_lock() {
    let (_root, spec) = fixture();
    let mut failed = spec.clone();
    failed.executable = "/usr/bin/false".into();
    assert!(ensure(failed).await.is_err());
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(spec.state_dir.join("startup.lock"))
        .unwrap();
    lock.lock().unwrap();
    let waiting = tokio::spawn(ensure(spec.clone()));
    tokio::time::sleep(Duration::from_millis(80)).await;
    waiting.abort();
    assert!(waiting.await.err().unwrap().is_cancelled());
    drop(lock);
    let client = ensure(spec).await.unwrap();
    shutdown(&client).await;
}
