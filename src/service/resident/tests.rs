use super::Operation;
use super::*;
use crate::coordinator::{LinkError, WorkerLink};
use crate::state::{Outcome, Receipt};
use crate::storage::{NewAttempt, Seen};
use crate::wire::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Slow {
    active: AttemptStatus,
    collecting: AtomicBool,
    offline: AtomicBool,
    renewals: AtomicUsize,
}
impl WorkerLink for Slow {
    fn call(
        &self,
        request: crate::wire::Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            if self.offline.load(Ordering::SeqCst) {
                return Err(LinkError::Unconfirmed);
            }
            match request {
                crate::wire::Request::Inspect(_) => Ok(Response::Inspect(InspectResponse {
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: WorkerId::parse("local").unwrap(),
                    worker_boot_id: self.active.worker_boot_id.clone(),
                    agent_epoch: AgentEpoch(1),
                    snapshot_digest: Digest::of_bytes(b"snapshot"),
                    policy_digest: Digest::of_bytes(b"policy"),
                    active_task: 1,
                    prefetch_pending: 0,
                    max_active_task: 2,
                    prefetch_per_worker: 1,
                    disk_free_bytes: 1,
                    draining: false,
                })),
                crate::wire::Request::GetAttempt(_) => {
                    Ok(Response::GetAttempt(self.active.clone()))
                }
                crate::wire::Request::RenewLease(r) => {
                    self.renewals.fetch_add(1, Ordering::SeqCst);
                    Ok(Response::RenewLease(RenewLeaseResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        lease_seq: r.lease_seq,
                        extended: true,
                        lease_remaining_ms: r.lease_ms,
                        state: AttemptState::Active,
                    }))
                }
                crate::wire::Request::GetResult(_) => {
                    self.collecting.store(true, Ordering::SeqCst);
                    std::future::pending().await
                }
                _ => Err(LinkError::Unconfirmed),
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_collection_does_not_block_lease_renewals_and_shutdown_preserves_unknown() {
    let root = tempfile::Builder::new()
        .prefix("rtr-clocks-")
        .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
        .unwrap();
    let mut config = crate::config::resolve(&crate::config::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    config.heartbeat_seconds.value = 1;
    config.stale_seconds.value = 3;
    let state = root.path().join("parent");
    let mut resident = Resident::start(
        &config,
        Inputs {
            state_dir: state.clone(),
            worker: WorkerInputs {
                state_dir: root.path().join("worker"),
                worker_id: WorkerId::parse("local").unwrap(),
                boot_id: WorkerBootId::parse("boot-test").unwrap(),
                supervisor_exe: "/bin/false".into(),
                cluster_key_path: root.path().join("key"),
                env: BTreeMap::from([
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    ("HOME".into(), root.path().display().to_string()),
                ]),
            },
            collection_interval: Duration::from_millis(10),
            collection: CollectConfig {
                budget_ms: 5000,
                ..Default::default()
            },
        },
    )
    .await
    .unwrap();
    let db = resident.coordinator.store();
    let run = RunId::generate();
    let source = SourceId::generate();
    let worker = WorkerId::parse("local").unwrap();
    let digest = Digest::of_bytes(b"manifest");
    db.put_source_ref(&source, &digest, 1).unwrap();
    db.create_run(&run, Some(&source), None, &serde_json::json!({}), 1)
        .unwrap();
    let mut active = None;
    for terminal in [false, true] {
        let task = TaskId::generate();
        let attempt = AttemptId::generate();
        let payload = AttemptPayload {
            setup: None,
            run_id: run.clone(),
            task_id: task.clone(),
            generation: Generation(1),
            source_id: source.clone(),
            plan_digest: digest.clone(),
            manifest_digest: digest.clone(),
            permit_id: None,
            commands: vec![],
            lease_ms: 60000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: vec![],
        };
        db.create_task(&task, &run, &serde_json::json!({}), 1)
            .unwrap();
        db.create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task.clone(),
                worker_id: worker.clone(),
                payload_hash: Digest::of_bytes(b"payload"),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        let placement = db
            .record_placement(
                &task,
                &worker,
                Some(&attempt),
                &serde_json::json!({"payload":payload}),
                1,
            )
            .unwrap();
        db.ack_placement(placement, 1).unwrap();
        let state = if terminal {
            AttemptState::Terminal
        } else {
            AttemptState::Active
        };
        let termination =
            terminal.then(|| Termination::new(Outcome::Succeeded, None, None).unwrap());
        let mut receipts = Receipts::default();
        receipts.mark(Receipt::WorkerAccepted, 1);
        db.record_seen(
            &attempt,
            true,
            Some(Seen {
                state,
                termination,
                receipts,
            }),
            Observation::Known,
            1,
        )
        .unwrap();
        if !terminal {
            active = Some(AttemptStatus {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: attempt,
                run_id: run.clone(),
                task_id: task,
                generation: Generation(1),
                worker_boot_id: WorkerBootId::parse("boot-test").unwrap(),
                agent_epoch: AgentEpoch(1),
                state,
                receipts,
                spawned_at_ms: Some(1),
                observation: Observation::Known,
                termination: None,
                nodes: vec![],
                steps: vec![],
                retired: false,
                log_truncated: false,
                log_dropped_bytes: 0,
            });
        }
    }
    let slow = Arc::new(Slow {
        active: active.unwrap(),
        collecting: AtomicBool::new(false),
        offline: AtomicBool::new(false),
        renewals: AtomicUsize::new(0),
    });
    // Replace only this test's local protocol server with a controlled link.
    resident.worker_connection.abort();
    resident.worker_connection = tokio::spawn(std::future::pending());
    resident.coordinator.attach(worker, slow.clone());
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if slow.collecting.load(Ordering::SeqCst) && slow.renewals.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    slow.offline.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = CoordinatorStore::open_read_only(&state)
                .unwrap()
                .get_attempt(&slow.active.attempt_id)
                .unwrap()
                .unwrap();
            if record.observation == Observation::Unknown {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(7), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let record = CoordinatorStore::open_read_only(&state)
        .unwrap()
        .get_attempt(&slow.active.attempt_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.observation, Observation::Unknown);
    assert_eq!(record.state, AttemptState::Active);
    assert!(record.termination.is_none());
    assert!(record.cancel_requested_at_ms.is_none());
}
