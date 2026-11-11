//! The steps of the coordinator that cannot be reached from outside at the
//! moment they matter: what happens between a placement and its submit.

use super::link::LinkFuture;
use super::*;
use crate::ids::{Digest, NodeId};
use std::sync::atomic::AtomicUsize;

/// A worker that counts what it is sent and answers nothing.
#[derive(Default)]
struct Silent {
    calls: AtomicUsize,
}

impl WorkerLink for Silent {
    fn call(&self, _request: Request) -> LinkFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(LinkError::Unconfirmed) })
    }
}

fn spec() -> TaskSpec {
    TaskSpec {
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::of_bytes(b"manifest"),
        commands: vec![CommandSpec {
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
    }
}

struct Fx {
    _dir: tempfile::TempDir,
    c: Coordinator,
    worker: WorkerId,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
    Fx {
        _dir: dir,
        c,
        worker: WorkerId::parse("w-a").unwrap(),
    }
}

impl Fx {
    /// Queue one task and commit its placement, as `dispatch` does before it
    /// sends anything.
    async fn placed(&self) -> (TaskId, AttemptId) {
        let source = SourceId::generate();
        self.c
            .store()
            .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
            .unwrap();
        let task = TaskId::generate();
        self.c
            .enqueue(RunId::generate(), source, vec![(task.clone(), spec())])
            .await
            .unwrap();
        let attempt = AttemptId::generate();
        let outcome = self
            .c
            .store()
            .place(
                &task,
                &self.worker,
                &attempt,
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap();
        assert!(matches!(outcome, PlaceOutcome::Placed(_)), "{outcome:?}");
        (task, attempt)
    }

    async fn send(&self, link: &Silent, attempt: &AttemptId) -> Sent {
        let mut report = TickReport::default();
        self.c
            .send_placed(&self.worker, link, attempt, &mut report)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn a_placed_attempt_is_sent_as_it_was_committed() {
    let f = fx();
    let link = Silent::default();
    let (_task, attempt) = f.placed().await;
    // Sent; this worker gives no answer, so it did not take it.
    assert_eq!(f.send(&link, &attempt).await, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 1);
    // An attempt that is not there (any more) is not sent.
    assert_eq!(f.send(&link, &AttemptId::generate()).await, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_cancel_recorded_after_the_placement_holds_the_submit_back() {
    let f = fx();
    let link = Silent::default();
    let (task, attempt) = f.placed().await;
    f.c.cancel_task(&task).await.unwrap();
    assert_eq!(f.send(&link, &attempt).await, Sent::Withheld);
    assert_eq!(link.calls.load(Ordering::SeqCst), 0, "nothing was sent");
}

#[tokio::test]
async fn a_retire_recorded_after_the_placement_holds_the_submit_back() {
    let f = fx();
    let link = Silent::default();
    let (_task, attempt) = f.placed().await;
    f.c.request_retire(&attempt, "move").await.unwrap();
    assert_eq!(f.send(&link, &attempt).await, Sent::Withheld);
    assert_eq!(link.calls.load(Ordering::SeqCst), 0, "nothing was sent");
}

#[test]
fn a_spec_is_checked_with_the_construction_every_placement_uses() {
    let (run, source, task) = (RunId::generate(), SourceId::generate(), TaskId::generate());
    let good = serde_json::to_value(spec()).unwrap();
    check_spec(&run, &source, &task, &good, 60_000).unwrap();
    // Not a task spec at all, and one whose command is not a command.
    let mut broken = good.clone();
    broken["commands"][0]["node_id"] = json!("");
    for bad in [json!({"nope": 1}), json!(7), broken] {
        let e = check_spec(&run, &source, &task, &bad, 60_000).unwrap_err();
        assert!(matches!(e, CoordinatorError::Invalid(_)), "{e:?}");
        assert!(e.to_string().contains(&task.to_string()), "{e}");
    }
    // The demand is the payload itself, under the hash that is recorded.
    let input = PlaceInput {
        run_id: run.clone(),
        task_id: task.clone(),
        generation: Generation(3),
        source_id: Some(source.clone()),
        spec: good,
    };
    let (hash, demand) = demand_of(&input, 60_000).unwrap();
    let payload: AttemptPayload = serde_json::from_value(demand["payload"].clone()).unwrap();
    assert_eq!(payload.generation, Generation(3));
    assert_eq!(payload.lease_ms, 60_000);
    assert_eq!(payload_hash(&payload).unwrap(), hash);
    // A run without a source has no payload.
    let no_source = PlaceInput {
        source_id: None,
        ..input
    };
    assert!(matches!(
        demand_of(&no_source, 60_000),
        Err(StorageError::Invalid(_))
    ));
}

#[tokio::test]
async fn futures_are_joined_in_their_order_whichever_ends_first() {
    let slow = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        1u32
    }) as Pin<Box<dyn Future<Output = u32> + Send>>;
    let fast = Box::pin(async { 2u32 }) as Pin<Box<dyn Future<Output = u32> + Send>>;
    assert_eq!(join_all(vec![slow, fast]).await, vec![1, 2]);
    assert_eq!(join_all::<u32>(vec![]).await, Vec::<u32>::new());
}
