//! The scheduler inside the coordinator, against several in-memory workers
//! on one machine: A01 (nobody names a host, and the work spreads), A02 (a
//! worker that joins takes the backlog), A03 (a busy worker against a cold
//! one, with the reason), A04 (submits from several sides, one decision at
//! a time, un-acknowledged demand counted), and what the forecast and the
//! coordinator left to the scheduler: two `fallback` answers, the age of a
//! forecast, a worker that keeps refusing a task, a worker without a number.
//!
//! No process is started here.

use crate::coordinator::{attempts, enqueue, fx, only, queued, FakeWorker, Fx, Lost, Op, Says};
use remote_task_runner::coordinator::{Coordinator, CoordinatorError, WorkerLink};
use remote_task_runner::error::{ApiError, BlockedReason, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::scheduler::decide::{
    Costs, Decision, Exclusion, Label, Rule, Unusable, Why, COLLECTION, SETUP, TRANSFER,
};
use remote_task_runner::scheduler::{
    decision_seq_of, reason_of, CostModel, EVENT_TASK_WAITING, EVENT_WORKER_EXCLUDED,
};
use remote_task_runner::state::Outcome;
use remote_task_runner::storage::QueuedTask;
use remote_task_runner::wire::{RuntimeSource, Uncertainty};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

fn reason(c: &Coordinator, attempt: &AttemptId) -> Decision {
    reason_of(c.store(), attempt)
        .unwrap()
        .expect("the decision is recorded with the placement")
}

fn compared(d: &Decision) -> Vec<&str> {
    d.compared.iter().map(|c| c.worker_id.as_str()).collect()
}

fn excluded(d: &Decision) -> Vec<(&str, Exclusion)> {
    let all = d.excluded.iter();
    all.map(|e| (e.worker_id.as_str(), e.why)).collect()
}

fn worker_of(c: &Coordinator, task: &TaskId) -> String {
    only(c, task).worker_id.to_string()
}

/// The last attempt of a task.
fn last(c: &Coordinator, task: &TaskId) -> remote_task_runner::storage::AttemptRecord {
    attempts(c, task).pop().unwrap()
}

fn events(c: &Coordinator, kind: &str) -> Vec<serde_json::Value> {
    let all = c.store().events_after(0, 10_000).unwrap();
    let of_kind = all.into_iter().filter(|e| e.kind == kind);
    of_kind.map(|e| e.payload).collect()
}

/// Estimates per worker, fixed by the test.
#[derive(Default)]
struct Fixed(Mutex<BTreeMap<String, Costs>>);

impl Fixed {
    fn set(&self, worker: &str, costs: Costs) {
        self.0.lock().unwrap().insert(worker.to_string(), costs);
    }
}

impl CostModel for Fixed {
    fn costs(&self, worker_id: &WorkerId, _task: &QueuedTask) -> Costs {
        let all = self.0.lock().unwrap();
        all.get(worker_id.as_str()).copied().unwrap_or_default()
    }
}

// ------------------------------------------------------------------- A01

#[tokio::test]
async fn a01_tasks_given_to_no_host_run_on_the_parent_and_on_both_children() {
    let f = fx();
    // Each of them alone could hold all three tasks.
    let parent = f.worker("local", (2, 1));
    let child_a = f.worker("w-a", (2, 1));
    let child_b = f.worker("w-b", (2, 1));
    let c = f.open(&[&parent, &child_a, &child_b]);
    let tasks = enqueue(&c, 3).await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 3);
    let on: Vec<String> = tasks.iter().map(|t| worker_of(&c, t)).collect();
    assert_eq!(on, ["local", "w-a", "w-b"], "one each, the free one next");
    for (worker, task) in [&parent, &child_a, &child_b].iter().zip(&tasks) {
        let a = only(&c, task);
        worker.start(&a.attempt_id);
        worker.finish(&a.attempt_id, Outcome::Succeeded);
        assert_eq!(f.cluster.starts_of(task), vec![worker.id.to_string()]);
    }
    c.tick().await.unwrap();
    for task in &tasks {
        let a = only(&c, task);
        assert_eq!(a.termination.unwrap().outcome(), Outcome::Succeeded);
    }
    // Why each went where it went is on record.
    let first = reason(&c, &only(&c, &tasks[0]).attempt_id);
    assert_eq!(first.why, Why::TieBreak(Rule::WorkerId));
    assert_eq!(compared(&first), ["local", "w-a", "w-b"]);
    let second = reason(&c, &only(&c, &tasks[1]).attempt_id);
    // The parent holds one now: the other two are about the same.
    assert_eq!(compared(&second), ["w-a", "w-b", "local"]);
    assert_eq!(second.compared[2].compare_ms, Some(20_000));
    let third = reason(&c, &only(&c, &tasks[2]).attempt_id);
    assert_eq!(third.why, Why::Earliest);
    assert_eq!(third.compared[0].compare_ms, Some(10_000));
}

#[tokio::test]
async fn the_same_facts_give_the_same_placements() {
    let mut runs = Vec::new();
    for _ in 0..2 {
        let f = fx();
        let workers = [
            f.worker("w-c", (2, 1)),
            f.worker("w-a", (2, 1)),
            f.worker("w-b", (1, 0)),
        ];
        let c = f.open(&[&workers[0], &workers[1], &workers[2]]);
        let tasks = enqueue(&c, 8).await;
        c.tick().await.unwrap();
        let on: Vec<Option<String>> = tasks
            .iter()
            .map(|t| attempts(&c, t).pop().map(|a| a.worker_id.to_string()))
            .collect();
        runs.push(on);
    }
    assert_eq!(runs[0], runs[1]);
    let w = |name: &str| Some(name.to_string());
    assert_eq!(
        runs[0],
        [
            w("w-a"),
            w("w-b"),
            w("w-c"),
            w("w-a"),
            w("w-c"),
            w("w-a"),
            w("w-c"),
            None
        ]
    );
}

#[tokio::test]
async fn a_preferred_worker_is_taken_while_the_others_are_not_faster_by_the_correction() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    let c = f.open(&[&a, &b]);
    c.scheduler().set_priority(&b.id, 1);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    // 10 s, 20 s and 30 s on w-b against 10 s + 60 s on w-a.
    for t in &tasks {
        assert_eq!(worker_of(&c, t), "w-b");
    }
    let d = reason(&c, &only(&c, &tasks[2]).attempt_id);
    assert_eq!(compared(&d), ["w-b", "w-a"]);
    assert_eq!(
        (d.compared[0].penalty_ms, d.compared[0].compare_ms),
        (0, Some(30_000))
    );
    assert_eq!(d.compared[1].penalty_ms, 60_000);
    assert_eq!(d.compared[1].predicted_ms, Some(10_000));
    assert_eq!(d.compared[1].compare_ms, Some(70_000));
}

// ------------------------------------------------------------------- A02

#[tokio::test]
async fn a02_a_worker_that_joins_takes_the_backlog_the_parent_kept() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    assert_eq!(worker_of(&c, &tasks[0]), "w-a");
    assert_eq!(
        queued(&c),
        tasks[1..].to_vec(),
        "the rest stays with the parent"
    );
    // Why the next one waits can be shown, and is recorded once.
    let waits = c.scheduler().waiting_reason(&tasks[1]).unwrap();
    assert_eq!(waits.why, Why::NoCandidate);
    assert_eq!(excluded(&waits), [("w-a", Exclusion::Full)]);
    let recorded = events(&c, EVENT_TASK_WAITING);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0]["task"], tasks[1].to_string());
    assert_eq!(recorded[0]["decision"]["why"]["kind"], "no_candidate");

    let b = f.worker("w-b", (1, 1));
    c.attach(b.id.clone(), Arc::clone(&b) as Arc<dyn WorkerLink>);
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2);
    assert!(queued(&c).is_empty());
    for t in &tasks[1..] {
        assert_eq!(worker_of(&c, t), "w-b");
        let d = reason(&c, &only(&c, t).attempt_id);
        assert_eq!(compared(&d), ["w-b"]);
        assert_eq!(d.why, Why::OnlyCandidate);
        assert_eq!(excluded(&d), [("w-a", Exclusion::Full)]);
    }
    assert_eq!(b.holds().len(), 2);
    assert_eq!(c.scheduler().waiting_reason(&tasks[1]), None);
}

/// Attaches a worker in the middle of a tick: when the first decision asks
/// for its estimates.
struct JoinsMidway {
    coordinator: OnceLock<Arc<Coordinator>>,
    late: Arc<FakeWorker>,
    joined: Mutex<bool>,
}

impl CostModel for JoinsMidway {
    fn costs(&self, _worker_id: &WorkerId, _task: &QueuedTask) -> Costs {
        let mut joined = self.joined.lock().unwrap();
        if !*joined {
            *joined = true;
            let c = self.coordinator.get().unwrap();
            c.attach(
                self.late.id.clone(),
                Arc::clone(&self.late) as Arc<dyn WorkerLink>,
            );
        }
        Costs::default()
    }
}

#[tokio::test]
async fn a02_a_worker_that_joins_during_a_tick_is_counted_in_from_the_next_decision() {
    let mut f = fx();
    let a = f.worker("w-b", (1, 0));
    let late = f.worker("w-a", (1, 0));
    let model = Arc::new(JoinsMidway {
        coordinator: OnceLock::new(),
        late: Arc::clone(&late),
        joined: Mutex::new(false),
    });
    f.costs = Arc::clone(&model) as Arc<dyn CostModel>;
    let c = Arc::new(f.open(&[&a]));
    assert!(model.coordinator.set(Arc::clone(&c)).is_ok());
    let tasks = enqueue(&c, 2).await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2, "both in the one tick");
    // The first decision did not know it yet.
    let first = reason(&c, &only(&c, &tasks[0]).attempt_id);
    assert_eq!((compared(&first), excluded(&first)), (vec!["w-b"], vec![]));
    assert_eq!(worker_of(&c, &tasks[1]), "w-a");
    let second = reason(&c, &only(&c, &tasks[1]).attempt_id);
    assert_eq!(compared(&second), ["w-a"]);
    assert_eq!(excluded(&second), [("w-b", Exclusion::Full)]);
}

// ------------------------------------------------------------------- A03

/// A worker that runs two tasks (20 s of queue) and has everything
/// prepared, and an idle one that must be set up first.
async fn busy_and_cold(cold_setup_ms: u64) -> (Fx, Coordinator, Arc<FakeWorker>, Decision) {
    let mut f = fx();
    let model = Arc::new(Fixed::default());
    model.set(
        "w-busy",
        Costs {
            ms: [Some(1000), Some(0), Some(1000)],
            cache_warm: Some(true),
            transfer_bytes: Some(1 << 10),
            affinity: true,
            ..Costs::default()
        },
    );
    model.set(
        "w-cold",
        Costs {
            ms: [Some(5000), Some(cold_setup_ms), Some(1000)],
            cache_warm: Some(false),
            transfer_bytes: Some(1 << 30),
            ..Costs::default()
        },
    );
    f.costs = model as Arc<dyn CostModel>;
    let busy = f.worker("w-busy", (2, 1));
    let cold = f.worker("w-cold", (2, 1));
    let c = f.open(&[&busy]);
    for t in enqueue(&c, 2).await {
        c.tick().await.unwrap();
        busy.start(&only(&c, &t).attempt_id);
    }
    c.tick().await.unwrap();
    c.attach(cold.id.clone(), Arc::clone(&cold) as Arc<dyn WorkerLink>);
    let task = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    let d = reason(&c, &only(&c, &task).attempt_id);
    (f, c, cold, d)
}

#[tokio::test]
async fn a03_a_busy_worker_beats_a_cold_one_and_the_reason_shows_queue_setup_and_transfer() {
    let (_f, _c, cold, d) = busy_and_cold(45_000).await;
    assert_eq!(d.winner.as_ref().unwrap().as_str(), "w-busy");
    assert_eq!(d.why, Why::Earliest);
    assert_eq!(compared(&d), ["w-busy", "w-cold"]);
    let (busy, slow) = (&d.compared[0], &d.compared[1]);
    // Busy: ready after 1 s of transfer, which the 20 s of queue overlap;
    // then 10 s of run and 1 s of collection.
    assert_eq!(busy.costs.ms[TRANSFER], Some(1000));
    assert_eq!(busy.costs.ms[SETUP], Some(0));
    assert_eq!(busy.costs.ms[COLLECTION], Some(1000));
    assert_eq!(busy.outlook.ready_ms, Some(1000));
    assert_eq!(busy.outlook.wait_ms, Some(19_000));
    assert_eq!(busy.outlook.execution_ms, Some(10_000));
    assert_eq!(busy.outlook.finish_ms, Some(30_000));
    assert_eq!(busy.predicted_ms, Some(31_000));
    assert_eq!((busy.load, busy.costs.cache_warm), (2, Some(true)));
    assert_eq!(busy.label, Label::Measured);
    // Cold: nothing queued, but 5 s of transfer and 45 s of setup first.
    assert_eq!(slow.costs.ms, [Some(5000), Some(45_000), Some(1000)]);
    assert_eq!(slow.outlook.ready_ms, Some(50_000));
    assert_eq!(slow.outlook.wait_ms, Some(0));
    assert_eq!(slow.predicted_ms, Some(61_000));
    assert_eq!((slow.load, slow.costs.cache_warm), (0, Some(false)));
    assert_eq!(slow.costs.transfer_bytes, Some(1 << 30));
    assert_eq!(
        d.summary(),
        "w-busy: earliest predicted completion; compared: [w-busy=31000ms measured, \
         w-cold=61000ms measured]; excluded: []"
    );
    // The worker was asked with the time before the task is ready there.
    let asked = cold.state.lock().unwrap().asked.last().cloned().unwrap();
    assert_eq!(asked.workload.nodes[0].external_delay_ms, Some(50_000));
    assert!(asked.pending.is_empty());
}

#[tokio::test]
async fn a03_a_cold_worker_that_is_set_up_quickly_beats_the_queue() {
    let (_f, _c, _cold, d) = busy_and_cold(5000).await;
    assert_eq!(compared(&d), ["w-cold", "w-busy"]);
    assert_eq!(d.compared[0].predicted_ms, Some(21_000));
    assert_eq!(d.compared[1].predicted_ms, Some(31_000));
    assert_eq!(d.why, Why::Earliest);
}

// ------------------------------------------------------------------- A04

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a04_submits_from_many_sides_are_decided_one_at_a_time() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    let c = Arc::new(f.open(&[&a, &b]));
    let mut clients = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&c);
        clients.push(tokio::spawn(async move {
            let task = enqueue(&c, 1).await.remove(0);
            c.tick().await.unwrap();
            task
        }));
    }
    let mut tasks = Vec::new();
    for client in clients {
        tasks.push(client.await.unwrap());
    }
    c.tick().await.unwrap();
    // Six slots in all: six tasks are placed once each, two wait.
    let mut seqs = BTreeSet::new();
    let mut per_worker: BTreeMap<String, u32> = BTreeMap::new();
    let mut placed = 0;
    for task in &tasks {
        let all = attempts(&c, task);
        assert!(all.len() <= 1, "{all:?}");
        for attempt in all {
            placed += 1;
            *per_worker.entry(attempt.worker_id.to_string()).or_default() += 1;
            let seq = decision_seq_of(c.store(), &attempt.attempt_id).unwrap();
            assert!(seqs.insert(seq.unwrap()), "one decision, one placement");
        }
    }
    assert_eq!(placed, 6);
    assert_eq!(
        per_worker,
        BTreeMap::from([("w-a".to_string(), 3), ("w-b".to_string(), 3)])
    );
    assert_eq!(seqs, (1..=6).collect::<BTreeSet<u64>>());
    assert_eq!(queued(&c).len(), 2);
    assert_eq!(a.holds().len(), 3);
    assert_eq!(b.holds().len(), 3);
}

#[tokio::test]
async fn a04_a_placement_no_worker_acknowledged_is_counted_by_the_next_decision() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    let c = f.open(&[&a, &b]);
    // The submit to w-a does not arrive, twice.
    a.set(|s| s.lose = vec![(Op::Submit, Lost::Request), (Op::Submit, Lost::Request)]);
    let first = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    let on_a = only(&c, &first);
    assert_eq!(on_a.worker_id, a.id);
    assert!(a.holds().is_empty());
    assert_eq!(c.store().unacked_demand().unwrap().len(), 1);

    let second = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    // The question to w-a named what is on its way there.
    let asked = a.state.lock().unwrap().asked.last().cloned().unwrap();
    assert_eq!(asked.pending.len(), 1);
    assert_eq!(asked.pending[0].attempt_id, on_a.attempt_id);
    assert_eq!(asked.pending[0].nodes.len(), 1);
    let asked_b = b.state.lock().unwrap().asked.last().cloned().unwrap();
    assert!(asked_b.pending.is_empty());
    // So w-a is one task behind, and the second goes to w-b.
    assert_eq!(worker_of(&c, &second), "w-b");
    let d = reason(&c, &only(&c, &second).attempt_id);
    assert_eq!(compared(&d), ["w-b", "w-a"]);
    assert_eq!(d.compared[1].outlook.wait_ms, Some(10_000));
    assert_eq!(d.compared[1].compare_ms, Some(20_000));
    assert_eq!(d.compared[1].load, 1);
    // The first is still only on w-a, and arrives there in the end.
    c.tick().await.unwrap();
    assert_eq!(a.holds(), vec![on_a.attempt_id.clone()]);
    assert_eq!(attempts(&c, &first).len(), 1);
}

// ------------------------------------------- two workers that say `fallback`

#[tokio::test]
async fn two_workers_without_history_are_ordered_by_their_ranking() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    a.set(|s| {
        s.forecasting.says = Says::Number(RuntimeSource::Fallback);
        s.forecasting.run_ms = 30_000;
    });
    b.set(|s| s.forecasting.says = Says::Number(RuntimeSource::Fallback));
    let c = f.open(&[&a, &b]);
    let task = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    assert_eq!(worker_of(&c, &task), "w-b");
    let d = reason(&c, &only(&c, &task).attempt_id);
    assert_eq!(d.why, Why::Earliest);
    for (at, ms) in [(0, 10_000), (1, 30_000)] {
        let w = &d.compared[at];
        assert_eq!(w.label, Label::Fallback);
        // No time, only the figure to order by.
        assert_eq!(
            (w.outlook.finish_ms, w.outlook.ranking_ms),
            (None, Some(ms))
        );
        assert_eq!(w.outlook.null_reason, Some(Uncertainty::NoHistory));
    }
    assert!(d
        .summary()
        .contains("w-b=rank 10000 fallback, w-a=rank 30000 fallback"));
}

// ------------------------------------------------- the age of a forecast

#[tokio::test]
async fn a_forecast_that_is_old_or_from_a_worker_that_restarted_is_not_used() {
    type Tweak = fn(&mut crate::coordinator::Forecasting);
    let cases: [(Tweak, Unusable); 3] = [
        (|f| f.ttl_ms = 0, Unusable::Expired),
        (|f| f.boot = "boot-2", Unusable::BootChanged),
        (|f| f.epoch = 2, Unusable::EpochChanged),
    ];
    for (tweak, why) in cases {
        let f = fx();
        // By its number w-a would win: it is the faster one.
        let a = f.worker("w-a", (2, 1));
        let b = f.worker("w-b", (2, 1));
        a.set(|s| {
            s.forecasting.run_ms = 1000;
            tweak(&mut s.forecasting);
        });
        let c = f.open(&[&a, &b]);
        let task = enqueue(&c, 1).await.remove(0);
        c.tick().await.unwrap();
        assert_eq!(worker_of(&c, &task), "w-b", "{why:?}");
        let d = reason(&c, &only(&c, &task).attempt_id);
        assert_eq!(compared(&d), ["w-b", "w-a"]);
        let unused = &d.compared[1];
        assert_eq!(unused.outlook.unusable, Some(why));
        assert_eq!((unused.label, unused.compare_ms), (Label::Unknown, None));
    }
}

#[tokio::test]
async fn the_age_of_a_forecast_is_measured_by_the_parent_from_before_it_asked() {
    let mut f = fx();
    // Longer than the parent lets a forecast live.
    f.sched.rules.forecast_ttl_ms = 100;
    f.sched.forecast_wait_ms = 2000;
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    // Both answers say they are good for a minute. w-b takes 300 ms; the
    // decision waits for it, and by then the answers are too old for the
    // parent, also the one that came at once.
    for w in [&a, &b] {
        w.set(|s| s.forecasting.ttl_ms = 60_000);
    }
    b.set(|s| s.forecasting.delay_ms = 300);
    let c = f.open(&[&a, &b]);
    let task = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    let d = reason(&c, &only(&c, &task).attempt_id);
    for w in &d.compared {
        assert_eq!(
            w.outlook.unusable,
            Some(Unusable::Expired),
            "{}",
            w.worker_id
        );
        assert_eq!(w.label, Label::Unknown);
    }
    // Still placed: by the tie rules, on the first by id.
    assert_eq!(d.why, Why::TieBreak(Rule::WorkerId));
    assert_eq!(worker_of(&c, &task), "w-a");
}

// ------------------------------------- a worker that keeps refusing a task

fn refusal(code: ErrorCode) -> ApiError {
    let mut e = ApiError::new(code, format!("{code}"));
    if code == ErrorCode::Blocked {
        e.blocked_reason = Some(BlockedReason::EnvironmentMismatch);
    }
    e
}

#[tokio::test]
async fn a_task_its_worker_keeps_refusing_is_retired_there_and_placed_elsewhere() {
    for code in [ErrorCode::Blocked, ErrorCode::ProtocolIncompatible] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a, &b]);
        a.set(|s| s.fault = Some(refusal(code)));
        let task = enqueue(&c, 1).await.remove(0);
        // Placed on w-a (the first by id), which refuses it.
        c.tick().await.unwrap();
        let first = only(&c, &task);
        assert_eq!(first.worker_id, a.id);
        assert_eq!(c.store().retire_tombstone(&first.attempt_id).unwrap(), None);
        // Refused again: the retire is asked for, and nothing moves yet.
        let report = c.tick().await.unwrap();
        assert!(report.retired.is_empty() && report.placed.is_empty());
        assert!(c
            .store()
            .retire_tombstone(&first.attempt_id)
            .unwrap()
            .is_some());
        assert_eq!(a.count(Op::Retire), 0);
        assert!(b.holds().is_empty(), "not before the retire is confirmed");
        let marked = c
            .store()
            .events_of(&first.attempt_id, EVENT_WORKER_EXCLUDED)
            .unwrap();
        assert_eq!(marked.len(), 1);
        assert_eq!(marked[0].payload["worker"], "w-a");
        assert_eq!(marked[0].payload["code"], code.to_string());
        // Confirmed: the task goes to the other worker, in the same tick.
        let report = c.tick().await.unwrap();
        assert_eq!(report.retired, vec![first.attempt_id.clone()]);
        assert_eq!(a.count(Op::Retire), 1);
        let all = attempts(&c, &task);
        assert_eq!(all.len(), 2, "{code:?}");
        assert!(
            all[0].retired && all[0].termination.is_none(),
            "it did not fail"
        );
        assert_eq!(all[1].worker_id, b.id);
        assert_eq!(b.holds(), vec![all[1].attempt_id.clone()]);
        let d = reason(&c, &all[1].attempt_id);
        assert_eq!(compared(&d), ["w-b"]);
        assert_eq!(excluded(&d), [("w-a", Exclusion::RefusedTask)]);
        // Another task is still offered to w-a.
        a.set(|s| s.fault = None);
        let other = enqueue(&c, 1).await.remove(0);
        c.tick().await.unwrap();
        assert_eq!(worker_of(&c, &other), "w-a");
    }
}

#[tokio::test]
async fn the_workers_that_refused_a_task_stay_excluded_across_a_restart() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let last_one = f.worker("w-c", (1, 0));
    for w in [&a, &b] {
        w.set(|s| s.fault = Some(refusal(ErrorCode::Blocked)));
    }
    let c = f.open(&[&a, &b, &last_one]);
    let task = enqueue(&c, 1).await.remove(0);
    // w-a: refused, refused again, retired; then w-b the same way.
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    assert_eq!(last(&c, &task).worker_id, b.id);
    c.tick().await.unwrap();
    assert!(c
        .store()
        .retire_tombstone(&last(&c, &task).attempt_id)
        .unwrap()
        .is_some());
    // The parent stops before the retire on w-b is confirmed.
    drop(c);
    let c = f.open(&[&a, &b, &last_one]);
    c.tick().await.unwrap();
    let all = attempts(&c, &task);
    assert_eq!(all.len(), 3);
    assert_eq!(all[2].worker_id, last_one.id);
    assert_eq!(last_one.holds(), vec![all[2].attempt_id.clone()]);
    let d = reason(&c, &all[2].attempt_id);
    assert_eq!(
        excluded(&d),
        [
            ("w-a", Exclusion::RefusedTask),
            ("w-b", Exclusion::RefusedTask)
        ]
    );
    assert_eq!(f.cluster.starts_of(&task), Vec::<String>::new());
}

#[tokio::test]
async fn a_task_every_worker_refused_waits_and_says_why() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    a.set(|s| s.fault = Some(refusal(ErrorCode::Blocked)));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 2).await;
    for _ in 0..6 {
        c.tick().await.unwrap();
    }
    // The first was taken off w-a and has nowhere to go. It did not fail.
    let all = attempts(&c, &tasks[0]);
    assert_eq!(all.len(), 1);
    assert!(all[0].retired && all[0].termination.is_none());
    let waits = c.scheduler().waiting_reason(&tasks[0]).unwrap();
    assert_eq!(waits.why, Why::NoCandidate);
    assert!(excluded(&waits).contains(&("w-a", Exclusion::RefusedTask)));
    // Once nothing changes any more, the reason is not recorded again.
    let recorded = events(&c, EVENT_TASK_WAITING).len();
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    assert_eq!(events(&c, EVENT_TASK_WAITING).len(), recorded);
    // It does not keep the one behind it from being tried.
    assert_eq!(attempts(&c, &tasks[1])[0].worker_id, a.id);
}

// ------------------------------------------------ a worker without a number

#[tokio::test]
async fn a_worker_that_gives_no_number_is_unknown_and_not_taken_for_free() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    let d_worker = f.worker("w-d", (2, 1));
    // w-a cannot say when it would be done; w-b can, and it is far off.
    a.set(|s| s.forecasting.says = Says::Nothing);
    b.set(|s| s.forecasting.run_ms = 3_600_000);
    d_worker.set(|s| s.draining = true);
    let c = f.open(&[&a, &b, &d_worker]);
    let task = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    assert_eq!(worker_of(&c, &task), "w-b");
    let d = reason(&c, &only(&c, &task).attempt_id);
    assert_eq!(compared(&d), ["w-b", "w-a"]);
    let unknown = &d.compared[1];
    assert_eq!(unknown.label, Label::Unknown);
    assert_eq!(unknown.outlook.null_reason, Some(Uncertainty::Overrun));
    assert_eq!((unknown.predicted_ms, unknown.compare_ms), (None, None));
    assert_eq!(excluded(&d), [("w-d", Exclusion::Draining)]);
    assert_eq!(
        d.summary(),
        "w-b: earliest predicted completion; compared: [w-b=3600000ms measured, \
         w-a=unknown (not counted as free)]; excluded: [w-d draining]"
    );
}

#[tokio::test]
async fn when_no_worker_gives_a_number_the_tasks_are_still_placed_by_what_the_parent_holds() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    a.set(|s| s.forecasting.says = Says::Nothing);
    // An error instead of an answer says as little.
    b.set(|s| s.forecasting.says = Says::Error);
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    let on: Vec<String> = tasks.iter().map(|t| worker_of(&c, t)).collect();
    assert_eq!(on, ["w-a", "w-b", "w-a"]);
    let d = reason(&c, &only(&c, &tasks[1]).attempt_id);
    assert_eq!(d.why, Why::TieBreak(Rule::Load));
    assert_eq!(compared(&d), ["w-b", "w-a"]);
    assert_eq!(d.compared[0].outlook.unusable, Some(Unusable::Refused));
    assert!(d.compared.iter().all(|w| w.label == Label::Unknown));
}

// ------------------------------------------- slow and silent workers

#[tokio::test]
async fn a_worker_that_does_not_answer_the_forecast_is_waited_for_once_per_tick() {
    let mut f = fx();
    f.sched.forecast_wait_ms = 300;
    let silent = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    silent.set(|s| s.forecasting.says = Says::Silence);
    let c = f.open(&[&silent, &b]);
    let tasks = enqueue(&c, 3).await;
    let started = Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(20), c.tick())
        .await
        .expect("the tick ends although a worker never answers")
        .unwrap();
    // One wait, not one per task, and far below the worker budget (10 s).
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(report.placed.len(), 3);
    assert_eq!(silent.count(Op::Forecast), 0, "it never answered");
    assert_eq!(b.count(Op::Forecast), 3);
    for t in &tasks {
        assert_eq!(worker_of(&c, t), "w-b");
        let d = reason(&c, &only(&c, t).attempt_id);
        assert_eq!(compared(&d), ["w-b"]);
        assert_eq!(excluded(&d), [("w-a", Exclusion::NoForecast)]);
    }
    // It is asked again by the next tick, and taken when it answers.
    silent.set(|s| s.forecasting.says = Says::Number(RuntimeSource::Measured));
    let more = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    assert_eq!(worker_of(&c, &more), "w-a");
}

#[tokio::test]
async fn a_worker_that_stalls_altogether_does_not_keep_tasks_from_the_others() {
    let mut f = fx();
    f.cfg.worker_budget_ms = 1000;
    let stalled = f.worker("w-a", (2, 1));
    let b = f.worker("w-b", (2, 1));
    let c = f.open(&[&stalled, &b]);
    // It holds one attempt, and then answers nothing but `inspect`.
    let held = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    assert_eq!(worker_of(&c, &held), "w-a");
    stalled.set(|s| s.stall = true);
    let tasks = enqueue(&c, 2).await;
    let started = Instant::now();
    c.tick().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    for t in &tasks {
        assert_eq!(worker_of(&c, t), "w-b");
        let d = reason(&c, &only(&c, t).attempt_id);
        assert_eq!(excluded(&d), [("w-a", Exclusion::Unreached)]);
    }
}

#[tokio::test]
async fn a_forecast_answered_with_a_newer_coordinators_epoch_stops_this_one() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    a.set(|s| s.max_epoch = u64::MAX);
    let task = enqueue(&c, 1).await.remove(0);
    let end = c.tick().await;
    assert!(
        matches!(end, Err(CoordinatorError::Fenced { .. })),
        "{end:?}"
    );
    assert!(c.is_fenced());
    assert!(attempts(&c, &task).is_empty(), "nothing was placed");
}
