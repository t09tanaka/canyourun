//! The durable queue of the coordinator: runs and tasks waiting for a
//! worker, the commit that places one, and the mirror of what a worker said.
//!
//! A task is queued when it may be placed: it was not cancelled, its run is
//! open, and every attempt it has (if any) is confirmed retired. A terminal
//! attempt ends the task; an attempt that is live, unknown, reconciling or
//! being retired keeps it where it is.

use super::{
    attempt_from_row, id, load_attempt, not_found, observation_str, run_of, AttemptRecord,
    CoordinatorStore, Placement, ATTEMPT, ATTEMPT_COLS,
};
use crate::ids::{AttemptId, Generation, PayloadHash, RunId, SourceId, TaskId, WorkerId};
use crate::state::{AttemptState, Observation, Outcome, Receipt, Receipts, Termination};
use crate::storage::common::{self, from_json, json};
use crate::storage::policy::state_path;
use crate::storage::{to_i64, to_u64, Result, StorageError};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::collections::BTreeSet;

/// The most tasks one `enqueue` accepts.
pub const MAX_TASKS_PER_RUN: usize = 10_000;

const QUEUED: &str = "t.cancel_requested_at_ms IS NULL
    AND EXISTS (SELECT 1 FROM run r WHERE r.run_id = t.run_id AND r.closed_at_ms IS NULL)
    AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id = t.task_id AND a.retired = 0)";

/// [`QUEUED`] for a step-1 file, which has no cancel of a task.
const QUEUED_V1: &str =
    "EXISTS (SELECT 1 FROM run r WHERE r.run_id = t.run_id AND r.closed_at_ms IS NULL)
    AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id = t.task_id AND a.retired = 0)";

/// The attempts (`a`) that leave their worker out for their task, given the
/// reason of their tombstone: the ones whose retire was confirmed. The scan
/// of the queue and the placement of one task read the same condition.
const EXCLUDES: &str = "a.retired = 1";

/// A task that may be placed now.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedTask {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub spec: Value,
    pub created_at_ms: u64,
}

/// The attempts of one worker that hold a slot: not terminal and not
/// confirmed retired. `active` may be running a command (`starting` onward);
/// `pending` were placed and have not started, acknowledged or not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerLoad {
    pub active: u32,
    pub pending: u32,
}

/// What the payload of a new attempt is built from, inside the placing
/// transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceInput {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub generation: Generation,
    pub source_id: Option<SourceId>,
    pub spec: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlaceOutcome {
    /// The attempt and its un-acknowledged demand are committed.
    Placed(Box<AttemptRecord>),
    /// The task may not be placed (any more): cancelled, closed, or it has
    /// an attempt that is not confirmed retired.
    NotQueued,
    /// The worker has no room; nothing was written.
    WorkerFull(WorkerLoad),
    /// No payload can be made from what the task holds (the reason);
    /// nothing was written.
    Unbuildable(String),
}

/// What a confirmed retire did to its attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum RetireEnd {
    /// The attempt is retired and its task may be placed again.
    Freed,
    /// The task was not to run again: the attempt is terminal with the
    /// result the parent fixed, and stays the task's last.
    Settled(Box<AttemptRecord>),
}

/// An attempt that still needs the coordinator's attention, with the facts
/// the next step depends on.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveAttempt {
    pub record: AttemptRecord,
    /// The demand recorded when it was placed (the payload to send).
    pub placement: Option<Placement>,
    /// Why a retire was asked for, while it is not confirmed.
    pub retire_reason: Option<String>,
}

/// What a worker reported about an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub state: AttemptState,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
}

fn outcome_word(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Cancelled => "cancelled",
        _ => "failed",
    }
}

fn load_of(conn: &Connection, worker: &str) -> Result<WorkerLoad> {
    let mut s = conn.prepare(
        "SELECT state FROM attempt WHERE worker_id = ?1 AND state <> 'terminal' AND retired = 0",
    )?;
    let rows = s.query_map([worker], |r| r.get::<_, String>(0))?;
    let mut load = WorkerLoad::default();
    for row in rows {
        match common::parse_state(&row?)? {
            AttemptState::Accepted | AttemptState::Preparing | AttemptState::Ready => {
                load.pending += 1
            }
            _ => load.active += 1,
        }
    }
    Ok(load)
}

fn placement_of(conn: &Connection, attempt_id: &str) -> Result<Option<Placement>> {
    let row = conn
        .query_row(
            "SELECT placement_id, task_id, worker_id, demand_json, created_at_ms, acked_at_ms
             FROM placement WHERE attempt_id = ?1 ORDER BY placement_id LIMIT 1",
            [attempt_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((pid, task, worker, demand, created, acked)) = row else {
        return Ok(None);
    };
    Ok(Some(Placement {
        placement_id: to_u64(pid)?,
        task_id: id("task_id", TaskId::parse(&task))?,
        worker_id: id("worker_id", WorkerId::parse(&worker))?,
        attempt_id: Some(id("attempt_id", AttemptId::parse(attempt_id))?),
        demand: from_json("demand", &demand)?,
        created_at_ms: to_u64(created)?,
        acked_at_ms: acked.map(to_u64).transpose()?,
    }))
}

impl CoordinatorStore {
    /// Accept a run and all its tasks in one commit (`coordinator accepted`):
    /// either the whole run is queued or none of it.
    pub fn enqueue(
        &self,
        run_id: &RunId,
        source_id: Option<&SourceId>,
        run_spec: &Value,
        tasks: &[(TaskId, Value)],
        at_ms: u64,
    ) -> Result<()> {
        if tasks.is_empty() || tasks.len() > MAX_TASKS_PER_RUN {
            return Err(StorageError::Invalid(format!(
                "a run has 1 to {MAX_TASKS_PER_RUN} tasks, not {}",
                tasks.len()
            )));
        }
        let run = run_id.to_string();
        let source = source_id.map(ToString::to_string);
        let run_spec = run_spec.to_string();
        let tasks: Vec<(String, String)> = tasks
            .iter()
            .map(|(t, spec)| (t.to_string(), spec.to_string()))
            .collect();
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO run (run_id, source_id, spec_json, created_at_ms) VALUES (?1,?2,?3,?4)",
                params![run, source, run_spec, to_i64(at_ms)?],
            )?;
            for (task, spec) in &tasks {
                tx.execute(
                    "INSERT INTO task (task_id, run_id, spec_json, created_at_ms)
                     VALUES (?1,?2,?3,?4)",
                    params![task, run, spec, to_i64(at_ms)?],
                )?;
            }
            common::append_event(
                tx,
                Some(&run),
                None,
                "run_created",
                &serde_json::json!({"tasks": tasks.len()}),
                at_ms,
            )?;
            Ok(())
        })
    }

    /// The tasks that may be placed, oldest first (insertion order within
    /// the same millisecond).
    pub fn queued_tasks(&self, limit: u32) -> Result<Vec<QueuedTask>> {
        self.queued_tasks_for(limit, &[], "")
    }

    /// [`Self::queued_tasks`] for a set of workers: the tasks that every one
    /// of `workers` is excluded for (see [`Self::excluded_workers`]) come
    /// after all the others, so that tasks nobody here can take never fill
    /// the scan in front of one that can be placed. With no workers the
    /// order is by age alone.
    pub fn queued_tasks_for(
        &self,
        limit: u32,
        workers: &[WorkerId],
        retire_reason: &str,
    ) -> Result<Vec<QueuedTask>> {
        use rusqlite::types::Value as Sql;
        let queued = if self.task_cancel { QUEUED } else { QUEUED_V1 };
        let mut args = vec![Sql::Integer(i64::from(limit))];
        let stranded = if workers.is_empty() {
            String::new()
        } else {
            args.push(Sql::Text(retire_reason.to_string()));
            args.push(Sql::Integer(to_i64(workers.len() as u64)?));
            args.extend(workers.iter().map(|w| Sql::Text(w.to_string())));
            let marks: Vec<String> = (0..workers.len()).map(|i| format!("?{}", i + 4)).collect();
            let marks = marks.join(",");
            format!(
                "(SELECT COUNT(DISTINCT a.worker_id) FROM attempt a
                  JOIN retire_tombstone rt ON rt.attempt_id = a.attempt_id
                  WHERE a.task_id = t.task_id AND {EXCLUDES}
                    AND rt.reason = ?2 AND a.worker_id IN ({marks})) >= ?3, "
            )
        };
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT t.task_id, t.run_id, t.spec_json, t.created_at_ms FROM task t
                 WHERE {queued} ORDER BY {stranded}t.created_at_ms, t.rowid LIMIT ?1"
            ))?;
            let rows = s.query_map(rusqlite::params_from_iter(args), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (task, run, spec, created) = row?;
                out.push(QueuedTask {
                    task_id: id("task_id", TaskId::parse(&task))?,
                    run_id: id("run_id", RunId::parse(&run))?,
                    spec: from_json("task spec", &spec)?,
                    created_at_ms: to_u64(created)?,
                });
            }
            Ok(out)
        })
    }

    pub fn worker_load(&self, worker_id: &WorkerId) -> Result<WorkerLoad> {
        let w = worker_id.to_string();
        self.db.call(move |c| load_of(c, &w))
    }

    /// Place a queued task on a worker: one commit holds the decision (the
    /// new attempt, `accepted`, with the next generation) and the demand it
    /// puts on the worker, which stays un-acknowledged until the worker's
    /// acceptance is recorded. Nothing is sent from here; the caller sends
    /// after this returns.
    ///
    /// Inside the transaction the task is checked to be queued still, the
    /// worker's load is counted again and given to `room`, and `build` makes
    /// the payload hash and the demand from the generation just assigned.
    pub fn place<R, B>(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        attempt_id: &AttemptId,
        room: R,
        build: B,
        at_ms: u64,
    ) -> Result<PlaceOutcome>
    where
        R: FnOnce(WorkerLoad) -> bool + Send + 'static,
        B: FnOnce(&PlaceInput) -> Result<(PayloadHash, Value)> + Send + 'static,
    {
        let epoch = self.epoch();
        let (task, worker, aid) = (
            task_id.to_string(),
            worker_id.to_string(),
            attempt_id.to_string(),
        );
        let task_id = task_id.clone();
        self.db.write(move |tx| {
            let row = tx
                .query_row(
                    &format!(
                        "SELECT t.run_id, t.spec_json,
                                (SELECT r.source_id FROM run r WHERE r.run_id = t.run_id)
                         FROM task t WHERE t.task_id = ?1 AND {QUEUED}"
                    ),
                    [&task],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()?;
            let Some((run, spec, source)) = row else {
                return Ok(PlaceOutcome::NotQueued);
            };
            let load = load_of(tx, &worker)?;
            if !room(load) {
                return Ok(PlaceOutcome::WorkerFull(load));
            }
            let generation: i64 = tx.query_row(
                "SELECT COALESCE(MAX(generation), 0) + 1 FROM attempt WHERE task_id = ?1",
                [&task],
                |r| r.get(0),
            )?;
            let input = PlaceInput {
                run_id: id("run_id", RunId::parse(&run))?,
                task_id,
                generation: Generation(to_u64(generation)?),
                source_id: source
                    .map(|s| id("source_id", SourceId::parse(&s)))
                    .transpose()?,
                spec: from_json("task spec", &spec)?,
            };
            let (hash, demand) = match build(&input) {
                Ok(built) => built,
                // A verdict on this task only; a fault still fails the call.
                Err(StorageError::Invalid(why)) => return Ok(PlaceOutcome::Unbuildable(why)),
                Err(e) => return Err(e),
            };
            let mut receipts = Receipts::default();
            receipts.mark(Receipt::CoordinatorAccepted, at_ms);
            tx.execute(
                "INSERT INTO attempt (attempt_id, task_id, worker_id, generation, payload_hash,
                    coordinator_epoch, state, receipts_json, created_at_ms)
                 VALUES (?1,?2,?3,?4,?5,?6,'accepted',?7,?8)",
                params![
                    aid,
                    task,
                    worker,
                    generation,
                    hash.as_str(),
                    to_i64(epoch.0)?,
                    json(&receipts),
                    to_i64(at_ms)?
                ],
            )?;
            tx.execute(
                "INSERT INTO placement (task_id, worker_id, attempt_id, demand_json, created_at_ms)
                 VALUES (?1,?2,?3,?4,?5)",
                params![task, worker, aid, demand.to_string(), to_i64(at_ms)?],
            )?;
            common::append_event(
                tx,
                Some(&run),
                Some(&aid),
                "attempt_placed",
                &serde_json::json!({"worker": worker, "generation": generation}),
                at_ms,
            )?;
            let rec = load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
            Ok(PlaceOutcome::Placed(Box::new(rec)))
        })
    }

    /// The attempts that are not terminal and not confirmed retired (of one
    /// worker, or of all), oldest first.
    pub fn live_attempts(&self, worker_id: Option<&WorkerId>) -> Result<Vec<LiveAttempt>> {
        let worker = worker_id.map(ToString::to_string);
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT {ATTEMPT_COLS} FROM attempt
                 WHERE state <> 'terminal' AND retired = 0 AND (?1 IS NULL OR worker_id = ?1)
                 ORDER BY created_at_ms, rowid"
            ))?;
            let records: Vec<AttemptRecord> = s
                .query_map([&worker], |r| Ok(attempt_from_row(r)))?
                .map(|r| r?)
                .collect::<Result<_>>()?;
            let mut out = Vec::with_capacity(records.len());
            for record in records {
                let aid = record.attempt_id.to_string();
                let retire_reason: Option<String> = c
                    .query_row(
                        "SELECT reason FROM retire_tombstone
                         WHERE attempt_id = ?1 AND confirmed_at_ms IS NULL",
                        [&aid],
                        |r| r.get(0),
                    )
                    .optional()?;
                out.push(LiveAttempt {
                    placement: placement_of(c, &aid)?,
                    retire_reason,
                    record,
                });
            }
            Ok(out)
        })
    }

    /// Record what is known about an attempt after talking (or failing to
    /// talk) to its worker, in one commit:
    ///
    /// * `accepted_by_worker`: the worker holds the attempt, so its demand
    ///   is acknowledged and the `worker accepted` receipt is marked;
    /// * `seen`: the worker's state, followed edge by edge from the mirrored
    ///   one (a state behind the mirror changes nothing), its receipts, and
    ///   the termination when it is terminal, fixed once;
    /// * `observation`: how well the coordinator knows the attempt now.
    ///
    /// An attempt that is terminal here keeps its result whatever is seen.
    pub fn record_seen(
        &self,
        attempt_id: &AttemptId,
        accepted_by_worker: bool,
        seen: Option<Seen>,
        observation: Observation,
        at_ms: u64,
    ) -> Result<AttemptRecord> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = run_of(tx, &aid)?;
            let before =
                load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
            if accepted_by_worker {
                tx.execute(
                    "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?1)
                     WHERE attempt_id = ?2",
                    params![to_i64(at_ms)?, aid],
                )?;
                common::mark_receipt(tx, ATTEMPT, &aid, Receipt::WorkerAccepted, at_ms)?;
            }
            if let Some(seen) = seen {
                crate::state::check_termination(seen.state, seen.termination.as_ref())
                    .map_err(|e| StorageError::Invalid(e.to_string()))?;
                for receipt in [Receipt::CommandRegistered, Receipt::Running] {
                    if seen.receipts.reached(receipt) {
                        common::mark_receipt(tx, ATTEMPT, &aid, receipt, at_ms)?;
                    }
                }
                for step in state_path(before.state, seen.state).unwrap_or_default() {
                    match (step, seen.termination) {
                        (AttemptState::Terminal, Some(t)) => {
                            common::finalize(tx, ATTEMPT, &aid, t, at_ms, run.as_deref())?;
                        }
                        _ => {
                            common::transition(tx, ATTEMPT, &aid, step, at_ms, run.as_deref())?;
                        }
                    }
                }
            }
            // An attempt that ended puts no demand on its worker any more,
            // acknowledged or not.
            tx.execute(
                "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?1)
                 WHERE attempt_id = ?2 AND EXISTS
                    (SELECT 1 FROM attempt a WHERE a.attempt_id = ?2 AND a.state = 'terminal')",
                params![to_i64(at_ms)?, aid],
            )?;
            if before.observation != observation {
                tx.execute(
                    "UPDATE attempt SET observation = ?1 WHERE attempt_id = ?2",
                    params![observation_str(observation), aid],
                )?;
                common::append_event(
                    tx,
                    run.as_deref(),
                    Some(&aid),
                    "attempt_observation",
                    &serde_json::json!({"observation": observation_str(observation)}),
                    at_ms,
                )?;
            }
            load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))
        })
    }

    /// Persist the cancel of a task before anything is sent: the task is
    /// never placed again, and its live attempt (if any) carries the request
    /// to its worker. Returns that attempt. Idempotent.
    pub fn cancel_task(&self, task_id: &TaskId, at_ms: u64) -> Result<Option<AttemptRecord>> {
        let task = task_id.to_string();
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE task SET cancel_requested_at_ms = COALESCE(cancel_requested_at_ms, ?1)
                 WHERE task_id = ?2",
                params![to_i64(at_ms)?, task],
            )?;
            if n != 1 {
                return Err(not_found(format!("task {task}")));
            }
            let live: Option<String> = tx
                .query_row(
                    "SELECT attempt_id FROM attempt
                     WHERE task_id = ?1 AND state <> 'terminal' AND retired = 0",
                    [&task],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(aid) = live else { return Ok(None) };
            let fresh = tx.execute(
                "UPDATE attempt SET cancel_requested_at_ms = ?1
                 WHERE attempt_id = ?2 AND cancel_requested_at_ms IS NULL",
                params![to_i64(at_ms)?, aid],
            )?;
            if fresh == 1 {
                let run = run_of(tx, &aid)?;
                common::append_event(
                    tx,
                    run.as_deref(),
                    Some(&aid),
                    "cancel_requested",
                    &Value::Null,
                    at_ms,
                )?;
            }
            load_attempt(tx, &aid)
        })
    }

    /// The retire of an attempt was confirmed by its worker (it never
    /// started there, or it stopped without completing). One commit decides
    /// what that means from what is recorded now, not from what the caller
    /// read before it asked the worker:
    ///
    /// * the task was cancelled: the attempt is settled `cancelled`;
    /// * the retire was asked for with `failed_reason` (the worker will
    ///   never take the attempt): it is settled `failed`;
    /// * otherwise the attempt is retired and the task is free again.
    ///
    /// A settled attempt keeps `retired = 0`, so its task is not placed
    /// again; the tombstone is confirmed in every case.
    pub fn finish_retire(
        &self,
        attempt_id: &AttemptId,
        failed_reason: &str,
        at_ms: u64,
    ) -> Result<RetireEnd> {
        let (aid, failed_reason) = (attempt_id.to_string(), failed_reason.to_string());
        self.db.write(move |tx| {
            let reason: String = tx
                .query_row(
                    "SELECT reason FROM retire_tombstone WHERE attempt_id = ?1",
                    [&aid],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or_else(|| not_found(format!("retire tombstone for {aid}")))?;
            let cancelled: bool = tx.query_row(
                "SELECT a.cancel_requested_at_ms IS NOT NULL OR t.cancel_requested_at_ms IS NOT NULL
                 FROM attempt a JOIN task t USING (task_id) WHERE a.attempt_id = ?1",
                [&aid],
                |r| r.get(0),
            )?;
            let outcome = if cancelled {
                Outcome::Cancelled
            } else if reason == failed_reason {
                Outcome::Failed
            } else {
                super::free_by_retire(tx, &aid, at_ms)?;
                return Ok(RetireEnd::Freed);
            };
            let before =
                load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
            if before.state == AttemptState::Terminal || before.retired {
                return Err(StorageError::IllegalState {
                    attempt: aid,
                    state: before.state,
                    why: "its retire was already concluded",
                });
            }
            super::confirm_tombstone(tx, &aid, at_ms)?;
            let run = run_of(tx, &aid)?;
            let termination = Termination::new(outcome, None, None)
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
            for step in state_path(before.state, AttemptState::Terminal).unwrap_or_default() {
                if step == AttemptState::Terminal {
                    common::finalize(tx, ATTEMPT, &aid, termination, at_ms, run.as_deref())?;
                } else {
                    common::transition(tx, ATTEMPT, &aid, step, at_ms, run.as_deref())?;
                }
            }
            tx.execute(
                "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?1)
                 WHERE attempt_id = ?2",
                params![to_i64(at_ms)?, aid],
            )?;
            tx.execute(
                "UPDATE attempt SET observation = 'known' WHERE attempt_id = ?1",
                [&aid],
            )?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&aid),
                "retire_confirmed",
                &serde_json::json!({"settled": outcome_word(outcome)}),
                at_ms,
            )?;
            let rec = load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
            Ok(RetireEnd::Settled(Box::new(rec)))
        })
    }

    /// When the cancel of a task was requested, if it was.
    pub fn task_cancel_requested(&self, task_id: &TaskId) -> Result<Option<u64>> {
        let task = task_id.to_string();
        // A step-1 file has no such request.
        let column = if self.task_cancel {
            "cancel_requested_at_ms"
        } else {
            "NULL"
        };
        self.db.call(move |c| {
            c.query_row(
                &format!("SELECT {column} FROM task WHERE task_id = ?1"),
                [&task],
                |r| r.get::<_, Option<i64>>(0),
            )
            .optional()?
            .ok_or_else(|| not_found(format!("task {task}")))?
            .map(to_u64)
            .transpose()
        })
    }

    /// The workers a task is not placed on again: those an attempt of it was
    /// taken off (its retire confirmed) by a retire asked for with
    /// `retire_reason`. The reason of the tombstone decides, nothing else: a
    /// retire for another reason excludes nobody.
    pub fn excluded_workers(
        &self,
        task_id: &TaskId,
        retire_reason: &str,
    ) -> Result<BTreeSet<WorkerId>> {
        let (task, reason) = (task_id.to_string(), retire_reason.to_string());
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT DISTINCT a.worker_id FROM attempt a
                 JOIN retire_tombstone rt ON rt.attempt_id = a.attempt_id
                 WHERE a.task_id = ?1 AND {EXCLUDES} AND rt.reason = ?2"
            ))?;
            let rows = s.query_map([&task, &reason], |r| r.get::<_, String>(0))?;
            let mut out = BTreeSet::new();
            for row in rows {
                out.insert(id("worker_id", WorkerId::parse(&row?))?);
            }
            Ok(out)
        })
    }

    /// Every attempt of a task, by generation.
    pub fn attempts_of_task(&self, task_id: &TaskId) -> Result<Vec<AttemptRecord>> {
        let task = task_id.to_string();
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT {ATTEMPT_COLS} FROM attempt WHERE task_id = ?1 ORDER BY generation"
            ))?;
            let rows = s.query_map([&task], |r| Ok(attempt_from_row(r)))?;
            rows.map(|r| r?).collect()
        })
    }

    /// What was recorded with the placement of an attempt (the payload that
    /// was decided, and the decision), acknowledged or not.
    pub fn placement_demand(&self, attempt_id: &AttemptId) -> Result<Option<Value>> {
        let aid = attempt_id.to_string();
        self.db.call(move |c| {
            let demand: Option<String> = c
                .query_row(
                    "SELECT demand_json FROM placement WHERE attempt_id = ?1
                     ORDER BY placement_id DESC LIMIT 1",
                    [&aid],
                    |r| r.get(0),
                )
                .optional()?;
            demand
                .map(|d| from_json("placement demand", &d))
                .transpose()
        })
    }

    /// After a restart nothing is known first-hand: every attempt that still
    /// needs attention is `reconciling` until its worker was asked. Returns
    /// how many.
    pub fn mark_reconciling(&self) -> Result<u32> {
        self.db.write(|tx| {
            let n = tx.execute(
                "UPDATE attempt SET observation = 'reconciling'
                 WHERE state <> 'terminal' AND retired = 0 AND observation <> 'reconciling'",
                [],
            )?;
            Ok(n as u32)
        })
    }
}
