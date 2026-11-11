//! The worker database: accepted attempts (idempotent by attempt id and
//! payload hash), tombstones, launch intents and process identity, leases,
//! log spool positions, results and `ack_collected`.
//!
//! The agent and the attempt supervisors are different processes and each
//! opens this file itself: WAL plus the busy timeout serialize their writes,
//! and every state change is a conditional update.

use super::common::{self, from_json, json, parse_state};
use super::policy::{
    decide_claim, decide_lease, decide_submit, Access, ClaimDecision, ExistingAttempt, GcCandidate,
    LeaseDecision, SubmitDecision, SubmitKey,
};
use super::{
    to_i64, to_u64, BlobRef, Db, DbKind, Event, Finalized, GcReport, NewBlobRef, Result,
    StorageError, DEFAULT_GC_BATCH,
};
use crate::ids::{
    AgentEpoch, AttemptId, CoordinatorEpoch, Generation, IdError, NodeId, PayloadHash, RunId,
    TaskId, WorkerBootId, WorkerId,
};
use crate::state::{AttemptState, Receipt, Receipts, Termination};
use crate::wire::{canonical_json, LeaseKey};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde_json::Value;
use std::path::Path;

const ATTEMPT: &str = "worker_attempt";

/// How long a worker waits for `ack_collected` after an attempt ended before
/// it stops protecting the attempt's files from GC (spec chapter 13: seven
/// days after terminal).
pub const DEFAULT_ACK_WAIT_MS: u64 = 7 * 24 * 60 * 60 * 1000;

fn id<T>(what: &str, r: std::result::Result<T, IdError>) -> Result<T> {
    r.map_err(|e| StorageError::Corrupt(format!("{what}: {e}")))
}

/// What the worker persists when it accepts an attempt.
#[derive(Debug, Clone)]
pub struct NewAcceptance {
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub generation: Generation,
    pub payload_hash: PayloadHash,
    pub coordinator_epoch: CoordinatorEpoch,
    pub spec: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedAttempt {
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub generation: Generation,
    pub payload_hash: PayloadHash,
    pub coordinator_epoch: CoordinatorEpoch,
    pub spec: Value,
    pub state: AttemptState,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
    pub cancel_requested_at_ms: Option<u64>,
    pub retired: bool,
    pub accepted_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
    /// `ack_collected` was accepted: the GC protection is lifted.
    pub acked_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmitOutcome {
    /// Persisted now; the receipt `worker accepted` is set.
    Accepted(AcceptedAttempt),
    /// A resend of the same attempt and payload.
    Existing(AcceptedAttempt),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    /// The attempt is known and now carries the cancel request.
    Requested(AcceptedAttempt),
    AlreadyRequested(AcceptedAttempt),
    /// The attempt has not arrived. A cancel tombstone was kept so that a
    /// late submit is refused.
    Tombstoned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub boot_id: WorkerBootId,
    pub pid: u32,
    /// Process start identity (not just the PID, which is reused).
    pub start_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchClaim {
    /// The intent is persisted: the supervisor may spawn, once.
    Granted,
    /// The node was claimed before. Check the recorded identity; never spawn.
    AlreadyClaimed { identity: Option<ProcessIdentity> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOutcome {
    Extended,
    /// A replay or an older renewal: nothing changed (`extended: false`).
    Stale,
}

/// Where the log spool stands, per stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogPosition {
    pub written_bytes: u64,
    pub dropped_bytes: u64,
    pub last_seq: u64,
}

impl LogPosition {
    /// Bytes were not stored (the spool limit): `log_truncated`.
    pub fn truncated(&self) -> bool {
        self.dropped_bytes > 0
    }
}

const COLS: &str = "attempt_id, run_id, task_id, generation, payload_hash, coordinator_epoch,
    spec_json, state, termination_json, receipts_json, cancel_requested_at_ms, retired,
    accepted_at_ms, terminal_at_ms, acked_at_ms";

fn from_row(r: &Row<'_>) -> Result<AcceptedAttempt> {
    let state = parse_state(&r.get::<_, String>(7)?)?;
    let termination: Option<Termination> = r
        .get::<_, Option<String>>(8)?
        .as_deref()
        .map(|s| from_json("termination", s))
        .transpose()?;
    crate::state::check_termination(state, termination.as_ref())
        .map_err(|e| StorageError::Corrupt(e.to_string()))?;
    Ok(AcceptedAttempt {
        attempt_id: id("attempt_id", AttemptId::parse(&r.get::<_, String>(0)?))?,
        run_id: id("run_id", RunId::parse(&r.get::<_, String>(1)?))?,
        task_id: id("task_id", TaskId::parse(&r.get::<_, String>(2)?))?,
        generation: Generation(to_u64(r.get(3)?)?),
        payload_hash: id("payload_hash", PayloadHash::parse(&r.get::<_, String>(4)?))?,
        coordinator_epoch: CoordinatorEpoch(to_u64(r.get(5)?)?),
        spec: from_json("spec", &r.get::<_, String>(6)?)?,
        state,
        termination,
        receipts: from_json("receipts", &r.get::<_, String>(9)?)?,
        cancel_requested_at_ms: r.get::<_, Option<i64>>(10)?.map(to_u64).transpose()?,
        retired: r.get::<_, i64>(11)? == 1,
        accepted_at_ms: to_u64(r.get(12)?)?,
        terminal_at_ms: r.get::<_, Option<i64>>(13)?.map(to_u64).transpose()?,
        acked_at_ms: r.get::<_, Option<i64>>(14)?.map(to_u64).transpose()?,
    })
}

fn load(conn: &Connection, attempt_id: &str) -> Result<Option<AcceptedAttempt>> {
    conn.query_row(
        &format!("SELECT {COLS} FROM worker_attempt WHERE attempt_id = ?1"),
        [attempt_id],
        |r| Ok(from_row(r)),
    )
    .optional()?
    .transpose()
}

fn require(conn: &Connection, attempt_id: &str) -> Result<AcceptedAttempt> {
    load(conn, attempt_id)?.ok_or_else(|| StorageError::NotFound {
        what: format!("attempt {attempt_id}"),
    })
}

/// Run `body` for a request that carries a coordinator epoch, in one
/// transaction. The epoch is checked first (an older one is `StaleEpoch` and
/// nothing is written) and a newer one is remembered. That remembering is
/// committed even when `body` fails with a verdict (conflict, retired, not
/// renewable, ...): the worker has seen the newer coordinator and must refuse
/// the older one from then on. `body`'s own writes are rolled back on failure
/// (a savepoint), so a failed request leaves nothing but the epoch.
fn under_epoch<T, F>(db: &Db, epoch: CoordinatorEpoch, body: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&Transaction<'_>) -> Result<T> + Send + 'static,
{
    db.write_then(move |tx| {
        observe_epoch(tx, epoch)?;
        tx.execute_batch("SAVEPOINT body")?;
        match body(tx) {
            Ok(v) => {
                tx.execute_batch("RELEASE body")?;
                Ok(Ok(v))
            }
            Err(e) => {
                tx.execute_batch("ROLLBACK TO body; RELEASE body")?;
                Ok(Err(e))
            }
        }
    })
}

/// Reject a request from an older coordinator epoch; remember the newest.
fn observe_epoch(conn: &Connection, epoch: CoordinatorEpoch) -> Result<()> {
    let current: i64 =
        conn.query_row("SELECT max_coordinator_epoch FROM worker_state", [], |r| {
            r.get(0)
        })?;
    let current = CoordinatorEpoch(to_u64(current)?);
    if epoch < current {
        return Err(StorageError::StaleEpoch {
            got: epoch,
            current,
        });
    }
    if epoch > current {
        conn.execute(
            "UPDATE worker_state SET max_coordinator_epoch = ?1",
            [to_i64(epoch.0)?],
        )?;
    }
    Ok(())
}

fn identity_of(
    boot: Option<String>,
    pid: Option<i64>,
    start: Option<String>,
) -> Result<Option<ProcessIdentity>> {
    match (boot, pid, start) {
        (Some(b), Some(p), Some(s)) => Ok(Some(ProcessIdentity {
            boot_id: id("boot_id", WorkerBootId::parse(&b))?,
            pid: u32::try_from(p).map_err(|_| StorageError::Corrupt(format!("pid {p}")))?,
            start_identity: s,
        })),
        (None, None, None) => Ok(None),
        _ => Err(StorageError::Corrupt("partial process identity".into())),
    }
}

pub struct WorkerStore {
    pub(crate) db: Db,
    worker_id: WorkerId,
}

impl WorkerStore {
    /// Open (creating or migrating) `worker.db` in `state_dir`. Safe to call
    /// from the agent and from supervisors at the same time.
    pub fn open(state_dir: &Path, worker_id: &WorkerId) -> Result<Self> {
        let db = Db::open(
            &state_dir.join("worker.db"),
            DbKind::Worker,
            Access::ReadWrite,
        )?;
        let w = worker_id.to_string();
        let existing = db.write(move |tx| {
            let existing: Option<String> = tx
                .query_row("SELECT worker_id FROM worker_state", [], |r| r.get(0))
                .optional()?;
            if existing.is_none() {
                tx.execute(
                    "INSERT INTO worker_state (singleton, worker_id, agent_epoch) VALUES (1, ?1, 0)",
                    [&w],
                )?;
            }
            Ok(existing)
        })?;
        if let Some(e) = existing {
            if e != worker_id.as_str() {
                return Err(StorageError::Invalid(format!(
                    "database belongs to worker {e}, not {worker_id}"
                )));
            }
        }
        Ok(Self {
            db,
            worker_id: worker_id.clone(),
        })
    }

    /// Read without migrating or writing.
    pub fn open_read_only(state_dir: &Path) -> Result<Self> {
        let db = Db::open(
            &state_dir.join("worker.db"),
            DbKind::Worker,
            Access::ReadOnly,
        )?;
        let w: String =
            db.call(|c| Ok(c.query_row("SELECT worker_id FROM worker_state", [], |r| r.get(0))?))?;
        Ok(Self {
            db,
            worker_id: id("worker_id", WorkerId::parse(&w))?,
        })
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// The highest coordinator epoch a request carried so far.
    pub fn max_coordinator_epoch(&self) -> Result<CoordinatorEpoch> {
        self.db.call(|c| {
            let e: i64 =
                c.query_row("SELECT max_coordinator_epoch FROM worker_state", [], |r| {
                    r.get(0)
                })?;
            Ok(CoordinatorEpoch(to_u64(e)?))
        })
    }

    /// A new agent process started: bump the agent epoch.
    pub fn start_agent(&self, at_ms: u64) -> Result<AgentEpoch> {
        self.db.write(move |tx| {
            tx.execute("UPDATE worker_state SET agent_epoch = agent_epoch + 1", [])?;
            let e: i64 = tx.query_row("SELECT agent_epoch FROM worker_state", [], |r| r.get(0))?;
            common::append_event(
                tx,
                None,
                None,
                "agent_started",
                &serde_json::json!({"agent_epoch": e}),
                at_ms,
            )?;
            Ok(AgentEpoch(to_u64(e)?))
        })
    }

    // -------------------------------------------------------------- accept

    /// Persist an attempt before answering. The same attempt id with the same
    /// payload, run, task and generation returns the existing record; any
    /// difference in those is a conflict; an attempt
    /// with a tombstone (a cancel or retire arrived first, or it was retired)
    /// is refused with `Retired`. A task that has an attempt which is not
    /// terminal refuses another one (`LiveAttemptExists`), a retired attempt
    /// included: it counts until it is confirmed stopped (terminal). Nothing
    /// here starts a command.
    pub fn submit(&self, new: NewAcceptance, at_ms: u64) -> Result<SubmitOutcome> {
        under_epoch(&self.db, new.coordinator_epoch, move |tx| {
            let aid = new.attempt_id.to_string();
            let existing = load(tx, &aid)?;
            let tombstoned = tx
                .query_row(
                    "SELECT 1 FROM tombstone WHERE attempt_id = ?1",
                    [&aid],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            let verdict = decide_submit(
                existing.as_ref().map(|e| ExistingAttempt {
                    key: SubmitKey {
                        run_id: &e.run_id,
                        task_id: &e.task_id,
                        generation: e.generation,
                        payload_hash: &e.payload_hash,
                    },
                    retired: e.retired,
                }),
                tombstoned,
                SubmitKey {
                    run_id: &new.run_id,
                    task_id: &new.task_id,
                    generation: new.generation,
                    payload_hash: &new.payload_hash,
                },
            );
            match (verdict, existing) {
                (SubmitDecision::Existing, Some(e)) => Ok(SubmitOutcome::Existing(e)),
                (SubmitDecision::Conflict, _) => Err(StorageError::PayloadConflict(new.attempt_id)),
                (SubmitDecision::Retired, _) => Err(StorageError::Retired(new.attempt_id)),
                (SubmitDecision::Insert, _) | (SubmitDecision::Existing, None) => {
                    let task = new.task_id.to_string();
                    let live: Option<String> = tx
                        .query_row(
                            "SELECT attempt_id FROM worker_attempt
                             WHERE task_id = ?1 AND state <> 'terminal'",
                            [&task],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if let Some(live) = live {
                        return Err(StorageError::LiveAttemptExists(live));
                    }
                    let mut receipts = Receipts::default();
                    receipts.mark(Receipt::WorkerAccepted, at_ms);
                    tx.execute(
                        "INSERT INTO worker_attempt (attempt_id, run_id, task_id, generation,
                            payload_hash, coordinator_epoch, spec_json, state, receipts_json,
                            accepted_at_ms)
                         VALUES (?1,?2,?3,?4,?5,?6,?7,'accepted',?8,?9)",
                        params![
                            aid,
                            new.run_id.as_str(),
                            task,
                            to_i64(new.generation.0)?,
                            new.payload_hash.as_str(),
                            to_i64(new.coordinator_epoch.0)?,
                            new.spec.to_string(),
                            json(&receipts),
                            to_i64(at_ms)?
                        ],
                    )?;
                    common::append_event(
                        tx,
                        Some(new.run_id.as_str()),
                        Some(&aid),
                        "attempt_accepted",
                        &Value::Null,
                        at_ms,
                    )?;
                    Ok(SubmitOutcome::Accepted(require(tx, &aid)?))
                }
            }
        })
    }

    pub fn get(&self, attempt_id: &AttemptId) -> Result<Option<AcceptedAttempt>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| load(c, &a))
    }

    pub fn list_nonterminal(&self) -> Result<Vec<AcceptedAttempt>> {
        self.db.call(|c| {
            let mut s = c.prepare(&format!(
                "SELECT {COLS} FROM worker_attempt WHERE state <> 'terminal'
                 ORDER BY accepted_at_ms, attempt_id"
            ))?;
            let rows = s.query_map([], |r| Ok(from_row(r)))?;
            rows.map(|r| r?).collect()
        })
    }

    /// The attempts this worker knows, newest first, at most `limit`: those
    /// in one of `states` when given, terminal ones only when
    /// `include_terminal`. The filter runs in the database, so `limit` counts
    /// matching attempts; a caller that must not answer with a part asks for
    /// one more than it will send.
    pub fn list_attempts(
        &self,
        states: Option<&[AttemptState]>,
        include_terminal: bool,
        limit: u32,
    ) -> Result<Vec<AcceptedAttempt>> {
        // The names are this crate's own constants, never input.
        let wanted = match states {
            None => "1".to_string(),
            Some(states) => {
                let names: Vec<String> = states
                    .iter()
                    .map(|s| format!("'{}'", common::state_str(*s)))
                    .collect();
                format!("state IN ({})", names.join(","))
            }
        };
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT {COLS} FROM worker_attempt
                 WHERE {wanted} AND (?1 OR state <> 'terminal')
                 ORDER BY accepted_at_ms DESC, attempt_id DESC LIMIT ?2"
            ))?;
            let rows = s.query_map(params![include_terminal, limit], |r| Ok(from_row(r)))?;
            rows.map(|r| r?).collect()
        })
    }

    /// How many attempts are not terminal.
    pub fn count_nonterminal(&self) -> Result<u64> {
        self.db.call(|c| {
            let n: i64 = c.query_row(
                "SELECT COUNT(*) FROM worker_attempt WHERE state <> 'terminal'",
                [],
                |r| r.get(0),
            )?;
            to_u64(n)
        })
    }

    /// How many launch intents the database holds, over every attempt.
    pub fn count_launches(&self) -> Result<u64> {
        self.db.call(|c| {
            let n: i64 = c.query_row("SELECT COUNT(*) FROM command_launch", [], |r| r.get(0))?;
            to_u64(n)
        })
    }

    /// When the last renewal was applied (this worker's wall clock), if any.
    pub fn lease_renewed_at(&self, attempt_id: &AttemptId) -> Result<Option<u64>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            c.query_row(
                "SELECT renewed_at_ms FROM lease WHERE attempt_id = ?1",
                [&a],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .map(to_u64)
            .transpose()
        })
    }

    // ------------------------------------------------------ cancel, retire

    /// Persist a cancel request before doing anything about it. For an
    /// attempt that has not arrived, keep a tombstone instead.
    pub fn cancel(
        &self,
        attempt_id: &AttemptId,
        epoch: CoordinatorEpoch,
        at_ms: u64,
    ) -> Result<CancelOutcome> {
        let a = attempt_id.to_string();
        under_epoch(&self.db, epoch, move |tx| {
            let Some(existing) = load(tx, &a)? else {
                tx.execute(
                    "INSERT OR IGNORE INTO tombstone (attempt_id, kind, coordinator_epoch, recorded_at_ms)
                     VALUES (?1,'cancel',?2,?3)",
                    params![a, to_i64(epoch.0)?, to_i64(at_ms)?],
                )?;
                return Ok(CancelOutcome::Tombstoned);
            };
            if existing.cancel_requested_at_ms.is_some() {
                return Ok(CancelOutcome::AlreadyRequested(existing));
            }
            tx.execute(
                "UPDATE worker_attempt SET cancel_requested_at_ms = ?1 WHERE attempt_id = ?2",
                params![to_i64(at_ms)?, a],
            )?;
            common::append_event(
                tx,
                Some(existing.run_id.as_str()),
                Some(&a),
                "cancel_requested",
                &Value::Null,
                at_ms,
            )?;
            Ok(CancelOutcome::Requested(require(tx, &a)?))
        })
    }

    /// Persist the retire tombstone and mark a known attempt retired: no
    /// later submit or lease renewal can revive it. Returns the attempt, if
    /// known, so the caller can see whether it still has to be stopped.
    pub fn retire(
        &self,
        attempt_id: &AttemptId,
        epoch: CoordinatorEpoch,
        at_ms: u64,
    ) -> Result<Option<AcceptedAttempt>> {
        let a = attempt_id.to_string();
        under_epoch(&self.db, epoch, move |tx| {
            tx.execute(
                "INSERT INTO tombstone (attempt_id, kind, coordinator_epoch, recorded_at_ms)
                 VALUES (?1,'retire',?2,?3)
                 ON CONFLICT (attempt_id) DO UPDATE SET kind = 'retire'",
                params![a, to_i64(epoch.0)?, to_i64(at_ms)?],
            )?;
            let Some(existing) = load(tx, &a)? else {
                return Ok(None);
            };
            if !existing.retired {
                tx.execute(
                    "UPDATE worker_attempt SET retired = 1,
                        cancel_requested_at_ms = COALESCE(cancel_requested_at_ms, ?1)
                     WHERE attempt_id = ?2",
                    params![to_i64(at_ms)?, a],
                )?;
                common::append_event(
                    tx,
                    Some(existing.run_id.as_str()),
                    Some(&a),
                    "retired",
                    &Value::Null,
                    at_ms,
                )?;
            }
            Ok(Some(require(tx, &a)?))
        })
    }

    /// `Some("cancel" | "retire")` when a tombstone exists for the id.
    pub fn tombstone(&self, attempt_id: &AttemptId) -> Result<Option<String>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            Ok(c.query_row(
                "SELECT kind FROM tombstone WHERE attempt_id = ?1",
                [&a],
                |r| r.get(0),
            )
            .optional()?)
        })
    }

    // ------------------------------------------------------------ progress

    pub fn transition(
        &self,
        attempt_id: &AttemptId,
        to: AttemptState,
        at_ms: u64,
    ) -> Result<AttemptState> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = require(tx, &a)?.run_id;
            common::transition(tx, ATTEMPT, &a, to, at_ms, Some(run.as_str()))
        })
    }

    /// Fix the termination, once; a competing writer gets the first result.
    pub fn finalize(
        &self,
        attempt_id: &AttemptId,
        termination: Termination,
        at_ms: u64,
    ) -> Result<Finalized> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = require(tx, &a)?.run_id;
            common::finalize(tx, ATTEMPT, &a, termination, at_ms, Some(run.as_str()))
        })
    }

    /// `command registered` or `running`; the others have their own paths.
    pub fn mark_receipt(
        &self,
        attempt_id: &AttemptId,
        receipt: Receipt,
        at_ms: u64,
    ) -> Result<bool> {
        if !matches!(receipt, Receipt::CommandRegistered | Receipt::Running) {
            return Err(StorageError::Invalid(format!(
                "a worker does not set {receipt:?} directly"
            )));
        }
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = require(tx, &a)?.run_id;
            let fresh = common::mark_receipt(tx, ATTEMPT, &a, receipt, at_ms)?;
            if fresh {
                // In the same commit, so the journal shows it exactly once.
                common::append_event(
                    tx,
                    Some(run.as_str()),
                    Some(&a),
                    "attempt_receipt",
                    &serde_json::json!({ "receipt": receipt }),
                    at_ms,
                )?;
            }
            Ok(fresh)
        })
    }

    // ------------------------------------------------------------- launch

    /// Claim the right to spawn the command of `node`. The launch intent is
    /// persisted in the same commit (and the first claim moves the attempt
    /// `Ready -> Starting`), so a crash after this never loses the fact that a
    /// spawn may have happened. Refused (an error) after a cancel or retire,
    /// once the lease expired, and in states that cannot start commands. The
    /// lease is read in the same transaction as the claim.
    pub fn claim_launch(
        &self,
        attempt_id: &AttemptId,
        node: &NodeId,
        at_ms: u64,
    ) -> Result<LaunchClaim> {
        let (a, n) = (attempt_id.to_string(), node.to_string());
        self.db.write(move |tx| {
            let rec = require(tx, &a)?;
            let launch: Option<(Option<String>, Option<i64>, Option<String>)> = tx
                .query_row(
                    "SELECT boot_id, pid, start_identity FROM command_launch
                     WHERE attempt_id = ?1 AND node_id = ?2",
                    params![a, n],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let lease_expired: Option<Option<i64>> = tx
                .query_row(
                    "SELECT expired_at_ms FROM lease WHERE attempt_id = ?1",
                    [&a],
                    |r| r.get(0),
                )
                .optional()?;
            let expired = lease_expired.flatten().is_some();
            let decision = decide_claim(
                rec.state,
                rec.cancel_requested_at_ms.is_some(),
                rec.retired,
                expired,
                launch.is_some(),
            );
            match decision {
                ClaimDecision::AlreadyClaimed => {
                    let (b, p, s) = launch.unwrap_or_default();
                    Ok(LaunchClaim::AlreadyClaimed {
                        identity: identity_of(b, p, s)?,
                    })
                }
                ClaimDecision::Refused if rec.retired => Err(StorageError::Retired(rec.attempt_id)),
                ClaimDecision::Refused => Err(StorageError::IllegalState {
                    attempt: a,
                    state: rec.state,
                    why: if expired {
                        "the lease expired: no command may start"
                    } else {
                        "no command may start (cancelled, or not ready)"
                    },
                }),
                ClaimDecision::StartAndGrant | ClaimDecision::Grant => {
                    if decision == ClaimDecision::StartAndGrant {
                        common::transition(
                            tx,
                            ATTEMPT,
                            &a,
                            AttemptState::Starting,
                            at_ms,
                            Some(rec.run_id.as_str()),
                        )?;
                    }
                    tx.execute(
                        "INSERT INTO command_launch (attempt_id, node_id, intent_at_ms) VALUES (?1,?2,?3)",
                        params![a, n, to_i64(at_ms)?],
                    )?;
                    Ok(LaunchClaim::Granted)
                }
            }
        })
    }

    /// Record the spawned process (boot id and start identity, not just the
    /// PID). Set once; the same identity again is a no-op.
    pub fn record_process_identity(
        &self,
        attempt_id: &AttemptId,
        node: &NodeId,
        identity: &ProcessIdentity,
        at_ms: u64,
    ) -> Result<()> {
        let (a, n, i) = (attempt_id.to_string(), node.to_string(), identity.clone());
        self.db.write(move |tx| {
            let row: Option<(Option<String>, Option<i64>, Option<String>)> = tx
                .query_row(
                    "SELECT boot_id, pid, start_identity FROM command_launch
                     WHERE attempt_id = ?1 AND node_id = ?2",
                    params![a, n],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let (b, p, s) = row.ok_or_else(|| StorageError::NotFound {
                what: format!("launch intent of {a}/{n}"),
            })?;
            if let Some(existing) = identity_of(b, p, s)? {
                return if existing == i {
                    Ok(())
                } else {
                    Err(StorageError::Immutable(format!(
                        "process identity of {a}/{n}"
                    )))
                };
            }
            tx.execute(
                "UPDATE command_launch SET boot_id = ?3, pid = ?4, start_identity = ?5,
                        identity_at_ms = ?6
                 WHERE attempt_id = ?1 AND node_id = ?2",
                params![
                    a,
                    n,
                    i.boot_id.as_str(),
                    i.pid,
                    i.start_identity,
                    to_i64(at_ms)?
                ],
            )?;
            Ok(())
        })
    }

    /// The launch intents of an attempt: node, intent time, identity if known.
    pub fn launches(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Vec<(NodeId, u64, Option<ProcessIdentity>)>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            let mut s = c.prepare(
                "SELECT node_id, intent_at_ms, boot_id, pid, start_identity FROM command_launch
                 WHERE attempt_id = ?1 ORDER BY intent_at_ms, node_id",
            )?;
            let rows = s.query_map([&a], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (n, t, b, p, s) = row?;
                out.push((
                    id("node_id", NodeId::parse(&n))?,
                    to_u64(t)?,
                    identity_of(b, p, s)?,
                ));
            }
            Ok(out)
        })
    }

    // -------------------------------------------------------------- lease

    /// Apply a renewal. Only a strictly greater `(coordinator_epoch,
    /// lease_seq)` extends; a replay is `Stale`. A renewal of an expired,
    /// cancelling, finalizing, terminal or retired attempt is an error and
    /// never restores the right to run.
    pub fn renew_lease(
        &self,
        attempt_id: &AttemptId,
        key: LeaseKey,
        at_ms: u64,
    ) -> Result<LeaseOutcome> {
        let a = attempt_id.clone();
        under_epoch(&self.db, key.coordinator_epoch, move |tx| {
            let aid = a.to_string();
            let rec = require(tx, &aid)?;
            let row: Option<(i64, i64, Option<i64>)> = tx
                .query_row(
                    "SELECT coordinator_epoch, lease_seq, expired_at_ms FROM lease WHERE attempt_id = ?1",
                    [&aid],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let applied = row
                .map(|(e, s, _)| -> Result<LeaseKey> {
                    Ok(LeaseKey {
                        coordinator_epoch: CoordinatorEpoch(to_u64(e)?),
                        lease_seq: to_u64(s)?,
                    })
                })
                .transpose()?;
            let expired = row.is_some_and(|(_, _, x)| x.is_some());
            match decide_lease(
                rec.state,
                rec.retired,
                expired,
                rec.cancel_requested_at_ms.is_some(),
                applied,
                key,
            ) {
                LeaseDecision::Refuse => Err(StorageError::LeaseNotRenewable(a)),
                LeaseDecision::Stale => Ok(LeaseOutcome::Stale),
                LeaseDecision::Extend => {
                    tx.execute(
                        "INSERT INTO lease (attempt_id, coordinator_epoch, lease_seq, renewed_at_ms)
                         VALUES (?1,?2,?3,?4)
                         ON CONFLICT (attempt_id) DO UPDATE SET coordinator_epoch = ?2,
                            lease_seq = ?3, renewed_at_ms = ?4",
                        params![aid, to_i64(key.coordinator_epoch.0)?, to_i64(key.lease_seq)?, to_i64(at_ms)?],
                    )?;
                    Ok(LeaseOutcome::Extended)
                }
            }
        })
    }

    /// The lease ran out (the supervisor saw it by its monotonic clock, or
    /// the agent before any supervisor started). Later renewals are refused.
    /// The first call writes `lease_expired` to the journal in the same
    /// commit; whoever comes second changes nothing. Returns whether this
    /// call was the first.
    pub fn expire_lease(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<bool> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = require(tx, &a)?.run_id;
            let already: Option<Option<i64>> = tx
                .query_row(
                    "SELECT expired_at_ms FROM lease WHERE attempt_id = ?1",
                    [&a],
                    |r| r.get(0),
                )
                .optional()?;
            if already.flatten().is_some() {
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO lease (attempt_id, coordinator_epoch, lease_seq, renewed_at_ms, expired_at_ms)
                 VALUES (?1, 0, 0, ?2, ?2)
                 ON CONFLICT (attempt_id) DO UPDATE SET expired_at_ms = ?2",
                params![a, to_i64(at_ms)?],
            )?;
            common::append_event(
                tx,
                Some(run.as_str()),
                Some(&a),
                "lease_expired",
                &Value::Null,
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// The highest applied key and whether the lease expired.
    pub fn lease(&self, attempt_id: &AttemptId) -> Result<Option<(LeaseKey, bool)>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            c.query_row(
                "SELECT coordinator_epoch, lease_seq, expired_at_ms FROM lease WHERE attempt_id = ?1",
                [&a],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, Option<i64>>(2)?)),
            )
            .optional()?
            .map(|(e, s, x)| {
                Ok((
                    LeaseKey {
                        coordinator_epoch: CoordinatorEpoch(to_u64(e)?),
                        lease_seq: to_u64(s)?,
                    },
                    x.is_some(),
                ))
            })
            .transpose()
        })
    }

    // ---------------------------------------------------------- log, result

    /// Advance a stream's spool position. Never moves backwards.
    pub fn update_log_position(
        &self,
        attempt_id: &AttemptId,
        stream: &str,
        pos: LogPosition,
    ) -> Result<LogPosition> {
        let (a, s) = (attempt_id.to_string(), stream.to_string());
        self.db.write(move |tx| {
            require(tx, &a)?;
            tx.execute(
                "INSERT INTO log_spool (attempt_id, stream, written_bytes, dropped_bytes, last_seq)
                 VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT (attempt_id, stream) DO UPDATE SET
                    written_bytes = MAX(written_bytes, ?3),
                    dropped_bytes = MAX(dropped_bytes, ?4),
                    last_seq = MAX(last_seq, ?5)",
                params![
                    a,
                    s,
                    to_i64(pos.written_bytes)?,
                    to_i64(pos.dropped_bytes)?,
                    to_i64(pos.last_seq)?
                ],
            )?;
            read_log(tx, &a, &s)
        })
    }

    pub fn log_position(&self, attempt_id: &AttemptId, stream: &str) -> Result<LogPosition> {
        let (a, s) = (attempt_id.to_string(), stream.to_string());
        self.db.call(move |c| read_log(c, &a, &s))
    }

    /// The log bytes stored by the other attempts of `attempt_id`'s task, as
    /// their stored positions say (both streams). The task's spool limit is
    /// shared by its attempts, so this is what the attempt cannot use. An
    /// attempt counts while its files can be on disk: until it is terminal,
    /// and after that as long as it has a `log` reference. Once GC dropped
    /// that reference the files are deleted (or queued for it) and their
    /// bytes are free again; the `log_spool` rows stay as a record.
    pub fn log_bytes_of_other_attempts(&self, attempt_id: &AttemptId) -> Result<u64> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            require(c, &a)?;
            let n: i64 = c.query_row(
                "SELECT COALESCE(SUM(l.written_bytes), 0)
                 FROM log_spool l JOIN worker_attempt o ON o.attempt_id = l.attempt_id
                 WHERE o.attempt_id <> ?1 AND o.task_id =
                    (SELECT task_id FROM worker_attempt WHERE attempt_id = ?1)
                   AND (o.state <> 'terminal' OR EXISTS
                    (SELECT 1 FROM blob_ref b
                     WHERE b.attempt_id = o.attempt_id AND b.kind = 'log'))",
                params![a],
                |r| r.get(0),
            )?;
            to_u64(n)
        })
    }

    /// Store the result of a terminal attempt, once. It is kept as canonical
    /// JSON (the wire form: sorted keys, integers only), and a second call is
    /// the same result when the canonical forms are equal, whatever the key
    /// order or spacing it was built with. Returns whether this call stored it
    /// (`false` for the replay of an equal result).
    pub fn record_result(&self, attempt_id: &AttemptId, result: &Value) -> Result<bool> {
        let text = canonical_json(result)
            .map_err(|e| StorageError::Invalid(format!("result is not canonical JSON: {e}")))?;
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let rec = require(tx, &a)?;
            if !rec.state.is_terminal() {
                return Err(StorageError::IllegalState {
                    attempt: a,
                    state: rec.state,
                    why: "a result is stored at terminal",
                });
            }
            let existing: Option<String> = tx.query_row(
                "SELECT result_json FROM worker_attempt WHERE attempt_id = ?1",
                [&a],
                |r| r.get(0),
            )?;
            match existing {
                None => {
                    tx.execute(
                        "UPDATE worker_attempt SET result_json = ?1 WHERE attempt_id = ?2",
                        params![text, a],
                    )?;
                    Ok(true)
                }
                Some(e) => {
                    let stored: Value = from_json("result", &e)?;
                    let stored = canonical_json(&stored)
                        .map_err(|e| StorageError::Corrupt(format!("result: {e}")))?;
                    if stored == text {
                        Ok(false)
                    } else {
                        Err(StorageError::Immutable(format!("result of {a}")))
                    }
                }
            }
        })
    }

    pub fn get_result(&self, attempt_id: &AttemptId) -> Result<Option<Value>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            let text: Option<Option<String>> = c
                .query_row(
                    "SELECT result_json FROM worker_attempt WHERE attempt_id = ?1",
                    [&a],
                    |r| r.get(0),
                )
                .optional()?;
            text.flatten().map(|t| from_json("result", &t)).transpose()
        })
    }

    /// The parent persisted the result: lift the GC protection. Idempotent;
    /// only a terminal attempt can be acknowledged. Returns whether this call
    /// recorded it.
    pub fn ack_collected(
        &self,
        attempt_id: &AttemptId,
        epoch: CoordinatorEpoch,
        at_ms: u64,
    ) -> Result<bool> {
        let a = attempt_id.to_string();
        under_epoch(&self.db, epoch, move |tx| {
            let rec = require(tx, &a)?;
            if !rec.state.is_terminal() {
                return Err(StorageError::IllegalState {
                    attempt: a,
                    state: rec.state,
                    why: "only a terminal attempt can be acknowledged",
                });
            }
            let n = tx.execute(
                "UPDATE worker_attempt SET acked_at_ms = ?1 WHERE attempt_id = ?2 AND acked_at_ms IS NULL",
                params![to_i64(at_ms)?, a],
            )?;
            if n == 1 {
                common::append_event(
                    tx,
                    Some(rec.run_id.as_str()),
                    Some(&a),
                    "ack_collected",
                    &Value::Null,
                    at_ms,
                )?;
            }
            Ok(n == 1)
        })
    }

    // -------------------------------------------------------------- events

    pub fn append_event(
        &self,
        run_id: Option<&RunId>,
        attempt_id: Option<&AttemptId>,
        kind: &str,
        payload: &Value,
        at_ms: u64,
    ) -> Result<u64> {
        let (r, a) = (
            run_id.map(ToString::to_string),
            attempt_id.map(ToString::to_string),
        );
        let (kind, payload) = (kind.to_string(), payload.clone());
        self.db.write(move |tx| {
            common::append_event(tx, r.as_deref(), a.as_deref(), &kind, &payload, at_ms)
        })
    }

    pub fn events_after(&self, after_seq: u64, limit: u32) -> Result<Vec<Event>> {
        self.db
            .call(move |c| common::events_after(c, after_seq, limit))
    }

    /// Every event of one attempt, oldest first, at most `limit`.
    pub fn events_of_attempt(&self, attempt_id: &AttemptId, limit: u32) -> Result<Vec<Event>> {
        let a = attempt_id.to_string();
        self.db
            .call(move |c| common::events_of_attempt(c, &a, limit))
    }

    /// The events of one attempt with one `kind`, oldest first.
    pub fn events_of(&self, attempt_id: &AttemptId, kind: &str) -> Result<Vec<Event>> {
        let (a, k) = (attempt_id.to_string(), kind.to_string());
        self.db.call(move |c| common::events_of(c, &a, &k))
    }

    /// For each `(key, payload)` of `wanted`, append an event of `kind` to the
    /// attempt unless it already has one of that kind whose `payload[key_field]`
    /// is `key`; the check and the appends are one transaction, so concurrent
    /// callers cannot both append. Returns how many were appended.
    pub fn append_events_unseen(
        &self,
        run_id: Option<&RunId>,
        attempt_id: &AttemptId,
        kind: &str,
        key_field: &str,
        wanted: &[(String, Value)],
        at_ms: u64,
    ) -> Result<u32> {
        let r = run_id.map(ToString::to_string);
        let (a, k, f) = (
            attempt_id.to_string(),
            kind.to_string(),
            key_field.to_string(),
        );
        let wanted = wanted.to_vec();
        self.db.write(move |tx| {
            common::append_events_unseen(tx, r.as_deref(), &a, &k, &f, &wanted, at_ms)
        })
    }

    // ------------------------------------------------------- references, GC

    pub fn add_blob_ref(&self, r: &NewBlobRef, at_ms: u64) -> Result<()> {
        let r = r.clone();
        self.db.write(move |tx| common::add_blob_ref(tx, &r, at_ms))
    }

    pub fn pin_blob(&self, ref_id: &str) -> Result<u32> {
        let r = ref_id.to_string();
        self.db.write(move |tx| common::pin(tx, &r, 1))
    }

    pub fn unpin_blob(&self, ref_id: &str) -> Result<u32> {
        let r = ref_id.to_string();
        self.db.write(move |tx| common::pin(tx, &r, -1))
    }

    /// The files waiting to be deleted: their last reference was dropped by
    /// GC. Delete each file, then call [`Self::complete_delete`].
    pub fn pending_deletes(&self) -> Result<Vec<String>> {
        self.db.call(|c| common::pending_deletes(c))
    }

    /// The file of `locator` is deleted: forget it, so the name may be used
    /// again. Idempotent; returns whether a queued locator was removed.
    pub fn complete_delete(&self, locator: &str) -> Result<bool> {
        let l = locator.to_string();
        self.db.write(move |tx| common::complete_delete(tx, &l))
    }

    /// Drop the references of attempts that ended at least `retention_ms` ago
    /// and whose `ack_collected` arrived, or that have waited `ack_wait_ms`
    /// since their end without one ([`DEFAULT_ACK_WAIT_MS`]: spec chapter 13's
    /// seven days after terminal). Running, awaiting-collection (within
    /// that wait) and pinned ones stay. A file whose last reference goes is
    /// queued in `pending_delete` in the same transaction; a new reference to
    /// it is refused until `complete_delete`. Tombstones, dedup records and
    /// the journal are kept. The pass is a series of transactions of
    /// [`DEFAULT_GC_BATCH`] references; a busy database is
    /// [`StorageError::Busy`] and the pass can simply be run again.
    pub fn gc(&self, now_ms: u64, retention_ms: u64, ack_wait_ms: u64) -> Result<GcReport> {
        self.gc_batched(now_ms, retention_ms, ack_wait_ms, DEFAULT_GC_BATCH)
    }

    pub(crate) fn gc_batched(
        &self,
        now_ms: u64,
        retention_ms: u64,
        ack_wait_ms: u64,
        batch: u32,
    ) -> Result<GcReport> {
        common::gc_all(
            &self.db,
            now_ms,
            retention_ms,
            Some(ack_wait_ms),
            batch,
            |conn, r: &BlobRef| {
                let rec = require(conn, &r.attempt_id)?;
                Ok(GcCandidate {
                    terminal_at_ms: rec.terminal_at_ms,
                    retired_at_ms: None,
                    collected: rec.acked_at_ms.is_some(),
                    collection_failed_at_ms: None,
                    missing_required_artifacts: 0,
                    pins: r.pins,
                })
            },
        )
    }
}

fn read_log(conn: &Connection, attempt_id: &str, stream: &str) -> Result<LogPosition> {
    let row: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT written_bytes, dropped_bytes, last_seq FROM log_spool
             WHERE attempt_id = ?1 AND stream = ?2",
            params![attempt_id, stream],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    Ok(match row {
        None => LogPosition::default(),
        Some((w, d, s)) => LogPosition {
            written_bytes: to_u64(w)?,
            dropped_bytes: to_u64(d)?,
            last_seq: to_u64(s)?,
        },
    })
}
