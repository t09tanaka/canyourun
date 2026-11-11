//! What the parent knows about collecting the outputs of an ended attempt
//! (spec chapter 13): the result it read and its digest, how far the logs
//! and artifacts are, and how the collection was closed.
//!
//! One row of `collection` per attempt. It is closed exactly once: after the
//! worker acknowledged `ack_collected` for the recorded digest, when there
//! is nothing to ask a worker for, or when the collection was given up.
//! The `collected` receipt of the attempt is set in the commit that closes
//! a complete collection, never before.

use super::{
    artifact_counts, attempt_from_row, id, load_attempt, not_found, run_of, ArtifactReceipt,
    AttemptRecord, CoordinatorStore, ATTEMPT, ATTEMPT_COLS,
};
use crate::ids::{AttemptId, Digest, RunId, TaskId, WorkerId};
use crate::results::plan::{run_verdict, task_standing, RunVerdict, TaskStanding, Verdict};
use crate::state::{Collection, Receipt, Termination};
use crate::storage::common::{self, from_json, BlobKind};
use crate::storage::{to_i64, to_u64, Result, StorageError};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

/// The collection of one attempt, as far as it is durable.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionRow {
    pub attempt_id: AttemptId,
    /// The digest of the result that is being (or was) collected.
    pub result_digest: Option<Digest>,
    /// That result, as the worker answered it.
    pub result: Option<Value>,
    /// Set once the logs were read to their end: what each stream holds.
    pub logs: Option<Value>,
    pub verdict: Option<Verdict>,
    pub acked_at_ms: Option<u64>,
    pub closed_at_ms: Option<u64>,
    /// Faults that count toward giving up.
    pub strikes: u32,
}

/// An ended attempt whose collection is not closed.
#[derive(Debug, Clone, PartialEq)]
pub struct Collectable {
    pub record: AttemptRecord,
    /// The payload that was sent (it declares the artifacts), if recorded.
    pub payload: Option<Value>,
    /// The parent fixed the result itself (a retire it concluded): what the
    /// worker holds, if anything, is not this attempt's result.
    pub settled_here: bool,
    pub row: Option<CollectionRow>,
}

/// A file of the parent's result store that an attempt references for GC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultRef {
    pub ref_id: String,
    pub kind: BlobKind,
    pub locator: String,
}

/// Reference a file of the result store. The same reference again is a
/// no-op; the same id for another file is refused.
fn add_ref(tx: &rusqlite::Transaction<'_>, attempt: &str, r: &ResultRef, at_ms: u64) -> Result<()> {
    let have: Option<String> = tx
        .query_row(
            "SELECT locator FROM blob_ref WHERE ref_id = ?1",
            [&r.ref_id],
            |row| row.get(0),
        )
        .optional()?;
    match have {
        Some(l) if l == r.locator => Ok(()),
        Some(_) => Err(StorageError::Immutable(format!("reference {}", r.ref_id))),
        None => common::add_blob_ref(
            tx,
            &common::NewBlobRef {
                ref_id: r.ref_id.clone(),
                attempt_id: attempt.to_string(),
                kind: r.kind,
                locator: r.locator.clone(),
            },
            at_ms,
        ),
    }
}

/// Something was verified and is durable: the faults before it are not held
/// against what follows.
fn clear_strikes(tx: &rusqlite::Transaction<'_>, attempt: &str) -> Result<()> {
    tx.execute(
        "UPDATE collection SET strikes = 0
         WHERE attempt_id = ?1 AND closed_at_ms IS NULL AND strikes > 0",
        [attempt],
    )?;
    Ok(())
}

/// Where a run stands, task by task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStanding {
    pub verdict: RunVerdict,
    pub tasks: Vec<(TaskId, TaskStanding)>,
}

const COLLECTION_COLS: &str = "attempt_id, result_digest, result_json, logs_json, verdict,
    acked_at_ms, closed_at_ms, strikes";

type RawRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    i64,
);

fn load_row(conn: &Connection, aid: &str) -> Result<Option<CollectionRow>> {
    let raw: Option<RawRow> = conn
        .query_row(
            &format!("SELECT {COLLECTION_COLS} FROM collection WHERE attempt_id = ?1"),
            [aid],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some((attempt, digest, result, logs, verdict, acked, closed, strikes)) = raw else {
        return Ok(None);
    };
    let verdict = match verdict {
        None => None,
        Some(v) => Some(
            Verdict::parse(&v)
                .ok_or_else(|| StorageError::Corrupt(format!("collection verdict {v}")))?,
        ),
    };
    Ok(Some(CollectionRow {
        attempt_id: id("attempt_id", AttemptId::parse(&attempt))?,
        result_digest: digest
            .map(|d| id("result_digest", Digest::parse(&d)))
            .transpose()?,
        result: result.map(|t| from_json("result", &t)).transpose()?,
        logs: logs.map(|t| from_json("logs", &t)).transpose()?,
        verdict,
        acked_at_ms: acked.map(to_u64).transpose()?,
        closed_at_ms: closed.map(to_u64).transpose()?,
        strikes: u32::try_from(strikes)
            .map_err(|_| StorageError::Corrupt(format!("collection strikes {strikes}")))?,
    }))
}

/// The row of an attempt that exists, made if it is not there yet.
fn ensure_row(conn: &Connection, aid: &str, at_ms: u64) -> Result<CollectionRow> {
    common::read_core(conn, ATTEMPT, aid)?;
    conn.execute(
        "INSERT OR IGNORE INTO collection (attempt_id, updated_at_ms) VALUES (?1, ?2)",
        params![aid, to_i64(at_ms)?],
    )?;
    load_row(conn, aid)?.ok_or_else(|| not_found(format!("collection of {aid}")))
}

/// When the collection of an attempt was closed, if it was.
pub(super) fn closed_at(conn: &Connection, aid: &str) -> Result<Option<u64>> {
    let at: Option<Option<i64>> = conn
        .query_row(
            "SELECT closed_at_ms FROM collection WHERE attempt_id = ?1",
            [aid],
            |r| r.get(0),
        )
        .optional()?;
    at.flatten().map(to_u64).transpose()
}

fn settled_here(conn: &Connection, aid: &str) -> Result<bool> {
    Ok(common::events_of(conn, aid, "retire_confirmed")?
        .iter()
        .any(|e| e.payload.get("settled").is_some_and(|s| !s.is_null())))
}

impl CoordinatorStore {
    /// The terminal attempts (of one worker, or of all) that are not retired
    /// and whose collection is not closed, oldest first.
    pub fn collectable(
        &self,
        worker_id: Option<&WorkerId>,
        limit: u32,
    ) -> Result<Vec<Collectable>> {
        let worker = worker_id.map(ToString::to_string);
        let collections = self.collections;
        self.db.call(move |c| {
            // A file from before collections has none that is closed.
            let open = if collections {
                "AND NOT EXISTS (SELECT 1 FROM collection c
                    WHERE c.attempt_id = attempt.attempt_id AND c.closed_at_ms IS NOT NULL)"
            } else {
                ""
            };
            let mut s = c.prepare(&format!(
                "SELECT {ATTEMPT_COLS} FROM attempt
                 WHERE state = 'terminal' AND retired = 0 AND (?1 IS NULL OR worker_id = ?1)
                   {open}
                 ORDER BY terminal_at_ms, rowid LIMIT ?2"
            ))?;
            let records: Vec<AttemptRecord> = s
                .query_map(params![worker, limit], |r| Ok(attempt_from_row(r)))?
                .map(|r| r?)
                .collect::<Result<_>>()?;
            let mut out = Vec::with_capacity(records.len());
            for record in records {
                let aid = record.attempt_id.to_string();
                let demand: Option<String> = c
                    .query_row(
                        "SELECT demand_json FROM placement WHERE attempt_id = ?1
                         ORDER BY placement_id LIMIT 1",
                        [&aid],
                        |r| r.get(0),
                    )
                    .optional()?;
                let payload = demand
                    .map(|d| from_json::<Value>("demand", &d))
                    .transpose()?
                    .and_then(|d| d.get("payload").cloned());
                out.push(Collectable {
                    payload,
                    settled_here: settled_here(c, &aid)?,
                    row: if collections {
                        load_row(c, &aid)?
                    } else {
                        None
                    },
                    record,
                });
            }
            Ok(out)
        })
    }

    pub fn collection(&self, attempt_id: &AttemptId) -> Result<Option<CollectionRow>> {
        let a = attempt_id.to_string();
        if !self.collections {
            return Ok(None);
        }
        self.db.call(move |c| load_row(c, &a))
    }

    /// The artifacts of an attempt that are verified and published here:
    /// the declared name and the digest it was published with.
    pub fn persisted_artifacts(&self, attempt_id: &AttemptId) -> Result<Vec<(String, Digest)>> {
        let a = attempt_id.to_string();
        self.db.call(move |c| {
            let mut s = c.prepare(
                "SELECT name, digest FROM artifact_receipt
                 WHERE attempt_id = ?1 AND persisted_at_ms IS NOT NULL ORDER BY name",
            )?;
            let rows = s
                .query_map([&a], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows.into_iter()
                .map(|(name, digest)| {
                    let digest = digest.ok_or_else(|| {
                        StorageError::Corrupt(format!("persisted artifact {name} has no digest"))
                    })?;
                    Ok((name, id("digest", Digest::parse(&digest))?))
                })
                .collect()
        })
    }

    /// Record the result that is collected from now on, with its digest and
    /// the artifacts the parent declared (`name`, required). The same digest
    /// again changes nothing. Another digest starts over: the logs are read
    /// again from where they are and the verdict is open; receipts of
    /// artifacts that were published stay (an artifact is immutable). A
    /// closed collection is not touched. `refs` (the files of the result
    /// store this collection writes) are referenced for GC in the same
    /// commit: no result is recorded whose files GC does not know. Returns
    /// whether this call recorded a (new) result.
    pub fn begin_collection(
        &self,
        attempt_id: &AttemptId,
        result_digest: &Digest,
        result: &Value,
        declared: &[(String, bool)],
        refs: &[ResultRef],
        at_ms: u64,
    ) -> Result<bool> {
        let refs = refs.to_vec();
        let (a, digest, text) = (
            attempt_id.to_string(),
            result_digest.clone(),
            result.to_string(),
        );
        let declared = declared.to_vec();
        self.db.write(move |tx| {
            let core = common::read_core(tx, ATTEMPT, &a)?;
            if !core.state.is_terminal() {
                return Err(StorageError::IllegalState {
                    attempt: a,
                    state: core.state,
                    why: "only an ended attempt is collected",
                });
            }
            let row = ensure_row(tx, &a, at_ms)?;
            if row.closed_at_ms.is_some() {
                return Ok(false);
            }
            for r in &refs {
                add_ref(tx, &a, r, at_ms)?;
            }
            if row.result_digest.as_ref() == Some(&digest) {
                return Ok(false);
            }
            let restarted = row.result_digest.is_some();
            tx.execute(
                "UPDATE collection SET result_digest = ?1, result_json = ?2, logs_json = NULL,
                    verdict = NULL, updated_at_ms = ?3 WHERE attempt_id = ?4",
                params![digest.as_str(), text, to_i64(at_ms)?, a],
            )?;
            for (name, required) in &declared {
                tx.execute(
                    "INSERT OR IGNORE INTO artifact_receipt (attempt_id, name, required)
                     VALUES (?1,?2,?3)",
                    params![a, name, required],
                )?;
            }
            let run = run_of(tx, &a)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collection_started",
                &json!({"result_digest": digest.as_str(), "restarted": restarted}),
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// The logs of the result with `result_digest` were read to their end.
    /// Returns whether the open collection is of that result.
    pub fn set_collection_logs(
        &self,
        attempt_id: &AttemptId,
        result_digest: &Digest,
        logs: &Value,
        at_ms: u64,
    ) -> Result<bool> {
        let (a, digest, text) = (
            attempt_id.to_string(),
            result_digest.clone(),
            logs.to_string(),
        );
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE collection SET logs_json = ?1, updated_at_ms = ?2
                 WHERE attempt_id = ?3 AND result_digest = ?4 AND closed_at_ms IS NULL",
                params![text, to_i64(at_ms)?, a, digest.as_str()],
            )?;
            Ok(n == 1)
        })
    }

    /// An artifact is verified and published in the result store: its
    /// receipt and the GC reference of its file, in one commit. Verified
    /// progress: the strikes of the open collection start over.
    pub fn record_collected_artifact(
        &self,
        receipt: &ArtifactReceipt,
        file: &ResultRef,
        at_ms: u64,
    ) -> Result<()> {
        let (receipt, file) = (receipt.clone(), file.clone());
        self.db.write(move |tx| {
            let a = receipt.attempt_id.to_string();
            add_ref(tx, &a, &file, at_ms)?;
            Self::record_artifact_in(tx, &receipt, at_ms)?;
            clear_strikes(tx, &a)
        })
    }

    /// What the open collection recorded is not (or no longer) in the
    /// result store as recorded: take the record back, so that it is
    /// collected again. `logs`: the logs are read again from their start;
    /// `artifacts`: these names are fetched again. Nothing is taken back
    /// from a closed collection. Returns whether the collection is open.
    pub fn recollect(
        &self,
        attempt_id: &AttemptId,
        logs: bool,
        artifacts: &[String],
        at_ms: u64,
    ) -> Result<bool> {
        let (a, names) = (attempt_id.to_string(), artifacts.to_vec());
        self.db.write(move |tx| {
            let row = ensure_row(tx, &a, at_ms)?;
            if row.closed_at_ms.is_some() {
                return Ok(false);
            }
            if logs {
                tx.execute(
                    "UPDATE collection SET logs_json = NULL, updated_at_ms = ?1
                     WHERE attempt_id = ?2",
                    params![to_i64(at_ms)?, a],
                )?;
            }
            for name in &names {
                tx.execute(
                    "UPDATE artifact_receipt SET persisted_at_ms = NULL
                     WHERE attempt_id = ?1 AND name = ?2",
                    params![a, name],
                )?;
            }
            let run = run_of(tx, &a)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collection_recollect",
                &json!({"logs": logs, "artifacts": names}),
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// Open a collection that was given up again, from its start: the
    /// result is read again, and everything that is still in the result
    /// store is verified again before it counts. Only a collection that was
    /// given up is reopened: one that the worker acknowledged, or that was
    /// closed without a worker, has nothing left to fetch.
    ///
    /// What this can still get depends on the worker: it keeps a result
    /// that was never acknowledged for its `ack_wait_ms` after the end and
    /// may delete it then. Reopened after that, the collection is answered
    /// `NOT_FOUND` and is given up again.
    ///
    /// The retention of what was collected is measured from the close; the
    /// failure that started it is taken back with the close. Returns whether
    /// this call reopened it.
    pub fn reopen_collection(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<bool> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let Some(row) = load_row(tx, &a)? else {
                return Ok(false);
            };
            if row.closed_at_ms.is_none() || row.verdict != Some(Verdict::GaveUp) {
                return Ok(false);
            }
            tx.execute(
                "UPDATE collection SET result_digest = NULL, result_json = NULL,
                    logs_json = NULL, verdict = NULL, acked_at_ms = NULL, closed_at_ms = NULL,
                    strikes = 0, updated_at_ms = ?1 WHERE attempt_id = ?2",
                params![to_i64(at_ms)?, a],
            )?;
            tx.execute(
                "UPDATE artifact_receipt SET persisted_at_ms = NULL WHERE attempt_id = ?1",
                [&a],
            )?;
            tx.execute(
                "UPDATE attempt SET collection_failed_at_ms = NULL WHERE attempt_id = ?1",
                [&a],
            )?;
            let run = run_of(tx, &a)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collection_reopened",
                &Value::Null,
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// A fault that counts toward giving up. Returns the strikes so far.
    pub fn strike_collection(&self, attempt_id: &AttemptId, why: &str, at_ms: u64) -> Result<u32> {
        let (a, why) = (attempt_id.to_string(), why.to_string());
        self.db.write(move |tx| {
            let row = ensure_row(tx, &a, at_ms)?;
            if row.closed_at_ms.is_some() {
                return Ok(row.strikes);
            }
            tx.execute(
                "UPDATE collection SET strikes = strikes + 1, updated_at_ms = ?1
                 WHERE attempt_id = ?2",
                params![to_i64(at_ms)?, a],
            )?;
            let run = run_of(tx, &a)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collection_fault",
                &json!({"why": why, "strikes": row.strikes + 1}),
                at_ms,
            )?;
            Ok(row.strikes + 1)
        })
    }

    /// Close the collection, in one commit with what follows from it:
    ///
    /// * `Complete`: the `collected` receipt is set. Refused unless every
    ///   required artifact has a persisted receipt.
    /// * `RequiredMissing`, `GaveUp`: the collection is recorded as failed
    ///   (GC measures the retention from now), and the attempt is never
    ///   `collected`.
    ///
    /// `result_digest` is the result the verdict is about; a collection that
    /// has moved on to another result is not closed (`GaveUp` needs none).
    /// `acked`: the worker acknowledged `ack_collected` for that digest.
    /// Returns whether this call closed it.
    pub fn close_collection(
        &self,
        attempt_id: &AttemptId,
        result_digest: Option<&Digest>,
        verdict: Verdict,
        acked: bool,
        at_ms: u64,
    ) -> Result<bool> {
        let (a, digest) = (attempt_id.to_string(), result_digest.cloned());
        self.db.write(move |tx| {
            let mut core = common::read_core(tx, ATTEMPT, &a)?;
            let illegal = |why| StorageError::IllegalState {
                attempt: a.clone(),
                state: core.state,
                why,
            };
            if !core.state.is_terminal() {
                return Err(illegal("only an ended attempt is collected"));
            }
            let row = ensure_row(tx, &a, at_ms)?;
            if row.closed_at_ms.is_some() {
                return Ok(false);
            }
            if verdict != Verdict::GaveUp && (digest.is_none() || row.result_digest != digest) {
                return Err(illegal("the collection is of another result"));
            }
            let at = to_i64(at_ms)?;
            let run = run_of(tx, &a)?;
            if verdict == Verdict::Complete {
                let (required, persisted) = artifact_counts(tx, &a)?;
                if Collection::derive(core.state, required, persisted, false)
                    != Collection::Collected
                {
                    return Err(illegal("required outputs are not all collected"));
                }
                if core.receipts.mark(Receipt::Collected, at_ms) {
                    tx.execute(
                        "UPDATE attempt SET receipts_json = ?1 WHERE attempt_id = ?2",
                        params![common::json(&core.receipts), a],
                    )?;
                    common::append_event(
                        tx,
                        run.as_deref(),
                        Some(&a),
                        "collected",
                        &Value::Null,
                        at_ms,
                    )?;
                }
            } else {
                let n = tx.execute(
                    "UPDATE attempt SET collection_failed_at_ms = ?1
                     WHERE attempt_id = ?2 AND collection_failed_at_ms IS NULL",
                    params![at, a],
                )?;
                if n == 1 {
                    common::append_event(
                        tx,
                        run.as_deref(),
                        Some(&a),
                        "collection_failed",
                        &json!({"why": verdict.as_str()}),
                        at_ms,
                    )?;
                }
            }
            tx.execute(
                "UPDATE collection SET verdict = ?1, acked_at_ms = ?2, closed_at_ms = ?3,
                    updated_at_ms = ?3 WHERE attempt_id = ?4",
                params![verdict.as_str(), acked.then_some(at), at, a],
            )?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collection_closed",
                &json!({"verdict": verdict.as_str(), "acked": acked}),
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// Where the run stands: each task by its current attempt (the one that
    /// is not retired) and that attempt's closed collection. `expected` is
    /// how many tasks the run was to have (its own tasks when `None`): a run
    /// that expected none is `no_work`, one with fewer tasks than expected
    /// is `missing`, and neither is a success.
    pub fn run_standing(&self, run_id: &RunId, expected: Option<u32>) -> Result<RunStanding> {
        let run = run_id.to_string();
        let (collections, task_cancel) = (self.collections, self.task_cancel);
        self.db.call(move |c| {
            let known: i64 =
                c.query_row("SELECT count(*) FROM run WHERE run_id = ?1", [&run], |r| {
                    r.get(0)
                })?;
            if known == 0 {
                return Err(not_found(format!("run {run}")));
            }
            // A file from before the cancel column has no cancelled task.
            let cancelled = if task_cancel {
                "cancel_requested_at_ms IS NOT NULL"
            } else {
                "0"
            };
            let mut s = c.prepare(&format!(
                "SELECT task_id, {cancelled} FROM task WHERE run_id = ?1 ORDER BY rowid"
            ))?;
            let rows = s
                .query_map([&run], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut tasks = Vec::with_capacity(rows.len());
            for (task, cancelled) in rows {
                let current: Option<String> = c
                    .query_row(
                        "SELECT attempt_id FROM attempt WHERE task_id = ?1 AND retired = 0
                         ORDER BY generation DESC LIMIT 1",
                        [&task],
                        |r| r.get(0),
                    )
                    .optional()?;
                let (outcome, verdict) = match &current {
                    None => (None, None),
                    Some(aid) => {
                        let rec = load_attempt(c, aid)?
                            .ok_or_else(|| not_found(format!("attempt {aid}")))?;
                        // A file from before collections has closed none.
                        let row = if collections { load_row(c, aid)? } else { None };
                        let closed = row
                            .filter(|row| row.closed_at_ms.is_some())
                            .and_then(|row| row.verdict);
                        (rec.termination.map(|t: Termination| t.outcome()), closed)
                    }
                };
                tasks.push((
                    id("task_id", TaskId::parse(&task))?,
                    task_standing(outcome, verdict, cancelled && current.is_none()),
                ));
            }
            let standings: Vec<TaskStanding> = tasks.iter().map(|(_, s)| *s).collect();
            let expected = expected.unwrap_or(u32::try_from(standings.len()).unwrap_or(u32::MAX));
            Ok(RunStanding {
                verdict: run_verdict(expected, &standings),
                tasks,
            })
        })
    }
}
