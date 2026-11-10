//! Operations both databases share: attempt state changes, the terminal
//! write, the event journal and the references GC works on. They take the
//! attempt table's name (a compile-time constant) because the two databases
//! name it differently.

use super::policy::{decide_gc, GcCandidate, GcVerdict, KeepReason};
use super::{to_i64, to_u64, Db, Finalized, Result, StorageError};
use crate::state::{check_termination, AttemptState, Receipt, Receipts, Termination};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::sync::Arc;

/// One journal entry. `seq` only grows and is never reused.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub run_id: Option<String>,
    pub attempt_id: Option<String>,
    pub kind: String,
    pub payload: Value,
    pub at_ms: u64,
}

pub(crate) fn append_event(
    conn: &Connection,
    run_id: Option<&str>,
    attempt_id: Option<&str>,
    kind: &str,
    payload: &Value,
    at_ms: u64,
) -> Result<u64> {
    conn.execute(
        "INSERT INTO event (run_id, attempt_id, kind, payload_json, at_ms) VALUES (?1,?2,?3,?4,?5)",
        params![
            run_id,
            attempt_id,
            kind,
            payload.to_string(),
            to_i64(at_ms)?
        ],
    )?;
    to_u64(conn.last_insert_rowid())
}

pub(crate) fn events_after(conn: &Connection, after_seq: u64, limit: u32) -> Result<Vec<Event>> {
    let mut s = conn.prepare(
        "SELECT seq, run_id, attempt_id, kind, payload_json, at_ms FROM event
         WHERE seq > ?1 ORDER BY seq LIMIT ?2",
    )?;
    let rows = s.query_map(params![to_i64(after_seq)?, limit], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (seq, run_id, attempt_id, kind, payload, at) = row?;
        out.push(Event {
            seq: to_u64(seq)?,
            run_id,
            attempt_id,
            kind,
            payload: serde_json::from_str(&payload)
                .map_err(|e| StorageError::Corrupt(format!("event payload: {e}")))?,
            at_ms: to_u64(at)?,
        });
    }
    Ok(out)
}

pub(crate) fn state_str(s: AttemptState) -> &'static str {
    match s {
        AttemptState::Accepted => "accepted",
        AttemptState::Preparing => "preparing",
        AttemptState::Ready => "ready",
        AttemptState::Starting => "starting",
        AttemptState::Active => "active",
        AttemptState::Cancelling => "cancelling",
        AttemptState::Finalizing => "finalizing",
        AttemptState::Terminal => "terminal",
    }
}

pub(crate) fn parse_state(s: &str) -> Result<AttemptState> {
    AttemptState::ALL
        .into_iter()
        .find(|st| state_str(*st) == s)
        .ok_or_else(|| StorageError::Corrupt(format!("attempt state {s:?}")))
}

pub(crate) fn json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("plain data serializes")
}

pub(crate) fn from_json<T: serde::de::DeserializeOwned>(what: &str, s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| StorageError::Corrupt(format!("{what}: {e}")))
}

/// The columns both attempt tables share.
#[derive(Debug, Clone)]
pub(crate) struct Core {
    pub state: AttemptState,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
}

pub(crate) fn read_core(conn: &Connection, table: &str, attempt_id: &str) -> Result<Core> {
    let row = conn
        .query_row(
            &format!(
                "SELECT state, termination_json, receipts_json
                 FROM {table} WHERE attempt_id = ?1"
            ),
            [attempt_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| StorageError::NotFound {
            what: format!("attempt {attempt_id}"),
        })?;
    let state = parse_state(&row.0)?;
    let termination: Option<Termination> = row
        .1
        .as_deref()
        .map(|s| from_json("termination", s))
        .transpose()?;
    check_termination(state, termination.as_ref())
        .map_err(|e| StorageError::Corrupt(e.to_string()))?;
    Ok(Core {
        state,
        termination,
        receipts: from_json("receipts", &row.2)?,
    })
}

/// A state change the state machine allows, as a conditional update. Never
/// reaches `Terminal`; use [`finalize`].
pub(crate) fn transition(
    conn: &Connection,
    table: &str,
    attempt_id: &str,
    to: AttemptState,
    at_ms: u64,
    run_id: Option<&str>,
) -> Result<AttemptState> {
    let core = read_core(conn, table, attempt_id)?;
    core.state.transition(to)?;
    let n = conn.execute(
        &format!("UPDATE {table} SET state = ?1 WHERE attempt_id = ?2 AND state = ?3"),
        params![state_str(to), attempt_id, state_str(core.state)],
    )?;
    if n != 1 {
        return Err(StorageError::IllegalState {
            attempt: attempt_id.to_string(),
            state: core.state,
            why: "changed concurrently",
        });
    }
    append_event(
        conn,
        run_id,
        Some(attempt_id),
        "attempt_state",
        &serde_json::json!({"from": state_str(core.state), "to": state_str(to)}),
        at_ms,
    )?;
    Ok(core.state)
}

/// Fix the termination exactly once. `Finalizing -> Terminal` is a
/// conditional update; if the attempt is already terminal the earlier
/// termination is returned and nothing changes.
pub(crate) fn finalize(
    conn: &Connection,
    table: &str,
    attempt_id: &str,
    termination: Termination,
    at_ms: u64,
    run_id: Option<&str>,
) -> Result<Finalized> {
    let core = read_core(conn, table, attempt_id)?;
    if let Some(existing) = core.termination {
        return Ok(Finalized::AlreadyTerminal(existing));
    }
    let (_, term) = core.state.finalize(termination)?;
    let mut receipts = core.receipts;
    receipts.mark(Receipt::Terminal, at_ms);
    let n = conn.execute(
        &format!(
            "UPDATE {table} SET state = 'terminal', termination_json = ?1, terminal_at_ms = ?2,
                    receipts_json = ?3
             WHERE attempt_id = ?4 AND state = 'finalizing' AND termination_json IS NULL"
        ),
        params![json(&term), to_i64(at_ms)?, json(&receipts), attempt_id],
    )?;
    if n != 1 {
        return Err(StorageError::IllegalState {
            attempt: attempt_id.to_string(),
            state: core.state,
            why: "changed concurrently",
        });
    }
    append_event(
        conn,
        run_id,
        Some(attempt_id),
        "attempt_terminal",
        &serde_json::to_value(term).expect("termination serializes"),
        at_ms,
    )?;
    Ok(Finalized::Won(term))
}

/// Record that `receipt` was reached; the first time wins. `Terminal` and
/// `Collected` have their own paths and are refused here.
pub(crate) fn mark_receipt(
    conn: &Connection,
    table: &str,
    attempt_id: &str,
    receipt: Receipt,
    at_ms: u64,
) -> Result<bool> {
    if matches!(receipt, Receipt::Terminal | Receipt::Collected) {
        return Err(StorageError::Invalid(format!(
            "{receipt:?} is set by finalize or collection, not directly"
        )));
    }
    let mut core = read_core(conn, table, attempt_id)?;
    let fresh = core.receipts.mark(receipt, at_ms);
    if fresh {
        conn.execute(
            &format!("UPDATE {table} SET receipts_json = ?1 WHERE attempt_id = ?2"),
            params![json(&core.receipts), attempt_id],
        )?;
    }
    Ok(fresh)
}

/// What a reference holds, for GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobKind {
    Input,
    Log,
    Result,
}

impl BlobKind {
    fn as_str(self) -> &'static str {
        match self {
            BlobKind::Input => "input",
            BlobKind::Log => "log",
            BlobKind::Result => "result",
        }
    }

    fn parse(s: &str) -> Result<Self> {
        match s {
            "input" => Ok(BlobKind::Input),
            "log" => Ok(BlobKind::Log),
            "result" => Ok(BlobKind::Result),
            other => Err(StorageError::Corrupt(format!("blob kind {other:?}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBlobRef {
    pub ref_id: String,
    pub attempt_id: String,
    pub kind: BlobKind,
    /// Opaque to storage: where the file layer put the bytes.
    pub locator: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    pub ref_id: String,
    pub attempt_id: String,
    pub kind: BlobKind,
    pub locator: String,
    pub pins: u32,
}

/// What a GC pass did. Storage drops the references and queues files whose
/// last reference went away in `pending_delete`; the caller deletes those
/// files and then calls `complete_delete` for each. `removed` is informational:
/// the list to act on is `pending_deletes()`, which also survives a crash or a
/// pass that stopped early.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GcReport {
    pub removed: Vec<BlobRef>,
    pub kept: Vec<(String, KeepReason)>,
    /// Locators queued for deletion by this pass (no reference names them any
    /// more).
    pub queued: Vec<String>,
}

/// References one GC transaction examines, so a large backlog never holds the
/// write lock for long.
pub const DEFAULT_GC_BATCH: u32 = 200;

pub(crate) fn add_blob_ref(conn: &Connection, r: &NewBlobRef, at_ms: u64) -> Result<()> {
    let queued: i64 = conn.query_row(
        "SELECT count(*) FROM pending_delete WHERE locator = ?1",
        [&r.locator],
        |row| row.get(0),
    )?;
    if queued > 0 {
        return Err(StorageError::LocatorPendingDelete(r.locator.clone()));
    }
    conn.execute(
        "INSERT INTO blob_ref (ref_id, attempt_id, kind, locator, created_at_ms)
         VALUES (?1,?2,?3,?4,?5)",
        params![
            r.ref_id,
            r.attempt_id,
            r.kind.as_str(),
            r.locator,
            to_i64(at_ms)?
        ],
    )?;
    Ok(())
}

pub(crate) fn pin(conn: &Connection, ref_id: &str, delta: i64) -> Result<u32> {
    let n = conn.execute(
        "UPDATE blob_ref SET pins = pins + ?1 WHERE ref_id = ?2 AND pins + ?1 >= 0",
        params![delta, ref_id],
    )?;
    if n != 1 {
        return Err(StorageError::NotFound {
            what: format!("reference {ref_id} (or pins would go negative)"),
        });
    }
    Ok(conn.query_row(
        "SELECT pins FROM blob_ref WHERE ref_id = ?1",
        [ref_id],
        |r| r.get(0),
    )?)
}

/// The locators waiting for their file to be deleted, oldest first.
pub(crate) fn pending_deletes(conn: &Connection) -> Result<Vec<String>> {
    let mut s =
        conn.prepare("SELECT locator FROM pending_delete ORDER BY queued_at_ms, locator")?;
    let rows = s.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

/// The file of `locator` is gone: forget it. Idempotent; returns whether a
/// row was removed.
pub(crate) fn complete_delete(conn: &Connection, locator: &str) -> Result<bool> {
    Ok(conn.execute("DELETE FROM pending_delete WHERE locator = ?1", [locator])? == 1)
}

/// Select up to `batch` references after `after` and drop the collectable
/// ones, in the caller's transaction. A locator that no reference names after
/// that is queued in `pending_delete` in the same transaction, so "last
/// reference gone" and "file may be deleted" are one atomic fact. `facts`
/// yields the owner's [`GcCandidate`] for a reference. Returns the report and
/// the last reference id examined (`None` when the table is exhausted).
pub(crate) fn gc_batch(
    conn: &Connection,
    now_ms: u64,
    retention_ms: u64,
    ack_wait_ms: Option<u64>,
    after: &str,
    batch: u32,
    facts: &impl Fn(&Connection, &BlobRef) -> Result<GcCandidate>,
) -> Result<(GcReport, Option<String>)> {
    let refs: Vec<BlobRef> = {
        let mut s = conn.prepare(
            "SELECT ref_id, attempt_id, kind, locator, pins FROM blob_ref
             WHERE ref_id > ?1 ORDER BY ref_id LIMIT ?2",
        )?;
        let rows = s.query_map(params![after, batch], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, u32>(4)?,
            ))
        })?;
        let mut v = Vec::new();
        for row in rows {
            let (ref_id, attempt_id, kind, locator, pins) = row?;
            v.push(BlobRef {
                ref_id,
                attempt_id,
                kind: BlobKind::parse(&kind)?,
                locator,
                pins,
            });
        }
        v
    };
    let last = if refs.len() as u64 == u64::from(batch) {
        refs.last().map(|r| r.ref_id.clone())
    } else {
        None
    };
    let mut report = GcReport::default();
    for r in refs {
        let candidate = facts(conn, &r)?;
        match decide_gc(&candidate, now_ms, retention_ms, ack_wait_ms) {
            GcVerdict::Collect => {
                conn.execute("DELETE FROM blob_ref WHERE ref_id = ?1", [&r.ref_id])?;
                let remaining: i64 = conn.query_row(
                    "SELECT count(*) FROM blob_ref WHERE locator = ?1",
                    [&r.locator],
                    |row| row.get(0),
                )?;
                if remaining == 0 {
                    let n = conn.execute(
                        "INSERT OR IGNORE INTO pending_delete (locator, queued_at_ms)
                         VALUES (?1, ?2)",
                        params![r.locator, to_i64(now_ms)?],
                    )?;
                    if n == 1 {
                        report.queued.push(r.locator.clone());
                    }
                }
                report.removed.push(r);
            }
            GcVerdict::Keep(why) => report.kept.push((r.ref_id, why)),
        }
    }
    Ok((report, last))
}

/// Run GC as a series of transactions of at most `batch` references each. A
/// failure (a busy database is [`StorageError::Busy`], retryable) stops the
/// pass: the batches already committed stay committed and their files are in
/// `pending_delete`, so a later pass or `pending_deletes()` picks them up.
pub(crate) fn gc_all<F>(
    db: &Db,
    now_ms: u64,
    retention_ms: u64,
    ack_wait_ms: Option<u64>,
    batch: u32,
    facts: F,
) -> Result<GcReport>
where
    F: Fn(&Connection, &BlobRef) -> Result<GcCandidate> + Send + Sync + 'static,
{
    if batch == 0 {
        return Err(StorageError::Invalid("gc batch must be positive".into()));
    }
    let facts = Arc::new(facts);
    let mut total = GcReport::default();
    let mut after = String::new();
    loop {
        let (f, a) = (Arc::clone(&facts), after.clone());
        let (rep, last) =
            db.write(move |tx| gc_batch(tx, now_ms, retention_ms, ack_wait_ms, &a, batch, &*f))?;
        total.removed.extend(rep.removed);
        total.kept.extend(rep.kept);
        total.queued.extend(rep.queued);
        match last {
            Some(l) => after = l,
            None => return Ok(total),
        }
    }
}
