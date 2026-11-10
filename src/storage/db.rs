//! One SQLite connection on a dedicated thread, plus open-time schema
//! migration.

use super::policy::{plan_schema, Access, CompatTable, Found, Incompat, SchemaPlan};
use super::{schema, DbKind, Result, StorageError};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

struct Inner {
    tx: Option<mpsc::Sender<Job>>,
    join: Option<JoinHandle<()>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Closing the channel ends the thread loop, which closes the connection.
        drop(self.tx.take());
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// A handle to the database thread. Cloning shares the connection; the
/// connection closes when the last clone is dropped.
#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<Inner>>,
    path: Arc<PathBuf>,
}

/// How long a connection waits for a lock held by another connection.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

impl Db {
    /// Open (creating or migrating as the compatibility table allows) the
    /// database of `kind` at `path`.
    pub fn open(path: &Path, kind: DbKind, access: Access) -> Result<Db> {
        let steps = schema::steps(kind);
        open_with(
            path,
            kind.as_str(),
            &steps,
            schema::compat(steps.len()),
            access,
        )
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Run `f` on the database thread and wait for its result.
    pub fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (rtx, rrx) = mpsc::sync_channel(1);
        let job: Job = Box::new(move |conn| {
            let out =
                catch_unwind(AssertUnwindSafe(|| f(conn))).unwrap_or(Err(StorageError::Panicked));
            let _ = rtx.send(out);
        });
        {
            let inner = self.inner.lock().map_err(|_| StorageError::Closed)?;
            let tx = inner.tx.as_ref().ok_or(StorageError::Closed)?;
            tx.send(job).map_err(|_| StorageError::Closed)?;
        }
        rrx.recv().map_err(|_| StorageError::Closed)?
    }

    /// Run `f` inside one `BEGIN IMMEDIATE` transaction. It commits (and is
    /// durable) only when `f` returns `Ok`; an error or a panic rolls back.
    pub fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T> + Send + 'static,
    {
        self.call(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let out = f(&tx)?;
            tx.commit()?;
            Ok(out)
        })
    }
}

impl Db {
    /// Like [`Db::write`], for a call whose business verdict may be an error
    /// that must not undo the writes made before it (a remembered epoch). `f`
    /// returns the verdict as a value: `Ok(verdict)` commits and then yields
    /// the verdict, whatever it is. An `Err` from `f` itself (a storage fault)
    /// rolls back as in `write`.
    pub fn write_then<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<Result<T>> + Send + 'static,
    {
        self.call(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let verdict = f(&tx)?;
            tx.commit()?;
            verdict
        })
    }
}

fn spawn(conn: Connection, path: &Path) -> Db {
    let (tx, rx) = mpsc::channel::<Job>();
    let join = std::thread::Builder::new()
        .name("rtr-storage".into())
        .spawn(move || {
            let mut conn = conn;
            while let Ok(job) = rx.recv() {
                job(&mut conn);
            }
        })
        .expect("spawn the storage thread");
    Db {
        inner: Arc::new(Mutex::new(Inner {
            tx: Some(tx),
            join: Some(join),
        })),
        path: Arc::new(path.to_path_buf()),
    }
}

fn probe(conn: &Connection, expected_kind: &str) -> Result<Found> {
    let has = |name: &str| -> Result<bool> {
        Ok(conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = ?1",
            [name],
            |r| r.get::<_, i64>(0),
        )? > 0)
    };
    if !has("schema_meta")? {
        let tables: i64 = conn.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))?;
        return Ok(if tables == 0 {
            Found::Empty
        } else {
            Found::Foreign
        });
    }
    let meta = |key: &str| -> Result<Option<String>> {
        match conn.query_row("SELECT value FROM schema_meta WHERE key = ?1", [key], |r| {
            r.get::<_, String>(0)
        }) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    };
    let kind = meta("kind")?;
    let version = meta("schema_version")?.and_then(|v| v.parse::<u32>().ok());
    match (kind, version) {
        (Some(kind), Some(v)) => {
            if kind != expected_kind {
                return Err(StorageError::KindMismatch {
                    expected: expected_kind.to_string(),
                    found: kind,
                });
            }
            Ok(Found::Version(v))
        }
        _ => Ok(Found::Foreign),
    }
}

fn read_version(tx: &rusqlite::Transaction<'_>) -> Result<u32> {
    let v: String = tx.query_row(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'",
        [],
        |r| r.get(0),
    )?;
    v.parse()
        .map_err(|_| StorageError::Corrupt(format!("schema_version {v:?}")))
}

/// `open` with an explicit step list and table (tests use their own).
///
/// Several processes open one database at about the same moment (the agent
/// and the supervisors; the very first open creates the file). SQLite does
/// not apply the busy timeout to every step of that (switching to WAL, the
/// first read of a file another connection is creating, which can even look
/// foreign half-way), so a read-write open holds an exclusive file lock
/// (`<db>.init-lock`) while it sets the file up, and a `Busy` that still
/// escapes is retried with a fresh connection until the busy timeout has
/// passed. Nothing has been written when the setup fails: every step is its
/// own transaction.
pub(crate) fn open_with(
    path: &Path,
    kind: &str,
    steps: &[String],
    table: CompatTable,
    access: Access,
) -> Result<Db> {
    let started = std::time::Instant::now();
    let mut pause = Duration::from_millis(2);
    loop {
        match open_once(path, kind, steps, table, access) {
            Err(e) if e.is_retryable() && started.elapsed() < BUSY_TIMEOUT => {
                std::thread::sleep(pause);
                pause = (pause * 2).min(Duration::from_millis(50));
            }
            other => return other,
        }
    }
}

/// Block until this process holds the setup lock of the database at `path`.
/// It is released when the returned file is dropped or the process dies.
fn init_lock(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut name = path.as_os_str().to_owned();
    name.push(".init-lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(name))?;
    file.lock()?;
    Ok(file)
}

fn open_once(
    path: &Path,
    kind: &str,
    steps: &[String],
    table: CompatTable,
    access: Access,
) -> Result<Db> {
    // Held until the setup below is done (dropped on return).
    let _init = if access == Access::ReadWrite {
        Some(init_lock(path)?)
    } else {
        None
    };
    let flags = match access {
        Access::ReadOnly => OpenFlags::SQLITE_OPEN_READ_ONLY,
        Access::ReadWrite => OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    } | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if access == Access::ReadOnly && !path.exists() {
        return Err(StorageError::NotFound {
            what: format!("database {}", path.display()),
        });
    }
    if let Some(parent) = path.parent() {
        if access == Access::ReadWrite {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut conn = match Connection::open_with_flags(path, flags) {
        Ok(c) => c,
        Err(rusqlite::Error::SqliteFailure(e, _))
            if e.code == rusqlite::ErrorCode::NotADatabase =>
        {
            return Err(StorageError::Incompatible(Incompat::Foreign));
        }
        Err(e) => return Err(e.into()),
    };
    conn.busy_timeout(BUSY_TIMEOUT)?;
    // Nothing below writes until the plan says the file is ours.
    let found = match probe(&conn, kind) {
        Err(StorageError::Sqlite(rusqlite::Error::SqliteFailure(e, _)))
            if e.code == rusqlite::ErrorCode::NotADatabase =>
        {
            return Err(StorageError::Incompatible(Incompat::Foreign));
        }
        other => other?,
    };
    let plan = plan_schema(found, access, table).map_err(StorageError::Incompatible)?;
    if access == Access::ReadWrite {
        let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StorageError::Corrupt(format!("journal_mode is {mode}")));
        }
        conn.pragma_update(None, "synchronous", "FULL")?;
    }
    conn.pragma_update(None, "foreign_keys", "ON")?;
    let from = match plan {
        SchemaPlan::AsIs => table.current,
        SchemaPlan::Create => 0,
        SchemaPlan::Migrate { from } => from,
    };
    for (i, sql) in steps.iter().enumerate().skip(from as usize) {
        apply_step(&mut conn, kind, i as u32 + 1, sql)?;
    }
    Ok(spawn(conn, path))
}

/// One migration step, atomically with its version bump. It re-reads the
/// version inside the transaction, so a concurrent opener that already did the
/// step makes this a no-op.
fn apply_step(conn: &mut Connection, kind: &str, to: u32, sql: &str) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let exists: i64 = tx.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name = 'schema_meta'",
        [],
        |r| r.get(0),
    )?;
    let version = if exists == 0 { 0 } else { read_version(&tx)? };
    if version >= to {
        return Ok(());
    }
    if version + 1 != to {
        return Err(StorageError::Corrupt(format!(
            "migration to {to} found version {version}"
        )));
    }
    if version == 0 {
        tx.execute_batch("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")?;
        tx.execute("INSERT INTO schema_meta VALUES ('kind', ?1)", [kind])?;
        tx.execute("INSERT INTO schema_meta VALUES ('schema_version', '0')", [])?;
    }
    tx.execute_batch(sql)?;
    tx.execute(
        "UPDATE schema_meta SET value = ?1 WHERE key = 'schema_version'",
        [to.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(n: u32) -> String {
        format!("CREATE TABLE t{n} (x INTEGER);")
    }

    fn steps(n: u32) -> Vec<String> {
        (1..=n).map(step).collect()
    }

    fn table(n: usize) -> CompatTable {
        schema::compat(n)
    }

    fn version(db: &Db) -> String {
        db.call(|c| {
            Ok(c.query_row(
                "SELECT value FROM schema_meta WHERE key='schema_version'",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .unwrap()
    }

    fn tables(db: &Db) -> Vec<String> {
        db.call(|c| {
            let mut s = c.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 't%' ORDER BY name",
            )?;
            let rows = s.query_map([], |r| r.get::<_, String>(0))?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .unwrap()
    }

    #[test]
    fn creates_uses_wal_full_and_reopens_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        let db = open_with(&p, "coordinator", &steps(1), table(1), Access::ReadWrite).unwrap();
        assert_eq!(version(&db), "1");
        let (mode, sync): (String, i64) = db
            .call(|c| {
                Ok((
                    c.query_row("PRAGMA journal_mode", [], |r| r.get(0))?,
                    c.query_row("PRAGMA synchronous", [], |r| r.get(0))?,
                ))
            })
            .unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(sync, 2, "FULL");
        drop(db);
        for _ in 0..3 {
            let db = open_with(&p, "coordinator", &steps(1), table(1), Access::ReadWrite).unwrap();
            assert_eq!(version(&db), "1");
            assert_eq!(tables(&db), vec!["t1"]);
        }
    }

    #[test]
    fn migrates_one_step_at_a_time_and_repeats_as_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        drop(open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap());
        // The newer binary knows three steps and migrates 1 -> 3.
        let db = open_with(&p, "worker", &steps(3), table(3), Access::ReadWrite).unwrap();
        assert_eq!(version(&db), "3");
        assert_eq!(tables(&db), vec!["t1", "t2", "t3"]);
        drop(db);
        let db = open_with(&p, "worker", &steps(3), table(3), Access::ReadWrite).unwrap();
        assert_eq!(version(&db), "3");
    }

    #[test]
    fn a_failing_step_rolls_back_and_keeps_the_old_version() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        drop(open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap());
        let mut bad = steps(2);
        bad[1] = "CREATE TABLE t2 (x INTEGER); CREATE TABLE nope (".into();
        assert!(open_with(&p, "worker", &bad, table(2), Access::ReadWrite).is_err());
        let db = open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap();
        assert_eq!(version(&db), "1");
        assert_eq!(tables(&db), vec!["t1"]);
    }

    #[test]
    fn a_newer_database_is_refused_and_left_byte_for_byte_alone() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        drop(open_with(&p, "worker", &steps(3), table(3), Access::ReadWrite).unwrap());
        // Make it look like a default (rollback-journal) file so a stray write
        // would show up in the bytes.
        {
            let c = Connection::open(&p).unwrap();
            let _: String = c
                .query_row("PRAGMA journal_mode = DELETE", [], |r| r.get(0))
                .unwrap();
        }
        let before = std::fs::read(&p).unwrap();
        for access in [Access::ReadWrite, Access::ReadOnly] {
            // An older binary that knows two steps.
            let err = open_with(&p, "worker", &steps(2), table(2), access)
                .err()
                .expect("must refuse");
            assert!(
                matches!(
                    err,
                    StorageError::Incompatible(Incompat::Newer {
                        found: 3,
                        current: 2
                    })
                ),
                "{err:?}"
            );
            assert_eq!(err.code(), crate::error::ErrorCode::ProtocolIncompatible);
        }
        assert_eq!(std::fs::read(&p).unwrap(), before);
    }

    #[test]
    fn foreign_and_wrong_kind_files_are_refused_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.db");
        {
            let c = Connection::open(&p).unwrap();
            c.execute_batch("CREATE TABLE other (x)").unwrap();
        }
        let before = std::fs::read(&p).unwrap();
        let err = open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite)
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::Incompatible(Incompat::Foreign)));
        assert_eq!(std::fs::read(&p).unwrap(), before);

        let junk = dir.path().join("junk.db");
        std::fs::write(&junk, vec![b'x'; 4096]).unwrap();
        let err = open_with(&junk, "worker", &steps(1), table(1), Access::ReadWrite)
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::Incompatible(Incompat::Foreign)));

        let w = dir.path().join("w.db");
        drop(open_with(&w, "worker", &steps(1), table(1), Access::ReadWrite).unwrap());
        let err = open_with(&w, "coordinator", &steps(1), table(1), Access::ReadWrite)
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::KindMismatch { .. }), "{err:?}");
    }

    #[test]
    fn read_only_reads_without_migrating_and_needs_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        let err = open_with(&p, "worker", &steps(1), table(1), Access::ReadOnly)
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::NotFound { .. }));
        assert!(!p.exists());
        drop(open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap());
        // A binary that knows 2 steps reads the version-1 file without migrating it.
        let ro = open_with(&p, "worker", &steps(2), table(2), Access::ReadOnly).unwrap();
        assert_eq!(version(&ro), "1");
        assert!(ro
            .call(|c| Ok(c.execute("INSERT INTO schema_meta VALUES ('x','y')", [])?))
            .is_err());
    }

    #[test]
    fn dropping_a_transaction_before_commit_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.db");
        let db = open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap();
        // A "crash": the transaction goes away without commit.
        db.call(|c| {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute("INSERT INTO t1 VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
        // An error and a panic roll back too, and the thread survives.
        let e = db.write(|tx| {
            tx.execute("INSERT INTO t1 VALUES (2)", [])?;
            Err::<(), _>(StorageError::Invalid("boom".into()))
        });
        assert!(e.is_err());
        let e = db.write(|tx| -> Result<()> {
            tx.execute("INSERT INTO t1 VALUES (3)", [])?;
            panic!("inside");
        });
        assert!(matches!(e, Err(StorageError::Panicked)));
        db.write(|tx| Ok(tx.execute("INSERT INTO t1 VALUES (4)", [])?))
            .unwrap();
        drop(db);
        let db = open_with(&p, "worker", &steps(1), table(1), Access::ReadWrite).unwrap();
        let rows: Vec<i64> = db
            .call(|c| {
                let mut s = c.prepare("SELECT x FROM t1")?;
                let r = s.query_map([], |r| r.get(0))?;
                Ok(r.collect::<std::result::Result<_, _>>()?)
            })
            .unwrap();
        assert_eq!(rows, vec![4]);
    }
}
