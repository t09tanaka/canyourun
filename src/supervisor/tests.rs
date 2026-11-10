use super::outcome::{
    drain_pending_deletes, finish_attempt, is_duplicate, store_artifact, sweep_tmp,
    sync_log_position, ArtifactInput, OutcomeError,
};
use super::spool::{AttemptSpool, SpoolError, SpoolReader, StreamStatus};
use super::spool_plan::{logs_locator, MAX_READ_BYTES, MIN_RECORD_BYTES};
use crate::ids::{
    AttemptId, CoordinatorEpoch, Digest, Generation, RunId, SourceId, TaskId, WorkerId,
};
use crate::state::{AttemptState, Outcome, Termination};
use crate::storage::{NewAcceptance, StorageError, SubmitOutcome, WorkerStore};
use crate::wire::LogStream::{Stderr, Stdout};
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use tempfile::TempDir;

const BIG: u64 = 1 << 30;

fn spool(limit: u64) -> (TempDir, AttemptId, AttemptSpool) {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    let s = AttemptSpool::open(dir.path(), &a, limit).unwrap();
    (dir, a, s)
}

/// Bytes that depend on the absolute offset and are mostly not UTF-8.
fn pattern(from: u64, len: u64) -> Vec<u8> {
    (from..from + len)
        .map(|i| (i.wrapping_mul(31).wrapping_add(7) % 251) as u8 | (0x80 * u8::from(i % 3 == 0)))
        .collect()
}

fn read_all(r: &SpoolReader, stream: crate::wire::LogStream) -> Vec<u8> {
    let (mut out, mut off) = (Vec::new(), 0);
    loop {
        let c = r.read(stream, off, 1000).unwrap();
        assert_eq!(c.offset, off);
        out.extend_from_slice(&c.data);
        if c.data.is_empty() {
            return out;
        }
        off = c.next_offset;
    }
}

#[test]
fn non_utf8_bytes_round_trip_per_stream() {
    let (dir, a, s) = spool(BIG);
    let all: Vec<u8> = (0..=255u8).collect();
    assert!(String::from_utf8(all.clone()).is_err());
    s.writer(Stdout).append(&all).unwrap();
    s.writer(Stdout).append(&[0xff, 0xfe, 0x00, b'\n']).unwrap();
    s.writer(Stderr).append(&[0xc3]).unwrap();
    s.writer(Stderr).append(&[0x28, 0xe2, 0x82]).unwrap();
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    let mut want = all;
    want.extend_from_slice(&[0xff, 0xfe, 0x00, b'\n']);
    assert_eq!(read_all(&r, Stdout), want);
    assert_eq!(read_all(&r, Stderr), [0xc3, 0x28, 0xe2, 0x82]);
    let c = r.read(Stdout, 0, 10).unwrap();
    // Small appends share records: one per stream here.
    assert_eq!((c.next_offset, c.seq, c.eof), (10, 1, false));
    let c = r.read(Stdout, 260, 10).unwrap();
    assert_eq!((c.data.len(), c.next_offset, c.eof), (0, 260, true));
}

#[test]
fn reads_resume_from_any_offset_without_duplicates() {
    let (dir, a, s) = spool(BIG);
    let w = s.writer(Stdout);
    for i in 0..10u64 {
        w.append(&pattern(i * 100, 100)).unwrap();
    }
    w.sync().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    let c = r.read(Stdout, 250, 100).unwrap();
    assert_eq!(c.data, pattern(250, 100));
    assert_eq!(c.next_offset, 350);
    // Past the end: nothing, at the end.
    let c = r.read(Stdout, 5000, 100).unwrap();
    assert_eq!((c.data.len(), c.offset, c.next_offset), (0, 1000, 1000));
    // Not finished, so not eof even at the end.
    assert!(!c.eof);
    // A huge max_bytes is capped, not allocated.
    let c = r.read(Stdout, 0, u32::MAX).unwrap();
    assert_eq!(c.data.len() as u64, 1000.min(MAX_READ_BYTES));
}

#[test]
fn limit_is_shared_and_the_rest_is_counted_as_dropped() {
    let (dir, a, s) = spool(10);
    assert_eq!(s.writer(Stdout).append(b"123456").unwrap().end, 6);
    // Only 4 bytes of room are left for both streams.
    let st = s.writer(Stderr).append(b"abcdef").unwrap();
    assert_eq!((st.end, st.dropped, st.truncated()), (4, 2, true));
    let st = s.writer(Stdout).append(b"zzz").unwrap();
    assert_eq!((st.end, st.dropped), (6, 3));
    // The pipe keeps being drained; only counters move.
    for _ in 0..5 {
        s.writer(Stdout).append(&[0u8; 100]).unwrap();
    }
    let (out, err) = s.finish().unwrap();
    assert_eq!((out.end, out.dropped), (6, 503));
    assert_eq!((err.end, err.dropped), (4, 2));
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), b"123456");
    assert_eq!(read_all(&r, Stderr), b"abcd");
    let c = r.read(Stdout, 6, 10).unwrap();
    assert!(c.eof && c.log_truncated);
    assert_eq!(c.dropped_bytes, 503);
}

#[test]
fn a_limit_hit_exactly_is_not_truncation() {
    let (_d, _a, s) = spool(4);
    let st = s.writer(Stdout).append(b"abcd").unwrap();
    assert!(!st.truncated());
    assert!(s.writer(Stdout).append(b"e").unwrap().truncated());
}

#[test]
fn a_zero_limit_keeps_nothing_but_records_the_loss() {
    let (dir, a, s) = spool(0);
    s.writer(Stdout).append(b"hello").unwrap();
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    let c = r.read(Stdout, 0, 10).unwrap();
    assert_eq!(
        (c.data.len(), c.dropped_bytes, c.log_truncated),
        (0, 5, true)
    );
}

#[test]
fn truncation_is_visible_to_a_reader_before_the_end() {
    let (dir, a, s) = spool(3);
    s.writer(Stdout).append(b"abcdef").unwrap();
    // No finish yet: the first drop is already durable in the index.
    let st = SpoolReader::open(dir.path(), &a)
        .unwrap()
        .status(Stdout)
        .unwrap();
    assert_eq!((st.end, st.dropped, st.seq), (3, 3, 1));
    // Further drops are batched; finish writes the exact count.
    s.writer(Stdout).append(b"gh").unwrap();
    let st = SpoolReader::open(dir.path(), &a)
        .unwrap()
        .status(Stdout)
        .unwrap();
    assert_eq!((st.dropped, st.seq), (3, 1));
    s.finish().unwrap();
    let st = SpoolReader::open(dir.path(), &a)
        .unwrap()
        .status(Stdout)
        .unwrap();
    assert_eq!((st.dropped, st.seq), (5, 2));
}

#[test]
fn many_drops_do_not_grow_the_index_per_append() {
    let (dir, a, s) = spool(1);
    let chunk = vec![1u8; 4096];
    for _ in 0..1000 {
        s.writer(Stdout).append(&chunk).unwrap();
    }
    let idx = fs::metadata(dir.path().join(a.as_str()).join("logs/stdout.idx")).unwrap();
    // 4 MB dropped: a few records, not one per append.
    assert!(idx.len() <= 16 * 6, "{}", idx.len());
}

#[test]
fn sequence_counts_index_records_not_appends() {
    let (dir, a, s) = spool(BIG);
    let w = s.writer(Stdout);
    assert_eq!(w.append(b"").unwrap().seq, 0);
    // Small appends wait for a record, but are stored at once.
    assert_eq!(w.append(b"a").unwrap().seq, 0);
    assert_eq!(w.append(b"bc").unwrap().end, 3);
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(r.status(Stdout).unwrap(), StreamStatus::default());
    w.sync().unwrap();
    let want = StreamStatus {
        end: 3,
        dropped: 0,
        seq: 1,
    };
    assert_eq!(w.status(), want);
    assert_eq!(r.status(Stdout).unwrap(), want);
    // Nothing new: no record.
    w.sync().unwrap();
    assert_eq!(w.status(), want);
    // A big enough append publishes by itself (the log is fsynced first).
    let big = MIN_RECORD_BYTES as usize;
    let st = w.append(&vec![7u8; big]).unwrap();
    assert_eq!((st.end, st.seq), (3 + big as u64, 2));
    assert_eq!(r.status(Stdout).unwrap().end, 3 + big as u64);
    assert_eq!(s.writer(Stderr).status(), StreamStatus::default());
}

#[test]
fn tiny_writes_do_not_grow_the_index_per_append() {
    let (dir, a, s) = spool(BIG);
    let w = s.writer(Stdout);
    let n = MIN_RECORD_BYTES as usize + 1000;
    for _ in 0..n {
        w.append(b"x").unwrap();
    }
    s.finish().unwrap();
    let idx = fs::metadata(dir.path().join(a.as_str()).join("logs/stdout.idx")).unwrap();
    // A million one-byte writes: one record per MIN_RECORD_BYTES and one at
    // the end, not one per write.
    assert_eq!(idx.len(), 16 * 2, "{}", idx.len());
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), vec![b'x'; n]);
}

#[test]
fn resume_after_a_crash_continues_without_rewinding_or_repeating() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    let first = {
        let s = AttemptSpool::open(dir.path(), &a, 100).unwrap();
        s.writer(Stdout).append(&pattern(0, 40)).unwrap();
        s.writer(Stderr).append(&pattern(0, 10)).unwrap();
        s.writer(Stdout).append(&pattern(40, 20)).unwrap();
        s.writer(Stdout).sync().unwrap();
        s.writer(Stderr).sync().unwrap();
        s.writer(Stdout).status()
        // dropped without finish: the supervisor died
    };
    assert_eq!(
        first,
        StreamStatus {
            end: 60,
            dropped: 0,
            seq: 1
        }
    );
    let s = AttemptSpool::open(dir.path(), &a, 100).unwrap();
    assert_eq!(s.writer(Stdout).status(), first);
    // The limit still counts what is already stored: 70 used, 30 left.
    let st = s.writer(Stdout).append(&pattern(60, 50)).unwrap();
    assert_eq!((st.end, st.dropped, st.seq), (90, 20, 2));
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), pattern(0, 90));
    assert_eq!(read_all(&r, Stderr), pattern(0, 10));
    // A finished spool cannot be reopened for writing, but can be read.
    drop(s);
    assert!(matches!(
        AttemptSpool::open(dir.path(), &a, 100),
        Err(SpoolError::Finished)
    ));
}

#[test]
fn resume_repairs_a_torn_index_and_unindexed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    {
        let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
        s.writer(Stdout).append(&pattern(0, 8)).unwrap();
        s.writer(Stdout).sync().unwrap();
        s.writer(Stdout).append(&pattern(8, 8)).unwrap();
        s.writer(Stdout).sync().unwrap();
    }
    let logs = dir.path().join(a.as_str()).join("logs");
    // The crash hit between the log write and the index write, and tore the
    // next index record.
    let mut log = fs::OpenOptions::new()
        .append(true)
        .open(logs.join("stdout.log"))
        .unwrap();
    log.write_all(&pattern(16, 5)).unwrap();
    let mut idx = fs::OpenOptions::new()
        .append(true)
        .open(logs.join("stdout.idx"))
        .unwrap();
    idx.write_all(&[9, 9, 9, 9, 9]).unwrap();
    // A reader meanwhile trusts only the whole records.
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(r.status(Stdout).unwrap().end, 16);
    let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
    let st = s.writer(Stdout).status();
    assert_eq!((st.end, st.seq), (21, 3));
    assert_eq!(fs::metadata(logs.join("stdout.idx")).unwrap().len(), 3 * 16);
    s.writer(Stdout).append(&pattern(21, 4)).unwrap();
    s.finish().unwrap();
    assert_eq!(read_all(&r, Stdout), pattern(0, 25));
}

#[test]
fn resume_drops_index_records_the_log_cannot_back() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    {
        let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
        s.writer(Stdout).append(&pattern(0, 8)).unwrap();
        s.writer(Stdout).sync().unwrap();
        s.writer(Stdout).append(&pattern(8, 8)).unwrap();
        s.writer(Stdout).sync().unwrap();
    }
    let logs = dir.path().join(a.as_str()).join("logs");
    // Power loss: the index reached the disk, the end of the log did not.
    fs::OpenOptions::new()
        .write(true)
        .open(logs.join("stdout.log"))
        .unwrap()
        .set_len(11)
        .unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(r.status(Stdout).unwrap().end, 11, "never beyond the log");
    let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
    assert_eq!(s.writer(Stdout).status().end, 11);
    s.writer(Stdout).append(&pattern(11, 5)).unwrap();
    s.finish().unwrap();
    assert_eq!(read_all(&r, Stdout), pattern(0, 16));
}

#[test]
fn only_one_writer_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
    assert!(matches!(
        AttemptSpool::open(dir.path(), &a, BIG),
        Err(SpoolError::Locked(_))
    ));
    drop(s);
    AttemptSpool::open(dir.path(), &a, BIG).unwrap();
}

#[test]
fn appends_after_finish_are_refused_and_finish_repeats() {
    let (_d, _a, s) = spool(BIG);
    s.writer(Stdout).append(b"x").unwrap();
    let first = s.finish().unwrap();
    assert_eq!(s.finish().unwrap(), first);
    assert!(matches!(
        s.writer(Stdout).append(b"y"),
        Err(SpoolError::Finished)
    ));
    assert!(matches!(
        s.writer(Stderr).append(b"y"),
        Err(SpoolError::Finished)
    ));
}

#[test]
fn a_reader_without_a_spool_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        SpoolReader::open(dir.path(), &AttemptId::generate()),
        Err(SpoolError::NotFound(_))
    ));
}

#[test]
fn two_threads_share_the_limit_exactly() {
    let (dir, a, s) = spool(10_000);
    std::thread::scope(|sc| {
        for stream in [Stdout, Stderr] {
            let s = &s;
            sc.spawn(move || {
                for i in 0..500u64 {
                    s.writer(stream).append(&pattern(i * 40, 40)).unwrap();
                }
            });
        }
    });
    let (o, e) = s.finish().unwrap();
    assert_eq!(o.end + e.end, 10_000);
    assert_eq!(o.dropped + e.dropped, 40_000 - 10_000);
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    // Each stream is a clean prefix of what it wrote.
    assert_eq!(read_all(&r, Stdout), pattern(0, o.end));
    assert_eq!(read_all(&r, Stderr), pattern(0, e.end));
}

// ------------------------------------------------- separate processes

/// The body of the child process of the tests below: append numbered chunks
/// forever (until killed) to the spool named by `RTR_SPOOL_ROOT` /
/// `RTR_SPOOL_ATTEMPT`. Does nothing in a normal run.
#[test]
fn child_writer_main() {
    let (Ok(root), Ok(attempt)) = (
        std::env::var("RTR_SPOOL_ROOT"),
        std::env::var("RTR_SPOOL_ATTEMPT"),
    ) else {
        return;
    };
    let attempt = AttemptId::parse(&attempt).unwrap();
    let s = AttemptSpool::open(Path::new(&root), &attempt, BIG).unwrap();
    let w = s.writer(Stdout);
    println!("ready");
    std::io::stdout().flush().unwrap();
    loop {
        let at = w.status().end;
        w.append(&pattern(at, 997)).unwrap();
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
}

fn spawn_child(root: &Path, a: &AttemptId) -> std::process::Child {
    let mut c = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "supervisor::tests::child_writer_main",
            "--nocapture",
        ])
        .env("RTR_SPOOL_ROOT", root)
        .env("RTR_SPOOL_ATTEMPT", a.as_str())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    // Wait until the child holds the spool.
    let mut out = std::io::BufReader::new(c.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert!(std::io::BufRead::read_line(&mut out, &mut line).unwrap() > 0);
        if line.trim() == "ready" {
            break;
        }
    }
    std::mem::forget(out);
    c
}

#[test]
fn another_process_writes_while_this_one_reads_then_crashes_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    let mut child = spawn_child(dir.path(), &a);
    // The writer lives in the other process: this one cannot take the spool.
    assert!(matches!(
        AttemptSpool::open(dir.path(), &a, BIG),
        Err(SpoolError::Locked(_))
    ));
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    let (mut got, mut off, mut last_seq) = (Vec::new(), 0u64, 0u64);
    while got.len() < 200_000 {
        let c = r.read(Stdout, off, 4096).unwrap();
        assert_eq!(c.offset, off, "the offset never moves back");
        assert!(c.seq >= last_seq);
        last_seq = c.seq;
        got.extend_from_slice(&c.data);
        off = c.next_offset;
        assert!(!c.eof);
    }
    assert_eq!(
        got,
        pattern(0, got.len() as u64),
        "a clean prefix, no tearing"
    );
    // Kill the writer without any chance to clean up.
    child.kill().unwrap();
    child.wait().unwrap();
    let seen = r.status(Stdout).unwrap();
    assert!(seen.end >= off);
    // A new supervisor resumes: nothing lost that was published, nothing repeated.
    let s = AttemptSpool::open(dir.path(), &a, BIG).unwrap();
    let st = s.writer(Stdout).status();
    assert!(st.end >= seen.end, "{st:?} vs {seen:?}");
    assert!(st.seq >= seen.seq);
    let after = st.end;
    s.writer(Stdout).append(&pattern(after, 123)).unwrap();
    s.finish().unwrap();
    let all = read_all(&r, Stdout);
    assert_eq!(all.len() as u64, after + 123);
    assert_eq!(all, pattern(0, after + 123));
    assert!(r.read(Stdout, after + 123, 10).unwrap().eof);
}

// ------------------------------------------------------ results, GC

struct Wk {
    _dir: TempDir,
    root: std::path::PathBuf,
    /// Where the commands run: the artifacts' sources are below it.
    ws: std::path::PathBuf,
    store: WorkerStore,
}

fn wk() -> Wk {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store = WorkerStore::open(&state, &WorkerId::parse("mini-a").unwrap()).unwrap();
    store.start_agent(1).unwrap();
    let root = dir.path().join("spool");
    let ws = dir.path().join("ws");
    fs::create_dir_all(&ws).unwrap();
    fs::create_dir_all(&root).unwrap();
    Wk {
        _dir: dir,
        root,
        ws,
        store,
    }
}

fn accept(w: &Wk) -> AttemptId {
    let task = TaskId::generate();
    let a = AttemptId::generate();
    let run = RunId::parse(&format!("run_{}", &task.as_str()["task_".len()..])).unwrap();
    match w
        .store
        .submit(
            NewAcceptance {
                attempt_id: a.clone(),
                run_id: run,
                task_id: task,
                generation: Generation(1),
                payload_hash: Digest::of_bytes(b"p"),
                coordinator_epoch: CoordinatorEpoch(1),
                spec: json!({"cmd": "true"}),
            },
            10,
        )
        .unwrap()
    {
        SubmitOutcome::Accepted(_) => {}
        other => panic!("{other:?}"),
    }
    for to in [AttemptState::Preparing, AttemptState::Finalizing] {
        w.store.transition(&a, to, 11).unwrap();
    }
    a
}

fn input<'a>(name: &'a str, src: &'a Path) -> ArtifactInput<'a> {
    ArtifactInput {
        name,
        source: src,
        source_id: SourceId::generate(),
        plan_digest: Digest::of_bytes(b"plan"),
        profile_fingerprint: Digest::of_bytes(b"prof"),
    }
}

#[test]
fn artifacts_are_copied_hashed_and_kept_apart_per_attempt() {
    let w = wk();
    let src = w.ws.join("work");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("out.bin"), [0xffu8, 0, 1, 2]).unwrap();
    let (a1, a2) = (AttemptId::generate(), AttemptId::generate());
    let r1 = store_artifact(
        &w.root,
        &a1,
        &w.ws,
        &input("d/out.bin", &src.join("out.bin")),
        100,
    )
    .unwrap();
    assert_eq!((r1.size, r1.path.as_str()), (4, "d/out.bin"));
    assert_eq!(r1.digest, Digest::of_bytes(&[0xff, 0, 1, 2]));
    assert_eq!(r1.attempt_id, a1);
    let stored = w.root.join(a1.as_str()).join("artifacts/d/out.bin");
    assert_eq!(fs::read(&stored).unwrap(), [0xff, 0, 1, 2]);
    // The same name and content again is a replay.
    store_artifact(
        &w.root,
        &a1,
        &w.ws,
        &input("d/out.bin", &src.join("out.bin")),
        100,
    )
    .unwrap();
    // Another content under the same name is refused and the first is kept.
    fs::write(src.join("other.bin"), b"different").unwrap();
    let e = store_artifact(
        &w.root,
        &a1,
        &w.ws,
        &input("d/out.bin", &src.join("other.bin")),
        100,
    );
    assert!(matches!(e, Err(OutcomeError::Conflict(_))), "{e:?}");
    assert_eq!(fs::read(&stored).unwrap(), [0xff, 0, 1, 2]);
    // A new attempt has its own store; the old result is untouched.
    store_artifact(
        &w.root,
        &a2,
        &w.ws,
        &input("d/out.bin", &src.join("other.bin")),
        100,
    )
    .unwrap();
    assert_eq!(fs::read(&stored).unwrap(), [0xff, 0, 1, 2]);
    // No temporary file is left next to the artifact.
    let names: Vec<_> = fs::read_dir(stored.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names.len(), 1);
}

#[test]
fn bad_artifacts_are_refused_before_anything_is_kept() {
    let w = wk();
    let a = AttemptId::generate();
    let src = w.ws.join("work");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("f"), b"12345").unwrap();
    for bad in ["../x", "/abs", "", "a//b", ".tmpx"] {
        let e = store_artifact(&w.root, &a, &w.ws, &input(bad, &src.join("f")), 100);
        assert!(matches!(e, Err(OutcomeError::BadName(_))), "{bad}: {e:?}");
    }
    let e = store_artifact(&w.root, &a, &w.ws, &input("big", &src.join("f")), 4);
    assert!(
        matches!(e, Err(OutcomeError::TooLarge { max: 4, .. })),
        "{e:?}"
    );
    let e = store_artifact(&w.root, &a, &w.ws, &input("dir", &src), 100);
    assert!(matches!(e, Err(OutcomeError::NotRegular(_))));
    std::os::unix::fs::symlink(src.join("f"), src.join("link")).unwrap();
    let e = store_artifact(&w.root, &a, &w.ws, &input("l", &src.join("link")), 100);
    assert!(matches!(e, Err(OutcomeError::NotRegular(_))));
    let e = store_artifact(&w.root, &a, &w.ws, &input("gone", &src.join("nope")), 100);
    assert!(matches!(e, Err(OutcomeError::Io(_))));
    // The too-large one left no partial file behind.
    let art = w.root.join(a.as_str()).join("artifacts");
    let leftovers: Vec<_> = walk(&art);
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd {
            let p = e.unwrap().path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

fn cancelled() -> Termination {
    Termination::new(Outcome::Cancelled, None, None).unwrap()
}

#[test]
fn finish_attempt_stores_the_result_once_and_records_truncation() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, 9).unwrap();
    s.writer(Stderr).append(&[0xff]).unwrap();
    s.writer(Stdout).append(b"0123456789").unwrap();
    let src = w.ws.join("f");
    fs::create_dir_all(&w.root).unwrap();
    fs::write(&src, b"art").unwrap();
    let art = store_artifact(&w.root, &a, &w.ws, &input("r.txt", &src), 10).unwrap();
    let rec = finish_attempt(
        &w.store,
        &w.root,
        &a,
        &s,
        cancelled(),
        std::slice::from_ref(&art),
        50,
    )
    .unwrap();
    assert!(rec.fresh);
    assert_eq!(rec.result["termination"]["outcome"], "cancelled");
    assert_eq!(rec.result["logs"]["stdout"]["bytes"], 8);
    assert_eq!(rec.result["logs"]["stdout"]["dropped_bytes"], 2);
    assert_eq!(rec.result["logs"]["stdout"]["log_truncated"], true);
    assert_eq!(rec.result["logs"]["stderr"]["log_truncated"], false);
    assert_eq!(rec.result["artifacts"][0]["name"], "r.txt");
    assert_eq!(w.store.get_result(&a).unwrap().unwrap(), rec.result);
    let pos = w.store.log_position(&a, "stdout").unwrap();
    assert_eq!(
        (pos.written_bytes, pos.dropped_bytes, pos.truncated()),
        (8, 2, true)
    );
    let ev = w.store.events_after(0, 100).unwrap();
    let trunc: Vec<_> = ev.iter().filter(|e| e.kind == "log_truncated").collect();
    assert_eq!(trunc.len(), 1);
    assert_eq!(trunc[0].payload["stream"], "stdout");
    assert_eq!(trunc[0].payload["dropped_bytes"], 2);
    // A replay (a retry after a crash) returns the same result and no new event.
    let again = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[art], 60).unwrap();
    assert!(!again.fresh);
    assert_eq!(again.result, rec.result);
    let ev = w.store.events_after(0, 100).unwrap();
    assert_eq!(ev.iter().filter(|e| e.kind == "log_truncated").count(), 1);
}

#[test]
fn a_different_second_result_is_refused() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stdout).append(b"x").unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 5).unwrap();
    // Artifacts that were not in the first result cannot be added later.
    let src = w.ws.join("f");
    fs::write(&src, b"late").unwrap();
    let late = store_artifact(&w.root, &a, &w.ws, &input("late", &src), 10).unwrap();
    let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[late], 6);
    assert!(matches!(e, Err(OutcomeError::Storage(_))), "{e:?}");
}

#[test]
fn artifacts_of_another_attempt_or_with_a_bad_path_are_refused() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let src = w.ws.join("f");
    fs::write(&src, b"x").unwrap();
    let other = store_artifact(
        &w.root,
        &AttemptId::generate(),
        &w.ws,
        &input("o", &src),
        10,
    )
    .unwrap();
    let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[other], 5);
    assert!(matches!(e, Err(OutcomeError::BadName(_))), "{e:?}");
    let mut bad = store_artifact(&w.root, &a, &w.ws, &input("ok", &src), 10).unwrap();
    bad.path = "../escape".into();
    let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[bad], 5);
    assert!(matches!(e, Err(OutcomeError::BadName(_))), "{e:?}");
    // Nothing was fixed by the refused calls.
    assert!(w.store.get_result(&a).unwrap().is_none());
}

#[test]
fn gc_queues_the_files_and_only_the_drain_deletes_them() {
    let w = wk();
    let a = accept(&w);
    let keep = accept(&w);
    for id in [&a, &keep] {
        let s = AttemptSpool::open(&w.root, id, BIG).unwrap();
        s.writer(Stdout).append(b"log").unwrap();
        let src = w.ws.join("f");
        fs::write(&src, b"art").unwrap();
        let art = store_artifact(&w.root, id, &w.ws, &input("a/b.txt", &src), 10).unwrap();
        finish_attempt(&w.store, &w.root, id, &s, cancelled(), &[art], 100).unwrap();
    }
    let logs = w.root.join(a.as_str()).join("logs");
    let art = w.root.join(a.as_str()).join("artifacts/a/b.txt");
    assert!(logs.is_dir() && art.is_file());
    // Acknowledged and past retention: both of `a`'s files are queued; `keep`
    // is pinned by the pin below.
    w.store.ack_collected(&a, CoordinatorEpoch(1), 200).unwrap();
    w.store.pin_blob(&format!("{keep}:logs")).unwrap();
    w.store
        .pin_blob(&format!("{keep}:artifact:a/b.txt"))
        .unwrap();
    let report = w.store.gc(1_000_000, 0, 0).unwrap();
    assert!(!report.removed.is_empty());
    let queued = w.store.pending_deletes().unwrap();
    assert!(queued.contains(&logs_locator(&a)));
    // Queued is not deleted: the files are still there until the drain.
    assert!(logs.is_dir() && art.is_file());
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    assert_eq!((d.deleted, d.foreign.len()), (2, 0));
    assert!(!logs.exists() && !art.exists());
    assert!(w.store.pending_deletes().unwrap().is_empty());
    // The pinned attempt's files stay.
    assert!(w.root.join(keep.as_str()).join("logs").is_dir());
    // A second drain is a no-op.
    assert_eq!(drain_pending_deletes(&w.store, &w.root).unwrap().deleted, 0);
}

#[test]
fn the_drain_never_touches_what_is_not_ours() {
    let w = wk();
    let a = accept(&w);
    fs::create_dir_all(&w.root).unwrap();
    let outside = w.root.join("precious");
    fs::write(&outside, b"keep").unwrap();
    // A reference whose locator escapes the root is queued by GC like any
    // other (storage treats it as opaque); the drain must refuse it.
    w.store
        .add_blob_ref(
            &crate::storage::NewBlobRef {
                ref_id: "evil".into(),
                attempt_id: a.to_string(),
                kind: crate::storage::BlobKind::Log,
                locator: "../precious".into(),
            },
            1,
        )
        .unwrap();
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 2).unwrap();
    w.store.ack_collected(&a, CoordinatorEpoch(1), 3).unwrap();
    w.store.gc(1_000_000, 0, 0).unwrap();
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    assert_eq!(d.foreign, ["../precious"]);
    assert!(outside.exists());
    // It stays queued, so the problem is visible, not silently dropped.
    assert_eq!(w.store.pending_deletes().unwrap(), ["../precious"]);
}

#[test]
fn artifacts_never_go_through_a_symlink_out_of_the_area() {
    let w = wk();
    let a = AttemptId::generate();
    let outside = w.root.parent().unwrap().join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("secret"), b"s3").unwrap();
    let src = w.ws.join("f");
    fs::create_dir_all(&w.root).unwrap();
    fs::write(&src, b"art").unwrap();
    let symlink = std::os::unix::fs::symlink;
    let is_refused = |r: Result<_, OutcomeError>| matches!(r, Err(OutcomeError::Symlink(_)));
    let listing = || {
        let mut v: Vec<_> = fs::read_dir(&outside)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        v.sort();
        v
    };

    // The attempt directory itself is a symlink.
    symlink(&outside, w.root.join(a.as_str())).unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
        &w.ws,
        &input("x", &src),
        10
    )));
    assert_eq!(listing(), ["secret"]);
    fs::remove_file(w.root.join(a.as_str())).unwrap();

    // `artifacts` is a symlink.
    fs::create_dir_all(w.root.join(a.as_str())).unwrap();
    symlink(&outside, w.root.join(a.as_str()).join("artifacts")).unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
        &w.ws,
        &input("x", &src),
        10
    )));
    assert_eq!(listing(), ["secret"]);
    fs::remove_file(w.root.join(a.as_str()).join("artifacts")).unwrap();

    // A subdirectory of the artifacts is a symlink.
    let arts = w.root.join(a.as_str()).join("artifacts");
    fs::create_dir_all(&arts).unwrap();
    symlink(&outside, arts.join("d")).unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
        &w.ws,
        &input("d/x", &src),
        10
    )));
    assert_eq!(listing(), ["secret"]);

    // The destination is a symlink to a file with the same content: the
    // replay check must not read through it.
    fs::write(outside.join("same"), b"art").unwrap();
    symlink(&outside.join("same"), arts.join("y")).unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
        &w.ws,
        &input("y", &src),
        10
    )));
    assert_eq!(fs::read(outside.join("same")).unwrap(), b"art");

    // A plain file where a directory is needed is refused too.
    fs::write(arts.join("file"), b"f").unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
        &w.ws,
        &input("file/x", &src),
        10
    )));
}

#[test]
fn the_drain_does_not_delete_through_a_symlink() {
    let w = wk();
    let a = accept(&w);
    let outside = w.root.parent().unwrap().join("outside2");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("precious"), b"keep").unwrap();
    fs::create_dir_all(w.root.join(a.as_str())).unwrap();
    std::os::unix::fs::symlink(&outside, w.root.join(a.as_str()).join("artifacts")).unwrap();
    let locator = format!("{a}/artifacts/precious");
    w.store
        .add_blob_ref(
            &crate::storage::NewBlobRef {
                ref_id: "r".into(),
                attempt_id: a.to_string(),
                kind: crate::storage::BlobKind::Result,
                locator: locator.clone(),
            },
            1,
        )
        .unwrap();
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 2).unwrap();
    w.store.ack_collected(&a, CoordinatorEpoch(1), 3).unwrap();
    w.store.gc(1_000_000, 0, 0).unwrap();
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    assert!(d.foreign.contains(&locator), "{d:?}");
    assert!(outside.join("precious").exists());
}

#[test]
fn a_failed_spool_is_reported_as_such_and_nothing_is_recorded() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stdout).append(b"x").unwrap();
    s.writer(Stdout).poison();
    assert!(matches!(
        s.writer(Stdout).append(b"y"),
        Err(SpoolError::Failed)
    ));
    assert!(matches!(s.writer(Stdout).sync(), Err(SpoolError::Failed)));
    let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 5);
    assert!(
        matches!(e, Err(OutcomeError::SpoolFailed(SpoolError::Failed))),
        "{e:?}"
    );
    // The attempt's result is left to the caller: nothing was stored or fixed.
    assert!(w.store.get_result(&a).unwrap().is_none());
    assert!(!w.store.get(&a).unwrap().unwrap().state.is_terminal());
}

#[test]
fn a_crash_between_result_and_event_is_repaired_by_the_next_call() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, 3).unwrap();
    s.writer(Stdout).append(b"abcdef").unwrap();
    // What finish_attempt does up to storing the result, then the crash.
    let (out, err) = s.finish().unwrap();
    w.store
        .update_log_position(&a, "stdout", out.into())
        .unwrap();
    w.store
        .update_log_position(&a, "stderr", err.into())
        .unwrap();
    let fixed = w.store.finalize(&a, cancelled(), 9).unwrap().termination();
    let sum = |s: StreamStatus| super::spool_plan::StreamSummary {
        bytes: s.end,
        dropped_bytes: s.dropped,
        last_seq: s.seq,
    };
    let result = super::spool_plan::result_json(&fixed, &[], sum(out), sum(err));
    w.store.record_result(&a, &result).unwrap();
    let count = |w: &Wk| {
        w.store
            .events_after(0, 100)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "log_truncated")
            .count()
    };
    assert_eq!(count(&w), 0);
    let r = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 10).unwrap();
    assert!(!r.fresh);
    assert_eq!(count(&w), 1);
    // Idempotent.
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 11).unwrap();
    assert_eq!(count(&w), 1);
}

// ---------------------------------------------------------------- review fixes

/// A small deterministic generator (xorshift64*), so the property tests are
/// the same on every run and every machine.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn bytes(&mut self, n: u64) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

fn logs_path(root: &Path, a: &AttemptId, file: &str) -> std::path::PathBuf {
    root.join(a.as_str()).join("logs").join(file)
}

#[test]
fn a_stream_that_dropped_keeps_dropping_after_a_resume_with_a_larger_limit() {
    let dir = tempfile::tempdir().unwrap();
    let a = AttemptId::generate();
    {
        let s = AttemptSpool::open(dir.path(), &a, 5).unwrap();
        s.writer(Stdout).append(b"0123456789").unwrap();
        // crash: the first drop was published at once
    }
    let s = AttemptSpool::open(dir.path(), &a, 1000).unwrap();
    let st = s.writer(Stdout).append(b"more").unwrap();
    assert_eq!((st.end, st.dropped), (5, 5 + 4));
    // The other stream never dropped: it may use the room the new limit gives.
    let st = s.writer(Stderr).append(b"err").unwrap();
    assert_eq!((st.end, st.dropped), (3, 0));
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), b"01234");
    assert_eq!(r.status(Stdout).unwrap().dropped, 9);
}

#[test]
fn the_heartbeat_stores_the_positions_and_they_only_move_forward() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stdout).append(b"abc").unwrap();
    s.writer(Stderr).append(b"de").unwrap();
    assert_eq!(w.store.log_position(&a, "stdout").unwrap().written_bytes, 0);
    let (o, e) = sync_log_position(&w.store, &a, &s).unwrap();
    assert_eq!((o.written_bytes, o.last_seq), (3, 1));
    assert_eq!((e.written_bytes, e.last_seq), (2, 1));
    assert_eq!(w.store.log_position(&a, "stdout").unwrap(), o);
    // Nothing new: the same positions. More output: they advance.
    assert_eq!(sync_log_position(&w.store, &a, &s).unwrap(), (o, e));
    s.writer(Stdout).append(b"fg").unwrap();
    let (o2, _) = sync_log_position(&w.store, &a, &s).unwrap();
    assert_eq!((o2.written_bytes, o2.last_seq), (5, 2));
    // The database never goes back, whatever an older call says.
    let back = w
        .store
        .update_log_position(&a, "stdout", StreamStatus::default().into())
        .unwrap();
    assert_eq!(back, o2);
    // A failed spool is an error, not a stale position.
    s.writer(Stdout).poison();
    assert!(matches!(
        sync_log_position(&w.store, &a, &s),
        Err(OutcomeError::Spool(SpoolError::Failed))
    ));
}

#[test]
fn only_a_primary_key_clash_counts_as_a_replayed_reference() {
    use crate::storage::StorageError;
    let failure = |extended: i32| {
        StorageError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::ConstraintViolation,
                extended_code: extended,
            },
            None,
        ))
    };
    assert!(is_duplicate(&failure(
        rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )));
    for other in [
        rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY,
        rusqlite::ffi::SQLITE_CONSTRAINT_CHECK,
        rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL,
    ] {
        assert!(!is_duplicate(&failure(other)), "{other}");
    }
    assert!(!is_duplicate(&StorageError::Closed));
    assert!(!is_duplicate(&StorageError::Sqlite(
        rusqlite::Error::QueryReturnedNoRows
    )));
}

fn art(w: &Wk, a: &AttemptId, name: &str, content: &[u8]) -> crate::wire::ArtifactRecord {
    let src = w.ws.join(format!("src-{}", name.replace('/', "_")));
    fs::write(&src, content).unwrap();
    store_artifact(&w.root, a, &w.ws, &input(name, &src), 1000).unwrap()
}

#[test]
fn a_replay_with_other_artifacts_is_refused_before_any_reference_is_added() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let first = art(&w, &a, "one", b"1");
    let r = finish_attempt(
        &w.store,
        &w.root,
        &a,
        &s,
        cancelled(),
        std::slice::from_ref(&first),
        5,
    )
    .unwrap();
    assert!(r.fresh);
    let late = art(&w, &a, "late", b"2");
    for other in [vec![first.clone(), late.clone()], vec![], vec![late]] {
        let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &other, 6);
        assert!(
            matches!(e, Err(OutcomeError::Storage(StorageError::Immutable(_)))),
            "{e:?}"
        );
    }
    // No reference was added for the refused artifact; the stored result is intact.
    assert!(w.store.pin_blob(&format!("{a}:artifact:late")).is_err());
    assert_eq!(w.store.get_result(&a).unwrap().unwrap(), r.result);
    // The order of the same set does not matter, and a replay does not look at
    // the files again.
    fs::remove_file(w.root.join(a.as_str()).join("artifacts/one")).unwrap();
    let again = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[first], 7).unwrap();
    assert!(!again.fresh);
}

#[test]
fn clashing_artifact_names_are_refused() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let go = |names: &[&str]| {
        // The same content: a file system that folds names keeps one file for both.
        let recs: Vec<_> = names.iter().map(|n| art(&w, &a, n, b"same")).collect();
        finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &recs, 5)
    };
    // The same path twice.
    let one = art(&w, &a, "dup", b"d");
    let e = finish_attempt(
        &w.store,
        &w.root,
        &a,
        &s,
        cancelled(),
        &[one.clone(), one],
        5,
    );
    assert!(matches!(e, Err(OutcomeError::NameClash(n)) if n == "dup"));
    // Names a case-folding or normalizing file system stores as one file.
    for pair in [["Readme", "README"], ["caf\u{e9}", "cafe\u{301}"]] {
        let e = go(&pair);
        assert!(
            matches!(e, Err(OutcomeError::NameClash(_))),
            "{pair:?}: {e:?}"
        );
    }
    // A name below another artifact (the store itself cannot make this one, so
    // the record is forged).
    let d = art(&w, &a, "d", b"d");
    let mut below = d.clone();
    below.name = "d/x".into();
    below.path = "d/x".into();
    let e = finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[d, below], 5);
    assert!(
        matches!(&e, Err(OutcomeError::NameClash(n)) if n == "d/x"),
        "{e:?}"
    );
    // Nothing was fixed by the refused calls; distinct names pass.
    assert!(w.store.get_result(&a).unwrap().is_none());
    assert!(go(&["p", "q/r", "p2"]).unwrap().fresh);
}

#[test]
fn recorded_artifacts_are_checked_against_the_files() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let rec = art(&w, &a, "d/f", b"content");
    let stored = w.root.join(a.as_str()).join("artifacts/d/f");
    let run = |r: &crate::wire::ArtifactRecord| {
        finish_attempt(
            &w.store,
            &w.root,
            &a,
            &s,
            cancelled(),
            std::slice::from_ref(r),
            5,
        )
    };
    let changed = |e: Result<_, OutcomeError>| matches!(e, Err(OutcomeError::ArtifactChanged(_)));
    // A record that lies about its size or digest.
    let mut bad = rec.clone();
    bad.size += 1;
    assert!(changed(run(&bad)));
    let mut bad = rec.clone();
    bad.digest = Digest::of_bytes(b"other");
    assert!(changed(run(&bad)));
    // Content changed on disk, same size.
    fs::write(&stored, b"CONTENT").unwrap();
    assert!(changed(run(&rec)));
    // The file is gone, or its directory is.
    fs::remove_file(&stored).unwrap();
    assert!(changed(run(&rec)));
    fs::remove_dir(stored.parent().unwrap()).unwrap();
    assert!(changed(run(&rec)));
    // Nothing was fixed; with the file back, it goes through.
    assert!(w.store.get_result(&a).unwrap().is_none());
    fs::create_dir(stored.parent().unwrap()).unwrap();
    fs::write(&stored, b"content").unwrap();
    assert!(run(&rec).unwrap().fresh);
    // A directory on the way that is a symlink is not followed.
    let w2 = wk();
    let a2 = accept(&w2);
    let s2 = AttemptSpool::open(&w2.root, &a2, BIG).unwrap();
    let rec2 = art(&w2, &a2, "d/f", b"content");
    let dir2 = w2.root.join(a2.as_str()).join("artifacts/d");
    let moved = w2.root.join("moved");
    fs::rename(&dir2, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &dir2).unwrap();
    let e = finish_attempt(&w2.store, &w2.root, &a2, &s2, cancelled(), &[rec2], 5);
    assert!(matches!(e, Err(OutcomeError::Symlink(_))), "{e:?}");
}

#[test]
fn a_source_is_taken_only_from_below_the_workspace_and_never_through_a_link() {
    let w = wk();
    let a = AttemptId::generate();
    let outside = w.root.parent().unwrap().join("elsewhere");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("x"), b"secret").unwrap();
    std::os::unix::fs::symlink(&outside, w.ws.join("out")).unwrap();
    let go = |name: &str, src: &Path| store_artifact(&w.root, &a, &w.ws, &input(name, src), 100);
    let refused = |r: Result<_, OutcomeError>| matches!(r, Err(OutcomeError::BadSource(_)));
    // A parent that is a symlink to a directory outside: absolute and relative.
    assert!(refused(go("n", &w.ws.join("out/x"))));
    assert!(refused(go("n", Path::new("out/x"))));
    // Outside the workspace altogether, or climbing out of it.
    assert!(refused(go("n", &outside.join("x"))));
    assert!(refused(go("n", &w.ws.join("../elsewhere/x"))));
    assert!(refused(go("n", Path::new("../elsewhere/x"))));
    assert!(refused(go("n", &w.ws)));
    // A parent that is a plain file.
    fs::write(w.ws.join("file"), b"f").unwrap();
    assert!(refused(go("n", &w.ws.join("file/x"))));
    // Nothing was kept.
    assert!(walk(&w.root.join(a.as_str())).is_empty());
    // A file inside a real subdirectory works, absolute or relative; a
    // workspace reached through a symlink is fine (it is trusted).
    fs::create_dir(w.ws.join("real")).unwrap();
    fs::write(w.ws.join("real/y"), b"ok").unwrap();
    assert_eq!(go("a", &w.ws.join("real/y")).unwrap().size, 2);
    assert_eq!(go("b", Path::new("real/y")).unwrap().size, 2);
    let via = w.root.parent().unwrap().join("ws-link");
    std::os::unix::fs::symlink(&w.ws, &via).unwrap();
    let r = store_artifact(&w.root, &a, &via, &input("c", &via.join("real/y")), 100);
    assert_eq!(r.unwrap().size, 2);
}

#[test]
fn leftover_temporary_files_are_swept_and_the_drain_removes_empty_directories() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let rec = art(&w, &a, "d/e/f", b"x");
    let arts = w.root.join(a.as_str()).join("artifacts");
    // What a crashed copy leaves.
    fs::write(arts.join(".tmpAAA"), b"half").unwrap();
    fs::write(arts.join("d/.tmpBBB"), b"half").unwrap();
    fs::write(arts.join("d/e/.tmpCCC"), b"half").unwrap();
    assert_eq!(sweep_tmp(&w.root, &a).unwrap(), 3);
    assert_eq!(walk(&arts), [arts.join("d/e/f")]);
    // finish_attempt sweeps too.
    fs::write(arts.join("d/.tmpDDD"), b"half").unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[rec], 5).unwrap();
    assert_eq!(walk(&arts), [arts.join("d/e/f")]);
    // No artifacts directory, or one that is not a directory: nothing to do.
    assert_eq!(sweep_tmp(&w.root, &AttemptId::generate()).unwrap(), 0);
    // The drain takes the files and then the directories above them, the
    // attempt's own included, even with a leftover in one of them.
    fs::write(arts.join("d/.tmpEEE"), b"half").unwrap();
    w.store.ack_collected(&a, CoordinatorEpoch(1), 6).unwrap();
    w.store.gc(1_000_000, 0, 0).unwrap();
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    assert_eq!((d.deleted, d.failed.len()), (2, 0));
    assert!(!w.root.join(a.as_str()).exists(), "{:?}", walk(&w.root));
    assert!(w.root.exists());
}

#[test]
fn one_file_that_cannot_be_deleted_does_not_stop_the_drain() {
    use std::os::unix::fs::PermissionsExt;
    let w = wk();
    let (a, b) = (accept(&w), accept(&w));
    let mut recs = vec![];
    for id in [&a, &b] {
        let s = AttemptSpool::open(&w.root, id, BIG).unwrap();
        let rec = art(&w, id, "d/f", b"x");
        finish_attempt(&w.store, &w.root, id, &s, cancelled(), &[rec], 5).unwrap();
        recs.push(rec_dir(&w, id));
    }
    for id in [&a, &b] {
        w.store.ack_collected(id, CoordinatorEpoch(1), 6).unwrap();
    }
    w.store.gc(1_000_000, 0, 0).unwrap();
    // `a`'s directory cannot be written to: its file cannot be unlinked.
    fs::set_permissions(&recs[0], fs::Permissions::from_mode(0o500)).unwrap();
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    fs::set_permissions(&recs[0], fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(d.failed.len(), 1, "{d:?}");
    assert_eq!(d.failed[0].0, format!("{a}/artifacts/d/f"));
    assert!(d.deleted >= 3, "{d:?}");
    // The failed one is still queued and the next pass completes it.
    assert_eq!(
        w.store.pending_deletes().unwrap(),
        [format!("{a}/artifacts/d/f")]
    );
    let d = drain_pending_deletes(&w.store, &w.root).unwrap();
    assert_eq!((d.deleted, d.failed.len()), (1, 0));
    assert!(w.store.pending_deletes().unwrap().is_empty());
}

fn rec_dir(w: &Wk, a: &AttemptId) -> std::path::PathBuf {
    w.root.join(a.as_str()).join("artifacts/d")
}

#[test]
fn truncation_events_carry_the_run_and_a_gapless_attempt_sequence() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, 2).unwrap();
    s.writer(Stdout).append(b"abc").unwrap();
    s.writer(Stderr).append(b"def").unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 5).unwrap();
    finish_attempt(&w.store, &w.root, &a, &s, cancelled(), &[], 6).unwrap();
    let run = w.store.get(&a).unwrap().unwrap().run_id;
    let ev = w.store.events_of(&a, "log_truncated").unwrap();
    assert_eq!(ev.len(), 2);
    assert!(ev.iter().all(|e| e.run_id.as_deref() == Some(run.as_str())));
    // The attempt's whole sequence has no hole, with the state events among it.
    let all: Vec<u64> = w
        .store
        .events_after(0, 1000)
        .unwrap()
        .iter()
        .filter(|e| e.attempt_id.as_deref() == Some(a.as_str()))
        .map(|e| e.attempt_seq.unwrap())
        .collect();
    assert_eq!(all, (1..=all.len() as u64).collect::<Vec<_>>());
}

// ------------------------------------------------------------ property tests

fn stored_len(root: &Path, a: &AttemptId, stem: &str) -> u64 {
    fs::metadata(logs_path(root, a, &format!("{stem}.log")))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// The records of an index, checked: whole, never going backwards, and none
/// claiming bytes the log lacks. Returns how many.
fn check_index(root: &Path, a: &AttemptId, stem: &str) -> u64 {
    let idx = fs::read(logs_path(root, a, &format!("{stem}.idx"))).unwrap();
    assert_eq!(idx.len() % 16, 0, "a torn record is left in the index");
    let len = stored_len(root, a, stem);
    let (mut end, mut dropped) = (0u64, 0u64);
    for r in idx.chunks(16) {
        let e = u64::from_le_bytes(r[..8].try_into().unwrap());
        let d = u64::from_le_bytes(r[8..].try_into().unwrap());
        assert!(
            e >= end && d >= dropped && e <= len,
            "{e} {d} after {end} {dropped}, log {len}"
        );
        (end, dropped) = (e, d);
    }
    idx.len() as u64 / 16
}

/// Write random chunks to both streams (and sometimes sync), returning what
/// each stream was given in all.
fn random_writes(rng: &mut Rng, s: &AttemptSpool, max_steps: u64) -> [Vec<u8>; 2] {
    let steps = rng.below(max_steps);
    let mut given = [Vec::new(), Vec::new()];
    for _ in 0..steps {
        let i = rng.below(2) as usize;
        let stream = [Stdout, Stderr][i];
        let n = [0, 1, 2, 7, 40, 300][rng.below(6) as usize];
        let chunk = rng.bytes(n);
        s.writer(stream).append(&chunk).unwrap();
        given[i].extend_from_slice(&chunk);
        if rng.below(4) == 0 {
            s.writer(stream).sync().unwrap();
        }
    }
    given
}

#[test]
fn any_crash_point_resumes_with_a_prefix_and_never_goes_back() {
    for case in 0..80u64 {
        let mut rng = Rng::new(case + 1);
        let dir = tempfile::tempdir().unwrap();
        let a = AttemptId::generate();
        let limit = rng.below(1500);
        let given = {
            let s = AttemptSpool::open(dir.path(), &a, limit).unwrap();
            random_writes(&mut rng, &s, 30)
            // dropped: the supervisor died
        };
        let before: Vec<Vec<u8>> = ["stdout", "stderr"]
            .iter()
            .map(|st| fs::read(logs_path(dir.path(), &a, &format!("{st}.log"))).unwrap())
            .collect();
        for i in 0..2 {
            // What was stored is the start of what was given.
            assert!(given[i].starts_with(&before[i]), "case {case}");
        }
        // Damage: cut the log (power loss), cut the index anywhere (a torn
        // record included), overwrite its tail or flip a byte of it.
        let mut kept = before.clone();
        for (i, st) in ["stdout", "stderr"].iter().enumerate() {
            let log = logs_path(dir.path(), &a, &format!("{st}.log"));
            let idx = logs_path(dir.path(), &a, &format!("{st}.idx"));
            match rng.below(5) {
                0 => {}
                1 => {
                    let l = rng.below(before[i].len() as u64 + 1);
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&log)
                        .unwrap()
                        .set_len(l)
                        .unwrap();
                    kept[i].truncate(l as usize);
                }
                2 => {
                    let n = fs::metadata(&idx).unwrap().len();
                    let l = rng.below(n + 1);
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&idx)
                        .unwrap()
                        .set_len(l)
                        .unwrap();
                }
                3 => {
                    let mut x = fs::read(&idx).unwrap();
                    let n = rng.below(40);
                    x.extend(rng.bytes(n));
                    fs::write(&idx, x).unwrap();
                }
                _ => {
                    let mut x = fs::read(&idx).unwrap();
                    if !x.is_empty() {
                        let at = rng.below(x.len() as u64) as usize;
                        x[at] ^= 1 << rng.below(8);
                        fs::write(&idx, x).unwrap();
                    }
                }
            }
        }
        let limit2 = rng.below(2500);
        let s = AttemptSpool::open(dir.path(), &a, limit2).unwrap();
        let r = SpoolReader::open(dir.path(), &a).unwrap();
        let mut seen = [StreamStatus::default(); 2];
        for (i, st) in ["stdout", "stderr"].iter().enumerate() {
            let stream = [Stdout, Stderr][i];
            let status = r.status(stream).unwrap();
            // The index is repaired to the log's real length, whole records only.
            assert_eq!(status.end, kept[i].len() as u64, "case {case}");
            assert_eq!(status.seq, check_index(dir.path(), &a, st), "case {case}");
            assert_eq!(read_all(&r, stream), kept[i], "case {case}");
            assert_eq!(s.writer(stream).status().end, status.end);
            seen[i] = status;
        }
        // More output: offsets only go forward, the sequence has no hole, and
        // the log is the old bytes followed by the new, a prefix of what came.
        let more = random_writes(&mut rng, &s, 30);
        s.finish().unwrap();
        for (i, st) in ["stdout", "stderr"].iter().enumerate() {
            let stream = [Stdout, Stderr][i];
            let status = r.status(stream).unwrap();
            assert!(
                status.end >= seen[i].end && status.seq >= seen[i].seq,
                "case {case}"
            );
            assert!(status.dropped >= seen[i].dropped, "case {case}");
            assert_eq!(status.seq, check_index(dir.path(), &a, st), "case {case}");
            let content = read_all(&r, stream);
            assert_eq!(content.len() as u64, status.end);
            assert!(content.starts_with(&kept[i]), "case {case}");
            assert!(
                more[i].starts_with(&content[kept[i].len()..]),
                "case {case}"
            );
            // A stream that had dropped before keeps nothing new.
            if seen[i].dropped > 0 {
                assert_eq!(status.end, seen[i].end, "case {case}");
            }
            assert!(r.read(stream, 0, 10).unwrap().eof || status.end > 10);
        }
        assert!(r.finished());
    }
}

#[test]
fn stored_plus_dropped_is_what_came_in_and_never_exceeds_the_limit() {
    for case in 0..150u64 {
        let mut rng = Rng::new(1000 + case);
        let limit = [0, 1, 2, 5, 64, 500, 5000][rng.below(7) as usize];
        let dir = tempfile::tempdir().unwrap();
        let a = AttemptId::generate();
        let s = AttemptSpool::open(dir.path(), &a, limit).unwrap();
        let mut given = [0u64; 2];
        let mut content: [Vec<u8>; 2] = [vec![], vec![]];
        for _ in 0..rng.below(60) {
            let i = rng.below(2) as usize;
            let n = [0, 1, 3, 17, 90, 700][rng.below(6) as usize];
            let chunk = rng.bytes(n);
            s.writer([Stdout, Stderr][i]).append(&chunk).unwrap();
            given[i] += n;
            content[i].extend_from_slice(&chunk);
        }
        let (out, err) = s.finish().unwrap();
        let (st, dr) = (out.end + err.end, out.dropped + err.dropped);
        assert_eq!(st + dr, given[0] + given[1], "case {case}");
        assert!(st <= limit, "case {case}: {st} > {limit}");
        // What was kept is the start of each stream, and a drop means the limit was reached.
        let r = SpoolReader::open(dir.path(), &a).unwrap();
        for (i, status) in [out, err].into_iter().enumerate() {
            let got = read_all(&r, [Stdout, Stderr][i]);
            assert_eq!(got.len() as u64, status.end);
            assert!(content[i].starts_with(&got), "case {case}");
            assert_eq!(status.end + status.dropped, given[i], "case {case}");
        }
        assert!(dr == 0 || st == limit, "case {case}");
    }
}

#[test]
fn an_index_scan_over_any_sixteen_bytes_keeps_a_valid_prefix() {
    use super::spool_plan::{IndexScan, Mark};
    let valid = |prev: Mark, m: Mark, len: u64| {
        m.end >= prev.end && m.dropped >= prev.dropped && m.end <= len
    };
    for case in 0..2000u64 {
        let mut rng = Rng::new(5000 + case);
        let len = rng.below(100);
        let n = rng.below(8);
        let mut recs: Vec<[u8; 16]> = vec![];
        let mut prev = Mark::default();
        for _ in 0..n {
            let rec = match rng.below(3) {
                // Anything at all.
                0 => rng.bytes(16).try_into().unwrap(),
                // Small numbers, so that records are often valid.
                1 => Mark {
                    end: rng.below(120),
                    dropped: rng.below(5),
                }
                .encode(),
                // A step forward from the previous one.
                _ => {
                    prev = Mark {
                        end: prev.end + rng.below(10),
                        dropped: prev.dropped + rng.below(3),
                    };
                    prev.encode()
                }
            };
            recs.push(rec);
        }
        let mut scan = IndexScan::new(len);
        let mut fed = 0;
        for r in &recs {
            if !scan.feed(r) {
                break;
            }
            fed += 1;
        }
        // Once stopped, a scan stays stopped.
        if fed < recs.len() {
            assert!(!scan.feed(&Mark::default().encode()));
        }
        let rec = scan.finish();
        assert_eq!(rec.valid_records, fed as u64, "case {case}");
        // The valid prefix is valid, record by record, and the next one is not.
        let mut p = Mark::default();
        for r in &recs[..fed] {
            let m = Mark::decode(r);
            assert!(valid(p, m, len), "case {case}");
            p = m;
        }
        if fed < recs.len() {
            assert!(!valid(p, Mark::decode(&recs[fed]), len), "case {case}");
        }
        // Whatever the log holds beyond it is covered by one more record.
        assert_eq!(rec.mark.end, len, "case {case}");
        assert_eq!(rec.mark.dropped, p.dropped, "case {case}");
        assert_eq!(rec.append.is_some(), p.end < len, "case {case}");
        assert_eq!(rec.seq, fed as u64 + u64::from(p.end < len), "case {case}");
    }
}

// ------------------------------------------------- the supervisor and the spool

use super::pump::{pump, Pipe};
use super::run::{settle, SpoolBeat};
use crate::secrets::mask::Masker;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

fn succeeded() -> Termination {
    Termination::new(Outcome::Succeeded, Some(crate::state::Exit::Code(0)), None).unwrap()
}

fn spool_failures(w: &Wk, a: &AttemptId) -> Vec<serde_json::Value> {
    let all = w.store.events_of(a, "spool_failed").unwrap();
    all.into_iter().map(|e| e.payload).collect()
}

#[test]
fn settling_closes_the_spool_and_stores_the_result() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stdout).append(b"hello").unwrap();
    let (t, here) = settle(&w.store, &w.root, &a, &s, succeeded()).unwrap();
    assert_eq!((t, here), (succeeded(), true));
    let result = w.store.get_result(&a).unwrap().unwrap();
    assert_eq!(result["logs"]["stdout"]["bytes"], 5);
    assert!(SpoolReader::open(&w.root, &a).unwrap().finished());
    assert!(spool_failures(&w, &a).is_empty());
    // Again (a retry after a crash): the same termination, fixed before.
    let (t, here) = settle(&w.store, &w.root, &a, &s, cancelled()).unwrap();
    assert_eq!((t, here), (succeeded(), false));
}

#[test]
fn a_spool_that_fails_at_the_end_makes_the_attempt_interrupted_with_an_event() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stdout).append(b"partial").unwrap();
    // The disk refused a write.
    s.writer(Stdout).poison();
    let (t, here) = settle(&w.store, &w.root, &a, &s, succeeded()).unwrap();
    // Not the success the command reported, and not a silent drop.
    assert_eq!(t.outcome(), Outcome::Interrupted);
    assert_eq!(t.exit(), Some(crate::state::Exit::Code(0)));
    assert!(here);
    let rec = w.store.get(&a).unwrap().unwrap();
    assert_eq!(rec.state, AttemptState::Terminal);
    assert_eq!(rec.termination, Some(t));
    let ev = spool_failures(&w, &a);
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["stage"], "finish");
    assert!(ev[0]["error"].as_str().unwrap().contains("spool"), "{ev:?}");
    // Nothing describes the incomplete output as a result.
    assert!(w.store.get_result(&a).unwrap().is_none());
    assert!(!SpoolReader::open(&w.root, &a).unwrap().finished());
    // A second settle changes nothing and does not repeat the event.
    let (again, here) = settle(&w.store, &w.root, &a, &s, succeeded()).unwrap();
    assert_eq!((again, here), (t, false));
    assert_eq!(spool_failures(&w, &a).len(), 1);
}

#[test]
fn a_cancel_stays_a_cancel_when_the_spool_fails() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    s.writer(Stderr).poison();
    let (t, _) = settle(&w.store, &w.root, &a, &s, cancelled()).unwrap();
    assert_eq!(t, cancelled());
    assert_eq!(spool_failures(&w, &a).len(), 1);
}

#[test]
fn the_heartbeat_stores_positions_only_when_they_moved() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let mut beat = SpoolBeat::default();
    let stored = || w.store.log_position(&a, "stdout").unwrap();
    s.writer(Stdout).append(b"abc").unwrap();
    assert_eq!(stored().written_bytes, 0);
    beat.beat(&w.store, &a, &s, None).unwrap();
    assert_eq!((stored().written_bytes, stored().last_seq), (3, 1));
    // The bytes are published to a reader of another process as well.
    let r = SpoolReader::open(&w.root, &a).unwrap();
    assert_eq!(r.read(Stdout, 0, 10).unwrap().data, b"abc");
    // Nothing new: no new index record (so, no sync) and no change.
    beat.beat(&w.store, &a, &s, None).unwrap();
    assert_eq!(s.writer(Stdout).status().seq, 1);
    s.writer(Stdout).append(b"de").unwrap();
    beat.beat(&w.store, &a, &s, None).unwrap();
    assert_eq!((stored().written_bytes, stored().last_seq), (5, 2));
    assert!(beat.failure.is_none());
    assert!(spool_failures(&w, &a).is_empty());
}

#[test]
fn the_heartbeat_records_a_spool_failure_once_and_is_not_an_error() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let mut beat = SpoolBeat::default();
    s.writer(Stdout).append(b"abc").unwrap();
    s.writer(Stdout).poison();
    beat.beat(&w.store, &a, &s, None).unwrap();
    assert!(beat.failure.is_some());
    let ev = spool_failures(&w, &a);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0]["stage"], "write");
    // The position that could not be made durable was not stored.
    assert_eq!(w.store.log_position(&a, "stdout").unwrap().written_bytes, 0);
    // Later beats do nothing more.
    beat.beat(&w.store, &a, &s, Some("again".into())).unwrap();
    assert_eq!(spool_failures(&w, &a).len(), 1);

    // What the reader thread reported is a failure too, with its own words.
    let b = accept(&w);
    let s = AttemptSpool::open(&w.root, &b, BIG).unwrap();
    let mut beat = SpoolBeat::default();
    beat.beat(&w.store, &b, &s, Some("disk full".into()))
        .unwrap();
    assert_eq!(beat.failure.as_deref(), Some("disk full"));
    assert_eq!(spool_failures(&w, &b)[0]["error"], "disk full");

    // An attempt the database does not know is a storage error, not a spool
    // failure: the caller keeps supervising and tries again.
    let unknown = AttemptId::generate();
    let s = AttemptSpool::open(&w.root, &unknown, BIG).unwrap();
    s.writer(Stdout).append(b"x").unwrap();
    let mut beat = SpoolBeat::default();
    assert!(beat.beat(&w.store, &unknown, &s, None).is_err());
    assert!(beat.failure.is_none());
}

/// Write all of `data` to a pipe opened without blocking, within a bound: a
/// reader that died must fail the test, not hang it.
fn write_bounded(mut out: fs::File, data: &[u8]) {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut rest = data;
    while !rest.is_empty() {
        match out.write(rest) {
            Ok(n) => rest = &rest[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(std::time::Instant::now() < until, "nobody reads the pipe");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => panic!("{e}"),
        }
    }
}

/// Run the pump against a writer in this process, as a command's
/// descriptors would write.
fn pumped(s: &AttemptSpool, pipe: &Pipe, write: impl FnOnce(fs::File) + Send) -> Option<String> {
    pumped_masked(s, pipe, &Masker::new([]), write)
}

fn pumped_masked(
    s: &AttemptSpool,
    pipe: &Pipe,
    mask: &Masker,
    write: impl FnOnce(fs::File) + Send,
) -> Option<String> {
    let done = AtomicBool::new(false);
    let error = Mutex::new(None);
    std::thread::scope(|sc| {
        let feeder = sc.spawn(|| pump(pipe, s.writer(Stdout), mask.stream(), &done, &error));
        let out = fs::OpenOptions::new()
            .append(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(pipe.path())
            .unwrap();
        write(out);
        done.store(true, Ordering::SeqCst);
        feeder.join().unwrap();
    });
    error.into_inner().unwrap()
}

#[test]
fn the_pump_carries_everything_written_before_the_end() {
    let (dir, a, s) = spool(BIG);
    let path = Pipe::path_for(dir.path(), &a, 0);
    let pipe = Pipe::open(&path).unwrap();
    // More than a pipe holds: the writer only gets through if it is read.
    let data = pattern(0, 600_000);
    let err = pumped(&s, &pipe, |out| write_bounded(out, &data));
    assert_eq!(err, None);
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), data);
    drop(pipe);
    assert!(!path.exists(), "the pipe is removed with its reader");
}

#[test]
fn the_pump_keeps_reading_when_the_spool_is_full_or_has_failed() {
    // Full: the rest is counted, the writer is never blocked.
    let (_d, a, s) = spool(10);
    let pipe = Pipe::open(&Pipe::path_for(_d.path(), &a, 0)).unwrap();
    let data = pattern(0, 300_000);
    assert_eq!(pumped(&s, &pipe, |o| write_bounded(o, &data)), None);
    let st = s.writer(Stdout).status();
    assert_eq!((st.end, st.dropped), (10, 299_990));

    // Failed: the first failure is reported, and the writer still finishes.
    let (_d, a, s) = spool(BIG);
    let pipe = Pipe::open(&Pipe::path_for(_d.path(), &a, 3)).unwrap();
    s.writer(Stdout).poison();
    let err = pumped(&s, &pipe, |o| write_bounded(o, &data));
    assert!(err.is_some_and(|e| e.contains("spool")));
}

#[test]
fn a_pipe_is_made_once_reused_after_a_crash_and_never_replaces_another_file() {
    let (dir, a, _s) = spool(BIG);
    let path = Pipe::path_for(dir.path(), &a, 1);
    assert!(path.starts_with(super::spool::logs_dir(dir.path(), &a)));
    let first = Pipe::open(&path).unwrap();
    assert_eq!(first.path(), path);
    // A crash leaves the FIFO behind: the next supervisor reads from it.
    std::mem::forget(first);
    let again = Pipe::open(&path).unwrap();
    drop(again);
    assert!(!path.exists());
    // A regular file or a symlink at the path is an error and stays.
    fs::write(&path, b"mine").unwrap();
    assert!(Pipe::open(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"mine");
    let link = Pipe::path_for(dir.path(), &a, 2);
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(Pipe::open(&link).is_err());
    assert!(link.symlink_metadata().is_ok() && path.exists());
}

#[test]
fn the_pump_masks_secrets_also_across_reads_and_at_the_very_end() {
    let (dir, a, s) = spool(BIG);
    let pipe = Pipe::open(&Pipe::path_for(dir.path(), &a, 0)).unwrap();
    let mask = Masker::new([b"hunter2-value".to_vec()]);
    let err = pumped_masked(&s, &pipe, &mask, |mut out| {
        // The secret arrives in two writes, far enough apart for two reads,
        // and the output ends with a secret and with a held-back prefix of one.
        out.write_all(b"a hunter2-").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        out.write_all(b"value b hunter2-value hunter2-").unwrap();
    });
    assert_eq!(err, None);
    s.finish().unwrap();
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), b"a *** b *** hunter2-");
}
