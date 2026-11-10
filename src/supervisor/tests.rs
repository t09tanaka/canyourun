use super::outcome::{
    drain_pending_deletes, finish_attempt, store_artifact, ArtifactInput, OutcomeError,
};
use super::spool::{AttemptSpool, SpoolError, SpoolReader, StreamStatus};
use super::spool_plan::{logs_locator, MAX_READ_BYTES};
use crate::ids::{
    AttemptId, CoordinatorEpoch, Digest, Generation, HomeworkId, RunId, SourceId, WorkerId,
};
use crate::state::{AttemptState, Outcome, Termination};
use crate::storage::{NewAcceptance, SubmitOutcome, WorkerStore};
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
    // A big enough append publishes by itself.
    let st = w.append(&vec![7u8; 5000]).unwrap();
    assert_eq!((st.end, st.seq), (5003, 2));
    assert_eq!(s.writer(Stderr).status(), StreamStatus::default());
}

#[test]
fn tiny_writes_do_not_grow_the_index_per_append() {
    let (dir, a, s) = spool(BIG);
    let w = s.writer(Stdout);
    for _ in 0..40_000 {
        w.append(b"x").unwrap();
    }
    s.finish().unwrap();
    let idx = fs::metadata(dir.path().join(a.as_str()).join("logs/stdout.idx")).unwrap();
    // 40 kB in one-byte writes: about one record per 4 KiB, not 40 000.
    assert!(idx.len() <= 16 * 12, "{}", idx.len());
    let r = SpoolReader::open(dir.path(), &a).unwrap();
    assert_eq!(read_all(&r, Stdout), vec![b'x'; 40_000]);
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
    store: WorkerStore,
}

fn wk() -> Wk {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let store = WorkerStore::open(&state, &WorkerId::parse("mini-a").unwrap()).unwrap();
    store.start_agent(1).unwrap();
    let root = dir.path().join("spool");
    Wk {
        _dir: dir,
        root,
        store,
    }
}

fn accept(w: &Wk) -> AttemptId {
    let hw = HomeworkId::generate();
    let a = AttemptId::generate();
    let run = RunId::parse(&format!("run_{}", &hw.as_str()["hw_".len()..])).unwrap();
    match w
        .store
        .submit(
            NewAcceptance {
                attempt_id: a.clone(),
                run_id: run,
                homework_id: hw,
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
    let src = w.root.join("work");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("out.bin"), [0xffu8, 0, 1, 2]).unwrap();
    let (a1, a2) = (AttemptId::generate(), AttemptId::generate());
    let r1 = store_artifact(&w.root, &a1, &input("d/out.bin", &src.join("out.bin")), 100).unwrap();
    assert_eq!((r1.size, r1.path.as_str()), (4, "d/out.bin"));
    assert_eq!(r1.digest, Digest::of_bytes(&[0xff, 0, 1, 2]));
    assert_eq!(r1.attempt_id, a1);
    let stored = w.root.join(a1.as_str()).join("artifacts/d/out.bin");
    assert_eq!(fs::read(&stored).unwrap(), [0xff, 0, 1, 2]);
    // The same name and content again is a replay.
    store_artifact(&w.root, &a1, &input("d/out.bin", &src.join("out.bin")), 100).unwrap();
    // Another content under the same name is refused and the first is kept.
    fs::write(src.join("other.bin"), b"different").unwrap();
    let e = store_artifact(
        &w.root,
        &a1,
        &input("d/out.bin", &src.join("other.bin")),
        100,
    );
    assert!(matches!(e, Err(OutcomeError::Conflict(_))), "{e:?}");
    assert_eq!(fs::read(&stored).unwrap(), [0xff, 0, 1, 2]);
    // A new attempt has its own store; the old result is untouched.
    store_artifact(
        &w.root,
        &a2,
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
    let src = w.root.join("work");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("f"), b"12345").unwrap();
    for bad in ["../x", "/abs", "", "a//b", ".tmpx"] {
        let e = store_artifact(&w.root, &a, &input(bad, &src.join("f")), 100);
        assert!(matches!(e, Err(OutcomeError::BadName(_))), "{bad}: {e:?}");
    }
    let e = store_artifact(&w.root, &a, &input("big", &src.join("f")), 4);
    assert!(
        matches!(e, Err(OutcomeError::TooLarge { max: 4, .. })),
        "{e:?}"
    );
    let e = store_artifact(&w.root, &a, &input("dir", &src), 100);
    assert!(matches!(e, Err(OutcomeError::NotRegular(_))));
    std::os::unix::fs::symlink(src.join("f"), src.join("link")).unwrap();
    let e = store_artifact(&w.root, &a, &input("l", &src.join("link")), 100);
    assert!(matches!(e, Err(OutcomeError::NotRegular(_))));
    let e = store_artifact(&w.root, &a, &input("gone", &src.join("nope")), 100);
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
    let src = w.root.join("f");
    fs::create_dir_all(&w.root).unwrap();
    fs::write(&src, b"art").unwrap();
    let art = store_artifact(&w.root, &a, &input("r.txt", &src), 10).unwrap();
    let rec = finish_attempt(
        &w.store,
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
    let again = finish_attempt(&w.store, &a, &s, cancelled(), &[art], 60).unwrap();
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
    finish_attempt(&w.store, &a, &s, cancelled(), &[], 5).unwrap();
    // Artifacts that were not in the first result cannot be added later.
    let src = w.root.join("f");
    fs::write(&src, b"late").unwrap();
    let late = store_artifact(&w.root, &a, &input("late", &src), 10).unwrap();
    let e = finish_attempt(&w.store, &a, &s, cancelled(), &[late], 6);
    assert!(matches!(e, Err(OutcomeError::Storage(_))), "{e:?}");
}

#[test]
fn artifacts_of_another_attempt_or_with_a_bad_path_are_refused() {
    let w = wk();
    let a = accept(&w);
    let s = AttemptSpool::open(&w.root, &a, BIG).unwrap();
    let src = w.root.join("f");
    fs::write(&src, b"x").unwrap();
    let other = store_artifact(&w.root, &AttemptId::generate(), &input("o", &src), 10).unwrap();
    let e = finish_attempt(&w.store, &a, &s, cancelled(), &[other], 5);
    assert!(matches!(e, Err(OutcomeError::BadName(_))), "{e:?}");
    let mut bad = store_artifact(&w.root, &a, &input("ok", &src), 10).unwrap();
    bad.path = "../escape".into();
    let e = finish_attempt(&w.store, &a, &s, cancelled(), &[bad], 5);
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
        let src = w.root.join("f");
        fs::write(&src, b"art").unwrap();
        let art = store_artifact(&w.root, id, &input("a/b.txt", &src), 10).unwrap();
        finish_attempt(&w.store, id, &s, cancelled(), &[art], 100).unwrap();
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
    finish_attempt(&w.store, &a, &s, cancelled(), &[], 2).unwrap();
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
    let src = w.root.join("f");
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
        &input("y", &src),
        10
    )));
    assert_eq!(fs::read(outside.join("same")).unwrap(), b"art");

    // A plain file where a directory is needed is refused too.
    fs::write(arts.join("file"), b"f").unwrap();
    assert!(is_refused(store_artifact(
        &w.root,
        &a,
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
    finish_attempt(&w.store, &a, &s, cancelled(), &[], 2).unwrap();
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
    let e = finish_attempt(&w.store, &a, &s, cancelled(), &[], 5);
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
    let r = finish_attempt(&w.store, &a, &s, cancelled(), &[], 10).unwrap();
    assert!(!r.fresh);
    assert_eq!(count(&w), 1);
    // Idempotent.
    finish_attempt(&w.store, &a, &s, cancelled(), &[], 11).unwrap();
    assert_eq!(count(&w), 1);
}
