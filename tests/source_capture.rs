//! Chapter 6 end to end, over throwaway Git repositories: A11 (what a capture
//! holds and that the original is untouched), A12 (SOURCE_BUSY), A13 (no
//! incomplete source is ever visible; the same content is the same id), A14
//! (unpushed commits, submodules, LFS), and the receiving side's refusals.

mod common;

use local_ci::source::{import_objects, seal_manifest, SourceKind, Stage};
use local_ci::ExecContext;
use remote_task_runner::error::ErrorCode;
use remote_task_runner::ids::RunId;
use remote_task_runner::source::diff::BlobWant;
use remote_task_runner::source::{
    capture, capture_for_run, CaptureParams, SourceRequest, SourceStore,
};
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

const PATH: &str = "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin";

fn git_cmd(dir: &Path, args: &[&str]) -> Command {
    let mut c = Command::new("git");
    c.args([
        "-c",
        "user.email=t@example.com",
        "-c",
        "user.name=t",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "protocol.file.allow=always",
    ])
    .args(args)
    .current_dir(dir)
    .env("PATH", PATH)
    .env("HOME", dir)
    // no user or system config: a machine-wide git-lfs filter would run
    .env("GIT_OPTIONAL_LOCKS", "0")
    .env("GIT_CONFIG_GLOBAL", "/dev/null")
    .env("GIT_CONFIG_NOSYSTEM", "1");
    c
}

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = git_cmd(dir, args).output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim_end().to_string()
}

fn git(dir: &Path, args: &[&str]) {
    git_out(dir, args);
}

fn env(home: &Path) -> BTreeMap<String, String> {
    [("PATH", PATH), ("HOME", home.to_str().unwrap())]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

struct Fixture {
    _tmp: common::Tmp,
    /// the runner's state dir
    state: PathBuf,
    /// the repository
    dir: PathBuf,
}

impl Fixture {
    fn env(&self) -> BTreeMap<String, String> {
        env(&self.state)
    }
    fn store(&self) -> SourceStore {
        SourceStore::open(&self.state).unwrap()
    }
    fn worktree(&self) -> SourceRequest {
        SourceRequest::Worktree {
            dir: self.dir.clone(),
        }
    }
}

fn fixture() -> Fixture {
    let tmp = common::tempdir().unwrap();
    let state = tmp.path().join("state");
    let dir = tmp.path().join("work");
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    std::fs::write(dir.join(".gitignore"), "ignored.log\n.env\nout/\n").unwrap();
    for (f, c) in [
        ("a.txt", "a\n"),
        ("staged.txt", "s0\n"),
        ("unstaged.txt", "u0\n"),
        ("gone.txt", "g\n"),
        ("run.sh", "#!/bin/sh\necho hi\n"),
    ] {
        std::fs::write(dir.join(f), c).unwrap();
    }
    std::fs::write(dir.join("blob.bin"), [0u8, 159, 146, 150, 1, 2, 3]).unwrap();
    std::os::unix::fs::symlink("a.txt", dir.join("link")).unwrap();
    std::fs::create_dir_all(dir.join("sub dir")).unwrap();
    std::fs::write(dir.join("sub dir/new\u{3042}.txt"), "kana\n").unwrap();
    git(&dir, &["add", "."]);
    git(&dir, &["commit", "-qm", "base"]);
    Fixture {
        _tmp: tmp,
        state,
        dir,
    }
}

/// staged / unstaged / staged-then-changed / untracked / deleted / mode /
/// binary / symlink, plus an ignored file that must stay out.
fn dirty(dir: &Path) {
    std::fs::write(dir.join("staged.txt"), "s1\n").unwrap();
    git(dir, &["add", "staged.txt"]);
    std::fs::write(dir.join("unstaged.txt"), "u1\n").unwrap();
    std::fs::write(dir.join("staged_then_changed.txt"), "x0\n").unwrap();
    git(dir, &["add", "staged_then_changed.txt"]);
    std::fs::write(dir.join("staged_then_changed.txt"), "x1\n").unwrap();
    std::fs::remove_file(dir.join("gone.txt")).unwrap();
    std::fs::set_permissions(dir.join("run.sh"), std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(dir.join("blob.bin"), [0u8, 255, 254, 0, 9]).unwrap();
    std::fs::remove_file(dir.join("link")).unwrap();
    std::os::unix::fs::symlink("/outside/the/repo", dir.join("link")).unwrap();
    std::fs::write(dir.join("untracked.txt"), "new\n").unwrap();
    std::fs::write(dir.join("ignored.log"), "noise\n").unwrap();
    std::fs::write(dir.join("exec_new.sh"), "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(
        dir.join("exec_new.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
}

/// path -> (kind, executable, content or link value)
type Listing = BTreeMap<String, (char, bool, Vec<u8>)>;

/// What is in a directory, without `.git`.
fn listing(root: &Path) -> Listing {
    fn walk(root: &Path, rel: &Path, out: &mut Listing) {
        for e in std::fs::read_dir(root.join(rel)).unwrap() {
            let e = e.unwrap();
            let p = rel.join(e.file_name());
            if p == Path::new(".git") {
                continue;
            }
            let full = root.join(&p);
            let meta = std::fs::symlink_metadata(&full).unwrap();
            let name = p.to_string_lossy().into_owned();
            if meta.file_type().is_symlink() {
                let t = std::fs::read_link(&full).unwrap();
                out.insert(name, ('l', false, t.to_string_lossy().as_bytes().to_vec()));
            } else if meta.is_dir() {
                walk(root, &p, out);
            } else {
                out.insert(
                    name,
                    ('f', meta.mode() & 0o111 != 0, std::fs::read(&full).unwrap()),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, Path::new(""), &mut out);
    out
}

/// What a capture must not change in the original.
fn original_state(
    dir: &Path,
) -> (
    Vec<u8>,
    String,
    String,
    String,
    BTreeMap<String, (char, bool, Vec<u8>)>,
) {
    (
        std::fs::read(dir.join(".git/index")).unwrap(),
        git_out(dir, &["for-each-ref"]),
        git_out(dir, &["rev-parse", "HEAD"]),
        git_out(dir, &["status", "--porcelain=v1", "-z", "--ignored"]),
        listing(dir),
    )
}

fn expected_listing(dir: &Path) -> Listing {
    let mut l = listing(dir);
    // ignored files are not part of a capture
    l.remove("ignored.log");
    l
}

fn busy_hook(f: impl Fn(u32) + Send + Sync + 'static) -> (local_ci::source::Hook, Arc<AtomicU32>) {
    let passes = Arc::new(AtomicU32::new(0));
    let p = passes.clone();
    let h: local_ci::source::Hook = Arc::new(move |s| {
        if let Stage::AfterRead(n) = s {
            p.fetch_add(1, Ordering::SeqCst);
            f(n);
        }
        Ok(())
    });
    (h, passes)
}

fn manifests_in(store: &SourceStore) -> Vec<String> {
    std::fs::read_dir(store.root().join("manifests"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn a11_every_kind_of_change_is_captured_and_unpacks_to_the_same_content() {
    let f = fixture();
    dirty(&f.dir);
    let before = original_state(&f.dir);
    let store = f.store();
    let got = capture(&f.env(), &store, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(original_state(&f.dir), before, "the original was written");

    let m = &got.manifest;
    assert_eq!(m.source_kind, SourceKind::Worktree);
    assert!(m.original_head.is_some() && m.index_tree.is_some() && m.worktree_tree.is_some());
    assert_ne!(m.index_tree, m.worktree_tree);
    assert_eq!(m.branch.as_deref(), Some("main"));
    assert!(m.cwd_relative.is_empty());
    assert!(m.secret_files.is_empty());

    let ci = store.manifest(&m.ci_source_id).unwrap().unwrap();
    let dest = f.state.join("unpacked");
    store.materialize(&ci, &dest).unwrap();
    assert_eq!(listing(&dest), expected_listing(&f.dir));
    // the ones that are easy to lose
    let l = listing(&dest);
    assert_eq!(l["link"].2, b"/outside/the/repo");
    assert!(l["exec_new.sh"].1 && !l["run.sh"].1);
    assert!(!l.contains_key("gone.txt") && !l.contains_key("ignored.log"));
    assert_eq!(l["staged_then_changed.txt"].2, b"x1\n");

    // a second capture of the unchanged input is the same source
    let again = capture(&f.env(), &store, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(again.manifest.source_id, m.source_id);
    assert_eq!(original_state(&f.dir), before);
}

#[test]
fn a11_a_capture_from_a_subdirectory_records_where_it_started() {
    let f = fixture();
    let req = SourceRequest::Worktree {
        dir: f.dir.join("sub dir"),
    };
    let got = capture(&f.env(), &f.store(), &req, &CaptureParams::default()).unwrap();
    assert_eq!(got.manifest.cwd_relative.display(), "sub dir");
}

#[test]
fn a12_an_input_that_keeps_moving_is_source_busy_and_nothing_is_published() {
    let f = fixture();
    let file = f.dir.join("a.txt");
    let (hook, passes) = busy_hook(move |n| std::fs::write(&file, format!("v{n}\n")).unwrap());
    let store = f.store();
    let err = capture(
        &f.env(),
        &store,
        &f.worktree(),
        &CaptureParams {
            hook: Some(hook),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SourceBusy);
    assert_eq!(err.code.exit_code(), 71);
    assert_eq!(passes.load(Ordering::SeqCst), 3, "exactly three passes");
    assert!(manifests_in(&store).is_empty());
}

#[test]
fn a12_the_attempt_budget_is_clamped_to_three() {
    let f = fixture();
    let file = f.dir.join("a.txt");
    let (hook, passes) = busy_hook(move |n| std::fs::write(&file, format!("v{n}\n")).unwrap());
    let err = capture(
        &f.env(),
        &f.store(),
        &f.worktree(),
        &CaptureParams {
            hook: Some(hook),
            max_attempts: Some(50),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SourceBusy);
    assert_eq!(passes.load(Ordering::SeqCst), 3);
}

#[test]
fn a12_a_change_that_stops_is_captured_on_the_next_pass() {
    let f = fixture();
    let file = f.dir.join("a.txt");
    let (hook, passes) = busy_hook(move |n| {
        if n == 1 {
            std::fs::write(&file, "edited during the first pass\n").unwrap()
        }
    });
    let store = f.store();
    let got = capture(
        &f.env(),
        &store,
        &f.worktree(),
        &CaptureParams {
            hook: Some(hook),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(passes.load(Ordering::SeqCst), 2);
    let ci = store.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let dest = f.state.join("ws");
    store.materialize(&ci, &dest).unwrap();
    assert_eq!(
        std::fs::read(dest.join("a.txt")).unwrap(),
        b"edited during the first pass\n"
    );
}

#[test]
fn a12_a_head_that_moves_during_capture_is_noticed() {
    let f = fixture();
    let d = f.dir.clone();
    let (hook, _) = busy_hook(move |n| {
        std::fs::write(d.join("h.txt"), format!("{n}\n")).unwrap();
        git(&d, &["add", "h.txt"]);
        git(&d, &["commit", "-qm", &format!("moving {n}")]);
    });
    let err = capture(
        &f.env(),
        &f.store(),
        &f.worktree(),
        &CaptureParams {
            hook: Some(hook),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SourceBusy);
}

#[test]
fn a13_a_capture_cut_off_before_publication_leaves_nothing_visible_and_resumes_as_the_same_id() {
    let f = fixture();
    dirty(&f.dir);
    // the reference: the id an uninterrupted capture gives, in a store of its own
    let reference = {
        let other = common::tempdir().unwrap();
        let s = SourceStore::open(other.path()).unwrap();
        capture(&f.env(), &s, &f.worktree(), &CaptureParams::default())
            .unwrap()
            .manifest
            .source_id
    };

    let store = f.store();
    let cut: local_ci::source::Hook = Arc::new(|s| match s {
        Stage::BeforePublish => anyhow::bail!("killed before the manifest was published"),
        _ => Ok(()),
    });
    let err = capture(
        &f.env(),
        &store,
        &f.worktree(),
        &CaptureParams {
            hook: Some(cut),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(
        manifests_in(&store).is_empty(),
        "an incomplete source is visible"
    );
    assert!(std::fs::read_dir(store.root().join("manifests"))
        .unwrap()
        .next()
        .is_none());

    let resumed = capture(&f.env(), &store, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(resumed.manifest.source_id, reference);
    assert_eq!(manifests_in(&store).len(), 1);
}

#[test]
fn a13_a_run_captures_once_and_every_later_call_gets_that_source() {
    let f = fixture();
    let store = f.store();
    let run = RunId::generate();
    let first = capture_for_run(
        &f.env(),
        &store,
        &run,
        &f.worktree(),
        &CaptureParams::default(),
    )
    .unwrap();
    // the input changes, a worker joins late, the run is re-dispatched
    std::fs::write(f.dir.join("a.txt"), "changed after the capture\n").unwrap();
    let second = capture_for_run(
        &f.env(),
        &store,
        &run,
        &f.worktree(),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(second.manifest, first.manifest);
    assert_eq!(manifests_in(&store).len(), 1);
    // another run captures what is there now
    let other = capture_for_run(
        &f.env(),
        &store,
        &RunId::generate(),
        &f.worktree(),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_ne!(other.manifest.source_id, first.manifest.source_id);
    // a record of the run survives reopening the store
    let reopened = f.store();
    assert_eq!(
        reopened.source_of_run(&run).unwrap().as_deref(),
        Some(first.manifest.ci_source_id.as_str())
    );
}

#[test]
fn a_capture_that_fails_does_not_bind_the_run() {
    let f = fixture();
    let store = f.store();
    let run = RunId::generate();
    let file = f.dir.join("a.txt");
    let (hook, _) = busy_hook(move |n| std::fs::write(&file, format!("v{n}\n")).unwrap());
    let err = capture_for_run(
        &f.env(),
        &store,
        &run,
        &f.worktree(),
        &CaptureParams {
            hook: Some(hook),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SourceBusy);
    assert_eq!(store.source_of_run(&run).unwrap(), None);
}

#[test]
fn a_fifo_is_an_explicit_error() {
    let f = fixture();
    let st = Command::new("mkfifo")
        .arg(f.dir.join("pipe"))
        .status()
        .unwrap();
    assert!(st.success());
    let err = capture(
        &f.env(),
        &f.store(),
        &f.worktree(),
        &CaptureParams::default(),
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(
        err.message.contains("FIFO") && err.message.contains("pipe"),
        "{err:?}"
    );
}

#[test]
fn a_directory_outside_git_is_captured() {
    let tmp = common::tempdir().unwrap();
    let dir = tmp.path().join("plain");
    std::fs::create_dir_all(dir.join("d")).unwrap();
    std::fs::write(dir.join("d/f.txt"), "f\n").unwrap();
    std::os::unix::fs::symlink("../outside", dir.join("d/l")).unwrap();
    let store = SourceStore::open(&tmp.path().join("state")).unwrap();
    let got = capture(
        &env(tmp.path()),
        &store,
        &SourceRequest::Directory { dir: dir.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(got.manifest.source_kind, SourceKind::Directory);
    assert!(got.manifest.execution_commit.is_none());
    let ci = store.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let dest = tmp.path().join("out");
    store.materialize(&ci, &dest).unwrap();
    assert_eq!(listing(&dest), listing(&dir));
}

// ---- A14 and the receiving side -------------------------------------------

/// Send everything `from` has for `id` to `to`, the way a transfer would:
/// blobs by hash, then the manifests, leaf first.
fn send(from: &SourceStore, to: &SourceStore, ci_id: &str) {
    let plan = from.plan_transfer(ci_id, to).unwrap();
    for w in &plan.blobs {
        let bytes = from.read_blob(&w.hash).unwrap();
        to.ingest_blob(w, &mut &bytes[..]).unwrap();
    }
    for id in &plan.manifests {
        to.publish_received(&from.manifest(id).unwrap().unwrap())
            .unwrap();
    }
}

#[test]
fn only_what_the_peer_lacks_is_planned_and_a_complete_peer_gets_nothing() {
    let f = fixture();
    dirty(&f.dir);
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let id = &got.manifest.ci_source_id;
    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();

    let all = a.plan_transfer(id, &b).unwrap();
    assert_eq!(all.manifests, vec![id.clone()]);
    assert!(all.bundle);
    let distinct: std::collections::BTreeSet<_> = got
        .manifest
        .object_manifest
        .entries
        .iter()
        .map(|e| e.hash.clone())
        .collect();
    assert_eq!(all.blobs.len(), distinct.len());

    // the peer already has one blob: it is not asked for again
    let first = &all.blobs[0];
    b.ingest_blob(first, &mut &a.read_blob(&first.hash).unwrap()[..])
        .unwrap();
    let rest = a.plan_transfer(id, &b).unwrap();
    assert_eq!(rest.blobs.len(), all.blobs.len() - 1);
    assert!(!rest.blobs.contains(first));

    send(&a, &b, id);
    let done = a.plan_transfer(id, &b).unwrap();
    assert!(done.manifests.is_empty() && done.blobs.is_empty() && !done.bundle);

    // what arrived unpacks to what the original captured
    let ci = b.manifest(id).unwrap().unwrap();
    let dest = other.path().join("ws");
    b.materialize(&ci, &dest).unwrap();
    assert_eq!(listing(&dest), expected_listing(&f.dir));
}

#[test]
fn a_blob_that_is_not_the_one_asked_for_is_not_stored() {
    let tmp = common::tempdir().unwrap();
    let s = SourceStore::open(tmp.path()).unwrap();
    let good = b"the content".to_vec();
    let hash = {
        use sha2::{Digest, Sha256};
        Sha256::digest(&good)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let want = BlobWant {
        hash: hash.clone(),
        size: good.len() as u64,
    };
    let err = s.ingest_blob(&want, &mut &b"tampered"[..]).unwrap_err();
    assert!(err.to_string().contains("not the expected"), "{err}");
    assert!(!remote_task_runner::source::Inventory::has_blob(&s, &hash));
    let err = s
        .ingest_blob(
            &BlobWant {
                size: 3,
                ..want.clone()
            },
            &mut &good[..],
        )
        .unwrap_err();
    assert!(err.to_string().contains("size"), "{err}");
    assert!(!remote_task_runner::source::Inventory::has_blob(&s, &hash));
    s.ingest_blob(&want, &mut &good[..]).unwrap();
    assert!(remote_task_runner::source::Inventory::has_blob(&s, &hash));
}

#[test]
fn a_manifest_is_not_visible_until_everything_it_names_has_arrived() {
    let f = fixture();
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let ci = a.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    // a transfer that stopped after the first blob
    let plan = a.plan_transfer(&ci.source_id, &b).unwrap();
    let w = &plan.blobs[0];
    b.ingest_blob(w, &mut &a.read_blob(&w.hash).unwrap()[..])
        .unwrap();
    assert!(b.publish_received(&ci).is_err());
    assert!(b.manifest(&ci.source_id).unwrap().is_none());
    assert!(manifests_in(&b).is_empty());
    // resumed with the same plan: complete, and then visible
    send(&a, &b, &ci.source_id);
    assert_eq!(b.manifest(&ci.source_id).unwrap().unwrap(), ci);
}

fn resealed(
    ci: &local_ci::source::SourceManifest,
    edit: impl FnOnce(&mut local_ci::source::SourceManifest),
) -> local_ci::source::SourceManifest {
    let mut m = ci.clone();
    edit(&mut m);
    seal_manifest(m)
}

#[test]
fn a_manifest_that_would_write_outside_the_workspace_is_refused() {
    let f = fixture();
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let ci = a.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    send(&a, &b, &ci.source_id);

    let bad_paths: Vec<(&str, Vec<u8>)> = vec![
        ("parent", b"../escape.txt".to_vec()),
        ("absolute", b"/tmp/escape.txt".to_vec()),
        ("dot-git", b".git/hooks/pre-commit".to_vec()),
        ("nested parent", b"a/../../escape".to_vec()),
    ];
    for (name, path) in bad_paths {
        let m = resealed(&ci, |m| {
            let mut e = m.object_manifest.entries[0].clone();
            e.path = local_ci::source::RelPath(path.clone());
            m.object_manifest.entries.push(e);
        });
        let err = b.publish_received(&m).unwrap_err();
        assert!(err.to_string().contains("refusing path"), "{name}: {err}");
        assert!(b.manifest(&m.source_id).unwrap().is_none(), "{name}");
        // and it is not unpacked either, from a store that does hold the blobs
        let dest = other.path().join(format!("dest-{name}"));
        assert!(b.materialize(&m, &dest).is_err(), "{name}");
        assert!(!dest.join("escape.txt").exists());
    }
    assert!(!other.path().join("escape.txt").exists());
}

#[test]
fn an_entry_below_a_symlink_entry_is_refused() {
    let f = fixture();
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let ci = a.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    // `link` is a symlink entry in the fixture; an entry below it would be
    // written through it
    let m = resealed(&ci, |m| {
        let mut e = m.object_manifest.entries[0].clone();
        e.path = local_ci::source::RelPath::new("link/escape.txt");
        m.object_manifest.entries.push(e);
    });
    let err = a.materialize(&m, &f.state.join("dest")).unwrap_err();
    assert!(err.to_string().contains("ThroughEntry"), "{err}");
    assert!(!f.state.join("dest").exists());
}

#[test]
fn a_manifest_that_does_not_match_its_seal_is_refused() {
    let f = fixture();
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let mut ci = a.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    send(&a, &f.store(), &ci.source_id);
    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    send(&a, &b, &ci.source_id);
    ci.branch = Some("tampered".into());
    assert!(b.publish_received(&ci).is_err());
    let mut old = ci.clone();
    old.branch = None;
    old.schema_version = 1;
    assert!(b.publish_received(&old).is_err());
}

fn bare_origin_and_clone(f: &Fixture) -> (PathBuf, PathBuf) {
    let root = f.dir.parent().unwrap();
    let origin = root.join("origin.git");
    git(
        root,
        &[
            "clone",
            "-q",
            "--bare",
            f.dir.to_str().unwrap(),
            "origin.git",
        ],
    );
    git(
        &f.dir,
        &["remote", "add", "origin", origin.to_str().unwrap()],
    );
    git(&f.dir, &["fetch", "-q", "origin"]);
    git(
        &f.dir,
        &["branch", "-q", "--set-upstream-to=origin/main", "main"],
    );
    (origin, root.join("worker-clone"))
}

#[test]
fn a14_unpushed_commits_travel_in_a_bundle_and_a_worker_never_fetches_origin() {
    let f = fixture();
    let (origin, worker) = bare_origin_and_clone(&f);
    // two commits that were never pushed, then uncommitted work on top
    std::fs::write(f.dir.join("unpushed1.txt"), "1\n").unwrap();
    git(&f.dir, &["add", "unpushed1.txt"]);
    git(&f.dir, &["commit", "-qm", "unpushed 1"]);
    std::fs::write(f.dir.join("unpushed2.txt"), "2\n").unwrap();
    git(&f.dir, &["add", "unpushed2.txt"]);
    git(&f.dir, &["commit", "-qm", "unpushed 2"]);
    dirty(&f.dir);
    let head = git_out(&f.dir, &["rev-parse", "HEAD"]);

    let a = f.store();
    let got = capture(
        &f.env(),
        &a,
        &f.worktree(),
        &CaptureParams {
            base: Some("origin/main".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let m = &got.manifest;
    assert_eq!(m.original_head.as_deref(), Some(head.as_str()));
    assert!(m.merge_base.is_some() && m.base.is_some());
    assert_ne!(
        m.merge_base, m.original_head,
        "the history is not on origin"
    );
    let ci = a.manifest(&m.ci_source_id).unwrap().unwrap();
    let ctx = ExecContext::new(f.env());
    let bundle = a.export_bundle(&ctx, &ci).unwrap();

    // the worker's clone has only what origin has
    git(
        f.dir.parent().unwrap(),
        &["clone", "-q", origin.to_str().unwrap(), "worker-clone"],
    );
    assert!(
        git_cmd(&worker, &["cat-file", "-e", &head])
            .status()
            .unwrap()
            .code()
            != Some(0)
    );
    // origin moves on afterwards: the worker must not depend on it
    let exec = import_objects(&ctx, &worker, &bundle, &ci).unwrap();
    assert_eq!(Some(exec.clone()), m.execution_commit);
    assert_eq!(
        git_out(&worker, &["rev-parse", &format!("{exec}^{{tree}}")]),
        m.worktree_tree.clone().unwrap()
    );
    assert_eq!(git_out(&worker, &["rev-parse", &format!("{exec}^")]), head);
    // the unpushed history is there
    assert_eq!(
        git_out(
            &worker,
            &["log", "--format=%s", &format!("{head}~2..{head}")]
        ),
        "unpushed 2\nunpushed 1"
    );
    // Known gap (reported to the owner of local-ci's bundle): the index tree
    // is not in the bundle, so a worker cannot reproduce `git diff --cached`
    // from the bundle alone.
    let index_tree = m.index_tree.clone().unwrap();
    let ok = git_cmd(&worker, &["cat-file", "-e", &index_tree])
        .status()
        .unwrap()
        .success();
    eprintln!("index_tree {index_tree} reachable on the worker: {ok}");
}

fn with_submodule(f: &Fixture) {
    let lib = f.dir.parent().unwrap().join("lib");
    std::fs::create_dir_all(&lib).unwrap();
    git(&lib, &["init", "-q", "-b", "main"]);
    std::fs::write(lib.join("lib.txt"), "lib\n").unwrap();
    git(&lib, &["add", "."]);
    git(&lib, &["commit", "-qm", "lib"]);
    git(
        &f.dir,
        &[
            "submodule",
            "add",
            "-q",
            lib.to_str().unwrap(),
            "vendor/lib",
        ],
    );
    git(&f.dir, &["commit", "-qm", "add submodule"]);
}

#[test]
fn a14_a_dirty_submodule_is_captured_whole_and_travels_with_its_parent() {
    let f = fixture();
    with_submodule(&f);
    // clean: pinned at its revision
    let a = f.store();
    let clean = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(clean.manifest.submodules.len(), 1);
    assert!(!clean.manifest.submodules[0].dirty);

    // dirty: edited and a new file, and a commit not in the superproject
    std::fs::write(f.dir.join("vendor/lib/lib.txt"), "edited lib\n").unwrap();
    std::fs::write(f.dir.join("vendor/lib/extra.txt"), "extra\n").unwrap();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let sub = &got.manifest.submodules[0];
    assert!(sub.dirty && sub.source_id.is_some());
    assert_ne!(got.manifest.source_id, clean.manifest.source_id);

    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    let plan = a.plan_transfer(&got.manifest.ci_source_id, &b).unwrap();
    assert_eq!(plan.manifests.len(), 2, "the submodule capture goes first");
    assert_eq!(plan.manifests[0], sub.source_id.clone().unwrap());
    send(&a, &b, &got.manifest.ci_source_id);
    let ci = b.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let dest = other.path().join("ws");
    b.materialize(&ci, &dest).unwrap();
    assert_eq!(
        std::fs::read(dest.join("vendor/lib/lib.txt")).unwrap(),
        b"edited lib\n"
    );
    assert_eq!(
        std::fs::read(dest.join("vendor/lib/extra.txt")).unwrap(),
        b"extra\n"
    );
}

#[test]
fn a_submodule_capture_that_has_not_arrived_blocks_publication() {
    let f = fixture();
    with_submodule(&f);
    std::fs::write(f.dir.join("vendor/lib/lib.txt"), "edited lib\n").unwrap();
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    let ci = a.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    for w in &a.plan_transfer(&ci.source_id, &b).unwrap().blobs {
        b.ingest_blob(w, &mut &a.read_blob(&w.hash).unwrap()[..])
            .unwrap();
    }
    let err = b.publish_received(&ci).unwrap_err();
    assert!(err.to_string().contains("submodule"), "{err}");
    assert!(b.manifest(&ci.source_id).unwrap().is_none());
}

// ---- LFS, with a real git-lfs ----------------------------------------------

fn lfs_repo(f: &Fixture) -> Vec<u8> {
    git(&f.dir, &["lfs", "install", "--local"]);
    git(&f.dir, &["lfs", "track", "*.dat"]);
    let payload = b"a large file\n".repeat(200);
    std::fs::write(f.dir.join("big.dat"), &payload).unwrap();
    git(&f.dir, &["add", ".gitattributes", "big.dat"]);
    git(&f.dir, &["commit", "-qm", "lfs"]);
    payload
}

#[test]
fn a14_an_lfs_file_is_captured_as_its_payload_and_the_pointer_is_not_what_travels() {
    let f = fixture();
    let payload = lfs_repo(&f);
    // the worktree holds the payload: a clean file as far as Git is concerned
    let a = f.store();
    let got = capture(&f.env(), &a, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(got.manifest.lfs.len(), 1);
    let l = &got.manifest.lfs[0];
    assert_eq!(l.path.display(), "big.dat");
    assert_eq!(l.size, payload.len() as u64);
    let e = got
        .manifest
        .object_manifest
        .entries
        .iter()
        .find(|e| e.path.display() == "big.dat")
        .unwrap();
    assert_eq!(e.hash, l.oid, "the blob is the payload");

    let other = common::tempdir().unwrap();
    let b = SourceStore::open(other.path()).unwrap();
    send(&a, &b, &got.manifest.ci_source_id);
    let ci = b.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let dest = other.path().join("ws");
    b.materialize(&ci, &dest).unwrap();
    assert_eq!(std::fs::read(dest.join("big.dat")).unwrap(), payload);
}

#[test]
fn a14_a_missing_lfs_payload_stops_before_anything_runs() {
    let f = fixture();
    lfs_repo(&f);
    // the file is back to its pointer and the LFS store has no payload
    git(&f.dir, &["rm", "-q", "--cached", "big.dat"]);
    std::fs::remove_dir_all(f.dir.join(".git/lfs")).unwrap();
    let pointer = git_out(&f.dir, &["show", "HEAD:big.dat"]);
    std::fs::write(f.dir.join("big.dat"), format!("{pointer}\n")).unwrap();
    git(&f.dir, &["reset", "-q", "HEAD", "--", "big.dat"]);
    let store = f.store();
    let err = capture(&f.env(), &store, &f.worktree(), &CaptureParams::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("LFS payload missing"), "{err:?}");
    assert!(manifests_in(&store).is_empty());
}

// ---- --snapshot commit ------------------------------------------------------

fn commit_req(f: &Fixture, rev: &str) -> SourceRequest {
    SourceRequest::Commit {
        dir: f.dir.clone(),
        rev: rev.into(),
    }
}

#[test]
fn a_commit_capture_holds_what_was_committed_and_nothing_else() {
    let f = fixture();
    let first = git_out(&f.dir, &["rev-parse", "HEAD"]);
    std::fs::write(f.dir.join("second.txt"), "2\n").unwrap();
    git(&f.dir, &["add", "second.txt"]);
    git(&f.dir, &["commit", "-qm", "second"]);
    dirty(&f.dir);
    let before = original_state(&f.dir);
    let store = f.store();

    let head = capture(
        &f.env(),
        &store,
        &commit_req(&f, "HEAD"),
        &CaptureParams::default(),
    )
    .unwrap();
    let m = &head.manifest;
    assert_eq!(m.source_kind, SourceKind::Commit);
    assert_eq!(
        m.original_head,
        Some(git_out(&f.dir, &["rev-parse", "HEAD"]))
    );
    assert_eq!(
        m.worktree_tree,
        Some(git_out(&f.dir, &["rev-parse", "HEAD^{tree}"]))
    );
    let ci = store.manifest(&m.ci_source_id).unwrap().unwrap();
    let dest = f.state.join("at-head");
    store.materialize(&ci, &dest).unwrap();
    let l = listing(&dest);
    assert_eq!(l["staged.txt"].2, b"s0\n");
    assert!(!l.contains_key("untracked.txt") && l.contains_key("gone.txt"));
    assert!(!l["run.sh"].1 && l["link"].2 == b"a.txt");
    assert_eq!(original_state(&f.dir), before, "the original was written");

    // an older commit
    let old = capture(
        &f.env(),
        &store,
        &commit_req(&f, &first),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_ne!(old.manifest.source_id, m.source_id);
    assert_eq!(old.manifest.original_head.as_deref(), Some(first.as_str()));
    let ci = store.manifest(&old.manifest.ci_source_id).unwrap().unwrap();
    let dest = f.state.join("at-first");
    store.materialize(&ci, &dest).unwrap();
    assert!(!listing(&dest).contains_key("second.txt"));
    // same repository, same id, whether captured as a worktree or a commit
    let wt = capture(&f.env(), &store, &f.worktree(), &CaptureParams::default()).unwrap();
    assert_eq!(wt.manifest.repository_id, m.repository_id);
    // the same commit is the same source
    let again = capture(
        &f.env(),
        &store,
        &commit_req(&f, "HEAD"),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(again.manifest.source_id, m.source_id);
    // no scratch checkout is left behind
    let left: Vec<_> = std::fs::read_dir(store.root().join("tmp"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().starts_with("commit-"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn a_commit_capture_started_in_a_subdirectory_and_with_a_base() {
    let f = fixture();
    let first = git_out(&f.dir, &["rev-parse", "HEAD"]);
    std::fs::write(f.dir.join("sub dir/more.txt"), "m\n").unwrap();
    git(&f.dir, &["add", "."]);
    git(&f.dir, &["commit", "-qm", "more"]);
    let req = SourceRequest::Commit {
        dir: f.dir.join("sub dir"),
        rev: "HEAD".into(),
    };
    let got = capture(
        &f.env(),
        &f.store(),
        &req,
        &CaptureParams {
            base: Some("HEAD~1".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(got.manifest.cwd_relative.display(), "sub dir");
    assert_eq!(got.manifest.base, Some(first.clone()));
    assert_eq!(got.manifest.merge_base, Some(first));
}

#[test]
fn a_commit_that_does_not_exist_or_looks_like_an_option_is_invalid_input() {
    let f = fixture();
    for rev in [
        "no-such-branch",
        "--output=/tmp/x",
        "",
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    ] {
        let err = capture(
            &f.env(),
            &f.store(),
            &commit_req(&f, rev),
            &CaptureParams::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput, "{rev:?}: {err:?}");
    }
}

#[test]
fn a_commit_with_an_lfs_file_is_captured_as_its_payload() {
    let f = fixture();
    let payload = lfs_repo(&f);
    let store = f.store();
    let got = capture(
        &f.env(),
        &store,
        &commit_req(&f, "HEAD"),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(got.manifest.lfs.len(), 1);
    let ci = store.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
    let dest = f.state.join("lfs-commit");
    store.materialize(&ci, &dest).unwrap();
    assert_eq!(std::fs::read(dest.join("big.dat")).unwrap(), payload);
}

#[test]
fn a_commit_with_a_submodule_is_captured_pinned_at_its_revision() {
    let f = fixture();
    with_submodule(&f);
    let store = f.store();
    let got = capture(
        &f.env(),
        &store,
        &commit_req(&f, "HEAD"),
        &CaptureParams::default(),
    )
    .unwrap();
    assert_eq!(got.manifest.submodules.len(), 1);
    let sub_rev = git_out(&f.dir, &["rev-parse", "HEAD:vendor/lib"]);
    assert_eq!(got.manifest.submodules[0].revision, sub_rev);
    assert!(!got.manifest.submodules[0].dirty);
}
