//! Spec 22.5 end to end, over throwaway repositories: attempt workspaces made
//! and removed (with a stand-in for `fast-worktree`, and with the real one
//! where it is installed), the original never written, concurrent attempts of
//! one repository, directory snapshots, `--in-place`, and a missing tool.
//!
//! The real `fast-worktree` is used when it is on the PATH; set
//! `RTR_REQUIRE_FAST_WORKTREE=1` (CI does) to make its absence a failure.

mod common;

use local_ci::source::SourceManifest as CiManifest;
use local_ci::ExecContext;
use remote_task_runner::error::{BlockedReason, ErrorCode};
use remote_task_runner::ids::AttemptId;
use remote_task_runner::source::{capture, CaptureParams, ObjectsRepo, SourceRequest, SourceStore};
use remote_task_runner::workspace::plan::FwFailure;
use remote_task_runner::workspace::{
    CliFastWorktree, FastWorktree, Workspace, WorkspaceKind, Workspaces,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const BASE_PATH: &str = "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin";

fn path_var() -> String {
    // the real tool lives in cargo's bin directory
    match std::env::var("HOME") {
        Ok(h) => format!("{BASE_PATH}:{h}/.cargo/bin"),
        Err(_) => BASE_PATH.to_string(),
    }
}

fn env(home: &Path) -> BTreeMap<String, String> {
    [
        ("PATH".to_string(), path_var()),
        ("HOME".to_string(), home.to_str().unwrap().to_string()),
        ("GIT_CONFIG_GLOBAL".to_string(), "/dev/null".to_string()),
        ("GIT_CONFIG_NOSYSTEM".to_string(), "1".to_string()),
        ("GIT_AUTHOR_NAME".to_string(), "t".to_string()),
        ("GIT_AUTHOR_EMAIL".to_string(), "t@example.com".to_string()),
        ("GIT_COMMITTER_NAME".to_string(), "t".to_string()),
        (
            "GIT_COMMITTER_EMAIL".to_string(),
            "t@example.com".to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

fn git_out(env: &BTreeMap<String, String>, dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-c", "protocol.file.allow=always"])
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(env)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim_end().to_string()
}

/// A stand-in for `fast-worktree` that needs only Git: it refuses a dirty
/// worktree on remove, as the real one does, and runs the project's hooks not
/// at all.
struct GitFw {
    env: BTreeMap<String, String>,
}

/// `git worktree add` and `remove` race with each other in one repository
/// (Git lists `.git/worktrees` while another call deletes an entry); the real
/// tool unregisters only its own entry, so the stand-in is serialized.
static GIT_WORKTREE: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl FastWorktree for GitFw {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        let _g = GIT_WORKTREE.lock().unwrap_or_else(|e| e.into_inner());
        let out = Command::new("git")
            .args(["worktree", "add", "-q", "-b", branch])
            .arg(dest)
            .arg(start)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if out.status.success() {
            Ok(())
        } else {
            Err(FwFailure::Failed {
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            })
        }
    }

    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        let _g = GIT_WORKTREE.lock().unwrap_or_else(|e| e.into_inner());
        let st = Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=all"])
            .current_dir(path)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if !st.stdout.is_empty() {
            return Err(FwFailure::Failed {
                code: Some(1),
                stderr: "uncommitted or untracked changes".into(),
            });
        }
        let out = Command::new("git")
            .args(["worktree", "remove"])
            .arg(path)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if out.status.success() {
            Ok(())
        } else {
            Err(FwFailure::Failed {
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            })
        }
    }
}

struct Backend {
    name: &'static str,
    real: bool,
    make: fn(BTreeMap<String, String>) -> Arc<dyn FastWorktree>,
}

impl Backend {
    /// The tool for a fixture: its own HOME, so no cache is shared.
    fn fw(&self, f: &Fixture) -> Arc<dyn FastWorktree> {
        (self.make)(f.env())
    }
}

fn real_available(home: &Path) -> bool {
    Command::new("fast-worktree")
        .arg("--version")
        .env_clear()
        .envs(env(home))
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn backends(home: &Path) -> Vec<Backend> {
    let mut v = vec![Backend {
        name: "git",
        real: false,
        make: |env| Arc::new(GitFw { env }),
    }];
    if real_available(home) {
        v.push(Backend {
            name: "fast-worktree",
            real: true,
            make: |env| Arc::new(CliFastWorktree::new(env)),
        });
    } else if std::env::var("RTR_REQUIRE_FAST_WORKTREE").is_ok() {
        panic!("fast-worktree is required here and is not installed");
    } else {
        eprintln!("fast-worktree is not installed: only the stand-in is tested");
    }
    v
}

struct Fixture {
    _tmp: common::Tmp,
    root: PathBuf,
    /// the repository the user works in
    dir: PathBuf,
    /// the worker's state directory
    state: PathBuf,
}

impl Fixture {
    fn env(&self) -> BTreeMap<String, String> {
        env(&self.root.join("home"))
    }
    fn git(&self, dir: &Path, args: &[&str]) -> String {
        git_out(&self.env(), dir, args)
    }
    fn store(&self) -> SourceStore {
        SourceStore::open(&self.root.join("capture-state")).unwrap()
    }
    fn workspaces(&self, fw: &Arc<dyn FastWorktree>) -> Workspaces {
        Workspaces::new(self.state.clone(), self.env(), fw.clone())
    }
    fn clone_of(&self, ws: &Workspaces) -> ObjectsRepo {
        ws.ensure_clone("github:example/repo", Some(self.dir.to_str().unwrap()))
            .unwrap()
    }
    /// The bundle of `m` unpacked into the clone, as a transfer would.
    fn deliver(&self, store: &SourceStore, repo: &ObjectsRepo, m: &CiManifest) {
        if repo.has_commit(m.execution_commit.as_deref().unwrap()) {
            return;
        }
        let bundle = store
            .export_bundle(&ExecContext::new(self.env()), m)
            .unwrap();
        repo.receive_bundle(&bundle, m).unwrap();
    }
    fn commit(&self, file: &str, content: &str) -> String {
        std::fs::write(self.dir.join(file), content).unwrap();
        self.git(&self.dir, &["add", "."]);
        self.git(&self.dir, &["commit", "-qm", file]);
        self.git(&self.dir, &["rev-parse", "HEAD"])
    }
    fn capture_commit(&self, store: &SourceStore, rev: &str) -> CiManifest {
        let c = capture(
            &self.env(),
            store,
            &SourceRequest::Commit {
                dir: self.dir.clone(),
                rev: rev.to_string(),
            },
            &CaptureParams::default(),
        )
        .unwrap();
        store.manifest(&c.manifest.ci_source_id).unwrap().unwrap()
    }
}

fn fixture() -> Fixture {
    let tmp = common::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let dir = root.join("work");
    std::fs::create_dir_all(root.join("home")).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    let f = Fixture {
        state: root.join("worker-state"),
        _tmp: tmp,
        root,
        dir,
    };
    f.git(&f.dir, &["init", "-q", "-b", "main"]);
    std::fs::write(f.dir.join(".gitignore"), "deps/\nlocal.env\n").unwrap();
    std::fs::write(
        f.dir.join(".fast-worktree.toml"),
        "[[post_add]]\nrun = \"mkdir -p deps && echo ready > deps/ready\"\n\n\
         [[pre_remove]]\nrun = \"echo gone > \\\"$FAST_WORKTREE_MAIN/.git/pre-remove-ran\\\"\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(f.dir.join("pkg")).unwrap();
    std::fs::write(f.dir.join("pkg/lib.txt"), "lib\n").unwrap();
    std::fs::write(f.dir.join("a.txt"), "a\n").unwrap();
    std::fs::write(f.dir.join("run.sh"), "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(
        f.dir.join("run.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    f.git(&f.dir, &["add", "."]);
    f.git(&f.dir, &["commit", "-qm", "base"]);
    f
}

/// Everything under `dir`, `.git` included: relative path, type, mode, and
/// the bytes (or link target).
fn tree_hash(dir: &Path) -> String {
    use std::os::unix::fs::PermissionsExt;
    fn walk(root: &Path, rel: &Path, h: &mut Sha256) {
        let mut names: Vec<_> = std::fs::read_dir(root.join(rel))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        for n in names {
            let p = rel.join(&n);
            let full = root.join(&p);
            let meta = std::fs::symlink_metadata(&full).unwrap();
            h.update(p.to_string_lossy().as_bytes());
            h.update(format!(":{:o}:", meta.permissions().mode()).as_bytes());
            if meta.is_dir() {
                h.update(b"d");
                walk(root, &p, h);
            } else if meta.file_type().is_symlink() {
                h.update(
                    std::fs::read_link(&full)
                        .unwrap()
                        .to_string_lossy()
                        .as_bytes(),
                );
            } else {
                h.update(std::fs::read(&full).unwrap());
            }
        }
    }
    let mut h = Sha256::new();
    walk(dir, Path::new(""), &mut h);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn worktrees(f: &Fixture, clone: &Path) -> Vec<String> {
    f.git(clone, &["worktree", "list", "--porcelain"])
        .lines()
        .filter_map(|l| l.strip_prefix("worktree ").map(str::to_string))
        .collect()
}

fn branches(f: &Fixture, clone: &Path) -> Vec<String> {
    f.git(
        clone,
        &["branch", "--list", "rtr/*", "--format=%(refname:short)"],
    )
    .lines()
    .map(str::to_string)
    .collect()
}

#[test]
fn add_and_remove_a_commit_workspace() {
    for b in backends(&common_home()) {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        assert!(w.root.ends_with(att.as_str()), "{}", b.name);
        assert_eq!(w.cwd, w.root);
        assert_eq!(
            std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
            "a\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("pkg/lib.txt")).unwrap(),
            "lib\n"
        );
        assert!(w.root.join(".git").exists());
        if b.real {
            // the project's own preparation ran
            assert_eq!(
                std::fs::read_to_string(w.root.join("deps/ready")).unwrap(),
                "ready\n"
            );
        }
        assert_eq!(branches(&f, repo.dir()).len(), 1);
        // the run leaves tracked changes and new files behind
        std::fs::write(w.root.join("a.txt"), "changed\n").unwrap();
        std::fs::write(w.root.join("out.log"), "x\n").unwrap();
        ws.remove(&w).unwrap();
        assert!(!w.root.exists(), "{}", b.name);
        assert!(branches(&f, repo.dir()).is_empty());
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
        if b.real {
            assert!(repo.dir().join(".git/pre-remove-ran").exists());
        }
        // removing again is fine
        ws.remove(&w).unwrap();
    }
}

fn common_home() -> PathBuf {
    // a home for the stand-in checks that never exists: only the PATH matters
    PathBuf::from("/nonexistent-home")
}

#[test]
fn a_subdirectory_is_the_working_directory() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Commit {
            dir: f.dir.join("pkg"),
            rev: "HEAD".into(),
        },
        &CaptureParams::default(),
    )
    .unwrap();
    let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    let w = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    assert_eq!(w.cwd, w.root.join("pkg"));
    assert!(w.cwd.join("lib.txt").exists());
    ws.remove(&w).unwrap();
}

#[test]
fn the_original_is_never_written() {
    for b in backends(&common_home()) {
        let f = fixture();
        // dirty: a tracked change, an untracked file, an ignored file to take
        std::fs::write(f.dir.join("a.txt"), "dirty\n").unwrap();
        std::fs::write(f.dir.join("new.txt"), "new\n").unwrap();
        std::fs::write(f.dir.join("local.env"), "SECRET_FREE=1\n").unwrap();
        let store = f.store();
        let c = capture(
            &f.env(),
            &store,
            &SourceRequest::Worktree { dir: f.dir.clone() },
            &CaptureParams {
                include: vec!["local.env".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        // the synthetic commit reaches the worker as a bundle
        f.deliver(&store, &repo, &m);
        let before = tree_hash(&f.dir);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        // the dirty state, and the ignored file that was asked for, arrived
        assert_eq!(
            std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
            "dirty\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("new.txt")).unwrap(),
            "new\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("local.env")).unwrap(),
            "SECRET_FREE=1\n"
        );
        // and the run wrecks the workspace
        std::fs::write(w.root.join("a.txt"), "wrecked\n").unwrap();
        std::fs::remove_file(w.root.join("pkg/lib.txt")).unwrap();
        ws.remove(&w).unwrap();
        assert_eq!(
            tree_hash(&f.dir),
            before,
            "{}: the original changed",
            b.name
        );
    }
}

#[test]
fn a_failed_unpack_leaves_nothing_behind() {
    for b in backends(&common_home()) {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        // a store that does not hold the blobs
        let empty = SourceStore::open(&f.root.join("empty-state")).unwrap();
        let att = AttemptId::generate();
        let e = ws.create(&att, &empty, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal, "{}: {e:?}", b.name);
        assert!(!f.state.join("workspaces").join(att.as_str()).exists());
        assert!(branches(&f, repo.dir()).is_empty());
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
        let scratch: Vec<_> = std::fs::read_dir(f.state.join("workspaces"))
            .unwrap()
            .collect();
        assert!(scratch.is_empty(), "scratch directories were left");
        // the same attempt can be tried again with the right store
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        ws.remove(&w).unwrap();
    }
}

#[test]
fn a_directory_snapshot_gets_a_plain_directory() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let plain = f.root.join("plain");
    std::fs::create_dir_all(plain.join("sub")).unwrap();
    std::fs::write(plain.join("sub/x.txt"), "x\n").unwrap();
    std::fs::write(plain.join("top.txt"), "t\n").unwrap();
    let store = f.store();
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Directory {
            dir: plain.join("sub"),
        },
        &CaptureParams::default(),
    )
    .unwrap();
    let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    let ws = f.workspaces(&b.fw(&f));
    let att = AttemptId::generate();
    let w = ws.create(&att, &store, &m, None).unwrap();
    assert_eq!(w.kind, WorkspaceKind::Directory);
    assert_eq!(std::fs::read_to_string(w.cwd.join("x.txt")).unwrap(), "x\n");
    assert!(!w.root.join(".git").exists());
    // a second create of the same attempt does not touch the first
    let e = ws.create(&att, &store, &m, None).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert!(w.cwd.join("x.txt").exists());
    ws.remove(&w).unwrap();
    assert!(!w.root.exists());
    // a failed unpack removes its directory
    let empty = SourceStore::open(&f.root.join("empty-state")).unwrap();
    let att2 = AttemptId::generate();
    assert!(ws.create(&att2, &empty, &m, None).is_err());
    assert!(!f.state.join("workspaces").join(att2.as_str()).exists());
}

#[test]
fn a_git_source_needs_its_clone_with_the_commit() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let ws = f.workspaces(&b.fw(&f));
    let e = ws
        .create(&AttemptId::generate(), &store, &m, None)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // an empty clone does not hold the commit
    let empty = ws.ensure_clone("github:example/empty", None).unwrap();
    let e = ws
        .create(&AttemptId::generate(), &store, &m, Some(&empty))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
}

#[test]
fn concurrent_attempts_of_one_repository_do_not_collide() {
    for b in backends(&common_home()) {
        let f = fixture();
        let store = Arc::new(f.store());
        let first = f.git(&f.dir, &["rev-parse", "HEAD"]);
        let second = f.commit("second.txt", "two\n");
        let m1 = f.capture_commit(&store, &first);
        let m2 = f.capture_commit(&store, &second);
        // receiving is the transfer layer's, one at a time; the workspaces are ours
        {
            let ws = f.workspaces(&b.fw(&f));
            let repo = f.clone_of(&ws);
            f.deliver(&store, &repo, &m1);
            f.deliver(&store, &repo, &m2);
        }
        // every thread asks for the clone itself, as separate processes would
        let results: Vec<_> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|i| {
                    let (f, store, b) = (&f, &store, &b);
                    let (m1, m2) = (&m1, &m2);
                    s.spawn(move || {
                        let ws = f.workspaces(&b.fw(f));
                        let repo = f.clone_of(&ws);
                        let m = if i % 2 == 0 { m1 } else { m2 };
                        let att = AttemptId::generate();
                        let w = ws.create(&att, store, m, Some(&repo)).unwrap();
                        (i, w, ws)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let clones: std::collections::BTreeSet<_> = results
            .iter()
            .map(|(_, w, _)| match &w.kind {
                WorkspaceKind::Worktree { clone, .. } => clone.clone(),
                k => panic!("{k:?}"),
            })
            .collect();
        assert_eq!(clones.len(), 1, "{}: one canonical clone", b.name);
        let roots: std::collections::BTreeSet<_> =
            results.iter().map(|(_, w, _)| w.root.clone()).collect();
        assert_eq!(roots.len(), 8);
        for (i, w, _) in &results {
            let has_second = w.root.join("second.txt").exists();
            assert_eq!(has_second, i % 2 == 1, "{}: attempt {i}", b.name);
            assert_eq!(
                std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
                "a\n"
            );
        }
        let clone = clones.into_iter().next().unwrap();
        assert_eq!(branches(&f, &clone).len(), 8);
        // and they go away together
        std::thread::scope(|s| {
            for (_, w, ws) in &results {
                s.spawn(move || ws.remove(w).unwrap());
            }
        });
        assert!(branches(&f, &clone).is_empty(), "{}", b.name);
        assert_eq!(worktrees(&f, &clone).len(), 1);
    }
}

#[test]
fn a_missing_or_failing_fast_worktree_is_an_environment_mismatch() {
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    // not on the PATH it is given
    let mut none = env(&f.root.join("home"));
    none.insert("PATH".into(), "/nonexistent-bin".into());
    let missing: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(none));
    // a stand-in that fails
    struct Broken;
    impl FastWorktree for Broken {
        fn add(&self, _: &Path, _: &Path, _: &str, _: &str) -> Result<(), FwFailure> {
            Err(FwFailure::Failed {
                code: Some(1),
                stderr: "FAILED post_add".into(),
            })
        }
        fn remove(&self, _: &Path, _: &Path) -> Result<(), FwFailure> {
            Err(FwFailure::Failed {
                code: Some(1),
                stderr: "stuck".into(),
            })
        }
    }
    let broken: Arc<dyn FastWorktree> = Arc::new(Broken);
    for fw in [missing, broken] {
        let ws = f.workspaces(&fw);
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        let e = ws.create(&att, &store, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Blocked, "{e:?}");
        assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(!f.state.join("workspaces").join(att.as_str()).exists());
        assert!(branches(&f, repo.dir()).is_empty());
    }
}

#[test]
fn in_place_uses_the_original_and_never_removes_it() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let before = tree_hash(&f.dir);
    // only the parent's local worker
    let e = ws.open_in_place(&f.dir, Path::new(""), false).unwrap_err();
    assert_eq!(e.blocked_reason, Some(BlockedReason::NoCompatibleWorker));
    let w = ws.open_in_place(&f.dir, Path::new("pkg"), true).unwrap();
    assert_eq!(w.kind, WorkspaceKind::InPlace);
    assert_eq!(w.root, std::fs::canonicalize(&f.dir).unwrap());
    assert_eq!(w.cwd, w.root.join("pkg"));
    ws.remove(&w).unwrap();
    assert_eq!(tree_hash(&f.dir), before, "the original changed");
    assert!(f.dir.join(".git").exists());
    // the runner's own state is not an in-place directory
    std::fs::create_dir_all(&f.state).unwrap();
    for d in [f.state.clone(), f.state.join("workspaces"), f.root.clone()] {
        std::fs::create_dir_all(&d).unwrap();
        let e = ws.open_in_place(&d, Path::new(""), true).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput, "{d:?}");
    }
    let e = ws
        .open_in_place(&f.root.join("nope"), Path::new(""), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    let e = ws
        .open_in_place(&f.dir, Path::new("missing"), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    let e = ws
        .open_in_place(&f.dir.join("a.txt"), Path::new(""), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
}

#[test]
fn only_attempt_workspaces_can_be_removed() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let before = tree_hash(&f.dir);
    for root in [f.dir.clone(), f.state.join("workspaces"), f.root.clone()] {
        let w = Workspace {
            cwd: root.clone(),
            root,
            kind: WorkspaceKind::Directory,
        };
        let e = ws.remove(&w).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
    }
    assert_eq!(tree_hash(&f.dir), before);
}

#[test]
fn the_clone_is_made_once_and_updated_from_origin() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    let again = f.clone_of(&ws);
    assert_eq!(repo.dir(), again.dir());
    let head = f.commit("later.txt", "later\n");
    assert!(!repo.has_commit(&head));
    ws.update_clone(&repo).unwrap();
    assert!(repo.has_commit(&head));
    // an empty clone has no origin to fetch from: nothing to do
    let empty = ws.ensure_clone("github:example/empty", None).unwrap();
    ws.update_clone(&empty).unwrap();
}

#[test]
fn a_create_after_a_crash_clears_the_stale_branch_and_registration() {
    for b in backends(&common_home()) {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        // a worktree made, then its directory lost (a crash, a cleaned disk)
        let dir = f.state.join("workspaces").join(att.as_str());
        std::fs::create_dir_all(f.state.join("workspaces")).unwrap();
        let commit = m.execution_commit.clone().unwrap();
        f.git(
            repo.dir(),
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                &format!("rtr/{att}"),
                dir.to_str().unwrap(),
                &commit,
            ],
        );
        std::fs::remove_dir_all(&dir).unwrap();
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        assert!(w.root.join("a.txt").exists(), "{}", b.name);
        ws.remove(&w).unwrap();
        assert!(branches(&f, repo.dir()).is_empty());
    }
}
