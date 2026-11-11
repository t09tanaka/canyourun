//! The setup builder (spec chapter 8, A15 to A17): one build for concurrent
//! consumers, a builder that dies or is cancelled, immutable templates,
//! receipt verification, and what a crash leaves. Builders are real
//! `supervise` processes; every wait is bounded and a failing test leaves no
//! process behind (the fixture kills what still runs under its directory).

use crate::common::kill_if_command;
use remote_task_runner::error::ApiError;
use remote_task_runner::ids::{
    AttemptId, Digest, Generation, PlanDigest, RunId, SourceId, TaskId, WorkerId,
};
use remote_task_runner::setup::builder::{BuilderWorkspaces, SetupConfig, SetupPoll, Setups};
use remote_task_runner::setup::store::remove_tree;
use remote_task_runner::wire::{AttemptPayload, ReuseScope, SetupSpec, SetupStep};
use remote_task_runner::workspace::{Workspace, WorkspaceKind};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_remote-task-runner");
const LIMIT: Duration = Duration::from_secs(90);

/// Workspaces as plain directories with a file in them.
struct DirWs {
    base: PathBuf,
}

impl DirWs {
    fn make(&self, attempt: &AttemptId) -> Workspace {
        let root = self.base.join("ws").join(attempt.as_str());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("package.json"), "{}\n").unwrap();
        Workspace {
            cwd: root.clone(),
            root,
            kind: WorkspaceKind::Directory,
            index_reproduced: true,
            warnings: Vec::new(),
        }
    }
}

impl BuilderWorkspaces for DirWs {
    fn create(&self, attempt: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
        Ok(self.make(attempt))
    }
    fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
        remove_tree(&ws.root).unwrap();
        Ok(())
    }
}

struct Fx {
    dir: tempfile::TempDir,
    ws: Arc<DirWs>,
    setups: Arc<Setups>,
    run: RunId,
}

fn cfg(base: &Path) -> SetupConfig {
    let mut c = SetupConfig::new(
        base.join("state"),
        WorkerId::parse("w-test").unwrap(),
        PathBuf::from(BIN),
    );
    c.poll_ms = 25;
    c.lease_ms = 3_000;
    c.term_grace_ms = 1_000;
    c.kill_wait_ms = 2_000;
    c.deadline_ms = 120_000;
    c.env = BTreeMap::from([
        (
            "PATH".to_string(),
            "/usr/bin:/bin:/usr/sbin:/sbin".to_string(),
        ),
        ("HOME".to_string(), base.display().to_string()),
    ]);
    c
}

fn fx() -> Fx {
    fx_with(|_, _| {})
}

/// A fixture whose configuration `change` may alter (it gets the fixture's
/// directory too).
fn fx_with(change: impl FnOnce(&mut SetupConfig, &Path)) -> Fx {
    // Not under the system temp directory: a process a failed test leaves
    // behind must not write into a directory a test runner clears.
    let dir = tempfile::Builder::new()
        .prefix("rtr-setup-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let ws = Arc::new(DirWs {
        base: dir.path().to_path_buf(),
    });
    let mut c = cfg(dir.path());
    change(&mut c, dir.path());
    let setups = Setups::open(c, ws.clone()).unwrap();
    Fx {
        dir,
        ws,
        setups,
        run: RunId::generate(),
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        // Whatever still runs for this fixture: found by its directory, which
        // every supervisor's and every step's command line holds, and killed
        // only while it still is the process with that command line.
        let pattern = self.dir.path().to_string_lossy().into_owned();
        for pid in pgrep(&pattern) {
            kill_if_command(pid, &pattern);
        }
        // the files sealed read-only
        let _ = remove_tree(self.dir.path());
    }
}

fn pgrep(pattern: &str) -> Vec<i32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", pattern])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

fn wait_for(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + LIMIT;
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn step(id: &str, run: &str) -> SetupStep {
    SetupStep {
        id: id.into(),
        dir: String::new(),
        run: run.into(),
        shell: None,
        env: BTreeMap::new(),
        unresolved_env: vec![],
    }
}

fn spec(steps: Vec<SetupStep>) -> SetupSpec {
    SetupSpec {
        steps,
        toolchain: vec![],
        scope: ReuseScope::Persistent,
        freshness_secs: None,
        secrets: vec![],
        no_cache: false,
        step_timeout_ms: None,
    }
}

impl Fx {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    fn templates(&self) -> PathBuf {
        self.state().join("setup/templates")
    }

    fn builders(&self) -> PathBuf {
        self.state().join("setup/builders")
    }

    fn payload(&self, spec: &SetupSpec) -> AttemptPayload {
        self.payload_of(&self.run, spec)
    }

    fn payload_of(&self, run: &RunId, spec: &SetupSpec) -> AttemptPayload {
        let d = Digest::of_bytes(b"the source");
        AttemptPayload {
            run_id: run.clone(),
            task_id: TaskId::generate(),
            generation: Generation(1),
            source_id: SourceId::generate(),
            plan_digest: PlanDigest::of_bytes(b"plan"),
            manifest_digest: d,
            permit_id: None,
            commands: vec![],
            lease_ms: 60_000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: vec![],
            setup: Some(spec.clone()),
        }
    }

    /// A consumer: an attempt with its own workspace.
    fn consumer(&self) -> (AttemptId, Workspace) {
        let a = AttemptId::generate();
        let ws = self.ws.make(&a);
        (a, ws)
    }

    fn poll(&self, a: &AttemptId, p: &AttemptPayload, ws: &Workspace) -> SetupPoll {
        self.setups.poll(a, p, ws).unwrap()
    }

    /// Ask until it is not `Pending`: (skipped step ids, note).
    fn done(
        &self,
        a: &AttemptId,
        p: &AttemptPayload,
        ws: &Workspace,
    ) -> (Vec<String>, Option<String>) {
        let end = Instant::now() + LIMIT;
        loop {
            if let SetupPoll::Done { skip, note } = self.poll(a, p, ws) {
                return (skip.into_iter().collect(), note);
            }
            assert!(Instant::now() < end, "the setup did not end");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn template_dirs(&self) -> Vec<PathBuf> {
        let Ok(rd) = std::fs::read_dir(self.templates()) else {
            return vec![];
        };
        rd.flatten().map(|e| e.path()).collect()
    }

    fn builder_dirs(&self) -> usize {
        std::fs::read_dir(self.builders()).map_or(0, |rd| rd.count())
    }

    /// The supervisor has recorded the process of the builder's step (it
    /// can be told from any other process after that).
    fn launch_recorded(&self) -> bool {
        use remote_task_runner::storage::WorkerStore;
        let Some(dir) = std::fs::read_dir(self.builders())
            .ok()
            .and_then(|mut rd| rd.next())
            .and_then(|e| e.ok())
            .map(|e| e.path())
        else {
            return false;
        };
        let Ok(meta) = std::fs::read(dir.join("meta.json")) else {
            return false;
        };
        let meta: serde_json::Value = serde_json::from_slice(&meta).unwrap();
        let attempt = AttemptId::parse(meta["attempt_id"].as_str().unwrap()).unwrap();
        let Ok(store) = WorkerStore::open_read_only(&dir) else {
            return false;
        };
        store
            .launches(&attempt)
            .is_ok_and(|l| l.iter().any(|(_, _, ident)| ident.is_some()))
    }

    /// A step that never ends: `bash <dir>/hang.sh`, whose command line
    /// names this fixture's directory, so that it is found (and killed by
    /// the fixture's cleanup) and never taken for another test's.
    fn hang(&self) -> String {
        let path = self.path("hang.sh");
        std::fs::write(&path, "while :; do sleep 1; done\n").unwrap();
        format!("exec bash {}", path.display())
    }

    fn hanging(&self) -> bool {
        !gone(&self.path("hang.sh").to_string_lossy())
    }

    fn supervisors(&self) -> Vec<i32> {
        pgrep(&self.builders().to_string_lossy())
    }
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |t| t.lines().count())
}

fn gone(pattern: &str) -> bool {
    pgrep(pattern).is_empty()
}

/// Installs `node_modules` (and counts how often it ran).
fn install(f: &Fx, count: &str, extra: &str) -> SetupStep {
    step(
        "i1",
        &format!(
            "echo x >> {}; mkdir -p node_modules/dep; echo dep > node_modules/dep/a.txt; {extra}",
            f.path(count).display()
        ),
    )
}

#[test]
fn two_concurrent_consumers_cause_one_build() {
    let f = fx();
    let s = spec(vec![install(&f, "count", "sleep 1")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (b, wb) = f.consumer();
    // the second asks while the first one's builder works
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    assert_eq!(f.poll(&b, &p, &wb), SetupPoll::Pending);
    let (skip_a, note_a) = f.done(&a, &p, &wa);
    let (skip_b, note_b) = f.done(&b, &p, &wb);
    assert_eq!((skip_a, note_a), (vec!["i1".to_string()], None));
    assert_eq!((skip_b, note_b), (vec!["i1".to_string()], None));
    assert_eq!(lines(&f.path("count")), 1);
    assert_eq!(f.setups.builds_started(), 1);
    for w in [&wa, &wb] {
        assert_eq!(
            std::fs::read_to_string(w.root.join("node_modules/dep/a.txt")).unwrap(),
            "dep\n"
        );
    }
    // each has a copy of its own
    std::fs::write(wa.root.join("node_modules/dep/a.txt"), "changed\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(wb.root.join("node_modules/dep/a.txt")).unwrap(),
        "dep\n"
    );
    // a late consumer finds the template and no builder starts
    let (c, wc) = f.consumer();
    assert_eq!(f.done(&c, &p, &wc).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 1);
    assert_eq!(lines(&f.path("count")), 1);
    // nothing of the builder is left but the template
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
    assert!(f.supervisors().is_empty());
    assert_eq!(f.template_dirs().len(), 1);
    assert!(f
        .template_dirs()
        .iter()
        .all(|d| d.join("receipt.json").is_file()));
}

#[test]
fn a_builder_that_dies_releases_its_waiters_and_a_later_attempt_rebuilds() {
    let f = fx();
    let (flag, started) = (f.path("flag"), f.path("started"));
    let run = format!(
        "if [ -e {flag} ]; then mkdir -p node_modules; echo ok > node_modules/x; \
         else touch {flag}; echo go > {started}; {hang}; fi",
        flag = flag.display(),
        started = started.display(),
        hang = f.hang()
    );
    let s = spec(vec![step("i1", &run)]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (b, wb) = f.consumer();
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    assert_eq!(f.poll(&b, &p, &wb), SetupPoll::Pending);
    wait_for("the step to run", || started.exists());
    wait_for("its process to be recorded", || f.launch_recorded());
    // the supervisor dies, as if killed
    let pids = f.supervisors();
    assert!(!pids.is_empty());
    for pid in pids {
        kill_if_command(pid, &f.builders().to_string_lossy());
    }
    // both are released with a clear outcome and run the steps themselves
    for (x, w) in [(&a, &wa), (&b, &wb)] {
        let (skip, note) = f.done(x, &p, w);
        assert!(skip.is_empty());
        let note = note.unwrap();
        assert!(note.contains("setup builder was lost"), "{note}");
        // no half-built template is visible, nothing was copied
        assert!(!w.root.join("node_modules").exists());
    }
    assert!(f.template_dirs().is_empty());
    // the step it started does not run on
    wait_for("the step to be stopped", || !f.hanging());
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
    // a later attempt starts a builder of its own, which succeeds now
    let (c, wc) = f.consumer();
    assert_eq!(f.done(&c, &p, &wc), (vec!["i1".to_string()], None));
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(f.template_dirs().len(), 1);
}

#[test]
fn a_supervisor_that_dies_before_it_started_anything_does_not_make_the_agent_run_the_steps() {
    // The supervisor of this fixture ends with a failure before it has
    // looked at its attempt: the attempt is `Ready`, with no launch. The
    // agent settles it in its own process, and must not run the step there.
    let f = fx_with(|c, dir| {
        let exe = dir.join("dead-supervisor.sh");
        crate::common::write_executable(&exe, "#!/bin/sh\nexit 3\n");
        c.supervisor_exe = exe;
    });
    let ran = f.path("ran");
    let s = spec(vec![step("i1", &format!("echo x >> {}", ran.display()))]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (skip, note) = f.done(&a, &p, &wa);
    assert!(skip.is_empty());
    let note = note.unwrap();
    assert!(note.contains("setup builder was lost"), "{note}");
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
    assert!(f.setups.unsettled().is_empty());
    assert!(!ran.exists(), "the step ran inside the agent");
    assert!(f.template_dirs().is_empty());
}

#[test]
fn a_failed_builder_tells_what_happened_and_nothing_is_kept() {
    let f = fx();
    let s = spec(vec![step("i1", "mkdir -p half; echo x > half/f; exit 3")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (skip, note) = f.done(&a, &p, &wa);
    assert!(skip.is_empty());
    assert!(note.unwrap().contains("exited with 3"));
    assert!(f.template_dirs().is_empty());
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
    // not remembered: the next attempt builds again
    let (b, wb) = f.consumer();
    assert!(f.done(&b, &p, &wb).1.is_some());
    assert_eq!(f.setups.builds_started(), 2);
}

#[test]
fn a_builder_that_times_out_is_stopped() {
    let f = fx();
    let mut s = spec(vec![step("i1", &f.hang())]);
    s.step_timeout_ms = Some(1_000);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (skip, note) = f.done(&a, &p, &wa);
    assert!(skip.is_empty());
    assert!(note.unwrap().contains("timed out"));
    wait_for("the step to be stopped", || !f.hanging());
    assert!(f.template_dirs().is_empty());
}

#[test]
fn a_consumer_that_leaves_does_not_stop_a_builder_others_wait_for() {
    let f = fx();
    let started = f.path("started");
    let s = spec(vec![install(
        &f,
        "count",
        &format!("echo go > {}; sleep 2", started.display()),
    )]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    let (b, wb) = f.consumer();
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    assert_eq!(f.poll(&b, &p, &wb), SetupPoll::Pending);
    wait_for("the step to run", || started.exists());
    f.setups.release(&a);
    assert_eq!(f.done(&b, &p, &wb), (vec!["i1".to_string()], None));
    assert_eq!(f.setups.builds_started(), 1);
    assert_eq!(f.template_dirs().len(), 1);
}

#[test]
fn the_last_consumer_that_leaves_stops_the_builder() {
    let f = fx();
    let started = f.path("started");
    let s = spec(vec![step(
        "i1",
        &format!(
            "mkdir -p node_modules; echo go > {}; {}",
            started.display(),
            f.hang()
        ),
    )]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    wait_for("the step to run", || started.exists());
    f.setups.release(&a);
    wait_for("the step to be stopped", || !f.hanging());
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
    assert!(f.supervisors().is_empty());
    assert!(f.template_dirs().is_empty());
    assert!(std::fs::read_dir(f.dir.path().join("ws"))
        .unwrap()
        .all(|e| e.unwrap().path() == wa.root));
    // an attempt that comes later is not handed the stopped builder
    let (b, wb) = f.consumer();
    assert_eq!(f.poll(&b, &p, &wb), SetupPoll::Pending);
    assert_eq!(f.setups.builds_started(), 2);
    f.setups.release(&b);
    wait_for("the second builder to be stopped", || !f.hanging());
}

#[test]
fn a_new_consumer_does_not_join_a_builder_that_is_being_stopped() {
    let f = fx();
    let started = f.path("started");
    let s = spec(vec![step(
        "i1",
        &format!("echo go >> {}; {}", started.display(), f.hang()),
    )]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    wait_for("the step to run", || started.exists());
    f.setups.release(&a);
    let (b, wb) = f.consumer();
    assert_eq!(f.poll(&b, &p, &wb), SetupPoll::Pending);
    assert_eq!(f.setups.builds_started(), 2);
    f.setups.release(&b);
    wait_for("both builders to be stopped", || !f.hanging());
}

#[test]
fn a_template_is_immutable_and_a_consumer_cannot_change_what_another_sees() {
    use local_ci::tree::tree_digest;
    use std::os::unix::fs::PermissionsExt;
    let f = fx();
    let s = spec(vec![install(&f, "count", "true")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
    let tdir = f.template_dirs().remove(0);
    let tree = tdir.join("tree");
    let before = tree_digest(&tree).unwrap().digest;
    // the template is read-only, and so is its receipt
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode();
    for p in [
        tree.join("node_modules/dep/a.txt"),
        tree.join("node_modules/dep"),
        tdir.join("receipt.json"),
    ] {
        assert_eq!(mode(&p) & 0o222, 0, "{p:?}");
    }
    // the copy is not
    assert_ne!(mode(&wa.root.join("node_modules/dep/a.txt")) & 0o200, 0);
    assert_ne!(mode(&wa.root.join("node_modules/dep")) & 0o200, 0);
    // the first consumer rewrites its copy, adds a file and deletes one
    std::fs::write(wa.root.join("node_modules/dep/a.txt"), "mine\n").unwrap();
    std::fs::write(wa.root.join("node_modules/extra"), "x\n").unwrap();
    // nothing is linked into the template
    let ino = |p: &Path| std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(p).unwrap());
    assert_ne!(
        ino(&wa.root.join("node_modules/dep/a.txt")),
        ino(&tree.join("node_modules/dep/a.txt"))
    );
    assert_eq!(tree_digest(&tree).unwrap().digest, before);
    // a second consumer sees what the template holds
    let (b, wb) = f.consumer();
    assert_eq!(f.done(&b, &p, &wb).0, ["i1"]);
    assert_eq!(
        std::fs::read_to_string(wb.root.join("node_modules/dep/a.txt")).unwrap(),
        "dep\n"
    );
    assert!(!wb.root.join("node_modules/extra").exists());
    assert_eq!(f.setups.builds_started(), 1);
    assert_eq!(tree_digest(&tree).unwrap().digest, before);
}

#[test]
fn building_a_plus_b_leaves_template_a_alone_and_one_cancel_breaks_no_other() {
    use local_ci::tree::tree_digest;
    let f = fx();
    let a_spec = spec(vec![install(&f, "count_a", "true")]);
    let ab_spec = spec(vec![install(&f, "count_a", "true"), {
        let mut s = step(
            "i2",
            &format!(
                "mkdir -p extra; echo b > extra/b; echo y >> {}",
                f.path("count_b").display()
            ),
        );
        s.dir = String::new();
        s
    }]);
    let pa = f.payload(&a_spec);
    let pab = f.payload(&ab_spec);
    let (c1, w1) = f.consumer();
    assert_eq!(f.done(&c1, &pa, &w1).0, ["i1"]);
    let tdir_a = f.template_dirs().remove(0);
    let before = tree_digest(&tdir_a.join("tree")).unwrap().digest;
    // A is in use (c1 holds its copy) while A+B is built, with another
    // consumer that is cancelled while it waits
    let (c2, w2) = f.consumer();
    let (c3, w3) = f.consumer();
    assert_eq!(f.poll(&c2, &pab, &w2), SetupPoll::Pending);
    assert_eq!(f.poll(&c3, &pab, &w3), SetupPoll::Pending);
    f.setups.release(&c2);
    assert_eq!(f.done(&c3, &pab, &w3).0, ["i1", "i2"]);
    // it is another template, in another directory
    let dirs = f.template_dirs();
    assert_eq!(dirs.len(), 2);
    assert_eq!(tree_digest(&tdir_a.join("tree")).unwrap().digest, before);
    assert!(!tdir_a.join("tree/extra").exists());
    assert!(w3.root.join("extra/b").is_file());
    assert!(w1.root.join("node_modules").is_dir() && !w1.root.join("extra").exists());
    // A is still what its consumers get
    let (c4, w4) = f.consumer();
    assert_eq!(f.done(&c4, &pa, &w4).0, ["i1"]);
    assert!(!w4.root.join("extra").exists());
    assert_eq!(f.setups.builds_started(), 2);
}

#[test]
fn a_template_whose_files_changed_is_not_used_and_is_rebuilt() {
    let f = fx();
    let s = spec(vec![install(&f, "count", "true")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
    // somebody changes a file of the template behind the store's back
    let tdir = f.template_dirs().remove(0);
    let file = tdir.join("tree/node_modules/dep/a.txt");
    for p in [tdir.join("tree/node_modules/dep"), file.clone()] {
        let mut perm = std::fs::metadata(&p).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
        std::fs::set_permissions(&p, perm).unwrap();
    }
    std::fs::write(&file, "tampered\n").unwrap();
    // the next consumer is told, gets nothing of it, and runs the steps
    let (b, wb) = f.consumer();
    let (skip, note) = f.done(&b, &p, &wb);
    assert!(skip.is_empty());
    assert!(note.unwrap().contains("receipt"));
    assert!(!wb.root.join("node_modules").exists());
    assert!(std::fs::read_dir(&wb.root).unwrap().all(|e| !e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".tmp-")));
    assert!(f.template_dirs().is_empty());
    // the one after that gets a rebuilt template
    let (c, wc) = f.consumer();
    assert_eq!(f.done(&c, &p, &wc).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(
        std::fs::read_to_string(wc.root.join("node_modules/dep/a.txt")).unwrap(),
        "dep\n"
    );
}

#[test]
fn a_damaged_receipt_means_a_rebuild() {
    use std::os::unix::fs::PermissionsExt;
    let f = fx();
    let s = spec(vec![install(&f, "count", "true")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
    let receipt = f.template_dirs().remove(0).join("receipt.json");
    let text = std::fs::read_to_string(&receipt).unwrap();
    std::fs::set_permissions(&receipt, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(
        &receipt,
        text.replace("\"steps\": [\n    \"i1\"", "\"steps\": [\n    \"zz\""),
    )
    .unwrap();
    let (b, wb) = f.consumer();
    assert_eq!(f.done(&b, &p, &wb).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 2);
}

#[test]
fn a_workspace_that_holds_the_path_already_is_not_written_over() {
    let f = fx();
    let s = spec(vec![install(&f, "count", "true")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
    let (b, wb) = f.consumer();
    std::fs::create_dir_all(wb.root.join("node_modules")).unwrap();
    std::fs::write(wb.root.join("node_modules/mine"), "x").unwrap();
    let (skip, note) = f.done(&b, &p, &wb);
    assert!(skip.is_empty());
    assert!(note.unwrap().contains("node_modules"));
    assert_eq!(
        std::fs::read_to_string(wb.root.join("node_modules/mine")).unwrap(),
        "x"
    );
    // the template stays for the others
    assert_eq!(f.template_dirs().len(), 1);
}

#[test]
fn no_cache_does_not_reuse_another_runs_template_but_shares_within_the_run() {
    let f = fx();
    let s = spec(vec![install(&f, "count", "sleep 1")]);
    let mut nc = s.clone();
    nc.no_cache = true;
    // run 1 builds a persistent template
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &f.payload(&s), &wa).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 1);
    // run 2 reuses it
    let run2 = RunId::generate();
    let (b, wb) = f.consumer();
    assert_eq!(f.done(&b, &f.payload_of(&run2, &s), &wb).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 1);
    // run 3 asks for no cache: it builds its own, and its two consumers
    // share that one build
    let run3 = RunId::generate();
    let p3 = f.payload_of(&run3, &nc);
    let (c, wc) = f.consumer();
    let (d, wd) = f.consumer();
    assert_eq!(f.poll(&c, &p3, &wc), SetupPoll::Pending);
    assert_eq!(f.poll(&d, &p3, &wd), SetupPoll::Pending);
    assert_eq!(f.done(&c, &p3, &wc).0, ["i1"]);
    assert_eq!(f.done(&d, &p3, &wd).0, ["i1"]);
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(lines(&f.path("count")), 2);
    assert_eq!(f.template_dirs().len(), 2);
}

#[test]
fn each_worker_builds_its_own_template() {
    // A15: no consumer depends on another host's setup having been done.
    let f1 = fx();
    let f2 = fx();
    let s = spec(vec![install(&f1, "count", "true")]);
    for f in [&f1, &f2] {
        let p = f.payload(&s);
        let (a, wa) = f.consumer();
        assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
        assert_eq!(f.setups.builds_started(), 1);
        assert!(wa.root.join("node_modules/dep/a.txt").is_file());
    }
    assert_eq!(lines(&f1.path("count")), 2);
}

#[test]
fn steps_that_cannot_be_prepared_in_advance_stay_with_their_consumer() {
    let f = fx();
    let mut s = spec(vec![install(&f, "count", "true"), step("i2", "true")]);
    s.steps[0].unresolved_env = vec!["X".into()];
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(
        f.poll(&a, &p, &wa),
        SetupPoll::Done {
            skip: Default::default(),
            note: None
        }
    );
    assert_eq!(f.setups.builds_started(), 0);
    // a payload without a setup asks for nothing
    let mut none = f.payload(&s);
    none.setup = None;
    assert_eq!(
        f.poll(&a, &none, &wa),
        SetupPoll::Done {
            skip: Default::default(),
            note: None
        }
    );
    // and a spec that is not valid is refused
    let mut bad = spec(vec![step("i1", "true")]);
    bad.steps[0].dir = "../out".into();
    let e = f
        .setups
        .poll(&AttemptId::generate(), &f.payload(&bad), &wa)
        .unwrap_err();
    assert_eq!(e.code, remote_task_runner::error::ErrorCode::InvalidInput);
}

#[test]
fn what_a_crash_left_is_cleaned_at_the_start() {
    let f = fx();
    let s = spec(vec![install(&f, "count", "true")]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.done(&a, &p, &wa).0, ["i1"]);
    let good = f.template_dirs().remove(0);
    // garbage of a crash: a scratch directory, a template without a receipt,
    // a builder that never started with its workspace, a builder directory
    // that was not described
    let tmp = f.templates().join(".tmp-01ARZ3NDEKTSV4RRFFQ69G5FAV");
    std::fs::create_dir_all(tmp.join("tree/node_modules")).unwrap();
    std::fs::write(tmp.join("tree/node_modules/x"), "x").unwrap();
    let half = f.templates().join("sha256-half");
    std::fs::create_dir_all(half.join("tree")).unwrap();
    let dead = AttemptId::generate();
    let dead_ws = f.ws.make(&dead);
    let bdir = f
        .builders()
        .join(remote_task_runner::ids::BuilderId::generate().as_str());
    std::fs::create_dir_all(&bdir).unwrap();
    std::fs::write(
        bdir.join("meta.json"),
        format!(
            r#"{{"builder_id":"{}","attempt_id":"{dead}","key":"sha256:k"}}"#,
            remote_task_runner::ids::BuilderId::generate()
        ),
    )
    .unwrap();
    std::fs::write(
        bdir.join("workspace.json"),
        serde_json::to_vec(&dead_ws).unwrap(),
    )
    .unwrap();
    let undescribed = f.builders().join("bld_undescribed");
    std::fs::create_dir_all(undescribed.join("junk")).unwrap();
    // a new start
    let again = Setups::open(cfg(f.dir.path()), f.ws.clone()).unwrap();
    assert!(!tmp.exists() && !half.exists());
    assert!(!bdir.exists() && !undescribed.exists());
    assert!(!dead_ws.root.exists());
    assert!(good.join("receipt.json").is_file());
    assert!(again.unsettled().is_empty());
    assert!(!again.owns_workspace(&dead));
}

#[test]
fn a_builder_still_alive_at_the_start_is_stopped_and_its_workspace_kept_meanwhile() {
    let f = fx();
    let started = f.path("started");
    let s = spec(vec![step(
        "i1",
        &format!("echo go > {}; {}", started.display(), f.hang()),
    )]);
    let p = f.payload(&s);
    let (a, wa) = f.consumer();
    assert_eq!(f.poll(&a, &p, &wa), SetupPoll::Pending);
    wait_for("the step to run", || started.exists());
    // another start of the setup area while that supervisor runs
    let again = Setups::open(cfg(f.dir.path()), f.ws.clone()).unwrap();
    let left = again.unsettled();
    assert_eq!(left.len(), 1);
    // the references of the builder are on disk
    let bdir = std::fs::read_dir(f.builders())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let refs: Vec<String> =
        serde_json::from_slice(&std::fs::read(bdir.join("consumers.json")).unwrap()).unwrap();
    assert_eq!(refs, [a.to_string()]);
    assert!(again.owns_workspace(&left[0]));
    // the supervisor was asked to stop; its builder (the first Setups) sees
    // it end and clears up
    wait_for("the step to be stopped", || !f.hanging());
    let (skip, note) = f.done(&a, &p, &wa);
    assert!(skip.is_empty() && note.is_some());
    wait_for("the builder to be removed", || f.builder_dirs() == 0);
}
