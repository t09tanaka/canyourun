//! The blob operations of the worker: a source sent as manifests, files and a
//! bundle becomes usable by `submit_attempt`; transfers resume, concurrent
//! senders do not disturb each other, and what the other host says (sizes,
//! digests, manifests, bundles) is checked before it is kept. `blob_get`
//! serves the artifacts of an attempt and nothing else.

use crate::proto_common::client_cfg;
use local_ci::source::SourceManifest as CiManifest;
use local_ci::ExecContext;
use remote_task_runner::error::{ApiError, BlockedReason, ConflictReason, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transfer::put_blob_as;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{connect, CallError, Client};
use remote_task_runner::source::{capture, CaptureParams, SourceRequest, SourceStore};
use remote_task_runner::state::{AttemptState, Outcome, Termination};
use remote_task_runner::storage::{AcceptedAttempt, NewAcceptance, WorkerStore};
use remote_task_runner::supervisor::outcome::{store_artifact, ArtifactInput, Forbidden};
use remote_task_runner::wire::*;
use remote_task_runner::worker::blob_policy::{
    bundle_prerequisites, lock_shard, parse_reservation, staged_name, BlobLimits, Reservation,
};
use remote_task_runner::worker::blobs::{self, BlobSink, Incoming};
use remote_task_runner::worker::machine::Machine;
use remote_task_runner::worker::serve::serve_connection;
use remote_task_runner::worker::{Agent, AgentConfig, Host};
use remote_task_runner::workspace::{CliFastWorktree, Workspace, Workspaces};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Semaphore;

use SourceBlobKind::{Bundle, Content, Manifest};

const T: Duration = Duration::from_secs(30);
const PATH: &str = "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin";

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args([
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
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim_end().to_string()
}

/// Bytes that do not compress into a pattern: `n` of them, from `seed`.
fn noise(seed: u64, n: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

struct Fx {
    dir: TempDir,
    cluster: ClusterId,
    agents: Mutex<Vec<Arc<Agent>>>,
    /// Where `fast-worktree` is, when the worker of the test needs it.
    tools: Option<PathBuf>,
    /// A directory that comes first on the worker's `PATH` (see
    /// [`Fx::git_shim`]).
    front: Option<PathBuf>,
}

/// The directory of the installed `fast-worktree` (a Git source is prepared
/// with it). Without one the test cannot run an attempt: that is a failure
/// where it is required (`RTR_REQUIRE_FAST_WORKTREE`, set in CI), and said
/// aloud elsewhere.
fn fast_worktree_dir() -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let found = std::env::split_paths(&path).find(|d| d.join("fast-worktree").is_file());
    if found.is_none() {
        assert!(
            std::env::var_os("RTR_REQUIRE_FAST_WORKTREE").is_none(),
            "fast-worktree is required here and is not installed"
        );
        eprintln!("fast-worktree is not installed: no attempt is run from the Git source");
    }
    found
}

fn fx() -> Fx {
    let dir = tempfile::Builder::new()
        .prefix("rtr-blobs-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    std::fs::create_dir_all(dir.path().join("home")).unwrap();
    Fx {
        dir,
        cluster: ClusterId::generate(),
        agents: Mutex::default(),
        tools: None,
        front: None,
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        for a in self.agents.lock().unwrap().iter() {
            a.shutdown();
        }
        // Whatever an attempt of this fixture still runs, by its recorded
        // identity (never by a number alone).
        if let Ok(store) = WorkerStore::open_read_only(&self.state()) {
            for rec in store.list_attempts(None, true, 500).unwrap_or_default() {
                for (_, _, ident) in store.launches(&rec.attempt_id).unwrap_or_default() {
                    if let Some(i) = ident {
                        crate::common::kill_recorded_group(i.pid, &i.start_identity);
                    }
                }
            }
        }
    }
}

impl Fx {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
    /// The worker's state directory.
    fn state(&self) -> PathBuf {
        self.path("state")
    }
    fn env(&self) -> BTreeMap<String, String> {
        let path = match &self.tools {
            Some(dir) => format!("{PATH}:{}", dir.display()),
            None => PATH.to_string(),
        };
        let path = match &self.front {
            Some(dir) => format!("{}:{path}", dir.display()),
            None => path,
        };
        [
            ("PATH", path.as_str()),
            ("HOME", self.path("home").to_str().unwrap()),
            ("GIT_CONFIG_GLOBAL", "/dev/null"),
            ("GIT_CONFIG_NOSYSTEM", "1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }
    /// Put a `git` in front of the real one for everything that runs with
    /// [`Self::env`] from now on. It is the real Git, except where
    /// [`Self::git_fails`] says otherwise.
    fn git_shim(&mut self) {
        let real = std::env::split_paths(PATH)
            .map(|d| d.join("git"))
            .find(|p| p.is_file())
            .expect("git is installed");
        let dir = self.path("shim");
        std::fs::create_dir_all(&dir).unwrap();
        // The repository a command is for: what follows `-C`, or where it
        // runs. The scratch repositories (`.clone.*`) are left alone.
        let text = format!(
            r#"#!/bin/sh
dir=""; prev=""; sub=""
for a in "$@"; do
  if [ "$prev" = "-C" ]; then dir="$a"; fi
  case "$a" in
    fetch|merge-base) if [ -z "$sub" ]; then sub="$a"; fi ;;
  esac
  prev="$a"
done
if [ -z "$dir" ]; then dir="$(pwd)"; fi
case "$dir" in
  */.clone.*) exec "{real}" "$@" ;;
esac
flag="{flags}/fail-$sub"
if [ -n "$sub" ] && [ -f "$flag" ]; then
  read -r pass code < "$flag"
  if [ "$pass" -le 0 ]; then
    echo "fatal: no $sub today" >&2
    exit "$code"
  fi
  echo "$((pass - 1)) $code" > "$flag"
fi
exec "{real}" "$@"
"#,
            real = real.display(),
            flags = dir.display(),
        );
        crate::common::write_executable(&dir.join("git"), &text);
        self.front = Some(dir);
    }
    /// From now on `git <subcommand>` in a clone of the worker (not in a
    /// scratch repository beside it) exits with `code`, after `pass` calls
    /// that go through. `None`: it works again.
    fn git_fails(&self, subcommand: &str, how: Option<(u32, i32)>) {
        let flag = self.path("shim").join(format!("fail-{subcommand}"));
        match how {
            Some((pass, code)) => std::fs::write(flag, format!("{pass} {code}\n")).unwrap(),
            None => {
                let _ = std::fs::remove_file(flag);
            }
        }
    }
    /// The store of the side that sends.
    fn parent(&self) -> SourceStore {
        SourceStore::open(&self.path("parent")).unwrap()
    }
    /// The worker's side, as a process that just started would open it.
    fn machine(&self) -> Machine {
        Machine::open(&self.state(), self.env(), self.state().join("cluster.key")).unwrap()
    }
    fn worker_store(&self) -> SourceStore {
        SourceStore::open(&self.state()).unwrap()
    }
    fn incoming(&self, source: &SourceId) -> PathBuf {
        self.state().join("incoming").join(source.as_str())
    }
    /// The staged files of a source: manifests and bundle, the files that
    /// are part-way (`parts/`) and the ones that wait whole (`files/`).
    fn staged(&self, source: &SourceId) -> BTreeSet<String> {
        let dir = self.incoming(source);
        let mut out = BTreeSet::new();
        for d in [dir.clone(), dir.join("parts"), dir.join("files")] {
            for e in std::fs::read_dir(d).into_iter().flatten() {
                let name = e.unwrap().file_name().into_string().unwrap();
                if !["parts", "files", "reserved"].contains(&name.as_str()) {
                    out.insert(name);
                }
            }
        }
        out
    }
    /// The bytes the worker lets the source take in all.
    fn reserved(&self, source: &SourceId) -> u64 {
        self.reservation(source).bytes
    }
    /// That, and the bytes of the disk it lets it take.
    fn reservation(&self, source: &SourceId) -> Reservation {
        let text = std::fs::read_to_string(self.incoming(source).join("reserved")).unwrap();
        parse_reservation(&text).unwrap()
    }
    /// The worker's clones (each a repository with no remote).
    fn clones(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.state().join("clones"))
            .into_iter()
            .flatten()
            .map(|e| e.unwrap().path())
            .filter(|p| p.join(".git").exists())
            .collect()
    }
    /// The bundle of a captured source for a worker that holds `have`.
    fn bundle(&self, root: &CiManifest, have: &[String]) -> Vec<u8> {
        let path = self
            .parent()
            .export_bundle(&ExecContext::new(self.env()), root, have)
            .unwrap();
        std::fs::read(path).unwrap()
    }

    /// A directory with a few files, one of them several chunks long.
    fn plain_dir(&self, name: &str, seed: u64) -> PathBuf {
        let dir = self.path(name);
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/x.txt"), format!("from the snapshot {seed}\n")).unwrap();
        std::fs::write(dir.join("big.bin"), noise(seed, 5000)).unwrap();
        std::fs::write(dir.join("empty"), b"").unwrap();
        dir
    }

    fn capture(&self, request: &SourceRequest) -> Parcel {
        let store = self.parent();
        let got = capture(&self.env(), &store, request, &CaptureParams::default()).unwrap();
        let root = store.manifest(&got.manifest.ci_source_id).unwrap().unwrap();
        let mut manifests = store.closure(&root).unwrap();
        manifests.reverse();
        let mut blobs = BTreeMap::new();
        for m in &manifests {
            for e in &m.object_manifest.entries {
                blobs.insert(e.hash.clone(), store.read_blob(&e.hash).unwrap());
            }
        }
        // for a worker that holds nothing: the whole history
        let bundle = root
            .execution_commit
            .as_ref()
            .map(|_| self.bundle(&root, &[]));
        Parcel {
            source: got.manifest.source_id.clone(),
            manifest_digest: got.manifest.manifest_digest.clone(),
            manifests: manifests
                .iter()
                .map(|m| serde_json::to_vec(m).unwrap())
                .collect(),
            root,
            blobs,
            bundle,
        }
    }

    fn directory(&self, name: &str, seed: u64) -> Parcel {
        let dir = self.plain_dir(name, seed);
        self.capture(&SourceRequest::Directory { dir })
    }

    fn cfg(&self) -> AgentConfig {
        let mut cfg = AgentConfig::new(
            self.state(),
            WorkerId::parse("w-test").unwrap(),
            WorkerBootId::parse("boot-a").unwrap(),
            PathBuf::from(env!("CARGO_BIN_EXE_remote-task-runner")),
        );
        cfg.poll_ms = 25;
        cfg.inhibit_sleep = false;
        cfg.retry_ms = 200;
        cfg.term_grace_ms = 2_000;
        cfg.kill_wait_ms = 2_000;
        cfg
    }
    fn start(&self) -> Arc<Agent> {
        let agent = Agent::start(self.cfg(), Arc::new(RealButCore(self.machine()))).unwrap();
        self.agents.lock().unwrap().push(agent.clone());
        agent
    }
    /// A new connection to the agent: its own stream and its own `hello`.
    async fn connect(&self, agent: &Arc<Agent>) -> Client {
        let (a, b) = duplex_pair(64 * 1024);
        let permits = Arc::new(Semaphore::new(64));
        tokio::spawn(serve_connection(
            agent.clone(),
            b,
            self.cluster.clone(),
            permits,
        ));
        connect(a, client_cfg(&self.cluster)).await.unwrap().client
    }
    fn wait_terminal(&self, a: &AttemptId) -> Outcome {
        let store = WorkerStore::open_read_only(&self.state()).unwrap();
        let start = Instant::now();
        loop {
            let rec = store.get(a).unwrap().unwrap();
            if rec.state == AttemptState::Terminal {
                return rec.termination.unwrap().outcome();
            }
            assert!(start.elapsed() < T, "the attempt did not end");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The real host, except for the process-wide core dump limit (this test
/// shares its process with the others).
struct RealButCore(Machine);

impl Host for RealButCore {
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        Ok(())
    }
    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        self.0.sweep_store_copies()
    }
    fn sweep_vault(&self) -> Result<(), ApiError> {
        self.0.sweep_vault()
    }
    fn reconcile_secrets(&self, e: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.reconcile_secrets(e)
    }
    fn sweep_workspaces(&self, h: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        self.0.sweep_workspaces(h)
    }
    fn prepare(&self, a: &AcceptedAttempt, p: &AttemptPayload) -> Result<Workspace, ApiError> {
        self.0.prepare(a, p)
    }
    fn secret_paths(&self, p: &AttemptPayload) -> Result<Option<Vec<String>>, ApiError> {
        self.0.secret_paths(p)
    }
    fn place_secrets(
        &self,
        a: &AcceptedAttempt,
        ws: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        self.0.place_secrets(a, ws, files)
    }
    fn secrets_ready(&self, a: &AcceptedAttempt) -> Result<bool, ApiError> {
        self.0.secrets_ready(a)
    }
    fn placed_secrets(
        &self,
        a: &AcceptedAttempt,
    ) -> Result<remote_task_runner::secrets::place::Placed, ApiError> {
        self.0.placed_secrets(a)
    }
    fn cleanup(&self, a: &AttemptId, ws: Option<&Workspace>) -> Result<(), ApiError> {
        self.0.cleanup(a, ws)
    }
    fn blobs(&self) -> Option<&dyn BlobSink> {
        self.0.blobs()
    }
}

/// Everything of one source, as the sending side holds it.
struct Parcel {
    source: SourceId,
    manifest_digest: String,
    root: CiManifest,
    /// The JSON of the manifests, the source's own first.
    manifests: Vec<Vec<u8>>,
    /// The files, by hash.
    blobs: BTreeMap<String, Vec<u8>>,
    bundle: Option<Vec<u8>>,
}

impl Parcel {
    /// A file of the source that is `n` bytes long.
    fn blob_of_len(&self, n: usize) -> (&str, &[u8]) {
        let (h, b) = self.blobs.iter().find(|(_, b)| b.len() == n).unwrap();
        (h, b)
    }
    fn refs(&self) -> Vec<BlobRef> {
        let r = |kind, bytes: &[u8]| BlobRef {
            kind,
            digest: Digest::of_bytes(bytes),
            total_size: bytes.len() as u64,
        };
        let mut out: Vec<BlobRef> = self.manifests.iter().map(|m| r(Manifest, m)).collect();
        out.extend(self.blobs.values().map(|b| r(Content, b)));
        out.extend(self.bundle.iter().map(|b| r(Bundle, b)));
        out
    }
}

fn limits() -> BlobLimits {
    BlobLimits {
        lock_wait_ms: 5_000,
        ..BlobLimits::default()
    }
}

/// One `blob_put` of `data` as the bytes at `offset` of a blob of `total`
/// bytes with `digest`.
#[allow(clippy::too_many_arguments)]
fn put_raw(
    sink: &dyn BlobSink,
    l: &BlobLimits,
    source: &SourceId,
    kind: SourceBlobKind,
    digest: &Digest,
    total: u64,
    offset: u64,
    data: &[u8],
) -> Result<BlobPutResponse, ApiError> {
    sink.put(
        l,
        &BlobPutRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            source_id: source.clone(),
            kind,
            digest: digest.clone(),
            total_size: total,
            offset,
            data: Bytes(data.to_vec()),
        },
    )
}

/// One chunk of `whole`: the bytes `from..to`.
fn put_part(
    sink: &dyn BlobSink,
    l: &BlobLimits,
    source: &SourceId,
    kind: SourceBlobKind,
    whole: &[u8],
    from: usize,
    to: usize,
) -> Result<BlobPutResponse, ApiError> {
    let digest = Digest::of_bytes(whole);
    let total = whole.len() as u64;
    put_raw(
        sink,
        l,
        source,
        kind,
        &digest,
        total,
        from as u64,
        &whole[from..to],
    )
}

/// The whole blob in one chunk.
fn put_all(
    sink: &dyn BlobSink,
    l: &BlobLimits,
    source: &SourceId,
    kind: SourceBlobKind,
    whole: &[u8],
) -> Result<BlobPutResponse, ApiError> {
    put_part(sink, l, source, kind, whole, 0, whole.len())
}

fn verify(
    sink: &dyn BlobSink,
    l: &BlobLimits,
    source: &SourceId,
    blobs: Vec<BlobRef>,
) -> BlobVerifyResponse {
    sink.verify(
        l,
        &BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: source.clone(),
            blobs,
        },
    )
    .unwrap()
}

/// Send everything of the parcel, manifests first.
fn send_all(sink: &dyn BlobSink, l: &BlobLimits, p: &Parcel) {
    for m in &p.manifests {
        assert!(put_all(sink, l, &p.source, Manifest, m).unwrap().complete);
    }
    for b in p.blobs.values() {
        assert!(put_all(sink, l, &p.source, Content, b).unwrap().complete);
    }
    if let Some(b) = &p.bundle {
        assert!(put_all(sink, l, &p.source, Bundle, b).unwrap().complete);
    }
}

async fn call(c: &Client, r: Request) -> Result<Response, ApiError> {
    match c.call(r, T).await {
        Ok(r) => Ok(r),
        Err(CallError::Remote(e)) => Err(e),
        Err(e) => panic!("{e:?}"),
    }
}

async fn verify_over(c: &Client, p: &Parcel) -> BlobVerifyResponse {
    let r = Request::BlobVerify(BlobVerifyRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        source_id: p.source.clone(),
        blobs: p.refs(),
    });
    match call(c, r).await.unwrap() {
        Response::BlobVerify(v) => v,
        other => panic!("{other:?}"),
    }
}

fn payload(p: &Parcel, script: &str) -> AttemptPayload {
    let digest = &p.manifest_digest;
    let mut env = BTreeMap::new();
    env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
    AttemptPayload {
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: p.source.clone(),
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(digest)
            .or_else(|_| Digest::parse(&format!("sha256:{digest}")))
            .unwrap(),
        permit_id: None,
        commands: vec![CommandSpec {
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            cwd: String::new(),
            env,
            depends_on: vec![],
            timeout_ms: None,
        }],
        lease_ms: 60_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: Vec::new(),
    }
}

// ------------------------------------------------------------ end to end

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_sent_over_two_connections_at_once_runs_an_attempt() {
    let f = fx();
    let p = Arc::new(f.directory("plain", 1));
    let agent = f.start();
    let c1 = f.connect(&agent).await;
    let c2 = f.connect(&agent).await;

    // Nothing is there yet, and nothing is taken before the manifest.
    let v = verify_over(&c1, &p).await;
    assert!(!v.ready && v.present.is_empty() && v.corrupt.is_empty());
    assert!(v.missing.iter().all(|m| m.received == 0));
    assert_eq!(v.missing.len(), p.refs().len());
    let (_, big) = p.blob_of_len(5000);
    let e = put_blob_as(&c1, CoordinatorEpoch(1), &p.source, Content, big, 0, 700, T)
        .await
        .unwrap_err();
    assert!(format!("{e:?}").contains("Conflict"), "{e:?}");
    assert!(!f.incoming(&p.source).exists());

    // Both connections send the whole source, in small chunks, at once.
    let send = |c: Client, chunk: usize| {
        let p = p.clone();
        tokio::spawn(async move {
            for m in &p.manifests {
                let r = put_blob_as(&c, CoordinatorEpoch(1), &p.source, Manifest, m, 0, chunk, T)
                    .await
                    .unwrap();
                assert!(r.complete);
            }
            for b in p.blobs.values() {
                let r = put_blob_as(&c, CoordinatorEpoch(1), &p.source, Content, b, 0, chunk, T)
                    .await
                    .unwrap();
                assert!(r.complete && r.received == b.len() as u64);
            }
            c
        })
    };
    let (a, b) = (send(c1, 300), send(c2, 170));
    let (c1, _c2) = (a.await.unwrap(), b.await.unwrap());

    let v = verify_over(&c1, &p).await;
    assert!(v.ready && v.missing.is_empty() && v.corrupt.is_empty());
    assert_eq!(v.present.len(), p.refs().len());
    // The staging of the source is gone, the store has what was sent.
    assert!(!f.incoming(&p.source).exists());
    let store = f.worker_store();
    for (hash, bytes) in &p.blobs {
        assert_eq!(&store.read_blob(hash).unwrap(), bytes);
    }

    // The attempt runs in the snapshot.
    let out = f.path("out");
    let a = AttemptId::generate();
    let submit = Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            payload(
                &p,
                &format!("cat sub/x.txt > {0}; wc -c < big.bin >> {0}", out.display()),
            ),
        )
        .unwrap(),
    );
    call(&c1, submit).await.unwrap();
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(text.starts_with("from the snapshot 1\n"), "{text}");
    assert_eq!(text.lines().nth(1).unwrap().trim(), "5000");

    // A blob of a source that is whole is answered without storing anything.
    let r = put_blob_as(&c1, CoordinatorEpoch(1), &p.source, Content, big, 0, 700, T)
        .await
        .unwrap();
    assert!(r.complete);
    let e = put_blob_as(
        &c1,
        CoordinatorEpoch(1),
        &p.source,
        Content,
        b"not of it",
        0,
        700,
        T,
    )
    .await
    .unwrap_err();
    assert!(format!("{e:?}").contains("InvalidInput"), "{e:?}");

    // blob_get serves artifacts of attempts only: this one has none, and an
    // attempt nobody knows is not found.
    for attempt in [a, AttemptId::generate()] {
        let get = Request::BlobGet(BlobGetRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: attempt,
            digest: Digest::of_bytes(big),
            offset: 0,
            max_bytes: 100,
        });
        assert_eq!(call(&c1, get).await.unwrap_err().code, ErrorCode::NotFound);
    }

    // A coordinator of an older epoch sends nothing.
    let c3 = f.connect(&agent).await;
    let late = f.directory("late", 2);
    let later = Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(5),
            AttemptId::generate(),
            payload(&late, "true"),
        )
        .unwrap(),
    );
    call(&c3, later).await.unwrap();
    let e = put_blob_as(
        &c3,
        CoordinatorEpoch(4),
        &late.source,
        Manifest,
        &late.manifests[0],
        0,
        700,
        T,
    )
    .await
    .unwrap_err();
    assert!(format!("{e:?}").contains("StaleEpoch"), "{e:?}");
    assert!(!f.incoming(&late.source).exists());
}

#[test]
fn a_git_source_needs_its_bundle_and_a_wrong_one_is_dropped() {
    let f = fx();
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    std::fs::write(repo.join("new.txt"), "untracked\n").unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let commit = p.root.execution_commit.clone().unwrap();
    let bundle = p.bundle.as_ref().unwrap();
    let bundle_ref = |b: &[u8]| BlobRef {
        kind: Bundle,
        digest: Digest::of_bytes(b),
        total_size: b.len() as u64,
    };

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    // A bundle before its manifest is refused.
    let e = put_all(sink, &l, &p.source, Bundle, bundle).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    for man in &p.manifests {
        put_all(sink, &l, &p.source, Manifest, man).unwrap();
    }
    for b in p.blobs.values() {
        put_all(sink, &l, &p.source, Content, b).unwrap();
    }
    // Everything but the commits: not ready, and the bundle is what is missing.
    let v = verify(sink, &l, &p.source, p.refs());
    assert!(!v.ready);
    assert_eq!(
        v.missing,
        vec![BlobProgress {
            digest: Digest::of_bytes(bundle),
            received: 0
        }]
    );

    // Bytes that are no bundle of this source: refused when they are whole,
    // and not kept.
    let junk = noise(7, 3000);
    put_part(sink, &l, &p.source, Bundle, &junk, 0, 1000).unwrap();
    let e = put_part(sink, &l, &p.source, Bundle, &junk, 1000, 3000).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("bundle"), "{}", e.message);
    assert!(f
        .staged(&p.source)
        .iter()
        .all(|n| !n.starts_with("bundle-")));
    let v = verify(
        sink,
        &l,
        &p.source,
        vec![bundle_ref(&junk), bundle_ref(bundle)],
    );
    assert!(!v.ready && v.present.is_empty());
    assert!(f
        .worker_store()
        .manifest(&p.root.source_id)
        .unwrap()
        .is_none());

    // The real one, in two parts over a restart of the worker.
    let half = bundle.len() / 2;
    let r = put_part(sink, &l, &p.source, Bundle, bundle, 0, half).unwrap();
    assert_eq!((r.received, r.complete), (half as u64, false));
    drop(m);
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let v = verify(sink, &l, &p.source, vec![bundle_ref(bundle)]);
    assert_eq!(v.missing[0].received, half as u64);
    let r = put_part(sink, &l, &p.source, Bundle, bundle, half, bundle.len()).unwrap();
    assert!(r.complete);

    let v = verify(sink, &l, &p.source, p.refs());
    assert!(v.ready && v.missing.is_empty());
    let published = f
        .worker_store()
        .manifest(&p.root.source_id)
        .unwrap()
        .unwrap();
    assert_eq!(published, p.root);
    // The commit is in the clone attempts of this repository are made from.
    let clones: Vec<PathBuf> = std::fs::read_dir(f.state().join("clones"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.join(".git").exists())
        .collect();
    assert_eq!(clones.len(), 1);
    assert_eq!(
        git(&clones[0], &["cat-file", "-t", &commit]),
        "commit".to_string()
    );
    assert!(!f.incoming(&p.source).exists());
}

#[test]
fn a_submodule_capture_travels_with_its_parent_and_only_when_it_is_named() {
    let f = fx();
    let lib = f.path("lib");
    std::fs::create_dir_all(&lib).unwrap();
    git(&lib, &["init", "-q", "-b", "main"]);
    std::fs::write(lib.join("lib.txt"), "lib\n").unwrap();
    git(&lib, &["add", "."]);
    git(&lib, &["commit", "-qm", "lib"]);
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "a\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    git(
        &repo,
        &[
            "submodule",
            "add",
            "-q",
            lib.to_str().unwrap(),
            "vendor/lib",
        ],
    );
    git(&repo, &["commit", "-qm", "add submodule"]);
    std::fs::write(repo.join("vendor/lib/lib.txt"), "edited lib\n").unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_eq!(p.manifests.len(), 2);
    let other = f.directory("plain", 3);

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    // The first manifest of a source is its own.
    let e = put_all(sink, &l, &p.source, Manifest, &p.manifests[1]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("first manifest"), "{}", e.message);
    assert!(!f.incoming(&p.source).exists());
    put_all(sink, &l, &p.source, Manifest, &p.manifests[0]).unwrap();
    // A manifest the source does not name is not taken for it.
    let e = put_all(sink, &l, &p.source, Manifest, &other.manifests[0]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("names this manifest"), "{}", e.message);

    // The files the source's own manifest names, and the bundle.
    let own: BTreeSet<&str> = p
        .root
        .object_manifest
        .entries
        .iter()
        .map(|e| e.hash.as_str())
        .collect();
    for (_, b) in p.blobs.iter().filter(|(h, _)| own.contains(h.as_str())) {
        put_all(sink, &l, &p.source, Content, b).unwrap();
    }
    put_all(sink, &l, &p.source, Bundle, p.bundle.as_ref().unwrap()).unwrap();
    // Everything but the submodule's capture: not published.
    assert!(!verify(sink, &l, &p.source, vec![]).ready);
    assert!(f
        .worker_store()
        .manifest(&p.root.source_id)
        .unwrap()
        .is_none());
    put_all(sink, &l, &p.source, Manifest, &p.manifests[1]).unwrap();
    for b in p.blobs.values() {
        put_all(sink, &l, &p.source, Content, b).unwrap();
    }
    assert!(verify(sink, &l, &p.source, p.refs()).ready);

    let store = f.worker_store();
    let root = store.manifest(&p.root.source_id).unwrap().unwrap();
    let dest = f.path("unpacked");
    store.materialize(&root, &dest).unwrap();
    assert_eq!(
        std::fs::read(dest.join("vendor/lib/lib.txt")).unwrap(),
        b"edited lib\n"
    );
}

// ------------------------------------------------------- resume and races

#[test]
fn a_transfer_that_was_cut_resumes_from_what_is_held() {
    let f = fx();
    let p = f.directory("plain", 4);
    let (hash, big) = p.blob_of_len(5000);
    let big_ref = || {
        vec![BlobRef {
            kind: Content,
            digest: Digest::of_bytes(big),
            total_size: 5000,
        }]
    };
    let l = limits();
    {
        let m = f.machine();
        let sink = m.blobs().unwrap();
        // The manifest itself is cut after its first bytes.
        let man = &p.manifests[0];
        let r = put_part(sink, &l, &p.source, Manifest, man, 0, 10).unwrap();
        assert_eq!((r.received, r.complete), (10, false));
        let r = put_part(sink, &l, &p.source, Manifest, man, 10, man.len()).unwrap();
        assert!(r.complete);
        let r = put_part(sink, &l, &p.source, Content, big, 0, 1200).unwrap();
        assert_eq!((r.received, r.complete), (1200, false));
    }
    // The connection is gone and the worker restarted: another process.
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let v = verify(sink, &l, &p.source, big_ref());
    assert_eq!(v.missing[0].received, 1200);
    // A chunk that leaves a gap changes nothing and says where to go on.
    let r = put_part(sink, &l, &p.source, Content, big, 3000, 4000).unwrap();
    assert_eq!((r.received, r.complete), (1200, false));
    // A chunk that overlaps adds only what is new; a repeat adds nothing.
    let r = put_part(sink, &l, &p.source, Content, big, 1000, 2500).unwrap();
    assert_eq!((r.received, r.complete), (2500, false));
    let r = put_part(sink, &l, &p.source, Content, big, 0, 1200).unwrap();
    assert_eq!((r.received, r.complete), (2500, false));
    let part = f
        .incoming(&p.source)
        .join("parts")
        .join(staged_name(Content, hash, 5000, false));
    assert_eq!(std::fs::read(&part).unwrap(), big[..2500]);
    let r = put_part(sink, &l, &p.source, Content, big, 2500, 5000).unwrap();
    assert_eq!((r.received, r.complete), (5000, true));
    assert!(!part.exists());
    // Whole, it waits in the staging area: the store takes the files of a
    // source when the source is published, not before.
    let whole = f
        .incoming(&p.source)
        .join("files")
        .join(staged_name(Content, hash, 5000, true));
    assert_eq!(std::fs::read(&whole).unwrap(), big);
    assert!(!f.worker_store().has_blob(hash));
    assert_eq!(verify(sink, &l, &p.source, big_ref()).present.len(), 1);
    // The same blob again is whole already.
    let r = put_part(sink, &l, &p.source, Content, big, 0, 100).unwrap();
    assert!(r.complete);
}

#[test]
fn two_workers_processes_on_one_state_take_the_same_blob_once() {
    let f = fx();
    let p = Arc::new(f.directory("plain", 5));
    let l = limits();
    // Two openings of the same state directory: nothing in memory is shared,
    // as between two processes.
    let machines = [Arc::new(f.machine()), Arc::new(f.machine())];
    put_all(
        machines[0].blobs().unwrap(),
        &l,
        &p.source,
        Manifest,
        &p.manifests[0],
    )
    .unwrap();
    std::thread::scope(|s| {
        for (i, m) in machines.iter().enumerate() {
            let (p, l) = (p.clone(), l.clone());
            s.spawn(move || {
                let sink = m.blobs().unwrap();
                let step = 97 + 40 * i;
                for b in p.blobs.values() {
                    // Each sender follows what the worker says it holds.
                    let mut at = 0;
                    loop {
                        let to = (at + step).min(b.len());
                        let r = put_part(sink, &l, &p.source, Content, b, at, to).unwrap();
                        if r.complete {
                            break;
                        }
                        assert!(r.received as usize <= b.len());
                        at = r.received as usize;
                    }
                }
            });
        }
    });
    let v = verify(machines[1].blobs().unwrap(), &l, &p.source, p.refs());
    assert!(v.ready && v.missing.is_empty() && v.corrupt.is_empty());
    let store = f.worker_store();
    for (hash, bytes) in &p.blobs {
        assert_eq!(&store.read_blob(hash).unwrap(), bytes);
    }
    assert!(!f.incoming(&p.source).exists());
}

#[test]
fn a_source_that_is_busy_answers_overloaded_and_takes_nothing() {
    let f = fx();
    let p = f.directory("plain", 6);
    let m = f.machine();
    let sink = m.blobs().unwrap();
    put_all(sink, &limits(), &p.source, Manifest, &p.manifests[0]).unwrap();
    // Another process holds the source's lock.
    let lock = std::fs::File::options()
        .write(true)
        .open(
            f.state()
                .join("incoming/locks")
                .join(format!("{}.lock", lock_shard(p.source.as_str()))),
        )
        .unwrap();
    lock.lock().unwrap();
    let l = BlobLimits {
        lock_wait_ms: 100,
        ..limits()
    };
    let (_, big) = p.blob_of_len(5000);
    let before = f.staged(&p.source);
    let e = put_all(sink, &l, &p.source, Content, big).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    let v = sink.verify(
        &l,
        &BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: p.source.clone(),
            blobs: vec![],
        },
    );
    assert_eq!(v.unwrap_err().code, ErrorCode::Overloaded);
    assert_eq!(f.staged(&p.source), before);
    drop(lock);
    assert!(put_all(sink, &l, &p.source, Content, big).unwrap().complete);
}

// ----------------------------------------------------- what is not taken

#[test]
fn bytes_that_do_not_have_the_digest_are_dropped_at_the_last_chunk() {
    let f = fx();
    let p = f.directory("plain", 8);
    let (hash, big) = p.blob_of_len(5000);
    let digest = Digest::of_bytes(big);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());

    // A manifest whose bytes are not the announced ones.
    let man = &p.manifests[0];
    let lie = Digest::of_bytes(b"another manifest");
    let e = put_raw(
        sink,
        &l,
        &p.source,
        Manifest,
        &lie,
        man.len() as u64,
        0,
        man,
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("digest"), "{}", e.message);
    assert!(!f.incoming(&p.source).exists());
    // Bytes with the right digest that are no manifest, and a manifest that
    // was changed after it was sealed.
    let e = put_all(sink, &l, &p.source, Manifest, b"{\"not\": \"a manifest\"}").unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let mut forged = p.root.clone();
    forged.object_manifest.entries[0].size += 1;
    let e = put_all(
        sink,
        &l,
        &p.source,
        Manifest,
        &serde_json::to_vec(&forged).unwrap(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(!f.incoming(&p.source).exists());
    put_all(sink, &l, &p.source, Manifest, man).unwrap();

    // A file: right size, right name, other bytes. Refused when the last
    // byte is there; nothing of it stays, and nothing reaches the store.
    let wrong = noise(99, 5000);
    let r = put_raw(
        sink,
        &l,
        &p.source,
        Content,
        &digest,
        5000,
        0,
        &wrong[..4000],
    )
    .unwrap();
    assert_eq!((r.received, r.complete), (4000, false));
    let e = put_raw(
        sink,
        &l,
        &p.source,
        Content,
        &digest,
        5000,
        4000,
        &wrong[4000..],
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("digest"), "{}", e.message);
    assert!(f
        .staged(&p.source)
        .iter()
        .all(|n| !n.starts_with("content-")));
    assert!(!f.worker_store().has_blob(hash));
    let v = verify(
        sink,
        &l,
        &p.source,
        vec![BlobRef {
            kind: Content,
            digest: digest.clone(),
            total_size: 5000,
        }],
    );
    assert_eq!(v.missing[0].received, 0);
    assert!(!v.ready);
    // The sender starts over with the real bytes.
    assert!(put_all(sink, &l, &p.source, Content, big).unwrap().complete);

    // What verify calls corrupt: held, but not with the size that is asked.
    let v = verify(
        sink,
        &l,
        &p.source,
        vec![BlobRef {
            kind: Content,
            digest,
            total_size: 4999,
        }],
    );
    assert_eq!(v.corrupt.len(), 1);
}

#[test]
fn sizes_counts_and_names_are_capped_before_anything_is_written() {
    let f = fx();
    let p = f.directory("plain", 9);
    let (_, big) = p.blob_of_len(5000);
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let l = limits();
    let nothing = |f: &Fx| !f.incoming(&p.source).exists();

    // A blob larger than the worker accepts, whatever is sent of it.
    let small = BlobLimits {
        max_blob_bytes: 4999,
        max_manifest_bytes: 16,
        ..limits()
    };
    let man = &p.manifests[0];
    let e = put_part(sink, &small, &p.source, Manifest, man, 0, 10).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(nothing(&f));
    // A chunk past the announced size, and a chunk over 1 MiB.
    let d = Digest::of_bytes(man);
    let e = put_raw(sink, &l, &p.source, Manifest, &d, 10, 5, &man[..10]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let huge = vec![0u8; (1 << 20) + 1];
    let e = put_raw(sink, &l, &p.source, Manifest, &d, 1 << 21, 0, &huge).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(nothing(&f));
    // A manifest that names a file larger than a blob may be, or more bytes
    // than a source may have: refused whole, nothing staged.
    for tight in [
        BlobLimits {
            max_blob_bytes: 4999,
            ..limits()
        },
        BlobLimits {
            max_source_bytes: 5000,
            ..limits()
        },
    ] {
        let e = put_all(sink, &tight, &p.source, Manifest, man).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("larger"), "{}", e.message);
        assert!(nothing(&f));
    }

    put_all(sink, &l, &p.source, Manifest, man).unwrap();
    let staged = f.staged(&p.source);
    // A file the manifest does not name, and one with another size.
    let e = put_all(sink, &l, &p.source, Content, b"not of this source").unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    let e = put_raw(
        sink,
        &l,
        &p.source,
        Content,
        &Digest::of_bytes(big),
        5001,
        0,
        big,
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("another size"), "{}", e.message);
    // A file over the cap that a manifest staged under a wider cap names.
    let e = put_all(sink, &small, &p.source, Content, big).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // A bundle for a source without Git objects.
    let e = put_all(sink, &l, &p.source, Bundle, b"bundle").unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert_eq!(f.staged(&p.source), staged);

    // No more blobs part-way than the limit.
    let one = BlobLimits {
        max_partials: 1,
        ..limits()
    };
    put_part(sink, &one, &p.source, Content, big, 0, 10).unwrap();
    let (_, text) = p
        .blobs
        .iter()
        .find(|(_, b)| b.starts_with(b"from"))
        .unwrap();
    let e = put_part(sink, &one, &p.source, Content, text, 0, 1).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    put_part(sink, &one, &p.source, Content, big, 10, 20).unwrap();

    // Too many entries in one blob_verify.
    let many = vec![
        BlobRef {
            kind: Content,
            digest: Digest::of_bytes(b"x"),
            total_size: 1,
        };
        4097
    ];
    let e = sink
        .verify(
            &l,
            &BlobVerifyRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                source_id: p.source.clone(),
                blobs: many,
            },
        )
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
}

#[test]
fn only_so_many_sources_are_part_way_and_an_abandoned_one_makes_room() {
    let f = fx();
    let parcels: Vec<Parcel> = (0..3)
        .map(|i| f.directory(&format!("d{i}"), 20 + i))
        .collect();
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let two = BlobLimits {
        max_staged_sources: 2,
        ..limits()
    };
    for p in &parcels[..2] {
        put_all(sink, &two, &p.source, Manifest, &p.manifests[0]).unwrap();
    }
    // A file of the second one arrived whole.
    let (hash, big) = parcels[1].blob_of_len(5000);
    assert!(
        put_all(sink, &two, &parcels[1].source, Content, big)
            .unwrap()
            .complete
    );
    let third = &parcels[2];
    let e = put_all(sink, &two, &third.source, Manifest, &third.manifests[0]).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(!f.incoming(&third.source).exists());
    // A file in the staging area that is not ours is never touched.
    let foreign = f.incoming(&parcels[0].source).join("notes.txt");
    std::fs::write(&foreign, "someone else's").unwrap();

    // Nobody added to the two for longer than the limit: they go.
    std::thread::sleep(Duration::from_millis(30));
    let aged = BlobLimits {
        stale_ms: 10,
        ..two.clone()
    };
    put_all(sink, &aged, &third.source, Manifest, &third.manifests[0]).unwrap();
    assert!(f.incoming(&third.source).exists());
    assert!(!f.incoming(&parcels[1].source).exists());
    // Nothing of a source that was dropped is in the store: its files
    // waited in the staging area.
    assert!(!f.worker_store().has_blob(hash));
    assert_eq!(
        f.staged(&parcels[0].source),
        BTreeSet::from(["notes.txt".to_string()])
    );
    assert_eq!(std::fs::read_to_string(&foreign).unwrap(), "someone else's");
    // The dropped source starts over like a new one.
    let v = verify(sink, &two, &parcels[1].source, parcels[1].refs());
    assert!(v.present.is_empty() && !v.ready);
}

// ---------------------------------------------------------------- blob_get

#[test]
fn blob_get_serves_the_artifacts_of_the_attempt_in_chunks_and_nothing_else() {
    let f = fx();
    let state = f.state();
    let root = state.join("attempts");
    std::fs::create_dir_all(&root).unwrap();
    let store = WorkerStore::open(&state, &WorkerId::parse("w-test").unwrap()).unwrap();
    let source = f.directory("plain", 30);
    let accept = |script: &str| {
        let a = AttemptId::generate();
        let p = payload(&source, script);
        let req = SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            p.clone(),
        )
        .unwrap();
        store
            .submit(
                NewAcceptance {
                    attempt_id: a.clone(),
                    run_id: p.run_id.clone(),
                    task_id: p.task_id.clone(),
                    generation: p.generation,
                    payload_hash: req.payload_hash,
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::to_value(&p).unwrap(),
                },
                1,
            )
            .unwrap();
        a
    };
    let (a, other) = (accept("true"), accept("false"));
    let ws = f.path("ws");
    std::fs::create_dir_all(ws.join("out")).unwrap();
    let data = noise(31, 700_000);
    std::fs::write(ws.join("out/report.bin"), &data).unwrap();
    std::fs::write(ws.join("empty"), b"").unwrap();
    let record = |attempt: &AttemptId, name: &str, file: &str| {
        store_artifact(
            &root,
            attempt,
            &ws,
            &ArtifactInput {
                name,
                source: Path::new(file),
                source_id: source.source.clone(),
                plan_digest: Digest::of_bytes(b"plan"),
                profile_fingerprint: Digest::of_bytes(b""),
                forbidden: &Forbidden::nothing(),
            },
            1 << 30,
        )
        .unwrap()
    };
    let artifacts = vec![
        record(&a, "out/report.bin", "out/report.bin"),
        record(&a, "empty", "empty"),
    ];
    let get = |attempt: &AttemptId, digest: &Digest, offset: u64, max_bytes: u32| {
        blobs::get(
            &store,
            &root,
            &BlobGetRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: attempt.clone(),
                digest: digest.clone(),
                offset,
                max_bytes,
            },
        )
    };
    let digest = Digest::of_bytes(&data);
    // Before the attempt has a result nothing of it is served.
    assert_eq!(
        get(&a, &digest, 0, 100).unwrap_err().code,
        ErrorCode::NotFound
    );
    let done = Termination::new(Outcome::Failed, None, None).unwrap();
    for (attempt, list) in [(&a, json!(artifacts)), (&other, json!([]))] {
        store
            .transition(attempt, AttemptState::Finalizing, 2)
            .unwrap();
        store.finalize(attempt, done, 2).unwrap();
        store
            .record_result(attempt, &json!({ "termination": done, "artifacts": list }))
            .unwrap();
    }

    // In chunks, never more than the chunk limit, to the end.
    let mut got = vec![];
    loop {
        let r = get(&a, &digest, got.len() as u64, u32::MAX).unwrap();
        assert_eq!((r.offset, r.total_size), (got.len() as u64, 700_000));
        assert!(r.data.0.len() <= 262_144);
        got.extend_from_slice(&r.data.0);
        if r.eof {
            break;
        }
        // Full chunks until the end: three answers for these 700 000 bytes.
        assert_eq!(r.data.0.len(), 262_144);
    }
    assert_eq!(got, data);
    let r = get(&a, &digest, 699_990, 4).unwrap();
    assert_eq!(
        (r.data.0.as_slice(), r.eof),
        (&data[699_990..699_994], false)
    );
    let r = get(&a, &digest, 700_000, 4).unwrap();
    assert!(r.data.0.is_empty() && r.eof);
    assert_eq!(
        get(&a, &digest, 700_001, 4).unwrap_err().code,
        ErrorCode::InvalidInput
    );
    let r = get(&a, &Digest::of_bytes(b""), 0, 4).unwrap();
    assert!(r.data.0.is_empty() && r.eof && r.total_size == 0);

    // Not by another attempt, not a digest the result does not list (a file
    // of the source store, say), not an attempt nobody knows.
    assert_eq!(
        get(&other, &digest, 0, 4).unwrap_err().code,
        ErrorCode::NotFound
    );
    let (_, big) = source.blob_of_len(5000);
    assert_eq!(
        get(&a, &Digest::of_bytes(big), 0, 4).unwrap_err().code,
        ErrorCode::NotFound
    );
    let nobody = AttemptId::generate();
    assert_eq!(
        get(&nobody, &digest, 0, 4).unwrap_err().code,
        ErrorCode::NotFound
    );

    // A file that is not as recorded is not served: changed in size, swapped
    // for a link, or gone.
    let file = root.join(a.as_str()).join("artifacts/out/report.bin");
    std::fs::write(&file, &data[..10]).unwrap();
    assert_eq!(
        get(&a, &digest, 0, 4).unwrap_err().code,
        ErrorCode::Internal
    );
    std::fs::remove_file(&file).unwrap();
    let outside = f.path("outside.bin");
    std::fs::write(&outside, &data).unwrap();
    std::os::unix::fs::symlink(&outside, &file).unwrap();
    assert_eq!(
        get(&a, &digest, 0, 4).unwrap_err().code,
        ErrorCode::Internal
    );
    std::fs::remove_file(&file).unwrap();
    assert_eq!(
        get(&a, &digest, 0, 4).unwrap_err().code,
        ErrorCode::NotFound
    );
    let dir = file.parent().unwrap();
    std::fs::remove_dir(dir).unwrap();
    std::os::unix::fs::symlink(f.path("ws/out"), dir).unwrap();
    assert_eq!(
        get(&a, &digest, 0, 4).unwrap_err().code,
        ErrorCode::Internal
    );
}

#[test]
fn a_source_whose_last_step_was_lost_is_published_by_sending_anything_again() {
    let f = fx();
    let p = f.directory("plain", 40);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    put_all(sink, &l, &p.source, Manifest, &p.manifests[0]).unwrap();
    // The files got into the store, and the worker died before it published
    // (here: they are put there behind its back).
    let store = f.worker_store();
    for (hash, bytes) in &p.blobs {
        let want = remote_task_runner::source::diff::BlobWant {
            hash: hash.clone(),
            size: bytes.len() as u64,
        };
        store.ingest_blob(&want, &mut &bytes[..]).unwrap();
    }
    drop(m);
    let m = f.machine();
    let sink = m.blobs().unwrap();
    // blob_verify changes nothing: everything is held, the source is not ready.
    for _ in 0..2 {
        let v = verify(sink, &l, &p.source, p.refs());
        assert!(!v.ready && v.missing.is_empty() && v.corrupt.is_empty());
    }
    assert!(store.manifest(&p.root.source_id).unwrap().is_none());
    // Any blob of it sent again completes the source.
    assert!(
        put_all(sink, &l, &p.source, Manifest, &p.manifests[0])
            .unwrap()
            .complete
    );
    assert!(verify(sink, &l, &p.source, p.refs()).ready);
    assert_eq!(store.manifest(&p.root.source_id).unwrap().unwrap(), p.root);
    // And a whole source sent once more is answered as held, storing nothing.
    send_all(sink, &l, &p);
    assert!(!f.incoming(&p.source).exists());
}

// ------------------------------------- Git sources, with no origin to ask

/// A repository cloned from an upstream (`up`), with a submodule (upstream:
/// `lib`): one commit that was never pushed, an edit that was never
/// committed, and an edit in the submodule.
fn project(f: &Fx) -> PathBuf {
    let lib = f.path("lib");
    std::fs::create_dir_all(&lib).unwrap();
    git(&lib, &["init", "-q", "-b", "main"]);
    std::fs::write(lib.join("lib.txt"), "lib\n").unwrap();
    git(&lib, &["add", "."]);
    git(&lib, &["commit", "-qm", "lib"]);
    let up = f.path("up");
    std::fs::create_dir_all(&up).unwrap();
    git(&up, &["init", "-q", "-b", "main"]);
    std::fs::write(up.join("a.txt"), "a\n").unwrap();
    std::fs::write(up.join("history.bin"), noise(77, 40_000)).unwrap();
    git(&up, &["add", "."]);
    git(&up, &["commit", "-qm", "base"]);
    git(
        &up,
        &[
            "submodule",
            "add",
            "-q",
            lib.to_str().unwrap(),
            "vendor/lib",
        ],
    );
    git(&up, &["commit", "-qm", "add submodule"]);
    git(
        f.dir.path(),
        &[
            "clone",
            "-q",
            "--recurse-submodules",
            up.to_str().unwrap(),
            "repo",
        ],
    );
    let repo = f.path("repo");
    std::fs::write(repo.join("a.txt"), "unpushed\n").unwrap();
    git(&repo, &["commit", "-qam", "never pushed"]);
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    std::fs::write(repo.join("vendor/lib/lib.txt"), "edited lib\n").unwrap();
    repo
}

/// What `blob_verify` says of the source before its bundle is built: asked
/// about the manifests and the files only.
async fn verify_before_bundle(c: &Client, p: &Parcel) -> BlobVerifyResponse {
    let r = Request::BlobVerify(BlobVerifyRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        source_id: p.source.clone(),
        blobs: p.refs().into_iter().filter(|b| b.kind != Bundle).collect(),
    });
    match call(c, r).await.unwrap() {
        Response::BlobVerify(v) => v,
        other => panic!("{other:?}"),
    }
}

/// Send a Git source as a coordinator does: manifests and files, then ask
/// what the worker's clone holds, then a bundle built on exactly that.
/// Returns the commits the worker listed and the bundle that was sent.
async fn send_git(c: &Client, f: &Fx, p: &Parcel) -> (Vec<String>, Vec<u8>) {
    let e1 = CoordinatorEpoch(1);
    for m in &p.manifests {
        let r = put_blob_as(c, e1, &p.source, Manifest, m, 0, 4000, T).await;
        assert!(r.unwrap().complete);
    }
    for b in p.blobs.values() {
        let r = put_blob_as(c, e1, &p.source, Content, b, 0, 4000, T).await;
        assert!(r.unwrap().complete);
    }
    let v = verify_before_bundle(c, p).await;
    assert!(!v.ready && v.bundle_needed, "{v:?}");
    assert!(v.missing.is_empty() && v.corrupt.is_empty(), "{v:?}");
    assert_eq!(v.have.len(), 1);
    assert_eq!(
        Some(&v.have[0].repository_id),
        p.root.repository_id.as_ref()
    );
    let have = v.have[0].commits.clone();
    let bundle = f.bundle(&p.root, &have);
    let r = put_blob_as(c, e1, &p.source, Bundle, &bundle, 0, 4000, T).await;
    assert!(r.unwrap().complete);
    let v = verify_before_bundle(c, p).await;
    assert!(v.ready && !v.bundle_needed, "{v:?}");
    (have, bundle)
}

/// Run `script` in an attempt made from the source and wait for it to
/// succeed. `false`: no attempt can be made here (see [`fast_worktree_dir`]).
async fn run_script(c: &Client, f: &Fx, p: &Parcel, script: &str) -> bool {
    if f.tools.is_none() {
        return false;
    }
    let a = AttemptId::generate();
    let submit = Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            a.clone(),
            payload(p, script),
        )
        .unwrap(),
    );
    call(c, submit).await.unwrap();
    let outcome = f.wait_terminal(&a);
    let store = WorkerStore::open_read_only(&f.state()).unwrap();
    let events = store.events_of_attempt(&a, 50);
    assert_eq!(outcome, Outcome::Succeeded, "{events:?}");
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_git_source_runs_on_a_worker_that_holds_nothing_and_the_next_one_travels_thin() {
    let mut f = fx();
    f.tools = fast_worktree_dir();
    let repo = project(&f);
    let first = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_eq!(first.manifests.len(), 2);
    assert!(first.root.submodules[0].dirty);
    // The next source: one more edit; the dirty submodule stays as it is.
    std::fs::write(repo.join("a.txt"), "edited again\n").unwrap();
    let second = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_ne!(first.source, second.source);
    let lib_capture = first.root.submodules[0].source_id.clone().unwrap();
    assert_eq!(
        second.root.submodules[0].source_id,
        Some(lib_capture.clone())
    );
    // There is nowhere to fetch from: the upstream of the repository and the
    // upstream of its submodule are gone before the worker sees anything.
    std::fs::remove_dir_all(f.path("up")).unwrap();
    std::fs::remove_dir_all(f.path("lib")).unwrap();

    let agent = f.start();
    let c = f.connect(&agent).await;
    assert!(f.clones().is_empty());

    // The worker holds nothing: the bundle is built on nothing, and brings
    // the whole history.
    let (have, bundle) = send_git(&c, &f, &first).await;
    assert_eq!(have, Vec::<String>::new());
    assert_eq!(bundle_prerequisites(&bundle), Ok(vec![]));
    let clones = f.clones();
    assert_eq!(clones.len(), 1);
    assert_eq!(git(&clones[0], &["remote"]), "");
    let out = f.path("out-1");
    let script = |out: &Path| {
        format!(
            "cat a.txt vendor/lib/lib.txt > {0} && git log -1 --format=%s HEAD >> {0} && wc -c < history.bin >> {0}",
            out.display()
        )
    };
    if run_script(&c, &f, &first, &script(&out)).await {
        let text = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        assert_eq!(lines, ["edited", "edited lib", "never pushed", "40000"]);
    }

    // The next source of the repository: the worker lists what its clone
    // holds, and the bundle brings only what is new.
    let lib_manifest = f.worker_store().object_store().manifest_path(&lib_capture);
    let inode = |p: &Path| std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(p).unwrap());
    let before = inode(&lib_manifest);
    let (have, thin) = send_git(&c, &f, &second).await;
    assert!(have.len() <= 64);
    assert!(
        have.contains(first.root.execution_commit.as_ref().unwrap()),
        "{have:?}"
    );
    let prerequisites = bundle_prerequisites(&thin).unwrap();
    assert!(!prerequisites.is_empty());
    let whole = second.bundle.as_ref().unwrap();
    assert_eq!(bundle_prerequisites(whole), Ok(vec![]));
    assert!(
        thin.len() + 30_000 < whole.len(),
        "{} against {}",
        thin.len(),
        whole.len()
    );
    // The submodule's capture was published with the first source: it is
    // taken as held, and the store's copy is not written again.
    assert_eq!(inode(&lib_manifest), before);
    let out = f.path("out-2");
    if run_script(&c, &f, &second, &script(&out)).await {
        let text = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        assert_eq!(
            lines,
            ["edited again", "edited lib", "never pushed", "40000"]
        );
    }
    assert_eq!(f.clones().len(), 1);
}

#[test]
fn a_bundle_built_on_what_the_worker_lacks_is_answered_with_what_it_holds() {
    let f = fx();
    let repo = project(&f);
    let first = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    git(&repo, &["commit", "-qam", "later"]);
    let later = git(&repo, &["rev-parse", "HEAD"]);
    std::fs::write(repo.join("a.txt"), "edited after that\n").unwrap();
    let second = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    // Built on a commit the coordinator has and the worker does not.
    let thin = f.bundle(&second.root, std::slice::from_ref(&later));
    assert_eq!(bundle_prerequisites(&thin), Ok(vec![later.clone()]));
    let base_missing = |e: &ApiError| match &e.conflict_reason {
        Some(ConflictReason::BundleBaseMissing { have }) => have.clone(),
        other => panic!("{other:?} in {e:?}"),
    };
    let no_bundle = |f: &Fx, p: &Parcel| {
        f.staged(&p.source)
            .iter()
            .all(|n| !n.starts_with("bundle-"))
    };

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    for man in &second.manifests {
        put_all(sink, &l, &second.source, Manifest, man).unwrap();
    }
    for b in second.blobs.values() {
        put_all(sink, &l, &second.source, Content, b).unwrap();
    }
    // A worker with no clone at all: the answer says so, the bundle is not
    // kept, and no clone is made for it.
    let e = put_all(sink, &l, &second.source, Bundle, &thin).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert_eq!(base_missing(&e), Vec::<String>::new());
    assert!(no_bundle(&f, &second));
    assert!(f.clones().is_empty());
    let v = verify(sink, &l, &second.source, vec![]);
    assert!(!v.ready && v.bundle_needed);
    assert_eq!(v.have[0].commits, Vec::<String>::new());

    // A worker whose clone holds another part of the history.
    send_all(sink, &l, &first);
    assert!(verify(sink, &l, &first.source, vec![]).ready);
    let e = put_all(sink, &l, &second.source, Bundle, &thin).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    let have = base_missing(&e);
    assert!(have.contains(first.root.execution_commit.as_ref().unwrap()));
    assert!(!have.contains(&later));
    assert!(no_bundle(&f, &second));
    assert_eq!(
        verify(sink, &l, &second.source, vec![]).have[0].commits,
        have
    );

    // A bundle whose pack is cut short is another matter: it is not asked
    // for again with another base. (Git fails on it as it does on a full
    // disk, so it is not laid on the bundle either: it stays until the next
    // one takes its place.)
    let whole = second.bundle.as_ref().unwrap();
    let mut broken = whole.clone();
    broken.truncate(whole.len() - 40);
    let e = put_all(sink, &l, &second.source, Bundle, &broken).unwrap_err();
    assert_eq!(e.code, ErrorCode::Internal);
    assert_eq!(e.conflict_reason, None);
    assert!(!no_bundle(&f, &second));
    assert!(!verify(sink, &l, &second.source, vec![]).ready);

    // Built on what the worker said it holds, it fits.
    let fitting = f.bundle(&second.root, &have);
    assert!(!bundle_prerequisites(&fitting).unwrap().is_empty());
    assert!(
        put_all(sink, &l, &second.source, Bundle, &fitting)
            .unwrap()
            .complete
    );
    assert!(verify(sink, &l, &second.source, vec![]).ready);
    assert_eq!(f.clones().len(), 1);
}

// ------------------------------------------------- what a source may take

/// The bytes of everything a parcel of a directory brings.
fn total_of(p: &Parcel) -> u64 {
    let files: usize = p.blobs.values().map(Vec::len).sum();
    (p.manifests[0].len() + files) as u64
}

#[test]
fn what_a_source_takes_is_reserved_before_it_is_written() {
    let f = fx();
    let (p, q, r) = (
        f.directory("p", 50),
        f.directory("q", 51),
        f.directory("r", 52),
    );
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    let man = &p.manifests[0];

    // The manifest itself counts for the source, with the files it names.
    let tight = BlobLimits {
        max_source_bytes: total_of(&p) - 1,
        ..limits()
    };
    let e = put_all(sink, &tight, &p.source, Manifest, man).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("larger"), "{}", e.message);
    assert!(!f.incoming(&p.source).exists());
    let exact = BlobLimits {
        max_source_bytes: total_of(&p),
        ..limits()
    };
    // Reserved from the first chunk of the manifest on, then with its files.
    put_part(sink, &exact, &p.source, Manifest, man, 0, 10).unwrap();
    assert_eq!(f.reserved(&p.source), man.len() as u64);
    put_part(sink, &exact, &p.source, Manifest, man, 10, man.len()).unwrap();
    assert_eq!(f.reserved(&p.source), total_of(&p));

    // All the sources that are part-way share one bound. A manifest there is
    // no room for stays where it is, and is taken once there is room.
    let shared = BlobLimits {
        max_incoming_bytes: total_of(&p) + total_of(&q) - 1,
        ..limits()
    };
    let e = put_all(sink, &shared, &q.source, Manifest, &q.manifests[0]).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert_eq!(f.reserved(&q.source), q.manifests[0].len() as u64);
    let (_, big) = q.blob_of_len(5000);
    let e = put_all(sink, &shared, &q.source, Content, big).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    let last = q.manifests[0].len() - 1;
    let r1 = put_part(
        sink,
        &l,
        &q.source,
        Manifest,
        &q.manifests[0],
        last,
        last + 1,
    )
    .unwrap();
    assert!(r1.complete);
    assert_eq!(f.reserved(&q.source), total_of(&q));
    // Not even the first byte of a manifest, when all the room is taken.
    let full = BlobLimits {
        max_incoming_bytes: total_of(&p) + total_of(&q),
        ..limits()
    };
    let e = put_part(sink, &full, &r.source, Manifest, &r.manifests[0], 0, 1).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(!f.incoming(&r.source).exists());

    // The disk keeps its floor.
    let floor = BlobLimits {
        min_free_bytes: u64::MAX / 2,
        ..limits()
    };
    let e = put_part(sink, &floor, &r.source, Manifest, &r.manifests[0], 0, 1).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(e.message.contains("disk"), "{}", e.message);
    assert!(!f.incoming(&r.source).exists());

    // Before the source's own manifest is staged, one manifest is part-way:
    // another one takes its place.
    let stray = noise(5, 300);
    put_part(sink, &l, &r.source, Manifest, &stray, 0, 100).unwrap();
    put_part(sink, &l, &r.source, Manifest, &r.manifests[0], 0, 10).unwrap();
    let parts: Vec<String> = f.staged(&r.source).into_iter().collect();
    let d = Digest::of_bytes(&r.manifests[0]);
    let hex = d.as_str().strip_prefix("sha256:").unwrap();
    assert_eq!(
        parts,
        [staged_name(
            Manifest,
            hex,
            r.manifests[0].len() as u64,
            false
        )]
    );
}

#[test]
fn a_source_holds_one_bundle_and_its_size_counts() {
    let f = fx();
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    put_all(sink, &l, &p.source, Manifest, &p.manifests[0]).unwrap();
    let base = f.reserved(&p.source);
    let bundles = |f: &Fx| -> Vec<String> {
        f.staged(&p.source)
            .into_iter()
            .filter(|n| n.starts_with("bundle-"))
            .collect()
    };
    let name = |b: &[u8]| {
        let d = Digest::of_bytes(b);
        let hex = d.as_str().strip_prefix("sha256:").unwrap().to_string();
        staged_name(Bundle, &hex, b.len() as u64, false)
    };

    let (one, two) = (noise(1, 3000), noise(2, 4000));
    put_part(sink, &l, &p.source, Bundle, &one, 0, 1000).unwrap();
    assert_eq!(bundles(&f), [name(&one)]);
    assert_eq!(f.reserved(&p.source), base + 3000);
    // Another bundle takes the place of the first: never two.
    put_part(sink, &l, &p.source, Bundle, &two, 0, 10).unwrap();
    assert_eq!(bundles(&f), [name(&two)]);
    assert_eq!(f.reserved(&p.source), base + 4000);
    // The same digest under another size is another blob.
    let d = Digest::of_bytes(&two);
    put_raw(sink, &l, &p.source, Bundle, &d, 4001, 0, &two[..10]).unwrap();
    assert_eq!(bundles(&f).len(), 1);
    // A bundle that does not fit the source with the rest of it.
    let tight = BlobLimits {
        max_source_bytes: base + 4999,
        ..limits()
    };
    let e = put_part(sink, &tight, &p.source, Bundle, &noise(3, 5000), 0, 10).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("larger"), "{}", e.message);
    assert!(bundles(&f).is_empty());
}

#[test]
fn a_hash_keeps_the_size_the_first_manifest_gave_it() {
    use local_ci::source::{seal_manifest, RelPath, SubmoduleRef};
    use remote_task_runner::source::classify::source_id_of_ci;
    let f = fx();
    let (top, sub) = (f.directory("top", 60), f.directory("sub", 61));
    let (hash, big) = top.blob_of_len(5000);
    // A capture that gives a file of the source another size, and a source
    // that names that capture. Both are sealed: only the sizes disagree.
    let mut lying = sub.root.clone();
    let entry = &mut lying.object_manifest.entries[0];
    entry.hash = hash.to_string();
    entry.size = 4 << 30;
    let lying = seal_manifest(lying);
    let mut root = top.root.clone();
    root.submodules = vec![SubmoduleRef {
        path: RelPath::new("vendor/sub"),
        revision: "0123456789abcdef0123456789abcdef01234567".into(),
        source_id: Some(lying.source_id.clone()),
        dirty: false,
    }];
    let root = seal_manifest(root);
    let source = source_id_of_ci(&root.source_id).unwrap();
    let root_bytes = serde_json::to_vec(&root).unwrap();

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    put_all(sink, &l, &source, Manifest, &root_bytes).unwrap();
    let before = (f.staged(&source), f.reserved(&source));
    let e = put_all(
        sink,
        &l,
        &source,
        Manifest,
        &serde_json::to_vec(&lying).unwrap(),
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(e.message.contains("another size"), "{}", e.message);
    // (what the refused manifest announced stays reserved: a reservation
    // never shrinks)
    let after = (f.staged(&source), f.reserved(&source));
    assert_eq!(after.0, before.0);
    assert!(after.1 >= before.1 && after.1 < before.1 + 4096);
    // The file is still taken with the size the source's manifest gives.
    let d = Digest::of_bytes(big);
    let e = put_raw(sink, &l, &source, Content, &d, 4 << 30, 0, &big[..10]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    assert!(put_all(sink, &l, &source, Content, big).unwrap().complete);
    // After a restart the staged manifests say the same.
    drop(m);
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let e = put_raw(sink, &l, &source, Content, &d, 4 << 30, 0, &big[..10]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
}

#[test]
fn sources_admitted_at_once_never_exceed_the_bound() {
    let f = fx();
    let parcels: Vec<Parcel> = (0..10)
        .map(|i| f.directory(&format!("d{i}"), 70 + i))
        .collect();
    let shards: BTreeSet<u64> = parcels
        .iter()
        .map(|p| lock_shard(p.source.as_str()))
        .collect();
    assert!(shards.len() > 3, "the sources share too few locks to race");
    let three = BlobLimits {
        max_staged_sources: 3,
        ..limits()
    };
    // Two openings of the state, as two processes; every source at once.
    let machines = [f.machine(), f.machine()];
    let go = std::sync::Barrier::new(parcels.len());
    let codes: Vec<Option<ErrorCode>> = std::thread::scope(|s| {
        let handles: Vec<_> = parcels
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let (m, go, l) = (&machines[i % 2], &go, &three);
                s.spawn(move || {
                    go.wait();
                    put_all(m.blobs().unwrap(), l, &p.source, Manifest, &p.manifests[0])
                        .err()
                        .map(|e| e.code)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let admitted = codes.iter().filter(|c| c.is_none()).count();
    assert_eq!(admitted, 3, "{codes:?}");
    assert!(codes.iter().flatten().all(|c| *c == ErrorCode::Overloaded));
    let staged = parcels
        .iter()
        .filter(|p| f.incoming(&p.source).exists())
        .count();
    assert_eq!(staged, 3);
}

/// Every file below `dir`, by its path from there, with its length.
fn tree_of(dir: &Path) -> BTreeMap<String, u64> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, u64>) {
        for e in std::fs::read_dir(dir).into_iter().flatten() {
            let path = e.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                out.insert(rel.to_string_lossy().into_owned(), meta.len());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// The scratch directories under the worker's clones (none stays).
fn clone_scratch(f: &Fx) -> Vec<PathBuf> {
    std::fs::read_dir(f.state().join("clones"))
        .into_iter()
        .flatten()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_dir() && !p.join(".git").exists())
        .collect()
}

/// `b`'s manifest, sealed again, with its first file given as the file
/// `hash` with `size` bytes; and the files it still names of `b`.
fn lying_about(b: &Parcel, hash: &str, size: u64) -> (SourceId, Vec<u8>, Vec<Vec<u8>>) {
    use local_ci::source::seal_manifest;
    use remote_task_runner::source::classify::source_id_of_ci;
    let mut m = b.root.clone();
    m.object_manifest.entries[0].hash = hash.to_string();
    m.object_manifest.entries[0].size = size;
    let m = seal_manifest(m);
    let files = m.object_manifest.entries[1..]
        .iter()
        .map(|e| b.blobs[&e.hash].clone())
        .collect();
    (
        source_id_of_ci(&m.source_id).unwrap(),
        serde_json::to_vec(&m).unwrap(),
        files,
    )
}

#[test]
fn a_source_that_gives_a_stored_file_another_size_leaves_nothing_in_the_store() {
    let f = fx();
    let (a, b, c) = (
        f.directory("a", 70),
        f.directory("b", 71),
        f.directory("c", 72),
    );
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    send_all(sink, &l, &a);
    let store = f.state().join("sources");

    // The store holds the file: a manifest that gives it another size is
    // refused when it arrives, before any file of that source is taken.
    let (held, _) = a.blob_of_len(5000);
    let before = tree_of(&store);
    for wrong in [4999, 5001, 4 << 30] {
        let (source, manifest, _) = lying_about(&b, held, wrong);
        let e = put_all(sink, &l, &source, Manifest, &manifest).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("another size"), "{}", e.message);
        assert!(!f.incoming(&source).exists());
    }
    assert_eq!(tree_of(&store), before);

    // The store gets the file after the manifest is staged: the source is
    // refused when it would be published, and none of the files it brought
    // goes into the store. With another source id it is the same again.
    let (later, _) = c.blob_of_len(5000);
    let mut staged = vec![];
    for wrong in [4999, 5001] {
        let (source, manifest, files) = lying_about(&b, later, wrong);
        put_all(sink, &l, &source, Manifest, &manifest).unwrap();
        for file in &files {
            put_all(sink, &l, &source, Content, file).unwrap();
        }
        assert!(f.incoming(&source).exists());
        staged.push((source, files));
    }
    send_all(sink, &l, &c);
    let before = tree_of(&store);
    let worker = f.worker_store();
    for (source, files) in &staged {
        let again = files.iter().find(|b| !b.is_empty()).unwrap();
        let e = put_all(sink, &l, source, Content, again).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("another size"), "{}", e.message);
        // nothing of it waits here either
        assert!(!f.incoming(source).exists());
        for file in files.iter().filter(|b| !b.is_empty()) {
            let d = Digest::of_bytes(file);
            let hex = d.as_str().strip_prefix("sha256:").unwrap();
            assert_eq!(worker.has_blob(hex), a.blobs.contains_key(hex));
        }
    }
    assert_eq!(tree_of(&store), before);
    // The sources that say what is so are taken as before.
    send_all(sink, &l, &b);
    assert!(verify(sink, &l, &b.source, b.refs()).ready);
}

#[test]
fn a_bundle_that_is_refused_writes_nothing_into_the_clone_and_makes_none() {
    let f = fx();
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let bundle = p.bundle.clone().unwrap();
    // Another repository, with commits under the names the bundle of a
    // source has to bring: a bundle of it unpacks, and is not the source's.
    let other = f.path("other");
    std::fs::create_dir_all(&other).unwrap();
    git(&other, &["init", "-q", "-b", "main"]);
    std::fs::write(other.join("x.txt"), noise(9, 2000)).unwrap();
    git(&other, &["add", "."]);
    git(&other, &["commit", "-qm", "something else"]);
    let foreign = |root: &CiManifest| {
        let names = [
            format!("refs/local-ci/snapshots/{}", root.source_id),
            format!("refs/local-ci/index/{}", root.source_id),
        ];
        // (the index ref is a commit with no history)
        let tree = git(&other, &["rev-parse", "HEAD^{tree}"]);
        let lone = git(&other, &["commit-tree", "-m", "index", &tree]);
        git(&other, &["update-ref", &names[0], "HEAD"]);
        git(&other, &["update-ref", &names[1], &lone]);
        let out = f.path("foreign.bundle");
        let out_s = out.to_str().unwrap();
        git(
            &other,
            &["bundle", "create", "-q", out_s, &names[0], &names[1]],
        );
        std::fs::read(out).unwrap()
    };

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    let send_rest = |p: &Parcel| {
        for man in &p.manifests {
            put_all(sink, &l, &p.source, Manifest, man).unwrap();
        }
        for b in p.blobs.values() {
            put_all(sink, &l, &p.source, Content, b).unwrap();
        }
    };
    send_rest(&p);

    // A repository the worker has no clone of. A bundle whose pack is
    // broken (its header is whole), time and again, and one that brings
    // other commits: no clone is made, and nothing is left beside.
    let mut broken = bundle.clone();
    let n = broken.len();
    for b in &mut broken[n - 40..] {
        *b ^= 0x55;
    }
    assert_eq!(bundle_prerequisites(&broken), Ok(vec![]));
    // (Git fails on a broken pack as it does on a full disk, so the worker
    // does not lay it on the bundle: `INTERNAL`, and the bundle stays until
    // another takes its place)
    let other_commits = foreign(&p.root);
    for (wrong, code) in [
        (&broken, ErrorCode::Internal),
        (&broken, ErrorCode::Internal),
        (&other_commits, ErrorCode::InvalidInput),
    ] {
        let e = put_all(sink, &l, &p.source, Bundle, wrong).unwrap_err();
        assert_eq!(e.code, code, "{e:?}");
        assert!(e.message.contains("bundle"), "{}", e.message);
        assert!(f.clones().is_empty());
        assert!(clone_scratch(&f).is_empty(), "{:?}", clone_scratch(&f));
        let staged = f.staged(&p.source);
        let kept = staged.iter().filter(|n| n.starts_with("bundle-")).count();
        assert_eq!(kept, usize::from(code == ErrorCode::Internal), "{staged:?}");
    }
    assert!(!verify(sink, &l, &p.source, p.refs()).ready);
    put_all(sink, &l, &p.source, Bundle, &bundle).unwrap();
    assert!(verify(sink, &l, &p.source, p.refs()).ready);
    let clones = f.clones();
    assert_eq!(clones.len(), 1);

    // The clone is there. The same two bundles for the next source of the
    // repository: its refs and its objects stay what they were.
    std::fs::write(repo.join("a.txt"), "edited once more\n").unwrap();
    let q = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    send_rest(&q);
    let state = |c: &Path| {
        (
            git(c, &["for-each-ref"]),
            git(c, &["count-objects", "-v"]),
            tree_of(&c.join(".git").join("objects")),
        )
    };
    let before = state(&clones[0]);
    let whole = f.bundle(&q.root, &[]);
    let mut broken = whole.clone();
    let n = broken.len();
    for b in &mut broken[n - 40..] {
        *b ^= 0x55;
    }
    let store_before = tree_of(&f.state().join("sources"));
    let other_commits = foreign(&q.root);
    for (wrong, code) in [
        (&broken, ErrorCode::Internal),
        (&other_commits, ErrorCode::InvalidInput),
    ] {
        let e = put_all(sink, &l, &q.source, Bundle, wrong).unwrap_err();
        assert_eq!(e.code, code, "{e:?}");
        assert_eq!(state(&clones[0]), before);
        assert!(clone_scratch(&f).is_empty());
        assert_eq!(f.clones(), clones);
    }
    // (and no file of the refused source went into the store)
    assert_eq!(tree_of(&f.state().join("sources")), store_before);
    put_all(sink, &l, &q.source, Bundle, &whole).unwrap();
    assert!(verify(sink, &l, &q.source, q.refs()).ready);
    assert!(clone_scratch(&f).is_empty());
}

#[test]
fn a_clone_is_tried_beside_and_an_empty_one_can_go() {
    let f = fx();
    let state = f.state();
    std::fs::create_dir_all(&state).unwrap();
    let ws = Workspaces::new(
        state.clone(),
        f.env(),
        Arc::new(CliFastWorktree::new(f.env())),
    );
    let id = "github:example/repo";
    // No clone: nothing to remove, and the scratch repository is empty.
    assert!(!ws.remove_clone_if_empty(id).unwrap());
    let (tmp, trial) = ws.rehearsal(id).unwrap();
    assert_eq!(git(trial.dir(), &["for-each-ref"]), "");
    assert!(f.clones().is_empty());
    let at = tmp.path().to_path_buf();
    drop(tmp);
    assert!(!at.exists());

    // An empty clone goes, one that holds a ref stays.
    let clone = ws.ensure_clone(id, None).unwrap();
    assert!(ws.remove_clone_if_empty(id).unwrap());
    assert!(!clone.dir().exists());
    let clone = ws.ensure_clone(id, None).unwrap();
    git(clone.dir(), &["commit", "-q", "--allow-empty", "-m", "one"]);
    let head = git(clone.dir(), &["rev-parse", "HEAD"]);
    git(
        clone.dir(),
        &["update-ref", "refs/local-ci/snapshots/s", &head],
    );
    assert!(!ws.remove_clone_if_empty(id).unwrap());
    assert!(clone.dir().join(".git").exists());

    // The scratch repository has the refs and the objects of the clone, and
    // what is written to it is not written to the clone.
    let refs = git(clone.dir(), &["for-each-ref"]);
    let objects = tree_of(&clone.dir().join(".git").join("objects"));
    let (tmp, trial) = ws.rehearsal(id).unwrap();
    assert_eq!(git(trial.dir(), &["for-each-ref"]), refs);
    assert_eq!(git(trial.dir(), &["cat-file", "-t", &head]), "commit");
    let tree = git(trial.dir(), &["mktree"]);
    let two = git(trial.dir(), &["commit-tree", "-m", "two", &tree]);
    git(
        trial.dir(),
        &["update-ref", "refs/local-ci/snapshots/t", &two],
    );
    assert_eq!(git(trial.dir(), &["cat-file", "-t", &two]), "commit");
    assert_eq!(git(clone.dir(), &["for-each-ref"]), refs);
    assert_eq!(tree_of(&clone.dir().join(".git").join("objects")), objects);
    drop(tmp);
    assert!(clone_scratch(&f).is_empty());
}

const GIB: u64 = 1 << 30;

/// The receiving side on a disk of `capacity` bytes besides what the worker
/// holds when it starts: the free space it reads is that, less what is
/// staged.
fn incoming_on_disk(f: &Fx, capacity: &Arc<AtomicU64>) -> Incoming {
    let state = f.state();
    std::fs::create_dir_all(&state).unwrap();
    let ws = Workspaces::new(
        state.clone(),
        f.env(),
        Arc::new(CliFastWorktree::new(f.env())),
    );
    let (capacity, staged) = (capacity.clone(), state.join("incoming"));
    Incoming::new(&state, f.worker_store(), ws, f.env()).with_free_reading(Box::new(move |_| {
        let used: u64 = tree_of(&staged)
            .iter()
            .filter(|(name, _)| name.ends_with(".part") || name.ends_with(".done"))
            .map(|(_, len)| len)
            .sum();
        Ok(capacity.load(Ordering::SeqCst).saturating_sub(used))
    }))
}

fn largest_of(p: &Parcel) -> u64 {
    p.blobs.values().map(|b| b.len() as u64).max().unwrap()
}

#[test]
fn the_disk_is_asked_for_what_every_source_reserved_and_has_not_written() {
    let f = fx();
    let (p, q) = (f.directory("p", 80), f.directory("q", 81));
    let (mp, mq) = (&p.manifests[0], &q.manifests[0]);
    // What a source takes of the disk: what it stages, and to publish it
    // its manifest once more and its largest file twice.
    let disk_of = |p: &Parcel| total_of(p) + p.manifests[0].len() as u64 + 2 * largest_of(p);
    let l = limits();
    assert_eq!(l.min_free_bytes, GIB);

    // One byte short of both sources and the floor of 1 GiB.
    let capacity = Arc::new(AtomicU64::new(GIB + disk_of(&p) + disk_of(&q) - 1));
    let sink = incoming_on_disk(&f, &capacity);
    put_all(&sink, &l, &p.source, Manifest, mp).unwrap();
    assert_eq!(
        f.reservation(&p.source),
        Reservation {
            bytes: total_of(&p),
            disk: disk_of(&p)
        }
    );
    // The first source has written its manifest and nothing else: the rest
    // of what it reserved is not there for the second one.
    let e = put_all(&sink, &l, &q.source, Manifest, mq).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(e.message.contains("disk"), "{}", e.message);
    // (the files of the first do not change that: the disk has that much
    // less, and that much less is still to come)
    let mut written = 0;
    for b in p.blobs.values() {
        if b.len() as u64 != largest_of(&p) {
            put_all(&sink, &l, &p.source, Content, b).unwrap();
            written += b.len();
        }
    }
    assert!(written > 0 && f.incoming(&p.source).exists());
    let last = mq.len() - 1;
    let e = put_part(&sink, &l, &q.source, Manifest, mq, last, last + 1).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(e.message.contains("disk"), "{}", e.message);
    // With the one byte, it is taken.
    capacity.fetch_add(1, Ordering::SeqCst);
    let r = put_part(&sink, &l, &q.source, Manifest, mq, last, last + 1).unwrap();
    assert!(r.complete);
}

#[test]
fn a_bundle_is_asked_of_the_disk_with_what_its_manifest_reserved() {
    let f = fx();
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    std::fs::write(repo.join("big.bin"), noise(3, 60_000)).unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let bundle = p.bundle.as_ref().unwrap();
    let size = bundle.len() as u64;
    assert_eq!(p.manifests.len(), 1);
    let l = limits();

    let capacity = Arc::new(AtomicU64::new(u64::MAX));
    let sink = incoming_on_disk(&f, &capacity);
    put_all(&sink, &l, &p.source, Manifest, &p.manifests[0]).unwrap();
    let first = f.reservation(&p.source);
    assert_eq!(first.bytes, total_of(&p));
    // The manifest reserved the files it names; none is written. The bundle
    // is staged and unpacked into the clone: twice its size more. A disk
    // that has room for the bundle and the floor, and not for the files that
    // are still to come as well, does not take its first byte.
    capacity.store(GIB + first.disk + 2 * size - 1, Ordering::SeqCst);
    assert!(GIB + first.disk + 2 * size - 1 > GIB + first.bytes + size);
    let e = put_part(&sink, &l, &p.source, Bundle, bundle, 0, 10).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    assert!(e.message.contains("disk"), "{}", e.message);
    assert!(f
        .staged(&p.source)
        .iter()
        .all(|n| !n.starts_with("bundle-")));
    assert_eq!(f.reservation(&p.source), first);
    capacity.fetch_add(1, Ordering::SeqCst);
    put_part(&sink, &l, &p.source, Bundle, bundle, 0, 10).unwrap();
    assert_eq!(
        f.reservation(&p.source),
        Reservation {
            bytes: first.bytes + size,
            disk: first.disk + 2 * size
        }
    );
}

#[test]
fn a_commit_with_no_repository_to_hold_it_is_refused_before_the_store_is_written() {
    use local_ci::source::{seal_manifest, RelPath, SubmoduleRef};
    use remote_task_runner::source::classify::source_id_of_ci;
    let f = fx();
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join("a.txt"), noise(4, 3000)).unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let d = f.directory("d", 90);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    let store = f.state().join("sources");
    let before = tree_of(&store);

    // A Git source that names no repository.
    let mut root = p.root.clone();
    root.repository_id = None;
    let root = seal_manifest(root);
    let source = source_id_of_ci(&root.source_id).unwrap();
    let mut sent = vec![(source, vec![serde_json::to_vec(&root).unwrap()], &p.blobs)];
    // A directory that names a dirty submodule capture with a commit.
    let mut top = d.root.clone();
    top.submodules = vec![SubmoduleRef {
        path: RelPath::new("vendor/sub"),
        revision: p.root.execution_commit.clone().unwrap(),
        source_id: Some(p.root.source_id.clone()),
        dirty: true,
    }];
    let top = seal_manifest(top);
    let source = source_id_of_ci(&top.source_id).unwrap();
    let mut both = d.blobs.clone();
    both.extend(p.blobs.clone());
    let manifests = vec![serde_json::to_vec(&top).unwrap(), p.manifests[0].clone()];
    sent.push((source, manifests, &both));

    for (source, manifests, blobs) in &sent {
        let mut refused = None;
        for man in manifests {
            if let Err(e) = put_all(sink, &l, source, Manifest, man) {
                refused = Some(e);
            }
        }
        for b in blobs.values() {
            if refused.is_none() {
                refused = put_all(sink, &l, source, Content, b).err();
            }
        }
        let e = refused.expect("the source is refused");
        assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
        assert_eq!(tree_of(&store), before, "{e:?}");
        assert!(!f.incoming(source).exists(), "{e:?}");
        assert!(f.clones().is_empty());
    }
}

#[test]
fn the_free_space_of_the_store_and_of_the_clones_counts_too() {
    let f = fx();
    let p = f.directory("p", 91);
    let l = limits();
    let state = f.state();
    std::fs::create_dir_all(&state).unwrap();
    for (full, ok) in [
        ("clones", false),
        ("sources", false),
        ("incoming", false),
        ("-", true),
    ] {
        let ws = Workspaces::new(
            state.clone(),
            f.env(),
            Arc::new(CliFastWorktree::new(f.env())),
        );
        let sink = Incoming::new(&state, f.worker_store(), ws, f.env()).with_free_reading(
            Box::new(move |at| {
                let hit = at.components().any(|c| c.as_os_str() == full);
                Ok(if hit { GIB } else { 64 * GIB })
            }),
        );
        let r = put_all(&sink, &l, &p.source, Manifest, &p.manifests[0]);
        match r {
            Ok(_) => assert!(ok, "{full}"),
            Err(e) => {
                assert!(!ok, "{full}: {e:?}");
                assert_eq!(e.code, ErrorCode::Overloaded);
                assert!(e.message.contains("disk"), "{}", e.message);
                assert!(!f.incoming(&p.source).exists());
            }
        }
    }
}

// ------------------------- dirty submodules inside dirty submodules

/// [`project`] one level deeper: the submodule (`vendor/lib`) has a
/// submodule of its own (`inner/deep`), and all three levels hold an edit
/// that was never committed.
fn nested_project(f: &Fx) -> PathBuf {
    let deep = f.path("deep");
    std::fs::create_dir_all(&deep).unwrap();
    git(&deep, &["init", "-q", "-b", "main"]);
    std::fs::write(deep.join("deep.txt"), "deep\n").unwrap();
    git(&deep, &["add", "."]);
    git(&deep, &["commit", "-qm", "deep"]);
    let lib = f.path("lib");
    std::fs::create_dir_all(&lib).unwrap();
    git(&lib, &["init", "-q", "-b", "main"]);
    std::fs::write(lib.join("lib.txt"), "lib\n").unwrap();
    git(&lib, &["add", "."]);
    git(&lib, &["commit", "-qm", "lib"]);
    git(
        &lib,
        &[
            "submodule",
            "add",
            "-q",
            deep.to_str().unwrap(),
            "inner/deep",
        ],
    );
    git(&lib, &["commit", "-qm", "add deep"]);
    let up = f.path("up");
    std::fs::create_dir_all(&up).unwrap();
    git(&up, &["init", "-q", "-b", "main"]);
    std::fs::write(up.join("a.txt"), "a\n").unwrap();
    std::fs::write(up.join("history.bin"), noise(78, 40_000)).unwrap();
    git(&up, &["add", "."]);
    git(&up, &["commit", "-qm", "base"]);
    git(
        &up,
        &[
            "submodule",
            "add",
            "-q",
            lib.to_str().unwrap(),
            "vendor/lib",
        ],
    );
    git(&up, &["commit", "-qm", "add submodule"]);
    git(
        f.dir.path(),
        &[
            "clone",
            "-q",
            "--recurse-submodules",
            up.to_str().unwrap(),
            "repo",
        ],
    );
    let repo = f.path("repo");
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    std::fs::write(repo.join("vendor/lib/lib.txt"), "edited lib\n").unwrap();
    std::fs::write(repo.join("vendor/lib/inner/deep/deep.txt"), "edited deep\n").unwrap();
    repo
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dirty_submodule_inside_a_dirty_submodule_reaches_the_worker_at_every_level() {
    let mut f = fx();
    f.tools = fast_worktree_dir();
    let repo = nested_project(&f);
    let first = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_eq!(first.manifests.len(), 3);
    let parent = f.parent();
    let lib = &first.root.submodules[0];
    assert!(lib.dirty);
    let lib_m = parent
        .manifest(lib.source_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let deep = &lib_m.submodules[0];
    assert!(deep.dirty);
    let deep_m = parent
        .manifest(deep.source_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    // The next source: one more edit at the top; both submodules stay.
    std::fs::write(repo.join("a.txt"), "edited again\n").unwrap();
    let second = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_ne!(first.source, second.source);
    assert_eq!(second.root.submodules[0].source_id, lib.source_id);
    // Nowhere to fetch from, at any level.
    for gone in ["up", "lib", "deep"] {
        std::fs::remove_dir_all(f.path(gone)).unwrap();
    }

    let agent = f.start();
    let c = f.connect(&agent).await;
    let (have, bundle) = send_git(&c, &f, &first).await;
    assert_eq!(have, Vec::<String>::new());
    assert_eq!(bundle_prerequisites(&bundle), Ok(vec![]));
    let clones = f.clones();
    assert_eq!(clones.len(), 1);
    let clone = &clones[0];

    // In the worker's clone: the snapshot ref of every level is the commit
    // its manifest names, with the tree it names, and each gitlink is the
    // commit of the level below.
    let commit_of = |m: &CiManifest| m.execution_commit.clone().unwrap();
    for m in [&first.root, &lib_m, &deep_m] {
        let r = format!("refs/local-ci/snapshots/{}", m.source_id);
        assert_eq!(git(clone, &["rev-parse", "--verify", &r]), commit_of(m));
        assert_eq!(
            Some(git(clone, &["rev-parse", &format!("{r}^{{tree}}")])),
            m.worktree_tree
        );
    }
    let gitlink = |m: &CiManifest, path: &str| {
        git(clone, &["rev-parse", &format!("{}:{path}", commit_of(m))])
    };
    assert_eq!(gitlink(&first.root, "vendor/lib"), commit_of(&lib_m));
    assert_eq!(gitlink(&lib_m, "inner/deep"), commit_of(&deep_m));

    // The files of every level come out of the worker's store.
    let worker = f.worker_store();
    let published = worker
        .manifest(&first.root.source_id)
        .unwrap()
        .expect("the source is published");
    let laid = f.path("laid");
    worker.materialize(&published, &laid).unwrap();
    let read = |p: &str| std::fs::read_to_string(laid.join(p)).unwrap();
    assert_eq!(read("a.txt"), "edited\n");
    assert_eq!(read("vendor/lib/lib.txt"), "edited lib\n");
    assert_eq!(read("vendor/lib/inner/deep/deep.txt"), "edited deep\n");

    let script = |out: &Path| {
        format!(
            "cat a.txt vendor/lib/lib.txt vendor/lib/inner/deep/deep.txt > {}",
            out.display()
        )
    };
    let out = f.path("out-1");
    if run_script(&c, &f, &first, &script(&out)).await {
        let text = std::fs::read_to_string(&out).unwrap();
        assert_eq!(text, "edited\nedited lib\nedited deep\n");
    }

    // The next source is built on what the clone holds: a thin bundle. The
    // snapshots of both submodules are in the clone already, with the same
    // commits, and are taken as they are.
    let (have, thin) = send_git(&c, &f, &second).await;
    assert!(
        have.contains(first.root.execution_commit.as_ref().unwrap()),
        "{have:?}"
    );
    assert!(!bundle_prerequisites(&thin).unwrap().is_empty());
    let whole = second.bundle.as_ref().unwrap();
    assert!(
        thin.len() + 30_000 < whole.len(),
        "{} against {}",
        thin.len(),
        whole.len()
    );
    let r = format!("refs/local-ci/snapshots/{}", second.root.source_id);
    assert_eq!(
        git(clone, &["rev-parse", "--verify", &r]),
        commit_of(&second.root)
    );
    assert_eq!(gitlink(&second.root, "vendor/lib"), commit_of(&lib_m));
    let out = f.path("out-2");
    if run_script(&c, &f, &second, &script(&out)).await {
        let text = std::fs::read_to_string(&out).unwrap();
        assert_eq!(text, "edited again\nedited lib\nedited deep\n");
    }
    assert_eq!(f.clones().len(), 1);
}

#[test]
fn a_bundle_waits_for_another_import_into_the_same_clone() {
    let f = fx();
    let repo = project(&f);
    let p = f.capture(&SourceRequest::Worktree { dir: repo });
    let m = f.machine();
    let sink = m.blobs().unwrap();
    let l = BlobLimits {
        lock_wait_ms: 50,
        ..limits()
    };
    for man in &p.manifests {
        put_all(sink, &l, &p.source, Manifest, man).unwrap();
    }
    for b in p.blobs.values() {
        put_all(sink, &l, &p.source, Content, b).unwrap();
    }
    // Another import into the clone of this repository is under way.
    let workspaces = Workspaces::new(f.state(), f.env(), Arc::new(CliFastWorktree::new(f.env())));
    let held = workspaces
        .import_lock(p.root.repository_id.as_ref().unwrap())
        .unwrap();
    held.lock().unwrap();
    let bundle = p.bundle.as_ref().unwrap();
    let e = put_all(sink, &l, &p.source, Bundle, bundle).unwrap_err();
    assert_eq!(e.code, ErrorCode::Overloaded);
    // (the lock of the clone, not the one of the source)
    assert!(e.message.contains("the clone of"), "{}", e.message);
    // Nothing was unpacked, no clone was made, and the bundle is kept.
    assert!(f.clones().is_empty());
    assert!(f.staged(&p.source).iter().any(|n| n.starts_with("bundle-")));
    drop(held);
    assert!(
        put_all(sink, &l, &p.source, Bundle, bundle)
            .unwrap()
            .complete
    );
    assert_eq!(f.clones().len(), 1);
    assert!(verify(sink, &l, &p.source, vec![]).ready);
}

#[test]
fn a_source_keeps_a_few_bundle_files_in_the_parent_store_and_they_can_be_removed() {
    use remote_task_runner::source::store::MAX_BUNDLES_PER_SOURCE;
    let f = fx();
    let repo = project(&f);
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    std::fs::write(repo.join("a.txt"), "another\n").unwrap();
    let other = f.capture(&SourceRequest::Worktree { dir: repo });
    let store = f.parent();
    let ctx = ExecContext::new(f.env());
    let files = |id: &str| -> Vec<PathBuf> {
        std::fs::read_dir(store.root().join("bundles"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                let name = p.file_name().unwrap().to_string_lossy().into_owned();
                name.starts_with(id) && name.ends_with(".bundle")
            })
            .collect()
    };
    // (the capture made one for a worker that holds nothing)
    assert_eq!(files(&p.root.source_id).len(), 1);
    assert_eq!(files(&other.root.source_id).len(), 1);
    // Every set of commits a peer names is another file: a peer that names
    // a new set each time does not fill the store. The oldest make room;
    // each file is given its own time here, one second after the one
    // before, so "oldest" does not hang on how fast the test runs.
    let stamp = |p: &Path, n: usize| {
        let at = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + n as u64);
        let f = std::fs::File::options().write(true).open(p).unwrap();
        f.set_modified(at).unwrap();
    };
    stamp(&files(&p.root.source_id)[0], 0);
    let mut written = vec![];
    let mut last = PathBuf::new();
    for i in 0..(MAX_BUNDLES_PER_SOURCE + 3) {
        let have = vec![format!("{:040x}", i + 1)];
        last = store.export_bundle(&ctx, &p.root, &have).unwrap();
        assert!(last.exists());
        stamp(&last, i + 1);
        written.push(last.clone());
        assert!(files(&p.root.source_id).len() <= MAX_BUNDLES_PER_SOURCE);
    }
    // What stays is what was written last.
    let sorted = |mut v: Vec<PathBuf>| {
        v.sort();
        v
    };
    assert_eq!(
        sorted(files(&p.root.source_id)),
        sorted(written[written.len() - MAX_BUNDLES_PER_SOURCE..].to_vec())
    );
    // What is not a commit id, and what is named twice, is not a new set.
    let noisy = vec![
        format!("{:040x}", MAX_BUNDLES_PER_SOURCE + 3),
        "not a commit".to_string(),
        format!("{:040x}", MAX_BUNDLES_PER_SOURCE + 3),
    ];
    assert_eq!(store.export_bundle(&ctx, &p.root, &noisy).unwrap(), last);
    // The other source's file was not touched, and is not removed with them.
    assert_eq!(files(&other.root.source_id).len(), 1);
    assert_eq!(
        store.remove_bundles(&p.root.source_id).unwrap(),
        MAX_BUNDLES_PER_SOURCE
    );
    assert!(files(&p.root.source_id).is_empty());
    assert_eq!(files(&other.root.source_id).len(), 1);
    assert_eq!(store.remove_bundles(&p.root.source_id).unwrap(), 0);
}

/// A repository with one commit and an edit on top, captured.
fn small_project(f: &Fx) -> (PathBuf, Parcel) {
    let repo = f.path("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join("a.txt"), "edited\n").unwrap();
    let p = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    (repo, p)
}

/// Everything of the parcel but its bundle.
fn send_but_bundle(sink: &dyn BlobSink, l: &BlobLimits, p: &Parcel) {
    for man in &p.manifests {
        put_all(sink, l, &p.source, Manifest, man).unwrap();
    }
    for b in p.blobs.values() {
        put_all(sink, l, &p.source, Content, b).unwrap();
    }
}

fn bundles_staged(f: &Fx, source: &SourceId) -> usize {
    let staged = f.staged(source);
    staged.iter().filter(|n| n.starts_with("bundle-")).count()
}

#[test]
fn a_git_that_fails_in_the_clone_is_not_laid_on_the_bundle() {
    let mut f = fx();
    f.git_shim();
    let (repo, p) = small_project(&f);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    send_but_bundle(sink, &l, &p);
    let bundle = p.bundle.clone().unwrap();

    // No clone yet. The bundle unpacks beside, and then the fetch into the
    // clone fails (as it does when the disk is full): the worker's fault.
    // The bundle is kept, and the clone made for it does not stay.
    f.git_fails("fetch", Some((0, 128)));
    for _ in 0..2 {
        let e = put_all(sink, &l, &p.source, Bundle, &bundle).unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal, "{e:?}");
        assert!(e.message.contains("no fetch today"), "{}", e.message);
        assert!(f.clones().is_empty());
        assert!(clone_scratch(&f).is_empty(), "{:?}", clone_scratch(&f));
        assert_eq!(bundles_staged(&f, &p.source), 1);
    }
    // Git works again: sending anything of the source once more is enough.
    f.git_fails("fetch", None);
    assert!(!verify(sink, &l, &p.source, p.refs()).ready);
    let again = put_all(sink, &l, &p.source, Manifest, &p.manifests[0]).unwrap();
    assert!(again.complete);
    assert!(verify(sink, &l, &p.source, p.refs()).ready);
    let clones = f.clones();
    assert_eq!(clones.len(), 1);

    // The clone is there. For the next source of the repository, Git cannot
    // say whether the commit is below its ref (it fails, which is not "no"):
    // `INTERNAL` again, and the bundle is kept.
    std::fs::write(repo.join("a.txt"), "edited once more\n").unwrap();
    let q = f.capture(&SourceRequest::Worktree { dir: repo });
    send_but_bundle(sink, &l, &q);
    let whole = f.bundle(&q.root, &[]);
    f.git_fails("merge-base", Some((0, 128)));
    let e = put_all(sink, &l, &q.source, Bundle, &whole).unwrap_err();
    assert_eq!(e.code, ErrorCode::Internal, "{e:?}");
    assert!(e.message.contains("no merge-base today"), "{}", e.message);
    assert_eq!(bundles_staged(&f, &q.source), 1);
    // (asking fails the same way: it is not answered with "send a bundle")
    let asked = sink.verify(
        &l,
        &BlobVerifyRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            source_id: q.source.clone(),
            blobs: vec![],
        },
    );
    assert_eq!(asked.unwrap_err().code, ErrorCode::Internal);
    f.git_fails("merge-base", None);
    let again = put_all(sink, &l, &q.source, Manifest, &q.manifests[0]).unwrap();
    assert!(again.complete);
    assert!(verify(sink, &l, &q.source, q.refs()).ready);
    assert!(clone_scratch(&f).is_empty());
}

#[test]
fn a_source_refused_after_its_bundle_unpacked_leaves_no_ref_in_the_clone() {
    let mut f = fx();
    f.git_shim();
    let (repo, p) = small_project(&f);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    let refs = |c: &Path| git(c, &["for-each-ref"]);

    // No clone yet, and Git says "no" in the clone to what it said "yes" to
    // beside it: the commit is not below its snapshot ref. Refused, and the
    // clone that was made for the bundle is gone with the refs.
    send_but_bundle(sink, &l, &p);
    let bundle = p.bundle.clone().unwrap();
    f.git_fails("merge-base", Some((0, 1)));
    let e = put_all(sink, &l, &p.source, Bundle, &bundle).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert!(e.message.contains("bundle is refused"), "{}", e.message);
    assert!(f.clones().is_empty());
    assert_eq!(bundles_staged(&f, &p.source), 0);
    f.git_fails("merge-base", None);
    put_all(sink, &l, &p.source, Bundle, &bundle).unwrap();
    assert!(verify(sink, &l, &p.source, p.refs()).ready);
    let clones = f.clones();
    assert_eq!(clones.len(), 1);
    let before = refs(&clones[0]);
    assert_eq!(before.lines().count(), 2, "{before}");

    // The clone is there, with the refs of the first source. The same for
    // the next one: its two refs (snapshot and index) are not left behind.
    std::fs::write(repo.join("a.txt"), "edited once more\n").unwrap();
    let q = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    send_but_bundle(sink, &l, &q);
    let whole = f.bundle(&q.root, &[]);
    f.git_fails("merge-base", Some((0, 1)));
    let e = put_all(sink, &l, &q.source, Bundle, &whole).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert_eq!(refs(&clones[0]), before);
    assert_eq!(bundles_staged(&f, &q.source), 0);

    // And when the "no" comes one question later, as the manifest is about
    // to be published: refused as well, with nothing left in the clone and
    // no manifest in the store.
    f.git_fails("merge-base", Some((1, 1)));
    let e = put_all(sink, &l, &q.source, Bundle, &whole).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert!(e.message.contains("source is refused"), "{}", e.message);
    assert_eq!(refs(&clones[0]), before);
    assert!(f
        .worker_store()
        .manifest(&q.root.source_id)
        .unwrap()
        .is_none());
    f.git_fails("merge-base", None);
    assert!(!verify(sink, &l, &q.source, q.refs()).ready);
    let again = put_all(sink, &l, &q.source, Bundle, &whole).unwrap();
    assert!(again.complete);
    assert!(verify(sink, &l, &q.source, q.refs()).ready);
    assert_eq!(refs(&clones[0]).lines().count(), 4);
}

#[test]
fn a_worker_does_not_start_with_a_git_that_is_too_old_or_not_there() {
    let f = fx();
    let open = |path: &Path| {
        let mut env = f.env();
        env.insert("PATH".into(), path.display().to_string());
        match Machine::open(&f.state(), env, f.state().join("cluster.key")) {
            Ok(_) => panic!("the worker started"),
            Err(e) => e,
        }
    };
    let bin = f.path("old");
    std::fs::create_dir_all(&bin).unwrap();
    let says = |text: &str| {
        let script = format!("#!/bin/sh\necho '{text}'\n");
        crate::common::write_executable(&bin.join("git"), &script);
    };
    // (2.31 brought `git fetch --atomic`)
    says("git version 2.30.9");
    let e = open(&bin);
    assert_eq!(e.code, ErrorCode::Blocked, "{e:?}");
    assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    assert!(e.message.contains("2.30"), "{}", e.message);
    assert!(e.message.contains("2.31"), "{}", e.message);
    says("something else");
    let e = open(&bin);
    assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    assert!(e.message.contains("something else"), "{}", e.message);
    crate::common::write_executable(&bin.join("git"), "#!/bin/sh\nexit 3\n");
    let e = open(&bin);
    assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    let empty = f.path("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let e = open(&empty);
    assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    assert!(e.message.contains("cannot run git"), "{}", e.message);
    says("git version 2.31.0");
    let env: BTreeMap<String, String> = [("PATH".to_string(), bin.display().to_string())].into();
    assert!(Machine::open(&f.state(), env, f.state().join("cluster.key")).is_ok());
}

#[test]
fn the_manifests_of_dirty_submodules_published_before_are_read_from_the_store() {
    let f = fx();
    let repo = nested_project(&f);
    let first = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_eq!(first.manifests.len(), 3);
    // The next source: one more edit at the top; both submodules stay.
    std::fs::write(repo.join("a.txt"), "edited again\n").unwrap();
    let second = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    assert_eq!(
        second.root.submodules[0].source_id,
        first.root.submodules[0].source_id
    );

    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    send_all(sink, &l, &first);
    assert!(verify(sink, &l, &first.source, first.refs()).ready);
    let clones = f.clones();
    assert_eq!(clones.len(), 1);

    // Of the second source only its own manifest and its own files travel:
    // the captures of the submodules are in the worker's store already.
    put_all(sink, &l, &second.source, Manifest, &second.manifests[0]).unwrap();
    for e in &second.root.object_manifest.entries {
        put_all(sink, &l, &second.source, Content, &second.blobs[&e.hash]).unwrap();
    }
    let staged = f.staged(&second.source);
    let manifests = staged.iter().filter(|n| n.starts_with("manifest-")).count();
    assert_eq!(manifests, 1, "{staged:?}");
    let v = verify(sink, &l, &second.source, vec![]);
    assert!(!v.ready && v.bundle_needed, "{v:?}");
    let thin = f.bundle(&second.root, &v.have[0].commits);
    assert!(!bundle_prerequisites(&thin).unwrap().is_empty());
    assert!(
        put_all(sink, &l, &second.source, Bundle, &thin)
            .unwrap()
            .complete
    );
    assert!(verify(sink, &l, &second.source, vec![]).ready);
    // The bundle was checked against all three manifests: the snapshot of
    // the source is in the clone, and its gitlink is the commit the stored
    // manifest of the submodule names.
    let lib = f
        .worker_store()
        .manifest(second.root.submodules[0].source_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let commit = second.root.execution_commit.clone().unwrap();
    let r = format!("refs/local-ci/snapshots/{}", second.root.source_id);
    assert_eq!(git(&clones[0], &["rev-parse", "--verify", &r]), commit);
    assert_eq!(
        Some(git(
            &clones[0],
            &["rev-parse", &format!("{commit}:vendor/lib")]
        )),
        lib.execution_commit
    );
}

#[test]
fn a_bundle_with_the_snapshot_of_a_submodule_at_another_commit_sets_no_ref() {
    let f = fx();
    let repo = nested_project(&f);
    let first = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    send_all(sink, &l, &first);
    let clones = f.clones();
    assert_eq!(clones.len(), 1);
    let before = git(&clones[0], &["for-each-ref"]);

    // The next source has another capture of the submodule (and of the
    // source itself); the one inside the submodule stays.
    std::fs::write(repo.join("a.txt"), "edited again\n").unwrap();
    std::fs::write(repo.join("vendor/lib/lib.txt"), "edited lib again\n").unwrap();
    let second = f.capture(&SourceRequest::Worktree { dir: repo.clone() });
    let lib = second.root.submodules[0].source_id.clone().unwrap();
    assert_ne!(Some(&lib), first.root.submodules[0].source_id.as_ref());
    send_but_bundle(sink, &l, &second);

    // Its bundle, with the snapshot ref of the submodule moved to another
    // commit than the manifest of the submodule names. Every other ref is
    // as it should be.
    let whole = second.bundle.clone().unwrap();
    let scratch = f.path("forge");
    std::fs::create_dir_all(&scratch).unwrap();
    git(&scratch, &["init", "-q", "-b", "main"]);
    let genuine = f.path("genuine.bundle");
    std::fs::write(&genuine, &whole).unwrap();
    let from = genuine.to_str().unwrap();
    git(&scratch, &["fetch", "-q", from, "refs/*:refs/*"]);
    let lib_ref = format!("refs/local-ci/snapshots/{lib}");
    let own = second.root.execution_commit.clone().unwrap();
    assert_ne!(git(&scratch, &["rev-parse", &lib_ref]), own);
    git(&scratch, &["update-ref", &lib_ref, &own]);
    let forged = f.path("forged.bundle");
    let to = forged.to_str().unwrap();
    git(&scratch, &["bundle", "create", "-q", to, "--all"]);
    let forged = std::fs::read(forged).unwrap();

    let e = put_all(sink, &l, &second.source, Bundle, &forged).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
    assert!(e.message.contains(&lib_ref), "{}", e.message);
    // Not one ref of it is in the clone: not the submodule's, and not the
    // source's own, which the bundle had right.
    assert_eq!(git(&clones[0], &["for-each-ref"]), before);
    assert!(clone_scratch(&f).is_empty());
    assert_eq!(bundles_staged(&f, &second.source), 0);
    put_all(sink, &l, &second.source, Bundle, &whole).unwrap();
    assert!(verify(sink, &l, &second.source, second.refs()).ready);
    assert_ne!(git(&clones[0], &["for-each-ref"]), before);
}

#[test]
fn a_manifest_that_names_a_commit_or_a_tree_by_something_else_is_refused() {
    use local_ci::source::seal_manifest;
    use remote_task_runner::source::classify::source_id_of_ci;
    let f = fx();
    let (_, p) = small_project(&f);
    let m = f.machine();
    let (sink, l) = (m.blobs().unwrap(), limits());
    let not_an_id = || Some("--upload-pack=x".to_string());
    let edits: [fn(&mut CiManifest, Option<String>); 4] = [
        |m, v| m.execution_commit = v,
        |m, v| m.merge_base = v,
        |m, v| m.index_tree = v,
        |m, v| m.worktree_tree = v,
    ];
    for edit in edits {
        let mut forged = p.root.clone();
        edit(&mut forged, not_an_id());
        let forged = seal_manifest(forged);
        let source = source_id_of_ci(&forged.source_id).unwrap();
        let bytes = serde_json::to_vec(&forged).unwrap();
        let e = put_all(sink, &l, &source, Manifest, &bytes).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput, "{e:?}");
        assert!(e.message.contains("not an id"), "{}", e.message);
        assert!(f.staged(&source).is_empty());
    }
}

#[test]
fn an_old_bundle_file_that_cannot_be_removed_does_not_fail_the_export() {
    use remote_task_runner::source::store::MAX_BUNDLES_PER_SOURCE;
    let f = fx();
    let (_, p) = small_project(&f);
    let store = f.parent();
    let ctx = ExecContext::new(f.env());
    // Something under the name of a bundle of the source that is no file,
    // and older than every bundle: the first to go, and it does not go.
    let stuck = store.root().join("bundles").join(format!(
        "{}-{}.bundle",
        p.root.source_id,
        "0".repeat(32)
    ));
    std::fs::create_dir(&stuck).unwrap();
    std::fs::File::open(&stuck)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(1_000_000_000))
        .unwrap();
    for i in 0..MAX_BUNDLES_PER_SOURCE {
        let have = vec![format!("{:040x}", i + 1)];
        let dest = store.export_bundle(&ctx, &p.root, &have).unwrap();
        assert!(dest.is_file());
    }
    assert!(stuck.is_dir());
}
