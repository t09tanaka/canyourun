//! Chapter 22.3 end to end (A43, A47): secret files and `.worktreeinclude`
//! captured from a throwaway repository, held by the parent, sent over an
//! in-process connection, placed by a fake worker, and erased.

mod common;
#[path = "proto_common/mod.rs"]
mod proto;

use local_ci::source::SecretSpec;
use proto::*;
use remote_task_runner::error::{BlockedReason, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::secrets::capture::{capture_run, SecretInputs};
use remote_task_runner::secrets::key::ClusterKey;
use remote_task_runner::secrets::place::WorkerSecrets;
use remote_task_runner::secrets::plan::hex_encode;
use remote_task_runner::secrets::send::send_bundle;
use remote_task_runner::secrets::vault::Vault;
use remote_task_runner::secrets::SecretError;
use remote_task_runner::source::{CaptureParams, SourceRequest, SourceStore};
use remote_task_runner::wire::*;
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const PATH: &str = "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin";
const ENV_TEXT: &str = "API_TOKEN=tok-9f8e7d6c5b\nDB_PASSWORD=\"pa ss-w0rd!\"\nDEBUG=1\n";

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args([
            "-c",
            "user.email=t@example.com",
            "-c",
            "user.name=t",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .env_clear()
        .env("PATH", PATH)
        .env("HOME", dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

struct Fx {
    _tmp: common::Tmp,
    state: PathBuf,
    repo: PathBuf,
    key: ClusterKey,
}

impl Fx {
    fn env(&self) -> BTreeMap<String, String> {
        [("PATH", PATH), ("HOME", self.state.to_str().unwrap())]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
    fn request(&self) -> SourceRequest {
        SourceRequest::Worktree {
            dir: self.repo.clone(),
        }
    }
}

fn fixture(worktreeinclude: Option<&str>) -> Fx {
    let tmp = common::tempdir().unwrap();
    let state = tmp.path().join("state");
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(repo.join("config")).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join(".gitignore"), ".env\n*.local\n").unwrap();
    std::fs::write(repo.join("tracked.txt"), "t\n").unwrap();
    if let Some(w) = worktreeinclude {
        std::fs::write(repo.join(".worktreeinclude"), w).unwrap();
    }
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    std::fs::write(repo.join(".env"), ENV_TEXT).unwrap();
    std::fs::write(repo.join("config/a.local"), "LOCAL=local-value-1\n").unwrap();
    std::fs::write(repo.join("untracked.txt"), "plain untracked\n").unwrap();
    Fx {
        _tmp: tmp,
        state,
        repo,
        key: ClusterKey::from_bytes(vec![5; 32]).unwrap(),
    }
}

fn do_capture(
    fx: &Fx,
    run: &RunId,
    declared: &[SecretSpec],
) -> Result<remote_task_runner::source::Captured, remote_task_runner::error::ApiError> {
    let store = SourceStore::open(&fx.state).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    capture_run(
        &fx.env(),
        &store,
        &vault,
        &SecretInputs {
            key: &fx.key,
            declared,
        },
        run,
        &fx.request(),
        &CaptureParams::default(),
    )
}

fn paths(c: &remote_task_runner::source::Captured) -> Vec<String> {
    c.manifest
        .secret_files
        .iter()
        .map(|s| s.path.display())
        .collect()
}

/// Every file under `dir` that holds `needle`.
fn files_holding(dir: &Path, needle: &[u8]) -> Vec<PathBuf> {
    let mut out = vec![];
    let mut todo = vec![dir.to_path_buf()];
    while let Some(d) = todo.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            let m = std::fs::symlink_metadata(&p).unwrap();
            if m.is_dir() {
                todo.push(p);
            } else if m.is_file() {
                if let Ok(b) = std::fs::read(&p) {
                    if b.windows(needle.len()).any(|w| w == needle) {
                        out.push(p);
                    }
                }
            }
        }
    }
    out
}

#[test]
fn worktreeinclude_picks_only_ignored_untracked_files_and_the_store_never_holds_them() {
    let fx = fixture(Some(".env\nconfig/*.local\nuntracked.txt\ntracked.txt\n"));
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    // the ignored ones only: not the untracked plain file, not the tracked one
    assert_eq!(paths(&got), [".env", "config/a.local"]);
    // the manifest holds path and HMAC, no plain hash and no content
    for s in &got.manifest.secret_files {
        assert!(s.hmac.starts_with("hmac-sha256:"));
    }
    let manifest_json = serde_json::to_string(&got.manifest).unwrap();
    assert!(!manifest_json.contains("tok-9f8e7d6c5b"));
    // plain untracked file is in the ordinary snapshot instead
    assert!(got
        .manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.display() == "untracked.txt"));
    assert!(!got
        .manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.display() == ".env"));
    // the content is nowhere in the source store or the worktree copy; only the run's hold has it
    let holders = files_holding(&fx.state, b"tok-9f8e7d6c5b");
    assert!(holders.is_empty(), "{holders:?}");
    let hex = hex_encode(b"tok-9f8e7d6c5b");
    let held = files_holding(&fx.state, hex.as_bytes());
    assert_eq!(held.len(), 1, "{held:?}");
    assert!(held[0].starts_with(fx.state.join("secrets")));
    assert_eq!(std::fs::metadata(&held[0]).unwrap().mode() & 0o777, 0o600);
    // the original working tree is untouched
    assert_eq!(
        std::fs::read_to_string(fx.repo.join(".env")).unwrap(),
        ENV_TEXT
    );
}

#[test]
fn declared_and_included_files_merge_into_one_entry_and_required_is_honoured() {
    let fx = fixture(Some(".env\n"));
    let declared = [
        SecretSpec {
            glob: ".env".into(),
            required: true,
        },
        SecretSpec {
            glob: "config/*.local".into(),
            required: false,
        },
    ];
    let got = do_capture(&fx, &RunId::generate(), &declared).unwrap();
    assert_eq!(paths(&got), [".env", "config/a.local"]);
    // a required glob that matches nothing fails the capture
    let fx2 = fixture(None);
    let missing = [SecretSpec {
        glob: "nothing.env".into(),
        required: true,
    }];
    assert!(do_capture(&fx2, &RunId::generate(), &missing).is_err());
    // no .worktreeinclude and no declaration: no secrets, and that is fine
    let none = do_capture(&fx2, &RunId::generate(), &[]).unwrap();
    assert!(none.manifest.secret_files.is_empty());
    assert!(none.secrets.is_none());
    // an include that matches nothing is not a failure
    let fx3 = fixture(Some("absent.env\n"));
    assert!(do_capture(&fx3, &RunId::generate(), &[])
        .unwrap()
        .manifest
        .secret_files
        .is_empty());
}

#[test]
fn a_path_with_glob_characters_is_matched_literally() {
    let fx = fixture(Some("odd\\[1\\].env\n"));
    std::fs::write(fx.repo.join(".gitignore"), ".env\n*.local\n*.env\n").unwrap();
    std::fs::write(fx.repo.join("odd[1].env"), "ODD=odd-value-1\n").unwrap();
    std::fs::write(fx.repo.join("odd1.env"), "OTHER=other-value-1\n").unwrap();
    let got = do_capture(&fx, &RunId::generate(), &[]).unwrap();
    assert_eq!(paths(&got), ["odd[1].env"]);
}

#[tokio::test]
async fn a43_the_secret_files_reach_the_worker_as_0600_and_are_gone_after_the_attempt() {
    let fx = fixture(Some(".env\nconfig/*.local\n"));
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    let bundle = vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .unwrap();
    assert_eq!(bundle.files.len(), 2);
    assert!(!format!("{bundle:?}").contains("tok-9f8e7d6c5b"));

    // the worker
    let wtmp = common::tempdir().unwrap();
    let wstate = wtmp.path().join("wstate");
    let ws = wtmp.path().join("attempt-ws");
    std::fs::create_dir_all(&wstate).unwrap();
    std::fs::create_dir_all(&ws).unwrap();
    let secrets = Arc::new(WorkerSecrets::open(&wstate).unwrap());
    let attempt = AttemptId::generate();
    let handler: Handler = {
        let (secrets, key, ws, attempt) = (
            Arc::clone(&secrets),
            fx.key.clone(),
            ws.clone(),
            attempt.clone(),
        );
        Arc::new(move |req| match req {
            Request::SecretBundlePut(r) => {
                Some(match secrets.place(&key, &r.attempt_id, &ws, &r.files) {
                    Ok(n) => Ok(Response::SecretBundlePut(SecretBundlePutResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: attempt.clone(),
                        verified: true,
                        placed: n,
                    })),
                    Err(e) => Err(e.to_api()),
                })
            }
            _ => None,
        })
    };
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, handler);
    let n = send_bundle(
        &conn.client,
        &bundle,
        CoordinatorEpoch(1),
        &attempt,
        Generation(1),
        T,
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    for (p, want) in [
        (".env", ENV_TEXT),
        ("config/a.local", "LOCAL=local-value-1\n"),
    ] {
        let f = ws.join(p);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), want);
        assert_eq!(std::fs::metadata(&f).unwrap().mode() & 0o777, 0o600);
    }
    // a re-send after a reconnect is accepted (the worker restarted before running)
    assert_eq!(
        send_bundle(
            &conn.client,
            &bundle,
            CoordinatorEpoch(1),
            &attempt,
            Generation(1),
            T
        )
        .await
        .unwrap(),
        2
    );
    // nowhere else on the worker: only the attempt workspace holds the value
    let at = b"tok-9f8e7d6c5b";
    assert_eq!(files_holding(&wstate, at), Vec::<PathBuf>::new());
    assert_eq!(files_holding(wtmp.path(), at), vec![ws.join(".env")]);

    // the attempt is terminal: erased; a restart's reconcile finds nothing more
    let rep = secrets.reconcile(|a| *a == attempt).unwrap();
    assert_eq!(rep.erased, vec![attempt.clone()]);
    assert!(!ws.join(".env").exists() && !ws.join("config/a.local").exists());
    assert!(files_holding(wtmp.path(), at).is_empty());

    // the parent deletes its hold when the run is terminal
    assert!(vault.has(&run));
    assert_eq!(vault.sweep(|r| *r == run).unwrap(), vec![run.clone()]);
    assert!(!vault.has(&run));
    assert!(files_holding(&fx.state, at).is_empty());
}

#[test]
fn a_changed_bundle_is_detected_by_its_hmac() {
    let fx = fixture(Some(".env\n"));
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    let file = fx.state.join("secrets").join(format!("{run}.json"));
    let text = std::fs::read_to_string(&file).unwrap();
    let orig = hex_encode(ENV_TEXT.as_bytes());
    let changed = hex_encode(
        ENV_TEXT
            .replace("tok-9f8e7d6c5b", "tok-0000000000")
            .as_bytes(),
    );
    assert!(text.contains(&orig));
    std::fs::write(&file, text.replace(&orig, &changed)).unwrap();
    let err = vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .unwrap_err();
    assert!(
        matches!(err, SecretError::Tampered { ref path } if path == ".env"),
        "{err}"
    );
    assert!(!err.to_string().contains("tok-"));
    // another cluster key does not verify either
    std::fs::write(&file, &text).unwrap();
    let other = ClusterKey::from_bytes(vec![6; 32]).unwrap();
    assert!(matches!(
        vault.load(&run, &got.manifest.secret_files, &other),
        Err(SecretError::Tampered { .. })
    ));
    assert!(vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .is_ok());
    // a hold that others can read is refused
    std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    assert!(matches!(
        vault.load(&run, &got.manifest.secret_files, &fx.key),
        Err(SecretError::Insecure { .. })
    ));
}

#[test]
fn a_run_that_lost_its_bundle_is_blocked_not_resumed_with_something_else() {
    let fx = fixture(Some(".env\n"));
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    vault.erase(&run).unwrap();
    // the file in the working tree is still there, and is not used
    assert!(fx.repo.join(".env").exists());
    let err = vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .unwrap_err();
    let api = err.to_api();
    assert_eq!(api.code, ErrorCode::Blocked);
    assert_eq!(api.blocked_reason, Some(BlockedReason::SecretUnavailable));
    // a retry of the capture binds to the same source and does not re-read the tree
    std::fs::write(fx.repo.join(".env"), "API_TOKEN=changed-later\n").unwrap();
    let again = do_capture(&fx, &run, &[]).unwrap();
    assert!(again.secrets.is_none());
    assert!(!vault.has(&run));
}

#[test]
fn two_runs_of_the_same_source_hold_their_own_bundles() {
    let fx = fixture(Some(".env\n"));
    let (r1, r2) = (RunId::generate(), RunId::generate());
    let a = do_capture(&fx, &r1, &[]).unwrap();
    let b = do_capture(&fx, &r2, &[]).unwrap();
    assert_eq!(a.manifest.manifest_digest, b.manifest.manifest_digest);
    let vault = Vault::open(&fx.state).unwrap();
    vault.erase(&r1).unwrap();
    assert!(vault.load(&r2, &b.manifest.secret_files, &fx.key).is_ok());
    assert!(vault.load(&r1, &a.manifest.secret_files, &fx.key).is_err());
}

#[test]
fn no_secrets_means_an_empty_bundle_without_a_hold() {
    let fx = fixture(None);
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    assert!(vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .unwrap()
        .is_empty());
}
