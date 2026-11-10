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
        let (secrets, key, ws, attempt, entries) = (
            Arc::clone(&secrets),
            fx.key.clone(),
            ws.clone(),
            attempt.clone(),
            got.manifest.secret_files.clone(),
        );
        Arc::new(move |req| match req {
            Request::SecretBundlePut(r) => Some(
                match secrets.place(&key, &r.attempt_id, &ws, &r.files, &entries) {
                    Ok(n) => Ok(Response::SecretBundlePut(SecretBundlePutResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: attempt.clone(),
                        verified: true,
                        placed: n,
                    })),
                    Err(e) => Err(e.to_api()),
                },
            ),
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
    // what is in place is exactly what the manifest names
    secrets
        .ensure_placed(&attempt, &got.manifest.secret_files)
        .unwrap();
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

// ---------------------------------------------------------------------------
// the stopgaps and the hardening of S8's review (the L1c work lifts the first two)
// ---------------------------------------------------------------------------

const TOK: &[u8] = b"tok-9f8e7d6c5b";

fn plain_env(state: &Path) -> BTreeMap<String, String> {
    [("PATH", PATH), ("HOME", state.to_str().unwrap())]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn spec(glob: &str) -> SecretSpec {
    SecretSpec {
        glob: glob.into(),
        required: false,
    }
}

fn capture_with(
    fx: &Fx,
    run: &RunId,
    declared: &[SecretSpec],
    request: &SourceRequest,
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
        request,
        &CaptureParams::default(),
    )
}

/// Nothing under `dir` holds the token, in plain form or as the vault's hex.
fn assert_no_plaintext(dir: &Path) {
    assert!(files_holding(dir, TOK).is_empty());
    assert!(files_holding(dir, hex_encode(TOK).as_bytes()).is_empty());
}

#[test]
fn a_directory_capture_with_secret_files_is_refused_and_no_plaintext_reaches_the_snapshot() {
    let tmp = common::tempdir().unwrap();
    let state = tmp.path().join("state");
    let dir = tmp.path().join("plain");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(".env"), ENV_TEXT).unwrap();
    std::fs::write(dir.join("code.txt"), "code\n").unwrap();
    let store = SourceStore::open(&state).unwrap();
    let vault = Vault::open(&state).unwrap();
    let key = ClusterKey::from_bytes(vec![5; 32]).unwrap();
    let req = SourceRequest::Directory { dir: dir.clone() };
    let run = |declared: &[SecretSpec]| {
        capture_run(
            &plain_env(&state),
            &store,
            &vault,
            &SecretInputs {
                key: &key,
                declared,
            },
            &RunId::generate(),
            &req,
            &CaptureParams::default(),
        )
    };
    // declared
    let err = run(&[spec(".env")]).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("not supported"), "{}", err.message);
    // a .worktreeinclude with a pattern
    std::fs::write(dir.join(".worktreeinclude"), "# comment\n.env\n").unwrap();
    assert_eq!(run(&[]).unwrap_err().code, ErrorCode::InvalidInput);
    assert_no_plaintext(&state);
    // a .worktreeinclude with nothing in it names no file
    std::fs::write(dir.join(".worktreeinclude"), "# nothing\n\n").unwrap();
    let ok = run(&[]).unwrap();
    assert!(ok.manifest.secret_files.is_empty());
    // no declaration at all is the ordinary directory capture
    std::fs::remove_file(dir.join(".worktreeinclude")).unwrap();
    assert!(run(&[]).is_ok());
}

#[test]
fn a_commit_capture_with_secret_files_is_refused_not_sent_without_them() {
    let fx = fixture(Some(".env\n"));
    let commit = SourceRequest::Commit {
        dir: fx.repo.clone(),
        rev: "HEAD".into(),
    };
    // .worktreeinclude matches an ignored file in the original worktree
    let err = capture_with(&fx, &RunId::generate(), &[], &commit).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("not supported"), "{}", err.message);
    // declared
    let fx2 = fixture(None);
    let commit2 = SourceRequest::Commit {
        dir: fx2.repo.clone(),
        rev: "HEAD".into(),
    };
    let err = capture_with(&fx2, &RunId::generate(), &[spec(".env")], &commit2).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    // neither: an ordinary commit capture
    let ok = capture_with(&fx2, &RunId::generate(), &[], &commit2).unwrap();
    assert!(ok.manifest.secret_files.is_empty());
    assert_no_plaintext(&fx.state);
    assert_no_plaintext(&fx2.state);
}

#[test]
fn a_bundle_the_run_could_not_adopt_is_adopted_by_the_next_call_after_its_hmac_is_checked() {
    let fx = fixture(Some(".env\n"));
    let store = SourceStore::open(&fx.state).unwrap();
    let params = CaptureParams {
        secret_files: vec![spec(".env")],
        secret_key: fx.key.as_bytes().to_vec(),
        ..CaptureParams::default()
    };
    // a capture that was bound to its run and then lost the process before the adoption
    let run = RunId::generate();
    let left = remote_task_runner::source::capture_for_run(
        &fx.env(),
        &store,
        &run,
        &fx.request(),
        &params,
    )
    .unwrap();
    let copy = left.secrets.clone().unwrap();
    assert!(copy.exists());
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    assert!(vault.has(&run));
    assert!(!copy.exists(), "the store's plaintext copy is gone");
    assert_eq!(
        vault
            .load(&run, &got.manifest.secret_files, &fx.key)
            .unwrap()
            .files
            .len(),
        1
    );
    assert_no_plaintext(&fx.state.join("sources"));

    // the same, but the copy was changed: refused, and nothing stays anywhere
    let run2 = RunId::generate();
    let left = remote_task_runner::source::capture_for_run(
        &fx.env(),
        &store,
        &run2,
        &fx.request(),
        &params,
    )
    .unwrap();
    let copy = left.secrets.unwrap();
    let text = std::fs::read_to_string(&copy).unwrap();
    let orig = hex_encode(ENV_TEXT.as_bytes());
    let changed = hex_encode(ENV_TEXT.replace("tok-9f8e", "tok-0000").as_bytes());
    std::fs::write(&copy, text.replace(&orig, &changed)).unwrap();
    let err = do_capture(&fx, &run2, &[]).unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal, "{}", err.message);
    assert!(!copy.exists());
    assert!(!vault.has(&run2));
    assert_no_plaintext(&fx.state.join("sources"));
}

#[test]
fn an_adoption_that_fails_leaves_no_plaintext_in_the_store() {
    use std::os::unix::fs::PermissionsExt;
    let fx = fixture(Some(".env\n"));
    let store = SourceStore::open(&fx.state).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    // the vault directory is not writable: the adoption fails
    let dir = fx.state.join("secrets");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    let run = RunId::generate();
    let r = capture_run(
        &fx.env(),
        &store,
        &vault,
        &SecretInputs {
            key: &fx.key,
            declared: &[],
        },
        &run,
        &fx.request(),
        &CaptureParams::default(),
    );
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(r.is_err());
    assert_no_plaintext(&fx.state);
}

#[test]
fn the_sweep_removes_the_leftover_bundles_nobody_will_adopt() {
    let fx = fixture(Some(".env\n"));
    let store = SourceStore::open(&fx.state).unwrap();
    let params = CaptureParams {
        secret_files: vec![spec(".env")],
        secret_key: fx.key.as_bytes().to_vec(),
        ..CaptureParams::default()
    };
    let left = remote_task_runner::source::capture_for_run(
        &fx.env(),
        &store,
        &RunId::generate(),
        &fx.request(),
        &params,
    )
    .unwrap();
    let copy = left.secrets.unwrap();
    let id = left.manifest.ci_source_id.clone();
    // a run still waits for it: kept
    assert_eq!(
        remote_task_runner::secrets::capture::sweep_store_copies(&store, |s| s == id).unwrap(),
        0
    );
    assert!(copy.exists());
    // nobody does: removed, and a second sweep has nothing to do
    assert_eq!(
        remote_task_runner::secrets::capture::sweep_store_copies(&store, |_| false).unwrap(),
        1
    );
    assert!(!copy.exists());
    assert_eq!(
        remote_task_runner::secrets::capture::sweep_store_copies(&store, |_| false).unwrap(),
        0
    );
    assert_no_plaintext(&fx.state.join("sources"));
}

#[test]
fn runs_of_one_source_that_capture_at_the_same_time_all_get_their_bundle() {
    let fx = fixture(Some(".env\nconfig/*.local\n"));
    let runs: Vec<RunId> = (0..6).map(|_| RunId::generate()).collect();
    let results: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = runs
            .iter()
            .map(|r| {
                let fx = &fx;
                s.spawn(move || do_capture(fx, r, &[]).unwrap())
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let vault = Vault::open(&fx.state).unwrap();
    for (run, got) in runs.iter().zip(&results) {
        let b = vault
            .load(run, &got.manifest.secret_files, &fx.key)
            .unwrap_or_else(|e| panic!("{run}: {e}"));
        assert_eq!(b.files.len(), 2);
    }
    assert_no_plaintext(&fx.state.join("sources"));
}

#[test]
fn a_capture_started_in_a_subdirectory_reads_the_root_worktreeinclude() {
    let fx = fixture(Some(".env\nconfig/*.local\n"));
    let sub = SourceRequest::Worktree {
        dir: fx.repo.join("config"),
    };
    let got = capture_with(&fx, &RunId::generate(), &[], &sub).unwrap();
    assert_eq!(paths(&got), [".env", "config/a.local"]);
    // the same as from the root
    let root = capture_with(&fx, &RunId::generate(), &[], &fx.request()).unwrap();
    assert_eq!(paths(&root), paths(&got));
}

#[test]
fn a_file_that_cannot_be_sent_fails_before_the_capture_with_where_it_came_from() {
    // a link standing in a declared glob
    let fx = fixture(None);
    std::os::unix::fs::symlink(fx.repo.join(".env"), fx.repo.join("link.local")).unwrap();
    let err = capture_with(&fx, &RunId::generate(), &[spec("*.local")], &fx.request()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(
        err.message.contains("[source] secret_files"),
        "{}",
        err.message
    );
    assert!(err.message.contains("*.local"), "{}", err.message);
    assert!(err.message.contains("link.local"), "{}", err.message);
    assert!(!fx.state.join("sources/secrets").exists() || files_holding(&fx.state, TOK).is_empty());
    // a link named by .worktreeinclude
    let fx = fixture(Some("link.local\n"));
    std::os::unix::fs::symlink(fx.repo.join(".env"), fx.repo.join("link.local")).unwrap();
    let err = capture_with(&fx, &RunId::generate(), &[], &fx.request()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains(".worktreeinclude"), "{}", err.message);
    assert!(err.message.contains("link.local"), "{}", err.message);
    // a file that is too large
    let fx = fixture(None);
    std::fs::write(fx.repo.join("big.local"), vec![b'x'; 512 * 1024 + 1]).unwrap();
    let err =
        capture_with(&fx, &RunId::generate(), &[spec("big.local")], &fx.request()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(
        err.message.contains("big.local") && err.message.contains("at most"),
        "{}",
        err.message
    );
    // exactly the limit is fine
    std::fs::write(fx.repo.join("big.local"), vec![b'x'; 512 * 1024]).unwrap();
    assert!(capture_with(&fx, &RunId::generate(), &[spec("big.local")], &fx.request()).is_ok());
    // too many files for one glob, and too many globs
    let fx = fixture(None);
    for i in 0..257 {
        std::fs::write(fx.repo.join(format!("n{i}.local")), "A=aaaaaaaa\n").unwrap();
    }
    let err =
        capture_with(&fx, &RunId::generate(), &[spec("n*.local")], &fx.request()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("257"), "{}", err.message);
    let many: Vec<SecretSpec> = (0..65).map(|i| spec(&format!("g{i}.env"))).collect();
    let err = capture_with(&fx, &RunId::generate(), &many, &fx.request()).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("65"), "{}", err.message);
}

#[test]
fn a_bundle_of_another_run_is_not_placed_for_this_attempt() {
    let a = fixture(Some(".env\n"));
    let got_a = do_capture(&a, &RunId::generate(), &[]).unwrap();
    // another run: the same path with another content, and a path A's manifest lacks
    let b = fixture(Some(".env\nconfig/*.local\n"));
    std::fs::write(b.repo.join(".env"), "API_TOKEN=other-run-value-1\n").unwrap();
    let run_b = RunId::generate();
    let got_b = do_capture(&b, &run_b, &[]).unwrap();
    let bundle_b = Vault::open(&b.state)
        .unwrap()
        .load(&run_b, &got_b.manifest.secret_files, &b.key)
        .unwrap();
    assert_eq!(bundle_b.files.len(), 2);
    let wtmp = common::tempdir().unwrap();
    let ws = wtmp.path().join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    let secrets = WorkerSecrets::open(wtmp.path()).unwrap();
    let attempt = AttemptId::generate();
    // the file A's manifest names, with B's content (same key, same path, other HMAC)
    let same_path = bundle_b.files.iter().find(|f| f.path == ".env").unwrap();
    let err = secrets
        .place(
            &a.key,
            &attempt,
            &ws,
            std::slice::from_ref(same_path),
            &got_a.manifest.secret_files,
        )
        .unwrap_err();
    assert!(matches!(err, SecretError::Tampered { .. }), "{err}");
    // a file A's manifest does not name
    let extra = bundle_b
        .files
        .iter()
        .find(|f| f.path == "config/a.local")
        .unwrap();
    let err = secrets
        .place(
            &a.key,
            &attempt,
            &ws,
            std::slice::from_ref(extra),
            &got_a.manifest.secret_files,
        )
        .unwrap_err();
    assert!(matches!(err, SecretError::Placement(_)), "{err}");
    assert_eq!(std::fs::read_dir(&ws).unwrap().count(), 0);
    // the whole of B's bundle, against B's own manifest, is fine
    secrets
        .place(
            &b.key,
            &attempt,
            &ws,
            &bundle_b.files,
            &got_b.manifest.secret_files,
        )
        .unwrap();
    secrets
        .ensure_placed(&attempt, &got_b.manifest.secret_files)
        .unwrap();
    // but this attempt's manifest is A's: what is in place is not what it names
    assert!(matches!(
        secrets.ensure_placed(&attempt, &got_a.manifest.secret_files),
        Err(SecretError::PathSetMismatch)
    ));
    assert!(matches!(
        secrets.ensure_placed(&attempt, &[]),
        Err(SecretError::PathSetMismatch)
    ));
    // a file that went missing, and one replaced by a link
    let (first, rest) = got_b.manifest.secret_files.split_first().unwrap();
    assert!(secrets.ensure_placed(&attempt, rest).is_err());
    std::fs::remove_file(ws.join(first.path.display())).unwrap();
    assert!(matches!(
        secrets.ensure_placed(&attempt, &got_b.manifest.secret_files),
        Err(SecretError::Placement(_))
    ));
    std::os::unix::fs::symlink(wtmp.path(), ws.join(first.path.display())).unwrap();
    assert!(matches!(
        secrets.ensure_placed(&attempt, &got_b.manifest.secret_files),
        Err(SecretError::Placement(_))
    ));
    // an attempt that needs none and has none
    secrets.ensure_placed(&AttemptId::generate(), &[]).unwrap();
}

#[test]
fn no_secret_type_shows_a_value_or_the_key_in_debug() {
    use remote_task_runner::secrets::envfile::secret_values;
    use remote_task_runner::secrets::mask::Masker;
    let fx = fixture(Some(".env\n"));
    let run = RunId::generate();
    let got = do_capture(&fx, &run, &[]).unwrap();
    let vault = Vault::open(&fx.state).unwrap();
    let bundle = vault
        .load(&run, &got.manifest.secret_files, &fx.key)
        .unwrap();
    let values = bundle.mask_values();
    let masker = Masker::new(values.values.clone());
    let key_hex = hex_encode(fx.key.as_bytes());
    let req = Request::SecretBundlePut(SecretBundlePutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        attempt_id: AttemptId::generate(),
        generation: Generation(1),
        files: bundle.files.clone(),
    });
    let params = CaptureParams {
        secret_files: vec![spec(".env")],
        secret_key: fx.key.as_bytes().to_vec(),
        ..CaptureParams::default()
    };
    let shown = [
        format!("{:?}", fx.key),
        format!("{bundle:?}"),
        format!("{:?}", bundle.files[0]),
        format!("{req:?}"),
        format!("{params:?}"),
        format!("{values:?}"),
        format!("{:?}", secret_values(ENV_TEXT.as_bytes())),
        format!("{masker:?}"),
        format!("{:?}", masker.stream()),
        format!("{:?}", got),
    ];
    let b64 = base64_of(ENV_TEXT.as_bytes());
    for text in &shown {
        for needle in [
            "tok-9f8e7d6c5b",
            "pa ss-w0rd!",
            "API_TOKEN=",
            key_hex.as_str(),
            &hex_encode(TOK),
            &hex_encode(ENV_TEXT.as_bytes()),
            &b64,
        ] {
            assert!(!text.contains(needle), "{needle} in {text}");
        }
        assert!(
            !text.contains(&format!("{:?}", fx.key.as_bytes())),
            "{text}"
        );
    }
    // a stream masker that holds bytes does not show them either
    let mut st = masker.stream();
    let _ = st.push(b"x tok-9f8e7d6c");
    assert!(st.pending() > 0);
    let held = format!("{st:?}");
    assert!(!held.contains("tok-9f8e"), "{held}");
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn stale_temporary_files_are_swept_with_the_vault_and_the_worker_index() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = common::tempdir().unwrap();
    let vault = Vault::open(tmp.path()).unwrap();
    let ws = WorkerSecrets::open(tmp.path()).unwrap();
    let long_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
    for dir in [tmp.path().join("secrets"), tmp.path().join("secret-index")] {
        let old = dir.join(".abc.01OLD.tmp");
        let fresh = dir.join(".abc.01FRESH.tmp");
        let other = dir.join("not-a-temp-file.txt");
        for p in [&old, &fresh, &other] {
            std::fs::write(p, b"plaintext").unwrap();
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        for p in [&old, &other] {
            std::fs::File::options()
                .write(true)
                .open(p)
                .unwrap()
                .set_modified(long_ago)
                .unwrap();
        }
    }
    vault.sweep(|_| false).unwrap();
    let rep = ws.reconcile(|_| true).unwrap();
    assert_eq!(rep.stale_tmp_removed, 1);
    for dir in [tmp.path().join("secrets"), tmp.path().join("secret-index")] {
        assert!(!dir.join(".abc.01OLD.tmp").exists());
        assert!(
            dir.join(".abc.01FRESH.tmp").exists(),
            "a fresh one may be in use"
        );
        assert!(dir.join("not-a-temp-file.txt").exists());
    }
}
