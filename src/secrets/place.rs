//! Secret files on the worker (spec 22.3, "workerでの配置" and "消去").
//!
//! Placement checks everything before it writes anything: the limits, every
//! path, and every file's HMAC under the cluster key. Files are then written
//! into the attempt workspace as 0600, walking the path one component at a
//! time and refusing a symlink (or anything that is not a directory) on the
//! way; the file itself is created with `create_new`, which never follows a
//! link. Nothing is written outside the workspace, and nothing is kept
//! anywhere else: not in the object store, a cache, a template, a log or an
//! artifact.
//!
//! Which files were placed is recorded first, in `<state>/secret-index/
//! <attempt>.json` (0600; paths only, never a content), so that the erase
//! after the attempt and the reconcile at worker start know what to remove
//! even after a crash in between. Erasing runs after the attempt's own
//! commands have run in that workspace, so it does not trust the tree: it
//! removes the file itself (a symlink standing in for it is removed, never its
//! target) and gives up on a path whose parent is no longer a plain directory.

use super::key::ClusterKey;
use super::plan::{check_limits, check_paths, ct_eq, to_wire_digest, Limit, MAX_FILES};
use super::vault::{read_private, write_private};
use super::{Result, SecretError};
use crate::ids::AttemptId;
use crate::wire::SecretFile;
use local_ci::source::{secret_digest, RelPath};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Record {
    attempt_id: String,
    workspace: String,
    paths: Vec<String>,
}

/// What an erase did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EraseReport {
    pub removed: Vec<String>,
    /// Already absent.
    pub gone: Vec<String>,
    /// A parent is no longer a plain directory (or the path is a directory):
    /// nothing was touched there.
    pub unreachable: Vec<String>,
}

/// What a reconcile pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub erased: Vec<AttemptId>,
    pub kept: Vec<AttemptId>,
    /// Records that could not be handled: the file name and why (no values).
    pub failed: Vec<(String, String)>,
}

pub struct WorkerSecrets {
    index_dir: PathBuf,
}

fn placement(msg: impl Into<String>) -> SecretError {
    SecretError::Placement(msg.into())
}

enum Step {
    Removed,
    Gone,
    Unreachable,
}

impl WorkerSecrets {
    /// `<state_dir>/secret-index`, made 0700 when missing; a link or a
    /// directory others can read is refused.
    pub fn open(state_dir: &Path) -> Result<Self> {
        let dir = state_dir.join("secret-index");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        let meta = fs::symlink_metadata(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        if !meta.file_type().is_dir() || meta.mode() & 0o077 != 0 {
            return Err(SecretError::Insecure {
                what: "secret index directory",
                path: dir,
            });
        }
        Ok(WorkerSecrets { index_dir: dir })
    }

    fn record_path(&self, attempt: &AttemptId) -> PathBuf {
        self.index_dir.join(format!("{attempt}.json"))
    }

    fn load_record(&self, attempt: &AttemptId) -> Result<Option<Record>> {
        match read_private(&self.record_path(attempt), "secret index") {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| SecretError::Format("the secret index is not readable".into())),
            Err(SecretError::Unavailable { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn save_record(&self, attempt: &AttemptId, rec: &Record) -> Result<()> {
        let bytes = serde_json::to_vec(rec)
            .map_err(|_| SecretError::Format("cannot write the secret index".into()))?;
        write_private(&self.record_path(attempt), &bytes)
    }

    /// Place `files` (one request's worth; more may follow for the same
    /// attempt) in `workspace`. Returns how many are in place. A failure
    /// leaves none of this call's new files behind.
    pub fn place(
        &self,
        key: &ClusterKey,
        attempt: &AttemptId,
        workspace: &Path,
        files: &[SecretFile],
    ) -> Result<u32> {
        // 1. everything is checked before anything is written
        let sizes: Vec<(usize, usize)> = files
            .iter()
            .map(|f| (f.path.len(), f.content.0.len()))
            .collect();
        check_limits(&sizes).map_err(SecretError::Limit)?;
        check_paths(files.iter().map(|f| f.path.as_str())).map_err(SecretError::BadPath)?;
        for f in files {
            let want = secret_digest(
                key.as_bytes(),
                &RelPath(f.path.clone().into_bytes()),
                &f.content.0,
            );
            let want = to_wire_digest(&want)
                .ok_or_else(|| SecretError::Format("unexpected digest form".into()))?;
            if !ct_eq(want.as_str().as_bytes(), f.hmac_digest.as_str().as_bytes()) {
                return Err(SecretError::Tampered {
                    path: f.path.clone(),
                });
            }
        }
        // 2. the workspace is a real directory
        let meta = fs::symlink_metadata(workspace)
            .map_err(|e| SecretError::io(format!("workspace {}", workspace.display()), e))?;
        if !meta.file_type().is_dir() {
            return Err(placement("the workspace is not a plain directory"));
        }
        let root = fs::canonicalize(workspace)
            .map_err(|e| SecretError::io(format!("workspace {}", workspace.display()), e))?;
        let root_text = root
            .to_str()
            .ok_or_else(|| placement("the workspace path is not valid UTF-8"))?
            .to_string();
        // 3. the record, before the first file
        let mut rec = self.load_record(attempt)?.unwrap_or(Record {
            attempt_id: attempt.to_string(),
            workspace: root_text.clone(),
            paths: vec![],
        });
        if rec.workspace != root_text {
            return Err(placement(
                "this attempt already has secret files in another workspace",
            ));
        }
        let prior: BTreeSet<String> = rec.paths.iter().cloned().collect();
        let mut all = prior.clone();
        all.extend(files.iter().map(|f| f.path.clone()));
        if all.len() > MAX_FILES {
            return Err(SecretError::Limit(Limit::TooManyFiles { count: all.len() }));
        }
        rec.paths = all.into_iter().collect();
        self.save_record(attempt, &rec)?;
        // 4. place; undo this call's files on failure
        let mut created: Vec<&str> = Vec::new();
        for f in files {
            match place_one(&root, &f.path, &f.content.0, prior.contains(&f.path)) {
                Ok(true) => created.push(&f.path),
                Ok(false) => {}
                Err(e) => {
                    for p in created {
                        let _ = erase_one(&root, p);
                    }
                    return Err(e);
                }
            }
        }
        Ok(files.len() as u32)
    }

    /// Remove the files placed for `attempt` and forget them. Idempotent.
    pub fn erase(&self, attempt: &AttemptId) -> Result<EraseReport> {
        let mut report = EraseReport::default();
        let Some(rec) = self.load_record(attempt)? else {
            return Ok(report);
        };
        let root = PathBuf::from(&rec.workspace);
        let mut first_error: Option<SecretError> = None;
        for p in &rec.paths {
            match erase_one(&root, p) {
                Ok(Step::Removed) => report.removed.push(p.clone()),
                Ok(Step::Gone) => report.gone.push(p.clone()),
                Ok(Step::Unreachable) => report.unreachable.push(p.clone()),
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_error {
            // the record stays, so a later pass tries again
            return Err(e);
        }
        match fs::remove_file(self.record_path(attempt)) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                Err(SecretError::io("secret index".to_string(), e))
            }
            _ => Ok(report),
        }
    }

    /// At worker start: erase the secret files of every attempt `should_erase`
    /// says is terminal (or unknown). Attempts that are still to run keep
    /// theirs. A pass never stops at the first failure.
    pub fn reconcile(&self, should_erase: impl Fn(&AttemptId) -> bool) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let rd = fs::read_dir(&self.index_dir)
            .map_err(|e| SecretError::io(format!("directory {}", self.index_dir.display()), e))?;
        let mut names: Vec<String> = Vec::new();
        for entry in rd {
            let entry = entry.map_err(|e| {
                SecretError::io(format!("directory {}", self.index_dir.display()), e)
            })?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        for name in names {
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(attempt) = AttemptId::parse(stem) else {
                report
                    .failed
                    .push((name.clone(), "not an attempt's record".into()));
                continue;
            };
            if !should_erase(&attempt) {
                report.kept.push(attempt);
                continue;
            }
            match self.erase(&attempt) {
                Ok(_) => report.erased.push(attempt),
                Err(e) => report.failed.push((name.clone(), e.to_string())),
            }
        }
        Ok(report)
    }
}

/// The directories above the last component of `rel`, created when missing,
/// each checked not to be a link; then the file.
fn place_one(root: &Path, rel: &str, content: &[u8], ours_already: bool) -> Result<bool> {
    let comps: Vec<&str> = rel.split('/').collect();
    let (last, parents) = comps
        .split_last()
        .ok_or_else(|| placement("empty secret file path"))?;
    let mut cur = root.to_path_buf();
    for c in parents {
        cur.push(c);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.file_type().is_dir() => {}
            Ok(_) => {
                return Err(placement(format!(
                    "{rel}: a parent directory is a link or not a directory"
                )))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                fs::create_dir(&cur)
                    .map_err(|e| SecretError::io(format!("directory for {rel}"), e))?;
                let m = fs::symlink_metadata(&cur)
                    .map_err(|e| SecretError::io(format!("directory for {rel}"), e))?;
                if !m.file_type().is_dir() {
                    return Err(placement(format!(
                        "{rel}: a parent directory was replaced while it was made"
                    )));
                }
            }
            Err(e) => return Err(SecretError::io(format!("directory for {rel}"), e)),
        }
    }
    let target = cur.join(last);
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&target)
    {
        Ok(mut f) => {
            let r = f
                .write_all(content)
                .and_then(|()| {
                    f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
                })
                .and_then(|()| f.sync_all());
            drop(f);
            let inside = fs::canonicalize(&cur).is_ok_and(|p| p.starts_with(root));
            if let Err(e) = r {
                let _ = fs::remove_file(&target);
                return Err(SecretError::io(format!("secret file {rel}"), e));
            }
            if !inside {
                let _ = fs::remove_file(&target);
                return Err(placement(format!("{rel}: the parent left the workspace")));
            }
            Ok(true)
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            // a re-sent request after a restart: the same file, by us, is fine
            let meta = fs::symlink_metadata(&target)
                .map_err(|e| SecretError::io(format!("secret file {rel}"), e))?;
            let same = ours_already
                && meta.file_type().is_file()
                && meta.mode() & 0o777 == 0o600
                && fs::read(&target).is_ok_and(|b| b == content);
            if same {
                Ok(false)
            } else {
                Err(placement(format!(
                    "{rel} already exists in the workspace; it is not overwritten"
                )))
            }
        }
        Err(e) => Err(SecretError::io(format!("secret file {rel}"), e)),
    }
}

/// Remove `rel` under `root` without following a link on the way.
fn erase_one(root: &Path, rel: &str) -> Result<Step> {
    match fs::symlink_metadata(root) {
        Ok(m) if m.file_type().is_dir() => {}
        Ok(_) => return Ok(Step::Unreachable),
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Step::Gone),
        Err(e) => return Err(SecretError::io(format!("workspace for {rel}"), e)),
    }
    let comps: Vec<&str> = rel.split('/').collect();
    let (last, parents) = comps
        .split_last()
        .ok_or_else(|| placement("empty secret file path"))?;
    let mut cur = root.to_path_buf();
    for c in parents {
        cur.push(c);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.file_type().is_dir() => {}
            Ok(_) => return Ok(Step::Unreachable),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Step::Gone),
            Err(e) => return Err(SecretError::io(format!("directory of {rel}"), e)),
        }
    }
    let target = cur.join(last);
    match fs::symlink_metadata(&target) {
        Ok(m) if m.file_type().is_dir() => Ok(Step::Unreachable),
        Ok(_) => match fs::remove_file(&target) {
            Ok(()) => Ok(Step::Removed),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Step::Gone),
            Err(e) => Err(SecretError::io(format!("secret file {rel}"), e)),
        },
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Step::Gone),
        Err(e) => Err(SecretError::io(format!("secret file {rel}"), e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Bytes;
    use std::os::unix::fs::{symlink, PermissionsExt};

    struct Env {
        _tmp: tempfile::TempDir,
        state: PathBuf,
        ws: PathBuf,
        outside: PathBuf,
        key: ClusterKey,
        ws_secrets: WorkerSecrets,
    }

    fn env() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let ws = tmp.path().join("ws");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&ws).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let ws_secrets = WorkerSecrets::open(&state).unwrap();
        Env {
            _tmp: tmp,
            state,
            ws,
            outside,
            key: ClusterKey::from_bytes(vec![7; 32]).unwrap(),
            ws_secrets,
        }
    }

    fn file(key: &ClusterKey, path: &str, content: &str) -> SecretFile {
        let d = secret_digest(
            key.as_bytes(),
            &RelPath(path.as_bytes().to_vec()),
            content.as_bytes(),
        );
        SecretFile {
            path: path.to_string(),
            hmac_digest: to_wire_digest(&d).unwrap(),
            content: Bytes(content.as_bytes().to_vec()),
        }
    }

    fn mode(p: &Path) -> u32 {
        fs::metadata(p).unwrap().mode() & 0o777
    }

    #[test]
    fn files_are_placed_as_0600_with_their_content_and_parents() {
        let e = env();
        let a = AttemptId::generate();
        let files = [
            file(&e.key, ".env", "TOKEN=abc-123456\n"),
            file(&e.key, "pkg/a/.env.test", "X=yyyy-9999\n"),
        ];
        assert_eq!(e.ws_secrets.place(&e.key, &a, &e.ws, &files).unwrap(), 2);
        assert_eq!(
            fs::read_to_string(e.ws.join(".env")).unwrap(),
            "TOKEN=abc-123456\n"
        );
        assert_eq!(mode(&e.ws.join(".env")), 0o600);
        assert_eq!(mode(&e.ws.join("pkg/a/.env.test")), 0o600);
        // the index lists paths, holds no content, and is private
        let idx = e.state.join("secret-index").join(format!("{a}.json"));
        assert_eq!(mode(&idx), 0o600);
        let text = fs::read_to_string(&idx).unwrap();
        assert!(text.contains(".env.test"));
        assert!(!text.contains("abc-123456") && !text.contains("yyyy-9999"));
        assert_eq!(mode(&e.state.join("secret-index")), 0o700);
    }

    #[test]
    fn a_wrong_digest_writes_nothing_at_all() {
        let e = env();
        let a = AttemptId::generate();
        let mut bad = file(&e.key, "b.env", "B=bbbb-2222");
        bad.content = Bytes(b"B=changed-22".to_vec());
        let files = [file(&e.key, "a.env", "A=aaaa-1111"), bad];
        let err = e.ws_secrets.place(&e.key, &a, &e.ws, &files).unwrap_err();
        assert!(matches!(err, SecretError::Tampered { ref path } if path == "b.env"));
        assert!(!err.to_string().contains("changed"));
        assert_eq!(fs::read_dir(&e.ws).unwrap().count(), 0);
        assert_eq!(
            fs::read_dir(e.state.join("secret-index")).unwrap().count(),
            0
        );
        // another cluster's key is the same refusal
        let other = ClusterKey::from_bytes(vec![9; 32]).unwrap();
        let good = [file(&e.key, "a.env", "A=aaaa-1111")];
        assert!(matches!(
            e.ws_secrets.place(&other, &a, &e.ws, &good),
            Err(SecretError::Tampered { .. })
        ));
        // a path is part of the digest: the same content under another path
        let mut moved = file(&e.key, "a.env", "A=aaaa-1111");
        moved.path = "c.env".into();
        assert!(matches!(
            e.ws_secrets.place(&e.key, &a, &e.ws, &[moved]),
            Err(SecretError::Tampered { .. })
        ));
    }

    #[test]
    fn paths_that_leave_the_workspace_or_repeat_are_refused_before_writing() {
        let e = env();
        let a = AttemptId::generate();
        for bad in [
            "../x.env",
            "/abs.env",
            "a/../../x",
            ".git/hooks/pre-commit",
            "",
        ] {
            let f = file(&e.key, bad, "V=vvvv-0000");
            assert!(
                matches!(
                    e.ws_secrets.place(&e.key, &a, &e.ws, &[f]),
                    Err(SecretError::BadPath(_))
                ),
                "{bad}"
            );
        }
        let dup = [
            file(&e.key, "d.env", "V=1111-1111"),
            file(&e.key, "d.env", "V=1111-1111"),
        ];
        assert!(matches!(
            e.ws_secrets.place(&e.key, &a, &e.ws, &dup),
            Err(SecretError::BadPath(_))
        ));
        let big = file(
            &e.key,
            "big.env",
            &"x".repeat(super::super::plan::MAX_FILE_BYTES + 1),
        );
        assert!(matches!(
            e.ws_secrets.place(&e.key, &a, &e.ws, &[big]),
            Err(SecretError::Limit(_))
        ));
        assert_eq!(fs::read_dir(&e.ws).unwrap().count(), 0);
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
    }

    #[test]
    fn a_symlinked_parent_is_not_followed() {
        let e = env();
        let a = AttemptId::generate();
        symlink(&e.outside, e.ws.join("linked")).unwrap();
        let err = e
            .ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "linked/.env", "S=ssss-0000")],
            )
            .unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)), "{err}");
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
        // a parent that is a file
        fs::write(e.ws.join("plain"), b"x").unwrap();
        assert!(e
            .ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "plain/.env", "S=ssss-0000")]
            )
            .is_err());
        // a symlinked directory deeper in the path
        fs::create_dir(e.ws.join("d")).unwrap();
        symlink(&e.outside, e.ws.join("d/deep")).unwrap();
        assert!(e
            .ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "d/deep/.env", "S=ssss-0000")]
            )
            .is_err());
        assert_eq!(fs::read_dir(&e.outside).unwrap().count(), 0);
    }

    #[test]
    fn a_symlink_standing_where_the_file_goes_is_not_written_through() {
        let e = env();
        let a = AttemptId::generate();
        let victim = e.outside.join("victim");
        fs::write(&victim, b"keep").unwrap();
        symlink(&victim, e.ws.join(".env")).unwrap();
        let err = e
            .ws_secrets
            .place(&e.key, &a, &e.ws, &[file(&e.key, ".env", "S=ssss-0000")])
            .unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)));
        assert_eq!(fs::read(&victim).unwrap(), b"keep");
        // a dangling link too
        symlink(e.outside.join("nothing"), e.ws.join(".env2")).unwrap();
        assert!(e
            .ws_secrets
            .place(&e.key, &a, &e.ws, &[file(&e.key, ".env2", "S=ssss-0000")])
            .is_err());
        assert!(!e.outside.join("nothing").exists());
    }

    #[test]
    fn a_workspace_that_is_a_link_or_missing_is_refused() {
        let e = env();
        let a = AttemptId::generate();
        let link = e.state.join("wslink");
        symlink(&e.ws, &link).unwrap();
        let f = [file(&e.key, ".env", "S=ssss-0000")];
        assert!(matches!(
            e.ws_secrets.place(&e.key, &a, &link, &f),
            Err(SecretError::Placement(_))
        ));
        assert!(matches!(
            e.ws_secrets.place(&e.key, &a, &e.state.join("none"), &f),
            Err(SecretError::Io { .. })
        ));
        assert!(!e.ws.join(".env").exists());
    }

    #[test]
    fn an_existing_file_is_never_overwritten_and_the_call_leaves_nothing() {
        let e = env();
        let a = AttemptId::generate();
        fs::write(e.ws.join("b.env"), b"mine").unwrap();
        let files = [
            file(&e.key, "a.env", "A=aaaa-1111"),
            file(&e.key, "b.env", "B=bbbb-2222"),
        ];
        let err = e.ws_secrets.place(&e.key, &a, &e.ws, &files).unwrap_err();
        assert!(matches!(err, SecretError::Placement(_)));
        assert_eq!(fs::read(e.ws.join("b.env")).unwrap(), b"mine");
        assert!(!e.ws.join("a.env").exists(), "the first file was undone");
    }

    #[test]
    fn a_resent_request_is_idempotent_but_changed_content_is_not_accepted() {
        let e = env();
        let a = AttemptId::generate();
        let files = [file(&e.key, ".env", "A=aaaa-1111")];
        assert_eq!(e.ws_secrets.place(&e.key, &a, &e.ws, &files).unwrap(), 1);
        assert_eq!(e.ws_secrets.place(&e.key, &a, &e.ws, &files).unwrap(), 1);
        // the file was changed by something else: not silently accepted
        fs::write(e.ws.join(".env"), b"tampered").unwrap();
        assert!(e.ws_secrets.place(&e.key, &a, &e.ws, &files).is_err());
        // widened permissions: not "ours" any more
        fs::write(e.ws.join(".env"), b"A=aaaa-1111").unwrap();
        fs::set_permissions(e.ws.join(".env"), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(e.ws_secrets.place(&e.key, &a, &e.ws, &files).is_err());
        // another attempt does not own it
        let b = AttemptId::generate();
        fs::set_permissions(e.ws.join(".env"), fs::Permissions::from_mode(0o600)).unwrap();
        assert!(e.ws_secrets.place(&e.key, &b, &e.ws, &files).is_err());
    }

    #[test]
    fn batches_for_one_attempt_add_up_and_stay_in_one_workspace() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .place(&e.key, &a, &e.ws, &[file(&e.key, "1.env", "A=aaaa-1111")])
            .unwrap();
        e.ws_secrets
            .place(&e.key, &a, &e.ws, &[file(&e.key, "2.env", "B=bbbb-2222")])
            .unwrap();
        let other = e.state.join("ws2");
        fs::create_dir(&other).unwrap();
        assert!(matches!(
            e.ws_secrets
                .place(&e.key, &a, &other, &[file(&e.key, "3.env", "C=cccc-3333")]),
            Err(SecretError::Placement(_))
        ));
        let r = e.ws_secrets.erase(&a).unwrap();
        assert_eq!(r.removed, ["1.env", "2.env"]);
        assert!(!e.ws.join("1.env").exists() && !e.ws.join("2.env").exists());
    }

    #[test]
    fn erase_removes_files_and_the_index_and_can_be_repeated() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[
                    file(&e.key, "x/.env", "A=aaaa-1111"),
                    file(&e.key, ".env", "B=bbbb-2222"),
                ],
            )
            .unwrap();
        let r = e.ws_secrets.erase(&a).unwrap();
        assert_eq!(r.removed.len(), 2);
        assert!(r.gone.is_empty() && r.unreachable.is_empty());
        assert!(!e.ws.join(".env").exists() && !e.ws.join("x/.env").exists());
        assert!(!e
            .state
            .join("secret-index")
            .join(format!("{a}.json"))
            .exists());
        assert_eq!(e.ws_secrets.erase(&a).unwrap(), EraseReport::default());
    }

    #[test]
    fn erase_after_the_workspace_was_tampered_with_follows_no_link() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[
                    file(&e.key, "d/.env", "A=aaaa-1111"),
                    file(&e.key, ".env", "B=bbbb-2222"),
                    file(&e.key, "gone.env", "C=cccc-3333"),
                ],
            )
            .unwrap();
        // the parent directory is swapped for a link to a directory outside
        let victim = e.outside.join(".env");
        fs::write(&victim, b"precious").unwrap();
        fs::remove_dir_all(e.ws.join("d")).unwrap();
        symlink(&e.outside, e.ws.join("d")).unwrap();
        // the file itself is swapped for a link to a file outside
        fs::remove_file(e.ws.join(".env")).unwrap();
        let precious2 = e.outside.join("p2");
        fs::write(&precious2, b"precious2").unwrap();
        symlink(&precious2, e.ws.join(".env")).unwrap();
        fs::remove_file(e.ws.join("gone.env")).unwrap();
        let r = e.ws_secrets.erase(&a).unwrap();
        assert_eq!(r.unreachable, ["d/.env"]);
        assert_eq!(r.removed, [".env"]);
        assert_eq!(r.gone, ["gone.env"]);
        assert_eq!(fs::read(&victim).unwrap(), b"precious");
        assert_eq!(fs::read(&precious2).unwrap(), b"precious2");
        assert!(fs::symlink_metadata(e.ws.join(".env")).is_err());
    }

    #[test]
    fn erase_copes_with_a_missing_or_replaced_workspace_and_a_directory_in_the_way() {
        let e = env();
        let a = AttemptId::generate();
        e.ws_secrets
            .place(&e.key, &a, &e.ws, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        fs::remove_dir_all(&e.ws).unwrap();
        assert_eq!(e.ws_secrets.erase(&a).unwrap().gone, [".env"]);
        // the workspace path became a link
        let b = AttemptId::generate();
        fs::create_dir(&e.ws).unwrap();
        e.ws_secrets
            .place(&e.key, &b, &e.ws, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        let moved = e.state.join("moved");
        fs::rename(&e.ws, &moved).unwrap();
        symlink(&moved, &e.ws).unwrap();
        assert_eq!(e.ws_secrets.erase(&b).unwrap().unreachable, [".env"]);
        assert!(moved.join(".env").exists(), "the link was not followed");
        // a directory where the file was
        let c = AttemptId::generate();
        let ws3 = e.state.join("ws3");
        fs::create_dir(&ws3).unwrap();
        e.ws_secrets
            .place(&e.key, &c, &ws3, &[file(&e.key, ".env", "A=aaaa-1111")])
            .unwrap();
        fs::remove_file(ws3.join(".env")).unwrap();
        fs::create_dir(ws3.join(".env")).unwrap();
        assert_eq!(e.ws_secrets.erase(&c).unwrap().unreachable, [".env"]);
    }

    #[test]
    fn reconcile_erases_terminal_attempts_and_keeps_the_others() {
        let e = env();
        let (t, r, u) = (
            AttemptId::generate(),
            AttemptId::generate(),
            AttemptId::generate(),
        );
        for (i, a) in [&t, &r, &u].into_iter().enumerate() {
            let ws = e.state.join(format!("ws{i}"));
            fs::create_dir(&ws).unwrap();
            e.ws_secrets
                .place(&e.key, a, &ws, &[file(&e.key, ".env", "A=aaaa-1111")])
                .unwrap();
        }
        // a file that is not an attempt's record, and one that is not a record
        fs::write(e.state.join("secret-index/junk.json"), b"{}").unwrap();
        fs::write(e.state.join("secret-index/notes.txt"), b"x").unwrap();
        let running = r.clone();
        let rep = e.ws_secrets.reconcile(|a| *a != running).unwrap();
        let mut erased = rep.erased.clone();
        erased.sort();
        let mut want = vec![t.clone(), u.clone()];
        want.sort();
        assert_eq!(erased, want);
        assert_eq!(rep.kept, vec![r.clone()]);
        assert_eq!(rep.failed.len(), 1);
        assert!(rep.failed[0].0 == "junk.json");
        assert!(!e.state.join("ws0/.env").exists() && !e.state.join("ws2/.env").exists());
        assert!(e.state.join("ws1/.env").exists());
        // a second pass has nothing left to erase
        let again = e.ws_secrets.reconcile(|_| true).unwrap();
        assert_eq!(again.erased, vec![r]);
    }

    #[test]
    fn a_damaged_index_is_reported_and_kept_for_a_later_pass() {
        let e = env();
        let a = AttemptId::generate();
        let idx = e.state.join("secret-index").join(format!("{a}.json"));
        fs::write(&idx, b"not json").unwrap();
        fs::set_permissions(&idx, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            e.ws_secrets.erase(&a),
            Err(SecretError::Format(_))
        ));
        let rep = e.ws_secrets.reconcile(|_| true).unwrap();
        assert_eq!(rep.failed.len(), 1);
        assert!(idx.exists());
        // an index others can read is refused as well
        fs::set_permissions(&idx, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            e.ws_secrets.erase(&a),
            Err(SecretError::Insecure { .. })
        ));
    }

    #[test]
    fn the_index_directory_must_be_private() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("secret-index");
        fs::create_dir(&d).unwrap();
        fs::set_permissions(&d, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            WorkerSecrets::open(tmp.path()),
            Err(SecretError::Insecure { .. })
        ));
    }

    #[test]
    fn too_many_files_over_several_requests_are_refused() {
        let e = env();
        let a = AttemptId::generate();
        let first: Vec<SecretFile> = (0..MAX_FILES)
            .map(|i| file(&e.key, &format!("f{i}"), "V=vvvv-0000"))
            .collect();
        e.ws_secrets.place(&e.key, &a, &e.ws, &first).unwrap();
        let err = e
            .ws_secrets
            .place(
                &e.key,
                &a,
                &e.ws,
                &[file(&e.key, "one-more", "V=vvvv-0000")],
            )
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::Limit(Limit::TooManyFiles { .. })
        ));
        assert!(!e.ws.join("one-more").exists());
    }
}
