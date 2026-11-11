//! What the agent hands to a supervisor process: a JSON file naming the
//! attempt, the nodes to run in order, the timing, and where output goes.

use super::error::{Result, SupervisorError};
use super::lease::LeaseSeed;
use super::spool_plan::check_artifact_plan;
use super::timeouts::TimeoutPolicy;
use crate::ids::{AttemptId, NodeId, PlanDigest, SourceId, WorkerId};
use crate::secrets::mask::Masker;
use crate::wire::MissingPolicy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Spec chapter 12 defaults.
pub const DEFAULT_LEASE_MS: u64 = 60_000;
pub const DEFAULT_TERM_GRACE_MS: u64 = 10_000;
/// The spec's default cap on the log bytes one task may store on a worker.
pub const DEFAULT_LOG_LIMIT_BYTES: u64 = 1 << 30;
/// The largest single artifact kept, unless the spec says otherwise (1 GiB).
pub const DEFAULT_ARTIFACT_BYTES: u64 = 1_073_741_824;
const DEFAULT_KILL_WAIT_MS: u64 = 10_000;
const DEFAULT_POLL_MS: u64 = 200;

fn d_lease() -> u64 {
    DEFAULT_LEASE_MS
}
fn d_grace() -> u64 {
    DEFAULT_TERM_GRACE_MS
}
fn d_kill_wait() -> u64 {
    DEFAULT_KILL_WAIT_MS
}
fn d_poll() -> u64 {
    DEFAULT_POLL_MS
}
fn d_true() -> bool {
    true
}
fn d_log_limit() -> u64 {
    DEFAULT_LOG_LIMIT_BYTES
}
fn d_artifact_bytes() -> u64 {
    DEFAULT_ARTIFACT_BYTES
}

/// One command of the attempt (a step, a setup builder, ...). Its stdout and
/// stderr go to the attempt's log spool (stdin is `/dev/null`). `Debug` shows
/// the names of `env` only: its values may be secrets.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub node_id: NodeId,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    /// The command's whole environment. Nothing of the supervisor's own
    /// environment is inherited.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub weight: Option<u32>,
    /// This command's own run-time limit; replaces the attempt's
    /// `command_timeout_ms` while it runs.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl std::fmt::Debug for NodeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSpec")
            .field("node_id", &self.node_id)
            .field("argv", &self.argv)
            .field("cwd", &self.cwd)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("weight", &self.weight)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

/// The secret values to replace in everything the supervisor stores of the
/// commands' output. They are secrets themselves: `Debug` shows only how
/// many there are, and the spec file that carries them must be readable by
/// the worker's user alone and removed by whoever wrote it.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaskValues(pub Vec<String>);

impl std::fmt::Debug for MaskValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaskValues({} values)", self.0.len())
    }
}

impl MaskValues {
    pub fn masker(&self) -> Masker {
        Masker::new(self.0.iter().map(|v| v.clone().into_bytes()))
    }
}

/// A file the commands leave in the workspace, to be kept as an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFile {
    /// The artifact's name and its relative path in the attempt's store.
    pub name: String,
    /// Where the commands leave it: below `workspace` (relative to it, or
    /// absolute and inside it).
    pub source: PathBuf,
    /// What its declaration says about the file not being there. Only
    /// recorded with an artifact that was not stored; never judged here.
    #[serde(default)]
    pub if_missing: MissingPolicy,
}

/// The artifacts of the attempt. The supervisor copies them out of the
/// workspace once the commands have ended and before it settles the attempt:
/// the result names them and is never changed afterwards, and the workspace
/// is removed as soon as the attempt is terminal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactsSpec {
    /// The attempt's workspace root; no source may lead out of it.
    pub workspace: PathBuf,
    /// Recorded with every artifact (spec chapter 13).
    pub source_id: SourceId,
    pub plan_digest: PlanDigest,
    pub profile_fingerprint: crate::ids::Digest,
    /// A file larger than this is not kept.
    #[serde(default = "d_artifact_bytes")]
    pub max_bytes: u64,
    pub files: Vec<ArtifactFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuperviseSpec {
    /// The worker's state directory (`worker.db` lives here).
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    pub attempt_id: AttemptId,
    /// Where task-traffic-controller keeps its ledger and leases.
    pub traffic_state_dir: PathBuf,
    /// Its configuration file; default `<traffic_state_dir>/config.toml`
    /// (built-in defaults when the file does not exist).
    #[serde(default)]
    pub traffic_config: Option<PathBuf>,
    pub nodes: Vec<NodeSpec>,
    #[serde(default = "d_lease")]
    pub lease_ms: u64,
    #[serde(default = "d_grace")]
    pub term_grace_ms: u64,
    #[serde(default = "d_kill_wait")]
    pub kill_wait_ms: u64,
    /// How often the supervisor looks at the database and the clock.
    #[serde(default = "d_poll")]
    pub poll_ms: u64,
    /// Each command's own run time.
    #[serde(default)]
    pub command_timeout_ms: Option<u64>,
    /// Waiting for admission, per command.
    #[serde(default)]
    pub resource_wait_timeout_ms: Option<u64>,
    /// The whole task, counted from when this supervisor starts (the
    /// agent passes what is left of it).
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    /// The lease as it stood when the agent started this supervisor (it
    /// began at the acceptance or the last renewal). Without it the
    /// supervisor counts a full `lease_ms` from its own start.
    #[serde(default)]
    pub lease_seed: Option<LeaseSeed>,
    /// Hold an idle-sleep inhibition while running.
    #[serde(default = "d_true")]
    pub inhibit_sleep: bool,
    /// The root of the worker's log spool and artifact store: the output of
    /// every node is stored under `<spool_root>/<attempt>/logs`. Default
    /// `<state_dir>/attempts`.
    #[serde(default)]
    pub spool_root: Option<PathBuf>,
    /// The cap on the log bytes the task may store on this worker, all its
    /// attempts together. This attempt gets what the others left.
    #[serde(default = "d_log_limit")]
    pub log_limit_bytes: u64,
    /// What the workspace creation could not do (`Workspace.warnings`); the
    /// supervisor writes them to the event journal when it starts.
    #[serde(default)]
    pub workspace_warnings: Vec<String>,
    /// Secret values masked in the commands' output before it is stored, and
    /// in the workspace warnings. Required, so that a writer who forgot it is
    /// told; an explicit empty list means nothing is masked.
    pub mask: MaskValues,
    /// The files to keep as artifacts; none when absent.
    #[serde(default)]
    pub artifacts: Option<ArtifactsSpec>,
}

impl SuperviseSpec {
    pub fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(SupervisorError::Spec(m.to_string()));
        if self.nodes.is_empty() {
            return bad("no nodes");
        }
        if self.lease_ms == 0 || self.poll_ms == 0 {
            return bad("lease_ms and poll_ms must be positive");
        }
        if self.lease_ms <= self.poll_ms {
            // The lease is counted less one poll for the delay in noticing it.
            return bad("lease_ms must be longer than poll_ms");
        }
        if self.command_timeout_ms == Some(0)
            || self.resource_wait_timeout_ms == Some(0)
            || self.deadline_ms == Some(0)
        {
            return bad("a timeout of zero is not a timeout");
        }
        if self.spool_root.as_ref().is_some_and(|r| !r.is_absolute()) {
            // A relative root would follow whatever directory the supervisor
            // happens to run in.
            return bad("spool_root must be an absolute path");
        }
        let mut seen = std::collections::BTreeSet::new();
        for n in &self.nodes {
            if n.argv.is_empty() || n.argv[0].is_empty() {
                return bad("a node has an empty command");
            }
            if !seen.insert(n.node_id.as_str()) {
                return bad("duplicate node_id");
            }
            if n.timeout_ms == Some(0) {
                return bad("a timeout of zero is not a timeout");
            }
        }
        if let Some(a) = &self.artifacts {
            if !a.workspace.is_absolute() {
                return bad("the artifacts' workspace must be an absolute path");
            }
            let files: Vec<(&str, &Path)> = a
                .files
                .iter()
                .map(|f| (f.name.as_str(), f.source.as_path()))
                .collect();
            if let Err(e) = check_artifact_plan(&a.workspace, &files) {
                return Err(SupervisorError::Spec(e.to_string()));
            }
        }
        Ok(())
    }

    pub fn timeout_policy(&self) -> TimeoutPolicy {
        TimeoutPolicy {
            command_ms: self.command_timeout_ms,
            resource_wait_ms: self.resource_wait_timeout_ms,
            deadline_ms: self.deadline_ms,
        }
    }

    /// The root of the log spool and artifact store.
    pub fn spool_dir(&self) -> PathBuf {
        self.spool_root
            .clone()
            .unwrap_or_else(|| self.state_dir.join("attempts"))
    }

    pub fn traffic_config_path(&self) -> PathBuf {
        self.traffic_config
            .clone()
            .unwrap_or_else(|| self.traffic_state_dir.join("config.toml"))
    }

    /// Read and validate the spec at `path`. It carries secrets (`mask`,
    /// `env`), so a file that is not the current user's own, or that its
    /// group or others can reach (any permission bit of theirs), is refused:
    /// write it with mode 0600. The check is made on the opened file, which
    /// is the one read.
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let meta = file.metadata()?;
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if !private_to(meta.uid(), meta.mode(), me) {
            return Err(SupervisorError::Spec(format!(
                "{} must belong to the current user and be closed to group and others (mode 0600)",
                path.display()
            )));
        }
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        let spec: SuperviseSpec =
            serde_json::from_str(&text).map_err(|e| SupervisorError::Spec(e.to_string()))?;
        spec.validate()?;
        Ok(spec)
    }
}

/// A file of `owner` with `mode` is `user`'s own and no one else's to read,
/// write or execute.
fn private_to(owner: u32, mode: u32, user: u32) -> bool {
    owner == user && mode & 0o077 == 0
}

/// The task-traffic-controller command id of node `index` of an attempt. The
/// node name itself may hold characters an id cannot, so the position is
/// used; the same attempt and node always give the same id (registration is
/// idempotent on it).
pub fn command_id(attempt: &AttemptId, index: usize) -> String {
    format!("rtr-{attempt}-{index}")
}

/// A digest of what the node runs, so a registration of the same id with a
/// different command is refused rather than adopted.
pub fn spec_digest(node: &NodeSpec) -> String {
    let mut h = Sha256::new();
    let doc = serde_json::json!({
        "node_id": node.node_id,
        "argv": node.argv,
        "cwd": node.cwd,
    });
    h.update(doc.to_string().as_bytes());
    format!("sha256:{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> NodeSpec {
        NodeSpec {
            node_id: NodeId::parse(id).unwrap(),
            argv: vec!["true".into()],
            cwd: "/".into(),
            env: BTreeMap::new(),
            weight: None,
            timeout_ms: None,
        }
    }

    fn spec() -> SuperviseSpec {
        SuperviseSpec {
            state_dir: "/s".into(),
            worker_id: WorkerId::parse("w").unwrap(),
            attempt_id: AttemptId::generate(),
            traffic_state_dir: "/t".into(),
            traffic_config: None,
            nodes: vec![node("a"), node("b")],
            lease_ms: DEFAULT_LEASE_MS,
            term_grace_ms: DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 1,
            poll_ms: 1,
            command_timeout_ms: None,
            resource_wait_timeout_ms: None,
            deadline_ms: None,
            lease_seed: None,
            inhibit_sleep: false,
            spool_root: Some("/spool".into()),
            log_limit_bytes: DEFAULT_LOG_LIMIT_BYTES,
            workspace_warnings: vec![],
            mask: MaskValues::default(),
            artifacts: None,
        }
    }

    #[test]
    fn a_good_spec_validates_and_round_trips_with_defaults() {
        let s = spec();
        s.validate().unwrap();
        let json = serde_json::to_string(&s).unwrap();
        let back: SuperviseSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
        let minimal = serde_json::json!({
            "state_dir": "/s", "worker_id": "w", "attempt_id": s.attempt_id,
            "traffic_state_dir": "/t",
            "nodes": [{"node_id": "a", "argv": ["true"], "cwd": "/"}],
            "mask": [],
        });
        // A spec that does not say what to mask is refused, not read as
        // "nothing to mask".
        let mut forgot = minimal.clone();
        forgot.as_object_mut().unwrap().remove("mask");
        let e = serde_json::from_value::<SuperviseSpec>(forgot).unwrap_err();
        assert!(e.to_string().contains("mask"), "{e}");
        let m: SuperviseSpec = serde_json::from_value(minimal).unwrap();
        assert_eq!(m.lease_ms, 60_000);
        assert_eq!(m.term_grace_ms, 10_000);
        assert!(m.inhibit_sleep);
        assert_eq!(m.log_limit_bytes, 1 << 30);
        assert!(m.workspace_warnings.is_empty());
        assert_eq!(m.spool_dir(), PathBuf::from("/s/attempts"));
        assert_eq!(s.spool_dir(), PathBuf::from("/spool"));
        assert!(m.mask.masker().is_empty());
        // The values to mask never show in a debug print of the spec.
        let mut secret = spec();
        secret.mask = MaskValues(vec!["hunter2-value".into()]);
        secret.nodes[0]
            .env
            .insert("API_TOKEN".into(), "hunter2-in-env".into());
        let shown = format!("{secret:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("API_TOKEN"), "{shown}");
        assert_eq!(secret.mask.masker().mask(b"a hunter2-value b"), b"a *** b");
        let back: SuperviseSpec =
            serde_json::from_str(&serde_json::to_string(&secret).unwrap()).unwrap();
        assert_eq!(back, secret);
        assert_eq!(m.traffic_config_path(), PathBuf::from("/t/config.toml"));
    }

    #[test]
    fn bad_specs_are_refused() {
        let mut s = spec();
        s.nodes.clear();
        assert!(s.validate().is_err());
        let mut s = spec();
        s.spool_root = Some("spool".into());
        assert!(s.validate().is_err());
        let mut s = spec();
        s.lease_ms = 0;
        assert!(s.validate().is_err());
        let mut s = spec();
        s.poll_ms = 0;
        assert!(s.validate().is_err());
        let mut s = spec();
        s.lease_ms = s.poll_ms;
        assert!(s.validate().is_err());
        for f in [
            |s: &mut SuperviseSpec| s.command_timeout_ms = Some(0),
            |s: &mut SuperviseSpec| s.resource_wait_timeout_ms = Some(0),
            |s: &mut SuperviseSpec| s.deadline_ms = Some(0),
        ] {
            let mut s = spec();
            f(&mut s);
            assert!(s.validate().is_err());
        }
        let mut s = spec();
        s.nodes[0].argv.clear();
        assert!(s.validate().is_err());
        let mut s = spec();
        s.nodes[0].argv = vec![String::new()];
        assert!(s.validate().is_err());
        let mut s = spec();
        s.nodes[1].node_id = s.nodes[0].node_id.clone();
        assert!(s.validate().is_err());
    }

    #[test]
    fn the_policy_and_config_path_follow_the_spec() {
        let mut s = spec();
        s.command_timeout_ms = Some(1);
        s.resource_wait_timeout_ms = Some(2);
        s.deadline_ms = Some(3);
        s.traffic_config = Some("/x/c.toml".into());
        let p = s.timeout_policy();
        assert_eq!(
            (p.command_ms, p.resource_wait_ms, p.deadline_ms),
            (Some(1), Some(2), Some(3))
        );
        assert_eq!(s.traffic_config_path(), PathBuf::from("/x/c.toml"));
    }

    #[test]
    fn command_ids_are_valid_for_the_ledger_and_stable() {
        let a = AttemptId::generate();
        let id = command_id(&a, 3);
        assert_eq!(id, command_id(&a, 3));
        assert_ne!(id, command_id(&a, 4));
        assert!(task_traffic_controller::managed::valid_id(&id));
    }

    #[test]
    fn the_digest_changes_with_the_command() {
        let a = node("a");
        let mut b = node("a");
        assert_eq!(spec_digest(&a), spec_digest(&b));
        b.argv.push("x".into());
        assert_ne!(spec_digest(&a), spec_digest(&b));
        let mut c = node("a");
        c.cwd = "/tmp".into();
        assert_ne!(spec_digest(&a), spec_digest(&c));
        assert!(spec_digest(&a).starts_with("sha256:"));
    }

    fn write_private(p: &Path, text: &str) {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let _ = std::fs::remove_file(p);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(p)
            .unwrap();
        f.write_all(text.as_bytes()).unwrap();
    }

    #[test]
    fn from_file_reads_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("spec.json");
        write_private(&p, &serde_json::to_string(&spec()).unwrap());
        assert!(SuperviseSpec::from_file(&p).is_ok());
        write_private(&p, "{");
        assert!(matches!(
            SuperviseSpec::from_file(&p),
            Err(SupervisorError::Spec(_))
        ));
        let mut bad = spec();
        bad.nodes.clear();
        write_private(&p, &serde_json::to_string(&bad).unwrap());
        assert!(SuperviseSpec::from_file(&p).is_err());
        assert!(SuperviseSpec::from_file(&dir.path().join("none.json")).is_err());
    }

    #[test]
    fn a_spec_file_others_can_reach_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("spec.json");
        write_private(&p, &serde_json::to_string(&spec()).unwrap());
        for mode in [0o640, 0o604, 0o620, 0o602, 0o610, 0o601, 0o644] {
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            let e = SuperviseSpec::from_file(&p).unwrap_err();
            assert!(
                matches!(&e, SupervisorError::Spec(m) if m.contains("0600")),
                "{mode:o}: {e}"
            );
        }
        for mode in [0o600, 0o400, 0o700] {
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(SuperviseSpec::from_file(&p).is_ok(), "{mode:o}");
        }
        // Another user's file is refused whatever its mode.
        assert!(private_to(501, 0o100600, 501));
        assert!(!private_to(0, 0o100600, 501));
        assert!(!private_to(501, 0o100600, 0));
        assert!(!private_to(501, 0o100640, 501));
    }
}
