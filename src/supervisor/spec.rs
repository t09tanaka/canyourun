//! What the agent hands to a supervisor process: a JSON file naming the
//! attempt, the nodes to run in order, the timing, and where output goes.

use super::error::{Result, SupervisorError};
use super::timeouts::TimeoutPolicy;
use crate::ids::{AttemptId, NodeId, WorkerId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Spec chapter 12 defaults.
pub const DEFAULT_LEASE_MS: u64 = 60_000;
pub const DEFAULT_TERM_GRACE_MS: u64 = 10_000;
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

/// One command of the attempt (a step, a setup builder, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub node_id: NodeId,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    /// The command's whole environment. Nothing of the supervisor's own
    /// environment is inherited.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Where the command's stdout and stderr are appended (stdin is
    /// `/dev/null`). The log spool reads from here. `None` inherits the
    /// supervisor's own, which is the null device when it runs detached.
    #[serde(default)]
    pub stdout_log: Option<PathBuf>,
    #[serde(default)]
    pub weight: Option<u32>,
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
    /// Hold an idle-sleep inhibition while running.
    #[serde(default = "d_true")]
    pub inhibit_sleep: bool,
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
        if self.command_timeout_ms == Some(0)
            || self.resource_wait_timeout_ms == Some(0)
            || self.deadline_ms == Some(0)
        {
            return bad("a timeout of zero is not a timeout");
        }
        let mut seen = std::collections::BTreeSet::new();
        for n in &self.nodes {
            if n.argv.is_empty() || n.argv[0].is_empty() {
                return bad("a node has an empty command");
            }
            if !seen.insert(n.node_id.as_str()) {
                return bad("duplicate node_id");
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

    pub fn traffic_config_path(&self) -> PathBuf {
        self.traffic_config
            .clone()
            .unwrap_or_else(|| self.traffic_state_dir.join("config.toml"))
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let spec: SuperviseSpec =
            serde_json::from_str(&text).map_err(|e| SupervisorError::Spec(e.to_string()))?;
        spec.validate()?;
        Ok(spec)
    }
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
            stdout_log: None,
            weight: None,
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
            inhibit_sleep: false,
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
        });
        let m: SuperviseSpec = serde_json::from_value(minimal).unwrap();
        assert_eq!(m.lease_ms, 60_000);
        assert_eq!(m.term_grace_ms, 10_000);
        assert!(m.inhibit_sleep);
        assert_eq!(m.traffic_config_path(), PathBuf::from("/t/config.toml"));
    }

    #[test]
    fn bad_specs_are_refused() {
        let mut s = spec();
        s.nodes.clear();
        assert!(s.validate().is_err());
        let mut s = spec();
        s.lease_ms = 0;
        assert!(s.validate().is_err());
        let mut s = spec();
        s.poll_ms = 0;
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

    #[test]
    fn from_file_reads_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("spec.json");
        std::fs::write(&p, serde_json::to_string(&spec()).unwrap()).unwrap();
        assert!(SuperviseSpec::from_file(&p).is_ok());
        std::fs::write(&p, "{").unwrap();
        assert!(matches!(
            SuperviseSpec::from_file(&p),
            Err(SupervisorError::Spec(_))
        ));
        let mut bad = spec();
        bad.nodes.clear();
        std::fs::write(&p, serde_json::to_string(&bad).unwrap()).unwrap();
        assert!(SuperviseSpec::from_file(&p).is_err());
        assert!(SuperviseSpec::from_file(&dir.path().join("none.json")).is_err());
    }
}
