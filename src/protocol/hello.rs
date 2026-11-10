//! `hello` negotiation (spec chapter 11). Pure: both sides' decisions, no I/O.
//!
//! A worker that does not fit gets `PROTOCOL_INCOMPATIBLE` and the connection
//! is closed; the coordinator never dispatches to it. The coordinator checks the
//! worker's answer as well, since the peer is not trusted to have chosen
//! something it offered.

use crate::error::{ApiError, ErrorCode};
use crate::ids::{AgentEpoch, ClusterId, CoordinatorEpoch, Digest, WorkerBootId, WorkerId};
use crate::wire::{HelloRequest, HelloResponse, SchemaVersion};

/// Protocol versions this build speaks.
pub const PROTOCOL_VERSIONS: &[u32] = &[1];

/// Features this build implements. `blob_resume` is offset and digest resume of
/// blob transfer, `log_resume` the offset and sequence resume of logs.
pub const SUPPORTED_FEATURES: &[&str] = &["blob_resume", "log_resume"];

/// What one side offers and requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalHello {
    pub cluster_id: ClusterId,
    pub versions: Vec<u32>,
    pub features: Vec<String>,
    /// Features without which the peer is refused.
    pub required_features: Vec<String>,
}

/// The agreed protocol version and features.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    pub version: u32,
    pub features: Vec<String>,
}

fn incompatible(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::ProtocolIncompatible, message)
}

/// Features in `ours` that `theirs` also lists, in our order.
fn common_features(ours: &[String], theirs: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for f in ours {
        if theirs.contains(f) && !out.contains(f) {
            out.push(f.clone());
        }
    }
    out
}

fn missing_required(required: &[String], have: &[String]) -> Option<String> {
    let missing: Vec<&str> = required
        .iter()
        .filter(|f| !have.contains(f))
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        None
    } else {
        Some(missing.join(", "))
    }
}

/// The worker's decision on a coordinator's `hello`. The `schema_version` is
/// read permissively by the wire layer and judged here.
pub fn negotiate(local: &LocalHello, req: &HelloRequest) -> Result<Negotiated, ApiError> {
    if let Err(e) = req.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    if req.cluster_id != local.cluster_id {
        return Err(incompatible("cluster_id mismatch"));
    }
    let version = local
        .versions
        .iter()
        .filter(|v| req.protocol_versions.contains(v))
        .max()
        .copied()
        .ok_or_else(|| {
            incompatible(format!(
                "no common protocol version (worker speaks {:?}, coordinator offered {:?})",
                local.versions, req.protocol_versions
            ))
        })?;
    let features = common_features(&local.features, &req.features);
    if let Some(missing) = missing_required(&local.required_features, &features) {
        return Err(incompatible(format!(
            "required features missing: {missing}"
        )));
    }
    Ok(Negotiated { version, features })
}

/// The coordinator's decision on a worker's `hello` answer.
pub fn verify_response(
    offered: &HelloRequest,
    required_features: &[String],
    resp: &HelloResponse,
) -> Result<Negotiated, ApiError> {
    if let Err(e) = resp.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    if resp.cluster_id != offered.cluster_id {
        return Err(incompatible("cluster_id mismatch"));
    }
    if !offered.protocol_versions.contains(&resp.protocol_version) {
        return Err(incompatible(format!(
            "worker chose protocol version {} that was not offered",
            resp.protocol_version
        )));
    }
    if let Some(extra) = resp.features.iter().find(|f| !offered.features.contains(f)) {
        return Err(incompatible(format!(
            "worker enabled feature {extra} that was not offered"
        )));
    }
    if let Some(missing) = missing_required(required_features, &resp.features) {
        return Err(incompatible(format!(
            "required features missing: {missing}"
        )));
    }
    Ok(Negotiated {
        version: resp.protocol_version,
        features: resp.features.clone(),
    })
}

/// What a worker reports about itself in `hello`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
    pub worker_id: WorkerId,
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub profile: String,
    pub profile_fingerprint: Digest,
    /// The highest coordinator epoch this worker has seen.
    pub coordinator_epoch: CoordinatorEpoch,
}

impl WorkerIdentity {
    pub fn response(&self, cluster_id: &ClusterId, negotiated: &Negotiated) -> HelloResponse {
        HelloResponse {
            schema_version: SchemaVersion::CURRENT,
            cluster_id: cluster_id.clone(),
            protocol_version: negotiated.version,
            features: negotiated.features.clone(),
            worker_id: self.worker_id.clone(),
            worker_boot_id: self.worker_boot_id.clone(),
            agent_epoch: self.agent_epoch,
            profile: self.profile.clone(),
            profile_fingerprint: self.profile_fingerprint.clone(),
            coordinator_epoch: self.coordinator_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CoordinatorId, RequestId};

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|x| x.to_string()).collect()
    }

    fn local(cluster: &ClusterId) -> LocalHello {
        LocalHello {
            cluster_id: cluster.clone(),
            versions: vec![1, 2],
            features: s(&["a", "b"]),
            required_features: vec![],
        }
    }

    fn request(cluster: &ClusterId) -> HelloRequest {
        HelloRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            cluster_id: cluster.clone(),
            coordinator_id: CoordinatorId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            protocol_versions: vec![2, 3],
            features: s(&["b", "c"]),
        }
    }

    fn identity() -> WorkerIdentity {
        WorkerIdentity {
            worker_id: WorkerId::parse("w1").unwrap(),
            worker_boot_id: WorkerBootId::parse("boot1").unwrap(),
            agent_epoch: AgentEpoch(4),
            profile: "p".into(),
            profile_fingerprint: Digest::of_bytes(b"p"),
            coordinator_epoch: CoordinatorEpoch(9),
        }
    }

    fn code(e: ApiError) -> (ErrorCode, String) {
        (e.code, e.message)
    }

    #[test]
    fn picks_the_highest_common_version_and_the_common_features() {
        let c = ClusterId::generate();
        let n = negotiate(&local(&c), &request(&c)).unwrap();
        assert_eq!(
            n,
            Negotiated {
                version: 2,
                features: s(&["b"])
            }
        );
        let mut req = request(&c);
        req.protocol_versions = vec![1, 2];
        assert_eq!(negotiate(&local(&c), &req).unwrap().version, 2);
        req.protocol_versions = vec![1];
        assert_eq!(negotiate(&local(&c), &req).unwrap().version, 1);
    }

    #[test]
    fn refuses_other_schema_versions_clusters_and_disjoint_versions() {
        let c = ClusterId::generate();
        let mut req = request(&c);
        req.schema_version = SchemaVersion(99);
        let (code_, msg) = code(negotiate(&local(&c), &req).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("99"));

        let req = request(&ClusterId::generate());
        let (code_, msg) = code(negotiate(&local(&c), &req).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("cluster_id"));

        let mut req = request(&c);
        req.protocol_versions = vec![5];
        let (code_, msg) = code(negotiate(&local(&c), &req).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("protocol version"));
        req.protocol_versions = vec![];
        assert!(negotiate(&local(&c), &req).is_err());
    }

    #[test]
    fn a_missing_required_feature_refuses_and_a_present_one_passes() {
        let c = ClusterId::generate();
        let mut l = local(&c);
        l.required_features = s(&["b"]);
        assert!(negotiate(&l, &request(&c)).is_ok());
        l.required_features = s(&["a", "c"]);
        let (code_, msg) = code(negotiate(&l, &request(&c)).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        // `c` is offered by the peer but unknown to us; `a` is ours but not offered.
        assert!(msg.contains("a, c"), "{msg}");
    }

    #[test]
    fn duplicate_offers_do_not_duplicate_features() {
        let c = ClusterId::generate();
        let mut l = local(&c);
        l.features = s(&["b", "b"]);
        assert_eq!(negotiate(&l, &request(&c)).unwrap().features, s(&["b"]));
    }

    #[test]
    fn the_coordinator_accepts_what_it_offered() {
        let c = ClusterId::generate();
        let req = request(&c);
        let n = negotiate(&local(&c), &req).unwrap();
        let resp = identity().response(&c, &n);
        assert_eq!(resp.protocol_version, 2);
        assert_eq!(resp.worker_id, identity().worker_id);
        assert_eq!(resp.agent_epoch, AgentEpoch(4));
        assert_eq!(resp.coordinator_epoch, CoordinatorEpoch(9));
        assert_eq!(resp.profile, "p");
        assert_eq!(resp.worker_boot_id, identity().worker_boot_id);
        assert_eq!(resp.profile_fingerprint, identity().profile_fingerprint);
        assert_eq!(verify_response(&req, &[], &resp).unwrap(), n);
        assert_eq!(verify_response(&req, &s(&["b"]), &resp).unwrap(), n);
    }

    #[test]
    fn the_coordinator_refuses_a_bad_answer() {
        let c = ClusterId::generate();
        let req = request(&c);
        let n = negotiate(&local(&c), &req).unwrap();
        let good = identity().response(&c, &n);

        let mut r = good.clone();
        r.schema_version = SchemaVersion(7);
        let (code_, msg) = code(verify_response(&req, &[], &r).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains('7'));

        let mut r = good.clone();
        r.cluster_id = ClusterId::generate();
        assert!(code(verify_response(&req, &[], &r).unwrap_err())
            .1
            .contains("cluster_id"));

        let mut r = good.clone();
        r.protocol_version = 1;
        assert!(code(verify_response(&req, &[], &r).unwrap_err())
            .1
            .contains("not offered"));

        let mut r = good.clone();
        r.features = s(&["b", "zzz"]);
        assert!(code(verify_response(&req, &[], &r).unwrap_err())
            .1
            .contains("zzz"));

        let (code_, msg) = code(verify_response(&req, &s(&["c"]), &good).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("required"));
    }

    #[test]
    fn the_shipped_constants_are_consistent() {
        assert!(!PROTOCOL_VERSIONS.is_empty());
        assert!(SUPPORTED_FEATURES.contains(&"blob_resume"));
    }
}
