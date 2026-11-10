//! Wire contract between coordinator and worker (spec chapter 11): request and
//! response DTOs for every operation, canonical JSON, and `payload_hash`.
//!
//! This module only defines types. Framing and transport belong to the protocol
//! layer; persistence belongs to storage.
//!
//! Request identity:
//! * every request carries a `request_id`;
//! * `submit_attempt` is idempotent on `attempt_id + payload_hash` (same pair
//!   returns the existing state, same attempt with another hash is `CONFLICT`);
//! * state-changing requests other than submit are idempotent on `request_id`
//!   (and `renew_lease` also on `lease_seq`);
//! * read-only requests are repeatable.
//!
//! `payload_hash` is `sha256` over the canonical JSON of the submitted
//! [`AttemptPayload`]; [`content_hash`] gives every other request a comparable
//! hash that ignores `request_id`, so a replay with the same id but different
//! content is detectable. Bulk and secret bytes are never hashed: see
//! [`content_hash`].
//!
//! Retire, cancel and late requests (the contract every worker implements):
//! * `retire_attempt` writes a tombstone; it is kept for the cluster's
//!   lifetime;
//! * `get_attempt` on a retired attempt is never `NOT_FOUND`: it answers the
//!   status with `retired: true` (or the `RETIRED` error when only a tombstone
//!   exists, see [`CancelAttemptRequest`]);
//! * a `submit_attempt` that arrives after the tombstone is refused with
//!   `RETIRED`, and so is any other mutation of that attempt;
//! * a `cancel_attempt` for an attempt that has not arrived yet also writes a
//!   tombstone, and the submit that arrives later is refused with `RETIRED`, so
//!   a cancelled attempt never spawns late.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};
use thiserror::Error;

use crate::error::ApiError;
use crate::ids::*;
use crate::state::{
    check_termination, AttemptState, Exit, Observation, Receipt, Receipts, StepState, Termination,
    TerminationStateError,
};

// ---------------------------------------------------------------------------
// schema version, canonical JSON, hashing
// ---------------------------------------------------------------------------

/// Version of every wire DTO's shape.
pub const WIRE_SCHEMA_VERSION: u32 = 1;

/// The `schema_version` field. It serializes as a number and deserializes any
/// number: a peer that speaks another version must still be able to read
/// `hello` and error replies to learn that. Whether the version is the one this
/// build speaks is a separate decision for the protocol layer, taken after
/// `hello`: call [`SchemaVersion::ensure_current`] (or
/// [`Request::ensure_current_schema`] and friends) on every other message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(pub u32);

/// A schema version this build does not speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("unsupported schema_version {got}, expected {expected}")]
pub struct UnsupportedSchemaVersion {
    pub got: u32,
    pub expected: u32,
}

impl SchemaVersion {
    pub const CURRENT: SchemaVersion = SchemaVersion(WIRE_SCHEMA_VERSION);

    /// The post-`hello` check: only [`WIRE_SCHEMA_VERSION`] is accepted.
    pub fn ensure_current(self) -> Result<(), UnsupportedSchemaVersion> {
        if self == Self::CURRENT {
            Ok(())
        } else {
            Err(UnsupportedSchemaVersion {
                got: self.0,
                expected: WIRE_SCHEMA_VERSION,
            })
        }
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::CURRENT
    }
}

impl Serialize for SchemaVersion {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        u32::deserialize(d).map(SchemaVersion)
    }
}

/// Canonical JSON could not be produced.
#[derive(Debug, Error)]
pub enum CanonicalError {
    #[error("value does not serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("non-integer number `{0}` is not allowed in canonical JSON")]
    Float(String),
}

/// Canonical JSON text: object keys sorted bytewise, no insignificant
/// whitespace, integers only. Independent of struct field order and of any
/// `preserve_order` feature of `serde_json`.
pub fn canonical_json(value: &Value) -> Result<String, CanonicalError> {
    let mut out = String::new();
    write_canonical(value, &mut out)?;
    Ok(out)
}

fn write_canonical(value: &Value, out: &mut String) -> Result<(), CanonicalError> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if n.is_f64() {
                return Err(CanonicalError::Float(n.to_string()));
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => out.push_str(&serde_json::to_string(s)?),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key)?);
                out.push(':');
                write_canonical(&map[*key], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// `sha256` of the canonical JSON of `value`.
pub fn payload_hash<T: Serialize>(value: &T) -> Result<PayloadHash, CanonicalError> {
    let v = serde_json::to_value(value)?;
    Ok(Digest::of_bytes(canonical_json(&v)?.as_bytes()))
}

/// The text [`content_hash`] digests: the request's canonical JSON without
/// `request_id`, with bulk and secret bytes replaced by what identifies them.
///
/// * `secret_bundle_put`: the plaintext `content` is dropped; each file
///   contributes its `path` and its HMAC digest. No plain hash of secret
///   content is made anywhere.
/// * `blob_put`: `data` is dropped; `digest`, `offset` and `total_size` stay.
///   Chunk bytes are verified against the blob digest, not by this hash.
fn content_hash_input(request: &Request) -> Result<String, CanonicalError> {
    // every request serializes to a JSON object (tagged by `op`)
    let mut map: serde_json::Map<String, Value> =
        serde_json::from_value(serde_json::to_value(request)?)?;
    map.remove("request_id");
    match request {
        Request::BlobPut(_) => {
            map.remove("data");
        }
        Request::SecretBundlePut(r) => {
            let files = r
                .files
                .iter()
                .map(|f| json!({"path": f.path, "hmac_digest": f.hmac_digest}))
                .collect();
            map.insert("files".to_string(), Value::Array(files));
        }
        _ => {}
    }
    canonical_json(&Value::Object(map))
}

/// Hash of a request's content with the top-level `request_id` removed, so a
/// replay with the same id but different content is detectable. Plaintext
/// secrets and blob chunk bytes are not hashed; see the input rules above.
pub fn content_hash(request: &Request) -> Result<PayloadHash, CanonicalError> {
    Ok(Digest::of_bytes(content_hash_input(request)?.as_bytes()))
}

// ---------------------------------------------------------------------------
// operations and identity
// ---------------------------------------------------------------------------

/// How a request is deduplicated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Idempotency {
    /// `attempt_id + payload_hash`.
    AttemptAndPayload,
    /// `request_id`.
    RequestId,
    /// No state change; may be repeated freely.
    ReadOnly,
}

/// Every protocol operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Hello,
    Inspect,
    Forecast,
    SubmitAttempt,
    GetAttempt,
    ListAttempts,
    RenewLease,
    CancelAttempt,
    RetireAttempt,
    WatchEvents,
    ReadLog,
    GetResult,
    AckCollected,
    BlobPut,
    BlobGet,
    BlobVerify,
    SecretBundlePut,
    EnvReport,
    EnvSync,
}

impl Operation {
    pub const ALL: [Operation; 19] = [
        Operation::Hello,
        Operation::Inspect,
        Operation::Forecast,
        Operation::SubmitAttempt,
        Operation::GetAttempt,
        Operation::ListAttempts,
        Operation::RenewLease,
        Operation::CancelAttempt,
        Operation::RetireAttempt,
        Operation::WatchEvents,
        Operation::ReadLog,
        Operation::GetResult,
        Operation::AckCollected,
        Operation::BlobPut,
        Operation::BlobGet,
        Operation::BlobVerify,
        Operation::SecretBundlePut,
        Operation::EnvReport,
        Operation::EnvSync,
    ];

    pub fn idempotency(self) -> Idempotency {
        match self {
            Operation::SubmitAttempt => Idempotency::AttemptAndPayload,
            Operation::RenewLease
            | Operation::CancelAttempt
            | Operation::RetireAttempt
            | Operation::AckCollected
            | Operation::BlobPut
            | Operation::SecretBundlePut
            | Operation::EnvSync => Idempotency::RequestId,
            Operation::Hello
            | Operation::Inspect
            | Operation::Forecast
            | Operation::GetAttempt
            | Operation::ListAttempts
            | Operation::WatchEvents
            | Operation::ReadLog
            | Operation::GetResult
            | Operation::BlobGet
            | Operation::BlobVerify
            | Operation::EnvReport => Idempotency::ReadOnly,
        }
    }
}

// ---------------------------------------------------------------------------
// shared building blocks
// ---------------------------------------------------------------------------

/// Bytes carried inside JSON as standard base64.
#[derive(Clone, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map(Bytes)
            .map_err(D::Error::custom)
    }
}

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes({} bytes)", self.0.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

// ---------------------------------------------------------------------------
// hello
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub cluster_id: ClusterId,
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    /// Protocol versions the coordinator speaks.
    pub protocol_versions: Vec<u32>,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloResponse {
    pub schema_version: SchemaVersion,
    /// The cluster this worker belongs to.
    pub cluster_id: ClusterId,
    /// The version chosen from the coordinator's list.
    pub protocol_version: u32,
    pub features: Vec<String>,
    pub worker_id: WorkerId,
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub profile: String,
    pub profile_fingerprint: Digest,
    /// The highest coordinator epoch the worker has seen.
    pub coordinator_epoch: CoordinatorEpoch,
}

// ---------------------------------------------------------------------------
// inspect
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectResponse {
    pub schema_version: SchemaVersion,
    pub worker_id: WorkerId,
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub snapshot_digest: SnapshotDigest,
    pub policy_digest: PolicyDigest,
    pub active_task: u32,
    pub prefetch_pending: u32,
    pub max_active_task: u32,
    pub prefetch_per_worker: u32,
    pub disk_free_bytes: u64,
    pub draining: bool,
}

// ---------------------------------------------------------------------------
// forecast
// ---------------------------------------------------------------------------

/// Concurrency caps the real executor applies to a node's group, in the three
/// places a command can sit (spec chapter 9: 登録中・待機中・実行中). `None` is
/// no cap on that stage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyLimits {
    #[serde(default)]
    pub max_registered: Option<u32>,
    #[serde(default)]
    pub max_waiting: Option<u32>,
    #[serde(default)]
    pub max_running: Option<u32>,
}

/// Ties a plan node to the real thing: which attempt and node it is, and the
/// ledger entry once it is registered. A registered node replaces its
/// provisional demand, so this is what prevents double counting (spec chapter 9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Correlation {
    pub attempt_id: AttemptId,
    pub node_id: NodeId,
    /// The real ledger entry, once the node has been registered.
    #[serde(default)]
    pub ledger_entry: Option<String>,
}

/// One node of a [`WorkloadPlan`] (spec chapter 9 "WorkloadPlan"). Every
/// number is `None` when unknown; an absent value is never read as zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadNode {
    pub node_id: NodeId,
    #[serde(default)]
    pub depends_on: Vec<NodeId>,
    pub priority: i32,
    /// CPU weight for admission.
    #[serde(default)]
    pub weight: Option<u32>,
    #[serde(default)]
    pub predicted_rss_bytes: Option<u64>,
    #[serde(default)]
    pub predicted_runtime_ms: Option<u64>,
    /// Preparation outside admission (transfer etc.).
    #[serde(default)]
    pub external_delay_ms: Option<u64>,
    /// Relative time until the node may register.
    #[serde(default)]
    pub ready_in_ms: Option<u64>,
    #[serde(default)]
    pub concurrency: ConcurrencyLimits,
    pub correlation: Correlation,
    /// A shared setup builder the node waits for.
    #[serde(default)]
    pub shared_builder: Option<BuilderId>,
}

/// A node whose correlation does not match its plan.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("node `{node_id}` correlation does not match its plan or node id")]
pub struct PlanError {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadPlan {
    pub attempt_id: AttemptId,
    pub plan_digest: PlanDigest,
    pub expansion_seq: u64,
    pub nodes: Vec<WorkloadNode>,
}

impl WorkloadPlan {
    /// Every node's correlation names this plan's attempt and its own node id.
    pub fn check(&self) -> Result<(), PlanError> {
        for n in &self.nodes {
            if n.correlation.attempt_id != self.attempt_id || n.correlation.node_id != n.node_id {
                return Err(PlanError {
                    node_id: n.node_id.clone(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub workload: WorkloadPlan,
    /// Accepted but unacknowledged work the coordinator is sending elsewhere
    /// to this worker (the overlay); counted once.
    #[serde(default)]
    pub pending: Vec<WorkloadPlan>,
}

/// Which nodes of which plan the worker already accounts for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    pub attempt_id: AttemptId,
    pub plan_digest: PlanDigest,
    pub expansion_seq: u64,
    pub node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// Where the runtime estimate comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSource {
    /// From recorded history.
    Measured,
    /// From the configured fallback; ranking only, never shown as ETA.
    Fallback,
    Unknown,
}

/// Why a forecast value is missing or weak (spec chapter 9 "不確実な予測").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Uncertainty {
    NoHistory,
    UnknownRss,
    /// A running entry went past its history.
    Overrun,
    /// Depends on a runtime condition not yet known.
    Conditional,
    /// A dynamic matrix has not expanded yet.
    DynamicMatrix,
    /// The live memory guard blocks admission.
    MemoryBlocked,
    StaleSample,
    ToolchainChanged,
}

/// Which [`ForecastTarget`] value a reason is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastField {
    ReadyInMs,
    QueueWaitAfterReadyMs,
    ExecutionMs,
    FinishInMs,
}

/// Why the node would currently wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    MemoryBlocked,
    ConcurrencyLimit,
    QueueAhead,
    ExternalDelay,
    SharedBuilder,
}

/// The forecast for the requested workload on this worker.
///
/// A value that cannot be computed is `null`, and `null_reasons` maps the field
/// to the reason. An existing entry running past its history is
/// `overrun`, never rounded to zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastTarget {
    pub plan_digest: PlanDigest,
    pub ready_in_ms: Option<u64>,
    pub queue_wait_after_ready_ms: Option<u64>,
    pub execution_ms: Option<u64>,
    pub finish_in_ms: Option<u64>,
    pub confidence: Confidence,
    pub runtime_source: RuntimeSource,
    #[serde(default)]
    pub history_count: u32,
    pub conditional: bool,
    #[serde(default)]
    pub overrun: bool,
    /// Present while the node would currently wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_reason: Option<WaitReason>,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub null_reasons: BTreeMap<ForecastField, Uncertainty>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastResponse {
    pub schema_version: SchemaVersion,
    pub worker_id: WorkerId,
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub snapshot_digest: SnapshotDigest,
    pub policy_digest: PolicyDigest,
    pub ttl_ms: u64,
    pub coverage: Vec<Coverage>,
    pub target: ForecastTarget,
}

// ---------------------------------------------------------------------------
// attempts
// ---------------------------------------------------------------------------

/// One command node of an attempt. `argv` is structured data; it is never
/// joined into a shell string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub node_id: NodeId,
    pub argv: Vec<String>,
    /// Relative to the attempt workspace.
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub depends_on: Vec<NodeId>,
    /// Execution timeout of this command; overrides the payload's
    /// `command_timeout_ms`. Queue wait never counts toward it.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// What a worker persists on acceptance. Its canonical JSON is hashed into
/// `payload_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptPayload {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub generation: Generation,
    pub source_id: SourceId,
    pub plan_digest: PlanDigest,
    pub manifest_digest: ManifestDigest,
    #[serde(default)]
    pub permit_id: Option<PermitId>,
    pub commands: Vec<CommandSpec>,
    pub lease_ms: u64,
    /// Default execution timeout per command (`CommandSpec::timeout_ms`
    /// overrides it). Queue wait is not part of it.
    #[serde(default)]
    pub command_timeout_ms: Option<u64>,
    /// Deadline for the whole task across preparation, waiting, running and
    /// collection; the termination records which phase it fired in.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    /// How long the attempt may wait for resources before it times out.
    #[serde(default)]
    pub resource_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    pub has_secret_bundle: bool,
}

/// Submit an attempt. If the attempt was retired, or cancelled before this
/// request arrived (a tombstone exists), the worker refuses it with `RETIRED`
/// and never spawns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitAttemptRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    /// Must equal `payload_hash(&payload)`.
    pub payload_hash: PayloadHash,
    pub payload: AttemptPayload,
}

impl SubmitAttemptRequest {
    /// Build a request with the correct `payload_hash`.
    pub fn new(
        request_id: RequestId,
        coordinator_epoch: CoordinatorEpoch,
        attempt_id: AttemptId,
        payload: AttemptPayload,
    ) -> Result<Self, CanonicalError> {
        Ok(SubmitAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id,
            coordinator_epoch,
            attempt_id,
            payload_hash: payload_hash(&payload)?,
            payload,
        })
    }

    /// True when `payload_hash` matches the payload.
    pub fn hash_matches(&self) -> Result<bool, CanonicalError> {
        Ok(self.payload_hash == payload_hash(&self.payload)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitAttemptResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    pub state: AttemptState,
    pub receipts: Receipts,
    /// True when an existing attempt with the same hash was returned.
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStatus {
    pub node_id: NodeId,
    pub state: StepState,
}

/// How one node of an attempt ended (or that it did not start).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeState {
    Succeeded,
    Failed,
    /// Not run on purpose; an explicit reason is required, so a missing result
    /// is never read as skipped.
    Skipped {
        reason: String,
    },
    Cancelled,
    TimedOut,
    Interrupted,
    /// The attempt ended before this node began.
    NotStarted,
}

/// The result of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeResult {
    pub node_id: NodeId,
    pub state: NodeState,
    #[serde(default)]
    pub exit: Option<Exit>,
}

/// The worker's view of one attempt.
///
/// `state` and `termination` always agree: `Terminal` has exactly one
/// termination and every other state has none. Deserialization enforces it;
/// [`AttemptStatus::check`] re-checks a value built by hand.
///
/// A retired attempt keeps answering `get_attempt` with this status and
/// `retired: true`; it is never `NOT_FOUND`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub struct AttemptStatus {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub generation: Generation,
    /// The boot and agent incarnation that produced this status.
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub state: AttemptState,
    #[serde(default)]
    pub receipts: Receipts,
    /// When the user command was started (UTC ms, diagnostic). This, not a
    /// receipt, says whether the command was launched: `running` may never be
    /// observed for a very short command.
    #[serde(default)]
    pub spawned_at_ms: Option<u64>,
    pub observation: Observation,
    /// Present exactly when `state` is `Terminal`.
    #[serde(default)]
    pub termination: Option<Termination>,
    #[serde(default)]
    pub nodes: Vec<NodeResult>,
    #[serde(default)]
    pub steps: Vec<StepStatus>,
    /// A retire tombstone exists for this attempt.
    #[serde(default)]
    pub retired: bool,
    #[serde(default)]
    pub log_truncated: bool,
    #[serde(default)]
    pub log_dropped_bytes: u64,
}

impl AttemptStatus {
    /// `state` is `Terminal` if and only if `termination` is present.
    pub fn check(&self) -> Result<(), TerminationStateError> {
        check_termination(self.state, self.termination.as_ref())
    }
}

impl Serialize for AttemptStatus {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        AttemptStatus::serialize(self, s)
    }
}

impl<'de> Deserialize<'de> for AttemptStatus {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let status = AttemptStatus::deserialize(d)?;
        status.check().map_err(D::Error::custom)?;
        Ok(status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetAttemptRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub attempt_id: AttemptId,
}

pub type GetAttemptResponse = AttemptStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListAttemptsRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    #[serde(default)]
    pub states: Option<Vec<AttemptState>>,
    #[serde(default)]
    pub include_terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListAttemptsResponse {
    pub schema_version: SchemaVersion,
    pub attempts: Vec<AttemptStatus>,
}

/// The ordering key of a lease renewal: `(coordinator_epoch, lease_seq)`,
/// compared lexicographically (the derived order follows the field order, which
/// is therefore load-bearing). A new coordinator epoch restarts `lease_seq`, and
/// its renewals still win over every renewal of an older epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LeaseKey {
    pub coordinator_epoch: CoordinatorEpoch,
    pub lease_seq: u64,
}

impl LeaseKey {
    /// True when a renewal with this key may extend the lease, given the
    /// highest key already applied. Only a strictly greater key extends:
    /// a resend of the same key and any smaller key do not.
    pub fn extends(self, applied: Option<LeaseKey>) -> bool {
        applied.is_none_or(|current| self > current)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewLeaseRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    /// Together with `coordinator_epoch` the ordering key
    /// ([`RenewLeaseRequest::key`]). A replay never extends the lease again.
    pub lease_seq: u64,
    pub lease_ms: u64,
}

impl RenewLeaseRequest {
    pub fn key(&self) -> LeaseKey {
        LeaseKey {
            coordinator_epoch: self.coordinator_epoch,
            lease_seq: self.lease_seq,
        }
    }
}

/// A renewal that is not newer than one already applied is answered normally
/// with `extended: false`; the lease is left as it was. An attempt that can no
/// longer be renewed (expired, cancelling, finalizing, terminal or retired) is
/// answered with the `LEASE_NOT_RENEWABLE` error, never by reviving it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewLeaseResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub lease_seq: u64,
    /// This request moved the lease deadline.
    pub extended: bool,
    pub lease_remaining_ms: u64,
    pub state: AttemptState,
}

/// Cancel an attempt. If the attempt has not arrived at the worker yet, the
/// worker still records the cancel by writing a tombstone, and the submit that
/// arrives later is refused with `RETIRED`: a cancelled attempt never spawns
/// late. The response then has `tombstone: true` and no `state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAttemptRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Cancel acceptance and cancel completion are different answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelPhase {
    Accepted,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAttemptResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub phase: CancelPhase,
    /// `None` only when the attempt never arrived and just a tombstone was
    /// written; no state is invented for it.
    #[serde(default)]
    pub state: Option<AttemptState>,
    /// A tombstone was written for an attempt that had not arrived.
    #[serde(default)]
    pub tombstone: bool,
}

/// Retire an attempt: write the tombstone (kept for the cluster's lifetime) and
/// confirm it is not running. Afterwards `get_attempt` answers `retired: true`
/// and any late submit is refused with `RETIRED`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireAttemptRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    pub generation: Generation,
}

/// What the worker could confirm when writing the retire tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetireStatus {
    /// Never started; a late submit is refused.
    NotStarted,
    /// Was running and is confirmed stopped.
    Stopped,
    AlreadyTerminal,
    /// Not confirmed; the attempt stays unknown on the coordinator.
    Unconfirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireAttemptResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub tombstone: bool,
    pub status: RetireStatus,
}

// ---------------------------------------------------------------------------
// events, logs, results
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEventsRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    #[serde(default)]
    pub attempt_id: Option<AttemptId>,
    /// Return events with `seq > after_seq`.
    pub after_seq: u64,
    pub limit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    StateChanged,
    ReceiptReached,
    StepChanged,
    LogTruncated,
    LeaseExpired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptEvent {
    pub seq: u64,
    pub attempt_id: AttemptId,
    pub kind: EventKind,
    #[serde(default)]
    pub state: Option<AttemptState>,
    #[serde(default)]
    pub receipt: Option<Receipt>,
    #[serde(default)]
    pub node_id: Option<NodeId>,
    #[serde(default)]
    pub step: Option<StepState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEventsResponse {
    pub schema_version: SchemaVersion,
    pub events: Vec<AttemptEvent>,
    pub next_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLogRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub attempt_id: AttemptId,
    pub node_id: NodeId,
    pub stream: LogStream,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLogResponse {
    pub schema_version: SchemaVersion,
    pub offset: u64,
    pub data: Bytes,
    pub next_offset: u64,
    pub eof: bool,
    pub log_truncated: bool,
    pub dropped_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub name: String,
    /// Relative path.
    pub path: String,
    pub size: u64,
    pub digest: Digest,
    pub source_id: SourceId,
    pub plan_digest: PlanDigest,
    pub attempt_id: AttemptId,
    pub profile_fingerprint: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetResultRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub attempt_id: AttemptId,
    pub generation: Generation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetResultResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    pub termination: Termination,
    #[serde(default)]
    pub nodes: Vec<NodeResult>,
    pub artifacts: Vec<ArtifactRecord>,
    /// Digest over the whole result; echoed back in `ack_collected`.
    pub result_digest: Digest,
}

/// Acknowledge a verified, durably saved result.
///
/// There is no `coordinator_epoch` here on purpose: after the coordinator
/// restarts and adopts an attempt, results produced under an older epoch must
/// still be collected (spec chapter 10), so the acknowledgement is keyed by
/// `attempt_id`, `generation` and `result_digest`, not by epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckCollectedRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    pub result_digest: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckCollectedResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    /// The worker released its collection GC protection.
    pub released: bool,
}

// ---------------------------------------------------------------------------
// blobs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobPutRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub source_id: SourceId,
    /// Digest of the complete blob.
    pub digest: Digest,
    pub total_size: u64,
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobPutResponse {
    pub schema_version: SchemaVersion,
    pub digest: Digest,
    /// Bytes now stored contiguously from 0; the next chunk starts here.
    pub received: u64,
    /// The whole blob is stored and its digest matches.
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobGetRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub digest: Digest,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobGetResponse {
    pub schema_version: SchemaVersion,
    pub digest: Digest,
    pub offset: u64,
    pub total_size: u64,
    pub data: Bytes,
    pub eof: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobVerifyRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub digests: Vec<Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobVerifyResponse {
    pub schema_version: SchemaVersion,
    pub present: Vec<Digest>,
    pub missing: Vec<Digest>,
    pub corrupt: Vec<Digest>,
}

// ---------------------------------------------------------------------------
// secret bundle (chapter 22.3)
// ---------------------------------------------------------------------------

/// `hmac-sha256:<64 lowercase hex>`: the keyed digest of a secret file. A
/// different type from [`Digest`] on purpose: a `sha256:` hash of data can be
/// checked by anyone who guesses the data, an HMAC only by the key holders,
/// and the two must never be taken for one another.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HmacDigest(String);

impl HmacDigest {
    pub const PREFIX: &'static str = "hmac-sha256:";

    pub fn parse(s: &str) -> Result<Self, IdError> {
        let ok = s.strip_prefix(Self::PREFIX).is_some_and(|hex| {
            hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        });
        if ok {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::BadDigest(s.to_string()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HmacDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for HmacDigest {
    type Error = IdError;
    fn try_from(s: String) -> Result<Self, IdError> {
        Self::parse(&s)
    }
}

impl From<HmacDigest> for String {
    fn from(d: HmacDigest) -> String {
        d.0
    }
}

/// One secret file. The worker always places it with mode `0600`. `Debug`
/// never prints the content.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretFile {
    pub path: String,
    /// HMAC digest keyed per cluster; never a plain hash of the content.
    pub hmac_digest: HmacDigest,
    pub content: Bytes,
}

impl fmt::Debug for SecretFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretFile")
            .field("path", &self.path)
            .field("hmac_digest", &self.hmac_digest)
            .field("content", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretBundlePutRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    pub files: Vec<SecretFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretBundlePutResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    /// Every file's HMAC digest matched.
    pub verified: bool,
    pub placed: u32,
}

// ---------------------------------------------------------------------------
// environment (chapter 22)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRequirement {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvReportRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub required: Vec<ToolRequirement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolMatch {
    Match,
    Missing,
    VersionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolStatus {
    pub name: String,
    pub required: String,
    #[serde(default)]
    pub installed: Option<String>,
    /// Version manager that would install it (nodenv, rustup, mise, ...).
    #[serde(default)]
    pub adapter: Option<String>,
    pub status: ToolMatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvReportResponse {
    pub schema_version: SchemaVersion,
    pub os: String,
    pub arch: String,
    pub profile_fingerprint: Digest,
    pub tools: Vec<ToolStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvSyncRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub tools: Vec<ToolRequirement>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncOutcome {
    Installed,
    Unchanged,
    Failed,
    /// Dry run: would be installed.
    WouldInstall,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSyncResult {
    pub name: String,
    pub outcome: SyncOutcome,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvSyncResponse {
    pub schema_version: SchemaVersion,
    pub results: Vec<ToolSyncResult>,
}

// ---------------------------------------------------------------------------
// request / response unions and the frame payload
// ---------------------------------------------------------------------------

macro_rules! operations {
    ($($variant:ident($req:ident, $resp:ident)),* $(,)?) => {
        /// Any request, tagged by `op`.
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(tag = "op", rename_all = "snake_case")]
        pub enum Request { $($variant($req)),* }

        /// Any successful response, tagged by `op`.
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(tag = "op", rename_all = "snake_case")]
        pub enum Response { $($variant($resp)),* }

        impl Request {
            pub fn operation(&self) -> Operation {
                match self { $(Request::$variant(_) => Operation::$variant),* }
            }
            pub fn request_id(&self) -> &RequestId {
                match self { $(Request::$variant(r) => &r.request_id),* }
            }
            pub fn schema_version(&self) -> SchemaVersion {
                match self { $(Request::$variant(r) => r.schema_version),* }
            }
            /// The post-`hello` schema check for this request.
            pub fn ensure_current_schema(&self) -> Result<(), UnsupportedSchemaVersion> {
                self.schema_version().ensure_current()
            }
        }

        impl Response {
            pub fn operation(&self) -> Operation {
                match self { $(Response::$variant(_) => Operation::$variant),* }
            }
            pub fn schema_version(&self) -> SchemaVersion {
                match self { $(Response::$variant(r) => r.schema_version),* }
            }
            /// The post-`hello` schema check for this response.
            pub fn ensure_current_schema(&self) -> Result<(), UnsupportedSchemaVersion> {
                self.schema_version().ensure_current()
            }
        }
    };
}

operations! {
    Hello(HelloRequest, HelloResponse),
    Inspect(InspectRequest, InspectResponse),
    Forecast(ForecastRequest, ForecastResponse),
    SubmitAttempt(SubmitAttemptRequest, SubmitAttemptResponse),
    GetAttempt(GetAttemptRequest, GetAttemptResponse),
    ListAttempts(ListAttemptsRequest, ListAttemptsResponse),
    RenewLease(RenewLeaseRequest, RenewLeaseResponse),
    CancelAttempt(CancelAttemptRequest, CancelAttemptResponse),
    RetireAttempt(RetireAttemptRequest, RetireAttemptResponse),
    WatchEvents(WatchEventsRequest, WatchEventsResponse),
    ReadLog(ReadLogRequest, ReadLogResponse),
    GetResult(GetResultRequest, GetResultResponse),
    AckCollected(AckCollectedRequest, AckCollectedResponse),
    BlobPut(BlobPutRequest, BlobPutResponse),
    BlobGet(BlobGetRequest, BlobGetResponse),
    BlobVerify(BlobVerifyRequest, BlobVerifyResponse),
    SecretBundlePut(SecretBundlePutRequest, SecretBundlePutResponse),
    EnvReport(EnvReportRequest, EnvReportResponse),
    EnvSync(EnvSyncRequest, EnvSyncResponse),
}

/// An error reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub schema_version: SchemaVersion,
    /// Absent when the request could not be parsed far enough to know it.
    #[serde(default)]
    pub request_id: Option<RequestId>,
    pub error: ApiError,
}

/// The JSON value inside one protocol frame (the length prefix and transport
/// are the protocol layer's concern).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Request(Request),
    Response {
        request_id: RequestId,
        response: Response,
    },
    Error(ErrorResponse),
}

impl Message {
    /// The request id this message belongs to, if known.
    pub fn request_id(&self) -> Option<&RequestId> {
        match self {
            Message::Request(r) => Some(r.request_id()),
            Message::Response { request_id, .. } => Some(request_id),
            Message::Error(e) => e.request_id.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::state::{Outcome, TimeoutInfo, TimeoutKind, TimeoutPhase};

    const SV: SchemaVersion = SchemaVersion::CURRENT;

    fn rid() -> RequestId {
        RequestId::generate()
    }
    fn aid() -> AttemptId {
        AttemptId::generate()
    }
    fn node(s: &str) -> NodeId {
        NodeId::parse(s).unwrap()
    }
    fn dg(s: &str) -> Digest {
        Digest::of_bytes(s.as_bytes())
    }
    fn hdg(s: &str) -> HmacDigest {
        let d = Digest::of_bytes(s.as_bytes());
        HmacDigest::parse(&format!("hmac-sha256:{}", &d.as_str()["sha256:".len()..])).unwrap()
    }
    fn worker() -> WorkerId {
        WorkerId::parse("mini-a").unwrap()
    }
    fn boot() -> WorkerBootId {
        WorkerBootId::parse("boot-1").unwrap()
    }

    fn plan() -> WorkloadPlan {
        let attempt_id = aid();
        WorkloadPlan {
            attempt_id: attempt_id.clone(),
            plan_digest: dg("plan"),
            expansion_seq: 1,
            nodes: vec![WorkloadNode {
                node_id: node("test"),
                depends_on: vec![node("setup")],
                priority: 0,
                weight: Some(2),
                predicted_rss_bytes: Some(1 << 20),
                predicted_runtime_ms: None,
                external_delay_ms: Some(10),
                ready_in_ms: None,
                concurrency: ConcurrencyLimits {
                    max_registered: Some(4),
                    max_waiting: None,
                    max_running: Some(2),
                },
                correlation: Correlation {
                    attempt_id,
                    node_id: node("test"),
                    ledger_entry: Some("entry-1".into()),
                },
                shared_builder: Some(BuilderId::generate()),
            }],
        }
    }

    fn payload() -> AttemptPayload {
        AttemptPayload {
            run_id: RunId::generate(),
            task_id: TaskId::generate(),
            generation: Generation(1),
            source_id: SourceId::generate(),
            plan_digest: dg("plan"),
            manifest_digest: dg("manifest"),
            permit_id: Some(PermitId::generate()),
            commands: vec![CommandSpec {
                node_id: node("test"),
                argv: vec!["cargo".into(), "test".into()],
                cwd: ".".into(),
                env: BTreeMap::from([("A".to_string(), "1".to_string())]),
                depends_on: vec![],
                timeout_ms: Some(1000),
            }],
            lease_ms: 60_000,
            command_timeout_ms: Some(900_000),
            deadline_ms: Some(3_600_000),
            resource_wait_timeout_ms: Some(120_000),
            has_secret_bundle: true,
        }
    }

    fn status() -> AttemptStatus {
        AttemptStatus {
            schema_version: SV,
            attempt_id: aid(),
            run_id: RunId::generate(),
            task_id: TaskId::generate(),
            generation: Generation(2),
            worker_boot_id: boot(),
            agent_epoch: AgentEpoch(2),
            state: AttemptState::Active,
            receipts: receipts(&[Receipt::WorkerAccepted, Receipt::Running]),
            spawned_at_ms: Some(1_700_000_000_000),
            observation: Observation::Known,
            termination: None,
            nodes: vec![NodeResult {
                node_id: node("setup"),
                state: NodeState::Succeeded,
                exit: Some(Exit::Code(0)),
            }],
            steps: vec![StepStatus {
                node_id: node("test"),
                state: StepState::WaitingResources,
            }],
            retired: false,
            log_truncated: true,
            log_dropped_bytes: 9,
        }
    }

    fn receipts(reached: &[Receipt]) -> Receipts {
        let mut r = Receipts::default();
        for (i, k) in reached.iter().enumerate() {
            r.mark(*k, 1000 + i as u64);
        }
        r
    }

    fn timed_out() -> Termination {
        Termination::new(
            Outcome::TimedOut,
            Some(Exit::Signal(9)),
            Some(TimeoutInfo {
                kind: TimeoutKind::Command,
                phase: TimeoutPhase::Running,
            }),
        )
        .unwrap()
    }

    fn terminal_status() -> AttemptStatus {
        AttemptStatus {
            state: AttemptState::Terminal,
            termination: Some(timed_out()),
            retired: true,
            nodes: vec![
                NodeResult {
                    node_id: node("test"),
                    state: NodeState::TimedOut,
                    exit: Some(Exit::Signal(9)),
                },
                NodeResult {
                    node_id: node("lint"),
                    state: NodeState::Skipped {
                        reason: "fail-fast".into(),
                    },
                    exit: None,
                },
                NodeResult {
                    node_id: node("deploy"),
                    state: NodeState::NotStarted,
                    exit: None,
                },
            ],
            ..status()
        }
    }

    fn artifact() -> ArtifactRecord {
        ArtifactRecord {
            name: "out".into(),
            path: "out/a.txt".into(),
            size: 3,
            digest: dg("a"),
            source_id: SourceId::generate(),
            plan_digest: dg("plan"),
            attempt_id: aid(),
            profile_fingerprint: dg("profile"),
        }
    }

    fn target() -> ForecastTarget {
        ForecastTarget {
            plan_digest: dg("plan"),
            ready_in_ms: Some(10_000),
            queue_wait_after_ready_ms: Some(20_000),
            execution_ms: None,
            finish_in_ms: None,
            confidence: Confidence::Medium,
            runtime_source: RuntimeSource::Measured,
            history_count: 4,
            conditional: true,
            overrun: false,
            wait_reason: Some(WaitReason::MemoryBlocked),
            assumptions: vec!["no new external load".into()],
            null_reasons: BTreeMap::from([
                (ForecastField::ExecutionMs, Uncertainty::NoHistory),
                (ForecastField::FinishInMs, Uncertainty::Overrun),
            ]),
        }
    }

    fn requests() -> Vec<Request> {
        let aid = aid();
        vec![
            Request::Hello(HelloRequest {
                schema_version: SV,
                request_id: rid(),
                cluster_id: ClusterId::generate(),
                coordinator_id: CoordinatorId::generate(),
                coordinator_epoch: CoordinatorEpoch(3),
                protocol_versions: vec![1],
                features: vec!["secret_bundle".into()],
            }),
            Request::Inspect(InspectRequest {
                schema_version: SV,
                request_id: rid(),
            }),
            Request::Forecast(ForecastRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                workload: plan(),
                pending: vec![plan()],
            }),
            Request::SubmitAttempt(
                SubmitAttemptRequest::new(rid(), CoordinatorEpoch(3), aid.clone(), payload())
                    .unwrap(),
            ),
            Request::GetAttempt(GetAttemptRequest {
                schema_version: SV,
                request_id: rid(),
                attempt_id: aid.clone(),
            }),
            Request::ListAttempts(ListAttemptsRequest {
                schema_version: SV,
                request_id: rid(),
                states: Some(vec![AttemptState::Active]),
                include_terminal: false,
            }),
            Request::RenewLease(RenewLeaseRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                attempt_id: aid.clone(),
                generation: Generation(1),
                lease_seq: 7,
                lease_ms: 60_000,
            }),
            Request::CancelAttempt(CancelAttemptRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                attempt_id: aid.clone(),
                generation: Generation(1),
                reason: Some("user".into()),
            }),
            Request::RetireAttempt(RetireAttemptRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                attempt_id: aid.clone(),
                generation: Generation(1),
            }),
            Request::WatchEvents(WatchEventsRequest {
                schema_version: SV,
                request_id: rid(),
                attempt_id: Some(aid.clone()),
                after_seq: 5,
                limit: 100,
            }),
            Request::ReadLog(ReadLogRequest {
                schema_version: SV,
                request_id: rid(),
                attempt_id: aid.clone(),
                node_id: node("test"),
                stream: LogStream::Stderr,
                offset: 10,
                max_bytes: 4096,
            }),
            Request::GetResult(GetResultRequest {
                schema_version: SV,
                request_id: rid(),
                attempt_id: aid.clone(),
                generation: Generation(1),
            }),
            Request::AckCollected(AckCollectedRequest {
                schema_version: SV,
                request_id: rid(),
                attempt_id: aid.clone(),
                generation: Generation(1),
                result_digest: dg("result"),
            }),
            Request::BlobPut(BlobPutRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                source_id: SourceId::generate(),
                digest: dg("blob"),
                total_size: 4,
                offset: 0,
                data: Bytes(vec![0, 1, 255, 3]),
            }),
            Request::BlobGet(BlobGetRequest {
                schema_version: SV,
                request_id: rid(),
                digest: dg("blob"),
                offset: 0,
                max_bytes: 10,
            }),
            Request::BlobVerify(BlobVerifyRequest {
                schema_version: SV,
                request_id: rid(),
                digests: vec![dg("a"), dg("b")],
            }),
            Request::SecretBundlePut(SecretBundlePutRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                attempt_id: aid.clone(),
                generation: Generation(1),
                files: vec![SecretFile {
                    path: ".env".into(),
                    hmac_digest: hdg("hmac"),
                    content: Bytes(b"K=V".to_vec()),
                }],
            }),
            Request::EnvReport(EnvReportRequest {
                schema_version: SV,
                request_id: rid(),
                required: vec![ToolRequirement {
                    name: "node".into(),
                    version: "22".into(),
                }],
            }),
            Request::EnvSync(EnvSyncRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(3),
                tools: vec![ToolRequirement {
                    name: "node".into(),
                    version: "22".into(),
                }],
                dry_run: true,
            }),
        ]
    }

    fn responses() -> Vec<Response> {
        vec![
            Response::Hello(HelloResponse {
                schema_version: SV,
                cluster_id: ClusterId::generate(),
                protocol_version: 1,
                features: vec![],
                worker_id: worker(),
                worker_boot_id: boot(),
                agent_epoch: AgentEpoch(2),
                profile: "default".into(),
                profile_fingerprint: dg("p"),
                coordinator_epoch: CoordinatorEpoch(3),
            }),
            Response::Inspect(InspectResponse {
                schema_version: SV,
                worker_id: worker(),
                worker_boot_id: boot(),
                agent_epoch: AgentEpoch(2),
                snapshot_digest: dg("s"),
                policy_digest: dg("p"),
                active_task: 1,
                prefetch_pending: 0,
                max_active_task: 2,
                prefetch_per_worker: 1,
                disk_free_bytes: 1 << 30,
                draining: false,
            }),
            Response::Forecast(ForecastResponse {
                schema_version: SV,
                worker_id: worker(),
                worker_boot_id: boot(),
                agent_epoch: AgentEpoch(2),
                snapshot_digest: dg("s"),
                policy_digest: dg("p"),
                ttl_ms: 2000,
                coverage: vec![Coverage {
                    attempt_id: aid(),
                    plan_digest: dg("prev"),
                    expansion_seq: 1,
                    node_ids: vec![node("setup"), node("test")],
                }],
                target: target(),
            }),
            Response::SubmitAttempt(SubmitAttemptResponse {
                schema_version: SV,
                attempt_id: aid(),
                generation: Generation(1),
                state: AttemptState::Accepted,
                receipts: receipts(&[Receipt::CoordinatorAccepted, Receipt::WorkerAccepted]),
                duplicate: true,
            }),
            Response::GetAttempt(status()),
            Response::ListAttempts(ListAttemptsResponse {
                schema_version: SV,
                attempts: vec![status(), terminal_status()],
            }),
            Response::RenewLease(RenewLeaseResponse {
                schema_version: SV,
                attempt_id: aid(),
                lease_seq: 7,
                extended: true,
                lease_remaining_ms: 59_000,
                state: AttemptState::Active,
            }),
            Response::CancelAttempt(CancelAttemptResponse {
                schema_version: SV,
                attempt_id: aid(),
                phase: CancelPhase::Accepted,
                state: Some(AttemptState::Cancelling),
                tombstone: false,
            }),
            Response::RetireAttempt(RetireAttemptResponse {
                schema_version: SV,
                attempt_id: aid(),
                tombstone: true,
                status: RetireStatus::NotStarted,
            }),
            Response::WatchEvents(WatchEventsResponse {
                schema_version: SV,
                events: vec![AttemptEvent {
                    seq: 6,
                    attempt_id: aid(),
                    kind: EventKind::StepChanged,
                    state: Some(AttemptState::Active),
                    receipt: Some(Receipt::Running),
                    node_id: Some(node("test")),
                    step: Some(StepState::Running),
                }],
                next_seq: 6,
            }),
            Response::ReadLog(ReadLogResponse {
                schema_version: SV,
                offset: 10,
                data: Bytes(vec![0xff, 0xfe]),
                next_offset: 12,
                eof: false,
                log_truncated: false,
                dropped_bytes: 0,
            }),
            Response::GetResult(GetResultResponse {
                schema_version: SV,
                attempt_id: aid(),
                generation: Generation(1),
                termination: timed_out(),
                nodes: terminal_status().nodes,
                artifacts: vec![artifact()],
                result_digest: dg("r"),
            }),
            Response::AckCollected(AckCollectedResponse {
                schema_version: SV,
                attempt_id: aid(),
                released: true,
            }),
            Response::BlobPut(BlobPutResponse {
                schema_version: SV,
                digest: dg("blob"),
                received: 4,
                complete: true,
            }),
            Response::BlobGet(BlobGetResponse {
                schema_version: SV,
                digest: dg("blob"),
                offset: 0,
                total_size: 4,
                data: Bytes(vec![1, 2, 3, 4]),
                eof: true,
            }),
            Response::BlobVerify(BlobVerifyResponse {
                schema_version: SV,
                present: vec![dg("a")],
                missing: vec![dg("b")],
                corrupt: vec![dg("c")],
            }),
            Response::SecretBundlePut(SecretBundlePutResponse {
                schema_version: SV,
                attempt_id: aid(),
                verified: true,
                placed: 1,
            }),
            Response::EnvReport(EnvReportResponse {
                schema_version: SV,
                os: "macos".into(),
                arch: "aarch64".into(),
                profile_fingerprint: dg("p"),
                tools: vec![ToolStatus {
                    name: "node".into(),
                    required: "22".into(),
                    installed: Some("20".into()),
                    adapter: Some("mise".into()),
                    status: ToolMatch::VersionMismatch,
                }],
            }),
            Response::EnvSync(EnvSyncResponse {
                schema_version: SV,
                results: vec![ToolSyncResult {
                    name: "node".into(),
                    outcome: SyncOutcome::WouldInstall,
                    message: None,
                }],
            }),
        ]
    }

    #[test]
    fn every_operation_has_a_request_and_a_response() {
        let reqs = requests();
        let resps = responses();
        assert_eq!(reqs.len(), Operation::ALL.len());
        assert_eq!(resps.len(), Operation::ALL.len());
        for (i, op) in Operation::ALL.iter().enumerate() {
            assert_eq!(reqs[i].operation(), *op);
            assert_eq!(resps[i].operation(), *op);
        }
    }

    #[test]
    fn requests_roundtrip_with_op_tag_and_schema_version() {
        for req in requests() {
            let v = serde_json::to_value(&req).unwrap();
            assert_eq!(v["schema_version"], 1);
            assert_eq!(v["request_id"], req.request_id().as_str());
            assert_eq!(v["op"], serde_json::to_value(req.operation()).unwrap());
            assert_eq!(req.schema_version(), SV);
            let back: Request = serde_json::from_value(v).unwrap();
            assert_eq!(back, req);
        }
    }

    #[test]
    fn responses_roundtrip_with_op_tag_and_schema_version() {
        for resp in responses() {
            let v = serde_json::to_value(&resp).unwrap();
            assert_eq!(v["schema_version"], 1);
            assert_eq!(v["op"], serde_json::to_value(resp.operation()).unwrap());
            assert_eq!(resp.schema_version(), SV);
            let back: Response = serde_json::from_value(v).unwrap();
            assert_eq!(back, resp);
        }
    }

    #[test]
    fn messages_roundtrip_and_expose_request_id() {
        let req = requests().remove(0);
        let id = req.request_id().clone();
        let m = Message::Request(req);
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "request");
        assert_eq!(v["op"], "hello");
        assert_eq!(serde_json::from_value::<Message>(v).unwrap(), m);
        assert_eq!(m.request_id(), Some(&id));

        let m = Message::Response {
            request_id: id.clone(),
            response: responses().remove(0),
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "response");
        assert_eq!(v["response"]["op"], "hello");
        assert_eq!(serde_json::from_value::<Message>(v).unwrap(), m);
        assert_eq!(m.request_id(), Some(&id));

        let m = Message::Error(ErrorResponse {
            schema_version: SV,
            request_id: Some(id.clone()),
            error: ApiError::new(ErrorCode::StaleEpoch, "old epoch"),
        });
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["error"]["code"], "STALE_EPOCH");
        assert_eq!(serde_json::from_value::<Message>(v).unwrap(), m);
        assert_eq!(m.request_id(), Some(&id));

        let anonymous = Message::Error(ErrorResponse {
            schema_version: SV,
            request_id: None,
            error: ApiError::new(ErrorCode::InvalidInput, "bad frame"),
        });
        assert_eq!(anonymous.request_id(), None);
        let v = serde_json::to_value(&anonymous).unwrap();
        assert_eq!(serde_json::from_value::<Message>(v).unwrap(), anonymous);
    }

    #[test]
    fn schema_version_deserializes_any_number_and_is_checked_separately() {
        assert_eq!(SchemaVersion::default(), SchemaVersion::CURRENT);
        for n in [0u32, 1, 2, 99] {
            let v: SchemaVersion = serde_json::from_str(&n.to_string()).unwrap();
            assert_eq!(v, SchemaVersion(n));
            assert_eq!(serde_json::to_string(&v).unwrap(), n.to_string());
            assert_eq!(v.ensure_current().is_ok(), n == 1, "{n}");
        }
        let e = SchemaVersion(2).ensure_current().unwrap_err();
        assert_eq!(
            e,
            UnsupportedSchemaVersion {
                got: 2,
                expected: 1
            }
        );
        assert!(e.to_string().contains("unsupported schema_version 2"));
        assert!(serde_json::from_str::<SchemaVersion>("-1").is_err());
        let mut v = serde_json::to_value(requests().remove(1)).unwrap();
        v.as_object_mut().unwrap().remove("schema_version");
        assert!(serde_json::from_value::<Request>(v).is_err());
    }

    #[test]
    fn other_versions_still_parse_so_hello_and_errors_can_be_read() {
        let bump = |mut v: Value| {
            v["schema_version"] = json!(2);
            v
        };
        // hello request and response
        let hello_req = requests().remove(0);
        let back: Request =
            serde_json::from_value(bump(serde_json::to_value(&hello_req).unwrap())).unwrap();
        assert_eq!(back.schema_version(), SchemaVersion(2));
        assert_eq!(
            back.ensure_current_schema().unwrap_err(),
            UnsupportedSchemaVersion {
                got: 2,
                expected: 1
            }
        );
        assert!(hello_req.ensure_current_schema().is_ok());
        let hello_resp = responses().remove(0);
        let back: Response =
            serde_json::from_value(bump(serde_json::to_value(&hello_resp).unwrap())).unwrap();
        assert!(back.ensure_current_schema().is_err());
        assert!(hello_resp.ensure_current_schema().is_ok());
        // error replies, bare and framed
        let err = ErrorResponse {
            schema_version: SV,
            request_id: None,
            error: ApiError::new(ErrorCode::ProtocolIncompatible, "old"),
        };
        let back: ErrorResponse =
            serde_json::from_value(bump(serde_json::to_value(&err).unwrap())).unwrap();
        assert_eq!(back.schema_version, SchemaVersion(2));
        assert_eq!(back.error.code, ErrorCode::ProtocolIncompatible);
        let framed = bump(serde_json::to_value(Message::Error(err)).unwrap());
        assert!(matches!(
            serde_json::from_value::<Message>(framed).unwrap(),
            Message::Error(_)
        ));
    }

    #[test]
    fn idempotency_is_pinned_per_operation() {
        use Idempotency::*;
        use Operation::*;
        let expect = [
            (Hello, ReadOnly),
            (Inspect, ReadOnly),
            (Forecast, ReadOnly),
            (SubmitAttempt, AttemptAndPayload),
            (GetAttempt, ReadOnly),
            (ListAttempts, ReadOnly),
            (RenewLease, RequestId),
            (CancelAttempt, RequestId),
            (RetireAttempt, RequestId),
            (WatchEvents, ReadOnly),
            (ReadLog, ReadOnly),
            (GetResult, ReadOnly),
            (AckCollected, RequestId),
            (BlobPut, RequestId),
            (BlobGet, ReadOnly),
            (BlobVerify, ReadOnly),
            (SecretBundlePut, RequestId),
            (EnvReport, ReadOnly),
            (EnvSync, RequestId),
        ];
        assert_eq!(expect.len(), Operation::ALL.len());
        for (op, idem) in expect {
            assert_eq!(op.idempotency(), idem, "{op:?}");
        }
        assert_eq!(
            serde_json::to_string(&Idempotency::AttemptAndPayload).unwrap(),
            "\"attempt_and_payload\""
        );
        assert_eq!(
            serde_json::to_string(&Operation::SecretBundlePut).unwrap(),
            "\"secret_bundle_put\""
        );
    }

    #[test]
    fn canonical_json_sorts_keys_and_is_compact() {
        let v = json!({"b": [1, {"z": null, "a": true}], "a": "x\"y", "c": -3});
        assert_eq!(
            canonical_json(&v).unwrap(),
            r#"{"a":"x\"y","b":[1,{"a":true,"z":null}],"c":-3}"#
        );
        assert_eq!(canonical_json(&json!([])).unwrap(), "[]");
        assert_eq!(canonical_json(&json!({})).unwrap(), "{}");
        assert_eq!(canonical_json(&json!(false)).unwrap(), "false");
        assert_eq!(
            canonical_json(&json!(18446744073709551615u64)).unwrap(),
            "18446744073709551615"
        );
    }

    #[test]
    fn canonical_json_rejects_floats() {
        let e = canonical_json(&json!({"a": 1.5})).unwrap_err();
        assert!(matches!(e, CanonicalError::Float(_)));
        assert!(e.to_string().contains("1.5"));
    }

    #[test]
    fn canonical_json_ignores_key_insertion_order() {
        let a: Value = serde_json::from_str(r#"{"x":1,"y":{"q":1,"p":2}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"y":{"p":2,"q":1},"x":1}"#).unwrap();
        assert_eq!(canonical_json(&a).unwrap(), canonical_json(&b).unwrap());
        assert_eq!(payload_hash(&a).unwrap(), payload_hash(&b).unwrap());
    }

    #[test]
    fn payload_hash_is_deterministic_and_content_sensitive() {
        let p = payload();
        let h = payload_hash(&p).unwrap();
        assert_eq!(h, payload_hash(&p.clone()).unwrap());
        let mut q = p.clone();
        q.lease_ms += 1;
        assert_ne!(h, payload_hash(&q).unwrap());
        // known vector pins the canonical form
        assert_eq!(
            payload_hash(&json!({"b": 1, "a": 2})).unwrap(),
            Digest::of_bytes(br#"{"a":2,"b":1}"#)
        );
    }

    #[test]
    fn submit_request_carries_a_matching_hash() {
        let req = SubmitAttemptRequest::new(rid(), CoordinatorEpoch(1), aid(), payload()).unwrap();
        assert_eq!(req.schema_version, SV);
        assert!(req.hash_matches().unwrap());
        assert_eq!(req.payload_hash, payload_hash(&req.payload).unwrap());
        let mut tampered = req.clone();
        tampered.payload.lease_ms += 1;
        assert!(!tampered.hash_matches().unwrap());
    }

    #[test]
    fn content_hash_ignores_request_id_only() {
        let a = Request::Inspect(InspectRequest {
            schema_version: SV,
            request_id: rid(),
        });
        let b = Request::Inspect(InspectRequest {
            schema_version: SV,
            request_id: rid(),
        });
        assert_ne!(a.request_id(), b.request_id());
        assert_eq!(content_hash(&a).unwrap(), content_hash(&b).unwrap());

        let hello = |epoch| {
            Request::Hello(HelloRequest {
                schema_version: SV,
                request_id: rid(),
                cluster_id: ClusterId::parse("cl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                coordinator_id: CoordinatorId::parse("co_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                coordinator_epoch: CoordinatorEpoch(epoch),
                protocol_versions: vec![1],
                features: vec![],
            })
        };
        assert_eq!(
            content_hash(&hello(1)).unwrap(),
            content_hash(&hello(1)).unwrap()
        );
        assert_ne!(
            content_hash(&hello(1)).unwrap(),
            content_hash(&hello(99)).unwrap()
        );
    }

    fn secret_put(content: &[u8], path: &str, hmac: &str, epoch: u64) -> Request {
        Request::SecretBundlePut(SecretBundlePutRequest {
            schema_version: SV,
            request_id: rid(),
            coordinator_epoch: CoordinatorEpoch(epoch),
            attempt_id: AttemptId::parse("att_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            generation: Generation(1),
            files: vec![SecretFile {
                path: path.into(),
                hmac_digest: hdg(hmac),
                content: Bytes(content.to_vec()),
            }],
        })
    }

    #[test]
    fn secret_bundle_hash_never_covers_the_plaintext() {
        let secret = b"TOPSECRET=hunter2";
        let req = secret_put(secret, ".env", "h1", 3);
        let input = content_hash_input(&req).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(secret);
        assert!(!input.contains(&b64) && !input.contains("hunter2"));
        assert!(!input.contains("\"content\""));
        assert!(input.contains(".env") && input.contains(dg("h1").as_str()));
        // the plain SHA-256 of the content is not in the input either
        assert!(!input.contains(Digest::of_bytes(secret).as_str()));
        // changing only the plaintext leaves the hash alone...
        let base = content_hash(&req).unwrap();
        assert_eq!(
            base,
            content_hash(&secret_put(b"other", ".env", "h1", 3)).unwrap()
        );
        // ...while path, HMAC digest and epoch move it
        assert_ne!(
            base,
            content_hash(&secret_put(secret, ".env.test", "h1", 3)).unwrap()
        );
        assert_ne!(
            base,
            content_hash(&secret_put(secret, ".env", "h2", 3)).unwrap()
        );
        assert_ne!(
            base,
            content_hash(&secret_put(secret, ".env", "h1", 4)).unwrap()
        );
        // the request id is still ignored
        assert_eq!(
            base,
            content_hash(&secret_put(secret, ".env", "h1", 3)).unwrap()
        );
    }

    fn blob_put(data: &[u8], digest: &str, offset: u64, total: u64) -> Request {
        Request::BlobPut(BlobPutRequest {
            schema_version: SV,
            request_id: rid(),
            coordinator_epoch: CoordinatorEpoch(3),
            source_id: SourceId::parse("src_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            digest: dg(digest),
            total_size: total,
            offset,
            data: Bytes(data.to_vec()),
        })
    }

    #[test]
    fn blob_put_hash_covers_digest_offset_and_size_not_data() {
        let req = blob_put(b"PLAINCHUNK", "blob", 0, 10);
        let input = content_hash_input(&req).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"PLAINCHUNK");
        assert!(!input.contains(&b64) && !input.contains("\"data\""));
        for key in ["digest", "offset", "total_size", "source_id"] {
            assert!(input.contains(&format!("\"{key}\"")), "{key}");
        }
        let base = content_hash(&req).unwrap();
        assert_eq!(
            base,
            content_hash(&blob_put(b"different!", "blob", 0, 10)).unwrap()
        );
        assert_ne!(
            base,
            content_hash(&blob_put(b"PLAINCHUNK", "blob2", 0, 10)).unwrap()
        );
        assert_ne!(
            base,
            content_hash(&blob_put(b"PLAINCHUNK", "blob", 5, 10)).unwrap()
        );
        assert_ne!(
            base,
            content_hash(&blob_put(b"PLAINCHUNK", "blob", 0, 11)).unwrap()
        );
    }

    #[test]
    fn other_requests_hash_their_whole_body() {
        // only secret and blob puts are special-cased
        let sync = |dry_run| {
            Request::EnvSync(EnvSyncRequest {
                schema_version: SV,
                request_id: rid(),
                coordinator_epoch: CoordinatorEpoch(1),
                tools: vec![],
                dry_run,
            })
        };
        assert_ne!(
            content_hash(&sync(true)).unwrap(),
            content_hash(&sync(false)).unwrap()
        );
        let text = content_hash_input(&sync(true)).unwrap();
        assert!(text.contains("\"op\":\"env_sync\"") && !text.contains("request_id"));
    }

    #[test]
    fn every_request_has_a_hashable_content() {
        // canonical JSON refuses floats, so every request shape must be integers only
        for req in requests() {
            content_hash(&req).unwrap();
        }
    }

    #[test]
    fn forecast_response_matches_the_spec_example_shape() {
        let v = serde_json::to_value(&responses()[2]).unwrap();
        for key in [
            "schema_version",
            "worker_id",
            "snapshot_digest",
            "policy_digest",
            "ttl_ms",
            "coverage",
            "target",
        ] {
            assert!(v.get(key).is_some(), "{key}");
        }
        let t = &v["target"];
        for key in [
            "plan_digest",
            "ready_in_ms",
            "queue_wait_after_ready_ms",
            "execution_ms",
            "finish_in_ms",
            "confidence",
            "runtime_source",
            "conditional",
            "assumptions",
        ] {
            assert!(t.get(key).is_some(), "{key}");
        }
        assert!(t["execution_ms"].is_null());
        assert_eq!(t["null_reasons"]["execution_ms"], "no_history");
        assert_eq!(t["null_reasons"]["finish_in_ms"], "overrun");
        assert_eq!(t["wait_reason"], "memory_blocked");
        assert_eq!(t["confidence"], "medium");
        assert_eq!(t["runtime_source"], "measured");
        assert_eq!(v["coverage"][0]["expansion_seq"], 1);
    }

    #[test]
    fn forecast_enums_and_defaults() {
        for (s, j) in [
            (RuntimeSource::Measured, "measured"),
            (RuntimeSource::Fallback, "fallback"),
            (RuntimeSource::Unknown, "unknown"),
        ] {
            assert_eq!(serde_json::to_string(&s).unwrap(), format!("\"{j}\""));
            assert_eq!(
                serde_json::from_str::<RuntimeSource>(&format!("\"{j}\"")).unwrap(),
                s
            );
        }
        for (c, j) in [(Confidence::Low, "low"), (Confidence::High, "high")] {
            assert_eq!(serde_json::to_string(&c).unwrap(), format!("\"{j}\""));
            assert_eq!(
                serde_json::from_str::<Confidence>(&format!("\"{j}\"")).unwrap(),
                c
            );
        }
        for (u, j) in [
            (Uncertainty::NoHistory, "no_history"),
            (Uncertainty::UnknownRss, "unknown_rss"),
            (Uncertainty::Overrun, "overrun"),
            (Uncertainty::Conditional, "conditional"),
            (Uncertainty::DynamicMatrix, "dynamic_matrix"),
            (Uncertainty::MemoryBlocked, "memory_blocked"),
            (Uncertainty::StaleSample, "stale_sample"),
            (Uncertainty::ToolchainChanged, "toolchain_changed"),
        ] {
            assert_eq!(serde_json::to_string(&u).unwrap(), format!("\"{j}\""));
            assert_eq!(
                serde_json::from_str::<Uncertainty>(&format!("\"{j}\"")).unwrap(),
                u
            );
        }
        for (w, j) in [
            (WaitReason::MemoryBlocked, "memory_blocked"),
            (WaitReason::ConcurrencyLimit, "concurrency_limit"),
            (WaitReason::QueueAhead, "queue_ahead"),
            (WaitReason::ExternalDelay, "external_delay"),
            (WaitReason::SharedBuilder, "shared_builder"),
        ] {
            assert_eq!(serde_json::to_string(&w).unwrap(), format!("\"{j}\""));
            assert_eq!(
                serde_json::from_str::<WaitReason>(&format!("\"{j}\"")).unwrap(),
                w
            );
        }
        for (f, j) in [
            (ForecastField::ReadyInMs, "ready_in_ms"),
            (
                ForecastField::QueueWaitAfterReadyMs,
                "queue_wait_after_ready_ms",
            ),
            (ForecastField::ExecutionMs, "execution_ms"),
            (ForecastField::FinishInMs, "finish_in_ms"),
        ] {
            assert_eq!(serde_json::to_string(&f).unwrap(), format!("\"{j}\""));
        }
        // a reason keyed by an unknown field is refused, free text is refused
        assert!(
            serde_json::from_value::<BTreeMap<ForecastField, Uncertainty>>(
                json!({"ready": "no_history"})
            )
            .is_err()
        );
        assert!(serde_json::from_str::<WaitReason>("\"memory\"").is_err());
        // optional target fields default; wait_reason is omitted when None
        let mut t = target();
        t.wait_reason = None;
        let v = serde_json::to_value(&t).unwrap();
        assert!(v.get("wait_reason").is_none());
        let minimal = json!({
            "plan_digest": dg("plan"), "ready_in_ms": null, "queue_wait_after_ready_ms": null,
            "execution_ms": null, "finish_in_ms": null, "confidence": "low",
            "runtime_source": "unknown", "conditional": false
        });
        let t: ForecastTarget = serde_json::from_value(minimal).unwrap();
        assert!(!t.overrun && t.history_count == 0 && t.assumptions.is_empty());
        assert!(t.null_reasons.is_empty() && t.wait_reason.is_none());
    }

    #[test]
    fn small_enums_use_snake_case() {
        let cases: Vec<(Value, &str)> = vec![
            (serde_json::to_value(LogStream::Stdout).unwrap(), "stdout"),
            (serde_json::to_value(LogStream::Stderr).unwrap(), "stderr"),
            (
                serde_json::to_value(CancelPhase::Completed).unwrap(),
                "completed",
            ),
            (
                serde_json::to_value(CancelPhase::Accepted).unwrap(),
                "accepted",
            ),
            (
                serde_json::to_value(RetireStatus::AlreadyTerminal).unwrap(),
                "already_terminal",
            ),
            (
                serde_json::to_value(RetireStatus::Stopped).unwrap(),
                "stopped",
            ),
            (
                serde_json::to_value(RetireStatus::Unconfirmed).unwrap(),
                "unconfirmed",
            ),
            (
                serde_json::to_value(EventKind::LeaseExpired).unwrap(),
                "lease_expired",
            ),
            (
                serde_json::to_value(EventKind::StateChanged).unwrap(),
                "state_changed",
            ),
            (
                serde_json::to_value(EventKind::ReceiptReached).unwrap(),
                "receipt_reached",
            ),
            (
                serde_json::to_value(EventKind::LogTruncated).unwrap(),
                "log_truncated",
            ),
            (serde_json::to_value(ToolMatch::Match).unwrap(), "match"),
            (serde_json::to_value(ToolMatch::Missing).unwrap(), "missing"),
            (
                serde_json::to_value(SyncOutcome::Installed).unwrap(),
                "installed",
            ),
            (
                serde_json::to_value(SyncOutcome::Unchanged).unwrap(),
                "unchanged",
            ),
            (serde_json::to_value(SyncOutcome::Failed).unwrap(), "failed"),
        ];
        for (v, s) in cases {
            assert_eq!(v, json!(s));
        }
        for s in [RetireStatus::NotStarted, RetireStatus::Stopped] {
            assert_eq!(
                serde_json::from_value::<RetireStatus>(serde_json::to_value(s).unwrap()).unwrap(),
                s
            );
        }
    }

    #[test]
    fn bytes_are_base64_and_debug_hides_content() {
        let b = Bytes(vec![0, 255, 16]);
        let j = serde_json::to_string(&b).unwrap();
        assert_eq!(j, "\"AP8Q\"");
        assert_eq!(serde_json::from_str::<Bytes>(&j).unwrap(), b);
        assert!(serde_json::from_str::<Bytes>("\"!!\"").is_err());
        assert_eq!(format!("{b:?}"), "Bytes(3 bytes)");
    }

    #[test]
    fn hmac_digest_is_its_own_type_and_form() {
        let h = hdg("x");
        assert!(h.as_str().starts_with("hmac-sha256:"));
        assert_eq!(h.to_string(), h.as_str());
        let j = serde_json::to_string(&h).unwrap();
        assert_eq!(serde_json::from_str::<HmacDigest>(&j).unwrap(), h);
        // a plain blob digest is not an HMAC digest, and the other way round
        let plain = dg("x").to_string();
        assert!(HmacDigest::parse(&plain).is_err());
        assert!(serde_json::from_str::<HmacDigest>(&format!("\"{plain}\"")).is_err());
        assert!(Digest::parse(h.as_str()).is_err());
        assert!(HmacDigest::parse(&format!("hmac-sha256:{}", "A".repeat(64))).is_err());
        assert!(HmacDigest::parse("hmac-sha256:abc").is_err());
        // content_hash follows the HMAC, and the wire form carries the prefix
        let a = secret_put(b"v", ".env", "h1", 1);
        let b = secret_put(b"v", ".env", "h2", 1);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
        let wire = serde_json::to_string(&a).unwrap();
        assert!(wire.contains("\"hmac-sha256:"));
        assert!(!wire.contains("\"sha256:"));
    }

    #[test]
    fn secret_file_debug_is_redacted() {
        let f = SecretFile {
            path: ".env".into(),
            hmac_digest: hdg("h"),
            content: Bytes(b"TOPSECRET".to_vec()),
        };
        let dbg = format!("{f:?}");
        assert!(dbg.contains("<redacted>") && dbg.contains(".env"));
        assert!(!dbg.contains("TOPSECRET") && !dbg.contains("VE9QU0VDUkVU"));
        let req = Request::SecretBundlePut(SecretBundlePutRequest {
            schema_version: SV,
            request_id: rid(),
            coordinator_epoch: CoordinatorEpoch(1),
            attempt_id: aid(),
            generation: Generation(1),
            files: vec![f],
        });
        assert!(!format!("{req:?}").contains("TOPSECRET"));
    }

    #[test]
    fn minimal_payload_defaults() {
        let mut v = serde_json::to_value(payload()).unwrap();
        let o = v.as_object_mut().unwrap();
        o.remove("permit_id");
        o.remove("deadline_ms");
        o.remove("command_timeout_ms");
        o.remove("resource_wait_timeout_ms");
        o.remove("has_secret_bundle");
        let p: AttemptPayload = serde_json::from_value(v).unwrap();
        assert!(p.permit_id.is_none() && p.deadline_ms.is_none() && !p.has_secret_bundle);
        assert!(p.command_timeout_ms.is_none() && p.resource_wait_timeout_ms.is_none());
    }

    #[test]
    fn the_three_timeouts_are_separate_payload_fields() {
        let v = serde_json::to_value(payload()).unwrap();
        assert_eq!(v["command_timeout_ms"], 900_000);
        assert_eq!(v["deadline_ms"], 3_600_000);
        assert_eq!(v["resource_wait_timeout_ms"], 120_000);
        let mut p = payload();
        let h = payload_hash(&p).unwrap();
        p.resource_wait_timeout_ms = None;
        assert_ne!(h, payload_hash(&p).unwrap());
    }

    #[test]
    fn terminal_status_must_carry_a_termination() {
        let ok = terminal_status();
        assert!(ok.check().is_ok());
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["termination"]["outcome"], "timed_out");
        assert_eq!(v["termination"]["timeout"]["kind"], "command");
        assert_eq!(v["retired"], true);
        assert_eq!(
            serde_json::from_value::<AttemptStatus>(v.clone()).unwrap(),
            ok
        );

        // Terminal without a termination, and a live state with one, both fail
        let mut missing = v.clone();
        missing["termination"] = Value::Null;
        let e = serde_json::from_value::<AttemptStatus>(missing.clone()).unwrap_err();
        assert!(e.to_string().contains("requires a termination"), "{e}");
        let mut live = v.clone();
        live["state"] = json!("finalizing");
        let e = serde_json::from_value::<AttemptStatus>(live.clone()).unwrap_err();
        assert!(e.to_string().contains("must not carry"), "{e}");
        let mut absent = v.clone();
        absent.as_object_mut().unwrap().remove("termination");
        assert!(serde_json::from_value::<AttemptStatus>(absent).is_err());
        assert_eq!(
            AttemptStatus {
                termination: None,
                ..ok.clone()
            }
            .check(),
            Err(TerminationStateError::Missing(AttemptState::Terminal))
        );
        assert_eq!(
            AttemptStatus {
                termination: Some(timed_out()),
                ..status()
            }
            .check(),
            Err(TerminationStateError::Unexpected(AttemptState::Active))
        );

        // the same check fires when the status is nested in a response or frame
        let list = json!({"op": "list_attempts", "schema_version": 1, "attempts": [live.clone()]});
        assert!(serde_json::from_value::<Response>(list).is_err());
        let list = json!({"schema_version": 1, "attempts": [missing.clone()]});
        assert!(serde_json::from_value::<ListAttemptsResponse>(list).is_err());
        let mut get = missing.clone();
        get["op"] = json!("get_attempt");
        assert!(serde_json::from_value::<Response>(get.clone()).is_err());
        let framed = json!({"type": "response", "request_id": rid(), "response": get});
        assert!(serde_json::from_value::<Message>(framed).is_err());
        // a contradictory termination inside a status is refused too
        let mut bad = v;
        bad["termination"]["exit"] = json!({"code": 999});
        assert!(serde_json::from_value::<AttemptStatus>(bad).is_err());
    }

    #[test]
    fn get_result_carries_a_validated_termination() {
        let mut v = serde_json::to_value(&responses()[11]).unwrap();
        assert_eq!(v["op"], "get_result");
        assert_eq!(v["termination"]["outcome"], "timed_out");
        assert!(v.get("outcome").is_none() && v.get("exit_code").is_none());
        v["termination"] = json!({"outcome": "timed_out"});
        assert!(serde_json::from_value::<GetResultResponse>(v).is_err());
    }

    #[test]
    fn node_results_serialize_with_an_explicit_state() {
        let r = NodeResult {
            node_id: node("lint"),
            state: NodeState::Skipped {
                reason: "fail-fast".into(),
            },
            exit: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v["state"],
            json!({"kind": "skipped", "reason": "fail-fast"})
        );
        assert_eq!(serde_json::from_value::<NodeResult>(v).unwrap(), r);
        for (state, kind) in [
            (NodeState::Succeeded, "succeeded"),
            (NodeState::Failed, "failed"),
            (NodeState::Cancelled, "cancelled"),
            (NodeState::TimedOut, "timed_out"),
            (NodeState::Interrupted, "interrupted"),
            (NodeState::NotStarted, "not_started"),
        ] {
            let v = serde_json::to_value(&state).unwrap();
            assert_eq!(v, json!({ "kind": kind }));
            assert_eq!(serde_json::from_value::<NodeState>(v).unwrap(), state);
        }
        // a skip without a reason is refused
        assert!(serde_json::from_value::<NodeState>(json!({"kind": "skipped"})).is_err());
        let r: NodeResult = serde_json::from_value(
            json!({"node_id": "t", "state": {"kind": "failed"}, "exit": {"code": 2}}),
        )
        .unwrap();
        assert_eq!(r.exit, Some(Exit::Code(2)));
    }

    #[test]
    fn lease_key_orders_by_epoch_then_sequence() {
        let k = |e, s| LeaseKey {
            coordinator_epoch: CoordinatorEpoch(e),
            lease_seq: s,
        };
        assert!(k(1, 9) < k(2, 0));
        assert!(k(2, 0) < k(2, 1));
        // nothing applied yet: any key extends
        assert!(k(0, 0).extends(None));
        let applied = Some(k(2, 5));
        assert!(!k(2, 5).extends(applied), "same key is a resend");
        assert!(!k(2, 4).extends(applied), "smaller sequence");
        assert!(
            !k(1, 99).extends(applied),
            "older epoch, however large the sequence"
        );
        assert!(k(2, 6).extends(applied));
        assert!(
            k(3, 0).extends(applied),
            "a new epoch restarts the sequence"
        );
        let v = serde_json::to_value(&requests()[6]).unwrap();
        let req: RenewLeaseRequest = serde_json::from_value(v).unwrap();
        assert_eq!(req.key(), k(3, 7));
        let v = serde_json::to_value(k(3, 7)).unwrap();
        assert_eq!(v, json!({"coordinator_epoch": 3, "lease_seq": 7}));
    }

    #[test]
    fn renew_response_says_whether_it_extended() {
        let v = serde_json::to_value(&responses()[6]).unwrap();
        assert_eq!(v["extended"], true);
        let mut v = v;
        v.as_object_mut().unwrap().remove("extended");
        assert!(serde_json::from_value::<Response>(v).is_err());
    }

    #[test]
    fn cancel_before_arrival_answers_with_a_tombstone_and_no_state() {
        let r = CancelAttemptResponse {
            schema_version: SV,
            attempt_id: aid(),
            phase: CancelPhase::Completed,
            state: None,
            tombstone: true,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert!(v["state"].is_null());
        assert_eq!(
            serde_json::from_value::<CancelAttemptResponse>(v).unwrap(),
            r
        );
        let minimal = json!({
            "schema_version": 1, "attempt_id": r.attempt_id, "phase": "accepted"
        });
        let m: CancelAttemptResponse = serde_json::from_value(minimal).unwrap();
        assert!(m.state.is_none() && !m.tombstone);
    }

    #[test]
    fn plan_correlation_must_match_attempt_and_node() {
        let p = plan();
        assert_eq!(p.check(), Ok(()));
        let mut wrong_attempt = p.clone();
        wrong_attempt.nodes[0].correlation.attempt_id = aid();
        assert_eq!(wrong_attempt.check().unwrap_err().node_id, node("test"));
        assert!(wrong_attempt
            .check()
            .unwrap_err()
            .to_string()
            .contains("test"));
        let mut wrong_node = p;
        wrong_node.nodes[0].correlation.node_id = node("other");
        assert!(wrong_node.check().is_err());
        assert_eq!(
            WorkloadPlan {
                nodes: vec![],
                ..wrong_node
            }
            .check(),
            Ok(())
        );
    }

    #[test]
    fn unknown_forecast_numbers_stay_unknown() {
        let mut v = serde_json::to_value(plan()).unwrap();
        let n = v["nodes"][0].as_object_mut().unwrap();
        for k in ["external_delay_ms", "ready_in_ms", "weight", "concurrency"] {
            n.remove(k);
        }
        let p: WorkloadPlan = serde_json::from_value(v).unwrap();
        let n = &p.nodes[0];
        assert_eq!(
            (n.external_delay_ms, n.ready_in_ms, n.weight),
            (None, None, None)
        );
        assert_eq!(n.concurrency, ConcurrencyLimits::default());
        // null and absent are the same unknown, and zero is a real value
        let mut v = serde_json::to_value(plan()).unwrap();
        v["nodes"][0]["ready_in_ms"] = json!(0);
        let p: WorkloadPlan = serde_json::from_value(v).unwrap();
        assert_eq!(p.nodes[0].ready_in_ms, Some(0));
        assert_eq!(
            serde_json::to_value(plan()).unwrap()["nodes"][0]["ready_in_ms"],
            Value::Null
        );
    }

    #[test]
    fn hello_response_names_its_cluster() {
        let v = serde_json::to_value(&responses()[0]).unwrap();
        assert!(v["cluster_id"].as_str().unwrap().starts_with("cl_"));
        let s = serde_json::to_value(status()).unwrap();
        assert_eq!(s["worker_boot_id"], "boot-1");
        assert_eq!(s["agent_epoch"], 2);
        assert_eq!(s["spawned_at_ms"], 1_700_000_000_000u64);
    }

    #[test]
    fn running_is_not_a_proxy_for_spawned() {
        // a quick command can be spawned and finish without `running` ever observed
        let st = AttemptStatus {
            receipts: receipts(&[Receipt::WorkerAccepted, Receipt::CommandRegistered]),
            spawned_at_ms: Some(5),
            ..status()
        };
        assert!(!st.receipts.reached(Receipt::Running));
        assert!(st.spawned_at_ms.is_some());
    }
}
