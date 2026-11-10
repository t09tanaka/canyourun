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
//! content is detectable.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

use crate::error::ApiError;
use crate::ids::*;
use crate::state::{AttemptState, Observation, Outcome, Receipt, StepState};

// ---------------------------------------------------------------------------
// schema version, canonical JSON, hashing
// ---------------------------------------------------------------------------

/// Version of every wire DTO's shape.
pub const WIRE_SCHEMA_VERSION: u32 = 1;

/// The `schema_version` field. Serializes as a number; deserialization rejects
/// any version other than [`WIRE_SCHEMA_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(pub u32);

impl SchemaVersion {
    pub const CURRENT: SchemaVersion = SchemaVersion(WIRE_SCHEMA_VERSION);
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
        let v = u32::deserialize(d)?;
        if v == WIRE_SCHEMA_VERSION {
            Ok(SchemaVersion(v))
        } else {
            Err(D::Error::custom(format!(
                "unsupported schema_version {v}, expected {WIRE_SCHEMA_VERSION}"
            )))
        }
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

/// Hash of a request's content with the top-level `request_id` removed.
pub fn content_hash(request: &Request) -> Result<PayloadHash, CanonicalError> {
    let mut v = serde_json::to_value(request)?;
    if let Value::Object(map) = &mut v {
        map.remove("request_id");
    }
    Ok(Digest::of_bytes(canonical_json(&v)?.as_bytes()))
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
    pub active_homework: u32,
    pub prefetch_pending: u32,
    pub max_active_homework: u32,
    pub prefetch_per_worker: u32,
    pub disk_free_bytes: u64,
    pub draining: bool,
}

// ---------------------------------------------------------------------------
// forecast
// ---------------------------------------------------------------------------

/// One node of a [`WorkloadPlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadNode {
    pub node_id: NodeId,
    #[serde(default)]
    pub depends_on: Vec<NodeId>,
    pub priority: i32,
    /// `None`: unknown, never read as zero.
    #[serde(default)]
    pub predicted_rss_bytes: Option<u64>,
    #[serde(default)]
    pub predicted_runtime_ms: Option<u64>,
    /// Preparation outside admission (transfer etc.).
    #[serde(default)]
    pub external_delay_ms: u64,
    /// Relative time until the node may register.
    #[serde(default)]
    pub ready_in_ms: u64,
    /// A shared setup builder the node waits for.
    #[serde(default)]
    pub shared_builder: Option<BuilderId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadPlan {
    pub attempt_id: AttemptId,
    pub plan_digest: PlanDigest,
    pub expansion_seq: u64,
    pub nodes: Vec<WorkloadNode>,
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

/// The forecast for the requested workload on this worker.
///
/// A value that cannot be computed is `null`, and `null_reasons` maps the field
/// name to the reason. An existing entry running past its history is
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
    pub wait_reason: Option<String>,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub null_reasons: BTreeMap<String, String>,
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
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// What a worker persists on acceptance. Its canonical JSON is hashed into
/// `payload_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptPayload {
    pub run_id: RunId,
    pub homework_id: HomeworkId,
    pub generation: Generation,
    pub source_id: SourceId,
    pub plan_digest: PlanDigest,
    pub manifest_digest: ManifestDigest,
    #[serde(default)]
    pub permit_id: Option<PermitId>,
    pub commands: Vec<CommandSpec>,
    pub lease_ms: u64,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub has_secret_bundle: bool,
}

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
    pub receipt: Receipt,
    /// True when an existing attempt with the same hash was returned.
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStatus {
    pub node_id: NodeId,
    pub state: StepState,
}

/// The worker's view of one attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptStatus {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub homework_id: HomeworkId,
    pub generation: Generation,
    pub state: AttemptState,
    pub receipt: Receipt,
    pub observation: Observation,
    #[serde(default)]
    pub outcome: Option<Outcome>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    /// Set when the command was killed by a signal.
    #[serde(default)]
    pub signal: Option<i32>,
    #[serde(default)]
    pub steps: Vec<StepStatus>,
    #[serde(default)]
    pub log_truncated: bool,
    #[serde(default)]
    pub log_dropped_bytes: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewLeaseRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub attempt_id: AttemptId,
    pub generation: Generation,
    /// Strictly increasing; a replay never extends the lease again.
    pub lease_seq: u64,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewLeaseResponse {
    pub schema_version: SchemaVersion,
    pub attempt_id: AttemptId,
    pub lease_seq: u64,
    pub lease_remaining_ms: u64,
    pub state: AttemptState,
}

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
    pub state: AttemptState,
}

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
    pub outcome: Outcome,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub signal: Option<i32>,
    pub artifacts: Vec<ArtifactRecord>,
    /// Digest over the whole result; echoed back in `ack_collected`.
    pub result_digest: Digest,
}

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

/// One secret file. `Debug` never prints the content.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretFile {
    pub path: String,
    /// Unix permission bits; the worker places the file with `0600`.
    pub mode: u32,
    /// HMAC digest keyed per cluster; never a plain hash of the content.
    pub hmac_digest: Digest,
    pub content: Bytes,
}

impl fmt::Debug for SecretFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretFile")
            .field("path", &self.path)
            .field("mode", &self.mode)
            .field("hmac_digest", &self.hmac_digest)
            .field("content", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretBundlePutRequest {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
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
        }

        impl Response {
            pub fn operation(&self) -> Operation {
                match self { $(Response::$variant(_) => Operation::$variant),* }
            }
            pub fn schema_version(&self) -> SchemaVersion {
                match self { $(Response::$variant(r) => r.schema_version),* }
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
    use serde_json::json;

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
    fn worker() -> WorkerId {
        WorkerId::parse("mini-a").unwrap()
    }
    fn boot() -> WorkerBootId {
        WorkerBootId::parse("boot-1").unwrap()
    }

    fn plan() -> WorkloadPlan {
        WorkloadPlan {
            attempt_id: aid(),
            plan_digest: dg("plan"),
            expansion_seq: 1,
            nodes: vec![WorkloadNode {
                node_id: node("test"),
                depends_on: vec![node("setup")],
                priority: 0,
                predicted_rss_bytes: Some(1 << 20),
                predicted_runtime_ms: None,
                external_delay_ms: 10,
                ready_in_ms: 5,
                shared_builder: Some(BuilderId::generate()),
            }],
        }
    }

    fn payload() -> AttemptPayload {
        AttemptPayload {
            run_id: RunId::generate(),
            homework_id: HomeworkId::generate(),
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
            deadline_ms: None,
            has_secret_bundle: true,
        }
    }

    fn status() -> AttemptStatus {
        AttemptStatus {
            schema_version: SV,
            attempt_id: aid(),
            run_id: RunId::generate(),
            homework_id: HomeworkId::generate(),
            generation: Generation(2),
            state: AttemptState::Active,
            receipt: Receipt::Running,
            observation: Observation::Known,
            outcome: None,
            exit_code: None,
            signal: Some(15),
            steps: vec![StepStatus {
                node_id: node("test"),
                state: StepState::WaitingResources,
            }],
            log_truncated: true,
            log_dropped_bytes: 9,
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
            wait_reason: Some("memory".into()),
            assumptions: vec!["no new external load".into()],
            null_reasons: BTreeMap::from([("execution_ms".into(), "no history".into())]),
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
                attempt_id: aid.clone(),
                generation: Generation(1),
                files: vec![SecretFile {
                    path: ".env".into(),
                    mode: 0o600,
                    hmac_digest: dg("hmac"),
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
                active_homework: 1,
                prefetch_pending: 0,
                max_active_homework: 2,
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
                receipt: Receipt::WorkerAccepted,
                duplicate: true,
            }),
            Response::GetAttempt(status()),
            Response::ListAttempts(ListAttemptsResponse {
                schema_version: SV,
                attempts: vec![status()],
            }),
            Response::RenewLease(RenewLeaseResponse {
                schema_version: SV,
                attempt_id: aid(),
                lease_seq: 7,
                lease_remaining_ms: 59_000,
                state: AttemptState::Active,
            }),
            Response::CancelAttempt(CancelAttemptResponse {
                schema_version: SV,
                attempt_id: aid(),
                phase: CancelPhase::Accepted,
                state: AttemptState::Cancelling,
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
                outcome: Outcome::Failed,
                exit_code: Some(1),
                signal: None,
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
    fn schema_version_other_than_current_is_rejected() {
        assert_eq!(SchemaVersion::default(), SchemaVersion::CURRENT);
        assert_eq!(
            serde_json::from_str::<SchemaVersion>("1").unwrap(),
            SchemaVersion(1)
        );
        assert!(serde_json::from_str::<SchemaVersion>("2").is_err());
        assert!(serde_json::from_str::<SchemaVersion>("0").is_err());
        let mut v = serde_json::to_value(requests().remove(1)).unwrap();
        v["schema_version"] = json!(2);
        let err = serde_json::from_value::<Request>(v)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported schema_version 2"), "{err}");
        let mut v = serde_json::to_value(requests().remove(1)).unwrap();
        v.as_object_mut().unwrap().remove("schema_version");
        assert!(serde_json::from_value::<Request>(v).is_err());
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
        assert_eq!(t["null_reasons"]["execution_ms"], "no history");
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
    fn secret_file_debug_is_redacted() {
        let f = SecretFile {
            path: ".env".into(),
            mode: 0o600,
            hmac_digest: dg("h"),
            content: Bytes(b"TOPSECRET".to_vec()),
        };
        let dbg = format!("{f:?}");
        assert!(dbg.contains("<redacted>") && dbg.contains(".env"));
        assert!(!dbg.contains("TOPSECRET") && !dbg.contains("VE9QU0VDUkVU"));
        let req = Request::SecretBundlePut(SecretBundlePutRequest {
            schema_version: SV,
            request_id: rid(),
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
        o.remove("has_secret_bundle");
        let p: AttemptPayload = serde_json::from_value(v).unwrap();
        assert!(p.permit_id.is_none() && p.deadline_ms.is_none() && !p.has_secret_bundle);
    }
}
