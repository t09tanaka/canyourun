//! Structured error codes and their single mapping to process exit codes.
//!
//! The JSON envelope tells apart: signal, bad input, unconfirmed communication
//! (`COMMUNICATION_UNKNOWN`), and artifact collection failure
//! (`COLLECTION_FAILED`). The CLI exit code alone is never the job's state.

use serde::{Deserialize, Serialize};

/// Process exit codes. The single definition site.
pub mod exit {
    /// Success.
    pub const SUCCESS: i32 = 0;
    /// A command ran and failed generically (the original code is passed through when known).
    pub const FAILURE: i32 = 1;
    /// Invalid input (bad arguments); clap also exits with this code.
    pub const INPUT_ERROR: i32 = 2;
    /// The command is defined but not implemented yet.
    pub const NOT_IMPLEMENTED: i32 = 70;
    /// The source is being captured by another process.
    pub const SOURCE_BUSY: i32 = 71;
    /// Same attempt, different payload.
    pub const CONFLICT: i32 = 72;
    /// The addressed object does not exist.
    pub const NOT_FOUND: i32 = 73;
    /// Worker and coordinator protocols do not match.
    pub const PROTOCOL_INCOMPATIBLE: i32 = 74;
    /// A stale coordinator epoch or agent epoch was used.
    pub const STALE_EPOCH: i32 = 75;
    /// Dispatch is blocked (no worker, secret, or environment).
    pub const BLOCKED: i32 = 76;
    /// The execution state could not be confirmed.
    pub const COMMUNICATION_UNKNOWN: i32 = 77;
    /// The command finished but required outputs were not collected.
    pub const COLLECTION_FAILED: i32 = 78;
    /// Unexpected internal failure.
    pub const INTERNAL: i32 = 79;
    /// Refused for now, nothing was done: ask again later. Outside `70..=79`
    /// (all taken); `EX_UNAVAILABLE` of sysexits.
    pub const OVERLOADED: i32 = 69;
    /// Conventional base for "terminated by signal N": `128 + N`.
    pub const SIGNAL_BASE: i32 = 128;
}

/// Highest signal number accepted by [`ErrorCode::exit_code_with`].
pub const MAX_SIGNAL: i32 = 64;
/// Highest exit code a process can report.
pub const MAX_EXIT_CODE: i32 = 255;

/// Machine-readable error code, serialized as `SCREAMING_SNAKE_CASE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    NotImplemented,
    InvalidInput,
    SourceBusy,
    Conflict,
    NotFound,
    ProtocolIncompatible,
    StaleEpoch,
    Blocked,
    CommunicationUnknown,
    CollectionFailed,
    CommandFailed,
    Signal,
    Internal,
    /// The attempt was retired (a tombstone exists). A late `submit_attempt`
    /// and any other mutation are refused with this code. `get_attempt` on an
    /// attempt the worker knows answers a status with `retired: true`; on a
    /// tombstone that has no accepted attempt behind it (a cancel or retire
    /// that arrived first) there is no status to build, so the worker answers
    /// this error. Neither case is `NOT_FOUND`.
    Retired,
    /// `renew_lease` could not extend the lease: the attempt is expired,
    /// cancelling, finalizing, terminal or retired. A merely stale or replayed
    /// renew is not an error; it answers `extended: false`.
    LeaseNotRenewable,
    /// The peer refused the request for now and committed nothing of it: too
    /// many requests wait, or its database was busy. The same request may be
    /// sent again. Unlike `SOURCE_BUSY` this says nothing about the input.
    Overloaded,
}

/// Why a task is blocked (accompanies `ErrorCode::Blocked`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedReason {
    NoCompatibleWorker,
    SecretUnavailable,
    EnvironmentMismatch,
}

/// Why a request was refused with `CONFLICT`, when the caller can act on
/// the reason (accompanies `ErrorCode::Conflict`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ConflictReason {
    /// The Git bundle of a source is built on commits the worker's clone of
    /// the repository does not hold. Nothing of the bundle was kept. `have`
    /// lists commits the clone does hold (see `blob_verify`): a bundle built
    /// on those is taken.
    BundleBaseMissing { have: Vec<String> },
    /// A `submit_attempt` names an attempt the worker already holds with
    /// another payload. Sending the same request again never succeeds.
    PayloadMismatch,
    /// A reason this version does not know (a newer peer sent it). What it
    /// carried is dropped: the caller treats the error as a plain
    /// `CONFLICT`.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    /// Every code.
    pub const ALL: [ErrorCode; 16] = [
        ErrorCode::NotImplemented,
        ErrorCode::InvalidInput,
        ErrorCode::SourceBusy,
        ErrorCode::Conflict,
        ErrorCode::NotFound,
        ErrorCode::ProtocolIncompatible,
        ErrorCode::StaleEpoch,
        ErrorCode::Blocked,
        ErrorCode::CommunicationUnknown,
        ErrorCode::CollectionFailed,
        ErrorCode::CommandFailed,
        ErrorCode::Signal,
        ErrorCode::Internal,
        ErrorCode::Retired,
        ErrorCode::LeaseNotRenewable,
        ErrorCode::Overloaded,
    ];

    /// The wire string, identical to the serde form.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::NotImplemented => "NOT_IMPLEMENTED",
            ErrorCode::InvalidInput => "INVALID_INPUT",
            ErrorCode::SourceBusy => "SOURCE_BUSY",
            ErrorCode::Conflict => "CONFLICT",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::ProtocolIncompatible => "PROTOCOL_INCOMPATIBLE",
            ErrorCode::StaleEpoch => "STALE_EPOCH",
            ErrorCode::Blocked => "BLOCKED",
            ErrorCode::CommunicationUnknown => "COMMUNICATION_UNKNOWN",
            ErrorCode::CollectionFailed => "COLLECTION_FAILED",
            ErrorCode::CommandFailed => "COMMAND_FAILED",
            ErrorCode::Signal => "SIGNAL",
            ErrorCode::Internal => "INTERNAL",
            ErrorCode::Retired => "RETIRED",
            ErrorCode::LeaseNotRenewable => "LEASE_NOT_RENEWABLE",
            ErrorCode::Overloaded => "OVERLOADED",
        }
    }

    /// The process exit code for this error. `Signal` and `CommandFailed`
    /// pass the command's own code through when known; use
    /// [`ErrorCode::exit_code_with`].
    pub fn exit_code(self) -> i32 {
        match self {
            ErrorCode::NotImplemented => exit::NOT_IMPLEMENTED,
            ErrorCode::InvalidInput => exit::INPUT_ERROR,
            ErrorCode::SourceBusy => exit::SOURCE_BUSY,
            ErrorCode::Conflict => exit::CONFLICT,
            ErrorCode::NotFound => exit::NOT_FOUND,
            ErrorCode::ProtocolIncompatible => exit::PROTOCOL_INCOMPATIBLE,
            ErrorCode::StaleEpoch => exit::STALE_EPOCH,
            ErrorCode::Blocked => exit::BLOCKED,
            ErrorCode::CommunicationUnknown => exit::COMMUNICATION_UNKNOWN,
            ErrorCode::CollectionFailed => exit::COLLECTION_FAILED,
            ErrorCode::CommandFailed => exit::FAILURE,
            ErrorCode::Signal => exit::SIGNAL_BASE,
            ErrorCode::Internal => exit::INTERNAL,
            // Both are "the addressed attempt no longer accepts this", the
            // same family as a payload conflict; the JSON code tells them apart.
            ErrorCode::Retired | ErrorCode::LeaseNotRenewable => exit::CONFLICT,
            ErrorCode::Overloaded => exit::OVERLOADED,
        }
    }

    /// Like [`ErrorCode::exit_code`], but with the command's own detail:
    ///
    /// * `Signal` with `n` in `1..=64` gives `128 + n`;
    /// * `CommandFailed` with `n` in `1..=255` passes `n` through;
    /// * a detail outside those ranges (including `0`, which would read as
    ///   success) is a broken report and becomes `INTERNAL`.
    ///
    /// A command may exit with any code, including `70..=79`, which this
    /// crate reserves for its own errors, so the process exit code alone never
    /// tells a command failure from a runner error. Decide the settled state
    /// from the JSON `code` (and `--json` output), not from the exit code.
    pub fn exit_code_with(self, detail: Option<i32>) -> i32 {
        match (self, detail) {
            (ErrorCode::Signal, Some(n)) if (1..=MAX_SIGNAL).contains(&n) => exit::SIGNAL_BASE + n,
            (ErrorCode::CommandFailed, Some(n)) if (1..=MAX_EXIT_CODE).contains(&n) => n,
            (ErrorCode::Signal | ErrorCode::CommandFailed, Some(_)) => exit::INTERNAL,
            _ => self.exit_code(),
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error body carried in `--json` envelopes and protocol error frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    /// Set when `code` is `BLOCKED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<BlockedReason>,
    /// May be set when `code` is `CONFLICT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_reason: Option<ConflictReason>,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        ApiError {
            code,
            message: message.into(),
            blocked_reason: None,
            conflict_reason: None,
        }
    }

    pub fn conflict(reason: ConflictReason, message: impl Into<String>) -> Self {
        ApiError {
            code: ErrorCode::Conflict,
            message: message.into(),
            blocked_reason: None,
            conflict_reason: Some(reason),
        }
    }

    pub fn blocked(reason: BlockedReason, message: impl Into<String>) -> Self {
        ApiError {
            code: ErrorCode::Blocked,
            message: message.into(),
            blocked_reason: Some(reason),
            conflict_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    #[test]
    fn wire_string_matches_serde_for_every_code() {
        for c in ErrorCode::ALL {
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_str()));
            assert_eq!(serde_json::from_str::<ErrorCode>(&json).unwrap(), c);
            assert_eq!(c.to_string(), c.as_str());
        }
    }

    #[test]
    fn exit_codes_are_unique_except_documented_overlaps() {
        let mut seen = HashSet::new();
        for c in ErrorCode::ALL {
            let documented_overlap = matches!(c, ErrorCode::Retired | ErrorCode::LeaseNotRenewable);
            assert!(
                seen.insert(c.exit_code()) || documented_overlap,
                "duplicate exit code for {c}"
            );
            assert_ne!(c.exit_code(), exit::SUCCESS);
        }
    }

    #[test]
    fn exit_code_mapping_is_pinned() {
        assert_eq!(ErrorCode::NotImplemented.exit_code(), 70);
        assert_eq!(ErrorCode::InvalidInput.exit_code(), 2);
        assert_eq!(ErrorCode::SourceBusy.exit_code(), 71);
        assert_eq!(ErrorCode::Conflict.exit_code(), 72);
        assert_eq!(ErrorCode::NotFound.exit_code(), 73);
        assert_eq!(ErrorCode::ProtocolIncompatible.exit_code(), 74);
        assert_eq!(ErrorCode::StaleEpoch.exit_code(), 75);
        assert_eq!(ErrorCode::Blocked.exit_code(), 76);
        assert_eq!(ErrorCode::CommunicationUnknown.exit_code(), 77);
        assert_eq!(ErrorCode::CollectionFailed.exit_code(), 78);
        assert_eq!(ErrorCode::CommandFailed.exit_code(), 1);
        assert_eq!(ErrorCode::Signal.exit_code(), 128);
        assert_eq!(ErrorCode::Internal.exit_code(), 79);
        assert_eq!(ErrorCode::Retired.exit_code(), 72);
        assert_eq!(ErrorCode::LeaseNotRenewable.exit_code(), 72);
        assert_eq!(ErrorCode::Overloaded.exit_code(), 69);
        assert_eq!(exit::SUCCESS, 0);
    }

    #[test]
    fn signal_and_command_failure_pass_detail_through() {
        assert_eq!(ErrorCode::Signal.exit_code_with(Some(15)), 143);
        assert_eq!(ErrorCode::Signal.exit_code_with(None), 128);
        assert_eq!(ErrorCode::CommandFailed.exit_code_with(Some(42)), 42);
        assert_eq!(ErrorCode::CommandFailed.exit_code_with(None), 1);
        assert_eq!(ErrorCode::Conflict.exit_code_with(Some(9)), 72);
    }

    #[test]
    fn command_failure_detail_is_range_checked() {
        let f = |n| ErrorCode::CommandFailed.exit_code_with(Some(n));
        assert_eq!(f(1), 1);
        assert_eq!(f(255), 255);
        assert_eq!(f(0), exit::INTERNAL);
        assert_eq!(f(256), exit::INTERNAL);
        assert_eq!(f(-1), exit::INTERNAL);
        // a command exit code may collide with the reserved range; it passes through
        assert_eq!(f(exit::CONFLICT), exit::CONFLICT);
    }

    #[test]
    fn signal_detail_is_range_checked() {
        let f = |n| ErrorCode::Signal.exit_code_with(Some(n));
        assert_eq!(f(1), 129);
        assert_eq!(f(64), 192);
        assert_eq!(f(0), exit::INTERNAL);
        assert_eq!(f(65), exit::INTERNAL);
        assert_eq!(f(-9), exit::INTERNAL);
    }

    #[test]
    fn the_four_distinguished_outcomes_have_distinct_codes() {
        let four = [
            ErrorCode::Signal,
            ErrorCode::InvalidInput,
            ErrorCode::CommunicationUnknown,
            ErrorCode::CollectionFailed,
        ];
        let strings: HashSet<_> = four.iter().map(|c| c.as_str()).collect();
        let codes: HashSet<_> = four.iter().map(|c| c.exit_code()).collect();
        assert_eq!(strings.len(), 4);
        assert_eq!(codes.len(), 4);
    }

    #[test]
    fn blocked_reasons_serialize_snake_case() {
        let e = ApiError::blocked(BlockedReason::SecretUnavailable, "bundle lost");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "BLOCKED");
        assert_eq!(v["blocked_reason"], "secret_unavailable");
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
        for (r, s) in [
            (BlockedReason::NoCompatibleWorker, "no_compatible_worker"),
            (BlockedReason::EnvironmentMismatch, "environment_mismatch"),
        ] {
            assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{s}\""));
        }
    }

    #[test]
    fn a_conflict_can_say_why_in_a_form_the_caller_reads() {
        let have = vec!["0123456789abcdef0123456789abcdef01234567".to_string()];
        let e = ApiError::conflict(
            ConflictReason::BundleBaseMissing { have: have.clone() },
            "send it again",
        );
        assert_eq!(e.code, ErrorCode::Conflict);
        assert_eq!(e.message, "send it again");
        assert_eq!(e.blocked_reason, None);
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "CONFLICT");
        assert_eq!(v["conflict_reason"]["reason"], "bundle_base_missing");
        assert_eq!(v["conflict_reason"]["have"][0], have[0]);
        assert!(v.get("blocked_reason").is_none());
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
        let blocked = ApiError::blocked(BlockedReason::SecretUnavailable, "x");
        assert_eq!(blocked.conflict_reason, None);
    }

    #[test]
    fn a_conflict_reason_of_a_newer_peer_is_read_as_unknown() {
        for reason in [
            json!({"reason": "clone_is_being_moved"}),
            json!({"reason": "clone_is_being_moved", "have": ["x"], "retry_after_ms": 5}),
        ] {
            let v = json!({"code": "CONFLICT", "message": "later", "conflict_reason": reason});
            let e: ApiError = serde_json::from_value(v).unwrap();
            assert_eq!(e.code, ErrorCode::Conflict);
            assert_eq!(e.message, "later");
            assert_eq!(e.conflict_reason, Some(ConflictReason::Unknown));
        }
        // a reason that is known keeps what it carries
        let known: ConflictReason =
            serde_json::from_value(json!({"reason": "bundle_base_missing", "have": ["a"]}))
                .unwrap();
        assert_eq!(
            known,
            ConflictReason::BundleBaseMissing {
                have: vec!["a".into()]
            }
        );
        assert_eq!(
            serde_json::to_value(ConflictReason::Unknown).unwrap(),
            json!({"reason": "unknown"})
        );
        let e = ApiError::conflict(ConflictReason::PayloadMismatch, "other payload");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["conflict_reason"], json!({"reason": "payload_mismatch"}));
        let back: ApiError = serde_json::from_value(v).unwrap();
        assert_eq!(back.conflict_reason, Some(ConflictReason::PayloadMismatch));
    }

    #[test]
    fn plain_errors_omit_blocked_reason() {
        let e = ApiError::new(ErrorCode::Conflict, "payload differs");
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("blocked_reason").is_none());
        assert!(v.get("conflict_reason").is_none());
        assert_eq!(v["message"], "payload differs");
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
    }
}
