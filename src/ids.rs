//! Identifiers and digests shared by every component (spec chapter 10).
//!
//! Generated identifiers are `<kind prefix>_<ULID>` (for example `run_01J...`).
//! Worker and boot names are validated strings over `[A-Za-z0-9._-]` (they name
//! state directories and SSH destinations). Node names are looser display
//! strings (see [`NodeId`]). Epochs and generations are plain `u64`. Every type serializes as a
//! bare string or number and validates on deserialization.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use ulid::Ulid;

/// Why a string is not a valid identifier or digest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("expected prefix `{expected}_`, got `{got}`")]
    WrongPrefix { expected: &'static str, got: String },
    #[error("`{0}` does not carry a valid ULID after the prefix")]
    BadUlid(String),
    #[error(
        "name must be 1..={max} characters of [A-Za-z0-9._-], not start with `.`, and not contain `..`"
    )]
    BadName { max: usize },
    #[error("node name must be 1..={max} bytes with no control characters")]
    BadNodeName { max: usize },
    #[error("digest must look like `sha256:<64 lowercase hex>`, got `{0}`")]
    BadDigest(String),
}

macro_rules! prefixed_id {
    ($(#[$doc:meta])* $name:ident, $prefix:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// The kind prefix, without the underscore.
            pub const PREFIX: &'static str = $prefix;

            /// A fresh identifier.
            pub fn generate() -> Self {
                Self(format!("{}_{}", $prefix, Ulid::new()))
            }

            /// Parse and validate `<prefix>_<ULID>`. The ULID part is re-encoded
            /// in its canonical upper-case form, so equal identifiers compare
            /// and hash equal whatever case they arrived in.
            pub fn parse(s: &str) -> Result<Self, IdError> {
                let rest = s
                    .strip_prefix($prefix)
                    .and_then(|r| r.strip_prefix('_'))
                    .ok_or_else(|| IdError::WrongPrefix {
                        expected: $prefix,
                        got: s.to_string(),
                    })?;
                let ulid = Ulid::from_string(rest).map_err(|_| IdError::BadUlid(s.to_string()))?;
                // Re-encode so that `run_01j...` and `run_01J...` are one identifier.
                Ok(Self(format!("{}_{}", $prefix, ulid)))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(s: String) -> Result<Self, IdError> {
                Self::parse(&s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }
    };
}

prefixed_id!(
    /// One coordinator plus its workers.
    ClusterId,
    "cl"
);
prefixed_id!(
    /// A coordinator installation.
    CoordinatorId,
    "co"
);
prefixed_id!(
    /// One CI or command submission.
    RunId,
    "run"
);
prefixed_id!(
    /// A placeable logical piece of work.
    TaskId,
    "task"
);
prefixed_id!(
    /// One execution attempt of a task.
    AttemptId,
    "att"
);
prefixed_id!(
    /// An idempotent request.
    RequestId,
    "req"
);
prefixed_id!(
    /// A captured input snapshot.
    SourceId,
    "src"
);
prefixed_id!(
    /// A shared setup builder.
    BuilderId,
    "bld"
);
prefixed_id!(
    /// A global matrix permit.
    PermitId,
    "prm"
);
prefixed_id!(
    /// A history measurement (deduplicates records).
    MeasurementId,
    "msr"
);

/// Longest accepted free-form name.
pub const MAX_NAME_LEN: usize = 128;

/// A name is `[A-Za-z0-9._-]{1,128}`, does not start with `.`, and has no `..`.
/// Names end up in file names and process arguments, so path-like and
/// whitespace-bearing forms are refused up front.
fn is_valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_NAME_LEN
        && !s.starts_with('.')
        && !s.contains("..")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Longest accepted node name.
pub const MAX_NODE_NAME_LEN: usize = 1024;

/// A node name is non-empty, at most [`MAX_NODE_NAME_LEN`] bytes, and has no
/// control characters. Anything else (spaces, `/`, `:`, `(`, `,`) is allowed.
fn is_valid_node_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_NODE_NAME_LEN && !s.chars().any(char::is_control)
}

macro_rules! name_id {
    ($(#[$doc:meta])* $name:ident, $valid:ident, $err:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Validate a free-form name.
            pub fn parse(s: &str) -> Result<Self, IdError> {
                if $valid(s) {
                    Ok(Self(s.to_string()))
                } else {
                    Err($err)
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(s: String) -> Result<Self, IdError> {
                Self::parse(&s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }
    };
}

name_id!(
    /// A worker name chosen by the user (for example `mini-a`).
    WorkerId,
    is_valid_name,
    IdError::BadName { max: MAX_NAME_LEN }
);
name_id!(
    /// The OS boot identifier reported by a worker.
    WorkerBootId,
    is_valid_name,
    IdError::BadName { max: MAX_NAME_LEN }
);
name_id!(
    /// A setup/step/shard node name inside a plan. It mirrors local-ci step,
    /// job and matrix variant names such as `test (ubuntu, 18)` or
    /// `build/linux:arm64`, so it may contain spaces, `/`, `:`, `(` and `,`.
    ///
    /// Never use a `NodeId` as a path component or process argument: it is not
    /// path-safe. Derive a safe name (for example a digest) first.
    NodeId,
    is_valid_node_name,
    IdError::BadNodeName {
        max: MAX_NODE_NAME_LEN
    }
);

macro_rules! counter {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl $name {
            /// The next value; `None` on overflow.
            pub fn next(self) -> Option<Self> {
                self.0.checked_add(1).map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

counter!(
    /// Rejects stale control requests after the coordinator restarts.
    CoordinatorEpoch
);
counter!(
    /// Distinguishes agent restarts; invalidates old forecasts.
    AgentEpoch
);
counter!(
    /// Generation of an attempt.
    Generation
);

/// `sha256:<64 lowercase hex>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Digest(String);

impl Digest {
    pub const PREFIX: &'static str = "sha256:";

    /// Digest of raw bytes.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let hash = Sha256::digest(bytes);
        let mut s = String::with_capacity(Self::PREFIX.len() + 64);
        s.push_str(Self::PREFIX);
        for b in hash {
            s.push_str(&format!("{b:02x}"));
        }
        Self(s)
    }

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

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Digest {
    type Error = IdError;
    fn try_from(s: String) -> Result<Self, IdError> {
        Self::parse(&s)
    }
}

impl From<Digest> for String {
    fn from(d: Digest) -> String {
        d.0
    }
}

/// Digest of a request payload (`payload_hash`).
pub type PayloadHash = Digest;
/// Digest of a frozen plan.
pub type PlanDigest = Digest;
/// Digest of a source manifest.
pub type ManifestDigest = Digest;
/// Digest of a worker's queue/ledger snapshot.
pub type SnapshotDigest = Digest;
/// Digest of a worker's policy.
pub type PolicyDigest = Digest;

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! prefixed_tests {
        ($($m:ident: $t:ident => $prefix:literal),* $(,)?) => {$(
            mod $m {
                use super::*;
                #[test]
                fn roundtrip_and_prefix() {
                    assert_eq!($t::PREFIX, $prefix);
                    let id = $t::generate();
                    assert!(id.as_str().starts_with(concat!($prefix, "_")));
                    assert_eq!(id.to_string(), id.as_str());
                    assert_eq!($t::parse(id.as_str()).unwrap(), id);
                    let json = serde_json::to_string(&id).unwrap();
                    assert_eq!(json, format!("\"{}\"", id.as_str()));
                    assert_eq!(serde_json::from_str::<$t>(&json).unwrap(), id);
                    assert_eq!(String::from(id.clone()), id.as_str());
                }
                #[test]
                fn rejects_wrong_prefix_and_bad_ulid() {
                    let ulid = Ulid::new();
                    let wrong = format!("zzz_{ulid}");
                    assert!(matches!($t::parse(&wrong), Err(IdError::WrongPrefix { .. })));
                    assert!(matches!($t::parse(&ulid.to_string()), Err(IdError::WrongPrefix { .. })));
                    let no_sep = format!("{}{ulid}", $prefix);
                    assert!(matches!($t::parse(&no_sep), Err(IdError::WrongPrefix { .. })));
                    let bad = format!("{}_nope", $prefix);
                    assert!(matches!($t::parse(&bad), Err(IdError::BadUlid(_))));
                    assert!(serde_json::from_str::<$t>(&format!("\"{wrong}\"")).is_err());
                }
            }
        )*};
    }

    prefixed_tests!(
        t_clusterid: ClusterId => "cl",
        t_coordinatorid: CoordinatorId => "co",
        t_runid: RunId => "run",
        t_taskid: TaskId => "task",
        t_attemptid: AttemptId => "att",
        t_requestid: RequestId => "req",
        t_sourceid: SourceId => "src",
        t_builderid: BuilderId => "bld",
        t_permitid: PermitId => "prm",
        t_measurementid: MeasurementId => "msr",
    );

    #[test]
    fn parse_canonicalizes_the_ulid_case() {
        let id = RunId::generate();
        let lower = format!("run_{}", id.as_str()[4..].to_lowercase());
        assert_ne!(lower, id.as_str());
        let parsed = RunId::parse(&lower).unwrap();
        assert_eq!(parsed, id);
        assert_eq!(parsed.as_str(), id.as_str());
        assert_eq!(
            serde_json::from_str::<RunId>(&format!("\"{lower}\"")).unwrap(),
            id
        );
        let mut set = std::collections::HashSet::new();
        set.insert(id.clone());
        assert!(set.contains(&parsed));
        // the prefix itself stays case-sensitive
        assert!(RunId::parse(&format!("RUN_{}", &id.as_str()[4..])).is_err());
    }

    #[test]
    fn generated_ids_are_distinct_and_other_kinds_are_rejected() {
        assert_ne!(RunId::generate(), RunId::generate());
        let run = RunId::generate();
        assert!(AttemptId::parse(run.as_str()).is_err());
    }

    #[test]
    fn id_error_messages_name_the_problem() {
        let e = RunId::parse("att_x").unwrap_err();
        assert!(e.to_string().contains("run_"));
        assert!(RunId::parse("run_x")
            .unwrap_err()
            .to_string()
            .contains("ULID"));
        assert!(WorkerId::parse("").unwrap_err().to_string().contains("128"));
        assert!(Digest::parse("x")
            .unwrap_err()
            .to_string()
            .contains("sha256"));
    }

    macro_rules! name_tests {
        ($($m:ident: $t:ident),*) => {$(
            mod $m {
                use super::*;
                #[test]
                fn accepts_and_roundtrips() {
                    let id = $t::parse("mini-a.local_1").unwrap();
                    assert_eq!(id.as_str(), "mini-a.local_1");
                    assert_eq!(id.to_string(), "mini-a.local_1");
                    let json = serde_json::to_string(&id).unwrap();
                    assert_eq!(json, "\"mini-a.local_1\"");
                    assert_eq!(serde_json::from_str::<$t>(&json).unwrap(), id);
                    assert_eq!(String::from(id), "mini-a.local_1");
                }
                #[test]
                fn rejects_bad_names() {
                    assert!($t::parse("").is_err());
                    assert!($t::parse("a b").is_err());
                    assert!($t::parse("a\nb").is_err());
                    assert!($t::parse("a\u{0}b").is_err());
                    for bad in ["a/b", "a:b", "a\\b", "a$b", "a*b", "é", ".hidden", ".", "..", "a..b", "a..", "-.."] {
                        assert!($t::parse(bad).is_err(), "{bad}");
                    }
                    for ok in ["a.b", "a-b", "a_b", "A9", "-a", "a.", "_x", "x-1.2_3"] {
                        assert!($t::parse(ok).is_ok(), "{ok}");
                    }
                    assert!($t::parse(&"x".repeat(MAX_NAME_LEN)).is_ok());
                    assert!($t::parse(&"x".repeat(MAX_NAME_LEN + 1)).is_err());
                    assert!(serde_json::from_str::<$t>("\"\"").is_err());
                }
            }
        )*};
    }
    name_tests!(t_worker: WorkerId, t_boot: WorkerBootId);

    mod t_node {
        use super::*;

        #[test]
        fn accepts_local_ci_names_and_roundtrips() {
            for ok in [
                "test (ubuntu, 18)",
                "build/linux:arm64",
                "a b",
                ".hidden",
                "..",
                "é",
                "mini-a.local_1",
            ] {
                let id = NodeId::parse(ok).unwrap();
                assert_eq!(id.as_str(), ok);
                assert_eq!(id.to_string(), ok);
                let json = serde_json::to_string(&id).unwrap();
                assert_eq!(serde_json::from_str::<NodeId>(&json).unwrap(), id);
                assert_eq!(String::from(id), ok);
            }
            assert!(WorkerId::parse("test (ubuntu, 18)").is_err());
            assert!(WorkerId::parse("build/linux:arm64").is_err());
        }

        #[test]
        fn rejects_empty_control_and_overlong() {
            for bad in ["", "a\nb", "a\tb", "a\u{0}b", "a\u{7f}b", "a\u{85}b"] {
                assert!(NodeId::parse(bad).is_err(), "{bad:?}");
            }
            assert!(NodeId::parse(&"x".repeat(MAX_NODE_NAME_LEN)).is_ok());
            assert!(NodeId::parse(&"x".repeat(MAX_NODE_NAME_LEN + 1)).is_err());
            assert!(serde_json::from_str::<NodeId>("\"\"").is_err());
            let msg = NodeId::parse("").unwrap_err().to_string();
            assert!(msg.contains("1024") && msg.contains("control"));
        }
    }

    #[test]
    fn counters_serialize_as_numbers_and_increment() {
        assert_eq!(serde_json::to_string(&CoordinatorEpoch(7)).unwrap(), "7");
        assert_eq!(
            serde_json::from_str::<AgentEpoch>("3").unwrap(),
            AgentEpoch(3)
        );
        assert_eq!(Generation(1).next(), Some(Generation(2)));
        assert_eq!(Generation(u64::MAX).next(), None);
        assert_eq!(Generation::default(), Generation(0));
        assert_eq!(CoordinatorEpoch(4).to_string(), "4");
        assert!(AgentEpoch(1) < AgentEpoch(2));
    }

    #[test]
    fn digest_known_vector_and_validation() {
        let d = Digest::of_bytes(b"abc");
        assert_eq!(
            d.as_str(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(d.to_string(), d.as_str());
        assert_eq!(Digest::parse(d.as_str()).unwrap(), d);
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(serde_json::from_str::<Digest>(&json).unwrap(), d);
        assert_eq!(String::from(d.clone()), d.as_str());
        let hex = &d.as_str()[7..];
        assert!(Digest::parse(hex).is_err());
        assert!(Digest::parse(&format!("sha256:{}", &hex[1..])).is_err());
        assert!(Digest::parse(&format!("sha256:{}0", hex)).is_err());
        assert!(Digest::parse(&format!("sha256:{}", hex.to_uppercase())).is_err());
        assert!(Digest::parse(&format!("sha512:{hex}")).is_err());
        assert!(Digest::parse(&format!("sha256:{}g", &hex[1..])).is_err());
        assert!(Digest::parse(&format!("sha256:{}/", &hex[1..])).is_err());
        assert!(Digest::parse(&format!("sha256:{}`", &hex[1..])).is_err());
        assert!(Digest::parse(&format!("sha256:{}:", &hex[1..])).is_err());
        assert!(Digest::parse(&format!("sha256:{}@", &hex[1..])).is_err());
        assert!(serde_json::from_str::<Digest>("\"nope\"").is_err());
    }
}
