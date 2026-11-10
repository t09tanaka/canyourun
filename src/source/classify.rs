//! Pure decisions around a capture: how many attempts, what a refusal means to
//! the caller, and how local-ci's content-derived source id becomes ours.

use crate::error::{ApiError, BlockedReason, ErrorCode};
use crate::ids::SourceId;
use local_ci::source::SourceError;
use ulid::Ulid;

/// The most captures of one input that are tried in all (spec chapter 6, step
/// 3). local-ci does the retrying; a caller must not wrap another loop around
/// it (3 x 3 attempts would be a different contract).
pub const MAX_CAPTURE_ATTEMPTS: u32 = 3;

/// The attempt budget to hand local-ci: the request clamped to `1..=3`, and 3
/// when nothing is asked.
pub fn attempt_budget(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(MAX_CAPTURE_ATTEMPTS)
        .clamp(1, MAX_CAPTURE_ATTEMPTS)
}

/// What a refused capture is to the caller. Nothing is dispatched on any of
/// these; of the codes a capture can end with, only `SOURCE_BUSY` says "try
/// again" (the input was moving). A worker that is too busy to take a request
/// answers `OVERLOADED`, which has its own exit code and is never a capture's.
pub fn error_code(e: &SourceError) -> ErrorCode {
    use SourceError::*;
    match e {
        Busy { .. } => ErrorCode::SourceBusy,
        LfsPayloadMissing { .. }
        | UnsupportedFileType { .. }
        | NestedRepository { .. }
        | RequiredSecretMissing { .. }
        | UnsafePath { .. }
        | Unmerged { .. }
        | SecretKeyTooShort { .. }
        | ShallowClone
        | UnsupportedObjectFormat { .. }
        | OriginalConfigChanged { .. }
        | SecretTooLarge { .. }
        | RepositoryUnreadable { .. } => ErrorCode::InvalidInput,
        FilterMissing { .. } | WorkspaceNotClean { .. } => ErrorCode::Blocked,
        // the runner picks the bundle's path (one per run, in a directory it
        // made): a path that cannot be written is the runner's, not the input's
        Corrupt { .. } | SecretBundlePath { .. } => ErrorCode::Internal,
    }
}

/// The error body for a refused capture. `blocked` ones carry their reason.
pub fn api_error(e: &SourceError) -> ApiError {
    let code = error_code(e);
    if code == ErrorCode::Blocked {
        ApiError::blocked(BlockedReason::EnvironmentMismatch, e.to_string())
    } else {
        ApiError::new(code, e.to_string())
    }
}

/// The error body for any failure of a capture: a [`SourceError`] says what it
/// is, everything else is an unexpected failure.
pub fn api_error_of(e: &anyhow::Error) -> ApiError {
    match e.downcast_ref::<SourceError>() {
        Some(s) => api_error(s),
        None => ApiError::new(ErrorCode::Internal, format!("{e:#}")),
    }
}

/// local-ci names a source `src-<32 hex>` after its content. That is 128 bits,
/// so it maps one to one onto the ULID of a `src_<ULID>`: the same content
/// always gives the same [`SourceId`], which is what lets a re-capture after a
/// crash resume under the same id.
pub fn source_id_of_ci(ci_id: &str) -> Option<SourceId> {
    let hex = ci_id.strip_prefix("src-")?;
    if hex.len() != 32 || !hex.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let n = u128::from_str_radix(hex, 16).ok()?;
    SourceId::parse(&format!("src_{}", Ulid::from(n))).ok()
}

/// The inverse of [`source_id_of_ci`]: the id local-ci's store knows.
pub fn ci_id_of(id: &SourceId) -> Option<String> {
    let ulid = Ulid::from_string(id.as_str().strip_prefix("src_")?).ok()?;
    Some(format!("src-{:032x}", ulid.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_errors() -> Vec<SourceError> {
        use SourceError::*;
        let s = |x: &str| x.to_string();
        vec![
            Busy {
                attempts: 3,
                last: s("moved"),
            },
            LfsPayloadMissing {
                path: s("p"),
                oid: s("o"),
            },
            UnsupportedFileType {
                path: s("p"),
                kind: s("FIFO"),
            },
            NestedRepository { path: s("p") },
            RequiredSecretMissing { glob: s("g") },
            UnsafePath {
                path: s("p"),
                why: s("w"),
            },
            Unmerged { path: s("p") },
            Corrupt { what: s("w") },
            SecretKeyTooShort { len: 1 },
            ShallowClone,
            UnsupportedObjectFormat {
                format: s("sha256"),
            },
            FilterMissing {
                filter: s("lfs"),
                command: s("git-lfs"),
            },
            WorkspaceNotClean {
                paths: vec![s("a")],
            },
            OriginalConfigChanged { keys: vec![s("k")] },
            RepositoryUnreadable {
                path: s("p"),
                detail: s("d"),
            },
            SecretBundlePath {
                path: s("p"),
                why: s("w"),
            },
            SecretTooLarge {
                path: s(".env"),
                size: 2,
                limit: 1,
            },
        ]
    }

    #[test]
    fn attempts_are_between_one_and_three() {
        assert_eq!(attempt_budget(None), 3);
        assert_eq!(attempt_budget(Some(0)), 1);
        assert_eq!(attempt_budget(Some(1)), 1);
        assert_eq!(attempt_budget(Some(2)), 2);
        assert_eq!(attempt_budget(Some(3)), 3);
        assert_eq!(attempt_budget(Some(4)), 3);
        assert_eq!(attempt_budget(Some(u32::MAX)), 3);
    }

    #[test]
    fn only_a_busy_input_is_source_busy() {
        for e in all_errors() {
            let busy = matches!(e, SourceError::Busy { .. });
            assert_eq!(error_code(&e) == ErrorCode::SourceBusy, busy, "{e}");
        }
        let a = api_error(&all_errors()[0]);
        assert_eq!(a.code, ErrorCode::SourceBusy);
        assert!(a.message.starts_with("SOURCE_BUSY"));
        assert_eq!(a.blocked_reason, None);
    }

    #[test]
    fn each_refusal_has_its_class() {
        use ErrorCode::*;
        let codes: Vec<_> = all_errors().iter().map(error_code).collect();
        assert_eq!(
            codes,
            vec![
                SourceBusy,
                InvalidInput,
                InvalidInput,
                InvalidInput,
                InvalidInput,
                InvalidInput,
                InvalidInput,
                Internal,
                InvalidInput,
                InvalidInput,
                InvalidInput,
                Blocked,
                Blocked,
                InvalidInput,
                InvalidInput,
                Internal,
                InvalidInput
            ]
        );
    }

    #[test]
    fn a_blocked_refusal_says_why() {
        let a = api_error(&SourceError::ShallowClone);
        assert_eq!((a.code, a.blocked_reason), (ErrorCode::InvalidInput, None));
        let a = api_error(&SourceError::FilterMissing {
            filter: "lfs".into(),
            command: "git-lfs".into(),
        });
        assert_eq!(a.code, ErrorCode::Blocked);
        assert_eq!(a.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    }

    #[test]
    fn an_error_that_is_not_a_source_error_is_internal() {
        let a = api_error_of(&anyhow::anyhow!("disk full"));
        assert_eq!(a.code, ErrorCode::Internal);
        assert!(a.message.contains("disk full"));
        let a = api_error_of(&anyhow::Error::new(SourceError::ShallowClone));
        assert_eq!(a.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn the_source_id_is_a_function_of_the_content_id() {
        let ci = "src-00112233445566778899aabbccddeeff";
        let id = source_id_of_ci(ci).unwrap();
        assert!(id.as_str().starts_with("src_"));
        assert_eq!(source_id_of_ci(ci).unwrap(), id);
        assert_eq!(ci_id_of(&id).unwrap(), ci);
        for other in [
            "src-00112233445566778899aabbccddeef0",
            "src-ffffffffffffffffffffffffffffffff",
            "src-00000000000000000000000000000000",
        ] {
            let o = source_id_of_ci(other).unwrap();
            assert_ne!(o, id);
            assert_eq!(ci_id_of(&o).unwrap(), other);
        }
    }

    #[test]
    fn ids_that_are_not_content_ids_are_refused() {
        for bad in [
            "",
            "src_00112233445566778899aabbccddeeff",
            "src-0011",
            "src-00112233445566778899aabbccddeeffaa",
            "src-0011223344556677889AABBCCDDEEFF",
            "src-0011223344556677889gaabbccddeeff",
            "run-00112233445566778899aabbccddeeff",
        ] {
            assert_eq!(source_id_of_ci(bad), None, "{bad}");
        }
        let run = SourceId::generate();
        assert!(ci_id_of(&run).is_some());
    }
}
