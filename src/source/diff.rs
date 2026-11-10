//! What a peer lacks, and whether a blob that arrived is the one asked for.
//! Pure: the store and the transport are somewhere else.

use local_ci::source::FileEntry;
use std::collections::BTreeMap;

/// A blob to send: its content hash (SHA-256, hex) and size.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct BlobWant {
    pub hash: String,
    pub size: u64,
}

/// The blobs of `entries` that the peer does not have: each once (the same
/// content under many paths is one blob), sorted by hash. `peer_has` answers
/// for the peer's store.
pub fn missing_blobs(entries: &[FileEntry], peer_has: impl Fn(&str) -> bool) -> Vec<BlobWant> {
    let mut out: BTreeMap<&str, u64> = BTreeMap::new();
    for e in entries {
        if !peer_has(&e.hash) {
            out.insert(&e.hash, e.size);
        }
    }
    out.into_iter()
        .map(|(h, size)| BlobWant {
            hash: h.to_string(),
            size,
        })
        .collect()
}

/// Is `s` the name of a blob: 64 lowercase hex digits (SHA-256)? A hash from
/// outside ends up in a path of the store, so nothing else is accepted.
pub fn is_blob_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// A blob that is not what was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobMismatch {
    Hash { expected: String, actual: String },
    Size { expected: u64, actual: u64 },
}

impl std::fmt::Display for BlobMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobMismatch::Hash { expected, actual } => {
                write!(f, "blob hash {actual} is not the expected {expected}")
            }
            BlobMismatch::Size { expected, actual } => {
                write!(f, "blob size {actual} is not the expected {expected}")
            }
        }
    }
}

impl std::error::Error for BlobMismatch {}

/// Compare what was stored (hash and size, as the store computed them) with
/// what was asked for. The hash is checked first: it is the identity.
pub fn check_blob(
    want: &BlobWant,
    actual_hash: &str,
    actual_size: u64,
) -> Result<(), BlobMismatch> {
    if want.hash != actual_hash {
        return Err(BlobMismatch::Hash {
            expected: want.hash.clone(),
            actual: actual_hash.to_string(),
        });
    }
    if want.size != actual_size {
        return Err(BlobMismatch::Size {
            expected: want.size,
            actual: actual_size,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use local_ci::source::RelPath;

    fn entry(path: &str, hash: &str, size: u64) -> FileEntry {
        FileEntry {
            path: RelPath::new(path),
            mode: 0o100644,
            size,
            hash: hash.into(),
        }
    }

    #[test]
    fn only_what_the_peer_lacks_is_listed_once_and_sorted() {
        let es = vec![
            entry("z", "bb", 2),
            entry("a", "cc", 3),
            entry("copy of z", "bb", 2),
            entry("have", "aa", 1),
            entry("empty", "dd", 0),
        ];
        let got = missing_blobs(&es, |h| h == "aa");
        assert_eq!(
            got,
            vec![
                BlobWant {
                    hash: "bb".into(),
                    size: 2
                },
                BlobWant {
                    hash: "cc".into(),
                    size: 3
                },
                BlobWant {
                    hash: "dd".into(),
                    size: 0
                },
            ]
        );
        assert!(missing_blobs(&es, |_| true).is_empty());
        assert!(missing_blobs(&[], |_| false).is_empty());
        assert_eq!(missing_blobs(&es, |_| false).len(), 4);
    }

    #[test]
    fn a_blob_must_match_hash_and_size() {
        let w = BlobWant {
            hash: "aa".into(),
            size: 5,
        };
        assert_eq!(check_blob(&w, "aa", 5), Ok(()));
        let e = check_blob(&w, "bb", 5).unwrap_err();
        assert_eq!(
            e,
            BlobMismatch::Hash {
                expected: "aa".into(),
                actual: "bb".into()
            }
        );
        assert!(e.to_string().contains("not the expected aa"));
        let e = check_blob(&w, "aa", 6).unwrap_err();
        assert_eq!(
            e,
            BlobMismatch::Size {
                expected: 5,
                actual: 6
            }
        );
        assert!(e.to_string().contains("size 6"));
        // a wrong hash is reported before a wrong size
        assert!(matches!(
            check_blob(&w, "bb", 6),
            Err(BlobMismatch::Hash { .. })
        ));
    }

    #[test]
    fn a_blob_name_is_sixty_four_lowercase_hex_digits() {
        assert!(is_blob_hash(&"0123456789abcdef".repeat(4)));
        for bad in [
            "".to_string(),
            "ab".to_string(),
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
            format!("../{}", "a".repeat(61)),
            format!("{}/", "a".repeat(63)),
        ] {
            assert!(!is_blob_hash(&bad), "{bad}");
        }
    }
}
