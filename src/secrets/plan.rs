//! The pure decisions of the secret path (spec 22.3): size limits, how a bundle
//! is cut into requests, the digest form on the wire, constant-time compare,
//! literal globs, path rules, hex. No I/O and no secret value is ever put into
//! a message produced here.

use crate::ids::Digest;
use crate::source::safety::{check_entries, check_rel_path, MODE_FILE};
use crate::wire::HmacDigest;
use local_ci::source::SecretSpec;
use std::ops::Range;

/// Most files in one bundle.
pub const MAX_FILES: usize = 256;
/// Largest single file. One request must fit in a protocol frame (2 MiB, with
/// the content in base64), so a file has to fit in half of it.
pub const MAX_FILE_BYTES: usize = 512 * 1024;
/// Most bytes in one bundle.
pub const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
/// Longest path, in bytes.
pub const MAX_PATH_BYTES: usize = 1024;
/// What one `secret_bundle_put` request may carry (content plus paths, before
/// base64), so that its frame stays well below the frame limit.
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Per-file cost in a batch besides content and path: the JSON around it.
const FILE_OVERHEAD: usize = 128;

/// Why a bundle is refused. Names sizes and counts only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Limit {
    TooManyFiles { count: usize },
    FileTooLarge { index: usize, bytes: usize },
    PathTooLong { index: usize, bytes: usize },
    TooManyBytes { bytes: usize },
    TooManyGlobs { count: usize },
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Limit::TooManyFiles { count } => {
                write!(f, "{count} secret files; at most {MAX_FILES} are allowed")
            }
            Limit::FileTooLarge { index, bytes } => write!(
                f,
                "secret file #{index} is {bytes} bytes; at most {MAX_FILE_BYTES} are allowed"
            ),
            Limit::PathTooLong { index, bytes } => write!(
                f,
                "path of secret file #{index} is {bytes} bytes; at most {MAX_PATH_BYTES} are allowed"
            ),
            Limit::TooManyBytes { bytes } => write!(
                f,
                "secret files hold {bytes} bytes; at most {MAX_TOTAL_BYTES} are allowed"
            ),
            Limit::TooManyGlobs { count } => write!(
                f,
                "{count} secret_files globs are declared; at most {MAX_DECLARED_GLOBS} are allowed"
            ),
        }
    }
}

/// Most `[source] secret_files` globs a project may declare.
pub const MAX_DECLARED_GLOBS: usize = 64;

/// The declared globs: not too many, none longer than a path may be.
pub fn check_declared(specs: &[SecretSpec]) -> Result<(), Limit> {
    if specs.len() > MAX_DECLARED_GLOBS {
        return Err(Limit::TooManyGlobs { count: specs.len() });
    }
    if let Some((index, s)) = specs
        .iter()
        .enumerate()
        .find(|(_, s)| s.glob.len() > MAX_PATH_BYTES)
    {
        return Err(Limit::PathTooLong {
            index,
            bytes: s.glob.len(),
        });
    }
    Ok(())
}

/// Check `(path length, content length)` of every file against the limits.
pub fn check_limits(sizes: &[(usize, usize)]) -> Result<(), Limit> {
    if sizes.len() > MAX_FILES {
        return Err(Limit::TooManyFiles { count: sizes.len() });
    }
    let mut total = 0usize;
    for (index, (path, content)) in sizes.iter().enumerate() {
        if *path > MAX_PATH_BYTES {
            return Err(Limit::PathTooLong {
                index,
                bytes: *path,
            });
        }
        if *content > MAX_FILE_BYTES {
            return Err(Limit::FileTooLarge {
                index,
                bytes: *content,
            });
        }
        total = total.saturating_add(*content);
    }
    if total > MAX_TOTAL_BYTES {
        return Err(Limit::TooManyBytes { bytes: total });
    }
    Ok(())
}

/// Cut files into consecutive requests, none above [`MAX_BATCH_BYTES`] (each
/// file counts its content, its path and a fixed overhead). A single file is
/// never split, and a file that fits the per-file limit fits a batch alone.
pub fn plan_batches(sizes: &[(usize, usize)]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut used = 0usize;
    for (i, (path, content)) in sizes.iter().enumerate() {
        let cost = path + content + FILE_OVERHEAD;
        if i > start && used + cost > MAX_BATCH_BYTES {
            out.push(start..i);
            start = i;
            used = 0;
        }
        used += cost;
    }
    if start < sizes.len() {
        out.push(start..sizes.len());
    }
    out
}

/// The wire type of a digest local-ci recorded as `hmac-sha256:<hex>`: the
/// same text, checked. `None` for anything else (a plain `sha256:` hash
/// included).
pub fn to_wire_digest(ci: &str) -> Option<HmacDigest> {
    HmacDigest::parse(ci).ok()
}

/// Equal in time that does not depend on where the first difference is.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// `path` as a glob that matches exactly that path.
pub fn escape_glob(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if matches!(c, '\\' | '*' | '?' | '[' | ']' | '{' | '}') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// The declared globs first, then one literal glob per `.worktreeinclude`
/// path that no declared glob already spells the same. Declared ones keep
/// their `required` flag; included ones are never required.
pub fn merge_specs(declared: &[SecretSpec], included_paths: &[String]) -> Vec<SecretSpec> {
    let mut out: Vec<SecretSpec> = declared.to_vec();
    for p in included_paths {
        let glob = escape_glob(p);
        if !out.iter().any(|s| s.glob == glob || s.glob == *p) {
            out.push(SecretSpec {
                glob,
                required: false,
            });
        }
    }
    out
}

/// Why a path is not placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadPath {
    Unsafe(String),
    TooLong(usize),
}

impl std::fmt::Display for BadPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BadPath::Unsafe(why) => write!(f, "unsafe secret file path: {why}"),
            BadPath::TooLong(n) => write!(f, "secret file path of {n} bytes is too long"),
        }
    }
}

/// A path the worker may place: relative, inside the workspace, not Git's
/// own directory, not too long.
pub fn check_path(path: &str) -> Result<(), BadPath> {
    if path.len() > MAX_PATH_BYTES {
        return Err(BadPath::TooLong(path.len()));
    }
    check_rel_path(path.as_bytes()).map_err(|v| BadPath::Unsafe(format!("{v:?}")))
}

/// All paths of a bundle are safe and no two name the same file (also on a
/// file system that folds case or Unicode forms).
pub fn check_paths<'a>(paths: impl IntoIterator<Item = &'a str>) -> Result<(), BadPath> {
    let all: Vec<&str> = paths.into_iter().collect();
    for p in &all {
        check_path(p)?;
    }
    check_entries(all.iter().map(|p| (p.as_bytes(), MODE_FILE)))
        .map_err(|e| BadPath::Unsafe(e.to_string()))
}

/// What a bundle's manifest listing is digested as, for the storage's
/// `secret_bundle_ref`: path and HMAC of every file, sorted by path, never the
/// content (the HMAC is keyed, so the digest reveals nothing about it).
pub fn listing_digest(entries: &[(String, HmacDigest)]) -> Digest {
    let mut sorted: Vec<&(String, HmacDigest)> = entries.iter().collect();
    sorted.sort();
    let mut text = String::new();
    for (path, hmac) in sorted {
        text.push_str(&path.len().to_string());
        text.push(':');
        text.push_str(path);
        text.push(' ');
        text.push_str(hmac.as_str());
        text.push('\n');
    }
    Digest::of_bytes(text.as_bytes())
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 15) as usize] as char);
    }
    s
}

/// Lower-case hex only. `None` for an odd length or another character.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    b.chunks(2)
        .map(|p| Some(nib(p[0])? * 16 + nib(p[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_limits_have_their_documented_values() {
        assert_eq!(MAX_FILES, 256);
        assert_eq!(MAX_FILE_BYTES, 524_288);
        assert_eq!(MAX_TOTAL_BYTES, 8_388_608);
        assert_eq!(MAX_PATH_BYTES, 1024);
        assert_eq!(MAX_BATCH_BYTES, 1_048_576);
        assert_eq!(FILE_OVERHEAD, 128);
    }

    #[test]
    fn limits_accept_the_boundaries_and_refuse_one_over() {
        assert!(check_limits(&[]).is_ok());
        assert!(check_limits(&[(MAX_PATH_BYTES, MAX_FILE_BYTES)]).is_ok());
        assert_eq!(
            check_limits(&[(MAX_PATH_BYTES + 1, 1)]),
            Err(Limit::PathTooLong {
                index: 0,
                bytes: MAX_PATH_BYTES + 1
            })
        );
        assert_eq!(
            check_limits(&[(1, 1), (1, MAX_FILE_BYTES + 1)]),
            Err(Limit::FileTooLarge {
                index: 1,
                bytes: MAX_FILE_BYTES + 1
            })
        );
        let many = vec![(1, 1); MAX_FILES];
        assert!(check_limits(&many).is_ok());
        let too_many = vec![(1, 1); MAX_FILES + 1];
        assert_eq!(
            check_limits(&too_many),
            Err(Limit::TooManyFiles {
                count: MAX_FILES + 1
            })
        );
        let files = MAX_TOTAL_BYTES / MAX_FILE_BYTES;
        let ok = vec![(1, MAX_FILE_BYTES); files];
        assert!(check_limits(&ok).is_ok());
        let over = vec![(1, MAX_FILE_BYTES); files + 1];
        assert_eq!(
            check_limits(&over),
            Err(Limit::TooManyBytes {
                bytes: MAX_FILE_BYTES * (files + 1)
            })
        );
    }

    #[test]
    fn declared_globs_are_bounded_in_number_and_length() {
        let spec = |glob: String| SecretSpec {
            glob,
            required: false,
        };
        let many = |n: usize| (0..n).map(|i| spec(format!("g{i}"))).collect::<Vec<_>>();
        assert_eq!(check_declared(&[]), Ok(()));
        assert_eq!(check_declared(&many(MAX_DECLARED_GLOBS)), Ok(()));
        assert_eq!(
            check_declared(&many(MAX_DECLARED_GLOBS + 1)),
            Err(Limit::TooManyGlobs {
                count: MAX_DECLARED_GLOBS + 1
            })
        );
        assert_eq!(check_declared(&[spec("a".repeat(MAX_PATH_BYTES))]), Ok(()));
        assert_eq!(
            check_declared(&[spec("a".into()), spec("a".repeat(MAX_PATH_BYTES + 1))]),
            Err(Limit::PathTooLong {
                index: 1,
                bytes: MAX_PATH_BYTES + 1
            })
        );
    }

    #[test]
    fn limit_messages_name_sizes_only() {
        for l in [
            Limit::TooManyFiles { count: 300 },
            Limit::FileTooLarge { index: 2, bytes: 9 },
            Limit::PathTooLong { index: 3, bytes: 9 },
            Limit::TooManyBytes { bytes: 77 },
            Limit::TooManyGlobs { count: 65 },
        ] {
            assert!(!l.to_string().is_empty());
        }
        assert!(Limit::TooManyGlobs { count: 65 }.to_string().contains("65"));
        assert!(Limit::FileTooLarge { index: 2, bytes: 9 }
            .to_string()
            .contains("#2"));
    }

    #[test]
    fn batches_are_consecutive_cover_everything_and_stay_in_budget() {
        assert!(plan_batches(&[]).is_empty());
        assert_eq!(plan_batches(&[(5, 5)]), vec![0..1]);
        // a first file that alone is over the budget is not preceded by an empty batch
        assert_eq!(plan_batches(&[(1, MAX_BATCH_BYTES)]), vec![0..1]);
        // three files of 400 KiB: two do not fit one 1 MiB batch with a third
        let k = 400 * 1024;
        let b = plan_batches(&[(5, k), (5, k), (5, k)]);
        assert_eq!(b, vec![0..2, 2..3]);
        // exactly the budget fits; one byte more starts a new batch
        let cost = |c: usize| 1 + c + FILE_OVERHEAD;
        let half = (MAX_BATCH_BYTES - cost(0) * 2) / 2;
        assert_eq!(plan_batches(&[(1, half), (1, half)]), vec![0..2]);
        assert_eq!(plan_batches(&[(1, half), (1, half + 1)]), vec![0..1, 1..2]);
        // a file larger than the budget by itself still gets its own batch
        assert_eq!(
            plan_batches(&[(1, 1), (1, MAX_BATCH_BYTES), (1, 1)]),
            vec![0..1, 1..2, 2..3]
        );
        // every maximum-size file fits a batch alone
        const { assert!(MAX_FILE_BYTES + MAX_PATH_BYTES + FILE_OVERHEAD <= MAX_BATCH_BYTES) };
    }

    #[test]
    fn the_wire_digest_keeps_the_hmac_and_refuses_other_forms() {
        let hex = "ab".repeat(32);
        let d = to_wire_digest(&format!("hmac-sha256:{hex}")).unwrap();
        assert_eq!(d.as_str(), format!("hmac-sha256:{hex}"));
        assert!(to_wire_digest(&format!("sha256:{hex}")).is_none());
        assert!(to_wire_digest("hmac-sha256:zz").is_none());
        assert!(to_wire_digest("").is_none());
    }

    #[test]
    fn ct_eq_compares_length_and_every_byte() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"xbc"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(!ct_eq(b"ab", b"abc"));
    }

    #[test]
    fn globs_are_escaped_literally() {
        assert_eq!(escape_glob(".env"), ".env");
        assert_eq!(
            escape_glob("a*b?c[d]{e}\\f"),
            "a\\*b\\?c\\[d\\]\\{e\\}\\\\f"
        );
        assert_eq!(escape_glob("設定/.env"), "設定/.env");
    }

    #[test]
    fn declared_specs_come_first_and_are_not_duplicated() {
        let declared = vec![
            SecretSpec {
                glob: ".env".into(),
                required: true,
            },
            SecretSpec {
                glob: "cfg/*.key".into(),
                required: false,
            },
        ];
        let out = merge_specs(
            &declared,
            &[".env".into(), "a[1].env".into(), "a[1].env".into()],
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], declared[0]);
        assert_eq!(out[1], declared[1]);
        assert_eq!(out[2].glob, "a\\[1\\].env");
        assert!(!out[2].required);
        // a declared glob that spells the path escaped is the same file too
        let out = merge_specs(
            &[SecretSpec {
                glob: "a\\[1\\].env".into(),
                required: true,
            }],
            &["a[1].env".into()],
        );
        assert_eq!(out.len(), 1);
        assert!(out[0].required);
        assert!(merge_specs(&[], &[]).is_empty());
    }

    #[test]
    fn paths_must_stay_inside_and_not_repeat() {
        assert!(check_path(".env").is_ok());
        assert!(check_path("a/b/.env").is_ok());
        assert!(matches!(check_path("/etc/passwd"), Err(BadPath::Unsafe(_))));
        assert!(matches!(check_path("../x"), Err(BadPath::Unsafe(_))));
        assert!(matches!(check_path("a/../x"), Err(BadPath::Unsafe(_))));
        assert!(matches!(check_path(".git/config"), Err(BadPath::Unsafe(_))));
        assert!(matches!(check_path(""), Err(BadPath::Unsafe(_))));
        assert!(check_path(&"a".repeat(MAX_PATH_BYTES)).is_ok());
        assert_eq!(
            check_path(&"a".repeat(MAX_PATH_BYTES + 1)),
            Err(BadPath::TooLong(MAX_PATH_BYTES + 1))
        );
        assert!(check_paths([".env", "a/.env"]).is_ok());
        assert!(check_paths([]).is_ok());
        assert!(matches!(
            check_paths([".env", "../x"]),
            Err(BadPath::Unsafe(_))
        ));
        assert!(matches!(
            check_paths([".env", ".env"]),
            Err(BadPath::Unsafe(_))
        ));
        assert!(matches!(
            check_paths(["A/.env", "a/.ENV"]),
            Err(BadPath::Unsafe(_))
        ));
        assert!(!BadPath::TooLong(3).to_string().is_empty());
        assert!(BadPath::Unsafe("x".into()).to_string().contains('x'));
    }

    #[test]
    fn the_listing_digest_ignores_order_and_separates_fields() {
        let h = |c: char| {
            HmacDigest::parse(&format!("hmac-sha256:{}", c.to_string().repeat(64))).unwrap()
        };
        let a = ("a".to_string(), h('1'));
        let b = ("b".to_string(), h('2'));
        assert_eq!(
            listing_digest(&[a.clone(), b.clone()]),
            listing_digest(&[b.clone(), a.clone()])
        );
        assert_ne!(
            listing_digest(std::slice::from_ref(&a)),
            listing_digest(std::slice::from_ref(&b))
        );
        // same HMAC under another path, same path under another HMAC
        assert_ne!(
            listing_digest(&[("ab".into(), h('1'))]),
            listing_digest(&[("a".into(), h('1'))])
        );
        assert_ne!(
            listing_digest(&[("a".into(), h('1'))]),
            listing_digest(&[("a".into(), h('2'))])
        );
        assert_ne!(listing_digest(&[]), listing_digest(&[a]));
    }

    #[test]
    fn hex_round_trips_and_refuses_bad_input() {
        assert_eq!(hex_encode(&[0, 15, 16, 255]), "000f10ff");
        assert_eq!(hex_decode("000f10ff").unwrap(), vec![0, 15, 16, 255]);
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
        assert!(hex_decode("0").is_none());
        assert!(hex_decode("0G").is_none());
        assert!(hex_decode("0F").is_none());
        assert!(hex_decode("g0").is_none());
        assert!(hex_decode("0g").is_none());
        // every nibble position counts
        assert_eq!(hex_decode("10").unwrap(), vec![16]);
        assert_eq!(hex_decode("01").unwrap(), vec![1]);
        assert_eq!(hex_decode("a5").unwrap(), vec![0xa5]);
        assert_eq!(hex_decode("5a").unwrap(), vec![0x5a]);
        assert_eq!(hex_decode("9f").unwrap(), vec![0x9f]);
    }
}
