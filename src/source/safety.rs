//! What may be written when a source is unpacked (spec chapter 6, last
//! paragraph): pure decisions over paths, modes and file types, no I/O.
//!
//! The capturing side already refuses these inputs, but the unpacking side
//! reads a manifest that came from another host. It decides again, over bytes
//! (paths are never converted lossily), before a single file is written.

use std::collections::BTreeSet;

/// Mode of a regular file.
pub const MODE_FILE: u32 = 0o100644;
/// Mode of an executable regular file.
pub const MODE_EXEC: u32 = 0o100755;
/// Mode of a symlink: the content of the entry is the link value.
pub const MODE_SYMLINK: u32 = 0o120000;

/// Why one path (or one entry) is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    Empty,
    Absolute,
    NulByte,
    /// A `.` component.
    CurrentDir,
    /// A `..` component.
    ParentDir,
    /// `a//b`, or a trailing `/`.
    EmptyComponent,
    /// A component naming Git's own directory (any letter case: some file
    /// systems fold it).
    GitDir,
    /// A mode other than a regular file or a symlink.
    BadMode(u32),
    /// The same path twice.
    Duplicate,
    /// The path lies below an entry that is a file or a symlink, so writing it
    /// would go through that entry.
    ThroughEntry(Vec<u8>),
}

/// A refused entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryViolation {
    pub path: Vec<u8>,
    pub why: Violation,
}

impl std::fmt::Display for EntryViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing path {:?}: {:?}",
            String::from_utf8_lossy(&self.path),
            self.why
        )
    }
}

impl std::error::Error for EntryViolation {}

/// Is `p` a path that stays inside the directory it is unpacked into?
pub fn check_rel_path(p: &[u8]) -> Result<(), Violation> {
    if p.is_empty() {
        return Err(Violation::Empty);
    }
    if p.contains(&0) {
        return Err(Violation::NulByte);
    }
    if p[0] == b'/' {
        return Err(Violation::Absolute);
    }
    for c in p.split(|b| *b == b'/') {
        match c {
            b"" => return Err(Violation::EmptyComponent),
            b"." => return Err(Violation::CurrentDir),
            b".." => return Err(Violation::ParentDir),
            _ if names_git_dir(c) => return Err(Violation::GitDir),
            _ => {}
        }
    }
    Ok(())
}

/// Only regular files and symlinks are unpacked.
pub fn check_mode(mode: u32) -> Result<(), Violation> {
    match mode {
        MODE_FILE | MODE_EXEC | MODE_SYMLINK => Ok(()),
        m => Err(Violation::BadMode(m)),
    }
}

/// The key two paths are compared by: what a case-folding, normalizing file
/// system (macOS, Windows) would treat as the same name. Each component is
/// put in Unicode NFC, lowercased, and stripped of the characters HFS+ ignores;
/// a component that is not UTF-8 is only ASCII-lowercased.
fn fold_component(c: &[u8]) -> Vec<u8> {
    use unicode_normalization::UnicodeNormalization;
    match std::str::from_utf8(c) {
        Ok(s) => {
            let nfc: String = s.nfc().collect();
            nfc.to_lowercase()
                .nfc()
                .filter(|ch| {
                    !matches!(*ch,
                        '\u{200c}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{206a}'..='\u{206f}' | '\u{feff}')
                })
                .collect::<String>()
                .into_bytes()
        }
        Err(_) => c.to_ascii_lowercase(),
    }
}

/// Does `c` name Git's own directory on some file system (any case, any
/// Unicode form, `GIT~1` as NTFS spells it)?
fn names_git_dir(c: &[u8]) -> bool {
    let k = fold_component(c);
    k == b".git" || k == b"git~1"
}

/// Decide over all entries `(path, mode)` of a source: every path is safe,
/// every mode is allowed, no path appears twice, and none lies below another
/// entry (a file or a symlink, which would make the write go through it).
/// "Twice" and "below" are decided on folded names (see [`fold_component`]):
/// `Link` and `link`, or a name in NFC and in NFD, are one path on the file
/// systems that fold.
pub fn check_entries<'a>(
    entries: impl IntoIterator<Item = (&'a [u8], u32)>,
) -> Result<(), EntryViolation> {
    let all: Vec<(&[u8], u32)> = entries.into_iter().collect();
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for (p, mode) in &all {
        let fail = |why| EntryViolation {
            path: p.to_vec(),
            why,
        };
        check_rel_path(p).map_err(fail)?;
        check_mode(*mode).map_err(fail)?;
        if !seen.insert(fold_path(p)) {
            return Err(fail(Violation::Duplicate));
        }
    }
    for (p, _) in &all {
        let mut key: Vec<u8> = vec![];
        let mut end = 0;
        let comps: Vec<&[u8]> = p.split(|b| *b == b'/').collect();
        for c in &comps[..comps.len() - 1] {
            if !key.is_empty() {
                key.push(b'/');
            }
            key.extend(fold_component(c));
            end += c.len() + usize::from(end > 0);
            if seen.contains(&key) {
                return Err(EntryViolation {
                    path: p.to_vec(),
                    why: Violation::ThroughEntry(p[..end].to_vec()),
                });
            }
        }
    }
    Ok(())
}

fn fold_path(p: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = vec![];
    for (i, c) in p.split(|b| *b == b'/').enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend(fold_component(c));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(p: &str) -> Result<(), Violation> {
        check_rel_path(p.as_bytes())
    }

    #[test]
    fn ordinary_paths_pass() {
        for p in [
            "a",
            "a/b",
            "dir/.gitignore",
            "x/.github/ci.yml",
            "..a",
            "a..",
            "a.b/c",
        ] {
            assert_eq!(rel(p), Ok(()), "{p}");
        }
        assert_eq!(check_rel_path("sub dir/\u{3042}.txt".as_bytes()), Ok(()));
        assert_eq!(check_rel_path(&[0xff, b'/', 0xfe]), Ok(()));
    }

    #[test]
    fn unsafe_paths_are_refused_for_their_own_reason() {
        assert_eq!(rel(""), Err(Violation::Empty));
        assert_eq!(rel("/etc/passwd"), Err(Violation::Absolute));
        assert_eq!(check_rel_path(b"a\0b"), Err(Violation::NulByte));
        assert_eq!(rel("./a"), Err(Violation::CurrentDir));
        assert_eq!(rel("a/./b"), Err(Violation::CurrentDir));
        assert_eq!(rel("../a"), Err(Violation::ParentDir));
        assert_eq!(rel("a/../b"), Err(Violation::ParentDir));
        assert_eq!(rel("a/.."), Err(Violation::ParentDir));
        assert_eq!(rel("a//b"), Err(Violation::EmptyComponent));
        assert_eq!(rel("a/"), Err(Violation::EmptyComponent));
        assert_eq!(rel(".git"), Err(Violation::GitDir));
        assert_eq!(rel("a/.git/hooks/x"), Err(Violation::GitDir));
        assert_eq!(rel(".GIT/config"), Err(Violation::GitDir));
        assert_eq!(rel("sub/.Git"), Err(Violation::GitDir));
        // names HFS+ and NTFS fold to Git's directory
        assert_eq!(rel(".g\u{200c}it/config"), Err(Violation::GitDir));
        assert_eq!(rel("GIT~1/config"), Err(Violation::GitDir));
        assert_eq!(rel(".gitx"), Ok(()));
    }

    #[test]
    fn only_files_and_symlinks_have_an_allowed_mode() {
        for m in [MODE_FILE, MODE_EXEC, MODE_SYMLINK] {
            assert_eq!(check_mode(m), Ok(()));
        }
        for m in [0o040000, 0o160000, 0o100600, 0o100664, 0o120755, 0, 0o644] {
            assert_eq!(check_mode(m), Err(Violation::BadMode(m)));
        }
    }

    fn entries(v: &[(&str, u32)]) -> Result<(), EntryViolation> {
        check_entries(v.iter().map(|(p, m)| (p.as_bytes(), *m)))
    }

    #[test]
    fn a_clean_set_of_entries_passes() {
        assert_eq!(
            entries(&[
                ("a", MODE_FILE),
                ("a.txt", MODE_FILE),
                ("d/b", MODE_EXEC),
                ("d/link", MODE_SYMLINK),
                ("d2/x", MODE_FILE),
            ]),
            Ok(())
        );
        assert_eq!(entries(&[]), Ok(()));
    }

    #[test]
    fn the_first_bad_path_or_mode_is_reported_with_its_path() {
        let e = entries(&[("ok", MODE_FILE), ("../x", MODE_FILE)]).unwrap_err();
        assert_eq!(
            (e.path.as_slice(), e.why),
            (&b"../x"[..], Violation::ParentDir)
        );
        let e = entries(&[("d", 0o040000)]).unwrap_err();
        assert_eq!(e.why, Violation::BadMode(0o040000));
        assert!(e.to_string().contains("\"d\""));
    }

    #[test]
    fn a_duplicate_path_is_refused() {
        let e = entries(&[("a", MODE_FILE), ("b", MODE_FILE), ("a", MODE_EXEC)]).unwrap_err();
        assert_eq!(
            (e.path.as_slice(), e.why),
            (&b"a"[..], Violation::Duplicate)
        );
    }

    #[test]
    fn writing_below_a_symlink_or_a_file_is_refused() {
        // the classic: a symlink that points outside, then a file "through" it
        let e = entries(&[("link", MODE_SYMLINK), ("link/escape", MODE_FILE)]).unwrap_err();
        assert_eq!(e.path, b"link/escape");
        assert_eq!(e.why, Violation::ThroughEntry(b"link".to_vec()));
        // the order of the entries does not matter
        let e = entries(&[("a/b/c", MODE_FILE), ("a/b", MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::ThroughEntry(b"a/b".to_vec()));
        // a sibling that merely shares a prefix is fine
        assert_eq!(entries(&[("ab", MODE_FILE), ("a/b", MODE_FILE)]), Ok(()));
        assert_eq!(entries(&[("a", MODE_FILE), ("a.b/c", MODE_FILE)]), Ok(()));
    }

    #[test]
    fn names_that_a_folding_file_system_merges_are_one_path() {
        // case
        let e = entries(&[("Link", MODE_SYMLINK), ("link", MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::Duplicate);
        // the classic: a symlink, then a file through it under another case
        let e = entries(&[("Link", MODE_SYMLINK), ("link/x", MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::ThroughEntry(b"link".to_vec()));
        let e = entries(&[
            ("link/x", MODE_FILE),
            ("d/Link", MODE_FILE),
            ("LINK", MODE_FILE),
        ])
        .unwrap_err();
        assert_eq!(e.why, Violation::ThroughEntry(b"link".to_vec()));
        // Unicode form: "e" + combining acute against the precomposed letter
        let nfd = "caf\u{65}\u{301}";
        let nfc = "caf\u{e9}";
        let e = entries(&[(nfd, MODE_FILE), (nfc, MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::Duplicate);
        let e = entries(&[(nfd, MODE_SYMLINK), (&format!("{nfc}/x"), MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::ThroughEntry(nfc.as_bytes().to_vec()));
        // both at once
        let e = entries(&[("CAF\u{c9}", MODE_FILE), (nfc, MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::Duplicate);
        // non-UTF-8 names are folded by ASCII case only, and never panic
        let e =
            check_entries([(&b"\xffA"[..], MODE_FILE), (&b"\xffa"[..], MODE_FILE)]).unwrap_err();
        assert_eq!(e.why, Violation::Duplicate);
        // different names stay different
        assert_eq!(entries(&[("a", MODE_FILE), ("b/a", MODE_FILE)]), Ok(()));
    }
}
