//! The `KEY=VALUE` values of a dotenv-style secret file, for the log mask
//! (spec 22.3, "ログ"). Pure.
//!
//! What counts as a value: the text after the first `=` of a line whose key is
//! a plain name (optionally after `export `). Single and double quoted values
//! may continue over lines; an unquoted value ends at ` #`. A double-quoted
//! value is taken both as written and with `\n \r \t \" \\` unescaped, because
//! a program may print either form. A value with line breaks is masked as a
//! whole and also line by line, since logs are cut at lines.
//!
//! A value shorter than [`MIN_MASK_BYTES`] is not masked: replacing every `1`
//! or `on` in a log would hide more than it protects, and a value that short
//! carries no secrecy that masking could keep. Nor is a common setting value
//! (`true`, `false`, `null`, `test`, `development`, `production`,
//! `localhost` and the like, in any case). The names of such keys are
//! returned (never the values) so the caller can say so.
//!
//! Only `KEY=VALUE` text is understood. A secret that is not of that shape is
//! not masked, and the caller must not rely on it: the `_authToken` line of an
//! `.npmrc` (`//host/:_authToken=...`, whose "key" is not a plain name), the
//! fields of a JSON file, a PEM block, a certificate. Masking is a safety
//! net for dotenv files, not a guarantee for every kind of secret file.

use std::collections::BTreeSet;

/// Shortest value (in bytes) that is masked.
pub const MIN_MASK_BYTES: usize = 8;

/// Values that are settings, not secrets, and appear in every log: never
/// masked (compared without regard to case). Their keys are reported as
/// skipped.
const COMMON_VALUES: &[&str] = &[
    "true",
    "false",
    "null",
    "none",
    "test",
    "testing",
    "development",
    "production",
    "staging",
    "localhost",
    "undefined",
    "enabled",
    "disabled",
    "debug",
];

fn is_common(v: &[u8]) -> bool {
    COMMON_VALUES
        .iter()
        .any(|c| v.eq_ignore_ascii_case(c.as_bytes()))
}

#[derive(Default, Clone, PartialEq, Eq)]
pub struct Values {
    /// The strings to mask, each at least [`MIN_MASK_BYTES`] long, no repeats.
    pub values: Vec<Vec<u8>>,
    /// Keys whose value (or part of it) was too short to mask.
    pub skipped_keys: Vec<String>,
}

/// Shows how many values there are and how long, never a value or a key.
impl std::fmt::Debug for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Values")
            .field("values", &self.values.len())
            .field(
                "value_bytes",
                &self.values.iter().map(Vec::len).collect::<Vec<_>>(),
            )
            .field("skipped_keys", &self.skipped_keys.len())
            .finish()
    }
}

fn is_key(k: &[u8]) -> bool {
    let Some(first) = k.first() else {
        return false;
    };
    (first.is_ascii_alphabetic() || *first == b'_')
        && k.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

fn trim(s: &[u8]) -> &[u8] {
    let start = s
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(s.len());
    let end = s
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map_or(start, |e| e + 1);
    &s[start..end]
}

fn unescape(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut it = s.iter().copied().peekable();
    while let Some(c) = it.next() {
        if c == b'\\' {
            let mapped = it.peek().and_then(|n| match n {
                b'n' => Some(b'\n'),
                b'r' => Some(b'\r'),
                b't' => Some(b'\t'),
                b'"' => Some(b'"'),
                b'\\' => Some(b'\\'),
                _ => None,
            });
            if let Some(m) = mapped {
                out.push(m);
                it.next();
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Index of the quote that closes a value opened with `quote`, in `rest` (the
/// text after the opening quote). A backslash escapes the next byte in double
/// quotes only.
fn closing_quote(rest: &[u8], quote: u8) -> Option<usize> {
    let mut escaped = false;
    for (i, &c) in rest.iter().enumerate() {
        if escaped {
            escaped = false;
        } else if quote == b'"' && c == b'\\' {
            escaped = true;
        } else if c == quote {
            return Some(i);
        }
    }
    None
}

fn unquoted(v: &[u8]) -> &[u8] {
    let end = v
        .windows(2)
        .position(|w| w[0].is_ascii_whitespace() && w[1] == b'#')
        .unwrap_or(v.len());
    trim(&v[..end])
}

/// The text of a quoted value that starts in `first` (after the opening
/// quote) and may continue over the following lines of `lines`.
fn quoted<'a>(first: &[u8], quote: u8, lines: &mut impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut buf = first.to_vec();
    loop {
        if let Some(n) = closing_quote(&buf, quote) {
            buf.truncate(n);
            return buf;
        }
        match lines.next() {
            Some(l) => {
                buf.push(b'\n');
                buf.extend_from_slice(l);
            }
            // never closed: the rest of the file is the value
            None => return trim(&buf).to_vec(),
        }
    }
}

/// All values of `content` that must be masked.
pub fn secret_values(content: &[u8]) -> Values {
    let mut found: Vec<(String, Vec<u8>)> = Vec::new();
    let mut lines = content.split(|c| *c == b'\n');
    while let Some(raw) = lines.next() {
        let line = trim(raw);
        let line = line
            .strip_prefix(b"export")
            .filter(|r| r.first().is_some_and(|c| c.is_ascii_whitespace()))
            .map_or(line, trim);
        if line.first().is_none_or(|c| *c == b'#') {
            continue;
        }
        let Some(eq) = line.iter().position(|c| *c == b'=') else {
            continue;
        };
        let key = trim(&line[..eq]);
        if !is_key(key) {
            continue;
        }
        let key = String::from_utf8_lossy(key).into_owned();
        let value = trim(&line[eq..][1..]);
        match value.first() {
            Some(&q) if q == b'"' || q == b'\'' => {
                let inner = quoted(&value[1..], q, &mut lines);
                let decoded = (q == b'"').then(|| unescape(&inner));
                found.push((key.clone(), inner.clone()));
                if let Some(un) = decoded.filter(|un| *un != inner) {
                    found.push((key, un));
                }
            }
            Some(_) => found.push((key, unquoted(value).to_vec())),
            None => {}
        }
    }
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut out = Values::default();
    let mut skipped: BTreeSet<String> = BTreeSet::new();
    for (key, value) in found {
        let mut pieces: Vec<&[u8]> = vec![&value];
        if value.contains(&b'\n') {
            pieces.extend(value.split(|c| *c == b'\n').map(trim));
        }
        for piece in pieces {
            if piece.is_empty() {
                continue;
            }
            if piece.len() < MIN_MASK_BYTES || is_common(piece) {
                skipped.insert(key.clone());
            } else if seen.insert(piece.to_vec()) {
                out.values.push(piece.to_vec());
            }
        }
    }
    out.skipped_keys = skipped.into_iter().collect();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vals(s: &str) -> Vec<String> {
        secret_values(s.as_bytes())
            .values
            .into_iter()
            .map(|v| String::from_utf8(v).unwrap())
            .collect()
    }

    #[test]
    fn plain_pairs_comments_export_and_blank_lines() {
        let v = vals("# note\n\nA_KEY=abcd1234\r\nexport B.key = xyz-5678 \nnot a pair\n=novalue1\n9BAD=aaaaaaaa\nexported=zzzz9999\nexport=wwww8888\n");
        assert_eq!(v, ["abcd1234", "xyz-5678", "zzzz9999", "wwww8888"]);
    }

    #[test]
    fn an_unquoted_value_ends_at_a_comment_but_keeps_a_hash_inside() {
        assert_eq!(vals("A=abcd#def # tail\n"), ["abcd#def"]);
        // masking too much is the safe side: a leading `#` stays in the value
        assert_eq!(vals("A=# alone!\n"), ["# alone!"]);
        assert_eq!(vals("A=value1234 \t#x"), ["value1234"]);
    }

    #[test]
    fn quoted_values_lose_their_quotes_and_may_span_lines() {
        assert_eq!(vals("A='single quoted'\n"), ["single quoted"]);
        assert_eq!(
            vals("A=\"double \\\"q\\\" x\"\n"),
            ["double \\\"q\\\" x", "double \"q\" x"]
        );
        assert_eq!(
            vals("  A =  \"abcd efgh\"  \nB=next-value\n"),
            ["abcd efgh", "next-value"]
        );
        let multi =
            vals("K=\"-----BEGIN KEY-----\nMIIBabcdef\n-----END KEY-----\"\nAFTER=after-1234\n");
        assert_eq!(
            multi,
            [
                "-----BEGIN KEY-----\nMIIBabcdef\n-----END KEY-----",
                "-----BEGIN KEY-----",
                "MIIBabcdef",
                "-----END KEY-----",
                "after-1234"
            ]
        );
        // unterminated: the rest of the file is the value
        assert_eq!(
            vals("A=\"open value\nmore text here\n"),
            ["open value\nmore text here", "open value", "more text here"]
        );
    }

    #[test]
    fn escapes_in_double_quotes_are_also_masked_in_their_decoded_form() {
        let v = vals("A=\"line1-aaa\\nline2-bbb\\t\\r\\\\x\\q\"\n");
        assert_eq!(v[0], "line1-aaa\\nline2-bbb\\t\\r\\\\x\\q");
        assert_eq!(v[1], "line1-aaa\nline2-bbb\t\r\\x\\q");
        assert_eq!(v[2], "line1-aaa");
        assert_eq!(v[3], "line2-bbb\t\r\\x\\q");
        // a trailing backslash is kept
        assert_eq!(vals("A=\"abcdefg\\\\\"\n"), ["abcdefg\\\\", "abcdefg\\"]);
        // single quotes do not unescape
        assert_eq!(vals("A='abcd\\nefgh'\n"), ["abcd\\nefgh"]);
    }

    #[test]
    fn short_values_are_skipped_by_key_and_never_returned() {
        let r = secret_values(b"A=1\nB=abc\nC=abcdefg\nD=\"x\ny\"\nA=on\nE=\nF=abcdefgh\n");
        assert_eq!(r.values, vec![b"abcdefgh".to_vec()]);
        assert_eq!(r.skipped_keys, ["A", "B", "C", "D"]);
        // exactly MIN_MASK_BYTES is masked
        assert_eq!(MIN_MASK_BYTES, 8);
        assert_eq!(secret_values(b"A=abcdefgh").values.len(), 1);
        assert_eq!(secret_values(b"A=abcdefg").values.len(), 0);
    }

    #[test]
    fn common_setting_values_are_not_masked_in_any_case() {
        for v in [
            "true",
            "FALSE",
            "null",
            "Test",
            "development",
            "Production",
            "LOCALHOST",
            "undefined",
            "disabled",
        ] {
            let r = secret_values(format!("MODE={v}\n").as_bytes());
            assert!(r.values.is_empty(), "{v}");
            // a skipped key is reported only when the value is short or common
            assert_eq!(r.skipped_keys, ["MODE"], "{v}");
        }
        // a value that merely contains one is a value
        assert_eq!(vals("A=production-key-1\n"), ["production-key-1"]);
        assert_eq!(vals("A=localhost:5432/db\n"), ["localhost:5432/db"]);
        // not in KEY=VALUE shape: not understood, so not masked
        assert!(vals("//registry.example.org/:_authToken=npm-token-abcdef\n").is_empty());
        assert!(vals("{\"token\": \"json-secret-abcdef\"}\n").is_empty());
    }

    #[test]
    fn debug_shows_counts_and_lengths_only() {
        let v = secret_values(b"API_KEY=super-secret-value\nTOKEN=another-secret-1\nSHORT=1\n");
        let shown = format!("{v:?}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(!shown.contains("API_KEY") && !shown.contains("TOKEN") && !shown.contains("SHORT"));
        assert!(shown.contains("values: 2"), "{shown}");
        assert!(
            shown.contains("[18, 16]") || shown.contains("[16, 18]"),
            "{shown}"
        );
        assert!(shown.contains("skipped_keys: 1"), "{shown}");
    }

    #[test]
    fn repeated_values_are_listed_once_and_binary_input_is_fine() {
        assert_eq!(vals("A=samesame\nB=samesame\n"), ["samesame"]);
        let r = secret_values(&[
            b'A', b'=', 0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, b'\n',
        ]);
        assert_eq!(
            r.values,
            vec![vec![0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8]]
        );
        assert_eq!(secret_values(b"").values.len(), 0);
        assert_eq!(secret_values(b"KEY").values.len(), 0);
        assert_eq!(secret_values(b"KEY=").values.len(), 0);
        assert_eq!(secret_values(b"A= \n").values.len(), 0);
        // an empty quoted value, and a blank line inside a value
        assert_eq!(secret_values(b"A=\"\"\n").values.len(), 0);
        assert_eq!(
            vals("A=\"aaaaaaaa\n\nbbbbbbbb\"\n"),
            ["aaaaaaaa\n\nbbbbbbbb", "aaaaaaaa", "bbbbbbbb"]
        );
        assert_eq!(secret_values(b"=\n=x").values.len(), 0);
    }

    #[test]
    fn helpers() {
        assert!(is_key(b"A_b-1.c"));
        assert!(is_key(b"_x"));
        assert!(!is_key(b""));
        assert!(!is_key(b"1a"));
        assert!(!is_key(b"a b"));
        assert_eq!(trim(b"  a b \n"), b"a b");
        assert_eq!(trim(b"   "), b"");
        assert_eq!(trim(b""), b"");
        assert_eq!(unescape(b"a\\n\\"), b"a\n\\");
    }
}
