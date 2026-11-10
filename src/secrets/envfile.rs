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
//! carries no secrecy that masking could keep. The names of such keys are
//! returned (never the values) so the caller can say so.

use std::collections::BTreeSet;

/// Shortest value (in bytes) that is masked.
pub const MIN_MASK_BYTES: usize = 4;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Values {
    /// The strings to mask, each at least [`MIN_MASK_BYTES`] long, no repeats.
    pub values: Vec<Vec<u8>>,
    /// Keys whose value (or part of it) was too short to mask.
    pub skipped_keys: Vec<String>,
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
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' && i + 1 < s.len() {
            let c = match s[i + 1] {
                b'n' => Some(b'\n'),
                b'r' => Some(b'\r'),
                b't' => Some(b'\t'),
                b'"' => Some(b'"'),
                b'\\' => Some(b'\\'),
                _ => None,
            };
            if let Some(c) = c {
                out.push(c);
                i += 2;
                continue;
            }
        }
        out.push(s[i]);
        i += 1;
    }
    out
}

/// Index of the quote that closes a value opened with `quote`, in `rest` (the
/// text after the opening quote). A backslash escapes the next byte in double
/// quotes only.
fn closing_quote(rest: &[u8], quote: u8) -> Option<usize> {
    let mut i = 0;
    while i < rest.len() {
        if quote == b'"' && rest[i] == b'\\' {
            i += 2;
            continue;
        }
        if rest[i] == quote {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn unquoted(v: &[u8]) -> &[u8] {
    let mut end = v.len();
    for i in 0..v.len() {
        if v[i] == b'#' && i > 0 && v[i - 1].is_ascii_whitespace() {
            end = i;
            break;
        }
    }
    trim(&v[..end])
}

/// All values of `content` that must be masked.
pub fn secret_values(content: &[u8]) -> Values {
    let mut found: Vec<(String, Vec<u8>)> = Vec::new();
    let mut pos = 0;
    while pos < content.len() {
        let line_end = content[pos..]
            .iter()
            .position(|c| *c == b'\n')
            .map_or(content.len(), |n| pos + n);
        let line = trim(&content[pos..line_end]);
        let mut next = line_end + 1;
        let line = line
            .strip_prefix(b"export")
            .filter(|r| r.first().is_some_and(|c| c.is_ascii_whitespace()))
            .map_or(line, trim);
        if line.is_empty() || line[0] == b'#' {
            pos = next;
            continue;
        }
        let Some(eq) = line.iter().position(|c| *c == b'=') else {
            pos = next;
            continue;
        };
        let key = trim(&line[..eq]);
        if !is_key(key) {
            pos = next;
            continue;
        }
        let key = String::from_utf8_lossy(key).into_owned();
        let value = trim(&line[eq + 1..]);
        match value.first() {
            Some(&q) if q == b'"' || q == b'\'' => {
                // the value may run over lines: look in the rest of the content
                let value_start = value.as_ptr() as usize - content.as_ptr() as usize;
                let rest = &content[value_start + 1..];
                let (inner, consumed) = match closing_quote(rest, q) {
                    Some(n) => (&rest[..n], value_start + 1 + n + 1),
                    None => (trim(rest), content.len()),
                };
                found.push((key.clone(), inner.to_vec()));
                if q == b'"' {
                    let un = unescape(inner);
                    if un != inner {
                        found.push((key, un));
                    }
                }
                // what follows the closing quote on its line is not a pair
                if consumed > next {
                    next = content[consumed.min(content.len())..]
                        .iter()
                        .position(|c| *c == b'\n')
                        .map_or(content.len(), |n| consumed + n + 1);
                }
            }
            Some(_) => found.push((key, unquoted(value).to_vec())),
            None => {}
        }
        pos = next;
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
            if piece.len() < MIN_MASK_BYTES {
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
        assert_eq!(vals("A=abc#def # tail\n"), ["abc#def"]);
        // masking too much is the safe side: a leading `#` stays in the value
        assert_eq!(vals("A=# alone\n"), ["# alone"]);
        assert_eq!(vals("A=value1234 \t#x"), ["value1234"]);
    }

    #[test]
    fn quoted_values_lose_their_quotes_and_may_span_lines() {
        assert_eq!(vals("A='single quoted'\n"), ["single quoted"]);
        assert_eq!(vals("A=\"double \\\"q\\\" x\"\n"), ["double \\\"q\\\" x", "double \"q\" x"]);
        assert_eq!(vals("  A =  \"abcd efgh\"  \nB=next-value\n"), ["abcd efgh", "next-value"]);
        let multi = vals("K=\"-----BEGIN KEY-----\nMIIBabcdef\n-----END KEY-----\"\nAFTER=after-1234\n");
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
        assert_eq!(vals("A=\"open value\nmore text here\n"), ["open value\nmore text here", "open value", "more text here"]);
    }

    #[test]
    fn escapes_in_double_quotes_are_also_masked_in_their_decoded_form() {
        let v = vals("A=\"line1-aaa\\nline2-bbb\\t\\r\\\\x\\q\"\n");
        assert_eq!(v[0], "line1-aaa\\nline2-bbb\\t\\r\\\\x\\q");
        assert_eq!(v[1], "line1-aaa\nline2-bbb\t\r\\x\\q");
        assert_eq!(v[2], "line1-aaa");
        assert_eq!(v[3], "line2-bbb\t\r\\x\\q");
        // a trailing backslash is kept
        assert_eq!(vals("A=\"abcde\\\\\"\n"), ["abcde\\\\", "abcde\\"]);
        // single quotes do not unescape
        assert_eq!(vals("A='ab\\ncd'\n"), ["ab\\ncd"]);
    }

    #[test]
    fn short_values_are_skipped_by_key_and_never_returned() {
        let r = secret_values(b"A=1\nB=abc\nC=abcd\nD=\"x\ny\"\nA=on\nE=\n");
        assert_eq!(r.values, vec![b"abcd".to_vec()]);
        assert_eq!(r.skipped_keys, ["A", "B", "D"]);
        // exactly MIN_MASK_BYTES is masked
        assert_eq!(secret_values(b"A=abcd").values.len(), 1);
        assert_eq!(secret_values(b"A=abc").values.len(), 0);
    }

    #[test]
    fn repeated_values_are_listed_once_and_binary_input_is_fine() {
        assert_eq!(vals("A=samesame\nB=samesame\n"), ["samesame"]);
        let r = secret_values(&[b'A', b'=', 0xff, 0xfe, 0xfd, 0xfc, b'\n']);
        assert_eq!(r.values, vec![vec![0xff, 0xfe, 0xfd, 0xfc]]);
        assert_eq!(secret_values(b"").values.len(), 0);
        assert_eq!(secret_values(b"KEY").values.len(), 0);
        assert_eq!(secret_values(b"KEY=").values.len(), 0);
        assert_eq!(secret_values(b"A= \n").values.len(), 0);
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
