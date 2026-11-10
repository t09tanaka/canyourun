//! Replacing secret values in log bytes (spec 22.3 "ログ", chapter 14). Pure,
//! over bytes, so a log spool can feed it chunks as they arrive.
//!
//! Rules:
//! - Every occurrence of every pattern is replaced. Occurrences that overlap or
//!   touch are one run and become one [`MASK`], so no byte of any occurrence
//!   survives (`abcd` and `cdef` in `abcdef` leave nothing, not `ef`).
//! - A pattern is a byte string. Patterns that are valid UTF-8 start and end on
//!   character boundaries in valid UTF-8 text, so masked text stays valid
//!   UTF-8; bytes that are not part of a match are never changed.
//! - The empty pattern is dropped (it would match everywhere). Short values are
//!   dropped earlier, by [`super::envfile`], which knows the key names.
//! - A secret cut by a chunk boundary is still found: [`StreamMasker`] holds
//!   back the last `longest pattern - 1` bytes until more arrive, so any split
//!   of the input gives the same output as the input in one piece.
//!   [`StreamMasker::finish`] releases the rest. A secret written across a
//!   line break the program itself inserted, or transformed (encoded, split by
//!   the program), is not recognised; masking is a safety net, not a guarantee.

use std::collections::BTreeSet;

/// What a secret is replaced with.
pub const MASK: &[u8] = b"***";

#[derive(Clone, PartialEq, Eq)]
pub struct Masker {
    patterns: Vec<Vec<u8>>,
    longest: usize,
}

/// Does not show the patterns.
impl std::fmt::Debug for Masker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Masker")
            .field("patterns", &self.patterns.len())
            .finish_non_exhaustive()
    }
}

impl Masker {
    pub fn new(patterns: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let set: BTreeSet<Vec<u8>> = patterns.into_iter().filter(|p| !p.is_empty()).collect();
        let patterns: Vec<Vec<u8>> = set.into_iter().collect();
        let longest = patterns.iter().map(Vec::len).max().unwrap_or(0);
        Masker { patterns, longest }
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Merged `[start, end)` runs of every occurrence in `hay`, in order.
    fn runs(&self, hay: &[u8]) -> Vec<(usize, usize)> {
        let mut all: Vec<(usize, usize)> = Vec::new();
        for p in &self.patterns {
            if p.len() > hay.len() {
                continue;
            }
            for start in 0..=hay.len() - p.len() {
                if hay[start..].starts_with(p) {
                    all.push((start, start + p.len()));
                }
            }
        }
        all.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (s, e) in all {
            match merged.last_mut() {
                Some(last) if s <= last.1 => last.1 = last.1.max(e),
                _ => merged.push((s, e)),
            }
        }
        merged
    }

    /// `input` with every secret replaced.
    pub fn mask(&self, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(input.len());
        let mut at = 0;
        for (s, e) in self.runs(input) {
            out.extend_from_slice(&input[at..s]);
            out.extend_from_slice(MASK);
            at = e;
        }
        out.extend_from_slice(&input[at..]);
        out
    }

    pub fn stream(&self) -> StreamMasker {
        StreamMasker {
            masker: self.clone(),
            held: Vec::new(),
        }
    }
}

/// Masks a log arriving in pieces. Output may lag the input by up to
/// `longest pattern - 1` bytes (plus the rest of a character, so that a piece
/// of valid UTF-8 text never ends inside one) until [`finish`](Self::finish).
#[derive(Debug, Clone)]
pub struct StreamMasker {
    masker: Masker,
    held: Vec<u8>,
}

impl StreamMasker {
    /// Feed the next bytes; returns what can be released.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.held.extend_from_slice(chunk);
        if self.masker.is_empty() {
            return std::mem::take(&mut self.held);
        }
        let hold = self.masker.longest - 1;
        let mut cut = self.held.len().saturating_sub(hold);
        // do not end the released part inside a character
        while cut > 0 && cut < self.held.len() && (self.held[cut] & 0xC0) == 0x80 {
            cut -= 1;
        }
        // an occurrence (or run of them) that reaches the cut is not complete
        for (s, e) in self.masker.runs(&self.held) {
            if s < cut && e >= cut {
                cut = s;
            }
        }
        let out = self.masker.mask(&self.held[..cut]);
        self.held.drain(..cut);
        out
    }

    /// The end of the input: release everything that is held.
    pub fn finish(self) -> Vec<u8> {
        self.masker.mask(&self.held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &[&str]) -> Masker {
        Masker::new(p.iter().map(|s| s.as_bytes().to_vec()))
    }

    fn mask_str(m: &Masker, s: &str) -> String {
        String::from_utf8(m.mask(s.as_bytes())).unwrap()
    }

    #[test]
    fn replaces_every_occurrence_and_leaves_the_rest_alone() {
        let k = m(&["hunter2!"]);
        assert_eq!(mask_str(&k, "no secret here"), "no secret here");
        assert_eq!(mask_str(&k, "pw=hunter2! and hunter2!"), "pw=*** and ***");
        assert_eq!(mask_str(&k, ""), "");
        assert_eq!(mask_str(&k, "hunter2"), "hunter2");
        assert_eq!(mask_str(&k, "hunter2!hunter2!"), "***");
    }

    #[test]
    fn no_patterns_changes_nothing_and_empty_patterns_are_dropped() {
        let none = Masker::new([]);
        assert!(none.is_empty());
        assert_eq!(none.mask(b"abc"), b"abc");
        let empty = Masker::new([Vec::new()]);
        assert!(empty.is_empty());
        assert_eq!(empty.mask(b"abc"), b"abc");
        assert!(!m(&["abcd"]).is_empty());
    }

    #[test]
    fn overlapping_and_touching_patterns_leave_no_remainder() {
        let k = m(&["abcd", "cdef"]);
        assert_eq!(mask_str(&k, "xabcdefx"), "x***x");
        // nested
        let k = m(&["abcdef", "bcd"]);
        assert_eq!(mask_str(&k, "abcdef"), "***");
        // touching
        let k = m(&["abcd", "efgh"]);
        assert_eq!(mask_str(&k, "abcdefgh!"), "***!");
        // the same pattern overlapping itself
        let k = m(&["aaaa"]);
        assert_eq!(mask_str(&k, "aaaaaaa"), "***");
        // a longer pattern at the same start wins over a shorter one
        let k = m(&["abcd", "abcdef"]);
        assert_eq!(mask_str(&k, "abcdef"), "***");
    }

    #[test]
    fn multibyte_patterns_keep_the_text_valid_utf8() {
        let k = m(&["パスワード", "ス"]);
        let out = mask_str(&k, "これは パスワード です。ス!");
        assert_eq!(out, "これは *** です。***!");
        // patterns are bytes: a pattern that is not UTF-8 masks bytes
        let k = Masker::new([vec![0xff, 0xfe]]);
        assert_eq!(k.mask(&[1, 0xff, 0xfe, 2]), b"\x01***\x02");
    }

    #[test]
    fn a_pattern_longer_than_the_input_is_skipped() {
        assert_eq!(m(&["abcdefgh"]).mask(b"abc"), b"abc");
    }

    #[test]
    fn debug_does_not_show_the_patterns() {
        let s = format!("{:?}", m(&["topsecret-value"]));
        assert!(!s.contains("topsecret"));
        assert!(s.contains("Masker"));
        let st = format!("{:?}", m(&["topsecret-value"]).stream());
        assert!(!st.contains("topsecret"));
    }

    /// A tiny deterministic generator, so the property tests need no crate.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn run_stream(k: &Masker, input: &[u8], cuts: &[usize]) -> Vec<u8> {
        let mut s = k.stream();
        let mut out = Vec::new();
        let mut at = 0;
        for c in cuts {
            out.extend(s.push(&input[at..*c]));
            at = *c;
        }
        out.extend(s.push(&input[at..]));
        out.extend(s.finish());
        out
    }

    #[test]
    fn any_split_of_the_input_gives_the_one_shot_output() {
        let mut rng = Lcg(7);
        let alphabet = ["a", "b", "c", "ab", "bc", "é", "ス", " ", "\n"];
        for round in 0..400 {
            let n_pat = 1 + rng.below(3);
            let pats: Vec<Vec<u8>> = (0..n_pat)
                .map(|_| {
                    (0..1 + rng.below(4))
                        .map(|_| alphabet[rng.below(alphabet.len())])
                        .collect::<String>()
                        .into_bytes()
                })
                .collect();
            let k = Masker::new(pats.clone());
            let text: String = (0..rng.below(40))
                .map(|_| alphabet[rng.below(alphabet.len())])
                .collect();
            let input = text.as_bytes();
            let whole = k.mask(input);
            let mut cuts: Vec<usize> = (0..rng.below(6)).map(|_| rng.below(input.len() + 1)).collect();
            cuts.sort_unstable();
            let streamed = run_stream(&k, input, &cuts);
            assert_eq!(streamed, whole, "round {round}: patterns {pats:?} text {text:?} cuts {cuts:?}");
            // byte by byte as well
            let all: Vec<usize> = (0..=input.len()).collect();
            assert_eq!(run_stream(&k, input, &all), whole, "round {round} bytewise");
        }
    }

    #[test]
    fn masked_output_contains_no_pattern_and_valid_text_stays_valid() {
        let mut rng = Lcg(99);
        let alphabet = ["x", "y", "ス", "é", "-"];
        for _ in 0..300 {
            let p: String = (0..4 + rng.below(3))
                .map(|_| alphabet[rng.below(alphabet.len())])
                .collect();
            let k = Masker::new([p.clone().into_bytes()]);
            let text: String = (0..rng.below(50))
                .map(|_| alphabet[rng.below(alphabet.len())])
                .collect();
            let out = k.mask(text.as_bytes());
            let out = String::from_utf8(out).expect("valid utf-8 stays valid");
            assert!(!out.contains(&p), "pattern {p:?} left in {out:?} from {text:?}");
        }
    }

    #[test]
    fn a_secret_cut_by_a_chunk_boundary_is_found() {
        let k = m(&["secret-value"]);
        let mut s = k.stream();
        let mut out = s.push(b"token=secret-");
        // the part that might begin a secret is held back
        assert!(!String::from_utf8_lossy(&out).contains("secret"));
        out.extend(s.push(b"value done"));
        out.extend(s.finish());
        assert_eq!(String::from_utf8(out).unwrap(), "token=*** done");
    }

    #[test]
    fn a_stream_without_patterns_passes_everything_at_once() {
        let mut s = Masker::new([]).stream();
        assert_eq!(s.push(b"abc"), b"abc");
        assert_eq!(s.push(b""), b"");
        assert_eq!(s.finish(), b"");
    }

    #[test]
    fn released_text_never_ends_inside_a_character() {
        let k = m(&["0123456789"]);
        let text = "ああああああああああ";
        let mut s = k.stream();
        let mut released = Vec::new();
        for b in text.as_bytes() {
            let piece = s.push(&[*b]);
            released.extend(&piece);
            assert!(std::str::from_utf8(&released).is_ok(), "{released:?}");
        }
        released.extend(s.finish());
        assert_eq!(String::from_utf8(released).unwrap(), text);
    }
}
