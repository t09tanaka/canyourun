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
        self.stream_limited(MAX_HELD_BYTES)
    }

    /// [`stream`](Self::stream) with another bound on the held bytes.
    pub fn stream_limited(&self, limit: usize) -> StreamMasker {
        StreamMasker {
            masker: self.clone(),
            held: Vec::new(),
            limit,
        }
    }
}

/// Most bytes [`StreamMasker`] holds back beyond what one secret needs. A
/// run of overlapping occurrences (`abab` in `ababab...`) could otherwise keep
/// growing; past this the run so far is released as one mask.
pub const MAX_HELD_BYTES: usize = 1024 * 1024;

/// Masks a log arriving in pieces. Output may lag the input by up to
/// `longest pattern - 1` bytes (plus the rest of a character, so that a piece
/// of valid UTF-8 text never ends inside one) until [`finish`](Self::finish);
/// what is held never exceeds [`MAX_HELD_BYTES`] plus one chunk. When a
/// pathological input (a chain of overlapping occurrences longer than that)
/// forces an early release, the bytes of the chain are masked as one and at
/// most `longest pattern - 1` bytes of a following overlap can show.
#[derive(Clone)]
pub struct StreamMasker {
    masker: Masker,
    held: Vec<u8>,
    limit: usize,
}

/// Does not show the held bytes or the patterns: only counts.
impl std::fmt::Debug for StreamMasker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamMasker")
            .field("patterns", &self.masker.patterns.len())
            .field("held_bytes", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl StreamMasker {
    /// Bytes held back at the moment (for a bound check).
    pub fn pending(&self) -> usize {
        self.held.len()
    }

    /// Feed the next bytes; returns what can be released.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.held.extend_from_slice(chunk);
        if self.masker.is_empty() {
            return std::mem::take(&mut self.held);
        }
        let hold = self.masker.longest - 1;
        let cut = self.held.len().saturating_sub(hold);
        // do not end the released part inside a character
        let boundary = |at: usize| {
            (0..=at)
                .rev()
                .find(|&i| i >= self.held.len() || (self.held[i] & 0xC0) != 0x80)
                .unwrap_or(0)
        };
        let mut cut = boundary(cut);
        // an occurrence (or run of them) that reaches the cut is not complete
        let runs = self.masker.runs(&self.held);
        for &(s, e) in &runs {
            if e >= cut {
                cut = cut.min(s);
            }
        }
        if self.held.len() - cut > self.limit {
            // too long a chain: release it, keeping only what a later
            // occurrence could still start in
            let mut forced = boundary(self.held.len().saturating_sub(hold));
            for &(s, e) in &runs {
                if s < forced {
                    forced = forced.max(e);
                }
            }
            cut = forced;
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
            let mut cuts: Vec<usize> = (0..rng.below(6))
                .map(|_| rng.below(input.len() + 1))
                .collect();
            cuts.sort_unstable();
            let streamed = run_stream(&k, input, &cuts);
            assert_eq!(
                streamed, whole,
                "round {round}: patterns {pats:?} text {text:?} cuts {cuts:?}"
            );
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
            assert!(
                !out.contains(&p),
                "pattern {p:?} left in {out:?} from {text:?}"
            );
        }
    }

    #[test]
    fn a_stream_holds_back_exactly_the_longest_pattern_minus_one() {
        let k = m(&["abcde"]);
        let mut s = k.stream();
        // 10 bytes, hold 4: six are released
        assert_eq!(s.push(b"0123456789"), b"012345");
        assert_eq!(s.push(b""), b"");
        assert_eq!(s.push(b"ab"), b"67");
        assert_eq!(s.finish(), b"89ab");
        // shorter than the hold: nothing yet
        let mut s = k.stream();
        assert_eq!(s.push(b"abcd"), b"");
        assert_eq!(s.push(b"x"), b"a");
        // a one-byte pattern holds nothing back, but a match at the very end
        // waits in case another touches it
        let mut s = m(&["z"]).stream();
        assert_eq!(s.push(b"abz"), b"ab");
        assert_eq!(s.push(b"zc"), b"***c");
        assert_eq!(s.finish(), b"");
    }

    #[test]
    fn the_released_part_never_ends_inside_a_character() {
        let k = m(&["01234"]);
        let mut s = k.stream();
        // 12 bytes, hold 4: the cut at 8 is inside the third character
        assert_eq!(s.push("ああああ".as_bytes()), "ああ".as_bytes());
        assert_eq!(s.finish(), "ああ".as_bytes());
        // a continuation byte right at the start, nothing before it to release
        let mut s = m(&["abc"]).stream();
        assert_eq!(s.push(&[0x80, 0x81]), b"");
        assert_eq!(s.finish(), [0x80, 0x81]);
    }

    #[test]
    fn a_run_that_ends_exactly_at_the_cut_waits_for_the_one_that_touches_it() {
        let k = m(&["abcd", "efgh"]);
        let mut s = k.stream();
        let mut out = s.push(b"xxabcdefg");
        assert_eq!(out, b"xx");
        out.extend(s.push(b"h"));
        out.extend(s.finish());
        assert_eq!(out, b"xx***");
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

    #[test]
    fn a_chain_of_overlapping_occurrences_does_not_grow_the_held_bytes_without_bound() {
        let k = m(&["abab"]);
        let mut s = k.stream_limited(64);
        let mut out = Vec::new();
        let mut peak = 0;
        for _ in 0..2000 {
            out.extend(s.push(b"ab"));
            peak = peak.max(s.pending());
        }
        // at most the limit plus what one more chunk and one pattern need
        assert!(peak <= 64 + 2 + 3, "{peak}");
        out.extend(s.finish());
        // every byte of the chain is masked, in a few runs rather than one
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("***"));
        assert!(text.matches("***").count() > 1);
        // what shows is at most a pattern's worth of overlap per forced release
        let stars = text.matches("***").count();
        let rest = text.len() - stars * 3;
        assert!(rest <= stars * 3, "{rest} bytes show among {stars} masks");
        // the default bound is 1 MiB and a normal input never reaches it
        assert_eq!(MAX_HELD_BYTES, 1024 * 1024);
        let mut d = k.stream();
        for _ in 0..1000 {
            d.push(b"ab");
        }
        assert!(d.pending() > 64, "an unforced chain is still whole");
        assert_eq!(String::from_utf8(d.finish()).unwrap(), "***");
    }

    #[test]
    fn a_forced_release_keeps_utf8_and_leaves_a_following_secret_masked() {
        let k = m(&["ああああ", "SECRET-VALUE"]);
        let mut s = k.stream_limited(32);
        let mut out = Vec::new();
        for _ in 0..200 {
            out.extend(s.push("あ".as_bytes()));
            assert!(std::str::from_utf8(&out).is_ok());
        }
        out.extend(s.push(b" then SECRET-VALUE here"));
        out.extend(s.finish());
        let text = String::from_utf8(out).unwrap();
        assert!(!text.contains("SECRET"), "{text}");
        assert!(text.ends_with(" here"));
    }

    #[test]
    fn the_forced_release_starts_just_over_the_limit_and_takes_the_whole_run() {
        let k = m(&["abab"]);
        let mut s = k.stream_limited(64);
        for n in 1..=32 {
            s.push(b"ab");
            assert_eq!(s.pending(), 2 * n, "held up to and including the limit");
        }
        let out = s.push(b"ab");
        assert_eq!(s.pending(), 0, "the whole run went out, as one mask");
        assert_eq!(out, b"***");
        // a run that starts exactly where the kept tail starts is kept
        let k = m(&["aaaa", "xyz"]);
        let mut s = k.stream_limited(2);
        let out = s.push(b"aaaaaaaaaaaaqxyz");
        assert_eq!(s.pending(), 3);
        assert_eq!(out, b"***q");
        assert_eq!(s.finish(), b"***");
    }

    #[test]
    fn stream_debug_counts_only() {
        let k = m(&["topsecret-value"]);
        let mut s = k.stream();
        s.push(b"abc");
        let shown = format!("{s:?}");
        assert!(shown.contains("StreamMasker"), "{shown}");
        assert!(shown.contains("patterns: 1"), "{shown}");
        assert!(shown.contains("held_bytes: 3"), "{shown}");
    }

    #[test]
    fn what_is_held_is_measured_from_the_cut_not_from_the_start() {
        // a long prefix does not make the held tail count as too long
        let k = m(&["secret-value"]);
        let mut s = k.stream_limited(16);
        let out = s.push(b"aaaaaaaaaaaaaaaaaaaa secret-value");
        assert_eq!(out, b"aaaaaaaaaaaaaaaaaaaa ");
        assert_eq!(s.pending(), 12);
        assert_eq!(s.finish(), b"***");
    }
}
