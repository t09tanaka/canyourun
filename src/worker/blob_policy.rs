//! The decisions of the blob operations, free of I/O (spec chapter 11).
//!
//! What a `blob_put` may announce, which bytes of a chunk are new, what a
//! staged file is called, which manifests and files belong to a source, how a
//! `blob_verify` entry is answered, and which part of an artifact a
//! `blob_get` returns. Everything here judges what another host said; the
//! bytes themselves are handled in [`super::blobs`].

use crate::wire::SourceBlobKind;

/// Bytes of one `blob_put` chunk, at most (1 MiB; a frame holds 2 MiB).
pub const MAX_PUT_CHUNK: usize = 1_048_576;
/// Lock files the sources share (a source always maps to the same one).
pub const LOCK_SHARDS: u64 = 64;

/// What the worker accepts from a coordinator, per blob and per source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobLimits {
    /// One file of a source.
    pub max_blob_bytes: u64,
    /// One manifest (its JSON).
    pub max_manifest_bytes: u64,
    /// The Git bundle of a source.
    pub max_bundle_bytes: u64,
    /// The files of one source, each once.
    pub max_source_bytes: u64,
    /// Manifests of one source (its own and its submodule captures).
    pub max_manifests: u32,
    /// Blobs of one source that are part-way at once.
    pub max_partials: u32,
    /// Sources that are part-way at once.
    pub max_staged_sources: u32,
    /// How long a request waits for another one on the same source before it
    /// answers `OVERLOADED`.
    pub lock_wait_ms: u64,
    /// A source nobody added to for this long may be dropped to make room.
    pub stale_ms: u64,
}

impl Default for BlobLimits {
    fn default() -> Self {
        BlobLimits {
            max_blob_bytes: 4_294_967_296,
            max_manifest_bytes: 67_108_864,
            max_bundle_bytes: 4_294_967_296,
            max_source_bytes: 34_359_738_368,
            max_manifests: 256,
            max_partials: 64,
            max_staged_sources: 16,
            lock_wait_ms: 30_000,
            stale_ms: 86_400_000,
        }
    }
}

/// The largest size a blob of `kind` may announce.
pub fn cap_of(limits: &BlobLimits, kind: SourceBlobKind) -> u64 {
    match kind {
        SourceBlobKind::Content => limits.max_blob_bytes,
        SourceBlobKind::Manifest => limits.max_manifest_bytes,
        SourceBlobKind::Bundle => limits.max_bundle_bytes,
    }
}

/// Why a `blob_put` is refused before anything is opened or written. `len`
/// is the length of the chunk that arrived, not a number the sender gave.
pub fn check_put(
    limits: &BlobLimits,
    kind: SourceBlobKind,
    total_size: u64,
    offset: u64,
    len: usize,
) -> Result<(), &'static str> {
    if len > MAX_PUT_CHUNK {
        return Err("the chunk is larger than 1 MiB");
    }
    if total_size > cap_of(limits, kind) {
        return Err("the blob is larger than this worker accepts");
    }
    match offset.checked_add(len as u64) {
        Some(end) if end <= total_size => Ok(()),
        _ => Err("the chunk runs past the announced size"),
    }
}

/// What to do with a chunk, given the `held` bytes of the staged file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStep {
    /// The chunk starts after the held bytes: nothing is written, the answer
    /// says where to resume.
    Behind,
    /// Append the chunk from byte `skip` on (the bytes before are held
    /// already). `complete`: the file then has every byte, and its digest is
    /// to be checked.
    Append { skip: usize, complete: bool },
    /// The staged file is longer than the blob can be: it is not this blob's.
    Overlong,
}

/// The rules of [`crate::protocol::chunk::ChunkReceiver`], on a file: a
/// repeated or overlapping chunk is harmless, a gap is not filled. The
/// caller has checked the chunk with [`check_put`].
pub fn plan_chunk(held: u64, total_size: u64, offset: u64, len: usize) -> ChunkStep {
    if held > total_size {
        return ChunkStep::Overlong;
    }
    if offset > held {
        return ChunkStep::Behind;
    }
    let end = offset + len as u64;
    let skip = (held - offset).min(len as u64) as usize;
    ChunkStep::Append {
        skip,
        complete: held.max(end) == total_size,
    }
}

fn kind_word(kind: SourceBlobKind) -> &'static str {
    match kind {
        SourceBlobKind::Content => "content",
        SourceBlobKind::Manifest => "manifest",
        SourceBlobKind::Bundle => "bundle",
    }
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// The name of a staged file: `<kind>-<hex>.part` while bytes are missing,
/// `.done` once the digest was checked. `hex` is the digest without its
/// prefix; nothing else of a request ever becomes part of a path.
pub fn staged_name(kind: SourceBlobKind, hex: &str, done: bool) -> String {
    let ext = if done { "done" } else { "part" };
    format!("{}-{hex}.{ext}", kind_word(kind))
}

/// The inverse of [`staged_name`]: kind, digest hex and whether it is done.
/// `None` for a name this module would not have made.
pub fn parse_staged_name(name: &str) -> Option<(SourceBlobKind, &str, bool)> {
    let (stem, ext) = name.rsplit_once('.')?;
    let done = match ext {
        "done" => true,
        "part" => false,
        _ => return None,
    };
    let (word, hex) = stem.split_once('-')?;
    let kind = [
        SourceBlobKind::Content,
        SourceBlobKind::Manifest,
        SourceBlobKind::Bundle,
    ]
    .into_iter()
    .find(|k| kind_word(*k) == word)?;
    is_hex64(hex).then_some((kind, hex, done))
}

/// Which lock file a source uses: FNV-1a of its id, over [`LOCK_SHARDS`].
pub fn lock_shard(source_id: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in source_id.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h % LOCK_SHARDS
}

/// May a new staged file be started when `partials` are part-way already?
pub fn may_start_partial(limits: &BlobLimits, partials: usize) -> bool {
    (partials as u64) < u64::from(limits.max_partials)
}

/// May another source be staged when `staged` are part-way already?
pub fn may_stage_source(limits: &BlobLimits, staged: usize) -> bool {
    (staged as u64) < u64::from(limits.max_staged_sources)
}

/// A staged source whose newest file is from `newest_ms` may be dropped at
/// `now_ms`. A clock that went backwards drops nothing.
pub fn is_stale(limits: &BlobLimits, now_ms: u64, newest_ms: u64) -> bool {
    now_ms
        .checked_sub(newest_ms)
        .is_some_and(|age| age > limits.stale_ms)
}

/// What a manifest that arrived whole is to the source it was sent for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestRole {
    /// The source's own manifest.
    Root,
    /// The capture of a submodule a staged manifest names.
    Submodule,
}

/// A manifest is taken only as the source's own, or, once that is staged, as
/// one a staged manifest names; and only while the source has room for one.
/// `is_root`: its id is the id of the source the request names.
pub fn manifest_role(
    limits: &BlobLimits,
    is_root: bool,
    root_staged: bool,
    named: bool,
    staged: usize,
) -> Result<ManifestRole, &'static str> {
    if staged as u64 >= u64::from(limits.max_manifests) {
        return Err("the source has too many manifests");
    }
    if is_root {
        return Ok(ManifestRole::Root);
    }
    if !root_staged {
        return Err("the first manifest of a source must be its own");
    }
    if !named {
        return Err("no manifest of the source names this manifest");
    }
    Ok(ManifestRole::Submodule)
}

/// The files a manifest adds to a source: none larger than a blob may be,
/// and the source's files (each once; `already` bytes are staged) within the
/// source's cap. `sizes` are the sizes of the files that are new to the
/// source. Returns the new total.
pub fn check_manifest_files(
    limits: &BlobLimits,
    already: u64,
    sizes: impl IntoIterator<Item = u64>,
) -> Result<u64, &'static str> {
    let mut sum = already;
    for size in sizes {
        if size > limits.max_blob_bytes {
            return Err("the manifest names a file larger than this worker accepts");
        }
        sum = match sum.checked_add(size) {
            Some(s) if s <= limits.max_source_bytes => s,
            _ => return Err("the source is larger than this worker accepts"),
        };
    }
    Ok(sum)
}

/// A file is taken only when a staged manifest names it with this size.
/// `named_size`: the size the manifests give for the digest, if any does.
pub fn check_content(named_size: Option<u64>, total_size: u64) -> Result<(), &'static str> {
    match named_size {
        None => Err("no manifest of the source names this blob"),
        Some(s) if s != total_size => Err("the manifest gives this blob another size"),
        Some(_) => Ok(()),
    }
}

/// The answer to one `blob_verify` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Present,
    /// Not held whole; `received` bytes are staged.
    Missing {
        received: u64,
    },
    Corrupt,
}

/// A file of the source: `stored_size` is the size of the blob in the store
/// when it is there, `partial` the length of its staged file when there is
/// one. A staged file never counts for more than the blob's size.
pub fn content_verdict(stored_size: Option<u64>, partial: Option<u64>, total_size: u64) -> Verdict {
    match stored_size {
        Some(s) if s == total_size => Verdict::Present,
        Some(_) => Verdict::Corrupt,
        None => missing(partial, total_size),
    }
}

/// A manifest or the bundle: held when its staged file is done, or when
/// nothing more is needed (`settled`: the source is published, or, for the
/// bundle, the commits it would bring are there).
pub fn staged_verdict(done: bool, settled: bool, partial: Option<u64>, total_size: u64) -> Verdict {
    if done || settled {
        Verdict::Present
    } else {
        missing(partial, total_size)
    }
}

fn missing(partial: Option<u64>, total_size: u64) -> Verdict {
    match partial {
        Some(p) if p <= total_size => Verdict::Missing { received: p },
        _ => Verdict::Missing { received: 0 },
    }
}

/// The part of an artifact of `size` bytes a `blob_get` returns: how many
/// bytes from `offset`, and whether they reach the end. `chunk` is the
/// largest answer (already clamped, never zero).
pub fn get_range(size: u64, offset: u64, chunk: usize) -> Result<(usize, bool), &'static str> {
    let Some(left) = size.checked_sub(offset) else {
        return Err("the offset is past the end of the artifact");
    };
    let len = left.min(chunk as u64);
    Ok((len as usize, len == left))
}

#[cfg(test)]
mod tests {
    use super::*;
    use SourceBlobKind::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn limits() -> BlobLimits {
        BlobLimits {
            max_blob_bytes: 100,
            max_manifest_bytes: 50,
            max_bundle_bytes: 70,
            max_source_bytes: 250,
            max_manifests: 3,
            max_partials: 2,
            max_staged_sources: 2,
            lock_wait_ms: 10,
            stale_ms: 1000,
        }
    }

    #[test]
    fn the_defaults_are_these_numbers() {
        let d = BlobLimits::default();
        assert_eq!(d.max_blob_bytes, 4 << 30);
        assert_eq!(d.max_manifest_bytes, 64 << 20);
        assert_eq!(d.max_bundle_bytes, 4 << 30);
        assert_eq!(d.max_source_bytes, 32 << 30);
        assert_eq!(
            (d.max_manifests, d.max_partials, d.max_staged_sources),
            (256, 64, 16)
        );
        assert_eq!((d.lock_wait_ms, d.stale_ms), (30_000, 24 * 3600 * 1000));
        assert_eq!(MAX_PUT_CHUNK, 1 << 20);
        assert_eq!(LOCK_SHARDS, 64);
    }

    #[test]
    fn each_kind_has_its_own_cap() {
        let l = limits();
        assert_eq!(cap_of(&l, Content), 100);
        assert_eq!(cap_of(&l, Manifest), 50);
        assert_eq!(cap_of(&l, Bundle), 70);
    }

    #[test]
    fn a_put_is_checked_on_what_arrived() {
        let l = limits();
        assert_eq!(check_put(&l, Content, 100, 0, 100), Ok(()));
        assert_eq!(check_put(&l, Content, 100, 90, 10), Ok(()));
        assert_eq!(check_put(&l, Content, 0, 0, 0), Ok(()));
        assert_eq!(check_put(&l, Content, 100, 100, 0), Ok(()));
        assert_eq!(
            check_put(&l, Content, 101, 0, 1),
            Err("the blob is larger than this worker accepts")
        );
        assert_eq!(check_put(&l, Manifest, 50, 0, 1), Ok(()));
        assert!(check_put(&l, Manifest, 51, 0, 1).is_err());
        assert!(check_put(&l, Bundle, 71, 0, 1).is_err());
        assert_eq!(
            check_put(&l, Content, 100, 91, 10),
            Err("the chunk runs past the announced size")
        );
        assert!(check_put(&l, Content, 100, 101, 0).is_err());
        assert!(check_put(&l, Content, 100, u64::MAX, 1).is_err());
        let big = BlobLimits {
            max_blob_bytes: u64::MAX,
            ..limits()
        };
        assert_eq!(check_put(&big, Content, 1 << 30, 0, MAX_PUT_CHUNK), Ok(()));
        assert_eq!(
            check_put(&big, Content, 1 << 30, 0, MAX_PUT_CHUNK + 1),
            Err("the chunk is larger than 1 MiB")
        );
    }

    #[test]
    fn a_chunk_is_appended_skipped_or_sent_back() {
        use ChunkStep::*;
        let step = |skip, complete| Append { skip, complete };
        // fresh
        assert_eq!(plan_chunk(0, 10, 0, 4), step(0, false));
        assert_eq!(plan_chunk(4, 10, 4, 6), step(0, true));
        assert_eq!(plan_chunk(0, 10, 0, 10), step(0, true));
        // overlap and repeat
        assert_eq!(plan_chunk(6, 10, 4, 4), step(2, false));
        assert_eq!(plan_chunk(6, 10, 4, 6), step(2, true));
        assert_eq!(plan_chunk(8, 10, 0, 4), step(4, false));
        assert_eq!(plan_chunk(8, 10, 4, 4), step(4, false));
        // everything is held: the digest is checked again
        assert_eq!(plan_chunk(10, 10, 0, 4), step(4, true));
        assert_eq!(plan_chunk(10, 10, 10, 0), step(0, true));
        // the empty blob
        assert_eq!(plan_chunk(0, 0, 0, 0), step(0, true));
        // a probe
        assert_eq!(plan_chunk(3, 10, 3, 0), step(0, false));
        // a gap
        assert_eq!(plan_chunk(3, 10, 4, 2), Behind);
        assert_eq!(plan_chunk(0, 10, 1, 0), Behind);
        // not this blob's file
        assert_eq!(plan_chunk(11, 10, 0, 4), Overlong);
        assert_eq!(plan_chunk(11, 10, 12, 0), Overlong);
    }

    #[test]
    fn staged_names_round_trip_and_nothing_else_parses() {
        assert_eq!(
            staged_name(Content, HEX, false),
            format!("content-{HEX}.part")
        );
        assert_eq!(
            staged_name(Manifest, HEX, true),
            format!("manifest-{HEX}.done")
        );
        assert_eq!(staged_name(Bundle, HEX, true), format!("bundle-{HEX}.done"));
        for kind in [Content, Manifest, Bundle] {
            for done in [false, true] {
                let name = staged_name(kind, HEX, done);
                assert_eq!(parse_staged_name(&name), Some((kind, HEX, done)));
            }
        }
        let upper = HEX.to_uppercase();
        for bad in [
            String::new(),
            "lock".to_string(),
            format!("content-{HEX}"),
            format!("content-{HEX}.tmp"),
            format!("other-{HEX}.part"),
            format!("content{HEX}.part"),
            format!("content-{}.part", &HEX[1..]),
            format!("content-{HEX}0.part"),
            format!("content-{upper}.part"),
            format!("content-{}g.part", &HEX[1..]),
            format!("content-{}/.part", &HEX[1..]),
        ] {
            assert_eq!(parse_staged_name(&bad), None, "{bad}");
        }
    }

    #[test]
    fn a_source_always_locks_the_same_shard() {
        assert_eq!(lock_shard(""), 0xcbf2_9ce4_8422_2325 % 64);
        assert_eq!(lock_shard("a"), 0xaf63_dc4c_8601_ec8c % 64);
        assert_eq!(lock_shard("foobar"), 0x8594_4171_f739_67e8 % 64);
        let shards: std::collections::BTreeSet<u64> =
            (0..500).map(|i| lock_shard(&format!("src_{i}"))).collect();
        assert!(shards.iter().all(|s| *s < LOCK_SHARDS));
        assert!(shards.len() > 32);
    }

    #[test]
    fn room_is_counted_before_a_file_or_a_source_is_started() {
        let l = limits();
        assert!(may_start_partial(&l, 0));
        assert!(may_start_partial(&l, 1));
        assert!(!may_start_partial(&l, 2));
        assert!(!may_start_partial(&l, 3));
        assert!(may_stage_source(&l, 1));
        assert!(!may_stage_source(&l, 2));
        assert!(!may_stage_source(&l, usize::MAX));
    }

    #[test]
    fn only_an_old_source_is_stale() {
        let l = limits();
        assert!(!is_stale(&l, 5000, 4000));
        assert!(is_stale(&l, 5001, 4000));
        assert!(!is_stale(&l, 4000, 5001));
        assert!(!is_stale(&l, 0, u64::MAX));
    }

    #[test]
    fn a_manifest_is_the_sources_own_or_one_it_names() {
        let l = limits();
        use ManifestRole::*;
        assert_eq!(manifest_role(&l, true, false, false, 0), Ok(Root));
        assert_eq!(manifest_role(&l, true, true, false, 2), Ok(Root));
        assert_eq!(manifest_role(&l, false, true, true, 1), Ok(Submodule));
        assert_eq!(
            manifest_role(&l, false, false, true, 0),
            Err("the first manifest of a source must be its own")
        );
        assert_eq!(
            manifest_role(&l, false, true, false, 1),
            Err("no manifest of the source names this manifest")
        );
        assert_eq!(
            manifest_role(&l, true, true, true, 3),
            Err("the source has too many manifests")
        );
        assert!(manifest_role(&l, false, true, true, 4).is_err());
    }

    #[test]
    fn the_files_of_a_manifest_fit_the_blob_and_the_source_caps() {
        let l = limits();
        assert_eq!(check_manifest_files(&l, 0, []), Ok(0));
        assert_eq!(check_manifest_files(&l, 50, [100, 100]), Ok(250));
        assert_eq!(
            check_manifest_files(&l, 51, [100, 100]),
            Err("the source is larger than this worker accepts")
        );
        assert_eq!(
            check_manifest_files(&l, 0, [1, 101]),
            Err("the manifest names a file larger than this worker accepts")
        );
        let wide = BlobLimits {
            max_blob_bytes: u64::MAX,
            max_source_bytes: u64::MAX,
            ..limits()
        };
        assert_eq!(check_manifest_files(&wide, 1, [u64::MAX - 1]), Ok(u64::MAX));
        assert!(check_manifest_files(&wide, 2, [u64::MAX - 1]).is_err());
    }

    #[test]
    fn a_file_is_taken_only_as_the_manifest_names_it() {
        assert_eq!(check_content(Some(7), 7), Ok(()));
        assert_eq!(
            check_content(Some(7), 8),
            Err("the manifest gives this blob another size")
        );
        assert_eq!(
            check_content(None, 7),
            Err("no manifest of the source names this blob")
        );
    }

    #[test]
    fn verify_answers_from_the_store_and_the_staged_files() {
        use Verdict::*;
        assert_eq!(content_verdict(Some(9), None, 9), Present);
        assert_eq!(content_verdict(Some(9), Some(3), 9), Present);
        assert_eq!(content_verdict(Some(8), None, 9), Corrupt);
        assert_eq!(content_verdict(None, None, 9), Missing { received: 0 });
        assert_eq!(content_verdict(None, Some(3), 9), Missing { received: 3 });
        assert_eq!(content_verdict(None, Some(9), 9), Missing { received: 9 });
        assert_eq!(content_verdict(None, Some(10), 9), Missing { received: 0 });

        assert_eq!(staged_verdict(true, false, None, 9), Present);
        assert_eq!(staged_verdict(false, true, Some(2), 9), Present);
        assert_eq!(
            staged_verdict(false, false, Some(2), 9),
            Missing { received: 2 }
        );
        assert_eq!(
            staged_verdict(false, false, None, 9),
            Missing { received: 0 }
        );
    }

    #[test]
    fn a_get_returns_a_part_within_the_artifact() {
        assert_eq!(get_range(10, 0, 4), Ok((4, false)));
        assert_eq!(get_range(10, 6, 4), Ok((4, true)));
        assert_eq!(get_range(10, 8, 4), Ok((2, true)));
        assert_eq!(get_range(10, 10, 4), Ok((0, true)));
        assert_eq!(get_range(0, 0, 4), Ok((0, true)));
        assert_eq!(
            get_range(10, 11, 4),
            Err("the offset is past the end of the artifact")
        );
        assert_eq!(get_range(u64::MAX, 0, 4), Ok((4, false)));
    }
}
