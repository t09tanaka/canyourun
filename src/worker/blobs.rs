//! The blob operations of the worker (spec chapter 11): receiving a source
//! from the coordinator, and serving an attempt's artifacts back.
//!
//! A source arrives as blobs of three kinds: its manifests (its own first,
//! then the captures of the submodules it names), its files (named by the
//! hash of their content) and, for a Git source, one bundle. A manifest is the
//! authority: only what a staged manifest names is taken, with the size it
//! gives. Everything waits in `<state_dir>/incoming/<source_id>/` until the
//! source is whole:
//!
//! ```text
//! manifest-<hex>-<size>.part|.done   bundle-<hex>-<size>.part|.done
//! parts/content-<hex>-<size>.part    files/content-<hex>-<size>.done
//! reserved                           (the bytes the source may take in all)
//! ```
//!
//! The only state of a transfer is those files (the length of a `.part` is
//! what was received), so a transfer that was cut resumes on another
//! connection or after a restart of the agent. The digest is checked when the
//! last byte is there. Once a source has everything, the bundle is unpacked
//! into the repository's clone, the files are moved into the store and the
//! manifests are published (leaf first, the source's own last); only then
//! can an attempt be prepared from it. A source that is dropped before that
//! leaves nothing in the store.
//!
//! The worker never asks origin for anything: the clone a bundle is unpacked
//! into has no remote and is made when the first bundle arrives. A
//! `blob_verify` tells the coordinator which commits the clone holds, so the
//! next bundle carries only what is missing.
//!
//! Requests for one source are serialized by a file lock, whichever
//! connection or process they come from; what the sources take together
//! (how many, how many bytes) is decided under one more lock for all of
//! them. The decisions are in [`super::blob_policy`].

use super::blob_policy::{
    bundle_prerequisites, check_content, check_manifest_files, check_put, check_reservation,
    content_verdict, displaces, get_range, is_stale, lock_shard, manifest_role, may_stage_source,
    may_start_partial, order_have, parse_staged_name, plan_chunk, source_total, staged_name,
    staged_verdict, BlobLimits, ChunkStep, Refusal, StagedName, Verdict, MAX_BUNDLE_HEADER,
    MAX_HAVE, MAX_SNAPSHOT_REFS,
};
use super::host::Host;
use super::policy::MAX_VERIFY_DIGESTS;
use crate::error::{ApiError, ConflictReason, ErrorCode};
use crate::ids::{Digest, SourceId};
use crate::platform::{free_bytes, open_beneath, wall_ms};
use crate::protocol::chunk::clamp_chunk;
use crate::source::classify::{ci_id_of, source_id_of_ci};
use crate::source::diff::{is_blob_hash, BlobWant};
use crate::source::store::{verify_manifest, ObjectsRepo};
use crate::source::SourceStore;
use crate::storage::WorkerStore;
use crate::supervisor::spool_plan::relative_path;
use crate::wire::{
    ArtifactRecord, BlobGetRequest, BlobGetResponse, BlobProgress, BlobPutRequest, BlobPutResponse,
    BlobVerifyRequest, BlobVerifyResponse, Bytes, RepositoryHave, SchemaVersion, SourceBlobKind,
};
use crate::workspace::plan::clone_dir_name;
use crate::workspace::Workspaces;
use local_ci::source::SourceManifest as CiManifest;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, TryLockError};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// Where a host takes a source in. The agent asks its [`Host`] for one.
pub trait BlobSink: Send + Sync {
    fn put(&self, limits: &BlobLimits, r: &BlobPutRequest) -> Result<BlobPutResponse, ApiError>;
    fn verify(
        &self,
        limits: &BlobLimits,
        r: &BlobVerifyRequest,
    ) -> Result<BlobVerifyResponse, ApiError>;
}

/// The file in a source's staging directory that holds its reservation.
const RESERVED: &str = "reserved";
const RESERVED_TMP: &str = "reserved.tmp";
/// Where the files of a source wait: part-way, and whole.
const PARTS: &str = "parts";
const FILES: &str = "files";

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

fn overloaded(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::Overloaded, m)
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn io_err(e: io::Error) -> ApiError {
    internal("incoming blobs", e)
}

fn private_dir(dir: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

fn hex_of(d: &Digest) -> &str {
    d.as_str()
        .strip_prefix(Digest::PREFIX)
        .expect("a digest has its prefix")
}

fn digest_of_file(path: &Path) -> io::Result<Digest> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Ok(Digest::parse(&format!("{}{hex}", Digest::PREFIX)).expect("a sha256 renders as a digest"))
}

/// The length of a staged file; `None` when there is none. Anything but a
/// regular file under that name is not ours and is an error.
fn staged_len(path: &Path) -> Result<Option<u64>, ApiError> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_file() => Ok(Some(m.len())),
        Ok(_) => Err(internal(
            "incoming blobs",
            format!("{} is not a regular file", path.display()),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err(e)),
    }
}

/// Our own staged file, gone. A missing one is fine.
fn remove_staged(path: &Path) {
    let _ = fs::remove_file(path);
}

/// A staged file of a source: its name, and what the name tells.
#[derive(Debug, Clone)]
struct StagedFile {
    name: String,
    kind: SourceBlobKind,
    hex: String,
    size: u64,
    done: bool,
}

impl StagedFile {
    fn as_name(&self) -> StagedName<'_> {
        StagedName {
            kind: self.kind,
            hex: &self.hex,
            size: self.size,
            done: self.done,
        }
    }
}

/// The names in a directory that are staged files of ours.
fn listing(dir: &Path) -> Result<Vec<StagedFile>, ApiError> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(io_err(e)),
    };
    let mut out = vec![];
    for entry in entries {
        let name = entry.map_err(io_err)?.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(n) = parse_staged_name(name) {
            out.push(StagedFile {
                name: name.to_string(),
                kind: n.kind,
                hex: n.hex.to_string(),
                size: n.size,
                done: n.done,
            });
        }
    }
    Ok(out)
}

/// What the manifests and the bundle of a source take: the sizes their
/// staged files (whole or part-way) were announced with.
fn announced(files: &[StagedFile]) -> (u64, u64) {
    let sum = |kind| {
        files
            .iter()
            .filter(|f| f.kind == kind)
            .fold(0u64, |a, f| a.saturating_add(f.size))
    };
    (sum(SourceBlobKind::Manifest), sum(SourceBlobKind::Bundle))
}

/// What is kept of a staged manifest between requests: not its body (that
/// is read again from its file when the source is published).
struct ManifestInfo {
    /// The digest (hex) and size its staged file is named by.
    hex: String,
    size: u64,
    /// The submodule captures it names, and whether each is a dirty one.
    subs: Vec<(String, bool)>,
    repository_id: Option<String>,
    execution_commit: Option<String>,
    worktree_tree: Option<String>,
}

/// The manifests staged for a source and what they ask for. Derived from the
/// staged files alone: it is rebuilt from them whenever it is not at hand.
#[derive(Default)]
struct Staged {
    /// By the id local-ci's store knows.
    manifests: BTreeMap<String, ManifestInfo>,
    /// The files the manifests name: hash to size.
    wanted: BTreeMap<String, u64>,
    /// The ones of `wanted` not seen whole yet.
    missing: BTreeSet<String>,
    /// The submodule captures the manifests name.
    named: BTreeSet<String>,
    /// The sum of `wanted`.
    bytes: u64,
}

impl Staged {
    /// Take `m`, whose files new to the source are `new` (see
    /// [`check_manifest_files`]: a size already known is never replaced).
    fn add(&mut self, m: &CiManifest, new: &BTreeMap<&str, u64>, hex: &str, size: u64) {
        for (hash, size) in new {
            self.wanted.insert(hash.to_string(), *size);
            self.missing.insert(hash.to_string());
            self.bytes = self.bytes.saturating_add(*size);
        }
        let mut subs = vec![];
        for s in &m.submodules {
            if let Some(id) = &s.source_id {
                self.named.insert(id.clone());
                subs.push((id.clone(), s.dirty));
            }
        }
        self.manifests.insert(
            m.source_id.clone(),
            ManifestInfo {
                hex: hex.to_string(),
                size,
                subs,
                repository_id: m.repository_id.clone(),
                execution_commit: m.execution_commit.clone(),
                worktree_tree: m.worktree_tree.clone(),
            },
        );
    }

    /// The staged manifests below `root`, leaf first, `root` last.
    fn leaf_first(&self, root: &str) -> Vec<String> {
        fn walk(s: &Staged, id: &str, seen: &mut BTreeSet<String>, out: &mut Vec<String>) {
            if !seen.insert(id.to_string()) {
                return;
            }
            let Some(m) = s.manifests.get(id) else {
                return;
            };
            for (sub, _) in &m.subs {
                walk(s, sub, seen, out);
            }
            out.push(id.to_string());
        }
        let mut out = vec![];
        walk(self, root, &mut BTreeSet::new(), &mut out);
        out
    }

    /// The sources whose commits the bundle carries: `root` and the dirty
    /// submodule captures the staged manifests name.
    fn carried(&self, root: &str) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = [root.to_string()].into();
        for m in self.manifests.values() {
            out.extend(m.subs.iter().filter(|s| s.1).map(|s| s.0.clone()));
        }
        out
    }
}

/// What a manifest from outside must be before anything of it is kept, on
/// top of the store's own check: every name that later becomes part of a
/// path in the store is well-formed.
fn check_manifest(m: &CiManifest) -> Result<(), ApiError> {
    verify_manifest(m).map_err(|e| invalid(format!("the manifest is refused: {e:#}")))?;
    if source_id_of_ci(&m.source_id).is_none() {
        return Err(invalid("the manifest has no well-formed id"));
    }
    if m.object_manifest
        .entries
        .iter()
        .any(|e| !is_blob_hash(&e.hash))
    {
        return Err(invalid(
            "the manifest names a file by something that is not a hash",
        ));
    }
    let subs = m.submodules.iter().filter_map(|s| s.source_id.as_deref());
    if subs.into_iter().any(|id| source_id_of_ci(id).is_none()) {
        return Err(invalid(
            "the manifest names a submodule capture by a malformed id",
        ));
    }
    Ok(())
}

fn entries_of(m: &CiManifest) -> impl Iterator<Item = (&str, u64)> {
    m.object_manifest
        .entries
        .iter()
        .map(|e| (e.hash.as_str(), e.size))
}

/// A manifest that was staged, read again and checked again.
fn read_manifest(path: &Path, limits: &BlobLimits) -> Result<CiManifest, ApiError> {
    let fits = staged_len(path)?.is_some_and(|n| n <= limits.max_manifest_bytes);
    if !fits {
        return Err(internal("incoming blobs", "a staged manifest is gone"));
    }
    let bytes = fs::read(path).map_err(io_err)?;
    let m: CiManifest = serde_json::from_slice(&bytes)
        .map_err(|e| invalid(format!("the manifest does not parse: {e}")))?;
    check_manifest(&m)?;
    Ok(m)
}

/// The first bytes of a staged bundle: enough to hold its header.
fn bundle_head(path: &Path) -> io::Result<Vec<u8>> {
    let mut head = vec![];
    File::open(path)?
        .take(MAX_BUNDLE_HEADER as u64)
        .read_to_end(&mut head)?;
    Ok(head)
}

fn read_reserved(dir: &Path) -> Option<u64> {
    fs::read_to_string(dir.join(RESERVED))
        .ok()?
        .trim()
        .parse()
        .ok()
}

enum Progress {
    /// Not whole yet; this many bytes are held.
    At(u64),
    /// Every byte is there and the digest is right: the staged file.
    Whole(PathBuf),
}

/// The receiving side of the worker's source store.
pub struct Incoming {
    /// `<state_dir>/incoming`.
    root: PathBuf,
    store: SourceStore,
    workspaces: Workspaces,
    env: BTreeMap<String, String>,
    /// What is staged per source, kept between requests. Only read and
    /// written under the source's file lock.
    cache: Mutex<BTreeMap<String, Staged>>,
}

impl Incoming {
    /// `env` is what Git runs with; `workspaces` names the clones the
    /// bundles are unpacked into (the ones attempts are prepared from).
    pub fn new(
        state_dir: &Path,
        store: SourceStore,
        workspaces: Workspaces,
        env: BTreeMap<String, String>,
    ) -> Self {
        Incoming {
            root: state_dir.join("incoming"),
            store,
            workspaces,
            env,
            cache: Mutex::default(),
        }
    }

    fn dir(&self, source: &SourceId) -> PathBuf {
        self.root.join(source.as_str())
    }

    fn lock_file(&self, name: &str) -> Result<File, ApiError> {
        let dir = self.root.join("locks");
        private_dir(&dir).map_err(io_err)?;
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(dir.join(format!("{name}.lock")))
            .map_err(io_err)
    }

    /// `f` locked, held until it is dropped. Whoever holds it (any
    /// connection or process) is waited for, but not for ever.
    fn wait_lock(f: File, limits: &BlobLimits, busy: &str) -> Result<File, ApiError> {
        let start = Instant::now();
        loop {
            match f.try_lock() {
                Ok(()) => return Ok(f),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(e)) => return Err(io_err(e)),
            }
            if start.elapsed() >= Duration::from_millis(limits.lock_wait_ms) {
                return Err(overloaded(busy));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The lock of `source`: one request on a source at a time.
    fn lock(&self, source: &SourceId, limits: &BlobLimits) -> Result<File, ApiError> {
        let f = self.lock_file(&lock_shard(source.as_str()).to_string())?;
        let busy = format!("source {source} is busy with another transfer");
        Self::wait_lock(f, limits, &busy)
    }

    /// The lock over all sources: how many are part-way and what they have
    /// reserved is read and changed only under it. Taken after a source's
    /// own lock, never before; whoever holds it only tries other sources'
    /// locks, and never waits for one.
    fn lock_all(&self, limits: &BlobLimits) -> Result<File, ApiError> {
        let f = self.lock_file("all")?;
        Self::wait_lock(f, limits, "the incoming sources are busy")
    }

    fn published(&self, ci_id: &str) -> Result<bool, ApiError> {
        self.store
            .manifest(ci_id)
            .map(|m| m.is_some())
            .map_err(|e| internal("source store", format!("{e:#}")))
    }

    /// Remove what we staged for a source, and its directory when nothing
    /// else is in it. Call under the source's lock.
    fn discard(&self, source: &str) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(source);
        let dir = self.root.join(source);
        for sub in [dir.join(PARTS), dir.join(FILES), dir.clone()] {
            for f in listing(&sub).unwrap_or_default() {
                remove_staged(&sub.join(f.name));
            }
        }
        let _ = fs::remove_dir(dir.join(PARTS));
        let _ = fs::remove_dir(dir.join(FILES));
        // the reservation goes last: a directory without one counts for all
        // a source may take
        remove_staged(&dir.join(RESERVED_TMP));
        remove_staged(&dir.join(RESERVED));
        let _ = fs::remove_dir(&dir);
    }

    /// Is no staged file of ours in the staging directory `dir`?
    fn holds_nothing(dir: &Path) -> bool {
        [dir.join(PARTS), dir.to_path_buf(), dir.join(FILES)]
            .iter()
            .all(|d| listing(d).is_ok_and(|l| l.is_empty()))
    }

    /// Nothing of a source that has no staged file stays.
    fn discard_if_empty(&self, source: &SourceId) {
        if Self::holds_nothing(&self.dir(source)) {
            self.discard(source.as_str());
        }
    }

    /// What is staged for the source: from the cache, or read again from the
    /// staged manifests (after a restart, or when another process added one).
    fn staged(&self, source: &SourceId, limits: &BlobLimits) -> Result<Staged, ApiError> {
        let dir = self.dir(source);
        let done: Vec<StagedFile> = listing(&dir)?
            .into_iter()
            .filter(|f| f.kind == SourceBlobKind::Manifest && f.done)
            .collect();
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(source.as_str());
        if let Some(st) = cached {
            let same = st.manifests.len() == done.len()
                && done
                    .iter()
                    .all(|f| st.manifests.values().any(|m| m.hex == f.hex));
            if same {
                return Ok(st);
            }
        }
        let mut st = Staged::default();
        for f in done {
            let path = dir.join(&f.name);
            let taken = read_manifest(&path, limits).ok().and_then(|m| {
                let new = check_manifest_files(limits, &st.wanted, entries_of(&m)).ok()?;
                st.add(&m, &new, &f.hex, f.size);
                Some(())
            });
            if taken.is_none() {
                // Not what this module would have promoted: ours, and useless.
                remove_staged(&path);
            }
        }
        Ok(st)
    }

    fn keep(&self, source: &SourceId, st: Staged) {
        if !self.dir(source).is_dir() {
            return;
        }
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        // The cache only saves reading manifests again: it is emptied rather
        // than grown past what may be staged at once.
        if cache.len() >= 64 {
            cache.clear();
        }
        cache.insert(source.as_str().to_string(), st);
    }

    /// The sources that are part-way: the directories named like a source.
    fn staged_sources(&self) -> Result<Vec<String>, ApiError> {
        let mut out = vec![];
        for entry in fs::read_dir(&self.root).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if SourceId::parse(name).is_ok_and(|id| id.as_str() == name) {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    /// When the newest thing of a staged source was written.
    fn newest_ms(dir: &Path) -> u64 {
        let ms = |m: fs::Metadata| {
            m.modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(u64::MAX, |d| d.as_millis() as u64)
        };
        let mut newest = fs::symlink_metadata(dir).map_or(u64::MAX, ms);
        for sub in [dir.to_path_buf(), dir.join(PARTS), dir.join(FILES)] {
            for entry in fs::read_dir(sub).into_iter().flatten().flatten() {
                newest = newest.max(entry.metadata().map_or(u64::MAX, ms));
            }
        }
        newest
    }

    /// Room for one more source, or `OVERLOADED`. When there is none, the
    /// sources nobody added to for a long time are dropped first (each under
    /// its own lock, and only when that lock is free). `source` is the one
    /// whose lock the caller holds; the caller holds the lock over all
    /// sources too.
    fn admit(&self, limits: &BlobLimits, source: &SourceId) -> Result<(), ApiError> {
        if may_stage_source(limits, self.staged_sources()?.len()) {
            return Ok(());
        }
        let now = wall_ms();
        for other in self.staged_sources()? {
            if !is_stale(limits, now, Self::newest_ms(&self.root.join(&other))) {
                continue;
            }
            if lock_shard(&other) == lock_shard(source.as_str()) {
                self.discard(&other);
                continue;
            }
            let f = self.lock_file(&lock_shard(&other).to_string())?;
            if f.try_lock().is_ok() {
                self.discard(&other);
            }
        }
        if may_stage_source(limits, self.staged_sources()?.len()) {
            return Ok(());
        }
        Err(overloaded("too many sources are being received"))
    }

    /// Let the source take `total` bytes in all ([`source_total`]), or say
    /// why not. With `create`, a source that has no staging directory yet
    /// gets one (when there is room for one more source). Counting the
    /// sources, adding up what they reserved, looking at the disk and
    /// writing the new reservation happen under the one lock over all
    /// sources, so two requests cannot both take the last room.
    fn reserve(
        &self,
        limits: &BlobLimits,
        source: &SourceId,
        total: Option<u64>,
        create: bool,
    ) -> Result<(), ApiError> {
        let _all = self.lock_all(limits)?;
        let dir = self.dir(source);
        let exists = dir.is_dir();
        if !exists {
            if !create {
                return Err(internal("incoming blobs", "the source is not staged"));
            }
            self.admit(limits, source)?;
        }
        let held = read_reserved(&dir).unwrap_or(0);
        let mut others = 0u64;
        for other in self.staged_sources()? {
            if other == source.as_str() {
                continue;
            }
            let other_dir = self.root.join(&other);
            let theirs = match read_reserved(&other_dir) {
                Some(n) => n,
                // being removed just now, or only something that is not
                // ours is left in it
                None if !other_dir.is_dir() || Self::holds_nothing(&other_dir) => 0,
                // unknown: it may take all a source may
                None => limits.max_source_bytes,
            };
            others = others.saturating_add(theirs);
        }
        let free = free_bytes(&self.root).map_err(io_err)?;
        let reserved =
            check_reservation(limits, total, held, others, free).map_err(|r| match r {
                Refusal::Source => invalid("the source is larger than this worker accepts"),
                Refusal::Incoming => {
                    overloaded("the sources being received take all the room there is for them")
                }
                Refusal::Disk => overloaded("the disk of this worker has no room for the source"),
            })?;
        if !exists {
            private_dir(&dir).map_err(io_err)?;
        }
        if !exists || reserved != held || read_reserved(&dir).is_none() {
            let tmp = dir.join(RESERVED_TMP);
            fs::write(&tmp, reserved.to_string()).map_err(io_err)?;
            fs::rename(&tmp, dir.join(RESERVED)).map_err(io_err)?;
        }
        Ok(())
    }

    /// Before the staged file of the blob of `r` is created: put away what
    /// it replaces, count what is part-way, and reserve what it adds.
    fn begin(
        &self,
        limits: &BlobLimits,
        r: &BlobPutRequest,
        st: &Staged,
        root_staged: bool,
    ) -> Result<(), ApiError> {
        let dir = self.dir(&r.source_id);
        let new = StagedName {
            kind: r.kind,
            hex: hex_of(&r.digest),
            size: r.total_size,
            done: false,
        };
        let home = match r.kind {
            SourceBlobKind::Content => dir.join(PARTS),
            _ => dir.clone(),
        };
        let mut kept = vec![];
        for f in listing(&dir)? {
            if displaces(&new, root_staged, &f.as_name()) {
                remove_staged(&dir.join(&f.name));
            } else {
                kept.push(f);
            }
        }
        let mut parts = kept.iter().filter(|f| !f.done).count();
        for f in listing(&dir.join(PARTS))? {
            if displaces(&new, root_staged, &f.as_name()) {
                remove_staged(&dir.join(PARTS).join(&f.name));
            } else {
                parts += 1;
            }
        }
        if !may_start_partial(limits, parts) {
            return Err(overloaded("too many blobs of the source are part-way"));
        }
        if r.kind != SourceBlobKind::Content {
            // a file is reserved when its manifest is staged; these when
            // they start
            let (manifests, bundle) = announced(&kept);
            let (manifests, bundle) = match r.kind {
                SourceBlobKind::Manifest => (manifests.checked_add(r.total_size), Some(bundle)),
                _ => (Some(manifests), bundle.checked_add(r.total_size)),
            };
            let total = manifests
                .zip(bundle)
                .and_then(|(m, b)| source_total(m, b, st.bytes));
            self.reserve(limits, &r.source_id, total, true)?;
        }
        private_dir(&home).map_err(io_err)
    }

    /// Take the chunk of `r` into `part`, the staged file of its blob.
    /// `begin` runs before the file is created.
    fn append(
        &self,
        part: &Path,
        r: &BlobPutRequest,
        begin: impl FnOnce() -> Result<(), ApiError>,
    ) -> Result<Progress, ApiError> {
        let held = staged_len(part)?;
        let data = &r.data.0;
        let (skip, complete) =
            match plan_chunk(held.unwrap_or(0), r.total_size, r.offset, data.len()) {
                ChunkStep::Behind => return Ok(Progress::At(held.unwrap_or(0))),
                ChunkStep::Overlong => {
                    remove_staged(part);
                    return Err(invalid(
                        "the staged bytes are not this blob's; send it again from the start",
                    ));
                }
                ChunkStep::Append { skip, complete } => (skip, complete),
            };
        let mut open = fs::OpenOptions::new();
        open.append(true).custom_flags(libc::O_NOFOLLOW);
        if held.is_none() {
            begin()?;
            open.create_new(true).mode(0o600);
        }
        let mut f = open.open(part).map_err(io_err)?;
        f.write_all(&data[skip..]).map_err(io_err)?;
        if !complete {
            return Ok(Progress::At(
                held.unwrap_or(0).max(r.offset + data.len() as u64),
            ));
        }
        f.sync_all().map_err(io_err)?;
        drop(f);
        if digest_of_file(part).map_err(io_err)? != r.digest {
            remove_staged(part);
            return Err(invalid(format!(
                "the bytes of {} do not have that digest; send it again from the start",
                r.digest
            )));
        }
        Ok(Progress::Whole(part.to_path_buf()))
    }

    /// A manifest arrived whole: take it for the source, or refuse it. A
    /// manifest that is refused for what it says is removed; one there is no
    /// room for right now (`OVERLOADED`) stays, and is taken when its last
    /// chunk is sent again.
    fn stage_manifest(
        &self,
        limits: &BlobLimits,
        r: &BlobPutRequest,
        root_id: &str,
        part: &Path,
        st: &mut Staged,
    ) -> Result<(), ApiError> {
        let dir = self.dir(&r.source_id);
        let hex = hex_of(&r.digest);
        let checked = (|| {
            let m = read_manifest(part, limits)?;
            manifest_role(
                limits,
                m.source_id == root_id,
                st.manifests.contains_key(root_id),
                st.named.contains(&m.source_id),
                st.manifests.len(),
            )
            .map_err(invalid)?;
            if st.manifests.contains_key(&m.source_id) {
                // The same manifest in another spelling: the staged one stays.
                return Ok(None);
            }
            let stored = self
                .store
                .manifest(&m.source_id)
                .map_err(|e| internal("source store", format!("{e:#}")))?;
            // A capture the store holds is never written again (the one that
            // is there is the one attempts were prepared from): the same
            // manifest is staged as any other, so the source knows it has
            // it, and is left out when the source is published; another one
            // under that id is refused.
            if stored.is_some_and(|stored| stored != m) {
                return Err(invalid(format!(
                    "another manifest is published as {}",
                    m.source_id
                )));
            }
            Ok(Some(m))
        })();
        let m = match checked {
            Ok(Some(m)) => m,
            Ok(None) => {
                remove_staged(part);
                return Ok(());
            }
            Err(e) => {
                remove_staged(part);
                return Err(e);
            }
        };
        let new = match check_manifest_files(limits, &st.wanted, entries_of(&m)) {
            Ok(new) => new,
            Err(why) => {
                remove_staged(part);
                return Err(invalid(why));
            }
        };
        let (manifests, bundle) = announced(&listing(&dir)?);
        let files = new
            .values()
            .try_fold(st.bytes, |sum, size| sum.checked_add(*size));
        let total = files.and_then(|f| source_total(manifests, bundle, f));
        if let Err(e) = self.reserve(limits, &r.source_id, total, false) {
            if e.code != ErrorCode::Overloaded {
                remove_staged(part);
            }
            return Err(e);
        }
        let done = dir.join(staged_name(r.kind, hex, r.total_size, true));
        fs::rename(part, done).map_err(io_err)?;
        st.add(&m, &new, hex, r.total_size);
        Ok(())
    }

    /// The clone of a repository the bundles are unpacked into, when there
    /// is one. Asking never makes one.
    fn objects(&self, repository_id: &str) -> Option<ObjectsRepo> {
        let dir = self
            .workspaces
            .clones_root()
            .join(clone_dir_name(repository_id));
        dir.join(".git")
            .exists()
            .then(|| ObjectsRepo::new(self.env.clone(), dir))
    }

    /// Does `repo` hold the commits the bundle of the source carries (those
    /// of the sources in `carried`), each below its snapshot ref and with
    /// the tree its manifest gives?
    fn commits_here(&self, repo: &ObjectsRepo, st: &Staged, carried: &BTreeSet<String>) -> bool {
        carried.iter().all(|id| match st.manifests.get(id) {
            Some(info) => info.execution_commit.as_deref().is_none_or(|commit| {
                repo.check_snapshot(id, commit, info.worktree_tree.as_deref())
                    .is_ok()
            }),
            // published before, by another transfer: ask the store's copy
            None => matches!(
                self.store.manifest(id),
                Ok(Some(m)) if repo.check_reachable(&m).is_ok()
            ),
        })
    }

    /// What the clone of the source's repository holds, for the coordinator
    /// to build a bundle on: the commits of the source that are there
    /// already, then the tips of the other snapshots. `None` when the
    /// repository is not known (no manifest of the source here, or it is not
    /// a Git source).
    fn have(&self, root_id: &str, st: &Staged) -> Option<RepositoryHave> {
        let (repository_id, ids) = match st.manifests.get(root_id) {
            Some(root) => {
                root.execution_commit.as_ref()?;
                (root.repository_id.clone()?, st.carried(root_id))
            }
            None => {
                let m = self.store.manifest(root_id).ok().flatten()?;
                m.execution_commit.as_ref()?;
                (m.repository_id.clone()?, [root_id.to_string()].into())
            }
        };
        let tips = self
            .objects(&repository_id)
            .and_then(|repo| repo.snapshot_tips(MAX_SNAPSHOT_REFS).ok())
            .unwrap_or_default();
        let needed: Vec<String> = tips
            .iter()
            .filter(|t| ids.contains(&t.0))
            .map(|t| t.1.clone())
            .collect();
        let tips = tips.into_iter().map(|t| (t.1, t.2)).collect();
        Some(RepositoryHave {
            repository_id,
            commits: order_have(&needed, tips, MAX_HAVE),
        })
    }

    fn bundle_done(dir: &Path) -> Result<Option<PathBuf>, ApiError> {
        Ok(listing(dir)?
            .into_iter()
            .find(|f| f.kind == SourceBlobKind::Bundle && f.done)
            .map(|f| dir.join(f.name)))
    }

    /// Unpack the staged `bundle` into the clone of `repository_id`.
    ///
    /// A bundle built on commits the clone does not hold is not a broken
    /// one: the coordinator built it on a list that is out of date (or on
    /// none). It is dropped, nothing is made, and the answer (`CONFLICT`,
    /// `bundle_base_missing`) says what the clone does hold, so the next
    /// bundle fits. A bundle that is not one, or does not bring what the
    /// manifest names, is `INVALID_INPUT`.
    fn unpack(
        &self,
        limits: &BlobLimits,
        bundle: &Path,
        repository_id: &str,
        root_id: &str,
        st: &Staged,
    ) -> Result<ObjectsRepo, ApiError> {
        let refuse = |why: String| {
            remove_staged(bundle);
            invalid(format!("the bundle is refused: {why}"))
        };
        let prerequisites = bundle_head(bundle)
            .map_err(io_err)
            .and_then(|head| bundle_prerequisites(&head).map_err(|why| refuse(why.to_string())))?;
        let existing = self.objects(repository_id);
        let fits = match &existing {
            Some(repo) => repo.holds_base(&prerequisites),
            None => prerequisites.is_empty(),
        };
        if !fits {
            remove_staged(bundle);
            let have = self.have(root_id, st).map_or(vec![], |h| h.commits);
            return Err(ApiError::conflict(
                ConflictReason::BundleBaseMissing { have },
                "the bundle is built on commits this worker does not hold; build it on what blob_verify lists",
            ));
        }
        let repo = match existing {
            Some(repo) => repo,
            None => self.workspaces.ensure_clone(repository_id, None)?,
        };
        let root = &st.manifests[root_id];
        let dir = bundle.parent().expect("a staged file is in a directory");
        let body = read_manifest(
            &dir.join(staged_name(
                SourceBlobKind::Manifest,
                &root.hex,
                root.size,
                true,
            )),
            limits,
        )?;
        repo.receive_bundle(bundle, &body)
            .map_err(|e| refuse(format!("{e:#}")))?;
        if !self.commits_here(&repo, st, &st.carried(root_id)) {
            return Err(refuse(
                "it does not bring the commits the manifests name".into(),
            ));
        }
        Ok(repo)
    }

    /// Publish the source when everything it needs is here. `Ok(false)`:
    /// something is still missing. An error is about what was sent (a bundle
    /// that does not fit, a manifest the store refuses).
    ///
    /// Everything that can refuse the source comes before the first file is
    /// moved into the store, so a source that is refused or dropped leaves
    /// nothing there.
    fn complete(
        &self,
        limits: &BlobLimits,
        source: &SourceId,
        root_id: &str,
        st: &mut Staged,
    ) -> Result<bool, ApiError> {
        if !st.manifests.contains_key(root_id) {
            return Ok(false);
        }
        for id in &st.named {
            if !st.manifests.contains_key(id) && !self.published(id)? {
                return Ok(false);
            }
        }
        let dir = self.dir(source);
        let waiting = |hash: &str, size: u64| {
            dir.join(FILES)
                .join(staged_name(SourceBlobKind::Content, hash, size, true))
        };
        while let Some(hash) = st.missing.first() {
            let here =
                self.store.has_blob(hash) || staged_len(&waiting(hash, st.wanted[hash]))?.is_some();
            if !here {
                return Ok(false);
            }
            st.missing.pop_first();
        }
        let root = &st.manifests[root_id];
        let carried = st.carried(root_id);
        let mut objects = None;
        if let (Some(_), Some(repository_id)) = (&root.execution_commit, &root.repository_id) {
            let repo = match Self::bundle_done(&dir)? {
                Some(bundle) => self.unpack(limits, &bundle, repository_id, root_id, st)?,
                None => match self.objects(repository_id) {
                    Some(repo) if self.commits_here(&repo, st, &carried) => repo,
                    _ => return Ok(false),
                },
            };
            objects = Some(repo);
        }
        let store_err = |e: anyhow::Error| internal("source store", format!("{e:#}"));
        for (hash, size) in &st.wanted {
            if self.store.has_blob(hash) {
                continue;
            }
            let path = waiting(hash, *size);
            let want = BlobWant {
                hash: hash.clone(),
                size: *size,
            };
            let mut f = File::open(&path).map_err(io_err)?;
            self.store.ingest_blob(&want, &mut f).map_err(store_err)?;
            remove_staged(&path);
        }
        for id in st.leaf_first(root_id) {
            if id != root_id && self.published(&id)? {
                // published by another transfer meanwhile: it stays as it is
                continue;
            }
            let info = &st.manifests[&id];
            let name = staged_name(SourceBlobKind::Manifest, &info.hex, info.size, true);
            let m = read_manifest(&dir.join(name), limits)?;
            self.store
                .publish_received_one(&m, carried.contains(&id), objects.as_ref())
                .map_err(|e| invalid(format!("the source is refused: {e:#}")))?;
        }
        self.discard(source.as_str());
        Ok(true)
    }

    fn put_locked(
        &self,
        limits: &BlobLimits,
        r: &BlobPutRequest,
        root_id: &str,
        st: &mut Staged,
    ) -> Result<Option<u64>, ApiError> {
        let dir = self.dir(&r.source_id);
        let hex = hex_of(&r.digest);
        let has_root = st.manifests.contains_key(root_id);
        let first = || {
            ApiError::new(
                ErrorCode::Conflict,
                format!("send the manifest of source {} first", r.source_id),
            )
        };
        let name = |done| staged_name(r.kind, hex, r.total_size, done);
        match r.kind {
            SourceBlobKind::Manifest => {
                if staged_len(&dir.join(name(true)))?.is_none() {
                    let part = dir.join(name(false));
                    let taken = self
                        .append(&part, r, || self.begin(limits, r, st, has_root))
                        .and_then(|p| match p {
                            Progress::At(n) => Ok(Some(n)),
                            Progress::Whole(part) => self
                                .stage_manifest(limits, r, root_id, &part, st)
                                .map(|()| None),
                        });
                    match taken {
                        Ok(Some(n)) => return Ok(Some(n)),
                        Ok(None) => {}
                        Err(e) => {
                            // Nothing of a source that has nothing staged stays.
                            self.discard_if_empty(&r.source_id);
                            return Err(e);
                        }
                    }
                }
            }
            SourceBlobKind::Content => {
                if !has_root {
                    return Err(first());
                }
                check_content(st.wanted.get(hex).copied(), r.total_size).map_err(invalid)?;
                let part = dir.join(PARTS).join(name(false));
                let done = dir.join(FILES).join(name(true));
                if self.store.has_blob(hex) || staged_len(&done)?.is_some() {
                    remove_staged(&part);
                } else {
                    match self.append(&part, r, || self.begin(limits, r, st, has_root))? {
                        Progress::At(n) => return Ok(Some(n)),
                        Progress::Whole(part) => {
                            // It waits here, whole, until the source is
                            // published: the store holds nothing of a source
                            // that may still be dropped.
                            private_dir(&dir.join(FILES)).map_err(io_err)?;
                            fs::rename(&part, &done).map_err(io_err)?;
                        }
                    }
                }
                st.missing.remove(hex);
            }
            SourceBlobKind::Bundle => {
                let Some(root) = st.manifests.get(root_id) else {
                    return Err(first());
                };
                if root.execution_commit.is_none() {
                    return Err(invalid("the source has no Git objects: it takes no bundle"));
                }
                if staged_len(&dir.join(name(true)))?.is_none() {
                    let part = dir.join(name(false));
                    match self.append(&part, r, || self.begin(limits, r, st, has_root))? {
                        Progress::At(n) => return Ok(Some(n)),
                        Progress::Whole(part) => {
                            fs::rename(&part, dir.join(name(true))).map_err(io_err)?;
                        }
                    }
                }
            }
        }
        self.complete(limits, &r.source_id, root_id, st)?;
        Ok(None)
    }

    fn verify_locked(
        &self,
        r: &BlobVerifyRequest,
        root_id: &str,
        ready: bool,
        st: &Staged,
    ) -> Result<BlobVerifyResponse, ApiError> {
        let dir = self.dir(&r.source_id);
        let bundle_staged = Self::bundle_done(&dir)?.is_some();
        // Are the commits a bundle would bring missing?
        let commits_missing = match st.manifests.get(root_id) {
            Some(root) => match (&root.execution_commit, &root.repository_id) {
                (Some(_), Some(repository_id)) => !self
                    .objects(repository_id)
                    .is_some_and(|repo| self.commits_here(&repo, st, &st.carried(root_id))),
                _ => false,
            },
            None => false,
        };
        let mut out = BlobVerifyResponse {
            schema_version: SchemaVersion::CURRENT,
            ready,
            bundle_needed: !ready && commits_missing && !bundle_staged,
            have: self.have(root_id, st).into_iter().collect(),
            present: vec![],
            missing: vec![],
            corrupt: vec![],
        };
        for b in &r.blobs {
            let hex = hex_of(&b.digest);
            let name = |done| staged_name(b.kind, hex, b.total_size, done);
            let verdict = match b.kind {
                SourceBlobKind::Content => {
                    // held whole: in the store, or waiting here under the
                    // size its manifest gives
                    let mut stored = fs::metadata(self.store.object_store().object_path(hex))
                        .ok()
                        .filter(|m| m.is_file())
                        .map(|m| m.len());
                    if let (None, Some(size)) = (stored, st.wanted.get(hex)) {
                        let waiting = dir.join(FILES).join(staged_name(b.kind, hex, *size, true));
                        stored = staged_len(&waiting)?;
                    }
                    let partial = staged_len(&dir.join(PARTS).join(name(false)))?;
                    content_verdict(stored, partial, b.total_size)
                }
                SourceBlobKind::Manifest | SourceBlobKind::Bundle => {
                    let done = staged_len(&dir.join(name(true)))?.is_some();
                    let partial = staged_len(&dir.join(name(false)))?;
                    let settled = ready
                        || (b.kind == SourceBlobKind::Bundle
                            && st.manifests.contains_key(root_id)
                            && !commits_missing);
                    staged_verdict(done, settled, partial, b.total_size)
                }
            };
            match verdict {
                Verdict::Present => out.present.push(b.digest.clone()),
                Verdict::Corrupt => out.corrupt.push(b.digest.clone()),
                Verdict::Missing { received } => out.missing.push(BlobProgress {
                    digest: b.digest.clone(),
                    received,
                }),
            }
        }
        Ok(out)
    }
}

impl BlobSink for Incoming {
    fn put(&self, limits: &BlobLimits, r: &BlobPutRequest) -> Result<BlobPutResponse, ApiError> {
        check_put(limits, r.kind, r.total_size, r.offset, r.data.0.len()).map_err(invalid)?;
        let root_id =
            ci_id_of(&r.source_id).ok_or_else(|| invalid("the source id is malformed"))?;
        private_dir(&self.root).map_err(io_err)?;
        let _lock = self.lock(&r.source_id, limits)?;
        let answer = |received, complete| BlobPutResponse {
            schema_version: SchemaVersion::CURRENT,
            digest: r.digest.clone(),
            received,
            complete,
        };
        if self.published(&root_id)? {
            // The source is whole: nothing more is taken for it.
            self.discard(r.source_id.as_str());
            if r.kind == SourceBlobKind::Content && !self.store.has_blob(hex_of(&r.digest)) {
                return Err(invalid("no manifest of the source names this blob"));
            }
            return Ok(answer(r.total_size, true));
        }
        let mut st = self.staged(&r.source_id, limits)?;
        let result = self.put_locked(limits, r, &root_id, &mut st);
        self.keep(&r.source_id, st);
        Ok(match result? {
            Some(received) => answer(received, false),
            None => answer(r.total_size, true),
        })
    }

    fn verify(
        &self,
        limits: &BlobLimits,
        r: &BlobVerifyRequest,
    ) -> Result<BlobVerifyResponse, ApiError> {
        if r.blobs.len() > MAX_VERIFY_DIGESTS {
            return Err(invalid("too many blobs in one blob_verify"));
        }
        let root_id =
            ci_id_of(&r.source_id).ok_or_else(|| invalid("the source id is malformed"))?;
        private_dir(&self.root).map_err(io_err)?;
        let _lock = self.lock(&r.source_id, limits)?;
        let ready = self.published(&root_id)?;
        let st = self.staged(&r.source_id, limits)?;
        let result = self.verify_locked(r, &root_id, ready, &st);
        self.keep(&r.source_id, st);
        result
    }
}

// ------------------------------------------------------------ agent side

fn sink(host: &dyn Host) -> Result<&dyn BlobSink, ApiError> {
    host.blobs().ok_or_else(|| {
        ApiError::new(
            ErrorCode::NotImplemented,
            "this worker does not receive sources",
        )
    })
}

/// `blob_put`: a chunk of a blob of a source. A coordinator of an epoch
/// older than one this worker has seen is refused.
pub fn put(
    store: &WorkerStore,
    host: &dyn Host,
    limits: &BlobLimits,
    r: &BlobPutRequest,
) -> Result<BlobPutResponse, ApiError> {
    let sink = sink(host)?;
    let seen = store
        .max_coordinator_epoch()
        .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
    if r.coordinator_epoch < seen {
        return Err(ApiError::new(
            ErrorCode::StaleEpoch,
            format!(
                "coordinator epoch {} is older than {}",
                r.coordinator_epoch.0, seen.0
            ),
        ));
    }
    sink.put(limits, r)
}

/// `blob_verify`: what of a source this worker holds. Changes nothing.
pub fn verify(
    host: &dyn Host,
    limits: &BlobLimits,
    r: &BlobVerifyRequest,
) -> Result<BlobVerifyResponse, ApiError> {
    sink(host)?.verify(limits, r)
}

/// The artifact file below `<attempts_root>/<attempt>/artifacts`, opened
/// without following a link anywhere on the way: each directory is opened
/// relative to the one before ([`open_beneath`]), so nothing that is renamed
/// between a check and the open can lead elsewhere.
fn open_artifact(dir: &Path, a: &ArtifactRecord) -> Result<File, ApiError> {
    let rel = relative_path(&a.path).ok_or_else(|| internal("stored artifact", &a.path))?;
    match open_beneath(dir, &rel) {
        Ok(f) => Ok(f),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(ApiError::new(
            ErrorCode::NotFound,
            format!("artifact {} is not on this worker any more", a.digest),
        )),
        Err(e) => Err(internal(
            "artifact store",
            format!("{} is not a plain file below the artifacts: {e}", a.path),
        )),
    }
}

/// `blob_get`: a part of an artifact of an ended attempt, by its digest.
/// Only what the attempt's stored result lists is served. The caller has
/// checked that the attempt is known.
pub fn get(
    store: &WorkerStore,
    attempts_root: &Path,
    r: &BlobGetRequest,
) -> Result<BlobGetResponse, ApiError> {
    let not_found = || {
        ApiError::new(
            ErrorCode::NotFound,
            format!("attempt {} has no artifact {}", r.attempt_id, r.digest),
        )
    };
    let result = store
        .get_result(&r.attempt_id)
        .map_err(|e| ApiError::new(e.code(), e.to_string()))?
        .ok_or_else(not_found)?;
    let artifacts: Vec<ArtifactRecord> = serde_json::from_value(result["artifacts"].clone())
        .map_err(|e| internal("stored artifacts", e))?;
    let a = artifacts
        .iter()
        .find(|a| a.digest == r.digest)
        .ok_or_else(not_found)?;
    let dir = attempts_root.join(r.attempt_id.as_str()).join("artifacts");
    let f = open_artifact(&dir, a)?;
    let meta = f.metadata().map_err(io_err)?;
    if !meta.is_file() || meta.len() != a.size {
        return Err(internal(
            "artifact store",
            "the artifact is not as recorded",
        ));
    }
    let (len, eof) = get_range(a.size, r.offset, clamp_chunk(r.max_bytes)).map_err(invalid)?;
    let mut data = vec![0u8; len];
    f.read_exact_at(&mut data, r.offset).map_err(io_err)?;
    Ok(BlobGetResponse {
        schema_version: SchemaVersion::CURRENT,
        digest: r.digest.clone(),
        offset: r.offset,
        total_size: a.size,
        data: Bytes(data),
        eof,
    })
}
