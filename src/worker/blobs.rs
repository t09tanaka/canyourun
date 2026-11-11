//! The blob operations of the worker (spec chapter 11): receiving a source
//! from the coordinator, and serving an attempt's artifacts back.
//!
//! A source arrives as blobs of three kinds: its manifests (its own first,
//! then the captures of the submodules it names), its files (named by the
//! hash of their content) and, for a Git source, one bundle. A manifest is the
//! authority: only what a staged manifest names is taken, with the size it
//! gives. Bytes land in `<state_dir>/incoming/<source_id>/`; the only state
//! of a transfer is the length of its staged file, so a transfer that was cut
//! resumes on another connection or after a restart of the agent. The digest
//! is checked when the last byte is there. Once a source has everything, the
//! bundle is unpacked into the repository's clone and the manifests are
//! published in the store (leaf first, the source's own last); only then can
//! an attempt be prepared from it.
//!
//! Requests for one source are serialized by a file lock, whichever
//! connection or process they come from. The decisions are in
//! [`super::blob_policy`].

use super::blob_policy::{
    check_content, check_manifest_files, check_put, content_verdict, get_range, is_stale,
    lock_shard, manifest_role, may_stage_source, may_start_partial, parse_staged_name, plan_chunk,
    staged_name, staged_verdict, BlobLimits, ChunkStep, Verdict,
};
use super::host::Host;
use super::policy::MAX_VERIFY_DIGESTS;
use crate::error::{ApiError, ErrorCode};
use crate::ids::{Digest, SourceId};
use crate::platform::wall_ms;
use crate::protocol::chunk::clamp_chunk;
use crate::source::classify::{ci_id_of, source_id_of_ci};
use crate::source::diff::{is_blob_hash, BlobWant};
use crate::source::store::{verify_manifest, ObjectsRepo};
use crate::source::SourceStore;
use crate::storage::WorkerStore;
use crate::supervisor::spool_plan::relative_path;
use crate::wire::{
    ArtifactRecord, BlobGetRequest, BlobGetResponse, BlobProgress, BlobPutRequest, BlobPutResponse,
    BlobVerifyRequest, BlobVerifyResponse, Bytes, SchemaVersion, SourceBlobKind,
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

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
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

/// The manifests staged for a source and what they ask for. Derived from the
/// staged files alone: it is rebuilt from them whenever it is not at hand.
#[derive(Default)]
struct Staged {
    /// By the id local-ci's store knows.
    manifests: BTreeMap<String, CiManifest>,
    /// The files the manifests name: hash to size.
    wanted: BTreeMap<String, u64>,
    /// The ones of `wanted` not seen in the store yet.
    missing: BTreeSet<String>,
    /// The submodule captures the manifests name.
    named: BTreeSet<String>,
    /// The sum of `wanted`.
    bytes: u64,
}

impl Staged {
    /// The sizes of the files `m` names that are new to the source, each once.
    fn new_files<'a>(&self, m: &'a CiManifest) -> BTreeMap<&'a str, u64> {
        m.object_manifest
            .entries
            .iter()
            .filter(|e| !self.wanted.contains_key(&e.hash))
            .map(|e| (e.hash.as_str(), e.size))
            .collect()
    }

    fn add(&mut self, m: CiManifest) {
        for e in &m.object_manifest.entries {
            if self.wanted.insert(e.hash.clone(), e.size).is_none() {
                self.bytes = self.bytes.saturating_add(e.size);
                self.missing.insert(e.hash.clone());
            }
        }
        for s in &m.submodules {
            if let Some(id) = &s.source_id {
                self.named.insert(id.clone());
            }
        }
        self.manifests.insert(m.source_id.clone(), m);
    }

    /// The staged manifests below `root`, leaf first, `root` last.
    fn leaf_first(&self, root: &str) -> Vec<CiManifest> {
        fn walk(s: &Staged, id: &str, seen: &mut BTreeSet<String>, out: &mut Vec<CiManifest>) {
            if !seen.insert(id.to_string()) {
                return;
            }
            let Some(m) = s.manifests.get(id) else {
                return;
            };
            for sub in &m.submodules {
                if let Some(id) = &sub.source_id {
                    walk(s, id, seen, out);
                }
            }
            out.push(m.clone());
        }
        let mut out = vec![];
        walk(self, root, &mut BTreeSet::new(), &mut out);
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

    fn lock_file(&self, source: &str) -> Result<File, ApiError> {
        let dir = self.root.join("locks");
        private_dir(&dir).map_err(io_err)?;
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(dir.join(format!("{}.lock", lock_shard(source))))
            .map_err(io_err)
    }

    /// The lock of `source`, held until the file is dropped. Another request
    /// on the same source (from any connection or process) is waited for,
    /// but not for ever.
    fn lock(&self, source: &SourceId, limits: &BlobLimits) -> Result<File, ApiError> {
        let f = self.lock_file(source.as_str())?;
        let start = Instant::now();
        loop {
            match f.try_lock() {
                Ok(()) => return Ok(f),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(e)) => return Err(io_err(e)),
            }
            if start.elapsed() >= Duration::from_millis(limits.lock_wait_ms) {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    format!("source {source} is busy with another transfer"),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn published(&self, ci_id: &str) -> Result<bool, ApiError> {
        self.store
            .manifest(ci_id)
            .map(|m| m.is_some())
            .map_err(|e| internal("source store", format!("{e:#}")))
    }

    /// The names in a source's staging directory that are ours.
    fn staged_files(dir: &Path) -> Result<Vec<(String, SourceBlobKind, bool)>, ApiError> {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(io_err(e)),
        };
        let mut out = vec![];
        for entry in entries {
            let name = entry.map_err(io_err)?.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some((kind, _, done)) = parse_staged_name(name) {
                out.push((name.to_string(), kind, done));
            }
        }
        Ok(out)
    }

    /// Remove what we staged for a source, and its directory when nothing
    /// else is in it. Call under the source's lock.
    fn discard(&self, source: &str) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(source);
        let dir = self.root.join(source);
        for (name, _, _) in Self::staged_files(&dir).unwrap_or_default() {
            remove_staged(&dir.join(name));
        }
        let _ = fs::remove_dir(&dir);
    }

    /// What is staged for the source: from the cache, or read again from the
    /// staged manifests (after a restart, or when another process added one).
    fn staged(&self, source: &SourceId, limits: &BlobLimits) -> Result<Staged, ApiError> {
        let dir = self.dir(source);
        let files = Self::staged_files(&dir)?;
        let done: Vec<&String> = files
            .iter()
            .filter(|(_, k, done)| *k == SourceBlobKind::Manifest && *done)
            .map(|(n, _, _)| n)
            .collect();
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(source.as_str());
        if let Some(st) = cached {
            if st.manifests.len() == done.len() {
                return Ok(st);
            }
        }
        let mut st = Staged::default();
        for name in done {
            let path = dir.join(name);
            let fits = staged_len(&path)?.is_some_and(|n| n <= limits.max_manifest_bytes);
            let parsed = fits
                .then(|| fs::read(&path).ok())
                .flatten()
                .and_then(|b| serde_json::from_slice::<CiManifest>(&b).ok())
                .filter(|m| check_manifest(m).is_ok());
            match parsed {
                Some(m) => st.add(m),
                // Not what this module would have promoted: ours, and useless.
                None => remove_staged(&path),
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
        for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
            newest = newest.max(entry.metadata().map_or(u64::MAX, ms));
        }
        newest
    }

    /// Room for one more source, or `OVERLOADED`. When there is none, the
    /// sources nobody added to for a long time are dropped first (each under
    /// its own lock, and only when that lock is free). `source` is the one
    /// whose lock the caller holds.
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
            let f = self.lock_file(&other)?;
            if f.try_lock().is_ok() {
                self.discard(&other);
            }
        }
        if may_stage_source(limits, self.staged_sources()?.len()) {
            return Ok(());
        }
        Err(ApiError::new(
            ErrorCode::Overloaded,
            "too many sources are being received",
        ))
    }

    /// Take the chunk of `r` into the staged file of its blob.
    fn append(
        &self,
        limits: &BlobLimits,
        dir: &Path,
        r: &BlobPutRequest,
    ) -> Result<Progress, ApiError> {
        let part = dir.join(staged_name(r.kind, hex_of(&r.digest), false));
        let held = staged_len(&part)?;
        let data = &r.data.0;
        let (skip, complete) =
            match plan_chunk(held.unwrap_or(0), r.total_size, r.offset, data.len()) {
                ChunkStep::Behind => return Ok(Progress::At(held.unwrap_or(0))),
                ChunkStep::Overlong => {
                    remove_staged(&part);
                    return Err(invalid(
                        "the staged bytes are not this blob's; send it again from the start",
                    ));
                }
                ChunkStep::Append { skip, complete } => (skip, complete),
            };
        let mut open = fs::OpenOptions::new();
        open.append(true).custom_flags(libc::O_NOFOLLOW);
        if held.is_none() {
            let partials = Self::staged_files(dir)?
                .iter()
                .filter(|(_, _, done)| !done)
                .count();
            if !may_start_partial(limits, partials) {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    "too many blobs of the source are part-way",
                ));
            }
            open.create_new(true).mode(0o600);
        }
        let mut f = open.open(&part).map_err(io_err)?;
        f.write_all(&data[skip..]).map_err(io_err)?;
        if !complete {
            return Ok(Progress::At(
                held.unwrap_or(0).max(r.offset + data.len() as u64),
            ));
        }
        f.sync_all().map_err(io_err)?;
        drop(f);
        if digest_of_file(&part).map_err(io_err)? != r.digest {
            remove_staged(&part);
            return Err(invalid(format!(
                "the bytes of {} do not have that digest; send it again from the start",
                r.digest
            )));
        }
        Ok(Progress::Whole(part))
    }

    /// A manifest arrived whole: take it for the source, or refuse it.
    fn stage_manifest(
        &self,
        limits: &BlobLimits,
        root_id: &str,
        part: &Path,
        done: &Path,
        st: &mut Staged,
    ) -> Result<(), ApiError> {
        let checked = (|| {
            let bytes = fs::read(part).map_err(io_err)?;
            let m: CiManifest = serde_json::from_slice(&bytes)
                .map_err(|e| invalid(format!("the manifest does not parse: {e}")))?;
            check_manifest(&m)?;
            manifest_role(
                limits,
                m.source_id == root_id,
                st.manifests.contains_key(root_id),
                st.named.contains(&m.source_id),
                st.manifests.len(),
            )
            .map_err(invalid)?;
            check_manifest_files(limits, st.bytes, st.new_files(&m).into_values())
                .map_err(invalid)?;
            Ok(m)
        })();
        let m = match checked {
            Ok(m) => m,
            Err(e) => {
                remove_staged(part);
                return Err(e);
            }
        };
        if st.manifests.contains_key(&m.source_id) {
            // The same manifest in another spelling: the staged one stays.
            remove_staged(part);
            return Ok(());
        }
        fs::rename(part, done).map_err(io_err)?;
        st.add(m);
        Ok(())
    }

    /// The clone of a repository the bundles are unpacked into: the one that
    /// is there, or (with `create`) a new empty one.
    fn objects(&self, repository_id: &str, create: bool) -> Result<Option<ObjectsRepo>, ApiError> {
        let dir = self
            .workspaces
            .clones_root()
            .join(clone_dir_name(repository_id));
        if dir.join(".git").exists() {
            return Ok(Some(ObjectsRepo::new(self.env.clone(), dir)));
        }
        if !create {
            return Ok(None);
        }
        self.workspaces.ensure_clone(repository_id, None).map(Some)
    }

    fn bundle_done(dir: &Path) -> Result<Option<PathBuf>, ApiError> {
        Ok(Self::staged_files(dir)?
            .into_iter()
            .find(|(_, k, done)| *k == SourceBlobKind::Bundle && *done)
            .map(|(name, _, _)| dir.join(name)))
    }

    /// Publish the source when everything it needs is here. `Ok(false)`:
    /// something is still missing. An error is about what was sent (a bundle
    /// that does not hold the commit, a manifest the store refuses).
    fn complete(
        &self,
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
        while let Some(hash) = st.missing.first() {
            if !self.store.has_blob(hash) {
                return Ok(false);
            }
            st.missing.pop_first();
        }
        let root = &st.manifests[root_id];
        let dir = self.dir(source);
        let mut objects = None;
        if let (Some(_), Some(repository_id)) = (&root.execution_commit, &root.repository_id) {
            // A clone is made only to unpack a bundle into it: asking
            // whether the commits are here never makes one.
            let repo = match Self::bundle_done(&dir)? {
                Some(bundle) => {
                    let repo = self
                        .objects(repository_id, true)?
                        .expect("a clone is made when there is none");
                    if let Err(e) = repo.receive_bundle(&bundle, root) {
                        remove_staged(&bundle);
                        return Err(invalid(format!("the bundle is refused: {e:#}")));
                    }
                    repo
                }
                None => match self.objects(repository_id, false)? {
                    Some(repo) if repo.check_reachable(root).is_ok() => repo,
                    _ => return Ok(false),
                },
            };
            objects = Some(repo);
        }
        self.store
            .publish_received_all(&st.leaf_first(root_id), objects.as_ref())
            .map_err(|e| invalid(format!("the source is refused: {e:#}")))?;
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
        match r.kind {
            SourceBlobKind::Manifest => {
                let done = dir.join(staged_name(r.kind, hex, true));
                if staged_len(&done)?.is_none() {
                    if !dir.is_dir() {
                        self.admit(limits, &r.source_id)?;
                        private_dir(&dir).map_err(io_err)?;
                    }
                    let part = match self.append(limits, &dir, r) {
                        Ok(Progress::At(n)) => return Ok(Some(n)),
                        Ok(Progress::Whole(part)) => part,
                        Err(e) => {
                            let _ = fs::remove_dir(&dir);
                            return Err(e);
                        }
                    };
                    let staged = self.stage_manifest(limits, root_id, &part, &done, st);
                    if staged.is_err() {
                        // Nothing of a source that never had a manifest stays.
                        let _ = fs::remove_dir(&dir);
                    }
                    staged?;
                }
            }
            SourceBlobKind::Content => {
                if !has_root {
                    return Err(first());
                }
                check_content(st.wanted.get(hex).copied(), r.total_size).map_err(invalid)?;
                let part = dir.join(staged_name(r.kind, hex, false));
                if self.store.has_blob(hex) {
                    remove_staged(&part);
                } else {
                    match self.append(limits, &dir, r)? {
                        Progress::At(n) => return Ok(Some(n)),
                        Progress::Whole(part) => {
                            let want = BlobWant {
                                hash: hex.to_string(),
                                size: r.total_size,
                            };
                            let taken = File::open(&part)
                                .map_err(anyhow::Error::from)
                                .and_then(|mut f| self.store.ingest_blob(&want, &mut f));
                            remove_staged(&part);
                            taken.map_err(|e| internal("source store", format!("{e:#}")))?;
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
                let done = dir.join(staged_name(r.kind, hex, true));
                if staged_len(&done)?.is_none() {
                    match self.append(limits, &dir, r)? {
                        Progress::At(n) => return Ok(Some(n)),
                        Progress::Whole(part) => {
                            // One bundle per source: a newer one replaces ours.
                            if let Some(old) = Self::bundle_done(&dir)? {
                                remove_staged(&old);
                            }
                            fs::rename(&part, &done).map_err(io_err)?;
                        }
                    }
                }
            }
        }
        self.complete(&r.source_id, root_id, st)?;
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
        // Do the commits a bundle would bring need no bundle any more?
        let commits_here = || -> Result<bool, ApiError> {
            let Some(root) = st.manifests.get(root_id) else {
                return Ok(false);
            };
            let (Some(_), Some(repository_id)) = (&root.execution_commit, &root.repository_id)
            else {
                return Ok(root.execution_commit.is_none());
            };
            Ok(self
                .objects(repository_id, false)?
                .is_some_and(|repo| repo.check_reachable(root).is_ok()))
        };
        let mut out = BlobVerifyResponse {
            schema_version: SchemaVersion::CURRENT,
            ready,
            present: vec![],
            missing: vec![],
            corrupt: vec![],
        };
        for b in &r.blobs {
            let hex = hex_of(&b.digest);
            let partial = staged_len(&dir.join(staged_name(b.kind, hex, false)))?;
            let done = || staged_len(&dir.join(staged_name(b.kind, hex, true)));
            let verdict = match b.kind {
                SourceBlobKind::Content => {
                    let stored = fs::metadata(self.store.object_store().object_path(hex))
                        .ok()
                        .filter(|m| m.is_file())
                        .map(|m| m.len());
                    content_verdict(stored, partial, b.total_size)
                }
                SourceBlobKind::Manifest => {
                    staged_verdict(done()?.is_some(), ready, partial, b.total_size)
                }
                SourceBlobKind::Bundle => {
                    let done = done()?.is_some();
                    let settled = ready || (!done && commits_here()?);
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
/// without following a link anywhere on the way.
fn open_artifact(dir: &Path, a: &ArtifactRecord) -> Result<File, ApiError> {
    let gone = || {
        ApiError::new(
            ErrorCode::NotFound,
            format!("artifact {} is not on this worker any more", a.digest),
        )
    };
    let rel = relative_path(&a.path).ok_or_else(|| internal("stored artifact", &a.path))?;
    let real_dir = |p: &Path| match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(internal("artifact store", "a directory is not one")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(gone()),
        Err(e) => Err(io_err(e)),
    };
    let mut cur = dir.to_path_buf();
    let comps: Vec<_> = rel.iter().collect();
    let (last, dirs) = comps.split_last().expect("a relative path has a component");
    real_dir(&cur)?;
    for c in dirs {
        cur.push(c);
        real_dir(&cur)?;
    }
    cur.push(last);
    match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&cur)
    {
        Ok(f) => Ok(f),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(gone()),
        Err(e) => Err(io_err(e)),
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
