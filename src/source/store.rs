//! The runner's private store of captured sources: local-ci's
//! [`ObjectStore`] under the state directory, plus what the runner adds to it
//! (the per-run record, difference planning, and verified receiving).

use super::classify::source_id_of_ci;
use super::commit::run as git_run;
use super::diff::{check_blob, is_blob_hash, missing_blobs, BlobWant};
use super::safety::{check_entries, check_rel_path};
use crate::ids::RunId;
use anyhow::{bail, Context, Result};
use local_ci::source::{
    export_from, import_objects, materialize_from, seal_manifest, ObjectStore, SourceError,
    SourceKind, SourceManifest as CiManifest, SCHEMA_VERSION,
};
use local_ci::ExecContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// What a peer's store holds: the question a transfer plan asks of it.
pub trait Inventory {
    fn has_blob(&self, hash: &str) -> bool;
    fn has_manifest(&self, ci_source_id: &str) -> bool;
    /// Is the commit among the peer's Git objects (the repository the bundle
    /// is unpacked into)? A manifest says nothing about this.
    fn has_commit(&self, sha: &str) -> bool;
}

/// What to send so that a peer can hold `manifest` complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferPlan {
    /// The sources (the requested one and the submodule captures below it)
    /// the peer lacks, leaf first: a manifest is published after the ones it
    /// names.
    pub manifests: Vec<String>,
    /// Blobs the peer lacks, over all those sources, each once.
    pub blobs: Vec<BlobWant>,
    /// The requested source has an execution commit and the peer's Git
    /// objects do not hold it (a bundle goes with it). Decided on the
    /// objects, not on whether the manifest is there: a manifest can arrive
    /// without its commits.
    pub bundle: bool,
}

#[derive(Debug, Clone)]
pub struct SourceStore {
    inner: ObjectStore,
}

/// A peer as a transfer sees it: its source store, and the Git repository the
/// bundles are unpacked into (the worker's clone of the repository; none when
/// the peer has none yet).
pub struct Peer<'a> {
    pub store: &'a SourceStore,
    pub objects: Option<&'a ObjectsRepo>,
}

impl Inventory for Peer<'_> {
    fn has_blob(&self, hash: &str) -> bool {
        self.store.has_blob(hash)
    }
    fn has_manifest(&self, ci_source_id: &str) -> bool {
        matches!(self.store.inner.load_manifest(ci_source_id), Ok(Some(_)))
    }
    fn has_commit(&self, sha: &str) -> bool {
        self.objects.is_some_and(|o| o.has_commit(sha))
    }
}

fn is_hex40(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// The Git repository on the receiving side that holds the objects of the
/// sources it received: the worker's canonical clone of the repository. It has
/// to have the merge base already (a bundle never carries it; nothing is
/// fetched from the network).
#[derive(Debug, Clone)]
pub struct ObjectsRepo {
    env: BTreeMap<String, String>,
    dir: PathBuf,
}

impl ObjectsRepo {
    pub fn new(env: BTreeMap<String, String>, dir: PathBuf) -> Self {
        ObjectsRepo { env, dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn has_commit(&self, sha: &str) -> bool {
        is_hex40(sha)
            && git_run(
                &self.env,
                &self.dir,
                &["cat-file", "-e", &format!("{sha}^{{commit}}")],
            )
            .is_ok()
    }

    /// Unpack the bundle of `m` into the repository, and check that the
    /// execution commit is there with the recorded tree (local-ci's
    /// `import_objects`; the manifest is verified first).
    pub fn receive_bundle(&self, bundle: &Path, m: &CiManifest) -> Result<()> {
        verify_manifest(m)?;
        import_objects(&ExecContext::new(self.env.clone()), &self.dir, bundle, m)?;
        Ok(())
    }

    /// The execution commit of `m` is reachable from the snapshot ref the
    /// bundle was unpacked under, and has the recorded tree.
    fn check_reachable(&self, m: &CiManifest) -> Result<()> {
        let Some(commit) = &m.execution_commit else {
            return Ok(());
        };
        if !is_hex40(commit) {
            return Err(corrupt(format!("not a commit id: {commit:?}")));
        }
        let r = format!("refs/local-ci/snapshots/{}", m.source_id);
        git_run(
            &self.env,
            &self.dir,
            &["merge-base", "--is-ancestor", commit, &r],
        )
        .map_err(|e| {
            corrupt(format!(
                "{}: the execution commit {commit} is not reachable in the receiving objects ({e:#})",
                m.source_id
            ))
        })?;
        if let Some(tree) = &m.worktree_tree {
            let got = git_run(
                &self.env,
                &self.dir,
                &["rev-parse", &format!("{commit}^{{tree}}")],
            )?;
            if &got != tree {
                return Err(corrupt(format!(
                    "commit {commit} has tree {got}, the manifest says {tree}"
                )));
            }
        }
        Ok(())
    }
}

fn corrupt(what: impl Into<String>) -> anyhow::Error {
    SourceError::Corrupt { what: what.into() }.into()
}

impl SourceStore {
    /// `<state_dir>/sources`, private to this user.
    pub fn open(state_dir: &Path) -> Result<Self> {
        let root = state_dir.join("sources");
        let inner = ObjectStore::open(&root)?;
        std::fs::create_dir_all(root.join("runs"))?;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        Ok(SourceStore { inner })
    }

    /// Is the blob in the store? (Only a well-formed hash can be.)
    pub fn has_blob(&self, hash: &str) -> bool {
        is_blob_hash(hash) && self.inner.has(hash)
    }

    pub fn object_store(&self) -> &ObjectStore {
        &self.inner
    }

    pub fn root(&self) -> &Path {
        self.inner.root()
    }

    /// A published manifest. `None` for a capture that never got to publish.
    pub fn manifest(&self, ci_source_id: &str) -> Result<Option<CiManifest>> {
        self.inner.load_manifest(ci_source_id)
    }

    /// The sources `m` is made of, leaf first: the captures of its
    /// submodules (nested ones included), then `m`. Each once.
    pub fn closure(&self, m: &CiManifest) -> Result<Vec<CiManifest>> {
        fn walk(
            s: &SourceStore,
            m: &CiManifest,
            seen: &mut BTreeSet<String>,
            out: &mut Vec<CiManifest>,
        ) -> Result<()> {
            if !seen.insert(m.source_id.clone()) {
                return Ok(());
            }
            for sub in &m.submodules {
                if let Some(id) = &sub.source_id {
                    let nested = s.manifest(id)?.ok_or_else(|| {
                        corrupt(format!(
                            "submodule {}: manifest {id} is not in the store",
                            sub.path.display()
                        ))
                    })?;
                    walk(s, &nested, seen, out)?;
                }
            }
            out.push(m.clone());
            Ok(())
        }
        let mut out = vec![];
        walk(self, m, &mut BTreeSet::new(), &mut out)?;
        Ok(out)
    }

    /// What `peer` lacks to hold `ci_source_id` complete. Only a difference is
    /// listed: a peer that has everything gets an empty plan.
    pub fn plan_transfer(&self, ci_source_id: &str, peer: &dyn Inventory) -> Result<TransferPlan> {
        let m = self
            .manifest(ci_source_id)?
            .with_context(|| format!("{ci_source_id} is not in the store"))?;
        let sources = self.closure(&m)?;
        let mut manifests = vec![];
        let mut entries = vec![];
        for s in &sources {
            if !peer.has_manifest(&s.source_id) {
                manifests.push(s.source_id.clone());
            }
            entries.extend(s.object_manifest.entries.iter().cloned());
        }
        Ok(TransferPlan {
            bundle: m
                .execution_commit
                .as_deref()
                .is_some_and(|c| !peer.has_commit(c)),
            blobs: missing_blobs(&entries, |h| peer.has_blob(h)),
            manifests,
        })
    }

    /// The bytes of a blob, to send.
    pub fn read_blob(&self, hash: &str) -> Result<Vec<u8>> {
        self.inner.read(hash)
    }

    /// Take in a blob that came from outside. It is stored only when its hash
    /// and size are what was asked for; the hash is computed here, not
    /// believed, and no more than the announced size is read.
    ///
    /// This is where a blob is checked: a blob in the store is under the hash
    /// of its own content, so `publish_received` only has to see that the
    /// blobs a manifest names are there with the size it says (the same hash
    /// cannot hold other content).
    pub fn ingest_blob(&self, want: &BlobWant, r: &mut impl Read) -> Result<()> {
        if !is_blob_hash(&want.hash) {
            return Err(corrupt(format!("not a blob hash: {:?}", want.hash)));
        }
        // hash first, in a scratch copy: a wrong blob must not enter the store
        let mut tmp = tempfile::Builder::new()
            .prefix(".recv.")
            .tempfile_in(self.inner.root().join("tmp"))?;
        let mut r = r.take(want.size.saturating_add(1));
        let mut h = Sha256::new();
        let mut n = 0u64;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let k = r.read(&mut buf)?;
            if k == 0 {
                break;
            }
            n += k as u64;
            if n > want.size {
                return Err(corrupt(format!(
                    "blob {} is longer than the announced {} bytes",
                    want.hash, want.size
                )));
            }
            h.update(&buf[..k]);
            tmp.write_all(&buf[..k])?;
        }
        tmp.as_file().sync_all()?;
        let actual: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        check_blob(want, &actual, n)?;
        let (stored, _) = self
            .inner
            .put_reader(&mut std::fs::File::open(tmp.path())?)?;
        if stored != want.hash {
            bail!("the store named {stored}, not {}", want.hash);
        }
        Ok(())
    }

    /// Make a manifest that came from outside visible, after checking it: the
    /// seal, every path, hash and mode, and that everything it names (blobs of
    /// the right size, the submodule captures) is already in the store, and,
    /// for a source with an execution commit, that the commit is reachable in
    /// `objects` (the bundle has been unpacked; see
    /// [`ObjectsRepo::receive_bundle`]). The manifest file is written last, by
    /// rename, so a source that can be loaded is complete; a transfer that
    /// stops earlier leaves nothing visible and can be resumed with the same
    /// plan.
    pub fn publish_received(&self, m: &CiManifest, objects: Option<&ObjectsRepo>) -> Result<()> {
        verify_manifest(m)?;
        if m.execution_commit.is_some() {
            match objects {
                Some(o) => o.check_reachable(m)?,
                None => {
                    return Err(corrupt(format!(
                        "{}: no receiving objects to hold its execution commit",
                        m.source_id
                    )))
                }
            }
        }
        self.publish_checked(m)
    }

    /// `publish_received` for the sources of a transfer, in plan order (leaf
    /// first, the requested source last). The objects are required for the
    /// requested source and for the dirty submodules below it: the bundle
    /// carries their commits. A clean submodule's capture has an execution
    /// commit too, but the bundle does not carry it (its files are in the
    /// blobs and its revision is in the parent's tree), so it is not asked
    /// for.
    pub fn publish_received_all(
        &self,
        manifests: &[CiManifest],
        objects: Option<&ObjectsRepo>,
    ) -> Result<()> {
        let mut carried: BTreeSet<&str> = BTreeSet::new();
        if let Some(last) = manifests.last() {
            carried.insert(&last.source_id);
        }
        for m in manifests {
            for sub in m.submodules.iter().filter(|s| s.dirty) {
                if let Some(id) = &sub.source_id {
                    carried.insert(id);
                }
            }
        }
        for m in manifests {
            if carried.contains(m.source_id.as_str()) {
                self.publish_received(m, objects)?;
            } else {
                self.publish_checked(m)?;
            }
        }
        Ok(())
    }

    fn publish_checked(&self, m: &CiManifest) -> Result<()> {
        verify_manifest(m)?;
        for e in &m.object_manifest.entries {
            let p = self.inner.object_path(&e.hash);
            match std::fs::metadata(&p) {
                Ok(meta) if meta.len() == e.size => {}
                Ok(meta) => {
                    return Err(corrupt(format!(
                        "blob {} has {} bytes, the manifest says {}",
                        e.hash,
                        meta.len(),
                        e.size
                    )))
                }
                Err(_) => {
                    return Err(corrupt(format!(
                        "blob {} ({}) is not in the store",
                        e.hash,
                        e.path.display()
                    )))
                }
            }
        }
        for s in &m.submodules {
            if let Some(id) = &s.source_id {
                if !self.inner_has_manifest(id) {
                    return Err(corrupt(format!(
                        "submodule {}: {id} is not in the store",
                        s.path.display()
                    )));
                }
            }
        }
        let bytes = serde_json::to_vec_pretty(m)?;
        let mut tmp = tempfile::Builder::new()
            .prefix(".manifest.")
            .tempfile_in(self.inner.root().join("tmp"))?;
        tmp.write_all(&bytes)?;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o644))?;
        tmp.as_file().sync_all()?;
        tmp.persist(self.inner.manifest_path(&m.source_id))
            .map_err(|e| e.error)?;
        Ok(())
    }

    fn inner_has_manifest(&self, ci_source_id: &str) -> bool {
        matches!(self.inner.load_manifest(ci_source_id), Ok(Some(_)))
    }

    /// Unpack `m` into `dest` (absent or empty). The paths of every source it
    /// is made of are decided on first; then local-ci writes the files, never
    /// through a symlink, and checks the result against the tree digest.
    pub fn materialize(&self, m: &CiManifest, dest: &Path) -> Result<()> {
        for s in self.closure(m)? {
            verify_manifest(&s)?;
        }
        materialize_from(&self.inner, m, dest)?;
        // where the run starts has to be a real directory of the unpacked tree
        let root = std::fs::canonicalize(dest)?;
        let cwd = std::fs::canonicalize(dest.join(m.cwd_relative.to_path())).map_err(|_| {
            corrupt(format!(
                "the working directory {:?} is not in the unpacked source",
                m.cwd_relative.display()
            ))
        })?;
        if !cwd.starts_with(&root) || !cwd.is_dir() {
            return Err(corrupt(format!(
                "the working directory {:?} is not a directory inside the unpacked source",
                m.cwd_relative.display()
            )));
        }
        Ok(())
    }

    /// The `git bundle` that carries the Git objects of `m` (the synthetic
    /// commit, the unpushed history below it down to the merge base, and
    /// dirty submodules' commits). A worker never fetches these from origin.
    pub fn export_bundle(&self, ctx: &ExecContext, m: &CiManifest) -> Result<PathBuf> {
        export_from(ctx, &self.inner, m)
    }

    fn run_record_path(&self, run: &RunId) -> PathBuf {
        self.inner.root().join("runs").join(run.as_str())
    }

    /// Hold the capture of `run` for the life of the returned file: a second
    /// caller (another thread or process) waits, then finds the record the
    /// first one made.
    pub fn lock_run(&self, run: &RunId) -> Result<std::fs::File> {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.inner.root().join("runs").join(format!("{run}.lock")))?;
        f.lock()?;
        Ok(f)
    }

    /// What a run captured, if anything (spec: one capture per run).
    pub fn run_record(&self, run: &RunId) -> Result<Option<RunRecord>> {
        match std::fs::read(self.run_record_path(run)) {
            Ok(b) => Ok(Some(
                serde_json::from_slice(&b).with_context(|| format!("the record of run {run}"))?,
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The source a run already captured, if any.
    pub fn source_of_run(&self, run: &RunId) -> Result<Option<String>> {
        Ok(self.run_record(run)?.map(|r| r.ci_source_id))
    }

    /// Record the capture of `run`, once. Returns the record the run is bound
    /// to: `record`, or the one an earlier call wrote.
    pub fn bind_run(&self, run: &RunId, record: &RunRecord) -> Result<RunRecord> {
        if source_id_of_ci(&record.ci_source_id).is_none() {
            bail!("not a source id: {}", record.ci_source_id);
        }
        let dest = self.run_record_path(run);
        let mut tmp = tempfile::Builder::new()
            .prefix(".run.")
            .tempfile_in(self.inner.root().join("tmp"))?;
        tmp.write_all(&serde_json::to_vec_pretty(record)?)?;
        tmp.as_file().sync_all()?;
        // a hard link fails when the record exists: the first writer wins
        match std::fs::hard_link(tmp.path(), &dest) {
            Ok(()) => Ok(record.clone()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                self.run_record(run)?.context("the run record disappeared")
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// What a run's capture was, written next to the source it produced. A later
/// call for the same run must ask for the same thing, or it is an error and
/// not a silent reuse of another source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub ci_source_id: String,
    pub source_kind: SourceKind,
    pub request: RequestSummary,
}

/// The parts of a capture request that decide what is captured. The
/// directory is left out: the same repository can be named by different
/// paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestSummary {
    pub kind: SourceKind,
    /// The commit asked for (`Commit` only).
    pub rev: Option<String>,
    pub base: Option<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

/// A manifest from outside is not taken for what it says: its schema, its
/// seal, and every path and mode are checked.
fn verify_manifest(m: &CiManifest) -> Result<()> {
    if m.schema_version != SCHEMA_VERSION {
        return Err(corrupt(format!(
            "manifest schema version {} is not supported (this is {SCHEMA_VERSION})",
            m.schema_version
        )));
    }
    if seal_manifest(m.clone()) != *m {
        return Err(corrupt(format!(
            "manifest {} does not match its digest",
            m.source_id
        )));
    }
    check_entries(
        m.object_manifest
            .entries
            .iter()
            .map(|e| (e.path.0.as_slice(), e.mode)),
    )
    .map_err(|v| SourceError::UnsafePath {
        path: String::from_utf8_lossy(&v.path).into_owned(),
        why: format!("{:?}", v.why),
    })?;
    // where the run starts: empty (the root) or a safe path
    if !m.cwd_relative.is_empty() {
        if let Err(why) = check_rel_path(&m.cwd_relative.0) {
            return Err(SourceError::UnsafePath {
                path: m.cwd_relative.display(),
                why: format!("{why:?}"),
            }
            .into());
        }
    }
    // a hash ends up in a path of the store
    for e in &m.object_manifest.entries {
        if !is_blob_hash(&e.hash) {
            return Err(corrupt(format!(
                "{}: {:?} is not a blob hash",
                e.path.display(),
                e.hash
            )));
        }
    }
    let unsafe_path = |p: &[u8]| check_rel_path(p).err();
    for p in m
        .submodules
        .iter()
        .map(|s| &s.path)
        .chain(m.lfs.iter().map(|l| &l.path))
        .chain(m.secret_files.iter().map(|s| &s.path))
    {
        if let Some(why) = unsafe_path(&p.0) {
            return Err(SourceError::UnsafePath {
                path: p.display(),
                why: format!("{why:?}"),
            }
            .into());
        }
    }
    Ok(())
}
