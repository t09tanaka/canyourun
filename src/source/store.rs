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
    import_objects, materialize_from, seal_manifest, ObjectStore, SourceError, SourceKind,
    SourceManifest as CiManifest, SCHEMA_VERSION,
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

/// Where the commit of a source is kept (local-ci's names).
const SNAPSHOTS: &str = "refs/local-ci/snapshots/";
const INDEXES: &str = "refs/local-ci/index/";

fn is_hex40(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// The Git repository on the receiving side that holds the objects of the
/// sources it received: the worker's canonical clone of the repository.
/// Nothing is fetched from the network: a bundle brings the history the
/// clone lacks, and what it is built on has to be in the clone already (the
/// first one is built on nothing).
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
    pub fn check_reachable(&self, m: &CiManifest) -> Result<()> {
        let Some(commit) = &m.execution_commit else {
            return Ok(());
        };
        self.check_snapshot(&m.source_id, commit, m.worktree_tree.as_deref())
    }

    /// [`Self::check_reachable`] from the three things it reads of a manifest.
    pub fn check_snapshot(
        &self,
        ci_source_id: &str,
        commit: &str,
        tree: Option<&str>,
    ) -> Result<()> {
        if !is_hex40(commit) {
            return Err(corrupt(format!("not a commit id: {commit:?}")));
        }
        if source_id_of_ci(ci_source_id).is_none() {
            return Err(corrupt(format!("not a source id: {ci_source_id:?}")));
        }
        let r = format!("{SNAPSHOTS}{ci_source_id}");
        git_run(
            &self.env,
            &self.dir,
            &["merge-base", "--is-ancestor", commit, &r],
        )
        .map_err(|e| {
            corrupt(format!(
                "{ci_source_id}: the execution commit {commit} is not reachable in the receiving objects ({e:#})"
            ))
        })?;
        if let Some(tree) = tree {
            let got = git_run(
                &self.env,
                &self.dir,
                &["rev-parse", &format!("{commit}^{{tree}}")],
            )?;
            if got != tree {
                return Err(corrupt(format!(
                    "commit {commit} has tree {got}, the manifest says {tree}"
                )));
            }
        }
        Ok(())
    }

    /// The snapshots this repository holds: the source id of each snapshot
    /// ref, its tip, and the committer time of the tip's first parent (the
    /// commit the capture was made on; 0 when there is none). At most
    /// `limit` refs are read. The tip itself says nothing about when: a
    /// snapshot commit has a fixed date.
    pub fn snapshot_tips(&self, limit: usize) -> Result<Vec<(String, String, u64)>> {
        let out = git_run(
            &self.env,
            &self.dir,
            &[
                "for-each-ref",
                &format!("--count={limit}"),
                "--format=%(refname) %(objectname) %(parent)",
                SNAPSHOTS,
            ],
        )?;
        let mut tips = vec![];
        for line in out.lines() {
            let mut words = line.split(' ');
            let (Some(name), Some(tip)) = (words.next(), words.next()) else {
                continue;
            };
            let Some(id) = name.strip_prefix(SNAPSHOTS) else {
                continue;
            };
            if !is_hex40(tip) {
                continue;
            }
            let parent = words.next().filter(|p| is_hex40(p)).map(str::to_string);
            tips.push((id.to_string(), tip.to_string(), parent));
        }
        let parents: BTreeSet<&str> = tips.iter().filter_map(|t| t.2.as_deref()).collect();
        let mut when: BTreeMap<String, u64> = BTreeMap::new();
        if !parents.is_empty() {
            let mut args = vec!["log", "--no-walk=unsorted", "--format=%H %ct"];
            args.extend(parents.iter().copied());
            for line in git_run(&self.env, &self.dir, &args)?.lines() {
                if let Some((sha, ct)) = line.split_once(' ') {
                    when.insert(sha.to_string(), ct.trim().parse().unwrap_or(0));
                }
            }
        }
        Ok(tips
            .into_iter()
            .map(|(id, tip, parent)| {
                let at = parent.and_then(|p| when.get(&p).copied()).unwrap_or(0);
                (id, tip, at)
            })
            .collect())
    }

    /// Does a bundle with these prerequisites fit this repository: is every
    /// one of them here, with its whole history, below a ref? (What `git
    /// bundle verify` asks, apart from the bundle itself.)
    pub fn holds_base(&self, prerequisites: &[String]) -> bool {
        if prerequisites.is_empty() {
            return true;
        }
        if prerequisites.iter().any(|p| !is_hex40(p)) {
            return false;
        }
        let mut args = vec!["rev-list", "-n", "1"];
        args.extend(prerequisites.iter().map(String::as_str));
        args.extend(["--not", "--all"]);
        matches!(git_run(&self.env, &self.dir, &args), Ok(out) if out.is_empty())
    }
}

/// Commits of a peer an export looks at, at most.
const MAX_BASIS: usize = 256;

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

    /// The bytes the store holds under the hash, when it holds the blob.
    pub fn blob_size(&self, hash: &str) -> Option<u64> {
        if !is_blob_hash(hash) {
            return None;
        }
        std::fs::metadata(self.inner.object_path(hash))
            .ok()
            .filter(|m| m.is_file())
            .map(|m| m.len())
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

    /// One step of [`Self::publish_received_all`], for a caller that holds
    /// one manifest at a time: `carried` says whether the bundle carries the
    /// commit of `m` (the requested source, or a dirty submodule's capture).
    pub fn publish_received_one(
        &self,
        m: &CiManifest,
        carried: bool,
        objects: Option<&ObjectsRepo>,
    ) -> Result<()> {
        if carried {
            self.publish_received(m, objects)
        } else {
            self.publish_checked(m)
        }
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

    /// The private repository of the store that holds the snapshot ref of
    /// the source `ci_source_id`.
    fn holder(&self, env: &BTreeMap<String, String>, ci_source_id: &str) -> Result<PathBuf> {
        let r = format!("{SNAPSHOTS}{ci_source_id}");
        for ent in std::fs::read_dir(self.inner.root().join("git"))? {
            let dir = ent?.path();
            if git_run(env, &dir, &["rev-parse", "--verify", "-q", &r]).is_ok() {
                return Ok(dir);
            }
        }
        bail!("the Git objects of {ci_source_id} are not in this store")
    }

    /// The captures of the dirty submodules below `m`, nested ones included.
    fn dirty_submodules(&self, m: &CiManifest, out: &mut Vec<String>) -> Result<()> {
        for s in &m.submodules {
            let Some(id) = &s.source_id else { continue };
            let nested = self.inner.load_manifest(id)?.with_context(|| {
                format!(
                    "submodule {}: manifest {id} is not in the store",
                    s.path.display()
                )
            })?;
            verify_manifest(&nested)?;
            if s.dirty && !out.contains(id) {
                out.push(id.clone());
            }
            self.dirty_submodules(&nested, out)?;
        }
        Ok(())
    }

    /// The `git bundle` that carries the Git objects of `m` to a peer that
    /// holds the commits `have` with their whole history (what a worker said
    /// of its clone; see `blob_verify`): the execution commit, the commit
    /// around the index tree, the commits of dirty submodules, and all the
    /// history below them that is not below a commit of `have`. With an empty
    /// `have` that is the whole history: the peer never fetches from origin,
    /// so nothing is left for it to find elsewhere. A commit of `have` this
    /// store does not know is ignored.
    ///
    /// The bundle always names every ref the peer will ask it for. Git leaves
    /// a ref out when its commit is below an excluded one (a dirty submodule
    /// that did not change keeps its capture, so the peer holds its commit
    /// already): then the parents of that commit are excluded instead.
    pub fn export_bundle(
        &self,
        ctx: &ExecContext,
        m: &CiManifest,
        have: &[String],
    ) -> Result<PathBuf> {
        let env = ctx.env();
        verify_manifest(m)?;
        if m.execution_commit.is_none() {
            bail!("{} has no Git objects ({:?})", m.source_id, m.source_kind);
        }
        let dir = self.holder(env, &m.source_id)?;
        let mut refs = vec![format!("{SNAPSHOTS}{}", m.source_id)];
        if m.index_tree.is_some() {
            // made by the capture, next to the snapshot ref
            refs.push(format!("{INDEXES}{}", m.source_id));
        }
        let mut subs = vec![];
        self.dirty_submodules(m, &mut subs)?;
        for id in &subs {
            let r = format!("{SNAPSHOTS}{id}");
            let from = self.holder(env, id)?;
            if from != dir {
                let from = from.to_string_lossy();
                let spec = format!("+{r}:{r}");
                git_run(
                    env,
                    &dir,
                    &[
                        "fetch",
                        "-q",
                        "--no-tags",
                        "--no-write-fetch-head",
                        &from,
                        &spec,
                    ],
                )
                .with_context(|| format!("cannot bring the objects of {id} along"))?;
            }
            refs.push(r);
        }
        let mut tips = vec![];
        for r in &refs {
            let tip = git_run(
                env,
                &dir,
                &["rev-parse", "--verify", "-q", &format!("{r}^{{commit}}")],
            )
            .with_context(|| format!("{r} is not in this store"))?;
            tips.push(tip);
        }
        let known = |c: &str| {
            is_hex40(c)
                && git_run(env, &dir, &["cat-file", "-e", &format!("{c}^{{commit}}")]).is_ok()
        };
        let mut not: BTreeSet<String> = have
            .iter()
            .take(MAX_BASIS)
            .filter(|c| known(c))
            .cloned()
            .collect();
        // Would the bundle leave `tip` out, with these exclusions?
        let left_out = |tip: &str, not: &BTreeSet<String>| -> Result<bool> {
            if not.is_empty() {
                return Ok(false);
            }
            let mut args = vec!["rev-list".to_string(), "-n".into(), "1".into(), tip.into()];
            args.extend(not.iter().map(|c| format!("^{c}")));
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            Ok(git_run(env, &dir, &args)?.is_empty())
        };
        let below = |tip: &str, c: &str| {
            git_run(env, &dir, &["merge-base", "--is-ancestor", tip, c]).is_ok()
        };
        // first the parents stand in for an exclusion that covers a tip; the
        // second round only drops what still covers one
        for stand_in in [true, false] {
            let mut parents = vec![];
            for tip in &tips {
                if !left_out(tip, &not)? {
                    continue;
                }
                not.retain(|c| !below(tip, c));
                if stand_in {
                    let ps = git_run(env, &dir, &["rev-parse", &format!("{tip}^@")])?;
                    parents.extend(ps.lines().filter(|p| is_hex40(p)).map(str::to_string));
                }
            }
            not.extend(parents);
        }
        let out_dir = self.inner.root().join("bundles");
        std::fs::create_dir_all(&out_dir)?;
        let mut h = Sha256::new();
        for c in &not {
            h.update(c.as_bytes());
        }
        let basis: String = h
            .finalize()
            .iter()
            .take(8)
            .map(|b| format!("{b:02x}"))
            .collect();
        let dest = out_dir.join(format!("{}.{basis}.bundle", m.source_id));
        let tmp = tempfile::Builder::new()
            .prefix(".bundle.")
            .tempfile_in(&out_dir)?
            .into_temp_path();
        // git writes the bundle through a lock file of its own beside it
        std::fs::remove_file(&tmp)?;
        let tmp_str = tmp.to_string_lossy().into_owned();
        let mut args: Vec<String> = vec!["bundle".into(), "create".into(), "-q".into(), tmp_str];
        args.extend(refs);
        args.extend(not.iter().map(|c| format!("^{c}")));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        git_run(env, &dir, &args)
            .with_context(|| format!("cannot bundle the commits of {}", m.source_id))?;
        tmp.persist(&dest).map_err(|e| e.error)?;
        Ok(dest)
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

    /// Where local-ci puts the plaintext secret bundle of a capture of this
    /// source (0600), until the parent's vault has taken it.
    pub fn secrets_path(&self, ci_source_id: &str) -> PathBuf {
        self.inner.secrets_path(ci_source_id)
    }

    /// Hold the store's plaintext secret bundles for the life of the returned
    /// file: they are named by source, so two captures of the same source
    /// (two runs) would otherwise write, adopt and remove the same file. The
    /// source id is not known before the capture, so this is one lock for all
    /// captures that carry secret files.
    pub fn lock_secrets(&self) -> Result<std::fs::File> {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.inner.root().join("secrets.lock"))?;
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
pub fn verify_manifest(m: &CiManifest) -> Result<()> {
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
