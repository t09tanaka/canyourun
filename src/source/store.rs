//! The runner's private store of captured sources: local-ci's
//! [`ObjectStore`] under the state directory, plus what the runner adds to it
//! (the per-run record, difference planning, and verified receiving).

use super::classify::source_id_of_ci;
use super::diff::{check_blob, missing_blobs, BlobWant};
use super::safety::{check_entries, check_rel_path};
use crate::ids::RunId;
use anyhow::{bail, Context, Result};
use local_ci::source::{
    export_from, materialize_from, seal_manifest, ObjectStore, SourceError,
    SourceManifest as CiManifest, SCHEMA_VERSION,
};
use local_ci::ExecContext;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// What a peer's store holds: the question a transfer plan asks of it.
pub trait Inventory {
    fn has_blob(&self, hash: &str) -> bool;
    fn has_manifest(&self, ci_source_id: &str) -> bool;
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
    /// The requested source has Git objects (a bundle goes with it) and the
    /// peer does not have the source yet.
    pub bundle: bool,
}

#[derive(Debug, Clone)]
pub struct SourceStore {
    inner: ObjectStore,
}

impl Inventory for SourceStore {
    fn has_blob(&self, hash: &str) -> bool {
        self.inner.has(hash)
    }
    fn has_manifest(&self, ci_source_id: &str) -> bool {
        matches!(self.inner.load_manifest(ci_source_id), Ok(Some(_)))
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
            bundle: m.execution_commit.is_some() && manifests.contains(&m.source_id),
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
    /// believed.
    pub fn ingest_blob(&self, want: &BlobWant, r: &mut impl Read) -> Result<()> {
        // hash first, in a scratch copy: a wrong blob must not enter the store
        let tmp = self.inner.root().join("tmp").join(format!(
            ".recv.{}.{}",
            std::process::id(),
            &want.hash[..want.hash.len().min(16)]
        ));
        let res = (|| -> Result<()> {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?;
            let mut h = Sha256::new();
            let mut n = 0u64;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let k = r.read(&mut buf)?;
                if k == 0 {
                    break;
                }
                h.update(&buf[..k]);
                f.write_all(&buf[..k])?;
                n += k as u64;
            }
            f.sync_all()?;
            let actual: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
            check_blob(want, &actual, n)?;
            let (stored, _) = self.inner.put_reader(&mut std::fs::File::open(&tmp)?)?;
            if stored != want.hash {
                bail!("the store named {stored}, not {}", want.hash);
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(&tmp);
        res
    }

    /// Make a manifest that came from outside visible, after checking it: the
    /// seal, every path and mode, and that everything it names (blobs of the
    /// right size, the submodule captures) is already in the store. The
    /// manifest file is written last, by rename, so a source that can be
    /// loaded is complete; a transfer that stops earlier leaves nothing
    /// visible and can be resumed with the same plan.
    pub fn publish_received(&self, m: &CiManifest) -> Result<()> {
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
                if !self.has_manifest(id) {
                    return Err(corrupt(format!(
                        "submodule {}: {id} is not in the store",
                        s.path.display()
                    )));
                }
            }
        }
        let bytes = serde_json::to_vec_pretty(m)?;
        let tmp = self.inner.root().join("tmp").join(format!(
            ".manifest.{}.{}",
            std::process::id(),
            m.source_id
        ));
        let res = (|| -> Result<()> {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            std::fs::rename(&tmp, self.inner.manifest_path(&m.source_id))?;
            Ok(())
        })();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res
    }

    /// Unpack `m` into `dest` (absent or empty). The paths of every source it
    /// is made of are decided on first; then local-ci writes the files, never
    /// through a symlink, and checks the result against the tree digest.
    pub fn materialize(&self, m: &CiManifest, dest: &Path) -> Result<()> {
        for s in self.closure(m)? {
            verify_manifest(&s)?;
        }
        materialize_from(&self.inner, m, dest)
    }

    /// The `git bundle` that carries the Git objects of `m` (the synthetic
    /// commit, the unpushed history below it down to the merge base, and
    /// dirty submodules' commits). A worker never fetches these from origin.
    pub fn export_bundle(&self, ctx: &ExecContext, m: &CiManifest) -> Result<PathBuf> {
        export_from(ctx, &self.inner, m)
    }

    fn run_record(&self, run: &RunId) -> PathBuf {
        self.inner.root().join("runs").join(run.as_str())
    }

    /// The source a run already captured, if any (spec: one capture per run).
    pub fn source_of_run(&self, run: &RunId) -> Result<Option<String>> {
        match std::fs::read_to_string(self.run_record(run)) {
            Ok(s) => Ok(Some(s.trim().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Record the source of `run`, once. Returns the source the run is
    /// bound to: `ci_source_id`, or the one an earlier (concurrent) call
    /// recorded.
    pub fn bind_run(&self, run: &RunId, ci_source_id: &str) -> Result<String> {
        if source_id_of_ci(ci_source_id).is_none() {
            bail!("not a source id: {ci_source_id}");
        }
        let dest = self.run_record(run);
        let tmp =
            self.inner
                .root()
                .join("tmp")
                .join(format!(".run.{}.{}", std::process::id(), run));
        std::fs::write(&tmp, ci_source_id)?;
        // a hard link fails when the record exists: the first writer wins
        let linked = std::fs::hard_link(&tmp, &dest);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => Ok(ci_source_id.to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => self
                .source_of_run(run)?
                .context("the run record disappeared"),
            Err(e) => Err(e.into()),
        }
    }
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
