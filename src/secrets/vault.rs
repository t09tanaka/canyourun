//! The parent's hold on a run's secret files (spec 22.3, "親での保持").
//!
//! local-ci writes the bundle of a capture into the source store, keyed by
//! source: two runs with the same input would share one file, and the end of
//! the first would take the bundle from the second. So the run takes its own
//! copy (`<state>/secrets/<run>.json`, 0600) and the store's copy is removed at
//! once. A retry of the run reads the same copy, never the working tree again.
//! The copy is deleted when the run is terminal.

use super::envfile::{secret_values, Values};
use super::key::ClusterKey;
use super::plan::{check_limits, check_paths, ct_eq, hex_decode, listing_digest, to_wire_digest};
use super::{Result, SecretError};
use crate::ids::{Digest, RunId};
use crate::wire::{Bytes, SecretFile};
use local_ci::source::{secret_digest, RelPath, SecretEntry};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// The files of one run with their keyed digests, ready to send. `Debug`
/// names the paths and never shows a content.
#[derive(Clone, PartialEq, Eq)]
pub struct HeldBundle {
    pub files: Vec<SecretFile>,
}

impl std::fmt::Debug for HeldBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldBundle")
            .field(
                "paths",
                &self
                    .files
                    .iter()
                    .map(|x| x.path.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl HeldBundle {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Digest of the listing (paths and HMACs, not contents), for the storage
    /// reference of this bundle.
    pub fn listing_digest(&self) -> Digest {
        let entries: Vec<(String, String)> = self
            .files
            .iter()
            .map(|f| (f.path.clone(), f.hmac_digest.to_string()))
            .collect();
        listing_digest(&entries)
    }

    /// The strings to mask in logs: the `KEY=VALUE` values of every file, and
    /// the keys whose value was too short to mask.
    pub fn mask_values(&self) -> Values {
        let mut all = Values::default();
        let mut seen = BTreeSet::new();
        let mut skipped = BTreeSet::new();
        for f in &self.files {
            let v = secret_values(&f.content.0);
            for x in v.values {
                if seen.insert(x.clone()) {
                    all.values.push(x);
                }
            }
            skipped.extend(v.skipped_keys);
        }
        all.skipped_keys = skipped.into_iter().collect();
        all
    }
}

#[derive(Deserialize)]
struct Stored {
    schema_version: u32,
    files: Vec<StoredFile>,
}

#[derive(Deserialize)]
struct StoredFile {
    path: StoredPath,
    content_hex: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredPath {
    Text(String),
    Hex { hex: String },
}

impl StoredPath {
    fn rel(&self) -> Option<RelPath> {
        match self {
            StoredPath::Text(s) => Some(RelPath(s.clone().into_bytes())),
            StoredPath::Hex { hex } => hex_decode(hex).map(RelPath),
        }
    }
}

fn unreadable() -> SecretError {
    SecretError::Format("the secret bundle is not readable".into())
}

/// A regular file (not a link) that only its owner can access.
pub(crate) fn read_private(path: &Path, what: &'static str) -> Result<Vec<u8>> {
    let meta = fs::symlink_metadata(path).map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            SecretError::Unavailable {
                what: what.to_string(),
            }
        } else {
            SecretError::io(format!("{what} {}", path.display()), e)
        }
    })?;
    if !meta.file_type().is_file() || meta.mode() & 0o077 != 0 {
        return Err(SecretError::Insecure {
            what,
            path: path.to_path_buf(),
        });
    }
    fs::read(path).map_err(|e| SecretError::io(format!("{what} {}", path.display()), e))
}

/// Write `bytes` to `path` as 0600, all at once: a reader never sees half of
/// it, and an existing file is replaced.
pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| SecretError::Format("secret path has no directory".into()))?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default();
    let tmp = dir.join(format!(".{name}.{}.tmp", ulid::Ulid::new()));
    let io = |e| SecretError::io(format!("secret file {}", path.display()), e);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io)?;
    let r = f.write_all(bytes).and_then(|()| f.sync_all());
    drop(f);
    let r = r.and_then(|()| fs::rename(&tmp, path));
    if let Err(e) = r {
        let _ = fs::remove_file(&tmp);
        return Err(io(e));
    }
    Ok(())
}

pub struct Vault {
    dir: PathBuf,
}

impl Vault {
    /// `<state_dir>/secrets`, made (0700) when missing; one that others can
    /// read, or that is a link, is refused.
    pub fn open(state_dir: &Path) -> Result<Vault> {
        let dir = state_dir.join("secrets");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        let meta = fs::symlink_metadata(&dir)
            .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
        if !meta.file_type().is_dir() || meta.mode() & 0o077 != 0 {
            return Err(SecretError::Insecure {
                what: "secret directory",
                path: dir,
            });
        }
        Ok(Vault { dir })
    }

    fn path(&self, run: &RunId) -> PathBuf {
        self.dir.join(format!("{run}.json"))
    }

    pub fn has(&self, run: &RunId) -> bool {
        fs::symlink_metadata(self.path(run)).is_ok()
    }

    /// Take the bundle local-ci wrote at `store_copy` for `run`. A run that
    /// already holds a bundle keeps it (a retry must not change it). The
    /// store's copy is removed either way.
    pub fn adopt(&self, run: &RunId, store_copy: &Path) -> Result<()> {
        let bytes = read_private(store_copy, "secret bundle")?;
        serde_json::from_slice::<Stored>(&bytes).map_err(|_| unreadable())?;
        if !self.has(run) {
            write_private(&self.path(run), &bytes)?;
        }
        match fs::remove_file(store_copy) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(SecretError::io(
                format!("secret bundle {}", store_copy.display()),
                e,
            )),
            _ => Ok(()),
        }
    }

    /// The files of `run`, checked against the manifest's `entries`: the same
    /// paths, and every HMAC equal to the one recomputed under `key`. A run
    /// whose manifest names no secret file has an empty bundle. A bundle that
    /// is missing is `Unavailable` (`blocked: secret_unavailable`): never an
    /// empty or current-from-disk replacement.
    pub fn load(
        &self,
        run: &RunId,
        entries: &[SecretEntry],
        key: &ClusterKey,
    ) -> Result<HeldBundle> {
        if entries.is_empty() {
            return Ok(HeldBundle { files: vec![] });
        }
        let bytes = read_private(&self.path(run), "secret bundle").map_err(|e| match e {
            SecretError::Unavailable { .. } => SecretError::Unavailable {
                what: format!("run {run}"),
            },
            other => other,
        })?;
        let stored: Stored = serde_json::from_slice(&bytes).map_err(|_| unreadable())?;
        if stored.schema_version != 1 {
            return Err(SecretError::Format(format!(
                "secret bundle schema {} is not supported",
                stored.schema_version
            )));
        }
        let mut have: Vec<(RelPath, Vec<u8>)> = Vec::new();
        for f in stored.files {
            let rel = f.path.rel().ok_or_else(unreadable)?;
            let content = hex_decode(&f.content_hex).ok_or_else(unreadable)?;
            have.push((rel, content));
        }
        let want: BTreeSet<&RelPath> = entries.iter().map(|e| &e.path).collect();
        let got: BTreeSet<&RelPath> = have.iter().map(|(p, _)| p).collect();
        if want != got || have.len() != entries.len() {
            return Err(SecretError::PathSetMismatch);
        }
        let mut files = Vec::new();
        for (rel, content) in have {
            let entry = entries
                .iter()
                .find(|e| e.path == rel)
                .ok_or(SecretError::PathSetMismatch)?;
            let path = String::from_utf8(rel.0.clone())
                .map_err(|_| SecretError::NonUtf8Path(rel.display()))?;
            let digest = secret_digest(key.as_bytes(), &rel, &content);
            if !ct_eq(digest.as_bytes(), entry.hmac.as_bytes()) {
                return Err(SecretError::Tampered { path });
            }
            let hmac_digest = to_wire_digest(&digest)
                .ok_or_else(|| SecretError::Format("unexpected digest form".into()))?;
            files.push(SecretFile {
                path,
                hmac_digest,
                content: Bytes(content),
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        check_paths(files.iter().map(|f| f.path.as_str())).map_err(SecretError::BadPath)?;
        let sizes: Vec<(usize, usize)> = files
            .iter()
            .map(|f| (f.path.len(), f.content.0.len()))
            .collect();
        check_limits(&sizes).map_err(SecretError::Limit)?;
        Ok(HeldBundle { files })
    }

    /// Delete the run's bundle (idempotent).
    pub fn erase(&self, run: &RunId) -> Result<()> {
        match fs::remove_file(self.path(run)) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(SecretError::io(
                format!("secret bundle of run {run}"),
                e,
            )),
            _ => Ok(()),
        }
    }

    /// Delete the bundle of every run `is_terminal` says is finished (and
    /// whose required collection is done). Returns the runs erased. A file
    /// that is not a run's bundle is left alone.
    pub fn sweep(&self, is_terminal: impl Fn(&RunId) -> bool) -> Result<Vec<RunId>> {
        let mut erased = Vec::new();
        let rd = fs::read_dir(&self.dir)
            .map_err(|e| SecretError::io(format!("directory {}", self.dir.display()), e))?;
        for entry in rd {
            let entry = entry
                .map_err(|e| SecretError::io(format!("directory {}", self.dir.display()), e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(run) = RunId::parse(stem) else {
                continue;
            };
            if is_terminal(&run) {
                self.erase(&run)?;
                erased.push(run);
            }
        }
        erased.sort();
        Ok(erased)
    }
}
