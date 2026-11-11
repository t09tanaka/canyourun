//! The worker's store of finished templates: one directory per template key,
//! made beside its place and moved there in one rename, sealed read-only,
//! never written again. A consumer gets a copy of its own; nothing is ever
//! linked into a template.
//!
//! Layout: `<root>/<key>/receipt.json` and `<root>/<key>/tree/<artifact>`.
//! Scratch directories are `<root>/.tmp-<ulid>`.

use super::policy::{check_receipt, place, Place, Reject};
use crate::ids::Digest;
use crate::wire::{PreparedTemplateReceipt, SetupSpec};
use local_ci::tree::tree_digest;
use std::fs;
use std::io;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// A receipt is a few kilobytes; one larger than this is not read.
const MAX_RECEIPT_BYTES: u64 = 1 << 20;
const TMP_PREFIX: &str = ".tmp-";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Io(String),
    /// What was read does not match its receipt, or is not what a template
    /// may hold. The template is not used.
    #[error("{0}")]
    Corrupt(String),
    /// A path the template would write already holds something else.
    #[error("`{0}` already holds something else")]
    Conflict(String),
}

fn io_err(what: &str, e: io::Error) -> StoreError {
    StoreError::Io(format!("{what}: {e}"))
}

#[derive(Debug, Clone)]
pub struct TemplateStore {
    root: PathBuf,
}

/// A template in the store. It has no way to change what it holds.
#[derive(Debug, Clone)]
pub struct Template {
    dir: PathBuf,
    pub receipt: PreparedTemplateReceipt,
}

impl Template {
    pub fn tree(&self) -> PathBuf {
        self.dir.join("tree")
    }
}

impl TemplateStore {
    pub fn open(root: PathBuf) -> Result<Self, StoreError> {
        fs::create_dir_all(&root).map_err(|e| io_err("template store", e))?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|e| io_err("template store", e))?;
        Ok(TemplateStore { root })
    }

    fn dir_of(&self, key: &str) -> PathBuf {
        // `sha256:<hex>`; the key is made by the worker, but is still not
        // trusted to be one path component.
        let name: String = key
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        self.root.join(name)
    }

    /// Remove what a crash left: scratch directories, and template
    /// directories without a readable receipt. Returns how many entries
    /// went. Runs at the start, before any builder.
    pub fn sweep(&self) -> Result<usize, StoreError> {
        let mut n = 0;
        let rd = fs::read_dir(&self.root).map_err(|e| io_err("template store", e))?;
        for ent in rd.flatten() {
            let path = ent.path();
            let name = ent.file_name().to_string_lossy().into_owned();
            let half = name.starts_with(TMP_PREFIX) || !path.join("receipt.json").is_file();
            if half {
                remove_tree(&path).map_err(|e| io_err("a leftover of a crash", e))?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// The template for `key`, when there is one that is the one asked for.
    /// One whose receipt is unreadable, for another key or source, or too old
    /// is removed (it would only be built again), and the answer is `None`.
    /// The trees are checked when they are copied out.
    pub fn get(
        &self,
        key: &str,
        source: &str,
        spec: &SetupSpec,
        now_ms: u64,
    ) -> Result<Option<Template>, StoreError> {
        let dir = self.dir_of(key);
        if fs::symlink_metadata(&dir).is_err() {
            return Ok(None);
        }
        match read_receipt(&dir) {
            Some(receipt)
                if dir.join("tree").is_dir()
                    && check_receipt(&receipt, key, source, spec, now_ms).is_ok() =>
            {
                Ok(Some(Template { dir, receipt }))
            }
            _ => {
                remove_tree(&dir).map_err(|e| io_err("an unusable template", e))?;
                Ok(None)
            }
        }
    }

    /// Why `get` would not use the template for `key`, if there is one that
    /// it would not use.
    pub fn refusal(
        &self,
        key: &str,
        source: &str,
        spec: &SetupSpec,
        now_ms: u64,
    ) -> Option<Reject> {
        let receipt = read_receipt(&self.dir_of(key))?;
        check_receipt(&receipt, key, source, spec, now_ms).err()
    }

    /// Keep what `receipt` describes, copied out of `built` (the workspace
    /// the builder prepared), as the template of `key`. The copy is made in
    /// a scratch directory of its own, checked against the receipt, sealed,
    /// and moved into place; a template that is not complete is never seen
    /// under its key. If one is there already (another builder of the same
    /// key finished first), it is the one returned.
    pub fn publish(
        &self,
        key: &str,
        receipt: &PreparedTemplateReceipt,
        built: &Path,
    ) -> Result<Template, StoreError> {
        if receipt.template_key != key {
            return Err(StoreError::Corrupt(
                "the receipt is for another template key".into(),
            ));
        }
        let tmp = self.root.join(format!("{TMP_PREFIX}{}", ulid::Ulid::new()));
        fs::create_dir(&tmp).map_err(|e| io_err("scratch directory", e))?;
        match self.fill(&tmp, receipt, built) {
            Ok(()) => {}
            Err(e) => {
                let _ = remove_tree(&tmp);
                return Err(e);
            }
        }
        let dir = self.dir_of(key);
        match fs::rename(&tmp, &dir) {
            Ok(()) => {}
            Err(_) if dir.join("receipt.json").is_file() => {
                let _ = remove_tree(&tmp);
            }
            Err(e) => {
                let _ = remove_tree(&tmp);
                return Err(io_err("cannot keep the template", e));
            }
        }
        let receipt = read_receipt(&dir)
            .ok_or_else(|| StoreError::Corrupt("the template is not readable".into()))?;
        Ok(Template { dir, receipt })
    }

    fn fill(
        &self,
        tmp: &Path,
        receipt: &PreparedTemplateReceipt,
        built: &Path,
    ) -> Result<(), StoreError> {
        let tree = tmp.join("tree");
        fs::create_dir(&tree).map_err(|e| io_err("scratch directory", e))?;
        for a in &receipt.artifacts {
            let to = tree.join(&a.path);
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err("scratch directory", e))?;
            }
            copy_tree(&built.join(&a.path), &to)?;
            let got = tree_digest(&to)
                .map_err(|e| StoreError::Corrupt(format!("`{}` cannot be read: {e:#}", a.path)))?;
            if got.digest != a.digest {
                return Err(StoreError::Corrupt(format!(
                    "`{}` changed while it was copied",
                    a.path
                )));
            }
        }
        let text = serde_json::to_vec_pretty(receipt)
            .map_err(|e| StoreError::Io(format!("receipt: {e}")))?;
        fs::write(tmp.join("receipt.json"), text).map_err(|e| io_err("receipt", e))?;
        seal(tmp).map_err(|e| io_err("sealing the template", e))
    }

    /// Remove the template of `key` (it did not match its receipt).
    pub fn discard(&self, key: &str) -> Result<(), StoreError> {
        remove_tree(&self.dir_of(key)).map_err(|e| io_err("discarding a template", e))
    }

    /// Copy the artifacts of `template` into the workspace at `dest`, each
    /// one writable, sharing nothing with the template, and checked against
    /// the receipt's digest after the copy. A path that holds something else
    /// already is `Conflict` and is not written over; one that holds the
    /// same tree already (a copy a crash cut short) is left. A copy that does
    /// not match is removed and is `Corrupt`: the template is not what its
    /// receipt says.
    pub fn materialize(&self, template: &Template, dest: &Path) -> Result<(), StoreError> {
        for a in &template.receipt.artifacts {
            let to = dest.join(&a.path);
            let exists = fs::symlink_metadata(&to).is_ok();
            let there = if exists {
                tree_digest(&to).ok().map(|d| d.digest)
            } else {
                None
            };
            match place(exists, there.as_deref(), &a.digest) {
                Place::AlreadyThere => continue,
                Place::Conflict => return Err(StoreError::Conflict(a.path.clone())),
                Place::Copy => {}
            }
            let parent = to.parent().expect("an artifact path has a parent here");
            fs::create_dir_all(parent).map_err(|e| io_err("workspace", e))?;
            let scratch = parent.join(format!("{TMP_PREFIX}{}", ulid::Ulid::new()));
            let done = copy_tree(&template.tree().join(&a.path), &scratch)
                .and_then(|()| make_writable(&scratch).map_err(|e| io_err("copy", e)))
                .and_then(|()| {
                    let got = tree_digest(&scratch)
                        .map_err(|e| StoreError::Corrupt(format!("`{}`: {e:#}", a.path)))?;
                    if got.digest == a.digest {
                        Ok(())
                    } else {
                        Err(StoreError::Corrupt(format!(
                            "the copy of `{}` is not what the receipt describes",
                            a.path
                        )))
                    }
                })
                .and_then(|()| fs::rename(&scratch, &to).map_err(|e| io_err("workspace", e)));
            if let Err(e) = done {
                let _ = remove_tree(&scratch);
                return Err(e);
            }
        }
        Ok(())
    }
}

fn read_receipt(dir: &Path) -> Option<PreparedTemplateReceipt> {
    let path = dir.join("receipt.json");
    let meta = fs::symlink_metadata(&path).ok()?;
    if !meta.is_file() || meta.len() > MAX_RECEIPT_BYTES {
        return None;
    }
    serde_json::from_slice(&fs::read(&path).ok()?).ok()
}

/// `from` as `to`, which must not exist: directories, regular files and
/// symlinks (as links, not followed). Nothing is a link into `from`.
fn copy_tree(from: &Path, to: &Path) -> Result<(), StoreError> {
    let meta = fs::symlink_metadata(from).map_err(|e| io_err("copy", e))?;
    let ty = meta.file_type();
    if ty.is_symlink() {
        let target = fs::read_link(from).map_err(|e| io_err("copy", e))?;
        symlink(target, to).map_err(|e| io_err("copy", e))
    } else if ty.is_dir() {
        // writable while it is filled, whatever the original's mode
        fs::create_dir(to).map_err(|e| io_err("copy", e))?;
        for ent in fs::read_dir(from).map_err(|e| io_err("copy", e))? {
            let ent = ent.map_err(|e| io_err("copy", e))?;
            copy_tree(&ent.path(), &to.join(ent.file_name()))?;
        }
        fs::set_permissions(
            to,
            fs::Permissions::from_mode(meta.permissions().mode() | 0o700),
        )
        .map_err(|e| io_err("copy", e))
    } else if ty.is_file() {
        // a copy of the bytes into a new file: never a link
        fs::copy(from, to)
            .map(|_| ())
            .map_err(|e| io_err("copy", e))
    } else {
        Err(StoreError::Corrupt(format!(
            "{} is neither a file, a directory nor a link",
            from.display()
        )))
    }
}

/// Every entry under `root` without a write bit (links are left alone).
fn seal(root: &Path) -> io::Result<()> {
    map_modes(root, &|mode| mode & !0o222)
}

/// The owner may write everything under `root`.
fn make_writable(root: &Path) -> io::Result<()> {
    map_modes(root, &|mode| mode | 0o200)
}

fn map_modes(path: &Path, f: &dyn Fn(u32) -> u32) -> io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(());
    }
    if meta.is_dir() {
        // a directory is entered while it can still be entered
        let open = meta.permissions().mode() | 0o700;
        fs::set_permissions(path, fs::Permissions::from_mode(open))?;
        for ent in fs::read_dir(path)? {
            map_modes(&ent?.path(), f)?;
        }
    }
    fs::set_permissions(
        path,
        fs::Permissions::from_mode(f(meta.permissions().mode())),
    )
}

/// Remove `path` (a sealed tree too). A path that is not there is removed.
pub fn remove_tree(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
        Ok(m) if m.is_dir() => {
            map_modes(path, &|mode| mode | 0o700)?;
            fs::remove_dir_all(path)
        }
        Ok(_) => fs::remove_file(path),
    }
}

/// The digest of a tree, `sha256:`-prefixed like the receipt's.
pub fn digest_of(path: &Path) -> Result<String, StoreError> {
    let d =
        tree_digest(path).map_err(|e| StoreError::Corrupt(format!("{}: {e:#}", path.display())))?;
    debug_assert!(d.digest.starts_with(Digest::PREFIX));
    Ok(d.digest)
}
