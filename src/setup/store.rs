//! The worker's store of finished templates: one directory per template key,
//! made beside its place and moved there in one rename, sealed read-only,
//! never written again. A consumer gets a copy of its own; nothing is ever
//! linked into a template.
//!
//! Layout: `<root>/<key>/receipt.json`, `<root>/<key>/tree/<path>` for every
//! added tree and every changed file, and `<root>/<key>.used` (when it was
//! last used, how big it is, the run it belongs to). Scratch directories,
//! and templates on their way out, are `<root>/.tmp-<ulid>`.
//!
//! One agent process owns a store. Everything that puts a template under
//! its key or takes it away happens under one lock, and takes it away by a
//! rename to a scratch name first: a reader sees a whole template or none.
//! A template handed out ([`Template`]) is pinned until it is dropped; a
//! pinned template is not evicted and not replaced.

use super::policy::{
    check_receipt, evictions, place, place_modified, place_removed, Gone, Kept, Limits, Place,
    Reject,
};
use crate::ids::Digest;
use crate::wire::{PreparedTemplateReceipt, SetupSpec};
use local_ci::tree::tree_digest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

/// A receipt is a few kilobytes (a few hundred with many changed entries);
/// one larger than this is not read.
const MAX_RECEIPT_BYTES: u64 = 4 << 20;
const TMP_PREFIX: &str = ".tmp-";
const USED_SUFFIX: &str = ".used";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Io(String),
    /// What was read does not match its receipt, or is not what a template
    /// may hold. The template is not used.
    #[error("{0}")]
    Corrupt(String),
    /// A path the template would write holds something else than the
    /// builder started from.
    #[error("`{0}` is not what the template was made over")]
    Conflict(String),
    /// A workspace was being changed, the change failed, and it could not
    /// be taken back: what the workspace holds is not known.
    #[error("{0}")]
    Dirty(String),
}

fn io_err(what: &str, e: io::Error) -> StoreError {
    StoreError::Io(format!("{what}: {e}"))
}

#[derive(Debug, Default)]
struct Shared {
    /// How many [`Template`]s are out, by directory name.
    pins: HashMap<String, usize>,
}

fn lock(m: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, Clone)]
pub struct TemplateStore {
    root: PathBuf,
    shared: Arc<Mutex<Shared>>,
}

#[derive(Debug)]
struct Pin {
    name: String,
    shared: Arc<Mutex<Shared>>,
}

impl Drop for Pin {
    fn drop(&mut self) {
        let mut sh = lock(&self.shared);
        if let Some(n) = sh.pins.get_mut(&self.name) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                sh.pins.remove(&self.name);
            }
        }
    }
}

/// A template in the store. It has no way to change what it holds; while it
/// lives the store does not evict or replace it.
#[derive(Debug, Clone)]
pub struct Template {
    dir: PathBuf,
    pub receipt: PreparedTemplateReceipt,
    _pin: Arc<Pin>,
}

impl Template {
    pub fn tree(&self) -> PathBuf {
        self.dir.join("tree")
    }
}

/// What the store remembers beside a template.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Used {
    last_used_ms: u64,
    bytes: u64,
    /// The run of a run-scope template.
    #[serde(default)]
    run: Option<String>,
}

/// One change of a workspace that is still to be made.
enum Step<'a> {
    Put {
        rel: &'a str,
        digest: &'a str,
        replace: bool,
    },
    Remove {
        rel: &'a str,
        dir: bool,
    },
}

/// How one change that was made is taken back.
enum Undo {
    Delete(PathBuf),
    Restore { kept: PathBuf, to: PathBuf },
    Mkdir(PathBuf),
}

impl TemplateStore {
    pub fn open(root: PathBuf) -> Result<Self, StoreError> {
        fs::create_dir_all(&root).map_err(|e| io_err("template store", e))?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|e| io_err("template store", e))?;
        Ok(TemplateStore {
            root,
            shared: Arc::default(),
        })
    }

    fn name_of(&self, key: &str) -> String {
        // `sha256:<hex>`; the key is made by the worker, but is still not
        // trusted to be one path component.
        key.chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect()
    }

    fn used_path(&self, name: &str) -> PathBuf {
        self.root.join(format!("{name}{USED_SUFFIX}"))
    }

    fn scratch(&self) -> PathBuf {
        self.root.join(format!("{TMP_PREFIX}{}", ulid::Ulid::new()))
    }

    /// Remove what a crash left: scratch directories, template directories
    /// without a readable receipt, and notes of templates that are gone.
    /// Returns how many entries went. Runs at the start, before any builder.
    pub fn sweep(&self) -> Result<usize, StoreError> {
        let mut n = 0;
        let rd = fs::read_dir(&self.root).map_err(|e| io_err("template store", e))?;
        for ent in rd.flatten() {
            let path = ent.path();
            let name = ent.file_name().to_string_lossy().into_owned();
            let half = match name.strip_suffix(USED_SUFFIX) {
                Some(of) => !self.root.join(of).join("receipt.json").is_file(),
                None => name.starts_with(TMP_PREFIX) || !path.join("receipt.json").is_file(),
            };
            if half {
                remove_tree(&path).map_err(|e| io_err("a leftover of a crash", e))?;
                n += 1;
            }
        }
        Ok(n)
    }

    fn pin(&self, sh: &mut Shared, name: &str) -> Arc<Pin> {
        *sh.pins.entry(name.to_string()).or_insert(0) += 1;
        Arc::new(Pin {
            name: name.to_string(),
            shared: self.shared.clone(),
        })
    }

    /// Take the template `name` out of its place (the lock is held): it is
    /// renamed to a scratch name, which the caller removes once the lock is
    /// released. Whoever still copies out of it finds it gone and is told so.
    fn retire(&self, _held: &mut Shared, name: &str) -> Option<PathBuf> {
        let _ = fs::remove_file(self.used_path(name));
        let away = self.scratch();
        fs::rename(self.root.join(name), &away).ok().map(|()| away)
    }

    fn read_used(&self, name: &str) -> Used {
        fs::read(self.used_path(name))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    fn write_used(&self, name: &str, used: &Used) {
        let bytes = serde_json::to_vec(used).expect("plain data");
        let _ = fs::write(self.used_path(name), bytes);
    }

    /// The template for `key`, when there is one that is the one asked for.
    /// One whose receipt is unreadable, for another key or source, or too old
    /// is taken out of the store (it would only be built again) unless
    /// somebody still copies out of it, and the answer is `None`. The trees
    /// are checked when they are copied out.
    pub fn get(&self, key: &str, source: &str, spec: &SetupSpec, now_ms: u64) -> Option<Template> {
        let name = self.name_of(key);
        let dir = self.root.join(&name);
        let mut away = None;
        let found = {
            let mut sh = lock(&self.shared);
            if fs::symlink_metadata(&dir).is_err() {
                None
            } else {
                match read_receipt(&dir) {
                    Some(receipt)
                        if dir.join("tree").is_dir()
                            && check_receipt(&receipt, key, source, spec, now_ms).is_ok() =>
                    {
                        let mut used = self.read_used(&name);
                        used.last_used_ms = now_ms;
                        self.write_used(&name, &used);
                        let _pin = self.pin(&mut sh, &name);
                        Some(Template { dir, receipt, _pin })
                    }
                    _ => {
                        if !sh.pins.contains_key(&name) {
                            away = self.retire(&mut sh, &name);
                        }
                        None
                    }
                }
            }
        };
        if let Some(p) = away {
            let _ = remove_tree(&p);
        }
        found
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
        let receipt = read_receipt(&self.root.join(self.name_of(key)))?;
        check_receipt(&receipt, key, source, spec, now_ms).err()
    }

    /// Keep what `receipt` describes, copied out of `built` (the workspace
    /// the builder prepared), as the template of `key`. The copy is made in
    /// a scratch directory of its own, checked against the receipt, written
    /// through to the disk, sealed, and moved into place; a template that is
    /// not complete is never seen under its key. One that is there already
    /// and that nobody copies out of was refused by `get` (or a builder
    /// would not have run): it is replaced. One somebody still copies out of
    /// stays, and is the one returned. `run` is the run of a run-scope
    /// template.
    pub fn publish(
        &self,
        key: &str,
        receipt: &PreparedTemplateReceipt,
        built: &Path,
        run: Option<&str>,
        now_ms: u64,
    ) -> Result<Template, StoreError> {
        if receipt.template_key != key {
            return Err(StoreError::Corrupt(
                "the receipt is for another template key".into(),
            ));
        }
        let tmp = self.scratch();
        fs::create_dir(&tmp).map_err(|e| io_err("scratch directory", e))?;
        let bytes = match self.fill(&tmp, receipt, built) {
            Ok(b) => b,
            Err(e) => {
                let _ = remove_tree(&tmp);
                return Err(e);
            }
        };
        let name = self.name_of(key);
        let dir = self.root.join(&name);
        let mut away = None;
        let placed = {
            let mut sh = lock(&self.shared);
            let there = fs::symlink_metadata(&dir).is_ok();
            let put = if there && sh.pins.contains_key(&name) {
                away = Some(tmp.clone());
                Ok(())
            } else {
                if there {
                    away = self.retire(&mut sh, &name);
                }
                fs::rename(&tmp, &dir)
                    .and_then(|()| sync_path(&self.root))
                    .map(|()| {
                        let used = Used {
                            last_used_ms: now_ms,
                            bytes,
                            run: run.map(str::to_string),
                        };
                        self.write_used(&name, &used);
                    })
            };
            put.map(|()| self.pin(&mut sh, &name))
        };
        if let Some(p) = away {
            let _ = remove_tree(&p);
        }
        let _pin = match placed {
            Ok(p) => p,
            Err(e) => {
                let _ = remove_tree(&tmp);
                return Err(io_err("cannot keep the template", e));
            }
        };
        let receipt = read_receipt(&dir)
            .ok_or_else(|| StoreError::Corrupt("the template is not readable".into()))?;
        Ok(Template { dir, receipt, _pin })
    }

    /// Fill the scratch directory; the bytes it holds.
    fn fill(
        &self,
        tmp: &Path,
        receipt: &PreparedTemplateReceipt,
        built: &Path,
    ) -> Result<u64, StoreError> {
        let tree = tmp.join("tree");
        fs::create_dir(&tree).map_err(|e| io_err("scratch directory", e))?;
        let added = receipt.artifacts.iter().map(|a| (&a.path, &a.digest));
        let changed = receipt.modified.iter().map(|m| (&m.path, &m.digest));
        let mut bytes = 0u64;
        for (path, digest) in added.chain(changed) {
            let to = tree.join(path);
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err("scratch directory", e))?;
            }
            copy_tree(&built.join(path), &to)?;
            let got = tree_digest(&to)
                .map_err(|e| StoreError::Corrupt(format!("`{path}` cannot be read: {e:#}")))?;
            if got.digest != *digest {
                return Err(StoreError::Corrupt(format!(
                    "`{path}` changed while it was copied"
                )));
            }
            bytes = bytes.saturating_add(got.bytes);
        }
        let text = serde_json::to_vec_pretty(receipt)
            .map_err(|e| StoreError::Io(format!("receipt: {e}")))?;
        fs::write(tmp.join("receipt.json"), text).map_err(|e| io_err("receipt", e))?;
        seal(tmp).map_err(|e| io_err("sealing the template", e))?;
        // on the disk before it gets its name: a crash after the rename
        // does not leave a template whose files are not there
        sync_tree(tmp).map_err(|e| io_err("writing the template through", e))?;
        Ok(bytes)
    }

    /// Take the template of `key` out of the store because it did not match
    /// its receipt, the one with the digest `judged`. A template another
    /// builder has put there since is another one, and stays.
    pub fn discard(&self, key: &str, judged: &str) {
        let name = self.name_of(key);
        let away = {
            let mut sh = lock(&self.shared);
            let same = read_receipt(&self.root.join(&name)).is_some_and(|r| r.digest == judged);
            if same {
                self.retire(&mut sh, &name)
            } else {
                None
            }
        };
        if let Some(p) = away {
            let _ = remove_tree(&p);
        }
    }

    /// Take out the templates that are too old, whose run is gone, or that
    /// were used least recently while the store is over its size, leaving
    /// those somebody copies out of. `run_gone` says whether a run has no
    /// attempt on this worker any more. Returns how many went.
    pub fn evict(&self, now_ms: u64, limits: &Limits, run_gone: &dyn Fn(&str) -> bool) -> usize {
        let mut away = Vec::new();
        {
            let mut sh = lock(&self.shared);
            let Ok(rd) = fs::read_dir(&self.root) else {
                return 0;
            };
            let mut kept = Vec::new();
            for ent in rd.flatten() {
                let name = ent.file_name().to_string_lossy().into_owned();
                if name.starts_with(TMP_PREFIX) || !ent.path().join("receipt.json").is_file() {
                    continue;
                }
                let used = self.read_used(&name);
                kept.push(Kept {
                    pinned: sh.pins.contains_key(&name),
                    run_gone: used.run.as_deref().is_some_and(run_gone),
                    bytes: used.bytes,
                    last_used_ms: used.last_used_ms,
                    name,
                });
            }
            for name in evictions(&kept, now_ms, limits) {
                away.extend(self.retire(&mut sh, &name));
            }
        }
        for p in &away {
            let _ = remove_tree(p);
        }
        away.len()
    }

    /// Make the workspace at `dest` what the builder's was after its steps:
    /// the added trees and changed files are copied in, each writable,
    /// sharing nothing with the template, and what the steps removed is
    /// removed.
    ///
    /// Everything is looked at before anything is written. An added path
    /// that holds something else, a changed or removed entry that is not
    /// what the builder started from, or a path that leads through a link,
    /// is `Conflict`: nothing is touched. What a crash cut short is taken up
    /// where it stopped (an entry that is already what the template says is
    /// left). The copies are made and checked against the receipt in a
    /// scratch directory of the store, outside the workspace; a copy that
    /// does not match is `Corrupt`. Only then is the workspace changed, by
    /// renames, and a change that fails is taken back, so that an error
    /// leaves the workspace as it was (`Dirty` when that failed too).
    pub fn materialize(&self, template: &Template, dest: &Path) -> Result<(), StoreError> {
        let r = &template.receipt;
        let conflict = |p: &str| Err(StoreError::Conflict(p.to_string()));
        let digest_at = |p: &Path| tree_digest(p).ok().map(|d| d.digest);
        let mut steps = Vec::new();
        for a in &r.artifacts {
            let to = dest.join(&a.path);
            if !real_parent(dest, &a.path) {
                return conflict(&a.path);
            }
            let exists = fs::symlink_metadata(&to).is_ok();
            let there = if exists { digest_at(&to) } else { None };
            match place(exists, there.as_deref(), &a.digest) {
                Place::AlreadyThere => {}
                Place::Conflict => return conflict(&a.path),
                Place::Copy => steps.push(Step::Put {
                    rel: &a.path,
                    digest: &a.digest,
                    replace: false,
                }),
            }
        }
        for m in &r.modified {
            let to = dest.join(&m.path);
            let is_leaf = fs::symlink_metadata(&to).is_ok_and(|x| !x.is_dir());
            if !real_parent(dest, &m.path) || !is_leaf {
                return conflict(&m.path);
            }
            match place_modified(digest_at(&to).as_deref(), &m.base, &m.digest) {
                Place::AlreadyThere => {}
                Place::Conflict => return conflict(&m.path),
                Place::Copy => steps.push(Step::Put {
                    rel: &m.path,
                    digest: &m.digest,
                    replace: true,
                }),
            }
        }
        // in reverse: what is in a removed directory goes before it
        for x in r.removed.iter().rev() {
            let to = dest.join(&x.path);
            let meta = fs::symlink_metadata(&to).ok();
            let is_dir = meta.as_ref().is_some_and(|m| m.is_dir());
            if meta.is_some() && !real_parent(dest, &x.path) {
                return conflict(&x.path);
            }
            let there = if meta.is_some() && !is_dir {
                digest_at(&to)
            } else {
                None
            };
            match place_removed(meta.is_some(), is_dir, there.as_deref(), x.base.as_deref()) {
                Gone::Already => {}
                Gone::Conflict => return conflict(&x.path),
                Gone::Remove => steps.push(Step::Remove {
                    rel: &x.path,
                    dir: is_dir,
                }),
            }
        }
        if steps.is_empty() {
            return Ok(());
        }
        let scratch = self.scratch();
        fs::create_dir(&scratch).map_err(|e| io_err("scratch directory", e))?;
        let done = apply(&template.tree(), dest, &scratch, &steps);
        let _ = remove_tree(&scratch);
        done
    }
}

/// Copy, check, then change the workspace; see [`TemplateStore::materialize`].
fn apply(tree: &Path, dest: &Path, scratch: &Path, steps: &[Step]) -> Result<(), StoreError> {
    let new_at = |i: usize| scratch.join(format!("new-{i}"));
    for (i, s) in steps.iter().enumerate() {
        let Step::Put { rel, digest, .. } = s else {
            continue;
        };
        let copy = new_at(i);
        copy_tree(&tree.join(rel), &copy)?;
        make_writable(&copy).map_err(|e| io_err("copy", e))?;
        let got = tree_digest(&copy).map_err(|e| StoreError::Corrupt(format!("`{rel}`: {e:#}")))?;
        if got.digest != **digest {
            return Err(StoreError::Corrupt(format!(
                "the copy of `{rel}` is not what the receipt describes"
            )));
        }
    }
    let mut undo: Vec<Undo> = Vec::new();
    let mut failed = None;
    for (i, s) in steps.iter().enumerate() {
        let made = match s {
            Step::Put { rel, replace, .. } => {
                let to = dest.join(rel);
                let kept = scratch.join(format!("old-{i}"));
                let moved = if *replace {
                    fs::rename(&to, &kept).map(|()| {
                        undo.push(Undo::Restore {
                            kept,
                            to: to.clone(),
                        })
                    })
                } else {
                    Ok(())
                };
                moved.and_then(|()| fs::rename(new_at(i), &to)).map(|()| {
                    if !*replace {
                        undo.push(Undo::Delete(to));
                    }
                })
            }
            Step::Remove { rel, dir: false } => {
                let to = dest.join(rel);
                let kept = scratch.join(format!("old-{i}"));
                fs::rename(&to, &kept).map(|()| undo.push(Undo::Restore { kept, to }))
            }
            Step::Remove { rel, dir: true } => {
                let to = dest.join(rel);
                // only an empty one goes: what else is in it is not the
                // builder's to remove
                fs::remove_dir(&to).map(|()| undo.push(Undo::Mkdir(to)))
            }
        };
        if let Err(e) = made {
            failed = Some(e);
            break;
        }
    }
    let Some(e) = failed else {
        return Ok(());
    };
    for u in undo.iter().rev() {
        let back = match u {
            Undo::Delete(to) => remove_tree(to),
            Undo::Restore { kept, to } => remove_tree(to).and_then(|()| fs::rename(kept, to)),
            Undo::Mkdir(to) => fs::create_dir(to),
        };
        if let Err(b) = back {
            return Err(StoreError::Dirty(format!(
                "the workspace could not be changed ({e}) and not be put back ({b})"
            )));
        }
    }
    Err(io_err("workspace", e))
}

/// Whether every directory on the way from `root` to the parent of `rel` is
/// a real directory: nothing is written through a link.
fn real_parent(root: &Path, rel: &str) -> bool {
    let mut at = root.to_path_buf();
    let mut parts: Vec<&str> = rel.split('/').collect();
    parts.pop();
    for part in parts {
        at.push(part);
        if !fs::symlink_metadata(&at).is_ok_and(|m| m.is_dir()) {
            return false;
        }
    }
    true
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
/// symlinks (as links, not followed). Nothing is a link into `from`. The
/// tree is walked with a list, not by recursion: its depth costs no stack.
fn copy_tree(from: &Path, to: &Path) -> Result<(), StoreError> {
    let mut todo = vec![(from.to_path_buf(), to.to_path_buf())];
    while let Some((from, to)) = todo.pop() {
        let meta = fs::symlink_metadata(&from).map_err(|e| io_err("copy", e))?;
        let ty = meta.file_type();
        if ty.is_symlink() {
            let target = fs::read_link(&from).map_err(|e| io_err("copy", e))?;
            symlink(target, &to).map_err(|e| io_err("copy", e))?;
        } else if ty.is_dir() {
            // writable while it is filled, whatever the original's mode
            fs::create_dir(&to).map_err(|e| io_err("copy", e))?;
            fs::set_permissions(
                &to,
                fs::Permissions::from_mode(meta.permissions().mode() | 0o700),
            )
            .map_err(|e| io_err("copy", e))?;
            for ent in fs::read_dir(&from).map_err(|e| io_err("copy", e))? {
                let ent = ent.map_err(|e| io_err("copy", e))?;
                todo.push((ent.path(), to.join(ent.file_name())));
            }
        } else if ty.is_file() {
            // a copy of the bytes into a new file: never a link
            fs::copy(&from, &to).map_err(|e| io_err("copy", e))?;
        } else {
            return Err(StoreError::Corrupt(format!(
                "{} is neither a file, a directory nor a link",
                from.display()
            )));
        }
    }
    Ok(())
}

/// Every entry under `root` without a write bit (links are left alone).
fn seal(root: &Path) -> io::Result<()> {
    map_modes(root, &|mode| mode & !0o222)
}

/// The owner may write everything under `root`.
fn make_writable(root: &Path) -> io::Result<()> {
    map_modes(root, &|mode| mode | 0o200)
}

/// One thing left to do while the modes of a tree are changed.
enum Mode {
    Visit(PathBuf),
    /// A directory whose entries are done gets its mode last.
    Set(PathBuf, u32),
}

/// Change the mode of everything under `path`, a directory after what is in
/// it. Walked with a list, not by recursion.
fn map_modes(path: &Path, f: &dyn Fn(u32) -> u32) -> io::Result<()> {
    let mut todo = vec![Mode::Visit(path.to_path_buf())];
    while let Some(next) = todo.pop() {
        let path = match next {
            Mode::Set(path, mode) => {
                fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
                continue;
            }
            Mode::Visit(path) => path,
        };
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        let mode = f(meta.permissions().mode());
        if meta.is_dir() {
            // a directory is entered while it can still be entered
            let open = meta.permissions().mode() | 0o700;
            fs::set_permissions(&path, fs::Permissions::from_mode(open))?;
            todo.push(Mode::Set(path.clone(), mode));
            for ent in fs::read_dir(&path)? {
                todo.push(Mode::Visit(ent?.path()));
            }
        } else {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
        }
    }
    Ok(())
}

/// Write `path` (a file or a directory) through to the disk.
fn sync_path(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

/// Write every file and directory under `root` through to the disk.
fn sync_tree(root: &Path) -> io::Result<()> {
    let mut todo = vec![root.to_path_buf()];
    while let Some(path) = todo.pop() {
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            for ent in fs::read_dir(&path)? {
                todo.push(ent?.path());
            }
        }
        sync_path(&path)?;
    }
    Ok(())
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
