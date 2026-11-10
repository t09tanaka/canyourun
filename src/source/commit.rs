//! `--snapshot commit --ref <ref>`: capture a commit as it is, uncommitted
//! work left out.
//!
//! local-ci captures working directories. A commit is captured by checking it
//! out into a scratch repository under the runner's store (the original is
//! only read: nothing is written to its index, refs or worktrees) and
//! capturing that. The scratch repository gets the commit and its history by
//! `git fetch` from the original, so the unpushed history travels like any
//! other, and the original's `origin` URL, so the repository id is the same as
//! for a worktree capture of the same repository.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

fn run(env: &BTreeMap<String, String>, cwd: &Path, args: &[&str]) -> Result<String> {
    run_with(env, cwd, args, &[])
}

/// The commands that write the scratch repository ignore the user's and the
/// system's Git configuration: a checkout must leave the committed bytes as
/// they are (an LFS pointer stays a pointer, no smudge filter reaches for the
/// network); the capture reads the payload from the LFS store itself.
fn run_scratch(env: &BTreeMap<String, String>, cwd: &Path, args: &[&str]) -> Result<String> {
    run_with(
        env,
        cwd,
        args,
        &[
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ],
    )
}

fn run_with(
    env: &BTreeMap<String, String>,
    cwd: &Path,
    args: &[&str],
    extra: &[(&str, &str)],
) -> Result<String> {
    let out = Command::new("git")
        .args(["-c", "protocol.file.allow=always"])
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .envs(extra.iter().copied())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("cannot run git")?;
    if !out.status.success() {
        bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_string())
}

/// `rev` must name a revision, never an option.
pub(crate) fn check_rev(rev: &str) -> Result<()> {
    if rev.is_empty() || rev.starts_with('-') || rev.contains('\0') {
        bail!("not a revision: {rev:?}");
    }
    Ok(())
}

/// A scratch checkout of one commit. Removed on drop.
pub(crate) struct Checkout {
    pub root: PathBuf,
    /// Where the capture starts (the original's cwd, inside the checkout).
    pub cwd: PathBuf,
    pub base: Option<String>,
}

impl Drop for Checkout {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub(crate) fn checkout(
    env: &BTreeMap<String, String>,
    scratch_parent: &Path,
    dir: &Path,
    rev: &str,
    base: Option<&str>,
) -> Result<Checkout> {
    check_rev(rev)?;
    let top = PathBuf::from(run(env, dir, &["rev-parse", "--show-toplevel"])?);
    let top = std::fs::canonicalize(top)?;
    let cwd_rel = std::fs::canonicalize(dir)?
        .strip_prefix(&top)
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let commit = run(
        env,
        &top,
        &["rev-parse", "--verify", &format!("{rev}^{{commit}}")],
    )
    .with_context(|| format!("{rev} is not a commit of this repository"))?;
    let base = match base {
        Some(b) => {
            check_rev(b)?;
            Some(run(
                env,
                &top,
                &["rev-parse", "--verify", &format!("{b}^{{commit}}")],
            )?)
        }
        None => None,
    };

    std::fs::create_dir_all(scratch_parent)?;
    let root = scratch_parent.join(format!("commit-{}-{}", std::process::id(), &commit[..12]));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let co = Checkout {
        cwd: root.join(&cwd_rel),
        root: root.clone(),
        base: base.clone(),
    };
    run_scratch(env, &root, &["init", "-q"])?;
    let src = top.to_string_lossy().into_owned();
    let mut fetch = vec![
        "-c",
        "uploadpack.allowAnySHA1InWant=true",
        "fetch",
        "-q",
        "--no-tags",
        "--no-write-fetch-head",
        &src,
        &commit,
    ];
    if let Some(b) = &base {
        fetch.push(b);
    }
    run_scratch(env, &root, &fetch)?;
    if let Ok(url) = run(env, &top, &["config", "--get", "remote.origin.url"]) {
        run_scratch(env, &root, &["remote", "add", "origin", &url])?;
    }
    run_scratch(env, &root, &["checkout", "-q", "--detach", &commit])?;
    // LFS payloads the original has stay where the capture looks for them
    let common = run(env, &top, &["rev-parse", "--git-common-dir"])?;
    let lfs = top.join(common).join("lfs");
    if lfs.is_dir() {
        let st = Command::new("cp")
            .arg("-R")
            .arg(&lfs)
            .arg(root.join(".git/lfs"))
            .status()?;
        if !st.success() {
            bail!("cannot copy the LFS store of the original");
        }
    }
    if !co.cwd.is_dir() {
        bail!("{} does not exist in {}", cwd_rel.display(), &commit[..12]);
    }
    Ok(co)
}
