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

pub(crate) fn run(env: &BTreeMap<String, String>, cwd: &Path, args: &[&str]) -> Result<String> {
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
    /// Where the capture starts (the original's cwd, inside the checkout).
    pub cwd: PathBuf,
    pub base: Option<String>,
    /// The directory itself: unique per call (another capture of the same
    /// commit, in this process or another, has its own), and removed with it.
    _dir: tempfile::TempDir,
}

/// The pinned commits of the submodules of `commit` (a Git directory), as
/// `(path, revision)`; nested ones are not listed.
fn gitlinks(
    env: &BTreeMap<String, String>,
    gitdir: &Path,
    commit: &str,
) -> Result<Vec<(String, String)>> {
    let gd = gitdir.to_string_lossy().into_owned();
    let out = run(
        env,
        gitdir,
        &["--git-dir", &gd, "ls-tree", "-r", "-z", commit],
    )?;
    let mut v = vec![];
    for rec in out.split('\0').filter(|r| !r.is_empty()) {
        let Some((meta, path)) = rec.split_once('\t') else {
            continue;
        };
        let mut f = meta.split(' ');
        if f.next() == Some("160000") {
            if let Some(sha) = f.nth(1) {
                v.push((path.to_string(), sha.to_string()));
            }
        }
    }
    Ok(v)
}

/// path -> name of the submodules `.gitmodules` of `commit` declares.
fn submodule_names(
    env: &BTreeMap<String, String>,
    gitdir: &Path,
    commit: &str,
) -> BTreeMap<String, String> {
    let gd = gitdir.to_string_lossy().into_owned();
    let blob = format!("{commit}:.gitmodules");
    let Ok(out) = run(
        env,
        gitdir,
        &[
            "--git-dir",
            &gd,
            "config",
            "--null",
            "--blob",
            &blob,
            "--get-regexp",
            r"^submodule\..*\.path$",
        ],
    ) else {
        return BTreeMap::new();
    };
    out.split('\0')
        .filter_map(|rec| rec.split_once('\n'))
        .filter_map(|(k, path)| {
            let name = k.strip_prefix("submodule.")?.strip_suffix(".path")?;
            Some((path.to_string(), name.to_string()))
        })
        .collect()
}

/// Put the submodules of `commit` (already checked out in `dest`) in `dest`,
/// each at its pinned revision, with its files: the commits are fetched into
/// scratch repositories from the Git directories the original already holds
/// (`<git dir>/modules/<name>`, or an old-style `.git` directory inside the
/// submodule's working directory), never from the network. A submodule that
/// cannot be had this way is an error naming its path: capturing only its
/// revision would give a source that does not hold what the commit holds.
fn init_submodules(
    env: &BTreeMap<String, String>,
    src_common: &Path,
    src_work: Option<&Path>,
    dest: &Path,
    commit: &str,
    prefix: &str,
) -> Result<()> {
    let links = gitlinks(env, src_common, commit)?;
    if links.is_empty() {
        return Ok(());
    }
    let names = submodule_names(env, src_common, commit);
    for (path, sha) in links {
        let shown = format!("{prefix}{path}");
        let name = names
            .get(&path)
            .with_context(|| format!("submodule {shown}: not declared in .gitmodules"))?;
        let mut cands = vec![src_common.join("modules").join(name)];
        if let Some(w) = src_work {
            cands.push(w.join(&path).join(".git"));
        }
        let from = cands
            .into_iter()
            .find(|c| c.is_dir())
            .with_context(|| {
                format!("submodule {shown}: the original has no clone of it (run `git submodule update --init` there)")
            })?;
        let sub = dest.join(&path);
        std::fs::create_dir_all(&sub)?;
        run_scratch(env, &sub, &["init", "-q"])?;
        let src = from.to_string_lossy().into_owned();
        run_scratch(
            env,
            &sub,
            &[
                "-c",
                "uploadpack.allowAnySHA1InWant=true",
                "fetch",
                "-q",
                "--no-tags",
                "--no-write-fetch-head",
                &src,
                &sha,
            ],
        )
        .with_context(|| format!("submodule {shown}: its revision {sha} is not in the original"))?;
        if let Ok(url) = run(
            env,
            &from,
            &["--git-dir", &src, "config", "--get", "remote.origin.url"],
        ) {
            run_scratch(env, &sub, &["remote", "add", "origin", &url])?;
        }
        run_scratch(env, &sub, &["checkout", "-q", "--detach", &sha])?;
        init_submodules(env, &from, None, &sub, &sha, &format!("{shown}/"))?;
    }
    Ok(())
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
    // a name nobody else has: an existing directory is never touched
    let dir = tempfile::Builder::new()
        .prefix(&format!("commit-{}-", &commit[..12]))
        .tempdir_in(scratch_parent)?;
    let root = dir.path().to_path_buf();
    let co = Checkout {
        cwd: root.join(&cwd_rel),
        base: base.clone(),
        _dir: dir,
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
    let common = PathBuf::from(run(
        env,
        &top,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?);
    init_submodules(env, &common, Some(&top), &root, &commit, "")?;
    if !co.cwd.is_dir() {
        bail!("{} does not exist in {}", cwd_rel.display(), &commit[..12]);
    }
    Ok(co)
}
