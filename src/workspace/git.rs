//! Running Git with an explicit environment (the process's own is never read).

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

pub(super) type Env = BTreeMap<String, String>;

fn run_with(env: &Env, cwd: &Path, extra: &[(&str, &OsStr)], args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(["-c", "protocol.file.allow=always"])
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .env("GIT_TERMINAL_PROMPT", "0");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let out = cmd.output().context("cannot run git")?;
    if !out.status.success() {
        bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_string())
}

pub(super) fn run(env: &Env, cwd: &Path, args: &[&str]) -> Result<String> {
    run_with(env, cwd, &[], args)
}

/// Whether the command succeeds (its output is not wanted).
pub(super) fn ok(env: &Env, cwd: &Path, args: &[&str]) -> bool {
    run(env, cwd, args).is_ok()
}

/// Git inside the worktree `dir` and nowhere else: `GIT_DIR` and
/// `GIT_WORK_TREE` are set explicitly (so a `.git` that is missing or wrong is
/// an error, never a search upward into whatever repository holds the state
/// directory), and discovery may not climb above `ceiling` either.
pub(super) fn run_in_worktree(
    env: &Env,
    dir: &Path,
    ceiling: &Path,
    args: &[&str],
) -> Result<String> {
    let git_dir = dir.join(".git");
    run_with(
        env,
        dir,
        &[
            ("GIT_DIR", git_dir.as_os_str()),
            ("GIT_WORK_TREE", dir.as_os_str()),
            ("GIT_CEILING_DIRECTORIES", ceiling.as_os_str()),
        ],
        args,
    )
}
