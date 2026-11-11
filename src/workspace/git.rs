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

/// The oldest Git a worker runs with: a bundle is unpacked with
/// `git fetch --atomic` (all refs or none), which came with 2.31.
const MIN_GIT: (u32, u32) = (2, 31);

/// The major and minor number in what `git --version` prints (`git version
/// 2.39.5 (Apple Git-154)`, `git version 2.43.0.windows.1`, `git version
/// 2.50.0-rc1`). `None`: this is not what Git prints.
fn parse_version(text: &str) -> Option<(u32, u32)> {
    let word = text
        .strip_prefix("git version ")?
        .split_whitespace()
        .next()?;
    let mut parts = word.split('.');
    let major = parts.next()?.parse().ok()?;
    // (the minor number may be the last part, with a suffix: `50-rc1`)
    let minor = parts.next()?;
    let digits = minor.bytes().take_while(u8::is_ascii_digit).count();
    let minor = minor[..digits].parse().ok()?;
    Some((major, minor))
}

/// Is `found` a Git this worker can run with?
fn version_ok(found: (u32, u32)) -> bool {
    found >= MIN_GIT
}

/// Ask the Git that `env` finds for its version, and refuse one that is not
/// there, does not answer, or is older than [`MIN_GIT`].
pub(super) fn check_version(env: &Env) -> Result<()> {
    let out = Command::new("git")
        .arg("--version")
        .current_dir("/")
        .env_clear()
        .envs(env)
        .stdin(std::process::Stdio::null())
        .output()
        .context("cannot run git")?;
    let text = String::from_utf8_lossy(&out.stdout);
    let text = text.trim();
    if !out.status.success() {
        bail!(
            "git --version failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let Some(found) = parse_version(text) else {
        bail!("git --version printed {text:?}, which names no version");
    };
    if !version_ok(found) {
        bail!(
            "Git {}.{} is too old: {}.{} or newer is needed (git fetch --atomic)",
            found.0,
            found.1,
            MIN_GIT.0,
            MIN_GIT.1
        );
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_version_is_read_from_what_git_prints() {
        for (text, want) in [
            ("git version 2.31.0", Some((2, 31))),
            ("git version 2.39.5 (Apple Git-154)", Some((2, 39))),
            ("git version 2.43.0.windows.1", Some((2, 43))),
            ("git version 2.50.0-rc1", Some((2, 50))),
            ("git version 2.50-rc1", Some((2, 50))),
            ("git version 3.0", Some((3, 0))),
            ("git version 10.2.1\n", Some((10, 2))),
            ("git version 1.8.3.1", Some((1, 8))),
            ("", None),
            ("git version", None),
            ("git version ", None),
            ("git version 2", None),
            ("git version 2.", None),
            ("git version 2.x", None),
            ("git version .31", None),
            ("git version two.31", None),
            ("git version -2.31", None),
            ("git version 2.-31", None),
            ("version 2.31.0", None),
            ("Git version 2.31.0", None),
            ("git version2.31.0", None),
            ("hg version 2.31.0", None),
            ("usage: git [--version]", None),
            ("git version 99999999999.1", None),
            ("git version 2.99999999999", None),
        ] {
            assert_eq!(parse_version(text), want, "{text:?}");
        }
    }

    #[test]
    fn a_git_older_than_the_atomic_fetch_is_refused() {
        assert_eq!(MIN_GIT, (2, 31));
        for (found, ok) in [
            ((2, 31), true),
            ((2, 32), true),
            ((2, 56), true),
            ((3, 0), true),
            ((10, 0), true),
            ((2, 30), false),
            ((2, 0), false),
            ((1, 99), false),
            ((0, 0), false),
        ] {
            assert_eq!(version_ok(found), ok, "{found:?}");
        }
    }
}
