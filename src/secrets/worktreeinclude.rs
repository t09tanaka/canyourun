//! The ignored files `.worktreeinclude` names (spec 22.3, 2026-11-10).
//!
//! `.worktreeinclude` is in `.gitignore` syntax and means "copy these ignored
//! files into a new worktree". Here it means "send these ignored files as
//! secrets". Git itself decides what matches, so the syntax is Git's:
//! `git ls-files --others --ignored --exclude-from=.worktreeinclude` lists the
//! untracked files the patterns match, and `git check-ignore` keeps those that
//! `.gitignore` really ignores. Untracked files that are not ignored belong to
//! the ordinary snapshot, and tracked files are commit content: neither is
//! taken here.
//!
//! Each file is returned as a path; [`super::plan::merge_specs`] turns them
//! into literal globs, so local-ci's capture reads exactly these files.

use super::plan::Limit;
use super::plan::MAX_FILES;
use super::{Result, SecretError};
use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::process::{Command, Stdio};

fn git(
    env: &BTreeMap<String, String>,
    root: &Path,
    args: &[&str],
    stdin: Option<Vec<u8>>,
) -> Result<(Option<i32>, Vec<u8>)> {
    let what = || format!("git {}", args.join(" "));
    let mut child = Command::new("git")
        .args(args)
        .current_dir(root)
        .env_clear()
        .envs(env)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SecretError::io(what(), e))?;
    let writer = stdin.map(|data| {
        let mut pipe = child.stdin.take().expect("piped");
        std::thread::spawn(move || {
            let _ = pipe.write_all(&data);
        })
    });
    let out = child
        .wait_with_output()
        .map_err(|e| SecretError::io(what(), e))?;
    if let Some(w) = writer {
        let _ = w.join();
    }
    Ok((out.status.code(), out.stdout))
}

/// The ignored files of the repository at `root` that its `.worktreeinclude`
/// matches, sorted. No `.worktreeinclude` (or none that matches) is an empty
/// list, never an error. A path that is not UTF-8, or more than
/// [`MAX_FILES`] files, is an error: a file that cannot be sent is not left
/// out silently.
pub fn ignored_matches(env: &BTreeMap<String, String>, root: &Path) -> Result<Vec<String>> {
    let list = root.join(".worktreeinclude");
    match fs::symlink_metadata(&list) {
        Ok(m) if m.file_type().is_file() => {}
        Ok(_) => {
            return Err(SecretError::Format(
                ".worktreeinclude must be a regular file".into(),
            ))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(SecretError::io(".worktreeinclude".to_string(), e)),
    }
    let (code, out) = git(
        env,
        root,
        &[
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-from=.worktreeinclude",
        ],
        None,
    )?;
    if code != Some(0) {
        return Err(SecretError::Format(
            "git could not list the files .worktreeinclude matches".into(),
        ));
    }
    let candidates: Vec<&[u8]> = out
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty() && !p.starts_with(b".git/"))
        .collect();
    if candidates.is_empty() {
        return Ok(vec![]);
    }
    let mut input = Vec::new();
    for c in &candidates {
        input.extend_from_slice(c);
        input.push(0);
    }
    let (code, ignored) = git(env, root, &["check-ignore", "-z", "--stdin"], Some(input))?;
    match code {
        Some(0) => {}
        Some(1) => return Ok(vec![]),
        _ => {
            return Err(SecretError::Format(
                "git could not check which files are ignored".into(),
            ))
        }
    }
    let mut paths = Vec::new();
    for p in ignored.split(|b| *b == 0).filter(|p| !p.is_empty()) {
        let s = String::from_utf8(p.to_vec())
            .map_err(|_| SecretError::NonUtf8Path(String::from_utf8_lossy(p).into_owned()))?;
        paths.push(s);
    }
    paths.sort();
    paths.dedup();
    if paths.len() > MAX_FILES {
        return Err(SecretError::Limit(Limit::TooManyFiles {
            count: paths.len(),
        }));
    }
    Ok(paths)
}
