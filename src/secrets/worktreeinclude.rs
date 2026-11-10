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
//!
//! The repository root is what `git rev-parse --show-toplevel` says for the
//! directory asked about, so a capture started in a subdirectory reads the
//! same `.worktreeinclude` and gets the same root-relative paths as one
//! started at the root.
//!
//! [`preflight`] looks at the candidates (what `.worktreeinclude` matches and
//! what the declared globs match) before the capture runs, so a file that is
//! a link, too large, not UTF-8 or too many fails at once, with where it came
//! from, instead of after local-ci has read everything.

use super::plan::{check_declared, check_limits, check_path, Limit, MAX_FILES, MAX_FILE_BYTES};
use super::{Result, SecretError};
use globset::{Glob, GlobSetBuilder};
use local_ci::source::SecretSpec;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::{ErrorKind, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const LIST_NAME: &str = ".worktreeinclude";

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

/// The root of the repository that contains `dir`.
pub fn repo_root(env: &BTreeMap<String, String>, dir: &Path) -> Result<PathBuf> {
    let (code, out) = git(env, dir, &["rev-parse", "--show-toplevel"], None)?;
    let text = out.strip_suffix(b"\n").unwrap_or(&out);
    if code != Some(0) || text.is_empty() {
        return Err(SecretError::Format(
            "git could not find the repository root".into(),
        ));
    }
    Ok(PathBuf::from(OsStr::from_bytes(text)))
}

/// Does `.worktreeinclude` text name any pattern (a line that is not blank
/// and not a comment)?
pub fn has_patterns(text: &[u8]) -> bool {
    text.split(|b| *b == b'\n').any(|l| {
        let l = l.trim_ascii();
        !l.is_empty() && l[0] != b'#'
    })
}

/// Is there a `.worktreeinclude` with a pattern in `dir` itself? For a
/// directory that is not a Git repository: the patterns cannot be evaluated
/// there, but their presence means files were meant to be sent.
pub fn declares_patterns(dir: &Path) -> Result<bool> {
    let list = dir.join(LIST_NAME);
    match fs::symlink_metadata(&list) {
        Ok(m) if m.file_type().is_file() && m.len() <= 1024 * 1024 => {
            let bytes = fs::read(&list).map_err(|e| SecretError::io(LIST_NAME.to_string(), e))?;
            Ok(has_patterns(&bytes))
        }
        Ok(m) if m.file_type().is_file() => Ok(true),
        Ok(_) => Err(SecretError::Format(
            ".worktreeinclude must be a regular file".into(),
        )),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(SecretError::io(LIST_NAME.to_string(), e)),
    }
}

/// The ignored files of the repository containing `dir` that its
/// `.worktreeinclude` (at the repository root) matches, root-relative and
/// sorted. No `.worktreeinclude` (or none that matches) is an empty list,
/// never an error. A path that is not UTF-8, or more than [`MAX_FILES`]
/// files, is an error: a file that cannot be sent is not left out silently.
pub fn ignored_matches(env: &BTreeMap<String, String>, dir: &Path) -> Result<Vec<String>> {
    let root = &repo_root(env, dir)?;
    let list = root.join(LIST_NAME);
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

fn untracked_paths(
    env: &BTreeMap<String, String>,
    root: &Path,
    ignored: bool,
) -> Result<Vec<Vec<u8>>> {
    let mut args = vec!["ls-files", "-z", "--others", "--exclude-standard"];
    if ignored {
        args.push("--ignored");
    }
    let (code, out) = git(env, root, &args, None)?;
    if code != Some(0) {
        return Err(SecretError::Format(
            "git could not list the untracked files".into(),
        ));
    }
    Ok(out
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty() && !p.starts_with(b".git/"))
        .map(<[u8]>::to_vec)
        .collect())
}

/// Look at every file a capture would take as a secret before it runs: the
/// files `included` (what `.worktreeinclude` matched) and the untracked files
/// the `declared` globs match (a glob matches a path, or a directory above it,
/// as local-ci does). Each must have a safe UTF-8 path, be a regular file (not
/// a link) and not be too large; their number and total size are bounded. The
/// error says where the file came from (`.worktreeinclude` or `[source]
/// secret_files`) and the pattern.
pub fn preflight(
    env: &BTreeMap<String, String>,
    dir: &Path,
    declared: &[SecretSpec],
    included: &[String],
) -> Result<()> {
    check_declared(declared).map_err(SecretError::Limit)?;
    let root = repo_root(env, dir)?;
    // (path, origin, pattern); declared globs win over `.worktreeinclude`
    let mut found: Vec<(Vec<u8>, &'static str, String)> = Vec::new();
    if !declared.is_empty() {
        let mut sets = Vec::new();
        for spec in declared {
            let glob = Glob::new(&spec.glob).map_err(|_| SecretError::Preflight {
                origin: "[source] secret_files",
                pattern: spec.glob.clone(),
                reason: "not a valid glob".into(),
            })?;
            let mut b = GlobSetBuilder::new();
            b.add(glob);
            sets.push(b.build().map_err(|_| SecretError::Preflight {
                origin: "[source] secret_files",
                pattern: spec.glob.clone(),
                reason: "not a valid glob".into(),
            })?);
        }
        let mut all = untracked_paths(env, &root, false)?;
        all.extend(untracked_paths(env, &root, true)?);
        all.sort();
        all.dedup();
        for p in all {
            let path = Path::new(OsStr::from_bytes(&p));
            let hit = sets.iter().position(|set| {
                set.is_match(path) || path.ancestors().skip(1).any(|a| set.is_match(a))
            });
            if let Some(i) = hit {
                found.push((p, "[source] secret_files", declared[i].glob.clone()));
            }
        }
    }
    for p in included {
        if !found.iter().any(|f| f.0 == p.as_bytes()) {
            found.push((p.clone().into_bytes(), LIST_NAME, p.clone()));
        }
    }
    let mut sizes = Vec::with_capacity(found.len());
    for (path, origin, pattern) in &found {
        let fail = |reason: String| SecretError::Preflight {
            origin,
            pattern: pattern.clone(),
            reason,
        };
        let text = std::str::from_utf8(path).map_err(|_| {
            fail(format!(
                "path {:?} is not valid UTF-8",
                String::from_utf8_lossy(path)
            ))
        })?;
        check_path(text).map_err(|e| fail(format!("{text}: {e}")))?;
        match fs::symlink_metadata(root.join(text)) {
            Ok(m) if m.file_type().is_file() => {
                if m.len() > MAX_FILE_BYTES as u64 {
                    return Err(fail(format!(
                        "{text} is {} bytes; at most {MAX_FILE_BYTES} are allowed",
                        m.len()
                    )));
                }
                sizes.push((text.len(), m.len() as usize));
            }
            Ok(_) => return Err(fail(format!("{text} is a link or not a regular file"))),
            // gone since it was listed: the capture itself will see what is left
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(SecretError::io(format!("secret file {text}"), e)),
        }
    }
    let (origin, pattern) = found
        .first()
        .map_or(("", String::new()), |f| (f.1, f.2.clone()));
    check_limits(&sizes).map_err(|l| SecretError::Preflight {
        origin,
        pattern,
        reason: l.to_string(),
    })?;
    Ok(())
}
