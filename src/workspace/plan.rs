//! Pure decisions of the workspace layer (spec 22.5, chapter 6): where an
//! attempt's workspace lives, which input gets which kind of workspace, when
//! `exec --in-place` is allowed, what may be removed, and what a missing or
//! failing `fast-worktree` means to the caller. No I/O.

use crate::error::{ApiError, BlockedReason, ErrorCode};
use crate::ids::AttemptId;
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};

/// What is protected from a run in an `--in-place` workspace: nothing under
/// these (relative to the original worktree) may be written by the runner's
/// own steps, and the supervisor applies the same list to the run.
pub const IN_PLACE_PROTECTED: &[&str] = &[".git"];

/// How much of a tool's stderr goes into an error message.
const MAX_STDERR_CHARS: usize = 2000;

/// Where the attempt's workspace is: one directory per attempt, named by the
/// attempt id (a ULID with a prefix, so it is a single safe path component).
pub fn workspace_dir(workspaces_root: &Path, attempt: &AttemptId) -> PathBuf {
    workspaces_root.join(attempt.as_str())
}

/// The branch of an attempt's worktree in the canonical clone.
pub fn branch_name(attempt: &AttemptId) -> String {
    format!("rtr/{}", attempt.as_str())
}

/// The directory name of the canonical clone of a repository: derived from the
/// repository id (`github:owner/name`, `root:<sha>`), which has characters a
/// path component should not.
pub fn clone_dir_name(repository_id: &str) -> String {
    let h = Sha256::digest(repository_id.as_bytes());
    let hex: String = h.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("r-{hex}")
}

/// What kind of workspace an input needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Flavor {
    /// A Git worktree of the canonical clone at `commit`, then the source's
    /// files laid over it.
    Git {
        commit: String,
        repository_id: String,
    },
    /// A plain directory the source is unpacked into (a directory snapshot).
    Directory,
}

/// A Git commit id: 40 hex digits (SHA-1) or 64 (SHA-256 repositories).
pub fn is_commit_id(s: &str) -> bool {
    matches!(s.len(), 40 | 64) && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A source with an execution commit is a Git source and needs the repository
/// it names; one without is a directory snapshot.
pub fn flavor_of(
    execution_commit: Option<&str>,
    repository_id: Option<&str>,
) -> Result<Flavor, ApiError> {
    let Some(commit) = execution_commit else {
        return Ok(Flavor::Directory);
    };
    if !is_commit_id(commit) {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!("the execution commit {commit:?} is not a commit id"),
        ));
    }
    match repository_id {
        Some(r) if !r.is_empty() => Ok(Flavor::Git {
            commit: commit.to_string(),
            repository_id: r.to_string(),
        }),
        _ => Err(ApiError::new(
            ErrorCode::InvalidInput,
            "a Git source without a repository id cannot get a canonical clone",
        )),
    }
}

/// A working directory relative to the workspace root: only plain components.
pub fn check_cwd_relative(p: &Path) -> Result<(), ApiError> {
    if p.components().all(|c| matches!(c, Component::Normal(_))) {
        Ok(())
    } else {
        Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!("the working directory {:?} is not a plain relative path", p),
        ))
    }
}

/// Whether `a` and `b` are the same directory or one holds the other
/// (lexically; callers pass canonical paths).
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// `exec --in-place` runs in the user's own worktree: only on the parent's
/// local worker, only in an absolute directory that is neither the file
/// system root nor the user's home (`home`, canonical, from the env map the
/// caller holds), and never where the runner keeps its own state (a run there
/// could rewrite what the runner trusts).
pub fn check_in_place(
    dir: &Path,
    cwd_relative: &Path,
    state_dir: &Path,
    home: Option<&Path>,
    parent_local: bool,
) -> Result<(), ApiError> {
    if !parent_local {
        return Err(ApiError::blocked(
            BlockedReason::NoCompatibleWorker,
            "--in-place runs only on the parent's local worker",
        ));
    }
    if !dir.is_absolute() {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!("--in-place needs an absolute directory, got {dir:?}"),
        ));
    }
    if dir.parent().is_none() || home == Some(dir) {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!("--in-place needs a project directory, not {dir:?}"),
        ));
    }
    check_cwd_relative(cwd_relative)?;
    if overlaps(dir, state_dir) {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!(
                "--in-place directory {dir:?} overlaps the runner's state directory {state_dir:?}"
            ),
        ));
    }
    Ok(())
}

/// Only a directory the runner made for an attempt may be removed: a direct
/// child of the workspaces root, named by an attempt id.
pub fn check_owned(workspaces_root: &Path, path: &Path) -> Result<AttemptId, ApiError> {
    let refuse = || {
        ApiError::new(
            ErrorCode::InvalidInput,
            format!("{path:?} is not an attempt workspace under {workspaces_root:?}"),
        )
    };
    if path.parent() != Some(workspaces_root) {
        return Err(refuse());
    }
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(refuse)?;
    AttemptId::parse(name).map_err(|_| refuse())
}

/// The clone a worktree belongs to, from the content of the worktree's `.git`
/// file (`gitdir: <clone>/.git/worktrees/<name>`): `None` unless it has
/// exactly that shape with an absolute path of plain components.
pub fn gitdir_clone(content: &str) -> Option<PathBuf> {
    let line = content.lines().next()?;
    let p = Path::new(line.strip_prefix("gitdir: ")?.trim());
    let plain = p
        .components()
        .all(|c| matches!(c, Component::RootDir | Component::Normal(_)));
    if !p.is_absolute() || !plain {
        return None;
    }
    let admin = p.parent()?;
    let dotgit = admin.parent()?;
    if admin.file_name()? != "worktrees" || dotgit.file_name()? != ".git" {
        return None;
    }
    Some(dotgit.parent()?.to_path_buf())
}

/// The record of a worktree workspace says where its clone is and which
/// branch to drop; both are used to run Git, so a record that does not match
/// what [`Workspaces::create`] makes (a clone directly under the clones root,
/// the attempt's own branch) is refused.
pub fn check_worktree_record(
    clones_root: &Path,
    clone: &Path,
    branch: &str,
    attempt: &AttemptId,
) -> Result<(), ApiError> {
    let is_clone = clone.parent() == Some(clones_root)
        && clone
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("r-"));
    if !is_clone || branch != branch_name(attempt) {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            format!(
                "the record of {attempt} (clone {clone:?}, branch {branch:?}) is not one made here"
            ),
        ));
    }
    Ok(())
}

/// Whether `git worktree list --porcelain` output lists a worktree of
/// `attempt` directly under one of `roots` (the workspaces root as written and
/// as resolved: Git prints whichever it recorded).
pub fn registered_for(list: &str, roots: &[PathBuf], attempt: &AttemptId) -> bool {
    list.lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .map(Path::new)
        .any(|p| {
            p.file_name().is_some_and(|n| n == attempt.as_str())
                && p.parent().is_some_and(|d| roots.iter().any(|r| r == d))
        })
}

/// What an entry of the workspaces directory is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// An attempt's workspace.
    Attempt(AttemptId),
    /// A scratch directory of a create (`.staging.*`, `.clone.*`).
    Scratch,
    /// Not ours: left alone.
    Foreign,
}

pub fn classify_entry(name: &str) -> Entry {
    if name.starts_with(".staging.") || name.starts_with(".clone.") {
        Entry::Scratch
    } else {
        AttemptId::parse(name).map_or(Entry::Foreign, Entry::Attempt)
    }
}

/// The last `cap` bytes of a stream: a tool's output is kept in bounded
/// memory, the end being where its failure is reported.
#[derive(Debug)]
pub struct Tail {
    cap: usize,
    buf: Vec<u8>,
    cut: bool,
}

impl Tail {
    pub fn new(cap: usize) -> Self {
        Tail {
            cap,
            buf: Vec::new(),
            cut: false,
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        if self.buf.len() > self.cap {
            let over = self.buf.len() - self.cap;
            self.buf.drain(..over);
            self.cut = true;
        }
    }

    pub fn text(&self) -> String {
        let t = String::from_utf8_lossy(&self.buf);
        if self.cut {
            format!("[earlier output dropped] {t}")
        } else {
            t.into_owned()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// Why a `fast-worktree` call did not work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FwFailure {
    /// The command is not installed (or not on the PATH it was given).
    Missing,
    /// It ran and failed (`None`: killed by a signal).
    Failed { code: Option<i32>, stderr: String },
    /// It ran longer than the caller allowed and was killed (its whole
    /// process group); `stderr` is the tail of what it had said.
    TimedOut { secs: u64, stderr: String },
    /// The environment map the caller gave lacks something it cannot run
    /// without (the process's own environment is never consulted).
    NoEnv { missing: Vec<String> },
}

/// A missing or failing `fast-worktree` is an environment mismatch: the
/// worker cannot prepare a workspace, and a retry on the same worker would not
/// help.
pub fn fw_error(op: &str, f: &FwFailure) -> ApiError {
    let msg = match f {
        FwFailure::Missing => format!(
            "fast-worktree {op}: the command is not installed on this worker (cargo install fast-worktree)"
        ),
        FwFailure::Failed { code, stderr } => {
            let tail: String = stderr.trim().chars().take(MAX_STDERR_CHARS).collect();
            let code = code.map_or("signal".to_string(), |c| c.to_string());
            format!("fast-worktree {op} failed ({code}): {tail}")
        }
        FwFailure::TimedOut { secs, stderr } => {
            let tail: String = stderr.trim().chars().take(MAX_STDERR_CHARS).collect();
            format!("fast-worktree {op} timed out after {secs}s: {tail}")
        }
        FwFailure::NoEnv { missing } => format!(
            "fast-worktree {op}: the environment given to it has no {} (the caller passes HOME, PATH and whatever the project's hooks read, such as NPM_CONFIG_* and PORTOOL_*)",
            missing.join(", ")
        ),
    };
    ApiError::blocked(BlockedReason::EnvironmentMismatch, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: &str = "0123456789abcdef0123456789abcdef01234567";

    fn att() -> AttemptId {
        AttemptId::parse("att_01J0000000000000000000000A").unwrap()
    }

    #[test]
    fn names_follow_the_attempt() {
        assert_eq!(
            workspace_dir(Path::new("/s/workspaces"), &att()),
            PathBuf::from("/s/workspaces/att_01J0000000000000000000000A")
        );
        assert_eq!(branch_name(&att()), "rtr/att_01J0000000000000000000000A");
    }

    #[test]
    fn clone_dir_names_are_stable_and_distinct() {
        let a = clone_dir_name("github:o/n");
        assert_eq!(a, clone_dir_name("github:o/n"));
        assert_ne!(a, clone_dir_name("github:o/m"));
        assert!(a.starts_with("r-") && a.len() == 2 + 16);
        // sha256("github:o/n") starts with these bytes: pins the derivation
        let h = Sha256::digest(b"github:o/n");
        let want: String = h[..8].iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(a, format!("r-{want}"));
    }

    #[test]
    fn flavor_follows_the_execution_commit() {
        assert_eq!(flavor_of(None, None).unwrap(), Flavor::Directory);
        assert_eq!(flavor_of(None, Some("x")).unwrap(), Flavor::Directory);
        assert_eq!(
            flavor_of(Some(C), Some("root:abc")).unwrap(),
            Flavor::Git {
                commit: C.into(),
                repository_id: "root:abc".into()
            }
        );
        for bad in ["", "abc", &C.to_uppercase(), &format!("{C}0"), &C[..39]] {
            let e = flavor_of(Some(bad), Some("r")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
        // a SHA-256 repository's commit id
        let long = "ab".repeat(32);
        assert!(flavor_of(Some(&long), Some("r")).is_ok());
        assert!(flavor_of(Some(&long[..63]), Some("r")).is_err());
        assert!(flavor_of(Some(&format!("{long}0")), Some("r")).is_err());
        let g = format!("g{}", &C[1..]);
        assert!(flavor_of(Some(&g), Some("r")).is_err());
        for none in [None, Some("")] {
            let e = flavor_of(Some(C), none).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn cwd_relative_has_plain_components_only() {
        for ok in ["", "a", "a/b c"] {
            assert!(check_cwd_relative(Path::new(ok)).is_ok(), "{ok}");
        }
        for bad in ["/a", "..", "a/../b", "./a", "a/.."] {
            let e = check_cwd_relative(Path::new(bad)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
    }

    #[test]
    fn in_place_rules() {
        let st = Path::new("/home/u/.state");
        let home = Path::new("/home/u");
        let ok = |d: &str, c: &str, local| {
            check_in_place(
                Path::new(d),
                Path::new(c),
                st,
                Some(Path::new("/home/h")),
                local,
            )
        };
        // the home directory and the root are not project directories
        let e = check_in_place(home, Path::new(""), Path::new("/s"), Some(home), true).unwrap_err();
        assert!(e.message.contains("project directory"), "{}", e.message);
        assert!(check_in_place(home, Path::new(""), Path::new("/s"), None, true).is_ok());
        let e =
            check_in_place(Path::new("/"), Path::new(""), Path::new("/s"), None, true).unwrap_err();
        assert!(e.message.contains("project directory"), "{}", e.message);
        assert!(ok("/home/u/work", "pkg", true).is_ok());
        assert!(ok("/home/u/work", "", true).is_ok());
        let e = ok("/home/u/work", "", false).unwrap_err();
        assert_eq!(e.code, ErrorCode::Blocked);
        assert_eq!(e.blocked_reason, Some(BlockedReason::NoCompatibleWorker));
        assert_eq!(
            ok("work", "", true).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            ok("/home/u/work", "../x", true).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        for d in [
            "/home/u/.state",
            "/home/u/.state/workspaces/a",
            "/home/u",
            "/",
        ] {
            let e = ok(d, "", true).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{d}");
        }
        assert!(ok("/home/u/.state2", "", true).is_ok());
        assert_eq!(IN_PLACE_PROTECTED, &[".git"]);
    }

    #[test]
    fn only_attempt_directories_are_owned() {
        let root = Path::new("/s/workspaces");
        let name = "att_01J0000000000000000000000A";
        assert_eq!(check_owned(root, &root.join(name)).unwrap(), att());
        for bad in [
            root.to_path_buf(),
            root.join("other"),
            root.join(name).join("sub"),
            PathBuf::from("/elsewhere").join(name),
            root.join(format!("{name}/..")),
            Path::new("/").to_path_buf(),
        ] {
            let e = check_owned(root, &bad).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad:?}");
        }
    }

    #[test]
    fn missing_or_failing_fast_worktree_is_an_environment_mismatch() {
        let m = fw_error("add", &FwFailure::Missing);
        assert_eq!(m.code, ErrorCode::Blocked);
        assert_eq!(m.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(m.message.contains("add") && m.message.contains("not installed"));
        let f = fw_error(
            "remove",
            &FwFailure::Failed {
                code: Some(3),
                stderr: "  boom\n".into(),
            },
        );
        assert_eq!(f.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(
            f.message.contains("remove failed (3): boom"),
            "{}",
            f.message
        );
        let t = fw_error(
            "add",
            &FwFailure::TimedOut {
                secs: 30,
                stderr: " hung\n".into(),
            },
        );
        assert_eq!(t.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(
            t.message.contains("add timed out after 30s: hung"),
            "{}",
            t.message
        );
        let n = fw_error(
            "add",
            &FwFailure::NoEnv {
                missing: vec!["HOME".into(), "PATH".into()],
            },
        );
        assert_eq!(n.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(n.message.contains("no HOME, PATH"), "{}", n.message);
        let s = fw_error(
            "add",
            &FwFailure::Failed {
                code: None,
                stderr: "x".repeat(MAX_STDERR_CHARS + 50),
            },
        );
        assert!(s.message.contains("(signal)"));
        assert_eq!(
            s.message.chars().filter(|c| *c == 'x').count(),
            MAX_STDERR_CHARS
        );
    }

    #[test]
    fn a_worktree_file_names_its_clone() {
        let g = |c: &str| gitdir_clone(c);
        assert_eq!(
            g("gitdir: /s/clones/r-1/.git/worktrees/att_x\n"),
            Some(PathBuf::from("/s/clones/r-1"))
        );
        for bad in [
            "",
            "ref: x",
            "gitdir: rel/.git/worktrees/a",
            "gitdir: /s/../x/.git/worktrees/a",
            "gitdir: /s/c/.git/other/a",
            "gitdir: /s/c/notgit/worktrees/a",
            "gitdir: /worktrees",
            "gitdir: /",
        ] {
            assert_eq!(g(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_worktree_record_must_be_one_made_here() {
        let cl = Path::new("/s/clones");
        let a = att();
        assert!(check_worktree_record(cl, &cl.join("r-ab"), &branch_name(&a), &a).is_ok());
        for (clone, branch) in [
            (PathBuf::from("/elsewhere/r-ab"), branch_name(&a)),
            (cl.join("x/r-ab"), branch_name(&a)),
            (cl.join("other"), branch_name(&a)),
            (cl.to_path_buf(), branch_name(&a)),
            (cl.join("r-ab"), "main".to_string()),
            (
                cl.join("r-ab"),
                "rtr/att_01J0000000000000000000000B".to_string(),
            ),
        ] {
            let e = check_worktree_record(cl, &clone, &branch, &a).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{clone:?} {branch}");
        }
    }

    #[test]
    fn registrations_are_matched_by_directory_and_attempt() {
        let a = att();
        let roots = [PathBuf::from("/s/ws"), PathBuf::from("/private/s/ws")];
        let list =
            format!("worktree /s/clones/r-1\nHEAD abc\n\nworktree /private/s/ws/{a}\nHEAD abc\n");
        assert!(registered_for(&list, &roots, &a));
        assert!(!registered_for(&list, &roots[..1], &a));
        let other = AttemptId::parse("att_01J0000000000000000000000B").unwrap();
        assert!(!registered_for(&list, &roots, &other));
        assert!(!registered_for(&format!("worktree /x/{a}\n"), &roots, &a));
        assert!(!registered_for("", &roots, &a));
    }

    #[test]
    fn entries_of_the_workspaces_directory() {
        assert_eq!(
            classify_entry("att_01J0000000000000000000000A"),
            Entry::Attempt(att())
        );
        assert_eq!(classify_entry(".staging.x1"), Entry::Scratch);
        assert_eq!(classify_entry(".clone.x1"), Entry::Scratch);
        assert_eq!(classify_entry("notes.txt"), Entry::Foreign);
        assert_eq!(classify_entry("att_short"), Entry::Foreign);
    }

    #[test]
    fn a_tail_keeps_the_end() {
        let mut exact = Tail::new(5);
        exact.push(b"12345");
        assert_eq!(exact.text(), "12345");
        let mut t = Tail::new(5);
        assert!(t.is_empty());
        t.push(b"abc");
        assert_eq!(t.text(), "abc");
        assert!(!t.is_empty());
        t.push(b"defgh");
        assert_eq!(t.text(), "[earlier output dropped] defgh");
        t.push(b"i");
        assert_eq!(t.text(), "[earlier output dropped] efghi");
    }
}
