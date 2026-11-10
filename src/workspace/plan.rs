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

fn is_hex40(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
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
    if !is_hex40(commit) {
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
/// local worker, only in an absolute directory, and never where the runner
/// keeps its own state (a run there could rewrite what the runner trusts).
pub fn check_in_place(
    dir: &Path,
    cwd_relative: &Path,
    state_dir: &Path,
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

/// Why a `fast-worktree` call did not work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FwFailure {
    /// The command is not installed (or not on the PATH it was given).
    Missing,
    /// It ran and failed (`None`: killed by a signal).
    Failed { code: Option<i32>, stderr: String },
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
        let ok = |d: &str, c: &str, local| check_in_place(Path::new(d), Path::new(c), st, local);
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
}
