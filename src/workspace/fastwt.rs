//! The `fast-worktree` command (spec 22.5), behind a trait so that everything
//! above it can be tested without the tool, and the real one can be tested
//! where it is installed.

use super::git::Env;
use super::plan::FwFailure;
use std::path::Path;
use std::process::Command;

/// Make and remove the worktree of an attempt in a canonical clone.
pub trait FastWorktree: Send + Sync {
    /// A worktree at `dest` on the new branch `branch`, starting at the commit
    /// `start`, with the project's dependencies prepared (its
    /// `.fast-worktree.toml`, `post_add` hooks included).
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure>;
    /// Remove the clean worktree at `path` (`pre_remove` hooks run).
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure>;
}

/// The installed `fast-worktree`, found on the PATH of `env`.
#[derive(Debug, Clone)]
pub struct CliFastWorktree {
    env: Env,
}

impl CliFastWorktree {
    pub fn new(env: Env) -> Self {
        CliFastWorktree { env }
    }

    fn run(&self, repo: &Path, args: &[&std::ffi::OsStr]) -> Result<(), FwFailure> {
        let out = Command::new("fast-worktree")
            .args(args)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    FwFailure::Missing
                } else {
                    FwFailure::Failed {
                        code: None,
                        stderr: e.to_string(),
                    }
                }
            })?;
        if out.status.success() {
            return Ok(());
        }
        // its report of what failed is on stderr, FAILED lines included
        let mut text = String::from_utf8_lossy(&out.stderr).into_owned();
        if text.trim().is_empty() {
            text = String::from_utf8_lossy(&out.stdout).into_owned();
        }
        Err(FwFailure::Failed {
            code: out.status.code(),
            stderr: text,
        })
    }
}

impl FastWorktree for CliFastWorktree {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        use std::ffi::OsStr;
        self.run(
            repo,
            &[
                OsStr::new("add"),
                dest.as_os_str(),
                OsStr::new("-b"),
                OsStr::new(branch),
                OsStr::new(start),
            ],
        )
    }

    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        use std::ffi::OsStr;
        self.run(repo, &[OsStr::new("remove"), path.as_os_str()])
    }
}
