//! The `fast-worktree` command (spec 22.5), behind a trait so that everything
//! above it can be tested without the tool, and the real one can be tested
//! where it is installed.

use super::git::Env;
use super::plan::{FwFailure, Tail};
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Make and remove the worktree of an attempt in a canonical clone.
pub trait FastWorktree: Send + Sync {
    /// A worktree at `dest` on the new branch `branch`, starting at the commit
    /// `start`, with the project's dependencies prepared (its
    /// `.fast-worktree.toml`, `post_add` hooks included).
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure>;
    /// Remove the clean worktree at `path` (`pre_remove` hooks run).
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure>;
}

/// How long one call may run unless the caller says otherwise: a `post_add`
/// hook installing dependencies is the slow part.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// How much of a call's output is kept: the end of each stream.
const KEEP_BYTES: usize = 64 * 1024;

/// How long to wait for the output streams to end once the process is gone
/// (a hook's daemon that outlived it may still hold them open).
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The installed `fast-worktree`, found on the PATH of `env`.
///
/// `env` is the whole environment the tool and the project's hooks run with
/// (the process's own is never read): the caller, the supervisor, passes what
/// they need, `HOME` and `PATH` always, and `NPM_CONFIG_*`, `PORTOOL_*` and the
/// like for the project's hooks. Without `HOME` or `PATH` a call fails with
/// [`FwFailure::NoEnv`] saying so, instead of a hook failing later for want
/// of it.
#[derive(Debug, Clone)]
pub struct CliFastWorktree {
    env: Env,
    timeout: Duration,
}

impl CliFastWorktree {
    /// With [`DEFAULT_TIMEOUT`] per call.
    pub fn new(env: Env) -> Self {
        CliFastWorktree {
            env,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// With `timeout` per call: past it the tool's whole process group is
    /// killed and the call is [`FwFailure::TimedOut`].
    pub fn with_timeout(env: Env, timeout: Duration) -> Self {
        CliFastWorktree { env, timeout }
    }

    fn run(&self, repo: &Path, args: &[&std::ffi::OsStr]) -> Result<(), FwFailure> {
        let missing: Vec<String> = ["HOME", "PATH"]
            .into_iter()
            .filter(|k| !self.env.contains_key(*k))
            .map(str::to_string)
            .collect();
        if !missing.is_empty() {
            return Err(FwFailure::NoEnv { missing });
        }
        let mut child = Command::new("fast-worktree")
            .args(args)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // its own group: a timeout kills the hooks it started too
            .process_group(0)
            .spawn()
            .map_err(|e| {
                // (a missing working directory also reads as NotFound)
                if e.kind() == std::io::ErrorKind::NotFound && repo.is_dir() {
                    FwFailure::Missing
                } else {
                    FwFailure::Failed {
                        code: None,
                        stderr: e.to_string(),
                    }
                }
            })?;
        let pgid = child.id() as libc::pid_t;
        let out = drain(child.stdout.take());
        let err = drain(child.stderr.take());
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break Some(s),
                Ok(None) if started.elapsed() >= self.timeout => break None,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => {
                    kill_group(pgid);
                    let _ = child.wait();
                    return Err(FwFailure::Failed {
                        code: None,
                        stderr: e.to_string(),
                    });
                }
            }
        };
        if status.is_none() {
            kill_group(pgid);
            let _ = child.wait();
        }
        let (out, err) = (out.finish(), err.finish());
        let Some(status) = status else {
            return Err(FwFailure::TimedOut {
                secs: self.timeout.as_secs(),
                stderr: if err.trim().is_empty() { out } else { err },
            });
        };
        if status.success() {
            return Ok(());
        }
        // its report of what failed is on stderr, FAILED lines included
        Err(FwFailure::Failed {
            code: status.code(),
            stderr: if err.trim().is_empty() { out } else { err },
        })
    }
}

fn kill_group(pgid: libc::pid_t) {
    // Group 0 is our own, 1 is init; killpg with either would hit far more
    // than the child we made.
    if pgid <= 1 {
        return;
    }
    // SAFETY: killpg only sends a signal to the group we made
    unsafe {
        libc::killpg(pgid, libc::SIGKILL);
    }
}

/// A stream being read on its own thread into a [`Tail`].
struct Drain {
    tail: Arc<Mutex<Tail>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn drain<R: Read + Send + 'static>(stream: Option<R>) -> Drain {
    let tail = Arc::new(Mutex::new(Tail::new(KEEP_BYTES)));
    let t = tail.clone();
    let thread = stream.map(|mut s| {
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = s.read(&mut buf) {
                if n == 0 {
                    break;
                }
                t.lock().unwrap_or_else(|e| e.into_inner()).push(&buf[..n]);
            }
        })
    });
    Drain { tail, thread }
}

impl Drain {
    /// What was read; waits a little for the stream to end, and leaves the
    /// reader behind if something still holds it open.
    fn finish(self) -> String {
        if let Some(t) = self.thread {
            let until = Instant::now() + DRAIN_GRACE;
            while !t.is_finished() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let text = self.tail.lock().unwrap_or_else(|e| e.into_inner()).text();
        text
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
