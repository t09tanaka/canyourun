//! The supervisor as its own process: `remote-task-runner supervise --spec F`.
//!
//! The agent starts it with [`spawn_detached`] and may then die, restart or
//! lose its SSH session: the supervisor is in its own session, ignores
//! hang-ups, and talks only to the worker database.

use super::error::SupervisorError;
use super::lock::SupervisorLock;
use super::run::{run, Report};
use super::spec::SuperviseSpec;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};

/// Exit codes of the supervisor process.
pub mod code {
    /// The attempt is settled (whatever its outcome).
    pub const SETTLED: i32 = 0;
    /// The spec could not be used.
    pub const BAD_SPEC: i32 = 2;
    /// The processes could not be confirmed stopped (after bounded retries);
    /// the attempt is unresolved and a `supervisor_unresolved` event says why.
    /// Not retried here: the parent's reconcile sees the supervisor lock free
    /// and the attempt non-terminal, starts `supervise` again, and recovery
    /// redoes the stop (S10/S12).
    pub const UNRESOLVED: i32 = 3;
    /// Another supervisor holds the attempt.
    pub const ALIVE: i32 = 4;
    /// Any other failure.
    pub const FAILED: i32 = 1;
}

/// Start `exe supervise --spec <spec>` detached from the caller: own session
/// and process group, null stdio.
pub fn spawn_detached(exe: &Path, spec: &Path) -> std::io::Result<Child> {
    let mut cmd = Command::new(exe);
    cmd.arg("supervise")
        .arg("--spec")
        .arg(spec)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: only async-signal-safe calls between fork and exec.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    cmd.spawn()
}

extern "C" fn ignore_hangup(_: libc::c_int) {}

/// Run the supervisor for the spec file and return the process exit code.
pub fn main(spec_path: &Path) -> i32 {
    // Not tied to a terminal or an SSH session that goes away.
    // SAFETY: setting dispositions with valid signal numbers.
    unsafe {
        // Leave the caller's session and process group. Fails harmlessly when
        // this process already leads its own (spawn_detached does that).
        libc::setsid();
        // A handler, not SIG_IGN: an ignored signal stays ignored across exec
        // and would reach the user's command; a handler is reset to the
        // default by exec, so the command keeps the default SIGHUP.
        libc::signal(
            libc::SIGHUP,
            ignore_hangup as extern "C" fn(libc::c_int) as usize,
        );
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let spec = match SuperviseSpec::from_file(spec_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("supervise: {e}");
            return code::BAD_SPEC;
        }
    };
    match supervise(&spec) {
        Ok(report) => {
            if let Some(why) = &report.unresolved {
                eprintln!("supervise: unresolved: {why}");
                code::UNRESOLVED
            } else {
                code::SETTLED
            }
        }
        Err(SupervisorError::Alive(_)) => code::ALIVE,
        Err(e @ SupervisorError::Spec(_)) => {
            eprintln!("supervise: {e}");
            code::BAD_SPEC
        }
        Err(e) => {
            eprintln!("supervise: {e}");
            code::FAILED
        }
    }
}

/// Take the attempt's supervisor lock, then run (or recover) it.
pub fn supervise(spec: &SuperviseSpec) -> Result<Report, SupervisorError> {
    let Some(_lock) = SupervisorLock::try_acquire(&spec.state_dir, &spec.attempt_id)? else {
        return Err(SupervisorError::Alive(spec.attempt_id.to_string()));
    };
    run(spec)
}
