//! Idle-sleep inhibition while a command runs (spec chapter 12, 15).
//!
//! The normal sleep stops the worker's capacity, so the supervisor holds an
//! inhibitor for as long as it runs. It is a helper child process tied to
//! the supervisor's life: when the supervisor dies the helper ends too, and
//! the inhibition is released.
//!
//! macOS: `caffeinate -i -w <supervisor pid>`. Linux: `systemd-inhibit`
//! (idle and sleep, blocking) around a `cat` that ends when its stdin pipe
//! closes. Where the helper is missing or refuses to start, the result is
//! [`Inhibit::Unavailable`] with the reason, and the caller records it and
//! carries on.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long the helper must stay alive for the inhibition to count as held.
const SETTLE: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub enum Inhibit {
    Held(SleepGuard),
    /// Not held; the reason is for the record.
    Unavailable(String),
}

/// Releases the inhibition when dropped.
#[derive(Debug)]
pub struct SleepGuard {
    child: Child,
    via: String,
}

impl SleepGuard {
    /// What holds the inhibition (the helper's program name).
    pub fn via(&self) -> &str {
        &self.via
    }
}

impl Drop for SleepGuard {
    fn drop(&mut self) {
        // Closing stdin ends the Linux helper; the kill covers the rest.
        drop(self.child.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The helper command for this OS: program and arguments. `watch_pid` is the
/// process whose life the macOS helper follows.
pub fn helper_argv(macos: bool, watch_pid: u32, who: &str, why: &str) -> Vec<String> {
    if macos {
        vec![
            "caffeinate".into(),
            "-i".into(),
            "-w".into(),
            watch_pid.to_string(),
        ]
    } else {
        vec![
            "systemd-inhibit".into(),
            "--what=idle:sleep".into(),
            "--mode=block".into(),
            format!("--who={who}"),
            format!("--why={why}"),
            "cat".into(),
        ]
    }
}

/// Hold an idle-sleep inhibition for the life of the returned guard.
pub fn acquire(who: &str, why: &str) -> Inhibit {
    acquire_with(&helper_argv(
        cfg!(target_os = "macos"),
        std::process::id(),
        who,
        why,
    ))
}

/// [`acquire`] with an explicit helper command (the first element is the
/// program). The helper counts as holding only if it is still running after a
/// short settle time.
pub fn acquire_with(argv: &[String]) -> Inhibit {
    acquire_within(argv, SETTLE)
}

/// [`acquire_with`] with the settle time given. A helper that exits returns
/// at once whatever the window, so only a helper that stays alive waits it out.
fn acquire_within(argv: &[String], settle: Duration) -> Inhibit {
    let Some((prog, args)) = argv.split_first() else {
        return Inhibit::Unavailable("no helper command".into());
    };
    let mut child = match Command::new(prog)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return Inhibit::Unavailable(format!("{prog}: {e}")),
    };
    let until = Instant::now() + settle;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Inhibit::Unavailable(format!("{prog} exited: {status}")),
            Ok(None) if Instant::now() >= until => {
                return Inhibit::Held(SleepGuard {
                    child,
                    via: prog.clone(),
                })
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Inhibit::Unavailable(format!("{prog}: {e}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn helper_commands_per_os() {
        assert_eq!(
            helper_argv(true, 77, "w", "why"),
            argv(&["caffeinate", "-i", "-w", "77"])
        );
        let linux = helper_argv(false, 77, "w", "running a command");
        assert_eq!(linux[0], "systemd-inhibit");
        assert!(linux.contains(&"--what=idle:sleep".to_string()));
        assert!(linux.contains(&"--why=running a command".to_string()));
        assert_eq!(linux.last().unwrap(), "cat");
    }

    #[test]
    fn a_missing_helper_is_unavailable_not_an_error() {
        match acquire_with(&argv(&["rtr-no-such-helper-binary"])) {
            Inhibit::Unavailable(why) => assert!(why.contains("rtr-no-such-helper-binary")),
            Inhibit::Held(_) => panic!("must not hold"),
        }
        assert!(matches!(acquire_with(&[]), Inhibit::Unavailable(_)));
    }

    #[test]
    fn a_helper_that_exits_at_once_is_unavailable() {
        // The window is only how long to wait for the exit to show; a helper
        // that is starved of CPU for longer than the production window would
        // otherwise be taken for one that stays.
        match acquire_within(&argv(&["false"]), Duration::from_secs(30)) {
            Inhibit::Unavailable(why) => assert!(why.contains("exited"), "{why}"),
            Inhibit::Held(_) => panic!("must not hold"),
        }
    }

    #[test]
    fn a_running_helper_is_held_and_released_on_drop() {
        let Inhibit::Held(guard) = acquire_with(&argv(&["cat"])) else {
            panic!("cat stays alive on an open stdin");
        };
        let pid = guard.child.id();
        assert_eq!(guard.via(), "cat");
        assert!(super::super::signal_pid(pid, 0));
        drop(guard);
        assert!(!super::super::signal_pid(pid, 0), "released");
    }

    #[test]
    fn acquire_on_this_os_is_held_or_explains_itself() {
        match acquire("remote-task-runner", "test") {
            Inhibit::Held(g) => assert!(!g.via().is_empty()),
            Inhibit::Unavailable(why) => assert!(!why.is_empty()),
        }
    }
}
