//! The `ssh` command line that starts a worker's `worker --stdio` bridge.
//!
//! The only user-controlled string is the destination, and it becomes exactly
//! one argv element after `--`. The remote command is a fixed binary path plus
//! fixed arguments, each single-quoted, because `ssh` joins its command words
//! with spaces and hands the result to the remote login shell. Port, identity,
//! ProxyCommand and ProxyJump (the tunnel to an ephemeral worker) belong to
//! `ssh_config`.

use thiserror::Error;

/// The arguments the remote binary is started with. Fixed; never user input.
pub const WORKER_STDIO_ARGS: [&str; 2] = ["worker", "--stdio"];

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SshError {
    #[error("invalid ssh destination {0:?}")]
    BadDestination(String),
    #[error("invalid remote binary {0:?}: use an absolute path or a bare command name")]
    BadBinary(String),
}

/// An ssh destination: `host`, `user@host`, an `ssh_config` alias or an
/// `ssh://` URI. It never starts with `-`, so it cannot be read as an option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget(String);

impl SshTarget {
    pub fn parse(s: &str) -> Result<Self, SshError> {
        let ok = !s.is_empty()
            && s.len() <= 255
            && !s.starts_with('-')
            && s.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || matches!(
                        b,
                        b'.' | b'_' | b'-' | b'@' | b':' | b'/' | b'[' | b']' | b'%' | b'+'
                    )
            });
        if ok {
            Ok(SshTarget(s.to_string()))
        } else {
            Err(SshError::BadDestination(s.to_string()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The remote command: a fixed binary and [`WORKER_STDIO_ARGS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCommand {
    binary: String,
}

impl RemoteCommand {
    /// `binary` is an absolute path or a bare command name. `~` and `$HOME` do
    /// not expand once quoted, so a path starting with `~` is refused.
    pub fn worker_stdio(binary: &str) -> Result<Self, SshError> {
        let shaped = binary.starts_with('/')
            || (!binary.is_empty() && !binary.contains('/') && !binary.starts_with('-'));
        let clean = !binary.chars().any(|c| c.is_control());
        if shaped && clean {
            Ok(RemoteCommand {
                binary: binary.to_string(),
            })
        } else {
            Err(SshError::BadBinary(binary.to_string()))
        }
    }

    /// The single string `ssh` sends as the remote command.
    pub fn shell_string(&self) -> String {
        let mut words = vec![quote(&self.binary)];
        words.extend(WORKER_STDIO_ARGS.iter().map(|a| quote(a)));
        words.join(" ")
    }
}

/// POSIX single-quote quoting: nothing inside is special to the shell.
pub fn quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
}

/// Seconds `ssh` waits for the TCP connection and the handshake. Without a
/// bound, a host that went away leaves the connect attempt hanging for the
/// system's TCP timeout (minutes).
pub const SSH_CONNECT_TIMEOUT_SECS: u32 = 15;
/// Seconds between keepalive probes on an established session, and how many
/// unanswered probes end it. A link that dies silently (a sleeping laptop, a
/// dropped NAT entry) is then noticed after about `interval * count` seconds
/// and the connection reports a disconnect, instead of looking alive forever.
pub const SSH_SERVER_ALIVE_INTERVAL_SECS: u32 = 15;
pub const SSH_SERVER_ALIVE_COUNT_MAX: u32 = 3;

/// The full `ssh` argv. `-T` because a pty would rewrite the binary stream;
/// `BatchMode` so a missing key fails instead of prompting on a pipe;
/// `ClearAllForwardings` so config-level forwards do not ride along;
/// `ConnectTimeout` and `ServerAlive*` bound a dead host and a dead link (see
/// the constants).
pub fn ssh_argv(target: &SshTarget, command: &RemoteCommand) -> Vec<String> {
    let mut argv = vec!["ssh".to_string(), "-T".to_string()];
    for option in [
        "BatchMode=yes".to_string(),
        "ClearAllForwardings=yes".to_string(),
        format!("ConnectTimeout={SSH_CONNECT_TIMEOUT_SECS}"),
        format!("ServerAliveInterval={SSH_SERVER_ALIVE_INTERVAL_SECS}"),
        format!("ServerAliveCountMax={SSH_SERVER_ALIVE_COUNT_MAX}"),
    ] {
        argv.push("-o".to_string());
        argv.push(option);
    }
    argv.push("--".to_string());
    argv.push(target.as_str().to_string());
    argv.push(command.shell_string());
    argv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destinations_that_look_like_options_or_shell_are_refused() {
        for bad in [
            "",
            "-oProxyCommand=touch /tmp/x",
            "-p",
            "host name",
            "host;rm",
            "host\n",
            "a'b",
            "a\"b",
            "$(x)",
            "`x`",
            "a|b",
            "a&b",
        ] {
            assert!(SshTarget::parse(bad).is_err(), "{bad:?}");
        }
        assert!(SshTarget::parse(&"a".repeat(256)).is_err());
        assert!(SshTarget::parse(&"a".repeat(255)).is_ok());
        for good in [
            "mini-a",
            "user@host.example",
            "ssh://u@h:2222",
            "[::1]",
            "colab_tunnel",
            "h%eth0",
            "a+b",
        ] {
            assert_eq!(SshTarget::parse(good).unwrap().as_str(), good);
        }
        assert!(SshError::BadDestination("x".into())
            .to_string()
            .contains("destination"));
    }

    #[test]
    fn binaries_are_absolute_or_bare() {
        for good in [
            "/opt/rtr/bin/remote-task-runner",
            "remote-task-runner",
            "/a b/c",
        ] {
            assert!(RemoteCommand::worker_stdio(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "~/bin/rtr",
            "bin/rtr",
            "./rtr",
            "-x",
            "/a\nb",
            "/a\tb",
            "rtr\0",
        ] {
            assert!(RemoteCommand::worker_stdio(bad).is_err(), "{bad:?}");
        }
        assert!(SshError::BadBinary("x".into())
            .to_string()
            .contains("remote binary"));
    }

    #[test]
    fn quoting_neutralises_every_shell_metacharacter() {
        assert_eq!(quote("abc"), "'abc'");
        assert_eq!(quote(""), "''");
        assert_eq!(quote("a b"), "'a b'");
        assert_eq!(quote("it's"), r"'it'\''s'");
        assert_eq!(quote("$(rm -rf /);`x`"), "'$(rm -rf /);`x`'");
        assert_eq!(quote("''"), r"''\'''\'''");
    }

    #[test]
    fn the_remote_command_is_fixed_and_quoted() {
        let c = RemoteCommand::worker_stdio("/opt/my tools/rtr").unwrap();
        assert_eq!(c.shell_string(), "'/opt/my tools/rtr' 'worker' '--stdio'");
        let c = RemoteCommand::worker_stdio("remote-task-runner").unwrap();
        assert_eq!(c.shell_string(), "'remote-task-runner' 'worker' '--stdio'");
    }

    #[test]
    fn argv_has_the_destination_once_after_the_option_terminator() {
        let t = SshTarget::parse("user@host").unwrap();
        let c = RemoteCommand::worker_stdio("/usr/local/bin/remote-task-runner").unwrap();
        let argv = ssh_argv(&t, &c);
        assert_eq!(
            argv,
            vec![
                "ssh",
                "-T",
                "-o",
                "BatchMode=yes",
                "-o",
                "ClearAllForwardings=yes",
                "-o",
                "ConnectTimeout=15",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=3",
                "--",
                "user@host",
                "'/usr/local/bin/remote-task-runner' 'worker' '--stdio'",
            ]
        );
        // Everything before `--` is fixed, whatever the destination.
        let other = ssh_argv(&SshTarget::parse("other").unwrap(), &c);
        assert_eq!(argv[..13], other[..13]);
        assert_eq!(argv.iter().filter(|a| a.contains("user@host")).count(), 1);
    }
}
