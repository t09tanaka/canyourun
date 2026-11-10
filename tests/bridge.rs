//! `worker --stdio` as a real process: bytes pass through to the agent's Unix
//! socket, stdout carries nothing else, and a missing agent is an explicit error.

#[path = "proto_common/mod.rs"]
mod common;

use common::*;
use remote_task_runner::ids::*;
use remote_task_runner::protocol::ssh::{ssh_argv, RemoteCommand, SshTarget};
use remote_task_runner::protocol::transport::{listen_unix, spawn_child, Io};
use remote_task_runner::protocol::{accept, connect};
use std::process::Stdio;
use std::sync::Arc;

/// Unix socket paths are limited to about 100 bytes; the default temp
/// directory can exceed that on macOS.
fn short_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("rtr")
        .tempdir_in("/tmp")
        .unwrap()
}

const BIN: &str = env!("CARGO_BIN_EXE_remote-task-runner");

/// An agent on `<home>/.remote-task-runner/agent.sock` that answers ack requests.
fn start_agent(home: &std::path::Path, cluster: &ClusterId) {
    let sock = home.join(".remote-task-runner").join("agent.sock");
    let listener = listen_unix(&sock).unwrap();
    let cluster = cluster.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let cfg = server_cfg(&cluster);
            tokio::spawn(async move {
                if let Ok(srv) = accept(Io::from_stream(stream), cfg).await {
                    serve(srv.incoming, Arc::new(|r| Some(Ok(ack_response(&r)))))
                        .await
                        .ok();
                }
            });
        }
    });
}

#[tokio::test]
async fn the_bridge_process_relays_the_protocol_to_the_agent() {
    let home = short_tempdir();
    let cluster = ClusterId::generate();
    start_agent(home.path(), &cluster);
    let mut child = tokio::process::Command::new(BIN)
        .args(["worker", "--stdio"])
        .env("HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let io = Io::new(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    // A successful hello proves stdout held protocol bytes and nothing else.
    let conn = connect(io, client_cfg(&cluster)).await.unwrap();
    for _ in 0..3 {
        let req = ack_request();
        assert_eq!(
            conn.client.call(req.clone(), T).await.unwrap(),
            ack_response(&req)
        );
    }
    drop(conn);
    // Closing our side ends the bridge cleanly.
    let status = child.wait().await.unwrap();
    assert!(status.success(), "{status:?}");
}

#[test]
fn without_an_agent_the_bridge_exits_with_an_explicit_error_and_a_clean_stdout() {
    let home = short_tempdir();
    let out = assert_cmd::Command::new(BIN)
        .args(["--json", "worker", "--stdio"])
        .env("HOME", home.path())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(73));
    assert!(out.stdout.is_empty(), "stdout must stay protocol-only");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("not running"), "{err}");

    let out = assert_cmd::Command::new(BIN)
        .args(["worker", "--stdio"])
        .env_remove("HOME")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
}

/// The whole SSH path with a fake `ssh` that, like the real one, joins nothing
/// and hands the remote command string to a shell. The remote binary path has a
/// space and a quote in it, so only correct quoting gets through.
#[tokio::test]
async fn the_ssh_command_line_reaches_the_bridge_through_a_remote_shell() {
    use std::os::unix::fs::PermissionsExt;
    let home = short_tempdir();
    let cluster = ClusterId::generate();
    start_agent(home.path(), &cluster);

    let tools = short_tempdir();
    let dir = tools.path().join("it's a dir");
    std::fs::create_dir_all(&dir).unwrap();
    let wrapper = dir.join("rtr wrapper.sh");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nHOME='{}' exec '{}' \"$@\"\n",
            home.path().display(),
            BIN
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let fake_ssh = tools.path().join("ssh");
    std::fs::write(
        &fake_ssh,
        "#!/bin/sh\n# skip options up to the terminator, then the destination\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nshift\nexec /bin/sh -c \"$1\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_ssh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let target = SshTarget::parse("worker-host").unwrap();
    let command = RemoteCommand::worker_stdio(wrapper.to_str().unwrap()).unwrap();
    let mut argv = ssh_argv(&target, &command);
    argv[0] = fake_ssh.to_str().unwrap().to_string();
    let t = spawn_child(&argv).unwrap();
    let conn = connect(t.io, client_cfg(&cluster)).await.unwrap();
    let req = ack_request();
    assert_eq!(
        conn.client.call(req.clone(), T).await.unwrap(),
        ack_response(&req)
    );
}

/// Lives here, not next to the socket unit tests: on macOS a child forked while
/// another test is creating a socket inherits that socket's descriptor.
#[tokio::test]
async fn a_child_process_is_a_transport() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut t = spawn_child(&["cat".to_string()]).unwrap();
    t.io.writer.write_all(b"ping").await.unwrap();
    t.io.writer.shutdown().await.unwrap();
    let mut got = Vec::new();
    t.io.reader.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"ping");
    assert!(spawn_child(&[]).is_err());
    assert!(spawn_child(&["/definitely/not/here".to_string()]).is_err());
}
