//! Byte streams for the protocol: Unix socket, in-process duplex, a child
//! process's stdin and stdout, and the `worker --stdio` bridge.
//!
//! Nothing here opens a TCP port. A remote worker is reached over `ssh`
//! ([`super::ssh`]), which runs the bridge; the local worker is a direct Unix
//! socket connection.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};

pub type BoxRead = Box<dyn AsyncRead + Send + Unpin>;
pub type BoxWrite = Box<dyn AsyncWrite + Send + Unpin>;

/// A connected byte stream, split into its two directions.
pub struct Io {
    pub reader: BoxRead,
    pub writer: BoxWrite,
}

impl Io {
    pub fn new<R, W>(reader: R, writer: W) -> Io
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Io {
            reader: Box::new(reader),
            writer: Box::new(writer),
        }
    }

    pub fn from_stream<S>(stream: S) -> Io
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (r, w) = tokio::io::split(stream);
        Io::new(r, w)
    }
}

/// Two connected in-process ends, for tests of the coordinator, the worker agent
/// and the protocol itself. Dropping one end is the peer dying: the other sees
/// end of stream. `buffer` is the capacity of each direction; a small one makes
/// the writer block, which is how a slow link is simulated.
pub fn duplex_pair(buffer: usize) -> (Io, Io) {
    let (a, b) = tokio::io::duplex(buffer);
    (Io::from_stream(a), Io::from_stream(b))
}

/// Connect to a worker agent's Unix socket (the local worker, or the bridge's
/// target).
pub async fn connect_unix(path: &Path) -> io::Result<Io> {
    Ok(Io::from_stream(UnixStream::connect(path).await?))
}

/// Listen on a Unix socket that only the owner can reach.
///
/// The parent directory is the access control: it is created with mode 0700
/// when missing, and an existing one whose mode lets group or others in is an
/// error (it is never loosened or tightened behind the caller's back; that
/// could be a shared directory such as `/tmp`). The socket file is then chmod
/// 0600 right after `bind`; the process-wide umask is not touched, so between
/// the two calls only the 0700 parent keeps others out, which is why that
/// precondition is enforced rather than assumed.
///
/// A stale socket file from a dead agent is replaced; a live one is an error.
/// Only a socket is ever removed: any other file at `path` is left alone and
/// reported.
pub fn listen_unix(path: &Path) -> io::Result<UnixListener> {
    use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        match std::fs::metadata(dir) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "the socket's parent is not a directory",
                    ));
                }
                if meta.permissions().mode() & 0o077 != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "the socket's directory {} is reachable by group or others; \
                             make it mode 0700",
                            dir.display()
                        ),
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(dir)?;
            }
            Err(e) => return Err(e),
        }
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another agent is listening on this socket",
                ));
            }
            std::fs::remove_file(path)?;
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the socket path is taken by something that is not a socket",
            ))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// A started child process whose stdin and stdout carry the protocol (for
/// example `ssh ... worker --stdio`). Its stderr is inherited: diagnostics reach
/// the user and never mix with protocol bytes. The child is killed when this
/// value is dropped.
pub struct ChildTransport {
    pub child: Child,
    pub io: Io,
}

/// A child's stdin whose `shutdown` really closes the pipe (tokio's own does
/// not), so the child sees end of input.
struct CloseOnShutdown(Option<tokio::process::ChildStdin>);

impl AsyncWrite for CloseOnShutdown {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.0.as_mut() {
            Some(w) => std::pin::Pin::new(w).poll_write(cx, buf),
            None => std::task::Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.0.as_mut() {
            Some(w) => std::pin::Pin::new(w).poll_flush(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if let Some(w) = self.0.as_mut() {
            if std::pin::Pin::new(w).poll_flush(cx).is_pending() {
                return std::task::Poll::Pending;
            }
        }
        self.0 = None;
        std::task::Poll::Ready(Ok(()))
    }
}

pub fn spawn_child(argv: &[String]) -> io::Result<ChildTransport> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty command"))?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    Ok(ChildTransport {
        child,
        io: Io::new(stdout, CloseOnShutdown(Some(stdin))),
    })
}

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("worker agent is not running (cannot connect to {path}): {source}")]
    AgentNotRunning { path: PathBuf, source: io::Error },
    #[error("bridge I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Relay `input` to the agent's Unix socket and the socket's output to
/// `output`, byte for byte. The bridge does not parse frames, so it works with
/// any protocol version. Input ending half-closes the socket and the relay then
/// waits for the agent to finish; the agent closing ends the relay.
pub async fn bridge<I, O>(socket: &Path, mut input: I, mut output: O) -> Result<(), BridgeError>
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
{
    let stream = UnixStream::connect(socket).await.map_err(|source| {
        if matches!(
            source.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        ) {
            BridgeError::AgentNotRunning {
                path: socket.to_path_buf(),
                source,
            }
        } else {
            BridgeError::Io(source)
        }
    })?;
    let (mut from_agent, mut to_agent) = stream.into_split();
    let up = async {
        tokio::io::copy(&mut input, &mut to_agent).await?;
        to_agent.shutdown().await
    };
    let down = async {
        tokio::io::copy(&mut from_agent, &mut output).await?;
        output.flush().await
    };
    tokio::pin!(up, down);
    tokio::select! {
        r = &mut down => r?,
        r = &mut up => {
            r?;
            down.await?;
        }
    }
    Ok(())
}

/// Where a worker agent listens: `<HOME>/.remote-task-runner/agent.sock`. The
/// environment arrives as a map; this never reads the process environment.
pub fn default_agent_socket(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    let home = env.get("HOME").filter(|h| !h.is_empty())?;
    Some(
        Path::new(home)
            .join(".remote-task-runner")
            .join("agent.sock"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// Unix socket paths are limited to about 100 bytes, which the default
    /// temp directory (a long per-user path on macOS) can exceed.
    fn short_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("rtr")
            .tempdir_in("/tmp")
            .unwrap()
    }

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_agent_socket_lives_under_home() {
        assert_eq!(
            default_agent_socket(&env(&[("HOME", "/home/u")])).unwrap(),
            PathBuf::from("/home/u/.remote-task-runner/agent.sock")
        );
        assert!(default_agent_socket(&env(&[])).is_none());
        assert!(default_agent_socket(&env(&[("HOME", "")])).is_none());
    }

    #[tokio::test]
    async fn the_bridge_relays_both_ways_and_stops_when_the_agent_closes() {
        let dir = short_tempdir();
        // The parent is made by `listen_unix` (0700); the tempdir itself may be wider.
        let sock = dir.path().join("agent").join("a.sock");
        let listener = listen_unix(&sock).unwrap();
        let agent = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut got = vec![0u8; 5];
            s.read_exact(&mut got).await.unwrap();
            s.write_all(&[0xff, 0xfe]).await.unwrap();
            got
        });
        let mut out = Vec::new();
        bridge(&sock, &b"hello"[..], &mut out).await.unwrap();
        assert_eq!(agent.await.unwrap(), b"hello");
        assert_eq!(out, vec![0xff, 0xfe]);
    }

    #[tokio::test]
    async fn a_missing_agent_is_an_explicit_error() {
        let dir = short_tempdir();
        let sock = dir.path().join("none.sock");
        let mut out = Vec::new();
        let err = bridge(&sock, &b""[..], &mut out).await.unwrap_err();
        assert!(matches!(err, BridgeError::AgentNotRunning { .. }));
        assert!(err.to_string().contains("not running"));
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn a_live_socket_is_not_replaced_but_a_stale_file_is() {
        let dir = short_tempdir();
        let sock = dir.path().join("s").join("a.sock");
        let first = listen_unix(&sock).unwrap();
        let err = listen_unix(&sock).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
        // A socket file nobody listens on (the agent died) is stale. Made with
        // std so the close on drop is immediate.
        let stale = dir.path().join("s").join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&stale).unwrap());
        assert!(stale.exists());
        let _second = listen_unix(&stale).unwrap();
        let _keep = first;
        let sock = stale;
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let dir_mode = std::fs::metadata(sock.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
    }

    #[tokio::test]
    async fn a_loose_parent_or_a_foreign_file_is_an_error_and_nothing_is_touched() {
        use std::os::unix::fs::PermissionsExt;
        let dir = short_tempdir();
        // A loose existing parent is refused, and its mode is left as it was.
        let loose = dir.path().join("loose");
        std::fs::create_dir(&loose).unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = listen_unix(&loose.join("a.sock")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        let mode = std::fs::metadata(&loose).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
        // A regular file at the socket path is never deleted.
        let tight = dir.path().join("tight");
        std::fs::create_dir(&tight).unwrap();
        std::fs::set_permissions(&tight, std::fs::Permissions::from_mode(0o700)).unwrap();
        let file = tight.join("a.sock");
        std::fs::write(&file, b"precious").unwrap();
        let err = listen_unix(&file).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&file).unwrap(), b"precious");
        // A parent that is a file is refused too.
        let err = listen_unix(&file.join("b.sock")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // A missing parent chain is created, every level 0700.
        let deep = dir.path().join("x").join("y").join("a.sock");
        let _l = listen_unix(&deep).unwrap();
        for d in [
            deep.parent().unwrap(),
            deep.parent().unwrap().parent().unwrap(),
        ] {
            let mode = std::fs::metadata(d).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{d:?}");
        }
    }

    #[tokio::test]
    async fn duplex_ends_see_each_others_close() {
        let (mut a, mut b) = duplex_pair(64);
        a.writer.write_all(b"x").await.unwrap();
        drop(a);
        let mut got = Vec::new();
        b.reader.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"x");
    }
}
