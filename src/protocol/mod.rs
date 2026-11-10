//! Wire protocol between coordinator and workers (spec chapters 3, 11, 13).
//!
//! The layer is split in two. The pure, I/O-free modules hold every decision
//! and are held to the coverage and mutation gates:
//!
//! - [`frame`]: the length-prefixed JSON frame, its size limit, the stream kind
//! - [`sched`]: which queued frame goes out next (control before log and blob)
//! - [`hello`]: version, feature and cluster negotiation
//! - [`chunk`]: offset and digest rules for blob and log transfer
//! - [`ssh`]: the `ssh` argv, with the remote command quoted for the shell
//!
//! The tokio modules move bytes: [`io`] (frame reader and writer), [`mux`]
//! (request correlation, the writer task, the inbound queues), [`session`]
//! (the hello handshake that starts a connection), [`transfer`] (resumable blob
//! and log helpers) and [`transport`] (Unix socket, in-process duplex, child
//! process and the `worker --stdio` bridge).

pub mod chunk;
pub mod frame;
pub mod hello;
pub mod io;
pub mod mux;
pub mod sched;
pub mod session;
pub mod ssh;
pub mod transfer;
pub mod transport;

pub use frame::{Body, Frame, FrameError, StreamKind, MAX_FRAME_LEN};
pub use mux::{CallError, Client, InboundRequest, Incoming, Responder, SendError, Unconfirmed};
pub use session::{accept, connect, Accepted, ClientConfig, ConnectError, Connected, ServerConfig};
pub use transport::Io;
