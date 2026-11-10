//! The handshake that starts a connection (`hello`), on both sides.
//!
//! [`connect`] is the coordinator's: it sends `hello`, checks the answer and
//! returns a [`Client`]. [`accept`] is the worker's: it waits for `hello`,
//! negotiates, and returns the [`Incoming`] queue of requests to serve. An
//! incompatible peer gets `PROTOCOL_INCOMPATIBLE` and the connection is closed.

use super::frame::{Body, Frame, FrameError, MAX_FRAME_LEN};
use super::hello::{
    negotiate, verify_response, LocalHello, Negotiated, WorkerIdentity, PROTOCOL_VERSIONS,
    SUPPORTED_FEATURES,
};
use super::io::{read_frame, write_frame};
use super::mux::{start, Client, Incoming};
use super::transport::Io;
use crate::error::{ApiError, ErrorCode};
use crate::ids::{ClusterId, CoordinatorEpoch, CoordinatorId, RequestId};
use crate::wire::{
    ErrorResponse, HelloRequest, HelloResponse, Message, Request, Response, SchemaVersion,
};
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("{0}")]
    Frame(FrameError),
    /// The peer refused us with this error.
    #[error("peer refused hello: {} ({})", .0.message, .0.code)]
    Refused(ApiError),
    /// We refused the peer with this error (and told it, on the worker side).
    #[error("incompatible peer: {}", .0.message)]
    Incompatible(ApiError),
    #[error("unexpected first frame: {0}")]
    Unexpected(&'static str),
    #[error("hello timed out")]
    TimedOut,
    #[error("connection closed during hello")]
    Closed,
}

impl ConnectError {
    /// The error code to report for a failed connection.
    pub fn api_error(&self) -> ApiError {
        match self {
            ConnectError::Refused(e) | ConnectError::Incompatible(e) => e.clone(),
            ConnectError::TimedOut | ConnectError::Closed => {
                ApiError::new(ErrorCode::CommunicationUnknown, self.to_string())
            }
            ConnectError::Frame(_) | ConnectError::Unexpected(_) => {
                ApiError::new(ErrorCode::ProtocolIncompatible, self.to_string())
            }
        }
    }
}

impl From<FrameError> for ConnectError {
    fn from(e: FrameError) -> Self {
        ConnectError::Frame(e)
    }
}

async fn read_first(
    io: &mut Io,
    max_frame: usize,
    timeout: Duration,
) -> Result<Frame, ConnectError> {
    match tokio::time::timeout(timeout, read_frame(&mut io.reader, max_frame)).await {
        Err(_) => Err(ConnectError::TimedOut),
        Ok(Err(e)) => Err(e.into()),
        Ok(Ok(None)) => Err(ConnectError::Closed),
        Ok(Ok(Some(frame))) => Ok(frame),
    }
}

// ---------------------------------------------------------------------------
// coordinator side
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub cluster_id: ClusterId,
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub protocol_versions: Vec<u32>,
    pub features: Vec<String>,
    pub required_features: Vec<String>,
    pub hello_timeout: Duration,
    pub max_frame: usize,
}

impl ClientConfig {
    pub fn new(
        cluster_id: ClusterId,
        coordinator_id: CoordinatorId,
        coordinator_epoch: CoordinatorEpoch,
    ) -> Self {
        ClientConfig {
            cluster_id,
            coordinator_id,
            coordinator_epoch,
            protocol_versions: PROTOCOL_VERSIONS.to_vec(),
            features: SUPPORTED_FEATURES.iter().map(|f| f.to_string()).collect(),
            required_features: Vec::new(),
            hello_timeout: Duration::from_secs(10),
            max_frame: MAX_FRAME_LEN,
        }
    }
}

/// A connection that passed `hello`.
pub struct Connected {
    pub client: Client,
    /// The worker's identity, epoch and profile as it reported them. A worker
    /// id that differs from the one expected is a new identity (a restarted
    /// ephemeral worker), not a reconnect; the coordinator decides what that
    /// means.
    pub worker: HelloResponse,
    pub negotiated: Negotiated,
}

pub async fn connect(mut io: Io, cfg: ClientConfig) -> Result<Connected, ConnectError> {
    let hello = HelloRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        cluster_id: cfg.cluster_id.clone(),
        coordinator_id: cfg.coordinator_id.clone(),
        coordinator_epoch: cfg.coordinator_epoch,
        protocol_versions: cfg.protocol_versions.clone(),
        features: cfg.features.clone(),
    };
    let request_id = hello.request_id.clone();
    let frame = Frame::message(0, Message::Request(Request::Hello(hello.clone())));
    write_frame(&mut io.writer, &frame, cfg.max_frame).await?;
    let reply = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let response = match reply.body {
        Body::Message(Message::Response {
            request_id: id,
            response: Response::Hello(resp),
        }) if id == request_id => resp,
        Body::Message(Message::Error(ErrorResponse { error, .. })) => {
            return Err(ConnectError::Refused(error))
        }
        _ => return Err(ConnectError::Unexpected("expected a hello response")),
    };
    let negotiated = verify_response(&hello, &cfg.required_features, &response)
        .map_err(ConnectError::Incompatible)?;
    let Io { reader, writer } = io;
    let (shared, outbox, _) = start(reader, writer, cfg.max_frame, false);
    Ok(Connected {
        client: Client::new(shared, outbox),
        worker: response,
        negotiated,
    })
}

// ---------------------------------------------------------------------------
// worker side
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub local: LocalHello,
    pub identity: WorkerIdentity,
    pub hello_timeout: Duration,
    pub max_frame: usize,
}

impl ServerConfig {
    pub fn new(cluster_id: ClusterId, identity: WorkerIdentity) -> Self {
        ServerConfig {
            local: LocalHello {
                cluster_id,
                versions: PROTOCOL_VERSIONS.to_vec(),
                features: SUPPORTED_FEATURES.iter().map(|f| f.to_string()).collect(),
                required_features: Vec::new(),
            },
            identity,
            hello_timeout: Duration::from_secs(10),
            max_frame: MAX_FRAME_LEN,
        }
    }
}

/// A connection whose coordinator passed `hello`.
pub struct Accepted {
    pub incoming: Incoming,
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub negotiated: Negotiated,
}

async fn refuse(io: &mut Io, max_frame: usize, request_id: Option<RequestId>, error: ApiError) {
    let frame = Frame::message(
        0,
        Message::Error(ErrorResponse {
            schema_version: SchemaVersion::CURRENT,
            request_id,
            error,
        }),
    );
    let _ = write_frame(&mut io.writer, &frame, max_frame).await;
    let _ = io.writer.shutdown().await;
}

pub async fn accept(mut io: Io, cfg: ServerConfig) -> Result<Accepted, ConnectError> {
    let first = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let Body::Message(Message::Request(Request::Hello(hello))) = first.body else {
        let error = ApiError::new(ErrorCode::InvalidInput, "hello must be the first request");
        refuse(&mut io, cfg.max_frame, None, error).await;
        return Err(ConnectError::Unexpected("first frame was not hello"));
    };
    let negotiated = match negotiate(&cfg.local, &hello) {
        Ok(n) => n,
        Err(error) => {
            refuse(
                &mut io,
                cfg.max_frame,
                Some(hello.request_id.clone()),
                error.clone(),
            )
            .await;
            return Err(ConnectError::Incompatible(error));
        }
    };
    let response = cfg.identity.response(&cfg.local.cluster_id, &negotiated);
    let reply = Frame::message(
        first.stream,
        Message::Response {
            request_id: hello.request_id.clone(),
            response: Response::Hello(response),
        },
    );
    write_frame(&mut io.writer, &reply, cfg.max_frame).await?;
    let Io { reader, writer } = io;
    let (_, _, incoming) = start(reader, writer, cfg.max_frame, true);
    Ok(Accepted {
        incoming: incoming.expect("a serving connection has an inbound queue"),
        coordinator_id: hello.coordinator_id,
        coordinator_epoch: hello.coordinator_epoch,
        negotiated,
    })
}
