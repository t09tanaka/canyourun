//! Request correlation, the writer task and the inbound queues.
//!
//! One connection carries many requests at once. Every request gets a stream id
//! and a waiter; the reader task matches replies to waiters by stream id and
//! request id. Sending goes through a [`Scheduler`] so control frames overtake
//! log and blob frames. Receiving never blocks on a slow consumer: inbound
//! requests go to unbounded queues (the peer is the authenticated coordinator)
//! and control requests are handed out before bulk ones.

use super::frame::{encode, kind_of_operation, Body, Frame, FrameError, StreamKind};
use super::io::read_frame;
use super::sched::{Outgoing, Scheduler};
use crate::error::{ApiError, ErrorCode};
use crate::ids::RequestId;
use crate::wire::{ErrorResponse, Message, Operation, Request, Response, SchemaVersion};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};

/// Log and blob frames that may wait for the writer at once. A sender of a big
/// blob blocks here instead of buffering the whole blob in memory.
pub const BULK_QUEUE_FRAMES: usize = 8;

/// Why no answer arrived. The request may or may not have run on the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unconfirmed {
    /// The connection ended while the request was outstanding.
    Disconnected,
    /// The request was sent and no answer came within the timeout.
    TimedOut,
}

#[derive(Debug, Error)]
pub enum CallError {
    /// The connection was already closed; the request was never sent, so it did
    /// not run.
    #[error("connection closed, request not sent")]
    NotSent,
    /// Communication unconfirmed: neither a failure nor "did not run". Keep the
    /// attempt `unknown`; never start the work elsewhere because of this.
    #[error("communication unconfirmed ({0:?})")]
    Unconfirmed(Unconfirmed),
    /// The worker answered with an error.
    #[error("{} ({})", .0.message, .0.code)]
    Remote(ApiError),
    /// The request could not be encoded (over the frame limit).
    #[error("request not sent: {0}")]
    Frame(FrameError),
}

impl CallError {
    pub fn is_unconfirmed(&self) -> bool {
        matches!(self, CallError::Unconfirmed(_))
    }

    /// The error as it should be reported: unconfirmed communication is
    /// `COMMUNICATION_UNKNOWN`.
    pub fn to_api_error(&self) -> ApiError {
        match self {
            CallError::Remote(e) => e.clone(),
            CallError::Unconfirmed(_) => {
                ApiError::new(ErrorCode::CommunicationUnknown, self.to_string())
            }
            CallError::NotSent | CallError::Frame(_) => {
                ApiError::new(ErrorCode::Internal, self.to_string())
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("connection closed")]
    Closed,
    #[error("{0}")]
    Frame(FrameError),
}

// ---------------------------------------------------------------------------
// outbox: the scheduler plus waking and backpressure
// ---------------------------------------------------------------------------

pub struct Outbox {
    sched: Mutex<Scheduler>,
    wake: Notify,
    bulk_slots: Semaphore,
    /// No new frames are accepted; the writer drains and finishes.
    shutdown: AtomicBool,
    /// The writer is gone; nothing will be written.
    dead: AtomicBool,
}

impl Outbox {
    pub fn new() -> Arc<Outbox> {
        Arc::new(Outbox {
            sched: Mutex::new(Scheduler::new()),
            wake: Notify::new(),
            bulk_slots: Semaphore::new(BULK_QUEUE_FRAMES),
            shutdown: AtomicBool::new(false),
            dead: AtomicBool::new(false),
        })
    }

    fn push(&self, item: Outgoing) -> Result<(), SendError> {
        if self.shutdown.load(Ordering::SeqCst) || self.dead.load(Ordering::SeqCst) {
            return Err(SendError::Closed);
        }
        self.sched.lock().expect("scheduler lock").push(item);
        self.wake.notify_one();
        Ok(())
    }

    /// Queue encoded bytes. Control frames never wait; bulk frames wait for a
    /// free slot.
    pub async fn send(&self, item: Outgoing) -> Result<(), SendError> {
        if item.kind != StreamKind::Control {
            match self.bulk_slots.acquire().await {
                Ok(permit) => permit.forget(),
                Err(_) => return Err(SendError::Closed),
            }
            if let Err(e) = self.push(item) {
                self.bulk_slots.add_permits(1);
                return Err(e);
            }
            Ok(())
        } else {
            self.push(item)
        }
    }

    /// Queue a control frame without waiting.
    pub fn send_control(&self, frame: &Frame, max_len: usize) -> Result<(), SendError> {
        let bytes = encode(frame, max_len).map_err(SendError::Frame)?;
        self.push(Outgoing {
            kind: StreamKind::Control,
            bytes,
        })
    }

    /// Stop accepting frames; the writer sends what is queued, then closes its
    /// half of the stream so the peer sees end of stream.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.wake.notify_one();
    }

    fn mark_dead(&self) {
        self.dead.store(true, Ordering::SeqCst);
        self.bulk_slots.close();
    }

    async fn next(&self) -> Option<Outgoing> {
        loop {
            {
                let mut sched = self.sched.lock().expect("scheduler lock");
                if let Some(item) = sched.pop() {
                    if item.kind != StreamKind::Control {
                        self.bulk_slots.add_permits(1);
                    }
                    return Some(item);
                }
                if self.shutdown.load(Ordering::SeqCst) {
                    return None;
                }
            }
            self.wake.notified().await;
        }
    }
}

/// Write queued frames until shutdown or a write error, then close the stream.
pub async fn run_writer<W: AsyncWrite + Unpin>(
    mut writer: W,
    outbox: Arc<Outbox>,
    shared: Arc<Shared>,
) {
    while let Some(item) = outbox.next().await {
        if writer.write_all(&item.bytes).await.is_err() || writer.flush().await.is_err() {
            break;
        }
    }
    outbox.mark_dead();
    let _ = writer.shutdown().await;
    shared.close();
}

// ---------------------------------------------------------------------------
// shared state: who is waiting for what
// ---------------------------------------------------------------------------

enum Waiter {
    Request {
        request_id: RequestId,
        op: Operation,
        tx: oneshot::Sender<Result<Response, CallError>>,
    },
    Ping {
        tx: oneshot::Sender<()>,
    },
}

#[derive(Default)]
struct State {
    closed: bool,
    pending: HashMap<u32, Waiter>,
}

pub struct Shared {
    state: Mutex<State>,
    next_stream: AtomicU32,
    max_frame: usize,
}

impl Shared {
    pub fn new(max_frame: usize) -> Arc<Shared> {
        Arc::new(Shared {
            state: Mutex::new(State::default()),
            next_stream: AtomicU32::new(1),
            max_frame,
        })
    }

    fn stream_id(&self) -> u32 {
        self.next_stream.fetch_add(1, Ordering::SeqCst)
    }

    fn register(&self, stream: u32, waiter: Waiter) -> Result<(), ()> {
        let mut st = self.state.lock().expect("state lock");
        if st.closed {
            return Err(());
        }
        st.pending.insert(stream, waiter);
        Ok(())
    }

    fn forget(&self, stream: u32) {
        self.state
            .lock()
            .expect("state lock")
            .pending
            .remove(&stream);
    }

    /// Take the waiter of `stream` if `matches` accepts it. A reply for an
    /// unknown stream (late after a timeout) or for another request is ignored.
    fn take_if(&self, stream: u32, matches: impl Fn(&Waiter) -> bool) -> Option<Waiter> {
        let mut st = self.state.lock().expect("state lock");
        if st.pending.get(&stream).is_some_and(matches) {
            st.pending.remove(&stream)
        } else {
            None
        }
    }

    /// The connection is over: every outstanding request is unconfirmed.
    pub fn close(&self) {
        let drained: Vec<Waiter> = {
            let mut st = self.state.lock().expect("state lock");
            st.closed = true;
            st.pending.drain().map(|(_, w)| w).collect()
        };
        for w in drained {
            if let Waiter::Request { tx, .. } = w {
                let _ = tx.send(Err(CallError::Unconfirmed(Unconfirmed::Disconnected)));
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().expect("state lock").closed
    }
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

struct ClientInner {
    shared: Arc<Shared>,
    outbox: Arc<Outbox>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.outbox.shutdown();
    }
}

/// The calling side of a connection. Cloneable; the connection is closed when
/// the last clone is dropped.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub(crate) fn new(shared: Arc<Shared>, outbox: Arc<Outbox>) -> Client {
        Client {
            inner: Arc::new(ClientInner { shared, outbox }),
        }
    }

    /// Send a request and wait for its answer.
    ///
    /// `Unconfirmed` (disconnect or timeout after the request was queued) means
    /// the worker may have run it. `NotSent` means it certainly did not.
    pub async fn call(&self, request: Request, timeout: Duration) -> Result<Response, CallError> {
        let shared = &self.inner.shared;
        let stream = shared.stream_id();
        let op = request.operation();
        let request_id = request.request_id().clone();
        let frame = Frame::message(stream, Message::Request(request));
        let bytes = encode(&frame, shared.max_frame).map_err(CallError::Frame)?;
        let (tx, rx) = oneshot::channel();
        shared
            .register(stream, Waiter::Request { request_id, op, tx })
            .map_err(|_| CallError::NotSent)?;
        let item = Outgoing {
            kind: frame.kind,
            bytes,
        };
        if self.inner.outbox.send(item).await.is_err() {
            shared.forget(stream);
            return Err(CallError::NotSent);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CallError::Unconfirmed(Unconfirmed::Disconnected)),
            Err(_) => {
                shared.forget(stream);
                Err(CallError::Unconfirmed(Unconfirmed::TimedOut))
            }
        }
    }

    /// A connection heartbeat: the round-trip time of a ping. Pings travel in the
    /// control queue, so a busy blob or log transfer does not delay them beyond
    /// the frame being written.
    pub async fn ping(&self, timeout: Duration) -> Result<Duration, CallError> {
        let shared = &self.inner.shared;
        let stream = shared.stream_id();
        let (tx, rx) = oneshot::channel();
        shared
            .register(stream, Waiter::Ping { tx })
            .map_err(|_| CallError::NotSent)?;
        let started = tokio::time::Instant::now();
        if self
            .inner
            .outbox
            .send_control(&Frame::ping(stream), shared.max_frame)
            .is_err()
        {
            shared.forget(stream);
            return Err(CallError::NotSent);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) => Ok(started.elapsed()),
            Ok(Err(_)) => Err(CallError::Unconfirmed(Unconfirmed::Disconnected)),
            Err(_) => {
                shared.forget(stream);
                Err(CallError::Unconfirmed(Unconfirmed::TimedOut))
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.shared.is_closed()
    }
}

// ---------------------------------------------------------------------------
// server side: inbound requests and responders
// ---------------------------------------------------------------------------

/// The way to answer one inbound request.
pub struct Responder {
    stream: u32,
    request_id: RequestId,
    outbox: Arc<Outbox>,
    max_frame: usize,
}

impl Responder {
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    async fn send(self, message: Message) -> Result<(), SendError> {
        let frame = Frame::message(self.stream, message);
        let bytes = encode(&frame, self.max_frame).map_err(SendError::Frame)?;
        self.outbox
            .send(Outgoing {
                kind: frame.kind,
                bytes,
            })
            .await
    }

    pub async fn respond(self, response: Response) -> Result<(), SendError> {
        let request_id = self.request_id.clone();
        self.send(Message::Response {
            request_id,
            response,
        })
        .await
    }

    pub async fn fail(self, error: ApiError) -> Result<(), SendError> {
        let request_id = Some(self.request_id.clone());
        self.send(Message::Error(ErrorResponse {
            schema_version: SchemaVersion::CURRENT,
            request_id,
            error,
        }))
        .await
    }
}

pub struct InboundRequest {
    pub request: Request,
    pub responder: Responder,
}

/// Inbound requests. Control requests come out before bulk ones. Spawn a task
/// per request: a slow `blob_put` handler must not hold up `renew_lease`.
pub struct Incoming {
    control: mpsc::UnboundedReceiver<InboundRequest>,
    bulk: mpsc::UnboundedReceiver<InboundRequest>,
    outbox: Arc<Outbox>,
}

impl Incoming {
    pub async fn next(&mut self) -> Option<InboundRequest> {
        tokio::select! {
            biased;
            Some(r) = self.control.recv() => Some(r),
            Some(r) = self.bulk.recv() => Some(r),
            else => None,
        }
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        self.outbox.shutdown();
    }
}

pub(crate) struct InboundSenders {
    control: mpsc::UnboundedSender<InboundRequest>,
    bulk: mpsc::UnboundedSender<InboundRequest>,
}

pub(crate) fn inbound_queues(outbox: Arc<Outbox>) -> (InboundSenders, Incoming) {
    let (control, control_rx) = mpsc::unbounded_channel();
    let (bulk, bulk_rx) = mpsc::unbounded_channel();
    (
        InboundSenders { control, bulk },
        Incoming {
            control: control_rx,
            bulk: bulk_rx,
            outbox,
        },
    )
}

fn error_message(request_id: Option<RequestId>, error: ApiError) -> Message {
    Message::Error(ErrorResponse {
        schema_version: SchemaVersion::CURRENT,
        request_id,
        error,
    })
}

/// Read frames until the peer closes or the stream breaks, then close the
/// connection's bookkeeping. Dropping the inbound senders ends [`Incoming`].
pub(crate) async fn run_reader<R: AsyncRead + Unpin>(
    mut reader: R,
    shared: Arc<Shared>,
    outbox: Arc<Outbox>,
    inbound: Option<InboundSenders>,
) {
    let max = shared.max_frame;
    while let Ok(Some(frame)) = read_frame(&mut reader, max).await {
        if !dispatch(frame, &shared, &outbox, inbound.as_ref()) {
            break;
        }
    }
    shared.close();
    outbox.shutdown();
}

/// Handle one frame. `false` stops the reader.
fn dispatch(
    frame: Frame,
    shared: &Arc<Shared>,
    outbox: &Arc<Outbox>,
    inbound: Option<&InboundSenders>,
) -> bool {
    let stream = frame.stream;
    let reply = |message: Message| {
        let _ = outbox.send_control(&Frame::message(stream, message), shared.max_frame);
    };
    match frame.body {
        Body::Ping => {
            let _ = outbox.send_control(&Frame::pong(stream), shared.max_frame);
        }
        Body::Pong => {
            if let Some(Waiter::Ping { tx }) =
                shared.take_if(stream, |w| matches!(w, Waiter::Ping { .. }))
            {
                let _ = tx.send(());
            }
        }
        Body::Message(Message::Response {
            request_id,
            response,
        }) => {
            let waiter = shared.take_if(
                stream,
                |w| matches!(w, Waiter::Request { request_id: id, .. } if *id == request_id),
            );
            if let Some(Waiter::Request { op, tx, .. }) = waiter {
                let result = if response.operation() != op {
                    Err(CallError::Remote(ApiError::new(
                        ErrorCode::Internal,
                        "response operation does not match the request",
                    )))
                } else if let Err(e) = response.ensure_current_schema() {
                    Err(CallError::Remote(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        e.to_string(),
                    )))
                } else {
                    Ok(response)
                };
                let _ = tx.send(result);
            }
        }
        Body::Message(Message::Error(e)) => {
            let waiter = shared.take_if(stream, |w| match w {
                Waiter::Request { request_id, .. } => {
                    e.request_id.as_ref().is_none_or(|id| id == request_id)
                }
                Waiter::Ping { .. } => false,
            });
            if let Some(Waiter::Request { tx, .. }) = waiter {
                let _ = tx.send(Err(CallError::Remote(e.error)));
            }
        }
        Body::Message(Message::Request(request)) => {
            let request_id = request.request_id().clone();
            let Some(inbound) = inbound else {
                reply(error_message(
                    Some(request_id),
                    ApiError::new(ErrorCode::InvalidInput, "this side does not serve requests"),
                ));
                return true;
            };
            if request.operation() == Operation::Hello {
                reply(error_message(
                    Some(request_id),
                    ApiError::new(ErrorCode::Conflict, "hello was already exchanged"),
                ));
                return true;
            }
            if let Err(e) = request.ensure_current_schema() {
                reply(error_message(
                    Some(request_id),
                    ApiError::new(ErrorCode::ProtocolIncompatible, e.to_string()),
                ));
                return true;
            }
            let queue = match kind_of_operation(request.operation()) {
                StreamKind::Control => &inbound.control,
                StreamKind::Log | StreamKind::Blob => &inbound.bulk,
            };
            let item = InboundRequest {
                request,
                responder: Responder {
                    stream,
                    request_id,
                    outbox: Arc::clone(outbox),
                    max_frame: shared.max_frame,
                },
            };
            // The application dropped its `Incoming`: nobody serves requests.
            return queue.send(item).is_ok();
        }
    }
    true
}

/// Start the writer and reader tasks over a split stream.
pub(crate) fn start<R, W>(
    reader: R,
    writer: W,
    max_frame: usize,
    serve: bool,
) -> (Arc<Shared>, Arc<Outbox>, Option<Incoming>)
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let shared = Shared::new(max_frame);
    let outbox = Outbox::new();
    tokio::spawn(run_writer(writer, Arc::clone(&outbox), Arc::clone(&shared)));
    let (senders, incoming) = if serve {
        let (s, i) = inbound_queues(Arc::clone(&outbox));
        (Some(s), Some(i))
    } else {
        (None, None)
    };
    tokio::spawn(run_reader(
        reader,
        Arc::clone(&shared),
        Arc::clone(&outbox),
        senders,
    ));
    (shared, outbox, incoming)
}
