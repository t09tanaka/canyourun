//! Request correlation, the writer task and the inbound queues.
//!
//! One connection carries many requests at once. Every request gets a stream id
//! and a waiter; the reader task matches replies to waiters by stream id and
//! request id. Sending goes through a [`Scheduler`] so control frames overtake
//! log and blob frames. Receiving never blocks on a slow consumer, and never
//! grows without bound either: inbound requests wait in queues with a count (and
//! for bulk, a byte) limit, control requests are handed out before bulk ones,
//! and what does not fit is refused at once, so the reader keeps reading and
//! heartbeats keep flowing. The outgoing control queue is bounded the same way.

use super::frame::{encode, kind_of_operation, Body, Frame, FrameError, StreamKind};
use super::io::{read_frame_sized, FRAME_IDLE_TIMEOUT};
use super::sched::{Outgoing, Scheduler};
use crate::error::{ApiError, ErrorCode};
use crate::ids::RequestId;
use crate::wire::{ErrorResponse, Message, Operation, Request, Response, SchemaVersion};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};

/// Log and blob frames that may wait for the writer at once. A sender of a big
/// blob blocks here instead of buffering the whole blob in memory.
pub const BULK_QUEUE_FRAMES: usize = 8;

/// Bulk (log and blob) requests that may wait unserved at once. A blob upload
/// moves one chunk per request and waits for the answer, so a healthy peer
/// has few in flight; more than this is a flood or a stuck handler.
pub const INBOUND_BULK_MAX_REQUESTS: usize = 16;
/// Wire bytes of the bulk requests that may wait unserved at once (four
/// largest frames).
pub const INBOUND_BULK_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Control requests that may wait unserved at once. They are small and cheap to
/// serve, so a backlog this deep means the peer is not following the protocol;
/// the connection is closed rather than a request dropped, because dropping a
/// control request (a lease renewal, a cancel) silently is worse than a
/// reconnect.
pub const INBOUND_CONTROL_MAX_REQUESTS: usize = 256;
/// Control frames that may wait for the writer at once, and their bytes. A peer
/// that sends requests but never reads the answers fills this; past it the
/// connection is closed and every waiter is `Unconfirmed`.
pub const OUTBOX_CONTROL_MAX_FRAMES: usize = 1024;
pub const OUTBOX_CONTROL_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Why no answer arrived. The request may or may not have run on the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unconfirmed {
    /// The connection ended while the request was outstanding.
    Disconnected,
    /// The request was sent and no answer came within the timeout.
    TimedOut,
    /// An answer came that this side cannot take as one: it is of another
    /// schema version, or it answers another operation. The peer may have
    /// done what was asked; this is not the peer's refusal.
    Unreadable,
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
    /// The control queue overflowed: the connection is being cut, and what is
    /// still queued is dropped.
    overflowed: AtomicBool,
    /// Wakes a writer stuck in a write to a peer that does not read.
    cut: Notify,
}

impl Outbox {
    pub fn new() -> Arc<Outbox> {
        Arc::new(Outbox {
            sched: Mutex::new(Scheduler::new()),
            wake: Notify::new(),
            bulk_slots: Semaphore::new(BULK_QUEUE_FRAMES),
            shutdown: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            overflowed: AtomicBool::new(false),
            cut: Notify::new(),
        })
    }

    fn push(&self, item: Outgoing) -> Result<(), SendError> {
        if self.shutdown.load(Ordering::SeqCst) || self.dead.load(Ordering::SeqCst) {
            return Err(SendError::Closed);
        }
        {
            let mut sched = self.sched.lock().expect("scheduler lock");
            if item.kind == StreamKind::Control
                && (sched.control_len() >= OUTBOX_CONTROL_MAX_FRAMES
                    || sched.control_bytes().saturating_add(item.bytes.len())
                        > OUTBOX_CONTROL_MAX_BYTES)
            {
                drop(sched);
                self.overflowed.store(true, Ordering::SeqCst);
                self.wake.notify_one();
                self.cut.notify_one();
                return Err(SendError::Closed);
            }
            sched.push(item);
        }
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

    /// The peer is not reading: the connection is being cut.
    pub(crate) fn is_overflowed(&self) -> bool {
        self.overflowed.load(Ordering::SeqCst)
    }

    fn mark_dead(&self) {
        self.dead.store(true, Ordering::SeqCst);
        self.bulk_slots.close();
    }

    async fn next(&self) -> Option<Outgoing> {
        loop {
            if self.is_overflowed() {
                return None;
            }
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
        // A peer that does not read blocks the write for good; the cut signal
        // (control queue overflow) is what ends it.
        let written = tokio::select! {
            biased;
            _ = outbox.cut.notified() => false,
            r = async {
                writer.write_all(&item.bytes).await?;
                writer.flush().await
            } => r.is_ok(),
        };
        if !written {
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

/// Removes a request's waiter when the call ends in any way, including its
/// future being dropped (the caller gave up or was cancelled). Without it the
/// entry would stay until the connection closes, and a long-lived connection
/// with many abandoned calls would leak.
struct ForgetOnDrop<'a> {
    shared: &'a Shared,
    stream: u32,
}

impl Drop for ForgetOnDrop<'_> {
    fn drop(&mut self) {
        self.shared.forget(self.stream);
    }
}

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
        let _forget = ForgetOnDrop { shared, stream };
        let item = Outgoing {
            kind: frame.kind,
            bytes,
        };
        if self.inner.outbox.send(item).await.is_err() {
            return Err(CallError::NotSent);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CallError::Unconfirmed(Unconfirmed::Disconnected)),
            Err(_) => Err(CallError::Unconfirmed(Unconfirmed::TimedOut)),
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
        let _forget = ForgetOnDrop { shared, stream };
        let started = tokio::time::Instant::now();
        if self
            .inner
            .outbox
            .send_control(&Frame::ping(stream), shared.max_frame)
            .is_err()
        {
            return Err(CallError::NotSent);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) => Ok(started.elapsed()),
            Ok(Err(_)) => Err(CallError::Unconfirmed(Unconfirmed::Disconnected)),
            Err(_) => Err(CallError::Unconfirmed(Unconfirmed::TimedOut)),
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
///
/// Every request gets an answer: a responder dropped without one (a handler
/// that panicked or forgot) answers `INTERNAL` so the caller does not wait for
/// its timeout and then treat the request as unconfirmed.
pub struct Responder {
    stream: u32,
    request_id: RequestId,
    outbox: Arc<Outbox>,
    max_frame: usize,
    answered: bool,
}

impl Responder {
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    fn error_frame(&self, error: ApiError) -> Frame {
        Frame::message(
            self.stream,
            error_message(Some(self.request_id.clone()), error),
        )
    }

    async fn send(mut self, message: Message) -> Result<(), SendError> {
        self.answered = true;
        let frame = Frame::message(self.stream, message);
        match encode(&frame, self.max_frame) {
            Ok(bytes) => {
                self.outbox
                    .send(Outgoing {
                        kind: frame.kind,
                        bytes,
                    })
                    .await
            }
            Err(e) => {
                // The answer cannot be written (over the frame limit): say so
                // instead of leaving the caller waiting.
                let fallback = self.error_frame(ApiError::new(
                    ErrorCode::Internal,
                    format!("the answer could not be encoded: {e}"),
                ));
                let _ = self.outbox.send_control(&fallback, self.max_frame);
                Err(SendError::Frame(e))
            }
        }
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
        let message = error_message(Some(self.request_id.clone()), error);
        self.send(message).await
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        if !self.answered {
            let frame = self.error_frame(ApiError::new(
                ErrorCode::Internal,
                "the request was dropped without an answer",
            ));
            let _ = self.outbox.send_control(&frame, self.max_frame);
        }
    }
}

pub struct InboundRequest {
    pub request: Request,
    pub responder: Responder,
}

/// Inbound requests. Control requests come out before bulk ones. Spawn a task
/// per request: a slow `blob_put` handler must not hold up `renew_lease`.
pub struct Incoming {
    control: mpsc::UnboundedReceiver<Queued>,
    bulk: mpsc::UnboundedReceiver<Queued>,
    budget: Arc<Budget>,
    outbox: Arc<Outbox>,
}

/// A request in a queue, with what it was charged against the budget.
struct Queued {
    request: InboundRequest,
    bytes: usize,
}

/// What waits unserved in the inbound queues. The reader adds, [`Incoming`]
/// subtracts when it hands a request out.
#[derive(Default)]
struct Budget {
    control_requests: AtomicUsize,
    bulk_requests: AtomicUsize,
    bulk_bytes: AtomicUsize,
}

impl Incoming {
    pub async fn next(&mut self) -> Option<InboundRequest> {
        tokio::select! {
            biased;
            Some(q) = self.control.recv() => {
                self.budget.control_requests.fetch_sub(1, Ordering::SeqCst);
                Some(q.request)
            }
            Some(q) = self.bulk.recv() => {
                self.budget.bulk_requests.fetch_sub(1, Ordering::SeqCst);
                self.budget.bulk_bytes.fetch_sub(q.bytes, Ordering::SeqCst);
                Some(q.request)
            }
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
    control: mpsc::UnboundedSender<Queued>,
    bulk: mpsc::UnboundedSender<Queued>,
    budget: Arc<Budget>,
}

pub(crate) fn inbound_queues(outbox: Arc<Outbox>) -> (InboundSenders, Incoming) {
    let (control, control_rx) = mpsc::unbounded_channel();
    let (bulk, bulk_rx) = mpsc::unbounded_channel();
    let budget = Arc::new(Budget::default());
    (
        InboundSenders {
            control,
            bulk,
            budget: Arc::clone(&budget),
        },
        Incoming {
            control: control_rx,
            bulk: bulk_rx,
            budget,
            outbox,
        },
    )
}

/// What the busy answer says: `OVERLOADED`, "refused for now, nothing was done,
/// ask again later". Not `SOURCE_BUSY`, which is about a moving input.
fn busy_error() -> ApiError {
    ApiError::new(
        ErrorCode::Overloaded,
        "the worker has too many bulk requests waiting; retry later",
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
    while let Ok(Some((frame, wire_len))) =
        read_frame_sized(&mut reader, max, FRAME_IDLE_TIMEOUT).await
    {
        if !dispatch(frame, wire_len, &shared, &outbox, inbound.as_ref()) || outbox.is_overflowed()
        {
            break;
        }
    }
    shared.close();
    outbox.shutdown();
}

/// Handle one frame. `false` stops the reader.
fn dispatch(
    frame: Frame,
    wire_len: usize,
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
                // An answer that cannot be read is made here, not by the
                // peer: the caller must not take it for the peer's word (a
                // refusal it could count), and the request may have run.
                let result =
                    if response.operation() != op || response.ensure_current_schema().is_err() {
                        Err(CallError::Unconfirmed(Unconfirmed::Unreadable))
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
            let kind = kind_of_operation(request.operation());
            let budget = &inbound.budget;
            // Reserve room before the request is built. Control overflow cuts
            // the connection; bulk overflow refuses just this request.
            match kind {
                StreamKind::Control => {
                    if budget.control_requests.load(Ordering::SeqCst)
                        >= INBOUND_CONTROL_MAX_REQUESTS
                    {
                        return false;
                    }
                    budget.control_requests.fetch_add(1, Ordering::SeqCst);
                }
                StreamKind::Log | StreamKind::Blob => {
                    let requests = budget.bulk_requests.load(Ordering::SeqCst);
                    let bytes = budget.bulk_bytes.load(Ordering::SeqCst);
                    if requests >= INBOUND_BULK_MAX_REQUESTS
                        || bytes.saturating_add(wire_len) > INBOUND_BULK_MAX_BYTES
                    {
                        reply(error_message(Some(request_id), busy_error()));
                        return true;
                    }
                    budget.bulk_requests.fetch_add(1, Ordering::SeqCst);
                    budget.bulk_bytes.fetch_add(wire_len, Ordering::SeqCst);
                }
            }
            let queued = Queued {
                request: InboundRequest {
                    request,
                    responder: Responder {
                        stream,
                        request_id,
                        outbox: Arc::clone(outbox),
                        max_frame: shared.max_frame,
                        answered: false,
                    },
                },
                bytes: wire_len,
            };
            // The application dropped its `Incoming`: nobody serves requests.
            let queue = match kind {
                StreamKind::Control => &inbound.control,
                StreamKind::Log | StreamKind::Blob => &inbound.bulk,
            };
            return queue.send(queued).is_ok();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;
    use crate::wire::AckCollectedRequest;

    fn ack() -> Request {
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: AttemptId::generate(),
            generation: Generation(1),
            result_digest: Digest::of_bytes(b"r"),
        })
    }

    fn pending(shared: &Shared) -> usize {
        shared.state.lock().unwrap().pending.len()
    }

    /// A client whose peer never answers: the writer drains into a big pipe.
    fn silent_client() -> (Client, Arc<Shared>, tokio::io::DuplexStream) {
        let (ours, theirs) = tokio::io::duplex(1 << 20);
        let shared = Shared::new(crate::protocol::frame::MAX_FRAME_LEN);
        let outbox = Outbox::new();
        let (_r, w) = tokio::io::split(ours);
        tokio::spawn(run_writer(w, Arc::clone(&outbox), Arc::clone(&shared)));
        (Client::new(Arc::clone(&shared), outbox), shared, theirs)
    }

    #[tokio::test]
    async fn a_call_dropped_by_its_caller_forgets_its_waiter() {
        let (client, shared, _peer) = silent_client();
        let gave_up = tokio::time::timeout(
            Duration::from_millis(50),
            client.call(ack(), Duration::from_secs(30)),
        )
        .await;
        assert!(gave_up.is_err());
        assert_eq!(pending(&shared), 0);
        let gave_up = tokio::time::timeout(
            Duration::from_millis(50),
            client.ping(Duration::from_secs(30)),
        )
        .await;
        assert!(gave_up.is_err());
        assert_eq!(pending(&shared), 0);
        // A call that times out by itself is forgotten as well.
        let err = client
            .call(ack(), Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, CallError::Unconfirmed(Unconfirmed::TimedOut)));
        assert_eq!(pending(&shared), 0);
    }
}
