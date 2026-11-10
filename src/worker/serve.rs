//! The agent on tokio: one task per connection, one blocking call per
//! request, and the periodic housekeeping.

use super::agent::{Agent, Session};
use crate::error::{ApiError, ErrorCode};
use crate::ids::ClusterId;
use crate::protocol::hello::WorkerIdentity;
use crate::protocol::transport::listen_unix;
use crate::protocol::{accept, ConnectError, InboundRequest, Io, ServerConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::{watch, Semaphore};

/// Requests of one agent that run at once. The rest wait in the connection's
/// inbound queue, which has its own limits.
pub const MAX_CONCURRENT_REQUESTS: usize = 64;

/// Why a connection ended.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The agent could not say who it is (the database did not answer). No
    /// `hello` was answered: an epoch made up here would be believed.
    #[error("worker identity: {}", .0.message)]
    Identity(ApiError),
    #[error(transparent)]
    Connect(#[from] ConnectError),
}

/// What the agent says about itself in `hello`.
pub fn identity(agent: &Agent) -> Result<WorkerIdentity, ApiError> {
    let cfg = agent.config();
    let coordinator_epoch = agent
        .store()
        .max_coordinator_epoch()
        .map_err(|e| ApiError::new(e.code(), format!("the highest coordinator epoch: {e}")))?;
    Ok(WorkerIdentity {
        worker_id: cfg.worker_id.clone(),
        worker_boot_id: cfg.boot_id.clone(),
        agent_epoch: agent.agent_epoch(),
        profile: cfg.profile.clone(),
        profile_fingerprint: cfg.profile_fingerprint.clone(),
        coordinator_epoch,
    })
}

/// Serve one connection until the peer closes it.
pub async fn serve_connection(
    agent: Arc<Agent>,
    io: Io,
    cluster_id: ClusterId,
    permits: Arc<Semaphore>,
) -> Result<(), ServeError> {
    let me = identity(&agent).map_err(ServeError::Identity)?;
    let accepted = accept(io, ServerConfig::new(cluster_id, me)).await?;
    let session = Session {
        coordinator_epoch: accepted.coordinator_epoch,
    };
    let mut incoming = accepted.incoming;
    loop {
        let Some(InboundRequest { request, responder }) = incoming.next().await else {
            return Ok(());
        };
        // No further request is taken from this connection until this one
        // may run: what cannot run yet stays queued where the protocol layer
        // counts it. An idle connection holds no permit.
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return Ok(());
        };
        let agent = agent.clone();
        tokio::spawn(async move {
            let done = tokio::task::spawn_blocking(move || agent.handle(session, request)).await;
            let _ = match done {
                Ok(Ok(response)) => responder.respond(response).await,
                Ok(Err(error)) => responder.fail(error).await,
                Err(_) => {
                    responder
                        .fail(ApiError::new(ErrorCode::Internal, "the request panicked"))
                        .await
                }
            };
            drop(permit);
        });
    }
}

/// Listen on the agent's socket until `stop` turns true. The state directory
/// is 0700 (the agent made it), which the listener requires.
pub async fn serve(
    agent: Arc<Agent>,
    cluster_id: ClusterId,
    tick: Duration,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener: UnixListener = listen_unix(&agent.config().socket_path())?;
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut ticker = tokio::time::interval(tick);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                // Not awaited: a slow pass (a cleanup, a GC) must not keep
                // connections waiting. The agent runs one pass at a time and
                // drops a tick that arrives during one.
                let agent = agent.clone();
                tokio::task::spawn_blocking(move || agent.tick());
            }
            conn = listener.accept() => {
                let (stream, _) = conn?;
                let (agent, cluster, permits) = (agent.clone(), cluster_id.clone(), permits.clone());
                tokio::spawn(async move {
                    let _ = serve_connection(agent, Io::from_stream(stream), cluster, permits).await;
                });
            }
        }
    }
}
