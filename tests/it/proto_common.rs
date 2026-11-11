//! Shared helpers for the protocol tests: a fake worker built on the public API.
#![allow(dead_code)]

use remote_task_runner::error::ApiError;
use remote_task_runner::ids::*;
use remote_task_runner::protocol::hello::WorkerIdentity;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{
    accept, connect, Accepted, ClientConfig, Connected, Incoming, ServerConfig,
};
use remote_task_runner::wire::*;
use std::sync::Arc;
use std::time::Duration;

pub const T: Duration = Duration::from_secs(5);

pub fn identity(name: &str) -> WorkerIdentity {
    WorkerIdentity {
        worker_id: WorkerId::parse(name).unwrap(),
        worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
        agent_epoch: AgentEpoch(1),
        profile: "default".into(),
        profile_fingerprint: Digest::of_bytes(b"profile"),
        coordinator_epoch: CoordinatorEpoch(1),
    }
}

pub fn client_cfg(cluster: &ClusterId) -> ClientConfig {
    ClientConfig::new(
        cluster.clone(),
        CoordinatorId::generate(),
        CoordinatorEpoch(1),
    )
}

pub fn server_cfg(cluster: &ClusterId) -> ServerConfig {
    ServerConfig::new(cluster.clone(), identity("w1"))
}

pub async fn pair(cluster: &ClusterId) -> (Connected, Accepted) {
    let (a, b) = duplex_pair(64 * 1024);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(cluster)),
        accept(b, server_cfg(cluster))
    );
    (c.unwrap(), s.unwrap())
}

pub type Handler = Arc<dyn Fn(Request) -> Option<Result<Response, ApiError>> + Send + Sync>;

/// Serve every inbound request in its own task. A handler returning `None`
/// never answers (the responder is kept alive, so the connection stays open).
pub fn serve(mut incoming: Incoming, handler: Handler) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut silent = Vec::new();
        while let Some(inbound) = incoming.next().await {
            let handler = Arc::clone(&handler);
            match handler(inbound.request) {
                Some(Ok(r)) => {
                    tokio::spawn(async move {
                        let _ = inbound.responder.respond(r).await;
                    });
                }
                Some(Err(e)) => {
                    tokio::spawn(async move {
                        let _ = inbound.responder.fail(e).await;
                    });
                }
                None => silent.push(inbound.responder),
            }
        }
    })
}

pub fn ack_request() -> Request {
    Request::AckCollected(AckCollectedRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: AttemptId::generate(),
        generation: Generation(1),
        result_digest: Digest::of_bytes(b"r"),
    })
}

pub fn ack_response(req: &Request) -> Response {
    let Request::AckCollected(r) = req else {
        panic!("not an ack")
    };
    Response::AckCollected(AckCollectedResponse {
        schema_version: SchemaVersion::CURRENT,
        attempt_id: r.attempt_id.clone(),
        released: true,
    })
}

pub fn pseudo_random(n: usize) -> Vec<u8> {
    let mut x = 0x2545F491u32;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}
