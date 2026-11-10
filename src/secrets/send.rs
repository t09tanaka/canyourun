//! Sending a run's secret files to a worker (spec 22.3, "転送").
//!
//! The bundle goes as `secret_bundle_put` requests, never as a blob: the blob
//! path carries a plain hash of the data and is meant for the object store.
//! Each request holds a few files with their HMAC digests (see
//! [`super::plan::plan_batches`]); the worker checks every digest before it
//! writes anything. A reconnect may simply send the bundle again: placement is
//! idempotent.

use super::plan::plan_batches;
use super::vault::HeldBundle;
use crate::ids::{AttemptId, CoordinatorEpoch, Generation, RequestId};
use crate::protocol::{CallError, Client};
use crate::wire::{Request, Response, SchemaVersion, SecretBundlePutRequest};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SendError {
    #[error(transparent)]
    Call(#[from] CallError),
    #[error("the worker did not accept the secret files: {0}")]
    Refused(&'static str),
}

/// The requests that carry `bundle`, in order.
pub fn batch_requests(
    bundle: &HeldBundle,
    coordinator_epoch: CoordinatorEpoch,
    attempt_id: &AttemptId,
    generation: Generation,
) -> Vec<Request> {
    let sizes: Vec<(usize, usize)> = bundle
        .files
        .iter()
        .map(|f| (f.path.len(), f.content.0.len()))
        .collect();
    plan_batches(&sizes)
        .into_iter()
        .map(|range| {
            Request::SecretBundlePut(SecretBundlePutRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                coordinator_epoch,
                attempt_id: attempt_id.clone(),
                generation,
                files: bundle.files[range].to_vec(),
            })
        })
        .collect()
}

/// Send every file; returns how many the worker placed. Each answer must say
/// every digest matched and every file of the request was placed.
pub async fn send_bundle(
    client: &Client,
    bundle: &HeldBundle,
    coordinator_epoch: CoordinatorEpoch,
    attempt_id: &AttemptId,
    generation: Generation,
    timeout: Duration,
) -> Result<u32, SendError> {
    let mut placed = 0;
    for request in batch_requests(bundle, coordinator_epoch, attempt_id, generation) {
        let Request::SecretBundlePut(r) = &request else {
            unreachable!("batch_requests builds secret_bundle_put only");
        };
        let sent = r.files.len() as u32;
        let Response::SecretBundlePut(resp) = client.call(request.clone(), timeout).await? else {
            return Err(SendError::Refused("another operation was answered"));
        };
        if !resp.verified {
            return Err(SendError::Refused("a digest did not match"));
        }
        if resp.placed != sent {
            return Err(SendError::Refused("not every file was placed"));
        }
        placed += resp.placed;
    }
    Ok(placed)
}
