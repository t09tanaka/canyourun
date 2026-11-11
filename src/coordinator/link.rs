//! How the coordinator reaches a worker: one request, one answer. The real
//! link is a protocol [`Client`]; tests put a worker of their own behind it.

use crate::error::ApiError;
use crate::protocol::{CallError, Client};
use crate::wire::{Request, Response};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Why a call has no answer to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkError {
    /// The worker answered with an error.
    Remote(ApiError),
    /// No answer: the request may or may not have arrived, and may or may
    /// not have been carried out. Never a reason to start the work elsewhere.
    Unconfirmed,
}

pub type LinkFuture<'a> = Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + 'a>>;

pub trait WorkerLink: Send + Sync {
    fn call(&self, request: Request) -> LinkFuture<'_>;
}

/// A worker behind a protocol connection.
pub struct ClientLink {
    client: Client,
    timeout: Duration,
}

impl ClientLink {
    pub fn new(client: Client, timeout: Duration) -> ClientLink {
        ClientLink { client, timeout }
    }
}

impl WorkerLink for ClientLink {
    fn call(&self, request: Request) -> LinkFuture<'_> {
        Box::pin(async move {
            match self.client.call(request, self.timeout).await {
                Ok(response) => Ok(response),
                Err(CallError::Remote(e)) => Err(LinkError::Remote(e)),
                // Not sent at all is treated like no answer: the attempt is
                // asked for again on the next pass, which decides.
                Err(_) => Err(LinkError::Unconfirmed),
            }
        })
    }
}
