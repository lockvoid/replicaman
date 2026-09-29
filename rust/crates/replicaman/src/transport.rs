//! HTTP transports preserve request bytes. Durable synchronization belongs to the engine.

use crate::error::{ReplicaError, ReplicaResult};
use crate::gzip;
use crate::protocol;
use crate::transport_retry::RetryDelay;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaEndpoint {
    Pull,
    Push,
    Verify,
}

impl ReplicaEndpoint {
    pub fn path(self) -> &'static str {
        match self {
            Self::Pull => "pull",
            Self::Push => "push",
            Self::Verify => "verify",
        }
    }
}

pub trait ReplicaTransport: Send + Sync {
    fn exchange(
        &self,
        endpoint: ReplicaEndpoint,
        body: Vec<u8>,
    ) -> BoxFuture<'_, ReplicaResult<Vec<u8>>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoWireTransport;

impl NoWireTransport {
    pub const REASON: &'static str = "this replica has no wire";
}

impl ReplicaTransport for NoWireTransport {
    fn exchange(&self, _: ReplicaEndpoint, _: Vec<u8>) -> BoxFuture<'_, ReplicaResult<Vec<u8>>> {
        Box::pin(async { Err(ReplicaError::Transport(Self::REASON.into())) })
    }
}

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub retry_after: Option<String>,
}

/// Hosts must preserve Retry-After and enforce response_limit while streaming and cancel I/O when the
/// returned future is dropped. Never buffer an unbounded body before checking it.
pub trait HttpClient: Send + Sync {
    fn request<'a>(
        &'a self,
        method: &'a str,
        url: &'a str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        response_limit: usize,
    ) -> BoxFuture<'a, ReplicaResult<HttpResponse>>;
}

type TokenSource = Box<dyn Fn() -> Option<String> + Send + Sync>;
type HeaderSource = Box<dyn Fn() -> Vec<(String, String)> + Send + Sync>;

pub struct HttpReplicaTransport<C: HttpClient> {
    retry_delay: RetryDelay,
    base_url: String,
    client: C,
    token: TokenSource,
    headers: HeaderSource,
}

impl<C: HttpClient> HttpReplicaTransport<C> {
    pub fn new(
        base_url: impl Into<String>,
        client: C,
        token: TokenSource,
        headers: HeaderSource,
    ) -> Self {
        Self {
            retry_delay: RetryDelay::default(),
            base_url: base_url.into(),
            client,
            token,
            headers,
        }
    }
}

impl<C: HttpClient> ReplicaTransport for HttpReplicaTransport<C> {
    fn exchange(
        &self,
        endpoint: ReplicaEndpoint,
        body: Vec<u8>,
    ) -> BoxFuture<'_, ReplicaResult<Vec<u8>>> {
        Box::pin(async move {
            self.retry_delay.wait().await;
            let url = format!(
                "{}/{}",
                self.base_url.trim_end_matches('/'),
                endpoint.path()
            );
            let mut headers = vec![
                ("Content-Type".into(), "application/json".into()),
                ("Content-Encoding".into(), "gzip".into()),
            ];
            if let Some(token) = (self.token)() {
                headers.push(("Authorization".into(), format!("Bearer {token}")));
            }
            headers.extend((self.headers)());
            let compressed = gzip::compress(&body)
                .map_err(|error| ReplicaError::Codec(format!("gzip encoding failed: {error}")))?;
            let response = self
                .client
                .request(
                    "POST",
                    &url,
                    headers,
                    Some(compressed),
                    protocol::RESPONSE_BYTES,
                )
                .await?;
            let status = response.status;
            if status == 429 || status >= 500 {
                self.retry_delay.record(response.retry_after.as_deref());
            }
            let response = response.body;
            if response.len() > protocol::RESPONSE_BYTES {
                return Err(ReplicaError::Transport(
                    "Response exceeded size limit".into(),
                ));
            }
            if status == 429 || status >= 500 {
                let detail = String::from_utf8_lossy(&response[..response.len().min(512)]);
                return Err(ReplicaError::Transport(format!("HTTP {status}: {detail}")));
            }
            if status != 200 {
                #[derive(serde::Deserialize)]
                struct Failure {
                    error: String,
                    message: Option<String>,
                }
                let failure: Failure = protocol::decode(&response)?;
                return Err(ReplicaError::Protocol {
                    code: failure.error.clone(),
                    message: format!(
                        "HTTP {status}: {}",
                        failure.message.unwrap_or(failure.error)
                    ),
                });
            }
            Ok(response)
        })
    }
}
