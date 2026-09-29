//! NEW suite — no upstream counterpart (upstream exercised `HTTPReplicaTransport`
//! only through `DecodeToleranceTests`). Every byte of encode/decode/gzip/auth/
//! header logic lives in-crate, so this pins it through a stub `HttpClient`.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::ReplicaError;
use crate::gzip;
use crate::transport::ReplicaEndpoint;
use crate::transport::{
    BoxFuture, HttpClient, HttpReplicaTransport, HttpResponse, ReplicaTransport,
};

#[derive(Clone, Debug)]
struct Request {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    response_limit: usize,
}

#[derive(Default)]
struct StubClient {
    requests: Mutex<Vec<Request>>,
    answer: Mutex<Option<HttpResponse>>,
}

impl StubClient {
    fn answering(status: u16, body: &[u8]) -> Arc<Self> {
        let client = Arc::new(Self::default());
        *client.answer.lock() = Some(HttpResponse {
            status,
            body: body.to_vec(),
            retry_after: None,
        });
        client
    }
}

impl HttpClient for Arc<StubClient> {
    fn request<'a>(
        &'a self,
        method: &'a str,
        url: &'a str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        response_limit: usize,
    ) -> BoxFuture<'a, crate::error::ReplicaResult<HttpResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request {
                method: method.to_owned(),
                url: url.to_owned(),
                headers,
                body,
                response_limit,
            });
            Ok(self.answer.lock().clone().unwrap_or(HttpResponse {
                status: 200,
                body: b"{}".to_vec(),
                retry_after: None,
            }))
        })
    }
}

fn transport(
    client: Arc<StubClient>,
    token: Option<&'static str>,
) -> HttpReplicaTransport<Arc<StubClient>> {
    HttpReplicaTransport::new(
        "https://api.test/replica/",
        client,
        Box::new(move || token.map(str::to_owned)),
        Box::new(|| vec![("X-Device".to_owned(), "mac".to_owned())]),
    )
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.as_str())
}

fn assert_bounded_authenticated_post(sent: &Request, endpoint: ReplicaEndpoint, request: &[u8]) {
    assert_eq!(sent.method, "POST");
    assert_eq!(
        sent.url,
        format!("https://api.test/replica/{}", endpoint.path())
    );
    assert_eq!(header(sent, "Authorization"), Some("Bearer tok-1"));
    assert_eq!(header(sent, "X-Device"), Some("mac"));
    assert_eq!(header(sent, "Content-Type"), Some("application/json"));
    assert_eq!(header(sent, "Content-Encoding"), Some("gzip"));
    assert_eq!(
        gzip::decompress(sent.body.as_ref().unwrap()).unwrap(),
        request
    );
    assert_eq!(sent.response_limit, crate::protocol::RESPONSE_BYTES);
}

#[tokio::test]
async fn every_endpoint_preserves_request_bytes_and_uses_bounded_authenticated_post() {
    use ReplicaEndpoint::{Pull, Push, Verify};
    let client = StubClient::answering(200, b"opaque response");
    let transport = transport(client.clone(), Some("tok-1"));
    let request = r#"{"shard":"user","text":"日本語","cursor":"9223372036854775807"}"#.as_bytes();
    for endpoint in [Push, Pull, Verify] {
        let response = transport.exchange(endpoint, request.to_vec()).await;
        assert_eq!(response.unwrap(), b"opaque response");
        let sent = client.requests.lock().last().unwrap().clone();
        assert_bounded_authenticated_post(&sent, endpoint, request);
    }
}

#[tokio::test]
async fn credentials_are_read_for_each_request() {
    let client = StubClient::answering(200, b"{}");
    let token = Arc::new(Mutex::new(None::<String>));
    let source = token.clone();
    let transport = HttpReplicaTransport::new(
        "https://api.test/replica",
        client.clone(),
        Box::new(move || source.lock().clone()),
        Box::new(Vec::new),
    );
    transport
        .exchange(ReplicaEndpoint::Pull, b"{}".to_vec())
        .await
        .unwrap();
    *token.lock() = Some("new-owner".into());
    transport
        .exchange(ReplicaEndpoint::Pull, b"{}".to_vec())
        .await
        .unwrap();
    let requests = client.requests.lock();
    assert_eq!(header(&requests[0], "Authorization"), None);
    assert_eq!(
        header(&requests[1], "Authorization"),
        Some("Bearer new-owner")
    );
}

#[test]
fn retry_after_seconds_dates_and_invalid_advice() {
    use crate::transport_retry::retry_after;
    use std::time::{Duration, SystemTime};
    let now = SystemTime::UNIX_EPOCH;
    assert_eq!(retry_after(Some("12"), now), Some(Duration::from_secs(12)));
    assert_eq!(
        retry_after(Some("Thu, 01 Jan 1970 00:00:12 GMT"), now),
        Some(Duration::from_secs(12))
    );
    assert_eq!(
        retry_after(
            Some("Thu, 01 Jan 1970 00:00:00 GMT"),
            now + Duration::from_secs(1)
        ),
        Some(Duration::ZERO)
    );
    for value in ["-1", "1.5", "NaN", "Infinity", "tomorrow", ""] {
        assert_eq!(retry_after(Some(value), now), None, "{value}");
    }
}

#[tokio::test]
async fn cancelling_a_throttled_exchange_sends_no_request() {
    let client = StubClient::answering(429, b"rate limited");
    client.answer.lock().as_mut().unwrap().retry_after = Some("86400".into());
    let transport = transport(client.clone(), None);
    assert!(matches!(
        transport
            .exchange(ReplicaEndpoint::Pull, b"{}".to_vec())
            .await,
        Err(ReplicaError::Transport(_))
    ));
    let waiting = transport.exchange(ReplicaEndpoint::Pull, b"{}".to_vec());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), waiting)
            .await
            .is_err()
    );
    assert_eq!(client.requests.lock().len(), 1);
}

#[tokio::test]
async fn structured_refusals_preserve_the_server_failure_code() {
    let client = StubClient::answering(
        409,
        br#"{"error":"MutationChanged","message":"MutationChanged","id":"0199"}"#,
    );
    let error = transport(client, None)
        .exchange(ReplicaEndpoint::Push, b"{}".to_vec())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ReplicaError::Protocol {
            code: "MutationChanged".into(),
            message: "HTTP 409: MutationChanged".into(),
        }
    );
}

#[tokio::test]
async fn malformed_error_bodies_never_become_successful_mutation_results() {
    for bytes in [b"gateway down".as_slice(), b"{}", b"null"] {
        let client = StubClient::answering(503, bytes);
        assert!(
            transport(client, None)
                .exchange(ReplicaEndpoint::Push, b"{}".to_vec())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn an_oversized_response_is_refused_even_if_the_host_ignored_its_limit() {
    let client = StubClient::answering(200, &vec![b'x'; crate::protocol::RESPONSE_BYTES + 1]);
    assert!(
        matches!(transport(client, None).exchange(ReplicaEndpoint::Pull, b"{}".to_vec()).await,
        Err(ReplicaError::Transport(reason)) if reason.contains("size limit"))
    );
}
