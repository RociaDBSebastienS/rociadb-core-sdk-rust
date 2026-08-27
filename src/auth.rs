//! Auth helpers for bearer tokens and API keys.
#![allow(clippy::doc_lazy_continuation)]

use crate::error::AuthResultExt;
use crate::{Result, RociaDbError};
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::{Request, Status, service::Interceptor};
use tracing::warn;

/// Floor applied to the derived refresh interval, in case the IdP ever
/// advertises a very short (or zero) token lifetime.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Response payload for OAuth2 client credentials token.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

/// Fetch a token using client credentials.
pub async fn fetch_token(
    http: &reqwest::Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<TokenResponse> {
    let res = http
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .auth_context("token request failed")?
        .error_for_status()
        .auth_context("token endpoint returned error")?;

    res.json::<TokenResponse>()
        .await
        .auth_context("failed to parse token response")
}

/// Token manager with cached authorization header.
#[derive(Clone)]
pub struct TokenManager {
    inner: Arc<TokenManagerInner>,
}

struct TokenManagerInner {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    header_value: Arc<RwLock<MetadataValue<Ascii>>>,
    /// `expires_in` (seconds) from the most recently fetched token, as
    /// reported by the IdP. Drives [`TokenManager::refresh_interval`].
    expires_in: AtomicU64,
    /// Wakes the background task spawned by [`TokenManager::spawn_refresh`]
    /// as soon as possible, without the caller waiting for the network
    /// round trip. See [`TokenManager::request_refresh`]. A `notify_one()`
    /// call with no task currently waiting stores a permit that the next
    /// `notified().await` consumes immediately, so a request issued between
    /// two loop iterations of the background task is never lost.
    refresh_notify: Notify,
}

impl TokenManager {
    /// Create a new token manager and fetch the first token.
    pub async fn new(
        http: reqwest::Client,
        token_url: String,
        client_id: String,
        client_secret: String,
    ) -> Result<Self> {
        let token = fetch_token(&http, &token_url, &client_id, &client_secret).await?;
        let expires_in = AtomicU64::new(token.expires_in);
        let header_value = Arc::new(RwLock::new(build_header(&token)?));

        Ok(Self {
            inner: Arc::new(TokenManagerInner {
                http,
                token_url,
                client_id,
                client_secret,
                header_value,
                expires_in,
                refresh_notify: Notify::new(),
            }),
        })
    }

    /// Derive a safe background-refresh interval from the token lifetime
    /// (`expires_in`, in seconds) most recently reported by the IdP,
    /// leaving margin so the token never actually expires between two
    /// refreshes: `max(expires_in * 2 / 3, 5s)`. With the IdP's fixed
    /// 600-second lifetime this yields a 400-second interval, i.e. a
    /// refresh with roughly a third of the token's lifetime still left.
    pub fn refresh_interval(&self) -> Duration {
        let expires_in = self.inner.expires_in.load(Ordering::Relaxed);
        let with_margin = expires_in.saturating_mul(2) / 3;
        Duration::from_secs(with_margin).max(MIN_REFRESH_INTERVAL)
    }

    /// Create an interceptor that injects the bearer token.
    pub fn interceptor(&self) -> BearerInterceptor {
        BearerInterceptor::new(Arc::clone(&self.inner.header_value))
    }

    /// Force a token refresh immediately.
    pub async fn refresh_now(&self) -> Result<()> {
        let token = fetch_token(
            &self.inner.http,
            &self.inner.token_url,
            &self.inner.client_id,
            &self.inner.client_secret,
        )
        .await?;
        let header = build_header(&token)?;
        self.inner
            .expires_in
            .store(token.expires_in, Ordering::Relaxed);
        let mut guard = self
            .inner
            .header_value
            .write()
            .map_err(|_| RociaDbError::auth("token header lock poisoned"))?;
        *guard = header;
        Ok(())
    }

    /// Request a token refresh without waiting for it.
    ///
    /// Unlike [`TokenManager::refresh_now`], this is **synchronous** and
    /// returns immediately: it only wakes the background task started by
    /// [`TokenManager::spawn_refresh`] (via a shared [`tokio::sync::Notify`])
    /// so it refreshes at the next opportunity, without the caller paying
    /// for the network round trip. If no background task is running (for
    /// example, [`TokenManager::spawn_refresh`] was never called), this is
    /// a harmless no-op — the notification is simply never consumed.
    pub fn request_refresh(&self) {
        self.inner.refresh_notify.notify_one();
    }

    /// Spawn a background refresh task. Returns a [`TokenRefreshGuard`]
    /// that stops the task on drop.
    pub fn spawn_refresh(&self, interval: Duration) -> TokenRefreshGuard {
        let manager = self.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = manager.refresh_now().await {
                            warn!(error = %err, "token refresh failed");
                        }
                    }
                    // Woken by `TokenManager::request_refresh` (and thus
                    // `RociaDbClient::invalidate_auth_token`) so a caller can
                    // signal "do not trust the cached token" without paying
                    // for the refresh round trip itself — this background
                    // task absorbs that latency instead.
                    _ = manager.inner.refresh_notify.notified() => {
                        if let Err(err) = manager.refresh_now().await {
                            warn!(error = %err, "requested token refresh failed");
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        TokenRefreshGuard {
            shutdown: Some(shutdown_tx),
            task,
        }
    }
}

fn build_header(token: &TokenResponse) -> Result<MetadataValue<Ascii>> {
    let bearer = format!("{} {}", token.token_type, token.access_token);
    bearer
        .parse::<MetadataValue<Ascii>>()
        .auth_context("invalid access token metadata value")
}

/// Drop guard for the refresh task.
///
/// Dropping this immediately stops the background refresh: bind it to a
/// variable that lives as long as the client needs auth to keep working
/// (`RociaDbBuilder::build` does this for you). `#[must_use]` catches the
/// common mistake of calling `spawn_refresh(..)` and discarding the result,
/// which would stop the refresh task right away.
#[must_use = "dropping the guard immediately stops the background token refresh task"]
pub struct TokenRefreshGuard {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for TokenRefreshGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

/// Interceptor that injects the bearer token when enabled.
#[derive(Clone)]
pub struct BearerInterceptor {
    header_value: Option<Arc<RwLock<MetadataValue<Ascii>>>>,
}

impl BearerInterceptor {
    fn new(header_value: Arc<RwLock<MetadataValue<Ascii>>>) -> Self {
        Self {
            header_value: Some(header_value),
        }
    }

    /// Create an interceptor that does nothing.
    pub(crate) fn disabled() -> Self {
        Self { header_value: None }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(header_value) = self.header_value.as_ref() {
            let header_value = header_value
                .read()
                .map_err(|_| Status::internal("token header lock poisoned"))?
                .clone();
            req.metadata_mut().insert("authorization", header_value);
        }
        Ok(req)
    }
}

/// Interceptor that validates an incoming API key.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    expected_key: String,
    header: MetadataKey<Ascii>,
}

impl ApiKeyInterceptor {
    /// Create an API key interceptor using the expected key.
    pub fn new(expected_key: String) -> Self {
        Self {
            expected_key,
            header: MetadataKey::from_static("x-api-key"),
        }
    }
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match request
            .metadata()
            .get(&self.header)
            .and_then(|value| value.to_str().ok())
        {
            Some(provided)
                if constant_time_eq(provided.as_bytes(), self.expected_key.as_bytes()) =>
            {
                Ok(request)
            }
            Some(_) => {
                warn!("invalid API key received");
                Err(Status::unauthenticated("invalid API key"))
            }
            None => {
                warn!("missing API key");
                Err(Status::unauthenticated("missing API key"))
            }
        }
    }
}

/// Constant-time byte comparison: always inspects every byte of the longer
/// input before returning, instead of short-circuiting on the first
/// mismatch (what `==` does on `&str`/`&[u8]`). Used by
/// [`ApiKeyInterceptor::call`] so the time a comparison takes cannot leak,
/// byte by byte, how much of the provided key matched the expected one.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() != b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        let byte_a = a.get(i).copied().unwrap_or(0);
        let byte_b = b.get(i).copied().unwrap_or(0);
        diff |= byte_a ^ byte_b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::{
        ApiKeyInterceptor, TokenManager, TokenManagerInner, TokenResponse, build_header,
        constant_time_eq,
    };
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tonic::Request;
    use tonic::metadata::{Ascii, MetadataValue};
    use tonic::service::Interceptor;

    // These tests lock in that `constant_time_eq` is a *correct* equality
    // check. The always-scan-every-byte property that gives it its
    // timing-safety guarantee is enforced by the implementation itself and
    // is not observable through a deterministic unit test.

    #[test]
    fn constant_time_eq_accepts_identical_byte_strings() {
        assert!(constant_time_eq(b"super-secret-key", b"super-secret-key"));
    }

    #[test]
    fn constant_time_eq_accepts_two_empty_slices() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn constant_time_eq_rejects_a_single_differing_byte_at_any_position() {
        let expected = b"abcdefgh";
        for i in 0..expected.len() {
            let mut candidate = expected.to_vec();
            candidate[i] ^= 0xFF;
            assert!(
                !constant_time_eq(&candidate, expected),
                "byte {i} differs from expected but was accepted as equal"
            );
        }
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths_even_when_one_is_a_prefix_of_the_other() {
        assert!(!constant_time_eq(b"short", b"short-but-longer"));
        assert!(!constant_time_eq(b"short-but-longer", b"short"));
    }

    fn request_with_api_key(key: Option<&str>) -> Request<()> {
        let mut request = Request::new(());
        if let Some(key) = key {
            request.metadata_mut().insert(
                "x-api-key",
                key.parse::<MetadataValue<Ascii>>()
                    .expect("test key must be a valid ascii metadata value"),
            );
        }
        request
    }

    #[test]
    fn api_key_interceptor_accepts_the_matching_key() {
        let mut interceptor = ApiKeyInterceptor::new("expected-key".to_string());
        let request = request_with_api_key(Some("expected-key"));
        assert!(interceptor.call(request).is_ok());
    }

    #[test]
    fn api_key_interceptor_rejects_a_mismatched_key() {
        let mut interceptor = ApiKeyInterceptor::new("expected-key".to_string());
        let request = request_with_api_key(Some("wrong-key"));
        let status = interceptor
            .call(request)
            .expect_err("a mismatched key must be rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn api_key_interceptor_rejects_a_missing_key() {
        let mut interceptor = ApiKeyInterceptor::new("expected-key".to_string());
        let request = request_with_api_key(None);
        let status = interceptor
            .call(request)
            .expect_err("a missing key must be rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    /// Builds a `TokenManager` directly from `TokenManagerInner` (rather
    /// than via `TokenManager::new`, which performs a real HTTP round trip)
    /// so these tests stay fully offline. `token_url` is deliberately not a
    /// well-formed URL: `reqwest` fails to parse it and any `.send()` call
    /// resolves immediately with an error, without ever opening a socket —
    /// so a test that calls `refresh_now` here stays deterministic and
    /// network-free too.
    fn offline_token_manager(header_value: MetadataValue<Ascii>) -> TokenManager {
        TokenManager {
            inner: Arc::new(TokenManagerInner {
                http: reqwest::Client::new(),
                token_url: "this is not a url".to_string(),
                client_id: "unused-client-id".to_string(),
                client_secret: "unused-client-secret".to_string(),
                header_value: Arc::new(RwLock::new(header_value)),
                expires_in: AtomicU64::new(600),
                refresh_notify: Notify::new(),
            }),
        }
    }

    fn sample_header(access_token: &str) -> MetadataValue<Ascii> {
        build_header(&TokenResponse {
            access_token: access_token.to_string(),
            expires_in: 600,
            token_type: "Bearer".to_string(),
        })
        .expect("a well-formed token response must build a valid header")
    }

    #[tokio::test]
    async fn request_refresh_stores_a_wake_permit_consumed_by_the_next_notified_await() {
        let manager = offline_token_manager(sample_header("token"));

        // `request_refresh` is a plain, non-async function — calling it
        // with no `.await` is itself part of what this test locks in:
        // unlike `refresh_now`, it must never make the caller wait for a
        // network round trip.
        manager.request_refresh();

        // A bounded wait: if `request_refresh` regressed into a no-op,
        // `notified()` would never resolve on its own and this test would
        // hang instead of failing outright — the timeout turns that into a
        // clean, fast failure.
        tokio::time::timeout(
            Duration::from_millis(200),
            manager.inner.refresh_notify.notified(),
        )
        .await
        .expect(
            "request_refresh must store a wake permit that the next notified().await consumes \
             immediately, without needing a concurrently waiting task",
        );
    }

    #[tokio::test]
    async fn refresh_now_never_replaces_a_still_valid_cached_token_when_the_refresh_fails() {
        let original_header = sample_header("still-valid-token");
        let manager = offline_token_manager(original_header.clone());

        let result = manager.refresh_now().await;
        assert!(
            result.is_err(),
            "a malformed token_url must make refresh_now fail"
        );

        let current = manager
            .inner
            .header_value
            .read()
            .expect("header lock must not be poisoned");
        assert_eq!(
            *current, original_header,
            "a failed refresh must never replace a still-cached, still-valid header value"
        );
    }
}
