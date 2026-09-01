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
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status, service::Interceptor};
use tracing::warn;

/// Floor applied to the derived refresh interval, in case the IdP ever
/// advertises a very short (or zero) token lifetime.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Response payload for OAuth2 client credentials token.
#[non_exhaustive]
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

#[cfg(test)]
mod tests {
    use super::{TokenManager, TokenManagerInner, TokenResponse, build_header};
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tonic::metadata::{Ascii, MetadataValue};

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
