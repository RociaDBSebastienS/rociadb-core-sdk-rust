//! EN: Auth helpers for bearer tokens and API keys.
//! FR: Helpers auth pour tokens bearer et cles API.
#![allow(clippy::doc_lazy_continuation)]

use crate::error::AuthResultExt;
use crate::{Result, RociaDbError};
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::{Request, Status, service::Interceptor};
use tracing::warn;

/// EN: Floor applied to the derived refresh interval, in case the IdP ever
/// advertises a very short (or zero) token lifetime.
/// FR: Plancher applique a l intervalle de refresh derive, au cas ou l IdP
/// annoncerait une duree de vie de token tres courte (voire nulle).
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// EN: Response payload for OAuth2 client credentials token.
/// FR: Payload de reponse pour token OAuth2 client credentials.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

/// EN: Fetch a token using client credentials.
/// FR: Recupere un token via client credentials.
///
/// EN: Arguments:
/// - `http`: Reqwest client instance.
/// - `token_url`: Token endpoint URL.
/// - `client_id`: OAuth2 client id.
/// - `client_secret`: OAuth2 client secret.
/// FR: Arguments:
/// - `http`: Instance du client Reqwest.
/// - `token_url`: URL de l endpoint token.
/// - `client_id`: Client id OAuth2.
/// - `client_secret`: Client secret OAuth2.
///
/// EN: Returns:
/// - `TokenResponse` on success.
/// FR: Returns:
/// - `TokenResponse` en cas de succes.
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

/// EN: Token manager with cached authorization header.
/// FR: Gestionnaire de token avec header authorisation en cache.
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
    /// EN: `expires_in` (seconds) from the most recently fetched token, as
    /// reported by the IdP. Drives [`TokenManager::refresh_interval`].
    /// FR: `expires_in` (secondes) du token le plus recemment recupere,
    /// tel que renvoye par l IdP. Pilote
    /// [`TokenManager::refresh_interval`].
    expires_in: AtomicU64,
}

impl TokenManager {
    /// EN: Create a new token manager and fetch the first token.
    /// FR: Cree un token manager et recupere le premier token.
    ///
    /// EN: Arguments:
    /// - `http`: Reqwest client instance.
    /// - `token_url`: Token endpoint URL.
    /// - `client_id`: OAuth2 client id.
    /// - `client_secret`: OAuth2 client secret.
    /// FR: Arguments:
    /// - `http`: Instance du client Reqwest.
    /// - `token_url`: URL de l endpoint token.
    /// - `client_id`: Client id OAuth2.
    /// - `client_secret`: Client secret OAuth2.
    ///
    /// EN: Returns:
    /// - `TokenManager` on success.
    /// FR: Returns:
    /// - `TokenManager` en cas de succes.
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
            }),
        })
    }

    /// EN: Derive a safe background-refresh interval from the token
    /// lifetime (`expires_in`, in seconds) most recently reported by the
    /// IdP, leaving margin so the token never actually expires between two
    /// refreshes: `max(expires_in * 2 / 3, 5s)`. With the IdP's fixed
    /// 600-second lifetime this yields a 400-second interval, i.e. a
    /// refresh with roughly a third of the token's lifetime still left.
    /// FR: Derive un intervalle de refresh en arriere-plan a partir de
    /// la duree de vie du token (`expires_in`, en secondes) la plus
    /// recemment renvoyee par l IdP, en laissant de la marge pour que le
    /// token n expire jamais reellement entre deux refresh :
    /// `max(expires_in * 2 / 3, 5s)`. Avec la duree de vie fixe de 600
    /// secondes de l IdP, cela donne un intervalle de 400 secondes, soit
    /// un refresh avec environ un tiers de la duree de vie du token
    /// restant.
    pub fn refresh_interval(&self) -> Duration {
        let expires_in = self.inner.expires_in.load(Ordering::Relaxed);
        let with_margin = expires_in.saturating_mul(2) / 3;
        Duration::from_secs(with_margin).max(MIN_REFRESH_INTERVAL)
    }

    /// EN: Create an interceptor that injects the bearer token.
    /// FR: Cree un interceptor qui injecte le bearer token.
    ///
    /// EN: Returns:
    /// - `BearerInterceptor` with token injection enabled.
    /// FR: Returns:
    /// - `BearerInterceptor` avec injection du token.
    pub fn interceptor(&self) -> BearerInterceptor {
        BearerInterceptor::new(Arc::clone(&self.inner.header_value))
    }

    /// EN: Force a token refresh immediately.
    /// FR: Force un refresh immediat du token.
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
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

    /// EN: Spawn a background refresh task.
    /// FR: Lance une tache de refresh en background.
    ///
    /// EN: Arguments:
    /// - `interval`: Refresh interval.
    /// FR: Arguments:
    /// - `interval`: Intervalle de refresh.
    ///
    /// EN: Returns:
    /// - `TokenRefreshGuard` to stop the task on drop.
    /// FR: Returns:
    /// - `TokenRefreshGuard` pour stopper la tache au drop.
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

// EN: Build the "authorization" metadata value.
// FR: Construit la valeur "authorization" pour les metadata.
fn build_header(token: &TokenResponse) -> Result<MetadataValue<Ascii>> {
    let bearer = format!("{} {}", token.token_type, token.access_token);
    bearer
        .parse::<MetadataValue<Ascii>>()
        .auth_context("invalid access token metadata value")
}

/// EN: Drop guard for the refresh task.
///
/// EN: Dropping this immediately stops the background refresh: bind it to
/// a variable that lives as long as the client needs auth to keep working
/// (`RociaDbBuilder::build` does this for you). `#[must_use]` catches the
/// common mistake of calling `spawn_refresh(..)` and discarding the
/// result, which would stop the refresh task right away.
/// FR: Guard de fin de vie pour la tache de refresh.
///
/// FR: Le laisser tomber (drop) stoppe immediatement le refresh en
/// arriere-plan : liez-le a une variable qui vit aussi longtemps que le
/// client a besoin que l auth continue de fonctionner
/// (`RociaDbBuilder::build` le fait pour vous). `#[must_use]` attrape
/// l erreur classique consistant a appeler `spawn_refresh(..)` en
/// jetant le resultat, ce qui stopperait la tache de refresh
/// immediatement.
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

/// EN: Interceptor that injects the bearer token when enabled.
/// FR: Interceptor qui injecte le bearer token si active.
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

    /// EN: Create an interceptor that does nothing.
    /// FR: Cree un interceptor inactif.
    ///
    /// EN: Returns:
    /// - `BearerInterceptor` with injection disabled.
    /// FR: Returns:
    /// - `BearerInterceptor` avec injection desactivee.
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

/// EN: Interceptor that validates an incoming API key.
/// FR: Interceptor qui valide une cle API entrante.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    expected_key: String,
    header: MetadataKey<Ascii>,
}

impl ApiKeyInterceptor {
    /// EN: Create an API key interceptor using the expected key.
    /// FR: Cree un interceptor API key avec la cle attendue.
    ///
    /// EN: Arguments:
    /// - `expected_key`: Expected API key string.
    /// FR: Arguments:
    /// - `expected_key`: Cle API attendue.
    ///
    /// EN: Returns:
    /// - `ApiKeyInterceptor`.
    /// FR: Returns:
    /// - `ApiKeyInterceptor`.
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
            Some(provided) if provided == self.expected_key => Ok(request),
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
