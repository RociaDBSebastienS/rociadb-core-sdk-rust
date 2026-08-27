//! EN: Auth helpers for bearer tokens and API keys.
//! FR: Helpers auth pour tokens bearer et cles API.
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
    /// EN: Wakes the background task spawned by [`TokenManager::spawn_refresh`]
    /// as soon as possible, without the caller waiting for the network
    /// round trip. See [`TokenManager::request_refresh`]. A `notify_one()`
    /// call with no task currently waiting stores a permit that the next
    /// `notified().await` consumes immediately, so a request issued between
    /// two loop iterations of the background task is never lost.
    /// FR: Reveille des que possible la tache en arriere-plan lancee par
    /// [`TokenManager::spawn_refresh`], sans que l appelant attende le
    /// round-trip reseau. Voir [`TokenManager::request_refresh`]. Un appel
    /// `notify_one()` sans tache en attente stocke un permit que le
    /// prochain `notified().await` consomme immediatement, donc une
    /// demande emise entre deux iterations de boucle de la tache en
    /// arriere-plan n est jamais perdue.
    refresh_notify: Notify,
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
                refresh_notify: Notify::new(),
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

    /// EN: Request a token refresh without waiting for it.
    ///
    /// Unlike [`TokenManager::refresh_now`], this is **synchronous** and
    /// returns immediately: it only wakes the background task started by
    /// [`TokenManager::spawn_refresh`] (via a shared [`tokio::sync::Notify`])
    /// so it refreshes at the next opportunity, without the caller paying
    /// for the network round trip. If no background task is running (for
    /// example, [`TokenManager::spawn_refresh`] was never called), this is
    /// a harmless no-op — the notification is simply never consumed.
    /// FR: Demande un refresh de token sans l attendre.
    ///
    /// Contrairement a [`TokenManager::refresh_now`], ceci est
    /// **synchrone** et retourne immediatement : cela ne fait que reveiller
    /// la tache en arriere-plan lancee par
    /// [`TokenManager::spawn_refresh`] (via un [`tokio::sync::Notify`]
    /// partage) pour qu elle rafraichisse a la prochaine occasion, sans que
    /// l appelant paie le round-trip reseau. Si aucune tache en
    /// arriere-plan ne tourne (par exemple,
    /// [`TokenManager::spawn_refresh`] n a jamais ete appelee), ceci est un
    /// no-op inoffensif — la notification n est simplement jamais
    /// consommee.
    pub fn request_refresh(&self) {
        self.inner.refresh_notify.notify_one();
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
                    // EN: Woken by `TokenManager::request_refresh` (and thus
                    // `RociaDbClient::invalidate_auth_token`) so a caller can
                    // signal "do not trust the cached token" without paying
                    // for the refresh round trip itself — this background
                    // task absorbs that latency instead.
                    // FR: Reveillee par `TokenManager::request_refresh` (et
                    // donc `RociaDbClient::invalidate_auth_token`) pour
                    // qu un appelant puisse signaler "ne fais plus confiance
                    // au token en cache" sans payer lui-meme le round-trip
                    // de refresh — cette tache en arriere-plan absorbe cette
                    // latence a sa place.
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

/// EN: Constant-time byte comparison: always inspects every byte of the
/// longer input before returning, instead of short-circuiting on the first
/// mismatch (what `==` does on `&str`/`&[u8]`). Used by
/// [`ApiKeyInterceptor::call`] so the time a comparison takes cannot leak,
/// byte by byte, how much of the provided key matched the expected one.
/// FR: Comparaison d octets en temps constant : inspecte toujours chaque
/// octet de l entree la plus longue avant de retourner, plutot que de
/// s arreter au premier octet different (ce que fait `==` sur
/// `&str`/`&[u8]`). Utilisee par [`ApiKeyInterceptor::call`] pour que le
/// temps pris par une comparaison ne puisse pas fuiter, octet par octet,
/// la portion de la cle fournie qui correspondait a la cle attendue.
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

    // EN: `constant_time_eq` is the timing-safety fix for
    // `ApiKeyInterceptor::call`, which used to compare the provided and
    // expected keys with a plain `==` (short-circuits on the first
    // mismatched byte, leaking timing information about how much of the
    // key matched). These tests lock in that `constant_time_eq` is a
    // *correct* equality check — the property `==` already had — while
    // the always-scan-every-byte property it adds is a non-functional
    // guarantee enforced by the implementation itself, not observable
    // through a deterministic unit test.
    // FR: `constant_time_eq` est le correctif de securite temporelle pour
    // `ApiKeyInterceptor::call`, qui comparait auparavant la cle fournie
    // et la cle attendue avec un simple `==` (s arrete au premier octet
    // different, fuitant une information de timing sur la portion de la
    // cle qui correspondait). Ces tests verrouillent le fait que
    // `constant_time_eq` est une comparaison d egalite *correcte` — la
    // propriete que `==` avait deja — tandis que la propriete "inspecte
    // toujours chaque octet" qu elle ajoute est une garantie non
    // fonctionnelle assuree par l implementation elle-meme, non
    // observable via un test unitaire deterministe.

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

    /// EN: Builds a `TokenManager` directly from `TokenManagerInner`
    /// (rather than via `TokenManager::new`, which performs a real HTTP
    /// round trip) so these tests stay fully offline. `token_url` is
    /// deliberately not a well-formed URL: `reqwest` fails to parse it
    /// and any `.send()` call resolves immediately with an error, without
    /// ever opening a socket — so a test that calls `refresh_now` here
    /// stays deterministic and network-free too.
    /// FR: Construit un `TokenManager` directement a partir de
    /// `TokenManagerInner` (plutot que via `TokenManager::new`, qui
    /// effectue un veritable aller-retour HTTP) pour que ces tests restent
    /// entierement hors-ligne. `token_url` n est deliberement pas une URL
    /// bien formee : `reqwest` echoue a la parser et tout appel
    /// `.send()` se resout immediatement avec une erreur, sans jamais
    /// ouvrir de socket — donc un test qui appelle `refresh_now` ici
    /// reste lui aussi deterministe et sans reseau.
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

        // EN: `request_refresh` is a plain, non-async function — calling
        // it with no `.await` is itself part of what this test locks in:
        // unlike `refresh_now`, it must never make the caller wait for a
        // network round trip.
        // FR: `request_refresh` est une fonction simple, non-async —
        // l appeler sans `.await` fait elle-meme partie de ce que ce test
        // verrouille : contrairement a `refresh_now`, elle ne doit jamais
        // faire attendre l appelant pour un round-trip reseau.
        manager.request_refresh();

        // EN: A bounded wait: if `request_refresh` regressed into a
        // no-op, `notified()` would never resolve on its own and this
        // test would hang instead of failing outright — the timeout turns
        // that into a clean, fast failure.
        // FR: Une attente bornee : si `request_refresh` regressait en
        // no-op, `notified()` ne se resoudrait jamais seule et ce test
        // bloquerait au lieu d echouer proprement — le timeout transforme
        // cela en un echec net et rapide.
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
        // EN: This is the Rust mirror of the TypeScript-side defect fix
        // (`TokenManager.metadata()` used to drop a still-valid cached
        // token when a refresh attempted within the skew margin failed).
        // Rust's `refresh_now` already only overwrites `header_value` on
        // success — this test locks that in so a future change cannot
        // regress it silently.
        // FR: C est le miroir cote Rust du correctif du defaut cote
        // TypeScript (`TokenManager.metadata()` perdait un token encore
        // valide en cache quand un refresh tente dans la marge de skew
        // echouait). Le `refresh_now` de Rust n ecrase deja `header_value`
        // qu en cas de succes — ce test verrouille ce comportement pour
        // qu un changement futur ne puisse pas le faire regresser
        // silencieusement.
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
