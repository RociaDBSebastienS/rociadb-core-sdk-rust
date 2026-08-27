//! EN: Rocia DB SDK client for gRPC upstream services.
//! FR: Client SDK Rocia DB pour les services gRPC upstream.
//!
//! EN: Quick example:
//! FR: Exemple rapide:
//! ```rust,no_run
//! use rocia_db_sdk::RociaDbBuilder;
//! use serde_json::json;
//!
//! # #[tokio::main]
//! # async fn main() -> rocia_db_sdk::Result<()> {
//! let client = RociaDbBuilder::new()
//!     .host("http://127.0.0.1:50051")
//!     .auth_client_credentials(
//!         "https://example.com/token",
//!         "client-id",
//!         "client-secret",
//!     )
//!     .build()
//!     .await?;
//!
//! client
//!     .create_document(
//!         "tenant-1",
//!         "products",
//!         "sku-123",
//!         json!({"sku": "sku-123"}),
//!         Some("product".to_string()),
//!         Some("products".to_string()),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
#![allow(clippy::doc_lazy_continuation)]

pub mod auth;
mod document;
mod error;
pub mod file;
pub mod graph;
#[doc(hidden)]
pub mod pb;
mod tenant;

pub use error::{Result, RociaDbError};
pub use file::FileUploadOptions;
pub use graph::{NeighborNode, NeighborPage};
pub use pb::upstream::v1::{
    CollectionInfo, DownloadResponse, Neighbor, StatResponse, UploadRequest,
};

use crate::error::{AuthResultExt, ConnectionResultExt, JsonResultExt, StatusResultExt};
use crate::pb::upstream::v1::document_service_client::DocumentServiceClient;
use crate::pb::upstream::v1::file_service_client::FileServiceClient;
use crate::pb::upstream::v1::graph_service_client::GraphServiceClient;
use crate::pb::upstream::v1::tenant_service_client::TenantServiceClient;
use crate::pb::upstream::v1::{
    AddEdgeRequest, FindByFieldRequest, GetDocRequest, GetNodeRequest, ListCollectionsRequest,
    ListDocRequest, PageRequest, PutDocRequest, PutNodeRequest, QueryDocRequest, QueryFilter,
    QueryOperator, QuerySort, SortDirection,
};
use auth::{BearerInterceptor, TokenManager, TokenRefreshGuard};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// EN: Max concurrent in-flight requests for batch operations.
/// FR: Nombre max de requetes simultanees pour les batchs.
const CONCURRENT_REQUESTS: usize = 10;
/// EN: Page size used when the caller does not provide one.
/// FR: Taille de page utilisee quand l appelant n en fournit pas.
const DEFAULT_PAGE_SIZE: u32 = 20;
const AUTH_TOKEN_URL_ENV: &str = "AUTH_TOKEN_URL";
const AUTH_CLIENT_ID_ENV: &str = "AUTH_CLIENT_ID";
const AUTH_CLIENT_SECRET_ENV: &str = "AUTH_CLIENT_SECRET";

#[derive(Debug, Clone)]
enum BuilderAuthConfig {
    Enabled {
        token_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    },
    Disabled,
}

/// EN: Builder for RociaDbClient.
/// FR: Builder pour RociaDbClient.
#[derive(Debug)]
pub struct RociaDbBuilder {
    host: Option<String>,
    auth: BuilderAuthConfig,
}

/// EN: gRPC client for document, graph, file, and tenant services.
/// FR: Client gRPC pour les services document, graph, file et tenant.
///
/// EN: `Clone` is cheap: clones share the same underlying channel, token
/// manager, and background token-refresh task (the refresh task keeps
/// running until every clone has been dropped). Every method takes `&self`
/// (not `&mut self`): each call clones the cheap, `Arc`-backed inner
/// service client before issuing its RPC, the same way the batch helpers
/// ([`RociaDbClient::put_nodes`], [`RociaDbClient::add_edges`]) always
/// have. A shared `RociaDbClient` behind an `Arc` therefore needs no
/// `Mutex` to be usable concurrently.
/// FR: `Clone` est peu couteux : les clones partagent le meme channel sous-
/// jacent, le meme gestionnaire de token, et la meme tache de refresh en
/// arriere-plan (la tache continue de tourner tant qu il reste un clone).
/// Chaque methode prend `&self` (pas `&mut self`) : chaque appel clone le
/// client de service interne, peu couteux car adosse a un `Arc`, avant
/// d emettre son RPC, comme le font deja les helpers de batch
/// ([`RociaDbClient::put_nodes`], [`RociaDbClient::add_edges`]). Un
/// `RociaDbClient` partage derriere un `Arc` n a donc besoin d aucun
/// `Mutex` pour etre utilisable de facon concurrente.
#[derive(Clone)]
pub struct RociaDbClient {
    upstream_document: DocumentServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_graph: GraphServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_file: FileServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_tenant: TenantServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    /// EN: `None` when auth is disabled. Used to service
    /// [`RociaDbClient::refresh_auth_token`].
    /// FR: `None` quand l auth est desactivee. Utilise pour
    /// [`RociaDbClient::refresh_auth_token`].
    token_manager: Option<TokenManager>,
    /// EN: Keeps the background token-refresh task alive for as long as
    /// this client (or any of its clones) exists. Never read directly,
    /// hence the leading underscore; it exists purely for its `Drop`.
    /// FR: Maintient la tache de refresh de token en arriere-plan tant que
    /// ce client (ou l un de ses clones) existe. Jamais lu directement,
    /// d ou le prefixe underscore ; elle n existe que pour son `Drop`.
    _token_refresh_guard: Option<Arc<TokenRefreshGuard>>,
}

/// EN: One page of listed items with the cursor for the next page.
/// FR: Une page d elements listes avec le curseur de la page suivante.
///
/// EN: `next_cursor` is `None` once the server has no further page. The cursor
/// is opaque and must be passed back unchanged.
/// FR: `next_cursor` vaut `None` quand le serveur n a plus de page. Le curseur
/// est opaque et doit etre reutilise tel quel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// EN: Build a `PageRequest` applying the SDK default page size.
///
/// EN: The server rejects `limit == 0` with `INVALID_ARGUMENT`; this is
/// rejected here too so the caller gets an immediate, clear error instead
/// of a round trip to the server. The server's own page-size ceiling
/// (`limits.max_page_size`, 200 by default) is intentionally not
/// duplicated here — it is configurable server-side, so any positive limit
/// is forwarded unchanged and the server has the final say.
/// FR: Construit un `PageRequest` en appliquant la taille de page par
/// defaut.
///
/// FR: Le serveur rejette `limit == 0` avec `INVALID_ARGUMENT` ; c est
/// aussi rejete ici pour une erreur immediate et claire plutot qu un
/// aller-retour reseau. Le plafond de taille de page cote serveur
/// (`limits.max_page_size`, 200 par defaut) n est volontairement pas
/// duplique ici — il est configurable cote serveur, donc toute limite
/// positive est transmise telle quelle et c est le serveur qui tranche.
pub(crate) fn page_request(
    limit: Option<u32>,
    cursor: Option<&str>,
) -> Result<Option<PageRequest>> {
    if limit == Some(0) {
        return Err(RociaDbError::validation(
            "page limit must be greater than zero",
        ));
    }
    Ok(Some(PageRequest {
        limit: limit.unwrap_or(DEFAULT_PAGE_SIZE),
        cursor: cursor.unwrap_or_default().to_string(),
    }))
}

/// EN: Map the protobuf empty-string cursor to `None`.
/// FR: Convertit le curseur protobuf vide en `None`.
pub(crate) fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// EN: Validate that `node_label` and `node_graph` are either both set or
/// both absent, before any network call. Pulled out of
/// [`RociaDbClient::create_document`] as a pure function so the rule is
/// unit-testable without a live client.
/// FR: Valide que `node_label` et `node_graph` sont soit tous les deux
/// renseignes, soit tous les deux absents, avant tout appel reseau. Extraite
/// de [`RociaDbClient::create_document`] en fonction pure pour que la regle
/// soit testable unitairement sans client reel.
fn validate_node_binding(node_label: &Option<String>, node_graph: &Option<String>) -> Result<()> {
    if node_label.is_some() != node_graph.is_some() {
        return Err(RociaDbError::validation(format!(
            "node_label and node_graph must be provided together (got node_label={:?}, node_graph={:?})",
            node_label, node_graph
        )));
    }
    Ok(())
}

/// EN: Supported document query operators exposed by the SDK.
/// FR: Operateurs de requetage document supportes par le SDK.
#[derive(Debug, Clone, Copy)]
pub enum DocumentQueryOperator {
    Eq,
    In,
    Contains,
}

impl DocumentQueryOperator {
    fn as_proto(self) -> i32 {
        match self {
            Self::Eq => QueryOperator::Eq as i32,
            Self::In => QueryOperator::In as i32,
            Self::Contains => QueryOperator::Contains as i32,
        }
    }
}

/// EN: Supported document sort directions exposed by the SDK.
/// FR: Directions de tri document supportees par le SDK.
#[derive(Debug, Clone, Copy)]
pub enum DocumentQuerySortDirection {
    Asc,
    Desc,
}

impl DocumentQuerySortDirection {
    fn as_proto(self) -> i32 {
        match self {
            Self::Asc => SortDirection::Asc as i32,
            Self::Desc => SortDirection::Desc as i32,
        }
    }
}

/// EN: Filter definition for `QueryDoc`.
/// FR: Definition d'un filtre pour `QueryDoc`.
#[derive(Debug, Clone)]
pub struct DocumentQueryFilter {
    pub field: String,
    pub operator: DocumentQueryOperator,
    pub values: Vec<Value>,
}

/// EN: Sort definition for `QueryDoc`.
/// FR: Definition d'un tri pour `QueryDoc`.
#[derive(Debug, Clone)]
pub struct DocumentQuerySort {
    pub field: String,
    pub direction: DocumentQuerySortDirection,
}

impl Default for RociaDbBuilder {
    fn default() -> Self {
        Self {
            host: Some("http://127.0.0.1:50051".to_string()),
            auth: BuilderAuthConfig::Enabled {
                token_url: None,
                client_id: None,
                client_secret: None,
            },
        }
    }
}

impl RociaDbBuilder {
    /// EN: Create a builder with default settings.
    /// FR: Cree un builder avec les valeurs par defaut.
    pub fn new() -> Self {
        Self::default()
    }

    /// EN: Set the upstream host (ex: http://127.0.0.1:50051).
    /// FR: Definit le host upstream (ex: http://127.0.0.1:50051).
    ///
    /// EN: Arguments:
    /// - `host`: Base URL for the gRPC endpoint.
    /// FR: Arguments:
    /// - `host`: URL de base pour l endpoint gRPC.
    ///
    /// EN: Returns:
    /// - Mutable builder reference.
    /// FR: Returns:
    /// - Reference mutable du builder.
    pub fn host(&mut self, host: impl Into<String>) -> &mut Self {
        self.host = Some(host.into());
        self
    }

    /// EN: Configure OAuth2 client credentials for upstream auth.
    /// FR: Configure les client credentials OAuth2 pour l auth upstream.
    ///
    /// EN: Arguments:
    /// - `token_url`: OAuth2 token endpoint.
    /// - `client_id`: OAuth2 client id.
    /// - `client_secret`: OAuth2 client secret.
    /// FR: Arguments:
    /// - `token_url`: Endpoint token OAuth2.
    /// - `client_id`: Client id OAuth2.
    /// - `client_secret`: Client secret OAuth2.
    ///
    /// EN: Returns:
    /// - Mutable builder reference.
    /// FR: Returns:
    /// - Reference mutable du builder.
    pub fn auth_client_credentials(
        &mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> &mut Self {
        self.auth = BuilderAuthConfig::Enabled {
            token_url: Some(token_url.into()),
            client_id: Some(client_id.into()),
            client_secret: Some(client_secret.into()),
        };
        self
    }

    /// EN: Disable auth headers on outgoing requests.
    /// FR: Desactive les headers d auth sur les requetes sortantes.
    ///
    /// EN: Returns:
    /// - Mutable builder reference.
    /// FR: Returns:
    /// - Reference mutable du builder.
    pub fn disable_auth(&mut self) -> &mut Self {
        self.auth = BuilderAuthConfig::Disabled;
        self
    }

    /// EN: Build a client connected to the upstream.
    ///
    /// EN: When auth is enabled, this fetches the first token and starts a
    /// background task that refreshes it before it expires (the IdP's
    /// tokens are short-lived — 600 seconds today) for as long as the
    /// returned `RociaDbClient` or any of its clones is kept alive. Call
    /// [`RociaDbClient::refresh_auth_token`] after an `UNAUTHENTICATED`
    /// error to force an out-of-band refresh.
    /// FR: Construit un client connecte a l upstream.
    ///
    /// FR: Quand l auth est activee, ceci recupere le premier token et
    /// demarre une tache en arriere-plan qui le rafraichit avant son
    /// expiration (les tokens de l IdP sont a courte duree de vie — 600
    /// secondes aujourd hui) tant que le `RociaDbClient` retourne ou l un
    /// de ses clones reste vivant. Appelez
    /// [`RociaDbClient::refresh_auth_token`] apres une erreur
    /// `UNAUTHENTICATED` pour forcer un refresh hors-bande.
    ///
    /// EN: Returns:
    /// - Connected `RociaDbClient`.
    /// FR: Returns:
    /// - `RociaDbClient` connecte.
    pub async fn build(&self) -> Result<RociaDbClient> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| RociaDbError::connection("missing upstream host"))?;
        info!(host = %host, "building rocia db client");
        let endpoint = Endpoint::from_shared(host.clone())
            .connection_context("invalid upstream host")?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .connection_context("failed to configure TLS")?;
        let channel = endpoint
            .connect()
            .await
            .connection_context("failed to connect to upstream")?;
        let (interceptor, token_manager, token_refresh_guard) = match &self.auth {
            BuilderAuthConfig::Disabled => {
                warn!(host = %host, "building rocia db client with auth disabled");
                (BearerInterceptor::disabled(), None, None)
            }
            BuilderAuthConfig::Enabled {
                token_url,
                client_id,
                client_secret,
            } => {
                let token_url = token_url
                    .clone()
                    .or_else(|| env::var(AUTH_TOKEN_URL_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::connection("missing auth token url (set AUTH_TOKEN_URL)")
                    })?;
                let client_id = client_id
                    .clone()
                    .or_else(|| env::var(AUTH_CLIENT_ID_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::connection("missing auth client id (set AUTH_CLIENT_ID)")
                    })?;
                let client_secret = client_secret
                    .clone()
                    .or_else(|| env::var(AUTH_CLIENT_SECRET_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::connection(
                            "missing auth client secret (set AUTH_CLIENT_SECRET)",
                        )
                    })?;

                debug!(
                    host = %host,
                    token_url = %token_url,
                    client_id = %client_id,
                    "initializing upstream token manager"
                );
                let token_manager =
                    TokenManager::new(reqwest::Client::new(), token_url, client_id, client_secret)
                        .await
                        .auth_context("failed to initialize token manager")?;
                let interceptor = token_manager.interceptor();
                // EN: The IdP token used to die silently after its
                // `expires_in` (600s here) because nothing ever refreshed
                // it. Start the background refresh now and keep the guard
                // alive inside the client for as long as it (or any clone
                // of it) exists.
                // FR: Le token de l IdP mourait silencieusement apres son
                // `expires_in` (600s ici) car rien ne le rafraichissait
                // jamais. On demarre le refresh en arriere-plan des
                // maintenant et on garde le guard vivant dans le client
                // tant qu il (ou l un de ses clones) existe.
                let refresh_interval = token_manager.refresh_interval();
                info!(
                    host = %host,
                    refresh_interval_secs = refresh_interval.as_secs(),
                    "starting background token refresh"
                );
                let guard = token_manager.spawn_refresh(refresh_interval);
                (interceptor, Some(token_manager), Some(Arc::new(guard)))
            }
        };
        let upstream_document =
            DocumentServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_graph =
            GraphServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_file =
            FileServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_tenant = TenantServiceClient::with_interceptor(channel, interceptor);
        info!(host = %host, "rocia db client ready");
        Ok(RociaDbClient {
            upstream_document,
            upstream_graph,
            upstream_file,
            upstream_tenant,
            token_manager,
            _token_refresh_guard: token_refresh_guard,
        })
    }
}

impl RociaDbClient {
    /// EN: Force an immediate refresh of the upstream auth token.
    ///
    /// Call this after an RPC fails with `UNAUTHENTICATED` — the server
    /// treats that status as the signal to renew the token, as opposed to
    /// `PERMISSION_DENIED`, which means the token is valid but lacks the
    /// required scope and retrying after a refresh will not help. A no-op
    /// returning `Ok(())` when the client was built with
    /// [`RociaDbBuilder::disable_auth`].
    /// FR: Force un renouvellement immediat du token d auth upstream.
    ///
    /// A appeler apres qu un appel RPC a echoue avec `UNAUTHENTICATED` —
    /// le serveur traite ce statut comme le signal de renouvellement du
    /// token, contrairement a `PERMISSION_DENIED`, qui signifie que le
    /// token est valide mais manque du scope requis et que reessayer
    /// apres un refresh n aidera pas. Ne fait rien et retourne `Ok(())`
    /// quand le client a ete construit avec
    /// [`RociaDbBuilder::disable_auth`].
    pub async fn refresh_auth_token(&self) -> Result<()> {
        match &self.token_manager {
            Some(manager) => manager.refresh_now().await,
            None => Ok(()),
        }
    }
}

impl RociaDbClient {
    /// EN: Create or update a document, and optionally a graph node reference.
    /// FR: Cree ou met a jour un document, et optionnellement un node graph.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `collection_name`: Document collection name.
    /// - `document_id`: Raw document id without graph label prefix.
    /// - `value`: JSON payload to store.
    /// - `node_label`: Optional graph node label.
    /// - `node_graph`: Optional graph name.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `collection_name`: Nom de la collection.
    /// - `document_id`: Id brut du document, sans prefix de label graph.
    /// - `value`: Contenu JSON a stocker.
    /// - `node_label`: Label du node graph (optionnel).
    /// - `node_graph`: Nom du graph (optionnel).
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
    ///
    /// EN: `node_label` and `node_graph` must be provided together: if only
    /// one of them is set, this returns an error before any network call
    /// (the two used to be silently ignored, which left the caller thinking
    /// a graph node had been created when it had not).
    ///
    /// This call is **not atomic**: the document is written first, and the
    /// graph node binding (when requested) is written second. If the node
    /// write fails, the document is left in place without its node
    /// binding — callers that need both or neither must handle that
    /// themselves (for example by retrying the node write, or by treating
    /// a document without its expected node as needing repair).
    /// FR: `node_label` et `node_graph` doivent etre fournis ensemble : si
    /// un seul des deux est renseigne, retourne une erreur avant tout appel
    /// reseau (les deux etaient auparavant ignores silencieusement, ce qui
    /// laissait l appelant croire qu un node graph avait ete cree alors
    /// que non).
    ///
    /// Cet appel n est **pas atomique** : le document est ecrit en premier,
    /// et le binding de node graph (si demande) est ecrit en second. Si
    /// l ecriture du node echoue, le document reste en place sans son
    /// binding de node — les appelants qui ont besoin des deux ou d aucun
    /// des deux doivent gerer cela eux-memes (par exemple en reessayant
    /// l ecriture du node, ou en traitant un document sans son node attendu
    /// comme necessitant une reparation).
    pub async fn create_document(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: Value,
        node_label: Option<String>,
        node_graph: Option<String>,
    ) -> Result<()> {
        validate_node_binding(&node_label, &node_graph)?;
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            has_node_binding = node_label.is_some() && node_graph.is_some(),
            "upserting document"
        );
        let json = serde_json::to_vec(&value)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to encode document json"
                );
            })
            .encode_context("document json")?;
        let doc = PutDocRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection_name.to_string(),
            id: document_id.to_string(),
            json,
            request_id: format!("upsert_document:{}:{}", collection_name, Uuid::new_v4()),
        };
        let mut upstream_document = self.upstream_document.clone();
        upstream_document
            .put_doc(doc)
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to upsert document"
                );
            })
            .status_context("failed to upsert document")?;
        info!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "document upserted"
        );
        if let (Some(label), Some(graph)) = (node_label, node_graph) {
            debug!(
                tenant_id = tenant_id,
                collection = collection_name,
                document_id = document_id,
                graph = %graph,
                label = %label,
                "upserting graph node binding for document"
            );
            let json = serde_json::to_vec(&json!({
                "collection": collection_name,
                "id": document_id,
            }))
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    graph = %graph,
                    label = %label,
                    error = %error,
                    "failed to encode node json"
                );
            })
            .encode_context("node json")?;
            let req = PutNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.clone(),
                node_id: format!("{}:{}", label, document_id),
                json,
                request_id: format!("upsert_node:{}", Uuid::new_v4()),
            };
            let mut upstream_graph = self.upstream_graph.clone();
            upstream_graph
                .put_node(req)
                .await
                .inspect_err(|error| {
                    error!(
                        tenant_id = tenant_id,
                        collection = collection_name,
                        document_id = document_id,
                        graph = %graph,
                        label = %label,
                        error = %error,
                        "failed to upsert graph node binding"
                    );
                })
                .status_context("failed to upsert graph node binding")?;
            info!(
                tenant_id = tenant_id,
                collection = collection_name,
                document_id = document_id,
                graph = %graph,
                label = %label,
                "graph node binding upserted"
            );
        }
        Ok(())
    }

    pub async fn search_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        search_field: &str,
        value: &impl Serialize,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>, u64)>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            search_field = search_field,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "searching documents by field"
        );
        let page = page_request(limit, cursor)?;

        let value_json = serde_json::to_vec(value)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to encode search value"
                );
            })
            .encode_context("search value")?;

        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .find_by_field(FindByFieldRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                field: search_field.to_string(),
                value_json,
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to search documents"
                );
            })
            .status_context("failed to search documents")?
            .into_inner();

        let resp = result
            .json
            .into_iter()
            .map(|data| serde_json::from_slice::<T>(&data))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to decode search results"
                );
            })
            .decode_context("search results")?;

        let next_cursor = result
            .page
            .and_then(|page| (!page.next_cursor.is_empty()).then_some(page.next_cursor));
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            search_field = search_field,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document search completed"
        );

        Ok((resp, next_cursor, result.total_count))
    }

    pub async fn list_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>, u64)>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing documents"
        );
        let page = page_request(limit, cursor)?;
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .list_doc(ListDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to list documents"
                );
            })
            .status_context("failed to list documents")?
            .into_inner();

        let resp = result
            .json
            .into_iter()
            .map(|data| serde_json::from_slice::<T>(&data))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to decode listed documents"
                );
            })
            .decode_context("listed documents")?;

        let next_cursor = result
            .page
            .and_then(|page| (!page.next_cursor.is_empty()).then_some(page.next_cursor));
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document listing completed"
        );

        Ok((resp, next_cursor, result.total_count))
    }

    /// EN: List the document collections holding at least one document.
    /// FR: Liste les collections de documents contenant au moins un document.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `limit`: Page size, defaults to 20.
    /// - `cursor`: Opaque cursor returned by the previous page.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `limit`: Taille de page, 20 par defaut.
    /// - `cursor`: Curseur opaque retourne par la page precedente.
    ///
    /// EN: Returns:
    /// - One page of `CollectionInfo`, each carrying its document count.
    /// FR: Returns:
    /// - Une page de `CollectionInfo`, chacun portant son nombre de documents.
    pub async fn list_collections(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<CollectionInfo>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing collections"
        );
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .list_collections(ListCollectionsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    error = %error,
                    "failed to list collections"
                );
            })
            .status_context("failed to list collections")?
            .into_inner();

        Ok(Page {
            items: result.collections,
            next_cursor: result.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// EN: Execute a paginated multi-filter document query.
    /// FR: Execute une requete document paginee avec plusieurs filtres.
    ///
    /// EN: The underlying server applies filters with logical AND and
    /// uses the provided sort list in order. The returned `next_cursor`
    /// is an opaque server cursor that should be fed back unchanged.
    /// FR: Le serveur applique les filtres avec un ET logique et utilise
    /// la liste de tri dans l ordre fourni. Le `next_cursor` retourne est
    /// opaque et doit etre reutilise tel quel.
    pub async fn query_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        filters: &[DocumentQueryFilter],
        sort: &[DocumentQuerySort],
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>, u64)>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            filter_count = filters.len(),
            sort_count = sort.len(),
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "querying documents"
        );

        let page = page_request(limit, cursor)?;

        let proto_filters = filters
            .iter()
            .map(|filter| -> Result<QueryFilter> {
                Ok(QueryFilter {
                    field: filter.field.clone(),
                    operator: filter.operator.as_proto(),
                    values_json: filter
                        .values
                        .iter()
                        .map(serde_json::to_vec)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .inspect_err(|error| {
                            error!(
                                tenant_id = tenant_id,
                                collection = collection_name,
                                field = %filter.field,
                                error = %error,
                                "failed to encode query filter value"
                            );
                        })
                        .encode_context("query filter value")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let proto_sort = sort
            .iter()
            .map(|sort| QuerySort {
                field: sort.field.clone(),
                direction: sort.direction.as_proto(),
            })
            .collect::<Vec<_>>();

        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .query_doc(QueryDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                filters: proto_filters,
                sort: proto_sort,
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to query documents"
                );
            })
            .status_context("failed to query documents")?
            .into_inner();

        let resp = result
            .json
            .into_iter()
            .map(|data| serde_json::from_slice::<T>(&data))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to decode queried documents"
                );
            })
            .decode_context("queried documents")?;

        let next_cursor = result
            .page
            .and_then(|page| (!page.next_cursor.is_empty()).then_some(page.next_cursor));
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document query completed"
        );

        Ok((resp, next_cursor, result.total_count))
    }

    pub async fn get_document<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "loading document"
        );
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .get_doc(GetDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                id: document_id.to_string(),
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to load document"
                );
            })
            .status_context("failed to load document")?
            .into_inner();

        let resp = serde_json::from_slice::<T>(&result.json)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to decode document"
                );
            })
            .decode_context("document")?;
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "document loaded"
        );

        Ok(resp)
    }

    /// EN: Upsert a batch of nodes in a graph with bounded concurrency.
    /// FR: Upsert un batch de nodes dans un graph avec concurrence bornee.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `data`: Map of `(label, id)` to JSON payload.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `data`: Map `(label, id)` vers payload JSON.
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
    pub async fn put_nodes(
        &self,
        tenant_id: &str,
        graph_name: &str,
        data: HashMap<(String, String), Value>,
    ) -> Result<()> {
        let tenant_id = tenant_id.to_string();
        let graph_name = graph_name.to_string();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_count = data.len(),
            "upserting graph nodes batch"
        );
        stream::iter(data)
            .map(|((label, id), value)| {
                let json = serde_json::to_vec(&value).encode_context("node json")?;
                Ok::<PutNodeRequest, RociaDbError>(PutNodeRequest {
                    tenant_id: tenant_id.clone(),
                    graph: graph_name.clone(),
                    node_id: format!("{}:{}", label, id),
                    json,
                    request_id: format!("upsert_node:{}", Uuid::new_v4()),
                })
            })
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |node| {
                let mut upstream = self.upstream_graph.clone();
                async move {
                    let graph = node.graph.clone();
                    let node_id = node.node_id.clone();
                    upstream
                        .put_node(node)
                        .await
                        .status_context("failed to upsert node")
                        .map_err(|error| {
                            error!(
                                graph = graph,
                                node_id = node_id,
                                error = %error,
                                "failed to upsert graph node"
                            );
                            error
                        })?;
                    Ok(())
                }
            })
            .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            "graph nodes batch upserted"
        );
        Ok(())
    }

    /// EN: Fetch a node and decode its JSON payload.
    /// FR: Recupere un node et decode son JSON.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `node_id`: Node id (format "label:id").
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `node_id`: Id du node (format "label:id").
    ///
    /// EN: Returns:
    /// - Decoded JSON payload.
    /// FR: Returns:
    /// - Payload JSON decode.
    pub async fn get_node(
        &self,
        tenant_id: &str,
        graph_name: &str,
        node_id: &str,
    ) -> Result<Value> {
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_id = node_id,
            "loading graph node"
        );
        let mut upstream_graph = self.upstream_graph.clone();
        let resp = upstream_graph
            .get_node(GetNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                node_id: node_id.to_string(),
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    node_id = node_id,
                    error = %error,
                    "failed to load graph node"
                );
            })
            .status_context("failed to load graph node")?
            .into_inner();
        let value = serde_json::from_slice(&resp.json)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    node_id = node_id,
                    error = %error,
                    "failed to decode node json"
                );
            })
            .decode_context("node json")?;
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_id = node_id,
            "graph node loaded"
        );
        Ok(value)
    }

    /// EN: Upsert a batch of edges with bounded concurrency.
    /// FR: Upsert un batch d edges avec concurrence bornee.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `data`: Map of `(from, to, label, edge_id)` to JSON payload.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `data`: Map `(from, to, label, edge_id_brut)` vers payload JSON.
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
    ///
    /// EN: The server returns `NOT_FOUND` for any edge whose `from` or `to`
    /// node does not already exist in `graph_name`: create both endpoint
    /// nodes before adding an edge between them.
    /// FR: Le serveur renvoie `NOT_FOUND` pour toute edge dont le node
    /// `from` ou `to` n existe pas deja dans `graph_name` : creez les deux
    /// nodes aux extremites avant d ajouter une edge entre eux.
    pub async fn add_edges(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edges: HashMap<(String, String, String, String), Value>,
    ) -> Result<()> {
        let tenant_id = tenant_id.to_string();
        let graph_name = graph_name.to_string();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_count = edges.len(),
            "upserting graph edges batch"
        );
        stream::iter(edges)
            .map(|((from, to, label, id), data)| {
                let json = serde_json::to_vec(&data).encode_context("edge json")?;
                let edge_id = id;
                debug!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    edge_id = edge_id,
                    from = from,
                    to = to,
                    label = label,
                    "prepared graph edge upsert"
                );

                Ok::<AddEdgeRequest, RociaDbError>(AddEdgeRequest {
                    tenant_id: tenant_id.clone(),
                    graph: graph_name.clone(),
                    edge_id,
                    from,
                    to,
                    label,
                    json,
                    request_id: Uuid::new_v4().to_string(),
                })
            })
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |edge| {
                let mut upstream = self.upstream_graph.clone();
                async move {
                    let graph = edge.graph.clone();
                    let edge_id = edge.edge_id.clone();
                    let from = edge.from.clone();
                    let to = edge.to.clone();
                    let label = edge.label.clone();
                    upstream
                        .add_edge(edge)
                        .await
                        .status_context("failed to add edge")
                        .map_err(|error| {
                            error!(
                                graph = graph,
                                edge_id = edge_id,
                                from = from,
                                to = to,
                                label = label,
                                error = %error,
                                "failed to upsert graph edge"
                            );
                            error
                        })?;
                    Ok(())
                }
            })
            .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            "graph edges batch upserted"
        );

        Ok(())
    }

    /// EN: Delete an edge by id.
    /// FR: Supprime un edge par id.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `edge_id`: Edge id to delete.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `edge_id`: Id de l edge a supprimer.
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
    pub async fn delete_edge(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edge_id: &str,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "deleting graph edge"
        );
        self.delete_edge_with_request_id(
            tenant_id,
            graph_name,
            edge_id,
            format!("delete_edge:{}", Uuid::new_v4()),
        )
        .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "graph edge deleted"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{RociaDbClient, validate_node_binding};
    use crate::RociaDbError;

    #[test]
    fn node_binding_accepts_both_absent() {
        validate_node_binding(&None, &None).expect("both absent must be accepted");
    }

    #[test]
    fn node_binding_accepts_both_present() {
        validate_node_binding(&Some("product".to_string()), &Some("products".to_string()))
            .expect("both present must be accepted");
    }

    #[test]
    fn node_binding_rejects_label_without_graph() {
        let error = validate_node_binding(&Some("product".to_string()), &None)
            .expect_err("node_label without node_graph must be rejected");
        // EN: This is a client-side validation rule, so it must come back
        // as `RociaDbError::Validation`, not folded into some catch-all
        // variant, and the message must stay informative.
        // FR: C est une regle de validation cote client, elle doit donc
        // revenir en `RociaDbError::Validation`, pas noyee dans une
        // variante fourre-tout, et le message doit rester informatif.
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("must be provided together"));
        assert!(error.to_string().contains("node_label=Some(\"product\")"));
    }

    #[test]
    fn node_binding_rejects_graph_without_label() {
        let error = validate_node_binding(&None, &Some("products".to_string()))
            .expect_err("node_graph without node_label must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("must be provided together"));
        assert!(error.to_string().contains("node_graph=Some(\"products\")"));
    }

    #[test]
    fn client_is_send_sync_so_an_arc_needs_no_mutex() {
        // EN: `RociaDbClient` methods take `&self`, not `&mut self` (each
        // call clones the cheap, Arc-backed inner service client before
        // issuing its RPC). This is only sound to share across tasks if
        // the type is both `Send` and `Sync`: a plain compile-time trait
        // assertion, not a runtime check, but it locks in the intent so a
        // future field that breaks it fails the build instead of shipping
        // silently.
        // FR: Les methodes de `RociaDbClient` prennent `&self`, pas `&mut
        // self` (chaque appel clone le client de service interne, peu
        // couteux car adosse a un Arc, avant d emettre son RPC). Ce n est
        // valide a partager entre taches que si le type est a la fois
        // `Send` et `Sync` : une simple assertion de trait a la
        // compilation, pas une verification a l execution, mais elle fige
        // l intention pour qu un futur champ qui la casserait fasse
        // echouer le build plutot que de partir en silence.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RociaDbClient>();
        assert_send_sync::<std::sync::Arc<RociaDbClient>>();
    }
}
