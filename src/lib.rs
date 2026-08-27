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
pub use file::{FileStreamUploadOptions, FileUploadOptions};
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
use futures::{TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::env;
use std::sync::Arc;
use std::time::Duration;
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
/// EN: Connect timeout applied in [`RociaDbBuilder::build`] when
/// [`RociaDbBuilder::connect_timeout`] was never called. Identical to the
/// TypeScript SDK's default `connectTimeoutMs` (10_000 ms) so a host that
/// never answers cannot hang either SDK's `build()`/`connect()` forever.
/// FR: Delai de connexion applique dans [`RociaDbBuilder::build`] quand
/// [`RociaDbBuilder::connect_timeout`] n a jamais ete appelee. Identique au
/// `connectTimeoutMs` par defaut du SDK TypeScript (10_000 ms), pour qu un
/// host qui ne repond jamais ne puisse bloquer indefiniment ni l un ni
/// l autre `build()`/`connect()`.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum BuilderAuthConfig {
    Enabled {
        token_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    },
    Disabled,
}

// EN: Manual `Debug` impl instead of `#[derive(Debug)]`: a derived impl
// would print `client_secret` in clear text, so any `format!("{:?}", ..)`
// or debug-level log of a `RociaDbBuilder` would leak the OAuth2 secret.
// FR: Impl `Debug` manuelle plutot que `#[derive(Debug)]` : une impl
// derivee afficherait `client_secret` en clair, donc tout
// `format!("{:?}", ..)` ou log de niveau debug d un `RociaDbBuilder`
// fuiterait le secret OAuth2.
impl std::fmt::Debug for BuilderAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled {
                token_url,
                client_id,
                client_secret: _,
            } => f
                .debug_struct("Enabled")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[redacted]")
                .finish(),
            Self::Disabled => f.write_str("Disabled"),
        }
    }
}

/// EN: Builder for RociaDbClient.
/// FR: Builder pour RociaDbClient.
#[derive(Debug)]
pub struct RociaDbBuilder {
    host: Option<String>,
    auth: BuilderAuthConfig,
    connect_timeout: Option<Duration>,
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

/// EN: One page of document results, together with the total number of
/// documents matching the request (before pagination).
///
/// EN: This replaces the anonymous `(Vec<T>, Option<String>, u64)` tuple
/// [`RociaDbClient::search_documents`], [`RociaDbClient::list_documents`],
/// and [`RociaDbClient::query_documents`] used to return: the same three
/// values, now named `items`, `next_cursor`, and `total_count` — consistent
/// with [`Page<T>`], which already has `items` and `next_cursor`.
///
/// The cost of `total_count` is **not** the same across the three methods
/// that produce it, because the server computes it differently for each:
/// - [`RociaDbClient::list_documents`] (`ListDoc`): free — the server keeps
///   a running per-collection counter updated on every write, so reading it
///   costs nothing beyond the listing itself.
/// - [`RociaDbClient::search_documents`] (`FindByField`): a count over the
///   matching field-index entries.
/// - [`RociaDbClient::query_documents`] (`QueryDoc`): expensive — the server
///   only knows the total once it has filtered the *complete* candidate set
///   for the query, so the cost scales with the number of candidates on
///   every single call. Do not call this in a loop expecting a cheap
///   number; fetch it once and cache it if the same query is issued
///   repeatedly.
/// FR: Une page de resultats document, avec le nombre total de documents
/// correspondant a la requete (avant pagination).
///
/// FR: Remplace le tuple anonyme `(Vec<T>, Option<String>, u64)` que
/// retournaient [`RociaDbClient::search_documents`],
/// [`RociaDbClient::list_documents`], et [`RociaDbClient::query_documents`] :
/// les trois memes valeurs, desormais nommees `items`, `next_cursor`, et
/// `total_count` — coherent avec [`Page<T>`], qui a deja `items` et
/// `next_cursor`.
///
/// Le cout de `total_count` n est **pas** le meme selon la methode qui le
/// produit, car le serveur le calcule differemment pour chacune :
/// - [`RociaDbClient::list_documents`] (`ListDoc`) : gratuit — le serveur
///   maintient un compteur par collection mis a jour a chaque ecriture, le
///   lire ne coute rien de plus que le listing lui-meme.
/// - [`RociaDbClient::search_documents`] (`FindByField`) : un comptage sur
///   les entrees d index de champ correspondantes.
/// - [`RociaDbClient::query_documents`] (`QueryDoc`) : couteux — le serveur
///   ne connait le total qu apres avoir filtre l integralite du jeu de
///   candidats de la requete, donc le cout croit avec le nombre de
///   candidats a chaque appel. Ne l appelez pas en boucle en pensant obtenir
///   un nombre gratuit ; recuperez-le une fois et mettez-le en cache si la
///   meme requete est reemise plusieurs fois.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub total_count: u64,
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

/// EN: One node to upsert, used by [`RociaDbClient::put_nodes`].
///
/// `node_id` is the **complete** node id (for example `"product:sku-1"`),
/// not a `(label, id)` pair for the SDK to reassemble: `label:id` remains a
/// usage convention, not something the server enforces or the SDK
/// recomposes. This is a breaking change from the 0.4.0 batch API, which
/// took a `HashMap<(String, String), Value>` keyed by `(label, id)` and
/// built `node_id` internally as `format!("{label}:{id}")` — a caller that
/// previously passed `("product", "sku-1")` now writes the node id itself:
/// `NodeInput { node_id: "product:sku-1".to_string(), .. }`.
/// FR: Un node a upserter, utilise par [`RociaDbClient::put_nodes`].
///
/// `node_id` est l id **complet** du node (par exemple `"product:sku-1"`),
/// pas un couple `(label, id)` que le SDK recomposerait : `label:id` reste
/// une convention d usage, pas quelque chose que le serveur impose ou que le
/// SDK recompose. C est un changement cassant par rapport a l API batch de
/// 0.4.0, qui prenait un `HashMap<(String, String), Value>` indexe par
/// `(label, id)` et construisait `node_id` en interne via
/// `format!("{label}:{id}")` — un appelant qui passait auparavant
/// `("product", "sku-1")` ecrit desormais l id de node lui-meme :
/// `NodeInput { node_id: "product:sku-1".to_string(), .. }`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInput {
    pub node_id: String,
    pub value: Value,
    /// EN: Idempotency key for this item's `PutNode` call. When `None`, one
    /// is generated automatically (`put_node:<uuid>` — the same prefix
    /// [`RociaDbClient::put_node`] uses for a single-item write, so a
    /// `PutNode` call always carries the same default prefix regardless of
    /// which path produced it). Provide it explicitly — and reuse the
    /// same value on a retry — so a batch replayed after a timeout resumes
    /// safely: the server deduplicates on `(tenant, operation,
    /// request_id)`, so a repeated `request_id` is recognized as the same
    /// write rather than a new one.
    /// FR: Cle d idempotence pour l appel `PutNode` de cet item. Quand elle
    /// vaut `None`, une cle est generee automatiquement (`put_node:<uuid>`
    /// — le meme prefixe qu utilise [`RociaDbClient::put_node`] pour une
    /// ecriture unitaire, pour qu un appel `PutNode` porte toujours le meme
    /// prefixe par defaut quel que soit le chemin qui l a produit).
    /// Fournissez-la explicitement — et reutilisez la meme valeur lors d un
    /// rejeu — pour qu un batch rejoue apres un timeout reprenne en toute
    /// securite : le serveur deduplique sur `(tenant, operation,
    /// request_id)`, donc un `request_id` repete est reconnu comme la meme
    /// ecriture plutot qu une nouvelle.
    pub request_id: Option<String>,
}

/// EN: One edge to upsert, used by [`RociaDbClient::add_edges`].
///
/// `edge_id` is raw and must not be prefixed with `label`.
/// FR: Un edge a upserter, utilise par [`RociaDbClient::add_edges`].
///
/// `edge_id` est brut et ne doit pas etre prefixe par `label`.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeInput {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub label: String,
    pub value: Value,
    /// EN: Idempotency key for this item's `AddEdge` call. When `None`, one
    /// is generated automatically (a bare UUID, same as before this type
    /// existed, with no prefix). See [`NodeInput::request_id`] for why
    /// reusing it on a retry matters.
    /// FR: Cle d idempotence pour l appel `AddEdge` de cet item. Quand elle
    /// vaut `None`, une cle est generee automatiquement (un UUID brut, comme
    /// avant l existence de ce type, sans prefixe). Voir
    /// [`NodeInput::request_id`] pour l importance de la reutiliser lors d
    /// un rejeu.
    pub request_id: Option<String>,
}

/// EN: Build the ordered `PutNodeRequest` batch for
/// [`RociaDbClient::put_nodes`]. Pulled out as a pure, network-free
/// function — the same way [`crate::file::chunk_upload_requests`] is for
/// uploads — so the batch's wire shape is unit-testable without a live
/// client: item order is preserved (`nodes` is consumed via `into_iter` in
/// the order given), duplicate `node_id`s are not merged (each `NodeInput`
/// becomes exactly one `PutNodeRequest`), and `request_id` is passed
/// through unchanged or defaulted to `put_node:<uuid>` when absent — the
/// same default prefix [`RociaDbClient::put_node`] uses for a single-item
/// write, so every `PutNode` call defaults consistently regardless of
/// whether it went through the batch or single-item path.
/// FR: Construit le batch `PutNodeRequest` ordonne pour
/// [`RociaDbClient::put_nodes`]. Extraite en fonction pure, sans reseau —
/// comme [`crate::file::chunk_upload_requests`] pour les uploads — pour que
/// la forme sur le fil du batch soit testable unitairement sans client
/// reel : l ordre des items est preserve (`nodes` est consomme via
/// `into_iter` dans l ordre fourni), les `node_id` dupliques ne sont pas
/// fusionnes (chaque `NodeInput` devient exactement un `PutNodeRequest`),
/// et `request_id` est transmis tel quel ou vaut par defaut
/// `put_node:<uuid>` quand il est absent — le meme prefixe par defaut
/// qu utilise [`RociaDbClient::put_node`] pour une ecriture unitaire, pour
/// que tout appel `PutNode` ait un defaut coherent, qu il passe par le
/// batch ou par le chemin unitaire.
fn build_put_node_requests(
    tenant_id: &str,
    graph_name: &str,
    nodes: Vec<NodeInput>,
) -> Result<Vec<PutNodeRequest>> {
    nodes
        .into_iter()
        .map(|node| {
            let json = serde_json::to_vec(&node.value).encode_context("node json")?;
            Ok(PutNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                node_id: node.node_id,
                json,
                request_id: node
                    .request_id
                    .unwrap_or_else(|| format!("put_node:{}", Uuid::new_v4())),
            })
        })
        .collect()
}

/// EN: Build the ordered `AddEdgeRequest` batch for
/// [`RociaDbClient::add_edges`]. Same rationale and guarantees as
/// [`build_put_node_requests`]: order preserved, duplicate `edge_id`s not
/// merged, `request_id` passed through unchanged or defaulted to a bare
/// UUID (no prefix) when absent.
/// FR: Construit le batch `AddEdgeRequest` ordonne pour
/// [`RociaDbClient::add_edges`]. Meme logique et memes garanties que
/// [`build_put_node_requests`] : ordre preserve, `edge_id` dupliques non
/// fusionnes, `request_id` transmis tel quel ou par defaut un UUID brut
/// (sans prefixe) quand il est absent.
fn build_add_edge_requests(
    tenant_id: &str,
    graph_name: &str,
    edges: Vec<EdgeInput>,
) -> Result<Vec<AddEdgeRequest>> {
    edges
        .into_iter()
        .map(|edge| {
            let json = serde_json::to_vec(&edge.value).encode_context("edge json")?;
            debug!(
                tenant_id = tenant_id,
                graph = graph_name,
                edge_id = edge.edge_id,
                from = edge.from,
                to = edge.to,
                label = edge.label,
                "prepared graph edge upsert"
            );
            Ok(AddEdgeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                edge_id: edge.edge_id,
                from: edge.from,
                to: edge.to,
                label: edge.label,
                json,
                request_id: edge
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
            })
        })
        .collect()
}

/// EN: Default idempotency key for the `PutDoc` write issued by
/// [`RociaDbClient::create_document`] when the caller does not use
/// [`RociaDbClient::create_document_with_request_id`] directly. Pulled out
/// as a pure, network-free function — the same reason
/// [`build_put_node_requests`] and [`build_add_edge_requests`] exist — so
/// the exact default prefix (`put_document:{collection}:<uuid>`, matching
/// [`RociaDbClient::put_document`]'s own default; see the request_id-prefix
/// consistency fix on [`build_put_node_requests`]) is unit-testable without
/// a live client or a network call.
/// FR: Cle d idempotence par defaut pour l ecriture `PutDoc` emise par
/// [`RociaDbClient::create_document`] quand l appelant n utilise pas
/// directement [`RociaDbClient::create_document_with_request_id`]. Extraite
/// en fonction pure, sans reseau — pour la meme raison que
/// [`build_put_node_requests`] et [`build_add_edge_requests`] existent —
/// pour que le prefixe par defaut exact (`put_document:{collection}:<uuid>`,
/// coherent avec le defaut de [`RociaDbClient::put_document`] ; voir le
/// correctif de coherence des prefixes de request_id sur
/// [`build_put_node_requests`]) soit testable unitairement sans client reel
/// ni appel reseau.
fn default_document_request_id(collection_name: &str) -> String {
    format!("put_document:{}:{}", collection_name, Uuid::new_v4())
}

/// EN: Reject a `host` URL whose path is neither empty nor `"/"`, before any
/// connection attempt. Mirrors the TypeScript SDK's `endpointFromHost`,
/// which rejects on `url.pathname !== "/"`: a mistyped host carrying a
/// leftover path (for example `http://127.0.0.1:50051/v1` pasted from
/// somewhere else) would otherwise be silently accepted by tonic, which
/// simply ignores the path component when dialing.
///
/// `http::Uri::path()` already returns `"/"` for a URI with no explicit
/// path component (verified against `http` 1.x), so this rejects strictly
/// more than "path is exactly absent" — matching what `URL::pathname`
/// reports on the TypeScript side.
/// FR: Rejette un `host` dont le chemin d URL n est ni vide ni `"/"`, avant
/// toute tentative de connexion. Reproduit `endpointFromHost` du SDK
/// TypeScript, qui rejette sur `url.pathname !== "/"` : un host mal saisi
/// portant un chemin residuel (par exemple `http://127.0.0.1:50051/v1`
/// colle depuis ailleurs) serait sinon silencieusement accepte par tonic,
/// qui ignore simplement la composante chemin lors de la connexion.
///
/// `http::Uri::path()` renvoie deja `"/"` pour une URI sans composante
/// chemin explicite (verifie avec `http` 1.x), donc ceci rejette strictement
/// plus que "le chemin est totalement absent" — coherent avec ce que
/// `URL::pathname` rapporte cote TypeScript.
fn validate_host_path(host: &str) -> Result<()> {
    let uri: http::Uri = host.parse().connection_context("invalid upstream host")?;
    let path = uri.path();
    if !path.is_empty() && path != "/" {
        return Err(RociaDbError::connection(format!(
            "RociaDB host must contain only a hostname and port, got path {path:?}"
        )));
    }
    Ok(())
}

/// EN: Resolve the connect timeout [`RociaDbBuilder::build`] applies:
/// `explicit` when [`RociaDbBuilder::connect_timeout`] was called, or
/// [`DEFAULT_CONNECT_TIMEOUT`] otherwise — rejecting a zero timeout either
/// way. Extracted as a pure, network-free function (mirrors
/// [`validate_host_path`]) so both the default value and the zero-timeout
/// rejection are unit-testable without ever dialing an upstream.
/// FR: Resout le delai de connexion applique par [`RociaDbBuilder::build`] :
/// `explicit` quand [`RociaDbBuilder::connect_timeout`] a ete appelee, ou
/// [`DEFAULT_CONNECT_TIMEOUT`] sinon — en rejetant un delai nul dans les
/// deux cas. Extraite en fonction pure, sans reseau (miroir de
/// [`validate_host_path`]) pour que la valeur par defaut et le rejet du
/// delai nul soient toutes deux testables unitairement sans jamais
/// composer un upstream.
fn resolve_connect_timeout(explicit: Option<Duration>) -> Result<Duration> {
    let connect_timeout = explicit.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
    if connect_timeout.is_zero() {
        return Err(RociaDbError::validation(
            "connect timeout must be greater than zero",
        ));
    }
    Ok(connect_timeout)
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
            connect_timeout: None,
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

    /// EN: Set the deadline used while connecting to the upstream host.
    ///
    /// The value is stored as-is here (no validation), the same way
    /// [`RociaDbBuilder::host`] and
    /// [`RociaDbBuilder::auth_client_credentials`] never validate before
    /// [`RociaDbBuilder::build`] — validation (rejecting a zero timeout)
    /// happens there instead. When this is never called, `build()` applies
    /// a 10-second default unconditionally: without any timeout at all,
    /// `.connect().await` could hang forever against a host with slow
    /// DNS/TCP, which is a robustness gap rather than a mere convenience.
    /// FR: Definit le delai applique pendant la connexion au host upstream.
    ///
    /// La valeur est stockee telle quelle ici (aucune validation), comme
    /// [`RociaDbBuilder::host`] et
    /// [`RociaDbBuilder::auth_client_credentials`] qui ne valident jamais
    /// avant [`RociaDbBuilder::build`] — la validation (rejet d un delai
    /// nul) s y produit a la place. Quand ceci n est jamais appele,
    /// `build()` applique un defaut de 10 secondes de facon
    /// inconditionnelle : sans aucun delai, `.connect().await` pourrait
    /// bloquer indefiniment face a un host avec un DNS/TCP lent, ce qui est
    /// une vraie lacune de robustesse plutot qu un simple confort.
    ///
    /// EN: Returns:
    /// - Mutable builder reference.
    /// FR: Returns:
    /// - Reference mutable du builder.
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connect_timeout = Some(timeout);
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
        validate_host_path(host)?;
        let connect_timeout = resolve_connect_timeout(self.connect_timeout)?;
        let endpoint = Endpoint::from_shared(host.clone())
            .connection_context("invalid upstream host")?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .connection_context("failed to configure TLS")?
            .connect_timeout(connect_timeout);
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

                // EN: `token_url`/`client_id` are deliberately not logged
                // here: they expose the auth infrastructure (IdP endpoint,
                // OAuth2 client identity) in any log pipeline configured at
                // debug level.
                // FR: `token_url`/`client_id` ne sont volontairement pas
                // journalises ici : ils exposent l infrastructure d auth
                // (endpoint de l IdP, identite du client OAuth2) dans tout
                // pipeline de logs configure au niveau debug.
                debug!(host = %host, "initializing upstream token manager");
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

    /// EN: Signal that the cached upstream auth token should no longer be
    /// trusted, without waiting for a fresh one.
    ///
    /// This is the lazy counterpart to
    /// [`RociaDbClient::refresh_auth_token`]: it is **synchronous** and
    /// returns immediately — it only wakes the background refresh task
    /// (started by [`RociaDbBuilder::build`]) so it refreshes at the next
    /// opportunity, instead of making the caller pay for the network round
    /// trip. Prefer this over [`RociaDbClient::refresh_auth_token`] when
    /// you just want to mark the token stale (for example, from a
    /// fire-and-forget error handler) rather than block until a new one is
    /// in hand before retrying. A no-op when the client was built with
    /// [`RociaDbBuilder::disable_auth`].
    /// FR: Signale que le token d auth upstream en cache ne doit plus etre
    /// considere fiable, sans attendre qu un nouveau soit disponible.
    ///
    /// C est le pendant paresseux de
    /// [`RociaDbClient::refresh_auth_token`] : **synchrone**, il retourne
    /// immediatement — il se contente de reveiller la tache de refresh en
    /// arriere-plan (demarree par [`RociaDbBuilder::build`]) pour qu elle
    /// rafraichisse a la prochaine occasion, plutot que de faire payer a
    /// l appelant le round-trip reseau. Preferez ceci a
    /// [`RociaDbClient::refresh_auth_token`] quand vous voulez juste
    /// marquer le token comme perime (par exemple depuis un gestionnaire
    /// d erreur fire-and-forget) plutot que bloquer jusqu a en avoir un
    /// nouveau en main avant de reessayer. Ne fait rien quand le client a
    /// ete construit avec [`RociaDbBuilder::disable_auth`].
    pub fn invalidate_auth_token(&self) {
        if let Some(manager) = &self.token_manager {
            manager.request_refresh();
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
        let request_id = default_document_request_id(collection_name);
        self.create_document_with_request_id(
            tenant_id,
            collection_name,
            document_id,
            &value,
            node_label,
            node_graph,
            request_id,
        )
        .await
    }

    /// EN: Same as [`RociaDbClient::create_document`], with a
    /// caller-provided idempotency key for the document write (the
    /// `PutDoc` call only — the graph node binding, when requested, keeps
    /// generating its own key, exactly as it already does in
    /// [`RociaDbClient::create_document`]). Reuse the same `request_id` on
    /// a retry so the server recognizes a repeated write instead of
    /// applying it twice.
    ///
    /// Unlike [`RociaDbClient::create_document`], `value` is generic over
    /// any `Serialize` type — consistent with
    /// [`RociaDbClient::put_document_with_request_id`],
    /// [`RociaDbClient::put_node_with_request_id`], and
    /// [`RociaDbClient::add_edge_with_request_id`] — rather than requiring
    /// the caller to pre-serialize into `serde_json::Value` first.
    /// FR: Identique a [`RociaDbClient::create_document`], avec une cle
    /// d idempotence fournie par l appelant pour l ecriture du document
    /// (l appel `PutDoc` uniquement — le binding de node graph, si demande,
    /// continue de generer sa propre cle, exactement comme le fait deja
    /// [`RociaDbClient::create_document`]). Reutilisez le meme
    /// `request_id` lors d un rejeu pour que le serveur reconnaisse une
    /// ecriture repetee plutot que de l appliquer deux fois.
    ///
    /// Contrairement a [`RociaDbClient::create_document`], `value` est
    /// generique sur tout type `Serialize` — coherent avec
    /// [`RociaDbClient::put_document_with_request_id`],
    /// [`RociaDbClient::put_node_with_request_id`], et
    /// [`RociaDbClient::add_edge_with_request_id`] — plutot que d exiger
    /// que l appelant pre-serialise d abord vers `serde_json::Value`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_document_with_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: &T,
        node_label: Option<String>,
        node_graph: Option<String>,
        request_id: impl Into<String>,
    ) -> Result<()> {
        validate_node_binding(&node_label, &node_graph)?;
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            has_node_binding = node_label.is_some() && node_graph.is_some(),
            "upserting document"
        );
        let json = serde_json::to_vec(value)
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
            request_id: request_id.into(),
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
                request_id: format!("put_node:{}", Uuid::new_v4()),
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

    /// EN: Find documents whose `search_field` equals `value` (`FindByField`).
    ///
    /// EN: `total_count` on the returned [`DocumentPage`] is a count over
    /// the matching field-index entries — see [`DocumentPage`] for how this
    /// compares to [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::query_documents`].
    /// FR: Cherche les documents dont `search_field` vaut `value`
    /// (`FindByField`).
    ///
    /// FR: `total_count` sur le [`DocumentPage`] retourne est un comptage
    /// sur les entrees d index de champ correspondantes — voir
    /// [`DocumentPage`] pour la comparaison avec
    /// [`RociaDbClient::list_documents`] et
    /// [`RociaDbClient::query_documents`].
    pub async fn search_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        search_field: &str,
        value: &impl Serialize,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
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

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
    }

    /// EN: Return one paginated page of every document in `collection_name`
    /// (`ListDoc`).
    ///
    /// EN: `total_count` on the returned [`DocumentPage`] is **free**: the
    /// server keeps a running per-collection counter updated on every
    /// write, so reading it costs nothing beyond the listing itself — see
    /// [`DocumentPage`] for how this compares to
    /// [`RociaDbClient::search_documents`] and
    /// [`RociaDbClient::query_documents`].
    /// FR: Retourne une page paginee de tous les documents de
    /// `collection_name` (`ListDoc`).
    ///
    /// FR: `total_count` sur le [`DocumentPage`] retourne est **gratuit** :
    /// le serveur maintient un compteur par collection mis a jour a chaque
    /// ecriture, le lire ne coute rien de plus que le listing lui-meme —
    /// voir [`DocumentPage`] pour la comparaison avec
    /// [`RociaDbClient::search_documents`] et
    /// [`RociaDbClient::query_documents`].
    pub async fn list_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
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

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
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
    ///
    /// EN: `total_count` on the returned [`DocumentPage`] is **expensive**:
    /// the server only knows it after filtering the complete candidate set
    /// for the query, so the cost scales with the number of candidates on
    /// every call — never call this in a loop just to get a count; see
    /// [`DocumentPage`] for the full comparison with
    /// [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::search_documents`].
    /// FR: Le serveur applique les filtres avec un ET logique et utilise
    /// la liste de tri dans l ordre fourni. Le `next_cursor` retourne est
    /// opaque et doit etre reutilise tel quel.
    ///
    /// FR: `total_count` sur le [`DocumentPage`] retourne est **couteux** :
    /// le serveur ne le connait qu apres avoir filtre l integralite du jeu
    /// de candidats de la requete, donc le cout croit avec le nombre de
    /// candidats a chaque appel — ne l appelez jamais en boucle juste pour
    /// obtenir un compte ; voir [`DocumentPage`] pour la comparaison
    /// complete avec [`RociaDbClient::list_documents`] et
    /// [`RociaDbClient::search_documents`].
    pub async fn query_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        filters: &[DocumentQueryFilter],
        sort: &[DocumentQuerySort],
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
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

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
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

    /// EN: Upsert a batch of nodes in a graph with bounded concurrency (at
    /// most 10 `PutNode` calls in flight at once). `nodes` is consumed in
    /// the order the caller provides — duplicate
    /// `node_id`s are **not** merged, both are sent, in order.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `nodes`: Ordered [`NodeInput`] items to upsert.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `nodes`: Items [`NodeInput`] ordonnes a upserter.
    ///
    /// EN: Returns:
    /// - `()` on success.
    /// FR: Returns:
    /// - `()` en cas de succes.
    ///
    /// EN: **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `nodes` sequence with the same [`NodeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, request_id)`, so already-applied writes are
    /// recognized and skipped rather than reapplied, and only the writes
    /// that never landed actually happen.
    /// FR: **Ce batch n est pas atomique et s arrete a la premiere erreur**
    /// : en cas d echec, les requetes en vol sont annulees et l erreur ne
    /// dit pas quels items avaient deja abouti. Pour reprendre apres un
    /// echec, rejouez la meme sequence `nodes` avec les memes valeurs de
    /// [`NodeInput::request_id`] que la premiere fois — le serveur
    /// deduplique sur `(tenant, operation, request_id)`, donc les ecritures
    /// deja appliquees sont reconnues et ignorees plutot que reappliquees,
    /// et seules celles qui n avaient pas abouti se produisent reellement.
    pub async fn put_nodes(
        &self,
        tenant_id: &str,
        graph_name: &str,
        nodes: impl IntoIterator<Item = NodeInput>,
    ) -> Result<()> {
        let nodes: Vec<NodeInput> = nodes.into_iter().collect();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_count = nodes.len(),
            "upserting graph nodes batch"
        );
        let requests = build_put_node_requests(tenant_id, graph_name, nodes)?;
        stream::iter(requests.into_iter().map(Ok::<_, RociaDbError>))
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

    /// EN: Upsert a batch of edges with bounded concurrency (at most 10
    /// `AddEdge` calls in flight at once). `edges` is consumed in the order
    /// the caller provides — duplicate `edge_id`s are **not** merged, both
    /// are sent, in order.
    ///
    /// EN: Arguments:
    /// - `tenant_id`: Tenant identifier.
    /// - `graph_name`: Graph name.
    /// - `edges`: Ordered [`EdgeInput`] items to upsert.
    /// FR: Arguments:
    /// - `tenant_id`: Identifiant du tenant.
    /// - `graph_name`: Nom du graph.
    /// - `edges`: Items [`EdgeInput`] ordonnes a upserter.
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
    ///
    /// EN: **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `edges` sequence with the same [`EdgeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, request_id)`, so already-applied writes are
    /// recognized and skipped rather than reapplied, and only the writes
    /// that never landed actually happen.
    /// FR: **Ce batch n est pas atomique et s arrete a la premiere erreur**
    /// : en cas d echec, les requetes en vol sont annulees et l erreur ne
    /// dit pas quels items avaient deja abouti. Pour reprendre apres un
    /// echec, rejouez la meme sequence `edges` avec les memes valeurs de
    /// [`EdgeInput::request_id`] que la premiere fois — le serveur
    /// deduplique sur `(tenant, operation, request_id)`, donc les ecritures
    /// deja appliquees sont reconnues et ignorees plutot que reappliquees,
    /// et seules celles qui n avaient pas abouti se produisent reellement.
    pub async fn add_edges(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edges: impl IntoIterator<Item = EdgeInput>,
    ) -> Result<()> {
        let edges: Vec<EdgeInput> = edges.into_iter().collect();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_count = edges.len(),
            "upserting graph edges batch"
        );
        let requests = build_add_edge_requests(tenant_id, graph_name, edges)?;
        stream::iter(requests.into_iter().map(Ok::<_, RociaDbError>))
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
    use super::{
        BearerInterceptor, DEFAULT_CONNECT_TIMEOUT, DocumentPage, DocumentServiceClient, EdgeInput,
        FileServiceClient, GraphServiceClient, NodeInput, RociaDbBuilder, RociaDbClient,
        TenantServiceClient, build_add_edge_requests, build_put_node_requests,
        default_document_request_id, resolve_connect_timeout, validate_host_path,
        validate_node_binding,
    };
    use crate::{FileStreamUploadOptions, RociaDbError};
    use futures::stream;
    use std::time::Duration;
    use tonic::transport::Endpoint;

    /// EN: A `RociaDbClient` wired to a channel that never actually dials
    /// (`Endpoint::connect_lazy` performs no I/O — it only builds a
    /// connector that would try to connect on the *first real RPC*). Used
    /// to test the client-side gating that must reject a request before
    /// ever reaching the network — if such a test regressed and the
    /// gating ran too late, it would hang or fail against the
    /// unreachable `127.0.0.1:1` host instead of returning promptly.
    /// FR: Un `RociaDbClient` cable sur un canal qui ne compose jamais
    /// reellement (`Endpoint::connect_lazy` n effectue aucune E/S — il ne
    /// fait que construire un connecteur qui tenterait de se connecter au
    /// *premier RPC reel*). Utilise pour tester les gardes-fous cote
    /// client qui doivent rejeter une requete avant d atteindre le
    /// reseau — si un tel test regressait et que la garde s executait
    /// trop tard, il bloquerait ou echouerait contre le host injoignable
    /// `127.0.0.1:1` au lieu de retourner promptement.
    fn lazy_test_client() -> RociaDbClient {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = BearerInterceptor::disabled();
        RociaDbClient {
            upstream_document: DocumentServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_graph: GraphServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_file: FileServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_tenant: TenantServiceClient::with_interceptor(channel, interceptor),
            token_manager: None,
            _token_refresh_guard: None,
        }
    }

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

    // EN: `build_put_node_requests` / `build_add_edge_requests` are the pure,
    // network-free cores of `RociaDbClient::put_nodes` /
    // `RociaDbClient::add_edges` (see their doc comments) — extracted for
    // exactly this reason: the 0.4.0 -> 0.5.0 rework replaced a
    // `HashMap`-keyed batch input with an ordered `Vec<NodeInput>` /
    // `Vec<EdgeInput>` precisely because a `HashMap` neither preserves
    // caller order nor keeps duplicate keys, and silently generated one
    // idempotency key per batch instead of one per item. These tests lock
    // in the three properties that motivated the change.
    // FR: `build_put_node_requests` / `build_add_edge_requests` sont les
    // coeurs purs, sans reseau, de `RociaDbClient::put_nodes` /
    // `RociaDbClient::add_edges` (voir leurs doc comments) — extraites
    // exactement pour cette raison : le passage de 0.4.0 a 0.5.0 a remplace
    // une entree de batch indexee par `HashMap` par un `Vec<NodeInput>` /
    // `Vec<EdgeInput>` ordonne, precisement parce qu une `HashMap` ne
    // preserve ni l ordre de l appelant ni les cles dupliquees, et generait
    // silencieusement une seule cle d idempotence pour tout le batch au
    // lieu d une par item. Ces tests verrouillent les trois proprietes qui
    // ont motive ce changement.

    #[test]
    fn put_node_requests_preserve_caller_order() {
        // EN: This is precisely what a `HashMap<(String, String), Value>`
        // could not guarantee — iteration order over a hash map is
        // unspecified, so the pre-0.5.0 batch could silently reorder
        // `PutNode` calls relative to what the caller wrote.
        // FR: C est precisement ce qu une `HashMap<(String, String), Value>`
        // ne pouvait pas garantir — l ordre d iteration d une hash map
        // n est pas specifie, donc le batch pre-0.5.0 pouvait reordonner
        // silencieusement les appels `PutNode` par rapport a ce que
        // l appelant avait ecrit.
        let nodes = vec![
            NodeInput {
                node_id: "product:3".to_string(),
                value: serde_json::json!({"n": 3}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 1}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:2".to_string(),
                value: serde_json::json!({"n": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.node_id.as_str()).collect();
        assert_eq!(ids, vec!["product:3", "product:1", "product:2"]);
    }

    #[test]
    fn put_node_requests_do_not_merge_duplicate_node_ids() {
        let nodes = vec![
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 1}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by node_id would have collapsed this to one request"
        );
        assert_eq!(requests[0].node_id, "product:1");
        assert_eq!(requests[1].node_id, "product:1");
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn put_node_requests_use_node_id_verbatim_with_no_label_recomposition() {
        // EN: 0.4.0 took a `(label, id)` pair and built `node_id` internally
        // as `format!("{label}:{id}")`. 0.5.0 takes the complete node id and
        // must forward it unchanged.
        // FR: 0.4.0 prenait un couple `(label, id)` et construisait
        // `node_id` en interne via `format!("{label}:{id}")`. 0.5.0 prend
        // l id complet du node et doit le transmettre tel quel.
        let nodes = vec![NodeInput {
            node_id: "product:sku-1".to_string(),
            value: serde_json::json!({}),
            request_id: None,
        }];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].node_id, "product:sku-1");
    }

    #[test]
    fn put_node_requests_pass_through_caller_supplied_request_id() {
        let nodes = vec![NodeInput {
            node_id: "product:1".to_string(),
            value: serde_json::json!({}),
            request_id: Some("caller-chosen-id".to_string()),
        }];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].request_id, "caller-chosen-id");
    }

    #[test]
    fn put_node_requests_default_request_id_matches_the_single_item_put_node_prefix() {
        // EN: `put_nodes` (batch) and `put_node` (single-item) both issue
        // `PutNode` calls, so an absent id must default to the exact same
        // prefix on both paths: `put_node:<uuid>`. This used to diverge —
        // the batch path defaulted to `upsert_node:<uuid>` instead — which
        // meant the same operation had two different default idempotency
        // key shapes depending only on which method the caller happened to
        // use.
        // FR: `put_nodes` (batch) et `put_node` (unitaire) emettent tous
        // deux des appels `PutNode`, donc un id absent doit avoir par
        // defaut exactement le meme prefixe sur les deux chemins :
        // `put_node:<uuid>`. Cela divergeait auparavant — le chemin batch
        // avait pour defaut `upsert_node:<uuid>` — ce qui faisait que la
        // meme operation avait deux formes de cle d idempotence par defaut
        // differentes selon la seule methode utilisee par l appelant.
        let nodes = vec![
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:2".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        for request in &requests {
            let uuid_part = request
                .request_id
                .strip_prefix("put_node:")
                .expect("default request_id must use the put_node: prefix");
            uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn add_edge_requests_preserve_caller_order() {
        let edges = vec![
            EdgeInput {
                edge_id: "e3".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "b".to_string(),
                to: "c".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e2".to_string(),
                from: "c".to_string(),
                to: "d".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.edge_id.as_str()).collect();
        assert_eq!(ids, vec!["e3", "e1", "e2"]);
    }

    #[test]
    fn add_edge_requests_do_not_merge_duplicate_edge_ids() {
        let edges = vec![
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({"v": 1}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({"v": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by edge_id would have collapsed this to one request"
        );
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn add_edge_requests_pass_through_caller_supplied_request_id() {
        let edges = vec![EdgeInput {
            edge_id: "e1".to_string(),
            from: "a".to_string(),
            to: "b".to_string(),
            label: "knows".to_string(),
            value: serde_json::json!({}),
            request_id: Some("caller-chosen-id".to_string()),
        }];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        assert_eq!(requests[0].request_id, "caller-chosen-id");
    }

    #[test]
    fn add_edge_requests_default_request_id_stays_a_bare_uuid() {
        // EN: Unlike nodes, the pre-0.5.0 default for edges had no prefix
        // (a bare `Uuid::new_v4().to_string()`) — must stay that way.
        // FR: Contrairement aux nodes, le defaut pre-0.5.0 pour les edges
        // n avait pas de prefixe (un `Uuid::new_v4().to_string()` brut) —
        // doit le rester.
        let edges = vec![
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e2".to_string(),
                from: "b".to_string(),
                to: "c".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        for request in &requests {
            uuid::Uuid::parse_str(&request.request_id)
                .expect("default request_id must be a bare uuid with no prefix");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn document_page_exposes_items_next_cursor_and_total_count() {
        let page = DocumentPage {
            items: vec!["a", "b"],
            next_cursor: Some("cursor-2".to_string()),
            total_count: 42,
        };
        assert_eq!(page.items, vec!["a", "b"]);
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
        assert_eq!(page.total_count, 42);
    }

    #[test]
    fn document_page_has_no_next_cursor_on_the_last_page() {
        // EN: `next_cursor: None` is the only correct representation of
        // "there is no further page" — see the type's doc comment.
        // FR: `next_cursor: None` est la seule representation correcte de
        // "il n y a pas de page suivante" — voir le doc comment du type.
        let page: DocumentPage<i32> = DocumentPage {
            items: vec![1, 2, 3],
            next_cursor: None,
            total_count: 3,
        };
        assert!(page.next_cursor.is_none());
        assert_eq!(page.items, vec![1, 2, 3]);
        assert_eq!(page.total_count, 3);
    }

    #[test]
    fn document_page_derives_clone_and_equality() {
        let page = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 1,
        };
        assert_eq!(page.clone(), page);
        let different = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 2,
        };
        assert_ne!(page, different);
    }

    // EN: `validate_host_path` mirrors the TypeScript SDK's
    // `endpointFromHost` pathname check (see the function's own doc
    // comment). A host URL with a leftover path component would
    // otherwise be silently accepted by tonic, which simply ignores it
    // when dialing.
    // FR: `validate_host_path` reproduit le controle de pathname de
    // `endpointFromHost` cote TypeScript (voir le doc comment de la
    // fonction). Un host avec un chemin residuel serait sinon
    // silencieusement accepte par tonic, qui l ignore lors de la
    // connexion.

    #[test]
    fn host_path_validation_accepts_a_host_with_no_path_component() {
        validate_host_path("http://127.0.0.1:50051").expect("an absent path must be accepted");
    }

    #[test]
    fn host_path_validation_accepts_a_bare_root_path() {
        validate_host_path("http://127.0.0.1:50051/").expect("a bare \"/\" must be accepted");
    }

    #[test]
    fn host_path_validation_rejects_a_host_carrying_a_leftover_path() {
        let error = validate_host_path("http://127.0.0.1:50051/v1")
            .expect_err("a host with a non-root path must be rejected");
        assert!(matches!(error, RociaDbError::Connection { .. }));
        assert!(
            error.to_string().contains("/v1"),
            "the error should name the offending path, got: {error}"
        );
    }

    // EN: `resolve_connect_timeout` is the pure core behind
    // `RociaDbBuilder::build`'s connect-timeout handling: no explicit
    // value falls back to `DEFAULT_CONNECT_TIMEOUT`, and a zero timeout
    // is always rejected regardless of where it came from.
    // FR: `resolve_connect_timeout` est le coeur pur derriere la gestion
    // du delai de connexion de `RociaDbBuilder::build` : aucune valeur
    // explicite retombe sur `DEFAULT_CONNECT_TIMEOUT`, et un delai nul
    // est toujours rejete quelle que soit son origine.

    #[test]
    fn default_connect_timeout_matches_the_typescript_sdk_default() {
        // EN: This is the exact parity value from the cahier des charges:
        // the TypeScript SDK's `connectTimeoutMs` default is 10_000 ms.
        // FR: C est la valeur de parite exacte du cahier des charges : le
        // defaut `connectTimeoutMs` du SDK TypeScript est 10_000 ms.
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn resolve_connect_timeout_falls_back_to_the_default_when_unset() {
        let timeout =
            resolve_connect_timeout(None).expect("the default timeout must always be accepted");
        assert_eq!(timeout, DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn resolve_connect_timeout_accepts_a_caller_supplied_positive_value() {
        let timeout = resolve_connect_timeout(Some(Duration::from_secs(3)))
            .expect("a positive explicit timeout must be accepted");
        assert_eq!(timeout, Duration::from_secs(3));
    }

    #[test]
    fn resolve_connect_timeout_rejects_zero() {
        let error = resolve_connect_timeout(Some(Duration::ZERO))
            .expect_err("a zero connect timeout must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn builder_connect_timeout_setter_stores_the_value_unvalidated() {
        // EN: Mirrors `RociaDbBuilder::host` / `auth_client_credentials`:
        // the setter never validates, only `build()` does (via
        // `resolve_connect_timeout`, tested above) — so even a
        // nonsensical zero duration must be stored as-is here.
        // FR: Reproduit `RociaDbBuilder::host` / `auth_client_credentials` :
        // le setter ne valide jamais, seul `build()` le fait (via
        // `resolve_connect_timeout`, teste ci-dessus) — donc meme une
        // duree nulle absurde doit etre stockee telle quelle ici.
        let mut builder = RociaDbBuilder::new();
        builder.connect_timeout(Duration::ZERO);
        assert_eq!(builder.connect_timeout, Some(Duration::ZERO));

        let mut builder = RociaDbBuilder::new();
        builder.connect_timeout(Duration::from_secs(42));
        assert_eq!(builder.connect_timeout, Some(Duration::from_secs(42)));
    }

    #[tokio::test]
    async fn build_rejects_a_zero_connect_timeout_before_any_network_call() {
        // EN: `validate_host_path` and the connect-timeout check both run
        // before `Endpoint::connect()`, so this must return promptly with
        // `Validation` instead of hanging or failing against the
        // (deliberately unreachable) host.
        // FR: `validate_host_path` et le controle du delai de connexion
        // s executent tous deux avant `Endpoint::connect()`, donc ceci
        // doit retourner promptement avec `Validation` plutot que de
        // bloquer ou echouer contre le host (deliberement injoignable).
        let mut builder = RociaDbBuilder::new();
        builder
            .host("http://127.0.0.1:1")
            .connect_timeout(Duration::ZERO);
        // EN: `RociaDbClient` intentionally does not derive `Debug` (it
        // would expose channel/interceptor internals), so `expect_err`
        // cannot be used here — match instead.
        // FR: `RociaDbClient` ne derive volontairement pas `Debug` (cela
        // exposerait les internals du canal/interceptor), donc
        // `expect_err` ne peut pas etre utilise ici — on utilise un match
        // a la place.
        let error = match builder.build().await {
            Ok(_) => panic!("a zero connect timeout must fail build()"),
            Err(error) => error,
        };
        assert!(matches!(error, RociaDbError::Validation(_)));
    }

    #[tokio::test]
    async fn build_rejects_a_host_with_a_leftover_path_before_any_network_call() {
        let mut builder = RociaDbBuilder::new();
        builder.host("http://127.0.0.1:1/v1");
        let error = match builder.build().await {
            Ok(_) => panic!("a host carrying a path must fail build()"),
            Err(error) => error,
        };
        assert!(matches!(error, RociaDbError::Connection { .. }));
    }

    // EN: `BuilderAuthConfig`'s manual `Debug` impl must redact
    // `client_secret` — a derived `Debug` (the pre-fix behavior) would
    // print it in clear text, so this test would fail against that old
    // behavior.
    // FR: L impl `Debug` manuelle de `BuilderAuthConfig` doit rediger
    // `client_secret` — un `Debug` derive (l ancien comportement) l aurait
    // affiche en clair, donc ce test echouerait contre cet ancien
    // comportement.
    #[test]
    fn builder_debug_output_redacts_the_client_secret() {
        let mut builder = RociaDbBuilder::new();
        builder.auth_client_credentials(
            "https://idp.example.com/token",
            "client-123",
            "super-secret-value",
        );
        let debug_output = format!("{builder:?}");
        assert!(
            !debug_output.contains("super-secret-value"),
            "the raw client_secret must never appear in Debug output, got: {debug_output}"
        );
        assert!(
            debug_output.contains("[redacted]"),
            "the redaction placeholder must appear, got: {debug_output}"
        );
        // EN: Non-sensitive fields must stay visible: only the secret is
        // redacted, not the whole auth config (still useful for
        // diagnostics).
        // FR: Les champs non sensibles doivent rester visibles : seul le
        // secret est redige, pas toute la config d auth (reste utile pour
        // le diagnostic).
        assert!(debug_output.contains("https://idp.example.com/token"));
        assert!(debug_output.contains("client-123"));
    }

    #[test]
    fn default_document_request_id_uses_the_put_document_prefix_with_a_fresh_uuid_each_time() {
        // EN: This is the request_id-prefix consistency fix applied to
        // `create_document`'s document write: it must default to the same
        // `put_document:{collection}:<uuid>` shape `put_document` itself
        // uses, not the pre-0.6.0 `upsert_document:...` prefix.
        // FR: C est le correctif de coherence des prefixes de request_id
        // applique a l ecriture document de `create_document` : elle doit
        // avoir par defaut la meme forme `put_document:{collection}:<uuid>`
        // qu utilise `put_document` lui-meme, pas le prefixe
        // `upsert_document:...` d avant 0.6.0.
        let first = default_document_request_id("catalog");
        let second = default_document_request_id("catalog");
        let uuid_part = first
            .strip_prefix("put_document:catalog:")
            .expect("default request_id must use the put_document:{collection}: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(
            first, second,
            "each call without an explicit request_id must get its own generated id"
        );
    }

    // EN: `upload_file_chunked`'s pre-flight validation (file size,
    // checksum length) must run — and fail — before the method ever
    // touches the network, so these tests run against a client wired to
    // an unreachable host and must still return promptly.
    // FR: La validation prealable de `upload_file_chunked` (taille du
    // fichier, longueur du checksum) doit s executer — et echouer — avant
    // que la methode ne touche le reseau, donc ces tests s executent
    // contre un client cable sur un host injoignable et doivent quand
    // meme retourner promptement.

    #[tokio::test]
    async fn upload_file_chunked_rejects_an_oversized_file_before_any_network_call() {
        let client = lazy_test_client();
        let oversized = 5u64 * 1024 * 1024 * 1024 + 1; // 5 GiB + 1 byte
        let result = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                oversized,
                vec![0u8; 32],
                stream::empty::<Vec<u8>>(),
                FileStreamUploadOptions::default(),
            )
            .await;
        let error = result.expect_err("a file over the 5 GiB limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("5 GiB"));
    }

    #[tokio::test]
    async fn upload_file_chunked_rejects_a_wrong_length_checksum_before_any_network_call() {
        let client = lazy_test_client();
        let result = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                0,
                vec![0u8; 10], // must be exactly 32 bytes (sha256)
                stream::empty::<Vec<u8>>(),
                FileStreamUploadOptions::default(),
            )
            .await;
        let error = result.expect_err("a checksum that is not 32 bytes must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("32 bytes"));
    }

    #[tokio::test]
    async fn invalidate_auth_token_is_a_harmless_no_op_when_auth_is_disabled() {
        // EN: `lazy_test_client()` itself needs a tokio runtime just to
        // build its (never-dialed) channel — but `invalidate_auth_token`
        // is called here with no `.await`, which is the point: it is
        // synchronous by design and must never need to wait on a network
        // round trip, unlike `refresh_auth_token`.
        // FR: `lazy_test_client()` a elle-meme besoin d un runtime tokio
        // seulement pour construire son canal (jamais compose) — mais
        // `invalidate_auth_token` est appelee ici sans `.await`, ce qui
        // est precisement le point : elle est synchrone par conception et
        // ne doit jamais avoir besoin d attendre un round-trip reseau,
        // contrairement a `refresh_auth_token`.
        let client = lazy_test_client();
        client.invalidate_auth_token();
    }
}
