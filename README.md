# rocia-db-sdk

EN: Rust client SDK for the Rocia DB gRPC upstream services (documents, graph,
files, tenants).
FR: SDK Rust pour les services gRPC upstream de Rocia DB (documents, graph,
fichiers, tenants).

EN: The Node.js/TypeScript SDK lives in its own sibling repository
([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts)), not in this checkout — there is no `typescript/`
directory here.
FR: Le SDK Node.js/TypeScript vit dans son propre depot voisin
([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts)), pas dans ce checkout — il n y a pas de repertoire
`typescript/` ici.

## Overview

EN: This crate provides a small client wrapper around the generated gRPC clients.
FR: Cette crate fournit un petit wrapper client autour des clients gRPC generes.

## Installation

EN: This is a standalone crate, not a workspace member: there is no
`crates/rocia-db-sdk` path inside it. Depend on it as a path dependency from a
sibling checkout, or as a git dependency:
FR: C est une crate autonome, pas un membre de workspace : il n y a pas de
chemin `crates/rocia-db-sdk` a l interieur. Ajoutez-la comme dependance de
chemin depuis un checkout voisin, ou comme dependance git :

```toml
[dependencies]
# From a sibling checkout:
rocia-db-sdk = { path = "../rocia-db-sdk-rust" }
# Or pinned to a tag from git:
# rocia-db-sdk = { git = "https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust", tag = "v0.3.0" }
```

## Quick Start

```rust
use rocia_db_sdk::RociaDbBuilder;
use serde_json::json;

# #[tokio::main]
# async fn main() -> rocia_db_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("http://127.0.0.1:50051")
    .auth_client_credentials(
        "https://example.com/token",
        "client-id",
        "client-secret",
    )
    .build()
    .await?;

client
    .create_document(
        "tenant-1",
        "products",
        "sku-123",
        json!({"sku": "sku-123", "label": "Widget"}),
        Some("product".to_string()),
        Some("products".to_string()),
    )
    .await?;

let node = client.get_node("tenant-1", "products", "product:sku-123").await?;
println!("node = {node}");
# Ok(())
# }
```

EN: `.host(...)` above is a plaintext (`http://`) local address for
illustration. See [Transport and TLS](#transport-and-tls) for what to use
against a real deployment.
FR: `.host(...)` ci-dessus est une adresse locale en clair (`http://`) a
titre d illustration. Voir [Transport and TLS](#transport-and-tls) pour ce
qu il faut utiliser face a un deploiement reel.

## Authentication

### Token lifetime and refresh

EN: The identity provider (`rocia-idp`) issues bearer tokens that live for
exactly **600 seconds**, hardcoded server-side — there is no negotiating a
longer-lived token. `RociaDbBuilder::build` fetches the first token and
starts a background task that refreshes it before it expires:
`max(expires_in * 2 / 3, 5s)`, i.e. roughly every 400 seconds for the current
600-second lifetime, leaving about a third of the lifetime as margin. The
task keeps running for as long as the returned `RociaDbClient`, or any of
its clones, stays alive; dropping the last clone stops it.
FR: Le fournisseur d identite (`rocia-idp`) emet des tokens bearer qui vivent
exactement **600 secondes**, en dur cote serveur — il n y a pas de token a
duree de vie plus longue a negocier. `RociaDbBuilder::build` recupere le
premier token et demarre une tache en arriere-plan qui le rafraichit avant
son expiration : `max(expires_in * 2 / 3, 5s)`, soit environ toutes les 400
secondes pour la duree de vie actuelle de 600 secondes, en laissant environ
un tiers de la duree de vie en marge. La tache continue de tourner tant que
le `RociaDbClient` retourne, ou l un de ses clones, reste vivant ; laisser
tomber le dernier clone l arrete.

EN: A client that never calls `RociaDbBuilder::build` (or that discards the
refresh guard some other way) simply dies after 10 minutes with
`UNAUTHENTICATED` on every call. The builder handles this for you; only
build your own `TokenManager` (see [Auth Helpers](#auth-helpers)) if you
need auth outside the `RociaDbClient` lifecycle.
FR: Un client qui n appelle jamais `RociaDbBuilder::build` (ou qui perd le
guard de refresh d une autre maniere) meurt simplement au bout de 10 minutes
avec `UNAUTHENTICATED` sur chaque appel. Le builder gere cela pour vous ; ne
construisez votre propre `TokenManager` (voir [Auth Helpers](#auth-helpers))
que si vous avez besoin de l auth en dehors du cycle de vie de
`RociaDbClient`.

### `UNAUTHENTICATED` vs `PERMISSION_DENIED`

EN: The two failure modes need different handling, and confusing them wastes
retries:
FR: Les deux modes d echec demandent un traitement different, et les
confondre gaspille des retries :

- `UNAUTHENTICATED` — the token is missing, expired, malformed, or issued by
  a different issuer. This **is** a signal to renew: call
  `RociaDbClient::refresh_auth_token()` and retry.
- `PERMISSION_DENIED` — the token is valid but lacks the required scope.
  Retrying after a refresh will not help, because a fresh token carries the
  same scope. This happens in exactly two cases: a read-only client calling
  one of the 7 write RPCs, or an admin-scoped token (from `rocia-idp`'s
  account-management API) calling *any* of the 22 RPCs, reads included — see
  [Tenancy and Authorization Scopes](#tenancy-and-authorization-scopes).

```rust
match client.get_document::<serde_json::Value>("tenant-1", "products", "sku-123").await {
    Ok(doc) => { /* ... */ }
    Err(err) if err.is_unauthenticated() => {
        client.refresh_auth_token().await?;
        // retry
    }
    Err(err) if err.is_permission_denied() => {
        // do not retry: wrong scope or wrong client_id
    }
    Err(err) => return Err(err),
}
```

EN: `is_unauthenticated()` and `is_permission_denied()` are shorthands on
`RociaDbError` for the two gRPC codes that matter most here — see
[API Conventions](#api-conventions) for the full typed-error surface,
including `.code()`, `.reason()`, and `.status()` for anything more
specific than these two predicates.
FR: `is_unauthenticated()` et `is_permission_denied()` sont des raccourcis
sur `RociaDbError` pour les deux codes gRPC qui comptent ici — voir
[API Conventions](#api-conventions) pour la surface complete de l erreur
typee, dont `.code()`, `.reason()`, et `.status()` pour tout ce qui va
au-dela de ces deux predicats.

EN: Every gRPC status also carries a `reason` metadata value (`invalid_argument`,
`not_found`, `already_exists`, `permission_denied`, `unauthenticated`,
`internal`) that pins down the cause more precisely than the gRPC code alone.
FR: Chaque statut gRPC porte aussi une metadonnee `reason` (`invalid_argument`,
`not_found`, `already_exists`, `permission_denied`, `unauthenticated`,
`internal`) qui precise la cause plus finement que le seul code gRPC.

### Default Auth Behavior

EN: Authentication is enabled by default in `RociaDbBuilder`.
FR: L authentification est activee par defaut dans `RociaDbBuilder`.

EN: If you do not call `auth_client_credentials`, the builder reads:
FR: Si tu n appelles pas `auth_client_credentials`, le builder lit:
- `AUTH_TOKEN_URL`
- `AUTH_CLIENT_ID`
- `AUTH_CLIENT_SECRET`

EN: For local/testing environments you can disable auth explicitly:
FR: Pour un environnement local/test tu peux desactiver l auth explicitement:

```rust
let client = RociaDbBuilder::new()
    .host("http://127.0.0.1:50051")
    .disable_auth()
    .build()
    .await?;
```

### Auth Helpers

EN: The `auth` module provides token and interceptor helpers directly, for
callers who need auth outside of `RociaDbClient` (for example, to reuse the
same token against a different service).
FR: Le module `auth` fournit directement des helpers de token et
d interceptor, pour les appelants qui ont besoin de l auth en dehors de
`RociaDbClient` (par exemple pour reutiliser le meme token contre un autre
service).

```rust
use rocia_db_sdk::auth::TokenManager;

let http = reqwest::Client::new();
let token_manager = TokenManager::new(
    http,
    "https://example.com/token".to_string(),
    "client-id".to_string(),
    "client-secret".to_string(),
).await?;
let _refresh_guard = token_manager.spawn_refresh(token_manager.refresh_interval());
```

EN: `spawn_refresh` returns a `#[must_use]` guard: dropping it immediately
stops the background refresh, which is why the example above binds it to a
variable instead of discarding it.
FR: `spawn_refresh` retourne un guard `#[must_use]` : le laisser tomber
arrete immediatement le refresh en arriere-plan, d ou le fait que l exemple
ci-dessus le lie a une variable plutot que de le jeter.

## Transport and TLS

EN: `rocia-db` does not implement TLS itself and never will: the server
always listens in plaintext. In production, TLS terminates at a reverse
proxy placed in front of it (the proxy holds the certificates and handles
renewal); the SDK then connects with `https://` to the proxy, typically on
port 443, and the proxy forwards plaintext gRPC (HTTP/2) to the backend on
its own port (5xxxx conventionally). Connecting `http://` directly to the
bare server, as in the examples above, is only appropriate for local
development or an already-encrypted internal network segment.
FR: `rocia-db` n implemente pas TLS lui-meme et ne le fera jamais : le
serveur ecoute toujours en clair. En production, TLS se termine a un
reverse proxy place devant lui (le proxy porte les certificats et gere leur
renouvellement) ; le SDK se connecte alors en `https://` au proxy,
generalement sur le port 443, et le proxy relaie du gRPC (HTTP/2) en clair
vers le backend sur son propre port. Se connecter directement en `http://`
au serveur nu, comme dans les exemples ci-dessus, ne convient qu au
developpement local ou a un segment reseau interne deja chiffre.

EN: The proxy must be configured for HTTP/2 end to end (no TLS-to-HTTP/1.1
downgrade toward the backend), or every gRPC call fails immediately, usually
with `UNAVAILABLE`.
FR: Le proxy doit etre configure en HTTP/2 de bout en bout (pas de
retrogradation TLS vers HTTP/1.1 cote backend), sinon tous les appels gRPC
echouent immediatement, generalement avec `UNAVAILABLE`.

## Batch Operations

EN: The client uses a bounded concurrency of 10 for batch upserts.
`put_nodes` and `add_edges` each take an ordered sequence of
`NodeInput`/`EdgeInput` structs — anything `IntoIterator<Item =
NodeInput>` / `IntoIterator<Item = EdgeInput>`, a `Vec` in the common case —
not a `HashMap`: items are dispatched in the order given, and two items
sharing the same id are never silently merged into one, both are sent.
FR: Le client utilise une concurrence bornee de 10 pour les batchs.
`put_nodes` et `add_edges` prennent chacun une sequence ordonnee de structs
`NodeInput`/`EdgeInput` — n importe quel `IntoIterator<Item =
NodeInput>` / `IntoIterator<Item = EdgeInput>`, un `Vec` dans le cas courant
— pas une `HashMap` : les items sont envoyes dans l ordre fourni, et deux
items partageant le meme id ne sont jamais fusionnes silencieusement, les
deux sont envoyes.

```rust
use rocia_db_sdk::{EdgeInput, NodeInput};
use serde_json::json;

let nodes = vec![
    NodeInput {
        node_id: "product:sku-1".to_string(),
        value: json!({"sku": "sku-1"}),
        request_id: None,
    },
    NodeInput {
        node_id: "product:sku-2".to_string(),
        value: json!({"sku": "sku-2"}),
        request_id: None,
    },
];
client.put_nodes("tenant-1", "products", nodes).await?;

let edges = vec![EdgeInput {
    edge_id: "1".to_string(),
    from: "product:sku-1".to_string(),
    to: "group:grp-1".to_string(),
    label: "belongs_to".to_string(),
    value: json!({"weight": 1}),
    request_id: None,
}];
client.add_edges("tenant-1", "products", edges).await?;
```

EN: `node_id` is the **complete** node id (`"product:sku-1"`) — the SDK
never recomposes it from a `(label, id)` pair, so build the full id
yourself. The edge id is raw and must not be prefixed with the label.
FR: `node_id` est l id **complet** du node (`"product:sku-1"`) — le SDK ne
le recompose jamais a partir d un couple `(label, id)`, donc construisez
l id complet vous-meme. L id d edge est brut et ne doit pas etre prefixe par
le label.

EN: `request_id: None` above lets the SDK generate a fresh idempotency key
for that item. Set it explicitly — and reuse the same value on a retry —
whenever a batch might need to be replayed; see
[Migrating to 0.5.0](#migrating-to-050) for why that matters and what it
looks like.
FR: `request_id: None` ci-dessus laisse le SDK generer une cle d idempotence
fraiche pour cet item. Fixez-la explicitement — et reutilisez la meme
valeur lors d un rejeu — des qu un batch peut avoir besoin d etre rejoue ;
voir [Migrating to 0.5.0](#migrating-to-050) pour comprendre pourquoi et a
quoi cela ressemble.

EN: `add_edges` (and `add_edge`) fail with `NOT_FOUND` for any edge whose
`from` or `to` node does not already exist in the graph — create both
endpoint nodes (via `put_nodes`/`put_node`) before adding edges between
them.
FR: `add_edges` (et `add_edge`) echouent avec `NOT_FOUND` pour toute edge
dont le node `from` ou `to` n existe pas deja dans le graph — creez les deux
nodes aux extremites (via `put_nodes`/`put_node`) avant d ajouter des edges
entre eux.

EN: **Neither batch is atomic: each stops at the first error.** In-flight
requests are cancelled, and the error does not say which items had already
succeeded. The correct way to resume is to replay the same items with the
same `request_id` values used on the first attempt — the server
deduplicates on `(tenant, operation, request_id)`, so already-applied
writes are recognized and skipped rather than reapplied, and only the
writes that never landed actually happen.
FR: **Aucun des deux batchs n est atomique : chacun s arrete a la premiere
erreur.** Les requetes en vol sont annulees, et l erreur ne dit pas quels
items avaient deja abouti. La bonne facon de reprendre est de rejouer les
memes items avec les memes valeurs de `request_id` que la premiere fois —
le serveur deduplique sur `(tenant, operation, request_id)`, donc les
ecritures deja appliquees sont reconnues et ignorees plutot que
reappliquees, et seules celles qui n avaient pas abouti se produisent
reellement.

## Neighbors

```rust
let neighbors = client
    .get_outgoing_neighbor_nodes::<serde_json::Value>(
        "tenant-1",
        "products",
        "product:sku-1",
        "belongs_to",
    )
    .await?;
```

EN: Use `neighbors_out` and `neighbors_in` when you need the raw, single-page
graph neighbors instead of hydrated node payloads across the full listing.
`get_outgoing_neighbor_nodes`/`get_incoming_neighbor_nodes` paginate to
completion internally, and only stop when `next_cursor` comes back absent —
never merely because one page was empty or shorter than requested. The
server can legitimately hand back an empty or short page in the middle of a
listing (a stale index entry surviving a deleted node, for example) with
more data still to come after it.
FR: Utilisez `neighbors_out` et `neighbors_in` quand vous avez besoin des
voisins graph bruts, page par page, plutot que des payloads de node hydrates
sur l ensemble du listing. `get_outgoing_neighbor_nodes`/
`get_incoming_neighbor_nodes` paginent jusqu au bout en interne, et ne
s arretent que quand `next_cursor` revient absent — jamais simplement parce
qu une page etait vide ou plus courte que demande. Le serveur peut
legitimement renvoyer une page vide ou courte au milieu d un listing (une
entree d index perimee survivant a un node supprime, par exemple) suivie
d autres donnees.

## File Operations

EN: `upload_file` and `download_file` are ergonomic in-memory helpers; the
underlying gRPC contract they implement is stricter than it looks and worth
understanding even if you never touch `upload_file_stream` directly.
FR: `upload_file` et `download_file` sont des aides ergonomiques en memoire ;
le contrat gRPC sous-jacent qu elles implementent est plus strict qu il n y
parait et vaut la peine d etre compris meme si vous ne touchez jamais
directement a `upload_file_stream`.

### The upload wire contract

EN:
- The server stores every file as a fixed sequence of exactly **1 MiB**
  (1,048,576-byte) chunks, and on download it always replays
  `ceil(size_bytes / 1 MiB)` chunk indexes — regardless of how the upload was
  actually sliced into messages. Uploading with any chunk size other than
  exactly 1 MiB (except the last, which may be shorter) makes a later
  download **silently return truncated or garbled data**: there is no
  server-side error, because the server has no way to know the upload used a
  different chunking scheme. `upload_file` always emits exactly-1-MiB chunks
  for you; this is why `FileUploadOptions` has no `chunk_size` knob.
- The **first** message of the upload stream must carry the metadata:
  `tenant_id`, `bucket`, `file_id`, `size_bytes` (the exact total byte
  count), `content_type`, `checksum`, and `request_id`. Every later message
  is only read for its `chunk` field.
- `checksum` must be exactly **32 raw bytes** — a SHA-256 digest. The server
  rejects any other length, including empty, with `INVALID_ARGUMENT`
  (`"checksum must be 32 bytes (sha256)"`); note that the server does *not*
  verify the checksum actually matches the uploaded bytes, only that it is
  32 bytes long. `upload_file` computes this SHA-256 digest of the buffer
  automatically when `FileUploadOptions.checksum` is `None`; if you supply
  your own, it must be exactly 32 bytes or the call fails client-side,
  before any network call.
- Each message's `chunk` must not exceed 1 MiB, or the server rejects it
  with `INVALID_ARGUMENT` (`"chunk exceeds 1 MiB"`). The sum of every
  `chunk`'s bytes across the stream must equal `size_bytes` exactly, or the
  server rejects the upload with `INVALID_ARGUMENT`
  (`"size_bytes does not match uploaded data"`).
- Files over the server's `limits.max_file_bytes` (**5 GiB by default**) are
  rejected. `upload_file` checks this client-side and returns a clear error
  before sending anything.
- An empty file is a valid, common case: it needs exactly one message
  (metadata only, empty `chunk`) and no data messages. `upload_file` handles
  this for you.
- The file only becomes visible (in `list_files`, `stat_file`,
  `download_file`) once the whole stream has been received and validated.
  An interrupted stream leaves orphaned chunks that a background GC
  eventually reclaims; the partial file never appears anywhere.

FR:
- Le serveur stocke chaque fichier en une sequence fixe de chunks d
  exactement **1 MiB** (1 048 576 octets), et au download il relit toujours
  les index `ceil(size_bytes / 1 MiB)` — quel que soit le decoupage reel de
  l upload en messages. Uploader avec une taille de chunk differente de 1
  MiB exactement (sauf le dernier, qui peut etre plus court) fait qu un
  download ulterieur **renvoie silencieusement des donnees tronquees ou
  corrompues** : il n y a pas d erreur cote serveur, car le serveur n a
  aucun moyen de savoir que l upload a utilise un decoupage different.
  `upload_file` emet toujours des chunks d exactement 1 MiB pour vous ; c est
  pourquoi `FileUploadOptions` n a pas de reglage `chunk_size`.
- Le **premier** message du flux d upload doit porter les metadonnees :
  `tenant_id`, `bucket`, `file_id`, `size_bytes` (le nombre total exact
  d octets), `content_type`, `checksum` et `request_id`. Chaque message
  suivant n est lu que pour son champ `chunk`.
- `checksum` doit faire exactement **32 octets bruts** — un digest SHA-256.
  Le serveur rejette toute autre longueur, y compris vide, avec
  `INVALID_ARGUMENT` (`"checksum must be 32 bytes (sha256)"`) ; notez que le
  serveur ne verifie *pas* que le checksum correspond reellement aux octets
  uploades, seulement qu il fait 32 octets. `upload_file` calcule ce digest
  SHA-256 du buffer automatiquement quand `FileUploadOptions.checksum` vaut
  `None` ; si vous en fournissez un vous-meme, il doit faire exactement 32
  octets sinon l appel echoue cote client, avant tout appel reseau.
- Le `chunk` de chaque message ne doit pas depasser 1 MiB, sinon le serveur
  le rejette avec `INVALID_ARGUMENT` (`"chunk exceeds 1 MiB"`). La somme des
  octets de `chunk` sur tout le flux doit egaler exactement `size_bytes`,
  sinon le serveur rejette l upload avec `INVALID_ARGUMENT`
  (`"size_bytes does not match uploaded data"`).
- Les fichiers au-dela de `limits.max_file_bytes` cote serveur (**5 GiB par
  defaut**) sont rejetes. `upload_file` verifie cela cote client et retourne
  une erreur claire avant d envoyer quoi que ce soit.
- Un fichier vide est un cas valide et courant : il ne demande qu un seul
  message (metadonnees seules, `chunk` vide) et aucun message de donnees.
  `upload_file` gere cela pour vous.
- Le fichier ne devient visible (dans `list_files`, `stat_file`,
  `download_file`) qu une fois tout le flux recu et valide. Un flux
  interrompu laisse des chunks orphelins qu un GC en arriere-plan finit par
  ramasser ; le fichier partiel n apparait jamais nulle part.

```rust
use rocia_db_sdk::file::FileUploadOptions;

// options.checksum: None -> upload_file computes SHA-256 of the buffer for
// you and chunks it into exactly-1-MiB messages. You do not need to touch
// checksum or chunking yourself for the common in-memory case.
client
    .upload_file(
        "tenant-1",
        "assets",
        "manual.txt",
        b"hello RociaDB",
        FileUploadOptions {
            content_type: "text/plain".into(),
            ..Default::default()
        },
    )
    .await?;

let metadata = client.stat_file("tenant-1", "assets", "manual.txt").await?;
let bytes = client.download_file("tenant-1", "assets", "manual.txt").await?;
client.delete_file("tenant-1", "assets", "manual.txt").await?;
```

EN: For large downloads, prefer `download_file_stream` to avoid buffering
the complete file in memory. Use `upload_file_stream` only for genuine
streaming uploads (data that never fits in memory); it is a low-level
escape hatch that does **not** rechunk or compute a checksum for you — you
are fully responsible for the wire contract above, and any deviation either
fails the upload outright or silently corrupts a later download.
FR: Pour les gros downloads, preferez `download_file_stream` pour eviter de
bufferiser tout le fichier en memoire. N utilisez `upload_file_stream` que
pour des uploads vraiment en streaming (donnees qui ne tiennent jamais en
memoire) ; c est une echappatoire bas niveau qui ne re-decoupe **ni** ne
calcule de checksum pour vous — vous etes entierement responsable du contrat
ci-dessus, et tout ecart fait soit echouer l upload directement, soit
corrompt silencieusement un download ulterieur.

## Pagination

EN: Every listing RPC (`ListDoc`, `ListCollections`, `ListGraphs`,
`ListNodes`, `NeighborsOut`/`NeighborsIn`, `ListBuckets`, `ListFiles`,
`ListTenants`, and `FindByField`/`QueryDoc`) shares the same rules:
FR: Chaque RPC de listing (`ListDoc`, `ListCollections`, `ListGraphs`,
`ListNodes`, `NeighborsOut`/`NeighborsIn`, `ListBuckets`, `ListFiles`,
`ListTenants`, et `FindByField`/`QueryDoc`) partage les memes regles :

- `limit == 0` is rejected client-side, immediately, with a clear error —
  no round trip to the server (`page limit must be greater than zero`).
- The server enforces its own ceiling, `limits.max_page_size`, **200 by
  default but configurable per deployment**. The SDK does *not* duplicate
  this ceiling client-side: any `limit >= 1` is forwarded unchanged, and the
  server has the final say (rejecting anything above its configured ceiling
  with `INVALID_ARGUMENT`).
- When `limit` is `None`, the SDK sends **20** — note this is the SDK's own
  default, not the server's own default of 50 when no `PageRequest` is sent
  at all; the SDK always sends an explicit `PageRequest`.
- **`next_cursor` empty (`None` in this SDK's `Page`/`NeighborPage` types) is
  the only end-of-list signal.** A page can legitimately be short, or even
  completely empty, in the middle of a listing (for example, an index entry
  surviving a document or node that was since deleted) while still carrying
  a fresh `next_cursor` — do not stop just because a page had few or no
  items; only stop when `next_cursor` comes back absent.
- One caveat in the other direction: when the total count is an exact
  multiple of `limit`, the last full page still carries a cursor, and the
  next call returns an empty page with no cursor. This is expected, not a
  bug — the server has no way to know it just handed out the last item.
- Cursors are opaque: never construct or parse one, only pass back what the
  server gave you. Do not persist a cursor across sessions; it is only
  meant to live for the duration of one pagination pass.

```rust
let mut cursor: Option<String> = None;
loop {
    let page = client.list_tenants(Some(100), cursor.as_deref()).await?;
    for tenant_id in &page.items {
        println!("tenant = {tenant_id}");
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
```

## Discovery

EN: Every listing method returns a `Page<T>` with an opaque `next_cursor` to
feed back unchanged, following the rules in [Pagination](#pagination).
FR: Chaque methode de listing retourne un `Page<T>` avec un `next_cursor`
opaque a reutiliser tel quel, en suivant les regles de
[Pagination](#pagination).

```rust
let collections = client.list_collections("tenant-1", Some(50), None).await?;
for info in &collections.items {
    println!("{} holds {} documents", info.collection, info.count);
}

let graphs = client.list_graphs("tenant-1", None, None).await?;
let nodes = client.list_nodes("tenant-1", "products", Some(100), None).await?;
let buckets = client.list_buckets("tenant-1", None, None).await?;
let files = client.list_files("tenant-1", "assets", Some(100), None).await?;
```

EN: `list_collections`'s `count` on each `CollectionInfo` is a maintained
counter, free to read regardless of collection size — the natural starting
point for a dashboard, since it gives both structure and volume in one call.
FR: Le `count` de chaque `CollectionInfo` renvoye par `list_collections` est
un compteur maintenu, gratuit a lire quelle que soit la taille de la
collection — le point de depart naturel d un dashboard, car il donne a la
fois la structure et le volume en un seul appel.

EN: `list_tenants` is the only method not scoped to a tenant: it enumerates the
whole deployment from a dedicated service, so expect `PERMISSION_DENIED` when
the credentials are limited to a single tenant.
FR: `list_tenants` est la seule methode non scopee a un tenant: elle enumere
tout le deploiement depuis un service dedie, donc attends-toi a
`PERMISSION_DENIED` si les credentials sont limites a un seul tenant.

## Document Queries and Business Rules

EN: A few server-side validations are easy to trip over and are not
re-checked client-side (except where noted), so they surface as
`INVALID_ARGUMENT` or `NOT_FOUND` from the RPC itself:
FR: Quelques validations cote serveur sont faciles a declencher par erreur
et ne sont pas reverifiees cote client (sauf mention contraire), donc elles
remontent en `INVALID_ARGUMENT` ou `NOT_FOUND` depuis le RPC lui-meme :

| RPC / method | Rule |
|---|---|
| `search_documents` (`FindByField`) | `value` must serialize to a JSON **scalar** (a string, number, bool, or null) — `"active"`, `42`, `true`, `null`. An object or array is rejected with `INVALID_ARGUMENT`. |
| `put_node`/`put_nodes` (`PutNode`) | `value` must serialize to a JSON **object**, never a scalar or an array. |
| `add_edge`/`add_edges` (`AddEdge`) | Fails with `NOT_FOUND` if either `from` or `to` does not already exist as a node in the graph. Create both endpoint nodes first. |
| `delete_edge`/`delete_edge_with_request_id` (`DeleteEdge`) | Fails with `NOT_FOUND` if the edge does not exist — **unlike** `delete_document`/`delete_file`, which are idempotent and succeed even when nothing was there to delete. |
| `query_documents` (`QueryDoc`, `Contains` operator) | Case-insensitive substring match, but a `Contains` term shorter than **3 characters is not indexable**. A query where *no* filter is indexable is refused with `INVALID_ARGUMENT` rather than served by a full scan — pair a short term with an `Eq` or `In` filter on another field. |
| `list_documents` (`ListDoc`) vs `query_documents` (`QueryDoc`) | The `u64` total count returned alongside the results is **free** on `list_documents` (a maintained counter) but **costly** on `query_documents` (computed after full filtering). Prefer `list_documents` when there is nothing to filter, and never call `query_documents` in a loop just to get a count. |
| `create_document`/`put_document`, `put_node`, `add_edge` (`json` payload) | The encoded JSON payload must not exceed the server's `limits.max_doc_bytes`, **2 MiB by default**. Larger payloads are rejected with `INVALID_ARGUMENT`. |

EN: Filters passed to `query_documents` combine with logical AND; results are
always tie-broken by document id, so ordering is total and stable across
pages.
FR: Les filtres passes a `query_documents` se combinent avec un ET logique ;
les resultats sont toujours departages par l id du document, donc l ordre
est total et stable entre les pages.

## Tenancy and Authorization Scopes

EN: **`tenant_id` is a business-level partition, not a security boundary.**
It is not derived from any caller identity — any authenticated client can
address any tenant. Enforcing which caller may touch which tenant is the
calling application's responsibility, not the server's.
FR: **`tenant_id` est une partition metier, pas une frontiere de securite.**
Il n est derive d aucune identite d appelant — n importe quel client
authentifie peut adresser n importe quel tenant. Faire respecter quel
appelant a le droit de toucher quel tenant est la responsabilite de
l application appelante, pas du serveur.

EN: Two token scopes matter for this SDK:
FR: Deux scopes de token comptent pour ce SDK :

- **read-only** — gets `PERMISSION_DENIED` on the 7 write RPCs:
  `create_document`/`put_document`, `delete_document`, `put_node`/`put_nodes`,
  `add_edge`/`add_edges`, `delete_edge`, `upload_file`/`upload_file_stream`,
  and `delete_file`. Every read method (documents, graph, files, tenants)
  works normally, which is enough to build a full read-only exploration
  console.
- **admin** — the scope used to create and rotate `rocia-idp` service
  accounts through its own account-management API — is refused on **all 22
  RPCs**, reads included. It has no business talking to the data plane: an
  admin-scoped token calling any method on `RociaDbClient` gets
  `PERMISSION_DENIED`. If a *read* call unexpectedly returns
  `PERMISSION_DENIED`, this is almost always the cause: double-check which
  `client_id` produced the token, since the data account and the admin
  account are two distinct credentials.

## API Conventions

EN: Document reads deserialize directly to the requested type. For graph reads, use
`get_node_as::<T>`, `get_outgoing_neighbor_nodes::<T>`, or
`get_incoming_neighbor_nodes::<T>` for the same behavior. `get_node` remains a
convenience method returning `serde_json::Value`.
FR: Les lectures de documents se deserialisent directement vers le type
demande. Pour les lectures graph, utilisez `get_node_as::<T>`,
`get_outgoing_neighbor_nodes::<T>` ou `get_incoming_neighbor_nodes::<T>` pour
le meme comportement. `get_node` reste une methode pratique retournant
`serde_json::Value`.

EN: Mutating helpers generate a unique `request_id` by default. Use the corresponding
`*_with_request_id` method when retries must reuse a stable idempotency key. Batch
operations generate one key per item, via `NodeInput::request_id` /
`EdgeInput::request_id` — `None` to auto-generate, `Some(..)` to control it
yourself.
FR: Les helpers de mutation generent un `request_id` unique par defaut.
Utilisez la methode `*_with_request_id` correspondante quand les retries
doivent reutiliser une cle d idempotence stable. Les operations batch
generent une cle par element, via `NodeInput::request_id` /
`EdgeInput::request_id` — `None` pour generer automatiquement, `Some(..)`
pour la controler vous-meme.

EN: Every listing method returns a named struct, never a bare tuple:
[`Page<T>`](#pagination) (`items`, `next_cursor`) when there is no total to
report, and [`DocumentPage<T>`](#pagination) (`items`, `next_cursor`,
`total_count`) for the three document-query methods that also report how
many results matched overall — `search_documents`, `list_documents`, and
`query_documents`. The one exception is naming, not shape: `neighbors_out`/
`neighbors_in` return `NeighborPage`, the same `next_cursor`-terminated page
but with the field named `neighbors` instead of `items`, since it carries
raw `Neighbor` records rather than a generic `T` — see
[Neighbors](#neighbors).
FR: Chaque methode de listing retourne une struct nommee, jamais un tuple
nu : [`Page<T>`](#pagination) (`items`, `next_cursor`) quand il n y a pas de
total a rapporter, et [`DocumentPage<T>`](#pagination) (`items`,
`next_cursor`, `total_count`) pour les trois methodes de requete document
qui rapportent aussi le nombre total de resultats correspondants —
`search_documents`, `list_documents`, et `query_documents`. La seule
exception est dans le nom, pas dans la forme : `neighbors_out`/
`neighbors_in` retournent `NeighborPage`, la meme page terminee par
`next_cursor` mais avec le champ nomme `neighbors` au lieu d `items`,
puisqu elle porte des `Neighbor` bruts plutot qu un `T` generique — voir
[Neighbors](#neighbors).

EN: Idempotency is scoped to `(tenant, operation, request_id)`: the same
`request_id` reused across two *different* operations (a `put_document`
then a `delete_document`, say) does not cancel or dedupe against each
other. Idempotency markers expire after the server's `gc.request_ttl_secs`,
**24 hours by default** — a replay after that window re-executes.
FR: L idempotence est scopee a `(tenant, operation, request_id)` : le meme
`request_id` reutilise sur deux operations *differentes* (un `put_document`
puis un `delete_document`, par exemple) ne s annulent pas ni ne se
dedupliquent entre eux. Les marqueurs d idempotence expirent apres
`gc.request_ttl_secs` cote serveur, **24 heures par defaut** — un rejeu
au-dela de cette fenetre est reexecute.

EN: Public methods return `rocia_db_sdk::Result<T>`, an alias for
`std::result::Result<T, RociaDbError>`. `RociaDbError` is a typed enum,
not a boxed `dyn Error`, so callers can `match` on the failure kind
directly instead of downcasting:
FR: Les methodes publiques retournent `rocia_db_sdk::Result<T>`, un alias
pour `std::result::Result<T, RociaDbError>`. `RociaDbError` est une enum
typee, pas un `dyn Error` boxe, donc les appelants peuvent faire un `match`
direct sur le type d echec plutot que de downcaster :

- `Status { operation, status }` — a gRPC call to the upstream server
  returned a non-OK status; carries the raw `tonic::Status`, so nothing is
  lost compared to calling the generated client directly. `.code()`
  returns the `tonic::Code`; `.reason()` returns the server's `reason`
  trailing-metadata value (`invalid_argument`, `not_found`,
  `already_exists`, `permission_denied`, `unauthenticated`, `internal`) —
  finer-grained than the code alone; `.status()` returns the raw
  `tonic::Status` for anything not covered by the two accessors above.
  `.is_unauthenticated()` and `.is_permission_denied()` are shorthands for
  the two codes that matter most for retry logic (see
  [`UNAUTHENTICATED` vs `PERMISSION_DENIED`](#unauthenticated-vs-permission_denied)).
- `Connection { .. }` — failed to connect to, or configure, the upstream
  endpoint (invalid host, TLS setup, connection refused, missing builder
  configuration).
- `Auth { .. }` — failed to obtain or refresh the upstream auth token.
- `Encode { context, .. }` / `Decode { context, .. }` — failed to encode a
  value as JSON before sending it upstream, or failed to decode a JSON
  payload received from upstream; wraps the underlying `serde_json::Error`.
- `Validation(String)` — a client-side rule was violated before any
  network call (a zero page limit, a checksum of the wrong length, an
  incomplete `node_label`/`node_graph` pair, a file size out of bounds,
  etc).

EN: Every variant implements `std::error::Error`, so `RociaDbError` composes
normally with `?` inside a function returning `anyhow::Result<...>` (or any
other error type with a blanket `From<E: std::error::Error>`), the same way
`anyhow::Error` did before. Only code that specifically downcast to
`tonic::Status` needs to change — see [Migrating to 0.4.0](#migrating-to-040).
FR: Chaque variante implemente `std::error::Error`, donc `RociaDbError` se
compose normalement avec `?` dans une fonction qui retourne un
`anyhow::Result<...>` (ou tout autre type d erreur avec un `From<E:
std::error::Error>` generique), de la meme facon que `anyhow::Error`
avant. Seul le code qui downcastait specifiquement vers `tonic::Status`
doit changer — voir [Migrating to 0.4.0](#migrating-to-040).

EN: The generated protobuf/gRPC types live in the `pb` module, but that
module is **not** part of the SDK's semver contract — a routine prost or
tonic upgrade can reshape it without the SDK's own API changing at all.
The handful of generated types that do appear in a public method signature
are re-exported individually at the crate root instead: `CollectionInfo`,
`StatResponse`, `Neighbor`, `UploadRequest`, `DownloadResponse`. Depend on
those re-exports, not on paths reaching into `pb` directly.
FR: Les types protobuf/gRPC generes vivent dans le module `pb`, mais ce
module ne fait **pas** partie du contrat semver du SDK — une montee de
version courante de prost ou tonic peut le remanier sans que l API du SDK
elle-meme ne change. La poignee de types generes qui apparaissent bien
dans une signature de methode publique sont reexportes individuellement a
la racine du crate a la place : `CollectionInfo`, `StatResponse`,
`Neighbor`, `UploadRequest`, `DownloadResponse`. Dependez de ces
reexports, pas de chemins qui plongent directement dans `pb`.

## Migrating to 0.5.0

EN: 0.5.0 unifies three call shapes in the public API — the document-query
return type, the graph batch input type, and what a node id means inside
that batch — with **no change to any observable server behavior**: the
number, order, and content of every RPC sent to the server are identical to
0.4.0. Four changes to audit call sites for:
FR: La 0.5.0 uniformise trois formes d appel de l API publique — le type de
retour des requetes document, le type d entree des batchs graph, et ce que
signifie un node id a l interieur de ce batch — **sans aucun changement de
comportement observable cote serveur** : le nombre, l ordre et le contenu
de chaque RPC envoye au serveur sont identiques a la 0.4.0. Quatre
changements a auditer dans vos points d appel :

1. **`search_documents`, `list_documents`, and `query_documents` now return
   [`DocumentPage<T>`](#pagination) instead of the anonymous
   `(Vec<T>, Option<String>, u64)` tuple.** The same three values are now
   named fields — `items`, `next_cursor`, `total_count` — consistent with
   [`Page<T>`](#pagination), which already had `items`/`next_cursor`.
   **Migration:** replace positional tuple destructuring with field access.

   ```rust
   // Before (0.4.0):
   let (docs, next_cursor, total) = client
       .list_documents::<serde_json::Value>("tenant-1", "products", Some(50), None)
       .await?;
   println!("{} of {total}, next={:?}", docs.len(), next_cursor);

   // After (0.5.0):
   let page = client
       .list_documents::<serde_json::Value>("tenant-1", "products", Some(50), None)
       .await?;
   println!("{} of {}, next={:?}", page.items.len(), page.total_count, page.next_cursor);
   ```

   FR: **`search_documents`, `list_documents`, et `query_documents`
   retournent desormais [`DocumentPage<T>`](#pagination) au lieu du tuple
   anonyme `(Vec<T>, Option<String>, u64)`.** Les trois memes valeurs sont
   desormais des champs nommes — `items`, `next_cursor`, `total_count` —
   coherents avec [`Page<T>`](#pagination), qui avait deja `items`/
   `next_cursor`. **Migration :** remplacez la destructuration positionnelle
   du tuple par un acces aux champs.

2. **`put_nodes` and `add_edges` now take an ordered sequence of
   `NodeInput`/`EdgeInput` structs instead of a `HashMap`.** A
   `HashMap` silently collapsed two items sharing the same key into one and
   gave no ordering guarantee — a `HashMap`'s iteration order is
   unspecified; an ordered sequence (a `Vec` in the common case) does
   neither.
   **Migration:** build a `Vec<NodeInput>`/`Vec<EdgeInput>` instead of a
   `HashMap`, one entry per item — see also point 3 below for what changes
   inside each `NodeInput`.

   ```rust
   use serde_json::json;

   // Before (0.4.0):
   use std::collections::HashMap;
   let mut nodes = HashMap::new();
   nodes.insert(("product".to_string(), "sku-1".to_string()), json!({"sku": "sku-1"}));
   client.put_nodes("tenant-1", "products", nodes).await?;

   // After (0.5.0):
   use rocia_db_sdk::NodeInput;
   let nodes = vec![NodeInput {
       node_id: "product:sku-1".to_string(),
       value: json!({"sku": "sku-1"}),
       request_id: None,
   }];
   client.put_nodes("tenant-1", "products", nodes).await?;
   ```

   FR: **`put_nodes` et `add_edges` prennent desormais une sequence ordonnee
   de structs `NodeInput`/`EdgeInput` au lieu d une `HashMap`.** Une
   `HashMap` fusionnait silencieusement deux items partageant la meme cle en
   un seul et ne garantissait aucun ordre — l ordre d iteration d une
   `HashMap` n est pas specifie ; une sequence ordonnee (un `Vec` dans le
   cas courant) ne fait ni l un ni l autre. **Migration :**
   construisez un `Vec<NodeInput>`/`Vec<EdgeInput>` au lieu d une `HashMap`,
   une entree par item — voir aussi le point 3 ci-dessous pour ce qui
   change a l interieur de chaque `NodeInput`.

3. **The easiest change to miss: `node_id` is now the complete id, and the
   SDK no longer recomposes it from a `(label, id)` pair.** 0.4.0's
   `HashMap<(String, String), Value>` was keyed by `(label, id)`, and
   `put_nodes` built the wire `node_id` internally as
   `format!("{label}:{id}")`. `NodeInput::node_id` is that
   already-complete string — pass `"product:sku-1"` directly, not
   `("product", "sku-1")`. **This is the migration trap to watch for**:
   passing the bare id (`"sku-1"`) instead of the full `"product:sku-1"`
   still compiles (both are plain `String`s) and the call still succeeds —
   it just silently upserts the wrong node id, with nothing failing loudly
   at compile time or runtime to catch the mistake.
   **Migration:** grep call sites that used to build the `HashMap` key's
   first two elements and concatenate them yourself into `node_id`, exactly
   the way the SDK used to.

   ```rust
   // Before (0.4.0): the SDK built node_id = "product:sku-1" for you.
   nodes.insert(("product".to_string(), "sku-1".to_string()), value);

   // After (0.5.0): you build node_id yourself.
   NodeInput { node_id: "product:sku-1".to_string(), value, request_id: None }
   // NOT: NodeInput { node_id: "sku-1".to_string(), .. } <- compiles, wrong node.
   ```

   FR: **Le changement le plus facile a manquer : `node_id` est desormais
   l id complet, et le SDK ne le recompose plus a partir d un couple
   `(label, id)`.** La `HashMap<(String, String), Value>` de la 0.4.0 etait
   indexee par `(label, id)`, et `put_nodes` construisait le `node_id` sur
   le fil en interne via `format!("{label}:{id}")`.
   `NodeInput::node_id` est cette chaine deja complete — passez
   `"product:sku-1"` directement, pas `("product", "sku-1")`. **C est le
   piege de migration a surveiller** : passer l id brut (`"sku-1"`) au lieu
   de l id complet `"product:sku-1"` compile quand meme (les deux sont de
   simples `String`) et l appel reussit quand meme — il upserte simplement
   le mauvais node id en silence, sans rien qui echoue bruyamment a la
   compilation ou a l execution pour attraper l erreur. **Migration :**
   grep vos points d appel qui construisaient les deux premiers elements de
   la cle `HashMap` et concatenez-les vous-meme dans `node_id`, exactement
   comme le SDK le faisait avant.

4. **`request_id` is now a field you set on every batch item, not something
   the SDK only ever generated for you internally — a real idempotency gain
   for retrying a partially-failed batch.** `put_nodes`/`add_edges` are
   **not atomic**: they stop at the first error, in-flight requests are
   cancelled, and the error does not say which items had already landed
   (see [Batch Operations](#batch-operations)). Before 0.5.0 there was no
   way to name each item's `request_id`, so a naive retry after a timeout
   had no way to avoid re-issuing every item from scratch. Set
   `request_id` explicitly and reuse the same value on retry: the server
   deduplicates on `(tenant, operation, request_id)`, so a replay
   recognizes and skips already-applied items, and only performs the
   writes that never landed.
   **Migration:** nothing is required to keep existing behavior — omitting
   `request_id` (`None`) reproduces the pre-0.5.0 auto-generated key. Set it
   explicitly wherever a batch might need to be replayed.

   ```rust
   use rocia_db_sdk::NodeInput;
   use serde_json::json;

   let nodes = vec![
       NodeInput {
           node_id: "product:sku-1".to_string(),
           value: json!({"sku": "sku-1"}),
           request_id: Some("batch-42:sku-1".to_string()),
       },
       NodeInput {
           node_id: "product:sku-2".to_string(),
           value: json!({"sku": "sku-2"}),
           request_id: Some("batch-42:sku-2".to_string()),
       },
   ];

   // First attempt times out partway through the batch.
   if client.put_nodes("tenant-1", "products", nodes.clone()).await.is_err() {
       // Retry with the exact same request_id values: nodes the server
       // already applied are recognized and skipped, not reapplied — only
       // the ones that never landed actually execute this time.
       client.put_nodes("tenant-1", "products", nodes).await?;
   }
   ```

   FR: **`request_id` est desormais un champ que vous fixez sur chaque item
   du batch, pas quelque chose que le SDK generait seulement en interne —
   un vrai gain d idempotence pour rejouer un batch partiellement echoue.**
   `put_nodes`/`add_edges` ne sont **pas atomiques** : ils s arretent a la
   premiere erreur, les requetes en vol sont annulees, et l erreur ne dit
   pas quels items avaient deja atterri (voir
   [Batch Operations](#batch-operations)). Avant la 0.5.0, il n y avait
   aucun moyen de nommer le `request_id` de chaque item, donc un retry
   naif apres un timeout n avait aucun moyen d eviter de reemettre chaque
   item depuis zero. Fixez `request_id` explicitement et reutilisez la
   meme valeur lors d un retry : le serveur deduplique sur `(tenant,
   operation, request_id)`, donc un rejeu reconnait et ignore les items
   deja appliques, et n execute que les ecritures qui n avaient pas
   abouti.
   **Migration :** rien n est requis pour conserver le comportement
   precedent — omettre `request_id` (`None`) reproduit la cle
   auto-generee d avant la 0.5.0. Fixez-le explicitement partout ou un
   batch peut avoir besoin d etre rejoue.

## Migrating to 0.4.0

EN: 0.4.0 reshapes the public API surface itself — error type, method
receiver, and the removal of deprecated aliases — with **no change to any
observable server behavior**: the wire contract and every validation rule
are exactly as in 0.3.0. Three changes to audit call sites for:
FR: La 0.4.0 remanie la surface de l API publique elle-meme — type d
erreur, receveur des methodes, retrait des alias depreciees — **sans
aucun changement de comportement observable cote serveur** : le contrat
reseau et chaque regle de validation sont exactement ceux de la 0.3.0.
Trois changements a auditer dans vos points d appel :

1. **Public methods now return `rocia_db_sdk::Result<T>` (an alias for
   `std::result::Result<T, RociaDbError>`) instead of `anyhow::Result<T>`.**
   `RociaDbError` is a typed enum — see [API Conventions](#api-conventions)
   above for its variants and accessors. **Migration:** this is the one
   change that genuinely needs code edits, and only if you inspected
   errors with `error.downcast_ref::<tonic::Status>()`; replace that with
   `error.status()`, `error.code()`, `error.reason()`, or the
   `is_unauthenticated()`/`is_permission_denied()` shorthands — see the
   rewritten example in
   [`UNAUTHENTICATED` vs `PERMISSION_DENIED`](#unauthenticated-vs-permission_denied).
   If instead your code only ever propagated SDK errors upward with `?`
   (for example inside a function returning `anyhow::Result<...>`),
   nothing needs to change: `RociaDbError` implements `std::error::Error`,
   so it converts the same way `anyhow::Error`'s sources always did.
2. **Every `RociaDbClient` method now takes `&self` instead of `&mut
   self`.** **Migration:** in practice this does not break existing call
   sites — Rust resolves a `&self` method the same way through a `&mut`
   binding or a plain value. The only symptom you may see is an
   `unused_mut` warning on a `let mut client = ...` that is no longer
   needed; drop the `mut`, or ignore the warning if your lints do not
   treat it as an error. This is also what makes a `RociaDbClient` shared
   behind an `Arc` safe to use concurrently without a `Mutex`: every
   method clones the cheap, `Arc`-backed inner service client before
   issuing its RPC, the same way the batch helpers (`put_nodes`,
   `add_edges`) always have.
3. **The `node_upsert`, `edges_upsert`, and `get_neighbors_nodes`
   compatibility aliases (deprecated since 0.2.0) have been removed.**
   **Migration:** replace `node_upsert` with `put_nodes`, `edges_upsert`
   with `add_edges`, and `get_neighbors_nodes` with
   `get_outgoing_neighbor_nodes` (or `get_incoming_neighbor_nodes`) for
   typed payloads via `get_outgoing_neighbor_nodes::<T>` — see
   [API Conventions](#api-conventions).

EN: None of the three changes above touch the wire contract: a 0.4.0
client and a 0.3.0 client observe the exact same server behavior for the
exact same calls.
FR: Aucun des trois changements ci-dessus ne touche au contrat reseau : un
client 0.4.0 et un client 0.3.0 observent exactement le meme comportement
serveur pour les memes appels.

## Migrating to 0.3.0

EN: 0.3.0 fixes three behavior bugs against the real server contract.
None of them change a public method's signature, but they change what
succeeds, what fails, and what data comes back — audit call sites
accordingly:
FR: La 0.3.0 corrige trois bugs de comportement par rapport au vrai contrat
serveur. Aucun ne change la signature d une methode publique, mais ils
changent ce qui reussit, ce qui echoue, et les donnees qui reviennent —
auditez vos points d appel en consequence :

1. **File uploads now match the server's on-disk chunking exactly, and send
   a real checksum.** Pre-0.3.0, `upload_file` sliced buffers into 64 KiB
   chunks and sent an empty `checksum`. The server always required exactly
   32 checksum bytes, so those uploads either failed outright, or — worse,
   for any file needing more than one 1 MiB server-side chunk — appeared to
   succeed while silently setting up a later `download_file` to return
   truncated or garbled data, because the server always replays
   `ceil(size_bytes / 1 MiB)` fixed-size chunk indexes regardless of how the
   upload was actually sliced. 0.3.0 always emits exactly-1-MiB chunks and
   computes a SHA-256 checksum automatically. **Migration:** no code change
   is required to adopt the fix — `FileUploadOptions::default()` already
   does the right thing. If you cannot be sure a file uploaded by a
   pre-0.3.0 client downloads correctly, re-upload it; there is no way to
   detect the corruption after the fact from the client side. If you were
   passing your own `checksum`, it must now be exactly 32 bytes or the call
   fails before any network request.
2. **`create_document` now rejects a partial graph binding instead of
   silently ignoring it.** Passing only one of `node_label`/`node_graph`
   (not both `Some` or both `None`) now returns an `Err` before any network
   call, where it previously left the caller believing a graph node had
   been created when none was. **Migration:** grep call sites for
   `create_document` and make sure both arguments are always provided
   together or both omitted.
3. **Full neighbor listings (`get_outgoing_neighbor_nodes`,
   `get_incoming_neighbor_nodes`, and the deprecated `get_neighbors_nodes`)
   no longer stop early on an empty or short page.** They previously
   stopped as soon as one page came back empty or shorter than the
   requested limit; since the server can legitimately return a short or
   empty page mid-listing (a stale index entry pointing at a deleted node,
   for example) followed by more data, this silently dropped every
   remaining page. **Migration:** if your code compensated for the
   truncation (extra calls, assumptions about result size), that
   workaround is no longer needed and results will now be larger/more
   complete than before.

EN: `create_document`'s document-then-node write remains **not atomic**: if
the node write fails after the document write succeeds, the document is
left without its graph binding. This was true before 0.3.0 too and has not
changed.
FR: L ecriture document-puis-node de `create_document` reste **non
atomique** : si l ecriture du node echoue apres que celle du document a
reussi, le document reste sans son binding graph. C etait deja vrai avant
la 0.3.0 et cela n a pas change.

## Development

```bash
PROTOC=/usr/bin/protoc cargo build
PROTOC=/usr/bin/protoc cargo fmt --all -- --check
PROTOC=/usr/bin/protoc cargo clippy --all-targets --all-features -- -D warnings
PROTOC=/usr/bin/protoc cargo test
```

EN: `PROTOC` only needs to point at a real `protoc` binary; `mise install`
(see `mise.toml`) installs a pinned one for you at
`~/.local/share/mise/installs/protoc/35.0/bin/protoc`, in which case the
`PROTOC=` prefix above is not needed.
FR: `PROTOC` doit seulement pointer vers un binaire `protoc` reel ;
`mise install` (voir `mise.toml`) en installe un pour vous a
`~/.local/share/mise/installs/protoc/35.0/bin/protoc`, auquel cas le prefixe
`PROTOC=` ci-dessus n est pas necessaire.
