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
# rocia-db-sdk = { git = "https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust", tag = "v0.6.0" }
```

EN: `Cargo.toml` declares `rust-version = "1.85"` (the first stable release
supporting `edition = "2024"`) — the minimum toolchain this crate is built
and tested against, mirroring the TypeScript SDK's `engines.node: ">=20"`.
FR: `Cargo.toml` declare `rust-version = "1.85"` (la premiere version stable
supportant `edition = "2024"`) — le toolchain minimum avec lequel cette
crate est construite et testee, en miroir du `engines.node: ">=20"` du SDK
TypeScript.

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

### Two ways to recover from `UNAUTHENTICATED`

EN: `refresh_auth_token()` (above) is **eager**: it awaits the round trip to
the identity provider and only returns once a fresh token is confirmed and
in hand, or propagates the fetch error otherwise — the right choice right
before retrying the call that just failed. `invalidate_auth_token()` is its
**lazy** counterpart: it is synchronous, returns immediately, and only wakes
the background refresh task so it fetches a fresh token at its next
opportunity — nobody pays for the network round trip inline. Reach for it
when you just want to mark the cached token stale (a fire-and-forget error
handler, for example) without blocking the current call on a fresh token
being in hand first:
FR: `refresh_auth_token()` (ci-dessus) est **eager** (avide) : il attend
l aller-retour vers le fournisseur d identite et ne retourne qu une fois un
nouveau token confirme et en main, ou propage l erreur de recuperation
sinon — le bon choix juste avant de rejouer l appel qui vient d echouer.
`invalidate_auth_token()` en est le pendant **paresseux** : il est
synchrone, retourne immediatement, et se contente de reveiller la tache de
refresh en arriere-plan pour qu elle recupere un token frais a la prochaine
occasion — personne ne paie l aller-retour reseau en ligne. Utilisez-le
quand vous voulez juste marquer le token en cache comme perime (un
gestionnaire d erreur fire-and-forget, par exemple) sans bloquer l appel en
cours sur un token frais deja en main :

```rust
// Somewhere that observed an UNAUTHENTICATED but is not the caller that
// needs to retry immediately (a background health check, for example):
// signal staleness and move on — no `.await`, no network call here.
client.invalidate_auth_token();
```

EN: Both are no-ops when the client was built with `RociaDbBuilder::disable_auth`.
Neither ever discards a still-valid cached token just because a background
refresh attempt failed: the interceptor keeps injecting the last known-good
token until a replacement is confirmed.
FR: Les deux sont des no-ops quand le client a ete construit avec
`RociaDbBuilder::disable_auth`. Aucun des deux ne jette jamais un token en
cache encore valide simplement parce qu une tentative de refresh en
arriere-plan a echoue : l interceptor continue d injecter le dernier token
connu comme bon jusqu a ce qu un remplacant soit confirme.

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

### Host validation and connect timeout

EN: `.host(...)` must be a bare `scheme://host:port` — no path component
beyond an absent one or a lone `/`. `RociaDbBuilder::build` rejects anything
else (`RociaDbError::Connection`) before attempting a connection, so a
mistyped host with a leftover path (`http://127.0.0.1:50051/v1`, pasted from
somewhere else) fails loudly instead of tonic silently ignoring the path
component and dialing the host anyway.
FR: `.host(...)` doit etre un simple `scheme://host:port` — sans composante
de chemin au-dela d une absente ou d un simple `/`. `RociaDbBuilder::build`
rejette tout le reste (`RociaDbError::Connection`) avant meme de tenter une
connexion, de sorte qu un host mal saisi avec un chemin residuel
(`http://127.0.0.1:50051/v1`, colle depuis ailleurs) echoue bruyamment
plutot que tonic n ignore silencieusement la composante chemin et ne
compose quand meme le host.

EN: `RociaDbBuilder::connect_timeout` sets the deadline applied while
connecting. It defaults to **10 seconds** if never called — `build()`
always applies some connect timeout, so a slow or unreachable DNS/TCP
target fails after a bounded wait instead of hanging `.await` forever.
FR: `RociaDbBuilder::connect_timeout` definit le delai applique pendant la
connexion. Il vaut **10 secondes** par defaut si jamais appele — `build()`
applique toujours un delai de connexion, de sorte qu une cible DNS/TCP
lente ou injoignable echoue apres une attente bornee plutot que de bloquer
`.await` indefiniment.

```rust
use rocia_db_sdk::RociaDbBuilder;
use std::time::Duration;

let client = RociaDbBuilder::new()
    .host("https://rociadb.internal:50051")
    .connect_timeout(Duration::from_secs(3))
    .auth_client_credentials("https://example.com/token", "client-id", "client-secret")
    .build()
    .await?;
```

EN: A zero-duration timeout is rejected with `RociaDbError::Validation` at
`build()` time, before any connection attempt.
FR: Un delai de duree nulle est rejete avec `RociaDbError::Validation` au
moment du `build()`, avant toute tentative de connexion.

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
underlying gRPC contract they implement is worth understanding even if you
never touch `upload_file_chunked` or `upload_file_stream` directly. There
are three levels of upload help, from most to least hand-holding:
`upload_file` (buffers the whole file, computes the checksum for you),
`upload_file_chunked` (streams arbitrarily-sized pieces without buffering
the whole file, but you supply the checksum), and `upload_file_stream` (a
raw pass-through — you build every protobuf message yourself). All three
are covered below.
FR: `upload_file` et `download_file` sont des aides ergonomiques en memoire ;
le contrat gRPC sous-jacent qu elles implementent vaut la peine d etre
compris meme si vous ne touchez jamais directement a `upload_file_chunked`
ou `upload_file_stream`. Il y a trois niveaux d aide a l upload, du plus au
moins assiste : `upload_file` (bufferise tout le fichier, calcule le
checksum pour vous), `upload_file_chunked` (streame des morceaux de taille
quelconque sans bufferiser tout le fichier, mais vous fournissez le
checksum), et `upload_file_stream` (un passe-plat brut — vous construisez
chaque message protobuf vous-meme). Les trois sont couverts ci-dessous.

### The upload wire contract

EN:
- **Chunk size is the client's choice, capped at 1 MiB — it is not a fixed
  requirement.** As of server `1.0.0-rc.16`, the server stores each chunk
  verbatim at its position in the stream and, on download, reads chunks back
  until it has collected `size_bytes` bytes in total — it no longer assumes
  any particular chunk size when replaying them. A single message's `chunk`
  larger than 1 MiB is rejected outright with `INVALID_ARGUMENT`
  (`"chunk exceeds 1 MiB"`); anything at or under that cap is fine, sliced
  however the client likes. `upload_file` and `upload_file_chunked` (see
  below) both still always emit exactly-1-MiB chunks (the last one may be
  shorter) — not because the server requires it, but because 1 MiB is the
  largest message the server allows, so it is also the fewest possible
  messages for a given file; this is also why `FileUploadOptions` has no
  `chunk_size` knob. **Before `rc.16`**, this was a correctness requirement,
  not just an efficiency choice: the server derived how many chunks to read
  back on download as `ceil(size_bytes / 1 MiB)`, so any upload chunk size
  other than exactly 1 MiB made a later download silently return truncated
  or garbled data, with no server-side error at all. That is why the SDK has
  always defaulted to exactly-1-MiB chunking, and why it still does: this
  chunking remains correct and optimal against `rc.16`, and it is the only
  chunking that stays safe against a pre-`rc.16` server. The same
  guessed-chunk-count bug affected `Delete` before `rc.16` (it stopped after
  the same assumed chunk count, leaving a tail of orphaned chunks behind for
  any file that had used a different chunk size); `Delete` now removes a
  whole file by prefix, regardless of how it was chunked.
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
- The sum of every `chunk`'s bytes across the stream must equal `size_bytes`
  exactly, or the server rejects the upload with `INVALID_ARGUMENT`
  (`"size_bytes does not match uploaded data"`) at the end of the stream —
  this is what makes `size_bytes` a value the SDK (and the server, on
  download) can trust, rather than just a caller-supplied claim.
- Re-uploading an existing `file_id` **replaces it, with no error for the
  duplicate** — there is no separate delete-then-upload dance required. The
  content served by `download_file`/`stat_file` afterward is always the
  newest upload.
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
- **La taille de chunk est le choix du client, plafonnee a 1 MiB — ce n est
  pas une exigence fixe.** Depuis le serveur `1.0.0-rc.16`, le serveur
  stocke chaque chunk tel quel a sa position dans le flux et, au download,
  relit des chunks jusqu a avoir recueilli au total `size_bytes` octets — il
  ne suppose plus aucune taille de chunk particuliere en les relisant. Un
  message dont le `chunk` depasse 1 MiB est rejete directement avec
  `INVALID_ARGUMENT` (`"chunk exceeds 1 MiB"`) ; tout ce qui est a la limite
  ou en dessous convient, decoupe comme le client le souhaite. `upload_file`
  et `upload_file_chunked` (voir plus bas) emettent tous deux toujours des
  chunks d exactement 1 MiB (le dernier peut etre plus court) — non pas
  parce que le serveur l exige, mais parce que 1 MiB est le plus gros
  message que le serveur autorise, donc aussi le moins de messages possible
  pour un fichier donne ; c est aussi pourquoi `FileUploadOptions` n a pas de
  reglage `chunk_size`. **Avant `rc.16`**, c etait une exigence de
  correction, pas seulement un choix d efficacite : le serveur deduisait le
  nombre de chunks a relire au download comme `ceil(size_bytes / 1 MiB)`,
  donc toute taille de chunk d upload differente de 1 MiB exactement faisait
  qu un download ulterieur renvoyait silencieusement des donnees tronquees
  ou corrompues, sans aucune erreur cote serveur. C est pourquoi le SDK a
  toujours decoupe par defaut en chunks d exactement 1 MiB, et pourquoi il
  continue de le faire : ce decoupage reste correct et optimal face a
  `rc.16`, et c est le seul decoupage qui reste sur face a un serveur
  pre-`rc.16`. Le meme bug de nombre de chunks devine affectait `Delete`
  avant `rc.16` (il s arretait apres le meme nombre de chunks suppose,
  laissant une queue de chunks orphelins pour tout fichier ayant utilise une
  autre taille de chunk) ; `Delete` supprime desormais un fichier entier par
  prefixe, quel que soit son decoupage.
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
- La somme des octets de `chunk` sur tout le flux doit egaler exactement
  `size_bytes`, sinon le serveur rejette l upload avec `INVALID_ARGUMENT`
  (`"size_bytes does not match uploaded data"`) a la fin du flux — c est ce
  qui fait de `size_bytes` une valeur a laquelle le SDK (et le serveur, au
  download) peuvent faire confiance, pas seulement une declaration de
  l appelant.
- Reuploader un `file_id` deja existant **le remplace, sans erreur pour le
  doublon** — aucune danse suppression-puis-upload n est necessaire. Le
  contenu servi par `download_file`/`stat_file` ensuite est toujours celui
  du dernier upload.
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

### Streaming an upload without buffering the whole file

EN: `upload_file_chunked` is the middle tier between `upload_file` (buffers
the whole file, computes the checksum for you) and `upload_file_stream`
(a raw pass-through with zero validation — see below). Give it a
`Stream<Item = Vec<u8>>` of arbitrarily-sized pieces — however the source
naturally produces them, a `64 KiB` `AsyncRead` wrapper, messages from
another stream, anything — and it re-buffers internally, always emitting
exactly-1-MiB gRPC messages to the server, the same chunking `upload_file`
produces from an in-memory buffer. `size_bytes` and `checksum` (the 32-byte
SHA-256 digest of the complete file) must both be supplied up front,
because file metadata travels on the very first gRPC message, before this
method has read a single byte from `chunks` — hash the source ahead of time
(a first pass over the file, for example) if you only have raw bytes to
start from. If `chunks` ends up producing more or fewer total bytes than
`size_bytes` declared, this fails client-side with
`RociaDbError::Validation` instead of sending a stream the server would
reject anyway at the end.
FR: `upload_file_chunked` est le palier intermediaire entre `upload_file`
(bufferise tout le fichier, calcule le checksum pour vous) et
`upload_file_stream` (un passe-plat brut sans aucune validation — voir plus
bas). Donnez-lui un `Stream<Item = Vec<u8>>` de morceaux de taille
quelconque — comme la source les produit naturellement, un wrapper
`AsyncRead` en `64 KiB`, des messages venant d un autre flux, n importe quoi
— et il re-bufferise en interne, en emettant toujours des messages gRPC
d exactement 1 MiB vers le serveur, le meme decoupage que `upload_file`
produit depuis un buffer en memoire. `size_bytes` et `checksum` (le digest
SHA-256 de 32 octets du fichier complet) doivent tous deux etre fournis a
l avance, car les metadonnees du fichier voyagent sur le tout premier
message gRPC, avant que cette methode n ait lu le moindre octet de `chunks`
— hachez la source a l avance (une premiere passe sur le fichier, par
exemple) si vous n avez que des octets bruts au depart. Si `chunks` finit
par produire plus ou moins d octets au total que ce que `size_bytes`
declarait, ceci echoue cote client avec `RociaDbError::Validation` plutot
que d envoyer un flux que le serveur rejetterait de toute facon a la fin.

```rust
use rocia_db_sdk::file::FileStreamUploadOptions;
use futures::stream;
use sha2::{Digest, Sha256};

let payload = std::fs::read("large-report.csv").expect("read source file");
let checksum = Sha256::digest(&payload).to_vec();
let size_bytes = payload.len() as u64;

// Any chunking the source naturally produces — 64 KiB here purely as an
// example. upload_file_chunked re-slices to the server's 1 MiB messages
// internally regardless of what you hand it.
let chunks: Vec<Vec<u8>> = payload.chunks(64 * 1024).map(|c| c.to_vec()).collect();

client
    .upload_file_chunked(
        "tenant-1",
        "reports",
        "large-report.csv",
        size_bytes,
        checksum,
        stream::iter(chunks),
        FileStreamUploadOptions {
            content_type: "text/csv".into(),
            ..Default::default()
        },
    )
    .await?;
```

EN: **Naming trap when porting code between SDKs:** despite doing the
re-chunking and validation, this method is not called `upload_file_stream`
— that name was already taken in this SDK by the raw, zero-validation
escape hatch below. The TypeScript SDK's `uploadFileStream` is this
method's counterpart, not Rust's `upload_file_stream`'s — see
[Parity with the TypeScript SDK](#parity-with-the-typescript-sdk) for the
full naming table.
FR: **Piege de nommage lors du portage de code entre SDKs :** malgre le
re-decoupage et la validation qu elle effectue, cette methode ne s appelle
pas `upload_file_stream` — ce nom etait deja pris dans ce SDK par
l echappatoire brute sans validation ci-dessous. `uploadFileStream` du SDK
TypeScript est l equivalent de cette methode-ci, pas de
`upload_file_stream` cote Rust — voir
[Parity with the TypeScript SDK](#parity-with-the-typescript-sdk) pour le
tableau de correspondance complet.

EN: For large downloads, prefer `download_file_stream` to avoid buffering
the complete file in memory. Use `upload_file_stream` only when you are
ready to build every protobuf message yourself and match the wire contract
above exactly; it is a low-level escape hatch that does **not** rechunk,
cap a chunk's size, or compute a checksum for you. Since `rc.16`, getting
the chunk *size* wrong here fails fast with `INVALID_ARGUMENT` rather than
silently corrupting a later download — but a wrong `size_bytes` total, or a
`checksum` that does not actually match the bytes (the server only checks
its length, never its content), can still slip through as an
upload that looks successful while carrying bad data. Prefer
`upload_file_chunked` above unless you specifically need to hand-build the
message stream.
FR: Pour les gros downloads, preferez `download_file_stream` pour eviter de
bufferiser tout le fichier en memoire. N utilisez `upload_file_stream` que
quand vous etes pret a construire chaque message protobuf vous-meme et a
respecter exactement le contrat sur le fil ci-dessus ; c est une
echappatoire bas niveau qui ne re-decoupe **ni** ne plafonne la taille d un
chunk **ni** ne calcule de checksum pour vous. Depuis `rc.16`, se tromper
sur la *taille* du chunk ici echoue rapidement avec `INVALID_ARGUMENT`
plutot que de corrompre silencieusement un download ulterieur — mais un
`size_bytes` total incorrect, ou un `checksum` qui ne correspond pas
reellement aux octets (le serveur ne verifie que sa longueur, jamais son
contenu), peut encore passer au travers sous la forme d un upload qui
semble reussi tout en portant des donnees erronees. Preferez
`upload_file_chunked` ci-dessus sauf besoin specifique de construire le
flux de messages a la main.

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

EN: `create_document_with_request_id` is that sibling for `create_document`:
`request_id` applies **only** to the document write (the `PutDoc` call) —
the graph node binding, when `node_label`/`node_graph` are both `Some`,
keeps generating its own key exactly as `create_document` already does.
Unlike `create_document` (which takes an already-serialized
`serde_json::Value`, to avoid a breaking signature change), this sibling is
generic over any `Serialize` type, consistent with
`put_document_with_request_id`/`put_node_with_request_id`/
`add_edge_with_request_id`:

```rust
use serde_json::json;

client
    .create_document_with_request_id(
        "tenant-1",
        "products",
        "sku-123",
        &json!({"sku": "sku-123", "label": "Widget"}),
        Some("product".to_string()),
        Some("products".to_string()),
        "reindex-job-42:sku-123",
    )
    .await?;
```

FR: `create_document_with_request_id` est ce sibling pour `create_document` :
`request_id` s applique **uniquement** a l ecriture du document (l appel
`PutDoc`) — le binding de node graph, quand `node_label`/`node_graph` sont
tous deux `Some`, continue de generer sa propre cle exactement comme le
fait deja `create_document`. Contrairement a `create_document` (qui prend
un `serde_json::Value` deja serialise, pour eviter un changement de
signature cassant), ce sibling est generique sur tout type `Serialize`,
coherent avec `put_document_with_request_id`/`put_node_with_request_id`/
`add_edge_with_request_id`.

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

## Parity with the TypeScript SDK

EN: This SDK and the TypeScript SDK
([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts))
cover the same 22 RPCs against the same server, and are maintained to the
same standard: **every capability available in one is available in the
other.** Neither imitates the other's syntax — this crate stays
snake_case/`Result`-idiomatic Rust, the TypeScript package stays
camelCase/exception-idiomatic TypeScript — but a piece of client code
should always have a mechanical translation from one SDK to the other.
Parity is about what you can *do*, not about matching method names
character for character, and most names do translate mechanically
(`put_nodes` ↔ `putNodes`, `get_outgoing_neighbor_nodes` ↔
`getOutgoingNeighborNodes`, and so on). The handful of places where a name
does **not** translate mechanically — where translating a call by ear
lands you on the wrong method — are the naming table below.
FR: Ce SDK et le SDK TypeScript
([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts))
couvrent les memes 22 RPC face au meme serveur, et sont maintenus au meme
niveau d exigence : **toute capacite disponible dans l un est disponible
dans l autre.** Aucun n imite la syntaxe de l autre — cette crate reste du
Rust idiomatique en snake_case/`Result`, le paquet TypeScript reste du
TypeScript idiomatique en camelCase/exceptions — mais un morceau de code
client doit toujours avoir une traduction mecanique d un SDK vers l autre.
La parite porte sur ce que vous pouvez *faire*, pas sur la correspondance
caractere pour caractere des noms de methode, et la plupart des noms se
traduisent bien mecaniquement (`put_nodes` ↔ `putNodes`,
`get_outgoing_neighbor_nodes` ↔ `getOutgoingNeighborNodes`, etc). Les
quelques endroits ou un nom ne se traduit **pas** mecaniquement — ou
traduire un appel a l oreille vous amene sur la mauvaise methode — sont
dans le tableau de correspondance ci-dessous.

| Capability / Capacite | Rust ([`rociadb-core-sdk-rust`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust)) | TypeScript ([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts)) | Note |
|---|---|---|---|
| Assisted streaming upload — re-chunks to the 1 MiB wire contract, validates the total, caller supplies the checksum | `upload_file_chunked` | `uploadFileStream` | Names do **not** correspond — see below. |
| Raw streaming upload — zero validation, caller builds every protobuf message | `upload_file_stream` | `uploadFileRaw` | Names do **not** correspond — the mirror image of the row above. |
| Idempotency key scoped to a `create_document` call's document write only (the graph node binding keeps its own auto-generated key) | `create_document_with_request_id` — a sibling method, `request_id: impl Into<String>` | `createDocument(..., { requestId })` — an options-object field | Same capability, different shape: a sibling method vs. an options field, the established pattern on each side. |
| Releasing the connection and the background token-refresh task | Drop the last live `RociaDbClient` clone | `client.close()` | No Rust method by design — see below. |
| Lazy token invalidation, at the level of the background refresh task itself (not the `RociaDbClient`-level wrapper, which *does* translate mechanically: `invalidate_auth_token` ↔ `invalidateToken`) | `TokenManager::request_refresh` | `TokenManager.invalidate()` | Different verb chosen independently on each side for the same "mark it stale, wake the background task, do not block" idea. |
| Standalone OAuth2 token fetch, usable outside of `TokenManager` | `auth::fetch_token` | `fetchOAuthToken` (exported from `auth.ts`, re-exported at the package root) | TypeScript needed a name that does not collide with the `fetch` Web API it wraps; Rust has no such collision. |
| Discriminating why an `Err` happened | `RociaDbError` — a `match`-able enum: `Status { .. }` / `Connection { .. }` / `Auth { .. }` / `Encode { .. }` / `Decode { .. }` / `Validation(String)` | `RociaDbError.kind: RociaDbErrorKind`, one class with a `"status" \| "connection" \| "auth" \| "encode" \| "decode" \| "validation"` field | Different shape, not just a different name — see below. |
| Escape hatch to the raw generated protobuf/gRPC types, to build a custom client against the same `.proto` | the `pb` module (`#[doc(hidden)] pub mod pb`; the handful of generated types that reach a public signature are re-exported individually at the crate root instead — `CollectionInfo`, `StatResponse`, `Neighbor`, `UploadRequest`, `DownloadResponse`) | the `rocia-db-sdk/proto` subpath export | Different mechanism, not just a different name: an in-crate module vs. a separate `package.json` `exports` entry. Neither is part of either package's semver contract. |

EN: **The error-kind trap, spelled out:** both sides recognize the exact
same six causes, in the same order, but represent the choice differently.
Rust's `RociaDbError` is a real sum type — matching on it is exhaustive,
and the compiler flags a missing arm. TypeScript keeps a single
`RociaDbError` class (so an existing `instanceof RociaDbError` check never
breaks) and puts the same six-way choice in a `kind` field instead —
narrowing on `error.kind` gets you the same exhaustiveness check from
`tsc`, just via a discriminated union instead of a variant match. Neither
representation is "the same code translated"; each is the idiomatic way to
express one closed set of causes in its own language.
FR: **Le piege du `kind` d erreur, explicite :** les deux cotes
reconnaissent exactement les six memes causes, dans le meme ordre, mais
representent ce choix differemment. Le `RociaDbError` de Rust est un vrai
sum type — le `match` dessus est exhaustif, et le compilateur signale un
bras manquant. TypeScript garde une seule classe `RociaDbError` (pour
qu un `instanceof RociaDbError` existant ne casse jamais) et met le meme
choix a six branches dans un champ `kind` a la place — un narrowing sur
`error.kind` donne la meme verification d exhaustivite de la part de
`tsc`, juste via une union discriminee plutot qu un match de variante.
Aucune des deux representations n est « le meme code traduit » ; chacune
est la maniere idiomatique d exprimer un meme ensemble ferme de causes
dans son propre langage.

EN: **The upload naming trap, spelled out:** `upload_file_chunked` (Rust)
and `uploadFileStream` (TypeScript) are the *same* capability — the middle
tier that re-chunks and validates for you (see
[Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file)).
`upload_file_stream` (Rust) and `uploadFileRaw` (TypeScript) are also the
*same* capability — the raw, zero-validation escape hatch. `upload_file_stream`
and `uploadFileStream` are **not** each other's counterpart, despite the
near-identical name: the Rust one is the raw escape hatch, the TypeScript
one is the validated middle tier. Porting upload code between the two SDKs
by matching names alone silently swaps which tier you land on.
FR: **Le piege de nommage de l upload, explicite :** `upload_file_chunked`
(Rust) et `uploadFileStream` (TypeScript) sont la *meme* capacite — le
palier intermediaire qui re-decoupe et valide pour vous (voir
[Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file)).
`upload_file_stream` (Rust) et `uploadFileRaw` (TypeScript) sont aussi la
*meme* capacite — l echappatoire brute sans validation.
`upload_file_stream` et `uploadFileStream` ne sont **pas** l equivalent
l un de l autre, malgre leur nom quasi identique : le Rust est
l echappatoire brute, le TypeScript est le palier intermediaire valide.
Porter du code d upload entre les deux SDKs en faisant correspondre les
noms seuls vous fait silencieusement changer de palier.

EN: **Why there is no Rust `close()`:** `RociaDbClient` is `Clone`, and
every clone shares one underlying channel and one background refresh task
by design (see [Authentication](#authentication)) — a `close(&self)` taking
`&self` would tear the channel down out from under every other live clone,
silently breaking that documented guarantee. The idiomatic Rust equivalent
already exists and gives the identical guarantee: drop the last clone.
`tonic::transport::Channel` is itself cheap to clone and shares one real
connection underneath, so this is not a weaker substitute — it is the same
guarantee, spelled the Rust way (RAII instead of an explicit call).
FR: **Pourquoi il n y a pas de `close()` en Rust :** `RociaDbClient` est
`Clone`, et chaque clone partage un meme channel sous-jacent et une meme
tache de refresh en arriere-plan par construction (voir
[Authentication](#authentication)) — un `close(&self)` prenant `&self`
couperait le channel sous les pieds de tous les autres clones vivants,
brisant silencieusement cette garantie deja documentee. L equivalent
idiomatique Rust existe deja et donne la meme garantie : laisser tomber le
dernier clone. `tonic::transport::Channel` est lui-meme peu couteux a
cloner et partage une seule connexion reelle en dessous, donc ce n est pas
un substitut plus faible — c est la meme garantie, exprimee a la maniere
Rust (RAII plutot qu un appel explicite).

EN: Two capabilities are intentionally kept on one side without a mirror on
the other: `ApiKeyInterceptor` (Rust only — it validates an *incoming* API
key, so it serves building a server or a test double, not talking to
RociaDB, which puts it out of scope for a client SDK), and having both
`RociaDbBuilder::build()` and a direct `RociaDbClient.connect()` entry
point (TypeScript only — the builder there is a thin wrapper with no
capability of its own, so duplicating a second entry point in Rust would
add an API to maintain for zero new capability).
FR: Deux capacites sont volontairement gardees d un seul cote sans miroir
de l autre : `ApiKeyInterceptor` (Rust uniquement — il valide une cle API
*entrante*, donc il sert a construire un serveur ou un test double, pas a
parler a RociaDB, ce qui le met hors du perimetre d un SDK client), et le
fait d avoir a la fois `RociaDbBuilder::build()` et un point d entree
direct `RociaDbClient.connect()` (TypeScript uniquement — le builder y est
un mince wrapper sans capacite propre, donc dupliquer un second point
d entree en Rust ajouterait une API a maintenir pour zero capacite
nouvelle).

## Migrating to 0.6.0

EN: 0.6.0 brings this SDK to full capability parity with the TypeScript SDK
— see [Parity with the TypeScript SDK](#parity-with-the-typescript-sdk)
above. **Every public method, type, and option that existed in 0.5.0 still
exists, with the same signature and the same behavior — this release only
adds.** Nothing is removed, nothing already-shipped becomes an error, and
every new capability below is opt-in: existing call sites keep compiling
and behaving exactly as before without touching them.
FR: La 0.6.0 amene ce SDK a une parite de capacites complete avec le SDK
TypeScript — voir
[Parity with the TypeScript SDK](#parity-with-the-typescript-sdk)
ci-dessus. **Chaque methode, type et option publics qui existaient en
0.5.0 existent toujours, avec la meme signature et le meme comportement —
cette version ne fait qu ajouter.** Rien n est retire, rien de deja livre
ne devient une erreur, et chaque nouvelle capacite ci-dessous est
opt-in : les points d appel existants continuent de compiler et de se
comporter exactement comme avant sans y toucher.

EN: **Documentation-only correction, no behavior change:** this README
used to state that the server required upload chunks of exactly 1 MiB and
that anything else silently corrupted a later download. That was accurate
against every server up to `1.0.0-rc.15`, and is no longer accurate against
`1.0.0-rc.16` and later — the server now reads a download back by
`size_bytes`, not by an assumed chunk count. This SDK's own chunking never
needed to change (it already always emitted exactly-1-MiB chunks, which
remains correct and is still the most efficient choice, and is still
required for correctness against a pre-`rc.16` server) — only the
documentation explaining *why* was wrong and has been corrected. See
[The upload wire contract](#the-upload-wire-contract) for the current
rules and [Migrating to 0.3.0](#migrating-to-030) for the historical note.
FR: **Correction de documentation uniquement, aucun changement de
comportement :** ce README affirmait auparavant que le serveur exigeait
des chunks d upload d exactement 1 MiB et que tout le reste corrompait
silencieusement un download ulterieur. C etait exact face a tout serveur
jusqu a `1.0.0-rc.15`, et ce ne l est plus face a `1.0.0-rc.16` et
au-dela — le serveur relit desormais un download d apres `size_bytes`, pas
d apres un nombre de chunks suppose. Le decoupage de ce SDK lui-meme n a
jamais eu besoin de changer (il emettait deja toujours des chunks
d exactement 1 MiB, ce qui reste correct et demeure le choix le plus
efficace, et reste requis pour la correction face a un serveur pre-`rc.16`)
— seule la documentation expliquant *pourquoi* etait fausse et a ete
corrigee. Voir [The upload wire contract](#the-upload-wire-contract) pour
les regles actuelles et [Migrating to 0.3.0](#migrating-to-030) pour la
note historique.

EN: New capabilities added in 0.6.0, each documented where linked:
FR: Nouvelles capacites ajoutees en 0.6.0, chacune documentee la ou elle
est liee :

- `RociaDbBuilder::connect_timeout` and host-path validation on `build()`
  — see [Host validation and connect timeout](#host-validation-and-connect-timeout).
- `RociaDbClient::create_document_with_request_id` — see
  [API Conventions](#api-conventions).
- `RociaDbClient::invalidate_auth_token` (the lazy counterpart to the
  existing `refresh_auth_token`) — see
  [Two ways to recover from `UNAUTHENTICATED`](#two-ways-to-recover-from-unauthenticated).
- `RociaDbClient::upload_file_chunked` and `FileStreamUploadOptions` — see
  [Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file).
- `rust-version = "1.85"` declared in `Cargo.toml` — see
  [Installation](#installation).

EN: Also fixed in 0.6.0, purely as internal hardening — no call site needs
to change for any of these:
FR: Egalement corrige en 0.6.0, uniquement en tant que durcissement
interne — aucun point d appel n a besoin de changer pour l un de ces
points :

- `RociaDbBuilder`'s `Debug` output no longer leaks `client_secret` in
  plaintext (it now prints `"[redacted]"`).
- `build()`'s debug-level log no longer includes `token_url`/`client_id`.
- `ApiKeyInterceptor`'s key comparison is now constant-time instead of a
  short-circuiting `==`, closing a timing side-channel.
- **The one item here with an observable, non-cosmetic effect:** the
  auto-generated `request_id` prefix for a document/node write is now
  always `put_document:{collection}:<uuid>` / `put_node:<uuid>`, whichever
  call path triggered it. Before 0.6.0, `create_document` and the batch
  path behind `put_nodes` generated `upsert_document:...` /
  `upsert_node:...` instead — an internal inconsistency (the single-item
  `put_document`/`put_node` methods already used the `put_*` prefix) that
  never affected correctness, but did mean the default idempotency-key
  *format* depended on which method produced it. Nothing to do if you
  never inspect or log auto-generated `request_id` values; expect the
  `put_*` prefix uniformly from 0.6.0 onward if you do.

EN: 0.6.0 is released together with, and version-numbered to match, the
TypeScript SDK's own 0.6.0 — a byproduct of bringing the two to capability
parity in the same pass, not a versioning scheme this changelog is
committing either SDK to for future releases.
FR: La 0.6.0 est publiee en meme temps que, et avec le meme numero de
version que, la 0.6.0 du SDK TypeScript — un sous-produit du fait d avoir
amene les deux a la parite de capacites dans la meme passe, pas un schema
de versionnement que ce changelog engage pour les versions futures d un
SDK ou de l autre.

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

   **This paragraph was accurate at the time it was written, against every
   server version up to `1.0.0-rc.15`.** Server `1.0.0-rc.16` (see
   [Migrating to 0.6.0](#migrating-to-060)) changed the download side of
   this contract: it now reads back exactly `size_bytes` bytes instead of
   guessing `ceil(size_bytes / 1 MiB)` chunk indexes, so an upload chunked
   at anything other than exactly 1 MiB no longer corrupts a later
   download. This SDK's behavior described above did not need to change —
   it already always emitted exactly-1-MiB chunks — but the *reason* it
   still does is now efficiency and pre-`rc.16` compatibility, not
   correctness against the current server. See
   [The upload wire contract](#the-upload-wire-contract) for the current
   rules.

   FR: **Ce paragraphe etait exact au moment ou il a ete ecrit, face a
   toute version serveur jusqu a `1.0.0-rc.15`.** Le serveur `1.0.0-rc.16`
   (voir [Migrating to 0.6.0](#migrating-to-060)) a change le cote download
   de ce contrat : il relit desormais exactement `size_bytes` octets au
   lieu de deviner des index de chunk via `ceil(size_bytes / 1 MiB)`, donc
   un upload decoupe autrement qu en exactement 1 MiB ne corrompt plus un
   download ulterieur. Le comportement de ce SDK decrit ci-dessus n a pas eu
   besoin de changer — il emettait deja toujours des chunks d exactement 1
   MiB — mais la *raison* pour laquelle il continue de le faire est
   desormais l efficacite et la compatibilite avec un serveur pre-`rc.16`,
   pas la correction face au serveur actuel. Voir
   [The upload wire contract](#the-upload-wire-contract) pour les regles
   actuelles.
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
