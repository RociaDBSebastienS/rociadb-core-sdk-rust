//! EN: File upload/download helpers.
//!
//! As of server `1.0.0-rc.16`, the upstream server stores each upload chunk
//! verbatim at its position in the stream and, on download, reads chunks
//! back until it has collected `size_bytes` bytes in total — it does not
//! assume any particular chunk size. A single message's `chunk` larger
//! than 1 MiB (1_048_576 bytes) is rejected outright with
//! `INVALID_ARGUMENT`; anything at or under that cap is accepted, sliced
//! however the caller likes.
//!
//! [`RociaDbClient::upload_file`] and [`RociaDbClient::upload_file_chunked`]
//! both still always emit exactly-1-MiB chunks (the last one may be
//! shorter) — not because the server requires it, but because 1 MiB is the
//! largest message the server allows, so it is also the fewest possible
//! messages for a given file. **Before `rc.16`**, this was a correctness
//! requirement rather than just an efficiency choice: the server derived
//! how many chunks to read back on download as `ceil(size_bytes / 1 MiB)`,
//! so any upload chunk size other than exactly 1 MiB made a later download
//! silently return truncated or garbled data, with no server-side error at
//! all — which is why this SDK has always chunked at exactly 1 MiB, and
//! why it still does: that chunking remains correct and optimal against
//! `rc.16`, and is the only chunking that stays safe against a pre-`rc.16`
//! server.
//!
//! Only reach for [`RociaDbClient::upload_file_stream`] directly if you
//! understand and reproduce the wire contract yourself; see its own docs
//! for what it does and does not validate.
//! FR: Aides pour l upload/download de fichiers.
//!
//! Depuis le serveur `1.0.0-rc.16`, le serveur upstream stocke chaque
//! chunk d upload tel quel a sa position dans le flux et, au download,
//! relit des chunks jusqu a avoir recueilli au total `size_bytes` octets —
//! il ne suppose aucune taille de chunk particuliere. Un message dont le
//! `chunk` depasse 1 MiB (1_048_576 octets) est rejete directement avec
//! `INVALID_ARGUMENT` ; tout ce qui est a la limite ou en dessous est
//! accepte, decoupe comme l appelant le souhaite.
//!
//! [`RociaDbClient::upload_file`] et [`RociaDbClient::upload_file_chunked`]
//! emettent tous deux toujours des chunks d exactement 1 MiB (le dernier
//! peut etre plus court) — non pas parce que le serveur l exige, mais
//! parce que 1 MiB est le plus gros message que le serveur autorise, donc
//! aussi le moins de messages possible pour un fichier donne. **Avant
//! `rc.16`**, c etait une exigence de correction plutot qu un simple choix
//! d efficacite : le serveur deduisait le nombre de chunks a relire au
//! download comme `ceil(size_bytes / 1 MiB)`, donc toute taille de chunk
//! d upload differente de 1 MiB exactement faisait qu un download
//! ulterieur renvoyait silencieusement des donnees tronquees ou
//! corrompues, sans aucune erreur cote serveur — c est pourquoi ce SDK a
//! toujours decoupe a exactement 1 MiB, et pourquoi il continue de le
//! faire : ce decoupage reste correct et optimal face a `rc.16`, et c est
//! le seul decoupage qui reste sur face a un serveur pre-`rc.16`.
//!
//! N utilisez [`RociaDbClient::upload_file_stream`] directement que si
//! vous comprenez et reproduisez vous-meme le contrat sur le fil ; voir sa
//! propre documentation pour ce qu elle valide et ne valide pas.
use crate::error::StatusResultExt;
use crate::pb::upstream::v1::{
    DeleteRequest, DownloadRequest, DownloadResponse, ListBucketsRequest, ListFilesRequest,
    StatRequest, StatResponse, UploadRequest,
};
use crate::{Page, Result, RociaDbClient, RociaDbError, non_empty, page_request};
use futures::{Stream, StreamExt, stream};
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::codec::Streaming;
use uuid::Uuid;

/// EN: Size of every upload message the SDK emits, except the last one.
/// This is the server's per-message ceiling (`CHUNK_SIZE`), so it is the
/// fewest messages a file can be sent in. Not configurable: see the module
/// docs.
/// FR: Taille de chaque message d upload emis par le SDK, sauf le dernier.
/// C est le plafond par message du serveur (`CHUNK_SIZE`), donc le plus
/// petit nombre de messages possible pour un fichier. Non configurable :
/// voir la doc du module.
const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB.

/// EN: Length in bytes of a SHA-256 digest, required by the server's
/// checksum validation.
/// FR: Longueur en octets d un digest SHA-256, exigee par la validation du
/// checksum cote serveur.
const CHECKSUM_LEN: usize = 32;

/// EN: Server-side max file size (`limits.max_file_bytes`, 5 GiB default).
/// FR: Taille de fichier max cote serveur (`limits.max_file_bytes`, 5 GiB
/// par defaut).
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Options applied to an ergonomic byte-buffer upload.
///
/// EN: There is intentionally no `chunk_size` knob. 1 MiB is the server's
/// per-message ceiling, so [`RociaDbClient::upload_file`] sending exactly
/// that (the last message may be shorter) is simply the fewest messages a
/// file can take. A smaller size would still be accepted — the server reads
/// a file back by its recorded `size_bytes` — but would buy nothing. See
/// the module docs.
/// FR: Il n y a volontairement pas de reglage `chunk_size`. 1 MiB est le
/// plafond par message du serveur, donc [`RociaDbClient::upload_file`] qui
/// envoie exactement cette taille (le dernier message peut etre plus court)
/// utilise simplement le moins de messages possible. Une taille plus petite
/// serait acceptee — le serveur relit un fichier d apres son `size_bytes`
/// enregistre — mais n apporterait rien. Voir la doc du module.
#[derive(Debug, Clone)]
pub struct FileUploadOptions {
    pub content_type: String,
    /// EN: SHA-256 digest of the uploaded bytes, as exactly 32 raw bytes.
    /// When `None`, [`RociaDbClient::upload_file`] computes it from the
    /// buffer automatically. When `Some`, it must be exactly 32 bytes or
    /// the upload fails before any network call — the server rejects any
    /// other length with `INVALID_ARGUMENT`.
    /// FR: Digest SHA-256 des octets uploades, en exactement 32 octets
    /// bruts. Quand il vaut `None`, [`RociaDbClient::upload_file`] le
    /// calcule automatiquement depuis le buffer. Quand il vaut `Some`, il
    /// doit faire exactement 32 octets sinon l upload echoue avant tout
    /// appel reseau — le serveur rejette toute autre longueur avec
    /// `INVALID_ARGUMENT`.
    pub checksum: Option<Vec<u8>>,
    pub request_id: Option<String>,
}

impl Default for FileUploadOptions {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            checksum: None,
            request_id: None,
        }
    }
}

/// Options applied to [`RociaDbClient::upload_file_chunked`].
///
/// EN: Unlike [`FileUploadOptions`], there is no `checksum` field here: for
/// a streaming upload the checksum cannot be computed automatically (the
/// whole point is to never hold the complete file in memory), so
/// [`RociaDbClient::upload_file_chunked`] takes it as its own required
/// `checksum` parameter instead of folding it into this struct — keeping it
/// there would wrongly suggest it is optional, the way it genuinely is on
/// [`FileUploadOptions::checksum`].
/// FR: Contrairement a [`FileUploadOptions`], il n y a pas de champ
/// `checksum` ici : pour un upload en streaming, le checksum ne peut pas
/// etre calcule automatiquement (tout l interet est de ne jamais garder le
/// fichier complet en memoire), donc [`RociaDbClient::upload_file_chunked`]
/// le prend comme son propre parametre `checksum` obligatoire plutot que de
/// le replier dans cette structure — l y laisser suggererait a tort qu il
/// est optionnel, comme il l est reellement sur
/// [`FileUploadOptions::checksum`].
#[derive(Debug, Clone)]
pub struct FileStreamUploadOptions {
    pub content_type: String,
    pub request_id: Option<String>,
}

impl Default for FileStreamUploadOptions {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            request_id: None,
        }
    }
}

impl RociaDbClient {
    /// EN: Upload a caller-built stream of protobuf `UploadRequest` messages.
    ///
    /// This is a low-level escape hatch for genuine streaming uploads (data
    /// that never fits in memory). The SDK does **not** rechunk or compute
    /// a checksum here — the caller is fully responsible for the wire
    /// contract the server enforces:
    /// - the **first** message must carry `tenant_id`, `bucket`, `file_id`,
    ///   `size_bytes` (the exact total byte count) and `checksum` set to
    ///   the SHA-256 digest of the whole file, as exactly 32 raw bytes;
    /// - every message's `chunk` must not exceed 1 MiB (1_048_576 bytes) —
    ///   below that cap, the server accepts any size, sliced however the
    ///   caller likes (see the module docs for why this SDK's own
    ///   `upload_file`/`upload_file_chunked` still always choose exactly
    ///   1 MiB chunks even though the server no longer requires it);
    /// - `content_type` and `checksum` on messages after the first are
    ///   ignored by the server and can be left empty.
    ///
    /// A `chunk` over 1 MiB, a checksum of the wrong length, or a
    /// mismatched `size_bytes` all fail the upload outright with
    /// `INVALID_ARGUMENT` rather than corrupting anything silently. The one
    /// thing the server never verifies is whether `checksum` actually
    /// matches the bytes sent — only that it is 32 bytes long — so a wrong
    /// checksum can still produce an upload that looks successful while
    /// carrying bad data.
    ///
    /// For the common case — uploading an in-memory byte buffer — use
    /// [`RociaDbClient::upload_file`] instead, which builds a correct
    /// stream for you; for a large source you cannot buffer but can still
    /// checksum ahead of time, prefer
    /// [`RociaDbClient::upload_file_chunked`], which re-chunks and
    /// validates for you.
    /// FR: Upload un flux d `UploadRequest` protobuf construit par
    /// l appelant.
    ///
    /// C est une echappatoire bas niveau pour les uploads vraiment en
    /// streaming (donnees qui ne tiennent jamais en memoire). Le SDK ne
    /// re-decoupe ni ne calcule de checksum ici : l appelant est
    /// entierement responsable du contrat impose par le serveur :
    /// - le **premier** message doit porter `tenant_id`, `bucket`,
    ///   `file_id`, `size_bytes` (le nombre total exact d octets) et
    ///   `checksum` regle au digest SHA-256 du fichier complet, en
    ///   exactement 32 octets bruts ;
    /// - le champ `chunk` de chaque message ne doit pas depasser 1 MiB
    ///   (1_048_576 octets) — en dessous de cette limite, le serveur
    ///   accepte n importe quelle taille, decoupee comme l appelant le
    ///   souhaite (voir la doc du module pour la raison pour laquelle
    ///   `upload_file`/`upload_file_chunked` de ce SDK choisissent quand
    ///   meme toujours des chunks d exactement 1 MiB alors que le serveur
    ///   ne l exige plus) ;
    /// - `content_type` et `checksum` sur les messages suivants sont
    ///   ignores par le serveur et peuvent rester vides.
    ///
    /// Un `chunk` de plus de 1 MiB, un checksum de mauvaise longueur, ou un
    /// `size_bytes` incoherent font tous echouer l upload directement avec
    /// `INVALID_ARGUMENT` plutot que de corrompre quoi que ce soit
    /// silencieusement. La seule chose que le serveur ne verifie jamais est
    /// si `checksum` correspond reellement aux octets envoyes — seulement
    /// qu il fait 32 octets — donc un checksum errone peut encore produire
    /// un upload qui semble reussi tout en portant des donnees erronees.
    ///
    /// Pour le cas courant — uploader un buffer d octets en memoire —
    /// utilisez plutot [`RociaDbClient::upload_file`], qui construit un
    /// flux correct pour vous ; pour une source volumineuse que vous ne
    /// pouvez pas bufferiser mais que vous pouvez quand meme checksummer a
    /// l avance, preferez [`RociaDbClient::upload_file_chunked`], qui
    /// re-decoupe et valide pour vous.
    ///
    /// Pour le cas courant — uploader un buffer d octets en memoire —
    /// utilisez plutot [`RociaDbClient::upload_file`], qui construit un
    /// flux correct pour vous.
    pub async fn upload_file_stream<S>(&self, requests: S) -> Result<()>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .upload(requests)
            .await
            .status_context("failed to upload file")?;
        Ok(())
    }

    /// EN: Upload an in-memory byte buffer, split into gRPC messages of the
    /// server's largest allowed chunk size.
    ///
    /// The buffer is always split into 1 MiB (1_048_576-byte) chunks,
    /// matching the server's `CHUNK_SIZE` cap (the last chunk may be
    /// shorter) — the fewest possible messages for a given file size. This
    /// is not configurable: see the module docs for why. When
    /// `options.checksum` is `None`, the SHA-256 digest of `bytes` is
    /// computed and sent automatically; when it is `Some`, it must be
    /// exactly 32 bytes or this returns an error before any network call.
    /// Files over 5 GiB (`limits.max_file_bytes`, the server default) are
    /// rejected client-side with a clear error instead of failing partway
    /// through the upload.
    /// FR: Upload un buffer d octets en memoire, decoupe en messages gRPC
    /// de la plus grande taille de chunk autorisee par le serveur.
    ///
    /// Le buffer est toujours decoupe en chunks de 1 MiB (1_048_576
    /// octets), correspondant au plafond `CHUNK_SIZE` du serveur (le
    /// dernier chunk peut etre plus court) — le moins de messages possible
    /// pour une taille de fichier donnee. Ce n est pas configurable : voir
    /// la doc du module pour la raison. Quand `options.checksum` vaut
    /// `None`, le digest SHA-256 de `bytes` est calcule et envoye
    /// automatiquement ; quand il vaut `Some`, il doit faire exactement 32
    /// octets sinon retourne une erreur avant tout appel reseau. Les
    /// fichiers de plus de 5 GiB (`limits.max_file_bytes`, valeur par
    /// defaut cote serveur) sont rejetes cote client avec une erreur
    /// claire plutot que de faire echouer l upload en cours de route.
    pub async fn upload_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: impl AsRef<[u8]>,
        options: FileUploadOptions,
    ) -> Result<()> {
        let bytes = bytes.as_ref();
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| RociaDbError::validation("file is too large"))?;
        validate_file_size(size_bytes)?;

        let checksum = resolve_checksum(options.checksum, bytes)?;
        let request_id = options
            .request_id
            .unwrap_or_else(|| format!("upload_file:{}", Uuid::new_v4()));

        let requests = chunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            bytes.to_vec(),
            options.content_type,
            checksum,
            request_id,
        );
        self.upload_file_stream(stream::iter(requests)).await
    }

    /// EN: Upload a stream of arbitrarily-sized byte chunks without
    /// buffering the complete file in memory.
    ///
    /// This is the middle tier between [`RociaDbClient::upload_file`]
    /// (buffers the whole file, computes the checksum for you) and
    /// [`RociaDbClient::upload_file_stream`] (a raw pass-through with zero
    /// validation, and the caller must already match the server's exact
    /// wire contract). `chunks` may be split however the source naturally
    /// produces data — a `64 KiB` `AsyncRead` wrapper, protobuf messages
    /// from another stream, anything — this method re-buffers internally
    /// and always emits exactly-1-MiB gRPC messages to the server (the last
    /// one may be shorter), the same chunking [`RociaDbClient::upload_file`]
    /// produces from an in-memory buffer.
    ///
    /// `size_bytes` must be the exact total the caller intends to send, and
    /// `checksum` must already be the 32-byte SHA-256 digest of the
    /// complete file: unlike [`RociaDbClient::upload_file`], neither can be
    /// computed for you here, because file metadata travels on the first
    /// gRPC message, before this method has read a single byte from
    /// `chunks`. Hash the source ahead of time (a first pass over the file,
    /// for example) if you only have raw bytes. `checksum` is validated to
    /// be exactly 32 bytes before any network call.
    ///
    /// If `chunks` ends up producing more or fewer total bytes than
    /// `size_bytes` declared, this fails with
    /// [`RociaDbError::Validation`] instead of silently sending a
    /// corrupt-on-download file: the server itself also checks this at the
    /// end of the stream, but catching it here gives a clearer, immediate
    /// error naming the actual byte counts involved.
    ///
    /// **Naming note**: despite matching the server's chunking contract,
    /// this is not called `upload_file_stream` — that name was already
    /// taken by the raw, zero-validation escape hatch above it. Do not
    /// assume the two names are interchangeable across languages: the
    /// TypeScript SDK's `uploadFileStream` is this method's counterpart,
    /// not [`RociaDbClient::upload_file_stream`]'s.
    /// FR: Upload un flux de morceaux d octets de taille quelconque sans
    /// bufferiser le fichier complet en memoire.
    ///
    /// C est le palier intermediaire entre [`RociaDbClient::upload_file`]
    /// (bufferise tout le fichier, calcule le checksum pour vous) et
    /// [`RociaDbClient::upload_file_stream`] (un passe-plat brut sans
    /// aucune validation, ou l appelant doit deja respecter exactement le
    /// contrat sur le fil du serveur). `chunks` peut etre decoupe comme la
    /// source le produit naturellement — un wrapper `AsyncRead` en
    /// `64 KiB`, des messages protobuf venant d un autre flux, n importe
    /// quoi — cette methode re-bufferise en interne et emet toujours des
    /// messages gRPC d exactement 1 MiB vers le serveur (le dernier peut
    /// etre plus court), le meme decoupage que
    /// [`RociaDbClient::upload_file`] produit depuis un buffer en memoire.
    ///
    /// `size_bytes` doit etre le total exact que l appelant compte
    /// envoyer, et `checksum` doit deja etre le digest SHA-256 de 32 octets
    /// du fichier complet : contrairement a
    /// [`RociaDbClient::upload_file`], aucun des deux ne peut etre calcule
    /// pour vous ici, car les metadonnees du fichier voyagent sur le
    /// premier message gRPC, avant que cette methode n ait lu le moindre
    /// octet de `chunks`. Hachez la source a l avance (une premiere passe
    /// sur le fichier, par exemple) si vous n avez que des octets bruts.
    /// `checksum` est valide comme faisant exactement 32 octets avant tout
    /// appel reseau.
    ///
    /// Si `chunks` finit par produire plus ou moins d octets au total que
    /// ce que `size_bytes` declarait, ceci echoue avec
    /// [`RociaDbError::Validation`] plutot que d envoyer silencieusement un
    /// fichier corrompu au download : le serveur verifie aussi cela lui-meme
    /// a la fin du flux, mais l attraper ici donne une erreur plus claire
    /// et immediate, nommant les comptes d octets reels en cause.
    ///
    /// **Note de nommage** : malgre le respect du contrat de decoupage du
    /// serveur, ceci ne s appelle pas `upload_file_stream` — ce nom etait
    /// deja pris par l echappatoire brute sans validation ci-dessus. Ne
    /// presumez pas que les deux noms se correspondent d un langage a
    /// l autre : `uploadFileStream` du SDK TypeScript est l equivalent de
    /// cette methode-ci, pas de [`RociaDbClient::upload_file_stream`].
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_file_chunked<S>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        size_bytes: u64,
        checksum: Vec<u8>,
        chunks: S,
        options: FileStreamUploadOptions,
    ) -> Result<()>
    where
        S: Stream<Item = Vec<u8>> + Send + 'static,
    {
        validate_file_size(size_bytes)?;
        require_checksum_len(&checksum)?;
        let request_id = options
            .request_id
            .unwrap_or_else(|| format!("upload_file:{}", Uuid::new_v4()));

        // EN: Set by `rechunk_upload_requests` when the source produced a
        // total byte count that does not match `size_bytes`, since the
        // outgoing `Stream<Item = UploadRequest>` itself has no channel to
        // carry an error — it can only end early. Checked below regardless
        // of whether the RPC itself succeeded or failed, so this
        // client-side validation error takes precedence over whatever the
        // server made of a stream that ended up short or truncated.
        // FR: Renseigne par `rechunk_upload_requests` quand la source a
        // produit un total d octets qui ne correspond pas a `size_bytes`,
        // car le `Stream<Item = UploadRequest>` sortant n a lui-meme aucun
        // canal pour transporter une erreur — il peut seulement se
        // terminer plus tot. Verifie ci-dessous que le RPC ait reussi ou
        // echoue, pour que cette erreur de validation cote client prenne
        // le pas sur ce que le serveur a fait d un flux termine court ou
        // tronque.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests = rechunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            size_bytes,
            options.content_type,
            checksum,
            request_id,
            chunks,
            Arc::clone(&error_slot),
        );

        let mut upstream_file = self.upstream_file.clone();
        let upload_result = upstream_file.upload(requests).await;
        if let Some(error) = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }
        upload_result.status_context("failed to upload file")?;
        Ok(())
    }

    /// Start a server-streaming download without buffering the complete file.
    pub async fn download_file_stream(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Streaming<DownloadResponse>> {
        let mut upstream_file = self.upstream_file.clone();
        Ok(upstream_file
            .download(DownloadRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .status_context("failed to start file download")?
            .into_inner())
    }

    /// Download a complete file into memory.
    pub async fn download_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Vec<u8>> {
        let mut stream = self
            .download_file_stream(tenant_id, bucket, file_id)
            .await?;
        let mut bytes = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .status_context("file download stream failed")?
        {
            bytes.extend_from_slice(&response.chunk);
        }
        Ok(bytes)
    }

    /// Return metadata for one stored file.
    pub async fn stat_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<StatResponse> {
        let mut upstream_file = self.upstream_file.clone();
        Ok(upstream_file
            .stat(StatRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .status_context("failed to stat file")?
            .into_inner())
    }

    /// Return one paginated page of bucket names holding at least one file.
    pub async fn list_buckets(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let mut upstream_file = self.upstream_file.clone();
        let response = upstream_file
            .list_buckets(ListBucketsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list buckets")?
            .into_inner();
        Ok(Page {
            items: response.buckets,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of file ids stored in one bucket.
    pub async fn list_files(
        &self,
        tenant_id: &str,
        bucket: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let mut upstream_file = self.upstream_file.clone();
        let response = upstream_file
            .list_files(ListFilesRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list files")?
            .into_inner();
        Ok(Page {
            items: response.file_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Delete one stored file using an automatically generated idempotency key.
    pub async fn delete_file(&self, tenant_id: &str, bucket: &str, file_id: &str) -> Result<()> {
        self.delete_file_with_request_id(
            tenant_id,
            bucket,
            file_id,
            format!("delete_file:{}", Uuid::new_v4()),
        )
        .await
    }

    /// Delete one stored file with a caller-provided idempotency key.
    pub async fn delete_file_with_request_id(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .delete(DeleteRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
                request_id: request_id.into(),
            })
            .await
            .status_context("failed to delete file")?;
        Ok(())
    }
}

/// EN: Resolve the checksum to send: computes the SHA-256 digest of `bytes`
/// automatically when `checksum` is `None`; when it is `Some`, validates it
/// is exactly [`CHECKSUM_LEN`] bytes before returning it. Pure and network-
/// free, so [`RociaDbClient::upload_file`] can fail fast on a bad checksum
/// before any RPC — the server rejects any other length with
/// `INVALID_ARGUMENT`.
/// FR: Resout le checksum a envoyer : calcule automatiquement le digest
/// SHA-256 de `bytes` quand `checksum` vaut `None` ; quand il vaut `Some`,
/// valide qu il fait exactement [`CHECKSUM_LEN`] octets avant de le
/// renvoyer. Pure et sans reseau, pour que
/// [`RociaDbClient::upload_file`] puisse echouer vite sur un checksum
/// invalide avant tout RPC — le serveur rejette toute autre longueur avec
/// `INVALID_ARGUMENT`.
fn resolve_checksum(checksum: Option<Vec<u8>>, bytes: &[u8]) -> Result<Vec<u8>> {
    match checksum {
        Some(checksum) => {
            require_checksum_len(&checksum)?;
            Ok(checksum)
        }
        None => Ok(Sha256::digest(bytes).to_vec()),
    }
}

/// EN: Validate that `checksum` is exactly [`CHECKSUM_LEN`] bytes, before
/// any network call. Shared by [`resolve_checksum`] (used by
/// [`RociaDbClient::upload_file`], where the checksum is optional and
/// computed automatically when absent) and
/// [`RociaDbClient::upload_file_chunked`] (where it is a required
/// parameter, since a streaming upload cannot compute it on the fly — see
/// that method's docs).
/// FR: Valide que `checksum` fait exactement [`CHECKSUM_LEN`] octets, avant
/// tout appel reseau. Partagee par [`resolve_checksum`] (utilisee par
/// [`RociaDbClient::upload_file`], ou le checksum est optionnel et calcule
/// automatiquement quand absent) et
/// [`RociaDbClient::upload_file_chunked`] (ou il est un parametre
/// obligatoire, car un upload en streaming ne peut pas le calculer a la
/// volee — voir la doc de cette methode).
fn require_checksum_len(checksum: &[u8]) -> Result<()> {
    if checksum.len() != CHECKSUM_LEN {
        return Err(RociaDbError::validation(format!(
            "checksum must be exactly {CHECKSUM_LEN} bytes (sha256), got {} bytes",
            checksum.len()
        )));
    }
    Ok(())
}

/// EN: Validate that `size_bytes` does not exceed [`MAX_FILE_BYTES`] (5
/// GiB), before any network call. Shared by [`RociaDbClient::upload_file`]
/// and [`RociaDbClient::upload_file_chunked`] so both reject an oversized
/// file with the same client-side error instead of letting the upload run
/// and fail server-side partway through.
/// FR: Valide que `size_bytes` ne depasse pas [`MAX_FILE_BYTES`] (5 GiB),
/// avant tout appel reseau. Partagee par [`RociaDbClient::upload_file`] et
/// [`RociaDbClient::upload_file_chunked`] pour que les deux rejettent un
/// fichier trop volumineux avec la meme erreur cote client, plutot que de
/// laisser l upload demarrer et echouer cote serveur en cours de route.
fn validate_file_size(size_bytes: u64) -> Result<()> {
    if size_bytes > MAX_FILE_BYTES {
        return Err(RociaDbError::validation(format!(
            "file is {size_bytes} bytes, which exceeds the server's {MAX_FILE_BYTES}-byte \
             (5 GiB) limit"
        )));
    }
    Ok(())
}

/// EN: Lazily build the per-chunk `UploadRequest` sequence for `bytes`.
///
/// Only the first request carries the file metadata (`tenant_id`,
/// `bucket`, `file_id`, `size_bytes`, `content_type`, `checksum`,
/// `request_id`): the server only reads those fields off the first message
/// of the stream (see module docs), so building them for every chunk would
/// just be wasted clones. Requests are produced on demand as the returned
/// iterator is polled by the outgoing stream, never collected into a `Vec`
/// up front.
/// FR: Construit paresseusement la sequence d `UploadRequest` par chunk
/// pour `bytes`.
///
/// Seule la premiere requete porte les metadonnees du fichier
/// (`tenant_id`, `bucket`, `file_id`, `size_bytes`, `content_type`,
/// `checksum`, `request_id`) : le serveur ne lit ces champs que sur le
/// premier message du flux (voir la doc du module), donc les construire
/// pour chaque chunk ne ferait que cloner pour rien. Les requetes sont
/// produites a la demande au fur et a mesure que l iterateur retourne est
/// consomme par le flux sortant, jamais collectees dans un `Vec` a l
/// avance.
fn chunk_upload_requests(
    tenant_id: String,
    bucket: String,
    file_id: String,
    bytes: Vec<u8>,
    content_type: String,
    checksum: Vec<u8>,
    request_id: String,
) -> impl Iterator<Item = UploadRequest> {
    let size_bytes = bytes.len() as u64;
    // EN: A zero-byte file still needs one message to carry the metadata,
    // even though it has no chunk to store.
    // FR: Un fichier de zero octet a quand meme besoin d un message pour
    // porter les metadonnees, meme s il n a aucun chunk a stocker.
    let chunk_count = if bytes.is_empty() {
        1
    } else {
        size_bytes.div_ceil(DEFAULT_CHUNK_SIZE as u64)
    };

    let mut tenant_id = Some(tenant_id);
    let mut bucket = Some(bucket);
    let mut file_id = Some(file_id);
    let mut content_type = Some(content_type);
    let mut checksum = Some(checksum);
    let mut request_id = Some(request_id);

    (0..chunk_count).map(move |index| {
        let start = index as usize * DEFAULT_CHUNK_SIZE;
        let end = (start + DEFAULT_CHUNK_SIZE).min(bytes.len());
        UploadRequest {
            tenant_id: tenant_id.take().unwrap_or_default(),
            bucket: bucket.take().unwrap_or_default(),
            file_id: file_id.take().unwrap_or_default(),
            size_bytes: if index == 0 { size_bytes } else { 0 },
            content_type: content_type.take().unwrap_or_default(),
            checksum: checksum.take().unwrap_or_default(),
            chunk: bytes[start..end].to_vec(),
            request_id: request_id.take().unwrap_or_default(),
        }
    })
}

/// EN: File metadata attached only to the first `UploadRequest` produced by
/// [`rechunk_upload_requests`]; every later message leaves these fields at
/// their protobuf default (see [`chunk_upload_requests`] for why).
/// FR: Metadonnees de fichier attachees uniquement au premier
/// `UploadRequest` produit par [`rechunk_upload_requests`] ; tous les
/// messages suivants laissent ces champs a leur defaut protobuf (voir
/// [`chunk_upload_requests`] pour la raison).
struct UploadMetadata {
    tenant_id: String,
    bucket: String,
    file_id: String,
    content_type: String,
    checksum: Vec<u8>,
    request_id: String,
}

/// EN: Mutable state driving [`rechunk_upload_requests`]'s
/// `stream::unfold`, boxed and type-erased over the caller's source stream
/// so the state itself stays a plain, non-generic type.
/// FR: Etat mutable pilotant le `stream::unfold` de
/// [`rechunk_upload_requests`], boxe et type-efface par rapport au flux
/// source de l appelant pour que l etat lui-meme reste un type simple, non
/// generique.
struct RechunkState {
    source: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
    buffer: Vec<u8>,
    size_bytes: u64,
    total_written: u64,
    wrote_any: bool,
    source_exhausted: bool,
    metadata: Option<UploadMetadata>,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
}

impl RechunkState {
    /// EN: Record the "would exceed / falls short of `size_bytes`"
    /// validation error into `error_slot`, so
    /// [`RociaDbClient::upload_file_chunked`] can surface it after the
    /// stream this state drives has ended.
    /// FR: Enregistre l erreur de validation "depasserait / n atteint pas
    /// `size_bytes`" dans `error_slot`, pour que
    /// [`RociaDbClient::upload_file_chunked`] puisse la faire remonter une
    /// fois le flux pilote par cet etat termine.
    fn record_size_error(&self, message: String) {
        let mut guard = self
            .error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(RociaDbError::validation(message));
    }

    /// EN: Turn `chunk` into the next `UploadRequest`, attaching the file
    /// metadata only if this is the first request ever produced (mirrors
    /// [`chunk_upload_requests`]'s `index == 0` special case).
    /// FR: Transforme `chunk` en le prochain `UploadRequest`, en attachant
    /// les metadonnees du fichier seulement s il s agit de la toute
    /// premiere requete produite (reproduit le cas particulier `index == 0`
    /// de [`chunk_upload_requests`]).
    fn next_request(&mut self, chunk: Vec<u8>) -> UploadRequest {
        match self.metadata.take() {
            Some(metadata) => UploadRequest {
                tenant_id: metadata.tenant_id,
                bucket: metadata.bucket,
                file_id: metadata.file_id,
                size_bytes: self.size_bytes,
                content_type: metadata.content_type,
                checksum: metadata.checksum,
                chunk,
                request_id: metadata.request_id,
            },
            None => UploadRequest {
                chunk,
                ..Default::default()
            },
        }
    }
}

/// EN: Re-chunk an arbitrarily-sized byte stream into `UploadRequest`
/// messages of exactly [`DEFAULT_CHUNK_SIZE`] (1 MiB) each — the last one
/// possibly shorter — the core of
/// [`RociaDbClient::upload_file_chunked`]. Never buffers more than one
/// outgoing chunk's worth of bytes at a time, unlike
/// [`chunk_upload_requests`], which already holds the complete file in
/// memory by the time it runs.
///
/// Validates as it goes, exactly like the TypeScript SDK's
/// `rechunkToUploadSize`: a chunk that would push the running total past
/// `size_bytes` is rejected *before* being turned into a request (so it is
/// never sent), and running short of `size_bytes` once `chunks` is
/// exhausted is detected right after the last real chunk. Because the
/// returned `Stream<Item = UploadRequest>` has no channel of its own to
/// carry an error — a tonic client-streaming call only accepts a stream
/// that produces requests, never `Result`s — any such failure is recorded
/// into `error_slot` instead, and the stream simply ends early (or, for a
/// short source, ends normally after reporting the mismatch). The caller
/// (see [`RociaDbClient::upload_file_chunked`]) checks `error_slot` once
/// the RPC settles.
///
/// An empty source (`size_bytes` 0, no bytes at all) still produces exactly
/// one empty request, because the server only learns the file's metadata
/// from a message, and an upload that writes nothing would never deliver
/// it — the same rule [`chunk_upload_requests`] applies for a zero-byte
/// in-memory buffer.
/// FR: Re-decoupe un flux d octets de taille quelconque en messages
/// `UploadRequest` d exactement [`DEFAULT_CHUNK_SIZE`] (1 MiB) chacun — le
/// dernier eventuellement plus court — le coeur de
/// [`RociaDbClient::upload_file_chunked`]. Ne bufferise jamais plus que
/// l equivalent d un seul chunk sortant a la fois, contrairement a
/// [`chunk_upload_requests`], qui detient deja le fichier complet en
/// memoire au moment ou elle s execute.
///
/// Valide au fur et a mesure, exactement comme `rechunkToUploadSize` du SDK
/// TypeScript : un chunk qui ferait depasser `size_bytes` au total en cours
/// est rejete *avant* d etre transforme en requete (donc jamais envoye), et
/// un total inferieur a `size_bytes` une fois `chunks` epuise est detecte
/// juste apres le dernier chunk reel. Comme le `Stream<Item =
/// UploadRequest>` retourne n a pas de canal propre pour transporter une
/// erreur — un appel gRPC client-streaming via tonic n accepte qu un flux
/// qui produit des requetes, jamais des `Result` — tout echec de ce genre
/// est enregistre dans `error_slot` a la place, et le flux se termine
/// simplement plus tot (ou, pour une source trop courte, se termine
/// normalement apres avoir signale l ecart). L appelant (voir
/// [`RociaDbClient::upload_file_chunked`]) verifie `error_slot` une fois le
/// RPC termine.
///
/// Une source vide (`size_bytes` 0, aucun octet) produit quand meme
/// exactement une requete vide, car le serveur n apprend les metadonnees du
/// fichier que via un message, et un upload qui n ecrit rien ne les
/// delivrerait jamais — la meme regle que [`chunk_upload_requests`]
/// applique pour un buffer en memoire de zero octet.
#[allow(clippy::too_many_arguments)]
fn rechunk_upload_requests<S>(
    tenant_id: String,
    bucket: String,
    file_id: String,
    size_bytes: u64,
    content_type: String,
    checksum: Vec<u8>,
    request_id: String,
    chunks: S,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
) -> impl Stream<Item = UploadRequest>
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    let state = RechunkState {
        source: Box::pin(chunks),
        buffer: Vec::new(),
        size_bytes,
        total_written: 0,
        wrote_any: false,
        source_exhausted: false,
        metadata: Some(UploadMetadata {
            tenant_id,
            bucket,
            file_id,
            content_type,
            checksum,
            request_id,
        }),
        error_slot,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if state.buffer.len() >= DEFAULT_CHUNK_SIZE {
                let piece_len = DEFAULT_CHUNK_SIZE as u64;
                if state.total_written + piece_len > state.size_bytes {
                    state.record_size_error(format!(
                        "upload_file_chunked received more data than size_bytes \
                         ({} bytes) declared",
                        state.size_bytes
                    ));
                    return None;
                }
                let piece: Vec<u8> = state.buffer.drain(..DEFAULT_CHUNK_SIZE).collect();
                state.total_written += piece_len;
                state.wrote_any = true;
                let request = state.next_request(piece);
                return Some((request, state));
            }

            if !state.source_exhausted {
                match state.source.next().await {
                    Some(piece) => {
                        state.buffer.extend(piece);
                        continue;
                    }
                    None => {
                        state.source_exhausted = true;
                        continue;
                    }
                }
            }

            // EN: Source exhausted, less than one full chunk buffered:
            // flush the remainder (possibly empty, for a zero-byte file).
            // FR: Source epuisee, moins d un chunk complet en buffer : on
            // vide le reste (eventuellement vide, pour un fichier de zero
            // octet).
            if !state.buffer.is_empty() || !state.wrote_any {
                let piece_len = state.buffer.len() as u64;
                if state.total_written + piece_len > state.size_bytes {
                    state.record_size_error(format!(
                        "upload_file_chunked received more data than size_bytes \
                         ({} bytes) declared",
                        state.size_bytes
                    ));
                    return None;
                }
                state.total_written += piece_len;
                state.wrote_any = true;
                let piece = std::mem::take(&mut state.buffer);
                let request = state.next_request(piece);
                return Some((request, state));
            }

            if state.total_written != state.size_bytes {
                state.record_size_error(format!(
                    "upload_file_chunked sent {} bytes but size_bytes declared {}",
                    state.total_written, state.size_bytes
                ));
            }
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKSUM_LEN, DEFAULT_CHUNK_SIZE, FileStreamUploadOptions, FileUploadOptions,
        MAX_FILE_BYTES, chunk_upload_requests, rechunk_upload_requests, require_checksum_len,
        resolve_checksum, validate_file_size,
    };
    use crate::RociaDbError;
    use crate::pb::upstream::v1::UploadRequest;
    use futures::executor::block_on;
    use futures::{StreamExt, stream};
    use std::sync::{Arc, Mutex};

    #[test]
    fn upload_options_have_safe_defaults() {
        let options = FileUploadOptions::default();
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.checksum.is_none());
        assert!(options.request_id.is_none());
    }

    #[test]
    fn upload_requests_chunk_at_exactly_one_mebibyte() {
        let bytes = vec![7u8; DEFAULT_CHUNK_SIZE + 10];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            bytes.clone(),
            "text/plain".into(),
            vec![0u8; CHECKSUM_LEN],
            "stable-request".into(),
        )
        .collect();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 10);
        assert_eq!(requests[0].size_bytes, bytes.len() as u64);
        // EN: Only the first message carries metadata; the server ignores
        // the rest of the fields on later messages.
        assert_eq!(requests[1].size_bytes, 0);
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].checksum.len(), CHECKSUM_LEN);
        assert!(requests[1].checksum.is_empty());
        assert!(
            requests
                .iter()
                .all(|request| request.request_id == "stable-request"
                    || request.request_id.is_empty())
        );
        assert_eq!(requests[0].request_id, "stable-request");
    }

    #[test]
    fn empty_upload_still_emits_one_request() {
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Vec::new(),
            FileUploadOptions::default().content_type,
            vec![0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].size_bytes, 0);
        assert!(requests[0].chunk.is_empty());
    }

    /// EN: Asserts that chunking `total_bytes` matches the chunking this
    /// SDK has always produced: every chunk but the last is exactly 1 MiB,
    /// and the sum of chunk bytes equals `size_bytes`. That chunking is
    /// still the fewest possible messages against the server's 1 MiB cap
    /// (anything else either fails the upload outright with
    /// `chunk exceeds 1 MiB` or `size_bytes does not match uploaded data`),
    /// and it is also the only chunking known to be safe against a
    /// pre-`1.0.0-rc.16` server, which derived how many chunks to read back
    /// on download as `ceil(size_bytes / 1 MiB)` and silently corrupted the
    /// download of any file uploaded with a different chunk size — see the
    /// module docs.
    /// FR: Verifie que le decoupage de `total_bytes` correspond a celui que
    /// ce SDK a toujours produit : chaque chunk sauf le dernier fait
    /// exactement 1 MiB, et la somme des octets de chunk egale
    /// `size_bytes`. Ce decoupage reste le moins de messages possible face
    /// au plafond de 1 MiB du serveur (tout le reste fait echouer l upload
    /// directement avec `chunk exceeds 1 MiB` ou
    /// `size_bytes does not match uploaded data`), et c est aussi le seul
    /// decoupage connu comme sur face a un serveur pre-`1.0.0-rc.16`, qui
    /// deduisait le nombre de chunks a relire au download comme
    /// `ceil(size_bytes / 1 MiB)` et corrompait silencieusement le
    /// download de tout fichier uploade avec une autre taille de chunk —
    /// voir la doc du module.
    fn assert_chunking_matches_server_contract(total_bytes: usize) {
        let bytes = vec![9u8; total_bytes];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            bytes.clone(),
            "application/octet-stream".into(),
            vec![0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();

        assert!(!requests.is_empty(), "at least one message is required");
        assert_eq!(requests[0].size_bytes, total_bytes as u64);

        let bytes_sent: usize = requests.iter().map(|request| request.chunk.len()).sum();
        assert_eq!(
            bytes_sent, total_bytes,
            "sum of chunk bytes must equal size_bytes exactly"
        );

        if total_bytes == 0 {
            assert_eq!(requests.len(), 1);
            assert!(requests[0].chunk.is_empty());
            return;
        }

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty(), "the last chunk must not be empty");
        assert!(
            last.chunk.len() <= DEFAULT_CHUNK_SIZE,
            "the last chunk must not exceed 1 MiB"
        );
    }

    #[test]
    fn chunking_zero_bytes() {
        assert_chunking_matches_server_contract(0);
    }

    #[test]
    fn chunking_one_byte() {
        assert_chunking_matches_server_contract(1);
    }

    #[test]
    fn chunking_exactly_one_mebibyte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn chunking_one_mebibyte_plus_one_byte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE + 1);
    }

    #[test]
    fn chunking_about_two_and_a_half_mebibytes() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE * 2 + DEFAULT_CHUNK_SIZE / 2);
    }

    #[test]
    fn resolve_checksum_computes_sha256_by_default() {
        // EN: Known-answer test for SHA-256("hello world"), independent of
        // the crate's own `Sha256::digest` call, so a wiring mistake
        // (wrong input bytes, wrong algorithm) would be caught even if it
        // still happened to produce 32 bytes.
        // FR: Test a reponse connue pour SHA-256("hello world"),
        // independant de l appel `Sha256::digest` du crate, pour attraper
        // une erreur de cablage (mauvais octets en entree, mauvais
        // algorithme) meme si elle produisait quand meme 32 octets.
        let checksum =
            resolve_checksum(None, b"hello world").expect("default checksum must succeed");
        assert_eq!(checksum.len(), CHECKSUM_LEN);
        assert_eq!(
            checksum,
            decode_hex("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[test]
    fn resolve_checksum_is_deterministic_and_content_dependent() {
        let first = resolve_checksum(None, b"payload-a").expect("checksum must succeed");
        let second = resolve_checksum(None, b"payload-a").expect("checksum must succeed");
        let different = resolve_checksum(None, b"payload-b").expect("checksum must succeed");
        assert_eq!(first, second, "same bytes must yield the same checksum");
        assert_ne!(
            first, different,
            "different bytes must yield a different checksum"
        );
    }

    #[test]
    fn resolve_checksum_accepts_caller_supplied_32_bytes() {
        let supplied = vec![7u8; CHECKSUM_LEN];
        let checksum = resolve_checksum(Some(supplied.clone()), b"irrelevant")
            .expect("a 32-byte checksum must be accepted as-is");
        assert_eq!(checksum, supplied);
    }

    #[test]
    fn resolve_checksum_rejects_wrong_length_before_any_network_call() {
        let error = resolve_checksum(Some(vec![1u8; 10]), b"irrelevant")
            .expect_err("a 10-byte checksum must be rejected");
        // EN: A checksum of the wrong length is a client-side rule, so it
        // must surface as `RociaDbError::Validation`, not a catch-all
        // variant, with a message precise enough to act on.
        // FR: Un checksum de mauvaise longueur est une regle cote client,
        // il doit donc apparaitre en `RociaDbError::Validation`, pas en
        // variante fourre-tout, avec un message assez precis pour agir.
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("32 bytes"));
        assert!(
            message.contains("got 10 bytes"),
            "message should report the actual (wrong) length, got: {message}"
        );
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex pair"))
            .collect()
    }

    #[test]
    fn file_stream_upload_options_have_safe_defaults() {
        let options = FileStreamUploadOptions::default();
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.request_id.is_none());
    }

    #[test]
    fn require_checksum_len_accepts_exactly_32_bytes() {
        require_checksum_len(&[0u8; CHECKSUM_LEN]).expect("32 bytes must be accepted");
    }

    #[test]
    fn require_checksum_len_rejects_wrong_length_before_any_network_call() {
        let error = require_checksum_len(&[1u8; 10]).expect_err("10 bytes must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("32 bytes"));
        assert!(message.contains("got 10 bytes"));
    }

    #[test]
    fn validate_file_size_accepts_exactly_the_5_gib_limit() {
        validate_file_size(MAX_FILE_BYTES).expect("exactly the limit must be accepted");
    }

    #[test]
    fn validate_file_size_rejects_one_byte_over_the_5_gib_limit() {
        let error = validate_file_size(MAX_FILE_BYTES + 1)
            .expect_err("one byte over the limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("5 GiB"));
    }

    /// EN: Drives [`rechunk_upload_requests`] to completion against an
    /// in-memory source and returns both the produced requests and
    /// whatever validation error, if any, ended up in `error_slot`. No
    /// network, no tokio runtime needed: `stream::iter` resolves
    /// synchronously, so `futures::executor::block_on` alone is enough to
    /// drive the `stream::unfold` chain to its end.
    /// FR: Pilote [`rechunk_upload_requests`] jusqu au bout contre une
    /// source en memoire et retourne a la fois les requetes produites et
    /// l eventuelle erreur de validation atterrie dans `error_slot`. Ni
    /// reseau ni runtime tokio necessaires : `stream::iter` se resout de
    /// facon synchrone, donc `futures::executor::block_on` seul suffit a
    /// derouler la chaine `stream::unfold` jusqu a sa fin.
    fn collect_rechunked(
        size_bytes: u64,
        source_pieces: Vec<Vec<u8>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>) {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                size_bytes,
                "application/octet-stream".into(),
                vec![0u8; CHECKSUM_LEN],
                "req".into(),
                stream::iter(source_pieces),
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        let error = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        (requests, error)
    }

    #[test]
    fn rechunk_exact_multiple_of_one_mebibyte_has_no_trailing_empty_chunk() {
        let total = DEFAULT_CHUNK_SIZE * 2;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![5u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(
            requests.len(),
            2,
            "an exact multiple of the chunk size must not emit a trailing empty request"
        );
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), DEFAULT_CHUNK_SIZE);
        // EN: Only the first message carries metadata, exactly like
        // `chunk_upload_requests`.
        // FR: Seul le premier message porte les metadonnees, exactement
        // comme `chunk_upload_requests`.
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].size_bytes, total as u64);
        assert_eq!(requests[1].size_bytes, 0);
    }

    #[test]
    fn rechunk_non_multiple_ends_with_a_short_last_chunk() {
        let total = DEFAULT_CHUNK_SIZE + 100;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![9u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 100);
    }

    #[test]
    fn rechunk_reassembles_many_small_source_pieces_byte_for_byte() {
        // EN: Feed the re-chunker a source split into many small (64 KiB)
        // pieces — nothing like the 1 MiB output chunk size — with a
        // distinctive byte pattern so any misordering or off-by-one
        // slicing bug would be caught, not just the total byte count.
        // FR: Alimente le re-decoupeur avec une source fragmentee en
        // beaucoup de petits morceaux (64 KiB) — rien a voir avec la
        // taille de chunk de sortie de 1 MiB — avec un motif d octets
        // distinctif pour qu un bug de reordonnancement ou de decoupage
        // off-by-one soit detecte, pas seulement le total d octets.
        let piece_len = 64 * 1024;
        let piece_count = 40; // ~2.5 MiB total: spans multiple 1 MiB output chunks
        let mut expected = Vec::new();
        let mut pieces = Vec::new();
        for i in 0..piece_count {
            let piece: Vec<u8> = (0..piece_len).map(|b| ((i * 7 + b) % 256) as u8).collect();
            expected.extend_from_slice(&piece);
            pieces.push(piece);
        }
        let total = expected.len() as u64;
        let (requests, error) = collect_rechunked(total, pieces);
        assert!(error.is_none(), "unexpected validation error: {error:?}");

        let reassembled: Vec<u8> = requests.iter().flat_map(|r| r.chunk.clone()).collect();
        assert_eq!(
            reassembled, expected,
            "reassembled bytes must exactly match the source, regardless of how it was chunked \
             on input"
        );

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty());
        assert!(last.chunk.len() <= DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_zero_byte_file_still_emits_one_metadata_carrying_request() {
        let (requests, error) = collect_rechunked(0, vec![]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].chunk.is_empty());
        assert_eq!(requests[0].size_bytes, 0);
        assert_eq!(
            requests[0].tenant_id, "tenant",
            "the sole request of an empty upload must still carry file metadata, otherwise the \
             server never learns about the file"
        );
    }

    #[test]
    fn rechunk_rejects_more_data_than_declared_size_bytes_before_sending_the_offending_chunk() {
        let declared = DEFAULT_CHUNK_SIZE as u64; // caller declares only 1 MiB
        // the source produces 2 MiB in a single piece
        let (requests, error) =
            collect_rechunked(declared, vec![vec![1u8; DEFAULT_CHUNK_SIZE * 2]]);
        let error = error.expect("an overflow must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("more data than size_bytes"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert!(
            sent <= declared as usize,
            "the chunk that would push the total past size_bytes must never be sent, got \
             {sent} bytes sent for a {declared}-byte declared size"
        );
    }

    #[test]
    fn rechunk_reports_a_shortfall_once_the_source_is_exhausted() {
        let declared = (DEFAULT_CHUNK_SIZE * 2) as u64; // caller declares 2 MiB
        // the source only ever produces 1 MiB
        let (requests, error) = collect_rechunked(declared, vec![vec![3u8; DEFAULT_CHUNK_SIZE]]);
        let error = error.expect("a shortfall must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("sent"));
        assert!(message.contains("but size_bytes declared"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert_eq!(sent, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_honors_caller_supplied_request_id_and_content_type_on_the_first_request_only() {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                10,
                "text/csv".into(),
                vec![0u8; CHECKSUM_LEN],
                "caller-request-id".into(),
                stream::iter(vec![vec![1u8; 10]]),
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        assert!(
            error_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].content_type, "text/csv");
        assert_eq!(requests[0].request_id, "caller-request-id");
    }
}
