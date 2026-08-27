//! EN: File upload/download helpers.
//!
//! The upstream server stores every file as a fixed sequence of 1 MiB
//! (1_048_576-byte) chunks and, on download, always replays
//! `ceil(size_bytes / 1 MiB)` chunk indexes, regardless of how many upload
//! messages were actually sent. Uploading with any chunk size other than
//! exactly 1 MiB (except the last, shorter, chunk) makes a later download
//! silently return truncated or garbled data: there is no server-side
//! error, because the server has no way to know the upload used a
//! different chunking scheme.
//!
//! [`RociaDbClient::upload_file`] always honors this contract. Only reach
//! for [`RociaDbClient::upload_file_stream`] directly if you understand and
//! reproduce it yourself.
//! FR: Aides pour l upload/download de fichiers.
//!
//! Le serveur upstream stocke chaque fichier en une sequence fixe de
//! chunks de 1 MiB (1_048_576 octets) et, au download, relit toujours les
//! index `ceil(size_bytes / 1 MiB)`, quel que soit le nombre de messages
//! reellement envoyes a l upload. Uploader avec une taille de chunk
//! differente de 1 MiB exactement (sauf le dernier, plus court) fait qu un
//! download ulterieur renvoie silencieusement des donnees tronquees ou
//! corrompues : il n y a pas d erreur cote serveur, car le serveur n a
//! aucun moyen de savoir que l upload a utilise un decoupage different.
//!
//! [`RociaDbClient::upload_file`] respecte toujours ce contrat. N utilisez
//! [`RociaDbClient::upload_file_stream`] directement que si vous
//! comprenez ce contrat et le reproduisez vous-meme.
use crate::pb::upstream::v1::{
    DeleteRequest, DownloadRequest, DownloadResponse, ListBucketsRequest, ListFilesRequest,
    StatRequest, StatResponse, UploadRequest,
};
use crate::{Page, RociaDbClient, non_empty, page_request};
use anyhow::{Context, Result, ensure};
use futures::{Stream, stream};
use sha2::{Digest, Sha256};
use tonic::codec::Streaming;
use uuid::Uuid;

/// EN: Chunk size the server stores files with (`CHUNK_SIZE` server-side).
/// Not configurable: see the module docs for why any other size corrupts
/// downloads.
/// FR: Taille de chunk avec laquelle le serveur stocke les fichiers
/// (`CHUNK_SIZE` cote serveur). Non configurable : voir la doc du module
/// pour la raison pour laquelle toute autre taille corrompt les downloads.
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
/// EN: There is intentionally no `chunk_size` knob: the server stores and
/// replays files in fixed 1 MiB chunks, so [`RociaDbClient::upload_file`]
/// always emits exactly-1-MiB chunks (the last one may be shorter). See the
/// module docs for what goes wrong if that ever changes.
/// FR: Il n y a volontairement pas de reglage `chunk_size` : le serveur
/// stocke et relit les fichiers par chunks fixes de 1 MiB, donc
/// [`RociaDbClient::upload_file`] emet toujours des chunks d exactement 1
/// MiB (le dernier peut etre plus court). Voir la doc du module pour ce
/// qui se passe si cela change.
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
    /// - every message's `chunk` must be exactly 1 MiB (1_048_576 bytes),
    ///   except the last one, which may be shorter;
    /// - `content_type` and `checksum` on messages after the first are
    ///   ignored by the server and can be left empty.
    ///
    /// Any deviation (wrong chunk size, missing or short checksum,
    /// mismatched `size_bytes`) either fails the upload outright or, worse,
    /// silently corrupts a *later* download: the server always replays
    /// `ceil(size_bytes / 1 MiB)` chunk indexes, regardless of how many
    /// messages were actually sent.
    ///
    /// For the common case — uploading an in-memory byte buffer — use
    /// [`RociaDbClient::upload_file`] instead, which builds a correct
    /// stream for you.
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
    /// - le champ `chunk` de chaque message doit faire exactement 1 MiB
    ///   (1_048_576 octets), sauf le dernier qui peut etre plus court ;
    /// - `content_type` et `checksum` sur les messages suivants sont
    ///   ignores par le serveur et peuvent rester vides.
    ///
    /// Tout ecart (mauvaise taille de chunk, checksum absent ou trop
    /// court, `size_bytes` incoherent) fait soit echouer l upload
    /// directement, soit pire, corrompt silencieusement un download
    /// *ulterieur* : le serveur relit toujours `ceil(size_bytes / 1 MiB)`
    /// index de chunk, quel que soit le nombre de messages reellement
    /// envoyes.
    ///
    /// Pour le cas courant — uploader un buffer d octets en memoire —
    /// utilisez plutot [`RociaDbClient::upload_file`], qui construit un
    /// flux correct pour vous.
    pub async fn upload_file_stream<S>(&mut self, requests: S) -> Result<()>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        self.upstream_file
            .upload(requests)
            .await
            .context("failed to upload file")?;
        Ok(())
    }

    /// EN: Upload an in-memory byte buffer, split into gRPC messages that
    /// match the server's fixed on-disk chunking exactly.
    ///
    /// The buffer is always split into 1 MiB (1_048_576-byte) chunks,
    /// matching the server's `CHUNK_SIZE` (the last chunk may be shorter).
    /// This is not configurable: see the module docs for what goes wrong
    /// with any other chunk size. When `options.checksum` is `None`, the
    /// SHA-256 digest of `bytes` is computed and sent automatically; when
    /// it is `Some`, it must be exactly 32 bytes or this returns an error
    /// before any network call. Files over 5 GiB
    /// (`limits.max_file_bytes`, the server default) are rejected
    /// client-side with a clear error instead of failing partway through
    /// the upload.
    /// FR: Upload un buffer d octets en memoire, decoupe en messages gRPC
    /// qui correspondent exactement au decoupage fixe du serveur sur
    /// disque.
    ///
    /// Le buffer est toujours decoupe en chunks de 1 MiB (1_048_576
    /// octets), comme le `CHUNK_SIZE` du serveur (le dernier chunk peut
    /// etre plus court). Ce n est pas configurable : voir la doc du module
    /// pour ce qui se passe avec une autre taille de chunk. Quand
    /// `options.checksum` vaut `None`, le digest SHA-256 de `bytes` est
    /// calcule et envoye automatiquement ; quand il vaut `Some`, il doit
    /// faire exactement 32 octets sinon retourne une erreur avant tout
    /// appel reseau. Les fichiers de plus de 5 GiB
    /// (`limits.max_file_bytes`, valeur par defaut cote serveur) sont
    /// rejetes cote client avec une erreur claire plutot que de faire
    /// echouer l upload en cours de route.
    pub async fn upload_file(
        &mut self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: impl AsRef<[u8]>,
        options: FileUploadOptions,
    ) -> Result<()> {
        let bytes = bytes.as_ref();
        let size_bytes = u64::try_from(bytes.len()).context("file is too large")?;
        ensure!(
            size_bytes <= MAX_FILE_BYTES,
            "file is {size_bytes} bytes, which exceeds the server's {MAX_FILE_BYTES}-byte \
             (5 GiB) limit"
        );

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

    /// Start a server-streaming download without buffering the complete file.
    pub async fn download_file_stream(
        &mut self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Streaming<DownloadResponse>> {
        Ok(self
            .upstream_file
            .download(DownloadRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .context("failed to start file download")?
            .into_inner())
    }

    /// Download a complete file into memory.
    pub async fn download_file(
        &mut self,
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
            .context("file download stream failed")?
        {
            bytes.extend_from_slice(&response.chunk);
        }
        Ok(bytes)
    }

    /// Return metadata for one stored file.
    pub async fn stat_file(
        &mut self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<StatResponse> {
        Ok(self
            .upstream_file
            .stat(StatRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .context("failed to stat file")?
            .into_inner())
    }

    /// Return one paginated page of bucket names holding at least one file.
    pub async fn list_buckets(
        &mut self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let response = self
            .upstream_file
            .list_buckets(ListBucketsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .context("failed to list buckets")?
            .into_inner();
        Ok(Page {
            items: response.buckets,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of file ids stored in one bucket.
    pub async fn list_files(
        &mut self,
        tenant_id: &str,
        bucket: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let response = self
            .upstream_file
            .list_files(ListFilesRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .context("failed to list files")?
            .into_inner();
        Ok(Page {
            items: response.file_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Delete one stored file using an automatically generated idempotency key.
    pub async fn delete_file(
        &mut self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<()> {
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
        &mut self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        self.upstream_file
            .delete(DeleteRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
                request_id: request_id.into(),
            })
            .await
            .context("failed to delete file")?;
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
            ensure!(
                checksum.len() == CHECKSUM_LEN,
                "checksum must be exactly {CHECKSUM_LEN} bytes (sha256), got {} bytes",
                checksum.len()
            );
            Ok(checksum)
        }
        None => Ok(Sha256::digest(bytes).to_vec()),
    }
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

#[cfg(test)]
mod tests {
    use super::{
        CHECKSUM_LEN, DEFAULT_CHUNK_SIZE, FileUploadOptions, chunk_upload_requests,
        resolve_checksum,
    };

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

    /// EN: Asserts that chunking `total_bytes` matches the server's exact
    /// on-disk contract: every chunk but the last is exactly 1 MiB, and the
    /// sum of chunk bytes equals `size_bytes` — anything else either fails
    /// the upload outright (`chunk exceeds 1 MiB`, `size_bytes does not
    /// match uploaded data`) or silently corrupts a later download (the
    /// server replays `ceil(size_bytes / 1 MiB)` fixed-size chunk indexes
    /// regardless of how the upload was actually sliced).
    /// FR: Verifie que le decoupage de `total_bytes` respecte exactement le
    /// contrat de stockage du serveur : chaque chunk sauf le dernier fait
    /// exactement 1 MiB, et la somme des octets de chunk egale
    /// `size_bytes` — tout le reste fait soit echouer l upload directement
    /// (`chunk exceeds 1 MiB`, `size_bytes does not match uploaded data`),
    /// soit corrompt silencieusement un download ulterieur (le serveur
    /// relit `ceil(size_bytes / 1 MiB)` index de chunk de taille fixe, quel
    /// que soit le decoupage reellement utilise a l upload).
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
        assert!(error.to_string().contains("32 bytes"));
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex pair"))
            .collect()
    }
}
