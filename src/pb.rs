//! EN: Generated protobuf types and gRPC clients.
//!
//! This module is **not** part of the SDK's semver contract: it is
//! regenerated from `proto/upstream/v1/upstream.proto` by prost/tonic, and
//! a routine prost or tonic upgrade can change field types, add fields, or
//! otherwise reshape these generated types without the SDK's own API
//! changing at all. It stays `pub` (rather than `pub(crate)`) only because
//! the crate's own submodules need `crate::pb::...` paths across module
//! boundaries; `#[doc(hidden)]` keeps it out of the published docs and out
//! of the surface callers should treat as stable.
//!
//! The handful of generated types that genuinely are part of the public
//! contract — because they appear in a public method signature and callers
//! need to name them — are re-exported individually at the crate root
//! instead: [`crate::CollectionInfo`], [`crate::StatResponse`],
//! [`crate::Neighbor`], [`crate::UploadRequest`], and
//! [`crate::DownloadResponse`]. Depend on those re-exports, not on paths
//! reaching into `pb` directly.
//! FR: Types protobuf generes et clients gRPC.
//!
//! Ce module ne fait **pas** partie du contrat semver du SDK : il est
//! regenere depuis `proto/upstream/v1/upstream.proto` par prost/tonic, et
//! une montee de version courante de prost ou tonic peut changer des types
//! de champs, ajouter des champs, ou autrement remanier ces types generes
//! sans que l API du SDK elle-meme ne change. Il reste `pub` (plutot que
//! `pub(crate)`) uniquement parce que les sous-modules du crate ont besoin
//! de chemins `crate::pb::...` d un module a l autre ; `#[doc(hidden)]` le
//! garde hors de la documentation publiee et hors de la surface que les
//! appelants doivent considerer stable.
//!
//! La poignee de types generes qui font reellement partie du contrat
//! public — car ils apparaissent dans une signature de methode publique et
//! les appelants ont besoin de les nommer — sont reexportes
//! individuellement a la racine du crate a la place :
//! [`crate::CollectionInfo`], [`crate::StatResponse`], [`crate::Neighbor`],
//! [`crate::UploadRequest`], et [`crate::DownloadResponse`]. Dependez de
//! ces reexports, pas de chemins qui plongent directement dans `pb`.
pub mod upstream {
    /// EN: Generated code for the rocia.v1 API.
    /// FR: Code genere pour l API rocia.v1.
    pub mod v1 {
        tonic::include_proto!("rocia.v1");
    }
}
