//! Generated protobuf types and gRPC clients.
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
pub mod upstream {
    /// Generated code for the rocia.v1 API.
    pub mod v1 {
        tonic::include_proto!("rocia.v1");
    }
}
