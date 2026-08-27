use crate::RociaDbClient;
use crate::pb::upstream::v1::{DeleteDocRequest, PutDocRequest};
use anyhow::{Context, Result};
use serde::Serialize;
use uuid::Uuid;

impl RociaDbClient {
    /// Create or replace one document without creating a graph binding.
    pub async fn put_document<T: Serialize + ?Sized>(
        &mut self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        value: &T,
    ) -> Result<()> {
        self.put_document_with_request_id(
            tenant_id,
            collection,
            document_id,
            value,
            format!("put_document:{collection}:{}", Uuid::new_v4()),
        )
        .await
    }

    /// Create or replace one document with a caller-provided idempotency key.
    pub async fn put_document_with_request_id<T: Serialize + ?Sized>(
        &mut self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        value: &T,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let json = serde_json::to_vec(value).context("failed to encode document json")?;
        self.upstream_document
            .put_doc(PutDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection.to_string(),
                id: document_id.to_string(),
                json,
                request_id: request_id.into(),
            })
            .await
            .context("failed to put document")?;
        Ok(())
    }

    /// Delete one document using an automatically generated idempotency key.
    pub async fn delete_document(
        &mut self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
    ) -> Result<()> {
        self.delete_document_with_request_id(
            tenant_id,
            collection,
            document_id,
            format!("delete_document:{collection}:{}", Uuid::new_v4()),
        )
        .await
    }

    /// Delete one document with a caller-provided idempotency key.
    pub async fn delete_document_with_request_id(
        &mut self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        self.upstream_document
            .delete_doc(DeleteDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection.to_string(),
                id: document_id.to_string(),
                request_id: request_id.into(),
            })
            .await
            .context("failed to delete document")?;
        Ok(())
    }
}
