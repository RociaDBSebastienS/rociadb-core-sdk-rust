use crate::pb::upstream::v1::ListTenantsRequest;
use crate::{Page, RociaDbClient, non_empty, page_request};
use anyhow::{Context, Result};

impl RociaDbClient {
    /// Return one paginated page of tenant ids known to the deployment.
    ///
    /// This RPC is not scoped to a tenant: it enumerates the whole deployment
    /// and may be restricted by a dedicated server-side authorization policy.
    pub async fn list_tenants(
        &mut self,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let response = self
            .upstream_tenant
            .list_tenants(ListTenantsRequest {
                page: page_request(limit, cursor)?,
            })
            .await
            .context("failed to list tenants")?
            .into_inner();
        Ok(Page {
            items: response.tenant_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }
}
