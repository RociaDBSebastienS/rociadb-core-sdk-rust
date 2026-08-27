//! EN: Compile-time proof that a `RociaDbClient` shared behind an `Arc` is
//! usable without a `Mutex`. This is the whole point of the 0.4.0 move from
//! `&mut self` to `&self`: with `&mut self`, none of the calls below would
//! type-check through an `Arc`, and callers had to serialise every request.
//! FR: Preuve a la compilation qu un `RociaDbClient` partage derriere un `Arc`
//! est utilisable sans `Mutex`. C est tout l interet du passage de `&mut self`
//! a `&self` en 0.4.0 : avec `&mut self`, aucun des appels ci-dessous ne
//! compilerait a travers un `Arc`, et l appelant devait serialiser chaque
//! requete.
//!
//! EN: Nothing here runs a request — the functions are never called. They only
//! have to compile.
//! FR: Rien ici n execute de requete — les fonctions ne sont jamais appelees.
//! Elles doivent seulement compiler.

use rocia_db_sdk::{
    DocumentQueryFilter, DocumentQueryOperator, EdgeInput, NodeInput, Result, RociaDbClient,
};
use std::sync::Arc;

#[allow(dead_code)]
async fn reads_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    let _: serde_json::Value = client.get_document("tenant", "products", "sku-1").await?;
    let _ = client.list_collections("tenant", Some(20), None).await?;
    let _ = client.list_graphs("tenant", None, None).await?;
    let _ = client.stat_file("tenant", "assets", "manual.txt").await?;
    let _ = client.list_tenants(None, None).await?;
    Ok(())
}

#[allow(dead_code)]
async fn writes_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    client
        .put_document("tenant", "products", "sku-1", &serde_json::json!({"a": 1}))
        .await?;
    client
        .put_node(
            "tenant",
            "catalog",
            "product:sku-1",
            &serde_json::json!({"a": 1}),
        )
        .await?;
    client
        .delete_document("tenant", "products", "sku-1")
        .await?;
    Ok(())
}

// EN: The shared client must survive being sent across tasks, which is what a
// caller actually does with an `Arc`.
// FR: Le client partage doit survivre a l envoi entre taches, ce qu un appelant
// fait reellement avec un `Arc`.
#[allow(dead_code)]
fn spawns_concurrent_readers(client: Arc<RociaDbClient>) {
    for _ in 0..4 {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let filters = [DocumentQueryFilter {
                field: "active".to_string(),
                operator: DocumentQueryOperator::Eq,
                values: vec![serde_json::json!(true)],
            }];
            let _: Result<rocia_db_sdk::DocumentPage<serde_json::Value>> = client
                .query_documents("tenant", "products", &filters, &[], Some(50), None)
                .await;
        });
    }
}

// EN: The 0.5.0 batch helpers take `impl IntoIterator` on `&self`. That
// combination is the one most likely to stop compiling through an `Arc`, so
// pin it here alongside the single-item calls.
// FR: Les helpers de batch 0.5.0 prennent un `impl IntoIterator` sur `&self`.
// C est la combinaison la plus susceptible de cesser de compiler a travers un
// `Arc`, on la verrouille donc ici a cote des appels unitaires.
#[allow(dead_code)]
async fn batches_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    client
        .put_nodes(
            "tenant",
            "catalog",
            vec![NodeInput {
                node_id: "product:sku-1".to_string(),
                value: serde_json::json!({"sku": "sku-1"}),
                request_id: Some("stable-node-key".to_string()),
            }],
        )
        .await?;
    client
        .add_edges(
            "tenant",
            "catalog",
            vec![EdgeInput {
                edge_id: "membership-1".to_string(),
                from: "product:sku-1".to_string(),
                to: "group:featured".to_string(),
                label: "belongs_to".to_string(),
                value: serde_json::json!({"weight": 1}),
                request_id: Some("stable-edge-key".to_string()),
            }],
        )
        .await?;
    Ok(())
}
