use std::sync::Arc;

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator,
};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, CollectorInstanceId, DeltaPublicationResult, ErrorCode, ErrorStatus, FencingToken,
    ImplementationPool, LeaseId, Measurement, ReceiptId, ReceiptState, TableLease, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{FailureDisposition, LeasePublicationFence, ReceiptFailure, ReceiptRecord, ValidatedDelivery};

#[tokio::test]
async fn validated_delivery_and_protocol_status_reuse_shared_contracts() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE telemetry.receipts.model_test (value BIGINT NOT NULL)",
        ))
        .await?;
    let schema = store
        .current_schema(completion.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("schema missing"))?
        .arrow_schema()?;
    let validated = SchemaValidator::new(store)
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?;
    let delivery = ValidatedDelivery::new(&validated, 3, 1, 24)?;
    assert_eq!(delivery.table_id(), validated.registered().table_id());
    assert_eq!(delivery.schema_fingerprint(), fingerprint_schema(&schema));

    let owner = CollectorInstanceId::new();
    let lease = TableLease::new(
        LeaseId::new(),
        delivery.table().clone(),
        owner,
        ImplementationPool::Direct,
        FencingToken::new(7)?,
        UnixMillis::new(1_000)?,
        UnixMillis::new(10_000)?,
    )?;
    let publication = DeltaPublicationResult::new(
        Measurement::new(12)?,
        Measurement::new(1)?,
        Measurement::new(3)?,
        Measurement::new(18)?,
        delivery.schema_fingerprint(),
        UnixMillis::new(4_000)?,
        lease.publication_fence(),
    );
    let record = ReceiptRecord {
        receipt_id: ReceiptId::new(),
        table_id: delivery.table_id(),
        table: delivery.table().clone(),
        implementation_pool: ImplementationPool::Direct,
        owner_instance_id: owner,
        lease_id: lease.lease_id(),
        fencing_epoch: lease.fencing_token(),
        schema_version: delivery.schema_version(),
        schema_fingerprint: delivery.schema_fingerprint(),
        state: ReceiptState::Published,
        failure_disposition: None,
        revision: 3,
        rows: delivery.rows(),
        batches: delivery.batches(),
        arrow_memory_bytes: delivery.arrow_memory_bytes(),
        lease_deadline_unix_ms: UnixMillis::new(9_000)?,
        heartbeat_at_unix_ms: UnixMillis::new(3_000)?,
        created_at_unix_ms: UnixMillis::new(2_000)?,
        updated_at_unix_ms: UnixMillis::new(4_000)?,
        publication: Some(publication),
        error: None,
    };
    assert!(record.can_client_purge());
    assert_eq!(record.protocol_status()?.state(), ReceiptState::Published);
    assert_eq!(record.publication_fence()?, lease.publication_fence());
    Ok(())
}

#[test]
fn failure_disposition_must_match_protocol_retryability() -> anyhow::Result<()> {
    let retryable = ErrorStatus::new(ErrorCode::PublicationFailed, "retry later")?;
    assert!(ReceiptFailure::new(FailureDisposition::Retryable, retryable.clone()).is_ok());
    assert!(ReceiptFailure::new(FailureDisposition::Permanent, retryable).is_err());

    let permanent = ErrorStatus::new(ErrorCode::PublicationOutcomeUnknown, "reconcile first")?;
    assert!(ReceiptFailure::new(FailureDisposition::Permanent, permanent).is_ok());
    Ok(())
}

#[test]
fn cached_delta_fence_rejects_invalid_successors() -> anyhow::Result<()> {
    let table = arrowcol_protocol::TableIdentity::new(
        arrowcol_protocol::CatalogName::new("telemetry")?,
        arrowcol_protocol::SchemaName::new("receipts")?,
        arrowcol_protocol::TableName::new("fence")?,
    );
    let owner = CollectorInstanceId::new();
    let lease = TableLease::new(
        LeaseId::new(),
        table.clone(),
        owner,
        ImplementationPool::Direct,
        FencingToken::new(1)?,
        UnixMillis::new(1_000)?,
        UnixMillis::new(2_000)?,
    )?;
    let fence = LeasePublicationFence::new(lease);
    let replacement = TableLease::new(
        LeaseId::new(),
        table,
        owner,
        ImplementationPool::Direct,
        FencingToken::new(1)?,
        UnixMillis::new(1_500)?,
        UnixMillis::new(3_000)?,
    )?;
    assert!(fence.update(replacement).is_err());
    fence.invalidate("lease heartbeat failed");
    Ok(())
}
