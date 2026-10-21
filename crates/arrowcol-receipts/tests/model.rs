use std::sync::Arc;

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator, ValidatedIncomingSchema,
};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, CollectorInstanceId, DeltaPublicationResult, ErrorCode, ErrorStatus, FencingToken,
    ImplementationPool, LeaseId, Measurement, ReceiptId, ReceiptState, SchemaFingerprint, TableLease, UnixMillis,
    fingerprint_schema,
};
use arrowcol_receipts::{
    AdoptionCandidate, FailureDisposition, LeasePublicationFence, ReceiptAdoption, ReceiptErrorKind, ReceiptFailure,
    ReceiptRecord, ValidatedDelivery,
};

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

/// Scenario: a collector persists a validated delivery's getter values (table
/// id and identity, schema version and fingerprint, rows, batches, Arrow
/// bytes) to its local durable buffer and rebuilds the delivery after a
/// restart without the original `ValidatedIncomingSchema`.
/// Guarantees: `ValidatedDelivery::recovered` equals `ValidatedDelivery::new`
/// for the same validated schema and counts, and rejects zero batches and
/// counts beyond the shared measurement range exactly as `new` does.
#[tokio::test]
async fn recovered_delivery_equals_validated_delivery_and_shares_its_rules() -> anyhow::Result<()> {
    let validated = validated_schema("recovered_test").await?;
    let delivery = ValidatedDelivery::new(&validated, 3, 2, 24)?;
    let persisted = (
        delivery.table_id(),
        delivery.table().clone(),
        delivery.schema_version(),
        delivery.schema_fingerprint(),
        delivery.rows().get(),
        delivery.batches().get(),
        delivery.arrow_memory_bytes().get(),
    );
    let recover = |rows: u64, batches: u64, bytes: u64| {
        ValidatedDelivery::recovered(
            persisted.0,
            persisted.1.clone(),
            persisted.2,
            persisted.3,
            rows,
            batches,
            bytes,
        )
    };
    assert_eq!(recover(persisted.4, persisted.5, persisted.6)?, delivery);
    let registered = validated.registered();
    assert_eq!(
        ValidatedDelivery::recovered(
            registered.table_id(),
            registered.identity().clone(),
            registered.version(),
            registered.fingerprint(),
            3,
            2,
            24,
        )?,
        delivery
    );

    for (rows, batches, bytes) in [(3, 0, 24), (u64::MAX, 1, 24), (3, u64::MAX, 24), (3, 1, u64::MAX)] {
        let recovered = recover(rows, batches, bytes).err().map(|error| error.kind());
        let admitted = ValidatedDelivery::new(&validated, rows, batches, bytes)
            .err()
            .map(|error| error.kind());
        assert_eq!(recovered, Some(ReceiptErrorKind::InvalidConfiguration));
        assert_eq!(recovered, admitted);
    }
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

/// Scenario: adoption requests with a lease held by another owner, a deadline
/// at or before the adoption time or past the table lease, a candidate that
/// expects an inactive state or a revision that disagrees with its state, a
/// duplicated candidate, or no candidates at all.
/// Guarantees: validation fails closed with fenced or invalid-configuration
/// errors before any store work and accepts well-formed or empty requests.
#[test]
fn adoption_requests_validate_lease_deadline_and_candidates() -> anyhow::Result<()> {
    let successor = CollectorInstanceId::new();
    let lease = test_lease(successor)?;
    let candidate = AdoptionCandidate::new(ReceiptId::new(), CollectorInstanceId::new(), ReceiptState::Buffered, 1);
    let valid = ReceiptAdoption {
        successor,
        lease: lease.clone(),
        lease_deadline: UnixMillis::new(9_000)?,
        adopted_at: UnixMillis::new(2_000)?,
        receipts: vec![
            candidate,
            AdoptionCandidate::new(ReceiptId::new(), successor, ReceiptState::Accepted, 0),
            AdoptionCandidate::new(ReceiptId::new(), successor, ReceiptState::Publishing, 2),
        ],
    };
    valid.validate()?;
    ReceiptAdoption {
        receipts: Vec::new(),
        ..valid.clone()
    }
    .validate()?;
    ReceiptAdoption {
        lease_deadline: lease.expires_at_unix_ms(),
        ..valid.clone()
    }
    .validate()?;

    let foreign = ReceiptAdoption {
        lease: test_lease(CollectorInstanceId::new())?,
        ..valid.clone()
    };
    assert_eq!(rejection(&foreign), Some(ReceiptErrorKind::Fenced));
    for (adopted_at, lease_deadline) in [(2_000, 2_000), (3_000, 2_000), (2_000, 10_001)] {
        let request = ReceiptAdoption {
            adopted_at: UnixMillis::new(adopted_at)?,
            lease_deadline: UnixMillis::new(lease_deadline)?,
            ..valid.clone()
        };
        assert_eq!(rejection(&request), Some(ReceiptErrorKind::InvalidConfiguration));
    }
    for (state, revision) in [
        (ReceiptState::Published, 3),
        (ReceiptState::Failed, 1),
        (ReceiptState::PublicationUnknown, 3),
        (ReceiptState::Buffered, 0),
        (ReceiptState::Accepted, 1),
        (ReceiptState::Publishing, 1),
    ] {
        let request = ReceiptAdoption {
            receipts: vec![AdoptionCandidate::new(ReceiptId::new(), successor, state, revision)],
            ..valid.clone()
        };
        assert_eq!(rejection(&request), Some(ReceiptErrorKind::InvalidConfiguration));
    }
    let duplicated = ReceiptAdoption {
        receipts: vec![candidate, candidate],
        ..valid
    };
    assert_eq!(rejection(&duplicated), Some(ReceiptErrorKind::InvalidConfiguration));
    Ok(())
}

/// Scenario: a successor derives adoption expectations from durable receipt
/// records and compares them with records that later changed.
/// Guarantees: `from_record` captures the receipt, owner, state, and revision,
/// and `matches` detects any change of receipt, owner, state, or revision.
#[test]
fn adoption_candidates_capture_and_match_current_receipt_identity() -> anyhow::Result<()> {
    let owner = CollectorInstanceId::new();
    let lease = test_lease(owner)?;
    let record = test_record(owner, &lease, ReceiptState::Buffered, 1)?;
    let candidate = AdoptionCandidate::from_record(&record);
    assert_eq!(
        candidate,
        AdoptionCandidate::new(record.receipt_id, owner, ReceiptState::Buffered, 1)
    );
    assert!(candidate.matches(&record));

    let mut other_receipt = record.clone();
    other_receipt.receipt_id = ReceiptId::new();
    let mut other_owner = record.clone();
    other_owner.owner_instance_id = CollectorInstanceId::new();
    let mut advanced = record.clone();
    advanced.state = ReceiptState::Publishing;
    advanced.revision = 2;
    let mut failed = record.clone();
    failed.state = ReceiptState::Failed;
    failed.revision = 2;
    for changed in [other_receipt, other_owner, advanced, failed] {
        assert!(!candidate.matches(&changed));
    }
    Ok(())
}

async fn validated_schema(table: &str) -> anyhow::Result<ValidatedIncomingSchema> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.receipts.{table} (value BIGINT NOT NULL)"
        )))
        .await?;
    let schema = store
        .current_schema(completion.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("schema missing"))?
        .arrow_schema()?;
    Ok(SchemaValidator::new(store)
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?)
}

fn rejection(request: &ReceiptAdoption) -> Option<ReceiptErrorKind> {
    request.validate().err().map(|error| error.kind())
}

fn test_lease(holder: CollectorInstanceId) -> anyhow::Result<TableLease> {
    Ok(TableLease::new(
        LeaseId::new(),
        arrowcol_protocol::TableIdentity::new(
            arrowcol_protocol::CatalogName::new("telemetry")?,
            arrowcol_protocol::SchemaName::new("receipts")?,
            arrowcol_protocol::TableName::new("adoption")?,
        ),
        holder,
        ImplementationPool::Direct,
        FencingToken::new(4)?,
        UnixMillis::new(1_000)?,
        UnixMillis::new(10_000)?,
    )?)
}

fn test_record(
    owner: CollectorInstanceId,
    lease: &TableLease,
    state: ReceiptState,
    revision: u64,
) -> anyhow::Result<ReceiptRecord> {
    Ok(ReceiptRecord {
        receipt_id: ReceiptId::new(),
        table_id: uuid::Uuid::new_v4(),
        table: lease.table().clone(),
        implementation_pool: ImplementationPool::Direct,
        owner_instance_id: owner,
        lease_id: lease.lease_id(),
        fencing_epoch: lease.fencing_token(),
        schema_version: arrowcol_catalog::SchemaVersion::new(1)?,
        schema_fingerprint: SchemaFingerprint::from_bytes([7; 32]),
        state,
        failure_disposition: None,
        revision,
        rows: Measurement::new(3)?,
        batches: Measurement::new(1)?,
        arrow_memory_bytes: Measurement::new(24)?,
        lease_deadline_unix_ms: UnixMillis::new(9_000)?,
        heartbeat_at_unix_ms: UnixMillis::new(3_000)?,
        created_at_unix_ms: UnixMillis::new(2_000)?,
        updated_at_unix_ms: UnixMillis::new(3_000)?,
        publication: None,
        error: None,
    })
}
