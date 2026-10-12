use std::{collections::BTreeSet, error::Error};

use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CatalogName, CollectorInstanceId, CollectorInstanceStatus,
    ErrorCode, ErrorStatus, FencingToken, ImplementationPool, InstanceLifecycleState, LeaseId, Measurement,
    PositiveMeasurement, PrincipalId, PrincipalIssuer, PrincipalScope, ReceiptId, ReceiptState, ReceiptStatus,
    ReceiptTransitionError, SchemaFingerprint, SchemaName, TableIdentity, TableLease, TableLeaseSuccessorError,
    TableName, UnixMillis,
};

fn table() -> Result<TableIdentity, Box<dyn Error>> {
    Ok(TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new("traces")?,
    ))
}

fn instance_id() -> Result<CollectorInstanceId, Box<dyn Error>> {
    Ok("11111111-1111-4111-8111-111111111111".parse()?)
}

fn lease_id() -> Result<LeaseId, Box<dyn Error>> {
    Ok("22222222-2222-4222-8222-222222222222".parse()?)
}

#[test]
fn receipt_transitions_require_state_specific_payloads() -> Result<(), Box<dyn Error>> {
    let receipt_id: ReceiptId = "33333333-3333-4333-8333-333333333333".parse()?;
    let accepted = ReceiptStatus::accepted(receipt_id, table()?, ImplementationPool::Direct, UnixMillis::new(100)?);
    let buffered = accepted.transition_to(ReceiptState::Buffered, UnixMillis::new(110)?, None, None)?;
    let publishing = buffered.transition_to(ReceiptState::Publishing, UnixMillis::new(120)?, None, None)?;

    let lease = TableLease::new(
        lease_id()?,
        table()?,
        instance_id()?,
        ImplementationPool::Direct,
        FencingToken::new(42)?,
        UnixMillis::new(90)?,
        UnixMillis::new(200)?,
    )?;
    let publication = arrowcol_protocol::DeltaPublicationResult::new(
        Measurement::new(7)?,
        Measurement::new(2)?,
        Measurement::new(1_000)?,
        Measurement::new(65_536)?,
        SchemaFingerprint::from_bytes([7; 32]),
        UnixMillis::new(130)?,
        lease.publication_fence(),
    );
    let published =
        publishing.transition_to(ReceiptState::Published, UnixMillis::new(130)?, Some(publication), None)?;
    assert_eq!(published.state(), ReceiptState::Published);
    assert_eq!(published.revision().get(), 3);
    assert!(published.publication().is_some());

    assert!(matches!(
        accepted.transition_to(ReceiptState::Published, UnixMillis::new(120)?, None, None),
        Err(ReceiptTransitionError::Invalid { .. })
    ));
    Ok(())
}

#[test]
fn uncertain_publication_requires_reconciliation_code() -> Result<(), Box<dyn Error>> {
    let accepted = ReceiptStatus::accepted(
        ReceiptId::new(),
        table()?,
        ImplementationPool::Dataflow,
        UnixMillis::new(100)?,
    );
    let publishing = accepted.transition_to(ReceiptState::Publishing, UnixMillis::new(101)?, None, None)?;
    let unknown = ErrorStatus::new(
        ErrorCode::PublicationOutcomeUnknown,
        "commit response was lost; reconcile the Delta log",
    )?
    .with_receipt_id(publishing.receipt_id());
    let status = publishing.transition_to(
        ReceiptState::PublicationUnknown,
        UnixMillis::new(102)?,
        None,
        Some(unknown),
    )?;
    assert_eq!(status.state(), ReceiptState::PublicationUnknown);
    assert!(!status.error().ok_or("missing error")?.is_retryable());
    Ok(())
}

#[test]
fn duplicate_deliveries_receive_distinct_server_receipts() {
    let first = ReceiptId::new();
    let second = ReceiptId::new();
    assert_ne!(first, second);
}

#[test]
fn lease_renewal_and_replacement_enforce_fencing() -> Result<(), Box<dyn Error>> {
    let original = TableLease::new(
        lease_id()?,
        table()?,
        instance_id()?,
        ImplementationPool::Direct,
        FencingToken::new(8)?,
        UnixMillis::new(100)?,
        UnixMillis::new(200)?,
    )?;
    let renewal = TableLease::new(
        original.lease_id(),
        table()?,
        original.holder_instance_id(),
        ImplementationPool::Direct,
        original.fencing_token(),
        UnixMillis::new(150)?,
        UnixMillis::new(300)?,
    )?;
    original.validate_successor(&renewal)?;

    let stale_replacement = TableLease::new(
        LeaseId::new(),
        table()?,
        CollectorInstanceId::new(),
        ImplementationPool::Dataflow,
        original.fencing_token(),
        UnixMillis::new(160)?,
        UnixMillis::new(260)?,
    )?;
    assert_eq!(
        original.validate_successor(&stale_replacement),
        Err(TableLeaseSuccessorError::ReplacementNotFenced)
    );
    Ok(())
}

#[test]
fn lifecycle_and_auth_context_validate_transitions_and_expiry() -> Result<(), Box<dyn Error>> {
    let starting = CollectorInstanceStatus::new(
        instance_id()?,
        ImplementationPool::Direct,
        InstanceLifecycleState::Starting,
        UnixMillis::new(10)?,
        None,
    )?;
    let ready = starting.transition_to(InstanceLifecycleState::Ready, UnixMillis::new(20)?, None)?;
    let draining = ready.transition_to(
        InstanceLifecycleState::Draining,
        UnixMillis::new(30)?,
        Some("rolling replacement".to_owned()),
    )?;
    assert_eq!(draining.state(), InstanceLifecycleState::Draining);

    let scopes = BTreeSet::from([
        PrincipalScope::new("arrow.write")?,
        PrincipalScope::new("receipt.read")?,
    ]);
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("client-1")?,
        PrincipalIssuer::new("https://identity.example.test")?,
        scopes,
    )?;
    let context = AuthContext::new(
        principal,
        AuthMethod::BearerToken,
        UnixMillis::new(100)?,
        Some(UnixMillis::new(200)?),
    )?;
    assert!(!context.is_expired_at(UnixMillis::new(199)?));
    assert!(context.is_expired_at(UnixMillis::new(200)?));
    Ok(())
}

#[test]
fn retryability_is_explicit_and_does_not_imply_idempotency() -> Result<(), Box<dyn Error>> {
    let retryable = ErrorStatus::new(ErrorCode::CapacityExhausted, "queue full")?
        .with_retry_after(PositiveMeasurement::new(250)?)?;
    assert!(retryable.is_retryable());
    assert_eq!(retryable.retry_after_ms().map(PositiveMeasurement::get), Some(250));

    let uncertain = ErrorStatus::new(ErrorCode::PublicationOutcomeUnknown, "reconcile receipt")?;
    assert!(!uncertain.is_retryable());
    assert!(uncertain.with_retry_after(PositiveMeasurement::new(250)?).is_err());
    Ok(())
}
