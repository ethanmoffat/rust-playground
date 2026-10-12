use std::error::Error;

use arrowcol_protocol::{
    AuthContext, CollectorCapabilities, CollectorInstanceStatus, ErrorStatus, PressureSnapshot, ReceiptStatus,
    TableLease,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

fn assert_snapshot<T>(fixture: &str) -> Result<(), Box<dyn Error>>
where
    T: DeserializeOwned + Serialize,
{
    let expected: Value = serde_json::from_str(fixture)?;
    let decoded: T = serde_json::from_str(fixture)?;
    let encoded = serde_json::to_value(decoded)?;
    assert_eq!(encoded, expected);
    Ok(())
}

#[test]
fn v1_json_fixtures_round_trip_as_wire_snapshots() -> Result<(), Box<dyn Error>> {
    assert_snapshot::<CollectorCapabilities>(include_str!("fixtures/collector-capabilities-v1.json"))?;
    assert_snapshot::<AuthContext>(include_str!("fixtures/auth-context-v1.json"))?;
    assert_snapshot::<PressureSnapshot>(include_str!("fixtures/pressure-snapshot-v1.json"))?;
    assert_snapshot::<TableLease>(include_str!("fixtures/table-lease-v1.json"))?;
    assert_snapshot::<CollectorInstanceStatus>(include_str!("fixtures/instance-status-v1.json"))?;
    assert_snapshot::<ReceiptStatus>(include_str!("fixtures/receipt-accepted-v1.json"))?;
    assert_snapshot::<ReceiptStatus>(include_str!("fixtures/receipt-published-v1.json"))?;
    assert_snapshot::<ErrorStatus>(include_str!("fixtures/error-status-v1.json"))?;
    Ok(())
}

#[test]
fn committed_v1_minimal_receipt_remains_backward_compatible() -> Result<(), Box<dyn Error>> {
    let status: ReceiptStatus = serde_json::from_str(include_str!("fixtures/receipt-accepted-v1.json"))?;
    assert!(status.publication().is_none());
    assert!(status.error().is_none());
    Ok(())
}

#[test]
fn wire_deserialization_revalidates_measurement_and_cross_field_bounds() -> Result<(), Box<dyn Error>> {
    let mut pressure: Value = serde_json::from_str(include_str!("fixtures/pressure-snapshot-v1.json"))?;
    pressure["active_streams"]["current"] = Value::from(257);
    assert!(serde_json::from_value::<PressureSnapshot>(pressure).is_err());

    let mut negative: Value = serde_json::from_str(include_str!("fixtures/pressure-snapshot-v1.json"))?;
    negative["buffered_bytes"]["current"] = Value::from(-1);
    assert!(serde_json::from_value::<PressureSnapshot>(negative).is_err());

    let mut receipt: Value = serde_json::from_str(include_str!("fixtures/receipt-published-v1.json"))?;
    receipt["publication"]["fence"]["table"]["table"] = Value::from("metrics");
    assert!(serde_json::from_value::<ReceiptStatus>(receipt).is_err());

    let mut capabilities: Value = serde_json::from_str(include_str!("fixtures/collector-capabilities-v1.json"))?;
    capabilities["capabilities"] = serde_json::json!(["receipt_status", "receipt_status"]);
    assert!(serde_json::from_value::<CollectorCapabilities>(capabilities).is_err());
    Ok(())
}
