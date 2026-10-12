//! Shared support for the standalone Arrow Flight routing spike.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{BinaryArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

pub const ROUTING_HEADER: &str = "x-arrowcol-table";
pub const REQUEST_ID_HEADER: &str = "x-arrowcol-request-id";
pub const BACKEND_HEADER: &str = "x-arrowcol-backend";
pub const FAIL_AFTER_HEADER: &str = "x-arrowcol-fail-after";
pub const ACK_EVERY_HEADER: &str = "x-arrowcol-ack-every";
pub const BACKEND_DELAY_HEADER: &str = "x-arrowcol-backend-delay-ms";
pub const EXPECTED_BATCHES_HEADER: &str = "x-arrowcol-expected-batches";
pub const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";
pub const MAX_ROUTING_KEY_BYTES: usize = 64;
pub const MAX_REQUEST_ID_BYTES: usize = 64;

pub fn validate_routing_key(value: &str) -> Result<(), String> {
    validate_token(value, MAX_ROUTING_KEY_BYTES, "routing key")
}

pub fn validate_request_id(value: &str) -> Result<(), String> {
    validate_token(value, MAX_REQUEST_ID_BYTES, "request id")
}

fn validate_token(value: &str, max_len: usize, label: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > max_len {
        return Err(format!("{label} exceeds {max_len} bytes"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(format!("{label} contains an unsupported byte"));
    }
    Ok(())
}

pub fn expected_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

pub fn make_record_batch(
    table: &str,
    batch_index: usize,
    rows: usize,
    payload_bytes_per_row: usize,
) -> Result<RecordBatch, ArrowError> {
    let sequence = UInt64Array::from_iter_values(
        (0..rows).map(|row| (batch_index.saturating_mul(rows).saturating_add(row)) as u64),
    );
    let tables = StringArray::from_iter_values((0..rows).map(|_| table));
    let payload = vec![(batch_index % 251) as u8; payload_bytes_per_row];
    let payloads = BinaryArray::from_iter_values((0..rows).map(|_| payload.as_slice()));

    RecordBatch::try_new(
        expected_schema(),
        vec![Arc::new(sequence), Arc::new(tables), Arc::new(payloads)],
    )
}

#[derive(Clone)]
pub struct EventSink {
    file: Arc<Mutex<File>>,
}

impl EventSink {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn emit(&self, event: impl AsRef<str>) -> io::Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("event log mutex poisoned"))?;
        writeln!(file, "ts_ms={now_ms}\t{}", event.as_ref())?;
        file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_key_is_bounded_and_ascii() {
        assert!(validate_routing_key("pool_a.table-42").is_ok());
        assert!(validate_routing_key("").is_err());
        assert!(validate_routing_key(&"a".repeat(MAX_ROUTING_KEY_BYTES + 1)).is_err());
        assert!(validate_routing_key("table/name").is_err());
        assert!(validate_routing_key("tåble").is_err());
    }

    #[test]
    fn record_batch_matches_expected_schema() {
        let batch =
            make_record_batch("alpha", 3, 4, 128).unwrap_or_else(|error| panic!("failed to build test batch: {error}"));
        assert_eq!(batch.schema(), expected_schema());
        assert_eq!(batch.num_rows(), 4);
    }
}
