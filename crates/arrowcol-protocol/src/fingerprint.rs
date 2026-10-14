use std::{collections::HashMap, fmt, str::FromStr};

use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionMode};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Stable name of the canonical schema fingerprint algorithm.
pub const SCHEMA_FINGERPRINT_ALGORITHM: &str = "arrowcol-arrow-schema-sha256-v1";

const FINGERPRINT_PREFIX: &str = "sha256:";
const FINGERPRINT_BYTES: usize = 32;
const FINGERPRINT_HEX_BYTES: usize = FINGERPRINT_BYTES * 2;
const FINGERPRINT_TEXT_BYTES: usize = FINGERPRINT_PREFIX.len() + FINGERPRINT_HEX_BYTES;

/// A deterministic SHA-256 fingerprint of a canonical Arrow schema encoding.
///
/// Its text and JSON representation is `sha256:` followed by 64 lowercase
/// hexadecimal digits.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaFingerprint([u8; FINGERPRINT_BYTES]);

impl SchemaFingerprint {
    /// Creates a fingerprint from its raw SHA-256 digest.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; FINGERPRINT_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the raw SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; FINGERPRINT_BYTES] {
        &self.0
    }
}

impl fmt::Debug for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(FINGERPRINT_PREFIX)?;
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for SchemaFingerprint {
    type Err = FingerprintParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != FINGERPRINT_TEXT_BYTES {
            return Err(FingerprintParseError::WrongLength {
                actual: value.len(),
                expected: FINGERPRINT_TEXT_BYTES,
            });
        }
        let encoded = value
            .strip_prefix(FINGERPRINT_PREFIX)
            .ok_or(FingerprintParseError::WrongPrefix)?;
        if !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(FingerprintParseError::InvalidHex);
        }

        let mut bytes = [0_u8; FINGERPRINT_BYTES];
        hex::decode_to_slice(encoded, &mut bytes).map_err(|_| FingerprintParseError::InvalidHex)?;
        Ok(Self(bytes))
    }
}

impl Serialize for SchemaFingerprint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SchemaFingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// A schema fingerprint supplied by a client in request metadata.
///
/// This is only a claim. A collector must compute the fingerprint of the
/// received Arrow schema and compare it with the catalog-approved fingerprint;
/// possession of this value is not schema proof or authorization.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClaimedSchemaFingerprint(SchemaFingerprint);

impl ClaimedSchemaFingerprint {
    /// Wraps a fingerprint as an untrusted client claim.
    #[must_use]
    pub const fn new(fingerprint: SchemaFingerprint) -> Self {
        Self(fingerprint)
    }

    /// Returns the claimed value for explicit comparison.
    #[must_use]
    pub const fn fingerprint(self) -> SchemaFingerprint {
        self.0
    }

    /// Computes and compares the received schema with this claim.
    ///
    /// A match only establishes that the claim describes the received schema.
    /// Callers must separately compare against authoritative catalog state.
    #[must_use]
    pub fn matches_received_schema(self, schema: &Schema) -> bool {
        self.0 == fingerprint_schema(schema)
    }
}

impl fmt::Display for ClaimedSchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ClaimedSchemaFingerprint {
    type Err = FingerprintParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Failure to parse a canonical schema fingerprint.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FingerprintParseError {
    /// The `sha256:` algorithm prefix was absent.
    #[error("schema fingerprint must start with sha256:")]
    WrongPrefix,
    /// The text had the wrong byte length.
    #[error("schema fingerprint is {actual} bytes; expected {expected}")]
    WrongLength {
        /// Actual byte length.
        actual: usize,
        /// Required byte length.
        expected: usize,
    },
    /// The digest was not canonical lowercase hexadecimal.
    #[error("schema fingerprint digest must be lowercase hexadecimal")]
    InvalidHex,
}

/// Computes the canonical Arrow schema fingerprint.
///
/// The canonical encoding preserves field order, names, nullability, all nested
/// type structure, dictionary ordering, time units and zones, decimal
/// precision/scale, union IDs/mode, map ordering flags, and sorted schema/field
/// metadata. Hash-map insertion order therefore has no effect.
#[must_use]
pub fn fingerprint_schema(schema: &Schema) -> SchemaFingerprint {
    let mut encoder = CanonicalEncoder::new();
    encoder.write_text(SCHEMA_FINGERPRINT_ALGORITHM);
    encoder.write_fields(&schema.fields);
    encoder.write_metadata(&schema.metadata);
    SchemaFingerprint::from_bytes(encoder.finish())
}

struct CanonicalEncoder {
    hasher: Sha256,
}

impl CanonicalEncoder {
    fn new() -> Self {
        Self { hasher: Sha256::new() }
    }

    fn finish(self) -> [u8; FINGERPRINT_BYTES] {
        self.hasher.finalize().into()
    }

    fn write_tag(&mut self, tag: u8) {
        self.hasher.update([tag]);
    }

    fn write_bool(&mut self, value: bool) {
        self.write_tag(u8::from(value));
    }

    fn write_i8(&mut self, value: i8) {
        self.hasher.update(value.to_le_bytes());
    }

    fn write_u8(&mut self, value: u8) {
        self.hasher.update([value]);
    }

    fn write_i32(&mut self, value: i32) {
        self.hasher.update(value.to_le_bytes());
    }

    fn write_text(&mut self, value: &str) {
        self.write_usize(value.len());
        self.hasher.update(value.as_bytes());
    }

    fn write_usize(&mut self, value: usize) {
        let decimal = value.to_string();
        self.hasher.update(decimal.as_bytes());
        self.write_tag(b':');
    }

    fn write_metadata(&mut self, metadata: &HashMap<String, String>) {
        let mut entries: Vec<_> = metadata.iter().collect();
        entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
        self.write_usize(entries.len());
        for (key, value) in entries {
            self.write_text(key);
            self.write_text(value);
        }
    }

    fn write_fields(&mut self, fields: &Fields) {
        self.write_usize(fields.len());
        for field in fields {
            self.write_field(field);
        }
    }

    fn write_field(&mut self, field: &Field) {
        self.write_tag(0xf0);
        self.write_text(field.name());
        self.write_bool(field.is_nullable());
        match field.dict_is_ordered() {
            None => self.write_tag(0),
            Some(false) => self.write_tag(1),
            Some(true) => self.write_tag(2),
        }
        self.write_data_type(field.data_type());
        self.write_metadata(field.metadata());
        self.write_tag(0xf1);
    }

    #[expect(clippy::too_many_lines)]
    fn write_data_type(&mut self, data_type: &DataType) {
        match data_type {
            DataType::Null => self.write_tag(0),
            DataType::Boolean => self.write_tag(1),
            DataType::Int8 => self.write_tag(2),
            DataType::Int16 => self.write_tag(3),
            DataType::Int32 => self.write_tag(4),
            DataType::Int64 => self.write_tag(5),
            DataType::UInt8 => self.write_tag(6),
            DataType::UInt16 => self.write_tag(7),
            DataType::UInt32 => self.write_tag(8),
            DataType::UInt64 => self.write_tag(9),
            DataType::Float16 => self.write_tag(10),
            DataType::Float32 => self.write_tag(11),
            DataType::Float64 => self.write_tag(12),
            DataType::Timestamp(unit, timezone) => {
                self.write_tag(13);
                self.write_time_unit(*unit);
                match timezone {
                    None => self.write_tag(0),
                    Some(timezone) => {
                        self.write_tag(1);
                        self.write_text(timezone);
                    }
                }
            }
            DataType::Date32 => self.write_tag(14),
            DataType::Date64 => self.write_tag(15),
            DataType::Time32(unit) => {
                self.write_tag(16);
                self.write_time_unit(*unit);
            }
            DataType::Time64(unit) => {
                self.write_tag(17);
                self.write_time_unit(*unit);
            }
            DataType::Duration(unit) => {
                self.write_tag(18);
                self.write_time_unit(*unit);
            }
            DataType::Interval(unit) => {
                self.write_tag(19);
                self.write_interval_unit(*unit);
            }
            DataType::Binary => self.write_tag(20),
            DataType::FixedSizeBinary(size) => {
                self.write_tag(21);
                self.write_i32(*size);
            }
            DataType::LargeBinary => self.write_tag(22),
            DataType::BinaryView => self.write_tag(23),
            DataType::Utf8 => self.write_tag(24),
            DataType::LargeUtf8 => self.write_tag(25),
            DataType::Utf8View => self.write_tag(26),
            DataType::List(field) => {
                self.write_tag(27);
                self.write_field(field);
            }
            DataType::ListView(field) => {
                self.write_tag(28);
                self.write_field(field);
            }
            DataType::FixedSizeList(field, size) => {
                self.write_tag(29);
                self.write_field(field);
                self.write_i32(*size);
            }
            DataType::LargeList(field) => {
                self.write_tag(30);
                self.write_field(field);
            }
            DataType::LargeListView(field) => {
                self.write_tag(31);
                self.write_field(field);
            }
            DataType::Struct(fields) => {
                self.write_tag(32);
                self.write_fields(fields);
            }
            DataType::Union(fields, mode) => {
                self.write_tag(33);
                self.write_union_mode(*mode);
                self.write_usize(fields.len());
                for (type_id, field) in fields.iter() {
                    self.write_i8(type_id);
                    self.write_field(field);
                }
            }
            DataType::Dictionary(key, value) => {
                self.write_tag(34);
                self.write_data_type(key);
                self.write_data_type(value);
            }
            DataType::Decimal32(precision, scale) => {
                self.write_tag(35);
                self.write_u8(*precision);
                self.write_i8(*scale);
            }
            DataType::Decimal64(precision, scale) => {
                self.write_tag(36);
                self.write_u8(*precision);
                self.write_i8(*scale);
            }
            DataType::Decimal128(precision, scale) => {
                self.write_tag(37);
                self.write_u8(*precision);
                self.write_i8(*scale);
            }
            DataType::Decimal256(precision, scale) => {
                self.write_tag(38);
                self.write_u8(*precision);
                self.write_i8(*scale);
            }
            DataType::Map(field, sorted) => {
                self.write_tag(39);
                self.write_field(field);
                self.write_bool(*sorted);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.write_tag(40);
                self.write_field(run_ends);
                self.write_field(values);
            }
        }
    }

    fn write_time_unit(&mut self, unit: TimeUnit) {
        self.write_tag(match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        });
    }

    fn write_interval_unit(&mut self, unit: IntervalUnit) {
        self.write_tag(match unit {
            IntervalUnit::YearMonth => 0,
            IntervalUnit::DayTime => 1,
            IntervalUnit::MonthDayNano => 2,
        });
    }

    fn write_union_mode(&mut self, mode: UnionMode) {
        self.write_tag(match mode {
            UnionMode::Sparse => 0,
            UnionMode::Dense => 1,
        });
    }
}
