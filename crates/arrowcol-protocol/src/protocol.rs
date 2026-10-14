use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

/// A supported Arrow collector protocol version.
///
/// The JSON representation is an integer. gRPC metadata uses the canonical
/// decimal representation returned by [`Display`](fmt::Display).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProtocolVersion {
    /// The initial production contract.
    V1,
}

impl ProtocolVersion {
    /// The protocol version emitted by this crate.
    pub const CURRENT: Self = Self::V1;

    /// Returns the numeric wire value.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        match self {
            Self::V1 => 1,
        }
    }
}

impl TryFrom<u16> for ProtocolVersion {
    type Error = ProtocolVersionError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            unsupported => Err(ProtocolVersionError::Unsupported(unsupported)),
        }
    }
}

impl FromStr for ProtocolVersion {
    type Err = ProtocolVersionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value
            .parse::<u16>()
            .map_err(|_| ProtocolVersionError::Malformed(value.to_owned()))?;
        Self::try_from(parsed)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_u16().fmt(formatter)
    }
}

impl Serialize for ProtocolVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(self.as_u16())
    }
}

impl<'de> Deserialize<'de> for ProtocolVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        Self::try_from(value).map_err(de::Error::custom)
    }
}

/// Failure to parse or negotiate a protocol version.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProtocolVersionError {
    /// The input was not an unsigned decimal protocol version.
    #[error("malformed protocol version {0:?}")]
    Malformed(String),
    /// The input was syntactically valid but is not supported.
    #[error("unsupported protocol version {0}")]
    Unsupported(u16),
}
