use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

/// Largest interoperable nonnegative contract measurement.
pub const MAX_MEASUREMENT: u64 = 9_223_372_036_854_775_807;

/// A nonnegative, bounded count or byte measurement.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Measurement(u64);

impl Measurement {
    /// The zero measurement.
    pub const ZERO: Self = Self(0);

    /// Creates a bounded measurement.
    ///
    /// # Errors
    ///
    /// Returns [`MeasurementError::TooLarge`] when the value exceeds the
    /// signed 64-bit interoperability range used by control-plane stores.
    pub const fn new(value: u64) -> Result<Self, MeasurementError> {
        if value > MAX_MEASUREMENT {
            return Err(MeasurementError::TooLarge {
                value,
                maximum: MAX_MEASUREMENT,
            });
        }
        Ok(Self(value))
    }

    /// Returns the numeric measurement.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Measurement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A positive, bounded capacity or delay measurement.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PositiveMeasurement(Measurement);

impl PositiveMeasurement {
    /// Creates a positive bounded measurement.
    ///
    /// # Errors
    ///
    /// Returns [`MeasurementError`] when the value is zero or too large.
    pub const fn new(value: u64) -> Result<Self, MeasurementError> {
        if value == 0 {
            return Err(MeasurementError::MustBePositive);
        }
        match Measurement::new(value) {
            Ok(value) => Ok(Self(value)),
            Err(error) => Err(error),
        }
    }

    /// Returns the numeric measurement.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl Serialize for PositiveMeasurement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for PositiveMeasurement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A bounded Unix timestamp expressed in milliseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct UnixMillis(Measurement);

impl UnixMillis {
    /// Creates a timestamp in milliseconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`MeasurementError::TooLarge`] when the value exceeds the wire
    /// contract's interoperable range.
    pub const fn new(value: u64) -> Result<Self, MeasurementError> {
        match Measurement::new(value) {
            Ok(value) => Ok(Self(value)),
            Err(error) => Err(error),
        }
    }

    /// Returns milliseconds since the Unix epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl<'de> Deserialize<'de> for UnixMillis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A utilization or ratio expressed in basis points (`0..=10_000`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BasisPoints(u16);

impl BasisPoints {
    /// Number of basis points representing 100 percent.
    pub const FULL_SCALE: u16 = 10_000;

    /// Creates a bounded basis-point value.
    ///
    /// # Errors
    ///
    /// Returns [`MeasurementError::BasisPointsOutOfRange`] above 10,000.
    pub const fn new(value: u16) -> Result<Self, MeasurementError> {
        if value > Self::FULL_SCALE {
            return Err(MeasurementError::BasisPointsOutOfRange(value));
        }
        Ok(Self(value))
    }

    /// Returns the basis-point value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl<'de> Deserialize<'de> for BasisPoints {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A current pressure measurement paired with its positive configured limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PressureGauge {
    current: Measurement,
    limit: PositiveMeasurement,
}

impl PressureGauge {
    /// Creates a pressure gauge whose current value does not exceed its limit.
    ///
    /// # Errors
    ///
    /// Returns [`MeasurementError::ExceedsLimit`] when `current > limit`.
    pub const fn new(current: Measurement, limit: PositiveMeasurement) -> Result<Self, MeasurementError> {
        if current.get() > limit.get() {
            return Err(MeasurementError::ExceedsLimit {
                current: current.get(),
                limit: limit.get(),
            });
        }
        Ok(Self { current, limit })
    }

    /// Returns the observed value.
    #[must_use]
    pub const fn current(self) -> Measurement {
        self.current
    }

    /// Returns the configured positive limit.
    #[must_use]
    pub const fn limit(self) -> PositiveMeasurement {
        self.limit
    }

    /// Returns utilization rounded down to basis points.
    #[must_use]
    pub fn utilization(self) -> BasisPoints {
        let scaled = u128::from(self.current.get()) * u128::from(BasisPoints::FULL_SCALE);
        let ratio = scaled / u128::from(self.limit.get());
        match u16::try_from(ratio) {
            Ok(ratio) => BasisPoints(ratio),
            Err(_) => BasisPoints(BasisPoints::FULL_SCALE),
        }
    }
}

impl<'de> Deserialize<'de> for PressureGauge {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            current: Measurement,
            limit: PositiveMeasurement,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.current, wire.limit).map_err(de::Error::custom)
    }
}

/// Failure to validate a bounded numeric contract value.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MeasurementError {
    /// The value exceeded the interoperable upper bound.
    #[error("measurement {value} exceeds maximum {maximum}")]
    TooLarge {
        /// Rejected value.
        value: u64,
        /// Largest supported value.
        maximum: u64,
    },
    /// A capacity or delay was zero.
    #[error("measurement must be positive")]
    MustBePositive,
    /// A current pressure value exceeded its configured bound.
    #[error("current measurement {current} exceeds limit {limit}")]
    ExceedsLimit {
        /// Rejected current value.
        current: u64,
        /// Configured upper limit.
        limit: u64,
    },
    /// A basis-point ratio was greater than 100 percent.
    #[error("basis points {0} exceed 10000")]
    BasisPointsOutOfRange(u16),
}
