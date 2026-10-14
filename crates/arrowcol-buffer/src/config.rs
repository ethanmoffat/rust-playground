use std::time::Duration;

use arrow_deltalake::sizing::{AdaptiveFileSizeConfig, FileSizeEstimatorError};

use crate::{BufferError, BufferErrorKind, BufferResult};

/// Byte, row, and batch capacity limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityLimits {
    /// Maximum logical Arrow bytes.
    pub bytes: u64,
    /// Maximum rows.
    pub rows: u64,
    /// Maximum record batches.
    pub batches: u64,
}

impl CapacityLimits {
    /// Creates positive capacity limits.
    ///
    /// # Errors
    ///
    /// Rejects zero dimensions.
    pub fn new(bytes: u64, rows: u64, batches: u64) -> BufferResult<Self> {
        let limits = Self { bytes, rows, batches };
        limits.validate()?;
        Ok(limits)
    }

    pub(crate) fn validate(self) -> BufferResult<()> {
        if self.bytes == 0 || self.rows == 0 || self.batches == 0 {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "buffer byte, row, and batch capacities must all be positive",
            ));
        }
        Ok(())
    }

    pub(crate) const fn contains(self, other: Self) -> bool {
        other.bytes <= self.bytes && other.rows <= self.rows && other.batches <= self.batches
    }
}

/// Direct buffer limits and flush policy.
#[derive(Clone, Debug)]
pub struct BufferConfig {
    /// Process-wide retained/in-flight Arrow capacity.
    pub global_capacity: CapacityLimits,
    /// Capacity independently enforced for each table.
    pub per_table_capacity: CapacityLimits,
    /// Maximum bytes in one table/schema flush group.
    pub max_arrow_memory_per_flush: u64,
    /// Maximum rows in one table/schema flush group.
    pub max_rows_per_flush: u64,
    /// Maximum batches in one table/schema flush group.
    pub max_batches_per_flush: u64,
    /// Oldest allowed queued age.
    pub max_age: Duration,
    /// Frequency of age-trigger checks.
    pub age_check_interval: Duration,
    /// Maximum simultaneous publication sink sessions.
    pub max_concurrent_flushes: usize,
    /// Receipt heartbeat horizon used during buffer and flush transitions.
    pub receipt_lease_duration: Duration,
    /// Adaptive compressed-file target estimator.
    pub adaptive_size: AdaptiveFileSizeConfig,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            global_capacity: CapacityLimits {
                bytes: 2 * 1024 * 1024 * 1024,
                rows: 10_000_000,
                batches: 4_096,
            },
            per_table_capacity: CapacityLimits {
                bytes: 512 * 1024 * 1024,
                rows: 2_000_000,
                batches: 1_024,
            },
            max_arrow_memory_per_flush: 512 * 1024 * 1024,
            max_rows_per_flush: 1_000_000,
            max_batches_per_flush: 512,
            max_age: Duration::from_secs(30),
            age_check_interval: Duration::from_millis(250),
            max_concurrent_flushes: 4,
            receipt_lease_duration: Duration::from_secs(60),
            adaptive_size: AdaptiveFileSizeConfig::default(),
        }
    }
}

impl BufferConfig {
    /// Validates all capacity, duration, concurrency, and estimator bounds.
    ///
    /// # Errors
    ///
    /// Returns the first invalid setting.
    pub fn validate(&self) -> BufferResult<()> {
        self.global_capacity.validate()?;
        self.per_table_capacity.validate()?;
        if !self.global_capacity.contains(self.per_table_capacity) {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "per-table capacity must not exceed global capacity",
            ));
        }
        if self.max_arrow_memory_per_flush == 0
            || self.max_arrow_memory_per_flush > self.per_table_capacity.bytes
            || self.max_rows_per_flush == 0
            || self.max_rows_per_flush > self.per_table_capacity.rows
            || self.max_batches_per_flush == 0
            || self.max_batches_per_flush > self.per_table_capacity.batches
        {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "flush limits must be positive and within per-table capacity",
            ));
        }
        if self.max_age.is_zero()
            || self.age_check_interval.is_zero()
            || self.receipt_lease_duration.is_zero()
            || self.max_concurrent_flushes == 0
        {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "age, heartbeat, and concurrency settings must be positive",
            ));
        }
        self.adaptive_size.validate().map_err(estimator_error)?;
        Ok(())
    }
}

fn estimator_error(error: FileSizeEstimatorError) -> BufferError {
    BufferError::new(
        BufferErrorKind::InvalidConfiguration,
        format!("adaptive file-size configuration is invalid: {error}"),
    )
}
