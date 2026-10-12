use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_deltalake::delta::{
    PublicationFence as DeltaPublicationFence, PublicationFenceContext, PublicationFenceError,
};
use arrowcol_protocol::{TableLease, UnixMillis};

/// Synchronous Delta commit fence backed by the latest successful SQL lease heartbeat.
///
/// The owner updates this cache only after SQL renewal succeeds. A failed
/// renewal should call [`Self::invalidate`], causing all later commit attempts
/// to fail closed before the Delta log write.
pub struct LeasePublicationFence {
    lease: RwLock<TableLease>,
    invalid_reason: Mutex<Option<String>>,
}

impl LeasePublicationFence {
    /// Creates a fence from an active SQL-backed table lease.
    #[must_use]
    pub fn new(lease: TableLease) -> Self {
        Self {
            lease: RwLock::new(lease),
            invalid_reason: Mutex::new(None),
        }
    }

    /// Replaces the cached lease after a successful SQL renewal.
    ///
    /// # Errors
    ///
    /// Returns a stale-ownership error if the successor changes ownership,
    /// identity, lineage, or fencing epoch.
    pub fn update(&self, successor: TableLease) -> Result<(), PublicationFenceError> {
        let mut lease = self
            .lease
            .write()
            .map_err(|_| PublicationFenceError::check_failed("lease cache lock poisoned"))?;
        lease
            .validate_successor(&successor)
            .map_err(|cause| PublicationFenceError::stale(cause.to_string()))?;
        *lease = successor;
        Ok(())
    }

    /// Permanently fails the fence closed after lease loss or heartbeat failure.
    pub fn invalidate(&self, reason: impl Into<String>) {
        if let Ok(mut invalid) = self.invalid_reason.lock() {
            *invalid = Some(reason.into());
        }
    }

    /// Returns the most recently confirmed lease.
    ///
    /// # Errors
    ///
    /// Returns a check failure if the cache lock was poisoned.
    pub fn lease(&self) -> Result<TableLease, PublicationFenceError> {
        self.lease
            .read()
            .map(|lease| lease.clone())
            .map_err(|_| PublicationFenceError::check_failed("lease cache lock poisoned"))
    }
}

impl DeltaPublicationFence for LeasePublicationFence {
    fn check_before_commit(&self, _context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        if let Some(reason) = self
            .invalid_reason
            .lock()
            .map_err(|_| PublicationFenceError::check_failed("lease invalidation lock poisoned"))?
            .as_ref()
        {
            return Err(PublicationFenceError::stale(reason.clone()));
        }
        let lease = self
            .lease
            .read()
            .map_err(|_| PublicationFenceError::check_failed("lease cache lock poisoned"))?;
        let now = unix_millis()?;
        if !lease.is_active_at(now) {
            return Err(PublicationFenceError::stale(
                "SQL table lease expired before Delta commit",
            ));
        }
        Ok(())
    }
}

fn unix_millis() -> Result<UnixMillis, PublicationFenceError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PublicationFenceError::check_failed("system clock is before the Unix epoch"))?
        .as_millis();
    let millis = u64::try_from(millis)
        .map_err(|_| PublicationFenceError::check_failed("system time exceeds the protocol range"))?;
    UnixMillis::new(millis).map_err(|cause| PublicationFenceError::check_failed(cause.to_string()))
}
