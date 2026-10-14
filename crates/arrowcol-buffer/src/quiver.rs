use crate::{BufferError, BufferErrorKind, BufferResult};

/// Native persistence capability advertised by this buffer build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceCapability {
    /// In-memory Arrow references with SQL receipt crash reconciliation.
    MemoryWithReceiptReconciliation,
    /// Generic native Quiver persistence is unavailable.
    QuiverUnsupported,
}

/// Typed boundary for a future generic Quiver persistence adapter.
pub struct QuiverPersistence;

impl QuiverPersistence {
    /// Returns the current checked-in Quiver capability.
    #[must_use]
    pub const fn capability() -> PersistenceCapability {
        PersistenceCapability::QuiverUnsupported
    }

    /// Refuses to invent persistence semantics unsupported by checked-in Quiver.
    ///
    /// Current Quiver persists fixed OTAP bundle slots and does not persist
    /// arbitrary Arrowcol table/schema identities, fingerprints, receipt IDs,
    /// or generic record batches.
    ///
    /// # Errors
    ///
    /// Always returns [`BufferErrorKind::UnsupportedCapability`].
    pub fn open() -> BufferResult<Self> {
        Err(BufferError::new(
            BufferErrorKind::UnsupportedCapability,
            "checked-in Quiver cannot persist arbitrary Arrowcol table/schema/receipt identities",
        ))
    }
}
