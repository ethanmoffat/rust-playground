//! Durable delivery receipts for direct Arrow collectors.
//!
//! Receipt creation consumes [`arrowcol_catalog::ValidatedIncomingSchema`], so
//! callers cannot persist an accepted delivery before catalog validation.
//! [`ValidatedDelivery::recovered`] only rebuilds a delivery that was validated
//! before its batch was persisted to a collector's local durable buffer.
//! SQL stores control metadata only; Arrow arrays remain in the buffer crate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod fence;
mod model;
mod sql_server;
mod store;

pub use error::{ReceiptError, ReceiptErrorKind, ReceiptResult};
pub use fence::LeasePublicationFence;
pub use model::{
    AdoptionCandidate, FailureDisposition, MAX_DURABLE_RECOVERY_WINDOW_MS, OwnerDrain, OwnerRegistration,
    PublicationBatch, ReceiptAdoption, ReceiptAdoptionReport, ReceiptFailure, ReceiptRecord, ReceiptVersion,
    ReconciliationReport, RejectedAdoption, TableLeaseRequest, ValidatedDelivery,
};
pub use sql_server::{SqlServerReceiptConfig, SqlServerReceiptStore};
pub use store::ReceiptStore;
