//! Durable delivery receipts for direct Arrow collectors.
//!
//! Receipt creation consumes [`arrowcol_catalog::ValidatedIncomingSchema`], so
//! callers cannot persist an accepted delivery before catalog validation.
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
    FailureDisposition, OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptFailure, ReceiptRecord, ReceiptVersion,
    ReconciliationReport, TableLeaseRequest, ValidatedDelivery,
};
pub use sql_server::{SqlServerReceiptConfig, SqlServerReceiptStore};
pub use store::ReceiptStore;
