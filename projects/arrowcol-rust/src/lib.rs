//! Production direct Arrow Flight collector.

#![forbid(unsafe_code)]

pub mod admin;
pub mod auth;
pub mod config;
pub mod control;
pub mod delta_sink;
pub mod ingest;
pub mod lease;
pub mod metrics;
pub mod runtime;

pub use config::{Cli, CollectorConfig, ConfigError};
pub use runtime::{RunOutcome, run};
