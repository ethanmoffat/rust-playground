//! Production many-table Arrow Flight benchmark and independent Delta validation.

#![forbid(unsafe_code)]

pub mod cli;
pub mod generator;
mod kusto;
pub mod manifest;
pub mod runner;
pub mod schema;
pub mod sdk;
pub mod transport;
pub mod validation;

pub use cli::{Cli, Command};
