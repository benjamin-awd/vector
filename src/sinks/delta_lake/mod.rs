//! Delta Lake sink for writing log events to Delta Lake tables.
//!
//! This sink writes batched log events to Delta Lake tables stored on cloud object storage (GCS).
//! It leverages the existing Arrow batch encoding infrastructure and the deltalake Rust crate
//! for transaction log management.

pub mod config;
pub mod request_builder;
pub mod schema;
pub mod service;
pub mod sink;

pub use config::DeltaLakeConfig;
