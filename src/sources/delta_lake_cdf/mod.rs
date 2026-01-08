//! Delta Lake Change Data Feed (CDF) source.
//!
//! This source streams Change Data Feed events from Delta Lake tables,
//! enabling CDC (Change Data Capture) pipelines with Vector.
//!
//! # Overview
//!
//! The Delta Lake CDF source:
//! - Polls Delta tables at configurable intervals for new versions
//! - Reads Change Data Feed records (inserts, updates, deletes)
//! - Converts records to Vector log events with CDF metadata
//! - Persists checkpoints for resumption after restart
//!
//! # Requirements
//!
//! The source Delta table must have Change Data Feed enabled:
//! ```sql
//! ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! sources:
//!   orders_cdc:
//!     type: delta_lake_cdf
//!     table_uri: "s3://data-lake/orders"
//!     storage_options:
//!       aws_region: "us-east-1"
//!     poll_interval_secs: 10
//!     start_position: latest
//!
//! sinks:
//!   clickhouse:
//!     type: clickhouse
//!     endpoint: "http://clickhouse:8123"
//!     table: "orders_changelog"
//!     inputs: ["orders_cdc"]
//! ```
//!
//! # Event Schema
//!
//! Each event includes CDF metadata:
//! - `_change_type`: "insert", "update_preimage", "update_postimage", or "delete"
//! - `_commit_version`: The Delta table version number
//! - `_commit_timestamp`: The commit timestamp
//!
//! Plus all data columns from the table (if `include_data: true`).

mod checkpoint;
mod config;
mod event;
mod source;

#[cfg(all(test, feature = "delta-lake-cdf-integration-tests"))]
mod integration_tests;

pub use config::DeltaLakeCdfConfig;

use crate::sources::Source;
