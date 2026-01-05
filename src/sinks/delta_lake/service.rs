//! Tower service for Delta Lake write operations.
//!
//! This module implements the service layer that handles actual writes to Delta Lake,
//! including Parquet file creation and transaction log management.

use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::internal_events::EndpointBytesSent;
use crate::sinks::prelude::*;

use super::request_builder::DeltaLakeRequest;

/// Response from Delta Lake write operations.
///
/// Contains metrics about the write operation including the number of files
/// written and bytes transferred.
#[derive(Debug)]
pub struct DeltaLakeResponse {
    /// Event byte size for metrics
    pub events_byte_size: GroupedCountByteSize,

    /// Number of Parquet files written
    pub files_written: usize,

    /// Bytes written (compressed Parquet size)
    pub bytes_written: usize,
}

impl DriverResponse for DeltaLakeResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.bytes_written)
    }
}

/// Tower service for Delta Lake writes.
///
/// This service handles the conversion of Parquet bytes back to RecordBatch,
/// then uses Delta Lake's RecordBatchWriter to create Parquet files and
/// transaction log entries.
///
/// The table is wrapped in RwLock to allow updating the cached snapshot after
/// successful commits, reducing unnecessary conflict retries.
///
/// ## Schema Evolution Support
///
/// This service supports automatic schema evolution through Delta Lake's merge mode.
/// When schema mismatches are detected, the service will:
/// 1. Reload the table to get the latest schema
/// 2. Retry the write operation with schema merge enabled
/// 3. Allow adding new columns (filled with nulls for existing data)
#[derive(Clone)]
pub struct DeltaLakeService {
    table: Arc<RwLock<DeltaTable>>,
    enable_schema_evolution: bool,
}

impl DeltaLakeService {
    /// Create a new Delta Lake service for the given table.
    pub fn new(table: DeltaTable, enable_schema_evolution: bool) -> Self {
        Self {
            table: Arc::new(RwLock::new(table)),
            enable_schema_evolution,
        }
    }

    /// Check if an error indicates a schema mismatch.
    ///
    /// Schema mismatch errors occur when:
    /// - Writing data with different field types
    /// - Writing data with new columns (without merge mode)
    /// - Writing data missing required non-nullable columns
    fn is_schema_mismatch_error(error: &DeltaTableError) -> bool {
        let error_str = error.to_string().to_lowercase();
        error_str.contains("schema")
            || error_str.contains("field")
            || error_str.contains("column")
            || error_str.contains("incompatible")
            || error_str.contains("type mismatch")
            || matches!(
                error,
                DeltaTableError::Arrow { .. }
                    | DeltaTableError::InvalidData { .. }
                    | DeltaTableError::SchemaMismatch { .. }
            )
    }
}

impl Service<DeltaLakeRequest> for DeltaLakeService {
    type Response = DeltaLakeResponse;
    type Error = DeltaTableError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Delta Lake writes are async, always ready
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: DeltaLakeRequest) -> Self::Future {
        let table_lock = Arc::clone(&self.table);
        let enable_schema_evolution = self.enable_schema_evolution;

        Box::pin(async move {
            // Get a clone of the current table snapshot
            let mut table = {
                let table_guard = table_lock.read().map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to acquire table read lock: {}", e))
                })?;
                table_guard.clone()
            };
            // Retry loop for handling concurrent transaction conflicts and schema evolution
            // When optimization operations (like z-order) rewrite files, or when schema changes,
            // we need to reload the table snapshot and retry the write
            const MAX_CONFLICT_RETRIES: usize = 3;
            const MAX_SCHEMA_RETRIES: usize = 1;
            let mut conflict_retry_count = 0;
            let mut schema_retry_count = 0;

            loop {
                // Deserialize Parquet bytes back to RecordBatch
                // Use Bytes directly as it implements ChunkReader
                let bytes = request.parquet_data.clone();
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to create Parquet reader: {}", e))
                })?;

                let reader = builder.build().map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to build Parquet reader: {}", e))
                })?;

                // Collect all record batches
                let batches: Vec<_> = reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| DeltaTableError::Generic(format!("Failed to read batches: {}", e)))?;

                // Log retry attempt for schema evolution
                if schema_retry_count > 0 {
                    info!(
                        message = "Retrying write after schema reload",
                        retry_count = schema_retry_count,
                    );
                }

                // Build write operation using DeltaTable methods directly
                // This supports schema merge mode for true schema evolution
                let mut write_builder = table
                    .clone()
                    .write(batches)
                    .with_save_mode(SaveMode::Append);

                // Enable schema merge when schema evolution is enabled
                // This allows adding new columns from incoming data
                if enable_schema_evolution {
                    write_builder = write_builder.with_schema_mode(SchemaMode::Merge);
                }

                // Execute write and commit
                match write_builder.await {
                    Ok(new_table) => {
                        // Success! Update the cached table to the latest version
                        // This prevents future requests from starting with stale snapshots
                        // DeltaTable::write() returns the updated table with new schema if merged
                        {
                            let mut table_guard = table_lock.write().map_err(|e| {
                                DeltaTableError::Generic(format!(
                                    "Failed to acquire table write lock: {}",
                                    e
                                ))
                            })?;
                            *table_guard = new_table.clone();
                        }

                        // Get the actual bytes written from the table metadata
                        // For now, estimate based on parquet data size
                        let bytes_written = request.parquet_data.len();

                        emit!(EndpointBytesSent {
                            byte_size: bytes_written,
                            protocol: "delta_lake",
                            endpoint: table.table_url().as_str(),
                        });

                        return Ok(DeltaLakeResponse {
                            events_byte_size: request
                                .request_metadata
                                .into_events_estimated_json_encoded_byte_size(),
                            files_written: 1, // One commit creates one file typically
                            bytes_written,
                        });
                    }
                    Err(e) => {
                        // Check error type for appropriate retry strategy
                        let is_concurrent_conflict = matches!(
                            &e,
                            DeltaTableError::Transaction { source }
                                if source.to_string().contains("ConcurrentDeleteRead")
                                    || source.to_string().contains("concurrent transaction deleted")
                        );

                        let is_schema_mismatch = Self::is_schema_mismatch_error(&e);

                        // Handle schema mismatch errors with reload and retry (if enabled)
                        if enable_schema_evolution
                            && is_schema_mismatch
                            && schema_retry_count < MAX_SCHEMA_RETRIES
                        {
                            schema_retry_count += 1;
                            warn!(
                                message = "Schema mismatch detected, reloading table schema and retrying",
                                error = %e,
                                retry_count = schema_retry_count,
                                max_retries = MAX_SCHEMA_RETRIES,
                            );

                            // Reload the table to get the latest schema
                            // This picks up schema evolution changes (new columns, type changes, etc.)
                            table.load().await.map_err(|load_err| {
                                DeltaTableError::Generic(format!(
                                    "Failed to reload table after schema mismatch: {}",
                                    load_err
                                ))
                            })?;

                            // Reset conflict retry count for fresh attempt
                            conflict_retry_count = 0;

                            // Continue to retry with fresh schema
                            continue;
                        }

                        // Handle concurrent transaction conflicts
                        if is_concurrent_conflict && conflict_retry_count < MAX_CONFLICT_RETRIES {
                            conflict_retry_count += 1;
                            warn!(
                                message = "Concurrent table modification detected, reloading and retrying",
                                retry_count = conflict_retry_count,
                                max_retries = MAX_CONFLICT_RETRIES,
                            );

                            // Reload the table to get the latest snapshot
                            // This picks up changes from optimize/vacuum operations
                            table.load().await.map_err(|load_err| {
                                DeltaTableError::Generic(format!(
                                    "Failed to reload table after conflict: {}",
                                    load_err
                                ))
                            })?;

                            // Continue to retry with fresh snapshot
                            continue;
                        }

                        // Not retriable, or exhausted retries
                        if is_schema_mismatch {
                            error!(
                                message = "Exhausted retries for schema mismatch",
                                error = %e,
                                retry_count = schema_retry_count,
                            );
                        } else if is_concurrent_conflict {
                            error!(
                                message = "Exhausted retries for concurrent conflict",
                                retry_count = conflict_retry_count,
                            );
                        }
                        return Err(e);
                    }
                }
            }
        })
    }
}

/// Retry logic for Delta Lake operations.
///
/// Determines which errors are retriable (network/storage) vs non-retriable (schema/data).
#[derive(Debug, Clone, Default)]
pub struct DeltaLakeRetryLogic;

impl RetryLogic for DeltaLakeRetryLogic {
    type Error = DeltaTableError;
    type Request = DeltaLakeRequest;
    type Response = DeltaLakeResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        match error {
            // Retry on storage/network errors
            DeltaTableError::ObjectStore { source: _ } => true,
            DeltaTableError::Io { source: _ } => true,

            // Don't retry schema/data errors
            DeltaTableError::Arrow { source: _ } => false,
            DeltaTableError::Kernel { source: _ } => false,
            DeltaTableError::InvalidData { violations: _ } => false,

            // Generic errors - be conservative, don't retry
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: Retry logic tests would require constructing DeltaTableError variants
    // which need their dependency types (object_store::Error, arrow::error::ArrowError).
    // These are better tested in integration tests where the full error flow occurs naturally.

    #[test]
    fn test_retry_logic_structure() {
        // Verify the retry logic can be instantiated
        let _retry_logic = super::DeltaLakeRetryLogic::default();
    }
}
