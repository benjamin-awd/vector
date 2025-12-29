//! Tower service for Delta Lake write operations.
//!
//! This module implements the service layer that handles actual writes to Delta Lake,
//! including Parquet file creation and transaction log management.

use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, DeltaTableError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
#[derive(Clone)]
pub struct DeltaLakeService {
    table: Arc<RwLock<DeltaTable>>,
}

impl DeltaLakeService {
    /// Create a new Delta Lake service for the given table.
    pub fn new(table: DeltaTable) -> Self {
        Self {
            table: Arc::new(RwLock::new(table)),
        }
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

        Box::pin(async move {
            // Get a clone of the current table snapshot
            let mut table = {
                let table_guard = table_lock.read().map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to acquire table read lock: {}", e))
                })?;
                table_guard.clone()
            };
            // Retry loop for handling concurrent transaction conflicts
            // When optimization operations (like z-order) rewrite files, we need to
            // reload the table snapshot and retry the write
            const MAX_CONFLICT_RETRIES: usize = 3;
            let mut retry_count = 0;

            loop {
                // Deserialize Parquet bytes back to RecordBatch
                // Use Bytes directly as it implements ChunkReader
                let bytes = request.parquet_data.clone();
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to create Parquet reader: {}", e))
                })?;

                let mut reader = builder.build().map_err(|e| {
                    DeltaTableError::Generic(format!("Failed to build Parquet reader: {}", e))
                })?;

                // Create Delta writer
                let mut writer = RecordBatchWriter::for_table(&table)?;

                // Write all batches (usually just one)
                while let Some(batch_result) = reader.next() {
                    let batch = batch_result.map_err(|e| {
                        DeltaTableError::Generic(format!("Failed to read batch: {}", e))
                    })?;
                    writer.write(batch).await?;
                }

                // Flush and commit (writes Parquet to storage + creates transaction log)
                // Returns the number of rows written
                match writer.flush_and_commit(&mut table).await {
                    Ok(_rows_written) => {
                        // Success! Update the cached table to the latest version
                        // This prevents future requests from starting with stale snapshots
                        {
                            let mut table_guard = table_lock.write().map_err(|e| {
                                DeltaTableError::Generic(format!(
                                    "Failed to acquire table write lock: {}",
                                    e
                                ))
                            })?;
                            *table_guard = table.clone();
                        }

                        // Get the actual bytes written from the table metadata
                        // For now, estimate based on parquet data size
                        let bytes_written = request.parquet_data.len();

                        return Ok(DeltaLakeResponse {
                            events_byte_size: request
                                .request_metadata
                                .into_events_estimated_json_encoded_byte_size(),
                            files_written: 1, // One commit creates one file typically
                            bytes_written,
                        });
                    }
                    Err(e) => {
                        // Check if this is a concurrent delete/read conflict
                        // This happens when optimization operations rewrite files
                        let is_concurrent_conflict = matches!(
                            &e,
                            DeltaTableError::Transaction { source }
                                if source.to_string().contains("ConcurrentDeleteRead")
                                    || source.to_string().contains("concurrent transaction deleted")
                        );

                        if is_concurrent_conflict && retry_count < MAX_CONFLICT_RETRIES {
                            retry_count += 1;
                            warn!(
                                message = "Concurrent table modification detected, reloading and retrying",
                                retry_count = retry_count,
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
                        } else {
                            // Not a concurrent conflict, or exhausted retries
                            if is_concurrent_conflict {
                                error!(
                                    message = "Exhausted retries for concurrent conflict",
                                    retry_count = retry_count,
                                );
                            }
                            return Err(e);
                        }
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
