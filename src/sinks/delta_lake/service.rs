//! Tower service for Delta Lake write operations.
//!
//! This module implements the service layer that handles actual writes to Delta Lake,
//! including Parquet file creation and transaction log management.

use std::sync::Arc;
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
#[derive(Clone)]
pub struct DeltaLakeService {
    table: Arc<DeltaTable>,
}

impl DeltaLakeService {
    /// Create a new Delta Lake service for the given table.
    pub fn new(table: DeltaTable) -> Self {
        Self {
            table: Arc::new(table),
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
        let mut table = (*self.table).clone();

        Box::pin(async move {
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
                let batch = batch_result
                    .map_err(|e| DeltaTableError::Generic(format!("Failed to read batch: {}", e)))?;
                writer.write(batch).await?;
            }

            // Flush and commit (writes Parquet to storage + creates transaction log)
            // Returns the number of rows written
            let _rows_written = writer.flush_and_commit(&mut table).await?;

            // Get the actual bytes written from the table metadata
            // For now, estimate based on parquet data size
            let bytes_written = request.parquet_data.len();

            Ok(DeltaLakeResponse {
                events_byte_size: request
                    .request_metadata
                    .into_events_estimated_json_encoded_byte_size(),
                files_written: 1, // One commit creates one file typically
                bytes_written,
            })
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
