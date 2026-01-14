use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use deltalake::DeltaTableError;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::checkpoints::create_checkpoint;
use deltalake::protocol::SaveMode;
use url::Url;

/// Interval at which to create checkpoints (every N versions).
/// Checkpoints compact the transaction log, reducing memory usage on subsequent reads.
const CHECKPOINT_INTERVAL: i64 = 10;

use crate::common::backoff::ExponentialBackoff;
use crate::internal_events::EndpointBytesSent;
use crate::sinks::prelude::*;

use super::request_builder::{DeltaLakeRequest, SharedSchema};

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
/// This service writes Arrow RecordBatches directly to Delta Lake tables,
/// avoiding the overhead of Parquet serialization/deserialization round-trips.
///
/// Opens a fresh table connection for each write request to ensure bounded memory
/// usage. This prevents accumulation of transaction log state that can cause OOM
/// issues with tables that have many versions.
///
/// ## Schema Evolution Support
///
/// This service supports automatic schema evolution through Delta Lake's merge mode.
/// When schema mismatches are detected, the service will:
/// 1. Reload the table to get the latest schema
/// 2. Retry the write operation with schema merge enabled
#[derive(Clone)]
pub struct DeltaLakeService {
    /// Table URI for opening fresh connections
    table_uri: String,
    /// Storage options for authentication
    storage_options: HashMap<String, String>,
    schema_evolution: bool,
    /// Shared schema reference, updated after successful writes
    shared_schema: SharedSchema,
}

impl DeltaLakeService {
    /// Create a new Delta Lake service for the given table.
    pub const fn new(
        table_uri: String,
        storage_options: HashMap<String, String>,
        schema_evolution: bool,
        shared_schema: SharedSchema,
    ) -> Self {
        Self {
            table_uri,
            storage_options,
            schema_evolution,
            shared_schema,
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
        let table_uri = self.table_uri.clone();
        let storage_options = self.storage_options.clone();
        let schema_evolution = self.schema_evolution;
        let shared_schema = Arc::clone(&self.shared_schema);

        Box::pin(async move {
            // Use batches directly from request - no Parquet deserialization needed
            let batches = request.batches;

            // Open a fresh table connection for each request to ensure bounded memory.
            // This prevents accumulation of transaction log state that can cause OOM
            // issues with tables that have many versions (e.g., 20k+ versions).
            // The table is dropped at the end of each request, freeing all memory.
            let parsed_uri = Url::parse(&table_uri).map_err(|e| {
                DeltaTableError::Generic(format!("Invalid table URI {}: {}", table_uri, e))
            })?;

            let mut table =
                deltalake::open_table_with_storage_options(parsed_uri, storage_options.clone())
                    .await
                    .map_err(|e| {
                        DeltaTableError::Generic(format!(
                            "Failed to open table {}: {}",
                            table_uri, e
                        ))
                    })?;

            // Retry loop for handling concurrent transaction conflicts and schema evolution
            // When optimization operations (like z-order) rewrite files, or when schema changes,
            // we need to reload the table snapshot and retry the write
            const MAX_CONFLICT_RETRIES: usize = 5;
            const MAX_SCHEMA_RETRIES: usize = 1;
            let mut conflict_retry_count = 0;
            let mut schema_retry_count = 0;
            // Exponential backoff for conflict retries to reduce thundering herd effect
            let mut backoff = ExponentialBackoff::default().max_delay(Duration::from_secs(30));

            loop {
                // Log retry attempt for schema evolution
                if schema_retry_count > 0 {
                    info!(
                        message = "Retrying write after schema reload",
                        retry_count = schema_retry_count,
                    );
                }

                // Build write operation using DeltaTable methods directly
                // This supports schema merge mode for true schema evolution
                // Clone batches for potential retries (RecordBatch clone is cheap - uses Arc internally)
                let mut write_builder = table
                    .clone()
                    .write(batches.clone())
                    .with_save_mode(SaveMode::Append);

                // Enable schema merge when schema evolution is enabled
                // This allows adding new columns from incoming data
                if schema_evolution {
                    write_builder = write_builder.with_schema_mode(SchemaMode::Merge);
                }

                // Execute write and commit
                match write_builder.await {
                    Ok(new_table) => {
                        // Update schema cache for request builder (used for schema evolution)
                        // We don't cache the full table state since we reload before each write,
                        // which bounds memory usage by not accumulating transaction log state.
                        let new_schema = new_table.schema();
                        let old_schema = shared_schema.load();

                        // Log and update schema if new fields were added
                        let new_fields: Vec<_> = new_schema
                            .fields()
                            .iter()
                            .filter(|f| old_schema.field_with_name(f.name()).is_err())
                            .map(|f| f.name().as_str())
                            .collect();

                        if !new_fields.is_empty() {
                            info!(
                                message = "Schema evolution: new fields added to table",
                                new_fields = ?new_fields,
                                total_fields = new_schema.fields().len(),
                                version = new_table.version(),
                            );
                            shared_schema.store(new_schema);
                        }

                        // Create checkpoint at regular intervals to compact the transaction log.
                        // This reduces memory usage on subsequent table loads by allowing readers
                        // to start from the checkpoint instead of reading the full log.
                        if let Some(version) = new_table.version() {
                            if version > 0 && version % CHECKPOINT_INTERVAL == 0 {
                                info!(
                                    message = "Creating checkpoint for Delta table",
                                    version = version,
                                );
                                match create_checkpoint(&new_table, None).await {
                                    Ok(()) => {
                                        info!(
                                            message = "Successfully created checkpoint",
                                            version = version,
                                        );
                                    }
                                    Err(e) => {
                                        // Log but don't fail the write - checkpointing is best-effort
                                        warn!(
                                            message = "Failed to create checkpoint, will retry on next interval",
                                            version = version,
                                            error = %e,
                                        );
                                    }
                                }
                            }
                        }

                        // Get the byte size from the request (Arrow in-memory size)
                        let bytes_written = request.byte_size;

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
                        // Concurrent conflicts can manifest as:
                        // 1. Transaction errors with ConcurrentDeleteRead (from optimize/vacuum)
                        // 2. ObjectStore errors with FAILED_PRECONDITION (from concurrent writes)
                        //    GCS returns FAILED_PRECONDITION when conditional write (if-generation-match)
                        //    fails because another writer committed the same version first.
                        let error_str = e.to_string();
                        let is_concurrent_conflict = matches!(
                            &e,
                            DeltaTableError::Transaction { source }
                                if source.to_string().contains("ConcurrentDeleteRead")
                                    || source.to_string().contains("concurrent transaction deleted")
                        ) || matches!(
                            &e,
                            DeltaTableError::ObjectStore { .. }
                                if error_str.contains("FAILED_PRECONDITION")
                                    || error_str.contains("precondition")
                                    || error_str.contains("Precondition")
                        );

                        let is_schema_mismatch = Self::is_schema_mismatch_error(&e);

                        // Handle schema mismatch errors with reload and retry (if enabled)
                        if schema_evolution
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

                            // Apply exponential backoff before retry to reduce thundering herd
                            let backoff_duration =
                                backoff.next().unwrap_or(Duration::from_secs(30));
                            warn!(
                                message = "Concurrent table modification detected, backing off then reloading",
                                retry_count = conflict_retry_count,
                                max_retries = MAX_CONFLICT_RETRIES,
                                backoff_ms = backoff_duration.as_millis(),
                            );
                            tokio::time::sleep(backoff_duration).await;

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
        let error_str = error.to_string();

        // Don't retry FAILED_PRECONDITION at Tower level - these are concurrent write
        // conflicts that are handled internally with table reload. Retrying here without
        // reload would cause infinite loops with stale version numbers.
        if error_str.contains("FAILED_PRECONDITION")
            || error_str.contains("precondition")
            || error_str.contains("Precondition")
        {
            return false;
        }

        match error {
            // Retry on storage/network errors (but not precondition failures, checked above)
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
