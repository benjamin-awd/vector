use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use deltalake::DeltaTableError;
use deltalake::ObjectStoreError;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use url::Url;

use crate::common::backoff::ExponentialBackoff;
use crate::internal_events::EndpointBytesSent;
use crate::sinks::prelude::*;

use super::request_builder::{DeltaLakeRequest, SharedSchema};

/// Classification of Delta Lake write errors.
///
/// Provides a single source of truth for error handling decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteErrorKind {
    /// Concurrent transaction conflict (e.g., from optimize/vacuum or another writer).
    /// Retried internally with table reload.
    ConcurrentConflict,
    /// Schema mismatch between incoming data and table schema.
    /// Retried internally with schema reload (if schema evolution is enabled).
    SchemaMismatch,
    /// Transient error (network, IO, timeout). Retried at Tower level.
    Transient,
    /// Non-retriable error (permissions, not found, invalid config, etc.).
    NonRetriable,
}

impl WriteErrorKind {
    /// Classify a DeltaTableError into a WriteErrorKind.
    pub fn from_delta_error(error: &DeltaTableError) -> Self {
        match error {
            DeltaTableError::ObjectStore { source } => Self::from_object_store_error(source),

            DeltaTableError::Transaction { source } => {
                let s = source.to_string();
                if s.contains("ConcurrentDeleteRead")
                    || s.contains("concurrent transaction deleted")
                {
                    WriteErrorKind::ConcurrentConflict
                } else {
                    WriteErrorKind::NonRetriable
                }
            }

            DeltaTableError::Arrow { .. }
            | DeltaTableError::InvalidData { .. }
            | DeltaTableError::SchemaMismatch { .. } => WriteErrorKind::SchemaMismatch,

            DeltaTableError::Io { .. } => WriteErrorKind::Transient,

            DeltaTableError::Kernel { .. } => WriteErrorKind::NonRetriable,

            _ => {
                let error_str = error.to_string().to_lowercase();
                if error_str.contains("schema")
                    || error_str.contains("field")
                    || error_str.contains("column")
                    || error_str.contains("incompatible")
                    || error_str.contains("type mismatch")
                {
                    WriteErrorKind::SchemaMismatch
                } else {
                    WriteErrorKind::Transient
                }
            }
        }
    }

    fn from_object_store_error(error: &ObjectStoreError) -> Self {
        match error {
            ObjectStoreError::Precondition { .. } | ObjectStoreError::AlreadyExists { .. } => {
                WriteErrorKind::ConcurrentConflict
            }
            ObjectStoreError::PermissionDenied { .. }
            | ObjectStoreError::Unauthenticated { .. }
            | ObjectStoreError::NotFound { .. }
            | ObjectStoreError::NotSupported { .. }
            | ObjectStoreError::NotImplemented
            | ObjectStoreError::UnknownConfigurationKey { .. }
            | ObjectStoreError::InvalidPath { .. } => WriteErrorKind::NonRetriable,
            _ => WriteErrorKind::Transient,
        }
    }

    /// Returns true if this error should be retried at the Tower level.
    pub fn is_retriable_at_tower_level(&self) -> bool {
        matches!(self, WriteErrorKind::Transient)
    }

    /// Returns true if this is a concurrent conflict requiring table reload.
    pub fn is_concurrent_conflict(&self) -> bool {
        matches!(self, WriteErrorKind::ConcurrentConflict)
    }

    /// Returns true if this is a schema mismatch requiring schema reload.
    pub fn is_schema_mismatch(&self) -> bool {
        matches!(self, WriteErrorKind::SchemaMismatch)
    }
}

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
                        // Classify the error using WriteErrorKind for consistent handling
                        let error_kind = WriteErrorKind::from_delta_error(&e);

                        // Log error classification for debugging retry behavior
                        warn!(
                            message = "Delta Lake write error occurred",
                            error = %e,
                            error_debug = ?e,
                            error_kind = ?error_kind,
                            conflict_retry_count = conflict_retry_count,
                            schema_retry_count = schema_retry_count,
                        );

                        // Handle schema mismatch errors with reload and retry (if enabled)
                        if schema_evolution
                            && error_kind.is_schema_mismatch()
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
                        if error_kind.is_concurrent_conflict()
                            && conflict_retry_count < MAX_CONFLICT_RETRIES
                        {
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

                        // Not retriable, or exhausted retries - log appropriate message
                        match error_kind {
                            WriteErrorKind::SchemaMismatch => {
                                error!(
                                    message = "Exhausted retries for schema mismatch",
                                    error = %e,
                                    retry_count = schema_retry_count,
                                );
                            }
                            WriteErrorKind::ConcurrentConflict => {
                                error!(
                                    message = "Exhausted retries for concurrent conflict",
                                    error = %e,
                                    retry_count = conflict_retry_count,
                                );
                            }
                            _ => {
                                error!(
                                    message = "Non-retriable Delta Lake error",
                                    error = %e,
                                    error_kind = ?error_kind,
                                );
                            }
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
/// Determines which errors are retriable at the Tower level.
/// Uses WriteErrorKind for consistent error classification across the codebase.
#[derive(Debug, Clone, Default)]
pub struct DeltaLakeRetryLogic;

impl RetryLogic for DeltaLakeRetryLogic {
    type Error = DeltaTableError;
    type Request = DeltaLakeRequest;
    type Response = DeltaLakeResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        WriteErrorKind::from_delta_error(error).is_retriable_at_tower_level()
    }
}
