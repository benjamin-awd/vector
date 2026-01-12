//! Main source logic for Delta Lake CDF streaming.

use deltalake::DeltaTable;
use deltalake::DeltaTableError;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use futures::StreamExt;
use std::sync::Arc;
use tokio::time::interval;
use vector_lib::EstimatedJsonEncodedSizeOf;
use vector_lib::config::LogNamespace;
use vector_lib::internal_event::{
    ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol,
};

use crate::SourceSender;
use crate::internal_events::{EventsReceived, StreamClosedError};
use crate::shutdown::ShutdownSignal;

use super::checkpoint::DeltaLakeCdfCheckpointer;
use super::config::DeltaLakeCdfConfig;
use super::event::convert_cdf_batches_to_events;

/// Run the CDF source main loop.
///
/// This function:
/// 1. Polls the Delta table at configured intervals for new versions
/// 2. Reads Change Data Feed records for any new versions
/// 3. Converts records to Vector events and sends them downstream
/// 4. Persists checkpoints for resumption after restart
pub async fn run_cdf_source(
    mut table: DeltaTable,
    config: DeltaLakeCdfConfig,
    mut current_version: i64,
    checkpointer: DeltaLakeCdfCheckpointer,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    let poll_interval = config.poll_interval_secs;
    let mut ticker = interval(poll_interval);

    // Create DataFusion context once for reuse
    let ctx = SessionContext::new();

    // Register metrics
    let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
    let events_received = register!(EventsReceived);

    info!(
        message = "Delta Lake CDF source started",
        table_uri = %config.table_uri,
        current_version = current_version,
    );

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                // Save checkpoint before shutting down
                if let Err(e) = checkpointer.write_checkpoint(current_version) {
                    error!(
                        message = "Failed to save checkpoint on shutdown",
                        error = %e,
                    );
                }
                info!(
                    message = "Delta Lake CDF source shutting down",
                    final_version = current_version,
                );
                return Ok(());
            }

            _ = ticker.tick() => {
                // Reload table to check for new versions
                if let Err(e) = table.load().await {
                    error!(
                        message = "Failed to reload Delta table",
                        error = %e,
                    );
                    continue;
                }

                let latest_version = table.version().unwrap_or(0);

                // Check for bounded read completion
                if let Some(end_version) = config.ending_version
                    && current_version > end_version
                {
                    info!(
                        message = "Reached ending version, stopping",
                        ending_version = end_version,
                    );
                    return Ok(());
                }

                // Skip if no new versions
                if latest_version < current_version {
                    debug!(
                        message = "No new versions available",
                        current_version = current_version,
                        latest_version = latest_version,
                    );
                    continue;
                }

                // Determine the end version for this batch
                let end_version = config
                    .ending_version
                    .map(|e| e.min(latest_version))
                    .unwrap_or(latest_version);

                debug!(
                    message = "Processing CDF versions",
                    start_version = current_version,
                    end_version = end_version,
                );

                // Create streaming CDF reader for the version range
                match create_cdf_stream(&ctx, &table, current_version, end_version).await {
                    Ok(mut stream) => {
                        let mut total_events: usize = 0;
                        let mut stream_error = false;

                        // Process batches as they arrive - no memory accumulation
                        while let Some(batch_result) = stream.next().await {
                            match batch_result {
                                Ok(batch) => {
                                    if batch.num_rows() == 0 {
                                        continue;
                                    }

                                    // Emit byte metrics for this batch
                                    let byte_size = batch.get_array_memory_size();
                                    bytes_received.emit(ByteSize(byte_size));

                                    // Convert batch to events
                                    match convert_cdf_batches_to_events(vec![batch], &config, log_namespace) {
                                        Ok(events) => {
                                            if events.is_empty() {
                                                continue;
                                            }

                                            let event_count = events.len();
                                            let json_size = events.estimated_json_encoded_size_of();
                                            events_received.emit(CountByteSize(event_count, json_size));

                                            // Send events downstream immediately
                                            if out.send_batch(events).await.is_err() {
                                                emit!(StreamClosedError { count: event_count });
                                                return Err(());
                                            }

                                            total_events += event_count;
                                        }
                                        Err(e) => {
                                            error!(
                                                message = "Failed to convert CDF batch to events",
                                                error = %e,
                                            );
                                            stream_error = true;
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Check if this is a "file not found" error
                                    if is_file_not_found_error(&e) {
                                        let new_version = latest_version + 1;
                                        warn!(
                                            message = "CDF data files not found during stream (likely removed by VACUUM), skipping to latest",
                                            error = %e,
                                            old_version = current_version,
                                            new_version = new_version,
                                        );
                                        current_version = new_version;
                                        if let Err(e) = checkpointer.write_checkpoint(current_version) {
                                            error!(message = "Failed to save checkpoint", error = %e);
                                        }
                                        stream_error = true;
                                        break;
                                    }

                                    error!(
                                        message = "Error reading CDF stream",
                                        error = %e,
                                    );
                                    stream_error = true;
                                    break;
                                }
                            }
                        }

                        // Only update checkpoint if stream completed successfully
                        if !stream_error {
                            if total_events > 0 {
                                debug!(
                                    message = "Sent CDF events",
                                    count = total_events,
                                    versions = format!("{}..{}", current_version, end_version),
                                );
                            } else {
                                debug!(message = "No CDF data in version range");
                            }

                            current_version = end_version + 1;
                            if let Err(e) = checkpointer.write_checkpoint(current_version) {
                                error!(
                                    message = "Failed to save checkpoint",
                                    error = %e,
                                    version = current_version,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // Check if this is a "file not found" error (vacuum deleted the files)
                        if is_file_not_found_error(&e) {
                            // Auto-recover: reset to latest version (skip unavailable history)
                            let new_version = latest_version + 1;
                            warn!(
                                message = "CDF data files not found (likely removed by VACUUM), skipping to latest",
                                error = %e,
                                old_version = current_version,
                                new_version = new_version,
                            );
                            current_version = new_version;
                            if let Err(e) = checkpointer.write_checkpoint(current_version) {
                                error!(message = "Failed to save checkpoint", error = %e);
                            }
                            continue;
                        }

                        match &e {
                            DeltaTableError::ChangeDataNotEnabled { version } => {
                                error!(
                                    message = "Change Data Feed is not enabled on table",
                                    %version,
                                    hint = "Enable CDF with: ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",
                                );
                                return Err(());
                            }
                            DeltaTableError::ChangeDataNotRecorded { version, .. } => {
                                // Auto-recover: reset to latest version (skip unavailable history)
                                let new_version = latest_version + 1;
                                warn!(
                                    message = "CDF data not available for version (likely removed by VACUUM), skipping to latest",
                                    unavailable_version = %version,
                                    new_version = new_version,
                                );
                                current_version = new_version;
                                if let Err(e) = checkpointer.write_checkpoint(current_version) {
                                    error!(message = "Failed to save checkpoint", error = %e);
                                }
                                continue;
                            }
                            DeltaTableError::ChangeDataInvalidVersionRange { start, end } => {
                                error!(
                                    message = "Invalid CDF version range",
                                    start_version = %start,
                                    end_version = %end,
                                );
                            }
                            _ => {
                                error!(
                                    message = "Failed to load CDF data",
                                    error = %e,
                                    start_version = current_version,
                                    end_version = end_version,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Create a streaming CDF reader for a version range.
///
/// Returns a stream of RecordBatches instead of collecting all data into memory.
/// This allows processing large version ranges incrementally without OOM risk.
async fn create_cdf_stream(
    ctx: &SessionContext,
    table: &DeltaTable,
    start_version: i64,
    end_version: i64,
) -> Result<impl futures::Stream<Item = Result<deltalake::arrow::record_batch::RecordBatch, DeltaTableError>>, DeltaTableError> {
    // Clone table and create CDF builder
    let cdf_builder = table
        .clone()
        .scan_cdf()
        .with_starting_version(start_version)
        .with_ending_version(end_version);

    // Create CDF table provider
    let cdf_provider = DeltaCdfTableProvider::try_new(cdf_builder)?;

    // Execute the CDF scan using DataFusion and return as stream
    let df = ctx.read_table(Arc::new(cdf_provider))?;
    let stream = df.execute_stream().await?;

    // Map the DataFusion error type to DeltaTableError
    Ok(stream.map(|result| {
        result.map_err(|e| DeltaTableError::Generic(format!("DataFusion stream error: {}", e)))
    }))
}

/// Check if an error indicates that files were not found (likely deleted by VACUUM).
fn is_file_not_found_error(error: &DeltaTableError) -> bool {
    let error_str = error.to_string();
    error_str.contains("not found")
        || error_str.contains("404")
        || error_str.contains("NoSuchKey")
        || error_str.contains("NotFound")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detects_s3_not_found_error() {
        // S3-style error
        let error = DeltaTableError::Generic(
            "Failed to parse parquet: External: Object at location ... not found: \
             Error performing GET ... 404 Not Found: NoSuchKey"
                .to_string(),
        );
        assert!(is_file_not_found_error(&error));
    }

    #[test]
    fn test_detects_gcs_not_found_error() {
        // GCS-style error (from user's actual error)
        let error = DeltaTableError::Generic(
            "Failed to parse parquet: External: Object at location \
             delta/vector_events/part-00000-xxx.snappy.parquet not found: \
             Error performing GET https://storage.googleapis.com/... \
             404 Not Found: NoSuchKey"
                .to_string(),
        );
        assert!(is_file_not_found_error(&error));
    }

    #[test]
    fn test_detects_azure_not_found_error() {
        // Azure-style error
        let error = DeltaTableError::Generic("Object not found: BlobNotFound".to_string());
        assert!(is_file_not_found_error(&error));
    }

    #[test]
    fn test_does_not_match_unrelated_errors() {
        let error =
            DeltaTableError::Generic("Schema mismatch: expected 5 columns, got 3".to_string());
        assert!(!is_file_not_found_error(&error));

        let error = DeltaTableError::Generic("Connection timeout after 30 seconds".to_string());
        assert!(!is_file_not_found_error(&error));
    }
}
