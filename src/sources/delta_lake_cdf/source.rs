//! Main source logic for Delta Lake CDF streaming.

use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::DeltaTable;
use deltalake::DeltaTableError;
use std::sync::Arc;
use tokio::time::interval;
use vector_lib::config::LogNamespace;
use vector_lib::internal_event::{
    ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol,
};
use vector_lib::EstimatedJsonEncodedSizeOf;

use crate::internal_events::{EventsReceived, StreamClosedError};
use crate::shutdown::ShutdownSignal;
use crate::SourceSender;

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
                if let Some(end_version) = config.ending_version {
                    if current_version > end_version {
                        info!(
                            message = "Reached ending version, stopping",
                            ending_version = end_version,
                        );
                        return Ok(());
                    }
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

                // Load CDF data for the version range
                match load_cdf_data(&ctx, &table, current_version, end_version).await {
                    Ok(batches) => {
                        if batches.is_empty() {
                            debug!(message = "No CDF data in version range");
                            current_version = end_version + 1;
                            continue;
                        }

                        // Calculate byte size for metrics
                        let byte_size: usize = batches.iter()
                            .map(|b| b.get_array_memory_size())
                            .sum();
                        bytes_received.emit(ByteSize(byte_size));

                        // Convert to Vector events
                        match convert_cdf_batches_to_events(batches, &config, log_namespace) {
                            Ok(events) => {
                                if events.is_empty() {
                                    debug!(message = "All events filtered out");
                                    current_version = end_version + 1;
                                    continue;
                                }

                                let event_count = events.len();
                                let json_size = events.estimated_json_encoded_size_of();
                                events_received.emit(CountByteSize(event_count, json_size));

                                // Send events downstream
                                if let Err(_) = out.send_batch(events).await {
                                    emit!(StreamClosedError { count: event_count });
                                    return Err(());
                                }

                                debug!(
                                    message = "Sent CDF events",
                                    count = event_count,
                                    versions = format!("{}..{}", current_version, end_version),
                                );

                                // Update checkpoint
                                current_version = end_version + 1;
                                if let Err(e) = checkpointer.write_checkpoint(current_version) {
                                    error!(
                                        message = "Failed to save checkpoint",
                                        error = %e,
                                        version = current_version,
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    message = "Failed to convert CDF data to events",
                                    error = %e,
                                );
                            }
                        }
                    }
                    Err(e) => {
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
                                error!(
                                    message = "CDF data not available for version (may have been removed by VACUUM)",
                                    %version,
                                    hint = "Delete checkpoint file to restart from available version",
                                );
                                return Err(());
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

/// Load Change Data Feed data from Delta table.
async fn load_cdf_data(
    ctx: &SessionContext,
    table: &DeltaTable,
    start_version: i64,
    end_version: i64,
) -> Result<Vec<deltalake::arrow::record_batch::RecordBatch>, DeltaTableError> {
    // Clone table and create CDF builder
    let cdf_builder = table
        .clone()
        .scan_cdf()
        .with_starting_version(start_version)
        .with_ending_version(end_version);

    // Create CDF table provider
    let cdf_provider = DeltaCdfTableProvider::try_new(cdf_builder)?;

    // Execute the CDF scan using DataFusion
    let df = ctx.read_table(Arc::new(cdf_provider))?;
    let batches = df.collect().await?;

    Ok(batches)
}
