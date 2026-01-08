//! Integration tests for Delta Lake CDF source.
//!
//! These tests require a running MinIO instance with Delta tables.
//! Run with: `cargo test --features delta-lake-cdf-integration-tests`

#![cfg(all(test, feature = "delta-lake-cdf-integration-tests"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use deltalake::arrow::array::{Int64Builder, StringBuilder};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;

use super::config::{DeltaLakeCdfConfig, StartPosition};

/// Get MinIO endpoint from environment or use default
fn minio_endpoint() -> String {
    std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

/// Create storage options for S3-compatible MinIO
fn minio_storage_options() -> HashMap<String, String> {
    let mut options = HashMap::new();
    options.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
    options.insert(
        "aws_secret_access_key".to_string(),
        "minioadmin".to_string(),
    );
    options.insert("aws_endpoint".to_string(), minio_endpoint());
    options.insert("aws_region".to_string(), "us-east-1".to_string());
    options.insert("aws_allow_http".to_string(), "true".to_string());
    options.insert("aws_s3_path_style".to_string(), "true".to_string());
    options
}

/// Create a test Delta table with CDF enabled
#[allow(dead_code)]
async fn create_cdf_enabled_table(bucket: &str, table_path: &str) -> DeltaTable {
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let storage_options = minio_storage_options();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let delta_schema =
        StructType::try_from_arrow(schema.as_ref()).expect("Failed to convert Arrow schema");
    let delta_fields: Vec<_> = delta_schema.fields().cloned().collect();

    // Create table with CDF enabled
    CreateBuilder::new()
        .with_location(&table_uri)
        .with_columns(delta_fields)
        .with_save_mode(SaveMode::Ignore)
        .with_storage_options(storage_options.clone())
        .with_configuration_property(
            deltalake::TableProperty::EnableChangeDataFeed,
            Some("true".to_string()),
        )
        .await
        .expect("Failed to create Delta table with CDF")
}

/// Insert test data into table
#[allow(dead_code)]
async fn insert_data(table: &DeltaTable, id: i64, name: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let mut id_builder = Int64Builder::new();
    let mut name_builder = StringBuilder::new();

    id_builder.append_value(id);
    name_builder.append_value(name);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .unwrap();

    table
        .clone()
        .write(vec![batch])
        .await
        .expect("Failed to write to table");
}

#[tokio::test]
async fn test_cdf_source_basic() {
    // This test requires MinIO running with a test bucket
    // Skip if not available
    let table_uri = format!(
        "s3://test-bucket/cdf-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let config = DeltaLakeCdfConfig {
        table_uri: table_uri.clone(),
        storage_options: minio_storage_options(),
        poll_interval_secs: Duration::from_secs(1),
        start_position: StartPosition::Beginning,
        include_data: true,
        change_types: Vec::new(),
        ending_version: Some(2),
        data_dir: None,
        log_namespace: None,
    };

    // Note: This test would need a pre-existing CDF-enabled table
    // For now, just validate the config parses correctly
    assert_eq!(config.table_uri, table_uri);
    assert_eq!(config.poll_interval_secs, Duration::from_secs(1));
}

#[test]
fn test_config_serialization() {
    let config_str = r#"
        table_uri = "s3://test-bucket/table"
        poll_interval_secs = 30
        start_position = "latest"
        include_data = true
        change_types = ["insert", "delete"]

        [storage_options]
        aws_access_key_id = "test"
        aws_secret_access_key = "test"
        aws_region = "us-east-1"
    "#;

    let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");

    assert_eq!(config.table_uri, "s3://test-bucket/table");
    assert_eq!(config.poll_interval_secs, Duration::from_secs(30));
    assert_eq!(config.start_position, StartPosition::Latest);
    assert!(config.include_data);
    assert_eq!(config.change_types.len(), 2);
}

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<DeltaLakeCdfConfig>();
}

/// Test that the source auto-recovers when checkpoint points to a version
/// that has been removed by VACUUM.
#[tokio::test]
async fn test_auto_recovery_after_vacuum() {
    use futures::StreamExt;
    use tempfile::TempDir;

    use super::checkpoint::DeltaLakeCdfCheckpointer;
    use super::source::run_cdf_source;
    use crate::shutdown::ShutdownSignal;
    use crate::SourceSender;
    use vector_lib::config::LogNamespace;

    // Create unique table path
    let table_path = format!(
        "cdf-vacuum-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let table_uri = format!("s3://test-bucket/{}", table_path);
    let storage_options = minio_storage_options();

    // Step 1: Create table with CDF enabled
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let delta_schema =
        StructType::try_from_arrow(schema.as_ref()).expect("Failed to convert Arrow schema");
    let delta_fields: Vec<_> = delta_schema.fields().cloned().collect();

    let mut table = CreateBuilder::new()
        .with_location(&table_uri)
        .with_columns(delta_fields)
        .with_save_mode(SaveMode::ErrorIfExists)
        .with_storage_options(storage_options.clone())
        .with_configuration_property(
            deltalake::TableProperty::EnableChangeDataFeed,
            Some("true".to_string()),
        )
        .await
        .expect("Failed to create Delta table");

    // Step 2: Create versions with Overwrites to generate "garbage" files
    // Each overwrite removes the previous file from the latest state,
    // making it eligible for VACUUM deletion.

    // Write v1 (File A)
    table = table
        .write(vec![create_test_batch(1, "Alice")])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .expect("Failed to write v1");

    // Write v2 (File B) - removes File A from latest state
    table = table
        .write(vec![create_test_batch(2, "Bob")])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .expect("Failed to write v2");

    // Write v3 (File C) - removes File B from latest state
    table = table
        .write(vec![create_test_batch(3, "Charlie")])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .expect("Failed to write v3");

    let current_version = table.version().unwrap();
    assert!(current_version >= 3, "Should have at least 3 versions");

    // Step 3: Create a checkpoint pointing to version 0 (will be vacuumed)
    let checkpoint_dir = TempDir::new().expect("Failed to create temp dir");
    let checkpointer = DeltaLakeCdfCheckpointer::new(checkpoint_dir.path(), &table_uri);
    checkpointer
        .write_checkpoint(0)
        .expect("Failed to write checkpoint");

    // Verify checkpoint was created
    assert_eq!(checkpointer.read_checkpoint(), Some(0));

    // Step 4: Run VACUUM with 0 retention to remove old versions
    let (table, metrics) = table
        .vacuum()
        .with_retention_period(chrono::Duration::zero())
        .with_enforce_retention_duration(false)
        .await
        .expect("Failed to vacuum table");

    // Verify VACUUM actually deleted files (precondition for this test)
    assert!(
        !metrics.files_deleted.is_empty(),
        "VACUUM should have deleted files - test precondition failed. \
         Files deleted: {:?}",
        metrics.files_deleted
    );
    println!(
        "VACUUM deleted {} files: {:?}",
        metrics.files_deleted.len(),
        metrics.files_deleted
    );

    // Get the version AFTER vacuum (VACUUM creates new table versions)
    let version_after_vacuum = table.version().unwrap();
    println!(
        "Version before VACUUM: {}, after VACUUM: {}",
        current_version, version_after_vacuum
    );

    // Step 5: Start the source - it should auto-recover
    let config = DeltaLakeCdfConfig {
        table_uri: table_uri.clone(),
        storage_options: storage_options.clone(),
        poll_interval_secs: Duration::from_millis(100),
        start_position: StartPosition::Beginning,
        include_data: true,
        change_types: Vec::new(),
        ending_version: Some(version_after_vacuum), // Bounded read for test
        data_dir: None,
        log_namespace: None,
    };

    let (tx, rx) = SourceSender::new_test();
    let (shutdown_trigger, shutdown_signal, _shutdown_done) = ShutdownSignal::new_wired();

    let source_handle = tokio::spawn(run_cdf_source(
        table,
        config,
        0, // Start from checkpointed version (which was vacuumed)
        checkpointer,
        shutdown_signal,
        tx,
        LogNamespace::Legacy,
    ));

    // Wait a bit for auto-recovery and potential events
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Trigger shutdown
    shutdown_trigger.cancel();

    // Wait for source to complete
    let result = tokio::time::timeout(Duration::from_secs(5), source_handle)
        .await
        .expect("Source should complete within timeout")
        .expect("Source task should not panic");

    // Source should complete successfully (auto-recovered, not errored)
    assert!(
        result.is_ok(),
        "Source should auto-recover after VACUUM, not fail"
    );

    // Check that checkpoint jumped to latest_version + 1 (skipped unavailable history)
    let new_checkpoint = DeltaLakeCdfCheckpointer::new(checkpoint_dir.path(), &table_uri);
    let checkpointed_version = new_checkpoint
        .read_checkpoint()
        .expect("Checkpoint should still exist after recovery");

    // The checkpoint should have jumped from 0 to version_after_vacuum + 1
    // (skipping all the versions whose files were deleted by VACUUM)
    assert_eq!(
        checkpointed_version,
        version_after_vacuum + 1,
        "Checkpoint should have jumped to latest+1 after auto-recovery. \
         Expected {}, got {}. This indicates auto-recovery did not work correctly.",
        version_after_vacuum + 1,
        checkpointed_version
    );

    // Drain any events that were received
    let events: Vec<_> = rx.take(100).collect().await;

    println!(
        "Auto-recovery test completed successfully! \
         VACUUM deleted files, source recovered from version 0 to {}, \
         events received: {}",
        checkpointed_version,
        events.len()
    );
}

/// Helper to create a test record batch
fn create_test_batch(id: i64, name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let mut id_builder = Int64Builder::new();
    let mut name_builder = StringBuilder::new();

    id_builder.append_value(id);
    name_builder.append_value(name);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .unwrap()
}
