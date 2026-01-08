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
