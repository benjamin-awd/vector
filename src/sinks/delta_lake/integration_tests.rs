#![cfg(all(test, feature = "delta-lake-integration-tests"))]

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::Array;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, open_table_with_storage_options};
use url::Url;

use crate::config::SinkConfig;

/// Get MinIO endpoint from environment or use default
fn minio_endpoint() -> String {
    std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

/// Create storage options for S3-compatible MinIO
fn minio_storage_options() -> HashMap<String, String> {
    let mut options = HashMap::new();

    // MinIO credentials
    options.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
    options.insert(
        "aws_secret_access_key".to_string(),
        "minioadmin".to_string(),
    );

    // MinIO endpoint
    let endpoint = minio_endpoint();
    options.insert("aws_endpoint".to_string(), endpoint);
    options.insert("aws_region".to_string(), "us-east-1".to_string());

    // S3-compatible settings
    options.insert("aws_allow_http".to_string(), "true".to_string());
    options.insert("aws_s3_path_style".to_string(), "true".to_string());

    options
}

/// Create a new Delta Lake table in MinIO
async fn create_delta_table(bucket: &str, table_path: &str, schema: Arc<Schema>) -> DeltaTable {
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let storage_options = minio_storage_options();

    // Convert Arrow schema to Delta schema using built-in conversion
    let delta_schema = StructType::try_from_arrow(schema.as_ref())
        .expect("Failed to convert Arrow schema to Delta schema");
    let delta_fields: Vec<_> = delta_schema.fields().cloned().collect();

    // Create table using Delta operations
    CreateBuilder::new()
        .with_location(&table_uri)
        .with_columns(delta_fields)
        .with_save_mode(SaveMode::Ignore)
        .with_storage_options(storage_options.clone())
        .await
        .expect("Failed to create Delta table")
}

/// Open an existing Delta table from MinIO
async fn open_delta_table(bucket: &str, table_path: &str) -> DeltaTable {
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let table_url = Url::parse(&table_uri).expect("Failed to parse table URI");
    let storage_options = minio_storage_options();

    open_table_with_storage_options(table_url, storage_options)
        .await
        .expect("Failed to open Delta table")
}

/// Read-path validation helper: verifies data can be read back correctly from a Delta table.
///
/// This function uses DataFusion to query the Delta table and verify:
/// - Data is readable (not corrupted)
/// - Row counts match expectations
/// - Column values can be accessed
///
/// # Arguments
/// * `table` - The Delta table to read from (should be loaded with latest snapshot)
/// * `expected_min_rows` - Minimum number of rows expected in the table
///
/// # Returns
/// The total number of rows read from the table
async fn assert_data_readable(table: &DeltaTable, expected_min_rows: usize) -> usize {
    let ctx = SessionContext::new();

    // Register the Delta table with DataFusion
    ctx.register_table("delta_table", Arc::new(table.clone()))
        .expect("Failed to register Delta table with DataFusion");

    // Query all data from the table
    let df = ctx
        .sql("SELECT * FROM delta_table")
        .await
        .expect("Failed to execute SQL query");

    // Collect results to verify data is readable
    let batches = df.collect().await.expect("Failed to collect query results");

    // Count total rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert!(
        total_rows >= expected_min_rows,
        "Expected at least {} rows, but found {}",
        expected_min_rows,
        total_rows
    );

    total_rows
}

/// Read-path validation helper: verifies specific column values exist in the table.
///
/// This function uses DataFusion to query the Delta table and verify that
/// a specific value exists in a column.
///
/// # Arguments
/// * `table` - The Delta table to read from
/// * `column` - The column name to check
/// * `expected_value` - The string value expected to exist in the column
///
/// # Returns
/// The number of rows matching the filter
async fn assert_column_value_exists(
    table: &DeltaTable,
    column: &str,
    expected_value: &str,
) -> usize {
    let ctx = SessionContext::new();

    ctx.register_table("delta_table", Arc::new(table.clone()))
        .expect("Failed to register Delta table with DataFusion");

    // Query with filter for the specific value
    let query = format!(
        "SELECT * FROM delta_table WHERE {} = '{}'",
        column, expected_value
    );
    let df = ctx
        .sql(&query)
        .await
        .expect("Failed to execute filtered SQL query");

    let batches = df
        .collect()
        .await
        .expect("Failed to collect filtered results");

    let matching_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert!(
        matching_rows > 0,
        "Expected to find rows where {} = '{}', but found none",
        column,
        expected_value
    );

    matching_rows
}

/// Polls a Delta table until a condition is met or timeout occurs.
///
/// # Arguments
/// * `bucket` - The S3 bucket name
/// * `table_path` - The path to the table within the bucket
/// * `condition` - A closure that takes a &DeltaTable and returns true when the condition is met
/// * `timeout_msg` - Message to display if timeout occurs
///
/// # Returns
/// The loaded DeltaTable once the condition is satisfied
async fn poll_table_until<F>(
    bucket: &str,
    table_path: &str,
    condition: F,
    timeout_msg: &str,
) -> DeltaTable
where
    F: Fn(&DeltaTable) -> bool,
{
    let mut table = open_delta_table(bucket, table_path).await;
    for attempt in 0..20 {
        table.load().await.expect("Failed to load table");
        if condition(&table) {
            return table;
        }
        if attempt == 19 {
            panic!("{}", timeout_msg);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    table
}

/// Read-path validation helper: reads all data and returns column values for verification.
///
/// This is useful for more complex assertions about the data content.
///
/// # Arguments
/// * `table` - The Delta table to read from
/// * `column` - The column name to extract values from
///
/// # Returns
/// A vector of string representations of the column values
async fn read_column_values(table: &DeltaTable, column: &str) -> Vec<String> {
    let ctx = SessionContext::new();

    ctx.register_table("delta_table", Arc::new(table.clone()))
        .expect("Failed to register Delta table with DataFusion");

    let query = format!("SELECT {} FROM delta_table", column);
    let df = ctx
        .sql(&query)
        .await
        .expect("Failed to execute column query");

    let batches = df
        .collect()
        .await
        .expect("Failed to collect column results");

    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() > 0 {
            let col = batch.column(0);
            for i in 0..col.len() {
                // Convert array value to string representation
                let value = if col.is_null(i) {
                    "NULL".to_string()
                } else {
                    // Use Arrow's display formatting
                    deltalake::arrow::util::display::array_value_to_string(col, i)
                        .unwrap_or_else(|_| "ERROR".to_string())
                };
                values.push(value);
            }
        }
    }

    values
}

#[tokio::test]
async fn test_delta_table_creation() {
    // Test that we can create a Delta Lake table in MinIO
    let bucket = "test-bucket";
    let table_path = format!("test-creation-{}", uuid::Uuid::new_v4());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
    ]));

    let _table = create_delta_table(bucket, &table_path, schema.clone()).await;

    // Verify table was created and can be opened
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    // Verify schema
    let table_schema = table.snapshot().unwrap().schema();
    let fields: Vec<_> = table_schema.fields().collect();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "id");
    assert_eq!(fields[1].name(), "message");
}

#[tokio::test]
async fn test_delta_table_schema_fields() {
    // Test schema field details
    let bucket = "test-bucket";
    let table_path = format!("test-schema-{}", uuid::Uuid::new_v4());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("count", DataType::Int32, true),
        Field::new(
            "created_at",
            // Delta Lake requires microsecond timestamps with timezone
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.unwrap();

    let table_schema = table.snapshot().unwrap().schema();
    let fields: Vec<_> = table_schema.fields().collect();
    assert_eq!(fields.len(), 4);

    // Verify field names
    let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id", "name", "count", "created_at"]);

    // Verify nullability
    assert!(!fields[0].is_nullable()); // id is not nullable
    assert!(fields[1].is_nullable()); // name is nullable
}

#[tokio::test]
async fn test_delta_table_multiple_tables() {
    // Test creating multiple independent tables
    let bucket = "test-bucket";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ]));

    // Create multiple tables
    let table1_path = format!("test-multi-1-{}", uuid::Uuid::new_v4());
    let table2_path = format!("test-multi-2-{}", uuid::Uuid::new_v4());
    let table3_path = format!("test-multi-3-{}", uuid::Uuid::new_v4());

    create_delta_table(bucket, &table1_path, schema.clone()).await;
    create_delta_table(bucket, &table2_path, schema.clone()).await;
    create_delta_table(bucket, &table3_path, schema.clone()).await;

    // Verify all can be opened independently
    let mut t1 = open_delta_table(bucket, &table1_path).await;
    let mut t2 = open_delta_table(bucket, &table2_path).await;
    let mut t3 = open_delta_table(bucket, &table3_path).await;

    t1.load().await.unwrap();
    t2.load().await.unwrap();
    t3.load().await.unwrap();

    // All should have version 0 (just created)
    assert_eq!(t1.version(), Some(0));
    assert_eq!(t2.version(), Some(0));
    assert_eq!(t3.version(), Some(0));
}

#[tokio::test]
async fn test_delta_table_with_complex_types() {
    // Test table with various complex Arrow types
    let bucket = "test-bucket";
    let table_path = format!("test-complex-{}", uuid::Uuid::new_v4());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("int8_field", DataType::Int8, true),
        Field::new("int16_field", DataType::Int16, true),
        Field::new("int32_field", DataType::Int32, true),
        Field::new("float32_field", DataType::Float32, true),
        Field::new("float64_field", DataType::Float64, true),
        Field::new("bool_field", DataType::Boolean, true),
        Field::new("binary_field", DataType::Binary, true),
        Field::new("string_field", DataType::Utf8, true),
    ]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.unwrap();

    let table_schema = table.snapshot().unwrap().schema();
    let fields: Vec<_> = table_schema.fields().collect();
    assert_eq!(fields.len(), 9);
}

#[tokio::test]
async fn test_delta_lake_basic_write() {
    use crate::config::SinkContext;
    use crate::sinks::delta_lake::DeltaLakeConfig;
    use crate::test_util::components::run_and_assert_sink_compliance;
    use futures::stream;
    use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    // Setup - create unique table for this test
    let bucket = "test-bucket";
    let table_path = format!("test-basic-write-{}", uuid::Uuid::new_v4());

    // Create Delta table with simple schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    // Build sink configuration
    // Using allow_nullable_fields: false to test strict schema enforcement
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let config = DeltaLakeConfig {
        table_uri,
        storage_options: minio_storage_options(),
        allow_nullable_fields: false,
        schema_evolution: false,
        batch: Default::default(),
        request: Default::default(),
        acknowledgements: Default::default(),
    };

    // Build the sink
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.expect("Failed to build sink");

    // Create test events with all required fields
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<Event> = (0..10)
        .map(|i| {
            let mut log = LogEvent::default();
            log.insert("message", format!("test message {}", i));
            log.insert("timestamp", i as i64);
            Event::Log(log)
        })
        .collect();

    let events_with_batch = events
        .into_iter()
        .map(|e| e.with_batch_notifier(&batch))
        .collect::<Vec<_>>();

    drop(batch);

    // Write events through the sink
    run_and_assert_sink_compliance(sink, stream::iter(events_with_batch), &[]).await;

    // Verify delivery
    assert_eq!(receiver.await, BatchStatus::Delivered);

    // Verify data was written to Delta Lake
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    // Verify table version increased (data was committed)
    assert!(
        table.version().unwrap() > 0,
        "Table version should be > 0 after write"
    );

    // Verify data was written by checking file URIs
    let files: Vec<_> = table
        .get_file_uris()
        .expect("Failed to get file URIs")
        .collect();
    assert!(!files.is_empty(), "No files written to Delta table");

    // Read-path validation: verify data can be read back correctly
    let total_rows = assert_data_readable(&table, 10).await;
    println!("Read back {} rows from Delta table", total_rows);

    // Verify specific message content exists
    assert_column_value_exists(&table, "message", "test message 5").await;

    // Read all message values and verify they contain expected patterns
    let messages = read_column_values(&table, "message").await;
    assert_eq!(messages.len(), 10, "Expected 10 messages");
    for i in 0..10 {
        let expected = format!("test message {}", i);
        assert!(
            messages.contains(&expected),
            "Missing message: {}",
            expected
        );
    }

    println!(
        "Successfully wrote and verified {} files to Delta table",
        files.len()
    );
}

#[tokio::test]
async fn test_delta_lake_schema_evolution() {
    use crate::config::SinkContext;
    use crate::sinks::delta_lake::DeltaLakeConfig;
    use crate::test_util::components::run_and_assert_sink_compliance;
    use futures::stream;
    use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    // Setup - create unique table for this test
    let bucket = "test-bucket";
    let table_path = format!("test-schema-evolution-{}", uuid::Uuid::new_v4());

    // Create Delta table with initial schema (only 'id' and 'name')
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    create_delta_table(bucket, &table_path, initial_schema.clone()).await;

    // Build first sink to write initial data
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let config1 = DeltaLakeConfig {
        table_uri: table_uri.clone(),
        storage_options: minio_storage_options(),
        allow_nullable_fields: true,
        schema_evolution: true,
        batch: Default::default(),
        request: Default::default(),
        acknowledgements: Default::default(),
    };

    let cx = SinkContext::default();
    let (sink1, _) = config1
        .build(cx.clone())
        .await
        .expect("Failed to build sink1");

    // Write initial events (only id and name)
    let (batch1, receiver1) = BatchNotifier::new_with_receiver();
    let events1: Vec<Event> = (0..5)
        .map(|i| {
            let mut log = LogEvent::default();
            log.insert("id", i as i64);
            log.insert("name", format!("user_{}", i));
            Event::Log(log)
        })
        .collect();

    let events1_with_batch = events1
        .into_iter()
        .map(|e| e.with_batch_notifier(&batch1))
        .collect::<Vec<_>>();
    drop(batch1);

    run_and_assert_sink_compliance(sink1, stream::iter(events1_with_batch), &[]).await;
    assert_eq!(receiver1.await, BatchStatus::Delivered);

    // Poll until table commit is visible (version > 0)
    let table = poll_table_until(
        bucket,
        &table_path,
        |t| t.version().unwrap_or(0) > 0,
        "Timeout waiting for initial write to commit",
    )
    .await;
    let version_after_first_write = table.version().unwrap();

    // Now manually add a new column to the Delta table schema
    // This simulates an external process (or another writer) evolving the schema
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    // Use Delta Lake's merge operation to add a new column
    // We'll do this by creating a record batch with the new schema and using DeltaTable methods
    use deltalake::arrow::array::{Int64Array, RecordBatch, StringArray};

    // Create a record batch with the new schema including 'email'
    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    let id_array = Int64Array::from(vec![100]); // Use a different ID to avoid conflicts
    let name_array = StringArray::from(vec!["schema_evolution_test"]);
    let email_array = StringArray::from(vec!["test@example.com"]);

    let record_batch = RecordBatch::try_new(
        Arc::clone(&evolved_schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(email_array),
        ],
    )
    .expect("Failed to create record batch");

    // Write with schema merge enabled using DeltaTable methods directly
    let _result = table
        .clone()
        .write(vec![record_batch])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
        .await
        .expect("Failed to write with schema evolution");

    // Poll until schema evolution commit is visible (version increased and schema has 'email' field)
    let _table = poll_table_until(
        bucket,
        &table_path,
        |t| {
            if t.version().unwrap_or(0) <= version_after_first_write {
                return false;
            }
            let schema = t.snapshot().unwrap().schema();
            schema.fields().any(|f| f.name() == "email")
        },
        "Timeout waiting for schema evolution to commit",
    )
    .await;

    // Build second sink - it should automatically detect and handle the evolved schema
    let config2 = DeltaLakeConfig {
        table_uri: table_uri.clone(),
        storage_options: minio_storage_options(),
        allow_nullable_fields: true,
        schema_evolution: true,
        batch: Default::default(),
        request: Default::default(),
        acknowledgements: Default::default(),
    };

    let cx2 = SinkContext::default();
    let (sink2, _) = config2.build(cx2).await.expect("Failed to build sink2");

    // Write events with the original schema (without email)
    // The sink should handle the schema mismatch by reloading
    let (batch2, receiver2) = BatchNotifier::new_with_receiver();
    let events2: Vec<Event> = (5..10)
        .map(|i| {
            let mut log = LogEvent::default();
            log.insert("id", i as i64);
            log.insert("name", format!("user_{}", i));
            // Intentionally NOT including 'email' to test schema evolution handling
            Event::Log(log)
        })
        .collect();

    let events2_with_batch = events2
        .into_iter()
        .map(|e| e.with_batch_notifier(&batch2))
        .collect::<Vec<_>>();
    drop(batch2);

    run_and_assert_sink_compliance(sink2, stream::iter(events2_with_batch), &[]).await;
    assert_eq!(receiver2.await, BatchStatus::Delivered);

    // Verify schema evolved and data was written
    let mut table_final = open_delta_table(bucket, &table_path).await;
    table_final
        .load()
        .await
        .expect("Failed to load final table");

    let final_schema = table_final.snapshot().unwrap().schema();
    let field_names: Vec<&str> = final_schema.fields().map(|f| f.name().as_str()).collect();

    // The schema should include the email field from the manual evolution
    println!("Final schema fields: {:?}", field_names);
    assert!(
        field_names.contains(&"email"),
        "Schema should include 'email' field"
    );
    assert!(
        table_final.version().unwrap() >= 2,
        "Table should have at least 2 versions"
    );

    // Read-path validation: verify all data including schema evolution
    // Initial write: 5 rows (id 0-4), schema evolution write: 1 row (id 100), second write: 5 rows (id 5-9)
    let total_rows = assert_data_readable(&table_final, 11).await;
    println!("Read back {} rows after schema evolution", total_rows);

    // Verify data from first write exists
    assert_column_value_exists(&table_final, "name", "user_0").await;
    assert_column_value_exists(&table_final, "name", "user_4").await;

    // Verify data from schema evolution write exists (with email)
    assert_column_value_exists(&table_final, "email", "test@example.com").await;

    // Verify data from second write exists
    assert_column_value_exists(&table_final, "name", "user_9").await;

    // Read all IDs and verify expected range
    let ids = read_column_values(&table_final, "id").await;
    assert_eq!(ids.len(), 11, "Expected 11 total rows");
    println!("All IDs in table: {:?}", ids);
}

#[tokio::test]
async fn test_delta_lake_concurrent_writes() {
    use crate::config::SinkContext;
    use crate::sinks::delta_lake::DeltaLakeConfig;
    use crate::test_util::components::run_and_assert_sink_compliance;
    use futures::stream;
    use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    // Setup - create unique table for this test
    let bucket = "test-bucket";
    let table_path = format!("test-concurrent-{}", uuid::Uuid::new_v4());

    // Create Delta table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("worker_id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, true),
    ]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    // Spawn multiple concurrent writers
    let num_workers = 3;
    let events_per_worker = 10;

    let mut handles = vec![];

    for worker_id in 0..num_workers {
        let table_uri = format!("s3://{}/{}", bucket, table_path);
        let storage_opts = minio_storage_options();

        let handle = tokio::spawn(async move {
            // Each worker builds its own sink
            let config = DeltaLakeConfig {
                table_uri,
                storage_options: storage_opts,
                allow_nullable_fields: true,
                schema_evolution: true,
                batch: Default::default(),
                request: Default::default(),
                acknowledgements: Default::default(),
            };

            let cx = SinkContext::default();
            let (sink, _) = config.build(cx).await.expect("Failed to build sink");

            // Create events for this worker
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            let events: Vec<Event> = (0..events_per_worker)
                .map(|i| {
                    let mut log = LogEvent::default();
                    log.insert("id", (worker_id * events_per_worker + i) as i64);
                    log.insert("worker_id", worker_id as i32);
                    log.insert("data", format!("worker_{}_event_{}", worker_id, i));
                    Event::Log(log)
                })
                .collect();

            let events_with_batch = events
                .into_iter()
                .map(|e| e.with_batch_notifier(&batch))
                .collect::<Vec<_>>();
            drop(batch);

            // Write concurrently
            run_and_assert_sink_compliance(sink, stream::iter(events_with_batch), &[]).await;

            receiver.await
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        let status = handle.await.expect("Worker task failed");
        assert_eq!(status, BatchStatus::Delivered);
    }

    // Verify all data was written
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    // Verify table has multiple versions (concurrent writes succeeded)
    let version = table.version().unwrap();
    assert!(
        version > 0,
        "Table should have commits from concurrent writes"
    );

    // Read-path validation: verify all concurrent writes landed
    let expected_total = num_workers * events_per_worker; // 3 workers * 10 events = 30 rows
    let total_rows = assert_data_readable(&table, expected_total).await;
    println!("Read back {} rows from concurrent writes", total_rows);

    // Verify data from each worker exists
    for worker_id in 0..num_workers {
        let expected_data = format!("worker_{}_event_0", worker_id);
        assert_column_value_exists(&table, "data", &expected_data).await;
    }

    // Read all worker_ids and verify distribution
    let worker_ids = read_column_values(&table, "worker_id").await;
    assert_eq!(
        worker_ids.len(),
        expected_total,
        "Expected {} total rows",
        expected_total
    );

    // Count events per worker
    for worker_id in 0..num_workers {
        let worker_count = worker_ids
            .iter()
            .filter(|&w| w == &worker_id.to_string())
            .count();
        assert_eq!(
            worker_count, events_per_worker,
            "Worker {} should have {} events, found {}",
            worker_id, events_per_worker, worker_count
        );
    }

    println!(
        "Concurrent writes completed successfully. Table version: {}. Total rows: {}",
        version, total_rows
    );
}

#[tokio::test]
async fn test_delta_lake_large_batch() {
    use crate::config::SinkContext;
    use crate::sinks::delta_lake::DeltaLakeConfig;
    use crate::sinks::util::BatchConfig;
    use crate::test_util::components::run_and_assert_sink_compliance;
    use futures::stream;
    use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    // Setup - create unique table for this test
    let bucket = "test-bucket";
    let table_path = format!("test-large-batch-{}", uuid::Uuid::new_v4());

    // Create Delta table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
    ]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    // Build sink with larger batch size
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let mut batch_config = BatchConfig::default();
    batch_config.max_events = Some(500); // Batch every 500 events

    let config = DeltaLakeConfig {
        table_uri,
        storage_options: minio_storage_options(),
        allow_nullable_fields: true,
        schema_evolution: true,
        batch: batch_config,
        request: Default::default(),
        acknowledgements: Default::default(),
    };

    let cx = SinkContext::default();
    let (sink, _) = config.build(cx).await.expect("Failed to build sink");

    // Create 1000+ events with large payloads
    let num_events = 1500;
    let large_payload = "x".repeat(1024); // 1KB payload per event

    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<Event> = (0..num_events)
        .map(|i| {
            let mut log = LogEvent::default();
            log.insert("id", i as i64);
            log.insert("message", format!("Large event number {}", i));
            log.insert("payload", large_payload.clone());
            Event::Log(log)
        })
        .collect();

    let events_with_batch = events
        .into_iter()
        .map(|e| e.with_batch_notifier(&batch))
        .collect::<Vec<_>>();
    drop(batch);

    // Write large batch
    run_and_assert_sink_compliance(sink, stream::iter(events_with_batch), &[]).await;

    // Verify delivery
    assert_eq!(receiver.await, BatchStatus::Delivered);

    // Verify data was written
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    // Verify table has data
    assert!(
        table.version().unwrap() > 0,
        "Table should have data written"
    );

    let files: Vec<_> = table
        .get_file_uris()
        .expect("Failed to get file URIs")
        .collect();
    assert!(!files.is_empty(), "No files written to Delta table");

    // Read-path validation: verify all events in large batch are readable
    let total_rows = assert_data_readable(&table, num_events).await;
    println!("Read back {} rows from large batch", total_rows);

    // Verify specific events exist (sample a few from different positions)
    assert_column_value_exists(&table, "message", "Large event number 0").await;
    assert_column_value_exists(&table, "message", "Large event number 500").await;
    assert_column_value_exists(&table, "message", "Large event number 1499").await;

    // Verify IDs span the expected range
    let ids = read_column_values(&table, "id").await;
    assert_eq!(ids.len(), num_events, "Expected {} events", num_events);

    // Verify first and last IDs
    assert!(ids.contains(&"0".to_string()), "Missing ID 0");
    assert!(ids.contains(&"1499".to_string()), "Missing ID 1499");

    println!(
        "Successfully wrote and verified {} events in large batch. Files: {}",
        num_events,
        files.len()
    );
}

#[tokio::test]
async fn test_delta_lake_schema_inference() {
    use crate::config::SinkContext;
    use crate::sinks::delta_lake::DeltaLakeConfig;
    use crate::test_util::components::run_and_assert_sink_compliance;
    use futures::stream;
    use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    // Setup - create unique table for this test
    let bucket = "test-bucket";
    let table_path = format!("test-schema-inference-{}", uuid::Uuid::new_v4());

    // Create Delta table with minimal schema (only 'id')
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    create_delta_table(bucket, &table_path, schema.clone()).await;

    // Build sink with auto schema evolution enabled
    let table_uri = format!("s3://{}/{}", bucket, table_path);
    let config = DeltaLakeConfig {
        table_uri,
        storage_options: minio_storage_options(),
        allow_nullable_fields: true,
        schema_evolution: true, // Enable schema inference and evolution
        batch: Default::default(),
        request: Default::default(),
        acknowledgements: Default::default(),
    };

    // Build the sink
    let cx = SinkContext::default();
    let (sink, _healthcheck) = config.build(cx).await.expect("Failed to build sink");

    // Create test events with extra fields NOT in the table schema
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<Event> = (0..10)
        .map(|i| {
            let mut log = LogEvent::default();
            log.insert("id", i as i64);
            log.insert("message", format!("test message {}", i)); // Not in schema
            log.insert("count", (i * 10) as i64); // Not in schema
            log.insert("active", i % 2 == 0); // Not in schema (boolean)
            Event::Log(log)
        })
        .collect();

    let events_with_batch = events
        .into_iter()
        .map(|e| e.with_batch_notifier(&batch))
        .collect::<Vec<_>>();

    drop(batch);

    // Write events through the sink
    run_and_assert_sink_compliance(sink, stream::iter(events_with_batch), &[]).await;

    // Verify delivery
    assert_eq!(receiver.await, BatchStatus::Delivered);

    // Verify schema evolved to include inferred fields
    let mut table = open_delta_table(bucket, &table_path).await;
    table.load().await.expect("Failed to load table");

    let table_schema = table.snapshot().unwrap().schema();
    let field_names: Vec<&str> = table_schema.fields().map(|f| f.name().as_str()).collect();

    println!("Schema after inference: {:?}", field_names);

    // Original field should exist
    assert!(field_names.contains(&"id"), "Schema should contain 'id'");

    // Inferred fields should be added
    assert!(
        field_names.contains(&"message"),
        "Schema should contain inferred 'message' field"
    );
    assert!(
        field_names.contains(&"count"),
        "Schema should contain inferred 'count' field"
    );
    assert!(
        field_names.contains(&"active"),
        "Schema should contain inferred 'active' field"
    );

    // Verify data was written correctly
    let total_rows = assert_data_readable(&table, 10).await;
    println!("Read back {} rows with inferred schema", total_rows);

    // Verify specific values exist
    assert_column_value_exists(&table, "message", "test message 5").await;

    // Read count values and verify they are integers
    let counts = read_column_values(&table, "count").await;
    assert_eq!(counts.len(), 10);
    assert!(counts.contains(&"0".to_string()));
    assert!(counts.contains(&"90".to_string()));

    // Verify booleans were inferred correctly
    let active_values = read_column_values(&table, "active").await;
    assert_eq!(active_values.len(), 10);
    assert!(
        active_values.contains(&"true".to_string()) || active_values.contains(&"TRUE".to_string())
    );
    assert!(
        active_values.contains(&"false".to_string())
            || active_values.contains(&"FALSE".to_string())
    );

    println!("Schema inference test passed. Inferred fields: message, count, active");
}
