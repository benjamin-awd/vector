//! Convert Arrow RecordBatches to Vector LogEvents.

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use deltalake::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::delta_datafusion::cdf::{
    CHANGE_TYPE_COL, COMMIT_TIMESTAMP_COL, COMMIT_VERSION_COL,
};
use vector_lib::config::{LegacyKey, LogNamespace};
use vector_lib::event::{Event, LogEvent};
use vector_lib::lookup::path;
use vrl::value::Value;

use super::config::{ChangeType, DeltaLakeCdfConfig};

/// Convert a batch of CDF RecordBatches to Vector Events.
pub fn convert_cdf_batches_to_events(
    batches: Vec<RecordBatch>,
    config: &DeltaLakeCdfConfig,
    log_namespace: LogNamespace,
) -> Result<Vec<Event>, String> {
    let mut events = Vec::new();

    for batch in batches {
        convert_batch_to_events(&batch, config, log_namespace, &mut events)?;
    }

    Ok(events)
}

/// Convert a single RecordBatch to Vector Events.
fn convert_batch_to_events(
    batch: &RecordBatch,
    config: &DeltaLakeCdfConfig,
    log_namespace: LogNamespace,
    events: &mut Vec<Event>,
) -> Result<(), String> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    if num_rows == 0 {
        return Ok(());
    }

    // Find CDF metadata column indices
    let change_type_idx = schema
        .index_of(CHANGE_TYPE_COL)
        .map_err(|_| format!("Missing {} column in CDF data", CHANGE_TYPE_COL))?;
    let commit_version_idx = schema
        .index_of(COMMIT_VERSION_COL)
        .map_err(|_| format!("Missing {} column in CDF data", COMMIT_VERSION_COL))?;
    let commit_timestamp_idx = schema
        .index_of(COMMIT_TIMESTAMP_COL)
        .map_err(|_| format!("Missing {} column in CDF data", COMMIT_TIMESTAMP_COL))?;

    // Get CDF metadata columns
    let change_type_col = batch.column(change_type_idx);
    let commit_version_col = batch.column(commit_version_idx);
    let commit_timestamp_col = batch.column(commit_timestamp_idx);

    for row_idx in 0..num_rows {
        // Extract change type
        let change_type_str = extract_string(change_type_col, row_idx)
            .ok_or_else(|| format!("Null change_type at row {}", row_idx))?;

        // Filter by change type if specified
        if !config.change_types.is_empty() {
            if let Some(ct) = ChangeType::from_cdf_string(&change_type_str) {
                if !config.change_types.contains(&ct) {
                    continue;
                }
            }
        }

        let mut log = LogEvent::default();

        // Insert CDF metadata columns
        insert_cdf_metadata(
            &mut log,
            log_namespace,
            &change_type_str,
            commit_version_col,
            commit_timestamp_col,
            row_idx,
        )?;

        // Include data columns if configured
        if config.include_data {
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let field_name = field.name();

                // Skip CDF metadata columns (already handled above)
                if field_name == CHANGE_TYPE_COL
                    || field_name == COMMIT_VERSION_COL
                    || field_name == COMMIT_TIMESTAMP_COL
                {
                    continue;
                }

                let column = batch.column(col_idx);
                let value = arrow_value_to_vrl(column, row_idx, field.data_type())?;

                log.insert(field_name.as_str(), value);
            }
        }

        // Add standard Vector source metadata
        log_namespace.insert_standard_vector_source_metadata(
            &mut log,
            DeltaLakeCdfConfig::NAME,
            Utc::now(),
        );

        events.push(Event::Log(log));
    }

    Ok(())
}

/// Insert CDF metadata columns into the log event.
fn insert_cdf_metadata(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    change_type: &str,
    commit_version_col: &ArrayRef,
    commit_timestamp_col: &ArrayRef,
    row_idx: usize,
) -> Result<(), String> {
    // Change type
    log_namespace.insert_source_metadata(
        DeltaLakeCdfConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("_change_type"))),
        path!("change_type"),
        change_type.to_string(),
    );

    // Commit version
    let commit_version = extract_int64(commit_version_col, row_idx)
        .ok_or_else(|| format!("Null commit_version at row {}", row_idx))?;

    log_namespace.insert_source_metadata(
        DeltaLakeCdfConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("_commit_version"))),
        path!("commit_version"),
        commit_version,
    );

    // Commit timestamp
    let commit_timestamp = extract_timestamp(commit_timestamp_col, row_idx)
        .ok_or_else(|| format!("Null commit_timestamp at row {}", row_idx))?;

    log_namespace.insert_source_metadata(
        DeltaLakeCdfConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("_commit_timestamp"))),
        path!("commit_timestamp"),
        commit_timestamp,
    );

    Ok(())
}

/// Convert an Arrow array value to a VRL Value.
fn arrow_value_to_vrl(
    column: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
) -> Result<Value, String> {
    if column.is_null(row_idx) {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Failed to downcast to BooleanArray")?;
            Ok(Value::Boolean(arr.value(row_idx)))
        }

        // Signed integers
        DataType::Int8 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("Failed to downcast to Int8Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::Int16 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("Failed to downcast to Int16Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::Int32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("Failed to downcast to Int32Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Failed to downcast to Int64Array")?;
            Ok(Value::Integer(arr.value(row_idx)))
        }

        // Unsigned integers
        DataType::UInt8 => {
            let arr = column
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or("Failed to downcast to UInt8Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::UInt16 => {
            let arr = column
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or("Failed to downcast to UInt16Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::UInt32 => {
            let arr = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or("Failed to downcast to UInt32Array")?;
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }
        DataType::UInt64 => {
            let arr = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Failed to downcast to UInt64Array")?;
            // Note: Large u64 values may overflow i64
            Ok(Value::Integer(arr.value(row_idx) as i64))
        }

        // Floating point
        DataType::Float32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("Failed to downcast to Float32Array")?;
            Ok(Value::Float(
                ordered_float::NotNan::new(arr.value(row_idx) as f64)
                    .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
            ))
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Failed to downcast to Float64Array")?;
            Ok(Value::Float(
                ordered_float::NotNan::new(arr.value(row_idx))
                    .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
            ))
        }

        // Strings
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Failed to downcast to StringArray")?;
            Ok(Value::Bytes(Bytes::from(arr.value(row_idx).to_string())))
        }
        DataType::LargeUtf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or("Failed to downcast to LargeStringArray")?;
            Ok(Value::Bytes(Bytes::from(arr.value(row_idx).to_string())))
        }

        // Binary
        DataType::Binary => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("Failed to downcast to BinaryArray")?;
            Ok(Value::Bytes(Bytes::from(arr.value(row_idx).to_vec())))
        }
        DataType::LargeBinary => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or("Failed to downcast to LargeBinaryArray")?;
            Ok(Value::Bytes(Bytes::from(arr.value(row_idx).to_vec())))
        }

        // Timestamps
        DataType::Timestamp(unit, _tz) => {
            let ts = extract_timestamp_by_unit(column, row_idx, unit)
                .ok_or("Failed to extract timestamp")?;
            Ok(Value::Timestamp(ts))
        }

        // Dates
        DataType::Date32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("Failed to downcast to Date32Array")?;
            let days = arr.value(row_idx);
            let ts = Utc
                .timestamp_opt(days as i64 * 86400, 0)
                .single()
                .ok_or("Invalid date32 value")?;
            Ok(Value::Timestamp(ts))
        }
        DataType::Date64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or("Failed to downcast to Date64Array")?;
            let millis = arr.value(row_idx);
            let ts = Utc
                .timestamp_millis_opt(millis)
                .single()
                .ok_or("Invalid date64 value")?;
            Ok(Value::Timestamp(ts))
        }

        // Complex types - serialize to JSON string
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
            // Serialize complex types to JSON for maximum compatibility
            let json_str = format_complex_value(column, row_idx)?;
            Ok(Value::Bytes(Bytes::from(json_str)))
        }

        // Fallback for other types
        _ => {
            // Try to get a string representation
            let display = deltalake::arrow::util::display::array_value_to_string(column, row_idx)
                .map_err(|e| format!("Failed to convert value to string: {}", e))?;
            Ok(Value::Bytes(Bytes::from(display)))
        }
    }
}

/// Extract a string value from an Arrow array.
fn extract_string(column: &ArrayRef, row_idx: usize) -> Option<String> {
    if column.is_null(row_idx) {
        return None;
    }

    match column.data_type() {
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>()?;
            Some(arr.value(row_idx).to_string())
        }
        _ => None,
    }
}

/// Extract an i64 value from an Arrow array.
fn extract_int64(column: &ArrayRef, row_idx: usize) -> Option<i64> {
    if column.is_null(row_idx) {
        return None;
    }

    match column.data_type() {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>()?;
            Some(arr.value(row_idx))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>()?;
            Some(arr.value(row_idx) as i64)
        }
        _ => None,
    }
}

/// Extract a timestamp from various Arrow timestamp types.
fn extract_timestamp(column: &ArrayRef, row_idx: usize) -> Option<DateTime<Utc>> {
    if column.is_null(row_idx) {
        return None;
    }

    match column.data_type() {
        DataType::Timestamp(unit, _) => extract_timestamp_by_unit(column, row_idx, unit),
        DataType::Int64 => {
            // Assume milliseconds if stored as plain Int64
            let arr = column.as_any().downcast_ref::<Int64Array>()?;
            Utc.timestamp_millis_opt(arr.value(row_idx)).single()
        }
        _ => None,
    }
}

/// Extract timestamp by time unit.
fn extract_timestamp_by_unit(
    column: &ArrayRef,
    row_idx: usize,
    unit: &deltalake::arrow::datatypes::TimeUnit,
) -> Option<DateTime<Utc>> {
    use deltalake::arrow::datatypes::TimeUnit;

    match unit {
        TimeUnit::Second => {
            let arr = column.as_any().downcast_ref::<TimestampSecondArray>()?;
            Utc.timestamp_opt(arr.value(row_idx), 0).single()
        }
        TimeUnit::Millisecond => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()?;
            Utc.timestamp_millis_opt(arr.value(row_idx)).single()
        }
        TimeUnit::Microsecond => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?;
            let micros = arr.value(row_idx);
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1000) as u32;
            Utc.timestamp_opt(secs, nanos).single()
        }
        TimeUnit::Nanosecond => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()?;
            let nanos = arr.value(row_idx);
            let secs = nanos / 1_000_000_000;
            let subsec_nanos = (nanos % 1_000_000_000) as u32;
            Utc.timestamp_opt(secs, subsec_nanos).single()
        }
    }
}

/// Format complex Arrow values (struct, list, map) as JSON strings.
fn format_complex_value(column: &ArrayRef, row_idx: usize) -> Result<String, String> {
    // Use Arrow's display utility for complex types
    // This provides a string representation that can be parsed as JSON-like format
    deltalake::arrow::util::display::array_value_to_string(column, row_idx)
        .map_err(|e| format!("Failed to format complex value: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int64Builder, StringBuilder};
    use deltalake::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_change_type", DataType::Utf8, false),
            Field::new("_commit_version", DataType::Int64, false),
            Field::new(
                "_commit_timestamp",
                DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut change_type_builder = StringBuilder::new();
        let mut commit_version_builder = Int64Builder::new();
        let mut commit_timestamp_builder =
            deltalake::arrow::array::TimestampMillisecondBuilder::new();
        let mut id_builder = Int64Builder::new();
        let mut name_builder = StringBuilder::new();

        // Add test data
        change_type_builder.append_value("insert");
        commit_version_builder.append_value(1);
        commit_timestamp_builder.append_value(1704067200000); // 2024-01-01 00:00:00 UTC
        id_builder.append_value(100);
        name_builder.append_value("Alice");

        change_type_builder.append_value("delete");
        commit_version_builder.append_value(2);
        commit_timestamp_builder.append_value(1704153600000); // 2024-01-02 00:00:00 UTC
        id_builder.append_value(100);
        name_builder.append_value("Alice");

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(change_type_builder.finish()),
                Arc::new(commit_version_builder.finish()),
                Arc::new(commit_timestamp_builder.finish()),
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_convert_batch_basic() {
        let batch = create_test_batch();
        let config = DeltaLakeCdfConfig {
            include_data: true,
            ..Default::default()
        };

        let events =
            convert_cdf_batches_to_events(vec![batch], &config, LogNamespace::Legacy).unwrap();

        assert_eq!(events.len(), 2);

        // Check first event (insert)
        let log1 = events[0].as_log();
        assert_eq!(
            log1.get("_change_type").unwrap().to_string_lossy(),
            "insert"
        );
        assert_eq!(log1.get("id").unwrap(), &Value::Integer(100));
        assert_eq!(log1.get("name").unwrap().to_string_lossy(), "Alice");

        // Check second event (delete)
        let log2 = events[1].as_log();
        assert_eq!(
            log2.get("_change_type").unwrap().to_string_lossy(),
            "delete"
        );
    }

    #[test]
    fn test_convert_batch_filter_change_types() {
        let batch = create_test_batch();
        let config = DeltaLakeCdfConfig {
            include_data: true,
            change_types: vec![ChangeType::Insert],
            ..Default::default()
        };

        let events =
            convert_cdf_batches_to_events(vec![batch], &config, LogNamespace::Legacy).unwrap();

        // Should only have insert events
        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        assert_eq!(log.get("_change_type").unwrap().to_string_lossy(), "insert");
    }

    #[test]
    fn test_convert_batch_exclude_data() {
        let batch = create_test_batch();
        let config = DeltaLakeCdfConfig {
            include_data: false,
            ..Default::default()
        };

        let events =
            convert_cdf_batches_to_events(vec![batch], &config, LogNamespace::Legacy).unwrap();

        assert_eq!(events.len(), 2);

        // Should only have CDF metadata, not data columns
        let log = events[0].as_log();
        assert!(log.get("_change_type").is_some());
        assert!(log.get("_commit_version").is_some());
        assert!(log.get("_commit_timestamp").is_some());
        assert!(log.get("id").is_none());
        assert!(log.get("name").is_none());
    }
}
