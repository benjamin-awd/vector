use std::io::Cursor;

use arrow::array::cast::AsArray;
use arrow::array::*;
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType as ArrowDataType, Date32Type, Date64Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use lookup::event_path;
use smallvec::{SmallVec, smallvec};
use vector_core::{
    config::{DataType, LogNamespace, log_schema},
    event::{Event, LogEvent},
    schema,
};
use vrl::value::KeyString;

use serde::{Deserialize, Serialize};

use super::Deserializer;

type VrlValue = vrl::value::Value;

/// Config used to build an `ArrowStreamDeserializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ArrowStreamDeserializerConfig;

impl ArrowStreamDeserializerConfig {
    /// Build the `ArrowStreamDeserializer` from this configuration.
    pub fn build(&self) -> vector_common::Result<ArrowStreamDeserializer> {
        Ok(ArrowStreamDeserializer)
    }

    /// The data type of events that are accepted by `ArrowStreamDeserializer`.
    pub fn output_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self, log_namespace: LogNamespace) -> schema::Definition {
        match log_namespace {
            LogNamespace::Legacy => {
                let mut definition = schema::Definition::empty_legacy_namespace()
                    .unknown_fields(vrl::value::Kind::any());

                if let Some(timestamp_key) = log_schema().timestamp_key() {
                    definition = definition.try_with_field(
                        timestamp_key,
                        vrl::value::Kind::any().or_timestamp(),
                        Some("timestamp"),
                    );
                }
                definition
            }
            LogNamespace::Vector => schema::Definition::new_with_default_metadata(
                vrl::value::Kind::any(),
                [log_namespace],
            ),
        }
    }
}

/// Deserializer that converts Arrow IPC stream bytes to `Event`s.
#[derive(Debug, Clone)]
pub struct ArrowStreamDeserializer;

impl Deserializer for ArrowStreamDeserializer {
    fn parse(
        &self,
        bytes: Bytes,
        log_namespace: LogNamespace,
    ) -> vector_common::Result<SmallVec<[Event; 1]>> {
        if bytes.is_empty() {
            return Ok(smallvec![]);
        }

        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)?;

        let mut events = SmallVec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let schema = batch.schema();
            let num_rows = batch.num_rows();

            // NOTE: Row-by-row conversion is straightforward but doesn't leverage Arrow's
            // columnar layout. A future optimization could process entire columns at once
            // and build events from pre-extracted vectors.
            for row in 0..num_rows {
                let mut log = LogEvent::default();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let value = arrow_value_to_vrl(column, row)?;
                    log.insert(event_path!(field.name().as_str()), value);
                }

                let mut event = Event::Log(log);
                if log_namespace == LogNamespace::Legacy
                    && let Some(timestamp_key) = log_schema().timestamp_key_target_path()
                {
                    let log = event.as_mut_log();
                    if !log.contains(timestamp_key) {
                        log.insert(timestamp_key, Utc::now());
                    }
                }
                events.push(event);
            }
        }

        Ok(events)
    }
}

/// Convert an Arrow array value at a given row index to a VRL Value.
fn arrow_value_to_vrl(array: &dyn Array, row: usize) -> vector_common::Result<VrlValue> {
    if array.is_null(row) {
        return Ok(VrlValue::Null);
    }

    let data_type = array.data_type();
    match data_type {
        ArrowDataType::Null => Ok(VrlValue::Null),

        ArrowDataType::Boolean => Ok(VrlValue::Boolean(array.as_boolean().value(row))),

        ArrowDataType::Int8 => Ok(VrlValue::Integer(
            array.as_primitive::<Int8Type>().value(row) as i64,
        )),
        ArrowDataType::Int16 => Ok(VrlValue::Integer(
            array.as_primitive::<Int16Type>().value(row) as i64,
        )),
        ArrowDataType::Int32 => Ok(VrlValue::Integer(
            array.as_primitive::<Int32Type>().value(row) as i64,
        )),
        ArrowDataType::Int64 => Ok(VrlValue::Integer(
            array.as_primitive::<Int64Type>().value(row),
        )),

        ArrowDataType::UInt8 => Ok(VrlValue::Integer(
            array.as_primitive::<UInt8Type>().value(row) as i64,
        )),
        ArrowDataType::UInt16 => Ok(VrlValue::Integer(
            array.as_primitive::<UInt16Type>().value(row) as i64,
        )),
        ArrowDataType::UInt32 => Ok(VrlValue::Integer(
            array.as_primitive::<UInt32Type>().value(row) as i64,
        )),
        // VRL's `From<u64>` does a wrapping `as i64` cast, which is consistent
        // with how VRL handles u64 values everywhere (e.g. JSON deserialization).
        ArrowDataType::UInt64 => Ok(VrlValue::from(
            array.as_primitive::<UInt64Type>().value(row),
        )),

        ArrowDataType::Float16 => Ok(VrlValue::from_f64_or_zero(
            array.as_primitive::<Float16Type>().value(row).to_f64(),
        )),
        ArrowDataType::Float32 => Ok(VrlValue::from_f64_or_zero(
            array.as_primitive::<Float32Type>().value(row) as f64,
        )),
        ArrowDataType::Float64 => Ok(VrlValue::from_f64_or_zero(
            array.as_primitive::<Float64Type>().value(row),
        )),

        ArrowDataType::Utf8 => Ok(VrlValue::from(array.as_string::<i32>().value(row))),
        ArrowDataType::LargeUtf8 => Ok(VrlValue::from(array.as_string::<i64>().value(row))),
        ArrowDataType::Utf8View => Ok(VrlValue::from(array.as_string_view().value(row))),

        ArrowDataType::Binary => Ok(VrlValue::from(Bytes::copy_from_slice(
            array.as_binary::<i32>().value(row),
        ))),
        ArrowDataType::LargeBinary => Ok(VrlValue::from(Bytes::copy_from_slice(
            array.as_binary::<i64>().value(row),
        ))),
        ArrowDataType::BinaryView => Ok(VrlValue::from(Bytes::copy_from_slice(
            array.as_binary_view().value(row),
        ))),

        ArrowDataType::Timestamp(unit, tz) => timestamp_to_vrl(array, row, unit, tz.as_deref()),

        ArrowDataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row) as i64;
            let dt = Utc
                .timestamp_opt(days * 86400, 0)
                .single()
                .ok_or("Invalid Date32 value")?;
            Ok(VrlValue::Timestamp(dt))
        }
        ArrowDataType::Date64 => {
            let millis = array.as_primitive::<Date64Type>().value(row);
            let dt = DateTime::from_timestamp_millis(millis).ok_or("Invalid Date64 value")?;
            Ok(VrlValue::Timestamp(dt))
        }

        // NOTE: Duration/Time32/Time64 values are converted to raw integers, losing the
        // unit information (seconds vs milliseconds vs microseconds vs nanoseconds).
        // The unit is available from the Arrow schema but not preserved in the VRL value.
        ArrowDataType::Time32(TimeUnit::Second) => Ok(VrlValue::Integer(
            array.as_primitive::<Time32SecondType>().value(row) as i64,
        )),
        ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(VrlValue::Integer(
            array.as_primitive::<Time32MillisecondType>().value(row) as i64,
        )),
        ArrowDataType::Time32(_) => {
            unreachable!("Arrow spec only allows Second/Millisecond for Time32")
        }
        ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(VrlValue::Integer(
            array.as_primitive::<Time64MicrosecondType>().value(row),
        )),
        ArrowDataType::Time64(TimeUnit::Nanosecond) => Ok(VrlValue::Integer(
            array.as_primitive::<Time64NanosecondType>().value(row),
        )),
        ArrowDataType::Time64(_) => {
            unreachable!("Arrow spec only allows Microsecond/Nanosecond for Time64")
        }

        ArrowDataType::Duration(TimeUnit::Second) => Ok(VrlValue::Integer(
            array.as_primitive::<DurationSecondType>().value(row),
        )),
        ArrowDataType::Duration(TimeUnit::Millisecond) => Ok(VrlValue::Integer(
            array.as_primitive::<DurationMillisecondType>().value(row),
        )),
        ArrowDataType::Duration(TimeUnit::Microsecond) => Ok(VrlValue::Integer(
            array.as_primitive::<DurationMicrosecondType>().value(row),
        )),
        ArrowDataType::Duration(TimeUnit::Nanosecond) => Ok(VrlValue::Integer(
            array.as_primitive::<DurationNanosecondType>().value(row),
        )),

        ArrowDataType::List(_) => list_to_vrl(array.as_list::<i32>().value(row).as_ref()),
        ArrowDataType::LargeList(_) => list_to_vrl(array.as_list::<i64>().value(row).as_ref()),
        ArrowDataType::FixedSizeList(_, _) => {
            list_to_vrl(array.as_fixed_size_list().value(row).as_ref())
        }

        ArrowDataType::Struct(fields) => {
            let arr = array.as_struct();
            let mut map = vrl::value::ObjectMap::new();
            for (i, field) in fields.iter().enumerate() {
                let col = arr.column(i);
                let val = arrow_value_to_vrl(col.as_ref(), row)?;
                map.insert(KeyString::from(field.name().as_str()), val);
            }
            Ok(VrlValue::Object(map))
        }

        ArrowDataType::Map(_, _) => {
            let arr = array.as_map();
            let entry = arr.value(row);
            let keys = entry.column(0);
            let values = entry.column(1);
            let mut map = vrl::value::ObjectMap::new();
            for i in 0..entry.len() {
                let key = arrow_value_to_vrl(keys.as_ref(), i)?;
                let key_str = match key {
                    VrlValue::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
                    other => other.to_string(),
                };
                let val = arrow_value_to_vrl(values.as_ref(), i)?;
                map.insert(KeyString::from(key_str), val);
            }
            Ok(VrlValue::Object(map))
        }

        ArrowDataType::Dictionary(key_type, _) => dispatch_dictionary(array, row, key_type),

        // NOTE: VRL has no native decimal type, so Decimal128/Decimal256 cannot be
        // losslessly represented. Users should cast to Float64 or Utf8 before ingestion.
        dt @ (ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _)) => Err(format!(
            "unsupported Arrow type: {dt}; consider converting to Float64 or Utf8 upstream"
        )
        .into()),

        other => Err(format!("unsupported Arrow type: {other}").into()),
    }
}

/// Convert an Arrow list-like array to a VRL Array.
fn list_to_vrl(values: &dyn Array) -> vector_common::Result<VrlValue> {
    (0..values.len())
        .map(|i| arrow_value_to_vrl(values, i))
        .collect::<Result<Vec<_>, _>>()
        .map(VrlValue::Array)
}

/// Dispatch dictionary-encoded arrays by key type using `AsArray`.
fn dispatch_dictionary(
    array: &dyn Array,
    row: usize,
    key_type: &ArrowDataType,
) -> vector_common::Result<VrlValue> {
    fn resolve<K: ArrowDictionaryKeyType>(
        array: &dyn Array,
        row: usize,
    ) -> vector_common::Result<VrlValue>
    where
        K::Native: TryInto<usize>,
    {
        let dict = array.as_dictionary::<K>();
        let key = dict.keys().value(row);
        let idx: usize = key.try_into().map_err(|_| "Dictionary key out of range")?;
        arrow_value_to_vrl(dict.values().as_ref(), idx)
    }

    match key_type {
        ArrowDataType::Int8 => resolve::<Int8Type>(array, row),
        ArrowDataType::Int16 => resolve::<Int16Type>(array, row),
        ArrowDataType::Int32 => resolve::<Int32Type>(array, row),
        ArrowDataType::Int64 => resolve::<Int64Type>(array, row),
        other => Err(format!("Unsupported dictionary key type: {other:?}").into()),
    }
}

/// Convert an Arrow Timestamp array value to a VRL Timestamp.
///
/// NOTE: The timezone parameter is currently ignored. Arrow timestamps store
/// UTC epoch values regardless of timezone metadata, so the converted timestamp
/// is correct. However, the original timezone annotation is lost and not
/// preserved as event metadata.
fn timestamp_to_vrl(
    array: &dyn Array,
    row: usize,
    unit: &TimeUnit,
    _tz: Option<&str>,
) -> vector_common::Result<VrlValue> {
    let dt = match unit {
        TimeUnit::Second => {
            let val = array.as_primitive::<TimestampSecondType>().value(row);
            DateTime::from_timestamp(val, 0).ok_or("Invalid timestamp seconds")?
        }
        TimeUnit::Millisecond => {
            let val = array.as_primitive::<TimestampMillisecondType>().value(row);
            DateTime::from_timestamp_millis(val).ok_or("Invalid timestamp millis")?
        }
        TimeUnit::Microsecond => {
            let val = array.as_primitive::<TimestampMicrosecondType>().value(row);
            DateTime::from_timestamp_micros(val).ok_or("Invalid timestamp micros")?
        }
        TimeUnit::Nanosecond => {
            let nanos = array.as_primitive::<TimestampNanosecondType>().value(row);
            let secs = nanos.div_euclid(1_000_000_000);
            let nsec = nanos.rem_euclid(1_000_000_000) as u32;
            DateTime::from_timestamp(secs, nsec).ok_or("Invalid timestamp nanos")?
        }
    };
    Ok(VrlValue::Timestamp(dt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use bytes::BufMut;
    use std::sync::Arc;

    /// Helper: build an Arrow IPC stream from record batches
    fn batches_to_ipc_bytes(schema: &Schema, batches: &[RecordBatch]) -> Bytes {
        let schema_ref = Arc::new(schema.clone());
        let mut buf = bytes::BytesMut::new().writer();
        let mut writer = StreamWriter::try_new(&mut buf, &schema_ref).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        buf.into_inner().freeze()
    }

    #[test]
    fn test_empty_bytes() {
        let deser = ArrowStreamDeserializer;
        let result = deser.parse(Bytes::new(), LogNamespace::Vector).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_basic_types() {
        let schema = Schema::new(vec![
            Field::new("bool_col", ArrowDataType::Boolean, false),
            Field::new("int32_col", ArrowDataType::Int32, false),
            Field::new("int64_col", ArrowDataType::Int64, false),
            Field::new("float64_col", ArrowDataType::Float64, false),
            Field::new("string_col", ArrowDataType::Utf8, false),
            Field::new("binary_col", ArrowDataType::Binary, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![123456789i64])),
                Arc::new(Float64Array::from(vec![2.5_f64])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(BinaryArray::from(vec![b"bytes" as &[u8]])),
            ],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        assert_eq!(log.get("bool_col").unwrap(), &VrlValue::Boolean(true));
        assert_eq!(log.get("int32_col").unwrap(), &VrlValue::Integer(42));
        assert_eq!(log.get("int64_col").unwrap(), &VrlValue::Integer(123456789));
        assert_eq!(log.get("string_col").unwrap(), &VrlValue::from("hello"));
        assert_eq!(
            log.get("binary_col").unwrap(),
            &VrlValue::from(Bytes::from_static(b"bytes"))
        );
    }

    #[test]
    fn test_all_int_types() {
        let schema = Schema::new(vec![
            Field::new("i8", ArrowDataType::Int8, false),
            Field::new("i16", ArrowDataType::Int16, false),
            Field::new("u8", ArrowDataType::UInt8, false),
            Field::new("u16", ArrowDataType::UInt16, false),
            Field::new("u32", ArrowDataType::UInt32, false),
            Field::new("u64", ArrowDataType::UInt64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int8Array::from(vec![-128i8])),
                Arc::new(Int16Array::from(vec![-32000i16])),
                Arc::new(UInt8Array::from(vec![255u8])),
                Arc::new(UInt16Array::from(vec![65535u16])),
                Arc::new(UInt32Array::from(vec![4_000_000u32])),
                Arc::new(UInt64Array::from(vec![9_000_000_000u64])),
            ],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        assert_eq!(log.get("i8").unwrap(), &VrlValue::Integer(-128));
        assert_eq!(log.get("i16").unwrap(), &VrlValue::Integer(-32000));
        assert_eq!(log.get("u8").unwrap(), &VrlValue::Integer(255));
        assert_eq!(log.get("u16").unwrap(), &VrlValue::Integer(65535));
        assert_eq!(log.get("u32").unwrap(), &VrlValue::Integer(4_000_000));
        assert_eq!(log.get("u64").unwrap(), &VrlValue::Integer(9_000_000_000));
    }

    #[test]
    fn test_uint64_large_value() {
        let schema = Schema::new(vec![Field::new("big", ArrowDataType::UInt64, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(UInt64Array::from(vec![u64::MAX]))],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        // VRL's From<u64> wraps via `as i64`, consistent with VRL convention
        let val = events[0].as_log().get("big").unwrap();
        assert_eq!(val, &VrlValue::from(u64::MAX));
    }

    #[test]
    fn test_timestamp_types() {
        let schema = Schema::new(vec![
            Field::new(
                "ts_s",
                ArrowDataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new(
                "ts_ms",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_ns",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampSecondArray::from(vec![1_700_000_000i64])),
                Arc::new(TimestampMillisecondArray::from(vec![1_700_000_000_000i64])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_000i64,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000i64,
                ])),
            ],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();

        let expected = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for field in &["ts_s", "ts_ms", "ts_us", "ts_ns"] {
            assert_eq!(
                log.get(*field).unwrap(),
                &VrlValue::Timestamp(expected),
                "Mismatch for {field}"
            );
        }
    }

    #[test]
    fn test_date_types() {
        let schema = Schema::new(vec![
            Field::new("d32", ArrowDataType::Date32, false),
            Field::new("d64", ArrowDataType::Date64, false),
        ]);

        // Date32: days since epoch. 19723 = 2024-01-01
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Date32Array::from(vec![19723])),
                Arc::new(Date64Array::from(vec![1704067200000i64])), // 2024-01-01 00:00:00 UTC in ms
            ],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();

        let expected = Utc.timestamp_opt(19723 * 86400, 0).single().unwrap();
        assert_eq!(log.get("d32").unwrap(), &VrlValue::Timestamp(expected));

        let expected_d64 = DateTime::from_timestamp_millis(1704067200000).unwrap();
        assert_eq!(log.get("d64").unwrap(), &VrlValue::Timestamp(expected_d64));
    }

    #[test]
    fn test_null_values() {
        let schema = Schema::new(vec![
            Field::new("nullable_str", ArrowDataType::Utf8, true),
            Field::new("nullable_int", ArrowDataType::Int64, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(vec![None as Option<&str>, Some("value")])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
            ],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].as_log().get("nullable_str").unwrap(),
            &VrlValue::Null
        );
        assert_eq!(
            events[0].as_log().get("nullable_int").unwrap(),
            &VrlValue::Integer(42)
        );
        assert_eq!(
            events[1].as_log().get("nullable_str").unwrap(),
            &VrlValue::from("value")
        );
        assert_eq!(
            events[1].as_log().get("nullable_int").unwrap(),
            &VrlValue::Null
        );
    }

    #[test]
    fn test_multiple_batches() {
        let schema = Schema::new(vec![Field::new("val", ArrowDataType::Int64, false)]);
        let schema_ref = Arc::new(schema.clone());

        let batch1 = RecordBatch::try_new(
            schema_ref.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let batch2 =
            RecordBatch::try_new(schema_ref, vec![Arc::new(Int64Array::from(vec![3, 4]))]).unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch1, batch2]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 4);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(
                event.as_log().get("val").unwrap(),
                &VrlValue::Integer(i as i64 + 1)
            );
        }
    }

    #[test]
    fn test_nested_struct() {
        let inner_fields = vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("score", ArrowDataType::Int64, false),
        ];
        let schema = Schema::new(vec![Field::new(
            "record",
            ArrowDataType::Struct(inner_fields.clone().into()),
            false,
        )]);

        let name_array = StringArray::from(vec!["alice"]);
        let score_array = Int64Array::from(vec![100i64]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", ArrowDataType::Utf8, false)),
                Arc::new(name_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("score", ArrowDataType::Int64, false)),
                Arc::new(score_array) as Arc<dyn Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(struct_array)]).unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        let record = log.get("record").unwrap();
        match record {
            VrlValue::Object(map) => {
                assert_eq!(map.get("name").unwrap(), &VrlValue::from("alice"));
                assert_eq!(map.get("score").unwrap(), &VrlValue::Integer(100));
            }
            _ => panic!("Expected Object, got {record:?}"),
        }
    }

    #[test]
    fn test_list_array() {
        let schema = Schema::new(vec![Field::new(
            "nums",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, false))),
            false,
        )]);

        let values = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 3, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Int64, false)),
            offsets,
            Arc::new(values),
            None,
        );

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(list_array)]).unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 2);
        let nums0 = events[0].as_log().get("nums").unwrap();
        assert_eq!(
            nums0,
            &VrlValue::Array(vec![
                VrlValue::Integer(1),
                VrlValue::Integer(2),
                VrlValue::Integer(3)
            ])
        );
        let nums1 = events[1].as_log().get("nums").unwrap();
        assert_eq!(
            nums1,
            &VrlValue::Array(vec![VrlValue::Integer(4), VrlValue::Integer(5)])
        );
    }

    #[test]
    fn test_dictionary_encoded() {
        let schema = Schema::new(vec![Field::new(
            "color",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
        )]);

        let keys = Int32Array::from(vec![0, 1, 0, 1]);
        let values = StringArray::from(vec!["red", "blue"]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict_array)]).unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 4);
        assert_eq!(
            events[0].as_log().get("color").unwrap(),
            &VrlValue::from("red")
        );
        assert_eq!(
            events[1].as_log().get("color").unwrap(),
            &VrlValue::from("blue")
        );
        assert_eq!(
            events[2].as_log().get("color").unwrap(),
            &VrlValue::from("red")
        );
        assert_eq!(
            events[3].as_log().get("color").unwrap(),
            &VrlValue::from("blue")
        );
    }

    #[test]
    fn test_legacy_namespace_adds_timestamp() {
        let schema = Schema::new(vec![Field::new("msg", ArrowDataType::Utf8, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Legacy).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        assert_eq!(log.get("msg").unwrap(), &VrlValue::from("hello"));
        // Legacy namespace should have a timestamp
        if let Some(timestamp_key) = log_schema().timestamp_key_target_path() {
            assert!(log.contains(timestamp_key));
        }
    }

    #[test]
    fn test_vector_namespace_no_extra_timestamp() {
        let schema = Schema::new(vec![Field::new("msg", ArrowDataType::Utf8, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        // Vector namespace should NOT auto-insert timestamp
        if let Some(timestamp_key) = log_schema().timestamp_key_target_path() {
            assert!(
                !log.contains(timestamp_key),
                "Vector namespace should not auto-insert timestamp"
            );
        }
    }

    #[test]
    fn test_invalid_input() {
        let deser = ArrowStreamDeserializer;
        let result = deser.parse(
            Bytes::from_static(b"not valid arrow ipc"),
            LogNamespace::Vector,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_with_encoder() {
        use crate::encoding::format::arrow::encode_events_to_arrow_ipc_stream;

        let ts = Utc::now();
        let schema = Schema::new(vec![
            Field::new("message", ArrowDataType::Utf8, true),
            Field::new("count", ArrowDataType::Int64, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]);

        let mut log = LogEvent::default();
        log.insert("message", "test round trip");
        log.insert("count", 42);
        log.insert("ts", ts);
        let events = vec![Event::Log(log)];

        let ipc_bytes = encode_events_to_arrow_ipc_stream(&events, Arc::new(schema)).unwrap();

        let deser = ArrowStreamDeserializer;
        let decoded = deser.parse(ipc_bytes, LogNamespace::Vector).unwrap();

        assert_eq!(decoded.len(), 1);
        let log = decoded[0].as_log();
        assert_eq!(
            log.get("message").unwrap(),
            &VrlValue::from("test round trip")
        );
        assert_eq!(log.get("count").unwrap(), &VrlValue::Integer(42));
        // Timestamp round-trip (millisecond precision)
        if let VrlValue::Timestamp(decoded_ts) = log.get("ts").unwrap() {
            assert_eq!(decoded_ts.timestamp_millis(), ts.timestamp_millis());
        } else {
            panic!("Expected Timestamp");
        }
    }

    #[test]
    fn test_map_type() {
        let key_field = Field::new("keys", ArrowDataType::Utf8, false);
        let value_field = Field::new("values", ArrowDataType::Int64, true);
        let entries_field = Field::new(
            "entries",
            ArrowDataType::Struct(vec![key_field.clone(), value_field.clone()].into()),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "props",
            ArrowDataType::Map(Arc::new(entries_field), false),
            false,
        )]);

        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int64Array::from(vec![Some(1), Some(2)]);
        let struct_array = StructArray::from(vec![
            (Arc::new(key_field), Arc::new(keys) as Arc<dyn Array>),
            (Arc::new(value_field), Arc::new(values) as Arc<dyn Array>),
        ]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2].into());
        let map_entries_field = Field::new("entries", struct_array.data_type().clone(), false);
        let map_array = MapArray::new(
            Arc::new(map_entries_field),
            offsets,
            struct_array,
            None,
            false,
        );

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(map_array)]).unwrap();

        let bytes = batches_to_ipc_bytes(&schema, &[batch]);
        let deser = ArrowStreamDeserializer;
        let events = deser.parse(bytes, LogNamespace::Vector).unwrap();

        assert_eq!(events.len(), 1);
        let props = events[0].as_log().get("props").unwrap();
        match props {
            VrlValue::Object(map) => {
                assert_eq!(map.get("a").unwrap(), &VrlValue::Integer(1));
                assert_eq!(map.get("b").unwrap(), &VrlValue::Integer(2));
            }
            _ => panic!("Expected Object, got {props:?}"),
        }
    }

    #[test]
    fn test_config_methods() {
        let config = ArrowStreamDeserializerConfig;
        assert_eq!(config.output_type(), DataType::Log);
        assert!(config.build().is_ok());

        // Schema definitions should not panic
        let _ = config.schema_definition(LogNamespace::Vector);
        let _ = config.schema_definition(LogNamespace::Legacy);
    }
}
