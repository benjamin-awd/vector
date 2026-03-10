use std::io::Cursor;

use arrow::array::cast::AsArray;
use arrow::array::*;
use arrow::datatypes::{
    ArrowTemporalType, DataType as ArrowDataType, Date32Type, Date64Type, Float64Type, Int64Type,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use lookup::event_path;
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use vector_core::{
    config::{DataType, LogNamespace, log_schema},
    event::{Event, LogEvent},
    schema,
};
use vrl::value::KeyString;

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

pub struct LogEvents(pub Vec<LogEvent>);

impl TryFrom<&RecordBatch> for LogEvents {
    type Error = vector_common::Error;

    fn try_from(batch: &RecordBatch) -> vector_common::Result<Self> {
        let mut logs: Vec<_> = (0..batch.num_rows()).map(|_| LogEvent::default()).collect();

        for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
            let name = field.name().as_str();
            let path = event_path!(name);
            for (log, value) in logs.iter_mut().zip(column_to_vrl(col)?) {
                log.insert(path, value);
            }
        }

        Ok(LogEvents(logs))
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

        let timestamp_key = match log_namespace {
            LogNamespace::Legacy => log_schema().timestamp_key_target_path(),
            _ => None,
        };

        let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
        let now = Utc::now();
        let mut events = SmallVec::new();

        for batch in reader {
            let LogEvents(logs) = LogEvents::try_from(&batch?)?;
            events.reserve(logs.len());

            for mut log in logs {
                if let Some(ts_key) = timestamp_key
                    && !log.contains(ts_key)
                {
                    log.insert(ts_key, now);
                }
                events.push(Event::Log(log));
            }
        }
        Ok(events)
    }
}

/// Extract an entire Arrow column into a `Vec<VrlValue>`.
///
/// The array is downcast once per column and iterated contiguously, avoiding
/// per-row dynamic dispatch and improving cache locality.
fn column_to_vrl(array: &dyn Array) -> vector_common::Result<Vec<VrlValue>> {
    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_boolean();
            Ok(arr
                .iter()
                .map(|v| match v {
                    Some(v) => VrlValue::Boolean(v),
                    None => VrlValue::Null,
                })
                .collect())
        }

        // All integer types are cast to Int64 (VRL's only integer representation).
        // Wrapping `as i64` for u64 is consistent with how VRL handles u64
        // values everywhere (e.g. JSON deserialization via `From<u64>`).
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => {
            let casted = arrow::compute::cast(array, &ArrowDataType::Int64)?;
            let arr = casted.as_primitive::<Int64Type>();
            Ok(arr
                .iter()
                .map(|v| match v {
                    Some(v) => VrlValue::Integer(v),
                    None => VrlValue::Null,
                })
                .collect())
        }

        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            let casted = arrow::compute::cast(array, &ArrowDataType::Float64)?;
            let arr = casted.as_primitive::<Float64Type>();
            Ok(arr
                .iter()
                .map(|v| match v {
                    Some(v) => VrlValue::from_f64_or_zero(v),
                    None => VrlValue::Null,
                })
                .collect())
        }

        ArrowDataType::Utf8 => {
            let arr = array.as_string::<i32>();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }
        ArrowDataType::Utf8View => {
            let arr = array.as_string_view();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }

        ArrowDataType::Binary => {
            let arr = array.as_binary::<i32>();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }
        ArrowDataType::LargeBinary => {
            let arr = array.as_binary::<i64>();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }
        ArrowDataType::BinaryView => {
            let arr = array.as_binary_view();
            Ok(arr
                .iter()
                .map(|v| v.map(VrlValue::from).unwrap_or(VrlValue::Null))
                .collect())
        }

        ArrowDataType::Timestamp(unit, _) => timestamp_column_to_vrl(array, unit),

        ArrowDataType::Date32 => {
            let arr = array.as_primitive::<Date32Type>();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        return Ok(VrlValue::Null);
                    }
                    arr.value_as_date(i)
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|dt| VrlValue::Timestamp(dt.and_utc()))
                        .ok_or_else(|| "Invalid Date32 value".into())
                })
                .collect()
        }
        ArrowDataType::Date64 => {
            let arr = array.as_primitive::<Date64Type>();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        return Ok(VrlValue::Null);
                    }
                    arr.value_as_datetime(i)
                        .map(|dt| VrlValue::Timestamp(dt.and_utc()))
                        .ok_or_else(|| "Invalid Date64 value".into())
                })
                .collect()
        }

        ArrowDataType::Time32(_) => {
            let casted = arrow::compute::cast(
                &arrow::compute::cast(array, &ArrowDataType::Int32)?,
                &ArrowDataType::Int64,
            )?;
            let arr = casted.as_primitive::<Int64Type>();
            Ok(arr
                .iter()
                .map(|v| match v {
                    Some(v) => VrlValue::Integer(v),
                    None => VrlValue::Null,
                })
                .collect())
        }
        ArrowDataType::Time64(_) | ArrowDataType::Duration(_) => {
            let casted = arrow::compute::cast(array, &ArrowDataType::Int64)?;
            let arr = casted.as_primitive::<Int64Type>();
            Ok(arr
                .iter()
                .map(|v| match v {
                    Some(v) => VrlValue::Integer(v),
                    None => VrlValue::Null,
                })
                .collect())
        }

        ArrowDataType::List(_) => {
            let arr = array.as_list::<i32>();
            (0..arr.len())
                .map(|row| {
                    if arr.is_null(row) {
                        Ok(VrlValue::Null)
                    } else {
                        column_to_vrl(arr.value(row).as_ref()).map(VrlValue::Array)
                    }
                })
                .collect()
        }
        ArrowDataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            (0..arr.len())
                .map(|row| {
                    if arr.is_null(row) {
                        Ok(VrlValue::Null)
                    } else {
                        column_to_vrl(arr.value(row).as_ref()).map(VrlValue::Array)
                    }
                })
                .collect()
        }
        ArrowDataType::FixedSizeList(..) => {
            let arr = array.as_fixed_size_list();
            (0..arr.len())
                .map(|row| {
                    if arr.is_null(row) {
                        Ok(VrlValue::Null)
                    } else {
                        column_to_vrl(arr.value(row).as_ref()).map(VrlValue::Array)
                    }
                })
                .collect()
        }

        ArrowDataType::Struct(fields) => {
            let arr = array.as_struct();
            // Recursively extract child columns, then transpose into per-row objects.
            let child_columns: Vec<Vec<VrlValue>> = fields
                .iter()
                .enumerate()
                .map(|(i, _)| column_to_vrl(arr.column(i)))
                .collect::<Result<_, _>>()?;
            let mut child_iters: Vec<std::vec::IntoIter<VrlValue>> =
                child_columns.into_iter().map(|c| c.into_iter()).collect();

            Ok((0..arr.len())
                .map(|row| {
                    if arr.is_null(row) {
                        for iter in &mut child_iters {
                            let _ = iter.next();
                        }
                        VrlValue::Null
                    } else {
                        let mut map = vrl::value::ObjectMap::new();
                        for (i, field) in fields.iter().enumerate() {
                            map.insert(
                                KeyString::from(field.name().as_str()),
                                child_iters[i]
                                    .next()
                                    .expect("child column length matches row count"),
                            );
                        }
                        VrlValue::Object(map)
                    }
                })
                .collect())
        }

        ArrowDataType::Map(..) => {
            let arr = array.as_map();
            (0..arr.len())
                .map(|row| {
                    if arr.is_null(row) {
                        return Ok(VrlValue::Null);
                    }
                    let entry = arr.value(row);
                    let keys = column_to_vrl(entry.column(0))?;
                    let values = column_to_vrl(entry.column(1))?;
                    let mut map = vrl::value::ObjectMap::new();
                    for (key, val) in keys.into_iter().zip(values) {
                        let key_str = match key {
                            VrlValue::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
                            other => other.to_string(),
                        };
                        map.insert(KeyString::from(key_str), val);
                    }
                    Ok(VrlValue::Object(map))
                })
                .collect()
        }

        ArrowDataType::Dictionary(..) => {
            let dict = array.as_any_dictionary();
            let keys = dict.normalized_keys();
            let decoded_values = column_to_vrl(dict.values().as_ref())?;
            Ok((0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        VrlValue::Null
                    } else {
                        decoded_values[keys[row]].clone()
                    }
                })
                .collect())
        }

        // NOTE: VRL has no native decimal type, so Decimal128/Decimal256 cannot be
        // losslessly represented. Users should cast to Float64 or Utf8 before ingestion.
        dt @ (ArrowDataType::Decimal128(..) | ArrowDataType::Decimal256(..)) => Err(format!(
            "unsupported Arrow type: {dt}; consider converting to Float64 or Utf8 upstream"
        )
        .into()),

        other => Err(format!("unsupported Arrow type: {other}").into()),
    }
}

/// Extract an Arrow Timestamp column into VRL Timestamps.
///
/// NOTE: Arrow timestamps store UTC epoch values regardless of timezone
/// metadata, so the converted timestamp is correct. However, the original
/// timezone annotation is lost and not preserved as event metadata.
fn timestamp_column_to_vrl(
    array: &dyn Array,
    unit: &TimeUnit,
) -> vector_common::Result<Vec<VrlValue>> {
    fn convert<T: ArrowTemporalType>(array: &dyn Array) -> vector_common::Result<Vec<VrlValue>>
    where
        i64: From<T::Native>,
    {
        let arr = array.as_primitive::<T>();
        arr.iter()
            .enumerate()
            .map(|(i, v)| match v {
                None => Ok(VrlValue::Null),
                Some(_) => arr
                    .value_as_datetime(i)
                    .map(|dt| VrlValue::Timestamp(dt.and_utc()))
                    .ok_or_else(|| "Invalid timestamp value".into()),
            })
            .collect()
    }

    match unit {
        TimeUnit::Second => convert::<TimestampSecondType>(array),
        TimeUnit::Millisecond => convert::<TimestampMillisecondType>(array),
        TimeUnit::Microsecond => convert::<TimestampMicrosecondType>(array),
        TimeUnit::Nanosecond => convert::<TimestampNanosecondType>(array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use bytes::BufMut;
    use chrono::{DateTime, TimeZone};
    use std::sync::Arc;

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

    fn parse_ipc(
        schema: &Schema,
        columns: Vec<Arc<dyn Array>>,
        ns: LogNamespace,
    ) -> SmallVec<[Event; 1]> {
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();
        let bytes = batches_to_ipc_bytes(schema, &[batch]);
        ArrowStreamDeserializer.parse(bytes, ns).unwrap()
    }

    fn parse_one(schema: &Schema, columns: Vec<Arc<dyn Array>>) -> LogEvent {
        let events = parse_ipc(schema, columns, LogNamespace::Vector);
        assert_eq!(events.len(), 1);
        events.into_iter().next().unwrap().into_log()
    }

    #[test]
    fn test_empty_bytes() {
        let result = ArrowStreamDeserializer
            .parse(Bytes::new(), LogNamespace::Vector)
            .unwrap();
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
            Field::new("i8", ArrowDataType::Int8, false),
            Field::new("i16", ArrowDataType::Int16, false),
            Field::new("u8", ArrowDataType::UInt8, false),
            Field::new("u16", ArrowDataType::UInt16, false),
            Field::new("u32", ArrowDataType::UInt32, false),
            Field::new("u64", ArrowDataType::UInt64, false),
        ]);

        let log = parse_one(
            &schema,
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![123456789i64])),
                Arc::new(Float64Array::from(vec![2.5_f64])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(BinaryArray::from(vec![b"bytes" as &[u8]])),
                Arc::new(Int8Array::from(vec![-128i8])),
                Arc::new(Int16Array::from(vec![-32000i16])),
                Arc::new(UInt8Array::from(vec![255u8])),
                Arc::new(UInt16Array::from(vec![65535u16])),
                Arc::new(UInt32Array::from(vec![4_000_000u32])),
                Arc::new(UInt64Array::from(vec![9_000_000_000u64])),
            ],
        );

        assert_eq!(log.get("bool_col").unwrap(), &VrlValue::Boolean(true));
        assert_eq!(log.get("int32_col").unwrap(), &VrlValue::Integer(42));
        assert_eq!(log.get("int64_col").unwrap(), &VrlValue::Integer(123456789));
        assert_eq!(log.get("string_col").unwrap(), &VrlValue::from("hello"));
        assert_eq!(
            log.get("binary_col").unwrap(),
            &VrlValue::from(Bytes::from_static(b"bytes"))
        );
        assert_eq!(log.get("i8").unwrap(), &VrlValue::Integer(-128));
        assert_eq!(log.get("i16").unwrap(), &VrlValue::Integer(-32000));
        assert_eq!(log.get("u8").unwrap(), &VrlValue::Integer(255));
        assert_eq!(log.get("u16").unwrap(), &VrlValue::Integer(65535));
        assert_eq!(log.get("u32").unwrap(), &VrlValue::Integer(4_000_000));
        assert_eq!(log.get("u64").unwrap(), &VrlValue::Integer(9_000_000_000));
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

        let log = parse_one(
            &schema,
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
        );

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
        let log = parse_one(
            &schema,
            vec![
                Arc::new(Date32Array::from(vec![19723])),
                Arc::new(Date64Array::from(vec![1704067200000i64])), // 2024-01-01 00:00:00 UTC in ms
            ],
        );

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

        let events = parse_ipc(
            &schema,
            vec![
                Arc::new(StringArray::from(vec![None as Option<&str>, Some("value")])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
            ],
            LogNamespace::Vector,
        );

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
        let events = ArrowStreamDeserializer
            .parse(bytes, LogNamespace::Vector)
            .unwrap();

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

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", ArrowDataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["alice"])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("score", ArrowDataType::Int64, false)),
                Arc::new(Int64Array::from(vec![100i64])) as Arc<dyn Array>,
            ),
        ]);

        let log = parse_one(&schema, vec![Arc::new(struct_array)]);
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

        let events = parse_ipc(&schema, vec![Arc::new(list_array)], LogNamespace::Vector);

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].as_log().get("nums").unwrap(),
            &VrlValue::Array(vec![
                VrlValue::Integer(1),
                VrlValue::Integer(2),
                VrlValue::Integer(3)
            ])
        );
        assert_eq!(
            events[1].as_log().get("nums").unwrap(),
            &VrlValue::Array(vec![VrlValue::Integer(4), VrlValue::Integer(5)])
        );
    }

    #[test]
    fn test_invalid_input() {
        let result = ArrowStreamDeserializer.parse(
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
        let decoded = ArrowStreamDeserializer
            .parse(ipc_bytes, LogNamespace::Vector)
            .unwrap();

        assert_eq!(decoded.len(), 1);
        let log = decoded[0].as_log();
        assert_eq!(
            log.get("message").unwrap(),
            &VrlValue::from("test round trip")
        );
        assert_eq!(log.get("count").unwrap(), &VrlValue::Integer(42));
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

        let log = parse_one(&schema, vec![Arc::new(map_array)]);
        match log.get("props").unwrap() {
            VrlValue::Object(map) => {
                assert_eq!(map.get("a").unwrap(), &VrlValue::Integer(1));
                assert_eq!(map.get("b").unwrap(), &VrlValue::Integer(2));
            }
            other => panic!("Expected Object, got {other:?}"),
        }
    }
}
