//! Columnar (Arrow) support for [`LogBatch`](super::LogBatch).
//!
//! This module is only compiled with the `columnar` feature. It holds the
//! universal fallback that turns a column-major Arrow `RecordBatch` back into
//! row-major [`LogEvent`]s (the reverse of the ClickHouse Arrow encoder), so
//! that every consumer which has not opted into reading the columnar
//! representation stays correct. It is a *materialization*, not the fast path.

use arrow::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
use chrono::{DateTime, Utc};
use ordered_float::NotNan;
use vrl::value::{ObjectMap, Value as VrlValue};

use super::{BatchMetadata, EventMetadata, LogEvent};

/// Explode a columnar batch into row-major [`LogEvent`]s, distributing the
/// batch-level metadata across the resulting rows.
///
/// - [`BatchMetadata::Shared`] clones its single metadata set onto every row
///   (one source frame ⇒ one finalizer set covering the whole batch).
/// - [`BatchMetadata::PerRow`] pairs `meta[i]` with row `i`.
pub(super) fn explode(batch: &RecordBatch, meta: BatchMetadata) -> Vec<LogEvent> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();

    // Build one field map per row.
    let mut rows: Vec<ObjectMap> = std::iter::repeat_with(ObjectMap::new)
        .take(num_rows)
        .collect();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let key = field.name().as_str();
        let column = batch.column(col_idx);
        let values = extract_column(column.as_ref());
        for (row, value) in values.into_iter().enumerate() {
            if let Some(value) = value {
                rows[row].insert(key.into(), value);
            }
        }
    }

    // Attach metadata.
    match meta {
        BatchMetadata::Shared(metadata) => rows
            .into_iter()
            .map(|fields| LogEvent::from_map(fields, metadata.clone()))
            .collect(),
        BatchMetadata::PerRow(metadatas) => {
            debug_assert_eq!(
                metadatas.len(),
                num_rows,
                "PerRow metadata length must match the batch row count"
            );
            rows.into_iter()
                .zip(metadatas)
                .map(|(fields, metadata)| LogEvent::from_map(fields, metadata))
                .collect()
        }
    }
}

/// Merge two `BatchMetadata` sidecars given their respective row counts.
///
/// Two identical `Shared` sets stay `Shared`; anything else is spread to a
/// `PerRow` sidecar so that index `i` continues to line up with row `i` after a
/// concat.
pub(super) fn merge_metadata(
    left: BatchMetadata,
    left_rows: usize,
    right: BatchMetadata,
    right_rows: usize,
) -> BatchMetadata {
    if let (BatchMetadata::Shared(l), BatchMetadata::Shared(r)) = (&left, &right)
        && l == r
    {
        return left;
    }
    let mut out = Vec::with_capacity(left_rows + right_rows);
    out.extend(spread(left, left_rows));
    out.extend(spread(right, right_rows));
    BatchMetadata::PerRow(out)
}

/// Expand a `BatchMetadata` into exactly `rows` per-row metadata sets.
fn spread(meta: BatchMetadata, rows: usize) -> Vec<EventMetadata> {
    match meta {
        BatchMetadata::Shared(m) => std::iter::repeat_n(m, rows).collect(),
        BatchMetadata::PerRow(v) => v,
    }
}

/// Extract each row of an Arrow column into a VRL [`Value`](VrlValue). `None`
/// marks a null slot (the field is simply omitted from that row's map, matching
/// the encoder, which does not emit nulls).
fn extract_column(array: &dyn Array) -> Vec<Option<VrlValue>> {
    let len = array.len();

    macro_rules! primitive {
        ($ty:ty, $map:expr) => {{
            let arr = array.as_any().downcast_ref::<$ty>().expect("array type");
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        #[allow(clippy::redundant_closure_call)]
                        Some(($map)(arr.value(i)))
                    }
                })
                .collect()
        }};
    }

    match array.data_type() {
        DataType::Boolean => primitive!(BooleanArray, |v: bool| VrlValue::Boolean(v)),
        DataType::Int8 => primitive!(Int8Array, |v: i8| VrlValue::Integer(i64::from(v))),
        DataType::Int16 => primitive!(Int16Array, |v: i16| VrlValue::Integer(i64::from(v))),
        DataType::Int32 => primitive!(Int32Array, |v: i32| VrlValue::Integer(i64::from(v))),
        DataType::Int64 => primitive!(Int64Array, |v: i64| VrlValue::Integer(v)),
        DataType::UInt8 => primitive!(UInt8Array, |v: u8| VrlValue::Integer(i64::from(v))),
        DataType::UInt16 => primitive!(UInt16Array, |v: u16| VrlValue::Integer(i64::from(v))),
        DataType::UInt32 => primitive!(UInt32Array, |v: u32| VrlValue::Integer(i64::from(v))),
        // u64 may exceed i64::MAX; wrap to i64 to preserve the bit pattern (the
        // encoder round-trips through the same ClickHouse column type).
        DataType::UInt64 => primitive!(UInt64Array, |v: u64| VrlValue::Integer(v as i64)),
        DataType::Float32 => primitive!(Float32Array, float_value_f32),
        DataType::Float64 => primitive!(Float64Array, float_value_f64),
        DataType::Utf8 => primitive!(StringArray, |v: &str| VrlValue::from(v)),
        DataType::LargeUtf8 => primitive!(LargeStringArray, |v: &str| VrlValue::from(v)),
        DataType::Timestamp(unit, _) => timestamp_column(array, *unit, len),
        // Unsupported types fall back to their Arrow display string, so the
        // materialization never panics on an exotic column.
        _ => display_column(array, len),
    }
}

fn float_value_f32(v: f32) -> VrlValue {
    NotNan::new(f64::from(v)).map_or(VrlValue::Null, VrlValue::Float)
}

fn float_value_f64(v: f64) -> VrlValue {
    NotNan::new(v).map_or(VrlValue::Null, VrlValue::Float)
}

fn timestamp_column(array: &dyn Array, unit: TimeUnit, len: usize) -> Vec<Option<VrlValue>> {
    let to_dt = |v: i64| -> Option<DateTime<Utc>> {
        match unit {
            TimeUnit::Second => DateTime::from_timestamp(v, 0),
            TimeUnit::Millisecond => DateTime::from_timestamp_millis(v),
            TimeUnit::Microsecond => DateTime::from_timestamp_micros(v),
            TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(v)),
        }
    };
    macro_rules! ts {
        ($ty:ty) => {{
            let arr = array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("timestamp array");
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        to_dt(arr.value(i)).map(VrlValue::Timestamp)
                    }
                })
                .collect()
        }};
    }
    match unit {
        TimeUnit::Second => ts!(TimestampSecondArray),
        TimeUnit::Millisecond => ts!(TimestampMillisecondArray),
        TimeUnit::Microsecond => ts!(TimestampMicrosecondArray),
        TimeUnit::Nanosecond => ts!(TimestampNanosecondArray),
    }
}

fn display_column(array: &dyn Array, len: usize) -> Vec<Option<VrlValue>> {
    match ArrayFormatter::try_new(array, &FormatOptions::default()) {
        Ok(formatter) => (0..len)
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(VrlValue::from(formatter.value(i).to_string()))
                }
            })
            .collect(),
        Err(_) => std::iter::repeat_with(|| None).take(len).collect(),
    }
}
