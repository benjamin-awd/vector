use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::json::writer::{LineDelimited, Writer as JsonWriter};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, Bytes};
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, SamplingMode, Throughput, criterion_group,
    measurement::WallTime,
};
use vector_lib::codecs::decoding::format::{ArrowStreamDeserializer, Deserializer, LogEvents};
use vector::event::LogEvent;
use vector_lib::config::LogNamespace;

type VrlValue = vrl::value::Value;

/// Serialize record batches to Arrow IPC stream bytes.
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

/// JSON-based deserializer for comparison: RecordBatch → JSON bytes → serde_json → VrlValue.
fn deserialize_via_json(batch: &RecordBatch) -> Vec<LogEvent> {
    // Write batch as newline-delimited JSON
    let mut buf = Vec::new();
    let mut writer = JsonWriter::<_, LineDelimited>::new(&mut buf);
    writer.write(batch).unwrap();
    writer.finish().unwrap();

    // Parse each JSON line into a LogEvent
    buf.split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let json_val: serde_json::Value = serde_json::from_slice(line).unwrap();
            let vrl_val = VrlValue::from(json_val);
            let mut log = LogEvent::default();
            if let VrlValue::Object(map) = vrl_val {
                for (k, v) in map {
                    log.insert(k.as_str(), v);
                }
            }
            log
        })
        .collect()
}

/// Build a flat-schema batch (strings + ints + floats + booleans + timestamps) with N rows.
fn flat_batch(n: usize) -> (Schema, RecordBatch) {
    let schema = Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("score", ArrowDataType::Float64, false),
        Field::new("active", ArrowDataType::Boolean, false),
        Field::new(
            "created_at",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]);

    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = (0..n).map(|i| format!("user_{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let scores: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();
    let actives: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();
    let timestamps: Vec<i64> = (0..n).map(|i| 1_700_000_000_000i64 + i as i64 * 1000).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(BooleanArray::from(actives)),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
        ],
    )
    .unwrap();

    (schema, batch)
}

/// Build a nested-schema batch (struct + list columns) with N rows.
fn nested_batch(n: usize) -> (Schema, RecordBatch) {
    let inner_fields = vec![
        Field::new("x", ArrowDataType::Int64, false),
        Field::new("y", ArrowDataType::Utf8, false),
    ];
    let schema = Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new(
            "meta",
            ArrowDataType::Struct(inner_fields.clone().into()),
            false,
        ),
        Field::new(
            "tags",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, false))),
            false,
        ),
    ]);

    let ids: Vec<i64> = (0..n as i64).collect();
    let xs: Vec<i64> = (0..n as i64).map(|i| i * 10).collect();
    let ys: Vec<String> = (0..n).map(|i| format!("val_{i}")).collect();
    let y_refs: Vec<&str> = ys.iter().map(|s| s.as_str()).collect();

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("x", ArrowDataType::Int64, false)),
            Arc::new(Int64Array::from(xs)) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("y", ArrowDataType::Utf8, false)),
            Arc::new(StringArray::from(y_refs)) as Arc<dyn Array>,
        ),
    ]);

    // Each row gets 3 tags
    let all_tags: Vec<String> = (0..n)
        .flat_map(|i| vec![format!("tag_{i}_a"), format!("tag_{i}_b"), format!("tag_{i}_c")])
        .collect();
    let tag_refs: Vec<&str> = all_tags.iter().map(|s| s.as_str()).collect();
    let offsets: Vec<i32> = (0..=n).map(|i| (i * 3) as i32).collect();
    let list_array = ListArray::new(
        Arc::new(Field::new("item", ArrowDataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(tag_refs)),
        None,
    );

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(struct_array),
            Arc::new(list_array),
        ],
    )
    .unwrap();

    (schema, batch)
}

fn arrow_deserializer(c: &mut Criterion) {
    let mut group: BenchmarkGroup<WallTime> = c.benchmark_group("arrow_deserializer");
    group.sampling_mode(SamplingMode::Auto);

    for size in [10, 100, 1_000, 10_000, 100_000] {
        let (schema, batch) = flat_batch(size);
        let ipc_bytes = batches_to_ipc_bytes(&schema, &[batch.clone()]);

        group.throughput(Throughput::Elements(size as u64));

        // Current column-wise approach (RecordBatch → LogEvents directly)
        group.bench_function(&format!("column_wise/flat/{size}"), |b| {
            b.iter_batched(
                || batch.clone(),
                |batch| {
                    let LogEvents(logs) = LogEvents::try_from(&batch).unwrap();
                    logs
                },
                BatchSize::SmallInput,
            )
        });

        // JSON round-trip approach (RecordBatch → JSON → serde_json → VrlValue)
        group.bench_function(&format!("json_roundtrip/flat/{size}"), |b| {
            b.iter_batched(
                || batch.clone(),
                |batch| deserialize_via_json(&batch),
                BatchSize::SmallInput,
            )
        });

        // Full IPC parse (includes IPC decoding) with current approach
        group.bench_function(&format!("full_ipc_parse/flat/{size}"), |b| {
            b.iter_batched(
                || ipc_bytes.clone(),
                |bytes| {
                    ArrowStreamDeserializer
                        .parse(bytes, LogNamespace::Vector)
                        .unwrap()
                },
                BatchSize::SmallInput,
            )
        });
    }

    // Nested schema benchmarks
    for size in [10, 100, 1_000, 10_000, 100_000] {
        let (schema, batch) = nested_batch(size);
        let _ipc_bytes = batches_to_ipc_bytes(&schema, &[batch.clone()]);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(&format!("column_wise/nested/{size}"), |b| {
            b.iter_batched(
                || batch.clone(),
                |batch| {
                    let LogEvents(logs) = LogEvents::try_from(&batch).unwrap();
                    logs
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(&format!("json_roundtrip/nested/{size}"), |b| {
            b.iter_batched(
                || batch.clone(),
                |batch| deserialize_via_json(&batch),
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.01)
        .significance_level(0.05)
        .confidence_level(0.95)
        .nresamples(100_000)
        .sample_size(100);
    targets = arrow_deserializer
);
