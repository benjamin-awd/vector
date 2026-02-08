//! Parquet file format codec for batched event encoding

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use bytes::{BufMut, BytesMut};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Arc};
use vector_config::{
    Configurable, GenerateError, ToValue, configurable_component, schema::SchemaGenerator,
};
use vector_core::event::Event;

use super::arrow::{ArrowEncodingError, build_record_batch, make_field_nullable};

/// Supported field types for Parquet/Arrow schema configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// UTF-8 string.
    String,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 64-bit integer.
    Uint64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Boolean.
    Boolean,
    /// Timestamp with microsecond precision (UTC).
    Timestamp,
    /// Date stored as days since epoch.
    Date,
    /// JSON stored as UTF-8 string.
    Json,
    /// Binary data.
    Binary,
    /// List of values of a single type.
    List {
        /// Element type.
        item: Box<FieldType>,
    },
    /// Nested struct with named fields.
    Struct {
        /// Struct fields.
        fields: Vec<FieldConfig>,
    },
    /// Key-value map.
    Map {
        /// Key type (must be a non-nullable scalar).
        key: Box<FieldType>,
        /// Value type.
        value: Box<FieldType>,
    },
}

impl FieldType {
    /// Convert to Arrow `DataType`.
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            FieldType::String => DataType::Utf8,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Uint64 => DataType::UInt64,
            FieldType::Float32 => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            FieldType::Date => DataType::Date32,
            FieldType::Json => DataType::Utf8,
            FieldType::Binary => DataType::Binary,
            FieldType::List { item } => {
                DataType::List(Arc::new(Field::new("item", item.to_arrow_type(), true)))
            }
            FieldType::Struct { fields } => {
                let arrow_fields: Vec<Field> =
                    fields.iter().map(FieldConfig::to_arrow_field).collect();
                DataType::Struct(Fields::from(arrow_fields))
            }
            FieldType::Map { key, value } => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", key.to_arrow_type(), false),
                        Field::new("value", value.to_arrow_type(), true),
                    ])),
                    false,
                )),
                false,
            ),
        }
    }
}

impl Configurable for FieldType {
    fn generate_schema(
        _: &RefCell<SchemaGenerator>,
    ) -> Result<vector_config::schema::SchemaObject, GenerateError> {
        Ok(vector_config::schema::SchemaObject::default())
    }
}

impl ToValue for FieldType {
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("could not convert FieldType to JSON")
    }
}

/// A field in the schema configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct FieldConfig {
    /// Field name.
    pub name: String,

    /// Field type.
    #[serde(rename = "type")]
    #[configurable(derived)]
    pub field_type: FieldType,

    /// Whether the field is nullable.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

impl FieldConfig {
    fn to_arrow_field(&self) -> Field {
        Field::new(&self.name, self.field_type.to_arrow_type(), self.nullable)
    }
}

fn default_nullable() -> bool {
    true
}

/// Schema configuration for Parquet/Arrow output.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct SchemaConfig {
    /// List of fields in the schema.
    #[serde(default)]
    #[configurable(derived)]
    pub fields: Vec<FieldConfig>,
}

impl SchemaConfig {
    /// Convert to an Arrow `Schema`.
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let arrow_fields: Vec<Field> = self
            .fields
            .iter()
            .map(FieldConfig::to_arrow_field)
            .collect();
        Arc::new(Schema::new(arrow_fields))
    }
}

/// Compression codec for Parquet files.
///
/// Parquet handles compression internally at the column-chunk level,
/// so this is separate from Vector's transport-level compression.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// Snappy compression (default). Fast with reasonable compression ratio.
    #[default]
    Snappy,
    /// Gzip compression. Better compression ratio, slower.
    Gzip,
    /// Zstandard compression. Good balance of speed and compression.
    Zstd,
    /// LZ4 compression. Very fast, lower compression ratio.
    Lz4,
    /// Brotli compression. High compression ratio, slower.
    Brotli,
    /// No compression.
    None,
}

impl From<&ParquetCompression> for parquet::basic::Compression {
    fn from(c: &ParquetCompression) -> Self {
        match c {
            ParquetCompression::Snappy => Self::SNAPPY,
            ParquetCompression::Gzip => Self::GZIP(GzipLevel::default()),
            ParquetCompression::Zstd => Self::ZSTD(ZstdLevel::default()),
            ParquetCompression::Lz4 => Self::LZ4,
            ParquetCompression::Brotli => Self::BROTLI(BrotliLevel::default()),
            ParquetCompression::None => Self::UNCOMPRESSED,
        }
    }
}

/// Configuration for Parquet serialization
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct ParquetSerializerConfig {
    /// The Arrow schema to use for encoding.
    #[serde(skip)]
    #[configurable(derived)]
    pub schema: Option<Schema>,

    /// Allow null values for non-nullable fields in the schema.
    #[serde(default)]
    #[configurable(derived)]
    pub allow_nullable_fields: bool,

    /// Compression codec for the Parquet file.
    #[serde(default)]
    #[configurable(derived)]
    pub compression: ParquetCompression,

    /// Maximum number of rows per row group.
    #[serde(default)]
    #[configurable(derived)]
    pub max_row_group_size: Option<usize>,
}

/// Parquet batch serializer that produces complete Parquet files.
#[derive(Clone, Debug)]
pub struct ParquetSerializer {
    schema: SchemaRef,
    writer_properties: WriterProperties,
}

impl ParquetSerializer {
    /// Create a new ParquetSerializer with the given configuration.
    pub fn new(config: ParquetSerializerConfig) -> Result<Self, vector_common::Error> {
        let schema = config
            .schema
            .ok_or_else(|| vector_common::Error::from("Parquet serializer requires a schema."))?;

        let schema = if config.allow_nullable_fields {
            let nullable_fields: Fields = schema
                .fields()
                .iter()
                .map(|f| make_field_nullable(f))
                .collect::<Result<_, _>>()
                .map_err(|e: ArrowEncodingError| vector_common::Error::from(e.to_string()))?;
            Schema::new_with_metadata(nullable_fields, schema.metadata().clone())
        } else {
            schema
        };

        let mut props_builder =
            WriterProperties::builder().set_compression((&config.compression).into());

        if let Some(max_row_group_size) = config.max_row_group_size {
            props_builder = props_builder.set_max_row_group_size(max_row_group_size);
        }

        Ok(Self {
            schema: SchemaRef::new(schema),
            writer_properties: props_builder.build(),
        })
    }
}

impl tokio_util::codec::Encoder<Vec<Event>> for ParquetSerializer {
    type Error = ArrowEncodingError;

    fn encode(&mut self, events: Vec<Event>, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        if events.is_empty() {
            return Err(ArrowEncodingError::NoEvents);
        }

        let record_batch = build_record_batch(Arc::clone(&self.schema), &events)?;

        let parquet_err =
            |source: parquet::errors::ParquetError| ArrowEncodingError::ParquetWrite {
                source: source.into(),
            };

        let mut writer = ArrowWriter::try_new(
            (&mut *buffer).writer(),
            Arc::clone(&self.schema),
            Some(self.writer_properties.clone()),
        )
        .map_err(parquet_err)?;

        writer.write(&record_batch).map_err(parquet_err)?;
        writer.close().map_err(parquet_err)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use tokio_util::codec::Encoder as _;
    use vector_core::event::{LogEvent, Value};

    fn create_event<V>(fields: Vec<(&str, V)>) -> Event
    where
        V: Into<Value>,
    {
        let mut log = LogEvent::default();
        for (key, value) in fields {
            log.insert(key, value.into());
        }
        Event::Log(log)
    }

    #[test]
    fn test_round_trip() {
        let schema = Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]);

        let config = ParquetSerializerConfig {
            schema: Some(schema.clone()),
            ..Default::default()
        };

        let mut serializer = ParquetSerializer::new(config).expect("Failed to create serializer");

        let events = vec![
            create_event(vec![
                ("message", Value::from("hello")),
                ("count", Value::Integer(1)),
            ]),
            create_event(vec![
                ("message", Value::from("world")),
                ("count", Value::Integer(2)),
            ]),
        ];

        let mut buffer = BytesMut::new();
        serializer
            .encode(events, &mut buffer)
            .expect("Encoding should succeed");

        assert!(!buffer.is_empty(), "Buffer should contain Parquet data");

        // Read back
        let bytes = buffer.freeze();
        let reader =
            ParquetRecordBatchReader::try_new(bytes, 1024).expect("Failed to create reader");

        let batches: Vec<_> = reader
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("Failed to read batches");
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let msg_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(msg_array.value(0), "hello");
        assert_eq!(msg_array.value(1), "world");

        let count_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        assert_eq!(count_array.value(0), 1);
        assert_eq!(count_array.value(1), 2);
    }

    #[test]
    fn test_all_compression_variants() {
        let schema = Schema::new(vec![Field::new("message", DataType::Utf8, true)]);

        let compressions = [
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Lz4,
            ParquetCompression::Brotli,
            ParquetCompression::None,
        ];

        for compression in compressions {
            let config = ParquetSerializerConfig {
                schema: Some(schema.clone()),
                compression,
                ..Default::default()
            };

            let mut serializer =
                ParquetSerializer::new(config).expect("Failed to create serializer");

            let events = vec![create_event(vec![("message", Value::from("test"))])];
            let mut buffer = BytesMut::new();
            serializer
                .encode(events, &mut buffer)
                .expect("Encoding should succeed");

            // Verify it's a valid Parquet file by reading it back
            let bytes = buffer.freeze();
            let reader =
                ParquetRecordBatchReader::try_new(bytes, 1024).expect("Failed to create reader");
            let batches: Vec<_> = reader
                .into_iter()
                .collect::<Result<_, _>>()
                .expect("Failed to read batches");
            assert_eq!(batches[0].num_rows(), 1);
        }
    }

    #[test]
    fn test_allow_nullable_fields() {
        let schema = Schema::new(vec![Field::new("strict_field", DataType::Int64, false)]);

        let config = ParquetSerializerConfig {
            schema: Some(schema),
            allow_nullable_fields: true,
            ..Default::default()
        };

        let mut serializer = ParquetSerializer::new(config).expect("Failed to create serializer");

        // Second event is missing the field
        let mut log1 = LogEvent::default();
        log1.insert("strict_field", 42);
        let log2 = LogEvent::default();
        let events = vec![Event::Log(log1), Event::Log(log2)];

        let mut buffer = BytesMut::new();
        serializer
            .encode(events, &mut buffer)
            .expect("Encoding should succeed with allow_nullable_fields");

        let bytes = buffer.freeze();
        let reader =
            ParquetRecordBatchReader::try_new(bytes, 1024).expect("Failed to create reader");
        let batches: Vec<_> = reader
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("Failed to read");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        assert_eq!(array.value(0), 42);
        assert!(array.is_null(1));
    }

    #[test]
    fn test_empty_events_returns_error() {
        let schema = Schema::new(vec![Field::new("message", DataType::Utf8, true)]);
        let config = ParquetSerializerConfig {
            schema: Some(schema),
            ..Default::default()
        };

        let mut serializer = ParquetSerializer::new(config).expect("Failed to create serializer");
        let mut buffer = BytesMut::new();
        let result = serializer.encode(vec![], &mut buffer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArrowEncodingError::NoEvents));
    }
}
