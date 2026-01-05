//! Request builder for Delta Lake sink.
//!
//! This module implements the request builder that converts batches of Vector events
//! into Parquet-encoded bytes suitable for writing to Delta Lake tables.

use std::io;
use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use vector_lib::codecs::encoding::format::build_record_batch;

use crate::codecs::{BatchSerializer, EncoderKind};
use crate::sinks::prelude::*;

use super::schema_inference::build_inferred_schema;

/// Request payload for Delta Lake writes.
///
/// Contains Parquet-encoded data along with the schema and metadata needed
/// for acknowledgments and metrics.
#[derive(Clone)]
pub struct DeltaLakeRequest {
    /// Parquet-encoded data (RecordBatch serialized to Parquet format)
    pub parquet_data: Bytes,

    /// Schema for deserialization (stored separately from Parquet data)
    pub schema: Arc<Schema>,

    /// Event finalizers for acknowledgments
    pub finalizers: EventFinalizers,

    /// Request metadata for metrics
    pub request_metadata: RequestMetadata,
}

impl MetaDescriptive for DeltaLakeRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.request_metadata
    }
}

impl crate::event::Finalizable for DeltaLakeRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

/// Shared schema reference that can be updated after successful writes.
/// Uses ArcSwap for lock-free reads with atomic updates.
pub type SharedSchema = Arc<ArcSwap<Schema>>;

/// Request builder for Delta Lake.
///
/// This builder converts batches of Vector events into Parquet-encoded bytes
/// by first transforming events to Arrow RecordBatch format, then serializing
/// to Parquet.
#[derive(Clone)]
pub struct DeltaLakeRequestBuilder {
    /// Encoder that includes the transformer and Arrow batch serializer
    pub encoder: (Transformer, EncoderKind),

    /// Whether to enable automatic schema evolution (infer new fields from events)
    pub schema_evolution: bool,

    /// Shared schema reference, updated after successful writes by the service
    pub shared_schema: SharedSchema,
}

impl RequestBuilder<Vec<Event>> for DeltaLakeRequestBuilder {
    type Metadata = (Arc<Schema>, EventFinalizers);
    type Events = Vec<Event>;
    type Encoder = (Transformer, EncoderKind);
    type Payload = Bytes; // Parquet bytes
    type Request = DeltaLakeRequest;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        // Parquet handles compression internally, so we disable Vector's compression
        Compression::None
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut events: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        // Extract schema from the Arrow batch encoder
        let schema = match &self.encoder.1 {
            EncoderKind::Batch(batch_encoder) => match batch_encoder.serializer() {
                BatchSerializer::Arrow(arrow_serializer) => Arc::clone(arrow_serializer.schema()),
            },
            _ => {
                panic!("Delta Lake requires batch Arrow encoding");
            }
        };

        let finalizers = events.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&events);

        ((schema, finalizers), metadata_builder, events)
    }

    fn encode_events(
        &self,
        events: Self::Events,
    ) -> Result<EncodeResult<Self::Payload>, Self::Error> {
        // Transform events using the transformer
        let mut transformed_events = Vec::with_capacity(events.len());
        let mut byte_size = telemetry().create_request_count_byte_size();

        for mut event in events {
            self.encoder.0.transform(&mut event);
            byte_size.add_event(&event, event.estimated_json_encoded_size_of());
            transformed_events.push(event);
        }

        // Get base schema from the shared schema reference (updated after successful writes)
        // ArcSwap provides lock-free reads
        let base_schema: SchemaRef = Arc::clone(&self.shared_schema.load());

        // Determine final schema: either base schema or merged with inferred fields
        let schema = if self.schema_evolution {
            let inferred = build_inferred_schema(&base_schema, &transformed_events);

            // Log when new fields are discovered
            let new_fields: Vec<_> = inferred
                .fields()
                .iter()
                .filter(|f| base_schema.field_with_name(f.name()).is_err())
                .map(|f| f.name().as_str())
                .collect();

            if !new_fields.is_empty() {
                info!(
                    message = "Schema evolution: discovered new fields from events",
                    new_fields = ?new_fields,
                    total_fields = inferred.fields().len(),
                );
            }

            inferred
        } else {
            base_schema
        };

        // Build RecordBatch using existing Arrow infrastructure
        let record_batch = build_record_batch(Arc::clone(&schema), &transformed_events)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Serialize RecordBatch to Parquet bytes
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        writer
            .write(&record_batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        writer
            .close()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let parquet_bytes = Bytes::from(buffer);

        Ok(EncodeResult::uncompressed(parquet_bytes, byte_size))
    }

    fn build_request(
        &self,
        (schema, finalizers): Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        DeltaLakeRequest {
            parquet_data: payload.into_payload(),
            schema,
            finalizers,
            request_metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_request_builder_structure() {
        // Verify the structure compiles
        // More comprehensive tests would require setting up a full Arrow encoder
        // which is better suited for integration tests

        // Note: Creating a real DeltaLakeRequestBuilder requires a full BatchEncoder
        // with Arrow schema, which is complex to mock. Integration tests should
        // cover the full request building flow.
    }
}
