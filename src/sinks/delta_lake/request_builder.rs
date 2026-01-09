//! Request builder for Delta Lake sink.
//!
//! This module converts batches of Vector events into Arrow RecordBatches
//! for writing to Delta Lake tables.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow::array::RecordBatch;
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use vector_lib::codecs::encoding::format::{build_record_batch, make_field_nullable};

use crate::sinks::prelude::*;

use super::schema_inference::build_inferred_schema;

/// Transform a schema to make all fields nullable.
fn make_schema_nullable(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| make_field_nullable(f).into())
            .collect::<Vec<FieldRef>>(),
        schema.metadata().clone(),
    )
}

/// Request payload for Delta Lake writes.
///
/// Contains Arrow RecordBatches ready for writing to Delta Lake.
/// By passing RecordBatches directly (instead of serializing to Parquet and back),
/// we avoid an expensive serialization round-trip.
#[derive(Clone)]
pub struct DeltaLakeRequest {
    /// Arrow RecordBatches to write
    pub batches: Vec<RecordBatch>,

    /// Event finalizers for acknowledgments
    pub finalizers: EventFinalizers,

    /// Request metadata for metrics
    pub request_metadata: RequestMetadata,

    /// Byte size of the batches (for metrics)
    pub byte_size: usize,
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
/// This builder converts batches of Vector events directly into Arrow RecordBatches,
/// avoiding the overhead of serializing to Parquet and deserializing back.
#[derive(Clone)]
pub struct DeltaLakeRequestBuilder {
    /// Transformer for event processing
    pub transformer: Transformer,

    /// Whether to enable automatic schema evolution (infer new fields from events)
    pub schema_evolution: bool,

    /// Whether to make all schema fields nullable
    pub allow_nullable_fields: bool,

    /// Shared schema reference, updated after successful writes by the service
    pub shared_schema: SharedSchema,
}

impl DeltaLakeRequestBuilder {
    /// Build a DeltaLakeRequest from a batch of events.
    pub fn build_request(&self, mut events: Vec<Event>) -> Result<DeltaLakeRequest, io::Error> {
        // Extract finalizers before transformation
        let finalizers = events.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&events);

        // Transform events
        let mut transformed_events = Vec::with_capacity(events.len());
        for mut event in events {
            self.transformer.transform(&mut event);
            transformed_events.push(event);
        }

        // Get base schema from the shared schema reference (updated after successful writes)
        let base_schema: SchemaRef = Arc::clone(&self.shared_schema.load());

        // Determine final schema: either base schema or merged with inferred fields
        let schema = if self.schema_evolution {
            build_inferred_schema(&base_schema, &transformed_events)
        } else {
            base_schema
        };

        // Make all fields nullable if configured
        let schema: SchemaRef = if self.allow_nullable_fields {
            Arc::new(make_schema_nullable(&schema))
        } else {
            schema
        };

        // Build RecordBatch directly - no Parquet serialization
        let record_batch = build_record_batch(Arc::clone(&schema), &transformed_events)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Calculate byte size for metrics (Arrow in-memory size)
        let byte_size = record_batch.get_array_memory_size();

        // Build request metadata using the byte size (since we don't have EncodeResult)
        let request_size = NonZeroUsize::new(byte_size).unwrap_or(NonZeroUsize::MIN);
        let request_metadata = metadata_builder.with_request_size(request_size);

        Ok(DeltaLakeRequest {
            batches: vec![record_batch],
            finalizers,
            request_metadata,
            byte_size,
        })
    }
}
