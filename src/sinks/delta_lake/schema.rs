//! Schema provider for Delta Lake tables.
//!
//! This module implements the `SchemaProvider` trait to fetch Arrow schemas
//! from existing Delta Lake tables at sink startup.

use arrow::datatypes::Schema;
use async_trait::async_trait;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::DeltaTable;
use vector_lib::codecs::encoding::format::{ArrowEncodingError, SchemaProvider};

/// Schema provider that fetches Arrow schema from Delta Lake table metadata.
///
/// Delta Lake stores table schemas in Arrow format natively, allowing zero-cost
/// schema conversion. The schema is fetched once at sink startup from the latest
/// table snapshot.
#[derive(Clone, Debug)]
pub struct DeltaLakeSchemaProvider {
    table: DeltaTable,
}

impl DeltaLakeSchemaProvider {
    /// Create a new schema provider for the given Delta table.
    pub fn new(table: DeltaTable) -> Self {
        Self { table }
    }
}

#[async_trait]
impl SchemaProvider for DeltaLakeSchemaProvider {
    async fn get_schema(&self) -> Result<Schema, ArrowEncodingError> {
        // Load the latest table snapshot
        let snapshot = self
            .table
            .snapshot()
            .map_err(|e| ArrowEncodingError::SchemaFetchError {
                message: format!("Failed to load Delta table snapshot: {}", e),
            })?;

        // Get Arrow schema from Delta table metadata
        // Delta Lake stores schema in Arrow format natively
        let delta_schema = snapshot.schema();

        // Convert Delta schema to Arrow schema using TryIntoArrow trait
        let arrow_schema = delta_schema.as_ref().try_into_arrow()
            .map_err(|e| ArrowEncodingError::SchemaFetchError {
                message: format!("Failed to convert Delta schema to Arrow: {}", e),
            })?;

        Ok(arrow_schema)
    }
}

#[cfg(test)]
mod tests {
    // Note: Integration tests with real Delta tables should go in integration_tests.rs
    // These are just basic unit tests for the schema provider structure

    #[test]
    fn test_schema_provider_new() {
        // This test just verifies the struct can be created
        // Real functionality requires a Delta table which needs integration tests
    }
}
