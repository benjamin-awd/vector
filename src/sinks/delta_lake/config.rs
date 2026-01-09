//! Configuration for the Delta Lake sink.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tower::ServiceBuilder;
use url::Url;
use vector_lib::codecs::encoding::SchemaProvider;
use vector_lib::configurable::configurable_component;
use vector_lib::sink::VectorSink;

use crate::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext};
use crate::sinks::util::{
    BatchConfig, RealtimeSizeBasedDefaultBatchSettings, ServiceBuilderExt, TowerRequestConfig,
};
use crate::sinks::{Healthcheck, prelude::*};

use super::request_builder::{DeltaLakeRequestBuilder, SharedSchema};
use super::schema::DeltaLakeSchemaProvider;
use super::service::{DeltaLakeRetryLogic, DeltaLakeService};
use super::sink::DeltaLakeSink;

/// Default value for allow_nullable_fields - enabled by default for better compatibility
const fn default_allow_nullable_fields() -> bool {
    true
}

/// Configuration for the `delta_lake` sink.
#[configurable_component(sink(
    "delta_lake",
    "Write log events to Delta Lake tables on cloud object storage."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DeltaLakeConfig {
    /// Full URI to the Delta Lake table.
    ///
    /// Supports multiple storage backends:
    /// - Google Cloud Storage: `gs://bucket/path/to/table`
    /// - Amazon S3: `s3://bucket/path/to/table`
    /// - S3-compatible (MinIO, etc.): `s3://bucket/path/to/table` with custom endpoint
    ///
    /// The table must already exist - automatic table creation is not yet supported.
    #[configurable(metadata(docs::examples = "gs://my-bucket/analytics/events"))]
    #[configurable(metadata(docs::examples = "s3://my-bucket/logs/application"))]
    pub table_uri: String,

    /// Storage-specific options.
    ///
    /// Configuration options specific to the storage backend:
    ///
    /// **For GCS:**
    /// - `google_service_account`: Path to service account JSON file
    ///
    /// **For S3:**
    /// - `aws_access_key_id`: AWS access key
    /// - `aws_secret_access_key`: AWS secret key
    /// - `aws_region`: AWS region (e.g., "us-east-1")
    /// - `aws_endpoint`: Custom S3 endpoint (for MinIO, LocalStack, etc.)
    /// - `aws_allow_http`: Allow HTTP connections (for local testing)
    /// - `aws_s3_path_style`: Use path-style addressing (for MinIO)
    ///
    /// If not provided, the sink will use default credentials from the environment.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// Enable automatic schema evolution.
    ///
    /// When enabled, the sink will:
    /// - Discover new fields from incoming events and include them in writes
    /// - Allow Delta Lake to merge new columns into the table schema
    /// - Handle external schema changes by reloading and retrying
    ///
    /// Discovered fields are always nullable since existing table rows won't have them.
    ///
    /// Disable this for strict schema enforcement where events must match the table schema exactly.
    #[configurable(metadata(docs::examples = true))]
    #[serde(default)]
    pub schema_evolution: bool,

    /// Allow nullable fields in the schema.
    ///
    /// When enabled, all fields in the schema will be treated as nullable,
    /// allowing events with missing fields to be written without errors.
    /// This is useful when incoming events may not contain all fields defined in the table schema.
    #[configurable(metadata(docs::examples = true))]
    #[serde(default = "default_allow_nullable_fields")]
    pub allow_nullable_fields: bool,

    /// Batching behavior configuration.
    ///
    /// For optimal Delta Lake performance, larger batch sizes (50-100MB) are recommended.
    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    /// Request handling configuration.
    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    /// Acknowledgements configuration.
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            table_uri: "gs://my-bucket/analytics/events".to_string(),
            storage_options: HashMap::new(),
            schema_evolution: false,
            allow_nullable_fields: default_allow_nullable_fields(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: AcknowledgementsConfig::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "delta_lake")]
impl SinkConfig for DeltaLakeConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let table_uri = Url::parse(&self.table_uri)
            .map_err(|e| format!("Invalid table URI {}: {}", self.table_uri, e))?;

        match table_uri.scheme() {
            "gs" | "s3" | "s3a" | "file" | "abfs" | "abfss" | "az" => {}
            scheme => {
                return Err(format!(
                    "Unsupported URI scheme '{}'. Supported: gs, s3, s3a, file, abfs, abfss, az",
                    scheme
                )
                .into());
            }
        }

        let mut table = deltalake::open_table_with_storage_options(
            table_uri.clone(),
            self.storage_options.clone(),
        )
        .await
        .map_err(|e| format!("Failed to open Delta table at {}: {}", self.table_uri, e))?;

        // Get schema from the Delta table
        let schema_provider = DeltaLakeSchemaProvider::new(&table);
        let schema = schema_provider
            .get_schema()
            .await
            .map_err(|e| format!("Failed to fetch schema from Delta table: {}", e))?;

        let shared_schema: SharedSchema = Arc::new(ArcSwap::from_pointee(schema));

        let request_builder = DeltaLakeRequestBuilder {
            transformer: Transformer::default(),
            schema_evolution: self.schema_evolution,
            allow_nullable_fields: self.allow_nullable_fields,
            shared_schema: Arc::clone(&shared_schema),
        };

        let service = DeltaLakeService::new(table.clone(), self.schema_evolution, shared_schema);
        let service = ServiceBuilder::new()
            .settings(self.request.into_settings(), DeltaLakeRetryLogic)
            .service(service);

        let batch_settings = self
            .batch
            .into_batcher_settings()
            .map_err(|e| format!("Failed to configure batching: {}", e))?;
        let sink = DeltaLakeSink::new(service, request_builder, batch_settings);

        // 9. Healthcheck - verify table is accessible
        let healthcheck = Box::pin(async move {
            table
                .load()
                .await
                .map_err(|e| format!("Health check failed: unable to load Delta table: {}", e))?;
            Ok(())
        });

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DeltaLakeConfig>();
    }

    #[test]
    fn test_config_gcs() {
        let config_str = r#"
            table_uri = "gs://test-bucket/test/table"

            [storage_options]
            google_service_account = "/path/to/creds.json"

            [batch]
            max_bytes = 104857600
            timeout_secs = 300
        "#;

        let config: DeltaLakeConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.table_uri, "gs://test-bucket/test/table");
        assert_eq!(
            config.storage_options.get("google_service_account"),
            Some(&"/path/to/creds.json".to_string())
        );
    }

    #[test]
    fn test_config_s3() {
        let config_str = r#"
            table_uri = "s3://test-bucket/test/table"

            [storage_options]
            aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
            aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            aws_region = "us-east-1"
        "#;

        let config: DeltaLakeConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.table_uri, "s3://test-bucket/test/table");
        assert_eq!(
            config.storage_options.get("aws_region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn test_config_s3_minio() {
        let config_str = r#"
            table_uri = "s3://test-bucket/test/table"

            [storage_options]
            aws_access_key_id = "minioadmin"
            aws_secret_access_key = "minioadmin"
            aws_region = "us-east-1"
            aws_endpoint = "http://localhost:9000"
            aws_allow_http = "true"
            aws_s3_path_style = "true"
        "#;

        let config: DeltaLakeConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.table_uri, "s3://test-bucket/test/table");
        assert_eq!(
            config.storage_options.get("aws_endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(
            config.storage_options.get("aws_allow_http"),
            Some(&"true".to_string())
        );
    }
}
