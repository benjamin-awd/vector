//! Configuration for the Delta Lake sink.

use std::collections::HashMap;

use tower::ServiceBuilder;
use url::Url;
use vector_lib::codecs::encoding::{ArrowStreamSerializerConfig, BatchSerializerConfig as BatchSerializerConfigLib, SchemaProvider};
use vector_lib::configurable::configurable_component;
use vector_lib::sink::VectorSink;

use crate::codecs::{BatchEncoder, BatchSerializer, EncoderKind};
use crate::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext};
use crate::sinks::util::{
    BatchConfig, RealtimeSizeBasedDefaultBatchSettings, ServiceBuilderExt, TowerRequestConfig,
};
use crate::sinks::{prelude::*, Healthcheck};

use super::request_builder::DeltaLakeRequestBuilder;
use super::schema::DeltaLakeSchemaProvider;
use super::service::{DeltaLakeRetryLogic, DeltaLakeService};
use super::sink::DeltaLakeSink;

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

    /// Batch encoding configuration.
    ///
    /// The schema is automatically fetched from the existing Delta table.
    #[configurable(derived)]
    #[serde(default)]
    pub batch_encoding: ArrowStreamSerializerConfig,

    /// Enable automatic schema evolution.
    ///
    /// When enabled, the sink will automatically handle schema changes in the Delta Lake table:
    /// - Reload the table schema when write failures indicate schema mismatches
    /// - Retry writes with updated schema information
    /// - Allow Delta Lake to merge compatible schema changes (new nullable columns)
    ///
    /// This is useful when multiple writers may be updating the table schema, or when
    /// the table schema evolves over time. Disable this if you want strict schema enforcement.
    #[configurable(metadata(docs::examples = true))]
    #[serde(default = "default_schema_evolution")]
    pub enable_schema_evolution: bool,

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

fn default_schema_evolution() -> bool {
    true
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            table_uri: "gs://my-bucket/analytics/events".to_string(),
            storage_options: HashMap::new(),
            batch_encoding: ArrowStreamSerializerConfig::default(),
            enable_schema_evolution: default_schema_evolution(),
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
        // 1. Parse and validate table URI
        let table_uri = Url::parse(&self.table_uri)
            .map_err(|e| format!("Invalid table URI {}: {}", self.table_uri, e))?;

        // Validate URI scheme
        match table_uri.scheme() {
            "gs" | "s3" | "s3a" | "file" | "abfs" | "abfss" | "az" => {}
            scheme => {
                return Err(format!(
                    "Unsupported URI scheme '{}'. Supported: gs, s3, s3a, file, abfs, abfss, az",
                    scheme
                )
                .into())
            }
        }

        // 2. Open Delta table with storage options
        let mut table = deltalake::open_table_with_storage_options(
            table_uri.clone(),
            self.storage_options.clone(),
        )
        .await
        .map_err(|e| {
            format!(
                "Failed to open Delta table at {}: {}",
                self.table_uri, e
            )
        })?;

        // 3. Fetch schema from Delta table
        let mut arrow_config = self.batch_encoding.clone();
        let schema_provider = DeltaLakeSchemaProvider::new(table.clone());
        let schema = schema_provider
            .get_schema()
            .await
            .map_err(|e| format!("Failed to fetch schema from Delta table: {}", e))?;

        arrow_config.schema = Some(schema);

        // 4. Build encoder
        let batch_config = BatchSerializerConfigLib::ArrowStream(arrow_config);
        let arrow_serializer = batch_config
            .build()
            .map_err(|e| format!("Failed to build Arrow serializer: {}", e))?;
        let batch_serializer = BatchSerializer::Arrow(arrow_serializer);
        let encoder = EncoderKind::Batch(BatchEncoder::new(batch_serializer));

        // 5. Build request builder
        let request_builder = DeltaLakeRequestBuilder {
            encoder: (Transformer::default(), encoder),
        };

        // 6. Build service with retries
        let service = DeltaLakeService::new(table.clone(), self.enable_schema_evolution);
        let service = ServiceBuilder::new()
            .settings(self.request.into_settings(), DeltaLakeRetryLogic::default())
            .service(service);

        // 7. Build sink
        let batch_settings = self
            .batch
            .into_batcher_settings()
            .map_err(|e| format!("Failed to configure batching: {}", e))?;
        let sink = DeltaLakeSink::new(service, request_builder, batch_settings);

        // 8. Healthcheck - verify table is accessible
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

            [batch_encoding]
            allow_nullable_fields = true

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
        assert!(config.batch_encoding.allow_nullable_fields);
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
