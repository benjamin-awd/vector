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
    /// GCS bucket containing the Delta Lake table.
    ///
    /// This is the name of the Google Cloud Storage bucket where the Delta Lake
    /// table is stored. Do not include the `gs://` prefix.
    #[configurable(metadata(docs::examples = "my-data-bucket"))]
    #[configurable(metadata(docs::examples = "analytics-prod"))]
    pub bucket: String,

    /// Path to the Delta table within the bucket.
    ///
    /// This path should point to an existing Delta Lake table directory.
    /// The table must already exist - automatic table creation is not yet supported.
    #[configurable(metadata(docs::examples = "analytics/events"))]
    #[configurable(metadata(docs::examples = "logs/application"))]
    pub table_path: String,

    /// GCS service account credentials path.
    ///
    /// Path to a JSON file containing the GCS service account key.
    /// If not provided, the sink will attempt to use default credentials
    /// from the environment.
    #[configurable(metadata(docs::examples = "/path/to/service-account-key.json"))]
    #[configurable(metadata(docs::examples = "/etc/vector/gcs-credentials.json"))]
    pub credentials_path: Option<String>,

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
            bucket: "my-bucket".to_string(),
            table_path: "analytics/events".to_string(),
            credentials_path: Some("/path/to/service-account.json".to_string()),
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
        // 1. Build table URI and storage options for GCS
        let table_uri_str = format!("gs://{}/{}", self.bucket, self.table_path);
        let table_uri = Url::parse(&table_uri_str)
            .map_err(|e| format!("Invalid table URI {}: {}", table_uri_str, e))?;

        // Configure storage options for GCS
        let mut storage_options = HashMap::new();
        if let Some(creds) = &self.credentials_path {
            storage_options.insert("google_service_account".to_string(), creds.clone());
        }

        // 2. Open Delta table with storage options
        let mut table = deltalake::open_table_with_storage_options(table_uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table at {}: {}", table_uri_str, e))?;

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
    fn test_config_with_all_fields() {
        let config_str = r#"
            bucket = "test-bucket"
            table_path = "test/table"
            credentials_path = "/path/to/creds.json"

            [batch_encoding]
            allow_nullable_fields = true

            [batch]
            max_bytes = 104857600
            timeout_secs = 300
        "#;

        let config: DeltaLakeConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.table_path, "test/table");
        assert_eq!(
            config.credentials_path,
            Some("/path/to/creds.json".to_string())
        );
        assert!(config.batch_encoding.allow_nullable_fields);
    }

    #[test]
    fn test_config_minimal() {
        let config_str = r#"
            bucket = "test-bucket"
            table_path = "test/table"
        "#;

        let config: DeltaLakeConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.table_path, "test/table");
        assert_eq!(config.credentials_path, None);
    }
}
