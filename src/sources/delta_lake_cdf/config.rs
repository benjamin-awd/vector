//! Configuration for the Delta Lake CDF source.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use serde_with::serde_as;
use url::Url;
use vector_lib::config::{DataType, LegacyKey, LogNamespace};
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::owned_value_path;
use vrl::value::Kind;

use crate::config::{GenerateConfig, SourceConfig, SourceContext, SourceOutput};

use super::checkpoint::DeltaLakeCdfCheckpointer;
use super::source::run_cdf_source;

/// Configuration for the `delta_lake_cdf` source.
#[serde_as]
#[configurable_component(source(
    "delta_lake_cdf",
    "Stream Change Data Feed (CDC) events from Delta Lake tables."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct DeltaLakeCdfConfig {
    /// Full URI to the Delta Lake table.
    ///
    /// Supports multiple storage backends:
    /// - Google Cloud Storage: `gs://bucket/path/to/table`
    /// - Amazon S3: `s3://bucket/path/to/table`
    /// - S3-compatible (MinIO, etc.): `s3://bucket/path/to/table` with custom endpoint
    /// - Azure Blob Storage: `abfs://container@account/path/to/table`
    /// - Local filesystem: `file:///path/to/table`
    ///
    /// The table must have Change Data Feed enabled (`delta.enableChangeDataFeed = true`).
    #[configurable(metadata(docs::examples = "s3://my-bucket/data/events"))]
    #[configurable(metadata(docs::examples = "gs://my-bucket/data/events"))]
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
    /// If not provided, the source will use default credentials from the environment.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// Interval between polling for new table versions.
    ///
    /// The source periodically checks for new commits to the Delta table
    /// and reads any new Change Data Feed records.
    #[serde(default = "default_poll_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 10))]
    #[configurable(metadata(docs::examples = 30))]
    pub poll_interval_secs: Duration,

    /// Where to start reading from on first run (when no checkpoint exists).
    #[serde(default)]
    #[configurable(derived)]
    pub start_position: StartPosition,

    /// Whether to include the full row data in events.
    ///
    /// When `true` (default), events include all columns from the Delta table.
    /// When `false`, events only include CDF metadata columns
    /// (`_change_type`, `_commit_version`, `_commit_timestamp`).
    #[serde(default = "default_include_data")]
    pub include_data: bool,

    /// Filter events by change type.
    ///
    /// If empty (default), all change types are included.
    /// Specify one or more types to filter the output.
    #[serde(default)]
    pub change_types: Vec<ChangeType>,

    /// Stop reading at this version (inclusive).
    ///
    /// Useful for bounded reads during testing. If not specified,
    /// the source continues reading indefinitely.
    #[serde(default)]
    pub ending_version: Option<i64>,

    /// Directory for storing checkpoints.
    ///
    /// The source persists its current position (version) to disk
    /// so it can resume from where it left off after restarts.
    ///
    /// Defaults to the global `data_dir` if not specified.
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    /// The namespace to use for logs. This overrides the global setting.
    #[serde(default)]
    #[configurable(metadata(docs::hidden))]
    pub log_namespace: Option<bool>,
}

/// Start position for reading Change Data Feed.
#[configurable_component]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// Start from the beginning of CDF history (version 0).
    ///
    /// This will read all historical changes if CDF was enabled from table creation.
    #[default]
    Beginning,

    /// Start from the latest version.
    ///
    /// Skip all existing data and only read new changes going forward.
    Latest,

    /// Start from a specific version number.
    Version(i64),
}

/// Change Data Feed change types.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    /// Row was inserted.
    Insert,
    /// Previous value of an updated row (before the update).
    UpdatePreimage,
    /// New value of an updated row (after the update).
    UpdatePostimage,
    /// Row was deleted.
    Delete,
}

impl ChangeType {
    /// Convert from CDF string representation.
    pub fn from_cdf_string(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(Self::Insert),
            "update_preimage" => Some(Self::UpdatePreimage),
            "update_postimage" => Some(Self::UpdatePostimage),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

const fn default_poll_interval() -> Duration {
    Duration::from_secs(10)
}

const fn default_include_data() -> bool {
    true
}

impl Default for DeltaLakeCdfConfig {
    fn default() -> Self {
        Self {
            table_uri: String::new(),
            storage_options: HashMap::new(),
            poll_interval_secs: default_poll_interval(),
            start_position: StartPosition::default(),
            include_data: default_include_data(),
            change_types: Vec::new(),
            ending_version: None,
            data_dir: None,
            log_namespace: None,
        }
    }
}

impl DeltaLakeCdfConfig {
    /// Validate the table URI scheme.
    fn validate_uri(&self) -> crate::Result<Url> {
        let table_uri = Url::parse(&self.table_uri)
            .map_err(|e| format!("Invalid table URI '{}': {}", self.table_uri, e))?;

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

        Ok(table_uri)
    }
}

impl GenerateConfig for DeltaLakeCdfConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            table_uri: "s3://my-bucket/data/events".to_string(),
            storage_options: HashMap::new(),
            poll_interval_secs: default_poll_interval(),
            start_position: StartPosition::default(),
            include_data: default_include_data(),
            change_types: Vec::new(),
            ending_version: None,
            data_dir: None,
            log_namespace: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "delta_lake_cdf")]
impl SourceConfig for DeltaLakeCdfConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);

        // Validate URI scheme
        let table_uri = self.validate_uri()?;

        // Open Delta table with storage options
        let table = deltalake::open_table_with_storage_options(
            table_uri.clone(),
            self.storage_options.clone(),
        )
        .await
        .map_err(|e| format!("Failed to open Delta table at {}: {}", self.table_uri, e))?;

        // Verify Change Data Feed is enabled on the table
        let cdf_enabled = table
            .snapshot()
            .map_err(|e| format!("Failed to get table snapshot: {}", e))?
            .table_config()
            .enable_change_data_feed
            .unwrap_or(false);

        if !cdf_enabled {
            return Err(format!(
                "Change Data Feed is not enabled on table '{}'. \
                 Set table property 'delta.enableChangeDataFeed' to 'true'.",
                self.table_uri
            )
            .into());
        }

        // Initialize checkpoint manager
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;
        let checkpointer = DeltaLakeCdfCheckpointer::new(&data_dir, &self.table_uri);

        // Determine starting version
        let start_version = determine_start_version(&checkpointer, &self.start_position, &table);

        Ok(Box::pin(run_cdf_source(
            table,
            self.clone(),
            start_version,
            checkpointer,
            cx.shutdown,
            cx.out,
            log_namespace,
        )))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);

        // Define the schema for CDF events
        let schema_definition = vector_lib::schema::Definition::default_for_namespace(
            &[log_namespace].into(),
        )
        .with_standard_vector_source_metadata()
        .with_source_metadata(
            DeltaLakeCdfConfig::NAME,
            Some(LegacyKey::Overwrite(owned_value_path!("_change_type"))),
            &owned_value_path!("change_type"),
            Kind::bytes(),
            Some("change_type"),
        )
        .with_source_metadata(
            DeltaLakeCdfConfig::NAME,
            Some(LegacyKey::Overwrite(owned_value_path!("_commit_version"))),
            &owned_value_path!("commit_version"),
            Kind::integer(),
            Some("commit_version"),
        )
        .with_source_metadata(
            DeltaLakeCdfConfig::NAME,
            Some(LegacyKey::Overwrite(owned_value_path!("_commit_timestamp"))),
            &owned_value_path!("commit_timestamp"),
            Kind::timestamp(),
            Some("commit_timestamp"),
        );

        vec![SourceOutput::new_maybe_logs(DataType::Log, schema_definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

/// Determine the starting version based on checkpoint and configuration.
fn determine_start_version(
    checkpointer: &DeltaLakeCdfCheckpointer,
    start_position: &StartPosition,
    table: &deltalake::DeltaTable,
) -> i64 {
    if let Some(checkpoint_version) = checkpointer.read_checkpoint() {
        info!(
            message = "Resuming from checkpoint",
            version = checkpoint_version,
        );
        return checkpoint_version;
    }

    // No checkpoint, use configured start position
    match start_position {
        StartPosition::Beginning => 0,
        StartPosition::Latest => {
            let version = table.version().unwrap_or(0);
            // Start from next version (don't process current state)
            version + 1
        }
        StartPosition::Version(v) => *v,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DeltaLakeCdfConfig>();
    }

    #[test]
    fn test_config_s3() {
        let config_str = r#"
            table_uri = "s3://test-bucket/test/table"
            poll_interval_secs = 30

            [storage_options]
            aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
            aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            aws_region = "us-east-1"
        "#;

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.table_uri, "s3://test-bucket/test/table");
        assert_eq!(config.poll_interval_secs, Duration::from_secs(30));
        assert_eq!(
            config.storage_options.get("aws_region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn test_config_minio() {
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

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(
            config.storage_options.get("aws_endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
    }

    #[test]
    fn test_config_start_position_beginning() {
        let config_str = r#"
            table_uri = "s3://bucket/table"
            start_position = "beginning"
        "#;

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.start_position, StartPosition::Beginning);
    }

    #[test]
    fn test_config_start_position_latest() {
        let config_str = r#"
            table_uri = "s3://bucket/table"
            start_position = "latest"
        "#;

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.start_position, StartPosition::Latest);
    }

    #[test]
    fn test_config_change_type_filter() {
        let config_str = r#"
            table_uri = "s3://bucket/table"
            change_types = ["insert", "delete"]
        "#;

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.change_types.len(), 2);
        assert!(config.change_types.contains(&ChangeType::Insert));
        assert!(config.change_types.contains(&ChangeType::Delete));
    }

    #[test]
    fn test_config_ending_version() {
        let config_str = r#"
            table_uri = "s3://bucket/table"
            ending_version = 100
        "#;

        let config: DeltaLakeCdfConfig = toml::from_str(config_str).expect("Config should parse");
        assert_eq!(config.ending_version, Some(100));
    }

    #[test]
    fn test_validate_uri_valid() {
        let config = DeltaLakeCdfConfig {
            table_uri: "s3://bucket/table".to_string(),
            ..Default::default()
        };
        assert!(config.validate_uri().is_ok());

        let config = DeltaLakeCdfConfig {
            table_uri: "gs://bucket/table".to_string(),
            ..Default::default()
        };
        assert!(config.validate_uri().is_ok());
    }

    #[test]
    fn test_validate_uri_invalid_scheme() {
        let config = DeltaLakeCdfConfig {
            table_uri: "http://bucket/table".to_string(),
            ..Default::default()
        };
        assert!(config.validate_uri().is_err());
    }

    #[test]
    fn test_change_type_from_string() {
        assert_eq!(
            ChangeType::from_cdf_string("insert"),
            Some(ChangeType::Insert)
        );
        assert_eq!(
            ChangeType::from_cdf_string("update_preimage"),
            Some(ChangeType::UpdatePreimage)
        );
        assert_eq!(
            ChangeType::from_cdf_string("update_postimage"),
            Some(ChangeType::UpdatePostimage)
        );
        assert_eq!(
            ChangeType::from_cdf_string("delete"),
            Some(ChangeType::Delete)
        );
        assert_eq!(ChangeType::from_cdf_string("unknown"), None);
    }
}
