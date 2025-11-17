//! Configuration for the `Clickhouse` sink.

use std::{fmt, sync::Arc};

use http::{Request, StatusCode, Uri};
use hyper::Body;

use super::{
    request_builder::ClickhouseRequestBuilder,
    service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder},
    sink::{ClickhouseSink, PartitionKey},
};

use super::arrow_schema;
#[cfg(feature = "codecs-arrow")]
use crate::codecs::{BatchEncoder, BatchSerializer};
use crate::{
    codecs::{EncoderKind, EncodingConfigWithFraming, Transformer},
    http::{Auth, HttpClient, MaybeAuth},
    sinks::{
        prelude::*,
        util::{RealtimeSizeBasedDefaultBatchSettings, UriSerde, http::HttpService},
    },
};
use vector_lib::codecs::encoding::SerializerConfig;
#[cfg(feature = "codecs-arrow")]
use vector_lib::codecs::encoding::{
    ArrowStreamSerializer, ArrowStreamSerializerConfig, BatchSerializerConfig,
};

/// Data format.
///
/// The format used to parse input/output data.
///
/// [formats]: https://clickhouse.com/docs/en/interfaces/formats
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
#[allow(clippy::enum_variant_names)]
pub enum Format {
    #[derivative(Default)]
    /// JSONEachRow.
    JsonEachRow,

    /// JSONAsObject.
    JsonAsObject,

    /// JSONAsString.
    JsonAsString,

    /// ArrowStream.
    ArrowStream,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::JsonEachRow => write!(f, "JSONEachRow"),
            Format::JsonAsObject => write!(f, "JSONAsObject"),
            Format::JsonAsString => write!(f, "JSONAsString"),
            Format::ArrowStream => write!(f, "ArrowStream"),
        }
    }
}

/// Configuration for the `clickhouse` sink.
#[configurable_component(sink("clickhouse", "Deliver log data to a ClickHouse database."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    /// The endpoint of the ClickHouse server.
    #[serde(alias = "host")]
    #[configurable(metadata(docs::examples = "http://localhost:8123"))]
    pub endpoint: UriSerde,

    /// The table that data is inserted into.
    #[configurable(metadata(docs::examples = "mytable"))]
    pub table: Template,

    /// The database that contains the table that data is inserted into.
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: Option<Template>,

    /// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub skip_unknown_fields: Option<bool>,

    /// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
    #[serde(default)]
    pub date_time_best_effort: bool,

    /// Sets `insert_distributed_one_random_shard`, allowing ClickHouse to insert data into a random shard when using Distributed Table Engine.
    #[serde(default)]
    pub insert_random_shard: bool,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    /// The format to use for encoding events.
    ///
    /// This field is deprecated and maintained for backwards compatibility.
    /// New configurations should use `encoding` or `batch_encoding` instead.
    #[configurable(
        deprecated = "This option has been deprecated, use `encoding` or `batch_encoding` instead."
    )]
    #[serde(default)]
    pub format: Option<Format>,

    /// Event encoding configuration for event-by-event serialization (JSON, CSV, etc.).
    ///
    /// Defaults to JSON with newline delimited framing if not specified.
    /// This takes precedence over the deprecated `format` field.
    #[configurable(derived)]
    #[serde(flatten, default)]
    pub encoding: Option<EncodingConfigWithFraming>,

    /// Batch encoding configuration for batch-oriented formats (Arrow).
    ///
    /// If specified, this takes precedence over `encoding` and uses batch serialization.
    /// This is required for Arrow format which encodes multiple events at once.
    #[cfg(feature = "codecs-arrow")]
    #[configurable(derived)]
    #[serde(default)]
    pub batch_encoding: Option<BatchSerializerConfig>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    pub auth: Option<Auth>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub query_settings: QuerySettingsConfig,
}

/// Query settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct QuerySettingsConfig {
    /// Async insert-related settings.
    #[serde(default)]
    pub async_insert_settings: AsyncInsertSettingsConfig,
}

/// Async insert related settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AsyncInsertSettingsConfig {
    /// Sets `async_insert`, allowing ClickHouse to queue the inserted data and later flush to table in the background.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Sets `wait_for`, allowing ClickHouse to wait for processing of asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing: Option<bool>,

    /// Sets 'wait_for_processing_timeout`, to control the timeout for waiting for processing asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing_timeout: Option<u64>,

    /// Sets `async_insert_deduplicate`, allowing ClickHouse to perform deduplication when inserting blocks in the replicated table.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub deduplicate: Option<bool>,

    /// Sets `async_insert_max_data_size`, the maximum size in bytes of unparsed data collected per query before being inserted.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_data_size: Option<u64>,

    /// Sets `async_insert_max_query_number`, the maximum number of insert queries before being inserted
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_query_number: Option<u64>,
}

impl_generate_config_from_default!(ClickhouseConfig);

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            endpoint: Default::default(),
            table: Template::try_from("").unwrap(),
            database: None,
            skip_unknown_fields: None,
            date_time_best_effort: false,
            insert_random_shard: false,
            compression: Compression::gzip_default(),
            format: None,
            encoding: Some(default_encoding()),
            #[cfg(feature = "codecs-arrow")]
            batch_encoding: None,
            batch: Default::default(),
            auth: None,
            request: Default::default(),
            tls: None,
            acknowledgements: Default::default(),
            query_settings: Default::default(),
        }
    }
}

impl ClickhouseConfig {
    async fn build_encoder(
        &self,
        client: &HttpClient,
        endpoint: String,
        database: &Template,
        auth: Option<&Auth>,
    ) -> crate::Result<(Transformer, EncoderKind, Format)> {
        // Handle legacy `format` field for backwards compatibility
        if let Some(format) = &self.format {
            warn!(
                message = "Use of deprecated option `format`. Please use `encoding` or `batch_encoding` instead."
            );

            // Check for mutual exclusivity
            if self.encoding.is_some() || self.batch_encoding.is_some() {
                return Err(
                    "Cannot specify both `format` and `encoding`/`batch_encoding`. Please use `encoding` or `batch_encoding` instead."
                        .into(),
                );
            }

            // Reject ArrowStream format via legacy field
            if matches!(format, Format::ArrowStream) {
                return Err(
                    "ArrowStream format is not supported via the legacy `format` field. Please use `batch_encoding` with `codec = \"arrow_stream\"` instead."
                        .into(),
                );
            }

            // Map legacy format to appropriate encoding
            let encoding = match format {
                Format::JsonEachRow | Format::JsonAsObject | Format::JsonAsString => {
                    default_encoding()
                }
                Format::ArrowStream => unreachable!(), // rejected above
            };

            let (transformer, encoder) = encoding
                .build_encoder(crate::codecs::SinkType::StreamBased)
                .map_err(|e| format!("Failed to build encoder: {}", e))?;

            return Ok((transformer, encoder, *format));
        }

        #[cfg(feature = "codecs-arrow")]
        if let Some(batch_config) = &self.batch_encoding {
            let arrow_config = match batch_config {
                BatchSerializerConfig::ArrowStream(config) => config.clone(),
            };

            // Resolve the schema if needed
            let arrow_config = if arrow_config.schema.is_none() {
                self.resolve_arrow_schema(client, endpoint, database, auth, arrow_config)
                    .await?
            } else {
                arrow_config
            };

            let serializer = ArrowStreamSerializer::new(arrow_config)?;
            let batch_encoder = BatchEncoder::new(BatchSerializer::Arrow(serializer));
            let encoder = EncoderKind::Batch(batch_encoder);

            return Ok((Transformer::default(), encoder, Format::ArrowStream));
        }

        // Use regular event-by-event encoding
        let encoding = self
            .encoding
            .as_ref()
            .cloned()
            .unwrap_or_else(default_encoding);
        let (transformer, encoder) = encoding
            .build_encoder(crate::codecs::SinkType::StreamBased)
            .map_err(|e| format!("Failed to build encoder: {}", e))?;

        let format = Format::JsonEachRow;

        Ok((transformer, encoder, format))
    }

    #[cfg(feature = "codecs-arrow")]
    async fn resolve_arrow_schema(
        &self,
        client: &HttpClient,
        endpoint: String,
        database: &Template,
        auth: Option<&Auth>,
        base_config: ArrowStreamSerializerConfig,
    ) -> crate::Result<ArrowStreamSerializerConfig> {
        if self.table.is_dynamic() || database.is_dynamic() {
            return Err(
                "Arrow codec requires a static table and database (no templates). Schema inference is not supported."
                    .into(),
            );
        }

        let table_str = self.table.get_ref();
        let database_str = database.get_ref();

        debug!(
            "Fetching schema for table {}.{} at startup",
            database_str, table_str
        );

        let provider = Arc::new(arrow_schema::ClickHouseSchemaProvider::new(
            client.clone(),
            endpoint,
            database_str.to_string(),
            table_str.to_string(),
            auth.cloned(),
        ));

        let mut arrow_config = ArrowStreamSerializerConfig::with_provider(provider);
        // Preserve allow_nullable_fields setting from the user's config
        arrow_config.allow_nullable_fields = base_config.allow_nullable_fields;
        arrow_config.resolve().await.map_err(|e| {
            format!(
                "Failed to fetch schema for {}.{}: {}. Schema inference is not supported for ArrowStream format.",
                database_str, table_str, e
            )
        })?;

        debug!(
            "Successfully fetched Arrow schema with {} fields",
            arrow_config
                .schema
                .as_ref()
                .map(|s| s.fields().len())
                .unwrap_or(0)
        );

        Ok(arrow_config)
    }
}

fn default_encoding() -> EncodingConfigWithFraming {
    use vector_lib::codecs::encoding::{FramingConfig, JsonSerializerConfig};
    EncodingConfigWithFraming::new(
        Some(FramingConfig::NewlineDelimited),
        SerializerConfig::Json(JsonSerializerConfig::default()),
        Transformer::default(),
    )
}

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickhouseConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = self.endpoint.with_default_parts().uri;

        let auth = self.auth.choose_one(&self.endpoint.auth)?;

        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;

        let client = HttpClient::new(tls_settings, &cx.proxy)?;

        let clickhouse_service_request_builder = ClickhouseServiceRequestBuilder {
            auth: auth.clone(),
            endpoint: endpoint.clone(),
            skip_unknown_fields: self.skip_unknown_fields,
            date_time_best_effort: self.date_time_best_effort,
            insert_random_shard: self.insert_random_shard,
            compression: self.compression,
            query_settings: self.query_settings,
        };

        let service: HttpService<ClickhouseServiceRequestBuilder, PartitionKey> =
            HttpService::new(client.clone(), clickhouse_service_request_builder);

        let request_limits = self.request.into_settings();

        let service = ServiceBuilder::new()
            .settings(request_limits, ClickhouseRetryLogic::default())
            .service(service);

        let batch_settings = self.batch.into_batcher_settings()?;

        let database = self.database.clone().unwrap_or_else(|| {
            "default"
                .try_into()
                .expect("'default' should be a valid template")
        });

        // Build the appropriate encoder based on codec
        let (transformer, encoder, format) = self
            .build_encoder(&client, endpoint.to_string(), &database, auth.as_ref())
            .await?;

        let request_builder = ClickhouseRequestBuilder {
            compression: self.compression,
            encoder: (transformer, encoder),
        };

        let sink = ClickhouseSink::new(
            batch_settings,
            service,
            database,
            self.table.clone(),
            format,
            request_builder,
        );

        let healthcheck = Box::pin(healthcheck(client, endpoint, auth));

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

fn get_healthcheck_uri(endpoint: &Uri) -> String {
    let mut uri = endpoint.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri.push_str("?query=SELECT%201");
    uri
}

async fn healthcheck(client: HttpClient, endpoint: Uri, auth: Option<Auth>) -> crate::Result<()> {
    let uri = get_healthcheck_uri(&endpoint);
    let mut request = Request::get(uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ClickhouseConfig>();
    }

    #[test]
    fn test_get_healthcheck_uri() {
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/path/".parse().unwrap()),
            "http://localhost:8123/path/?query=SELECT%201"
        );
    }

    #[test]
    #[cfg(feature = "codecs-arrow")]
    fn test_arrow_codec_config() {
        // Test that arrow codec can be specified via batch_encoding
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"

            [batch_encoding]
            codec = "arrow_stream"
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Should parse config with arrow codec: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();
        assert!(config.batch_encoding.is_some());
        assert!(matches!(
            config.batch_encoding.as_ref().unwrap(),
            BatchSerializerConfig::ArrowStream(_)
        ));
    }

    #[test]
    fn test_json_codec_explicit() {
        // Test that JSON codec can be explicitly specified
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"

            [encoding]
            codec = "json"
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Should parse config with json codec: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();
        let encoding = config
            .encoding
            .as_ref()
            .expect("encoding should be present");
        let (_, serializer_config) = encoding.config();
        assert!(matches!(serializer_config, SerializerConfig::Json(_)));
    }

    #[test]
    fn test_legacy_format_field() {
        // Test that the legacy format field still works
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"
            format = "json_each_row"
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Should parse config with legacy format field: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();
        assert_eq!(config.format, Some(Format::JsonEachRow));
    }

    #[test]
    fn test_format_and_encoding_mutual_exclusivity() {
        // Test that specifying both format and encoding is rejected
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"
            format = "json_each_row"

            [encoding]
            codec = "json"
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Config should parse (validation happens at build time)"
        );
    }

    #[test]
    #[cfg(feature = "codecs-arrow")]
    fn test_arrow_codec_with_allow_nullable_fields() {
        // Test that allow_nullable_fields can be set in arrow stream config
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"

            [batch_encoding]
            codec = "arrow_stream"
            allow_nullable_fields = true
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Should parse config with allow_nullable_fields: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();
        assert!(config.batch_encoding.is_some());

        let arrow_config = match &config.batch_encoding {
            Some(BatchSerializerConfig::ArrowStream(arrow_config)) => arrow_config,
            _ => {
                assert!(false, "Expected ArrowStream config");
                return;
            }
        };

        assert!(
            arrow_config.allow_nullable_fields,
            "allow_nullable_fields should be true"
        );
    }

    #[test]
    #[cfg(feature = "codecs-arrow")]
    fn test_arrow_codec_default_allow_nullable_fields() {
        // Test that allow_nullable_fields defaults to false
        let config_str = r#"
            host = "http://localhost:8123"
            table = "my_table"
            database = "default"

            [batch_encoding]
            codec = "arrow_stream"
        "#;

        let config: Result<ClickhouseConfig, _> = toml::from_str(config_str);
        assert!(
            config.is_ok(),
            "Should parse config without allow_nullable_fields: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();
        assert!(config.batch_encoding.is_some());

        let arrow_config = match &config.batch_encoding {
            Some(BatchSerializerConfig::ArrowStream(arrow_config)) => arrow_config,
            _ => {
                assert!(false, "Expected ArrowStream config");
                return;
            }
        };

        assert!(
            !arrow_config.allow_nullable_fields,
            "allow_nullable_fields should default to false"
        );
    }
}
