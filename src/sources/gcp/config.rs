#![allow(unused_imports)]
use crate::codecs::{Decoder, DecodingConfig};
use crate::config::{log_schema, LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::serde::{default_decoding, default_framing_message_based};
use crate::sources::gcp::source::run;
use crate::sources::Source;

use vector_config::configurable_component;
use vector_lib::codecs::decoding::{DeserializerConfig, FramingConfig};

/// Configuration for the `gcs` source.
#[configurable_component(source("gcs", "Reads objects from a Google Cloud Storage bucket."))]
#[derive(Clone, Debug)]
pub struct GcsConfig {
    /// The GCS bucket name.
    #[configurable(metadata(docs::examples = "my-log-bucket"))]
    pub bucket: String,

    /// An optional prefix for filtering objects.
    #[configurable(metadata(docs::examples = "logs/nginx/"))]
    pub prefix: Option<String>,

    #[configurable(derived)]
    #[serde(default = "default_framing_message_based")]
    pub framing: FramingConfig,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    pub decoding: DeserializerConfig,
    // You could add more options here, such as:
    // - endpoint: For custom GCS-compatible endpoints.
    // - service_account_path: To specify a credentials file directly.
    /// The namespace to use for logs. This overrides the global setting.
    #[serde(default)]
    #[configurable(metadata(docs::hidden))]
    pub log_namespace: Option<bool>,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            bucket: "foo".to_string(),
            prefix: None,
            decoding: default_decoding(),
            framing: default_framing_message_based(),
            log_namespace: None,
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "gcs")]
impl SourceConfig for GcsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);
        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;

       let source = run(self.clone(), decoder, cx);

        Ok(Box::pin(source))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);

        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata();

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl_generate_config_from_default!(GcsConfig);
