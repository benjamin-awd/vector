use crate::codecs::Transformer;
use vector_lib::codecs::encoding::Codec;
use vector_lib::codecs::encoding::{FramingConfig, SerializerConfig};
use vector_lib::configurable::configurable_component;

/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
/// Configures how events are encoded into raw bytes.
/// The selected encoding also determines which input types (logs, metrics, traces) are supported.
pub struct EncodingConfig {
    #[serde(flatten)]
    encoding: SerializerConfig,

    #[serde(flatten)]
    transformer: Transformer,
}

impl EncodingConfig {
    /// Creates a new `EncodingConfig` with the provided `SerializerConfig` and `Transformer`.
    pub const fn new(encoding: SerializerConfig, transformer: Transformer) -> Self {
        Self {
            encoding,
            transformer,
        }
    }

    /// Returns a reference to the underlying [`SerializerConfig`]
    /// that defines how events are serialized before optional framing.
    pub fn encoding(&self) -> &SerializerConfig {
        &self.encoding
    }

    /// Build a `Transformer` that applies the encoding rules to an event before serialization.
    pub fn transformer(&self) -> Transformer {
        self.transformer.clone()
    }

    /// Build the `Codec` for this config.
    pub fn build(&self) -> crate::Result<Codec> {
        self.encoding.build()
    }
}

impl<T> From<T> for EncodingConfig
where
    T: Into<SerializerConfig>,
{
    fn from(encoding: T) -> Self {
        Self {
            encoding: encoding.into(),
            transformer: Default::default(),
        }
    }
}

/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct EncodingConfigWithFraming {
    #[configurable(derived)]
    framing: Option<FramingConfig>,

    #[configurable(derived)]
    encoding: EncodingConfig,
}

impl EncodingConfigWithFraming {
    /// Build a `Transformer` that applies the encoding rules to an event before serialization.
    pub fn transformer(&self) -> Transformer {
        self.encoding.transformer.clone()
    }

    /// Build the `Codec` for this config.
    pub fn build(&self) -> crate::Result<Codec> {
        // Build the base codec, which will have the default framer for streaming types.
        let base_codec = self.encoding.build()?;

        // If the user provided a `framing` override, apply it.
        if let Some(framing_override) = &self.framing {
            match base_codec {
                Codec::Stream(serializer, _default_framer) => {
                    // Replace the default framer with the user's override.
                    let new_framer = framing_override.build();
                    Ok(Codec::Stream(serializer, new_framer))
                }
                Codec::Batch(_) => Err(
                    "The configured codec is a batch format and does not support custom framing."
                        .into(),
                ),
            }
        } else {
            // No override, just return the codec with its default framing.
            Ok(base_codec)
        }
    }
}

/// The way a sink processes outgoing events.
pub enum SinkType {
    /// Events are sent in a continuous stream.
    StreamBased,
    /// Events are sent in a batch as a message.
    MessageBased,
}

impl<F, S> From<(Option<F>, S)> for EncodingConfigWithFraming
where
    F: Into<FramingConfig>,
    S: Into<SerializerConfig>,
{
    fn from((framing, encoding): (Option<F>, S)) -> Self {
        Self {
            framing: framing.map(Into::into),
            encoding: encoding.into().into(),
        }
    }
}

#[cfg(test)]
mod test {
    use vector_lib::lookup::lookup_v2::{parse_value_path, ConfigValuePath};

    use super::*;
    use crate::codecs::encoding::TimestampFormat;
    use vector_lib::codecs::encoding::{Framer, StreamingSerializer};
    #[test]
    fn deserialize_encoding_config() {
        let string = r#"
            {
                "codec": "json",
                "only_fields": ["a.b[0]"],
                "except_fields": ["ignore_me"],
                "timestamp_format": "unix"
            }
        "#;

        let encoding_config = serde_json::from_str::<EncodingConfig>(string).unwrap();

        let codec = encoding_config.build().unwrap();

        if let Codec::Stream(serializer, framer) = codec {
            assert!(matches!(serializer.as_ref(), StreamingSerializer::Json(_)));
            assert!(matches!(framer, Framer::NewlineDelimited(_)));
        } else {
            panic!("Expected a streaming codec!");
        }

        let transformer = encoding_config.transformer();
        assert_eq!(
            transformer.only_fields(),
            &Some(vec![ConfigValuePath(parse_value_path("a.b[0]").unwrap())])
        );
        assert_eq!(transformer.except_fields(), &Some(vec!["ignore_me".into()]));
        assert_eq!(transformer.timestamp_format(), &Some(TimestampFormat::Unix));
    }

    #[test]
    fn deserialize_and_build_config_with_framing() {
        let string = r#"
            {
                "framing": {
                    "method": "newline_delimited"
                },
                "encoding": {
                    "codec": "json",
                    "only_fields": ["a.b[0]"],
                    "except_fields": ["ignore_me"],
                    "timestamp_format": "unix"
                }
            }
        "#;

        let encoding_config = serde_json::from_str::<EncodingConfigWithFraming>(string).unwrap();

        let codec = encoding_config.build().unwrap();

        assert!(matches!(codec, Codec::Stream(_, _)));
        if let Codec::Stream(serializer, framer) = codec {
            assert!(matches!(serializer.as_ref(), StreamingSerializer::Json(_)));
            assert!(matches!(framer, Framer::NewlineDelimited(_)));
        } else {
            panic!("Expected a streaming codec!");
        }

        let transformer = encoding_config.transformer();

        assert_eq!(
            transformer.only_fields(),
            &Some(vec![ConfigValuePath(parse_value_path("a.b[0]").unwrap())])
        );
        assert_eq!(transformer.except_fields(), &Some(vec!["ignore_me".into()]));
        assert_eq!(transformer.timestamp_format(), &Some(TimestampFormat::Unix));
    }

    #[test]
    fn deserialize_encoding_config_without_framing() {
        let string = r#"
            {
                "encoding": {
                    "codec": "json",
                    "only_fields": ["a.b[0]"],
                    "except_fields": ["ignore_me"],
                    "timestamp_format": "unix"
                }
            }
        "#;

        let encoding_config = serde_json::from_str::<EncodingConfigWithFraming>(string).unwrap();
        let codec = encoding_config.build().unwrap();

        if let Codec::Stream(serializer, framer) = codec {
            assert!(matches!(serializer.as_ref(), StreamingSerializer::Json(_)));
            // When no framing is specified, the default for JSON is NewlineDelimited
            assert!(matches!(framer, Framer::NewlineDelimited(_)));
        } else {
            panic!("Expected a streaming codec!");
        }

        let transformer = encoding_config.transformer();
        assert_eq!(
            transformer.only_fields(),
            &Some(vec![ConfigValuePath(parse_value_path("a.b[0]").unwrap())])
        );
        assert_eq!(transformer.except_fields(), &Some(vec!["ignore_me".into()]));
        assert_eq!(transformer.timestamp_format(), &Some(TimestampFormat::Unix));
    }
}
