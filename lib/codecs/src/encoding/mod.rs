//! A collection of support structures that are used in the process of encoding
//! events into bytes.

pub mod format;
pub mod framing;

use std::fmt::Debug;

use bytes::BytesMut;
pub use format::{
    AvroSerializer, AvroSerializerConfig, AvroSerializerOptions, CefSerializer,
    CefSerializerConfig, CsvSerializer, CsvSerializerConfig, GelfSerializer, GelfSerializerConfig,
    JsonSerializer, JsonSerializerConfig, JsonSerializerOptions, LogfmtSerializer,
    LogfmtSerializerConfig, NativeJsonSerializer, NativeJsonSerializerConfig, NativeSerializer,
    NativeSerializerConfig, ParquetSerializer, ParquetSerializerConfig, ProtobufSerializer,
    ProtobufSerializerConfig, ProtobufSerializerOptions, RawMessageSerializer,
    RawMessageSerializerConfig, TextSerializer, TextSerializerConfig,
};
pub use framing::{
    BoxedFramer, BoxedFramingError, BytesEncoder, BytesEncoderConfig, CharacterDelimitedEncoder,
    CharacterDelimitedEncoderConfig, CharacterDelimitedEncoderOptions, LengthDelimitedEncoder,
    LengthDelimitedEncoderConfig, NewlineDelimitedEncoder, NewlineDelimitedEncoderConfig,
};
use vector_config::configurable_component;
use vector_core::{config::DataType, event::Event, schema};

/// An error that occurred while building an encoder.
pub type BuildError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error that occurred while encoding structured events into byte frames.
#[derive(Debug)]
pub enum Error {
    /// The error occurred while encoding the byte frame boundaries.
    FramingError(BoxedFramingError),
    /// The error occurred while serializing a structured event into bytes.
    SerializingError(vector_common::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FramingError(error) => write!(formatter, "FramingError({error})"),
            Self::SerializingError(error) => write!(formatter, "SerializingError({error})"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::FramingError(Box::new(error))
    }
}

/// Framing configuration.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The framing method."))]
pub enum FramingConfig {
    /// Event data is not delimited at all.
    Bytes,

    /// Event data is delimited by a single ASCII (7-bit) character.
    CharacterDelimited(CharacterDelimitedEncoderConfig),

    /// Event data is prefixed with its length in bytes.
    ///
    /// The prefix is a 32-bit unsigned integer, little endian.
    LengthDelimited(LengthDelimitedEncoderConfig),

    /// Event data is delimited by a newline (LF) character.
    NewlineDelimited,
}

impl From<BytesEncoderConfig> for FramingConfig {
    fn from(_: BytesEncoderConfig) -> Self {
        Self::Bytes
    }
}

impl From<CharacterDelimitedEncoderConfig> for FramingConfig {
    fn from(config: CharacterDelimitedEncoderConfig) -> Self {
        Self::CharacterDelimited(config)
    }
}

impl From<LengthDelimitedEncoderConfig> for FramingConfig {
    fn from(config: LengthDelimitedEncoderConfig) -> Self {
        Self::LengthDelimited(config)
    }
}

impl From<NewlineDelimitedEncoderConfig> for FramingConfig {
    fn from(_: NewlineDelimitedEncoderConfig) -> Self {
        Self::NewlineDelimited
    }
}

impl FramingConfig {
    /// Build the `Framer` from this configuration.
    pub fn build(&self) -> Framer {
        match self {
            FramingConfig::Bytes => Framer::Bytes(BytesEncoderConfig.build()),
            FramingConfig::CharacterDelimited(config) => Framer::CharacterDelimited(config.build()),
            FramingConfig::LengthDelimited(config) => Framer::LengthDelimited(config.build()),
            FramingConfig::NewlineDelimited => {
                Framer::NewlineDelimited(NewlineDelimitedEncoderConfig.build())
            }
        }
    }
}

/// Produce a byte stream from byte frames.
#[derive(Debug, Clone)]
pub enum Framer {
    /// Uses a `BytesEncoder` for framing.
    Bytes(BytesEncoder),
    /// Uses a `CharacterDelimitedEncoder` for framing.
    CharacterDelimited(CharacterDelimitedEncoder),
    /// Uses a `LengthDelimitedEncoder` for framing.
    LengthDelimited(LengthDelimitedEncoder),
    /// Uses a `NewlineDelimitedEncoder` for framing.
    NewlineDelimited(NewlineDelimitedEncoder),
    /// Uses an opaque `Encoder` implementation for framing.
    Boxed(BoxedFramer),
}

impl From<BytesEncoder> for Framer {
    fn from(encoder: BytesEncoder) -> Self {
        Self::Bytes(encoder)
    }
}

impl From<CharacterDelimitedEncoder> for Framer {
    fn from(encoder: CharacterDelimitedEncoder) -> Self {
        Self::CharacterDelimited(encoder)
    }
}

impl From<LengthDelimitedEncoder> for Framer {
    fn from(encoder: LengthDelimitedEncoder) -> Self {
        Self::LengthDelimited(encoder)
    }
}

impl From<NewlineDelimitedEncoder> for Framer {
    fn from(encoder: NewlineDelimitedEncoder) -> Self {
        Self::NewlineDelimited(encoder)
    }
}

impl From<BoxedFramer> for Framer {
    fn from(encoder: BoxedFramer) -> Self {
        Self::Boxed(encoder)
    }
}

impl tokio_util::codec::Encoder<()> for Framer {
    type Error = BoxedFramingError;

    fn encode(&mut self, _: (), buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Framer::Bytes(framer) => framer.encode((), buffer),
            Framer::CharacterDelimited(framer) => framer.encode((), buffer),
            Framer::LengthDelimited(framer) => framer.encode((), buffer),
            Framer::NewlineDelimited(framer) => framer.encode((), buffer),
            Framer::Boxed(framer) => framer.encode((), buffer),
        }
    }
}

/// Serializer configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "codec", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The codec to use for encoding events."))]
pub enum SerializerConfig {
    /// Encodes an event as an [Apache Avro][apache_avro] message.
    ///
    /// [apache_avro]: https://avro.apache.org/
    Avro {
        /// Apache Avro-specific encoder options.
        avro: AvroSerializerOptions,
    },

    /// Encodes an event as a CEF (Common Event Format) formatted message.
    ///
    Cef(
        /// Options for the CEF encoder.
        CefSerializerConfig,
    ),

    /// Encodes an event as a CSV message.
    ///
    /// This codec must be configured with fields to encode.
    ///
    Csv(CsvSerializerConfig),

    /// Encodes an event as a [GELF][gelf] message.
    ///
    /// This codec is experimental for the following reason:
    ///
    /// The GELF specification is more strict than the actual Graylog receiver.
    /// Vector's encoder currently adheres more strictly to the GELF spec, with
    /// the exception that some characters such as `@`  are allowed in field names.
    ///
    /// Other GELF codecs, such as Loki's, use a [Go SDK][implementation] that is maintained
    /// by Graylog and is much more relaxed than the GELF spec.
    ///
    /// Going forward, Vector will use that [Go SDK][implementation] as the reference implementation, which means
    /// the codec might continue to relax the enforcement of the specification.
    ///
    /// [gelf]: https://docs.graylog.org/docs/gelf
    /// [implementation]: https://github.com/Graylog2/go-gelf/blob/v2/gelf/reader.go
    Gelf,

    /// Encodes an event as [JSON][json].
    ///
    /// [json]: https://www.json.org/
    Json(JsonSerializerConfig),

    /// Encodes an event as a [logfmt][logfmt] message.
    ///
    /// [logfmt]: https://brandur.org/logfmt
    Logfmt,

    /// Encodes an event in the [native Protocol Buffers format][vector_native_protobuf].
    ///
    /// This codec is **[experimental][experimental]**.
    ///
    /// [vector_native_protobuf]: https://github.com/vectordotdev/vector/blob/master/lib/vector-core/proto/event.proto
    /// [experimental]: https://vector.dev/highlights/2022-03-31-native-event-codecs
    Native,

    /// Encodes an event in the [native JSON format][vector_native_json].
    ///
    /// This codec is **[experimental][experimental]**.
    ///
    /// [vector_native_json]: https://github.com/vectordotdev/vector/blob/master/lib/codecs/tests/data/native_encoding/schema.cue
    /// [experimental]: https://vector.dev/highlights/2022-03-31-native-event-codecs
    NativeJson,

    /// Encodes an event as a [Protobuf][protobuf] message.
    ///
    /// [protobuf]: https://protobuf.dev/
    Protobuf(ProtobufSerializerConfig),

    /// Encodes events as Parquet.
    Parquet(ParquetSerializerConfig),

    /// No encoding.
    ///
    /// This encoding uses the `message` field of a log event.
    ///
    /// Be careful if you are modifying your log events (for example, by using a `remap`
    /// transform) and removing the message field while doing additional parsing on it, as this
    /// could lead to the encoding emitting empty strings for the given event.
    RawMessage,

    /// Plain text encoding.
    ///
    /// This encoding uses the `message` field of a log event. For metrics, it uses an
    /// encoding that resembles the Prometheus export format.
    ///
    /// Be careful if you are modifying your log events (for example, by using a `remap`
    /// transform) and removing the message field while doing additional parsing on it, as this
    /// could lead to the encoding emitting empty strings for the given event.
    Text(TextSerializerConfig),
}

impl From<AvroSerializerConfig> for SerializerConfig {
    fn from(config: AvroSerializerConfig) -> Self {
        Self::Avro { avro: config.avro }
    }
}

impl From<CefSerializerConfig> for SerializerConfig {
    fn from(config: CefSerializerConfig) -> Self {
        Self::Cef(config)
    }
}

impl From<CsvSerializerConfig> for SerializerConfig {
    fn from(config: CsvSerializerConfig) -> Self {
        Self::Csv(config)
    }
}

impl From<GelfSerializerConfig> for SerializerConfig {
    fn from(_: GelfSerializerConfig) -> Self {
        Self::Gelf
    }
}

impl From<JsonSerializerConfig> for SerializerConfig {
    fn from(config: JsonSerializerConfig) -> Self {
        Self::Json(config)
    }
}

impl From<LogfmtSerializerConfig> for SerializerConfig {
    fn from(_: LogfmtSerializerConfig) -> Self {
        Self::Logfmt
    }
}

impl From<NativeSerializerConfig> for SerializerConfig {
    fn from(_: NativeSerializerConfig) -> Self {
        Self::Native
    }
}

impl From<NativeJsonSerializerConfig> for SerializerConfig {
    fn from(_: NativeJsonSerializerConfig) -> Self {
        Self::NativeJson
    }
}

impl From<ParquetSerializerConfig> for SerializerConfig {
    fn from(config: ParquetSerializerConfig) -> Self {
        Self::Parquet(config)
    }
}

impl From<ProtobufSerializerConfig> for SerializerConfig {
    fn from(config: ProtobufSerializerConfig) -> Self {
        Self::Protobuf(config)
    }
}

impl From<RawMessageSerializerConfig> for SerializerConfig {
    fn from(_: RawMessageSerializerConfig) -> Self {
        Self::RawMessage
    }
}

impl From<TextSerializerConfig> for SerializerConfig {
    fn from(config: TextSerializerConfig) -> Self {
        Self::Text(config)
    }
}

#[derive(Debug, Clone)]
pub enum Codec {
    Stream(Box<StreamingSerializer>, Framer),
    Batch(BatchSerializer),
}

impl Codec {
    /// Gets the appropriate `Content-Type` header for the codec.
    pub fn content_type(&self) -> &'static str {
        match self {
            // Logic for streaming codecs depends on both the serializer and the framer.
            Codec::Stream(serializer, framer) => match (&**serializer, framer) {
                (
                    StreamingSerializer::Json(_) | StreamingSerializer::NativeJson(_),
                    Framer::NewlineDelimited(_),
                ) => "application/x-ndjson",
                (
                    StreamingSerializer::Gelf(_)
                    | StreamingSerializer::Json(_)
                    | StreamingSerializer::NativeJson(_),
                    Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
                ) => "application/json",
                _ => "text/plain", // A safe default for other streaming types
            },

            // Logic for batch codecs depends only on the serializer.
            Codec::Batch(serializer) => match serializer {
                BatchSerializer::Parquet(_) => "application/octet-stream",
            },
        }
    }

    /// Gets the prefix that should enclose a batch of events.
    ///
    /// This is mainly for "pseudo-batching" streaming formats like JSON array.
    /// True batch formats like Parquet don't need an external prefix.
    pub fn batch_prefix(&self) -> &[u8] {
        match self {
            Codec::Stream(serializer, framer) => match (&**serializer, framer) {
                (
                    StreamingSerializer::Json(_) | StreamingSerializer::NativeJson(_),
                    Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
                ) => b"[",
                _ => b"",
            },
            Codec::Batch(_) => b"", // Batch formats are self-contained.
        }
    }

    /// Gets the suffix that should enclose a batch of events.
    pub fn batch_suffix(&self, empty_batch: bool) -> &[u8] {
        match self {
            Codec::Stream(serializer, framer) => match (&**serializer, framer, empty_batch) {
                (
                    StreamingSerializer::Json(_) | StreamingSerializer::NativeJson(_),
                    Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
                    _,
                ) => b"]",
                (StreamingSerializer::Text(_), Framer::NewlineDelimited(_), false) => b"\n",
                _ => b"",
            },
            Codec::Batch(_) => b"", // Batch formats are self-contained.
        }
    }

    /// Checks if the underlying serializer supports encoding to a JSON value.
    ///
    /// This capability is only relevant for streaming serializers. Batch serializers
    /// will always return `false`.
    pub fn supports_json(&self) -> bool {
        match self {
            Codec::Stream(serializer, _) => serializer.supports_json(),
            Codec::Batch(_) => false,
        }
    }

    /// Encodes an event and represents it as a JSON value.
    ///
    /// Panics if the underlying serializer does not support encoding to JSON. This will
    /// always panic for `Codec::Batch` variants.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        match self {
            Codec::Stream(serializer, _) => serializer.to_json_value(event),
            Codec::Batch(_) => {
                panic!("Batch codecs like Parquet do not support JSON value encoding.")
            }
        }
    }
}

impl SerializerConfig {
    pub fn build(&self) -> Result<Codec, BuildError> {
        match self {
            Self::Json(config) => {
                let serializer = StreamingSerializer::Json(config.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }

            Self::Parquet(config) => {
                let serializer = BatchSerializer::Parquet(config.build()?);
                Ok(Codec::Batch(serializer))
            }

            Self::Avro { avro } => {
                let serializer = StreamingSerializer::Avro(
                    AvroSerializerConfig::new(avro.schema.clone()).build()?,
                );
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Cef(config) => {
                let serializer = StreamingSerializer::Cef(config.build()?);
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Csv(config) => {
                let serializer = StreamingSerializer::Csv(config.build()?);
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Gelf => {
                let serializer = StreamingSerializer::Gelf(GelfSerializerConfig::new().build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Logfmt => {
                let serializer = StreamingSerializer::Logfmt(LogfmtSerializerConfig.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Native => {
                let serializer = StreamingSerializer::Native(NativeSerializerConfig.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::NativeJson => {
                let serializer =
                    StreamingSerializer::NativeJson(NativeJsonSerializerConfig.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Protobuf(config) => {
                let serializer = StreamingSerializer::Protobuf(config.build()?);
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::RawMessage => {
                let serializer =
                    StreamingSerializer::RawMessage(RawMessageSerializerConfig.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
            Self::Text(config) => {
                let serializer = StreamingSerializer::Text(config.build());
                let framer = self.default_stream_framing().build();
                Ok(Codec::Stream(serializer.into(), framer))
            }
        }
    }

    /// Return an appropriate default framer for the given serializer.
    pub fn default_stream_framing(&self) -> FramingConfig {
        match self {
            Self::Avro { .. } | Self::Native | Self::Protobuf(_) => {
                FramingConfig::LengthDelimited(LengthDelimitedEncoderConfig::default())
            }
            Self::Cef(_)
            | Self::Csv(_)
            | Self::Json(_)
            | Self::Logfmt
            | Self::NativeJson
            | Self::RawMessage
            | Self::Text(_) => FramingConfig::NewlineDelimited,
            Self::Gelf => {
                FramingConfig::CharacterDelimited(CharacterDelimitedEncoderConfig::new(0))
            }
            Self::Parquet(_) => {
                panic!("The 'parquet' codec is a batch-based format and does not support default stream framing.")
            }
        }
    }

    /// The data type of events that are accepted by this `Serializer`.
    pub fn input_type(&self) -> DataType {
        match self {
            Self::Avro { avro } => AvroSerializerConfig::new(avro.schema.clone()).input_type(),
            Self::Cef(config) => config.input_type(),
            Self::Csv(config) => config.input_type(),
            Self::Gelf => GelfSerializerConfig::input_type(),
            Self::Json(config) => config.input_type(),
            Self::Logfmt => LogfmtSerializerConfig.input_type(),
            Self::Native => NativeSerializerConfig.input_type(),
            Self::NativeJson => NativeJsonSerializerConfig.input_type(),
            Self::Parquet(config) => config.input_type(), // Added Parquet
            Self::Protobuf(config) => config.input_type(),
            Self::RawMessage => RawMessageSerializerConfig.input_type(),
            Self::Text(config) => config.input_type(),
        }
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        match self {
            Self::Avro { avro } => {
                AvroSerializerConfig::new(avro.schema.clone()).schema_requirement()
            }
            Self::Cef(config) => config.schema_requirement(),
            Self::Csv(config) => config.schema_requirement(),
            Self::Gelf => GelfSerializerConfig::schema_requirement(),
            Self::Json(config) => config.schema_requirement(),
            Self::Logfmt => LogfmtSerializerConfig.schema_requirement(),
            Self::Native => NativeSerializerConfig.schema_requirement(),
            Self::NativeJson => NativeJsonSerializerConfig.schema_requirement(),
            Self::Parquet(config) => config.schema_requirement(), // Added Parquet
            Self::Protobuf(config) => config.schema_requirement(),
            Self::RawMessage => RawMessageSerializerConfig.schema_requirement(),
            Self::Text(config) => config.schema_requirement(),
        }
    }
}

/// Serialize structured events as bytes.
#[derive(Debug, Clone)]
pub enum StreamingSerializer {
    /// Uses an `AvroSerializer` for serialization.
    Avro(AvroSerializer),
    /// Uses a `CefSerializer` for serialization.
    Cef(CefSerializer),
    /// Uses a `CsvSerializer` for serialization.
    Csv(CsvSerializer),
    /// Uses a `GelfSerializer` for serialization.
    Gelf(GelfSerializer),
    /// Uses a `JsonSerializer` for serialization.
    Json(JsonSerializer),
    /// Uses a `LogfmtSerializer` for serialization.
    Logfmt(LogfmtSerializer),
    /// Uses a `NativeSerializer` for serialization.
    Native(NativeSerializer),
    /// Uses a `NativeJsonSerializer` for serialization.
    NativeJson(NativeJsonSerializer),
    /// Uses a `ProtobufSerializer` for serialization.
    Protobuf(ProtobufSerializer),
    /// Uses a `RawMessageSerializer` for serialization.
    RawMessage(RawMessageSerializer),
    /// Uses a `TextSerializer` for serialization.
    Text(TextSerializer),
}

impl tokio_util::codec::Encoder<Event> for StreamingSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Self::Avro(s) => s.encode(event, buffer),
            Self::Cef(s) => s.encode(event, buffer),
            Self::Csv(s) => s.encode(event, buffer),
            Self::Gelf(s) => s.encode(event, buffer),
            Self::Json(s) => s.encode(event, buffer),
            Self::Logfmt(s) => s.encode(event, buffer),
            Self::Native(s) => s.encode(event, buffer),
            Self::NativeJson(s) => s.encode(event, buffer),
            Self::Protobuf(s) => s.encode(event, buffer),
            Self::RawMessage(s) => s.encode(event, buffer),
            Self::Text(s) => s.encode(event, buffer),
        }
    }
}

impl StreamingSerializer {
    /// Check if the serializer supports encoding an event to JSON via `Serializer::to_json_value`.
    pub fn supports_json(&self) -> bool {
        match self {
            StreamingSerializer::Json(_)
            | StreamingSerializer::NativeJson(_)
            | StreamingSerializer::Gelf(_) => true,
            StreamingSerializer::Avro(_)
            | StreamingSerializer::Cef(_)
            | StreamingSerializer::Csv(_)
            | StreamingSerializer::Logfmt(_)
            | StreamingSerializer::Text(_)
            | StreamingSerializer::Native(_)
            | StreamingSerializer::Protobuf(_)
            | StreamingSerializer::RawMessage(_) => false,
        }
    }

    /// Encode event and represent it as JSON value.
    ///
    /// # Panics
    ///
    /// Panics if the serializer does not support encoding to JSON. Call `Serializer::supports_json`
    /// if you need to determine the capability to encode to JSON at runtime.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        match self {
            StreamingSerializer::Gelf(serializer) => serializer.to_json_value(event),
            StreamingSerializer::Json(serializer) => serializer.to_json_value(event),
            StreamingSerializer::NativeJson(serializer) => serializer.to_json_value(event),
            StreamingSerializer::Avro(_)
            | StreamingSerializer::Cef(_)
            | StreamingSerializer::Csv(_)
            | StreamingSerializer::Logfmt(_)
            | StreamingSerializer::Text(_)
            | StreamingSerializer::Native(_)
            | StreamingSerializer::Protobuf(_)
            | StreamingSerializer::RawMessage(_) => {
                panic!("Serializer does not support JSON")
            }
        }
    }
}

impl From<AvroSerializer> for StreamingSerializer {
    fn from(serializer: AvroSerializer) -> Self {
        Self::Avro(serializer)
    }
}

impl From<CefSerializer> for StreamingSerializer {
    fn from(serializer: CefSerializer) -> Self {
        Self::Cef(serializer)
    }
}

impl From<CsvSerializer> for StreamingSerializer {
    fn from(serializer: CsvSerializer) -> Self {
        Self::Csv(serializer)
    }
}

impl From<GelfSerializer> for StreamingSerializer {
    fn from(serializer: GelfSerializer) -> Self {
        Self::Gelf(serializer)
    }
}

impl From<JsonSerializer> for StreamingSerializer {
    fn from(serializer: JsonSerializer) -> Self {
        Self::Json(serializer)
    }
}

impl From<LogfmtSerializer> for StreamingSerializer {
    fn from(serializer: LogfmtSerializer) -> Self {
        Self::Logfmt(serializer)
    }
}

impl From<NativeSerializer> for StreamingSerializer {
    fn from(serializer: NativeSerializer) -> Self {
        Self::Native(serializer)
    }
}

impl From<NativeJsonSerializer> for StreamingSerializer {
    fn from(serializer: NativeJsonSerializer) -> Self {
        Self::NativeJson(serializer)
    }
}

impl From<ProtobufSerializer> for StreamingSerializer {
    fn from(serializer: ProtobufSerializer) -> Self {
        Self::Protobuf(serializer)
    }
}

impl From<RawMessageSerializer> for StreamingSerializer {
    fn from(serializer: RawMessageSerializer) -> Self {
        Self::RawMessage(serializer)
    }
}

impl From<TextSerializer> for StreamingSerializer {
    fn from(serializer: TextSerializer) -> Self {
        Self::Text(serializer)
    }
}

pub trait BatchEncoder {
    type Error;

    fn encode_batch(&mut self, events: &[Event], buffer: &mut BytesMut) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub enum BatchSerializer {
    /// Uses a `ParquetSerializer` for serialization.
    Parquet(ParquetSerializer),
}

impl BatchEncoder for BatchSerializer {
    type Error = vector_common::Error;

    fn encode_batch(&mut self, events: &[Event], buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Self::Parquet(s) => s.encode_batch(events, buffer),
        }
    }
}

impl From<ParquetSerializer> for BatchSerializer {
    fn from(serializer: ParquetSerializer) -> Self {
        Self::Parquet(serializer)
    }
}
