use crate::internal_events::{EncoderFramingError, EncoderSerializeError};
use bytes::BytesMut;
use tokio_util::codec::Encoder as TokioEncoder;
use vector_lib::codecs::encoding::{Codec, Error}; // Make sure your `use` path is correct
use vector_lib::event::Event;

#[derive(Debug, Clone)]
/// An encoder that provides a streaming `tokio_util::codec::Encoder` interface
/// for stream-based codecs.
pub struct Encoder {
    codec: Codec,
}

impl Encoder {
    /// Creates a new `Encoder` from a `Codec`.
    pub fn new(codec: Codec) -> Self {
        Self { codec }
    }
}

/// This implementation allows the `Encoder` to be used with streaming utilities
/// like `FramedWrite`.
impl TokioEncoder<Event> for Encoder {
    type Error = Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        // We match on the codec to ensure we only try to encode with a streaming codec.
        match &mut self.codec {
            Codec::Stream(serializer, framer) => {
                // This logic is the same as before, but it's now safely
                // scoped to only run for streaming codecs.
                let len = buffer.len();
                let mut payload = buffer.split_off(len);

                serializer.encode(event, &mut payload).map_err(|error| {
                    emit!(EncoderSerializeError { error: &error });
                    Error::SerializingError(error)
                })?;

                framer.encode((), &mut payload).map_err(|error| {
                    emit!(EncoderFramingError { error: &error });
                    Error::FramingError(error)
                })?;

                buffer.unsplit(payload);
                Ok(())
            }
            Codec::Batch(_) => {
                // This encoder is for streaming sinks, so if it's given a batch codec,
                // it's a programming error. We panic to fail fast.
                panic!("Attempted to use a batch-only codec in a streaming sink context.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::FramedWrite;
    use vector_lib::codecs::encoding::{
        BoxedFramingError, Codec, Framer, SerializerConfig, StreamingSerializer,
        TextSerializerConfig,
    };
    use vector_lib::event::LogEvent;

    use super::*;

    // No changes are needed for your test helper structs.
    #[derive(Debug, Clone)]
    struct ParenEncoder;

    impl ParenEncoder {
        pub(super) const fn new() -> Self {
            Self
        }
    }

    impl tokio_util::codec::Encoder for ParenEncoder {
        type Error = BoxedFramingError;

        fn encode(&mut self, _: (), dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.reserve(2);
            let inner = dst.split();
            dst.put_u8(b'(');
            dst.unsplit(inner);
            dst.put_u8(b')');
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct ErrorNthEncoder<T>(T, usize, usize)
    where
        T: tokio_util::codec::Encoder<(), Error = BoxedFramingError>;

    impl<T> ErrorNthEncoder<T>
    where
        T: tokio_util::codec::Encoder<(), Error = BoxedFramingError>,
    {
        pub(super) const fn new(encoder: T, n: usize) -> Self {
            Self(encoder, 0, n)
        }
    }

    impl<T> tokio_util::codec::Encoder for ErrorNthEncoder<T>
    where
        T: tokio_util::codec::Encoder<(), Error = BoxedFramingError>,
    {
        type Error = BoxedFramingError;

        fn encode(&mut self, _: (), dst: &mut BytesMut) -> Result<(), Self::Error> {
            self.0.encode((), dst)?;
            let result = if self.1 == self.2 {
                Err(Box::new(std::io::Error::other("error")) as _)
            } else {
                Ok(())
            };
            self.1 += 1;
            result
        }
    }

    #[tokio::test]
    async fn test_encode_events_sink_empty() {
        let serializer = StreamingSerializer::Text(TextSerializerConfig::default().build());
        let framer = Framer::Boxed(Box::new(ParenEncoder::new()));
        let codec = Codec::Stream(serializer.into(), framer);
        let encoder = Encoder::new(codec);

        let source = futures::stream::iter(vec![
            Event::Log(LogEvent::from("foo")),
            Event::Log(LogEvent::from("bar")),
            Event::Log(LogEvent::from("baz")),
        ])
        .map(Ok);
        let sink = Vec::new();
        let mut framed = FramedWrite::new(sink, encoder);
        source.forward(&mut framed).await.unwrap();
        let sink = framed.into_inner();
        assert_eq!(sink, b"(foo)(bar)(baz)");
    }

    #[tokio::test]
    async fn test_encode_events_sink_non_empty() {
        let serializer = StreamingSerializer::Text(TextSerializerConfig::default().build());
        let framer = Framer::Boxed(Box::new(ParenEncoder::new()));
        let codec = Codec::Stream(serializer.into(), framer);
        let encoder = Encoder::new(codec);

        let source = futures::stream::iter(vec![
            Event::Log(LogEvent::from("bar")),
            Event::Log(LogEvent::from("baz")),
            Event::Log(LogEvent::from("bat")),
        ])
        .map(Ok);
        let sink = Vec::from("(foo)");
        let mut framed = FramedWrite::new(sink, encoder);
        source.forward(&mut framed).await.unwrap();
        let sink = framed.into_inner();
        assert_eq!(sink, b"(foo)(bar)(baz)(bat)");
    }

    #[tokio::test]
    async fn test_encode_events_sink_empty_handle_framing_error() {
        let serializer = StreamingSerializer::Text(TextSerializerConfig::default().build());
        let framer = Framer::Boxed(Box::new(ErrorNthEncoder::new(ParenEncoder::new(), 1)));
        let codec = Codec::Stream(serializer.into(), framer);
        let encoder = Encoder::new(codec);

        let source = futures::stream::iter(vec![
            Event::Log(LogEvent::from("foo")),
            Event::Log(LogEvent::from("bar")),
            Event::Log(LogEvent::from("baz")),
        ])
        .map(Ok);
        let sink = Vec::new();
        let mut framed = FramedWrite::new(sink, encoder);
        assert!(source.forward(&mut framed).await.is_err());
        framed.flush().await.unwrap();
        let sink = framed.into_inner();
        assert_eq!(sink, b"(foo)");
    }

    #[tokio::test]
    async fn test_encode_events_sink_non_empty_handle_framing_error() {
        let serializer = StreamingSerializer::Text(TextSerializerConfig::default().build());
        let framer = Framer::Boxed(Box::new(ErrorNthEncoder::new(ParenEncoder::new(), 1)));
        let codec = Codec::Stream(serializer.into(), framer);
        let encoder = Encoder::new(codec);

        let source = futures::stream::iter(vec![
            Event::Log(LogEvent::from("bar")),
            Event::Log(LogEvent::from("baz")),
            Event::Log(LogEvent::from("bat")),
        ])
        .map(Ok);
        let sink = Vec::from("(foo)");
        let mut framed = FramedWrite::new(sink, encoder);
        assert!(source.forward(&mut framed).await.is_err());
        framed.flush().await.unwrap();
        let sink = framed.into_inner();
        assert_eq!(sink, b"(foo)(bar)");
    }

    #[tokio::test]
    async fn test_encode_batch_newline() {
        // Here we build the codec directly from the config, which is a more
        // common use case for standard (non-test) framers.
        let config = SerializerConfig::Text(TextSerializerConfig::default());
        let codec = config.build().unwrap();
        let encoder = Encoder::new(codec);

        let source = futures::stream::iter(vec![
            Event::Log(LogEvent::from("bar")),
            Event::Log(LogEvent::from("baz")),
            Event::Log(LogEvent::from("bat")),
        ])
        .map(Ok);
        let sink: Vec<u8> = Vec::new();
        let mut framed = FramedWrite::new(sink, encoder);
        source.forward(&mut framed).await.unwrap();
        let sink = framed.into_inner();
        assert_eq!(sink, b"bar\nbaz\nbat\n");
    }
}
