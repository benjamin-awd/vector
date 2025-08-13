use std::{io, num::NonZeroUsize};

use bytes::Bytes;
use vector_lib::request_metadata::{GroupedCountByteSize, RequestMetadata};

use super::{encoding::Encoder, metadata::RequestMetadataBuilder, Compression, Compressor};
use bytes::BytesMut;

pub fn default_request_builder_concurrency_limit() -> NonZeroUsize {
    if let Some(limit) = std::env::var("VECTOR_EXPERIMENTAL_REQUEST_BUILDER_CONCURRENCY")
        .map(|value| value.parse::<NonZeroUsize>().ok())
        .ok()
        .flatten()
    {
        return limit;
    }

    crate::app::worker_threads().unwrap_or_else(|| NonZeroUsize::new(8).expect("static"))
}

pub struct EncodeResult<P> {
    pub payload: P,
    pub uncompressed_byte_size: usize,
    pub transformed_json_size: GroupedCountByteSize,
    pub compressed_byte_size: Option<usize>,
}

impl<P> EncodeResult<P>
where
    P: AsRef<[u8]>,
{
    pub fn uncompressed(payload: P, transformed_json_size: GroupedCountByteSize) -> Self {
        let uncompressed_byte_size = payload.as_ref().len();
        Self {
            payload,
            uncompressed_byte_size,
            transformed_json_size,
            compressed_byte_size: None,
        }
    }

    pub fn compressed(
        payload: P,
        uncompressed_byte_size: usize,
        transformed_json_size: GroupedCountByteSize,
    ) -> Self {
        let compressed_byte_size = payload.as_ref().len();
        Self {
            payload,
            uncompressed_byte_size,
            transformed_json_size,
            compressed_byte_size: Some(compressed_byte_size),
        }
    }
}

impl<P> EncodeResult<P> {
    // Can't be `const` because you can't (yet?) run deconstructors in a const context, which is what this function does
    // by dropping the (un)compressed sizes.
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_payload(self) -> P {
        self.payload
    }
}
/// Generalized interface for defining how a batch of events will be turned into a request.
pub trait RequestBuilder<Input> {
    type Metadata;
    type Events: AsRef<[Event]>;
    type Payload: From<Bytes> + AsRef<[u8]>;
    type Request;
    type Error: From<io::Error>;

    /// Gets the compression algorithm used by this request builder.
    fn compression(&self) -> Compression;

    /// Gets the transformer used by this request builder.
    fn transformer(&self) -> &Transformer;

    /// Gets a mutable reference to the codec.
    fn codec(&mut self) -> &mut Codec;

    /// Splits apart the input into the metadata and event portions.
    fn split_input(&self, input: Input) -> (Self::Metadata, RequestMetadataBuilder, Self::Events);

    /// Builds a request for the given metadata and payload.
    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request;

    /// Splits apart the input into the metadata and event portions.
    ///
    /// The metadata should be any information that needs to be passed back to `build_request`
    /// as-is, such as event finalizers, while the events are the actual events to process.
    fn encode_events(
        &mut self,
        events: Self::Events,
    ) -> Result<EncodeResult<Self::Payload>, Self::Error> {
        let events_slice = events.as_ref();

        // 1. Transform events and calculate metrics first.
        let mut transformed_events = Vec::with_capacity(events_slice.len());
        let mut byte_size = telemetry().create_request_count_byte_size();
        for event in events_slice.iter() {
            let mut transformed_event = event.clone();
            self.transformer().transform(&mut transformed_event);
            byte_size.add_event(&transformed_event, transformed_event.estimated_json_encoded_size_of());
            transformed_events.push(transformed_event);
        }

        // 2. Encode the transformed events into a buffer using the Codec.
        let mut buffer = BytesMut::new();
        self.codec()
            .encode_batch(&transformed_events, &mut buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let uncompressed_byte_size = buffer.len();

        // 3. Compress the resulting buffer.
        let mut compressor = Compressor::from(self.compression());
        compressor.write_all(&buffer)?;
        let payload = compressor.into_inner().freeze();

        // 4. Build and return the final result.
        let result = if self.compression().is_compressed() {
            EncodeResult::compressed(payload.into(), uncompressed_byte_size, byte_size)
        } else {
            EncodeResult::uncompressed(payload.into(), byte_size)
        };

        Ok(result)
    }
}

/// Generalized interface for defining how a batch of events will incrementally be turned into requests.
///
/// As opposed to `RequestBuilder`, this trait provides the means to incrementally build requests
/// from a single batch of events, where all events in the batch may not fit into a single request.
/// This can be important for sinks where the underlying service has limitations on the size of a
/// request, or how many events may be present, necessitating a batch be split up into multiple requests.
///
/// While batches can be limited in size before being handed off to a request builder, we can't
/// always know in advance how large the encoded payload will be, which requires us to be able to
/// potentially split a batch into multiple requests.
pub trait IncrementalRequestBuilder<Input> {
    type Metadata;
    type Payload;
    type Request;
    type Error;

    /// Incrementally encodes the given input, potentially generating multiple payloads.
    fn encode_events_incremental(
        &mut self,
        input: Input,
    ) -> Vec<Result<(Self::Metadata, Self::Payload), Self::Error>>;

    /// Builds a request for the given metadata and payload.
    fn build_request(&mut self, metadata: Self::Metadata, payload: Self::Payload) -> Self::Request;
}
