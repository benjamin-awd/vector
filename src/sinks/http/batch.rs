//! Batch settings for the `http` sink.

use vector_lib::codecs::encoding::{Codec, StreamingSerializer};
use vector_lib::stream::batcher::limiter::ItemBatchSize;
use vector_lib::{event::Event, ByteSizeOf, EstimatedJsonEncodedSizeOf};

/// Uses the configured encoder to determine batch sizing.
#[derive(Clone)]
pub(super) struct HttpBatchSizer {
    pub(super) codec: Codec,
}

impl ItemBatchSize<Event> for HttpBatchSizer {
    fn size(&self, item: &Event) -> usize {
        match &self.codec {
            Codec::Stream(serializer, _) => {
                match **serializer {
                    // For JSON-like formats, use the more accurate estimated size.
                    StreamingSerializer::Json(_) | StreamingSerializer::NativeJson(_) => {
                        item.estimated_json_encoded_size_of().get()
                    }
                    // For all other streaming formats, use the default size.
                    _ => item.size_of(),
                }
            }
            Codec::Batch(_) => {
                unimplemented!("Batch codecs are not supported yet for the HTTP sink.")
            }
        }
    }
}
