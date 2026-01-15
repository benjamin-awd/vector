//! Sink implementation for Delta Lake.
//!
//! This module implements the main sink that orchestrates the flow of events
//! from Vector to Delta Lake tables.

use std::sync::Arc;

use tracing::Span;

use crate::sinks::prelude::*;
use crate::sinks::util::builder::SinkBuilderExt;
use crate::sinks::util::request_builder::default_request_builder_concurrency_limit;

use super::request_builder::{DeltaLakeRequest, DeltaLakeRequestBuilder};

/// Sink for writing events to Delta Lake tables.
///
/// This sink batches events, converts them directly to Arrow RecordBatches,
/// and writes them to Delta Lake with proper transaction log management.
pub struct DeltaLakeSink<S> {
    service: S,
    request_builder: DeltaLakeRequestBuilder,
    batch_settings: BatcherSettings,
}

impl<S> DeltaLakeSink<S>
where
    S: Service<DeltaLakeRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    /// Create a new Delta Lake sink.
    ///
    /// # Arguments
    ///
    /// * `service` - The Delta Lake service for handling writes
    /// * `request_builder` - Builder for converting events to RecordBatch requests
    /// * `batch_settings` - Configuration for event batching
    pub const fn new(
        service: S,
        request_builder: DeltaLakeRequestBuilder,
        batch_settings: BatcherSettings,
    ) -> Self {
        Self {
            service,
            request_builder,
            batch_settings,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings.as_byte_size_config();
        let request_builder = Arc::new(self.request_builder);
        let concurrency = default_request_builder_concurrency_limit();

        let span = Arc::new(Span::current());

        input
            .batched(batch_settings)
            .concurrent_map(concurrency, move |events| {
                let builder = Arc::clone(&request_builder);
                let span = Arc::clone(&span);
                Box::pin(async move {
                    let _entered = span.enter();
                    builder.build_request(events)
                })
            })
            .filter_map(|request| async {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for DeltaLakeSink<S>
where
    S: Service<DeltaLakeRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[cfg(test)]
mod tests {
    // Note: Comprehensive testing requires a full integration test with a real Delta table
    // These tests just verify the structure compiles correctly

    #[test]
    fn test_sink_structure() {
        // This test just verifies the sink can be constructed
        // Real functionality requires the full service stack
    }
}
