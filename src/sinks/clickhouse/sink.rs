//! Implementation of the `clickhouse` sink.

use futures_util::Stream;

use super::{config::Format, request_builder::ClickhouseRequestBuilder};
use crate::{
    event::EventArray,
    sinks::{prelude::*, util::http::HttpRequest},
};

#[cfg(not(feature = "columnar"))]
use crate::event::into_event_stream;
#[cfg(feature = "columnar")]
use crate::{
    event::{EventContainer, LogBatch, LogRepr},
    sinks::util::Compressor,
};

pub struct ClickhouseSink<S> {
    batch_settings: BatcherSettings,
    service: S,
    database: ConfinedTemplate,
    table: ConfinedTemplate,
    format: Format,
    request_builder: ClickhouseRequestBuilder,
}

impl<S> ClickhouseSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    pub const fn new(
        batch_settings: BatcherSettings,
        service: S,
        database: ConfinedTemplate,
        table: ConfinedTemplate,
        format: Format,
        request_builder: ClickhouseRequestBuilder,
    ) -> Self {
        Self {
            batch_settings,
            service,
            database,
            table,
            format,
            request_builder,
        }
    }

    /// Build the row-oriented request stream: the historical path that flattens events, batches
    /// them by `(database, table)`, and encodes each batch via the configured encoder.
    fn row_request_stream<'a>(
        row_events: impl Stream<Item = Event> + Send + 'a,
        database: ConfinedTemplate,
        table: ConfinedTemplate,
        format: Format,
        batch_settings: BatcherSettings,
        request_builder: ClickhouseRequestBuilder,
    ) -> impl Stream<Item = HttpRequest<PartitionKey>> + Send + 'a {
        row_events
            .batched_partitioned(
                KeyPartitioner::new(database, table, format),
                batch_settings.timeout,
                move |_| batch_settings.as_byte_size_config(),
            )
            .filter_map(|(key, batch)| async move { key.map(move |k| (k, batch)) })
            .request_builder(default_request_builder_concurrency_limit(), request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
    }

    /// The row-only path used when the `columnar` feature is disabled: flatten every `EventArray`
    /// into events and run the historical pipeline. Behaviorally identical to the pre-columnar sink.
    #[cfg(not(feature = "columnar"))]
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let Self {
            batch_settings,
            service,
            database,
            table,
            format,
            request_builder,
        } = *self;

        Self::row_request_stream(
            input.flat_map(into_event_stream),
            database,
            table,
            format,
            batch_settings,
            request_builder,
        )
        .into_driver(service)
        .run()
        .await
    }

    /// The columnar-aware path. A `LogRepr::Columns` batch is encoded straight to Arrow IPC and
    /// sent un-flattened; everything else flows through the historical row pipeline. Both lanes feed
    /// a single driver so ordering, concurrency, and retries are shared.
    #[cfg(feature = "columnar")]
    async fn run_inner(self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        use futures::SinkExt;

        let Self {
            batch_settings,
            service,
            database,
            table,
            format,
            request_builder,
        } = *self;
        let compression = request_builder.compression;

        // Router splits the input: pre-built columnar requests on one lane, flattened row events on
        // the other. Bounded channels apply backpressure to the source.
        let (mut row_tx, row_rx) = futures::channel::mpsc::channel::<Event>(1024);
        let (mut col_tx, col_rx) = futures::channel::mpsc::channel::<HttpRequest<PartitionKey>>(64);

        let router = {
            let database = database.clone();
            let table = table.clone();
            async move {
                while let Some(events) = input.next().await {
                    match events {
                        EventArray::Logs(log_batch)
                            if matches!(log_batch.repr(), LogRepr::Columns { .. }) =>
                        {
                            if let Some(request) = build_columnar_request(
                                log_batch,
                                &database,
                                &table,
                                format,
                                compression,
                            ) && col_tx.send(request).await.is_err()
                            {
                                break;
                            }
                        }
                        other => {
                            for event in other.into_events() {
                                if row_tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        };

        let row_requests = Self::row_request_stream(
            row_rx,
            database,
            table,
            format,
            batch_settings,
            request_builder,
        );

        let driver = futures::stream::select(col_rx, row_requests)
            .into_driver(service)
            .run();

        let (_, result) = futures::join!(router, driver);
        result
    }
}

/// Encode a columnar [`LogBatch`] into a ClickHouse `ArrowStream` HTTP request without ever
/// materializing per-event rows: the `RecordBatch` is written straight to Arrow IPC, compressed,
/// and wrapped. Returns `None` if the batch is not columnar or encoding fails.
#[cfg(feature = "columnar")]
fn build_columnar_request(
    mut log_batch: LogBatch,
    database: &ConfinedTemplate,
    table: &ConfinedTemplate,
    format: Format,
    compression: Compression,
) -> Option<HttpRequest<PartitionKey>> {
    use std::io::Write;

    // Finalizers live in the batch metadata; take them before consuming the batch.
    let finalizers = log_batch.take_finalizers();
    let (record_batch, _meta) = log_batch.into_record_batch().ok()?;

    // ArrowStream requires static database/table templates, so a default event renders the literal.
    let default_event = Event::Log(LogEvent::default());
    let database = KeyPartitioner::render(database, &default_event, "database_key")?;
    let table = KeyPartitioner::render(table, &default_event, "table_key")?;

    let ipc = match vector_lib::codecs::encoding::encode_record_batch(&record_batch) {
        Ok(ipc) => ipc,
        Err(error) => {
            emit!(SinkRequestBuildError { error });
            return None;
        }
    };
    let request_encoded_size = ipc.len();

    let mut compressor = Compressor::from(compression);
    compressor.write_all(&ipc).ok()?;
    let payload = compressor.finish().ok()?.freeze();
    let request_wire_size = payload.len();

    let event_count = record_batch.num_rows();
    let events_byte_size = record_batch.get_array_memory_size();
    let json_size =
        GroupedCountByteSize::from(CountByteSize(event_count, JsonSize::new(events_byte_size)));
    let request_metadata = RequestMetadata::new(
        event_count,
        events_byte_size,
        request_encoded_size,
        request_wire_size,
        json_size,
    );

    Some(HttpRequest::new(
        payload,
        finalizers,
        request_metadata,
        PartitionKey {
            database,
            table,
            format,
        },
    ))
}

#[async_trait::async_trait]
impl<S> StreamSink<EventArray> for ClickhouseSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub database: String,
    pub table: String,
    pub format: Format,
}

/// KeyPartitioner that partitions events by (database, table) pair.
struct KeyPartitioner {
    database: ConfinedTemplate,
    table: ConfinedTemplate,
    format: Format,
}

impl KeyPartitioner {
    const fn new(database: ConfinedTemplate, table: ConfinedTemplate, format: Format) -> Self {
        Self {
            database,
            table,
            format,
        }
    }

    fn render(template: &ConfinedTemplate, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                });
            })
            .ok()
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<PartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database_key")?;
        let table = Self::render(&self.table, item, "table_key")?;
        Some(PartitionKey {
            database,
            table,
            format: self.format,
        })
    }
}
