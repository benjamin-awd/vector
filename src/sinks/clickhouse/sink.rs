//! Implementation of the `clickhouse` sink.

use crate::sinks::prelude::*;
use clickhouse::{Client, Row};
use serde::Serialize;
use futures::stream::BoxStream;


// TODO: figure a way to automatically create Clickhouse schema
#[derive(Debug, Clone, Row, Serialize)]
struct MessageRow {
    message: String,
}

pub struct ClickhouseSink<S> {
    batch_settings: BatcherSettings,
    client: Client,
    database: Template,
    table: Template,
}

impl <S> ClickhouseSink<S> {
    pub fn new(
        batch_settings: BatcherSettings,
        client: Client,
        database: Template,
        table: Template,
    ) -> Self {
        Self {
            batch_settings,
            client,
            database,
            table,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        input
            .batched_partitioned(
                KeyPartitioner::new(self.database, self.table),
                || batch_settings.as_byte_size_config(),
            )
            .filter_map(|(key, batch)| async move { key.map(move |k| (k, batch)) })
            .for_each_concurrent(25, move |(key, batch)| {
                let client = self.client.clone();

                // The get_finalizers() doesn't exist, need another way to move forward
                // let finalizers = events.get_finalizers();

                async move {
                    // Convert Vector events into our strongly-typed MessageRow struct
                    let rows: Vec<MessageRow> = events
                        .into_iter()
                        .filter_map(|event| {
                            let log = event.as_log();
                            // This is a simple conversion; a real implementation
                            // would have more robust error handling.
                            Some(MessageRow {
                                message: log.get_field("message").map(|v| v.to_string_lossy()).unwrap_or_default(),
                            })
                        })
                        .collect();

                    if rows.is_empty() {
                        return;
                    }

                    // Use the client to insert the rows
                    let table_name = format!("`{}`.`{}`", key.database, key.table);
                    let mut insert = match client.insert(&table_name) {
                        Ok(i) => i,
                        Err(error) => {
                            emit!(SinkRequestBuildError {
                                error,
                                finalizers,
                                ..Default::default()
                            });
                            return;
                        }
                    };

                    match insert.write_all(&rows).await {
                        Ok(_) => {
                            if let Err(error) = insert.end().await {
                                emit!(SinkRequestBuildError { error, finalizers, ..Default::default() });
                            }
                        }
                        Err(error) => {
                             emit!(SinkRequestBuildError { error, finalizers, ..Default::default() });
                        }
                    };
                }
            })
            .await;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for ClickhouseSink<S> {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub database: String,
    pub table: String,
}

/// KeyPartitioner that partitions events by (database, table) pair.
struct KeyPartitioner {
    database: Template,
    table: Template,
}

impl KeyPartitioner {
    const fn new(database: Template, table: Template) -> Self {
        Self {
            database,
            table,
        }
    }

    fn render(template: &Template, item: &Event, field: &'static str) -> Option<String> {
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
        Some(PartitionKey { database, table })
    }
}
