#![allow(unused_imports)]
use futures_util::StreamExt;
use opendal::{services::Gcs, Operator};

use crate::{
    codecs::{Decoder, DecodingConfig},
    config::SourceContext,
    event::{BatchNotifier, BatchStatus, Event, MaybeAsLogMut, Value},
    gcp::{GcpAuthConfig, GcpAuthenticator, Scope, PUBSUB_URL},
    internal_events::{
        GcpPubsubConnectError, GcpPubsubReceiveError, GcpPubsubStreamingPullError,
        StreamClosedError,
    },
    serde::{bool_or_struct, default_decoding, default_framing_message_based},
    shutdown::ShutdownSignal,
    sources::util,
    tls::{TlsConfig, TlsSettings},
    SourceSender,
};
use opendal::Error;
use opendal::layers::{LoggingLayer, RetryLayer};
use crate::sources::gcp::config::GcsConfig;

#[derive(Clone)]
pub struct GcsSource {
    config: GcsConfig,
    operator: Operator,
}

impl GcsSource {
    pub fn new(config: GcsConfig) -> Self {
        let mut builder = Gcs::default().bucket(&config.bucket);

        // OpenDAL will automatically use Application Default Credentials.
        // You can add logic here to use other auth methods from the config if needed.

        let operator = Operator::new(builder)
            .expect("Failed to build GCS operator")
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();

        Self { config, operator }
    }
}

pub async fn run(
    config: GcsConfig,
    decoder: Decoder,
    cx: SourceContext,
) -> Result<(), ()> {
    let source = GcsSource::new(config);
    let prefix = source.config.prefix.as_deref().unwrap_or("");
    let out = cx.out;

    let mut lister = match source.operator.lister(prefix).await {
        Ok(lister) => lister,
        Err(err) => {
            error!(message = "Failed to create lister for GCS bucket", %err);
            return Err(());
        }
    };

    while let Some(entry_result) = lister.next().await {
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                error!(message = "Failed to get next entry from GCS", %err);
                continue; // Skip to the next entry
            }
        };

        let path = entry.path();
        info!(message = "Reading object.", path);

        let content = match source.operator.read(path).await {
            Ok(content) => content,
            Err(err) => {
                error!(message = "Failed to read object.", path, %err);
                continue;
            }
        };
        // ... process content ...
    }

    info!("Finished processing all objects in GCS bucket.");
    Ok(())
}
