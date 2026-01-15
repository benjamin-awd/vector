mod checkpoint;
mod config;
mod event;
mod source;

#[cfg(all(test, feature = "delta-lake-cdf-integration-tests"))]
mod integration_tests;

pub use config::DeltaLakeCdfConfig;

use crate::sources::Source;
