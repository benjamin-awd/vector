//! Checkpoint persistence for Delta Lake CDF source.
//!
//! The checkpointer stores the next version to read from, allowing the source
//! to resume from where it left off after restarts.

use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const CHECKPOINT_FILE_NAME: &str = "delta_cdf_checkpoint.json";

/// Checkpoint data persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaCdfCheckpoint {
    /// The next version to read from.
    next_version: i64,
    /// Table URI for validation (ensure checkpoint matches current table).
    table_uri: String,
    /// Timestamp of last checkpoint update.
    updated_at: DateTime<Utc>,
}

/// Manages checkpoint persistence for the Delta Lake CDF source.
#[derive(Debug)]
pub struct DeltaLakeCdfCheckpointer {
    checkpoint_path: PathBuf,
    table_uri: String,
}

impl DeltaLakeCdfCheckpointer {
    /// Create a new checkpointer for the given data directory and table URI.
    pub fn new(data_dir: &Path, table_uri: &str) -> Self {
        Self {
            checkpoint_path: data_dir.join(CHECKPOINT_FILE_NAME),
            table_uri: table_uri.to_string(),
        }
    }

    /// Read the checkpoint from disk.
    ///
    /// Returns `None` if the checkpoint file doesn't exist or is invalid.
    /// Returns `Some(version)` with the next version to read from.
    pub fn read_checkpoint(&self) -> Option<i64> {
        let content = match std::fs::read_to_string(&self.checkpoint_path) {
            Ok(content) => content,
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(
                        message = "Failed to read checkpoint file",
                        path = ?self.checkpoint_path,
                        error = %e,
                    );
                }
                return None;
            }
        };

        let checkpoint: DeltaCdfCheckpoint = match serde_json::from_str(&content) {
            Ok(cp) => cp,
            Err(e) => {
                warn!(
                    message = "Failed to parse checkpoint file",
                    path = ?self.checkpoint_path,
                    error = %e,
                );
                return None;
            }
        };

        // Validate table URI matches
        if checkpoint.table_uri != self.table_uri {
            warn!(
                message = "Checkpoint table URI mismatch, ignoring checkpoint",
                checkpoint_uri = %checkpoint.table_uri,
                current_uri = %self.table_uri,
            );
            return None;
        }

        debug!(
            message = "Loaded checkpoint",
            next_version = checkpoint.next_version,
            updated_at = %checkpoint.updated_at,
        );

        Some(checkpoint.next_version)
    }

    /// Write the checkpoint to disk.
    ///
    /// Uses atomic write (write to temp file, then rename) to prevent corruption.
    pub fn write_checkpoint(&self, next_version: i64) -> io::Result<()> {
        let checkpoint = DeltaCdfCheckpoint {
            next_version,
            table_uri: self.table_uri.clone(),
            updated_at: Utc::now(),
        };

        let content = serde_json::to_string_pretty(&checkpoint)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Write to temp file first for atomic operation
        let tmp_path = self.checkpoint_path.with_extension("tmp");

        // Ensure parent directory exists
        if let Some(parent) = self.checkpoint_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&tmp_path, content)?;

        // Atomic rename
        std::fs::rename(&tmp_path, &self.checkpoint_path)?;

        debug!(
            message = "Checkpoint saved",
            next_version = next_version,
            path = ?self.checkpoint_path,
        );

        Ok(())
    }

    /// Get the checkpoint file path (for testing/debugging).
    #[cfg(test)]
    pub fn checkpoint_path(&self) -> &Path {
        &self.checkpoint_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let checkpointer =
            DeltaLakeCdfCheckpointer::new(temp_dir.path(), "s3://bucket/table");

        // Initially no checkpoint
        assert!(checkpointer.read_checkpoint().is_none());

        // Write checkpoint
        checkpointer.write_checkpoint(42).unwrap();

        // Read it back
        assert_eq!(checkpointer.read_checkpoint(), Some(42));

        // Update checkpoint
        checkpointer.write_checkpoint(100).unwrap();
        assert_eq!(checkpointer.read_checkpoint(), Some(100));
    }

    #[test]
    fn test_checkpoint_uri_mismatch() {
        let temp_dir = TempDir::new().unwrap();

        // Write checkpoint for one table
        let checkpointer1 =
            DeltaLakeCdfCheckpointer::new(temp_dir.path(), "s3://bucket/table1");
        checkpointer1.write_checkpoint(42).unwrap();

        // Try to read with different table URI
        let checkpointer2 =
            DeltaLakeCdfCheckpointer::new(temp_dir.path(), "s3://bucket/table2");
        assert!(checkpointer2.read_checkpoint().is_none());
    }

    #[test]
    fn test_checkpoint_file_format() {
        let temp_dir = TempDir::new().unwrap();
        let checkpointer =
            DeltaLakeCdfCheckpointer::new(temp_dir.path(), "s3://bucket/table");

        checkpointer.write_checkpoint(42).unwrap();

        // Verify the file content is valid JSON
        let content = std::fs::read_to_string(checkpointer.checkpoint_path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["next_version"], 42);
        assert_eq!(parsed["table_uri"], "s3://bucket/table");
        assert!(parsed["updated_at"].is_string());
    }

    #[test]
    fn test_checkpoint_corrupt_file() {
        let temp_dir = TempDir::new().unwrap();
        let checkpointer =
            DeltaLakeCdfCheckpointer::new(temp_dir.path(), "s3://bucket/table");

        // Write corrupt data
        std::fs::write(checkpointer.checkpoint_path(), "not valid json").unwrap();

        // Should return None for corrupt file
        assert!(checkpointer.read_checkpoint().is_none());
    }

    #[test]
    fn test_checkpoint_missing_dir() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("nested").join("dir");
        let checkpointer =
            DeltaLakeCdfCheckpointer::new(&nested_path, "s3://bucket/table");

        // Should create parent directories
        checkpointer.write_checkpoint(42).unwrap();
        assert_eq!(checkpointer.read_checkpoint(), Some(42));
    }
}
