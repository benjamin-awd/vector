// In src/codecs/parquet.rs

use crate::encoding::BatchEncoder;
use bytes::BufMut;
use bytes::BytesMut;
use parquet::{
    data_type::{ByteArray, ByteArrayType},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::sync::Arc;
use vector_config_macros::configurable_component;
use vector_core::{config::DataType, event::Event, schema};

/// Config for building a `ParquetSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct ParquetSerializerConfig {
    // Configuration options for Parquet, such as schema definition
}

impl ParquetSerializerConfig {
    pub fn build(&self) -> Result<ParquetSerializer, vector_common::Error> {
        // Logic to build the serializer from the config
        Ok(ParquetSerializer::new())
    }

    pub fn input_type(&self) -> DataType {
        DataType::all_bits()
    }

    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

#[derive(Debug, Clone)]
pub struct ParquetSerializer {}

impl ParquetSerializer {
    pub fn new() -> Self {
        Self {
            // Initialize Parquet options here
        }
    }
}

impl Default for ParquetSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchEncoder for ParquetSerializer {
    type Error = vector_common::Error;

    fn encode_batch(&mut self, events: &[Event], buffer: &mut BytesMut) -> Result<(), Self::Error> {
        // 1. Define a Parquet schema.
        // In a real-world scenario, this would be more dynamic or configurable.
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY message (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type)?);
        let props = Arc::new(WriterProperties::builder().build());

        // 2. Create a Parquet writer that writes to an in-memory buffer.
        let mut writer = SerializedFileWriter::new(buffer.writer(), schema.clone(), props)?;
        let mut row_group_writer = writer.next_row_group()?;

        // 3. Write events to the row group.
        if !events.is_empty() {
            // Get the writer for the next column. Since our schema has only one
            // column, we can safely access it here.
            if let Some(mut col_writer) = row_group_writer.next_column()? {
                // Get a typed writer for the `BYTE_ARRAY` column to work with string data.
                let typed_writer = col_writer.typed::<ByteArrayType>();

                // Convert each event's message into Parquet's `ByteArray` format.
                let values: Vec<ByteArray> = events
                    .iter()
                    .map(|event| {
                        let message_bytes: &[u8] = event
                            .maybe_as_log()
                            .and_then(|log| log.get("message"))
                            .and_then(|value| value.as_bytes())
                            .map(|b| b.as_ref())
                            .unwrap_or(&[]);

                        ByteArray::from(message_bytes)
                    })
                    .collect();

                // Write the batch of values. `None` is used for definition and
                // repetition levels because the field is `REQUIRED`.
                typed_writer.write_batch(&values, None, None)?;

                // It's important to close the column writer to finalize its data.
                col_writer.close()?;
            }
        }

        // 4. Close the row group and writer to finalize the Parquet data.
        row_group_writer.close()?;
        writer.close()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import everything from the parent module
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    use vector_core::event::{Event, LogEvent};

    #[test]
    fn it_encodes_a_batch_of_events() {
        // 1. ARRANGE: Set up the test
        let mut serializer = ParquetSerializer::new();
        let mut buffer = BytesMut::new();

        let event1 = Event::Log(LogEvent::from("hello world"));
        let event2 = Event::Log(LogEvent::from("testing parquet"));
        let event3 = Event::Log(LogEvent::from("final event"));

        // Create a batch of sample events
        let events = vec![event1, event2, event3];

        // 2. ACT: Run the code we want to test
        serializer
            .encode_batch(&events, &mut buffer)
            .expect("Encoding failed");

        // 3. ASSERT: Verify the output is correct
        assert!(
            !buffer.is_empty(),
            "Buffer should not be empty after encoding"
        );

        // Use a Parquet reader to parse the bytes we just created
        let buffer_bytes = buffer.freeze(); // Convert BytesMut -> Bytes for the reader
        let reader = SerializedFileReader::new(buffer_bytes).expect("Failed to create reader");

        // Get an iterator over the rows in the Parquet data
        let mut row_iter = reader
            .get_row_iter(None)
            .expect("Failed to get row iterator");

        // Check each row against the original event data
        assert_eq!(
            row_iter.next().unwrap().unwrap().get_string(0).unwrap(),
            "hello world"
        );
        assert_eq!(
            row_iter.next().unwrap().unwrap().get_string(0).unwrap(),
            "testing parquet"
        );
        assert_eq!(
            row_iter.next().unwrap().unwrap().get_string(0).unwrap(),
            "final event"
        );

        // Ensure there are no more rows
        assert!(row_iter.next().is_none());
    }
}
