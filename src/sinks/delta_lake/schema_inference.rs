//! Schema inference for Delta Lake sink.
//!
//! This module provides functionality to infer Arrow schema from Vector events,
//! merging discovered fields with an existing table schema.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};

use crate::sinks::prelude::*;

/// Infers an Arrow DataType from a Vector Value.
///
/// Returns `None` for null values since type cannot be determined.
pub fn infer_type_from_value(value: &Value) -> Option<DataType> {
    match value {
        Value::Integer(_) => Some(DataType::Int64),
        Value::Float(_) => Some(DataType::Float64),
        Value::Boolean(_) => Some(DataType::Boolean),
        Value::Timestamp(_) => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        Value::Bytes(_) => Some(DataType::Utf8),
        Value::Object(_) => Some(DataType::Utf8), // Serialize as JSON
        Value::Array(_) => Some(DataType::Utf8),  // Serialize as JSON
        Value::Regex(_) => Some(DataType::Utf8),  // Serialize as string
        Value::Null => None,                      // Cannot infer type from null
    }
}

/// Discovers fields from a batch of events.
///
/// Returns a HashMap of field_name -> DataType for all fields found
/// across all events in the batch. First non-null value determines the type.
pub fn discover_fields_from_events(events: &[Event]) -> HashMap<String, DataType> {
    let mut discovered: HashMap<String, DataType> = HashMap::new();

    for event in events {
        let Event::Log(log) = event else {
            continue;
        };

        let Some(fields) = log.all_event_fields() else {
            continue;
        };

        for (key, value) in fields {
            // Skip if we've already discovered this field
            if discovered.contains_key(key.as_ref()) {
                continue;
            }

            if let Some(data_type) = infer_type_from_value(value) {
                discovered.insert(key.to_string(), data_type);
            }
        }
    }

    discovered
}

/// Merges discovered fields with an existing schema.
///
/// - Fields in base_schema are preserved with their original types
/// - New fields are added as nullable (since existing rows won't have them)
/// - Returns a new schema containing both base and discovered fields
pub fn merge_schema_with_discovered(
    base_schema: &Schema,
    discovered: &HashMap<String, DataType>,
) -> Schema {
    let mut fields: Vec<FieldRef> = base_schema.fields().iter().cloned().collect();
    let existing_names: std::collections::HashSet<String> =
        fields.iter().map(|f| f.name().clone()).collect();

    for (name, data_type) in discovered {
        if !existing_names.contains(name) {
            // New fields are always nullable
            fields.push(Field::new(name, data_type.clone(), true).into());
        }
    }

    Schema::new_with_metadata(fields, base_schema.metadata().clone())
}

/// Builds a merged schema from base schema and events.
///
/// This is the main entry point for schema inference.
pub fn build_inferred_schema(base_schema: &Schema, events: &[Event]) -> SchemaRef {
    let discovered = discover_fields_from_events(events);
    let merged = merge_schema_with_discovered(base_schema, &discovered);
    SchemaRef::new(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vrl::value::ObjectMap;

    #[test]
    fn test_infer_type_from_value() {
        assert_eq!(
            infer_type_from_value(&Value::Integer(42)),
            Some(DataType::Int64)
        );
        assert_eq!(
            infer_type_from_value(&Value::Float(ordered_float::NotNan::new(3.15).unwrap())),
            Some(DataType::Float64)
        );
        assert_eq!(
            infer_type_from_value(&Value::Boolean(true)),
            Some(DataType::Boolean)
        );
        assert_eq!(
            infer_type_from_value(&Value::Bytes("hello".into())),
            Some(DataType::Utf8)
        );
        assert_eq!(infer_type_from_value(&Value::Null), None);
    }

    #[test]
    fn test_discover_fields_from_events() {
        let mut log = LogEvent::default();
        log.insert("foo", 42i64);
        log.insert("bar", "hello");
        let events = vec![Event::Log(log)];

        let discovered = discover_fields_from_events(&events);

        assert_eq!(discovered.get("foo"), Some(&DataType::Int64));
        assert_eq!(discovered.get("bar"), Some(&DataType::Utf8));
    }

    #[test]
    fn test_discover_fields_first_type_wins() {
        // First event has foo as integer
        let mut log1 = LogEvent::default();
        log1.insert("foo", 42i64);

        // Second event has foo as string (should be ignored)
        let mut log2 = LogEvent::default();
        log2.insert("foo", "string_value");

        let events = vec![Event::Log(log1), Event::Log(log2)];
        let discovered = discover_fields_from_events(&events);

        // First type (Int64) should win
        assert_eq!(discovered.get("foo"), Some(&DataType::Int64));
    }

    #[test]
    fn test_merge_schema_with_discovered() {
        let base = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let mut discovered = HashMap::new();
        discovered.insert("foo".to_string(), DataType::Boolean);
        discovered.insert("id".to_string(), DataType::Int64); // Duplicate, should be ignored

        let merged = merge_schema_with_discovered(&base, &discovered);

        assert_eq!(merged.fields().len(), 3);
        // Base fields preserved
        assert_eq!(merged.field(0).name(), "id");
        assert!(!merged.field(0).is_nullable());
        // New field added as nullable
        assert!(merged.field_with_name("foo").unwrap().is_nullable());
    }

    #[test]
    fn test_build_inferred_schema() {
        let base = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let mut log = LogEvent::default();
        log.insert("id", 1i64);
        log.insert("new_field", "value");
        let events = vec![Event::Log(log)];

        let merged = build_inferred_schema(&base, &events);

        assert_eq!(merged.fields().len(), 2);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("new_field").is_ok());
        assert!(merged.field_with_name("new_field").unwrap().is_nullable());
    }

    #[test]
    fn test_nested_fields_are_flattened() {
        // Test that nested object fields are flattened
        let mut log = LogEvent::default();
        let mut obj = ObjectMap::new();
        obj.insert("nested".into(), Value::Integer(42));
        log.insert("obj_field", Value::Object(obj));

        let events = vec![Event::Log(log)];
        let discovered = discover_fields_from_events(&events);

        // Flattened field should be Int64
        assert_eq!(discovered.get("obj_field.nested"), Some(&DataType::Int64));
        // The parent object key itself is not in the flattened output
        assert_eq!(discovered.get("obj_field"), None);
    }
}
