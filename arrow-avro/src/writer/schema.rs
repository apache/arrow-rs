use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::RecordBatch;
use crate::codec::{AvroDataType, AvroField, Codec};
use crate::schema::Schema;

fn record_batch_to_avro_schema<'a>(
    batch: &'a RecordBatch,
    record_name: &'a str,
    top_level_data_type: &'a AvroDataType,
) -> Schema<'a> {
    top_level_data_type.to_avro_schema(record_name)
}

pub fn to_avro_json_schema(
    batch: &RecordBatch,
    record_name: &str,
) -> Result<String, serde_json::Error> {
    let avro_fields: Vec<AvroField> = batch
        .schema()
        .fields()
        .iter()
        .map(|arrow_field| crate::codec::arrow_field_to_avro_field(arrow_field))
        .collect();
    let top_level_data_type = AvroDataType::from_codec(
        Codec::Struct(Arc::from(avro_fields)),
    );
    let avro_schema = record_batch_to_avro_schema(batch, record_name, &top_level_data_type);
    serde_json::to_string_pretty(&avro_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray, RecordBatch, ArrayRef, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use serde_json::{json, Value};
    use std::sync::Arc;

    #[test]
    fn test_record_batch_to_avro_schema_basic() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let col_id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let col_name = Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col_id, col_name])
            .expect("Failed to create RecordBatch");

        // Convert the batch -> Avro `Schema`
        let avro_schema = to_avro_json_schema(&batch, "MyTestRecord")
            .expect("Failed to convert RecordBatch to Avro JSON schema");
        let actual_json: Value = serde_json::from_str(&avro_schema)
            .expect("Invalid JSON returned by to_avro_json_schema");

        let expected_json = json!({
            "type": "record",
            "name": "MyTestRecord",
            "aliases": [],
            "doc": null,
            "logicalType": null,
            "fields": [
                {
                    "name": "id",
                    "doc": null,
                    "type": "int"
                },
                {
                    "name": "name",
                    "doc": null,
                    "type": ["null", "string"]
                }
            ]
        });

        // Compare the two JSON objects
        assert_eq!(
            actual_json, expected_json,
            "Avro Schema JSON does not match expected"
        );
    }

    #[test]
    fn test_to_avro_json_schema_basic() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("desc", DataType::Utf8, true),
        ]));

        let col_id = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let col_desc = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col_id, col_desc])
            .expect("Failed to create RecordBatch");

        let json_schema_string = to_avro_json_schema(&batch, "AnotherTestRecord")
            .expect("Failed to convert RecordBatch to Avro JSON schema");

        let actual_json: Value = serde_json::from_str(&json_schema_string)
            .expect("Invalid JSON returned by to_avro_json_schema");

        let expected_json = json!({
            "type": "record",
            "name": "AnotherTestRecord",
            "aliases": [],
            "doc": null,
            "logicalType": null,
            "fields": [
                {
                    "name": "id",
                    "type": "int",
                    "doc": null,
                },
                {
                    "name": "desc",
                    "type": ["null", "string"],
                    "doc": null,
                }
            ]
        });

        assert_eq!(
            actual_json, expected_json,
            "JSON schema mismatch for to_avro_json_schema"
        );
    }

    #[test]
    fn test_to_avro_json_schema_single_nonnull_int() {
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let col_id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col_id])
            .expect("Failed to create RecordBatch");

        let avro_json_string = to_avro_json_schema(&batch, "MySingleIntRecord")
            .expect("Failed to generate Avro JSON schema");

        let actual_json: Value = serde_json::from_str(&avro_json_string)
            .expect("Failed to parse Avro JSON schema");

        let expected_json = json!({
            "type": "record",
            "name": "MySingleIntRecord",
            "aliases": [],
            "doc": null,
            "logicalType": null,
            "fields": [
                {
                    "name": "id",
                    "type": "int",
                    "doc": null,
                }
            ]
        });

        // Compare
        assert_eq!(actual_json, expected_json, "Avro JSON schema mismatch");
    }

    #[test]
    fn test_to_avro_json_schema_two_fields_nullable_string() {
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let col_id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let col_name = Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col_id, col_name])
            .expect("Failed to create RecordBatch");

        let avro_json_string = to_avro_json_schema(&batch, "MyRecord")
            .expect("Failed to generate Avro JSON schema");

        let actual_json: Value = serde_json::from_str(&avro_json_string)
            .expect("Failed to parse Avro JSON schema");

        let expected_json = json!({
            "type": "record",
            "name": "MyRecord",
            "aliases": [],
            "doc": null,
            "logicalType": null,
            "fields": [
                {
                    "name": "id",
                    "type": "int",
                    "doc": null,
                },
                {
                    "name": "name",
                    "doc": null,
                    "type": [
                        "null",
                        "string",
                    ]
                }
            ]
        });

        // Compare
        assert_eq!(actual_json, expected_json, "Avro JSON schema mismatch");
    }

    #[test]
    fn test_to_avro_json_schema_nested_struct() {
        let inner_fields = Fields::from(vec![
            Field::new("inner_int", DataType::Int32, false),
            Field::new("inner_str", DataType::Utf8, true),
        ]);

        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("my_struct", DataType::Struct(inner_fields), true)
        ]));

        let inner_int_col = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        let inner_str_col = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;

        let fields_arrays = vec![
            (
                Arc::new(Field::new("inner_int", DataType::Int32, false)),
                inner_int_col,
            ),
            (
                Arc::new(Field::new("inner_str", DataType::Utf8, true)),
                inner_str_col,
            ),
        ];

        let struct_array = StructArray::from(fields_arrays);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(struct_array)],
        )
            .expect("Failed to create RecordBatch");

        let avro_json_string = to_avro_json_schema(&batch, "NestedRecord")
            .expect("Failed to generate Avro JSON schema");

        let actual_json: Value = serde_json::from_str(&avro_json_string)
            .expect("Failed to parse Avro JSON schema");

        let expected_json = json!({
            "type": "record",
            "name": "NestedRecord",
            "aliases": [],
            "doc": null,
            "logicalType": null,
            "fields": [
                {
                    "name": "my_struct",
                    "doc": null,
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "my_struct",
                            "aliases": [],
                            "doc": null,
                            "logicalType": null,
                            "fields": [
                                {
                                    "name": "inner_int",
                                    "type": "int",
                                    "doc": null,
                                },
                                {
                                    "name": "inner_str",
                                    "doc": null,
                                    "type": [
                                        "null",
                                        "string",
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        // Compare
        assert_eq!(actual_json, expected_json, "Avro JSON schema mismatch");
    }
}
