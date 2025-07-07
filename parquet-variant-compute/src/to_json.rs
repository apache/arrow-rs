use arrow::array::{Array, ArrayRef, BinaryArray, StringArray, StringBuilder, StructArray};
use arrow::datatypes::DataType;
use arrow_schema::ArrowError;
use parquet_variant::{variant_to_json_string, Variant};

pub fn batch_variant_to_json_string(input: &ArrayRef) -> Result<StringArray, ArrowError> {
    let struct_array = input
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ArrowError::CastError("Expected StructArray as input".into()))?;

    // Validate field types
    let data_type = struct_array.data_type();
    match data_type {
        DataType::Struct(inner_fields) => {
            if inner_fields.len() != 2
                || inner_fields[0].data_type() != &DataType::Binary
                || inner_fields[1].data_type() != &DataType::Binary
            {
                return Err(ArrowError::CastError(
                    "Expected struct with two binary fields".into(),
                ));
            }
        }
        _ => {
            return Err(ArrowError::CastError(
                "Expected StructArray with known fields".into(),
            ))
        }
    }

    let value_array = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ArrowError::CastError("Expected BinaryArray for 'value'".into()))?;

    let metadata_array = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ArrowError::CastError("Expected BinaryArray for 'metadata'".into()))?;

    let mut builder = StringBuilder::new();

    for i in 0..struct_array.len() {
        if struct_array.is_null(i) {
            builder.append_null();
        } else {
            let metadata = metadata_array.value(i);
            let value = value_array.value(i);
            let variant = Variant::new(metadata, value);
            let json_string = variant_to_json_string(&variant)?;
            builder.append_value(&json_string);
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use crate::batch_variant_to_json_string;
    use arrow::array::{Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow_schema::Fields;
    use std::sync::Arc;

    #[test]
    fn test_batch_variant_to_json_string() {
        let mut metadata_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();

        // Row 0: [1, 0, 0], [12, 0]
        metadata_builder.append_value(&[1, 0, 0]);
        value_builder.append_value(&[12, 0]);

        // Row 1: null
        metadata_builder.append_null();
        value_builder.append_null();

        // Row 2: [1, 1, 0, 1, 97], [2, 1, 0, 0, 1, 32]
        metadata_builder.append_value(&[1, 1, 0, 1, 97]);
        value_builder.append_value(&[2, 1, 0, 0, 2, 12, 32]);

        // Row 3: [1, 0, 0], [0]
        metadata_builder.append_value(&[1, 0, 0]);
        value_builder.append_value(&[0]);

        // Row 4: null
        metadata_builder.append_null();
        value_builder.append_null();

        let metadata_array = Arc::new(metadata_builder.finish()) as ArrayRef;
        let value_array = Arc::new(value_builder.finish()) as ArrayRef;

        let fields: Fields = vec![
            Field::new("value", DataType::Binary, true),
            Field::new("metadata", DataType::Binary, true),
        ]
        .into();

        let mut validity = BooleanBufferBuilder::new(value_array.len());
        for i in 0..value_array.len() {
            let is_valid = value_array.is_valid(i) && metadata_array.is_valid(i);
            validity.append(is_valid);
        }
        let null_buffer = NullBuffer::new(validity.finish());

        let struct_array = StructArray::new(
            fields,
            vec![value_array.clone(), metadata_array.clone()],
            Some(null_buffer), // Null bitmap (let Arrow infer from children)
        );

        let input = Arc::new(struct_array) as ArrayRef;

        let result = batch_variant_to_json_string(&input).unwrap();

        // Expected output: ["0", null, "{\"a\":32}", "null", null]
        let expected = vec![Some("0"), None, Some("{\"a\":32}"), Some("null"), None];

        let result_vec: Vec<Option<&str>> = (0..result.len())
            .map(|i| {
                if result.is_null(i) {
                    None
                } else {
                    Some(result.value(i))
                }
            })
            .collect();

        assert_eq!(result_vec, expected);
    }
}
