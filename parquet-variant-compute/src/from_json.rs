use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, StringArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use arrow_schema::ArrowError;
use parquet_variant::{json_to_variant, VariantBuilder};

fn variant_arrow_repr() -> DataType {
    let metadata_field = Field::new("metadata", DataType::Binary, true);
    let value_field = Field::new("value", DataType::Binary, true);
    let fields = vec![metadata_field, value_field];
    DataType::Struct(fields.into())
}

pub fn batch_json_string_to_variant(input: &ArrayRef) -> Result<StructArray, ArrowError> {
    let input_string_array = match input.as_any().downcast_ref::<StringArray>() {
        Some(string_array) => Ok(string_array),
        None => Err(ArrowError::CastError(
            "Expected reference to StringArray as input".into(),
        )),
    }?;

    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = BinaryBuilder::new();
    let mut validity = BooleanBufferBuilder::new(input.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            metadata_builder.append_null();
            value_builder.append_null();
            validity.append(false);
        } else {
            let mut vb = VariantBuilder::new();
            json_to_variant(input_string_array.value(i), &mut vb)?;
            let (metadata, value) = vb.finish();
            metadata_builder.append_value(&metadata);
            value_builder.append_value(&value);
            validity.append(true);
        }
    }
    let struct_fields: Vec<ArrayRef> = vec![
        Arc::new(metadata_builder.finish()),
        Arc::new(value_builder.finish()),
    ];
    let variant_fields = match variant_arrow_repr() {
        DataType::Struct(fields) => fields,
        _ => unreachable!("variant_arrow_repr is hard-coded and must match the expected schema"),
    };
    let null_buffer = NullBuffer::new(validity.finish());
    Ok(StructArray::new(
        variant_fields,
        struct_fields,
        Some(null_buffer),
    ))
}

#[cfg(test)]
mod test {
    use crate::batch_json_string_to_variant;
    use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
    use arrow_schema::ArrowError;
    use parquet_variant::{Variant, VariantBuilder};
    use std::sync::Arc;

    #[test]
    fn test_batch_json_string_to_variant() -> Result<(), ArrowError> {
        let input = StringArray::from(vec![
            Some("1"),
            None,
            Some("{\"a\": 32}"),
            Some("null"),
            None,
        ]);
        let array_ref: ArrayRef = Arc::new(input);
        let output = batch_json_string_to_variant(&array_ref).unwrap();

        let struct_array = &output;
        let metadata_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let value_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(struct_array.is_null(0), false);
        assert_eq!(struct_array.is_null(1), true);
        assert_eq!(struct_array.is_null(2), false);
        assert_eq!(struct_array.is_null(3), false);
        assert_eq!(struct_array.is_null(4), true);

        assert_eq!(metadata_array.value(0), &[1, 0, 0]);
        assert_eq!(value_array.value(0), &[12, 1]);

        {
            let mut vb = VariantBuilder::new();
            let mut ob = vb.new_object();
            ob.insert("a", Variant::Int8(32));
            ob.finish()?;
            let (object_metadata, object_value) = vb.finish();
            assert_eq!(metadata_array.value(2), &object_metadata);
            assert_eq!(value_array.value(2), &object_value);
        }

        assert_eq!(metadata_array.value(3), &[1, 0, 0]);
        assert_eq!(value_array.value(3), &[0]);

        assert!(metadata_array.is_null(1));
        assert!(value_array.is_null(1));
        assert!(metadata_array.is_null(4));
        assert!(value_array.is_null(4));
        Ok(())
    }
}
