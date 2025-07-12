use arrow::{
    array::{Array, ArrayRef, BinaryArray, StructArray},
    error::Result,
};
use arrow_schema::{ArrowError, DataType};

pub fn variant_from_struct_array(
    input: &ArrayRef,
) -> Result<(&StructArray, &BinaryArray, &BinaryArray)> {
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

    let metadata_array = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ArrowError::CastError("Expected BinaryArray for 'metadata'".into()))?;

    let value_array = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ArrowError::CastError("Expected BinaryArray for 'value'".into()))?;

    return Ok((struct_array, metadata_array, value_array));
}
