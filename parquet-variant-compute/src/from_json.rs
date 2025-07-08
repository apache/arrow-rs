// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module for transforming a batch of JSON strings into a batch of Variants represented as
//! STRUCT<metadata: BINARY, value: BINARY>

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BooleanBufferBuilder, StringArray, StructArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_schema::ArrowError;
use parquet_variant::{json_to_variant, VariantBuilder};

fn variant_arrow_repr() -> DataType {
    let metadata_field = Field::new("metadata", DataType::Binary, true);
    let value_field = Field::new("value", DataType::Binary, true);
    let fields = vec![metadata_field, value_field];
    DataType::Struct(fields.into())
}

/// Parse a batch of JSON strings into a batch of Variants represented as
/// STRUCT<metadata: BINARY, value: BINARY> where nulls are preserved. The JSON strings in the input
/// must be valid.
pub fn batch_json_string_to_variant(input: &ArrayRef) -> Result<StructArray, ArrowError> {
    let input_string_array = match input.as_any().downcast_ref::<StringArray>() {
        Some(string_array) => Ok(string_array),
        None => Err(ArrowError::CastError(
            "Expected reference to StringArray as input".into(),
        )),
    }?;

    // Zero-copy builders
    let mut metadata_buffer: Vec<u8> = Vec::with_capacity(input.len() * 128);
    let mut metadata_offsets: Vec<i32> = Vec::with_capacity(input.len() + 1);
    let mut metadata_validity = BooleanBufferBuilder::new(input.len());
    let mut metadata_current_offset: i32 = 0;
    metadata_offsets.push(metadata_current_offset);

    let mut value_buffer: Vec<u8> = Vec::with_capacity(input.len() * 128);
    let mut value_offsets: Vec<i32> = Vec::with_capacity(input.len() + 1);
    let mut value_validity = BooleanBufferBuilder::new(input.len());
    let mut value_current_offset: i32 = 0;
    value_offsets.push(value_current_offset);

    let mut validity = BooleanBufferBuilder::new(input.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            metadata_validity.append(false);
            value_validity.append(false);
            metadata_offsets.push(metadata_current_offset);
            value_offsets.push(value_current_offset);
            validity.append(false);
        } else {
            let mut vb = VariantBuilder::new();
            json_to_variant(input_string_array.value(i), &mut vb)?;
            let (metadata, value) = vb.finish();
            validity.append(true);

            metadata_current_offset += metadata.len() as i32;
            metadata_buffer.extend(metadata);
            metadata_offsets.push(metadata_current_offset);
            metadata_validity.append(true);

            value_current_offset += value.len() as i32;
            value_buffer.extend(value);
            value_offsets.push(value_current_offset);
            value_validity.append(true);
            println!("{value_current_offset} {metadata_current_offset}");
        }
    }
    let metadata_offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(metadata_offsets));
    let metadata_data_buffer = Buffer::from_vec(metadata_buffer);
    let metadata_null_buffer = NullBuffer::new(metadata_validity.finish());

    let value_offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(value_offsets));
    let value_data_buffer = Buffer::from_vec(value_buffer);
    let value_null_buffer = NullBuffer::new(value_validity.finish());

    let metadata_array = BinaryArray::new(
        metadata_offsets_buffer,
        metadata_data_buffer,
        Some(metadata_null_buffer),
    );
    let value_array = BinaryArray::new(
        value_offsets_buffer,
        value_data_buffer,
        Some(value_null_buffer),
    );

    let struct_fields: Vec<ArrayRef> = vec![Arc::new(metadata_array), Arc::new(value_array)];
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
