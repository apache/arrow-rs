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

//! Module for transforming a batch of Variants into JSON strings.

use crate::VariantArray;
use arrow::array::{ArrayRef, BooleanBufferBuilder, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::ArrowError;
use parquet_variant_json::VariantToJson;

/// Transform a [`VariantArray`] to a batch of JSON strings where nulls are preserved.
pub fn variant_to_json(input: &ArrayRef) -> Result<StringArray, ArrowError> {
    let array = VariantArray::try_new(input)?;

    // Zero-copy builder
    // The size per JSON string is assumed to be 128 bytes. If this holds true, resizing could be
    // minimized for performance.
    let mut json_buffer: Vec<u8> = Vec::with_capacity(array.len() * 128);
    let mut offsets: Vec<i32> = Vec::with_capacity(array.len() + 1);
    let mut validity = BooleanBufferBuilder::new(array.len());
    let mut current_offset: i32 = 0;
    offsets.push(current_offset);

    for i in 0..array.len() {
        if array.is_null(i) {
            validity.append(false);
            offsets.push(current_offset);
        } else {
            let variant = array.try_value(i)?;
            let start_len = json_buffer.len();
            variant.to_json(&mut json_buffer)?;
            let written = (json_buffer.len() - start_len) as i32;
            current_offset += written;
            offsets.push(current_offset);
            validity.append(true);
        }
    }

    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let value_buffer = Buffer::from_vec(json_buffer);
    let null_buffer = NullBuffer::new(validity.finish());

    StringArray::try_new(offsets_buffer, value_buffer, Some(null_buffer))
}

#[cfg(test)]
mod test {
    use crate::variant_to_json;
    use arrow::array::{Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow_schema::Fields;
    use std::sync::Arc;

    #[test]
    fn test_variant_to_json() {
        let mut metadata_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();

        // Row 0: [1, 0, 0], [12, 0]
        metadata_builder.append_value([1, 0, 0]);
        value_builder.append_value([12, 0]);

        // Row 1: null
        metadata_builder.append_null();
        value_builder.append_null();

        // Row 2: [1, 1, 0, 1, 97], [2, 1, 0, 0, 1, 32]
        metadata_builder.append_value([1, 1, 0, 1, 97]);
        value_builder.append_value([2, 1, 0, 0, 2, 12, 32]);

        // Row 3: [1, 0, 0], [0]
        metadata_builder.append_value([1, 0, 0]);
        value_builder.append_value([0]);

        // Row 4: null
        metadata_builder.append_null();
        value_builder.append_null();

        let metadata_array = Arc::new(metadata_builder.finish()) as ArrayRef;
        let value_array = Arc::new(value_builder.finish()) as ArrayRef;

        let fields: Fields = vec![
            Field::new("metadata", DataType::Binary, true),
            Field::new("value", DataType::Binary, true),
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
            vec![metadata_array.clone(), value_array.clone()],
            Some(null_buffer), // Null bitmap (let Arrow infer from children)
        );

        let input = Arc::new(struct_array) as ArrayRef;

        let result = variant_to_json(&input).unwrap();

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
