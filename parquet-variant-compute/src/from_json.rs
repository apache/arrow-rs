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

use crate::variant_array::VariantArray;
use crate::variant_array_builder::VariantArrayBuilder;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow_schema::ArrowError;
use parquet_variant::VariantBuilder;
use parquet_variant_json::json_to_variant;

/// Parse a batch of JSON strings into a batch of Variants represented as
/// STRUCT<metadata: BINARY, value: BINARY> where nulls are preserved. The JSON strings in the input
/// must be valid.
pub fn batch_json_string_to_variant(input: &ArrayRef) -> Result<VariantArray, ArrowError> {
    let input_string_array = match input.as_any().downcast_ref::<StringArray>() {
        Some(string_array) => Ok(string_array),
        None => Err(ArrowError::CastError(
            "Expected reference to StringArray as input".into(),
        )),
    }?;

    let mut variant_array_builder = VariantArrayBuilder::new(input_string_array.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            // The subfields are expected to be non-nullable according to the parquet variant spec.
            variant_array_builder.append_null();
        } else {
            let mut vb = VariantBuilder::new();
            json_to_variant(input_string_array.value(i), &mut vb)?;
            let (metadata, value) = vb.finish();
            variant_array_builder.append_variant_buffers(&metadata, &value);
        }
    }
    Ok(variant_array_builder.finish())
}

#[cfg(test)]
mod test {
    use crate::batch_json_string_to_variant;
    use arrow::array::{Array, ArrayRef, AsArray, StringArray};
    use arrow_schema::ArrowError;
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
        let variant_array = batch_json_string_to_variant(&array_ref).unwrap();

        let metadata_array = variant_array.metadata_field();
        let value_array = variant_array.value_field();

        // Compare row 0
        assert!(!variant_array.is_null(0));
        assert_eq!(variant_array.value(0).as_int8(), Some(1));

        // Compare row 1
        assert!(variant_array.is_null(1));

        // Compare row 2
        assert!(!variant_array.is_null(2));
        {
            let variant = variant_array.value(2);
            let obj = variant.as_object().unwrap();
            assert_eq!(obj.len(), 1);
            assert_eq!(obj.get("a").unwrap().as_int8(), Some(32));
        }

        // Compare row 3 (Note this is a variant NULL, not a null row)
        assert!(!variant_array.is_null(3));
        assert_eq!(variant_array.value(3).as_null(), Some(()));

        // Compare row 4
        assert!(variant_array.is_null(4));

        // Ensure that the subfields are non-nullable but have 0-length entries for null rows
        assert!(!metadata_array.is_null(1));
        assert!(!value_array.is_null(1));
        assert!(!metadata_array.is_null(4));
        assert!(!value_array.is_null(4));

        // Null rows should have 0-length metadata and value
        assert_eq!(metadata_array.as_binary_view().value(1).len(), 0);
        assert_eq!(value_array.as_binary_view().value(1).len(), 0);
        assert_eq!(metadata_array.as_binary_view().value(4).len(), 0);
        assert_eq!(value_array.as_binary_view().value(4).len(), 0);
        Ok(())
    }
}
