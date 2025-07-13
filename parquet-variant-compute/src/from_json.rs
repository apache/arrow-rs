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

use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Fields};
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

    let metadata_field = Field::new("metadata", DataType::BinaryView, false);
    let value_field = Field::new("value", DataType::BinaryView, false);

    let schema = Fields::from(vec![metadata_field, value_field]);

    let mut variant_array_builder =
        VariantArrayBuilder::try_new(input_string_array.len(), schema).unwrap();

    for i in 0..input.len() {
        if input.is_null(i) {
            // The subfields are expected to be non-nullable according to the parquet variant spec.
            variant_array_builder.append_null();
        } else {
            let mut vb = variant_array_builder.variant_builder();
            // parse JSON directly to the variant builder
            json_to_variant(input_string_array.value(i), &mut vb)?;
            vb.finish()
        }
    }
    Ok(variant_array_builder.build())
}

#[cfg(test)]
mod test {
    use crate::batch_json_string_to_variant;
    use arrow::array::{Array, ArrayRef, AsArray, StringArray};
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
        let variant_array = batch_json_string_to_variant(&array_ref).unwrap();

        let metadata_array = variant_array.metadata_field().as_binary_view();
        let value_array = variant_array.value_field().as_binary_view();

        // Compare row 0
        assert!(!variant_array.is_null(0));
        assert_eq!(variant_array.value(0), Variant::Int8(1));

        // Compare row 1
        assert!(variant_array.is_null(1));

        // Compare row 2
        assert!(!variant_array.is_null(2));
        {
            let mut vb = VariantBuilder::new();
            let mut ob = vb.new_object();
            ob.insert("a", Variant::Int8(32));
            ob.finish()?;
            let (object_metadata, object_value) = vb.finish();
            let expected = Variant::new(&object_metadata, &object_value);
            assert_eq!(variant_array.value(2), expected);
        }

        // Compare row 3 (Note this is a variant NULL, not a null row)
        assert!(!variant_array.is_null(3));
        assert_eq!(variant_array.value(3), Variant::Null);

        // Compare row 4
        assert!(variant_array.is_null(4));

        // Ensure that the subfields are not nullable
        assert!(!metadata_array.is_null(1));
        assert!(!value_array.is_null(1));
        assert!(!metadata_array.is_null(4));
        assert!(!value_array.is_null(4));
        Ok(())
    }
}
