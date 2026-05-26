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
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::ArrowError;
use parquet_variant_json::JsonToVariant;

/// Macro to convert string array to variant array
macro_rules! string_array_to_variant {
    ($input:expr, $array:expr, $builder:expr) => {{
        for i in 0..$input.len() {
            if $input.is_null(i) {
                $builder.append_null();
            } else {
                $builder.append_json($array.value(i))?;
            }
        }
    }};
}

/// Parse a batch of JSON strings into a batch of Variants represented as
/// STRUCT<metadata: BINARY, value: BINARY> where nulls are preserved. The JSON strings in the input
/// must be valid.
///
/// Supports the following string array types:
/// - [`StringArray`]
/// - [`LargeStringArray`]
/// - [`StringViewArray`]
pub fn json_to_variant(input: &ArrayRef) -> Result<VariantArray, ArrowError> {
    let mut variant_array_builder = VariantArrayBuilder::new(input.len());

    // Try each string array type in sequence
    if let Some(string_array) = input.as_any().downcast_ref::<StringArray>() {
        string_array_to_variant!(input, string_array, variant_array_builder);
    } else if let Some(large_string_array) = input.as_any().downcast_ref::<LargeStringArray>() {
        string_array_to_variant!(input, large_string_array, variant_array_builder);
    } else if let Some(string_view_array) = input.as_any().downcast_ref::<StringViewArray>() {
        string_array_to_variant!(input, string_view_array, variant_array_builder);
    } else {
        return Err(ArrowError::CastError(
            "Expected reference to StringArray, LargeStringArray, or StringViewArray as input"
                .into(),
        ));
    }

    Ok(variant_array_builder.build())
}

#[cfg(test)]
mod test {
    use crate::json_to_variant;
    use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use arrow_schema::ArrowError;
    use parquet_variant::{Variant, VariantBuilder};
    use std::sync::Arc;

    #[test]
    fn test_json_to_variant() -> Result<(), ArrowError> {
        let input = StringArray::from(vec![
            Some("1"),
            None,
            Some("{\"a\": 32}"),
            Some("null"),
            None,
        ]);
        let array_ref: ArrayRef = Arc::new(input);
        let variant_array = json_to_variant(&array_ref).unwrap();

        let metadata_array = variant_array.metadata_field();
        let value_array = variant_array.value_field().expect("value field");

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
            ob.finish();
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

    #[test]
    fn test_json_to_variant_large_string() -> Result<(), ArrowError> {
        let input = LargeStringArray::from(vec![
            Some("1"),
            None,
            Some("{\"a\": 32}"),
            Some("null"),
            None,
        ]);
        let array_ref: ArrayRef = Arc::new(input);
        let variant_array = json_to_variant(&array_ref).unwrap();

        let metadata_array = variant_array.metadata_field();
        let value_array = variant_array.value_field().expect("value field");

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
            ob.finish();
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

    #[test]
    fn test_json_to_variant_string_view() -> Result<(), ArrowError> {
        let input = StringViewArray::from(vec![
            Some("1"),
            None,
            Some("{\"a\": 32}"),
            Some("null"),
            None,
        ]);
        let array_ref: ArrayRef = Arc::new(input);
        let variant_array = json_to_variant(&array_ref).unwrap();

        let metadata_array = variant_array.metadata_field();
        let value_array = variant_array.value_field().expect("value field");

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
            ob.finish();
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
