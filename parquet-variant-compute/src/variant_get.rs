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
use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef},
    compute::CastOptions,
    error::Result,
};
use arrow_schema::{ArrowError, Field};
use parquet_variant::VariantPath;

use crate::{VariantArray, VariantArrayBuilder};

/// Returns an array with the specified path extracted from the variant values.
///
/// The return array type depends on the `as_type` field of the options parameter
/// 1. `as_type: None`: a VariantArray is returned. The values in this new VariantArray will point
///    to the specified path.
/// 2. `as_type: Some(<specific field>)`: an array of the specified type is returned.
pub fn variant_get(input: &ArrayRef, options: GetOptions) -> Result<ArrayRef> {
    let variant_array: &VariantArray = input.as_any().downcast_ref().ok_or_else(|| {
        ArrowError::InvalidArgumentError(
            "expected a VariantArray as the input for variant_get".to_owned(),
        )
    })?;

    if let Some(as_type) = options.as_type {
        return Err(ArrowError::NotYetImplemented(format!(
            "getting a {as_type} from a VariantArray is not implemented yet",
        )));
    }

    let mut builder = VariantArrayBuilder::new(variant_array.len());
    for i in 0..variant_array.len() {
        let new_variant = variant_array.value(i);
        // TODO: perf?
        let new_variant = new_variant.get_path(&options.path);
        match new_variant {
            // TODO: we're decoding the value and doing a copy into a variant value again. This
            // copy can be much smarter.
            Some(new_variant) => builder.append_variant(new_variant),
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.build()))
}

/// Controls the action of the variant_get kernel.
#[derive(Debug, Clone)]
pub struct GetOptions<'a> {
    /// What path to extract
    pub path: VariantPath<'a>,
    /// if `as_type` is None, the returned array will itself be a VariantArray.
    ///
    /// if `as_type` is `Some(type)` the field is returned as the specified type.
    pub as_type: Option<Field>,
    /// Controls the casting behavior (e.g. error vs substituting null on cast error).
    pub cast_options: CastOptions<'a>,
}

impl<'a> GetOptions<'a> {
    /// Construct options to get the specified path as a variant.
    pub fn new_with_path(path: VariantPath<'a>) -> Self {
        Self {
            path,
            as_type: None,
            cast_options: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, StringArray};

    use crate::batch_json_string_to_variant;
    use crate::VariantArray;

    use super::{variant_get, GetOptions};

    fn single_variant_get_test(
        input_json: &str,
        path: parquet_variant::VariantPath,
        expected_json: &str,
    ) {
        // Create input array from JSON string
        let input_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(input_json)]));
        let input_variant_array_ref: ArrayRef =
            Arc::new(batch_json_string_to_variant(&input_array_ref).unwrap());

        let result =
            variant_get(&input_variant_array_ref, GetOptions::new_with_path(path)).unwrap();

        // Create expected array from JSON string
        let expected_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(expected_json)]));
        let expected_variant_array = batch_json_string_to_variant(&expected_array_ref).unwrap();

        let result_array: &VariantArray = result.as_any().downcast_ref().unwrap();
        assert_eq!(
            result_array.len(),
            1,
            "Expected result array to have length 1"
        );
        assert!(
            result_array.nulls().is_none(),
            "Expected no nulls in result array"
        );
        let result_variant = result_array.value(0);
        let expected_variant = expected_variant_array.value(0);
        assert_eq!(
            result_variant, expected_variant,
            "Result variant does not match expected variant"
        );
    }

    #[test]
    fn get_primitive_variant_field() {
        single_variant_get_test(
            r#"{"some_field": 1234}"#,
            parquet_variant::VariantPath::from("some_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_list_index() {
        single_variant_get_test(
            "[1234, 5678]",
            parquet_variant::VariantPath::from(0),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_inside_object_of_object() {
        single_variant_get_test(
            r#"{"top_level_field": {"inner_field": 1234}}"#,
            parquet_variant::VariantPath::from("top_level_field").join("inner_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_inside_list_of_object() {
        single_variant_get_test(
            r#"[{"some_field": 1234}]"#,
            parquet_variant::VariantPath::from(0).join("some_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_inside_object_of_list() {
        single_variant_get_test(
            r#"{"some_field": [1234]}"#,
            parquet_variant::VariantPath::from("some_field").join(0),
            "1234",
        );
    }

    #[test]
    fn get_complex_variant() {
        single_variant_get_test(
            r#"{"top_level_field": {"inner_field": 1234}}"#,
            parquet_variant::VariantPath::from("top_level_field"),
            r#"{"inner_field": 1234}"#,
        );
    }
}
