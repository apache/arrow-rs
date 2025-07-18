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
#[derive(Debug, Clone, Default)]
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
    /// Construct default options to get the specified path as a variant.
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct options to get the specified path as a variant.
    pub fn new_with_path(path: VariantPath<'a>) -> Self {
        Self {
            path,
            as_type: None,
            cast_options: Default::default(),
        }
    }

    /// Specify the type to return.
    pub fn with_as_type(mut self, as_type: Option<Field>) -> Self {
        self.as_type = as_type;
        self
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, BinaryViewArray, Int32Array, StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow_schema::{DataType, Field, FieldRef, Fields};
    use parquet_variant::{Variant, VariantPath};

    use crate::VariantArray;
    use crate::{batch_json_string_to_variant, VariantArrayBuilder};

    use super::{variant_get, GetOptions};

    fn single_variant_get_test(input_json: &str, path: VariantPath, expected_json: &str) {
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
            VariantPath::from("some_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_list_index() {
        single_variant_get_test("[1234, 5678]", VariantPath::from(0), "1234");
    }

    #[test]
    fn get_primitive_variant_inside_object_of_object() {
        single_variant_get_test(
            r#"{"top_level_field": {"inner_field": 1234}}"#,
            VariantPath::from("top_level_field").join("inner_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_inside_list_of_object() {
        single_variant_get_test(
            r#"[{"some_field": 1234}]"#,
            VariantPath::from(0).join("some_field"),
            "1234",
        );
    }

    #[test]
    fn get_primitive_variant_inside_object_of_list() {
        single_variant_get_test(
            r#"{"some_field": [1234]}"#,
            VariantPath::from("some_field").join(0),
            "1234",
        );
    }

    #[test]
    fn get_complex_variant() {
        single_variant_get_test(
            r#"{"top_level_field": {"inner_field": 1234}}"#,
            VariantPath::from("top_level_field"),
            r#"{"inner_field": 1234}"#,
        );
    }

    /// Shredding: extract a value as a VariantArray
    #[test]
    fn get_variant_shredded_int32_as_variant() {
        let array = shredded_int32_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result: &VariantArray = result.as_any().downcast_ref().unwrap();
        assert_eq!(result.len(), 4);

        // Expect the values are the same as the original values
        assert_eq!(result.value(0), Variant::Int32(34));
        assert!(!result.is_valid(1));
        assert_eq!(result.value(2), Variant::from("N/A"));
        assert_eq!(result.value(3), Variant::Int32(100));
    }

    /// Shredding: extract a value as an Int32Array
    #[test]
    fn get_variant_shredded_int32_as_int32() {
        // Extract the typed value as Int32Array
        let array = shredded_int32_variant_array();
        let options = GetOptions::new()
            // specify we want the typed value as Int32
            .with_as_type(Some(Field::new("typed_value", DataType::Int32, true)));
        let result = variant_get(&array, options).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(34), None, None, Some(100)]));
        assert_eq!(&result, &expected)
    }

    /// Perfect Shredding: extract the typed value as a VariantArray
    #[test]
    fn get_variant_perfectly_shredded_int32_as_variant() {
        let array = perfectly_shredded_int32_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result: &VariantArray = result.as_any().downcast_ref().unwrap();
        assert_eq!(result.len(), 3);

        // Expect the values are the same as the original values
        assert_eq!(result.value(0), Variant::Int32(1));
        assert_eq!(result.value(1), Variant::Int32(2));
        assert_eq!(result.value(2), Variant::Int32(3));
    }

    /// Shredding: Extract the typed value as Int32Array
    #[test]
    fn get_variant_perfectly_shredded_int32_as_int32() {
        // Extract the typed value as Int32Array
        let array = perfectly_shredded_int32_variant_array();
        let options = GetOptions::new()
            // specify we want the typed value as Int32
            .with_as_type(Some(Field::new("typed_value", DataType::Int32, true)));
        let result = variant_get(&array, options).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]));
        assert_eq!(&result, &expected)
    }

    /// Return a VariantArray that represents a perfectly "shredded" variant
    /// for the following example (3 Variant::Int32 values):
    ///
    /// ```text
    /// 1
    /// 2
    /// 3
    /// ```
    ///
    /// The schema of the corresponding `StructArray` would look like this:
    ///
    /// ```text
    /// StructArray {
    ///   metadata: BinaryViewArray,
    ///   typed_value: Int32Array,
    /// }
    /// ```
    fn perfectly_shredded_int32_variant_array() -> ArrayRef {
        // At the time of writing, the `VariantArrayBuilder` does not support shredding.
        // so we must construct the array manually.  see https://github.com/apache/arrow-rs/issues/7895
        let (metadata, _value) = { parquet_variant::VariantBuilder::new().finish() };

        let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 3));
        let typed_value = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata))
            .with_field("typed_value", Arc::new(typed_value))
            .build();

        Arc::new(
            VariantArray::try_new(Arc::new(struct_array)).expect("should create variant array"),
        )
    }

    /// Return a VariantArray that represents a normal "shredded" variant
    /// for the following example
    ///
    /// Based on the example from [the doc]
    ///
    /// [the doc]: https://docs.google.com/document/d/1pw0AWoMQY3SjD7R4LgbPvMjG_xSCtXp3rZHkVp9jpZ4/edit?tab=t.0
    ///
    /// ```text
    /// 34
    /// null (an Arrow NULL, not a Variant::Null)
    /// "n/a" (a string)
    /// 100
    /// ```
    ///
    /// The schema of the corresponding `StructArray` would look like this:
    ///
    /// ```text
    /// StructArray {
    ///   metadata: BinaryViewArray,
    ///   value: BinaryViewArray,
    ///   typed_value: Int32Array,
    /// }
    /// ```
    fn shredded_int32_variant_array() -> ArrayRef {
        // At the time of writing, the `VariantArrayBuilder` does not support shredding.
        // so we must construct the array manually.  see https://github.com/apache/arrow-rs/issues/7895
        let (metadata, string_value) = {
            let mut builder = parquet_variant::VariantBuilder::new();
            builder.append_value("n/a");
            builder.finish()
        };

        let nulls = NullBuffer::from(vec![
            true,  // row 0 non null
            false, // row 1 is null
            true,  // row 2 non null
            true,  // row 3 non null
        ]);

        // metadata is the same for all rows
        let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 4));

        // See https://docs.google.com/document/d/1pw0AWoMQY3SjD7R4LgbPvMjG_xSCtXp3rZHkVp9jpZ4/edit?disco=AAABml8WQrY
        // about why row1 is an empty but non null, value.
        let values = BinaryViewArray::from(vec![
            None,                // row 0 is shredded, so no value
            Some(b"" as &[u8]),  // row 1 is null, so empty value (why?)
            Some(&string_value), // copy the string value "N/A"
            None,                // row 3 is shredded, so no value
        ]);

        let typed_value = Int32Array::from(vec![
            Some(34),  // row 0 is shredded, so it has a value
            None,      // row 1 is null, so no value
            None,      // row 2 is a string, so no typed value
            Some(100), // row 3 is shredded, so it has a value
        ]);

        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", metadata)
            .with_field("typed_value", Arc::new(typed_value))
            .with_field("value", Arc::new(values))
            .with_nulls(nulls)
            .build();

        Arc::new(
            VariantArray::try_new(Arc::new(struct_array)).expect("should create variant array"),
        )
    }

    /// Builds struct arrays from component fields
    ///
    /// TODO: move to arrow crate
    #[derive(Debug, Default, Clone)]
    struct StructArrayBuilder {
        fields: Vec<FieldRef>,
        arrays: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    }

    impl StructArrayBuilder {
        fn new() -> Self {
            Default::default()
        }

        /// Add an array to this struct array as a field with the specified name.
        fn with_field(mut self, field_name: &str, array: ArrayRef) -> Self {
            let field = Field::new(field_name, array.data_type().clone(), true);
            self.fields.push(Arc::new(field));
            self.arrays.push(array);
            self
        }

        /// Set the null buffer for this struct array.
        fn with_nulls(mut self, nulls: NullBuffer) -> Self {
            self.nulls = Some(nulls);
            self
        }

        pub fn build(self) -> StructArray {
            let Self {
                fields,
                arrays,
                nulls,
            } = self;
            StructArray::new(Fields::from(fields), arrays, nulls)
        }
    }
}
