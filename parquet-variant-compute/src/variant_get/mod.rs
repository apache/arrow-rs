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
use arrow::{
    array::{Array, ArrayRef},
    compute::CastOptions,
    error::Result,
};
use arrow_schema::{ArrowError, FieldRef};
use parquet_variant::VariantPath;

use crate::variant_array::ShreddingState;
use crate::variant_get::output::instantiate_output_builder;
use crate::VariantArray;

mod output;

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

    // Create the output writer based on the specified output options
    let output_builder = instantiate_output_builder(options.clone())?;

    // Dispatch based on the shredding state of the input variant array
    match variant_array.shredding_state() {
        ShreddingState::PartiallyShredded {
            metadata,
            value,
            typed_value,
        } => output_builder.partially_shredded(variant_array, metadata, value, typed_value),
        ShreddingState::Typed {
            metadata,
            typed_value,
        } => output_builder.typed(variant_array, metadata, typed_value),
        ShreddingState::Unshredded { metadata, value } => {
            output_builder.unshredded(variant_array, metadata, value)
        }
        ShreddingState::AllNull { metadata } => output_builder.all_null(variant_array, metadata),
    }
}

/// Controls the action of the variant_get kernel.
#[derive(Debug, Clone, Default)]
pub struct GetOptions<'a> {
    /// What path to extract
    pub path: VariantPath<'a>,
    /// if `as_type` is None, the returned array will itself be a VariantArray.
    ///
    /// if `as_type` is `Some(type)` the field is returned as the specified type.
    pub as_type: Option<FieldRef>,
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
    pub fn with_as_type(mut self, as_type: Option<FieldRef>) -> Self {
        self.as_type = as_type;
        self
    }

    /// Specify the cast options to use when casting to the specified type.
    pub fn with_cast_options(mut self, cast_options: CastOptions<'a>) -> Self {
        self.cast_options = cast_options;
        self
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryViewArray, Float16Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray, StructArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::CastOptions;
    use arrow_schema::{DataType, Field, FieldRef, Fields};
    use parquet_variant::{Variant, VariantPath};

    use crate::json_to_variant;
    use crate::VariantArray;

    use super::{variant_get, GetOptions};

    fn single_variant_get_test(input_json: &str, path: VariantPath, expected_json: &str) {
        // Create input array from JSON string
        let input_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(input_json)]));
        let input_variant_array_ref: ArrayRef =
            Arc::new(json_to_variant(&input_array_ref).unwrap());

        let result =
            variant_get(&input_variant_array_ref, GetOptions::new_with_path(path)).unwrap();

        // Create expected array from JSON string
        let expected_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(expected_json)]));
        let expected_variant_array = json_to_variant(&expected_array_ref).unwrap();

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

    /// Partial Shredding: extract a value as a VariantArray
    macro_rules! numeric_partially_shredded_test {
        ($primitive_type:ty, $data_fn:ident) => {
            let array = $data_fn();
            let options = GetOptions::new();
            let result = variant_get(&array, options).unwrap();

            // expect the result is a VariantArray
            let result: &VariantArray = result.as_any().downcast_ref().unwrap();
            assert_eq!(result.len(), 4);

            // Expect the values are the same as the original values
            assert_eq!(
                result.value(0),
                Variant::from(<$primitive_type>::try_from(34u8).unwrap())
            );
            assert!(!result.is_valid(1));
            assert_eq!(result.value(2), Variant::from("n/a"));
            assert_eq!(
                result.value(3),
                Variant::from(<$primitive_type>::try_from(100u8).unwrap())
            );
        };
    }

    #[test]
    fn get_variant_partially_shredded_int8_as_variant() {
        numeric_partially_shredded_test!(i8, partially_shredded_int8_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_int16_as_variant() {
        numeric_partially_shredded_test!(i16, partially_shredded_int16_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_int32_as_variant() {
        numeric_partially_shredded_test!(i32, partially_shredded_int32_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_int64_as_variant() {
        numeric_partially_shredded_test!(i64, partially_shredded_int64_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_uint8_as_variant() {
        numeric_partially_shredded_test!(u8, partially_shredded_uint8_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_uint16_as_variant() {
        numeric_partially_shredded_test!(u16, partially_shredded_uint16_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_uint32_as_variant() {
        numeric_partially_shredded_test!(u32, partially_shredded_uint32_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_uint64_as_variant() {
        numeric_partially_shredded_test!(u64, partially_shredded_uint64_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_float16_as_variant() {
        numeric_partially_shredded_test!(half::f16, partially_shredded_float16_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_float32_as_variant() {
        numeric_partially_shredded_test!(f32, partially_shredded_float32_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_float64_as_variant() {
        numeric_partially_shredded_test!(f64, partially_shredded_float64_variant_array);
    }

    /// Shredding: extract a value as an Int32Array
    #[test]
    fn get_variant_shredded_int32_as_int32_safe_cast() {
        // Extract the typed value as Int32Array
        let array = partially_shredded_int32_variant_array();
        // specify we want the typed value as Int32
        let field = Field::new("typed_value", DataType::Int32, true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(34),
            None,
            None, // "n/a" is not an Int32 so converted to null
            Some(100),
        ]));
        assert_eq!(&result, &expected)
    }

    /// Shredding: extract a value as an Int32Array, unsafe cast (should error on "n/a")

    #[test]
    fn get_variant_shredded_int32_as_int32_unsafe_cast() {
        // Extract the typed value as Int32Array
        let array = partially_shredded_int32_variant_array();
        let field = Field::new("typed_value", DataType::Int32, true);
        let cast_options = CastOptions {
            safe: false, // unsafe cast
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);

        let err = variant_get(&array, options).unwrap_err();
        // TODO make this error message nicer (not Debug format)
        assert_eq!(err.to_string(), "Cast error: Failed to extract primitive of type Int32 from variant ShortString(ShortString(\"n/a\")) at path VariantPath([])");
    }

    /// Perfect Shredding: extract the typed value as a VariantArray
    macro_rules! numeric_perfectly_shredded_test {
        ($primitive_type:ty, $data_fn:ident) => {
            let array = $data_fn();
            let options = GetOptions::new();
            let result = variant_get(&array, options).unwrap();

            // expect the result is a VariantArray
            let result: &VariantArray = result.as_any().downcast_ref().unwrap();
            assert_eq!(result.len(), 3);

            // Expect the values are the same as the original values
            assert_eq!(
                result.value(0),
                Variant::from(<$primitive_type>::try_from(1u8).unwrap())
            );
            assert_eq!(
                result.value(1),
                Variant::from(<$primitive_type>::try_from(2u8).unwrap())
            );
            assert_eq!(
                result.value(2),
                Variant::from(<$primitive_type>::try_from(3u8).unwrap())
            );
        };
    }

    #[test]
    fn get_variant_perfectly_shredded_int8_as_variant() {
        numeric_perfectly_shredded_test!(i8, perfectly_shredded_int8_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_int16_as_variant() {
        numeric_perfectly_shredded_test!(i16, perfectly_shredded_int16_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_int32_as_variant() {
        numeric_perfectly_shredded_test!(i32, perfectly_shredded_int32_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_int64_as_variant() {
        numeric_perfectly_shredded_test!(i64, perfectly_shredded_int64_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_uint8_as_variant() {
        numeric_perfectly_shredded_test!(u8, perfectly_shredded_uint8_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_uint16_as_variant() {
        numeric_perfectly_shredded_test!(u16, perfectly_shredded_uint16_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_uint32_as_variant() {
        numeric_perfectly_shredded_test!(u32, perfectly_shredded_uint32_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_uint64_as_variant() {
        numeric_perfectly_shredded_test!(u64, perfectly_shredded_uint64_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_float16_as_variant() {
        numeric_perfectly_shredded_test!(half::f16, perfectly_shredded_float16_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_float32_as_variant() {
        numeric_perfectly_shredded_test!(f32, perfectly_shredded_float32_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_float64_as_variant() {
        numeric_perfectly_shredded_test!(f64, perfectly_shredded_float64_variant_array);
    }

    /// Shredding: Extract the typed value as Int32Array
    #[test]
    fn get_variant_perfectly_shredded_int32_as_int32() {
        // Extract the typed value as Int32Array
        let array = perfectly_shredded_int32_variant_array();
        // specify we want the typed value as Int32
        let field = Field::new("typed_value", DataType::Int32, true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]));
        assert_eq!(&result, &expected)
    }

    /// AllNull: extract a value as a VariantArray
    #[test]
    fn get_variant_all_null_as_variant() {
        let array = all_null_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result: &VariantArray = result.as_any().downcast_ref().unwrap();
        assert_eq!(result.len(), 3);

        // All values should be null
        assert!(!result.is_valid(0));
        assert!(!result.is_valid(1));
        assert!(!result.is_valid(2));
    }

    /// AllNull: extract a value as an Int32Array
    #[test]
    fn get_variant_all_null_as_int32() {
        let array = all_null_variant_array();
        // specify we want the typed value as Int32
        let field = Field::new("typed_value", DataType::Int32, true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Option::<i32>::None,
            Option::<i32>::None,
            Option::<i32>::None,
        ]));
        assert_eq!(&result, &expected)
    }

    #[test]
    fn get_variant_perfectly_shredded_int16_as_int16() {
        // Extract the typed value as Int16Array
        let array = perfectly_shredded_int16_variant_array();
        // specify we want the typed value as Int16
        let field = Field::new("typed_value", DataType::Int16, true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();
        let expected: ArrayRef = Arc::new(Int16Array::from(vec![Some(1), Some(2), Some(3)]));
        assert_eq!(&result, &expected)
    }

    /// Return a VariantArray that represents a perfectly "shredded" variant
    /// for the given typed value.
    ///
    /// The schema of the corresponding `StructArray` would look like this:
    ///
    /// ```text
    /// StructArray {
    ///   metadata: BinaryViewArray,
    ///   typed_value: Int32Array,
    /// }
    /// ```
    macro_rules! numeric_perfectly_shredded_variant_array_fn {
        ($func:ident, $array_type:ident, $primitive_type:ty) => {
            fn $func() -> ArrayRef {
                // At the time of writing, the `VariantArrayBuilder` does not support shredding.
                // so we must construct the array manually.  see https://github.com/apache/arrow-rs/issues/7895
                let (metadata, _value) = { parquet_variant::VariantBuilder::new().finish() };
                let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 3));
                let typed_value = $array_type::from(vec![
                    Some(<$primitive_type>::try_from(1u8).unwrap()),
                    Some(<$primitive_type>::try_from(2u8).unwrap()),
                    Some(<$primitive_type>::try_from(3u8).unwrap()),
                ]);

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata))
                    .with_field("typed_value", Arc::new(typed_value))
                    .build();

                Arc::new(
                    VariantArray::try_new(Arc::new(struct_array))
                        .expect("should create variant array"),
                )
            }
        };
    }

    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_int8_variant_array,
        Int8Array,
        i8
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_int16_variant_array,
        Int16Array,
        i16
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_int32_variant_array,
        Int32Array,
        i32
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_int64_variant_array,
        Int64Array,
        i64
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_uint8_variant_array,
        UInt8Array,
        u8
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_uint16_variant_array,
        UInt16Array,
        u16
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_uint32_variant_array,
        UInt32Array,
        u32
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_uint64_variant_array,
        UInt64Array,
        u64
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_float16_variant_array,
        Float16Array,
        half::f16
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_float32_variant_array,
        Float32Array,
        f32
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_float64_variant_array,
        Float64Array,
        f64
    );

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
    macro_rules! numeric_partially_shredded_variant_array_fn {
        ($func:ident, $array_type:ident, $primitive_type:ty) => {
            fn $func() -> ArrayRef {
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

                let typed_value = $array_type::from(vec![
                    Some(<$primitive_type>::try_from(34u8).unwrap()), // row 0 is shredded, so it has a value
                    None,                                             // row 1 is null, so no value
                    None, // row 2 is a string, so no typed value
                    Some(<$primitive_type>::try_from(100u8).unwrap()), // row 3 is shredded, so it has a value
                ]);

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata))
                    .with_field("typed_value", Arc::new(typed_value))
                    .with_field("value", Arc::new(values))
                    .with_nulls(nulls)
                    .build();

                Arc::new(
                    VariantArray::try_new(Arc::new(struct_array))
                        .expect("should create variant array"),
                )
            }
        };
    }

    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_int8_variant_array,
        Int8Array,
        i8
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_int16_variant_array,
        Int16Array,
        i16
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_int32_variant_array,
        Int32Array,
        i32
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_int64_variant_array,
        Int64Array,
        i64
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_uint8_variant_array,
        UInt8Array,
        u8
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_uint16_variant_array,
        UInt16Array,
        u16
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_uint32_variant_array,
        UInt32Array,
        u32
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_uint64_variant_array,
        UInt64Array,
        u64
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_float16_variant_array,
        Float16Array,
        half::f16
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_float32_variant_array,
        Float32Array,
        f32
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_float64_variant_array,
        Float64Array,
        f64
    );

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

    /// Return a VariantArray that represents an "all null" variant
    /// for the following example (3 null values):
    ///
    /// ```text
    /// null
    /// null
    /// null
    /// ```
    ///
    /// The schema of the corresponding `StructArray` would look like this:
    ///
    /// ```text
    /// StructArray {
    ///   metadata: BinaryViewArray,
    /// }
    /// ```
    fn all_null_variant_array() -> ArrayRef {
        let (metadata, _value) = { parquet_variant::VariantBuilder::new().finish() };

        let nulls = NullBuffer::from(vec![
            false, // row 0 is null
            false, // row 1 is null
            false, // row 2 is null
        ]);

        // metadata is the same for all rows (though they're all null)
        let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 3));

        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata))
            .with_nulls(nulls)
            .build();

        Arc::new(
            VariantArray::try_new(Arc::new(struct_array)).expect("should create variant array"),
        )
    }
}
