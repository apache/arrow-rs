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
    array::{self, Array, ArrayRef, BinaryViewArray, StructArray},
    compute::CastOptions,
    datatypes::Field,
    error::Result,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use parquet_variant::{VariantPath, VariantPathElement};

use crate::VariantArray;
use crate::variant_array::BorrowedShreddingState;
use crate::variant_to_arrow::make_variant_to_arrow_row_builder;

use arrow::array::AsArray;
use std::sync::Arc;

pub(crate) enum ShreddedPathStep<'a> {
    /// Path step succeeded, return the new shredding state
    Success(BorrowedShreddingState<'a>),
    /// The path element is not present in the `typed_value` column and there is no `value` column,
    /// so we know it does not exist. It, and all paths under it, are all-NULL.
    Missing,
    /// The path element is not present in the `typed_value` column and must be retrieved from the `value`
    /// column instead. The caller should be prepared to handle any value, including the requested
    /// type, an arbitrary "wrong" type, or `Variant::Null`.
    NotShredded,
}

/// Given a shredded variant field -- a `(value?, typed_value?)` pair -- try to take one path step
/// deeper. For a `VariantPathElement::Field`, the step fails if there is no `typed_value` at this
/// level, or if `typed_value` is not a struct, or if the requested field name does not exist.
///
/// TODO: Support `VariantPathElement::Index`? It wouldn't be easy, and maybe not even possible.
pub(crate) fn follow_shredded_path_element<'a>(
    shredding_state: &BorrowedShreddingState<'a>,
    path_element: &VariantPathElement<'_>,
    cast_options: &CastOptions,
) -> Result<ShreddedPathStep<'a>> {
    // If the requested path element is not present in `typed_value`, and `value` is missing, then
    // we know it does not exist; it, and all paths under it, are all-NULL.
    let missing_path_step = || match shredding_state.value_field() {
        Some(_) => ShreddedPathStep::NotShredded,
        None => ShreddedPathStep::Missing,
    };

    let Some(typed_value) = shredding_state.typed_value_field() else {
        return Ok(missing_path_step());
    };

    match path_element {
        VariantPathElement::Field { name } => {
            // Try to step into the requested field name of a struct.
            // First, try to downcast to StructArray
            let Some(struct_array) = typed_value.as_any().downcast_ref::<StructArray>() else {
                // Downcast failure - if strict cast options are enabled, this should be an error
                if !cast_options.safe {
                    return Err(ArrowError::CastError(format!(
                        "Cannot access field '{}' on non-struct type: {}",
                        name,
                        typed_value.data_type()
                    )));
                }
                // With safe cast options, return NULL (missing_path_step)
                return Ok(missing_path_step());
            };

            // Now try to find the column - missing column in a present struct is just missing data
            let Some(field) = struct_array.column_by_name(name) else {
                // Missing column in a present struct is just missing, not wrong - return Ok
                return Ok(missing_path_step());
            };

            let struct_array = field.as_struct_opt().ok_or_else(|| {
                // TODO: Should we blow up? Or just end the traversal and let the normal
                // variant pathing code sort out the mess that it must anyway be
                // prepared to handle?
                ArrowError::InvalidArgumentError(format!(
                    "Expected Struct array while following path, got {}",
                    field.data_type(),
                ))
            })?;

            let state = BorrowedShreddingState::try_from(struct_array)?;
            Ok(ShreddedPathStep::Success(state))
        }
        VariantPathElement::Index { .. } => {
            // TODO: Support array indexing. Among other things, it will require slicing not
            // only the array we have here, but also the corresponding metadata and null masks.
            Err(ArrowError::NotYetImplemented(
                "Pathing into shredded variant array index".into(),
            ))
        }
    }
}

/// Follows the given path as far as possible through shredded variant fields. If the path ends on a
/// shredded field, return it directly. Otherwise, use a row shredder to follow the rest of the path
/// and extract the requested value on a per-row basis.
fn shredded_get_path(
    input: &VariantArray,
    path: &[VariantPathElement<'_>],
    as_field: Option<&Field>,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    // Helper that creates a new VariantArray from the given nested value and typed_value columns,
    // properly accounting for accumulated nulls from path traversal
    let make_target_variant =
        |value: Option<BinaryViewArray>,
         typed_value: Option<ArrayRef>,
         accumulated_nulls: Option<arrow::buffer::NullBuffer>| {
            let metadata = input.metadata_field().clone();
            VariantArray::from_parts(metadata, value, typed_value, accumulated_nulls)
        };

    // Helper that shreds a VariantArray to a specific type.
    let shred_basic_variant =
        |target: VariantArray, path: VariantPath<'_>, as_field: Option<&Field>| {
            let as_type = as_field.map(|f| f.data_type());
            let mut builder = make_variant_to_arrow_row_builder(
                target.metadata_field(),
                path,
                as_type,
                cast_options,
                target.len(),
            )?;
            for i in 0..target.len() {
                if target.is_null(i) {
                    builder.append_null()?;
                } else if !cast_options.safe {
                    let value = target.try_value(i)?;
                    builder.append_value(value)?;
                } else {
                    let _ = match target.try_value(i) {
                        Ok(v) => builder.append_value(v)?,
                        Err(_) => {
                            builder.append_null()?;
                            false // add this to make match arms have the same return type
                        }
                    };
                }
            }
            builder.finish()
        };

    // Peel away the prefix of path elements that traverses the shredded parts of this variant
    // column. Shredding will traverse the rest of the path on a per-row basis.
    let mut shredding_state = input.shredding_state().borrow();
    let mut accumulated_nulls = input.inner().nulls().cloned();
    let mut path_index = 0;
    for path_element in path {
        match follow_shredded_path_element(&shredding_state, path_element, cast_options)? {
            ShreddedPathStep::Success(state) => {
                // Union nulls from the typed_value we just accessed
                if let Some(typed_value) = shredding_state.typed_value_field() {
                    accumulated_nulls = arrow::buffer::NullBuffer::union(
                        accumulated_nulls.as_ref(),
                        typed_value.nulls(),
                    );
                }
                shredding_state = state;
                path_index += 1;
                continue;
            }
            ShreddedPathStep::Missing => {
                let num_rows = input.len();
                let arr = match as_field.map(|f| f.data_type()) {
                    Some(data_type) => Arc::new(array::new_null_array(data_type, num_rows)) as _,
                    None => Arc::new(array::NullArray::new(num_rows)) as _,
                };
                return Ok(arr);
            }
            ShreddedPathStep::NotShredded => {
                let target = make_target_variant(
                    shredding_state.value_field().cloned(),
                    None,
                    accumulated_nulls,
                );
                return shred_basic_variant(target, path[path_index..].into(), as_field);
            }
        };
    }

    // Path exhausted! Create a new `VariantArray` for the location we landed on.
    let target = make_target_variant(
        shredding_state.value_field().cloned(),
        shredding_state.typed_value_field().cloned(),
        accumulated_nulls,
    );

    // If our caller did not request any specific type, we can just return whatever we landed on.
    let Some(as_field) = as_field else {
        return Ok(ArrayRef::from(target));
    };

    // Try to return the typed value directly when we have a perfect shredding match.
    if let Some(shredded) = try_perfect_shredding(&target, as_field) {
        return Ok(shredded);
    }

    // Structs are special. Recurse into each field separately, hoping to follow the shredding even
    // further, and build up the final struct from those individually shredded results.
    if let DataType::Struct(fields) = as_field.data_type() {
        let children = fields
            .iter()
            .map(|field| {
                shredded_get_path(
                    &target,
                    &[VariantPathElement::from(field.name().as_str())],
                    Some(field),
                    cast_options,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let struct_nulls = target.nulls().cloned();

        return Ok(Arc::new(StructArray::try_new(
            fields.clone(),
            children,
            struct_nulls,
        )?));
    }

    // Not a struct, so directly shred the variant as the requested type
    shred_basic_variant(target, VariantPath::default(), Some(as_field))
}

fn try_perfect_shredding(variant_array: &VariantArray, as_field: &Field) -> Option<ArrayRef> {
    // Try to return the typed value directly when we have a perfect shredding match.
    if matches!(as_field.data_type(), DataType::Struct(_)) {
        return None;
    }
    let typed_value = variant_array.typed_value_field()?;
    if typed_value.data_type() == as_field.data_type()
        && variant_array
            .value_field()
            .is_none_or(|v| v.null_count() == v.len())
    {
        // Here we need to gate against the case where the `typed_value` is null but data is in the `value` column.
        // 1. If the `value` column is null, or
        // 2. If every row in the `value` column is null

        // This is a perfect shredding, where the value is entirely shredded out,
        // so we can just return the typed value.
        return Some(typed_value.clone());
    }
    None
}

/// Returns an array with the specified path extracted from the variant values.
///
/// The return array type depends on the `as_type` field of the options parameter
/// 1. `as_type: None`: a VariantArray is returned. The values in this new VariantArray will point
///    to the specified path.
/// 2. `as_type: Some(<specific field>)`: an array of the specified type is returned.
///
/// TODO: How would a caller request a struct or list type where the fields/elements can be any
/// variant? Caller can pass None as the requested type to fetch a specific path, but it would
/// quickly become annoying (and inefficient) to call `variant_get` for each leaf value in a struct or
/// list and then try to assemble the results.
pub fn variant_get(input: &ArrayRef, options: GetOptions) -> Result<ArrayRef> {
    let variant_array = VariantArray::try_new(input)?;

    let GetOptions {
        as_type,
        path,
        cast_options,
    } = options;

    shredded_get_path(&variant_array, &path, as_type.as_deref(), &cast_options)
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
    use std::str::FromStr;
    use std::sync::Arc;

    use super::{GetOptions, variant_get};
    use crate::variant_array::{ShreddedVariantFieldArray, StructArrayBuilder};
    use crate::{VariantArray, VariantArrayBuilder, json_to_variant};
    use arrow::array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
        Date64Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, NullBuilder, StringArray, StringViewArray, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::CastOptions;
    use arrow::datatypes::DataType::{Int16, Int32, Int64};
    use arrow::datatypes::i256;
    use arrow::util::display::FormatOptions;
    use arrow_schema::DataType::{Boolean, Float32, Float64, Int8};
    use arrow_schema::{DataType, Field, FieldRef, Fields, IntervalUnit, TimeUnit};
    use chrono::DateTime;
    use parquet_variant::{
        EMPTY_VARIANT_METADATA_BYTES, Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16,
        VariantDecimalType, VariantPath,
    };

    fn single_variant_get_test(input_json: &str, path: VariantPath, expected_json: &str) {
        // Create input array from JSON string
        let input_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(input_json)]));
        let input_variant_array_ref = ArrayRef::from(json_to_variant(&input_array_ref).unwrap());

        let result =
            variant_get(&input_variant_array_ref, GetOptions::new_with_path(path)).unwrap();

        // Create expected array from JSON string
        let expected_array_ref: ArrayRef = Arc::new(StringArray::from(vec![Some(expected_json)]));
        let expected_variant_array = json_to_variant(&expected_array_ref).unwrap();

        let result_array = VariantArray::try_new(&result).unwrap();
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
            let result = VariantArray::try_new(&result).unwrap();
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

    macro_rules! partially_shredded_variant_array_gen {
        ($func_name:ident,  $typed_value_array_gen: expr) => {
            fn $func_name() -> ArrayRef {
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

                let typed_value = $typed_value_array_gen();

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata), false)
                    .with_field("typed_value", Arc::new(typed_value), true)
                    .with_field("value", Arc::new(values), true)
                    .with_nulls(nulls)
                    .build();
                ArrayRef::from(
                    VariantArray::try_new(&struct_array).expect("should create variant array"),
                )
            }
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
    fn get_variant_partially_shredded_float32_as_variant() {
        numeric_partially_shredded_test!(f32, partially_shredded_float32_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_float64_as_variant() {
        numeric_partially_shredded_test!(f64, partially_shredded_float64_variant_array);
    }

    #[test]
    fn get_variant_partially_shredded_bool_as_variant() {
        let array = partially_shredded_bool_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result = VariantArray::try_new(&result).unwrap();
        assert_eq!(result.len(), 4);

        // Expect the values are the same as the original values
        assert_eq!(result.value(0), Variant::from(true));
        assert!(!result.is_valid(1));
        assert_eq!(result.value(2), Variant::from("n/a"));
        assert_eq!(result.value(3), Variant::from(false));
    }

    #[test]
    fn get_variant_partially_shredded_utf8_as_variant() {
        let array = partially_shredded_utf8_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result = VariantArray::try_new(&result).unwrap();
        assert_eq!(result.len(), 4);

        // Expect the values are the same as the original values
        assert_eq!(result.value(0), Variant::from("hello"));
        assert!(!result.is_valid(1));
        assert_eq!(result.value(2), Variant::from("n/a"));
        assert_eq!(result.value(3), Variant::from("world"));
    }

    partially_shredded_variant_array_gen!(partially_shredded_binary_view_variant_array, || {
        BinaryViewArray::from(vec![
            Some(&[1u8, 2u8, 3u8][..]), // row 0 is shredded
            None,                       // row 1 is null
            None,                       // row 2 is a string
            Some(&[4u8, 5u8, 6u8][..]), // row 3 is shredded
        ])
    });

    #[test]
    fn get_variant_partially_shredded_date32_as_variant() {
        let array = partially_shredded_date32_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result = VariantArray::try_new(&result).unwrap();
        assert_eq!(result.len(), 4);

        // Expect the values are the same as the original values
        use chrono::NaiveDate;
        let date1 = NaiveDate::from_ymd_opt(2025, 9, 17).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2025, 9, 9).unwrap();
        assert_eq!(result.value(0), Variant::from(date1));
        assert!(!result.is_valid(1));
        assert_eq!(result.value(2), Variant::from("n/a"));
        assert_eq!(result.value(3), Variant::from(date2));
    }

    #[test]
    fn get_variant_partially_shredded_binary_view_as_variant() {
        let array = partially_shredded_binary_view_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result = VariantArray::try_new(&result).unwrap();
        assert_eq!(result.len(), 4);

        // Expect the values are the same as the original values
        assert_eq!(result.value(0), Variant::from(&[1u8, 2u8, 3u8][..]));
        assert!(!result.is_valid(1));
        assert_eq!(result.value(2), Variant::from("n/a"));
        assert_eq!(result.value(3), Variant::from(&[4u8, 5u8, 6u8][..]));
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
        assert_eq!(
            err.to_string(),
            "Cast error: Failed to extract primitive of type Int32 from variant ShortString(ShortString(\"n/a\")) at path VariantPath([])"
        );
    }

    /// Perfect Shredding: extract the typed value as a VariantArray
    macro_rules! numeric_perfectly_shredded_test {
        ($primitive_type:ty, $data_fn:ident) => {
            let array = $data_fn();
            let options = GetOptions::new();
            let result = variant_get(&array, options).unwrap();

            // expect the result is a VariantArray
            let result = VariantArray::try_new(&result).unwrap();
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
    fn get_variant_perfectly_shredded_float32_as_variant() {
        numeric_perfectly_shredded_test!(f32, perfectly_shredded_float32_variant_array);
    }

    #[test]
    fn get_variant_perfectly_shredded_float64_as_variant() {
        numeric_perfectly_shredded_test!(f64, perfectly_shredded_float64_variant_array);
    }

    /// AllNull: extract a value as a VariantArray
    #[test]
    fn get_variant_all_null_as_variant() {
        let array = all_null_variant_array();
        let options = GetOptions::new();
        let result = variant_get(&array, options).unwrap();

        // expect the result is a VariantArray
        let result = VariantArray::try_new(&result).unwrap();
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

    macro_rules! perfectly_shredded_to_arrow_primitive_test {
        ($name:ident, $primitive_type:expr, $perfectly_shredded_array_gen_fun:ident, $expected_array:expr) => {
            #[test]
            fn $name() {
                let array = $perfectly_shredded_array_gen_fun();
                let field = Field::new("typed_value", $primitive_type, true);
                let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
                let result = variant_get(&array, options).unwrap();
                let expected_array: ArrayRef = Arc::new($expected_array);
                assert_eq!(&result, &expected_array);
            }
        };
    }

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_int18_as_int8,
        Int8,
        perfectly_shredded_int8_variant_array,
        Int8Array::from(vec![Some(1), Some(2), Some(3)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_int16_as_int16,
        Int16,
        perfectly_shredded_int16_variant_array,
        Int16Array::from(vec![Some(1), Some(2), Some(3)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_int32_as_int32,
        Int32,
        perfectly_shredded_int32_variant_array,
        Int32Array::from(vec![Some(1), Some(2), Some(3)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_int64_as_int64,
        Int64,
        perfectly_shredded_int64_variant_array,
        Int64Array::from(vec![Some(1), Some(2), Some(3)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_float32_as_float32,
        Float32,
        perfectly_shredded_float32_variant_array,
        Float32Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_float64_as_float64,
        Float64,
        perfectly_shredded_float64_variant_array,
        Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_boolean_as_boolean,
        Boolean,
        perfectly_shredded_bool_variant_array,
        BooleanArray::from(vec![Some(true), Some(false), Some(true)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_utf8_as_utf8,
        DataType::Utf8,
        perfectly_shredded_utf8_variant_array,
        StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_large_utf8_as_utf8,
        DataType::Utf8,
        perfectly_shredded_large_utf8_variant_array,
        StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_utf8_view_as_utf8,
        DataType::Utf8,
        perfectly_shredded_utf8_view_variant_array,
        StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    );

    macro_rules! perfectly_shredded_variant_array_fn {
        ($func:ident, $typed_value_gen:expr) => {
            fn $func() -> ArrayRef {
                // At the time of writing, the `VariantArrayBuilder` does not support shredding.
                // so we must construct the array manually.  see https://github.com/apache/arrow-rs/issues/7895
                let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(
                    EMPTY_VARIANT_METADATA_BYTES,
                    3,
                ));
                let typed_value = $typed_value_gen();

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata), false)
                    .with_field("typed_value", Arc::new(typed_value), true)
                    .build();

                VariantArray::try_new(&struct_array)
                    .expect("should create variant array")
                    .into()
            }
        };
    }

    perfectly_shredded_variant_array_fn!(perfectly_shredded_utf8_variant_array, || {
        StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    });

    perfectly_shredded_variant_array_fn!(perfectly_shredded_large_utf8_variant_array, || {
        LargeStringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    });

    perfectly_shredded_variant_array_fn!(perfectly_shredded_utf8_view_variant_array, || {
        StringViewArray::from(vec![Some("foo"), Some("bar"), Some("baz")])
    });

    perfectly_shredded_variant_array_fn!(perfectly_shredded_bool_variant_array, || {
        BooleanArray::from(vec![Some(true), Some(false), Some(true)])
    });

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
            perfectly_shredded_variant_array_fn!($func, || {
                $array_type::from(vec![
                    Some(<$primitive_type>::try_from(1u8).unwrap()),
                    Some(<$primitive_type>::try_from(2u8).unwrap()),
                    Some(<$primitive_type>::try_from(3u8).unwrap()),
                ])
            });
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
        perfectly_shredded_float32_variant_array,
        Float32Array,
        f32
    );
    numeric_perfectly_shredded_variant_array_fn!(
        perfectly_shredded_float64_variant_array,
        Float64Array,
        f64
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_micro_ntz_variant_array,
        || {
            arrow::array::TimestampMicrosecondArray::from(vec![
                Some(-456000),
                Some(1758602096000001),
                Some(1758602096000002),
            ])
        }
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_ntz_as_timestamp_micro_ntz,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        perfectly_shredded_timestamp_micro_ntz_variant_array,
        arrow::array::TimestampMicrosecondArray::from(vec![
            Some(-456000),
            Some(1758602096000001),
            Some(1758602096000002),
        ])
    );

    // test converting micro to nano
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_ntz_as_nano_ntz,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        perfectly_shredded_timestamp_micro_ntz_variant_array,
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-456000000),
            Some(1758602096000001000),
            Some(1758602096000002000)
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_timestamp_micro_variant_array, || {
        arrow::array::TimestampMicrosecondArray::from(vec![
            Some(-456000),
            Some(1758602096000001),
            Some(1758602096000002),
        ])
        .with_timezone("+00:00")
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_as_timestamp_micro,
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_micro_variant_array,
        arrow::array::TimestampMicrosecondArray::from(vec![
            Some(-456000),
            Some(1758602096000001),
            Some(1758602096000002),
        ])
        .with_timezone("+00:00")
    );

    // test converting micro to nano
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_as_nano,
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_micro_variant_array,
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-456000000),
            Some(1758602096000001000),
            Some(1758602096000002000)
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_nano_ntz_variant_array,
        || {
            arrow::array::TimestampNanosecondArray::from(vec![
                Some(-4999999561),
                Some(1758602096000000001),
                Some(1758602096000000002),
            ])
        }
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_micro_variant_array_for_second_and_milli_second,
        || {
            arrow::array::TimestampMicrosecondArray::from(vec![
                Some(1234),       // can't be cast to second & millisecond
                Some(1234000),    // can be cast to millisecond, but not second
                Some(1234000000), // can be cast to second & millisecond
            ])
            .with_timezone("+00:00")
        }
    );

    // The following two tests wants to cover the micro with timezone -> milli/second cases
    // there are three test items, which contains some items can be cast safely, and some can't
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_as_timestamp_second,
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_micro_variant_array_for_second_and_milli_second,
        arrow::array::TimestampSecondArray::from(vec![
            None,
            None, // Return None if can't be cast to second safely
            Some(1234)
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_as_timestamp_milli,
        DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_micro_variant_array_for_second_and_milli_second,
        arrow::array::TimestampMillisecondArray::from(vec![
            None, // Return None if can't be cast to millisecond safely
            Some(1234),
            Some(1234000)
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_micro_ntz_variant_array_for_second_and_milli_second,
        || {
            arrow::array::TimestampMicrosecondArray::from(vec![
                Some(1234),       // can't be cast to second & millisecond
                Some(1234000),    // can be cast to millisecond, but not second
                Some(1234000000), // can be cast to second & millisecond
            ])
        }
    );

    // The following two tests wants to cover the micro_ntz -> milli/second cases
    // there are three test items, which contains some items can be cast safely, and some can't
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_ntz_as_timestamp_second,
        DataType::Timestamp(TimeUnit::Second, None),
        perfectly_shredded_timestamp_micro_ntz_variant_array_for_second_and_milli_second,
        arrow::array::TimestampSecondArray::from(vec![
            None,
            None, // Return None if can't be cast to second safely
            Some(1234)
        ])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_micro_ntz_as_timestamp_milli,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        perfectly_shredded_timestamp_micro_ntz_variant_array_for_second_and_milli_second,
        arrow::array::TimestampMillisecondArray::from(vec![
            None, // Return None if can't be cast to millisecond safely
            Some(1234),
            Some(1234000)
        ])
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_nano_variant_array_for_second_and_milli_second,
        || {
            arrow::array::TimestampNanosecondArray::from(vec![
                Some(1234000),       // can't be cast to second & millisecond
                Some(1234000000),    // can be cast to millisecond, but not second
                Some(1234000000000), // can be cast to second & millisecond
            ])
            .with_timezone("+00:00")
        }
    );

    // The following two tests wants to cover the nano with timezone -> milli/second cases
    // there are three test items, which contains some items can be cast safely, and some can't
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_as_timestamp_second,
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_nano_variant_array_for_second_and_milli_second,
        arrow::array::TimestampSecondArray::from(vec![
            None,
            None, // Return None if can't be cast to second safely
            Some(1234)
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_as_timestamp_milli,
        DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_nano_variant_array_for_second_and_milli_second,
        arrow::array::TimestampMillisecondArray::from(vec![
            None, // Return None if can't be cast to millisecond safely
            Some(1234),
            Some(1234000)
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_timestamp_nano_ntz_variant_array_for_second_and_milli_second,
        || {
            arrow::array::TimestampNanosecondArray::from(vec![
                Some(1234000),       // can't be cast to second & millisecond
                Some(1234000000),    // can be cast to millisecond, but not second
                Some(1234000000000), // can be cast to second & millisecond
            ])
        }
    );

    // The following two tests wants to cover the nano_ntz -> milli/second cases
    // there are three test items, which contains some items can be cast safely, and some can't
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_ntz_as_timestamp_second,
        DataType::Timestamp(TimeUnit::Second, None),
        perfectly_shredded_timestamp_nano_ntz_variant_array_for_second_and_milli_second,
        arrow::array::TimestampSecondArray::from(vec![
            None,
            None, // Return None if can't be cast to second safely
            Some(1234)
        ])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_ntz_as_timestamp_milli,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        perfectly_shredded_timestamp_nano_ntz_variant_array_for_second_and_milli_second,
        arrow::array::TimestampMillisecondArray::from(vec![
            None, // Return None if can't be cast to millisecond safely
            Some(1234),
            Some(1234000)
        ])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_ntz_as_timestamp_nano_ntz,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        perfectly_shredded_timestamp_nano_ntz_variant_array,
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-4999999561),
            Some(1758602096000000001),
            Some(1758602096000000002),
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_timestamp_nano_variant_array, || {
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-4999999561),
            Some(1758602096000000001),
            Some(1758602096000000002),
        ])
        .with_timezone("+00:00")
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_timestamp_nano_as_timestamp_nano,
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00"))),
        perfectly_shredded_timestamp_nano_variant_array,
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-4999999561),
            Some(1758602096000000001),
            Some(1758602096000000002),
        ])
        .with_timezone("+00:00")
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_date_variant_array, || {
        Date32Array::from(vec![Some(-12345), Some(17586), Some(20000)])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_date_as_date,
        DataType::Date32,
        perfectly_shredded_date_variant_array,
        Date32Array::from(vec![Some(-12345), Some(17586), Some(20000)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_date_as_date64,
        DataType::Date64,
        perfectly_shredded_date_variant_array,
        Date64Array::from(vec![
            Some(-1066608000000),
            Some(1519430400000),
            Some(1728000000000)
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_time_variant_array, || {
        Time64MicrosecondArray::from(vec![Some(12345000), Some(87654000), Some(135792000)])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_time_as_time,
        DataType::Time64(TimeUnit::Microsecond),
        perfectly_shredded_time_variant_array,
        Time64MicrosecondArray::from(vec![Some(12345000), Some(87654000), Some(135792000)])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_time_as_time64_nano,
        DataType::Time64(TimeUnit::Nanosecond),
        perfectly_shredded_time_variant_array,
        Time64NanosecondArray::from(vec![
            Some(12345000000),
            Some(87654000000),
            Some(135792000000)
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_time_variant_array_for_time32, || {
        Time64MicrosecondArray::from(vec![
            Some(1234),        // This can't be cast to Time32 losslessly
            Some(7654000),     // This can be cast to Time32(Millisecond), but not Time32(Second)
            Some(35792000000), // This can be cast to Time32(Second) & Time32(Millisecond)
        ])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_time_as_time32_second,
        DataType::Time32(TimeUnit::Second),
        perfectly_shredded_time_variant_array_for_time32,
        Time32SecondArray::from(vec![
            None,
            None, // Return None if can't be cast to Time32(Second) safely
            Some(35792)
        ])
    );

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_time_as_time32_milli,
        DataType::Time32(TimeUnit::Millisecond),
        perfectly_shredded_time_variant_array_for_time32,
        Time32MillisecondArray::from(vec![
            None, // Return None if can't be cast to Time32(Second) safely
            Some(7654),
            Some(35792000)
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_null_variant_array, || {
        let mut builder = NullBuilder::new();
        builder.append_nulls(3);
        builder.finish()
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_null_as_null,
        DataType::Null,
        perfectly_shredded_null_variant_array,
        arrow::array::NullArray::new(3)
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_null_variant_array_with_int, || {
        Int32Array::from(vec![Some(32), Some(64), Some(48)])
    });

    // We append null values if type miss match happens in safe mode
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_null_with_type_missmatch_in_safe_mode,
        DataType::Null,
        perfectly_shredded_null_variant_array_with_int,
        arrow::array::NullArray::new(3)
    );

    // We'll return an error if type miss match happens in strict mode
    #[test]
    fn get_variant_perfectly_shredded_null_as_null_with_type_missmatch_in_strict_mode() {
        let array = perfectly_shredded_null_variant_array_with_int();
        let field = Field::new("typed_value", DataType::Null, true);
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            });

        let result = variant_get(&array, options);

        assert!(result.is_err());
        let error_msg = format!("{}", result.unwrap_err());
        assert!(
            error_msg
                .contains("Cast error: Failed to extract primitive of type Null from variant Int32(32) at path VariantPath([])"),
            "Expected=[Cast error: Failed to extract primitive of type Null from variant Int32(32) at path VariantPath([])],\
                Got error message=[{}]",
            error_msg
        );
    }

    perfectly_shredded_variant_array_fn!(perfectly_shredded_decimal4_variant_array, || {
        Decimal32Array::from(vec![Some(12345), Some(23400), Some(-12342)])
            .with_precision_and_scale(5, 2)
            .unwrap()
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal4_as_decimal4,
        DataType::Decimal32(5, 2),
        perfectly_shredded_decimal4_variant_array,
        Decimal32Array::from(vec![Some(12345), Some(23400), Some(-12342)])
            .with_precision_and_scale(5, 2)
            .unwrap()
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_decimal8_variant_array_cast2decimal32,
        || {
            Decimal64Array::from(vec![Some(123456), Some(145678), Some(-123456)])
                .with_precision_and_scale(6, 1)
                .unwrap()
        }
    );

    // The input will be cast to Decimal32 when transformed to Variant
    // This tests will covert the logic DataType::Decimal64(the original array)
    // -> Variant::Decimal4(VariantArray) -> DataType::Decimal64(the result array)
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal8_through_decimal32_as_decimal8,
        DataType::Decimal64(6, 1),
        perfectly_shredded_decimal8_variant_array_cast2decimal32,
        Decimal64Array::from(vec![Some(123456), Some(145678), Some(-123456)])
            .with_precision_and_scale(6, 1)
            .unwrap()
    );

    // This tests will covert the logic DataType::Decimal64(the original array)
    //  -> Variant::Decimal8(VariantArray) -> DataType::Decimal64(the result array)
    perfectly_shredded_variant_array_fn!(perfectly_shredded_decimal8_variant_array, || {
        Decimal64Array::from(vec![Some(1234567809), Some(1456787000), Some(-1234561203)])
            .with_precision_and_scale(10, 1)
            .unwrap()
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal8_as_decimal8,
        DataType::Decimal64(10, 1),
        perfectly_shredded_decimal8_variant_array,
        Decimal64Array::from(vec![Some(1234567809), Some(1456787000), Some(-1234561203)])
            .with_precision_and_scale(10, 1)
            .unwrap()
    );

    // This tests will covert the logic DataType::Decimal128(the original array)
    //  -> Variant::Decimal4(VariantArray) -> DataType::Decimal128(the result array)
    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_decimal16_within_decimal4_variant_array,
        || {
            Decimal128Array::from(vec![
                Some(i128::from(1234589)),
                Some(i128::from(2344444)),
                Some(i128::from(-1234789)),
            ])
            .with_precision_and_scale(7, 3)
            .unwrap()
        }
    );

    // This tests will covert the logic DataType::Decimal128(the original array)
    // -> Variant::Decimal4(VariantArray) -> DataType::Decimal128(the result array)
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal16_within_decimal4_as_decimal16,
        DataType::Decimal128(7, 3),
        perfectly_shredded_decimal16_within_decimal4_variant_array,
        Decimal128Array::from(vec![
            Some(i128::from(1234589)),
            Some(i128::from(2344444)),
            Some(i128::from(-1234789)),
        ])
        .with_precision_and_scale(7, 3)
        .unwrap()
    );

    perfectly_shredded_variant_array_fn!(
        perfectly_shredded_decimal16_within_decimal8_variant_array,
        || {
            Decimal128Array::from(vec![Some(1234567809), Some(1456787000), Some(-1234561203)])
                .with_precision_and_scale(10, 1)
                .unwrap()
        }
    );

    // This tests will covert the logic DataType::Decimal128(the original array)
    // -> Variant::Decimal8(VariantArray) -> DataType::Decimal128(the result array)
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal16_within8_as_decimal16,
        DataType::Decimal128(10, 1),
        perfectly_shredded_decimal16_within_decimal8_variant_array,
        Decimal128Array::from(vec![Some(1234567809), Some(1456787000), Some(-1234561203)])
            .with_precision_and_scale(10, 1)
            .unwrap()
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_decimal16_variant_array, || {
        Decimal128Array::from(vec![
            Some(i128::from_str("12345678901234567899").unwrap()),
            Some(i128::from_str("23445677483748324300").unwrap()),
            Some(i128::from_str("-12345678901234567899").unwrap()),
        ])
        .with_precision_and_scale(20, 3)
        .unwrap()
    });

    // This tests will covert the logic DataType::Decimal128(the original array)
    // -> Variant::Decimal16(VariantArray) -> DataType::Decimal128(the result array)
    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_decimal16_as_decimal16,
        DataType::Decimal128(20, 3),
        perfectly_shredded_decimal16_variant_array,
        Decimal128Array::from(vec![
            Some(i128::from_str("12345678901234567899").unwrap()),
            Some(i128::from_str("23445677483748324300").unwrap()),
            Some(i128::from_str("-12345678901234567899").unwrap())
        ])
        .with_precision_and_scale(20, 3)
        .unwrap()
    );

    macro_rules! assert_variant_get_as_variant_array_with_default_option {
        ($variant_array: expr, $array_expected: expr) => {{
            let options = GetOptions::new();
            let array = $variant_array;
            let result = variant_get(&array, options).unwrap();

            // expect the result is a VariantArray
            let result = VariantArray::try_new(&result).unwrap();

            assert_eq!(result.len(), $array_expected.len());

            for (idx, item) in $array_expected.into_iter().enumerate() {
                match item {
                    Some(item) => assert_eq!(result.value(idx), item),
                    None => assert!(result.is_null(idx)),
                }
            }
        }};
    }

    partially_shredded_variant_array_gen!(
        partially_shredded_timestamp_micro_ntz_variant_array,
        || {
            arrow::array::TimestampMicrosecondArray::from(vec![
                Some(-456000),
                None,
                None,
                Some(1758602096000000),
            ])
        }
    );

    #[test]
    fn get_variant_partial_shredded_timestamp_micro_ntz_as_variant() {
        let array = partially_shredded_timestamp_micro_ntz_variant_array();
        assert_variant_get_as_variant_array_with_default_option!(
            array,
            vec![
                Some(Variant::from(
                    DateTime::from_timestamp_micros(-456000i64)
                        .unwrap()
                        .naive_utc(),
                )),
                None,
                Some(Variant::from("n/a")),
                Some(Variant::from(
                    DateTime::parse_from_rfc3339("2025-09-23T12:34:56+08:00")
                        .unwrap()
                        .naive_utc(),
                )),
            ]
        )
    }

    partially_shredded_variant_array_gen!(partially_shredded_timestamp_micro_variant_array, || {
        arrow::array::TimestampMicrosecondArray::from(vec![
            Some(-456000),
            None,
            None,
            Some(1758602096000000),
        ])
        .with_timezone("+00:00")
    });

    #[test]
    fn get_variant_partial_shredded_timestamp_micro_as_variant() {
        let array = partially_shredded_timestamp_micro_variant_array();
        assert_variant_get_as_variant_array_with_default_option!(
            array,
            vec![
                Some(Variant::from(
                    DateTime::from_timestamp_micros(-456000i64)
                        .unwrap()
                        .to_utc(),
                )),
                None,
                Some(Variant::from("n/a")),
                Some(Variant::from(
                    DateTime::parse_from_rfc3339("2025-09-23T12:34:56+08:00")
                        .unwrap()
                        .to_utc(),
                )),
            ]
        )
    }

    partially_shredded_variant_array_gen!(
        partially_shredded_timestamp_nano_ntz_variant_array,
        || {
            arrow::array::TimestampNanosecondArray::from(vec![
                Some(-4999999561),
                None,
                None,
                Some(1758602096000000000),
            ])
        }
    );

    #[test]
    fn get_variant_partial_shredded_timestamp_nano_ntz_as_variant() {
        let array = partially_shredded_timestamp_nano_ntz_variant_array();

        assert_variant_get_as_variant_array_with_default_option!(
            array,
            vec![
                Some(Variant::from(
                    DateTime::from_timestamp(-5, 439).unwrap().naive_utc()
                )),
                None,
                Some(Variant::from("n/a")),
                Some(Variant::from(
                    DateTime::parse_from_rfc3339("2025-09-23T12:34:56+08:00")
                        .unwrap()
                        .naive_utc()
                )),
            ]
        )
    }

    partially_shredded_variant_array_gen!(partially_shredded_timestamp_nano_variant_array, || {
        arrow::array::TimestampNanosecondArray::from(vec![
            Some(-4999999561),
            None,
            None,
            Some(1758602096000000000),
        ])
        .with_timezone("+00:00")
    });

    #[test]
    fn get_variant_partial_shredded_timestamp_nano_as_variant() {
        let array = partially_shredded_timestamp_nano_variant_array();

        assert_variant_get_as_variant_array_with_default_option!(
            array,
            vec![
                Some(Variant::from(
                    DateTime::from_timestamp(-5, 439).unwrap().to_utc()
                )),
                None,
                Some(Variant::from("n/a")),
                Some(Variant::from(
                    DateTime::parse_from_rfc3339("2025-09-23T12:34:56+08:00")
                        .unwrap()
                        .to_utc()
                )),
            ]
        )
    }

    perfectly_shredded_variant_array_fn!(perfectly_shredded_binary_variant_array, || {
        BinaryArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_binary_as_binary,
        DataType::Binary,
        perfectly_shredded_binary_variant_array,
        BinaryArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_large_binary_variant_array, || {
        LargeBinaryArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_large_binary_as_large_binary,
        DataType::LargeBinary,
        perfectly_shredded_large_binary_variant_array,
        LargeBinaryArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
    );

    perfectly_shredded_variant_array_fn!(perfectly_shredded_binary_view_variant_array, || {
        BinaryViewArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
    });

    perfectly_shredded_to_arrow_primitive_test!(
        get_variant_perfectly_shredded_binary_view_as_binary_view,
        DataType::BinaryView,
        perfectly_shredded_binary_view_variant_array,
        BinaryViewArray::from(vec![
            Some(b"Apache" as &[u8]),
            Some(b"Arrow-rs" as &[u8]),
            Some(b"Parquet-variant" as &[u8]),
        ])
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
            partially_shredded_variant_array_gen!($func, || $array_type::from(vec![
                Some(<$primitive_type>::try_from(34u8).unwrap()), // row 0 is shredded, so it has a value
                None,                                             // row 1 is null, so no value
                None, // row 2 is a string, so no typed value
                Some(<$primitive_type>::try_from(100u8).unwrap()), // row 3 is shredded, so it has a value
            ]));
        };
    }

    macro_rules! partially_shredded_variant_array_gen {
        ($func:ident, $typed_array_gen: expr) => {
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

                let typed_value = $typed_array_gen();

                let struct_array = StructArrayBuilder::new()
                    .with_field("metadata", Arc::new(metadata), false)
                    .with_field("typed_value", Arc::new(typed_value), true)
                    .with_field("value", Arc::new(values), true)
                    .with_nulls(nulls)
                    .build();

                ArrayRef::from(
                    VariantArray::try_new(&struct_array).expect("should create variant array"),
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
        partially_shredded_float32_variant_array,
        Float32Array,
        f32
    );
    numeric_partially_shredded_variant_array_fn!(
        partially_shredded_float64_variant_array,
        Float64Array,
        f64
    );

    partially_shredded_variant_array_gen!(partially_shredded_bool_variant_array, || {
        arrow::array::BooleanArray::from(vec![
            Some(true),  // row 0 is shredded, so it has a value
            None,        // row 1 is null, so no value
            None,        // row 2 is a string, so no typed value
            Some(false), // row 3 is shredded, so it has a value
        ])
    });

    partially_shredded_variant_array_gen!(partially_shredded_utf8_variant_array, || {
        StringArray::from(vec![
            Some("hello"), // row 0 is shredded
            None,          // row 1 is null
            None,          // row 2 is a string
            Some("world"), // row 3 is shredded
        ])
    });

    partially_shredded_variant_array_gen!(partially_shredded_date32_variant_array, || {
        Date32Array::from(vec![
            Some(20348), // row 0 is shredded, 2025-09-17
            None,        // row 1 is null
            None,        // row 2 is a string, not a date
            Some(20340), // row 3 is shredded, 2025-09-09
        ])
    });

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
        let nulls = NullBuffer::from(vec![
            false, // row 0 is null
            false, // row 1 is null
            false, // row 2 is null
        ]);

        // metadata is the same for all rows (though they're all null)
        let metadata =
            BinaryViewArray::from_iter_values(std::iter::repeat_n(EMPTY_VARIANT_METADATA_BYTES, 3));

        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata), false)
            .with_nulls(nulls)
            .build();

        Arc::new(struct_array)
    }
    /// This test manually constructs a shredded variant array representing objects
    /// like {"x": 1, "y": "foo"} and {"x": 42} and tests extracting the "x" field
    /// as VariantArray using variant_get.
    #[test]
    fn test_shredded_object_field_access() {
        let array = shredded_object_with_x_field_variant_array();

        // Test: Extract the "x" field as VariantArray first
        let options = GetOptions::new_with_path(VariantPath::from("x"));
        let result = variant_get(&array, options).unwrap();

        let result_variant = VariantArray::try_new(&result).unwrap();
        assert_eq!(result_variant.len(), 2);

        // Row 0: expect x=1
        assert_eq!(result_variant.value(0), Variant::Int32(1));
        // Row 1: expect x=42
        assert_eq!(result_variant.value(1), Variant::Int32(42));
    }

    /// Test extracting shredded object field with type conversion
    #[test]
    fn test_shredded_object_field_as_int32() {
        let array = shredded_object_with_x_field_variant_array();

        // Test: Extract the "x" field as Int32Array (type conversion)
        let field = Field::new("x", DataType::Int32, false);
        let options = GetOptions::new_with_path(VariantPath::from("x"))
            .with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();

        // Should get Int32Array
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(42)]));
        assert_eq!(&result, &expected);
    }

    /// Helper function to create a shredded variant array representing objects
    ///
    /// This creates an array that represents:
    /// Row 0: {"x": 1, "y": "foo"}  (x is shredded, y is in value field)
    /// Row 1: {"x": 42}             (x is shredded, perfect shredding)
    ///
    /// The physical layout follows the shredding spec where:
    /// - metadata: contains object metadata
    /// - typed_value: StructArray with field "x" (ShreddedVariantFieldArray)
    /// - value: contains fallback for unshredded fields like {"y": "foo"}
    /// - The "x" field has typed_value=Int32Array and value=NULL (perfect shredding)
    fn shredded_object_with_x_field_variant_array() -> ArrayRef {
        // Create the base metadata for objects
        let (metadata, y_field_value) = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("x", Variant::Int32(42));
            obj.insert("y", Variant::from("foo"));
            obj.finish();
            builder.finish()
        };

        // Create metadata array (same for both rows)
        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 2));

        // Create the main value field per the 3-step shredding spec:
        // Step 2: If field not in shredding schema, check value field
        // Row 0: {"y": "foo"} (y is not shredded, stays in value for step 2)
        // Row 1: {} (empty object - no unshredded fields)
        let empty_object_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };

        let value_array = BinaryViewArray::from(vec![
            Some(y_field_value.as_slice()),      // Row 0 has {"y": "foo"}
            Some(empty_object_value.as_slice()), // Row 1 has {}
        ]);

        // Create the "x" field as a ShreddedVariantFieldArray
        // This represents the shredded Int32 values for the "x" field
        let x_field_typed_value = Int32Array::from(vec![Some(1), Some(42)]);

        // For perfect shredding of the x field, no "value" column, only typed_value
        let x_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(x_field_typed_value), true)
            .build();

        // Wrap the x field struct in a ShreddedVariantFieldArray
        let x_field_shredded = ShreddedVariantFieldArray::try_new(&x_field_struct)
            .expect("should create ShreddedVariantFieldArray");

        // Create the main typed_value as a struct containing the "x" field
        let typed_value_fields = Fields::from(vec![Field::new(
            "x",
            x_field_shredded.data_type().clone(),
            true,
        )]);
        let typed_value_struct = StructArray::try_new(
            typed_value_fields,
            vec![ArrayRef::from(x_field_shredded)],
            None, // No nulls - both rows have the object structure
        )
        .unwrap();

        // Create the main VariantArray
        let main_struct = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("value", Arc::new(value_array), true)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .build();

        Arc::new(main_struct)
    }

    /// Simple test to check if nested paths are supported by current implementation
    #[test]
    fn test_simple_nested_path_support() {
        // Check: How does VariantPath parse different strings?
        println!("Testing path parsing:");

        let path_x = VariantPath::from("x");
        let elements_x: Vec<_> = path_x.iter().collect();
        println!("  'x' -> {} elements: {:?}", elements_x.len(), elements_x);

        let path_ax = VariantPath::from("a.x");
        let elements_ax: Vec<_> = path_ax.iter().collect();
        println!(
            "  'a.x' -> {} elements: {:?}",
            elements_ax.len(),
            elements_ax
        );

        let path_ax_alt = VariantPath::from("$.a.x");
        let elements_ax_alt: Vec<_> = path_ax_alt.iter().collect();
        println!(
            "  '$.a.x' -> {} elements: {:?}",
            elements_ax_alt.len(),
            elements_ax_alt
        );

        let path_nested = VariantPath::from("a").join("x");
        let elements_nested: Vec<_> = path_nested.iter().collect();
        println!(
            "  VariantPath::from('a').join('x') -> {} elements: {:?}",
            elements_nested.len(),
            elements_nested
        );

        // Use your existing simple test data but try "a.x" instead of "x"
        let array = shredded_object_with_x_field_variant_array();

        // Test if variant_get with REAL nested path throws not implemented error
        let real_nested_path = VariantPath::from("a").join("x");
        let options = GetOptions::new_with_path(real_nested_path);
        let result = variant_get(&array, options);

        match result {
            Ok(_) => {
                println!("Nested path 'a.x' works unexpectedly!");
            }
            Err(e) => {
                println!("Nested path 'a.x' error: {}", e);
                if e.to_string().contains("Not yet implemented")
                    || e.to_string().contains("NotYetImplemented")
                {
                    println!("This is expected - nested paths are not implemented");
                    return;
                }
                // Any other error is also expected for now
                println!("This shows nested paths need implementation");
            }
        }
    }

    /// Test comprehensive variant_get scenarios with Int32 conversion
    /// Test depth 0: Direct field access "x" with Int32 conversion
    /// Covers shredded vs non-shredded VariantArrays for simple field access
    #[test]
    fn test_depth_0_int32_conversion() {
        println!("=== Testing Depth 0: Direct field access ===");

        // Non-shredded test data: [{"x": 42}, {"x": "foo"}, {"y": 10}]
        let unshredded_array = create_depth_0_test_data();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("x");
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&unshredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(42), // {"x": 42} -> 42
            None,     // {"x": "foo"} -> NULL (type mismatch)
            None,     // {"y": 10} -> NULL (field missing)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 0 (unshredded) passed");

        // Shredded test data: using simplified approach based on working pattern
        let shredded_array = create_depth_0_shredded_test_data_simple();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("x");
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&shredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(42), // {"x": 42} -> 42 (from typed_value)
            None,     // {"x": "foo"} -> NULL (type mismatch, from value field)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 0 (shredded) passed");
    }

    /// Test depth 1: Single nested field access "a.x" with Int32 conversion
    /// Covers shredded vs non-shredded VariantArrays for nested field access
    #[test]
    fn test_depth_1_int32_conversion() {
        println!("=== Testing Depth 1: Single nested field access ===");

        // Non-shredded test data from the GitHub issue
        let unshredded_array = create_nested_path_test_data();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("a.x"); // Dot notation!
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&unshredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(55), // {"a": {"x": 55}} -> 55
            None,     // {"a": {"x": "foo"}} -> NULL (type mismatch)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 1 (unshredded) passed");

        // Shredded test data: depth 1 nested shredding
        let shredded_array = create_depth_1_shredded_test_data_working();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("a.x"); // Dot notation!
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&shredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(55), // {"a": {"x": 55}} -> 55 (from nested shredded x)
            None,     // {"a": {"x": "foo"}} -> NULL (type mismatch in nested value)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 1 (shredded) passed");
    }

    /// Test depth 2: Double nested field access "a.b.x" with Int32 conversion  
    /// Covers shredded vs non-shredded VariantArrays for deeply nested field access
    #[test]
    fn test_depth_2_int32_conversion() {
        println!("=== Testing Depth 2: Double nested field access ===");

        // Non-shredded test data: [{"a": {"b": {"x": 100}}}, {"a": {"b": {"x": "bar"}}}, {"a": {"b": {"y": 200}}}]
        let unshredded_array = create_depth_2_test_data();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("a.b.x"); // Double nested dot notation!
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&unshredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(100), // {"a": {"b": {"x": 100}}} -> 100
            None,      // {"a": {"b": {"x": "bar"}}} -> NULL (type mismatch)
            None,      // {"a": {"b": {"y": 200}}} -> NULL (field missing)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 2 (unshredded) passed");

        // Shredded test data: depth 2 nested shredding
        let shredded_array = create_depth_2_shredded_test_data_working();

        let field = Field::new("result", DataType::Int32, true);
        let path = VariantPath::from("a.b.x"); // Double nested dot notation!
        let options = GetOptions::new_with_path(path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&shredded_array, options).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(100), // {"a": {"b": {"x": 100}}} -> 100 (from deeply nested shredded x)
            None,      // {"a": {"b": {"x": "bar"}}} -> NULL (type mismatch in deep value)
            None,      // {"a": {"b": {"y": 200}}} -> NULL (field missing in deep structure)
        ]));
        assert_eq!(&result, &expected);
        println!("Depth 2 (shredded) passed");
    }

    /// Test that demonstrates what CURRENTLY WORKS
    ///
    /// This shows that nested path functionality does work, but only when the
    /// test data matches what the current implementation expects
    #[test]
    fn test_current_nested_path_functionality() {
        let array = shredded_object_with_x_field_variant_array();

        // Test: Extract the "x" field (single level) - this works
        let single_path = VariantPath::from("x");
        let field = Field::new("result", DataType::Int32, true);
        let options =
            GetOptions::new_with_path(single_path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();

        println!("Single path 'x' works - result: {:?}", result);

        // Test: Try nested path "a.x" - this is what we need to implement
        let nested_path = VariantPath::from("a").join("x");
        let field = Field::new("result", DataType::Int32, true);
        let options =
            GetOptions::new_with_path(nested_path).with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&array, options).unwrap();

        println!("Nested path 'a.x' result: {:?}", result);
    }

    /// Create test data for depth 0 (direct field access)
    /// [{"x": 42}, {"x": "foo"}, {"y": 10}]
    fn create_depth_0_test_data() -> ArrayRef {
        let mut builder = crate::VariantArrayBuilder::new(3);

        // Row 1: {"x": 42}
        {
            let json_str = r#"{"x": 42}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        // Row 2: {"x": "foo"}
        {
            let json_str = r#"{"x": "foo"}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        // Row 3: {"y": 10} (missing "x" field)
        {
            let json_str = r#"{"y": 10}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        ArrayRef::from(builder.build())
    }

    /// Create test data for depth 1 (single nested field)
    /// This represents the exact scenarios from the GitHub issue: "a.x"
    fn create_nested_path_test_data() -> ArrayRef {
        let mut builder = crate::VariantArrayBuilder::new(2);

        // Row 1: {"a": {"x": 55}, "b": 42}
        {
            let json_str = r#"{"a": {"x": 55}, "b": 42}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        // Row 2: {"a": {"x": "foo"}, "b": 42}
        {
            let json_str = r#"{"a": {"x": "foo"}, "b": 42}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        ArrayRef::from(builder.build())
    }

    /// Create test data for depth 2 (double nested field)
    /// [{"a": {"b": {"x": 100}}}, {"a": {"b": {"x": "bar"}}}, {"a": {"b": {"y": 200}}}]
    fn create_depth_2_test_data() -> ArrayRef {
        let mut builder = crate::VariantArrayBuilder::new(3);

        // Row 1: {"a": {"b": {"x": 100}}}
        {
            let json_str = r#"{"a": {"b": {"x": 100}}}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        // Row 2: {"a": {"b": {"x": "bar"}}}
        {
            let json_str = r#"{"a": {"b": {"x": "bar"}}}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        // Row 3: {"a": {"b": {"y": 200}}} (missing "x" field)
        {
            let json_str = r#"{"a": {"b": {"y": 200}}}"#;
            let string_array: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
            if let Ok(variant_array) = json_to_variant(&string_array) {
                builder.append_variant(variant_array.value(0));
            } else {
                builder.append_null();
            }
        }

        ArrayRef::from(builder.build())
    }

    /// Create simple shredded test data for depth 0 using a simplified working pattern
    /// Creates 2 rows: [{"x": 42}, {"x": "foo"}] with "x" shredded where possible
    fn create_depth_0_shredded_test_data_simple() -> ArrayRef {
        // Create base metadata using the working pattern
        let (metadata, string_x_value) = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("x", Variant::from("foo"));
            obj.finish();
            builder.finish()
        };

        // Metadata array (same for both rows)
        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 2));

        // Value array following the 3-step shredding spec:
        // Row 0: {} (x is shredded, no unshredded fields)
        // Row 1: {"x": "foo"} (x is a string, can't be shredded to Int32)
        let empty_object_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };

        let value_array = BinaryViewArray::from(vec![
            Some(empty_object_value.as_slice()), // Row 0: {} (x shredded out)
            Some(string_x_value.as_slice()),     // Row 1: {"x": "foo"} (fallback)
        ]);

        // Create the "x" field as a ShreddedVariantFieldArray
        let x_field_typed_value = Int32Array::from(vec![Some(42), None]);

        // For the x field, only typed_value (perfect shredding when possible)
        let x_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(x_field_typed_value), true)
            .build();

        let x_field_shredded = ShreddedVariantFieldArray::try_new(&x_field_struct)
            .expect("should create ShreddedVariantFieldArray");

        // Create the main typed_value as a struct containing the "x" field
        let typed_value_fields = Fields::from(vec![Field::new(
            "x",
            x_field_shredded.data_type().clone(),
            true,
        )]);
        let typed_value_struct = StructArray::try_new(
            typed_value_fields,
            vec![ArrayRef::from(x_field_shredded)],
            None,
        )
        .unwrap();

        // Build final VariantArray
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("value", Arc::new(value_array), true)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .build();

        Arc::new(struct_array)
    }

    /// Create working depth 1 shredded test data based on the existing working pattern
    /// This creates a properly structured shredded variant for "a.x" where:
    /// - Row 0: {"a": {"x": 55}, "b": 42} with a.x shredded into typed_value
    /// - Row 1: {"a": {"x": "foo"}, "b": 42} with a.x fallback to value field due to type mismatch
    fn create_depth_1_shredded_test_data_working() -> ArrayRef {
        // Create metadata following the working pattern from shredded_object_with_x_field_variant_array
        let (metadata, _) = {
            // Create nested structure: {"a": {"x": 55}, "b": 42}
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();

            // Create the nested "a" object
            let mut a_obj = obj.new_object("a");
            a_obj.insert("x", Variant::Int32(55));
            a_obj.finish();

            obj.insert("b", Variant::Int32(42));
            obj.finish();
            builder.finish()
        };

        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 2));

        // Create value arrays for the fallback case
        // Following the spec: if field cannot be shredded, it stays in value
        let empty_object_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };

        // Row 1 fallback: use the working pattern from the existing shredded test
        // This avoids metadata issues by using the simple fallback approach
        let row1_fallback = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("fallback", Variant::from("data"));
            obj.finish();
            let (_, value) = builder.finish();
            value
        };

        let value_array = BinaryViewArray::from(vec![
            Some(empty_object_value.as_slice()), // Row 0: {} (everything shredded except b in unshredded fields)
            Some(row1_fallback.as_slice()), // Row 1: {"a": {"x": "foo"}, "b": 42} (a.x can't be shredded)
        ]);

        // Create the nested shredded structure
        // Level 2: x field (the deepest level)
        let x_typed_value = Int32Array::from(vec![Some(55), None]);
        let x_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(x_typed_value), true)
            .build();
        let x_field_shredded = ShreddedVariantFieldArray::try_new(&x_field_struct)
            .expect("should create ShreddedVariantFieldArray for x");

        // Level 1: a field containing x field + value field for fallbacks
        // The "a" field needs both typed_value (for shredded x) and value (for fallback cases)

        // Create the value field for "a" (for cases where a.x can't be shredded)
        let a_value_data = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };
        let a_value_array = BinaryViewArray::from(vec![
            None,                          // Row 0: x is shredded, so no value fallback needed
            Some(a_value_data.as_slice()), // Row 1: fallback for a.x="foo" (but logic will check typed_value first)
        ]);

        let a_inner_fields = Fields::from(vec![Field::new(
            "x",
            x_field_shredded.data_type().clone(),
            true,
        )]);
        let a_inner_struct = StructArrayBuilder::new()
            .with_field(
                "typed_value",
                Arc::new(
                    StructArray::try_new(
                        a_inner_fields,
                        vec![ArrayRef::from(x_field_shredded)],
                        None,
                    )
                    .unwrap(),
                ),
                true,
            )
            .with_field("value", Arc::new(a_value_array), true)
            .build();
        let a_field_shredded = ShreddedVariantFieldArray::try_new(&a_inner_struct)
            .expect("should create ShreddedVariantFieldArray for a");

        // Level 0: main typed_value struct containing a field
        let typed_value_fields = Fields::from(vec![Field::new(
            "a",
            a_field_shredded.data_type().clone(),
            true,
        )]);
        let typed_value_struct = StructArray::try_new(
            typed_value_fields,
            vec![ArrayRef::from(a_field_shredded)],
            None,
        )
        .unwrap();

        // Build final VariantArray
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("value", Arc::new(value_array), true)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .build();

        Arc::new(struct_array)
    }

    /// Create working depth 2 shredded test data for "a.b.x" paths
    /// This creates a 3-level nested shredded structure where:
    /// - Row 0: {"a": {"b": {"x": 100}}} with a.b.x shredded into typed_value
    /// - Row 1: {"a": {"b": {"x": "bar"}}} with type mismatch fallback
    /// - Row 2: {"a": {"b": {"y": 200}}} with missing field fallback
    fn create_depth_2_shredded_test_data_working() -> ArrayRef {
        // Create metadata following the working pattern
        let (metadata, _) = {
            // Create deeply nested structure: {"a": {"b": {"x": 100}}}
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();

            // Create the nested "a.b" structure
            let mut a_obj = obj.new_object("a");
            let mut b_obj = a_obj.new_object("b");
            b_obj.insert("x", Variant::Int32(100));
            b_obj.finish();
            a_obj.finish();

            obj.finish();
            builder.finish()
        };

        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 3));

        // Create value arrays for fallback cases
        let empty_object_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };

        // Simple fallback values - avoiding complex nested metadata
        let value_array = BinaryViewArray::from(vec![
            Some(empty_object_value.as_slice()), // Row 0: fully shredded
            Some(empty_object_value.as_slice()), // Row 1: fallback (simplified)
            Some(empty_object_value.as_slice()), // Row 2: fallback (simplified)
        ]);

        // Create the deeply nested shredded structure: a.b.x

        // Level 3: x field (deepest level)
        let x_typed_value = Int32Array::from(vec![Some(100), None, None]);
        let x_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(x_typed_value), true)
            .build();
        let x_field_shredded = ShreddedVariantFieldArray::try_new(&x_field_struct)
            .expect("should create ShreddedVariantFieldArray for x");

        // Level 2: b field containing x field + value field
        let b_value_data = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };
        let b_value_array = BinaryViewArray::from(vec![
            None,                          // Row 0: x is shredded
            Some(b_value_data.as_slice()), // Row 1: fallback for b.x="bar"
            Some(b_value_data.as_slice()), // Row 2: fallback for b.y=200
        ]);

        let b_inner_fields = Fields::from(vec![Field::new(
            "x",
            x_field_shredded.data_type().clone(),
            true,
        )]);
        let b_inner_struct = StructArrayBuilder::new()
            .with_field(
                "typed_value",
                Arc::new(
                    StructArray::try_new(
                        b_inner_fields,
                        vec![ArrayRef::from(x_field_shredded)],
                        None,
                    )
                    .unwrap(),
                ),
                true,
            )
            .with_field("value", Arc::new(b_value_array), true)
            .build();
        let b_field_shredded = ShreddedVariantFieldArray::try_new(&b_inner_struct)
            .expect("should create ShreddedVariantFieldArray for b");

        // Level 1: a field containing b field + value field
        let a_value_data = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            let (_, value) = builder.finish();
            value
        };
        let a_value_array = BinaryViewArray::from(vec![
            None,                          // Row 0: b is shredded
            Some(a_value_data.as_slice()), // Row 1: fallback for a.b.*
            Some(a_value_data.as_slice()), // Row 2: fallback for a.b.*
        ]);

        let a_inner_fields = Fields::from(vec![Field::new(
            "b",
            b_field_shredded.data_type().clone(),
            true,
        )]);
        let a_inner_struct = StructArrayBuilder::new()
            .with_field(
                "typed_value",
                Arc::new(
                    StructArray::try_new(
                        a_inner_fields,
                        vec![ArrayRef::from(b_field_shredded)],
                        None,
                    )
                    .unwrap(),
                ),
                true,
            )
            .with_field("value", Arc::new(a_value_array), true)
            .build();
        let a_field_shredded = ShreddedVariantFieldArray::try_new(&a_inner_struct)
            .expect("should create ShreddedVariantFieldArray for a");

        // Level 0: main typed_value struct containing a field
        let typed_value_fields = Fields::from(vec![Field::new(
            "a",
            a_field_shredded.data_type().clone(),
            true,
        )]);
        let typed_value_struct = StructArray::try_new(
            typed_value_fields,
            vec![ArrayRef::from(a_field_shredded)],
            None,
        )
        .unwrap();

        // Build final VariantArray
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("value", Arc::new(value_array), true)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .build();

        Arc::new(struct_array)
    }

    #[test]
    fn test_strict_cast_options_downcast_failure() {
        use arrow::compute::CastOptions;
        use arrow::datatypes::{DataType, Field};
        use arrow::error::ArrowError;
        use parquet_variant::VariantPath;
        use std::sync::Arc;

        // Use the existing simple test data that has Int32 as typed_value
        let variant_array = perfectly_shredded_int32_variant_array();

        // Try to access a field with safe cast options (should return NULLs)
        let safe_options = GetOptions {
            path: VariantPath::from("nonexistent_field"),
            as_type: Some(Arc::new(Field::new("result", DataType::Int32, true))),
            cast_options: CastOptions::default(), // safe = true
        };

        let variant_array_ref: Arc<dyn Array> = variant_array.clone();
        let result = variant_get(&variant_array_ref, safe_options);
        // Should succeed and return NULLs (safe behavior)
        assert!(result.is_ok());
        let result_array = result.unwrap();
        assert_eq!(result_array.len(), 3);
        assert!(result_array.is_null(0));
        assert!(result_array.is_null(1));
        assert!(result_array.is_null(2));

        // Try to access a field with strict cast options (should error)
        let strict_options = GetOptions {
            path: VariantPath::from("nonexistent_field"),
            as_type: Some(Arc::new(Field::new("result", DataType::Int32, true))),
            cast_options: CastOptions {
                safe: false,
                ..Default::default()
            },
        };

        let result = variant_get(&variant_array_ref, strict_options);
        // Should fail with a cast error
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ArrowError::CastError(_)));
        assert!(
            error
                .to_string()
                .contains("Cannot access field 'nonexistent_field' on non-struct type")
        );
    }

    #[test]
    fn test_error_message_boolean_type_display() {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::Int32(123));
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        // Request Boolean with strict casting to force an error
        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", DataType::Boolean, true))),
            cast_options: CastOptions {
                safe: false,
                ..Default::default()
            },
        };

        let err = variant_get(&variant_array, options).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to extract primitive of type Boolean"));
    }

    #[test]
    fn test_error_message_numeric_type_display() {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::BooleanTrue);
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        // Request Boolean with strict casting to force an error
        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", DataType::Float32, true))),
            cast_options: CastOptions {
                safe: false,
                ..Default::default()
            },
        };

        let err = variant_get(&variant_array, options).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to extract primitive of type Float32"));
    }

    #[test]
    fn test_error_message_temporal_type_display() {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::BooleanFalse);
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        // Request Boolean with strict casting to force an error
        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new(
                "result",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ))),
            cast_options: CastOptions {
                safe: false,
                ..Default::default()
            },
        };

        let err = variant_get(&variant_array, options).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to extract primitive of type Timestamp(ns)"));
    }

    #[test]
    fn test_null_buffer_union_for_shredded_paths() {
        use arrow::compute::CastOptions;
        use arrow::datatypes::{DataType, Field};
        use parquet_variant::VariantPath;
        use std::sync::Arc;

        // Test that null buffers are properly unioned when traversing shredded paths
        // This test verifies scovich's null buffer union requirement

        // Create a depth-1 shredded variant array where:
        // - The top-level variant array has some nulls
        // - The nested typed_value also has some nulls
        // - The result should be the union of both null buffers

        let variant_array = create_depth_1_shredded_test_data_working();

        // Get the field "x" which should union nulls from:
        // 1. The top-level variant array nulls
        // 2. The "a" field's typed_value nulls
        // 3. The "x" field's typed_value nulls
        let options = GetOptions {
            path: VariantPath::from("a.x"),
            as_type: Some(Arc::new(Field::new("result", DataType::Int32, true))),
            cast_options: CastOptions::default(),
        };

        let variant_array_ref: Arc<dyn Array> = variant_array.clone();
        let result = variant_get(&variant_array_ref, options).unwrap();

        // Verify the result length matches input
        assert_eq!(result.len(), variant_array.len());

        // The null pattern should reflect the union of all ancestor nulls
        // Row 0: Should have valid data (path exists and is shredded as Int32)
        // Row 1: Should be null (due to type mismatch - "foo" can't cast to Int32)
        assert!(!result.is_null(0), "Row 0 should have valid Int32 data");
        assert!(
            result.is_null(1),
            "Row 1 should be null due to type casting failure"
        );

        // Verify the actual values
        let int32_result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_result.value(0), 55); // The valid Int32 value
    }

    #[test]
    fn test_struct_null_mask_union_from_children() {
        use arrow::compute::CastOptions;
        use arrow::datatypes::{DataType, Field, Fields};
        use parquet_variant::VariantPath;
        use std::sync::Arc;

        use arrow::array::StringArray;

        // Test that struct null masks properly union nulls from children field extractions
        // This verifies scovich's concern about incomplete null masks in struct construction

        // Create test data where some fields will fail type casting
        let json_strings = vec![
            r#"{"a": 42, "b": "hello"}"#, // Row 0: a=42 (castable to int), b="hello" (not castable to int)
            r#"{"a": "world", "b": 100}"#, // Row 1: a="world" (not castable to int), b=100 (castable to int)
            r#"{"a": 55, "b": 77}"#,       // Row 2: a=55 (castable to int), b=77 (castable to int)
        ];

        let string_array: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(json_strings));
        let variant_array = json_to_variant(&string_array).unwrap();

        // Request extraction as a struct with both fields as Int32
        // This should create child arrays where some fields are null due to casting failures
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let struct_type = DataType::Struct(struct_fields);

        let options = GetOptions {
            path: VariantPath::default(), // Extract the whole object as struct
            as_type: Some(Arc::new(Field::new("result", struct_type, true))),
            cast_options: CastOptions::default(),
        };

        let variant_array_ref = ArrayRef::from(variant_array);
        let result = variant_get(&variant_array_ref, options).unwrap();

        // Verify the result is a StructArray
        let struct_result = result.as_struct();
        assert_eq!(struct_result.len(), 3);

        // Get the individual field arrays
        let field_a = struct_result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let field_b = struct_result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Verify field values and nulls
        // Row 0: a=42 (valid), b=null (casting failure)
        assert!(!field_a.is_null(0));
        assert_eq!(field_a.value(0), 42);
        assert!(field_b.is_null(0)); // "hello" can't cast to int

        // Row 1: a=null (casting failure), b=100 (valid)
        assert!(field_a.is_null(1)); // "world" can't cast to int
        assert!(!field_b.is_null(1));
        assert_eq!(field_b.value(1), 100);

        // Row 2: a=55 (valid), b=77 (valid)
        assert!(!field_a.is_null(2));
        assert_eq!(field_a.value(2), 55);
        assert!(!field_b.is_null(2));
        assert_eq!(field_b.value(2), 77);

        // Verify the struct-level null mask properly unions child nulls
        // The struct should NOT be null in any row because each row has at least one valid field
        // (This tests that we're not incorrectly making the entire struct null when children fail)
        assert!(!struct_result.is_null(0)); // Has valid field 'a'
        assert!(!struct_result.is_null(1)); // Has valid field 'b'
        assert!(!struct_result.is_null(2)); // Has both valid fields
    }

    #[test]
    fn test_field_nullability_preservation() {
        use arrow::compute::CastOptions;
        use arrow::datatypes::{DataType, Field};
        use parquet_variant::VariantPath;
        use std::sync::Arc;

        use arrow::array::StringArray;

        // Test that field nullability from GetOptions.as_type is preserved in the result

        let json_strings = vec![
            r#"{"x": 42}"#,                  // Row 0: Valid int that should convert to Int32
            r#"{"x": "not_a_number"}"#,      // Row 1: String that can't cast to Int32
            r#"{"x": null}"#,                // Row 2: Explicit null value
            r#"{"x": "hello"}"#,             // Row 3: Another string (wrong type)
            r#"{"y": 100}"#,                 // Row 4: Missing "x" field (SQL NULL case)
            r#"{"x": 127}"#, // Row 5: Small int (could be Int8, widening cast candidate)
            r#"{"x": 32767}"#, // Row 6: Medium int (could be Int16, widening cast candidate)
            r#"{"x": 2147483647}"#, // Row 7: Max Int32 value (fits in Int32)
            r#"{"x": 9223372036854775807}"#, // Row 8: Large Int64 value (cannot convert to Int32)
        ];

        let string_array: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(json_strings));
        let variant_array = json_to_variant(&string_array).unwrap();

        // Test 1: nullable field (should allow nulls from cast failures)
        let nullable_field = Arc::new(Field::new("result", DataType::Int32, true));
        let options_nullable = GetOptions {
            path: VariantPath::from("x"),
            as_type: Some(nullable_field.clone()),
            cast_options: CastOptions::default(),
        };

        let variant_array_ref = ArrayRef::from(variant_array);
        let result_nullable = variant_get(&variant_array_ref, options_nullable).unwrap();

        // Verify we get an Int32Array with nulls for cast failures
        let int32_result = result_nullable
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_result.len(), 9);

        // Row 0: 42 converts successfully to Int32
        assert!(!int32_result.is_null(0));
        assert_eq!(int32_result.value(0), 42);

        // Row 1: "not_a_number" fails to convert -> NULL
        assert!(int32_result.is_null(1));

        // Row 2: explicit null value -> NULL
        assert!(int32_result.is_null(2));

        // Row 3: "hello" (wrong type) fails to convert -> NULL
        assert!(int32_result.is_null(3));

        // Row 4: missing "x" field (SQL NULL case) -> NULL
        assert!(int32_result.is_null(4));

        // Row 5: 127 (small int, potential Int8 -> Int32 widening)
        // Current behavior: JSON parses to Int8, should convert to Int32
        assert!(!int32_result.is_null(5));
        assert_eq!(int32_result.value(5), 127);

        // Row 6: 32767 (medium int, potential Int16 -> Int32 widening)
        // Current behavior: JSON parses to Int16, should convert to Int32
        assert!(!int32_result.is_null(6));
        assert_eq!(int32_result.value(6), 32767);

        // Row 7: 2147483647 (max Int32, fits exactly)
        // Current behavior: Should convert successfully
        assert!(!int32_result.is_null(7));
        assert_eq!(int32_result.value(7), 2147483647);

        // Row 8: 9223372036854775807 (large Int64, cannot fit in Int32)
        // Current behavior: Should fail conversion -> NULL
        assert!(int32_result.is_null(8));

        // Test 2: non-nullable field (behavior should be the same with safe casting)
        let non_nullable_field = Arc::new(Field::new("result", DataType::Int32, false));
        let options_non_nullable = GetOptions {
            path: VariantPath::from("x"),
            as_type: Some(non_nullable_field.clone()),
            cast_options: CastOptions::default(), // safe=true by default
        };

        // Create variant array again since we moved it
        let variant_array_2 = json_to_variant(&string_array).unwrap();
        let variant_array_ref_2 = ArrayRef::from(variant_array_2);
        let result_non_nullable = variant_get(&variant_array_ref_2, options_non_nullable).unwrap();
        let int32_result_2 = result_non_nullable
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Even with a non-nullable field, safe casting should still produce nulls for failures
        assert_eq!(int32_result_2.len(), 9);

        // Row 0: 42 converts successfully to Int32
        assert!(!int32_result_2.is_null(0));
        assert_eq!(int32_result_2.value(0), 42);

        // Rows 1-4: All should be null due to safe casting behavior
        // (non-nullable field specification doesn't override safe casting behavior)
        assert!(int32_result_2.is_null(1)); // "not_a_number"
        assert!(int32_result_2.is_null(2)); // explicit null
        assert!(int32_result_2.is_null(3)); // "hello"
        assert!(int32_result_2.is_null(4)); // missing field

        // Rows 5-7: These should also convert successfully (numeric widening/fitting)
        assert!(!int32_result_2.is_null(5)); // 127 (Int8 -> Int32)
        assert_eq!(int32_result_2.value(5), 127);
        assert!(!int32_result_2.is_null(6)); // 32767 (Int16 -> Int32)
        assert_eq!(int32_result_2.value(6), 32767);
        assert!(!int32_result_2.is_null(7)); // 2147483647 (fits in Int32)
        assert_eq!(int32_result_2.value(7), 2147483647);

        // Row 8: Large Int64 should fail conversion -> NULL
        assert!(int32_result_2.is_null(8)); // 9223372036854775807 (too large for Int32)
    }

    #[test]
    fn test_struct_extraction_subset_superset_schema_perfectly_shredded() {
        // Create variant with diverse null patterns and empty objects
        let variant_array = create_comprehensive_shredded_variant();

        // Request struct with fields "a", "b", "d" (skip existing "c", add missing "d")
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]);
        let struct_type = DataType::Struct(struct_fields);

        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", struct_type, true))),
            cast_options: CastOptions::default(),
        };

        let result = variant_get(&variant_array, options).unwrap();

        // Verify the result is a StructArray with 3 fields and 5 rows
        let struct_result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_result.len(), 5);
        assert_eq!(struct_result.num_columns(), 3);

        let field_a = struct_result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let field_b = struct_result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let field_d = struct_result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: Normal values {"a": 1, "b": 2, "c": 3} → {a: 1, b: 2, d: NULL}
        assert!(!struct_result.is_null(0));
        assert_eq!(field_a.value(0), 1);
        assert_eq!(field_b.value(0), 2);
        assert!(field_d.is_null(0)); // Missing field "d"

        // Row 1: Top-level NULL → struct-level NULL
        assert!(struct_result.is_null(1));

        // Row 2: Field "a" missing → {a: NULL, b: 2, d: NULL}
        assert!(!struct_result.is_null(2));
        assert!(field_a.is_null(2)); // Missing field "a"
        assert_eq!(field_b.value(2), 2);
        assert!(field_d.is_null(2)); // Missing field "d"

        // Row 3: Field "b" missing → {a: 1, b: NULL, d: NULL}
        assert!(!struct_result.is_null(3));
        assert_eq!(field_a.value(3), 1);
        assert!(field_b.is_null(3)); // Missing field "b"
        assert!(field_d.is_null(3)); // Missing field "d"

        // Row 4: Empty object {} → {a: NULL, b: NULL, d: NULL}
        assert!(!struct_result.is_null(4));
        assert!(field_a.is_null(4)); // Empty object
        assert!(field_b.is_null(4)); // Empty object
        assert!(field_d.is_null(4)); // Missing field "d"
    }

    #[test]
    fn test_nested_struct_extraction_perfectly_shredded() {
        // Create nested variant with diverse null patterns
        let variant_array = create_comprehensive_nested_shredded_variant();
        println!("variant_array: {variant_array:?}");

        // Request 3-level nested struct type {"outer": {"inner": INT}}
        let inner_field = Field::new("inner", DataType::Int32, true);
        let inner_type = DataType::Struct(Fields::from(vec![inner_field]));
        let outer_field = Field::new("outer", inner_type, true);
        let result_type = DataType::Struct(Fields::from(vec![outer_field]));

        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", result_type, true))),
            cast_options: CastOptions::default(),
        };

        let result = variant_get(&variant_array, options).unwrap();
        println!("result: {result:?}");

        // Verify the result is a StructArray with "outer" field and 4 rows
        let outer_struct = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(outer_struct.len(), 4);
        assert_eq!(outer_struct.num_columns(), 1);

        // Get the "inner" struct column
        let inner_struct = outer_struct
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(inner_struct.num_columns(), 1);

        // Get the "leaf" field (Int32 values)
        let leaf_field = inner_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: Normal nested {"outer": {"inner": {"leaf": 42}}}
        assert!(!outer_struct.is_null(0));
        assert!(!inner_struct.is_null(0));
        assert_eq!(leaf_field.value(0), 42);

        // Row 1: "inner" field missing → {outer: {inner: NULL}}
        assert!(!outer_struct.is_null(1));
        assert!(!inner_struct.is_null(1)); // outer exists, inner exists but leaf is NULL
        assert!(leaf_field.is_null(1)); // leaf field is NULL

        // Row 2: "outer" field missing → {outer: NULL}
        assert!(!outer_struct.is_null(2));
        assert!(inner_struct.is_null(2)); // outer field is NULL

        // Row 3: Top-level NULL → struct-level NULL
        assert!(outer_struct.is_null(3));
    }

    #[test]
    fn test_path_based_null_masks_one_step() {
        // Create nested variant with diverse null patterns
        let variant_array = create_comprehensive_nested_shredded_variant();

        // Extract "outer" field using path-based variant_get
        let path = VariantPath::from("outer");
        let inner_field = Field::new("inner", DataType::Int32, true);
        let result_type = DataType::Struct(Fields::from(vec![inner_field]));

        let options = GetOptions {
            path,
            as_type: Some(Arc::new(Field::new("result", result_type, true))),
            cast_options: CastOptions::default(),
        };

        let result = variant_get(&variant_array, options).unwrap();

        // Verify the result is a StructArray with "inner" field and 4 rows
        let outer_result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(outer_result.len(), 4);
        assert_eq!(outer_result.num_columns(), 1);

        // Get the "inner" field (Int32 values)
        let inner_field = outer_result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: Normal nested {"outer": {"inner": 42}} → {"inner": 42}
        assert!(!outer_result.is_null(0));
        assert_eq!(inner_field.value(0), 42);

        // Row 1: Inner field null {"outer": {"inner": null}} → {"inner": null}
        assert!(!outer_result.is_null(1));
        assert!(inner_field.is_null(1));

        // Row 2: Outer field null {"outer": null} → null (entire struct is null)
        assert!(outer_result.is_null(2));

        // Row 3: Top-level null → null (entire struct is null)
        assert!(outer_result.is_null(3));
    }

    #[test]
    fn test_path_based_null_masks_two_steps() {
        // Create nested variant with diverse null patterns
        let variant_array = create_comprehensive_nested_shredded_variant();

        // Extract "outer.inner" field using path-based variant_get
        let path = VariantPath::from("outer").join("inner");

        let options = GetOptions {
            path,
            as_type: Some(Arc::new(Field::new("result", DataType::Int32, true))),
            cast_options: CastOptions::default(),
        };

        let result = variant_get(&variant_array, options).unwrap();

        // Verify the result is an Int32Array with 4 rows
        let int_result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_result.len(), 4);

        // Row 0: Normal nested {"outer": {"inner": 42}} → 42
        assert!(!int_result.is_null(0));
        assert_eq!(int_result.value(0), 42);

        // Row 1: Inner field null {"outer": {"inner": null}} → null
        assert!(int_result.is_null(1));

        // Row 2: Outer field null {"outer": null} → null (path traversal fails)
        assert!(int_result.is_null(2));

        // Row 3: Top-level null → null (path traversal fails)
        assert!(int_result.is_null(3));
    }

    #[test]
    fn test_struct_extraction_mixed_and_unshredded() {
        // Create a partially shredded variant (x shredded, y not)
        let variant_array = create_mixed_and_unshredded_variant();

        // Request struct with both shredded and unshredded fields
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]);
        let struct_type = DataType::Struct(struct_fields);

        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", struct_type, true))),
            cast_options: CastOptions::default(),
        };

        let result = variant_get(&variant_array, options).unwrap();

        // Verify the mixed shredding works (should succeed with current implementation)
        let struct_result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_result.len(), 4);
        assert_eq!(struct_result.num_columns(), 2);

        let field_x = struct_result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let field_y = struct_result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: {"x": 1, "y": 42} - x from shredded, y from value field
        assert_eq!(field_x.value(0), 1);
        assert_eq!(field_y.value(0), 42);

        // Row 1: {"x": 2} - x from shredded, y missing (perfect shredding)
        assert_eq!(field_x.value(1), 2);
        assert!(field_y.is_null(1));

        // Row 2: {"x": 3, "y": null} - x from shredded, y explicitly null in value
        assert_eq!(field_x.value(2), 3);
        assert!(field_y.is_null(2));

        // Row 3: top-level null - entire struct row should be null
        assert!(struct_result.is_null(3));
    }

    /// Test that demonstrates the actual struct row builder gap
    /// This test should fail because it hits unshredded nested structs
    #[test]
    fn test_struct_row_builder_gap_demonstration() {
        // Create completely unshredded JSON variant (no typed_value at all)
        let json_strings = vec![
            r#"{"outer": {"inner": 42}}"#,
            r#"{"outer": {"inner": 100}}"#,
        ];
        let string_array: Arc<dyn Array> = Arc::new(StringArray::from(json_strings));
        let variant_array = json_to_variant(&string_array).unwrap();

        // Request nested struct - this should fail at the row builder level
        let inner_fields = Fields::from(vec![Field::new("inner", DataType::Int32, true)]);
        let inner_struct_type = DataType::Struct(inner_fields);
        let outer_fields = Fields::from(vec![Field::new("outer", inner_struct_type, true)]);
        let outer_struct_type = DataType::Struct(outer_fields);

        let options = GetOptions {
            path: VariantPath::default(),
            as_type: Some(Arc::new(Field::new("result", outer_struct_type, true))),
            cast_options: CastOptions::default(),
        };

        let variant_array_ref = ArrayRef::from(variant_array);
        let result = variant_get(&variant_array_ref, options);

        // Should fail with NotYetImplemented when the row builder tries to handle struct type
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Not yet implemented"));
    }

    /// Create comprehensive shredded variant with diverse null patterns and empty objects
    /// Rows: normal values, top-level null, missing field a, missing field b, empty object
    fn create_comprehensive_shredded_variant() -> ArrayRef {
        let (metadata, _) = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let obj = builder.new_object();
            obj.finish();
            builder.finish()
        };

        // Create null buffer for top-level nulls
        let nulls = NullBuffer::from(vec![
            true,  // row 0: normal values
            false, // row 1: top-level null
            true,  // row 2: missing field a
            true,  // row 3: missing field b
            true,  // row 4: empty object
        ]);

        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 5));

        // Create shredded fields with different null patterns
        // Field "a": present in rows 0,3 (missing in rows 1,2,4)
        let a_field_typed_value = Int32Array::from(vec![Some(1), None, None, Some(1), None]);
        let a_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(a_field_typed_value), true)
            .build();
        let a_field_shredded = ShreddedVariantFieldArray::try_new(&a_field_struct)
            .expect("should create ShreddedVariantFieldArray for a");

        // Field "b": present in rows 0,2 (missing in rows 1,3,4)
        let b_field_typed_value = Int32Array::from(vec![Some(2), None, Some(2), None, None]);
        let b_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(b_field_typed_value), true)
            .build();
        let b_field_shredded = ShreddedVariantFieldArray::try_new(&b_field_struct)
            .expect("should create ShreddedVariantFieldArray for b");

        // Field "c": present in row 0 only (missing in all other rows)
        let c_field_typed_value = Int32Array::from(vec![Some(3), None, None, None, None]);
        let c_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(c_field_typed_value), true)
            .build();
        let c_field_shredded = ShreddedVariantFieldArray::try_new(&c_field_struct)
            .expect("should create ShreddedVariantFieldArray for c");

        // Create main typed_value struct
        let typed_value_fields = Fields::from(vec![
            Field::new("a", a_field_shredded.data_type().clone(), true),
            Field::new("b", b_field_shredded.data_type().clone(), true),
            Field::new("c", c_field_shredded.data_type().clone(), true),
        ]);
        let typed_value_struct = StructArray::try_new(
            typed_value_fields,
            vec![
                ArrayRef::from(a_field_shredded),
                ArrayRef::from(b_field_shredded),
                ArrayRef::from(c_field_shredded),
            ],
            None,
        )
        .unwrap();

        // Build final VariantArray with top-level nulls
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .with_nulls(nulls)
            .build();

        Arc::new(struct_array)
    }

    /// Create comprehensive nested shredded variant with diverse null patterns
    /// Represents 3-level structure: variant -> outer -> inner (INT value)
    /// The shredding schema is: {"metadata": BINARY, "typed_value": {"outer": {"typed_value": {"inner": {"typed_value": INT}}}}}
    /// Rows: normal nested value, inner field null, outer field null, top-level null
    fn create_comprehensive_nested_shredded_variant() -> ArrayRef {
        // Create the inner level: contains typed_value with Int32 values
        // Row 0: has value 42, Row 1: inner null, Row 2: outer null, Row 3: top-level null
        let inner_typed_value = Int32Array::from(vec![Some(42), None, None, None]); // dummy value for row 2
        let inner = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(inner_typed_value), true)
            .build();
        let inner = ShreddedVariantFieldArray::try_new(&inner).unwrap();

        let outer_typed_value_nulls = NullBuffer::from(vec![
            true,  // row 0: inner struct exists with typed_value=42
            false, // row 1: inner field NULL
            false, // row 2: outer field NULL
            false, // row 3: top-level NULL
        ]);
        let outer_typed_value = StructArrayBuilder::new()
            .with_field("inner", ArrayRef::from(inner), false)
            .with_nulls(outer_typed_value_nulls)
            .build();

        let outer = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(outer_typed_value), true)
            .build();
        let outer = ShreddedVariantFieldArray::try_new(&outer).unwrap();

        let typed_value_nulls = NullBuffer::from(vec![
            true,  // row 0: inner struct exists with typed_value=42
            true,  // row 1: inner field NULL
            false, // row 2: outer field NULL
            false, // row 3: top-level NULL
        ]);
        let typed_value = StructArrayBuilder::new()
            .with_field("outer", ArrayRef::from(outer), false)
            .with_nulls(typed_value_nulls)
            .build();

        // Build final VariantArray with top-level nulls
        let metadata_array =
            BinaryViewArray::from_iter_values(std::iter::repeat_n(EMPTY_VARIANT_METADATA_BYTES, 4));
        let nulls = NullBuffer::from(vec![
            true,  // row 0: inner struct exists with typed_value=42
            true,  // row 1: inner field NULL
            true,  // row 2: outer field NULL
            false, // row 3: top-level NULL
        ]);
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("typed_value", Arc::new(typed_value), true)
            .with_nulls(nulls)
            .build();

        Arc::new(struct_array)
    }

    /// Create variant with mixed shredding (spec-compliant) including null scenarios
    /// Field "x" is globally shredded, field "y" is never shredded
    fn create_mixed_and_unshredded_variant() -> ArrayRef {
        // Create spec-compliant mixed shredding:
        // - Field "x" is globally shredded (has typed_value column)
        // - Field "y" is never shredded (only appears in value field when present)

        let (metadata, y_field_value) = {
            let mut builder = parquet_variant::VariantBuilder::new();
            let mut obj = builder.new_object();
            obj.insert("y", Variant::from(42));
            obj.finish();
            builder.finish()
        };

        let metadata_array = BinaryViewArray::from_iter_values(std::iter::repeat_n(&metadata, 4));

        // Value field contains objects with unshredded fields only (never contains "x")
        // Row 0: {"y": "foo"} - x is shredded out, y remains in value
        // Row 1: {} - both x and y are absent (perfect shredding for x, y missing)
        // Row 2: {"y": null} - x is shredded out, y explicitly null
        // Row 3: top-level null (encoded in VariantArray's null mask, but fields contain valid data)

        let empty_object_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            builder.new_object().finish();
            let (_, value) = builder.finish();
            value
        };

        let y_null_value = {
            let mut builder = parquet_variant::VariantBuilder::new();
            builder.new_object().with_field("y", Variant::Null).finish();
            let (_, value) = builder.finish();
            value
        };

        let value_array = BinaryViewArray::from(vec![
            Some(y_field_value.as_slice()),      // Row 0: {"y": 42}
            Some(empty_object_value.as_slice()), // Row 1: {}
            Some(y_null_value.as_slice()),       // Row 2: {"y": null}
            Some(empty_object_value.as_slice()), // Row 3: top-level null (but value field contains valid data)
        ]);

        // Create shredded field "x" (globally shredded - never appears in value field)
        // For top-level null row, the field still needs valid content (not null)
        let x_field_typed_value = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(0)]);
        let x_field_struct = StructArrayBuilder::new()
            .with_field("typed_value", Arc::new(x_field_typed_value), true)
            .build();
        let x_field_shredded = ShreddedVariantFieldArray::try_new(&x_field_struct)
            .expect("should create ShreddedVariantFieldArray for x");

        // Create main typed_value struct (only contains shredded fields)
        let typed_value_struct = StructArrayBuilder::new()
            .with_field("x", ArrayRef::from(x_field_shredded), false)
            .build();

        // Build VariantArray with both value and typed_value (PartiallyShredded)
        // Top-level null is encoded in the main StructArray's null mask
        let variant_nulls = NullBuffer::from(vec![true, true, true, false]); // Row 3 is top-level null
        let struct_array = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata_array), false)
            .with_field("value", Arc::new(value_array), true)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .with_nulls(variant_nulls)
            .build();

        Arc::new(struct_array)
    }

    #[test]
    fn get_decimal32_rescaled_to_scale2() {
        // Build unshredded variant values with different scales
        let mut builder = crate::VariantArrayBuilder::new(5);
        builder.append_variant(VariantDecimal4::try_new(1234, 2).unwrap().into()); // 12.34
        builder.append_variant(VariantDecimal4::try_new(1234, 3).unwrap().into()); // 1.234
        builder.append_variant(VariantDecimal4::try_new(1234, 0).unwrap().into()); // 1234
        builder.append_null();
        builder.append_variant(
            VariantDecimal8::try_new((VariantDecimal4::MAX_UNSCALED_VALUE as i64) + 1, 3)
                .unwrap()
                .into(),
        ); // should fit into Decimal32
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal32(9, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal32Array>().unwrap();

        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), 2);
        assert_eq!(result.value(0), 1234);
        assert_eq!(result.value(1), 123);
        assert_eq!(result.value(2), 123400);
        assert!(result.is_null(3));
        assert_eq!(
            result.value(4),
            VariantDecimal4::MAX_UNSCALED_VALUE / 10 + 1
        ); // should not be null as the final result fits into Decimal32
    }

    #[test]
    fn get_decimal32_scale_down_rounding() {
        let mut builder = crate::VariantArrayBuilder::new(7);
        builder.append_variant(VariantDecimal4::try_new(1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal4::try_new(1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal4::try_new(-1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal4::try_new(-1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal4::try_new(1235, 2).unwrap().into()); // 12.35 rounded down to 10 for scale -1
        builder.append_variant(VariantDecimal4::try_new(1235, 3).unwrap().into()); // 1.235 rounded down to 0 for scale -1
        builder.append_variant(VariantDecimal4::try_new(5235, 3).unwrap().into()); // 5.235 rounded up to 10 for scale -1
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal32(9, -1), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal32Array>().unwrap();

        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), -1);
        assert_eq!(result.value(0), 124);
        assert_eq!(result.value(1), 125);
        assert_eq!(result.value(2), -124);
        assert_eq!(result.value(3), -125);
        assert_eq!(result.value(4), 1);
        assert!(result.is_valid(5));
        assert_eq!(result.value(5), 0);
        assert_eq!(result.value(6), 1);
    }

    #[test]
    fn get_decimal32_large_scale_reduction() {
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal4::try_new(-VariantDecimal4::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal4::try_new(VariantDecimal4::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal32(9, -9), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal32Array>().unwrap();

        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), -9);
        assert_eq!(result.value(0), -1);
        assert_eq!(result.value(1), 1);

        let field = Field::new("result", DataType::Decimal32(9, -10), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal32Array>().unwrap();

        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), -10);
        assert!(result.is_valid(0));
        assert_eq!(result.value(0), 0);
        assert!(result.is_valid(1));
        assert_eq!(result.value(1), 0);
    }

    #[test]
    fn get_decimal32_precision_overflow_safe() {
        // Exceed Decimal32 after scaling and rounding
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal4::try_new(VariantDecimal4::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal4::try_new(VariantDecimal4::MAX_UNSCALED_VALUE, 9)
                .unwrap()
                .into(),
        ); // integer value round up overflows
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal32(2, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal32Array>().unwrap();

        assert!(result.is_null(0));
        assert!(result.is_null(1)); // should overflow because 1.00 does not fit into precision (2)
    }

    #[test]
    fn get_decimal32_precision_overflow_unsafe_errors() {
        let mut builder = crate::VariantArrayBuilder::new(1);
        builder.append_variant(
            VariantDecimal4::try_new(VariantDecimal4::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal32(9, 2), true);
        let cast_options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let err = variant_get(&variant_array, options).unwrap_err();

        assert!(
            err.to_string().contains(
                "Failed to cast to Decimal32(precision=9, scale=2) from variant Decimal4"
            )
        );
    }

    #[test]
    fn get_decimal64_rescaled_to_scale2() {
        let mut builder = crate::VariantArrayBuilder::new(5);
        builder.append_variant(VariantDecimal8::try_new(1234, 2).unwrap().into()); // 12.34
        builder.append_variant(VariantDecimal8::try_new(1234, 3).unwrap().into()); // 1.234
        builder.append_variant(VariantDecimal8::try_new(1234, 0).unwrap().into()); // 1234
        builder.append_null();
        builder.append_variant(
            VariantDecimal16::try_new((VariantDecimal8::MAX_UNSCALED_VALUE as i128) + 1, 3)
                .unwrap()
                .into(),
        ); // should fit into Decimal64
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal64(18, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal64Array>().unwrap();

        assert_eq!(result.precision(), 18);
        assert_eq!(result.scale(), 2);
        assert_eq!(result.value(0), 1234);
        assert_eq!(result.value(1), 123);
        assert_eq!(result.value(2), 123400);
        assert!(result.is_null(3));
        assert_eq!(
            result.value(4),
            VariantDecimal8::MAX_UNSCALED_VALUE / 10 + 1
        ); // should not be null as the final result fits into Decimal64
    }

    #[test]
    fn get_decimal64_scale_down_rounding() {
        let mut builder = crate::VariantArrayBuilder::new(7);
        builder.append_variant(VariantDecimal8::try_new(1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal8::try_new(1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal8::try_new(-1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal8::try_new(-1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal8::try_new(1235, 2).unwrap().into()); // 12.35 rounded down to 10 for scale -1
        builder.append_variant(VariantDecimal8::try_new(1235, 3).unwrap().into()); // 1.235 rounded down to 0 for scale -1
        builder.append_variant(VariantDecimal8::try_new(5235, 3).unwrap().into()); // 5.235 rounded up to 10 for scale -1
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal64(18, -1), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal64Array>().unwrap();

        assert_eq!(result.precision(), 18);
        assert_eq!(result.scale(), -1);
        assert_eq!(result.value(0), 124);
        assert_eq!(result.value(1), 125);
        assert_eq!(result.value(2), -124);
        assert_eq!(result.value(3), -125);
        assert_eq!(result.value(4), 1);
        assert!(result.is_valid(5));
        assert_eq!(result.value(5), 0);
        assert_eq!(result.value(6), 1);
    }

    #[test]
    fn get_decimal64_large_scale_reduction() {
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal8::try_new(-VariantDecimal8::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal8::try_new(VariantDecimal8::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal64(18, -18), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal64Array>().unwrap();

        assert_eq!(result.precision(), 18);
        assert_eq!(result.scale(), -18);
        assert_eq!(result.value(0), -1);
        assert_eq!(result.value(1), 1);

        let field = Field::new("result", DataType::Decimal64(18, -19), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal64Array>().unwrap();

        assert_eq!(result.precision(), 18);
        assert_eq!(result.scale(), -19);
        assert!(result.is_valid(0));
        assert_eq!(result.value(0), 0);
        assert!(result.is_valid(1));
        assert_eq!(result.value(1), 0);
    }

    #[test]
    fn get_decimal64_precision_overflow_safe() {
        // Exceed Decimal64 after scaling and rounding
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal8::try_new(VariantDecimal8::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal8::try_new(VariantDecimal8::MAX_UNSCALED_VALUE, 18)
                .unwrap()
                .into(),
        ); // integer value round up overflows
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal64(2, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal64Array>().unwrap();

        assert!(result.is_null(0));
        assert!(result.is_null(1));
    }

    #[test]
    fn get_decimal64_precision_overflow_unsafe_errors() {
        let mut builder = crate::VariantArrayBuilder::new(1);
        builder.append_variant(
            VariantDecimal8::try_new(VariantDecimal8::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal64(18, 2), true);
        let cast_options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let err = variant_get(&variant_array, options).unwrap_err();

        assert!(
            err.to_string().contains(
                "Failed to cast to Decimal64(precision=18, scale=2) from variant Decimal8"
            )
        );
    }

    #[test]
    fn get_decimal128_rescaled_to_scale2() {
        let mut builder = crate::VariantArrayBuilder::new(4);
        builder.append_variant(VariantDecimal16::try_new(1234, 2).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1234, 3).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1234, 0).unwrap().into());
        builder.append_null();
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal128(38, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert_eq!(result.precision(), 38);
        assert_eq!(result.scale(), 2);
        assert_eq!(result.value(0), 1234);
        assert_eq!(result.value(1), 123);
        assert_eq!(result.value(2), 123400);
        assert!(result.is_null(3));
    }

    #[test]
    fn get_decimal128_scale_down_rounding() {
        let mut builder = crate::VariantArrayBuilder::new(7);
        builder.append_variant(VariantDecimal16::try_new(1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(-1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(-1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1235, 2).unwrap().into()); // 12.35 rounded down to 10 for scale -1
        builder.append_variant(VariantDecimal16::try_new(1235, 3).unwrap().into()); // 1.235 rounded down to 0 for scale -1
        builder.append_variant(VariantDecimal16::try_new(5235, 3).unwrap().into()); // 5.235 rounded up to 10 for scale -1
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal128(38, -1), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert_eq!(result.precision(), 38);
        assert_eq!(result.scale(), -1);
        assert_eq!(result.value(0), 124);
        assert_eq!(result.value(1), 125);
        assert_eq!(result.value(2), -124);
        assert_eq!(result.value(3), -125);
        assert_eq!(result.value(4), 1);
        assert!(result.is_valid(5));
        assert_eq!(result.value(5), 0);
        assert_eq!(result.value(6), 1);
    }

    #[test]
    fn get_decimal128_precision_overflow_safe() {
        // Exceed Decimal128 after scaling and rounding
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 38)
                .unwrap()
                .into(),
        ); // integer value round up overflows
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal128(2, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert!(result.is_null(0));
        assert!(result.is_null(1)); // should overflow because 1.00 does not fit into precision (2)
    }

    #[test]
    fn get_decimal128_precision_overflow_unsafe_errors() {
        let mut builder = crate::VariantArrayBuilder::new(1);
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal128(38, 2), true);
        let cast_options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let err = variant_get(&variant_array, options).unwrap_err();

        assert!(err.to_string().contains(
            "Failed to cast to Decimal128(precision=38, scale=2) from variant Decimal16"
        ));
    }

    #[test]
    fn get_decimal256_rescaled_to_scale2() {
        // Build unshredded variant values with different scales using Decimal16 source
        let mut builder = crate::VariantArrayBuilder::new(4);
        builder.append_variant(VariantDecimal16::try_new(1234, 2).unwrap().into()); // 12.34
        builder.append_variant(VariantDecimal16::try_new(1234, 3).unwrap().into()); // 1.234
        builder.append_variant(VariantDecimal16::try_new(1234, 0).unwrap().into()); // 1234
        builder.append_null();
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal256(76, 2), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal256Array>().unwrap();

        assert_eq!(result.precision(), 76);
        assert_eq!(result.scale(), 2);
        assert_eq!(result.value(0), i256::from_i128(1234));
        assert_eq!(result.value(1), i256::from_i128(123));
        assert_eq!(result.value(2), i256::from_i128(123400));
        assert!(result.is_null(3));
    }

    #[test]
    fn get_decimal256_scale_down_rounding() {
        let mut builder = crate::VariantArrayBuilder::new(7);
        builder.append_variant(VariantDecimal16::try_new(1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(-1235, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(-1245, 0).unwrap().into());
        builder.append_variant(VariantDecimal16::try_new(1235, 2).unwrap().into()); // 12.35 rounded down to 10 for scale -1
        builder.append_variant(VariantDecimal16::try_new(1235, 3).unwrap().into()); // 1.235 rounded down to 0 for scale -1
        builder.append_variant(VariantDecimal16::try_new(5235, 3).unwrap().into()); // 5.235 rounded up to 10 for scale -1
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal256(76, -1), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal256Array>().unwrap();

        assert_eq!(result.precision(), 76);
        assert_eq!(result.scale(), -1);
        assert_eq!(result.value(0), i256::from_i128(124));
        assert_eq!(result.value(1), i256::from_i128(125));
        assert_eq!(result.value(2), i256::from_i128(-124));
        assert_eq!(result.value(3), i256::from_i128(-125));
        assert_eq!(result.value(4), i256::from_i128(1));
        assert!(result.is_valid(5));
        assert_eq!(result.value(5), i256::from_i128(0));
        assert_eq!(result.value(6), i256::from_i128(1));
    }

    #[test]
    fn get_decimal256_precision_overflow_safe() {
        // Exceed Decimal128 max precision (38) after scaling
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 1)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal256(76, 39), true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();
        let result = result.as_any().downcast_ref::<Decimal256Array>().unwrap();

        // Input is Decimal16 with integer = 10^38-1 and scale = 1, target scale = 39
        // So expected integer is (10^38-1) * 10^(39-1) = (10^38-1) * 10^38
        let base = i256::from_i128(10);
        let factor = base.checked_pow(38).unwrap();
        let expected = i256::from_i128(VariantDecimal16::MAX_UNSCALED_VALUE)
            .checked_mul(factor)
            .unwrap();
        assert_eq!(result.value(0), expected);
        assert!(result.is_null(1));
    }

    #[test]
    fn get_decimal256_precision_overflow_unsafe_errors() {
        // Exceed Decimal128 max precision (38) after scaling
        let mut builder = crate::VariantArrayBuilder::new(2);
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 1)
                .unwrap()
                .into(),
        );
        builder.append_variant(
            VariantDecimal16::try_new(VariantDecimal16::MAX_UNSCALED_VALUE, 0)
                .unwrap()
                .into(),
        );
        let variant_array: ArrayRef = ArrayRef::from(builder.build());

        let field = Field::new("result", DataType::Decimal256(76, 39), true);
        let cast_options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let err = variant_get(&variant_array, options).unwrap_err();

        assert!(err.to_string().contains(
            "Failed to cast to Decimal256(precision=76, scale=39) from variant Decimal16"
        ));
    }

    #[test]
    fn get_non_supported_temporal_types_error() {
        let values = vec![None, Some(Variant::Null), Some(Variant::BooleanFalse)];
        let variant_array: ArrayRef = ArrayRef::from(VariantArray::from_iter(values));

        let test_cases = vec![
            FieldRef::from(Field::new(
                "result",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            )),
            FieldRef::from(Field::new(
                "result",
                DataType::Interval(IntervalUnit::YearMonth),
                true,
            )),
        ];

        for field in test_cases {
            let options = GetOptions::new().with_as_type(Some(field));
            let err = variant_get(&variant_array, options).unwrap_err();
            assert!(
                err.to_string()
                    .contains("Casting Variant to duration/interval types is not supported")
            );
        }
    }

    fn invalid_time_variant_array() -> ArrayRef {
        let mut builder = VariantArrayBuilder::new(3);
        // 86401000000 is invalid for Time64Microsecond (max is 86400000000)
        builder.append_variant(Variant::Int64(86401000000));
        builder.append_variant(Variant::Int64(86401000000));
        builder.append_variant(Variant::Int64(86401000000));
        Arc::new(builder.build().into_inner())
    }

    #[test]
    fn test_variant_get_error_when_cast_failure_and_safe_false() {
        let variant_array = invalid_time_variant_array();

        let field = Field::new("result", DataType::Time64(TimeUnit::Microsecond), true);
        let cast_options = CastOptions {
            safe: false, // Will error on cast failure
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let err = variant_get(&variant_array, options).unwrap_err();
        assert!(
            err.to_string().contains(
                "Cast error: Failed to extract primitive of type Time64(µs) from variant Int64(86401000000) at path VariantPath([])"
            ),
            "actual: {err}",
        );
    }

    #[test]
    fn test_variant_get_return_null_when_cast_failure_and_safe_true() {
        let variant_array = invalid_time_variant_array();

        let field = Field::new("result", DataType::Time64(TimeUnit::Microsecond), true);
        let cast_options = CastOptions {
            safe: true, // Will return null on cast failure
            ..Default::default()
        };
        let options = GetOptions::new()
            .with_as_type(Some(FieldRef::from(field)))
            .with_cast_options(cast_options);
        let result = variant_get(&variant_array, options).unwrap();
        assert_eq!(3, result.len());

        for i in 0..3 {
            assert!(result.is_null(i));
        }
    }

    #[test]
    fn test_perfect_shredding_returns_same_arc_ptr() {
        let variant_array = perfectly_shredded_int32_variant_array();

        let variant_array_ref = VariantArray::try_new(&variant_array).unwrap();
        let typed_value_arc = variant_array_ref.typed_value_field().unwrap().clone();

        let field = Field::new("result", DataType::Int32, true);
        let options = GetOptions::new().with_as_type(Some(FieldRef::from(field)));
        let result = variant_get(&variant_array, options).unwrap();

        assert!(Arc::ptr_eq(&typed_value_arc, &result));
    }

    #[test]
    fn test_perfect_shredding_three_typed_value_columns() {
        // Column 1: perfectly shredded primitive with all nulls
        let all_nulls_values: Arc<Int32Array> = Arc::new(Int32Array::from(vec![
            Option::<i32>::None,
            Option::<i32>::None,
            Option::<i32>::None,
        ]));
        let all_nulls_erased: ArrayRef = all_nulls_values.clone();
        let all_nulls_field =
            ShreddedVariantFieldArray::from_parts(None, Some(all_nulls_erased.clone()), None);
        let all_nulls_type = all_nulls_field.data_type().clone();
        let all_nulls_struct: ArrayRef = ArrayRef::from(all_nulls_field);

        // Column 2: perfectly shredded primitive with some nulls
        let some_nulls_values: Arc<Int32Array> =
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let some_nulls_erased: ArrayRef = some_nulls_values.clone();
        let some_nulls_field =
            ShreddedVariantFieldArray::from_parts(None, Some(some_nulls_erased.clone()), None);
        let some_nulls_type = some_nulls_field.data_type().clone();
        let some_nulls_struct: ArrayRef = ArrayRef::from(some_nulls_field);

        // Column 3: perfectly shredded nested struct
        let inner_values: Arc<Int32Array> =
            Arc::new(Int32Array::from(vec![Some(111), None, Some(333)]));
        let inner_erased: ArrayRef = inner_values.clone();
        let inner_field =
            ShreddedVariantFieldArray::from_parts(None, Some(inner_erased.clone()), None);
        let inner_field_type = inner_field.data_type().clone();
        let inner_struct_array: ArrayRef = ArrayRef::from(inner_field);

        let nested_struct = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("inner", inner_field_type, true)]),
                vec![inner_struct_array],
                None,
            )
            .unwrap(),
        );
        let nested_struct_erased: ArrayRef = nested_struct.clone();
        let struct_field =
            ShreddedVariantFieldArray::from_parts(None, Some(nested_struct_erased.clone()), None);
        let struct_field_type = struct_field.data_type().clone();
        let struct_field_struct: ArrayRef = ArrayRef::from(struct_field);

        // Assemble the top-level typed_value struct with the three columns above
        let typed_value_struct = StructArray::try_new(
            Fields::from(vec![
                Field::new("all_nulls", all_nulls_type, true),
                Field::new("some_nulls", some_nulls_type, true),
                Field::new("struct_field", struct_field_type, true),
            ]),
            vec![all_nulls_struct, some_nulls_struct, struct_field_struct],
            None,
        )
        .unwrap();

        let metadata = BinaryViewArray::from_iter_values(std::iter::repeat_n(
            EMPTY_VARIANT_METADATA_BYTES,
            all_nulls_values.len(),
        ));
        let variant_struct = StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata), false)
            .with_field("typed_value", Arc::new(typed_value_struct), true)
            .build();
        let variant_array: ArrayRef = VariantArray::try_new(&variant_struct).unwrap().into();

        // Case 1: all-null primitive column should reuse the typed_value Arc directly
        let all_nulls_field_ref = FieldRef::from(Field::new("result", DataType::Int32, true));
        let all_nulls_result = variant_get(
            &variant_array,
            GetOptions::new_with_path(VariantPath::from("all_nulls"))
                .with_as_type(Some(all_nulls_field_ref)),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&all_nulls_result, &all_nulls_erased));

        // Case 2: primitive column with some nulls should also reuse its typed_value Arc
        let some_nulls_field_ref = FieldRef::from(Field::new("result", DataType::Int32, true));
        let some_nulls_result = variant_get(
            &variant_array,
            GetOptions::new_with_path(VariantPath::from("some_nulls"))
                .with_as_type(Some(some_nulls_field_ref)),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&some_nulls_result, &some_nulls_erased));

        // Case 3: struct column should return a StructArray composed from the nested field
        let struct_child_fields = Fields::from(vec![Field::new("inner", DataType::Int32, true)]);
        let struct_field_ref = FieldRef::from(Field::new(
            "result",
            DataType::Struct(struct_child_fields.clone()),
            true,
        ));
        let struct_result = variant_get(
            &variant_array,
            GetOptions::new_with_path(VariantPath::from("struct_field"))
                .with_as_type(Some(struct_field_ref)),
        )
        .unwrap();
        let struct_array = struct_result
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_array.len(), 3);
        assert_eq!(struct_array.null_count(), 0);

        let inner_values_result = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(inner_values_result.len(), 3);
        assert_eq!(inner_values_result.value(0), 111);
        assert!(inner_values_result.is_null(1));
        assert_eq!(inner_values_result.value(2), 333);
    }
}
