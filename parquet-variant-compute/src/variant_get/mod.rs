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
    error::Result,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use parquet_variant::{VariantPath, VariantPathElement};

use crate::variant_array::ShreddingState;
use crate::{variant_array::ShreddedVariantFieldArray, VariantArray};

use std::sync::Arc;

mod output;

pub(crate) enum ShreddedPathStep<'a> {
    /// Path step succeeded, return the new shredding state
    Success(&'a ShreddingState),
    /// The path element is not present in the `typed_value` column and there is no `value` column,
    /// so we we know it does not exist. It, and all paths under it, are all-NULL.
    Missing,
    /// The path element is not present in the `typed_value` and must be retrieved from the `value`
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
    shredding_state: &'a ShreddingState,
    path_element: &VariantPathElement<'_>,
) -> Result<ShreddedPathStep<'a>> {
    // If the requested path element 's not present in `typed_value`, and `value` is missing, then
    // we know it does not exist; it, and all paths under it, are all-NULL.
    let missing_path_step = || {
        if shredding_state.value_field().is_none() {
            ShreddedPathStep::Missing
        } else {
            ShreddedPathStep::NotShredded
        }
    };

    let Some(typed_value) = shredding_state.typed_value_field() else {
        return Ok(missing_path_step());
    };

    match path_element {
        VariantPathElement::Field { name } => {
            // Try to step into the requested field name of a struct.
            let Some(field) = typed_value
                .as_any()
                .downcast_ref::<StructArray>()
                .and_then(|typed_value| typed_value.column_by_name(name))
            else {
                return Ok(missing_path_step());
            };

            let field = field
                .as_any()
                .downcast_ref::<ShreddedVariantFieldArray>()
                .ok_or_else(|| {
                    // TODO: Should we blow up? Or just end the traversal and let the normal
                    // variant pathing code sort out the mess that it must anyway be
                    // prepared to handle?
                    ArrowError::InvalidArgumentError(format!(
                        "Expected a ShreddedVariantFieldArray, got {:?} instead",
                        field.data_type(),
                    ))
                })?;

            Ok(ShreddedPathStep::Success(field.shredding_state()))
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
    as_type: Option<&DataType>,
) -> Result<ArrayRef> {
    // Helper that creates a new VariantArray from the given nested value and typed_value columns,
    let make_target_variant = |value: Option<BinaryViewArray>, typed_value: Option<ArrayRef>| {
        let metadata = input.metadata_field().clone();
        let nulls = input.inner().nulls().cloned();
        VariantArray::from_parts(
            metadata,
            value,
            typed_value,
            nulls,
        )
    };

    // Helper that shreds a VariantArray to a specific type.
    let shred_basic_variant = |target: VariantArray, path: VariantPath<'_>, as_type: Option<&DataType>| {
        let mut builder = output::struct_output::make_shredding_row_builder(path, as_type)?;
        for i in 0..target.len() {
            if target.is_null(i) {
                builder.append_null()?;
            } else {
                builder.append_value(&target.value(i))?;
            }
        }
        builder.finish()
    };

    // Peel away the prefix of path elements that traverses the shredded parts of this variant
    // column. Shredding will traverse the rest of the path on a per-row basis.
    let mut shredding_state = input.shredding_state();
    let mut path_index = 0;
    for path_element in path {
        match follow_shredded_path_element(shredding_state, path_element)? {
            ShreddedPathStep::Success(state) => {
                shredding_state = state;
                path_index += 1;
                continue;
            }
            ShreddedPathStep::Missing => {
                let num_rows = input.len();
                let arr = match as_type {
                    Some(data_type) => Arc::new(array::new_null_array(data_type, num_rows)) as _,
                    None => Arc::new(array::NullArray::new(num_rows)) as _,
                };
                return Ok(arr);
            }
            ShreddedPathStep::NotShredded => {
                let target = make_target_variant(shredding_state.value_field().cloned(), None);
                return shred_basic_variant(target, path[path_index..].into(), as_type);
            }
        };
    }

    // Path exhausted! Create a new `VariantArray` for the location we landed on.
    let target = make_target_variant(
        shredding_state.value_field().cloned(),
        shredding_state.typed_value_field().cloned(),
    );

    // If our caller did not request any specific type, we can just return whatever we landed on.
    let Some(data_type) = as_type else {
        return Ok(Arc::new(target));
    };

    // Structs are special. Recurse into each field separately, hoping to follow the shredding even
    // further, and build up the final struct from those individually shredded results.
    if let DataType::Struct(fields) = data_type {
        let children = fields
            .iter()
            .map(|field| {
                shredded_get_path(
                    &target,
                    &[VariantPathElement::from(field.name().as_str())],
                    Some(field.data_type()),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        return Ok(Arc::new(StructArray::try_new(
            fields.clone(),
            children,
            target.nulls().cloned(),
        )?));
    }

    // Not a struct, so directly shred the variant as the requested type
    shred_basic_variant(target, VariantPath::default(), as_type)
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
    let variant_array: &VariantArray = input.as_any().downcast_ref().ok_or_else(|| {
        ArrowError::InvalidArgumentError(
            "expected a VariantArray as the input for variant_get".to_owned(),
        )
    })?;

    let GetOptions { as_type, path, .. } = &options;

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

    use arrow::array::{Array, ArrayRef, BinaryViewArray, Int32Array, StringArray};
    use arrow::buffer::NullBuffer;
    use arrow::compute::CastOptions;
    use arrow_schema::{DataType, Field, FieldRef};
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
        assert_eq!(result.value(2), Variant::from("n/a"));
        assert_eq!(result.value(3), Variant::Int32(100));
    }

    /// Shredding: extract a value as an Int32Array
    #[test]
    fn get_variant_shredded_int32_as_int32_safe_cast() {
        // Extract the typed value as Int32Array
        let array = shredded_int32_variant_array();
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
        let array = shredded_int32_variant_array();
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

        let struct_array = crate::variant_array::StructArrayBuilder::new()
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

        let struct_array = crate::variant_array::StructArrayBuilder::new()
            .with_field("metadata", Arc::new(metadata))
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
