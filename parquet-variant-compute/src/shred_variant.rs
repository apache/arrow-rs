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

//! Module for shredding VariantArray with a given schema.

use crate::variant_array::{ShreddedVariantFieldArray, StructArrayBuilder};
use crate::variant_to_arrow::{
    PrimitiveVariantToArrowRowBuilder, make_primitive_variant_to_arrow_row_builder,
};
use crate::{VariantArray, VariantValueArrayBuilder};
use arrow::array::{ArrayRef, BinaryViewArray, NullBufferBuilder};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Fields};
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantBuilderExt};

use indexmap::IndexMap;
use std::sync::Arc;

/// Shreds the input binary variant using a target shredding schema derived from the requested data type.
///
/// For example, requesting `DataType::Int64` would produce an output variant array with the schema:
///
/// ```text
/// {
///    metadata: BINARY,
///    value: BINARY,
///    typed_value: LONG,
/// }
/// ```
///
/// Similarly, requesting `DataType::Struct` with two integer fields `a` and `b` would produce an
/// output variant array with the schema:
///
/// ```text
/// {
///   metadata: BINARY,
///   value: BINARY,
///   typed_value: {
///     a: {
///       value: BINARY,
///       typed_value: INT,
///     },
///     b: {
///       value: BINARY,
///       typed_value: INT,
///     },
///   }
/// }
/// ```
pub fn shred_variant(array: &VariantArray, as_type: &DataType) -> Result<VariantArray> {
    if array.typed_value_field().is_some() {
        return Err(ArrowError::InvalidArgumentError(
            "Input is already shredded".to_string(),
        ));
    }

    if array.value_field().is_none() {
        // all-null case -- nothing to do.
        return Ok(array.clone());
    };

    let cast_options = CastOptions::default();
    let mut builder = make_variant_to_shredded_variant_arrow_row_builder(
        as_type,
        &cast_options,
        array.len(),
        true,
    )?;
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null()?;
        } else {
            builder.append_value(array.value(i))?;
        }
    }
    let (value, typed_value, nulls) = builder.finish()?;
    Ok(VariantArray::from_parts(
        array.metadata_field().clone(),
        Some(value),
        Some(typed_value),
        nulls,
    ))
}

pub(crate) fn make_variant_to_shredded_variant_arrow_row_builder<'a>(
    data_type: &'a DataType,
    cast_options: &'a CastOptions,
    capacity: usize,
    top_level: bool,
) -> Result<VariantToShreddedVariantRowBuilder<'a>> {
    let builder = match data_type {
        DataType::Struct(fields) => {
            let typed_value_builder = VariantToShreddedObjectVariantRowBuilder::try_new(
                fields,
                cast_options,
                capacity,
                top_level,
            )?;
            VariantToShreddedVariantRowBuilder::Object(typed_value_builder)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(..) => {
            return Err(ArrowError::NotYetImplemented(
                "Shredding variant array values as arrow lists".to_string(),
            ));
        }
        _ => {
            let builder =
                make_primitive_variant_to_arrow_row_builder(data_type, cast_options, capacity)?;
            let typed_value_builder =
                VariantToShreddedPrimitiveVariantRowBuilder::new(builder, capacity, top_level);
            VariantToShreddedVariantRowBuilder::Primitive(typed_value_builder)
        }
    };
    Ok(builder)
}

pub(crate) enum VariantToShreddedVariantRowBuilder<'a> {
    Primitive(VariantToShreddedPrimitiveVariantRowBuilder<'a>),
    Object(VariantToShreddedObjectVariantRowBuilder<'a>),
}
impl<'a> VariantToShreddedVariantRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.append_null(),
            Object(b) => b.append_null(),
        }
    }

    pub fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.append_value(value),
            Object(b) => b.append_value(value),
        }
    }

    pub fn finish(self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        use VariantToShreddedVariantRowBuilder::*;
        match self {
            Primitive(b) => b.finish(),
            Object(b) => b.finish(),
        }
    }
}

/// A top-level variant shredder -- appending NULL produces typed_value=NULL and value=Variant::Null
pub(crate) struct VariantToShreddedPrimitiveVariantRowBuilder<'a> {
    value_builder: VariantValueArrayBuilder,
    typed_value_builder: PrimitiveVariantToArrowRowBuilder<'a>,
    nulls: NullBufferBuilder,
    top_level: bool,
}

impl<'a> VariantToShreddedPrimitiveVariantRowBuilder<'a> {
    pub(crate) fn new(
        typed_value_builder: PrimitiveVariantToArrowRowBuilder<'a>,
        capacity: usize,
        top_level: bool,
    ) -> Self {
        Self {
            value_builder: VariantValueArrayBuilder::new(capacity),
            typed_value_builder,
            nulls: NullBufferBuilder::new(capacity),
            top_level,
        }
    }
    fn append_null(&mut self) -> Result<()> {
        // Only the top-level struct that represents the variant can be nullable; object fields and
        // array elements are non-nullable.
        self.nulls.append(!self.top_level);
        self.value_builder.append_null();
        self.typed_value_builder.append_null()
    }
    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        self.nulls.append_non_null();
        if self.typed_value_builder.append_value(&value)? {
            self.value_builder.append_null();
        } else {
            self.value_builder.append_value(value);
        }
        Ok(true)
    }
    fn finish(mut self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        Ok((
            self.value_builder.build()?,
            self.typed_value_builder.finish()?,
            self.nulls.finish(),
        ))
    }
}

pub(crate) struct VariantToShreddedObjectVariantRowBuilder<'a> {
    value_builder: VariantValueArrayBuilder,
    typed_value_builders: IndexMap<&'a str, VariantToShreddedVariantRowBuilder<'a>>,
    typed_value_nulls: NullBufferBuilder,
    nulls: NullBufferBuilder,
    top_level: bool,
}

impl<'a> VariantToShreddedObjectVariantRowBuilder<'a> {
    fn try_new(
        fields: &'a Fields,
        cast_options: &'a CastOptions,
        capacity: usize,
        top_level: bool,
    ) -> Result<Self> {
        let typed_value_builders = fields.iter().map(|field| {
            let builder = make_variant_to_shredded_variant_arrow_row_builder(
                field.data_type(),
                cast_options,
                capacity,
                false,
            )?;
            Ok((field.name().as_str(), builder))
        });
        Ok(Self {
            value_builder: VariantValueArrayBuilder::new(capacity),
            typed_value_builders: typed_value_builders.collect::<Result<_>>()?,
            typed_value_nulls: NullBufferBuilder::new(capacity),
            nulls: NullBufferBuilder::new(capacity),
            top_level,
        })
    }

    fn append_null(&mut self) -> Result<()> {
        // Only the top-level struct that represents the variant can be nullable; object fields and
        // array elements are non-nullable.
        self.nulls.append(!self.top_level);
        self.value_builder.append_null();
        self.typed_value_nulls.append_null();
        for (_, typed_value_builder) in &mut self.typed_value_builders {
            typed_value_builder.append_null()?;
        }
        Ok(())
    }
    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        let Variant::Object(ref obj) = value else {
            // Not an object => fall back
            self.nulls.append_non_null();
            self.value_builder.append_value(value);
            self.typed_value_nulls.append_null();
            for (_, typed_value_builder) in &mut self.typed_value_builders {
                typed_value_builder.append_null()?;
            }
            return Ok(false);
        };

        // Route the object's fields by name as either shredded or unshredded
        let mut builder = self.value_builder.builder_ext(value.metadata());
        let mut object_builder = builder.try_new_object()?;
        let mut seen = std::collections::HashSet::new();
        let mut partially_shredded = false;
        for (field_name, value) in obj.iter() {
            match self.typed_value_builders.get_mut(field_name) {
                Some(typed_value_builder) => {
                    typed_value_builder.append_value(value)?;
                    seen.insert(field_name);
                }
                None => {
                    object_builder.insert_bytes(field_name, value);
                    partially_shredded = true;
                }
            }
        }

        // Handle missing fields
        for (field_name, typed_value_builder) in &mut self.typed_value_builders {
            if !seen.contains(field_name) {
                typed_value_builder.append_null()?;
            }
        }

        // Only emit the value if it captured any unshredded object fields
        if partially_shredded {
            object_builder.finish();
        } else {
            drop(object_builder);
            self.value_builder.append_null();
        }

        self.typed_value_nulls.append_non_null();
        self.nulls.append_non_null();
        Ok(true)
    }
    fn finish(mut self) -> Result<(BinaryViewArray, ArrayRef, Option<NullBuffer>)> {
        let mut builder = StructArrayBuilder::new();
        for (field_name, typed_value_builder) in self.typed_value_builders {
            let (value, typed_value, nulls) = typed_value_builder.finish()?;
            let array =
                ShreddedVariantFieldArray::from_parts(Some(value), Some(typed_value), nulls);
            builder = builder.with_field(field_name, ArrayRef::from(array), false);
        }
        if let Some(nulls) = self.typed_value_nulls.finish() {
            builder = builder.with_nulls(nulls);
        }
        Ok((
            self.value_builder.build()?,
            Arc::new(builder.build()),
            self.nulls.finish(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VariantArrayBuilder;
    use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_variant::{ObjectBuilder, ReadOnlyMetadataBuilder, Variant, VariantBuilder};
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_already_shredded_input_error() {
        // Create a VariantArray that already has typed_value_field
        // First create a valid VariantArray, then extract its parts to construct a shredded one
        let temp_array = VariantArray::from_iter(vec![Some(Variant::from("test"))]);
        let metadata = temp_array.metadata_field().clone();
        let value = temp_array.value_field().unwrap().clone();
        let typed_value = Arc::new(Int64Array::from(vec![42])) as ArrayRef;

        let shredded_array =
            VariantArray::from_parts(metadata, Some(value), Some(typed_value), None);

        let result = shred_variant(&shredded_array, &DataType::Int64);
        assert!(matches!(
            result.unwrap_err(),
            ArrowError::InvalidArgumentError(_)
        ));
    }

    #[test]
    fn test_all_null_input() {
        // Create VariantArray with no value field (all null case)
        let metadata = BinaryViewArray::from_iter_values([&[1u8, 0u8]]); // minimal valid metadata
        let all_null_array = VariantArray::from_parts(metadata, None, None, None);
        let result = shred_variant(&all_null_array, &DataType::Int64).unwrap();

        // Should return array with no value/typed_value fields
        assert!(result.value_field().is_none());
        assert!(result.typed_value_field().is_none());
    }

    #[test]
    fn test_unsupported_list_schema() {
        let input = VariantArray::from_iter([Variant::from(42)]);
        let list_schema = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        shred_variant(&input, &list_schema).expect_err("unsupported");
    }

    #[test]
    fn test_invalid_fixed_size_binary_shredding() {
        let mock_uuid_1 = Uuid::new_v4();

        let input = VariantArray::from_iter([Some(Variant::from(mock_uuid_1)), None]);

        // shred_variant only supports FixedSizeBinary(16). Any other length will err.
        let err = shred_variant(&input, &DataType::FixedSizeBinary(17)).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: FixedSizeBinary(17) is not a valid variant shredding type. Only FixedSizeBinary(16) for UUID is supported."
        );
    }

    #[test]
    fn test_uuid_shredding() {
        let mock_uuid_1 = Uuid::new_v4();
        let mock_uuid_2 = Uuid::new_v4();

        let input = VariantArray::from_iter([
            Some(Variant::from(mock_uuid_1)),
            None,
            Some(Variant::from(false)),
            Some(Variant::from(mock_uuid_2)),
        ]);

        let variant_array = shred_variant(&input, &DataType::FixedSizeBinary(16)).unwrap();

        // // inspect the typed_value Field and make sure it contains the canonical Uuid extension type
        // let typed_value_field = variant_array
        //     .inner()
        //     .fields()
        //     .into_iter()
        //     .find(|f| f.name() == "typed_value")
        //     .unwrap();

        // assert!(
        //     typed_value_field
        //         .try_extension_type::<extension::Uuid>()
        //         .is_ok()
        // );

        // probe the downcasted typed_value array to make sure uuids are shredded correctly
        let uuids = variant_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(uuids.len(), 4);

        assert!(!uuids.is_null(0));

        let got_uuid_1: &[u8] = uuids.value(0);
        assert_eq!(got_uuid_1, mock_uuid_1.as_bytes());

        assert!(uuids.is_null(1));
        assert!(uuids.is_null(2));

        assert!(!uuids.is_null(3));

        let got_uuid_2: &[u8] = uuids.value(3);
        assert_eq!(got_uuid_2, mock_uuid_2.as_bytes());
    }

    #[test]
    fn test_primitive_shredding_comprehensive() {
        // Test mixed scenarios in a single array
        let input = VariantArray::from_iter(vec![
            Some(Variant::from(42i64)),   // successful shred
            Some(Variant::from("hello")), // failed shred (string)
            Some(Variant::from(100i64)),  // successful shred
            None,                         // array-level null
            Some(Variant::Null),          // variant null
            Some(Variant::from(3i8)),     // successful shred (int8->int64 conversion)
        ]);

        let result = shred_variant(&input, &DataType::Int64).unwrap();

        // Verify structure
        let metadata_field = result.metadata_field();
        let value_field = result.value_field().unwrap();
        let typed_value_field = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Check specific outcomes for each row
        assert_eq!(result.len(), 6);

        // Row 0: 42 -> should shred successfully
        assert!(!result.is_null(0));
        assert!(value_field.is_null(0)); // value should be null when shredded
        assert!(!typed_value_field.is_null(0));
        assert_eq!(typed_value_field.value(0), 42);

        // Row 1: "hello" -> should fail to shred
        assert!(!result.is_null(1));
        assert!(!value_field.is_null(1)); // value should contain original
        assert!(typed_value_field.is_null(1)); // typed_value should be null
        assert_eq!(
            Variant::new(metadata_field.value(1), value_field.value(1)),
            Variant::from("hello")
        );

        // Row 2: 100 -> should shred successfully
        assert!(!result.is_null(2));
        assert!(value_field.is_null(2));
        assert_eq!(typed_value_field.value(2), 100);

        // Row 3: array null -> should be null in result
        assert!(result.is_null(3));

        // Row 4: Variant::Null -> should not shred (it's a null variant, not an integer)
        assert!(!result.is_null(4));
        assert!(!value_field.is_null(4)); // should contain Variant::Null
        assert_eq!(
            Variant::new(metadata_field.value(4), value_field.value(4)),
            Variant::Null
        );
        assert!(typed_value_field.is_null(4));

        // Row 5: 3i8 -> should shred successfully (int8->int64 conversion)
        assert!(!result.is_null(5));
        assert!(value_field.is_null(5)); // value should be null when shredded
        assert!(!typed_value_field.is_null(5));
        assert_eq!(typed_value_field.value(5), 3);
    }

    #[test]
    fn test_primitive_different_target_types() {
        let input = VariantArray::from_iter(vec![
            Variant::from(42i32),
            Variant::from(3.15f64),
            Variant::from("not_a_number"),
        ]);

        // Test Int32 target
        let result_int32 = shred_variant(&input, &DataType::Int32).unwrap();
        let typed_value_int32 = result_int32
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(typed_value_int32.value(0), 42);
        assert!(typed_value_int32.is_null(1)); // float doesn't convert to int32
        assert!(typed_value_int32.is_null(2)); // string doesn't convert to int32

        // Test Float64 target
        let result_float64 = shred_variant(&input, &DataType::Float64).unwrap();
        let typed_value_float64 = result_float64
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(typed_value_float64.value(0), 42.0); // int converts to float
        assert_eq!(typed_value_float64.value(1), 3.15);
        assert!(typed_value_float64.is_null(2)); // string doesn't convert
    }

    #[test]
    fn test_object_shredding_comprehensive() {
        let mut builder = VariantArrayBuilder::new(7);

        // Row 0: Fully shredded object
        builder
            .new_object()
            .with_field("score", 95.5f64)
            .with_field("age", 30i64)
            .finish();

        // Row 1: Partially shredded object (extra email field)
        builder
            .new_object()
            .with_field("score", 87.2f64)
            .with_field("age", 25i64)
            .with_field("email", "bob@example.com")
            .finish();

        // Row 2: Missing field (no score)
        builder.new_object().with_field("age", 35i64).finish();

        // Row 3: Type mismatch (score is string, age is string)
        builder
            .new_object()
            .with_field("score", "ninety-five")
            .with_field("age", "thirty")
            .finish();

        // Row 4: Non-object
        builder.append_variant(Variant::from("not an object"));

        // Row 5: Empty object
        builder.new_object().finish();

        // Row 6: Null
        builder.append_null();

        // Row 7: Object with only "wrong" fields
        builder.new_object().with_field("foo", 10).finish();

        // Row 8: Object with one "right" and one "wrong" field
        builder
            .new_object()
            .with_field("score", 66.67f64)
            .with_field("foo", 10)
            .finish();

        let input = builder.build();

        // Create target schema: struct<score: float64, age: int64>
        // Both types are supported for shredding
        let fields = Fields::from(vec![
            Field::new("score", DataType::Float64, true),
            Field::new("age", DataType::Int64, true),
        ]);
        let target_schema = DataType::Struct(fields);

        let result = shred_variant(&input, &target_schema).unwrap();

        // Verify structure
        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());
        assert_eq!(result.len(), 9);

        let metadata = result.metadata_field();

        let value = result.value_field().unwrap();
        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Extract score and age fields from typed_value struct
        let score_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("score").unwrap())
                .unwrap();
        let age_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("age").unwrap()).unwrap();

        let score_value = score_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let score_typed_value = score_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let age_value = age_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let age_typed_value = age_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Set up exhaustive checking of all shredded columns and their nulls/values
        struct ShreddedValue<'m, 'v, T> {
            value: Option<Variant<'m, 'v>>,
            typed_value: Option<T>,
        }
        struct ShreddedStruct<'m, 'v> {
            score: ShreddedValue<'m, 'v, f64>,
            age: ShreddedValue<'m, 'v, i64>,
        }
        fn get_value<'m, 'v>(
            i: usize,
            metadata: &'m BinaryViewArray,
            value: &'v BinaryViewArray,
        ) -> Variant<'m, 'v> {
            Variant::new(metadata.value(i), value.value(i))
        }
        let expect = |i, expected_result: Option<ShreddedValue<ShreddedStruct>>| {
            match expected_result {
                Some(ShreddedValue {
                    value: expected_value,
                    typed_value: expected_typed_value,
                }) => {
                    assert!(result.is_valid(i));
                    match expected_value {
                        Some(expected_value) => {
                            assert!(value.is_valid(i));
                            assert_eq!(expected_value, get_value(i, metadata, value));
                        }
                        None => {
                            assert!(value.is_null(i));
                        }
                    }
                    match expected_typed_value {
                        Some(ShreddedStruct {
                            score: expected_score,
                            age: expected_age,
                        }) => {
                            assert!(typed_value.is_valid(i));
                            assert!(score_field.is_valid(i)); // non-nullable
                            assert!(age_field.is_valid(i)); // non-nullable
                            match expected_score.value {
                                Some(expected_score_value) => {
                                    assert!(score_value.is_valid(i));
                                    assert_eq!(
                                        expected_score_value,
                                        get_value(i, metadata, score_value)
                                    );
                                }
                                None => {
                                    assert!(score_value.is_null(i));
                                }
                            }
                            match expected_score.typed_value {
                                Some(expected_score) => {
                                    assert!(score_typed_value.is_valid(i));
                                    assert_eq!(expected_score, score_typed_value.value(i));
                                }
                                None => {
                                    assert!(score_typed_value.is_null(i));
                                }
                            }
                            match expected_age.value {
                                Some(expected_age_value) => {
                                    assert!(age_value.is_valid(i));
                                    assert_eq!(
                                        expected_age_value,
                                        get_value(i, metadata, age_value)
                                    );
                                }
                                None => {
                                    assert!(age_value.is_null(i));
                                }
                            }
                            match expected_age.typed_value {
                                Some(expected_age) => {
                                    assert!(age_typed_value.is_valid(i));
                                    assert_eq!(expected_age, age_typed_value.value(i));
                                }
                                None => {
                                    assert!(age_typed_value.is_null(i));
                                }
                            }
                        }
                        None => {
                            assert!(typed_value.is_null(i));
                        }
                    }
                }
                None => {
                    assert!(result.is_null(i));
                }
            };
        };

        // Row 0: Fully shredded - both fields shred successfully
        expect(
            0,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(95.5),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(30),
                    },
                }),
            }),
        );

        // Row 1: Partially shredded - value contains extra email field
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("email", "bob@example.com")
            .finish();
        let (m, v) = builder.finish();
        let expected_value = Variant::new(&m, &v);

        expect(
            1,
            Some(ShreddedValue {
                value: Some(expected_value),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(87.2),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(25),
                    },
                }),
            }),
        );

        // Row 2: Fully shredded -- missing score field
        expect(
            2,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: Some(35),
                    },
                }),
            }),
        );

        // Row 3: Type mismatches - both score and age are strings
        expect(
            3,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: Some(Variant::from("ninety-five")),
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: Some(Variant::from("thirty")),
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 4: Non-object - falls back to value field
        expect(
            4,
            Some(ShreddedValue {
                value: Some(Variant::from("not an object")),
                typed_value: None,
            }),
        );

        // Row 5: Empty object
        expect(
            5,
            Some(ShreddedValue {
                value: None,
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 6: Null
        expect(6, None);

        // Helper to correctly create a variant object using a row's existing metadata
        let object_with_foo_field = |i| {
            use parquet_variant::{ParentState, ValueBuilder, VariantMetadata};
            let metadata = VariantMetadata::new(metadata.value(i));
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
            let mut value_builder = ValueBuilder::new();
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            ObjectBuilder::new(state, false)
                .with_field("foo", 10)
                .finish();
            (metadata, value_builder.into_inner())
        };

        // Row 7: Object with only a "wrong" field
        let (m, v) = object_with_foo_field(7);
        expect(
            7,
            Some(ShreddedValue {
                value: Some(Variant::new_with_metadata(m, &v)),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );

        // Row 8: Object with one "wrong" and one "right" field
        let (m, v) = object_with_foo_field(8);
        expect(
            8,
            Some(ShreddedValue {
                value: Some(Variant::new_with_metadata(m, &v)),
                typed_value: Some(ShreddedStruct {
                    score: ShreddedValue {
                        value: None,
                        typed_value: Some(66.67),
                    },
                    age: ShreddedValue {
                        value: None,
                        typed_value: None,
                    },
                }),
            }),
        );
    }

    #[test]
    fn test_object_different_schemas() {
        // Create object with multiple fields
        let mut builder = VariantArrayBuilder::new(1);
        builder
            .new_object()
            .with_field("id", 123i32)
            .with_field("age", 25i64)
            .with_field("score", 95.5f64)
            .finish();
        let input = builder.build();

        // Test with schema containing only id field
        let schema1 = DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int32, true)]));
        let result1 = shred_variant(&input, &schema1).unwrap();
        let value_field1 = result1.value_field().unwrap();
        assert!(!value_field1.is_null(0)); // should contain {"age": 25, "score": 95.5}

        // Test with schema containing id and age fields
        let schema2 = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let result2 = shred_variant(&input, &schema2).unwrap();
        let value_field2 = result2.value_field().unwrap();
        assert!(!value_field2.is_null(0)); // should contain {"score": 95.5}

        // Test with schema containing all fields
        let schema3 = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let result3 = shred_variant(&input, &schema3).unwrap();
        let value_field3 = result3.value_field().unwrap();
        assert!(value_field3.is_null(0)); // fully shredded, no remaining fields
    }

    #[test]
    fn test_uuid_shredding_in_objects() {
        let mock_uuid_1 = Uuid::new_v4();
        let mock_uuid_2 = Uuid::new_v4();
        let mock_uuid_3 = Uuid::new_v4();

        let mut builder = VariantArrayBuilder::new(6);

        // Row 0: Fully shredded object with both UUID fields
        builder
            .new_object()
            .with_field("id", mock_uuid_1)
            .with_field("session_id", mock_uuid_2)
            .finish();

        // Row 1: Partially shredded object - UUID fields plus extra field
        builder
            .new_object()
            .with_field("id", mock_uuid_2)
            .with_field("session_id", mock_uuid_3)
            .with_field("name", "test_user")
            .finish();

        // Row 2: Missing UUID field (no session_id)
        builder.new_object().with_field("id", mock_uuid_1).finish();

        // Row 3: Type mismatch - id is UUID but session_id is a string
        builder
            .new_object()
            .with_field("id", mock_uuid_3)
            .with_field("session_id", "not-a-uuid")
            .finish();

        // Row 4: Object with non-UUID value in id field
        builder
            .new_object()
            .with_field("id", 12345i64)
            .with_field("session_id", mock_uuid_1)
            .finish();

        // Row 5: Null
        builder.append_null();

        let input = builder.build();

        let fields = Fields::from(vec![
            Field::new("id", DataType::FixedSizeBinary(16), true),
            Field::new("session_id", DataType::FixedSizeBinary(16), true),
        ]);
        let target_schema = DataType::Struct(fields);

        let result = shred_variant(&input, &target_schema).unwrap();

        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());
        assert_eq!(result.len(), 6);

        let metadata = result.metadata_field();
        let value = result.value_field().unwrap();
        let typed_value = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Extract id and session_id fields from typed_value struct
        let id_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("id").unwrap()).unwrap();
        let session_id_field =
            ShreddedVariantFieldArray::try_new(typed_value.column_by_name("session_id").unwrap())
                .unwrap();

        let id_value = id_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let id_typed_value = id_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let session_id_value = session_id_field
            .value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        let session_id_typed_value = session_id_field
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        // Row 0: Fully shredded - both UUID fields shred successfully
        assert!(result.is_valid(0));

        assert!(value.is_null(0)); // fully shredded, no remaining fields
        assert!(id_value.is_null(0));
        assert!(session_id_value.is_null(0));

        assert!(typed_value.is_valid(0));
        assert!(id_typed_value.is_valid(0));
        assert!(session_id_typed_value.is_valid(0));

        assert_eq!(id_typed_value.value(0), mock_uuid_1.as_bytes());
        assert_eq!(session_id_typed_value.value(0), mock_uuid_2.as_bytes());

        // Row 1: Partially shredded - value contains extra name field
        assert!(result.is_valid(1));

        assert!(value.is_valid(1)); // contains unshredded "name" field
        assert!(typed_value.is_valid(1));

        assert!(id_value.is_null(1));
        assert!(id_typed_value.is_valid(1));
        assert_eq!(id_typed_value.value(1), mock_uuid_2.as_bytes());

        assert!(session_id_value.is_null(1));
        assert!(session_id_typed_value.is_valid(1));
        assert_eq!(session_id_typed_value.value(1), mock_uuid_3.as_bytes());

        // Verify the value field contains the name field
        let row_1_variant = Variant::new(metadata.value(1), value.value(1));
        let Variant::Object(obj) = row_1_variant else {
            panic!("Expected object");
        };

        assert_eq!(obj.get("name"), Some(Variant::from("test_user")));

        // Row 2: Missing session_id field
        assert!(result.is_valid(2));

        assert!(value.is_null(2)); // fully shredded, no extra fields
        assert!(typed_value.is_valid(2));

        assert!(id_value.is_null(2));
        assert!(id_typed_value.is_valid(2));
        assert_eq!(id_typed_value.value(2), mock_uuid_1.as_bytes());

        assert!(session_id_value.is_null(2));
        assert!(session_id_typed_value.is_null(2)); // missing field

        // Row 3: Type mismatch - session_id is a string, not UUID
        assert!(result.is_valid(3));

        assert!(value.is_null(3)); // no extra fields
        assert!(typed_value.is_valid(3));

        assert!(id_value.is_null(3));
        assert!(id_typed_value.is_valid(3));
        assert_eq!(id_typed_value.value(3), mock_uuid_3.as_bytes());

        assert!(session_id_value.is_valid(3)); // type mismatch, stored in value
        assert!(session_id_typed_value.is_null(3));
        let session_id_variant = Variant::new(metadata.value(3), session_id_value.value(3));
        assert_eq!(session_id_variant, Variant::from("not-a-uuid"));

        // Row 4: Type mismatch - id is int64, not UUID
        assert!(result.is_valid(4));

        assert!(value.is_null(4)); // no extra fields
        assert!(typed_value.is_valid(4));

        assert!(id_value.is_valid(4)); // type mismatch, stored in value
        assert!(id_typed_value.is_null(4));
        let id_variant = Variant::new(metadata.value(4), id_value.value(4));
        assert_eq!(id_variant, Variant::from(12345i64));

        assert!(session_id_value.is_null(4));
        assert!(session_id_typed_value.is_valid(4));
        assert_eq!(session_id_typed_value.value(4), mock_uuid_1.as_bytes());

        // Row 5: Null
        assert!(result.is_null(5));
    }

    #[test]
    fn test_spec_compliance() {
        let input = VariantArray::from_iter(vec![Variant::from(42i64), Variant::from("hello")]);

        let result = shred_variant(&input, &DataType::Int64).unwrap();

        // Test field access by name (not position)
        let inner_struct = result.inner();
        assert!(inner_struct.column_by_name("metadata").is_some());
        assert!(inner_struct.column_by_name("value").is_some());
        assert!(inner_struct.column_by_name("typed_value").is_some());

        // Test metadata preservation
        assert_eq!(result.metadata_field().len(), input.metadata_field().len());
        // The metadata should be the same reference (cheap clone)
        // Note: BinaryViewArray doesn't have a .values() method, so we compare the arrays directly
        assert_eq!(result.metadata_field().len(), input.metadata_field().len());

        // Test output structure correctness
        assert_eq!(result.len(), input.len());
        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());

        // For primitive shredding, verify that value and typed_value are never both non-null
        // (This rule applies to primitives; for objects, both can be non-null for partial shredding)
        let value_field = result.value_field().unwrap();
        let typed_value_field = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..result.len() {
            if !result.is_null(i) {
                let value_is_null = value_field.is_null(i);
                let typed_value_is_null = typed_value_field.is_null(i);
                // For primitive shredding, at least one should be null
                assert!(
                    value_is_null || typed_value_is_null,
                    "Row {}: both value and typed_value are non-null for primitive shredding",
                    i
                );
            }
        }
    }
}
