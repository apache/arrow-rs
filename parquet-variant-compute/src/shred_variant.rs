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
    make_primitive_variant_to_arrow_row_builder, PrimitiveVariantToArrowRowBuilder,
};
use crate::{VariantArray, VariantValueArrayBuilder};
use arrow::array::Array as _;
use arrow::array::{ArrayRef, BinaryViewArray, NullBufferBuilder};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Fields};
use arrow::error::{ArrowError, Result};
use parquet_variant::{ObjectBuilder, ReadOnlyMetadataBuilder, Variant, EMPTY_VARIANT_METADATA};

use indexmap::IndexMap;
use std::sync::Arc;

pub fn shred_variant(array: &VariantArray, as_type: &DataType) -> Result<VariantArray> {
    if array.typed_value_field().is_some() {
        return Err(ArrowError::InvalidArgumentError(
            "Input is already shredded".to_string(),
        ));
    }

    if array.value_field().is_none() {
        // all-null case
        return Ok(VariantArray::from_parts(
            array.metadata_field().clone(),
            None,
            None,
            None,
        ));
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
    len: usize,
    top_level: bool,
) -> Result<VariantToShreddedVariantRowBuilder<'a>> {
    let builder = match data_type {
        DataType::Struct(fields) => {
            let typed_value_builder = VariantToShreddedObjectVariantRowBuilder::try_new(
                fields,
                cast_options,
                len,
                top_level,
            )?;
            VariantToShreddedVariantRowBuilder::Object(typed_value_builder)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(..) => {
            // TODO: Special handling for shredded variant arrays
            return Err(ArrowError::NotYetImplemented(
                "shred_variant not yet implemented for lists".to_string(),
            ));
        }
        _ => {
            let builder =
                make_primitive_variant_to_arrow_row_builder(data_type, cast_options, len)?;
            let typed_value_builder =
                VariantToShreddedPrimitiveVariantRowBuilder::new(builder, len, top_level);
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
        len: usize,
        top_level: bool,
    ) -> Self {
        Self {
            value_builder: VariantValueArrayBuilder::new(len),
            typed_value_builder,
            nulls: NullBufferBuilder::new(len),
            top_level,
        }
    }
    fn append_null(&mut self) -> Result<()> {
        if self.top_level {
            self.nulls.append_null();
            self.value_builder.append_null();
        } else {
            self.nulls.append_non_null();
            self.value_builder.append_value(Variant::Null);
        }
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
        len: usize,
        top_level: bool,
    ) -> Result<Self> {
        let typed_value_builders = fields.iter().map(|field| {
            let builder = make_variant_to_shredded_variant_arrow_row_builder(
                field.data_type(),
                cast_options,
                len,
                top_level,
            )?;
            Ok((field.name().as_str(), builder))
        });
        Ok(Self {
            value_builder: VariantValueArrayBuilder::new(len),
            typed_value_builders: typed_value_builders.collect::<Result<_>>()?,
            typed_value_nulls: NullBufferBuilder::new(len),
            nulls: NullBufferBuilder::new(len),
            top_level,
        })
    }

    fn append_null(&mut self) -> Result<()> {
        if self.top_level {
            self.nulls.append_null();
            self.value_builder.append_null();
        } else {
            self.nulls.append_non_null();
            self.value_builder.append_value(Variant::Null);
        }
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
        let metadata = value.metadata().cloned().unwrap_or(EMPTY_VARIANT_METADATA);
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
        let state = self.value_builder.parent_state(&mut metadata_builder);
        let mut object_builder = ObjectBuilder::new(state, false);
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
            builder = builder.with_field(field_name, Arc::new(array), false);
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
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_variant::{Variant, VariantBuilderExt};
    use std::sync::Arc;

    fn create_test_variant_array(values: Vec<Option<Variant<'_, '_>>>) -> VariantArray {
        let mut builder = VariantArrayBuilder::new(values.len());
        for value in values {
            match value {
                Some(v) => builder.append_variant(v),
                None => builder.append_null(),
            }
        }
        builder.build()
    }

    // Input Validation Tests (3 tests - cannot consolidate)

    #[test]
    fn test_already_shredded_input_error() {
        // Create a VariantArray that already has typed_value_field
        // First create a valid VariantArray, then extract its parts to construct a shredded one
        let temp_array = create_test_variant_array(vec![Some(Variant::from("test"))]);
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
        let input = create_test_variant_array(vec![Some(Variant::from(42))]);
        let list_schema = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        shred_variant(&input, &list_schema).expect_err("unsupported");
    }

    // Primitive Shredding Tests (2 consolidated tests)

    #[test]
    fn test_primitive_shredding_comprehensive() {
        // Test mixed scenarios in a single array
        let input = create_test_variant_array(vec![
            Some(Variant::from(42i64)),   // successful shred
            Some(Variant::from("hello")), // failed shred (string)
            Some(Variant::from(100i64)),  // successful shred
            None,                         // array-level null
            Some(Variant::Null),          // variant null
            Some(Variant::from(3i8)),     // successful shred (int8->int64 conversion)
        ]);

        let result = shred_variant(&input, &DataType::Int64).unwrap();
        println!("result: {:?}", result);

        // Verify structure
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

        // Row 2: 100 -> should shred successfully
        assert!(!result.is_null(2));
        assert!(value_field.is_null(2));
        assert_eq!(typed_value_field.value(2), 100);

        // Row 3: array null -> should be null in result
        assert!(result.is_null(3));

        // Row 4: Variant::Null -> should not shred (it's a null variant, not an integer)
        assert!(!result.is_null(4));
        assert!(!value_field.is_null(4)); // should contain Variant::Null
        assert!(typed_value_field.is_null(4));

        // Row 5: 3i8 -> should shred successfully (int8->int64 conversion)
        assert!(!result.is_null(5));
        assert!(value_field.is_null(5)); // value should be null when shredded
        assert!(!typed_value_field.is_null(5));
        assert_eq!(typed_value_field.value(5), 3);
    }

    #[test]
    fn test_primitive_different_target_types() {
        let input = create_test_variant_array(vec![
            Some(Variant::from(42i32)),
            Some(Variant::from(3.15f64)),
            Some(Variant::from("not_a_number")),
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

    // Object Shredding Tests (2 consolidated tests)

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

        let input = builder.build();
        println!("input: {input:?}");

        // Create target schema: struct<score: float64, age: int64>
        // Both types are supported for shredding
        let fields = Fields::from(vec![
            Field::new("score", DataType::Float64, true),
            Field::new("age", DataType::Int64, true),
        ]);
        let target_schema = DataType::Struct(fields);

        let result = shred_variant(&input, &target_schema).unwrap();
        println!("result: {result:?}");

        // Verify structure
        assert!(result.value_field().is_some());
        assert!(result.typed_value_field().is_some());
        assert_eq!(result.len(), 7);

        let value_field = result.value_field().unwrap();
        let typed_value_struct = result
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        // Extract score and age fields from typed_value struct
        let score_field_array = typed_value_struct
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<crate::variant_array::ShreddedVariantFieldArray>()
            .unwrap();
        let age_field_array = typed_value_struct
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<crate::variant_array::ShreddedVariantFieldArray>()
            .unwrap();

        let score_typed_values = score_field_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let age_typed_values = age_field_array
            .typed_value_field()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Row 0: Fully shredded - both fields shred successfully
        assert!(value_field.is_null(0)); // no unshredded fields
        assert_eq!(score_typed_values.value(0), 95.5); // score successfully shredded
        assert_eq!(age_typed_values.value(0), 30); // age successfully shredded

        // Row 1: Partially shredded - value contains extra email field
        assert!(!value_field.is_null(1)); // contains {"email": "bob@example.com"}
        assert_eq!(score_typed_values.value(1), 87.2); // score successfully shredded
        assert_eq!(age_typed_values.value(1), 25); // age successfully shredded

        // Row 2: Missing score field
        assert!(value_field.is_null(2)); // no unshredded fields
        assert!(score_typed_values.is_null(2)); // score is missing
        assert_eq!(age_typed_values.value(2), 35); // age successfully shredded

        // Row 3: Type mismatches - both score and age are strings
        assert!(value_field.is_null(3)); // no unshredded fields (but both fields have fallback values)
        assert!(score_typed_values.is_null(3)); // score failed to shred (string "ninety-five")
        assert!(age_typed_values.is_null(3)); // age failed to shred (string "thirty")
                                              // Both should be in their respective field's value arrays (type mismatch fallback)
        let score_value_field = score_field_array.value_field().unwrap();
        let age_value_field = age_field_array.value_field().unwrap();
        assert!(!score_value_field.is_null(3)); // contains "ninety-five" as variant
        assert!(!age_value_field.is_null(3)); // contains "thirty" as variant

        // Row 4: Non-object - falls back to value field
        assert!(!value_field.is_null(4)); // contains "not an object"
        assert!(typed_value_struct.is_null(4)); // typed_value is null for non-objects

        // Row 5: Empty object
        assert!(value_field.is_null(5)); // no unshredded fields
        assert!(score_typed_values.is_null(5)); // score is missing
        assert!(age_typed_values.is_null(5)); // age is missing

        // Row 6: Null
        assert!(result.is_null(6));
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

    // Specification Compliance Test (1 consolidated test)

    #[test]
    fn test_spec_compliance() {
        let input = create_test_variant_array(vec![
            Some(Variant::from(42i64)),
            Some(Variant::from("hello")),
        ]);

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
