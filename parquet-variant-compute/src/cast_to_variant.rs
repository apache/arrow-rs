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

use std::collections::HashMap;

use crate::type_conversion::decimal_to_variant_decimal;
use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{
    Array, AsArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::compute::kernels::cast;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, ArrowTemporalType, ArrowTimestampType, Date32Type,
    Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    RunEndIndexType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::temporal_conversions::{as_date, as_datetime, as_time};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{DateTime, TimeZone, Utc};
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal16, VariantDecimal4,
    VariantDecimal8,
};

/// Casts a typed arrow [`Array`] to a [`VariantArray`]. This is useful when you
/// need to convert a specific data type
///
/// # Arguments
/// * `input` - A reference to the input [`Array`] to cast
///
/// # Notes
/// If the input array element is null, the corresponding element in the
/// output `VariantArray` will also be null (not `Variant::Null`).
///
/// # Example
/// ```
/// # use arrow::array::{Array, ArrayRef, Int64Array};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::cast_to_variant::cast_to_variant;
/// // input is an Int64Array, which will be cast to a VariantArray
/// let input = Int64Array::from(vec![Some(1), None, Some(3)]);
/// let result = cast_to_variant(&input).unwrap();
/// assert_eq!(result.len(), 3);
/// assert_eq!(result.value(0), Variant::Int64(1));
/// assert!(result.is_null(1)); // note null, not Variant::Null
/// assert_eq!(result.value(2), Variant::Int64(3));
/// ```
///
/// For `DataType::Timestamp`s: if the timestamp has any level of precision
/// greater than a microsecond, it will be truncated. For example
/// `1970-01-01T00:00:01.234567890Z`
/// will be truncated to
/// `1970-01-01T00:00:01.234567Z`
pub fn cast_to_variant(input: &dyn Array) -> Result<VariantArray, ArrowError> {
    // Create row builder for the input array type
    let mut row_builder = make_arrow_to_variant_row_builder(input.data_type(), input)?;

    // Create output array builder
    let mut array_builder = VariantArrayBuilder::new(input.len());

    // Process each row using the row builder
    for i in 0..input.len() {
        let mut builder = array_builder.variant_builder();
        row_builder.append_row(i, &mut builder)?;
        builder.finish();
    }

    Ok(array_builder.build())
}

/// Factory function to create the appropriate row builder for a given DataType
fn make_arrow_to_variant_row_builder<'a>(
    data_type: &'a DataType,
    array: &'a dyn Array,
) -> Result<ArrowToVariantRowBuilder<'a>, ArrowError> {
    let builder = match data_type {
        DataType::Null => ArrowToVariantRowBuilder::Null(NullArrowToVariantBuilder),
        DataType::Boolean => {
            ArrowToVariantRowBuilder::Boolean(BooleanArrowToVariantBuilder::new(array))
        }
        DataType::Int8 => {
            ArrowToVariantRowBuilder::PrimitiveInt8(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Int16 => {
            ArrowToVariantRowBuilder::PrimitiveInt16(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Int32 => {
            ArrowToVariantRowBuilder::PrimitiveInt32(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Int64 => {
            ArrowToVariantRowBuilder::PrimitiveInt64(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::UInt8 => {
            ArrowToVariantRowBuilder::PrimitiveUInt8(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::UInt16 => {
            ArrowToVariantRowBuilder::PrimitiveUInt16(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::UInt32 => {
            ArrowToVariantRowBuilder::PrimitiveUInt32(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::UInt64 => {
            ArrowToVariantRowBuilder::PrimitiveUInt64(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Float16 => {
            ArrowToVariantRowBuilder::PrimitiveFloat16(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Float32 => {
            ArrowToVariantRowBuilder::PrimitiveFloat32(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Float64 => {
            ArrowToVariantRowBuilder::PrimitiveFloat64(PrimitiveArrowToVariantBuilder::new(array))
        }
        DataType::Decimal32(_, scale) => {
            ArrowToVariantRowBuilder::Decimal32(Decimal32ArrowToVariantBuilder::new(array, *scale))
        }
        DataType::Decimal64(_, scale) => {
            ArrowToVariantRowBuilder::Decimal64(Decimal64ArrowToVariantBuilder::new(array, *scale))
        }
        DataType::Decimal128(_, scale) => ArrowToVariantRowBuilder::Decimal128(
            Decimal128ArrowToVariantBuilder::new(array, *scale),
        ),
        DataType::Decimal256(_, scale) => ArrowToVariantRowBuilder::Decimal256(
            Decimal256ArrowToVariantBuilder::new(array, *scale),
        ),
        DataType::Timestamp(time_unit, time_zone) => match time_unit {
            TimeUnit::Second => ArrowToVariantRowBuilder::TimestampSecond(
                TimestampArrowToVariantBuilder::new(array, time_zone.is_some()),
            ),
            TimeUnit::Millisecond => ArrowToVariantRowBuilder::TimestampMillisecond(
                TimestampArrowToVariantBuilder::new(array, time_zone.is_some()),
            ),
            TimeUnit::Microsecond => ArrowToVariantRowBuilder::TimestampMicrosecond(
                TimestampArrowToVariantBuilder::new(array, time_zone.is_some()),
            ),
            TimeUnit::Nanosecond => ArrowToVariantRowBuilder::TimestampNanosecond(
                TimestampArrowToVariantBuilder::new(array, time_zone.is_some()),
            ),
        },
        DataType::Date32 => ArrowToVariantRowBuilder::Date32(DateArrowToVariantBuilder::new(array)),
        DataType::Date64 => ArrowToVariantRowBuilder::Date64(DateArrowToVariantBuilder::new(array)),
        DataType::Time32(time_unit) => match time_unit {
            TimeUnit::Second => {
                ArrowToVariantRowBuilder::Time32Second(TimeArrowToVariantBuilder::new(array))
            }
            TimeUnit::Millisecond => {
                ArrowToVariantRowBuilder::Time32Millisecond(TimeArrowToVariantBuilder::new(array))
            }
            _ => {
                return Err(ArrowError::CastError(format!(
                    "Unsupported Time32 unit: {time_unit:?}"
                )))
            }
        },
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Microsecond => {
                ArrowToVariantRowBuilder::Time64Microsecond(TimeArrowToVariantBuilder::new(array))
            }
            TimeUnit::Nanosecond => {
                ArrowToVariantRowBuilder::Time64Nanosecond(TimeArrowToVariantBuilder::new(array))
            }
            _ => {
                return Err(ArrowError::CastError(format!(
                    "Unsupported Time64 unit: {time_unit:?}"
                )))
            }
        },
        DataType::Duration(_) | DataType::Interval(_) => {
            return Err(ArrowError::InvalidArgumentError(
                "Casting duration/interval types to Variant is not supported. \
                 The Variant format does not define duration/interval types."
                    .to_string(),
            ))
        }
        DataType::Binary => {
            ArrowToVariantRowBuilder::Binary(BinaryArrowToVariantBuilder::new(array))
        }
        DataType::LargeBinary => {
            ArrowToVariantRowBuilder::LargeBinary(BinaryArrowToVariantBuilder::new(array))
        }
        DataType::BinaryView => {
            ArrowToVariantRowBuilder::BinaryView(BinaryViewArrowToVariantBuilder::new(array))
        }
        DataType::FixedSizeBinary(_) => ArrowToVariantRowBuilder::FixedSizeBinary(
            FixedSizeBinaryArrowToVariantBuilder::new(array),
        ),
        DataType::Utf8 => ArrowToVariantRowBuilder::String(StringArrowToVariantBuilder::new(array)),
        DataType::LargeUtf8 => {
            ArrowToVariantRowBuilder::LargeString(StringArrowToVariantBuilder::new(array))
        }
        DataType::Utf8View => {
            ArrowToVariantRowBuilder::Utf8View(Utf8ViewArrowToVariantBuilder::new(array))
        }
        DataType::List(_) => ArrowToVariantRowBuilder::List(ListArrowToVariantBuilder::new(array)?),
        DataType::LargeList(_) => {
            ArrowToVariantRowBuilder::LargeList(ListArrowToVariantBuilder::new(array)?)
        }
        DataType::Struct(_) => {
            ArrowToVariantRowBuilder::Struct(StructArrowToVariantBuilder::new(array.as_struct())?)
        }
        DataType::Map(_, _) => ArrowToVariantRowBuilder::Map(MapArrowToVariantBuilder::new(array)?),
        DataType::Union(_, _) => {
            ArrowToVariantRowBuilder::Union(UnionArrowToVariantBuilder::new(array)?)
        }
        DataType::Dictionary(_, _) => {
            ArrowToVariantRowBuilder::Dictionary(DictionaryArrowToVariantBuilder::new(array)?)
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => ArrowToVariantRowBuilder::RunEndEncodedInt16(
                RunEndEncodedArrowToVariantBuilder::new(array)?,
            ),
            DataType::Int32 => ArrowToVariantRowBuilder::RunEndEncodedInt32(
                RunEndEncodedArrowToVariantBuilder::new(array)?,
            ),
            DataType::Int64 => ArrowToVariantRowBuilder::RunEndEncodedInt64(
                RunEndEncodedArrowToVariantBuilder::new(array)?,
            ),
            _ => {
                return Err(ArrowError::CastError(format!(
                    "Unsupported run ends type: {:?}",
                    run_ends.data_type()
                )));
            }
        },
        dt => {
            return Err(ArrowError::CastError(format!(
                "Unsupported data type for casting to Variant: {dt:?}",
            )));
        }
    };
    Ok(builder)
}

// TODO do we need a cast_with_options to allow specifying conversion behavior,
// e.g. how to handle overflows, whether to convert to Variant::Null or return
// an error, etc. ?

// ============================================================================
// Row-oriented builders for efficient Arrow-to-Variant conversion
// ============================================================================

/// Row builder for converting Arrow arrays to VariantArray row by row
pub(crate) enum ArrowToVariantRowBuilder<'a> {
    PrimitiveInt8(PrimitiveArrowToVariantBuilder<'a, Int8Type>),
    PrimitiveInt16(PrimitiveArrowToVariantBuilder<'a, Int16Type>),
    PrimitiveInt32(PrimitiveArrowToVariantBuilder<'a, Int32Type>),
    PrimitiveInt64(PrimitiveArrowToVariantBuilder<'a, Int64Type>),
    PrimitiveUInt8(PrimitiveArrowToVariantBuilder<'a, UInt8Type>),
    PrimitiveUInt16(PrimitiveArrowToVariantBuilder<'a, UInt16Type>),
    PrimitiveUInt32(PrimitiveArrowToVariantBuilder<'a, UInt32Type>),
    PrimitiveUInt64(PrimitiveArrowToVariantBuilder<'a, UInt64Type>),
    PrimitiveFloat16(PrimitiveArrowToVariantBuilder<'a, Float16Type>),
    PrimitiveFloat32(PrimitiveArrowToVariantBuilder<'a, Float32Type>),
    PrimitiveFloat64(PrimitiveArrowToVariantBuilder<'a, Float64Type>),
    Decimal32(Decimal32ArrowToVariantBuilder<'a>),
    Decimal64(Decimal64ArrowToVariantBuilder<'a>),
    Decimal128(Decimal128ArrowToVariantBuilder<'a>),
    Decimal256(Decimal256ArrowToVariantBuilder<'a>),
    Boolean(BooleanArrowToVariantBuilder<'a>),
    String(StringArrowToVariantBuilder<'a, i32>),
    LargeString(StringArrowToVariantBuilder<'a, i64>),
    Binary(BinaryArrowToVariantBuilder<'a, i32>),
    LargeBinary(BinaryArrowToVariantBuilder<'a, i64>),
    BinaryView(BinaryViewArrowToVariantBuilder<'a>),
    FixedSizeBinary(FixedSizeBinaryArrowToVariantBuilder<'a>),
    Utf8View(Utf8ViewArrowToVariantBuilder<'a>),
    Struct(StructArrowToVariantBuilder<'a>),
    Null(NullArrowToVariantBuilder),
    RunEndEncodedInt16(RunEndEncodedArrowToVariantBuilder<'a, Int16Type>),
    RunEndEncodedInt32(RunEndEncodedArrowToVariantBuilder<'a, Int32Type>),
    RunEndEncodedInt64(RunEndEncodedArrowToVariantBuilder<'a, Int64Type>),
    Dictionary(DictionaryArrowToVariantBuilder<'a>),
    List(ListArrowToVariantBuilder<'a, i32>),
    LargeList(ListArrowToVariantBuilder<'a, i64>),
    Map(MapArrowToVariantBuilder<'a>),
    Union(UnionArrowToVariantBuilder<'a>),
    TimestampSecond(TimestampArrowToVariantBuilder<'a, TimestampSecondType>),
    TimestampMillisecond(TimestampArrowToVariantBuilder<'a, TimestampMillisecondType>),
    TimestampMicrosecond(TimestampArrowToVariantBuilder<'a, TimestampMicrosecondType>),
    TimestampNanosecond(TimestampArrowToVariantBuilder<'a, TimestampNanosecondType>),
    Date32(DateArrowToVariantBuilder<'a, Date32Type>),
    Date64(DateArrowToVariantBuilder<'a, Date64Type>),
    Time32Second(TimeArrowToVariantBuilder<'a, Time32SecondType>),
    Time32Millisecond(TimeArrowToVariantBuilder<'a, Time32MillisecondType>),
    Time64Microsecond(TimeArrowToVariantBuilder<'a, Time64MicrosecondType>),
    Time64Nanosecond(TimeArrowToVariantBuilder<'a, Time64NanosecondType>),
}

impl<'a> ArrowToVariantRowBuilder<'a> {
    pub fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        match self {
            ArrowToVariantRowBuilder::PrimitiveInt8(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveInt16(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveInt32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveInt64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveUInt8(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveUInt16(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveUInt32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveUInt64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveFloat16(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveFloat32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::PrimitiveFloat64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Decimal32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Decimal64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Decimal128(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Decimal256(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Boolean(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::String(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::LargeString(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Binary(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::LargeBinary(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::BinaryView(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::FixedSizeBinary(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Utf8View(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Struct(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Null(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::RunEndEncodedInt16(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::RunEndEncodedInt32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::RunEndEncodedInt64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Dictionary(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::List(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::LargeList(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Map(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Union(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::TimestampSecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::TimestampMillisecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::TimestampMicrosecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::TimestampNanosecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Date32(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Date64(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Time32Second(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Time32Millisecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Time64Microsecond(b) => b.append_row(index, builder),
            ArrowToVariantRowBuilder::Time64Nanosecond(b) => b.append_row(index, builder),
        }
    }
}

/// Macro to define (possibly generic) row builders with consistent structure and behavior.
/// Supports optional extra fields that are passed to the constructor.
macro_rules! define_row_builder {
    (
        struct $name:ident<$lifetime:lifetime $(, $generic:ident: $($bound:path)+)?>
        $(where $where_path:path: $where_bound:path)?
        $({ $($field:ident: $field_type:ty),* $(,)? })?,
        |$array_param:ident| -> $array_type:ty { $init_expr:expr },
        |$value:ident| $value_transform:expr
    ) => {
        pub(crate) struct $name<$lifetime $(, $generic: $($bound)+)?>
        $(where $where_path: $where_bound)?
        {
            array: &$lifetime $array_type,
            $($($field: $field_type,)*)?
        }

        impl<$lifetime $(, $generic: $($bound)+)?> $name<$lifetime $(, $generic)?>
        $(where $where_path: $where_bound)?
        {
            fn new($array_param: &$lifetime dyn Array $(, $($field: $field_type),*)?) -> Self {
                Self {
                    array: $init_expr,
                    $($($field,)*)?
                }
            }

            fn append_row(&self, index: usize, builder: &mut impl VariantBuilderExt) -> Result<(), ArrowError> {
                if self.array.is_null(index) {
                    builder.append_null();
                } else {
                    let $value = self.array.value(index);
                    // Capture fields as variables the transform can access (hygiene)
                    $($(let $field = &self.$field;)*)?
                    builder.append_value($value_transform);
                }
                Ok(())
            }
        }
    };
}

// ============================================================================
// Generic row builders generated by macro
// ============================================================================

// Primitive builder - handles all primitive types
define_row_builder!(
    struct PrimitiveArrowToVariantBuilder<'a, T: ArrowPrimitiveType>
    where T::Native: Into<Variant<'a, 'a>>,
    |array| -> PrimitiveArray<T> { array.as_primitive() },
    |value| value
);

// Boolean builder - handles BooleanArray
define_row_builder!(
    struct BooleanArrowToVariantBuilder<'a>,
    |array| -> arrow::array::BooleanArray { array.as_boolean() },
    |value| value
);

// Generic String builder for StringArray (Utf8 and LargeUtf8)
define_row_builder!(
    struct StringArrowToVariantBuilder<'a, O: OffsetSizeTrait>,
    |array| -> GenericStringArray<O> { array.as_string() },
    |value| value
);

/// Struct builder for StructArray
pub(crate) struct StructArrowToVariantBuilder<'a> {
    struct_array: &'a arrow::array::StructArray,
    field_builders: Vec<(&'a str, ArrowToVariantRowBuilder<'a>)>,
}

impl<'a> StructArrowToVariantBuilder<'a> {
    fn new(struct_array: &'a arrow::array::StructArray) -> Result<Self, ArrowError> {
        let mut field_builders = Vec::new();

        // Create a row builder for each field
        for (field_name, field_array) in struct_array
            .column_names()
            .iter()
            .zip(struct_array.columns().iter())
        {
            let field_builder =
                make_arrow_to_variant_row_builder(field_array.data_type(), field_array.as_ref())?;
            field_builders.push((*field_name, field_builder));
        }

        Ok(Self {
            struct_array,
            field_builders,
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        if self.struct_array.is_null(index) {
            builder.append_null();
        } else {
            // Create object builder for this struct row
            let mut obj_builder = builder.try_new_object()?;

            // Process each field
            for (field_name, row_builder) in &mut self.field_builders {
                let mut field_builder =
                    parquet_variant::ObjectFieldBuilder::new(field_name, &mut obj_builder);
                row_builder.append_row(index, &mut field_builder)?;
            }

            obj_builder.finish();
        }
        Ok(())
    }
}

/// Null builder that always appends null
pub(crate) struct NullArrowToVariantBuilder;

impl NullArrowToVariantBuilder {
    fn append_row(
        &mut self,
        _index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        builder.append_null();
        Ok(())
    }
}

/// Run-end encoded array builder with efficient sequential access
pub(crate) struct RunEndEncodedArrowToVariantBuilder<'a, R: RunEndIndexType> {
    run_array: &'a arrow::array::RunArray<R>,
    values_builder: Box<ArrowToVariantRowBuilder<'a>>,

    run_ends: &'a [R::Native],
    run_number: usize, // Physical index into run_ends and values
    run_start: usize,  // Logical start index of current run
}

impl<'a, R: RunEndIndexType> RunEndEncodedArrowToVariantBuilder<'a, R> {
    fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let Some(run_array) = array.as_run_opt() else {
            return Err(ArrowError::CastError("Expected RunArray".to_string()));
        };

        let values = run_array.values();
        let values_builder =
            make_arrow_to_variant_row_builder(values.data_type(), values.as_ref())?;

        Ok(Self {
            run_array,
            values_builder: Box::new(values_builder),
            run_ends: run_array.run_ends().values(),
            run_number: 0,
            run_start: 0,
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        self.set_run_for_index(index)?;

        // Handle null values
        if self.run_array.values().is_null(self.run_number) {
            builder.append_null();
            return Ok(());
        }

        // Re-encode the value
        self.values_builder.append_row(self.run_number, builder)?;

        Ok(())
    }

    fn set_run_for_index(&mut self, index: usize) -> Result<(), ArrowError> {
        if index >= self.run_start {
            let Some(run_end) = self.run_ends.get(self.run_number) else {
                return Err(ArrowError::CastError(format!(
                    "Index {index} beyond run array"
                )));
            };
            if index < run_end.as_usize() {
                return Ok(());
            }
            if index == run_end.as_usize() {
                self.run_number += 1;
                self.run_start = run_end.as_usize();
                return Ok(());
            }
        }

        // Use partition_point for all non-sequential cases
        let run_number = self
            .run_ends
            .partition_point(|&run_end| run_end.as_usize() <= index);
        if run_number >= self.run_ends.len() {
            return Err(ArrowError::CastError(format!(
                "Index {index} beyond run array"
            )));
        }
        self.run_number = run_number;
        self.run_start = match run_number {
            0 => 0,
            _ => self.run_ends[run_number - 1].as_usize(),
        };
        Ok(())
    }
}

/// Dictionary array builder with simple O(1) indexing
pub(crate) struct DictionaryArrowToVariantBuilder<'a> {
    keys: &'a dyn Array, // only needed for null checks
    normalized_keys: Vec<usize>,
    values_builder: Box<ArrowToVariantRowBuilder<'a>>,
}

impl<'a> DictionaryArrowToVariantBuilder<'a> {
    fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let dict_array = array.as_any_dictionary();
        let values = dict_array.values();
        let values_builder =
            make_arrow_to_variant_row_builder(values.data_type(), values.as_ref())?;

        // WARNING: normalized_keys panics if values is empty
        let normalized_keys = match values.len() {
            0 => Vec::new(),
            _ => dict_array.normalized_keys(),
        };

        Ok(Self {
            keys: dict_array.keys(),
            normalized_keys,
            values_builder: Box::new(values_builder),
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        if self.keys.is_null(index) {
            builder.append_null();
        } else {
            let normalized_key = self.normalized_keys[index];
            self.values_builder.append_row(normalized_key, builder)?;
        }
        Ok(())
    }
}

/// Generic list builder for List and LargeList types
pub(crate) struct ListArrowToVariantBuilder<'a, O: OffsetSizeTrait> {
    list_array: &'a arrow::array::GenericListArray<O>,
    values_builder: Box<ArrowToVariantRowBuilder<'a>>,
}

impl<'a, O: OffsetSizeTrait> ListArrowToVariantBuilder<'a, O> {
    fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let list_array = array.as_list();
        let values = list_array.values();
        let values_builder =
            make_arrow_to_variant_row_builder(values.data_type(), values.as_ref())?;

        Ok(Self {
            list_array,
            values_builder: Box::new(values_builder),
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        if self.list_array.is_null(index) {
            builder.append_null();
            return Ok(());
        }

        let offsets = self.list_array.offsets();
        let start = offsets[index].as_usize();
        let end = offsets[index + 1].as_usize();

        let mut list_builder = builder.try_new_list()?;
        for value_index in start..end {
            self.values_builder
                .append_row(value_index, &mut list_builder)?;
        }
        list_builder.finish();
        Ok(())
    }
}

/// Map builder for MapArray types
pub(crate) struct MapArrowToVariantBuilder<'a> {
    map_array: &'a arrow::array::MapArray,
    key_strings: arrow::array::StringArray,
    values_builder: Box<ArrowToVariantRowBuilder<'a>>,
}

impl<'a> MapArrowToVariantBuilder<'a> {
    fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let map_array = array.as_map();

        // Pre-cast keys to strings once (like existing convert_map code)
        let keys = cast(map_array.keys(), &DataType::Utf8)?;
        let key_strings = keys.as_string::<i32>().clone();

        // Create recursive builder for values
        let values = map_array.values();
        let values_builder =
            make_arrow_to_variant_row_builder(values.data_type(), values.as_ref())?;

        Ok(Self {
            map_array,
            key_strings,
            values_builder: Box::new(values_builder),
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        // Check for NULL map first (via null bitmap)
        if self.map_array.is_null(index) {
            builder.append_null();
            return Ok(());
        }

        let offsets = self.map_array.offsets();
        let start = offsets[index].as_usize();
        let end = offsets[index + 1].as_usize();

        // Create object builder for this map (even if empty)
        let mut object_builder = builder.try_new_object()?;

        // Add each key-value pair (loop does nothing for empty maps - correct!)
        for kv_index in start..end {
            let key = self.key_strings.value(kv_index);
            let mut field_builder = ObjectFieldBuilder::new(key, &mut object_builder);
            self.values_builder
                .append_row(kv_index, &mut field_builder)?;
        }

        object_builder.finish(); // Empty map becomes empty object {}
        Ok(())
    }
}

/// Union builder for both sparse and dense union arrays
pub(crate) struct UnionArrowToVariantBuilder<'a> {
    union_array: &'a arrow::array::UnionArray,
    child_builders: HashMap<i8, Box<ArrowToVariantRowBuilder<'a>>>,
}

impl<'a> UnionArrowToVariantBuilder<'a> {
    fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let union_array = array.as_union();
        let type_ids = union_array.type_ids();

        // Create child builders for each union field
        let mut child_builders = HashMap::new();
        for &type_id in type_ids {
            let child_array = union_array.child(type_id);
            let child_builder =
                make_arrow_to_variant_row_builder(child_array.data_type(), child_array.as_ref())?;
            child_builders.insert(type_id, Box::new(child_builder));
        }

        Ok(Self {
            union_array,
            child_builders,
        })
    }

    fn append_row(
        &mut self,
        index: usize,
        builder: &mut impl VariantBuilderExt,
    ) -> Result<(), ArrowError> {
        let type_id = self.union_array.type_id(index);
        let value_offset = self.union_array.value_offset(index);

        // Delegate to the appropriate child builder, or append null to handle an invalid type_id
        match self.child_builders.get_mut(&type_id) {
            Some(child_builder) => child_builder.append_row(value_offset, builder)?,
            None => builder.append_null(),
        }

        Ok(())
    }
}

// Decimal32 builder for Arrow Decimal32Array
define_row_builder!(
    struct Decimal32ArrowToVariantBuilder<'a> {
        scale: i8,
    },
    |array| -> arrow::array::Decimal32Array { array.as_primitive() },
    |value| decimal_to_variant_decimal!(value, scale, i32, VariantDecimal4)
);

// Decimal64 builder for Arrow Decimal64Array
define_row_builder!(
    struct Decimal64ArrowToVariantBuilder<'a> {
        scale: i8,
    },
    |array| -> arrow::array::Decimal64Array { array.as_primitive() },
    |value| decimal_to_variant_decimal!(value, scale, i64, VariantDecimal8)
);

// Decimal128 builder for Arrow Decimal128Array
define_row_builder!(
    struct Decimal128ArrowToVariantBuilder<'a> {
        scale: i8,
    },
    |array| -> arrow::array::Decimal128Array { array.as_primitive() },
    |value| decimal_to_variant_decimal!(value, scale, i128, VariantDecimal16)
);

// Decimal256 builder for Arrow Decimal256Array
define_row_builder!(
    struct Decimal256ArrowToVariantBuilder<'a> {
        scale: i8,
    },
    |array| -> arrow::array::Decimal256Array { array.as_primitive() },
    |value| {
        // Decimal256 needs special handling - convert to i128 if possible
        match value.to_i128() {
            Some(i128_val) => decimal_to_variant_decimal!(i128_val, scale, i128, VariantDecimal16),
            None => Variant::Null, // Value too large for i128
        }
    }
);

// Generic Binary builder for Arrow BinaryArray and LargeBinaryArray
define_row_builder!(
    struct BinaryArrowToVariantBuilder<'a, O: OffsetSizeTrait>,
    |array| -> GenericBinaryArray<O> { array.as_binary() },
    |value| value
);

// BinaryView builder - handles BinaryViewArray
define_row_builder!(
    struct BinaryViewArrowToVariantBuilder<'a>,
    |array| -> arrow::array::BinaryViewArray { array.as_byte_view() },
    |value| value
);

// FixedSizeBinary builder - handles FixedSizeBinaryArray
define_row_builder!(
    struct FixedSizeBinaryArrowToVariantBuilder<'a>,
    |array| -> arrow::array::FixedSizeBinaryArray { array.as_fixed_size_binary() },
    |value| value
);

// Utf8View builder - handles StringViewArray
define_row_builder!(
    struct Utf8ViewArrowToVariantBuilder<'a>,
    |array| -> arrow::array::StringViewArray { array.as_string_view() },
    |value| value
);

// Generic Timestamp builder for Arrow timestamp arrays
define_row_builder!(
    struct TimestampArrowToVariantBuilder<'a, T: ArrowTimestampType> {
        has_time_zone: bool,
    },
    |array| -> arrow::array::PrimitiveArray<T> { array.as_primitive() },
    |value| {
        // Convert using Arrow's temporal conversion functions
        let Some(naive_datetime) = as_datetime::<T>(value) else {
            return Err(ArrowError::CastError(
                "Failed to convert Arrow timestamp value to chrono::NaiveDateTime".to_string(),
            ));
        };
        if *has_time_zone {
            // Has timezone -> DateTime<Utc> -> TimestampMicros/TimestampNanos
            let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime);
            Variant::from(utc_dt) // Uses From<DateTime<Utc>> for Variant
        } else {
            // No timezone -> NaiveDateTime -> TimestampNtzMicros/TimestampNtzNanos
            Variant::from(naive_datetime) // Uses From<NaiveDateTime> for Variant
        }
    }
);

// Generic Date builder for Arrow date arrays (Date32, Date64)
define_row_builder!(
    struct DateArrowToVariantBuilder<'a, T: ArrowTemporalType>
    where i64: From<T::Native>,
    |array| -> PrimitiveArray<T> { array.as_primitive() },
    |value| {
        let date_value = i64::from(value);
        as_date::<T>(date_value).map(Variant::from).unwrap_or(Variant::Null)
    }
);

// Generic Time builder for Arrow time arrays (Time32, Time64)
define_row_builder!(
    struct TimeArrowToVariantBuilder<'a, T: ArrowTemporalType>
    where i64: From<T::Native>,
    |array| -> PrimitiveArray<T> { array.as_primitive() },
    |value| {
        let time_value = i64::from(value);
        as_time::<T>(time_value).map(Variant::from).unwrap_or(Variant::Null)
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray, DurationMicrosecondArray,
        DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
        FixedSizeBinaryBuilder, Float16Array, Float32Array, Float64Array, GenericByteBuilder,
        GenericByteViewBuilder, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeListArray,
        LargeStringArray, ListArray, MapArray, NullArray, StringArray, StringRunBuilder,
        StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array, UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{
        i256, BinaryType, BinaryViewType, IntervalDayTime, IntervalMonthDayNano, LargeBinaryType,
    };
    use arrow_schema::{DataType, Field, Fields, UnionFields};
    use arrow_schema::{
        DECIMAL128_MAX_PRECISION, DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION,
    };
    use chrono::{NaiveDate, NaiveTime};
    use half::f16;
    use parquet_variant::{Variant, VariantBuilder, VariantDecimal16};
    use std::{sync::Arc, vec};

    macro_rules! max_unscaled_value {
        (32, $precision:expr) => {
            (u32::pow(10, $precision as u32) - 1) as i32
        };
        (64, $precision:expr) => {
            (u64::pow(10, $precision as u32) - 1) as i64
        };
        (128, $precision:expr) => {
            (u128::pow(10, $precision as u32) - 1) as i128
        };
    }

    #[test]
    fn test_cast_to_variant_null() {
        run_test(Arc::new(NullArray::new(2)), vec![None, None])
    }

    #[test]
    fn test_cast_to_variant_bool() {
        run_test(
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            vec![
                Some(Variant::BooleanTrue),
                None,
                Some(Variant::BooleanFalse),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_int8() {
        run_test(
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i8::MAX),
            ])),
            vec![
                Some(Variant::Int8(i8::MIN)),
                None,
                Some(Variant::Int8(-1)),
                Some(Variant::Int8(1)),
                Some(Variant::Int8(i8::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int16() {
        run_test(
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i16::MAX),
            ])),
            vec![
                Some(Variant::Int16(i16::MIN)),
                None,
                Some(Variant::Int16(-1)),
                Some(Variant::Int16(1)),
                Some(Variant::Int16(i16::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int32() {
        run_test(
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i32::MAX),
            ])),
            vec![
                Some(Variant::Int32(i32::MIN)),
                None,
                Some(Variant::Int32(-1)),
                Some(Variant::Int32(1)),
                Some(Variant::Int32(i32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int64() {
        run_test(
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i64::MAX),
            ])),
            vec![
                Some(Variant::Int64(i64::MIN)),
                None,
                Some(Variant::Int64(-1)),
                Some(Variant::Int64(1)),
                Some(Variant::Int64(i64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint8() {
        run_test(
            Arc::new(UInt8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(127),
                Some(u8::MAX),
            ])),
            vec![
                Some(Variant::Int8(0)),
                None,
                Some(Variant::Int8(1)),
                Some(Variant::Int8(127)),
                Some(Variant::Int16(255)), // u8::MAX cannot fit in Int8
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint16() {
        run_test(
            Arc::new(UInt16Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(32767),
                Some(u16::MAX),
            ])),
            vec![
                Some(Variant::Int16(0)),
                None,
                Some(Variant::Int16(1)),
                Some(Variant::Int16(32767)),
                Some(Variant::Int32(65535)), // u16::MAX cannot fit in Int16
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint32() {
        run_test(
            Arc::new(UInt32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2147483647),
                Some(u32::MAX),
            ])),
            vec![
                Some(Variant::Int32(0)),
                None,
                Some(Variant::Int32(1)),
                Some(Variant::Int32(2147483647)),
                Some(Variant::Int64(4294967295)), // u32::MAX cannot fit in Int32
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint64() {
        run_test(
            Arc::new(UInt64Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(9223372036854775807),
                Some(u64::MAX),
            ])),
            vec![
                Some(Variant::Int64(0)),
                None,
                Some(Variant::Int64(1)),
                Some(Variant::Int64(9223372036854775807)),
                Some(Variant::Decimal16(
                    // u64::MAX cannot fit in Int64
                    VariantDecimal16::try_from(18446744073709551615).unwrap(),
                )),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float16() {
        run_test(
            Arc::new(Float16Array::from(vec![
                Some(f16::MIN),
                None,
                Some(f16::from_f32(-1.5)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(1.5)),
                Some(f16::MAX),
            ])),
            vec![
                Some(Variant::Float(f16::MIN.into())),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f16::MAX.into())),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float32() {
        run_test(
            Arc::new(Float32Array::from(vec![
                Some(f32::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f32::MAX),
            ])),
            vec![
                Some(Variant::Float(f32::MIN)),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float64() {
        run_test(
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f64::MAX),
            ])),
            vec![
                Some(Variant::Double(f64::MIN)),
                None,
                Some(Variant::Double(-1.5)),
                Some(Variant::Double(0.0)),
                Some(Variant::Double(1.5)),
                Some(Variant::Double(f64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The min of Decimal32 with positive scale that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The max of Decimal32 with positive scale that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32_negative_scale() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The min of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The max of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(
                        -max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(
                        max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The min of Decimal64 with positive scale that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The max of Decimal64 with positive scale that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64_negative_scale() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The min of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The max of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(
                        -max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(
                        max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The min of Decimal128 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The max of Decimal128 with positive scale that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128_negative_scale() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The min of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The max of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The min of Decimal256 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The max of Decimal256 with positive scale that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256_negative_scale() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The min of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The max of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_timestamp() {
        let run_array_tests =
            |microseconds: i64, array_ntz: Arc<dyn Array>, array_tz: Arc<dyn Array>| {
                let timestamp = DateTime::from_timestamp_nanos(microseconds * 1000);
                run_test(
                    array_tz,
                    vec![Some(Variant::TimestampMicros(timestamp)), None],
                );
                run_test(
                    array_ntz,
                    vec![
                        Some(Variant::TimestampNtzMicros(timestamp.naive_utc())),
                        None,
                    ],
                );
            };

        let nanosecond = 1234567890;
        let microsecond = 1234567;
        let millisecond = 1234;
        let second = 1;

        let second_array = TimestampSecondArray::from(vec![Some(second), None]);
        run_array_tests(
            second * 1000 * 1000,
            Arc::new(second_array.clone()),
            Arc::new(second_array.with_timezone("+01:00".to_string())),
        );

        let millisecond_array = TimestampMillisecondArray::from(vec![Some(millisecond), None]);
        run_array_tests(
            millisecond * 1000,
            Arc::new(millisecond_array.clone()),
            Arc::new(millisecond_array.with_timezone("+01:00".to_string())),
        );

        let microsecond_array = TimestampMicrosecondArray::from(vec![Some(microsecond), None]);
        run_array_tests(
            microsecond,
            Arc::new(microsecond_array.clone()),
            Arc::new(microsecond_array.with_timezone("+01:00".to_string())),
        );

        let timestamp = DateTime::from_timestamp_nanos(nanosecond);
        let nanosecond_array = TimestampNanosecondArray::from(vec![Some(nanosecond), None]);
        run_test(
            Arc::new(nanosecond_array.clone()),
            vec![
                Some(Variant::TimestampNtzNanos(timestamp.naive_utc())),
                None,
            ],
        );
        run_test(
            Arc::new(nanosecond_array.with_timezone("+01:00".to_string())),
            vec![Some(Variant::TimestampNanos(timestamp)), None],
        );
    }

    #[test]
    fn test_cast_to_variant_date() {
        // Date32Array
        run_test(
            Arc::new(Date32Array::from(vec![
                Some(Date32Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date32Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date32Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );

        // Date64Array
        run_test(
            Arc::new(Date64Array::from(vec![
                Some(Date64Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date64Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date64Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_time32_second() {
        let array: Time32SecondArray = vec![Some(1), Some(86_399), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(1, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(86_399, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time32_millisecond() {
        let array: Time32MillisecondArray = vec![Some(123_456), Some(456_000), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_000_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(456, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time64_micro() {
        let array: Time64MicrosecondArray = vec![Some(1), Some(123_456_789), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time64_nano() {
        let array: Time64NanosecondArray =
            vec![Some(1), Some(1001), Some(123_456_789_012), None].into();
        run_test(
            Arc::new(array),
            // as we can only present with micro second, so the nano second will round donw to 0
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_duration_or_interval_errors() {
        let arrays: Vec<Box<dyn Array>> = vec![
            // Duration types
            Box::new(DurationSecondArray::from(vec![Some(10), None, Some(-5)])),
            Box::new(DurationMillisecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            Box::new(DurationMicrosecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            Box::new(DurationNanosecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            // Interval types
            Box::new(IntervalYearMonthArray::from(vec![Some(12), None, Some(-6)])),
            Box::new(IntervalDayTimeArray::from(vec![
                Some(IntervalDayTime::new(12, 0)),
                None,
                Some(IntervalDayTime::new(-6, 0)),
            ])),
            Box::new(IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(12, 0, 0)),
                None,
                Some(IntervalMonthDayNano::new(-6, 0, 0)),
            ])),
        ];

        for array in arrays {
            let result = cast_to_variant(array.as_ref());
            assert!(result.is_err());
            match result.unwrap_err() {
                ArrowError::InvalidArgumentError(msg) => {
                    assert!(
                        msg.contains("Casting duration/interval types to Variant is not supported")
                    );
                    assert!(
                        msg.contains("The Variant format does not define duration/interval types")
                    );
                }
                _ => panic!("Expected InvalidArgumentError"),
            }
        }
    }

    #[test]
    fn test_cast_to_variant_binary() {
        // BinaryType
        let mut builder = GenericByteBuilder::<BinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let binary_array = builder.finish();
        run_test(
            Arc::new(binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // LargeBinaryType
        let mut builder = GenericByteBuilder::<LargeBinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let large_binary_array = builder.finish();
        run_test(
            Arc::new(large_binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // BinaryViewType
        let mut builder = GenericByteViewBuilder::<BinaryViewType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let byte_view_array = builder.finish();
        run_test(
            Arc::new(byte_view_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_fixed_size_binary() {
        let v1 = vec![1, 2];
        let v2 = vec![3, 4];
        let v3 = vec![5, 6];

        let mut builder = FixedSizeBinaryBuilder::new(2);
        builder.append_value(&v1).unwrap();
        builder.append_value(&v2).unwrap();
        builder.append_null();
        builder.append_value(&v3).unwrap();
        let array = builder.finish();

        run_test(
            Arc::new(array),
            vec![
                Some(Variant::Binary(&v1)),
                Some(Variant::Binary(&v2)),
                None,
                Some(Variant::Binary(&v3)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world"), Some("test")];
        let string_array = StringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
                Some(Variant::from("test")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "a".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = StringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_large_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_array = LargeStringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "b".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = LargeStringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_utf8_view() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_view_array = StringViewArray::from(short_strings.clone());

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "c".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_view_array = StringViewArray::from(long_strings);

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_list() {
        // List Array
        let data = vec![Some(vec![Some(0), Some(1), Some(2)]), None];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0);
            list.append_value(1);
            list.append_value(2);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(list_array), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_sliced_list() {
        // List Array
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            None,
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3);
            list.append_value(4);
            list.append_value(5);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(list_array.slice(1, 2)), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_large_list() {
        // Large List Array
        let data = vec![Some(vec![Some(0), Some(1), Some(2)]), None];
        let large_list_array = LargeListArray::from_iter_primitive::<Int64Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0i64);
            list.append_value(1i64);
            list.append_value(2i64);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(large_list_array), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_sliced_large_list() {
        // List Array
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            None,
        ];
        let large_list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i64);
            list.append_value(4i64);
            list.append_value(5i64);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(
            Arc::new(large_list_array.slice(1, 2)),
            vec![Some(variant), None],
        );
    }

    #[test]
    fn test_cast_to_variant_struct() {
        // Test a simple struct with two fields: id (int64) and age (int32)
        let id_array = Int64Array::from(vec![Some(1001), Some(1002), None, Some(1003)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30), Some(35), None]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("age", DataType::Int32, true),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            None, // no nulls at the struct level
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 4);

        // Check first row: {"id": 1001, "age": 25}
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: {"id": 1002, "age": 30}
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));
        assert_eq!(obj2.get("age"), Some(Variant::from(30i32)));

        // Check third row: {"age": 35} (id is null, so omitted)
        let variant3 = result.value(2);
        let obj3 = variant3.as_object().unwrap();
        assert_eq!(obj3.get("id"), None);
        assert_eq!(obj3.get("age"), Some(Variant::from(35i32)));

        // Check fourth row: {"id": 1003} (age is null, so omitted)
        let variant4 = result.value(3);
        let obj4 = variant4.as_object().unwrap();
        assert_eq!(obj4.get("id"), Some(Variant::from(1003i64)));
        assert_eq!(obj4.get("age"), None);
    }

    #[test]
    fn test_cast_to_variant_struct_with_nulls() {
        // Test struct with null values at the struct level
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30)]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create null buffer to make second row null
        let null_buffer = NullBuffer::from(vec![true, false]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            Some(null_buffer),
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row: {"id": 1001, "age": 25}
        assert!(!result.is_null(0));
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: null struct
        assert!(result.is_null(1));
    }

    #[test]
    fn test_cast_to_variant_struct_performance() {
        // Test with a larger struct to demonstrate performance optimization
        // This test ensures that field arrays are only converted once, not per row
        let size = 1000;

        let id_array = Int64Array::from((0..size).map(|i| Some(i as i64)).collect::<Vec<_>>());
        let age_array = Int32Array::from(
            (0..size)
                .map(|i| Some((i % 100) as i32))
                .collect::<Vec<_>>(),
        );
        let score_array =
            Float64Array::from((0..size).map(|i| Some(i as f64 * 0.1)).collect::<Vec<_>>());

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(id_array),
                Arc::new(age_array),
                Arc::new(score_array),
            ],
            None,
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a few sample rows
        let variant0 = result.value(0);
        let obj0 = variant0.as_object().unwrap();
        assert_eq!(obj0.get("id"), Some(Variant::from(0i64)));
        assert_eq!(obj0.get("age"), Some(Variant::from(0i32)));
        assert_eq!(obj0.get("score"), Some(Variant::from(0.0f64)));

        let variant999 = result.value(999);
        let obj999 = variant999.as_object().unwrap();
        assert_eq!(obj999.get("id"), Some(Variant::from(999i64)));
        assert_eq!(obj999.get("age"), Some(Variant::from(99i32))); // 999 % 100 = 99
        assert_eq!(obj999.get("score"), Some(Variant::from(99.9f64)));
    }

    #[test]
    fn test_cast_to_variant_struct_performance_large() {
        // Test with even larger struct and more fields to demonstrate optimization benefits
        let size = 10000;
        let num_fields = 10;

        // Create arrays for many fields
        let mut field_arrays: Vec<ArrayRef> = Vec::new();
        let mut fields = Vec::new();

        for field_idx in 0..num_fields {
            match field_idx % 4 {
                0 => {
                    // Int64 fields
                    let array = Int64Array::from(
                        (0..size)
                            .map(|i| Some(i as i64 + field_idx as i64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int_field_{}", field_idx),
                        DataType::Int64,
                        false,
                    ));
                }
                1 => {
                    // Int32 fields
                    let array = Int32Array::from(
                        (0..size)
                            .map(|i| Some((i % 1000) as i32 + field_idx as i32))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int32_field_{}", field_idx),
                        DataType::Int32,
                        false,
                    ));
                }
                2 => {
                    // Float64 fields
                    let array = Float64Array::from(
                        (0..size)
                            .map(|i| Some(i as f64 * 0.1 + field_idx as f64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("float_field_{}", field_idx),
                        DataType::Float64,
                        false,
                    ));
                }
                _ => {
                    // Binary fields
                    let binary_data: Vec<Option<&[u8]>> = (0..size)
                        .map(|i| {
                            // Use static data to avoid lifetime issues in tests
                            match i % 3 {
                                0 => Some(b"test_data_0" as &[u8]),
                                1 => Some(b"test_data_1" as &[u8]),
                                _ => Some(b"test_data_2" as &[u8]),
                            }
                        })
                        .collect();
                    let array = BinaryArray::from(binary_data);
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("binary_field_{}", field_idx),
                        DataType::Binary,
                        false,
                    ));
                }
            }
        }

        let struct_array = StructArray::new(Fields::from(fields), field_arrays, None);

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a sample of rows
        for sample_idx in [0, size / 4, size / 2, size - 1] {
            let variant = result.value(sample_idx);
            let obj = variant.as_object().unwrap();

            // Should have all fields
            assert_eq!(obj.len(), num_fields);

            // Verify a few field values
            if let Some(int_field_0) = obj.get("int_field_0") {
                assert_eq!(int_field_0, Variant::from(sample_idx as i64));
            }
            if let Some(float_field_2) = obj.get("float_field_2") {
                assert_eq!(float_field_2, Variant::from(sample_idx as f64 * 0.1 + 2.0));
            }
        }
    }

    #[test]
    fn test_cast_to_variant_nested_struct() {
        // Test nested struct: person with location struct
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let x_array = Float64Array::from(vec![Some(40.7), Some(37.8)]);
        let y_array = Float64Array::from(vec![Some(-74.0), Some(-122.4)]);

        // Create location struct
        let location_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]);
        let location_struct = StructArray::new(
            location_fields.clone(),
            vec![Arc::new(x_array), Arc::new(y_array)],
            None,
        );

        // Create person struct containing location
        let person_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("location", DataType::Struct(location_fields), true),
        ]);
        let person_struct = StructArray::new(
            person_fields,
            vec![Arc::new(id_array), Arc::new(location_struct)],
            None,
        );

        let result = cast_to_variant(&person_struct).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));

        let location_variant1 = obj1.get("location").unwrap();
        let location_obj1 = location_variant1.as_object().unwrap();
        assert_eq!(location_obj1.get("x"), Some(Variant::from(40.7f64)));
        assert_eq!(location_obj1.get("y"), Some(Variant::from(-74.0f64)));

        // Check second row
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));

        let location_variant2 = obj2.get("location").unwrap();
        let location_obj2 = location_variant2.as_object().unwrap();
        assert_eq!(location_obj2.get("x"), Some(Variant::from(37.8f64)));
        assert_eq!(location_obj2.get("y"), Some(Variant::from(-122.4f64)));
    }

    #[test]
    fn test_cast_to_variant_map() {
        let keys = vec!["key1", "key2", "key3"];
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let entry_offsets = vec![0, 1, 3];
        let map_array =
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap();

        let result = cast_to_variant(&map_array).unwrap();
        // [{"key1":1}]
        let variant1 = result.value(0);
        assert_eq!(
            variant1.as_object().unwrap().get("key1").unwrap(),
            Variant::from(1)
        );

        // [{"key2":2},{"key3":3}]
        let variant2 = result.value(1);
        assert_eq!(
            variant2.as_object().unwrap().get("key2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant2.as_object().unwrap().get("key3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_map_with_nulls_and_empty() {
        use arrow::array::{Int32Array, MapArray, StringArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, Fields};
        use std::sync::Arc;

        // Create entries struct array
        let keys = StringArray::from(vec!["key1", "key2", "key3"]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let entries_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        // Create offsets for 4 maps: [0..1], [1..1], [1..1], [1..3]
        let offsets = OffsetBuffer::new(vec![0, 1, 1, 1, 3].into());

        // Create null buffer - map at index 2 is NULL
        let null_buffer = Some(NullBuffer::from(vec![true, true, false, true]));

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false,
        ));

        let map_array = MapArray::try_new(map_field, offsets, entries, null_buffer, false).unwrap();

        let result = cast_to_variant(&map_array).unwrap();

        // Map 0: {"key1": 1}
        let variant0 = result.value(0);
        assert_eq!(
            variant0.as_object().unwrap().get("key1").unwrap(),
            Variant::from(1)
        );

        // Map 1: {} (empty, not null)
        let variant1 = result.value(1);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.len(), 0); // Empty object

        // Map 2: null (actual NULL)
        assert!(result.is_null(2));

        // Map 3: {"key2": 2, "key3": 3}
        let variant3 = result.value(3);
        assert_eq!(
            variant3.as_object().unwrap().get("key2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant3.as_object().unwrap().get("key3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_map_with_non_string_keys() {
        let offsets = OffsetBuffer::new(vec![0, 1, 3].into());
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("values", DataType::Int32, false),
        ]);
        let columns = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
        ];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));

        let map_array = MapArray::new(field.clone(), offsets.clone(), entries.clone(), None, false);

        let result = cast_to_variant(&map_array).unwrap();

        let variant1 = result.value(0);
        assert_eq!(
            variant1.as_object().unwrap().get("1").unwrap(),
            Variant::from(1)
        );

        let variant2 = result.value(1);
        assert_eq!(
            variant2.as_object().unwrap().get("2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant2.as_object().unwrap().get("3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_union_sparse() {
        // Create a sparse union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), None, None, None, Some(34), None]);
        let float_array = Float64Array::from(vec![None, Some(3.2), None, Some(32.5), None, None]);
        let string_array = StringArray::from(vec![None, None, Some("hello"), None, None, None]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = UnionFields::new(
            vec![0, 1, 2],
            vec![
                Field::new("int_field", DataType::Int32, false),
                Field::new("float_field", DataType::Float64, false),
                Field::new("string_field", DataType::Utf8, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            None, // Sparse union
            children,
        )
        .unwrap();

        run_test(
            Arc::new(union_array),
            vec![
                Some(Variant::Int32(1)),
                Some(Variant::Double(3.2)),
                Some(Variant::from("hello")),
                Some(Variant::Double(32.5)),
                Some(Variant::Int32(34)),
                None,
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_union_dense() {
        // Create a dense union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), Some(34), None]);
        let float_array = Float64Array::from(vec![3.2, 32.5]);
        let string_array = StringArray::from(vec!["hello"]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 0, 0, 1, 1, 2]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let union_fields = UnionFields::new(
            vec![0, 1, 2],
            vec![
                Field::new("int_field", DataType::Int32, false),
                Field::new("float_field", DataType::Float64, false),
                Field::new("string_field", DataType::Utf8, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            Some(offsets), // Dense union
            children,
        )
        .unwrap();

        run_test(
            Arc::new(union_array),
            vec![
                Some(Variant::Int32(1)),
                Some(Variant::Double(3.2)),
                Some(Variant::from("hello")),
                Some(Variant::Double(32.5)),
                Some(Variant::Int32(34)),
                None,
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary() {
        let values = StringArray::from(vec!["apple", "banana", "cherry", "date"]);
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0), Some(3)]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                None,
                Some(Variant::from("cherry")),
                Some(Variant::from("apple")),
                Some(Variant::from("date")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary_with_nulls() {
        // Test dictionary with null values in the values array
        let values = StringArray::from(vec![Some("a"), None, Some("c")]);
        let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("a")),
                None, // key 1 points to null value
                Some(Variant::from("c")),
                Some(Variant::from("a")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded() {
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_value("apple");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("cherry");
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("cherry")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded_with_nulls() {
        use arrow::array::StringRunBuilder;
        use arrow::datatypes::Int32Type;

        // Test run-end encoded array with nulls
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_null();
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_null();
        builder.append_null();
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                None,
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                None,
                None,
            ],
        );
    }

    /// Converts the given `Array` to a `VariantArray` and tests the conversion
    /// against the expected values. It also tests the handling of nulls by
    /// setting one element to null and verifying the output.
    fn run_test(values: ArrayRef, expected: Vec<Option<Variant>>) {
        // test without nulls
        let variant_array = cast_to_variant(&values).unwrap();
        assert_eq!(variant_array.len(), expected.len());
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(!variant_array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(variant_array.value(i), *value, "mismatch at index {i}");
                }
                None => {
                    assert!(variant_array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }
}

#[cfg(test)]
mod row_builder_tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_primitive_row_builder() {
        // Test Int32Array
        let int_array = Int32Array::from(vec![Some(42), None, Some(100)]);
        let mut row_builder =
            make_arrow_to_variant_row_builder(int_array.data_type(), &int_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        // Test first value
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(0, &mut variant_builder).unwrap();
        variant_builder.finish();

        // Test null value
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(1, &mut variant_builder).unwrap();
        variant_builder.finish();

        // Test second value
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(2, &mut variant_builder).unwrap();
        variant_builder.finish();

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);
        assert_eq!(variant_array.value(0), Variant::Int32(42));
        assert!(variant_array.is_null(1));
        assert_eq!(variant_array.value(2), Variant::Int32(100));
    }

    #[test]
    fn test_string_row_builder() {
        let string_array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let mut row_builder =
            make_arrow_to_variant_row_builder(string_array.data_type(), &string_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(0, &mut variant_builder).unwrap();
        variant_builder.finish();
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(1, &mut variant_builder).unwrap();
        variant_builder.finish();
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(2, &mut variant_builder).unwrap();
        variant_builder.finish();

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);
        assert_eq!(variant_array.value(0), Variant::from("hello"));
        assert!(variant_array.is_null(1));
        assert_eq!(variant_array.value(2), Variant::from("world"));
    }

    #[test]
    fn test_boolean_row_builder() {
        let bool_array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let mut row_builder =
            make_arrow_to_variant_row_builder(bool_array.data_type(), &bool_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(0, &mut variant_builder).unwrap();
        variant_builder.finish();
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(1, &mut variant_builder).unwrap();
        variant_builder.finish();
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(2, &mut variant_builder).unwrap();
        variant_builder.finish();

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);
        assert_eq!(variant_array.value(0), Variant::from(true));
        assert!(variant_array.is_null(1));
        assert_eq!(variant_array.value(2), Variant::from(false));
    }

    #[test]
    fn test_struct_row_builder() {
        use arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;

        // Create a struct array with int and string fields
        let int_field = Field::new("id", DataType::Int32, true);
        let string_field = Field::new("name", DataType::Utf8, true);

        let int_array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let string_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        let struct_array = StructArray::try_new(
            vec![int_field, string_field].into(),
            vec![
                Arc::new(int_array) as ArrayRef,
                Arc::new(string_array) as ArrayRef,
            ],
            None,
        )
        .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(struct_array.data_type(), &struct_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        // Test first row
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(0, &mut variant_builder).unwrap();
        variant_builder.finish();

        // Test second row (with null int field)
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(1, &mut variant_builder).unwrap();
        variant_builder.finish();

        // Test third row (with null string field)
        let mut variant_builder = array_builder.variant_builder();
        row_builder.append_row(2, &mut variant_builder).unwrap();
        variant_builder.finish();

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Check first row - should have both fields
        let first_variant = variant_array.value(0);
        assert_eq!(first_variant.get_object_field("id"), Some(Variant::from(1)));
        assert_eq!(
            first_variant.get_object_field("name"),
            Some(Variant::from("Alice"))
        );

        // Check second row - should have name field but not id (null field omitted)
        let second_variant = variant_array.value(1);
        assert_eq!(second_variant.get_object_field("id"), None); // null field omitted
        assert_eq!(
            second_variant.get_object_field("name"),
            Some(Variant::from("Bob"))
        );

        // Check third row - should have id field but not name (null field omitted)
        let third_variant = variant_array.value(2);
        assert_eq!(third_variant.get_object_field("id"), Some(Variant::from(3)));
        assert_eq!(third_variant.get_object_field("name"), None); // null field omitted
    }

    #[test]
    fn test_run_end_encoded_row_builder() {
        use arrow::array::{Int32Array, RunArray};
        use arrow::datatypes::Int32Type;

        // Create a run-end encoded array: [A, A, B, B, B, C]
        // run_ends: [2, 5, 6]
        // values: ["A", "B", "C"]
        let values = StringArray::from(vec!["A", "B", "C"]);
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(run_array.data_type(), &run_array).unwrap();
        let mut array_builder = VariantArrayBuilder::new(6);

        // Test sequential access (most common case)
        for i in 0..6 {
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut variant_builder).unwrap();
            variant_builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 6);

        // Verify the values
        assert_eq!(variant_array.value(0), Variant::from("A")); // Run 0
        assert_eq!(variant_array.value(1), Variant::from("A")); // Run 0
        assert_eq!(variant_array.value(2), Variant::from("B")); // Run 1
        assert_eq!(variant_array.value(3), Variant::from("B")); // Run 1
        assert_eq!(variant_array.value(4), Variant::from("B")); // Run 1
        assert_eq!(variant_array.value(5), Variant::from("C")); // Run 2
    }

    #[test]
    fn test_run_end_encoded_random_access() {
        use arrow::array::{Int32Array, RunArray};
        use arrow::datatypes::Int32Type;

        // Create a run-end encoded array: [A, A, B, B, B, C]
        let values = StringArray::from(vec!["A", "B", "C"]);
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(run_array.data_type(), &run_array).unwrap();

        // Test random access pattern (backward jumps, forward jumps)
        let access_pattern = [0, 5, 2, 4, 1, 3]; // Mix of all cases
        let expected_values = ["A", "C", "B", "B", "A", "B"];

        for (i, &index) in access_pattern.iter().enumerate() {
            let mut array_builder = VariantArrayBuilder::new(1);
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(index, &mut variant_builder).unwrap();
            variant_builder.finish();

            let variant_array = array_builder.build();
            assert_eq!(variant_array.value(0), Variant::from(expected_values[i]));
        }
    }

    #[test]
    fn test_run_end_encoded_with_nulls() {
        use arrow::array::{Int32Array, RunArray};
        use arrow::datatypes::Int32Type;

        // Create a run-end encoded array with null values: [A, A, null, null, B]
        let values = StringArray::from(vec![Some("A"), None, Some("B")]);
        let run_ends = Int32Array::from(vec![2, 4, 5]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(run_array.data_type(), &run_array).unwrap();
        let mut array_builder = VariantArrayBuilder::new(5);

        // Test sequential access
        for i in 0..5 {
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut variant_builder).unwrap();
            variant_builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 5);

        // Verify the values
        assert_eq!(variant_array.value(0), Variant::from("A")); // Run 0
        assert_eq!(variant_array.value(1), Variant::from("A")); // Run 0
        assert!(variant_array.is_null(2)); // Run 1 (null)
        assert!(variant_array.is_null(3)); // Run 1 (null)
        assert_eq!(variant_array.value(4), Variant::from("B")); // Run 2
    }

    #[test]
    fn test_dictionary_row_builder() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        // Create a dictionary array: keys=[0, 1, 0, 2, 1], values=["apple", "banana", "cherry"]
        let values = StringArray::from(vec!["apple", "banana", "cherry"]);
        let keys = Int32Array::from(vec![0, 1, 0, 2, 1]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(dict_array.data_type(), &dict_array).unwrap();
        let mut array_builder = VariantArrayBuilder::new(5);

        // Test sequential access
        for i in 0..5 {
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut variant_builder).unwrap();
            variant_builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 5);

        // Verify the values match the dictionary lookup
        assert_eq!(variant_array.value(0), Variant::from("apple")); // keys[0] = 0 -> values[0] = "apple"
        assert_eq!(variant_array.value(1), Variant::from("banana")); // keys[1] = 1 -> values[1] = "banana"
        assert_eq!(variant_array.value(2), Variant::from("apple")); // keys[2] = 0 -> values[0] = "apple"
        assert_eq!(variant_array.value(3), Variant::from("cherry")); // keys[3] = 2 -> values[2] = "cherry"
        assert_eq!(variant_array.value(4), Variant::from("banana")); // keys[4] = 1 -> values[1] = "banana"
    }

    #[test]
    fn test_dictionary_with_nulls() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        // Create a dictionary array with null keys: keys=[0, null, 1, null, 2], values=["x", "y", "z"]
        let values = StringArray::from(vec!["x", "y", "z"]);
        let keys = Int32Array::from(vec![Some(0), None, Some(1), None, Some(2)]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(dict_array.data_type(), &dict_array).unwrap();
        let mut array_builder = VariantArrayBuilder::new(5);

        // Test sequential access
        for i in 0..5 {
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut variant_builder).unwrap();
            variant_builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 5);

        // Verify the values and nulls
        assert_eq!(variant_array.value(0), Variant::from("x")); // keys[0] = 0 -> values[0] = "x"
        assert!(variant_array.is_null(1)); // keys[1] = null
        assert_eq!(variant_array.value(2), Variant::from("y")); // keys[2] = 1 -> values[1] = "y"
        assert!(variant_array.is_null(3)); // keys[3] = null
        assert_eq!(variant_array.value(4), Variant::from("z")); // keys[4] = 2 -> values[2] = "z"
    }

    #[test]
    fn test_dictionary_random_access() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        // Create a dictionary array: keys=[0, 1, 2, 0, 1, 2], values=["red", "green", "blue"]
        let values = StringArray::from(vec!["red", "green", "blue"]);
        let keys = Int32Array::from(vec![0, 1, 2, 0, 1, 2]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(dict_array.data_type(), &dict_array).unwrap();

        // Test random access pattern
        let access_pattern = [5, 0, 3, 1, 4, 2]; // Random order
        let expected_values = ["blue", "red", "red", "green", "green", "blue"];

        for (i, &index) in access_pattern.iter().enumerate() {
            let mut array_builder = VariantArrayBuilder::new(1);
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(index, &mut variant_builder).unwrap();
            variant_builder.finish();

            let variant_array = array_builder.build();
            assert_eq!(variant_array.value(0), Variant::from(expected_values[i]));
        }
    }

    #[test]
    fn test_nested_dictionary() {
        use arrow::array::{DictionaryArray, Int32Array, StructArray};
        use arrow::datatypes::{Field, Int32Type};

        // Create a dictionary with struct values
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(id_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(name_array) as ArrayRef,
            ),
        ]);

        let keys = Int32Array::from(vec![0, 1, 0, 2, 1]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(struct_array)).unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(dict_array.data_type(), &dict_array).unwrap();
        let mut array_builder = VariantArrayBuilder::new(5);

        // Test sequential access
        for i in 0..5 {
            let mut variant_builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut variant_builder).unwrap();
            variant_builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 5);

        // Verify the nested struct values
        let first_variant = variant_array.value(0);
        assert_eq!(first_variant.get_object_field("id"), Some(Variant::from(1)));
        assert_eq!(
            first_variant.get_object_field("name"),
            Some(Variant::from("Alice"))
        );

        let second_variant = variant_array.value(1);
        assert_eq!(
            second_variant.get_object_field("id"),
            Some(Variant::from(2))
        );
        assert_eq!(
            second_variant.get_object_field("name"),
            Some(Variant::from("Bob"))
        );

        // Test that repeated keys give same values
        let third_variant = variant_array.value(2);
        assert_eq!(third_variant.get_object_field("id"), Some(Variant::from(1)));
        assert_eq!(
            third_variant.get_object_field("name"),
            Some(Variant::from("Alice"))
        );
    }

    #[test]
    fn test_list_row_builder() {
        use arrow::array::ListArray;

        // Create a list array: [[1, 2], [3, 4, 5], null, []]
        let data = vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            None,
            Some(vec![]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(list_array.data_type(), &list_array).unwrap();
        let mut variant_array_builder = VariantArrayBuilder::new(list_array.len());

        for i in 0..list_array.len() {
            let mut builder = variant_array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = variant_array_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 4);

        // Row 0: [1, 2]
        let row0 = variant_array.value(0);
        let list0 = row0.as_list().unwrap();
        assert_eq!(list0.len(), 2);
        assert_eq!(list0.get(0), Some(Variant::from(1)));
        assert_eq!(list0.get(1), Some(Variant::from(2)));

        // Row 1: [3, 4, 5]
        let row1 = variant_array.value(1);
        let list1 = row1.as_list().unwrap();
        assert_eq!(list1.len(), 3);
        assert_eq!(list1.get(0), Some(Variant::from(3)));
        assert_eq!(list1.get(1), Some(Variant::from(4)));
        assert_eq!(list1.get(2), Some(Variant::from(5)));

        // Row 2: null
        assert!(variant_array.is_null(2));

        // Row 3: []
        let row3 = variant_array.value(3);
        let list3 = row3.as_list().unwrap();
        assert_eq!(list3.len(), 0);
    }

    #[test]
    fn test_large_list_row_builder() {
        use arrow::array::LargeListArray;

        // Create a large list array: [[1, 2], null]
        let data = vec![Some(vec![Some(1i64), Some(2i64)]), None];
        let list_array = LargeListArray::from_iter_primitive::<Int64Type, _, _>(data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(list_array.data_type(), &list_array).unwrap();
        let mut variant_array_builder = VariantArrayBuilder::new(list_array.len());

        for i in 0..list_array.len() {
            let mut builder = variant_array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = variant_array_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 2);

        // Row 0: [1, 2]
        let row0 = variant_array.value(0);
        let list0 = row0.as_list().unwrap();
        assert_eq!(list0.len(), 2);
        assert_eq!(list0.get(0), Some(Variant::from(1i64)));
        assert_eq!(list0.get(1), Some(Variant::from(2i64)));

        // Row 1: null
        assert!(variant_array.is_null(1));
    }

    #[test]
    fn test_sliced_list_row_builder() {
        use arrow::array::ListArray;

        // Create a list array: [[1, 2], [3, 4, 5], [6]]
        let data = vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        // Slice to get just the middle element: [[3, 4, 5]]
        let sliced_array = list_array.slice(1, 1);

        let mut row_builder =
            make_arrow_to_variant_row_builder(sliced_array.data_type(), &sliced_array).unwrap();
        let mut variant_array_builder = VariantArrayBuilder::new(sliced_array.len());

        // Test the single row
        let mut builder = variant_array_builder.variant_builder();
        row_builder.append_row(0, &mut builder).unwrap();
        builder.finish();

        let variant_array = variant_array_builder.build();

        // Verify result
        assert_eq!(variant_array.len(), 1);

        // Row 0: [3, 4, 5]
        let row0 = variant_array.value(0);
        let list0 = row0.as_list().unwrap();
        assert_eq!(list0.len(), 3);
        assert_eq!(list0.get(0), Some(Variant::from(3)));
        assert_eq!(list0.get(1), Some(Variant::from(4)));
        assert_eq!(list0.get(2), Some(Variant::from(5)));
    }

    #[test]
    fn test_nested_list_row_builder() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        // Build the nested structure manually
        let inner_field = Arc::new(Field::new("item", DataType::Int32, true));
        let inner_list_field = Arc::new(Field::new("item", DataType::List(inner_field), true));

        let values_data = vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])];
        let values_list = ListArray::from_iter_primitive::<Int32Type, _, _>(values_data);

        let outer_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 2].into());
        let outer_list = ListArray::new(
            inner_list_field,
            outer_offsets,
            Arc::new(values_list),
            Some(arrow::buffer::NullBuffer::from(vec![true, false])),
        );

        let mut row_builder =
            make_arrow_to_variant_row_builder(outer_list.data_type(), &outer_list).unwrap();
        let mut variant_array_builder = VariantArrayBuilder::new(outer_list.len());

        for i in 0..outer_list.len() {
            let mut builder = variant_array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = variant_array_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 2);

        // Row 0: [[1, 2], [3]]
        let row0 = variant_array.value(0);
        let outer_list0 = row0.as_list().unwrap();
        assert_eq!(outer_list0.len(), 2);

        let inner_list0_0 = outer_list0.get(0).unwrap();
        let inner_list0_0 = inner_list0_0.as_list().unwrap();
        assert_eq!(inner_list0_0.len(), 2);
        assert_eq!(inner_list0_0.get(0), Some(Variant::from(1)));
        assert_eq!(inner_list0_0.get(1), Some(Variant::from(2)));

        let inner_list0_1 = outer_list0.get(1).unwrap();
        let inner_list0_1 = inner_list0_1.as_list().unwrap();
        assert_eq!(inner_list0_1.len(), 1);
        assert_eq!(inner_list0_1.get(0), Some(Variant::from(3)));

        // Row 1: null
        assert!(variant_array.is_null(1));
    }

    #[test]
    fn test_map_row_builder() {
        use arrow::array::{Int32Array, MapArray, StringArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, Fields};
        use std::sync::Arc;

        // Create the entries struct array (key-value pairs)
        let keys = StringArray::from(vec!["key1", "key2", "key3"]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let entries_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![Arc::new(keys), Arc::new(values)],
            None, // No nulls in the entries themselves
        );

        // Create offsets for 4 maps: [0..1], [1..1], [1..1], [1..3]
        // Map 0: {"key1": 1}    (1 entry)
        // Map 1: {}             (0 entries - empty)
        // Map 2: null           (0 entries but NULL via null buffer)
        // Map 3: {"key2": 2, "key3": 3}  (2 entries)
        let offsets = OffsetBuffer::new(vec![0, 1, 1, 1, 3].into());

        // Create null buffer - map at index 2 is NULL
        let null_buffer = Some(NullBuffer::from(vec![true, true, false, true]));

        // Create the map field
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false, // Keys are non-nullable
        ));

        // Create MapArray using try_new
        let map_array = MapArray::try_new(
            map_field,
            offsets,
            entries,
            null_buffer,
            false, // not ordered
        )
        .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(map_array.data_type(), &map_array).unwrap();
        let mut variant_array_builder = VariantArrayBuilder::new(4);

        // Test each row
        for i in 0..4 {
            let mut builder = variant_array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = variant_array_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 4);

        // Map 0: {"key1": 1}
        let map0 = variant_array.value(0);
        let obj0 = map0.as_object().unwrap();
        assert_eq!(obj0.len(), 1);
        assert_eq!(obj0.get("key1"), Some(Variant::from(1)));

        // Map 1: {} (empty object, not null)
        let map1 = variant_array.value(1);
        let obj1 = map1.as_object().unwrap();
        assert_eq!(obj1.len(), 0); // Empty object

        // Map 2: null (actual NULL)
        assert!(variant_array.is_null(2));

        // Map 3: {"key2": 2, "key3": 3}
        let map3 = variant_array.value(3);
        let obj3 = map3.as_object().unwrap();
        assert_eq!(obj3.len(), 2);
        assert_eq!(obj3.get("key2"), Some(Variant::from(2)));
        assert_eq!(obj3.get("key3"), Some(Variant::from(3)));
    }

    #[test]
    fn test_union_sparse_row_builder() {
        use arrow::array::{Float64Array, Int32Array, StringArray, UnionArray};
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{DataType, Field, UnionFields};
        use std::sync::Arc;

        // Create a sparse union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), None, None, None, Some(34), None]);
        let float_array = Float64Array::from(vec![None, Some(3.2), None, Some(32.5), None, None]);
        let string_array = StringArray::from(vec![None, None, Some("hello"), None, None, None]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = UnionFields::new(
            vec![0, 1, 2],
            vec![
                Field::new("int_field", DataType::Int32, false),
                Field::new("float_field", DataType::Float64, false),
                Field::new("string_field", DataType::Utf8, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            None, // Sparse union
            children,
        )
        .unwrap();

        // Test the row builder
        let mut row_builder =
            make_arrow_to_variant_row_builder(union_array.data_type(), &union_array).unwrap();

        let mut variant_builder = VariantArrayBuilder::new(union_array.len());
        for i in 0..union_array.len() {
            let mut builder = variant_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }
        let variant_array = variant_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 6);

        // Row 0: int 1
        assert_eq!(variant_array.value(0), Variant::Int32(1));

        // Row 1: float 3.2
        assert_eq!(variant_array.value(1), Variant::Double(3.2));

        // Row 2: string "hello"
        assert_eq!(variant_array.value(2), Variant::from("hello"));

        // Row 3: float 32.5
        assert_eq!(variant_array.value(3), Variant::Double(32.5));

        // Row 4: int 34
        assert_eq!(variant_array.value(4), Variant::Int32(34));

        // Row 5: null (int array has null at this position)
        assert!(variant_array.is_null(5));
    }

    #[test]
    fn test_union_dense_row_builder() {
        use arrow::array::{Float64Array, Int32Array, StringArray, UnionArray};
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{DataType, Field, UnionFields};
        use std::sync::Arc;

        // Create a dense union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), Some(34), None]);
        let float_array = Float64Array::from(vec![3.2, 32.5]);
        let string_array = StringArray::from(vec!["hello"]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 0, 0, 1, 1, 2]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let union_fields = UnionFields::new(
            vec![0, 1, 2],
            vec![
                Field::new("int_field", DataType::Int32, false),
                Field::new("float_field", DataType::Float64, false),
                Field::new("string_field", DataType::Utf8, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            Some(offsets), // Dense union
            children,
        )
        .unwrap();

        // Test the row builder
        let mut row_builder =
            make_arrow_to_variant_row_builder(union_array.data_type(), &union_array).unwrap();

        let mut variant_builder = VariantArrayBuilder::new(union_array.len());
        for i in 0..union_array.len() {
            let mut builder = variant_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }
        let variant_array = variant_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 6);

        // Row 0: int 1 (offset 0 in int_array)
        assert_eq!(variant_array.value(0), Variant::Int32(1));

        // Row 1: float 3.2 (offset 0 in float_array)
        assert_eq!(variant_array.value(1), Variant::Double(3.2));

        // Row 2: string "hello" (offset 0 in string_array)
        assert_eq!(variant_array.value(2), Variant::from("hello"));

        // Row 3: float 32.5 (offset 1 in float_array)
        assert_eq!(variant_array.value(3), Variant::Double(32.5));

        // Row 4: int 34 (offset 1 in int_array)
        assert_eq!(variant_array.value(4), Variant::Int32(34));

        // Row 5: null (offset 2 in int_array, which has null)
        assert!(variant_array.is_null(5));
    }

    #[test]
    fn test_union_sparse_type_ids_row_builder() {
        use arrow::array::{Int32Array, StringArray, UnionArray};
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{DataType, Field, UnionFields};
        use std::sync::Arc;

        // Create a sparse union with non-contiguous type IDs (1, 3)
        let int_array = Int32Array::from(vec![Some(42), None]);
        let string_array = StringArray::from(vec![None, Some("test")]);
        let type_ids = [1, 3].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = UnionFields::new(
            vec![1, 3], // Non-contiguous type IDs
            vec![
                Field::new("int_field", DataType::Int32, false),
                Field::new("string_field", DataType::Utf8, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![Arc::new(int_array), Arc::new(string_array)];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            None, // Sparse union
            children,
        )
        .unwrap();

        // Test the row builder
        let mut row_builder =
            make_arrow_to_variant_row_builder(union_array.data_type(), &union_array).unwrap();

        let mut variant_builder = VariantArrayBuilder::new(union_array.len());
        for i in 0..union_array.len() {
            let mut builder = variant_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }
        let variant_array = variant_builder.build();

        // Verify results
        assert_eq!(variant_array.len(), 2);

        // Row 0: int 42 (type_id = 1)
        assert_eq!(variant_array.value(0), Variant::Int32(42));

        // Row 1: string "test" (type_id = 3)
        assert_eq!(variant_array.value(1), Variant::from("test"));
    }

    #[test]
    fn test_decimal32_row_builder() {
        use arrow::array::Decimal32Array;
        use parquet_variant::VariantDecimal4;

        // Test Decimal32Array with scale 2 (e.g., for currency: 12.34)
        let decimal_array = Decimal32Array::from(vec![Some(1234), None, Some(-5678)])
            .with_precision_and_scale(9, 2)
            .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(decimal_array.data_type(), &decimal_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..decimal_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: 12.34 (1234 with scale 2)
        assert_eq!(
            variant_array.value(0),
            Variant::from(VariantDecimal4::try_new(1234, 2).unwrap())
        );

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: -56.78 (-5678 with scale 2)
        assert_eq!(
            variant_array.value(2),
            Variant::from(VariantDecimal4::try_new(-5678, 2).unwrap())
        );
    }

    #[test]
    fn test_decimal128_row_builder() {
        use arrow::array::Decimal128Array;
        use parquet_variant::VariantDecimal16;

        // Test Decimal128Array with negative scale (multiply by 10^|scale|)
        let decimal_array = Decimal128Array::from(vec![Some(123), None, Some(456)])
            .with_precision_and_scale(10, -2)
            .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(decimal_array.data_type(), &decimal_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..decimal_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: 123 * 10^2 = 12300 with scale 0 (negative scale handling)
        assert_eq!(
            variant_array.value(0),
            Variant::from(VariantDecimal16::try_new(12300, 0).unwrap())
        );

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 456 * 10^2 = 45600 with scale 0
        assert_eq!(
            variant_array.value(2),
            Variant::from(VariantDecimal16::try_new(45600, 0).unwrap())
        );
    }

    #[test]
    fn test_decimal256_overflow_row_builder() {
        use arrow::array::Decimal256Array;
        use arrow::datatypes::i256;

        // Test Decimal256Array with a value that overflows i128
        let large_value = i256::from_i128(i128::MAX) + i256::from(1); // Overflows i128
        let decimal_array = Decimal256Array::from(vec![Some(large_value), Some(i256::from(123))])
            .with_precision_and_scale(76, 3)
            .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(decimal_array.data_type(), &decimal_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(2);

        for i in 0..decimal_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 2);

        // Row 0: overflow value becomes Variant::Null
        assert_eq!(variant_array.value(0), Variant::Null);

        // Row 1: normal value converts successfully
        assert_eq!(
            variant_array.value(1),
            Variant::from(VariantDecimal16::try_new(123, 3).unwrap())
        );
    }

    #[test]
    fn test_binary_row_builder() {
        use arrow::array::BinaryArray;

        // Test BinaryArray with various binary data
        let binary_data = vec![
            Some(b"hello".as_slice()),
            None,
            Some(b"\x00\x01\x02\xFF".as_slice()),
            Some(b"".as_slice()), // Empty binary
        ];
        let binary_array = BinaryArray::from(binary_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(binary_array.data_type(), &binary_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..binary_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: "hello" bytes
        assert_eq!(variant_array.value(0), Variant::from(b"hello".as_slice()));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: binary with special bytes
        let bytes = [0x00, 0x01, 0x02, 0xFF];
        assert_eq!(variant_array.value(2), Variant::from(bytes.as_slice()));

        // Row 3: empty binary
        let bytes = [];
        assert_eq!(variant_array.value(3), Variant::from(bytes.as_slice()));
    }

    #[test]
    fn test_large_binary_row_builder() {
        use arrow::array::LargeBinaryArray;

        // Test LargeBinaryArray
        let binary_data = vec![
            Some(b"large binary data".as_slice()),
            None,
            Some(b"another large chunk".as_slice()),
        ];
        let large_binary_array = LargeBinaryArray::from(binary_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(large_binary_array.data_type(), &large_binary_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..large_binary_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: large binary data
        assert_eq!(
            variant_array.value(0),
            Variant::from(b"large binary data".as_slice())
        );

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: another large chunk
        assert_eq!(
            variant_array.value(2),
            Variant::from(b"another large chunk".as_slice())
        );
    }

    #[test]
    fn test_binary_view_row_builder() {
        use arrow::array::BinaryViewArray;

        // Test BinaryViewArray
        let binary_data = vec![
            Some(b"short".as_slice()),
            None,
            Some(b"this is a longer binary view that exceeds inline storage".as_slice()),
        ];
        let binary_view_array = BinaryViewArray::from(binary_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(binary_view_array.data_type(), &binary_view_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..binary_view_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: short binary
        assert_eq!(variant_array.value(0), Variant::from(b"short".as_slice()));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: long binary view
        assert_eq!(
            variant_array.value(2),
            Variant::from(b"this is a longer binary view that exceeds inline storage".as_slice())
        );
    }

    #[test]
    fn test_fixed_size_binary_row_builder() {
        use arrow::array::FixedSizeBinaryArray;

        // Test FixedSizeBinaryArray with 4-byte values
        let binary_data = vec![
            Some([0x01, 0x02, 0x03, 0x04]),
            None,
            Some([0xFF, 0xFE, 0xFD, 0xFC]),
        ];
        let fixed_binary_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(binary_data.into_iter(), 4)
                .unwrap();

        let mut row_builder =
            make_arrow_to_variant_row_builder(fixed_binary_array.data_type(), &fixed_binary_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..fixed_binary_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: fixed size binary
        let bytes = [0x01, 0x02, 0x03, 0x04];
        assert_eq!(variant_array.value(0), Variant::from(bytes.as_slice()));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: another fixed size binary
        let bytes = [0xFF, 0xFE, 0xFD, 0xFC];
        assert_eq!(variant_array.value(2), Variant::from(bytes.as_slice()));
    }

    #[test]
    fn test_utf8_view_row_builder() {
        use arrow::array::StringViewArray;

        // Test StringViewArray (Utf8View)
        let string_data = vec![
            Some("short"),
            None,
            Some("this is a much longer string that will be stored out-of-line in the buffer"),
        ];
        let string_view_array = StringViewArray::from(string_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(string_view_array.data_type(), &string_view_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..string_view_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: short string
        assert_eq!(variant_array.value(0), Variant::from("short"));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: long string view
        assert_eq!(
            variant_array.value(2),
            Variant::from(
                "this is a much longer string that will be stored out-of-line in the buffer"
            )
        );
    }

    #[test]
    fn test_timestamp_second_row_builder() {
        use arrow::array::TimestampSecondArray;

        // Test TimestampSecondArray without timezone
        let timestamp_data = vec![
            Some(1609459200), // 2021-01-01 00:00:00 UTC
            None,
            Some(1640995200), // 2022-01-01 00:00:00 UTC
        ];
        let timestamp_array = TimestampSecondArray::from(timestamp_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(timestamp_array.data_type(), &timestamp_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..timestamp_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: 2021-01-01 00:00:00 (no timezone -> NaiveDateTime -> TimestampNtzMicros)
        let expected_naive = DateTime::from_timestamp(1609459200, 0).unwrap().naive_utc();
        assert_eq!(variant_array.value(0), Variant::from(expected_naive));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 2022-01-01 00:00:00
        let expected_naive2 = DateTime::from_timestamp(1640995200, 0).unwrap().naive_utc();
        assert_eq!(variant_array.value(2), Variant::from(expected_naive2));
    }

    #[test]
    fn test_timestamp_with_timezone_row_builder() {
        use arrow::array::TimestampMicrosecondArray;
        use chrono::DateTime;

        // Test TimestampMicrosecondArray with timezone
        let timestamp_data = vec![
            Some(1609459200000000), // 2021-01-01 00:00:00 UTC (in microseconds)
            None,
            Some(1640995200000000), // 2022-01-01 00:00:00 UTC (in microseconds)
        ];
        let timezone = "UTC".to_string();
        let timestamp_array =
            TimestampMicrosecondArray::from(timestamp_data).with_timezone(timezone.clone());

        let mut row_builder =
            make_arrow_to_variant_row_builder(timestamp_array.data_type(), &timestamp_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..timestamp_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: 2021-01-01 00:00:00 UTC (with timezone -> DateTime<Utc> -> TimestampMicros)
        let expected_utc = DateTime::from_timestamp(1609459200, 0).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_utc));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 2022-01-01 00:00:00 UTC
        let expected_utc2 = DateTime::from_timestamp(1640995200, 0).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_utc2));
    }

    #[test]
    fn test_timestamp_nanosecond_precision_row_builder() {
        use arrow::array::TimestampNanosecondArray;

        // Test TimestampNanosecondArray with nanosecond precision
        let timestamp_data = vec![
            Some(1609459200123456789), // 2021-01-01 00:00:00.123456789 UTC
            None,
            Some(1609459200000000000), // 2021-01-01 00:00:00.000000000 UTC (no fractional seconds)
        ];
        let timestamp_array = TimestampNanosecondArray::from(timestamp_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(timestamp_array.data_type(), &timestamp_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..timestamp_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: with nanoseconds -> should use TimestampNtzNanos
        let expected_with_nanos = DateTime::from_timestamp(1609459200, 123456789)
            .unwrap()
            .naive_utc();
        assert_eq!(variant_array.value(0), Variant::from(expected_with_nanos));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: no fractional seconds -> should use TimestampNtzMicros
        let expected_no_nanos = DateTime::from_timestamp(1609459200, 0).unwrap().naive_utc();
        assert_eq!(variant_array.value(2), Variant::from(expected_no_nanos));
    }

    #[test]
    fn test_timestamp_millisecond_row_builder() {
        use arrow::array::TimestampMillisecondArray;

        // Test TimestampMillisecondArray
        let timestamp_data = vec![
            Some(1609459200123), // 2021-01-01 00:00:00.123 UTC
            None,
            Some(1609459200000), // 2021-01-01 00:00:00.000 UTC
        ];
        let timestamp_array = TimestampMillisecondArray::from(timestamp_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(timestamp_array.data_type(), &timestamp_array)
                .unwrap();

        let mut array_builder = VariantArrayBuilder::new(3);

        for i in 0..timestamp_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 3);

        // Row 0: with milliseconds -> TimestampNtzMicros (123ms = 123000000ns)
        let expected_with_millis = DateTime::from_timestamp(1609459200, 123000000)
            .unwrap()
            .naive_utc();
        assert_eq!(variant_array.value(0), Variant::from(expected_with_millis));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: no fractional seconds -> TimestampNtzMicros
        let expected_no_millis = DateTime::from_timestamp(1609459200, 0).unwrap().naive_utc();
        assert_eq!(variant_array.value(2), Variant::from(expected_no_millis));
    }

    #[test]
    fn test_date32_row_builder() {
        use arrow::array::Date32Array;
        use chrono::NaiveDate;

        // Test Date32Array with various dates
        let date_data = vec![
            Some(0), // 1970-01-01
            None,
            Some(19723),   // 2024-01-01 (days since epoch)
            Some(-719162), // 0001-01-01 (near minimum)
        ];
        let date_array = Date32Array::from(date_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(date_array.data_type(), &date_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..date_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 1970-01-01 (epoch)
        let expected_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_epoch));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 2024-01-01
        let expected_2024 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_2024));

        // Row 3: 0001-01-01 (near minimum date)
        let expected_min = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_min));
    }

    #[test]
    fn test_date64_row_builder() {
        use arrow::array::Date64Array;
        use chrono::NaiveDate;

        // Test Date64Array with various dates (milliseconds since epoch)
        let date_data = vec![
            Some(0), // 1970-01-01
            None,
            Some(1704067200000), // 2024-01-01 (milliseconds since epoch)
            Some(86400000),      // 1970-01-02
        ];
        let date_array = Date64Array::from(date_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(date_array.data_type(), &date_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..date_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 1970-01-01 (epoch)
        let expected_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_epoch));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 2024-01-01
        let expected_2024 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_2024));

        // Row 3: 1970-01-02
        let expected_next_day = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_next_day));
    }

    #[test]
    fn test_time32_second_row_builder() {
        use arrow::array::Time32SecondArray;
        use chrono::NaiveTime;

        // Test Time32SecondArray with various times (seconds since midnight)
        let time_data = vec![
            Some(0), // 00:00:00
            None,
            Some(3661),  // 01:01:01
            Some(86399), // 23:59:59
        ];
        let time_array = Time32SecondArray::from(time_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(time_array.data_type(), &time_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..time_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 00:00:00 (midnight)
        let expected_midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_midnight));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 01:01:01
        let expected_time = NaiveTime::from_hms_opt(1, 1, 1).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_time));

        // Row 3: 23:59:59 (last second of day)
        let expected_last = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_last));
    }

    #[test]
    fn test_time32_millisecond_row_builder() {
        use arrow::array::Time32MillisecondArray;
        use chrono::NaiveTime;

        // Test Time32MillisecondArray with various times (milliseconds since midnight)
        let time_data = vec![
            Some(0), // 00:00:00.000
            None,
            Some(3661123),  // 01:01:01.123
            Some(86399999), // 23:59:59.999
        ];
        let time_array = Time32MillisecondArray::from(time_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(time_array.data_type(), &time_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..time_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 00:00:00.000 (midnight)
        let expected_midnight = NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_midnight));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 01:01:01.123
        let expected_time = NaiveTime::from_hms_milli_opt(1, 1, 1, 123).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_time));

        // Row 3: 23:59:59.999 (last millisecond of day)
        let expected_last = NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_last));
    }

    #[test]
    fn test_time64_microsecond_row_builder() {
        use arrow::array::Time64MicrosecondArray;
        use chrono::NaiveTime;

        // Test Time64MicrosecondArray with various times (microseconds since midnight)
        let time_data = vec![
            Some(0), // 00:00:00.000000
            None,
            Some(3661123456),  // 01:01:01.123456
            Some(86399999999), // 23:59:59.999999
        ];
        let time_array = Time64MicrosecondArray::from(time_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(time_array.data_type(), &time_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..time_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 00:00:00.000000 (midnight)
        let expected_midnight = NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_midnight));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 01:01:01.123456
        let expected_time = NaiveTime::from_hms_micro_opt(1, 1, 1, 123456).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_time));

        // Row 3: 23:59:59.999999 (last microsecond of day)
        let expected_last = NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_last));
    }

    #[test]
    fn test_time64_nanosecond_row_builder() {
        use arrow::array::Time64NanosecondArray;
        use chrono::NaiveTime;

        // Test Time64NanosecondArray with various times (nanoseconds since midnight)
        let time_data = vec![
            Some(0), // 00:00:00.000000000
            None,
            Some(3661123456789),  // 01:01:01.123456789
            Some(86399999999999), // 23:59:59.999999999
        ];
        let time_array = Time64NanosecondArray::from(time_data);

        let mut row_builder =
            make_arrow_to_variant_row_builder(time_array.data_type(), &time_array).unwrap();

        let mut array_builder = VariantArrayBuilder::new(4);

        for i in 0..time_array.len() {
            let mut builder = array_builder.variant_builder();
            row_builder.append_row(i, &mut builder).unwrap();
            builder.finish();
        }

        let variant_array = array_builder.build();
        assert_eq!(variant_array.len(), 4);

        // Row 0: 00:00:00.000000000 (midnight)
        let expected_midnight = NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap();
        assert_eq!(variant_array.value(0), Variant::from(expected_midnight));

        // Row 1: null
        assert!(variant_array.is_null(1));

        // Row 2: 01:01:01.123456789 -> truncated to 01:01:01.123456000 (microsecond precision)
        let expected_time = NaiveTime::from_hms_micro_opt(1, 1, 1, 123456).unwrap();
        assert_eq!(variant_array.value(2), Variant::from(expected_time));

        // Row 3: 23:59:59.999999999 -> truncated to 23:59:59.999999000 (microsecond precision)
        let expected_last = NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap();
        assert_eq!(variant_array.value(3), Variant::from(expected_last));
    }
}
