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

//! Module for unshredding VariantArray by folding typed_value columns back into the value column.

use crate::arrow_to_variant::ListLikeArray;
use crate::{BorrowedShreddingState, VariantArray, VariantValueArrayBuilder};
use arrow::array::{
    Array, AsArray as _, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    GenericListArray, GenericListViewArray, PrimitiveArray, StringArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Decimal32Type, Decimal64Type, Decimal128Type,
    DecimalType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::error::{ArrowError, Result};
use arrow::temporal_conversions::time64us_to_time;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use parquet_variant::{
    ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal4, VariantDecimal8,
    VariantDecimal16, VariantDecimalType, VariantMetadata,
};
use std::marker::PhantomData;
use uuid::Uuid;

/// Removes all (nested) typed_value columns from a VariantArray by converting them back to binary
/// variant and merging the resulting values back into the value column.
///
/// This function efficiently converts a shredded VariantArray back to an unshredded form where all
/// data resides in the value column.
///
/// # Arguments
/// * `array` - The VariantArray to unshred
///
/// # Returns
/// A new VariantArray with all data in the value column and no typed_value column
///
/// # Errors
/// - If the shredded data contains spec violations (e.g., field name conflicts)
/// - If unsupported data types are encountered in typed_value columns
pub fn unshred_variant(array: &VariantArray) -> Result<VariantArray> {
    // Check if already unshredded (optimization for common case)
    if array.typed_value_field().is_none() && array.value_field().is_some() {
        return Ok(array.clone());
    }

    // NOTE: None/None at top-level is technically invalid, but the shredding spec requires us to
    // emit `Variant::Null` when a required value is missing.
    let nulls = array.nulls();
    let mut row_builder = UnshredVariantRowBuilder::try_new_opt(array.shredding_state().borrow())?
        .unwrap_or_else(|| UnshredVariantRowBuilder::null(nulls));

    let metadata = array.metadata_field();
    let mut value_builder = VariantValueArrayBuilder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            value_builder.append_null();
        } else {
            let metadata = VariantMetadata::new(metadata.value(i));
            let mut value_builder = value_builder.builder_ext(&metadata);
            row_builder.append_row(&mut value_builder, &metadata, i)?;
        }
    }

    let value = value_builder.build()?;
    Ok(VariantArray::from_parts(
        metadata.clone(),
        Some(value),
        None,
        nulls.cloned(),
    ))
}

/// Row builder for converting shredded VariantArray rows back to unshredded form
enum UnshredVariantRowBuilder<'a> {
    PrimitiveInt8(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Int8Type>>),
    PrimitiveInt16(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Int16Type>>),
    PrimitiveInt32(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Int32Type>>),
    PrimitiveInt64(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Int64Type>>),
    PrimitiveFloat32(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Float32Type>>),
    PrimitiveFloat64(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Float64Type>>),
    Decimal32(DecimalUnshredRowBuilder<'a, Decimal32Type, VariantDecimal4>),
    Decimal64(DecimalUnshredRowBuilder<'a, Decimal64Type, VariantDecimal8>),
    Decimal128(DecimalUnshredRowBuilder<'a, Decimal128Type, VariantDecimal16>),
    PrimitiveDate32(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Date32Type>>),
    PrimitiveTime64(UnshredPrimitiveRowBuilder<'a, PrimitiveArray<Time64MicrosecondType>>),
    TimestampMicrosecond(TimestampUnshredRowBuilder<'a, TimestampMicrosecondType>),
    TimestampNanosecond(TimestampUnshredRowBuilder<'a, TimestampNanosecondType>),
    PrimitiveBoolean(UnshredPrimitiveRowBuilder<'a, BooleanArray>),
    PrimitiveString(UnshredPrimitiveRowBuilder<'a, StringArray>),
    PrimitiveBinaryView(UnshredPrimitiveRowBuilder<'a, BinaryViewArray>),
    PrimitiveUuid(UnshredPrimitiveRowBuilder<'a, FixedSizeBinaryArray>),
    List(ListUnshredVariantBuilder<'a, GenericListArray<i32>>),
    LargeList(ListUnshredVariantBuilder<'a, GenericListArray<i64>>),
    ListView(ListUnshredVariantBuilder<'a, GenericListViewArray<i32>>),
    LargeListView(ListUnshredVariantBuilder<'a, GenericListViewArray<i64>>),
    FixedSizeList(ListUnshredVariantBuilder<'a, FixedSizeListArray>),
    Struct(StructUnshredVariantBuilder<'a>),
    ValueOnly(ValueOnlyUnshredVariantBuilder<'a>),
    Null(NullUnshredVariantBuilder<'a>),
}

impl<'a> UnshredVariantRowBuilder<'a> {
    /// Creates an all-null row builder.
    fn null(nulls: Option<&'a NullBuffer>) -> Self {
        Self::Null(NullUnshredVariantBuilder::new(nulls))
    }

    /// Appends a single row at the given value index to the supplied builder.
    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        match self {
            Self::PrimitiveInt8(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveInt16(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveInt32(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveInt64(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveFloat32(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveFloat64(b) => b.append_row(builder, metadata, index),
            Self::Decimal32(b) => b.append_row(builder, metadata, index),
            Self::Decimal64(b) => b.append_row(builder, metadata, index),
            Self::Decimal128(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveDate32(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveTime64(b) => b.append_row(builder, metadata, index),
            Self::TimestampMicrosecond(b) => b.append_row(builder, metadata, index),
            Self::TimestampNanosecond(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveBoolean(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveString(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveBinaryView(b) => b.append_row(builder, metadata, index),
            Self::PrimitiveUuid(b) => b.append_row(builder, metadata, index),
            Self::List(b) => b.append_row(builder, metadata, index),
            Self::LargeList(b) => b.append_row(builder, metadata, index),
            Self::ListView(b) => b.append_row(builder, metadata, index),
            Self::LargeListView(b) => b.append_row(builder, metadata, index),
            Self::FixedSizeList(b) => b.append_row(builder, metadata, index),
            Self::Struct(b) => b.append_row(builder, metadata, index),
            Self::ValueOnly(b) => b.append_row(builder, metadata, index),
            Self::Null(b) => b.append_row(builder, metadata, index),
        }
    }

    /// Creates a new UnshredVariantRowBuilder from shredding state
    /// Returns None for None/None case - caller decides how to handle based on context
    fn try_new_opt(shredding_state: BorrowedShreddingState<'a>) -> Result<Option<Self>> {
        let value = shredding_state.value_field();
        let typed_value = shredding_state.typed_value_field();
        let Some(typed_value) = typed_value else {
            // Copy the value across directly, if present. Else caller decides what to do.
            return Ok(value.map(|v| Self::ValueOnly(ValueOnlyUnshredVariantBuilder::new(v))));
        };

        // Has typed_value -> determine type and create appropriate builder
        macro_rules! primitive_builder {
            ($enum_variant:ident, $cast_fn:ident) => {
                Self::$enum_variant(UnshredPrimitiveRowBuilder::new(
                    value,
                    typed_value.$cast_fn(),
                ))
            };
        }

        let builder = match typed_value.data_type() {
            DataType::Int8 => primitive_builder!(PrimitiveInt8, as_primitive),
            DataType::Int16 => primitive_builder!(PrimitiveInt16, as_primitive),
            DataType::Int32 => primitive_builder!(PrimitiveInt32, as_primitive),
            DataType::Int64 => primitive_builder!(PrimitiveInt64, as_primitive),
            DataType::Float32 => primitive_builder!(PrimitiveFloat32, as_primitive),
            DataType::Float64 => primitive_builder!(PrimitiveFloat64, as_primitive),
            DataType::Decimal32(p, s) if VariantDecimal4::is_valid_precision_and_scale(p, s) => {
                Self::Decimal32(DecimalUnshredRowBuilder::new(value, typed_value, *s as _))
            }
            DataType::Decimal64(p, s) if VariantDecimal8::is_valid_precision_and_scale(p, s) => {
                Self::Decimal64(DecimalUnshredRowBuilder::new(value, typed_value, *s as _))
            }
            DataType::Decimal128(p, s) if VariantDecimal16::is_valid_precision_and_scale(p, s) => {
                Self::Decimal128(DecimalUnshredRowBuilder::new(value, typed_value, *s as _))
            }
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "{} is not a valid variant shredding type",
                    typed_value.data_type()
                )));
            }
            DataType::Date32 => primitive_builder!(PrimitiveDate32, as_primitive),
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive_builder!(PrimitiveTime64, as_primitive)
            }
            DataType::Time64(time_unit) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Time64({time_unit}) is not a valid variant shredding type",
                )));
            }
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => Self::TimestampMicrosecond(
                TimestampUnshredRowBuilder::new(value, typed_value, timezone.is_some()),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Self::TimestampNanosecond(
                TimestampUnshredRowBuilder::new(value, typed_value, timezone.is_some()),
            ),
            DataType::Timestamp(time_unit, _) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Timestamp({time_unit}) is not a valid variant shredding type",
                )));
            }
            DataType::Boolean => primitive_builder!(PrimitiveBoolean, as_boolean),
            DataType::Utf8 => primitive_builder!(PrimitiveString, as_string),
            DataType::BinaryView => primitive_builder!(PrimitiveBinaryView, as_binary_view),
            DataType::FixedSizeBinary(16) => {
                primitive_builder!(PrimitiveUuid, as_fixed_size_binary)
            }
            DataType::FixedSizeBinary(size) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "FixedSizeBinary({size}) is not a valid variant shredding type",
                )));
            }
            DataType::Struct(_) => Self::Struct(StructUnshredVariantBuilder::try_new(
                value,
                typed_value.as_struct(),
            )?),
            DataType::List(_) => Self::List(ListUnshredVariantBuilder::try_new(
                value,
                typed_value.as_list(),
            )?),
            DataType::LargeList(_) => Self::LargeList(ListUnshredVariantBuilder::try_new(
                value,
                typed_value.as_list(),
            )?),
            DataType::ListView(_) => Self::ListView(ListUnshredVariantBuilder::try_new(
                value,
                typed_value.as_list_view(),
            )?),
            DataType::LargeListView(_) => Self::LargeListView(ListUnshredVariantBuilder::try_new(
                value,
                typed_value.as_list_view(),
            )?),
            DataType::FixedSizeList(_, _) => Self::FixedSizeList(
                ListUnshredVariantBuilder::try_new(value, typed_value.as_fixed_size_list())?,
            ),
            _ => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unshredding not yet supported for type: {}",
                    typed_value.data_type()
                )));
            }
        };
        Ok(Some(builder))
    }
}

/// Builder for arrays with neither typed_value nor value (all NULL/Variant::Null)
struct NullUnshredVariantBuilder<'a> {
    nulls: Option<&'a NullBuffer>,
}

impl<'a> NullUnshredVariantBuilder<'a> {
    fn new(nulls: Option<&'a NullBuffer>) -> Self {
        Self { nulls }
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        _metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        if self.nulls.is_some_and(|nulls| nulls.is_null(index)) {
            builder.append_null();
        } else {
            builder.append_value(Variant::Null);
        }
        Ok(())
    }
}

/// Builder for arrays that only have value column (already unshredded)
struct ValueOnlyUnshredVariantBuilder<'a> {
    value: &'a arrow::array::BinaryViewArray,
}

impl<'a> ValueOnlyUnshredVariantBuilder<'a> {
    fn new(value: &'a BinaryViewArray) -> Self {
        Self { value }
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        if self.value.is_null(index) {
            builder.append_null();
        } else {
            let variant = Variant::new_with_metadata(metadata.clone(), self.value.value(index));
            builder.append_value(variant);
        }
        Ok(())
    }
}

/// Extension trait that directly adds row builder support for arrays that correspond to primitive
/// variant types.
trait AppendToVariantBuilder: Array {
    fn append_to_variant_builder(
        &self,
        builder: &mut impl VariantBuilderExt,
        index: usize,
    ) -> Result<()>;
}

/// Macro that handles the unshredded case (typed_value is missing or NULL) and returns early if
/// handled.  If not handled (shredded case), validates and returns the extracted value.
macro_rules! handle_unshredded_case {
    ($self:expr, $builder:expr, $metadata:expr, $index:expr, $partial_shredding:expr) => {{
        let value = $self.value.as_ref().filter(|v| v.is_valid($index));
        let value = value.map(|v| Variant::new_with_metadata($metadata.clone(), v.value($index)));

        // If typed_value is null, handle unshredded case and return early
        if $self.typed_value.is_null($index) {
            match value {
                Some(value) => $builder.append_value(value),
                None => $builder.append_null(),
            }
            return Ok(());
        }

        // Only partial shredding allows value and typed_value to both be non-NULL
        if !$partial_shredding && value.is_some() {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid shredded variant: both value and typed_value are non-null".to_string(),
            ));
        }

        // Return the extracted value for the partial shredded case
        value
    }};
}

/// Generic unshred builder that works with any Array implementing AppendToVariantBuilder
struct UnshredPrimitiveRowBuilder<'a, T> {
    value: Option<&'a BinaryViewArray>,
    typed_value: &'a T,
}

impl<'a, T: AppendToVariantBuilder> UnshredPrimitiveRowBuilder<'a, T> {
    fn new(value: Option<&'a BinaryViewArray>, typed_value: &'a T) -> Self {
        Self { value, typed_value }
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        handle_unshredded_case!(self, builder, metadata, index, false);

        // If we get here, typed_value is valid and value is NULL
        self.typed_value.append_to_variant_builder(builder, index)
    }
}

// Macro to generate AppendToVariantBuilder implementations with optional value transformation
macro_rules! impl_append_to_variant_builder {
    ($array_type:ty $(, |$v:ident| $transform:expr)? ) => {
        impl AppendToVariantBuilder for $array_type {
            fn append_to_variant_builder(
                &self,
                builder: &mut impl VariantBuilderExt,
                index: usize,
            ) -> Result<()> {
                let value = self.value(index);
                $(
                    let $v = value;
                    let value = $transform;
                )?
                builder.append_value(value);
                Ok(())
            }
        }
    };
}

impl_append_to_variant_builder!(BooleanArray);
impl_append_to_variant_builder!(StringArray);
impl_append_to_variant_builder!(BinaryViewArray);
impl_append_to_variant_builder!(PrimitiveArray<Int8Type>);
impl_append_to_variant_builder!(PrimitiveArray<Int16Type>);
impl_append_to_variant_builder!(PrimitiveArray<Int32Type>);
impl_append_to_variant_builder!(PrimitiveArray<Int64Type>);
impl_append_to_variant_builder!(PrimitiveArray<Float32Type>);
impl_append_to_variant_builder!(PrimitiveArray<Float64Type>);

impl_append_to_variant_builder!(PrimitiveArray<Date32Type>, |days_since_epoch| {
    Date32Type::to_naive_date(days_since_epoch)
});

impl_append_to_variant_builder!(
    PrimitiveArray<Time64MicrosecondType>,
    |micros_since_midnight| {
        time64us_to_time(micros_since_midnight).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid Time64 microsecond value: {micros_since_midnight}"
            ))
        })?
    }
);

// UUID from FixedSizeBinary(16)
// NOTE: FixedSizeBinaryArray guarantees the byte length, so we can safely unwrap
impl_append_to_variant_builder!(FixedSizeBinaryArray, |bytes| {
    Uuid::from_slice(bytes).unwrap()
});

/// Trait for timestamp types to handle conversion to `DateTime<Utc>`
trait TimestampType: ArrowPrimitiveType<Native = i64> {
    fn to_datetime_utc(value: i64) -> Result<DateTime<Utc>>;
}

impl TimestampType for TimestampMicrosecondType {
    fn to_datetime_utc(micros: i64) -> Result<DateTime<Utc>> {
        DateTime::from_timestamp_micros(micros).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid timestamp microsecond value: {micros}"
            ))
        })
    }
}

impl TimestampType for TimestampNanosecondType {
    fn to_datetime_utc(nanos: i64) -> Result<DateTime<Utc>> {
        Ok(DateTime::from_timestamp_nanos(nanos))
    }
}

/// Generic builder for timestamp types that handles timezone-aware conversion
struct TimestampUnshredRowBuilder<'a, T: TimestampType> {
    value: Option<&'a BinaryViewArray>,
    typed_value: &'a PrimitiveArray<T>,
    has_timezone: bool,
}

impl<'a, T: TimestampType> TimestampUnshredRowBuilder<'a, T> {
    fn new(
        value: Option<&'a BinaryViewArray>,
        typed_value: &'a dyn Array,
        has_timezone: bool,
    ) -> Self {
        Self {
            value,
            typed_value: typed_value.as_primitive(),
            has_timezone,
        }
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        handle_unshredded_case!(self, builder, metadata, index, false);

        // If we get here, typed_value is valid and value is NULL
        let timestamp_value = self.typed_value.value(index);
        let dt = T::to_datetime_utc(timestamp_value)?;
        if self.has_timezone {
            builder.append_value(dt);
        } else {
            builder.append_value(dt.naive_utc());
        }
        Ok(())
    }
}

/// Generic builder for decimal unshredding
struct DecimalUnshredRowBuilder<'a, A: DecimalType, V>
where
    V: VariantDecimalType<Native = A::Native>,
{
    value: Option<&'a BinaryViewArray>,
    typed_value: &'a PrimitiveArray<A>,
    scale: i8,
    _phantom: PhantomData<V>,
}

impl<'a, A: DecimalType, V> DecimalUnshredRowBuilder<'a, A, V>
where
    V: VariantDecimalType<Native = A::Native>,
{
    fn new(value: Option<&'a BinaryViewArray>, typed_value: &'a dyn Array, scale: i8) -> Self {
        Self {
            value,
            typed_value: typed_value.as_primitive(),
            scale,
            _phantom: PhantomData,
        }
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        handle_unshredded_case!(self, builder, metadata, index, false);

        let raw = self.typed_value.value(index);
        let variant = V::try_new_with_signed_scale(raw, self.scale)?;
        builder.append_value(variant);
        Ok(())
    }
}

/// Builder for unshredding struct/object types with nested fields
struct StructUnshredVariantBuilder<'a> {
    value: Option<&'a arrow::array::BinaryViewArray>,
    typed_value: &'a arrow::array::StructArray,
    field_unshredders: IndexMap<&'a str, Option<UnshredVariantRowBuilder<'a>>>,
}

impl<'a> StructUnshredVariantBuilder<'a> {
    fn try_new(value: Option<&'a BinaryViewArray>, typed_value: &'a StructArray) -> Result<Self> {
        // Create unshredders for each field in constructor
        let mut field_unshredders = IndexMap::new();
        for (field, field_array) in typed_value.fields().iter().zip(typed_value.columns()) {
            // Factory returns None for None/None case -- these are missing fields we should skip
            let Some(field_array) = field_array.as_struct_opt() else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Invalid shredded variant object field: expected Struct, got {}",
                    field_array.data_type()
                )));
            };
            let field_unshredder = UnshredVariantRowBuilder::try_new_opt(field_array.try_into()?)?;
            field_unshredders.insert(field.name().as_ref(), field_unshredder);
        }

        Ok(Self {
            value,
            typed_value,
            field_unshredders,
        })
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        let value = handle_unshredded_case!(self, builder, metadata, index, true);

        // If we get here, typed_value is valid and value may or may not be valid
        let mut object_builder = builder.try_new_object()?;

        // Process typed fields (skip empty builders that indicate missing fields)
        for (field_name, field_unshredder_opt) in &mut self.field_unshredders {
            if let Some(field_unshredder) = field_unshredder_opt {
                let mut field_builder = ObjectFieldBuilder::new(field_name, &mut object_builder);
                field_unshredder.append_row(&mut field_builder, metadata, index)?;
            }
        }

        // Process any unshredded fields (partial shredding)
        if let Some(value) = value {
            let Variant::Object(object) = value else {
                return Err(ArrowError::InvalidArgumentError(
                    "Expected object in value field for partially shredded struct".to_string(),
                ));
            };

            for (field_name, field_value) in object.iter() {
                if self.field_unshredders.contains_key(field_name) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Field '{field_name}' appears in both typed_value and value",
                    )));
                }
                object_builder.insert_bytes(field_name, field_value);
            }
        }

        object_builder.finish();
        Ok(())
    }
}

/// Builder for unshredding list/array types with recursive element processing
struct ListUnshredVariantBuilder<'a, L: ListLikeArray> {
    value: Option<&'a BinaryViewArray>,
    typed_value: &'a L,
    element_unshredder: Box<UnshredVariantRowBuilder<'a>>,
}

impl<'a, L: ListLikeArray> ListUnshredVariantBuilder<'a, L> {
    fn try_new(value: Option<&'a BinaryViewArray>, typed_value: &'a L) -> Result<Self> {
        // Create a recursive unshredder for the list elements
        // The element type comes from the values array of the list
        let element_values = typed_value.values();

        // For shredded lists, each element would be a ShreddedVariantFieldArray (struct)
        // Extract value/typed_value from the element struct
        let Some(element_values) = element_values.as_struct_opt() else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid shredded variant array element: expected Struct, got {}",
                element_values.data_type()
            )));
        };

        // Create recursive unshredder for elements
        //
        // NOTE: A None/None array element is technically invalid, but the shredding spec
        // requires us to emit `Variant::Null` when a required value is missing.
        let element_unshredder = UnshredVariantRowBuilder::try_new_opt(element_values.try_into()?)?
            .unwrap_or_else(|| UnshredVariantRowBuilder::null(None));

        Ok(Self {
            value,
            typed_value,
            element_unshredder: Box::new(element_unshredder),
        })
    }

    fn append_row(
        &mut self,
        builder: &mut impl VariantBuilderExt,
        metadata: &VariantMetadata,
        index: usize,
    ) -> Result<()> {
        handle_unshredded_case!(self, builder, metadata, index, false);

        // If we get here, typed_value is valid and value is NULL -- process the list elements
        let mut list_builder = builder.try_new_list()?;
        for element_index in self.typed_value.element_range(index) {
            self.element_unshredder
                .append_row(&mut list_builder, metadata, element_index)?;
        }

        list_builder.finish();
        Ok(())
    }
}

// TODO: This code is covered by tests in `parquet/tests/variant_integration.rs`. Does that suffice?
// Or do we also need targeted stand-alone unit tests for full coverage?
