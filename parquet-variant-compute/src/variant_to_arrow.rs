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

use crate::shred_variant::{
    VariantToShreddedVariantRowBuilder, make_variant_to_shredded_variant_arrow_row_builder,
};
use crate::type_conversion::{
    PrimitiveFromVariant, TimestampFromVariant, variant_to_unscaled_decimal,
};
use crate::variant_array::ShreddedVariantFieldArray;
use crate::{VariantArray, VariantValueArrayBuilder};
use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, BinaryBuilder, BinaryLikeArrayBuilder, BinaryViewArray,
    BinaryViewBuilder, BooleanBuilder, FixedSizeBinaryBuilder, GenericListArray,
    GenericListViewArray, LargeBinaryBuilder, LargeStringBuilder, NullArray, NullBufferBuilder,
    OffsetSizeTrait, PrimitiveBuilder, StringBuilder, StringLikeArrayBuilder, StringViewBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{CastOptions, DecimalCast};
use arrow::datatypes::{self, DataType, DecimalType};
use arrow::error::{ArrowError, Result};
use arrow_schema::{FieldRef, TimeUnit};
use parquet_variant::{Variant, VariantList, VariantPath};
use std::sync::Arc;

/// Builder for converting variant values into strongly typed Arrow arrays.
///
/// Useful for variant_get kernels that need to extract specific paths from variant values, possibly
/// with casting of leaf values to specific types.
pub(crate) enum VariantToArrowRowBuilder<'a> {
    Primitive(PrimitiveVariantToArrowRowBuilder<'a>),
    BinaryVariant(VariantToBinaryVariantArrowRowBuilder),

    // Path extraction wrapper - contains a boxed enum for any of the above
    WithPath(VariantPathRowBuilder<'a>),
}

impl<'a> VariantToArrowRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.append_null(),
            BinaryVariant(b) => b.append_null(),
            WithPath(path_builder) => path_builder.append_null(),
        }
    }

    pub fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.append_value(&value),
            BinaryVariant(b) => b.append_value(value),
            WithPath(path_builder) => path_builder.append_value(value),
        }
    }

    pub fn finish(self) -> Result<ArrayRef> {
        use VariantToArrowRowBuilder::*;
        match self {
            Primitive(b) => b.finish(),
            BinaryVariant(b) => b.finish(),
            WithPath(path_builder) => path_builder.finish(),
        }
    }
}

pub(crate) fn make_variant_to_arrow_row_builder<'a>(
    metadata: &BinaryViewArray,
    path: VariantPath<'a>,
    data_type: Option<&'a DataType>,
    cast_options: &'a CastOptions,
    capacity: usize,
) -> Result<VariantToArrowRowBuilder<'a>> {
    use VariantToArrowRowBuilder::*;

    let mut builder = match data_type {
        // If no data type was requested, build an unshredded VariantArray.
        None => BinaryVariant(VariantToBinaryVariantArrowRowBuilder::new(
            metadata.clone(),
            capacity,
        )),
        Some(DataType::Struct(_)) => {
            return Err(ArrowError::NotYetImplemented(
                "Converting unshredded variant objects to arrow structs".to_string(),
            ));
        }
        Some(
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..),
        ) => {
            return Err(ArrowError::NotYetImplemented(
                "Converting unshredded variant arrays to arrow lists".to_string(),
            ));
        }
        Some(data_type) => {
            let builder =
                make_primitive_variant_to_arrow_row_builder(data_type, cast_options, capacity)?;
            Primitive(builder)
        }
    };

    // Wrap with path extraction if needed
    if !path.is_empty() {
        builder = WithPath(VariantPathRowBuilder {
            builder: Box::new(builder),
            path,
        })
    };

    Ok(builder)
}

/// Builder for converting primitive variant values to Arrow arrays. It is used by both
/// `VariantToArrowRowBuilder` (below) and `VariantToShreddedPrimitiveVariantRowBuilder` (in
/// `shred_variant.rs`).
pub(crate) enum PrimitiveVariantToArrowRowBuilder<'a> {
    Null(VariantToNullArrowRowBuilder<'a>),
    Boolean(VariantToBooleanArrowRowBuilder<'a>),
    Int8(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int8Type>),
    Int16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int16Type>),
    Int32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int32Type>),
    Int64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Int64Type>),
    UInt8(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt8Type>),
    UInt16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt16Type>),
    UInt32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt32Type>),
    UInt64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::UInt64Type>),
    Float16(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float16Type>),
    Float32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float32Type>),
    Float64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Float64Type>),
    Decimal32(VariantToDecimalArrowRowBuilder<'a, datatypes::Decimal32Type>),
    Decimal64(VariantToDecimalArrowRowBuilder<'a, datatypes::Decimal64Type>),
    Decimal128(VariantToDecimalArrowRowBuilder<'a, datatypes::Decimal128Type>),
    Decimal256(VariantToDecimalArrowRowBuilder<'a, datatypes::Decimal256Type>),
    TimestampSecond(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampSecondType>),
    TimestampSecondNtz(VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampSecondType>),
    TimestampMilli(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampMillisecondType>),
    TimestampMilliNtz(
        VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampMillisecondType>,
    ),
    TimestampMicro(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampMicrosecondType>),
    TimestampMicroNtz(
        VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampMicrosecondType>,
    ),
    TimestampNano(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampNanosecondType>),
    TimestampNanoNtz(VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampNanosecondType>),
    Time32Second(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Time32SecondType>),
    Time32Milli(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Time32MillisecondType>),
    Time64Micro(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Time64MicrosecondType>),
    Time64Nano(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Time64NanosecondType>),
    Date32(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Date32Type>),
    Date64(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Date64Type>),
    Uuid(VariantToUuidArrowRowBuilder<'a>),
    String(VariantToStringArrowBuilder<'a, StringBuilder>),
    LargeString(VariantToStringArrowBuilder<'a, LargeStringBuilder>),
    StringView(VariantToStringArrowBuilder<'a, StringViewBuilder>),
    Binary(VariantToBinaryArrowRowBuilder<'a, BinaryBuilder>),
    LargeBinary(VariantToBinaryArrowRowBuilder<'a, LargeBinaryBuilder>),
    BinaryView(VariantToBinaryArrowRowBuilder<'a, BinaryViewBuilder>),
}

impl<'a> PrimitiveVariantToArrowRowBuilder<'a> {
    pub fn append_null(&mut self) -> Result<()> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Null(b) => b.append_null(),
            Boolean(b) => b.append_null(),
            Int8(b) => b.append_null(),
            Int16(b) => b.append_null(),
            Int32(b) => b.append_null(),
            Int64(b) => b.append_null(),
            UInt8(b) => b.append_null(),
            UInt16(b) => b.append_null(),
            UInt32(b) => b.append_null(),
            UInt64(b) => b.append_null(),
            Float16(b) => b.append_null(),
            Float32(b) => b.append_null(),
            Float64(b) => b.append_null(),
            Decimal32(b) => b.append_null(),
            Decimal64(b) => b.append_null(),
            Decimal128(b) => b.append_null(),
            Decimal256(b) => b.append_null(),
            TimestampSecond(b) => b.append_null(),
            TimestampSecondNtz(b) => b.append_null(),
            TimestampMilli(b) => b.append_null(),
            TimestampMilliNtz(b) => b.append_null(),
            TimestampMicro(b) => b.append_null(),
            TimestampMicroNtz(b) => b.append_null(),
            TimestampNano(b) => b.append_null(),
            TimestampNanoNtz(b) => b.append_null(),
            Time32Second(b) => b.append_null(),
            Time32Milli(b) => b.append_null(),
            Time64Micro(b) => b.append_null(),
            Time64Nano(b) => b.append_null(),
            Date32(b) => b.append_null(),
            Date64(b) => b.append_null(),
            Uuid(b) => b.append_null(),
            String(b) => b.append_null(),
            LargeString(b) => b.append_null(),
            StringView(b) => b.append_null(),
            Binary(b) => b.append_null(),
            LargeBinary(b) => b.append_null(),
            BinaryView(b) => b.append_null(),
        }
    }

    pub fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Null(b) => b.append_value(value),
            Boolean(b) => b.append_value(value),
            Int8(b) => b.append_value(value),
            Int16(b) => b.append_value(value),
            Int32(b) => b.append_value(value),
            Int64(b) => b.append_value(value),
            UInt8(b) => b.append_value(value),
            UInt16(b) => b.append_value(value),
            UInt32(b) => b.append_value(value),
            UInt64(b) => b.append_value(value),
            Float16(b) => b.append_value(value),
            Float32(b) => b.append_value(value),
            Float64(b) => b.append_value(value),
            Decimal32(b) => b.append_value(value),
            Decimal64(b) => b.append_value(value),
            Decimal128(b) => b.append_value(value),
            Decimal256(b) => b.append_value(value),
            TimestampSecond(b) => b.append_value(value),
            TimestampSecondNtz(b) => b.append_value(value),
            TimestampMilli(b) => b.append_value(value),
            TimestampMilliNtz(b) => b.append_value(value),
            TimestampMicro(b) => b.append_value(value),
            TimestampMicroNtz(b) => b.append_value(value),
            TimestampNano(b) => b.append_value(value),
            TimestampNanoNtz(b) => b.append_value(value),
            Time32Second(b) => b.append_value(value),
            Time32Milli(b) => b.append_value(value),
            Time64Micro(b) => b.append_value(value),
            Time64Nano(b) => b.append_value(value),
            Date32(b) => b.append_value(value),
            Date64(b) => b.append_value(value),
            Uuid(b) => b.append_value(value),
            String(b) => b.append_value(value),
            LargeString(b) => b.append_value(value),
            StringView(b) => b.append_value(value),
            Binary(b) => b.append_value(value),
            LargeBinary(b) => b.append_value(value),
            BinaryView(b) => b.append_value(value),
        }
    }

    pub fn finish(self) -> Result<ArrayRef> {
        use PrimitiveVariantToArrowRowBuilder::*;
        match self {
            Null(b) => b.finish(),
            Boolean(b) => b.finish(),
            Int8(b) => b.finish(),
            Int16(b) => b.finish(),
            Int32(b) => b.finish(),
            Int64(b) => b.finish(),
            UInt8(b) => b.finish(),
            UInt16(b) => b.finish(),
            UInt32(b) => b.finish(),
            UInt64(b) => b.finish(),
            Float16(b) => b.finish(),
            Float32(b) => b.finish(),
            Float64(b) => b.finish(),
            Decimal32(b) => b.finish(),
            Decimal64(b) => b.finish(),
            Decimal128(b) => b.finish(),
            Decimal256(b) => b.finish(),
            TimestampSecond(b) => b.finish(),
            TimestampSecondNtz(b) => b.finish(),
            TimestampMilli(b) => b.finish(),
            TimestampMilliNtz(b) => b.finish(),
            TimestampMicro(b) => b.finish(),
            TimestampMicroNtz(b) => b.finish(),
            TimestampNano(b) => b.finish(),
            TimestampNanoNtz(b) => b.finish(),
            Time32Second(b) => b.finish(),
            Time32Milli(b) => b.finish(),
            Time64Micro(b) => b.finish(),
            Time64Nano(b) => b.finish(),
            Date32(b) => b.finish(),
            Date64(b) => b.finish(),
            Uuid(b) => b.finish(),
            String(b) => b.finish(),
            LargeString(b) => b.finish(),
            StringView(b) => b.finish(),
            Binary(b) => b.finish(),
            LargeBinary(b) => b.finish(),
            BinaryView(b) => b.finish(),
        }
    }
}

/// Creates a row builder that converts primitive `Variant` values into the requested Arrow data type.
pub(crate) fn make_primitive_variant_to_arrow_row_builder<'a>(
    data_type: &'a DataType,
    cast_options: &'a CastOptions,
    capacity: usize,
) -> Result<PrimitiveVariantToArrowRowBuilder<'a>> {
    use PrimitiveVariantToArrowRowBuilder::*;

    let builder =
        match data_type {
            DataType::Null => Null(VariantToNullArrowRowBuilder::new(cast_options, capacity)),
            DataType::Boolean => {
                Boolean(VariantToBooleanArrowRowBuilder::new(cast_options, capacity))
            }
            DataType::Int8 => Int8(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Int16 => Int16(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Int32 => Int32(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Int64 => Int64(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::UInt8 => UInt8(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::UInt16 => UInt16(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::UInt32 => UInt32(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::UInt64 => UInt64(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Float16 => Float16(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Float32 => Float32(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Float64 => Float64(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Decimal32(precision, scale) => Decimal32(
                VariantToDecimalArrowRowBuilder::new(cast_options, capacity, *precision, *scale)?,
            ),
            DataType::Decimal64(precision, scale) => Decimal64(
                VariantToDecimalArrowRowBuilder::new(cast_options, capacity, *precision, *scale)?,
            ),
            DataType::Decimal128(precision, scale) => Decimal128(
                VariantToDecimalArrowRowBuilder::new(cast_options, capacity, *precision, *scale)?,
            ),
            DataType::Decimal256(precision, scale) => Decimal256(
                VariantToDecimalArrowRowBuilder::new(cast_options, capacity, *precision, *scale)?,
            ),
            DataType::Date32 => Date32(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Date64 => Date64(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Time32(TimeUnit::Second) => Time32Second(
                VariantToPrimitiveArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Time32(TimeUnit::Millisecond) => Time32Milli(
                VariantToPrimitiveArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Time32(t) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "The unit for Time32 must be second/millisecond, received {t:?}"
                )));
            }
            DataType::Time64(TimeUnit::Microsecond) => Time64Micro(
                VariantToPrimitiveArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Time64Nano(
                VariantToPrimitiveArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Time64(t) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "The unit for Time64 must be micro/nano seconds, received {t:?}"
                )));
            }
            DataType::Timestamp(TimeUnit::Second, None) => TimestampSecondNtz(
                VariantToTimestampNtzArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Timestamp(TimeUnit::Second, tz) => TimestampSecond(
                VariantToTimestampArrowRowBuilder::new(cast_options, capacity, tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None) => TimestampMilliNtz(
                VariantToTimestampNtzArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => TimestampMilli(
                VariantToTimestampArrowRowBuilder::new(cast_options, capacity, tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => TimestampMicroNtz(
                VariantToTimestampNtzArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => TimestampMicro(
                VariantToTimestampArrowRowBuilder::new(cast_options, capacity, tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => TimestampNanoNtz(
                VariantToTimestampNtzArrowRowBuilder::new(cast_options, capacity),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => TimestampNano(
                VariantToTimestampArrowRowBuilder::new(cast_options, capacity, tz.clone()),
            ),
            DataType::Duration(_) | DataType::Interval(_) => {
                return Err(ArrowError::InvalidArgumentError(
                    "Casting Variant to duration/interval types is not supported. \
                    The Variant format does not define duration/interval types."
                        .to_string(),
                ));
            }
            DataType::Binary => Binary(VariantToBinaryArrowRowBuilder::new(cast_options, capacity)),
            DataType::LargeBinary => {
                LargeBinary(VariantToBinaryArrowRowBuilder::new(cast_options, capacity))
            }
            DataType::BinaryView => {
                BinaryView(VariantToBinaryArrowRowBuilder::new(cast_options, capacity))
            }
            DataType::FixedSizeBinary(16) => {
                Uuid(VariantToUuidArrowRowBuilder::new(cast_options, capacity))
            }
            DataType::FixedSizeBinary(_) => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "DataType {data_type:?} not yet implemented"
                )));
            }
            DataType::Utf8 => String(VariantToStringArrowBuilder::new(cast_options, capacity)),
            DataType::LargeUtf8 => {
                LargeString(VariantToStringArrowBuilder::new(cast_options, capacity))
            }
            DataType::Utf8View => {
                StringView(VariantToStringArrowBuilder::new(cast_options, capacity))
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
            | DataType::Union(..)
            | DataType::Dictionary(..)
            | DataType::RunEndEncoded(..) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Casting to {data_type:?} is not applicable for primitive Variant types"
                )));
            }
        };
    Ok(builder)
}

pub(crate) enum ArrayVariantToArrowRowBuilder<'a> {
    List(VariantToListArrowRowBuilder<'a, i32, false>),
    LargeList(VariantToListArrowRowBuilder<'a, i64, false>),
    ListView(VariantToListArrowRowBuilder<'a, i32, true>),
    LargeListView(VariantToListArrowRowBuilder<'a, i64, true>),
}

impl<'a> ArrayVariantToArrowRowBuilder<'a> {
    pub(crate) fn try_new(
        data_type: &'a DataType,
        cast_options: &'a CastOptions,
        capacity: usize,
    ) -> Result<Self> {
        use ArrayVariantToArrowRowBuilder::*;

        // Make List/ListView builders without repeating the constructor boilerplate.
        macro_rules! make_list_builder {
            ($variant:ident, $offset:ty, $is_view:expr, $field:ident) => {
                $variant(VariantToListArrowRowBuilder::<$offset, $is_view>::try_new(
                    $field.clone(),
                    $field.data_type(),
                    cast_options,
                    capacity,
                )?)
            };
        }

        let builder = match data_type {
            DataType::List(field) => make_list_builder!(List, i32, false, field),
            DataType::LargeList(field) => make_list_builder!(LargeList, i64, false, field),
            DataType::ListView(field) => make_list_builder!(ListView, i32, true, field),
            DataType::LargeListView(field) => make_list_builder!(LargeListView, i64, true, field),
            DataType::FixedSizeList(..) => {
                return Err(ArrowError::NotYetImplemented(
                    "Converting unshredded variant arrays to arrow fixed-size lists".to_string(),
                ));
            }
            other => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Casting to {other:?} is not applicable for array Variant types"
                )));
            }
        };
        Ok(builder)
    }

    pub(crate) fn append_null(&mut self) {
        match self {
            Self::List(builder) => builder.append_null(),
            Self::LargeList(builder) => builder.append_null(),
            Self::ListView(builder) => builder.append_null(),
            Self::LargeListView(builder) => builder.append_null(),
        }
    }

    pub(crate) fn append_value(&mut self, list: VariantList<'_, '_>) -> Result<()> {
        match self {
            Self::List(builder) => builder.append_value(list),
            Self::LargeList(builder) => builder.append_value(list),
            Self::ListView(builder) => builder.append_value(list),
            Self::LargeListView(builder) => builder.append_value(list),
        }
    }

    pub(crate) fn finish(self) -> Result<ArrayRef> {
        match self {
            Self::List(builder) => builder.finish(),
            Self::LargeList(builder) => builder.finish(),
            Self::ListView(builder) => builder.finish(),
            Self::LargeListView(builder) => builder.finish(),
        }
    }
}

/// A thin wrapper whose only job is to extract a specific path from a variant value and pass the
/// result to a nested builder.
pub(crate) struct VariantPathRowBuilder<'a> {
    builder: Box<VariantToArrowRowBuilder<'a>>,
    path: VariantPath<'a>,
}

impl<'a> VariantPathRowBuilder<'a> {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null()
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        if let Some(v) = value.get_path(&self.path) {
            self.builder.append_value(v)
        } else {
            self.builder.append_null()?;
            Ok(false)
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        self.builder.finish()
    }
}

macro_rules! define_variant_to_primitive_builder {
    (struct $name:ident<$lifetime:lifetime $(, $generic:ident: $bound:path )?>
    |$array_param:ident $(, $field:ident: $field_type:ty)?| -> $builder_name:ident $(< $array_type:ty >)? { $init_expr: expr },
    |$value: ident| $value_transform:expr,
    type_name: $type_name:expr) => {
        pub(crate) struct $name<$lifetime $(, $generic : $bound )?>
        {
            builder: $builder_name $(<$array_type>)?,
            cast_options: &$lifetime CastOptions<$lifetime>,
        }

        impl<$lifetime $(, $generic: $bound+ )?> $name<$lifetime $(, $generic )?> {
            fn new(
                cast_options: &$lifetime CastOptions<$lifetime>,
                $array_param: usize,
                // add this so that $init_expr can use it
                $( $field: $field_type, )?
            ) -> Self {
                Self {
                    builder: $init_expr,
                    cast_options,
                }
            }

            fn append_null(&mut self) -> Result<()> {
                self.builder.append_null();
                Ok(())
            }

            fn append_value(&mut self, $value: &Variant<'_, '_>) -> Result<bool> {
                if let Some(v) = $value_transform {
                    self.builder.append_value(v);
                    Ok(true)
                } else {
                    if !self.cast_options.safe {
                        // Unsafe casting: return error on conversion failure
                        return Err(ArrowError::CastError(format!(
                            "Failed to extract primitive of type {} from variant {:?} at path VariantPath([])",
                            $type_name,
                            $value
                        )));
                    }
                    // Safe casting: append null on conversion failure
                    self.builder.append_null();
                    Ok(false)
                }
            }

            // Add this to silence unused mut warning from macro-generated code
            // This is mainly for `FakeNullBuilder`
            #[allow(unused_mut)]
            fn finish(mut self) -> Result<ArrayRef> {
                Ok(Arc::new(self.builder.finish()))
            }
        }
    }
}

define_variant_to_primitive_builder!(
    struct VariantToStringArrowBuilder<'a, B: StringLikeArrayBuilder>
    |capacity| -> B { B::with_capacity(capacity) },
    |value| value.as_string(),
    type_name: B::type_name()
);

define_variant_to_primitive_builder!(
    struct VariantToBooleanArrowRowBuilder<'a>
    |capacity| -> BooleanBuilder { BooleanBuilder::with_capacity(capacity) },
    |value|  value.as_boolean(),
    type_name: datatypes::BooleanType::DATA_TYPE
);

define_variant_to_primitive_builder!(
    struct VariantToPrimitiveArrowRowBuilder<'a, T:PrimitiveFromVariant>
    |capacity| -> PrimitiveBuilder<T> { PrimitiveBuilder::<T>::with_capacity(capacity) },
    |value| T::from_variant(value),
    type_name: T::DATA_TYPE
);

define_variant_to_primitive_builder!(
    struct VariantToTimestampNtzArrowRowBuilder<'a, T:TimestampFromVariant<true>>
    |capacity| -> PrimitiveBuilder<T> { PrimitiveBuilder::<T>::with_capacity(capacity) },
    |value| T::from_variant(value),
    type_name: T::DATA_TYPE
);

define_variant_to_primitive_builder!(
    struct VariantToTimestampArrowRowBuilder<'a, T:TimestampFromVariant<false>>
    |capacity, tz: Option<Arc<str>> | -> PrimitiveBuilder<T> {
        PrimitiveBuilder::<T>::with_capacity(capacity).with_timezone_opt(tz)
    },
    |value| T::from_variant(value),
    type_name: T::DATA_TYPE
);

define_variant_to_primitive_builder!(
    struct VariantToBinaryArrowRowBuilder<'a, B: BinaryLikeArrayBuilder>
    |capacity| -> B { B::with_capacity(capacity) },
    |value| value.as_u8_slice(),
    type_name: B::type_name()
);

/// Builder for converting variant values to arrow Decimal values
pub(crate) struct VariantToDecimalArrowRowBuilder<'a, T>
where
    T: DecimalType,
    T::Native: DecimalCast,
{
    builder: PrimitiveBuilder<T>,
    cast_options: &'a CastOptions<'a>,
    precision: u8,
    scale: i8,
}

impl<'a, T> VariantToDecimalArrowRowBuilder<'a, T>
where
    T: DecimalType,
    T::Native: DecimalCast,
{
    fn new(
        cast_options: &'a CastOptions<'a>,
        capacity: usize,
        precision: u8,
        scale: i8,
    ) -> Result<Self> {
        let builder = PrimitiveBuilder::<T>::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)?;
        Ok(Self {
            builder,
            cast_options,
            precision,
            scale,
        })
    }

    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        if let Some(scaled) = variant_to_unscaled_decimal::<T>(value, self.precision, self.scale) {
            self.builder.append_value(scaled);
            Ok(true)
        } else if self.cast_options.safe {
            self.builder.append_null();
            Ok(false)
        } else {
            Err(ArrowError::CastError(format!(
                "Failed to cast to {}(precision={}, scale={}) from variant {:?}",
                T::PREFIX,
                self.precision,
                self.scale,
                value
            )))
        }
    }

    fn finish(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// Builder for converting variant values to FixedSizeBinary(16) for UUIDs
pub(crate) struct VariantToUuidArrowRowBuilder<'a> {
    builder: FixedSizeBinaryBuilder,
    cast_options: &'a CastOptions<'a>,
}

impl<'a> VariantToUuidArrowRowBuilder<'a> {
    fn new(cast_options: &'a CastOptions<'a>, capacity: usize) -> Self {
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            cast_options,
        }
    }

    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: &Variant<'_, '_>) -> Result<bool> {
        match value.as_uuid() {
            Some(uuid) => {
                self.builder
                    .append_value(uuid.as_bytes())
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                Ok(true)
            }
            None if self.cast_options.safe => {
                self.builder.append_null();
                Ok(false)
            }
            None => Err(ArrowError::CastError(format!(
                "Failed to extract UUID from variant {value:?}",
            ))),
        }
    }

    fn finish(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

pub(crate) struct VariantToListArrowRowBuilder<'a, O, const IS_VIEW: bool>
where
    O: OffsetSizeTrait + ArrowNativeTypeOp,
{
    field: FieldRef,
    offsets: Vec<O>,
    element_builder: Box<VariantToShreddedVariantRowBuilder<'a>>,
    nulls: NullBufferBuilder,
    current_offset: O,
}

impl<'a, O, const IS_VIEW: bool> VariantToListArrowRowBuilder<'a, O, IS_VIEW>
where
    O: OffsetSizeTrait + ArrowNativeTypeOp,
{
    fn try_new(
        field: FieldRef,
        element_data_type: &'a DataType,
        cast_options: &'a CastOptions,
        capacity: usize,
    ) -> Result<Self> {
        if capacity >= isize::MAX as usize {
            return Err(ArrowError::ComputeError(
                "Capacity exceeds isize::MAX when reserving list offsets".to_string(),
            ));
        }
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::ZERO);
        let element_builder = make_variant_to_shredded_variant_arrow_row_builder(
            element_data_type,
            cast_options,
            capacity,
            false,
        )?;
        Ok(Self {
            field,
            offsets,
            element_builder: Box::new(element_builder),
            nulls: NullBufferBuilder::new(capacity),
            current_offset: O::ZERO,
        })
    }

    fn append_null(&mut self) {
        self.offsets.push(self.current_offset);
        self.nulls.append_null();
    }

    fn append_value(&mut self, list: VariantList<'_, '_>) -> Result<()> {
        for element in list.iter() {
            self.element_builder.append_value(element)?;
            self.current_offset = self.current_offset.add_checked(O::ONE)?;
        }
        self.offsets.push(self.current_offset);
        self.nulls.append_non_null();
        Ok(())
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let (value, typed_value, nulls) = self.element_builder.finish()?;
        let element_array =
            ShreddedVariantFieldArray::from_parts(Some(value), Some(typed_value), nulls);
        let field = Arc::new(
            self.field
                .as_ref()
                .clone()
                .with_data_type(element_array.data_type().clone()),
        );

        if IS_VIEW {
            // NOTE: `offsets` is never empty (constructor pushes an entry)
            let mut sizes = Vec::with_capacity(self.offsets.len() - 1);
            for i in 1..self.offsets.len() {
                sizes.push(self.offsets[i] - self.offsets[i - 1]);
            }
            self.offsets.pop();
            let list_view_array = GenericListViewArray::<O>::new(
                field,
                ScalarBuffer::from(self.offsets),
                ScalarBuffer::from(sizes),
                ArrayRef::from(element_array),
                self.nulls.finish(),
            );
            Ok(Arc::new(list_view_array))
        } else {
            let list_array = GenericListArray::<O>::new(
                field,
                OffsetBuffer::<O>::new(ScalarBuffer::from(self.offsets)),
                ArrayRef::from(element_array),
                self.nulls.finish(),
            );
            Ok(Arc::new(list_array))
        }
    }
}

/// Builder for creating VariantArray output (for path extraction without type conversion)
pub(crate) struct VariantToBinaryVariantArrowRowBuilder {
    metadata: BinaryViewArray,
    builder: VariantValueArrayBuilder,
    nulls: NullBufferBuilder,
}

impl VariantToBinaryVariantArrowRowBuilder {
    fn new(metadata: BinaryViewArray, capacity: usize) -> Self {
        Self {
            metadata,
            builder: VariantValueArrayBuilder::new(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }
}

impl VariantToBinaryVariantArrowRowBuilder {
    fn append_null(&mut self) -> Result<()> {
        self.builder.append_null();
        self.nulls.append_null();
        Ok(())
    }

    fn append_value(&mut self, value: Variant<'_, '_>) -> Result<bool> {
        self.builder.append_value(value);
        self.nulls.append_non_null();
        Ok(true)
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let variant_array = VariantArray::from_parts(
            self.metadata,
            Some(self.builder.build()?),
            None, // no typed_value column
            self.nulls.finish(),
        );

        Ok(ArrayRef::from(variant_array))
    }
}

#[derive(Default)]
struct FakeNullBuilder {
    item_count: usize,
}

impl FakeNullBuilder {
    fn append_value(&mut self, _: ()) {
        self.item_count += 1;
    }

    fn append_null(&mut self) {
        self.item_count += 1;
    }

    fn finish(self) -> NullArray {
        NullArray::new(self.item_count)
    }
}

define_variant_to_primitive_builder!(
    struct VariantToNullArrowRowBuilder<'a>
    |_capacity| -> FakeNullBuilder { FakeNullBuilder::default() },
    |value| value.as_null(),
    type_name: "Null"
);

#[cfg(test)]
mod tests {
    use super::make_primitive_variant_to_arrow_row_builder;
    use arrow::compute::CastOptions;
    use arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
    use arrow::error::ArrowError;
    use std::sync::Arc;

    #[test]
    fn make_primitive_builder_rejects_non_primitive_types() {
        let cast_options = CastOptions::default();
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let struct_fields = Fields::from(vec![Field::new("child", DataType::Int32, true)]);
        let map_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ])),
            true,
        ));
        let union_fields =
            UnionFields::try_new(vec![1], vec![Field::new("child", DataType::Int32, true)])
                .unwrap();
        let run_ends_field = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let ree_values_field = Arc::new(Field::new("values", DataType::Utf8, true));

        let non_primitive_types = vec![
            DataType::List(item_field.clone()),
            DataType::LargeList(item_field.clone()),
            DataType::ListView(item_field.clone()),
            DataType::LargeListView(item_field.clone()),
            DataType::FixedSizeList(item_field.clone(), 2),
            DataType::Struct(struct_fields.clone()),
            DataType::Map(map_entries_field.clone(), false),
            DataType::Union(union_fields.clone(), UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::RunEndEncoded(run_ends_field.clone(), ree_values_field.clone()),
        ];

        for data_type in non_primitive_types {
            let err =
                match make_primitive_variant_to_arrow_row_builder(&data_type, &cast_options, 1) {
                    Ok(_) => panic!("non-primitive type {data_type:?} should be rejected"),
                    Err(err) => err,
                };

            match err {
                ArrowError::InvalidArgumentError(msg) => {
                    assert!(msg.contains(&format!("{data_type:?}")));
                }
                other => panic!("expected InvalidArgumentError, got {other:?}"),
            }
        }
    }
}
