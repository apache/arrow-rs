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

use arrow::array::{
    ArrayRef, BinaryViewArray, BooleanBuilder, NullArray, NullBufferBuilder, PrimitiveBuilder,
};
use arrow::compute::{CastOptions, DecimalCast};
use arrow::datatypes::{self, DataType, DecimalType};
use arrow::error::{ArrowError, Result};
use parquet_variant::{Variant, VariantPath};

use crate::type_conversion::{
    PrimitiveFromVariant, TimestampFromVariant, variant_to_unscaled_decimal,
};
use crate::{VariantArray, VariantValueArrayBuilder};

use arrow_schema::TimeUnit;
use std::sync::Arc;

/// Builder for converting variant values to primitive Arrow arrays. It is used by both
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
    TimestampMicro(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampMicrosecondType>),
    TimestampMicroNtz(
        VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampMicrosecondType>,
    ),
    TimestampNano(VariantToTimestampArrowRowBuilder<'a, datatypes::TimestampNanosecondType>),
    TimestampNanoNtz(VariantToTimestampNtzArrowRowBuilder<'a, datatypes::TimestampNanosecondType>),
    Time(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Time64MicrosecondType>),
    Date(VariantToPrimitiveArrowRowBuilder<'a, datatypes::Date32Type>),
}

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
            TimestampMicro(b) => b.append_null(),
            TimestampMicroNtz(b) => b.append_null(),
            TimestampNano(b) => b.append_null(),
            TimestampNanoNtz(b) => b.append_null(),
            Time(b) => b.append_null(),
            Date(b) => b.append_null(),
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
            TimestampMicro(b) => b.append_value(value),
            TimestampMicroNtz(b) => b.append_value(value),
            TimestampNano(b) => b.append_value(value),
            TimestampNanoNtz(b) => b.append_value(value),
            Time(b) => b.append_value(value),
            Date(b) => b.append_value(value),
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
            TimestampMicro(b) => b.finish(),
            TimestampMicroNtz(b) => b.finish(),
            TimestampNano(b) => b.finish(),
            TimestampNanoNtz(b) => b.finish(),
            Time(b) => b.finish(),
            Date(b) => b.finish(),
        }
    }
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

/// Creates a primitive row builder, returning Err if the requested data type is not primitive.
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
            DataType::Date32 => Date(VariantToPrimitiveArrowRowBuilder::new(
                cast_options,
                capacity,
            )),
            DataType::Time64(TimeUnit::Microsecond) => Time(
                VariantToPrimitiveArrowRowBuilder::new(cast_options, capacity),
            ),
            _ if data_type.is_primitive() => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Primitive data_type {data_type:?} not yet implemented"
                )));
            }
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Not a primitive type: {data_type:?}"
                )));
            }
        };
    Ok(builder)
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

struct FakeNullBuilder(NullArray);

impl FakeNullBuilder {
    fn new(capacity: usize) -> Self {
        Self(NullArray::new(capacity))
    }
    fn append_value<T>(&mut self, _: T) {}
    fn append_null(&mut self) {}

    fn finish(self) -> NullArray {
        self.0
    }
}

define_variant_to_primitive_builder!(
    struct VariantToNullArrowRowBuilder<'a>
    |capacity| -> FakeNullBuilder { FakeNullBuilder::new(capacity) },
    |_value|  Some(Variant::Null),
    type_name: "Null"
);
