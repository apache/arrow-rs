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

//! Module for transforming a typed arrow `Array` to `VariantArray`.

use arrow::compute::{DecimalCast, rescale_decimal};
use arrow::datatypes::{
    self, ArrowPrimitiveType, ArrowTimestampType, Decimal32Type, Decimal64Type, Decimal128Type,
    DecimalType,
};
use chrono::Timelike;
use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};

/// Extension trait for Arrow primitive types that can extract their native value from a Variant
pub(crate) trait PrimitiveFromVariant: ArrowPrimitiveType {
    fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native>;
}

/// Extension trait for Arrow timestamp types that can extract their native value from a Variant
/// We can't use [`PrimitiveFromVariant`] directly because we need _two_ implementations for each
/// timestamp type -- the `NTZ` param here.
pub(crate) trait TimestampFromVariant<const NTZ: bool>: ArrowTimestampType {
    fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native>;
}

/// Macro to generate PrimitiveFromVariant implementations for Arrow primitive types
macro_rules! impl_primitive_from_variant {
    ($arrow_type:ty, $variant_method:ident $(, $cast_fn:expr)?) => {
        impl PrimitiveFromVariant for $arrow_type {
            fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native> {
                let value = variant.$variant_method();
                $( let value = value.and_then($cast_fn); )?
                value
            }
        }
    };
}

macro_rules! impl_timestamp_from_variant {
    ($timestamp_type:ty, $variant_method:ident, ntz=$ntz:ident, $cast_fn:expr $(,)?) => {
        impl TimestampFromVariant<{ $ntz }> for $timestamp_type {
            fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native> {
                variant.$variant_method().and_then($cast_fn)
            }
        }
    };
}

impl_primitive_from_variant!(datatypes::Int32Type, as_int32);
impl_primitive_from_variant!(datatypes::Int16Type, as_int16);
impl_primitive_from_variant!(datatypes::Int8Type, as_int8);
impl_primitive_from_variant!(datatypes::Int64Type, as_int64);
impl_primitive_from_variant!(datatypes::UInt8Type, as_u8);
impl_primitive_from_variant!(datatypes::UInt16Type, as_u16);
impl_primitive_from_variant!(datatypes::UInt32Type, as_u32);
impl_primitive_from_variant!(datatypes::UInt64Type, as_u64);
impl_primitive_from_variant!(datatypes::Float16Type, as_f16);
impl_primitive_from_variant!(datatypes::Float32Type, as_f32);
impl_primitive_from_variant!(datatypes::Float64Type, as_f64);
impl_primitive_from_variant!(datatypes::Date32Type, as_naive_date, |v| {
    Some(datatypes::Date32Type::from_naive_date(v))
});
impl_primitive_from_variant!(datatypes::Date64Type, as_naive_date, |v| {
    Some(datatypes::Date64Type::from_naive_date(v))
});
impl_primitive_from_variant!(datatypes::Time32SecondType, as_time_utc, |v| {
    // Return None if there are leftover nanoseconds
    if v.nanosecond() != 0 {
        None
    } else {
        Some(v.num_seconds_from_midnight() as i32)
    }
});
impl_primitive_from_variant!(datatypes::Time32MillisecondType, as_time_utc, |v| {
    // Return None if there are leftover microseconds
    if v.nanosecond() % 1_000_000 != 0 {
        None
    } else {
        Some((v.num_seconds_from_midnight() * 1_000) as i32 + (v.nanosecond() / 1_000_000) as i32)
    }
});
impl_primitive_from_variant!(datatypes::Time64MicrosecondType, as_time_utc, |v| {
    Some((v.num_seconds_from_midnight() * 1_000_000 + v.nanosecond() / 1_000) as i64)
});
impl_primitive_from_variant!(datatypes::Time64NanosecondType, as_time_utc, |v| {
    // convert micro to nano seconds
    Some(v.num_seconds_from_midnight() as i64 * 1_000_000_000 + v.nanosecond() as i64)
});
impl_timestamp_from_variant!(
    datatypes::TimestampSecondType,
    as_timestamp_ntz_nanos,
    ntz = true,
    |timestamp| {
        // Return None if there are leftover nanoseconds
        if timestamp.nanosecond() != 0 {
            None
        } else {
            Self::make_value(timestamp)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampSecondType,
    as_timestamp_nanos,
    ntz = false,
    |timestamp| {
        // Return None if there are leftover nanoseconds
        if timestamp.nanosecond() != 0 {
            None
        } else {
            Self::make_value(timestamp.naive_utc())
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMillisecondType,
    as_timestamp_ntz_nanos,
    ntz = true,
    |timestamp| {
        // Return None if there are leftover microseconds
        if timestamp.nanosecond() % 1_000_000 != 0 {
            None
        } else {
            Self::make_value(timestamp)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMillisecondType,
    as_timestamp_nanos,
    ntz = false,
    |timestamp| {
        // Return None if there are leftover microseconds
        if timestamp.nanosecond() % 1_000_000 != 0 {
            None
        } else {
            Self::make_value(timestamp.naive_utc())
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMicrosecondType,
    as_timestamp_ntz_micros,
    ntz = true,
    Self::make_value,
);
impl_timestamp_from_variant!(
    datatypes::TimestampMicrosecondType,
    as_timestamp_micros,
    ntz = false,
    |timestamp| Self::make_value(timestamp.naive_utc())
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    as_timestamp_ntz_nanos,
    ntz = true,
    Self::make_value
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    as_timestamp_nanos,
    ntz = false,
    |timestamp| Self::make_value(timestamp.naive_utc())
);

/// Returns the unscaled integer representation for Arrow decimal type `O`
/// from a `Variant`.
///
/// - `precision` and `scale` specify the target Arrow decimal parameters
/// - Integer variants (`Int8/16/32/64`) are treated as decimals with scale 0
/// - Decimal variants (`Decimal4/8/16`) use their embedded precision and scale
///
/// The value is rescaled to (`precision`, `scale`) using `rescale_decimal` and
/// returns `None` if it cannot fit the requested precision.
pub(crate) fn variant_to_unscaled_decimal<O>(
    variant: &Variant<'_, '_>,
    precision: u8,
    scale: i8,
) -> Option<O::Native>
where
    O: DecimalType,
    O::Native: DecimalCast,
{
    match variant {
        Variant::Int8(i) => rescale_decimal::<Decimal32Type, O>(
            *i as i32,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int16(i) => rescale_decimal::<Decimal32Type, O>(
            *i as i32,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int32(i) => rescale_decimal::<Decimal32Type, O>(
            *i,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int64(i) => rescale_decimal::<Decimal64Type, O>(
            *i,
            VariantDecimal8::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Decimal4(d) => rescale_decimal::<Decimal32Type, O>(
            d.integer(),
            VariantDecimal4::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        Variant::Decimal8(d) => rescale_decimal::<Decimal64Type, O>(
            d.integer(),
            VariantDecimal8::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        Variant::Decimal16(d) => rescale_decimal::<Decimal128Type, O>(
            d.integer(),
            VariantDecimal16::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        _ => None,
    }
}

/// Convert the value at a specific index in the given array into a `Variant`.
macro_rules! non_generic_conversion_single_value {
    ($array:expr, $cast_fn:expr, $index:expr) => {{
        let array = $array;
        if array.is_null($index) {
            Ok(Variant::Null)
        } else {
            let cast_value = $cast_fn(array.value($index));
            Ok(Variant::from(cast_value))
        }
    }};
}
pub(crate) use non_generic_conversion_single_value;

/// Convert the value at a specific index in the given array into a `Variant`,
/// using `method` requiring a generic type to downcast the generic array
/// to a specific array type and `cast_fn` to transform the element.
macro_rules! generic_conversion_single_value {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        $crate::type_conversion::non_generic_conversion_single_value!(
            $input.$method::<$t>(),
            $cast_fn,
            $index
        )
    }};
}
pub(crate) use generic_conversion_single_value;

macro_rules! generic_conversion_single_value_with_result {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        let arr = $input.$method::<$t>();
        let v = arr.value($index);
        match ($cast_fn)(v) {
            Ok(var) => Ok(Variant::from(var)),
            Err(e) => Err(ArrowError::CastError(format!(
                "Cast failed at index {idx} (array type: {ty}): {e}",
                idx = $index,
                ty = <$t as ::arrow::datatypes::ArrowPrimitiveType>::DATA_TYPE
            ))),
        }
    }};
}

pub(crate) use generic_conversion_single_value_with_result;

/// Convert the value at a specific index in the given array into a `Variant`.
macro_rules! primitive_conversion_single_value {
    ($t:ty, $input:expr, $index:expr) => {{
        $crate::type_conversion::generic_conversion_single_value!(
            $t,
            as_primitive,
            |v| v,
            $input,
            $index
        )
    }};
}
pub(crate) use primitive_conversion_single_value;
