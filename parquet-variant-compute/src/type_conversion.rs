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

use arrow::array::ArrowNativeTypeOp;
use arrow::compute::DecimalCast;
use arrow::datatypes::{
    self, ArrowPrimitiveType, ArrowTimestampType, Decimal32Type, Decimal64Type, Decimal128Type,
    DecimalType,
};
use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};

/// Options for controlling the behavior of `cast_to_variant_with_options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastOptions {
    /// If true, return error on conversion failure. If false, insert null for failed conversions.
    pub strict: bool,
}

impl Default for CastOptions {
    fn default() -> Self {
        Self { strict: true }
    }
}

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
                $( let value = value.map($cast_fn); )?
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
impl_primitive_from_variant!(
    datatypes::Date32Type,
    as_naive_date,
    datatypes::Date32Type::from_naive_date
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

/// Rescale a decimal from (input_precision, input_scale) to (output_precision, output_scale)
/// and return the scaled value if it fits the output precision. Similar to the implementation in
/// decimal.rs in arrow-cast.
pub(crate) fn rescale_decimal<I, O>(
    value: I::Native,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> Option<O::Native>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast,
    O::Native: DecimalCast,
{
    let delta_scale = output_scale - input_scale;

    // Determine if the cast is infallible based on precision/scale math
    let is_infallible_cast =
        is_infallible_decimal_cast(input_precision, input_scale, output_precision, output_scale);

    let scaled = if delta_scale == 0 {
        O::Native::from_decimal(value)
    } else if delta_scale > 0 {
        let mul = O::Native::from_decimal(10_i128)
            .and_then(|t| t.pow_checked(delta_scale as u32).ok())?;
        O::Native::from_decimal(value).and_then(|x| x.mul_checked(mul).ok())
    } else {
        // delta_scale is guaranteed to be > 0, but may also be larger than I::MAX_PRECISION. If so, the
        // scale change divides out more digits than the input has precision and the result of the cast
        // is always zero. For example, if we try to apply delta_scale=10 a decimal32 value, the largest
        // possible result is 999999999/10000000000 = 0.0999999999, which rounds to zero. Smaller values
        // (e.g. 1/10000000000) or larger delta_scale (e.g. 999999999/10000000000000) produce even
        // smaller results, which also round to zero. In that case, just return an array of zeros.
        let delta_scale = delta_scale.unsigned_abs() as usize;
        let Some(max) = I::MAX_FOR_EACH_PRECISION.get(delta_scale) else {
            return Some(O::Native::ZERO);
        };
        let div = max.add_wrapping(I::Native::ONE);
        let half = div.div_wrapping(I::Native::ONE.add_wrapping(I::Native::ONE));
        let half_neg = half.neg_wrapping();

        // div is >= 10 and so this cannot overflow
        let d = value.div_wrapping(div);
        let r = value.mod_wrapping(div);

        // Round result
        let adjusted = match value >= I::Native::ZERO {
            true if r >= half => d.add_wrapping(I::Native::ONE),
            false if r <= half_neg => d.sub_wrapping(I::Native::ONE),
            _ => d,
        };
        O::Native::from_decimal(adjusted)
    };

    scaled.filter(|v| is_infallible_cast || O::is_valid_decimal_precision(*v, output_precision))
}

/// Returns true if casting from (input_precision, input_scale) to
/// (output_precision, output_scale) is infallible based on precision/scale math.
fn is_infallible_decimal_cast(
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> bool {
    let delta_scale = output_scale - input_scale;
    let input_precision = input_precision as i8;
    let output_precision = output_precision as i8;
    if delta_scale >= 0 {
        // if the gain in precision (digits) is greater than the multiplication due to scaling
        // every number will fit into the output type
        // Example: If we are starting with any number of precision 5 [xxxxx],
        // then an increase of scale by 3 will have the following effect on the representation:
        // [xxxxx] -> [xxxxx000], so for the cast to be infallible, the output type
        // needs to provide at least 8 digits precision
        input_precision + delta_scale <= output_precision
    } else {
        // if the reduction of the input number through scaling (dividing) is greater
        // than a possible precision loss (plus potential increase via rounding)
        // every input number will fit into the output type
        // Example: If we are starting with any number of precision 5 [xxxxx],
        // then and decrease the scale by 3 will have the following effect on the representation:
        // [xxxxx] -> [xx] (+ 1 possibly, due to rounding).
        // The rounding may add an additional digit, so for the cast to be infallible,
        // the output type needs to have at least 3 digits of precision.
        // e.g. Decimal(5, 3) 99.999 to Decimal(3, 0) will result in 100:
        // [99999] -> [99] + 1 = [100], a cast to Decimal(2, 0) would not be possible
        input_precision + delta_scale < output_precision
    }
}

/// Convert the value at a specific index in the given array into a `Variant`.
macro_rules! non_generic_conversion_single_value {
    ($array:expr, $cast_fn:expr, $index:expr) => {{
        let array = $array;
        if array.is_null($index) {
            Variant::Null
        } else {
            let cast_value = $cast_fn(array.value($index));
            Variant::from(cast_value)
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

/// Convert a decimal value to a `VariantDecimal`
macro_rules! decimal_to_variant_decimal {
    ($v:ident, $scale:expr, $value_type:ty, $variant_type:ty) => {{
        let (v, scale) = if *$scale < 0 {
            // For negative scale, we need to multiply the value by 10^|scale|
            // For example: 123 with scale -2 becomes 12300 with scale 0
            let v =
                <$value_type>::checked_pow(10, (-*$scale) as u32).and_then(|m| m.checked_mul($v));
            (v, 0u8)
        } else {
            (Some($v), *$scale as u8)
        };

        // Return an Option to allow callers to decide whether to error (strict)
        // or append null (non-strict) on conversion failure
        v.and_then(|v| <$variant_type>::try_new(v, scale).ok())
    }};
}
pub(crate) use decimal_to_variant_decimal;
