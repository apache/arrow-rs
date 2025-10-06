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

use arrow::datatypes::{self, ArrowPrimitiveType};
use parquet_variant::Variant;

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

/// Macro to generate PrimitiveFromVariant implementations for Arrow primitive types
macro_rules! impl_primitive_from_variant {
    ($arrow_type:ty, $variant_method:ident) => {
        impl PrimitiveFromVariant for $arrow_type {
            fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native> {
                variant.$variant_method()
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

macro_rules! scale_variant_decimal {
    ($variant:expr, $variant_method:ident, $to_int_ty:expr, $output_scale:expr, $precision:expr, $validate:path) => {{
        let variant = $variant.$variant_method()?;
        let input_scale = variant.scale() as i8;
        let variant = $to_int_ty(variant.integer());
        let ten = $to_int_ty(10);

        let scaled = if input_scale == $output_scale {
            Some(variant)
        } else if input_scale < $output_scale {
            // scale_up means output has more fractional digits than input
            // multiply integer by 10^(output_scale - input_scale)
            let delta = ($output_scale - input_scale) as u32;
            let mul = ten.checked_pow(delta)?;
            variant.checked_mul(mul)
        } else {
            // scale_down means output has fewer fractional digits than input
            // divide by 10^(input_scale - output_scale) with rounding
            let delta = (input_scale - $output_scale) as u32;
            let div = ten.checked_pow(delta)?;
            let d = variant.checked_div(div)?;
            let r = variant % div;

            // rounding in the same way as convert_to_smaller_scale_decimal in arrow-cast
            let half = div.checked_div($to_int_ty(2))?;
            let half_neg = half.checked_neg()?;
            let adjusted = match variant >= $to_int_ty(0) {
                true if r >= half => d.checked_add($to_int_ty(1))?,
                false if r <= half_neg => d.checked_sub($to_int_ty(1))?,
                _ => d,
            };
            Some(adjusted)
        };

        scaled.filter(|v| $validate(*v, $precision))
    }};
}
pub(crate) use scale_variant_decimal;

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
