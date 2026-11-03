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

use crate::cast::*;
use crate::parse::{DecimalParseError, parse_decimal_checked};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};

/// A utility trait that provides checked conversions between
/// decimal types inspired by [`NumCast`]
pub trait DecimalCast: Sized {
    /// Convert the decimal to an i32
    fn to_i32(self) -> Option<i32>;

    /// Convert the decimal to an i64
    fn to_i64(self) -> Option<i64>;

    /// Convert the decimal to an i128
    fn to_i128(self) -> Option<i128>;

    /// Convert the decimal to an i256
    fn to_i256(self) -> Option<i256>;

    /// Convert a decimal from a decimal
    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self>;

    /// Convert a decimal from a f64
    fn from_f64(n: f64) -> Option<Self>;
}

impl DecimalCast for i32 {
    fn to_i32(self) -> Option<i32> {
        Some(self)
    }

    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }

    fn to_i128(self) -> Option<i128> {
        Some(self as i128)
    }

    fn to_i256(self) -> Option<i256> {
        Some(i256::from_i128(self as i128))
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i32()
    }

    fn from_f64(n: f64) -> Option<Self> {
        n.to_i32()
    }
}

impl DecimalCast for i64 {
    fn to_i32(self) -> Option<i32> {
        i32::try_from(self).ok()
    }

    fn to_i64(self) -> Option<i64> {
        Some(self)
    }

    fn to_i128(self) -> Option<i128> {
        Some(self as i128)
    }

    fn to_i256(self) -> Option<i256> {
        Some(i256::from_i128(self as i128))
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i64()
    }

    fn from_f64(n: f64) -> Option<Self> {
        // Call implementation explicitly otherwise this resolves to `to_i64`
        // in arrow-buffer that behaves differently.
        num_traits::ToPrimitive::to_i64(&n)
    }
}

impl DecimalCast for i128 {
    fn to_i32(self) -> Option<i32> {
        i32::try_from(self).ok()
    }

    fn to_i64(self) -> Option<i64> {
        i64::try_from(self).ok()
    }

    fn to_i128(self) -> Option<i128> {
        Some(self)
    }

    fn to_i256(self) -> Option<i256> {
        Some(i256::from_i128(self))
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i128()
    }

    fn from_f64(n: f64) -> Option<Self> {
        n.to_i128()
    }
}

impl DecimalCast for i256 {
    fn to_i32(self) -> Option<i32> {
        self.to_i128().map(|x| i32::try_from(x).ok())?
    }

    fn to_i64(self) -> Option<i64> {
        self.to_i128().map(|x| i64::try_from(x).ok())?
    }

    fn to_i128(self) -> Option<i128> {
        self.to_i128()
    }

    fn to_i256(self) -> Option<i256> {
        Some(self)
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i256()
    }

    fn from_f64(n: f64) -> Option<Self> {
        i256::from_f64(n)
    }
}

/// Construct closures to upscale decimals from `(input_precision, input_scale)` to
/// `(output_precision, output_scale)`.
///
/// Returns `(f_fallible, f_infallible)` where:
/// * `f_fallible` yields `None` when the requested cast would overflow
/// * `f_infallible` is present only when every input is guaranteed to succeed; otherwise it is `None`
///   and callers must fall back to `f_fallible`
///
/// Returns `None` if the required scale increase `delta_scale = output_scale - input_scale`
/// exceeds the supported precomputed precision table `O::MAX_FOR_EACH_PRECISION`.
/// In that case, the caller should treat this as an overflow for the output scale
/// and handle it accordingly (e.g., return a cast error).
#[expect(clippy::type_complexity)]
fn make_upscaler<I: DecimalType, O: DecimalType>(
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> Option<(
    impl Fn(I::Native) -> Option<O::Native>,
    Option<impl Fn(I::Native) -> O::Native>,
)>
where
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let delta_scale = output_scale as i16 - input_scale as i16;

    // O::MAX_FOR_EACH_PRECISION[k] stores 10^k - 1 (e.g., 9, 99, 999, ...).
    // Adding 1 yields exactly 10^k without computing a power at runtime.
    // Using the precomputed table avoids pow(10, k) and its checked/overflow
    // handling, which is faster and simpler for scaling by 10^delta_scale.
    let max = O::MAX_FOR_EACH_PRECISION.get(delta_scale as usize)?;
    let mul = max.add_wrapping(O::Native::ONE);
    let f_fallible = move |x| O::Native::from_decimal(x)?.mul_checked(mul).ok();

    // if the gain in precision (digits) is greater than the multiplication due to scaling
    // every number will fit into the output type
    // Example: If we are starting with any number of precision 5 [xxxxx],
    // then an increase of scale by 3 will have the following effect on the representation:
    // [xxxxx] -> [xxxxx000], so for the cast to be infallible, the output type
    // needs to provide at least 8 digits precision
    let is_infallible_cast = (input_precision as i16) + delta_scale <= (output_precision as i16);
    let f_infallible = is_infallible_cast
        .then_some(move |x| O::Native::from_decimal(x).unwrap().mul_wrapping(mul));
    Some((f_fallible, f_infallible))
}

/// Construct closures to downscale decimals from `(input_precision, input_scale)` to
/// `(output_precision, output_scale)`.
///
/// Returns `(f_fallible, f_infallible)` where:
/// * `f_fallible` yields `None` when the requested cast would overflow
/// * `f_infallible` is present only when every input is guaranteed to succeed; otherwise it is `None`
///   and callers must fall back to `f_fallible`
///
/// Returns `None` if the required scale reduction `delta_scale = input_scale - output_scale`
/// exceeds the supported precomputed precision table `I::MAX_FOR_EACH_PRECISION`.
/// In this scenario, any value would round to zero (e.g., dividing by 10^k where k exceeds the
/// available precision). Callers should therefore produce zero values (preserving nulls) rather
/// than returning an error.
#[expect(clippy::type_complexity)]
fn make_downscaler<I: DecimalType, O: DecimalType>(
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> Option<(
    impl Fn(I::Native) -> Option<O::Native>,
    Option<impl Fn(I::Native) -> O::Native>,
)>
where
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let delta_scale = input_scale as i16 - output_scale as i16;

    // delta_scale is guaranteed to be > 0, but may also be larger than I::MAX_PRECISION. If so, the
    // scale change divides out more digits than the input has precision and the result of the cast
    // is always zero. For example, if we try to apply delta_scale=10 a decimal32 value, the largest
    // possible result is 999999999/10000000000 = 0.0999999999, which rounds to zero. Smaller values
    // (e.g. 1/10000000000) or larger delta_scale (e.g. 999999999/10000000000000) produce even
    // smaller results, which also round to zero. In that case, just return an array of zeros.
    let max = I::MAX_FOR_EACH_PRECISION.get(delta_scale as usize)?;

    let div = max.add_wrapping(I::Native::ONE);
    let half = div.div_wrapping(I::Native::ONE.add_wrapping(I::Native::ONE));
    let half_neg = half.neg_wrapping();

    let f_fallible = move |x: I::Native| {
        // div is >= 10 and so this cannot overflow
        let (d, r) = x.div_rem_wrapping(div);

        // Round result
        let adjusted = match x >= I::Native::ZERO {
            true if r >= half => d.add_wrapping(I::Native::ONE),
            false if r <= half_neg => d.sub_wrapping(I::Native::ONE),
            _ => d,
        };
        O::Native::from_decimal(adjusted)
    };

    // if the reduction of the input number through scaling (dividing) is greater
    // than a possible precision loss (plus potential increase via rounding)
    // every input number will fit into the output type
    // Example: If we are starting with any number of precision 5 [xxxxx],
    // then and decrease the scale by 3 will have the following effect on the representation:
    // [xxxxx] -> [xx] (+ 1 possibly, due to rounding).
    // The rounding may add a digit, so the cast to be infallible,
    // the output type needs to have at least 3 digits of precision.
    // e.g. Decimal(5, 3) 99.999 to Decimal(3, 0) will result in 100:
    // [99999] -> [99] + 1 = [100], a cast to Decimal(2, 0) would not be possible
    let is_infallible_cast = (input_precision as i16) - delta_scale < (output_precision as i16);
    let f_infallible = is_infallible_cast.then_some(move |x| f_fallible(x).unwrap());
    Some((f_fallible, f_infallible))
}

/// Apply the rescaler function to the value.
/// If the rescaler is infallible, use the infallible function.
/// Otherwise, use the fallible function and validate the precision.
fn apply_rescaler<I: DecimalType, O: DecimalType>(
    value: I::Native,
    output_precision: u8,
    f: impl Fn(I::Native) -> Option<O::Native>,
    f_infallible: Option<impl Fn(I::Native) -> O::Native>,
) -> Option<O::Native>
where
    I::Native: DecimalCast,
    O::Native: DecimalCast,
{
    if let Some(f_infallible) = f_infallible {
        Some(f_infallible(value))
    } else {
        f(value).filter(|v| O::is_valid_decimal_precision(*v, output_precision))
    }
}

/// Rescales a decimal value from `(input_precision, input_scale)` to
/// `(output_precision, output_scale)` and returns the converted number when it fits
/// within the output precision.
///
/// The function first validates that the requested precision and scale are supported for
/// both the source and destination decimal types. It then either upscales (multiplying
/// by an appropriate power of ten) or downscales (dividing with rounding) the input value.
/// When the scaling factor exceeds the precision table of the destination type, the value
/// is treated as an overflow for upscaling, or rounded to zero for downscaling (as any
/// possible result would be zero at the requested scale).
///
/// This mirrors the column-oriented helpers of decimal casting but operates on a single value
/// (row-level) instead of an entire array.
///
/// Returns `None` if the value cannot be represented with the requested precision.
pub fn rescale_decimal<I: DecimalType, O: DecimalType>(
    value: I::Native,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> Option<O::Native>
where
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    validate_decimal_precision_and_scale::<I>(input_precision, input_scale).ok()?;
    validate_decimal_precision_and_scale::<O>(output_precision, output_scale).ok()?;

    if input_scale <= output_scale {
        let (f, f_infallible) =
            make_upscaler::<I, O>(input_precision, input_scale, output_precision, output_scale)?;
        apply_rescaler::<I, O>(value, output_precision, f, f_infallible)
    } else {
        let Some((f, f_infallible)) =
            make_downscaler::<I, O>(input_precision, input_scale, output_precision, output_scale)
        else {
            // Scale reduction exceeds supported precision; result mathematically rounds to zero
            return Some(O::Native::ZERO);
        };
        apply_rescaler::<I, O>(value, output_precision, f, f_infallible)
    }
}

fn cast_decimal_to_decimal_error<I, O>(
    output_precision: u8,
    output_scale: i8,
) -> impl Fn(<I as ArrowPrimitiveType>::Native) -> ArrowError
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    move |x: I::Native| {
        ArrowError::CastError(format!(
            "Cannot cast to {}({}, {}). Overflowing on {:?}",
            O::PREFIX,
            output_precision,
            output_scale,
            x
        ))
    }
}

fn apply_decimal_cast<I: DecimalType, O: DecimalType>(
    array: &PrimitiveArray<I>,
    output_precision: u8,
    output_scale: i8,
    f_fallible: impl Fn(I::Native) -> Option<O::Native>,
    f_infallible: Option<impl Fn(I::Native) -> O::Native>,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array = if let Some(f_infallible) = f_infallible {
        array.unary(f_infallible)
    } else if cast_options.safe {
        array.unary_opt(|x| {
            f_fallible(x).filter(|v| O::is_valid_decimal_precision(*v, output_precision))
        })
    } else {
        let error = cast_decimal_to_decimal_error::<I, O>(output_precision, output_scale);
        array.try_unary(|x| {
            let v = f_fallible(x).ok_or_else(|| error(x))?;
            O::validate_decimal_precision(v, output_precision, output_scale).map(|()| v)
        })?
    };
    Ok(array)
}

fn convert_to_smaller_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    if let Some((f_fallible, f_infallible)) =
        make_downscaler::<I, O>(input_precision, input_scale, output_precision, output_scale)
    {
        apply_decimal_cast(
            array,
            output_precision,
            output_scale,
            f_fallible,
            f_infallible,
            cast_options,
        )
    } else {
        // Scale reduction exceeds supported precision; result mathematically rounds to zero
        let zeros = vec![O::Native::ZERO; array.len()];
        Ok(PrimitiveArray::new(zeros.into(), array.nulls().cloned()))
    }
}

fn convert_to_bigger_or_equal_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    if let Some((f, f_infallible)) =
        make_upscaler::<I, O>(input_precision, input_scale, output_precision, output_scale)
    {
        apply_decimal_cast(
            array,
            output_precision,
            output_scale,
            f,
            f_infallible,
            cast_options,
        )
    } else {
        // Scale increase exceeds supported precision; return overflow error
        Err(ArrowError::CastError(format!(
            "Cannot cast to {}({}, {}). Value overflows for output scale",
            O::PREFIX,
            output_precision,
            output_scale
        )))
    }
}

// Only support one type of decimal cast operations
pub(crate) fn cast_decimal_to_decimal_same_type<T>(
    array: &PrimitiveArray<T>,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<T> =
        if input_scale == output_scale && input_precision <= output_precision {
            array.clone()
        } else if input_scale <= output_scale {
            convert_to_bigger_or_equal_scale_decimal::<T, T>(
                array,
                input_precision,
                input_scale,
                output_precision,
                output_scale,
                cast_options,
            )?
        } else {
            // input_scale > output_scale
            convert_to_smaller_scale_decimal::<T, T>(
                array,
                input_precision,
                input_scale,
                output_precision,
                output_scale,
                cast_options,
            )?
        };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

// Support two different types of decimal cast operations
pub(crate) fn cast_decimal_to_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_precision: u8,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<O> = if input_scale > output_scale {
        convert_to_smaller_scale_decimal::<I, O>(
            array,
            input_precision,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    } else {
        convert_to_bigger_or_equal_scale_decimal::<I, O>(
            array,
            input_precision,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

/// Parses the given string as a decimal with the given scale, returning the
/// unscaled representation in the decimal type's native integer (e.g. `i32`
/// for `Decimal32Type`, `i256` for `Decimal256Type`).
///
/// Returns an error if the input is not a valid decimal string, or if the
/// scaled and rounded value does not fit the maximum precision of the decimal
/// type. The caller is responsible for validating the result against any
/// smaller target precision.
#[deprecated(
    since = "60.0.0",
    note = "Use `arrow_cast::parse::parse_decimal` instead"
)]
pub fn parse_string_to_decimal_native<T: DecimalType>(
    value_str: &str,
    scale: usize,
) -> Result<T::Native, ArrowError> {
    let overflow = || {
        ArrowError::InvalidArgumentError(format!(
            "Cannot convert {value_str} to {}: Overflow",
            T::PREFIX
        ))
    };
    let scale = i8::try_from(scale).map_err(|_| overflow())?;
    parse_decimal_checked::<T>(value_str, T::MAX_PRECISION, scale).map_err(|e| match e {
        DecimalParseError::InvalidFormat => {
            ArrowError::InvalidArgumentError(format!("Invalid decimal format: {value_str:?}"))
        }
        DecimalParseError::Overflow => overflow(),
    })
}

pub(crate) fn generic_string_to_decimal_cast<'a, T, S>(
    from: &'a S,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: DecimalType,
    &'a S: StringArrayType<'a>,
{
    if cast_options.safe {
        let iter = from
            .iter()
            .map(|v| parse_decimal_checked::<T>(v?, precision, scale).ok());
        // Benefit:
        //     15-19% faster than appending to a PrimitiveBuilder (measured
        //     with the cast_kernels string-to-decimal benchmarks)
        // Soundness:
        //     The iterator is trustedLen because it comes from a `StringArray`.
        Ok(unsafe {
            PrimitiveArray::<T>::from_trusted_len_iter(iter)
                .with_precision_and_scale(precision, scale)?
        })
    } else {
        let mut builder = PrimitiveBuilder::<T>::with_capacity(from.len());
        for v in from.iter() {
            match v {
                Some(v) => {
                    let v = parse_decimal_checked::<T>(v, precision, scale).map_err(|e| {
                        let reason = match e {
                            DecimalParseError::InvalidFormat => "invalid decimal format",
                            DecimalParseError::Overflow => "value does not fit",
                        };
                        ArrowError::CastError(format!(
                            "Cannot cast string '{v}' to value of {}({precision}, {scale}) type: {reason}",
                            T::PREFIX,
                        ))
                    })?;
                    builder.append_value(v);
                }
                None => builder.append_null(),
            }
        }
        builder.finish().with_precision_and_scale(precision, scale)
    }
}

pub(crate) fn string_to_decimal_cast<T: DecimalType, Offset: OffsetSizeTrait>(
    from: &GenericStringArray<Offset>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError> {
    generic_string_to_decimal_cast::<T, GenericStringArray<Offset>>(
        from,
        precision,
        scale,
        cast_options,
    )
}

pub(crate) fn string_view_to_decimal_cast<T: DecimalType>(
    from: &StringViewArray,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError> {
    generic_string_to_decimal_cast::<T, StringViewArray>(from, precision, scale, cast_options)
}

/// Cast Utf8 to decimal
pub(crate) fn cast_string_to_decimal<T: DecimalType, Offset: OffsetSizeTrait>(
    from: &dyn Array,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    validate_decimal_precision_and_scale::<T>(precision, scale)?;

    let result = match from.data_type() {
        DataType::Utf8View => string_view_to_decimal_cast::<T>(
            from.as_any().downcast_ref::<StringViewArray>().unwrap(),
            precision,
            scale,
            cast_options,
        )?,
        DataType::Utf8 | DataType::LargeUtf8 => string_to_decimal_cast::<T, Offset>(
            from.as_any()
                .downcast_ref::<GenericStringArray<Offset>>()
                .unwrap(),
            precision,
            scale,
            cast_options,
        )?,
        other => {
            return Err(ArrowError::ComputeError(format!(
                "Cannot cast {other:?} to decimal",
            )));
        }
    };

    Ok(Arc::new(result))
}

pub(crate) fn cast_floating_point_to_decimal<T: ArrowPrimitiveType, D>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: DecimalCast + NativeFromI64,
{
    validate_decimal_precision_and_scale::<D>(precision, scale)?;
    let float_scale = FloatScale::new(scale);
    let inputs = array.values();
    let len = inputs.len();
    let max = D::MAX_FOR_EACH_PRECISION[precision as usize];
    let min = max.neg_wrapping();

    // Pass 1, over every slot (null slots too, with plain arithmetic and no calls): the
    // floating-point path's value, and two bitmaps, `valid` (the value is the result) and
    // `exact` (the slot needs the exact path). The kernels get the product `p` of the input `x`
    // with 10^scale and return the value, `valid` and `not exact` (a negation per word is
    // cheaper than one per lane); they capture their invariants by value.
    let words = len.div_ceil(64);
    let mut values: Vec<D::Native> = vec![D::Native::default(); len];
    let mut valid: Vec<u64> = vec![0; words];
    let mut exact: Vec<u64> = vec![0; words];
    // Below 2^53 the result is the rounded product (`|p| < 2^53` also rejects NaN and
    // infinities). For precision <= 15 every valid result is below 10^15 < 2^53, so no slot
    // needs the exact path; without branches, this kernel vectorizes.
    let max_f = (10_i64.pow(precision.min(15) as u32) - 1) as f64;
    let low = move |p: f64, _: f64| {
        let rounded = p.round();
        (
            D::Native::from_i64_wrapping(rounded as i64),
            rounded.abs() <= max_f,
            true,
        )
    };
    // From precision 16 on, every product below 2^53 is in range (2^53 < 10^16). A product of
    // 2^53 or more needs the exact path, except for an integer below 2^53, which is its own
    // shortest representation: for scale in 0..=22 its result is the exact i128 product, in
    // range if |x| <= int_max.
    let int_pow10 = usize::try_from(scale)
        .ok()
        .and_then(|s| POW10_I128.get(s))
        .copied();
    let int_max = match (int_pow10, max.to_i128()) {
        (Some(pow10), Some(max)) => (max / pow10).min(1 << 53) as f64,
        // 2^53 * 10^22 < 2^127 <= max
        _ => 9_007_199_254_740_992.0,
    };
    let high = move |p: f64, x: f64| {
        if p.abs() < 9_007_199_254_740_992.0 {
            (D::Native::from_i64_wrapping(p.round() as i64), true, true)
        } else if let Some(pow10) = int_pow10.filter(|_| is_small_integer(x)) {
            let product = D::Native::from_decimal(x as i64 as i128 * pow10);
            (product.unwrap_or_default(), x.abs() <= int_max, true)
        } else {
            // NaN is invalid; infinities take the exact path, which rejects them
            (D::Native::default(), false, p.is_nan())
        }
    };
    if precision <= 15 {
        fast_pass(
            inputs,
            &mut values,
            &mut valid,
            &mut exact,
            float_scale.mul,
            low,
        )
    } else {
        fast_pass(
            inputs,
            &mut values,
            &mut valid,
            &mut exact,
            float_scale.mul,
            high,
        )
    }

    // Pass 2: the exact path for the slots that need it (a product of 2^53 or more that is not a
    // small integer, with precision >= 16), skipping nulls.
    let nulls = array.nulls();
    let exact = BooleanBuffer::new(Buffer::from_vec(exact), 0, len);
    let exact = match nulls {
        Some(n) => &exact & n.inner(),
        None => exact,
    };
    for i in exact.set_indices() {
        let result = shortest_decimal_scaled(inputs[i].as_(), scale)
            .and_then(D::Native::from_decimal)
            .filter(|&n| n >= min && n <= max);
        if let Some(n) = result {
            values[i] = n;
            valid[i / 64] |= 1 << (i % 64);
        }
    }

    let valid = BooleanBuffer::new(Buffer::from_vec(valid), 0, len);
    let valid = match nulls {
        Some(n) => &valid & n.inner(),
        None => valid,
    };
    if !cast_options.safe && valid.count_set_bits() != array.len() - array.null_count() {
        // the first failing row's error, as the scalar function reports it
        let i = (0..len)
            .find(|&i| nulls.is_none_or(|n| n.is_valid(i)) && !valid.value(i))
            .expect("a failing row");
        let input: f64 = inputs[i].as_();
        let error = match float_to_decimal_native::<D>(input, float_scale) {
            Some(n) => FloatToDecimalError::<D>::PrecisionOverflow(n),
            None => FloatToDecimalError::<D>::NativeOverflow,
        };
        return Err(error.into_arrow_error(inputs[i], precision, scale));
    }
    let nulls = NullBuffer::new(valid);
    let nulls = (nulls.null_count() > 0).then_some(nulls);
    PrimitiveArray::<D>::new(ScalarBuffer::from(values), nulls)
        .with_precision_and_scale(precision, scale)
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Converts a floating point value to the unscaled integer representation of a decimal.
///
/// Scales the input by `10^scale` and rounds half away from zero. When the scaled value reaches
/// 2^53, where an `f64` no longer holds every integer, the shortest decimal representation of the
/// input (the digits arrow displays it with) is scaled exactly instead, so e.g. `295161.0` at
/// scale 15 is `295161 * 10^15`. Returns an error
/// if the precision or scale is invalid, or the result does not fit the native
/// type or the requested precision. Non-finite inputs are rejected.
/// Use [`float_to_decimal_opt`] when the reason for failure is not needed.
///
/// ```
/// use arrow_array::types::Decimal32Type;
/// use arrow_cast::cast::float_to_decimal;
///
/// assert_eq!(float_to_decimal::<Decimal32Type>(12.345, 5, 2).unwrap(), 1235);
/// assert!(float_to_decimal::<Decimal32Type>(12345.678, 5, 2).is_err());
/// ```
#[inline]
pub fn float_to_decimal<D>(input: f64, precision: u8, scale: i8) -> Result<D::Native, ArrowError>
where
    D: DecimalType,
    D::Native: DecimalCast,
{
    validate_decimal_precision_and_scale::<D>(precision, scale)?;
    float_to_decimal_checked::<D>(input, FloatScale::new(scale), precision)
        .map_err(|e| e.into_arrow_error(input, precision, scale))
}

/// Like [`float_to_decimal`], but returns `None` on failure.
/// With valid precision and scale, conversion failures do not allocate an error message.
///
/// ```
/// use arrow_array::types::Decimal32Type;
/// use arrow_cast::cast::float_to_decimal_opt;
///
/// assert_eq!(float_to_decimal_opt::<Decimal32Type>(12.345, 5, 2), Some(1235));
/// assert_eq!(float_to_decimal_opt::<Decimal32Type>(12345.678, 5, 2), None);
/// ```
pub fn float_to_decimal_opt<D>(input: f64, precision: u8, scale: i8) -> Option<D::Native>
where
    D: DecimalType,
    D::Native: DecimalCast,
{
    validate_decimal_precision_and_scale::<D>(precision, scale).ok()?;
    float_to_decimal_checked::<D>(input, FloatScale::new(scale), precision).ok()
}

// Keep failure reporting allocation-free for scalar Option and safe array casts.
enum FloatToDecimalError<D: DecimalType> {
    NativeOverflow,
    PrecisionOverflow(D::Native),
}

impl<D: DecimalType> FloatToDecimalError<D> {
    fn into_arrow_error(self, input: impl std::fmt::Debug, precision: u8, scale: i8) -> ArrowError {
        match self {
            Self::NativeOverflow => ArrowError::CastError(format!(
                "Cannot cast to {}({}, {}). Overflowing on {:?}",
                D::PREFIX,
                precision,
                scale,
                input
            )),
            Self::PrecisionOverflow(value) => {
                D::validate_decimal_precision(value, precision, scale).unwrap_err()
            }
        }
    }
}

// Precision and scale must already be validated.
#[inline(always)]
fn float_to_decimal_checked<D>(
    input: f64,
    scale: FloatScale,
    precision: u8,
) -> Result<D::Native, FloatToDecimalError<D>>
where
    D: DecimalType,
    D::Native: DecimalCast,
{
    let value =
        float_to_decimal_native::<D>(input, scale).ok_or(FloatToDecimalError::NativeOverflow)?;
    if D::is_valid_decimal_precision(value, precision) {
        Ok(value)
    } else {
        Err(FloatToDecimalError::PrecisionOverflow(value))
    }
}

/// What [`float_to_decimal_native`] needs to know about a scale, computed once per array.
#[derive(Clone, Copy)]
struct FloatScale {
    scale: i8,
    /// `10^scale` as an `f64`
    mul: f64,
}

impl FloatScale {
    #[inline]
    fn new(scale: i8) -> Self {
        Self {
            scale,
            mul: 10_f64.powi(scale as i32),
        }
    }
}

/// `input * 10^scale` rounded half away from zero, or `None` if `input` is not finite or the
/// result does not fit `D::Native`.
///
/// The product is computed in floating point while it is below 2^53. Above that an `f64` no
/// longer holds every integer, and the floating-point product is off by up to half its ulp:
/// `295161.0` at scale 15 would give `295160999999999967232`. There the shortest decimal
/// representation of `input` (the digits arrow displays it with) is scaled exactly instead.
#[inline(always)]
fn float_to_decimal_native<D>(input: f64, scale: FloatScale) -> Option<D::Native>
where
    D: DecimalType,
    D::Native: DecimalCast,
{
    let p = scale.mul * input;
    // false for NaN and infinities
    if p.abs() < 9_007_199_254_740_992.0 {
        // |p.round()| <= 2^53: exact as i64, and cheaper than converting f64 to i128
        return D::Native::from_decimal(p.round() as i64);
    }
    shortest_decimal_scaled(input, scale.scale).and_then(D::Native::from_decimal)
}

/// A decimal native from an `i64` without a range check: the array cast's first pass converts
/// every slot, valid or not, and masks the invalid ones.
pub(crate) trait NativeFromI64 {
    fn from_i64_wrapping(v: i64) -> Self;
}

impl NativeFromI64 for i32 {
    fn from_i64_wrapping(v: i64) -> Self {
        v as i32
    }
}

impl NativeFromI64 for i64 {
    fn from_i64_wrapping(v: i64) -> Self {
        v
    }
}

impl NativeFromI64 for i128 {
    fn from_i64_wrapping(v: i64) -> Self {
        v as i128
    }
}

impl NativeFromI64 for i256 {
    fn from_i64_wrapping(v: i64) -> Self {
        i256::from_i128(v as i128)
    }
}

/// Pass 1 of the array cast: `kernel` on every input and its product with 10^scale
/// (`x * mul`), 64 at a time, into `values` and one word per 64 inputs of the `valid` and
/// `exact` bitmaps; the kernel returns the value, `valid` and `not exact`. A separate function
/// over plain slices, so that the loop holds its invariants and bit accumulators in registers;
/// kernels without branches vectorize.
#[inline(never)]
fn fast_pass<T, N, K>(
    inputs: &[T],
    values: &mut [N],
    valid: &mut [u64],
    exact: &mut [u64],
    mul: f64,
    kernel: K,
) where
    T: AsPrimitive<f64>,
    K: Fn(f64, f64) -> (N, bool, bool),
{
    let chunks = inputs.chunks(64).zip(values.chunks_mut(64));
    for ((inputs, values), (valid, exact)) in chunks.zip(valid.iter_mut().zip(exact.iter_mut())) {
        let (mut valid_bits, mut not_exact_bits) = (0u64, 0u64);
        for (bit, (&input, value)) in inputs.iter().zip(values.iter_mut()).enumerate() {
            let x: f64 = input.as_();
            let (n, is_valid, not_exact) = kernel(mul * x, x);
            valid_bits |= (is_valid as u64) << bit;
            not_exact_bits |= (not_exact as u64) << bit;
            *value = n;
        }
        *valid = valid_bits;
        // the bits past a short last chunk stay clear
        *exact = !not_exact_bits & (u64::MAX >> (64 - inputs.len()));
    }
}

/// `10^i` for `i` in `0..=22`.
const POW10_I128: [i128; 23] = {
    let mut table = [1i128; 23];
    let mut i = 1;
    while i < table.len() {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// Whether `v` is an integer below 2^53 in magnitude. Such an integer is its own shortest
/// representation: its rounding interval is at most 1 wide and holds no other integer.
#[inline(always)]
fn is_small_integer(v: f64) -> bool {
    v.abs() < 9_007_199_254_740_992.0 && v == v.trunc()
}

/// The exact fallback for [`float_to_decimal_native`]: the shortest round-trip digits of `input`
/// from `ryu` (as arrow displays floats), scaled in integer arithmetic; `None` if
/// `input` is not finite or the result does not fit `i256`.
#[inline(never)]
fn shortest_decimal_scaled(input: f64, scale: i8) -> Option<i256> {
    if !input.is_finite() {
        return None;
    }
    // an integer below 2^53 is its own shortest representation: scale it without formatting
    if scale >= 0 && is_small_integer(input) {
        if let Some(&pow) = POW10_I128.get(scale as usize) {
            return Some(i256::from_i128(input as i64 as i128 * pow));
        }
        return i256::from_i128(input as i64 as i128)
            .checked_mul(i256::from_i128(10).checked_pow(scale as u32)?);
    }
    let negative = input.is_sign_negative();
    let mut buffer = ryu::Buffer::new();
    // Ryū emits at most 17 significant digits. Parse its known ASCII grammar in one
    // pass, without general-purpose string splitting or exponent parsing.
    let text = buffer.format_finite(input.abs()).as_bytes();
    let mut digits = 0u64;
    let mut fractional = 0i32;
    let mut after_dot = false;
    let mut i = 0;
    while i < text.len() && text[i] != b'e' {
        let c = text[i];
        if c == b'.' {
            after_dot = true;
        } else {
            digits = digits * 10 + (c - b'0') as u64;
            fractional += after_dot as i32;
        }
        i += 1;
    }
    let mut exponent = 0i32;
    if i < text.len() {
        i += 1;
        let negative_exponent = text[i] == b'-';
        if negative_exponent || text[i] == b'+' {
            i += 1;
        }
        while i < text.len() {
            exponent = exponent * 10 + (text[i] - b'0') as i32;
            i += 1;
        }
        if negative_exponent {
            exponent = -exponent;
        }
    }
    if digits == 0 {
        return Some(i256::ZERO);
    }
    let power = exponent - fractional + scale as i32;
    let magnitude = if power >= 0 {
        // digits < 10^17: the product fits i128 up to 10^21
        match POW10_I128.get(power as usize) {
            Some(&pow) if power <= 21 => i256::from_i128(digits as i128 * pow),
            _ => i256::from_i128(digits as i128)
                .checked_mul(i256::from_i128(10).checked_pow(power as u32)?)?,
        }
    } else {
        let k = power.unsigned_abs();
        // digits < 10^17, so digits / 10^k < 0.5 once k > 17
        if k > 17 {
            i256::ZERO
        } else {
            let divisor = 10u64.pow(k);
            let (q, r) = (digits / divisor, digits % divisor);
            i256::from_i128((q + (r >= divisor - r) as u64) as i128)
        }
    };
    Some(if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    })
}

/// Cast a single floating point value to a decimal native with the given multiple.
/// Returns `None` if the scaled and rounded value does not fit the native integer
/// type. The caller must separately validate the decimal precision.
#[deprecated(since = "60.0.0", note = "Use `float_to_decimal` instead")]
#[inline(always)]
pub fn single_float_to_decimal<D>(input: f64, mul: f64) -> Option<D::Native>
where
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: DecimalCast,
{
    D::Native::from_f64((mul * input).round())
}

pub(crate) fn cast_decimal_to_integer<D, T>(
    array: &dyn Array,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: NumCast,
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ToPrimitive,
{
    let array = array.as_primitive::<D>();

    // div is always > 0, so div_wrapping(div) can never overflow/panic
    let div: D::Native = D::Native::usize_as(10)
        .pow_checked(scale.unsigned_abs() as u32)
        .map_err(|_| {
            ArrowError::CastError(format!(
                "Cannot cast to {:?}. The scale {} causes overflow.",
                D::PREFIX,
                scale,
            ))
        })?;

    let mut value_builder = PrimitiveBuilder::<T>::with_capacity(array.len());

    if scale < 0 {
        match cast_options.safe {
            true => {
                for i in 0..array.len() {
                    if array.is_null(i) {
                        value_builder.append_null();
                    } else {
                        let v = array
                            .value(i)
                            .mul_checked(div)
                            .ok()
                            .and_then(<T::Native as NumCast>::from::<D::Native>);
                        value_builder.append_option(v);
                    }
                }
            }
            false => {
                for i in 0..array.len() {
                    if array.is_null(i) {
                        value_builder.append_null();
                    } else {
                        let v = array.value(i).mul_checked(div)?;

                        let value =
                            <T::Native as NumCast>::from::<D::Native>(v).ok_or_else(|| {
                                ArrowError::CastError(format!(
                                    "value of {:?} is out of range {}",
                                    v,
                                    T::DATA_TYPE
                                ))
                            })?;

                        value_builder.append_value(value);
                    }
                }
            }
        }
    } else {
        match cast_options.safe {
            true => {
                for i in 0..array.len() {
                    if array.is_null(i) {
                        value_builder.append_null();
                    } else {
                        let v = array.value(i).div_wrapping(div);
                        let v = <T::Native as NumCast>::from(v);
                        value_builder.append_option(v);
                    }
                }
            }
            false => {
                for i in 0..array.len() {
                    if array.is_null(i) {
                        value_builder.append_null();
                    } else {
                        let v = array.value(i).div_wrapping(div);

                        let value =
                            <T::Native as NumCast>::from::<D::Native>(v).ok_or_else(|| {
                                ArrowError::CastError(format!(
                                    "value of {:?} is out of range {}",
                                    v,
                                    T::DATA_TYPE
                                ))
                            })?;

                        value_builder.append_value(value);
                    }
                }
            }
        }
    }
    Ok(Arc::new(value_builder.finish()))
}

/// Cast a decimal array to a floating point array.
///
/// Conversion is lossy and follows standard floating point semantics. Values
/// that exceed the representable range become `INFINITY` or `-INFINITY` without
/// returning an error.
pub(crate) fn cast_decimal_to_float<D: DecimalType, T: ArrowPrimitiveType, F>(
    array: &dyn Array,
    op: F,
) -> Result<ArrayRef, ArrowError>
where
    F: Fn(D::Native) -> T::Native,
{
    let array = array.as_primitive::<D>();
    let array = array.unary::<_, T>(op);
    Ok(Arc::new(array))
}

/// Convert an integer to a decimal native value without wrapping.
///
/// `AsPrimitive` / `as` silently truncates when the source is wider than `M`
/// (for example `5_000_000_000i64 as i32`). All integer sources fit in `i128`
/// losslessly, which [`DecimalCast`] then converts to the decimal native type
/// with a range check. For types that always fit (e.g. `i64` to `Decimal128`) this
/// should get optimized to being equivalent to `i64 as i128`.
fn integer_to_decimal_native<I, M>(value: I) -> Option<M>
where
    I: Into<i128>,
    M: DecimalCast,
{
    M::from_decimal(value.into())
}

/// Returns true if every value of `array` has at most `digits` decimal digits.
///
/// If the source type cannot hold that many digits, every value fits;
/// otherwise the data is scanned, including null slots, since `unary`
/// converts those as well.
fn integers_fit_digits<T>(array: &PrimitiveArray<T>, digits: u32) -> bool
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp,
{
    let Ok(bound) = T::Native::usize_as(10).pow_checked(digits) else {
        // The type's maximum is below the bound, and its minimum, one larger
        // in magnitude, is a power of two that no power of ten can equal.
        return true;
    };
    let hi = bound.sub_wrapping(T::Native::ONE);
    // Unsigned types have no negative values to bound.
    let lo = T::Native::ZERO.sub_checked(hi).unwrap_or(T::Native::ZERO);

    // The scan works in chunks, so an array that does not fit is rejected after
    // its first failing chunk. Each chunk is checked without branching, so it
    // vectorizes.
    array
        .values()
        .chunks(64)
        .all(|chunk| chunk.iter().fold(true, |ok, &v| ok & (v >= lo) & (v <= hi)))
}

fn cast_integer_to_decimal<
    T: ArrowPrimitiveType,
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M,
>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + Into<i128>,
    M: ArrowNativeTypeOp + DecimalCast,
{
    let overflow = |v: T::Native| {
        ArrowError::CastError(format!(
            "Cannot cast to {}({precision}, {scale}). Overflowing on {v:?}",
            D::PREFIX,
        ))
    };

    // An invalid precision or scale would let the bound below admit values the
    // decimal native type cannot hold, so reject it before choosing a kernel.
    validate_decimal_precision_and_scale::<D>(precision, scale)?;

    // Scan for values that don't fit in the target type before choosing a
    // kernel: in the common case that all values fit, the cast is infallible
    // and we can use the `unary` kernel, which is very fast. Empirically, the
    // cost of doing the out-of-range scan is small relative to the win from
    // using a faster kernel.
    //
    // A value fits when it has at most `precision - scale` decimal digits: a
    // nonnegative scale appends that many trailing zeros, and a negative scale
    // removes digits first. Validation guarantees the difference is not
    // negative.
    let digits = (precision as i32 - scale as i32) as u32;
    let all_fit = integers_fit_digits(array, digits);

    let array = if scale < 0 {
        // Compute the scale factor once in the source type. Scaling before the
        // checked conversion permits values that only fit the decimal native
        // type after scaling.
        //
        // scale_factor is always > 0, so div_wrapping(scale_factor) can never
        // overflow/panic
        let scale_factor = T::Native::usize_as(10)
            .pow_checked(scale.unsigned_abs() as u32)
            .ok();

        match (scale_factor, cast_options.safe) {
            (Some(scale_factor), _) if all_fit => array.unary::<_, D>(|v| {
                integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))
                    .expect("value fits the decimal")
            }),
            (Some(scale_factor), true) => array.unary_opt::<_, D>(|v| {
                let v = integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))?;
                (D::is_valid_decimal_precision(v, precision)).then_some(v)
            }),
            (Some(scale_factor), false) => array.try_unary::<_, D, _>(|v| {
                let v = integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))
                    .ok_or_else(|| overflow(v))?;
                D::validate_decimal_precision(v, precision, scale).map(|()| v)
            })?,
            // A scale factor that overflows the source type is larger than all
            // source values, so integer division produces zero.
            //
            // For a well formed decimal scale, this path should never be reachable.
            (None, _) => array.unary::<_, D>(|_| M::ZERO),
        }
    } else {
        let scale_factor = M::usize_as(10)
            .pow_checked(scale.unsigned_abs() as u32)
            .map_err(|_| {
                ArrowError::CastError(format!(
                    "Cannot cast to {:?}({}, {}). The scale causes overflow.",
                    D::PREFIX,
                    precision,
                    scale,
                ))
            })?;

        if all_fit {
            array.unary::<_, D>(|v| {
                integer_to_decimal_native::<_, M>(v)
                    .expect("value fits the decimal")
                    .mul_wrapping(scale_factor)
            })
        } else if cast_options.safe {
            array.unary_opt::<_, D>(|v| {
                let v = integer_to_decimal_native::<_, M>(v)
                    .and_then(|v| v.mul_checked(scale_factor).ok())?;
                (D::is_valid_decimal_precision(v, precision)).then_some(v)
            })
        } else {
            array.try_unary::<_, D, _>(|v| {
                let v = integer_to_decimal_native::<_, M>(v)
                    .ok_or_else(|| overflow(v))
                    .and_then(|v| v.mul_checked(scale_factor))?;
                D::validate_decimal_precision(v, precision, scale).map(|()| v)
            })?
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(precision, scale)?))
}

pub(crate) fn cast_from_decimal<D, F>(
    array: &dyn Array,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    as_float: F,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ToPrimitive,
    F: Fn(D::Native) -> f64,
{
    use DataType::*;
    // cast decimal to other type
    match to_type {
        UInt8 => cast_decimal_to_integer::<D, UInt8Type>(array, *scale, cast_options),
        UInt16 => cast_decimal_to_integer::<D, UInt16Type>(array, *scale, cast_options),
        UInt32 => cast_decimal_to_integer::<D, UInt32Type>(array, *scale, cast_options),
        UInt64 => cast_decimal_to_integer::<D, UInt64Type>(array, *scale, cast_options),
        Int8 => cast_decimal_to_integer::<D, Int8Type>(array, *scale, cast_options),
        Int16 => cast_decimal_to_integer::<D, Int16Type>(array, *scale, cast_options),
        Int32 => cast_decimal_to_integer::<D, Int32Type>(array, *scale, cast_options),
        Int64 => cast_decimal_to_integer::<D, Int64Type>(array, *scale, cast_options),
        Float16 => cast_decimal_to_float::<D, Float16Type, _>(array, |x| {
            half::f16::from_f64(single_decimal_to_float_lossy::<D, F>(
                &as_float,
                x,
                <i32 as From<i8>>::from(*scale),
            ))
        }),
        Float32 => cast_decimal_to_f32::<D, F>(array, &as_float, *scale),
        Float64 => cast_decimal_to_f64::<D, F>(array, &as_float, *scale),
        Utf8View => value_to_string_view(array, cast_options),
        Utf8 => value_to_string::<i32>(array, cast_options),
        LargeUtf8 => value_to_string::<i64>(array, cast_options),
        Null => Ok(new_null_array(to_type, array.len())),
        _ => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
    }
}

pub(crate) fn cast_to_decimal<D, M>(
    array: &dyn Array,
    precision: &u8,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M: ArrowNativeTypeOp + DecimalCast + NativeFromI64,
{
    use DataType::*;
    // cast data to decimal
    match from_type {
        UInt8 => cast_integer_to_decimal::<_, D, M>(
            array.as_primitive::<UInt8Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int8 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int8Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float16 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float32 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float64 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Utf8View | Utf8 => {
            cast_string_to_decimal::<D, i32>(array, *precision, *scale, cast_options)
        }
        LargeUtf8 => cast_string_to_decimal::<D, i64>(array, *precision, *scale, cast_options),
        Null => Ok(new_null_array(to_type, array.len())),
        _ => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_float_to_decimal() {
        fn check<D: DecimalType>()
        where
            D::Native: DecimalCast,
        {
            for (input, scale, expected) in [
                (12.345, 2, 1235),
                (-12.345, 2, -1235),
                (999.99, 2, 99999),
                (-999.99, 2, -99999),
                (99999.25, 0, 99999),
                (999992.5, -1, 99999),
                (0.0, 2, 0),
            ] {
                assert_eq!(
                    float_to_decimal_opt::<D>(input, 5, scale),
                    float_to_decimal::<D>(input, 5, scale).ok(),
                );
                assert_eq!(
                    float_to_decimal::<D>(input, 5, scale)
                        .unwrap()
                        .to_i128()
                        .unwrap(),
                    expected,
                );
            }
            for (input, scale) in [
                (12345.678, 2),
                (-12345.678, 2),
                (999.9975, 2),
                (-999.9975, 2),
                (99999.75, 0),
                (-99999.75, 0),
                (999997.5, -1),
                (-999997.5, -1),
                (f64::NAN, 0),
                (f64::INFINITY, 0),
                (f64::NEG_INFINITY, 0),
                (f64::MAX, 0),
            ] {
                assert!(float_to_decimal::<D>(input, 5, scale).is_err());
                assert!(float_to_decimal_opt::<D>(input, 5, scale).is_none());
            }
            for (precision, scale) in [(0, 0), (D::MAX_PRECISION + 1, 0), (5, 6)] {
                assert!(float_to_decimal::<D>(0.0, precision, scale).is_err());
                assert!(float_to_decimal_opt::<D>(0.0, precision, scale).is_none());
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    #[expect(deprecated)]
    fn test_single_float_to_decimal_preserves_native_range() {
        // Deprecation preserves acceptance of values that fit i32 but exceed
        // Decimal32's maximum precision. The new API checks that precision.
        for (input, expected) in [
            (1_000_000_000.0, 1_000_000_000),
            (-1_000_000_000.0, -1_000_000_000),
            (999_999_999.75, 1_000_000_000),
            (-999_999_999.75, -1_000_000_000),
        ] {
            assert_eq!(
                single_float_to_decimal::<Decimal32Type>(input, 1.0),
                Some(expected)
            );
            assert!(float_to_decimal::<Decimal32Type>(input, 9, 0).is_err());
        }
        assert_eq!(
            single_float_to_decimal::<Decimal32Type>(999_999_999.0, 1.0),
            Some(999_999_999),
        );
        assert_eq!(
            single_float_to_decimal::<Decimal32Type>(12.345, 100.0),
            Some(1235)
        );
        assert_eq!(
            single_float_to_decimal::<Decimal32Type>(f64::MAX, 1.0),
            None
        );
    }

    #[test]
    #[expect(deprecated)]
    fn test_parse_string_to_decimal_native() {
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123.456", 2).unwrap(),
            12346
        );
        // The value is checked against the maximum precision of the type, not
        // against any target precision
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>(&"9".repeat(38), 0).unwrap(),
            10_i128.pow(38) - 1
        );
        assert!(
            parse_string_to_decimal_native::<Decimal128Type>(&i128::MAX.to_string(), 0).is_err()
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("abc", 2)
                .unwrap_err()
                .to_string(),
            "Invalid argument error: Invalid decimal format: \"abc\""
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal32Type>("1", 10)
                .unwrap_err()
                .to_string(),
            "Invalid argument error: Cannot convert 1 to Decimal32: Overflow"
        );
    }

    #[test]
    fn test_rescale_decimal_upscale_within_precision() {
        let result = rescale_decimal::<Decimal128Type, Decimal128Type>(
            12_345_i128, // 123.45 with scale 2
            5,
            2,
            8,
            5,
        );
        assert_eq!(result, Some(12_345_000_i128));
    }

    #[test]
    fn test_rescale_decimal_downscale_rounds_half_away_from_zero() {
        let positive = rescale_decimal::<Decimal128Type, Decimal128Type>(
            1_050_i128, // 1.050 with scale 3
            5, 3, 5, 1,
        );
        assert_eq!(positive, Some(11_i128)); // 1.1 with scale 1

        let negative = rescale_decimal::<Decimal128Type, Decimal128Type>(
            -1_050_i128, // -1.050 with scale 3
            5,
            3,
            5,
            1,
        );
        assert_eq!(negative, Some(-11_i128)); // -1.1 with scale 1
    }

    #[test]
    fn test_rescale_decimal_downscale_large_delta_returns_zero() {
        let result = rescale_decimal::<Decimal32Type, Decimal32Type>(12_345_i32, 9, 9, 9, 4);
        assert_eq!(result, Some(0_i32));
    }

    #[test]
    fn test_rescale_decimal_upscale_overflow_returns_none() {
        let result = rescale_decimal::<Decimal32Type, Decimal32Type>(9_999_i32, 4, 0, 5, 2);
        assert_eq!(result, None);
    }

    #[test]
    fn test_rescale_decimal256_extreme_scales() {
        assert_eq!(
            rescale_decimal::<Decimal256Type, Decimal256Type>(i256::ONE, 76, -76, 76, 0),
            None
        );
        assert_eq!(
            rescale_decimal::<Decimal256Type, Decimal256Type>(i256::ZERO, 76, -76, 76, 0),
            Some(i256::ZERO)
        );
        assert_eq!(
            rescale_decimal::<Decimal256Type, Decimal256Type>(i256::ONE, 76, i8::MIN, 76, 76),
            None
        );
        assert_eq!(
            rescale_decimal::<Decimal256Type, Decimal256Type>(i256::ONE, 76, 76, 76, i8::MIN),
            Some(i256::ZERO)
        );
    }

    #[test]
    fn test_rescale_decimal_invalid_input_precision_scale_returns_none() {
        let result = rescale_decimal::<Decimal128Type, Decimal128Type>(123_i128, 39, 39, 38, 38);
        assert_eq!(result, None);
    }

    #[test]
    fn test_rescale_decimal_invalid_output_precision_scale_returns_none() {
        let result = rescale_decimal::<Decimal128Type, Decimal128Type>(123_i128, 38, 38, 39, 39);
        assert_eq!(result, None);
    }

    /// Reference for [`float_to_decimal_native`], independent of `ryu`: the shortest decimal
    /// that round-trips to `input` and is nearest to it (ties to an even last digit), rounded
    /// half away from zero at `scale` digits; `None` if the result does not fit `i256`.
    /// The number of significant digits comes from `core`'s `Display` (shortest round-trip),
    /// the digits from the exact expansion of `input` rounded half-even to that many.
    fn shortest_scaled(input: f64, scale: i8) -> Option<i256> {
        let digits_of = |text: &str| -> (Vec<u8>, i32) {
            // all digits without the point, and the number of integer digits
            let (int_part, frac_part) = text.split_once('.').unwrap_or((text, ""));
            let all: Vec<u8> = int_part
                .bytes()
                .chain(frac_part.bytes())
                .map(|c| c - b'0')
                .collect();
            (all, int_part.len() as i32)
        };
        let (shortest, _) = digits_of(&format!("{}", input.abs()));
        let significant = shortest
            .iter()
            .skip_while(|&&d| d == 0)
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .skip_while(|&&&d| d == 0)
            .count();
        if significant == 0 {
            return Some(i256::ZERO);
        }
        // exact expansion: 1100 fraction digits cover every f64
        let (exact, int_len) = digits_of(&format!("{:.1100}", input.abs()));
        let first = exact.iter().position(|&d| d != 0).unwrap();
        // value = 0.d1 d2 ... * 10^point
        let point = int_len - first as i32;
        let sig = &exact[first..];
        let mut kept: Vec<u8> = sig[..significant].to_vec();
        let rest = &sig[significant..];
        let round_up = match rest.first() {
            Some(&d) if d > 5 => true,
            Some(&5) => rest[1..].iter().any(|&d| d != 0) || kept.last().unwrap() % 2 == 1,
            _ => false,
        };
        let mut value =
            i256::from_string(&kept.iter().map(|d| (d + b'0') as char).collect::<String>())?;
        if round_up {
            value = value.checked_add(i256::ONE)?;
        }
        kept.clear();
        // value * 10^(point - significant) is the shortest decimal; scale it and round
        let power = point - significant as i32 + scale as i32;
        let ten = i256::from_i128(10);
        let scaled = if power >= 0 {
            value.checked_mul(ten.checked_pow(power as u32)?)?
        } else {
            match ten.checked_pow(power.unsigned_abs()) {
                Some(divisor) => {
                    let (q, r) = value.checked_div_rem(divisor)?;
                    if r >= divisor - r { q + i256::ONE } else { q }
                }
                None => i256::ZERO,
            }
        };
        Some(if input < 0.0 {
            scaled.wrapping_neg()
        } else {
            scaled
        })
    }

    /// Below 2^53 the floating-point product, rounded, as before; from 2^53 on the reference.
    fn check_float_to_decimal<D>(input: f64, scale: i8)
    where
        D: DecimalType,
        D::Native: DecimalCast + std::fmt::Debug + PartialEq,
    {
        let p = input * 10_f64.powi(scale as i32);
        let expected = if p.abs() < 9_007_199_254_740_992.0 {
            D::Native::from_f64(p.round())
        } else if input.is_finite() {
            shortest_scaled(input, scale).and_then(D::Native::from_decimal)
        } else {
            None
        };
        let actual = float_to_decimal_native::<D>(input, FloatScale::new(scale));
        assert_eq!(actual, expected, "input {input:e} scale {scale}");
    }

    #[test]
    fn test_float_to_decimal_large_products() {
        // from 2^53 on, the floating-point product is inexact: 295161.0 at scale 15 gave
        // 295160999999999967232
        assert_eq!(
            float_to_decimal::<Decimal128Type>(295161.0, 31, 15).unwrap(),
            295161 * 10_i128.pow(15)
        );
        assert_eq!(
            float_to_decimal::<Decimal128Type>(123456789.0, 38, 20).unwrap(),
            123456789 * 10_i128.pow(20)
        );
        assert_eq!(
            float_to_decimal::<Decimal128Type>(1234.56, 38, 20).unwrap(),
            123456 * 10_i128.pow(18)
        );
        assert_eq!(
            float_to_decimal::<Decimal128Type>(-0.1, 38, 20).unwrap(),
            -(10_i128.pow(19))
        );
        assert_eq!(
            float_to_decimal::<Decimal256Type>(0.1, 76, 55).unwrap(),
            i256::from_i128(10).wrapping_pow(54)
        );
        assert_eq!(
            float_to_decimal::<Decimal256Type>(1.5e30, 40, 0).unwrap(),
            i256::from_i128(15) * i256::from_i128(10).wrapping_pow(29)
        );
        // below 2^53 the result is unchanged: the rounded floating-point product
        assert_eq!(
            float_to_decimal::<Decimal64Type>(1.005, 10, 2).unwrap(),
            100
        );
        assert_eq!(float_to_decimal::<Decimal64Type>(0.15, 10, 1).unwrap(), 2);
        assert_eq!(float_to_decimal::<Decimal64Type>(-2.5, 10, 0).unwrap(), -3);
        assert_eq!(
            float_to_decimal::<Decimal32Type>(12.345, 5, 2).unwrap(),
            1235
        );
        assert_eq!(
            float_to_decimal::<Decimal128Type>(123456.0, 10, -2).unwrap(),
            1235
        );
        // tiny, huge and non-finite
        assert_eq!(
            float_to_decimal::<Decimal128Type>(5e-324, 38, 38).unwrap(),
            0
        );
        assert_eq!(float_to_decimal::<Decimal128Type>(-0.0, 38, 2).unwrap(), 0);
        assert!(float_to_decimal::<Decimal256Type>(f64::MAX, 76, 0).is_err());
        assert!(float_to_decimal::<Decimal128Type>(f64::NAN, 38, 2).is_err());
        assert!(float_to_decimal::<Decimal128Type>(f64::INFINITY, 38, 2).is_err());
        assert!(float_to_decimal::<Decimal128Type>(1.7e38, 38, 0).is_err());
        assert!(float_to_decimal_opt::<Decimal128Type>(1.7e38, 38, 0).is_none());
    }

    #[test]
    fn test_float_to_decimal_matches_reference() {
        // deterministic xorshift over sign, exponent and mantissa
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut checked = 0;
        for _ in 0..20_000 {
            let r = next();
            // exponents roughly from 2^-80 to 2^130: results from 0 to beyond i256
            let exponent = 1023 - 80 + (r >> 53) % 210;
            let input =
                f64::from_bits((r & (1 << 63)) | (exponent << 52) | (next() & ((1 << 52) - 1)));
            let scale = (next() % 120) as i8 - 40; // -40 ..= 79, clamped per type below
            if (0..=9).contains(&scale) {
                check_float_to_decimal::<Decimal32Type>(input, scale);
            }
            if (-20..=18).contains(&scale) {
                check_float_to_decimal::<Decimal64Type>(input, scale);
            }
            if (-40..=38).contains(&scale) {
                check_float_to_decimal::<Decimal128Type>(input, scale);
            }
            if scale <= 76 {
                check_float_to_decimal::<Decimal256Type>(input, scale);
            }
            checked += 1;
        }
        assert_eq!(checked, 20_000);
        // around the switch to the exact path: products near 2^53
        for _ in 0..20_000 {
            let s = (next() % 23) as i32;
            let factor = 1.0 + ((next() % 2001) as f64 - 1000.0) * 1e-6;
            let base = 9_007_199_254_740_992.0 * factor / 10f64.powi(s);
            let delta = (next() % 7) as i64 - 3;
            let input = f64::from_bits((base.to_bits() as i64 + delta) as u64);
            check_float_to_decimal::<Decimal128Type>(input, s as i8);
            check_float_to_decimal::<Decimal128Type>(-input, s as i8);
        }
        // integers and values with few binary digits, the common case
        for v in [
            1.0,
            3.0,
            99.99,
            1234.5678,
            295161.0,
            1e15,
            1e17,
            123456789.125,
        ] {
            for scale in 0..=38 {
                check_float_to_decimal::<Decimal128Type>(v, scale);
                check_float_to_decimal::<Decimal128Type>(-v, scale);
            }
        }
    }

    #[test]
    fn test_low_precision_float_array_boundaries() {
        fn check<D: DecimalType>()
        where
            D::Native: DecimalCast + NativeFromI64,
        {
            for precision in [1, D::MAX_PRECISION.min(15)] {
                for scale in [-128, -20, -1, 0, 1, precision as i8] {
                    let mul = 10_f64.powi(scale as i32);
                    let max = (10_i64.pow(precision as u32) - 1) as f64;
                    let samples = [
                        None,
                        Some(0.0),
                        Some(-0.0),
                        Some(max / mul),
                        Some(-max / mul),
                        Some((max + 0.5) / mul),
                        Some((-max - 0.5) / mul),
                        Some((max - 0.5) / mul),
                        Some(f64::MAX),
                        Some(f64::NAN),
                        Some(f64::INFINITY),
                        Some(f64::NEG_INFINITY),
                    ];
                    let array = Float64Array::from_iter(samples.into_iter().cycle().take(140));
                    // Exercise nonzero bitmap offsets and different first failures.
                    for offset in 0..samples.len() {
                        let array = array.slice(offset, 100);
                        let safe = cast_floating_point_to_decimal::<Float64Type, D>(
                            &array,
                            precision,
                            scale,
                            &CastOptions::default(),
                        )
                        .unwrap();
                        let actual = safe.as_primitive::<D>();
                        let expected = array
                            .iter()
                            .map(|v| float_to_decimal_opt::<D>(v?, precision, scale))
                            .collect::<Vec<_>>();
                        assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
                        let strict = cast_floating_point_to_decimal::<Float64Type, D>(
                            &array,
                            precision,
                            scale,
                            &CastOptions {
                                safe: false,
                                ..Default::default()
                            },
                        );
                        let first_error = array
                            .iter()
                            .flatten()
                            .find_map(|v| float_to_decimal::<D>(v, precision, scale).err());
                        match (strict, first_error) {
                            (Err(actual), Some(expected)) => {
                                assert_eq!(actual.to_string(), expected.to_string());
                            }
                            (Ok(actual), None) => assert_eq!(actual.as_ref(), safe.as_ref()),
                            (actual, expected) => panic!("{actual:?} vs {expected:?}"),
                        }
                    }
                }
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    fn test_cast_float_array_to_decimal_matches_scalar() {
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for &(precision, scale) in &[(38u8, 15i8), (38, 20), (20, 3), (12, 2), (38, -2), (15, 10)] {
            for big_share in [0u64, 1, 50] {
                let values: Vec<Option<f64>> = (0..1000)
                    .map(|_| match next() % 100 {
                        0..=4 => None,
                        5 => Some(f64::NAN),
                        6 => Some(f64::INFINITY),
                        r if r < 7 + big_share => Some((next() % 1_000_000_000) as f64 * 1e7),
                        _ => Some(((next() % 2_000_000) as f64 - 1e6) / 64.0),
                    })
                    .collect();
                let array = Float64Array::from(values.clone());
                let to_type = DataType::Decimal128(precision, scale);
                // safe: element by element as float_to_decimal_opt
                let safe = cast_with_options(&array, &to_type, &CastOptions::default()).unwrap();
                let safe = safe.as_primitive::<Decimal128Type>();
                for (i, v) in values.iter().enumerate() {
                    let expected =
                        v.and_then(|v| float_to_decimal_opt::<Decimal128Type>(v, precision, scale));
                    assert_eq!(safe.is_valid(i).then(|| safe.value(i)), expected, "{v:?}");
                }
                // strict: the first failing element's error, or the same values
                let strict_options = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                let strict = cast_with_options(&array, &to_type, &strict_options);
                let first_error = values
                    .iter()
                    .flatten()
                    .find_map(|&v| float_to_decimal::<Decimal128Type>(v, precision, scale).err());
                match (strict, first_error) {
                    (Err(actual), Some(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string())
                    }
                    (Ok(actual), None) => assert_eq!(actual.as_ref(), safe as &dyn Array),
                    (actual, expected) => panic!("{actual:?} vs {expected:?}"),
                }
            }
        }
    }

    #[test]
    fn test_float_array_integer_shortcut_boundaries() {
        fn check<D: DecimalType>(precision: u8, scale: i8)
        where
            D::Native: DecimalCast + NativeFromI64,
        {
            let max = D::MAX_FOR_EACH_PRECISION[precision as usize];
            let int_max = match (usize::try_from(scale).ok(), max.to_i128()) {
                (Some(s), Some(max)) if s < POW10_I128.len() => (max / POW10_I128[s]) as f64,
                _ => 9_007_199_254_740_992.0,
            };
            let mut samples = vec![
                None,
                Some(0.0),
                Some(1.0),
                Some(-1.0),
                Some(1.5),
                Some(9_007_199_254_740_991.0),
                Some(9_007_199_254_740_992.0),
                Some(-9_007_199_254_740_992.0),
                Some(f64::NAN),
                Some(f64::MAX),
            ];
            for v in [int_max - 1.0, int_max, int_max + 1.0, int_max * 10.0] {
                samples.push(Some(v));
                samples.push(Some(-v));
                samples.push(Some(v + 0.5));
            }
            let array = Float64Array::from_iter(samples.iter().cycle().take(150).copied());
            for offset in [0, 3, 64] {
                let array = array.slice(offset, 80);
                let safe = cast_floating_point_to_decimal::<Float64Type, D>(
                    &array,
                    precision,
                    scale,
                    &CastOptions::default(),
                )
                .unwrap();
                let expected = array
                    .iter()
                    .map(|v| float_to_decimal_opt::<D>(v?, precision, scale))
                    .collect::<Vec<_>>();
                assert_eq!(
                    safe.as_primitive::<D>().iter().collect::<Vec<_>>(),
                    expected,
                    "{} precision {precision} scale {scale} offset {offset}",
                    D::PREFIX
                );
                let strict = cast_floating_point_to_decimal::<Float64Type, D>(
                    &array,
                    precision,
                    scale,
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                );
                let first_error = array
                    .iter()
                    .flatten()
                    .find_map(|v| float_to_decimal::<D>(v, precision, scale).err());
                match (strict, first_error) {
                    (Err(actual), Some(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string());
                    }
                    (Ok(actual), None) => assert_eq!(actual.as_ref(), safe.as_ref()),
                    (actual, expected) => panic!("{actual:?} vs {expected:?}"),
                }
            }
        }
        for scale in [-1, 0, 3, 15, 22, 23] {
            for precision in [16, 18] {
                if scale <= precision as i8 {
                    check::<Decimal64Type>(precision, scale);
                    check::<Decimal128Type>(precision, scale);
                    check::<Decimal256Type>(precision, scale);
                }
            }
            check::<Decimal128Type>(38, scale);
            check::<Decimal256Type>(39, scale);
            check::<Decimal256Type>(76, scale);
        }
    }
}
