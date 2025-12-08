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
#[allow(clippy::type_complexity)]
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
    let delta_scale = output_scale - input_scale;

    // O::MAX_FOR_EACH_PRECISION[k] stores 10^k - 1 (e.g., 9, 99, 999, ...).
    // Adding 1 yields exactly 10^k without computing a power at runtime.
    // Using the precomputed table avoids pow(10, k) and its checked/overflow
    // handling, which is faster and simpler for scaling by 10^delta_scale.
    let max = O::MAX_FOR_EACH_PRECISION.get(delta_scale as usize)?;
    let mul = max.add_wrapping(O::Native::ONE);
    let f_fallible = move |x| O::Native::from_decimal(x).and_then(|x| x.mul_checked(mul).ok());

    // if the gain in precision (digits) is greater than the multiplication due to scaling
    // every number will fit into the output type
    // Example: If we are starting with any number of precision 5 [xxxxx],
    // then an increase of scale by 3 will have the following effect on the representation:
    // [xxxxx] -> [xxxxx000], so for the cast to be infallible, the output type
    // needs to provide at least 8 digits precision
    let is_infallible_cast = (input_precision as i8) + delta_scale <= (output_precision as i8);
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
#[allow(clippy::type_complexity)]
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
    let delta_scale = input_scale - output_scale;

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
        let d = x.div_wrapping(div);
        let r = x.mod_wrapping(div);

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
    let is_infallible_cast = (input_precision as i8) - delta_scale < (output_precision as i8);
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
            f_fallible(x).ok_or_else(|| error(x)).and_then(|v| {
                O::validate_decimal_precision(v, output_precision, output_scale).map(|_| v)
            })
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

/// Parses given string to specified decimal native (i128/i256) based on given
/// scale. Returns an `Err` if it cannot parse given string.
pub(crate) fn parse_string_to_decimal_native<T: DecimalType>(
    value_str: &str,
    scale: usize,
) -> Result<T::Native, ArrowError>
where
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let value_str = value_str.trim();
    let parts: Vec<&str> = value_str.split('.').collect();
    if parts.len() > 2 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Invalid decimal format: {value_str:?}"
        )));
    }

    let (negative, first_part) = if parts[0].is_empty() {
        (false, parts[0])
    } else {
        match parts[0].as_bytes()[0] {
            b'-' => (true, &parts[0][1..]),
            b'+' => (false, &parts[0][1..]),
            _ => (false, parts[0]),
        }
    };

    let integers = first_part;
    let decimals = if parts.len() == 2 { parts[1] } else { "" };

    if !integers.is_empty() && !integers.as_bytes()[0].is_ascii_digit() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Invalid decimal format: {value_str:?}"
        )));
    }

    if !decimals.is_empty() && !decimals.as_bytes()[0].is_ascii_digit() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Invalid decimal format: {value_str:?}"
        )));
    }

    // Adjust decimal based on scale
    let mut number_decimals = if decimals.len() > scale {
        let decimal_number = i256::from_string(decimals).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Cannot parse decimal format: {value_str}"))
        })?;

        let div = i256::from_i128(10_i128).pow_checked((decimals.len() - scale) as u32)?;

        let half = div.div_wrapping(i256::from_i128(2));
        let half_neg = half.neg_wrapping();

        let d = decimal_number.div_wrapping(div);
        let r = decimal_number.mod_wrapping(div);

        // Round result
        let adjusted = match decimal_number >= i256::ZERO {
            true if r >= half => d.add_wrapping(i256::ONE),
            false if r <= half_neg => d.sub_wrapping(i256::ONE),
            _ => d,
        };

        let integers = if !integers.is_empty() {
            i256::from_string(integers)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "Cannot parse decimal format: {value_str}"
                    ))
                })
                .map(|v| v.mul_wrapping(i256::from_i128(10_i128).pow_wrapping(scale as u32)))?
        } else {
            i256::ZERO
        };

        format!("{}", integers.add_wrapping(adjusted))
    } else {
        let padding = if scale > decimals.len() { scale } else { 0 };

        let decimals = format!("{decimals:0<padding$}");
        format!("{integers}{decimals}")
    };

    if negative {
        number_decimals.insert(0, '-');
    }

    let value = i256::from_string(number_decimals.as_str()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Cannot convert {} to {}: Overflow",
            value_str,
            T::PREFIX
        ))
    })?;

    T::Native::from_decimal(value).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("Cannot convert {} to {}", value_str, T::PREFIX))
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
    T::Native: DecimalCast + ArrowNativeTypeOp,
    &'a S: StringArrayType<'a>,
{
    if cast_options.safe {
        let iter = from.iter().map(|v| {
            v.and_then(|v| parse_string_to_decimal_native::<T>(v, scale as usize).ok())
                .and_then(|v| T::is_valid_decimal_precision(v, precision).then_some(v))
        });
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe {
            PrimitiveArray::<T>::from_trusted_len_iter(iter)
                .with_precision_and_scale(precision, scale)?
        })
    } else {
        let vec = from
            .iter()
            .map(|v| {
                v.map(|v| {
                    parse_string_to_decimal_native::<T>(v, scale as usize)
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{v}' to value of {} type",
                                T::DATA_TYPE,
                            ))
                        })
                        .and_then(|v| T::validate_decimal_precision(v, precision, scale).map(|_| v))
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe {
            PrimitiveArray::<T>::from_trusted_len_iter(vec.iter())
                .with_precision_and_scale(precision, scale)?
        })
    }
}

pub(crate) fn string_to_decimal_cast<T, Offset: OffsetSizeTrait>(
    from: &GenericStringArray<Offset>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    generic_string_to_decimal_cast::<T, GenericStringArray<Offset>>(
        from,
        precision,
        scale,
        cast_options,
    )
}

pub(crate) fn string_view_to_decimal_cast<T>(
    from: &StringViewArray,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    generic_string_to_decimal_cast::<T, StringViewArray>(from, precision, scale, cast_options)
}

/// Cast Utf8 to decimal
pub(crate) fn cast_string_to_decimal<T, Offset: OffsetSizeTrait>(
    from: &dyn Array,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    if scale < 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot cast string to decimal with negative scale {scale}"
        )));
    }

    if scale > T::MAX_SCALE {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot cast string to decimal greater than maximum scale {}",
            T::MAX_SCALE
        )));
    }

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
    <D as ArrowPrimitiveType>::Native: DecimalCast,
{
    let mul = 10_f64.powi(scale as i32);

    if cast_options.safe {
        array
            .unary_opt::<_, D>(|v| {
                D::Native::from_f64((mul * v.as_()).round())
                    .filter(|v| D::is_valid_decimal_precision(*v, precision))
            })
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    } else {
        array
            .try_unary::<_, D, _>(|v| {
                D::Native::from_f64((mul * v.as_()).round())
                    .ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Cannot cast to {}({}, {}). Overflowing on {:?}",
                            D::PREFIX,
                            precision,
                            scale,
                            v
                        ))
                    })
                    .and_then(|v| D::validate_decimal_precision(v, precision, scale).map(|_| v))
            })?
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    }
}

pub(crate) fn cast_decimal_to_integer<D, T>(
    array: &dyn Array,
    base: D::Native,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: NumCast,
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + ToPrimitive,
{
    let array = array.as_primitive::<D>();

    let div: D::Native = base.pow_checked(scale as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}. The scale {} causes overflow.",
            D::PREFIX,
            scale,
        ))
    })?;

    let mut value_builder = PrimitiveBuilder::<T>::with_capacity(array.len());

    if cast_options.safe {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array
                    .value(i)
                    .div_checked(div)
                    .ok()
                    .and_then(<T::Native as NumCast>::from::<D::Native>);

                value_builder.append_option(v);
            }
        }
    } else {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array.value(i).div_checked(div)?;

                let value = <T::Native as NumCast>::from::<D::Native>(v).ok_or_else(|| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string_to_decimal_native() -> Result<(), ArrowError> {
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("0", 0)?,
            0_i128
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("0", 5)?,
            0_i128
        );

        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123", 0)?,
            123_i128
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123", 5)?,
            12300000_i128
        );

        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123.45", 0)?,
            123_i128
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123.45", 5)?,
            12345000_i128
        );

        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123.4567891", 0)?,
            123_i128
        );
        assert_eq!(
            parse_string_to_decimal_native::<Decimal128Type>("123.4567891", 5)?,
            12345679_i128
        );
        Ok(())
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
    fn test_rescale_decimal_invalid_input_precision_scale_returns_none() {
        let result = rescale_decimal::<Decimal128Type, Decimal128Type>(123_i128, 39, 39, 38, 38);
        assert_eq!(result, None);
    }

    #[test]
    fn test_rescale_decimal_invalid_output_precision_scale_returns_none() {
        let result = rescale_decimal::<Decimal128Type, Decimal128Type>(123_i128, 38, 38, 39, 39);
        assert_eq!(result, None);
    }
}
