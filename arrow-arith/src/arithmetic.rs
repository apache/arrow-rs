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

//! Defines basic arithmetic kernels for `PrimitiveArrays`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use crate::arity::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_buffer::ArrowNativeType;
use arrow_schema::*;
use std::cmp::min;
use std::sync::Arc;

/// Helper function to perform math lambda function on values from two arrays. If either
/// left or right value is null then the output value is also null, so `1 + null` is
/// `null`.
///
/// # Errors
///
/// This function errors if the arrays have different lengths
#[deprecated(note = "Use arrow_arith::arity::binary")]
pub fn math_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<PrimitiveArray<LT>, ArrowError>
where
    LT: ArrowNumericType,
    RT: ArrowNumericType,
    F: Fn(LT::Native, RT::Native) -> LT::Native,
{
    binary(left, right, op)
}

/// Calculates the modulus operation `left % right` on two SIMD inputs.
/// The lower-most bits of `valid_mask` specify which vector lanes are considered as valid.
///
/// # Errors
///
/// This function returns a [`ArrowError::DivideByZero`] if a valid element in `right` is `0`
#[cfg(feature = "simd")]
#[inline]
fn simd_checked_modulus<T: ArrowNumericType>(
    valid_mask: Option<u64>,
    left: T::Simd,
    right: T::Simd,
) -> Result<T::Simd, ArrowError> {
    let zero = T::init(T::Native::ZERO);
    let one = T::init(T::Native::ONE);

    let right_no_invalid_zeros = match valid_mask {
        Some(mask) => {
            let simd_mask = T::mask_from_u64(mask);
            // select `1` for invalid lanes, which will be a no-op during division later
            T::mask_select(simd_mask, right, one)
        }
        None => right,
    };

    let zero_mask = T::eq(right_no_invalid_zeros, zero);

    if T::mask_any(zero_mask) {
        Err(ArrowError::DivideByZero)
    } else {
        Ok(T::bin_op(left, right_no_invalid_zeros, |a, b| a % b))
    }
}

/// Calculates the division operation `left / right` on two SIMD inputs.
/// The lower-most bits of `valid_mask` specify which vector lanes are considered as valid.
///
/// # Errors
///
/// This function returns a [`ArrowError::DivideByZero`] if a valid element in `right` is `0`
#[cfg(feature = "simd")]
#[inline]
fn simd_checked_divide<T: ArrowNumericType>(
    valid_mask: Option<u64>,
    left: T::Simd,
    right: T::Simd,
) -> Result<T::Simd, ArrowError> {
    let zero = T::init(T::Native::ZERO);
    let one = T::init(T::Native::ONE);

    let right_no_invalid_zeros = match valid_mask {
        Some(mask) => {
            let simd_mask = T::mask_from_u64(mask);
            // select `1` for invalid lanes, which will be a no-op during division later
            T::mask_select(simd_mask, right, one)
        }
        None => right,
    };

    let zero_mask = T::eq(right_no_invalid_zeros, zero);

    if T::mask_any(zero_mask) {
        Err(ArrowError::DivideByZero)
    } else {
        Ok(T::bin_op(left, right_no_invalid_zeros, |a, b| a / b))
    }
}

/// Applies `op` on the remainder elements of two input chunks and writes the result into
/// the remainder elements of `result_chunks`.
/// The lower-most bits of `valid_mask` specify which elements are considered as valid.
///
/// # Errors
///
/// This function returns a [`ArrowError::DivideByZero`] if a valid element in `right` is `0`
#[cfg(feature = "simd")]
#[inline]
fn simd_checked_divide_op_remainder<T, F>(
    valid_mask: Option<u64>,
    left_chunks: std::slice::ChunksExact<T::Native>,
    right_chunks: std::slice::ChunksExact<T::Native>,
    result_chunks: std::slice::ChunksExactMut<T::Native>,
    op: F,
) -> Result<(), ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let result_remainder = result_chunks.into_remainder();
    let left_remainder = left_chunks.remainder();
    let right_remainder = right_chunks.remainder();

    result_remainder
        .iter_mut()
        .zip(left_remainder.iter().zip(right_remainder.iter()))
        .enumerate()
        .try_for_each(|(i, (result_scalar, (left_scalar, right_scalar)))| {
            if valid_mask.map(|mask| mask & (1 << i) != 0).unwrap_or(true) {
                if right_scalar.is_zero() {
                    return Err(ArrowError::DivideByZero);
                }
                *result_scalar = op(*left_scalar, *right_scalar);
            } else {
                *result_scalar = T::default_value();
            }
            Ok(())
        })?;

    Ok(())
}

/// Creates a new PrimitiveArray by applying `simd_op` to the `left` and `right` input array.
/// If the length of the arrays is not multiple of the number of vector lanes
/// then the remainder of the array will be calculated using `scalar_op`.
/// Any operation on a `NULL` value will result in a `NULL` value in the output.
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * there is an element where both left and right values are valid and the right value is `0`
#[cfg(feature = "simd")]
fn simd_checked_divide_op<T, SI, SC>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    simd_op: SI,
    scalar_op: SC,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    SI: Fn(Option<u64>, T::Simd, T::Simd) -> Result<T::Simd, ArrowError>,
    SC: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // Create the combined `Bitmap`
    let nulls = arrow_buffer::NullBuffer::union(left.nulls(), right.nulls());

    let lanes = T::lanes();
    let buffer_size = left.len() * std::mem::size_of::<T::Native>();
    let mut result =
        arrow_buffer::MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    match &nulls {
        Some(b) => {
            let valid_chunks = b.inner().bit_chunks();

            // process data in chunks of 64 elements since we also get 64 bits of validity information at a time

            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(64);
            let mut left_chunks = left.values().chunks_exact(64);
            let mut right_chunks = right.values().chunks_exact(64);

            valid_chunks
                .iter()
                .zip((&mut result_chunks).zip((&mut left_chunks).zip(&mut right_chunks)))
                .try_for_each(
                    |(mut mask, (result_slice, (left_slice, right_slice)))| {
                        // split chunks further into slices corresponding to the vector length
                        // the compiler is able to unroll this inner loop and remove bounds checks
                        // since the outer chunk size (64) is always a multiple of the number of lanes
                        result_slice
                            .chunks_exact_mut(lanes)
                            .zip(left_slice.chunks_exact(lanes).zip(right_slice.chunks_exact(lanes)))
                            .try_for_each(|(result_slice, (left_slice, right_slice))| -> Result<(), ArrowError> {
                                let simd_left = T::load(left_slice);
                                let simd_right = T::load(right_slice);

                                let simd_result = simd_op(Some(mask), simd_left, simd_right)?;

                                T::write(simd_result, result_slice);

                                // skip the shift and avoid overflow for u8 type, which uses 64 lanes.
                                mask >>= T::lanes() % 64;

                                Ok(())
                            })
                    },
                )?;

            let valid_remainder = valid_chunks.remainder_bits();

            simd_checked_divide_op_remainder::<T, _>(
                Some(valid_remainder),
                left_chunks,
                right_chunks,
                result_chunks,
                scalar_op,
            )?;
        }
        None => {
            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(lanes);
            let mut left_chunks = left.values().chunks_exact(lanes);
            let mut right_chunks = right.values().chunks_exact(lanes);

            (&mut result_chunks)
                .zip((&mut left_chunks).zip(&mut right_chunks))
                .try_for_each(
                |(result_slice, (left_slice, right_slice))| -> Result<(), ArrowError> {
                    let simd_left = T::load(left_slice);
                    let simd_right = T::load(right_slice);

                    let simd_result = simd_op(None, simd_left, simd_right)?;

                    T::write(simd_result, result_slice);

                    Ok(())
                },
            )?;

            simd_checked_divide_op_remainder::<T, _>(
                None,
                left_chunks,
                right_chunks,
                result_chunks,
                scalar_op,
            )?;
        }
    }

    Ok(PrimitiveArray::new(result.into(), nulls))
}

fn math_safe_divide_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<ArrayRef, ArrowError>
where
    LT: ArrowNumericType,
    RT: ArrowNumericType,
    F: Fn(LT::Native, RT::Native) -> Option<LT::Native>,
{
    let array: PrimitiveArray<LT> = binary_opt::<_, _, _, LT>(left, right, op)?;
    Ok(Arc::new(array) as ArrayRef)
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::add_wrapping")]
pub fn add<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    binary(left, right, |a, b| a.add_wrapping(b))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add` instead.
#[deprecated(note = "Use arrow_arith::numeric::add")]
pub fn add_checked<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_binary(left, right, |a, b| a.add_checked(b))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_dyn_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::add_wrapping")]
pub fn add_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    crate::numeric::add_wrapping(&left, &right)
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::add")]
pub fn add_dyn_checked(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    crate::numeric::add(&left, &right)
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_scalar_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::add_wrapping")]
pub fn add_scalar<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    Ok(unary(array, |value| value.add_wrapping(scalar)))
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add_scalar` instead.
#[deprecated(note = "Use arrow_arith::numeric::add")]
pub fn add_scalar_checked<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_unary(array, |value| value.add_checked(scalar))
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_scalar_checked_dyn` instead.
///
/// This returns an `Err` when the input array is not supported for adding operation.
#[deprecated(note = "Use arrow_arith::numeric::add_wrapping")]
pub fn add_scalar_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    unary_dyn::<_, T>(array, |value| value.add_wrapping(scalar))
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add_scalar_dyn` instead.
///
/// As this kernel has the branching costs and also prevents LLVM from vectorising it correctly,
/// it is usually much slower than non-checking variant.
#[deprecated(note = "Use arrow_arith::numeric::add")]
pub fn add_scalar_checked_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    try_unary_dyn::<_, T>(array, |value| value.add_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub_wrapping")]
pub fn subtract<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    binary(left, right, |a, b| a.sub_wrapping(b))
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub")]
pub fn subtract_checked<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_binary(left, right, |a, b| a.sub_checked(b))
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_dyn_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub_wrapping")]
pub fn subtract_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    crate::numeric::sub_wrapping(&left, &right)
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub")]
pub fn subtract_dyn_checked(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    crate::numeric::sub(&left, &right)
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_scalar_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub_wrapping")]
pub fn subtract_scalar<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    Ok(unary(array, |value| value.sub_wrapping(scalar)))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_scalar` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub")]
pub fn subtract_scalar_checked<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_unary(array, |value| value.sub_checked(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_scalar_checked_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub_wrapping")]
pub fn subtract_scalar_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    unary_dyn::<_, T>(array, |value| value.sub_wrapping(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_scalar_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::sub")]
pub fn subtract_scalar_checked_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    try_unary_dyn::<_, T>(array, |value| value.sub_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `-` operation on an array. If value is null then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `negate_checked` instead.
pub fn negate<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    Ok(unary(array, |x| x.neg_wrapping()))
}

/// Perform `-` operation on an array. If value is null then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `negate` instead.
pub fn negate_checked<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_unary(array, |value| value.neg_checked())
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_check` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul_wrapping")]
pub fn multiply<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    binary(left, right, |a, b| a.mul_wrapping(b))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul")]
pub fn multiply_checked<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_binary(left, right, |a, b| a.mul_checked(b))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_dyn_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul_wrapping")]
pub fn multiply_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    crate::numeric::mul_wrapping(&left, &right)
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul")]
pub fn multiply_dyn_checked(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    crate::numeric::mul(&left, &right)
}

/// Returns the precision and scale of the result of a multiplication of two decimal types,
/// and the divisor for fixed point multiplication.
fn get_fixed_point_info(
    left: (u8, i8),
    right: (u8, i8),
    required_scale: i8,
) -> Result<(u8, i8, i256), ArrowError> {
    let product_scale = left.1 + right.1;
    let precision = min(left.0 + right.0 + 1, DECIMAL128_MAX_PRECISION);

    if required_scale > product_scale {
        return Err(ArrowError::ComputeError(format!(
            "Required scale {} is greater than product scale {}",
            required_scale, product_scale
        )));
    }

    let divisor =
        i256::from_i128(10).pow_wrapping((product_scale - required_scale) as u32);

    Ok((precision, product_scale, divisor))
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply_dyn` or `multiply_dyn_checked` instead.
pub fn multiply_fixed_point_dyn(
    left: &dyn Array,
    right: &dyn Array,
    required_scale: i8,
) -> Result<ArrayRef, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
            let left = left.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let right = right.as_any().downcast_ref::<Decimal128Array>().unwrap();

            multiply_fixed_point(left, right, required_scale)
                .map(|a| Arc::new(a) as ArrayRef)
        }
        (_, _) => Err(ArrowError::CastError(format!(
            "Unsupported data type {}, {}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply` or `multiply_checked` instead.
pub fn multiply_fixed_point_checked(
    left: &PrimitiveArray<Decimal128Type>,
    right: &PrimitiveArray<Decimal128Type>,
    required_scale: i8,
) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let (precision, product_scale, divisor) = get_fixed_point_info(
        (left.precision(), left.scale()),
        (right.precision(), right.scale()),
        required_scale,
    )?;

    if required_scale == product_scale {
        return try_binary::<_, _, _, Decimal128Type>(left, right, |a, b| {
            a.mul_checked(b)
        })?
        .with_precision_and_scale(precision, required_scale);
    }

    try_binary::<_, _, _, Decimal128Type>(left, right, |a, b| {
        let a = i256::from_i128(a);
        let b = i256::from_i128(b);

        let mut mul = a.wrapping_mul(b);
        mul = divide_and_round::<Decimal256Type>(mul, divisor);
        mul.to_i128().ok_or_else(|| {
            ArrowError::ComputeError(format!("Overflow happened on: {:?} * {:?}", a, b))
        })
    })
    .and_then(|a| a.with_precision_and_scale(precision, required_scale))
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_fixed_point_checked` instead.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply` or `multiply_checked` instead.
pub fn multiply_fixed_point(
    left: &PrimitiveArray<Decimal128Type>,
    right: &PrimitiveArray<Decimal128Type>,
    required_scale: i8,
) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let (precision, product_scale, divisor) = get_fixed_point_info(
        (left.precision(), left.scale()),
        (right.precision(), right.scale()),
        required_scale,
    )?;

    if required_scale == product_scale {
        return binary(left, right, |a, b| a.mul_wrapping(b))?
            .with_precision_and_scale(precision, required_scale);
    }

    binary::<_, _, _, Decimal128Type>(left, right, |a, b| {
        let a = i256::from_i128(a);
        let b = i256::from_i128(b);

        let mut mul = a.wrapping_mul(b);
        mul = divide_and_round::<Decimal256Type>(mul, divisor);
        mul.as_i128()
    })
    .and_then(|a| a.with_precision_and_scale(precision, required_scale))
}

/// Divide a decimal native value by given divisor and round the result.
fn divide_and_round<I>(input: I::Native, div: I::Native) -> I::Native
where
    I: DecimalType,
    I::Native: ArrowNativeTypeOp,
{
    let d = input.div_wrapping(div);
    let r = input.mod_wrapping(div);

    let half = div.div_wrapping(I::Native::from_usize(2).unwrap());
    let half_neg = half.neg_wrapping();

    // Round result
    match input >= I::Native::ZERO {
        true if r >= half => d.add_wrapping(I::Native::ONE),
        false if r <= half_neg => d.sub_wrapping(I::Native::ONE),
        _ => d,
    }
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_scalar_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul_wrapping")]
pub fn multiply_scalar<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    Ok(unary(array, |value| value.mul_wrapping(scalar)))
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_scalar` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul")]
pub fn multiply_scalar_checked<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    try_unary(array, |value| value.mul_checked(scalar))
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_scalar_checked_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul_wrapping")]
pub fn multiply_scalar_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    unary_dyn::<_, T>(array, |value| value.mul_wrapping(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_scalar_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::mul")]
pub fn multiply_scalar_checked_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef, ArrowError> {
    try_unary_dyn::<_, T>(array, |value| value.mul_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `left % right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
#[deprecated(note = "Use arrow_arith::numeric::rem")]
pub fn modulus<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    #[cfg(feature = "simd")]
    return simd_checked_divide_op(&left, &right, simd_checked_modulus::<T>, |a, b| {
        a.mod_wrapping(b)
    });
    #[cfg(not(feature = "simd"))]
    return try_binary(left, right, |a, b| {
        if b.is_zero() {
            Err(ArrowError::DivideByZero)
        } else {
            Ok(a.mod_wrapping(b))
        }
    });
}

/// Perform `left % right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
#[deprecated(note = "Use arrow_arith::numeric::rem")]
pub fn modulus_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    crate::numeric::rem(&left, &right)
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
///
/// When `simd` feature is not enabled. This detects overflow and returns an `Err` for that.
/// For an non-overflow-checking variant, use `divide` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_checked<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    #[cfg(feature = "simd")]
    return simd_checked_divide_op(&left, &right, simd_checked_divide::<T>, |a, b| {
        a.div_wrapping(b)
    });
    #[cfg(not(feature = "simd"))]
    return try_binary(left, right, |a, b| a.div_checked(b));
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// If any right hand value is zero, the operation value will be replaced with null in the
/// result.
///
/// Unlike [`divide`] or [`divide_checked`], division by zero will yield a null value in the
/// result instead of returning an `Err`.
///
/// For floating point types overflow will saturate at INF or -INF
/// preserving the expected sign value.
///
/// For integer types overflow will wrap around.
///
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_opt<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    binary_opt(left, right, |a, b| {
        if b.is_zero() {
            None
        } else {
            Some(a.div_wrapping(b))
        }
    })
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `divide_dyn_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    fn divide_op<T: ArrowPrimitiveType>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError> {
        try_binary(left, right, |a, b| {
            if b.is_zero() {
                Err(ArrowError::DivideByZero)
            } else {
                Ok(a.div_wrapping(b))
            }
        })
    }

    downcast_primitive_array!(
        (left, right) => divide_op(left, right).map(|a| Arc::new(a) as ArrayRef),
        _ => Err(ArrowError::CastError(format!(
            "Unsupported data type {}, {}",
            left.data_type(), right.data_type()
        )))
    )
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `divide_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_dyn_checked(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    crate::numeric::div(&left, &right)
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// If any right hand value is zero, the operation value will be replaced with null in the
/// result.
///
/// Unlike `divide_dyn` or `divide_dyn_checked`, division by zero will get a null value instead
/// returning an `Err`, this also doesn't check overflowing, overflowing will just wrap
/// the result around.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_dyn_opt(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    downcast_primitive_array!(
        (left, right) => {
            math_safe_divide_op(left, right, |a, b| {
                if b.is_zero() {
                    None
                } else {
                    Some(a.div_wrapping(b))
                }
            })
        }
        _ => Err(ArrowError::CastError(format!(
            "Unsupported data type {}, {}",
            left.data_type(), right.data_type()
        )))
    )
}

/// Perform `left / right` operation on two arrays without checking for
/// division by zero or overflow.
///
/// For floating point types, overflow and division by zero follows normal floating point rules
///
/// For integer types overflow will wrap around. Division by zero will currently panic, although
/// this may be subject to change see <https://github.com/apache/arrow-rs/issues/2647>
///
/// If either left or right value is null then the result is also null.
///
/// For an overflow-checking variant, use `divide_checked` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide<T: ArrowNumericType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError> {
    // TODO: This is incorrect as div_wrapping has side-effects for integer types
    // and so may panic on null values (#2647)
    binary(left, right, |a, b| a.div_wrapping(b))
}

/// Modulus every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
#[deprecated(note = "Use arrow_arith::numeric::rem")]
pub fn modulus_scalar<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    modulo: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    if modulo.is_zero() {
        return Err(ArrowError::DivideByZero);
    }

    Ok(unary(array, |a| a.mod_wrapping(modulo)))
}

/// Modulus every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
#[deprecated(note = "Use arrow_arith::numeric::rem")]
pub fn modulus_scalar_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    modulo: T::Native,
) -> Result<ArrayRef, ArrowError> {
    if modulo.is_zero() {
        return Err(ArrowError::DivideByZero);
    }
    unary_dyn::<_, T>(array, |value| value.mod_wrapping(modulo))
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_scalar<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
    divisor: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError> {
    if divisor.is_zero() {
        return Err(ArrowError::DivideByZero);
    }
    Ok(unary(array, |a| a.div_wrapping(divisor)))
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`. The given array must be a `PrimitiveArray` of the type
/// same as the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `divide_scalar_checked_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_scalar_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    divisor: T::Native,
) -> Result<ArrayRef, ArrowError> {
    if divisor.is_zero() {
        return Err(ArrowError::DivideByZero);
    }
    unary_dyn::<_, T>(array, |value| value.div_wrapping(divisor))
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`. The given array must be a `PrimitiveArray` of the type
/// same as the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `divide_scalar_dyn` instead.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_scalar_checked_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    divisor: T::Native,
) -> Result<ArrayRef, ArrowError> {
    if divisor.is_zero() {
        return Err(ArrowError::DivideByZero);
    }

    try_unary_dyn::<_, T>(array, |value| value.div_checked(divisor))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type
/// same as the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// If any right hand value is zero, the operation value will be replaced with null in the
/// result.
///
/// Unlike `divide_scalar_dyn` or `divide_scalar_checked_dyn`, division by zero will get a
/// null value instead returning an `Err`, this also doesn't check overflowing, overflowing
/// will just wrap the result around.
#[deprecated(note = "Use arrow_arith::numeric::div")]
pub fn divide_scalar_opt_dyn<T: ArrowNumericType>(
    array: &dyn Array,
    divisor: T::Native,
) -> Result<ArrayRef, ArrowError> {
    if divisor.is_zero() {
        match array.data_type() {
            DataType::Dictionary(_, value_type) => {
                return Ok(new_null_array(value_type.as_ref(), array.len()))
            }
            _ => return Ok(new_null_array(array.data_type(), array.len())),
        }
    }

    unary_dyn::<_, T>(array, |value| value.div_wrapping(divisor))
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use arrow_array::builder::{
        BooleanBufferBuilder, BufferBuilder, PrimitiveDictionaryBuilder,
    };
    use arrow_array::cast::AsArray;
    use arrow_array::temporal_conversions::SECONDS_IN_DAY;
    use arrow_buffer::buffer::NullBuffer;
    use arrow_buffer::i256;
    use arrow_data::ArrayDataBuilder;
    use chrono::NaiveDate;
    use half::f16;

    #[test]
    fn test_primitive_array_add() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 8]);
        let c = add(&a, &b).unwrap();
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert_eq!(17, c.value(3));
        assert_eq!(17, c.value(4));
    }

    #[test]
    fn test_date32_month_add() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b =
            IntervalYearMonthArray::from(vec![IntervalYearMonthType::make_value(1, 2)]);
        let c = add_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2001, 3, 1).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2001, 3, 1).unwrap())
        );
    }

    #[test]
    fn test_date32_day_time_add() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b = IntervalDayTimeArray::from(vec![IntervalDayTimeType::make_value(1, 2)]);
        let c = add_dyn(&a, &b).unwrap();
        assert_eq!(
            c.as_primitive::<Date32Type>().value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 1, 2).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        assert_eq!(
            c.as_primitive::<Date32Type>().value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 1, 2).unwrap())
        );
    }

    #[test]
    fn test_date32_month_day_nano_add() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
                1, 2, 3,
            )]);
        let c = add_dyn(&a, &b).unwrap();
        assert_eq!(
            c.as_primitive::<Date32Type>().value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 2, 3).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        assert_eq!(
            c.as_primitive::<Date32Type>().value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 2, 3).unwrap())
        );
    }

    #[test]
    fn test_date64_month_add() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b =
            IntervalYearMonthArray::from(vec![IntervalYearMonthType::make_value(1, 2)]);
        let c = add_dyn(&a, &b).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2001, 3, 1).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2001, 3, 1).unwrap())
        );
    }

    #[test]
    fn test_date64_day_time_add() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b = IntervalDayTimeArray::from(vec![IntervalDayTimeType::make_value(1, 2)]);
        let c = add_dyn(&a, &b).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 1, 2).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 1, 2).unwrap())
        );
    }

    #[test]
    fn test_date64_month_day_nano_add() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
                1, 2, 3,
            )]);
        let c = add_dyn(&a, &b).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 2, 3).unwrap())
        );

        let c = add_dyn(&b, &a).unwrap();
        assert_eq!(
            c.as_primitive::<Date64Type>().value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2000, 2, 3).unwrap())
        );
    }

    #[test]
    fn test_primitive_array_add_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), Some(8), Some(9)]);
        let b = Int32Array::from(vec![Some(6), Some(7), Some(8), None, Some(8)]);
        let c = add_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(17, c.value(4));
    }

    #[test]
    fn test_primitive_array_add_scalar_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), None, Some(9)]);
        let b = 1_i32;
        let c = add_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(6, c.value(0));
        assert_eq!(7, c.value(1));
        assert_eq!(8, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(10, c.value(4));

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append_null();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();
        let b = -1_i32;

        let c = add_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let values = c
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(4, values.value(c.key(0).unwrap()));
        assert!(c.is_null(1));
        assert_eq!(6, values.value(c.key(2).unwrap()));
        assert_eq!(7, values.value(c.key(3).unwrap()));
        assert_eq!(8, values.value(c.key(4).unwrap()));
    }

    #[test]
    fn test_primitive_array_subtract_dyn() {
        let a = Int32Array::from(vec![Some(51), Some(6), Some(15), Some(8), Some(9)]);
        let b = Int32Array::from(vec![Some(6), Some(7), Some(8), None, Some(8)]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(45, c.value(0));
        assert_eq!(-1, c.value(1));
        assert_eq!(7, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(1, c.value(4));
    }

    #[test]
    fn test_date32_month_subtract() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 7, 1).unwrap(),
        )]);
        let b =
            IntervalYearMonthArray::from(vec![IntervalYearMonthType::make_value(6, 3)]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(1994, 4, 1).unwrap())
        );
    }

    #[test]
    fn test_date32_day_time_subtract() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 3, 29).unwrap(),
        )]);
        let b =
            IntervalDayTimeArray::from(vec![IntervalDayTimeType::make_value(1, 86500)]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2023, 3, 27).unwrap())
        );
    }

    #[test]
    fn test_date32_month_day_nano_subtract() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 3, 15).unwrap(),
        )]);
        let b =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
                1, 2, 0,
            )]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2023, 2, 13).unwrap())
        );
    }

    #[test]
    fn test_date64_month_subtract() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 7, 1).unwrap(),
        )]);
        let b =
            IntervalYearMonthArray::from(vec![IntervalYearMonthType::make_value(6, 3)]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(1994, 4, 1).unwrap())
        );
    }

    #[test]
    fn test_date64_day_time_subtract() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 3, 29).unwrap(),
        )]);
        let b =
            IntervalDayTimeArray::from(vec![IntervalDayTimeType::make_value(1, 86500)]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2023, 3, 27).unwrap())
        );
    }

    #[test]
    fn test_date64_month_day_nano_subtract() {
        let a = Date64Array::from(vec![Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 3, 15).unwrap(),
        )]);
        let b =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
                1, 2, 0,
            )]);
        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
            Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2023, 2, 13).unwrap())
        );
    }

    #[test]
    fn test_primitive_array_subtract_scalar_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), None, Some(9)]);
        let b = 1_i32;
        let c = subtract_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(4, c.value(0));
        assert_eq!(5, c.value(1));
        assert_eq!(6, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(8, c.value(4));

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append_null();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();
        let b = -1_i32;

        let c = subtract_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let values = c
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(6, values.value(c.key(0).unwrap()));
        assert!(c.is_null(1));
        assert_eq!(8, values.value(c.key(2).unwrap()));
        assert_eq!(9, values.value(c.key(3).unwrap()));
        assert_eq!(10, values.value(c.key(4).unwrap()));
    }

    #[test]
    fn test_primitive_array_multiply_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), Some(8), Some(9)]);
        let b = Int32Array::from(vec![Some(6), Some(7), Some(8), None, Some(8)]);
        let c = multiply_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(30, c.value(0));
        assert_eq!(42, c.value(1));
        assert_eq!(56, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(72, c.value(4));
    }

    #[test]
    fn test_primitive_array_divide_dyn() {
        let a = Int32Array::from(vec![Some(15), Some(6), Some(1), Some(8), Some(9)]);
        let b = Int32Array::from(vec![Some(5), Some(3), Some(1), None, Some(3)]);
        let c = divide_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, c.value(0));
        assert_eq!(2, c.value(1));
        assert_eq!(1, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(3, c.value(4));
    }

    #[test]
    fn test_primitive_array_multiply_scalar_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), None, Some(9)]);
        let b = 2_i32;
        let c = multiply_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(10, c.value(0));
        assert_eq!(12, c.value(1));
        assert_eq!(14, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(18, c.value(4));

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append_null();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();
        let b = -1_i32;

        let c = multiply_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let values = c
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(-5, values.value(c.key(0).unwrap()));
        assert!(c.is_null(1));
        assert_eq!(-7, values.value(c.key(2).unwrap()));
        assert_eq!(-8, values.value(c.key(3).unwrap()));
        assert_eq!(-9, values.value(c.key(4).unwrap()));
    }

    #[test]
    fn test_primitive_array_add_sliced() {
        let a = Int32Array::from(vec![0, 0, 0, 5, 6, 7, 8, 9, 0]);
        let b = Int32Array::from(vec![0, 0, 0, 6, 7, 8, 9, 8, 0]);
        let a = a.slice(3, 5);
        let b = b.slice(3, 5);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(5, a.value(0));
        assert_eq!(6, b.value(0));

        let c = add(a, b).unwrap();
        assert_eq!(5, c.len());
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert_eq!(17, c.value(3));
        assert_eq!(17, c.value(4));
    }

    #[test]
    fn test_primitive_array_add_mismatched_length() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8]);
        let e = add(&a, &b).expect_err("should have failed due to different lengths");
        assert_eq!(
            "ComputeError(\"Cannot perform binary operation on arrays of different length\")",
            format!("{e:?}")
        );
    }

    #[test]
    fn test_primitive_array_add_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = add_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![18, 17, 12, 11, 4]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_add_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let actual = add_scalar(&a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(12), Some(11), None]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_primitive_array_subtract() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![5, 4, 3, 2, 1]);
        let c = subtract(&a, &b).unwrap();
        assert_eq!(-4, c.value(0));
        assert_eq!(-2, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(2, c.value(3));
        assert_eq!(4, c.value(4));
    }

    #[test]
    fn test_primitive_array_subtract_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = subtract_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![12, 11, 6, 5, -2]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_subtract_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let actual = subtract_scalar(&a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(6), Some(5), None]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_primitive_array_multiply() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 8]);
        let c = multiply(&a, &b).unwrap();
        assert_eq!(30, c.value(0));
        assert_eq!(42, c.value(1));
        assert_eq!(56, c.value(2));
        assert_eq!(72, c.value(3));
        assert_eq!(72, c.value(4));
    }

    #[test]
    fn test_primitive_array_multiply_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = multiply_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![45, 42, 27, 24, 3]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_multiply_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let actual = multiply_scalar(&a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(27), Some(24), None]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_primitive_array_divide() {
        let a = Int32Array::from(vec![15, 15, 8, 1, 9]);
        let b = Int32Array::from(vec![5, 6, 8, 9, 1]);
        let c = divide(&a, &b).unwrap();
        assert_eq!(3, c.value(0));
        assert_eq!(2, c.value(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_int_array_modulus() {
        let a = Int32Array::from(vec![15, 15, 8, 1, 9]);
        let b = Int32Array::from(vec![5, 6, 8, 9, 1]);
        let c = modulus(&a, &b).unwrap();
        assert_eq!(0, c.value(0));
        assert_eq!(3, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert_eq!(0, c.value(4));

        let c = modulus_dyn(&a, &b).unwrap();
        let c = c.as_primitive::<Int32Type>();
        assert_eq!(0, c.value(0));
        assert_eq!(3, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert_eq!(0, c.value(4));
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: DivideByZero"
    )]
    fn test_int_array_modulus_divide_by_zero() {
        let a = Int32Array::from(vec![1]);
        let b = Int32Array::from(vec![0]);
        modulus(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: DivideByZero"
    )]
    fn test_int_array_modulus_dyn_divide_by_zero() {
        let a = Int32Array::from(vec![1]);
        let b = Int32Array::from(vec![0]);
        modulus_dyn(&a, &b).unwrap();
    }

    #[test]
    fn test_int_array_modulus_overflow_wrapping() {
        let a = Int32Array::from(vec![i32::MIN]);
        let b = Int32Array::from(vec![-1]);
        let result = modulus(&a, &b).unwrap();
        assert_eq!(0, result.value(0))
    }

    #[test]
    fn test_primitive_array_divide_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = divide_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![5, 4, 3, 2, 0]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_divide_scalar_dyn() {
        let a = Int32Array::from(vec![Some(5), Some(6), Some(7), None, Some(9)]);
        let b = 2_i32;
        let c = divide_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, c.value(0));
        assert_eq!(3, c.value(1));
        assert_eq!(3, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(4, c.value(4));

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append_null();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();
        let b = -2_i32;

        let c = divide_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let values = c
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(-2, values.value(c.key(0).unwrap()));
        assert!(c.is_null(1));
        assert_eq!(-3, values.value(c.key(2).unwrap()));
        assert_eq!(-4, values.value(c.key(3).unwrap()));
        assert_eq!(-4, values.value(c.key(4).unwrap()));

        let e = divide_scalar_dyn::<Int32Type>(&a, 0_i32)
            .expect_err("should have failed due to divide by zero");
        assert_eq!("DivideByZero", format!("{e:?}"));
    }

    #[test]
    fn test_primitive_array_divide_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let actual = divide_scalar(&a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(3), Some(2), None]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_int_array_modulus_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = modulus_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![0, 2, 0, 2, 1]);
        assert_eq!(c, expected);

        let c = modulus_scalar_dyn::<Int32Type>(&a, b).unwrap();
        let c = c.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![0, 2, 0, 2, 1]);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_int_array_modulus_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let actual = modulus_scalar(&a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(0), Some(2), None]);
        assert_eq!(actual, expected);

        let actual = modulus_scalar_dyn::<Int32Type>(&a, 3).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![None, Some(0), Some(2), None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: DivideByZero"
    )]
    fn test_int_array_modulus_scalar_divide_by_zero() {
        let a = Int32Array::from(vec![1]);
        modulus_scalar(&a, 0).unwrap();
    }

    #[test]
    fn test_int_array_modulus_scalar_overflow_wrapping() {
        let a = Int32Array::from(vec![i32::MIN]);
        let result = modulus_scalar(&a, -1).unwrap();
        assert_eq!(0, result.value(0));

        let result = modulus_scalar_dyn::<Int32Type>(&a, -1).unwrap();
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(0, result.value(0));
    }

    #[test]
    fn test_primitive_array_divide_sliced() {
        let a = Int32Array::from(vec![0, 0, 0, 15, 15, 8, 1, 9, 0]);
        let b = Int32Array::from(vec![0, 0, 0, 5, 6, 8, 9, 1, 0]);
        let a = a.slice(3, 5);
        let b = b.slice(3, 5);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = divide(a, b).unwrap();
        assert_eq!(5, c.len());
        assert_eq!(3, c.value(0));
        assert_eq!(2, c.value(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_primitive_array_modulus_sliced() {
        let a = Int32Array::from(vec![0, 0, 0, 15, 15, 8, 1, 9, 0]);
        let b = Int32Array::from(vec![0, 0, 0, 5, 6, 8, 9, 1, 0]);
        let a = a.slice(3, 5);
        let b = b.slice(3, 5);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = modulus(a, b).unwrap();
        assert_eq!(5, c.len());
        assert_eq!(0, c.value(0));
        assert_eq!(3, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert_eq!(0, c.value(4));
    }

    #[test]
    fn test_primitive_array_divide_with_nulls() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9), None]);
        let b = Int32Array::from(vec![Some(5), Some(6), Some(8), Some(9), None, None]);
        let c = divide_checked(&a, &b).unwrap();
        assert_eq!(3, c.value(0));
        assert!(c.is_null(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert!(c.is_null(4));
        assert!(c.is_null(5));
    }

    #[test]
    fn test_primitive_array_modulus_with_nulls() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9), None]);
        let b = Int32Array::from(vec![Some(5), Some(6), Some(8), Some(9), None, None]);
        let c = modulus(&a, &b).unwrap();
        assert_eq!(0, c.value(0));
        assert!(c.is_null(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert!(c.is_null(4));
        assert!(c.is_null(5));
    }

    #[test]
    fn test_primitive_array_divide_scalar_with_nulls() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9), None]);
        let b = 3;
        let c = divide_scalar(&a, b).unwrap();
        let expected =
            Int32Array::from(vec![Some(5), None, Some(2), Some(0), Some(3), None]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_modulus_scalar_with_nulls() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9), None]);
        let b = 3;
        let c = modulus_scalar(&a, b).unwrap();
        let expected =
            Int32Array::from(vec![Some(0), None, Some(2), Some(1), Some(0), None]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_divide_with_nulls_sliced() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(15),
            None,
            Some(8),
            Some(1),
            Some(9),
            None,
            None,
        ]);
        let b = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(5),
            Some(6),
            Some(8),
            Some(9),
            None,
            None,
            None,
        ]);

        let a = a.slice(8, 6);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let b = b.slice(8, 6);
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = divide_checked(a, b).unwrap();
        assert_eq!(6, c.len());
        assert_eq!(3, c.value(0));
        assert!(c.is_null(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert!(c.is_null(4));
        assert!(c.is_null(5));
    }

    #[test]
    fn test_primitive_array_modulus_with_nulls_sliced() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(15),
            None,
            Some(8),
            Some(1),
            Some(9),
            None,
            None,
        ]);
        let b = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(5),
            Some(6),
            Some(8),
            Some(9),
            None,
            None,
            None,
        ]);

        let a = a.slice(8, 6);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let b = b.slice(8, 6);
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = modulus(a, b).unwrap();
        assert_eq!(6, c.len());
        assert_eq!(0, c.value(0));
        assert!(c.is_null(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert!(c.is_null(4));
        assert!(c.is_null(5));
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_int_array_divide_by_zero_with_checked() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        divide_checked(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_f32_array_divide_by_zero_with_checked() {
        let a = Float32Array::from(vec![15.0]);
        let b = Float32Array::from(vec![0.0]);
        divide_checked(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "attempt to divide by zero")]
    fn test_int_array_divide_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        divide(&a, &b).unwrap();
    }

    #[test]
    fn test_f32_array_divide_by_zero() {
        let a = Float32Array::from(vec![1.5, 0.0, -1.5]);
        let b = Float32Array::from(vec![0.0, 0.0, 0.0]);
        let result = divide(&a, &b).unwrap();
        assert_eq!(result.value(0), f32::INFINITY);
        assert!(result.value(1).is_nan());
        assert_eq!(result.value(2), f32::NEG_INFINITY);
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_int_array_divide_dyn_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        divide_dyn(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_f32_array_divide_dyn_by_zero() {
        let a = Float32Array::from(vec![1.5]);
        let b = Float32Array::from(vec![0.0]);
        divide_dyn(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_i32_array_modulus_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        modulus(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_i32_array_modulus_dyn_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        modulus_dyn(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_f32_array_modulus_by_zero() {
        let a = Float32Array::from(vec![1.5]);
        let b = Float32Array::from(vec![0.0]);
        modulus(&a, &b).unwrap();
    }

    #[test]
    fn test_f32_array_modulus_dyn_by_zero() {
        let a = Float32Array::from(vec![1.5]);
        let b = Float32Array::from(vec![0.0]);
        let result = modulus_dyn(&a, &b).unwrap();
        assert!(result.as_primitive::<Float32Type>().value(0).is_nan());
    }

    #[test]
    fn test_f64_array_divide() {
        let a = Float64Array::from(vec![15.0, 15.0, 8.0]);
        let b = Float64Array::from(vec![5.0, 6.0, 8.0]);
        let c = divide(&a, &b).unwrap();
        assert_eq!(3.0, c.value(0));
        assert_eq!(2.5, c.value(1));
        assert_eq!(1.0, c.value(2));
    }

    #[test]
    fn test_primitive_array_add_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None]);
        let b = Int32Array::from(vec![None, None, Some(6), Some(7)]);
        let c = add(&a, &b).unwrap();
        assert!(c.is_null(0));
        assert!(c.is_null(1));
        assert!(!c.is_null(2));
        assert!(c.is_null(3));
        assert_eq!(13, c.value(2));
    }

    #[test]
    fn test_primitive_array_negate() {
        let a: Int64Array = (0..100).map(Some).collect();
        let actual = negate(&a).unwrap();
        let expected: Int64Array = (0..100).map(|i| Some(-i)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_negate_checked_overflow() {
        let a = Int32Array::from(vec![i32::MIN]);
        let actual = negate(&a).unwrap();
        let expected = Int32Array::from(vec![i32::MIN]);
        assert_eq!(expected, actual);

        let err = negate_checked(&a);
        err.expect_err("negate_checked should detect overflow");
    }

    #[test]
    fn test_arithmetic_kernel_should_not_rely_on_padding() {
        let a: UInt8Array = (0..128_u8).map(Some).collect();
        let a = a.slice(63, 65);
        let a = a.as_any().downcast_ref::<UInt8Array>().unwrap();

        let b: UInt8Array = (0..128_u8).map(Some).collect();
        let b = b.slice(63, 65);
        let b = b.as_any().downcast_ref::<UInt8Array>().unwrap();

        let actual = add(a, b).unwrap();
        let actual: Vec<Option<u8>> = actual.iter().collect();
        let expected: Vec<Option<u8>> =
            (63..63_u8 + 65_u8).map(|i| Some(i + i)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_add_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);
        let b = Int32Array::from(vec![1, 1]);

        let wrapped = add(&a, &b);
        let expected = Int32Array::from(vec![-2147483648, -2147483647]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = add_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_subtract_wrapping_overflow() {
        let a = Int32Array::from(vec![-2]);
        let b = Int32Array::from(vec![i32::MAX]);

        let wrapped = subtract(&a, &b);
        let expected = Int32Array::from(vec![i32::MAX]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = subtract_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_mul_wrapping_overflow() {
        let a = Int32Array::from(vec![10]);
        let b = Int32Array::from(vec![i32::MAX]);

        let wrapped = multiply(&a, &b);
        let expected = Int32Array::from(vec![-10]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = multiply_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    #[cfg(not(feature = "simd"))]
    fn test_primitive_div_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MIN]);
        let b = Int32Array::from(vec![-1]);

        let wrapped = divide(&a, &b);
        let expected = Int32Array::from(vec![-2147483648]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = divide_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_add_scalar_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);

        let wrapped = add_scalar(&a, 1);
        let expected = Int32Array::from(vec![-2147483648, -2147483647]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = add_scalar_checked(&a, 1);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_subtract_scalar_wrapping_overflow() {
        let a = Int32Array::from(vec![-2]);

        let wrapped = subtract_scalar(&a, i32::MAX);
        let expected = Int32Array::from(vec![i32::MAX]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = subtract_scalar_checked(&a, i32::MAX);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_mul_scalar_wrapping_overflow() {
        let a = Int32Array::from(vec![10]);

        let wrapped = multiply_scalar(&a, i32::MAX);
        let expected = Int32Array::from(vec![-10]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = multiply_scalar_checked(&a, i32::MAX);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_add_scalar_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);

        let wrapped = add_scalar_dyn::<Int32Type>(&a, 1).unwrap();
        let expected =
            Arc::new(Int32Array::from(vec![-2147483648, -2147483647])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = add_scalar_checked_dyn::<Int32Type>(&a, 1);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_subtract_scalar_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![-2]);

        let wrapped = subtract_scalar_dyn::<Int32Type>(&a, i32::MAX).unwrap();
        let expected = Arc::new(Int32Array::from(vec![i32::MAX])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = subtract_scalar_checked_dyn::<Int32Type>(&a, i32::MAX);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_mul_scalar_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![10]);

        let wrapped = multiply_scalar_dyn::<Int32Type>(&a, i32::MAX).unwrap();
        let expected = Arc::new(Int32Array::from(vec![-10])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = multiply_scalar_checked_dyn::<Int32Type>(&a, i32::MAX);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_div_scalar_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MIN]);

        let wrapped = divide_scalar_dyn::<Int32Type>(&a, -1).unwrap();
        let expected = Arc::new(Int32Array::from(vec![-2147483648])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = divide_scalar_checked_dyn::<Int32Type>(&a, -1);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_div_opt_overflow_division_by_zero() {
        let a = Int32Array::from(vec![i32::MIN]);
        let b = Int32Array::from(vec![-1]);

        let wrapped = divide(&a, &b);
        let expected = Int32Array::from(vec![-2147483648]);
        assert_eq!(expected, wrapped.unwrap());

        let overflow = divide_opt(&a, &b);
        let expected = Int32Array::from(vec![-2147483648]);
        assert_eq!(expected, overflow.unwrap());

        let b = Int32Array::from(vec![0]);
        let overflow = divide_opt(&a, &b);
        let expected = Int32Array::from(vec![None]);
        assert_eq!(expected, overflow.unwrap());
    }

    #[test]
    fn test_primitive_add_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);
        let b = Int32Array::from(vec![1, 1]);

        let wrapped = add_dyn(&a, &b).unwrap();
        let expected =
            Arc::new(Int32Array::from(vec![-2147483648, -2147483647])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = add_dyn_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_subtract_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![-2]);
        let b = Int32Array::from(vec![i32::MAX]);

        let wrapped = subtract_dyn(&a, &b).unwrap();
        let expected = Arc::new(Int32Array::from(vec![i32::MAX])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = subtract_dyn_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_mul_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![10]);
        let b = Int32Array::from(vec![i32::MAX]);

        let wrapped = multiply_dyn(&a, &b).unwrap();
        let expected = Arc::new(Int32Array::from(vec![-10])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = multiply_dyn_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_div_dyn_wrapping_overflow() {
        let a = Int32Array::from(vec![i32::MIN]);
        let b = Int32Array::from(vec![-1]);

        let wrapped = divide_dyn(&a, &b).unwrap();
        let expected = Arc::new(Int32Array::from(vec![-2147483648])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = divide_dyn_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    fn test_decimal128() {
        let a = Decimal128Array::from_iter_values([1, 2, 4, 5]);
        let b = Decimal128Array::from_iter_values([7, -3, 6, 3]);
        let e = Decimal128Array::from_iter_values([8, -1, 10, 8]);
        let r = add(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = Decimal128Array::from_iter_values([-6, 5, -2, 2]);
        let r = subtract(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = Decimal128Array::from_iter_values([7, -6, 24, 15]);
        let r = multiply(&a, &b).unwrap();
        assert_eq!(e, r);

        let a = Decimal128Array::from_iter_values([23, 56, 32, 55]);
        let b = Decimal128Array::from_iter_values([1, -2, 4, 5]);
        let e = Decimal128Array::from_iter_values([23, -28, 8, 11]);
        let r = divide(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256() {
        let a = Decimal256Array::from_iter_values(
            [1, 2, 4, 5].into_iter().map(i256::from_i128),
        );
        let b = Decimal256Array::from_iter_values(
            [7, -3, 6, 3].into_iter().map(i256::from_i128),
        );
        let e = Decimal256Array::from_iter_values(
            [8, -1, 10, 8].into_iter().map(i256::from_i128),
        );
        let r = add(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = Decimal256Array::from_iter_values(
            [-6, 5, -2, 2].into_iter().map(i256::from_i128),
        );
        let r = subtract(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = Decimal256Array::from_iter_values(
            [7, -6, 24, 15].into_iter().map(i256::from_i128),
        );
        let r = multiply(&a, &b).unwrap();
        assert_eq!(e, r);

        let a = Decimal256Array::from_iter_values(
            [23, 56, 32, 55].into_iter().map(i256::from_i128),
        );
        let b = Decimal256Array::from_iter_values(
            [1, -2, 4, 5].into_iter().map(i256::from_i128),
        );
        let e = Decimal256Array::from_iter_values(
            [23, -28, 8, 11].into_iter().map(i256::from_i128),
        );
        let r = divide(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_div_scalar_dyn_opt_overflow_division_by_zero() {
        let a = Int32Array::from(vec![i32::MIN]);

        let division_by_zero = divide_scalar_opt_dyn::<Int32Type>(&a, 0);
        let expected = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        assert_eq!(&expected, &division_by_zero.unwrap());

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(i32::MIN).unwrap();
        let a = builder.finish();

        let division_by_zero = divide_scalar_opt_dyn::<Int32Type>(&a, 0);
        assert_eq!(&expected, &division_by_zero.unwrap());
    }

    #[test]
    fn test_sum_f16() {
        let a = Float16Array::from_iter_values([
            f16::from_f32(0.1),
            f16::from_f32(0.2),
            f16::from_f32(1.5),
            f16::from_f32(-0.1),
        ]);
        let b = Float16Array::from_iter_values([
            f16::from_f32(5.1),
            f16::from_f32(6.2),
            f16::from_f32(-1.),
            f16::from_f32(-2.1),
        ]);
        let expected = Float16Array::from_iter_values(
            a.values().iter().zip(b.values()).map(|(a, b)| a + b),
        );

        let c = add(&a, &b).unwrap();
        assert_eq!(c, expected);
    }

    #[test]
    fn test_resize_builder() {
        let mut null_buffer_builder = BooleanBufferBuilder::new(16);
        null_buffer_builder.append_slice(&[
            false, false, false, false, false, false, false, false, false, false, false,
            false, false, true, true, true,
        ]);
        // `resize` resizes the buffer length to the ceil of byte numbers.
        // So the underlying buffer is not changed.
        null_buffer_builder.resize(13);
        assert_eq!(null_buffer_builder.len(), 13);

        let nulls = null_buffer_builder.finish();
        assert_eq!(nulls.count_set_bits(), 0);
        let nulls = NullBuffer::new(nulls);
        assert_eq!(nulls.null_count(), 13);

        let mut data_buffer_builder = BufferBuilder::<i32>::new(13);
        data_buffer_builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let data_buffer = data_buffer_builder.finish();

        let arg1: Int32Array = ArrayDataBuilder::new(DataType::Int32)
            .len(13)
            .nulls(Some(nulls))
            .buffers(vec![data_buffer])
            .build()
            .unwrap()
            .into();

        assert_eq!(arg1.null_count(), 13);

        let mut data_buffer_builder = BufferBuilder::<i32>::new(13);
        data_buffer_builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let data_buffer = data_buffer_builder.finish();

        let arg2: Int32Array = ArrayDataBuilder::new(DataType::Int32)
            .len(13)
            .buffers(vec![data_buffer])
            .build()
            .unwrap()
            .into();

        assert_eq!(arg2.null_count(), 0);

        let result_dyn = add_dyn(&arg1, &arg2).unwrap();
        let result = result_dyn.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 13);
        assert_eq!(result.null_count(), 13);
    }

    #[test]
    fn test_primitive_array_add_mut_by_binary_mut() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);

        let c = binary_mut(a, &b, |a, b| a.add_wrapping(b))
            .unwrap()
            .unwrap();
        let expected = Int32Array::from(vec![Some(16), None, Some(12), None, Some(6)]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_add_mut_wrapping_overflow_by_try_binary_mut() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);
        let b = Int32Array::from(vec![1, 1]);

        let wrapped = binary_mut(a, &b, |a, b| a.add_wrapping(b))
            .unwrap()
            .unwrap();
        let expected = Int32Array::from(vec![-2147483648, -2147483647]);
        assert_eq!(expected, wrapped);

        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);
        let b = Int32Array::from(vec![1, 1]);
        let overflow = try_binary_mut(a, &b, |a, b| a.add_checked(b));
        let _ = overflow.unwrap().expect_err("overflow should be detected");
    }

    #[test]
    fn test_primitive_add_scalar_by_unary_mut() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = unary_mut(a, |value| value.add_wrapping(b)).unwrap();
        let expected = Int32Array::from(vec![18, 17, 12, 11, 4]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_add_scalar_overflow_by_try_unary_mut() {
        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);

        let wrapped = unary_mut(a, |value| value.add_wrapping(1)).unwrap();
        let expected = Int32Array::from(vec![-2147483648, -2147483647]);
        assert_eq!(expected, wrapped);

        let a = Int32Array::from(vec![i32::MAX, i32::MIN]);
        let overflow = try_unary_mut(a, |value| value.add_checked(1));
        let _ = overflow.unwrap().expect_err("overflow should be detected");
    }

    #[test]
    fn test_decimal_add_scalar_dyn() {
        let a = Decimal128Array::from(vec![100, 210, 320])
            .with_precision_and_scale(38, 2)
            .unwrap();

        let result = add_scalar_dyn::<Decimal128Type>(&a, 1).unwrap();
        let result = result
            .as_primitive::<Decimal128Type>()
            .clone()
            .with_precision_and_scale(38, 2)
            .unwrap();
        let expected = Decimal128Array::from(vec![101, 211, 321])
            .with_precision_and_scale(38, 2)
            .unwrap();

        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_decimal_multiply_allow_precision_loss() {
        // Overflow happening as i128 cannot hold multiplying result.
        // [123456789]
        let a = Decimal128Array::from(vec![123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [10]
        let b = Decimal128Array::from(vec![10000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let err = multiply_dyn_checked(&a, &b).unwrap_err();
        assert!(err.to_string().contains(
            "Overflow happened on: 123456789000000000000000000 * 10000000000000000000"
        ));

        // Allow precision loss.
        let result = multiply_fixed_point_checked(&a, &b, 28).unwrap();
        // [1234567890]
        let expected =
            Decimal128Array::from(vec![12345678900000000000000000000000000000])
                .with_precision_and_scale(38, 28)
                .unwrap();

        assert_eq!(&expected, &result);
        assert_eq!(
            result.value_as_string(0),
            "1234567890.0000000000000000000000000000"
        );

        // Rounding case
        // [0.000000000000000001, 123456789.555555555555555555, 1.555555555555555555]
        let a = Decimal128Array::from(vec![
            1,
            123456789555555555555555555,
            1555555555555555555,
        ])
        .with_precision_and_scale(38, 18)
        .unwrap();

        // [1.555555555555555555, 11.222222222222222222, 0.000000000000000001]
        let b = Decimal128Array::from(vec![1555555555555555555, 11222222222222222222, 1])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let result = multiply_fixed_point_checked(&a, &b, 28).unwrap();
        // [
        //    0.0000000000000000015555555556,
        //    1385459527.2345679012071330528765432099,
        //    0.0000000000000000015555555556
        // ]
        let expected = Decimal128Array::from(vec![
            15555555556,
            13854595272345679012071330528765432099,
            15555555556,
        ])
        .with_precision_and_scale(38, 28)
        .unwrap();

        assert_eq!(&expected, &result);

        // Rounded the value "1385459527.234567901207133052876543209876543210".
        assert_eq!(
            result.value_as_string(1),
            "1385459527.2345679012071330528765432099"
        );
        assert_eq!(result.value_as_string(0), "0.0000000000000000015555555556");
        assert_eq!(result.value_as_string(2), "0.0000000000000000015555555556");

        let a = Decimal128Array::from(vec![1230])
            .with_precision_and_scale(4, 2)
            .unwrap();

        let b = Decimal128Array::from(vec![1000])
            .with_precision_and_scale(4, 2)
            .unwrap();

        // Required scale is same as the product of the input scales. Behavior is same as multiply.
        let result = multiply_fixed_point_checked(&a, &b, 4).unwrap();
        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), 4);

        let expected = multiply_checked(&a, &b)
            .unwrap()
            .with_precision_and_scale(9, 4)
            .unwrap();
        assert_eq!(&expected, &result);

        // Required scale cannot be larger than the product of the input scales.
        let result = multiply_fixed_point_checked(&a, &b, 5).unwrap_err();
        assert!(result
            .to_string()
            .contains("Required scale 5 is greater than product scale 4"));
    }

    #[test]
    fn test_decimal_multiply_allow_precision_loss_overflow() {
        // [99999999999123456789]
        let a = Decimal128Array::from(vec![99999999999123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [9999999999910]
        let b = Decimal128Array::from(vec![9999999999910000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let err = multiply_fixed_point_checked(&a, &b, 28).unwrap_err();
        assert!(err.to_string().contains(
            "Overflow happened on: 99999999999123456789000000000000000000 * 9999999999910000000000000000000"
        ));

        let result = multiply_fixed_point(&a, &b, 28).unwrap();
        let expected =
            Decimal128Array::from(vec![62946009661555981610246871926660136960])
                .with_precision_and_scale(38, 28)
                .unwrap();

        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_decimal_multiply_fixed_point() {
        // [123456789]
        let a = Decimal128Array::from(vec![123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [10]
        let b = Decimal128Array::from(vec![10000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // `multiply` overflows on this case.
        let result = multiply(&a, &b).unwrap();
        let expected =
            Decimal128Array::from(vec![-16672482290199102048610367863168958464])
                .with_precision_and_scale(38, 10)
                .unwrap();
        assert_eq!(&expected, &result);

        // Avoid overflow by reducing the scale.
        let result = multiply_fixed_point(&a, &b, 28).unwrap();
        // [1234567890]
        let expected =
            Decimal128Array::from(vec![12345678900000000000000000000000000000])
                .with_precision_and_scale(38, 28)
                .unwrap();

        assert_eq!(&expected, &result);
        assert_eq!(
            result.value_as_string(0),
            "1234567890.0000000000000000000000000000"
        );
    }

    #[test]
    fn test_timestamp_second_add_interval() {
        // timestamp second + interval year month
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 + SECONDS_IN_DAY * (365 + 31 + 28),
            2 + SECONDS_IN_DAY * (365 + 31 + 28),
            3 + SECONDS_IN_DAY * (365 + 31 + 28),
            4 + SECONDS_IN_DAY * (365 + 31 + 28),
            5 + SECONDS_IN_DAY * (365 + 31 + 28),
        ]);
        assert_eq!(result, &expected);

        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();
        assert_eq!(result, &expected);

        // timestamp second + interval day time
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 + SECONDS_IN_DAY,
            2 + SECONDS_IN_DAY,
            3 + SECONDS_IN_DAY,
            4 + SECONDS_IN_DAY,
            5 + SECONDS_IN_DAY,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();
        assert_eq!(result, &expected);

        // timestamp second + interval month day nanosecond
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 + SECONDS_IN_DAY,
            2 + SECONDS_IN_DAY,
            3 + SECONDS_IN_DAY,
            4 + SECONDS_IN_DAY,
            5 + SECONDS_IN_DAY,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_timestamp_second_subtract_interval() {
        // timestamp second + interval year month
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 - SECONDS_IN_DAY * (31 + 30 + 365),
            2 - SECONDS_IN_DAY * (31 + 30 + 365),
            3 - SECONDS_IN_DAY * (31 + 30 + 365),
            4 - SECONDS_IN_DAY * (31 + 30 + 365),
            5 - SECONDS_IN_DAY * (31 + 30 + 365),
        ]);
        assert_eq!(&expected, result);

        // timestamp second + interval day time
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 - SECONDS_IN_DAY,
            2 - SECONDS_IN_DAY,
            3 - SECONDS_IN_DAY,
            4 - SECONDS_IN_DAY,
            5 - SECONDS_IN_DAY,
        ]);
        assert_eq!(&expected, result);

        // timestamp second + interval month day nanosecond
        let a = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();

        let expected = TimestampSecondArray::from(vec![
            1 - SECONDS_IN_DAY,
            2 - SECONDS_IN_DAY,
            3 - SECONDS_IN_DAY,
            4 - SECONDS_IN_DAY,
            5 - SECONDS_IN_DAY,
        ]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_millisecond_add_interval() {
        // timestamp millisecond + interval year month
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000,
            2 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000,
            3 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000,
            4 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000,
            5 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000,
        ]);
        assert_eq!(result, &expected);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();
        assert_eq!(result, &expected);

        // timestamp millisecond + interval day time
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000,
            2 + SECONDS_IN_DAY * 1_000,
            3 + SECONDS_IN_DAY * 1_000,
            4 + SECONDS_IN_DAY * 1_000,
            5 + SECONDS_IN_DAY * 1_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();
        assert_eq!(result, &expected);

        // timestamp millisecond + interval month day nanosecond
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000,
            2 + SECONDS_IN_DAY * 1_000,
            3 + SECONDS_IN_DAY * 1_000,
            4 + SECONDS_IN_DAY * 1_000,
            5 + SECONDS_IN_DAY * 1_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_timestamp_millisecond_subtract_interval() {
        // timestamp millisecond + interval year month
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000,
            2 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000,
            3 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000,
            4 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000,
            5 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp millisecond + interval day time
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000,
            2 - SECONDS_IN_DAY * 1_000,
            3 - SECONDS_IN_DAY * 1_000,
            4 - SECONDS_IN_DAY * 1_000,
            5 - SECONDS_IN_DAY * 1_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp millisecond + interval month day nanosecond
        let a = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMillisecondType>();

        let expected = TimestampMillisecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000,
            2 - SECONDS_IN_DAY * 1_000,
            3 - SECONDS_IN_DAY * 1_000,
            4 - SECONDS_IN_DAY * 1_000,
            5 - SECONDS_IN_DAY * 1_000,
        ]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_microsecond_add_interval() {
        // timestamp microsecond + interval year month
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000,
            2 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000,
            3 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000,
            4 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000,
            5 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000,
        ]);
        assert_eq!(result, &expected);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(result, &expected);

        // timestamp microsecond + interval day time
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000_000,
            2 + SECONDS_IN_DAY * 1_000_000,
            3 + SECONDS_IN_DAY * 1_000_000,
            4 + SECONDS_IN_DAY * 1_000_000,
            5 + SECONDS_IN_DAY * 1_000_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(result, &expected);

        // timestamp microsecond + interval month day nanosecond
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000_000,
            2 + SECONDS_IN_DAY * 1_000_000,
            3 + SECONDS_IN_DAY * 1_000_000,
            4 + SECONDS_IN_DAY * 1_000_000,
            5 + SECONDS_IN_DAY * 1_000_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_timestamp_microsecond_subtract_interval() {
        // timestamp microsecond + interval year month
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000,
            2 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000,
            3 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000,
            4 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000,
            5 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp microsecond + interval day time
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000_000,
            2 - SECONDS_IN_DAY * 1_000_000,
            3 - SECONDS_IN_DAY * 1_000_000,
            4 - SECONDS_IN_DAY * 1_000_000,
            5 - SECONDS_IN_DAY * 1_000_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp microsecond + interval month day nanosecond
        let a = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampMicrosecondType>();

        let expected = TimestampMicrosecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000_000,
            2 - SECONDS_IN_DAY * 1_000_000,
            3 - SECONDS_IN_DAY * 1_000_000,
            4 - SECONDS_IN_DAY * 1_000_000,
            5 - SECONDS_IN_DAY * 1_000_000,
        ]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_nanosecond_add_interval() {
        // timestamp nanosecond + interval year month
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000_000,
            2 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000_000,
            3 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000_000,
            4 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000_000,
            5 + SECONDS_IN_DAY * (31 + 28 + 365) * 1_000_000_000,
        ]);
        assert_eq!(result, &expected);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();
        assert_eq!(result, &expected);

        // timestamp nanosecond + interval day time
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000_000_000,
            2 + SECONDS_IN_DAY * 1_000_000_000,
            3 + SECONDS_IN_DAY * 1_000_000_000,
            4 + SECONDS_IN_DAY * 1_000_000_000,
            5 + SECONDS_IN_DAY * 1_000_000_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();
        assert_eq!(result, &expected);

        // timestamp nanosecond + interval month day nanosecond
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = add_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 + SECONDS_IN_DAY * 1_000_000_000,
            2 + SECONDS_IN_DAY * 1_000_000_000,
            3 + SECONDS_IN_DAY * 1_000_000_000,
            4 + SECONDS_IN_DAY * 1_000_000_000,
            5 + SECONDS_IN_DAY * 1_000_000_000,
        ]);
        assert_eq!(&expected, result);
        let result = add_dyn(&b, &a).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_timestamp_nanosecond_subtract_interval() {
        // timestamp nanosecond + interval year month
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalYearMonthArray::from(vec![
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
            Some(IntervalYearMonthType::make_value(1, 2)),
        ]);

        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000_000,
            2 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000_000,
            3 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000_000,
            4 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000_000,
            5 - SECONDS_IN_DAY * (31 + 30 + 365) * 1_000_000_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp nanosecond + interval day time
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
            Some(IntervalDayTimeType::make_value(1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000_000_000,
            2 - SECONDS_IN_DAY * 1_000_000_000,
            3 - SECONDS_IN_DAY * 1_000_000_000,
            4 - SECONDS_IN_DAY * 1_000_000_000,
            5 - SECONDS_IN_DAY * 1_000_000_000,
        ]);
        assert_eq!(&expected, result);

        // timestamp nanosecond + interval month day nanosecond
        let a = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
        ]);
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<TimestampNanosecondType>();

        let expected = TimestampNanosecondArray::from(vec![
            1 - SECONDS_IN_DAY * 1_000_000_000,
            2 - SECONDS_IN_DAY * 1_000_000_000,
            3 - SECONDS_IN_DAY * 1_000_000_000,
            4 - SECONDS_IN_DAY * 1_000_000_000,
            5 - SECONDS_IN_DAY * 1_000_000_000,
        ]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_second_subtract_timestamp() {
        let a = TimestampSecondArray::from(vec![0, 2, 4, 6, 8]);
        let b = TimestampSecondArray::from(vec![1, 2, 3, 4, 5]);
        let expected = DurationSecondArray::from(vec![-1, 0, 1, 2, 3]);

        // unchecked
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<DurationSecondType>();
        assert_eq!(&expected, result);

        // checked
        let result = subtract_dyn_checked(&a, &b).unwrap();
        let result = result.as_primitive::<DurationSecondType>();
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_second_subtract_timestamp_overflow() {
        let a = TimestampSecondArray::from(vec![
            <TimestampSecondType as ArrowPrimitiveType>::Native::MAX,
        ]);
        let b = TimestampSecondArray::from(vec![
            <TimestampSecondType as ArrowPrimitiveType>::Native::MIN,
        ]);

        // checked
        let result = subtract_dyn_checked(&a, &b);
        assert!(&result.is_err());
    }

    #[test]
    fn test_timestamp_microsecond_subtract_timestamp() {
        let a = TimestampMicrosecondArray::from(vec![0, 2, 4, 6, 8]);
        let b = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
        let expected = DurationMicrosecondArray::from(vec![-1, 0, 1, 2, 3]);

        // unchecked
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<DurationMicrosecondType>();
        assert_eq!(&expected, result);

        // checked
        let result = subtract_dyn_checked(&a, &b).unwrap();
        let result = result.as_primitive::<DurationMicrosecondType>();
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_microsecond_subtract_timestamp_overflow() {
        let a = TimestampMicrosecondArray::from(vec![i64::MAX]);
        let b = TimestampMicrosecondArray::from(vec![i64::MIN]);

        // checked
        let result = subtract_dyn_checked(&a, &b);
        assert!(&result.is_err());
    }

    #[test]
    fn test_timestamp_millisecond_subtract_timestamp() {
        let a = TimestampMillisecondArray::from(vec![0, 2, 4, 6, 8]);
        let b = TimestampMillisecondArray::from(vec![1, 2, 3, 4, 5]);
        let expected = DurationMillisecondArray::from(vec![-1, 0, 1, 2, 3]);

        // unchecked
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<DurationMillisecondType>();
        assert_eq!(&expected, result);

        // checked
        let result = subtract_dyn_checked(&a, &b).unwrap();
        let result = result.as_primitive::<DurationMillisecondType>();
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_millisecond_subtract_timestamp_overflow() {
        let a = TimestampMillisecondArray::from(vec![i64::MAX]);
        let b = TimestampMillisecondArray::from(vec![i64::MIN]);

        // checked
        let result = subtract_dyn_checked(&a, &b);
        assert!(&result.is_err());
    }

    #[test]
    fn test_timestamp_nanosecond_subtract_timestamp() {
        let a = TimestampNanosecondArray::from(vec![0, 2, 4, 6, 8]);
        let b = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let expected = DurationNanosecondArray::from(vec![-1, 0, 1, 2, 3]);

        // unchecked
        let result = subtract_dyn(&a, &b).unwrap();
        let result = result.as_primitive::<DurationNanosecondType>();
        assert_eq!(&expected, result);

        // checked
        let result = subtract_dyn_checked(&a, &b).unwrap();
        let result = result.as_primitive::<DurationNanosecondType>();
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_timestamp_nanosecond_subtract_timestamp_overflow() {
        let a = TimestampNanosecondArray::from(vec![
            <TimestampNanosecondType as ArrowPrimitiveType>::Native::MAX,
        ]);
        let b = TimestampNanosecondArray::from(vec![
            <TimestampNanosecondType as ArrowPrimitiveType>::Native::MIN,
        ]);

        // checked
        let result = subtract_dyn_checked(&a, &b);
        assert!(&result.is_err());
    }
}
