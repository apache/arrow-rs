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

use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use num::{One, Zero};

use crate::buffer::Buffer;
#[cfg(feature = "simd")]
use crate::buffer::MutableBuffer;
use crate::compute::kernels::arity::unary;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes;
use crate::datatypes::ArrowNumericType;
use crate::error::{ArrowError, Result};
use crate::{array::*, util::bit_util};
use num::traits::Pow;
#[cfg(feature = "simd")]
use std::borrow::BorrowMut;
#[cfg(feature = "simd")]
use std::slice::{ChunksExact, ChunksExactMut};

/// Helper function to perform math lambda function on values from two arrays. If either
/// left or right value is null then the output value is also null, so `1 + null` is
/// `null`.
///
/// # Errors
///
/// This function errors if the arrays have different lengths
pub fn math_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let values = left
        .values()
        .iter()
        .zip(right.values().iter())
        .map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size from a PrimitiveArray
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = unsafe {
        ArrayData::new_unchecked(
            T::DATA_TYPE,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };
    Ok(PrimitiveArray::<T>::from(data))
}

/// Helper function for operations where a valid `0` on the right array should
/// result in an [ArrowError::DivideByZero], namely the division and modulo operations
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * there is an element where both left and right values are valid and the right value is `0`
fn math_checked_divide_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: One + Zero,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let buffer = if let Some(b) = &null_bit_buffer {
        let values = left.values().iter().zip(right.values()).enumerate().map(
            |(i, (left, right))| {
                let is_valid = unsafe { bit_util::get_bit_raw(b.as_ptr(), i) };
                if is_valid {
                    if right.is_zero() {
                        Err(ArrowError::DivideByZero)
                    } else {
                        Ok(op(*left, *right))
                    }
                } else {
                    Ok(T::default_value())
                }
            },
        );
        // Safety: Iterator comes from a PrimitiveArray which reports its size correctly
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    } else {
        // no value is null
        let values = left
            .values()
            .iter()
            .zip(right.values())
            .map(|(left, right)| {
                if right.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    Ok(op(*left, *right))
                }
            });
        // Safety: Iterator comes from a PrimitiveArray which reports its size correctly
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    }?;

    let data = unsafe {
        ArrayData::new_unchecked(
            T::DATA_TYPE,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };
    Ok(PrimitiveArray::<T>::from(data))
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
) -> Result<T::Simd>
where
    T::Native: One + Zero,
{
    let zero = T::init(T::Native::zero());
    let one = T::init(T::Native::one());

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
) -> Result<T::Simd>
where
    T::Native: One + Zero,
{
    let zero = T::init(T::Native::zero());
    let one = T::init(T::Native::one());

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
    left_chunks: ChunksExact<T::Native>,
    right_chunks: ChunksExact<T::Native>,
    result_chunks: ChunksExactMut<T::Native>,
    op: F,
) -> Result<()>
where
    T: ArrowNumericType,
    T::Native: Zero,
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
                if *right_scalar == T::Native::zero() {
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
fn simd_checked_divide_op<T, SIMD_OP, SCALAR_OP>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: One + Zero,
    SIMD_OP: Fn(Option<u64>, T::Simd, T::Simd) -> Result<T::Simd>,
    SCALAR_OP: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // Create the combined `Bitmap`
    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let lanes = T::lanes();
    let buffer_size = left.len() * std::mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    match &null_bit_buffer {
        Some(b) => {
            // combine_option_bitmap returns a slice or new buffer starting at 0
            let valid_chunks = b.bit_chunks(0, left.len());

            // process data in chunks of 64 elements since we also get 64 bits of validity information at a time

            // safety: result is newly created above, always written as a T below
            let mut result_chunks =
                unsafe { result.typed_data_mut().chunks_exact_mut(64) };
            let mut left_chunks = left.values().chunks_exact(64);
            let mut right_chunks = right.values().chunks_exact(64);

            valid_chunks
                .iter()
                .zip(
                    result_chunks
                        .borrow_mut()
                        .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut())),
                )
                .try_for_each(
                    |(mut mask, (result_slice, (left_slice, right_slice)))| {
                        // split chunks further into slices corresponding to the vector length
                        // the compiler is able to unroll this inner loop and remove bounds checks
                        // since the outer chunk size (64) is always a multiple of the number of lanes
                        result_slice
                            .chunks_exact_mut(lanes)
                            .zip(left_slice.chunks_exact(lanes).zip(right_slice.chunks_exact(lanes)))
                            .try_for_each(|(result_slice, (left_slice, right_slice))| -> Result<()> {
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
            // safety: result is newly created above, always written as a T below
            let mut result_chunks =
                unsafe { result.typed_data_mut().chunks_exact_mut(lanes) };
            let mut left_chunks = left.values().chunks_exact(lanes);
            let mut right_chunks = right.values().chunks_exact(lanes);

            result_chunks
                .borrow_mut()
                .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
                .try_for_each(
                    |(result_slice, (left_slice, right_slice))| -> Result<()> {
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

    let data = unsafe {
        ArrayData::new_unchecked(
            T::DATA_TYPE,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.into()],
            vec![],
        )
    };
    Ok(PrimitiveArray::<T>::from(data))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn add<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    math_op(left, right, |a, b| a + b)
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
pub fn add_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    Ok(unary(array, |value| value + scalar))
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn subtract<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Sub<Output = T::Native>,
{
    math_op(left, right, |a, b| a - b)
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
pub fn subtract_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero,
{
    Ok(unary(array, |value| value - scalar))
}

/// Perform `-` operation on an array. If value is null then the result is also null.
pub fn negate<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Neg<Output = T::Native>,
{
    Ok(unary(array, |x| -x))
}

/// Raise array with floating point values to the power of a scalar.
pub fn powf_scalar<T>(
    array: &PrimitiveArray<T>,
    raise: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowFloatNumericType,
    T::Native: Pow<T::Native, Output = T::Native>,
{
    Ok(unary(array, |x| x.pow(raise)))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn multiply<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Mul<Output = T::Native>,
{
    math_op(left, right, |a, b| a * b)
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
pub fn multiply_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + Zero
        + One,
{
    Ok(unary(array, |value| value * scalar))
}

/// Perform `left % right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
pub fn modulus<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Rem<Output = T::Native> + Zero + One,
{
    #[cfg(feature = "simd")]
    return simd_checked_divide_op(&left, &right, simd_checked_modulus::<T>, |a, b| {
        a % b
    });
    #[cfg(not(feature = "simd"))]
    return math_checked_divide_op(left, right, |a, b| a % b);
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
pub fn divide<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Div<Output = T::Native> + Zero + One,
{
    #[cfg(feature = "simd")]
    return simd_checked_divide_op(&left, &right, simd_checked_divide::<T>, |a, b| a / b);
    #[cfg(not(feature = "simd"))]
    return math_checked_divide_op(left, right, |a, b| a / b);
}

/// Perform `left / right` operation on two arrays without checking for division by zero.
/// The result of dividing by zero follows normal floating point rules.
/// If either left or right value is null then the result is also null. If any right hand value is zero then the result of this
pub fn divide_unchecked<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowFloatNumericType,
    T::Native: Div<Output = T::Native>,
{
    math_op(left, right, |a, b| a / b)
}

/// Modulus every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
pub fn modulus_scalar<T>(
    array: &PrimitiveArray<T>,
    modulo: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Rem<Output = T::Native> + Zero,
{
    if modulo.is_zero() {
        return Err(ArrowError::DivideByZero);
    }

    Ok(unary(array, |a| a % modulo))
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
pub fn divide_scalar<T>(
    array: &PrimitiveArray<T>,
    divisor: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Div<Output = T::Native> + Zero,
{
    if divisor.is_zero() {
        return Err(ArrowError::DivideByZero);
    }
    Ok(unary(array, |a| a / divisor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

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
        let e = add(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
        assert_eq!(
            "ComputeError(\"Cannot perform math operation on arrays of different length\")",
            format!("{:?}", e)
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
        let a = as_primitive_array(&a);
        let actual = add_scalar(a, 3).unwrap();
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
        let a = as_primitive_array(&a);
        let actual = subtract_scalar(a, 3).unwrap();
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
        let a = as_primitive_array(&a);
        let actual = multiply_scalar(a, 3).unwrap();
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
    fn test_primitive_array_modulus() {
        let a = Int32Array::from(vec![15, 15, 8, 1, 9]);
        let b = Int32Array::from(vec![5, 6, 8, 9, 1]);
        let c = modulus(&a, &b).unwrap();
        assert_eq!(0, c.value(0));
        assert_eq!(3, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(1, c.value(3));
        assert_eq!(0, c.value(4));
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
    fn test_primitive_array_divide_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let a = as_primitive_array(&a);
        let actual = divide_scalar(a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(3), Some(2), None]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_primitive_array_modulus_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = modulus_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![0, 2, 0, 2, 1]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_primitive_array_modulus_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let a = as_primitive_array(&a);
        let actual = modulus_scalar(a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(0), Some(2), None]);
        assert_eq!(actual, expected);
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
        let c = divide(&a, &b).unwrap();
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

        let c = divide(a, b).unwrap();
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
    fn test_primitive_array_divide_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        divide(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_primitive_array_modulus_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        modulus(&a, &b).unwrap();
    }

    #[test]
    fn test_primitive_array_divide_f64() {
        let a = Float64Array::from(vec![15.0, 15.0, 8.0]);
        let b = Float64Array::from(vec![5.0, 6.0, 8.0]);
        let c = divide(&a, &b).unwrap();
        assert!(3.0 - c.value(0) < f64::EPSILON);
        assert!(2.5 - c.value(1) < f64::EPSILON);
        assert!(1.0 - c.value(2) < f64::EPSILON);
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
        let a: Int64Array = (0..100).into_iter().map(Some).collect();
        let actual = negate(&a).unwrap();
        let expected: Int64Array = (0..100).into_iter().map(|i| Some(-i)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_arithmetic_kernel_should_not_rely_on_padding() {
        let a: UInt8Array = (0..128_u8).into_iter().map(Some).collect();
        let a = a.slice(63, 65);
        let a = a.as_any().downcast_ref::<UInt8Array>().unwrap();

        let b: UInt8Array = (0..128_u8).into_iter().map(Some).collect();
        let b = b.slice(63, 65);
        let b = b.as_any().downcast_ref::<UInt8Array>().unwrap();

        let actual = add(a, b).unwrap();
        let actual: Vec<Option<u8>> = actual.iter().collect();
        let expected: Vec<Option<u8>> = (63..63_u8 + 65_u8)
            .into_iter()
            .map(|i| Some(i + i))
            .collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_raise_power_scalar() {
        let a = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let actual = powf_scalar(&a, 2.0).unwrap();
        let expected = Float64Array::from(vec![1.0, 4.0, 9.0]);
        assert_eq!(expected, actual);
        let a = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
        let actual = powf_scalar(&a, 2.0).unwrap();
        let expected = Float64Array::from(vec![Some(1.0), None, Some(9.0)]);
        assert_eq!(expected, actual);
    }
}
