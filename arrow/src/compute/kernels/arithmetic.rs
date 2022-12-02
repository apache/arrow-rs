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

use crate::array::*;
#[cfg(feature = "simd")]
use crate::buffer::MutableBuffer;
use crate::compute::kernels::arity::unary;
use crate::compute::{
    binary, binary_opt, try_binary, try_unary, try_unary_dyn, unary_dyn,
};
use crate::datatypes::{
    ArrowNativeTypeOp, ArrowNumericType, DataType, Date32Type, Date64Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
};
#[cfg(feature = "dyn_arith_dict")]
use crate::datatypes::{
    Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::error::{ArrowError, Result};
use crate::{datatypes, downcast_primitive_array};
use num::traits::Pow;
#[cfg(feature = "simd")]
use std::borrow::BorrowMut;
#[cfg(feature = "simd")]
use std::slice::{ChunksExact, ChunksExactMut};
use std::sync::Arc;

/// Helper function to perform math lambda function on values from two arrays. If either
/// left or right value is null then the output value is also null, so `1 + null` is
/// `null`.
///
/// # Errors
///
/// This function errors if the arrays have different lengths
pub fn math_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<PrimitiveArray<LT>>
where
    LT: ArrowNumericType,
    RT: ArrowNumericType,
    F: Fn(LT::Native, RT::Native) -> LT::Native,
    LT::Native: ArrowNativeTypeOp,
    RT::Native: ArrowNativeTypeOp,
{
    binary(left, right, op)
}

/// This is similar to `math_op` as it performs given operation between two input primitive arrays.
/// But the given operation can return `Err` if overflow is detected. For the case, this function
/// returns an `Err`.
fn math_checked_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<PrimitiveArray<LT>>
where
    LT: ArrowNumericType,
    RT: ArrowNumericType,
    F: Fn(LT::Native, RT::Native) -> Result<LT::Native>,
    LT::Native: ArrowNativeTypeOp,
    RT::Native: ArrowNativeTypeOp,
{
    try_binary(left, right, op)
}

/// Helper function for operations where a valid `0` on the right array should
/// result in an [ArrowError::DivideByZero], namely the division and modulo operations
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * there is an element where both left and right values are valid and the right value is `0`
fn math_checked_divide_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<PrimitiveArray<LT>>
where
    LT: ArrowNumericType,
    RT: ArrowNumericType,
    F: Fn(LT::Native, RT::Native) -> Result<LT::Native>,
{
    try_binary(left, right, op)
}

/// Helper function for operations where a valid `0` on the right array should
/// result in an [ArrowError::DivideByZero], namely the division and modulo operations
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * there is an element where both left and right values are valid and the right value is `0`
#[cfg(feature = "dyn_arith_dict")]
fn math_checked_divide_op_on_iters<T, F>(
    left: impl Iterator<Item = Option<T::Native>>,
    right: impl Iterator<Item = Option<T::Native>>,
    op: F,
    len: usize,
    null_bit_buffer: Option<crate::buffer::Buffer>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> Result<T::Native>,
{
    let buffer = if null_bit_buffer.is_some() {
        let values = left.zip(right).map(|(left, right)| {
            if let (Some(l), Some(r)) = (left, right) {
                op(l, r)
            } else {
                Ok(T::default_value())
            }
        });
        // Safety: Iterator comes from a PrimitiveArray which reports its size correctly
        unsafe { crate::buffer::Buffer::try_from_trusted_len_iter(values) }
    } else {
        // no value is null
        let values = left
            .map(|l| l.unwrap())
            .zip(right.map(|r| r.unwrap()))
            .map(|(left, right)| op(left, right));
        // Safety: Iterator comes from a PrimitiveArray which reports its size correctly
        unsafe { crate::buffer::Buffer::try_from_trusted_len_iter(values) }
    }?;

    let data = unsafe {
        ArrayData::new_unchecked(
            T::DATA_TYPE,
            len,
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
    T::Native: ArrowNativeTypeOp,
{
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
) -> Result<T::Simd>
where
    T::Native: ArrowNativeTypeOp,
{
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
    left_chunks: ChunksExact<T::Native>,
    right_chunks: ChunksExact<T::Native>,
    result_chunks: ChunksExactMut<T::Native>,
    op: F,
) -> Result<()>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
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
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
    SI: Fn(Option<u64>, T::Simd, T::Simd) -> Result<T::Simd>,
    SC: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // Create the combined `Bitmap`
    let null_bit_buffer = arrow_data::bit_mask::combine_option_bitmap(
        &[left.data_ref(), right.data_ref()],
        left.len(),
    );

    let lanes = T::lanes();
    let buffer_size = left.len() * std::mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    match &null_bit_buffer {
        Some(b) => {
            // combine_option_bitmap returns a slice or new buffer starting at 0
            let valid_chunks = b.bit_chunks(0, left.len());

            // process data in chunks of 64 elements since we also get 64 bits of validity information at a time

            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(64);
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
            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(lanes);
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

/// Applies $OP to $LEFT and $RIGHT which are two dictionaries which have (the same) key type $KT
#[cfg(feature = "dyn_arith_dict")]
macro_rules! typed_dict_op {
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $KT: tt, $MATH_OP: ident) => {{
        match ($LEFT.value_type(), $RIGHT.value_type()) {
            (DataType::Int8, DataType::Int8) => {
                let array = $MATH_OP::<$KT, Int8Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Int16, DataType::Int16) => {
                let array = $MATH_OP::<$KT, Int16Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Int32, DataType::Int32) => {
                let array = $MATH_OP::<$KT, Int32Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Int64, DataType::Int64) => {
                let array = $MATH_OP::<$KT, Int64Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::UInt8, DataType::UInt8) => {
                let array = $MATH_OP::<$KT, UInt8Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::UInt16, DataType::UInt16) => {
                let array = $MATH_OP::<$KT, UInt16Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::UInt32, DataType::UInt32) => {
                let array = $MATH_OP::<$KT, UInt32Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::UInt64, DataType::UInt64) => {
                let array = $MATH_OP::<$KT, UInt64Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Float32, DataType::Float32) => {
                let array = $MATH_OP::<$KT, Float32Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Float64, DataType::Float64) => {
                let array = $MATH_OP::<$KT, Float64Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2)) if s1 == s2 => {
                let array = $MATH_OP::<$KT, Decimal128Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (DataType::Decimal256(_, s1), DataType::Decimal256(_, s2)) if s1 == s2 => {
                let array = $MATH_OP::<$KT, Decimal256Type, _>($LEFT, $RIGHT, $OP)?;
                Ok(Arc::new(array))
            }
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot perform arithmetic operation on two dictionary arrays of different value types ({} and {})",
                t1, t2
            ))),
        }
    }};
}

#[cfg(feature = "dyn_arith_dict")]
macro_rules! typed_dict_math_op {
   // Applies `LEFT OP RIGHT` when `LEFT` and `RIGHT` both are `DictionaryArray`
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $MATH_OP: ident) => {{
        match ($LEFT.data_type(), $RIGHT.data_type()) {
            (DataType::Dictionary(left_key_type, _), DataType::Dictionary(right_key_type, _))=> {
                match (left_key_type.as_ref(), right_key_type.as_ref()) {
                    (DataType::Int8, DataType::Int8) => {
                        let left = as_dictionary_array::<Int8Type>($LEFT);
                        let right = as_dictionary_array::<Int8Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, Int8Type, $MATH_OP)
                    }
                    (DataType::Int16, DataType::Int16) => {
                        let left = as_dictionary_array::<Int16Type>($LEFT);
                        let right = as_dictionary_array::<Int16Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, Int16Type, $MATH_OP)
                    }
                    (DataType::Int32, DataType::Int32) => {
                        let left = as_dictionary_array::<Int32Type>($LEFT);
                        let right = as_dictionary_array::<Int32Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, Int32Type, $MATH_OP)
                    }
                    (DataType::Int64, DataType::Int64) => {
                        let left = as_dictionary_array::<Int64Type>($LEFT);
                        let right = as_dictionary_array::<Int64Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, Int64Type, $MATH_OP)
                    }
                    (DataType::UInt8, DataType::UInt8) => {
                        let left = as_dictionary_array::<UInt8Type>($LEFT);
                        let right = as_dictionary_array::<UInt8Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, UInt8Type, $MATH_OP)
                    }
                    (DataType::UInt16, DataType::UInt16) => {
                        let left = as_dictionary_array::<UInt16Type>($LEFT);
                        let right = as_dictionary_array::<UInt16Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, UInt16Type, $MATH_OP)
                    }
                    (DataType::UInt32, DataType::UInt32) => {
                        let left = as_dictionary_array::<UInt32Type>($LEFT);
                        let right = as_dictionary_array::<UInt32Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, UInt32Type, $MATH_OP)
                    }
                    (DataType::UInt64, DataType::UInt64) => {
                        let left = as_dictionary_array::<UInt64Type>($LEFT);
                        let right = as_dictionary_array::<UInt64Type>($RIGHT);
                        typed_dict_op!(left, right, $OP, UInt64Type, $MATH_OP)
                    }
                    (t1, t2) => Err(ArrowError::CastError(format!(
                        "Cannot perform arithmetic operation on two dictionary arrays of different key types ({} and {})",
                        t1, t2
                    ))),
                }
            }
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot perform arithmetic operation on dictionary array with non-dictionary array ({} and {})",
                t1, t2
            ))),
        }
    }};
}

#[cfg(not(feature = "dyn_arith_dict"))]
macro_rules! typed_dict_math_op {
   // Applies `LEFT OP RIGHT` when `LEFT` and `RIGHT` both are `DictionaryArray`
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $MATH_OP: ident) => {{
        Err(ArrowError::CastError(format!(
            "Arithmetic on arrays of type {} with array of type {} requires \"dyn_arith_dict\" feature",
            $LEFT.data_type(), $RIGHT.data_type()
        )))
    }};
}

/// Perform given operation on two `DictionaryArray`s.
/// Returns an error if the two arrays have different value type
#[cfg(feature = "dyn_arith_dict")]
fn math_op_dict<K, T, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    K: ArrowNumericType,
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
    T::Native: ArrowNativeTypeOp,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(format!(
            "Cannot perform operation on arrays of different length ({}, {})",
            left.len(),
            right.len()
        )));
    }

    // Safety justification: Since the inputs are valid Arrow arrays, all values are
    // valid indexes into the dictionary (which is verified during construction)

    let left_iter = unsafe {
        left.values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .take_iter_unchecked(left.keys_iter())
    };

    let right_iter = unsafe {
        right
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .take_iter_unchecked(right.keys_iter())
    };

    let result = left_iter
        .zip(right_iter)
        .map(|(left_value, right_value)| {
            if let (Some(left), Some(right)) = (left_value, right_value) {
                Some(op(left, right))
            } else {
                None
            }
        })
        .collect();

    Ok(result)
}

/// Perform given operation on two `DictionaryArray`s.
/// Returns an error if the two arrays have different value type
#[cfg(feature = "dyn_arith_dict")]
fn math_checked_op_dict<K, T, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    K: ArrowNumericType,
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> Result<T::Native>,
    T::Native: ArrowNativeTypeOp,
{
    // left and right's value types are supposed to be same as guaranteed by the caller macro now.
    if left.value_type() != T::DATA_TYPE {
        return Err(ArrowError::NotYetImplemented(format!(
            "Cannot perform provided operation on dictionary array of value type {}",
            left.value_type()
        )));
    }

    let left = left.downcast_dict::<PrimitiveArray<T>>().unwrap();
    let right = right.downcast_dict::<PrimitiveArray<T>>().unwrap();

    try_binary(left, right, op)
}

/// Helper function for operations where a valid `0` on the right array should
/// result in an [ArrowError::DivideByZero], namely the division and modulo operations
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * there is an element where both left and right values are valid and the right value is `0`
#[cfg(feature = "dyn_arith_dict")]
fn math_divide_checked_op_dict<K, T, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    K: ArrowNumericType,
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> Result<T::Native>,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(format!(
            "Cannot perform operation on arrays of different length ({}, {})",
            left.len(),
            right.len()
        )));
    }

    let null_bit_buffer = arrow_data::bit_mask::combine_option_bitmap(
        &[left.data_ref(), right.data_ref()],
        left.len(),
    );

    // Safety justification: Since the inputs are valid Arrow arrays, all values are
    // valid indexes into the dictionary (which is verified during construction)

    let left_iter = unsafe {
        left.values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .take_iter_unchecked(left.keys_iter())
    };

    let right_iter = unsafe {
        right
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .take_iter_unchecked(right.keys_iter())
    };

    math_checked_divide_op_on_iters(
        left_iter,
        right_iter,
        op,
        left.len(),
        null_bit_buffer,
    )
}

#[cfg(feature = "dyn_arith_dict")]
fn math_divide_safe_op_dict<K, T, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<ArrayRef>
where
    K: ArrowNumericType,
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> Option<T::Native>,
{
    let left = left.downcast_dict::<PrimitiveArray<T>>().unwrap();
    let right = right.downcast_dict::<PrimitiveArray<T>>().unwrap();
    let array: PrimitiveArray<T> = binary_opt::<_, _, _, T>(left, right, op)?;
    Ok(Arc::new(array) as ArrayRef)
}

fn math_safe_divide_op<LT, RT, F>(
    left: &PrimitiveArray<LT>,
    right: &PrimitiveArray<RT>,
    op: F,
) -> Result<ArrayRef>
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
pub fn add<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    math_op(left, right, |a, b| a.add_wrapping(b))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add` instead.
pub fn add_checked<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_binary(left, right, |a, b| a.add_checked(b))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_dyn_checked` instead.
pub fn add_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(left, right, |a, b| a.add_wrapping(b), math_op_dict)
        }
        DataType::Date32 => {
            let l = as_primitive_array::<Date32Type>(left);
            match right.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let r = as_primitive_array::<IntervalYearMonthType>(right);
                    let res = math_op(l, r, Date32Type::add_year_months)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let r = as_primitive_array::<IntervalDayTimeType>(right);
                    let res = math_op(l, r, Date32Type::add_day_time)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let r = as_primitive_array::<IntervalMonthDayNanoType>(right);
                    let res = math_op(l, r, Date32Type::add_month_day_nano)?;
                    Ok(Arc::new(res))
                }
                _ => Err(ArrowError::CastError(format!(
                    "Cannot perform arithmetic operation between array of type {} and array of type {}",
                    left.data_type(), right.data_type()
                ))),
            }
        }
        DataType::Date64 => {
            let l = as_primitive_array::<Date64Type>(left);
            match right.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let r = as_primitive_array::<IntervalYearMonthType>(right);
                    let res = math_op(l, r, Date64Type::add_year_months)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let r = as_primitive_array::<IntervalDayTimeType>(right);
                    let res = math_op(l, r, Date64Type::add_day_time)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let r = as_primitive_array::<IntervalMonthDayNanoType>(right);
                    let res = math_op(l, r, Date64Type::add_month_day_nano)?;
                    Ok(Arc::new(res))
                }
                _ => Err(ArrowError::CastError(format!(
                    "Cannot perform arithmetic operation between array of type {} and array of type {}",
                    left.data_type(), right.data_type()
                ))),
            }
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_op(left, right, |a, b| a.add_wrapping(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add_dyn` instead.
pub fn add_dyn_checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| a.add_checked(b),
                math_checked_op_dict
            )
        }
        DataType::Date32 => {
            let l = as_primitive_array::<Date32Type>(left);
            match right.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let r = as_primitive_array::<IntervalYearMonthType>(right);
                    let res = math_op(l, r, Date32Type::add_year_months)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let r = as_primitive_array::<IntervalDayTimeType>(right);
                    let res = math_op(l, r, Date32Type::add_day_time)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let r = as_primitive_array::<IntervalMonthDayNanoType>(right);
                    let res = math_op(l, r, Date32Type::add_month_day_nano)?;
                    Ok(Arc::new(res))
                }
                _ => Err(ArrowError::CastError(format!(
                    "Cannot perform arithmetic operation between array of type {} and array of type {}",
                    left.data_type(), right.data_type()
                ))),
            }
        }
        DataType::Date64 => {
            let l = as_primitive_array::<Date64Type>(left);
            match right.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let r = as_primitive_array::<IntervalYearMonthType>(right);
                    let res = math_op(l, r, Date64Type::add_year_months)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let r = as_primitive_array::<IntervalDayTimeType>(right);
                    let res = math_op(l, r, Date64Type::add_day_time)?;
                    Ok(Arc::new(res))
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let r = as_primitive_array::<IntervalMonthDayNanoType>(right);
                    let res = math_op(l, r, Date64Type::add_month_day_nano)?;
                    Ok(Arc::new(res))
                }
                _ => Err(ArrowError::CastError(format!(
                    "Cannot perform arithmetic operation between array of type {} and array of type {}",
                    left.data_type(), right.data_type()
                ))),
            }
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_checked_op(left, right, |a, b| a.add_checked(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `add_scalar_checked` instead.
pub fn add_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    Ok(unary(array, |value| value.add_wrapping(scalar)))
}

/// Add every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `add_scalar` instead.
pub fn add_scalar_checked<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn add_scalar_dyn<T>(array: &dyn Array, scalar: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn add_scalar_checked_dyn<T>(array: &dyn Array, scalar: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary_dyn::<_, T>(array, |value| value.add_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_checked` instead.
pub fn subtract<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    math_op(left, right, |a, b| a.sub_wrapping(b))
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract` instead.
pub fn subtract_checked<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_binary(left, right, |a, b| a.sub_checked(b))
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_dyn_checked` instead.
pub fn subtract_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(left, right, |a, b| a.sub_wrapping(b), math_op_dict)
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_op(left, right, |a, b| a.sub_wrapping(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_dyn` instead.
pub fn subtract_dyn_checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| a.sub_checked(b),
                math_checked_op_dict
            )
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_checked_op(left, right, |a, b| a.sub_checked(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_scalar_checked` instead.
pub fn subtract_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    Ok(unary(array, |value| value.sub_wrapping(scalar)))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_scalar` instead.
pub fn subtract_scalar_checked<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary(array, |value| value.sub_checked(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `subtract_scalar_checked_dyn` instead.
pub fn subtract_scalar_dyn<T>(array: &dyn Array, scalar: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    unary_dyn::<_, T>(array, |value| value.sub_wrapping(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `subtract_scalar_dyn` instead.
pub fn subtract_scalar_checked_dyn<T>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary_dyn::<_, T>(array, |value| value.sub_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `-` operation on an array. If value is null then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `negate_checked` instead.
pub fn negate<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    Ok(unary(array, |x| x.neg_wrapping()))
}

/// Perform `-` operation on an array. If value is null then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `negate` instead.
pub fn negate_checked<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary(array, |value| value.neg_checked())
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
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_check` instead.
pub fn multiply<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    math_op(left, right, |a, b| a.mul_wrapping(b))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply` instead.
pub fn multiply_checked<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_binary(left, right, |a, b| a.mul_checked(b))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_dyn_checked` instead.
pub fn multiply_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(left, right, |a, b| a.mul_wrapping(b), math_op_dict)
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_op(left, right, |a, b| a.mul_wrapping(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_dyn` instead.
pub fn multiply_dyn_checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| a.mul_checked(b),
                math_checked_op_dict
            )
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_checked_op(left, right, |a, b| a.mul_checked(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_scalar_checked` instead.
pub fn multiply_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    Ok(unary(array, |value| value.mul_wrapping(scalar)))
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_scalar` instead.
pub fn multiply_scalar_checked<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary(array, |value| value.mul_checked(scalar))
}

/// Multiply every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_scalar_checked_dyn` instead.
pub fn multiply_scalar_dyn<T>(array: &dyn Array, scalar: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    unary_dyn::<_, T>(array, |value| value.mul_wrapping(scalar))
}

/// Subtract every value in an array by a scalar. If any value in the array is null then the
/// result is also null. The given array must be a `PrimitiveArray` of the type same as
/// the scalar, or a `DictionaryArray` of the value type same as the scalar.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `multiply_scalar_dyn` instead.
pub fn multiply_scalar_checked_dyn<T>(
    array: &dyn Array,
    scalar: T::Native,
) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    try_unary_dyn::<_, T>(array, |value| value.mul_checked(scalar))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Perform `left % right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
pub fn modulus<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
///
/// When `simd` feature is not enabled. This detects overflow and returns an `Err` for that.
/// For an non-overflow-checking variant, use `divide` instead.
pub fn divide_checked<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_checked_divide_op(&left, &right, simd_checked_divide::<T>, |a, b| {
        a.div_wrapping(b)
    });
    #[cfg(not(feature = "simd"))]
    return math_checked_divide_op(left, right, |a, b| a.div_checked(b));
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
pub fn divide_opt<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn divide_dyn(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| {
                    if b.is_zero() {
                        Err(ArrowError::DivideByZero)
                    } else {
                        Ok(a.div_wrapping(b))
                    }
                },
                math_divide_checked_op_dict
            )
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_checked_divide_op(left, right, |a, b| {
                        if b.is_zero() {
                            Err(ArrowError::DivideByZero)
                        } else {
                            Ok(a.div_wrapping(b))
                        }
                    }).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `divide_dyn` instead.
pub fn divide_dyn_checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| a.div_checked(b),
                math_divide_checked_op_dict
            )
        }
        _ => {
            downcast_primitive_array!(
                (left, right) => {
                    math_checked_divide_op(left, right, |a, b| a.div_checked(b)).map(|a| Arc::new(a) as ArrayRef)
                }
                _ => Err(ArrowError::CastError(format!(
                    "Unsupported data type {}, {}",
                    left.data_type(), right.data_type()
                )))
            )
        }
    }
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
pub fn divide_dyn_opt(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Dictionary(_, _) => {
            typed_dict_math_op!(
                left,
                right,
                |a, b| {
                    if b.is_zero() {
                        None
                    } else {
                        Some(a.div_wrapping(b))
                    }
                },
                math_divide_safe_op_dict
            )
        }
        _ => {
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
    }
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
pub fn divide<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    // TODO: This is incorrect as div_wrapping has side-effects for integer types
    // and so may panic on null values (#2647)
    math_op(left, right, |a, b| a.div_wrapping(b))
}

/// Modulus every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
pub fn modulus_scalar<T>(
    array: &PrimitiveArray<T>,
    modulo: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    if modulo.is_zero() {
        return Err(ArrowError::DivideByZero);
    }

    Ok(unary(array, |a| a.mod_wrapping(modulo)))
}

/// Divide every value in an array by a scalar. If any value in the array is null then the
/// result is also null. If the scalar is zero then the result of this operation will be
/// `Err(ArrowError::DivideByZero)`.
pub fn divide_scalar<T>(
    array: &PrimitiveArray<T>,
    divisor: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn divide_scalar_dyn<T>(array: &dyn Array, divisor: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn divide_scalar_checked_dyn<T>(
    array: &dyn Array,
    divisor: T::Native,
) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
pub fn divide_scalar_opt_dyn<T>(array: &dyn Array, divisor: T::Native) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
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
mod tests {
    use super::*;
    use crate::array::Int32Array;
    use crate::compute::{binary_mut, try_binary_mut, try_unary_mut, unary_mut};
    use crate::datatypes::{Date64Type, Int32Type, Int8Type};
    use arrow_buffer::i256;
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
    }

    #[test]
    fn test_date32_day_time_add() {
        let a = Date32Array::from(vec![Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        )]);
        let b = IntervalDayTimeArray::from(vec![IntervalDayTimeType::make_value(1, 2)]);
        let c = add_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
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
        let c = c.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(
            c.value(0),
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
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
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
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
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
        let c = c.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(
            c.value(0),
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_primitive_array_add_dyn_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null();
        builder.append(10).unwrap();
        let b = builder.finish();

        let c = add_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(19, c.value(4));
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_primitive_array_subtract_dyn_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(15).unwrap();
        builder.append(8).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(20).unwrap();
        let a = builder.finish();

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null();
        builder.append(10).unwrap();
        let b = builder.finish();

        let c = subtract_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(9, c.value(0));
        assert_eq!(1, c.value(1));
        assert_eq!(-1, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(10, c.value(4));
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_primitive_array_multiply_dyn_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null();
        builder.append(10).unwrap();
        let b = builder.finish();

        let c = multiply_dyn(&a, &b).unwrap();
        let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(30, c.value(0));
        assert_eq!(42, c.value(1));
        assert_eq!(56, c.value(2));
        assert!(c.is_null(3));
        assert_eq!(90, c.value(4));
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_primitive_array_divide_dyn_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(15).unwrap();
        builder.append(6).unwrap();
        builder.append(1).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        let a = builder.finish();

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append(3).unwrap();
        builder.append(1).unwrap();
        builder.append_null();
        builder.append(3).unwrap();
        let b = builder.finish();

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
    fn test_int_array_modulus() {
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
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: DivideByZero"
    )]
    fn test_int_array_modulus_divide_by_zero() {
        let a = Int32Array::from(vec![1]);
        let b = Int32Array::from(vec![0]);
        modulus(&a, &b).unwrap();
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
        assert_eq!("DivideByZero", format!("{:?}", e));
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
    fn test_int_array_modulus_scalar() {
        let a = Int32Array::from(vec![15, 14, 9, 8, 1]);
        let b = 3;
        let c = modulus_scalar(&a, b).unwrap();
        let expected = Int32Array::from(vec![0, 2, 0, 2, 1]);
        assert_eq!(c, expected);
    }

    #[test]
    fn test_int_array_modulus_scalar_sliced() {
        let a = Int32Array::from(vec![Some(15), None, Some(9), Some(8), None]);
        let a = a.slice(1, 4);
        let a = as_primitive_array(&a);
        let actual = modulus_scalar(a, 3).unwrap();
        let expected = Int32Array::from(vec![None, Some(0), Some(2), None]);
        assert_eq!(actual, expected);
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
        assert_eq!(0, result.value(0))
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_int_array_divide_dyn_by_zero_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(15).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(0).unwrap();
        let b = builder.finish();

        divide_dyn(&a, &b).unwrap();
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    #[cfg(feature = "dyn_arith_dict")]
    fn test_f32_dict_array_divide_dyn_by_zero() {
        use crate::datatypes::Float32Type;
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Float32Type>::with_capacity(1, 1);
        builder.append(1.5).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Float32Type>::with_capacity(1, 1);
        builder.append(0.0).unwrap();
        let b = builder.finish();

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
    fn test_f32_array_modulus_by_zero() {
        let a = Float32Array::from(vec![1.5]);
        let b = Float32Array::from(vec![0.0]);
        modulus(&a, &b).unwrap();
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
        let a: Int64Array = (0..100).into_iter().map(Some).collect();
        let actual = negate(&a).unwrap();
        let expected: Int64Array = (0..100).into_iter().map(|i| Some(-i)).collect();
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_dictionary_add_dyn_wrapping_overflow() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(2, 2);
        builder.append(i32::MAX).unwrap();
        builder.append(i32::MIN).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(2, 2);
        builder.append(1).unwrap();
        builder.append(1).unwrap();
        let b = builder.finish();

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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_dictionary_subtract_dyn_wrapping_overflow() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(-2).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(i32::MAX).unwrap();
        let b = builder.finish();

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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_dictionary_mul_dyn_wrapping_overflow() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(10).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(i32::MAX).unwrap();
        let b = builder.finish();

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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_dictionary_div_dyn_wrapping_overflow() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(i32::MIN).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(-1).unwrap();
        let b = builder.finish();

        let wrapped = divide_dyn(&a, &b).unwrap();
        let expected = Arc::new(Int32Array::from(vec![-2147483648])) as ArrayRef;
        assert_eq!(&expected, &wrapped);

        let overflow = divide_dyn_checked(&a, &b);
        overflow.expect_err("overflow should be detected");
    }

    #[test]
    #[cfg(feature = "dyn_arith_dict")]
    fn test_div_dyn_opt_overflow_division_by_zero() {
        let a = Int32Array::from(vec![i32::MIN]);
        let b = Int32Array::from(vec![0]);

        let division_by_zero = divide_dyn_opt(&a, &b);
        let expected = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        assert_eq!(&expected, &division_by_zero.unwrap());

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(i32::MIN).unwrap();
        let a = builder.finish();

        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(1, 1);
        builder.append(0).unwrap();
        let b = builder.finish();

        let division_by_zero = divide_dyn_opt(&a, &b);
        assert_eq!(&expected, &division_by_zero.unwrap());
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

        let null_buffer = null_buffer_builder.finish();

        // `count_set_bits_offset` takes len in bits as parameter.
        assert_eq!(null_buffer.count_set_bits_offset(0, 13), 0);

        let mut data_buffer_builder = BufferBuilder::<i32>::new(13);
        data_buffer_builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let data_buffer = data_buffer_builder.finish();

        let arg1: Int32Array = ArrayDataBuilder::new(DataType::Int32)
            .len(13)
            .null_count(13)
            .buffers(vec![data_buffer])
            .null_bit_buffer(Some(null_buffer))
            .build()
            .unwrap()
            .into();

        assert_eq!(arg1.null_count(), 13);

        let mut data_buffer_builder = BufferBuilder::<i32>::new(13);
        data_buffer_builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let data_buffer = data_buffer_builder.finish();

        let arg2: Int32Array = ArrayDataBuilder::new(DataType::Int32)
            .len(13)
            .null_count(0)
            .buffers(vec![data_buffer])
            .null_bit_buffer(None)
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
    #[cfg(feature = "dyn_arith_dict")]
    fn test_dict_decimal() {
        let values = Decimal128Array::from_iter_values([0, 1, 2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::try_new(&keys, &values).unwrap();

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let result = add_dyn(&array1, &array2).unwrap();
        let expected =
            Arc::new(Decimal128Array::from(vec![8, 9, 2, 8, 6, 5])) as ArrayRef;
        assert_eq!(&result, &expected);

        let result = subtract_dyn(&array1, &array2).unwrap();
        let expected =
            Arc::new(Decimal128Array::from(vec![-6, -5, 8, 0, 0, -5])) as ArrayRef;
        assert_eq!(&result, &expected);

        let values = Decimal256Array::from_iter_values([
            i256::from_i128(0),
            i256::from_i128(1),
            i256::from_i128(2),
            i256::from_i128(3),
            i256::from_i128(4),
            i256::from_i128(5),
        ]);
        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(5), Some(4), Some(3), None]);
        let array1 = DictionaryArray::try_new(&keys, &values).unwrap();

        let values = Decimal256Array::from_iter_values([
            i256::from_i128(7),
            i256::from_i128(-3),
            i256::from_i128(4),
            i256::from_i128(3),
            i256::from_i128(5),
        ]);
        let keys =
            Int8Array::from(vec![Some(0_i8), Some(0), None, Some(2), Some(3), Some(4)]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let result = add_dyn(&array1, &array2).unwrap();
        let expected = Arc::new(Decimal256Array::from(vec![
            Some(i256::from_i128(8)),
            None,
            None,
            Some(i256::from_i128(8)),
            Some(i256::from_i128(6)),
            None,
        ])) as ArrayRef;

        assert_eq!(&result, &expected);

        let result = subtract_dyn(&array1, &array2).unwrap();
        let expected = Arc::new(Decimal256Array::from(vec![
            Some(i256::from_i128(-6)),
            None,
            None,
            Some(i256::from_i128(0)),
            Some(i256::from_i128(0)),
            None,
        ])) as ArrayRef;
        assert_eq!(&result, &expected);
    }
}
