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

//! Defines basic comparison kernels for [`PrimitiveArray`]s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use regex::Regex;
use std::collections::HashMap;

use crate::array::*;
use crate::buffer::{Buffer, MutableBuffer};
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{ArrowNumericType, DataType};
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

/// Helper function to perform boolean lambda function on values from two arrays, this
/// version does not attempt to use SIMD.
macro_rules! compare_op {
    ($left: expr, $right:expr, $op:expr) => {{
        if $left.len() != $right.len() {
            return Err(ArrowError::ComputeError(
                "Cannot perform comparison operation on arrays of different length"
                    .to_string(),
            ));
        }

        let null_bit_buffer =
            combine_option_bitmap($left.data_ref(), $right.data_ref(), $left.len())?;

        let comparison = (0..$left.len()).map(|i| $op($left.value(i), $right.value(i)));
        // same size as $left.len() and $right.len()
        let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(comparison) };

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from(buffer)],
            vec![],
        );
        Ok(BooleanArray::from(data))
    }};
}

macro_rules! compare_op_primitive {
    ($left: expr, $right:expr, $op:expr) => {{
        if $left.len() != $right.len() {
            return Err(ArrowError::ComputeError(
                "Cannot perform comparison operation on arrays of different length"
                    .to_string(),
            ));
        }

        let null_bit_buffer =
            combine_option_bitmap($left.data_ref(), $right.data_ref(), $left.len())?;

        let mut values = MutableBuffer::from_len_zeroed(($left.len() + 7) / 8);
        let lhs_chunks_iter = $left.values().chunks_exact(8);
        let lhs_remainder = lhs_chunks_iter.remainder();
        let rhs_chunks_iter = $right.values().chunks_exact(8);
        let rhs_remainder = rhs_chunks_iter.remainder();
        let chunks = $left.len() / 8;

        values[..chunks]
            .iter_mut()
            .zip(lhs_chunks_iter)
            .zip(rhs_chunks_iter)
            .for_each(|((byte, lhs), rhs)| {
                lhs.iter()
                    .zip(rhs.iter())
                    .enumerate()
                    .for_each(|(i, (&lhs, &rhs))| {
                        *byte |= if $op(lhs, rhs) { 1 << i } else { 0 };
                    });
            });

        if !lhs_remainder.is_empty() {
            let last = &mut values[chunks];
            lhs_remainder
                .iter()
                .zip(rhs_remainder.iter())
                .enumerate()
                .for_each(|(i, (&lhs, &rhs))| {
                    *last |= if $op(lhs, rhs) { 1 << i } else { 0 };
                });
        };
        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from(values)],
            vec![],
        );
        Ok(BooleanArray::from(data))
    }};
}

macro_rules! compare_op_scalar {
    ($left: expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left.data().null_buffer().cloned();

        let comparison = (0..$left.len()).map(|i| $op($left.value(i), $right));
        // same as $left.len()
        let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(comparison) };

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from(buffer)],
            vec![],
        );
        Ok(BooleanArray::from(data))
    }};
}

macro_rules! compare_op_scalar_primitive {
    ($left: expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left.data().null_buffer().cloned();

        let mut values = MutableBuffer::from_len_zeroed(($left.len() + 7) / 8);
        let lhs_chunks_iter = $left.values().chunks_exact(8);
        let lhs_remainder = lhs_chunks_iter.remainder();
        let chunks = $left.len() / 8;

        values[..chunks]
            .iter_mut()
            .zip(lhs_chunks_iter)
            .for_each(|(byte, chunk)| {
                chunk.iter().enumerate().for_each(|(i, &c_i)| {
                    *byte |= if $op(c_i, $right) { 1 << i } else { 0 };
                });
            });
        if !lhs_remainder.is_empty() {
            let last = &mut values[chunks];
            lhs_remainder.iter().enumerate().for_each(|(i, &lhs)| {
                *last |= if $op(lhs, $right) { 1 << i } else { 0 };
            });
        };

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from(values)],
            vec![],
        );
        Ok(BooleanArray::from(data))
    }};
}

/// Evaluate `op(left, right)` for [`PrimitiveArray`]s using a specified
/// comparison function.
pub fn no_simd_compare_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op_primitive!(left, right, op)
}

/// Evaluate `op(left, right)` for [`PrimitiveArray`] and scalar using
/// a specified comparison function.
pub fn no_simd_compare_op_scalar<T, F>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op_scalar_primitive!(left, right, op)
}

/// Perform SQL `left LIKE right` operation on [`StringArray`] / [`LargeStringArray`].
///
/// There are two wildcards supported with the LIKE operator:
///
/// 1. `%` - The percent sign represents zero, one, or multiple characters
/// 2. `_` - The underscore represents a single character
///
/// For example:
/// ```
/// use arrow::array::{StringArray, BooleanArray};
/// use arrow::compute::like_utf8;
///
/// let strings = StringArray::from(vec!["Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = StringArray::from(vec!["A%", "B%", "A.", "A."]);
///
/// let result = like_utf8(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, false, true]));
/// ```
pub fn like_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(re.is_match(haystack));
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

/// Perform SQL `left LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn like_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    let null_bit_buffer = left.data().null_buffer().cloned();
    let bytes = bit_util::ceil(left.len(), 8);
    let mut bool_buf = MutableBuffer::from_len_zeroed(bytes);
    let bool_slice = bool_buf.as_slice_mut();

    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        for i in 0..left.len() {
            if left.value(i) == right {
                bit_util::set_bit(bool_slice, i);
            }
        }
    } else if right.ends_with('%') && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use starts_with
        let starts_with = &right[..right.len() - 1];
        for i in 0..left.len() {
            if left.value(i).starts_with(starts_with) {
                bit_util::set_bit(bool_slice, i);
            }
        }
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        let ends_with = &right[1..];
        for i in 0..left.len() {
            if left.value(i).ends_with(ends_with) {
                bit_util::set_bit(bool_slice, i);
            }
        }
    } else {
        let re_pattern = right.replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })?;

        for i in 0..left.len() {
            let haystack = left.value(i);
            if re.is_match(haystack) {
                bit_util::set_bit(bool_slice, i);
            }
        }
    };

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![bool_buf.into()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Perform SQL `left NOT LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(!re.is_match(haystack));
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Perform SQL `left NOT LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    let null_bit_buffer = left.data().null_buffer().cloned();
    let mut result = BooleanBufferBuilder::new(left.len());

    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        for i in 0..left.len() {
            result.append(left.value(i) != right);
        }
    } else if right.ends_with('%') && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use ends_with
        for i in 0..left.len() {
            result.append(!left.value(i).starts_with(&right[..right.len() - 1]));
        }
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use starts_with
        for i in 0..left.len() {
            result.append(!left.value(i).ends_with(&right[1..]));
        }
    } else {
        let re_pattern = right.replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })?;
        for i in 0..left.len() {
            let haystack = left.value(i);
            result.append(!re.is_match(haystack));
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn eq_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a == b)
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn eq_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a == b)
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn neq_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a != b)
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn neq_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a != b)
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn lt_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a < b)
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn lt_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a < b)
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn lt_eq_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a <= b)
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn lt_eq_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a <= b)
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn gt_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a > b)
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn gt_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a > b)
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn gt_eq_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a >= b)
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn gt_eq_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a >= b)
}

/// Helper function to perform boolean lambda function on values from two arrays using
/// SIMD.
#[cfg(feature = "simd")]
fn simd_compare_op<T, SIMD_OP, SCALAR_OP>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    SIMD_OP: Fn(T::Simd, T::Simd) -> T::SimdMask,
    SCALAR_OP: Fn(T::Native, T::Native) -> bool,
{
    use std::borrow::BorrowMut;

    let len = left.len();
    if len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer = combine_option_bitmap(left.data_ref(), right.data_ref(), len)?;

    let lanes = T::lanes();
    let buffer_size = bit_util::ceil(len, 8);
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    // this is currently the case for all our datatypes and allows us to always append full bytes
    assert!(
        lanes % 8 == 0,
        "Number of vector lanes must be multiple of 8"
    );
    let mut left_chunks = left.values().chunks_exact(lanes);
    let mut right_chunks = right.values().chunks_exact(lanes);

    let result_remainder = left_chunks
        .borrow_mut()
        .zip(right_chunks.borrow_mut())
        .fold(
            result.typed_data_mut(),
            |result_slice, (left_slice, right_slice)| {
                let simd_left = T::load(left_slice);
                let simd_right = T::load(right_slice);
                let simd_result = simd_op(simd_left, simd_right);

                let bitmask = T::mask_to_u64(&simd_result);
                let bytes = bitmask.to_le_bytes();
                &result_slice[0..lanes / 8].copy_from_slice(&bytes[0..lanes / 8]);

                &mut result_slice[lanes / 8..]
            },
        );

    let left_remainder = left_chunks.remainder();
    let right_remainder = right_chunks.remainder();

    assert_eq!(left_remainder.len(), right_remainder.len());

    let remainder_bitmask = left_remainder
        .iter()
        .zip(right_remainder.iter())
        .enumerate()
        .fold(0_u64, |mut mask, (i, (scalar_left, scalar_right))| {
            let bit = if scalar_op(*scalar_left, *scalar_right) {
                1_u64
            } else {
                0_u64
            };
            mask |= bit << i;
            mask
        });
    let remainder_mask_as_bytes =
        &remainder_bitmask.to_le_bytes()[0..bit_util::ceil(left_remainder.len(), 8)];
    result_remainder.copy_from_slice(remainder_mask_as_bytes);

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        null_bit_buffer,
        0,
        vec![result.into()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Helper function to perform boolean lambda function on values from an array and a scalar value using
/// SIMD.
#[cfg(feature = "simd")]
fn simd_compare_op_scalar<T, SIMD_OP, SCALAR_OP>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    SIMD_OP: Fn(T::Simd, T::Simd) -> T::SimdMask,
    SCALAR_OP: Fn(T::Native, T::Native) -> bool,
{
    use std::borrow::BorrowMut;

    let len = left.len();

    let lanes = T::lanes();
    let buffer_size = bit_util::ceil(len, 8);
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    // this is currently the case for all our datatypes and allows us to always append full bytes
    assert!(
        lanes % 8 == 0,
        "Number of vector lanes must be multiple of 8"
    );
    let mut left_chunks = left.values().chunks_exact(lanes);
    let simd_right = T::init(right);

    let result_remainder = left_chunks.borrow_mut().fold(
        result.typed_data_mut(),
        |result_slice, left_slice| {
            let simd_left = T::load(left_slice);
            let simd_result = simd_op(simd_left, simd_right);

            let bitmask = T::mask_to_u64(&simd_result);
            let bytes = bitmask.to_le_bytes();
            &result_slice[0..lanes / 8].copy_from_slice(&bytes[0..lanes / 8]);

            &mut result_slice[lanes / 8..]
        },
    );

    let left_remainder = left_chunks.remainder();

    let remainder_bitmask =
        left_remainder
            .iter()
            .enumerate()
            .fold(0_u64, |mut mask, (i, scalar_left)| {
                let bit = if scalar_op(*scalar_left, right) {
                    1_u64
                } else {
                    0_u64
                };
                mask |= bit << i;
                mask
            });
    let remainder_mask_as_bytes =
        &remainder_bitmask.to_le_bytes()[0..bit_util::ceil(left_remainder.len(), 8)];
    result_remainder.copy_from_slice(remainder_mask_as_bytes);

    let null_bit_buffer = left
        .data_ref()
        .null_buffer()
        .map(|b| b.bit_slice(left.offset(), left.len()));

    // null count is the same as in the input since the right side of the scalar comparison cannot be null
    let null_count = left.null_count();

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        Some(null_count),
        null_bit_buffer,
        0,
        vec![result.into()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Perform `left == right` operation on two arrays.
pub fn eq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a == b);
}

/// Perform `left == right` operation on an array and a scalar value.
pub fn eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a == b);
}

/// Perform `left != right` operation on two arrays.
pub fn neq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a != b);
}

/// Perform `left != right` operation on an array and a scalar value.
pub fn neq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a != b);
}

/// Perform `left < right` operation on two arrays. Null values are less than non-null
/// values.
pub fn lt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::lt, |a, b| a < b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a < b);
}

/// Perform `left < right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::lt, |a, b| a < b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a < b);
}

/// Perform `left <= right` operation on two arrays. Null values are less than non-null
/// values.
pub fn lt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::le, |a, b| a <= b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a <= b);
}

/// Perform `left <= right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::le, |a, b| a <= b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a <= b);
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::gt, |a, b| a > b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a > b);
}

/// Perform `left > right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::gt, |a, b| a > b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a > b);
}

/// Perform `left >= right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::ge, |a, b| a >= b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a >= b);
}

/// Perform `left >= right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::ge, |a, b| a >= b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a >= b);
}

/// Checks if a [`GenericListArray`] contains a value in the [`PrimitiveArray`]
pub fn contains<T, OffsetSize>(
    left: &PrimitiveArray<T>,
    right: &GenericListArray<OffsetSize>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(left.data_ref(), right.data_ref(), left_len)? {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.as_slice();

    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = bool_buf.as_slice_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        0,
        vec![bool_buf.into()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

/// Checks if a [`GenericListArray`] contains a value in the [`GenericStringArray`]
pub fn contains_utf8<OffsetSize>(
    left: &GenericStringArray<OffsetSize>,
    right: &ListArray,
) -> Result<BooleanArray>
where
    OffsetSize: StringOffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(left.data_ref(), right.data_ref(), left_len)? {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.as_slice();

    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = &mut bool_buf;

    for i in 0..left_len {
        // contains(null, null) = false
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list
                .as_any()
                .downcast_ref::<GenericStringArray<OffsetSize>>()
                .unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        0,
        vec![bool_buf.into()],
        vec![],
    );
    Ok(BooleanArray::from(data))
}

// create a buffer and fill it with valid bits
#[inline]
fn new_all_set_buffer(len: usize) -> Buffer {
    let buffer = MutableBuffer::new(len);
    let buffer = buffer.with_bitset(len, true);

    buffer.into()
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::Int8Type;
    use crate::{array::Int32Array, array::Int64Array, datatypes::Field};

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64 {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let b = Int64Array::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    /// Evaluate `KERNEL` with one vectors and one scalar as inputs and assert against the expected output.
    /// `A_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let c = $KERNEL(&a, $B).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    #[test]
    fn test_primitive_array_eq() {
        cmp_i64!(
            eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_scalar() {
        cmp_i64_scalar!(
            eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_with_slice() {
        let a = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let b_slice = b.slice(5, 5);
        let c = b_slice.as_any().downcast_ref().unwrap();
        let d = eq(&c, &a).unwrap();
        assert!(d.value(0));
        assert!(d.value(1));
        assert!(d.value(2));
        assert!(!d.value(3));
        assert!(d.value(4));
    }

    #[test]
    fn test_primitive_array_neq() {
        cmp_i64!(
            neq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_neq_scalar() {
        cmp_i64_scalar!(
            neq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt() {
        cmp_i64!(
            lt,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar() {
        cmp_i64_scalar!(
            lt_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_nulls() {
        cmp_i64!(
            lt,
            vec![None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            vec![None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar_nulls() {
        cmp_i64_scalar!(
            lt_scalar,
            vec![None, Some(1), Some(2), Some(3), None, Some(1), Some(2), Some(3), Some(2), None],
            2,
            vec![None, Some(true), Some(false), Some(false), None, Some(true), Some(false), Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq() {
        cmp_i64!(
            lt_eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar() {
        cmp_i64_scalar!(
            lt_eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_nulls() {
        cmp_i64!(
            lt_eq,
            vec![None, None, Some(1), None, None, Some(1), None, None, Some(1)],
            vec![None, Some(1), Some(0), None, Some(1), Some(2), None, None, Some(3)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            lt_eq_scalar,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt() {
        cmp_i64!(
            gt,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar() {
        cmp_i64_scalar!(
            gt_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_nulls() {
        cmp_i64!(
            gt,
            vec![None, None, Some(1), None, None, Some(2), None, None, Some(3)],
            vec![None, Some(1), Some(1), None, Some(1), Some(1), None, Some(1), Some(1)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar_nulls() {
        cmp_i64_scalar!(
            gt_scalar,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(false), Some(true), None, Some(false), Some(true), None, Some(false), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq() {
        cmp_i64!(
            gt_eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar() {
        cmp_i64_scalar!(
            gt_eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        cmp_i64!(
            gt_eq,
            vec![None, None, Some(1), None, Some(1), Some(2), None, None, Some(1)],
            vec![None, Some(1), None, None, Some(1), Some(1), None, Some(2), Some(2)],
            vec![None, None, None, None, Some(true), Some(true), None, None, Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            gt_eq_scalar,
            vec![None, Some(1), Some(2), None, Some(2), Some(3), None, Some(3), Some(4)],
            2,
            vec![None, Some(false), Some(true), None, Some(true), Some(true), None, Some(true), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_compare_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b: Int32Array = (100..200).map(Some).collect();
        let b = b.slice(50, 50);
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = lt(&a, &b).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_compare_scalar_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = lt_scalar(&a, 200).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_length_of_result_buffer() {
        // `item_count` is chosen to not be a multiple of the number of SIMD lanes for this
        // type (`Int8Type`), 64.
        let item_count = 130;

        let select_mask: BooleanArray = vec![true; item_count].into();

        let array_a: PrimitiveArray<Int8Type> = vec![1; item_count].into();
        let array_b: PrimitiveArray<Int8Type> = vec![2; item_count].into();
        let result_mask = gt_eq(&array_a, &array_b).unwrap();

        assert_eq!(
            result_mask.data().buffers()[0].len(),
            select_mask.data().buffers()[0].len()
        );
    }

    // Expected behaviour:
    // contains(1, [1, 2, null]) = true
    // contains(3, [1, 2, null]) = false
    // contains(null, [1, 2, null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains() {
        let value_data = Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            None,
            Some(7),
        ])
        .data()
        .clone();
        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 6, 6, 9]);
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00001011]))
            .build();

        //  [[0, 1, 2], [3, 4, 5], null, [6, null, 7]]
        let list_array = LargeListArray::from(list_data);

        let nulls = Int32Array::from(vec![None, None, None, None]);
        let nulls_result = contains(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = Int32Array::from(vec![Some(0), Some(0), Some(0), Some(0)]);
        let values_result = contains(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, false, false, false]),
        );
    }

    // Expected behaviour:
    // contains("ab", ["ab", "cd", null]) = true
    // contains("ef", ["ab", "cd", null]) = false
    // contains(null, ["ab", "cd", null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains_utf8() {
        let values_builder = StringBuilder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_value("Lorem").unwrap();
        builder.values().append_value("ipsum").unwrap();
        builder.values().append_null().unwrap();
        builder.append(true).unwrap();
        builder.values().append_value("sit").unwrap();
        builder.values().append_value("amet").unwrap();
        builder.values().append_value("Lorem").unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value("ipsum").unwrap();
        builder.append(true).unwrap();

        //  [["Lorem", "ipsum", null], ["sit", "amet", "Lorem"], null, ["ipsum"]]
        // value_offsets = [0, 3, 6, 6]
        let list_array = builder.finish();

        let nulls = StringArray::from(vec![None, None, None, None]);
        let nulls_result = contains_utf8(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = StringArray::from(vec![
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
        ]);
        let values_result = contains_utf8(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, true, false, false]),
        );
    }

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }

                let left = LargeStringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        like_utf8,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        like_utf8_scalar,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        like_utf8_scalar,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        like_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        like_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        like_utf8_scalar,
        vec![false, true, false, false]
    );

    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_utf8,
        vec![false, false, false, true, true, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        nlike_utf8_scalar,
        vec![false, false, true, true]
    );

    test_utf8!(
        test_utf8_array_eq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        eq_utf8,
        vec![true, false, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_eq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        eq_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        nlike_utf8_scalar,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        nlike_utf8_scalar,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        nlike_utf8_scalar,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nlike_utf8_scalar,
        vec![true, false, true, true]
    );

    test_utf8!(
        test_utf8_array_neq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        neq_utf8,
        vec![false, true, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_neq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        neq_utf8_scalar,
        vec![false, true, true, true]
    );

    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        vec![true, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_eq_utf8_scalar,
        vec![true, true, true, false]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        vec![false, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_utf8_scalar,
        vec![false, false, false, true]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        vec![false, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_eq_utf8_scalar,
        vec![false, false, true, true]
    );
}
