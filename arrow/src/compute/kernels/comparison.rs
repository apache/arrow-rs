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

use crate::array::*;
use crate::buffer::{bitwise_bin_op_helper, buffer_unary_not, Buffer, MutableBuffer};
use crate::compute::binary_boolean_kernel;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{
    ArrowNumericType, DataType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::error::{ArrowError, Result};
use crate::util::bit_util;
use regex::{escape, Regex};
use std::any::type_name;
use std::collections::HashMap;

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

        // Safety:
        // `i < $left.len()` and $left.len() == $right.len()
        let comparison = (0..$left.len())
            .map(|i| unsafe { $op($left.value_unchecked(i), $right.value_unchecked(i)) });
        // same size as $left.len() and $right.len()
        let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(comparison) };

        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                $left.len(),
                None,
                null_bit_buffer,
                0,
                vec![Buffer::from(buffer)],
                vec![],
            )
        };
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
        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                $left.len(),
                None,
                null_bit_buffer,
                0,
                vec![Buffer::from(values)],
                vec![],
            )
        };
        Ok(BooleanArray::from(data))
    }};
}

macro_rules! compare_op_scalar {
    ($left:expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left
            .data()
            .null_buffer()
            .map(|b| b.bit_slice($left.offset(), $left.len()));

        // Safety:
        // `i < $left.len()`
        let comparison =
            (0..$left.len()).map(|i| unsafe { $op($left.value_unchecked(i), $right) });
        // same as $left.len()
        let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(comparison) };

        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                $left.len(),
                None,
                null_bit_buffer,
                0,
                vec![Buffer::from(buffer)],
                vec![],
            )
        };
        Ok(BooleanArray::from(data))
    }};
}

macro_rules! compare_op_scalar_primitive {
    ($left: expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left
            .data()
            .null_buffer()
            .map(|b| b.bit_slice($left.offset(), $left.len()));

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

        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                $left.len(),
                None,
                null_bit_buffer,
                0,
                vec![Buffer::from(values)],
                vec![],
            )
        };
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

fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

/// Evaluate regex `op(left)` matching `right` on [`StringArray`] / [`LargeStringArray`]
///
/// If `negate_regex` is true, the regex expression will be negated. (for example, with `not like`)
fn regex_like<OffsetSize, F>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
    negate_regex: bool,
    op: F,
) -> Result<BooleanArray>
where
    OffsetSize: StringOffsetSizeTrait,
    F: Fn(&str) -> Result<Regex>,
{
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
            let re_pattern = escape(pat).replace("%", ".*").replace("_", ".");
            let re = op(&re_pattern)?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(if negate_regex {
            !re.is_match(haystack)
        } else {
            re.is_match(haystack)
        });
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.finish()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
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
/// let patterns = StringArray::from(vec!["A%", "B%", "A.", "A_"]);
///
/// let result = like_utf8(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, false, true]));
/// ```
pub fn like_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })
    })
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
        let re_pattern = escape(right).replace("%", ".*").replace("_", ".");
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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![bool_buf.into()],
            vec![],
        )
    };
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
    regex_like(left, right, true, |re_pattern| {
        Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })
    })
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
        let re_pattern = escape(right).replace("%", ".*").replace("_", ".");
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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.finish()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Perform SQL `left ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn ilike_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {}",
                e
            ))
        })
    })
}

/// Perform SQL `left ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn ilike_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray> {
    let null_bit_buffer = left.data().null_buffer().cloned();
    let mut result = BooleanBufferBuilder::new(left.len());

    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        for i in 0..left.len() {
            result.append(left.value(i) == right);
        }
    } else if right.ends_with('%') && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use ends_with
        for i in 0..left.len() {
            result.append(
                left.value(i)
                    .to_uppercase()
                    .starts_with(&right[..right.len() - 1].to_uppercase()),
            );
        }
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use starts_with
        for i in 0..left.len() {
            result.append(
                left.value(i)
                    .to_uppercase()
                    .ends_with(&right[1..].to_uppercase()),
            );
        }
    } else {
        let re_pattern = escape(right).replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {}",
                e
            ))
        })?;
        for i in 0..left.len() {
            let haystack = left.value(i);
            result.append(re.is_match(haystack));
        }
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.finish()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] / [`LargeStringArray`].
/// If `regex_array` element has an empty value, the corresponding result value is always true.
///
/// `flags_array` are optional [`StringArray`] / [`LargeStringArray`] flag, which allow
/// special search modes, such as case insensitive and multi-line mode.
/// See the documentation [here](https://docs.rs/regex/1.5.4/regex/#grouping-and-flags)
/// for more information.
pub fn regexp_is_match_utf8<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &GenericStringArray<OffsetSize>,
    flags_array: Option<&GenericStringArray<OffsetSize>>,
) -> Result<BooleanArray> {
    if array.len() != regex_array.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let null_bit_buffer =
        combine_option_bitmap(array.data_ref(), regex_array.data_ref(), array.len())?;

    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let mut result = BooleanBufferBuilder::new(array.len());

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(regex_array.iter().zip(flags.iter()).map(
            |(pattern, flags)| {
                pattern.map(|pattern| match flags {
                    Some(flag) => format!("(?{}){}", flag, pattern),
                    None => pattern.to_string(),
                })
            },
        )) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };

    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                // Required for Postgres compatibility:
                // SELECT 'foobarbequebaz' ~ ''); = true
                (Some(_), Some(pattern)) if pattern == *"" => {
                    result.append(true);
                }
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re.clone(),
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Regular expression did not compile: {:?}",
                                    e
                                ))
                            })?;
                            patterns.insert(pattern, re.clone());
                            re
                        }
                    };
                    result.append(re.is_match(value));
                }
                _ => result.append(false),
            }
            Ok(())
        })
        .collect::<Result<Vec<()>>>()?;

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.finish()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`regexp_is_match_utf8`] for more details.
pub fn regexp_is_match_utf8_scalar<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex: &str,
    flag: Option<&str>,
) -> Result<BooleanArray> {
    let null_bit_buffer = array.data().null_buffer().cloned();
    let mut result = BooleanBufferBuilder::new(array.len());

    let pattern = match flag {
        Some(flag) => format!("(?{}){}", flag, regex),
        None => regex.to_string(),
    };
    if pattern.is_empty() {
        result.append_n(array.len(), true);
    } else {
        let re = Regex::new(pattern.as_str()).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Regular expression did not compile: {:?}",
                e
            ))
        })?;
        for i in 0..array.len() {
            let value = array.value(i);
            result.append(re.is_match(value));
        }
    }

    let buffer = result.finish();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };
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

#[inline]
fn binary_boolean_op<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Copy + Fn(u64, u64) -> u64,
{
    binary_boolean_kernel(
        left,
        right,
        |left: &Buffer,
         left_offset_in_bits: usize,
         right: &Buffer,
         right_offset_in_bits: usize,
         len_in_bits: usize| {
            bitwise_bin_op_helper(
                left,
                left_offset_in_bits,
                right,
                right_offset_in_bits,
                len_in_bits,
                op,
            )
        },
    )
}

/// Perform `left == right` operation on [`BooleanArray`]
pub fn eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| !(a ^ b))
}

/// Perform `left != right` operation on [`BooleanArray`]
pub fn neq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| (a ^ b))
}

/// Perform `left < right` operation on [`BooleanArray`]
pub fn lt_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| ((!a) & b))
}

/// Perform `left <= right` operation on [`BooleanArray`]
pub fn lt_eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| !(a & (!b)))
}

/// Perform `left > right` operation on [`BooleanArray`]
pub fn gt_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| (a & (!b)))
}

/// Perform `left >= right` operation on [`BooleanArray`]
pub fn gt_eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_op(left, right, |a, b| !((!a) & b))
}

/// Perform `left == right` operation on [`BooleanArray`] and a scalar
pub fn eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    let len = left.len();
    let left_offset = left.offset();

    let values = if right {
        left.values().bit_slice(left_offset, len)
    } else {
        buffer_unary_not(left.values(), left.offset(), left.len())
    };

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            left.data_ref()
                .null_bitmap()
                .as_ref()
                .map(|b| b.bits.bit_slice(left_offset, len)),
            0,
            vec![values],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

/// Perform `left < right` operation on [`BooleanArray`] and a scalar
pub fn lt_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a: bool, b: bool| !a & b)
}

/// Perform `left <= right` operation on [`BooleanArray`] and a scalar
pub fn lt_eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a <= b)
}

/// Perform `left > right` operation on [`BooleanArray`] and a scalar
pub fn gt_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a: bool, b: bool| a & !b)
}

/// Perform `left >= right` operation on [`BooleanArray`] and a scalar
pub fn gt_eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a >= b)
}

/// Perform `left != right` operation on [`BooleanArray`] and a scalar
pub fn neq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray> {
    eq_bool_scalar(left, !right)
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
    assert_eq!(lanes % 8, 0, "Number of vector lanes must be multiple of 8");
    let mut left_chunks = left.values().chunks_exact(lanes);
    let mut right_chunks = right.values().chunks_exact(lanes);

    // safety: result is newly created above, always written as a T below
    let result_chunks = unsafe { result.typed_data_mut() };
    let result_remainder = left_chunks
        .borrow_mut()
        .zip(right_chunks.borrow_mut())
        .fold(result_chunks, |result_slice, (left_slice, right_slice)| {
            let simd_left = T::load(left_slice);
            let simd_right = T::load(right_slice);
            let simd_result = simd_op(simd_left, simd_right);

            let bitmask = T::mask_to_u64(&simd_result);
            let bytes = bitmask.to_le_bytes();
            result_slice[0..lanes / 8].copy_from_slice(&bytes[0..lanes / 8]);

            &mut result_slice[lanes / 8..]
        });

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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            null_bit_buffer,
            0,
            vec![result.into()],
            vec![],
        )
    };
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

    // safety: result is newly created above, always written as a T below
    let result_chunks = unsafe { result.typed_data_mut() };
    let result_remainder =
        left_chunks
            .borrow_mut()
            .fold(result_chunks, |result_slice, left_slice| {
                let simd_left = T::load(left_slice);
                let simd_result = simd_op(simd_left, simd_right);

                let bitmask = T::mask_to_u64(&simd_result);
                let bytes = bitmask.to_le_bytes();
                result_slice[0..lanes / 8].copy_from_slice(&bytes[0..lanes / 8]);

                &mut result_slice[lanes / 8..]
            });

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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            Some(null_count),
            null_bit_buffer,
            0,
            vec![result.into()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

macro_rules! typed_cmp {
    ($LEFT: expr, $RIGHT: expr, $T: ident, $OP: ident) => {{
        let left = $LEFT.as_any().downcast_ref::<$T>().ok_or_else(|| {
            ArrowError::CastError(format!(
                "Left array cannot be cast to {}",
                type_name::<$T>()
            ))
        })?;
        let right = $RIGHT.as_any().downcast_ref::<$T>().ok_or_else(|| {
            ArrowError::CastError(format!(
                "Right array cannot be cast to {}",
                type_name::<$T>(),
            ))
        })?;
        $OP(left, right)
    }};
    ($LEFT: expr, $RIGHT: expr, $T: ident, $OP: ident, $TT: tt) => {{
        let left = $LEFT.as_any().downcast_ref::<$T>().ok_or_else(|| {
            ArrowError::CastError(format!(
                "Left array cannot be cast to {}",
                type_name::<$T>()
            ))
        })?;
        let right = $RIGHT.as_any().downcast_ref::<$T>().ok_or_else(|| {
            ArrowError::CastError(format!(
                "Right array cannot be cast to {}",
                type_name::<$T>(),
            ))
        })?;
        $OP::<$TT>(left, right)
    }};
}

macro_rules! typed_compares {
    ($LEFT: expr, $RIGHT: expr, $OP_BOOL: ident, $OP_PRIM: ident, $OP_STR: ident) => {{
        match ($LEFT.data_type(), $RIGHT.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                typed_cmp!($LEFT, $RIGHT, BooleanArray, $OP_BOOL)
            }
            (DataType::Int8, DataType::Int8) => {
                typed_cmp!($LEFT, $RIGHT, Int8Array, $OP_PRIM, Int8Type)
            }
            (DataType::Int16, DataType::Int16) => {
                typed_cmp!($LEFT, $RIGHT, Int16Array, $OP_PRIM, Int16Type)
            }
            (DataType::Int32, DataType::Int32) => {
                typed_cmp!($LEFT, $RIGHT, Int32Array, $OP_PRIM, Int32Type)
            }
            (DataType::Int64, DataType::Int64) => {
                typed_cmp!($LEFT, $RIGHT, Int64Array, $OP_PRIM, Int64Type)
            }
            (DataType::UInt8, DataType::UInt8) => {
                typed_cmp!($LEFT, $RIGHT, UInt8Array, $OP_PRIM, UInt8Type)
            }
            (DataType::UInt16, DataType::UInt16) => {
                typed_cmp!($LEFT, $RIGHT, UInt16Array, $OP_PRIM, UInt16Type)
            }
            (DataType::UInt32, DataType::UInt32) => {
                typed_cmp!($LEFT, $RIGHT, UInt32Array, $OP_PRIM, UInt32Type)
            }
            (DataType::UInt64, DataType::UInt64) => {
                typed_cmp!($LEFT, $RIGHT, UInt64Array, $OP_PRIM, UInt64Type)
            }
            (DataType::Float32, DataType::Float32) => {
                typed_cmp!($LEFT, $RIGHT, Float32Array, $OP_PRIM, Float32Type)
            }
            (DataType::Float64, DataType::Float64) => {
                typed_cmp!($LEFT, $RIGHT, Float64Array, $OP_PRIM, Float64Type)
            }
            (DataType::Utf8, DataType::Utf8) => {
                typed_cmp!($LEFT, $RIGHT, StringArray, $OP_STR, i32)
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                typed_cmp!($LEFT, $RIGHT, LargeStringArray, $OP_STR, i64)
            }
            (t1, t2) if t1 == t2 => Err(ArrowError::NotYetImplemented(format!(
                "Comparing arrays of type {} is not yet implemented",
                t1
            ))),
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot compare two arrays of different types ({} and {})",
                t1, t2
            ))),
        }
    }};
}

/// Perform `left == right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, eq_bool, eq, eq_utf8)
}

/// Perform `left != right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn neq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, neq_bool, neq, neq_utf8)
}

/// Perform `left < right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn lt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, lt_bool, lt, lt_utf8)
}

/// Perform `left <= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn lt_eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, lt_eq_bool, lt_eq, lt_eq_utf8)
}

/// Perform `left > right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn gt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, gt_bool, gt, gt_utf8)
}

/// Perform `left >= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
pub fn gt_eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray> {
    typed_compares!(left, right, gt_eq_bool, gt_eq, gt_eq_utf8)
}

/// Perform `left == right` operation on two [`PrimitiveArray`]s.
pub fn eq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a == b);
}

/// Perform `left == right` operation on a [`PrimitiveArray`] and a scalar value.
pub fn eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a == b);
}

/// Perform `left != right` operation on two [`PrimitiveArray`]s.
pub fn neq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op!(left, right, |a, b| a != b);
}

/// Perform `left != right` operation on a [`PrimitiveArray`] and a scalar value.
pub fn neq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar!(left, right, |a, b| a != b);
}

/// Perform `left < right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
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

/// Perform `left < right` operation on a [`PrimitiveArray`] and a scalar value.
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

/// Perform `left <= right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
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

/// Perform `left <= right` operation on a [`PrimitiveArray`] and a scalar value.
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

/// Perform `left > right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
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

/// Perform `left > right` operation on a [`PrimitiveArray`] and a scalar value.
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

/// Perform `left >= right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
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

/// Perform `left >= right` operation on a [`PrimitiveArray`] and a scalar value.
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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            None,
            0,
            vec![bool_buf.into()],
            vec![],
        )
    };
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

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            None,
            0,
            vec![bool_buf.into()],
            vec![],
        )
    };
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
        ($KERNEL:ident, $DYN_KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let b = Int64Array::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // slice and test if the dynamic array works
            let a = a.slice(0, a.len());
            let b = b.slice(0, b.len());
            let c = $DYN_KERNEL(a.as_ref(), b.as_ref()).unwrap();
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
            eq_dyn,
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
        let d = eq(c, &a).unwrap();
        assert!(d.value(0));
        assert!(d.value(1));
        assert!(d.value(2));
        assert!(!d.value(3));
        assert!(d.value(4));
    }

    #[test]
    fn test_primitive_array_eq_scalar_with_slice() {
        let a = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let a = a.slice(1, 3);
        let a: &Int32Array = as_primitive_array(&a);
        let a_eq = eq_scalar(a, 2).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
    }

    #[test]
    fn test_primitive_array_neq() {
        cmp_i64!(
            neq,
            neq_dyn,
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
    fn test_boolean_array_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None,  Some(false)]
                .into();

        let res: Vec<Option<bool>> = eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_neq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = neq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = lt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = lt_eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(true), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = gt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(false), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = gt_eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let res2: Vec<Option<bool>> = eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_neq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            neq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);

        let res2: Vec<Option<bool>> = neq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = lt_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(false), None]);

        let res2: Vec<Option<bool>> = lt_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            lt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let res2: Vec<Option<bool>> =
            lt_eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_gt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = gt_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);

        let res2: Vec<Option<bool>> = gt_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_gt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            gt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(true), None]);

        let res2: Vec<Option<bool>> =
            gt_eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
    }

    #[test]
    fn test_primitive_array_lt() {
        cmp_i64!(
            lt,
            lt_dyn,
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
            lt_dyn,
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
            lt_eq_dyn,
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
            lt_eq_dyn,
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
            gt_dyn,
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
            gt_dyn,
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
            gt_eq_dyn,
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
            gt_eq_dyn,
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
        let actual = lt(a, b).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_compare_scalar_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = lt_scalar(a, 200).unwrap();
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
            .build()
            .unwrap();

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

    #[test]
    fn test_utf8_eq_scalar_on_slice() {
        let a = StringArray::from(vec![Some("hi"), None, Some("hello"), Some("world")]);
        let a = a.slice(1, 3);
        let a = as_string_array(&a);
        let a_eq = eq_utf8_scalar(a, "hello").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
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

    macro_rules! test_flag_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right, None).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let flag = Some(StringArray::from($flag));
                let res = $op(&left, &right, flag.as_ref()).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_flag_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right, None).unwrap();
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
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let flag = Some($flag);
                let res = $op(&left, $right, flag).unwrap();
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
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like_utf8,
        vec![true, true, true, false, false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        like_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        like_utf8_scalar,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        like_utf8_scalar,
        vec![true, false, false]
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

    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_utf8,
        vec![false, false, false, true, true, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_escape_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        nlike_utf8_scalar,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        nlike_utf8_scalar,
        vec![false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        nlike_utf8_scalar,
        vec![false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        nlike_utf8_scalar,
        vec![false, false, true, true]
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
        test_utf8_array_ilike,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike_utf8,
        vec![true, true, true, false, false, true, false]
    );
    test_utf8_scalar!(
        ilike_utf8_scalar_escape_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        ilike_utf8_scalar,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_ilike_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%AR%",
        ilike_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_start,
        vec!["arrow", "parrow", "arrows", "ARR"],
        "aRRow%",
        ilike_utf8_scalar,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_end,
        vec!["ArroW", "parrow", "ARRowS", "arr"],
        "%arrow",
        ilike_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        ilike_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        ilike_utf8_scalar,
        vec![false, true, false, false]
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
    test_flag_utf8!(
        test_utf8_array_regexp_is_match,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"],
        vec!["^ar", "^AR", "ow$", "OW$", "foo", ""],
        regexp_is_match_utf8,
        vec![true, false, true, false, false, true]
    );
    test_flag_utf8!(
        test_utf8_array_regexp_is_match_insensitive,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"],
        vec!["^ar", "^AR", "ow$", "OW$", "foo", ""],
        vec!["i"; 6],
        regexp_is_match_utf8,
        vec![true, true, true, true, false, true]
    );

    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "^ar",
        regexp_is_match_utf8_scalar,
        vec![true, false, false, false]
    );
    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_empty_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "",
        regexp_is_match_utf8_scalar,
        vec![true, true, true, true]
    );
    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_insensitive_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "^ar",
        "i",
        regexp_is_match_utf8_scalar,
        vec![true, true, false, false]
    );
}
