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

//! Defines boolean kernels on Arrow `BooleanArray`'s, e.g. `AND`, `OR` and `NOT`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use crate::array::{Array, ArrayData, BooleanArray};
use crate::buffer::{
    bitwise_bin_op_helper, bitwise_quaternary_op_helper, buffer_bin_and, buffer_bin_or,
    buffer_unary_not, Buffer, MutableBuffer,
};
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::util::bit_util::ceil;
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{make_array, ArrayRef};
use arrow_buffer::buffer::bitwise_unary_op_helper;

/// Updates null buffer based on data buffer and null buffer of the operand at other side
/// in boolean AND kernel with Kleene logic. In short, because for AND kernel, null AND false
/// results false. So we cannot simply AND two null buffers. This function updates null buffer
/// of one side if other side is a false value.
pub(crate) fn build_null_buffer_for_and_kleene(
    left_data: &ArrayData,
    left_offset: usize,
    right_data: &ArrayData,
    right_offset: usize,
    len_in_bits: usize,
) -> Option<Buffer> {
    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];

    let left_null_buffer = left_data.null_buffer();
    let right_null_buffer = right_data.null_buffer();

    match (left_null_buffer, right_null_buffer) {
        (None, None) => None,
        (Some(left_null_buffer), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is false (because null AND false = false).
            Some(bitwise_bin_op_helper(
                left_null_buffer,
                left_offset,
                right_buffer,
                right_offset,
                len_in_bits,
                |a, b| a | !b,
            ))
        }
        (None, Some(right_null_buffer)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_null_buffer,
                right_offset,
                left_buffer,
                left_offset,
                len_in_bits,
                |a, b| a | !b,
            ))
        }
        (Some(left_null_buffer), Some(right_null_buffer)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & !d)) & (c | (a & !b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_null_buffer,
                    left_buffer,
                    right_null_buffer,
                    right_buffer,
                ],
                [left_offset, left_offset, right_offset, right_offset],
                len_in_bits,
                |a, b, c, d| (a | (c & !d)) & (c | (a & !b)),
            ))
        }
    }
}

/// For AND/OR kernels, the result of null buffer is simply a bitwise `and` operation.
pub(crate) fn build_null_buffer_for_and_or(
    left_data: &ArrayData,
    _left_offset: usize,
    right_data: &ArrayData,
    _right_offset: usize,
    len_in_bits: usize,
) -> Option<Buffer> {
    // `arrays` are not empty, so safely do `unwrap` directly.
    combine_option_bitmap(&[left_data, right_data], len_in_bits).unwrap()
}

/// Updates null buffer based on data buffer and null buffer of the operand at other side
/// in boolean OR kernel with Kleene logic. In short, because for OR kernel, null OR true
/// results true. So we cannot simply AND two null buffers. This function updates null
/// buffer of one side if other side is a true value.
pub(crate) fn build_null_buffer_for_or_kleene(
    left_data: &ArrayData,
    left_offset: usize,
    right_data: &ArrayData,
    right_offset: usize,
    len_in_bits: usize,
) -> Option<Buffer> {
    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];

    let left_null_buffer = left_data.null_buffer();
    let right_null_buffer = right_data.null_buffer();

    match (left_null_buffer, right_null_buffer) {
        (None, None) => None,
        (Some(left_null_buffer), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is true (because null OR true = true).
            Some(bitwise_bin_op_helper(
                left_null_buffer,
                left_offset,
                right_buffer,
                right_offset,
                len_in_bits,
                |a, b| a | b,
            ))
        }
        (None, Some(right_null_buffer)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_null_buffer,
                right_offset,
                left_buffer,
                left_offset,
                len_in_bits,
                |a, b| a | b,
            ))
        }
        (Some(left_null_buffer), Some(right_null_buffer)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & d)) & (c | (a & b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_null_buffer,
                    left_buffer,
                    right_null_buffer,
                    right_buffer,
                ],
                [left_offset, left_offset, right_offset, right_offset],
                len_in_bits,
                |a, b, c, d| (a | (c & d)) & (c | (a & b)),
            ))
        }
    }
}

/// Helper function to implement binary kernels
pub(crate) fn binary_boolean_kernel<F, U>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
    null_op: U,
) -> Result<BooleanArray>
where
    F: Fn(&Buffer, usize, &Buffer, usize, usize) -> Buffer,
    U: Fn(&ArrayData, usize, &ArrayData, usize, usize) -> Option<Buffer>,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let len = left.len();

    let left_data = left.data_ref();
    let right_data = right.data_ref();

    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];
    let left_offset = left.offset();
    let right_offset = right.offset();

    let null_bit_buffer = null_op(left_data, left_offset, right_data, right_offset, len);

    let values = op(left_buffer, left_offset, right_buffer, right_offset, len);

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            null_bit_buffer,
            0,
            vec![values],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Performs `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::and;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let and_ab = and(&a, &b)?;
/// assert_eq!(and_ab, BooleanArray::from(vec![Some(false), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(left, right, buffer_bin_and, build_null_buffer_for_and_or)
}

/// Logical 'and' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` and `null` = `null`
/// * `null` and `true` = `null`
/// * `false` and `null` = `false`
/// * `null` and `false` = `false`
/// * `null` and `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'and' false is always false.
/// For a different null behavior, see function \"and\".
///
/// # Example
///
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::and_kleene;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let and_ab = and_kleene(&a, &b)?;
/// assert_eq!(and_ab, BooleanArray::from(vec![None, Some(false), None]));
/// # Ok(())
/// # }
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn and_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(
        left,
        right,
        buffer_bin_and,
        build_null_buffer_for_and_kleene,
    )
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::or;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let or_ab = or(&a, &b)?;
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(left, right, buffer_bin_or, build_null_buffer_for_and_or)
}

/// Logical 'or' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` or `null` = `true`
/// * `null` or `true` = `true`
/// * `false` or `null` = `null`
/// * `null` or `false` = `null`
/// * `null` or `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'or' true is always true.
/// For a different null behavior, see function \"or\".
///
/// # Example
///
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::or_kleene;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let or_ab = or_kleene(&a, &b)?;
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), None, None]));
/// # Ok(())
/// # }
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn or_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(left, right, buffer_bin_or, build_null_buffer_for_or_kleene)
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
/// # Error
/// This function never errors. It returns an error for consistency.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::not;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let not_a = not(&a)?;
/// assert_eq!(not_a, BooleanArray::from(vec![Some(true), Some(false), None]));
/// # Ok(())
/// # }
/// ```
pub fn not(left: &BooleanArray) -> Result<BooleanArray> {
    let left_offset = left.offset();
    let len = left.len();

    let data = left.data_ref();
    let null_bit_buffer = data
        .null_bitmap()
        .as_ref()
        .map(|b| b.buffer().bit_slice(left_offset, len));

    let values = buffer_unary_not(&data.buffers()[0], left_offset, len);

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            null_bit_buffer,
            0,
            vec![values],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow::error::Result;
/// use arrow::array::BooleanArray;
/// use arrow::compute::kernels::boolean::is_null;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_null = is_null(&a)?;
/// assert_eq!(a_is_null, BooleanArray::from(vec![false, false, true]));
/// # Ok(())
/// # }
/// ```
pub fn is_null(input: &dyn Array) -> Result<BooleanArray> {
    let len = input.len();

    let output = match input.data_ref().null_buffer() {
        None => {
            let len_bytes = ceil(len, 8);
            MutableBuffer::from_len_zeroed(len_bytes).into()
        }
        Some(buffer) => buffer_unary_not(buffer, input.offset(), len),
    };

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            None,
            0,
            vec![output],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is not null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow::error::Result;
/// use arrow::array::BooleanArray;
/// use arrow::compute::kernels::boolean::is_not_null;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_not_null = is_not_null(&a)?;
/// assert_eq!(a_is_not_null, BooleanArray::from(vec![true, true, false]));
/// # Ok(())
/// # }
/// ```
pub fn is_not_null(input: &dyn Array) -> Result<BooleanArray> {
    let len = input.len();

    let output = match input.data_ref().null_buffer() {
        None => {
            let len_bytes = ceil(len, 8);
            MutableBuffer::new(len_bytes)
                .with_bitset(len_bytes, true)
                .into()
        }
        Some(buffer) => buffer.bit_slice(input.offset(), len),
    };

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            None,
            0,
            vec![output],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

/// Copies original array, setting validity bit to false if a secondary comparison
/// boolean array is set to true
///
/// Typically used to implement NULLIF.
pub fn nullif(left: &dyn Array, right: &BooleanArray) -> Result<ArrayRef> {
    let left_data = left.data();
    let right_data = right.data();

    if left_data.len() != right_data.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let len = left_data.len();
    let left_offset = left_data.offset();

    // left=0 (null)   right=null       output bitmap=null
    // left=0          right=1          output bitmap=null
    // left=1 (set)    right=null       output bitmap=set   (passthrough)
    // left=1          right=1 & comp=true    output bitmap=null
    // left=1          right=1 & comp=false   output bitmap=set
    //
    // Thus: result = left null bitmap & (!right_values | !right_bitmap)
    //              OR left null bitmap & !(right_values & right_bitmap)

    // Compute right_values & right_bitmap
    let (right, right_offset) = match right_data.null_buffer() {
        Some(buffer) => (
            buffer_bin_and(
                &right_data.buffers()[0],
                right_data.offset(),
                buffer,
                right_data.offset(),
                len,
            ),
            0,
        ),
        None => (right_data.buffers()[0].clone(), right_data.offset()),
    };

    // Compute left null bitmap & !right
    let mut valid_count = 0;
    let combined = match left_data.null_buffer() {
        Some(left) => {
            bitwise_bin_op_helper(left, left_offset, &right, right_offset, len, |l, r| {
                let t = l & !r;
                valid_count += t.count_ones() as usize;
                t
            })
        }
        None => {
            let buffer = bitwise_unary_op_helper(&right, right_offset, len, |b| {
                let t = !b;
                valid_count += t.count_ones() as usize;
                t
            });
            // We need to compensate for the additional bits read from the end
            let remainder_len = len % 64;
            if remainder_len != 0 {
                valid_count -= 64 - remainder_len
            }
            buffer
        }
    };

    // Need to construct null buffer with offset of left
    let null_buffer = match left_data.offset() {
        0 => combined,
        _ => {
            let mut builder = BooleanBufferBuilder::new(len + left_offset);
            // Pad with 0s up to offset
            builder.resize(left_offset);
            builder.append_packed_range(0..len, &combined);
            builder.finish()
        }
    };

    let null_count = len - valid_count;
    let data = left_data
        .clone()
        .into_builder()
        .null_bit_buffer(Some(null_buffer))
        .null_count(null_count);

    // SAFETY:
    // Only altered null mask
    Ok(make_array(unsafe { data.build_unchecked() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayRef, Int32Array};
    use arrow_array::builder::{BooleanBuilder, Int32Builder, StructBuilder};
    use arrow_array::cast::{as_boolean_array, as_primitive_array, as_string_array};
    use arrow_array::types::Int32Type;
    use arrow_array::{StringArray, StructArray};
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, true, true, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_boolean_array_kleene_no_remainder() {
        let n = 1024;
        let a = BooleanArray::from(vec![true; n]);
        let b = BooleanArray::from(vec![None; n]);
        let result = or_kleene(&a, &b).unwrap();

        assert_eq!(result, a);
    }

    #[test]
    fn test_bool_array_and_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_right_sided_nulls() {
        let a = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is absent
        assert!(a.data_ref().null_bitmap().is_none());

        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is present
        assert!(b.data_ref().null_bitmap().is_some());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_left_sided_nulls() {
        let a = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is absent
        assert!(a.data_ref().null_bitmap().is_some());

        let b = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is present
        assert!(b.data_ref().null_bitmap().is_none());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, true]);
        let c = not(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not_sliced() {
        let a = BooleanArray::from(vec![None, Some(true), Some(false), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let c = not(a).unwrap();

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true,
            true,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset_mod8() {
        let a = BooleanArray::from(vec![
            false, false, true, true, false, false, false, false, false, false, false,
            false,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let a = a.slice(0, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset1() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true,
            true,
        ]);
        let b = BooleanArray::from(vec![false, true, false, true]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset2() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_nulls_offset() {
        let a = BooleanArray::from(vec![None, Some(false), Some(true), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let b = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);

        let b = b.slice(2, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected =
            BooleanArray::from(vec![Some(false), Some(false), None, Some(true)]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_not_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert_eq!(None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullif_int_array() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9)]);
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]);

        let res = as_primitive_array::<Int32Type>(&res);
        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a = Int32Array::from(vec![None, Some(15), Some(8), Some(1), Some(9)]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(a, comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        let res = as_primitive_array::<Int32Type>(&res);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_string() {
        let s = StringArray::from_iter([
            Some("hello"),
            None,
            Some("world"),
            Some("a"),
            Some("b"),
            None,
            None,
        ]);
        let select = BooleanArray::from_iter([
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            None,
        ]);

        let a = nullif(&s, &select).unwrap();
        let r: Vec<_> = as_string_array(&a).iter().collect();
        assert_eq!(
            r,
            vec![None, None, Some("world"), None, Some("b"), None, None]
        );

        let s = s.slice(2, 3);
        let select = select.slice(1, 3);
        let select = as_boolean_array(select.as_ref());
        let a = nullif(s.as_ref(), select).unwrap();
        let r: Vec<_> = as_string_array(&a).iter().collect();
        assert_eq!(r, vec![None, Some("a"), None]);
    }

    #[test]
    fn test_nullif_int_large_left_offset() {
        let a = Int32Array::from(vec![
            Some(-1), // 0
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1), // 8
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            None,     // 16
            Some(15), // 17
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(17, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_int_large_right_offset() {
        let a = Int32Array::from(vec![
            None,     // 0
            Some(15), // 1
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 8
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 16
            Some(false), // 17
            Some(false), // 18
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(18, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_boolean_offset() {
        let a = BooleanArray::from(vec![
            None,       // 0
            Some(true), // 1
            Some(false),
            Some(true),
            Some(true),
        ]);
        let a = a.slice(1, 3); // Some(true), Some(false), Some(true)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),  // False => keep it
            Some(false), // None => keep it
            None,        // true => None
        ]);
        assert_eq!(&expected, res)
    }

    struct Foo {
        a: Option<i32>,
        b: Option<bool>,
        /// Whether the entry should be valid.
        is_valid: bool,
    }

    impl Foo {
        fn new_valid(a: i32, b: bool) -> Foo {
            Self {
                a: Some(a),
                b: Some(b),
                is_valid: true,
            }
        }

        fn new_null() -> Foo {
            Self {
                a: None,
                b: None,
                is_valid: false,
            }
        }
    }

    /// Struct Array equality is a bit weird -- we need to have the *child values*
    /// correct even if the enclosing struct indicates it is null. But we
    /// also need the top level is_valid bits to be correct.
    fn create_foo_struct(values: Vec<Foo>) -> StructArray {
        let mut struct_array = StructBuilder::new(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Boolean, true),
            ],
            vec![
                Box::new(Int32Builder::with_capacity(values.len())),
                Box::new(BooleanBuilder::with_capacity(values.len())),
            ],
        );

        for value in values {
            struct_array
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_option(value.a);
            struct_array
                .field_builder::<BooleanBuilder>(1)
                .unwrap()
                .append_option(value.b);
            struct_array.append(value.is_valid);
        }

        struct_array.finish()
    }

    #[test]
    fn test_nullif_struct_slices() {
        let struct_array = create_foo_struct(vec![
            Foo::new_valid(7, true),
            Foo::new_valid(15, false),
            Foo::new_valid(8, true),
            Foo::new_valid(12, false),
            Foo::new_null(),
            Foo::new_null(),
            Foo::new_valid(42, true),
        ]);

        // Some({a: 15, b: false}), Some({a: 8, b: true}), Some({a: 12, b: false}),
        // None, None
        let struct_array = struct_array.slice(1, 5);
        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 5); // Some(false), None, Some(true), Some(false), None
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&struct_array, comp).unwrap();
        let res = res.as_any().downcast_ref::<StructArray>().unwrap();

        let expected = create_foo_struct(vec![
            // Some(false) -> keep
            Foo::new_valid(15, false),
            // None -> keep
            Foo::new_valid(8, true),
            // Some(true) -> null out. But child values are still there.
            Foo {
                a: Some(12),
                b: Some(false),
                is_valid: false,
            },
            // Some(false) -> keep, but was null
            Foo::new_null(),
            // None -> keep, but was null
            Foo::new_null(),
        ]);

        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_no_nulls() {
        let a = Int32Array::from(vec![Some(15), Some(7), Some(8), Some(1), Some(9)]);
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();
        let res = as_primitive_array::<Int32Type>(res.as_ref());

        let expected = Int32Array::from(vec![Some(15), Some(7), None, Some(1), Some(9)]);
        assert_eq!(res, &expected);
    }
}
