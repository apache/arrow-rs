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

use std::ops::Not;

use crate::array::{make_array, Array, ArrayData, ArrayRef, BooleanArray};
use crate::buffer::{
    buffer_bin_and, buffer_bin_or, buffer_unary_not, Buffer, MutableBuffer,
};
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::util::bit_util::{ceil, round_upto_multiple_of_64};
use core::iter;
use lexical_core::Integer;

fn binary_boolean_kleene_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(u64, u64, u64, u64) -> (u64, u64),
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    // length and offset of boolean array is measured in bits
    let len = left.len();

    // result length measured in bytes (incl. remainder)
    let mut result_len = round_upto_multiple_of_64(len) / 8;
    // The iterator that applies the kleene_op closure always chains an additional iteration
    // for the remainder chunk, even without a remainder. If the remainder is absent
    // (length % 64 == 0), kleene_op would resize the result buffers (value_buffer and
    // valid_buffer) to store 8 additional bytes, because result_len wouldn't include a remainder
    // chunk. The resizing is unnecessary and expensive. We can prevent it by adding 8 bytes to
    // result_len here. Nonetheless, all bits of these 8 bytes will be 0.
    if len % 64 == 0 {
        result_len += 8;
    }

    let mut value_buffer = MutableBuffer::new(result_len);
    let mut valid_buffer = MutableBuffer::new(result_len);

    let kleene_op = |((left_data, left_valid), (right_data, right_valid)): (
        (u64, u64),
        (u64, u64),
    )| {
        let left_true = left_valid & left_data;
        let left_false = left_valid & !left_data;

        let right_true = right_valid & right_data;
        let right_false = right_valid & !right_data;

        let (value, valid) = op(left_true, left_false, right_true, right_false);

        value_buffer.extend_from_slice(&[value]);
        valid_buffer.extend_from_slice(&[valid]);
    };

    let left_offset = left.offset();
    let right_offset = right.offset();

    let left_buffer = left.values();
    let right_buffer = right.values();

    let left_chunks = left_buffer.bit_chunks(left_offset, len);
    let right_chunks = right_buffer.bit_chunks(right_offset, len);

    let left_rem = left_chunks.remainder_bits();
    let right_rem = right_chunks.remainder_bits();

    let opt_left_valid_chunks_and_rem = left
        .data_ref()
        .null_buffer()
        .map(|b| b.bit_chunks(left_offset, len))
        .map(|chunks| (chunks.iter(), chunks.remainder_bits()));
    let opt_right_valid_chunks_and_rem = right
        .data_ref()
        .null_buffer()
        .map(|b| b.bit_chunks(right_offset, len))
        .map(|chunks| (chunks.iter(), chunks.remainder_bits()));

    match (
        opt_left_valid_chunks_and_rem,
        opt_right_valid_chunks_and_rem,
    ) {
        (
            Some((left_valid_chunks, left_valid_rem)),
            Some((right_valid_chunks, right_valid_rem)),
        ) => {
            left_chunks
                .iter()
                .zip(left_valid_chunks)
                .zip(right_chunks.iter().zip(right_valid_chunks))
                .chain(iter::once((
                    (left_rem, left_valid_rem),
                    (right_rem, right_valid_rem),
                )))
                .for_each(kleene_op);
        }
        (Some((left_valid_chunks, left_valid_rem)), None) => {
            left_chunks
                .iter()
                .zip(left_valid_chunks)
                .zip(right_chunks.iter().zip(iter::repeat(u64::MAX)))
                .chain(iter::once((
                    (left_rem, left_valid_rem),
                    (right_rem, u64::MAX),
                )))
                .for_each(kleene_op);
        }
        (None, Some((right_valid_chunks, right_valid_rem))) => {
            left_chunks
                .iter()
                .zip(iter::repeat(u64::MAX))
                .zip(right_chunks.iter().zip(right_valid_chunks))
                .chain(iter::once((
                    (left_rem, u64::MAX),
                    (right_rem, right_valid_rem),
                )))
                .for_each(kleene_op);
        }
        (None, None) => {
            left_chunks
                .iter()
                .zip(iter::repeat(u64::MAX))
                .zip(right_chunks.iter().zip(iter::repeat(u64::MAX)))
                .chain(iter::once(((left_rem, u64::MAX), (right_rem, u64::MAX))))
                .for_each(kleene_op);
        }
    };

    let bool_buffer: Buffer = value_buffer.into();
    let bool_valid_buffer: Buffer = valid_buffer.into();

    let array_data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        Some(bool_valid_buffer),
        left_offset,
        vec![bool_buffer],
        vec![],
    );

    Ok(BooleanArray::from(array_data))
}

/// Helper function to implement binary kernels
fn binary_boolean_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(&Buffer, usize, &Buffer, usize, usize) -> Buffer,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let len = left.len();

    let left_data = left.data_ref();
    let right_data = right.data_ref();
    let null_bit_buffer = combine_option_bitmap(&left_data, &right_data, len)?;

    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];
    let left_offset = left.offset();
    let right_offset = right.offset();

    let values = op(&left_buffer, left_offset, &right_buffer, right_offset, len);

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        null_bit_buffer,
        0,
        vec![values],
        vec![],
    );
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
    binary_boolean_kernel(&left, &right, buffer_bin_and)
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
    if left.null_count().is_zero() && right.null_count().is_zero() {
        return and(left, right);
    }

    let op = |left_true, left_false, right_true, right_false| {
        (
            left_true & right_true,
            left_false | right_false | (left_true & right_true),
        )
    };

    binary_boolean_kleene_kernel(left, right, op)
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
    binary_boolean_kernel(&left, &right, buffer_bin_or)
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
    if left.null_count().is_zero() && right.null_count().is_zero() {
        return or(left, right);
    }

    let op = |left_true, left_false, right_true, right_false| {
        (
            left_true | right_true,
            left_true | right_true | (left_false & right_false),
        )
    };

    binary_boolean_kleene_kernel(left, right, op)
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
        .map(|b| b.bits.bit_slice(left_offset, len));

    let values = buffer_unary_not(&data.buffers()[0], left_offset, len);

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        null_bit_buffer,
        0,
        vec![values],
        vec![],
    );
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
pub fn is_null(input: &Array) -> Result<BooleanArray> {
    let len = input.len();

    let output = match input.data_ref().null_buffer() {
        None => {
            let len_bytes = ceil(len, 8);
            MutableBuffer::from_len_zeroed(len_bytes).into()
        }
        Some(buffer) => buffer_unary_not(buffer, input.offset(), len),
    };

    let data =
        ArrayData::new(DataType::Boolean, len, None, None, 0, vec![output], vec![]);

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
pub fn is_not_null(input: &Array) -> Result<BooleanArray> {
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

    let data =
        ArrayData::new(DataType::Boolean, len, None, None, 0, vec![output], vec![]);

    Ok(BooleanArray::from(data))
}

/// Copies original array, setting null bit to true if a secondary comparison
/// boolean array is set to true. Typically used to implement NULLIF.
pub fn nullif(left: &ArrayRef, right: &BooleanArray) -> Result<ArrayRef> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let left_data = left.data();

    // If right has no trues and no nulls, then it will pass the left array
    // unmodified.
    if right.null_count() == 0
        && right
            .values()
            .count_set_bits_offset(right.offset(), right.len())
            == 0
    {
        return Ok(left.clone());
    }

    // If left is already all null, then it will pass the left array unmodified.
    if left.null_count() == left.len() {
        return Ok(left.clone());
    }

    // Null bitmaps are inverted -- (true if valid).
    //
    // The output should be null if left_is_null | (!right_is_null & right_value).
    // This corresponds to the output being valid if:
    //  !(!left_is_valid | (right_is_valid & right_value)) =
    //  left_is_valid & !(right_is_valid & right_value)
    let right_combo_buffer = match right.data().null_bitmap() {
        Some(right_is_valid) => {
            // NOTE: right values and bitmaps are combined and stay at bit offset
            // right.offset()
            (right_is_valid.buffer_ref() & right.values())?.not()
        }
        None => !right.values(),
    };

    // Check again -- if all of the falses in the right corresponded to nulls, we
    // can still pass the left unmodified.
    if right_combo_buffer.count_set_bits_offset(right.offset(), right.len())
        == right.len()
    {
        return Ok(left.clone());
    }

    // Now, we shift the right buffer to align with hte left buffer.
    let right_combo_buffer = if left.offset() == 0 {
        // The left offset is 0. Just slice the right buffer to line up.
        right_combo_buffer.bit_slice(right.offset(), right.len())
    } else {
        let bit_length = left.offset() + left.len();
        let byte_length = (bit_length + 7) / 8;
        let mut shift_builder = MutableBuffer::new(byte_length);
        let (pad_bytes, pad_bits) = (left.offset() / 8, left.offset() % 8);

        // Pad with a number of 0 bytes to cover the byte-aligned part of the left
        // offset.
        shift_builder.extend_zeros(pad_bytes);

        // Pad with the non-aligned part of the left offset.
        let used_right_bits = if pad_bits > 0 {
            let right_bits = usize::min(8 - pad_bits, right.len());
            let partial = right_combo_buffer.bit_slice(right.offset(), right_bits)[0];
            let partial = partial << pad_bits;
            shift_builder.push(partial);
            right_bits
        } else {
            0
        };
        if used_right_bits < right.len() {
            // Finally, copy over the remaining bits of `right`.
            let remaining = right_combo_buffer.bit_slice(
                right.offset() + used_right_bits,
                right.len() - used_right_bits,
            );
            shift_builder.extend_from_slice(remaining.as_slice());
        }

        shift_builder.into()
    };

    // AND of original left null bitmap with right expression
    // We've shifted the right buffer to line up with the left offset,
    // so this can just do a straight-up `&`.
    let modified_null_buffer = match left_data.null_bitmap() {
        Some(left_is_valid) => (&right_combo_buffer & left_is_valid.buffer_ref())?,
        None => right_combo_buffer,
    };

    // Construct new array with same values but modified null bitmap
    let data = ArrayData::new(
        left.data_type().clone(),
        left.len(),
        None, // force new to compute the number of null bits
        Some(modified_null_buffer),
        left.offset(),
        left_data.buffers().to_vec(),
        left_data.child_data().to_vec(),
    );
    Ok(make_array(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        ArrayRef, BooleanBuilder, Int32Array, Int32Builder, StructArray, StructBuilder,
    };
    use crate::datatypes::Field;
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
    fn test_binary_boolean_kleene_kernel() {
        // the kleene kernel is based on chunking and we want to also create
        // cases, where the number of values is not a multiple of 64
        for &value in [true, false].iter() {
            for &is_valid in [true, false].iter() {
                for &n in [0usize, 1, 63, 64, 65, 127, 128].iter() {
                    let a = BooleanArray::from(vec![Some(true); n]);
                    let b = BooleanArray::from(vec![None; n]);

                    let result = binary_boolean_kleene_kernel(&a, &b, |_, _, _, _| {
                        let tmp_value = if value { u64::MAX } else { 0 };
                        let tmp_is_valid = if is_valid { u64::MAX } else { 0 };
                        (tmp_value, tmp_is_valid)
                    })
                    .unwrap();

                    assert_eq!(result.len(), n);
                    (0..n).for_each(|idx| {
                        assert_eq!(value, result.value(idx));
                        assert_eq!(is_valid, result.is_valid(idx));
                    });
                }
            }
        }
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
        let c = not(&a).unwrap();

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

        let c = and(&a, &b).unwrap();

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

        let c = and(&a, &b).unwrap();

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

        let c = and(&a, &b).unwrap();

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

        let c = and(&a, &b).unwrap();

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

        let c = and(&a, &b).unwrap();

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
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
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
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
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
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullif_int_array() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(15),
            None,
            Some(8),
            Some(1),
            Some(9),
        ]));
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]);

        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(15),
            Some(8),
            Some(1),
            Some(9),
        ]));
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)

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
        let res = nullif(&a, &comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_int_large_left_offset() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
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
        ]));
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
        let res = nullif(&a, &comp).unwrap();
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
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,     // 0
            Some(15), // 1
            Some(8),
            Some(1),
            Some(9),
        ]));
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
        let res = nullif(&a, &comp).unwrap();
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
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,       // 0
            Some(true), // 1
            Some(false),
            Some(true),
            Some(true),
        ]));
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
        let res = nullif(&a, &comp).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),  // False => keep it
            Some(false), // None => keep it
            None,        // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_passthrough_all_false() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(15),
            Some(8),
            None,
            Some(9),
        ]));
        let a = a.slice(1, 3); // Some(15), Some(8), None

        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, &comp).unwrap();
        let res_array = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // False => keep it
            None,     // False => keep it
        ]);
        assert_eq!(&expected, res_array);
        assert!(Arc::ptr_eq(&a, &res));
    }

    #[test]
    fn test_nullif_passthrough_all_null_or_false() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(15),
            Some(8),
            None,
            Some(9),
        ]));
        let a = a.slice(1, 3); // Some(15), Some(8), None

        let comp = BooleanArray::from(vec![
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(false),
        ]);
        let comp = comp.slice(2, 3); // Some(false), Some(false), None
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, &comp).unwrap();
        let res_array = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // False => keep it
            None,     // Null => keep it
        ]);
        assert_eq!(&expected, res_array);
        assert!(Arc::ptr_eq(&a, &res));
    }

    #[test]
    fn test_nullif_passthrough_all_already_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None, None, None, None]));
        let a = a.slice(1, 3); // None, None, None

        let comp = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
        ]);
        let comp = comp.slice(2, 3); // Some(true), Some(false), None
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, &comp).unwrap();
        let res_array = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(&expected, res_array);
        assert!(Arc::ptr_eq(&a, &res));
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
                Box::new(Int32Builder::new(values.len())),
                Box::new(BooleanBuilder::new(values.len())),
            ],
        );

        for value in values {
            struct_array
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_option(value.a)
                .unwrap();
            struct_array
                .field_builder::<BooleanBuilder>(1)
                .unwrap()
                .append_option(value.b)
                .unwrap();
            struct_array.append(value.is_valid).unwrap();
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

        let struct_array: ArrayRef = Arc::new(struct_array);
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
        let res = nullif(&struct_array, &comp).unwrap();
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
}
