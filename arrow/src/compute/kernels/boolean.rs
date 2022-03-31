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

use crate::array::{
    make_array, Array, ArrayData, ArrayRef, BooleanArray, PrimitiveArray,
};
use crate::buffer::{
    buffer_bin_and, buffer_bin_or, buffer_unary_not, Buffer, MutableBuffer,
};
use crate::compute::util::{combine_option_bitmap, combine_option_buffers};
use crate::datatypes::{ArrowNumericType, DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::util::bit_util::{ceil, round_upto_multiple_of_64};
use core::iter;
use num::Zero;

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

    let array_data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            Some(bool_valid_buffer),
            left_offset,
            vec![bool_buffer],
            vec![],
        )
    };

    Ok(BooleanArray::from(array_data))
}

/// Helper function to implement binary kernels
pub(crate) fn binary_boolean_kernel<F>(
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
    let null_bit_buffer = combine_option_bitmap(left_data, right_data, len)?;

    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];
    let left_offset = left.offset();
    let right_offset = right.offset();

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
    binary_boolean_kernel(left, right, buffer_bin_and)
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
    binary_boolean_kernel(left, right, buffer_bin_or)
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

/// Copies original array, setting null bit to true if a secondary comparison boolean array is set to true.
/// Typically used to implement NULLIF.
// NOTE: For now this only supports Primitive Arrays.  Although the code could be made generic, the issue
// is that currently the bitmap operations result in a final bitmap which is aligned to bit 0, and thus
// the left array's data needs to be sliced to a new offset, and for non-primitive arrays shifting the
// data might be too complicated.   In the future, to avoid shifting left array's data, we could instead
// shift the final bitbuffer to the right, prepending with 0's instead.
pub fn nullif<T>(
    left: &PrimitiveArray<T>,
    right: &BooleanArray,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let left_data = left.data();
    let right_data = right.data();

    // If left has no bitmap, create a new one with all values set for nullity op later
    // left=0 (null)   right=null       output bitmap=null
    // left=0          right=1          output bitmap=null
    // left=1 (set)    right=null       output bitmap=set   (passthrough)
    // left=1          right=1 & comp=true    output bitmap=null
    // left=1          right=1 & comp=false   output bitmap=set
    //
    // Thus: result = left null bitmap & (!right_values | !right_bitmap)
    //              OR left null bitmap & !(right_values & right_bitmap)
    //
    // Do the right expression !(right_values & right_bitmap) first since there are two steps
    // TRICK: convert BooleanArray buffer as a bitmap for faster operation
    let right_combo_buffer = match right.data().null_bitmap() {
        Some(right_bitmap) => {
            // NOTE: right values and bitmaps are combined and stay at bit offset right.offset()
            (right.values() & &right_bitmap.bits).ok().map(|b| b.not())
        }
        None => Some(!right.values()),
    };

    // AND of original left null bitmap with right expression
    // Here we take care of the possible offsets of the left and right arrays all at once.
    let modified_null_buffer = match left_data.null_bitmap() {
        Some(left_null_bitmap) => match right_combo_buffer {
            Some(rcb) => Some(buffer_bin_and(
                &left_null_bitmap.bits,
                left_data.offset(),
                &rcb,
                right_data.offset(),
                left_data.len(),
            )),
            None => Some(
                left_null_bitmap
                    .bits
                    .bit_slice(left_data.offset(), left.len()),
            ),
        },
        None => right_combo_buffer
            .map(|rcb| rcb.bit_slice(right_data.offset(), right_data.len())),
    };

    // Align/shift left data on offset as needed, since new bitmaps are shifted and aligned to 0 already
    // NOTE: this probably only works for primitive arrays.
    let data_buffers = if left.offset() == 0 {
        left_data.buffers().to_vec()
    } else {
        // Shift each data buffer by type's bit_width * offset.
        left_data
            .buffers()
            .iter()
            .map(|buf| buf.slice(left.offset() * T::get_byte_width()))
            .collect::<Vec<_>>()
    };

    // Construct new array with same values but modified null bitmap
    // TODO: shift data buffer as needed
    let data = unsafe {
        ArrayData::new_unchecked(
            T::DATA_TYPE,
            left.len(),
            None, // force new to compute the number of null bits
            modified_null_buffer,
            0, // No need for offset since left data has been shifted
            data_buffers,
            left_data.child_data().to_vec(),
        )
    };
    Ok(PrimitiveArray::<T>::from(data))
}

/// Creates a (mostly) zero-copy slice of the given buffers so that they can be combined
/// in the same array with other buffers that start at offset 0.
/// The only buffers that need an actual copy are booleans and validity (if they are not byte-aligned).
/// This is useful when a kernel calculates a new validity bitmap but wants to reuse other buffers.
fn slice_buffers(
    buffers: &[Buffer],
    offset: usize,
    len: usize,
    data_type: &DataType,
    child_data: &[ArrayData],
) -> (Vec<Buffer>, Vec<ArrayData>) {
    use std::mem::size_of;

    if offset == 0 {
        return (buffers.to_vec(), child_data.to_vec());
    }

    // we only need to do something special to child data in 2 of the match branches
    let mut result_child_data = None;

    let result_buffers = match data_type {
        DataType::Boolean => vec![buffers[0].bit_slice(offset, len)],
        DataType::Int8 | DataType::UInt8 => {
            vec![buffers[0].slice(offset * size_of::<u8>())]
        }
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => {
            vec![buffers[0].slice(offset * size_of::<u16>())]
        }
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => {
            vec![buffers[0].slice(offset * size_of::<u32>())]
        }
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
            vec![buffers[0].slice(offset * size_of::<u64>())]
        }
        DataType::Timestamp(_, _) | DataType::Duration(_) => {
            vec![buffers[0].slice(offset * size_of::<u64>())]
        }
        DataType::Date32 | DataType::Time32(_) => {
            vec![buffers[0].slice(offset * size_of::<u32>())]
        }
        DataType::Date64 | DataType::Time64(_) => {
            vec![buffers[0].slice(offset * size_of::<u64>())]
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            vec![buffers[0].slice(offset * size_of::<u32>())]
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            vec![buffers[0].slice(offset * 2 * size_of::<u32>())]
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            vec![buffers[0].slice(offset * (2 * size_of::<u32>() + size_of::<u64>()))]
        }
        DataType::Decimal(_, _) => vec![buffers[0].slice(offset * size_of::<i128>())],
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 | DataType::UInt8 => {
                vec![buffers[0].slice(offset * size_of::<u8>())]
            }
            DataType::Int16 | DataType::UInt16 => {
                vec![buffers[0].slice(offset * size_of::<u16>())]
            }
            DataType::Int32 | DataType::UInt32 => {
                vec![buffers[0].slice(offset * size_of::<u32>())]
            }
            DataType::Int64 | DataType::UInt64 => {
                vec![buffers[0].slice(offset * size_of::<u64>())]
            }
            _ => unreachable!(),
        },
        DataType::List(_) => {
            // safe because for List the first buffer is guaranteed to contain i32 offsets
            vec![buffers[0].slice(offset * size_of::<i32>())]
        }
        DataType::LargeList(_) => {
            // safe because for LargeList the first buffer is guaranteed to contain i64 offsets
            vec![buffers[0].slice(offset * size_of::<i64>())]
        }
        DataType::Binary | DataType::Utf8 => {
            // safe because for Binary/Utf8 the first buffer is guaranteed to contain i32 offsets
            vec![
                buffers[0].slice(offset * size_of::<i32>()),
                buffers[1].clone(),
            ]
        }
        DataType::LargeBinary | DataType::LargeUtf8 => {
            // safe because for LargeBinary/LargeUtf8 the first buffer is guaranteed to contain i64 offsets
            vec![
                buffers[0].slice(offset * size_of::<i64>()),
                buffers[1].clone(),
            ]
        }
        DataType::FixedSizeBinary(size) => {
            vec![buffers[0].slice(offset * (*size as usize))]
        }
        DataType::FixedSizeList(_field, size) => {
            result_child_data = Some(
                child_data
                    .iter()
                    .map(|d| {
                        slice_array_data(
                            d,
                            offset * (*size as usize),
                            len * (*size as usize),
                        )
                    })
                    .collect(),
            );
            vec![]
        }
        DataType::Struct(_) => {
            result_child_data = Some(
                child_data
                    .iter()
                    .map(|d| slice_array_data(d, offset, len))
                    .collect(),
            );
            vec![]
        }
        DataType::Union(..) => {
            // TODO: verify this is actually correct
            if buffers.len() > 1 {
                // dense union, type ids and offsets
                vec![
                    buffers[0].slice(offset * size_of::<i8>()),
                    buffers[1].slice(offset * size_of::<u32>()),
                ]
            } else {
                // sparse union with only type ids
                // should this also slice the child arrays?
                vec![buffers[0].slice(offset * size_of::<i8>())]
            }
        }
        DataType::Map(_, _) => {
            // TODO: verify this is actually correct
            result_child_data =
                Some(child_data.iter().map(|d| d.slice(offset, len)).collect());
            vec![]
        }
        DataType::Null => vec![],
    };

    (
        result_buffers,
        result_child_data.unwrap_or_else(|| child_data.to_vec()),
    )
}

fn slice_array_data(data: &ArrayData, offset: usize, len: usize) -> ArrayData {
    assert!(offset >= data.offset());
    assert!((offset - data.offset()) + len <= data.len());

    if offset == 0 {
        return data.clone();
    }

    let result_valid_buffer = data.null_buffer().map(|b| b.bit_slice(offset, len));

    let (result_data_buffers, result_child_data) = slice_buffers(
        data.buffers(),
        offset,
        len,
        data.data_type(),
        data.child_data(),
    );

    unsafe {
        ArrayData::new_unchecked(
            data.data_type().clone(),
            len,
            None,
            result_valid_buffer,
            0,
            result_data_buffers,
            result_child_data,
        )
    }
}

pub fn nullif_alternative(
    array: &ArrayRef,
    condition: &BooleanArray,
) -> Result<ArrayRef> {
    if array.len() != condition.len() {
        return Err(ArrowError::ComputeError(
            "Inputs to conditional kernels must have the same length".to_string(),
        ));
    }

    let condition_buffer = combine_option_buffers(
        condition.data().buffers().first(),
        condition.offset(),
        condition.data().null_buffer(),
        condition.offset(),
        condition.len(),
    )
    .map(|b| !&b);

    let result_valid_buffer = combine_option_buffers(
        condition_buffer.as_ref(),
        0,
        array.data().null_buffer(),
        array.offset(),
        condition.len(),
    );

    let (result_data_buffers, result_child_data) = slice_buffers(
        array.data().buffers(),
        array.offset(),
        array.len(),
        array.data_type(),
        array.data().child_data(),
    );

    let data = unsafe {
        ArrayData::new_unchecked(
            array.data_type().clone(),
            condition.len(),
            None,
            result_valid_buffer,
            0,
            result_data_buffers,
            result_child_data,
        )
    };

    Ok(make_array(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        ArrayRef, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Int16Array,
        Int32Array, ListArray, StringArray, StructArray,
    };
    use crate::datatypes::{Field, Float64Type, Int32Type};
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
        let res = nullif_alternative(&a, &comp).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]));

        assert_eq!(&expected, &res);
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a = Int32Array::from(vec![None, Some(15), Some(8), Some(1), Some(9)]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)
                               // let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
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
        let res = nullif_alternative(&a, comp).unwrap();

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]));
        assert_eq!(&expected, &res)
    }

    #[test]
    fn test_nullif_dict() {
        let array = DictionaryArray::<Int32Type>::from_iter(["a", "b", "c"]);
        let array_ref = Arc::new(array) as ArrayRef;
        let condition = BooleanArray::from(vec![true, false, true]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let expected_keys = &[None, Some(1), None].iter().collect::<Int32Array>();
        let actual_keys = result.keys();

        assert_eq!(expected_keys, actual_keys);
    }

    #[test]
    fn test_nullif_dict_sliced() {
        let array =
            DictionaryArray::<Int32Type>::from_iter(["a", "b", "c", "b", "c", "b"]);
        let array_ref = Arc::new(array) as ArrayRef;
        let array_ref = array_ref.slice(3, 3);
        let condition = BooleanArray::from(vec![true, true, false]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let expected_keys = &[None, None, Some(1)]
            .iter()
            .collect::<PrimitiveArray<Int32Type>>();
        let actual_keys = result.keys();

        assert_eq!(expected_keys, actual_keys);
    }

    #[test]
    fn test_nullif_dict_cond_sliced() {
        let array = DictionaryArray::<Int32Type>::from_iter(["a", "b", "c"]);
        let array_ref = Arc::new(array) as ArrayRef;
        let condition = BooleanArray::from(vec![true, false, true, true, true, false]);
        let condition = condition.slice(3, 3);
        let condition = condition.as_any().downcast_ref::<BooleanArray>().unwrap();

        let result = nullif_alternative(&array_ref, condition).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let expected_keys = &[None, None, Some(2)]
            .iter()
            .collect::<PrimitiveArray<Int32Type>>();
        let actual_keys = result.keys();

        assert_eq!(expected_keys, actual_keys);
    }

    #[test]
    fn test_nullif_list() {
        let array = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            None,
            Some(vec![Some(4.0), None, Some(6.0)]),
            Some(vec![]),
            Some(vec![None, Some(8.0), None]),
        ]);
        let array_ref = Arc::new(array) as ArrayRef;
        let condition = BooleanArray::from(vec![true, false, false, false, true]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        let expected = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            None,
            None,
            Some(vec![Some(4.0), None, Some(6.0)]),
            Some(vec![]),
            None,
        ]);
        let expected = &expected;

        // simple assert_eq on ListArrays does not work if their offsets are different
        // see https://github.com/apache/arrow-rs/issues/626
        assert_eq!(expected.len(), result.len());
        for i in 0..expected.len() {
            assert_eq!(expected.is_valid(i), result.is_valid(i));
            if expected.is_valid(i) {
                assert_eq!(&expected.value(i), &result.value(i));
            }
        }
    }

    #[test]
    fn test_nullif_list_sliced() {
        let array = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            None,
            Some(vec![Some(4.0), None, Some(6.0)]),
            Some(vec![]),
            Some(vec![None, Some(8.0), None]),
        ]);
        let array_ref = array.slice(1, 4);
        let condition = BooleanArray::from(vec![false, false, false, true]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        let expected = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            None,
            Some(vec![Some(4.0), None, Some(6.0)]),
            Some(vec![]),
            None,
        ]);
        let expected = &expected;

        // simple assert_eq on ListArrays does not work if their offsets are different
        // see https://github.com/apache/arrow-rs/issues/626
        assert_eq!(expected.len(), result.len());
        for i in 0..expected.len() {
            assert_eq!(expected.is_valid(i), result.is_valid(i));
            if expected.is_valid(i) {
                assert_eq!(&expected.value(i), &result.value(i));
            }
        }
    }

    #[test]
    fn test_nullif_strings() {
        let array = StringArray::from(vec!["abc", "", "def", "gh", "ijkl"]);
        let array_ref = Arc::new(array) as ArrayRef;
        let condition = BooleanArray::from(vec![true, false, false, false, true]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        let expected =
            StringArray::from(vec![None, Some(""), Some("def"), Some("gh"), None]);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_nullif_strings_sliced() {
        let array = StringArray::from(vec!["abc", "", "def", "gh", "ijkl"]);
        let array_ref = array.slice(1, 4);
        let condition = BooleanArray::from(vec![false, false, false, true]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        let expected = StringArray::from(vec![Some(""), Some("def"), Some("gh"), None]);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_nullif_fixed_size_binary_sliced() {
        let array = FixedSizeBinaryArray::try_from_iter(
            vec![b"abc", b"def", b"ghi", b"jkl"].into_iter(),
        )
        .unwrap();
        let array_ref = array.slice(1, 2);
        let condition = BooleanArray::from(vec![false, false]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        let expected = FixedSizeBinaryArray::try_from_sparse_iter(
            vec![Some(b"def"), Some(b"ghi")].into_iter(),
        )
        .unwrap();

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_nullif_fixed_size_list_sliced() {
        let primitives =
            Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        let fixed_size_array = make_array(
            ArrayData::try_new(
                DataType::FixedSizeList(
                    Box::new(Field::new("item", primitives.data_type().clone(), true)),
                    3,
                ),
                5,
                None,
                None,
                0,
                vec![],
                vec![primitives.data().clone()],
            )
            .unwrap(),
        );
        let array_ref = fixed_size_array.slice(1, 3);
        let condition = BooleanArray::from(vec![false, true, false]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        let expected = make_array(
            ArrayData::try_new(
                DataType::FixedSizeList(
                    Box::new(Field::new("item", primitives.data_type().clone(), true)),
                    3,
                ),
                3,
                None,
                Some(MutableBuffer::from_iter(vec![true, false, true]).into()),
                0,
                vec![],
                vec![primitives.slice(3, 9).data().clone()],
            )
            .unwrap(),
        );
        let expected = expected
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_nullif_struct_sliced() {
        let array = StructArray::try_from(vec![
            (
                "a",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringArray::from(vec![
                    "abc", "de", "dghi", "jk", "lm", "nop", "q", "rstu",
                ])),
            ),
        ])
        .unwrap();
        let array_ref = array.slice(2, 4);

        let condition = BooleanArray::from(vec![false, true, false, false]);

        let result = nullif_alternative(&array_ref, &condition).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();

        let expected = StructArray::try_from(vec![
            (
                "a",
                Arc::new(Int32Array::from(vec![Some(3), None, Some(5), Some(6)]))
                    as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringArray::from(vec![
                    Some("dghi"),
                    None,
                    Some("lm"),
                    Some("nop"),
                ])),
            ),
        ])
        .unwrap();

        // StructArray comparison does not seem to respect validity bitmap of the struct itself
        // assert_eq!(result, &expected);

        assert!(result.is_valid(0));
        assert!(!result.is_valid(1));
        assert!(result.is_valid(2));
        assert!(result.is_valid(3));

        assert_eq!(
            result.column(0),
            &(Arc::new(Int32Array::from(vec![3, 4, 5, 6])) as ArrayRef)
        );
        assert_eq!(
            result.column(1),
            &(Arc::new(StringArray::from(vec!["dghi", "jk", "lm", "nop"])) as ArrayRef)
        );
    }
}
