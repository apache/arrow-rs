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

use super::null_sentinel;
use arrow_array::builder::BufferBuilder;
use arrow_array::types::ByteArrayType;
use arrow_array::*;
use arrow_buffer::bit_util::ceil;
use arrow_buffer::{ArrowNativeType, MutableBuffer};
use arrow_data::{ArrayDataBuilder, MAX_INLINE_VIEW_LEN};
use arrow_schema::DataType;
use builder::make_view;

/// The block size of the variable length encoding
pub const BLOCK_SIZE: usize = 32;

/// The first block is split into `MINI_BLOCK_COUNT` mini-blocks
///
/// This helps to reduce the space amplification for small strings
pub const MINI_BLOCK_COUNT: usize = 4;

/// The mini block size
pub const MINI_BLOCK_SIZE: usize = BLOCK_SIZE / MINI_BLOCK_COUNT;

/// The continuation token
pub const BLOCK_CONTINUATION: u8 = 0xFF;

pub const EMPTY_SENTINEL: u8 = 0b00000001;

/// Indicates a non-empty string
pub const NON_EMPTY_SENTINEL: u8 = 0b00000010;
pub const NULL_SENTINEL: u8 = null_sentinel();

// u8 must be smaller value than u16 in the bit representation so we can sort by them
pub const LENGTH_TYPE_U8: u8 =  0b00000100;
pub const LENGTH_TYPE_U16: u8 = 0b00001000;
pub const LENGTH_TYPE_U32: u8 = 0b00010000;
pub const LENGTH_TYPE_U64: u8 = 0b00100000;

/// Returns the length of the encoded representation of a byte array, including the null byte
#[inline]
pub fn encoded_len(a: Option<&[u8]>) -> usize {
    padded_length(a.map(|x| x.len()))
}


#[inline]
fn get_number_of_bits_needed_to_encode(len: usize) -> usize {
    (usize::BITS as usize - len.leading_zeros() as usize + 7) / 8
}

/// Returns the padded length of the encoded length of the given length
#[inline]
pub fn padded_length(a: Option<usize>) -> usize {
    let value_len = match a {
        None => 0,
        Some(a) if a == 0 => 0,
        Some(a) => get_number_of_bits_needed_to_encode(a) + a,
    };

    value_len
      // ctrl byte
      + 1
}

/// Variable length values are encoded as
///
/// - single `0_u8` if null
/// - single `1_u8` if empty array
/// - `2_u8` if not empty, followed by one or more blocks
///
/// where a block is encoded as
///
/// - [`BLOCK_SIZE`] bytes of string data, padded with 0s
/// - `0xFF_u8` if this is not the last block for this string
/// - otherwise the length of the block as a `u8`
pub fn encode<'a, I: Iterator<Item = Option<&'a [u8]>>>(
    data: &mut [u8],
    offsets: &mut [usize],
    i: I,
) {
    for (offset, maybe_val) in offsets.iter_mut().skip(1).zip(i) {
        *offset += encode_one(&mut data[*offset..], maybe_val);
    }
}

/// Calls [`encode`] with optimized iterator for generic byte arrays
pub(crate) fn encode_generic_byte_array<T: ByteArrayType>(
    data: &mut [u8],
    offsets: &mut [usize],
    input_array: &GenericByteArray<T>,
) {
    let input_offsets = input_array.value_offsets();
    let bytes = input_array.values().as_slice();

    if let Some(null_buffer) = input_array.nulls().filter(|x| x.null_count() > 0) {
        let input_iter =
            input_offsets
                .windows(2)
                .zip(null_buffer.iter())
                .map(|(start_end, is_valid)| {
                    if is_valid {
                        let item_range = start_end[0].as_usize()..start_end[1].as_usize();
                        // SAFETY: the offsets of the input are valid by construction
                        // so it is ok to use unsafe here
                        let item = unsafe { bytes.get_unchecked(item_range) };
                        Some(item)
                    } else {
                        None
                    }
                });

        encode(data, offsets, input_iter);
    } else {
        // Skip null checks
        let input_iter = input_offsets.windows(2).map(|start_end| {
            let item_range = start_end[0].as_usize()..start_end[1].as_usize();
            // SAFETY: the offsets of the input are valid by construction
            // so it is ok to use unsafe here
            let item = unsafe { bytes.get_unchecked(item_range) };
            Some(item)
        });

        encode(data, offsets, input_iter);
    }
}

pub fn encode_null(out: &mut [u8]) -> usize {
    out[0] = null_sentinel();
    1
}


#[inline]
pub fn encode_one(out: &mut [u8], val: Option<&[u8]>) -> usize {
    match val {
        None => encode_null(out),
        Some(val) => fast_encode_bytes(out, val),
    }
}

/// Faster encode_blocks that first copy all the data and then iterate over it and
#[inline]
pub(crate) fn fast_encode_bytes(out: &mut [u8], val: &[u8]) -> usize {
    // Write `2_u8` to demarcate as non-empty, non-null string
    out[0] = NON_EMPTY_SENTINEL;

    // TODO - in desc should do max minus the length so the order will be different (longer strings sort before shorter ones)
    let start_data_offset = {
        let len = val.len();

        match get_number_of_bits_needed_to_encode(len) {
            0 => {
                out[0] = EMPTY_SENTINEL;
                return 1;
            }
            1 => {
                out[0] |= LENGTH_TYPE_U8;

                // encode length
                let start_data_offset = 1 + size_of::<u8>();
                unsafe { out.get_unchecked_mut(1..start_data_offset) }.copy_from_slice(&(len as u8).to_be_bytes());

                start_data_offset
            }
            2 => {
                out[0] |= LENGTH_TYPE_U16;

                // encode length
                let start_data_offset = 1 + size_of::<u16>();
                unsafe { out.get_unchecked_mut(1..start_data_offset) }.copy_from_slice(&(len as u16).to_be_bytes());

                start_data_offset
            }
            4 => {
                out[0] |= LENGTH_TYPE_U32;

                // encode length
                let start_data_offset = 1 + size_of::<u32>();
                unsafe { out.get_unchecked_mut(1..start_data_offset) }.copy_from_slice(&(len as u32).to_be_bytes());

                start_data_offset
            }
            8 => {
                out[0] |= LENGTH_TYPE_U64;

                // encode length
                let start_data_offset = 1 + size_of::<u64>();
                unsafe { out.get_unchecked_mut(1..start_data_offset) }.copy_from_slice(&(len as u64).to_be_bytes());

                start_data_offset
            }
            bits_required => {
                unreachable!("invalid length type {len}. numbr of bits required {bits_required}");
            }
        }
    };

    let len = start_data_offset + val.len();
    out[start_data_offset..len].copy_from_slice(val);

    len
}

/// Decodes a single block of data
/// The `f` function accepts a slice of the decoded data, it may be called multiple times
pub fn decode_blocks_fast(row: &[u8], f: impl FnMut(&[u8])) -> usize {
    decode_blocks_fast_order(row, f)
}

/// Decodes a single block of data
/// The `f` function accepts a slice of the decoded data, it may be called multiple times
pub fn decode_blocks_fast_order(row: &[u8], mut f: impl FnMut(&[u8])) -> usize {
    // TODO - we can avoid the no if we change the ifs
    let normalized_ctrl_byte = row[0];

    if normalized_ctrl_byte == EMPTY_SENTINEL || normalized_ctrl_byte == NULL_SENTINEL {
        // Empty or null string
        return 1;
    }

    let (len, start_offset) = if normalized_ctrl_byte & LENGTH_TYPE_U8 > 0 {
        let len_normalized = row[1];
        let len = len_normalized as usize;
        (len, size_of::<u8>())
    } else if normalized_ctrl_byte & LENGTH_TYPE_U16 > 0 {
        let bytes = &row[1..3];
        let bytes_array: [u8; 2] = bytes.try_into().unwrap();
        // let bytes_needed: [u8; 2] = row[1..=1 + size_of::<u16>()].try_into().unwrap();
        let raw_len = u16::from_be_bytes(bytes_array);
        let len_normalized = raw_len;

        (len_normalized as usize, size_of::<u16>())
    } else if normalized_ctrl_byte & LENGTH_TYPE_U32 > 0 {
        let bytes_needed: [u8; 4] = row[1..=1 + size_of::<u32>()].try_into().unwrap();
        let raw_len = u32::from_be_bytes(bytes_needed);
        let len_normalized = raw_len;

        (len_normalized as usize, size_of::<u32>())
    } else if normalized_ctrl_byte & LENGTH_TYPE_U64 > 0 {
        let bytes_needed: [u8; 8] = row[1..=1 + size_of::<u64>()].try_into().unwrap();
        let raw_len = u64::from_be_bytes(bytes_needed);
        let len_normalized = raw_len;

        (len_normalized as usize, size_of::<u64>())
    } else {
        unreachable!("invalid length type");
    };

    // + 1 for the control byte
    let start_offset = start_offset + 1;

    f(&row[start_offset..start_offset + len]);
    start_offset + len
}
//
// /// Writes `val` in `SIZE` blocks with the appropriate continuation tokens
// #[inline]
// fn encode_mini_blocks(out: &mut [u8], val: &[u8]) -> usize {
//     const SIZE: usize = MINI_BLOCK_SIZE;
//
//
//     let block_count = ceil(val.len(), SIZE);
//     let end_offset = block_count * (SIZE + 1);
//     let to_write = &mut out[..end_offset];
//
//     let chunks = val.chunks_exact(SIZE);
//     let remainder = chunks.remainder();
//     for (input, output) in chunks.clone().zip(to_write.chunks_exact_mut(SIZE + 1)) {
//         let input: &[u8; SIZE] = input.try_into().unwrap();
//         let out_block: &mut [u8; SIZE] = (&mut output[..SIZE]).try_into().unwrap();
//
//         *out_block = *input;
//
//         // Indicate that there are further blocks to follow
//         output[SIZE] = BLOCK_CONTINUATION;
//     }
//
//     if !remainder.is_empty() {
//         let start_offset = (block_count - 1) * (SIZE + 1);
//         to_write[start_offset..start_offset + remainder.len()].copy_from_slice(remainder);
//         *to_write.last_mut().unwrap() = remainder.len() as u8;
//     } else {
//         // We must overwrite the continuation marker written by the loop above
//         *to_write.last_mut().unwrap() = SIZE as u8;
//     }
//     end_offset
// }


/// Decodes a single block of data
/// The `f` function accepts a slice of the decoded data, it may be called multiple times
pub fn decode_blocks(row: &[u8], mut f: impl FnMut(&[u8])) -> usize {
    decode_blocks_fast(row, &mut f)
}

/// Returns the number of bytes of encoded data
fn decoded_len(row: &[u8]) -> usize {
    let mut len = 0;
    decode_blocks(row, |block| len += block.len());
    len
}

/// Decodes a binary array from `rows` with the provided `options`
pub fn decode_binary<I: OffsetSizeTrait>(
    rows: &mut [&[u8]],
) -> GenericBinaryArray<I> {
    let len = rows.len();
    let mut null_count = 0;
    let nulls = MutableBuffer::collect_bool(len, |x| {
        let valid = rows[x][0] != null_sentinel();
        null_count += !valid as usize;
        valid
    });

    let values_capacity = rows.iter().map(|row| decoded_len(row)).sum();
    let mut offsets = BufferBuilder::<I>::new(len + 1);
    offsets.append(I::zero());
    let mut values = MutableBuffer::new(values_capacity);

    for row in rows {
        let offset = decode_blocks(row, |b| values.extend_from_slice(b));
        *row = &row[offset..];
        offsets.append(I::from_usize(values.len()).expect("offset overflow"))
    }

    let d = match I::IS_LARGE {
        true => DataType::LargeBinary,
        false => DataType::Binary,
    };

    let builder = ArrayDataBuilder::new(d)
        .len(len)
        .null_count(null_count)
        .null_bit_buffer(Some(nulls.into()))
        .add_buffer(offsets.finish())
        .add_buffer(values.into());

    // SAFETY:
    // Valid by construction above
    unsafe { GenericBinaryArray::from(builder.build_unchecked()) }
}

fn decode_binary_view_inner(
    rows: &mut [&[u8]],
    validate_utf8: bool,
) -> BinaryViewArray {
    let len = rows.len();
    let inline_str_max_len = MAX_INLINE_VIEW_LEN as usize;

    let mut null_count = 0;

    let nulls = MutableBuffer::collect_bool(len, |x| {
        let valid = rows[x][0] != null_sentinel();
        null_count += !valid as usize;
        valid
    });

    // If we are validating UTF-8, decode all string values (including short strings)
    // into the values buffer and validate UTF-8 once. If not validating,
    // we save memory by only copying long strings to the values buffer, as short strings
    // will be inlined into the view and do not need to be stored redundantly.
    let values_capacity = if validate_utf8 {
        // Capacity for all long and short strings
        rows.iter().map(|row| decoded_len(row)).sum()
    } else {
        // Capacity for all long strings plus room for one short string
        rows.iter().fold(0, |acc, row| {
            let len = decoded_len(row);
            if len > inline_str_max_len {
                acc + len
            } else {
                acc
            }
        }) + inline_str_max_len
    };
    let mut values = MutableBuffer::new(values_capacity);

    let mut views = BufferBuilder::<u128>::new(len);
    for row in rows {
        let start_offset = values.len();
        let offset = decode_blocks(row, |b| values.extend_from_slice(b));
        // assert_eq!(values.len(), start_offset + offset, "offset is too large");
        // Measure string length via change in values buffer.
        // Used to check if decoded value should be truncated (short string) when validate_utf8 is false
        let decoded_len = values.len() - start_offset;
        if row[0] == null_sentinel() {
            debug_assert_eq!(offset, 1);
            debug_assert_eq!(start_offset, values.len());
            views.append(0);
        } else {
            // Safety: we just appended the data to the end of the buffer
            let val = unsafe { values.get_unchecked_mut(start_offset..) };


            let view = make_view(val, 0, start_offset as u32);
            views.append(view);

            // truncate inline string in values buffer if validate_utf8 is false
            if !validate_utf8 && decoded_len <= inline_str_max_len {
                values.truncate(start_offset);
            }
        }
        *row = &row[offset..];
    }

    if validate_utf8 {
        // the values contains all data, no matter if it is short or long
        // we can validate utf8 in one go.
        std::str::from_utf8(values.as_slice()).unwrap();
    }

    let builder = ArrayDataBuilder::new(DataType::BinaryView)
        .len(len)
        .null_count(null_count)
        .null_bit_buffer(Some(nulls.into()))
        .add_buffer(views.finish())
        .add_buffer(values.into());

    // SAFETY:
    // Valid by construction above
    unsafe { BinaryViewArray::from(builder.build_unchecked()) }
}

/// Decodes a binary view array from `rows` with the provided `options`
pub fn decode_binary_view(rows: &mut [&[u8]]) -> BinaryViewArray {
    decode_binary_view_inner(rows, false)
}

/// Decodes a string array from `rows` with the provided `options`
///
/// # Safety
///
/// The row must contain valid UTF-8 data
pub unsafe fn decode_string<I: OffsetSizeTrait>(
    rows: &mut [&[u8]],
    validate_utf8: bool,
) -> GenericStringArray<I> {
    let decoded = decode_binary::<I>(rows);

    if validate_utf8 {
        return GenericStringArray::from(decoded);
    }

    let builder = decoded
        .into_data()
        .into_builder()
        .data_type(GenericStringArray::<I>::DATA_TYPE);

    // SAFETY:
    // Row data must have come from a valid UTF-8 array
    GenericStringArray::from(unsafe { builder.build_unchecked() })
}

/// Decodes a string view array from `rows` with the provided `options`
///
/// # Safety
///
/// The row must contain valid UTF-8 data
pub unsafe fn decode_string_view(
    rows: &mut [&[u8]],
    validate_utf8: bool,
) -> StringViewArray {
    let view = decode_binary_view_inner(rows, validate_utf8);
    unsafe { view.to_string_view_unchecked() }
}
