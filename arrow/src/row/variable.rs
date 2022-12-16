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

use crate::compute::SortOptions;
use crate::row::{null_sentinel, Rows};
use crate::util::bit_util::ceil;
use arrow_array::builder::BufferBuilder;
use arrow_array::{Array, GenericBinaryArray, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::MutableBuffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;

/// The block size of the variable length encoding
pub const BLOCK_SIZE: usize = 32;

/// The continuation token
pub const BLOCK_CONTINUATION: u8 = 0xFF;

/// Indicates an empty string
pub const EMPTY_SENTINEL: u8 = 1;

/// Indicates a non-empty string
pub const NON_EMPTY_SENTINEL: u8 = 2;

/// Returns the length of the encoded representation of a byte array, including the null byte
#[inline]
pub fn encoded_len(a: Option<&[u8]>) -> usize {
    padded_length(a.map(|x| x.len()))
}

/// Returns the padded length of the encoded length of the given length
#[inline]
pub fn padded_length(a: Option<usize>) -> usize {
    match a {
        Some(a) => 1 + ceil(a, BLOCK_SIZE) * (BLOCK_SIZE + 1),
        None => 1,
    }
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
    out: &mut Rows,
    i: I,
    opts: SortOptions,
) {
    for (offset, maybe_val) in out.offsets.iter_mut().skip(1).zip(i) {
        *offset += encode_one(&mut out.buffer[*offset..], maybe_val, opts);
    }
}

pub fn encode_one(out: &mut [u8], val: Option<&[u8]>, opts: SortOptions) -> usize {
    match val {
        Some(val) if val.is_empty() => {
            out[0] = match opts.descending {
                true => !EMPTY_SENTINEL,
                false => EMPTY_SENTINEL,
            };
            1
        }
        Some(val) => {
            let block_count = ceil(val.len(), BLOCK_SIZE);
            let end_offset = 1 + block_count * (BLOCK_SIZE + 1);
            let to_write = &mut out[..end_offset];

            // Write `2_u8` to demarcate as non-empty, non-null string
            to_write[0] = NON_EMPTY_SENTINEL;

            let chunks = val.chunks_exact(BLOCK_SIZE);
            let remainder = chunks.remainder();
            for (input, output) in chunks
                .clone()
                .zip(to_write[1..].chunks_exact_mut(BLOCK_SIZE + 1))
            {
                let input: &[u8; BLOCK_SIZE] = input.try_into().unwrap();
                let out_block: &mut [u8; BLOCK_SIZE] =
                    (&mut output[..BLOCK_SIZE]).try_into().unwrap();

                *out_block = *input;

                // Indicate that there are further blocks to follow
                output[BLOCK_SIZE] = BLOCK_CONTINUATION;
            }

            if !remainder.is_empty() {
                let start_offset = 1 + (block_count - 1) * (BLOCK_SIZE + 1);
                to_write[start_offset..start_offset + remainder.len()]
                    .copy_from_slice(remainder);
                *to_write.last_mut().unwrap() = remainder.len() as u8;
            } else {
                // We must overwrite the continuation marker written by the loop above
                *to_write.last_mut().unwrap() = BLOCK_SIZE as u8;
            }

            if opts.descending {
                // Invert bits
                to_write.iter_mut().for_each(|v| *v = !*v)
            }
            end_offset
        }
        None => {
            out[0] = null_sentinel(opts);
            1
        }
    }
}

/// Returns the number of bytes of encoded data
fn decoded_len(row: &[u8], options: SortOptions) -> usize {
    let (non_empty_sentinel, continuation) = match options.descending {
        true => (!NON_EMPTY_SENTINEL, !BLOCK_CONTINUATION),
        false => (NON_EMPTY_SENTINEL, BLOCK_CONTINUATION),
    };

    if row[0] != non_empty_sentinel {
        // Empty or null string
        return 0;
    }

    let mut str_len = 0;
    let mut idx = 1;
    loop {
        let sentinel = row[idx + BLOCK_SIZE];
        if sentinel == continuation {
            idx += BLOCK_SIZE + 1;
            str_len += BLOCK_SIZE;
            continue;
        }
        let block_len = match options.descending {
            true => !sentinel,
            false => sentinel,
        };
        return str_len + block_len as usize;
    }
}

/// Decodes a binary array from `rows` with the provided `options`
pub fn decode_binary<I: OffsetSizeTrait>(
    rows: &mut [&[u8]],
    options: SortOptions,
) -> GenericBinaryArray<I> {
    let len = rows.len();
    let mut null_count = 0;
    let nulls = MutableBuffer::collect_bool(len, |x| {
        let valid = rows[x][0] != null_sentinel(options);
        null_count += !valid as usize;
        valid
    });

    let values_capacity = rows.iter().map(|row| decoded_len(row, options)).sum();
    let mut offsets = BufferBuilder::<I>::new(len + 1);
    offsets.append(I::zero());
    let mut values = MutableBuffer::new(values_capacity);

    for row in rows {
        let str_length = decoded_len(row, options);
        let mut to_read = str_length;
        let mut offset = 1;
        while to_read >= BLOCK_SIZE {
            to_read -= BLOCK_SIZE;

            values.extend_from_slice(&row[offset..offset + BLOCK_SIZE]);
            offset += BLOCK_SIZE + 1;
        }

        if to_read != 0 {
            values.extend_from_slice(&row[offset..offset + to_read]);
            offset += BLOCK_SIZE + 1;
        }
        *row = &row[offset..];

        offsets.append(I::from_usize(values.len()).expect("offset overflow"))
    }

    if options.descending {
        values.as_slice_mut().iter_mut().for_each(|o| *o = !*o)
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

/// Decodes a string array from `rows` with the provided `options`
///
/// # Safety
///
/// The row must contain valid UTF-8 data
pub unsafe fn decode_string<I: OffsetSizeTrait>(
    rows: &mut [&[u8]],
    options: SortOptions,
    validate_utf8: bool,
) -> GenericStringArray<I> {
    let decoded = decode_binary::<I>(rows, options);

    if validate_utf8 {
        return GenericStringArray::from(decoded);
    }

    let builder = decoded
        .into_data()
        .into_builder()
        .data_type(GenericStringArray::<I>::DATA_TYPE);

    // SAFETY:
    // Row data must have come from a valid UTF-8 array
    GenericStringArray::from(builder.build_unchecked())
}
