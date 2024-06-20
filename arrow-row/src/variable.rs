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

use crate::null_sentinel;
use arrow_array::builder::BufferBuilder;
use arrow_array::*;
use arrow_buffer::bit_util::ceil;
use arrow_buffer::MutableBuffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType, SortOptions};

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
        Some(a) if a <= BLOCK_SIZE => 1 + ceil(a, MINI_BLOCK_SIZE) * (MINI_BLOCK_SIZE + 1),
        // Each miniblock ends with a 1 byte continuation, therefore add
        // `(MINI_BLOCK_COUNT - 1)` additional bytes over non-miniblock size
        Some(a) => MINI_BLOCK_COUNT + ceil(a, BLOCK_SIZE) * (BLOCK_SIZE + 1),
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
    data: &mut [u8],
    offsets: &mut [usize],
    i: I,
    opts: SortOptions,
) {
    for (offset, maybe_val) in offsets.iter_mut().skip(1).zip(i) {
        *offset += encode_one(&mut data[*offset..], maybe_val, opts);
    }
}

pub fn encode_null(out: &mut [u8], opts: SortOptions) -> usize {
    out[0] = null_sentinel(opts);
    1
}

pub fn encode_empty(out: &mut [u8], opts: SortOptions) -> usize {
    out[0] = match opts.descending {
        true => !EMPTY_SENTINEL,
        false => EMPTY_SENTINEL,
    };
    1
}

pub fn encode_one(out: &mut [u8], val: Option<&[u8]>, opts: SortOptions) -> usize {
    match val {
        None => encode_null(out, opts),
        Some([]) => encode_empty(out, opts),
        Some(val) => {
            // Write `2_u8` to demarcate as non-empty, non-null string
            out[0] = NON_EMPTY_SENTINEL;

            let len = if val.len() <= BLOCK_SIZE {
                1 + encode_blocks::<MINI_BLOCK_SIZE>(&mut out[1..], val)
            } else {
                let (initial, rem) = val.split_at(BLOCK_SIZE);
                let offset = encode_blocks::<MINI_BLOCK_SIZE>(&mut out[1..], initial);
                out[offset] = BLOCK_CONTINUATION;
                1 + offset + encode_blocks::<BLOCK_SIZE>(&mut out[1 + offset..], rem)
            };

            if opts.descending {
                // Invert bits
                out[..len].iter_mut().for_each(|v| *v = !*v)
            }
            len
        }
    }
}

/// Writes `val` in `SIZE` blocks with the appropriate continuation tokens
#[inline]
fn encode_blocks<const SIZE: usize>(out: &mut [u8], val: &[u8]) -> usize {
    let block_count = ceil(val.len(), SIZE);
    let end_offset = block_count * (SIZE + 1);
    let to_write = &mut out[..end_offset];

    let chunks = val.chunks_exact(SIZE);
    let remainder = chunks.remainder();
    for (input, output) in chunks.clone().zip(to_write.chunks_exact_mut(SIZE + 1)) {
        let input: &[u8; SIZE] = input.try_into().unwrap();
        let out_block: &mut [u8; SIZE] = (&mut output[..SIZE]).try_into().unwrap();

        *out_block = *input;

        // Indicate that there are further blocks to follow
        output[SIZE] = BLOCK_CONTINUATION;
    }

    if !remainder.is_empty() {
        let start_offset = (block_count - 1) * (SIZE + 1);
        to_write[start_offset..start_offset + remainder.len()].copy_from_slice(remainder);
        *to_write.last_mut().unwrap() = remainder.len() as u8;
    } else {
        // We must overwrite the continuation marker written by the loop above
        *to_write.last_mut().unwrap() = SIZE as u8;
    }
    end_offset
}

pub fn decode_blocks(row: &[u8], options: SortOptions, mut f: impl FnMut(&[u8])) -> usize {
    let (non_empty_sentinel, continuation) = match options.descending {
        true => (!NON_EMPTY_SENTINEL, !BLOCK_CONTINUATION),
        false => (NON_EMPTY_SENTINEL, BLOCK_CONTINUATION),
    };

    if row[0] != non_empty_sentinel {
        // Empty or null string
        return 1;
    }

    // Extracts the block length from the sentinel
    let block_len = |sentinel: u8| match options.descending {
        true => !sentinel as usize,
        false => sentinel as usize,
    };

    let mut idx = 1;
    for _ in 0..MINI_BLOCK_COUNT {
        let sentinel = row[idx + MINI_BLOCK_SIZE];
        if sentinel != continuation {
            f(&row[idx..idx + block_len(sentinel)]);
            return idx + MINI_BLOCK_SIZE + 1;
        }
        f(&row[idx..idx + MINI_BLOCK_SIZE]);
        idx += MINI_BLOCK_SIZE + 1;
    }

    loop {
        let sentinel = row[idx + BLOCK_SIZE];
        if sentinel != continuation {
            f(&row[idx..idx + block_len(sentinel)]);
            return idx + BLOCK_SIZE + 1;
        }
        f(&row[idx..idx + BLOCK_SIZE]);
        idx += BLOCK_SIZE + 1;
    }
}

/// Returns the number of bytes of encoded data
fn decoded_len(row: &[u8], options: SortOptions) -> usize {
    let mut len = 0;
    decode_blocks(row, options, |block| len += block.len());
    len
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
        let offset = decode_blocks(row, options, |b| values.extend_from_slice(b));
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

/// Decodes a binary view array from `rows` with the provided `options`
pub fn decode_binary_view(rows: &mut [&[u8]], options: SortOptions) -> BinaryViewArray {
    let decoded: GenericBinaryArray<i64> = decode_binary(rows, options);

    // Better performance might be to directly build the binary view instead of building to BinaryArray and then casting
    // I suspect that the overhead is not a big deal.
    // If it is, we can reimplement the `decode_binary_view` function to directly build the StringViewArray
    BinaryViewArray::from(&decoded)
}

/// Decodes a string view array from `rows` with the provided `options`
///
/// # Safety
///
/// The row must contain valid UTF-8 data
pub unsafe fn decode_string_view(
    rows: &mut [&[u8]],
    options: SortOptions,
    validate_utf8: bool,
) -> StringViewArray {
    let decoded: GenericStringArray<i64> = decode_string(rows, options, validate_utf8);
    // Better performance might be to directly build the string view instead of building to StringArray and then casting
    // I suspect that the overhead is not a big deal.
    // If it is, we can reimplement the `decode_string_view` function to directly build the StringViewArray
    StringViewArray::from(&decoded)
}
