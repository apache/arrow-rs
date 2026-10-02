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
use arrow_array::types::ByteArrayType;
use arrow_array::*;
use arrow_buffer::bit_util::ceil;
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_data::MAX_INLINE_VIEW_LEN;
use arrow_schema::{ArrowError, SortOptions};
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

/// Indicates an empty string
pub const EMPTY_SENTINEL: u8 = 1;

/// Indicates a non-empty string
pub const NON_EMPTY_SENTINEL: u8 = 2;

/// Indicates a Null value (for DataType::Null)
pub const NULL_VALUE_SENTINEL: u8 = 3;

/// Returns the padded length of the encoded length of the given length
#[inline]
pub fn padded_length(a: Option<usize>) -> usize {
    match a {
        Some(a) => non_null_padded_length(a),
        None => 1,
    }
}

/// Returns the padded length of the encoded length of the given length
#[inline]
pub(crate) fn non_null_padded_length(len: usize) -> usize {
    if len <= BLOCK_SIZE {
        1 + ceil(len, MINI_BLOCK_SIZE) * (MINI_BLOCK_SIZE + 1)
    } else {
        // Each miniblock ends with a 1 byte continuation, therefore add
        // `(MINI_BLOCK_COUNT - 1)` additional bytes over non-miniblock size
        MINI_BLOCK_COUNT + ceil(len, BLOCK_SIZE) * (BLOCK_SIZE + 1)
    }
}

/// Decodes a single byte from each row, where a valid value is determined via
/// a configurable sentinel. Optionally returns `None` if there are no null values.
pub(crate) fn decode_nulls_sentinel(rows: &[&[u8]], options: SortOptions) -> Option<NullBuffer> {
    let null_sentinel = null_sentinel(options);
    let nulls = BooleanBuffer::collect_bool(rows.len(), |x| rows[x][0] != null_sentinel);
    let nulls = NullBuffer::new(nulls);
    (nulls.null_count() > 0).then_some(nulls)
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

/// Calls [`encode`] with optimized iterator for generic byte arrays
pub(crate) fn encode_generic_byte_array<T: ByteArrayType>(
    data: &mut [u8],
    offsets: &mut [usize],
    input_array: &GenericByteArray<T>,
    opts: SortOptions,
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

        encode(data, offsets, input_iter, opts);
    } else {
        // Skip null checks
        let input_iter = input_offsets.windows(2).map(|start_end| {
            let item_range = start_end[0].as_usize()..start_end[1].as_usize();
            // SAFETY: the offsets of the input are valid by construction
            // so it is ok to use unsafe here
            let item = unsafe { bytes.get_unchecked(item_range) };
            Some(item)
        });

        encode(data, offsets, input_iter, opts);
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

/// Ensure `NullArray`s don't get encoded as empty lists which can lose their length
pub fn encode_null_value(out: &mut [u8], opts: SortOptions) -> usize {
    out[0] = match opts.descending {
        true => !NON_EMPTY_SENTINEL,
        false => NON_EMPTY_SENTINEL,
    };
    out[1] = match opts.descending {
        true => !NULL_VALUE_SENTINEL,
        false => NULL_VALUE_SENTINEL,
    };
    2
}

#[inline]
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

    let (chunks, remainder) = val.as_chunks::<SIZE>();
    #[expect(clippy::chunks_exact_to_as_chunks)]
    // Requires using generic parameters in const operations: generic_const_exprs
    let to_write_chunks = to_write.chunks_exact_mut(SIZE + 1);
    for (input, output) in chunks.iter().zip(to_write_chunks) {
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

/// Decodes a single block of data
/// The `f` function accepts a slice of the decoded data, it may be called multiple times
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
    let nulls = decode_nulls_sentinel(rows, options);

    let values_capacity = rows.iter().map(|row| decoded_len(row, options)).sum();
    let mut offsets = Vec::<I>::with_capacity(len + 1);
    offsets.push(I::zero());
    let mut values = MutableBuffer::new(values_capacity);

    for row in rows {
        let offset = decode_blocks(row, options, |b| values.extend_from_slice(b));
        *row = &row[offset..];
        offsets.push(I::from_usize(values.len()).expect("offset overflow"))
    }

    if options.descending {
        values.as_slice_mut().iter_mut().for_each(|o| *o = !*o)
    }

    // SAFETY:
    // Valid by construction above
    unsafe {
        GenericBinaryArray::new_unchecked(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values.into(),
            nulls,
        )
    }
}

/// The most bytes one data buffer of a view array can hold, since a view
/// addresses its value with a 32 bit offset into that buffer
const MAX_VIEW_BUFFER_LEN: usize = i32::MAX as usize;

fn decode_binary_view_inner<const VALIDATE_UTF8: bool>(
    rows: &mut [&[u8]],
    options: SortOptions,
) -> Result<BinaryViewArray, ArrowError> {
    let len = rows.len();
    let inline_str_max_len = MAX_INLINE_VIEW_LEN as usize;

    let nulls = decode_nulls_sentinel(rows, options);

    // Capacity for all long strings plus room for one short string
    let mut values_capacity = inline_str_max_len;
    let mut inline_capacity = 0;
    for row in rows.iter() {
        let len = decoded_len(row, options);
        if len > inline_str_max_len {
            values_capacity += len;
        } else if VALIDATE_UTF8 {
            inline_capacity += len;
        }
    }

    // One buffer only reaches as far as a 32 bit offset, so values that do not
    // all fit it are decoded onto as many buffers as they need
    if values_capacity - inline_str_max_len > MAX_VIEW_BUFFER_LEN {
        return decode_binary_view_rolling::<VALIDATE_UTF8>(
            rows,
            options,
            nulls,
            values_capacity,
            inline_capacity,
            MAX_VIEW_BUFFER_LEN,
        );
    }

    let mut values = MutableBuffer::new(values_capacity);
    let mut view_utf8_validation_buffer = if VALIDATE_UTF8 {
        Vec::with_capacity(inline_capacity)
    } else {
        Vec::new()
    };

    let null_sentinel = null_sentinel(options);
    let mut views = vec![0_u128; len];
    for (i, row) in rows.iter_mut().enumerate() {
        let start_offset = values.len();
        let offset = decode_blocks(row, options, |b| values.extend_from_slice(b));
        // Measure string length via change in values buffer. This way we can
        // overwrite short strings in the values buffer as we inline those to
        // views.
        let decoded_len = values.len() - start_offset;
        if row[0] == null_sentinel {
            debug_assert_eq!(offset, 1);
            debug_assert_eq!(start_offset, values.len());
        } else {
            // Safety: we just appended the data to the end of the buffer
            let val = unsafe { values.get_unchecked_mut(start_offset..) };

            if options.descending {
                val.iter_mut().for_each(|o| *o = !*o);
            }

            views[i] = make_view(val, 0, start_offset as u32);

            if decoded_len <= inline_str_max_len {
                if VALIDATE_UTF8 {
                    view_utf8_validation_buffer.extend_from_slice(val);
                }
                values.truncate(start_offset);
            }
        }
        *row = &row[offset..];
    }

    if VALIDATE_UTF8 {
        std::str::from_utf8(&values).unwrap();
        std::str::from_utf8(&view_utf8_validation_buffer).unwrap();
    }

    // SAFETY:
    // Valid by construction above
    Ok(unsafe { BinaryViewArray::new_unchecked(views.into(), [values.into()].into(), nulls) })
}

/// Decodes rows whose values outgrow one data buffer, rolling onto the next one
/// as the current buffer runs out of offsets to address them with.
///
/// `max_buffer_len` is a parameter so tests can force a roll without decoding
/// two gigabytes.
fn decode_binary_view_rolling<const VALIDATE_UTF8: bool>(
    rows: &mut [&[u8]],
    options: SortOptions,
    nulls: Option<NullBuffer>,
    values_capacity: usize,
    inline_capacity: usize,
    max_buffer_len: usize,
) -> Result<BinaryViewArray, ArrowError> {
    let len = rows.len();
    let inline_str_max_len = MAX_INLINE_VIEW_LEN as usize;

    // Values roll onto another buffer when they do not all fit one, but a single
    // value has to sit in one buffer for one view to address it
    let longest = rows
        .iter()
        .map(|row| decoded_len(row, options))
        .max()
        .unwrap_or(0);
    if longest > max_buffer_len {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{longest} byte value too long to decode into a view array, one data buffer addresses at most {max_buffer_len} bytes"
        )));
    }

    let mut remaining_long = values_capacity - inline_str_max_len;
    let mut completed: Vec<Buffer> = Vec::new();
    let mut block_id = 0_u32;
    let mut values = MutableBuffer::new(values_capacity.min(max_buffer_len));
    let mut view_utf8_validation_buffer = if VALIDATE_UTF8 {
        Vec::with_capacity(inline_capacity)
    } else {
        Vec::new()
    };

    let null_sentinel = null_sentinel(options);
    let mut views = vec![0_u128; len];
    for (i, row) in rows.iter_mut().enumerate() {
        let mut start_offset = values.len();
        let offset = decode_blocks(row, options, |b| values.extend_from_slice(b));
        let decoded_len = values.len() - start_offset;
        if row[0] == null_sentinel {
            debug_assert_eq!(offset, 1);
            debug_assert_eq!(start_offset, values.len());
        } else {
            // A value the current buffer cannot address moves to a fresh one and
            // every view from here on carries the next block id
            if decoded_len > inline_str_max_len && start_offset + decoded_len > max_buffer_len {
                let mut rolled =
                    MutableBuffer::new(remaining_long.min(max_buffer_len).max(decoded_len));
                rolled.extend_from_slice(&values[start_offset..]);
                values.truncate(start_offset);
                if VALIDATE_UTF8 {
                    std::str::from_utf8(&values).unwrap();
                }
                completed.push(std::mem::replace(&mut values, rolled).into());
                block_id += 1;
                start_offset = 0;
            }

            // Safety: we just appended the data to the end of the buffer
            let val = unsafe { values.get_unchecked_mut(start_offset..) };

            if options.descending {
                val.iter_mut().for_each(|o| *o = !*o);
            }

            views[i] = make_view(val, block_id, start_offset as u32);

            if decoded_len <= inline_str_max_len {
                if VALIDATE_UTF8 {
                    view_utf8_validation_buffer.extend_from_slice(val);
                }
                values.truncate(start_offset);
            } else {
                remaining_long -= decoded_len;
            }
        }
        *row = &row[offset..];
    }

    if VALIDATE_UTF8 {
        std::str::from_utf8(&values).unwrap();
        std::str::from_utf8(&view_utf8_validation_buffer).unwrap();
    }

    completed.push(values.into());

    // SAFETY:
    // Valid by construction above
    Ok(unsafe { BinaryViewArray::new_unchecked(views.into(), completed.into(), nulls) })
}

/// Decodes a binary view array from `rows` with the provided `options`
///
/// Values are spread over as many data buffers as they need. Returns an error if
/// a single value is longer than `i32::MAX` bytes, the most one data buffer can
/// address
pub fn decode_binary_view(
    rows: &mut [&[u8]],
    options: SortOptions,
) -> Result<BinaryViewArray, ArrowError> {
    decode_binary_view_inner::<false>(rows, options)
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

    let (offsets, values, nulls) = decoded.into_parts();

    // SAFETY:
    // Row data must have come from a valid UTF-8 array
    unsafe { GenericStringArray::new_unchecked(offsets, values, nulls) }
}

/// Decodes a string view array from `rows` with the provided `options`
///
/// # Safety
///
/// The row must contain valid UTF-8 data
///
/// Values are spread over as many data buffers as they need. Returns an error if
/// a single value is longer than `i32::MAX` bytes, the most one data buffer can
/// address
pub unsafe fn decode_string_view(
    rows: &mut [&[u8]],
    options: SortOptions,
    validate_utf8: bool,
) -> Result<StringViewArray, ArrowError> {
    let view = if validate_utf8 {
        decode_binary_view_inner::<true>(rows, options)?
    } else {
        decode_binary_view_inner::<false>(rows, options)?
    };
    Ok(unsafe { view.to_string_view_unchecked() })
}

pub fn decode_null_value(rows: &mut [&[u8]], options: SortOptions) {
    for row in rows.iter_mut() {
        let (sentinel1, sentinel2) = match options.descending {
            true => (!NON_EMPTY_SENTINEL, !NULL_VALUE_SENTINEL),
            false => (NON_EMPTY_SENTINEL, NULL_VALUE_SENTINEL),
        };
        debug_assert_eq!(row[0], sentinel1, "Expected NULL_VALUE_SENTINEL at byte 0");
        debug_assert_eq!(row[1], sentinel2, "Expected NULL_VALUE_SENTINEL at byte 1");
        *row = &row[2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RowConverter, SortField};
    use arrow_array::StringViewArray;
    use arrow_schema::DataType;
    use std::sync::Arc;

    fn encode(values: &StringViewArray, options: SortOptions) -> Vec<Vec<u8>> {
        let field = SortField::new_with_options(DataType::Utf8View, options);
        let converter = RowConverter::new(vec![field]).unwrap();
        let rows = converter
            .convert_columns(&[Arc::new(values.clone()) as _])
            .unwrap();
        rows.iter().map(|row| row.as_ref().to_vec()).collect()
    }

    /// Decodes with the rolling path, with a buffer length a test can reach
    fn decode_rolling(
        rows: &mut [&[u8]],
        options: SortOptions,
        cap: usize,
    ) -> Result<BinaryViewArray, ArrowError> {
        let inline_str_max_len = MAX_INLINE_VIEW_LEN as usize;
        let nulls = decode_nulls_sentinel(rows, options);
        let mut values_capacity = inline_str_max_len;
        let mut inline_capacity = 0;
        for row in rows.iter() {
            let len = decoded_len(row, options);
            if len > inline_str_max_len {
                values_capacity += len;
            } else {
                inline_capacity += len;
            }
        }
        decode_binary_view_rolling::<true>(
            rows,
            options,
            nulls,
            values_capacity,
            inline_capacity,
            cap,
        )
    }

    fn roundtrip(values: &StringViewArray, options: SortOptions, cap: usize) -> StringViewArray {
        let encoded = encode(values, options);
        let mut rows: Vec<&[u8]> = encoded.iter().map(|row| row.as_slice()).collect();
        let decoded = decode_rolling(&mut rows, options, cap).unwrap();
        unsafe { decoded.to_string_view_unchecked() }
    }

    #[test]
    fn values_past_the_buffer_length_roll_onto_the_next_buffer() {
        let values = StringViewArray::from(vec![
            Some("the quick brown fox jumped"),
            None,
            Some("inline"),
            Some("over the lazy dog and kept going"),
            Some(""),
            Some("and a third one longer than inline"),
        ]);

        for descending in [false, true] {
            for nulls_first in [false, true] {
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                let decoded = roundtrip(&values, options, 40);
                assert!(
                    decoded.data_buffers().len() > 1,
                    "expected a roll, got {} buffer",
                    decoded.data_buffers().len()
                );
                assert_eq!(
                    decoded.iter().collect::<Vec<_>>(),
                    values.iter().collect::<Vec<_>>(),
                    "descending={descending} nulls_first={nulls_first}"
                );

                let decoded = roundtrip(&values, options, 4096);
                assert_eq!(decoded.data_buffers().len(), 1);
                assert_eq!(
                    decoded.iter().collect::<Vec<_>>(),
                    values.iter().collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn one_value_longer_than_a_whole_buffer_is_an_error() {
        let values = StringViewArray::from(vec![Some("a value longer than the cap below")]);
        let encoded = encode(&values, SortOptions::default());
        let mut rows: Vec<&[u8]> = encoded.iter().map(|row| row.as_slice()).collect();
        let err = decode_rolling(&mut rows, SortOptions::default(), 20)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("too long to decode into a view array"),
            "{err}"
        );
    }
}
