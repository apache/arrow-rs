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

use crate::arrow::buffer::bit_util::iter_set_bits_rev;
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::errors::{ParquetError, Result};
use crate::util::utf8::check_valid_utf8;
use arrow_array::{ArrayRef, OffsetSizeTrait, make_array};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowType;

/// A buffer of variable-sized byte arrays that can be converted into
/// a corresponding [`ArrayRef`]
#[derive(Debug)]
pub struct OffsetBuffer<I: OffsetSizeTrait> {
    pub offsets: Vec<I>,
    pub values: Vec<u8>,
}

impl<I: OffsetSizeTrait> OffsetBuffer<I> {
    /// Create a new `OffsetBuffer` with capacity for at least `capacity` elements
    ///
    /// Pre-allocates the offsets vector to avoid reallocations during reading.
    /// The values vector is not pre-allocated as its size is unpredictable.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(I::default());
        Self {
            offsets,
            values: Vec::new(),
        }
    }

    /// Returns the number of byte arrays in this buffer
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// If `validate_utf8` this verifies that the first character of `data` is
    /// the start of a UTF-8 codepoint
    ///
    /// Note: This does not verify that the entirety of `data` is valid
    /// UTF-8. This should be done by calling [`Self::check_valid_utf8`] after
    /// all data has been written
    pub fn try_push(&mut self, data: &[u8], validate_utf8: bool) -> Result<()> {
        if validate_utf8 && let Some(&b) = data.first() {
            // A valid code-point iff it does not start with 0b10xxxxxx
            // Bit-magic taken from `std::str::is_char_boundary`
            if (b as i8) < -0x40 {
                return Err(ParquetError::General(
                    "encountered non UTF-8 data".to_string(),
                ));
            }
        }

        self.values.extend_from_slice(data);

        let index_offset = I::from_usize(self.values.len())
            .ok_or_else(|| general_err!("index overflow decoding byte array"))?;

        self.offsets.push(index_offset);
        Ok(())
    }

    /// Extends this buffer with a list of keys
    ///
    /// For each value `key` in `keys` this will insert
    /// `&dict_values[dict_offsets[key]..dict_offsets[key+1]]`
    ///
    /// Note: This will validate offsets are valid
    pub fn extend_from_dictionary<K: ArrowNativeType, V: ArrowNativeType>(
        &mut self,
        keys: &[K],
        dict_offsets: &[V],
        dict_values: &[u8],
    ) -> Result<()> {
        self.offsets.reserve(keys.len());
        let offsets_start = self.offsets.len();
        let offsets_dst = self.offsets.as_mut_ptr();

        // Values and offsets are written through raw pointers, as this is
        // much faster than `extend_from_slice` and `push` for short values.
        // `len` bytes of `self.values` and `offsets_start + count` offsets
        // are initialized, and the lengths are updated before returning.
        let mut len = self.values.len();
        let mut dst = self.values.as_mut_ptr();
        let mut count = 0;
        let mut result = Ok(());
        for key in keys {
            let index = key.as_usize();
            // Not `index + 1`, which overflows for a negative key
            if index >= dict_offsets.len().saturating_sub(1) {
                result = Err(general_err!(
                    "dictionary key beyond bounds of dictionary: 0..{}",
                    dict_offsets.len().saturating_sub(1)
                ));
                break;
            }
            let start_offset = dict_offsets[index].as_usize();
            let end_offset = dict_offsets[index + 1].as_usize();
            if start_offset > end_offset || end_offset > dict_values.len() {
                result = Err(general_err!("invalid dictionary offsets"));
                break;
            }
            let value_len = end_offset - start_offset;
            let Some(offset) = I::from_usize(len + value_len) else {
                result = Err(general_err!("index overflow decoding byte array"));
                break;
            };

            if self.values.capacity() - len < value_len {
                // SAFETY: the first `len` bytes and `offsets_start + count`
                // offsets are initialized
                unsafe {
                    self.values.set_len(len);
                    self.offsets.set_len(offsets_start + count);
                }
                self.values.reserve(value_len);
                dst = self.values.as_mut_ptr();
            }
            // SAFETY: `start_offset + value_len <= dict_values.len()` was
            // checked above, `self.values` has capacity for `len + value_len`
            // bytes, and it is a different allocation from `dict_values`
            unsafe {
                copy_value(
                    dict_values,
                    start_offset,
                    value_len,
                    dst.add(len),
                    self.values.capacity() - len,
                )
            };
            len += value_len;
            // SAFETY: capacity for `keys.len()` more offsets was reserved above,
            // and `count < keys.len()`
            unsafe { offsets_dst.add(offsets_start + count).write(offset) };
            count += 1;
        }
        // SAFETY: the first `len` bytes and `offsets_start + count` offsets
        // were initialized above
        unsafe {
            self.values.set_len(len);
            self.offsets.set_len(offsets_start + count);
        }
        result
    }

    /// Validates that `&self.values[start_offset..]` is a valid UTF-8 sequence
    ///
    /// This MUST be combined with validating that the offsets start on a character
    /// boundary, otherwise it would be possible for the values array to be a valid UTF-8
    /// sequence, but not the individual string slices it contains
    ///
    /// [`Self::try_push`] can perform this validation check on insertion
    pub fn check_valid_utf8(&self, start_offset: usize) -> Result<()> {
        check_valid_utf8(&self.values.as_slice()[start_offset..])
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(self, null_buffer: Option<Buffer>, data_type: ArrowType) -> ArrayRef {
        let array_data_builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .add_buffer(Buffer::from_vec(self.offsets))
            .add_buffer(Buffer::from_vec(self.values))
            .null_bit_buffer(null_buffer);

        let data = match cfg!(debug_assertions) {
            true => array_data_builder.build().unwrap(),
            false => unsafe { array_data_builder.build_unchecked() },
        };

        make_array(data)
    }
}

/// Number of bytes [`copy_value`] copies at once
const COPY_CHUNK: usize = 16;

/// Copies `src[start..start + len]` to `dst`
///
/// Values of up to `4 * COPY_CHUNK` bytes are copied in whole `COPY_CHUNK`
/// byte chunks when both `src` past `start` and `dst` have room for them, as
/// this is faster than `memcpy` for short values. This may then write up to
/// `COPY_CHUNK` bytes past `dst + len`, but never past `dst + dst_len`.
///
/// # Safety
///
/// - `start + len <= src.len()`
/// - `len <= dst_len`, and `dst` must be valid for writes of `dst_len` bytes
/// - `dst..dst + dst_len` must not overlap `src`
#[inline(always)]
unsafe fn copy_value(src: &[u8], start: usize, len: usize, dst: *mut u8, dst_len: usize) {
    // SAFETY: `start <= src.len()`
    let src_ptr = unsafe { src.as_ptr().add(start) };
    // SAFETY: only called below when `i + COPY_CHUNK <= available`, so the
    // chunk is within both `src[start..]` and `dst..dst + dst_len`
    let copy_chunk =
        |i: usize| unsafe { std::ptr::copy_nonoverlapping(src_ptr.add(i), dst.add(i), COPY_CHUNK) };
    let available = (src.len() - start).min(dst_len);
    if len <= COPY_CHUNK && available >= COPY_CHUNK {
        copy_chunk(0);
    } else if len > COPY_CHUNK
        && len <= 4 * COPY_CHUNK
        && available >= len.next_multiple_of(COPY_CHUNK)
    {
        // Explicitly unrolled, as LLVM turns the equivalent loop into `memcpy`
        copy_chunk(0);
        copy_chunk(COPY_CHUNK);
        if len > 2 * COPY_CHUNK {
            copy_chunk(2 * COPY_CHUNK);
            if len > 3 * COPY_CHUNK {
                copy_chunk(3 * COPY_CHUNK);
            }
        }
    } else {
        // SAFETY: `src[start..start + len]` is valid, and `dst` is valid for
        // `len` bytes
        unsafe { std::ptr::copy_nonoverlapping(src_ptr, dst, len) };
    }
}

impl<I: OffsetSizeTrait> ValuesBuffer for OffsetBuffer<I> {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn reserve_exact(&mut self, additional: usize) {
        self.offsets.reserve_exact(additional);
    }

    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()> {
        if self.offsets.len() != read_offset + values_read + 1 {
            return Err(general_err!(
                "found inconsistent offsets while padding nulls: expected {} offsets, got {}",
                read_offset + values_read + 1,
                self.offsets.len()
            ));
        }
        self.offsets
            .resize(read_offset + levels_read + 1, I::default());

        let offsets = &mut self.offsets;

        let mut last_pos = read_offset + levels_read + 1;
        let mut last_start_offset = I::from_usize(self.values.len()).unwrap();

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range
            .clone()
            .rev()
            .zip(iter_set_bits_rev(valid_mask))
        {
            if level_pos < value_pos || level_pos >= last_pos {
                return Err(general_err!("found corrupt level data while padding nulls"));
            }

            let end_offset = offsets[value_pos + 1];
            let start_offset = offsets[value_pos];

            // Fill in any nulls
            for x in &mut offsets[level_pos + 1..last_pos] {
                *x = end_offset;
            }

            if level_pos == value_pos {
                return Ok(());
            }

            offsets[level_pos] = start_offset;
            last_pos = level_pos;
            last_start_offset = start_offset;
        }

        // Pad leading nulls up to `last_offset`
        for x in &mut offsets[values_range.start + 1..last_pos] {
            *x = last_start_offset
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, LargeStringArray, StringArray};

    #[test]
    fn test_offset_buffer_empty() {
        let buffer = OffsetBuffer::<i32>::with_capacity(0);
        let array = buffer.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 0);
    }

    #[test]
    fn test_offset_buffer_append() {
        let mut buffer = OffsetBuffer::<i64>::with_capacity(0);
        buffer.try_push(b"hello", true).unwrap();
        buffer.try_push(b"bar", true).unwrap();
        buffer
            .extend_from_dictionary(&[1, 3, 0, 2], &[0, 2, 4, 5, 6], b"abcdef")
            .unwrap();

        let array = buffer.into_array(None, ArrowType::LargeUtf8);
        let strings = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            vec!["hello", "bar", "cd", "f", "ab", "e"]
        )
    }

    #[test]
    fn test_offset_buffer_extend_from_dictionary_lengths() {
        // Values around each multiple of the chunk size of `copy_value`,
        // including ones ending near the end of the dictionary
        let lengths = [
            0, 1, 7, 15, 16, 17, 31, 32, 33, 47, 48, 49, 63, 64, 65, 100, 2, 0, 3, 17, 40, 0,
        ];
        let dict: Vec<String> = lengths
            .iter()
            .enumerate()
            .map(|(i, len)| {
                (0..*len)
                    .map(|j| (b'a' + ((i + j) % 26) as u8) as char)
                    .collect()
            })
            .collect();
        let mut dict_offsets = vec![0i32];
        let mut dict_values = Vec::new();
        for v in &dict {
            dict_values.extend_from_slice(v.as_bytes());
            dict_offsets.push(dict_values.len() as i32);
        }
        // Exactly sized, so reads past the end are caught by sanitizers
        let dict_values = dict_values.into_boxed_slice();
        let keys: Vec<i32> = (0..200).map(|i| (i * 7) % dict.len() as i32).collect();

        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        buffer.try_push(b"first", true).unwrap();
        // In several calls, so that some start with little spare capacity
        for chunk in keys.chunks(30) {
            buffer
                .extend_from_dictionary(chunk, &dict_offsets, &dict_values)
                .unwrap();
        }

        let array = buffer.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        let expected: Vec<&str> = std::iter::once("first")
            .chain(keys.iter().map(|k| dict[*k as usize].as_str()))
            .collect();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn test_offset_buffer_extend_from_dictionary_capacity() {
        let dict_offsets = [0i32, 0, 16, 32];
        let dict_values = b"aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb";

        // Empty values don't allocate
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        buffer
            .extend_from_dictionary(&[0; 10], &dict_offsets, dict_values)
            .unwrap();
        assert_eq!(buffer.offsets, [0; 11]);
        assert_eq!(buffer.values.capacity(), 0);

        // Values that exactly fill the capacity don't grow it
        let keys: Vec<i32> = (0..64).map(|i| 1 + i % 2).collect();
        buffer.values.reserve_exact(64 * 16);
        let capacity = buffer.values.capacity();
        buffer
            .extend_from_dictionary(&keys, &dict_offsets, dict_values)
            .unwrap();
        assert_eq!(buffer.values.len(), 64 * 16);
        assert_eq!(buffer.values.capacity(), capacity);

        // A short value at the end of the capacity is not copied in a whole
        // chunk, even if the source has room for one
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        buffer.values.reserve_exact(1);
        let capacity = buffer.values.capacity();
        buffer
            .extend_from_dictionary(&[0], &[0i32, 1], dict_values)
            .unwrap();
        assert_eq!(buffer.values, b"a");
        assert_eq!(buffer.values.capacity(), capacity);
    }

    #[test]
    fn test_offset_buffer_extend_from_dictionary_err() {
        let dict_offsets = [0i32, 2, 5, 9];
        let dict_values = b"abcdefgh";

        // Values before the invalid key are kept, and the buffer stays valid
        for (keys, err) in [
            (&[1, 0, 3, 1][..], "dictionary key beyond bounds"),
            (&[1, 0, -1, 1][..], "dictionary key beyond bounds"),
            (&[1, 0, 2, 1][..], "invalid dictionary offsets"),
        ] {
            let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
            buffer.try_push(b"first", true).unwrap();
            let e = buffer
                .extend_from_dictionary(keys, &dict_offsets, dict_values)
                .unwrap_err();
            assert!(e.to_string().contains(err), "{e}");
            assert_eq!(buffer.offsets, [0, 5, 8, 10]);
            assert_eq!(buffer.values, b"firstcdeab");
        }
    }

    #[test]
    fn test_offset_buffer() {
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        for v in ["hello", "world", "cupcakes", "a", "b", "c"] {
            buffer.try_push(v.as_bytes(), false).unwrap()
        }
        let split = std::mem::replace(&mut buffer, OffsetBuffer::with_capacity(0));

        let array = split.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            vec!["hello", "world", "cupcakes", "a", "b", "c"]
        );

        buffer.try_push(b"test", false).unwrap();
        let array = buffer.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            vec!["test"]
        );
    }

    #[test]
    fn test_offset_buffer_pad_nulls() {
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        let values = ["a", "b", "c", "def", "gh"];
        for v in &values {
            buffer.try_push(v.as_bytes(), false).unwrap()
        }

        let valid = [
            true, false, false, true, false, true, false, true, true, false, false,
        ];
        let valid_mask = Buffer::from_iter(valid.iter().copied());

        // Both trailing and leading nulls
        buffer
            .pad_nulls(1, values.len() - 1, valid.len() - 1, valid_mask.as_slice())
            .unwrap();

        let array = buffer.into_array(Some(valid_mask), ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
                Some("a"),
                None,
                None,
                Some("b"),
                None,
                Some("c"),
                None,
                Some("def"),
                Some("gh"),
                None,
                None
            ]
        );
    }

    #[test]
    fn test_utf8_validation() {
        let valid_2_byte_utf8 = &[0b11001000, 0b10001000];
        std::str::from_utf8(valid_2_byte_utf8).unwrap();
        let valid_3_byte_utf8 = &[0b11101000, 0b10001000, 0b10001000];
        std::str::from_utf8(valid_3_byte_utf8).unwrap();
        let valid_4_byte_utf8 = &[0b11110010, 0b10101000, 0b10101001, 0b10100101];
        std::str::from_utf8(valid_4_byte_utf8).unwrap();

        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        buffer.try_push(valid_2_byte_utf8, true).unwrap();
        buffer.try_push(valid_3_byte_utf8, true).unwrap();
        buffer.try_push(valid_4_byte_utf8, true).unwrap();

        // Cannot append string starting with incomplete codepoint
        buffer.try_push(&valid_2_byte_utf8[1..], true).unwrap_err();
        buffer.try_push(&valid_3_byte_utf8[1..], true).unwrap_err();
        buffer.try_push(&valid_3_byte_utf8[2..], true).unwrap_err();
        buffer.try_push(&valid_4_byte_utf8[1..], true).unwrap_err();
        buffer.try_push(&valid_4_byte_utf8[2..], true).unwrap_err();
        buffer.try_push(&valid_4_byte_utf8[3..], true).unwrap_err();

        // Can append data containing an incomplete codepoint
        buffer.try_push(&[0b01111111, 0b10111111], true).unwrap();

        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.values.len(), 11);

        buffer.try_push(valid_3_byte_utf8, true).unwrap();

        // Should fail due to incomplete codepoint
        buffer.check_valid_utf8(0).unwrap_err();

        // After broken codepoint -> success
        buffer.check_valid_utf8(11).unwrap();

        // Fails if run from middle of codepoint
        buffer.check_valid_utf8(12).unwrap_err();
    }

    #[test]
    fn test_pad_nulls_corrupt_input_returns_err() {
        // Corrupt input must produce a decode error rather than panicking.

        // Offsets inconsistent with `values_read`: only one value was pushed,
        // but three are claimed to have been read.
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        buffer.try_push(b"a", false).unwrap();
        let valid_mask = Buffer::from_iter([true, false, false]);
        let err = buffer
            .pad_nulls(0, 3, 3, valid_mask.as_slice())
            .unwrap_err();
        assert!(
            err.to_string().contains("inconsistent offsets"),
            "unexpected error: {err}"
        );

        // Valid mask has fewer set bits than `values_read`, which previously
        // tripped an assertion in the null-padding loop.
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        for v in ["a", "b", "c"] {
            buffer.try_push(v.as_bytes(), false).unwrap();
        }
        let valid_mask = Buffer::from_iter([true, false, false]);
        let err = buffer
            .pad_nulls(0, 3, 3, valid_mask.as_slice())
            .unwrap_err();
        assert!(
            err.to_string().contains("corrupt level data"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_pad_nulls_empty() {
        let mut buffer = OffsetBuffer::<i32>::with_capacity(0);
        let valid_mask = Buffer::from_iter(std::iter::repeat_n(false, 9));
        buffer.pad_nulls(0, 0, 9, valid_mask.as_slice()).unwrap();

        let array = buffer.into_array(Some(valid_mask), ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(strings.len(), 9);
        assert!(strings.iter().all(|x| x.is_none()))
    }
}
