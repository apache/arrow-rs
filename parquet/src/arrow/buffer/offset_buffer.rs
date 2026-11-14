// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::arrow::record_reader::buffer::{ValuesBuffer, count_set_bits, mask_bits};
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
    // Always inlined, as it is called for every value by the PLAIN and
    // DELTA_BYTE_ARRAY decoders, and is otherwise not inlined into their loops
    #[inline(always)]
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

        for key in keys {
            let index = key.as_usize();
            if index + 1 >= dict_offsets.len() {
                return Err(general_err!(
                    "dictionary key beyond bounds of dictionary: 0..{}",
                    dict_offsets.len().saturating_sub(1)
                ));
            }
            let start_offset = dict_offsets[index].as_usize();
            let end_offset = dict_offsets[index + 1].as_usize();

            // Dictionary values are verified when decoding dictionary page
            self.values
                .extend_from_slice(&dict_values[start_offset..end_offset]);
            let index_offset = I::from_usize(self.values.len())
                .ok_or_else(|| general_err!("index overflow decoding byte array"))?;
            self.offsets.push(index_offset);
        }
        Ok(())
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
        let levels_end = read_offset + levels_read;
        if count_set_bits(valid_mask, read_offset, levels_end) != values_read {
            return Err(general_err!("found corrupt level data while padding nulls"));
        }
        self.offsets.resize(levels_end + 1, I::default());
        spread_offsets(
            &mut self.offsets,
            read_offset,
            read_offset + values_read,
            valid_mask,
        );
        Ok(())
    }
}

/// Spreads the offsets of the values `start..value_end` over the levels
/// `start..offsets.len() - 1`, giving each null level an empty value, where
/// `valid_mask` has exactly one set bit per value in that range of levels.
///
/// Level `l` ends where the last value at or before it ends, so working down from
/// the last level, each level's end offset is that of the last value not yet
/// placed, which valid levels then consume. Like `spread_values`, this goes a
/// 64-bit mask word at a time: a word of valid levels is one block move, a word of
/// nulls one fill, a word with few valid levels one fill per run of nulls, and
/// otherwise a branchless write of every level.
fn spread_offsets<I: Copy>(
    offsets: &mut [I],
    start: usize,
    mut value_end: usize,
    valid_mask: &[u8],
) {
    let mut level_end = offsets.len() - 1;
    // Once as many values as levels remain, they are all in place.
    while level_end > value_end {
        let word_start = ((level_end - 1) & !63).max(start);
        let len = level_end - word_start;
        let word = mask_bits(valid_mask, word_start, len);
        let valid = word.count_ones() as usize;
        if valid == len {
            offsets.copy_within(value_end + 1 - len..value_end + 1, word_start + 1);
            value_end -= len;
        } else if valid * 2 <= len {
            let mut reversed = word.reverse_bits();
            let mut fill_end = level_end;
            while reversed != 0 {
                let level = word_start + 63 - reversed.trailing_zeros() as usize;
                let end = offsets[value_end];
                offsets[level + 1..=fill_end].fill(end);
                value_end -= 1;
                fill_end = level;
                reversed &= reversed - 1;
            }
            let end = offsets[value_end];
            offsets[word_start + 1..=fill_end].fill(end);
        } else {
            for level in (word_start..level_end).rev() {
                offsets[level + 1] = offsets[value_end];
                value_end -= ((word >> (level - word_start)) & 1) as usize;
            }
        }
        level_end = word_start;
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
    fn test_pad_nulls_random_masks() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..1000 {
            let read_offset = rng.random_range(0..100);
            let levels_read = rng.random_range(0..300);
            let density = [0.0, 0.02, 0.5, 0.98, 1.0][rng.random_range(0..5)];
            let valid: Vec<bool> = (0..levels_read).map(|_| rng.random_bool(density)).collect();
            let values_read = valid.iter().filter(|v| **v).count();

            let mut buffer = OffsetBuffer::<i64>::with_capacity(0);
            let mut expected = vec![];
            for i in 0..read_offset {
                let v = format!("p{i}");
                buffer.try_push(v.as_bytes(), false).unwrap();
                expected.push(Some(v));
            }
            let mut next = 0;
            for &is_valid in &valid {
                if is_valid {
                    let v = "x".repeat(next % 5) + &next.to_string();
                    buffer.try_push(v.as_bytes(), false).unwrap();
                    expected.push(Some(v));
                    next += 1;
                } else {
                    expected.push(None);
                }
            }
            let mask = Buffer::from_iter(
                std::iter::repeat_n(true, read_offset).chain(valid.iter().copied()),
            );
            buffer
                .pad_nulls(read_offset, values_read, levels_read, mask.as_slice())
                .unwrap();

            let array = buffer.into_array(Some(mask), ArrowType::LargeUtf8);
            let strings = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let actual: Vec<Option<String>> =
                strings.iter().map(|s| s.map(str::to_string)).collect();
            assert_eq!(actual, expected);
            // Nulls must be empty, not just masked
            let offsets = strings.value_offsets();
            for (i, is_valid) in valid.iter().enumerate() {
                if !is_valid {
                    let level = read_offset + i;
                    assert_eq!(offsets[level], offsets[level + 1]);
                }
            }
        }
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
