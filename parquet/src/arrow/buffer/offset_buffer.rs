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
use arrow_array::{make_array, ArrayRef, OffsetSizeTrait};
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

impl<I: OffsetSizeTrait> Default for OffsetBuffer<I> {
    fn default() -> Self {
        let mut offsets = Vec::new();
        offsets.resize(1, I::default());
        Self {
            offsets,
            values: Vec::new(),
        }
    }
}

impl<I: OffsetSizeTrait> OffsetBuffer<I> {
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
        if validate_utf8 {
            if let Some(&b) = data.first() {
                // A valid code-point iff it does not start with 0b10xxxxxx
                // Bit-magic taken from `std::str::is_char_boundary`
                if (b as i8) < -0x40 {
                    return Err(ParquetError::General(
                        "encountered non UTF-8 data".to_string(),
                    ));
                }
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
            self.try_push(&dict_values[start_offset..end_offset], false)?;
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
        match std::str::from_utf8(&self.values.as_slice()[start_offset..]) {
            Ok(_) => Ok(()),
            Err(e) => Err(general_err!("encountered non UTF-8 data: {}", e)),
        }
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
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        assert_eq!(self.offsets.len(), read_offset + values_read + 1);
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
            assert!(level_pos >= value_pos);
            assert!(level_pos < last_pos);

            let end_offset = offsets[value_pos + 1];
            let start_offset = offsets[value_pos];

            // Fill in any nulls
            for x in &mut offsets[level_pos + 1..last_pos] {
                *x = end_offset;
            }

            if level_pos == value_pos {
                return;
            }

            offsets[level_pos] = start_offset;
            last_pos = level_pos;
            last_start_offset = start_offset;
        }

        // Pad leading nulls up to `last_offset`
        for x in &mut offsets[values_range.start + 1..last_pos] {
            *x = last_start_offset
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, LargeStringArray, StringArray};

    #[test]
    fn test_offset_buffer_empty() {
        let buffer = OffsetBuffer::<i32>::default();
        let array = buffer.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 0);
    }

    #[test]
    fn test_offset_buffer_append() {
        let mut buffer = OffsetBuffer::<i64>::default();
        buffer.try_push("hello".as_bytes(), true).unwrap();
        buffer.try_push("bar".as_bytes(), true).unwrap();
        buffer
            .extend_from_dictionary(&[1, 3, 0, 2], &[0, 2, 4, 5, 6], "abcdef".as_bytes())
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
        let mut buffer = OffsetBuffer::<i32>::default();
        for v in ["hello", "world", "cupcakes", "a", "b", "c"] {
            buffer.try_push(v.as_bytes(), false).unwrap()
        }
        let split = std::mem::take(&mut buffer);

        let array = split.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            vec!["hello", "world", "cupcakes", "a", "b", "c"]
        );

        buffer.try_push("test".as_bytes(), false).unwrap();
        let array = buffer.into_array(None, ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
            vec!["test"]
        );
    }

    #[test]
    fn test_offset_buffer_pad_nulls() {
        let mut buffer = OffsetBuffer::<i32>::default();
        let values = ["a", "b", "c", "def", "gh"];
        for v in &values {
            buffer.try_push(v.as_bytes(), false).unwrap()
        }

        let valid = [
            true, false, false, true, false, true, false, true, true, false, false,
        ];
        let valid_mask = Buffer::from_iter(valid.iter().copied());

        // Both trailing and leading nulls
        buffer.pad_nulls(1, values.len() - 1, valid.len() - 1, valid_mask.as_slice());

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

        let mut buffer = OffsetBuffer::<i32>::default();
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
    fn test_pad_nulls_empty() {
        let mut buffer = OffsetBuffer::<i32>::default();
        let valid_mask = Buffer::from_iter(std::iter::repeat(false).take(9));
        buffer.pad_nulls(0, 0, 9, valid_mask.as_slice());

        let array = buffer.into_array(Some(valid_mask), ArrowType::Utf8);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(strings.len(), 9);
        assert!(strings.iter().all(|x| x.is_none()))
    }
}
