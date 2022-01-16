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

use crate::arrow::record_reader::buffer::{
    BufferQueue, ScalarBuffer, ScalarValue, ValuesBuffer,
};
use crate::column::reader::decoder::ValuesBufferSlice;
use crate::errors::{ParquetError, Result};
use arrow::array::{make_array, ArrayDataBuilder, ArrayRef, OffsetSizeTrait};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowNativeType, DataType as ArrowType};

/// A buffer of variable-sized byte arrays that can be converted into
/// a corresponding [`ArrayRef`]
pub struct OffsetBuffer<I: ScalarValue> {
    pub offsets: ScalarBuffer<I>,
    pub values: ScalarBuffer<u8>,
}

impl<I: ScalarValue> Default for OffsetBuffer<I> {
    fn default() -> Self {
        let mut offsets = ScalarBuffer::new();
        offsets.resize(1);
        Self {
            offsets,
            values: ScalarBuffer::new(),
        }
    }
}

impl<I: OffsetSizeTrait + ScalarValue> OffsetBuffer<I> {
    /// Returns the number of byte arrays in this buffer
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// If `validate_utf8` this verifies that the first character of `data` is
    /// the start of a UTF-8 codepoint
    ///
    /// Note: This does not verify that the entirety of `data` is valid
    /// UTF-8. This should be done by calling [`Self::values_as_str`] after
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
    pub fn extend_from_dictionary<K: ArrowNativeType, V: ArrowNativeType>(
        &mut self,
        keys: &[K],
        dict_offsets: &[V],
        dict_values: &[u8],
    ) -> Result<()> {
        for key in keys {
            let index = key.to_usize().unwrap();
            if index + 1 >= dict_offsets.len() {
                return Err(general_err!("invalid offset in byte array: {}", index));
            }
            let start_offset = dict_offsets[index].to_usize().unwrap();
            let end_offset = dict_offsets[index + 1].to_usize().unwrap();

            // Dictionary values are verified when decoding dictionary page
            self.try_push(&dict_values[start_offset..end_offset], false)?;
        }
        Ok(())
    }

    /// Returns the values buffer as a string slice, returning an error
    /// if it is invalid UTF-8
    ///
    /// `start_offset` is the offset in bytes from the start
    pub fn values_as_str(&self, start_offset: usize) -> Result<&str> {
        std::str::from_utf8(&self.values.as_slice()[start_offset..]).map_err(|e| {
            ParquetError::General(format!("encountered non UTF-8 data: {}", e))
        })
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(
        self,
        null_buffer: Option<Buffer>,
        data_type: ArrowType,
    ) -> ArrayRef {
        let mut array_data_builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .add_buffer(self.offsets.into())
            .add_buffer(self.values.into());

        if let Some(buffer) = null_buffer {
            array_data_builder = array_data_builder.null_bit_buffer(buffer);
        }

        let data = match cfg!(debug_assertions) {
            true => array_data_builder.build().unwrap(),
            false => unsafe { array_data_builder.build_unchecked() },
        };

        make_array(data)
    }
}

impl<I: OffsetSizeTrait + ScalarValue> BufferQueue for OffsetBuffer<I> {
    type Output = Self;
    type Slice = Self;

    fn split_off(&mut self, len: usize) -> Self::Output {
        let remaining_offsets = self.offsets.len() - len - 1;
        let offsets = self.offsets.as_slice();

        let end_offset = offsets[len];

        let mut new_offsets = ScalarBuffer::new();
        new_offsets.reserve(remaining_offsets + 1);
        for v in &offsets[len..] {
            new_offsets.push(*v - end_offset)
        }

        self.offsets.resize(len + 1);

        Self {
            offsets: std::mem::replace(&mut self.offsets, new_offsets),
            values: self.values.take(end_offset.to_usize().unwrap()),
        }
    }

    fn spare_capacity_mut(&mut self, _batch_size: usize) -> &mut Self::Slice {
        self
    }

    fn set_len(&mut self, len: usize) {
        assert_eq!(self.offsets.len(), len + 1);
    }
}

impl<I: OffsetSizeTrait + ScalarValue> ValuesBuffer for OffsetBuffer<I> {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        rev_position_iter: impl Iterator<Item = usize>,
    ) {
        assert_eq!(self.offsets.len(), read_offset + values_read + 1);
        self.offsets.resize(read_offset + levels_read + 1);

        let offsets = self.offsets.as_slice_mut();

        let mut last_pos = read_offset + levels_read + 1;
        let mut last_start_offset = I::from_usize(self.values.len()).unwrap();

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.clone().rev().zip(rev_position_iter) {
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

impl<I: ScalarValue> ValuesBufferSlice for OffsetBuffer<I> {
    fn capacity(&self) -> usize {
        usize::MAX
    }
}
