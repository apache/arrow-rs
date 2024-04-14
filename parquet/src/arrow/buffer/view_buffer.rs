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
use arrow_array::{make_array, ArrayRef};
use arrow_buffer::{ArrowNativeType, Buffer, ToByteSlice};
use arrow_data::{ArrayDataBuilder, ByteView};
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;

/// A buffer of variable-sized byte arrays that can be converted into
/// a corresponding [`ArrayRef`]
#[derive(Debug, Default)]
pub struct ViewBuffer {
    pub views: Vec<u128>,
    /// If encoding in (`PLAIN`, `DELTA_LENGTH_BYTE_ARRAY`), we use `plain_buffer`
    /// to hold the page data without copy.
    pub plain_buffer: Option<Bytes>,
    /// If encoding is `DELTA_BYTE_ARRAY`, we use `delta_buffer` to build data buffer
    /// since this encoding's page data not hold full data.
    ///
    /// If encoding in (`PLAIN_DICTIONARY`, `RLE_DICTIONARY`), we need these two buffers
    /// cause these encoding first build dict then use dict to read data.
    pub delta_buffer: Vec<u8>,
}

impl ViewBuffer {
    /// Returns the number of byte arrays in this buffer
    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// add entire page buf to [`ViewBuffer`], avoid copy data.
    pub fn add_buffer(&mut self, buf: Bytes) {
        if self.plain_buffer.is_none() {
            self.plain_buffer = Some(buf);
        }
    }

    /// Push data to [`ViewBuffer`], since we already hold full data through [`Self::add_buffer`],
    /// we only need to slice the data to build the view.
    pub fn try_push_with_offset(&mut self, start_offset: usize, end_offset: usize) -> Result<()> {
        let data = &self.plain_buffer.as_ref().unwrap()[start_offset..end_offset];
        let length: u32 = (end_offset - start_offset) as u32;
        if length <= 12 {
            let mut view_buffer = [0; 16];
            view_buffer[0..4].copy_from_slice(&length.to_le_bytes());
            view_buffer[4..4 + length as usize].copy_from_slice(data);
            self.views.push(u128::from_le_bytes(view_buffer));
            return Ok(());
        }

        let view = ByteView {
            length,
            prefix: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            buffer_index: 0,
            offset: start_offset as u32,
        };
        self.views.push(view.into());
        Ok(())
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
        let length: u32 = data.len().try_into().unwrap();
        if length <= 12 {
            let mut view_buffer = [0; 16];
            view_buffer[0..4].copy_from_slice(&length.to_le_bytes());
            view_buffer[4..4 + length as usize].copy_from_slice(data);
            self.views.push(u128::from_le_bytes(view_buffer));
            return Ok(());
        }

        let offset = self.delta_buffer.len() as u32;
        self.delta_buffer.extend_from_slice(data);

        let view = ByteView {
            length,
            prefix: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            buffer_index: 0,
            offset,
        };
        self.views.push(view.into());
        Ok(())
    }

    /// Extends this buffer with a list of keys
    pub fn extend_from_dictionary<K: ArrowNativeType>(
        &mut self,
        keys: &[K],
        dict_views: &[u128],
        dict_buffers: &[u8],
    ) -> Result<()> {
        for key in keys {
            let index = key.as_usize();
            if index >= dict_views.len() {
                return Err(general_err!(
                    "dictionary key beyond bounds of dictionary: 0..{}",
                    dict_views.len()
                ));
            }

            let view = dict_views[index];
            let len = view as u32;

            // Dictionary values are verified when decoding dictionary page, so validate_utf8 is false here.
            if len <= 12 {
                self.views.push(view);
            } else {
                let offset = (view >> 96) as usize;
                self.try_push(&dict_buffers[offset..offset + len as usize], false)?
            };
        }
        Ok(())
    }

    /// Validates that `&self.views[start_offset..]`'s all data are valid UTF-8 sequence
    pub fn check_valid_utf8(&self, start_offset: usize) -> Result<()> {
        let views_slice = &self.views.as_slice()[start_offset..];
        // check inlined view first
        for view in views_slice {
            if *view as u32 > 12 {
                continue;
            }
            let len = *view as u32;
            if let Err(e) = std::str::from_utf8(&view.to_byte_slice()[4..4 + len as usize]) {
                return Err(general_err!("encountered non UTF-8 data: {}", e));
            }
        }
        let first_buffer = views_slice.iter().find(|view| (**view) as u32 > 12);
        if first_buffer.is_none() {
            return Ok(());
        }
        let first_buffer_offset = ((*first_buffer.unwrap()) >> 96) as u32 as usize;
        if self.plain_buffer.is_none() {
            match std::str::from_utf8(&self.delta_buffer[first_buffer_offset..]) {
                Ok(_) => Ok(()),
                Err(e) => Err(general_err!("encountered non UTF-8 data: {}", e)),
            }
        } else {
            match std::str::from_utf8(&self.plain_buffer.as_ref().unwrap()[first_buffer_offset..]) {
                Ok(_) => Ok(()),
                Err(e) => Err(general_err!("encountered non UTF-8 data: {}", e)),
            }
        }
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(self, null_buffer: Option<Buffer>, data_type: ArrowType) -> ArrayRef {
        let len = self.len();
        let array_data_builder = {
            let builder = ArrayDataBuilder::new(data_type)
                .len(len)
                .add_buffer(Buffer::from_vec(self.views))
                .null_bit_buffer(null_buffer);

            if self.plain_buffer.is_none() {
                builder.add_buffer(Buffer::from_vec(self.delta_buffer))
            } else {
                builder.add_buffer(self.plain_buffer.unwrap().into())
            }
        };

        let data = match cfg!(debug_assertions) {
            true => array_data_builder.build().unwrap(),
            false => unsafe { array_data_builder.build_unchecked() },
        };

        make_array(data)
    }
}

impl ValuesBuffer for ViewBuffer {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        self.views.resize(read_offset + levels_read, 0);

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            self.views[level_pos] = self.views[value_pos];
        }
    }
}
