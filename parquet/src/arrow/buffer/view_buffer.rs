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
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::{ArrayDataBuilder, ByteView};
use arrow_schema::DataType as ArrowType;

/// A buffer of variable-sized byte arrays that can be converted into
/// a corresponding [`ArrayRef`]
#[derive(Debug, Default)]
pub struct ViewBuffer {
    pub views: Vec<u128>,
    pub buffers: Vec<u8>,
}

impl ViewBuffer {
    /// Returns the number of byte arrays in this buffer
    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

        let offset = self.buffers.len() as u32;
        self.buffers.extend_from_slice(data);

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

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(self, null_buffer: Option<Buffer>, data_type: ArrowType) -> ArrayRef {
        let len = self.len();
        let array_data_builder = ArrayDataBuilder::new(data_type)
            .len(len)
            .add_buffer(Buffer::from_vec(self.views))
            .add_buffer(Buffer::from_vec(self.buffers))
            .null_bit_buffer(null_buffer);

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
