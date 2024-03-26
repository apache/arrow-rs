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
use arrow_array::builder::GenericByteViewBuilder;
use arrow_array::types::BinaryViewType;
use arrow_array::ArrayRef;
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType as ArrowType;
use std::sync::Arc;

/// A buffer of variable-sized byte arrays that can be converted into
/// a corresponding [`ArrayRef`]
#[derive(Debug, Default)]
pub struct ViewBuffer {
    pub values: Vec<Option<Vec<u8>>>,
}

impl ViewBuffer {
    /// Returns the number of byte arrays in this buffer
    pub fn len(&self) -> usize {
        self.values.len()
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
        self.values.push(Some(data.to_vec()));
        Ok(())
    }

    /// Extends this buffer with a list of keys
    pub fn extend_from_dictionary<K: ArrowNativeType>(
        &mut self,
        keys: &[K],
        dict: &[Option<Vec<u8>>],
    ) -> Result<()> {
        for key in keys {
            let index = key.as_usize();
            if index + 1 > dict.len() {
                return Err(general_err!(
                    "dictionary key beyond bounds of dictionary: 0..{}",
                    dict.len()
                ));
            }

            let value = dict.get(index).unwrap();

            // Dictionary values are verified when decoding dictionary page
            self.try_push(value.as_ref().unwrap(), false)?;
        }
        Ok(())
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(self, _null_buffer: Option<Buffer>, data_type: ArrowType) -> ArrayRef {
        let mut builder =
            GenericByteViewBuilder::<BinaryViewType>::with_capacity(self.values.len());
        self.values
            .into_iter()
            .for_each(|v| builder.append_option(v));

        match data_type {
            ArrowType::BinaryView => Arc::new(builder.finish()),
            ArrowType::Utf8View => Arc::new(builder.finish().to_stringview().unwrap()),
            _ => unreachable!(),
        }
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
        self.values.resize(read_offset + levels_read, None);

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            self.values[level_pos] = self.values[value_pos].take();
        }
    }
}
