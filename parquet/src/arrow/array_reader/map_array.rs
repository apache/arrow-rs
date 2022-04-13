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

use crate::arrow::array_reader::ArrayReader;
use crate::errors::ParquetError::ArrowError;
use crate::errors::{Result, ParquetError};
use arrow::array::{ArrayDataBuilder, ArrayRef, MapArray};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::DataType as ArrowType;
use arrow::datatypes::ToByteSlice;
use arrow::util::bit_util;
use std::any::Any;
use std::sync::Arc;

/// Implementation of a map array reader.
pub struct MapArrayReader {
    key_reader: Box<dyn ArrayReader>,
    value_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    map_def_level: i16,
    map_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl MapArrayReader {
    pub fn new(
        key_reader: Box<dyn ArrayReader>,
        value_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            key_reader,
            value_reader,
            data_type,
            map_def_level: rep_level,
            map_rep_level: def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for MapArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let key_array = self.key_reader.next_batch(batch_size)?;
        let value_array = self.value_reader.next_batch(batch_size)?;

        // Check that key and value have the same lengths
        let key_length = key_array.len();
        if key_length != value_array.len() {
            return Err(general_err!(
                "Map key and value should have the same lengths."
            ));
        }

        let def_levels = self
            .key_reader
            .get_def_levels()
            .ok_or_else(|| ArrowError("item_reader def levels are None.".to_string()))?;
        let rep_levels = self
            .key_reader
            .get_rep_levels()
            .ok_or_else(|| ArrowError("item_reader rep levels are None.".to_string()))?;

        if !((def_levels.len() == rep_levels.len()) && (rep_levels.len() == key_length)) {
            return Err(ArrowError(
                "Expected item_reader def_levels and rep_levels to be same length as batch".to_string(),
            ));
        }

        let entry_data_type = if let ArrowType::Map(field, _) = &self.data_type {
            field.data_type().clone()
        } else {
            return Err(ArrowError("Expected a map arrow type".to_string()));
        };

        let entry_data = ArrayDataBuilder::new(entry_data_type)
            .len(key_length)
            .add_child_data(key_array.data().clone())
            .add_child_data(value_array.data().clone());
        let entry_data = unsafe { entry_data.build_unchecked() };

        let entry_len = rep_levels.iter().filter(|level| **level == 0).count();

        // first item in each list has rep_level = 0, subsequent items have rep_level = 1
        let mut offsets: Vec<i32> = Vec::new();
        let mut cur_offset = 0;
        def_levels.iter().zip(rep_levels).for_each(|(d, r)| {
            if *r == 0 || d == &self.map_def_level {
                offsets.push(cur_offset);
            }
            if d > &self.map_def_level {
                cur_offset += 1;
            }
        });
        offsets.push(cur_offset);

        let num_bytes = bit_util::ceil(offsets.len(), 8);
        // TODO: A useful optimization is to use the null count to fill with
        // 0 or null, to reduce individual bits set in a loop.
        // To favour dense data, set every slot to true, then unset
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        let mut list_index = 0;
        for i in 0..rep_levels.len() {
            // If the level is lower than empty, then the slot is null.
            // When a list is non-nullable, its empty level = null level,
            // so this automatically factors that in.
            if rep_levels[i] == 0 && def_levels[i] < self.map_def_level {
                // should be empty list
                bit_util::unset_bit(null_slice, list_index);
            }
            if rep_levels[i] == 0 {
                list_index += 1;
            }
        }
        let value_offsets = Buffer::from(&offsets.to_byte_slice());

        // Now we can build array data
        let array_data = ArrayDataBuilder::new(self.data_type.clone())
            .len(entry_len)
            .add_buffer(value_offsets)
            .null_bit_buffer(null_buf.into())
            .add_child_data(entry_data);

        let array_data = unsafe { array_data.build_unchecked() };

        Ok(Arc::new(MapArray::from(array_data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

#[cfg(test)]
mod tests {
    //TODO: Add unit tests (#1561)
}
