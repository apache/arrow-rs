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
use crate::errors::{ParquetError, Result};
use arrow::array::{Array, ArrayDataBuilder, ArrayRef, MapArray};
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
    #[allow(unused)]
    map_rep_level: i16,
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
            // These are the wrong way round https://github.com/apache/arrow-rs/issues/1699
            map_def_level: rep_level,
            map_rep_level: def_level,
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

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let key_len = self.key_reader.read_records(batch_size)?;
        let value_len = self.value_reader.read_records(batch_size)?;
        // Check that key and value have the same lengths
        if key_len != value_len {
            return Err(general_err!(
                "Map key and value should have the same lengths."
            ));
        }
        Ok(key_len)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let key_array = self.key_reader.consume_batch()?;
        let value_array = self.value_reader.consume_batch()?;

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
            .add_child_data(key_array.into_data())
            .add_child_data(value_array.into_data());
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
            .null_bit_buffer(Some(null_buf.into()))
            .add_child_data(entry_data);

        let array_data = unsafe { array_data.build_unchecked() };

        Ok(Arc::new(MapArray::from(array_data)))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let key_skipped = self.key_reader.skip_records(num_records)?;
        let value_skipped = self.value_reader.skip_records(num_records)?;
        if key_skipped != value_skipped {
            return Err(general_err!(
                "MapArrayReader out of sync, skipped {} keys and {} values",
                key_skipped,
                value_skipped
            ));
        }
        Ok(key_skipped)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        // Children definition levels should describe the same parent structure,
        // so return key_reader only
        self.key_reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        // Children repetition levels should describe the same parent structure,
        // so return key_reader only
        self.key_reader.get_rep_levels()
    }
}

#[cfg(test)]
mod tests {
    //TODO: Add unit tests (#1561)
}
