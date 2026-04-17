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

use crate::basic::Type;
use crate::data_type::Int96;
use crate::data_type::private::ParquetValueType;
use crate::schema::types::ColumnDescriptor;

/// A helper to estimate the size of plain encoding of the values
/// that were written to the dictionary encoder.
///
/// This is used to enhance the dictionary fallback heuristic with the logic
/// that the writer should fall back to the plain encoding when at a certain point,
/// e.g. after encoding the first batch, the total size of unencoded data
/// is calculated as smaller than `(encodedSize + dictionarySize)`.
pub struct PlainDataSizeCounter {
    raw_data_byte_size: usize,
    // Cached type length to improve performance for fixed-length types.
    type_length: usize,
}

impl PlainDataSizeCounter {
    pub fn new(desc: &ColumnDescriptor) -> Self {
        Self {
            raw_data_byte_size: 0,
            type_length: desc.type_length() as usize,
        }
    }

    /// Updates the counter with the given slice.
    pub fn update<T: ParquetValueType>(&mut self, values: &[T]) {
        let raw_size = match T::PHYSICAL_TYPE {
            Type::BOOLEAN => values.len(),
            Type::INT32 | Type::FLOAT => 4 * values.len(),
            Type::INT64 | Type::DOUBLE => 8 * values.len(),
            Type::INT96 => Int96::SIZE_IN_BYTES * values.len(),
            Type::BYTE_ARRAY => {
                // For variable-length types, the length prefix and the actual data are are encoded.
                values.iter().map(|value| value.dict_encoding_size()).sum()
            }
            Type::FIXED_LEN_BYTE_ARRAY => self.type_length * values.len(),
        };
        self.raw_data_byte_size = self.raw_data_byte_size.saturating_add(raw_size);
    }

    /// Like `update`, but specialized for byte array data exposed by Arrow
    /// array accessors.
    #[cfg(feature = "arrow")]
    #[inline]
    pub fn update_byte_array(&mut self, value: &[u8]) {
        // For variable-length types, the length prefix and the actual data are are encoded.
        let raw_size = std::mem::size_of::<u32>() + value.len();
        self.raw_data_byte_size = self.raw_data_byte_size.saturating_add(raw_size);
    }

    /// Returns the total size in bytes of values passed to `update` as if they were written
    /// in plain encoding, without a dictionary.
    #[inline]
    pub fn plain_encoded_data_size(&self) -> usize {
        self.raw_data_byte_size
    }
}
