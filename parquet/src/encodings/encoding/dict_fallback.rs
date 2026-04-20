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

use crate::basic::{Encoding, Type};
use crate::column::writer::encoder::DataPageValues;
use crate::data_type::Int96;
use crate::data_type::private::ParquetValueType;
use crate::schema::types::ColumnDescriptor;

// TODO: it's possible to tighten the worst-case estimate for fallback encodings
// other than PLAIN, e.g. by estimating the bit widths of delta encoded values,
// possibly making use of the min/max statistics if they are available.
// This would require more complex logic in the counter, and it's not clear if
// the improvement in the heuristic would be worth it.
// The size of the plain encoding should be a reasonable bottom estimate
// for any fallback encoding.

/// A helper to estimate the favorability of the dictionary encoding
/// compared to a pessimistic estimate of the size of data encoded without
/// the dictionary.
///
/// This is used to enhance the dictionary fallback heuristic with the logic
/// that the writer should fall back to a non-dictionary encoding when,
/// after encoding a prescribed minimum number of values, the worst case on
/// the size of data encoded without the dictionary is calculated as smaller
/// than `(encodedSize + dictionarySize)`.
pub struct DictFallbackCounter {
    // Estimated size of the data encoded without the dictionary, in bytes.
    raw_data_size: usize,
    // Size of the data encoded with the dictionary, in bytes.
    encoded_data_size: usize,
    // Number of values passed to the counter.
    num_values: usize,
    // Minimum number of values to sample before
    // the counter can return a favorable estimate for fallback.
    min_sample_len: usize,
    // Cached type length to improve performance for fixed-length types.
    type_length: usize,
}

impl DictFallbackCounter {
    pub fn new(desc: &ColumnDescriptor, min_sample_len: usize) -> Self {
        Self {
            raw_data_size: 0,
            encoded_data_size: 0,
            num_values: 0,
            min_sample_len,
            type_length: desc.type_length() as usize,
        }
    }

    /// Updates the counter with the given slice of values.
    pub fn update_values<T: ParquetValueType>(&mut self, values: &[T]) {
        let raw_size = match T::PHYSICAL_TYPE {
            Type::BOOLEAN => values.len(),
            Type::INT32 | Type::FLOAT => 4 * values.len(),
            Type::INT64 | Type::DOUBLE => 8 * values.len(),
            Type::INT96 => Int96::SIZE_IN_BYTES * values.len(),
            Type::BYTE_ARRAY => {
                // For variable-length types, the length prefix and the actual data are are encoded.
                values.iter().map(|value| value.plain_encoded_size()).sum()
            }
            Type::FIXED_LEN_BYTE_ARRAY => self.type_length * values.len(),
        };
        self.raw_data_size = self.raw_data_size.saturating_add(raw_size);
        self.num_values += values.len();
    }

    /// Like `update_values`, but specialized for byte array data exposed by Arrow
    /// array accessors. Updates the counter with the single given byte array value.
    #[cfg(feature = "arrow")]
    #[inline]
    pub fn update_byte_array(&mut self, value: &[u8]) {
        let raw_size = std::mem::size_of::<u32>() + value.len();
        self.raw_data_size = self.raw_data_size.saturating_add(raw_size);
        self.num_values += 1;
    }

    /// Increments the total counted size of dictionary encoded data
    /// for a page encoded with the dictionary encoding.
    pub fn commit_page<T>(&mut self, page: &DataPageValues<T>)
    where
        T: ParquetValueType,
    {
        assert_eq!(
            page.encoding,
            Encoding::RLE_DICTIONARY,
            "should only be used with the dictionary encoder"
        );
        self.encoded_data_size = self.encoded_data_size.saturating_add(page.buf.len());
    }

    /// If the number of dictionary encoded values accounted so far
    /// reaches or exceeds the configured minimum, returns true to indicate
    /// that the counting should be stopped, otherwise returns false.
    fn min_sample_len_reached(&self) -> bool {
        self.num_values >= self.min_sample_len
    }

    /// Returns `Some(true)` if the estimated size of plainly encoded data, in bytes,
    /// would not exceed the size of data encoded with a dictionary,
    /// as counted by the `commit_page` calls made on this counter and the provided size of
    /// the encoded dictionary page.
    /// This method returns `None` until the minimum number of values given in
    /// `DictFallbackCounter::new` has been processed. The third alternative,
    /// `Some(false)`, indicates that the sample size is sufficient, but the dictionary encoding
    /// is not-unfavorable, that is, the collected metrics show no clear advantage in falling
    /// back to plain (or other, presumably more efficient) encoding.
    #[inline]
    pub fn is_dict_encoding_unfavorable(&self, dict_encoded_size: usize) -> Option<bool> {
        self.min_sample_len_reached().then_some(
            self.raw_data_size <= dict_encoded_size.saturating_add(self.encoded_data_size),
        )
    }
}
