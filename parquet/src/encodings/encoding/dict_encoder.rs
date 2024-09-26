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

// ----------------------------------------------------------------------
// Dictionary encoding

use bytes::Bytes;

use crate::basic::{Encoding, Type};
use crate::data_type::private::ParquetValueType;
use crate::data_type::DataType;
use crate::encodings::encoding::{Encoder, PlainEncoder};
use crate::encodings::rle::RleEncoder;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::num_required_bits;
use crate::util::interner::{Interner, Storage};

#[derive(Debug)]
struct KeyStorage<T: DataType> {
    uniques: Vec<T::T>,

    /// size of unique values (keys) in the dictionary, in bytes.
    size_in_bytes: usize,

    type_length: usize,
}

impl<T: DataType> Storage for KeyStorage<T> {
    type Key = u64;
    type Value = T::T;

    fn get(&self, idx: Self::Key) -> &Self::Value {
        &self.uniques[idx as usize]
    }

    fn push(&mut self, value: &Self::Value) -> Self::Key {
        let (base_size, num_elements) = value.dict_encoding_size();

        let unique_size = match T::get_physical_type() {
            Type::BYTE_ARRAY => base_size + num_elements,
            Type::FIXED_LEN_BYTE_ARRAY => self.type_length,
            _ => base_size,
        };
        self.size_in_bytes += unique_size;

        let key = self.uniques.len() as u64;
        self.uniques.push(value.clone());
        key
    }

    fn estimated_memory_size(&self) -> usize {
        self.size_in_bytes + self.uniques.capacity() * std::mem::size_of::<T::T>()
    }
}

/// Dictionary encoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary page is written first, before the data pages of the column chunk.
///
/// Dictionary page format: the entries in the dictionary - in dictionary order -
/// using the plain encoding.
///
/// Data page format: the bit width used to encode the entry ids stored as 1 byte
/// (max bit width = 32), followed by the values encoded using RLE/Bit packed described
/// above (with the given bit width).
pub struct DictEncoder<T: DataType> {
    interner: Interner<KeyStorage<T>>,

    /// The buffered indices
    indices: Vec<u64>,
}

impl<T: DataType> DictEncoder<T> {
    /// Creates new dictionary encoder.
    pub fn new(desc: ColumnDescPtr) -> Self {
        let storage = KeyStorage {
            uniques: vec![],
            size_in_bytes: 0,
            type_length: desc.type_length() as usize,
        };

        Self {
            interner: Interner::new(storage),
            indices: vec![],
        }
    }

    /// Returns true if dictionary entries are sorted, false otherwise.
    pub fn is_sorted(&self) -> bool {
        // Sorting is not supported currently.
        false
    }

    /// Returns number of unique values (keys) in the dictionary.
    pub fn num_entries(&self) -> usize {
        self.interner.storage().uniques.len()
    }

    /// Returns size of unique values (keys) in the dictionary, in bytes.
    pub fn dict_encoded_size(&self) -> usize {
        self.interner.storage().size_in_bytes
    }

    /// Writes out the dictionary values with PLAIN encoding in a byte buffer, and return
    /// the result.
    pub fn write_dict(&self) -> Result<Bytes> {
        let mut plain_encoder = PlainEncoder::<T>::new();
        plain_encoder.put(&self.interner.storage().uniques)?;
        plain_encoder.flush_buffer()
    }

    /// Writes out the dictionary values with RLE encoding in a byte buffer, and return
    /// the result.
    pub fn write_indices(&mut self) -> Result<Bytes> {
        let buffer_len = self.estimated_data_encoded_size();
        let mut buffer = Vec::with_capacity(buffer_len);
        buffer.push(self.bit_width());

        // Write bit width in the first byte
        let mut encoder = RleEncoder::new_from_buf(self.bit_width(), buffer);
        for index in &self.indices {
            encoder.put(*index)
        }
        self.indices.clear();
        Ok(encoder.consume().into())
    }

    fn put_one(&mut self, value: &T::T) {
        self.indices.push(self.interner.intern(value));
    }

    #[inline]
    fn bit_width(&self) -> u8 {
        num_required_bits(self.num_entries().saturating_sub(1) as u64)
    }
}

impl<T: DataType> Encoder<T> for DictEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.indices.reserve(values.len());
        for i in values {
            self.put_one(i)
        }
        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN_DICTIONARY
    }

    /// Returns an estimate of the data page size in bytes
    ///
    /// This includes:
    /// <already_written_encoded_byte_size> + <estimated_encoded_size_of_unflushed_bytes>
    fn estimated_data_encoded_size(&self) -> usize {
        let bit_width = self.bit_width();
        RleEncoder::max_buffer_size(bit_width, self.indices.len())
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        self.write_indices()
    }

    /// Returns the estimated total memory usage
    ///
    /// For this encoder, the indices are unencoded bytes (refer to [`Self::write_indices`]).
    fn estimated_memory_size(&self) -> usize {
        self.interner.storage().size_in_bytes + self.indices.len() * std::mem::size_of::<usize>()
    }
}
