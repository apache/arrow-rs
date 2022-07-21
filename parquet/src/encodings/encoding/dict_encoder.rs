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

use crate::basic::{Encoding, Type};
use crate::data_type::{AsBytes, DataType};
use crate::encodings::encoding::{Encoder, PlainEncoder};
use crate::encodings::rle::RleEncoder;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::num_required_bits;
use crate::util::memory::ByteBufferPtr;
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::Write;
use crate::data_type::private::ParquetValueType;

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
    /// Descriptor for the column to be encoded.
    desc: ColumnDescPtr,

    state: ahash::RandomState,

    /// Used to provide a lookup from value to unique value
    ///
    /// Note: `u64`'s hash implementation is not used, instead the raw entry
    /// API is used to store keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<u64, (), ()>,

    /// The unique observed values.
    uniques: Vec<T::T>,

    /// The buffered indices
    indices: Vec<u64>,

    /// Size in bytes needed to encode this dictionary.
    uniques_size_in_bytes: usize,
}

impl<T: DataType> DictEncoder<T> {
    /// Creates new dictionary encoder.
    pub fn new(desc: ColumnDescPtr) -> Self {
        Self {
            desc,
            state: Default::default(),
            dedup: HashMap::with_hasher(()),
            uniques: vec![],
            indices: vec![],
            uniques_size_in_bytes: 0,
        }
    }

    /// Returns true if dictionary entries are sorted, false otherwise.
    pub fn is_sorted(&self) -> bool {
        // Sorting is not supported currently.
        false
    }

    /// Returns number of unique values (keys) in the dictionary.
    pub fn num_entries(&self) -> usize {
        self.uniques.len()
    }

    /// Returns size of unique values (keys) in the dictionary, in bytes.
    pub fn dict_encoded_size(&self) -> usize {
        self.uniques_size_in_bytes
    }

    /// Writes out the dictionary values with PLAIN encoding in a byte buffer, and return
    /// the result.
    pub fn write_dict(&self) -> Result<ByteBufferPtr> {
        let mut plain_encoder = PlainEncoder::<T>::new(self.desc.clone(), vec![]);
        plain_encoder.put(&self.uniques)?;
        plain_encoder.flush_buffer()
    }

    /// Writes out the dictionary values with RLE encoding in a byte buffer, and return
    /// the result.
    pub fn write_indices(&mut self) -> Result<ByteBufferPtr> {
        let buffer_len = self.estimated_data_encoded_size();
        let mut buffer = vec![0; buffer_len];
        buffer[0] = self.bit_width() as u8;

        // Write bit width in the first byte
        buffer.write_all((self.bit_width() as u8).as_bytes())?;
        let mut encoder = RleEncoder::new_from_buf(self.bit_width(), buffer, 1);
        for index in &self.indices {
            if !encoder.put(*index as u64)? {
                return Err(general_err!("Encoder doesn't have enough space"));
            }
        }
        self.indices.clear();
        Ok(ByteBufferPtr::new(encoder.consume()?))
    }

    fn compute_hash(state: &ahash::RandomState, value: &T::T) -> u64 {
        let mut hasher = state.build_hasher();
        value.as_bytes().hash(&mut hasher);
        hasher.finish()
    }

    fn put_one(&mut self, value: &T::T) {
        let hash = Self::compute_hash(&self.state, value);

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |index| value == &self.uniques[*index as usize]);

        let index = match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let index = self.uniques.len() as u64;
                self.uniques.push(value.clone());

                let (base_size, num_elements) = value.dict_encoding_size();

                let unique_size = match T::get_physical_type() {
                    Type::BYTE_ARRAY => base_size + num_elements,
                    Type::FIXED_LEN_BYTE_ARRAY => self.desc.type_length() as usize,
                    _ => base_size,
                };
                self.uniques_size_in_bytes += unique_size;

                *entry
                    .insert_with_hasher(hash, index, (), |index| {
                        Self::compute_hash(&self.state, &self.uniques[*index as usize])
                    })
                    .0
            }
        };

        self.indices.push(index);
    }

    #[inline]
    fn bit_width(&self) -> u8 {
        let num_entries = self.uniques.len();
        if num_entries <= 1 {
            num_entries as u8
        } else {
            num_required_bits(num_entries as u64 - 1)
        }
    }
}

impl<T: DataType> Encoder<T> for DictEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
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

    fn estimated_data_encoded_size(&self) -> usize {
        let bit_width = self.bit_width();
        1 + RleEncoder::min_buffer_size(bit_width)
            + RleEncoder::max_buffer_size(bit_width, self.indices.len())
    }

    fn flush_buffer(&mut self) -> Result<ByteBufferPtr> {
        self.write_indices()
    }
}
