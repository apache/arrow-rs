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

//! Packed byte dictionary interning shared by Arrow and physical encoders.

use crate::data_type::private::byte_array_length;
use crate::errors::{ParquetError, Result};

/// Maximum size of a value that can be inlined in the hash table
const SHORT_VALUE_LEN: usize = size_of::<u64>() - 1;

/// Entry in the hash table
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry {
    /// hash of the value (stored to avoid recomputing it in hash table check)
    hash: u64,
    /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
    /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
    offset_or_inline: OffsetOrInlineView,
    index_in_dict: u64,
}

/// Either offset to the dictionary page bytes or inline bytes when the bytes are short enough
/// to avoid doing the indirection
///
/// For inlined view:
///   +------------+---------------+------------------------------+
///   | 1 bit set  | len {7 bits}  | value {56 bits}              |
///   +------------+---------------+------------------------------+
///
/// For non-inlined view:
///   +--------------+--------------------------------------------+
///   | 1 bit unset  | offset in dict {63 bits}                   |
///   +--------------+--------------------------------------------+
///
/// the inline view contain it's length to avoid having another field for length in the entry
/// since if the hashes are the same, most probably the length will be the same, so we can do the indirection check
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OffsetOrInlineView(u64);

impl OffsetOrInlineView {
    const INLINE_FLAG: u64 = 1 << 63;
    const LEN_SHIFT: u32 = 56;

    #[inline]
    fn is_non_inlined(&self) -> bool {
        (self.0 & Self::INLINE_FLAG) == 0
    }

    #[inline]
    fn is_inlined(&self) -> bool {
        !self.is_non_inlined()
    }

    #[inline]
    fn new_inlined(value: &[u8]) -> Self {
        debug_assert!(value.len() <= 7);

        // Value bytes occupy the low 56 bits, byte 0 in the least significant position
        let mut buf = [0u8; 8];
        buf[..value.len()].copy_from_slice(value);
        let packed = u64::from_le_bytes(buf);

        Self(Self::INLINE_FLAG | ((value.len() as u64) << Self::LEN_SHIFT) | packed)
    }

    #[inline]
    fn new_non_inlined(offset: u64) -> Self {
        // `offset` is a position in the dictionary page `Vec`, whose length is at
        // most `isize::MAX`, so it always fits in 63 bits and can't collide with
        // `INLINE_FLAG`
        // also, having the inline flag set for non-inlined values would mean the dictionary page is at least 9 exabytes.
        debug_assert_eq!(
            offset & Self::INLINE_FLAG,
            0,
            "dictionary page offset must fit in 63 bits"
        );
        let non_inlined_value = offset;
        Self(non_inlined_value)
    }

    fn maybe_non_inlined_value(&self) -> Option<u64> {
        if self.is_non_inlined() {
            // Not inlined is when the first bit is unset, so the value
            Some(self.0)
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ByteArrayInterner {
    state: ahash::RandomState,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry>,
    /// Encoded dictionary page: each value prefixed by its length as a `u32`
    pub(crate) page: Vec<u8>,
}

impl ByteArrayInterner {
    #[inline]
    pub(crate) fn intern(&mut self, value: &[u8]) -> Result<u64> {
        let hash = self.state.hash_one(value);

        // value is "small" - can be inlined
        let (found_entry, inline) = if let Some(inline) = maybe_inline_value(value) {
            // is value is already present in the set?
            let entry = self.map.find(hash, |header| {
                // value is stored inline so no need to consult buffer
                // (this is the "small string optimization")
                //
                // no need to compare the hashes since the hash is a function of the value which is compared here
                let is_same = inline == header.offset_or_inline;

                if is_same {
                    debug_assert_eq!(
                        header.hash, hash,
                        "hashes must match if the values are the same"
                    );
                }

                is_same
            });

            (entry, Some(inline))
        } else {
            // value is not "small"
            // Check if the value is already present in the set
            let entry = self.map.find(hash, |header| {
                // compare hash
                if header.hash != hash {
                    return false;
                }

                // If the value is not inlined compare it
                let Some(offset) = header.offset_or_inline.maybe_non_inlined_value() else {
                    // that value is inline, so it must be different
                    return false;
                };

                self.is_at(offset, value)
            });

            (entry, None)
        };

        if let Some(entry) = found_entry {
            return Ok(entry.index_in_dict);
        }

        // if no existing entry, make a new one
        self.insert(value, hash, inline)
    }

    /// Returns whether `value` is in the dictionary page at `offset`
    #[inline(always)]
    fn is_at(&self, offset: u64, value: &[u8]) -> bool {
        let offset = offset as usize;
        // SAFETY: every value in the page is preceded by its length
        let stored_len = unsafe { self.page.get_unchecked(offset - 4..offset) };
        // Rarely false, as the hashes matched, so the branch predicts well and
        // the comparison's reads issue before the length is known
        if u32::from_le_bytes(stored_len.try_into().unwrap()) as usize != value.len() {
            return false;
        }
        // SAFETY: the value at `offset` has the length just checked
        let existing = unsafe { self.page.get_unchecked(offset..offset + value.len()) };
        existing == value
    }

    pub(crate) fn num_unique_values(&self) -> usize {
        self.map.len()
    }

    /// Appends `value` to the dictionary, in the empty slot at `pos`
    fn insert(
        &mut self,
        value: &[u8],
        hash: u64,
        inline: Option<OffsetOrInlineView>,
    ) -> Result<u64> {
        // If inline provided it should inlined
        debug_assert!(inline.is_none_or(|i| i.is_inlined()));

        // The dictionary page is PLAIN encoded, where each value is prefixed by
        // its length as a 4-byte little endian integer so we need to reject that since we use unsafe access later
        //
        // From the parquet-format spec:
        // > Encoding PLAIN:
        // > [...] For the byte array type, it encodes the length as a 4-byte little
        // > endian integer, followed by the bytes.
        // >
        // > Dictionary Encoding:
        // > [...] Dictionary page format: the entries in the dictionary using the plain encoding.
        // >
        //
        // Reference:
        // - Plain encoding: https://github.com/apache/parquet-format/blob/53b8bd8576827080d4382fa457003f45b41cda6e/Encodings.md?plain=1#L72-L73
        // - Plain dictionary encoding: https://github.com/apache/parquet-format/blob/53b8bd8576827080d4382fa457003f45b41cda6e/Encodings.md?plain=1#L83
        let len = byte_array_length(value.len()).map_err(|_| {
            ParquetError::General(format!(
                "Byte array value is too large to be written in dictionary: {} bytes",
                value.len()
            ))
        })?;

        let key = self.num_unique_values() as u64;

        self.page.reserve(4 + value.len());
        self.page.extend_from_slice(&len.to_le_bytes());
        let offset = self.page.len() as u64;
        self.page.extend_from_slice(value);

        // Make the new entry
        let new_header = Entry {
            hash,
            offset_or_inline: inline.unwrap_or_else(|| OffsetOrInlineView::new_non_inlined(offset)),
            index_in_dict: key,
        };
        self.map
            .insert_unique(hash, new_header, |header| header.hash);

        Ok(key)
    }

    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.page.capacity() * std::mem::size_of::<u8>() + self.map.allocation_size()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        ByteArrayInternerIterator {
            values: &self.page,
            current_index: 0,
        }
    }
}

/// Iterates the values of a [`ByteArrayInterner`]'s dictionary page
///
/// `values` must be a page built by [`ByteArrayInterner::insert`], which is a
/// sequence of values each preceded by its exact length as a `u32`, and
/// `current_index` must start at 0, so it is always at the start of a value
struct ByteArrayInternerIterator<'a> {
    values: &'a [u8],
    current_index: usize,
}

impl<'a> Iterator for ByteArrayInternerIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.values.len() {
            return None;
        }

        // SAFETY: `current_index` is at the start of a value and is not past
        // the end of the page, so the value's 4-byte length prefix is in bounds
        let len_bytes = unsafe {
            self.values
                .get_unchecked(self.current_index..self.current_index + 4)
        };
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

        let start = self.current_index + 4;
        let end = start + len;

        // SAFETY: `insert` writes exactly `len` bytes after the prefix, and
        // rejects values whose length does not fit in a `u32`, so the prefix is
        // never truncated and `start..end` is within the page
        let value = unsafe { self.values.get_unchecked(start..end) };

        self.current_index = end;

        Some(value)
    }
}

fn maybe_inline_value(value: &[u8]) -> Option<OffsetOrInlineView> {
    if value.len() <= SHORT_VALUE_LEN {
        Some(OffsetOrInlineView::new_inlined(value))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_values_preserve_length_and_all_bytes() {
        let mut interner = ByteArrayInterner::default();
        let values: Vec<Vec<u8>> = (0..=8)
            .flat_map(|len| [vec![0; len], vec![0xff; len]])
            .collect();
        let mut unique = Vec::new();
        for value in &values {
            let expected = unique.iter().position(|v| v == value).unwrap_or_else(|| {
                unique.push(value.clone());
                unique.len() - 1
            });
            assert_eq!(interner.intern(value).unwrap(), expected as u64);
        }
        assert_eq!(interner.iter().collect::<Vec<_>>(), unique);
        for value in &unique {
            assert_eq!(maybe_inline_value(value).is_some(), value.len() <= 7);
        }
    }

    #[test]
    fn cached_hashes_and_first_seen_ids_survive_growth() {
        let mut interner = ByteArrayInterner::default();
        let values: Vec<_> = (0..4096_u64)
            .flat_map(|i| [i.to_le_bytes()[..7].to_vec(), i.to_le_bytes().to_vec()])
            .collect();
        for (index, value) in values.iter().enumerate() {
            assert_eq!(interner.intern(value).unwrap(), index as u64);
        }
        for (index, value) in values.iter().enumerate().rev() {
            assert_eq!(interner.intern(value).unwrap(), index as u64);
        }
        assert_eq!(interner.num_unique_values(), values.len());
        assert_eq!(interner.iter().collect::<Vec<_>>(), values);
        for entry in &interner.map {
            assert_eq!(
                entry.hash,
                interner
                    .state
                    .hash_one(&values[entry.index_in_dict as usize])
            );
        }
    }
}
