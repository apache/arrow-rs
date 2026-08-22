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

use crate::arrow::buffer::offset_buffer::OffsetBuffer;
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::errors::{ParquetError, Result};
use ahash::RandomState;
use arrow_array::{Array, DictionaryArray, downcast_integer};
use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, OffsetSizeTrait, cast::AsArray, make_array,
    types::ArrowDictionaryKeyType,
};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowType;
use hashbrown::HashMap as HbHashMap;
use hashbrown::hash_map::Entry;
use std::hash::{BuildHasher, Hasher};
use std::mem::size_of;
use std::ptr::write_unaligned;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::Arc;

/// An array of variable length byte arrays that are potentially dictionary encoded
/// and can be converted into a corresponding [`ArrayRef`]
pub enum DictionaryBuffer<K: ArrowNativeType, V: OffsetSizeTrait> {
    Dict { keys: Vec<K>, values: ArrayRef },
    Values { values: OffsetBuffer<V> },
}

impl<K: ArrowNativeType + Ord, V: OffsetSizeTrait> DictionaryBuffer<K, V> {
    #[cfg_attr(not(test), expect(unused))]
    pub fn len(&self) -> usize {
        match self {
            Self::Dict { keys, .. } => keys.len(),
            Self::Values { values } => values.len(),
        }
    }

    /// Returns a mutable reference to a keys array
    ///
    /// Returns None if the dictionary needs to be recomputed
    ///
    /// # Panics
    ///
    /// Panics if the dictionary is too large for `K`
    pub fn as_keys(&mut self, dictionary: &ArrayRef) -> Option<&mut Vec<K>> {
        assert!(K::from_usize(dictionary.len()).is_some());

        match self {
            Self::Dict { keys, values } => {
                // Need to discard fat pointer for equality check
                // - https://stackoverflow.com/a/67114787
                // - https://github.com/rust-lang/rust/issues/46139
                let values_ptr = std::ptr::from_ref(values.as_ref()).cast::<()>();
                let dict_ptr = std::ptr::from_ref(dictionary.as_ref()).cast::<()>();
                if values_ptr == dict_ptr {
                    Some(keys)
                } else if keys.is_empty() {
                    *values = Arc::clone(dictionary);
                    Some(keys)
                } else {
                    None
                }
            }
            Self::Values { values } if values.is_empty() => {
                *self = Self::Dict {
                    keys: Default::default(),
                    values: Arc::clone(dictionary),
                };
                match self {
                    Self::Dict { keys, .. } => Some(keys),
                    Self::Values { .. } => unreachable!(),
                }
            }
            Self::Values { .. } => None,
        }
    }

    /// Returns a mutable reference to a values array
    ///
    /// If this is currently dictionary encoded, this will convert from the
    /// dictionary encoded representation
    pub fn spill_values(&mut self) -> Result<&mut OffsetBuffer<V>> {
        match self {
            Self::Values { values } => Ok(values),
            Self::Dict { keys, values } => {
                let mut spilled = OffsetBuffer::with_capacity(0);
                let data = values.to_data();
                let dict_buffers = data.buffers();
                let dict_offsets = dict_buffers[0].typed_data::<V>();
                let dict_values = dict_buffers[1].as_slice();

                if values.is_empty() {
                    // If dictionary is empty, zero pad offsets
                    spilled.offsets.resize(keys.len() + 1, V::default());
                } else {
                    // Note: at this point null positions will have arbitrary dictionary keys
                    // and this will hydrate them to the corresponding byte array. This is
                    // likely sub-optimal, as we would prefer zero length null "slots", but
                    // spilling is already a degenerate case and so it is unclear if this is
                    // worth optimising for, e.g. by keeping a null mask around
                    spilled.extend_from_dictionary(keys.as_slice(), dict_offsets, dict_values)?;
                }

                *self = Self::Values { values: spilled };
                match self {
                    Self::Values { values } => Ok(values),
                    Self::Dict { .. } => unreachable!(),
                }
            }
        }
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    pub fn into_array(
        self,
        null_buffer: Option<Buffer>,
        data_type: &ArrowType,
        hash_scratch: &mut MutableBuffer,
    ) -> Result<ArrayRef> {
        assert!(matches!(data_type, ArrowType::Dictionary(_, _)));

        match self {
            Self::Dict { keys, values } => {
                // Validate keys unless dictionary is empty
                if !values.is_empty() {
                    let min = K::from_usize(0).unwrap();
                    let max = K::from_usize(values.len()).unwrap();

                    // using copied and fold gets auto-vectorized since rust 1.70
                    // all/any would allow early exit on invalid values
                    // but in the happy case all values have to be checked anyway
                    if !keys
                        .as_slice()
                        .iter()
                        .copied()
                        .fold(true, |a, x| a && x >= min && x < max)
                    {
                        return Err(general_err!(
                            "dictionary key beyond bounds of dictionary: 0..{}",
                            values.len()
                        ));
                    }
                }

                let ArrowType::Dictionary(_, value_type) = data_type else {
                    unreachable!()
                };
                let values = if let ArrowType::FixedSizeBinary(size) = **value_type {
                    let binary = values.as_binary::<i32>();
                    Arc::new(FixedSizeBinaryArray::new(
                        size,
                        binary.values().clone(),
                        binary.nulls().cloned(),
                    )) as _
                } else {
                    values
                };

                let builder = ArrayDataBuilder::new(data_type.clone())
                    .len(keys.len())
                    .add_buffer(Buffer::from_vec(keys))
                    .add_child_data(values.into_data())
                    .null_bit_buffer(null_buffer);

                let data = match cfg!(debug_assertions) {
                    true => builder.build().unwrap(),
                    false => unsafe { builder.build_unchecked() },
                };

                Ok(make_array(data))
            }
            Self::Values { values } => {
                let (key_type, value_type) = match data_type {
                    ArrowType::Dictionary(k, v) => (k, v.as_ref().clone()),
                    _ => unreachable!(),
                };

                hash_byte_slices(&values.offsets, &values.values, hash_scratch);
                let hashes = hashes_as_u64(hash_scratch);

                pack_values_from_offsets(key_type, &value_type, &values, hashes, null_buffer)
            }
        }
    }
}

impl<K: ArrowNativeType, V: OffsetSizeTrait> ValuesBuffer for DictionaryBuffer<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        Self::Values {
            values: OffsetBuffer::with_capacity(capacity),
        }
    }

    fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Dict { keys, .. } => keys.reserve_exact(additional),
            Self::Values { values, .. } => values.reserve_exact(additional),
        }
    }

    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()> {
        match self {
            Self::Dict { keys, .. } => {
                keys.resize(read_offset + levels_read, K::default());
                keys.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
            Self::Values { values, .. } => {
                values.pad_nulls(read_offset, values_read, levels_read, valid_mask)
            }
        }
    }
}

macro_rules! offsets_dict_helper {
    ($k:ty, $key_type:ident, $value_type:ident, $values:ident, $hashes:ident, $null_buffer:ident) => {
        pack_values_from_offsets_impl::<$k, _>(
            $values,
            $hashes,
            $null_buffer,
            $key_type,
            $value_type,
        )
    };
}

fn pack_values_from_offsets<V: OffsetSizeTrait>(
    key_type: &ArrowType,
    value_type: &ArrowType,
    values: &OffsetBuffer<V>,
    hashes: &[u64],
    null_buffer: Option<Buffer>,
) -> Result<ArrayRef> {
    downcast_integer! {
        key_type => (offsets_dict_helper, key_type, value_type, values, hashes, null_buffer),
        _ => unreachable!(),
    }
}

// Avoids double-hashing: keys are already high-quality u64 hashes from ahash,
// so we pass them through directly rather than re-hashing inside the HashMap.
struct PassthroughHasher(u64);
impl std::hash::Hasher for PassthroughHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}
#[derive(Default)]
struct BuildPassthroughHasher;
impl std::hash::BuildHasher for BuildPassthroughHasher {
    type Hasher = PassthroughHasher;
    fn build_hasher(&self) -> PassthroughHasher {
        PassthroughHasher(0)
    }
}

/// Builds a [`DictionaryArray`] directly from a flat [`OffsetBuffer`] using pre-computed
/// hashes to deduplicate values in a single pass, avoiding the intermediate StringArray
/// materialization
fn pack_values_from_offsets_impl<K: ArrowDictionaryKeyType, V: OffsetSizeTrait>(
    offset_buffer: &OffsetBuffer<V>,
    hashes: &[u64],
    null_buffer: Option<Buffer>,
    key_type: &ArrowType,
    value_type: &ArrowType,
) -> Result<ArrayRef> {
    let dict_type = ArrowType::Dictionary(Box::new(key_type.clone()), Box::new(value_type.clone()));
    let num_values = offset_buffer.len();

    let mut keys: Vec<K::Native> = Vec::with_capacity(num_values);
    let mut unique_offsets: Vec<V> = Vec::with_capacity(num_values + 1);
    unique_offsets.push(V::default());
    let mut unique_bytes: Vec<u8> = Vec::with_capacity(offset_buffer.values.len());

    let mut dedup: HbHashMap<u64, (usize, usize), BuildPassthroughHasher> =
        HbHashMap::with_capacity_and_hasher(num_values, BuildPassthroughHasher);

    // Tracks colliding values (same hash, different bytes) so repeated occurrences
    // are deduplicated rather than inserted as new entries. Empty in the common case.
    let mut collision_overflow: Vec<(u64, usize, usize)> = Vec::new();

    for (input_idx, &hash) in hashes.iter().enumerate() {
        let byte_start = offset_buffer.offsets[input_idx].as_usize();
        let byte_end = offset_buffer.offsets[input_idx + 1].as_usize();
        let bytes = &offset_buffer.values[byte_start..byte_end];

        let output_idx = match dedup.entry(hash) {
            Entry::Occupied(entry) => {
                let (first_input_idx, existing_output_idx) = *entry.get();
                let first_start = offset_buffer.offsets[first_input_idx].as_usize();
                let first_end = offset_buffer.offsets[first_input_idx + 1].as_usize();
                if &offset_buffer.values[first_start..first_end] == bytes {
                    existing_output_idx
                } else {
                    // True hash collision — check overflow list before inserting.
                    let existing = collision_overflow
                        .iter()
                        .find(|&&(entry_hash, collision_input_idx, _)| {
                            if entry_hash != hash {
                                return false;
                            }
                            let collision_start =
                                offset_buffer.offsets[collision_input_idx].as_usize();
                            let collision_end =
                                offset_buffer.offsets[collision_input_idx + 1].as_usize();
                            &offset_buffer.values[collision_start..collision_end] == bytes
                        })
                        .map(|&(_, _, collision_output_idx)| collision_output_idx);

                    match existing {
                        Some(collision_output_idx) => collision_output_idx,
                        None => {
                            let new_output_idx = unique_offsets.len() - 1;
                            unique_bytes.extend_from_slice(bytes);
                            let new_end = V::from_usize(unique_bytes.len()).ok_or_else(|| {
                                general_err!("offset overflow building dictionary")
                            })?;
                            unique_offsets.push(new_end);
                            collision_overflow.push((hash, input_idx, new_output_idx));
                            new_output_idx
                        }
                    }
                }
            }
            Entry::Vacant(entry) => {
                let output_idx = unique_offsets.len() - 1;
                unique_bytes.extend_from_slice(bytes);
                let new_end = V::from_usize(unique_bytes.len())
                    .ok_or_else(|| general_err!("offset overflow building dictionary"))?;
                unique_offsets.push(new_end);
                entry.insert((input_idx, output_idx));
                output_idx
            }
        };

        let key = K::Native::from_usize(output_idx)
            .ok_or_else(|| general_err!("dictionary key overflow"))?;
        keys.push(key);
    }

    let num_unique = unique_offsets.len() - 1;

    // SAFETY: buffers are constructed directly from typed Vecs above; offsets are
    // monotonically non-decreasing and bounded by unique_bytes.len(), and all
    // key values are within 0..num_unique, so the invariants Arrow requires hold.
    let value_data = unsafe {
        arrow_data::ArrayData::builder(value_type.clone())
            .len(num_unique)
            .add_buffer(Buffer::from_vec(unique_offsets))
            .add_buffer(Buffer::from_vec(unique_bytes))
            .build_unchecked()
    };

    // SAFETY: keys are within 0..num_unique and value_data is valid.
    let dict_array: DictionaryArray<K> = unsafe {
        arrow_data::ArrayData::builder(dict_type)
            .len(keys.len())
            .add_buffer(Buffer::from_vec(keys))
            .add_child_data(value_data)
            .null_bit_buffer(null_buffer)
            .build_unchecked()
            .into()
    };

    Ok(Arc::new(dict_array))
}

fn hash_byte_slices<I: ArrowNativeType>(offsets: &[I], values: &[u8], scratch: &mut MutableBuffer) {
    let count = offsets.len().saturating_sub(1);
    scratch.clear();
    scratch.resize(count * size_of::<u64>(), 0u8);

    let state = RandomState::new();

    // SAFETY: MutableBuffer is 64-byte aligned; scratch is sized to exactly count * size_of::<u64>()
    let hash_slots = unsafe { from_raw_parts_mut(scratch.as_mut_ptr().cast::<u64>(), count) };

    for idx in 0..count {
        let start = offsets[idx].as_usize();
        let end = offsets[idx + 1].as_usize();
        let mut hasher = state.build_hasher();
        hasher.write(&values[start..end]);
        // SAFETY: idx is within 0..count
        unsafe { write_unaligned(hash_slots.as_mut_ptr().add(idx), hasher.finish()) };
    }
}

#[inline]
fn hashes_as_u64(scratch: &[u8]) -> &[u64] {
    let n = scratch.len() / size_of::<u64>();
    // SAFETY: scratch was written as u64s by hash_byte_slices
    unsafe { from_raw_parts(scratch.as_ptr().cast::<u64>(), n) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::cast;
    use arrow_array::StringArray;
    use arrow_array::types::*;

    #[test]
    fn test_dictionary_buffer() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8));

        let d1: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world", "", "a", "b"]));

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);

        // Read some data preserving the dictionary
        let values = &[1, 0, 3, 2, 4];
        buffer.as_keys(&d1).unwrap().extend_from_slice(values);

        let mut valid = vec![false, false, true, true, false, true, true, true];
        let valid_buffer = Buffer::from_iter(valid.iter().copied());
        buffer
            .pad_nulls(0, values.len(), valid.len(), valid_buffer.as_slice())
            .unwrap();

        // Read some data not preserving the dictionary

        let values = buffer.spill_values().unwrap();
        let read_offset = values.len();
        values.try_push(b"bingo", false).unwrap();
        values.try_push(b"bongo", false).unwrap();

        valid.extend_from_slice(&[false, false, true, false, true]);
        let null_buffer = Buffer::from_iter(valid.iter().copied());
        buffer
            .pad_nulls(read_offset, 2, 5, null_buffer.as_slice())
            .unwrap();

        assert_eq!(buffer.len(), 13);
        let split = std::mem::replace(&mut buffer, DictionaryBuffer::with_capacity(0));

        let array = split
            .into_array(Some(null_buffer), &dict_type, &mut MutableBuffer::new(0))
            .unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
                None,
                None,
                Some("world"),
                Some("hello"),
                None,
                Some("a"),
                Some(""),
                Some("b"),
                None,
                None,
                Some("bingo"),
                None,
                Some("bongo")
            ]
        );

        // Can recreate with new dictionary as values is empty
        assert!(matches!(&buffer, DictionaryBuffer::Values { .. }));
        assert_eq!(buffer.len(), 0);
        let d2 = Arc::new(StringArray::from(vec!["bingo", ""])) as ArrayRef;
        buffer
            .as_keys(&d2)
            .unwrap()
            .extend_from_slice(&[0, 1, 0, 1]);

        let array = std::mem::replace(&mut buffer, DictionaryBuffer::with_capacity(0))
            .into_array(None, &dict_type, &mut MutableBuffer::new(0))
            .unwrap();
        assert_eq!(array.data_type(), &dict_type);

        let strings = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("bingo"), Some(""), Some("bingo"), Some("")]
        );

        // Can recreate with new dictionary as keys empty
        assert!(matches!(&buffer, DictionaryBuffer::Values { .. }));
        assert_eq!(buffer.len(), 0);
        let d3 = Arc::new(StringArray::from(vec!["bongo"])) as ArrayRef;
        buffer.as_keys(&d3).unwrap().extend_from_slice(&[0, 0]);

        // Cannot change dictionary as keys not empty
        let d4 = Arc::new(StringArray::from(vec!["bananas"])) as ArrayRef;
        assert!(buffer.as_keys(&d4).is_none());
    }

    #[test]
    fn test_validates_keys() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8));

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
        let d = Arc::new(StringArray::from(vec!["", "f"])) as ArrayRef;
        buffer.as_keys(&d).unwrap().extend_from_slice(&[0, 2, 0]);

        let err = buffer
            .into_array(None, &dict_type, &mut MutableBuffer::new(0))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("dictionary key beyond bounds of dictionary: 0..2"),
            "{}",
            err
        );

        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
        let d = Arc::new(StringArray::from(vec![""])) as ArrayRef;
        buffer.as_keys(&d).unwrap().extend_from_slice(&[0, 1, 0]);

        let err = buffer.spill_values().unwrap_err().to_string();
        assert!(
            err.contains("dictionary key beyond bounds of dictionary: 0..1"),
            "{}",
            err
        );
    }

    /// A dictionary requested with LargeUtf8 values must come back with LargeUtf8, not Utf8.
    #[test]
    fn test_values_path_large_utf8_type() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::LargeUtf8));
        let mut buffer = DictionaryBuffer::<i32, i64>::with_capacity(0);
        let values = buffer.spill_values().unwrap();
        for s in ["foo", "bar", "foo"] {
            values.try_push(s.as_bytes(), false).unwrap();
        }

        let array = buffer
            .into_array(None, &dict_type, &mut MutableBuffer::new(0))
            .unwrap();
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        assert_eq!(dict.data_type(), &dict_type);
        assert_eq!(dict.values().data_type(), &ArrowType::LargeUtf8);
    }

    /// 128 unique strings fill Int8 keys to capacity (last index = 127 = i8::MAX).
    /// A 129th unique string overflows and must return an error.
    #[test]
    fn test_int8_key_overflow_boundary() {
        let dict_type = ArrowType::Dictionary(Box::new(ArrowType::Int8), Box::new(ArrowType::Utf8));
        let mut scratch = MutableBuffer::new(0);

        {
            let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
            let values = buffer.spill_values().unwrap();
            for i in 0u32..128 {
                values
                    .try_push(format!("val{i}").as_bytes(), false)
                    .unwrap();
            }
            buffer
                .into_array(None, &dict_type, &mut scratch)
                .expect("128 unique strings must fit: last index is 127 = i8::MAX");
        }

        {
            let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
            let values = buffer.spill_values().unwrap();
            for i in 0u32..129 {
                values
                    .try_push(format!("val{i}").as_bytes(), false)
                    .unwrap();
            }
            let err = buffer
                .into_array(None, &dict_type, &mut scratch)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("dictionary key overflow"),
                "expected 'dictionary key overflow', got: {err}"
            );
        }
    }

    /// A colliding string repeated after its initial collision must reuse its key,
    /// not be inserted again as a new dictionary entry.
    #[test]
    fn test_hash_collision_deduplication() {
        let key_type = ArrowType::Int32;
        let value_type = ArrowType::Utf8;

        // "alpha", "beta", "beta" — all forced to share the same hash.
        let mut ob = OffsetBuffer::<i32>::with_capacity(3);
        ob.try_push(b"alpha", false).unwrap();
        ob.try_push(b"beta", false).unwrap();
        ob.try_push(b"beta", false).unwrap();
        let hashes = [0xdeadbeef_u64; 3];

        let array = pack_values_from_offsets_impl::<Int32Type, i32>(
            &ob,
            &hashes,
            None,
            &key_type,
            &value_type,
        )
        .unwrap();

        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        assert_eq!(dict.values().len(), 2);
        let keys: Vec<i32> = dict.keys().values().iter().copied().collect();
        assert_eq!(keys, vec![0, 1, 1]);
    }

    /// A dictionary requested with Binary values must come back with Binary, not Utf8.
    #[test]
    fn test_values_path_binary_type() {
        let dict_type =
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Binary));
        let mut buffer = DictionaryBuffer::<i32, i32>::with_capacity(0);
        let values = buffer.spill_values().unwrap();
        for s in [b"abc".as_ref(), b"\x00\xff", b"abc"] {
            values.try_push(s, false).unwrap();
        }

        let array = buffer
            .into_array(None, &dict_type, &mut MutableBuffer::new(0))
            .unwrap();
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        assert_eq!(dict.data_type(), &dict_type);
        assert_eq!(dict.values().data_type(), &ArrowType::Binary);
    }
}
