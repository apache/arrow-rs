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

use crate::interleave::interleave;
use ahash::RandomState;
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowDictionaryKeyType, ByteArrayType};
use arrow_array::{Array, ArrayRef, DictionaryArray, GenericByteArray};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

/// A best effort interner that maintains a fixed number of buckets
/// and interns keys based on their hash value
///
/// Hash collisions will result in replacement
struct Interner<'a, V> {
    state: RandomState,
    buckets: Vec<Option<(&'a [u8], V)>>,
    shift: u32,
}

impl<'a, V> Interner<'a, V> {
    fn new(capacity: usize) -> Self {
        // Add additional buckets to help reduce collisions
        let shift = (capacity as u64 + 128).leading_zeros();
        let num_buckets = (u64::MAX >> shift) as usize;
        let buckets = (0..num_buckets.saturating_add(1)).map(|_| None).collect();
        Self {
            // A fixed seed to ensure deterministic behaviour
            state: RandomState::with_seeds(0, 0, 0, 0),
            buckets,
            shift,
        }
    }

    fn intern<F: FnOnce() -> Result<V, E>, E>(
        &mut self,
        new: &'a [u8],
        f: F,
    ) -> Result<&V, E> {
        let hash = self.state.hash_one(new);
        let bucket_idx = hash >> self.shift;
        Ok(match &mut self.buckets[bucket_idx as usize] {
            Some((current, v)) => {
                if *current != new {
                    *v = f()?;
                    *current = new;
                }
                v
            }
            slot => &slot.insert((new, f()?)).1,
        })
    }
}

pub struct MergedDictionaries<K: ArrowDictionaryKeyType> {
    /// Provides `key_mappings[`array_idx`][`old_key`] -> new_key`
    pub key_mappings: Vec<Vec<K::Native>>,
    /// The new values
    pub values: ArrayRef,
}

/// A weak heuristic of whether to merge dictionary values that aims to only
/// perform the expensive computation when is likely to yield at least
/// some return over the naive approach used by MutableArrayData
///
/// `len` is the total length of the merged output
pub fn should_merge_dictionary_values<K: ArrowDictionaryKeyType>(
    arrays: &[&dyn Array],
    len: usize,
) -> bool {
    let first_array = &arrays[0].to_data();
    let first_values = &first_array.child_data()[0];

    let mut single_dictionary = true;
    let mut total_values = first_values.len();
    for a in arrays.iter().skip(1) {
        let data = a.to_data();

        let values = &data.child_data()[0];
        total_values += values.len();
        single_dictionary &= ArrayData::ptr_eq(values, first_values);
    }

    let overflow = K::Native::from_usize(total_values).is_none();
    let values_exceed_length = total_values >= len;
    let is_supported = first_values.data_type().is_byte_array();

    !single_dictionary && is_supported && (overflow || values_exceed_length)
}

/// Given an array of dictionaries and an optional row mask compute a values array
/// containing referenced values, along with mappings from the [`DictionaryArray`]
/// keys to the new keys within this values array. Best-effort will be made to ensure
/// that the dictionary values are unique
pub fn merge_dictionary_values<K: ArrowDictionaryKeyType>(
    dictionaries: &[(&DictionaryArray<K>, Option<BooleanBuffer>)],
) -> Result<MergedDictionaries<K>, ArrowError> {
    let mut num_values = 0;

    let mut values = Vec::with_capacity(dictionaries.len());
    let mut value_slices = Vec::with_capacity(dictionaries.len());

    for (dictionary, key_mask) in dictionaries {
        let values_mask = match key_mask {
            Some(key_mask) => compute_values_mask(dictionary, key_mask.set_indices()),
            None => compute_values_mask(dictionary, 0..dictionary.len()),
        };
        let v = dictionary.values().as_ref();
        num_values += v.len();
        value_slices.push(get_masked_values(v, &values_mask));
        values.push(v)
    }

    // Map from value to new index
    let mut interner = Interner::new(num_values);
    // Interleave indices for new values array
    let mut indices = Vec::with_capacity(num_values);

    // Compute the mapping for each dictionary
    let key_mappings = dictionaries
        .iter()
        .enumerate()
        .zip(value_slices)
        .map(|((dictionary_idx, (dictionary, _)), values)| {
            let zero = K::Native::from_usize(0).unwrap();
            let mut mapping = vec![zero; dictionary.values().len()];

            for (value_idx, value) in values {
                mapping[value_idx] = *interner.intern(value, || {
                    match K::Native::from_usize(indices.len()) {
                        Some(idx) => {
                            indices.push((dictionary_idx, value_idx));
                            Ok(idx)
                        }
                        None => Err(ArrowError::DictionaryKeyOverflowError),
                    }
                })?;
            }
            Ok(mapping)
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    Ok(MergedDictionaries {
        key_mappings,
        values: interleave(&values, &indices)?,
    })
}

/// Return a mask identifying the values that are referenced by keys in `dictionary`
/// at the positions indicated by `selection`
fn compute_values_mask<K, I>(
    dictionary: &DictionaryArray<K>,
    selection: I,
) -> BooleanBuffer
where
    K: ArrowDictionaryKeyType,
    I: IntoIterator<Item = usize>,
{
    let len = dictionary.values().len();
    let mut builder =
        BooleanBufferBuilder::new_from_buffer(MutableBuffer::new_null(len), len);

    let keys = dictionary.keys();

    for i in selection {
        if keys.is_valid(i) {
            let key = keys.values()[i];
            builder.set_bit(key.as_usize(), true)
        }
    }
    builder.finish()
}

/// Return a Vec containing for each set index in `mask`, the index and byte value of that index
fn get_masked_values<'a>(
    array: &'a dyn Array,
    mask: &BooleanBuffer,
) -> Vec<(usize, &'a [u8])> {
    match array.data_type() {
        DataType::Utf8 => masked_bytes(array.as_string::<i32>(), mask),
        DataType::LargeUtf8 => masked_bytes(array.as_string::<i64>(), mask),
        DataType::Binary => masked_bytes(array.as_binary::<i32>(), mask),
        DataType::LargeBinary => masked_bytes(array.as_binary::<i64>(), mask),
        _ => unimplemented!(),
    }
}

/// Compute [`get_masked_values`] for a [`GenericByteArray`]
///
/// Note: this does not check the null mask and will return values contained in null slots
fn masked_bytes<'a, T: ByteArrayType>(
    array: &'a GenericByteArray<T>,
    mask: &BooleanBuffer,
) -> Vec<(usize, &'a [u8])> {
    let mut out = Vec::with_capacity(mask.count_set_bits());
    for idx in mask.set_indices() {
        out.push((idx, array.value(idx).as_ref()))
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::dictionary::merge_dictionary_values;
    use arrow_array::cast::as_string_array;
    use arrow_array::types::Int32Type;
    use arrow_array::DictionaryArray;
    use arrow_buffer::BooleanBuffer;

    #[test]
    fn test_merge_strings() {
        let a =
            DictionaryArray::<Int32Type>::from_iter(["a", "b", "a", "b", "d", "c", "e"]);
        let b = DictionaryArray::<Int32Type>::from_iter(["c", "f", "c", "d", "a", "d"]);
        let merged = merge_dictionary_values(&[(&a, None), (&b, None)]).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "e", "f"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 1, 2, 3, 4]);
        assert_eq!(&merged.key_mappings[1], &[3, 5, 2, 0]);

        let a_slice = a.slice(1, 4);
        let merged = merge_dictionary_values(&[(&a_slice, None), (&b, None)]).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "f"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 1, 2, 0, 0]);
        assert_eq!(&merged.key_mappings[1], &[3, 4, 2, 0]);

        // Mask out only ["b", "b", "d"] from a
        let mask =
            BooleanBuffer::from_iter([false, true, false, true, true, false, false]);
        let merged = merge_dictionary_values(&[(&a, Some(mask)), (&b, None)]).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["b", "d", "c", "f", "a"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 0, 1, 0, 0]);
        assert_eq!(&merged.key_mappings[1], &[2, 3, 1, 4]);
    }
}
