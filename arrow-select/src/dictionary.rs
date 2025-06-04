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
use arrow_array::types::{
    ArrowDictionaryKeyType, BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type,
};
use arrow_array::{Array, ArrayRef, DictionaryArray, GenericByteArray};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};

/// A best effort interner that maintains a fixed number of buckets
/// and interns keys based on their hash value
///
/// Hash collisions will result in replacement
struct Interner<'a, V> {
    state: RandomState,
    buckets: Vec<Option<InternerBucket<'a, V>>>,
    shift: u32,
}

/// A single bucket in [`Interner`].
type InternerBucket<'a, V> = (Option<&'a [u8]>, V);

impl<'a, V> Interner<'a, V> {
    /// Capacity controls the number of unique buckets allocated within the Interner
    ///
    /// A larger capacity reduces the probability of hash collisions, and should be set
    /// based on an approximation of the upper bound of unique values
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
        new: Option<&'a [u8]>,
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

/// Performs a cheap, pointer-based comparison of two byte array
///
/// See [`ScalarBuffer::ptr_eq`]
fn bytes_ptr_eq<T: ByteArrayType>(a: &dyn Array, b: &dyn Array) -> bool {
    match (a.as_bytes_opt::<T>(), b.as_bytes_opt::<T>()) {
        (Some(a), Some(b)) => {
            let values_eq = a.values().ptr_eq(b.values()) && a.offsets().ptr_eq(b.offsets());
            match (a.nulls(), b.nulls()) {
                (Some(a), Some(b)) => values_eq && a.inner().ptr_eq(b.inner()),
                (None, None) => values_eq,
                _ => false,
            }
        }
        _ => false,
    }
}

/// A type-erased function that compares two array for pointer equality
type PtrEq = dyn Fn(&dyn Array, &dyn Array) -> bool;

/// A weak heuristic of whether to merge dictionary values that aims to only
/// perform the expensive merge computation when it is likely to yield at least
/// some return over the naive approach used by MutableArrayData
///
/// `len` is the total length of the merged output
pub fn should_merge_dictionary_values<K: ArrowDictionaryKeyType>(
    dictionaries: &[&DictionaryArray<K>],
    len: usize,
) -> bool {
    use DataType::*;
    let first_values = dictionaries[0].values().as_ref();
    let ptr_eq: Box<PtrEq> = match first_values.data_type() {
        Utf8 => Box::new(bytes_ptr_eq::<Utf8Type>),
        LargeUtf8 => Box::new(bytes_ptr_eq::<LargeUtf8Type>),
        Binary => Box::new(bytes_ptr_eq::<BinaryType>),
        LargeBinary => Box::new(bytes_ptr_eq::<LargeBinaryType>),
        _ => return false,
    };

    let mut single_dictionary = true;
    let mut total_values = first_values.len();
    for dict in dictionaries.iter().skip(1) {
        let values = dict.values().as_ref();
        total_values += values.len();
        if single_dictionary {
            single_dictionary = ptr_eq(first_values, values)
        }
    }

    let overflow = K::Native::from_usize(total_values).is_none();
    let values_exceed_length = total_values >= len;

    !single_dictionary && (overflow || values_exceed_length)
}

/// Given an array of dictionaries and an optional key mask compute a values array
/// containing referenced values, along with mappings from the [`DictionaryArray`]
/// keys to the new keys within this values array. Best-effort will be made to ensure
/// that the dictionary values are unique
///
/// This method is meant to be very fast and the output dictionary values
/// may not be unique, unlike `GenericByteDictionaryBuilder` which is slower
/// but produces unique values
pub fn merge_dictionary_values<K: ArrowDictionaryKeyType>(
    dictionaries: &[&DictionaryArray<K>],
    masks: Option<&[BooleanBuffer]>,
) -> Result<MergedDictionaries<K>, ArrowError> {
    let mut num_values = 0;

    let mut values_arrays = Vec::with_capacity(dictionaries.len());
    let mut value_slices = Vec::with_capacity(dictionaries.len());

    for (idx, dictionary) in dictionaries.iter().enumerate() {
        let mask = masks.and_then(|m| m.get(idx));
        let key_mask_owned;
        let key_mask = match (dictionary.nulls(), mask) {
            (Some(n), None) => Some(n.inner()),
            (None, Some(n)) => Some(n),
            (Some(n), Some(m)) => {
                key_mask_owned = n.inner() & m;
                Some(&key_mask_owned)
            }
            (None, None) => None,
        };
        let keys = dictionary.keys().values();
        let values = dictionary.values().as_ref();
        let values_mask = compute_values_mask(keys, key_mask, values.len());

        let masked_values = get_masked_values(values, &values_mask);
        num_values += masked_values.len();
        value_slices.push(masked_values);
        values_arrays.push(values)
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
        .map(|((dictionary_idx, dictionary), values)| {
            let zero = K::Native::from_usize(0).unwrap();
            let mut mapping = vec![zero; dictionary.values().len()];

            for (value_idx, value) in values {
                mapping[value_idx] =
                    *interner.intern(value, || match K::Native::from_usize(indices.len()) {
                        Some(idx) => {
                            indices.push((dictionary_idx, value_idx));
                            Ok(idx)
                        }
                        None => Err(ArrowError::DictionaryKeyOverflowError),
                    })?;
            }
            Ok(mapping)
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    Ok(MergedDictionaries {
        key_mappings,
        values: interleave(&values_arrays, &indices)?,
    })
}

/// Return a mask identifying the values that are referenced by keys in `dictionary`
/// at the positions indicated by `selection`
fn compute_values_mask<K: ArrowNativeType>(
    keys: &ScalarBuffer<K>,
    mask: Option<&BooleanBuffer>,
    max_key: usize,
) -> BooleanBuffer {
    let mut builder = BooleanBufferBuilder::new(max_key);
    builder.advance(max_key);

    match mask {
        Some(n) => n
            .set_indices()
            .for_each(|idx| builder.set_bit(keys[idx].as_usize(), true)),
        None => keys
            .iter()
            .for_each(|k| builder.set_bit(k.as_usize(), true)),
    }
    builder.finish()
}

/// Return a Vec containing for each set index in `mask`, the index and byte value of that index
fn get_masked_values<'a>(
    array: &'a dyn Array,
    mask: &BooleanBuffer,
) -> Vec<(usize, Option<&'a [u8]>)> {
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
) -> Vec<(usize, Option<&'a [u8]>)> {
    let mut out = Vec::with_capacity(mask.count_set_bits());
    for idx in mask.set_indices() {
        out.push((
            idx,
            array.is_valid(idx).then_some(array.value(idx).as_ref()),
        ))
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::dictionary::merge_dictionary_values;
    use arrow_array::cast::as_string_array;
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Int32Array, StringArray};
    use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
    use std::sync::Arc;

    #[test]
    fn test_merge_strings() {
        let a = DictionaryArray::<Int32Type>::from_iter(["a", "b", "a", "b", "d", "c", "e"]);
        let b = DictionaryArray::<Int32Type>::from_iter(["c", "f", "c", "d", "a", "d"]);
        let merged = merge_dictionary_values(&[&a, &b], None).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "e", "f"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 1, 2, 3, 4]);
        assert_eq!(&merged.key_mappings[1], &[3, 5, 2, 0]);

        let a_slice = a.slice(1, 4);
        let merged = merge_dictionary_values(&[&a_slice, &b], None).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "f"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 1, 2, 0, 0]);
        assert_eq!(&merged.key_mappings[1], &[3, 4, 2, 0]);

        // Mask out only ["b", "b", "d"] from a
        let a_mask = BooleanBuffer::from_iter([false, true, false, true, true, false, false]);
        let b_mask = BooleanBuffer::new_set(b.len());
        let merged = merge_dictionary_values(&[&a, &b], Some(&[a_mask, b_mask])).unwrap();

        let values = as_string_array(merged.values.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["b", "d", "c", "f", "a"]);

        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 0, 1, 0, 0]);
        assert_eq!(&merged.key_mappings[1], &[2, 3, 1, 4]);
    }

    #[test]
    fn test_merge_nulls() {
        let buffer = Buffer::from(b"helloworldbingohelloworld");
        let offsets = OffsetBuffer::from_lengths([5, 5, 5, 5, 5]);
        let nulls = NullBuffer::from(vec![true, false, true, true, true]);
        let values = StringArray::new(offsets, buffer, Some(nulls));

        let key_values = vec![1, 2, 3, 1, 8, 2, 3];
        let key_nulls = NullBuffer::from(vec![true, true, false, true, false, true, true]);
        let keys = Int32Array::new(key_values.into(), Some(key_nulls));
        let a = DictionaryArray::new(keys, Arc::new(values));
        // [NULL, "bingo", NULL, NULL, NULL, "bingo", "hello"]

        let b = DictionaryArray::new(Int32Array::new_null(10), Arc::new(StringArray::new_null(0)));

        let merged = merge_dictionary_values(&[&a, &b], None).unwrap();
        let expected = StringArray::from(vec![None, Some("bingo"), Some("hello")]);
        assert_eq!(merged.values.as_ref(), &expected);
        assert_eq!(merged.key_mappings.len(), 2);
        assert_eq!(&merged.key_mappings[0], &[0, 0, 1, 2, 0]);
        assert_eq!(&merged.key_mappings[1], &[] as &[i32; 0]);
    }

    #[test]
    fn test_merge_keys_smaller() {
        let values = StringArray::from_iter_values(["a", "b"]);
        let keys = Int32Array::from_iter_values([1]);
        let a = DictionaryArray::new(keys, Arc::new(values));

        let merged = merge_dictionary_values(&[&a], None).unwrap();
        let expected = StringArray::from(vec!["b"]);
        assert_eq!(merged.values.as_ref(), &expected);
    }
}
