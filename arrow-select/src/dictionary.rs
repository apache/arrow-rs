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

//! Dictionary utilities for Arrow arrays

use std::sync::Arc;

use crate::filter::filter;
use crate::interleave::interleave;
use ahash::RandomState;
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, BinaryType, ByteArrayType, LargeBinaryType,
    LargeUtf8Type, Utf8Type,
};
use arrow_array::{
    AnyDictionaryArray, Array, ArrayRef, ArrowNativeTypeOp, BooleanArray, DictionaryArray,
    GenericByteArray, PrimitiveArray, downcast_dictionary_array,
};
use arrow_array::{cast::AsArray, downcast_primitive};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, ScalarBuffer, ToByteSlice};
use arrow_schema::{ArrowError, DataType};

/// Garbage collects a [DictionaryArray] by removing unreferenced values.
///
/// Returns a new [DictionaryArray] such that there are no values
/// that are not referenced by at least one key. There may still be duplicate
/// values.
///
/// See also [`garbage_collect_any_dictionary`] if you need to handle multiple dictionary types
pub fn garbage_collect_dictionary<K: ArrowDictionaryKeyType>(
    dictionary: &DictionaryArray<K>,
) -> Result<DictionaryArray<K>, ArrowError> {
    let keys = dictionary.keys();
    let values = dictionary.values();

    let mask = dictionary.occupancy();

    // If no work to do, return the original dictionary
    if mask.count_set_bits() == values.len() {
        return Ok(dictionary.clone());
    }

    // Create a mapping from the old keys to the new keys, use a Vec for easy indexing
    let mut key_remap = vec![K::Native::ZERO; values.len()];
    for (new_idx, old_idx) in mask.set_indices().enumerate() {
        key_remap[old_idx] = K::Native::from_usize(new_idx)
            .expect("new index should fit in K::Native, as old index was in range");
    }

    // ... and then build the new keys array
    let new_keys = keys.unary(|key| {
        key_remap
            .get(key.as_usize())
            .copied()
            // nulls may be present in the keys, and they will have arbitrary value; we don't care
            // and can safely return zero
            .unwrap_or(K::Native::ZERO)
    });

    // Create a new values array by filtering using the mask
    let values = filter(dictionary.values(), &BooleanArray::new(mask, None))?;

    DictionaryArray::try_new(new_keys, values)
}

/// Equivalent to [`garbage_collect_dictionary`] but without requiring casting to a specific key type.
pub fn garbage_collect_any_dictionary(
    dictionary: &dyn AnyDictionaryArray,
) -> Result<ArrayRef, ArrowError> {
    // FIXME: this is a workaround for MSRV Rust versions below 1.86 where trait upcasting is not stable.
    // From 1.86 onward, `&dyn AnyDictionaryArray` can be directly passed to `downcast_dictionary_array!`.
    let dictionary = &*dictionary.slice(0, dictionary.len());
    downcast_dictionary_array!(
        dictionary => garbage_collect_dictionary(dictionary).map(|dict| Arc::new(dict) as ArrayRef),
        _ => unreachable!("have a dictionary array")
    )
}

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

pub(crate) struct MergedDictionaries<K: ArrowDictionaryKeyType> {
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
type PtrEq = fn(&dyn Array, &dyn Array) -> bool;

/// A weak heuristic of whether to merge dictionary values that aims to only
/// perform the expensive merge computation when it is likely to yield at least
/// some return over the naive approach used by MutableArrayData
///
/// `len` is the total length of the merged output
///
/// Returns `(should_merge, has_overflow)` where:
/// - `should_merge`: whether dictionary values should be merged
/// - `has_overflow`: whether the combined dictionary values would overflow the key type
pub(crate) fn should_merge_dictionary_values<K: ArrowDictionaryKeyType>(
    dictionaries: &[&DictionaryArray<K>],
    len: usize,
) -> (bool, bool) {
    use DataType::*;
    let first_values = dictionaries[0].values().as_ref();
    let ptr_eq: PtrEq = match first_values.data_type() {
        Utf8 => bytes_ptr_eq::<Utf8Type>,
        LargeUtf8 => bytes_ptr_eq::<LargeUtf8Type>,
        Binary => bytes_ptr_eq::<BinaryType>,
        LargeBinary => bytes_ptr_eq::<LargeBinaryType>,
        dt => {
            if !dt.is_primitive() {
                return (
                    false,
                    K::Native::from_usize(dictionaries.iter().map(|d| d.values().len()).sum())
                        .is_none(),
                );
            }
            |a, b| a.to_data().ptr_eq(&b.to_data())
        }
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

    (
        !single_dictionary && (overflow || values_exceed_length),
        overflow,
    )
}

/// Given an array of dictionaries and an optional key mask compute a values array
/// containing referenced values, along with mappings from the [`DictionaryArray`]
/// keys to the new keys within this values array. Best-effort will be made to ensure
/// that the dictionary values are unique
///
/// This method is meant to be very fast and the output dictionary values
/// may not be unique, unlike `GenericByteDictionaryBuilder` which is slower
/// but produces unique values
pub(crate) fn merge_dictionary_values<K: ArrowDictionaryKeyType>(
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

/// Process primitive array values to bytes
fn masked_primitives_to_bytes<'a, T: ArrowPrimitiveType>(
    array: &'a PrimitiveArray<T>,
    mask: &BooleanBuffer,
) -> Vec<(usize, Option<&'a [u8]>)>
where
    T::Native: ToByteSlice,
{
    let mut out = Vec::with_capacity(mask.count_set_bits());
    let values = array.values();
    for idx in mask.set_indices() {
        out.push((
            idx,
            array.is_valid(idx).then_some(values[idx].to_byte_slice()),
        ))
    }
    out
}

macro_rules! masked_primitive_to_bytes_helper {
    ($t:ty, $array:expr, $mask:expr) => {
        masked_primitives_to_bytes::<$t>($array.as_primitive(), $mask)
    };
}

/// Return a Vec containing for each set index in `mask`, the index and byte value of that index
fn get_masked_values<'a>(
    array: &'a dyn Array,
    mask: &BooleanBuffer,
) -> Vec<(usize, Option<&'a [u8]>)> {
    downcast_primitive! {
        array.data_type() => (masked_primitive_to_bytes_helper, array, mask),
        DataType::Utf8 => masked_bytes(array.as_string::<i32>(), mask),
        DataType::LargeUtf8 => masked_bytes(array.as_string::<i64>(), mask),
        DataType::Binary => masked_bytes(array.as_binary::<i32>(), mask),
        DataType::LargeBinary => masked_bytes(array.as_binary::<i64>(), mask),
        _ => unimplemented!("Dictionary merging for type {} is not implemented", array.data_type()),
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
    use super::*;

    use arrow_array::cast::as_string_array;
    use arrow_array::types::Int8Type;
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Int8Array, Int32Array, StringArray};
    use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
    use std::sync::Arc;

    #[test]
    fn test_garbage_collect_i32_dictionary() {
        let values = StringArray::from_iter_values(["a", "b", "c", "d"]);
        let keys = Int32Array::from_iter_values([0, 1, 1, 3, 0, 0, 1]);
        let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

        // Only "a", "b", "d" are referenced, "c" is not
        let gc = garbage_collect_dictionary(&dict).unwrap();

        let expected_values = StringArray::from_iter_values(["a", "b", "d"]);
        let expected_keys = Int32Array::from_iter_values([0, 1, 1, 2, 0, 0, 1]);
        let expected = DictionaryArray::<Int32Type>::new(expected_keys, Arc::new(expected_values));

        assert_eq!(gc, expected);
    }

    #[test]
    fn test_garbage_collect_any_dictionary() {
        let values = StringArray::from_iter_values(["a", "b", "c", "d"]);
        let keys = Int32Array::from_iter_values([0, 1, 1, 3, 0, 0, 1]);
        let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

        let gc = garbage_collect_any_dictionary(&dict).unwrap();

        let expected_values = StringArray::from_iter_values(["a", "b", "d"]);
        let expected_keys = Int32Array::from_iter_values([0, 1, 1, 2, 0, 0, 1]);
        let expected = DictionaryArray::<Int32Type>::new(expected_keys, Arc::new(expected_values));

        assert_eq!(gc.as_ref(), &expected);
    }

    #[test]
    fn test_garbage_collect_with_nulls() {
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int8Array::from(vec![Some(2), None, Some(0)]);
        let dict = DictionaryArray::<Int8Type>::new(keys, Arc::new(values));

        let gc = garbage_collect_dictionary(&dict).unwrap();

        let expected_values = StringArray::from_iter_values(["a", "c"]);
        let expected_keys = Int8Array::from(vec![Some(1), None, Some(0)]);
        let expected = DictionaryArray::<Int8Type>::new(expected_keys, Arc::new(expected_values));

        assert_eq!(gc, expected);
    }

    #[test]
    fn test_garbage_collect_empty_dictionary() {
        let values = StringArray::from_iter_values::<&str, _>([]);
        let keys = Int32Array::from_iter_values([]);
        let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

        let gc = garbage_collect_dictionary(&dict).unwrap();

        assert_eq!(gc, dict);
    }

    #[test]
    fn test_garbage_collect_dictionary_all_unreferenced() {
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int32Array::from(vec![None, None, None]);
        let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));

        let gc = garbage_collect_dictionary(&dict).unwrap();

        // All keys are null, so dictionary values can be empty
        let expected_values = StringArray::from_iter_values::<&str, _>([]);
        let expected_keys = Int32Array::from(vec![None, None, None]);
        let expected = DictionaryArray::<Int32Type>::new(expected_keys, Arc::new(expected_values));

        assert_eq!(gc, expected);
    }

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
