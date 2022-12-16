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

use crate::builder::{ArrayBuilder, PrimitiveBuilder, StringBuilder};
use crate::types::ArrowDictionaryKeyType;
use crate::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use std::any::Any;
use std::sync::Arc;

/// Array builder for `DictionaryArray` that stores Strings. For example to map a set of byte indices
/// to String values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
///
/// ```
/// // Create a dictionary array indexed by bytes whose values are Strings.
/// // It can thus hold up to 256 distinct string values.
///
/// # use arrow_array::builder::StringDictionaryBuilder;
/// # use arrow_array::{Int8Array, StringArray};
/// # use arrow_array::types::Int8Type;
///
/// let mut builder = StringDictionaryBuilder::<Int8Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append("abc").unwrap();
/// builder.append_null();
/// builder.append("def").unwrap();
/// builder.append("def").unwrap();
/// builder.append("abc").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///   array.keys(),
///   &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
/// );
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(ava.value(0), "abc");
/// assert_eq!(ava.value(1), "def");
///
/// ```
#[derive(Debug)]
pub struct StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    state: ahash::RandomState,
    /// Used to provide a lookup from string value to key type
    ///
    /// Note: K's hash implementation is not used, instead the raw entry
    /// API is used to store keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<K::Native, (), ()>,

    keys_builder: PrimitiveBuilder<K>,
    values_builder: StringBuilder,
}

impl<K> Default for StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Creates a new `StringDictionaryBuilder`
    pub fn new() -> Self {
        let keys_builder = PrimitiveBuilder::new();
        let values_builder = StringBuilder::new();
        Self {
            state: Default::default(),
            dedup: HashMap::with_capacity_and_hasher(keys_builder.capacity(), ()),
            keys_builder,
            values_builder,
        }
    }

    /// Creates a new `StringDictionaryBuilder` with the provided capacities
    ///
    /// `keys_capacity`: the number of keys, i.e. length of array to build
    /// `value_capacity`: the number of distinct dictionary values, i.e. size of dictionary
    /// `string_capacity`: the total number of bytes of all distinct strings in the dictionary
    pub fn with_capacity(
        keys_capacity: usize,
        value_capacity: usize,
        string_capacity: usize,
    ) -> Self {
        Self {
            state: Default::default(),
            dedup: Default::default(),
            keys_builder: PrimitiveBuilder::with_capacity(keys_capacity),
            values_builder: StringBuilder::with_capacity(value_capacity, string_capacity),
        }
    }

    /// Creates a new `StringDictionaryBuilder` from a keys capacity and a dictionary
    /// which is initialized with the given values.
    /// The indices of those dictionary values are used as keys.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_array::builder::StringDictionaryBuilder;
    /// # use arrow_array::{Int16Array, StringArray};
    ///
    /// let dictionary_values = StringArray::from(vec![None, Some("abc"), Some("def")]);
    ///
    /// let mut builder = StringDictionaryBuilder::new_with_dictionary(3, &dictionary_values).unwrap();
    /// builder.append("def").unwrap();
    /// builder.append_null();
    /// builder.append("abc").unwrap();
    ///
    /// let dictionary_array = builder.finish();
    ///
    /// let keys = dictionary_array.keys();
    ///
    /// assert_eq!(keys, &Int16Array::from(vec![Some(2), None, Some(1)]));
    /// ```
    pub fn new_with_dictionary(
        keys_capacity: usize,
        dictionary_values: &StringArray,
    ) -> Result<Self, ArrowError> {
        let state = ahash::RandomState::default();
        let dict_len = dictionary_values.len();

        let mut dedup = HashMap::with_capacity_and_hasher(dict_len, ());

        let values_len = dictionary_values.value_data().len();
        let mut values_builder = StringBuilder::with_capacity(dict_len, values_len);

        for (idx, maybe_value) in dictionary_values.iter().enumerate() {
            match maybe_value {
                Some(value) => {
                    let hash = state.hash_one(value.as_bytes());

                    let key = K::Native::from_usize(idx)
                        .ok_or(ArrowError::DictionaryKeyOverflowError)?;

                    let entry =
                        dedup.raw_entry_mut().from_hash(hash, |key: &K::Native| {
                            value.as_bytes() == get_bytes(&values_builder, key)
                        });

                    if let RawEntryMut::Vacant(v) = entry {
                        v.insert_with_hasher(hash, key, (), |key| {
                            state.hash_one(get_bytes(&values_builder, key))
                        });
                    }

                    values_builder.append_value(value);
                }
                None => values_builder.append_null(),
            }
        }

        Ok(Self {
            state,
            dedup,
            keys_builder: PrimitiveBuilder::with_capacity(keys_capacity),
            values_builder,
        })
    }
}

impl<K> ArrayBuilder for StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Returns the builder as an non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.keys_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.keys_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    ///
    /// Returns an error if the new index would overflow the key type.
    pub fn append(&mut self, value: impl AsRef<str>) -> Result<K::Native, ArrowError> {
        let value = value.as_ref();

        let state = &self.state;
        let storage = &mut self.values_builder;
        let hash = state.hash_one(value.as_bytes());

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |key| value.as_bytes() == get_bytes(storage, key));

        let key = match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let index = storage.len();
                storage.append_value(value);
                let key = K::Native::from_usize(index)
                    .ok_or(ArrowError::DictionaryKeyOverflowError)?;

                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        state.hash_one(get_bytes(storage, key))
                    })
                    .0
            }
        };
        self.keys_builder.append_value(key);

        Ok(key)
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.dedup.clear();
        let values = self.values_builder.finish();
        let keys = self.keys_builder.finish();

        let data_type =
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(DataType::Utf8));

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }

    /// Builds the `DictionaryArray` without resetting the builder.
    pub fn finish_cloned(&self) -> DictionaryArray<K> {
        let values = self.values_builder.finish_cloned();
        let keys = self.keys_builder.finish_cloned();

        let data_type =
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(DataType::Utf8));

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }
}

fn get_bytes<'a, K: ArrowNativeType>(values: &'a StringBuilder, key: &K) -> &'a [u8] {
    let offsets = values.offsets_slice();
    let values = values.values_slice();

    let idx = key.as_usize();
    let end_offset = offsets[idx + 1].as_usize();
    let start_offset = offsets[idx].as_usize();

    &values[start_offset..end_offset]
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::array::Int8Array;
    use crate::types::{Int16Type, Int8Type};

    #[test]
    fn test_string_dictionary_builder() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ava.value(0), "abc");
        assert_eq!(ava.value(1), "def");
    }

    #[test]
    fn test_string_dictionary_builder_finish_cloned() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let mut array = builder.finish_cloned();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ava.value(0), "abc");
        assert_eq!(ava.value(1), "def");

        builder.append("abc").unwrap();
        builder.append("ghi").unwrap();
        builder.append("def").unwrap();

        array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(1),
                Some(0),
                Some(0),
                Some(2),
                Some(1)
            ])
        );

        // Values are polymorphic and so require a downcast.
        let av2 = array.values();
        let ava2: &StringArray = av2.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ava2.value(0), "abc");
        assert_eq!(ava2.value(1), "def");
        assert_eq!(ava2.value(2), "ghi");
    }

    #[test]
    fn test_string_dictionary_builder_with_existing_dictionary() {
        let dictionary = StringArray::from(vec![None, Some("def"), Some("abc")]);

        let mut builder =
            StringDictionaryBuilder::new_with_dictionary(6, &dictionary).unwrap();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        builder.append("ghi").unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(2), None, Some(1), Some(1), Some(2), Some(3)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert!(!ava.is_valid(0));
        assert_eq!(ava.value(1), "def");
        assert_eq!(ava.value(2), "abc");
        assert_eq!(ava.value(3), "ghi");
    }

    #[test]
    fn test_string_dictionary_builder_with_reserved_null_value() {
        let dictionary: Vec<Option<&str>> = vec![None];
        let dictionary = StringArray::from(dictionary);

        let mut builder =
            StringDictionaryBuilder::<Int16Type>::new_with_dictionary(4, &dictionary)
                .unwrap();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();

        assert!(array.is_null(1));
        assert!(!array.is_valid(1));

        let keys = array.keys();

        assert_eq!(keys.value(0), 1);
        assert!(keys.is_null(1));
        // zero initialization is currently guaranteed by Buffer allocation and resizing
        assert_eq!(keys.value(1), 0);
        assert_eq!(keys.value(2), 2);
        assert_eq!(keys.value(3), 1);
    }
}
