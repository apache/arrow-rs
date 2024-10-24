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

use crate::builder::{ArrayBuilder, GenericByteBuilder, PrimitiveBuilder};
use crate::types::{ArrowDictionaryKeyType, ByteArrayType, GenericBinaryType, GenericStringType};
use crate::{Array, ArrayRef, DictionaryArray, GenericByteArray};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};
use hashbrown::HashTable;
use std::any::Any;
use std::sync::Arc;

/// Builder for [`DictionaryArray`] of [`GenericByteArray`]
///
/// For example to map a set of byte indices to String values. Note that
/// the use of a `HashMap` here will not scale to very large arrays or
/// result in an ordered dictionary.
#[derive(Debug)]
pub struct GenericByteDictionaryBuilder<K, T>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    state: ahash::RandomState,
    dedup: HashTable<usize>,

    keys_builder: PrimitiveBuilder<K>,
    values_builder: GenericByteBuilder<T>,
}

impl<K, T> Default for GenericByteDictionaryBuilder<K, T>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T> GenericByteDictionaryBuilder<K, T>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    /// Creates a new `GenericByteDictionaryBuilder`
    pub fn new() -> Self {
        let keys_builder = PrimitiveBuilder::new();
        let values_builder = GenericByteBuilder::<T>::new();
        Self {
            state: Default::default(),
            dedup: HashTable::with_capacity(keys_builder.capacity()),
            keys_builder,
            values_builder,
        }
    }

    /// Creates a new `GenericByteDictionaryBuilder` with the provided capacities
    ///
    /// `keys_capacity`: the number of keys, i.e. length of array to build
    /// `value_capacity`: the number of distinct dictionary values, i.e. size of dictionary
    /// `data_capacity`: the total number of bytes of all distinct bytes in the dictionary
    pub fn with_capacity(
        keys_capacity: usize,
        value_capacity: usize,
        data_capacity: usize,
    ) -> Self {
        Self {
            state: Default::default(),
            dedup: Default::default(),
            keys_builder: PrimitiveBuilder::with_capacity(keys_capacity),
            values_builder: GenericByteBuilder::<T>::with_capacity(value_capacity, data_capacity),
        }
    }

    /// Creates a new `GenericByteDictionaryBuilder` from a keys capacity and a dictionary
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
        dictionary_values: &GenericByteArray<T>,
    ) -> Result<Self, ArrowError> {
        let state = ahash::RandomState::default();
        let dict_len = dictionary_values.len();

        let mut dedup = HashTable::with_capacity(dict_len);

        let values_len = dictionary_values.value_data().len();
        let mut values_builder = GenericByteBuilder::<T>::with_capacity(dict_len, values_len);

        K::Native::from_usize(dictionary_values.len())
            .ok_or(ArrowError::DictionaryKeyOverflowError)?;

        for (idx, maybe_value) in dictionary_values.iter().enumerate() {
            match maybe_value {
                Some(value) => {
                    let value_bytes: &[u8] = value.as_ref();
                    let hash = state.hash_one(value_bytes);

                    dedup
                        .entry(
                            hash,
                            |idx: &usize| value_bytes == get_bytes(&values_builder, *idx),
                            |idx: &usize| state.hash_one(get_bytes(&values_builder, *idx)),
                        )
                        .or_insert(idx);

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

impl<K, T> ArrayBuilder for GenericByteDictionaryBuilder<K, T>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
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

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

impl<K, T> GenericByteDictionaryBuilder<K, T>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    fn get_or_insert_key(&mut self, value: impl AsRef<T::Native>) -> Result<K::Native, ArrowError> {
        let value_native: &T::Native = value.as_ref();
        let value_bytes: &[u8] = value_native.as_ref();

        let state = &self.state;
        let storage = &mut self.values_builder;
        let hash = state.hash_one(value_bytes);

        let idx = *self
            .dedup
            .entry(
                hash,
                |idx| value_bytes == get_bytes(storage, *idx),
                |idx| state.hash_one(get_bytes(storage, *idx)),
            )
            .or_insert_with(|| {
                let idx = storage.len();
                storage.append_value(value);
                idx
            })
            .get();

        let key = K::Native::from_usize(idx).ok_or(ArrowError::DictionaryKeyOverflowError)?;

        Ok(key)
    }

    /// Append a value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    ///
    /// Returns an error if the new index would overflow the key type.
    pub fn append(&mut self, value: impl AsRef<T::Native>) -> Result<K::Native, ArrowError> {
        let key = self.get_or_insert_key(value)?;
        self.keys_builder.append_value(key);
        Ok(key)
    }

    /// Append a value multiple times to the array.
    /// This is the same as `append` but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// Returns an error if the new index would overflow the key type.
    pub fn append_n(
        &mut self,
        value: impl AsRef<T::Native>,
        count: usize,
    ) -> Result<K::Native, ArrowError> {
        let key = self.get_or_insert_key(value)?;
        self.keys_builder.append_value_n(key, count);
        Ok(key)
    }

    /// Infallibly append a value to this builder
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    pub fn append_value(&mut self, value: impl AsRef<T::Native>) {
        self.append(value).expect("dictionary key overflow");
    }

    /// Infallibly append a value to this builder repeatedly `count` times.
    /// This is the same as `append_value` but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    pub fn append_values(&mut self, value: impl AsRef<T::Native>, count: usize) {
        self.append_n(value, count)
            .expect("dictionary key overflow");
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.keys_builder.append_null()
    }

    /// Infallibly append `n` null slots into the builder
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.keys_builder.append_nulls(n)
    }

    /// Append an `Option` value into the builder
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<T::Native>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Append an `Option` value into the builder repeatedly `count` times.
    /// This is the same as `append_option` but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    pub fn append_options(&mut self, value: Option<impl AsRef<T::Native>>, count: usize) {
        match value {
            None => self.keys_builder.append_nulls(count),
            Some(v) => self.append_values(v, count),
        };
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.dedup.clear();
        let values = self.values_builder.finish();
        let keys = self.keys_builder.finish();

        let data_type = DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(T::DATA_TYPE));

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

        let data_type = DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(T::DATA_TYPE));

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.keys_builder.validity_slice()
    }
}

impl<K: ArrowDictionaryKeyType, T: ByteArrayType, V: AsRef<T::Native>> Extend<Option<V>>
    for GenericByteDictionaryBuilder<K, T>
{
    #[inline]
    fn extend<I: IntoIterator<Item = Option<V>>>(&mut self, iter: I) {
        for v in iter {
            self.append_option(v)
        }
    }
}

fn get_bytes<T: ByteArrayType>(values: &GenericByteBuilder<T>, idx: usize) -> &[u8] {
    let offsets = values.offsets_slice();
    let values = values.values_slice();

    let end_offset = offsets[idx + 1].as_usize();
    let start_offset = offsets[idx].as_usize();

    &values[start_offset..end_offset]
}

/// Builder for [`DictionaryArray`] of [`StringArray`](crate::array::StringArray)
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
/// builder.append_n("def", 2).unwrap();  // appends "def" twice with a single lookup
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
pub type StringDictionaryBuilder<K> = GenericByteDictionaryBuilder<K, GenericStringType<i32>>;

/// Builder for [`DictionaryArray`] of [`LargeStringArray`](crate::array::LargeStringArray)
pub type LargeStringDictionaryBuilder<K> = GenericByteDictionaryBuilder<K, GenericStringType<i64>>;

/// Builder for [`DictionaryArray`] of [`BinaryArray`](crate::array::BinaryArray)
///
/// ```
/// // Create a dictionary array indexed by bytes whose values are binary.
/// // It can thus hold up to 256 distinct binary values.
///
/// # use arrow_array::builder::BinaryDictionaryBuilder;
/// # use arrow_array::{BinaryArray, Int8Array};
/// # use arrow_array::types::Int8Type;
///
/// let mut builder = BinaryDictionaryBuilder::<Int8Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append(b"abc").unwrap();
/// builder.append_null();
/// builder.append(b"def").unwrap();
/// builder.append(b"def").unwrap();
/// builder.append(b"abc").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///   array.keys(),
///   &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
/// );
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &BinaryArray = av.as_any().downcast_ref::<BinaryArray>().unwrap();
///
/// assert_eq!(ava.value(0), b"abc");
/// assert_eq!(ava.value(1), b"def");
///
/// ```
pub type BinaryDictionaryBuilder<K> = GenericByteDictionaryBuilder<K, GenericBinaryType<i32>>;

/// Builder for [`DictionaryArray`] of [`LargeBinaryArray`](crate::array::LargeBinaryArray)
pub type LargeBinaryDictionaryBuilder<K> = GenericByteDictionaryBuilder<K, GenericBinaryType<i64>>;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Int8Array;
    use crate::types::{Int16Type, Int32Type, Int8Type, Utf8Type};
    use crate::{BinaryArray, StringArray};

    fn test_bytes_dictionary_builder<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteDictionaryBuilder::<Int8Type, T>::new();
        builder.append(values[0]).unwrap();
        builder.append_null();
        builder.append(values[1]).unwrap();
        builder.append(values[1]).unwrap();
        builder.append(values[0]).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> = av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(*ava.value(0), *values[0]);
        assert_eq!(*ava.value(1), *values[1]);
    }

    #[test]
    fn test_string_dictionary_builder() {
        test_bytes_dictionary_builder::<GenericStringType<i32>>(vec!["abc", "def"]);
    }

    #[test]
    fn test_binary_dictionary_builder() {
        test_bytes_dictionary_builder::<GenericBinaryType<i32>>(vec![b"abc", b"def"]);
    }

    fn test_bytes_dictionary_builder_finish_cloned<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteDictionaryBuilder::<Int8Type, T>::new();

        builder.append(values[0]).unwrap();
        builder.append_null();
        builder.append(values[1]).unwrap();
        builder.append(values[1]).unwrap();
        builder.append(values[0]).unwrap();
        let mut array = builder.finish_cloned();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> = av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava.value(0), values[0]);
        assert_eq!(ava.value(1), values[1]);

        builder.append(values[0]).unwrap();
        builder.append(values[2]).unwrap();
        builder.append(values[1]).unwrap();

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
        let ava2: &GenericByteArray<T> =
            av2.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava2.value(0), values[0]);
        assert_eq!(ava2.value(1), values[1]);
        assert_eq!(ava2.value(2), values[2]);
    }

    #[test]
    fn test_string_dictionary_builder_finish_cloned() {
        test_bytes_dictionary_builder_finish_cloned::<GenericStringType<i32>>(vec![
            "abc", "def", "ghi",
        ]);
    }

    #[test]
    fn test_binary_dictionary_builder_finish_cloned() {
        test_bytes_dictionary_builder_finish_cloned::<GenericBinaryType<i32>>(vec![
            b"abc", b"def", b"ghi",
        ]);
    }

    fn test_bytes_dictionary_builder_with_existing_dictionary<T>(
        dictionary: GenericByteArray<T>,
        values: Vec<&T::Native>,
    ) where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder =
            GenericByteDictionaryBuilder::<Int8Type, T>::new_with_dictionary(6, &dictionary)
                .unwrap();
        builder.append(values[0]).unwrap();
        builder.append_null();
        builder.append(values[1]).unwrap();
        builder.append(values[1]).unwrap();
        builder.append(values[0]).unwrap();
        builder.append(values[2]).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(2), None, Some(1), Some(1), Some(2), Some(3)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> = av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert!(!ava.is_valid(0));
        assert_eq!(ava.value(1), values[1]);
        assert_eq!(ava.value(2), values[0]);
        assert_eq!(ava.value(3), values[2]);
    }

    #[test]
    fn test_string_dictionary_builder_with_existing_dictionary() {
        test_bytes_dictionary_builder_with_existing_dictionary::<GenericStringType<i32>>(
            StringArray::from(vec![None, Some("def"), Some("abc")]),
            vec!["abc", "def", "ghi"],
        );
    }

    #[test]
    fn test_binary_dictionary_builder_with_existing_dictionary() {
        let values: Vec<Option<&[u8]>> = vec![None, Some(b"def"), Some(b"abc")];
        test_bytes_dictionary_builder_with_existing_dictionary::<GenericBinaryType<i32>>(
            BinaryArray::from(values),
            vec![b"abc", b"def", b"ghi"],
        );
    }

    fn test_bytes_dictionary_builder_with_reserved_null_value<T>(
        dictionary: GenericByteArray<T>,
        values: Vec<&T::Native>,
    ) where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder =
            GenericByteDictionaryBuilder::<Int16Type, T>::new_with_dictionary(4, &dictionary)
                .unwrap();
        builder.append(values[0]).unwrap();
        builder.append_null();
        builder.append(values[1]).unwrap();
        builder.append(values[0]).unwrap();
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

    #[test]
    fn test_string_dictionary_builder_with_reserved_null_value() {
        let v: Vec<Option<&str>> = vec![None];
        test_bytes_dictionary_builder_with_reserved_null_value::<GenericStringType<i32>>(
            StringArray::from(v),
            vec!["abc", "def"],
        );
    }

    #[test]
    fn test_binary_dictionary_builder_with_reserved_null_value() {
        let values: Vec<Option<&[u8]>> = vec![None];
        test_bytes_dictionary_builder_with_reserved_null_value::<GenericBinaryType<i32>>(
            BinaryArray::from(values),
            vec![b"abc", b"def"],
        );
    }

    #[test]
    fn test_extend() {
        let mut builder = GenericByteDictionaryBuilder::<Int32Type, Utf8Type>::new();
        builder.extend(["a", "b", "c", "a", "b", "c"].into_iter().map(Some));
        builder.extend(["c", "d", "a"].into_iter().map(Some));
        let dict = builder.finish();
        assert_eq!(dict.keys().values(), &[0, 1, 2, 0, 1, 2, 2, 3, 0]);
        assert_eq!(dict.values().len(), 4);
    }
}
