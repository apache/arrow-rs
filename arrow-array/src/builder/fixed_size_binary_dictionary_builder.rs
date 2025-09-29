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

use crate::builder::{ArrayBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder};
use crate::types::ArrowDictionaryKeyType;
use crate::{Array, ArrayRef, DictionaryArray, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType::FixedSizeBinary;
use arrow_schema::{ArrowError, DataType};
use hashbrown::HashTable;
use num_traits::NumCast;
use std::any::Any;
use std::sync::Arc;

/// Builder for [`DictionaryArray`] of [`FixedSizeBinaryArray`]
///
/// The output array has a dictionary of unique, fixed-size binary values. The
/// builder handles deduplication.
///
/// # Example
/// ```
/// # use arrow_array::builder::{FixedSizeBinaryDictionaryBuilder};
/// # use arrow_array::array::{Array, FixedSizeBinaryArray};
/// # use arrow_array::DictionaryArray;
/// # use arrow_array::types::Int8Type;
/// // Build 3 byte FixedBinaryArrays
/// let byte_width = 3;
/// let mut builder = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
/// builder.append("abc").unwrap();
/// builder.append_null();
/// builder.append(b"def").unwrap();
/// builder.append(b"def").unwrap(); // duplicate value
/// // Result is a Dictionary Array
/// let array = builder.finish();
/// let dict_array = array.as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
/// // The array represents "abc", null, "def", "def"
/// assert_eq!(array.keys().len(), 4);
/// // but there are only 2 unique values
/// assert_eq!(array.values().len(), 2);
/// let values = dict_array.values().as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
/// assert_eq!(values.value(0), "abc".as_bytes());
/// assert_eq!(values.value(1), "def".as_bytes());
/// ```
///
/// [`FixedSizeBinaryArray`]: crate::FixedSizeBinaryArray
#[derive(Debug)]
pub struct FixedSizeBinaryDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    state: ahash::RandomState,
    dedup: HashTable<usize>,

    keys_builder: PrimitiveBuilder<K>,
    values_builder: FixedSizeBinaryBuilder,
    byte_width: i32,
}

impl<K> FixedSizeBinaryDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Creates a new `FixedSizeBinaryDictionaryBuilder`
    pub fn new(byte_width: i32) -> Self {
        let keys_builder = PrimitiveBuilder::new();
        let values_builder = FixedSizeBinaryBuilder::new(byte_width);
        Self {
            state: Default::default(),
            dedup: HashTable::with_capacity(keys_builder.capacity()),
            keys_builder,
            values_builder,
            byte_width,
        }
    }

    /// Creates a new `FixedSizeBinaryDictionaryBuilder` with the provided capacities
    ///
    /// `keys_capacity`: the number of keys, i.e. length of array to build
    /// `value_capacity`: the number of distinct dictionary values, i.e. size of dictionary
    /// `byte_width`: the byte width for individual values in the values array
    pub fn with_capacity(keys_capacity: usize, value_capacity: usize, byte_width: i32) -> Self {
        Self {
            state: Default::default(),
            dedup: Default::default(),
            keys_builder: PrimitiveBuilder::with_capacity(keys_capacity),
            values_builder: FixedSizeBinaryBuilder::with_capacity(value_capacity, byte_width),
            byte_width,
        }
    }

    /// Creates a new `FixedSizeBinaryDictionaryBuilder` from the existing builder with the same
    /// keys and values, but with a new data type for the keys.
    ///
    /// # Example
    /// ```
    /// # use arrow_array::builder::FixedSizeBinaryDictionaryBuilder;
    /// # use arrow_array::types::{UInt8Type, UInt16Type, UInt64Type};
    /// # use arrow_array::UInt16Array;
    /// # use arrow_schema::ArrowError;
    ///
    /// let mut u8_keyed_builder = FixedSizeBinaryDictionaryBuilder::<UInt8Type>::new(2);
    /// // appending too many values causes the dictionary to overflow
    /// for i in 0..=255 {
    ///     u8_keyed_builder.append_value(vec![0, i]);
    /// }
    /// let result = u8_keyed_builder.append(vec![1, 0]);
    /// assert!(matches!(result, Err(ArrowError::DictionaryKeyOverflowError{})));
    ///
    /// // we need to upgrade to a larger key type
    /// let mut u16_keyed_builder = FixedSizeBinaryDictionaryBuilder::<UInt16Type>::try_new_from_builder(u8_keyed_builder).unwrap();
    /// let dictionary_array = u16_keyed_builder.finish();
    /// let keys = dictionary_array.keys();
    ///
    /// assert_eq!(keys, &UInt16Array::from_iter(0..256));
    /// ```
    pub fn try_new_from_builder<K2>(
        mut source: FixedSizeBinaryDictionaryBuilder<K2>,
    ) -> Result<Self, ArrowError>
    where
        K::Native: NumCast,
        K2: ArrowDictionaryKeyType,
        K2::Native: NumCast,
    {
        let state = source.state;
        let dedup = source.dedup;
        let values_builder = source.values_builder;
        let byte_width = source.byte_width;

        let source_keys = source.keys_builder.finish();
        let new_keys: PrimitiveArray<K> = source_keys.try_unary(|value| {
            num_traits::cast::cast::<K2::Native, K::Native>(value).ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Can't cast dictionary keys from source type {:?} to type {:?}",
                    K2::DATA_TYPE,
                    K::DATA_TYPE
                ))
            })
        })?;

        // drop source key here because currently source_keys and new_keys are holding reference to
        // the same underlying null_buffer. Below we want to call new_keys.into_builder() it must
        // be the only reference holder.
        drop(source_keys);

        Ok(Self {
            state,
            dedup,
            keys_builder: new_keys
                .into_builder()
                .expect("underlying buffer has no references"),
            values_builder,
            byte_width,
        })
    }
}

impl<K> ArrayBuilder for FixedSizeBinaryDictionaryBuilder<K>
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

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

impl<K> FixedSizeBinaryDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    fn get_or_insert_key(&mut self, value: impl AsRef<[u8]>) -> Result<K::Native, ArrowError> {
        let value_bytes: &[u8] = value.as_ref();

        let state = &self.state;
        let storage = &mut self.values_builder;
        let hash = state.hash_one(value_bytes);

        let idx = *self
            .dedup
            .entry(
                hash,
                |idx| value_bytes == get_bytes(storage, self.byte_width, *idx),
                |idx| state.hash_one(get_bytes(storage, self.byte_width, *idx)),
            )
            .or_insert_with(|| {
                let idx = storage.len();
                let _ = storage.append_value(value);
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
    pub fn append(&mut self, value: impl AsRef<[u8]>) -> Result<K::Native, ArrowError> {
        if self.byte_width != value.as_ref().len() as i32 {
            Err(ArrowError::InvalidArgumentError(format!(
                "Invalid input length passed to FixedSizeBinaryBuilder. Expected {} got {}",
                self.byte_width,
                value.as_ref().len()
            )))
        } else {
            let key = self.get_or_insert_key(value)?;
            self.keys_builder.append_value(key);
            Ok(key)
        }
    }

    /// Append a value multiple times to the array.
    /// This is the same as [`Self::append`] but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// Returns an error if the new index would overflow the key type.
    pub fn append_n(
        &mut self,
        value: impl AsRef<[u8]>,
        count: usize,
    ) -> Result<K::Native, ArrowError> {
        if self.byte_width != value.as_ref().len() as i32 {
            Err(ArrowError::InvalidArgumentError(format!(
                "Invalid input length passed to FixedSizeBinaryBuilder. Expected {} got {}",
                self.byte_width,
                value.as_ref().len()
            )))
        } else {
            let key = self.get_or_insert_key(value)?;
            self.keys_builder.append_value_n(key, count);
            Ok(key)
        }
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.keys_builder.append_null()
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.keys_builder.append_nulls(n);
    }

    /// Infallibly append a value to this builder
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.append(value).expect("dictionary key overflow");
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.dedup.clear();
        let values = self.values_builder.finish();
        let keys = self.keys_builder.finish();

        let data_type = DataType::Dictionary(
            Box::new(K::DATA_TYPE),
            Box::new(FixedSizeBinary(self.byte_width)),
        );

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

        let data_type = DataType::Dictionary(
            Box::new(K::DATA_TYPE),
            Box::new(FixedSizeBinary(self.byte_width)),
        );

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }

    /// Builds the `DictionaryArray` without resetting the values builder or
    /// the internal de-duplication map.
    ///
    /// The advantage of doing this is that the values will represent the entire
    /// set of what has been built so-far by this builder and ensures
    /// consistency in the assignment of keys to values across multiple calls
    /// to `finish_preserve_values`. This enables ipc writers to efficiently
    /// emit delta dictionaries.
    ///
    /// The downside to this is that building the record requires creating a
    /// copy of the values, which can become slowly more expensive if the
    /// dictionary grows.
    ///
    /// Additionally, if record batches from multiple different dictionary
    /// builders for the same column are fed into a single ipc writer, beware
    /// that entire dictionaries are likely to be re-sent frequently even when
    /// the majority of the values are not used by the current record batch.
    pub fn finish_preserve_values(&mut self) -> DictionaryArray<K> {
        let values = self.values_builder.finish_cloned();
        let keys = self.keys_builder.finish();

        let data_type = DataType::Dictionary(
            Box::new(K::DATA_TYPE),
            Box::new(FixedSizeBinary(self.byte_width)),
        );

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }
}

fn get_bytes(values: &FixedSizeBinaryBuilder, byte_width: i32, idx: usize) -> &[u8] {
    let values = values.values_slice();
    let start = idx * byte_width.as_usize();
    let end = idx * byte_width.as_usize() + byte_width.as_usize();
    &values[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::{Int8Type, Int16Type, Int32Type, UInt8Type, UInt16Type};
    use crate::{ArrowPrimitiveType, FixedSizeBinaryArray, Int8Array};

    #[test]
    fn test_fixed_size_dictionary_builder() {
        let values = ["abc", "def"];

        let mut b = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
        assert_eq!(b.append(values[0]).unwrap(), 0);
        b.append_null();
        assert_eq!(b.append(values[1]).unwrap(), 1);
        assert_eq!(b.append(values[1]).unwrap(), 1);
        assert_eq!(b.append(values[0]).unwrap(), 0);
        b.append_nulls(2);
        assert_eq!(b.append(values[0]).unwrap(), 0);
        let array = b.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(1),
                Some(0),
                None,
                None,
                Some(0)
            ]),
        );

        // Values are polymorphic and so require a downcast.
        let ava = array
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(ava.value(0), values[0].as_bytes());
        assert_eq!(ava.value(1), values[1].as_bytes());
    }

    #[test]
    fn test_fixed_size_dictionary_builder_append_n() {
        let values = ["abc", "def"];
        let mut b = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
        assert_eq!(b.append_n(values[0], 2).unwrap(), 0);
        assert_eq!(b.append_n(values[1], 3).unwrap(), 1);
        assert_eq!(b.append_n(values[0], 2).unwrap(), 0);
        let array = b.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![
                Some(0),
                Some(0),
                Some(1),
                Some(1),
                Some(1),
                Some(0),
                Some(0),
            ]),
        );

        // Values are polymorphic and so require a downcast.
        let ava = array
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(ava.value(0), values[0].as_bytes());
        assert_eq!(ava.value(1), values[1].as_bytes());
    }

    #[test]
    fn test_fixed_size_dictionary_builder_wrong_size() {
        let mut b = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
        let err = b.append(b"too long").unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Invalid input length passed to FixedSizeBinaryBuilder. Expected 3 got 8"
        );
        let err = b.append("").unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Invalid input length passed to FixedSizeBinaryBuilder. Expected 3 got 0"
        );
        let err = b.append_n("a", 3).unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Invalid input length passed to FixedSizeBinaryBuilder. Expected 3 got 1"
        );
    }

    #[test]
    fn test_fixed_size_dictionary_builder_finish_cloned() {
        let values = ["abc", "def", "ghi"];

        let mut builder = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);

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
        let ava = array
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(ava.value(0), values[0].as_bytes());
        assert_eq!(ava.value(1), values[1].as_bytes());

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
        let ava2 = array
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(ava2.value(0), values[0].as_bytes());
        assert_eq!(ava2.value(1), values[1].as_bytes());
        assert_eq!(ava2.value(2), values[2].as_bytes());
    }

    fn _test_try_new_from_builder_generic_for_key_types<K1, K2>(values: Vec<[u8; 3]>)
    where
        K1: ArrowDictionaryKeyType,
        K1::Native: NumCast,
        K2: ArrowDictionaryKeyType,
        K2::Native: NumCast + From<u8>,
    {
        let mut source = FixedSizeBinaryDictionaryBuilder::<K1>::new(3);
        source.append_value(values[0]);
        source.append_null();
        source.append_value(values[1]);
        source.append_value(values[2]);

        let mut result =
            FixedSizeBinaryDictionaryBuilder::<K2>::try_new_from_builder(source).unwrap();
        let array = result.finish();

        let mut expected_keys_builder = PrimitiveBuilder::<K2>::new();
        expected_keys_builder
            .append_value(<<K2 as ArrowPrimitiveType>::Native as From<u8>>::from(0u8));
        expected_keys_builder.append_null();
        expected_keys_builder
            .append_value(<<K2 as ArrowPrimitiveType>::Native as From<u8>>::from(1u8));
        expected_keys_builder
            .append_value(<<K2 as ArrowPrimitiveType>::Native as From<u8>>::from(2u8));
        let expected_keys = expected_keys_builder.finish();
        assert_eq!(array.keys(), &expected_keys);

        let av = array.values();
        let ava = av.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(ava.value(0), values[0]);
        assert_eq!(ava.value(1), values[1]);
        assert_eq!(ava.value(2), values[2]);
    }

    #[test]
    fn test_try_new_from_builder() {
        let values = vec![[1, 2, 3], [5, 6, 7], [6, 7, 8]];
        // test cast to bigger size unsigned
        _test_try_new_from_builder_generic_for_key_types::<UInt8Type, UInt16Type>(values.clone());
        // test cast going to smaller size unsigned
        _test_try_new_from_builder_generic_for_key_types::<UInt16Type, UInt8Type>(values.clone());
        // test cast going to bigger size signed
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, Int16Type>(values.clone());
        // test cast going to smaller size signed
        _test_try_new_from_builder_generic_for_key_types::<Int32Type, Int16Type>(values.clone());
        // test going from signed to signed for different size changes
        _test_try_new_from_builder_generic_for_key_types::<UInt8Type, Int16Type>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, UInt8Type>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, UInt16Type>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int32Type, Int16Type>(values.clone());
    }

    #[test]
    fn test_try_new_from_builder_cast_fails() {
        let mut source_builder = FixedSizeBinaryDictionaryBuilder::<UInt16Type>::new(2);
        for i in 0u16..257u16 {
            source_builder.append_value(vec![(i >> 8) as u8, i as u8]);
        }

        // there should be too many values that we can't downcast to the underlying type
        // we have keys that wouldn't fit into UInt8Type
        let result =
            FixedSizeBinaryDictionaryBuilder::<UInt8Type>::try_new_from_builder(source_builder);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, ArrowError::CastError(_)));
            assert_eq!(
                e.to_string(),
                "Cast error: Can't cast dictionary keys from source type UInt16 to type UInt8"
            );
        }
    }

    #[test]
    fn test_finish_preserve_values() {
        // Create the first dictionary
        let mut builder = FixedSizeBinaryDictionaryBuilder::<Int32Type>::new(3);
        builder.append_value("aaa");
        builder.append_value("bbb");
        builder.append_value("ccc");
        let dict = builder.finish_preserve_values();
        assert_eq!(dict.keys().values(), &[0, 1, 2]);
        let values = dict
            .downcast_dict::<FixedSizeBinaryArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                Some("aaa".as_bytes()),
                Some("bbb".as_bytes()),
                Some("ccc".as_bytes())
            ]
        );

        // Create a new dictionary
        builder.append_value("ddd");
        builder.append_value("eee");
        let dict2 = builder.finish_preserve_values();

        // Make sure the keys are assigned after the old ones and we have the
        // right values
        assert_eq!(dict2.keys().values(), &[3, 4]);
        let values = dict2
            .downcast_dict::<FixedSizeBinaryArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(values, [Some("ddd".as_bytes()), Some("eee".as_bytes())]);

        // Check that we have all of the expected values
        let all_values = dict2
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(
            all_values,
            [
                Some("aaa".as_bytes()),
                Some("bbb".as_bytes()),
                Some("ccc".as_bytes()),
                Some("ddd".as_bytes()),
                Some("eee".as_bytes())
            ]
        );
    }
}
