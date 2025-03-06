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
use crate::{Array, ArrayRef, DictionaryArray};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType::FixedSizeBinary;
use arrow_schema::{ArrowError, DataType};
use hashbrown::HashTable;
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

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.keys_builder.append_null()
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

    use crate::types::Int8Type;
    use crate::{FixedSizeBinaryArray, Int8Array};

    #[test]
    fn test_fixed_size_dictionary_builder() {
        let values = ["abc", "def"];

        let mut b = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
        assert_eq!(b.append(values[0]).unwrap(), 0);
        b.append_null();
        assert_eq!(b.append(values[1]).unwrap(), 1);
        assert_eq!(b.append(values[1]).unwrap(), 1);
        assert_eq!(b.append(values[0]).unwrap(), 0);
        let array = b.finish();

        assert_eq!(
            array.keys(),
            &Int8Array::from(vec![Some(0), None, Some(1), Some(1), Some(0)]),
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
        assert_eq!(err, "Invalid argument error: Invalid input length passed to FixedSizeBinaryBuilder. Expected 3 got 8");
        let err = b.append("").unwrap_err().to_string();
        assert_eq!(err, "Invalid argument error: Invalid input length passed to FixedSizeBinaryBuilder. Expected 3 got 0");
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
}
