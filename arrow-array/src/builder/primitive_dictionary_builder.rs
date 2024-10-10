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

use crate::builder::{ArrayBuilder, PrimitiveBuilder};
use crate::types::ArrowDictionaryKeyType;
use crate::{Array, ArrayRef, ArrowPrimitiveType, DictionaryArray};
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Wraps a type implementing `ToByteSlice` implementing `Hash` and `Eq` for it
///
/// This is necessary to handle types such as f32, which don't natively implement these
#[derive(Debug)]
struct Value<T>(T);

impl<T: ToByteSlice> std::hash::Hash for Value<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_byte_slice().hash(state)
    }
}

impl<T: ToByteSlice> PartialEq for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_byte_slice().eq(other.0.to_byte_slice())
    }
}

impl<T: ToByteSlice> Eq for Value<T> {}

/// Builder for [`DictionaryArray`] of [`PrimitiveArray`](crate::array::PrimitiveArray)
///
/// # Example:
///
/// ```
///
/// # use arrow_array::builder::PrimitiveDictionaryBuilder;
/// # use arrow_array::types::{UInt32Type, UInt8Type};
/// # use arrow_array::{Array, UInt32Array, UInt8Array};
///
/// let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::new();
///  builder.append(12345678).unwrap();
///  builder.append_null();
///  builder.append(22345678).unwrap();
///  let array = builder.finish();
///
///  assert_eq!(
///      array.keys(),
///      &UInt8Array::from(vec![Some(0), None, Some(1)])
///  );
///
///  // Values are polymorphic and so require a downcast.
///  let av = array.values();
///  let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
///  let avs: &[u32] = ava.values();
///
///  assert!(!array.is_null(0));
///  assert!(array.is_null(1));
///  assert!(!array.is_null(2));
///
///  assert_eq!(avs, &[12345678, 22345678]);
/// ```
#[derive(Debug)]
pub struct PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    keys_builder: PrimitiveBuilder<K>,
    values_builder: PrimitiveBuilder<V>,
    map: HashMap<Value<V::Native>, usize>,
}

impl<K, V> Default for PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveDictionaryBuilder`.
    pub fn new() -> Self {
        Self {
            keys_builder: PrimitiveBuilder::new(),
            values_builder: PrimitiveBuilder::new(),
            map: HashMap::new(),
        }
    }

    /// Creates a new `PrimitiveDictionaryBuilder` from the provided keys and values builders.
    ///
    /// # Panics
    ///
    /// This method panics if `keys_builder` or `values_builder` is not empty.
    pub fn new_from_empty_builders(
        keys_builder: PrimitiveBuilder<K>,
        values_builder: PrimitiveBuilder<V>,
    ) -> Self {
        assert!(
            keys_builder.is_empty() && values_builder.is_empty(),
            "keys and values builders must be empty"
        );
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }

    /// Creates a new `PrimitiveDictionaryBuilder` from existing `PrimitiveBuilder`s of keys and values.
    ///
    /// # Safety
    ///
    /// caller must ensure that the passed in builders are valid for DictionaryArray.
    pub unsafe fn new_from_builders(
        keys_builder: PrimitiveBuilder<K>,
        values_builder: PrimitiveBuilder<V>,
    ) -> Self {
        let keys = keys_builder.values_slice();
        let values = values_builder.values_slice();
        let mut map = HashMap::with_capacity(values.len());

        keys.iter().zip(values.iter()).for_each(|(key, value)| {
            map.insert(Value(*value), K::Native::to_usize(*key).unwrap());
        });

        Self {
            keys_builder,
            values_builder,
            map,
        }
    }

    /// Creates a new `PrimitiveDictionaryBuilder` with the provided capacities
    ///
    /// `keys_capacity`: the number of keys, i.e. length of array to build
    /// `values_capacity`: the number of distinct dictionary values, i.e. size of dictionary
    pub fn with_capacity(keys_capacity: usize, values_capacity: usize) -> Self {
        Self {
            keys_builder: PrimitiveBuilder::with_capacity(keys_capacity),
            values_builder: PrimitiveBuilder::with_capacity(values_capacity),
            map: HashMap::with_capacity(values_capacity),
        }
    }
}

impl<K, V> ArrayBuilder for PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
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

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
{
    #[inline]
    fn get_or_insert_key(&mut self, value: V::Native) -> Result<K::Native, ArrowError> {
        match self.map.get(&Value(value)) {
            Some(&key) => {
                Ok(K::Native::from_usize(key).ok_or(ArrowError::DictionaryKeyOverflowError)?)
            }
            None => {
                let key = self.values_builder.len();
                self.values_builder.append_value(value);
                self.map.insert(Value(value), key);
                Ok(K::Native::from_usize(key).ok_or(ArrowError::DictionaryKeyOverflowError)?)
            }
        }
    }

    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    #[inline]
    pub fn append(&mut self, value: V::Native) -> Result<K::Native, ArrowError> {
        let key = self.get_or_insert_key(value)?;
        self.keys_builder.append_value(key);
        Ok(key)
    }

    /// Append a value multiple times to the array.
    /// This is the same as `append` but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// Returns an error if the new index would overflow the key type.
    pub fn append_n(&mut self, value: V::Native, count: usize) -> Result<K::Native, ArrowError> {
        let key = self.get_or_insert_key(value)?;
        self.keys_builder.append_value_n(key, count);
        Ok(key)
    }

    /// Infallibly append a value to this builder
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    #[inline]
    pub fn append_value(&mut self, value: V::Native) {
        self.append(value).expect("dictionary key overflow");
    }

    /// Infallibly append a value to this builder repeatedly `count` times.
    /// This is the same as `append_value` but allows to append the same value multiple times without doing multiple lookups.
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of the dictionary values array would exceed `T::Native::MAX`
    pub fn append_values(&mut self, value: V::Native, count: usize) {
        self.append_n(value, count)
            .expect("dictionary key overflow");
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.keys_builder.append_null()
    }

    /// Append `n` null slots into the builder
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
    pub fn append_option(&mut self, value: Option<V::Native>) {
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
    pub fn append_options(&mut self, value: Option<V::Native>, count: usize) {
        match value {
            None => self.keys_builder.append_nulls(count),
            Some(v) => self.append_values(v, count),
        };
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let values = self.values_builder.finish();
        let keys = self.keys_builder.finish();

        let data_type =
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(values.data_type().clone()));

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

        let data_type = DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(V::DATA_TYPE));

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }

    /// Returns the current dictionary values buffer as a slice
    pub fn values_slice(&self) -> &[V::Native] {
        self.values_builder.values_slice()
    }

    /// Returns the current dictionary values buffer as a mutable slice
    pub fn values_slice_mut(&mut self) -> &mut [V::Native] {
        self.values_builder.values_slice_mut()
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.keys_builder.validity_slice()
    }
}

impl<K: ArrowDictionaryKeyType, P: ArrowPrimitiveType> Extend<Option<P::Native>>
    for PrimitiveDictionaryBuilder<K, P>
{
    #[inline]
    fn extend<T: IntoIterator<Item = Option<P::Native>>>(&mut self, iter: T) {
        for v in iter {
            self.append_option(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::UInt32Array;
    use crate::array::UInt8Array;
    use crate::builder::Decimal128Builder;
    use crate::types::{Decimal128Type, Int32Type, UInt32Type, UInt8Type};

    #[test]
    fn test_primitive_dictionary_builder() {
        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(3, 2);
        builder.append(12345678).unwrap();
        builder.append_null();
        builder.append(22345678).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.keys(),
            &UInt8Array::from(vec![Some(0), None, Some(1)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
        let avs: &[u32] = ava.values();

        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));

        assert_eq!(avs, &[12345678, 22345678]);
    }

    #[test]
    fn test_extend() {
        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        builder.extend([1, 2, 3, 1, 2, 3, 1, 2, 3].into_iter().map(Some));
        builder.extend([4, 5, 1, 3, 1].into_iter().map(Some));
        let dict = builder.finish();
        assert_eq!(
            dict.keys().values(),
            &[0, 1, 2, 0, 1, 2, 0, 1, 2, 3, 4, 0, 2, 0]
        );
        assert_eq!(dict.values().len(), 5);
    }

    #[test]
    #[should_panic(expected = "DictionaryKeyOverflowError")]
    fn test_primitive_dictionary_overflow() {
        let mut builder =
            PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(257, 257);
        // 256 unique keys.
        for i in 0..256 {
            builder.append(i + 1000).unwrap();
        }
        // Special error if the key overflows (256th entry)
        builder.append(1257).unwrap();
    }

    #[test]
    fn test_primitive_dictionary_with_builders() {
        let keys_builder = PrimitiveBuilder::<Int32Type>::new();
        let values_builder = Decimal128Builder::new().with_data_type(DataType::Decimal128(1, 2));
        let mut builder =
            PrimitiveDictionaryBuilder::<Int32Type, Decimal128Type>::new_from_empty_builders(
                keys_builder,
                values_builder,
            );
        let dict_array = builder.finish();
        assert_eq!(dict_array.value_type(), DataType::Decimal128(1, 2));
        assert_eq!(
            dict_array.data_type(),
            &DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Decimal128(1, 2)),
            )
        );
    }
}
