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
use crate::{
    Array, ArrayRef, ArrowPrimitiveType, DictionaryArray, PrimitiveArray, TypedDictionaryArray,
};
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use arrow_schema::{ArrowError, DataType};
use num_traits::NumCast;
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

/// Builder for [`DictionaryArray`] of [`PrimitiveArray`]
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
        let values_capacity = values_builder.capacity();
        Self {
            keys_builder,
            values_builder,
            map: HashMap::with_capacity(values_capacity),
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

    /// Creates a new `PrimitiveDictionaryBuilder` from the existing builder with the same
    /// keys and values, but with a new data type for the keys.
    ///
    /// # Example
    /// ```
    /// #
    /// # use arrow_array::builder::PrimitiveDictionaryBuilder;
    /// # use arrow_array::types::{UInt8Type, UInt16Type, UInt64Type};
    /// # use arrow_array::UInt16Array;
    /// # use arrow_schema::ArrowError;
    ///
    /// let mut u8_keyed_builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt64Type>::new();
    ///
    /// // appending too many values causes the dictionary to overflow
    /// for i in 0..256 {
    ///     u8_keyed_builder.append_value(i);
    /// }
    /// let result = u8_keyed_builder.append(256);
    /// assert!(matches!(result, Err(ArrowError::DictionaryKeyOverflowError{})));
    ///
    /// // we need to upgrade to a larger key type
    /// let mut u16_keyed_builder = PrimitiveDictionaryBuilder::<UInt16Type, UInt64Type>::try_new_from_builder(u8_keyed_builder).unwrap();
    /// let dictionary_array = u16_keyed_builder.finish();
    /// let keys = dictionary_array.keys();
    ///
    /// assert_eq!(keys, &UInt16Array::from_iter(0..256));
    pub fn try_new_from_builder<K2>(
        mut source: PrimitiveDictionaryBuilder<K2, V>,
    ) -> Result<Self, ArrowError>
    where
        K::Native: NumCast,
        K2: ArrowDictionaryKeyType,
        K2::Native: NumCast,
    {
        let map = source.map;
        let values_builder = source.values_builder;

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
            map,
            keys_builder: new_keys
                .into_builder()
                .expect("underlying buffer has no references"),
            values_builder,
        })
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

    /// Extends builder with dictionary
    ///
    /// This is the same as [`Self::extend`] but is faster as it translates
    /// the dictionary values once rather than doing a lookup for each item in the iterator
    ///
    /// when dictionary values are null (the actual mapped values) the keys are null
    ///
    pub fn extend_dictionary(
        &mut self,
        dictionary: &TypedDictionaryArray<K, PrimitiveArray<V>>,
    ) -> Result<(), ArrowError> {
        let values = dictionary.values();

        let v_len = values.len();
        let k_len = dictionary.keys().len();
        if v_len == 0 && k_len == 0 {
            return Ok(());
        }

        // All nulls
        if v_len == 0 {
            self.append_nulls(k_len);
            return Ok(());
        }

        if k_len == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Dictionary keys should not be empty when values are not empty".to_string(),
            ));
        }

        // Orphan values will be carried over to the new dictionary
        let mapped_values = values
            .iter()
            // Dictionary values can technically be null, so we need to handle that
            .map(|dict_value| {
                dict_value
                    .map(|dict_value| self.get_or_insert_key(dict_value))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Just insert the keys without additional lookups
        dictionary.keys().iter().for_each(|key| match key {
            None => self.append_null(),
            Some(original_dict_index) => {
                let index = original_dict_index.as_usize().min(v_len - 1);
                match mapped_values[index] {
                    None => self.append_null(),
                    Some(mapped_value) => self.keys_builder.append_value(mapped_value),
                }
            }
        });

        Ok(())
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

    use crate::array::{Int32Array, UInt8Array, UInt32Array};
    use crate::builder::Decimal128Builder;
    use crate::cast::AsArray;
    use crate::types::{
        Date32Type, Decimal128Type, DurationNanosecondType, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, TimestampNanosecondType, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    };

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

    #[test]
    fn test_extend_dictionary() {
        let some_dict = {
            let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
            builder.extend([1, 2, 3, 1, 2, 3, 1, 2, 3].into_iter().map(Some));
            builder.extend([None::<i32>]);
            builder.extend([4, 5, 1, 3, 1].into_iter().map(Some));
            builder.append_null();
            builder.finish()
        };

        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        builder.extend([6, 6, 7, 6, 5].into_iter().map(Some));
        builder
            .extend_dictionary(&some_dict.downcast_dict().unwrap())
            .unwrap();
        let dict = builder.finish();

        assert_eq!(dict.values().len(), 7);

        let values = dict
            .downcast_dict::<Int32Array>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(
            values,
            [
                Some(6),
                Some(6),
                Some(7),
                Some(6),
                Some(5),
                Some(1),
                Some(2),
                Some(3),
                Some(1),
                Some(2),
                Some(3),
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(4),
                Some(5),
                Some(1),
                Some(3),
                Some(1),
                None
            ]
        );
    }

    #[test]
    fn test_extend_dictionary_with_null_in_mapped_value() {
        let some_dict = {
            let mut values_builder = PrimitiveBuilder::<Int32Type>::new();
            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();

            // Manually build a dictionary values that the mapped values have null
            values_builder.append_null();
            keys_builder.append_value(0);
            values_builder.append_value(42);
            keys_builder.append_value(1);

            let values = values_builder.finish();
            let keys = keys_builder.finish();

            let data_type = DataType::Dictionary(
                Box::new(Int32Type::DATA_TYPE),
                Box::new(values.data_type().clone()),
            );

            let builder = keys
                .into_data()
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values.into_data()]);

            DictionaryArray::from(unsafe { builder.build_unchecked() })
        };

        let some_dict_values = some_dict.values().as_primitive::<Int32Type>();
        assert_eq!(
            some_dict_values.into_iter().collect::<Vec<_>>(),
            &[None, Some(42)]
        );

        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        builder
            .extend_dictionary(&some_dict.downcast_dict().unwrap())
            .unwrap();
        let dict = builder.finish();

        assert_eq!(dict.values().len(), 1);

        let values = dict
            .downcast_dict::<Int32Array>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(values, [None, Some(42)]);
    }

    #[test]
    fn test_extend_all_null_dictionary() {
        let some_dict = {
            let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
            builder.append_nulls(2);
            builder.finish()
        };

        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        builder
            .extend_dictionary(&some_dict.downcast_dict().unwrap())
            .unwrap();
        let dict = builder.finish();

        assert_eq!(dict.values().len(), 0);

        let values = dict
            .downcast_dict::<Int32Array>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(values, [None, None]);
    }

    #[test]
    fn creating_dictionary_from_builders_should_use_values_capacity_for_the_map() {
        let builder = PrimitiveDictionaryBuilder::<Int32Type, crate::types::TimestampMicrosecondType>::new_from_empty_builders(
                  PrimitiveBuilder::with_capacity(1).with_data_type(DataType::Int32),
                  PrimitiveBuilder::with_capacity(2).with_data_type(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+08:00".into()))),
              );

        assert!(
            builder.map.capacity() >= builder.values_builder.capacity(),
            "map capacity {} should be at least the values capacity {}",
            builder.map.capacity(),
            builder.values_builder.capacity()
        )
    }

    fn _test_try_new_from_builder_generic_for_key_types<K1, K2, V>(values: Vec<V::Native>)
    where
        K1: ArrowDictionaryKeyType,
        K1::Native: NumCast,
        K2: ArrowDictionaryKeyType,
        K2::Native: NumCast + From<u8>,
        V: ArrowPrimitiveType,
    {
        let mut source = PrimitiveDictionaryBuilder::<K1, V>::new();
        source.append(values[0]).unwrap();
        source.append_null();
        source.append(values[1]).unwrap();
        source.append(values[2]).unwrap();

        let mut result = PrimitiveDictionaryBuilder::<K2, V>::try_new_from_builder(source).unwrap();
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
        let ava = av.as_any().downcast_ref::<PrimitiveArray<V>>().unwrap();
        assert_eq!(ava.value(0), values[0]);
        assert_eq!(ava.value(1), values[1]);
        assert_eq!(ava.value(2), values[2]);
    }

    fn _test_try_new_from_builder_generic_for_value<T>(values: Vec<T::Native>)
    where
        T: ArrowPrimitiveType,
    {
        // test cast to bigger size unsigned
        _test_try_new_from_builder_generic_for_key_types::<UInt8Type, UInt16Type, T>(
            values.clone(),
        );
        // test cast going to smaller size unsigned
        _test_try_new_from_builder_generic_for_key_types::<UInt16Type, UInt8Type, T>(
            values.clone(),
        );
        // test cast going to bigger size signed
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, Int16Type, T>(values.clone());
        // test cast going to smaller size signed
        _test_try_new_from_builder_generic_for_key_types::<Int32Type, Int16Type, T>(values.clone());
        // test going from signed to signed for different size changes
        _test_try_new_from_builder_generic_for_key_types::<UInt8Type, Int16Type, T>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, UInt8Type, T>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int8Type, UInt16Type, T>(values.clone());
        _test_try_new_from_builder_generic_for_key_types::<Int32Type, Int16Type, T>(values.clone());
    }

    #[test]
    fn test_try_new_from_builder() {
        // test unsigned types
        _test_try_new_from_builder_generic_for_value::<UInt8Type>(vec![1, 2, 3]);
        _test_try_new_from_builder_generic_for_value::<UInt16Type>(vec![1, 2, 3]);
        _test_try_new_from_builder_generic_for_value::<UInt32Type>(vec![1, 2, 3]);
        _test_try_new_from_builder_generic_for_value::<UInt64Type>(vec![1, 2, 3]);
        // test signed types
        _test_try_new_from_builder_generic_for_value::<Int8Type>(vec![-1, 0, 1]);
        _test_try_new_from_builder_generic_for_value::<Int16Type>(vec![-1, 0, 1]);
        _test_try_new_from_builder_generic_for_value::<Int32Type>(vec![-1, 0, 1]);
        _test_try_new_from_builder_generic_for_value::<Int64Type>(vec![-1, 0, 1]);
        // test some date types
        _test_try_new_from_builder_generic_for_value::<Date32Type>(vec![5, 6, 7]);
        _test_try_new_from_builder_generic_for_value::<DurationNanosecondType>(vec![1, 2, 3]);
        _test_try_new_from_builder_generic_for_value::<TimestampNanosecondType>(vec![1, 2, 3]);
        // test some floating point types
        _test_try_new_from_builder_generic_for_value::<Float32Type>(vec![0.1, 0.2, 0.3]);
        _test_try_new_from_builder_generic_for_value::<Float64Type>(vec![-0.1, 0.2, 0.3]);
    }

    #[test]
    fn test_try_new_from_builder_cast_fails() {
        let mut source_builder = PrimitiveDictionaryBuilder::<UInt16Type, UInt64Type>::new();
        for i in 0..257 {
            source_builder.append_value(i);
        }

        // there should be too many values that we can't downcast to the underlying type
        // we have keys that wouldn't fit into UInt8Type
        let result = PrimitiveDictionaryBuilder::<UInt8Type, UInt64Type>::try_new_from_builder(
            source_builder,
        );
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
        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::new();
        builder.append(10).unwrap();
        builder.append(20).unwrap();
        let array = builder.finish_preserve_values();
        assert_eq!(array.keys(), &UInt8Array::from(vec![Some(0), Some(1)]));
        let values: &[u32] = array
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values();
        assert_eq!(values, &[10, 20]);

        // Create a new dictionary
        builder.append(30).unwrap();
        builder.append(40).unwrap();
        let array2 = builder.finish_preserve_values();

        // Make sure the keys are assigned after the old ones
        // and that we have the right values
        assert_eq!(array2.keys(), &UInt8Array::from(vec![Some(2), Some(3)]));
        let values = array2
            .downcast_dict::<UInt32Array>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(30), Some(40)]);

        // Check that we have all of the expected values
        let all_values: &[u32] = array2
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values();
        assert_eq!(all_values, &[10, 20, 30, 40]);
    }
}
