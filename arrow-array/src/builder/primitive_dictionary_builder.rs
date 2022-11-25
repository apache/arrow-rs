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
use crate::{Array, ArrayRef, ArrowPrimitiveType, DictionaryArray};
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::collections::hash_map::Entry;
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

/// Array builder for `DictionaryArray`. For example to map a set of byte indices
/// to f32 values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
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
    map: HashMap<Value<V::Native>, K::Native>,
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
    K: ArrowPrimitiveType,
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

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    #[inline]
    pub fn append(&mut self, value: V::Native) -> Result<K::Native, ArrowError> {
        let key = match self.map.entry(Value(value)) {
            Entry::Vacant(vacant) => {
                // Append new value.
                let key = K::Native::from_usize(self.values_builder.len())
                    .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                self.values_builder.append_value(value);
                vacant.insert(key);
                key
            }
            Entry::Occupied(o) => *o.get(),
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
        self.map.clear();
        let values = self.values_builder.finish();
        let keys = self.keys_builder.finish();

        let data_type =
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(V::DATA_TYPE));

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
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(V::DATA_TYPE));

        let builder = keys
            .into_data()
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values.into_data()]);

        DictionaryArray::from(unsafe { builder.build_unchecked() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::array::UInt32Array;
    use crate::array::UInt8Array;
    use crate::types::{UInt32Type, UInt8Type};

    #[test]
    fn test_primitive_dictionary_builder() {
        let mut builder =
            PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(3, 2);
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
}
