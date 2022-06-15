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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::array::ArrayRef;
use crate::array::ArrowPrimitiveType;
use crate::array::DictionaryArray;
use crate::datatypes::ArrowNativeType;
use crate::datatypes::ToByteSlice;
use crate::error::{ArrowError, Result};

use super::ArrayBuilder;
use super::PrimitiveBuilder;

/// Array builder for `DictionaryArray`. For example to map a set of byte indices
/// to f32 values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
///
/// # Example:
///
/// ```
///  use arrow::array::{
///      Array, PrimitiveBuilder, PrimitiveDictionaryBuilder,
///      UInt8Array, UInt32Array,
///    };
///  use arrow::datatypes::{UInt8Type, UInt32Type};
///
///  let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
///  let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
///  let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
///  builder.append(12345678).unwrap();
///  builder.append_null().unwrap();
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
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(
        keys_builder: PrimitiveBuilder<K>,
        values_builder: PrimitiveBuilder<V>,
    ) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
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
    pub fn append(&mut self, value: V::Native) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.to_byte_slice()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value)?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.to_byte_slice().into(), key);
            Ok(key)
        }
    }

    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let value_ref: ArrayRef = Arc::new(self.values_builder.finish());
        self.keys_builder.finish_dict(value_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::array::UInt32Array;
    use crate::array::UInt8Array;
    use crate::datatypes::UInt32Type;
    use crate::datatypes::UInt8Type;

    #[test]
    fn test_primitive_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
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
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(257);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(257);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        // 256 unique keys.
        for i in 0..256 {
            builder.append(i + 1000).unwrap();
        }
        // Special error if the key overflows (256th entry)
        builder.append(1257).unwrap();
    }
}
