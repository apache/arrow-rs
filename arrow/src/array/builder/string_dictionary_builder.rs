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

use crate::array::array::Array;
use crate::array::ArrayBuilder;
use crate::array::ArrayRef;
use crate::array::ArrowDictionaryKeyType;
use crate::array::DictionaryArray;
use crate::array::PrimitiveBuilder;
use crate::array::StringArray;
use crate::array::StringBuilder;
use crate::datatypes::ArrowNativeType;
use crate::error::{ArrowError, Result};

/// Array builder for `DictionaryArray` that stores Strings. For example to map a set of byte indices
/// to String values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
///
/// ```
/// use arrow::{
///   array::{
///     Int8Array, StringArray,
///     PrimitiveBuilder, StringBuilder, StringDictionaryBuilder,
///   },
///   datatypes::Int8Type,
/// };
///
/// // Create a dictionary array indexed by bytes whose values are Strings.
/// // It can thus hold up to 256 distinct string values.
///
/// let key_builder = PrimitiveBuilder::<Int8Type>::new(100);
/// let value_builder = StringBuilder::new(100);
/// let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
///
/// // The builder builds the dictionary value by value
/// builder.append("abc").unwrap();
/// builder.append_null().unwrap();
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
    keys_builder: PrimitiveBuilder<K>,
    values_builder: StringBuilder,
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Creates a new `StringDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(keys_builder: PrimitiveBuilder<K>, values_builder: StringBuilder) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }

    /// Creates a new `StringDictionaryBuilder` from a keys builder and a dictionary
    /// which is initialized with the given values.
    /// The indices of those dictionary values are used as keys.
    ///
    /// # Example
    ///
    /// ```
    /// use arrow::datatypes::Int16Type;
    /// use arrow::array::{StringArray, StringDictionaryBuilder, PrimitiveBuilder, Int16Array};
    /// use std::convert::TryFrom;
    ///
    /// let dictionary_values = StringArray::from(vec![None, Some("abc"), Some("def")]);
    ///
    /// let mut builder = StringDictionaryBuilder::new_with_dictionary(PrimitiveBuilder::<Int16Type>::new(3), &dictionary_values).unwrap();
    /// builder.append("def").unwrap();
    /// builder.append_null().unwrap();
    /// builder.append("abc").unwrap();
    ///
    /// let dictionary_array = builder.finish();
    ///
    /// let keys = dictionary_array.keys();
    ///
    /// assert_eq!(keys, &Int16Array::from(vec![Some(2), None, Some(1)]));
    /// ```
    pub fn new_with_dictionary(
        keys_builder: PrimitiveBuilder<K>,
        dictionary_values: &StringArray,
    ) -> Result<Self> {
        let dict_len = dictionary_values.len();
        let mut values_builder =
            StringBuilder::with_capacity(dict_len, dictionary_values.value_data().len());
        let mut map: HashMap<Box<[u8]>, K::Native> = HashMap::with_capacity(dict_len);
        for i in 0..dict_len {
            if dictionary_values.is_valid(i) {
                let value = dictionary_values.value(i);
                map.insert(
                    value.as_bytes().into(),
                    K::Native::from_usize(i)
                        .ok_or(ArrowError::DictionaryKeyOverflowError)?,
                );
                values_builder.append_value(value)?;
            } else {
                values_builder.append_null()?;
            }
        }
        Ok(Self {
            keys_builder,
            values_builder,
            map,
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
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    pub fn append(&mut self, value: impl AsRef<str>) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.as_ref().as_bytes()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value.as_ref())?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.as_ref().as_bytes().into(), key);
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
    use crate::array::Int8Array;
    use crate::datatypes::Int16Type;
    use crate::datatypes::Int8Type;

    #[test]
    fn test_string_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<Int8Type>::new(5);
        let value_builder = StringBuilder::new(2);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
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
    fn test_string_dictionary_builder_with_existing_dictionary() {
        let dictionary = StringArray::from(vec![None, Some("def"), Some("abc")]);

        let key_builder = PrimitiveBuilder::<Int8Type>::new(6);
        let mut builder =
            StringDictionaryBuilder::new_with_dictionary(key_builder, &dictionary)
                .unwrap();
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
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

        let key_builder = PrimitiveBuilder::<Int16Type>::new(4);
        let mut builder =
            StringDictionaryBuilder::new_with_dictionary(key_builder, &dictionary)
                .unwrap();
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
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
