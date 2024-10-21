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

use crate::types::bytes::ByteArrayNativeType;
use std::{any::Any, sync::Arc};

use crate::{
    types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, RunEndIndexType, Utf8Type},
    ArrayRef, ArrowPrimitiveType, RunArray,
};

use super::{ArrayBuilder, GenericByteBuilder, PrimitiveBuilder};

use arrow_buffer::ArrowNativeType;

/// Builder for [`RunArray`] of [`GenericByteArray`](crate::array::GenericByteArray)
///
/// # Example:
///
/// ```
///
/// # use arrow_array::builder::GenericByteRunBuilder;
/// # use arrow_array::{GenericByteArray, BinaryArray};
/// # use arrow_array::types::{BinaryType, Int16Type};
/// # use arrow_array::{Array, Int16Array};
/// # use arrow_array::cast::AsArray;
///
/// let mut builder =
/// GenericByteRunBuilder::<Int16Type, BinaryType>::new();
/// builder.extend([Some(b"abc"), Some(b"abc"), None, Some(b"def")].into_iter());
/// builder.append_value(b"def");
/// builder.append_null();
/// let array = builder.finish();
///
/// assert_eq!(array.run_ends().values(), &[2, 3, 5, 6]);
///
/// let av = array.values();
///
/// assert!(!av.is_null(0));
/// assert!(av.is_null(1));
/// assert!(!av.is_null(2));
/// assert!(av.is_null(3));
///
/// // Values are polymorphic and so require a downcast.
/// let ava: &BinaryArray = av.as_binary();
///
/// assert_eq!(ava.value(0), b"abc");
/// assert_eq!(ava.value(2), b"def");
/// ```
#[derive(Debug)]
pub struct GenericByteRunBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    run_ends_builder: PrimitiveBuilder<R>,
    values_builder: GenericByteBuilder<V>,
    current_value: Vec<u8>,
    has_current_value: bool,
    current_run_end_index: usize,
    prev_run_end_index: usize,
}

impl<R, V> Default for GenericByteRunBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R, V> GenericByteRunBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    /// Creates a new `GenericByteRunBuilder`
    pub fn new() -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::new(),
            values_builder: GenericByteBuilder::<V>::new(),
            current_value: Vec::new(),
            has_current_value: false,
            current_run_end_index: 0,
            prev_run_end_index: 0,
        }
    }

    /// Creates a new `GenericByteRunBuilder` with the provided capacity
    ///
    /// `capacity`: the expected number of run-end encoded values.
    /// `data_capacity`: the expected number of bytes of run end encoded values
    pub fn with_capacity(capacity: usize, data_capacity: usize) -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::with_capacity(capacity),
            values_builder: GenericByteBuilder::<V>::with_capacity(capacity, data_capacity),
            current_value: Vec::new(),
            has_current_value: false,
            current_run_end_index: 0,
            prev_run_end_index: 0,
        }
    }
}

impl<R, V> ArrayBuilder for GenericByteRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ByteArrayType,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the length of logical array encoded by
    /// the eventual runs array.
    fn len(&self) -> usize {
        self.current_run_end_index
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

impl<R, V> GenericByteRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ByteArrayType,
{
    /// Appends optional value to the logical array encoded by the RunArray.
    pub fn append_option(&mut self, input_value: Option<impl AsRef<V::Native>>) {
        match input_value {
            Some(value) => self.append_value(value),
            None => self.append_null(),
        }
    }

    /// Appends value to the logical array encoded by the RunArray.
    pub fn append_value(&mut self, input_value: impl AsRef<V::Native>) {
        let value: &[u8] = input_value.as_ref().as_ref();
        if !self.has_current_value {
            self.append_run_end();
            self.current_value.extend_from_slice(value);
            self.has_current_value = true;
        } else if self.current_value.as_slice() != value {
            self.append_run_end();
            self.current_value.clear();
            self.current_value.extend_from_slice(value);
        }
        self.current_run_end_index += 1;
    }

    /// Appends null to the logical array encoded by the RunArray.
    pub fn append_null(&mut self) {
        if self.has_current_value {
            self.append_run_end();
            self.current_value.clear();
            self.has_current_value = false;
        }
        self.current_run_end_index += 1;
    }

    /// Creates the RunArray and resets the builder.
    /// Panics if RunArray cannot be built.
    pub fn finish(&mut self) -> RunArray<R> {
        // write the last run end to the array.
        self.append_run_end();

        // reset the run end index to zero.
        self.current_value.clear();
        self.has_current_value = false;
        self.current_run_end_index = 0;
        self.prev_run_end_index = 0;

        // build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish();
        let values_array = self.values_builder.finish();
        RunArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    /// Creates the RunArray and without resetting the builder.
    /// Panics if RunArray cannot be built.
    pub fn finish_cloned(&self) -> RunArray<R> {
        let mut run_ends_array = self.run_ends_builder.finish_cloned();
        let mut values_array = self.values_builder.finish_cloned();

        // Add current run if one exists
        if self.prev_run_end_index != self.current_run_end_index {
            let mut run_end_builder = run_ends_array.into_builder().unwrap();
            let mut values_builder = values_array.into_builder().unwrap();
            self.append_run_end_with_builders(&mut run_end_builder, &mut values_builder);
            run_ends_array = run_end_builder.finish();
            values_array = values_builder.finish();
        }

        RunArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    // Appends the current run to the array.
    fn append_run_end(&mut self) {
        // empty array or the function called without appending any value.
        if self.prev_run_end_index == self.current_run_end_index {
            return;
        }
        let run_end_index = self.run_end_index_as_native();
        self.run_ends_builder.append_value(run_end_index);
        if self.has_current_value {
            let slice = self.current_value.as_slice();
            let native = unsafe {
                // Safety:
                // As self.current_value is created from V::Native. The value V::Native can be
                // built back from the bytes without validations
                V::Native::from_bytes_unchecked(slice)
            };
            self.values_builder.append_value(native);
        } else {
            self.values_builder.append_null();
        }
        self.prev_run_end_index = self.current_run_end_index;
    }

    // Similar to `append_run_end` but on custom builders.
    // Used in `finish_cloned` which is not suppose to mutate `self`.
    fn append_run_end_with_builders(
        &self,
        run_ends_builder: &mut PrimitiveBuilder<R>,
        values_builder: &mut GenericByteBuilder<V>,
    ) {
        let run_end_index = self.run_end_index_as_native();
        run_ends_builder.append_value(run_end_index);
        if self.has_current_value {
            let slice = self.current_value.as_slice();
            let native = unsafe {
                // Safety:
                // As self.current_value is created from V::Native. The value V::Native can be
                // built back from the bytes without validations
                V::Native::from_bytes_unchecked(slice)
            };
            values_builder.append_value(native);
        } else {
            values_builder.append_null();
        }
    }

    fn run_end_index_as_native(&self) -> R::Native {
        R::Native::from_usize(self.current_run_end_index).unwrap_or_else(|| {
            panic!(
                "Cannot convert the value {} from `usize` to native form of arrow datatype {}",
                self.current_run_end_index,
                R::DATA_TYPE
            )
        })
    }
}

impl<R, V, S> Extend<Option<S>> for GenericByteRunBuilder<R, V>
where
    R: RunEndIndexType,
    V: ByteArrayType,
    S: AsRef<V::Native>,
{
    fn extend<T: IntoIterator<Item = Option<S>>>(&mut self, iter: T) {
        for elem in iter {
            self.append_option(elem);
        }
    }
}

/// Builder for [`RunArray`] of [`StringArray`](crate::array::StringArray)
///
/// ```
/// // Create a run-end encoded array with run-end indexes data type as `i16`.
/// // The encoded values are Strings.
///
/// # use arrow_array::builder::StringRunBuilder;
/// # use arrow_array::{Int16Array, StringArray};
/// # use arrow_array::types::Int16Type;
/// # use arrow_array::cast::AsArray;
/// #
/// let mut builder = StringRunBuilder::<Int16Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append_value("abc");
/// builder.append_null();
/// builder.extend([Some("def"), Some("def"), Some("abc")]);
/// let array = builder.finish();
///
/// assert_eq!(array.run_ends().values(), &[1, 2, 4, 5]);
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &StringArray = av.as_string::<i32>();
///
/// assert_eq!(ava.value(0), "abc");
/// assert!(av.is_null(1));
/// assert_eq!(ava.value(2), "def");
/// assert_eq!(ava.value(3), "abc");
///
/// ```
pub type StringRunBuilder<K> = GenericByteRunBuilder<K, Utf8Type>;

/// Builder for [`RunArray`] of [`LargeStringArray`](crate::array::LargeStringArray)
pub type LargeStringRunBuilder<K> = GenericByteRunBuilder<K, LargeUtf8Type>;

/// Builder for [`RunArray`] of [`BinaryArray`](crate::array::BinaryArray)
///
/// ```
/// // Create a run-end encoded array with run-end indexes data type as `i16`.
/// // The encoded data is binary values.
///
/// # use arrow_array::builder::BinaryRunBuilder;
/// # use arrow_array::{BinaryArray, Int16Array};
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::types::Int16Type;
///
/// let mut builder = BinaryRunBuilder::<Int16Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append_value(b"abc");
/// builder.append_null();
/// builder.extend([Some(b"def"), Some(b"def"), Some(b"abc")]);
/// let array = builder.finish();
///
/// assert_eq!(array.run_ends().values(), &[1, 2, 4, 5]);
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &BinaryArray = av.as_binary();
///
/// assert_eq!(ava.value(0), b"abc");
/// assert!(av.is_null(1));
/// assert_eq!(ava.value(2), b"def");
/// assert_eq!(ava.value(3), b"abc");
///
/// ```
pub type BinaryRunBuilder<K> = GenericByteRunBuilder<K, BinaryType>;

/// Builder for [`RunArray`] of [`LargeBinaryArray`](crate::array::LargeBinaryArray)
pub type LargeBinaryRunBuilder<K> = GenericByteRunBuilder<K, LargeBinaryType>;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::cast::AsArray;
    use crate::types::{Int16Type, Int32Type};
    use crate::GenericByteArray;
    use crate::Int16RunArray;

    fn test_bytes_run_builder<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteRunBuilder::<Int16Type, T>::new();
        builder.append_value(values[0]);
        builder.append_value(values[0]);
        builder.append_value(values[0]);
        builder.append_null();
        builder.append_null();
        builder.append_value(values[1]);
        builder.append_value(values[1]);
        builder.append_value(values[2]);
        builder.append_value(values[2]);
        builder.append_value(values[2]);
        builder.append_value(values[2]);
        let array = builder.finish();

        assert_eq!(array.len(), 11);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 2);

        assert_eq!(array.run_ends().values(), &[3, 5, 7, 11]);

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> = av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(*ava.value(0), *values[0]);
        assert!(ava.is_null(1));
        assert_eq!(*ava.value(2), *values[1]);
        assert_eq!(*ava.value(3), *values[2]);
    }

    #[test]
    fn test_string_run_builder() {
        test_bytes_run_builder::<Utf8Type>(vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_string_run_builder_with_empty_strings() {
        test_bytes_run_builder::<Utf8Type>(vec!["abc", "", "ghi"]);
    }

    #[test]
    fn test_binary_run_builder() {
        test_bytes_run_builder::<BinaryType>(vec![b"abc", b"def", b"ghi"]);
    }

    fn test_bytes_run_builder_finish_cloned<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteRunBuilder::<Int16Type, T>::new();

        builder.append_value(values[0]);
        builder.append_null();
        builder.append_value(values[1]);
        builder.append_value(values[1]);
        builder.append_value(values[0]);
        let mut array: Int16RunArray = builder.finish_cloned();

        assert_eq!(array.len(), 5);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 1);

        assert_eq!(array.run_ends().values(), &[1, 2, 4, 5]);

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> = av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava.value(0), values[0]);
        assert!(ava.is_null(1));
        assert_eq!(ava.value(2), values[1]);
        assert_eq!(ava.value(3), values[0]);

        // Append last value before `finish_cloned` (`value[0]`) again and ensure it has only
        // one entry in final output.
        builder.append_value(values[0]);
        builder.append_value(values[0]);
        builder.append_value(values[1]);
        array = builder.finish();

        assert_eq!(array.len(), 8);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 1);

        assert_eq!(array.run_ends().values(), &[1, 2, 4, 7, 8]);

        // Values are polymorphic and so require a downcast.
        let av2 = array.values();
        let ava2: &GenericByteArray<T> =
            av2.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava2.value(0), values[0]);
        assert!(ava2.is_null(1));
        assert_eq!(ava2.value(2), values[1]);
        // The value appended before and after `finish_cloned` has only one entry.
        assert_eq!(ava2.value(3), values[0]);
        assert_eq!(ava2.value(4), values[1]);
    }

    #[test]
    fn test_string_run_builder_finish_cloned() {
        test_bytes_run_builder_finish_cloned::<Utf8Type>(vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_binary_run_builder_finish_cloned() {
        test_bytes_run_builder_finish_cloned::<BinaryType>(vec![b"abc", b"def", b"ghi"]);
    }

    #[test]
    fn test_extend() {
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.extend(["a", "a", "a", "", "", "b", "b"].into_iter().map(Some));
        builder.extend(["b", "cupcakes", "cupcakes"].into_iter().map(Some));
        let array = builder.finish();

        assert_eq!(array.len(), 10);
        assert_eq!(array.run_ends().values(), &[3, 5, 8, 10]);

        let str_array = array.values().as_string::<i32>();
        assert_eq!(str_array.value(0), "a");
        assert_eq!(str_array.value(1), "");
        assert_eq!(str_array.value(2), "b");
        assert_eq!(str_array.value(3), "cupcakes");
    }
}
