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

use crate::{
    types::{
        BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, RunEndIndexType,
        Utf8Type,
    },
    ArrowPrimitiveType, RunEndEncodedArray,
};

use super::{GenericByteBuilder, PrimitiveBuilder};

use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;

/// Array builder for [`RunEndEncodedArray`] for String and Binary types.
///
/// # Example:
///
/// ```
///
/// # use arrow_array::builder::GenericByteREEArrayBuilder;
/// # use arrow_array::{GenericByteArray, BinaryArray};
/// # use arrow_array::types::{BinaryType, Int16Type};
/// # use arrow_array::{Array, Int16Array};
///
/// let mut builder =
/// GenericByteREEArrayBuilder::<Int16Type, BinaryType>::new();
/// builder.append_value(b"abc").unwrap();
/// builder.append_value(b"abc").unwrap();
/// builder.append_null().unwrap();
/// builder.append_value(b"def").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///     array.run_ends(),
///     &Int16Array::from(vec![Some(2), Some(3), Some(4)])
/// );
///
/// let av = array.values();
///
/// assert!(!av.is_null(0));
/// assert!(av.is_null(1));
/// assert!(!av.is_null(2));
///
/// // Values are polymorphic and so require a downcast.
/// let ava: &BinaryArray = av.as_any().downcast_ref::<BinaryArray>().unwrap();
///
/// assert_eq!(ava.value(0), b"abc");
/// assert_eq!(ava.value(2), b"def");
/// ```
#[derive(Debug)]
pub struct GenericByteREEArrayBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    run_ends_builder: PrimitiveBuilder<R>,
    values_builder: GenericByteBuilder<V>,
    current_value: Option<Vec<u8>>,
    current_run_end_index: usize,
}

impl<R, V> Default for GenericByteREEArrayBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R, V> GenericByteREEArrayBuilder<R, V>
where
    R: ArrowPrimitiveType,
    V: ByteArrayType,
{
    /// Creates a new `GenericByteREEArrayBuilder`
    pub fn new() -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::new(),
            values_builder: GenericByteBuilder::<V>::new(),
            current_value: None,
            current_run_end_index: 0,
        }
    }

    /// Creates a new `GenericByteREEArrayBuilder` with the provided capacity
    ///
    /// `capacity`: the expected number of run-end encoded values.
    /// `data_capacity`: the expected number of bytes of run end encoded values
    pub fn with_capacity(capacity: usize, data_capacity: usize) -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::with_capacity(capacity),
            values_builder: GenericByteBuilder::<V>::with_capacity(
                capacity,
                data_capacity,
            ),
            current_value: None,
            current_run_end_index: 0,
        }
    }
}

impl<R, V> GenericByteREEArrayBuilder<R, V>
where
    R: RunEndIndexType,
    V: ByteArrayType,
{
    /// Appends optional value to the logical array encoded by the RunEndEncodedArray.
    pub fn append_option(
        &mut self,
        input_value: Option<impl AsRef<V::Native>>,
    ) -> Result<(), ArrowError> {
        match input_value {
            Some(value) => self.append_value(value)?,
            None => self.append_null()?,
        }
        Ok(())
    }

    /// Appends value to the logical array encoded by the RunEndEncodedArray.
    pub fn append_value(
        &mut self,
        input_value: impl AsRef<V::Native>,
    ) -> Result<(), ArrowError> {
        let value: &[u8] = input_value.as_ref().as_ref();
        match self.current_value.as_deref() {
            None if self.current_run_end_index > 0 => {
                self.append_run_end()?;
                self.current_value = Some(value.to_owned());
            }
            None if self.current_run_end_index == 0 => {
                self.current_value = Some(value.to_owned());
            }
            Some(current_value) if current_value != value => {
                self.append_run_end()?;
                self.current_value = Some(value.to_owned());
            }
            _ => {}
        }
        self.current_run_end_index = self
            .current_run_end_index
            .checked_add(1)
            .ok_or(ArrowError::RunEndIndexOverflowError)?;
        Ok(())
    }

    /// Appends null to the logical array encoded by the RunEndEncodedArray.
    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        if self.current_value.is_some() {
            self.append_run_end()?;
            self.current_value = None;
        }
        self.current_run_end_index = self
            .current_run_end_index
            .checked_add(1)
            .ok_or(ArrowError::RunEndIndexOverflowError)?;
        Ok(())
    }

    /// Creates the RunEndEncodedArray and resets the builder.
    /// Panics if RunEndEncodedArray cannot be built.
    pub fn finish(&mut self) -> RunEndEncodedArray<R> {
        // write the last run end to the array.
        self.append_run_end().unwrap();

        // reset the run end index to zero.
        self.current_value = None;
        self.current_run_end_index = 0;

        // build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish();
        let values_array = self.values_builder.finish();
        RunEndEncodedArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    /// Creates the RunEndEncodedArray and without resetting the builder.
    /// Panics if RunEndEncodedArray cannot be built.
    pub fn finish_cloned(&mut self) -> RunEndEncodedArray<R> {
        // write the last run end to the array.
        self.append_run_end().unwrap();

        // build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish_cloned();
        let values_array = self.values_builder.finish_cloned();
        RunEndEncodedArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    // Appends the current run to the array
    fn append_run_end(&mut self) -> Result<(), ArrowError> {
        let run_end_index = R::Native::from_usize(self.current_run_end_index)
            .ok_or_else(|| {
                ArrowError::ParseError(format!(
                    "Cannot convert the value {} from `usize` to native form of arrow datatype {}",
                    self.current_run_end_index,
                    R::DATA_TYPE
                ))
            })?;
        self.run_ends_builder.append_value(run_end_index);
        match self.current_value.as_deref() {
            Some(value) => self.values_builder.append_slice(value),
            None => self.values_builder.append_null(),
        }
        Ok(())
    }
}

/// Array builder for [`RunEndEncodedArray`] that encodes strings ([`Utf8Type`]).
///
/// ```
/// // Create a run-end encoded array with run-end indexes data type as `i16`.
/// // The encoded values are Strings.
///
/// # use arrow_array::builder::StringREEArrayBuilder;
/// # use arrow_array::{Int16Array, StringArray};
/// # use arrow_array::types::Int16Type;
///
/// let mut builder = StringREEArrayBuilder::<Int16Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append_value("abc").unwrap();
/// builder.append_null();
/// builder.append_value("def").unwrap();
/// builder.append_value("def").unwrap();
/// builder.append_value("abc").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///   array.run_ends(),
///   &Int16Array::from(vec![Some(1), Some(2), Some(4), Some(5)])
/// );
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(ava.value(0), "abc");
/// assert!(av.is_null(1));
/// assert_eq!(ava.value(2), "def");
/// assert_eq!(ava.value(3), "abc");
///
/// ```
pub type StringREEArrayBuilder<K> = GenericByteREEArrayBuilder<K, Utf8Type>;

/// Array builder for [`RunEndEncodedArray`] that encodes large strings ([`LargeUtf8Type`]). See [`StringREEArrayBuilder`] for an example.
pub type LargeStringREEArrayBuilder<K> = GenericByteREEArrayBuilder<K, LargeUtf8Type>;

/// Array builder for [`RunEndEncodedArray`] that encodes binary values([`BinaryType`]).
///
/// ```
/// // Create a run-end encoded array with run-end indexes data type as `i16`.
/// // The encoded data is binary values.
///
/// # use arrow_array::builder::BinaryREEArrayBuilder;
/// # use arrow_array::{BinaryArray, Int16Array};
/// # use arrow_array::types::Int16Type;
///
/// let mut builder = BinaryREEArrayBuilder::<Int16Type>::new();
///
/// // The builder builds the dictionary value by value
/// builder.append_value(b"abc").unwrap();
/// builder.append_null();
/// builder.append_value(b"def").unwrap();
/// builder.append_value(b"def").unwrap();
/// builder.append_value(b"abc").unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///   array.run_ends(),
///   &Int16Array::from(vec![Some(1), Some(2), Some(4), Some(5)])
/// );
///
/// // Values are polymorphic and so require a downcast.
/// let av = array.values();
/// let ava: &BinaryArray = av.as_any().downcast_ref::<BinaryArray>().unwrap();
///
/// assert_eq!(ava.value(0), b"abc");
/// assert!(av.is_null(1));
/// assert_eq!(ava.value(2), b"def");
/// assert_eq!(ava.value(3), b"abc");
///
/// ```
pub type BinaryREEArrayBuilder<K> = GenericByteREEArrayBuilder<K, BinaryType>;

/// Array builder for [`RunEndEncodedArray`] that encodes large binary values([`LargeBinaryType`]).
/// See documentation of [`BinaryREEArrayBuilder`] for an example.
pub type LargeBinaryREEArrayBuilder<K> = GenericByteREEArrayBuilder<K, LargeBinaryType>;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::types::Int16Type;
    use crate::GenericByteArray;
    use crate::Int16Array;

    fn test_bytes_ree_array_buider<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteREEArrayBuilder::<Int16Type, T>::new();
        builder.append_value(values[0]).unwrap();
        builder.append_value(values[0]).unwrap();
        builder.append_value(values[0]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(values[1]).unwrap();
        builder.append_value(values[1]).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.run_ends(),
            &Int16Array::from(vec![Some(3), Some(5), Some(7)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> =
            av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(*ava.value(0), *values[0]);
        assert!(ava.is_null(1));
        assert_eq!(*ava.value(2), *values[1]);
    }

    #[test]
    fn test_string_ree_array_buider() {
        test_bytes_ree_array_buider::<Utf8Type>(vec!["abc", "def"]);
    }

    #[test]
    fn test_binary_ree_array_buider() {
        test_bytes_ree_array_buider::<BinaryType>(vec![b"abc", b"def"]);
    }

    fn test_bytes_ree_array_buider_finish_cloned<T>(values: Vec<&T::Native>)
    where
        T: ByteArrayType,
        <T as ByteArrayType>::Native: PartialEq,
        <T as ByteArrayType>::Native: AsRef<<T as ByteArrayType>::Native>,
    {
        let mut builder = GenericByteREEArrayBuilder::<Int16Type, T>::new();

        builder.append_value(values[0]).unwrap();
        builder.append_null().unwrap();
        builder.append_value(values[1]).unwrap();
        builder.append_value(values[1]).unwrap();
        builder.append_value(values[0]).unwrap();
        let mut array = builder.finish_cloned();

        assert_eq!(
            array.run_ends(),
            &Int16Array::from(vec![Some(1), Some(2), Some(4), Some(5)])
        );

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &GenericByteArray<T> =
            av.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava.value(0), values[0]);
        assert!(ava.is_null(1));
        assert_eq!(ava.value(2), values[1]);
        assert_eq!(ava.value(3), values[0]);

        builder.append_value(values[0]).unwrap();
        builder.append_value(values[0]).unwrap();
        builder.append_value(values[1]).unwrap();

        array = builder.finish();

        assert_eq!(
            array.run_ends(),
            &Int16Array::from(
                vec![Some(1), Some(2), Some(4), Some(5), Some(7), Some(8),]
            )
        );

        // Values are polymorphic and so require a downcast.
        let av2 = array.values();
        let ava2: &GenericByteArray<T> =
            av2.as_any().downcast_ref::<GenericByteArray<T>>().unwrap();

        assert_eq!(ava2.value(0), values[0]);
        assert!(ava2.is_null(1));
        assert_eq!(ava2.value(2), values[1]);
        assert_eq!(ava2.value(3), values[0]);
        assert_eq!(ava2.value(4), values[0]);
        assert_eq!(ava2.value(5), values[1]);
    }

    #[test]
    fn test_string_ree_array_buider_finish_cloned() {
        test_bytes_ree_array_buider_finish_cloned::<Utf8Type>(vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_binary_ree_array_buider_finish_cloned() {
        test_bytes_ree_array_buider_finish_cloned::<BinaryType>(vec![
            b"abc", b"def", b"ghi",
        ]);
    }
}
