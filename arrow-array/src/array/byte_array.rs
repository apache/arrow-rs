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

use crate::array::{get_offsets, print_long_array};
use crate::builder::GenericByteBuilder;
use crate::iterator::ArrayIter;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::ByteArrayType;
use crate::{Array, ArrayAccessor, ArrayRef, OffsetSizeTrait, Scalar};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::sync::Arc;

/// An array of [variable length byte arrays](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout)
///
/// See [`StringArray`] and [`LargeStringArray`] for storing utf8 encoded string data
///
/// See [`BinaryArray`] and [`LargeBinaryArray`] for storing arbitrary bytes
///
/// # Example: From a Vec
///
/// ```
/// # use arrow_array::{Array, GenericByteArray, types::Utf8Type};
/// let arr: GenericByteArray<Utf8Type> = vec!["hello", "world", ""].into();
/// assert_eq!(arr.value_data(), b"helloworld");
/// assert_eq!(arr.value_offsets(), &[0, 5, 10, 10]);
/// let values: Vec<_> = arr.iter().collect();
/// assert_eq!(values, &[Some("hello"), Some("world"), Some("")]);
/// ```
///
/// # Example: From an optional Vec
///
/// ```
/// # use arrow_array::{Array, GenericByteArray, types::Utf8Type};
/// let arr: GenericByteArray<Utf8Type> = vec![Some("hello"), Some("world"), Some(""), None].into();
/// assert_eq!(arr.value_data(), b"helloworld");
/// assert_eq!(arr.value_offsets(), &[0, 5, 10, 10, 10]);
/// let values: Vec<_> = arr.iter().collect();
/// assert_eq!(values, &[Some("hello"), Some("world"), Some(""), None]);
/// ```
///
/// # Example: From an iterator of option
///
/// ```
/// # use arrow_array::{Array, GenericByteArray, types::Utf8Type};
/// let arr: GenericByteArray<Utf8Type> = (0..5).map(|x| (x % 2 == 0).then(|| x.to_string())).collect();
/// let values: Vec<_> = arr.iter().collect();
/// assert_eq!(values, &[Some("0"), None, Some("2"), None, Some("4")]);
/// ```
///
/// # Example: Using Builder
///
/// ```
/// # use arrow_array::Array;
/// # use arrow_array::builder::GenericByteBuilder;
/// # use arrow_array::types::Utf8Type;
/// let mut builder = GenericByteBuilder::<Utf8Type>::new();
/// builder.append_value("hello");
/// builder.append_null();
/// builder.append_value("world");
/// let array = builder.finish();
/// let values: Vec<_> = array.iter().collect();
/// assert_eq!(values, &[Some("hello"), None, Some("world")]);
/// ```
///
/// [`StringArray`]: crate::StringArray
/// [`LargeStringArray`]: crate::LargeStringArray
/// [`BinaryArray`]: crate::BinaryArray
/// [`LargeBinaryArray`]: crate::LargeBinaryArray
pub struct GenericByteArray<T: ByteArrayType> {
    data_type: DataType,
    value_offsets: OffsetBuffer<T::Offset>,
    value_data: Buffer,
    nulls: Option<NullBuffer>,
}

impl<T: ByteArrayType> Clone for GenericByteArray<T> {
    fn clone(&self) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            value_offsets: self.value_offsets.clone(),
            value_data: self.value_data.clone(),
            nulls: self.nulls.clone(),
        }
    }
}

impl<T: ByteArrayType> GenericByteArray<T> {
    /// Data type of the array.
    pub const DATA_TYPE: DataType = T::DATA_TYPE;

    /// Create a new [`GenericByteArray`] from the provided parts, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`GenericByteArray::try_new`] returns an error
    pub fn new(
        offsets: OffsetBuffer<T::Offset>,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self::try_new(offsets, values, nulls).unwrap()
    }

    /// Create a new [`GenericByteArray`] from the provided parts, returning an error on failure
    ///
    /// # Errors
    ///
    /// * `offsets.len() - 1 != nulls.len()`
    /// * Any consecutive pair of `offsets` does not denote a valid slice of `values`
    pub fn try_new(
        offsets: OffsetBuffer<T::Offset>,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let len = offsets.len() - 1;

        // Verify that each pair of offsets is a valid slices of values
        T::validate(&offsets, &values)?;

        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for {}{}Array, expected {len} got {}",
                    T::Offset::PREFIX,
                    T::PREFIX,
                    n.len(),
                )));
            }
        }

        Ok(Self {
            data_type: T::DATA_TYPE,
            value_offsets: offsets,
            value_data: values,
            nulls,
        })
    }

    /// Create a new [`GenericByteArray`] from the provided parts, without validation
    ///
    /// # Safety
    ///
    /// Safe if [`Self::try_new`] would not error
    pub unsafe fn new_unchecked(
        offsets: OffsetBuffer<T::Offset>,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            value_offsets: offsets,
            value_data: values,
            nulls,
        }
    }

    /// Create a new [`GenericByteArray`] of length `len` where all values are null
    pub fn new_null(len: usize) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            value_offsets: OffsetBuffer::new_zeroed(len),
            value_data: MutableBuffer::new(0).into(),
            nulls: Some(NullBuffer::new_null(len)),
        }
    }

    /// Create a new [`Scalar`] from `v`
    pub fn new_scalar(value: impl AsRef<T::Native>) -> Scalar<Self> {
        Scalar::new(Self::from_iter_values(std::iter::once(value)))
    }

    /// Creates a [`GenericByteArray`] based on an iterator of values without nulls
    pub fn from_iter_values<Ptr, I>(iter: I) -> Self
    where
        Ptr: AsRef<T::Native>,
        I: IntoIterator<Item = Ptr>,
    {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets = MutableBuffer::new((data_len + 1) * std::mem::size_of::<T::Offset>());
        offsets.push(T::Offset::usize_as(0));

        let mut values = MutableBuffer::new(0);
        for s in iter {
            let s: &[u8] = s.as_ref().as_ref();
            values.extend_from_slice(s);
            offsets.push(T::Offset::usize_as(values.len()));
        }

        T::Offset::from_usize(values.len()).expect("offset overflow");
        let offsets = Buffer::from(offsets);

        // Safety: valid by construction
        let value_offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

        Self {
            data_type: T::DATA_TYPE,
            value_data: values.into(),
            value_offsets,
            nulls: None,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (OffsetBuffer<T::Offset>, Buffer, Option<NullBuffer>) {
        (self.value_offsets, self.value_data, self.nulls)
    }

    /// Returns the length for value at index `i`.
    /// # Panics
    /// Panics if index `i` is out of bounds.
    #[inline]
    pub fn value_length(&self, i: usize) -> T::Offset {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// Returns a reference to the offsets of this array
    ///
    /// Unlike [`Self::value_offsets`] this returns the [`OffsetBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn offsets(&self) -> &OffsetBuffer<T::Offset> {
        &self.value_offsets
    }

    /// Returns the values of this array
    ///
    /// Unlike [`Self::value_data`] this returns the [`Buffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn values(&self) -> &Buffer {
        &self.value_data
    }

    /// Returns the raw value data
    pub fn value_data(&self) -> &[u8] {
        self.value_data.as_slice()
    }

    /// Returns true if all data within this array is ASCII
    pub fn is_ascii(&self) -> bool {
        let offsets = self.value_offsets();
        let start = offsets.first().unwrap();
        let end = offsets.last().unwrap();
        self.value_data()[start.as_usize()..end.as_usize()].is_ascii()
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[T::Offset] {
        &self.value_offsets
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> &T::Native {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);

        // Soundness
        // pointer alignment & location is ensured by RawPtrBox
        // buffer bounds/offset is ensured by the value_offset invariants

        // Safety of `to_isize().unwrap()`
        // `start` and `end` are &OffsetSize, which is a generic type that implements the
        // OffsetSizeTrait. Currently, only i32 and i64 implement OffsetSizeTrait,
        // both of which should cleanly cast to isize on an architecture that supports
        // 32/64-bit offsets
        let b = std::slice::from_raw_parts(
            self.value_data.as_ptr().offset(start.to_isize().unwrap()),
            (end - start).to_usize().unwrap(),
        );

        // SAFETY:
        // ArrayData is valid
        T::Native::from_bytes_unchecked(b)
    }

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn value(&self, i: usize) -> &T::Native {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a {}{}Array of length {}",
            i,
            T::Offset::PREFIX,
            T::PREFIX,
            self.len()
        );
        // SAFETY:
        // Verified length above
        unsafe { self.value_unchecked(i) }
    }

    /// constructs a new iterator
    pub fn iter(&self) -> ArrayIter<&Self> {
        ArrayIter::new(self)
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            value_offsets: self.value_offsets.slice(offset, length),
            value_data: self.value_data.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
        }
    }

    /// Returns `GenericByteBuilder` of this byte array for mutating its values if the underlying
    /// offset and data buffers are not shared by others.
    pub fn into_builder(self) -> Result<GenericByteBuilder<T>, Self> {
        let len = self.len();
        let value_len = T::Offset::as_usize(self.value_offsets()[len] - self.value_offsets()[0]);

        let data = self.into_data();
        let null_bit_buffer = data.nulls().map(|b| b.inner().sliced());

        let element_len = std::mem::size_of::<T::Offset>();
        let offset_buffer = data.buffers()[0]
            .slice_with_length(data.offset() * element_len, (len + 1) * element_len);

        let element_len = std::mem::size_of::<u8>();
        let value_buffer = data.buffers()[1]
            .slice_with_length(data.offset() * element_len, value_len * element_len);

        drop(data);

        let try_mutable_null_buffer = match null_bit_buffer {
            None => Ok(None),
            Some(null_buffer) => {
                // Null buffer exists, tries to make it mutable
                null_buffer.into_mutable().map(Some)
            }
        };

        let try_mutable_buffers = match try_mutable_null_buffer {
            Ok(mutable_null_buffer) => {
                // Got mutable null buffer, tries to get mutable value buffer
                let try_mutable_offset_buffer = offset_buffer.into_mutable();
                let try_mutable_value_buffer = value_buffer.into_mutable();

                // try_mutable_offset_buffer.map(...).map_err(...) doesn't work as the compiler complains
                // mutable_null_buffer is moved into map closure.
                match (try_mutable_offset_buffer, try_mutable_value_buffer) {
                    (Ok(mutable_offset_buffer), Ok(mutable_value_buffer)) => unsafe {
                        Ok(GenericByteBuilder::<T>::new_from_buffer(
                            mutable_offset_buffer,
                            mutable_value_buffer,
                            mutable_null_buffer,
                        ))
                    },
                    (Ok(mutable_offset_buffer), Err(value_buffer)) => Err((
                        mutable_offset_buffer.into(),
                        value_buffer,
                        mutable_null_buffer.map(|b| b.into()),
                    )),
                    (Err(offset_buffer), Ok(mutable_value_buffer)) => Err((
                        offset_buffer,
                        mutable_value_buffer.into(),
                        mutable_null_buffer.map(|b| b.into()),
                    )),
                    (Err(offset_buffer), Err(value_buffer)) => Err((
                        offset_buffer,
                        value_buffer,
                        mutable_null_buffer.map(|b| b.into()),
                    )),
                }
            }
            Err(mutable_null_buffer) => {
                // Unable to get mutable null buffer
                Err((offset_buffer, value_buffer, Some(mutable_null_buffer)))
            }
        };

        match try_mutable_buffers {
            Ok(builder) => Ok(builder),
            Err((offset_buffer, value_buffer, null_bit_buffer)) => {
                let builder = ArrayData::builder(T::DATA_TYPE)
                    .len(len)
                    .add_buffer(offset_buffer)
                    .add_buffer(value_buffer)
                    .null_bit_buffer(null_bit_buffer);

                let array_data = unsafe { builder.build_unchecked() };
                let array = GenericByteArray::<T>::from(array_data);

                Err(array)
            }
        }
    }
}

impl<T: ByteArrayType> std::fmt::Debug for GenericByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}{}Array\n[\n", T::Offset::PREFIX, T::PREFIX)?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<T: ByteArrayType> Array for GenericByteArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.value_offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.value_offsets.len() <= 1
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn logical_null_count(&self) -> usize {
        // More efficient that the default implementation
        self.null_count()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut sum = self.value_offsets.inner().inner().capacity();
        sum += self.value_data.capacity();
        if let Some(x) = &self.nulls {
            sum += x.buffer().capacity()
        }
        sum
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.get_buffer_memory_size()
    }
}

impl<'a, T: ByteArrayType> ArrayAccessor for &'a GenericByteArray<T> {
    type Item = &'a T::Native;

    fn value(&self, index: usize) -> Self::Item {
        GenericByteArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericByteArray::value_unchecked(self, index)
    }
}

impl<T: ByteArrayType> From<ArrayData> for GenericByteArray<T> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.data_type(),
            &Self::DATA_TYPE,
            "{}{}Array expects DataType::{}",
            T::Offset::PREFIX,
            T::PREFIX,
            Self::DATA_TYPE
        );
        assert_eq!(
            data.buffers().len(),
            2,
            "{}{}Array data should contain 2 buffers only (offsets and values)",
            T::Offset::PREFIX,
            T::PREFIX,
        );
        // SAFETY:
        // ArrayData is valid, and verified type above
        let value_offsets = unsafe { get_offsets(&data) };
        let value_data = data.buffers()[1].clone();
        Self {
            value_offsets,
            value_data,
            data_type: T::DATA_TYPE,
            nulls: data.nulls().cloned(),
        }
    }
}

impl<T: ByteArrayType> From<GenericByteArray<T>> for ArrayData {
    fn from(array: GenericByteArray<T>) -> Self {
        let len = array.len();

        let offsets = array.value_offsets.into_inner().into_inner();
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .buffers(vec![offsets, array.value_data])
            .nulls(array.nulls);

        unsafe { builder.build_unchecked() }
    }
}

impl<'a, T: ByteArrayType> IntoIterator for &'a GenericByteArray<T> {
    type Item = Option<&'a T::Native>;
    type IntoIter = ArrayIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIter::new(self)
    }
}

impl<'a, Ptr, T: ByteArrayType> FromIterator<&'a Option<Ptr>> for GenericByteArray<T>
where
    Ptr: AsRef<T::Native> + 'a,
{
    fn from_iter<I: IntoIterator<Item = &'a Option<Ptr>>>(iter: I) -> Self {
        iter.into_iter()
            .map(|o| o.as_ref().map(|p| p.as_ref()))
            .collect()
    }
}

impl<Ptr, T: ByteArrayType> FromIterator<Option<Ptr>> for GenericByteArray<T>
where
    Ptr: AsRef<T::Native>,
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = GenericByteBuilder::with_capacity(iter.size_hint().0, 1024);
        builder.extend(iter);
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{BinaryArray, StringArray};
    use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};

    #[test]
    fn try_new() {
        let data = Buffer::from_slice_ref("helloworld");
        let offsets = OffsetBuffer::new(vec![0, 5, 10].into());
        StringArray::new(offsets.clone(), data.clone(), None);

        let nulls = NullBuffer::new_null(3);
        let err =
            StringArray::try_new(offsets.clone(), data.clone(), Some(nulls.clone())).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Incorrect length of null buffer for StringArray, expected 2 got 3");

        let err = BinaryArray::try_new(offsets.clone(), data.clone(), Some(nulls)).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Incorrect length of null buffer for BinaryArray, expected 2 got 3");

        let non_utf8_data = Buffer::from_slice_ref(b"he\xFFloworld");
        let err = StringArray::try_new(offsets.clone(), non_utf8_data.clone(), None).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 2");

        BinaryArray::new(offsets, non_utf8_data, None);

        let offsets = OffsetBuffer::new(vec![0, 5, 11].into());
        let err = StringArray::try_new(offsets.clone(), data.clone(), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Offset of 11 exceeds length of values 10"
        );

        let err = BinaryArray::try_new(offsets.clone(), data, None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Maximum offset of 11 is larger than values of length 10"
        );

        let non_ascii_data = Buffer::from_slice_ref("heìloworld");
        StringArray::new(offsets.clone(), non_ascii_data.clone(), None);
        BinaryArray::new(offsets, non_ascii_data.clone(), None);

        let offsets = OffsetBuffer::new(vec![0, 3, 10].into());
        let err = StringArray::try_new(offsets.clone(), non_ascii_data.clone(), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Split UTF-8 codepoint at offset 3"
        );

        BinaryArray::new(offsets, non_ascii_data, None);
    }
}
