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

use crate::array::{empty_offsets, print_long_array};
use crate::builder::GenericByteBuilder;
use crate::iterator::ArrayIter;
use crate::raw_pointer::RawPtrBox;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::ByteArrayType;
use crate::{Array, ArrayAccessor, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use std::any::Any;

/// Generic struct for variable-size byte arrays
///
/// See [`StringArray`] and [`LargeStringArray`] for storing utf8 encoded string data
///
/// See [`BinaryArray`] and [`LargeBinaryArray`] for storing arbitrary bytes
///
/// [`StringArray`]: crate::StringArray
/// [`LargeStringArray`]: crate::LargeStringArray
/// [`BinaryArray`]: crate::BinaryArray
/// [`LargeBinaryArray`]: crate::LargeBinaryArray
pub struct GenericByteArray<T: ByteArrayType> {
    data: ArrayData,
    value_offsets: RawPtrBox<T::Offset>,
    value_data: RawPtrBox<u8>,
}

impl<T: ByteArrayType> Clone for GenericByteArray<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            value_offsets: self.value_offsets,
            value_data: self.value_data,
        }
    }
}

impl<T: ByteArrayType> GenericByteArray<T> {
    /// Data type of the array.
    pub const DATA_TYPE: DataType = T::DATA_TYPE;

    /// Returns the length for value at index `i`.
    /// # Panics
    /// Panics if index `i` is out of bounds.
    #[inline]
    pub fn value_length(&self, i: usize) -> T::Offset {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// Returns the raw value data
    pub fn value_data(&self) -> &[u8] {
        self.data.buffers()[1].as_slice()
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
        // Soundness
        //     pointer alignment & location is ensured by RawPtrBox
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.value_offsets.as_ptr().add(self.data.offset()),
                self.len() + 1,
            )
        }
    }


    /// Returns the raw bytes for the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn raw_value_unchecked(&self, i: usize) -> &[u8] {
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
        std::slice::from_raw_parts(
            self.value_data.as_ptr().offset(start.to_isize().unwrap()),
            (end - start).to_usize().unwrap(),
        )
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> &T::Native {
        let b = self.raw_value_unchecked(i);

        // SAFETY:
        // ArrayData is valid
        T::Native::from_bytes_unchecked(b)
    }

    /// Returns the raw data for the element at index `i`
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn raw_value(&self, i: usize) -> &[u8] {
        assert!(
            i < self.data.len(),
            "Trying to access an element at index {} from a {}{}Array of length {}",
            i,
            T::Offset::PREFIX,
            T::PREFIX,
            self.len()
        );
        // SAFETY:
        // Verified length above
        unsafe { self.raw_value_unchecked(i) }
    }

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn value(&self, i: usize) -> &T::Native {
        assert!(
            i < self.data.len(),
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

    /// Returns `GenericByteBuilder` of this byte array for mutating its values if the underlying
    /// offset and data buffers are not shared by others.
    pub fn into_builder(self) -> Result<GenericByteBuilder<T>, Self> {
        let len = self.len();
        let null_bit_buffer = self
            .data
            .null_buffer()
            .map(|b| b.bit_slice(self.data.offset(), len));

        let element_len = std::mem::size_of::<T::Offset>();
        let offset_buffer = self.data.buffers()[0]
            .slice_with_length(self.data.offset() * element_len, (len + 1) * element_len);

        let element_len = std::mem::size_of::<u8>();
        let value_len =
            T::Offset::as_usize(self.value_offsets()[len] - self.value_offsets()[0]);
        let value_buffer = self.data.buffers()[1]
            .slice_with_length(self.data.offset() * element_len, value_len * element_len);

        drop(self.data);

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

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
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
        // Handle case of empty offsets
        let offsets = match data.is_empty() && data.buffers()[0].is_empty() {
            true => empty_offsets::<T::Offset>().as_ptr() as *const _,
            false => data.buffers()[0].as_ptr(),
        };
        let values = data.buffers()[1].as_ptr();
        Self {
            data,
            // SAFETY:
            // ArrayData must be valid, and validated data type above
            value_offsets: unsafe { RawPtrBox::new(offsets) },
            value_data: unsafe { RawPtrBox::new(values) },
        }
    }
}

impl<T: ByteArrayType> From<GenericByteArray<T>> for ArrayData {
    fn from(array: GenericByteArray<T>) -> Self {
        array.data
    }
}

impl<'a, T: ByteArrayType> IntoIterator for &'a GenericByteArray<T> {
    type Item = Option<&'a T::Native>;
    type IntoIter = ArrayIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIter::new(self)
    }
}
