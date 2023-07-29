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

use crate::array::print_long_array;
use crate::builder::GenericByteViewBuilder;
use crate::iterator::ArrayIter;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::{BinaryViewType, ByteViewType, StringViewType};
use crate::{Array, ArrayAccessor, ArrayRef};
use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow_data::view::View;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

/// An array of variable length byte view arrays
pub struct GenericByteViewArray<T: ByteViewType> {
    data_type: DataType,
    views: ScalarBuffer<u128>,
    buffers: Vec<Buffer>,
    nulls: Option<NullBuffer>,
    phantom: PhantomData<T>,
}

impl<T: ByteViewType> Clone for GenericByteViewArray<T> {
    fn clone(&self) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            views: self.views.clone(),
            buffers: self.buffers.clone(),
            nulls: self.nulls.clone(),
            phantom: Default::default(),
        }
    }
}

impl<T: ByteViewType> GenericByteViewArray<T> {
    /// Create a new [`GenericByteViewArray`] from the provided parts, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`GenericByteViewArray::try_new`] returns an error
    pub fn new(
        views: ScalarBuffer<u128>,
        buffers: Vec<Buffer>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self::try_new(views, buffers, nulls).unwrap()
    }

    /// Create a new [`GenericByteViewArray`] from the provided parts, returning an error on failure
    ///
    /// # Errors
    ///
    /// * `views.len() != nulls.len()`
    /// * [ByteViewType::validate] fails
    pub fn try_new(
        views: ScalarBuffer<u128>,
        buffers: Vec<Buffer>,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        // Verify data is valid
        T::validate(&views, &buffers)?;

        if let Some(n) = nulls.as_ref() {
            if n.len() != views.len() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for {}ViewArray, expected {} got {}",
                    T::PREFIX,
                    views.len(),
                    n.len(),
                )));
            }
        }

        Ok(Self {
            data_type: T::DATA_TYPE,
            phantom: Default::default(),
            views,
            buffers,
            nulls,
        })
    }

    /// Create a new [`GenericByteViewArray`] from the provided parts, without validation
    ///
    /// # Safety
    ///
    /// Safe if [`Self::try_new`] would not error
    pub unsafe fn new_unchecked(
        views: ScalarBuffer<u128>,
        buffers: Vec<Buffer>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            phantom: Default::default(),
            views,
            buffers,
            nulls,
        }
    }

    /// Create a new [`GenericByteViewArray`] of length `len` where all values are null
    pub fn new_null(len: usize) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            views: vec![0; len].into(),
            buffers: vec![],
            nulls: Some(NullBuffer::new_null(len)),
            phantom: Default::default(),
        }
    }

    /// Creates a [`GenericByteViewArray`] based on an iterator of values without nulls
    pub fn from_iter_values<Ptr, I>(iter: I) -> Self
    where
        Ptr: AsRef<T::Native>,
        I: IntoIterator<Item = Ptr>,
    {
        let iter = iter.into_iter();
        let mut builder = GenericByteViewBuilder::<T>::with_capacity(iter.size_hint().0);
        for v in iter {
            builder.append_value(v);
        }
        builder.finish()
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (ScalarBuffer<u128>, Vec<Buffer>, Option<NullBuffer>) {
        (self.views, self.buffers, self.nulls)
    }

    /// Returns the views buffer
    #[inline]
    pub fn views(&self) -> &ScalarBuffer<u128> {
        &self.views
    }

    /// Returns the buffers storing string data
    #[inline]
    pub fn data_buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn value(&self, i: usize) -> &T::Native {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a {}ViewArray of length {}",
            i,
            T::PREFIX,
            self.len()
        );

        assert!(i < self.views.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, idx: usize) -> &T::Native {
        let v = self.views.get_unchecked(idx);
        let len = *v as u32;
        let b = if len <= 12 {
            let ptr = self.views.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr.add(idx * 16 + 4), len as usize)
        } else {
            let view = View::from(*v);
            let data = self.buffers.get_unchecked(view.buffer_index as usize);
            let offset = view.offset as usize;
            data.get_unchecked(offset..offset + len as usize)
        };
        T::Native::from_bytes_unchecked(b)
    }

    /// constructs a new iterator
    pub fn iter(&self) -> ArrayIter<&Self> {
        ArrayIter::new(self)
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            views: self.views.slice(offset, length),
            buffers: self.buffers.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
            phantom: Default::default(),
        }
    }
}

impl<T: ByteViewType> std::fmt::Debug for GenericByteViewArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}ViewArray\n[\n", T::PREFIX)?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<T: ByteViewType> Array for GenericByteViewArray<T> {
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
        self.views.len()
    }

    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut sum = self.buffers.iter().map(|b| b.capacity()).sum::<usize>();
        sum += self.views.inner().capacity();
        if let Some(x) = &self.nulls {
            sum += x.buffer().capacity()
        }
        sum
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.get_buffer_memory_size()
    }
}

impl<'a, T: ByteViewType> ArrayAccessor for &'a GenericByteViewArray<T> {
    type Item = &'a T::Native;

    fn value(&self, index: usize) -> Self::Item {
        GenericByteViewArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericByteViewArray::value_unchecked(self, index)
    }
}

impl<'a, T: ByteViewType> IntoIterator for &'a GenericByteViewArray<T> {
    type Item = Option<&'a T::Native>;
    type IntoIter = ArrayIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIter::new(self)
    }
}

impl<T: ByteViewType> From<ArrayData> for GenericByteViewArray<T> {
    fn from(value: ArrayData) -> Self {
        let views = value.buffers()[0].clone();
        let views = ScalarBuffer::new(views, value.offset(), value.len());
        let buffers = value.buffers()[1..].to_vec();
        Self {
            data_type: T::DATA_TYPE,
            views,
            buffers,
            nulls: value.nulls().cloned(),
            phantom: Default::default(),
        }
    }
}

impl<T: ByteViewType> From<GenericByteViewArray<T>> for ArrayData {
    fn from(mut array: GenericByteViewArray<T>) -> Self {
        let len = array.len();
        array.buffers.insert(0, array.views.into_inner());
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .buffers(array.buffers)
            .nulls(array.nulls);

        unsafe { builder.build_unchecked() }
    }
}

impl<Ptr, T: ByteViewType> FromIterator<Option<Ptr>> for GenericByteViewArray<T>
where
    Ptr: AsRef<T::Native>,
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = GenericByteViewBuilder::<T>::with_capacity(iter.size_hint().0);
        builder.extend(iter);
        builder.finish()
    }
}

/// A [`GenericByteViewArray`] of `str`
///
/// ```
/// # use arrow_array::StringViewArray;
/// let array = StringViewArray::from_iter_values(vec!["hello", "world", "foo", "large payload over 12 bytes"]);
/// assert_eq!(array.value(0), "hello");
/// assert_eq!(array.value(3), "large payload over 12 bytes");
/// ```
pub type StringViewArray = GenericByteViewArray<StringViewType>;

/// A [`GenericByteViewArray`] of `[u8]`
pub type BinaryViewArray = GenericByteViewArray<BinaryViewType>;
