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
use arrow_data::{ArrayData, ArrayDataBuilder, ByteView};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

/// [Variable-size Binary View Layout]: An array of variable length bytes view arrays.
///
/// Different than [`crate::GenericByteArray`] as it stores both an offset and length
/// meaning that take / filter operations can be implemented without copying the underlying data.
///
/// See [`StringViewArray`] for storing utf8 encoded string data and
/// [`BinaryViewArray`] for storing bytes.
///
/// [Variable-size Binary View Layout]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
///
/// A `GenericByteViewArray` stores variable length byte strings. An array of
/// `N` elements is stored as `N` fixed length "views" and a variable number
/// of variable length "buffers".
///
/// Each view is a `u128` value  layout is different depending on the
/// length of the string stored at that location:
///
/// ```text
///                         ┌──────┬────────────────────────┐
///                         │length│      string value      │
///    Strings (len <= 12)  │      │    (padded with 0)     │
///                         └──────┴────────────────────────┘
///                          0    31                      127
///
///                         ┌───────┬───────┬───────┬───────┐
///                         │length │prefix │  buf  │offset │
///    Strings (len > 12)   │       │       │ index │       │
///                         └───────┴───────┴───────┴───────┘
///                          0    31       63      95    127
/// ```
///
/// * Strings with length <= 12 are stored directly in the view.
///
/// * Strings with length > 12: The first four bytes are stored inline in the
/// view and the entire string is stored in one of the buffers.
///
/// Unlike [`GenericByteArray`], there are no constraints on the offsets other
/// than they must point into a valid buffer. However, they can be out of order,
/// non continuous and overlapping.
///
/// For example, in the following diagram, the strings "FishWasInTownToday" and
/// "CrumpleFacedFish" are both longer than 12 bytes and thus are stored in a
/// separate buffer while the string "LavaMonster" is stored inlined in the
/// view. In this case, the same bytes for "Fish" are used to store both strings.
///
/// ```text
///                                                                            ┌───┐
///                         ┌──────┬──────┬──────┬──────┐               offset │...│
/// "FishWasInTownTodayYay" │  21  │ Fish │  0   │ 115  │─ ─              103  │Mr.│
///                         └──────┴──────┴──────┴──────┘   │      ┌ ─ ─ ─ ─ ▶ │Cru│
///                         ┌──────┬──────┬──────┬──────┐                      │mpl│
/// "CrumpleFacedFish"      │  16  │ Crum │  0   │ 103  │─ ─│─ ─ ─ ┘           │eFa│
///                         └──────┴──────┴──────┴──────┘                      │ced│
///                         ┌──────┬────────────────────┐   └ ─ ─ ─ ─ ─ ─ ─ ─ ▶│Fis│
/// "LavaMonster"           │  11  │   LavaMonster\0    │                      │hWa│
///                         └──────┴────────────────────┘               offset │sIn│
///                                                                       115  │Tow│
///                                                                            │nTo│
///                                                                            │day│
///                                  u128 "views"                              │Yay│
///                                                                   buffer 0 │...│
///                                                                            └───┘
/// ```
/// [`GenericByteArray`]: crate::array::GenericByteArray
pub struct GenericByteViewArray<T: ByteViewType + ?Sized> {
    data_type: DataType,
    views: ScalarBuffer<u128>,
    buffers: Vec<Buffer>,
    phantom: PhantomData<T>,
    nulls: Option<NullBuffer>,
}

impl<T: ByteViewType + ?Sized> Clone for GenericByteViewArray<T> {
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

impl<T: ByteViewType + ?Sized> GenericByteViewArray<T> {
    /// Create a new [`GenericByteViewArray`] from the provided parts, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`GenericByteViewArray::try_new`] returns an error
    pub fn new(views: ScalarBuffer<u128>, buffers: Vec<Buffer>, nulls: Option<NullBuffer>) -> Self {
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
            views,
            buffers,
            nulls,
            phantom: Default::default(),
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
            let view = ByteView::from(*v);
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

impl<T: ByteViewType + ?Sized> Debug for GenericByteViewArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}ViewArray\n[\n", T::PREFIX)?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<T: ByteViewType + ?Sized> Array for GenericByteViewArray<T> {
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

impl<'a, T: ByteViewType + ?Sized> ArrayAccessor for &'a GenericByteViewArray<T> {
    type Item = &'a T::Native;

    fn value(&self, index: usize) -> Self::Item {
        GenericByteViewArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericByteViewArray::value_unchecked(self, index)
    }
}

impl<'a, T: ByteViewType + ?Sized> IntoIterator for &'a GenericByteViewArray<T> {
    type Item = Option<&'a T::Native>;
    type IntoIter = ArrayIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIter::new(self)
    }
}

impl<T: ByteViewType + ?Sized> From<ArrayData> for GenericByteViewArray<T> {
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

impl<T: ByteViewType + ?Sized> From<GenericByteViewArray<T>> for ArrayData {
    fn from(mut array: GenericByteViewArray<T>) -> Self {
        let len = array.len();
        array.buffers.insert(0, array.views.into_inner());
        let builder = ArrayDataBuilder::new(T::DATA_TYPE)
            .len(len)
            .buffers(array.buffers)
            .nulls(array.nulls);

        unsafe { builder.build_unchecked() }
    }
}

impl<Ptr, T: ByteViewType + ?Sized> FromIterator<Option<Ptr>> for GenericByteViewArray<T>
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

/// A [`GenericByteViewArray`] of `[u8]`
///
/// # Example
/// ```
/// use arrow_array::BinaryViewArray;
/// let array = BinaryViewArray::from_iter_values(vec![b"hello" as &[u8], b"world", b"lulu", b"large payload over 12 bytes"]);
/// assert_eq!(array.value(0), b"hello");
/// assert_eq!(array.value(3), b"large payload over 12 bytes");
/// ```
pub type BinaryViewArray = GenericByteViewArray<BinaryViewType>;

/// A [`GenericByteViewArray`] that stores uf8 data
///
/// # Example
/// ```
/// use arrow_array::StringViewArray;
/// let array = StringViewArray::from_iter_values(vec!["hello", "world", "lulu", "large payload over 12 bytes"]);
/// assert_eq!(array.value(0), "hello");
/// assert_eq!(array.value(3), "large payload over 12 bytes");
/// ```
pub type StringViewArray = GenericByteViewArray<StringViewType>;

impl From<Vec<&str>> for StringViewArray {
    fn from(v: Vec<&str>) -> Self {
        Self::from_iter_values(v)
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::StringViewBuilder;
    use crate::{Array, BinaryViewArray, StringViewArray};
    use arrow_buffer::{Buffer, ScalarBuffer};
    use arrow_data::ByteView;

    #[test]
    fn try_new() {
        let array = StringViewArray::from_iter_values(vec![
            "hello",
            "world",
            "lulu",
            "large payload over 12 bytes",
        ]);
        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(3), "large payload over 12 bytes");

        let array = BinaryViewArray::from_iter_values(vec![
            b"hello".as_slice(),
            b"world".as_slice(),
            b"lulu".as_slice(),
            b"large payload over 12 bytes".as_slice(),
        ]);
        assert_eq!(array.value(0), b"hello");
        assert_eq!(array.value(3), b"large payload over 12 bytes");

        // test empty array
        let array = {
            let mut builder = StringViewBuilder::new();
            builder.finish()
        };
        assert!(array.is_empty());

        // test builder append
        let array = {
            let mut builder = StringViewBuilder::new();
            builder.append_value("hello");
            builder.append_null();
            builder.append_option(Some("large payload over 12 bytes"));
            builder.finish()
        };
        assert_eq!(array.value(0), "hello");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "large payload over 12 bytes");

        // test builder's in_progress re-created
        let array = {
            // make a builder with small block size.
            let mut builder = StringViewBuilder::new().with_block_size(14);
            builder.append_value("large payload over 12 bytes");
            builder.append_option(Some("another large payload over 12 bytes that double than the first one, so that we can trigger the in_progress in builder re-created"));
            builder.finish()
        };
        assert_eq!(array.value(0), "large payload over 12 bytes");
        assert_eq!(array.value(1), "another large payload over 12 bytes that double than the first one, so that we can trigger the in_progress in builder re-created");
        assert_eq!(2, array.buffers.len());
    }

    #[test]
    #[should_panic(expected = "Invalid buffer index at 0: got index 3 but only has 1 buffers")]
    fn new_with_invalid_view_data() {
        let v = "large payload over 12 bytes";
        let view = ByteView {
            length: 13,
            prefix: u32::from_le_bytes(v.as_bytes()[0..4].try_into().unwrap()),
            buffer_index: 3,
            offset: 1,
        };
        let views = ScalarBuffer::from(vec![view.into()]);
        let buffers = vec![Buffer::from_slice_ref(v)];
        StringViewArray::new(views, buffers, None);
    }

    #[test]
    #[should_panic(
        expected = "Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 0"
    )]
    fn new_with_invalid_utf8_data() {
        let v: Vec<u8> = vec![0xf0, 0x80, 0x80, 0x80];
        let view = ByteView {
            length: v.len() as u32,
            prefix: u32::from_le_bytes(v[0..4].try_into().unwrap()),
            buffer_index: 0,
            offset: 0,
        };
        let views = ScalarBuffer::from(vec![view.into()]);
        let buffers = vec![Buffer::from_slice_ref(v)];
        StringViewArray::new(views, buffers, None);
    }

    #[test]
    #[should_panic(expected = "View at index 0 contained non-zero padding for string of length 1")]
    fn new_with_invalid_zero_padding() {
        let mut data = [0; 12];
        data[0] = b'H';
        data[11] = 1; // no zero padding

        let mut view_buffer = [0; 16];
        view_buffer[0..4].copy_from_slice(&1u32.to_le_bytes());
        view_buffer[4..].copy_from_slice(&data);

        let view = ByteView::from(u128::from_le_bytes(view_buffer));
        let views = ScalarBuffer::from(vec![view.into()]);
        let buffers = vec![];
        StringViewArray::new(views, buffers, None);
    }

    #[test]
    #[should_panic(expected = "Mismatch between embedded prefix and data")]
    fn test_mismatch_between_embedded_prefix_and_data() {
        let input_str_1 = "Hello, Rustaceans!";
        let input_str_2 = "Hallo, Rustaceans!";
        let length = input_str_1.len() as u32;
        assert!(input_str_1.len() > 12);

        let mut view_buffer = [0; 16];
        view_buffer[0..4].copy_from_slice(&length.to_le_bytes());
        view_buffer[4..8].copy_from_slice(&input_str_1.as_bytes()[0..4]);
        view_buffer[8..12].copy_from_slice(&0u32.to_le_bytes());
        view_buffer[12..].copy_from_slice(&0u32.to_le_bytes());
        let view = ByteView::from(u128::from_le_bytes(view_buffer));
        let views = ScalarBuffer::from(vec![view.into()]);
        let buffers = vec![Buffer::from_slice_ref(input_str_2.as_bytes())];

        StringViewArray::new(views, buffers, None);
    }
}
