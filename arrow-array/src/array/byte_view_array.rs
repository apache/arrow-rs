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
use crate::builder::{ArrayBuilder, GenericByteViewBuilder};
use crate::iterator::ArrayIter;
use crate::types::bytes::ByteArrayNativeType;
use crate::types::{BinaryViewType, ByteViewType, StringViewType};
use crate::{Array, ArrayAccessor, ArrayRef, GenericByteArray, OffsetSizeTrait, Scalar};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder, ByteView};
use arrow_schema::{ArrowError, DataType};
use core::str;
use num::ToPrimitive;
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use super::ByteArrayType;

/// [Variable-size Binary View Layout]: An array of variable length bytes view arrays.
///
/// [Variable-size Binary View Layout]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
///
/// This is different from [`GenericByteArray`] as it stores both an offset and
/// length meaning that take / filter operations can be implemented without
/// copying the underlying data. In addition, it stores an inlined prefix which
/// can be used to speed up comparisons.
///
/// # See Also
///
/// * [`StringViewArray`] for storing utf8 encoded string data
/// * [`BinaryViewArray`] for storing bytes
/// * [`ByteView`] to interpret `u128`s layout of the views.
///
/// [`ByteView`]: arrow_data::ByteView
///
/// # Notes
///
/// Comparing two `GenericByteViewArray` using PartialEq compares by structure,
/// not by value. as there are many different buffer layouts to represent the
/// same data (e.g. different offsets, different buffer sizes, etc).
///
/// # Layout: "views" and buffers
///
/// A `GenericByteViewArray` stores variable length byte strings. An array of
/// `N` elements is stored as `N` fixed length "views" and a variable number
/// of variable length "buffers".
///
/// Each view is a `u128` value whose layout is different depending on the
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
/// * Strings with length <= 12 are stored directly in the view. See
///   [`Self::inline_value`] to access the inlined prefix from a short view.
///
/// * Strings with length > 12: The first four bytes are stored inline in the
///   view and the entire string is stored in one of the buffers. See [`ByteView`]
///   to access the fields of the these views.
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
/// [`ByteView`]: arrow_data::ByteView
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

// PartialEq
impl<T: ByteViewType + ?Sized> PartialEq for GenericByteViewArray<T> {
    fn eq(&self, other: &Self) -> bool {
        other.data_type.eq(&self.data_type)
            && other.views.eq(&self.views)
            && other.buffers.eq(&self.buffers)
            && other.nulls.eq(&self.nulls)
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

    /// Create a new [`Scalar`] from `value`
    pub fn new_scalar(value: impl AsRef<T::Native>) -> Scalar<Self> {
        Scalar::new(Self::from_iter_values(std::iter::once(value)))
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

    /// Returns the element at index `i` without bounds checking
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds
    /// of the array
    pub unsafe fn value_unchecked(&self, idx: usize) -> &T::Native {
        let v = self.views.get_unchecked(idx);
        let len = *v as u32;
        let b = if len <= 12 {
            Self::inline_value(v, len as usize)
        } else {
            let view = ByteView::from(*v);
            let data = self.buffers.get_unchecked(view.buffer_index as usize);
            let offset = view.offset as usize;
            data.get_unchecked(offset..offset + len as usize)
        };
        T::Native::from_bytes_unchecked(b)
    }

    /// Returns the first `len` bytes the inline value of the view.
    ///
    /// # Safety
    /// - The `view` must be a valid element from `Self::views()` that adheres to the view layout.
    /// - The `len` must be the length of the inlined value. It should never be larger than 12.
    #[inline(always)]
    pub unsafe fn inline_value(view: &u128, len: usize) -> &[u8] {
        debug_assert!(len <= 12);
        std::slice::from_raw_parts((view as *const u128 as *const u8).wrapping_add(4), len)
    }

    /// Constructs a new iterator for iterating over the values of this array
    pub fn iter(&self) -> ArrayIter<&Self> {
        ArrayIter::new(self)
    }

    /// Returns an iterator over the bytes of this array, including null values
    pub fn bytes_iter(&self) -> impl Iterator<Item = &[u8]> {
        self.views.iter().map(move |v| {
            let len = *v as u32;
            if len <= 12 {
                unsafe { Self::inline_value(v, len as usize) }
            } else {
                let view = ByteView::from(*v);
                let data = &self.buffers[view.buffer_index as usize];
                let offset = view.offset as usize;
                unsafe { data.get_unchecked(offset..offset + len as usize) }
            }
        })
    }

    /// Returns an iterator over the first `prefix_len` bytes of each array
    /// element, including null values.
    ///
    /// If `prefix_len` is larger than the element's length, the iterator will
    /// return an empty slice (`&[]`).
    pub fn prefix_bytes_iter(&self, prefix_len: usize) -> impl Iterator<Item = &[u8]> {
        self.views().into_iter().map(move |v| {
            let len = (*v as u32) as usize;

            if len < prefix_len {
                return &[] as &[u8];
            }

            if prefix_len <= 4 || len <= 12 {
                unsafe { StringViewArray::inline_value(v, prefix_len) }
            } else {
                let view = ByteView::from(*v);
                let data = unsafe {
                    self.data_buffers()
                        .get_unchecked(view.buffer_index as usize)
                };
                let offset = view.offset as usize;
                unsafe { data.get_unchecked(offset..offset + prefix_len) }
            }
        })
    }

    /// Returns an iterator over the last `suffix_len` bytes of each array
    /// element, including null values.
    ///
    /// Note that for [`StringViewArray`] the last bytes may start in the middle
    /// of a UTF-8 codepoint, and thus may not be a valid `&str`.
    ///
    /// If `suffix_len` is larger than the element's length, the iterator will
    /// return an empty slice (`&[]`).
    pub fn suffix_bytes_iter(&self, suffix_len: usize) -> impl Iterator<Item = &[u8]> {
        self.views().into_iter().map(move |v| {
            let len = (*v as u32) as usize;

            if len < suffix_len {
                return &[] as &[u8];
            }

            if len <= 12 {
                unsafe { &StringViewArray::inline_value(v, len)[len - suffix_len..] }
            } else {
                let view = ByteView::from(*v);
                let data = unsafe {
                    self.data_buffers()
                        .get_unchecked(view.buffer_index as usize)
                };
                let offset = view.offset as usize;
                unsafe { data.get_unchecked(offset + len - suffix_len..offset + len) }
            }
        })
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

    /// Returns a "compacted" version of this array
    ///
    /// The original array will *not* be modified
    ///
    /// # Garbage Collection
    ///
    /// Before GC:
    /// ```text
    ///                                        ┌──────┐                 
    ///                                        │......│                 
    ///                                        │......│                 
    /// ┌────────────────────┐       ┌ ─ ─ ─ ▶ │Data1 │   Large buffer  
    /// │       View 1       │─ ─ ─ ─          │......│  with data that
    /// ├────────────────────┤                 │......│ is not referred
    /// │       View 2       │─ ─ ─ ─ ─ ─ ─ ─▶ │Data2 │ to by View 1 or
    /// └────────────────────┘                 │......│      View 2     
    ///                                        │......│                 
    ///    2 views, refer to                   │......│                 
    ///   small portions of a                  └──────┘                 
    ///      large buffer                                               
    /// ```
    ///                                                                
    /// After GC:
    ///
    /// ```text
    /// ┌────────────────────┐                 ┌─────┐    After gc, only
    /// │       View 1       │─ ─ ─ ─ ─ ─ ─ ─▶ │Data1│     data that is  
    /// ├────────────────────┤       ┌ ─ ─ ─ ▶ │Data2│    pointed to by  
    /// │       View 2       │─ ─ ─ ─          └─────┘     the views is  
    /// └────────────────────┘                                 left      
    ///                                                                  
    ///                                                                  
    ///         2 views                                                  
    /// ```
    /// This method will compact the data buffers by recreating the view array and only include the data
    /// that is pointed to by the views.
    ///
    /// Note that it will copy the array regardless of whether the original array is compact.
    /// Use with caution as this can be an expensive operation, only use it when you are sure that the view
    /// array is significantly smaller than when it is originally created, e.g., after filtering or slicing.
    ///
    /// Note: this function does not attempt to canonicalize / deduplicate values. For this
    /// feature see  [`GenericByteViewBuilder::with_deduplicate_strings`].
    pub fn gc(&self) -> Self {
        let mut builder = GenericByteViewBuilder::<T>::with_capacity(self.len());

        for v in self.iter() {
            builder.append_option(v);
        }

        builder.finish()
    }

    /// Compare two [`GenericByteViewArray`] at index `left_idx` and `right_idx`
    ///
    /// Comparing two ByteView types are non-trivial.
    /// It takes a bit of patience to understand why we don't just compare two &[u8] directly.
    ///
    /// ByteView types give us the following two advantages, and we need to be careful not to lose them:
    /// (1) For string/byte smaller than 12 bytes, the entire data is inlined in the view.
    ///     Meaning that reading one array element requires only one memory access
    ///     (two memory access required for StringArray, one for offset buffer, the other for value buffer).
    ///
    /// (2) For string/byte larger than 12 bytes, we can still be faster than (for certain operations) StringArray/ByteArray,
    ///     thanks to the inlined 4 bytes.
    ///     Consider equality check:
    ///     If the first four bytes of the two strings are different, we can return false immediately (with just one memory access).
    ///
    /// If we directly compare two &[u8], we materialize the entire string (i.e., make multiple memory accesses), which might be unnecessary.
    /// - Most of the time (eq, ord), we only need to look at the first 4 bytes to know the answer,
    ///   e.g., if the inlined 4 bytes are different, we can directly return unequal without looking at the full string.
    ///
    /// # Order check flow
    /// (1) if both string are smaller than 12 bytes, we can directly compare the data inlined to the view.
    /// (2) if any of the string is larger than 12 bytes, we need to compare the full string.
    ///     (2.1) if the inlined 4 bytes are different, we can return the result immediately.
    ///     (2.2) o.w., we need to compare the full string.
    ///
    /// # Safety
    /// The left/right_idx must within range of each array
    pub unsafe fn compare_unchecked(
        left: &GenericByteViewArray<T>,
        left_idx: usize,
        right: &GenericByteViewArray<T>,
        right_idx: usize,
    ) -> std::cmp::Ordering {
        let l_view = left.views().get_unchecked(left_idx);
        let l_len = *l_view as u32;

        let r_view = right.views().get_unchecked(right_idx);
        let r_len = *r_view as u32;

        if l_len <= 12 && r_len <= 12 {
            let l_data = unsafe { GenericByteViewArray::<T>::inline_value(l_view, l_len as usize) };
            let r_data = unsafe { GenericByteViewArray::<T>::inline_value(r_view, r_len as usize) };
            return l_data.cmp(r_data);
        }

        // one of the string is larger than 12 bytes,
        // we then try to compare the inlined data first
        let l_inlined_data = unsafe { GenericByteViewArray::<T>::inline_value(l_view, 4) };
        let r_inlined_data = unsafe { GenericByteViewArray::<T>::inline_value(r_view, 4) };
        if r_inlined_data != l_inlined_data {
            return l_inlined_data.cmp(r_inlined_data);
        }

        // unfortunately, we need to compare the full data
        let l_full_data: &[u8] = unsafe { left.value_unchecked(left_idx).as_ref() };
        let r_full_data: &[u8] = unsafe { right.value_unchecked(right_idx).as_ref() };

        l_full_data.cmp(r_full_data)
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

/// Efficiently convert a [`GenericByteArray`] to a [`GenericByteViewArray`]
///
/// For example this method can convert a [`StringArray`] to a
/// [`StringViewArray`].
///
/// If the offsets are all less than u32::MAX, the new [`GenericByteViewArray`]
/// is built without copying the underlying string data (views are created
/// directly into the existing buffer)
///
/// [`StringArray`]: crate::StringArray
impl<FROM, V> From<&GenericByteArray<FROM>> for GenericByteViewArray<V>
where
    FROM: ByteArrayType,
    FROM::Offset: OffsetSizeTrait + ToPrimitive,
    V: ByteViewType<Native = FROM::Native>,
{
    fn from(byte_array: &GenericByteArray<FROM>) -> Self {
        let offsets = byte_array.offsets();

        let can_reuse_buffer = match offsets.last() {
            Some(offset) => offset.as_usize() < u32::MAX as usize,
            None => true,
        };

        if can_reuse_buffer {
            // build views directly pointing to the existing buffer
            let len = byte_array.len();
            let mut views_builder = GenericByteViewBuilder::<V>::with_capacity(len);
            let str_values_buf = byte_array.values().clone();
            let block = views_builder.append_block(str_values_buf);
            for (i, w) in offsets.windows(2).enumerate() {
                let offset = w[0].as_usize();
                let end = w[1].as_usize();
                let length = end - offset;

                if byte_array.is_null(i) {
                    views_builder.append_null();
                } else {
                    // Safety: the input was a valid array so it valid UTF8 (if string). And
                    // all offsets were valid
                    unsafe {
                        views_builder.append_view_unchecked(block, offset as u32, length as u32)
                    }
                }
            }
            assert_eq!(views_builder.len(), len);
            views_builder.finish()
        } else {
            // Otherwise, create a new buffer for large strings
            // TODO: the original buffer could still be used
            // by making multiple slices of u32::MAX length
            GenericByteViewArray::<V>::from_iter(byte_array.iter())
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

impl<'a, Ptr, T> FromIterator<&'a Option<Ptr>> for GenericByteViewArray<T>
where
    Ptr: AsRef<T::Native> + 'a,
    T: ByteViewType + ?Sized,
{
    fn from_iter<I: IntoIterator<Item = &'a Option<Ptr>>>(iter: I) -> Self {
        iter.into_iter()
            .map(|o| o.as_ref().map(|p| p.as_ref()))
            .collect()
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

impl BinaryViewArray {
    /// Convert the [`BinaryViewArray`] to [`StringViewArray`]
    /// If items not utf8 data, validate will fail and error returned.
    pub fn to_string_view(self) -> Result<StringViewArray, ArrowError> {
        StringViewType::validate(self.views(), self.data_buffers())?;
        unsafe { Ok(self.to_string_view_unchecked()) }
    }

    /// Convert the [`BinaryViewArray`] to [`StringViewArray`]
    /// # Safety
    /// Caller is responsible for ensuring that items in array are utf8 data.
    pub unsafe fn to_string_view_unchecked(self) -> StringViewArray {
        StringViewArray::new_unchecked(self.views, self.buffers, self.nulls)
    }
}

impl From<Vec<&[u8]>> for BinaryViewArray {
    fn from(v: Vec<&[u8]>) -> Self {
        Self::from_iter_values(v)
    }
}

impl From<Vec<Option<&[u8]>>> for BinaryViewArray {
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        v.into_iter().collect()
    }
}

/// A [`GenericByteViewArray`] that stores utf8 data
///
/// # Example
/// ```
/// use arrow_array::StringViewArray;
/// let array = StringViewArray::from_iter_values(vec!["hello", "world", "lulu", "large payload over 12 bytes"]);
/// assert_eq!(array.value(0), "hello");
/// assert_eq!(array.value(3), "large payload over 12 bytes");
/// ```
pub type StringViewArray = GenericByteViewArray<StringViewType>;

impl StringViewArray {
    /// Convert the [`StringViewArray`] to [`BinaryViewArray`]
    pub fn to_binary_view(self) -> BinaryViewArray {
        unsafe { BinaryViewArray::new_unchecked(self.views, self.buffers, self.nulls) }
    }

    /// Returns true if all data within this array is ASCII
    pub fn is_ascii(&self) -> bool {
        // Alternative (but incorrect): directly check the underlying buffers
        // (1) Our string view might be sparse, i.e., a subset of the buffers,
        //      so even if the buffer is not ascii, we can still be ascii.
        // (2) It is quite difficult to know the range of each buffer (unlike StringArray)
        // This means that this operation is quite expensive, shall we cache the result?
        //  i.e. track `is_ascii` in the builder.
        self.iter().all(|v| match v {
            Some(v) => v.is_ascii(),
            None => true,
        })
    }
}

impl From<Vec<&str>> for StringViewArray {
    fn from(v: Vec<&str>) -> Self {
        Self::from_iter_values(v)
    }
}

impl From<Vec<Option<&str>>> for StringViewArray {
    fn from(v: Vec<Option<&str>>) -> Self {
        v.into_iter().collect()
    }
}

impl From<Vec<String>> for StringViewArray {
    fn from(v: Vec<String>) -> Self {
        Self::from_iter_values(v)
    }
}

impl From<Vec<Option<String>>> for StringViewArray {
    fn from(v: Vec<Option<String>>) -> Self {
        v.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::{BinaryViewBuilder, StringViewBuilder};
    use crate::{Array, BinaryViewArray, StringViewArray};
    use arrow_buffer::{Buffer, ScalarBuffer};
    use arrow_data::ByteView;

    #[test]
    fn try_new_string() {
        let array = StringViewArray::from_iter_values(vec![
            "hello",
            "world",
            "lulu",
            "large payload over 12 bytes",
        ]);
        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(3), "large payload over 12 bytes");
    }

    #[test]
    fn try_new_binary() {
        let array = BinaryViewArray::from_iter_values(vec![
            b"hello".as_slice(),
            b"world".as_slice(),
            b"lulu".as_slice(),
            b"large payload over 12 bytes".as_slice(),
        ]);
        assert_eq!(array.value(0), b"hello");
        assert_eq!(array.value(3), b"large payload over 12 bytes");
    }

    #[test]
    fn try_new_empty_string() {
        // test empty array
        let array = {
            let mut builder = StringViewBuilder::new();
            builder.finish()
        };
        assert!(array.is_empty());
    }

    #[test]
    fn try_new_empty_binary() {
        // test empty array
        let array = {
            let mut builder = BinaryViewBuilder::new();
            builder.finish()
        };
        assert!(array.is_empty());
    }

    #[test]
    fn test_append_string() {
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
    }

    #[test]
    fn test_append_binary() {
        // test builder append
        let array = {
            let mut builder = BinaryViewBuilder::new();
            builder.append_value(b"hello");
            builder.append_null();
            builder.append_option(Some(b"large payload over 12 bytes"));
            builder.finish()
        };
        assert_eq!(array.value(0), b"hello");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), b"large payload over 12 bytes");
    }

    #[test]
    fn test_in_progress_recreation() {
        let array = {
            // make a builder with small block size.
            let mut builder = StringViewBuilder::new().with_fixed_block_size(14);
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
        let view = ByteView::new(13, &v.as_bytes()[0..4])
            .with_buffer_index(3)
            .with_offset(1);
        let views = ScalarBuffer::from(vec![view.into()]);
        let buffers = vec![Buffer::from_slice_ref(v)];
        StringViewArray::new(views, buffers, None);
    }

    #[test]
    #[should_panic(
        expected = "Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 0"
    )]
    fn new_with_invalid_utf8_data() {
        let v: Vec<u8> = vec![
            // invalid UTF8
            0xf0, 0x80, 0x80, 0x80, // more bytes to make it larger than 12
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let view = ByteView::new(v.len() as u32, &v[0..4]);
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

    #[test]
    fn test_gc() {
        let test_data = [
            Some("longer than 12 bytes"),
            Some("short"),
            Some("t"),
            Some("longer than 12 bytes"),
            None,
            Some("short"),
        ];

        let array = {
            let mut builder = StringViewBuilder::new().with_fixed_block_size(8); // create multiple buffers
            test_data.into_iter().for_each(|v| builder.append_option(v));
            builder.finish()
        };
        assert!(array.buffers.len() > 1);

        fn check_gc(to_test: &StringViewArray) {
            let gc = to_test.gc();
            assert_ne!(to_test.data_buffers().len(), gc.data_buffers().len());

            to_test.iter().zip(gc.iter()).for_each(|(a, b)| {
                assert_eq!(a, b);
            });
            assert_eq!(to_test.len(), gc.len());
        }

        check_gc(&array);
        check_gc(&array.slice(1, 3));
        check_gc(&array.slice(2, 1));
        check_gc(&array.slice(2, 2));
        check_gc(&array.slice(3, 1));
    }

    #[test]
    fn test_eq() {
        let test_data = [
            Some("longer than 12 bytes"),
            None,
            Some("short"),
            Some("again, this is longer than 12 bytes"),
        ];

        let array1 = {
            let mut builder = StringViewBuilder::new().with_fixed_block_size(8);
            test_data.into_iter().for_each(|v| builder.append_option(v));
            builder.finish()
        };
        let array2 = {
            // create a new array with the same data but different layout
            let mut builder = StringViewBuilder::new().with_fixed_block_size(100);
            test_data.into_iter().for_each(|v| builder.append_option(v));
            builder.finish()
        };
        assert_eq!(array1, array1.clone());
        assert_eq!(array2, array2.clone());
        assert_ne!(array1, array2);
    }
}
