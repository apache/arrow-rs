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
use arrow_data::{ArrayData, ArrayDataBuilder, ByteView, MAX_INLINE_VIEW_LEN};
use arrow_schema::{ArrowError, DataType};
use core::str;
use num_traits::ToPrimitive;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use super::ByteArrayType;

/// [Variable-size Binary View Layout]: An array of variable length bytes views.
///
/// This array type is used to store variable length byte data (e.g. Strings, Binary)
/// and has efficient operations such as `take`, `filter`, and comparison.
///
/// [Variable-size Binary View Layout]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
///
/// This is different from [`GenericByteArray`], which also stores variable
/// length byte data, as it represents strings with an offset and length. `take`
/// and `filter` like operations are implemented by manipulating the "views"
/// (`u128`) without modifying the bytes. Each view also stores an inlined
/// prefix which speed up comparisons.
///
/// # See Also
///
/// * [`StringViewArray`] for storing utf8 encoded string data
/// * [`BinaryViewArray`] for storing bytes
/// * [`ByteView`] to interpret `u128`s layout of the views.
///
/// [`ByteView`]: arrow_data::ByteView
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
/// * Strings with length <= 12 ([`MAX_INLINE_VIEW_LEN`]) are stored directly in
///   the view. See [`Self::inline_value`] to access the inlined prefix from a
///   short view.
///
/// * Strings with length > 12: The first four bytes are stored inline in the
///   view and the entire string is stored in one of the buffers. See [`ByteView`]
///   to access the fields of the these views.
///
/// As with other arrays, the optimized kernels in [`arrow_compute`] are likely
/// the easiest and fastest way to work with this data. However, it is possible
/// to access the views and buffers directly for more control.
///
/// For example
///
/// ```rust
/// # use arrow_array::StringViewArray;
/// # use arrow_array::Array;
/// use arrow_data::ByteView;
/// let array = StringViewArray::from(vec![
///   "hello",
///   "this string is longer than 12 bytes",
///   "this string is also longer than 12 bytes"
/// ]);
///
/// // ** Examine the first view (short string) **
/// assert!(array.is_valid(0)); // Check for nulls
/// let short_view: u128 = array.views()[0]; // "hello"
/// // get length of the string
/// let len = short_view as u32;
/// assert_eq!(len, 5); // strings less than 12 bytes are stored in the view
/// // SAFETY: `view` is a valid view
/// let value = unsafe {
///   StringViewArray::inline_value(&short_view, len as usize)
/// };
/// assert_eq!(value, b"hello");
///
/// // ** Examine the third view (long string) **
/// assert!(array.is_valid(12)); // Check for nulls
/// let long_view: u128 = array.views()[2]; // "this string is also longer than 12 bytes"
/// let len = long_view as u32;
/// assert_eq!(len, 40); // strings longer than 12 bytes are stored in the buffer
/// let view = ByteView::from(long_view); // use ByteView to access the fields
/// assert_eq!(view.length, 40);
/// assert_eq!(view.buffer_index, 0);
/// assert_eq!(view.offset, 35); // data starts after the first long string
/// // Views for long strings store a 4 byte prefix
/// let prefix = view.prefix.to_le_bytes();
/// assert_eq!(&prefix, b"this");
/// let value = array.value(2); // get the string value (see `value` implementation for how to access the bytes directly)
/// assert_eq!(value, "this string is also longer than 12 bytes");
/// ```
///
/// [`MAX_INLINE_VIEW_LEN`]: arrow_data::MAX_INLINE_VIEW_LEN
/// [`arrow_compute`]: https://docs.rs/arrow/latest/arrow/compute/index.html
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
/// "LavaMonster"           │  11  │   LavaMonster      │                      │hWa│
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
        if cfg!(feature = "force_validate") {
            return Self::new(views, buffers, nulls);
        }

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
    ///
    /// Note: This method does not check for nulls and the value is arbitrary
    /// (but still well-defined) if [`is_null`](Self::is_null) returns true for the index.
    ///
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
    /// Note: This method does not check for nulls and the value is arbitrary
    /// if [`is_null`](Self::is_null) returns true for the index.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds
    /// of the array
    pub unsafe fn value_unchecked(&self, idx: usize) -> &T::Native {
        let v = unsafe { self.views.get_unchecked(idx) };
        let len = *v as u32;
        let b = if len <= MAX_INLINE_VIEW_LEN {
            unsafe { Self::inline_value(v, len as usize) }
        } else {
            let view = ByteView::from(*v);
            let data = unsafe { self.buffers.get_unchecked(view.buffer_index as usize) };
            let offset = view.offset as usize;
            unsafe { data.get_unchecked(offset..offset + len as usize) }
        };
        unsafe { T::Native::from_bytes_unchecked(b) }
    }

    /// Returns the first `len` bytes the inline value of the view.
    ///
    /// # Safety
    /// - The `view` must be a valid element from `Self::views()` that adheres to the view layout.
    /// - The `len` must be the length of the inlined value. It should never be larger than [`MAX_INLINE_VIEW_LEN`].
    #[inline(always)]
    pub unsafe fn inline_value(view: &u128, len: usize) -> &[u8] {
        debug_assert!(len <= MAX_INLINE_VIEW_LEN as usize);
        unsafe {
            std::slice::from_raw_parts((view as *const u128 as *const u8).wrapping_add(4), len)
        }
    }

    /// Constructs a new iterator for iterating over the values of this array
    pub fn iter(&self) -> ArrayIter<&Self> {
        ArrayIter::new(self)
    }

    /// Returns an iterator over the bytes of this array, including null values
    pub fn bytes_iter(&self) -> impl Iterator<Item = &[u8]> {
        self.views.iter().map(move |v| {
            let len = *v as u32;
            if len <= MAX_INLINE_VIEW_LEN {
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

            if prefix_len <= 4 || len as u32 <= MAX_INLINE_VIEW_LEN {
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

            if len as u32 <= MAX_INLINE_VIEW_LEN {
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
        // 1) Read basic properties once
        let len = self.len(); // number of elements
        let nulls = self.nulls().cloned(); // reuse & clone existing null bitmap

        // 1.5) Fast path: if there are no buffers, just reuse original views and no data blocks
        if self.data_buffers().is_empty() {
            return unsafe {
                GenericByteViewArray::new_unchecked(
                    self.views().clone(),
                    vec![], // empty data blocks
                    nulls,
                )
            };
        }

        // 2) Calculate total size of all non-inline data and detect if any exists
        let total_large = self.total_buffer_bytes_used();

        // 2.5) Fast path: if there is no non-inline data, avoid buffer allocation & processing
        if total_large == 0 {
            // Views are inline-only or all null; just reuse original views and no data blocks
            return unsafe {
                GenericByteViewArray::new_unchecked(
                    self.views().clone(),
                    vec![], // empty data blocks
                    nulls,
                )
            };
        }

        let (views_buf, data_blocks) = if total_large < i32::MAX as usize {
            // fast path, the entire data fits in a single buffer
            // 3) Allocate exactly capacity for all non-inline data
            let mut data_buf = Vec::with_capacity(total_large);

            // 4) Iterate over views and process each inline/non-inline view
            let views_buf: Vec<u128> = (0..len)
                .map(|i| unsafe { self.copy_view_to_buffer(i, 0, &mut data_buf) })
                .collect();
            let data_block = Buffer::from_vec(data_buf);
            let data_blocks = vec![data_block];
            (views_buf, data_blocks)
        } else {
            // slow path, need to split into multiple buffers

            struct GcCopyGroup {
                total_buffer_bytes: usize,
                total_len: usize,
            }

            impl GcCopyGroup {
                fn new(total_buffer_bytes: u32, total_len: usize) -> Self {
                    Self {
                        total_buffer_bytes: total_buffer_bytes as usize,
                        total_len,
                    }
                }
            }

            let mut groups = Vec::new();
            let mut current_length = 0;
            let mut current_elements = 0;

            for view in self.views() {
                let len = *view as u32;
                if len > MAX_INLINE_VIEW_LEN {
                    if current_length + len > i32::MAX as u32 {
                        // Start a new group
                        groups.push(GcCopyGroup::new(current_length, current_elements));
                        current_length = 0;
                        current_elements = 0;
                    }
                    current_length += len;
                    current_elements += 1;
                }
            }
            if current_elements != 0 {
                groups.push(GcCopyGroup::new(current_length, current_elements));
            }
            debug_assert!(groups.len() <= i32::MAX as usize);

            // 3) Copy the buffers group by group
            let mut views_buf = Vec::with_capacity(len);
            let mut data_blocks = Vec::with_capacity(groups.len());

            let mut current_view_idx = 0;

            for (group_idx, gc_copy_group) in groups.iter().enumerate() {
                let mut data_buf = Vec::with_capacity(gc_copy_group.total_buffer_bytes);

                // Directly push views to avoid intermediate Vec allocation
                let new_views = (current_view_idx..current_view_idx + gc_copy_group.total_len).map(
                    |view_idx| {
                        // safety: the view index came from iterating over valid range
                        unsafe {
                            self.copy_view_to_buffer(view_idx, group_idx as i32, &mut data_buf)
                        }
                    },
                );
                views_buf.extend(new_views);

                data_blocks.push(Buffer::from_vec(data_buf));
                current_view_idx += gc_copy_group.total_len;
            }
            (views_buf, data_blocks)
        };

        // 5) Wrap up views buffer
        let views_scalar = ScalarBuffer::from(views_buf);

        // SAFETY: views_scalar, data_blocks, and nulls are correctly aligned and sized
        unsafe { GenericByteViewArray::new_unchecked(views_scalar, data_blocks, nulls) }
    }

    /// Copy the i‑th view into `data_buf` if it refers to an out‑of‑line buffer.
    ///
    /// # Safety
    ///
    /// - `i < self.len()`.
    /// - Every element in `self.views()` must currently refer to a valid slice
    ///   inside one of `self.buffers`.
    /// - `data_buf` must be ready to have additional bytes appended.
    /// - After this call, the returned view will have its
    ///   `buffer_index` reset to `buffer_idx` and its `offset` updated so that it points
    ///   into the bytes just appended at the end of `data_buf`.
    #[inline(always)]
    unsafe fn copy_view_to_buffer(
        &self,
        i: usize,
        buffer_idx: i32,
        data_buf: &mut Vec<u8>,
    ) -> u128 {
        // SAFETY: `i < self.len()` ensures this is in‑bounds.
        let raw_view = unsafe { *self.views().get_unchecked(i) };
        let mut bv = ByteView::from(raw_view);

        // Inline‑small views stay as‑is.
        if bv.length <= MAX_INLINE_VIEW_LEN {
            raw_view
        } else {
            // SAFETY: `bv.buffer_index` and `bv.offset..bv.offset+bv.length`
            // must both lie within valid ranges for `self.buffers`.
            let buffer = unsafe { self.buffers.get_unchecked(bv.buffer_index as usize) };
            let start = bv.offset as usize;
            let end = start + bv.length as usize;
            let slice = unsafe { buffer.get_unchecked(start..end) };

            // Copy out‑of‑line data into our single “0” buffer.
            let new_offset = data_buf.len() as u32;
            data_buf.extend_from_slice(slice);

            bv.buffer_index = buffer_idx as u32;
            bv.offset = new_offset;
            bv.into()
        }
    }

    /// Returns the total number of bytes used by all non inlined views in all
    /// buffers.
    ///
    /// Note this does not account for views that point at the same underlying
    /// data in buffers
    ///
    /// For example, if the array has three strings views:
    /// * View with length = 9 (inlined)
    /// * View with length = 32 (non inlined)
    /// * View with length = 16 (non inlined)
    ///
    /// Then this method would report 48
    pub fn total_buffer_bytes_used(&self) -> usize {
        self.views()
            .iter()
            .map(|v| {
                let len = *v as u32;
                if len > MAX_INLINE_VIEW_LEN {
                    len as usize
                } else {
                    0
                }
            })
            .sum()
    }

    /// Compare two [`GenericByteViewArray`] at index `left_idx` and `right_idx`
    ///
    /// Comparing two ByteView types are non-trivial.
    /// It takes a bit of patience to understand why we don't just compare two &[u8] directly.
    ///
    /// ByteView types give us the following two advantages, and we need to be careful not to lose them:
    /// (1) For string/byte smaller than [`MAX_INLINE_VIEW_LEN`] bytes, the entire data is inlined in the view.
    ///     Meaning that reading one array element requires only one memory access
    ///     (two memory access required for StringArray, one for offset buffer, the other for value buffer).
    ///
    /// (2) For string/byte larger than [`MAX_INLINE_VIEW_LEN`] bytes, we can still be faster than (for certain operations) StringArray/ByteArray,
    ///     thanks to the inlined 4 bytes.
    ///     Consider equality check:
    ///     If the first four bytes of the two strings are different, we can return false immediately (with just one memory access).
    ///
    /// If we directly compare two &[u8], we materialize the entire string (i.e., make multiple memory accesses), which might be unnecessary.
    /// - Most of the time (eq, ord), we only need to look at the first 4 bytes to know the answer,
    ///   e.g., if the inlined 4 bytes are different, we can directly return unequal without looking at the full string.
    ///
    /// # Order check flow
    /// (1) if both string are smaller than [`MAX_INLINE_VIEW_LEN`] bytes, we can directly compare the data inlined to the view.
    /// (2) if any of the string is larger than [`MAX_INLINE_VIEW_LEN`] bytes, we need to compare the full string.
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
    ) -> Ordering {
        let l_view = unsafe { left.views().get_unchecked(left_idx) };
        let l_byte_view = ByteView::from(*l_view);

        let r_view = unsafe { right.views().get_unchecked(right_idx) };
        let r_byte_view = ByteView::from(*r_view);

        let l_len = l_byte_view.length;
        let r_len = r_byte_view.length;

        if l_len <= 12 && r_len <= 12 {
            return Self::inline_key_fast(*l_view).cmp(&Self::inline_key_fast(*r_view));
        }

        // one of the string is larger than 12 bytes,
        // we then try to compare the inlined data first

        // Note: In theory, ByteView is only used for string which is larger than 12 bytes,
        // but we can still use it to get the inlined prefix for shorter strings.
        // The prefix is always the first 4 bytes of the view, for both short and long strings.
        let l_inlined_be = l_byte_view.prefix.swap_bytes();
        let r_inlined_be = r_byte_view.prefix.swap_bytes();
        if l_inlined_be != r_inlined_be {
            return l_inlined_be.cmp(&r_inlined_be);
        }

        // unfortunately, we need to compare the full data
        let l_full_data: &[u8] = unsafe { left.value_unchecked(left_idx).as_ref() };
        let r_full_data: &[u8] = unsafe { right.value_unchecked(right_idx).as_ref() };

        l_full_data.cmp(r_full_data)
    }

    /// Builds a 128-bit composite key for an inline value:
    ///
    /// - High 96 bits: the inline data in big-endian byte order (for correct lexicographical sorting).
    /// - Low  32 bits: the length in big-endian byte order, acting as a tiebreaker so shorter strings
    ///   (or those with fewer meaningful bytes) always numerically sort before longer ones.
    ///
    /// This function extracts the length and the 12-byte inline string data from the raw
    /// little-endian `u128` representation, converts them to big-endian ordering, and packs them
    /// into a single `u128` value suitable for fast, branchless comparisons.
    ///
    /// # Why include length?
    ///
    /// A pure 96-bit content comparison can’t distinguish between two values whose inline bytes
    /// compare equal—either because one is a true prefix of the other or because zero-padding
    /// hides extra bytes. By tucking the 32-bit length into the lower bits, a single `u128` compare
    /// handles both content and length in one go.
    ///
    /// Example: comparing "bar" (3 bytes) vs "bar\0" (4 bytes)
    ///
    /// | String     | Bytes 0–4 (length LE) | Bytes 4–16 (data + padding)    |
    /// |------------|-----------------------|---------------------------------|
    /// | `"bar"`   | `03 00 00 00`         | `62 61 72` + 9 × `00`           |
    /// | `"bar\0"`| `04 00 00 00`         | `62 61 72 00` + 8 × `00`        |
    ///
    /// Both inline parts become `62 61 72 00…00`, so they tie on content. The length field
    /// then differentiates:
    ///
    /// ```text
    /// key("bar")   = 0x0000000000000000000062617200000003
    /// key("bar\0") = 0x0000000000000000000062617200000004
    /// ⇒ key("bar") < key("bar\0")
    /// ```
    /// # Inlining and Endianness
    ///
    /// - We start by calling `.to_le_bytes()` on the `raw` `u128`, because Rust’s native in‑memory
    ///   representation is little‑endian on x86/ARM.
    /// - We extract the low 32 bits numerically (`raw as u32`)—this step is endianness‑free.
    /// - We copy the 12 bytes of inline data (original order) into `buf[0..12]`.
    /// - We serialize `length` as big‑endian into `buf[12..16]`.
    /// - Finally, `u128::from_be_bytes(buf)` treats `buf[0]` as the most significant byte
    ///   and `buf[15]` as the least significant, producing a `u128` whose integer value
    ///   directly encodes “inline data then length” in big‑endian form.
    ///
    /// This ensures that a simple `u128` comparison is equivalent to the desired
    /// lexicographical comparison of the inline bytes followed by length.
    #[inline(always)]
    pub fn inline_key_fast(raw: u128) -> u128 {
        // 1. Decompose `raw` into little‑endian bytes:
        //    - raw_bytes[0..4]  = length in LE
        //    - raw_bytes[4..16] = inline string data
        let raw_bytes = raw.to_le_bytes();

        // 2. Numerically truncate to get the low 32‑bit length (endianness‑free).
        let length = raw as u32;

        // 3. Build a 16‑byte buffer in big‑endian order:
        //    - buf[0..12]  = inline string bytes (in original order)
        //    - buf[12..16] = length.to_be_bytes() (BE)
        let mut buf = [0u8; 16];
        buf[0..12].copy_from_slice(&raw_bytes[4..16]); // inline data

        // Why convert length to big-endian for comparison?
        //
        // Rust (on most platforms) stores integers in little-endian format,
        // meaning the least significant byte is at the lowest memory address.
        // For example, an u32 value like 0x22345677 is stored in memory as:
        //
        //   [0x77, 0x56, 0x34, 0x22]  // little-endian layout
        //    ^     ^     ^     ^
        //  LSB   ↑↑↑           MSB
        //
        // This layout is efficient for arithmetic but *not* suitable for
        // lexicographic (dictionary-style) comparison of byte arrays.
        //
        // To compare values by byte order—e.g., for sorted keys or binary trees—
        // we must convert them to **big-endian**, where:
        //
        //   - The most significant byte (MSB) comes first (index 0)
        //   - The least significant byte (LSB) comes last (index N-1)
        //
        // In big-endian, the same u32 = 0x22345677 would be represented as:
        //
        //   [0x22, 0x34, 0x56, 0x77]
        //
        // This ordering aligns with natural string/byte sorting, so calling
        // `.to_be_bytes()` allows us to construct
        // keys where standard numeric comparison (e.g., `<`, `>`) behaves
        // like lexicographic byte comparison.
        buf[12..16].copy_from_slice(&length.to_be_bytes()); // length in BE

        // 4. Deserialize the buffer as a big‑endian u128:
        //    buf[0] is MSB, buf[15] is LSB.
        // Details:
        // Note on endianness and layout:
        //
        // Although `buf[0]` is stored at the lowest memory address,
        // calling `u128::from_be_bytes(buf)` interprets it as the **most significant byte (MSB)**,
        // and `buf[15]` as the **least significant byte (LSB)**.
        //
        // This is the core principle of **big-endian decoding**:
        //   - Byte at index 0 maps to bits 127..120 (highest)
        //   - Byte at index 1 maps to bits 119..112
        //   - ...
        //   - Byte at index 15 maps to bits 7..0 (lowest)
        //
        // So even though memory layout goes from low to high (left to right),
        // big-endian treats the **first byte** as highest in value.
        //
        // This guarantees that comparing two `u128` keys is equivalent to lexicographically
        // comparing the original inline bytes, followed by length.
        u128::from_be_bytes(buf)
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

/// SAFETY: Correctly implements the contract of Arrow Arrays
unsafe impl<T: ByteViewType + ?Sized> Array for GenericByteViewArray<T> {
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

    fn shrink_to_fit(&mut self) {
        self.views.shrink_to_fit();
        self.buffers.iter_mut().for_each(|b| b.shrink_to_fit());
        self.buffers.shrink_to_fit();
        if let Some(nulls) = &mut self.nulls {
            nulls.shrink_to_fit();
        }
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
        unsafe { GenericByteViewArray::value_unchecked(self, index) }
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
/// See [`GenericByteViewArray`] for format and layout details.
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
        unsafe { StringViewArray::new_unchecked(self.views, self.buffers, self.nulls) }
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
/// See [`GenericByteViewArray`] for format and layout details.
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
    use crate::types::BinaryViewType;
    use crate::{
        Array, BinaryViewArray, GenericBinaryArray, GenericByteViewArray, StringViewArray,
    };
    use arrow_buffer::{Buffer, ScalarBuffer};
    use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};

    const BLOCK_SIZE: u32 = 8;

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
        assert_eq!(
            array.value(1),
            "another large payload over 12 bytes that double than the first one, so that we can trigger the in_progress in builder re-created"
        );
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

    /// 1) Empty array: no elements, expect gc to return empty with no data buffers
    #[test]
    fn test_gc_empty_array() {
        let array = StringViewBuilder::new()
            .with_fixed_block_size(BLOCK_SIZE)
            .finish();
        let gced = array.gc();
        // length and null count remain zero
        assert_eq!(gced.len(), 0);
        assert_eq!(gced.null_count(), 0);
        // no underlying data buffers should be allocated
        assert!(
            gced.data_buffers().is_empty(),
            "Expected no data buffers for empty array"
        );
    }

    /// 2) All inline values (<= INLINE_LEN): capacity-only data buffer, same values
    #[test]
    fn test_gc_all_inline() {
        let mut builder = StringViewBuilder::new().with_fixed_block_size(BLOCK_SIZE);
        // append many short strings, each exactly INLINE_LEN long
        for _ in 0..100 {
            let s = "A".repeat(MAX_INLINE_VIEW_LEN as usize);
            builder.append_option(Some(&s));
        }
        let array = builder.finish();
        let gced = array.gc();
        // Since all views fit inline, data buffer is empty
        assert_eq!(
            gced.data_buffers().len(),
            0,
            "Should have no data buffers for inline values"
        );
        assert_eq!(gced.len(), 100);
        // verify element-wise equality
        array.iter().zip(gced.iter()).for_each(|(orig, got)| {
            assert_eq!(orig, got, "Inline value mismatch after gc");
        });
    }

    /// 3) All large values (> INLINE_LEN): each must be copied into the new data buffer
    #[test]
    fn test_gc_all_large() {
        let mut builder = StringViewBuilder::new().with_fixed_block_size(BLOCK_SIZE);
        let large_str = "X".repeat(MAX_INLINE_VIEW_LEN as usize + 5);
        // append multiple large strings
        for _ in 0..50 {
            builder.append_option(Some(&large_str));
        }
        let array = builder.finish();
        let gced = array.gc();
        // New data buffers should be populated (one or more blocks)
        assert!(
            !gced.data_buffers().is_empty(),
            "Expected data buffers for large values"
        );
        assert_eq!(gced.len(), 50);
        // verify that every large string emerges unchanged
        array.iter().zip(gced.iter()).for_each(|(orig, got)| {
            assert_eq!(orig, got, "Large view mismatch after gc");
        });
    }

    /// 4) All null elements: ensure null bitmap handling path is correct
    #[test]
    fn test_gc_all_nulls() {
        let mut builder = StringViewBuilder::new().with_fixed_block_size(BLOCK_SIZE);
        for _ in 0..20 {
            builder.append_null();
        }
        let array = builder.finish();
        let gced = array.gc();
        // length and null count match
        assert_eq!(gced.len(), 20);
        assert_eq!(gced.null_count(), 20);
        // data buffers remain empty for null-only array
        assert!(
            gced.data_buffers().is_empty(),
            "No data should be stored for nulls"
        );
    }

    /// 5) Random mix of inline, large, and null values with slicing tests
    #[test]
    fn test_gc_random_mixed_and_slices() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut builder = StringViewBuilder::new().with_fixed_block_size(BLOCK_SIZE);
        // Keep a Vec of original Option<String> for later comparison
        let mut original: Vec<Option<String>> = Vec::new();

        for _ in 0..200 {
            if rng.random_bool(0.1) {
                // 10% nulls
                builder.append_null();
                original.push(None);
            } else {
                // random length between 0 and twice the inline limit
                let len = rng.random_range(0..(MAX_INLINE_VIEW_LEN * 2));
                let s: String = "A".repeat(len as usize);
                builder.append_option(Some(&s));
                original.push(Some(s));
            }
        }

        let array = builder.finish();
        // Test multiple slice ranges to ensure offset logic is correct
        for (offset, slice_len) in &[(0, 50), (10, 100), (150, 30)] {
            let sliced = array.slice(*offset, *slice_len);
            let gced = sliced.gc();
            // Build expected slice of Option<&str>
            let expected: Vec<Option<&str>> = original[*offset..(*offset + *slice_len)]
                .iter()
                .map(|opt| opt.as_deref())
                .collect();

            assert_eq!(gced.len(), *slice_len, "Slice length mismatch");
            // Compare element-wise
            gced.iter().zip(expected.iter()).for_each(|(got, expect)| {
                assert_eq!(got, *expect, "Value mismatch in mixed slice after gc");
            });
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_gc_huge_array() {
        // Construct multiple 128 MiB BinaryView entries so total > 4 GiB
        let block_len: usize = 128 * 1024 * 1024; // 128 MiB per view
        let num_views: usize = 36;

        // Create a single 128 MiB data block with a simple byte pattern
        let buffer = Buffer::from_vec(vec![0xAB; block_len]);
        let buffer2 = Buffer::from_vec(vec![0xFF; block_len]);

        // Append this block and then add many views pointing to it
        let mut builder = BinaryViewBuilder::new();
        let block_id = builder.append_block(buffer);
        for _ in 0..num_views / 2 {
            builder
                .try_append_view(block_id, 0, block_len as u32)
                .expect("append view into 128MiB block");
        }
        let block_id2 = builder.append_block(buffer2);
        for _ in 0..num_views / 2 {
            builder
                .try_append_view(block_id2, 0, block_len as u32)
                .expect("append view into 128MiB block");
        }

        let array = builder.finish();
        let total = array.total_buffer_bytes_used();
        assert!(
            total > u32::MAX as usize,
            "Expected total non-inline bytes to exceed 4 GiB, got {}",
            total
        );

        // Run gc and verify correctness
        let gced = array.gc();
        assert_eq!(gced.len(), num_views, "Length mismatch after gc");
        assert_eq!(gced.null_count(), 0, "Null count mismatch after gc");
        assert_ne!(
            gced.data_buffers().len(),
            1,
            "gc with huge buffer should not consolidate data into a single buffer"
        );

        // Element-wise equality check across the entire array
        array.iter().zip(gced.iter()).for_each(|(orig, got)| {
            assert_eq!(orig, got, "Value mismatch after gc on huge array");
        });
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
        assert_eq!(array1, array2);
    }

    /// Integration tests for `inline_key_fast` covering:
    ///
    /// 1. Monotonic ordering across increasing lengths and lexical variations.
    /// 2. Cross-check against `GenericBinaryArray` comparison to ensure semantic equivalence.
    ///
    /// This also includes a specific test for the “bar” vs. “bar\0” case, demonstrating why
    /// the length field is required even when all inline bytes fit in 12 bytes.
    ///
    /// The test includes strings that verify correct byte order (prevent reversal bugs),
    /// and length-based tie-breaking in the composite key.
    ///
    /// The test confirms that `inline_key_fast` produces keys which sort consistently
    /// with the expected lexicographical order of the raw byte arrays.
    #[test]
    fn test_inline_key_fast_various_lengths_and_lexical() {
        /// Helper to create a raw u128 value representing an inline ByteView:
        /// - `length`: number of meaningful bytes (must be ≤ 12)
        /// - `data`: the actual inline data bytes
        ///
        /// The first 4 bytes encode length in little-endian,
        /// the following 12 bytes contain the inline string data (unpadded).
        fn make_raw_inline(length: u32, data: &[u8]) -> u128 {
            assert!(length as usize <= 12, "Inline length must be ≤ 12");
            assert!(
                data.len() == length as usize,
                "Data length must match `length`"
            );

            let mut raw_bytes = [0u8; 16];
            raw_bytes[0..4].copy_from_slice(&length.to_le_bytes()); // length stored little-endian
            raw_bytes[4..(4 + data.len())].copy_from_slice(data); // inline data
            u128::from_le_bytes(raw_bytes)
        }

        // Test inputs: various lengths and lexical orders,
        // plus special cases for byte order and length tie-breaking
        let test_inputs: Vec<&[u8]> = vec![
            b"a",
            b"aa",
            b"aaa",
            b"aab",
            b"abcd",
            b"abcde",
            b"abcdef",
            b"abcdefg",
            b"abcdefgh",
            b"abcdefghi",
            b"abcdefghij",
            b"abcdefghijk",
            b"abcdefghijkl",
            // Tests for byte-order reversal bug:
            // Without the fix, "backend one" would compare as "eno dnekcab",
            // causing incorrect sort order relative to "backend two".
            b"backend one",
            b"backend two",
            // Tests length-tiebreaker logic:
            // "bar" (3 bytes) and "bar\0" (4 bytes) have identical inline data,
            // so only the length differentiates their ordering.
            b"bar",
            b"bar\0",
            // Additional lexical and length tie-breaking cases with same prefix, in correct lex order:
            b"than12Byt",
            b"than12Bytes",
            b"than12Bytes\0",
            b"than12Bytesx",
            b"than12Bytex",
            b"than12Bytez",
            // Additional lexical tests
            b"xyy",
            b"xyz",
            b"xza",
        ];

        // Create a GenericBinaryArray for cross-comparison of lex order
        let array: GenericBinaryArray<i32> =
            GenericBinaryArray::from(test_inputs.iter().map(|s| Some(*s)).collect::<Vec<_>>());

        for i in 0..array.len() - 1 {
            let v1 = array.value(i);
            let v2 = array.value(i + 1);

            // Assert the array's natural lexical ordering is correct
            assert!(v1 < v2, "Array compare failed: {v1:?} !< {v2:?}");

            // Assert the keys produced by inline_key_fast reflect the same ordering
            let key1 = GenericByteViewArray::<BinaryViewType>::inline_key_fast(make_raw_inline(
                v1.len() as u32,
                v1,
            ));
            let key2 = GenericByteViewArray::<BinaryViewType>::inline_key_fast(make_raw_inline(
                v2.len() as u32,
                v2,
            ));

            assert!(
                key1 < key2,
                "Key compare failed: key({v1:?})=0x{key1:032x} !< key({v2:?})=0x{key2:032x}",
            );
        }
    }
}
