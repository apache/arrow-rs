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

use crate::data::types::OffsetType;
use crate::data::ArrayDataLayout;
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    use super::*;

    pub trait ListOffsetSealed {
        /// Downcast [`ArrayDataList`] to `[ListArrayData`]
        fn downcast_ref(data: &ArrayDataList) -> Option<&ListArrayData<Self>>
        where
            Self: ListOffset;

        /// Downcast [`ArrayDataList`] to `[ListArrayData`]
        fn downcast(data: ArrayDataList) -> Option<ListArrayData<Self>>
        where
            Self: ListOffset;

        /// Cast [`ListArrayData`] to [`ArrayDataList`]
        fn upcast(v: ListArrayData<Self>) -> ArrayDataList
        where
            Self: ListOffset;
    }
}

/// Types of offset used by variable length list arrays
pub trait ListOffset: private::ListOffsetSealed + ArrowNativeType {
    const TYPE: OffsetType;
}

impl ListOffset for i32 {
    const TYPE: OffsetType = OffsetType::Int32;
}

impl private::ListOffsetSealed for i32 {
    fn downcast_ref(data: &ArrayDataList) -> Option<&ListArrayData<Self>>
    where
        Self: ListOffset,
    {
        match data {
            ArrayDataList::Small(v) => Some(v),
            ArrayDataList::Large(_) => None,
        }
    }

    fn downcast(data: ArrayDataList) -> Option<ListArrayData<Self>>
    where
        Self: ListOffset,
    {
        match data {
            ArrayDataList::Small(v) => Some(v),
            ArrayDataList::Large(_) => None,
        }
    }

    fn upcast(v: ListArrayData<Self>) -> ArrayDataList
    where
        Self: ListOffset,
    {
        ArrayDataList::Small(v)
    }
}

impl ListOffset for i64 {
    const TYPE: OffsetType = OffsetType::Int64;
}

impl private::ListOffsetSealed for i64 {
    fn downcast_ref(data: &ArrayDataList) -> Option<&ListArrayData<Self>>
    where
        Self: ListOffset,
    {
        match data {
            ArrayDataList::Small(_) => None,
            ArrayDataList::Large(v) => Some(v),
        }
    }

    fn downcast(data: ArrayDataList) -> Option<ListArrayData<Self>>
    where
        Self: ListOffset,
    {
        match data {
            ArrayDataList::Small(_) => None,
            ArrayDataList::Large(v) => Some(v),
        }
    }

    fn upcast(v: ListArrayData<Self>) -> ArrayDataList
    where
        Self: ListOffset,
    {
        ArrayDataList::Large(v)
    }
}

/// Applies op to each variant of [`ListArrayData`]
#[macro_export]
macro_rules! list_op {
    ($array:ident, $op:block) => {
        match $array {
            ArrayDataList::Small($array) => $op
            ArrayDataList::Large($array) => $op
        }
    };
}

/// An enumeration of the types of [`ListArrayData`]
#[derive(Debug, Clone)]
pub enum ArrayDataList {
    Small(ListArrayData<i32>),
    Large(ListArrayData<i64>),
}

impl ArrayDataList {
    /// Downcast this [`ArrayDataList`] to the corresponding [`ListArrayData`]
    pub fn downcast_ref<O: ListOffset>(&self) -> Option<&ListArrayData<O>> {
        O::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataList`] to the corresponding [`ListArrayData`]
    pub fn downcast<O: ListOffset>(self) -> Option<ListArrayData<O>> {
        O::downcast(self)
    }

    /// Returns the values of this [`ArrayDataList`]
    pub fn values(&self) -> &ArrayData {
        let s = self;
        list_op!(s, { s.values() })
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let s = self;
        list_op!(s, { s.slice(offset, len).into() })
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        let s = self;
        list_op!(s, { s.layout() })
    }

    /// Creates a new [`ArrayDataList`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`ListArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder, offset: OffsetType) -> Self {
        match offset {
            OffsetType::Int32 => Self::Small(ListArrayData::from_raw(builder)),
            OffsetType::Int64 => Self::Large(ListArrayData::from_raw(builder)),
        }
    }
}

impl<O: ListOffset> From<ListArrayData<O>> for ArrayDataList {
    fn from(value: ListArrayData<O>) -> Self {
        O::upcast(value)
    }
}

/// ArrayData for [variable-size list arrays](https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout)
#[derive(Debug, Clone)]
pub struct ListArrayData<O: ListOffset> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    offsets: OffsetBuffer<O>,
    values: Box<ArrayData>,
}

impl<O: ListOffset> ListArrayData<O> {
    /// Create a new [`ListArrayData`]
    ///
    /// # Safety
    ///
    /// - Each consecutive window of `offsets` must identify a valid slice of `child`
    /// - `nulls.len() == offsets.len() - 1`
    /// - `data_type` must be valid for this layout
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
        values: ArrayData,
    ) -> Self {
        Self {
            data_type,
            nulls,
            offsets,
            values: Box::new(values),
        }
    }

    /// Creates a new [`ListArrayData`] from an [`ArrayDataBuilder`]
    ///
    /// # Safety
    ///
    /// See [`Self::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder) -> Self {
        let offsets = builder.buffers.into_iter().next().unwrap();
        let values = builder.child_data.into_iter().next().unwrap();

        let offsets = match builder.len {
            0 => OffsetBuffer::new_empty(),
            _ => OffsetBuffer::new_unchecked(ScalarBuffer::new(
                offsets,
                builder.offset,
                builder.len + 1,
            )),
        };

        Self {
            offsets,
            data_type: builder.data_type,
            nulls: builder.nulls,
            values: Box::new(values),
        }
    }

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len().wrapping_sub(1)
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the offsets
    #[inline]
    pub fn offsets(&self) -> &OffsetBuffer<O> {
        &self.offsets
    }

    /// Returns the values of this [`ListArrayData`]
    #[inline]
    pub fn values(&self) -> &ArrayData {
        self.values.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`ListArrayData`]
    pub fn into_parts(
        self,
    ) -> (DataType, OffsetBuffer<O>, Option<NullBuffer>, ArrayData) {
        (self.data_type, self.offsets, self.nulls, *self.values)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
            offsets: self.offsets.slice(offset, len),
            values: self.values.clone(),
        }
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.len(),
            offset: 0,
            nulls: self.nulls.as_ref(),
            buffers: Buffers::one(self.offsets.inner().inner()),
            child_data: std::slice::from_ref(self.values.as_ref()),
        }
    }
}

/// ArrayData for [fixed-size list arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout)
#[derive(Debug, Clone)]
pub struct FixedSizeListArrayData {
    data_type: DataType,
    len: usize,
    element_size: usize,
    nulls: Option<NullBuffer>,
    child: Box<ArrayData>,
}

impl FixedSizeListArrayData {
    /// Create a new [`FixedSizeListArrayData`]
    ///
    /// # Safety
    ///
    /// - `data_type` must be valid for this layout
    /// - `nulls.len() == values.len() / element_size == len`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        len: usize,
        element_size: usize,
        nulls: Option<NullBuffer>,
        child: ArrayData,
    ) -> Self {
        Self {
            data_type,
            len,
            element_size,
            nulls,
            child: Box::new(child),
        }
    }

    /// Creates a new [`FixedSizeListArrayData`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`FixedSizeListArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder, size: usize) -> Self {
        let child =
            builder.child_data[0].slice(builder.offset * size, builder.len * size);
        Self {
            data_type: builder.data_type,
            len: builder.len,
            element_size: size,
            nulls: builder.nulls,
            child: Box::new(child),
        }
    }

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the size of each element
    #[inline]
    pub fn element_size(&self) -> usize {
        self.element_size
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the child data
    #[inline]
    pub fn child(&self) -> &ArrayData {
        self.child.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`FixedSizeListArrayData`]
    pub fn into_parts(self) -> (DataType, Option<NullBuffer>, ArrayData) {
        (self.data_type, self.nulls, *self.child)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let offset_element = offset.checked_mul(self.element_size).expect("overflow");
        let len_element = len.checked_mul(self.element_size).expect("overflow");
        let child = self.child.slice(offset_element, len_element);

        Self {
            len,
            data_type: self.data_type.clone(),
            element_size: self.element_size,
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
            child: Box::new(child),
        }
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.len,
            offset: 0,
            nulls: self.nulls.as_ref(),
            buffers: Buffers::default(),
            child_data: std::slice::from_ref(self.child.as_ref()),
        }
    }
}
