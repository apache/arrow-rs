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
use crate::ArrayData;
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::{ArrowNativeType, Buffer};
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

/// Types of offset used by variable length byte arrays
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

/// ArrayData for variable length list arrays
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
}

impl<O: ListOffset> From<ListArrayData<O>> for ArrayDataList {
    fn from(value: ListArrayData<O>) -> Self {
        O::upcast(value)
    }
}

/// ArrayData for [variable-size list arrays](https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout)
pub struct ListArrayData<O: ListOffset> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    offsets: ScalarBuffer<O>,
    child: Box<ArrayData>,
}

impl<O: ListOffset> ListArrayData<O> {
    /// Create a new [`ListArrayData`]
    ///
    /// # Safety
    ///
    /// - Each consecutive window of `offsets` must identify a valid slice of `child`
    /// - `nulls.len() == offsets.len() + 1`
    /// - `data_type` must be valid for this layout
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: ScalarBuffer<O>,
        nulls: Option<NullBuffer>,
        child: ArrayData,
    ) -> Self {
        Self {
            data_type,
            nulls,
            offsets,
            child: Box::new(child),
        }
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the offsets
    #[inline]
    pub fn offsets(&self) -> &[O] {
        &self.offsets
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
}


