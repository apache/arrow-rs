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
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    pub trait ListOffsetSealed {}
}

/// Types of offset used by variable length list arrays
pub trait ListOffset: private::ListOffsetSealed + ArrowNativeType {
    const TYPE: OffsetType;
}

impl ListOffset for i32 {
    const TYPE: OffsetType = OffsetType::Int32;
}

impl private::ListOffsetSealed for i32 {}

impl ListOffset for i64 {
    const TYPE: OffsetType = OffsetType::Int64;
}

impl private::ListOffsetSealed for i64 {}

/// ArrayData for [variable-size list arrays](https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout)
#[derive(Debug, Clone)]
pub struct ListArrayData<O: ListOffset> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    offsets: OffsetBuffer<O>,
    values: ArrayData,
}

impl<O: ListOffset> ListArrayData<O> {
    /// Create a new [`ListArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::List(O::TYPE)`
    /// - Each consecutive window of `offsets` must identify a valid slice of `values`
    /// - `nulls.len() == offsets.len() - 1`
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
            values,
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
        &self.values
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
        (self.data_type, self.offsets, self.nulls, self.values)
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
}

impl<O: ListOffset> From<ArrayData> for ListArrayData<O> {
    fn from(data: ArrayData) -> Self {
        let offsets = data.buffers.into_iter().next().unwrap();
        let values = data.child_data.into_iter().next().unwrap();

        let offsets = match data.len {
            0 => OffsetBuffer::new_empty(),
            // Safety:
            // ArrayData is valid
            _ => unsafe {
                OffsetBuffer::new_unchecked(ScalarBuffer::new(
                    offsets,
                    data.offset,
                    data.len + 1,
                ))
            },
        };

        Self {
            offsets,
            data_type: data.data_type,
            nulls: data.nulls,
            values,
        }
    }
}

impl<O: ListOffset> From<ListArrayData<O>> for ArrayData {
    fn from(value: ListArrayData<O>) -> Self {
        Self {
            data_type: value.data_type,
            len: value.offsets.len().wrapping_sub(1),
            offset: 0,
            nulls: value.nulls,
            buffers: vec![value.offsets.into_inner().into_inner()],
            child_data: vec![value.values],
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
    values: ArrayData,
}

impl FixedSizeListArrayData {
    /// Create a new [`FixedSizeListArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::FixedSizeList(element_size)`
    /// - `nulls.len() == values.len() / element_size == len`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        len: usize,
        element_size: usize,
        nulls: Option<NullBuffer>,
        values: ArrayData,
    ) -> Self {
        Self {
            data_type,
            len,
            element_size,
            nulls,
            values,
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
        &self.values
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`FixedSizeListArrayData`]
    pub fn into_parts(self) -> (DataType, Option<NullBuffer>, ArrayData) {
        (self.data_type, self.nulls, self.values)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let offset_element = offset.checked_mul(self.element_size).expect("overflow");
        let len_element = len.checked_mul(self.element_size).expect("overflow");
        let values = self.values.slice(offset_element, len_element);

        Self {
            len,
            data_type: self.data_type.clone(),
            element_size: self.element_size,
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
            values,
        }
    }
}

impl From<ArrayData> for FixedSizeListArrayData {
    fn from(data: ArrayData) -> Self {
        let size = match data.data_type {
            DataType::FixedSizeList(_, size) => size as _,
            d => panic!("invalid data type for FixedSizeListArrayData: {d}"),
        };

        let values = data.child_data[0].slice(data.offset * size, data.len * size);
        Self {
            data_type: data.data_type,
            len: data.len,
            element_size: size,
            nulls: data.nulls,
            values,
        }
    }
}

impl From<FixedSizeListArrayData> for ArrayData {
    fn from(value: FixedSizeListArrayData) -> Self {
        Self {
            data_type: value.data_type,
            len: value.len,
            offset: 0,
            buffers: vec![],
            child_data: vec![value.values],
            nulls: value.nulls,
        }
    }
}
