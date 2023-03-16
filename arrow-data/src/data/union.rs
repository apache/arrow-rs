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

use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::ScalarBuffer;
use arrow_schema::{DataType, UnionMode};

/// ArrayData for [union arrays](https://arrow.apache.org/docs/format/Columnar.html#union-layout)
#[derive(Debug, Clone)]
pub struct UnionArrayData {
    data_type: DataType,
    type_ids: ScalarBuffer<i8>,
    offsets: Option<ScalarBuffer<i32>>,
    children: Vec<ArrayData>,
}

impl UnionArrayData {
    /// Creates a new [`UnionArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Union(mode)`
    /// - `offsets` is `Some` iff the above `mode == UnionMode::Sparse`
    /// - `type_ids` must only contain values corresponding to a field in `data_type`
    /// - `children` must match the field definitions in `data_type`
    /// - For each value id in type_ids, the corresponding offset, must be in bounds for the child
    pub unsafe fn new_unchecked(
        data_type: DataType,
        type_ids: ScalarBuffer<i8>,
        offsets: Option<ScalarBuffer<i32>>,
        children: Vec<ArrayData>,
    ) -> Self {
        Self {
            data_type,
            type_ids,
            offsets,
            children,
        }
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.type_ids.len()
    }

    /// Returns the type ids for this array
    #[inline]
    pub fn type_ids(&self) -> &ScalarBuffer<i8> {
        &self.type_ids
    }

    /// Returns the offsets for this array if this is a dense union
    #[inline]
    pub fn offsets(&self) -> Option<&ScalarBuffer<i32>> {
        self.offsets.as_ref()
    }

    /// Returns the children of this array
    #[inline]
    pub fn children(&self) -> &[ArrayData] {
        &self.children
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`UnionArrayData`]
    pub fn into_parts(
        self,
    ) -> (
        DataType,
        ScalarBuffer<i8>,
        Option<ScalarBuffer<i32>>,
        Vec<ArrayData>,
    ) {
        (self.data_type, self.type_ids, self.offsets, self.children)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let (offsets, children) = match &self.offsets {
            Some(offsets) => (Some(offsets.slice(offset, len)), self.children.clone()),
            None => (
                None,
                self.children.iter().map(|c| c.slice(offset, len)).collect(),
            ),
        };
        Self {
            data_type: self.data_type.clone(),
            type_ids: self.type_ids.slice(offset, len),
            offsets,
            children,
        }
    }
}

impl From<ArrayData> for UnionArrayData {
    fn from(value: ArrayData) -> Self {
        match value.data_type {
            DataType::Union(_, _, UnionMode::Sparse) => {
                let type_ids = value.buffers.into_iter().next().unwrap();
                let type_ids = ScalarBuffer::new(type_ids, value.offset, value.len);
                let children = value
                    .child_data
                    .into_iter()
                    .map(|x| x.slice(value.offset, value.len))
                    .collect();

                Self {
                    type_ids,
                    children,
                    data_type: value.data_type,
                    offsets: None,
                }
            }
            DataType::Union(_, _, UnionMode::Dense) => {
                let mut iter = value.buffers.into_iter();
                let type_ids = iter.next().unwrap();
                let offsets = iter.next().unwrap();
                let type_ids = ScalarBuffer::new(type_ids, value.offset, value.len);
                let offsets = ScalarBuffer::new(offsets, value.offset, value.len);

                Self {
                    type_ids,
                    data_type: value.data_type,
                    offsets: Some(offsets),
                    children: value.child_data,
                }
            }
            d => panic!("invalid data type for UnionArrayData: {d}"),
        }
    }
}

impl From<UnionArrayData> for ArrayData {
    fn from(value: UnionArrayData) -> Self {
        let len = value.type_ids.len();
        let buffers = match value.offsets {
            Some(offsets) => vec![value.type_ids.into_inner(), offsets.into_inner()],
            None => vec![value.type_ids.into_inner()],
        };

        Self {
            data_type: value.data_type,
            len,
            offset: 0,
            nulls: None,
            buffers,
            child_data: value.children,
        }
    }
}
