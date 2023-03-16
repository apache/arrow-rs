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

use crate::data::types::PhysicalType;
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::NullBuffer;
use arrow_schema::DataType;

/// ArrayData for [struct arrays](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)
#[derive(Debug, Clone)]
pub struct StructArrayData {
    data_type: DataType,
    len: usize,
    nulls: Option<NullBuffer>,
    children: Vec<ArrayData>,
}

impl StructArrayData {
    /// Create a new [`StructArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Struct`
    /// - all child data and nulls must have length matching `len`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        children: Vec<ArrayData>,
    ) -> Self {
        Self {
            data_type,
            len,
            nulls,
            children,
        }
    }

    /// Returns the length of this [`StructArrayData`]
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if this [`StructArrayData`] has zero length
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the primitive values
    #[inline]
    pub fn children(&self) -> &[ArrayData] {
        &self.children
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`StructArrayData`]
    pub fn into_parts(self) -> (DataType, Option<NullBuffer>, Vec<ArrayData>) {
        (self.data_type, self.nulls, self.children)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            len,
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
            children: self.children.iter().map(|c| c.slice(offset, len)).collect(),
        }
    }
}

impl From<ArrayData> for StructArrayData {
    fn from(mut data: ArrayData) -> Self {
        assert_eq!(PhysicalType::from(&data.data_type), PhysicalType::Struct);
        Self {
            data_type: data.data_type,
            len: data.len,
            nulls: data.nulls,
            // Don't slice children as assume offset already applied (#1750)
            children: data.child_data,
        }
    }
}

impl From<StructArrayData> for ArrayData {
    fn from(value: StructArrayData) -> Self {
        Self {
            data_type: value.data_type,
            len: value.len,
            offset: 0,
            buffers: vec![],
            child_data: value.children,
            nulls: value.nulls,
        }
    }
}
