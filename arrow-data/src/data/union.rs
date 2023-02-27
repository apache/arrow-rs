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

use crate::ArrayData;
use arrow_buffer::buffer::ScalarBuffer;
use arrow_schema::DataType;

/// ArrayData for union arrays
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
    /// - `data_type` must be valid for this layout
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

    /// Returns the type ids for this array if this is a dense union
    #[inline]
    pub fn type_ids(&self) -> &[i8] {
        &self.type_ids
    }

    /// Returns the offsets for this array if this is a dense union
    #[inline]
    pub fn offsets(&self) -> Option<&[i32]> {
        self.offsets.as_deref()
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
}
