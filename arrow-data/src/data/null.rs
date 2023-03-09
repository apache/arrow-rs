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
use crate::data::ArrayDataLayout;
use crate::{ArrayDataBuilder, Buffers};
use arrow_schema::DataType;

/// ArrayData for [null arrays](https://arrow.apache.org/docs/format/Columnar.html#null-layout)
#[derive(Debug, Clone)]
pub struct NullArrayData {
    data_type: DataType,
    len: usize,
}

impl NullArrayData {
    /// Create a new [`NullArrayData`]
    ///
    /// # Panic
    ///
    /// - `PhysicalType::from(&data_type) != PhysicalType::Null`
    pub fn new(data_type: DataType, len: usize) -> Self {
        assert_eq!(
            PhysicalType::from(&data_type),
            PhysicalType::Null,
            "Illegal physical type for NullArrayData of datatype {data_type:?}",
        );
        Self { data_type, len }
    }

    /// Create a new [`NullArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Null`
    pub unsafe fn new_unchecked(data_type: DataType, len: usize) -> Self {
        Self { data_type, len }
    }

    /// Creates a new [`NullArrayData`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`NullArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder) -> Self {
        Self {
            data_type: builder.data_type,
            len: builder.len,
        }
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the [`DataType`] and length of this [`NullArrayData`]
    pub fn into_parts(self) -> (DataType, usize) {
        (self.data_type, self.len)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let new_len = offset.saturating_add(len);
        assert!(new_len <= self.len);
        Self {
            data_type: self.data_type.clone(),
            len,
        }
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.len,
            offset: 0,
            nulls: None,
            buffers: Buffers::default(),
            child_data: &[],
        }
    }
}
