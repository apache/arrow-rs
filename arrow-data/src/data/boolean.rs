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
use arrow_buffer::buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::DataType;

#[derive(Debug, Clone)]
pub struct BooleanArrayData {
    data_type: DataType,
    values: BooleanBuffer,
    nulls: Option<NullBuffer>,
}

impl BooleanArrayData {
    /// Create a new [`BooleanArrayData`]
    ///
    /// # Panics
    ///
    /// Panics if
    /// - `nulls` and `values` are different lengths
    /// - `PhysicalType::from(&data_type) != PhysicalType::Boolean`
    pub fn new(
        data_type: DataType,
        values: BooleanBuffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let physical = PhysicalType::from(&data_type);
        assert_eq!(
            physical, PhysicalType::Boolean,
            "Illegal physical type for BooleanArrayData of datatype {:?}, expected {:?} got {:?}",
            data_type,
            PhysicalType::Boolean,
            physical
        );

        if let Some(n) = nulls.as_ref() {
            assert_eq!(values.len(), n.len())
        }
        Self {
            data_type,
            values,
            nulls,
        }
    }

    /// Create a new [`BooleanArrayData`]
    ///
    /// # Safety
    ///
    /// - `nulls` and `values` are the same lengths
    /// - `PhysicalType::from(&data_type) == PhysicalType::Boolean`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        values: BooleanBuffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type,
            values,
            nulls,
        }
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the boolean values
    #[inline]
    pub fn values(&self) -> &BooleanBuffer {
        &self.values
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`BooleanArrayData`]
    pub fn into_parts(self) -> (DataType, BooleanBuffer, Option<NullBuffer>) {
        (self.data_type, self.values, self.nulls)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset, len),
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
        }
    }
}

impl From<ArrayData> for BooleanArrayData {
    fn from(data: ArrayData) -> Self {
        let values = data.buffers.into_iter().next().unwrap();
        let values = BooleanBuffer::new(values, data.offset, data.len);
        Self {
            values,
            data_type: data.data_type,
            nulls: data.nulls,
        }
    }
}

impl From<BooleanArrayData> for ArrayData {
    fn from(value: BooleanArrayData) -> Self {
        Self {
            data_type: value.data_type,
            len: value.values.len(),
            offset: value.values.offset(),
            buffers: vec![value.values.into_inner()],
            nulls: value.nulls,
            child_data: vec![],
        }
    }
}
