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

use crate::data::primitive::{Primitive, PrimitiveArrayData};
use crate::data::types::RunEndType;
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{RunEndBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    use super::*;

    pub trait RunEndSealed {
        const ENDS_TYPE: DataType;
    }
}

pub trait RunEnd: private::RunEndSealed + ArrowNativeType {
    const TYPE: RunEndType;
}

macro_rules! run_end {
    ($t:ty,$v:ident) => {
        impl RunEnd for $t {
            const TYPE: RunEndType = RunEndType::$v;
        }
        impl private::RunEndSealed for $t {
            const ENDS_TYPE: DataType = DataType::$v;
        }
    };
}

run_end!(i16, Int16);
run_end!(i32, Int32);
run_end!(i64, Int64);

/// ArrayData for [run-end encoded arrays](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout)
#[derive(Debug, Clone)]
pub struct RunArrayData<E: RunEnd> {
    data_type: DataType,
    run_ends: RunEndBuffer<E>,
    values: ArrayData,
}

impl<E: RunEnd> RunArrayData<E> {
    /// Create a new [`RunArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Run(E::TYPE)`
    /// - `run_ends` must contain monotonically increasing, positive values `<= len`
    /// - `run_ends.get_end_physical_index() < values.len()`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        run_ends: RunEndBuffer<E>,
        values: ArrayData,
    ) -> Self {
        Self {
            data_type,
            run_ends,
            values,
        }
    }

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.run_ends.len()
    }

    /// Returns the offset
    #[inline]
    pub fn offset(&self) -> usize {
        self.run_ends.offset()
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.run_ends.is_empty()
    }

    /// Returns the run ends
    #[inline]
    pub fn run_ends(&self) -> &RunEndBuffer<E> {
        &self.run_ends
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the child data
    #[inline]
    pub fn values(&self) -> &ArrayData {
        &self.values
    }

    /// Returns the underlying parts of this [`RunArrayData`]
    pub fn into_parts(self) -> (DataType, RunEndBuffer<E>, ArrayData) {
        (self.data_type, self.run_ends, self.values)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            run_ends: self.run_ends.slice(offset, len),
            values: self.values.clone(),
        }
    }
}

impl<E: RunEnd> From<ArrayData> for RunArrayData<E> {
    fn from(data: ArrayData) -> Self {
        let mut iter = data.child_data.into_iter();
        let child1 = iter.next().unwrap();
        let child2 = iter.next().unwrap();

        let run_ends = child1.buffers.into_iter().next().unwrap();
        let run_ends = ScalarBuffer::new(run_ends, child1.offset, child1.len);
        // Safety:
        // ArrayData must be valid
        let run_ends =
            unsafe { RunEndBuffer::new_unchecked(run_ends, data.offset, data.len) };

        Self {
            run_ends,
            values: child2,
            data_type: data.data_type,
        }
    }
}

impl<E: RunEnd> From<RunArrayData<E>> for ArrayData {
    fn from(value: RunArrayData<E>) -> Self {
        let len = value.run_ends.len();
        let offset = value.run_ends.offset();
        let inner = value.run_ends.into_inner();

        // Safety:
        // Valid by construction
        let child1 = unsafe {
            ArrayDataBuilder::new(E::ENDS_TYPE)
                .len(inner.len())
                .buffers(vec![inner.into_inner()])
                .build_unchecked()
        };

        Self {
            data_type: value.data_type,
            len,
            offset,
            buffers: vec![],
            child_data: vec![child1, value.values],
            nulls: None,
        }
    }
}
