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
use crate::data::ArrayDataLayout;
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{RunEndBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    use super::*;

    pub trait RunEndSealed {
        const ENDS_TYPE: DataType;

        /// Downcast [`ArrayDataRun`] to `[RunArrayData`]
        fn downcast_ref(data: &ArrayDataRun) -> Option<&RunArrayData<Self>>
        where
            Self: RunEnd;

        /// Downcast [`ArrayDataRun`] to `[RunArrayData`]
        fn downcast(data: ArrayDataRun) -> Option<RunArrayData<Self>>
        where
            Self: RunEnd;

        /// Cast [`RunArrayData`] to [`ArrayDataRun`]
        fn upcast(v: RunArrayData<Self>) -> ArrayDataRun
        where
            Self: RunEnd;
    }
}

pub trait RunEnd: private::RunEndSealed + ArrowNativeType + Primitive {
    const TYPE: RunEndType;
}

macro_rules! run_end {
    ($t:ty,$v:ident) => {
        impl RunEnd for $t {
            const TYPE: RunEndType = RunEndType::$v;
        }
        impl private::RunEndSealed for $t {
            const ENDS_TYPE: DataType = DataType::$v;

            fn downcast_ref(data: &ArrayDataRun) -> Option<&RunArrayData<Self>> {
                match data {
                    ArrayDataRun::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn downcast(data: ArrayDataRun) -> Option<RunArrayData<Self>> {
                match data {
                    ArrayDataRun::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn upcast(v: RunArrayData<Self>) -> ArrayDataRun {
                ArrayDataRun::$v(v)
            }
        }
    };
}

run_end!(i16, Int16);
run_end!(i32, Int32);
run_end!(i64, Int64);

/// Applies op to each variant of [`ArrayDataRun`]
macro_rules! run_op {
    ($array:ident, $op:block) => {
        match $array {
            ArrayDataRun::Int16($array) => $op
            ArrayDataRun::Int32($array) => $op
            ArrayDataRun::Int64($array) => $op
        }
    };
}

/// An enumeration of the types of [`RunArrayData`]
#[derive(Debug, Clone)]
pub enum ArrayDataRun {
    Int16(RunArrayData<i16>),
    Int32(RunArrayData<i32>),
    Int64(RunArrayData<i64>),
}

impl ArrayDataRun {
    /// Downcast this [`ArrayDataRun`] to the corresponding [`RunArrayData`]
    pub fn downcast_ref<E: RunEnd>(&self) -> Option<&RunArrayData<E>> {
        <E as private::RunEndSealed>::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataRun`] to the corresponding [`RunArrayData`]
    pub fn downcast<E: RunEnd>(self) -> Option<RunArrayData<E>> {
        <E as private::RunEndSealed>::downcast(self)
    }

    /// Returns the values of this [`ArrayDataRun`]
    #[inline]
    pub fn values(&self) -> &ArrayData {
        let s = self;
        run_op!(s, { s.values() })
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let s = self;
        run_op!(s, { s.slice(offset, len).into() })
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        let s = self;
        run_op!(s, { s.layout() })
    }

    /// Creates a new [`ArrayDataRun`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`RunArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder, run: RunEndType) -> Self {
        use RunEndType::*;
        match run {
            Int16 => Self::Int16(RunArrayData::from_raw(builder)),
            Int32 => Self::Int32(RunArrayData::from_raw(builder)),
            Int64 => Self::Int64(RunArrayData::from_raw(builder)),
        }
    }
}

impl<E: RunEnd> From<RunArrayData<E>> for ArrayDataRun {
    fn from(value: RunArrayData<E>) -> Self {
        <E as private::RunEndSealed>::upcast(value)
    }
}

/// ArrayData for [run-end encoded arrays](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout)
#[derive(Debug, Clone)]
pub struct RunArrayData<E: RunEnd> {
    data_type: DataType,
    run_ends: RunEndBuffer<E>,
    /// The children of this RunArrayData
    /// 1: the run ends
    /// 2: the values
    children: Box<[ArrayData; 2]>,
}

impl<E: RunEnd> RunArrayData<E> {
    /// Create a new [`RunArrayData`]
    ///
    /// # Safety
    ///
    /// - `data_type` must be valid for this layout
    /// - `run_ends` must contain monotonically increasing, positive values `<= len`
    /// - `run_ends.len() == child.len()`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        run_ends: RunEndBuffer<E>,
        values: ArrayData,
    ) -> Self {
        let inner = run_ends.inner();
        let child = ArrayDataBuilder::new(E::ENDS_TYPE)
            .len(inner.len())
            .buffers(vec![inner.inner().clone()])
            .build_unchecked();

        Self {
            data_type,
            run_ends,
            children: Box::new([child, values]),
        }
    }

    /// Creates a new [`RunArrayData`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`RunArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder) -> Self {
        let mut iter = builder.child_data.into_iter();
        let child1 = iter.next().unwrap();
        let child2 = iter.next().unwrap();

        let p = ScalarBuffer::new(child1.buffers[0].clone(), child1.offset, child1.len);
        let run_ends = RunEndBuffer::new_unchecked(p, builder.offset, builder.len);

        Self {
            run_ends,
            data_type: builder.data_type,
            children: Box::new([child1, child2]),
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
        &self.children[1]
    }

    /// Returns the underlying parts of this [`RunArrayData`]
    pub fn into_parts(self) -> (DataType, RunEndBuffer<E>, ArrayData) {
        let child = self.children.into_iter().nth(1).unwrap();
        (self.data_type, self.run_ends, child)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            run_ends: self.run_ends.slice(offset, len),
            children: self.children.clone(),
        }
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.run_ends.len(),
            offset: self.run_ends.offset(),
            nulls: None,
            buffers: Buffers::default(),
            child_data: self.children.as_ref(),
        }
    }
}
