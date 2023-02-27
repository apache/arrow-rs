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

use crate::data::types::RunEndType;
use crate::ArrayData;
use arrow_buffer::buffer::ScalarBuffer;
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use std::marker::PhantomData;

mod private {
    use super::*;

    pub trait RunEndSealed {
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

pub trait RunEnd: private::RunEndSealed + ArrowNativeType {
    const TYPE: RunEndType;
}

macro_rules! run_end {
    ($t:ty,$v:ident) => {
        impl RunEnd for $t {
            const TYPE: RunEndType = RunEndType::$v;
        }
        impl private::RunEndSealed for $t {
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

/// An enumeration of the types of [`RunArrayData`]
pub enum ArrayDataRun {
    Int16(RunArrayData<i16>),
    Int32(RunArrayData<i32>),
    Int64(RunArrayData<i64>),
}

impl ArrayDataRun {
    /// Downcast this [`ArrayDataRun`] to the corresponding [`RunArrayData`]
    pub fn downcast_ref<E: RunEnd>(&self) -> Option<&RunArrayData<E>> {
        E::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataRun`] to the corresponding [`RunArrayData`]
    pub fn downcast<E: RunEnd>(self) -> Option<RunArrayData<E>> {
        E::downcast(self)
    }
}

impl<E: RunEnd> From<RunArrayData<E>> for ArrayDataRun {
    fn from(value: RunArrayData<E>) -> Self {
        E::upcast(value)
    }
}

/// ArrayData for [run-end encoded arrays](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout)
pub struct RunArrayData<E: RunEnd> {
    data_type: DataType,
    run_ends: ScalarBuffer<E>,
    child: Box<ArrayData>,
}

impl<E: RunEnd> RunArrayData<E> {
    /// Create a new [`RunArrayData`]
    ///
    /// # Safety
    ///
    /// - `data_type` must be valid for this layout
    /// - `run_ends` must contain monotonically increasing, positive values `<= child.len()`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        run_ends: ScalarBuffer<E>,
        child: ArrayData,
    ) -> Self {
        Self {
            data_type,
            run_ends,
            child: Box::new(child),
        }
    }

    /// Returns the run ends
    #[inline]
    pub fn run_ends(&self) -> &[E] {
        &self.run_ends
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the child data
    #[inline]
    pub fn child(&self) -> &ArrayData {
        self.child.as_ref()
    }
}
