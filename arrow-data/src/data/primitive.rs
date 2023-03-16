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

use crate::data::types::{PhysicalType, PrimitiveType};
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::{i256, ArrowNativeType};
use arrow_schema::DataType;
use half::f16;

mod private {
    pub trait PrimitiveSealed {}
}

pub trait Primitive: private::PrimitiveSealed + ArrowNativeType {
    const TYPE: PrimitiveType;
}

macro_rules! primitive {
    ($t:ty,$v:ident) => {
        impl Primitive for $t {
            const TYPE: PrimitiveType = PrimitiveType::$v;
        }
        impl private::PrimitiveSealed for $t {}
    };
}

primitive!(i8, Int8);
primitive!(i16, Int16);
primitive!(i32, Int32);
primitive!(i64, Int64);
primitive!(i128, Int128);
primitive!(i256, Int256);
primitive!(u8, UInt8);
primitive!(u16, UInt16);
primitive!(u32, UInt32);
primitive!(u64, UInt64);
primitive!(f16, Float16);
primitive!(f32, Float32);
primitive!(f64, Float64);

/// ArrayData for [fixed size arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout) of [`Primitive`]
#[derive(Debug, Clone)]
pub struct PrimitiveArrayData<T: Primitive> {
    data_type: DataType,
    values: ScalarBuffer<T>,
    nulls: Option<NullBuffer>,
}

impl<T: Primitive> PrimitiveArrayData<T> {
    /// Create a new [`PrimitiveArrayData`]
    ///
    /// # Panics
    ///
    /// Panics if
    /// - `PhysicalType::from(&data_type) != PhysicalType::Primitive(T::TYPE)`
    /// - `nulls` and `values` are different lengths
    pub fn new(
        data_type: DataType,
        values: ScalarBuffer<T>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        assert_eq!(
            PhysicalType::from(&data_type),
            PhysicalType::Primitive(T::TYPE),
            "Illegal physical type for PrimitiveArrayData of datatype {data_type:?}",
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

    /// Create a new [`PrimitiveArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Primitive(T::TYPE)`
    /// - `nulls` and `values` must be the same length
    pub unsafe fn new_unchecked(
        data_type: DataType,
        values: ScalarBuffer<T>,
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

    /// Returns the primitive values
    #[inline]
    pub fn values(&self) -> &ScalarBuffer<T> {
        &self.values
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`PrimitiveArrayData`]
    pub fn into_parts(self) -> (DataType, ScalarBuffer<T>, Option<NullBuffer>) {
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

impl<T: Primitive> From<ArrayData> for PrimitiveArrayData<T> {
    fn from(data: ArrayData) -> Self {
        let values = data.buffers.into_iter().next().unwrap();
        let values = ScalarBuffer::new(values, data.offset, data.len);
        Self {
            values,
            data_type: data.data_type,
            nulls: data.nulls,
        }
    }
}

impl<T: Primitive> From<PrimitiveArrayData<T>> for ArrayData {
    fn from(value: PrimitiveArrayData<T>) -> Self {
        Self {
            data_type: value.data_type,
            len: value.values.len(),
            offset: 0,
            buffers: vec![value.values.into_inner()],
            child_data: vec![],
            nulls: value.nulls,
        }
    }
}
