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
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::{i256, ArrowNativeType};
use arrow_schema::DataType;
use half::f16;

mod private {
    use super::*;

    pub trait PrimitiveSealed {
        /// Downcast [`ArrayDataPrimitive`] to `[PrimitiveArrayData`]
        fn downcast_ref(data: &ArrayDataPrimitive) -> Option<&PrimitiveArrayData<Self>>
        where
            Self: Primitive;

        /// Downcast [`ArrayDataPrimitive`] to `[PrimitiveArrayData`]
        fn downcast(data: ArrayDataPrimitive) -> Option<PrimitiveArrayData<Self>>
        where
            Self: Primitive;

        /// Cast [`ArrayDataPrimitive`] to [`ArrayDataPrimitive`]
        fn upcast(v: PrimitiveArrayData<Self>) -> ArrayDataPrimitive
        where
            Self: Primitive;
    }
}

pub trait Primitive: private::PrimitiveSealed + ArrowNativeType {
    const TYPE: PrimitiveType;
}

macro_rules! primitive {
    ($t:ty,$v:ident) => {
        impl Primitive for $t {
            const TYPE: PrimitiveType = PrimitiveType::$v;
        }
        impl private::PrimitiveSealed for $t {
            fn downcast_ref(
                data: &ArrayDataPrimitive,
            ) -> Option<&PrimitiveArrayData<Self>> {
                match data {
                    ArrayDataPrimitive::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn downcast(data: ArrayDataPrimitive) -> Option<PrimitiveArrayData<Self>> {
                match data {
                    ArrayDataPrimitive::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn upcast(v: PrimitiveArrayData<Self>) -> ArrayDataPrimitive {
                ArrayDataPrimitive::$v(v)
            }
        }
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

/// An enumeration of the types of [`PrimitiveArrayData`]
pub enum ArrayDataPrimitive {
    Int8(PrimitiveArrayData<i8>),
    Int16(PrimitiveArrayData<i16>),
    Int32(PrimitiveArrayData<i32>),
    Int64(PrimitiveArrayData<i64>),
    Int128(PrimitiveArrayData<i128>),
    Int256(PrimitiveArrayData<i256>),
    UInt8(PrimitiveArrayData<u8>),
    UInt16(PrimitiveArrayData<u16>),
    UInt32(PrimitiveArrayData<u32>),
    UInt64(PrimitiveArrayData<u64>),
    Float16(PrimitiveArrayData<f16>),
    Float32(PrimitiveArrayData<f32>),
    Float64(PrimitiveArrayData<f64>),
}

impl ArrayDataPrimitive {
    /// Downcast this [`ArrayDataPrimitive`] to the corresponding [`PrimitiveArrayData`]
    pub fn downcast_ref<P: Primitive>(&self) -> Option<&PrimitiveArrayData<P>> {
        P::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataPrimitive`] to the corresponding [`PrimitiveArrayData`]
    pub fn downcast<P: Primitive>(self) -> Option<PrimitiveArrayData<P>> {
        P::downcast(self)
    }
}

/// ArrayData for [fixed size arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout) of [`Primitive`]
#[derive(Debug, Clone)]
pub struct PrimitiveArrayData<T: Primitive> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    values: ScalarBuffer<T>,
}

impl<P: Primitive> From<PrimitiveArrayData<P>> for ArrayDataPrimitive {
    fn from(value: PrimitiveArrayData<P>) -> Self {
        P::upcast(value)
    }
}

impl<T: Primitive> PrimitiveArrayData<T> {
    /// Create a new [`PrimitiveArrayData`]
    ///
    /// # Panics
    ///
    /// Panics if
    /// - `nulls` and `values` are different lengths
    /// - `data_type` is not compatible with `T`
    pub fn new(
        data_type: DataType,
        values: ScalarBuffer<T>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let physical = PhysicalType::from(&data_type);
        assert!(
            matches!(physical, PhysicalType::Primitive(p) if p == T::TYPE),
            "Illegal physical type for PrimitiveArrayData of datatype {:?}, expected {:?} got {:?}",
            data_type,
            T::TYPE,
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

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the primitive values
    #[inline]
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
