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
use crate::data::ArrayDataLayout;
use crate::{ArrayDataBuilder, Buffers};
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

/// Applies op to each variant of [`ArrayDataPrimitive`]
macro_rules! primitive_op {
    ($array:ident, $op:block) => {
        match $array {
            ArrayDataPrimitive::Int8($array) => $op
            ArrayDataPrimitive::Int16($array) => $op
            ArrayDataPrimitive::Int32($array) => $op
            ArrayDataPrimitive::Int64($array) => $op
            ArrayDataPrimitive::Int128($array) => $op
            ArrayDataPrimitive::Int256($array) => $op
            ArrayDataPrimitive::UInt8($array) => $op
            ArrayDataPrimitive::UInt16($array) => $op
            ArrayDataPrimitive::UInt32($array) => $op
            ArrayDataPrimitive::UInt64($array) => $op
            ArrayDataPrimitive::Float16($array) => $op
            ArrayDataPrimitive::Float32($array) => $op
            ArrayDataPrimitive::Float64($array) => $op
        }
    };
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
#[derive(Debug, Clone)]
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

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let s = self;
        primitive_op!(s, { s.slice(offset, len).into() })
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        let s = self;
        primitive_op!(s, { s.layout() })
    }

    /// Creates a new [`ArrayDataPrimitive`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`PrimitiveArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(
        builder: ArrayDataBuilder,
        primitive: PrimitiveType,
    ) -> Self {
        use PrimitiveType::*;
        match primitive {
            Int8 => Self::Int8(PrimitiveArrayData::from_raw(builder)),
            Int16 => Self::Int16(PrimitiveArrayData::from_raw(builder)),
            Int32 => Self::Int32(PrimitiveArrayData::from_raw(builder)),
            Int64 => Self::Int64(PrimitiveArrayData::from_raw(builder)),
            Int128 => Self::Int128(PrimitiveArrayData::from_raw(builder)),
            Int256 => Self::Int256(PrimitiveArrayData::from_raw(builder)),
            UInt8 => Self::UInt8(PrimitiveArrayData::from_raw(builder)),
            UInt16 => Self::UInt16(PrimitiveArrayData::from_raw(builder)),
            UInt32 => Self::UInt32(PrimitiveArrayData::from_raw(builder)),
            UInt64 => Self::UInt64(PrimitiveArrayData::from_raw(builder)),
            Float16 => Self::Float16(PrimitiveArrayData::from_raw(builder)),
            Float32 => Self::Float32(PrimitiveArrayData::from_raw(builder)),
            Float64 => Self::Float64(PrimitiveArrayData::from_raw(builder)),
        }
    }
}

impl<P: Primitive> From<PrimitiveArrayData<P>> for ArrayDataPrimitive {
    fn from(value: PrimitiveArrayData<P>) -> Self {
        P::upcast(value)
    }
}

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
    /// - `nulls` and `values` are different lengths
    /// - `data_type` is not compatible with `T`
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
    /// - `nulls` and `values` must be the same length
    /// - `data_type` must be compatible with `T`
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

    /// Creates a new [`PrimitiveArrayData`] from an [`ArrayDataBuilder`]
    ///
    /// # Safety
    ///
    /// See [`PrimitiveArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder) -> Self {
        let values = builder.buffers.into_iter().next().unwrap();
        let values = ScalarBuffer::new(values, builder.offset, builder.len);
        Self {
            values,
            data_type: builder.data_type,
            nulls: builder.nulls,
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

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.values.len(),
            offset: 0,
            nulls: self.nulls.as_ref(),
            buffers: Buffers::one(self.values.inner()),
            child_data: &[],
        }
    }
}
