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

use crate::data::types::{BytesType, OffsetType};
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType;
use std::marker::PhantomData;

mod private {
    use super::*;

    pub trait BytesSealed {
        /// Create from bytes without performing any validation
        ///
        /// # Safety
        ///
        /// If `str`, `b` must be a valid UTF-8 sequence
        unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self;

        /// Downcast [`ArrayDataBytes`] to `[ArrayDataBytesOffset`]
        fn downcast_ref(data: &ArrayDataBytes) -> Option<&ArrayDataBytesOffset<Self>>
        where
            Self: Bytes;

        /// Downcast [`ArrayDataBytes`] to `[ArrayDataBytesOffset`]
        fn downcast(data: ArrayDataBytes) -> Option<ArrayDataBytesOffset<Self>>
        where
            Self: Bytes;

        /// Cast [`ArrayDataBytesOffset`] to [`ArrayDataBytes`]
        fn upcast(v: ArrayDataBytesOffset<Self>) -> ArrayDataBytes
        where
            Self: Bytes;
    }

    pub trait BytesOffsetSealed {
        /// Downcast [`ArrayDataBytesOffset`] to `[BytesArrayData`]
        fn downcast_ref<B: Bytes + ?Sized>(
            data: &ArrayDataBytesOffset<B>,
        ) -> Option<&BytesArrayData<Self, B>>
        where
            Self: BytesOffset;

        /// Downcast [`ArrayDataBytesOffset`] to `[BytesArrayData`]
        fn downcast<B: Bytes + ?Sized>(
            data: ArrayDataBytesOffset<B>,
        ) -> Option<BytesArrayData<Self, B>>
        where
            Self: BytesOffset;

        /// Cast [`BytesArrayData`] to [`ArrayDataBytesOffset`]
        fn upcast<B: Bytes + ?Sized>(
            v: BytesArrayData<Self, B>,
        ) -> ArrayDataBytesOffset<B>
        where
            Self: BytesOffset;
    }
}

/// Types backed by a variable length slice of bytes
pub trait Bytes: private::BytesSealed {
    const TYPE: BytesType;
}

impl Bytes for [u8] {
    const TYPE: BytesType = BytesType::Binary;
}

impl private::BytesSealed for [u8] {
    unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
        b
    }

    fn downcast_ref(data: &ArrayDataBytes) -> Option<&ArrayDataBytesOffset<Self>> {
        match data {
            ArrayDataBytes::Binary(v) => Some(v),
            ArrayDataBytes::Utf8(_) => None,
        }
    }

    fn downcast(data: ArrayDataBytes) -> Option<ArrayDataBytesOffset<Self>> {
        match data {
            ArrayDataBytes::Binary(v) => Some(v),
            ArrayDataBytes::Utf8(_) => None,
        }
    }

    fn upcast(v: ArrayDataBytesOffset<Self>) -> ArrayDataBytes {
        ArrayDataBytes::Binary(v)
    }
}

impl Bytes for str {
    const TYPE: BytesType = BytesType::Utf8;
}

impl private::BytesSealed for str {
    unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
        std::str::from_utf8_unchecked(b)
    }

    fn downcast_ref(data: &ArrayDataBytes) -> Option<&ArrayDataBytesOffset<Self>> {
        match data {
            ArrayDataBytes::Binary(_) => None,
            ArrayDataBytes::Utf8(v) => Some(v),
        }
    }

    fn downcast(data: ArrayDataBytes) -> Option<ArrayDataBytesOffset<Self>> {
        match data {
            ArrayDataBytes::Binary(_) => None,
            ArrayDataBytes::Utf8(v) => Some(v),
        }
    }

    fn upcast(v: ArrayDataBytesOffset<Self>) -> ArrayDataBytes {
        ArrayDataBytes::Utf8(v)
    }
}

/// Types of offset used by variable length byte arrays
pub trait BytesOffset: private::BytesOffsetSealed + ArrowNativeType {
    const TYPE: OffsetType;
}

impl BytesOffset for i32 {
    const TYPE: OffsetType = OffsetType::Int32;
}

impl private::BytesOffsetSealed for i32 {
    fn downcast_ref<B: Bytes + ?Sized>(
        data: &ArrayDataBytesOffset<B>,
    ) -> Option<&BytesArrayData<Self, B>> {
        match data {
            ArrayDataBytesOffset::Small(v) => Some(v),
            ArrayDataBytesOffset::Large(_) => None,
        }
    }

    fn downcast<B: Bytes + ?Sized>(
        data: ArrayDataBytesOffset<B>,
    ) -> Option<BytesArrayData<Self, B>> {
        match data {
            ArrayDataBytesOffset::Small(v) => Some(v),
            ArrayDataBytesOffset::Large(_) => None,
        }
    }

    fn upcast<B: Bytes + ?Sized>(v: BytesArrayData<Self, B>) -> ArrayDataBytesOffset<B> {
        ArrayDataBytesOffset::Small(v)
    }
}

impl BytesOffset for i64 {
    const TYPE: OffsetType = OffsetType::Int64;
}

impl private::BytesOffsetSealed for i64 {
    fn downcast_ref<B: Bytes + ?Sized>(
        data: &ArrayDataBytesOffset<B>,
    ) -> Option<&BytesArrayData<Self, B>> {
        match data {
            ArrayDataBytesOffset::Small(_) => None,
            ArrayDataBytesOffset::Large(v) => Some(v),
        }
    }

    fn downcast<B: Bytes + ?Sized>(
        data: ArrayDataBytesOffset<B>,
    ) -> Option<BytesArrayData<Self, B>> {
        match data {
            ArrayDataBytesOffset::Small(_) => None,
            ArrayDataBytesOffset::Large(v) => Some(v),
        }
    }

    fn upcast<B: Bytes + ?Sized>(v: BytesArrayData<Self, B>) -> ArrayDataBytesOffset<B> {
        ArrayDataBytesOffset::Large(v)
    }
}

/// An enumeration of the types of [`ArrayDataBytesOffset`]
pub enum ArrayDataBytes {
    Binary(ArrayDataBytesOffset<[u8]>),
    Utf8(ArrayDataBytesOffset<str>),
}

impl ArrayDataBytes {
    /// Downcast this [`ArrayDataBytes`] to the corresponding [`BytesArrayData`]
    pub fn downcast_ref<O: BytesOffset, B: Bytes + ?Sized>(
        &self,
    ) -> Option<&BytesArrayData<O, B>> {
        O::downcast_ref(B::downcast_ref(self)?)
    }

    /// Downcast this [`ArrayDataBytes`] to the corresponding [`BytesArrayData`]
    pub fn downcast<O: BytesOffset, B: Bytes + ?Sized>(
        self,
    ) -> Option<BytesArrayData<O, B>> {
        O::downcast(B::downcast(self)?)
    }
}

/// An enumeration of the types of [`BytesArrayData`]
pub enum ArrayDataBytesOffset<B: Bytes + ?Sized> {
    Small(BytesArrayData<i32, B>),
    Large(BytesArrayData<i64, B>),
}

impl<O: BytesOffset, B: Bytes + ?Sized> From<BytesArrayData<O, B>> for ArrayDataBytes {
    fn from(value: BytesArrayData<O, B>) -> Self {
        B::upcast(O::upcast(value))
    }
}

/// ArrayData for arrays of [`Bytes`]
pub struct BytesArrayData<O: BytesOffset, B: Bytes + ?Sized> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    offsets: ScalarBuffer<O>,
    values: Buffer,
    phantom: PhantomData<B>,
}

impl<O: BytesOffset, B: Bytes> BytesArrayData<O, B> {
    /// Creates a new [`BytesArrayData`]
    ///
    /// # Safety
    ///
    /// - Each consecutive window of `offsets` must identify a valid slice of `values`
    /// - `nulls.len() == offsets.len() + 1`
    /// - `data_type` must be valid for this layout
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: ScalarBuffer<O>,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type,
            nulls,
            offsets,
            values,
            phantom: Default::default(),
        }
    }

    /// Returns the raw byte data
    #[inline]
    pub fn values(&self) -> &B {
        // Safety:
        // Bytes must be valid
        unsafe { B::from_bytes_unchecked(self.values.as_slice()) }
    }

    /// Returns the offsets
    #[inline]
    pub fn offsets(&self) -> &[O] {
        &self.offsets
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn null_buffer(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

/// ArrayData for fixed size binary arrays
pub struct FixedSizeBinaryArrayData {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    values: Buffer,
}

impl FixedSizeBinaryArrayData {
    /// Creates a new [`FixedSizeBinaryArrayData`]
    ///
    /// # Safety
    ///
    /// - `data_type` must be valid for this layout
    /// - `nulls.len() == values.len() / element_size`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type,
            nulls,
            values,
        }
    }

    /// Returns the raw byte data
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.values
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn null_buffer(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
