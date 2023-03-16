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
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType;
use std::marker::PhantomData;

mod private {
    pub trait BytesSealed {
        /// Create from bytes without performing any validation
        ///
        /// # Safety
        ///
        /// If `str`, `b` must be a valid UTF-8 sequence
        unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self;
    }
    pub trait BytesOffsetSealed {}
}

/// Types backed by a variable length slice of bytes
pub trait Bytes: private::BytesSealed + std::fmt::Debug {
    const TYPE: BytesType;
}

impl Bytes for [u8] {
    const TYPE: BytesType = BytesType::Binary;
}

impl private::BytesSealed for [u8] {
    unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
        b
    }
}

impl Bytes for str {
    const TYPE: BytesType = BytesType::Utf8;
}

impl private::BytesSealed for str {
    unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
        std::str::from_utf8_unchecked(b)
    }
}

/// Types of offset used by variable length byte arrays
pub trait BytesOffset: private::BytesOffsetSealed + ArrowNativeType {
    const TYPE: OffsetType;
}

impl BytesOffset for i32 {
    const TYPE: OffsetType = OffsetType::Int32;
}

impl private::BytesOffsetSealed for i32 {}

impl BytesOffset for i64 {
    const TYPE: OffsetType = OffsetType::Int64;
}

impl private::BytesOffsetSealed for i64 {}

/// ArrayData for [variable-sized arrays](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout) of [`Bytes`]
#[derive(Debug)]
pub struct BytesArrayData<O: BytesOffset, B: Bytes + ?Sized> {
    data_type: DataType,
    offsets: OffsetBuffer<O>,
    values: Buffer,
    nulls: Option<NullBuffer>,
    phantom: PhantomData<B>,
}

impl<O: BytesOffset, B: Bytes + ?Sized> Clone for BytesArrayData<O, B> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.clone(),
            offsets: self.offsets.clone(),
            values: self.values.clone(),
            phantom: Default::default(),
        }
    }
}

impl<O: BytesOffset, B: Bytes + ?Sized> BytesArrayData<O, B> {
    /// Creates a new [`BytesArrayData`]
    ///
    /// # Safety
    ///
    /// - Each consecutive window of `offsets` must identify a valid slice of `values`
    /// - `nulls.len() == offsets.len() - 1`
    /// - `PhysicalType::from(&data_type) == PhysicalType::Bytes(O::TYPE, B::TYPE)`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: OffsetBuffer<O>,
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

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len().wrapping_sub(1)
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
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
    pub fn offsets(&self) -> &OffsetBuffer<O> {
        &self.offsets
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`BytesArrayData`]
    pub fn into_parts(self) -> (DataType, OffsetBuffer<O>, Buffer, Option<NullBuffer>) {
        (self.data_type, self.offsets, self.values, self.nulls)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            values: self.values.clone(),
            offsets: self.offsets.slice(offset, len),
            data_type: self.data_type.clone(),
            nulls: self.nulls().as_ref().map(|x| x.slice(offset, len)),
            phantom: Default::default(),
        }
    }
}

impl<O: BytesOffset, B: Bytes + ?Sized> From<ArrayData> for BytesArrayData<O, B> {
    fn from(data: ArrayData) -> Self {
        let mut iter = data.buffers.into_iter();
        let offsets = iter.next().unwrap();
        let values = iter.next().unwrap();

        let offsets = match data.len {
            0 => OffsetBuffer::new_empty(),
            // Safety:
            // ArrayData is valid
            _ => unsafe {
                OffsetBuffer::new_unchecked(ScalarBuffer::new(
                    offsets,
                    data.offset,
                    data.len + 1,
                ))
            },
        };

        Self {
            values,
            offsets,
            data_type: data.data_type,
            nulls: data.nulls,
            phantom: Default::default(),
        }
    }
}

impl<O: BytesOffset, B: Bytes + ?Sized> From<BytesArrayData<O, B>> for ArrayData {
    fn from(value: BytesArrayData<O, B>) -> Self {
        Self {
            data_type: value.data_type,
            len: value.offsets.len().wrapping_sub(1),
            offset: 0,
            nulls: value.nulls,
            buffers: vec![value.offsets.into_inner().into_inner(), value.values],
            child_data: vec![],
        }
    }
}

/// ArrayData for [fixed-size arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout) of bytes
#[derive(Debug, Clone)]
pub struct FixedSizeBinaryArrayData {
    data_type: DataType,
    len: usize,
    element_size: usize,
    values: Buffer,
    nulls: Option<NullBuffer>,
}

impl FixedSizeBinaryArrayData {
    /// Creates a new [`FixedSizeBinaryArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::FixedSizeBinary(element_size)`
    /// - `nulls.len() == values.len() / element_size == len`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        len: usize,
        element_size: usize,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            data_type,
            nulls,
            values,
            len,
            element_size,
        }
    }

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the size of each element
    #[inline]
    pub fn element_size(&self) -> usize {
        self.element_size
    }

    /// Returns the raw byte data
    #[inline]
    pub fn values(&self) -> &[u8] {
        &self.values
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`FixedSizeBinaryArrayData`]
    pub fn into_parts(self) -> (DataType, Buffer, Option<NullBuffer>) {
        (self.data_type, self.values, self.nulls)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let offset_element = offset.checked_mul(self.element_size).expect("overflow");
        let len_element = len.checked_mul(self.element_size).expect("overflow");
        let values = self.values.slice_with_length(offset_element, len_element);

        Self {
            len,
            values,
            data_type: self.data_type.clone(),
            element_size: self.element_size,
            nulls: self.nulls().as_ref().map(|x| x.slice(offset, len)),
        }
    }
}

impl From<ArrayData> for FixedSizeBinaryArrayData {
    fn from(data: ArrayData) -> Self {
        let size = match data.data_type {
            DataType::FixedSizeBinary(size) => size as usize,
            d => panic!("invalid data type for FixedSizeBinaryArrayData: {d}"),
        };

        let values =
            data.buffers[0].slice_with_length(data.offset * size, data.len * size);
        Self {
            values,
            data_type: data.data_type,
            len: data.len,
            element_size: size,
            nulls: data.nulls,
        }
    }
}

impl From<FixedSizeBinaryArrayData> for ArrayData {
    fn from(value: FixedSizeBinaryArrayData) -> Self {
        Self {
            data_type: value.data_type,
            len: value.len,
            offset: 0,
            buffers: vec![value.values],
            child_data: vec![],
            nulls: value.nulls,
        }
    }
}
