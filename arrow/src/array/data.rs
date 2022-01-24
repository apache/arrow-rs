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

//! Contains `ArrayData`, a generic representation of Arrow array data which encapsulates
//! common attributes and operations for Arrow array.

use crate::datatypes::{DataType, IntervalUnit, UnionMode};
use crate::error::{ArrowError, Result};
use crate::{bitmap::Bitmap, datatypes::ArrowNativeType};
use crate::{
    buffer::{Buffer, MutableBuffer},
    util::bit_util,
};
use half::f16;
use std::convert::TryInto;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use super::equal::equal;

#[inline]
pub(crate) fn count_nulls(
    null_bit_buffer: Option<&Buffer>,
    offset: usize,
    len: usize,
) -> usize {
    if let Some(buf) = null_bit_buffer {
        len.checked_sub(buf.count_set_bits_offset(offset, len))
            .unwrap()
    } else {
        0
    }
}

/// creates 2 [`MutableBuffer`]s with a given `capacity` (in slots).
#[inline]
pub(crate) fn new_buffers(data_type: &DataType, capacity: usize) -> [MutableBuffer; 2] {
    let empty_buffer = MutableBuffer::new(0);
    match data_type {
        DataType::Null => [empty_buffer, MutableBuffer::new(0)],
        DataType::Boolean => {
            let bytes = bit_util::ceil(capacity, 8);
            let buffer = MutableBuffer::new(bytes);
            [buffer, empty_buffer]
        }
        DataType::UInt8 => [
            MutableBuffer::new(capacity * mem::size_of::<u8>()),
            empty_buffer,
        ],
        DataType::UInt16 => [
            MutableBuffer::new(capacity * mem::size_of::<u16>()),
            empty_buffer,
        ],
        DataType::UInt32 => [
            MutableBuffer::new(capacity * mem::size_of::<u32>()),
            empty_buffer,
        ],
        DataType::UInt64 => [
            MutableBuffer::new(capacity * mem::size_of::<u64>()),
            empty_buffer,
        ],
        DataType::Int8 => [
            MutableBuffer::new(capacity * mem::size_of::<i8>()),
            empty_buffer,
        ],
        DataType::Int16 => [
            MutableBuffer::new(capacity * mem::size_of::<i16>()),
            empty_buffer,
        ],
        DataType::Int32 => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Int64 => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Float16 => [
            MutableBuffer::new(capacity * mem::size_of::<f16>()),
            empty_buffer,
        ],
        DataType::Float32 => [
            MutableBuffer::new(capacity * mem::size_of::<f32>()),
            empty_buffer,
        ],
        DataType::Float64 => [
            MutableBuffer::new(capacity * mem::size_of::<f64>()),
            empty_buffer,
        ],
        DataType::Date32 | DataType::Time32(_) => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(_, _) => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Interval(IntervalUnit::YearMonth) => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Interval(IntervalUnit::DayTime) => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Interval(IntervalUnit::MonthDayNano) => [
            MutableBuffer::new(capacity * mem::size_of::<i128>()),
            empty_buffer,
        ],
        DataType::Utf8 | DataType::Binary => {
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i32>());
            // safety: `unsafe` code assumes that this buffer is initialized with one element
            buffer.push(0i32);
            [buffer, MutableBuffer::new(capacity * mem::size_of::<u8>())]
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i64>());
            // safety: `unsafe` code assumes that this buffer is initialized with one element
            buffer.push(0i64);
            [buffer, MutableBuffer::new(capacity * mem::size_of::<u8>())]
        }
        DataType::List(_) | DataType::Map(_, _) => {
            // offset buffer always starts with a zero
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i32>());
            buffer.push(0i32);
            [buffer, empty_buffer]
        }
        DataType::LargeList(_) => {
            // offset buffer always starts with a zero
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i64>());
            buffer.push(0i64);
            [buffer, empty_buffer]
        }
        DataType::FixedSizeBinary(size) => {
            [MutableBuffer::new(capacity * *size as usize), empty_buffer]
        }
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => [
                MutableBuffer::new(capacity * mem::size_of::<u8>()),
                empty_buffer,
            ],
            DataType::UInt16 => [
                MutableBuffer::new(capacity * mem::size_of::<u16>()),
                empty_buffer,
            ],
            DataType::UInt32 => [
                MutableBuffer::new(capacity * mem::size_of::<u32>()),
                empty_buffer,
            ],
            DataType::UInt64 => [
                MutableBuffer::new(capacity * mem::size_of::<u64>()),
                empty_buffer,
            ],
            DataType::Int8 => [
                MutableBuffer::new(capacity * mem::size_of::<i8>()),
                empty_buffer,
            ],
            DataType::Int16 => [
                MutableBuffer::new(capacity * mem::size_of::<i16>()),
                empty_buffer,
            ],
            DataType::Int32 => [
                MutableBuffer::new(capacity * mem::size_of::<i32>()),
                empty_buffer,
            ],
            DataType::Int64 => [
                MutableBuffer::new(capacity * mem::size_of::<i64>()),
                empty_buffer,
            ],
            _ => unreachable!(),
        },
        DataType::FixedSizeList(_, _) | DataType::Struct(_) => {
            [empty_buffer, MutableBuffer::new(0)]
        }
        DataType::Decimal(_, _) => [
            MutableBuffer::new(capacity * mem::size_of::<u8>()),
            empty_buffer,
        ],
        DataType::Union(_, _) => unimplemented!(),
    }
}

/// Ensures that at least `min_size` elements of type `data_type` can
/// be stored in a buffer of `buffer_size`.
///
/// `buffer_index` is used in error messages to identify which buffer
/// had the invalid index
fn ensure_size(
    data_type: &DataType,
    min_size: usize,
    buffer_size: usize,
    buffer_index: usize,
) -> Result<()> {
    // if min_size is zero, may not have buffers (e.g. NullArray)
    if min_size > 0 && buffer_size < min_size {
        Err(ArrowError::InvalidArgumentError(format!(
            "Need at least {} bytes in buffers[{}] in array of type {:?}, but got {}",
            buffer_size, buffer_index, data_type, min_size
        )))
    } else {
        Ok(())
    }
}

/// Maps 2 [`MutableBuffer`]s into a vector of [Buffer]s whose size depends on `data_type`.
#[inline]
pub(crate) fn into_buffers(
    data_type: &DataType,
    buffer1: MutableBuffer,
    buffer2: MutableBuffer,
) -> Vec<Buffer> {
    match data_type {
        DataType::Null | DataType::Struct(_) => vec![],
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => vec![buffer1.into(), buffer2.into()],
        _ => vec![buffer1.into()],
    }
}

/// An generic representation of Arrow array data which encapsulates common attributes and
/// operations for Arrow array. Specific operations for different arrays types (e.g.,
/// primitive, list, struct) are implemented in `Array`.
#[derive(Debug, Clone)]
pub struct ArrayData {
    /// The data type for this array data
    data_type: DataType,

    /// The number of elements in this array data
    len: usize,

    /// The number of null elements in this array data
    null_count: usize,

    /// The offset into this array data, in number of items
    offset: usize,

    /// The buffers for this array data. Note that depending on the array types, this
    /// could hold different kinds of buffers (e.g., value buffer, value offset buffer)
    /// at different positions.
    buffers: Vec<Buffer>,

    /// The child(ren) of this array. Only non-empty for nested types, currently
    /// `ListArray` and `StructArray`.
    child_data: Vec<ArrayData>,

    /// The null bitmap. A `None` value for this indicates all values are non-null in
    /// this array.
    null_bitmap: Option<Bitmap>,
}

pub type ArrayDataRef = Arc<ArrayData>;

impl ArrayData {
    /// Create a new ArrayData instance;
    ///
    /// If `null_count` is not specified, the number of nulls in
    /// null_bit_buffer is calculated
    ///
    /// # Safety
    ///
    /// The input values *must* form a valid Arrow array for
    /// `data_type`, or undefined behavior can results.
    ///
    /// Note: This is a low level API and most users of the arrow
    /// crate should create arrays using the methods in the `array`
    /// module.
    pub unsafe fn new_unchecked(
        data_type: DataType,
        len: usize,
        null_count: Option<usize>,
        null_bit_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrayData>,
    ) -> Self {
        let null_count = match null_count {
            None => count_nulls(null_bit_buffer.as_ref(), offset, len),
            Some(null_count) => null_count,
        };
        let null_bitmap = null_bit_buffer.map(Bitmap::from);
        Self {
            data_type,
            len,
            null_count,
            offset,
            buffers,
            child_data,
            null_bitmap,
        }
    }

    /// Create a new ArrayData, validating that the provided buffers
    /// form a valid Arrow array of the specified data type.
    ///
    /// If `null_count` is not specified, the number of nulls in
    /// null_bit_buffer is calculated
    ///
    /// Note: This is a low level API and most users of the arrow
    /// crate should create arrays using the methods in the `array`
    /// module.
    pub fn try_new(
        data_type: DataType,
        len: usize,
        null_count: Option<usize>,
        null_bit_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrayData>,
    ) -> Result<Self> {
        // Safety justification: `validate_full` is called below
        let new_self = unsafe {
            Self::new_unchecked(
                data_type,
                len,
                null_count,
                null_bit_buffer,
                offset,
                buffers,
                child_data,
            )
        };

        // As the data is not trusted, do a full validation of its contents
        new_self.validate_full()?;
        Ok(new_self)
    }

    /// Returns a builder to construct a `ArrayData` instance.
    #[inline]
    pub const fn builder(data_type: DataType) -> ArrayDataBuilder {
        ArrayDataBuilder::new(data_type)
    }

    /// Returns a reference to the data type of this array data
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns a slice of buffers for this array data
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers[..]
    }

    /// Returns a slice of children data arrays
    pub fn child_data(&self) -> &[ArrayData] {
        &self.child_data[..]
    }

    /// Returns whether the element at index `i` is null
    pub fn is_null(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return !b.is_set(self.offset + i);
        }
        false
    }

    /// Returns a reference to the null bitmap of this array data
    #[inline]
    pub const fn null_bitmap(&self) -> &Option<Bitmap> {
        &self.null_bitmap
    }

    /// Returns a reference to the null buffer of this array data.
    pub fn null_buffer(&self) -> Option<&Buffer> {
        self.null_bitmap().as_ref().map(|b| b.buffer_ref())
    }

    /// Returns whether the element at index `i` is not null
    pub fn is_valid(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return b.is_set(self.offset + i);
        }
        true
    }

    /// Returns the length (i.e., number of elements) of this array
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    // Returns whether array data is empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the offset of this array
    #[inline]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the total number of nulls in this array
    #[inline]
    pub const fn null_count(&self) -> usize {
        self.null_count
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [ArrayData].
    pub fn get_buffer_memory_size(&self) -> usize {
        let mut size = 0;
        for buffer in &self.buffers {
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            size += bitmap.get_buffer_memory_size()
        }
        for child in &self.child_data {
            size += child.get_buffer_memory_size();
        }
        size
    }

    /// Returns the total number of bytes of memory occupied physically by this [ArrayData].
    pub fn get_array_memory_size(&self) -> usize {
        let mut size = mem::size_of_val(self);

        // Calculate rest of the fields top down which contain actual data
        for buffer in &self.buffers {
            size += mem::size_of::<Buffer>();
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            // this includes the size of the bitmap struct itself, since it is stored directly in
            // this struct we already counted those bytes in the size_of_val(self) above
            size += bitmap.get_array_memory_size();
            size -= mem::size_of::<Bitmap>();
        }
        for child in &self.child_data {
            size += child.get_array_memory_size();
        }

        size
    }

    /// Creates a zero-copy slice of itself. This creates a new [ArrayData]
    /// with a different offset, len and a shifted null bitmap.
    ///
    /// # Panics
    ///
    /// Panics if `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> ArrayData {
        assert!((offset + length) <= self.len());

        if let DataType::Struct(_) = self.data_type() {
            // Slice into children
            let new_offset = self.offset + offset;
            let new_data = ArrayData {
                data_type: self.data_type().clone(),
                len: length,
                null_count: count_nulls(self.null_buffer(), new_offset, length),
                offset: new_offset,
                buffers: self.buffers.clone(),
                // Slice child data, to propagate offsets down to them
                child_data: self
                    .child_data()
                    .iter()
                    .map(|data| data.slice(offset, length))
                    .collect(),
                null_bitmap: self.null_bitmap().clone(),
            };

            new_data
        } else {
            let mut new_data = self.clone();

            new_data.len = length;
            new_data.offset = offset + self.offset;

            new_data.null_count =
                count_nulls(new_data.null_buffer(), new_data.offset, new_data.len);

            new_data
        }
    }

    /// Returns the `buffer` as a slice of type `T` starting at self.offset
    /// # Panics
    /// This function panics if:
    /// * the buffer is not byte-aligned with type T, or
    /// * the datatype is `Boolean` (it corresponds to a bit-packed buffer where the offset is not applicable)
    #[inline]
    pub(crate) fn buffer<T: ArrowNativeType>(&self, buffer: usize) -> &[T] {
        let values = unsafe { self.buffers[buffer].as_slice().align_to::<T>() };
        if !values.0.is_empty() || !values.2.is_empty() {
            panic!("The buffer is not byte-aligned with its interpretation")
        };
        assert_ne!(self.data_type, DataType::Boolean);
        &values.1[self.offset..]
    }

    /// Returns a new empty [ArrayData] valid for `data_type`.
    pub fn new_empty(data_type: &DataType) -> Self {
        let buffers = new_buffers(data_type, 0);
        let [buffer1, buffer2] = buffers;
        let buffers = into_buffers(data_type, buffer1, buffer2);

        let child_data = match data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal(_, _) => vec![],
            DataType::List(field) => {
                vec![Self::new_empty(field.data_type())]
            }
            DataType::FixedSizeList(field, _) => {
                vec![Self::new_empty(field.data_type())]
            }
            DataType::LargeList(field) => {
                vec![Self::new_empty(field.data_type())]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| Self::new_empty(field.data_type()))
                .collect(),
            DataType::Map(field, _) => {
                vec![Self::new_empty(field.data_type())]
            }
            DataType::Union(_, _) => unimplemented!(),
            DataType::Dictionary(_, data_type) => {
                vec![Self::new_empty(data_type)]
            }
        };

        // Data was constructed correctly above
        unsafe {
            Self::new_unchecked(
                data_type.clone(),
                0,
                Some(0),
                None,
                0,
                buffers,
                child_data,
            )
        }
    }

    /// "cheap" validation of an `ArrayData`. Ensures buffers are
    /// sufficiently sized to store `len` + `offset` total elements of
    /// `data_type` and performs other inexpensive consistency checks.
    ///
    /// This check is "cheap" in the sense that it does not validate the
    /// contents of the buffers (e.g. that all offsets for UTF8 arrays
    /// are within the bounds of the values buffer).
    ///
    /// See [`validate_full`] to validate fully the offset content
    /// and the validitiy of utf8 data
    pub fn validate(&self) -> Result<()> {
        // Need at least this mich space in each buffer
        let len_plus_offset = self.len + self.offset;

        // Check that the data layout conforms to the spec
        let layout = layout(&self.data_type);

        if self.buffers.len() != layout.buffers.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected {} buffers in array of type {:?}, got {}",
                layout.buffers.len(),
                self.data_type,
                self.buffers.len(),
            )));
        }

        for (i, (buffer, spec)) in
            self.buffers.iter().zip(layout.buffers.iter()).enumerate()
        {
            match spec {
                BufferSpec::FixedWidth { byte_width } => {
                    let min_buffer_size = len_plus_offset
                        .checked_mul(*byte_width)
                        .expect("integer overflow computing min buffer size");

                    if buffer.len() < min_buffer_size {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Need at least {} bytes in buffers[{}] in array of type {:?}, but got {}",
                            min_buffer_size, i, self.data_type, buffer.len()
                        )));
                    }
                }
                BufferSpec::VariableWidth => {
                    // not cheap to validate (need to look at the
                    // data). Partially checked in validate_offsets
                    // called below. Can check with `validate_full`
                }
                BufferSpec::BitMap => {
                    let min_buffer_size = bit_util::ceil(len_plus_offset, 8);
                    if buffer.len() < min_buffer_size {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Need at least {} bytes for bitmap in buffers[{}] in array of type {:?}, but got {}",
                            min_buffer_size, i, self.data_type, buffer.len()
                        )));
                    }
                }
                BufferSpec::AlwaysNull => {
                    // Nothing to validate
                }
            }
        }

        if self.null_count > self.len {
            return Err(ArrowError::InvalidArgumentError(format!(
                "null_count {} for an array exceeds length of {} elements",
                self.null_count, self.len
            )));
        }

        // check null bit buffer size
        if let Some(null_bit_map) = self.null_bitmap.as_ref() {
            let null_bit_buffer = null_bit_map.buffer_ref();
            let needed_len = bit_util::ceil(len_plus_offset, 8);
            if null_bit_buffer.len() < needed_len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "null_bit_buffer size too small. got {} needed {}",
                    null_bit_buffer.len(),
                    needed_len
                )));
            }
        } else if self.null_count > 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Array of type {} has {} nulls but no null bitmap",
                self.data_type, self.null_count
            )));
        }

        self.validate_child_data()?;

        // Additional Type specific checks
        match &self.data_type {
            DataType::Utf8 | DataType::Binary => {
                self.validate_offsets::<i32>(&self.buffers[0], self.buffers[1].len())?;
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                self.validate_offsets::<i64>(&self.buffers[0], self.buffers[1].len())?;
            }
            DataType::Dictionary(key_type, _value_type) => {
                // At the moment, constructing a DictionaryArray will also check this
                if !DataType::is_dictionary_key_type(key_type) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionary values must be integer, but was {}",
                        key_type
                    )));
                }
            }
            _ => {}
        };

        Ok(())
    }

    /// Returns a reference to the data in `buffer` as a typed slice
    /// (typically `&[i32]` or `&[i64]`) after validating. The
    /// returned slice is guaranteed to have at least `self.len + 1`
    /// entries
    fn typed_offsets<'a, T: ArrowNativeType + num::Num + std::fmt::Display>(
        &'a self,
        buffer: &'a Buffer,
    ) -> Result<&'a [T]> {
        // Validate that there are the correct number of offsets for this array's length
        let required_offsets = self.len + self.offset + 1;

        // An empty list-like array can have 0 offsets
        if buffer.is_empty() {
            return Ok(&[]);
        }

        if (buffer.len() / std::mem::size_of::<T>()) < required_offsets {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Offsets buffer size (bytes): {} isn't large enough for {}. Length {} needs {}",
                buffer.len(), self.data_type, self.len, required_offsets
            )));
        }

        // Justification: buffer size was validated above
        Ok(unsafe {
            &(buffer.typed_data::<T>()[self.offset..self.offset + self.len + 1])
        })
    }

    /// Does a cheap sanity check that the `self.len` values in `buffer` are valid
    /// offsets (of type T) into some other buffer of `values_length` bytes long
    fn validate_offsets<T: ArrowNativeType + num::Num + std::fmt::Display>(
        &self,
        buffer: &Buffer,
        values_length: usize,
    ) -> Result<()> {
        // Justification: buffer size was validated above
        let offsets = self.typed_offsets::<T>(buffer)?;
        if offsets.is_empty() {
            return Ok(());
        }

        let first_offset = offsets[0].to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Error converting offset[0] ({}) to usize for {}",
                offsets[0], self.data_type
            ))
        })?;

        let last_offset = offsets[self.len].to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Error converting offset[{}] ({}) to usize for {}",
                self.len, offsets[self.len], self.data_type
            ))
        })?;

        if first_offset > values_length {
            return Err(ArrowError::InvalidArgumentError(format!(
                "First offset {} of {} is larger than values length {}",
                first_offset, self.data_type, values_length,
            )));
        }

        if last_offset > values_length {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Last offset {} of {} is larger than values length {}",
                last_offset, self.data_type, values_length,
            )));
        }

        if first_offset > last_offset {
            return Err(ArrowError::InvalidArgumentError(format!(
                "First offset {} in {} is smaller than last offset {}",
                first_offset, self.data_type, last_offset,
            )));
        }

        Ok(())
    }

    /// Validates the layout of `child_data` ArrayData structures
    fn validate_child_data(&self) -> Result<()> {
        match &self.data_type {
            DataType::List(field) | DataType::Map(field, _) => {
                let values_data = self.get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i32>(&self.buffers[0], values_data.len)?;
                Ok(())
            }
            DataType::LargeList(field) => {
                let values_data = self.get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i64>(&self.buffers[0], values_data.len)?;
                Ok(())
            }
            DataType::FixedSizeList(field, list_size) => {
                let values_data = self.get_single_valid_child_data(field.data_type())?;

                let list_size: usize = (*list_size).try_into().map_err(|_| {
                    ArrowError::InvalidArgumentError(format!(
                        "{} has a negative list_size {}",
                        self.data_type, list_size
                    ))
                })?;

                let expected_values_len = self.len
                    .checked_mul(list_size)
                    .expect("integer overflow computing expected number of expected values in FixedListSize");

                if values_data.len < expected_values_len {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Values length {} is less than the length ({}) multiplied by the value size ({}) for {}",
                        values_data.len, list_size, list_size, self.data_type
                    )));
                }

                Ok(())
            }
            DataType::Struct(fields) => {
                self.validate_num_child_data(fields.len())?;
                for (i, field) in fields.iter().enumerate() {
                    let field_data = self.get_valid_child_data(i, field.data_type())?;

                    // Ensure child field has sufficient size
                    if field_data.len < self.len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "{} child array #{} for field {} has length smaller than expected for struct array ({} < {})",
                            self.data_type, i, field.name(), field_data.len, self.len
                        )));
                    }
                }
                Ok(())
            }
            DataType::Union(fields, mode) => {
                self.validate_num_child_data(fields.len())?;

                for (i, field) in fields.iter().enumerate() {
                    let field_data = self.get_valid_child_data(i, field.data_type())?;

                    if mode == &UnionMode::Sparse
                        && field_data.len < (self.len + self.offset)
                    {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Sparse union child array #{} has length smaller than expected for union array ({} < {})",
                            i, field_data.len, self.len + self.offset
                        )));
                    }
                }
                Ok(())
            }
            DataType::Dictionary(_key_type, value_type) => {
                self.get_single_valid_child_data(value_type)?;
                Ok(())
            }
            _ => {
                // other types do not have child data
                if !self.child_data.is_empty() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Expected no child arrays for type {} but got {}",
                        self.data_type,
                        self.child_data.len()
                    )));
                }
                Ok(())
            }
        }
    }

    /// Ensures that this array data has a single child_data with the
    /// expected type, and calls `validate()` on it. Returns a
    /// reference to that child_data
    fn get_single_valid_child_data(
        &self,
        expected_type: &DataType,
    ) -> Result<&ArrayData> {
        self.validate_num_child_data(1)?;
        self.get_valid_child_data(0, expected_type)
    }

    /// Returns `Err` if self.child_data does not have exactly `expected_len` elements
    fn validate_num_child_data(&self, expected_len: usize) -> Result<()> {
        if self.child_data().len() != expected_len {
            Err(ArrowError::InvalidArgumentError(format!(
                "Value data for {} should contain {} child data array(s), had {}",
                self.data_type(),
                expected_len,
                self.child_data.len()
            )))
        } else {
            Ok(())
        }
    }

    /// Ensures that `child_data[i]` has the expected type, calls
    /// `validate()` on it, and returns a reference to that child_data
    fn get_valid_child_data(
        &self,
        i: usize,
        expected_type: &DataType,
    ) -> Result<&ArrayData> {
        let values_data = self.child_data
            .get(i)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "{} did not have enough child arrays. Expected at least {} but had only {}",
                    self.data_type, i+1, self.child_data.len()
                ))
            })?;

        if expected_type != &values_data.data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Child type mismatch for {}. Expected {} but child data had {}",
                self.data_type, expected_type, values_data.data_type
            )));
        }

        values_data.validate()?;
        Ok(values_data)
    }

    /// "expensive" validation that ensures:
    ///
    /// 1. Null count is correct
    /// 2. All offsets are valid
    /// 3. All String data is  valid UTF-8
    /// 3. All dictionary offsets are valid
    ///
    /// Does not (yet) check
    /// 1. Union type_ids are valid (see https://github.com/apache/arrow-rs/issues/85)
    /// Note calls `validate()` internally
    pub fn validate_full(&self) -> Result<()> {
        // Check all buffer sizes prior to looking at them more deeply in this function
        self.validate()?;

        let null_bitmap_buffer = self
            .null_bitmap
            .as_ref()
            .map(|null_bitmap| null_bitmap.buffer_ref());

        let actual_null_count = count_nulls(null_bitmap_buffer, self.offset, self.len);
        if actual_null_count != self.null_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "null_count value ({}) doesn't match actual number of nulls in array ({})",
                self.null_count, actual_null_count
            )));
        }

        match &self.data_type {
            DataType::Utf8 => {
                self.validate_utf8::<i32>()?;
            }
            DataType::LargeUtf8 => {
                self.validate_utf8::<i64>()?;
            }
            DataType::Binary => {
                self.validate_offsets_full::<i32>(self.buffers[1].len())?;
            }
            DataType::LargeBinary => {
                self.validate_offsets_full::<i64>(self.buffers[1].len())?;
            }
            DataType::List(_) | DataType::Map(_, _) => {
                let child = &self.child_data[0];
                self.validate_offsets_full::<i32>(child.len + child.offset)?;
            }
            DataType::LargeList(_) => {
                let child = &self.child_data[0];
                self.validate_offsets_full::<i64>(child.len + child.offset)?;
            }
            DataType::Union(_, _) => {
                // Validate Union Array as part of implementing new Union semantics
                // See comments in `ArrayData::validate()`
                // https://github.com/apache/arrow-rs/issues/85
                //
                // TODO file follow on ticket for full union validation
            }
            DataType::Dictionary(key_type, _value_type) => {
                let dictionary_length: i64 = self.child_data[0].len.try_into().unwrap();
                let max_value = dictionary_length - 1;
                match key_type.as_ref() {
                    DataType::UInt8 => self.check_bounds::<u8>(max_value)?,
                    DataType::UInt16 => self.check_bounds::<u16>(max_value)?,
                    DataType::UInt32 => self.check_bounds::<u32>(max_value)?,
                    DataType::UInt64 => self.check_bounds::<u64>(max_value)?,
                    DataType::Int8 => self.check_bounds::<i8>(max_value)?,
                    DataType::Int16 => self.check_bounds::<i16>(max_value)?,
                    DataType::Int32 => self.check_bounds::<i32>(max_value)?,
                    DataType::Int64 => self.check_bounds::<i64>(max_value)?,
                    _ => unreachable!(),
                }
            }
            _ => {
                // No extra validation check required for other types
            }
        };

        // validate all children recursively
        self.child_data
            .iter()
            .enumerate()
            .try_for_each(|(i, child_data)| {
                child_data.validate_full().map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "{} child #{} invalid: {}",
                        self.data_type, i, e
                    ))
                })
            })?;

        Ok(())
    }

    /// Calls the `validate(item_index, range)` function for each of
    /// the ranges specified in the arrow offset buffer of type
    /// `T`. Also validates that each offset is smaller than
    /// `max_offset`
    ///
    /// For example, the offset buffer contained `[1, 2, 4]`, this
    /// function would call `validate([1,2])`, and `validate([2,4])`
    fn validate_each_offset<T, V>(
        &self,
        offset_buffer: &Buffer,
        offset_limit: usize,
        validate: V,
    ) -> Result<()>
    where
        T: ArrowNativeType + std::convert::TryInto<usize> + num::Num + std::fmt::Display,
        V: Fn(usize, Range<usize>) -> Result<()>,
    {
        // An empty binary-like array can have 0 offsets
        if self.len == 0 && offset_buffer.is_empty() {
            return Ok(());
        }

        let offsets = self.typed_offsets::<T>(offset_buffer)?;

        offsets
            .iter()
            .zip(offsets.iter().skip(1))
            .enumerate()
            .map(|(i, (&start_offset, &end_offset))| {
                let start_offset: usize = start_offset
                    .try_into()
                    .map_err(|_| {
                        ArrowError::InvalidArgumentError(format!(
                            "Offset invariant failure: could not convert start_offset {} to usize in slot {}",
                            start_offset, i))
                    })?;
                let end_offset: usize = end_offset
                    .try_into()
                    .map_err(|_| {
                        ArrowError::InvalidArgumentError(format!(
                            "Offset invariant failure: Could not convert end_offset {} to usize in slot {}",
                            end_offset, i+1))
                    })?;

                if start_offset > offset_limit {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: offset for slot {} out of bounds: {} > {}",
                        i, start_offset, offset_limit))
                    );
                }

                if end_offset > offset_limit {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: offset for slot {} out of bounds: {} > {}",
                        i, end_offset, offset_limit))
                    );
                }

                // check range actually is low -> high
                if start_offset > end_offset {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: non-monotonic offset at slot {}: {} > {}",
                        i, start_offset, end_offset))
                    );
                }

                Ok((i, start_offset..end_offset))
            })
            .try_for_each(|res: Result<(usize, Range<usize>)>| {
                let (item_index, range) = res?;
                validate(item_index, range)
            })
    }

    /// Ensures that all strings formed by the offsets in buffers[0]
    /// into buffers[1] are valid utf8 sequences
    fn validate_utf8<T>(&self) -> Result<()>
    where
        T: ArrowNativeType + std::convert::TryInto<usize> + num::Num + std::fmt::Display,
    {
        let offset_buffer = &self.buffers[0];
        let values_buffer = &self.buffers[1].as_slice();

        self.validate_each_offset::<T, _>(
            offset_buffer,
            values_buffer.len(),
            |string_index, range| {
                std::str::from_utf8(&values_buffer[range.clone()]).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "Invalid UTF8 sequence at string index {} ({:?}): {}",
                        string_index, range, e
                    ))
                })?;
                Ok(())
            },
        )
    }

    /// Ensures that all offsets in buffers[0] into buffers[1] are
    /// between `0` and `offset_limit`
    fn validate_offsets_full<T>(&self, offset_limit: usize) -> Result<()>
    where
        T: ArrowNativeType + std::convert::TryInto<usize> + num::Num + std::fmt::Display,
    {
        let offset_buffer = &self.buffers[0];

        self.validate_each_offset::<T, _>(
            offset_buffer,
            offset_limit,
            |_string_index, _range| {
                // No validation applied to each value, but the iteration
                // itself applies bounds checking to each range
                Ok(())
            },
        )
    }

    /// Validates that each value in self.buffers (typed as T)
    /// is within the range [0, max_value], inclusive
    fn check_bounds<T>(&self, max_value: i64) -> Result<()>
    where
        T: ArrowNativeType + std::convert::TryInto<i64> + num::Num + std::fmt::Display,
    {
        let required_len = self.len + self.offset;
        let buffer = &self.buffers[0];

        // This should have been checked as part of `validate()` prior
        // to calling `validate_full()` but double check to be sure
        assert!(buffer.len() / std::mem::size_of::<T>() >= required_len);

        // Justification: buffer size was validated above
        let indexes: &[T] =
            unsafe { &(buffer.typed_data::<T>()[self.offset..self.offset + self.len]) };

        indexes.iter().enumerate().try_for_each(|(i, &dict_index)| {
            // Do not check the value is null (value can be arbitrary)
            if self.is_null(i) {
                return Ok(());
            }
            let dict_index: i64 = dict_index.try_into().map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "Value at position {} out of bounds: {} (can not convert to i64)",
                    i, dict_index
                ))
            })?;

            if dict_index < 0 || dict_index > max_value {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Value at position {} out of bounds: {} (should be in [0, {}])",
                    i, dict_index, max_value
                )));
            }
            Ok(())
        })
    }

    /// Returns true if this `ArrayData` is equal to `other`, using pointer comparisons
    /// to determine buffer equality. This is cheaper than `PartialEq::eq` but may
    /// return false when the arrays are logically equal
    pub fn ptr_eq(&self, other: &Self) -> bool {
        if self.offset != other.offset
            || self.len != other.len
            || self.null_count != other.null_count
            || self.data_type != other.data_type
            || self.buffers.len() != other.buffers.len()
            || self.child_data.len() != other.child_data.len()
        {
            return false;
        }

        match (&self.null_bitmap, &other.null_bitmap) {
            (Some(a), Some(b)) if a.bits.as_ptr() != b.bits.as_ptr() => return false,
            (Some(_), None) | (None, Some(_)) => return false,
            _ => {}
        };

        if !self
            .buffers
            .iter()
            .zip(other.buffers.iter())
            .all(|(a, b)| a.as_ptr() == b.as_ptr())
        {
            return false;
        }

        self.child_data
            .iter()
            .zip(other.child_data.iter())
            .all(|(a, b)| a.ptr_eq(b))
    }
}

/// Return the expected [`DataTypeLayout`] Arrays of this data
/// type are expected to have
fn layout(data_type: &DataType) -> DataTypeLayout {
    // based on C/C++ implementation in
    // https://github.com/apache/arrow/blob/661c7d749150905a63dd3b52e0a04dac39030d95/cpp/src/arrow/type.h (and .cc)
    use std::mem::size_of;
    match data_type {
        DataType::Null => DataTypeLayout::new_empty(),
        DataType::Boolean => DataTypeLayout {
            buffers: vec![BufferSpec::BitMap],
        },
        DataType::Int8 => DataTypeLayout::new_fixed_width(size_of::<i8>()),
        DataType::Int16 => DataTypeLayout::new_fixed_width(size_of::<i16>()),
        DataType::Int32 => DataTypeLayout::new_fixed_width(size_of::<i32>()),
        DataType::Int64 => DataTypeLayout::new_fixed_width(size_of::<i64>()),
        DataType::UInt8 => DataTypeLayout::new_fixed_width(size_of::<u8>()),
        DataType::UInt16 => DataTypeLayout::new_fixed_width(size_of::<u16>()),
        DataType::UInt32 => DataTypeLayout::new_fixed_width(size_of::<u32>()),
        DataType::UInt64 => DataTypeLayout::new_fixed_width(size_of::<u64>()),
        DataType::Float16 => DataTypeLayout::new_fixed_width(size_of::<f16>()),
        DataType::Float32 => DataTypeLayout::new_fixed_width(size_of::<f32>()),
        DataType::Float64 => DataTypeLayout::new_fixed_width(size_of::<f64>()),
        DataType::Timestamp(_, _) => DataTypeLayout::new_fixed_width(size_of::<i64>()),
        DataType::Date32 => DataTypeLayout::new_fixed_width(size_of::<i32>()),
        DataType::Date64 => DataTypeLayout::new_fixed_width(size_of::<i64>()),
        DataType::Time32(_) => DataTypeLayout::new_fixed_width(size_of::<i32>()),
        DataType::Time64(_) => DataTypeLayout::new_fixed_width(size_of::<i64>()),
        DataType::Interval(IntervalUnit::YearMonth) => {
            DataTypeLayout::new_fixed_width(size_of::<i32>())
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            DataTypeLayout::new_fixed_width(size_of::<i64>())
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            DataTypeLayout::new_fixed_width(size_of::<i128>())
        }
        DataType::Duration(_) => DataTypeLayout::new_fixed_width(size_of::<i64>()),
        DataType::Binary => DataTypeLayout::new_binary(size_of::<i32>()),
        DataType::FixedSizeBinary(bytes_per_value) => {
            let bytes_per_value: usize = (*bytes_per_value)
                .try_into()
                .expect("negative size for fixed size binary");
            DataTypeLayout::new_fixed_width(bytes_per_value)
        }
        DataType::LargeBinary => DataTypeLayout::new_binary(size_of::<i64>()),
        DataType::Utf8 => DataTypeLayout::new_binary(size_of::<i32>()),
        DataType::LargeUtf8 => DataTypeLayout::new_binary(size_of::<i64>()),
        DataType::List(_) => DataTypeLayout::new_fixed_width(size_of::<i32>()),
        DataType::FixedSizeList(_, _) => DataTypeLayout::new_empty(), // all in child data
        DataType::LargeList(_) => DataTypeLayout::new_fixed_width(size_of::<i32>()),
        DataType::Struct(_) => DataTypeLayout::new_empty(), // all in child data,
        DataType::Union(_, mode) => {
            let type_ids = BufferSpec::FixedWidth {
                byte_width: size_of::<i8>(),
            };

            DataTypeLayout {
                buffers: match mode {
                    UnionMode::Sparse => {
                        vec![type_ids]
                    }
                    UnionMode::Dense => {
                        vec![
                            type_ids,
                            BufferSpec::FixedWidth {
                                byte_width: size_of::<i32>(),
                            },
                        ]
                    }
                },
            }
        }
        DataType::Dictionary(key_type, _value_type) => layout(key_type),
        DataType::Decimal(_, _) => {
            // Decimals are always some fixed width; The rust implementation
            // always uses 16 bytes / size of i128
            DataTypeLayout::new_fixed_width(size_of::<i128>())
        }
        DataType::Map(_, _) => {
            // same as ListType
            DataTypeLayout::new_fixed_width(size_of::<i32>())
        }
    }
}

/// Layout specification for a data type
#[derive(Debug, PartialEq)]
// Note: Follows structure from C++: https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h#L91
struct DataTypeLayout {
    /// A vector of buffer layout specifications, one for each expected buffer
    pub buffers: Vec<BufferSpec>,
}

impl DataTypeLayout {
    /// Describes a basic numeric array where each element has a fixed width
    pub fn new_fixed_width(byte_width: usize) -> Self {
        Self {
            buffers: vec![BufferSpec::FixedWidth { byte_width }],
        }
    }

    /// Describes arrays which have no data of their own
    /// (e.g. FixedSizeList). Note such arrays may still have a Null
    /// Bitmap
    pub fn new_empty() -> Self {
        Self { buffers: vec![] }
    }

    /// Describes a basic numeric array where each element has a fixed
    /// with offset buffer of `offset_byte_width` bytes, followed by a
    /// variable width data buffer
    pub fn new_binary(offset_byte_width: usize) -> Self {
        Self {
            buffers: vec![
                // offsets
                BufferSpec::FixedWidth {
                    byte_width: offset_byte_width,
                },
                // values
                BufferSpec::VariableWidth,
            ],
        }
    }
}

/// Layout specification for a single data type buffer
#[derive(Debug, PartialEq)]
enum BufferSpec {
    /// each element has a fixed width
    FixedWidth { byte_width: usize },
    /// Variable width, such as string data for utf8 data
    VariableWidth,
    /// Buffer holds a bitmap.
    ///
    /// Note: Unlike the C++ implementation, the null/validity buffer
    /// is handled specially rather than as another of the buffers in
    /// the spec, so this variant is only used for the Boolean type.
    BitMap,
    /// Buffer is always null. Unused currently in Rust implementation,
    /// (used in C++ for Union type)
    AlwaysNull,
}

impl PartialEq for ArrayData {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

/// Builder for `ArrayData` type
#[derive(Debug)]
pub struct ArrayDataBuilder {
    data_type: DataType,
    len: usize,
    null_count: Option<usize>,
    null_bit_buffer: Option<Buffer>,
    offset: usize,
    buffers: Vec<Buffer>,
    child_data: Vec<ArrayData>,
}

impl ArrayDataBuilder {
    #[inline]
    pub const fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            len: 0,
            null_count: None,
            null_bit_buffer: None,
            offset: 0,
            buffers: vec![],
            child_data: vec![],
        }
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub const fn len(mut self, n: usize) -> Self {
        self.len = n;
        self
    }

    pub fn null_count(mut self, null_count: usize) -> Self {
        self.null_count = Some(null_count);
        self
    }

    pub fn null_bit_buffer(mut self, buf: Buffer) -> Self {
        self.null_bit_buffer = Some(buf);
        self
    }

    #[inline]
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    pub fn buffers(mut self, v: Vec<Buffer>) -> Self {
        self.buffers = v;
        self
    }

    pub fn add_buffer(mut self, b: Buffer) -> Self {
        self.buffers.push(b);
        self
    }

    pub fn child_data(mut self, v: Vec<ArrayData>) -> Self {
        self.child_data = v;
        self
    }

    pub fn add_child_data(mut self, r: ArrayData) -> Self {
        self.child_data.push(r);
        self
    }

    /// Creates an array data, without any validation
    ///
    /// # Safety
    ///
    /// The same caveats as [`ArrayData::new_unchecked`]
    /// apply.
    pub unsafe fn build_unchecked(self) -> ArrayData {
        ArrayData::new_unchecked(
            self.data_type,
            self.len,
            self.null_count,
            self.null_bit_buffer,
            self.offset,
            self.buffers,
            self.child_data,
        )
    }

    /// Creates an array data, validating all inputs
    pub fn build(self) -> Result<ArrayData> {
        ArrayData::try_new(
            self.data_type,
            self.len,
            self.null_count,
            self.null_bit_buffer,
            self.offset,
            self.buffers,
            self.child_data,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{
        Array, BooleanBuilder, Int32Array, Int32Builder, Int64Array, StringArray,
        StructBuilder, UInt64Array,
    };
    use crate::buffer::Buffer;
    use crate::datatypes::Field;
    use crate::util::bit_util;

    #[test]
    fn test_builder() {
        // Buffer needs to be at least 25 long
        let v = (0..25).collect::<Vec<i32>>();
        let b1 = Buffer::from_slice_ref(&v);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(20)
            .offset(5)
            .add_buffer(b1)
            .null_bit_buffer(Buffer::from(vec![
                0b01011111, 0b10110101, 0b01100011, 0b00011110,
            ]))
            .build()
            .unwrap();

        assert_eq!(20, arr_data.len());
        assert_eq!(10, arr_data.null_count());
        assert_eq!(5, arr_data.offset());
        assert_eq!(1, arr_data.buffers().len());
        assert_eq!(
            Buffer::from_slice_ref(&v).as_slice(),
            arr_data.buffers()[0].as_slice()
        );
    }

    #[test]
    fn test_builder_with_child_data() {
        let child_arr_data = ArrayData::try_new(
            DataType::Int32,
            5,
            Some(0),
            None,
            0,
            vec![Buffer::from_slice_ref(&[1i32, 2, 3, 4, 5])],
            vec![],
        )
        .unwrap();

        let data_type = DataType::Struct(vec![Field::new("x", DataType::Int32, true)]);

        let arr_data = ArrayData::builder(data_type)
            .len(5)
            .offset(0)
            .add_child_data(child_arr_data.clone())
            .build()
            .unwrap();

        assert_eq!(5, arr_data.len());
        assert_eq!(1, arr_data.child_data().len());
        assert_eq!(child_arr_data, arr_data.child_data()[0]);
    }

    #[test]
    fn test_null_count() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(make_i32_buffer(16))
            .null_bit_buffer(Buffer::from(bit_v))
            .build()
            .unwrap();
        assert_eq!(13, arr_data.null_count());

        // Test with offset
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(12)
            .offset(2)
            .add_buffer(make_i32_buffer(14)) // requires at least 14 bytes of space,
            .null_bit_buffer(Buffer::from(bit_v))
            .build()
            .unwrap();
        assert_eq!(10, arr_data.null_count());
    }

    #[test]
    fn test_null_buffer_ref() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(make_i32_buffer(16))
            .null_bit_buffer(Buffer::from(bit_v))
            .build()
            .unwrap();
        assert!(arr_data.null_buffer().is_some());
        assert_eq!(&bit_v, arr_data.null_buffer().unwrap().as_slice());
    }

    #[test]
    fn test_slice() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(make_i32_buffer(16))
            .null_bit_buffer(Buffer::from(bit_v))
            .build()
            .unwrap();
        let new_data = data.slice(1, 15);
        assert_eq!(data.len() - 1, new_data.len());
        assert_eq!(1, new_data.offset());
        assert_eq!(data.null_count(), new_data.null_count());

        // slice of a slice (removes one null)
        let new_data = new_data.slice(1, 14);
        assert_eq!(data.len() - 2, new_data.len());
        assert_eq!(2, new_data.offset());
        assert_eq!(data.null_count() - 1, new_data.null_count());
    }

    #[test]
    fn test_equality() {
        let int_data = ArrayData::builder(DataType::Int32)
            .len(1)
            .add_buffer(make_i32_buffer(1))
            .build()
            .unwrap();

        let float_data = ArrayData::builder(DataType::Float32)
            .len(1)
            .add_buffer(make_f32_buffer(1))
            .build()
            .unwrap();
        assert_ne!(int_data, float_data);
        assert!(!int_data.ptr_eq(&float_data));
        assert!(int_data.ptr_eq(&int_data));

        let int_data_clone = int_data.clone();
        assert_eq!(int_data, int_data_clone);
        assert!(int_data.ptr_eq(&int_data_clone));
        assert!(int_data_clone.ptr_eq(&int_data));

        let int_data_slice = int_data_clone.slice(1, 0);
        assert!(int_data_slice.ptr_eq(&int_data_slice));
        assert!(!int_data.ptr_eq(&int_data_slice));
        assert!(!int_data_slice.ptr_eq(&int_data));

        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref(&[0_i32, 2_i32, 2_i32, 5_i32]);
        let string_data = ArrayData::try_new(
            DataType::Utf8,
            3,
            Some(1),
            Some(Buffer::from_iter(vec![true, false, true])),
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();

        assert_ne!(float_data, string_data);
        assert!(!float_data.ptr_eq(&string_data));

        assert!(string_data.ptr_eq(&string_data));

        let string_data_cloned = string_data.clone();
        assert!(string_data_cloned.ptr_eq(&string_data));
        assert!(string_data.ptr_eq(&string_data_cloned));

        let string_data_slice = string_data.slice(1, 2);
        assert!(string_data_slice.ptr_eq(&string_data_slice));
        assert!(!string_data_slice.ptr_eq(&string_data))
    }

    #[test]
    fn test_count_nulls() {
        let null_buffer = Some(Buffer::from(vec![0b00010110, 0b10011111]));
        let count = count_nulls(null_buffer.as_ref(), 0, 16);
        assert_eq!(count, 7);

        let count = count_nulls(null_buffer.as_ref(), 4, 8);
        assert_eq!(count, 3);
    }

    #[test]
    #[should_panic(
        expected = "Need at least 80 bytes in buffers[0] in array of type Int64, but got 8"
    )]
    fn test_buffer_too_small() {
        let buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        // should fail as the declared size (10*8 = 80) is larger than the underlying bfufer (8)
        ArrayData::try_new(DataType::Int64, 10, Some(0), None, 0, vec![buffer], vec![])
            .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Need at least 16 bytes in buffers[0] in array of type Int64, but got 8"
    )]
    fn test_buffer_too_small_offset() {
        let buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        // should fail -- size is ok, but also has offset
        ArrayData::try_new(DataType::Int64, 1, Some(0), None, 1, vec![buffer], vec![])
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected 1 buffers in array of type Int64, got 2")]
    fn test_bad_number_of_buffers() {
        let buffer1 = Buffer::from_slice_ref(&[0i32, 2i32]);
        let buffer2 = Buffer::from_slice_ref(&[0i32, 2i32]);
        ArrayData::try_new(
            DataType::Int64,
            1,
            Some(0),
            None,
            0,
            vec![buffer1, buffer2],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "integer overflow computing min buffer size")]
    fn test_fixed_width_overflow() {
        let buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        ArrayData::try_new(
            DataType::Int64,
            usize::MAX,
            Some(0),
            None,
            0,
            vec![buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "null_bit_buffer size too small. got 1 needed 2")]
    fn test_bitmap_too_small() {
        let buffer = make_i32_buffer(9);
        let null_bit_buffer = Buffer::from(vec![0b11111111]);

        ArrayData::try_new(
            DataType::Int32,
            9,
            Some(0),
            Some(null_bit_buffer),
            0,
            vec![buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "null_count 3 for an array exceeds length of 2 elements")]
    fn test_bad_null_count() {
        let buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        ArrayData::try_new(DataType::Int32, 2, Some(3), None, 0, vec![buffer], vec![])
            .unwrap();
    }

    // Test creating a dictionary with a non integer type
    #[test]
    #[should_panic(expected = "Dictionary values must be integer, but was Utf8")]
    fn test_non_int_dictionary() {
        let i32_buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        let data_type =
            DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32));
        let child_data = ArrayData::try_new(
            DataType::Int32,
            1,
            Some(0),
            None,
            0,
            vec![i32_buffer.clone()],
            vec![],
        )
        .unwrap();
        ArrayData::try_new(
            data_type,
            1,
            Some(0),
            None,
            0,
            vec![i32_buffer.clone(), i32_buffer],
            vec![child_data],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected LargeUtf8 but child data had Utf8")]
    fn test_mismatched_dictionary_types() {
        // test w/ dictionary created with a child array data that has type different than declared
        let string_array: StringArray =
            vec![Some("foo"), Some("bar")].into_iter().collect();
        let i32_buffer = Buffer::from_slice_ref(&[0i32, 1i32]);
        // Dict says LargeUtf8 but array is Utf8
        let data_type = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::LargeUtf8),
        );
        let child_data = string_array.data().clone();
        ArrayData::try_new(
            data_type,
            1,
            Some(0),
            None,
            0,
            vec![i32_buffer],
            vec![child_data],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Offsets buffer size (bytes): 8 isn't large enough for Utf8. Length 2 needs 3"
    )]
    fn test_validate_offsets_i32() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref(&[0i32, 2i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Offsets buffer size (bytes): 16 isn't large enough for LargeUtf8. Length 2 needs 3"
    )]
    fn test_validate_offsets_i64() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref(&[0i64, 2i64]);
        ArrayData::try_new(
            DataType::LargeUtf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Error converting offset[0] (-2) to usize for Utf8")]
    fn test_validate_offsets_negative_first_i32() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref(&[-2i32, 1i32, 3i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Error converting offset[2] (-3) to usize for Utf8")]
    fn test_validate_offsets_negative_last_i32() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref(&[0i32, 2i32, -3i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "First offset 4 in Utf8 is smaller than last offset 3")]
    fn test_validate_offsets_range_too_small() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        // start offset is larger than end
        let offsets_buffer = Buffer::from_slice_ref(&[4i32, 2i32, 3i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Last offset 10 of Utf8 is larger than values length 6")]
    fn test_validate_offsets_range_too_large() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        // 10 is off the end of the buffer
        let offsets_buffer = Buffer::from_slice_ref(&[0i32, 2i32, 10i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "First offset 10 of Utf8 is larger than values length 6")]
    fn test_validate_offsets_first_too_large() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        // 10 is off the end of the buffer
        let offsets_buffer = Buffer::from_slice_ref(&[10i32, 2i32, 10i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    fn test_validate_offsets_first_too_large_skipped() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        // 10 is off the end of the buffer, but offset starts at 1 so it is skipped
        let offsets_buffer = Buffer::from_slice_ref(&[10i32, 2i32, 3i32, 4i32]);
        let data = ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            1,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
        let array: StringArray = data.into();
        let expected: StringArray = vec![Some("c"), Some("d")].into_iter().collect();
        assert_eq!(array, expected);
    }

    #[test]
    #[should_panic(expected = "Last offset 8 of Utf8 is larger than values length 6")]
    fn test_validate_offsets_last_too_large() {
        let data_buffer = Buffer::from_slice_ref(&"abcdef".as_bytes());
        // 10 is off the end of the buffer
        let offsets_buffer = Buffer::from_slice_ref(&[5i32, 7i32, 8i32]);
        ArrayData::try_new(
            DataType::Utf8,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Values length 4 is less than the length (2) multiplied by the value size (2) for FixedSizeList"
    )]
    fn test_validate_fixed_size_list() {
        // child has 4 elements,
        let child_array = vec![Some(1), Some(2), Some(3), None]
            .into_iter()
            .collect::<Int32Array>();

        // but claim we have 3 elements for a fixed size of 2
        // 10 is off the end of the buffer
        let field = Field::new("field", DataType::Int32, true);
        ArrayData::try_new(
            DataType::FixedSizeList(Box::new(field), 2),
            3,
            None,
            None,
            0,
            vec![],
            vec![child_array.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Child type mismatch for Struct")]
    fn test_validate_struct_child_type() {
        let field1 = vec![Some(1), Some(2), Some(3), None]
            .into_iter()
            .collect::<Int32Array>();

        // validate the the type of struct fields matches child fields
        ArrayData::try_new(
            DataType::Struct(vec![Field::new("field1", DataType::Int64, true)]),
            3,
            None,
            None,
            0,
            vec![],
            vec![field1.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "child array #0 for field field1 has length smaller than expected for struct array (4 < 6)"
    )]
    fn test_validate_struct_child_length() {
        // field length only has 4 items, but array claims to have 6
        let field1 = vec![Some(1), Some(2), Some(3), None]
            .into_iter()
            .collect::<Int32Array>();

        ArrayData::try_new(
            DataType::Struct(vec![Field::new("field1", DataType::Int32, true)]),
            6,
            None,
            None,
            0,
            vec![],
            vec![field1.data().clone()],
        )
        .unwrap();
    }

    /// Test that the array of type `data_type` that has invalid utf8 data errors
    fn check_utf8_validation<T: ArrowNativeType>(data_type: DataType) {
        // 0x80 is a utf8 continuation sequence and is not a valid utf8 sequence itself
        let data_buffer = Buffer::from_slice_ref(&[b'a', b'a', 0x80, 0x00]);
        let offsets: Vec<T> = [0, 2, 3]
            .iter()
            .map(|&v| T::from_usize(v).unwrap())
            .collect();

        let offsets_buffer = Buffer::from_slice_ref(&offsets);
        ArrayData::try_new(
            data_type,
            2,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid UTF8 sequence at string index 1 (2..3)")]
    fn test_validate_utf8_content() {
        check_utf8_validation::<i32>(DataType::Utf8);
    }

    #[test]
    #[should_panic(expected = "Invalid UTF8 sequence at string index 1 (2..3)")]
    fn test_validate_large_utf8_content() {
        check_utf8_validation::<i64>(DataType::LargeUtf8);
    }

    /// Test that the array of type `data_type` that has invalid indexes (out of bounds)
    fn check_index_out_of_bounds_validation<T: ArrowNativeType>(data_type: DataType) {
        let data_buffer = Buffer::from_slice_ref(&[b'a', b'b', b'c', b'd']);
        // First two offsets are fine, then 5 is out of bounds
        let offsets: Vec<T> = [0, 1, 2, 5, 2]
            .iter()
            .map(|&v| T::from_usize(v).unwrap())
            .collect();

        let offsets_buffer = Buffer::from_slice_ref(&offsets);
        ArrayData::try_new(
            data_type,
            4,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 2 out of bounds: 5 > 4"
    )]
    fn test_validate_utf8_out_of_bounds() {
        check_index_out_of_bounds_validation::<i32>(DataType::Utf8);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 2 out of bounds: 5 > 4"
    )]
    fn test_validate_large_utf8_out_of_bounds() {
        check_index_out_of_bounds_validation::<i64>(DataType::LargeUtf8);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 2 out of bounds: 5 > 4"
    )]
    fn test_validate_binary_out_of_bounds() {
        check_index_out_of_bounds_validation::<i32>(DataType::Binary);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 2 out of bounds: 5 > 4"
    )]
    fn test_validate_large_binary_out_of_bounds() {
        check_index_out_of_bounds_validation::<i64>(DataType::LargeBinary);
    }

    // validate that indexes don't go bacwards check indexes that go backwards
    fn check_index_backwards_validation<T: ArrowNativeType>(data_type: DataType) {
        let data_buffer = Buffer::from_slice_ref(&[b'a', b'b', b'c', b'd']);
        // First three offsets are fine, then 1 goes backwards
        let offsets: Vec<T> = [0, 1, 2, 2, 1]
            .iter()
            .map(|&v| T::from_usize(v).unwrap())
            .collect();

        let offsets_buffer = Buffer::from_slice_ref(&offsets);
        ArrayData::try_new(
            data_type,
            4,
            None,
            None,
            0,
            vec![offsets_buffer, data_buffer],
            vec![],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1"
    )]
    fn test_validate_utf8_index_backwards() {
        check_index_backwards_validation::<i32>(DataType::Utf8);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1"
    )]
    fn test_validate_large_utf8_index_backwards() {
        check_index_backwards_validation::<i64>(DataType::LargeUtf8);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1"
    )]
    fn test_validate_binary_index_backwards() {
        check_index_backwards_validation::<i32>(DataType::Binary);
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: non-monotonic offset at slot 3: 2 > 1"
    )]
    fn test_validate_large_binary_index_backwards() {
        check_index_backwards_validation::<i64>(DataType::LargeBinary);
    }

    #[test]
    #[should_panic(
        expected = "Value at position 1 out of bounds: 3 (should be in [0, 1])"
    )]
    fn test_validate_dictionary_index_too_large() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

        // 3 is not a valid index into the values (only 0 and 1)
        let keys: Int32Array = [Some(1), Some(3)].into_iter().collect();

        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        ArrayData::try_new(
            data_type,
            2,
            None,
            None,
            0,
            vec![keys.data().buffers[0].clone()],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Value at position 1 out of bounds: -1 (should be in [0, 1]"
    )]
    fn test_validate_dictionary_index_negative() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

        // -1 is not a valid index at all!
        let keys: Int32Array = [Some(1), Some(-1)].into_iter().collect();

        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        ArrayData::try_new(
            data_type,
            2,
            None,
            None,
            0,
            vec![keys.data().buffers[0].clone()],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    #[test]
    fn test_validate_dictionary_index_negative_but_not_referenced() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

        // -1 is not a valid index at all, but the array is length 1
        // so the -1 should not be looked at
        let keys: Int32Array = [Some(1), Some(-1)].into_iter().collect();

        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        // Expect this not to panic
        ArrayData::try_new(
            data_type,
            1,
            None,
            None,
            0,
            vec![keys.data().buffers[0].clone()],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Value at position 0 out of bounds: 18446744073709551615 (can not convert to i64)"
    )]
    fn test_validate_dictionary_index_giant_negative() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();

        // -1 is not a valid index at all!
        let keys: UInt64Array = [Some(u64::MAX), Some(1)].into_iter().collect();

        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        ArrayData::try_new(
            data_type,
            2,
            None,
            None,
            0,
            vec![keys.data().buffers[0].clone()],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    /// Test that the list of type `data_type` generates correct offset out of bounds errors
    fn check_list_offsets<T: ArrowNativeType>(data_type: DataType) {
        let values: Int32Array =
            [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();

        // 5 is an invalid offset into a list of only three values
        let offsets: Vec<T> = [0, 2, 5, 4]
            .iter()
            .map(|&v| T::from_usize(v).unwrap())
            .collect();
        let offsets_buffer = Buffer::from_slice_ref(&offsets);

        ArrayData::try_new(
            data_type,
            3,
            None,
            None,
            0,
            vec![offsets_buffer],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 1 out of bounds: 5 > 4"
    )]
    fn test_validate_list_offsets() {
        let field_type = Field::new("f", DataType::Int32, true);
        check_list_offsets::<i32>(DataType::List(Box::new(field_type)));
    }

    #[test]
    #[should_panic(
        expected = "Offset invariant failure: offset for slot 1 out of bounds: 5 > 4"
    )]
    fn test_validate_large_list_offsets() {
        let field_type = Field::new("f", DataType::Int32, true);
        check_list_offsets::<i64>(DataType::LargeList(Box::new(field_type)));
    }

    /// Test that the list of type `data_type` generates correct errors for negative offsets
    #[test]
    #[should_panic(
        expected = "Offset invariant failure: Could not convert end_offset -1 to usize in slot 2"
    )]
    fn test_validate_list_negative_offsets() {
        let values: Int32Array =
            [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();
        let field_type = Field::new("f", values.data_type().clone(), true);
        let data_type = DataType::List(Box::new(field_type));

        // -1 is an invalid offset any way you look at it
        let offsets: Vec<i32> = vec![0, 2, -1, 4];
        let offsets_buffer = Buffer::from_slice_ref(&offsets);

        ArrayData::try_new(
            data_type,
            3,
            None,
            None,
            0,
            vec![offsets_buffer],
            vec![values.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "child #0 invalid: Invalid argument error: Value at position 1 out of bounds: -1 (should be in [0, 1])"
    )]
    /// test that children are validated recursively (aka bugs in child data of struct also are flagged)
    fn test_validate_recursive() {
        // Form invalid dictionary array
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        // -1 is not a valid index
        let keys: Int32Array = [Some(1), Some(-1), Some(1)].into_iter().collect();

        let dict_data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        // purposely create an invalid child data
        let dict_data = unsafe {
            ArrayData::new_unchecked(
                dict_data_type,
                2,
                None,
                None,
                0,
                vec![keys.data().buffers[0].clone()],
                vec![values.data().clone()],
            )
        };

        // Now, try and create a struct with this invalid child data (and expect an error)
        let data_type =
            DataType::Struct(vec![Field::new("d", dict_data.data_type().clone(), true)]);

        ArrayData::try_new(data_type, 1, None, None, 0, vec![], vec![dict_data]).unwrap();
    }

    /// returns a buffer initialized with some constant value for tests
    fn make_i32_buffer(n: usize) -> Buffer {
        Buffer::from_slice_ref(&vec![42i32; n])
    }

    /// returns a buffer initialized with some constant value for tests
    fn make_f32_buffer(n: usize) -> Buffer {
        Buffer::from_slice_ref(&vec![42f32; n])
    }

    #[test]
    #[should_panic(expected = "Expected Int64 but child data had Int32")]
    fn test_validate_union_different_types() {
        let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

        let field2 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

        let type_ids = Buffer::from_slice_ref(&[0i8, 1i8]);

        ArrayData::try_new(
            DataType::Union(
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true), // data is int32
                ],
                UnionMode::Sparse,
            ),
            2,
            None,
            None,
            0,
            vec![type_ids],
            vec![field1.data().clone(), field2.data().clone()],
        )
        .unwrap();
    }

    // sparse with wrong sized children
    #[test]
    #[should_panic(
        expected = "Sparse union child array #1 has length smaller than expected for union array (1 < 2)"
    )]
    fn test_validate_union_sparse_different_child_len() {
        let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

        // field 2 only has 1 item but array should have 2
        let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

        let type_ids = Buffer::from_slice_ref(&[0i8, 1i8]);

        ArrayData::try_new(
            DataType::Union(
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
                UnionMode::Sparse,
            ),
            2,
            None,
            None,
            0,
            vec![type_ids],
            vec![field1.data().clone(), field2.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected 2 buffers in array of type Union")]
    fn test_validate_union_dense_without_offsets() {
        let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

        let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

        let type_ids = Buffer::from_slice_ref(&[0i8, 1i8]);

        ArrayData::try_new(
            DataType::Union(
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
                UnionMode::Dense,
            ),
            2,
            None,
            None,
            0,
            vec![type_ids], // need offsets buffer here too
            vec![field1.data().clone(), field2.data().clone()],
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Need at least 8 bytes in buffers[1] in array of type Union"
    )]
    fn test_validate_union_dense_with_bad_len() {
        let field1 = vec![Some(1), Some(2)].into_iter().collect::<Int32Array>();

        let field2 = vec![Some(1)].into_iter().collect::<Int64Array>();

        let type_ids = Buffer::from_slice_ref(&[0i8, 1i8]);
        let offsets = Buffer::from_slice_ref(&[0i32]); // should have 2 offsets, but only have 1

        ArrayData::try_new(
            DataType::Union(
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Int64, true),
                ],
                UnionMode::Dense,
            ),
            2,
            None,
            None,
            0,
            vec![type_ids, offsets],
            vec![field1.data().clone(), field2.data().clone()],
        )
        .unwrap();
    }

    #[test]
    fn test_try_new_sliced_struct() {
        let mut builder = StructBuilder::new(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Boolean, true),
            ],
            vec![
                Box::new(Int32Builder::new(5)),
                Box::new(BooleanBuilder::new(5)),
            ],
        );

        // struct[0] = { a: 10, b: true }
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_option(Some(10))
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_option(Some(true))
            .unwrap();
        builder.append(true).unwrap();

        // struct[1] = null
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_option(None)
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_option(None)
            .unwrap();
        builder.append(false).unwrap();

        // struct[2] = { a: null, b: false }
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_option(None)
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_option(Some(false))
            .unwrap();
        builder.append(true).unwrap();

        // struct[3] = { a: 21, b: null }
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_option(Some(21))
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_option(None)
            .unwrap();
        builder.append(true).unwrap();

        // struct[4] = { a: 18, b: false }
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_option(Some(18))
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_option(Some(false))
            .unwrap();
        builder.append(true).unwrap();

        let struct_array = builder.finish();
        let struct_array_slice = struct_array.slice(1, 3);
        let struct_array_data = struct_array_slice.data();

        let cloned_data = ArrayData::try_new(
            struct_array_slice.data_type().clone(),
            struct_array_slice.len(),
            None, // force new to compute the number of null bits
            struct_array_data.null_buffer().cloned(),
            struct_array_slice.offset(),
            struct_array_data.buffers().to_vec(),
            struct_array_data.child_data().to_vec(),
        )
        .unwrap();
        let cloned = crate::array::make_array(cloned_data);

        assert_eq!(&struct_array_slice, &cloned);
    }
}
