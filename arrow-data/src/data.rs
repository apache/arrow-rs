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

use crate::{bit_iterator::BitSliceIterator, bitmap::Bitmap};
use arrow_buffer::bit_chunk_iterator::BitChunks;
use arrow_buffer::{bit_util, ArrowNativeType, Buffer, MutableBuffer};
use arrow_schema::{ArrowError, DataType, IntervalUnit, UnionMode};
use half::f16;
use std::convert::TryInto;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use crate::equal;

#[inline]
pub(crate) fn contains_nulls(
    null_bit_buffer: Option<&Buffer>,
    offset: usize,
    len: usize,
) -> bool {
    match null_bit_buffer {
        Some(buffer) => match BitSliceIterator::new(buffer, offset, len).next() {
            Some((start, end)) => start != 0 || end != len,
            None => len != 0, // No non-null values
        },
        None => false, // No null buffer
    }
}

#[inline]
pub(crate) fn count_nulls(
    null_bit_buffer: Option<&Buffer>,
    offset: usize,
    len: usize,
) -> usize {
    if let Some(buf) = null_bit_buffer {
        len - buf.count_set_bits_offset(offset, len)
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
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => [
            MutableBuffer::new(capacity * mem::size_of::<u8>()),
            empty_buffer,
        ],
        DataType::Union(_, _, mode) => {
            let type_ids = MutableBuffer::new(capacity * mem::size_of::<i8>());
            match mode {
                UnionMode::Sparse => [type_ids, empty_buffer],
                UnionMode::Dense => {
                    let offsets = MutableBuffer::new(capacity * mem::size_of::<i32>());
                    [type_ids, offsets]
                }
            }
        }
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
        DataType::Null | DataType::Struct(_) | DataType::FixedSizeList(_, _) => vec![],
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => vec![buffer1.into(), buffer2.into()],
        DataType::Union(_, _, mode) => {
            match mode {
                // Based on Union's DataTypeLayout
                UnionMode::Sparse => vec![buffer1.into()],
                UnionMode::Dense => vec![buffer1.into(), buffer2.into()],
            }
        }
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
    /// null_bit_buffer is calculated.
    ///
    /// If the number of nulls is 0 then the null_bit_buffer
    /// is set to `None`.
    ///
    /// # Safety
    ///
    /// The input values *must* form a valid Arrow array for
    /// `data_type`, or undefined behavior can result.
    ///
    /// Note: This is a low level API and most users of the arrow
    /// crate should create arrays using the methods in the `array`
    /// module.
    #[allow(clippy::let_and_return)]
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
        let null_bitmap = null_bit_buffer.filter(|_| null_count > 0).map(Bitmap::from);
        let new_self = Self {
            data_type,
            len,
            null_count,
            offset,
            buffers,
            child_data,
            null_bitmap,
        };

        // Provide a force_validate mode
        #[cfg(feature = "force_validate")]
        new_self.validate_data().unwrap();
        new_self
    }

    /// Create a new ArrayData, validating that the provided buffers form a valid
    /// Arrow array of the specified data type.
    ///
    /// If the number of nulls in `null_bit_buffer` is 0 then the null_bit_buffer
    /// is set to `None`.
    ///
    /// Internally this calls through to [`Self::validate_data`]
    ///
    /// Note: This is a low level API and most users of the arrow crate should create
    /// arrays using the builders found in [arrow_array](https://docs.rs/arrow-array)
    pub fn try_new(
        data_type: DataType,
        len: usize,
        null_bit_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrayData>,
    ) -> Result<Self, ArrowError> {
        // we must check the length of `null_bit_buffer` first
        // because we use this buffer to calculate `null_count`
        // in `Self::new_unchecked`.
        if let Some(null_bit_buffer) = null_bit_buffer.as_ref() {
            let needed_len = bit_util::ceil(len + offset, 8);
            if null_bit_buffer.len() < needed_len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "null_bit_buffer size too small. got {} needed {}",
                    null_bit_buffer.len(),
                    needed_len
                )));
            }
        }
        // Safety justification: `validate_full` is called below
        let new_self = unsafe {
            Self::new_unchecked(
                data_type,
                len,
                None,
                null_bit_buffer,
                offset,
                buffers,
                child_data,
            )
        };

        // As the data is not trusted, do a full validation of its contents
        // We don't need to validate children as we can assume that the
        // [`ArrayData`] in `child_data` have already been validated through
        // a call to `ArrayData::try_new` or created using unsafe
        new_self.validate_data()?;
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
    pub const fn null_bitmap(&self) -> Option<&Bitmap> {
        self.null_bitmap.as_ref()
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
                null_bitmap: self.null_bitmap().cloned(),
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
    pub fn buffer<T: ArrowNativeType>(&self, buffer: usize) -> &[T] {
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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => vec![],
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
            DataType::Union(fields, _, _) => fields
                .iter()
                .map(|field| Self::new_empty(field.data_type()))
                .collect(),
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
    /// See [ArrayData::validate_data] to validate fully the offset content
    /// and the validity of utf8 data
    pub fn validate(&self) -> Result<(), ArrowError> {
        // Need at least this mich space in each buffer
        let len_plus_offset = self.len + self.offset;

        // Check that the data layout conforms to the spec
        let layout = layout(&self.data_type);

        if !layout.can_contain_null_mask && self.null_bitmap.is_some() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Arrays of type {:?} cannot contain a null bitmask",
                self.data_type,
            )));
        }

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
                self.validate_offsets::<i32>(self.buffers[1].len())?;
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                self.validate_offsets::<i64>(self.buffers[1].len())?;
            }
            DataType::Dictionary(key_type, _value_type) => {
                // At the moment, constructing a DictionaryArray will also check this
                if !DataType::is_dictionary_key_type(key_type) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionary key type must be integer, but was {}",
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
    /// entries.
    ///
    /// For an empty array, the `buffer` can also be empty.
    fn typed_offsets<T: ArrowNativeType + num::Num>(&self) -> Result<&[T], ArrowError> {
        // An empty list-like array can have 0 offsets
        if self.len == 0 && self.buffers[0].is_empty() {
            return Ok(&[]);
        }

        self.typed_buffer(0, self.len + 1)
    }

    /// Returns a reference to the data in `buffers[idx]` as a typed slice after validating
    fn typed_buffer<T: ArrowNativeType + num::Num>(
        &self,
        idx: usize,
        len: usize,
    ) -> Result<&[T], ArrowError> {
        let buffer = &self.buffers[idx];

        let required_len = (len + self.offset) * mem::size_of::<T>();

        if buffer.len() < required_len {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Buffer {} of {} isn't large enough. Expected {} bytes got {}",
                idx,
                self.data_type,
                required_len,
                buffer.len()
            )));
        }

        Ok(&buffer.typed_data::<T>()[self.offset..self.offset + len])
    }

    /// Does a cheap sanity check that the `self.len` values in `buffer` are valid
    /// offsets (of type T) into some other buffer of `values_length` bytes long
    fn validate_offsets<T: ArrowNativeType + num::Num + std::fmt::Display>(
        &self,
        values_length: usize,
    ) -> Result<(), ArrowError> {
        // Justification: buffer size was validated above
        let offsets = self.typed_offsets::<T>()?;
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
    fn validate_child_data(&self) -> Result<(), ArrowError> {
        match &self.data_type {
            DataType::List(field) | DataType::Map(field, _) => {
                let values_data = self.get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i32>(values_data.len)?;
                Ok(())
            }
            DataType::LargeList(field) => {
                let values_data = self.get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i64>(values_data.len)?;
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
            DataType::Union(fields, _, mode) => {
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
    ) -> Result<&ArrayData, ArrowError> {
        self.validate_num_child_data(1)?;
        self.get_valid_child_data(0, expected_type)
    }

    /// Returns `Err` if self.child_data does not have exactly `expected_len` elements
    fn validate_num_child_data(&self, expected_len: usize) -> Result<(), ArrowError> {
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
    ) -> Result<&ArrayData, ArrowError> {
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

    /// Validate that the data contained within this [`ArrayData`] is valid
    ///
    /// 1. Null count is correct
    /// 2. All offsets are valid
    /// 3. All String data is valid UTF-8
    /// 4. All dictionary offsets are valid
    ///
    /// Internally this calls:
    ///
    /// * [`Self::validate`]
    /// * [`Self::validate_nulls`]
    /// * [`Self::validate_values`]
    ///
    /// Note: this does not recurse into children, for a recursive variant
    /// see [`Self::validate_full`]
    pub fn validate_data(&self) -> Result<(), ArrowError> {
        self.validate()?;

        self.validate_nulls()?;
        self.validate_values()?;
        Ok(())
    }

    /// Performs a full recursive validation of this [`ArrayData`] and all its children
    ///
    /// This is equivalent to calling [`Self::validate_data`] on this [`ArrayData`]
    /// and all its children recursively
    pub fn validate_full(&self) -> Result<(), ArrowError> {
        self.validate_data()?;
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

    /// Validates the values stored within this [`ArrayData`] are valid
    /// without recursing into child [`ArrayData`]
    ///
    /// Does not (yet) check
    /// 1. Union type_ids are valid see [#85](https://github.com/apache/arrow-rs/issues/85)
    /// Validates the the null count is correct and that any
    /// nullability requirements of its children are correct
    pub fn validate_nulls(&self) -> Result<(), ArrowError> {
        let nulls = self.null_buffer();

        let actual_null_count = count_nulls(nulls, self.offset, self.len);
        if actual_null_count != self.null_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "null_count value ({}) doesn't match actual number of nulls in array ({})",
                self.null_count, actual_null_count
            )));
        }

        // In general non-nullable children should not contain nulls, however, for certain
        // types, such as StructArray and FixedSizeList, nulls in the parent take up
        // space in the child. As such we permit nulls in the children in the corresponding
        // positions for such types
        match &self.data_type {
            DataType::List(f) | DataType::LargeList(f) | DataType::Map(f, _) => {
                if !f.is_nullable() {
                    self.validate_non_nullable(None, 0, &self.child_data[0])?
                }
            }
            DataType::FixedSizeList(field, len) => {
                let child = &self.child_data[0];
                if !field.is_nullable() {
                    match nulls {
                        Some(nulls) => {
                            let element_len = *len as usize;
                            let mut buffer =
                                MutableBuffer::new_null(element_len * self.len);

                            // Expand each bit within `null_mask` into `element_len`
                            // bits, constructing the implicit mask of the child elements
                            for i in 0..self.len {
                                if !bit_util::get_bit(nulls.as_ref(), self.offset + i) {
                                    continue;
                                }
                                for j in 0..element_len {
                                    bit_util::set_bit(
                                        buffer.as_mut(),
                                        i * element_len + j,
                                    )
                                }
                            }
                            let mask = buffer.into();
                            self.validate_non_nullable(Some(&mask), 0, child)?;
                        }
                        None => self.validate_non_nullable(None, 0, child)?,
                    }
                }
            }
            DataType::Struct(fields) => {
                for (field, child) in fields.iter().zip(&self.child_data) {
                    if !field.is_nullable() {
                        self.validate_non_nullable(nulls, self.offset, child)?
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Verifies that `child` contains no nulls not present in `mask`
    fn validate_non_nullable(
        &self,
        mask: Option<&Buffer>,
        offset: usize,
        data: &ArrayData,
    ) -> Result<(), ArrowError> {
        let mask = match mask {
            Some(mask) => mask.as_ref(),
            None => return match data.null_count {
                0 => Ok(()),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "non-nullable child of type {} contains nulls not present in parent {}",
                    data.data_type(),
                    self.data_type
                ))),
            },
        };

        match data.null_buffer() {
            Some(nulls) => {
                let mask = BitChunks::new(mask, offset, data.len);
                let nulls = BitChunks::new(nulls.as_ref(), data.offset, data.len);
                mask
                    .iter()
                    .zip(nulls.iter())
                    .chain(std::iter::once((
                        mask.remainder_bits(),
                        nulls.remainder_bits(),
                    ))).try_for_each(|(m, c)| {
                    if (m & !c) != 0 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "non-nullable child of type {} contains nulls not present in parent",
                            data.data_type()
                        )))
                    }
                    Ok(())
                })
            }
            None => Ok(()),
        }
    }

    /// Validates the values stored within this [`ArrayData`] are valid
    /// without recursing into child [`ArrayData`]
    ///
    /// Does not (yet) check
    /// 1. Union type_ids are valid see [#85](https://github.com/apache/arrow-rs/issues/85)
    pub fn validate_values(&self) -> Result<(), ArrowError> {
        match &self.data_type {
            DataType::Utf8 => self.validate_utf8::<i32>(),
            DataType::LargeUtf8 => self.validate_utf8::<i64>(),
            DataType::Binary => self.validate_offsets_full::<i32>(self.buffers[1].len()),
            DataType::LargeBinary => {
                self.validate_offsets_full::<i64>(self.buffers[1].len())
            }
            DataType::List(_) | DataType::Map(_, _) => {
                let child = &self.child_data[0];
                self.validate_offsets_full::<i32>(child.len)
            }
            DataType::LargeList(_) => {
                let child = &self.child_data[0];
                self.validate_offsets_full::<i64>(child.len)
            }
            DataType::Union(_, _, _) => {
                // Validate Union Array as part of implementing new Union semantics
                // See comments in `ArrayData::validate()`
                // https://github.com/apache/arrow-rs/issues/85
                //
                // TODO file follow on ticket for full union validation
                Ok(())
            }
            DataType::Dictionary(key_type, _value_type) => {
                let dictionary_length: i64 = self.child_data[0].len.try_into().unwrap();
                let max_value = dictionary_length - 1;
                match key_type.as_ref() {
                    DataType::UInt8 => self.check_bounds::<u8>(max_value),
                    DataType::UInt16 => self.check_bounds::<u16>(max_value),
                    DataType::UInt32 => self.check_bounds::<u32>(max_value),
                    DataType::UInt64 => self.check_bounds::<u64>(max_value),
                    DataType::Int8 => self.check_bounds::<i8>(max_value),
                    DataType::Int16 => self.check_bounds::<i16>(max_value),
                    DataType::Int32 => self.check_bounds::<i32>(max_value),
                    DataType::Int64 => self.check_bounds::<i64>(max_value),
                    _ => unreachable!(),
                }
            }
            _ => {
                // No extra validation check required for other types
                Ok(())
            }
        }
    }

    /// Calls the `validate(item_index, range)` function for each of
    /// the ranges specified in the arrow offsets buffer of type
    /// `T`. Also validates that each offset is smaller than
    /// `offset_limit`
    ///
    /// For an empty array, the offsets buffer can either be empty
    /// or contain a single `0`.
    ///
    /// For example, the offsets buffer contained `[1, 2, 4]`, this
    /// function would call `validate([1,2])`, and `validate([2,4])`
    fn validate_each_offset<T, V>(
        &self,
        offset_limit: usize,
        validate: V,
    ) -> Result<(), ArrowError>
    where
        T: ArrowNativeType + TryInto<usize> + num::Num + std::fmt::Display,
        V: Fn(usize, Range<usize>) -> Result<(), ArrowError>,
    {
        self.typed_offsets::<T>()?
            .iter()
            .enumerate()
            .map(|(i, x)| {
                // check if the offset can be converted to usize
                let r = x.to_usize().ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: Could not convert offset {} to usize at position {}",
                        x, i))}
                    );
                // check if the offset exceeds the limit
                match r {
                    Ok(n) if n <= offset_limit => Ok((i, n)),
                    Ok(_) => Err(ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: offset at position {} out of bounds: {} > {}",
                        i, x, offset_limit))
                    ),
                    Err(e) => Err(e),
                }
            })
            .scan(0_usize, |start, end| {
                // check offsets are monotonically increasing
                match end {
                    Ok((i, end)) if *start <= end => {
                        let range = Some(Ok((i, *start..end)));
                        *start = end;
                        range
                    }
                    Ok((i, end)) => Some(Err(ArrowError::InvalidArgumentError(format!(
                        "Offset invariant failure: non-monotonic offset at slot {}: {} > {}",
                        i - 1, start, end))
                    )),
                    Err(err) => Some(Err(err)),
                }
            })
            .skip(1) // the first element is meaningless
            .try_for_each(|res: Result<(usize, Range<usize>), ArrowError>| {
                let (item_index, range) = res?;
                validate(item_index-1, range)
            })
    }

    /// Ensures that all strings formed by the offsets in `buffers[0]`
    /// into `buffers[1]` are valid utf8 sequences
    fn validate_utf8<T>(&self) -> Result<(), ArrowError>
    where
        T: ArrowNativeType + TryInto<usize> + num::Num + std::fmt::Display,
    {
        let values_buffer = &self.buffers[1].as_slice();
        if let Ok(values_str) = std::str::from_utf8(values_buffer) {
            // Validate Offsets are correct
            self.validate_each_offset::<T, _>(
                values_buffer.len(),
                |string_index, range| {
                    if !values_str.is_char_boundary(range.start)
                        || !values_str.is_char_boundary(range.end)
                    {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "incomplete utf-8 byte sequence from index {}",
                            string_index
                        )));
                    }
                    Ok(())
                },
            )
        } else {
            // find specific offset that failed utf8 validation
            self.validate_each_offset::<T, _>(
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
    }

    /// Ensures that all offsets in `buffers[0]` into `buffers[1]` are
    /// between `0` and `offset_limit`
    fn validate_offsets_full<T>(&self, offset_limit: usize) -> Result<(), ArrowError>
    where
        T: ArrowNativeType + TryInto<usize> + num::Num + std::fmt::Display,
    {
        self.validate_each_offset::<T, _>(offset_limit, |_string_index, _range| {
            // No validation applied to each value, but the iteration
            // itself applies bounds checking to each range
            Ok(())
        })
    }

    /// Validates that each value in self.buffers (typed as T)
    /// is within the range [0, max_value], inclusive
    fn check_bounds<T>(&self, max_value: i64) -> Result<(), ArrowError>
    where
        T: ArrowNativeType + TryInto<i64> + num::Num + std::fmt::Display,
    {
        let required_len = self.len + self.offset;
        let buffer = &self.buffers[0];

        // This should have been checked as part of `validate()` prior
        // to calling `validate_full()` but double check to be sure
        assert!(buffer.len() / mem::size_of::<T>() >= required_len);

        // Justification: buffer size was validated above
        let indexes: &[T] =
            &buffer.typed_data::<T>()[self.offset..self.offset + self.len];

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

    /// Converts this [`ArrayData`] into an [`ArrayDataBuilder`]
    pub fn into_builder(self) -> ArrayDataBuilder {
        self.into()
    }
}

/// Return the expected [`DataTypeLayout`] Arrays of this data
/// type are expected to have
pub fn layout(data_type: &DataType) -> DataTypeLayout {
    // based on C/C++ implementation in
    // https://github.com/apache/arrow/blob/661c7d749150905a63dd3b52e0a04dac39030d95/cpp/src/arrow/type.h (and .cc)
    use std::mem::size_of;
    match data_type {
        DataType::Null => DataTypeLayout {
            buffers: vec![],
            can_contain_null_mask: false,
        },
        DataType::Boolean => DataTypeLayout {
            buffers: vec![BufferSpec::BitMap],
            can_contain_null_mask: true,
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
        DataType::Union(_, _, mode) => {
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
                can_contain_null_mask: false,
            }
        }
        DataType::Dictionary(key_type, _value_type) => layout(key_type),
        DataType::Decimal128(_, _) => {
            // Decimals are always some fixed width; The rust implementation
            // always uses 16 bytes / size of i128
            DataTypeLayout::new_fixed_width(size_of::<i128>())
        }
        DataType::Decimal256(_, _) => {
            // Decimals are always some fixed width.
            DataTypeLayout::new_fixed_width(32)
        }
        DataType::Map(_, _) => {
            // same as ListType
            DataTypeLayout::new_fixed_width(size_of::<i32>())
        }
    }
}

/// Layout specification for a data type
#[derive(Debug, PartialEq, Eq)]
// Note: Follows structure from C++: https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h#L91
pub struct DataTypeLayout {
    /// A vector of buffer layout specifications, one for each expected buffer
    pub buffers: Vec<BufferSpec>,

    /// Can contain a null bitmask
    pub can_contain_null_mask: bool,
}

impl DataTypeLayout {
    /// Describes a basic numeric array where each element has a fixed width
    pub fn new_fixed_width(byte_width: usize) -> Self {
        Self {
            buffers: vec![BufferSpec::FixedWidth { byte_width }],
            can_contain_null_mask: true,
        }
    }

    /// Describes arrays which have no data of their own
    /// (e.g. FixedSizeList). Note such arrays may still have a Null
    /// Bitmap
    pub fn new_empty() -> Self {
        Self {
            buffers: vec![],
            can_contain_null_mask: true,
        }
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
            can_contain_null_mask: true,
        }
    }
}

/// Layout specification for a single data type buffer
#[derive(Debug, PartialEq, Eq)]
pub enum BufferSpec {
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
    #[allow(dead_code)]
    AlwaysNull,
}

impl PartialEq for ArrayData {
    fn eq(&self, other: &Self) -> bool {
        equal::equal(self, other)
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

    pub fn data_type(self, data_type: DataType) -> Self {
        Self { data_type, ..self }
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

    pub fn null_bit_buffer(mut self, buf: Option<Buffer>) -> Self {
        self.null_bit_buffer = buf;
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
    pub fn build(self) -> Result<ArrayData, ArrowError> {
        ArrayData::try_new(
            self.data_type,
            self.len,
            self.null_bit_buffer,
            self.offset,
            self.buffers,
            self.child_data,
        )
    }
}

impl From<ArrayData> for ArrayDataBuilder {
    fn from(d: ArrayData) -> Self {
        // TODO: Store Bitmap on ArrayData (#1799)
        let null_bit_buffer = d.null_buffer().cloned();
        Self {
            null_bit_buffer,
            data_type: d.data_type,
            len: d.len,
            null_count: Some(d.null_count),
            offset: d.offset,
            buffers: d.buffers,
            child_data: d.child_data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    // See arrow/tests/array_data_validation.rs for test of array validation

    /// returns a buffer initialized with some constant value for tests
    fn make_i32_buffer(n: usize) -> Buffer {
        Buffer::from_slice_ref(&vec![42i32; n])
    }

    /// returns a buffer initialized with some constant value for tests
    fn make_f32_buffer(n: usize) -> Buffer {
        Buffer::from_slice_ref(&vec![42f32; n])
    }

    #[test]
    fn test_builder() {
        // Buffer needs to be at least 25 long
        let v = (0..25).collect::<Vec<i32>>();
        let b1 = Buffer::from_slice_ref(&v);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(20)
            .offset(5)
            .add_buffer(b1)
            .null_bit_buffer(Some(Buffer::from(vec![
                0b01011111, 0b10110101, 0b01100011, 0b00011110,
            ])))
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
            None,
            0,
            vec![Buffer::from_slice_ref([1i32, 2, 3, 4, 5])],
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
            .null_bit_buffer(Some(Buffer::from(bit_v)))
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
            .null_bit_buffer(Some(Buffer::from(bit_v)))
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
            .null_bit_buffer(Some(Buffer::from(bit_v)))
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
            .null_bit_buffer(Some(Buffer::from(bit_v)))
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

        let data_buffer = Buffer::from_slice_ref("abcdef".as_bytes());
        let offsets_buffer = Buffer::from_slice_ref([0_i32, 2_i32, 2_i32, 5_i32]);
        let string_data = ArrayData::try_new(
            DataType::Utf8,
            3,
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
    fn test_contains_nulls() {
        let buffer: Buffer =
            MutableBuffer::from_iter([false, false, false, true, true, false]).into();

        assert!(contains_nulls(Some(&buffer), 0, 6));
        assert!(contains_nulls(Some(&buffer), 0, 3));
        assert!(!contains_nulls(Some(&buffer), 3, 2));
        assert!(!contains_nulls(Some(&buffer), 0, 0));
    }

    #[test]
    fn test_into_buffers() {
        let data_types = vec![
            DataType::Union(vec![], vec![], UnionMode::Dense),
            DataType::Union(vec![], vec![], UnionMode::Sparse),
        ];

        for data_type in data_types {
            let buffers = new_buffers(&data_type, 0);
            let [buffer1, buffer2] = buffers;
            let buffers = into_buffers(&data_type, buffer1, buffer2);

            let layout = layout(&data_type);
            assert_eq!(buffers.len(), layout.buffers.len());
        }
    }
}
