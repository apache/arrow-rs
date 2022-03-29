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

use std::borrow::Borrow;
use std::convert::{From, TryInto};
use std::fmt;
use std::{any::Any, iter::FromIterator};

use super::BooleanBufferBuilder;
use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData,
    FixedSizeListArray, GenericBinaryIter, GenericListArray, OffsetSizeTrait,
};
use crate::buffer::Buffer;
use crate::datatypes::{
    validate_decimal_precision, DECIMAL_DEFAULT_SCALE, DECIMAL_MAX_PRECISION,
    DECIMAL_MAX_SCALE,
};
use crate::error::{ArrowError, Result};
use crate::util::bit_util;
use crate::{buffer::MutableBuffer, datatypes::DataType};

/// Like OffsetSizeTrait, but specialized for Binary
// This allow us to expose a constant datatype for the GenericBinaryArray
pub trait BinaryOffsetSizeTrait: OffsetSizeTrait {
    const DATA_TYPE: DataType;
}

impl BinaryOffsetSizeTrait for i32 {
    const DATA_TYPE: DataType = DataType::Binary;
}

impl BinaryOffsetSizeTrait for i64 {
    const DATA_TYPE: DataType = DataType::LargeBinary;
}

/// See [`BinaryArray`] and [`LargeBinaryArray`] for storing
/// binary data.
pub struct GenericBinaryArray<OffsetSize: BinaryOffsetSizeTrait> {
    data: ArrayData,
    value_offsets: RawPtrBox<OffsetSize>,
    value_data: RawPtrBox<u8>,
}

impl<OffsetSize: BinaryOffsetSizeTrait> GenericBinaryArray<OffsetSize> {
    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> OffsetSize {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[1].clone()
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[OffsetSize] {
        // Soundness
        //     pointer alignment & location is ensured by RawPtrBox
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.value_offsets.as_ptr().add(self.data.offset()),
                self.len() + 1,
            )
        }
    }

    /// Returns the element at index `i` as bytes slice
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);

        // Soundness
        // pointer alignment & location is ensured by RawPtrBox
        // buffer bounds/offset is ensured by the value_offset invariants

        // Safety of `to_isize().unwrap()`
        // `start` and `end` are &OffsetSize, which is a generic type that implements the
        // OffsetSizeTrait. Currently, only i32 and i64 implement OffsetSizeTrait,
        // both of which should cleanly cast to isize on an architecture that supports
        // 32/64-bit offsets
        std::slice::from_raw_parts(
            self.value_data.as_ptr().offset(start.to_isize().unwrap()),
            (end - start).to_usize().unwrap(),
        )
    }

    /// Returns the element at index `i` as bytes slice
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.data.len(), "BinaryArray out of bounds access");
        //Soundness: length checked above, offset buffer length is 1 larger than logical array length
        let end = unsafe { self.value_offsets().get_unchecked(i + 1) };
        let start = unsafe { self.value_offsets().get_unchecked(i) };

        // Soundness
        // pointer alignment & location is ensured by RawPtrBox
        // buffer bounds/offset is ensured by the value_offset invariants

        // Safety of `to_isize().unwrap()`
        // `start` and `end` are &OffsetSize, which is a generic type that implements the
        // OffsetSizeTrait. Currently, only i32 and i64 implement OffsetSizeTrait,
        // both of which should cleanly cast to isize on an architecture that supports
        // 32/64-bit offsets
        unsafe {
            std::slice::from_raw_parts(
                self.value_data.as_ptr().offset(start.to_isize().unwrap()),
                (*end - *start).to_usize().unwrap(),
            )
        }
    }

    /// Creates a [GenericBinaryArray] from a vector of byte slices
    ///
    /// See also [`Self::from_iter_values`]
    pub fn from_vec(v: Vec<&[u8]>) -> Self {
        Self::from_iter_values(v)
    }

    /// Creates a [GenericBinaryArray] from a vector of Optional (null) byte slices
    pub fn from_opt_vec(v: Vec<Option<&[u8]>>) -> Self {
        v.into_iter().collect()
    }

    fn from_list(v: GenericListArray<OffsetSize>) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "BinaryArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "BinaryArray can only be created from List<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(v.data_ref().buffers()[0].clone())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }

    /// Creates a `GenericBinaryArray` based on an iterator of values without nulls
    pub fn from_iter_values<Ptr, I>(iter: I) -> Self
    where
        Ptr: AsRef<[u8]>,
        I: IntoIterator<Item = Ptr>,
    {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets =
            MutableBuffer::new((data_len + 1) * std::mem::size_of::<OffsetSize>());
        let mut values = MutableBuffer::new(0);

        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        for s in iter {
            let s = s.as_ref();
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s);
        }

        // iterator size hint may not be correct so compute the actual number of offsets
        assert!(!offsets.is_empty()); // wrote at least one
        let actual_len = (offsets.len() / std::mem::size_of::<OffsetSize>()) - 1;

        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(actual_len)
            .add_buffer(offsets.into())
            .add_buffer(values.into());
        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    pub fn take_iter<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&[u8]>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value(index)))
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    /// # Safety
    ///
    /// caller must ensure that the offsets in the iterator are less than the array len()
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&[u8]>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }
}

impl<'a, T: BinaryOffsetSizeTrait> GenericBinaryArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> GenericBinaryIter<'a, T> {
        GenericBinaryIter::<'a, T>::new(self)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> fmt::Debug for GenericBinaryArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let prefix = if OffsetSize::is_large() { "Large" } else { "" };

        write!(f, "{}BinaryArray\n[\n", prefix)?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> Array for GenericBinaryArray<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> From<ArrayData>
    for GenericBinaryArray<OffsetSize>
{
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.data_type(),
            &<OffsetSize as BinaryOffsetSizeTrait>::DATA_TYPE,
            "[Large]BinaryArray expects Datatype::[Large]Binary"
        );
        assert_eq!(
            data.buffers().len(),
            2,
            "BinaryArray data should contain 2 buffers only (offsets and values)"
        );
        let offsets = data.buffers()[0].as_ptr();
        let values = data.buffers()[1].as_ptr();
        Self {
            data,
            value_offsets: unsafe { RawPtrBox::new(offsets) },
            value_data: unsafe { RawPtrBox::new(values) },
        }
    }
}

impl<Ptr, OffsetSize: BinaryOffsetSizeTrait> FromIterator<Option<Ptr>>
    for GenericBinaryArray<OffsetSize>
where
    Ptr: AsRef<[u8]>,
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets = Vec::with_capacity(data_len + 1);
        let mut values = Vec::new();
        let mut null_buf = MutableBuffer::new_null(data_len);
        let mut length_so_far: OffsetSize = OffsetSize::zero();
        offsets.push(length_so_far);

        {
            let null_slice = null_buf.as_slice_mut();

            for (i, s) in iter.enumerate() {
                if let Some(s) = s {
                    let s = s.as_ref();
                    bit_util::set_bit(null_slice, i);
                    length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                    values.extend_from_slice(s);
                }
                // always add an element in offsets
                offsets.push(length_so_far);
            }
        }

        // calculate actual data_len, which may be different from the iterator's upper bound
        let data_len = offsets.len() - 1;
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(data_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .null_bit_buffer(null_buf.into());
        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }
}

/// An array where each element contains 0 or more bytes.
/// The byte length of each element is represented by an i32.
///
/// # Examples
///
/// Create a BinaryArray from a vector of byte slices.
///
/// ```
/// use arrow::array::{Array, BinaryArray};
/// let values: Vec<&[u8]> =
///     vec![b"one", b"two", b"", b"three"];
/// let array = BinaryArray::from_vec(values);
/// assert_eq!(4, array.len());
/// assert_eq!(b"one", array.value(0));
/// assert_eq!(b"two", array.value(1));
/// assert_eq!(b"", array.value(2));
/// assert_eq!(b"three", array.value(3));
/// ```
///
/// Create a BinaryArray from a vector of Optional (null) byte slices.
///
/// ```
/// use arrow::array::{Array, BinaryArray};
/// let values: Vec<Option<&[u8]>> =
///     vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
/// let array = BinaryArray::from_opt_vec(values);
/// assert_eq!(5, array.len());
/// assert_eq!(b"one", array.value(0));
/// assert_eq!(b"two", array.value(1));
/// assert_eq!(b"", array.value(3));
/// assert_eq!(b"three", array.value(4));
/// assert!(!array.is_null(0));
/// assert!(!array.is_null(1));
/// assert!(array.is_null(2));
/// assert!(!array.is_null(3));
/// assert!(!array.is_null(4));
/// ```
///
pub type BinaryArray = GenericBinaryArray<i32>;

/// An array where each element contains 0 or more bytes.
/// The byte length of each element is represented by an i64.
///
/// # Examples
///
/// Create a LargeBinaryArray from a vector of byte slices.
///
/// ```
/// use arrow::array::{Array, LargeBinaryArray};
/// let values: Vec<&[u8]> =
///     vec![b"one", b"two", b"", b"three"];
/// let array = LargeBinaryArray::from_vec(values);
/// assert_eq!(4, array.len());
/// assert_eq!(b"one", array.value(0));
/// assert_eq!(b"two", array.value(1));
/// assert_eq!(b"", array.value(2));
/// assert_eq!(b"three", array.value(3));
/// ```
///
/// Create a LargeBinaryArray from a vector of Optional (null) byte slices.
///
/// ```
/// use arrow::array::{Array, LargeBinaryArray};
/// let values: Vec<Option<&[u8]>> =
///     vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
/// let array = LargeBinaryArray::from_opt_vec(values);
/// assert_eq!(5, array.len());
/// assert_eq!(b"one", array.value(0));
/// assert_eq!(b"two", array.value(1));
/// assert_eq!(b"", array.value(3));
/// assert_eq!(b"three", array.value(4));
/// assert!(!array.is_null(0));
/// assert!(!array.is_null(1));
/// assert!(array.is_null(2));
/// assert!(!array.is_null(3));
/// assert!(!array.is_null(4));
/// ```
///
pub type LargeBinaryArray = GenericBinaryArray<i64>;

impl<'a, T: BinaryOffsetSizeTrait> IntoIterator for &'a GenericBinaryArray<T> {
    type Item = Option<&'a [u8]>;
    type IntoIter = GenericBinaryIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GenericBinaryIter::<'a, T>::new(self)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> From<Vec<Option<&[u8]>>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        Self::from_opt_vec(v)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> From<Vec<&[u8]>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(v: Vec<&[u8]>) -> Self {
        Self::from_iter_values(v)
    }
}

impl<T: BinaryOffsetSizeTrait> From<GenericListArray<T>> for GenericBinaryArray<T> {
    fn from(v: GenericListArray<T>) -> Self {
        Self::from_list(v)
    }
}

/// A type of `FixedSizeListArray` whose elements are binaries.
///
/// # Examples
///
/// Create an array from an iterable argument of byte slices.
///
/// ```
///    use arrow::array::{Array, FixedSizeBinaryArray};
///    let input_arg = vec![ vec![1, 2], vec![3, 4], vec![5, 6] ];
///    let arr = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();
///
///    assert_eq!(3, arr.len());
///
/// ```
/// Create an array from an iterable argument of sparse byte slices.
/// Sparsity means that the input argument can contain `None` items.
/// ```
///    use arrow::array::{Array, FixedSizeBinaryArray};
///    let input_arg = vec![ None, Some(vec![7, 8]), Some(vec![9, 10]), None, Some(vec![13, 14]) ];
///    let arr = FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
///    assert_eq!(5, arr.len())
///
/// ```
///
pub struct FixedSizeBinaryArray {
    data: ArrayData,
    value_data: RawPtrBox<u8>,
    length: i32,
}

impl FixedSizeBinaryArray {
    /// Returns the element at index `i` as a byte slice.
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(
            i < self.data.len(),
            "FixedSizeBinaryArray out of bounds access"
        );
        let offset = i.checked_add(self.data.offset()).unwrap();
        unsafe {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.value_data.as_ptr().offset(pos as isize),
                (self.value_offset_at(offset + 1) - pos) as usize,
            )
        }
    }

    /// Returns the element at index `i` as a byte slice.
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let offset = i.checked_add(self.data.offset()).unwrap();
        let pos = self.value_offset_at(offset);
        std::slice::from_raw_parts(
            self.value_data.as_ptr().offset(pos as isize),
            (self.value_offset_at(offset + 1) - pos) as usize,
        )
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        self.length
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    /// Create an array from an iterable argument of sparse byte slices.
    /// Sparsity means that items returned by the iterator are optional, i.e input argument can
    /// contain `None` items.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow::array::FixedSizeBinaryArray;
    /// let input_arg = vec![
    ///     None,
    ///     Some(vec![7, 8]),
    ///     Some(vec![9, 10]),
    ///     None,
    ///     Some(vec![13, 14]),
    ///     None,
    /// ];
    /// let array = FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if argument has length zero, or sizes of nested slices don't match.
    pub fn try_from_sparse_iter<T, U>(mut iter: T) -> Result<Self>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let mut byte = 0;
        let mut null_buf = MutableBuffer::from_len_zeroed(0);
        let mut buffer = MutableBuffer::from_len_zeroed(0);
        let mut prepend = 0;
        iter.try_for_each(|item| -> Result<()> {
            // extend null bitmask by one byte per each 8 items
            if byte == 0 {
                null_buf.push(0u8);
                byte = 8;
            }
            byte -= 1;

            if let Some(slice) = item {
                let slice = slice.as_ref();
                if let Some(size) = size {
                    if size != slice.len() {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Nested array size mismatch: one is {}, and the other is {}",
                            size,
                            slice.len()
                        )));
                    }
                } else {
                    size = Some(slice.len());
                    buffer.extend_zeros(slice.len() * prepend);
                }
                bit_util::set_bit(null_buf.as_slice_mut(), len);
                buffer.extend_from_slice(slice);
            } else if let Some(size) = size {
                buffer.extend_zeros(size);
            } else {
                prepend += 1;
            }

            len += 1;

            Ok(())
        })?;

        if len == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Input iterable argument has no data".to_owned(),
            ));
        }

        let size = size.unwrap_or(0);
        let array_data = unsafe {
            ArrayData::new_unchecked(
                DataType::FixedSizeBinary(size as i32),
                len,
                None,
                Some(null_buf.into()),
                0,
                vec![buffer.into()],
                vec![],
            )
        };
        Ok(FixedSizeBinaryArray::from(array_data))
    }

    /// Create an array from an iterable argument of byte slices.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow::array::FixedSizeBinaryArray;
    /// let input_arg = vec![
    ///     vec![1, 2],
    ///     vec![3, 4],
    ///     vec![5, 6],
    /// ];
    /// let array = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if argument has length zero, or sizes of nested slices don't match.
    pub fn try_from_iter<T, U>(mut iter: T) -> Result<Self>
    where
        T: Iterator<Item = U>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let mut buffer = MutableBuffer::from_len_zeroed(0);
        iter.try_for_each(|item| -> Result<()> {
            let slice = item.as_ref();
            if let Some(size) = size {
                if size != slice.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Nested array size mismatch: one is {}, and the other is {}",
                        size,
                        slice.len()
                    )));
                }
            } else {
                size = Some(slice.len());
            }
            buffer.extend_from_slice(slice);

            len += 1;

            Ok(())
        })?;

        if len == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Input iterable argument has no data".to_owned(),
            ));
        }

        let size = size.unwrap_or(0);
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(size as i32))
            .len(len)
            .add_buffer(buffer.into());
        let array_data = unsafe { array_data.build_unchecked() };
        Ok(FixedSizeBinaryArray::from(array_data))
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.length * i as i32
    }
}

impl From<ArrayData> for FixedSizeBinaryArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "FixedSizeBinaryArray data should contain 1 buffer only (values)"
        );
        let value_data = data.buffers()[0].as_ptr();
        let length = match data.data_type() {
            DataType::FixedSizeBinary(len) => *len,
            _ => panic!("Expected data type to be FixedSizeBinary"),
        };
        Self {
            data,
            value_data: unsafe { RawPtrBox::new(value_data) },
            length,
        }
    }
}

/// Creates a `FixedSizeBinaryArray` from `FixedSizeList<u8>` array
impl From<FixedSizeListArray> for FixedSizeBinaryArray {
    fn from(v: FixedSizeListArray) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "FixedSizeBinaryArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "FixedSizeBinaryArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(DataType::FixedSizeBinary(v.value_length()))
            .len(v.len())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }
}

impl fmt::Debug for FixedSizeBinaryArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FixedSizeBinaryArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl Array for FixedSizeBinaryArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }
}

/// `DecimalArray` stores fixed width decimal numbers,
/// with a fixed precision and scale.
///
/// # Examples
///
/// ```
///    use arrow::array::{Array, DecimalArray};
///    use arrow::datatypes::DataType;
///
///    // Create a DecimalArray with the default precision and scale
///    let decimal_array: DecimalArray = vec![
///       Some(8_887_000_000),
///       None,
///       Some(-8_887_000_000),
///     ]
///     .into_iter().collect();
///
///    // set precision and scale so values are interpreted
///    // as `8887.000000`, `Null`, and `-8887.000000`
///    let decimal_array = decimal_array
///     .with_precision_and_scale(23, 6)
///     .unwrap();
///
///    assert_eq!(&DataType::Decimal(23, 6), decimal_array.data_type());
///    assert_eq!(8_887_000_000, decimal_array.value(0));
///    assert_eq!("8887.000000", decimal_array.value_as_string(0));
///    assert_eq!(3, decimal_array.len());
///    assert_eq!(1, decimal_array.null_count());
///    assert_eq!(32, decimal_array.value_offset(2));
///    assert_eq!(16, decimal_array.value_length());
///    assert_eq!(23, decimal_array.precision());
///    assert_eq!(6, decimal_array.scale());
/// ```
///
pub struct DecimalArray {
    data: ArrayData,
    value_data: RawPtrBox<u8>,
    precision: usize,
    scale: usize,
    length: i32,
}

impl DecimalArray {
    /// Returns the element at index `i` as i128.
    pub fn value(&self, i: usize) -> i128 {
        assert!(i < self.data.len(), "DecimalArray out of bounds access");
        let offset = i.checked_add(self.data.offset()).unwrap();
        let raw_val = unsafe {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.value_data.as_ptr().offset(pos as isize),
                (self.value_offset_at(offset + 1) - pos) as usize,
            )
        };
        let as_array = raw_val.try_into();
        match as_array {
            Ok(v) if raw_val.len() == 16 => i128::from_le_bytes(v),
            _ => panic!("DecimalArray elements are not 128bit integers."),
        }
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        self.length
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.length * i as i32
    }

    #[inline]
    pub fn value_as_string(&self, row: usize) -> String {
        let value = self.value(row);
        let value_str = value.to_string();

        if self.scale == 0 {
            value_str
        } else {
            let (sign, rest) = value_str.split_at(if value >= 0 { 0 } else { 1 });

            if rest.len() > self.scale {
                // Decimal separator is in the middle of the string
                let (whole, decimal) = value_str.split_at(value_str.len() - self.scale);
                format!("{}.{}", whole, decimal)
            } else {
                // String has to be padded
                format!("{}0.{:0>width$}", sign, rest, width = self.scale)
            }
        }
    }

    pub fn from_fixed_size_list_array(
        v: FixedSizeListArray,
        precision: usize,
        scale: usize,
    ) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "DecimalArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "DecimalArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(DataType::Decimal(precision, scale))
            .len(v.len())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let array_data = unsafe { builder.build_unchecked() };
        Self::from(array_data)
    }

    /// Creates a [DecimalArray] with default precision and scale,
    /// based on an iterator of `i128` values without nulls
    pub fn from_iter_values<I: IntoIterator<Item = i128>>(iter: I) -> Self {
        let val_buf: Buffer = iter.into_iter().collect();
        let data = unsafe {
            ArrayData::new_unchecked(
                Self::default_type(),
                val_buf.len() / std::mem::size_of::<i128>(),
                None,
                None,
                0,
                vec![val_buf],
                vec![],
            )
        };
        DecimalArray::from(data)
    }

    /// Return the precision (total digits) that can be stored by this array
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Return the scale (digits after the decimal) that can be stored by this array
    pub fn scale(&self) -> usize {
        self.scale
    }

    /// Returns a DecimalArray with the same data as self, with the
    /// specified precision.
    ///
    /// Returns an Error if:
    /// 1. `precision` is larger than [`DECIMAL_MAX_PRECISION`]
    /// 2. `scale` is larger than [`DECIMAL_MAX_SCALE`];
    /// 3. `scale` is > `precision`
    pub fn with_precision_and_scale(
        mut self,
        precision: usize,
        scale: usize,
    ) -> Result<Self> {
        if precision > DECIMAL_MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "precision {} is greater than max {}",
                precision, DECIMAL_MAX_PRECISION
            )));
        }
        if scale > DECIMAL_MAX_SCALE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than max {}",
                scale, DECIMAL_MAX_SCALE
            )));
        }
        if scale > precision {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than precision {}",
                scale, precision
            )));
        }

        // Ensure that all values are within the requested
        // precision. For performance, only check if the precision is
        // decreased
        if precision < self.precision {
            for v in self.iter().flatten() {
                validate_decimal_precision(v, precision)?;
            }
        }

        assert_eq!(
            self.data.data_type(),
            &DataType::Decimal(self.precision, self.scale)
        );

        // safety: self.data is valid DataType::Decimal as checked above
        let new_data_type = DataType::Decimal(precision, scale);
        self.precision = precision;
        self.scale = scale;
        self.data = self.data.with_data_type(new_data_type);
        Ok(self)
    }

    /// The default precision and scale used when not specified.
    pub fn default_type() -> DataType {
        // Keep maximum precision
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE)
    }
}

impl From<ArrayData> for DecimalArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DecimalArray data should contain 1 buffer only (values)"
        );
        let values = data.buffers()[0].as_ptr();
        let (precision, scale) = match data.data_type() {
            DataType::Decimal(precision, scale) => (*precision, *scale),
            _ => panic!("Expected data type to be Decimal"),
        };
        let length = 16;
        Self {
            data,
            value_data: unsafe { RawPtrBox::new(values) },
            precision,
            scale,
            length,
        }
    }
}

impl From<DecimalArray> for ArrayData {
    fn from(array: DecimalArray) -> Self {
        array.data
    }
}

/// an iterator that returns Some(i128) or None, that can be used on a
/// DecimalArray
#[derive(Debug)]
pub struct DecimalIter<'a> {
    array: &'a DecimalArray,
    current: usize,
    current_end: usize,
}

impl<'a> DecimalIter<'a> {
    pub fn new(array: &'a DecimalArray) -> Self {
        Self {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> std::iter::Iterator for DecimalIter<'a> {
    type Item = Option<i128>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            // TODO: Improve performance by avoiding bounds check here
            // (by using adding a `value_unchecked, for example)
            if self.array.is_null(old) {
                Some(None)
            } else {
                Some(Some(self.array.value(old)))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.array.len() - self.current;
        (remain, Some(remain))
    }
}

/// iterator has have known size.
impl<'a> std::iter::ExactSizeIterator for DecimalIter<'a> {}

impl<'a> IntoIterator for &'a DecimalArray {
    type Item = Option<i128>;
    type IntoIter = DecimalIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DecimalIter::<'a>::new(self)
    }
}

impl<'a> DecimalArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> DecimalIter<'a> {
        DecimalIter::new(self)
    }
}

impl<Ptr: Borrow<Option<i128>>> FromIterator<Ptr> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let size_hint = upper.unwrap_or(lower);

        let mut null_buf = BooleanBufferBuilder::new(size_hint);

        let buffer: Buffer = iter
            .map(|item| {
                if let Some(a) = item.borrow() {
                    null_buf.append(true);
                    *a
                } else {
                    null_buf.append(false);
                    // arbitrary value for NULL
                    0
                }
            })
            .collect();

        let data = unsafe {
            ArrayData::new_unchecked(
                Self::default_type(),
                null_buf.len(),
                None,
                Some(null_buf.into()),
                0,
                vec![buffer],
                vec![],
            )
        };
        DecimalArray::from(data)
    }
}

impl fmt::Debug for DecimalArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DecimalArray<{}, {}>\n[\n", self.precision, self.scale)?;
        print_long_array(self, f, |array, index, f| {
            let formatted_decimal = array.value_as_string(index);

            write!(f, "{}", formatted_decimal)
        })?;
        write!(f, "]")
    }
}

impl Array for DecimalArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{DecimalBuilder, LargeListArray, ListArray},
        datatypes::Field,
    };

    use super::*;

    #[test]
    fn test_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], unsafe {
            binary_array.value_unchecked(0)
        });
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([] as [u8; 0], unsafe { binary_array.value_unchecked(1) });
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(2)
        });
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }
    }

    #[test]
    fn test_binary_array_with_offsets() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::Binary)
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!(5, binary_array.value_offsets()[0]);
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offsets()[1]);
        assert_eq!(7, binary_array.value_length(1));
    }

    #[test]
    fn test_large_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i64; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::LargeBinary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], unsafe {
            binary_array.value_unchecked(0)
        });
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([] as [u8; 0], unsafe { binary_array.value_unchecked(1) });
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(2)
        });
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }
    }

    #[test]
    fn test_large_binary_array_with_offsets() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i64; 4] = [0, 5, 5, 12];

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::LargeBinary)
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(1)
        });
        assert_eq!(5, binary_array.value_offsets()[0]);
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offsets()[1]);
        assert_eq!(7, binary_array.value_length(1));
    }

    #[test]
    fn test_binary_array_from_list_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data1 = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array1 = BinaryArray::from(array_data1);

        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::UInt8, false)));
        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(values_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(array_data2);
        let binary_array2 = BinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        assert_eq!(binary_array1.value_offsets(), binary_array2.value_offsets());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value(i), unsafe {
                binary_array2.value_unchecked(i)
            });
            assert_eq!(binary_array1.value_length(i), binary_array2.value_length(i));
        }
    }

    #[test]
    fn test_large_binary_array_from_list_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let offsets: [i64; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data1 = ArrayData::builder(DataType::LargeBinary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array1 = LargeBinaryArray::from(array_data1);

        let data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::UInt8, false)));
        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(values_data)
            .build()
            .unwrap();
        let list_array = LargeListArray::from(array_data2);
        let binary_array2 = LargeBinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        assert_eq!(binary_array1.value_offsets(), binary_array2.value_offsets());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value(i), unsafe {
                binary_array2.value_unchecked(i)
            });
            assert_eq!(binary_array1.value_length(i), binary_array2.value_length(i));
        }
    }

    fn test_generic_binary_array_from_opt_vec<T: BinaryOffsetSizeTrait>() {
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let array = GenericBinaryArray::<T>::from_opt_vec(values);
        assert_eq!(array.len(), 5);
        assert_eq!(array.value(0), b"one");
        assert_eq!(array.value(1), b"two");
        assert_eq!(array.value(3), b"");
        assert_eq!(array.value(4), b"three");
        assert!(!array.is_null(0));
        assert!(!array.is_null(1));
        assert!(array.is_null(2));
        assert!(!array.is_null(3));
        assert!(!array.is_null(4));
    }

    #[test]
    fn test_large_binary_array_from_opt_vec() {
        test_generic_binary_array_from_opt_vec::<i64>()
    }

    #[test]
    fn test_binary_array_from_opt_vec() {
        test_generic_binary_array_from_opt_vec::<i32>()
    }

    #[test]
    fn test_binary_array_from_unbound_iter() {
        // iterator that doesn't declare (upper) size bound
        let value_iter = (0..)
            .scan(0usize, |pos, i| {
                if *pos < 10 {
                    *pos += 1;
                    Some(Some(format!("value {}", i)))
                } else {
                    // actually returns up to 10 values
                    None
                }
            })
            // limited using take()
            .take(100);

        let (_, upper_size_bound) = value_iter.size_hint();
        // the upper bound, defined by take above, is 100
        assert_eq!(upper_size_bound, Some(100));
        let binary_array: BinaryArray = value_iter.collect();
        // but the actual number of items in the array should be 10
        assert_eq!(binary_array.len(), 10);
    }

    #[test]
    #[should_panic(
        expected = "assertion failed: `(left == right)`\n  left: `UInt32`,\n \
                    right: `UInt8`: BinaryArray can only be created from List<u8> arrays, \
                    mismatched data types."
    )]
    fn test_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::UInt32, false)));
        let array_data = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(values_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(array_data);
        drop(BinaryArray::from(list_array));
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let fixed_size_binary_array = FixedSizeBinaryArray::from(array_data);
        assert_eq!(3, fixed_size_binary_array.len());
        assert_eq!(0, fixed_size_binary_array.null_count());
        assert_eq!(
            [b'h', b'e', b'l', b'l', b'o'],
            fixed_size_binary_array.value(0)
        );
        assert_eq!(
            [b't', b'h', b'e', b'r', b'e'],
            fixed_size_binary_array.value(1)
        );
        assert_eq!(
            [b'a', b'r', b'r', b'o', b'w'],
            fixed_size_binary_array.value(2)
        );
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(10, fixed_size_binary_array.value_offset(2));
        for i in 0..3 {
            assert!(fixed_size_binary_array.is_valid(i));
            assert!(!fixed_size_binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let fixed_size_binary_array = FixedSizeBinaryArray::from(array_data);
        assert_eq!(
            [b't', b'h', b'e', b'r', b'e'],
            fixed_size_binary_array.value(0)
        );
        assert_eq!(
            [b'a', b'r', b'r', b'o', b'w'],
            fixed_size_binary_array.value(1)
        );
        assert_eq!(2, fixed_size_binary_array.len());
        assert_eq!(5, fixed_size_binary_array.value_offset(0));
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(10, fixed_size_binary_array.value_offset(1));
    }

    #[test]
    #[should_panic(
        expected = "FixedSizeBinaryArray can only be created from FixedSizeList<u8> arrays"
    )]
    fn test_fixed_size_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();

        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::Binary, false)),
                4,
            ))
            .len(3)
            .add_child_data(values_data)
            .build_unchecked()
        };
        let list_array = FixedSizeListArray::from(array_data);
        drop(FixedSizeBinaryArray::from(list_array));
    }

    #[test]
    #[should_panic(expected = "BinaryArray out of bounds access")]
    fn test_binary_array_get_value_index_out_of_bound() {
        let values: [u8; 12] =
            [104, 101, 108, 108, 111, 112, 97, 114, 113, 117, 101, 116];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array = BinaryArray::from(array_data);
        binary_array.value(4);
    }

    #[test]
    fn test_binary_array_fmt_debug() {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let arr = FixedSizeBinaryArray::from(array_data);
        assert_eq!(
            "FixedSizeBinaryArray<5>\n[\n  [104, 101, 108, 108, 111],\n  [116, 104, 101, 114, 101],\n  [97, 114, 114, 111, 119],\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_decimal_array() {
        // let val_8887: [u8; 16] = [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // let val_neg_8887: [u8; 16] = [64, 36, 75, 238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255];
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal(23, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let decimal_array = DecimalArray::from(array_data);
        assert_eq!(8_887_000_000, decimal_array.value(0));
        assert_eq!(-8_887_000_000, decimal_array.value(1));
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    fn test_decimal_append_error_value() {
        let mut decimal_builder = DecimalBuilder::new(10, 5, 3);
        let mut result = decimal_builder.append_value(123456);
        let mut error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 123456 is too large to store in a Decimal of precision 5. Max is 99999",
            error.to_string()
        );
        decimal_builder.append_value(12345).unwrap();
        let arr = decimal_builder.finish();
        assert_eq!("12.345", arr.value_as_string(0));

        decimal_builder = DecimalBuilder::new(10, 2, 1);
        result = decimal_builder.append_value(100);
        error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 100 is too large to store in a Decimal of precision 2. Max is 99",
            error.to_string()
        );
        decimal_builder.append_value(99).unwrap();
        result = decimal_builder.append_value(-100);
        error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: -100 is too small to store in a Decimal of precision 2. Min is -99",
            error.to_string()
        );
        decimal_builder.append_value(-99).unwrap();
        let arr = decimal_builder.finish();
        assert_eq!("9.9", arr.value_as_string(0));
        assert_eq!("-9.9", arr.value_as_string(1));
    }
    #[test]
    fn test_decimal_from_iter_values() {
        let array = DecimalArray::from_iter_values(vec![-100, 0, 101].into_iter());
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal(38, 10));
        assert_eq!(-100, array.value(0));
        assert!(!array.is_null(0));
        assert_eq!(0, array.value(1));
        assert!(!array.is_null(1));
        assert_eq!(101, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_from_iter() {
        let array: DecimalArray = vec![Some(-100), None, Some(101)].into_iter().collect();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal(38, 10));
        assert_eq!(-100, array.value(0));
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(101, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_iter() {
        let data = vec![Some(-100), None, Some(101)];
        let array: DecimalArray = data.clone().into_iter().collect();

        let collected: Vec<_> = array.iter().collect();
        assert_eq!(data, collected);
    }

    #[test]
    fn test_decimal_into_iter() {
        let data = vec![Some(-100), None, Some(101)];
        let array: DecimalArray = data.clone().into_iter().collect();

        let collected: Vec<_> = array.into_iter().collect();
        assert_eq!(data, collected);
    }

    #[test]
    fn test_decimal_iter_sized() {
        let data = vec![Some(-100), None, Some(101)];
        let array: DecimalArray = data.into_iter().collect();
        let mut iter = array.into_iter();

        // is exact sized
        assert_eq!(array.len(), 3);

        // size_hint is reported correctly
        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        iter.next().unwrap();
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_decimal_array_value_as_string() {
        let arr = [123450, -123450, 100, -100, 10, -10, 0]
            .into_iter()
            .map(Some)
            .collect::<DecimalArray>()
            .with_precision_and_scale(6, 3)
            .unwrap();

        assert_eq!("123.450", arr.value_as_string(0));
        assert_eq!("-123.450", arr.value_as_string(1));
        assert_eq!("0.100", arr.value_as_string(2));
        assert_eq!("-0.100", arr.value_as_string(3));
        assert_eq!("0.010", arr.value_as_string(4));
        assert_eq!("-0.010", arr.value_as_string(5));
        assert_eq!("0.000", arr.value_as_string(6));
    }

    #[test]
    fn test_decimal_array_with_precision_and_scale() {
        let arr = DecimalArray::from_iter_values([12345, 456, 7890, -123223423432432])
            .with_precision_and_scale(20, 2)
            .unwrap();

        assert_eq!(arr.data_type(), &DataType::Decimal(20, 2));
        assert_eq!(arr.precision(), 20);
        assert_eq!(arr.scale(), 2);

        let actual: Vec<_> = (0..arr.len()).map(|i| arr.value_as_string(i)).collect();
        let expected = vec!["123.45", "4.56", "78.90", "-1232234234324.32"];

        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(
        expected = "-123223423432432 is too small to store in a Decimal of precision 5. Min is -99999"
    )]
    fn test_decimal_array_with_precision_and_scale_out_of_range() {
        DecimalArray::from_iter_values([12345, 456, 7890, -123223423432432])
            // precision is too small to hold value
            .with_precision_and_scale(5, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "precision 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision() {
        DecimalArray::from_iter_values([12345, 456])
            .with_precision_and_scale(40, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_scale() {
        DecimalArray::from_iter_values([12345, 456])
            .with_precision_and_scale(20, 40)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 10 is greater than precision 4")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision_and_scale() {
        DecimalArray::from_iter_values([12345, 456])
            .with_precision_and_scale(4, 10)
            .unwrap();
    }

    #[test]
    fn test_decimal_array_fmt_debug() {
        let arr = [Some(8887000000), Some(-8887000000), None]
            .iter()
            .collect::<DecimalArray>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        assert_eq!(
            "DecimalArray<23, 6>\n[\n  8887.000000,\n  -8887.000000,\n  null,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_fixed_size_binary_array_from_iter() {
        let input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let arr = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();

        assert_eq!(2, arr.value_length());
        assert_eq!(3, arr.len())
    }

    #[test]
    fn test_all_none_fixed_size_binary_array_from_sparse_iter() {
        let none_option: Option<[u8; 32]> = None;
        let input_arg = vec![none_option, none_option, none_option];
        let arr =
            FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
        assert_eq!(0, arr.value_length());
        assert_eq!(3, arr.len())
    }

    #[test]
    fn test_fixed_size_binary_array_from_sparse_iter() {
        let input_arg = vec![
            None,
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
        ];
        let arr =
            FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
        assert_eq!(2, arr.value_length());
        assert_eq!(5, arr.len())
    }

    #[test]
    fn test_binary_array_all_null() {
        let data = vec![None];
        let array = BinaryArray::from(data);
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_large_binary_array_all_null() {
        let data = vec![None];
        let array = LargeBinaryArray::from(data);
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn fixed_size_binary_array_all_null() {
        let data = vec![None] as Vec<Option<String>>;
        let array = FixedSizeBinaryArray::try_from_sparse_iter(data.into_iter()).unwrap();
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }
}
