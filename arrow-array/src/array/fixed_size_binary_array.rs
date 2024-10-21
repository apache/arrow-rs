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

use crate::array::print_long_array;
use crate::iterator::FixedSizeBinaryIter;
use crate::{Array, ArrayAccessor, ArrayRef, FixedSizeListArray, Scalar};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{bit_util, ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::sync::Arc;

/// An array of [fixed size binary arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout)
///
/// # Examples
///
/// Create an array from an iterable argument of byte slices.
///
/// ```
///    use arrow_array::{Array, FixedSizeBinaryArray};
///    let input_arg = vec![ vec![1, 2], vec![3, 4], vec![5, 6] ];
///    let arr = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();
///
///    assert_eq!(3, arr.len());
///
/// ```
/// Create an array from an iterable argument of sparse byte slices.
/// Sparsity means that the input argument can contain `None` items.
/// ```
///    use arrow_array::{Array, FixedSizeBinaryArray};
///    let input_arg = vec![ None, Some(vec![7, 8]), Some(vec![9, 10]), None, Some(vec![13, 14]) ];
///    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 2).unwrap();
///    assert_eq!(5, arr.len())
///
/// ```
///
#[derive(Clone)]
pub struct FixedSizeBinaryArray {
    data_type: DataType, // Must be DataType::FixedSizeBinary(value_length)
    value_data: Buffer,
    nulls: Option<NullBuffer>,
    len: usize,
    value_length: i32,
}

impl FixedSizeBinaryArray {
    /// Create a new [`FixedSizeBinaryArray`] with `size` element size, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(size: i32, values: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self::try_new(size, values, nulls).unwrap()
    }

    /// Create a new [`Scalar`] from `value`
    pub fn new_scalar(value: impl AsRef<[u8]>) -> Scalar<Self> {
        let v = value.as_ref();
        Scalar::new(Self::new(v.len() as _, Buffer::from(v), None))
    }

    /// Create a new [`FixedSizeBinaryArray`] from the provided parts, returning an error on failure
    ///
    /// # Errors
    ///
    /// * `size < 0`
    /// * `values.len() / size != nulls.len()`
    pub fn try_new(
        size: i32,
        values: Buffer,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let data_type = DataType::FixedSizeBinary(size);
        let s = size.to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Size cannot be negative, got {}", size))
        })?;

        let len = values.len() / s;
        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for FixedSizeBinaryArray, expected {} got {}",
                    len,
                    n.len(),
                )));
            }
        }

        Ok(Self {
            data_type,
            value_data: values,
            value_length: size,
            nulls,
            len,
        })
    }

    /// Create a new [`FixedSizeBinaryArray`] of length `len` where all values are null
    ///
    /// # Panics
    ///
    /// Panics if
    ///
    /// * `size < 0`
    /// * `size * len` would overflow `usize`
    pub fn new_null(size: i32, len: usize) -> Self {
        let capacity = size.to_usize().unwrap().checked_mul(len).unwrap();
        Self {
            data_type: DataType::FixedSizeBinary(size),
            value_data: MutableBuffer::new(capacity).into(),
            nulls: Some(NullBuffer::new_null(len)),
            value_length: size,
            len,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (i32, Buffer, Option<NullBuffer>) {
        (self.value_length, self.value_data, self.nulls)
    }

    /// Returns the element at index `i` as a byte slice.
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a FixedSizeBinaryArray of length {}",
            i,
            self.len()
        );
        let offset = i + self.offset();
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
        let offset = i + self.offset();
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
        self.value_offset_at(self.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        self.value_length
    }

    /// Returns the values of this array.
    ///
    /// Unlike [`Self::value_data`] this returns the [`Buffer`]
    /// allowing for zero-copy cloning.
    #[inline]
    pub fn values(&self) -> &Buffer {
        &self.value_data
    }

    /// Returns the raw value data.
    pub fn value_data(&self) -> &[u8] {
        self.value_data.as_slice()
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced FixedSizeBinaryArray cannot exceed the existing length"
        );

        let size = self.value_length as usize;

        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, len)),
            value_length: self.value_length,
            value_data: self.value_data.slice_with_length(offset * size, len * size),
            len,
        }
    }

    /// Create an array from an iterable argument of sparse byte slices.
    /// Sparsity means that items returned by the iterator are optional, i.e input argument can
    /// contain `None` items.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_array::FixedSizeBinaryArray;
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
    #[deprecated(
        note = "This function will fail if the iterator produces only None values; prefer `try_from_sparse_iter_with_size`"
    )]
    pub fn try_from_sparse_iter<T, U>(mut iter: T) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let mut byte = 0;

        let iter_size_hint = iter.size_hint().0;
        let mut null_buf = MutableBuffer::new(bit_util::ceil(iter_size_hint, 8));
        let mut buffer = MutableBuffer::new(0);

        let mut prepend = 0;
        iter.try_for_each(|item| -> Result<(), ArrowError> {
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
                    let len = slice.len();
                    size = Some(len);
                    // Now that we know how large each element is we can reserve
                    // sufficient capacity in the underlying mutable buffer for
                    // the data.
                    buffer.reserve(iter_size_hint * len);
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

        let null_buf = BooleanBuffer::new(null_buf.into(), 0, len);
        let nulls = Some(NullBuffer::new(null_buf)).filter(|n| n.null_count() > 0);

        let size = size.unwrap_or(0) as i32;
        Ok(Self {
            data_type: DataType::FixedSizeBinary(size),
            value_data: buffer.into(),
            nulls,
            value_length: size,
            len,
        })
    }

    /// Create an array from an iterable argument of sparse byte slices.
    /// Sparsity means that items returned by the iterator are optional, i.e input argument can
    /// contain `None` items. In cases where the iterator returns only `None` values, this
    /// also takes a size parameter to ensure that the a valid FixedSizeBinaryArray is still
    /// created.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_array::FixedSizeBinaryArray;
    /// let input_arg = vec![
    ///     None,
    ///     Some(vec![7, 8]),
    ///     Some(vec![9, 10]),
    ///     None,
    ///     Some(vec![13, 14]),
    ///     None,
    /// ];
    /// let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 2).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if argument has length zero, or sizes of nested slices don't match.
    pub fn try_from_sparse_iter_with_size<T, U>(mut iter: T, size: i32) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut byte = 0;

        let iter_size_hint = iter.size_hint().0;
        let mut null_buf = MutableBuffer::new(bit_util::ceil(iter_size_hint, 8));
        let mut buffer = MutableBuffer::new(iter_size_hint * (size as usize));

        iter.try_for_each(|item| -> Result<(), ArrowError> {
            // extend null bitmask by one byte per each 8 items
            if byte == 0 {
                null_buf.push(0u8);
                byte = 8;
            }
            byte -= 1;

            if let Some(slice) = item {
                let slice = slice.as_ref();
                if size as usize != slice.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Nested array size mismatch: one is {}, and the other is {}",
                        size,
                        slice.len()
                    )));
                }

                bit_util::set_bit(null_buf.as_slice_mut(), len);
                buffer.extend_from_slice(slice);
            } else {
                buffer.extend_zeros(size as usize);
            }

            len += 1;

            Ok(())
        })?;

        let null_buf = BooleanBuffer::new(null_buf.into(), 0, len);
        let nulls = Some(NullBuffer::new(null_buf)).filter(|n| n.null_count() > 0);

        Ok(Self {
            data_type: DataType::FixedSizeBinary(size),
            value_data: buffer.into(),
            nulls,
            len,
            value_length: size,
        })
    }

    /// Create an array from an iterable argument of byte slices.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_array::FixedSizeBinaryArray;
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
    pub fn try_from_iter<T, U>(mut iter: T) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = U>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let iter_size_hint = iter.size_hint().0;
        let mut buffer = MutableBuffer::new(0);

        iter.try_for_each(|item| -> Result<(), ArrowError> {
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
                let len = slice.len();
                size = Some(len);
                buffer.reserve(iter_size_hint * len);
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

        let size = size.unwrap_or(0).try_into().unwrap();
        Ok(Self {
            data_type: DataType::FixedSizeBinary(size),
            value_data: buffer.into(),
            nulls: None,
            value_length: size,
            len,
        })
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.value_length * i as i32
    }

    /// constructs a new iterator
    pub fn iter(&self) -> FixedSizeBinaryIter<'_> {
        FixedSizeBinaryIter::new(self)
    }
}

impl From<ArrayData> for FixedSizeBinaryArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "FixedSizeBinaryArray data should contain 1 buffer only (values)"
        );
        let value_length = match data.data_type() {
            DataType::FixedSizeBinary(len) => *len,
            _ => panic!("Expected data type to be FixedSizeBinary"),
        };

        let size = value_length as usize;
        let value_data =
            data.buffers()[0].slice_with_length(data.offset() * size, data.len() * size);

        Self {
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            len: data.len(),
            value_data,
            value_length,
        }
    }
}

impl From<FixedSizeBinaryArray> for ArrayData {
    fn from(array: FixedSizeBinaryArray) -> Self {
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(array.len)
            .buffers(vec![array.value_data])
            .nulls(array.nulls);

        unsafe { builder.build_unchecked() }
    }
}

/// Creates a `FixedSizeBinaryArray` from `FixedSizeList<u8>` array
impl From<FixedSizeListArray> for FixedSizeBinaryArray {
    fn from(v: FixedSizeListArray) -> Self {
        let value_len = v.value_length();
        let v = v.into_data();
        assert_eq!(
            v.child_data().len(),
            1,
            "FixedSizeBinaryArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        let child_data = &v.child_data()[0];

        assert_eq!(
            child_data.child_data().len(),
            0,
            "FixedSizeBinaryArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            child_data.data_type(),
            &DataType::UInt8,
            "FixedSizeBinaryArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );
        assert_eq!(
            child_data.null_count(),
            0,
            "The child array cannot contain null values."
        );

        let builder = ArrayData::builder(DataType::FixedSizeBinary(value_len))
            .len(v.len())
            .offset(v.offset())
            .add_buffer(child_data.buffers()[0].slice(child_data.offset()))
            .nulls(v.nulls().cloned());

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }
}

impl From<Vec<Option<&[u8]>>> for FixedSizeBinaryArray {
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        #[allow(deprecated)]
        Self::try_from_sparse_iter(v.into_iter()).unwrap()
    }
}

impl From<Vec<&[u8]>> for FixedSizeBinaryArray {
    fn from(v: Vec<&[u8]>) -> Self {
        Self::try_from_iter(v.into_iter()).unwrap()
    }
}

impl<const N: usize> From<Vec<&[u8; N]>> for FixedSizeBinaryArray {
    fn from(v: Vec<&[u8; N]>) -> Self {
        Self::try_from_iter(v.into_iter()).unwrap()
    }
}

impl std::fmt::Debug for FixedSizeBinaryArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FixedSizeBinaryArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl Array for FixedSizeBinaryArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut sum = self.value_data.capacity();
        if let Some(n) = &self.nulls {
            sum += n.buffer().capacity();
        }
        sum
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.get_buffer_memory_size()
    }
}

impl<'a> ArrayAccessor for &'a FixedSizeBinaryArray {
    type Item = &'a [u8];

    fn value(&self, index: usize) -> Self::Item {
        FixedSizeBinaryArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        FixedSizeBinaryArray::value_unchecked(self, index)
    }
}

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = FixedSizeBinaryIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FixedSizeBinaryIter::<'a>::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::RecordBatch;
    use arrow_schema::{Field, Schema};

    use super::*;

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
        assert_eq!(0, fixed_size_binary_array.value_offset(0));
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(5, fixed_size_binary_array.value_offset(1));
    }

    #[test]
    fn test_fixed_size_binary_array_from_fixed_size_list_array() {
        let values = [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .offset(2)
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();
        // [null, [10, 11, 12, 13]]
        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, false)),
                4,
            ))
            .len(2)
            .offset(1)
            .add_child_data(values_data)
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b101])))
            .build_unchecked()
        };
        let list_array = FixedSizeListArray::from(array_data);
        let binary_array = FixedSizeBinaryArray::from(list_array);

        assert_eq!(2, binary_array.len());
        assert_eq!(1, binary_array.null_count());
        assert!(binary_array.is_null(0));
        assert!(binary_array.is_valid(1));
        assert_eq!(&[10, 11, 12, 13], binary_array.value(1));
    }

    #[test]
    #[should_panic(
        expected = "FixedSizeBinaryArray can only be created from FixedSizeList<u8> arrays"
    )]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_fixed_size_binary_array_from_incorrect_fixed_size_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();

        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Binary, false)),
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
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_fixed_size_binary_array_from_fixed_size_list_array_with_child_nulls_failed() {
        let values = [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(values))
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b101010101010])))
            .build()
            .unwrap();

        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, false)),
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
    fn test_fixed_size_binary_array_fmt_debug() {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let arr = FixedSizeBinaryArray::from(array_data);
        assert_eq!(
            "FixedSizeBinaryArray<5>\n[\n  [104, 101, 108, 108, 111],\n  [116, 104, 101, 114, 101],\n  [97, 114, 114, 111, 119],\n]",
            format!("{arr:?}")
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
        #[allow(deprecated)]
        let arr = FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
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
        #[allow(deprecated)]
        let arr = FixedSizeBinaryArray::try_from_sparse_iter(input_arg.iter().cloned()).unwrap();
        assert_eq!(2, arr.value_length());
        assert_eq!(5, arr.len());

        let arr =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 2).unwrap();
        assert_eq!(2, arr.value_length());
        assert_eq!(5, arr.len());
    }

    #[test]
    fn test_fixed_size_binary_array_from_sparse_iter_with_size_all_none() {
        let input_arg = vec![None, None, None, None, None] as Vec<Option<Vec<u8>>>;

        let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 16)
            .unwrap();
        assert_eq!(16, arr.value_length());
        assert_eq!(5, arr.len())
    }

    #[test]
    fn test_fixed_size_binary_array_from_vec() {
        let values = vec!["one".as_bytes(), b"two", b"six", b"ten"];
        let array = FixedSizeBinaryArray::from(values);
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 0);
        assert_eq!(array.value(0), b"one");
        assert_eq!(array.value(1), b"two");
        assert_eq!(array.value(2), b"six");
        assert_eq!(array.value(3), b"ten");
        assert!(!array.is_null(0));
        assert!(!array.is_null(1));
        assert!(!array.is_null(2));
        assert!(!array.is_null(3));
    }

    #[test]
    #[should_panic(expected = "Nested array size mismatch: one is 3, and the other is 5")]
    fn test_fixed_size_binary_array_from_vec_incorrect_length() {
        let values = vec!["one".as_bytes(), b"two", b"three", b"four"];
        let _ = FixedSizeBinaryArray::from(values);
    }

    #[test]
    fn test_fixed_size_binary_array_from_opt_vec() {
        let values = vec![
            Some("one".as_bytes()),
            Some(b"two"),
            None,
            Some(b"six"),
            Some(b"ten"),
        ];
        let array = FixedSizeBinaryArray::from(values);
        assert_eq!(array.len(), 5);
        assert_eq!(array.value(0), b"one");
        assert_eq!(array.value(1), b"two");
        assert_eq!(array.value(3), b"six");
        assert_eq!(array.value(4), b"ten");
        assert!(!array.is_null(0));
        assert!(!array.is_null(1));
        assert!(array.is_null(2));
        assert!(!array.is_null(3));
        assert!(!array.is_null(4));
    }

    #[test]
    #[should_panic(expected = "Nested array size mismatch: one is 3, and the other is 5")]
    fn test_fixed_size_binary_array_from_opt_vec_incorrect_length() {
        let values = vec![
            Some("one".as_bytes()),
            Some(b"two"),
            None,
            Some(b"three"),
            Some(b"four"),
        ];
        let _ = FixedSizeBinaryArray::from(values);
    }

    #[test]
    fn fixed_size_binary_array_all_null() {
        let data = vec![None] as Vec<Option<String>>;
        let array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(data.into_iter(), 0).unwrap();
        array
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    // Test for https://github.com/apache/arrow-rs/issues/1390
    fn fixed_size_binary_array_all_null_in_batch_with_schema() {
        let schema = Schema::new(vec![Field::new("a", DataType::FixedSizeBinary(2), true)]);

        let none_option: Option<[u8; 2]> = None;
        let item = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![none_option, none_option, none_option].into_iter(),
            2,
        )
        .unwrap();

        // Should not panic
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(item)]).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a FixedSizeBinaryArray of length 3"
    )]
    fn test_fixed_size_binary_array_get_value_index_out_of_bound() {
        let values = vec![Some("one".as_bytes()), Some(b"two"), None];
        let array = FixedSizeBinaryArray::from(values);

        array.value(4);
    }

    #[test]
    fn test_constructors() {
        let buffer = Buffer::from_vec(vec![0_u8; 10]);
        let a = FixedSizeBinaryArray::new(2, buffer.clone(), None);
        assert_eq!(a.len(), 5);

        let nulls = NullBuffer::new_null(5);
        FixedSizeBinaryArray::new(2, buffer.clone(), Some(nulls));

        let a = FixedSizeBinaryArray::new(3, buffer.clone(), None);
        assert_eq!(a.len(), 3);

        let nulls = NullBuffer::new_null(3);
        FixedSizeBinaryArray::new(3, buffer.clone(), Some(nulls));

        let err = FixedSizeBinaryArray::try_new(-1, buffer.clone(), None).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Size cannot be negative, got -1"
        );

        let nulls = NullBuffer::new_null(3);
        let err = FixedSizeBinaryArray::try_new(2, buffer, Some(nulls)).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Incorrect length of null buffer for FixedSizeBinaryArray, expected 5 got 3");
    }
}
