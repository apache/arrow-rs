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

use std::convert::From;
use std::fmt;
use std::{any::Any, iter::FromIterator};

use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData, GenericBinaryIter,
    GenericListArray, OffsetSizeTrait,
};
use crate::array::array::ArrayAccessor;
use crate::buffer::Buffer;
use crate::util::bit_util;
use crate::{buffer::MutableBuffer, datatypes::DataType};

/// See [`BinaryArray`] and [`LargeBinaryArray`] for storing
/// binary data.
pub struct GenericBinaryArray<OffsetSize: OffsetSizeTrait> {
    data: ArrayData,
    value_offsets: RawPtrBox<OffsetSize>,
    value_data: RawPtrBox<u8>,
}

impl<OffsetSize: OffsetSizeTrait> GenericBinaryArray<OffsetSize> {
    /// Data type of the array.
    pub const DATA_TYPE: DataType = if OffsetSize::IS_LARGE {
        DataType::LargeBinary
    } else {
        DataType::Binary
    };

    /// Get the data type of the array.
    #[deprecated(note = "please use `Self::DATA_TYPE` instead")]
    pub const fn get_data_type() -> DataType {
        Self::DATA_TYPE
    }

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
            v.data_ref().child_data().len(),
            1,
            "BinaryArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        let child_data = &v.data_ref().child_data()[0];

        assert_eq!(
            child_data.child_data().len(),
            0,
            "BinaryArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            child_data.data_type(),
            &DataType::UInt8,
            "BinaryArray can only be created from List<u8> arrays, mismatched data types."
        );
        assert_eq!(
            child_data.null_count(),
            0,
            "The child array cannot contain null values."
        );

        let builder = ArrayData::builder(Self::DATA_TYPE)
            .len(v.len())
            .offset(v.offset())
            .add_buffer(v.data_ref().buffers()[0].clone())
            .add_buffer(child_data.buffers()[0].slice(child_data.offset()))
            .null_bit_buffer(v.data_ref().null_buffer().cloned());

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }

    /// Creates a [`GenericBinaryArray`] based on an iterator of values without nulls
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

        let array_data = ArrayData::builder(Self::DATA_TYPE)
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
    /// caller must ensure that the indexes in the iterator are less than the `array.len()`
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&[u8]>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }
}

impl<'a, T: OffsetSizeTrait> GenericBinaryArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> GenericBinaryIter<'a, T> {
        GenericBinaryIter::<'a, T>::new(self)
    }
}

impl<OffsetSize: OffsetSizeTrait> fmt::Debug for GenericBinaryArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let prefix = OffsetSize::PREFIX;

        write!(f, "{}BinaryArray\n[\n", prefix)?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<OffsetSize: OffsetSizeTrait> Array for GenericBinaryArray<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl<'a, OffsetSize: OffsetSizeTrait> ArrayAccessor
    for &'a GenericBinaryArray<OffsetSize>
{
    type Item = &'a [u8];

    fn value(&self, index: usize) -> Self::Item {
        GenericBinaryArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericBinaryArray::value_unchecked(self, index)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<ArrayData> for GenericBinaryArray<OffsetSize> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.data_type(),
            &Self::DATA_TYPE,
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

impl<OffsetSize: OffsetSizeTrait> From<GenericBinaryArray<OffsetSize>> for ArrayData {
    fn from(array: GenericBinaryArray<OffsetSize>) -> Self {
        array.data
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<Option<&[u8]>>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        Self::from_opt_vec(v)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<&[u8]>> for GenericBinaryArray<OffsetSize> {
    fn from(v: Vec<&[u8]>) -> Self {
        Self::from_iter_values(v)
    }
}

impl<T: OffsetSizeTrait> From<GenericListArray<T>> for GenericBinaryArray<T> {
    fn from(v: GenericListArray<T>) -> Self {
        Self::from_list(v)
    }
}

impl<Ptr, OffsetSize: OffsetSizeTrait> FromIterator<Option<Ptr>>
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
        let array_data = ArrayData::builder(Self::DATA_TYPE)
            .len(data_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .null_bit_buffer(Some(null_buf.into()));
        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }
}

impl<'a, T: OffsetSizeTrait> IntoIterator for &'a GenericBinaryArray<T> {
    type Item = Option<&'a [u8]>;
    type IntoIter = GenericBinaryIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GenericBinaryIter::<'a, T>::new(self)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array::ListArray, datatypes::Field};

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

    fn _test_generic_binary_array_from_list_array<O: OffsetSizeTrait>() {
        let values = b"helloparquet";
        let child_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let offsets = [0, 5, 5, 12].map(|n| O::from_usize(n).unwrap());

        // Array data: ["hello", "", "parquet"]
        let array_data1 = ArrayData::builder(GenericBinaryArray::<O>::DATA_TYPE)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        let binary_array1 = GenericBinaryArray::<O>::from(array_data1);

        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt8, false),
        ));

        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data2);
        let binary_array2 = GenericBinaryArray::<O>::from(list_array);

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
    fn test_binary_array_from_list_array() {
        _test_generic_binary_array_from_list_array::<i32>();
    }

    #[test]
    fn test_large_binary_array_from_list_array() {
        _test_generic_binary_array_from_list_array::<i64>();
    }

    fn _test_generic_binary_array_from_list_array_with_offset<O: OffsetSizeTrait>() {
        let values = b"HelloArrowAndParquet";
        // b"ArrowAndParquet"
        let child_data = ArrayData::builder(DataType::UInt8)
            .len(15)
            .offset(5)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();

        let offsets = [0, 5, 8, 15].map(|n| O::from_usize(n).unwrap());
        let null_buffer = Buffer::from_slice_ref(&[0b101]);
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt8, false),
        ));

        // [None, Some(b"Parquet")]
        let array_data = ArrayData::builder(data_type)
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .null_bit_buffer(Some(null_buffer))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data);
        let binary_array = GenericBinaryArray::<O>::from(list_array);

        assert_eq!(2, binary_array.len());
        assert_eq!(1, binary_array.null_count());
        assert!(binary_array.is_null(0));
        assert!(binary_array.is_valid(1));
        assert_eq!(b"Parquet", binary_array.value(1));
    }

    #[test]
    fn test_binary_array_from_list_array_with_offset() {
        _test_generic_binary_array_from_list_array_with_offset::<i32>();
    }

    #[test]
    fn test_large_binary_array_from_list_array_with_offset() {
        _test_generic_binary_array_from_list_array_with_offset::<i64>();
    }

    fn _test_generic_binary_array_from_list_array_with_child_nulls_failed<
        O: OffsetSizeTrait,
    >() {
        let values = b"HelloArrow";
        let child_data = ArrayData::builder(DataType::UInt8)
            .len(10)
            .add_buffer(Buffer::from(&values[..]))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b1010101010])))
            .build()
            .unwrap();

        let offsets = [0, 5, 10].map(|n| O::from_usize(n).unwrap());
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt8, false),
        ));

        // [None, Some(b"Parquet")]
        let array_data = ArrayData::builder(data_type)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data);
        drop(GenericBinaryArray::<O>::from(list_array));
    }

    #[test]
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_binary_array_from_list_array_with_child_nulls_failed() {
        _test_generic_binary_array_from_list_array_with_child_nulls_failed::<i32>();
    }

    #[test]
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_large_binary_array_from_list_array_with_child_nulls_failed() {
        _test_generic_binary_array_from_list_array_with_child_nulls_failed::<i64>();
    }

    fn test_generic_binary_array_from_opt_vec<T: OffsetSizeTrait>() {
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
}
