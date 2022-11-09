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

use crate::iterator::FixedSizeBinaryIter;
use crate::raw_pointer::RawPtrBox;
use crate::{print_long_array, Array, ArrayAccessor, FixedSizeListArray};
use arrow_buffer::{bit_util, Buffer, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::any::Any;

/// An array where each element is a fixed-size sequence of bytes.
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
    /// # Panics
    /// Panics if index `i` is out of bounds.
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(
            i < self.data.len(),
            "Trying to access an element at index {} from a FixedSizeBinaryArray of length {}",
            i,
            self.len()
        );
        let offset = i + self.data.offset();
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
        let offset = i + self.data.offset();
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
    pub fn try_from_sparse_iter<T, U>(iter: T) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        Self::try_from_sparse_iter_with_size(iter, None)
    }

    pub fn try_from_sparse_iter_with_size<T, U>(
        mut iter: T,
        asserted_size: Option<i32>,
    ) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut detected_size = None;
        let mut byte = 0;
        let mut null_buf = MutableBuffer::from_len_zeroed(0);
        let mut buffer = MutableBuffer::from_len_zeroed(0);
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
                if let Some(detected_size) = detected_size {
                    if detected_size != slice.len() {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Nested array size mismatch: one is {}, and the other is {}",
                            detected_size,
                            slice.len()
                        )));
                    }
                } else {
                    detected_size = Some(slice.len());
                    buffer.extend_zeros(slice.len() * prepend);
                }
                bit_util::set_bit(null_buf.as_slice_mut(), len);
                buffer.extend_from_slice(slice);
            } else if let Some(size) = detected_size {
                buffer.extend_zeros(size);
            } else {
                prepend += 1;
            }

            len += 1;

            Ok(())
        })?;

        if len == 0 && asserted_size.is_none() {
            return Err(ArrowError::InvalidArgumentError(
                "Input iterable argument has no data".to_owned(),
            ));
        }

        let size = if let Some(detected_size) = detected_size {
            // Either asserted size is zero (because we are entering this
            // function by way of FixedSizeBinaryArray::try_from_sparse_iter), or
            // the detected size has to be equal to the asserted size. If neither
            // of these conditions hold then we need to report an error.
            if let Some(asserted_size) = asserted_size {
                if detected_size as i32 != asserted_size {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Nested array size mismatch: detected size is {}, and the asserted size is {}",
                        detected_size,
                        asserted_size
                    )));
                }
                asserted_size
            } else {
                detected_size as i32
            }
        } else {
            if let Some(asserted_size) = asserted_size {
                // If we _haven't_ detected a size, then our provided iterator has
                // only produced `None` values. We need to extend the buffer to the
                // expected size else we'll segfault if we attempt to use this array.
                buffer.extend_zeros(len * (asserted_size as usize));
            }

            asserted_size.unwrap_or(0)
        };

        let array_data = unsafe {
            ArrayData::new_unchecked(
                DataType::FixedSizeBinary(size),
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
        let mut buffer = MutableBuffer::from_len_zeroed(0);
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

impl From<FixedSizeBinaryArray> for ArrayData {
    fn from(array: FixedSizeBinaryArray) -> Self {
        array.data
    }
}

/// Creates a `FixedSizeBinaryArray` from `FixedSizeList<u8>` array
impl From<FixedSizeListArray> for FixedSizeBinaryArray {
    fn from(v: FixedSizeListArray) -> Self {
        assert_eq!(
            v.data_ref().child_data().len(),
            1,
            "FixedSizeBinaryArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        let child_data = &v.data_ref().child_data()[0];

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

        let builder = ArrayData::builder(DataType::FixedSizeBinary(v.value_length()))
            .len(v.len())
            .offset(v.offset())
            .add_buffer(child_data.buffers()[0].slice(child_data.offset()))
            .null_bit_buffer(v.data_ref().null_buffer().cloned());

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }
}

impl From<Vec<Option<&[u8]>>> for FixedSizeBinaryArray {
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        Self::try_from_sparse_iter(v.into_iter()).unwrap()
    }
}

impl From<Vec<&[u8]>> for FixedSizeBinaryArray {
    fn from(v: Vec<&[u8]>) -> Self {
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

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
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
    use std::sync::Arc;

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
        assert_eq!(5, fixed_size_binary_array.value_offset(0));
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(10, fixed_size_binary_array.value_offset(1));
    }

    #[test]
    fn test_fixed_size_binary_array_from_fixed_size_list_array() {
        let values = [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .offset(2)
            .add_buffer(Buffer::from_slice_ref(&values))
            .build()
            .unwrap();
        // [null, [10, 11, 12, 13]]
        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt8, false)),
                4,
            ))
            .len(2)
            .offset(1)
            .add_child_data(values_data)
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b101])))
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
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_fixed_size_binary_array_from_fixed_size_list_array_with_child_nulls_failed() {
        let values = [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(&values))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b101010101010])))
            .build()
            .unwrap();

        let array_data = unsafe {
            ArrayData::builder(DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt8, false)),
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
    fn test_fixed_size_binary_array_from_sparse_iter_with_size_all_none() {
        let input_arg = vec![None, None, None, None, None] as Vec<Option<Vec<u8>>>;

        let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            input_arg.into_iter(),
            Some(16),
        )
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
        let array = FixedSizeBinaryArray::try_from_sparse_iter(data.into_iter()).unwrap();
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    // Test for https://github.com/apache/arrow-rs/issues/1390
    #[should_panic(
        expected = "column types must match schema types, expected FixedSizeBinary(2) but found FixedSizeBinary(0) at column index 0"
    )]
    fn fixed_size_binary_array_all_null_in_batch_with_schema() {
        let schema =
            Schema::new(vec![Field::new("a", DataType::FixedSizeBinary(2), true)]);

        let none_option: Option<[u8; 2]> = None;
        let item = FixedSizeBinaryArray::try_from_sparse_iter(
            vec![none_option, none_option, none_option].into_iter(),
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
}
