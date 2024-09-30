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

use crate::types::{ByteArrayType, GenericBinaryType};
use crate::{Array, GenericByteArray, GenericListArray, GenericStringArray, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::DataType;

/// A [`GenericBinaryArray`] for storing `[u8]`
pub type GenericBinaryArray<OffsetSize> = GenericByteArray<GenericBinaryType<OffsetSize>>;

impl<OffsetSize: OffsetSizeTrait> GenericBinaryArray<OffsetSize> {
    /// Get the data type of the array.
    #[deprecated(note = "please use `Self::DATA_TYPE` instead")]
    pub const fn get_data_type() -> DataType {
        Self::DATA_TYPE
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
        let v = v.into_data();
        assert_eq!(
            v.child_data().len(),
            1,
            "BinaryArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        let child_data = &v.child_data()[0];

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
            .add_buffer(v.buffers()[0].clone())
            .add_buffer(child_data.buffers()[0].slice(child_data.offset()))
            .nulls(v.nulls().cloned());

        let data = unsafe { builder.build_unchecked() };
        Self::from(data)
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    pub fn take_iter<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&'a [u8]>> {
        indexes.map(|opt_index| opt_index.map(|index| self.value(index)))
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    /// # Safety
    ///
    /// caller must ensure that the indexes in the iterator are less than the `array.len()`
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&'a [u8]>> {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<Option<&[u8]>>> for GenericBinaryArray<OffsetSize> {
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

impl<OffsetSize: OffsetSizeTrait> From<GenericStringArray<OffsetSize>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(value: GenericStringArray<OffsetSize>) -> Self {
        let builder = value
            .into_data()
            .into_builder()
            .data_type(GenericBinaryType::<OffsetSize>::DATA_TYPE);

        // Safety:
        // A StringArray is a valid BinaryArray
        Self::from(unsafe { builder.build_unchecked() })
    }
}

/// A [`GenericBinaryArray`] of `[u8]` using `i32` offsets
///
/// The byte length of each element is represented by an i32.
///
/// # Examples
///
/// Create a BinaryArray from a vector of byte slices.
///
/// ```
/// use arrow_array::{Array, BinaryArray};
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
/// use arrow_array::{Array, BinaryArray};
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
/// See [`GenericByteArray`] for more information and examples
pub type BinaryArray = GenericBinaryArray<i32>;

/// A [`GenericBinaryArray`] of `[u8]` using `i64` offsets
///
/// # Examples
///
/// Create a LargeBinaryArray from a vector of byte slices.
///
/// ```
/// use arrow_array::{Array, LargeBinaryArray};
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
/// use arrow_array::{Array, LargeBinaryArray};
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
/// See [`GenericByteArray`] for more information and examples
pub type LargeBinaryArray = GenericBinaryArray<i64>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ListArray, StringArray};
    use arrow_buffer::Buffer;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
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
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
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
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
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
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
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
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();
        let binary_array1 = GenericBinaryArray::<O>::from(array_data1);

        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Arc::new(Field::new(
            "item",
            DataType::UInt8,
            false,
        )));

        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data2);
        let binary_array2 = GenericBinaryArray::<O>::from(list_array);

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
        let null_buffer = Buffer::from_slice_ref([0b101]);
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Arc::new(Field::new(
            "item",
            DataType::UInt8,
            false,
        )));

        // [None, Some(b"Parquet")]
        let array_data = ArrayData::builder(data_type)
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(offsets))
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

    fn _test_generic_binary_array_from_list_array_with_child_nulls_failed<O: OffsetSizeTrait>() {
        let values = b"HelloArrow";
        let child_data = ArrayData::builder(DataType::UInt8)
            .len(10)
            .add_buffer(Buffer::from(&values[..]))
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b1010101010])))
            .build()
            .unwrap();

        let offsets = [0, 5, 10].map(|n| O::from_usize(n).unwrap());
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Arc::new(Field::new(
            "item",
            DataType::UInt8,
            true,
        )));

        // [None, Some(b"Parquet")]
        let array_data = ArrayData::builder(data_type)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(offsets))
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
                    Some(Some(format!("value {i}")))
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
        expected = "BinaryArray can only be created from List<u8> arrays, mismatched data types."
    )]
    fn test_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let data_type = DataType::List(Arc::new(Field::new("item", DataType::UInt32, false)));
        let array_data = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_child_data(values_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(array_data);
        drop(BinaryArray::from(list_array));
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a BinaryArray of length 3"
    )]
    fn test_binary_array_get_value_index_out_of_bound() {
        let values: [u8; 12] = [104, 101, 108, 108, 111, 112, 97, 114, 113, 117, 101, 116];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();
        let binary_array = BinaryArray::from(array_data);
        binary_array.value(4);
    }

    #[test]
    #[should_panic(expected = "LargeBinaryArray expects DataType::LargeBinary")]
    fn test_binary_array_validation() {
        let array = BinaryArray::from_iter_values([&[1, 2]]);
        let _ = LargeBinaryArray::from(array.into_data());
    }

    #[test]
    fn test_binary_array_all_null() {
        let data = vec![None];
        let array = BinaryArray::from(data);
        array
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_large_binary_array_all_null() {
        let data = vec![None];
        let array = LargeBinaryArray::from(data);
        array
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_empty_offsets() {
        let string = BinaryArray::from(
            ArrayData::builder(DataType::Binary)
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .build()
                .unwrap(),
        );
        assert_eq!(string.value_offsets(), &[0]);
        let string = LargeBinaryArray::from(
            ArrayData::builder(DataType::LargeBinary)
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[0]);
    }

    #[test]
    fn test_to_from_string() {
        let s = StringArray::from_iter_values(["a", "b", "c", "d"]);
        let b = BinaryArray::from(s.clone());
        let sa = StringArray::from(b); // Performs UTF-8 validation again

        assert_eq!(s, sa);
    }
}
