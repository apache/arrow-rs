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

use crate::types::GenericStringType;
use crate::{GenericBinaryArray, GenericByteArray, GenericListArray, OffsetSizeTrait};
use arrow_schema::{ArrowError, DataType};

/// A [`GenericByteArray`] for storing `str`
pub type GenericStringArray<OffsetSize> = GenericByteArray<GenericStringType<OffsetSize>>;

impl<OffsetSize: OffsetSizeTrait> GenericStringArray<OffsetSize> {
    /// Get the data type of the array.
    #[deprecated(note = "please use `Self::DATA_TYPE` instead")]
    pub const fn get_data_type() -> DataType {
        Self::DATA_TYPE
    }

    /// Returns the number of `Unicode Scalar Value` in the string at index `i`.
    /// # Performance
    /// This function has `O(n)` time complexity where `n` is the string length.
    /// If you can make sure that all chars in the string are in the range `U+0x0000` ~ `U+0x007F`,
    /// please use the function [`value_length`](#method.value_length) which has O(1) time complexity.
    pub fn num_chars(&self, i: usize) -> usize {
        self.value(i).chars().count()
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    pub fn take_iter<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&'a str>> {
        indexes.map(|opt_index| opt_index.map(|index| self.value(index)))
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    /// # Safety
    ///
    /// caller must ensure that the indexes in the iterator are less than the `array.len()`
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&'a str>> {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }

    /// Fallibly creates a [`GenericStringArray`] from a [`GenericBinaryArray`] returning
    /// an error if [`GenericBinaryArray`] contains invalid UTF-8 data
    pub fn try_from_binary(v: GenericBinaryArray<OffsetSize>) -> Result<Self, ArrowError> {
        let (offsets, values, nulls) = v.into_parts();
        Self::try_new(offsets, values, nulls)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericListArray<OffsetSize>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: GenericListArray<OffsetSize>) -> Self {
        GenericBinaryArray::<OffsetSize>::from(v).into()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericBinaryArray<OffsetSize>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: GenericBinaryArray<OffsetSize>) -> Self {
        Self::try_from_binary(v).unwrap()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<Option<&str>>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<Option<&str>>) -> Self {
        v.into_iter().collect()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<&str>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<&str>) -> Self {
        Self::from_iter_values(v)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<Option<String>>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<Option<String>>) -> Self {
        v.into_iter().collect()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<String>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<String>) -> Self {
        Self::from_iter_values(v)
    }
}

/// A [`GenericStringArray`] of `str` using `i32` offsets
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::StringArray;
/// // Create from Vec<Option<&str>>
/// let arr = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
/// // Create from Vec<&str>
/// let arr = StringArray::from(vec!["foo", "bar", "baz"]);
/// // Create from iter/collect (requires Option<&str>)
/// let arr: StringArray = std::iter::repeat(Some("foo")).take(10).collect();
/// ```
///
/// Construction and Access
///
/// ```
/// # use arrow_array::StringArray;
/// let array = StringArray::from(vec![Some("foo"), None, Some("bar")]);
/// assert_eq!(array.value(0), "foo");
/// ```
///
/// See [`GenericByteArray`] for more information and examples
pub type StringArray = GenericStringArray<i32>;

/// A [`GenericStringArray`] of `str` using `i64` offsets
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::LargeStringArray;
/// // Create from Vec<Option<&str>>
/// let arr = LargeStringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
/// // Create from Vec<&str>
/// let arr = LargeStringArray::from(vec!["foo", "bar", "baz"]);
/// // Create from iter/collect (requires Option<&str>)
/// let arr: LargeStringArray = std::iter::repeat(Some("foo")).take(10).collect();
/// ```
///
/// Construction and Access
///
/// ```
/// use arrow_array::LargeStringArray;
/// let array = LargeStringArray::from(vec![Some("foo"), None, Some("bar")]);
/// assert_eq!(array.value(2), "bar");
/// ```
///
/// See [`GenericByteArray`] for more information and examples
pub type LargeStringArray = GenericStringArray<i64>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{ListBuilder, PrimitiveBuilder, StringBuilder};
    use crate::types::UInt8Type;
    use crate::Array;
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_string_array_from_u8_slice() {
        let values: Vec<&str> = vec!["hello", "", "A£ऀ𖼚𝌆৩ƐZ"];

        // Array data: ["hello", "", "A£ऀ𖼚𝌆৩ƐZ"]
        let string_array = StringArray::from(values);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("hello", unsafe { string_array.value_unchecked(0) });
        assert_eq!("", string_array.value(1));
        assert_eq!("", unsafe { string_array.value_unchecked(1) });
        assert_eq!("A£ऀ𖼚𝌆৩ƐZ", string_array.value(2));
        assert_eq!("A£ऀ𖼚𝌆৩ƐZ", unsafe {
            string_array.value_unchecked(2)
        });
        assert_eq!(20, string_array.value_length(2)); // 1 + 2 + 3 + 4 + 4 + 3 + 2 + 1
        assert_eq!(8, string_array.num_chars(2));
        for i in 0..3 {
            assert!(string_array.is_valid(i));
            assert!(!string_array.is_null(i));
        }
    }

    #[test]
    #[should_panic(expected = "StringArray expects DataType::Utf8")]
    fn test_string_array_from_int() {
        let array = LargeStringArray::from(vec!["a", "b"]);
        drop(StringArray::from(array.into_data()));
    }

    #[test]
    fn test_large_string_array_from_u8_slice() {
        let values: Vec<&str> = vec!["hello", "", "A£ऀ𖼚𝌆৩ƐZ"];

        // Array data: ["hello", "", "A£ऀ𖼚𝌆৩ƐZ"]
        let string_array = LargeStringArray::from(values);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("hello", unsafe { string_array.value_unchecked(0) });
        assert_eq!("", string_array.value(1));
        assert_eq!("", unsafe { string_array.value_unchecked(1) });
        assert_eq!("A£ऀ𖼚𝌆৩ƐZ", string_array.value(2));
        assert_eq!("A£ऀ𖼚𝌆৩ƐZ", unsafe {
            string_array.value_unchecked(2)
        });
        assert_eq!(5, string_array.value_offsets()[2]);
        assert_eq!(20, string_array.value_length(2)); // 1 + 2 + 3 + 4 + 4 + 3 + 2 + 1
        assert_eq!(8, string_array.num_chars(2));
        for i in 0..3 {
            assert!(string_array.is_valid(i));
            assert!(!string_array.is_null(i));
        }
    }

    #[test]
    fn test_nested_string_array() {
        let string_builder = StringBuilder::with_capacity(3, 10);
        let mut list_of_string_builder = ListBuilder::new(string_builder);

        list_of_string_builder.values().append_value("foo");
        list_of_string_builder.values().append_value("bar");
        list_of_string_builder.append(true);

        list_of_string_builder.values().append_value("foobar");
        list_of_string_builder.append(true);
        let list_of_strings = list_of_string_builder.finish();

        assert_eq!(list_of_strings.len(), 2);

        let first_slot = list_of_strings.value(0);
        let first_list = first_slot.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(first_list.len(), 2);
        assert_eq!(first_list.value(0), "foo");
        assert_eq!(unsafe { first_list.value_unchecked(0) }, "foo");
        assert_eq!(first_list.value(1), "bar");
        assert_eq!(unsafe { first_list.value_unchecked(1) }, "bar");

        let second_slot = list_of_strings.value(1);
        let second_list = second_slot.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(second_list.len(), 1);
        assert_eq!(second_list.value(0), "foobar");
        assert_eq!(unsafe { second_list.value_unchecked(0) }, "foobar");
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a StringArray of length 3"
    )]
    fn test_string_array_get_value_index_out_of_bound() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_slice_ref(values))
            .build()
            .unwrap();
        let string_array = StringArray::from(array_data);
        string_array.value(4);
    }

    #[test]
    fn test_string_array_fmt_debug() {
        let arr: StringArray = vec!["hello", "arrow"].into();
        assert_eq!(
            "StringArray\n[\n  \"hello\",\n  \"arrow\",\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_large_string_array_fmt_debug() {
        let arr: LargeStringArray = vec!["hello", "arrow"].into();
        assert_eq!(
            "LargeStringArray\n[\n  \"hello\",\n  \"arrow\",\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_string_array_from_iter() {
        let data = [Some("hello"), None, Some("arrow")];
        let data_vec = data.to_vec();
        // from Vec<Option<&str>>
        let array1 = StringArray::from(data_vec.clone());
        // from Iterator<Option<&str>>
        let array2: StringArray = data_vec.clone().into_iter().collect();
        // from Iterator<Option<String>>
        let array3: StringArray = data_vec
            .into_iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect();
        // from Iterator<&Option<&str>>
        let array4: StringArray = data.iter().collect::<StringArray>();

        assert_eq!(array1, array2);
        assert_eq!(array2, array3);
        assert_eq!(array3, array4);
    }

    #[test]
    fn test_string_array_from_iter_values() {
        let data = ["hello", "hello2"];
        let array1 = StringArray::from_iter_values(data.iter());

        assert_eq!(array1.value(0), "hello");
        assert_eq!(array1.value(1), "hello2");

        // Also works with String types.
        let data2 = ["goodbye".to_string(), "goodbye2".to_string()];
        let array2 = StringArray::from_iter_values(data2.iter());

        assert_eq!(array2.value(0), "goodbye");
        assert_eq!(array2.value(1), "goodbye2");
    }

    #[test]
    fn test_string_array_from_unbound_iter() {
        // iterator that doesn't declare (upper) size bound
        let string_iter = (0..)
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

        let (_, upper_size_bound) = string_iter.size_hint();
        // the upper bound, defined by take above, is 100
        assert_eq!(upper_size_bound, Some(100));
        let string_array: StringArray = string_iter.collect();
        // but the actual number of items in the array should be 10
        assert_eq!(string_array.len(), 10);
    }

    #[test]
    fn test_string_array_all_null() {
        let data: Vec<Option<&str>> = vec![None];
        let array = StringArray::from(data);
        array
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_large_string_array_all_null() {
        let data: Vec<Option<&str>> = vec![None];
        let array = LargeStringArray::from(data);
        array
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    fn _test_generic_string_array_from_list_array<O: OffsetSizeTrait>() {
        let values = b"HelloArrowAndParquet";
        // "ArrowAndParquet"
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

        // [None, Some("Parquet")]
        let array_data = ArrayData::builder(data_type)
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .null_bit_buffer(Some(null_buffer))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data);
        let string_array = GenericStringArray::<O>::from(list_array);

        assert_eq!(2, string_array.len());
        assert_eq!(1, string_array.null_count());
        assert!(string_array.is_null(0));
        assert!(string_array.is_valid(1));
        assert_eq!("Parquet", string_array.value(1));
    }

    #[test]
    fn test_string_array_from_list_array() {
        _test_generic_string_array_from_list_array::<i32>();
    }

    #[test]
    fn test_large_string_array_from_list_array() {
        _test_generic_string_array_from_list_array::<i64>();
    }

    fn _test_generic_string_array_from_list_array_with_child_nulls_failed<O: OffsetSizeTrait>() {
        let values = b"HelloArrow";
        let child_data = ArrayData::builder(DataType::UInt8)
            .len(10)
            .add_buffer(Buffer::from(&values[..]))
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b1010101010])))
            .build()
            .unwrap();

        let offsets = [0, 5, 10].map(|n| O::from_usize(n).unwrap());

        // It is possible to create a null struct containing a non-nullable child
        // see https://github.com/apache/arrow-rs/pull/3244 for details
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
        drop(GenericStringArray::<O>::from(list_array));
    }

    #[test]
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_string_array_from_list_array_with_child_nulls_failed() {
        _test_generic_string_array_from_list_array_with_child_nulls_failed::<i32>();
    }

    #[test]
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_large_string_array_from_list_array_with_child_nulls_failed() {
        _test_generic_string_array_from_list_array_with_child_nulls_failed::<i64>();
    }

    fn _test_generic_string_array_from_list_array_wrong_type<O: OffsetSizeTrait>() {
        let values = b"HelloArrow";
        let child_data = ArrayData::builder(DataType::UInt16)
            .len(5)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();

        let offsets = [0, 2, 3].map(|n| O::from_usize(n).unwrap());
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Arc::new(Field::new(
            "item",
            DataType::UInt16,
            false,
        )));

        let array_data = ArrayData::builder(data_type)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_child_data(child_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<O>::from(array_data);
        drop(GenericStringArray::<O>::from(list_array));
    }

    #[test]
    #[should_panic(
        expected = "BinaryArray can only be created from List<u8> arrays, mismatched data types."
    )]
    fn test_string_array_from_list_array_wrong_type() {
        _test_generic_string_array_from_list_array_wrong_type::<i32>();
    }

    #[test]
    #[should_panic(
        expected = "BinaryArray can only be created from List<u8> arrays, mismatched data types."
    )]
    fn test_large_string_array_from_list_array_wrong_type() {
        _test_generic_string_array_from_list_array_wrong_type::<i64>();
    }

    #[test]
    #[should_panic(
        expected = "Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 0"
    )]
    fn test_list_array_utf8_validation() {
        let mut builder = ListBuilder::new(PrimitiveBuilder::<UInt8Type>::new());
        builder.values().append_value(0xFF);
        builder.append(true);
        let list = builder.finish();
        let _ = StringArray::from(list);
    }

    #[test]
    fn test_empty_offsets() {
        let string = StringArray::from(
            ArrayData::builder(DataType::Utf8)
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[0]);

        let string = LargeStringArray::from(
            ArrayData::builder(DataType::LargeUtf8)
                .buffers(vec![Buffer::from(&[]), Buffer::from(&[])])
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[0]);
    }

    #[test]
    fn test_into_builder() {
        let array: StringArray = vec!["hello", "arrow"].into();

        // Append values
        let mut builder = array.into_builder().unwrap();

        builder.append_value("rust");

        let expected: StringArray = vec!["hello", "arrow", "rust"].into();
        let array = builder.finish();
        assert_eq!(expected, array);
    }

    #[test]
    fn test_into_builder_err() {
        let array: StringArray = vec!["hello", "arrow"].into();

        // Clone it, so we cannot get a mutable builder back
        let shared_array = array.clone();

        let err_return = array.into_builder().unwrap_err();
        assert_eq!(&err_return, &shared_array);
    }
}
