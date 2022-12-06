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
use crate::{
    Array, GenericBinaryArray, GenericByteArray, GenericListArray, OffsetSizeTrait,
};
use arrow_buffer::{bit_util, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;

/// Generic struct for \[Large\]StringArray
///
/// See [`StringArray`] and [`LargeStringArray`] for storing
/// specific string data.
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

    /// Convert a list array to a string array.
    ///
    /// Note: this performs potentially expensive UTF-8 validation, consider using
    /// [`StringBuilder`][crate::builder::StringBuilder] to avoid this
    ///
    /// # Panics
    ///
    /// This method panics if the array contains non-UTF-8 data
    fn from_list(v: GenericListArray<OffsetSize>) -> Self {
        assert_eq!(
            v.data_ref().child_data().len(),
            1,
            "StringArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        let child_data = &v.data_ref().child_data()[0];

        assert_eq!(
            child_data.child_data().len(),
            0,
            "StringArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            child_data.data_type(),
            &DataType::UInt8,
            "StringArray can only be created from List<u8> arrays, mismatched data types."
        );
        assert_eq!(
            child_data.null_count(),
            0,
            "The child array cannot contain null values."
        );

        let builder = ArrayData::builder(Self::DATA_TYPE)
            .len(v.len())
            .offset(v.offset())
            .add_buffer(v.data().buffers()[0].clone())
            .add_buffer(child_data.buffers()[0].slice(child_data.offset()))
            .null_bit_buffer(v.data().null_buffer().cloned());

        Self::from(builder.build().unwrap())
    }

    /// Creates a [`GenericStringArray`] based on an iterator of values without nulls
    pub fn from_iter_values<Ptr, I>(iter: I) -> Self
    where
        Ptr: AsRef<str>,
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

        for i in iter {
            let s = i.as_ref();
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s.as_bytes());
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
    ) -> impl Iterator<Item = Option<&str>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value(index)))
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    /// # Safety
    ///
    /// caller must ensure that the indexes in the iterator are less than the `array.len()`
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<&str>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }
}

impl<'a, Ptr, OffsetSize: OffsetSizeTrait> FromIterator<&'a Option<Ptr>>
    for GenericStringArray<OffsetSize>
where
    Ptr: AsRef<str> + 'a,
{
    /// Creates a [`GenericStringArray`] based on an iterator of `Option` references.
    fn from_iter<I: IntoIterator<Item = &'a Option<Ptr>>>(iter: I) -> Self {
        // Convert each owned Ptr into &str and wrap in an owned `Option`
        let iter = iter.into_iter().map(|o| o.as_ref().map(|p| p.as_ref()));
        // Build a `GenericStringArray` with the resulting iterator
        iter.collect::<GenericStringArray<OffsetSize>>()
    }
}

impl<Ptr, OffsetSize: OffsetSizeTrait> FromIterator<Option<Ptr>>
    for GenericStringArray<OffsetSize>
where
    Ptr: AsRef<str>,
{
    /// Creates a [`GenericStringArray`] based on an iterator of [`Option`]s
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let offset_size = std::mem::size_of::<OffsetSize>();
        let mut offsets = MutableBuffer::new((data_len + 1) * offset_size);
        let mut values = MutableBuffer::new(0);
        let mut null_buf = MutableBuffer::new_null(data_len);
        let null_slice = null_buf.as_slice_mut();
        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        for (i, s) in iter.enumerate() {
            let value_bytes = if let Some(ref s) = s {
                // set null bit
                bit_util::set_bit(null_slice, i);
                let s_bytes = s.as_ref().as_bytes();
                length_so_far += OffsetSize::from_usize(s_bytes.len()).unwrap();
                s_bytes
            } else {
                b""
            };
            values.extend_from_slice(value_bytes);
            offsets.push(length_so_far);
        }

        // calculate actual data_len, which may be different from the iterator's upper bound
        let data_len = (offsets.len() / offset_size) - 1;
        let array_data = ArrayData::builder(Self::DATA_TYPE)
            .len(data_len)
            .add_buffer(offsets.into())
            .add_buffer(values.into())
            .null_bit_buffer(Some(null_buf.into()));
        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericListArray<OffsetSize>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: GenericListArray<OffsetSize>) -> Self {
        GenericStringArray::<OffsetSize>::from_list(v)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericBinaryArray<OffsetSize>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: GenericBinaryArray<OffsetSize>) -> Self {
        let offsets = v.value_offsets();
        let values = v.value_data();

        // We only need to validate that all values are valid UTF-8
        let validated = std::str::from_utf8(values).expect("Invalid UTF-8 sequence");
        for offset in offsets.iter() {
            assert!(
                validated.is_char_boundary(offset.as_usize()),
                "Invalid UTF-8 sequence"
            )
        }

        let builder = v.into_data().into_builder().data_type(Self::DATA_TYPE);
        // SAFETY:
        // Validated UTF-8 above
        Self::from(unsafe { builder.build_unchecked() })
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<Option<&str>>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: Vec<Option<&str>>) -> Self {
        v.into_iter().collect()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<&str>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<&str>) -> Self {
        Self::from_iter_values(v)
    }
}

impl<OffsetSize: OffsetSizeTrait> From<Vec<String>> for GenericStringArray<OffsetSize> {
    fn from(v: Vec<String>) -> Self {
        Self::from_iter_values(v)
    }
}

/// An array where each element is a variable-sized sequence of bytes representing a string
/// whose maximum length (in bytes) is represented by a i32.
///
/// Example
///
/// ```
/// use arrow_array::StringArray;
/// let array = StringArray::from(vec![Some("foo"), None, Some("bar")]);
/// assert_eq!(array.value(0), "foo");
/// ```
pub type StringArray = GenericStringArray<i32>;

/// An array where each element is a variable-sized sequence of bytes representing a string
/// whose maximum length (in bytes) is represented by a i64.
///
/// Example
///
/// ```
/// use arrow_array::LargeStringArray;
/// let array = LargeStringArray::from(vec![Some("foo"), None, Some("bar")]);
/// assert_eq!(array.value(2), "bar");
/// ```
pub type LargeStringArray = GenericStringArray<i64>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{ListBuilder, StringBuilder};
    use arrow_buffer::Buffer;
    use arrow_schema::Field;

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
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_large_string_array_fmt_debug() {
        let arr: LargeStringArray = vec!["hello", "arrow"].into();
        assert_eq!(
            "LargeStringArray\n[\n  \"hello\",\n  \"arrow\",\n]",
            format!("{:?}", arr)
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
        let data = vec!["hello", "hello2"];
        let array1 = StringArray::from_iter_values(data.iter());

        assert_eq!(array1.value(0), "hello");
        assert_eq!(array1.value(1), "hello2");
    }

    #[test]
    fn test_string_array_from_unbound_iter() {
        // iterator that doesn't declare (upper) size bound
        let string_iter = (0..)
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

        let (_, upper_size_bound) = string_iter.size_hint();
        // the upper bound, defined by take above, is 100
        assert_eq!(upper_size_bound, Some(100));
        let string_array: StringArray = string_iter.collect();
        // but the actual number of items in the array should be 10
        assert_eq!(string_array.len(), 10);
    }

    #[test]
    fn test_string_array_all_null() {
        let data = vec![None];
        let array = StringArray::from(data);
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_large_string_array_all_null() {
        let data = vec![None];
        let array = LargeStringArray::from(data);
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn bad_size_collect_string() {
        use crate::util::test_util::BadIterator;
        let data = vec![Some("foo"), None, Some("bar")];
        let expected: StringArray = data.clone().into_iter().collect();

        // Iterator reports too many items
        let arr: StringArray = BadIterator::new(3, 10, data.clone()).collect();
        assert_eq!(expected, arr);

        // Iterator reports too few items
        let arr: StringArray = BadIterator::new(3, 1, data.clone()).collect();
        assert_eq!(expected, arr);
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn bad_size_collect_large_string() {
        use crate::util::test_util::BadIterator;
        let data = vec![Some("foo"), None, Some("bar")];
        let expected: LargeStringArray = data.clone().into_iter().collect();

        // Iterator reports too many items
        let arr: LargeStringArray = BadIterator::new(3, 10, data.clone()).collect();
        assert_eq!(expected, arr);

        // Iterator reports too few items
        let arr: LargeStringArray = BadIterator::new(3, 1, data.clone()).collect();
        assert_eq!(expected, arr);
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn bad_size_iter_values_string() {
        use crate::util::test_util::BadIterator;
        let data = vec!["foo", "bar", "baz"];
        let expected: StringArray = data.clone().into_iter().map(Some).collect();

        // Iterator reports too many items
        let arr = StringArray::from_iter_values(BadIterator::new(3, 10, data.clone()));
        assert_eq!(expected, arr);

        // Iterator reports too few items
        let arr = StringArray::from_iter_values(BadIterator::new(3, 1, data.clone()));
        assert_eq!(expected, arr);
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn bad_size_iter_values_large_string() {
        use crate::util::test_util::BadIterator;
        let data = vec!["foo", "bar", "baz"];
        let expected: LargeStringArray = data.clone().into_iter().map(Some).collect();

        // Iterator reports too many items
        let arr =
            LargeStringArray::from_iter_values(BadIterator::new(3, 10, data.clone()));
        assert_eq!(expected, arr);

        // Iterator reports too few items
        let arr =
            LargeStringArray::from_iter_values(BadIterator::new(3, 1, data.clone()));
        assert_eq!(expected, arr);
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
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt8, false),
        ));

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

    fn _test_generic_string_array_from_list_array_with_child_nulls_failed<
        O: OffsetSizeTrait,
    >() {
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
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt8, true),
        ));

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
    fn test_stirng_array_from_list_array_with_child_nulls_failed() {
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
        let data_type = GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(Box::new(
            Field::new("item", DataType::UInt16, false),
        ));

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
        expected = "StringArray can only be created from List<u8> arrays, mismatched data types."
    )]
    fn test_string_array_from_list_array_wrong_type() {
        _test_generic_string_array_from_list_array_wrong_type::<i32>();
    }

    #[test]
    #[should_panic(
        expected = "StringArray can only be created from List<u8> arrays, mismatched data types."
    )]
    fn test_large_string_array_from_list_array_wrong_type() {
        _test_generic_string_array_from_list_array_wrong_type::<i32>();
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
}
