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

//! Defines kernel to extract a substring of an Array
//! Supported array types:
//! [GenericStringArray], [GenericBinaryArray], [FixedSizeBinaryArray], [DictionaryArray]

use arrow_array::builder::BufferBuilder;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use num::Zero;
use std::cmp::Ordering;
use std::sync::Arc;

/// Returns an [`ArrayRef`] with substrings of all the elements in `array`.
///
/// # Arguments
///
/// * `start` - The start index of all substrings.
///   If `start >= 0`, then count from the start of the string,
///   otherwise count from the end of the string.
///
/// * `length`(option) - The length of all substrings.
///   If `length` is [None], then the substring is from `start` to the end of the string.
///
/// Attention: Both `start` and `length` are counted by byte, not by char.
///
/// # Basic usage
/// ```
/// # use arrow_array::StringArray;
/// # use arrow_string::substring::substring;
/// let array = StringArray::from(vec![Some("arrow"), None, Some("rust")]);
/// let result = substring(&array, 1, Some(4)).unwrap();
/// let result = result.as_any().downcast_ref::<StringArray>().unwrap();
/// assert_eq!(result, &StringArray::from(vec![Some("rrow"), None, Some("ust")]));
/// ```
///
/// # Error
/// - The function errors when the passed array is not a [`GenericStringArray`],
///   [`GenericBinaryArray`], [`FixedSizeBinaryArray`] or [`DictionaryArray`]
///   with supported array type as its value type.
/// - The function errors if the offset of a substring in the input array is
///   at invalid char boundary (only for \[Large\]String array).
///   It is recommended to use [`substring_by_char`] if the input array may
///   contain non-ASCII chars.
///
/// ## Example of trying to get an invalid utf-8 format substring
/// ```
/// # use arrow_array::StringArray;
/// # use arrow_string::substring::substring;
/// let array = StringArray::from(vec![Some("E=mc²")]);
/// let error = substring(&array, 0, Some(5)).unwrap_err().to_string();
/// assert!(error.contains("invalid utf-8 boundary"));
/// ```
pub fn substring(
    array: &dyn Array,
    start: i64,
    length: Option<u64>,
) -> Result<ArrayRef, ArrowError> {
    macro_rules! substring_dict {
        ($kt: ident, $($t: ident: $gt: ident), *) => {
            match $kt.as_ref() {
                $(
                    &DataType::$t => {
                        let dict = array
                            .as_any()
                            .downcast_ref::<DictionaryArray<$gt>>()
                            .unwrap_or_else(|| {
                                panic!("Expect 'DictionaryArray<{}>' but got array of data type {:?}",
                                       stringify!($gt), array.data_type())
                            });
                        let values = substring(dict.values(), start, length)?;
                        let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                        Ok(Arc::new(result))
                    },
                )*
                    t => panic!("Unsupported dictionary key type: {}", t)
            }
        }
    }

    match array.data_type() {
        DataType::Dictionary(kt, _) => {
            substring_dict!(
                kt,
                Int8: Int8Type,
                Int16: Int16Type,
                Int32: Int32Type,
                Int64: Int64Type,
                UInt8: UInt8Type,
                UInt16: UInt16Type,
                UInt32: UInt32Type,
                UInt64: UInt64Type
            )
        }
        DataType::LargeBinary => byte_substring(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("A large binary is expected"),
            start,
            length.map(|e| e as i64),
        ),
        DataType::Binary => byte_substring(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("A binary is expected"),
            start as i32,
            length.map(|e| e as i32),
        ),
        DataType::FixedSizeBinary(old_len) => fixed_size_binary_substring(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("a fixed size binary is expected"),
            *old_len,
            start as i32,
            length.map(|e| e as i32),
        ),
        DataType::LargeUtf8 => byte_substring(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            length.map(|e| e as i64),
        ),
        DataType::Utf8 => byte_substring(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start as i32,
            length.map(|e| e as i32),
        ),
        _ => Err(ArrowError::ComputeError(format!(
            "substring does not support type {:?}",
            array.data_type()
        ))),
    }
}

/// Substrings based on character index
///
/// # Arguments
/// * `array` - The input string array
///
/// * `start` - The start index of all substrings.
///   If `start >= 0`, then count from the start of the string,
///   otherwise count from the end of the string.
///
/// * `length`(option) - The length of all substrings.
///   If `length` is `None`, then the substring is from `start` to the end of the string.
///
/// Attention: Both `start` and `length` are counted by char.
///
/// # Performance
///
/// This function is slower than [substring]. Theoretically, the time complexity
/// is `O(n)` where `n` is the length of the value buffer. It is recommended to
/// use [substring] if the input array only contains ASCII chars.
///
/// # Basic usage
/// ```
/// # use arrow_array::StringArray;
/// # use arrow_string::substring::substring_by_char;
/// let array = StringArray::from(vec![Some("arrow"), None, Some("Γ ⊢x:T")]);
/// let result = substring_by_char(&array, 1, Some(4)).unwrap();
/// assert_eq!(result, StringArray::from(vec![Some("rrow"), None, Some(" ⊢x:")]));
/// ```
pub fn substring_by_char<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    start: i64,
    length: Option<u64>,
) -> Result<GenericStringArray<OffsetSize>, ArrowError> {
    let mut vals = BufferBuilder::<u8>::new({
        let offsets = array.value_offsets();
        (offsets[array.len()] - offsets[0]).to_usize().unwrap()
    });
    let mut new_offsets = BufferBuilder::<OffsetSize>::new(array.len() + 1);
    new_offsets.append(OffsetSize::zero());
    let length = length.map(|len| len.to_usize().unwrap());

    array.iter().for_each(|val| {
        if let Some(val) = val {
            let char_count = val.chars().count();
            let start = if start >= 0 {
                start.to_usize().unwrap()
            } else {
                char_count - (-start).to_usize().unwrap().min(char_count)
            };
            let (start_offset, end_offset) = get_start_end_offset(val, start, length);
            vals.append_slice(&val.as_bytes()[start_offset..end_offset]);
        }
        new_offsets.append(OffsetSize::from_usize(vals.len()).unwrap());
    });
    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array.len(),
            None,
            array.nulls().map(|b| b.inner().sliced()),
            0,
            vec![new_offsets.finish(), vals.finish()],
            vec![],
        )
    };
    Ok(GenericStringArray::<OffsetSize>::from(data))
}

/// * `val` - string
/// * `start` - the start char index of the substring
/// * `length` - the char length of the substring
///
/// Return the `start` and `end` offset (by byte) of the substring
fn get_start_end_offset(val: &str, start: usize, length: Option<usize>) -> (usize, usize) {
    let len = val.len();
    let mut offset_char_iter = val.char_indices();
    let start_offset = offset_char_iter
        .nth(start)
        .map_or(len, |(offset, _)| offset);
    let end_offset = length.map_or(len, |length| {
        if length > 0 {
            offset_char_iter
                .nth(length - 1)
                .map_or(len, |(offset, _)| offset)
        } else {
            start_offset
        }
    });
    (start_offset, end_offset)
}

fn byte_substring<T: ByteArrayType>(
    array: &GenericByteArray<T>,
    start: T::Offset,
    length: Option<T::Offset>,
) -> Result<ArrayRef, ArrowError>
where
    <T as ByteArrayType>::Native: PartialEq,
{
    let offsets = array.value_offsets();
    let data = array.value_data();
    let zero = <T::Offset as Zero>::zero();

    // When array is [Large]StringArray, we will check whether `offset` is at a valid char boundary.
    let check_char_boundary = {
        |offset: T::Offset| {
            if !matches!(T::DATA_TYPE, DataType::Utf8 | DataType::LargeUtf8) {
                return Ok(offset);
            }
            // Safety: a StringArray must contain valid UTF8 data
            let data_str = unsafe { std::str::from_utf8_unchecked(data) };
            let offset_usize = offset.as_usize();
            if data_str.is_char_boundary(offset_usize) {
                Ok(offset)
            } else {
                Err(ArrowError::ComputeError(format!(
                    "The offset {offset_usize} is at an invalid utf-8 boundary."
                )))
            }
        }
    };

    // start and end offsets of all substrings
    let mut new_starts_ends: Vec<(T::Offset, T::Offset)> = Vec::with_capacity(array.len());
    let mut new_offsets: Vec<T::Offset> = Vec::with_capacity(array.len() + 1);
    let mut len_so_far = zero;
    new_offsets.push(zero);

    offsets
        .windows(2)
        .try_for_each(|pair| -> Result<(), ArrowError> {
            let new_start = match start.cmp(&zero) {
                Ordering::Greater => check_char_boundary((pair[0] + start).min(pair[1]))?,
                Ordering::Equal => pair[0],
                Ordering::Less => check_char_boundary((pair[1] + start).max(pair[0]))?,
            };
            let new_end = match length {
                Some(length) => check_char_boundary((length + new_start).min(pair[1]))?,
                None => pair[1],
            };
            len_so_far += new_end - new_start;
            new_starts_ends.push((new_start, new_end));
            new_offsets.push(len_so_far);
            Ok(())
        })?;

    // concatenate substrings into a buffer
    let mut new_values = MutableBuffer::new(new_offsets.last().unwrap().as_usize());

    new_starts_ends
        .iter()
        .map(|(start, end)| {
            let start = start.as_usize();
            let end = end.as_usize();
            &data[start..end]
        })
        .for_each(|slice| new_values.extend_from_slice(slice));

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericByteArray::<T>::DATA_TYPE,
            array.len(),
            None,
            array.nulls().map(|b| b.inner().sliced()),
            0,
            vec![Buffer::from_vec(new_offsets), new_values.into()],
            vec![],
        )
    };
    Ok(make_array(data))
}

fn fixed_size_binary_substring(
    array: &FixedSizeBinaryArray,
    old_len: i32,
    start: i32,
    length: Option<i32>,
) -> Result<ArrayRef, ArrowError> {
    let new_start = if start >= 0 {
        start.min(old_len)
    } else {
        (old_len + start).max(0)
    };
    let new_len = match length {
        Some(len) => len.min(old_len - new_start),
        None => old_len - new_start,
    };

    // build value buffer
    let num_of_elements = array.len();
    let data = array.value_data();
    let mut new_values = MutableBuffer::new(num_of_elements * (new_len as usize));
    (0..num_of_elements)
        .map(|idx| {
            let offset = array.value_offset(idx);
            (
                (offset + new_start) as usize,
                (offset + new_start + new_len) as usize,
            )
        })
        .for_each(|(start, end)| new_values.extend_from_slice(&data[start..end]));

    let array_data = unsafe {
        ArrayData::new_unchecked(
            DataType::FixedSizeBinary(new_len),
            num_of_elements,
            None,
            array.nulls().map(|b| b.inner().sliced()),
            0,
            vec![new_values.into()],
            vec![],
        )
    };

    Ok(make_array(array_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A helper macro to generate test cases.
    /// # Arguments
    /// * `input` - A vector which array can be built from.
    /// * `start` - The start index of the substring.
    /// * `len` - The length of the substring.
    /// * `result` - The expected result of substring, which is a vector that array can be built from.
    /// # Return
    /// A vector of `(input, start, len, result)`.
    ///
    /// Users can provide any number of `(start, len, result)` to generate test cases for one `input`.
    macro_rules! gen_test_cases {
        ($input:expr, $(($start:expr, $len:expr, $result:expr)), *) => {
            [
                $(
                    ($input.clone(), $start, $len, $result),
                )*
            ]
        };
    }

    /// A helper macro to test the substring functions.
    /// # Arguments
    /// * `cases` - The test cases which is a vector of `(input, start, len, result)`.
    ///   Please look at [`gen_test_cases`] to find how to generate it.
    /// * `array_ty` - The array type.
    /// * `substring_fn` - Either [`substring`] or [`substring_by_char`].
    macro_rules! do_test {
        ($cases:expr, $array_ty:ty, $substring_fn:ident) => {
            $cases
                .into_iter()
                .for_each(|(array, start, length, expected)| {
                    let array = <$array_ty>::from(array);
                    let result = $substring_fn(&array, start, length).unwrap();
                    let result = result.as_any().downcast_ref::<$array_ty>().unwrap();
                    let expected = <$array_ty>::from(expected);
                    assert_eq!(&expected, result);
                })
        };
    }

    fn with_nulls_generic_binary<O: OffsetSizeTrait>() {
        let input = vec![
            Some("hello".as_bytes()),
            None,
            Some(&[0xf8, 0xf9, 0xff, 0xfa]),
        ];
        // all-nulls array is always identical
        let base_case = gen_test_cases!(
            vec![None, None, None],
            (-1, Some(1), vec![None, None, None])
        );
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // 0 length -> Nothing
            (0, Some(0), vec![Some(&[]), None, Some(&[])]),
            // high start -> Nothing
            (1000, Some(0), vec![Some(&[]), None, Some(&[])]),
            // high negative start -> identity
            (-1000, None, input.clone()),
            // high length -> identity
            (0, Some(1000), input.clone())
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericBinaryArray<O>,
            substring
        );
    }

    #[test]
    fn with_nulls_binary() {
        with_nulls_generic_binary::<i32>()
    }

    #[test]
    fn with_nulls_large_binary() {
        with_nulls_generic_binary::<i64>()
    }

    fn without_nulls_generic_binary<O: OffsetSizeTrait>() {
        let input = vec!["hello".as_bytes(), b"", &[0xf8, 0xf9, 0xff, 0xfa]];
        // empty array is always identical
        let base_case = gen_test_cases!(
            vec!["".as_bytes(), b"", b""],
            (2, Some(1), vec!["".as_bytes(), b"", b""])
        );
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // increase start
            (1, None, vec![b"ello", b"", &[0xf9, 0xff, 0xfa]]),
            (2, None, vec![b"llo", b"", &[0xff, 0xfa]]),
            (3, None, vec![b"lo", b"", &[0xfa]]),
            (10, None, vec![b"", b"", b""]),
            // increase start negatively
            (-1, None, vec![b"o", b"", &[0xfa]]),
            (-2, None, vec![b"lo", b"", &[0xff, 0xfa]]),
            (-3, None, vec![b"llo", b"", &[0xf9, 0xff, 0xfa]]),
            (-10, None, input.clone()),
            // increase length
            (1, Some(1), vec![b"e", b"", &[0xf9]]),
            (1, Some(2), vec![b"el", b"", &[0xf9, 0xff]]),
            (1, Some(3), vec![b"ell", b"", &[0xf9, 0xff, 0xfa]]),
            (1, Some(4), vec![b"ello", b"", &[0xf9, 0xff, 0xfa]]),
            (-3, Some(1), vec![b"l", b"", &[0xf9]]),
            (-3, Some(2), vec![b"ll", b"", &[0xf9, 0xff]]),
            (-3, Some(3), vec![b"llo", b"", &[0xf9, 0xff, 0xfa]]),
            (-3, Some(4), vec![b"llo", b"", &[0xf9, 0xff, 0xfa]])
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericBinaryArray<O>,
            substring
        );
    }

    #[test]
    fn without_nulls_binary() {
        without_nulls_generic_binary::<i32>()
    }

    #[test]
    fn without_nulls_large_binary() {
        without_nulls_generic_binary::<i64>()
    }

    fn generic_binary_with_non_zero_offset<O: OffsetSizeTrait>() {
        let values = 0_u8..15;
        let offsets = &[
            O::zero(),
            O::from_usize(5).unwrap(),
            O::from_usize(10).unwrap(),
            O::from_usize(15).unwrap(),
        ];
        // set the first and third element to be valid
        let bitmap = [0b101_u8];

        let data = ArrayData::builder(GenericBinaryArray::<O>::DATA_TYPE)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from_iter(values))
            .null_bit_buffer(Some(Buffer::from(bitmap)))
            .offset(1)
            .build()
            .unwrap();
        // array is `[null, [10, 11, 12, 13, 14]]`
        let array = GenericBinaryArray::<O>::from(data);
        // result is `[null, [11, 12, 13, 14]]`
        let result = substring(&array, 1, None).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<GenericBinaryArray<O>>()
            .unwrap();
        let expected =
            GenericBinaryArray::<O>::from_opt_vec(vec![None, Some(&[11_u8, 12, 13, 14])]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn binary_with_non_zero_offset() {
        generic_binary_with_non_zero_offset::<i32>()
    }

    #[test]
    fn large_binary_with_non_zero_offset() {
        generic_binary_with_non_zero_offset::<i64>()
    }

    #[test]
    fn with_nulls_fixed_size_binary() {
        let input = vec![Some("cat".as_bytes()), None, Some(&[0xf8, 0xf9, 0xff])];
        // all-nulls array is always identical
        let base_case =
            gen_test_cases!(vec![None, None, None], (3, Some(2), vec![None, None, None]));
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // increase start
            (1, None, vec![Some(b"at"), None, Some(&[0xf9, 0xff])]),
            (2, None, vec![Some(b"t"), None, Some(&[0xff])]),
            (3, None, vec![Some(b""), None, Some(b"")]),
            (10, None, vec![Some(b""), None, Some(b"")]),
            // increase start negatively
            (-1, None, vec![Some(b"t"), None, Some(&[0xff])]),
            (-2, None, vec![Some(b"at"), None, Some(&[0xf9, 0xff])]),
            (-3, None, input.clone()),
            (-10, None, input.clone()),
            // increase length
            (1, Some(1), vec![Some(b"a"), None, Some(&[0xf9])]),
            (1, Some(2), vec![Some(b"at"), None, Some(&[0xf9, 0xff])]),
            (1, Some(3), vec![Some(b"at"), None, Some(&[0xf9, 0xff])]),
            (-3, Some(1), vec![Some(b"c"), None, Some(&[0xf8])]),
            (-3, Some(2), vec![Some(b"ca"), None, Some(&[0xf8, 0xf9])]),
            (-3, Some(3), input.clone()),
            (-3, Some(4), input.clone())
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            FixedSizeBinaryArray,
            substring
        );
    }

    #[test]
    fn without_nulls_fixed_size_binary() {
        let input = vec!["cat".as_bytes(), b"dog", &[0xf8, 0xf9, 0xff]];
        // empty array is always identical
        let base_case = gen_test_cases!(
            vec!["".as_bytes(), &[], &[]],
            (1, Some(2), vec!["".as_bytes(), &[], &[]])
        );
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // increase start
            (1, None, vec![b"at", b"og", &[0xf9, 0xff]]),
            (2, None, vec![b"t", b"g", &[0xff]]),
            (3, None, vec![&[], &[], &[]]),
            (10, None, vec![&[], &[], &[]]),
            // increase start negatively
            (-1, None, vec![b"t", b"g", &[0xff]]),
            (-2, None, vec![b"at", b"og", &[0xf9, 0xff]]),
            (-3, None, input.clone()),
            (-10, None, input.clone()),
            // increase length
            (1, Some(1), vec![b"a", b"o", &[0xf9]]),
            (1, Some(2), vec![b"at", b"og", &[0xf9, 0xff]]),
            (1, Some(3), vec![b"at", b"og", &[0xf9, 0xff]]),
            (-3, Some(1), vec![b"c", b"d", &[0xf8]]),
            (-3, Some(2), vec![b"ca", b"do", &[0xf8, 0xf9]]),
            (-3, Some(3), input.clone()),
            (-3, Some(4), input.clone())
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            FixedSizeBinaryArray,
            substring
        );
    }

    #[test]
    fn fixed_size_binary_with_non_zero_offset() {
        let values: [u8; 15] = *b"hellotherearrow";
        // set the first and third element to be valid
        let bits_v = [0b101_u8];

        let data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .offset(1)
            .null_bit_buffer(Some(Buffer::from(bits_v)))
            .build()
            .unwrap();
        // array is `[null, "arrow"]`
        let array = FixedSizeBinaryArray::from(data);
        // result is `[null, "rrow"]`
        let result = substring(&array, 1, None).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let expected = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![None, Some(b"rrow")].into_iter(),
            4,
        )
        .unwrap();
        assert_eq!(result, &expected);
    }

    fn with_nulls_generic_string<O: OffsetSizeTrait>() {
        let input = vec![Some("hello"), None, Some("word")];
        // all-nulls array is always identical
        let base_case = gen_test_cases!(vec![None, None, None], (0, None, vec![None, None, None]));
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // 0 length -> Nothing
            (0, Some(0), vec![Some(""), None, Some("")]),
            // high start -> Nothing
            (1000, Some(0), vec![Some(""), None, Some("")]),
            // high negative start -> identity
            (-1000, None, input.clone()),
            // high length -> identity
            (0, Some(1000), input.clone())
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericStringArray<O>,
            substring
        );
    }

    #[test]
    fn with_nulls_string() {
        with_nulls_generic_string::<i32>()
    }

    #[test]
    fn with_nulls_large_string() {
        with_nulls_generic_string::<i64>()
    }

    fn without_nulls_generic_string<O: OffsetSizeTrait>() {
        let input = vec!["hello", "", "word"];
        // empty array is always identical
        let base_case = gen_test_cases!(vec!["", "", ""], (0, None, vec!["", "", ""]));
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            (1, None, vec!["ello", "", "ord"]),
            (2, None, vec!["llo", "", "rd"]),
            (3, None, vec!["lo", "", "d"]),
            (10, None, vec!["", "", ""]),
            // increase start negatively
            (-1, None, vec!["o", "", "d"]),
            (-2, None, vec!["lo", "", "rd"]),
            (-3, None, vec!["llo", "", "ord"]),
            (-10, None, input.clone()),
            // increase length
            (1, Some(1), vec!["e", "", "o"]),
            (1, Some(2), vec!["el", "", "or"]),
            (1, Some(3), vec!["ell", "", "ord"]),
            (1, Some(4), vec!["ello", "", "ord"]),
            (-3, Some(1), vec!["l", "", "o"]),
            (-3, Some(2), vec!["ll", "", "or"]),
            (-3, Some(3), vec!["llo", "", "ord"]),
            (-3, Some(4), vec!["llo", "", "ord"])
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericStringArray<O>,
            substring
        );
    }

    #[test]
    fn without_nulls_string() {
        without_nulls_generic_string::<i32>()
    }

    #[test]
    fn without_nulls_large_string() {
        without_nulls_generic_string::<i64>()
    }

    fn generic_string_with_non_zero_offset<O: OffsetSizeTrait>() {
        let values = b"hellotherearrow";
        let offsets = &[
            O::zero(),
            O::from_usize(5).unwrap(),
            O::from_usize(10).unwrap(),
            O::from_usize(15).unwrap(),
        ];
        // set the first and third element to be valid
        let bitmap = [0b101_u8];

        let data = ArrayData::builder(GenericStringArray::<O>::DATA_TYPE)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from(values))
            .null_bit_buffer(Some(Buffer::from(bitmap)))
            .offset(1)
            .build()
            .unwrap();
        // array is `[null, "arrow"]`
        let array = GenericStringArray::<O>::from(data);
        // result is `[null, "rrow"]`
        let result = substring(&array, 1, None).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .unwrap();
        let expected = GenericStringArray::<O>::from(vec![None, Some("rrow")]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn string_with_non_zero_offset() {
        generic_string_with_non_zero_offset::<i32>()
    }

    #[test]
    fn large_string_with_non_zero_offset() {
        generic_string_with_non_zero_offset::<i64>()
    }

    fn with_nulls_generic_string_by_char<O: OffsetSizeTrait>() {
        let input = vec![Some("hello"), None, Some("Γ ⊢x:T")];
        // all-nulls array is always identical
        let base_case = gen_test_cases!(vec![None, None, None], (0, None, vec![None, None, None]));
        let cases = gen_test_cases!(
            input,
            // identity
            (0, None, input.clone()),
            // 0 length -> Nothing
            (0, Some(0), vec![Some(""), None, Some("")]),
            // high start -> Nothing
            (1000, Some(0), vec![Some(""), None, Some("")]),
            // high negative start -> identity
            (-1000, None, input.clone()),
            // high length -> identity
            (0, Some(1000), input.clone())
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericStringArray<O>,
            substring_by_char
        );
    }

    #[test]
    fn with_nulls_string_by_char() {
        with_nulls_generic_string_by_char::<i32>()
    }

    #[test]
    fn with_nulls_large_string_by_char() {
        with_nulls_generic_string_by_char::<i64>()
    }

    fn without_nulls_generic_string_by_char<O: OffsetSizeTrait>() {
        let input = vec!["hello", "", "Γ ⊢x:T"];
        // empty array is always identical
        let base_case = gen_test_cases!(vec!["", "", ""], (0, None, vec!["", "", ""]));
        let cases = gen_test_cases!(
            input,
            //identity
            (0, None, input.clone()),
            // increase start
            (1, None, vec!["ello", "", " ⊢x:T"]),
            (2, None, vec!["llo", "", "⊢x:T"]),
            (3, None, vec!["lo", "", "x:T"]),
            (10, None, vec!["", "", ""]),
            // increase start negatively
            (-1, None, vec!["o", "", "T"]),
            (-2, None, vec!["lo", "", ":T"]),
            (-4, None, vec!["ello", "", "⊢x:T"]),
            (-10, None, input.clone()),
            // increase length
            (1, Some(1), vec!["e", "", " "]),
            (1, Some(2), vec!["el", "", " ⊢"]),
            (1, Some(3), vec!["ell", "", " ⊢x"]),
            (1, Some(6), vec!["ello", "", " ⊢x:T"]),
            (-4, Some(1), vec!["e", "", "⊢"]),
            (-4, Some(2), vec!["el", "", "⊢x"]),
            (-4, Some(3), vec!["ell", "", "⊢x:"]),
            (-4, Some(4), vec!["ello", "", "⊢x:T"])
        );

        do_test!(
            [&base_case[..], &cases[..]].concat(),
            GenericStringArray<O>,
            substring_by_char
        );
    }

    #[test]
    fn without_nulls_string_by_char() {
        without_nulls_generic_string_by_char::<i32>()
    }

    #[test]
    fn without_nulls_large_string_by_char() {
        without_nulls_generic_string_by_char::<i64>()
    }

    fn generic_string_by_char_with_non_zero_offset<O: OffsetSizeTrait>() {
        let values = "S→T = Πx:S.T";
        let offsets = &[
            O::zero(),
            O::from_usize(values.char_indices().nth(3).map(|(pos, _)| pos).unwrap()).unwrap(),
            O::from_usize(values.char_indices().nth(6).map(|(pos, _)| pos).unwrap()).unwrap(),
            O::from_usize(values.len()).unwrap(),
        ];
        // set the first and third element to be valid
        let bitmap = [0b101_u8];

        let data = ArrayData::builder(GenericStringArray::<O>::DATA_TYPE)
            .len(2)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_buffer(Buffer::from(values.as_bytes()))
            .null_bit_buffer(Some(Buffer::from(bitmap)))
            .offset(1)
            .build()
            .unwrap();
        // array is `[null, "Πx:S.T"]`
        let array = GenericStringArray::<O>::from(data);
        // result is `[null, "x:S.T"]`
        let result = substring_by_char(&array, 1, None).unwrap();
        let expected = GenericStringArray::<O>::from(vec![None, Some("x:S.T")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn string_with_non_zero_offset_by_char() {
        generic_string_by_char_with_non_zero_offset::<i32>()
    }

    #[test]
    fn large_string_with_non_zero_offset_by_char() {
        generic_string_by_char_with_non_zero_offset::<i64>()
    }

    #[test]
    fn dictionary() {
        _dictionary::<Int8Type>();
        _dictionary::<Int16Type>();
        _dictionary::<Int32Type>();
        _dictionary::<Int64Type>();
        _dictionary::<UInt8Type>();
        _dictionary::<UInt16Type>();
        _dictionary::<UInt32Type>();
        _dictionary::<UInt64Type>();
    }

    fn _dictionary<K: ArrowDictionaryKeyType>() {
        const TOTAL: i32 = 100;

        let v = ["aaa", "bbb", "ccc", "ddd", "eee"];
        let data: Vec<Option<&str>> = (0..TOTAL)
            .map(|n| {
                let i = n % 5;
                if i == 3 {
                    None
                } else {
                    Some(v[i as usize])
                }
            })
            .collect();

        let dict_array: DictionaryArray<K> = data.clone().into_iter().collect();

        let expected: Vec<Option<&str>> = data.iter().map(|opt| opt.map(|s| &s[1..3])).collect();

        let res = substring(&dict_array, 1, Some(2)).unwrap();
        let actual = res.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        let actual: Vec<Option<&str>> = actual
            .values()
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap()
            .take_iter(actual.keys_iter())
            .collect();

        for i in 0..TOTAL as usize {
            assert_eq!(expected[i], actual[i],);
        }
    }

    #[test]
    fn check_invalid_array_type() {
        let array = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let err = substring(&array, 0, None).unwrap_err().to_string();
        assert!(err.contains("substring does not support type"));
    }

    // tests for the utf-8 validation checking
    #[test]
    fn check_start_index() {
        let array = StringArray::from(vec![Some("E=mc²"), Some("ascii")]);
        let err = substring(&array, -1, None).unwrap_err().to_string();
        assert!(err.contains("invalid utf-8 boundary"));
    }

    #[test]
    fn check_length() {
        let array = StringArray::from(vec![Some("E=mc²"), Some("ascii")]);
        let err = substring(&array, 0, Some(5)).unwrap_err().to_string();
        assert!(err.contains("invalid utf-8 boundary"));
    }

    #[test]
    fn non_utf8_bytes() {
        // non-utf8 bytes
        let bytes: &[u8] = &[0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD, 0xE8, 0xAF, 0xAD];
        let array = BinaryArray::from(vec![Some(bytes)]);
        let arr = substring(&array, 0, Some(5)).unwrap();
        let actual = arr.as_any().downcast_ref::<BinaryArray>().unwrap();

        let expected_bytes: &[u8] = &[0xE4, 0xBD, 0xA0, 0xE5, 0xA5];
        let expected = BinaryArray::from(vec![Some(expected_bytes)]);
        assert_eq!(expected, *actual);
    }
}
