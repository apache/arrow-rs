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

//! Defines kernel for length of string arrays and binary arrays

use arrow_array::*;
use arrow_array::{cast::AsArray, types::*};
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

fn length_impl<P: ArrowPrimitiveType>(
    offsets: &OffsetBuffer<P::Native>,
    nulls: Option<&NullBuffer>,
) -> ArrayRef {
    let v: Vec<_> = offsets
        .windows(2)
        .map(|w| w[1].sub_wrapping(w[0]))
        .collect();
    Arc::new(PrimitiveArray::<P>::new(v.into(), nulls.cloned()))
}

fn bit_length_impl<P: ArrowPrimitiveType>(
    offsets: &OffsetBuffer<P::Native>,
    nulls: Option<&NullBuffer>,
) -> ArrayRef {
    let bits = P::Native::usize_as(8);
    let c = |w: &[P::Native]| w[1].sub_wrapping(w[0]).mul_wrapping(bits);
    let v: Vec<_> = offsets.windows(2).map(c).collect();
    Arc::new(PrimitiveArray::<P>::new(v.into(), nulls.cloned()))
}

/// Returns an array of Int32/Int64 denoting the length of each value in the array.
///
/// For list array, length is the number of elements in each list.
/// For string array and binary array, length is the number of bytes of each value.
///
/// * this only accepts ListArray/LargeListArray, StringArray/LargeStringArray/StringViewArray, BinaryArray/LargeBinaryArray, and FixedSizeListArray,
///   or DictionaryArray with above Arrays as values
/// * length of null is null.
pub fn length(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    if let Some(d) = array.as_any_dictionary_opt() {
        let lengths = length(d.values().as_ref())?;
        return Ok(d.with_values(lengths));
    }

    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            Ok(length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            Ok(length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::Utf8 => {
            let list = array.as_string::<i32>();
            Ok(length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeUtf8 => {
            let list = array.as_string::<i64>();
            Ok(length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::Utf8View => {
            let list = array.as_string_view();
            let v = list.views().iter().map(|v| *v as i32).collect::<Vec<_>>();
            Ok(Arc::new(PrimitiveArray::<Int32Type>::new(
                v.into(),
                list.nulls().cloned(),
            )))
        }
        DataType::Binary => {
            let list = array.as_binary::<i32>();
            Ok(length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeBinary => {
            let list = array.as_binary::<i64>();
            Ok(length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::FixedSizeBinary(len) | DataType::FixedSizeList(_, len) => Ok(Arc::new(
            Int32Array::new(vec![*len; array.len()].into(), array.nulls().cloned()),
        )),
        DataType::BinaryView => {
            let list = array.as_binary_view();
            let v = list.views().iter().map(|v| *v as i32).collect::<Vec<_>>();
            Ok(Arc::new(PrimitiveArray::<Int32Type>::new(
                v.into(),
                list.nulls().cloned(),
            )))
        }
        other => Err(ArrowError::ComputeError(format!(
            "length not supported for {other:?}"
        ))),
    }
}

/// Returns an array of Int32/Int64 denoting the number of bits in each value in the array.
///
/// * this only accepts StringArray/Utf8, LargeString/LargeUtf8, BinaryArray and LargeBinaryArray,
///   or DictionaryArray with above Arrays as values
/// * bit_length of null is null.
/// * bit_length is in number of bits
pub fn bit_length(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    if let Some(d) = array.as_any_dictionary_opt() {
        let lengths = bit_length(d.values().as_ref())?;
        return Ok(d.with_values(lengths));
    }

    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            Ok(bit_length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            Ok(bit_length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::Utf8 => {
            let list = array.as_string::<i32>();
            Ok(bit_length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeUtf8 => {
            let list = array.as_string::<i64>();
            Ok(bit_length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::Utf8View => {
            let list = array.as_string_view();
            let values = list
                .views()
                .iter()
                .map(|view| (*view as i32).wrapping_mul(8))
                .collect();
            Ok(Arc::new(Int32Array::new(values, array.nulls().cloned())))
        }
        DataType::Binary => {
            let list = array.as_binary::<i32>();
            Ok(bit_length_impl::<Int32Type>(list.offsets(), list.nulls()))
        }
        DataType::LargeBinary => {
            let list = array.as_binary::<i64>();
            Ok(bit_length_impl::<Int64Type>(list.offsets(), list.nulls()))
        }
        DataType::FixedSizeBinary(len) => Ok(Arc::new(Int32Array::new(
            vec![*len * 8; array.len()].into(),
            array.nulls().cloned(),
        ))),
        other => Err(ArrowError::ComputeError(format!(
            "bit_length not supported for {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;
    use arrow_schema::Field;

    fn length_cases_string() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        // a large array
        let values = [
            "one",
            "on",
            "o",
            "",
            "this is a longer string to test string array with",
        ];
        let values = values.into_iter().cycle().take(4096).collect();
        let expected = [3, 2, 1, 0, 49].into_iter().cycle().take(4096).collect();

        vec![
            (vec!["hello", " ", "world"], 3, vec![5, 1, 5]),
            (vec!["hello", " ", "world", "!"], 4, vec![5, 1, 5, 1]),
            (vec!["💖"], 1, vec![4]),
            (values, 4096, expected),
        ]
    }

    macro_rules! length_binary_helper {
        ($offset_ty: ty, $result_ty: ty, $kernel: ident, $value: expr, $expected: expr) => {{
            let array = GenericBinaryArray::<$offset_ty>::from($value);
            let result = $kernel(&array).unwrap();
            let result = result.as_any().downcast_ref::<$result_ty>().unwrap();
            let expected: $result_ty = $expected.into();
            assert_eq!(&expected, result);
        }};
    }

    macro_rules! length_list_helper {
        ($offset_ty: ty, $result_ty: ty, $element_ty: ty, $value: expr, $expected: expr) => {{
            let array =
                GenericListArray::<$offset_ty>::from_iter_primitive::<$element_ty, _, _>($value);
            let result = length(&array).unwrap();
            let result = result.as_any().downcast_ref::<$result_ty>().unwrap();
            let expected: $result_ty = $expected.into();
            assert_eq!(&expected, result);
        }};
    }

    #[test]
    fn length_test_string() {
        length_cases_string()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
            })
    }

    #[test]
    fn length_test_large_string() {
        length_cases_string()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value as i64, result.value(i));
                });
            })
    }

    #[test]
    fn length_test_string_view() {
        length_cases_string()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringViewArray::from(input);
                let result = length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
            })
    }

    #[test]
    fn length_test_binary() {
        let value: Vec<&[u8]> = vec![b"zero", b"one", &[0xff, 0xf8]];
        let result: Vec<i32> = vec![4, 3, 2];
        length_binary_helper!(i32, Int32Array, length, value, result)
    }

    #[test]
    fn length_test_large_binary() {
        let value: Vec<&[u8]> = vec![b"zero", &[0xff, 0xf8], b"two"];
        let result: Vec<i64> = vec![4, 2, 3];
        length_binary_helper!(i64, Int64Array, length, value, result)
    }

    #[test]
    fn length_test_binary_view() {
        let value: Vec<&[u8]> = vec![
            b"zero",
            &[0xff, 0xf8],
            b"two",
            b"this is a longer string to test binary array with",
        ];
        let expected: Vec<i32> = vec![4, 2, 3, 49];

        let array = BinaryViewArray::from(value);
        let result = length(&array).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected: Int32Array = expected.into();
        assert_eq!(&expected, result);
    }

    #[test]
    fn length_test_list() {
        let value = vec![
            Some(vec![]),
            Some(vec![Some(1), Some(2), Some(4)]),
            Some(vec![Some(0)]),
        ];
        let result: Vec<i32> = vec![0, 3, 1];
        length_list_helper!(i32, Int32Array, Int32Type, value, result)
    }

    #[test]
    fn length_test_large_list() {
        let value = vec![
            Some(vec![]),
            Some(vec![Some(1.1), Some(2.2), Some(3.3)]),
            Some(vec![None]),
        ];
        let result: Vec<i64> = vec![0, 3, 1];
        length_list_helper!(i64, Int64Array, Float32Type, value, result)
    }

    type OptionStr = Option<&'static str>;

    fn length_null_cases_string() -> Vec<(Vec<OptionStr>, usize, Vec<Option<i32>>)> {
        vec![(
            vec![Some("one"), None, Some("three"), Some("four")],
            4,
            vec![Some(3), None, Some(5), Some(4)],
        )]
    }

    #[test]
    fn length_null_string() {
        length_null_cases_string()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(&expected, result);
            })
    }

    #[test]
    fn length_null_large_string() {
        length_null_cases_string()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

                // convert to i64
                let expected: Int64Array = expected
                    .iter()
                    .map(|e| e.map(|e| e as i64))
                    .collect::<Vec<_>>()
                    .into();
                assert_eq!(&expected, result);
            })
    }

    #[test]
    fn length_null_binary() {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"zero"), None, Some(&[0xff, 0xf8]), Some(b"three")];
        let result: Vec<Option<i32>> = vec![Some(4), None, Some(2), Some(5)];
        length_binary_helper!(i32, Int32Array, length, value, result)
    }

    #[test]
    fn length_null_large_binary() {
        let value: Vec<Option<&[u8]>> =
            vec![Some(&[0xff, 0xf8]), None, Some(b"two"), Some(b"three")];
        let result: Vec<Option<i64>> = vec![Some(2), None, Some(3), Some(5)];
        length_binary_helper!(i64, Int64Array, length, value, result)
    }

    #[test]
    fn length_null_list() {
        let value = vec![
            Some(vec![]),
            None,
            Some(vec![Some(1), None, Some(2), Some(4)]),
            Some(vec![Some(0)]),
        ];
        let result: Vec<Option<i32>> = vec![Some(0), None, Some(4), Some(1)];
        length_list_helper!(i32, Int32Array, Int8Type, value, result)
    }

    #[test]
    fn length_null_large_list() {
        let value = vec![
            Some(vec![]),
            None,
            Some(vec![Some(1.1), None, Some(4.0)]),
            Some(vec![Some(0.1)]),
        ];
        let result: Vec<Option<i64>> = vec![Some(0), None, Some(3), Some(1)];
        length_list_helper!(i64, Int64Array, Float32Type, value, result)
    }

    /// Tests that length is not valid for u64.
    #[test]
    fn length_wrong_type() {
        let array: UInt64Array = vec![1u64].into();

        assert!(length(&array).is_err());
    }

    /// Tests with an offset
    #[test]
    fn length_offsets_string() {
        let a = StringArray::from(vec![Some("hello"), Some(" "), Some("world"), None]);
        let b = a.slice(1, 3);
        let result = length(&b).unwrap();
        let result: &Int32Array = result.as_primitive();

        let expected = Int32Array::from(vec![Some(1), Some(5), None]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn length_offsets_binary() {
        let value: Vec<Option<&[u8]>> = vec![Some(b"hello"), Some(b" "), Some(&[0xff, 0xf8]), None];
        let a = BinaryArray::from(value);
        let b = a.slice(1, 3);
        let result = length(&b).unwrap();
        let result: &Int32Array = result.as_primitive();

        let expected = Int32Array::from(vec![Some(1), Some(2), None]);
        assert_eq!(&expected, result);
    }

    fn bit_length_cases() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        // a large array
        let values = ["one", "on", "o", ""];
        let values = values.into_iter().cycle().take(4096).collect();
        let expected = [24, 16, 8, 0].into_iter().cycle().take(4096).collect();

        vec![
            (vec!["hello", " ", "world", "!"], 4, vec![40, 8, 40, 8]),
            (vec!["💖"], 1, vec![32]),
            (vec!["josé"], 1, vec![40]),
            (values, 4096, expected),
        ]
    }

    #[test]
    fn bit_length_test_string() {
        bit_length_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
            })
    }

    #[test]
    fn bit_length_test_large_string() {
        bit_length_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value as i64, result.value(i));
                });
            })
    }

    #[test]
    fn bit_length_test_utf8view() {
        bit_length_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let string_array = StringViewArray::from(input);
                let result = bit_length(&string_array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
            })
    }

    #[test]
    fn bit_length_null_utf8view() {
        bit_length_null_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(&expected, result);
            })
    }
    #[test]
    fn bit_length_binary() {
        let value: Vec<&[u8]> = vec![b"one", &[0xff, 0xf8], b"three"];
        let expected: Vec<i32> = vec![24, 16, 40];
        length_binary_helper!(i32, Int32Array, bit_length, value, expected)
    }

    #[test]
    fn bit_length_large_binary() {
        let value: Vec<&[u8]> = vec![b"zero", b" ", &[0xff, 0xf8]];
        let expected: Vec<i64> = vec![32, 8, 16];
        length_binary_helper!(i64, Int64Array, bit_length, value, expected)
    }

    fn bit_length_null_cases() -> Vec<(Vec<OptionStr>, usize, Vec<Option<i32>>)> {
        vec![(
            vec![Some("one"), None, Some("three"), Some("four")],
            4,
            vec![Some(24), None, Some(40), Some(32)],
        )]
    }

    #[test]
    fn bit_length_null_string() {
        bit_length_null_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(&expected, result);
            })
    }

    #[test]
    fn bit_length_null_large_string() {
        bit_length_null_cases()
            .into_iter()
            .for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array).unwrap();
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

                // convert to i64
                let expected: Int64Array = expected
                    .iter()
                    .map(|e| e.map(|e| e as i64))
                    .collect::<Vec<_>>()
                    .into();
                assert_eq!(&expected, result);
            })
    }

    #[test]
    fn bit_length_null_binary() {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"one"), None, Some(b"three"), Some(&[0xff, 0xf8])];
        let expected: Vec<Option<i32>> = vec![Some(24), None, Some(40), Some(16)];
        length_binary_helper!(i32, Int32Array, bit_length, value, expected)
    }

    #[test]
    fn bit_length_null_large_binary() {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"one"), None, Some(&[0xff, 0xf8]), Some(b"four")];
        let expected: Vec<Option<i64>> = vec![Some(24), None, Some(16), Some(32)];
        length_binary_helper!(i64, Int64Array, bit_length, value, expected)
    }

    /// Tests that bit_length is not valid for u64.
    #[test]
    fn bit_length_wrong_type() {
        let array: UInt64Array = vec![1u64].into();

        assert!(bit_length(&array).is_err());
    }

    /// Tests with an offset
    #[test]
    fn bit_length_offsets_string() {
        let a = StringArray::from(vec![Some("hello"), Some(" "), Some("world"), None]);
        let b = a.slice(1, 3);
        let result = bit_length(&b).unwrap();
        let result: &Int32Array = result.as_primitive();

        let expected = Int32Array::from(vec![Some(8), Some(40), None]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn bit_length_offsets_binary() {
        let value: Vec<Option<&[u8]>> = vec![Some(b"hello"), Some(&[]), Some(b"world"), None];
        let a = BinaryArray::from(value);
        let b = a.slice(1, 3);
        let result = bit_length(&b).unwrap();
        let result: &Int32Array = result.as_primitive();

        let expected = Int32Array::from(vec![Some(0), Some(40), None]);
        assert_eq!(&expected, result);
    }

    #[test]
    fn length_dictionary() {
        _length_dictionary::<Int8Type>();
        _length_dictionary::<Int16Type>();
        _length_dictionary::<Int32Type>();
        _length_dictionary::<Int64Type>();
        _length_dictionary::<UInt8Type>();
        _length_dictionary::<UInt16Type>();
        _length_dictionary::<UInt32Type>();
        _length_dictionary::<UInt64Type>();
    }

    fn _length_dictionary<K: ArrowDictionaryKeyType>() {
        const TOTAL: i32 = 100;

        let v = ["aaaa", "bb", "ccccc", "ddd", "eeeeee"];
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

        let expected: Vec<Option<i32>> =
            data.iter().map(|opt| opt.map(|s| s.len() as i32)).collect();

        let res = length(&dict_array).unwrap();
        let actual = res.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        let actual: Vec<Option<i32>> = actual
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .take_iter(dict_array.keys_iter())
            .collect();

        for i in 0..TOTAL as usize {
            assert_eq!(expected[i], actual[i],);
        }
    }

    #[test]
    fn bit_length_dictionary() {
        _bit_length_dictionary::<Int8Type>();
        _bit_length_dictionary::<Int16Type>();
        _bit_length_dictionary::<Int32Type>();
        _bit_length_dictionary::<Int64Type>();
        _bit_length_dictionary::<UInt8Type>();
        _bit_length_dictionary::<UInt16Type>();
        _bit_length_dictionary::<UInt32Type>();
        _bit_length_dictionary::<UInt64Type>();
    }

    fn _bit_length_dictionary<K: ArrowDictionaryKeyType>() {
        const TOTAL: i32 = 100;

        let v = ["aaaa", "bb", "ccccc", "ddd", "eeeeee"];
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

        let expected: Vec<Option<i32>> = data
            .iter()
            .map(|opt| opt.map(|s| (s.chars().count() * 8) as i32))
            .collect();

        let res = bit_length(&dict_array).unwrap();
        let actual = res.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        let actual: Vec<Option<i32>> = actual
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .take_iter(dict_array.keys_iter())
            .collect();

        for i in 0..TOTAL as usize {
            assert_eq!(expected[i], actual[i],);
        }
    }

    #[test]
    fn test_fixed_size_list_length() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8]))
            .build()
            .unwrap();
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .nulls(Some(nulls))
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let lengths = length(&list_array).unwrap();
        let lengths = lengths.as_primitive::<Int32Type>();

        assert_eq!(lengths.len(), 3);
        assert_eq!(lengths.value(0), 3);
        assert!(lengths.is_null(1));
        assert_eq!(lengths.value(2), 3);
    }

    #[test]
    fn test_fixed_size_binary() {
        let array = FixedSizeBinaryArray::new(4, [0; 16].into(), None);
        let result = length(&array).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![4; 4]));

        let result = bit_length(&array).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![32; 4]));
    }
}
