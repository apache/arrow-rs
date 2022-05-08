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

use crate::{array::*, buffer::Buffer, datatypes::ArrowPrimitiveType};
use crate::{
    datatypes::*,
    error::{ArrowError, Result},
};

use std::sync::Arc;

macro_rules! unary_offsets {
    ($array: expr, $data_type: expr, $op: expr) => {{
        let slice = $array.value_offsets();

        let lengths = slice.windows(2).map(|offset| $op(offset[1] - offset[0]));

        // JUSTIFICATION
        //  Benefit
        //      ~60% speedup
        //  Soundness
        //      `values` come from a slice iterator with a known size.
        let buffer = unsafe { Buffer::from_trusted_len_iter(lengths) };

        let null_bit_buffer = $array
            .data_ref()
            .null_buffer()
            .map(|b| b.bit_slice($array.offset(), $array.len()));

        let data = unsafe {
            ArrayData::new_unchecked(
                $data_type,
                $array.len(),
                None,
                null_bit_buffer,
                0,
                vec![buffer],
                vec![],
            )
        };
        make_array(data)
    }};
}

macro_rules! kernel_dict {
    ($array: ident, $kernel: expr, $kt: ident, $($t: ident: $gt: ident), *) => {
        match $kt.as_ref() {
            $(&DataType::$t => {
                let dict = $array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$gt>>()
                    .unwrap_or_else(|| {
                        panic!("Expect 'DictionaryArray<{}>' but got array of data type {:?}",
                            stringify!($gt), $array.data_type())
                    });
                let values = $kernel(dict.values())?;
                let result = DictionaryArray::try_new(dict.keys(), &values)?;
                    Ok(Arc::new(result))
                },
            )*
            t => panic!("Unsupported dictionary key type: {}", t)
        }
    }
}

fn length_list<O, T>(array: &dyn Array) -> ArrayRef
where
    O: OffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .unwrap();
    unary_offsets!(array, T::DATA_TYPE, |x| x)
}

fn length_binary<O, T>(array: &dyn Array) -> ArrayRef
where
    O: BinaryOffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: BinaryOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericBinaryArray<O>>()
        .unwrap();
    unary_offsets!(array, T::DATA_TYPE, |x| x)
}

fn length_string<O, T>(array: &dyn Array) -> ArrayRef
where
    O: StringOffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: StringOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap();
    unary_offsets!(array, T::DATA_TYPE, |x| x)
}

fn bit_length_binary<O, T>(array: &dyn Array) -> ArrayRef
where
    O: BinaryOffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: BinaryOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericBinaryArray<O>>()
        .unwrap();
    let bits_in_bytes = O::from_usize(8).unwrap();
    unary_offsets!(array, T::DATA_TYPE, |x| x * bits_in_bytes)
}

fn bit_length_string<O, T>(array: &dyn Array) -> ArrayRef
where
    O: StringOffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: StringOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap();
    let bits_in_bytes = O::from_usize(8).unwrap();
    unary_offsets!(array, T::DATA_TYPE, |x| x * bits_in_bytes)
}

/// Returns an array of Int32/Int64 denoting the length of each value in the array.
/// For list array, length is the number of elements in each list.
/// For string array and binary array, length is the number of bytes of each value.
///
/// * this only accepts ListArray/LargeListArray, StringArray/LargeStringArray andBinaryArray/LargeBinaryArray,
///   or DictionaryArray with above Arrays as values
/// * length of null is null.
pub fn length(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(kt, _) => {
            kernel_dict!(
                array,
                |a| { length(a) },
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
        DataType::List(_) => Ok(length_list::<i32, Int32Type>(array)),
        DataType::LargeList(_) => Ok(length_list::<i64, Int64Type>(array)),
        DataType::Utf8 => Ok(length_string::<i32, Int32Type>(array)),
        DataType::LargeUtf8 => Ok(length_string::<i64, Int64Type>(array)),
        DataType::Binary => Ok(length_binary::<i32, Int32Type>(array)),
        DataType::LargeBinary => Ok(length_binary::<i64, Int64Type>(array)),
        other => Err(ArrowError::ComputeError(format!(
            "length not supported for {:?}",
            other
        ))),
    }
}

/// Returns an array of Int32/Int64 denoting the number of bits in each value in the array.
///
/// * this only accepts StringArray/Utf8, LargeString/LargeUtf8, BinaryArray and LargeBinaryArray,
///   or DictionaryArray with above Arrays as values
/// * bit_length of null is null.
/// * bit_length is in number of bits
pub fn bit_length(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(kt, _) => {
            kernel_dict!(
                array,
                |a| { bit_length(a) },
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
        DataType::Utf8 => Ok(bit_length_string::<i32, Int32Type>(array)),
        DataType::LargeUtf8 => Ok(bit_length_string::<i64, Int64Type>(array)),
        DataType::Binary => Ok(bit_length_binary::<i32, Int32Type>(array)),
        DataType::LargeBinary => Ok(bit_length_binary::<i64, Int64Type>(array)),
        other => Err(ArrowError::ComputeError(format!(
            "bit_length not supported for {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
        [&v[..], &v[..]].concat()
    }

    fn length_cases_string() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        // a large array
        let mut values = vec!["one", "on", "o", ""];
        let mut expected = vec![3, 2, 1, 0];
        for _ in 0..10 {
            values = double_vec(values);
            expected = double_vec(expected);
        }

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
            let result = $kernel(&array)?;
            let result = result.as_any().downcast_ref::<$result_ty>().unwrap();
            let expected: $result_ty = $expected.into();
            assert_eq!(expected.data(), result.data());
            Ok(())
        }};
    }

    macro_rules! length_list_helper {
        ($offset_ty: ty, $result_ty: ty, $element_ty: ty, $value: expr, $expected: expr) => {{
            let array =
                GenericListArray::<$offset_ty>::from_iter_primitive::<$element_ty, _, _>(
                    $value,
                );
            let result = length(&array)?;
            let result = result.as_any().downcast_ref::<$result_ty>().unwrap();
            let expected: $result_ty = $expected.into();
            assert_eq!(expected.data(), result.data());
            Ok(())
        }};
    }

    #[test]
    #[cfg_attr(miri, ignore)] // running forever
    fn length_test_string() -> Result<()> {
        length_cases_string()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
                Ok(())
            })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // running forever
    fn length_test_large_string() -> Result<()> {
        length_cases_string()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value as i64, result.value(i));
                });
                Ok(())
            })
    }

    #[test]
    fn length_test_binary() -> Result<()> {
        let value: Vec<&[u8]> = vec![b"zero", b"one", &[0xff, 0xf8]];
        let result: Vec<i32> = vec![4, 3, 2];
        length_binary_helper!(i32, Int32Array, length, value, result)
    }

    #[test]
    fn length_test_large_binary() -> Result<()> {
        let value: Vec<&[u8]> = vec![b"zero", &[0xff, 0xf8], b"two"];
        let result: Vec<i64> = vec![4, 2, 3];
        length_binary_helper!(i64, Int64Array, length, value, result)
    }

    #[test]
    fn length_test_list() -> Result<()> {
        let value = vec![
            Some(vec![]),
            Some(vec![Some(1), Some(2), Some(4)]),
            Some(vec![Some(0)]),
        ];
        let result: Vec<i32> = vec![0, 3, 1];
        length_list_helper!(i32, Int32Array, Int32Type, value, result)
    }

    #[test]
    fn length_test_large_list() -> Result<()> {
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
    fn length_null_string() -> Result<()> {
        length_null_cases_string()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn length_null_large_string() -> Result<()> {
        length_null_cases_string()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

                // convert to i64
                let expected: Int64Array = expected
                    .iter()
                    .map(|e| e.map(|e| e as i64))
                    .collect::<Vec<_>>()
                    .into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn length_null_binary() -> Result<()> {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"zero"), None, Some(&[0xff, 0xf8]), Some(b"three")];
        let result: Vec<Option<i32>> = vec![Some(4), None, Some(2), Some(5)];
        length_binary_helper!(i32, Int32Array, length, value, result)
    }

    #[test]
    fn length_null_large_binary() -> Result<()> {
        let value: Vec<Option<&[u8]>> =
            vec![Some(&[0xff, 0xf8]), None, Some(b"two"), Some(b"three")];
        let result: Vec<Option<i64>> = vec![Some(2), None, Some(3), Some(5)];
        length_binary_helper!(i64, Int64Array, length, value, result)
    }

    #[test]
    fn length_null_list() -> Result<()> {
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
    fn length_null_large_list() -> Result<()> {
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
    fn length_offsets_string() -> Result<()> {
        let a = StringArray::from(vec![Some("hello"), Some(" "), Some("world"), None]);
        let b = a.slice(1, 3);
        let result = length(b.as_ref())?;
        let result: &Int32Array = as_primitive_array(&result);

        let expected = Int32Array::from(vec![Some(1), Some(5), None]);
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn length_offsets_binary() -> Result<()> {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"hello"), Some(b" "), Some(&[0xff, 0xf8]), None];
        let a = BinaryArray::from(value);
        let b = a.slice(1, 3);
        let result = length(b.as_ref())?;
        let result: &Int32Array = as_primitive_array(&result);

        let expected = Int32Array::from(vec![Some(1), Some(2), None]);
        assert_eq!(&expected, result);

        Ok(())
    }

    fn bit_length_cases() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        // a large array
        let mut values = vec!["one", "on", "o", ""];
        let mut expected = vec![24, 16, 8, 0];
        for _ in 0..10 {
            values = double_vec(values);
            expected = double_vec(expected);
        }

        vec![
            (vec!["hello", " ", "world", "!"], 4, vec![40, 8, 40, 8]),
            (vec!["💖"], 1, vec![32]),
            (vec!["josé"], 1, vec![40]),
            (values, 4096, expected),
        ]
    }

    #[test]
    #[cfg_attr(miri, ignore)] // error: this test uses too much memory to run on CI
    fn bit_length_test_string() -> Result<()> {
        bit_length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
                Ok(())
            })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // error: this test uses too much memory to run on CI
    fn bit_length_test_large_string() -> Result<()> {
        bit_length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value as i64, result.value(i));
                });
                Ok(())
            })
    }

    #[test]
    fn bit_length_binary() -> Result<()> {
        let value: Vec<&[u8]> = vec![b"one", &[0xff, 0xf8], b"three"];
        let expected: Vec<i32> = vec![24, 16, 40];
        length_binary_helper!(i32, Int32Array, bit_length, value, expected)
    }

    #[test]
    fn bit_length_large_binary() -> Result<()> {
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
    fn bit_length_null_string() -> Result<()> {
        bit_length_null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn bit_length_null_large_string() -> Result<()> {
        bit_length_null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

                // convert to i64
                let expected: Int64Array = expected
                    .iter()
                    .map(|e| e.map(|e| e as i64))
                    .collect::<Vec<_>>()
                    .into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn bit_length_null_binary() -> Result<()> {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"one"), None, Some(b"three"), Some(&[0xff, 0xf8])];
        let expected: Vec<Option<i32>> = vec![Some(24), None, Some(40), Some(16)];
        length_binary_helper!(i32, Int32Array, bit_length, value, expected)
    }

    #[test]
    fn bit_length_null_large_binary() -> Result<()> {
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
    fn bit_length_offsets_string() -> Result<()> {
        let a = StringArray::from(vec![Some("hello"), Some(" "), Some("world"), None]);
        let b = a.slice(1, 3);
        let result = bit_length(b.as_ref())?;
        let result: &Int32Array = as_primitive_array(&result);

        let expected = Int32Array::from(vec![Some(8), Some(40), None]);
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn bit_length_offsets_binary() -> Result<()> {
        let value: Vec<Option<&[u8]>> =
            vec![Some(b"hello"), Some(&[]), Some(b"world"), None];
        let a = BinaryArray::from(value);
        let b = a.slice(1, 3);
        let result = bit_length(b.as_ref())?;
        let result: &Int32Array = as_primitive_array(&result);

        let expected = Int32Array::from(vec![Some(0), Some(40), None]);
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn length_dictionary() -> Result<()> {
        _length_dictionary::<Int8Type>()?;
        _length_dictionary::<Int16Type>()?;
        _length_dictionary::<Int32Type>()?;
        _length_dictionary::<Int64Type>()?;
        _length_dictionary::<UInt8Type>()?;
        _length_dictionary::<UInt16Type>()?;
        _length_dictionary::<UInt32Type>()?;
        _length_dictionary::<UInt64Type>()?;
        Ok(())
    }

    fn _length_dictionary<K: ArrowDictionaryKeyType>() -> Result<()> {
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

        let res = length(&dict_array)?;
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

        Ok(())
    }

    #[test]
    fn bit_length_dictionary() -> Result<()> {
        _bit_length_dictionary::<Int8Type>()?;
        _bit_length_dictionary::<Int16Type>()?;
        _bit_length_dictionary::<Int32Type>()?;
        _bit_length_dictionary::<Int64Type>()?;
        _bit_length_dictionary::<UInt8Type>()?;
        _bit_length_dictionary::<UInt16Type>()?;
        _bit_length_dictionary::<UInt32Type>()?;
        _bit_length_dictionary::<UInt64Type>()?;
        Ok(())
    }

    fn _bit_length_dictionary<K: ArrowDictionaryKeyType>() -> Result<()> {
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

        let res = bit_length(&dict_array)?;
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

        Ok(())
    }
}
