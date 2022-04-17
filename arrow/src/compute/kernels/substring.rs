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

//! Defines kernel to extract a substring of a \[Large\]StringArray

use crate::buffer::MutableBuffer;
use crate::{array::*, buffer::Buffer};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};
use std::cmp::Ordering;

fn generic_substring<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    start: OffsetSize,
    length: &Option<OffsetSize>,
) -> Result<ArrayRef> {
    let offsets = array.value_offsets();
    let null_bit_buffer = array.data_ref().null_buffer().cloned();
    let values = array.value_data();
    let data = values.as_slice();
    let zero = OffsetSize::zero();

    // check the `idx` is a valid char boundary or not
    // If yes, return `idx`, else return error
    let check_char_boundary = |idx: OffsetSize| {
        let idx_usize = idx.to_usize().unwrap();
        if idx_usize == data.len() || data[idx_usize] >> 6 != 0b10_u8 {
            Ok(idx)
        } else {
            Err(ArrowError::ComputeError(format!("The index {} is invalid because {:#010b} is not the head byte of a char.", idx_usize, data[idx_usize])))
        }
    };

    // function for calculating the start index of each string
    let cal_new_start: Box<dyn Fn(OffsetSize, OffsetSize) -> Result<OffsetSize>> =
        match start.cmp(&zero) {
            // count from the start of string
            // and check it is a valid char boundary
            Ordering::Greater => Box::new(|old_start, end| {
                check_char_boundary((old_start + start).min(end))
            }),
            Ordering::Equal => Box::new(|old_start, _| Ok(old_start)),
            // count from the end of string
            Ordering::Less => Box::new(|old_start, end| {
                check_char_boundary((end + start).max(old_start))
            }),
        };

    // function for calculating the end index of each string
    let cal_new_end: Box<dyn Fn(OffsetSize, OffsetSize) -> Result<OffsetSize>> =
        match length {
            Some(length) => {
                Box::new(|start, end| check_char_boundary((*length + start).min(end)))
            }
            None => Box::new(|_, end| Ok(end)),
        };

    // start and end offsets for each substring
    let mut new_starts_ends: Vec<(OffsetSize, OffsetSize)> =
        Vec::with_capacity(array.len());
    let mut new_offsets: Vec<OffsetSize> = Vec::with_capacity(array.len() + 1);
    let mut len_so_far = zero;
    new_offsets.push(zero);

    offsets.windows(2).try_for_each(|pair| -> Result<()> {
        let new_start = cal_new_start(pair[0], pair[1])?;
        let new_end = cal_new_end(new_start, pair[1])?;
        len_so_far += new_end - new_start;
        new_starts_ends.push((new_start, new_end));
        new_offsets.push(len_so_far);
        Ok(())
    })?;

    // concatenate substrings into a buffer
    let mut new_values =
        MutableBuffer::new(new_offsets.last().unwrap().to_usize().unwrap());

    new_starts_ends
        .iter()
        .map(|(start, end)| {
            let start = start.to_usize().unwrap();
            let end = end.to_usize().unwrap();
            &data[start..end]
        })
        .for_each(|slice| new_values.extend_from_slice(slice));

    let data = unsafe {
        ArrayData::new_unchecked(
            <OffsetSize as StringOffsetSizeTrait>::DATA_TYPE,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from_slice_ref(&new_offsets), new_values.into()],
            vec![],
        )
    };
    Ok(make_array(data))
}

/// Returns an ArrayRef with substrings of all the elements in `array`.
///
/// # Arguments
///
/// * `start` - The start index of all substrings.
/// If `start >= 0`, then count from the start of the string,
/// otherwise count from the end of the string.
///
/// * `length`(option) - The length of all substrings.
/// If `length` is `None`, then the substring is from `start` to the end of the string.
///
/// Attention: Both `start` and `length` are counted by byte, not by char.
///
/// # Basic usage
/// ```
/// # use arrow::array::StringArray;
/// # use arrow::compute::kernels::substring::substring;
/// let array = StringArray::from(vec![Some("arrow"), None, Some("rust")]);
/// let result = substring(&array, 1, &Some(4)).unwrap();
/// let result = result.as_any().downcast_ref::<StringArray>().unwrap();
/// assert_eq!(result, &StringArray::from(vec![Some("rrow"), None, Some("ust")]));
/// ```
///
/// # Error
/// - The function errors when the passed array is not a \[Large\]String array.
/// - The function errors when you try to create a substring in the middle of a multibyte character.
/// ```
/// # use arrow::array::StringArray;
/// # use arrow::compute::kernels::substring::substring;
/// let array = StringArray::from(vec![Some("E=mc²")]);
/// let result = substring(&array, -1, &None);
/// assert_eq!(result.is_err(), true);
/// ```
pub fn substring(
    array: &dyn Array,
    start: i64,
    length: &Option<u64>,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::LargeUtf8 => generic_substring(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            &length.map(|e| e as i64),
        ),
        DataType::Utf8 => generic_substring(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start as i32,
            &length.map(|e| e as i32),
        ),
        _ => Err(ArrowError::ComputeError(format!(
            "substring does not support type {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_nulls<T: 'static + Array + PartialEq + From<Vec<Option<&'static str>>>>(
    ) -> Result<()> {
        let cases = vec![
            // identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // 0 length -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high start -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                1000,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high negative start -> identity
            (
                vec![Some("hello"), None, Some("word")],
                -1000,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // high length -> identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(1000),
                vec![Some("hello"), None, Some("word")],
            ),
        ];

        cases.into_iter().try_for_each::<_, Result<()>>(
            |(array, start, length, expected)| {
                let array = T::from(array);
                let result: ArrayRef = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());

                let result = result.as_any().downcast_ref::<T>().unwrap();
                let expected = T::from(expected);
                assert_eq!(&expected, result);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn with_nulls_string() -> Result<()> {
        with_nulls::<StringArray>()
    }

    #[test]
    fn with_nulls_large_string() -> Result<()> {
        with_nulls::<LargeStringArray>()
    }

    fn without_nulls<T: 'static + Array + PartialEq + From<Vec<Option<&'static str>>>>(
    ) -> Result<()> {
        let cases = vec![
            // increase start
            (
                vec!["hello", "", "word"],
                0,
                None,
                vec!["hello", "", "word"],
            ),
            (vec!["hello", "", "word"], 1, None, vec!["ello", "", "ord"]),
            (vec!["hello", "", "word"], 2, None, vec!["llo", "", "rd"]),
            (vec!["hello", "", "word"], 3, None, vec!["lo", "", "d"]),
            (vec!["hello", "", "word"], 10, None, vec!["", "", ""]),
            // increase start negatively
            (vec!["hello", "", "word"], -1, None, vec!["o", "", "d"]),
            (vec!["hello", "", "word"], -2, None, vec!["lo", "", "rd"]),
            (vec!["hello", "", "word"], -3, None, vec!["llo", "", "ord"]),
            (
                vec!["hello", "", "word"],
                -10,
                None,
                vec!["hello", "", "word"],
            ),
            // increase length
            (vec!["hello", "", "word"], 1, Some(1), vec!["e", "", "o"]),
            (vec!["hello", "", "word"], 1, Some(2), vec!["el", "", "or"]),
            (
                vec!["hello", "", "word"],
                1,
                Some(3),
                vec!["ell", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                1,
                Some(4),
                vec!["ello", "", "ord"],
            ),
            (vec!["hello", "", "word"], -3, Some(1), vec!["l", "", "o"]),
            (vec!["hello", "", "word"], -3, Some(2), vec!["ll", "", "or"]),
            (
                vec!["hello", "", "word"],
                -3,
                Some(3),
                vec!["llo", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                -3,
                Some(4),
                vec!["llo", "", "ord"],
            ),
        ];

        cases.into_iter().try_for_each::<_, Result<()>>(
            |(array, start, length, expected)| {
                let array = StringArray::from(array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());
                let result = result.as_any().downcast_ref::<StringArray>().unwrap();
                let expected = StringArray::from(expected);
                assert_eq!(&expected, result,);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn without_nulls_string() -> Result<()> {
        without_nulls::<StringArray>()
    }

    #[test]
    fn without_nulls_large_string() -> Result<()> {
        without_nulls::<LargeStringArray>()
    }

    #[test]
    fn check_invalid_array_type() {
        let array = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        assert_eq!(substring(&array, 0, &None).is_err(), true);
    }

    // tests for the utf-8 validation checking
    #[test]
    fn check_start_index() {
        let array = StringArray::from(vec![Some("E=mc²"), Some("ascii")]);
        let result = substring(&array, -1, &None);
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn check_length() {
        let array = StringArray::from(vec![Some("E=mc²"), Some("ascii")]);
        let result = substring(&array, 0, &Some(5));
        assert_eq!(result.is_err(), true);
    }
}
