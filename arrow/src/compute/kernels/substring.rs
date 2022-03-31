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

    let new_starts: Vec<OffsetSize> = if start >= zero {
        offsets
            .windows(2)
            .map(|pair| (pair[0] + start).min(pair[1]))
            .collect()
    } else {
        offsets
            .windows(2)
            .map(|pair| (pair[1] + start).max(pair[0]))
            .collect()
    };

    let new_length: Vec<OffsetSize> = if let Some(length) = length {
        offsets[1..]
            .iter()
            .zip(new_starts.iter())
            .map(|(end, start)| *(length.min(&(*end - *start))))
            .collect()
    } else {
        offsets[1..]
            .iter()
            .zip(new_starts.iter())
            .map(|(end, start)| *end - *start)
            .collect()
    };

    let new_offsets: Vec<OffsetSize> = [zero]
        .iter()
        .copied()
        .chain(new_length.iter().scan(zero, |len_so_far, &len| {
            *len_so_far += len;
            Some(*len_so_far)
        }))
        .collect();

    let new_values = {
        let mut new_values =
            MutableBuffer::new(new_offsets.last().unwrap().to_usize().unwrap());
        new_starts
            .iter()
            .zip(new_length.iter())
            .map(|(start, length)| {
                (start.to_usize().unwrap(), length.to_usize().unwrap())
            })
            .map(|(start, length)| &data[start..start + length])
            .for_each(|slice| new_values.extend_from_slice(slice));
        new_values
    };

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

/// Returns an ArrayRef with a substring starting from `start` and with optional length `length` of each of the elements in `array`.
/// `start` can be negative, in which case the start counts from the end of the string.
/// this function errors when the passed array is not a \[Large\]String array.
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
}
