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

use arrow_array::builder::BufferBuilder;
use arrow_array::*;
use arrow_data::bit_mask::combine_option_bitmap;
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;

/// Returns the elementwise concatenation of a [`StringArray`].
///
/// An index of the resulting [`StringArray`] is null if any of
/// `StringArray` are null at that location.
///
/// ```text
/// e.g:
///
///   ["Hello"] + ["World"] = ["HelloWorld"]
///
///   ["a", "b"] + [None, "c"] = [None, "bc"]
/// ```
///
/// An error will be returned if `left` and `right` have different lengths
pub fn concat_elements_utf8<Offset: OffsetSizeTrait>(
    left: &GenericStringArray<Offset>,
    right: &GenericStringArray<Offset>,
) -> Result<GenericStringArray<Offset>, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(format!(
            "Arrays must have the same length: {} != {}",
            left.len(),
            right.len()
        )));
    }

    let output_bitmap = combine_option_bitmap(&[left.data(), right.data()], left.len());

    let left_offsets = left.value_offsets();
    let right_offsets = right.value_offsets();

    let left_values = left.value_data();
    let right_values = right.value_data();

    let mut output_values = BufferBuilder::<u8>::new(
        left_values.len() + right_values.len()
            - left_offsets[0].as_usize()
            - right_offsets[0].as_usize(),
    );

    let mut output_offsets = BufferBuilder::<Offset>::new(left_offsets.len());
    output_offsets.append(Offset::zero());
    for (left_idx, right_idx) in left_offsets.windows(2).zip(right_offsets.windows(2)) {
        output_values
            .append_slice(&left_values[left_idx[0].as_usize()..left_idx[1].as_usize()]);
        output_values.append_slice(
            &right_values[right_idx[0].as_usize()..right_idx[1].as_usize()],
        );
        output_offsets.append(Offset::from_usize(output_values.len()).unwrap());
    }

    let builder = ArrayDataBuilder::new(GenericStringArray::<Offset>::DATA_TYPE)
        .len(left.len())
        .add_buffer(output_offsets.finish())
        .add_buffer(output_values.finish())
        .null_bit_buffer(output_bitmap);

    // SAFETY - offsets valid by construction
    Ok(unsafe { builder.build_unchecked() }.into())
}

/// Returns the elementwise concatenation of [`StringArray`].
/// ```text
/// e.g:
///   ["a", "b"] + [None, "c"] + [None, "d"] = [None, "bcd"]
/// ```
///
/// An error will be returned if the [`StringArray`] are of different lengths
pub fn concat_elements_utf8_many<Offset: OffsetSizeTrait>(
    arrays: &[&GenericStringArray<Offset>],
) -> Result<GenericStringArray<Offset>, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::ComputeError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    let size = arrays[0].len();
    if !arrays.iter().all(|array| array.len() == size) {
        return Err(ArrowError::ComputeError(format!(
            "Arrays must have the same length of {}",
            size,
        )));
    }

    let output_bitmap = combine_option_bitmap(
        arrays
            .iter()
            .map(|a| a.data())
            .collect::<Vec<_>>()
            .as_slice(),
        size,
    );

    let data_values = arrays
        .iter()
        .map(|array| array.value_data())
        .collect::<Vec<_>>();

    let mut offsets = arrays
        .iter()
        .map(|a| a.value_offsets().iter().peekable())
        .collect::<Vec<_>>();

    let mut output_values = BufferBuilder::<u8>::new(
        data_values
            .iter()
            .zip(offsets.iter_mut())
            .map(|(data, offset)| data.len() - offset.peek().unwrap().as_usize())
            .sum(),
    );

    let mut output_offsets = BufferBuilder::<Offset>::new(size + 1);
    output_offsets.append(Offset::zero());
    for _ in 0..size {
        data_values
            .iter()
            .zip(offsets.iter_mut())
            .for_each(|(values, offset)| {
                let index_start = offset.next().unwrap().as_usize();
                let index_end = offset.peek().unwrap().as_usize();
                output_values.append_slice(&values[index_start..index_end]);
            });
        output_offsets.append(Offset::from_usize(output_values.len()).unwrap());
    }

    let builder = ArrayDataBuilder::new(GenericStringArray::<Offset>::DATA_TYPE)
        .len(size)
        .add_buffer(output_offsets.finish())
        .add_buffer(output_values.finish())
        .null_bit_buffer(output_bitmap);

    // SAFETY - offsets valid by construction
    Ok(unsafe { builder.build_unchecked() }.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    #[test]
    fn test_string_concat() {
        let left = [Some("foo"), Some("bar"), None]
            .into_iter()
            .collect::<StringArray>();
        let right = [None, Some("yyy"), Some("zzz")]
            .into_iter()
            .collect::<StringArray>();

        let output = concat_elements_utf8(&left, &right).unwrap();

        let expected = [None, Some("baryyy"), None]
            .into_iter()
            .collect::<StringArray>();

        assert_eq!(output, expected);
    }

    #[test]
    fn test_string_concat_empty_string() {
        let left = [Some("foo"), Some(""), Some("bar")]
            .into_iter()
            .collect::<StringArray>();
        let right = [Some("baz"), Some(""), Some("")]
            .into_iter()
            .collect::<StringArray>();

        let output = concat_elements_utf8(&left, &right).unwrap();

        let expected = [Some("foobaz"), Some(""), Some("bar")]
            .into_iter()
            .collect::<StringArray>();

        assert_eq!(output, expected);
    }

    #[test]
    fn test_string_concat_no_null() {
        let left = StringArray::from(vec!["foo", "bar"]);
        let right = StringArray::from(vec!["bar", "baz"]);

        let output = concat_elements_utf8(&left, &right).unwrap();

        let expected = StringArray::from(vec!["foobar", "barbaz"]);

        assert_eq!(output, expected);
    }

    #[test]
    fn test_string_concat_error() {
        let left = StringArray::from(vec!["foo", "bar"]);
        let right = StringArray::from(vec!["baz"]);

        let output = concat_elements_utf8(&left, &right);

        assert_eq!(
            output.unwrap_err().to_string(),
            "Compute error: Arrays must have the same length: 2 != 1".to_string()
        );
    }

    #[test]
    fn test_string_concat_slice() {
        let left = &StringArray::from(vec![None, Some("foo"), Some("bar"), Some("baz")]);
        let right = &StringArray::from(vec![Some("boo"), None, Some("far"), Some("faz")]);

        let left_slice = left.slice(0, 3);
        let right_slice = right.slice(1, 3);
        let output = concat_elements_utf8(
            left_slice
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap(),
            right_slice
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap(),
        )
        .unwrap();

        let expected = [None, Some("foofar"), Some("barfaz")]
            .into_iter()
            .collect::<StringArray>();

        assert_eq!(output, expected);

        let left_slice = left.slice(2, 2);
        let right_slice = right.slice(1, 2);

        let output = concat_elements_utf8(
            left_slice
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap(),
            right_slice
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap(),
        )
        .unwrap();

        let expected = [None, Some("bazfar")].into_iter().collect::<StringArray>();

        assert_eq!(output, expected);
    }

    #[test]
    fn test_string_concat_error_empty() {
        assert_eq!(
            concat_elements_utf8_many::<i32>(&[])
                .unwrap_err()
                .to_string(),
            "Compute error: concat requires input of at least one array".to_string()
        );
    }

    #[test]
    fn test_string_concat_one() {
        let expected = [None, Some("baryyy"), None]
            .into_iter()
            .collect::<StringArray>();

        let output = concat_elements_utf8_many(&[&expected]).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    fn test_string_concat_many() {
        let foo = StringArray::from(vec![Some("f"), Some("o"), Some("o"), None]);
        let bar = StringArray::from(vec![None, Some("b"), Some("a"), Some("r")]);
        let baz = StringArray::from(vec![Some("b"), None, Some("a"), Some("z")]);

        let output = concat_elements_utf8_many(&[&foo, &bar, &baz]).unwrap();

        let expected = [None, None, Some("oaa"), None]
            .into_iter()
            .collect::<StringArray>();

        assert_eq!(output, expected);
    }
}
