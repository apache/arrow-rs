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

use crate::array::*;
use crate::compute::util::combine_option_bitmap;
use crate::error::{ArrowError, Result};

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
) -> Result<GenericStringArray<Offset>> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(format!(
            "Arrays must have the same length: {} != {}",
            left.len(),
            right.len()
        )));
    }

    let output_bitmap = combine_option_bitmap(&[left.data(), right.data()], left.len())?;

    let left_offsets = left.value_offsets();
    let right_offsets = right.value_offsets();

    let left_buffer = left.value_data();
    let right_buffer = right.value_data();
    let left_values = left_buffer.as_slice();
    let right_values = right_buffer.as_slice();

    let mut output_values = BufferBuilder::<u8>::new(
        left_values.len() + right_values.len()
            - left_offsets[0].to_usize().unwrap()
            - right_offsets[0].to_usize().unwrap(),
    );

    let mut output_offsets = BufferBuilder::<Offset>::new(left_offsets.len());
    output_offsets.append(Offset::zero());
    for (left_idx, right_idx) in left_offsets.windows(2).zip(right_offsets.windows(2)) {
        output_values.append_slice(
            &left_values
                [left_idx[0].to_usize().unwrap()..left_idx[1].to_usize().unwrap()],
        );
        output_values.append_slice(
            &right_values
                [right_idx[0].to_usize().unwrap()..right_idx[1].to_usize().unwrap()],
        );
        output_offsets.append(Offset::from_usize(output_values.len()).unwrap());
    }

    let builder = ArrayDataBuilder::new(GenericStringArray::<Offset>::get_data_type())
        .len(left.len())
        .add_buffer(output_offsets.finish())
        .add_buffer(output_values.finish())
        .null_bit_buffer(output_bitmap);

    // SAFETY - offsets valid by construction
    Ok(unsafe { builder.build_unchecked() }.into())
}

#[cfg(test)]
mod tests {
    use super::*;
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

        assert!(output.is_err());
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
}
