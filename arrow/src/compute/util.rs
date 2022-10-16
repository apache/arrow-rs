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

//! Common utilities for computation kernels.

use crate::array::*;
use crate::buffer::{buffer_bin_and, Buffer};
use crate::error::{ArrowError, Result};

/// Combines the null bitmaps of multiple arrays using a bitwise `and` operation.
///
/// This function is useful when implementing operations on higher level arrays.
#[allow(clippy::unnecessary_wraps)]
pub(super) fn combine_option_bitmap(
    arrays: &[&ArrayData],
    len_in_bits: usize,
) -> Result<Option<Buffer>> {
    arrays
        .iter()
        .map(|array| (array.null_buffer().cloned(), array.offset()))
        .reduce(|acc, buffer_and_offset| match (acc, buffer_and_offset) {
            ((None, _), (None, _)) => (None, 0),
            ((Some(buffer), offset), (None, _)) | ((None, _), (Some(buffer), offset)) => {
                (Some(buffer), offset)
            }
            ((Some(buffer_left), offset_left), (Some(buffer_right), offset_right)) => (
                Some(buffer_bin_and(
                    &buffer_left,
                    offset_left,
                    &buffer_right,
                    offset_right,
                    len_in_bits,
                )),
                0,
            ),
        })
        .map_or(
            Err(ArrowError::ComputeError(
                "Arrays must not be empty".to_string(),
            )),
            |(buffer, offset)| {
                Ok(buffer.map(|buffer| buffer.bit_slice(offset, len_in_bits)))
            },
        )
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::array::ArrayData;
    use crate::buffer::buffer_bin_or;
    use crate::datatypes::DataType;

    /// Compares the null bitmaps of two arrays using a bitwise `or` operation.
    ///
    /// This function is useful when implementing operations on higher level arrays.
    pub(super) fn compare_option_bitmap(
        left_data: &ArrayData,
        right_data: &ArrayData,
        len_in_bits: usize,
    ) -> Result<Option<Buffer>> {
        let left_offset_in_bits = left_data.offset();
        let right_offset_in_bits = right_data.offset();

        let left = left_data.null_buffer();
        let right = right_data.null_buffer();

        match left {
            None => match right {
                None => Ok(None),
                Some(r) => Ok(Some(r.bit_slice(right_offset_in_bits, len_in_bits))),
            },
            Some(l) => match right {
                None => Ok(Some(l.bit_slice(left_offset_in_bits, len_in_bits))),

                Some(r) => Ok(Some(buffer_bin_or(
                    l,
                    left_offset_in_bits,
                    r,
                    right_offset_in_bits,
                    len_in_bits,
                ))),
            },
        }
    }

    fn make_data_with_null_bit_buffer(
        len: usize,
        offset: usize,
        null_bit_buffer: Option<Buffer>,
    ) -> Arc<ArrayData> {
        let buffer = Buffer::from(&vec![11; len + offset]);

        Arc::new(
            ArrayData::try_new(
                DataType::UInt8,
                len,
                null_bit_buffer,
                offset,
                vec![buffer],
                vec![],
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_combine_option_bitmap() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let some_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b01001010])));
        let inverse_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10110101])));
        let some_other_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b11010111])));
        assert_eq!(
            combine_option_bitmap(&[], 8).unwrap_err().to_string(),
            "Compute error: Arrays must not be empty",
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap], 8).unwrap()
        );
        assert_eq!(
            None,
            combine_option_bitmap(&[&none_bitmap, &none_bitmap], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap, &none_bitmap], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b11010111])),
            combine_option_bitmap(&[&none_bitmap, &some_other_bitmap], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&[&some_bitmap, &some_bitmap], 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b0])),
            combine_option_bitmap(&[&some_bitmap, &inverse_bitmap], 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01000010])),
            combine_option_bitmap(&[&some_bitmap, &some_other_bitmap, &none_bitmap], 8,)
                .unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b00001001])),
            combine_option_bitmap(
                &[
                    &some_bitmap.slice(3, 5),
                    &inverse_bitmap.slice(2, 5),
                    &some_other_bitmap.slice(1, 5)
                ],
                5,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_combine_option_bitmap_with_offsets() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let bitmap0 =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10101010])));
        let bitmap1 =
            make_data_with_null_bit_buffer(8, 1, Some(Buffer::from([0b01010100, 0b1])));
        let bitmap2 =
            make_data_with_null_bit_buffer(8, 2, Some(Buffer::from([0b10101000, 0b10])));
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap2], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1, &none_bitmap], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&none_bitmap, &bitmap2], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap0, &bitmap1], 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b10101010])),
            combine_option_bitmap(&[&bitmap1, &bitmap2], 8).unwrap()
        );
    }

    #[test]
    fn test_compare_option_bitmap() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let some_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b01001010])));
        let inverse_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10110101])));
        assert_eq!(
            None,
            compare_option_bitmap(&none_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&some_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&none_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&some_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b11111111])),
            compare_option_bitmap(&some_bitmap, &inverse_bitmap, 8,).unwrap()
        );
    }
}
