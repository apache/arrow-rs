// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines windowing functions, like `shift`ing

use crate::concat::concat;
use arrow_array::{Array, ArrayRef, make_array, new_null_array};
use arrow_schema::ArrowError;
use num_traits::abs;

/// Shifts array by defined number of items (to left or right)
/// A positive value for `offset` shifts the array to the right
/// a negative value shifts the array to the left.
/// # Examples
/// ```
/// # use arrow_array::Int32Array;
/// # use arrow_select::window::shift;
/// let a: Int32Array = vec![Some(1), None, Some(4)].into();
///
/// // shift array 1 element to the right
/// let res = shift(&a, 1).unwrap();
/// let expected: Int32Array = vec![None, Some(1), None].into();
/// assert_eq!(res.as_ref(), &expected);
///
/// // shift array 1 element to the left
/// let res = shift(&a, -1).unwrap();
/// let expected: Int32Array = vec![None, Some(4), None].into();
/// assert_eq!(res.as_ref(), &expected);
///
/// // shift array 0 element, although not recommended
/// let res = shift(&a, 0).unwrap();
/// let expected: Int32Array = vec![Some(1), None, Some(4)].into();
/// assert_eq!(res.as_ref(), &expected);
///
/// // shift array 3 element to the right
/// let res = shift(&a, 3).unwrap();
/// let expected: Int32Array = vec![None, None, None].into();
/// assert_eq!(res.as_ref(), &expected);
/// ```
pub fn shift(array: &dyn Array, offset: i64) -> Result<ArrayRef, ArrowError> {
    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(make_array(array.to_data()))
    } else if offset == i64::MIN || abs(offset) >= value_len {
        Ok(new_null_array(array.data_type(), array.len()))
    } else {
        // Concatenate both arrays, add nulls after if shift > 0 else before
        if offset > 0 {
            let length = array.len() - offset as usize;
            let slice = array.slice(0, length);

            // Generate array with remaining `null` items
            let null_arr = new_null_array(array.data_type(), offset as usize);
            concat(&[null_arr.as_ref(), slice.as_ref()])
        } else {
            let offset = -offset as usize;
            let length = array.len() - offset;
            let slice = array.slice(offset, length);

            // Generate array with remaining `null` items
            let null_arr = new_null_array(array.data_type(), offset);
            concat(&[slice.as_ref(), null_arr.as_ref()])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, Int32DictionaryArray};
    use rstest::rstest;

    #[rstest]
    #[case::negative(-1, vec![None, Some(4), None])]
    #[case::positive(1, vec![None, Some(1), None])]
    #[case::zero(0, vec![Some(1), None, Some(4)])]
    #[case::positive_boundary(3, vec![None, None, None])]
    #[case::negative_boundary(-3, vec![None, None, None])]
    #[case::minimum_offset(i64::MIN, vec![None, None, None])]
    #[case::positive_beyond_length(1000, vec![None, None, None])]
    #[case::negative_beyond_length(-1000, vec![None, None, None])]
    fn test_shift_int32(#[case] offset: i64, #[case] expected: Vec<Option<i32>>) {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, offset).unwrap();
        let expected = Int32Array::from(expected);
        assert_eq!(res.as_ref(), &expected);
    }

    #[rstest]
    #[case::negative(-1, vec![None, Some(4.), None])]
    #[case::positive(1, vec![None, Some(1.), None])]
    fn test_shift_float64(#[case] offset: i64, #[case] expected: Vec<Option<f64>>) {
        let a: Float64Array = vec![Some(1.), None, Some(4.)].into();
        let res = shift(&a, offset).unwrap();
        let expected = Float64Array::from(expected);
        assert_eq!(res.as_ref(), &expected);
    }

    #[rstest]
    #[case::negative(-1, [None, Some("beta"), Some("alpha"), None])]
    #[case::positive(1, [None, Some("alpha"), None, Some("beta")])]
    fn test_shift_int32_dict(#[case] offset: i64, #[case] expected: [Option<&str>; 4]) {
        let a: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();
        let res = shift(&a, offset).unwrap();
        let expected: Int32DictionaryArray = expected.iter().copied().collect();
        assert_eq!(res.as_ref(), &expected);
    }
}
