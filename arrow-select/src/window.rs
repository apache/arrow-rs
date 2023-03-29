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

//! Defines windowing functions, like `shift`ing

use crate::concat::concat;
use arrow_array::{make_array, new_null_array, Array, ArrayRef};
use arrow_schema::ArrowError;
use num::abs;

/// Shifts array by defined number of items (to left or right)
/// A positive value for `offset` shifts the array to the right
/// a negative value shifts the array to the left.
/// # Examples
/// ```
/// # use arrow_array::Int32Array;
/// # use arrow_select::window::shift;
///
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

    #[test]
    fn test_shift_neg() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, -1).unwrap();
        let expected: Int32Array = vec![None, Some(4), None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_pos() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 1).unwrap();
        let expected: Int32Array = vec![None, Some(1), None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_neg_float64() {
        let a: Float64Array = vec![Some(1.), None, Some(4.)].into();
        let res = shift(&a, -1).unwrap();
        let expected: Float64Array = vec![None, Some(4.), None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_pos_float64() {
        let a: Float64Array = vec![Some(1.), None, Some(4.)].into();
        let res = shift(&a, 1).unwrap();
        let expected: Float64Array = vec![None, Some(1.), None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_neg_int32_dict() {
        let a: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();
        let res = shift(&a, -1).unwrap();
        let expected: Int32DictionaryArray = [None, Some("beta"), Some("alpha"), None]
            .iter()
            .copied()
            .collect();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_pos_int32_dict() {
        let a: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();
        let res = shift(&a, 1).unwrap();
        let expected: Int32DictionaryArray = [None, Some("alpha"), None, Some("beta")]
            .iter()
            .copied()
            .collect();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_nil() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 0).unwrap();
        let expected: Int32Array = vec![Some(1), None, Some(4)].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_boundary_pos() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 3).unwrap();
        let expected: Int32Array = vec![None, None, None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_boundary_neg() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, -3).unwrap();
        let expected: Int32Array = vec![None, None, None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_boundary_neg_min() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, i64::MIN).unwrap();
        let expected: Int32Array = vec![None, None, None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_large_pos() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 1000).unwrap();
        let expected: Int32Array = vec![None, None, None].into();
        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_large_neg() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, -1000).unwrap();
        let expected: Int32Array = vec![None, None, None].into();
        assert_eq!(res.as_ref(), &expected);
    }
}
