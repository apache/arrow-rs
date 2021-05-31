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

use crate::array::{Array, ArrayRef};
use crate::{array::PrimitiveArray, datatypes::ArrowPrimitiveType, error::Result};
use crate::{
    array::{make_array, new_null_array},
    compute::concat,
};
use num::{abs, clamp};

/// Shifts array by defined number of items (to left or right)
/// A positive value for `offset` shifts the array to the right
/// a negative value shifts the array to the left.
/// # Examples
/// ```
/// use arrow::array::Int32Array;
/// use arrow::error::Result;
/// use arrow::compute::shift;
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
/// // shift array 3 element tot he right
/// let res = shift(&a, 3).unwrap();
/// let expected: Int32Array = vec![None, None, None].into();
/// assert_eq!(res.as_ref(), &expected);
/// ```
pub fn shift(array: &Array, offset: i64) -> Result<ArrayRef> {
    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(make_array(values.data_ref().clone()))
    } else if offset == i64::MIN || abs(offset) >= value_len {
        Ok(new_null_array(array.data_type(), array.len()))
    } else {
        let slice_offset = clamp(-offset, 0, value_len) as usize;
        let length = array.len() - abs(offset) as usize;
        let slice = array.slice(slice_offset, length);

        // Generate array with remaining `null` items
        let nulls = abs(offset) as usize;
        let null_arr = new_null_array(array.data_type(), nulls);

        // Concatenate both arrays, add nulls after if shift > 0 else before
        if offset > 0 {
            concat(&[null_arr.as_ref(), slice.as_ref()])
        } else {
            concat(&[slice.as_ref(), null_arr.as_ref()])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

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
