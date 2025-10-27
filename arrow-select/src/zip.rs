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

//! [`zip`]: Combine values from two arrays based on boolean mask

use crate::filter::{SlicesIterator, prep_null_mask_filter};
use arrow_array::*;
use arrow_buffer::BooleanBuffer;
use arrow_data::transform::MutableArrayData;
use arrow_schema::ArrowError;

/// Zip two arrays by some boolean mask.
///
/// - Where `mask` is `true`, values of `truthy` are taken
/// - Where `mask` is `false` or `NULL`, values of `falsy` are taken
///
/// # Example: `zip` two arrays
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray, Int32Array};
/// # use arrow_select::zip::zip;
/// // mask: [true, true, false, NULL, true]
/// let mask = BooleanArray::from(vec![
///   Some(true), Some(true), Some(false), None, Some(true)
/// ]);
/// // truthy array: [1, NULL, 3, 4, 5]
/// let truthy = Int32Array::from(vec![
///   Some(1), None, Some(3), Some(4), Some(5)
/// ]);
/// // falsy array: [10, 20, 30, 40, 50]
/// let falsy = Int32Array::from(vec![
///   Some(10), Some(20), Some(30), Some(40), Some(50)
/// ]);
/// // zip with this mask select the first, second and last value from `truthy`
/// // and the third and fourth value from `falsy`
/// let result = zip(&mask, &truthy, &falsy).unwrap();
/// // Expected: [1, NULL, 30, 40, 5]
/// let expected: ArrayRef = Arc::new(Int32Array::from(vec![
///   Some(1), None, Some(30), Some(40), Some(5)
/// ]));
/// assert_eq!(&result, &expected);
/// ```
///
/// # Example: `zip` and array with a scalar
///
/// Use `zip` to replace certain values in an array with a scalar
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray, Int32Array};
/// # use arrow_select::zip::zip;
/// // mask: [true, true, false, NULL, true]
/// let mask = BooleanArray::from(vec![
///   Some(true), Some(true), Some(false), None, Some(true)
/// ]);
/// //  array: [1, NULL, 3, 4, 5]
/// let arr = Int32Array::from(vec![
///   Some(1), None, Some(3), Some(4), Some(5)
/// ]);
/// // scalar: 42
/// let scalar = Int32Array::new_scalar(42);
/// // zip the array with the  mask select the first, second and last value from `arr`
/// // and fill the third and fourth value with the scalar 42
/// let result = zip(&mask, &arr, &scalar).unwrap();
/// // Expected: [1, NULL, 42, 42, 5]
/// let expected: ArrayRef = Arc::new(Int32Array::from(vec![
///   Some(1), None, Some(42), Some(42), Some(5)
/// ]));
/// assert_eq!(&result, &expected);
/// ```
pub fn zip(
    mask: &BooleanArray,
    truthy: &dyn Datum,
    falsy: &dyn Datum,
) -> Result<ArrayRef, ArrowError> {
    let (truthy, truthy_is_scalar) = truthy.get();
    let (falsy, falsy_is_scalar) = falsy.get();

    if truthy.data_type() != falsy.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "arguments need to have the same data type".into(),
        ));
    }

    if truthy_is_scalar && truthy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }
    if !truthy_is_scalar && truthy.len() != mask.len() {
        return Err(ArrowError::InvalidArgumentError(
            "all arrays should have the same length".into(),
        ));
    }
    if falsy_is_scalar && falsy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }
    if !falsy_is_scalar && falsy.len() != mask.len() {
        return Err(ArrowError::InvalidArgumentError(
            "all arrays should have the same length".into(),
        ));
    }

    let falsy = falsy.to_data();
    let truthy = truthy.to_data();

    let mut mutable = MutableArrayData::new(vec![&truthy, &falsy], false, truthy.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;

    let mask = maybe_prep_null_mask_filter(mask);
    SlicesIterator::from(&mask).for_each(|(start, end)| {
        // the gap needs to be filled with falsy values
        if start > filled {
            if falsy_is_scalar {
                for _ in filled..start {
                    // Copy the first item from the 'falsy' array into the output buffer.
                    mutable.extend(1, 0, 1);
                }
            } else {
                mutable.extend(1, filled, start);
            }
        }
        // fill with truthy values
        if truthy_is_scalar {
            for _ in start..end {
                // Copy the first item from the 'truthy' array into the output buffer.
                mutable.extend(0, 0, 1);
            }
        } else {
            mutable.extend(0, start, end);
        }
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        if falsy_is_scalar {
            for _ in filled..mask.len() {
                // Copy the first item from the 'falsy' array into the output buffer.
                mutable.extend(1, 0, 1);
            }
        } else {
            mutable.extend(1, filled, mask.len());
        }
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

fn maybe_prep_null_mask_filter(predicate: &BooleanArray) -> BooleanBuffer {
    // Nulls are treated as false
    if predicate.null_count() == 0 {
        predicate.values().clone()
    } else {
        let cleaned = prep_null_mask_filter(predicate);
        let (boolean_buffer, _) = cleaned.into_parts();
        boolean_buffer
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_buffer::{BooleanBuffer, NullBuffer};

    #[test]
    fn test_zip_kernel_one() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(3), Some(6), Some(7), Some(3)]);
        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &a, &b).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(6), Some(7), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_two() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(3), Some(6), Some(7), Some(3)]);
        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &a, &b).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, Some(3), Some(7), None, Some(3)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_falsy_1() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &a, &fallback).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(42), Some(42), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_falsy_2() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &a, &fallback).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(7), None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_truthy_1() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &fallback, &a).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(7), None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_truthy_2() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &fallback, &a).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(42), Some(42), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_both() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(123), Some(123), Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_none_1() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), None, None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_none_2() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, None, Some(42), Some(42), None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_primitive_array_with_nulls_is_mask_should_be_treated_as_false() {
        let truthy = Int32Array::from_iter_values(vec![1, 2, 3, 4, 5, 6]);
        let falsy = Int32Array::from_iter_values(vec![7, 8, 9, 10, 11, 12]);

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &truthy, &falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(9),
            Some(10), // true in mask but null
            Some(11),
            Some(12),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_primitive_scalar_with_boolean_array_mask_with_nulls_should_be_treated_as_false()
     {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![
            Some(42),
            Some(42),
            Some(123),
            Some(123), // true in mask but null
            Some(123),
            Some(123),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_string_array_with_nulls_is_mask_should_be_treated_as_false() {
        let truthy = StringArray::from_iter_values(vec!["1", "2", "3", "4", "5", "6"]);
        let falsy = StringArray::from_iter_values(vec!["7", "8", "9", "10", "11", "12"]);

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &truthy, &falsy).unwrap();
        let actual = out.as_string::<i32>();
        let expected = StringArray::from_iter_values(vec![
            "1", "2", "9", "10", // true in mask but null
            "11", "12",
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_large_string_scalar_with_boolean_array_mask_with_nulls_should_be_treated_as_false()
     {
        let scalar_truthy = Scalar::new(LargeStringArray::from_iter_values(["test"]));
        let scalar_falsy = Scalar::new(LargeStringArray::from_iter_values(["something else"]));

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let expected = LargeStringArray::from_iter(vec![
            Some("test"),
            Some("test"),
            Some("something else"),
            Some("something else"), // true in mask but null
            Some("something else"),
            Some("something else"),
        ]);
        assert_eq!(actual, &expected);
    }
}
