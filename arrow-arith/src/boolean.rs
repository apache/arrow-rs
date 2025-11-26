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

//! Defines boolean kernels on Arrow `BooleanArray`'s, e.g. `AND`, `OR` and `NOT`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use arrow_array::*;
use arrow_buffer::buffer::{bitwise_bin_op_helper, bitwise_quaternary_op_helper};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::ArrowError;

/// Logical 'and' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` and `null` = `null`
/// * `null` and `true` = `null`
/// * `false` and `null` = `false`
/// * `null` and `false` = `false`
/// * `null` and `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'and' false is always false.
/// For a different null behavior, see function \"and\".
///
/// # Example
///
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::and_kleene;
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let and_ab = and_kleene(&a, &b).unwrap();
/// assert_eq!(and_ab, BooleanArray::from(vec![None, Some(false), None]));
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn and_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let left_values = left.values();
    let right_values = right.values();

    let buffer = match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(left_null_buffer), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is false (because null AND false = false).
            Some(bitwise_bin_op_helper(
                left_null_buffer.buffer(),
                left_null_buffer.offset(),
                right_values.inner(),
                right_values.offset(),
                left.len(),
                |a, b| a | !b,
            ))
        }
        (None, Some(right_null_buffer)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_null_buffer.buffer(),
                right_null_buffer.offset(),
                left_values.inner(),
                left_values.offset(),
                left.len(),
                |a, b| a | !b,
            ))
        }
        (Some(left_null_buffer), Some(right_null_buffer)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & !d)) & (c | (a & !b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_null_buffer.buffer(),
                    left_values.inner(),
                    right_null_buffer.buffer(),
                    right_values.inner(),
                ],
                [
                    left_null_buffer.offset(),
                    left_values.offset(),
                    right_null_buffer.offset(),
                    right_values.offset(),
                ],
                left.len(),
                |a, b, c, d| (a | (c & !d)) & (c | (a & !b)),
            ))
        }
    };
    let nulls = buffer.map(|b| NullBuffer::new(BooleanBuffer::new(b, 0, left.len())));
    Ok(BooleanArray::new(left_values & right_values, nulls))
}

/// Logical 'or' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` or `null` = `true`
/// * `null` or `true` = `true`
/// * `false` or `null` = `null`
/// * `null` or `false` = `null`
/// * `null` or `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'or' true is always true.
/// For a different null behavior, see function \"or\".
///
/// # Example
///
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::or_kleene;
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let or_ab = or_kleene(&a, &b).unwrap();
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), None, None]));
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn or_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let left_values = left.values();
    let right_values = right.values();

    let buffer = match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(left_nulls), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is true (because null OR true = true).
            Some(bitwise_bin_op_helper(
                left_nulls.buffer(),
                left_nulls.offset(),
                right_values.inner(),
                right_values.offset(),
                left.len(),
                |a, b| a | b,
            ))
        }
        (None, Some(right_nulls)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_nulls.buffer(),
                right_nulls.offset(),
                left_values.inner(),
                left_values.offset(),
                left.len(),
                |a, b| a | b,
            ))
        }
        (Some(left_nulls), Some(right_nulls)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & d)) & (c | (a & b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_nulls.buffer(),
                    left_values.inner(),
                    right_nulls.buffer(),
                    right_values.inner(),
                ],
                [
                    left_nulls.offset(),
                    left_values.offset(),
                    right_nulls.offset(),
                    right_values.offset(),
                ],
                left.len(),
                |a, b, c, d| (a | (c & d)) & (c | (a & b)),
            ))
        }
    };

    let nulls = buffer.map(|b| NullBuffer::new(BooleanBuffer::new(b, 0, left.len())));
    Ok(BooleanArray::new(left_values | right_values, nulls))
}

/// Helper function to implement binary kernels
pub(crate) fn binary_boolean_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(&BooleanBuffer, &BooleanBuffer) -> BooleanBuffer,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let values = op(left.values(), right.values());
    Ok(BooleanArray::new(values, nulls))
}

/// Performs `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::and;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let and_ab = and(&a, &b).unwrap();
/// assert_eq!(and_ab, BooleanArray::from(vec![Some(false), Some(true), None]));
/// ```
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    left.binary(right, |a, b| a & b)
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::or;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let or_ab = or(&a, &b).unwrap();
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), Some(true), None]));
/// ```
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    left.binary(right, |a, b| a | b)
}

/// Performs `AND_NOT` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::{and, not, and_not};
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let andn_ab = and_not(&a, &b).unwrap();
/// assert_eq!(andn_ab, BooleanArray::from(vec![Some(false), Some(false), None]));
/// // It's equal to and(left, not(right))
/// assert_eq!(andn_ab, and(&a, &not(&b).unwrap()).unwrap());
pub fn and_not(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    left.binary(right, |a, b| a & !b)
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
/// # Error
/// This function never errors. It returns an error for consistency.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::not;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let not_a = not(&a).unwrap();
/// assert_eq!(not_a, BooleanArray::from(vec![Some(true), Some(false), None]));
/// ```
pub fn not(left: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    Ok(left.unary(|a| !a))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::is_null;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_null = is_null(&a).unwrap();
/// assert_eq!(a_is_null, BooleanArray::from(vec![false, false, true]));
/// ```
pub fn is_null(input: &dyn Array) -> Result<BooleanArray, ArrowError> {
    let values = match input.logical_nulls() {
        None => BooleanBuffer::new_unset(input.len()),
        Some(nulls) => !nulls.inner(),
    };

    Ok(BooleanArray::new(values, None))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is not null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::is_not_null;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_not_null = is_not_null(&a).unwrap();
/// assert_eq!(a_is_not_null, BooleanArray::from(vec![true, true, false]));
/// ```
pub fn is_not_null(input: &dyn Array) -> Result<BooleanArray, ArrowError> {
    let values = match input.logical_nulls() {
        None => BooleanBuffer::new_set(input.len()),
        Some(n) => n.inner().clone(),
    };
    Ok(BooleanArray::new(values, None))
}

#[cfg(test)]
mod tests {
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{DataType, Field, UnionFields};

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, true, true, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_not() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and_not(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, true, false]);

        assert_eq!(c, expected);
        assert_eq!(c, and(&a, &not(&b).unwrap()).unwrap());
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_boolean_array_kleene_no_remainder() {
        let n = 1024;
        let a = BooleanArray::from(vec![true; n]);
        let b = BooleanArray::from(vec![None; n]);
        let result = or_kleene(&a, &b).unwrap();

        assert_eq!(result, a);
    }

    #[test]
    fn test_bool_array_and_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_right_sided_nulls() {
        let a = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is absent
        assert!(a.nulls().is_none());

        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is present
        assert!(b.nulls().is_some());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_left_sided_nulls() {
        let a = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is absent
        assert!(a.nulls().is_some());

        let b = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is present
        assert!(b.nulls().is_none());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, true]);
        let c = not(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not_sliced() {
        let a = BooleanArray::from(vec![None, Some(true), Some(false), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let c = not(a).unwrap();

        let expected = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset_mod8() {
        let a = BooleanArray::from(vec![
            false, false, true, true, false, false, false, false, false, false, false, false,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(0, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset1() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from(vec![false, true, false, true]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset2() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_nulls_offset() {
        let a = BooleanArray::from(vec![None, Some(false), Some(true), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let b = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);

        let b = b.slice(2, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![Some(false), Some(false), None, Some(true)]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_null_array_is_null() {
        let a = NullArray::new(3);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_null_array_is_not_null() {
        let a = NullArray::new(3);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_dense_union_is_null() {
        // union of [{A=1}, {A=}, {B=3.2}, {B=}, {C="a"}, {C=}]
        let int_array = Int32Array::from(vec![Some(1), None]);
        let float_array = Float64Array::from(vec![Some(3.2), None]);
        let str_array = StringArray::from(vec![Some("a"), None]);
        let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1, 0, 1, 0, 1]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array = UnionArray::try_new(union_fields(), type_ids, Some(offsets), children).unwrap();

        let result = is_null(&array).unwrap();

        let expected = &BooleanArray::from(vec![false, true, false, true, false, true]);
        assert_eq!(expected, &result);
    }

    #[test]
    fn test_sparse_union_is_null() {
        // union of [{A=1}, {A=}, {B=3.2}, {B=}, {C="a"}, {C=}]
        let int_array = Int32Array::from(vec![Some(1), None, None, None, None, None]);
        let float_array = Float64Array::from(vec![None, None, Some(3.2), None, None, None]);
        let str_array = StringArray::from(vec![None, None, None, None, Some("a"), None]);
        let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array = UnionArray::try_new(union_fields(), type_ids, None, children).unwrap();

        let result = is_null(&array).unwrap();

        let expected = &BooleanArray::from(vec![false, true, false, true, false, true]);
        assert_eq!(expected, &result);
    }

    fn union_fields() -> UnionFields {
        [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
            (2, Arc::new(Field::new("C", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn test_boolean_kernels_with_nulls_and_offsets() {
        // Construct BooleanArrays with mixed values and nulls
        let left = BooleanArray::from(vec![
            Some(true), Some(false), None, Some(true), Some(false), None, Some(true)
        ]);
        let right = BooleanArray::from(vec![
            None, Some(true), Some(false), None, Some(true), Some(false), Some(true)
        ]);

        // Create sliced views with non-zero offsets
        let left_sliced = left.slice(1, 5); // Some(false), None, Some(true), Some(false), None
        let right_sliced = right.slice(2, 5); // Some(false), None, Some(true), Some(false), Some(true)

        // Test and
        let result_full = and(&left, &right).unwrap();
        let result_sliced = and(&left_sliced, &right_sliced).unwrap();

        let expected_full = BooleanArray::from(vec![
            None, Some(false), None, None, Some(false), None, Some(true)
        ]);
        let expected_sliced = BooleanArray::from(vec![
            Some(false), None, Some(true), Some(false), None
        ]);

        assert_eq!(result_full, expected_full);
        assert_eq!(result_sliced, expected_sliced);

        // Test or
        let result_full = or(&left, &right).unwrap();
        let result_sliced = or(&left_sliced, &right_sliced).unwrap();

        let expected_full = BooleanArray::from(vec![
            None, Some(true), None, None, Some(true), None, Some(true)
        ]);
        let expected_sliced = BooleanArray::from(vec![
            Some(false), None, Some(true), Some(false), None
        ]);

        assert_eq!(result_full, expected_full);
        assert_eq!(result_sliced, expected_sliced);

        // Test and_kleene: true if both true, false if either false, null otherwise
        let result_full = and_kleene(&left, &right).unwrap();
        let result_sliced = and_kleene(&left_sliced, &right_sliced).unwrap();

        let expected_full = BooleanArray::from(vec![
            None, Some(false), Some(false), None, Some(false), Some(false), Some(true)
        ]);
        let expected_sliced = BooleanArray::from(vec![
            Some(false), None, Some(true), Some(false), None
        ]);

        assert_eq!(result_full, expected_full);
        assert_eq!(result_sliced, expected_sliced);

        // Test or_kleene: false if both false, true if either true, null otherwise
        let result_full = or_kleene(&left, &right).unwrap();
        let result_sliced = or_kleene(&left_sliced, &right_sliced).unwrap();

        let expected_full = BooleanArray::from(vec![
            Some(true), Some(true), None, Some(true), Some(true), None, Some(true)
        ]);
        let expected_sliced = BooleanArray::from(vec![
            Some(false), None, Some(true), Some(false), Some(true)
        ]);

        assert_eq!(result_full, expected_full);
        assert_eq!(result_sliced, expected_sliced);

        // Test not
        let result_full = not(&left).unwrap();
        let result_sliced = not(&left_sliced).unwrap();

        let expected_full = BooleanArray::from(vec![
            Some(false), Some(true), None, Some(false), Some(true), None, Some(false)
        ]);
        let expected_sliced = BooleanArray::from(vec![
            Some(true), None, Some(false), Some(true), None
        ]);

        assert_eq!(result_full, expected_full);
        assert_eq!(result_sliced, expected_sliced);
    }

    #[test]
    fn test_boolean_kernels_zero_length_and_all_null() {
        // Empty arrays
        let empty = BooleanArray::from(Vec::<Option<bool>>::new());
        let result_and = and(&empty, &empty).unwrap();
        let result_or = or(&empty, &empty).unwrap();
        let result_not = not(&empty).unwrap();
        let result_and_kleene = and_kleene(&empty, &empty).unwrap();
        let result_or_kleene = or_kleene(&empty, &empty).unwrap();

        assert_eq!(result_and.len(), 0);
        assert_eq!(result_or.len(), 0);
        assert_eq!(result_not.len(), 0);
        assert_eq!(result_and_kleene.len(), 0);
        assert_eq!(result_or_kleene.len(), 0);

        // All-null arrays
        let all_null = BooleanArray::new_null(5);
        let result_and = and(&all_null, &all_null).unwrap();
        let result_or = or(&all_null, &all_null).unwrap();
        let result_not = not(&all_null).unwrap();
        let result_and_kleene = and_kleene(&all_null, &all_null).unwrap();
        let result_or_kleene = or_kleene(&all_null, &all_null).unwrap();

        assert_eq!(result_and, all_null);
        assert_eq!(result_or, all_null);
        assert_eq!(result_not, all_null);
        assert_eq!(result_and_kleene, all_null);
        assert_eq!(result_or_kleene, all_null);

        // Array with only first element non-null
        let partial = BooleanArray::from(vec![Some(true), None, None, None, None]);
        let result_not = not(&partial).unwrap();
        let expected_not = BooleanArray::from(vec![Some(false), None, None, None, None]);
        assert_eq!(result_not, expected_not);

        // Array with only last element non-null
        let partial = BooleanArray::from(vec![None, None, None, None, Some(false)]);
        let result_not = not(&partial).unwrap();
        let expected_not = BooleanArray::from(vec![None, None, None, None, Some(true)]);
        assert_eq!(result_not, expected_not);
    }

    // Helper functions for reference implementations
    fn ref_and_sql(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a & b),
            _ => None,
        }
    }

    fn ref_or_sql(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a | b),
            _ => None,
        }
    }

    fn ref_and_kleene(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a & b),
            (None, Some(b)) => if !b { Some(false) } else { None },
            (Some(a), None) => if !a { Some(false) } else { None },
            (None, None) => None,
        }
    }

    fn ref_or_kleene(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a | b),
            (None, Some(b)) => if b { Some(true) } else { None },
            (Some(a), None) => if a { Some(true) } else { None },
            (None, None) => None,
        }
    }

    fn ref_not(a: Option<bool>) -> Option<bool> {
        a.map(|x| !x)
    }

    #[test]
    fn test_boolean_kernels_random_equivalence() {
        use rand::{Rng, SeedableRng};

        // Use a fixed seed for reproducible tests
        let mut rng = rand::rngs::StdRng::from_seed([48u8; 32]);

        for _ in 0..20 { // 20 random iterations
            // Pick random length 1..64
            let len = rng.random_range(1..=64);

            // Generate random Vec<Option<bool>> for left and right
            let mut left_vec = Vec::with_capacity(len);
            let mut right_vec = Vec::with_capacity(len);
            for _ in 0..len {
                let is_null = rng.random_bool(0.2); // 20% chance of null
                let val = if is_null { None } else { Some(rng.random_bool(0.5)) };
                left_vec.push(val);
                let is_null = rng.random_bool(0.2);
                let val = if is_null { None } else { Some(rng.random_bool(0.5)) };
                right_vec.push(val);
            }

            // Construct BooleanArrays
            let left = BooleanArray::from(left_vec.clone());
            let right = BooleanArray::from(right_vec.clone());

            // Construct sliced variants if possible
            let (left_slice, right_slice) = if len > 1 {
                let slice_len = len - 1;
                (left.slice(1, slice_len), right.slice(1, slice_len))
            } else {
                (left.clone(), right.clone()) // fallback for len=1
            };

            // Test each kernel
            let kernels = vec![
                ("and", Box::new(|l: &BooleanArray, r: &BooleanArray| and(l, r).unwrap()) as Box<dyn Fn(&BooleanArray, &BooleanArray) -> BooleanArray>),
                ("or", Box::new(|l, r| or(l, r).unwrap())),
                ("and_kleene", Box::new(|l, r| and_kleene(l, r).unwrap())),
                ("or_kleene", Box::new(|l, r| or_kleene(l, r).unwrap())),
            ];

            for (name, kernel) in kernels {
                // Full arrays
                let result = kernel(&left, &right);
                let expected: Vec<Option<bool>> = left_vec.iter().zip(&right_vec).map(|(a, b)| match name {
                    "and" => ref_and_sql(*a, *b),
                    "or" => ref_or_sql(*a, *b),
                    "and_kleene" => ref_and_kleene(*a, *b),
                    "or_kleene" => ref_or_kleene(*a, *b),
                    _ => unreachable!(),
                }).collect();
                let result_vec: Vec<Option<bool>> = result.iter().collect();
                assert_eq!(result_vec, expected, "Full {} mismatch", name);

                // Sliced arrays
                if len > 1 {
                    let result_slice = kernel(&left_slice, &right_slice);
                    let expected_slice: Vec<Option<bool>> = left_vec[1..].iter().zip(&right_vec[1..]).map(|(a, b)| match name {
                        "and" => ref_and_sql(*a, *b),
                        "or" => ref_or_sql(*a, *b),
                        "and_kleene" => ref_and_kleene(*a, *b),
                        "or_kleene" => ref_or_kleene(*a, *b),
                        _ => unreachable!(),
                    }).collect();
                    let result_slice_vec: Vec<Option<bool>> = result_slice.iter().collect();
                    assert_eq!(result_slice_vec, expected_slice, "Sliced {} mismatch", name);
                }
            }

            // Test not separately
            let result_not = not(&left).unwrap();
            let expected_not: Vec<Option<bool>> = left_vec.iter().map(|a| ref_not(*a)).collect();
            let result_not_vec: Vec<Option<bool>> = result_not.iter().collect();
            assert_eq!(result_not_vec, expected_not, "Full not mismatch");

            if len > 1 {
                let result_not_slice = not(&left_slice).unwrap();
                let expected_not_slice: Vec<Option<bool>> = left_vec[1..].iter().map(|a| ref_not(*a)).collect();
                let result_not_slice_vec: Vec<Option<bool>> = result_not_slice.iter().collect();
                assert_eq!(result_not_slice_vec, expected_not_slice, "Sliced not mismatch");
            }
        }
    }

    #[test]
    fn test_boolean_array_byte_boundary_regressions() {
        // Test historically dangerous bitmap patterns for BooleanArray binary/unary operations
        // Construct BooleanArray from Vec<Option<bool>> with length 10: [T, F, None, T, F, None, T, F, None, T]
        // Underlying bitmap: bits for values and nulls
        let data = vec![Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true)];
        let array = BooleanArray::from(data.clone());

        // Slice cases: (slice_start, slice_len, description)
        let slice_cases = vec![
            (0, 5, "start=0, len=5"),
            (1, 4, "start=1, len=4 (offset+len=5)"),
            (3, 5, "start=3, len=5 (cross potential boundary)"),
            (5, 5, "start=5, len=5"),
        ];

        for (start, len, desc) in slice_cases {
            let slice = array.slice(start, len);
            let slice_data = &data[start..start+len];

            // Test unary NOT
            let result_not = slice.unary(|a| !a);
            let expected_not: Vec<Option<bool>> = slice_data.iter().map(|x| x.map(|b| !b)).collect();
            let result_not_vec: Vec<Option<bool>> = result_not.iter().collect();
            assert_eq!(result_not_vec, expected_not, "NOT {} mismatch", desc);

            // For binary, need another slice; use the same slice for simplicity, but with different op
            // Test binary AND with itself (should be identity for non-null)
            let result_and = slice.binary(&slice, |a, b| a & b).unwrap();
            let expected_and: Vec<Option<bool>> = slice_data.iter().map(|x| match x {
                Some(b) => Some(b & b),
                None => None,
            }).collect();
            let result_and_vec: Vec<Option<bool>> = result_and.iter().collect();
            assert_eq!(result_and_vec, expected_and, "AND self {} mismatch", desc);

            // Test binary OR with itself
            let result_or = slice.binary(&slice, |a, b| a | b).unwrap();
            let expected_or: Vec<Option<bool>> = slice_data.iter().map(|x| match x {
                Some(b) => Some(b | b),
                None => None,
            }).collect();
            let result_or_vec: Vec<Option<bool>> = result_or.iter().collect();
            assert_eq!(result_or_vec, expected_or, "OR self {} mismatch", desc);
        }
    }

    #[test]
    fn test_and_kleene_byte_boundary_regressions() {
        // Test and_kleene with slices that hit byte boundaries
        let left_data = vec![Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true)];
        let right_data = vec![Some(false), Some(true), Some(true), Some(false), Some(true), Some(false), Some(true), Some(false), Some(true), Some(false)];
        let left = BooleanArray::from(left_data.clone());
        let right = BooleanArray::from(right_data.clone());

        // Slice cases
        let slice_cases = vec![
            (0, 5),
            (1, 4),
            (3, 5),
            (5, 5),
        ];

        for (start, len) in slice_cases {
            let left_slice = left.slice(start, len);
            let right_slice = right.slice(start, len);
            let left_slice_data = &left_data[start..start+len];
            let right_slice_data = &right_data[start..start+len];

            let result = and_kleene(&left_slice, &right_slice).unwrap();
            let expected: Vec<Option<bool>> = left_slice_data.iter().zip(right_slice_data).map(|(a, b)| match (a, b) {
                (Some(a), Some(b)) => Some(a & b),
                (None, Some(b)) => if !b { Some(false) } else { None },
                (Some(a), None) => if !a { Some(false) } else { None },
                (None, None) => None,
            }).collect();
            let result_vec: Vec<Option<bool>> = result.iter().collect();
            assert_eq!(result_vec, expected, "and_kleene slice start={}, len={} mismatch", start, len);
        }
    }
}
