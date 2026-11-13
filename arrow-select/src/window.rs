// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines windowing functions, like `shift`ing

use crate::concat::concat;
use crate::take::take;
use arrow_array::{Array, ArrayRef, UInt64Array, make_array, new_null_array};
use arrow_schema::{ArrowError, DataType};
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
    } else if matches!(
        array.data_type(),
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
    ) {
        // No validity bitmap, so `take` decides where the nulls can go or errors.
        let indices = UInt64Array::from_iter((0..value_len).map(|index| {
            index
                .checked_sub(offset)
                .filter(|source| (0..value_len).contains(source))
                .map(|source| source as u64)
        }));
        take(array, &indices, None)
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
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{Float64Array, Int32Array, Int32DictionaryArray, RunArray, UnionArray};
    use arrow_schema::{Field, UnionFields};
    use std::sync::Arc;

    fn sparse_union(fields: Vec<Field>) -> UnionArray {
        let type_ids = (0..fields.len() as i8).collect::<Vec<_>>();
        let children = fields
            .iter()
            .map(|_| Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)
            .collect();
        let fields = UnionFields::try_new(type_ids, fields).unwrap();
        UnionArray::try_new(fields, vec![0, 0].into(), None, children).unwrap()
    }

    fn run_array(values_nullable: bool) -> ArrayRef {
        let run =
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2]), &Int32Array::from(vec![7]))
                .unwrap();
        let data_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, values_nullable)),
        );
        make_array(
            run.into_data()
                .into_builder()
                .data_type(data_type)
                .build()
                .unwrap(),
        )
    }

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

    #[test]
    fn test_shift_union_without_nullable_field() {
        let union = sparse_union(vec![Field::new("a", DataType::Int32, false)]);
        // Issue 10992: a kernel returns an error when no union field can hold the
        // new null. The message is the one `take` returns for null indices.
        for offset in [1, -1, 2] {
            let err = shift(&union, offset).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Compute error: Cannot take null indices from a union with no field that can represent nulls"
            );
        }
    }

    #[test]
    fn test_shift_union_puts_nulls_in_nullable_field() {
        let union = sparse_union(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);
        let res = shift(&union, 1).unwrap();
        let res = res.as_any().downcast_ref::<UnionArray>().unwrap();
        // Row 0 is vacated, so it selects "b" (type id 1), the only field that can
        // hold a null; row 1 keeps row 0 of the input, which selects "a" with value 1.
        assert_eq!(res.type_ids(), &[1, 0]);
        assert_eq!(res.child(0).null_count(), 0);
        assert!(res.logical_nulls().unwrap().is_null(0));
        assert_eq!(res.value(1).as_primitive::<Int32Type>().value(0), 1);
        res.to_data().validate_full().unwrap();
    }

    #[test]
    fn test_shift_run_end_encoded_non_nullable_values() {
        let run = run_array(false);
        // Issue 10992: a run-end encoded array whose values field is not nullable
        // cannot hold the new null, so `shift` returns the error `take` reports.
        for offset in [1, -1, 2] {
            let err = shift(run.as_ref(), offset).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Compute error: Cannot take null indices from a RunEndEncoded array with a non-nullable values field"
            );
        }
    }

    #[test]
    fn test_shift_run_end_encoded_nullable_values() {
        let res = shift(run_array(true).as_ref(), -1).unwrap();
        let res = res.as_run::<Int32Type>();
        // The input is [7, 7]; shifting left by one gives [7, null].
        let values = res.values().as_primitive::<Int32Type>();
        let logical = (0..res.len())
            .map(|i| {
                let physical = res.get_physical_index(i);
                values.is_valid(physical).then(|| values.value(physical))
            })
            .collect::<Vec<_>>();
        assert_eq!(logical, vec![Some(7), None]);
    }
}
