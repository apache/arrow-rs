// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Comparison kernels for `Array`s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.
//!

use arrow_array::cast::*;

use arrow_array::*;
use arrow_buffer::{BooleanBuffer, MutableBuffer, NullBuffer, bit_util};
use arrow_schema::ArrowError;

/// Checks if a [`GenericListArray`] contains a value in the [`PrimitiveArray`]
pub fn in_list<T, OffsetSize>(
    left: &PrimitiveArray<T>,
    right: &GenericListArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let mut bool_buf = MutableBuffer::try_from_len_zeroed(num_bytes)
        .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
    let bool_slice = bool_buf.as_slice_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
            let list = right.value(i);
            let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    break;
                }
            }
        }
    }

    let values = BooleanBuffer::new(bool_buf.into(), 0, left_len);
    Ok(BooleanArray::new(values, None))
}

/// Checks if a [`GenericListArray`] contains a value in the [`GenericStringArray`]
pub fn in_list_utf8<OffsetSize>(
    left: &GenericStringArray<OffsetSize>,
    right: &ListArray,
) -> Result<BooleanArray, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let mut bool_buf = MutableBuffer::try_from_len_zeroed(num_bytes)
        .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
    let bool_slice = &mut bool_buf;

    for i in 0..left_len {
        // contains(null, null) = false
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
            let list = right.value(i);
            let list = list.as_string::<OffsetSize>();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    break;
                }
            }
        }
    }
    let values = BooleanBuffer::new(bool_buf.into(), 0, left_len);
    Ok(BooleanArray::new(values, None))
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        ListBuilder, PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder,
    };
    use arrow_array::types::*;
    use arrow_buffer::{ArrowNativeType, Buffer, IntervalDayTime, IntervalMonthDayNano, i256};
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field};
    use half::f16;
    use rstest::rstest;

    use super::*;

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<T>` or `Vec<Option<T>>` where `T` is the native
    /// type of the data type of the Arrow array element.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_vec {
        ($KERNEL:path, $ARRAY:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = $ARRAY::from($A_VEC);
            let b = $ARRAY::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // slice and test if still works
            let a = a.slice(0, a.len());
            let b = b.slice(0, b.len());
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // test with a larger version of the same data to ensure we cover the chunked part of the comparison
            let mut a = vec![];
            let mut b = vec![];
            let mut e = vec![];
            for _i in 0..10 {
                a.extend($A_VEC);
                b.extend($B_VEC);
                e.extend($EXPECTED);
            }
            let a = $ARRAY::from(a);
            let b = $ARRAY::from(b);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from(e), c);
        };
    }

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64 {
        ($KERNEL:path, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            cmp_vec!($KERNEL, Int64Array, $A_VEC, $B_VEC, $EXPECTED);
        };
    }

    /// Evaluate `KERNEL` with one vectors and one scalar as inputs and assert against the expected output.
    /// `A_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64_scalar {
        ($KERNEL:path, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let b = Int64Array::new_scalar($B);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // test with a larger version of the same data to ensure we cover the chunked part of the comparison
            let mut a = vec![];
            let mut e = vec![];
            for _i in 0..10 {
                a.extend($A_VEC);
                e.extend($EXPECTED);
            }
            let a = Int64Array::from(a);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from(e), c);

        };
    }

    #[test]
    fn test_primitive_array_eq() {
        cmp_i64!(
            crate::cmp::eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            TimestampSecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            Time32SecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            Time32MillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            Time64MicrosecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            Time64NanosecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            crate::cmp::eq,
            IntervalYearMonthArray,
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                // 1 year
                IntervalYearMonthType::make_value(1, 0),
            ],
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(1, 2),
                // NB 12 months is treated as equal to a year (as the underlying
                // type stores number of months)
                IntervalYearMonthType::make_value(0, 12),
            ],
            vec![true, false, true]
        );

        cmp_vec!(
            crate::cmp::eq,
            IntervalMonthDayNanoArray,
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(3, 2, 1),
                // 1 month
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                IntervalMonthDayNanoType::make_value(1, 0, 0),
            ],
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                // 30 days is not treated as a month
                IntervalMonthDayNanoType::make_value(0, 30, 0),
                // 100 days
                IntervalMonthDayNanoType::make_value(0, 100, 0),
            ],
            vec![true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_scalar() {
        cmp_i64_scalar!(
            crate::cmp::eq,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_with_slice() {
        let a = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let b_slice = b.slice(5, 5);
        let d = crate::cmp::eq(&b_slice, &a).unwrap();
        assert!(d.value(0));
        assert!(d.value(1));
        assert!(d.value(2));
        assert!(!d.value(3));
        assert!(d.value(4));
    }

    #[test]
    fn test_primitive_array_eq_scalar_with_slice() {
        let a = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let a = a.slice(1, 3);
        let a_eq = crate::cmp::eq(&a, &Int32Array::new_scalar(2)).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
    }

    #[test]
    fn test_primitive_array_neq() {
        cmp_i64!(
            crate::cmp::neq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );

        cmp_vec!(
            crate::cmp::neq,
            TimestampMillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_neq_scalar() {
        cmp_i64_scalar!(
            crate::cmp::neq,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![Some(true), Some(false), Some(true), Some(false), None, None])]
    #[case::neq(crate::cmp::neq, vec![Some(false), Some(true), Some(false), Some(true), None, None])]
    #[case::lt(crate::cmp::lt, vec![Some(false), Some(true), Some(false), Some(false), None, None])]
    #[case::lt_eq(crate::cmp::lt_eq, vec![Some(true), Some(true), Some(true), Some(false), None, None])]
    #[case::gt(crate::cmp::gt, vec![Some(false), Some(false), Some(false), Some(true), None, None])]
    #[case::gt_eq(crate::cmp::gt_eq, vec![Some(true), Some(false), Some(true), Some(true), None, None])]
    fn test_boolean_array_comparison(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();
        let result: Vec<Option<bool>> = compare(&a, &b).unwrap().iter().collect();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::eq_false(crate::cmp::eq, false, vec![Some(false), Some(true), None])]
    #[case::eq_true(crate::cmp::eq, true, vec![Some(true), Some(false), None])]
    #[case::neq_false(crate::cmp::neq, false, vec![Some(true), Some(false), None])]
    #[case::neq_true(crate::cmp::neq, true, vec![Some(false), Some(true), None])]
    #[case::lt_false(crate::cmp::lt, false, vec![Some(false), Some(false), None])]
    #[case::lt_true(crate::cmp::lt, true, vec![Some(false), Some(true), None])]
    #[case::lt_eq_false(crate::cmp::lt_eq, false, vec![Some(false), Some(true), None])]
    #[case::lt_eq_true(crate::cmp::lt_eq, true, vec![Some(true), Some(true), None])]
    #[case::gt_false(crate::cmp::gt, false, vec![Some(true), Some(false), None])]
    #[case::gt_true(crate::cmp::gt, true, vec![Some(false), Some(false), None])]
    #[case::gt_eq_false(crate::cmp::gt_eq, false, vec![Some(true), Some(true), None])]
    #[case::gt_eq_true(crate::cmp::gt_eq, true, vec![Some(true), Some(false), None])]
    fn test_boolean_array_scalar_comparison(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] scalar: bool,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(scalar);
        let result: Vec<Option<bool>> = compare(&a, &b).unwrap().iter().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_primitive_array_lt() {
        cmp_i64!(
            crate::cmp::lt,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );

        cmp_vec!(
            crate::cmp::lt,
            TimestampMillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );

        cmp_vec!(
            crate::cmp::lt,
            IntervalDayTimeArray,
            vec![
                IntervalDayTimeType::make_value(1, 0),
                IntervalDayTimeType::make_value(0, 1000),
                IntervalDayTimeType::make_value(1, 1000),
                IntervalDayTimeType::make_value(1, 3000),
                // 90M milliseconds
                IntervalDayTimeType::make_value(0, 90_000_000),
            ],
            vec![
                IntervalDayTimeType::make_value(0, 1000),
                IntervalDayTimeType::make_value(1, 0),
                IntervalDayTimeType::make_value(10, 0),
                IntervalDayTimeType::make_value(2, 1),
                // NB even though 1 day is less than 90M milliseconds long,
                // it compares as greater because the underlying type stores
                // days and milliseconds as different fields
                IntervalDayTimeType::make_value(0, 12),
            ],
            vec![false, true, true, true ,false]
        );

        cmp_vec!(
            crate::cmp::lt,
            IntervalYearMonthArray,
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                IntervalYearMonthType::make_value(1, 2),
                // 1 year
                IntervalYearMonthType::make_value(1, 0),
            ],
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                // NB 12 months is treated as equal to a year (as the underlying
                // type stores number of months)
                IntervalYearMonthType::make_value(0, 12),
            ],
            vec![false, false, true, false]
        );

        cmp_vec!(
            crate::cmp::lt,
            IntervalMonthDayNanoArray,
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(3, 2, 1),
                // 1 month
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                IntervalMonthDayNanoType::make_value(1, 2, 0),
                IntervalMonthDayNanoType::make_value(1, 0, 0),
            ],
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(2, 0, 0),
                // 30 days is not treated as a month
                IntervalMonthDayNanoType::make_value(0, 30, 0),
                // 100 days (note is treated as greater than 1 month as the underlying integer representation)
                IntervalMonthDayNanoType::make_value(0, 100, 0),
            ],
            vec![false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar() {
        cmp_i64_scalar!(
            crate::cmp::lt,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_nulls() {
        cmp_i64!(
            crate::cmp::lt,
            vec![None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            vec![None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );

        cmp_vec!(
            crate::cmp::lt,
            TimestampMillisecondArray,
            vec![None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            vec![None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar_nulls() {
        cmp_i64_scalar!(
            crate::cmp::lt,
            vec![None, Some(1), Some(2), Some(3), None, Some(1), Some(2), Some(3), Some(2), None],
            2,
            vec![None, Some(true), Some(false), Some(false), None, Some(true), Some(false), Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq() {
        cmp_i64!(
            crate::cmp::lt_eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar() {
        cmp_i64_scalar!(
            crate::cmp::lt_eq,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_nulls() {
        cmp_i64!(
            crate::cmp::lt_eq,
            vec![None, None, Some(1), None, None, Some(1), None, None, Some(1)],
            vec![None, Some(1), Some(0), None, Some(1), Some(2), None, None, Some(3)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            crate::cmp::lt_eq,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt() {
        cmp_i64!(
            crate::cmp::gt,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar() {
        cmp_i64_scalar!(
            crate::cmp::gt,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_nulls() {
        cmp_i64!(
            crate::cmp::gt,
            vec![None, None, Some(1), None, None, Some(2), None, None, Some(3)],
            vec![None, Some(1), Some(1), None, Some(1), Some(1), None, Some(1), Some(1)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar_nulls() {
        cmp_i64_scalar!(
            crate::cmp::gt,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(false), Some(true), None, Some(false), Some(true), None, Some(false), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq() {
        cmp_i64!(
            crate::cmp::gt_eq,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar() {
        cmp_i64_scalar!(
            crate::cmp::gt_eq,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        cmp_i64!(
            crate::cmp::gt_eq,
            vec![None, None, Some(1), None, Some(1), Some(2), None, None, Some(1)],
            vec![None, Some(1), None, None, Some(1), Some(1), None, Some(2), Some(2)],
            vec![None, None, None, None, Some(true), Some(true), None, None, Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            crate::cmp::gt_eq,
            vec![None, Some(1), Some(2), None, Some(2), Some(3), None, Some(3), Some(4)],
            2,
            vec![None, Some(false), Some(true), None, Some(true), Some(true), None, Some(true), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_compare_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let b: Int32Array = (100..200).map(Some).collect();
        let b = b.slice(50, 50);
        let actual = crate::cmp::lt(&a, &b).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_compare_scalar_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let scalar = Int32Array::new_scalar(200);
        let actual = crate::cmp::lt(&a, &scalar).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_length_of_result_buffer() {
        // `item_count` is chosen to not be a multiple of the number of SIMD lanes for this
        // type (`Int8Type`), 64.
        let item_count = 130;

        let select_mask: BooleanArray = vec![true; item_count].into();

        let array_a: PrimitiveArray<Int8Type> = vec![1; item_count].into();
        let array_b: PrimitiveArray<Int8Type> = vec![2; item_count].into();
        let result_mask = crate::cmp::gt_eq(&array_a, &array_b).unwrap();

        assert_eq!(result_mask.values().len(), select_mask.values().len());
    }

    // Expected behaviour:
    // contains(1, [1, 2, null]) = true
    // contains(3, [1, 2, null]) = false
    // contains(null, [1, 2, null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains() {
        let value_data = Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            None,
            Some(7),
        ])
        .into_data();
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 6, 9]);
        let list_data_type =
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from([0b00001011])))
            .build()
            .unwrap();

        //  [[0, 1, 2], [3, 4, 5], null, [6, null, 7]]
        let list_array = LargeListArray::from(list_data);

        let nulls = Int32Array::from(vec![None, None, None, None]);
        let nulls_result = in_list(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = Int32Array::from(vec![Some(0), Some(0), Some(0), Some(0)]);
        let values_result = in_list(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, false, false, false]),
        );
    }

    #[test]
    fn test_interval_array() {
        let a = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 6)),
            Some(IntervalDayTime::new(4, 834)),
            None,
            Some(IntervalDayTime::new(2, 3)),
            None
        ]);
        let b = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(0, 4)),
            Some(IntervalDayTime::new(0, 6)),
            Some(IntervalDayTime::new(0, 834)),
            None,
            Some(IntervalDayTime::new(2, 3)),
            None
        ]);
        let res = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(
            &res,
            &BooleanArray::from(vec![Some(false), Some(true), Some(false), None, Some(true), None])
        );

        let a = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 0, 6)),
            Some(IntervalMonthDayNano::new(2, 0, 0)),
            Some(IntervalMonthDayNano::new(2, -5, 0)),
            None,
            Some(IntervalMonthDayNano::new(0, 0, 2)),
            Some(IntervalMonthDayNano::new(5, 0, -23)),
        ]);
        let b = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 0, 6)),
            Some(IntervalMonthDayNano::new(2, 3, 0)),
            Some(IntervalMonthDayNano::new(5, -5, 0)),
            None,
            Some(IntervalMonthDayNano::new(-1, 0, 2)),
            None,
        ]);
        let res = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(
            &res,
            &BooleanArray::from(vec![Some(false), Some(true), Some(true), None, Some(false), None])
        );

        let a =
            IntervalYearMonthArray::from(vec![Some(0), Some(623), Some(834), None, Some(3), None]);
        let b = IntervalYearMonthArray::from(
            vec![Some(86), Some(5), Some(834), Some(6), Some(86), None],
        );
        let res = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(
            &res,
            &BooleanArray::from(vec![Some(false), Some(true), Some(true), None, Some(false), None])
        );
    }

    macro_rules! test_binary {
        ($test_name:ident, $left:expr, $right:expr, $op:path, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = BinaryArray::from_vec($left);
                let right = BinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from_vec($left);
                let right = LargeBinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    #[test]
    fn test_binary_eq_scalar_on_slice() {
        let a = BinaryArray::from_opt_vec(vec![Some(b"hi"), None, Some(b"hello"), Some(b"world")]);
        let a = a.slice(1, 3);
        let a = as_generic_binary_array::<i32>(&a);
        let b = BinaryArray::new_scalar(b"hello");
        let a_eq = crate::cmp::eq(a, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
    }

    macro_rules! test_binary_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = BinaryArray::from_vec($left);
                let right = BinaryArray::new_scalar($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from_vec($left);
                let right = LargeBinaryArray::new_scalar($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    test_binary!(
        test_binary_array_eq,
        vec![b"arrow", b"arrow", b"arrow", b"arrow", &[0xff, 0xf8]],
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        crate::cmp::eq,
        vec![true, false, false, false, true]
    );

    test_binary_scalar!(
        test_binary_array_eq_scalar,
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        b"arrow",
        crate::cmp::eq,
        vec![true, false, false, false, false]
    );

    test_binary!(
        test_binary_array_neq,
        vec![b"arrow", b"arrow", b"arrow", b"arrow", &[0xff, 0xf8]],
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf9]],
        crate::cmp::neq,
        vec![false, true, true, true, true]
    );
    test_binary_scalar!(
        test_binary_array_neq_scalar,
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        b"arrow",
        crate::cmp::neq,
        vec![false, true, true, true, true]
    );

    test_binary!(
        test_binary_array_lt,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf9]],
        crate::cmp::lt,
        vec![true, true, false, false, true]
    );
    test_binary_scalar!(
        test_binary_array_lt_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        b"flight",
        crate::cmp::lt,
        vec![true, true, false, false, false]
    );

    test_binary!(
        test_binary_array_lt_eq,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8, 0xf9]],
        crate::cmp::lt_eq,
        vec![true, true, true, false, true]
    );
    test_binary_scalar!(
        test_binary_array_lt_eq_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        b"flight",
        crate::cmp::lt_eq,
        vec![true, true, true, false, false]
    );

    test_binary!(
        test_binary_array_gt,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf9]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8]],
        crate::cmp::gt,
        vec![false, false, false, true, true]
    );
    test_binary_scalar!(
        test_binary_array_gt_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        b"flight",
        crate::cmp::gt,
        vec![false, false, false, true, true]
    );

    test_binary!(
        test_binary_array_gt_eq,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8]],
        crate::cmp::gt_eq,
        vec![false, false, true, true, true]
    );
    test_binary_scalar!(
        test_binary_array_gt_eq_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        b"flight",
        crate::cmp::gt_eq,
        vec![false, false, true, true, true]
    );

    // Expected behaviour:
    // contains("ab", ["ab", "cd", null]) = true
    // contains("ef", ["ab", "cd", null]) = false
    // contains(null, ["ab", "cd", null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains_utf8() {
        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_value("Lorem");
        builder.values().append_value("ipsum");
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value("sit");
        builder.values().append_value("amet");
        builder.values().append_value("Lorem");
        builder.append(true);
        builder.append(false);
        builder.values().append_value("ipsum");
        builder.append(true);

        //  [["Lorem", "ipsum", null], ["sit", "amet", "Lorem"], null, ["ipsum"]]
        // value_offsets = [0, 3, 6, 6]
        let list_array = builder.finish();

        let v: Vec<Option<&str>> = vec![None, None, None, None];
        let nulls = StringArray::from(v);
        let nulls_result = in_list_utf8(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = StringArray::from(vec![
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
        ]);
        let values_result = in_list_utf8(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, true, false, false]),
        );
    }

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_utf8_view {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringViewArray::from_iter_values($left);
                let right = StringViewArray::from_iter_values($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    #[test]
    fn test_utf8_eq_scalar_on_slice() {
        let a = StringArray::from(vec![Some("hi"), None, Some("hello"), Some("world"), Some("")]);
        let a = a.slice(1, 4);
        let scalar = StringArray::new_scalar("hello");
        let a_eq = crate::cmp::eq(&a, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false), Some(false)])
        );

        let scalar = StringArray::new_scalar("");
        let a_eq2 = crate::cmp::eq(&a, &scalar).unwrap();

        assert_eq!(
            a_eq2,
            BooleanArray::from(vec![None, Some(false), Some(false), Some(true)])
        );
    }

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::new_scalar($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::new_scalar($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
        ($test_name:ident, $test_name_dyn:ident, $left:expr, $right:expr, $op:expr, $op_dyn:expr, $expected:expr) => {
            test_utf8_scalar!($test_name, $left, $right, $op, $expected);
            test_utf8_scalar!($test_name_dyn, $left, $right, $op_dyn, $expected);
        };
    }

    macro_rules! test_utf8_view_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringViewArray::from_iter_values($left);
                let right = StringViewArray::new_scalar($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
    }

    const LARGE_1: &str = "prefix-larger than 12 bytes string";
    const LARGE_2: &str = "prefix-larger but different string";
    const SMALL_1: &str = "pref1";
    const SMALL_2: &str = "pref2";
    /// Below are two carefully crafted arrays that compares:
    /// (1) 2 large strings that have the same first 4 bytes but are different in the remaining bytes
    /// (2) 1 large and 1 small string that are the same in the first 4 bytes but different in the remaining bytes
    /// (3) 2 small strings that are the same in the first 4 byes
    const TEST_ARRAY_1: [&str; 5] = [LARGE_1, LARGE_1, SMALL_1, SMALL_1, LARGE_1];
    const TEST_ARRAY_2: [&str; 5] = [LARGE_1, LARGE_2, SMALL_1, SMALL_2, SMALL_1];

    test_utf8!(
        test_utf8_array_eq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        crate::cmp::eq,
        [true, false, false, false]
    );
    test_utf8_view!(
        test_utf8_view_array_eq,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::eq,
        [true, false, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_eq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        crate::cmp::eq,
        [true, false, false, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_eq_large_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::eq,
        [true, false, false, false, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_eq_small_scalar,
        TEST_ARRAY_2,
        SMALL_1,
        crate::cmp::eq,
        [false, false, true, false, true]
    );

    test_utf8!(
        test_utf8_array_neq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        crate::cmp::neq,
        [false, true, true, true]
    );
    test_utf8_view!(
        test_utf8_view_array_neq,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::neq,
        [false, true, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_neq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        crate::cmp::neq,
        [false, true, true, true]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_neq_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::neq,
        [false, true, true, true, true]
    );

    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        crate::cmp::lt,
        [true, true, false, false]
    );
    test_utf8_view!(
        test_utf8_view_array_lt,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::lt,
        [false, false, false, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        crate::cmp::lt,
        [true, true, false, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_lt_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::lt,
        [false, true, true, true, true]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_lt_scalar_small,
        TEST_ARRAY_2,
        SMALL_1,
        crate::cmp::lt,
        [false, false, false, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        crate::cmp::lt_eq,
        [true, true, true, false]
    );
    test_utf8_view!(
        test_utf8_view_array_lt_eq,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::lt_eq,
        [true, false, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        crate::cmp::lt_eq,
        [true, true, true, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_lt_eq_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::lt_eq,
        [true, true, true, true, true]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        crate::cmp::gt,
        [false, false, false, true]
    );
    test_utf8_view!(
        test_utf8_view_array_gt,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::gt,
        [false, true, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        crate::cmp::gt,
        [false, false, false, true]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_gt_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::gt,
        [false, false, false, false, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_gt_scalar_small,
        TEST_ARRAY_2,
        SMALL_1,
        crate::cmp::gt,
        [true, true, false, true, false]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        crate::cmp::gt_eq,
        [false, false, true, true]
    );
    test_utf8_view!(
        test_utf8_view_array_gt_eq,
        TEST_ARRAY_1,
        TEST_ARRAY_2,
        crate::cmp::gt_eq,
        [true, true, true, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        crate::cmp::gt_eq,
        [false, false, true, true]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_gt_eq_scalar,
        TEST_ARRAY_2,
        LARGE_1,
        crate::cmp::gt_eq,
        [true, false, false, false, false]
    );
    test_utf8_view_scalar!(
        test_utf8_view_array_gt_eq_scalar_small,
        TEST_ARRAY_2,
        SMALL_1,
        crate::cmp::gt_eq,
        [true, true, true, true, true]
    );

    #[rstest]
    #[case::eq(crate::cmp::eq, BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)]))]
    #[case::neq(crate::cmp::neq, BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(true)]))]
    #[case::lt(crate::cmp::lt, BooleanArray::from(vec![true, true,false,false,false]))]
    #[case::lt_eq(crate::cmp::lt_eq, BooleanArray::from(vec![true,true,true,true,false]))]
    #[case::gt(crate::cmp::gt, BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(false), Some(true)]))]
    #[case::gt_eq(crate::cmp::gt_eq, BooleanArray::from(vec![false,false,true,true,true]))]
    fn test_dyn_scalar(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: BooleanArray,
    ) {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let scalar = Int32Array::new_scalar(8);
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)]))]
    #[case::neq(crate::cmp::neq, BooleanArray::from(vec![true,true,false,false,true]))]
    #[case::lt(crate::cmp::lt, BooleanArray::from(vec![true,true,false,false,false]))]
    #[case::lt_eq(crate::cmp::lt_eq, BooleanArray::from(vec![true, true,true,true,false]))]
    #[case::gt(crate::cmp::gt, BooleanArray::from(vec![false,false,false,false,true]))]
    #[case::gt_eq(crate::cmp::gt_eq, BooleanArray::from(vec![false, false,true,true,true]))]
    fn test_dyn_scalar_float(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: BooleanArray,
    ) {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float32Array::new_scalar(8.);
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let scalar = Float64Array::new_scalar(8.);
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, Int8Type {}, 123, 123, BooleanArray::from(vec![Some(true), None, Some(false)]))]
    #[case::neq(crate::cmp::neq, Int32Type {}, 22, 23, BooleanArray::from(vec![Some(true), None, Some(false)]))]
    #[case::lt(crate::cmp::lt, Int8Type {}, 123, 123, BooleanArray::from(vec![Some(false), None, Some(true)]))]
    #[case::lt_eq(crate::cmp::lt_eq, Int8Type {}, 123, 23, BooleanArray::from(vec![Some(false), None, Some(true)]))]
    #[case::gt(crate::cmp::gt, Int8Type {}, 123, 23, BooleanArray::from(vec![Some(true), None, Some(false)]))]
    #[case::gt_eq(crate::cmp::gt_eq, Int8Type {}, 22, 23, BooleanArray::from(vec![Some(false), None, Some(true)]))]
    fn test_dyn_scalar_with_dict<K: ArrowDictionaryKeyType>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] scalar_key_type: K,
        #[case] first_value: i32,
        #[case] scalar_value: i32,
        #[case] expected: BooleanArray,
    ) {
        let _ = scalar_key_type;
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(first_value).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<K>::new_scalar(Int32Array::new_scalar(scalar_value));
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
    }

    fn test_primitive_dyn_scalar<T: ArrowPrimitiveType>(array: PrimitiveArray<T>) {
        let b = PrimitiveArray::<T>::new_scalar(T::Native::usize_as(8));
        let a_eq = crate::cmp::eq(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), None, Some(false)])
        );

        let a_eq = crate::cmp::gt_eq(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), None, Some(true)])
        );

        let a_eq = crate::cmp::gt(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(false), None, Some(true)])
        );

        let a_eq = crate::cmp::lt_eq(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(true), None, Some(false)])
        );

        let a_eq = crate::cmp::lt(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), None, Some(false)])
        );
    }

    #[test]
    fn test_timestamp_dyn_scalar() {
        let array = TimestampSecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampNanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_date32_dyn_scalar() {
        let array = Date32Array::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_date64_dyn_scalar() {
        let array = Date64Array::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_time32_dyn_scalar() {
        let array = Time32SecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = Time32MillisecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_time64_dyn_scalar() {
        let array = Time64MicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = Time64NanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_interval_dyn_scalar() {
        let array = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(1, 0)),
            None,
            Some(IntervalDayTime::new(8, 0)),
            None,
            Some(IntervalDayTime::new(10, 0)),
        ]);
        test_primitive_dyn_scalar(array);

        let array = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 0, 0)),
            None,
            Some(IntervalMonthDayNano::new(8, 0, 0)),
            None,
            Some(IntervalMonthDayNano::new(10, 0, 0)),
        ]);
        test_primitive_dyn_scalar(array);

        let array = IntervalYearMonthArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_duration_dyn_scalar() {
        let array = DurationSecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationMillisecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationNanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, b"flight", vec![Some(false), Some(false), Some(true), Some(false), Some(false), None])]
    #[case::neq(crate::cmp::neq, b"flight", vec![Some(true), Some(true), Some(false), Some(true), Some(true), None])]
    #[case::lt(crate::cmp::lt, b"flight", vec![Some(true), Some(true), Some(false), Some(false), Some(false), None])]
    #[case::lt_eq(crate::cmp::lt_eq, b"flight", vec![Some(true), Some(true), Some(true), Some(false), Some(false), None])]
    #[case::gt(crate::cmp::gt, b"flight", vec![Some(false), Some(false), Some(false), Some(true), Some(true), None])]
    #[case::gt_eq(crate::cmp::gt_eq, b"\xff\xf8", vec![Some(false), Some(false), Some(false), Some(false), Some(true), None])]
    fn test_dyn_binary_scalar<O: OffsetSizeTrait>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] scalar: &[u8],
        #[case] expected: Vec<Option<bool>>,
        #[values(0_i32, 0_i64)] offset_type: O,
    ) {
        let _ = offset_type;
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = GenericBinaryArray::<O>::from(data);
        let scalar = GenericBinaryArray::<O>::new_scalar(scalar);
        let expected = BooleanArray::from(expected);
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![Some(false), Some(false), Some(false), Some(true)])]
    #[case::neq(crate::cmp::neq, vec![Some(true), Some(true), Some(true), Some(false)])]
    fn test_dyn_fixed_size_binary_scalar(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let array = FixedSizeBinaryArray::try_from(vec![&[0u8], &[0u8], &[0u8], &[1u8]]).unwrap();
        let scalar = FixedSizeBinaryArray::new_scalar([1u8]);
        let expected = BooleanArray::from(expected);
        assert_eq!(compare(&array, &scalar).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, "xyz", vec![Some(false), Some(false), Some(true)])]
    #[case::neq(crate::cmp::neq, "xyz", vec![Some(true), Some(true), Some(false)])]
    #[case::lt(crate::cmp::lt, "xyz", vec![Some(true), Some(true), Some(false)])]
    #[case::lt_eq(crate::cmp::lt_eq, "def", vec![Some(true), Some(true), Some(false)])]
    #[case::gt(crate::cmp::gt, "def", vec![Some(false), Some(false), Some(true)])]
    #[case::gt_eq(crate::cmp::gt_eq, "def", vec![Some(false), Some(true), Some(true)])]
    fn test_dyn_utf8_scalar(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] scalar: &str,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar(scalar);
        let actual = compare(&array, &scalar).unwrap();
        assert_eq!(actual, BooleanArray::from(expected));
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, "abc", vec![Some(false), None, Some(true), Some(true), Some(false)])]
    #[case::neq(crate::cmp::neq, "abc", vec![Some(true), None, Some(false), Some(false), Some(true)])]
    #[case::lt(crate::cmp::lt, "abc", vec![Some(true), None, Some(false), Some(false), Some(true)])]
    #[case::lt_eq(crate::cmp::lt_eq, "xyz", vec![Some(true), None, Some(true), Some(true), Some(false)])]
    #[case::gt(crate::cmp::gt, "xyz", vec![Some(false), None, Some(false), Some(false), Some(true)])]
    #[case::gt_eq(crate::cmp::gt_eq, "xyz", vec![Some(false), None, Some(true), Some(true), Some(true)])]
    fn test_dyn_utf8_scalar_with_dict(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] last_value: &str,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append(last_value).unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let actual = compare(&array, &scalar).unwrap();
        assert_eq!(actual, BooleanArray::from(expected));
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        BooleanArray::from(vec![true, false, true]),
        vec![Some(false), Some(true), Some(false)]
    )]
    #[case::neq(
        crate::cmp::neq,
        BooleanArray::from(vec![true, false, true]),
        vec![Some(true), Some(false), Some(true)]
    )]
    #[case::lt(
        crate::cmp::lt,
        BooleanArray::from(vec![Some(true), Some(false), Some(true), None]),
        vec![Some(false), Some(false), Some(false), None]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        BooleanArray::from(vec![true, false, true]),
        vec![Some(false), Some(true), Some(false)]
    )]
    #[case::gt(
        crate::cmp::gt,
        BooleanArray::from(vec![true, false, true]),
        vec![Some(true), Some(false), Some(true)]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        BooleanArray::from(vec![true, false, true]),
        vec![Some(true), Some(true), Some(true)]
    )]
    fn test_dyn_bool_scalar(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] array: BooleanArray,
        #[case] expected: Vec<Option<bool>>,
    ) {
        let scalar = BooleanArray::new_scalar(false);
        let actual = compare(&array, &scalar).unwrap();
        assert_eq!(actual, BooleanArray::from(expected));
    }

    #[test]
    fn test_eq_dyn_neq_dyn_fixed_size_binary() {
        let values1: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x01])];
        let values2: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x00])];

        let array1 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values1.into_iter(), 2).unwrap();
        let array2 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values2.into_iter(), 2).unwrap();

        let result = crate::cmp::eq(&array1, &array2).unwrap();
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false)]),
            result
        );

        let result = crate::cmp::neq(&array1, &array2).unwrap();
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true)]),
            result
        );
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, [2_i8, 3, 4], [2_i8, 4, 4], vec![true, false, true])]
    #[case::neq(crate::cmp::neq, [2_i8, 3, 4], [2_i8, 4, 4], vec![false, true, false])]
    #[case::lt(crate::cmp::lt, [3_i8, 4, 4], [4_i8, 3, 4], vec![true, false, false])]
    #[case::lt_eq(crate::cmp::lt_eq, [3_i8, 4, 4], [4_i8, 3, 4], vec![true, false, true])]
    #[case::gt(crate::cmp::gt, [3_i8, 4, 4], [4_i8, 3, 4], vec![false, true, false])]
    #[case::gt_eq(crate::cmp::gt_eq, [3_i8, 4, 4], [4_i8, 3, 4], vec![false, true, true])]
    fn test_dyn_dictionary_i8_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] left_keys: [i8; 3],
        #[case] right_keys: [i8; 3],
        #[case] expected: Vec<bool>,
    ) {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = Int8Array::from_iter_values(left_keys);
        let keys2 = Int8Array::from_iter_values(right_keys);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        assert_eq!(
            compare(&dict_array1, &dict_array2).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[rstest]
    #[case::uint64(
        Arc::new(UInt64Array::from_iter_values([10_u64, 11, 12, 13, 14, 15, 16, 17])),
        [1_u64, 3, 4],
        [2_u64, 3, 5],
        vec![false, true, false],
        vec![true, false, true]
    )]
    #[case::binary(
        Arc::new(
            ["hello", "", "parquet"]
                .into_iter()
                .map(|b| Some(b.as_bytes()))
                .collect::<BinaryArray>()
        ),
        [0_u64, 1, 2],
        [0_u64, 2, 1],
        vec![true, false, false],
        vec![false, true, true]
    )]
    #[case::interval_day_time(
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 6)),
            Some(IntervalDayTime::new(4, 10)),
        ])),
        [1_u64, 0, 3],
        [2_u64, 1, 3],
        vec![false, true, true],
        vec![true, false, false]
    )]
    #[case::date32(
        Arc::new(Date32Array::from(vec![1, 6, 10, 2, 3, 5])),
        [1_u64, 0, 3],
        [2_u64, 0, 3],
        vec![false, true, true],
        vec![true, false, false]
    )]
    fn test_dyn_dictionary_uint64_keys_eq_neq(
        #[case] values: ArrayRef,
        #[case] left_keys: [u64; 3],
        #[case] right_keys: [u64; 3],
        #[case] expected_eq: Vec<bool>,
        #[case] expected_neq: Vec<bool>,
    ) {
        let keys1 = UInt64Array::from_iter_values(left_keys);
        let keys2 = UInt64Array::from_iter_values(right_keys);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        assert_eq!(
            crate::cmp::eq(&dict_array1, &dict_array2).unwrap(),
            BooleanArray::from(expected_eq)
        );
        assert_eq!(
            crate::cmp::neq(&dict_array1, &dict_array2).unwrap(),
            BooleanArray::from(expected_neq)
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_utf8_array() {
        let test1 = ["a", "a", "b", "c"];
        let test2 = ["a", "b", "b", "c"];

        let dict_array1: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        let dict_array2: DictionaryArray<Int8Type> = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, [1_u64, 1, 1], [0_u64, 1, 0], vec![false, true, false])]
    #[case::neq(crate::cmp::neq, [1_u64, 1, 1], [0_u64, 1, 0], vec![true, false, true])]
    #[case::lt(crate::cmp::lt, [1_u64, 1, 0], [0_u64, 1, 1], vec![true, false, false])]
    #[case::lt_eq(crate::cmp::lt_eq, [1_u64, 1, 0], [0_u64, 1, 1], vec![true, true, false])]
    #[case::gt(crate::cmp::gt, [1_u64, 1, 0], [0_u64, 1, 1], vec![false, false, true])]
    #[case::gt_eq(crate::cmp::gt_eq, [1_u64, 1, 0], [0_u64, 1, 1], vec![false, true, true])]
    fn test_dyn_dictionary_bool_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] left_keys: [u64; 3],
        #[case] right_keys: [u64; 3],
        #[case] expected: Vec<bool>,
    ) {
        let values = BooleanArray::from(vec![true, false]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values(left_keys);
        let keys2 = UInt64Array::from_iter_values(right_keys);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        assert_eq!(
            compare(&dict_array1, &dict_array2).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        [Some(12_i8), None, Some(14)],
        vec![Some(true), None, Some(true)],
        vec![Some(true), None, Some(true)]
    )]
    #[case::neq(
        crate::cmp::neq,
        [Some(12_i8), None, Some(14)],
        vec![Some(false), None, Some(false)],
        vec![Some(false), None, Some(false)]
    )]
    #[case::lt(
        crate::cmp::lt,
        [Some(12_i8), None, Some(11)],
        vec![Some(false), None, Some(false)],
        vec![Some(false), None, Some(true)]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        [Some(12_i8), None, Some(11)],
        vec![Some(true), None, Some(false)],
        vec![Some(true), None, Some(true)]
    )]
    #[case::gt(
        crate::cmp::gt,
        [Some(12_i8), None, Some(11)],
        vec![Some(false), None, Some(true)],
        vec![Some(false), None, Some(false)]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        [Some(12_i8), None, Some(11)],
        vec![Some(true), None, Some(true)],
        vec![Some(true), None, Some(false)]
    )]
    fn test_dyn_dictionary_to_int8_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] array_values: [Option<i8>; 3],
        #[case] expected_dictionary_left: Vec<Option<bool>>,
        #[case] expected_dictionary_right: Vec<Option<bool>>,
    ) {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = Int8Array::from_iter(array_values);

        assert_eq!(
            compare(&dict_array, &array).unwrap(),
            BooleanArray::from(expected_dictionary_left)
        );
        assert_eq!(
            compare(&array, &dict_array).unwrap(),
            BooleanArray::from(expected_dictionary_right)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 10.0],
        vec![f32::NAN, f32::NAN, 8.0, 8.0, 10.0],
        vec![true, false, true, true, true]
    )]
    #[case::neq(
        crate::cmp::neq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 10.0],
        vec![f32::NAN, f32::NAN, 8.0, 8.0, 10.0],
        vec![false, true, false, false, false]
    )]
    #[case::lt(
        crate::cmp::lt,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0],
        vec![false, true, false, true, false, false]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0],
        vec![true, true, true, true, false, false]
    )]
    #[case::gt(
        crate::cmp::gt,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0],
        vec![false, false, false, false, true, true]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0],
        vec![true, false, true, false, true, true]
    )]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_dyn_float_nan<A: Array>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] left: Vec<f32>,
        #[case] right: Vec<f32>,
        #[case] expected: Vec<bool>,
        #[values(
            |values: Vec<f32>| Float16Array::from_iter_values(values.into_iter().map(f16::from_f32)),
            Float32Array::from,
            |values: Vec<f32>| Float64Array::from_iter_values(values.into_iter().map(f64::from))
        )]
        make_array: fn(Vec<f32>) -> A,
    ) {
        let left = make_array(left);
        let right = make_array(right);
        assert_eq!(
            compare(&left, &right).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        vec![true, false, false, false, false]
    )]
    #[case::neq(
        crate::cmp::neq,
        vec![false, true, true, true, true]
    )]
    #[case::lt(
        crate::cmp::lt,
        vec![false, true, true, true, true]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        vec![true, true, true, true, true]
    )]
    #[case::gt(
        crate::cmp::gt,
        vec![false, false, false, false, false]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        vec![true, false, false, false, false]
    )]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_dyn_scalar_float_nan<A: Array>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<bool>,
        #[values(
            |values: Vec<f32>| Float16Array::from_iter_values(values.into_iter().map(f16::from_f32)),
            Float32Array::from,
            |values: Vec<f32>| Float64Array::from_iter_values(values.into_iter().map(f64::from))
        )]
        make_array: fn(Vec<f32>) -> A,
    ) {
        let array = make_array(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Scalar::new(make_array(vec![f32::NAN]));
        assert_eq!(
            compare(&array, &scalar).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        ["a", "a", "b", "c"],
        ["a", "b", "b", "d"],
        vec![Some(true), None, None, Some(false)],
        vec![Some(true), None, None, Some(false)]
    )]
    #[case::neq(
        crate::cmp::neq,
        ["a", "a", "b", "c"],
        ["a", "b", "b", "d"],
        vec![Some(false), None, None, Some(true)],
        vec![Some(false), None, None, Some(true)]
    )]
    #[case::lt(
        crate::cmp::lt,
        ["abc", "abc", "b", "cde"],
        ["abc", "b", "b", "def"],
        vec![Some(false), None, None, Some(true)],
        vec![Some(false), None, None, Some(false)]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        ["abc", "abc", "b", "cde"],
        ["abc", "b", "b", "def"],
        vec![Some(true), None, None, Some(true)],
        vec![Some(true), None, None, Some(false)]
    )]
    #[case::gt(
        crate::cmp::gt,
        ["abc", "abc", "b", "cde"],
        ["abc", "b", "b", "def"],
        vec![Some(false), None, None, Some(false)],
        vec![Some(false), None, None, Some(true)]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        ["abc", "abc", "b", "cde"],
        ["abc", "b", "b", "def"],
        vec![Some(true), None, None, Some(false)],
        vec![Some(true), None, None, Some(true)]
    )]
    fn test_dyn_dictionary_to_utf8_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] dictionary_values: [&str; 4],
        #[case] array_values: [&str; 4],
        #[case] expected_dictionary_left: Vec<Option<bool>>,
        #[case] expected_dictionary_right: Vec<Option<bool>>,
    ) {
        let dict_array: DictionaryArray<Int8Type> = dictionary_values
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let array: StringArray = array_values
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        assert_eq!(
            compare(&dict_array, &array).unwrap(),
            BooleanArray::from(expected_dictionary_left)
        );
        assert_eq!(
            compare(&array, &dict_array).unwrap(),
            BooleanArray::from(expected_dictionary_right)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        vec![Some(true), None, Some(true), Some(false)],
        vec![Some(true), None, Some(true), Some(false)]
    )]
    #[case::neq(
        crate::cmp::neq,
        vec![Some(false), None, Some(false), Some(true)],
        vec![Some(false), None, Some(false), Some(true)]
    )]
    #[case::lt(
        crate::cmp::lt,
        vec![Some(false), None, Some(false), Some(true)],
        vec![Some(false), None, Some(false), Some(false)]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        vec![Some(true), None, Some(true), Some(true)],
        vec![Some(true), None, Some(true), Some(false)]
    )]
    #[case::gt(
        crate::cmp::gt,
        vec![Some(false), None, Some(false), Some(false)],
        vec![Some(false), None, Some(false), Some(true)]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        vec![Some(true), None, Some(true), Some(false)],
        vec![Some(true), None, Some(true), Some(true)]
    )]
    fn test_dyn_dictionary_to_binary_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected_dictionary_left: Vec<Option<bool>>,
        #[case] expected_dictionary_right: Vec<Option<bool>>,
    ) {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys = UInt64Array::from(vec![Some(0_u64), None, Some(2), Some(2)]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array: BinaryArray = ["hello", "", "parquet", "test"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        assert_eq!(
            compare(&dict_array, &array).unwrap(),
            BooleanArray::from(expected_dictionary_left)
        );
        assert_eq!(
            compare(&array, &dict_array).unwrap(),
            BooleanArray::from(expected_dictionary_right)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 10.0],
        vec![f32::NAN, 8.0, 10.0],
        [0_i8, 0, 1, 1, 2].to_vec(),
        vec![true, false, true, true, true]
    )]
    #[case::neq(
        crate::cmp::neq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 10.0],
        vec![f32::NAN, 8.0, 10.0],
        [0_i8, 0, 1, 1, 2].to_vec(),
        vec![false, true, false, false, false]
    )]
    #[case::lt(
        crate::cmp::lt,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, 8.0, 9.0, 10.0, 1.0],
        [0_i8, 0, 1, 2, 3, 4].to_vec(),
        vec![false, true, false, true, false, false]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, 8.0, 9.0, 10.0, 1.0],
        [0_i8, 0, 1, 2, 3, 4].to_vec(),
        vec![true, true, true, true, false, false]
    )]
    #[case::gt(
        crate::cmp::gt,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, 8.0, 9.0, 10.0, 1.0],
        [0_i8, 0, 1, 2, 3, 4].to_vec(),
        vec![false, false, false, false, true, true]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN],
        vec![f32::NAN, 8.0, 9.0, 10.0, 1.0],
        [0_i8, 0, 1, 2, 3, 4].to_vec(),
        vec![true, false, true, false, true, true]
    )]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_dyn_dict_non_dict_float_nan<A: Array + 'static>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] left: Vec<f32>,
        #[case] values: Vec<f32>,
        #[case] keys: Vec<i8>,
        #[case] expected: Vec<bool>,
        #[values(
            |values: Vec<f32>| Float16Array::from_iter_values(values.into_iter().map(f16::from_f32)),
            Float32Array::from,
            |values: Vec<f32>| Float64Array::from_iter_values(values.into_iter().map(f64::from))
        )]
        make_array: fn(Vec<f32>) -> A,
    ) {
        let array = make_array(left);
        let dictionary = DictionaryArray::new(
            Int8Array::from_iter_values(keys),
            Arc::new(make_array(values)),
        );
        assert_eq!(
            compare(&array, &dictionary).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[rstest]
    #[case::eq(
        crate::cmp::eq,
        vec![Some(true), None, None, Some(false)],
        vec![Some(true), None, None, Some(false)]
    )]
    #[case::neq(
        crate::cmp::neq,
        vec![Some(false), None, None, Some(true)],
        vec![Some(false), None, None, Some(true)]
    )]
    #[case::lt(
        crate::cmp::lt,
        vec![Some(false), None, None, Some(true)],
        vec![Some(false), None, None, Some(false)]
    )]
    #[case::lt_eq(
        crate::cmp::lt_eq,
        vec![Some(true), None, None, Some(true)],
        vec![Some(true), None, None, Some(false)]
    )]
    #[case::gt(
        crate::cmp::gt,
        vec![Some(false), None, None, Some(false)],
        vec![Some(false), None, None, Some(true)]
    )]
    #[case::gt_eq(
        crate::cmp::gt_eq,
        vec![Some(true), None, None, Some(false)],
        vec![Some(true), None, None, Some(true)]
    )]
    fn test_dyn_dictionary_to_boolean_array(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected_dictionary_left: Vec<Option<bool>>,
        #[case] expected_dictionary_right: Vec<Option<bool>>,
    ) {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = BooleanArray::from(test2);

        assert_eq!(
            compare(&dict_array, &array).unwrap(),
            BooleanArray::from(expected_dictionary_left)
        );
        assert_eq!(
            compare(&array, &dict_array).unwrap(),
            BooleanArray::from(expected_dictionary_right)
        );
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![false, false, false, true, true, false])]
    #[case::lt(crate::cmp::lt, vec![true, true, false, false, false, true])]
    #[case::lt_eq(crate::cmp::lt_eq, vec![true, true, false, true, true, true])]
    #[case::gt(crate::cmp::gt, vec![false, false, true, false, false, false])]
    #[case::gt_eq(crate::cmp::gt_eq, vec![false, false, true, true, true, false])]
    fn test_cmp_dict_decimal<T: DecimalType>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<bool>,
        #[values(
            (
                vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
                    .into_iter().collect::<Decimal32Array>(),
                vec![Some(7), Some(-3), Some(4), Some(3), Some(5)]
                    .into_iter().collect::<Decimal32Array>()
            ),
            (
                Decimal64Array::from_iter_values([0, 1, 2, 3, 4, 5]),
                Decimal64Array::from_iter_values([7, -3, 4, 3, 5])
            ),
            (
                Decimal128Array::from_iter_values([0, 1, 2, 3, 4, 5]),
                Decimal128Array::from_iter_values([7, -3, 4, 3, 5])
            ),
            (
                Decimal256Array::from_iter_values([0, 1, 2, 3, 4, 5].map(i256::from_i128)),
                Decimal256Array::from_iter_values([7, -3, 4, 3, 5].map(i256::from_i128))
            )
        )]
        values: (PrimitiveArray<T>, PrimitiveArray<T>),
    ) {
        let (values1, values2) = values;
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::new(keys, Arc::new(values1));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values2));
        let expected = BooleanArray::from(expected);
        assert_eq!(compare(&array1, &array2).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![false, false, false, true, true, false])]
    #[case::lt(crate::cmp::lt, vec![true, true, false, false, false, true])]
    #[case::lt_eq(crate::cmp::lt_eq, vec![true, true, false, true, true, true])]
    #[case::gt(crate::cmp::gt, vec![false, false, true, false, false, false])]
    #[case::gt_eq(crate::cmp::gt_eq, vec![false, false, true, true, true, false])]
    fn test_cmp_dict_non_dict_decimal<T: DecimalType>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<bool>,
        #[values(Decimal32Type {}, Decimal64Type {}, Decimal128Type {}, Decimal256Type {})]
        decimal_type: T,
    ) where
        T::Native: From<i32>,
    {
        let _ = decimal_type;
        let array1 = PrimitiveArray::<T>::from_iter_values([1, 2, 5, 4, 3, 0].map(T::Native::from));
        let values = PrimitiveArray::<T>::from_iter_values([7, -3, 4, 3, 5].map(T::Native::from));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));
        let expected = BooleanArray::from(expected);
        assert_eq!(compare(&array1, &array2).unwrap(), expected);
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![false, false, true, false])]
    #[case::lt(crate::cmp::lt, vec![true, false, false, false])]
    #[case::lt_eq(crate::cmp::lt_eq, vec![true, false, true, false])]
    #[case::gt(crate::cmp::gt, vec![false, true, false, true])]
    #[case::gt_eq(crate::cmp::gt_eq, vec![false, true, true, true])]
    fn test_decimal<T: DecimalType>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<bool>,
        #[values(Decimal32Type {}, Decimal64Type {}, Decimal128Type {}, Decimal256Type {})]
        decimal_type: T,
    ) where
        T::Native: From<i32>,
    {
        let _ = decimal_type;
        let a = PrimitiveArray::<T>::from_iter_values([1, 2, 4, 5].map(T::Native::from));
        let b = PrimitiveArray::<T>::from_iter_values([7, -3, 4, 3].map(T::Native::from));
        let expected = BooleanArray::from(expected);
        assert_eq!(expected, compare(&a, &b).unwrap());
    }

    #[rstest]
    #[case::eq(crate::cmp::eq, vec![Some(false), Some(false), Some(true), None, Some(false), Some(false)])]
    #[case::neq(crate::cmp::neq, vec![Some(true), Some(true), Some(false), None, Some(true), Some(true)])]
    #[case::lt(crate::cmp::lt, vec![Some(true), Some(true), Some(false), None, Some(false), Some(false)])]
    #[case::lt_eq(crate::cmp::lt_eq, vec![Some(true), Some(true), Some(true), None, Some(false), Some(false)])]
    #[case::gt(crate::cmp::gt, vec![Some(false), Some(false), Some(false), None, Some(true), Some(true)])]
    #[case::gt_eq(crate::cmp::gt_eq, vec![Some(false), Some(false), Some(true), None, Some(true), Some(true)])]
    fn test_decimal_scalar<T: DecimalType>(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] expected: Vec<Option<bool>>,
        #[values(Decimal32Type {}, Decimal128Type {})] decimal_type: T,
    ) where
        T::Native: From<i32>,
    {
        let _ = decimal_type;
        let a = PrimitiveArray::<T>::from_iter(
            [Some(1), Some(2), Some(3), None, Some(4), Some(5)]
                .into_iter()
                .map(|value| value.map(T::Native::from)),
        );
        let b = PrimitiveArray::<T>::new_scalar(T::Native::from(3));
        let expected = BooleanArray::from(expected);
        assert_eq!(expected, compare(&a, &b).unwrap());
    }

    #[rstest]
    #[case::three_eq(crate::cmp::eq, i256::from_i128(3), vec![false, false, true, false, false])]
    #[case::three_neq(crate::cmp::neq, i256::from_i128(3), vec![true, true, false, true, true])]
    #[case::three_lt(crate::cmp::lt, i256::from_i128(3), vec![true, true, false, false, false])]
    #[case::three_lt_eq(crate::cmp::lt_eq, i256::from_i128(3), vec![true, true, true, false, false])]
    #[case::three_gt(crate::cmp::gt, i256::from_i128(3), vec![false, false, false, true, true])]
    #[case::three_gt_eq(crate::cmp::gt_eq, i256::from_i128(3), vec![false, false, true, true, true])]
    #[case::max_eq(crate::cmp::eq, i256::MAX, vec![false, false, false, false, false])]
    #[case::max_neq(crate::cmp::neq, i256::MAX, vec![true, true, true, true, true])]
    #[case::max_lt(crate::cmp::lt, i256::MAX, vec![true, true, true, true, true])]
    #[case::max_lt_eq(crate::cmp::lt_eq, i256::MAX, vec![true, true, true, true, true])]
    #[case::max_gt(crate::cmp::gt, i256::MAX, vec![false, false, false, false, false])]
    #[case::max_gt_eq(crate::cmp::gt_eq, i256::MAX, vec![false, false, false, false, false])]
    fn test_decimal256_scalar(
        #[case] compare: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
        #[case] scalar: i256,
        #[case] expected: Vec<bool>,
    ) {
        let array =
            Decimal256Array::from_iter_values([1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let scalar = Decimal256Array::new_scalar(scalar);
        assert_eq!(
            compare(&array, &scalar).unwrap(),
            BooleanArray::from(expected)
        );
    }

    #[test]
    fn test_floating_zeros() {
        let a = Float32Array::from(vec![0.0_f32, -0.0]);
        let b = Float32Array::from(vec![-0.0_f32, 0.0]);

        let result = crate::cmp::eq(&a, &b).unwrap();
        let excepted = BooleanArray::from(vec![false, false]);
        assert_eq!(excepted, result);

        let scalar = Float32Array::new_scalar(0.0);
        let result = crate::cmp::eq(&a, &scalar).unwrap();
        let excepted = BooleanArray::from(vec![true, false]);
        assert_eq!(excepted, result);

        let scalar = Float32Array::new_scalar(-0.0);
        let result = crate::cmp::eq(&a, &scalar).unwrap();
        let excepted = BooleanArray::from(vec![false, true]);
        assert_eq!(excepted, result);
    }

    #[test]
    fn test_dictionary_nested_nulls() {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let v1 = Arc::new(Int32Array::from(vec![Some(0), None, Some(2)]));
        let a = DictionaryArray::new(keys.clone(), v1);
        let v2 = Arc::new(Int32Array::from(vec![None, Some(0), Some(2)]));
        let b = DictionaryArray::new(keys, v2);

        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(r.null_count(), 2);
        assert!(r.is_valid(2));
    }
}
