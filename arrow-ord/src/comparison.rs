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

//! Comparison kernels for `Array`s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.
//!

use arrow_array::cast::*;

use arrow_array::*;
use arrow_buffer::{bit_util, BooleanBuffer, MutableBuffer, NullBuffer};
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
    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = bool_buf.as_slice_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
            let list = right.value(i);
            let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
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
    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = &mut bool_buf;

    for i in 0..left_len {
        // contains(null, null) = false
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
            let list = right.value(i);
            let list = list.as_string::<OffsetSize>();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
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
    use arrow_buffer::{i256, ArrowNativeType, Buffer, IntervalDayTime, IntervalMonthDayNano};
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field};
    use half::f16;

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

    #[test]
    fn test_boolean_array_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::eq(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_neq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::neq(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::lt(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::lt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(true), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::gt(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(false), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = crate::cmp::gt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(false);
        let res1: Vec<Option<bool>> = crate::cmp::eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);
        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_neq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(false);

        let res1: Vec<Option<bool>> = crate::cmp::neq(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);
        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::neq(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(false);

        let res1: Vec<Option<bool>> = crate::cmp::lt(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(false), None]);

        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::lt(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(false);
        let res1: Vec<Option<bool>> = crate::cmp::lt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::lt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_gt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();
        let b = BooleanArray::new_scalar(false);
        let res1: Vec<Option<bool>> = crate::cmp::gt(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);
        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::gt(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_gt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let b = BooleanArray::new_scalar(false);
        let res1: Vec<Option<bool>> = crate::cmp::gt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(true), None]);
        let b = BooleanArray::new_scalar(true);
        let res2: Vec<Option<bool>> = crate::cmp::gt_eq(&a, &b).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
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
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
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
        "arrow".as_bytes(),
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
        "arrow".as_bytes(),
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
        "flight".as_bytes(),
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
        "flight".as_bytes(),
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
        "flight".as_bytes(),
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
        "flight".as_bytes(),
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

    #[test]
    fn test_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::eq(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let b = DictionaryArray::<Int8Type>::new_scalar(Int32Array::new_scalar(123));

        let a_eq = crate::cmp::eq(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_scalar_float() {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let expected =
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)]);
        let b = Float32Array::new_scalar(8.);
        assert_eq!(crate::cmp::eq(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::eq(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::lt(&array, &b).unwrap();
        assert_eq!(a_eq, BooleanArray::from(vec![true, true,false,false,false]));
    }

    #[test]
    fn test_lt_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let b = DictionaryArray::<Int8Type>::new_scalar(Int32Array::new_scalar(123));
        let a_eq = crate::cmp::lt(&array, &b).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_lt_dyn_scalar_float() {
        let array: Float32Array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(vec![true,true,false,false,false]);
        let b = Float32Array::new_scalar(8.);

        assert_eq!(crate::cmp::lt(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::lt(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_lt_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::lt_eq(&array, &b).unwrap();
        assert_eq!(a_eq, BooleanArray::from(vec![true,true,true,true,false]));
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

    #[test]
    fn test_lt_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let right = DictionaryArray::<Int8Type>::new_scalar(Int32Array::new_scalar(23));
        let a_eq = crate::cmp::lt_eq(&array, &right).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_scalar_float() {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let b = Float32Array::new_scalar(8.);
        let expected = BooleanArray::from(vec![true, true,true,true,false]);
        assert_eq!(crate::cmp::lt_eq(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::lt_eq(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let scalar = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::gt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), Some(false), Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_gt_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let right = DictionaryArray::<Int8Type>::new_scalar(Int32Array::new_scalar(23));
        let a_eq = crate::cmp::gt(&array, &right).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_gt_dyn_scalar_float() {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(vec![false,false,false,false,true]);
        let b = Float32Array::new_scalar(8.);
        assert_eq!(crate::cmp::gt(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::gt(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_gt_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::gt_eq(&array, &b).unwrap();
        assert_eq!(a_eq, BooleanArray::from(vec![false,false,true,true,true]));
    }

    #[test]
    fn test_gt_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(22).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let right = DictionaryArray::<Int8Type>::new_scalar(Int32Array::new_scalar(23));
        let a_eq = crate::cmp::gt_eq(&array, &right).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_scalar_float() {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let b = Float32Array::new_scalar(8.);
        let expected = BooleanArray::from(vec![false, false,true,true,true]);
        assert_eq!(crate::cmp::gt_eq(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::gt_eq(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_neq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let scalar = Int32Array::new_scalar(8);
        let a_eq = crate::cmp::neq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_neq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(22).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(Int32Array::new_scalar(23));
        let a_eq = crate::cmp::neq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_neq_dyn_scalar_float() {
        let array = Float32Array::from(vec![6.0, 7.0, 8.0, 8.0, 10.0]);
        let b = Float32Array::new_scalar(8.);
        let expected = BooleanArray::from(vec![true,true,false,false,true]);
        assert_eq!(crate::cmp::neq(&array, &b).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        let b = Float64Array::new_scalar(8.);
        assert_eq!(crate::cmp::neq(&array, &b).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar("flight");
        let large_scalar = LargeBinaryArray::new_scalar("flight");
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), None],
        );

        assert_eq!(crate::cmp::eq(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::eq(&large_array, &large_scalar).unwrap(),
            expected
        );

        let fsb_array = FixedSizeBinaryArray::from(vec![&[0u8], &[0u8], &[0u8], &[1u8]]);
        let scalar = FixedSizeBinaryArray::new_scalar([1u8]);
        let expected = BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]);
        assert_eq!(crate::cmp::eq(&fsb_array, &scalar).unwrap(), expected);
    }

    #[test]
    fn test_neq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar("flight");
        let large_scalar = LargeBinaryArray::new_scalar("flight");
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), None],
        );

        assert_eq!(crate::cmp::neq(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::neq(&large_array, &large_scalar).unwrap(),
            expected
        );

        let fsb_array = FixedSizeBinaryArray::from(vec![&[0u8], &[0u8], &[0u8], &[1u8]]);
        let scalar = FixedSizeBinaryArray::new_scalar([1u8]);
        let expected = BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]);
        assert_eq!(crate::cmp::neq(&fsb_array, &scalar).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar("flight");
        let large_scalar = LargeBinaryArray::new_scalar("flight");
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), None],
        );

        assert_eq!(crate::cmp::lt(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::lt(&large_array, &large_scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_lt_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar("flight");
        let large_scalar = LargeBinaryArray::new_scalar("flight");
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(false), Some(false), None],
        );

        assert_eq!(crate::cmp::lt_eq(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::lt_eq(&large_array, &large_scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_gt_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar("flight");
        let large_scalar = LargeBinaryArray::new_scalar("flight");
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), None],
        );

        assert_eq!(crate::cmp::gt(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::gt(&large_array, &large_scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_gt_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = BinaryArray::new_scalar([0xff, 0xf8]);
        let large_scalar = LargeBinaryArray::new_scalar([0xff, 0xf8]);
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), None],
        );

        assert_eq!(crate::cmp::gt_eq(&array, &scalar).unwrap(), expected);
        assert_eq!(
            crate::cmp::gt_eq(&large_array, &large_scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("xyz");
        let a_eq = crate::cmp::eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("xyz");
        let a_eq = crate::cmp::lt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::lt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("def");
        let a_eq = crate::cmp::lt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::lt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("def");
        let a_eq = crate::cmp::gt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::gt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_gt_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("def");
        let a_eq = crate::cmp::gt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_gt_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::gt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_neq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let scalar = StringArray::new_scalar("xyz");
        let a_eq = crate::cmp::neq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_neq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let scalar = DictionaryArray::<Int32Type>::new_scalar(StringArray::new_scalar("def"));
        let a_eq = crate::cmp::neq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::lt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(false), None])
        );
    }

    #[test]
    fn test_gt_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::gt(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::lt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::gt_eq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_neq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let scalar = BooleanArray::new_scalar(false);
        let a_eq = crate::cmp::neq(&array, &scalar).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );
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

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = Int8Array::from_iter_values([2_i8, 3, 4]);
        let keys2 = Int8Array::from_iter_values([2_i8, 4, 4]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_u64_array() {
        let values = UInt64Array::from_iter_values([10_u64, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 3, 4]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 3, 5]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
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

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([0_u64, 1, 2]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 2, 1]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_interval_array() {
        let values = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 6)),
            Some(IntervalDayTime::new(4, 10)),
        ]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 1, 3]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_date_array() {
        let values = Date32Array::from(vec![1, 6, 10, 2, 3, 5]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 0, 3]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 1]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 0]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::eq(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = crate::cmp::neq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    fn test_lt_dyn_gt_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = Int8Array::from_iter_values([3_i8, 4, 4]);
        let keys2 = Int8Array::from_iter_values([4_i8, 3, 4]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::lt(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = crate::cmp::lt_eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));

        let result = crate::cmp::gt(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = crate::cmp::gt_eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_lt_dyn_gt_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 0]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 1]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = crate::cmp::lt(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = crate::cmp::lt_eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, true, false]));

        let result = crate::cmp::gt(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, false, true])
        );

        let result = crate::cmp::gt_eq(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = Int8Array::from_iter([Some(12_i8), None, Some(14)]);

        let result = crate::cmp::eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = crate::cmp::eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = crate::cmp::neq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = crate::cmp::neq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = Int8Array::from_iter([Some(12_i8), None, Some(11)]);

        let result = crate::cmp::lt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = crate::cmp::lt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let result = crate::cmp::lt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );

        let result = crate::cmp::lt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = crate::cmp::gt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let result = crate::cmp::gt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = crate::cmp::gt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = crate::cmp::gt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);
        let array2 = Float16Array::from(
            vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)],
        );
        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let array2 = Float64Array::from(vec![f64::NAN, f64::NAN, 8.0, 8.0, 10.0]);

        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let array2 = Float16Array::from(vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]);
        let array2: Float64Array =
            Float64Array::from(vec![f64::NAN, f64::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_gt_eq_dyn_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let array2 = Float16Array::from(vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]);
        let array2 = Float64Array::from(vec![f64::NAN, f64::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_scalar_neq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);
        let scalar = Float16Array::new_scalar(f16::NAN);

        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::eq(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::neq(&array, &scalar).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float32Array::new_scalar(f32::NAN);
        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::eq(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::neq(&array, &scalar).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float64Array::new_scalar(f64::NAN);
        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::eq(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::neq(&array, &scalar).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_scalar_lt_eq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);
        let scalar = Float16Array::new_scalar(f16::NAN);

        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::lt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array, &scalar).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float32Array::new_scalar(f32::NAN);

        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::lt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array, &scalar).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float64Array::new_scalar(f64::NAN);
        let expected = BooleanArray::from(vec![false, true, true, true, true]);
        assert_eq!(crate::cmp::lt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array, &scalar).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_scalar_gt_eq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![
           f16::NAN,
           f16::from_f32(7.0),
           f16::from_f32(8.0),
           f16::from_f32(8.0),
           f16::from_f32(10.0),
       ]);
        let scalar = Float16Array::new_scalar(f16::NAN);
        let expected = BooleanArray::from(vec![false, false, false, false, false]);
        assert_eq!(crate::cmp::gt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::gt_eq(&array, &scalar).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float32Array::new_scalar(f32::NAN);
        let expected = BooleanArray::from(vec![false, false, false, false, false]);
        assert_eq!(crate::cmp::gt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::gt_eq(&array, &scalar).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let scalar = Float64Array::new_scalar(f64::NAN);
        let expected = BooleanArray::from(vec![false, false, false, false, false]);
        assert_eq!(crate::cmp::gt(&array, &scalar).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, false, false, false]);
        assert_eq!(crate::cmp::gt_eq(&array, &scalar).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_to_utf8_array() {
        let test1 = ["a", "a", "b", "c"];
        let test2 = ["a", "b", "b", "d"];

        let dict_array: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let array: StringArray = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = crate::cmp::eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::neq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::neq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_utf8_array() {
        let test1 = ["abc", "abc", "b", "cde"];
        let test2 = ["abc", "b", "b", "def"];

        let dict_array: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let array: StringArray = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = crate::cmp::lt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::lt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = crate::cmp::lt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = crate::cmp::lt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::gt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = crate::cmp::gt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::gt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::gt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_to_binary_array() {
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

        let result = crate::cmp::eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = crate::cmp::eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = crate::cmp::neq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = crate::cmp::neq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_binary_array() {
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

        let result = crate::cmp::lt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = crate::cmp::lt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false)])
        );

        let result = crate::cmp::lt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true)])
        );

        let result = crate::cmp::lt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = crate::cmp::gt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false)])
        );

        let result = crate::cmp::gt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = crate::cmp::gt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = crate::cmp::gt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dict_non_dict_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(10.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let values = Float32Array::from(vec![f32::NAN, 8.0, 10.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let values = Float64Array::from(vec![f64::NAN, 8.0, 10.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![true, false, true, true, true]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(crate::cmp::neq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_dict_non_dict_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]);
        let values = Float64Array::from(vec![f64::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, true, false, true, false, false]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, true, true, false, false]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_gt_eq_dyn_dict_non_dict_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]);
        let values = Float64Array::from(vec![f64::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, false, true, true]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, false, true, false, true, true]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = BooleanArray::from(test2);

        let result = crate::cmp::eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::neq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::neq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = BooleanArray::from(test2);

        let result = crate::cmp::lt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::lt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = crate::cmp::lt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = crate::cmp::lt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::gt(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = crate::cmp::gt(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = crate::cmp::gt_eq(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = crate::cmp::gt_eq(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
    }

    #[test]
    fn test_cmp_dict_decimal128() {
        let values = Decimal128Array::from_iter_values([0, 1, 2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, true, true, false]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, false, false, true]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, false, false, false]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, true, true, false]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_cmp_dict_non_dict_decimal128() {
        let array1: Decimal128Array = Decimal128Array::from_iter_values([1, 2, 5, 4, 3, 0]);

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, true, true, false]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, false, false, true]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, false, false, false]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, true, true, false]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_cmp_dict_decimal256() {
        let values =
            Decimal256Array::from_iter_values([0, 1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values =
            Decimal256Array::from_iter_values([7, -3, 4, 3, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, true, true, false]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, false, false, true]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, false, false, false]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, true, true, false]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_cmp_dict_non_dict_decimal256() {
        let array1: Decimal256Array =
            Decimal256Array::from_iter_values([1, 2, 5, 4, 3, 0].into_iter().map(i256::from_i128));

        let values =
            Decimal256Array::from_iter_values([7, -3, 4, 3, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(vec![false, false, false, true, true, false]);
        assert_eq!(crate::cmp::eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, false, false, true]);
        assert_eq!(crate::cmp::lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, true, true, true]);
        assert_eq!(crate::cmp::lt_eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, false, false, false]);
        assert_eq!(crate::cmp::gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(vec![false, false, true, true, true, false]);
        assert_eq!(crate::cmp::gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_decimal128() {
        let a = Decimal128Array::from_iter_values([1, 2, 4, 5]);
        let b = Decimal128Array::from_iter_values([7, -3, 4, 3]);
        let e = BooleanArray::from(vec![false, false, true, false]);
        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, false, false]);
        let r = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, true, false]);
        let r = crate::cmp::lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, false, true]);
        let r = crate::cmp::gt(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, true, true]);
        let r = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal128_scalar() {
        let a = Decimal128Array::from(vec![Some(1), Some(2), Some(3), None, Some(4), Some(5)]);
        let b = Decimal128Array::new_scalar(3_i128);
        // array eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), None, Some(false), Some(false)],
        );
        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array neq scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), None, Some(true), Some(true)],
        );
        let r = crate::cmp::neq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), None, Some(false), Some(false)],
        );
        let r = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt_eq scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), None, Some(false), Some(false)],
        );
        let r = crate::cmp::lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), None, Some(true), Some(true)],
        );
        let r = crate::cmp::gt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt_eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), None, Some(true), Some(true)],
        );
        let r = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256() {
        let a = Decimal256Array::from_iter_values([1, 2, 4, 5].into_iter().map(i256::from_i128));
        let b = Decimal256Array::from_iter_values([7, -3, 4, 3].into_iter().map(i256::from_i128));
        let e = BooleanArray::from(vec![false, false, true, false]);
        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, false, false]);
        let r = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, true, false]);
        let r = crate::cmp::lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, false, true]);
        let r = crate::cmp::gt(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, true, true]);
        let r = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256_scalar_i128() {
        let a = Decimal256Array::from_iter_values([1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let b = Decimal256Array::new_scalar(i256::from_i128(3));
        // array eq scalar
        let e = BooleanArray::from(vec![false, false, true, false, false]);
        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array neq scalar
        let e = BooleanArray::from(vec![true, true, false, true, true]);
        let r = crate::cmp::neq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt scalar
        let e = BooleanArray::from(vec![true, true, false, false, false]);
        let r = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt_eq scalar
        let e = BooleanArray::from(vec![true, true, true, false, false]);
        let r = crate::cmp::lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt scalar
        let e = BooleanArray::from(vec![false, false, false, true, true]);
        let r = crate::cmp::gt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt_eq scalar
        let e = BooleanArray::from(vec![false, false, true, true, true]);
        let r = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256_scalar_i256() {
        let a = Decimal256Array::from_iter_values([1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let b = Decimal256Array::new_scalar(i256::MAX);
        // array eq scalar
        let e = BooleanArray::from(vec![false, false, false, false, false]);
        let r = crate::cmp::eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array neq scalar
        let e = BooleanArray::from(vec![true, true, true, true, true]);
        let r = crate::cmp::neq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt scalar
        let e = BooleanArray::from(vec![true, true, true, true, true]);
        let r = crate::cmp::lt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array lt_eq scalar
        let e = BooleanArray::from(vec![true, true, true, true, true]);
        let r = crate::cmp::lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt scalar
        let e = BooleanArray::from(vec![false, false, false, false, false]);
        let r = crate::cmp::gt(&a, &b).unwrap();
        assert_eq!(e, r);

        // array gt_eq scalar
        let e = BooleanArray::from(vec![false, false, false, false, false]);
        let r = crate::cmp::gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);
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
