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

//! Contains functions and function factories to compare arrays.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::{ArrowError, SortOptions};
use std::cmp::Ordering;

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

/// If parent sort order is descending we need to invert the value of nulls_first so that
/// when the parent is sorted based on the produced ranks, nulls are still ordered correctly
fn child_opts(opts: SortOptions) -> SortOptions {
    SortOptions {
        descending: false,
        nulls_first: opts.nulls_first != opts.descending,
    }
}

fn compare<A, F>(l: &A, r: &A, opts: SortOptions, cmp: F) -> DynComparator
where
    A: Array + Clone,
    F: Fn(usize, usize) -> Ordering + Send + Sync + 'static,
{
    let l = l.logical_nulls().filter(|x| x.null_count() > 0);
    let r = r.logical_nulls().filter(|x| x.null_count() > 0);
    match (opts.nulls_first, opts.descending) {
        (true, true) => compare_impl::<true, true, _>(l, r, cmp),
        (true, false) => compare_impl::<true, false, _>(l, r, cmp),
        (false, true) => compare_impl::<false, true, _>(l, r, cmp),
        (false, false) => compare_impl::<false, false, _>(l, r, cmp),
    }
}

fn compare_impl<const NULLS_FIRST: bool, const DESCENDING: bool, F>(
    l: Option<NullBuffer>,
    r: Option<NullBuffer>,
    cmp: F,
) -> DynComparator
where
    F: Fn(usize, usize) -> Ordering + Send + Sync + 'static,
{
    let cmp = move |i, j| match DESCENDING {
        true => cmp(i, j).reverse(),
        false => cmp(i, j),
    };

    let (left_null, right_null) = match NULLS_FIRST {
        true => (Ordering::Less, Ordering::Greater),
        false => (Ordering::Greater, Ordering::Less),
    };

    match (l, r) {
        (None, None) => Box::new(cmp),
        (Some(l), None) => Box::new(move |i, j| match l.is_null(i) {
            true => left_null,
            false => cmp(i, j),
        }),
        (None, Some(r)) => Box::new(move |i, j| match r.is_null(j) {
            true => right_null,
            false => cmp(i, j),
        }),
        (Some(l), Some(r)) => Box::new(move |i, j| match (l.is_null(i), r.is_null(j)) {
            (true, true) => Ordering::Equal,
            (true, false) => left_null,
            (false, true) => right_null,
            (false, false) => cmp(i, j),
        }),
    }
}

fn compare_primitive<T: ArrowPrimitiveType>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> DynComparator
where
    T::Native: ArrowNativeTypeOp,
{
    let left = left.as_primitive::<T>();
    let right = right.as_primitive::<T>();
    let l_values = left.values().clone();
    let r_values = right.values().clone();

    compare(&left, &right, opts, move |i, j| {
        l_values[i].compare(r_values[j])
    })
}

fn compare_boolean(left: &dyn Array, right: &dyn Array, opts: SortOptions) -> DynComparator {
    let left = left.as_boolean();
    let right = right.as_boolean();

    let l_values = left.values().clone();
    let r_values = right.values().clone();

    compare(left, right, opts, move |i, j| {
        l_values.value(i).cmp(&r_values.value(j))
    })
}

fn compare_bytes<T: ByteArrayType>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> DynComparator {
    let left = left.as_bytes::<T>();
    let right = right.as_bytes::<T>();

    let l = left.clone();
    let r = right.clone();
    compare(left, right, opts, move |i, j| {
        let l: &[u8] = l.value(i).as_ref();
        let r: &[u8] = r.value(j).as_ref();
        l.cmp(r)
    })
}

fn compare_byte_view<T: ByteViewType>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> DynComparator {
    let left = left.as_byte_view::<T>();
    let right = right.as_byte_view::<T>();

    let l = left.clone();
    let r = right.clone();
    compare(left, right, opts, move |i, j| {
        crate::cmp::compare_byte_view(&l, i, &r, j)
    })
}

fn compare_dict<K: ArrowDictionaryKeyType>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_dictionary::<K>();
    let right = right.as_dictionary::<K>();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.values().as_ref(), right.values().as_ref(), c_opts)?;
    let left_keys = left.keys().values().clone();
    let right_keys = right.keys().values().clone();

    let f = compare(left, right, opts, move |i, j| {
        let l = left_keys[i].as_usize();
        let r = right_keys[j].as_usize();
        cmp(l, r)
    });
    Ok(f)
}

fn compare_list<O: OffsetSizeTrait>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_list::<O>();
    let right = right.as_list::<O>();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.values().as_ref(), right.values().as_ref(), c_opts)?;

    let l_o = left.offsets().clone();
    let r_o = right.offsets().clone();
    let f = compare(left, right, opts, move |i, j| {
        let l_end = l_o[i + 1].as_usize();
        let l_start = l_o[i].as_usize();

        let r_end = r_o[j + 1].as_usize();
        let r_start = r_o[j].as_usize();

        for (i, j) in (l_start..l_end).zip(r_start..r_end) {
            match cmp(i, j) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        (l_end - l_start).cmp(&(r_end - r_start))
    });
    Ok(f)
}

fn compare_fixed_list(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_fixed_size_list();
    let right = right.as_fixed_size_list();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.values().as_ref(), right.values().as_ref(), c_opts)?;

    let l_size = left.value_length().to_usize().unwrap();
    let r_size = right.value_length().to_usize().unwrap();
    let size_cmp = l_size.cmp(&r_size);

    let f = compare(left, right, opts, move |i, j| {
        let l_start = i * l_size;
        let l_end = l_start + l_size;
        let r_start = j * r_size;
        let r_end = r_start + r_size;
        for (i, j) in (l_start..l_end).zip(r_start..r_end) {
            match cmp(i, j) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        size_cmp
    });
    Ok(f)
}

fn compare_struct(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_struct();
    let right = right.as_struct();

    if left.columns().len() != right.columns().len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot compare StructArray with different number of columns".to_string(),
        ));
    }

    let c_opts = child_opts(opts);
    let columns = left.columns().iter().zip(right.columns());
    let comparators = columns
        .map(|(l, r)| make_comparator(l, r, c_opts))
        .collect::<Result<Vec<_>, _>>()?;

    let f = compare(left, right, opts, move |i, j| {
        for cmp in &comparators {
            match cmp(i, j) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        Ordering::Equal
    });
    Ok(f)
}

#[deprecated(note = "Use make_comparator")]
#[doc(hidden)]
pub fn build_compare(left: &dyn Array, right: &dyn Array) -> Result<DynComparator, ArrowError> {
    make_comparator(left, right, SortOptions::default())
}

/// Returns a comparison function that compares two values at two different positions
/// between the two arrays.
///
/// For comparing arrays element-wise, see also the vectorised kernels in [`crate::cmp`].
///
/// If `nulls_first` is true `NULL` values will be considered less than any non-null value,
/// otherwise they will be considered greater.
///
/// # Basic Usage
///
/// ```
/// # use std::cmp::Ordering;
/// # use arrow_array::Int32Array;
/// # use arrow_ord::ord::make_comparator;
/// # use arrow_schema::SortOptions;
/// #
/// let array1 = Int32Array::from(vec![1, 2]);
/// let array2 = Int32Array::from(vec![3, 4]);
///
/// let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(cmp(0, 1), Ordering::Less);
///
/// let array1 = Int32Array::from(vec![Some(1), None]);
/// let array2 = Int32Array::from(vec![None, Some(2)]);
/// let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();
///
/// assert_eq!(cmp(0, 1), Ordering::Less); // Some(1) vs Some(2)
/// assert_eq!(cmp(1, 1), Ordering::Less); // None vs Some(2)
/// assert_eq!(cmp(1, 0), Ordering::Equal); // None vs None
/// assert_eq!(cmp(0, 0), Ordering::Greater); // Some(1) vs None
/// ```
///
/// # Postgres-compatible Nested Comparison
///
/// Whilst SQL prescribes ternary logic for nulls, that is comparing a value against a NULL yields
/// a NULL, many systems, including postgres, instead apply a total ordering to comparison of
/// nested nulls. That is nulls within nested types are either greater than any value (postgres),
/// or less than any value (Spark).
///
/// In particular
///
/// ```ignore
/// { a: 1, b: null } == { a: 1, b: null } => true
/// { a: 1, b: null } == { a: 1, b: 1 } => false
/// { a: 1, b: null } == null => null
/// null == null => null
/// ```
///
/// This could be implemented as below
///
/// ```
/// # use arrow_array::{Array, BooleanArray};
/// # use arrow_buffer::NullBuffer;
/// # use arrow_ord::cmp;
/// # use arrow_ord::ord::make_comparator;
/// # use arrow_schema::{ArrowError, SortOptions};
/// fn eq(a: &dyn Array, b: &dyn Array) -> Result<BooleanArray, ArrowError> {
///     if !a.data_type().is_nested() {
///         return cmp::eq(&a, &b); // Use faster vectorised kernel
///     }
///
///     let cmp = make_comparator(a, b, SortOptions::default())?;
///     let len = a.len().min(b.len());
///     let values = (0..len).map(|i| cmp(i, i).is_eq()).collect();
///     let nulls = NullBuffer::union(a.nulls(), b.nulls());
///     Ok(BooleanArray::new(values, nulls))
/// }
/// ````
pub fn make_comparator(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    use arrow_schema::DataType::*;

    macro_rules! primitive_helper {
        ($t:ty, $left:expr, $right:expr, $nulls_first:expr) => {
            Ok(compare_primitive::<$t>($left, $right, $nulls_first))
        };
    }
    downcast_primitive! {
        left.data_type(), right.data_type() => (primitive_helper, left, right, opts),
        (Boolean, Boolean) => Ok(compare_boolean(left, right, opts)),
        (Utf8, Utf8) => Ok(compare_bytes::<Utf8Type>(left, right, opts)),
        (LargeUtf8, LargeUtf8) => Ok(compare_bytes::<LargeUtf8Type>(left, right, opts)),
        (Utf8View, Utf8View) => Ok(compare_byte_view::<StringViewType>(left, right, opts)),
        (Binary, Binary) => Ok(compare_bytes::<BinaryType>(left, right, opts)),
        (LargeBinary, LargeBinary) => Ok(compare_bytes::<LargeBinaryType>(left, right, opts)),
        (BinaryView, BinaryView) => Ok(compare_byte_view::<BinaryViewType>(left, right, opts)),
        (FixedSizeBinary(_), FixedSizeBinary(_)) => {
            let left = left.as_fixed_size_binary();
            let right = right.as_fixed_size_binary();

            let l = left.clone();
            let r = right.clone();
            Ok(compare(left, right, opts, move |i, j| {
                l.value(i).cmp(r.value(j))
            }))
        },
        (List(_), List(_)) => compare_list::<i32>(left, right, opts),
        (LargeList(_), LargeList(_)) => compare_list::<i64>(left, right, opts),
        (FixedSizeList(_, _), FixedSizeList(_, _)) => compare_fixed_list(left, right, opts),
        (Struct(_), Struct(_)) => compare_struct(left, right, opts),
        (Dictionary(l_key, _), Dictionary(r_key, _)) => {
             macro_rules! dict_helper {
                ($t:ty, $left:expr, $right:expr, $opts: expr) => {
                     compare_dict::<$t>($left, $right, $opts)
                 };
             }
            downcast_integer! {
                 l_key.as_ref(), r_key.as_ref() => (dict_helper, left, right, opts),
                 _ => unreachable!()
             }
        },
        (lhs, rhs) => Err(ArrowError::InvalidArgumentError(match lhs == rhs {
            true => format!("The data type type {lhs:?} has no natural order"),
            false => "Can't compare arrays of different types".to_string(),
        }))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_buffer::{i256, IntervalDayTime, OffsetBuffer};
    use arrow_schema::{DataType, Field, Fields};
    use half::f16;
    use std::sync::Arc;

    #[test]
    fn test_fixed_size_binary() {
        let items = vec![vec![1u8], vec![2u8]];
        let array = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
    }

    #[test]
    fn test_fixed_size_binary_fixed_size_binary() {
        let items = vec![vec![1u8]];
        let array1 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();
        let items = vec![vec![2u8]];
        let array2 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
    }

    #[test]
    fn test_i32() {
        let array = Int32Array::from(vec![1, 2]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
    }

    #[test]
    fn test_i32_i32() {
        let array1 = Int32Array::from(vec![1]);
        let array2 = Int32Array::from(vec![2]);

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
    }

    #[test]
    fn test_f16() {
        let array = Float16Array::from(vec![f16::from_f32(1.0), f16::from_f32(2.0)]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
    }

    #[test]
    fn test_f64() {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
    }

    #[test]
    fn test_f64_nan() {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Equal, cmp(1, 1));
    }

    #[test]
    fn test_f64_zeros() {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Greater, cmp(1, 0));
    }

    #[test]
    fn test_interval_day_time() {
        let array = IntervalDayTimeArray::from(vec![
            // 0 days, 1 second
            IntervalDayTimeType::make_value(0, 1000),
            // 1 day, 2 milliseconds
            IntervalDayTimeType::make_value(1, 2),
            // 90M milliseconds (which is more than is in 1 day)
            IntervalDayTimeType::make_value(0, 90_000_000),
        ]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Greater, cmp(1, 0));

        // somewhat confusingly, while 90M milliseconds is more than 1 day,
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(Ordering::Greater, cmp(1, 2));
        assert_eq!(Ordering::Less, cmp(2, 1));
    }

    #[test]
    fn test_interval_year_month() {
        let array = IntervalYearMonthArray::from(vec![
            // 1 year, 0 months
            IntervalYearMonthType::make_value(1, 0),
            // 0 years, 13 months
            IntervalYearMonthType::make_value(0, 13),
            // 1 year, 1 month
            IntervalYearMonthType::make_value(1, 1),
        ]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Greater, cmp(1, 0));

        // the underlying representation is months, so both quantities are the same
        assert_eq!(Ordering::Equal, cmp(1, 2));
        assert_eq!(Ordering::Equal, cmp(2, 1));
    }

    #[test]
    fn test_interval_month_day_nano() {
        let array = IntervalMonthDayNanoArray::from(vec![
            // 100 days
            IntervalMonthDayNanoType::make_value(0, 100, 0),
            // 1 month
            IntervalMonthDayNanoType::make_value(1, 0, 0),
            // 100 day, 1 nanoseconds
            IntervalMonthDayNanoType::make_value(0, 100, 2),
        ]);

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Greater, cmp(1, 0));

        // somewhat confusingly, while 100 days is more than 1 month in all cases
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(Ordering::Greater, cmp(1, 2));
        assert_eq!(Ordering::Less, cmp(2, 1));
    }

    #[test]
    fn test_decimal() {
        let array = vec![Some(5_i128), Some(2_i128), Some(3_i128)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();
        assert_eq!(Ordering::Less, cmp(1, 0));
        assert_eq!(Ordering::Greater, cmp(0, 2));
    }

    #[test]
    fn test_decimali256() {
        let array = vec![
            Some(i256::from_i128(5_i128)),
            Some(i256::from_i128(2_i128)),
            Some(i256::from_i128(3_i128)),
        ]
        .into_iter()
        .collect::<Decimal256Array>()
        .with_precision_and_scale(53, 6)
        .unwrap();

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();
        assert_eq!(Ordering::Less, cmp(1, 0));
        assert_eq!(Ordering::Greater, cmp(0, 2));
    }

    #[test]
    fn test_dict() {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = data.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = make_comparator(&array, &array, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Equal, cmp(3, 4));
        assert_eq!(Ordering::Greater, cmp(2, 3));
    }

    #[test]
    fn test_multiple_dict() {
        let d1 = vec!["a", "b", "c", "d"];
        let a1 = d1.into_iter().collect::<DictionaryArray<Int16Type>>();
        let d2 = vec!["e", "f", "g", "a"];
        let a2 = d2.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = make_comparator(&a1, &a2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Equal, cmp(0, 3));
        assert_eq!(Ordering::Greater, cmp(1, 3));
    }

    #[test]
    fn test_primitive_dict() {
        let values = Int32Array::from(vec![1_i32, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Int32Array::from(vec![2_i32, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    #[test]
    fn test_float_dict() {
        let values = Float32Array::from(vec![1.0, 0.5, 2.1, 5.5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        let values = Float32Array::from(vec![1.2, 3.2, 4.0, 5.5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    #[test]
    fn test_timestamp_dict() {
        let values = TimestampSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = TimestampSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    #[test]
    fn test_interval_dict() {
        let v1 = IntervalDayTime::new(0, 1);
        let v2 = IntervalDayTime::new(0, 2);
        let v3 = IntervalDayTime::new(12, 2);

        let values = IntervalDayTimeArray::from(vec![Some(v1), Some(v2), None, Some(v3)]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = IntervalDayTimeArray::from(vec![Some(v3), Some(v2), None, Some(v1)]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0)); // v1 vs v3
        assert_eq!(Ordering::Equal, cmp(0, 3)); // v1 vs v1
        assert_eq!(Ordering::Greater, cmp(3, 3)); // v3 vs v1
        assert_eq!(Ordering::Greater, cmp(3, 1)); // v3 vs v2
        assert_eq!(Ordering::Greater, cmp(3, 2)); // v3 vs v2
    }

    #[test]
    fn test_duration_dict() {
        let values = DurationSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = DurationSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    #[test]
    fn test_decimal_dict() {
        let values = Decimal128Array::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal128Array::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    #[test]
    fn test_decimal256_dict() {
        let values = Decimal256Array::from(vec![
            i256::from_i128(1),
            i256::from_i128(0),
            i256::from_i128(2),
            i256::from_i128(5),
        ]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal256Array::from(vec![
            i256::from_i128(2),
            i256::from_i128(3),
            i256::from_i128(4),
            i256::from_i128(5),
        ]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 0));
        assert_eq!(Ordering::Less, cmp(0, 3));
        assert_eq!(Ordering::Equal, cmp(3, 3));
        assert_eq!(Ordering::Greater, cmp(3, 1));
        assert_eq!(Ordering::Greater, cmp(3, 2));
    }

    fn test_bytes_impl<T: ByteArrayType>() {
        let offsets = OffsetBuffer::from_lengths([3, 3, 1]);
        let a = GenericByteArray::<T>::new(offsets, b"abcdefa".into(), None);
        let cmp = make_comparator(&a, &a, SortOptions::default()).unwrap();

        assert_eq!(Ordering::Less, cmp(0, 1));
        assert_eq!(Ordering::Greater, cmp(0, 2));
        assert_eq!(Ordering::Equal, cmp(1, 1));
    }

    #[test]
    fn test_bytes() {
        test_bytes_impl::<Utf8Type>();
        test_bytes_impl::<LargeUtf8Type>();
        test_bytes_impl::<BinaryType>();
        test_bytes_impl::<LargeBinaryType>();
    }

    #[test]
    fn test_lists() {
        let mut a = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        a.extend([
            Some(vec![Some(vec![Some(1), Some(2), None]), Some(vec![None])]),
            Some(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(1)]),
            ]),
            Some(vec![]),
        ]);
        let a = a.finish();
        let mut b = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        b.extend([
            Some(vec![Some(vec![Some(1), Some(2), None]), Some(vec![None])]),
            Some(vec![
                Some(vec![Some(1), Some(2), None]),
                Some(vec![Some(1)]),
            ]),
            Some(vec![
                Some(vec![Some(1), Some(2), Some(3), Some(4)]),
                Some(vec![Some(1)]),
            ]),
            None,
        ]);
        let b = b.finish();

        let opts = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let cmp = make_comparator(&a, &b, opts).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Equal);
        assert_eq!(cmp(0, 1), Ordering::Less);
        assert_eq!(cmp(0, 2), Ordering::Less);
        assert_eq!(cmp(1, 2), Ordering::Less);
        assert_eq!(cmp(1, 3), Ordering::Greater);
        assert_eq!(cmp(2, 0), Ordering::Less);

        let opts = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let cmp = make_comparator(&a, &b, opts).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Equal);
        assert_eq!(cmp(0, 1), Ordering::Less);
        assert_eq!(cmp(0, 2), Ordering::Less);
        assert_eq!(cmp(1, 2), Ordering::Greater);
        assert_eq!(cmp(1, 3), Ordering::Greater);
        assert_eq!(cmp(2, 0), Ordering::Greater);

        let opts = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let cmp = make_comparator(&a, &b, opts).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Equal);
        assert_eq!(cmp(0, 1), Ordering::Greater);
        assert_eq!(cmp(0, 2), Ordering::Greater);
        assert_eq!(cmp(1, 2), Ordering::Greater);
        assert_eq!(cmp(1, 3), Ordering::Less);
        assert_eq!(cmp(2, 0), Ordering::Greater);

        let opts = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let cmp = make_comparator(&a, &b, opts).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Equal);
        assert_eq!(cmp(0, 1), Ordering::Greater);
        assert_eq!(cmp(0, 2), Ordering::Greater);
        assert_eq!(cmp(1, 2), Ordering::Less);
        assert_eq!(cmp(1, 3), Ordering::Less);
        assert_eq!(cmp(2, 0), Ordering::Less);
    }

    #[test]
    fn test_struct() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new_list("b", Field::new("item", DataType::Int32, true), true),
        ]);

        let a = Int32Array::from(vec![Some(1), Some(2), None, None]);
        let mut b = ListBuilder::new(Int32Builder::new());
        b.extend([Some(vec![Some(1), Some(2)]), Some(vec![None]), None, None]);
        let b = b.finish();

        let nulls = Some(NullBuffer::from_iter([true, true, true, false]));
        let values = vec![Arc::new(a) as _, Arc::new(b) as _];
        let s1 = StructArray::new(fields.clone(), values, nulls);

        let a = Int32Array::from(vec![None, Some(2), None]);
        let mut b = ListBuilder::new(Int32Builder::new());
        b.extend([None, None, Some(vec![])]);
        let b = b.finish();

        let values = vec![Arc::new(a) as _, Arc::new(b) as _];
        let s2 = StructArray::new(fields.clone(), values, None);

        let opts = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let cmp = make_comparator(&s1, &s2, opts).unwrap();
        assert_eq!(cmp(0, 1), Ordering::Less); // (1, [1, 2]) cmp (2, None)
        assert_eq!(cmp(0, 0), Ordering::Greater); // (1, [1, 2]) cmp (None, None)
        assert_eq!(cmp(1, 1), Ordering::Greater); // (2, [None]) cmp (2, None)
        assert_eq!(cmp(2, 2), Ordering::Less); // (None, None) cmp (None, [])
        assert_eq!(cmp(3, 0), Ordering::Less); // None cmp (None, [])
        assert_eq!(cmp(2, 0), Ordering::Equal); // (None, None) cmp (None, None)
        assert_eq!(cmp(3, 0), Ordering::Less); // None cmp (None, None)

        let opts = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let cmp = make_comparator(&s1, &s2, opts).unwrap();
        assert_eq!(cmp(0, 1), Ordering::Greater); // (1, [1, 2]) cmp (2, None)
        assert_eq!(cmp(0, 0), Ordering::Greater); // (1, [1, 2]) cmp (None, None)
        assert_eq!(cmp(1, 1), Ordering::Greater); // (2, [None]) cmp (2, None)
        assert_eq!(cmp(2, 2), Ordering::Less); // (None, None) cmp (None, [])
        assert_eq!(cmp(3, 0), Ordering::Less); // None cmp (None, [])
        assert_eq!(cmp(2, 0), Ordering::Equal); // (None, None) cmp (None, None)
        assert_eq!(cmp(3, 0), Ordering::Less); // None cmp (None, None)

        let opts = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let cmp = make_comparator(&s1, &s2, opts).unwrap();
        assert_eq!(cmp(0, 1), Ordering::Greater); // (1, [1, 2]) cmp (2, None)
        assert_eq!(cmp(0, 0), Ordering::Less); // (1, [1, 2]) cmp (None, None)
        assert_eq!(cmp(1, 1), Ordering::Less); // (2, [None]) cmp (2, None)
        assert_eq!(cmp(2, 2), Ordering::Greater); // (None, None) cmp (None, [])
        assert_eq!(cmp(3, 0), Ordering::Greater); // None cmp (None, [])
        assert_eq!(cmp(2, 0), Ordering::Equal); // (None, None) cmp (None, None)
        assert_eq!(cmp(3, 0), Ordering::Greater); // None cmp (None, None)

        let opts = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let cmp = make_comparator(&s1, &s2, opts).unwrap();
        assert_eq!(cmp(0, 1), Ordering::Less); // (1, [1, 2]) cmp (2, None)
        assert_eq!(cmp(0, 0), Ordering::Less); // (1, [1, 2]) cmp (None, None)
        assert_eq!(cmp(1, 1), Ordering::Less); // (2, [None]) cmp (2, None)
        assert_eq!(cmp(2, 2), Ordering::Greater); // (None, None) cmp (None, [])
        assert_eq!(cmp(3, 0), Ordering::Greater); // None cmp (None, [])
        assert_eq!(cmp(2, 0), Ordering::Equal); // (None, None) cmp (None, None)
        assert_eq!(cmp(3, 0), Ordering::Greater); // None cmp (None, None)
    }
}
