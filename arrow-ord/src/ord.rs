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

use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};
use num::Float;
use std::cmp::Ordering;

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

/// compares two floats, placing NaNs at last
fn cmp_nans_last<T: Float>(a: &T, b: &T) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(b).unwrap(),
    }
}

fn compare_primitives<T: ArrowPrimitiveType>(
    left: &dyn Array,
    right: &dyn Array,
) -> DynComparator
where
    T::Native: Ord,
{
    let left: PrimitiveArray<T> = PrimitiveArray::from(left.data().clone());
    let right: PrimitiveArray<T> = PrimitiveArray::from(right.data().clone());
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_boolean(left: &dyn Array, right: &dyn Array) -> DynComparator {
    let left: BooleanArray = BooleanArray::from(left.data().clone());
    let right: BooleanArray = BooleanArray::from(right.data().clone());

    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_float<T: ArrowPrimitiveType>(
    left: &dyn Array,
    right: &dyn Array,
) -> DynComparator
where
    T::Native: Float,
{
    let left: PrimitiveArray<T> = PrimitiveArray::from(left.data().clone());
    let right: PrimitiveArray<T> = PrimitiveArray::from(right.data().clone());
    Box::new(move |i, j| cmp_nans_last(&left.value(i), &right.value(j)))
}

fn compare_string<T>(left: &dyn Array, right: &dyn Array) -> DynComparator
where
    T: OffsetSizeTrait,
{
    let left: StringArray = StringArray::from(left.data().clone());
    let right: StringArray = StringArray::from(right.data().clone());

    Box::new(move |i, j| left.value(i).cmp(right.value(j)))
}

fn compare_dict_primitive<K, V>(left: &dyn Array, right: &dyn Array) -> DynComparator
where
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
    V::Native: Ord,
{
    let left = left.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    let right = right.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    let left_keys: PrimitiveArray<K> = PrimitiveArray::from(left.keys().data().clone());
    let right_keys: PrimitiveArray<K> = PrimitiveArray::from(right.keys().data().clone());
    let left_values: PrimitiveArray<V> =
        PrimitiveArray::from(left.values().data().clone());
    let right_values: PrimitiveArray<V> =
        PrimitiveArray::from(right.values().data().clone());

    Box::new(move |i: usize, j: usize| {
        let key_left = left_keys.value(i).as_usize();
        let key_right = right_keys.value(j).as_usize();
        let left = left_values.value(key_left);
        let right = right_values.value(key_right);
        left.cmp(&right)
    })
}

fn compare_dict_string<T>(left: &dyn Array, right: &dyn Array) -> DynComparator
where
    T: ArrowDictionaryKeyType,
{
    let left = left.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();

    let left_keys: PrimitiveArray<T> = PrimitiveArray::from(left.keys().data().clone());
    let right_keys: PrimitiveArray<T> = PrimitiveArray::from(right.keys().data().clone());
    let left_values = StringArray::from(left.values().data().clone());
    let right_values = StringArray::from(right.values().data().clone());

    Box::new(move |i: usize, j: usize| {
        let key_left = left_keys.value(i).as_usize();
        let key_right = right_keys.value(j).as_usize();
        let left = left_values.value(key_left);
        let right = right_values.value(key_right);
        left.cmp(right)
    })
}

fn cmp_dict_primitive<VT>(
    key_type: &DataType,
    left: &dyn Array,
    right: &dyn Array,
) -> Result<DynComparator, ArrowError>
where
    VT: ArrowPrimitiveType,
    VT::Native: Ord,
{
    use DataType::*;

    Ok(match key_type {
        UInt8 => compare_dict_primitive::<UInt8Type, VT>(left, right),
        UInt16 => compare_dict_primitive::<UInt16Type, VT>(left, right),
        UInt32 => compare_dict_primitive::<UInt32Type, VT>(left, right),
        UInt64 => compare_dict_primitive::<UInt64Type, VT>(left, right),
        Int8 => compare_dict_primitive::<Int8Type, VT>(left, right),
        Int16 => compare_dict_primitive::<Int16Type, VT>(left, right),
        Int32 => compare_dict_primitive::<Int32Type, VT>(left, right),
        Int64 => compare_dict_primitive::<Int64Type, VT>(left, right),
        t => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Dictionaries do not support keys of type {:?}",
                t
            )));
        }
    })
}

/// returns a comparison function that compares two values at two different positions
/// between the two arrays.
/// The arrays' types must be equal.
/// # Example
/// ```
/// use arrow_array::Int32Array;
/// use arrow_ord::ord::build_compare;
///
/// let array1 = Int32Array::from(vec![1, 2]);
/// let array2 = Int32Array::from(vec![3, 4]);
///
/// let cmp = build_compare(&array1, &array2).unwrap();
///
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(std::cmp::Ordering::Less, (cmp)(0, 1));
/// ```
// This is a factory of comparisons.
// The lifetime 'a enforces that we cannot use the closure beyond any of the array's lifetime.
pub fn build_compare(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<DynComparator, ArrowError> {
    use arrow_schema::{DataType::*, IntervalUnit::*, TimeUnit::*};
    Ok(match (left.data_type(), right.data_type()) {
        (a, b) if a != b => {
            return Err(ArrowError::InvalidArgumentError(
                "Can't compare arrays of different types".to_string(),
            ));
        }
        (Boolean, Boolean) => compare_boolean(left, right),
        (UInt8, UInt8) => compare_primitives::<UInt8Type>(left, right),
        (UInt16, UInt16) => compare_primitives::<UInt16Type>(left, right),
        (UInt32, UInt32) => compare_primitives::<UInt32Type>(left, right),
        (UInt64, UInt64) => compare_primitives::<UInt64Type>(left, right),
        (Int8, Int8) => compare_primitives::<Int8Type>(left, right),
        (Int16, Int16) => compare_primitives::<Int16Type>(left, right),
        (Int32, Int32) => compare_primitives::<Int32Type>(left, right),
        (Int64, Int64) => compare_primitives::<Int64Type>(left, right),
        (Float32, Float32) => compare_float::<Float32Type>(left, right),
        (Float64, Float64) => compare_float::<Float64Type>(left, right),
        (Date32, Date32) => compare_primitives::<Date32Type>(left, right),
        (Date64, Date64) => compare_primitives::<Date64Type>(left, right),
        (Time32(Second), Time32(Second)) => {
            compare_primitives::<Time32SecondType>(left, right)
        }
        (Time32(Millisecond), Time32(Millisecond)) => {
            compare_primitives::<Time32MillisecondType>(left, right)
        }
        (Time64(Microsecond), Time64(Microsecond)) => {
            compare_primitives::<Time64MicrosecondType>(left, right)
        }
        (Time64(Nanosecond), Time64(Nanosecond)) => {
            compare_primitives::<Time64NanosecondType>(left, right)
        }
        (Timestamp(Second, _), Timestamp(Second, _)) => {
            compare_primitives::<TimestampSecondType>(left, right)
        }
        (Timestamp(Millisecond, _), Timestamp(Millisecond, _)) => {
            compare_primitives::<TimestampMillisecondType>(left, right)
        }
        (Timestamp(Microsecond, _), Timestamp(Microsecond, _)) => {
            compare_primitives::<TimestampMicrosecondType>(left, right)
        }
        (Timestamp(Nanosecond, _), Timestamp(Nanosecond, _)) => {
            compare_primitives::<TimestampNanosecondType>(left, right)
        }
        (Interval(YearMonth), Interval(YearMonth)) => {
            compare_primitives::<IntervalYearMonthType>(left, right)
        }
        (Interval(DayTime), Interval(DayTime)) => {
            compare_primitives::<IntervalDayTimeType>(left, right)
        }
        (Interval(MonthDayNano), Interval(MonthDayNano)) => {
            compare_primitives::<IntervalMonthDayNanoType>(left, right)
        }
        (Duration(Second), Duration(Second)) => {
            compare_primitives::<DurationSecondType>(left, right)
        }
        (Duration(Millisecond), Duration(Millisecond)) => {
            compare_primitives::<DurationMillisecondType>(left, right)
        }
        (Duration(Microsecond), Duration(Microsecond)) => {
            compare_primitives::<DurationMicrosecondType>(left, right)
        }
        (Duration(Nanosecond), Duration(Nanosecond)) => {
            compare_primitives::<DurationNanosecondType>(left, right)
        }
        (Utf8, Utf8) => compare_string::<i32>(left, right),
        (LargeUtf8, LargeUtf8) => compare_string::<i64>(left, right),
        (
            Dictionary(key_type_lhs, value_type_lhs),
            Dictionary(key_type_rhs, value_type_rhs),
        ) => {
            if key_type_lhs != key_type_rhs || value_type_lhs != value_type_rhs {
                return Err(ArrowError::InvalidArgumentError(
                    "Can't compare arrays of different types".to_string(),
                ));
            }

            let key_type_lhs = key_type_lhs.as_ref();

            match value_type_lhs.as_ref() {
                Int8 => cmp_dict_primitive::<Int8Type>(key_type_lhs, left, right)?,
                Int16 => cmp_dict_primitive::<Int16Type>(key_type_lhs, left, right)?,
                Int32 => cmp_dict_primitive::<Int32Type>(key_type_lhs, left, right)?,
                Int64 => cmp_dict_primitive::<Int64Type>(key_type_lhs, left, right)?,
                UInt8 => cmp_dict_primitive::<UInt8Type>(key_type_lhs, left, right)?,
                UInt16 => cmp_dict_primitive::<UInt16Type>(key_type_lhs, left, right)?,
                UInt32 => cmp_dict_primitive::<UInt32Type>(key_type_lhs, left, right)?,
                UInt64 => cmp_dict_primitive::<UInt64Type>(key_type_lhs, left, right)?,
                Utf8 => match key_type_lhs {
                    UInt8 => compare_dict_string::<UInt8Type>(left, right),
                    UInt16 => compare_dict_string::<UInt16Type>(left, right),
                    UInt32 => compare_dict_string::<UInt32Type>(left, right),
                    UInt64 => compare_dict_string::<UInt64Type>(left, right),
                    Int8 => compare_dict_string::<Int8Type>(left, right),
                    Int16 => compare_dict_string::<Int16Type>(left, right),
                    Int32 => compare_dict_string::<Int32Type>(left, right),
                    Int64 => compare_dict_string::<Int64Type>(left, right),
                    lhs => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Dictionaries do not support keys of type {:?}",
                            lhs
                        )));
                    }
                },
                t => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionaries of value data type {:?} are not supported",
                        t
                    )));
                }
            }
        }
        (Decimal128(_, _), Decimal128(_, _)) => {
            let left: Decimal128Array = Decimal128Array::from(left.data().clone());
            let right: Decimal128Array = Decimal128Array::from(right.data().clone());
            Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
        }
        (FixedSizeBinary(_), FixedSizeBinary(_)) => {
            let left: FixedSizeBinaryArray =
                FixedSizeBinaryArray::from(left.data().clone());
            let right: FixedSizeBinaryArray =
                FixedSizeBinaryArray::from(right.data().clone());

            Box::new(move |i, j| left.value(i).cmp(right.value(j)))
        }
        (lhs, _) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The data type type {:?} has no natural order",
                lhs
            )));
        }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow_array::{FixedSizeBinaryArray, Float64Array, Int32Array};
    use std::cmp::Ordering;

    #[test]
    fn test_fixed_size_binary() {
        let items = vec![vec![1u8], vec![2u8]];
        let array = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
    }

    #[test]
    fn test_fixed_size_binary_fixed_size_binary() {
        let items = vec![vec![1u8]];
        let array1 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();
        let items = vec![vec![2u8]];
        let array2 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 0));
    }

    #[test]
    fn test_i32() {
        let array = Int32Array::from(vec![1, 2]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
    }

    #[test]
    fn test_i32_i32() {
        let array1 = Int32Array::from(vec![1]);
        let array2 = Int32Array::from(vec![2]);

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 0));
    }

    #[test]
    fn test_f64() {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
    }

    #[test]
    fn test_f64_nan() {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
    }

    #[test]
    fn test_f64_zeros() {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Equal, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(1, 0));
    }

    #[test]
    fn test_decimal() {
        let array = vec![Some(5_i128), Some(2_i128), Some(3_i128)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        let cmp = build_compare(&array, &array).unwrap();
        assert_eq!(Ordering::Less, (cmp)(1, 0));
        assert_eq!(Ordering::Greater, (cmp)(0, 2));
    }

    #[test]
    fn test_dict() {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = data.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
    }

    #[test]
    fn test_multiple_dict() {
        let d1 = vec!["a", "b", "c", "d"];
        let a1 = d1.into_iter().collect::<DictionaryArray<Int16Type>>();
        let d2 = vec!["e", "f", "g", "a"];
        let a2 = d2.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = build_compare(&a1, &a2).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        assert_eq!(Ordering::Equal, (cmp)(0, 3));
        assert_eq!(Ordering::Greater, (cmp)(1, 3));
    }

    #[test]
    fn test_primitive_dict() {
        let values = Int32Array::from(vec![1_i32, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();

        let values = Int32Array::from(vec![2_i32, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        assert_eq!(Ordering::Less, (cmp)(0, 3));
        assert_eq!(Ordering::Equal, (cmp)(3, 3));
        assert_eq!(Ordering::Greater, (cmp)(3, 1));
        assert_eq!(Ordering::Greater, (cmp)(3, 2));
    }
}
