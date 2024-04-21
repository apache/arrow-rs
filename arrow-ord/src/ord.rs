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
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq)]
pub enum Compare {
    Less,
    Greater,
    Equal,
    LeftNull,
    RightNull,
    BothNull,
}

impl Compare {
    pub fn ordering(&self, null_first: bool) -> Ordering {
        match self {
            Self::Less => Ordering::Less,
            Self::Greater => Ordering::Greater,
            Self::Equal => Ordering::Equal,
            Self::LeftNull => {
                if null_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            Self::RightNull => {
                if null_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            Self::BothNull => Ordering::Equal,
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::LeftNull | Self::RightNull | Self::BothNull)
    }

    #[inline]
    pub const fn reverse(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::Greater => Self::Less,
            _ => self,
        }
    }
}

impl From<Ordering> for Compare {
    fn from(ordering: Ordering) -> Self {
        match ordering {
            Ordering::Less => Self::Less,
            Ordering::Greater => Self::Greater,
            Ordering::Equal => Self::Equal,
        }
    }
}

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Compare + Send + Sync>;

fn compare_primitive<T: ArrowPrimitiveType>(left: &dyn Array, right: &dyn Array) -> DynComparator
where
    T::Native: ArrowNativeTypeOp,
{
    let left = left.as_primitive::<T>().clone();
    let right = right.as_primitive::<T>().clone();
    Box::new(move |i, j| match (left.is_null(i), right.is_null(j)) {
        (true, true) => Compare::BothNull,
        (true, false) => Compare::LeftNull,
        (false, true) => Compare::RightNull,
        (false, false) => left.value(i).compare(right.value(j)).into(),
    })
}

fn compare_boolean(left: &dyn Array, right: &dyn Array) -> DynComparator {
    let left: BooleanArray = left.as_boolean().clone();
    let right: BooleanArray = right.as_boolean().clone();

    Box::new(move |i, j| match (left.is_null(i), right.is_null(j)) {
        (true, true) => Compare::BothNull,
        (true, false) => Compare::LeftNull,
        (false, true) => Compare::RightNull,
        (false, false) => left.value(i).cmp(&right.value(j)).into(),
    })
}

fn compare_bytes<T: ByteArrayType>(left: &dyn Array, right: &dyn Array) -> DynComparator {
    let left = left.as_bytes::<T>().clone();
    let right = right.as_bytes::<T>().clone();

    Box::new(move |i, j| match (left.is_null(i), right.is_null(j)) {
        (true, true) => Compare::BothNull,
        (true, false) => Compare::LeftNull,
        (false, true) => Compare::RightNull,
        (false, false) => {
            let l: &[u8] = left.value(i).as_ref();
            let r: &[u8] = right.value(j).as_ref();
            l.cmp(r).into()
        }
    })
}

fn compare_dict<K: ArrowDictionaryKeyType>(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_dictionary::<K>();
    let right = right.as_dictionary::<K>();

    let cmp = build_compare(left.values().as_ref(), right.values().as_ref())?;
    let left_keys = left.keys().clone();
    let right_keys = right.keys().clone();

    Ok(Box::new(move |i, j| {
        match (left_keys.is_null(i), right_keys.is_null(j)) {
            (true, true) => Compare::BothNull,
            (true, false) => Compare::LeftNull,
            (false, true) => Compare::RightNull,
            (false, false) => {
                let l = left_keys.value(i).as_usize();
                let r = right_keys.value(j).as_usize();
                cmp(l, r).into()
            }
        }
    }))
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
/// assert_eq!(std::cmp::Ordering::Less, cmp(0, 1));
/// ```
// This is a factory of comparisons.
// The lifetime 'a enforces that we cannot use the closure beyond any of the array's lifetime.
pub fn build_compare(left: &dyn Array, right: &dyn Array) -> Result<DynComparator, ArrowError> {
    use arrow_schema::DataType::*;
    macro_rules! primitive_helper {
        ($t:ty, $left:expr, $right:expr) => {
            Ok(compare_primitive::<$t>($left, $right))
        };
    }
    downcast_primitive! {
        left.data_type(), right.data_type() => (primitive_helper, left, right),
        (Boolean, Boolean) => Ok(compare_boolean(left, right)),
        (Utf8, Utf8) => Ok(compare_bytes::<Utf8Type>(left, right)),
        (LargeUtf8, LargeUtf8) => Ok(compare_bytes::<LargeUtf8Type>(left, right)),
        (Binary, Binary) => Ok(compare_bytes::<BinaryType>(left, right)),
        (LargeBinary, LargeBinary) => Ok(compare_bytes::<LargeBinaryType>(left, right)),
        (FixedSizeBinary(_), FixedSizeBinary(_)) => {
            let left = left.as_fixed_size_binary().clone();
            let right = right.as_fixed_size_binary().clone();
            Ok(Box::new(move |i, j| {
                match (left.is_null(i), right.is_null(j)) {
                    (true, true) => Compare::BothNull,
                    (true, false) => Compare::LeftNull,
                    (false, true) => Compare::RightNull,
                    (false, false) => {
                        let l = left.value(i).as_ref();
                        let r = right.value(j).as_ref();
                        l.cmp(r).into()
                    }
                }
            }))
        },
        (Dictionary(l_key, _), Dictionary(r_key, _)) => {
             macro_rules! dict_helper {
                ($t:ty, $left:expr, $right:expr) => {
                     compare_dict::<$t>($left, $right)
                 };
             }
            downcast_integer! {
                 l_key.as_ref(), r_key.as_ref() => (dict_helper, left, right),
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
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_buffer::{i256, OffsetBuffer};
    use half::f16;
    use std::sync::Arc;

    #[test]
    fn test_fixed_size_binary() {
        let input_arg = vec![
            None,
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
        ];
        let a1 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 2).unwrap();

        let input_arg = vec![
            Some(vec![7, 8]),
            Some(vec![1, 3]),
            None,
            Some(vec![238, 3]),
            None,
        ];
        let a2 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(input_arg.into_iter(), 2).unwrap();

        let cmp = build_compare(&a1, &a2).unwrap();

        assert_eq!(Compare::Equal, cmp(1, 0));
        assert_eq!(Compare::LeftNull, cmp(0, 0));
        assert_eq!(Compare::Greater, cmp(2, 1));
        assert_eq!(Compare::BothNull, cmp(0, 2));
        assert_eq!(Compare::LeftNull, cmp(3, 3));
        assert_eq!(Compare::RightNull, cmp(4, 2));
    }

    #[test]
    fn test_fixed_size_binary_fixed_size_binary() {
        let items = vec![vec![1u8]];
        let array1 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();
        let items = vec![vec![2u8]];
        let array2 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
    }

    #[test]
    fn test_i32() {
        let a1 = Int32Array::from(vec![Some(1), None, Some(5)]);

        let cmp = build_compare(&a1, &a1).unwrap();
        assert_eq!(Compare::Less, cmp(0, 2));
        assert_eq!(Compare::BothNull, cmp(1, 1));

        let a2 = Int32Array::from(vec![Some(3), Some(4), None]);
        let cmp = build_compare(&a1, &a2).unwrap();
        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::LeftNull, cmp(1, 1));
        assert_eq!(Compare::RightNull, cmp(2, 2));
        assert_eq!(Compare::Greater, cmp(2, 0));
    }

    #[test]
    fn test_bool() {
        let a1 = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let a2 = BooleanArray::from(vec![Some(false), Some(true), None]);
        let cmp = build_compare(&a1, &a2).unwrap();
        assert_eq!(Compare::Greater, cmp(0, 0));
        assert_eq!(Compare::Equal, cmp(0, 1));
        assert_eq!(Compare::LeftNull, cmp(1, 0));
        assert_eq!(Compare::BothNull, cmp(1, 2));
        assert_eq!(Compare::RightNull, cmp(2, 2));
    }

    #[test]
    fn test_f16() {
        let array = Float16Array::from(vec![f16::from_f32(1.0), f16::from_f32(2.0)]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
    }

    #[test]
    fn test_f64() {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
    }

    #[test]
    fn test_f64_nan() {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Equal, cmp(1, 1));
    }

    #[test]
    fn test_f64_zeros() {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Greater, cmp(1, 0));
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

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Greater, cmp(1, 0));

        // somewhat confusingly, while 90M milliseconds is more than 1 day,
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(Compare::Greater, cmp(1, 2));
        assert_eq!(Compare::Less, cmp(2, 1));
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

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Greater, cmp(1, 0));

        // the underlying representation is months, so both quantities are the same
        assert_eq!(Compare::Equal, cmp(1, 2));
        assert_eq!(Compare::Equal, cmp(2, 1));
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

        let cmp = build_compare(&array, &array).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Greater, cmp(1, 0));

        // somewhat confusingly, while 100 days is more than 1 month in all cases
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(Compare::Greater, cmp(1, 2));
        assert_eq!(Compare::Less, cmp(2, 1));
    }

    #[test]
    fn test_decimal() {
        let array = vec![Some(5_i128), Some(2_i128), Some(3_i128)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        let cmp = build_compare(&array, &array).unwrap();
        assert_eq!(Compare::Less, cmp(1, 0));
        assert_eq!(Compare::Greater, cmp(0, 2));
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

        let cmp = build_compare(&array, &array).unwrap();
        assert_eq!(Compare::Less, cmp(1, 0));
        assert_eq!(Compare::Greater, cmp(0, 2));
    }

    #[test]
    fn test_dict() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("a");
        builder.append_value("b");
        builder.append_null();
        builder.append_value("b");
        builder.append_null();
        builder.append_null();
        builder.append_value("c");
        let a1 = builder.finish();

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_null();
        builder.append_value("a");
        builder.append_value("b");
        builder.append_value("b");
        builder.append_value("c");
        builder.append_null();
        builder.append_null();
        let a2 = builder.finish();

        let cmp = build_compare(&a1, &a2).unwrap();

        assert_eq!(Compare::RightNull, cmp(0, 0));
        assert_eq!(Compare::LeftNull, cmp(2, 1));
        assert_eq!(Compare::Equal, cmp(1, 2));
        assert_eq!(Compare::LeftNull, cmp(2, 3));
        assert_eq!(Compare::BothNull, cmp(2, 0));
        assert_eq!(Compare::Greater, cmp(6, 1));
    }

    #[test]
    fn test_multiple_dict() {
        let d1 = vec!["a", "b", "c", "d"];
        let a1 = d1.into_iter().collect::<DictionaryArray<Int16Type>>();
        let d2 = vec!["e", "f", "g", "a"];
        let a2 = d2.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = build_compare(&a1, &a2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Equal, cmp(0, 3));
        assert_eq!(Compare::Greater, cmp(1, 3));
    }

    #[test]
    fn test_primitive_dict() {
        let values = Int32Array::from(vec![1_i32, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Int32Array::from(vec![2_i32, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    #[test]
    fn test_float_dict() {
        let values = Float32Array::from(vec![1.0, 0.5, 2.1, 5.5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        let values = Float32Array::from(vec![1.2, 3.2, 4.0, 5.5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    #[test]
    fn test_timestamp_dict() {
        let values = TimestampSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = TimestampSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    #[test]
    fn test_interval_dict() {
        let values = IntervalDayTimeArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = IntervalDayTimeArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    #[test]
    fn test_duration_dict() {
        let values = DurationSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = DurationSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    #[test]
    fn test_decimal_dict() {
        let values = Decimal128Array::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal128Array::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
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

        let cmp = build_compare(&array1, &array2).unwrap();

        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Less, cmp(0, 3));
        assert_eq!(Compare::Equal, cmp(3, 3));
        assert_eq!(Compare::Greater, cmp(3, 1));
        assert_eq!(Compare::Greater, cmp(3, 2));
    }

    fn test_bytes_impl<T: ByteArrayType>() {
        let offsets = OffsetBuffer::from_lengths([3, 3, 1]);
        let a = GenericByteArray::<T>::new(offsets, b"abcdefa".into(), None);
        let cmp = build_compare(&a, &a).unwrap();

        assert_eq!(Compare::Less, cmp(0, 1));
        assert_eq!(Compare::Greater, cmp(0, 2));
        assert_eq!(Compare::Equal, cmp(1, 1));
    }

    #[test]
    fn test_string() {
        let a1 = StringArray::from(vec![Some("a"), None, Some("abcd")]);
        let a2 = StringArray::from(vec![Some("ab"), Some("abcd"), None]);
        let cmp = build_compare(&a1, &a2).unwrap();
        assert_eq!(Compare::Less, cmp(0, 0));
        assert_eq!(Compare::Equal, cmp(2, 1));
        assert_eq!(Compare::Greater, cmp(2, 0));
        assert_eq!(Compare::RightNull, cmp(0, 2));
        assert_eq!(Compare::BothNull, cmp(1, 2));
        assert_eq!(Compare::LeftNull, cmp(1, 1));
    }

    #[test]
    fn test_bytes() {
        test_bytes_impl::<Utf8Type>();
        test_bytes_impl::<LargeUtf8Type>();
        test_bytes_impl::<BinaryType>();
        test_bytes_impl::<LargeBinaryType>();
    }
}
