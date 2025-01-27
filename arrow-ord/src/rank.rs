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

//! Provides `rank` function to assign a rank to each value in an array

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{
    downcast_primitive_array, Array, ArrowNativeTypeOp, BooleanArray, GenericByteArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, SortOptions};
use std::cmp::Ordering;

/// Whether `arrow_ord::rank` can rank an array of given data type.
pub(crate) fn can_rank(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
}

/// Assigns a rank to each value in `array` based on its position in the sorted order
///
/// Where values are equal, they will be assigned the highest of their ranks,
/// leaving gaps in the overall rank assignment
///
/// ```
/// # use arrow_array::StringArray;
/// # use arrow_ord::rank::rank;
/// let array = StringArray::from(vec![Some("foo"), None, Some("foo"), None, Some("bar")]);
/// let ranks = rank(&array, None).unwrap();
/// assert_eq!(ranks, &[5, 2, 5, 2, 3]);
/// ```
pub fn rank(array: &dyn Array, options: Option<SortOptions>) -> Result<Vec<u32>, ArrowError> {
    let options = options.unwrap_or_default();
    let ranks = downcast_primitive_array! {
        array => primitive_rank(array.values(), array.nulls(), options),
        DataType::Boolean => boolean_rank(array.as_boolean(), options),
        DataType::Utf8 => bytes_rank(array.as_bytes::<Utf8Type>(), options),
        DataType::LargeUtf8 => bytes_rank(array.as_bytes::<LargeUtf8Type>(), options),
        DataType::Binary => bytes_rank(array.as_bytes::<BinaryType>(), options),
        DataType::LargeBinary => bytes_rank(array.as_bytes::<LargeBinaryType>(), options),
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
    };
    Ok(ranks)
}

#[inline(never)]
fn primitive_rank<T: ArrowNativeTypeOp>(
    values: &[T],
    nulls: Option<&NullBuffer>,
    options: SortOptions,
) -> Vec<u32> {
    let len: u32 = values.len().try_into().unwrap();
    let to_sort = match nulls.filter(|n| n.null_count() > 0) {
        Some(n) => n
            .valid_indices()
            .map(|idx| (values[idx], idx as u32))
            .collect(),
        None => values.iter().copied().zip(0..len).collect(),
    };
    rank_impl(values.len(), to_sort, options, T::compare, T::is_eq)
}

#[inline(never)]
fn bytes_rank<T: ByteArrayType>(array: &GenericByteArray<T>, options: SortOptions) -> Vec<u32> {
    let to_sort: Vec<(&[u8], u32)> = match array.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => n
            .valid_indices()
            .map(|idx| (array.value(idx).as_ref(), idx as u32))
            .collect(),
        None => (0..array.len())
            .map(|idx| (array.value(idx).as_ref(), idx as u32))
            .collect(),
    };
    rank_impl(array.len(), to_sort, options, Ord::cmp, PartialEq::eq)
}

fn rank_impl<T, C, E>(
    len: usize,
    mut valid: Vec<(T, u32)>,
    options: SortOptions,
    compare: C,
    eq: E,
) -> Vec<u32>
where
    T: Copy,
    C: Fn(T, T) -> Ordering,
    E: Fn(T, T) -> bool,
{
    // We can use an unstable sort as we combine equal values later
    valid.sort_unstable_by(|a, b| compare(a.0, b.0));
    if options.descending {
        valid.reverse();
    }

    let (mut valid_rank, null_rank) = match options.nulls_first {
        true => (len as u32, (len - valid.len()) as u32),
        false => (valid.len() as u32, len as u32),
    };

    let mut out: Vec<_> = vec![null_rank; len];
    if let Some(v) = valid.last() {
        out[v.1 as usize] = valid_rank;
    }

    let mut count = 1; // Number of values in rank
    for w in valid.windows(2).rev() {
        match eq(w[0].0, w[1].0) {
            true => {
                count += 1;
                out[w[0].1 as usize] = valid_rank;
            }
            false => {
                valid_rank -= count;
                count = 1;
                out[w[0].1 as usize] = valid_rank
            }
        }
    }

    out
}

/// Return the index for the rank when ranking boolean array
///
/// The index is calculated as follows:
/// if is_null is true, the index is 2
/// if is_null is false and the value is true, the index is 1
/// otherwise, the index is 0
///
/// false is 0 and true is 1 because these are the value when cast to number
#[inline]
fn get_boolean_rank_index(value: bool, is_null: bool) -> usize {
    let is_null_num = is_null as usize;
    (is_null_num << 1) | (value as usize & !is_null_num)
}

#[inline(never)]
fn boolean_rank(array: &BooleanArray, options: SortOptions) -> Vec<u32> {
    let null_count = array.null_count() as u32;
    let true_count = array.true_count() as u32;
    let false_count = array.len() as u32 - null_count - true_count;

    // Rank values for [false, true, null] in that order
    //
    // The value for a rank is last value rank + own value count
    // this means that if we have the following order: `false`, `true` and then `null`
    // the ranks will be:
    // - false: false_count
    // - true: false_count + true_count
    // - null: false_count + true_count + null_count
    //
    // If we have the following order: `null`, `false` and then `true`
    // the ranks will be:
    // - false: null_count + false_count
    // - true: null_count + false_count + true_count
    // - null: null_count
    //
    // You will notice that the last rank is always the total length of the array but we don't use it for readability on how the rank is calculated
    let ranks_index: [u32; 3] = match (options.descending, options.nulls_first) {
        // The order is null, true, false
        (true, true) => [
            null_count + true_count + false_count,
            null_count + true_count,
            null_count,
        ],
        // The order is true, false, null
        (true, false) => [
            true_count + false_count,
            true_count,
            true_count + false_count + null_count,
        ],
        // The order is null, false, true
        (false, true) => [
            null_count + false_count,
            null_count + false_count + true_count,
            null_count,
        ],
        // The order is false, true, null
        (false, false) => [
            false_count,
            false_count + true_count,
            false_count + true_count + null_count,
        ],
    };

    match array.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => array
            .values()
            .iter()
            .zip(n.iter())
            .map(|(value, is_valid)| ranks_index[get_boolean_rank_index(value, !is_valid)])
            .collect::<Vec<u32>>(),
        None => array
            .values()
            .iter()
            .map(|value| ranks_index[value as usize])
            .collect::<Vec<u32>>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;

    #[test]
    fn test_primitive() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = Int32Array::from(vec![Some(1), Some(1), None, Some(3), Some(3), Some(4)]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[3, 3, 1, 5, 5, 6]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[6, 6, 1, 4, 4, 2]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[2, 2, 6, 4, 4, 5]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[5, 5, 6, 3, 3, 1]);

        // Test with non-zero null values
        let nulls = NullBuffer::from(vec![true, true, false, true, false, false]);
        let a = Int32Array::new(vec![1, 4, 3, 4, 5, 5].into(), Some(nulls));
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[4, 6, 3, 6, 3, 3]);
    }

    #[test]
    fn test_get_boolean_rank_index() {
        assert_eq!(get_boolean_rank_index(true, true), 2);
        assert_eq!(get_boolean_rank_index(false, true), 2);
        assert_eq!(get_boolean_rank_index(true, false), 1);
        assert_eq!(get_boolean_rank_index(false, false), 0);
    }

    #[test]
    fn test_nullable_booleans() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), Some(false)]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 5, 1, 3, 3]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[3, 3, 1, 5, 5]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[4, 4, 5, 2, 2]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[2, 2, 5, 4, 4]);

        // Test with non-zero null values
        let nulls = NullBuffer::from(vec![true, true, false, true, true]);
        let a = BooleanArray::new(vec![true, true, true, false, false].into(), Some(nulls));
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 5, 1, 3, 3]);
    }

    #[test]
    fn test_booleans() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = BooleanArray::from(vec![true, false, false, false, true]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 3, 3, 3, 5]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[2, 5, 5, 5, 2]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[5, 3, 3, 3, 5]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[2, 5, 5, 5, 2]);
    }

    #[test]
    fn test_bytes() {
        let v = vec!["foo", "fo", "bar", "bar"];
        let values = StringArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[4, 3, 2, 2]);

        let values = LargeStringArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[4, 3, 2, 2]);

        let v: Vec<&[u8]> = vec![&[1, 2], &[0], &[1, 2, 3], &[1, 2]];
        let values = LargeBinaryArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 1, 4, 3]);

        let values = BinaryArray::from(v);
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 1, 4, 3]);
    }
}
