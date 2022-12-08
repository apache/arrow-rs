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

//! Defines sort kernel for `ArrayRef`

use crate::ord::{build_compare, DynComparator};
use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};
use arrow_select::take::take;
use std::cmp::Ordering;

/// Sort the `ArrayRef` using `SortOptions`.
///
/// Performs a sort on values and indices. Nulls are ordered according
/// to the `nulls_first` flag in `options`.  Floats are sorted using
/// IEEE 754 totalOrder
///
/// Returns an `ArrowError::ComputeError(String)` if the array type is
/// either unsupported by `sort_to_indices` or `take`.
///
/// Note: this is an unstable_sort, meaning it may not preserve the
/// order of equal elements.
///
/// # Example
/// ```rust
/// # use std::sync::Arc;
/// # use arrow_array::{Int32Array, ArrayRef};
/// # use arrow_ord::sort::sort;
/// let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1]));
/// let sorted_array = sort(&array, None).unwrap();
/// let sorted_array = sorted_array.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(sorted_array, &Int32Array::from(vec![1, 2, 3, 4, 5]));
/// ```
pub fn sort(
    values: &ArrayRef,
    options: Option<SortOptions>,
) -> Result<ArrayRef, ArrowError> {
    let indices = sort_to_indices(values, options, None)?;
    take(values.as_ref(), &indices, None)
}

/// Sort the `ArrayRef` partially.
///
/// If `limit` is specified, the resulting array will contain only
/// first `limit` in the sort order. Any data data after the limit
/// will be discarded.
///
/// Note: this is an unstable_sort, meaning it may not preserve the
/// order of equal elements.
///
/// # Example
/// ```rust
/// # use std::sync::Arc;
/// # use arrow_array::{Int32Array, ArrayRef};
/// # use arrow_ord::sort::{sort_limit, SortOptions};
/// let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1]));
///
/// // Find the the top 2 items
/// let sorted_array = sort_limit(&array, None, Some(2)).unwrap();
/// let sorted_array = sorted_array.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(sorted_array, &Int32Array::from(vec![1, 2]));
///
/// // Find the bottom top 2 items
/// let options = Some(SortOptions {
///                  descending: true,
///                  ..Default::default()
///               });
/// let sorted_array = sort_limit(&array, options, Some(2)).unwrap();
/// let sorted_array = sorted_array.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(sorted_array, &Int32Array::from(vec![5, 4]));
/// ```
pub fn sort_limit(
    values: &ArrayRef,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<ArrayRef, ArrowError> {
    let indices = sort_to_indices(values, options, limit)?;
    take(values.as_ref(), &indices, None)
}

/// we can only do this if the T is primitive
#[inline]
fn sort_unstable_by<T, F>(array: &mut [T], limit: usize, cmp: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    if array.len() == limit {
        array.sort_unstable_by(cmp);
    } else {
        partial_sort(array, limit, cmp);
    }
}

fn cmp<T>(l: T, r: T) -> Ordering
where
    T: Ord,
{
    l.cmp(&r)
}

// partition indices into valid and null indices
fn partition_validity(array: &ArrayRef) -> (Vec<u32>, Vec<u32>) {
    match array.null_count() {
        // faster path
        0 => ((0..(array.len() as u32)).collect(), vec![]),
        _ => {
            let indices = 0..(array.len() as u32);
            indices.partition(|index| array.is_valid(*index as usize))
        }
    }
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
/// limit is an option for partial_sort
pub fn sort_to_indices(
    values: &ArrayRef,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    let options = options.unwrap_or_default();

    let (v, n) = partition_validity(values);

    Ok(match values.data_type() {
        DataType::Decimal128(_, _) => {
            sort_primitive::<Decimal128Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Decimal256(_, _) => {
            sort_primitive::<Decimal256Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Boolean => sort_boolean(values, v, n, &options, limit),
        DataType::Int8 => {
            sort_primitive::<Int8Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Int16 => {
            sort_primitive::<Int16Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Int32 => {
            sort_primitive::<Int32Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Int64 => {
            sort_primitive::<Int64Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::UInt8 => {
            sort_primitive::<UInt8Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::UInt16 => {
            sort_primitive::<UInt16Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::UInt32 => {
            sort_primitive::<UInt32Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::UInt64 => {
            sort_primitive::<UInt64Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Float32 => sort_primitive::<Float32Type, _>(
            values,
            v,
            n,
            |x, y| x.total_cmp(&y),
            &options,
            limit,
        ),
        DataType::Float64 => sort_primitive::<Float64Type, _>(
            values,
            v,
            n,
            |x, y| x.total_cmp(&y),
            &options,
            limit,
        ),
        DataType::Date32 => {
            sort_primitive::<Date32Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Date64 => {
            sort_primitive::<Date64Type, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Time32(TimeUnit::Second) => {
            sort_primitive::<Time32SecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            sort_primitive::<Time32MillisecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            sort_primitive::<Time64MicrosecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            sort_primitive::<Time64NanosecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            sort_primitive::<TimestampSecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            sort_primitive::<TimestampMillisecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            sort_primitive::<TimestampMicrosecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            sort_primitive::<TimestampNanosecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            sort_primitive::<IntervalYearMonthType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            sort_primitive::<IntervalDayTimeType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            sort_primitive::<IntervalMonthDayNanoType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Duration(TimeUnit::Second) => {
            sort_primitive::<DurationSecondType, _>(values, v, n, cmp, &options, limit)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            sort_primitive::<DurationMillisecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            sort_primitive::<DurationMicrosecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            sort_primitive::<DurationNanosecondType, _>(
                values, v, n, cmp, &options, limit,
            )
        }
        DataType::Utf8 => sort_string::<i32>(values, v, n, &options, limit),
        DataType::LargeUtf8 => sort_string::<i64>(values, v, n, &options, limit),
        DataType::List(field) | DataType::FixedSizeList(field, _) => match field
            .data_type()
        {
            DataType::Int8 => sort_list::<i32, Int8Type>(values, v, n, &options, limit),
            DataType::Int16 => sort_list::<i32, Int16Type>(values, v, n, &options, limit),
            DataType::Int32 => sort_list::<i32, Int32Type>(values, v, n, &options, limit),
            DataType::Int64 => sort_list::<i32, Int64Type>(values, v, n, &options, limit),
            DataType::UInt8 => sort_list::<i32, UInt8Type>(values, v, n, &options, limit),
            DataType::UInt16 => {
                sort_list::<i32, UInt16Type>(values, v, n, &options, limit)
            }
            DataType::UInt32 => {
                sort_list::<i32, UInt32Type>(values, v, n, &options, limit)
            }
            DataType::UInt64 => {
                sort_list::<i32, UInt64Type>(values, v, n, &options, limit)
            }
            DataType::Float32 => {
                sort_list::<i32, Float32Type>(values, v, n, &options, limit)
            }
            DataType::Float64 => {
                sort_list::<i32, Float64Type>(values, v, n, &options, limit)
            }
            t => {
                return Err(ArrowError::ComputeError(format!(
                    "Sort not supported for list type {:?}",
                    t
                )));
            }
        },
        DataType::LargeList(field) => match field.data_type() {
            DataType::Int8 => sort_list::<i64, Int8Type>(values, v, n, &options, limit),
            DataType::Int16 => sort_list::<i64, Int16Type>(values, v, n, &options, limit),
            DataType::Int32 => sort_list::<i64, Int32Type>(values, v, n, &options, limit),
            DataType::Int64 => sort_list::<i64, Int64Type>(values, v, n, &options, limit),
            DataType::UInt8 => sort_list::<i64, UInt8Type>(values, v, n, &options, limit),
            DataType::UInt16 => {
                sort_list::<i64, UInt16Type>(values, v, n, &options, limit)
            }
            DataType::UInt32 => {
                sort_list::<i64, UInt32Type>(values, v, n, &options, limit)
            }
            DataType::UInt64 => {
                sort_list::<i64, UInt64Type>(values, v, n, &options, limit)
            }
            DataType::Float32 => {
                sort_list::<i64, Float32Type>(values, v, n, &options, limit)
            }
            DataType::Float64 => {
                sort_list::<i64, Float64Type>(values, v, n, &options, limit)
            }
            t => {
                return Err(ArrowError::ComputeError(format!(
                    "Sort not supported for list type {:?}",
                    t
                )));
            }
        },
        DataType::Dictionary(_, _) => {
            let value_null_first = if options.descending {
                // When sorting dictionary in descending order, we take inverse of of null ordering
                // when sorting the values. Because if `nulls_first` is true, null must be in front
                // of non-null value. As we take the sorted order of value array to sort dictionary
                // keys, these null values will be treated as smallest ones and be sorted to the end
                // of sorted result. So we set `nulls_first` to false when sorting dictionary value
                // array to make them as largest ones, then null values will be put at the beginning
                // of sorted dictionary result.
                !options.nulls_first
            } else {
                options.nulls_first
            };
            let value_options = Some(SortOptions {
                descending: false,
                nulls_first: value_null_first,
            });
            downcast_dictionary_array!(
                values => match values.values().data_type() {
                    dt if DataType::is_primitive(dt) => {
                        let dict_values = values.values();
                        let sorted_value_indices = sort_to_indices(dict_values, value_options, None)?;
                        let value_indices_map = sorted_rank(&sorted_value_indices);
                        sort_primitive_dictionary::<_, _>(values, &value_indices_map, v, n, options, limit, cmp)
                    },
                    DataType::Utf8 => {
                        let dict_values = values.values();
                        let sorted_value_indices = sort_to_indices(dict_values, value_options, None)?;
                        let value_indices_map = sorted_rank(&sorted_value_indices);
                        sort_string_dictionary::<_>(values, &value_indices_map, v, n, &options, limit)
                    },
                    t => return Err(ArrowError::ComputeError(format!(
                        "Unsupported dictionary value type {}", t
                    ))),
                },
                t => return Err(ArrowError::ComputeError(format!(
                    "Unsupported datatype {}", t
                ))),
            )
        }
        DataType::Binary | DataType::FixedSizeBinary(_) => {
            sort_binary::<i32>(values, v, n, &options, limit)
        }
        DataType::LargeBinary => sort_binary::<i64>(values, v, n, &options, limit),
        t => {
            return Err(ArrowError::ComputeError(format!(
                "Sort not supported for data type {:?}",
                t
            )));
        }
    })
}

/// Options that define how sort kernels should behave
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            // default to nulls first to match spark's behavior
            nulls_first: true,
        }
    }
}

/// Sort boolean values
///
/// when a limit is present, the sort is pair-comparison based as k-select might be more efficient,
/// when the limit is absent, binary partition is used to speed up (which is linear).
///
/// TODO maybe partition_validity call can be eliminated in this case
/// and [tri-color sort](https://en.wikipedia.org/wiki/Dutch_national_flag_problem)
/// can be used instead.
fn sort_boolean(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    mut null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Unable to downcast to boolean array");
    let descending = options.descending;

    let valids_len = value_indices.len();
    let nulls_len = null_indices.len();

    let mut len = values.len();
    let valids = if let Some(limit) = limit {
        len = limit.min(len);
        // create tuples that are used for sorting
        let mut valids = value_indices
            .into_iter()
            .map(|index| (index, values.value(index as usize)))
            .collect::<Vec<(u32, bool)>>();

        sort_valids(descending, &mut valids, &mut null_indices, len, cmp);
        valids
    } else {
        // when limit is not present, we have a better way than sorting: we can just partition
        // the vec into [false..., true...] or [true..., false...] when descending
        // TODO when https://github.com/rust-lang/rust/issues/62543 is merged we can use partition_in_place
        let (mut a, b): (Vec<_>, Vec<_>) = value_indices
            .into_iter()
            .map(|index| (index, values.value(index as usize)))
            .partition(|(_, value)| *value == descending);
        a.extend(b);
        if descending {
            null_indices.reverse();
        }
        a
    };

    let nulls = null_indices;

    // collect results directly into a buffer instead of a vec to avoid another aligned allocation
    let result_capacity = len * std::mem::size_of::<u32>();
    let mut result = MutableBuffer::new(result_capacity);
    // sets len to capacity so we can access the whole buffer as a typed slice
    result.resize(result_capacity, 0);
    let result_slice: &mut [u32] = result.typed_data_mut();

    if options.nulls_first {
        let size = nulls_len.min(len);
        result_slice[0..size].copy_from_slice(&nulls[0..size]);
        if nulls_len < len {
            insert_valid_values(result_slice, nulls_len, &valids[0..len - size]);
        }
    } else {
        // nulls last
        let size = valids.len().min(len);
        insert_valid_values(result_slice, 0, &valids[0..size]);
        if len > size {
            result_slice[valids_len..].copy_from_slice(&nulls[0..(len - valids_len)]);
        }
    }

    let result_data = unsafe {
        ArrayData::new_unchecked(
            DataType::UInt32,
            len,
            Some(0),
            None,
            0,
            vec![result.into()],
            vec![],
        )
    };

    UInt32Array::from(result_data)
}

/// Sort primitive values
fn sort_primitive<T, F>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    cmp: F,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    F: Fn(T::Native, T::Native) -> Ordering,
{
    // create tuples that are used for sorting
    let valids = {
        let values = as_primitive_array::<T>(values);
        value_indices
            .into_iter()
            .map(|index| (index, values.value(index as usize)))
            .collect::<Vec<(u32, T::Native)>>()
    };
    sort_primitive_inner(values.len(), null_indices, cmp, options, limit, valids)
}

/// Given a list of indices that yield a sorted order, returns the ordered
/// rank of each index
///
/// e.g. [2, 4, 3, 1, 0] -> [4, 3, 0, 2, 1]
fn sorted_rank(sorted_value_indices: &UInt32Array) -> Vec<u32> {
    assert_eq!(sorted_value_indices.null_count(), 0);
    let sorted_indices = sorted_value_indices.values();
    let mut out: Vec<_> = (0..sorted_indices.len() as u32).collect();
    out.sort_unstable_by_key(|x| sorted_indices[*x as usize]);
    out
}

/// Sort dictionary encoded primitive values
fn sort_primitive_dictionary<K, F>(
    values: &DictionaryArray<K>,
    value_indices_map: &[u32],
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
    cmp: F,
) -> UInt32Array
where
    K: ArrowDictionaryKeyType,
    F: Fn(u32, u32) -> Ordering,
{
    let keys: &PrimitiveArray<K> = values.keys();

    // create tuples that are used for sorting
    let valids = value_indices
        .into_iter()
        .map(|index| {
            let key: K::Native = keys.value(index as usize);
            (index, value_indices_map[key.as_usize()])
        })
        .collect::<Vec<(u32, u32)>>();

    sort_primitive_inner::<_, _>(keys.len(), null_indices, cmp, &options, limit, valids)
}

// sort is instantiated a lot so we only compile this inner version for each native type
fn sort_primitive_inner<T, F>(
    value_len: usize,
    null_indices: Vec<u32>,
    cmp: F,
    options: &SortOptions,
    limit: Option<usize>,
    mut valids: Vec<(u32, T)>,
) -> UInt32Array
where
    T: ArrowNativeType,
    T: PartialOrd,
    F: Fn(T, T) -> Ordering,
{
    let mut nulls = null_indices;

    let valids_len = valids.len();
    let nulls_len = nulls.len();
    let mut len = value_len;

    if let Some(limit) = limit {
        len = limit.min(len);
    }

    sort_valids(options.descending, &mut valids, &mut nulls, len, cmp);

    // collect results directly into a buffer instead of a vec to avoid another aligned allocation
    let result_capacity = len * std::mem::size_of::<u32>();
    let mut result = MutableBuffer::new(result_capacity);
    // sets len to capacity so we can access the whole buffer as a typed slice
    result.resize(result_capacity, 0);
    let result_slice: &mut [u32] = result.typed_data_mut();

    if options.nulls_first {
        let size = nulls_len.min(len);
        result_slice[0..size].copy_from_slice(&nulls[0..size]);
        if nulls_len < len {
            insert_valid_values(result_slice, nulls_len, &valids[0..len - size]);
        }
    } else {
        // nulls last
        let size = valids.len().min(len);
        insert_valid_values(result_slice, 0, &valids[0..size]);
        if len > size {
            result_slice[valids_len..].copy_from_slice(&nulls[0..(len - valids_len)]);
        }
    }

    let result_data = unsafe {
        ArrayData::new_unchecked(
            DataType::UInt32,
            len,
            Some(0),
            None,
            0,
            vec![result.into()],
            vec![],
        )
    };

    UInt32Array::from(result_data)
}

// insert valid and nan values in the correct order depending on the descending flag
fn insert_valid_values<T>(result_slice: &mut [u32], offset: usize, valids: &[(u32, T)]) {
    let valids_len = valids.len();
    // helper to append the index part of the valid tuples
    let append_valids = move |dst_slice: &mut [u32]| {
        debug_assert_eq!(dst_slice.len(), valids_len);
        dst_slice
            .iter_mut()
            .zip(valids.iter())
            .for_each(|(dst, src)| *dst = src.0)
    };

    append_valids(&mut result_slice[offset..offset + valids.len()]);
}

/// Sort strings
fn sort_string<Offset: OffsetSizeTrait>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let values = values
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    sort_string_helper(
        values,
        value_indices,
        null_indices,
        options,
        limit,
        |array, idx| array.value(idx as usize),
    )
}

/// Sort dictionary encoded strings
fn sort_string_dictionary<T: ArrowDictionaryKeyType>(
    values: &DictionaryArray<T>,
    value_indices_map: &[u32],
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let keys: &PrimitiveArray<T> = values.keys();

    // create tuples that are used for sorting
    let valids = value_indices
        .into_iter()
        .map(|index| {
            let key: T::Native = keys.value(index as usize);
            (index, value_indices_map[key.as_usize()])
        })
        .collect::<Vec<(u32, u32)>>();

    sort_primitive_inner::<_, _>(keys.len(), null_indices, cmp, options, limit, valids)
}

/// shared implementation between dictionary encoded and plain string arrays
#[inline]
fn sort_string_helper<'a, A: Array, F>(
    values: &'a A,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
    value_fn: F,
) -> UInt32Array
where
    F: Fn(&'a A, u32) -> &str,
{
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, value_fn(values, index)))
        .collect::<Vec<(u32, &str)>>();
    let mut nulls = null_indices;
    let descending = options.descending;
    let mut len = values.len();

    if let Some(limit) = limit {
        len = limit.min(len);
    }

    sort_valids(descending, &mut valids, &mut nulls, len, cmp);
    // collect the order of valid tuplies
    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();

    if options.nulls_first {
        nulls.append(&mut valid_indices);
        nulls.truncate(len);
        UInt32Array::from(nulls)
    } else {
        // no need to sort nulls as they are in the correct order already
        valid_indices.append(&mut nulls);
        valid_indices.truncate(len);
        UInt32Array::from(valid_indices)
    }
}

fn sort_list<S, T>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array
where
    S: OffsetSizeTrait,
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
{
    sort_list_inner::<S>(values, value_indices, null_indices, options, limit)
}

fn sort_list_inner<S>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    mut null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array
where
    S: OffsetSizeTrait,
{
    let mut valids: Vec<(u32, ArrayRef)> = values
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .map_or_else(
            || {
                let values = as_generic_list_array::<S>(values);
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
            |values| {
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
        );

    let mut len = values.len();
    let descending = options.descending;

    if let Some(limit) = limit {
        len = limit.min(len);
    }
    sort_valids_array(descending, &mut valids, &mut null_indices, len);

    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();
    if options.nulls_first {
        null_indices.append(&mut valid_indices);
        null_indices.truncate(len);
        UInt32Array::from(null_indices)
    } else {
        valid_indices.append(&mut null_indices);
        valid_indices.truncate(len);
        UInt32Array::from(valid_indices)
    }
}

fn sort_binary<S>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    mut null_indices: Vec<u32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> UInt32Array
where
    S: OffsetSizeTrait,
{
    let mut valids: Vec<(u32, &[u8])> = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .map_or_else(
            || {
                let values = as_generic_binary_array::<S>(values);
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
            |values| {
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
        );

    let mut len = values.len();
    let descending = options.descending;

    if let Some(limit) = limit {
        len = limit.min(len);
    }

    sort_valids(descending, &mut valids, &mut null_indices, len, cmp);

    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();
    if options.nulls_first {
        null_indices.append(&mut valid_indices);
        null_indices.truncate(len);
        UInt32Array::from(null_indices)
    } else {
        valid_indices.append(&mut null_indices);
        valid_indices.truncate(len);
        UInt32Array::from(valid_indices)
    }
}

/// Compare two `Array`s based on the ordering defined in [build_compare]
fn cmp_array(a: &dyn Array, b: &dyn Array) -> Ordering {
    let cmp_op = build_compare(a, b).unwrap();
    let length = a.len().max(b.len());

    for i in 0..length {
        let result = cmp_op(i, i);
        if result != Ordering::Equal {
            return result;
        }
    }
    Ordering::Equal
}

/// One column to be used in lexicographical sort
#[derive(Clone, Debug)]
pub struct SortColumn {
    pub values: ArrayRef,
    pub options: Option<SortOptions>,
}

/// Sort a list of `ArrayRef` using `SortOptions` provided for each array.
///
/// Performs a stable lexicographical sort on values and indices.
///
/// Returns an `ArrowError::ComputeError(String)` if any of the array type is either unsupported by
/// `lexsort_to_indices` or `take`.
///
/// Example:
///
/// ```
/// # use std::convert::From;
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, StringArray, PrimitiveArray};
/// # use arrow_array::types::Int64Type;
/// # use arrow_array::cast::as_primitive_array;
/// # use arrow_ord::sort::{SortColumn, SortOptions, lexsort};
///
/// let sorted_columns = lexsort(&vec![
///     SortColumn {
///         values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
///             None,
///             Some(-2),
///             Some(89),
///             Some(-64),
///             Some(101),
///         ])) as ArrayRef,
///         options: None,
///     },
///     SortColumn {
///         values: Arc::new(StringArray::from(vec![
///             Some("hello"),
///             Some("world"),
///             Some(","),
///             Some("foobar"),
///             Some("!"),
///         ])) as ArrayRef,
///         options: Some(SortOptions {
///             descending: true,
///             nulls_first: false,
///         }),
///     },
/// ], None).unwrap();
///
/// assert_eq!(as_primitive_array::<Int64Type>(&sorted_columns[0]).value(1), -64);
/// assert!(sorted_columns[0].is_null(0));
/// ```
///
/// Note: for multi-column sorts without a limit, using the [row format](https://docs.rs/arrow/latest/arrow/row/)
/// may be significantly faster
///
pub fn lexsort(
    columns: &[SortColumn],
    limit: Option<usize>,
) -> Result<Vec<ArrayRef>, ArrowError> {
    let indices = lexsort_to_indices(columns, limit)?;
    columns
        .iter()
        .map(|c| take(c.values.as_ref(), &indices, None))
        .collect()
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
///
/// Note: for multi-column sorts without a limit, using the [row format](https://docs.rs/arrow/latest/arrow/row/)
/// may be significantly faster
pub fn lexsort_to_indices(
    columns: &[SortColumn],
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }
    if columns.len() == 1 {
        // fallback to non-lexical sort
        let column = &columns[0];
        return sort_to_indices(&column.values, column.options, limit);
    }

    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return Err(ArrowError::ComputeError(
            "lexical sort columns have different row counts".to_string(),
        ));
    };

    let mut value_indices = (0..row_count).collect::<Vec<usize>>();
    let mut len = value_indices.len();

    if let Some(limit) = limit {
        len = limit.min(len);
    }

    let lexicographical_comparator = LexicographicalComparator::try_new(columns)?;
    // uint32 can be sorted unstably
    sort_unstable_by(&mut value_indices, len, |a, b| {
        lexicographical_comparator.compare(*a, *b)
    });

    Ok(UInt32Array::from_iter_values(
        value_indices.iter().take(len).map(|i| *i as u32),
    ))
}

/// It's unstable_sort, may not preserve the order of equal elements
pub fn partial_sort<T, F>(v: &mut [T], limit: usize, mut is_less: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    let (before, _mid, _after) = v.select_nth_unstable_by(limit, &mut is_less);
    before.sort_unstable_by(is_less);
}

type LexicographicalCompareItem<'a> = (
    &'a ArrayData, // data
    DynComparator, // comparator
    SortOptions,   // sort_option
);

/// A lexicographical comparator that wraps given array data (columns) and can lexicographically compare data
/// at given two indices. The lifetime is the same at the data wrapped.
pub struct LexicographicalComparator<'a> {
    compare_items: Vec<LexicographicalCompareItem<'a>>,
}

impl LexicographicalComparator<'_> {
    /// lexicographically compare values at the wrapped columns with given indices.
    pub fn compare(&self, a_idx: usize, b_idx: usize) -> Ordering {
        for (data, comparator, sort_option) in &self.compare_items {
            match (data.is_valid(a_idx), data.is_valid(b_idx)) {
                (true, true) => {
                    match (comparator)(a_idx, b_idx) {
                        // equal, move on to next column
                        Ordering::Equal => continue,
                        order => {
                            if sort_option.descending {
                                return order.reverse();
                            } else {
                                return order;
                            }
                        }
                    }
                }
                (false, true) => {
                    return if sort_option.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (true, false) => {
                    return if sort_option.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                // equal, move on to next column
                (false, false) => continue,
            }
        }

        Ordering::Equal
    }

    /// Create a new lex comparator that will wrap the given sort columns and give comparison
    /// results with two indices.
    pub fn try_new(
        columns: &[SortColumn],
    ) -> Result<LexicographicalComparator<'_>, ArrowError> {
        let compare_items = columns
            .iter()
            .map(|column| {
                // flatten and convert build comparators
                // use ArrayData for is_valid checks later to avoid dynamic call
                let values = column.values.as_ref();
                let data = values.data_ref();
                Ok((
                    data,
                    build_compare(values, values)?,
                    column.options.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(LexicographicalComparator { compare_items })
    }
}

fn sort_valids<T, U>(
    descending: bool,
    valids: &mut [(u32, T)],
    nulls: &mut [U],
    len: usize,
    mut cmp: impl FnMut(T, T) -> Ordering,
) where
    T: ?Sized + Copy,
{
    let valids_len = valids.len();
    if !descending {
        sort_unstable_by(valids, len.min(valids_len), |a, b| cmp(a.1, b.1));
    } else {
        sort_unstable_by(valids, len.min(valids_len), |a, b| cmp(a.1, b.1).reverse());
        // reverse to keep a stable ordering
        nulls.reverse();
    }
}

fn sort_valids_array<T>(
    descending: bool,
    valids: &mut [(u32, ArrayRef)],
    nulls: &mut [T],
    len: usize,
) {
    let valids_len = valids.len();
    if !descending {
        sort_unstable_by(valids, len.min(valids_len), |a, b| {
            cmp_array(a.1.as_ref(), b.1.as_ref())
        });
    } else {
        sort_unstable_by(valids, len.min(valids_len), |a, b| {
            cmp_array(a.1.as_ref(), b.1.as_ref()).reverse()
        });
        // reverse to keep a stable ordering
        nulls.reverse();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::i256;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::convert::TryFrom;
    use std::sync::Arc;

    fn create_decimal128_array(data: Vec<Option<i128>>) -> Decimal128Array {
        data.into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap()
    }

    fn create_decimal256_array(data: Vec<Option<i256>>) -> Decimal256Array {
        data.into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(53, 6)
            .unwrap()
    }

    fn test_sort_to_indices_decimal128_array(
        data: Vec<Option<i128>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<u32>,
    ) {
        let output = create_decimal128_array(data);
        let expected = UInt32Array::from(expected_data);
        let output =
            sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_to_indices_decimal256_array(
        data: Vec<Option<i256>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<u32>,
    ) {
        let output = create_decimal256_array(data);
        let expected = UInt32Array::from(expected_data);
        let output =
            sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_decimal128_array(
        data: Vec<Option<i128>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<i128>>,
    ) {
        let output = create_decimal128_array(data);
        let expected = Arc::new(create_decimal128_array(expected_data)) as ArrayRef;
        let output = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected)
    }

    fn test_sort_decimal256_array(
        data: Vec<Option<i256>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<i256>>,
    ) {
        let output = create_decimal256_array(data);
        let expected = Arc::new(create_decimal256_array(expected_data)) as ArrayRef;
        let output = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected)
    }

    fn test_sort_to_indices_boolean_arrays(
        data: Vec<Option<bool>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<u32>,
    ) {
        let output = BooleanArray::from(data);
        let expected = UInt32Array::from(expected_data);
        let output =
            sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_to_indices_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<u32>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = UInt32Array::from(expected_data);
        let output =
            sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = Arc::new(PrimitiveArray::<T>::from(expected_data)) as ArrayRef;
        let output = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected)
    }

    fn test_sort_to_indices_string_arrays(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<u32>,
    ) {
        let output = StringArray::from(data);
        let expected = UInt32Array::from(expected_data);
        let output =
            sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
        assert_eq!(output, expected)
    }

    /// Tests both Utf8 and LargeUtf8
    fn test_sort_string_arrays(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<&str>>,
    ) {
        let output = StringArray::from(data.clone());
        let expected = Arc::new(StringArray::from(expected_data.clone())) as ArrayRef;
        let output = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected);

        let output = LargeStringArray::from(data);
        let expected = Arc::new(LargeStringArray::from(expected_data)) as ArrayRef;
        let output = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected)
    }

    fn test_sort_string_dict_arrays<T: ArrowDictionaryKeyType>(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<&str>>,
    ) {
        let array = data.into_iter().collect::<DictionaryArray<T>>();
        let array_values = array.values().clone();
        let dict = array_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Unable to get dictionary values");

        let sorted = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(array) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(array) as ArrayRef), options).unwrap(),
        };
        let sorted = sorted
            .as_any()
            .downcast_ref::<DictionaryArray<T>>()
            .unwrap();
        let sorted_values = sorted.values();
        let sorted_dict = sorted_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Unable to get dictionary values");
        let sorted_keys = sorted.keys();

        assert_eq!(sorted_dict, dict);

        let sorted_strings = StringArray::try_from(
            (0..sorted.len())
                .map(|i| {
                    if sorted.is_valid(i) {
                        Some(sorted_dict.value(sorted_keys.value(i).as_usize()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<Option<&str>>>(),
        )
        .expect("Unable to create string array from dictionary");
        let expected =
            StringArray::try_from(expected_data).expect("Unable to create string array");

        assert_eq!(sorted_strings, expected)
    }

    fn test_sort_primitive_dict_arrays<K: ArrowDictionaryKeyType, T: ArrowPrimitiveType>(
        keys: PrimitiveArray<K>,
        values: PrimitiveArray<T>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let array = DictionaryArray::<K>::try_new(&keys, &values).unwrap();
        let array_values = array.values().clone();
        let dict = array_values
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("Unable to get dictionary values");

        let sorted = match limit {
            Some(_) => {
                sort_limit(&(Arc::new(array) as ArrayRef), options, limit).unwrap()
            }
            _ => sort(&(Arc::new(array) as ArrayRef), options).unwrap(),
        };
        let sorted = sorted
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .unwrap();
        let sorted_values = sorted.values();
        let sorted_dict = sorted_values
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("Unable to get dictionary values");
        let sorted_keys = sorted.keys();

        assert_eq!(sorted_dict, dict);

        let sorted_values: PrimitiveArray<T> = From::<Vec<Option<T::Native>>>::from(
            (0..sorted.len())
                .map(|i| {
                    let key = sorted_keys.value(i).as_usize();
                    if sorted.is_valid(i) && sorted_dict.is_valid(key) {
                        Some(sorted_dict.value(key))
                    } else {
                        None
                    }
                })
                .collect::<Vec<Option<T::Native>>>(),
        );
        let expected: PrimitiveArray<T> =
            From::<Vec<Option<T::Native>>>::from(expected_data);

        assert_eq!(sorted_values, expected)
    }

    fn test_sort_list_arrays<T>(
        data: Vec<Option<Vec<Option<T::Native>>>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<Vec<Option<T::Native>>>>,
        fixed_length: Option<i32>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        // for FixedSizedList
        if let Some(length) = fixed_length {
            let input = Arc::new(FixedSizeListArray::from_iter_primitive::<T, _, _>(
                data.clone(),
                length,
            ));
            let sorted = match limit {
                Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
                _ => sort(&(input as ArrayRef), options).unwrap(),
            };
            let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<T, _, _>(
                expected_data.clone(),
                length,
            )) as ArrayRef;

            assert_eq!(&sorted, &expected);
        }

        // for List
        let input = Arc::new(ListArray::from_iter_primitive::<T, _, _>(data.clone()));
        let sorted = match limit {
            Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
            _ => sort(&(input as ArrayRef), options).unwrap(),
        };
        let expected = Arc::new(ListArray::from_iter_primitive::<T, _, _>(
            expected_data.clone(),
        )) as ArrayRef;

        assert_eq!(&sorted, &expected);

        // for LargeList
        let input = Arc::new(LargeListArray::from_iter_primitive::<T, _, _>(data));
        let sorted = match limit {
            Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
            _ => sort(&(input as ArrayRef), options).unwrap(),
        };
        let expected = Arc::new(LargeListArray::from_iter_primitive::<T, _, _>(
            expected_data,
        )) as ArrayRef;

        assert_eq!(&sorted, &expected);
    }

    fn test_lex_sort_arrays(
        input: Vec<SortColumn>,
        expected_output: Vec<ArrayRef>,
        limit: Option<usize>,
    ) {
        let sorted = lexsort(&input, limit).unwrap();

        for (result, expected) in sorted.iter().zip(expected_output.iter()) {
            assert_eq!(result, expected);
        }
    }

    /// slice all arrays in expected_output to offset/length
    fn slice_arrays(
        expected_output: Vec<ArrayRef>,
        offset: usize,
        length: usize,
    ) -> Vec<ArrayRef> {
        expected_output
            .into_iter()
            .map(|array| array.slice(offset, length))
            .collect()
    }

    fn test_sort_binary_arrays(
        data: Vec<Option<Vec<u8>>>,
        options: Option<SortOptions>,
        limit: Option<usize>,
        expected_data: Vec<Option<Vec<u8>>>,
        fixed_length: Option<i32>,
    ) {
        // Fixed size binary array
        if let Some(length) = fixed_length {
            let input = Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    data.iter().cloned(),
                    length,
                )
                .unwrap(),
            );
            let sorted = match limit {
                Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
                None => sort(&(input as ArrayRef), options).unwrap(),
            };
            let expected = Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    expected_data.iter().cloned(),
                    length,
                )
                .unwrap(),
            ) as ArrayRef;

            assert_eq!(&sorted, &expected);
        }

        // Generic size binary array
        fn make_generic_binary_array<S: OffsetSizeTrait>(
            data: &[Option<Vec<u8>>],
        ) -> Arc<GenericBinaryArray<S>> {
            Arc::new(GenericBinaryArray::<S>::from_opt_vec(
                data.iter()
                    .map(|binary| binary.as_ref().map(Vec::as_slice))
                    .collect(),
            ))
        }

        // BinaryArray
        let input = make_generic_binary_array::<i32>(&data);
        let sorted = match limit {
            Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
            None => sort(&(input as ArrayRef), options).unwrap(),
        };
        let expected = make_generic_binary_array::<i32>(&expected_data) as ArrayRef;
        assert_eq!(&sorted, &expected);

        // LargeBinaryArray
        let input = make_generic_binary_array::<i64>(&data);
        let sorted = match limit {
            Some(_) => sort_limit(&(input as ArrayRef), options, limit).unwrap(),
            None => sort(&(input as ArrayRef), options).unwrap(),
        };
        let expected = make_generic_binary_array::<i64>(&expected_data) as ArrayRef;
        assert_eq!(&sorted, &expected);
    }

    #[test]
    fn test_sort_to_indices_primitives() {
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            None,
            None,
            vec![0, 5, 3, 1, 4, 2],
        );

        // descending
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0], // [2, 4, 1, 3, 5, 0]
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![
                None,
                Some(0.005),
                Some(20.22),
                Some(-10.3),
                Some(0.005),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 5, 0],
        );

        // descending, nulls first
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![None, Some(0.1), Some(0.2), Some(-1.3), Some(0.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(10.1), Some(100.2), Some(-1.3), Some(10.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 1, 4, 3],
        );

        // valid values less than limit with extra nulls
        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![3, 0, 1],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![1, 2, 3],
        );

        // more nulls than limit
        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![Some(1.0), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![1, 2],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![Some(1.0), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![0, 1],
        );
    }

    #[test]
    fn test_sort_to_indices_primitive_more_nulls_than_limit() {
        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, None, Some(3), None, Some(1), None, Some(2)],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![4, 6],
        );
    }

    #[test]
    fn test_sort_boolean() {
        // boolean
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            None,
            None,
            vec![0, 5, 1, 4, 2, 3],
        );

        // boolean, descending
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 3, 1, 4, 5, 0],
        );

        // boolean, descending, nulls first
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![5, 0, 2, 3, 1, 4],
        );

        // boolean, descending, nulls first, limit
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![5, 0, 2],
        );

        // valid values less than limit with extra nulls
        test_sort_to_indices_boolean_arrays(
            vec![Some(true), None, None, Some(false)],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![3, 0, 1],
        );

        test_sort_to_indices_boolean_arrays(
            vec![Some(true), None, None, Some(false)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![1, 2, 3],
        );

        // more nulls than limit
        test_sort_to_indices_boolean_arrays(
            vec![Some(true), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![1, 2],
        );

        test_sort_to_indices_boolean_arrays(
            vec![Some(true), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![0, 1],
        );
    }

    #[test]
    fn test_sort_indices_decimal128() {
        // decimal default
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            None,
            None,
            vec![0, 6, 4, 2, 3, 5, 1],
        );
        // decimal descending
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![1, 5, 3, 2, 4, 6, 0],
        );
        // decimal null_first and descending
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![6, 0, 1, 5, 3, 2, 4],
        );
        // decimal null_first
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 6, 4, 2, 3, 5, 1],
        );
        // limit
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            None,
            Some(3),
            vec![0, 6, 4],
        );
        // limit descending
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            vec![1, 5, 3],
        );
        // limit descending null_first
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![6, 0, 1],
        );
        // limit null_first
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![0, 6, 4],
        );
    }

    #[test]
    fn test_sort_indices_decimal256() {
        // decimal default
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            None,
            None,
            vec![0, 6, 4, 2, 3, 5, 1],
        );
        // decimal descending
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![1, 5, 3, 2, 4, 6, 0],
        );
        // decimal null_first and descending
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![6, 0, 1, 5, 3, 2, 4],
        );
        // decimal null_first
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 6, 4, 2, 3, 5, 1],
        );
        // limit
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            None,
            Some(3),
            vec![0, 6, 4],
        );
        // limit descending
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            vec![1, 5, 3],
        );
        // limit descending null_first
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![6, 0, 1],
        );
        // limit null_first
        test_sort_to_indices_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![0, 6, 4],
        );
    }

    #[test]
    fn test_sort_indices_decimal256_max_min() {
        test_sort_to_indices_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 1, 4, 2, 3],
        );

        test_sort_to_indices_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 3, 2, 4, 1],
        );

        test_sort_to_indices_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(4),
            vec![0, 1, 4, 2],
        );

        test_sort_to_indices_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(4),
            vec![0, 3, 2, 4],
        );
    }

    #[test]
    fn test_sort_decimal128() {
        // decimal default
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            None,
            None,
            vec![None, None, Some(1), Some(2), Some(3), Some(4), Some(5)],
        );
        // decimal descending
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5), Some(4), Some(3), Some(2), Some(1), None, None],
        );
        // decimal null_first and descending
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(5), Some(4), Some(3), Some(2), Some(1)],
        );
        // decimal null_first
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(1), Some(2), Some(3), Some(4), Some(5)],
        );
        // limit
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            None,
            Some(3),
            vec![None, None, Some(1)],
        );
        // limit descending
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            vec![Some(5), Some(4), Some(3)],
        );
        // limit descending null_first
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(5)],
        );
        // limit null_first
        test_sort_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(1)],
        );
    }

    #[test]
    fn test_sort_decimal256() {
        // decimal default
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            None,
            None,
            vec![None, None, Some(1), Some(2), Some(3), Some(4), Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal descending
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5), Some(4), Some(3), Some(2), Some(1), None, None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal null_first and descending
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(5), Some(4), Some(3), Some(2), Some(1)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal null_first
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(1), Some(2), Some(3), Some(4), Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            None,
            Some(3),
            vec![None, None, Some(1)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit descending
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            vec![Some(5), Some(4), Some(3)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit descending null_first
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit null_first
        test_sort_decimal256_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(1)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
    }

    #[test]
    fn test_sort_decimal256_max_min() {
        test_sort_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some(i256::MIN),
                Some(i256::from_i128(-1)),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
            ],
        );

        test_sort_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some(i256::MAX),
                Some(i256::from_i128(1)),
                Some(i256::from_i128(-1)),
                Some(i256::MIN),
            ],
        );

        test_sort_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(4),
            vec![None, None, Some(i256::MIN), Some(i256::from_i128(-1))],
        );

        test_sort_decimal256_array(
            vec![
                None,
                Some(i256::MIN),
                Some(i256::from_i128(1)),
                Some(i256::MAX),
                Some(i256::from_i128(-1)),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(4),
            vec![None, None, Some(i256::MAX), Some(i256::from_i128(1))],
        );
    }

    #[test]
    fn test_sort_primitives() {
        // default case
        test_sort_primitive_arrays::<UInt8Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt16Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt32Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt64Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );

        // descending
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );

        // descending, nulls first
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );

        test_sort_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(2)],
        );

        test_sort_primitive_arrays::<Float32Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(2.0), Some(0.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(f64::NAN), Some(2.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
        );

        // int8 nulls first
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Float32Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1.0), Some(0.0), Some(0.0), Some(2.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![None, None, Some(-1.0), Some(0.0), Some(2.0), Some(f64::NAN)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![Some(1.0), Some(f64::NAN), Some(f64::NAN), Some(f64::NAN)],
        );

        // limit
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![Some(1.0), Some(f64::NAN)],
        );

        // limit with actual value
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(2.0), Some(4.0), Some(3.0), Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![Some(1.0), Some(2.0), Some(3.0)],
        );

        // valid values less than limit with extra nulls
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![Some(1.0), Some(2.0), None],
        );

        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(1.0)],
        );

        // more nulls than limit
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![None, None],
        );

        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(2.0), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some(2.0), None],
        );
    }

    #[test]
    fn test_sort_to_indices_strings() {
        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            None,
            vec![0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 4, 1, 5, 3, 0],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![3, 0, 2, 4, 1, 5],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![3, 0, 2],
        );

        // valid values less than limit with extra nulls
        test_sort_to_indices_string_arrays(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![3, 0, 1],
        );

        test_sort_to_indices_string_arrays(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![1, 2, 3],
        );

        // more nulls than limit
        test_sort_to_indices_string_arrays(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![1, 2],
        );

        test_sort_to_indices_string_arrays(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![0, 1],
        );
    }

    #[test]
    fn test_sort_strings() {
        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some("sad")],
        );

        // valid values less than limit with extra nulls
        test_sort_string_arrays(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![Some("abc"), Some("def"), None],
        );

        test_sort_string_arrays(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some("abc")],
        );

        // more nulls than limit
        test_sort_string_arrays(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![None, None],
        );

        test_sort_string_arrays(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some("def"), None],
        );
    }

    #[test]
    fn test_sort_string_dicts() {
        test_sort_string_dict_arrays::<Int8Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_dict_arrays::<Int32Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some("sad")],
        );

        // valid values less than limit with extra nulls
        test_sort_string_dict_arrays::<Int16Type>(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![Some("abc"), Some("def"), None],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![Some("def"), None, None, Some("abc")],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some("abc")],
        );

        // more nulls than limit
        test_sort_string_dict_arrays::<Int16Type>(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![None, None],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![Some("def"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some("def"), None],
        );
    }

    #[test]
    fn test_sort_list() {
        test_sort_list_arrays::<Int8Type>(
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(4)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
                Some(vec![Some(4)]),
            ],
            Some(1),
        );

        test_sort_list_arrays::<Float32Type>(
            vec![
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(4.0), Some(3.0), Some(2.0), Some(1.0)]),
                Some(vec![Some(2.0), Some(3.0), Some(4.0)]),
                Some(vec![Some(3.0), Some(3.0), Some(3.0), Some(3.0)]),
                Some(vec![Some(1.0), Some(1.0)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(1.0), Some(1.0)]),
                Some(vec![Some(2.0), Some(3.0), Some(4.0)]),
                Some(vec![Some(3.0), Some(3.0), Some(3.0), Some(3.0)]),
                Some(vec![Some(4.0), Some(3.0), Some(2.0), Some(1.0)]),
            ],
            None,
        );

        test_sort_list_arrays::<Float64Type>(
            vec![
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(4.0), Some(3.0), Some(2.0), Some(1.0)]),
                Some(vec![Some(2.0), Some(3.0), Some(4.0)]),
                Some(vec![Some(3.0), Some(3.0), Some(3.0), Some(3.0)]),
                Some(vec![Some(1.0), Some(1.0)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(1.0), Some(1.0)]),
                Some(vec![Some(2.0), Some(3.0), Some(4.0)]),
                Some(vec![Some(3.0), Some(3.0), Some(3.0), Some(3.0)]),
                Some(vec![Some(4.0), Some(3.0), Some(2.0), Some(1.0)]),
            ],
            None,
        );

        test_sort_list_arrays::<Int32Type>(
            vec![
                Some(vec![Some(1), Some(0)]),
                Some(vec![Some(4), Some(3), Some(2), Some(1)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), Some(3), Some(3)]),
                Some(vec![Some(1), Some(1)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(1), Some(0)]),
                Some(vec![Some(1), Some(1)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), Some(3), Some(3)]),
                Some(vec![Some(4), Some(3), Some(2), Some(1)]),
            ],
            None,
        );

        test_sort_list_arrays::<Int32Type>(
            vec![
                None,
                Some(vec![Some(4), None, Some(2)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                None,
                Some(vec![Some(3), Some(3), None]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), None]),
                Some(vec![Some(4), None, Some(2)]),
                None,
                None,
            ],
            Some(3),
        );

        test_sort_list_arrays::<Int32Type>(
            vec![
                Some(vec![Some(1), Some(0)]),
                Some(vec![Some(4), Some(3), Some(2), Some(1)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), Some(3), Some(3)]),
                Some(vec![Some(1), Some(1)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some(vec![Some(1), Some(0)]), Some(vec![Some(1), Some(1)])],
            None,
        );

        // valid values less than limit with extra nulls
        test_sort_list_arrays::<Int32Type>(
            vec![Some(vec![Some(1)]), None, None, Some(vec![Some(2)])],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![Some(vec![Some(1)]), Some(vec![Some(2)]), None],
            None,
        );

        test_sort_list_arrays::<Int32Type>(
            vec![Some(vec![Some(1)]), None, None, Some(vec![Some(2)])],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(vec![Some(1)])],
            None,
        );

        // more nulls than limit
        test_sort_list_arrays::<Int32Type>(
            vec![Some(vec![Some(1)]), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![None, None],
            None,
        );

        test_sort_list_arrays::<Int32Type>(
            vec![Some(vec![Some(1)]), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some(vec![Some(1)]), None],
            None,
        );
    }

    #[test]
    fn test_sort_binary() {
        test_sort_binary_arrays(
            vec![
                Some(vec![0, 0, 0]),
                Some(vec![0, 0, 5]),
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 1]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![0, 0, 0]),
                Some(vec![0, 0, 1]),
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 5]),
                Some(vec![0, 0, 7]),
            ],
            Some(3),
        );

        // with nulls
        test_sort_binary_arrays(
            vec![
                Some(vec![0, 0, 0]),
                None,
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 1]),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![0, 0, 0]),
                Some(vec![0, 0, 1]),
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                None,
                None,
            ],
            Some(3),
        );

        test_sort_binary_arrays(
            vec![
                Some(vec![3, 5, 7]),
                None,
                Some(vec![1, 7, 1]),
                Some(vec![2, 7, 3]),
                None,
                Some(vec![1, 4, 3]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![1, 4, 3]),
                Some(vec![1, 7, 1]),
                Some(vec![2, 7, 3]),
                Some(vec![3, 5, 7]),
                None,
                None,
            ],
            Some(3),
        );

        // descending
        test_sort_binary_arrays(
            vec![
                Some(vec![0, 0, 0]),
                None,
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 1]),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 1]),
                Some(vec![0, 0, 0]),
                None,
                None,
            ],
            Some(3),
        );

        // nulls first
        test_sort_binary_arrays(
            vec![
                Some(vec![0, 0, 0]),
                None,
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 1]),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![
                None,
                None,
                Some(vec![0, 0, 0]),
                Some(vec![0, 0, 1]),
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
            ],
            Some(3),
        );

        // limit
        test_sort_binary_arrays(
            vec![
                Some(vec![0, 0, 0]),
                None,
                Some(vec![0, 0, 3]),
                Some(vec![0, 0, 7]),
                Some(vec![0, 0, 1]),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(4),
            vec![None, None, Some(vec![0, 0, 0]), Some(vec![0, 0, 1])],
            Some(3),
        );

        // var length
        test_sort_binary_arrays(
            vec![
                Some(b"Hello".to_vec()),
                None,
                Some(b"from".to_vec()),
                Some(b"Apache".to_vec()),
                Some(b"Arrow-rs".to_vec()),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(b"Apache".to_vec()),
                Some(b"Arrow-rs".to_vec()),
                Some(b"Hello".to_vec()),
                Some(b"from".to_vec()),
                None,
                None,
            ],
            None,
        );

        // limit
        test_sort_binary_arrays(
            vec![
                Some(b"Hello".to_vec()),
                None,
                Some(b"from".to_vec()),
                Some(b"Apache".to_vec()),
                Some(b"Arrow-rs".to_vec()),
                None,
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(4),
            vec![
                None,
                None,
                Some(b"Apache".to_vec()),
                Some(b"Arrow-rs".to_vec()),
            ],
            None,
        );
    }

    #[test]
    fn test_lex_sort_single_column() {
        let input = vec![SortColumn {
            values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(17),
                Some(2),
                Some(-1),
                Some(0),
            ])) as ArrayRef,
            options: None,
        }];
        let expected = vec![Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(0),
            Some(2),
            Some(17),
        ])) as ArrayRef];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input.clone(), slice_arrays(expected, 0, 2), Some(2));

        // Explicitly test a limit on the sort as a demonstration
        let expected = vec![Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(0),
            Some(2),
        ])) as ArrayRef];
        test_lex_sort_arrays(input, expected, Some(3));
    }

    #[test]
    fn test_lex_sort_unaligned_rows() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![None, Some(-1)]))
                    as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo")])) as ArrayRef,
                options: None,
            },
        ];
        assert!(
            lexsort(&input, None).is_err(),
            "lexsort should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lex_sort_mixed_types() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                    Some(101),
                    Some(8),
                    Some(7),
                    Some(102),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(-1),
                    Some(-2),
                    Some(-3),
                    Some(-4),
                ])) as ArrayRef,
                options: None,
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(0),
                Some(0),
                Some(2),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                Some(7),
                Some(101),
                Some(102),
                Some(8),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-3),
                Some(-1),
                Some(-4),
                Some(-2),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 2), Some(2));

        // test mix of string and in64 with option
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("9"),
                    Some("7"),
                    Some("bar"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(0),
                Some(0),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("9"),
                Some("foo"),
                Some("bar"),
                Some("7"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 3), Some(3));

        // test sort with nulls first
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                None,
                None,
                Some(2),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some("foo"),
                Some("hello"),
                Some("world"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 1), Some(1));

        // test sort with nulls last
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(-1),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("world"),
                Some("foo"),
                None,
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 2), Some(2));

        // test sort with opposite options
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    Some(-1),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("bar"),
                Some("world"),
                None,
                Some("foo"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(
            input.clone(),
            slice_arrays(expected.clone(), 0, 5),
            Some(5),
        );

        // Limiting by more rows than present is ok
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 5), Some(10));
    }

    #[test]
    fn test_partial_sort() {
        let mut before: Vec<&str> = vec![
            "a", "cat", "mat", "on", "sat", "the", "xxx", "xxxx", "fdadfdsf",
        ];
        let mut d = before.clone();
        d.sort_unstable();

        for last in 0..before.len() {
            partial_sort(&mut before, last, |a, b| a.cmp(b));
            assert_eq!(&d[0..last], &before.as_slice()[0..last]);
        }
    }

    #[test]
    fn test_partial_rand_sort() {
        let size = 1000u32;
        let mut rng = StdRng::seed_from_u64(42);
        let mut before: Vec<u32> = (0..size).map(|_| rng.gen::<u32>()).collect();
        let mut d = before.clone();
        let last = (rng.next_u32() % size) as usize;
        d.sort_unstable();

        partial_sort(&mut before, last, |a, b| a.cmp(b));
        assert_eq!(&d[0..last], &before[0..last]);
    }

    #[test]
    fn test_sort_int8_dicts() {
        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Int8Array::from(vec![1, 3, 5]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, Some(1), Some(3), Some(5), Some(5)],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Int8Array::from(vec![1, 3, 5]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5), Some(5), Some(3), Some(1), None, None],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Int8Array::from(vec![1, 3, 5]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![Some(1), Some(3), Some(5), Some(5), None, None],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Int8Array::from(vec![1, 3, 5]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(5)],
        );

        // Values have `None`.
        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Int8Array::from(vec![Some(1), Some(3), None, Some(5)]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, None, Some(1), Some(3), Some(5), Some(5)],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Int8Array::from(vec![Some(1), Some(3), None, Some(5)]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![Some(1), Some(3), Some(5), Some(5), None, None, None],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Int8Array::from(vec![Some(1), Some(3), None, Some(5)]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5), Some(5), Some(3), Some(1), None, None, None],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Int8Array::from(vec![Some(1), Some(3), None, Some(5)]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, None, Some(5), Some(5), Some(3), Some(1)],
        );
    }

    #[test]
    fn test_sort_f32_dicts() {
        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Float32Array::from(vec![1.2, 3.0, 5.1]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, Some(1.2), Some(3.0), Some(5.1), Some(5.1)],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Float32Array::from(vec![1.2, 3.0, 5.1]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5.1), Some(5.1), Some(3.0), Some(1.2), None, None],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Float32Array::from(vec![1.2, 3.0, 5.1]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![Some(1.2), Some(3.0), Some(5.1), Some(5.1), None, None],
        );

        let keys =
            Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Float32Array::from(vec![1.2, 3.0, 5.1]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some(5.1)],
        );

        // Values have `None`.
        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Float32Array::from(vec![Some(1.2), Some(3.0), None, Some(5.1)]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, None, Some(1.2), Some(3.0), Some(5.1), Some(5.1)],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Float32Array::from(vec![Some(1.2), Some(3.0), None, Some(5.1)]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![Some(1.2), Some(3.0), Some(5.1), Some(5.1), None, None, None],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Float32Array::from(vec![Some(1.2), Some(3.0), None, Some(5.1)]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![Some(5.1), Some(5.1), Some(3.0), Some(1.2), None, None, None],
        );

        let keys = Int8Array::from(vec![
            Some(1_i8),
            None,
            Some(3),
            None,
            Some(2),
            Some(3),
            Some(0),
        ]);
        let values = Float32Array::from(vec![Some(1.2), Some(3.0), None, Some(5.1)]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![None, None, None, Some(5.1), Some(5.1), Some(3.0), Some(1.2)],
        );
    }
}
