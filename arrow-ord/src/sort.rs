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

use crate::ord::{make_comparator, DynComparator};
use arrow_array::builder::BufferBuilder;
use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::BooleanBufferBuilder;
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::rank::rank;
pub use arrow_schema::SortOptions;

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
/// # use arrow_array::Int32Array;
/// # use arrow_ord::sort::sort;
/// let array = Int32Array::from(vec![5, 4, 3, 2, 1]);
/// let sorted_array = sort(&array, None).unwrap();
/// assert_eq!(sorted_array.as_ref(), &Int32Array::from(vec![1, 2, 3, 4, 5]));
/// ```
pub fn sort(values: &dyn Array, options: Option<SortOptions>) -> Result<ArrayRef, ArrowError> {
    downcast_primitive_array!(
        values => sort_native_type(values, options),
        DataType::RunEndEncoded(_, _) => sort_run(values, options, None),
        _ => {
            let indices = sort_to_indices(values, options, None)?;
            take(values, &indices, None)
        }
    )
}

fn sort_native_type<T>(
    primitive_values: &PrimitiveArray<T>,
    options: Option<SortOptions>,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
{
    let sort_options = options.unwrap_or_default();

    let mut mutable_buffer = vec![T::default_value(); primitive_values.len()];
    let mutable_slice = &mut mutable_buffer;

    let input_values = primitive_values.values().as_ref();

    let nulls_count = primitive_values.null_count();
    let valid_count = primitive_values.len() - nulls_count;

    let null_bit_buffer = match nulls_count > 0 {
        true => {
            let mut validity_buffer = BooleanBufferBuilder::new(primitive_values.len());
            if sort_options.nulls_first {
                validity_buffer.append_n(nulls_count, false);
                validity_buffer.append_n(valid_count, true);
            } else {
                validity_buffer.append_n(valid_count, true);
                validity_buffer.append_n(nulls_count, false);
            }
            Some(validity_buffer.finish().into())
        }
        false => None,
    };

    if let Some(nulls) = primitive_values.nulls().filter(|n| n.null_count() > 0) {
        let values_slice = match sort_options.nulls_first {
            true => &mut mutable_slice[nulls_count..],
            false => &mut mutable_slice[..valid_count],
        };

        for (write_index, index) in nulls.valid_indices().enumerate() {
            values_slice[write_index] = primitive_values.value(index);
        }

        values_slice.sort_unstable_by(|a, b| a.compare(*b));
        if sort_options.descending {
            values_slice.reverse();
        }
    } else {
        mutable_slice.copy_from_slice(input_values);
        mutable_slice.sort_unstable_by(|a, b| a.compare(*b));
        if sort_options.descending {
            mutable_slice.reverse();
        }
    }

    Ok(Arc::new(
        PrimitiveArray::<T>::new(mutable_buffer.into(), null_bit_buffer)
            .with_data_type(primitive_values.data_type().clone()),
    ))
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
/// # use arrow_array::Int32Array;
/// # use arrow_ord::sort::{sort_limit, SortOptions};
/// let array = Int32Array::from(vec![5, 4, 3, 2, 1]);
///
/// // Find the the top 2 items
/// let sorted_array = sort_limit(&array, None, Some(2)).unwrap();
/// assert_eq!(sorted_array.as_ref(), &Int32Array::from(vec![1, 2]));
///
/// // Find the bottom top 2 items
/// let options = Some(SortOptions {
///                  descending: true,
///                  ..Default::default()
///               });
/// let sorted_array = sort_limit(&array, options, Some(2)).unwrap();
/// assert_eq!(sorted_array.as_ref(), &Int32Array::from(vec![5, 4]));
/// ```
pub fn sort_limit(
    values: &dyn Array,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<ArrayRef, ArrowError> {
    if let DataType::RunEndEncoded(_, _) = values.data_type() {
        return sort_run(values, options, limit);
    }
    let indices = sort_to_indices(values, options, limit)?;
    take(values, &indices, None)
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

// partition indices into valid and null indices
fn partition_validity(array: &dyn Array) -> (Vec<u32>, Vec<u32>) {
    match array.null_count() {
        // faster path
        0 => ((0..(array.len() as u32)).collect(), vec![]),
        _ => {
            let indices = 0..(array.len() as u32);
            indices.partition(|index| array.is_valid(*index as usize))
        }
    }
}

/// Whether `arrow_ord::rank` can rank an array of given data type.
fn can_rank(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        )
}

/// Whether `sort_to_indices` can sort an array of given data type.
fn can_sort_to_indices(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
        || match data_type {
            DataType::List(f) if can_rank(f.data_type()) => true,
            DataType::LargeList(f) if can_rank(f.data_type()) => true,
            DataType::FixedSizeList(f, _) if can_rank(f.data_type()) => true,
            DataType::Dictionary(_, values) if can_rank(values.as_ref()) => true,
            DataType::RunEndEncoded(_, f) if can_sort_to_indices(f.data_type()) => true,
            _ => false,
        }
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices.
/// Floats are sorted using IEEE 754 totalOrder.  `limit` is an option for [partial_sort].
pub fn sort_to_indices(
    array: &dyn Array,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    let options = options.unwrap_or_default();

    let (v, n) = partition_validity(array);

    Ok(downcast_primitive_array! {
        array => sort_primitive(array, v, n, options, limit),
        DataType::Boolean => sort_boolean(array.as_boolean(), v, n, options, limit),
        DataType::Utf8 => sort_bytes(array.as_string::<i32>(), v, n, options, limit),
        DataType::LargeUtf8 => sort_bytes(array.as_string::<i64>(), v, n, options, limit),
        DataType::Utf8View => sort_byte_view(array.as_string_view(), v, n, options, limit),
        DataType::Binary => sort_bytes(array.as_binary::<i32>(), v, n, options, limit),
        DataType::LargeBinary => sort_bytes(array.as_binary::<i64>(), v, n, options, limit),
        DataType::BinaryView => sort_byte_view(array.as_binary_view(), v, n, options, limit),
        DataType::FixedSizeBinary(_) => sort_fixed_size_binary(array.as_fixed_size_binary(), v, n, options, limit),
        DataType::List(_) => sort_list(array.as_list::<i32>(), v, n, options, limit)?,
        DataType::LargeList(_) => sort_list(array.as_list::<i64>(), v, n, options, limit)?,
        DataType::FixedSizeList(_, _) => sort_fixed_size_list(array.as_fixed_size_list(), v, n, options, limit)?,
        DataType::Dictionary(_, _) => downcast_dictionary_array!{
            array => sort_dictionary(array, v, n, options, limit)?,
            _ => unreachable!()
        }
        DataType::RunEndEncoded(run_ends_field, _) => match run_ends_field.data_type() {
            DataType::Int16 => sort_run_to_indices::<Int16Type>(array, options, limit),
            DataType::Int32 => sort_run_to_indices::<Int32Type>(array, options, limit),
            DataType::Int64 => sort_run_to_indices::<Int64Type>(array, options, limit),
            dt => {
                return Err(ArrowError::ComputeError(format!(
                    "Invalid run end data type: {dt}"
                )))
            }
        },
        t => {
            return Err(ArrowError::ComputeError(format!(
                "Sort not supported for data type {t:?}"
            )));
        }
    })
}

fn sort_boolean(
    values: &BooleanArray,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(u32, bool)>>();
    sort_impl(options, &mut valids, &null_indices, limit, |a, b| a.cmp(&b)).into()
}

fn sort_primitive<T: ArrowPrimitiveType>(
    values: &PrimitiveArray<T>,
    value_indices: Vec<u32>,
    nulls: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(u32, T::Native)>>();
    sort_impl(options, &mut valids, &nulls, limit, T::Native::compare).into()
}

fn sort_bytes<T: ByteArrayType>(
    values: &GenericByteArray<T>,
    value_indices: Vec<u32>,
    nulls: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize).as_ref()))
        .collect::<Vec<(u32, &[u8])>>();

    sort_impl(options, &mut valids, &nulls, limit, Ord::cmp).into()
}

fn sort_byte_view<T: ByteViewType>(
    values: &GenericByteViewArray<T>,
    value_indices: Vec<u32>,
    nulls: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize).as_ref()))
        .collect::<Vec<(u32, &[u8])>>();
    sort_impl(options, &mut valids, &nulls, limit, Ord::cmp).into()
}

fn sort_fixed_size_binary(
    values: &FixedSizeBinaryArray,
    value_indices: Vec<u32>,
    nulls: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let mut valids = value_indices
        .iter()
        .copied()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(u32, &[u8])>>();
    sort_impl(options, &mut valids, &nulls, limit, Ord::cmp).into()
}

fn sort_dictionary<K: ArrowDictionaryKeyType>(
    dict: &DictionaryArray<K>,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    let keys: &PrimitiveArray<K> = dict.keys();
    let rank = child_rank(dict.values().as_ref(), options)?;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| {
            let key: K::Native = keys.value(index as usize);
            (index, rank[key.as_usize()])
        })
        .collect::<Vec<(u32, u32)>>();

    Ok(sort_impl(options, &mut valids, &null_indices, limit, |a, b| a.cmp(&b)).into())
}

fn sort_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    let rank = child_rank(array.values().as_ref(), options)?;
    let offsets = array.value_offsets();
    let mut valids = value_indices
        .into_iter()
        .map(|index| {
            let end = offsets[index as usize + 1].as_usize();
            let start = offsets[index as usize].as_usize();
            (index, &rank[start..end])
        })
        .collect::<Vec<(u32, &[u32])>>();
    Ok(sort_impl(options, &mut valids, &null_indices, limit, Ord::cmp).into())
}

fn sort_fixed_size_list(
    array: &FixedSizeListArray,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: SortOptions,
    limit: Option<usize>,
) -> Result<UInt32Array, ArrowError> {
    let rank = child_rank(array.values().as_ref(), options)?;
    let size = array.value_length() as usize;
    let mut valids = value_indices
        .into_iter()
        .map(|index| {
            let start = index as usize * size;
            (index, &rank[start..start + size])
        })
        .collect::<Vec<(u32, &[u32])>>();
    Ok(sort_impl(options, &mut valids, &null_indices, limit, Ord::cmp).into())
}

#[inline(never)]
fn sort_impl<T: Copy>(
    options: SortOptions,
    valids: &mut [(u32, T)],
    nulls: &[u32],
    limit: Option<usize>,
    mut cmp: impl FnMut(T, T) -> Ordering,
) -> Vec<u32> {
    let v_limit = match (limit, options.nulls_first) {
        (Some(l), true) => l.saturating_sub(nulls.len()).min(valids.len()),
        _ => valids.len(),
    };

    match options.descending {
        false => sort_unstable_by(valids, v_limit, |a, b| cmp(a.1, b.1)),
        true => sort_unstable_by(valids, v_limit, |a, b| cmp(a.1, b.1).reverse()),
    }

    let len = valids.len() + nulls.len();
    let limit = limit.unwrap_or(len).min(len);
    let mut out = Vec::with_capacity(len);
    match options.nulls_first {
        true => {
            out.extend_from_slice(&nulls[..nulls.len().min(limit)]);
            let remaining = limit - out.len();
            out.extend(valids.iter().map(|x| x.0).take(remaining));
        }
        false => {
            out.extend(valids.iter().map(|x| x.0).take(limit));
            let remaining = limit - out.len();
            out.extend_from_slice(&nulls[..remaining])
        }
    }
    out
}

/// Computes the rank for a set of child values
fn child_rank(values: &dyn Array, options: SortOptions) -> Result<Vec<u32>, ArrowError> {
    // If parent sort order is descending we need to invert the value of nulls_first so that
    // when the parent is sorted based on the produced ranks, nulls are still ordered correctly
    let value_options = Some(SortOptions {
        descending: false,
        nulls_first: options.nulls_first != options.descending,
    });
    rank(values, value_options)
}

// Sort run array and return sorted run array.
// The output RunArray will be encoded at the same level as input run array.
// For e.g. an input RunArray { run_ends = [2,4,6,8], values = [1,2,1,2] }
// will result in output RunArray { run_ends = [2,4,6,8], values = [1,1,2,2] }
// and not RunArray { run_ends = [4,8], values = [1,2] }
fn sort_run(
    values: &dyn Array,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<ArrayRef, ArrowError> {
    match values.data_type() {
        DataType::RunEndEncoded(run_ends_field, _) => match run_ends_field.data_type() {
            DataType::Int16 => sort_run_downcasted::<Int16Type>(values, options, limit),
            DataType::Int32 => sort_run_downcasted::<Int32Type>(values, options, limit),
            DataType::Int64 => sort_run_downcasted::<Int64Type>(values, options, limit),
            dt => unreachable!("Not valid run ends data type {dt}"),
        },
        dt => Err(ArrowError::InvalidArgumentError(format!(
            "Input is not a run encoded array. Input data type {dt}"
        ))),
    }
}

fn sort_run_downcasted<R: RunEndIndexType>(
    values: &dyn Array,
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<ArrayRef, ArrowError> {
    let run_array = values.as_any().downcast_ref::<RunArray<R>>().unwrap();

    // Determine the length of output run array.
    let output_len = if let Some(limit) = limit {
        limit.min(run_array.len())
    } else {
        run_array.len()
    };

    let run_ends = run_array.run_ends();

    let mut new_run_ends_builder = BufferBuilder::<R::Native>::new(run_ends.len());
    let mut new_run_end: usize = 0;
    let mut new_physical_len: usize = 0;

    let consume_runs = |run_length, _| {
        new_run_end += run_length;
        new_physical_len += 1;
        new_run_ends_builder.append(R::Native::from_usize(new_run_end).unwrap());
    };

    let (values_indices, run_values) = sort_run_inner(run_array, options, output_len, consume_runs);

    let new_run_ends = unsafe {
        // Safety:
        // The function builds a valid run_ends array and hence need not be validated.
        ArrayDataBuilder::new(R::DATA_TYPE)
            .len(new_physical_len)
            .add_buffer(new_run_ends_builder.finish())
            .build_unchecked()
    };

    // slice the sorted value indices based on limit.
    let new_values_indices: PrimitiveArray<UInt32Type> = values_indices
        .slice(0, new_run_ends.len())
        .into_data()
        .into();

    let new_values = take(&run_values, &new_values_indices, None)?;

    // Build sorted run array
    let builder = ArrayDataBuilder::new(run_array.data_type().clone())
        .len(new_run_end)
        .add_child_data(new_run_ends)
        .add_child_data(new_values.into_data());
    let array_data: RunArray<R> = unsafe {
        // Safety:
        //  This function builds a valid run array and hence can skip validation.
        builder.build_unchecked().into()
    };
    Ok(Arc::new(array_data))
}

// Sort to indices for run encoded array.
// This function will be slow for run array as it decodes the physical indices to
// logical indices and to get the run array back, the logical indices has to be
// encoded back to run array.
fn sort_run_to_indices<R: RunEndIndexType>(
    values: &dyn Array,
    options: SortOptions,
    limit: Option<usize>,
) -> UInt32Array {
    let run_array = values.as_any().downcast_ref::<RunArray<R>>().unwrap();
    let output_len = if let Some(limit) = limit {
        limit.min(run_array.len())
    } else {
        run_array.len()
    };
    let mut result: Vec<u32> = Vec::with_capacity(output_len);

    //Add all logical indices belonging to a physical index to the output
    let consume_runs = |run_length, logical_start| {
        result.extend(logical_start as u32..(logical_start + run_length) as u32);
    };
    sort_run_inner(run_array, Some(options), output_len, consume_runs);

    UInt32Array::from(result)
}

fn sort_run_inner<R: RunEndIndexType, F>(
    run_array: &RunArray<R>,
    options: Option<SortOptions>,
    output_len: usize,
    mut consume_runs: F,
) -> (PrimitiveArray<UInt32Type>, ArrayRef)
where
    F: FnMut(usize, usize),
{
    // slice the run_array.values based on offset and length.
    let start_physical_index = run_array.get_start_physical_index();
    let end_physical_index = run_array.get_end_physical_index();
    let physical_len = end_physical_index - start_physical_index + 1;
    let run_values = run_array.values().slice(start_physical_index, physical_len);

    // All the values have to be sorted irrespective of input limit.
    let values_indices = sort_to_indices(&run_values, options, None).unwrap();

    let mut remaining_len = output_len;

    let run_ends = run_array.run_ends().values();

    assert_eq!(
        0,
        values_indices.null_count(),
        "The output of sort_to_indices should not have null values. Its values is {}",
        values_indices.null_count()
    );

    // Calculate `run length` of sorted value indices.
    // Find the `logical index` at which the run starts.
    // Call the consumer using the run length and starting logical index.
    for physical_index in values_indices.values() {
        // As the values were sliced with offset = start_physical_index, it has to be added back
        // before accessing `RunArray::run_ends`
        let physical_index = *physical_index as usize + start_physical_index;

        // calculate the run length and logical index of sorted values
        let (run_length, logical_index_start) = unsafe {
            // Safety:
            // The index will be within bounds as its in bounds of start_physical_index
            // and len, both of which are within bounds of run_array
            if physical_index == start_physical_index {
                (
                    run_ends.get_unchecked(physical_index).as_usize() - run_array.offset(),
                    0,
                )
            } else if physical_index == end_physical_index {
                let prev_run_end = run_ends.get_unchecked(physical_index - 1).as_usize();
                (
                    run_array.offset() + run_array.len() - prev_run_end,
                    prev_run_end - run_array.offset(),
                )
            } else {
                let prev_run_end = run_ends.get_unchecked(physical_index - 1).as_usize();
                (
                    run_ends.get_unchecked(physical_index).as_usize() - prev_run_end,
                    prev_run_end - run_array.offset(),
                )
            }
        };
        let new_run_length = run_length.min(remaining_len);
        consume_runs(new_run_length, logical_index_start);
        remaining_len -= new_run_length;

        if remaining_len == 0 {
            break;
        }
    }

    if remaining_len > 0 {
        panic!("Remaining length should be zero its values is {remaining_len}")
    }
    (values_indices, run_values)
}

/// One column to be used in lexicographical sort
#[derive(Clone, Debug)]
pub struct SortColumn {
    /// The column to sort
    pub values: ArrayRef,
    /// Sort options for this column
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
/// # use arrow_array::cast::AsArray;
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
/// assert_eq!(sorted_columns[0].as_primitive::<Int64Type>().value(1), -64);
/// assert!(sorted_columns[0].is_null(0));
/// ```
///
/// Note: for multi-column sorts without a limit, using the [row format](https://docs.rs/arrow-row/latest/arrow_row/)
/// may be significantly faster
///
pub fn lexsort(columns: &[SortColumn], limit: Option<usize>) -> Result<Vec<ArrayRef>, ArrowError> {
    let indices = lexsort_to_indices(columns, limit)?;
    columns
        .iter()
        .map(|c| take(c.values.as_ref(), &indices, None))
        .collect()
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
///
/// Note: for multi-column sorts without a limit, using the [row format](https://docs.rs/arrow-row/latest/arrow_row/)
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
    if columns.len() == 1 && can_sort_to_indices(columns[0].values.data_type()) {
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
    if let Some(n) = limit.checked_sub(1) {
        let (before, _mid, _after) = v.select_nth_unstable_by(n, &mut is_less);
        before.sort_unstable_by(is_less);
    }
}

/// A lexicographical comparator that wraps given array data (columns) and can lexicographically compare data
/// at given two indices. The lifetime is the same at the data wrapped.
pub struct LexicographicalComparator {
    compare_items: Vec<DynComparator>,
}

impl LexicographicalComparator {
    /// lexicographically compare values at the wrapped columns with given indices.
    pub fn compare(&self, a_idx: usize, b_idx: usize) -> Ordering {
        for comparator in &self.compare_items {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        Ordering::Equal
    }

    /// Create a new lex comparator that will wrap the given sort columns and give comparison
    /// results with two indices.
    pub fn try_new(columns: &[SortColumn]) -> Result<LexicographicalComparator, ArrowError> {
        let compare_items = columns
            .iter()
            .map(|c| {
                make_comparator(
                    c.values.as_ref(),
                    c.values.as_ref(),
                    c.options.unwrap_or_default(),
                )
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(LexicographicalComparator { compare_items })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{
        FixedSizeListBuilder, Int64Builder, ListBuilder, PrimitiveRunBuilder,
    };
    use arrow_buffer::{i256, NullBuffer};
    use arrow_schema::Field;
    use half::f16;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};

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
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
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
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
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
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
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
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
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
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
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
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
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
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
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
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options, limit).unwrap();
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
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected);

        let output = LargeStringArray::from(data.clone());
        let expected = Arc::new(LargeStringArray::from(expected_data.clone())) as ArrayRef;
        let output = match limit {
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected);

        let output = StringViewArray::from(data);
        let expected = Arc::new(StringViewArray::from(expected_data)) as ArrayRef;
        let output = match limit {
            Some(_) => sort_limit(&(Arc::new(output) as ArrayRef), options, limit).unwrap(),
            _ => sort(&(Arc::new(output) as ArrayRef), options).unwrap(),
        };
        assert_eq!(&output, &expected);
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
            Some(_) => sort_limit(&(Arc::new(array) as ArrayRef), options, limit).unwrap(),
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

        let sorted_strings = StringArray::from_iter((0..sorted.len()).map(|i| {
            if sorted.is_valid(i) {
                Some(sorted_dict.value(sorted_keys.value(i).as_usize()))
            } else {
                None
            }
        }));
        let expected = StringArray::from(expected_data);

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
        let array = DictionaryArray::<K>::new(keys, Arc::new(values));
        let array_values = array.values().clone();
        let dict = array_values.as_primitive::<T>();

        let sorted = match limit {
            Some(_) => sort_limit(&(Arc::new(array) as ArrayRef), options, limit).unwrap(),
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
        let expected: PrimitiveArray<T> = From::<Vec<Option<T::Native>>>::from(expected_data);

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
    fn slice_arrays(expected_output: Vec<ArrayRef>, offset: usize, length: usize) -> Vec<ArrayRef> {
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
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(data.iter().cloned(), length)
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
        test_sort_to_indices_primitive_arrays::<Float16Type>(
            vec![
                None,
                Some(f16::from_f32(-0.05)),
                Some(f16::from_f32(2.225)),
                Some(f16::from_f32(-1.01)),
                Some(f16::from_f32(-0.05)),
                None,
            ],
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
            vec![2, 1, 4, 3, 0, 5],
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 0, 5],
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 0, 5],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 0, 5],
        );

        test_sort_to_indices_primitive_arrays::<Float16Type>(
            vec![
                None,
                Some(f16::from_f32(0.005)),
                Some(f16::from_f32(20.22)),
                Some(f16::from_f32(-10.3)),
                Some(f16::from_f32(0.005)),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 0, 5],
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
            vec![2, 1, 4, 3, 0, 5],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![2, 1, 4, 3, 0, 5],
        );

        // descending, nulls first
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float16Type>(
            vec![
                None,
                Some(f16::from_f32(0.1)),
                Some(f16::from_f32(0.2)),
                Some(f16::from_f32(-1.3)),
                Some(f16::from_f32(0.01)),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![None, Some(0.1), Some(0.2), Some(-1.3), Some(0.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(10.1), Some(100.2), Some(-1.3), Some(10.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 1, 4, 3],
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
            vec![2, 3, 1, 4, 0, 5],
        );

        // boolean, descending, nulls first
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 5, 2, 3, 1, 4],
        );

        // boolean, descending, nulls first, limit
        test_sort_to_indices_boolean_arrays(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![0, 5, 2],
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
            vec![1, 5, 3, 2, 4, 0, 6],
        );
        // decimal null_first and descending
        test_sort_to_indices_decimal128_array(
            vec![None, Some(5), Some(2), Some(3), Some(1), Some(4), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 6, 1, 5, 3, 2, 4],
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
            vec![0, 6, 1],
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
        let data = vec![
            None,
            Some(i256::from_i128(5)),
            Some(i256::from_i128(2)),
            Some(i256::from_i128(3)),
            Some(i256::from_i128(1)),
            Some(i256::from_i128(4)),
            None,
        ];

        // decimal default
        test_sort_to_indices_decimal256_array(data.clone(), None, None, vec![0, 6, 4, 2, 3, 5, 1]);
        // decimal descending
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![1, 5, 3, 2, 4, 0, 6],
        );
        // decimal null_first and descending
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 6, 1, 5, 3, 2, 4],
        );
        // decimal null_first
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 6, 4, 2, 3, 5, 1],
        );
        // limit
        test_sort_to_indices_decimal256_array(data.clone(), None, Some(3), vec![0, 6, 4]);
        // limit descending
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            vec![1, 5, 3],
        );
        // limit descending null_first
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            vec![0, 6, 1],
        );
        // limit null_first
        test_sort_to_indices_decimal256_array(
            data,
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
        let data = vec![
            None,
            Some(i256::MIN),
            Some(i256::from_i128(1)),
            Some(i256::MAX),
            Some(i256::from_i128(-1)),
        ];
        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            vec![0, 1, 4, 2, 3],
        );

        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            vec![0, 3, 2, 4, 1],
        );

        test_sort_to_indices_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(4),
            vec![0, 1, 4, 2],
        );

        test_sort_to_indices_decimal256_array(
            data.clone(),
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
        let data = vec![
            None,
            Some(i256::from_i128(5)),
            Some(i256::from_i128(2)),
            Some(i256::from_i128(3)),
            Some(i256::from_i128(1)),
            Some(i256::from_i128(4)),
            None,
        ];
        // decimal default
        test_sort_decimal256_array(
            data.clone(),
            None,
            None,
            [None, None, Some(1), Some(2), Some(3), Some(4), Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal descending
        test_sort_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            [Some(5), Some(4), Some(3), Some(2), Some(1), None, None]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal null_first and descending
        test_sort_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            None,
            [None, None, Some(5), Some(4), Some(3), Some(2), Some(1)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // decimal null_first
        test_sort_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            None,
            [None, None, Some(1), Some(2), Some(3), Some(4), Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit
        test_sort_decimal256_array(
            data.clone(),
            None,
            Some(3),
            [None, None, Some(1)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit descending
        test_sort_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            Some(3),
            [Some(5), Some(4), Some(3)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit descending null_first
        test_sort_decimal256_array(
            data.clone(),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            Some(3),
            [None, None, Some(5)]
                .iter()
                .map(|v| v.map(i256::from_i128))
                .collect(),
        );
        // limit null_first
        test_sort_decimal256_array(
            data,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            [None, None, Some(1)]
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

        test_sort_primitive_arrays::<Float16Type>(
            vec![
                None,
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(2.0)),
                Some(f16::from_f32(-1.0)),
                Some(f16::from_f32(0.0)),
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
                Some(f16::from_f32(2.0)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(-1.0)),
            ],
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
        test_sort_primitive_arrays::<Float16Type>(
            vec![
                None,
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(2.0)),
                Some(f16::from_f32(-1.0)),
                Some(f16::from_f32(0.0)),
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
                Some(f16::from_f32(-1.0)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(2.0)),
            ],
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
            vec![2, 4, 1, 5, 0, 3],
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
            vec![0, 3, 2, 4, 1, 5],
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
            vec![0, 3, 2],
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
                Some("long string longer than 12 bytes"),
                None,
                Some("glad"),
                Some("lang string longer than 12 bytes"),
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
                Some("lang string longer than 12 bytes"),
                Some("long string longer than 12 bytes"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                Some("long string longer than 12 bytes"),
                None,
                Some("glad"),
                Some("lang string longer than 12 bytes"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            None,
            vec![
                Some("sad"),
                Some("long string longer than 12 bytes"),
                Some("lang string longer than 12 bytes"),
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
                Some("long string longer than 12 bytes"),
                Some("sad"),
                None,
                Some("glad"),
                Some("lang string longer than 12 bytes"),
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
                Some("lang string longer than 12 bytes"),
                Some("long string longer than 12 bytes"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("long string longer than 12 bytes"),
                Some("sad"),
                None,
                Some("glad"),
                Some("lang string longer than 12 bytes"),
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
                Some("long string longer than 12 bytes"),
                Some("lang string longer than 12 bytes"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("long string longer than 12 bytes"),
                Some("sad"),
                None,
                Some("glad"),
                Some("lang string longer than 12 bytes"),
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
            vec![
                Some("def long string longer than 12"),
                None,
                None,
                Some("abc"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(3),
            vec![Some("abc"), Some("def long string longer than 12"), None],
        );

        test_sort_string_arrays(
            vec![
                Some("def long string longer than 12"),
                None,
                None,
                Some("abc"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(3),
            vec![None, None, Some("abc")],
        );

        // more nulls than limit
        test_sort_string_arrays(
            vec![Some("def long string longer than 12"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            Some(2),
            vec![None, None],
        );

        test_sort_string_arrays(
            vec![Some("def long string longer than 12"), None, None, None],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            Some(2),
            vec![Some("def long string longer than 12"), None],
        );
    }

    #[test]
    fn test_sort_run_to_run() {
        test_sort_run_inner(|array, sort_options, limit| sort_run(array, sort_options, limit));
    }

    #[test]
    fn test_sort_run_to_indices() {
        test_sort_run_inner(|array, sort_options, limit| {
            let indices = sort_to_indices(array, sort_options, limit).unwrap();
            take(array, &indices, None)
        });
    }

    fn test_sort_run_inner<F>(sort_fn: F)
    where
        F: Fn(&dyn Array, Option<SortOptions>, Option<usize>) -> Result<ArrayRef, ArrowError>,
    {
        // Create an input array for testing
        let total_len = 80;
        let vals: Vec<Option<i32>> = vec![Some(1), None, Some(2), Some(3), Some(4), None, Some(5)];
        let repeats: Vec<usize> = vec![1, 3, 2, 4];
        let mut input_array: Vec<Option<i32>> = Vec::with_capacity(total_len);
        for ix in 0_usize..32 {
            let repeat: usize = repeats[ix % repeats.len()];
            let val: Option<i32> = vals[ix % vals.len()];
            input_array.resize(input_array.len() + repeat, val);
        }

        // create run array using input_array
        // Encode the input_array to run array
        let mut builder =
            PrimitiveRunBuilder::<Int16Type, Int32Type>::with_capacity(input_array.len());
        builder.extend(input_array.iter().copied());
        let run_array = builder.finish();

        // slice lengths that are tested
        let slice_lens = [
            1, 2, 3, 4, 5, 6, 7, 37, 38, 39, 40, 41, 42, 43, 74, 75, 76, 77, 78, 79, 80,
        ];
        for slice_len in slice_lens {
            test_sort_run_inner2(
                input_array.as_slice(),
                &run_array,
                0,
                slice_len,
                None,
                &sort_fn,
            );
            test_sort_run_inner2(
                input_array.as_slice(),
                &run_array,
                total_len - slice_len,
                slice_len,
                None,
                &sort_fn,
            );
            // Test with non zero limit
            if slice_len > 1 {
                test_sort_run_inner2(
                    input_array.as_slice(),
                    &run_array,
                    0,
                    slice_len,
                    Some(slice_len / 2),
                    &sort_fn,
                );
                test_sort_run_inner2(
                    input_array.as_slice(),
                    &run_array,
                    total_len - slice_len,
                    slice_len,
                    Some(slice_len / 2),
                    &sort_fn,
                );
            }
        }
    }

    fn test_sort_run_inner2<F>(
        input_array: &[Option<i32>],
        run_array: &RunArray<Int16Type>,
        offset: usize,
        length: usize,
        limit: Option<usize>,
        sort_fn: &F,
    ) where
        F: Fn(&dyn Array, Option<SortOptions>, Option<usize>) -> Result<ArrayRef, ArrowError>,
    {
        // Run the sort and build actual result
        let sliced_array = run_array.slice(offset, length);
        let sorted_sliced_array = sort_fn(&sliced_array, None, limit).unwrap();
        let sorted_run_array = sorted_sliced_array
            .as_any()
            .downcast_ref::<RunArray<Int16Type>>()
            .unwrap();
        let typed_run_array = sorted_run_array
            .downcast::<PrimitiveArray<Int32Type>>()
            .unwrap();
        let actual: Vec<Option<i32>> = typed_run_array.into_iter().collect();

        // build expected result.
        let mut sliced_input = input_array[offset..(offset + length)].to_owned();
        sliced_input.sort();
        let expected = if let Some(limit) = limit {
            sliced_input.iter().take(limit).copied().collect()
        } else {
            sliced_input
        };

        assert_eq!(expected, actual)
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

        test_sort_list_arrays::<Float16Type>(
            vec![
                Some(vec![Some(f16::from_f32(1.0)), Some(f16::from_f32(0.0))]),
                Some(vec![
                    Some(f16::from_f32(4.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(2.0)),
                    Some(f16::from_f32(1.0)),
                ]),
                Some(vec![
                    Some(f16::from_f32(2.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(4.0)),
                ]),
                Some(vec![
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                ]),
                Some(vec![Some(f16::from_f32(1.0)), Some(f16::from_f32(1.0))]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            None,
            vec![
                Some(vec![Some(f16::from_f32(1.0)), Some(f16::from_f32(0.0))]),
                Some(vec![Some(f16::from_f32(1.0)), Some(f16::from_f32(1.0))]),
                Some(vec![
                    Some(f16::from_f32(2.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(4.0)),
                ]),
                Some(vec![
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(3.0)),
                ]),
                Some(vec![
                    Some(f16::from_f32(4.0)),
                    Some(f16::from_f32(3.0)),
                    Some(f16::from_f32(2.0)),
                    Some(f16::from_f32(1.0)),
                ]),
            ],
            None,
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
        test_lex_sort_arrays(input.clone(), slice_arrays(expected.clone(), 0, 5), Some(5));

        // Limiting by more rows than present is ok
        test_lex_sort_arrays(input, slice_arrays(expected, 0, 5), Some(10));

        // test with FixedSizeListArray, arrays order: [UInt32, FixedSizeList(UInt32, 1)]

        // case1
        let primitive_array_data = vec![
            Some(2),
            Some(3),
            Some(2),
            Some(0),
            None,
            Some(2),
            Some(1),
            Some(2),
        ];
        let list_array_data = vec![
            None,
            Some(vec![Some(4)]),
            Some(vec![Some(3)]),
            Some(vec![Some(1)]),
            Some(vec![Some(5)]),
            Some(vec![Some(0)]),
            Some(vec![Some(2)]),
            Some(vec![None]),
        ];

        let expected_primitive_array_data = vec![
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            Some(3),
        ];
        let expected_list_array_data = vec![
            Some(vec![Some(5)]),
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            None, // <-
            Some(vec![None]),
            Some(vec![Some(0)]),
            Some(vec![Some(3)]), // <-
            Some(vec![Some(4)]),
        ];
        test_lex_sort_mixed_types_with_fixed_size_list::<Int32Type>(
            primitive_array_data.clone(),
            list_array_data.clone(),
            expected_primitive_array_data.clone(),
            expected_list_array_data,
            None,
            None,
        );

        // case2
        let primitive_array_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let list_array_options = SortOptions {
            descending: false,
            nulls_first: false, // has been modified
        };
        let expected_list_array_data = vec![
            Some(vec![Some(5)]),
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            Some(vec![Some(0)]), // <-
            Some(vec![Some(3)]),
            Some(vec![None]),
            None, // <-
            Some(vec![Some(4)]),
        ];
        test_lex_sort_mixed_types_with_fixed_size_list::<Int32Type>(
            primitive_array_data.clone(),
            list_array_data.clone(),
            expected_primitive_array_data.clone(),
            expected_list_array_data,
            Some(primitive_array_options),
            Some(list_array_options),
        );

        // case3
        let primitive_array_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let list_array_options = SortOptions {
            descending: true, // has been modified
            nulls_first: true,
        };
        let expected_list_array_data = vec![
            Some(vec![Some(5)]),
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            None, // <-
            Some(vec![None]),
            Some(vec![Some(3)]),
            Some(vec![Some(0)]), // <-
            Some(vec![Some(4)]),
        ];
        test_lex_sort_mixed_types_with_fixed_size_list::<Int32Type>(
            primitive_array_data.clone(),
            list_array_data.clone(),
            expected_primitive_array_data,
            expected_list_array_data,
            Some(primitive_array_options),
            Some(list_array_options),
        );

        // test with ListArray/LargeListArray, arrays order: [List<UInt32>/LargeList<UInt32>, UInt32]

        let list_array_data = vec![
            Some(vec![Some(2), Some(1)]), // 0
            None,                         // 10
            Some(vec![Some(3)]),          // 1
            Some(vec![Some(2), Some(0)]), // 2
            Some(vec![None, Some(2)]),    // 3
            Some(vec![Some(0)]),          // none
            None,                         // 11
            Some(vec![Some(2), None]),    // 4
            Some(vec![None]),             // 5
            Some(vec![Some(2), Some(1)]), // 6
        ];
        let primitive_array_data = vec![
            Some(0),
            Some(10),
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(11),
            Some(4),
            Some(5),
            Some(6),
        ];
        let expected_list_array_data = vec![
            None,
            None,
            Some(vec![None]),
            Some(vec![None, Some(2)]),
            Some(vec![Some(0)]),
            Some(vec![Some(2), None]),
            Some(vec![Some(2), Some(0)]),
            Some(vec![Some(2), Some(1)]),
            Some(vec![Some(2), Some(1)]),
            Some(vec![Some(3)]),
        ];
        let expected_primitive_array_data = vec![
            Some(10),
            Some(11),
            Some(5),
            Some(3),
            None,
            Some(4),
            Some(2),
            Some(0),
            Some(6),
            Some(1),
        ];
        test_lex_sort_mixed_types_with_list::<Int32Type>(
            list_array_data.clone(),
            primitive_array_data.clone(),
            expected_list_array_data,
            expected_primitive_array_data,
            None,
            None,
        );
    }

    fn test_lex_sort_mixed_types_with_fixed_size_list<T>(
        primitive_array_data: Vec<Option<T::Native>>,
        list_array_data: Vec<Option<Vec<Option<T::Native>>>>,
        expected_primitive_array_data: Vec<Option<T::Native>>,
        expected_list_array_data: Vec<Option<Vec<Option<T::Native>>>>,
        primitive_array_options: Option<SortOptions>,
        list_array_options: Option<SortOptions>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<T>::from(primitive_array_data.clone()))
                    as ArrayRef,
                options: primitive_array_options,
            },
            SortColumn {
                values: Arc::new(FixedSizeListArray::from_iter_primitive::<T, _, _>(
                    list_array_data.clone(),
                    1,
                )) as ArrayRef,
                options: list_array_options,
            },
        ];

        let expected = vec![
            Arc::new(PrimitiveArray::<T>::from(
                expected_primitive_array_data.clone(),
            )) as ArrayRef,
            Arc::new(FixedSizeListArray::from_iter_primitive::<T, _, _>(
                expected_list_array_data.clone(),
                1,
            )) as ArrayRef,
        ];

        test_lex_sort_arrays(input.clone(), expected.clone(), None);
        test_lex_sort_arrays(input.clone(), slice_arrays(expected.clone(), 0, 5), Some(5));
    }

    fn test_lex_sort_mixed_types_with_list<T>(
        list_array_data: Vec<Option<Vec<Option<T::Native>>>>,
        primitive_array_data: Vec<Option<T::Native>>,
        expected_list_array_data: Vec<Option<Vec<Option<T::Native>>>>,
        expected_primitive_array_data: Vec<Option<T::Native>>,
        list_array_options: Option<SortOptions>,
        primitive_array_options: Option<SortOptions>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        macro_rules! run_test {
            ($ARRAY_TYPE:ident) => {
                let input = vec![
                    SortColumn {
                        values: Arc::new(<$ARRAY_TYPE>::from_iter_primitive::<T, _, _>(
                            list_array_data.clone(),
                        )) as ArrayRef,
                        options: list_array_options.clone(),
                    },
                    SortColumn {
                        values: Arc::new(PrimitiveArray::<T>::from(primitive_array_data.clone()))
                            as ArrayRef,
                        options: primitive_array_options.clone(),
                    },
                ];

                let expected = vec![
                    Arc::new(<$ARRAY_TYPE>::from_iter_primitive::<T, _, _>(
                        expected_list_array_data.clone(),
                    )) as ArrayRef,
                    Arc::new(PrimitiveArray::<T>::from(
                        expected_primitive_array_data.clone(),
                    )) as ArrayRef,
                ];

                test_lex_sort_arrays(input.clone(), expected.clone(), None);
                test_lex_sort_arrays(input.clone(), slice_arrays(expected.clone(), 0, 5), Some(5));
            };
        }
        run_test!(ListArray);
        run_test!(LargeListArray);
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
        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Int8Array::from(vec![1, 3, 5]);
        test_sort_primitive_dict_arrays::<Int8Type, Int8Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, Some(1), Some(3), Some(5), Some(5)],
        );

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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
        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
        let values = Float32Array::from(vec![1.2, 3.0, 5.1]);
        test_sort_primitive_dict_arrays::<Int8Type, Float32Type>(
            keys,
            values,
            None,
            None,
            vec![None, None, Some(1.2), Some(3.0), Some(5.1), Some(5.1)],
        );

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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

        let keys = Int8Array::from(vec![Some(1_i8), None, Some(2), None, Some(2), Some(0)]);
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

    #[test]
    fn test_lexicographic_comparator_null_dict_values() {
        let values = Int32Array::new(
            vec![1, 2, 3, 4].into(),
            Some(NullBuffer::from(vec![true, false, false, true])),
        );
        let keys = Int32Array::new(
            vec![0, 1, 53, 3].into(),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        // [1, NULL, NULL, 4]
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let comparator = LexicographicalComparator::try_new(&[SortColumn {
            values: Arc::new(dict),
            options: None,
        }])
        .unwrap();
        // 1.cmp(NULL)
        assert_eq!(comparator.compare(0, 1), Ordering::Greater);
        // NULL.cmp(NULL)
        assert_eq!(comparator.compare(2, 1), Ordering::Equal);
        // NULL.cmp(4)
        assert_eq!(comparator.compare(2, 3), Ordering::Less);
    }

    #[test]
    fn sort_list_equal() {
        let a = {
            let mut builder = FixedSizeListBuilder::new(Int64Builder::new(), 2);
            for value in [[1, 5], [0, 3], [1, 3]] {
                builder.values().append_slice(&value);
                builder.append(true);
            }
            builder.finish()
        };

        let sort_indices = sort_to_indices(&a, None, None).unwrap();
        assert_eq!(sort_indices.values(), &[1, 2, 0]);

        let a = {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for value in [[1, 5], [0, 3], [1, 3]] {
                builder.values().append_slice(&value);
                builder.append(true);
            }
            builder.finish()
        };

        let sort_indices = sort_to_indices(&a, None, None).unwrap();
        assert_eq!(sort_indices.values(), &[1, 2, 0]);
    }

    #[test]
    fn sort_struct_fallback_to_lexsort() {
        let float = Arc::new(Float32Array::from(vec![1.0, -0.1, 3.5, 1.0]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Float32, false)),
                float.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
        ]);

        assert!(!can_sort_to_indices(struct_array.data_type()));
        assert!(sort_to_indices(&struct_array, None, None)
            .err()
            .unwrap()
            .to_string()
            .contains("Sort not supported for data type"));

        let sort_columns = vec![SortColumn {
            values: Arc::new(struct_array.clone()) as ArrayRef,
            options: None,
        }];
        let sorted = lexsort(&sort_columns, None).unwrap();

        let expected_struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Float32, false)),
                Arc::new(Float32Array::from(vec![-0.1, 1.0, 1.0, 3.5])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![28, 31, 42, 19])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&sorted[0], &expected_struct_array);
    }
}
