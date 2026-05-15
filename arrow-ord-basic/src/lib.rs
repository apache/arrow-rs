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

//! Basic comparator factories shared by Arrow crates that need to compare
//! arbitrary array slots without pulling in the full [`arrow-ord`] crate.
//!
//! The only public surface is [`make_comparator`] (with [`DynComparator`] as the
//! returned function type). `arrow-ord` re-exports both from here, so its
//! public API is unchanged.
//!
//! This crate exists so that crates such as `arrow-select` can use slot-wise
//! comparison (e.g. for the run-end-encoded `take` fast path) without taking on
//! the full ordering kernel suite — which would either create a circular
//! dependency (`arrow-ord` already depends on `arrow-select`) or force every
//! downstream user of `arrow-array` to compile the comparator machinery whether
//! they need it or not.

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::{ArrowError, DataType, SortOptions};
use std::{cmp::Ordering, collections::HashMap};

fn compare_run_end_encoded<R: RunEndIndexType>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_run::<R>();
    let right = right.as_run::<R>();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.values().as_ref(), right.values().as_ref(), c_opts)?;

    let l_run_ends = left.run_ends().clone();
    let r_run_ends = right.run_ends().clone();

    let f = compare(left, right, opts, move |i, j| {
        let l_physical = l_run_ends.get_physical_index(i);
        let r_physical = r_run_ends.get_physical_index(j);
        cmp(l_physical, r_physical)
    });
    Ok(f)
}

/// Compare values at arbitrary indices in two arrays.
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
        compare_byte_view_values(&l, i, &r, j)
    })
}

fn compare_byte_view_values<T: ByteViewType>(
    left: &GenericByteViewArray<T>,
    left_idx: usize,
    right: &GenericByteViewArray<T>,
    right_idx: usize,
) -> Ordering {
    assert!(left_idx < left.len());
    assert!(right_idx < right.len());

    if left.data_buffers().is_empty() && right.data_buffers().is_empty() {
        let l_view = unsafe { left.views().get_unchecked(left_idx) };
        let r_view = unsafe { right.views().get_unchecked(right_idx) };
        return GenericByteViewArray::<T>::inline_key_fast(*l_view)
            .cmp(&GenericByteViewArray::<T>::inline_key_fast(*r_view));
    }

    unsafe { GenericByteViewArray::compare_unchecked(left, left_idx, right, right_idx) }
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

fn compare_list_view<O: OffsetSizeTrait>(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_list_view::<O>();
    let right = right.as_list_view::<O>();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.values().as_ref(), right.values().as_ref(), c_opts)?;

    let l_offsets = left.offsets().clone();
    let l_sizes = left.sizes().clone();
    let r_offsets = right.offsets().clone();
    let r_sizes = right.sizes().clone();

    let f = compare(left, right, opts, move |i, j| {
        let l_start = l_offsets[i].as_usize();
        let l_len = l_sizes[i].as_usize();
        let l_end = l_start + l_len;

        let r_start = r_offsets[j].as_usize();
        let r_len = r_sizes[j].as_usize();
        let r_end = r_start + r_len;

        for (i, j) in (l_start..l_end).zip(r_start..r_end) {
            match cmp(i, j) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        l_len.cmp(&r_len)
    });
    Ok(f)
}

fn compare_map(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_map();
    let right = right.as_map();

    let c_opts = child_opts(opts);
    let cmp = make_comparator(left.entries(), right.entries(), c_opts)?;

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

fn compare_union(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<DynComparator, ArrowError> {
    let left = left.as_union();
    let right = right.as_union();

    let (left_fields, left_mode) = match left.data_type() {
        DataType::Union(fields, mode) => (fields, mode),
        _ => unreachable!(),
    };
    let (right_fields, right_mode) = match right.data_type() {
        DataType::Union(fields, mode) => (fields, mode),
        _ => unreachable!(),
    };

    if left_fields != right_fields {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare UnionArrays with different fields: left={left_fields:?}, right={right_fields:?}"
        )));
    }

    if left_mode != right_mode {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare UnionArrays with different modes: left={left_mode:?}, right={right_mode:?}"
        )));
    }

    let c_opts = child_opts(opts);

    let mut field_comparators = HashMap::with_capacity(left_fields.len());

    for (type_id, _field) in left_fields.iter() {
        let left_child = left.child(type_id);
        let right_child = right.child(type_id);
        let cmp = make_comparator(left_child.as_ref(), right_child.as_ref(), c_opts)?;

        field_comparators.insert(type_id, cmp);
    }

    let left_type_ids = left.type_ids().clone();
    let right_type_ids = right.type_ids().clone();

    let left_offsets = left.offsets().cloned();
    let right_offsets = right.offsets().cloned();

    let f = compare(left, right, opts, move |i, j| {
        let left_type_id = left_type_ids[i];
        let right_type_id = right_type_ids[j];

        match left_type_id.cmp(&right_type_id) {
            Ordering::Equal => {
                let left_offset = left_offsets.as_ref().map(|o| o[i] as usize).unwrap_or(i);
                let right_offset = right_offsets.as_ref().map(|o| o[j] as usize).unwrap_or(j);

                let cmp = field_comparators
                    .get(&left_type_id)
                    .expect("type id not found in field_comparators");

                cmp(left_offset, right_offset)
            }
            other => other,
        }
    });
    Ok(f)
}

/// Returns a comparison function that compares two values at two arbitrary indices.
///
/// If `nulls_first` is true, null values are considered less than any non-null
/// value; otherwise they are considered greater. This is primarily shared by
/// crates that need repeated slot comparisons without constructing sliced arrays.
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
        (ListView(_), ListView(_)) => compare_list_view::<i32>(left, right, opts),
        (LargeListView(_), LargeListView(_)) => compare_list_view::<i64>(left, right, opts),
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
        (RunEndEncoded(l_run_ends, _), RunEndEncoded(r_run_ends, _)) => {
            macro_rules! run_end_helper {
                ($t:ty, $left:expr, $right:expr, $opts:expr) => {
                    compare_run_end_encoded::<$t>($left, $right, $opts)
                };
            }
            downcast_run_end_index! {
                l_run_ends.data_type(), r_run_ends.data_type() => (run_end_helper, left, right, opts),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Cannot compare RunEndEncoded arrays with different run ends types: left={:?}, right={:?}",
                    l_run_ends.data_type(),
                    r_run_ends.data_type()
                )))
            }
        },
        (Map(_, _), Map(_, _)) => compare_map(left, right, opts),
        (Null, Null) => Ok(Box::new(|_, _| Ordering::Equal)),
        (Union(_, _), Union(_, _)) => compare_union(left, right, opts),
        (lhs, rhs) => Err(ArrowError::InvalidArgumentError(match lhs == rhs {
            true => format!("The data type type {lhs:?} has no natural order"),
            false => "Can't compare arrays of different types".to_string(),
        }))
    }
}
