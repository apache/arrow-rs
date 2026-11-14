// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`minify`]: Copy an array into buffers that hold only the data its rows reference
//!
//! Arrays often keep memory alive that their visible rows do not use:
//!
//! * A sliced array shares the buffers of the original, unsliced array.
//! * A sliced list, map or byte array shares the full child array or value buffer.
//! * A view array can reference a small part of large data buffers.
//! * A dictionary can hold values that no key references.
//! * A dense union can hold child rows that no row references.
//!
//! [`minify`] returns a logically equal array without this unused memory.
//!
//! # Reuse policy
//!
//! [`minify`] copies a buffer only when its allocation is larger than the
//! visible data needs (rounded up to 64 bytes). Otherwise it reuses the buffer
//! without a copy. This makes `minify` cheap on an array that is already
//! minimal, and calling it twice copies nothing the second time.
//!
//! A buffer that other arrays share counts as too large when its allocation
//! is larger than the visible data. `minify` copies it, so the total memory in
//! use can grow until the other arrays drop the shared buffer.
//!
//! A buffer can also appear more than once inside `array`, for example in
//! two struct columns over the same array, or in two dictionaries over the
//! same values. Struct columns over the same rows of the same buffers share
//! one minified column. In other cases `minify` can copy the buffer once per
//! reference. If the result then uses more memory than `array` (beyond 64
//! bytes per allocation), `minify` returns `array` unchanged.
//!
//! # Null rows
//!
//! `minify` gives every null row of a byte, list, map or list view array an
//! empty value range, and every null row of a byte view array an empty view.
//! The child array or value buffers then hold only the data of valid rows.
//!
//! An O(rows) check over the offsets and the null bitmap finds out if any null
//! row has a non-empty range. If none has, `minify` copies or slices the
//! referenced range in one piece. Otherwise it copies the ranges of each run of
//! valid rows.
//!
//! # Sizes
//!
//! [`compacted_size`] returns the bytes that `minify` would return, without a
//! copy. [`row_sizes`] attributes these bytes to rows, so a caller can cut a
//! batch at byte boundaries with prefix sums or [`split_by_size`].
//!
//! # Limitations
//!
//! * `minify` does not deduplicate dictionary values or merge equal runs.
//! * Null rows of a struct or fixed size list array keep their child data.
//!   The child rows stay at the same positions, so `minify` cannot drop them.
//! * The rows of a sparse union keep the data of all children, also of the
//!   children that their type ids do not select.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer,
    ScalarBuffer, bit_util,
};
use arrow_data::transform::{Capacities, MutableArrayData};
use arrow_data::{ArrayData, ByteView, MAX_INLINE_VIEW_LEN};
use arrow_schema::{
    ArrowError, DataType, FieldRef, IntervalUnit, TimeUnit, UnionFields, UnionMode,
};

use crate::take::take;

/// Returns an array that is logically equal to `array` and whose buffers hold
/// only the data that its rows reference.
///
/// The result has an offset of zero, except when `minify` returns `array`
/// unchanged because aliased buffers would make the result larger (see the
/// [module documentation](self)). `minify` applies itself recursively to
/// child arrays, dictionary values and run-end encoded values.
///
/// See the [module documentation](self) for the policy on when `minify`
/// reuses a buffer instead of copying it.
///
/// # Example
///
/// ```
/// # use arrow_array::{Array, Int32Array};
/// # use arrow_select::minify::minify;
/// let array = Int32Array::from_iter_values(0..1024);
/// let sliced = array.slice(10, 4);
/// // The slice keeps the whole 4 KiB buffer alive
/// assert_eq!(sliced.get_buffer_memory_size(), 4096);
///
/// let minified = minify(&sliced).unwrap();
/// assert_eq!(minified.as_ref(), &sliced as &dyn Array);
/// assert_eq!(minified.get_buffer_memory_size(), 64);
/// ```
///
/// # Errors
///
/// `minify` returns `InvalidArgumentError` for a time type with an invalid
/// unit, a dictionary with an invalid key type and a run-end encoded array
/// with an invalid run end type. It also returns the error of an array
/// constructor that rejects the minified parts.
pub fn minify(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let minified = minify_inner(array)?;
    // Buffers that `array` references more than once can be copied once per
    // reference. Then `array` itself uses less memory. The slack of 64 bytes
    // per allocation ignores the rounding of small allocations.
    let (after, allocations) = allocated_memory(&minified.to_data());
    let (before, _) = allocated_memory(&array.to_data());
    if grew(before, after, allocations) {
        return Ok(make_array(array.to_data()));
    }
    Ok(minified)
}

/// Returns `true` if `after` bytes in `allocations` allocations exceed
/// `before` bytes by more than 64 bytes per allocation.
fn grew(before: usize, after: usize, allocations: usize) -> bool {
    after > before + 64 * allocations
}

/// Returns the capacity and the number of all distinct allocations that
/// `data` and its children reference.
fn allocated_memory(data: &ArrayData) -> (usize, usize) {
    fn visit(data: &ArrayData, seen: &mut HashSet<*const u8>, total: &mut usize) {
        let buffers = data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()));
        for b in buffers {
            if seen.insert(b.data_ptr().as_ptr()) {
                *total += b.capacity();
            }
        }
        data.child_data().iter().for_each(|c| visit(c, seen, total));
    }
    let mut total = 0;
    let mut seen = HashSet::new();
    visit(data, &mut seen, &mut total);
    (total, seen.len())
}

/// Returns `true` if `a` and `b` describe the same rows of the same buffers.
fn same_data(a: &ArrayData, b: &ArrayData) -> bool {
    let same_buffer = |x: &Buffer, y: &Buffer| x.as_ptr() == y.as_ptr() && x.len() == y.len();
    let same_nulls = match (a.nulls(), b.nulls()) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            x.offset() == y.offset() && x.len() == y.len() && same_buffer(x.buffer(), y.buffer())
        }
        _ => false,
    };
    a.data_type() == b.data_type()
        && a.len() == b.len()
        && a.offset() == b.offset()
        && same_nulls
        && a.buffers().len() == b.buffers().len()
        && a.buffers()
            .iter()
            .zip(b.buffers())
            .all(|(x, y)| same_buffer(x, y))
        && a.child_data().len() == b.child_data().len()
        && a.child_data()
            .iter()
            .zip(b.child_data())
            .all(|(x, y)| same_data(x, y))
}

fn minify_inner(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    macro_rules! primitive {
        ($t:ty) => {
            Ok(Arc::new(minify_primitive(array.as_primitive::<$t>())))
        };
    }
    match array.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(array.len()))),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float16 => primitive!(Float16Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Decimal32(_, _) => primitive!(Decimal32Type),
        DataType::Decimal64(_, _) => primitive!(Decimal64Type),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Decimal256(_, _) => primitive!(Decimal256Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        t @ (DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond)) => Err(
            ArrowError::InvalidArgumentError(format!("invalid time unit in {t}")),
        ),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondType),
        DataType::Interval(IntervalUnit::YearMonth) => primitive!(IntervalYearMonthType),
        DataType::Interval(IntervalUnit::DayTime) => primitive!(IntervalDayTimeType),
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive!(IntervalMonthDayNanoType),
        DataType::Boolean => {
            let array = array.as_boolean();
            let values = minify_bool_buffer(array.values());
            Ok(Arc::new(BooleanArray::new(
                values,
                minify_nulls(array.nulls()),
            )))
        }
        DataType::Utf8 => Ok(Arc::new(minify_bytes(array.as_bytes::<Utf8Type>()))),
        DataType::LargeUtf8 => Ok(Arc::new(minify_bytes(array.as_bytes::<LargeUtf8Type>()))),
        DataType::Binary => Ok(Arc::new(minify_bytes(array.as_bytes::<BinaryType>()))),
        DataType::LargeBinary => Ok(Arc::new(minify_bytes(array.as_bytes::<LargeBinaryType>()))),
        DataType::Utf8View => Ok(Arc::new(minify_byte_view(
            array.as_byte_view::<StringViewType>(),
        ))),
        DataType::BinaryView => Ok(Arc::new(minify_byte_view(
            array.as_byte_view::<BinaryViewType>(),
        ))),
        DataType::FixedSizeBinary(size) => {
            let array = array.as_fixed_size_binary();
            let values = minify_buffer(array.values());
            Ok(Arc::new(FixedSizeBinaryArray::try_new_with_len(
                *size,
                values,
                minify_nulls(array.nulls()),
                array.len(),
            )?))
        }
        DataType::List(field) => minify_list(array.as_list::<i32>(), field),
        DataType::LargeList(field) => minify_list(array.as_list::<i64>(), field),
        DataType::ListView(field) => minify_list_view(array.as_list_view::<i32>(), field),
        DataType::LargeListView(field) => minify_list_view(array.as_list_view::<i64>(), field),
        DataType::FixedSizeList(field, size) => {
            let array = array.as_fixed_size_list();
            // `FixedSizeListArray::slice` already slices the child array
            let values = minify_inner(array.values().as_ref())?;
            Ok(Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                *size,
                values,
                minify_nulls(array.nulls()),
                array.len(),
            )?))
        }
        DataType::Map(field, ordered) => {
            let array = array.as_map();
            let (offsets, entries) =
                minify_list_child(array.offsets(), array.entries(), array.nulls())?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                minify_nulls(array.nulls()),
                *ordered,
            )?))
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            // `StructArray::slice` already slices the child arrays
            // Columns over the same data share one minified column
            let mut done: Vec<(ArrayData, ArrayRef)> = vec![];
            let mut columns = Vec::with_capacity(array.num_columns());
            for c in array.columns() {
                let data = c.to_data();
                let column = match done.iter().find(|(d, _)| same_data(d, &data)) {
                    Some((_, m)) => Arc::clone(m),
                    None => {
                        let m = minify_inner(c.as_ref())?;
                        done.push((data, Arc::clone(&m)));
                        m
                    }
                };
                columns.push(column);
            }
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                minify_nulls(array.nulls()),
                array.len(),
            )?))
        }
        DataType::Union(fields, mode) => minify_union(array.as_union(), fields, *mode),
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => minify_dictionary(array.as_dictionary::<Int8Type>()),
            DataType::Int16 => minify_dictionary(array.as_dictionary::<Int16Type>()),
            DataType::Int32 => minify_dictionary(array.as_dictionary::<Int32Type>()),
            DataType::Int64 => minify_dictionary(array.as_dictionary::<Int64Type>()),
            DataType::UInt8 => minify_dictionary(array.as_dictionary::<UInt8Type>()),
            DataType::UInt16 => minify_dictionary(array.as_dictionary::<UInt16Type>()),
            DataType::UInt32 => minify_dictionary(array.as_dictionary::<UInt32Type>()),
            DataType::UInt64 => minify_dictionary(array.as_dictionary::<UInt64Type>()),
            t => Err(ArrowError::InvalidArgumentError(format!(
                "invalid dictionary key type {t}"
            ))),
        },
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => minify_run(array.as_run::<Int16Type>()),
            DataType::Int32 => minify_run(array.as_run::<Int32Type>()),
            DataType::Int64 => minify_run(array.as_run::<Int64Type>()),
            t => Err(ArrowError::InvalidArgumentError(format!(
                "invalid run end type {t}"
            ))),
        },
    }
}

/// Returns `true` if `buffer` owns an allocation that is larger than its
/// length (rounded up to 64 bytes).
fn is_oversized(buffer: &Buffer) -> bool {
    buffer.capacity() > bit_util::round_upto_multiple_of_64(buffer.len())
}

/// Returns `buffer`, or a copy of `buffer` if its allocation is oversized.
fn minify_buffer(buffer: &Buffer) -> Buffer {
    if is_oversized(buffer) {
        Buffer::from_slice_ref(buffer.as_slice())
    } else {
        buffer.clone()
    }
}

fn minify_scalar_buffer<T: ArrowNativeType>(buffer: &ScalarBuffer<T>) -> ScalarBuffer<T> {
    ScalarBuffer::new(minify_buffer(buffer.inner()), 0, buffer.len())
}

fn minify_bool_buffer(buffer: &BooleanBuffer) -> BooleanBuffer {
    // `sliced` copies the bits when the offset is not a multiple of 8.
    // Otherwise it returns a zero-copy slice, which `minify_buffer` checks.
    BooleanBuffer::new(minify_buffer(&buffer.sliced()), 0, buffer.len())
}

/// Copies the null buffer if needed, and drops it if it has no nulls.
fn minify_nulls(nulls: Option<&NullBuffer>) -> Option<NullBuffer> {
    let nulls = nulls.filter(|n| n.null_count() > 0)?;
    let buffer = minify_bool_buffer(nulls.inner());
    // SAFETY: the bits and so the null count do not change
    Some(unsafe { NullBuffer::new_unchecked(buffer, nulls.null_count()) })
}

/// Returns offsets that start at zero, plus the start and end of the child
/// range that the original offsets reference.
fn rebase_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
) -> (OffsetBuffer<O>, usize, usize) {
    let start = offsets[0];
    let end = offsets[offsets.len() - 1];
    let values = if start == O::zero() {
        minify_scalar_buffer(offsets.inner())
    } else {
        offsets.iter().map(|o| *o - start).collect()
    };
    // SAFETY: subtracting the first offset keeps the offsets monotonic and non-negative
    let offsets = unsafe { OffsetBuffer::new_unchecked(values) };
    (offsets, start.as_usize(), end.as_usize())
}

/// Returns the null buffer if a null row has a non-empty range in `offsets`.
///
/// Offsets never decrease, so a run of null rows has only empty ranges if
/// the offsets at its start and end are equal. The check costs O(runs).
fn nulls_with_ranges<'a, O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&'a NullBuffer>,
) -> Option<&'a NullBuffer> {
    let nulls = nulls.filter(|n| n.null_count() > 0)?;
    let mut null_start = 0;
    for (start, end) in nulls.valid_slices().chain([(nulls.len(), nulls.len())]) {
        if offsets[null_start] != offsets[start] {
            return Some(nulls);
        }
        null_start = end;
    }
    None
}

/// Returns offsets that give every null row an empty range, plus the child
/// ranges of the valid rows. Adjacent child ranges merge into one range.
fn compact_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: &NullBuffer,
) -> (OffsetBuffer<O>, Vec<Range<usize>>) {
    let mut ranges: Vec<Range<usize>> = vec![];
    let mut compacted = Vec::with_capacity(offsets.len());
    compacted.push(O::zero());
    let mut null_start = 0;
    for (s, e) in nulls.valid_slices() {
        let last = compacted[compacted.len() - 1];
        compacted.extend(std::iter::repeat_n(last, s - null_start));
        let base = offsets[s];
        compacted.extend(offsets[s + 1..=e].iter().map(|o| last + (*o - base)));
        null_start = e;

        push_range(&mut ranges, base.as_usize()..offsets[e].as_usize());
    }
    let last = compacted[compacted.len() - 1];
    compacted.resize(offsets.len(), last);
    // SAFETY: the offsets start at zero and never decrease
    let offsets = unsafe { OffsetBuffer::new_unchecked(compacted.into()) };
    (offsets, ranges)
}

/// Returns `true` if [`gather_ranges`] gathers `data_type` by its parts
/// instead of with `MutableArrayData`.
///
/// `MutableArrayData` copies a child row that several rows share once per
/// row. Dense union rows can share child rows, and list views can overlap.
/// `MutableArrayData` also splits the runs of a run-end encoded array in
/// its own way, so the result of `minify` would depend on the gather path.
fn gathers_parts(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(_, UnionMode::Dense)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::RunEndEncoded(_, _) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => gathers_parts(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| gathers_parts(f.data_type())),
        DataType::Union(fields, UnionMode::Sparse) => {
            fields.iter().any(|(_, f)| gathers_parts(f.data_type()))
        }
        DataType::Dictionary(_, values) => gathers_parts(values),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8View
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => false,
    }
}

/// Appends `range` to `ranges`, and merges it with the last range if they
/// are adjacent. Skips an empty range.
fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    match ranges.last_mut() {
        Some(r) if r.end == range.start => r.end = range.end,
        _ if range.start < range.end => ranges.push(range),
        _ => {}
    }
}

/// Copies `ranges` of `values` back to back into a new minified array.
///
/// See [`gathers_parts`] for the types that `gather_ranges` gathers without
/// `MutableArrayData`. It selects their rows without a copy of the children,
/// and `minify` then keeps shared child rows shared.
fn gather_ranges(values: &dyn Array, ranges: &[Range<usize>]) -> Result<ArrayRef, ArrowError> {
    let len = ranges.iter().map(|r| r.len()).sum();
    if !gathers_parts(values.data_type()) {
        return gather_ranges_copy(values, ranges, len);
    }
    let gathered: ArrayRef = match values.data_type() {
        DataType::Union(fields, UnionMode::Dense) => Arc::new(select_dense_union(
            values.as_union(),
            fields,
            ranges.iter().flat_map(|r| r.clone()),
        )),
        // `take` keeps the child of a list view
        DataType::ListView(_) | DataType::LargeListView(_) => {
            let indices = ranges.iter().flat_map(|r| r.start as u64..r.end as u64);
            take(values, &UInt64Array::from_iter_values(indices), None)?
        }
        DataType::List(field) => gather_list(values.as_list::<i32>(), field, ranges)?,
        DataType::LargeList(field) => gather_list(values.as_list::<i64>(), field, ranges)?,
        DataType::Map(field, ordered) => {
            let array = values.as_map();
            let (offsets, child_ranges) = gather_offsets(array.offsets(), array.nulls(), ranges);
            let entries = gather_ranges(array.entries(), &child_ranges)?;
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                gather_nulls(array.nulls(), ranges, len),
                *ordered,
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let array = values.as_fixed_size_list();
            let size = *size as usize;
            let child_ranges: Vec<_> = ranges
                .iter()
                .map(|r| r.start * size..r.end * size)
                .collect();
            Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                size as i32,
                gather_ranges(array.values().as_ref(), &child_ranges)?,
                gather_nulls(array.nulls(), ranges, len),
                len,
            )?)
        }
        DataType::Struct(fields) => {
            let array = values.as_struct();
            let columns = array
                .columns()
                .iter()
                .map(|c| gather_ranges(c.as_ref(), ranges))
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                gather_nulls(array.nulls(), ranges, len),
                len,
            )?)
        }
        DataType::Union(fields, UnionMode::Sparse) => {
            let array = values.as_union();
            let type_ids = ranges
                .iter()
                .flat_map(|r| array.type_ids()[r.clone()].iter().copied())
                .collect();
            let children = fields
                .iter()
                .map(|(type_id, _)| gather_ranges(array.child(type_id).as_ref(), ranges))
                .collect::<Result<Vec<_>, _>>()?;
            // SAFETY: each row keeps its type id and its row in every child
            Arc::new(unsafe { UnionArray::new_unchecked(fields.clone(), type_ids, None, children) })
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => gather_run(values.as_run::<Int16Type>(), ranges)?,
            DataType::Int32 => gather_run(values.as_run::<Int32Type>(), ranges)?,
            DataType::Int64 => gather_run(values.as_run::<Int64Type>(), ranges)?,
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "invalid run end type {t}"
                )));
            }
        },
        // `MutableArrayData` keeps the values of a dictionary, and `minify`
        // prunes them afterwards. `gathers_parts` returns `false` for the
        // flat types, so they do not reach this match.
        DataType::Dictionary(_, _)
        | DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8View
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => return gather_ranges_copy(values, ranges, len),
    };
    minify_inner(gathered.as_ref())
}

/// Copies `ranges` of `values` with `MutableArrayData`.
fn gather_ranges_copy(
    values: &dyn Array,
    ranges: &[Range<usize>],
    len: usize,
) -> Result<ArrayRef, ArrowError> {
    let data = values.to_data();
    // Exact byte capacities let `minify` reuse the gathered buffers
    fn bytes<O: OffsetSizeTrait>(data: &ArrayData, ranges: &[Range<usize>]) -> usize {
        let o = data.buffer::<O>(0);
        ranges
            .iter()
            .map(|r| (o[r.end] - o[r.start]).as_usize())
            .sum()
    }
    let capacities = match values.data_type() {
        DataType::Utf8 | DataType::Binary => {
            Capacities::Binary(len, Some(bytes::<i32>(&data, ranges)))
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            Capacities::Binary(len, Some(bytes::<i64>(&data, ranges)))
        }
        _ => Capacities::Array(len),
    };
    let mut mutable = MutableArrayData::with_capacities(vec![&data], false, capacities);
    for r in ranges {
        mutable.try_extend(0, r.start, r.end)?;
    }
    minify_inner(make_array(mutable.freeze()).as_ref())
}

/// Returns the offsets of `ranges` of rows back to back, plus the child
/// ranges that the valid rows reference. Null rows get an empty range, as in
/// [`compact_offsets`], so that `minify` of the result gathers nothing again.
fn gather_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
    ranges: &[Range<usize>],
) -> (OffsetBuffer<O>, Vec<Range<usize>>) {
    let mut gathered = Vec::with_capacity(ranges.iter().map(|r| r.len()).sum::<usize>() + 1);
    gathered.push(O::zero());
    let mut child_ranges = vec![];
    let mut extend = |start: usize, end: usize, valid: bool| {
        let last = gathered[gathered.len() - 1];
        if valid {
            let base = offsets[start];
            gathered.extend(offsets[start + 1..=end].iter().map(|o| last + (*o - base)));
            push_range(&mut child_ranges, base.as_usize()..offsets[end].as_usize());
        } else {
            gathered.extend(std::iter::repeat_n(last, end - start));
        }
    };
    let nulls = nulls.filter(|n| n.null_count() > 0);
    for r in ranges {
        match nulls {
            None => extend(r.start, r.end, true),
            Some(nulls) => {
                let mut null_start = r.start;
                for (s, e) in nulls.inner().slice(r.start, r.len()).set_slices() {
                    extend(null_start, r.start + s, false);
                    extend(r.start + s, r.start + e, true);
                    null_start = r.start + e;
                }
                extend(null_start, r.end, false);
            }
        }
    }
    // SAFETY: the offsets start at zero and never decrease
    let offsets = unsafe { OffsetBuffer::new_unchecked(gathered.into()) };
    (offsets, child_ranges)
}

fn gather_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
    ranges: &[Range<usize>],
) -> Result<ArrayRef, ArrowError> {
    let len = ranges.iter().map(|r| r.len()).sum();
    let (offsets, child_ranges) = gather_offsets(array.offsets(), array.nulls(), ranges);
    let values = gather_ranges(array.values().as_ref(), &child_ranges)?;
    let nulls = gather_nulls(array.nulls(), ranges, len);
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        offsets,
        values,
        nulls,
    )?))
}

fn gather_run<R: RunEndIndexType>(
    array: &RunArray<R>,
    ranges: &[Range<usize>],
) -> Result<ArrayRef, ArrowError> {
    let mut run_ends = vec![];
    let mut value_ranges = vec![];
    let mut len = 0;
    for r in ranges.iter().filter(|r| !r.is_empty()) {
        let slice = array.slice(r.start, r.len());
        let ends = slice.run_ends();
        let (start, end) = (
            ends.get_start_physical_index(),
            ends.get_end_physical_index(),
        );
        let mut sliced = ends.sliced_values().map(|e| len + e.as_usize());
        // A range that starts in the run where the previous range ended
        // extends that run. Otherwise every range that hits a long run would
        // copy the run value again.
        if continues_run(&value_ranges, start) {
            let first = sliced.next().expect("non-empty range");
            *run_ends.last_mut().expect("previous run") = R::Native::usize_as(first);
            push_range(&mut value_ranges, start + 1..end + 1);
        } else {
            push_range(&mut value_ranges, start..end + 1);
        }
        run_ends.extend(sliced.map(R::Native::usize_as));
        len += r.len();
    }
    let values = gather_ranges(array.values().as_ref(), &value_ranges)?;
    let run_ends = PrimitiveArray::<R>::from_iter_values(run_ends);
    Ok(Arc::new(RunArray::<R>::try_new(
        &run_ends,
        values.as_ref(),
    )?))
}

/// Returns `true` if the last gathered value range ends with run `start`.
fn continues_run(value_ranges: &[Range<usize>], start: usize) -> bool {
    value_ranges.last().is_some_and(|v| v.end == start + 1)
}

fn gather_nulls(
    nulls: Option<&NullBuffer>,
    ranges: &[Range<usize>],
    len: usize,
) -> Option<NullBuffer> {
    let nulls = nulls.filter(|n| n.null_count() > 0)?;
    let mut builder = BooleanBufferBuilder::new(len);
    for r in ranges {
        builder.append_buffer(&nulls.inner().slice(r.start, r.len()));
    }
    Some(NullBuffer::new(builder.finish()))
}

/// Returns the dense union of `rows` of `array`. The result shares the
/// children of `array`.
fn select_dense_union(
    array: &UnionArray,
    fields: &UnionFields,
    rows: impl Iterator<Item = usize> + Clone,
) -> UnionArray {
    let offsets = array.offsets().expect("dense union has offsets");
    let type_ids = rows.clone().map(|i| array.type_ids()[i]).collect();
    let offsets = rows.map(|i| offsets[i]).collect();
    let children = fields
        .iter()
        .map(|(type_id, _)| Arc::clone(array.child(type_id)))
        .collect();
    // SAFETY: each row keeps its type id and its offset into the same child
    unsafe { UnionArray::new_unchecked(fields.clone(), type_ids, Some(offsets), children) }
}

/// Returns minified offsets and child for a list or map array.
fn minify_list_child<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    values: &dyn Array,
    nulls: Option<&NullBuffer>,
) -> Result<(OffsetBuffer<O>, ArrayRef), ArrowError> {
    match nulls_with_ranges(offsets, nulls) {
        None => {
            let (offsets, start, end) = rebase_offsets(offsets);
            Ok((offsets, minify_inner(&values.slice(start, end - start))?))
        }
        Some(nulls) => {
            let (offsets, ranges) = compact_offsets(offsets, nulls);
            Ok((offsets, gather_ranges(values, &ranges)?))
        }
    }
}

fn minify_primitive<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> PrimitiveArray<T> {
    PrimitiveArray::<T>::new(
        minify_scalar_buffer(array.values()),
        minify_nulls(array.nulls()),
    )
    .with_data_type(array.data_type().clone())
}

fn minify_bytes<T: ByteArrayType>(array: &GenericByteArray<T>) -> GenericByteArray<T> {
    let (offsets, values) = match nulls_with_ranges(array.offsets(), array.nulls()) {
        None => {
            let (offsets, start, end) = rebase_offsets(array.offsets());
            let values = minify_buffer(&array.values().slice_with_length(start, end - start));
            (offsets, values)
        }
        Some(nulls) => {
            let (offsets, ranges) = compact_offsets(array.offsets(), nulls);
            let bytes = array.values().as_slice();
            let mut values = Vec::with_capacity(offsets[offsets.len() - 1].as_usize());
            for r in ranges {
                values.extend_from_slice(&bytes[r]);
            }
            (offsets, Buffer::from_vec(values))
        }
    };
    // SAFETY: the offsets and values describe the same byte strings as before
    unsafe { GenericByteArray::new_unchecked(offsets, values, minify_nulls(array.nulls())) }
}

/// The out-of-line bytes that the valid rows of a view array reference.
struct ViewData {
    /// The disjoint, sorted ranges as `(buffer index, start, end)`. Bytes
    /// that several views share appear once.
    merged: Vec<(u32, usize, usize)>,
    /// The bytes in `merged` per data buffer
    referenced: Vec<usize>,
    /// `true` if a null row has an out-of-line view
    null_views: bool,
}

/// Merges the out-of-line ranges of the valid views among `rows` of `array`.
///
/// Null rows reference nothing. `minify` gives them an empty view, as it
/// gives null rows of a byte array an empty range.
fn view_data<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
    nulls: Option<&NullBuffer>,
    rows: impl Iterator<Item = usize>,
) -> ViewData {
    let mut ranges = vec![];
    let mut null_views = false;
    let views = array.views();
    for i in rows {
        let v = &views[i];
        let len = *v as u32;
        if len <= MAX_INLINE_VIEW_LEN {
            continue;
        }
        if nulls.is_some_and(|n| n.is_null(i)) {
            null_views = true;
            continue;
        }
        let view = ByteView::from(*v);
        // A data buffer can be longer than `u32::MAX`, so the end can overflow `u32`
        let start = view.offset as usize;
        ranges.push((view.buffer_index, start, start + len as usize));
    }
    // Builders and `gc` write views in order, so the sort is often not needed
    if !ranges.is_sorted() {
        ranges.sort_unstable();
    }
    let mut merged: Vec<(u32, usize, usize)> = vec![];
    for (b, s, e) in ranges {
        match merged.last_mut() {
            Some((mb, _, me)) if *mb == b && s <= *me => *me = (*me).max(e),
            _ => merged.push((b, s, e)),
        }
    }
    let mut referenced = vec![0; array.data_buffers().len()];
    for (b, s, e) in &merged {
        referenced[*b as usize] += e - s;
    }
    ViewData {
        merged,
        referenced,
        null_views,
    }
}

/// Returns `true` if `minify` keeps `buffer`, a data buffer whose valid
/// views reference `referenced` distinct bytes. The views must reference
/// every byte, and the allocation must not be oversized.
fn keeps_view_buffer(buffer: &Buffer, referenced: usize) -> bool {
    referenced > 0 && referenced == buffer.len() && !is_oversized(buffer)
}

/// Byte views can share bytes, appear out of order and leave gaps in the
/// data buffers.
///
/// `minify` keeps each data buffer whose bytes the valid views all reference.
/// It copies the referenced ranges of the other buffers back to back into new
/// buffers, and moves each view to the copy of its range. Views that share
/// bytes keep sharing them. Unreferenced buffers go away.
fn minify_byte_view<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
) -> GenericByteViewArray<T> {
    let nulls = minify_nulls(array.nulls());
    let ViewData {
        merged,
        referenced,
        null_views,
    } = view_data(array, nulls.as_ref(), 0..array.len());
    let old_buffers = array.data_buffers();
    let kept: Vec<bool> = old_buffers
        .iter()
        .zip(&referenced)
        .map(|(b, r)| keeps_view_buffer(b, *r))
        .collect();
    if !null_views && kept.iter().all(|k| *k) {
        let views = minify_scalar_buffer(array.views());
        // SAFETY: the views, data buffers and null bits stay the same
        return unsafe { GenericByteViewArray::new_unchecked(views, old_buffers.clone(), nulls) };
    }

    // The new index of each kept buffer
    let mut new_index = vec![0u32; old_buffers.len()];
    let mut buffers = vec![];
    for (i, b) in old_buffers.iter().enumerate().filter(|(i, _)| kept[*i]) {
        new_index[i] = buffers.len() as u32;
        buffers.push(b.clone());
    }
    // The new buffer index and offset of each copied range in `merged`
    let copied: Vec<usize> = (0..merged.len())
        .filter(|i| !kept[merged[*i].0 as usize])
        .collect();
    let mut moved = vec![(0u32, 0u32); merged.len()];
    let mut start = 0;
    while start < copied.len() {
        // A view offset is a `u32`. `gc` limits a buffer to `i32::MAX` bytes.
        let mut end = start;
        let mut len = 0usize;
        while end < copied.len() {
            let (_, s, e) = merged[copied[end]];
            let range_len = e - s;
            if end > start && len + range_len > i32::MAX as usize {
                break;
            }
            len += range_len;
            end += 1;
        }
        let mut bytes = Vec::with_capacity(len);
        for i in &copied[start..end] {
            let (b, s, e) = merged[*i];
            moved[*i] = (buffers.len() as u32, bytes.len() as u32);
            bytes.extend_from_slice(&old_buffers[b as usize][s..e]);
        }
        buffers.push(Buffer::from_vec(bytes));
        start = end;
    }

    let views = array
        .views()
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let len = *v as u32;
            if len <= MAX_INLINE_VIEW_LEN {
                return *v;
            }
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return 0;
            }
            let view = ByteView::from(*v);
            let (b, o) = (view.buffer_index, view.offset);
            if kept[b as usize] {
                return view.with_buffer_index(new_index[b as usize]).as_u128();
            }
            let r = merged.partition_point(|(mb, ms, _)| (*mb, *ms) <= (b, o as usize)) - 1;
            let (new_buffer, base) = moved[r];
            view.with_buffer_index(new_buffer)
                .with_offset(base + (o as usize - merged[r].1) as u32)
                .as_u128()
        })
        .collect();
    // SAFETY: every view of a valid row points at a copy of its bytes or at
    // its kept buffer, and null rows have an empty view
    unsafe { GenericByteViewArray::new_unchecked(views, buffers.into(), nulls) }
}

fn minify_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let (offsets, values) = minify_list_child(array.offsets(), array.values(), array.nulls())?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        offsets,
        values,
        minify_nulls(array.nulls()),
    )?))
}

/// The child rows that a set of list views references.
struct ListViewChild {
    /// The disjoint, sorted child ranges of the valid non-empty views
    merged: Vec<Range<usize>>,
    /// `true` if a null view has a non-zero offset or size
    null_views: bool,
    /// The largest offset of an empty valid view. Validation requires it to
    /// lie inside the child, so the child slice must not cut it off.
    empty_end: usize,
}

/// Merges the child ranges of `views`, given as `(valid, offset, size)`.
fn list_view_child(views: impl Iterator<Item = (bool, usize, usize)>) -> ListViewChild {
    let mut ranges = vec![];
    let mut null_views = false;
    let mut empty_end = 0;
    for (valid, offset, size) in views {
        if !valid {
            null_views |= offset != 0 || size != 0;
        } else if size == 0 {
            empty_end = empty_end.max(offset);
        } else {
            ranges.push(offset..offset + size);
        }
    }
    ranges.sort_unstable_by_key(|r| r.start);
    let mut merged: Vec<Range<usize>> = vec![];
    for r in ranges {
        match merged.last_mut() {
            Some(m) if r.start <= m.end => m.end = m.end.max(r.end),
            _ => merged.push(r),
        }
    }
    ListViewChild {
        merged,
        null_views,
        empty_end,
    }
}

/// List views can overlap, appear out of order and leave gaps in the child.
///
/// `minify` merges the child ranges of the valid views into disjoint ranges.
/// It copies these ranges back to back into a new child, and moves each view
/// by the size of the gaps before it. Overlapping views stay shared, and the
/// gaps go away. If the valid views cover one range, `minify` slices the
/// child to it instead of a copy.
///
/// Null rows and empty views get an offset and a size of zero.
fn minify_list_view<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let nulls = minify_nulls(array.nulls());
    let is_valid = |i: usize| nulls.as_ref().is_none_or(|n| n.is_valid(i));
    let views = || {
        array
            .offsets()
            .iter()
            .zip(array.sizes())
            .enumerate()
            .map(|(i, (o, s))| (is_valid(i), o.as_usize(), s.as_usize()))
    };

    let ListViewChild {
        merged,
        null_views,
        empty_end,
    } = list_view_child(views());

    // The offset in the new child of the first row of each merged range
    let mut bases = Vec::with_capacity(merged.len());
    let mut len = 0;
    for r in &merged {
        bases.push(len);
        len += r.len();
    }
    let moved = |offset: usize| {
        let i = merged.partition_point(|r| r.start <= offset) - 1;
        bases[i] + offset - merged[i].start
    };

    let (offsets, sizes, values) = match merged.as_slice() {
        [] | [_] => {
            let range = merged.first().cloned().unwrap_or(0..0);
            let values = minify_inner(&array.values().slice(range.start, range.len()))?;
            if range.start == 0 && !null_views && empty_end <= range.end {
                let offsets = minify_scalar_buffer(array.offsets());
                let sizes = minify_scalar_buffer(array.sizes());
                (offsets, sizes, values)
            } else {
                let (offsets, sizes) = views()
                    .map(|(valid, offset, size)| match valid && size > 0 {
                        true => (O::usize_as(offset - range.start), O::usize_as(size)),
                        false => (O::zero(), O::zero()),
                    })
                    .unzip::<_, _, Vec<_>, Vec<_>>();
                (offsets.into(), sizes.into(), values)
            }
        }
        _ => {
            let values = gather_ranges(array.values().as_ref(), &merged)?;
            let (offsets, sizes) = views()
                .map(|(valid, offset, size)| match valid && size > 0 {
                    true => (O::usize_as(moved(offset)), O::usize_as(size)),
                    false => (O::zero(), O::zero()),
                })
                .unzip::<_, _, Vec<_>, Vec<_>>();
            (offsets.into(), sizes.into(), values)
        }
    };
    // SAFETY: every view of a valid row lies inside `values`, and null rows
    // have an offset and a size of zero
    Ok(Arc::new(unsafe {
        GenericListViewArray::<O>::new_unchecked(Arc::clone(field), offsets, sizes, values, nulls)
    }))
}

/// Dense unions keep only the child rows that a row references, in order of
/// ascending order. Rows that reference the same child row keep sharing it. Sparse unions slice every child to the union's rows.
fn minify_union(
    array: &UnionArray,
    fields: &UnionFields,
    mode: UnionMode,
) -> Result<ArrayRef, ArrowError> {
    let type_ids = minify_scalar_buffer(array.type_ids());
    match mode {
        // `UnionArray::slice` already slices the children of a sparse union
        UnionMode::Sparse => {
            let children = fields
                .iter()
                .map(|(type_id, _)| minify_inner(array.child(type_id).as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            // SAFETY: the type ids and the child rows they reference stay the same
            Ok(Arc::new(unsafe {
                UnionArray::new_unchecked(fields.clone(), type_ids, None, children)
            }))
        }
        UnionMode::Dense => {
            let old_offsets = array.offsets().expect("dense union has offsets");
            let DenseUnionRows {
                offsets,
                mut indices,
                in_order,
            } = dense_union_rows(array, fields, 0..array.len());
            if in_order
                && fields
                    .iter()
                    .all(|(id, _)| indices[id as usize].len() == array.child(id).len())
            {
                let children = fields
                    .iter()
                    .map(|(type_id, _)| minify_inner(array.child(type_id).as_ref()))
                    .collect::<Result<Vec<_>, _>>()?;
                let offsets = Some(minify_scalar_buffer(old_offsets));
                // SAFETY: the type ids, offsets and child rows stay the same
                return Ok(Arc::new(unsafe {
                    UnionArray::new_unchecked(fields.clone(), type_ids, offsets, children)
                }));
            }
            let children = fields
                .iter()
                .map(|(type_id, _)| {
                    let child = array.child(type_id).as_ref();
                    // `take` copies shared rows of a nested dense union once
                    // per row, and merges equal runs of a run-end encoded
                    // array. `gather_ranges` does neither, so the result does
                    // not depend on the values.
                    let mut ranges = vec![];
                    for i in std::mem::take(&mut indices[type_id as usize]) {
                        push_range(&mut ranges, i as usize..i as usize + 1);
                    }
                    gather_ranges(child, &ranges)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let offsets = Some(ScalarBuffer::from(offsets));
            // SAFETY: each offset indexes the child row that `gather_ranges` kept for it
            Ok(Arc::new(unsafe {
                UnionArray::new_unchecked(fields.clone(), type_ids, offsets, children)
            }))
        }
    }
}

/// The child rows that a set of dense union rows references.
struct DenseUnionRows {
    /// The new offset of each row
    offsets: Vec<i32>,
    /// The referenced child rows of each type id, in ascending order
    indices: Vec<Vec<u32>>,
    /// `true` if every new offset equals the old offset
    in_order: bool,
}

/// Numbers the child rows that `rows` of `array` reference in ascending
/// order. Rows that share a child row keep sharing it.
fn dense_union_rows(
    array: &UnionArray,
    fields: &UnionFields,
    rows: impl Iterator<Item = usize> + Clone,
) -> DenseUnionRows {
    let type_ids = array.type_ids();
    let old_offsets = array.offsets().expect("dense union has offsets");
    // Type ids are in `0..128`, so a `Vec` can index them
    let mut indices: Vec<Vec<u32>> = vec![Vec::new(); 128];
    // The smallest and largest offset per type id. The lookup tables span
    // only these offsets, so the cost depends on the rows and not on the
    // child length.
    let mut bounds = vec![(i32::MAX, -1); 128];
    for i in rows.clone() {
        let (min, max) = &mut bounds[type_ids[i] as usize];
        *min = (*min).min(old_offsets[i]);
        *max = (*max).max(old_offsets[i]);
    }
    // Maps each child row in the bounds to its new offset, or -1 if no row
    // uses it yet
    let mut new_offsets: Vec<Vec<i32>> = vec![Vec::new(); 128];
    for (type_id, _) in fields.iter() {
        let (min, max) = bounds[type_id as usize];
        if max >= min {
            new_offsets[type_id as usize] = vec![-1; (max - min) as usize + 1];
        }
    }
    for i in rows.clone() {
        let (type_id, offset) = (type_ids[i] as usize, old_offsets[i]);
        new_offsets[type_id][(offset - bounds[type_id].0) as usize] = 0;
    }
    // Numbers the used child rows in ascending order. Gathered ranges then
    // never go back, so a run-end encoded child copies each run value once.
    for (type_id, _) in fields.iter() {
        let (t, min) = (type_id as usize, bounds[type_id as usize].0);
        for (j, new_offset) in new_offsets[t].iter_mut().enumerate() {
            if *new_offset == 0 {
                *new_offset = indices[t].len() as i32;
                indices[t].push((min as usize + j) as u32);
            }
        }
    }
    let mut in_order = true;
    let offsets = rows
        .map(|i| {
            let (type_id, offset) = (type_ids[i] as usize, old_offsets[i]);
            let new_offset = new_offsets[type_id][(offset - bounds[type_id].0) as usize];
            in_order &= new_offset == offset;
            new_offset
        })
        .collect();
    DenseUnionRows {
        offsets,
        indices,
        in_order,
    }
}

fn minify_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> Result<ArrayRef, ArrowError> {
    let array = garbage_collect_values(array)?;
    let keys = minify_primitive(array.keys());
    let values = minify_inner(array.values().as_ref())?;
    // SAFETY: `garbage_collect_dictionary` returns valid keys, and `minify`
    // keeps the keys and the length of the values
    Ok(Arc::new(unsafe {
        DictionaryArray::<K>::new_unchecked(keys, values)
    }))
}

/// Like [`garbage_collect_dictionary`], but copies the values with
/// [`gather_ranges`]. `gather_ranges` keeps child rows that the values share,
/// and does not merge equal runs of run-end encoded values as `take` does.
///
/// [`garbage_collect_dictionary`]: crate::dictionary::garbage_collect_dictionary
fn garbage_collect_values<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> Result<DictionaryArray<K>, ArrowError> {
    let (min, mask) = used_values(array.keys());
    if min == 0 && mask.len() == array.values().len() && mask.count_set_bits() == mask.len() {
        return Ok(array.clone());
    }
    // The span of used keys bounds the cost, not the length of the values
    let mut new_keys = vec![K::Native::ZERO; mask.len()];
    for (new_key, old_key) in mask.set_indices().enumerate() {
        new_keys[old_key] =
            K::Native::from_usize(new_key).ok_or(ArrowError::DictionaryKeyOverflowError)?;
    }
    // Null keys can hold any value
    let keys = array.keys().unary(|k| {
        new_keys
            .get(k.as_usize().wrapping_sub(min))
            .copied()
            .unwrap_or(K::Native::ZERO)
    });
    let ranges: Vec<_> = mask.set_slices().map(|(s, e)| s + min..e + min).collect();
    let values = gather_ranges(array.values().as_ref(), &ranges)?;
    DictionaryArray::try_new(keys, values)
}

/// Returns the smallest valid key, and a mask over the keys from the
/// smallest to the largest valid key with a bit for each used key.
fn used_values<K: ArrowDictionaryKeyType>(keys: &PrimitiveArray<K>) -> (usize, BooleanBuffer) {
    let valid = || {
        keys.values()
            .iter()
            .enumerate()
            .filter(|(i, _)| keys.is_valid(*i))
            .map(|(_, k)| k.as_usize())
    };
    let Some(min) = valid().min() else {
        return (0, BooleanBuffer::new_unset(0));
    };
    let max = valid().max().expect("a valid key");
    let mut used = BooleanBufferBuilder::new(max - min + 1);
    used.append_n(max - min + 1, false);
    valid().for_each(|k| used.set_bit(k - min, true));
    (min, used.finish())
}

fn minify_run<R: RunEndIndexType>(array: &RunArray<R>) -> Result<ArrayRef, ArrowError> {
    let run_ends = array.run_ends();
    let values = if run_ends.is_empty() {
        array.values().slice(0, 0)
    } else {
        let start = run_ends.get_start_physical_index();
        let end = run_ends.get_end_physical_index();
        array.values().slice(start, end - start + 1)
    };
    // The run ends are already minimal if the array uses all of them unchanged
    let run_ends = if run_ends.offset() == 0 && run_ends.len() == run_ends.max_value() {
        PrimitiveArray::<R>::new(minify_scalar_buffer(run_ends.inner()), None)
    } else {
        PrimitiveArray::<R>::from_iter_values(run_ends.sliced_values())
    };
    let values = minify_inner(values.as_ref())?;
    Ok(Arc::new(RunArray::<R>::try_new(
        &run_ends,
        values.as_ref(),
    )?))
}

/// Returns the bytes of the buffers that [`minify`] returns for `array`,
/// without a copy.
///
/// The count is the sum of the buffer lengths of the result, including child
/// arrays, dictionary values and null buffers. It is not the allocated
/// capacity: `minify` can reuse a buffer whose allocation is up to 63 bytes
/// larger than its length.
///
/// The count is also not the size of an IPC message. An IPC writer pads each
/// buffer, by default to a multiple of 64 bytes, and adds metadata. For
/// small arrays with many buffers the IPC body can be many times larger.
///
/// `compacted_size` makes the same decisions as `minify` about which rows,
/// child rows and buffers to keep, so the result is exact. The exception is
/// an array that references the same buffer more than once: `minify` can then
/// return the array unchanged (see the [module documentation](self)).
///
/// # Example
///
/// ```
/// # use arrow_array::{Array, Int32Array};
/// # use arrow_select::minify::{compacted_size, minify};
/// let array = Int32Array::from_iter_values(0..1024);
/// let sliced = array.slice(10, 4);
/// assert_eq!(compacted_size(&sliced).unwrap(), 16);
/// assert_eq!(minify(&sliced).unwrap().to_data().buffers()[0].len(), 16);
/// ```
///
/// # Errors
///
/// `compacted_size` returns `InvalidArgumentError` for a time type with an
/// invalid unit, a dictionary with an invalid key type and a run-end encoded
/// array with an invalid run end type.
pub fn compacted_size(array: &dyn Array) -> Result<usize, ArrowError> {
    let all = 0..array.len();
    gathered_size(array, std::slice::from_ref(&all))
}

/// Returns the bytes of the null buffer that `minify` keeps for `ranges` of
/// rows, gathered back to back into `len` rows.
fn gathered_null_size(nulls: Option<&NullBuffer>, ranges: &[Range<usize>], len: usize) -> usize {
    let Some(nulls) = nulls.filter(|n| n.null_count() > 0) else {
        return 0;
    };
    let has_nulls = match ranges {
        [r] if r.len() == nulls.len() => true,
        _ => ranges
            .iter()
            .any(|r| nulls.inner().slice(r.start, r.len()).count_set_bits() < r.len()),
    };
    if has_nulls { bit_util::ceil(len, 8) } else { 0 }
}

/// Calls `f` with the child ranges of the valid rows among `ranges` of rows.
fn valid_child_ranges<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
    ranges: &[Range<usize>],
    mut f: impl FnMut(Range<usize>),
) {
    let nulls = nulls.filter(|n| n.null_count() > 0);
    for r in ranges {
        match nulls {
            None => f(offsets[r.start].as_usize()..offsets[r.end].as_usize()),
            Some(nulls) => {
                for (s, e) in nulls.inner().slice(r.start, r.len()).set_slices() {
                    f(offsets[r.start + s].as_usize()..offsets[r.start + e].as_usize())
                }
            }
        }
    }
}

/// Returns the bytes of `minify` of `ranges` of rows of `array`, gathered
/// back to back. This models both `minify` of the whole array (one range)
/// and `minify` of the arrays that [`gather_ranges`] returns.
fn gathered_size(array: &dyn Array, ranges: &[Range<usize>]) -> Result<usize, ArrowError> {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let nulls = gathered_null_size(array.nulls(), ranges, len);
    let size = match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => bit_util::ceil(len, 8) + nulls,
        DataType::FixedSizeBinary(size) => len * *size as usize + nulls,
        DataType::Utf8 => bytes_size(array.as_bytes::<Utf8Type>(), ranges) + nulls,
        DataType::LargeUtf8 => bytes_size(array.as_bytes::<LargeUtf8Type>(), ranges) + nulls,
        DataType::Binary => bytes_size(array.as_bytes::<BinaryType>(), ranges) + nulls,
        DataType::LargeBinary => bytes_size(array.as_bytes::<LargeBinaryType>(), ranges) + nulls,
        DataType::Utf8View => view_size(array.as_byte_view::<StringViewType>(), ranges) + nulls,
        DataType::BinaryView => view_size(array.as_byte_view::<BinaryViewType>(), ranges) + nulls,
        DataType::List(_) => {
            let a = array.as_list::<i32>();
            list_size(a.offsets(), a.values().as_ref(), a.nulls(), ranges)? + nulls
        }
        DataType::LargeList(_) => {
            let a = array.as_list::<i64>();
            list_size(a.offsets(), a.values().as_ref(), a.nulls(), ranges)? + nulls
        }
        DataType::Map(_, _) => {
            let a = array.as_map();
            list_size(a.offsets(), a.entries(), a.nulls(), ranges)? + nulls
        }
        DataType::ListView(_) => list_view_size(array.as_list_view::<i32>(), ranges)? + nulls,
        DataType::LargeListView(_) => list_view_size(array.as_list_view::<i64>(), ranges)? + nulls,
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            let mut child_ranges = vec![];
            for r in ranges {
                push_range(&mut child_ranges, r.start * size..r.end * size);
            }
            let values = array.as_fixed_size_list().values();
            gathered_size(values.as_ref(), &child_ranges)? + nulls
        }
        DataType::Struct(_) => {
            let mut size = nulls;
            for c in array.as_struct().columns() {
                size += gathered_size(c.as_ref(), ranges)?;
            }
            size
        }
        DataType::Union(fields, UnionMode::Sparse) => {
            let array = array.as_union();
            let mut size = len;
            for (type_id, _) in fields.iter() {
                size += gathered_size(array.child(type_id).as_ref(), ranges)?;
            }
            size
        }
        DataType::Union(fields, UnionMode::Dense) => {
            let array = array.as_union();
            let rows = dense_union_rows(array, fields, ranges.iter().flat_map(|r| r.clone()));
            let mut size = len * (1 + 4);
            for (type_id, _) in fields.iter() {
                let mut child_ranges = vec![];
                for i in &rows.indices[type_id as usize] {
                    push_range(&mut child_ranges, *i as usize..*i as usize + 1);
                }
                size += gathered_size(array.child(type_id).as_ref(), &child_ranges)?;
            }
            size
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary_size(array.as_dictionary::<Int8Type>(), ranges)? + nulls,
            DataType::Int16 => dictionary_size(array.as_dictionary::<Int16Type>(), ranges)? + nulls,
            DataType::Int32 => dictionary_size(array.as_dictionary::<Int32Type>(), ranges)? + nulls,
            DataType::Int64 => dictionary_size(array.as_dictionary::<Int64Type>(), ranges)? + nulls,
            DataType::UInt8 => dictionary_size(array.as_dictionary::<UInt8Type>(), ranges)? + nulls,
            DataType::UInt16 => {
                dictionary_size(array.as_dictionary::<UInt16Type>(), ranges)? + nulls
            }
            DataType::UInt32 => {
                dictionary_size(array.as_dictionary::<UInt32Type>(), ranges)? + nulls
            }
            DataType::UInt64 => {
                dictionary_size(array.as_dictionary::<UInt64Type>(), ranges)? + nulls
            }
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "invalid dictionary key type {t}"
                )));
            }
        },
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run_size(array.as_run::<Int16Type>(), ranges)?,
            DataType::Int32 => run_size(array.as_run::<Int32Type>(), ranges)?,
            DataType::Int64 => run_size(array.as_run::<Int64Type>(), ranges)?,
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "invalid run end type {t}"
                )));
            }
        },
        t @ (DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond)) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "invalid time unit in {t}"
            )));
        }
        DataType::Int8 | DataType::UInt8 => len + nulls,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => len * 2 + nulls,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Decimal32(_, _)
        | DataType::Date32
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Interval(IntervalUnit::YearMonth) => len * 4 + nulls,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Decimal64(_, _)
        | DataType::Date64
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => len * 8 + nulls,
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => {
            len * 16 + nulls
        }
        DataType::Decimal256(_, _) => len * 32 + nulls,
    };
    Ok(size)
}

fn bytes_size<T: ByteArrayType>(array: &GenericByteArray<T>, ranges: &[Range<usize>]) -> usize {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let mut values = 0;
    valid_child_ranges(array.offsets(), array.nulls(), ranges, |r| {
        values += r.len()
    });
    (len + 1) * std::mem::size_of::<T::Offset>() + values
}

/// `minify` keeps a data buffer only if the valid views reference all of its
/// bytes, and copies the distinct referenced bytes of the other buffers. Both
/// cases hold exactly the distinct referenced bytes.
fn view_size<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
    ranges: &[Range<usize>],
) -> usize {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let rows = ranges.iter().flat_map(|r| r.clone());
    let data: usize = view_data(array, array.nulls(), rows)
        .referenced
        .iter()
        .sum();
    len * 16 + data
}

/// Returns the bytes of the offsets and the child of a list or map array.
fn list_size<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    values: &dyn Array,
    nulls: Option<&NullBuffer>,
    ranges: &[Range<usize>],
) -> Result<usize, ArrowError> {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let mut child_ranges = vec![];
    valid_child_ranges(offsets, nulls, ranges, |r| push_range(&mut child_ranges, r));
    Ok((len + 1) * O::get_byte_width() + gathered_size(values, &child_ranges)?)
}

fn list_view_size<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    ranges: &[Range<usize>],
) -> Result<usize, ArrowError> {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let (offsets, sizes) = (array.offsets(), array.sizes());
    let views = ranges.iter().flat_map(|r| r.clone()).map(|i| {
        (
            array.is_valid(i),
            offsets[i].as_usize(),
            sizes[i].as_usize(),
        )
    });
    let child = list_view_child(views);
    Ok(2 * len * O::get_byte_width() + gathered_size(array.values().as_ref(), &child.merged)?)
}

fn dictionary_size<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    ranges: &[Range<usize>],
) -> Result<usize, ArrowError> {
    let len: usize = ranges.iter().map(|r| r.len()).sum();
    let keys = array.keys();
    let valid = || {
        ranges
            .iter()
            .flat_map(|r| r.clone())
            .filter(|i| keys.is_valid(*i))
            .map(|i| keys.values()[i].as_usize())
    };
    // One mask over the span of the used keys, as in `garbage_collect_values`.
    // A mask per range would cost O(ranges * span).
    let value_ranges: Vec<_> = match (valid().min(), valid().max()) {
        (Some(min), Some(max)) => {
            let mut used = BooleanBufferBuilder::new(max - min + 1);
            used.append_n(max - min + 1, false);
            valid().for_each(|k| used.set_bit(k - min, true));
            let used = used.finish();
            used.set_slices().map(|(s, e)| s + min..e + min).collect()
        }
        _ => vec![],
    };
    let values = gathered_size(array.values().as_ref(), &value_ranges)?;
    Ok(len * K::Native::get_byte_width() + values)
}

fn run_size<R: RunEndIndexType>(
    array: &RunArray<R>,
    ranges: &[Range<usize>],
) -> Result<usize, ArrowError> {
    let mut runs = 0;
    let mut value_ranges = vec![];
    for r in ranges.iter().filter(|r| !r.is_empty()) {
        let slice = array.run_ends().slice(r.start, r.len());
        let (start, end) = (
            slice.get_start_physical_index(),
            slice.get_end_physical_index(),
        );
        // Mirrors `gather_run`: a range that continues the last run adds no run
        let first = start + continues_run(&value_ranges, start) as usize;
        runs += end + 1 - first;
        push_range(&mut value_ranges, first..end + 1);
    }
    let values = gathered_size(array.values().as_ref(), &value_ranges)?;
    Ok(runs * R::Native::get_byte_width() + values)
}

/// Adds the compacted bytes of each row of `array` to `sizes`.
///
/// `sizes` must have one entry per row. `row_sizes` adds to the entries, so a
/// caller can sum the row sizes of all columns of a batch into one slice.
///
/// # Attribution
///
/// `row_sizes` charges each row the bytes that it references:
///
/// | Part | Charge |
/// |------|--------|
/// | Fixed width value, offset, view, type id, union offset, dictionary key | Each row |
/// | Bytes of a byte array or a non-inline view | The row that references them |
/// | Null buffer and boolean values | 1 byte to each row whose index is a multiple of 8 |
/// | Child rows of a list, map or list view | The parent row, if it is valid |
/// | Child rows of a fixed size list | The parent row |
/// | Child row of a union | The union row |
/// | Dictionary value | Each row whose key references it |
/// | Run end and run value of a run-end encoded array | Each row of the run |
///
/// A null row of a byte, list or map array pays only its offset, because
/// `minify` empties its range. A null row of a byte view array pays only its
/// view, because `minify` gives it an empty view.
///
/// # Error bound
///
/// Let `s` be the sum of `sizes[a..b]` and `c` be
/// `compacted_size(&array.slice(a, b - a))`. For an array that shares no data
/// between rows, `s` and `c` differ only by these terms:
///
/// * `c` holds one more offset than its rows for each offset buffer.
/// * Each bitmap of `c` differs from its charge by at most one byte for each
///   contiguous run of rows it holds.
/// * `s` charges a bitmap that `minify` drops if the slice has no nulls.
///
/// Rows can share data: dictionary values, the runs of a run-end encoded
/// array, dense union child rows, the child rows of overlapping list views,
/// and the bytes of views that point to the same bytes. `row_sizes` charges
/// shared data to every row that references it, and `compacted_size` counts
/// it once. Then `s` can exceed `c` by the size of the shared data times the
/// number of extra references.
///
/// The additions saturate at `u64::MAX`.
///
/// # Errors
///
/// `row_sizes` returns `InvalidArgumentError` if `sizes` does not have one
/// entry per row, for a time type with an invalid unit, for a dictionary with
/// an invalid key type and for a run-end encoded array with an invalid run end
/// type.
pub fn row_sizes(array: &dyn Array, sizes: &mut [u64]) -> Result<(), ArrowError> {
    if sizes.len() != array.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "row_sizes needs {} entries, got {}",
            array.len(),
            sizes.len()
        )));
    }
    add_row_sizes(array, sizes)
}

/// Adds 1 byte to each row whose index is a multiple of 8.
fn add_bitmap(sizes: &mut [u64]) {
    sizes
        .iter_mut()
        .step_by(8)
        .for_each(|s| *s = s.saturating_add(1));
}

fn add_each(sizes: &mut [u64], size: usize) {
    sizes
        .iter_mut()
        .for_each(|s| *s = s.saturating_add(size as u64));
}

/// Returns the row sizes of `array`.
fn child_row_sizes(array: &dyn Array) -> Result<Vec<u64>, ArrowError> {
    let mut sizes = vec![0; array.len()];
    add_row_sizes(array, &mut sizes)?;
    Ok(sizes)
}

/// Returns the prefix sums of the row sizes of `array`, with a leading zero.
fn child_prefix_sizes(array: &dyn Array) -> Result<Vec<u64>, ArrowError> {
    let mut sizes = vec![0; array.len() + 1];
    add_row_sizes(array, &mut sizes[1..])?;
    for i in 1..sizes.len() {
        sizes[i] = sizes[i].saturating_add(sizes[i - 1]);
    }
    Ok(sizes)
}

fn add_row_sizes(array: &dyn Array, sizes: &mut [u64]) -> Result<(), ArrowError> {
    if array.nulls().is_some_and(|n| n.null_count() > 0) {
        add_bitmap(sizes);
    }
    match array.data_type() {
        DataType::Null => {}
        DataType::Boolean => add_bitmap(sizes),
        DataType::FixedSizeBinary(size) => add_each(sizes, *size as usize),
        DataType::Utf8 => add_bytes(array.as_bytes::<Utf8Type>(), sizes),
        DataType::LargeUtf8 => add_bytes(array.as_bytes::<LargeUtf8Type>(), sizes),
        DataType::Binary => add_bytes(array.as_bytes::<BinaryType>(), sizes),
        DataType::LargeBinary => add_bytes(array.as_bytes::<LargeBinaryType>(), sizes),
        DataType::Utf8View | DataType::BinaryView => {
            let data = array.to_data();
            let views = data.buffer::<u128>(0);
            for (i, (s, v)) in sizes.iter_mut().zip(views).enumerate() {
                let len = *v as u32;
                // `minify` gives a null row an empty view
                let bytes = if len > MAX_INLINE_VIEW_LEN && array.is_valid(i) {
                    len as u64
                } else {
                    0
                };
                *s = s.saturating_add(16 + bytes);
            }
        }
        DataType::List(_) => {
            let a = array.as_list::<i32>();
            add_list(a.offsets(), a.values().as_ref(), a.nulls(), sizes)?
        }
        DataType::LargeList(_) => {
            let a = array.as_list::<i64>();
            add_list(a.offsets(), a.values().as_ref(), a.nulls(), sizes)?
        }
        DataType::Map(_, _) => {
            let a = array.as_map();
            add_list(a.offsets(), a.entries(), a.nulls(), sizes)?
        }
        DataType::ListView(_) => add_list_view(array.as_list_view::<i32>(), sizes)?,
        DataType::LargeListView(_) => add_list_view(array.as_list_view::<i64>(), sizes)?,
        DataType::FixedSizeList(_, size) => {
            let a = array.as_fixed_size_list();
            let size = *size as usize;
            if size > 0 {
                let child = child_row_sizes(a.values().as_ref())?;
                // `minify` keeps the child rows of null rows
                for (s, c) in sizes.iter_mut().zip(child.chunks(size)) {
                    *s = c.iter().fold(*s, |a, b| a.saturating_add(*b));
                }
            }
        }
        DataType::Struct(_) => {
            for c in array.as_struct().columns() {
                add_row_sizes(c.as_ref(), sizes)?;
            }
        }
        DataType::Union(fields, UnionMode::Sparse) => {
            add_each(sizes, 1);
            let array = array.as_union();
            for (type_id, _) in fields.iter() {
                add_row_sizes(array.child(type_id).as_ref(), sizes)?;
            }
        }
        DataType::Union(fields, UnionMode::Dense) => {
            let array = array.as_union();
            let offsets = array.offsets().expect("dense union has offsets");
            // Only the child rows between the smallest and largest offset
            let mut bounds = vec![(usize::MAX, 0); 128];
            for (t, o) in array.type_ids().iter().zip(offsets) {
                let (min, max) = &mut bounds[*t as usize];
                *min = (*min).min(*o as usize);
                *max = (*max).max(*o as usize);
            }
            let mut children = vec![vec![]; 128];
            for (type_id, _) in fields.iter() {
                let (min, max) = bounds[type_id as usize];
                if min <= max {
                    let child = array.child(type_id).slice(min, max - min + 1);
                    children[type_id as usize] = child_row_sizes(child.as_ref())?;
                }
            }
            for ((s, t), o) in sizes.iter_mut().zip(array.type_ids()).zip(offsets) {
                let child = children[*t as usize][*o as usize - bounds[*t as usize].0];
                *s = s.saturating_add(1 + 4 + child);
            }
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => add_dictionary(array.as_dictionary::<Int8Type>(), sizes)?,
            DataType::Int16 => add_dictionary(array.as_dictionary::<Int16Type>(), sizes)?,
            DataType::Int32 => add_dictionary(array.as_dictionary::<Int32Type>(), sizes)?,
            DataType::Int64 => add_dictionary(array.as_dictionary::<Int64Type>(), sizes)?,
            DataType::UInt8 => add_dictionary(array.as_dictionary::<UInt8Type>(), sizes)?,
            DataType::UInt16 => add_dictionary(array.as_dictionary::<UInt16Type>(), sizes)?,
            DataType::UInt32 => add_dictionary(array.as_dictionary::<UInt32Type>(), sizes)?,
            DataType::UInt64 => add_dictionary(array.as_dictionary::<UInt64Type>(), sizes)?,
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "invalid dictionary key type {t}"
                )));
            }
        },
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => add_run(array.as_run::<Int16Type>(), sizes)?,
            DataType::Int32 => add_run(array.as_run::<Int32Type>(), sizes)?,
            DataType::Int64 => add_run(array.as_run::<Int64Type>(), sizes)?,
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "invalid run end type {t}"
                )));
            }
        },
        t @ (DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond)) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "invalid time unit in {t}"
            )));
        }
        DataType::Int8 | DataType::UInt8 => add_each(sizes, 1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => add_each(sizes, 2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Decimal32(_, _)
        | DataType::Date32
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Interval(IntervalUnit::YearMonth) => add_each(sizes, 4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Decimal64(_, _)
        | DataType::Date64
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => add_each(sizes, 8),
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => {
            add_each(sizes, 16)
        }
        DataType::Decimal256(_, _) => add_each(sizes, 32),
    }
    Ok(())
}

fn add_bytes<T: ByteArrayType>(array: &GenericByteArray<T>, sizes: &mut [u64]) {
    let width = std::mem::size_of::<T::Offset>() as u64;
    let offsets = array.offsets().windows(2);
    match array.nulls().filter(|n| n.null_count() > 0) {
        None => {
            for (s, o) in sizes.iter_mut().zip(offsets) {
                *s = s.saturating_add(width + (o[1] - o[0]).as_usize() as u64);
            }
        }
        Some(nulls) => {
            for ((s, o), valid) in sizes.iter_mut().zip(offsets).zip(nulls.iter()) {
                let bytes = if valid {
                    (o[1] - o[0]).as_usize() as u64
                } else {
                    0
                };
                *s = s.saturating_add(width + bytes);
            }
        }
    }
}

fn add_list<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    values: &dyn Array,
    nulls: Option<&NullBuffer>,
    sizes: &mut [u64],
) -> Result<(), ArrowError> {
    let start = offsets[0].as_usize();
    let end = offsets[offsets.len() - 1].as_usize();
    let child = child_prefix_sizes(&values.slice(start, end - start))?;
    let width = O::get_byte_width() as u64;
    for (i, (s, o)) in sizes.iter_mut().zip(offsets.windows(2)).enumerate() {
        *s = s.saturating_add(width);
        if nulls.is_none_or(|n| n.is_valid(i)) {
            *s = s.saturating_add(child[o[1].as_usize() - start] - child[o[0].as_usize() - start]);
        }
    }
    Ok(())
}

fn add_list_view<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    sizes: &mut [u64],
) -> Result<(), ArrowError> {
    let views = || {
        (0..array.len())
            .filter(|i| array.is_valid(*i) && !array.sizes()[*i].is_zero())
            .map(|i| {
                let offset = array.offsets()[i].as_usize();
                (i, offset, offset + array.sizes()[i].as_usize())
            })
    };
    let start = views().map(|(_, s, _)| s).min().unwrap_or(0);
    let end = views().map(|(_, _, e)| e).max().unwrap_or(0);
    let child = child_prefix_sizes(&array.values().slice(start, end - start))?;
    add_each(sizes, 2 * O::get_byte_width());
    for (i, s, e) in views() {
        sizes[i] = sizes[i].saturating_add(child[e - start] - child[s - start]);
    }
    Ok(())
}

fn add_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    sizes: &mut [u64],
) -> Result<(), ArrowError> {
    let keys = array.keys();
    // Only the span of used keys, so a slice of a large dictionary costs
    // O(slice + span) and not O(values)
    let (min, used) = used_values(keys);
    let values = child_row_sizes(&array.values().slice(min, used.len()))?;
    let width = K::Native::get_byte_width() as u64;
    for (i, (s, k)) in sizes.iter_mut().zip(keys.values()).enumerate() {
        *s = s.saturating_add(width);
        if keys.is_valid(i) {
            *s = s.saturating_add(values[k.as_usize() - min]);
        }
    }
    Ok(())
}

fn add_run<R: RunEndIndexType>(array: &RunArray<R>, sizes: &mut [u64]) -> Result<(), ArrowError> {
    let run_ends = array.run_ends();
    if run_ends.is_empty() {
        return Ok(());
    }
    let start = run_ends.get_start_physical_index();
    let end = run_ends.get_end_physical_index();
    let values = child_row_sizes(&array.values().slice(start, end - start + 1))?;
    let width = R::Native::get_byte_width() as u64;
    let mut run_start = 0;
    for (run_end, value) in run_ends.sliced_values().zip(values) {
        let run_end = run_end.as_usize();
        sizes[run_start..run_end]
            .iter_mut()
            .for_each(|s| *s = s.saturating_add(width.saturating_add(value)));
        run_start = run_end;
    }
    Ok(())
}

/// Splits rows with `sizes` into consecutive ranges whose sizes sum to at
/// most `max_size`.
///
/// A row larger than `max_size` gets a range of its own. Pass the result of
/// [`row_sizes`], summed over the columns of a batch, to split the batch.
///
/// The sizes are estimates, so [`compacted_size`] of a range is not a hard
/// cap (see the error bound of [`row_sizes`]):
///
/// * A range can compact to more than `max_size`. Each offset buffer holds
///   one offset more than its rows, and each bitmap can need one byte more
///   than its charge. This overhead grows with the number of offset buffers
///   and bitmaps in the schema, and matters most for small ranges.
/// * Shared data counts once per row that references it. A dictionary with
///   large values, a run-end encoded array with long runs, or views that
///   share bytes can then make every range one row long, although the whole
///   array compacts to less than `max_size`.
///
/// ```
/// # use arrow_select::minify::split_by_size;
/// assert_eq!(split_by_size(&[4, 4, 4, 9, 1], 8), vec![0..2, 2..3, 3..4, 4..5]);
/// ```
pub fn split_by_size(sizes: &[u64], max_size: u64) -> Vec<Range<usize>> {
    let mut ranges = vec![];
    let mut start = 0;
    let mut sum = 0u64;
    for (i, size) in sizes.iter().enumerate() {
        if i > start && sum.checked_add(*size).is_none_or(|s| s > max_size) {
            ranges.push(start..i);
            start = i;
            sum = 0;
        }
        sum = sum.saturating_add(*size);
    }
    if start < sizes.len() {
        ranges.push(start..sizes.len());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{
        BinaryViewBuilder, Int32Builder, ListBuilder, MapBuilder, StringBuilder,
        StringDictionaryBuilder, StringViewBuilder, make_view,
    };
    use arrow_array::types::{Int8Type, IntervalMonthDayNanoType};
    use arrow_buffer::IntervalMonthDayNano;
    use arrow_data::ArrayData;
    use arrow_schema::{Field, Fields, IntervalUnit, TimeUnit, UnionFields};

    const N: usize = 200;

    /// Sums the capacity of all distinct allocations, including children.
    fn allocated(data: &ArrayData) -> usize {
        allocated_memory(data).0
    }

    /// Collects the start pointer of every buffer, including children.
    fn pointers(data: &ArrayData, out: &mut Vec<*const u8>) {
        out.extend(data.buffers().iter().map(|b| b.as_ptr()));
        out.extend(data.nulls().map(|n| n.buffer().as_ptr()));
        data.child_data().iter().for_each(|c| pointers(c, out));
    }

    fn nulls_every(k: usize) -> NullBuffer {
        NullBuffer::from_iter((0..N).map(|i| i % k != 0))
    }

    /// Checks `minify` on several slices of `array`.
    fn check(array: ArrayRef) {
        let len = array.len();
        for (offset, slice_len) in [
            (0, len),
            (1, len - 2),
            (3, 4),
            (7, 1),
            (len / 2, len / 4),
            (len, 0),
        ] {
            let sliced = array.slice(offset, slice_len);
            let minified = minify(sliced.as_ref()).unwrap();
            let data = minified.to_data();
            data.validate_full().unwrap();
            assert_eq!(data, sliced.to_data(), "{offset} {slice_len}");
            assert_eq!(data.offset(), 0);
            assert_eq!(minified.data_type(), sliced.data_type());

            let before = allocated(&sliced.to_data());
            let after = allocated(&data);
            assert!(after <= before, "{after} > {before}");
            if slice_len <= len / 4 && before > 0 {
                assert!(after < before, "{offset} {slice_len}: {after} >= {before}");
            }

            // A second call copies nothing
            let again = minify(minified.as_ref()).unwrap();
            let (mut a, mut b) = (vec![], vec![]);
            pointers(&data, &mut a);
            pointers(&again.to_data(), &mut b);
            assert_eq!(a, b, "{offset} {slice_len}");
        }
    }

    fn int32s() -> ArrayRef {
        Arc::new(Int32Array::new(
            (0..N as i32).collect(),
            Some(nulls_every(3)),
        ))
    }

    fn strings() -> ArrayRef {
        Arc::new(StringArray::from_iter(
            (0..N).map(|i| (i % 4 != 0).then(|| format!("value-{i}"))),
        ))
    }

    fn string_views() -> ArrayRef {
        let mut b = StringViewBuilder::new().with_fixed_block_size(1024);
        for i in 0..N {
            match i % 3 {
                0 => b.append_null(),
                1 => b.append_value(format!("s{i}")),
                _ => b.append_value(format!("a string longer than twelve bytes {i}")),
            }
        }
        Arc::new(b.finish())
    }

    fn list_of_int32() -> ArrayRef {
        let mut b = ListBuilder::new(Int32Builder::new());
        for i in 0..N {
            if i % 5 == 0 {
                b.append_null();
            } else {
                b.append_value((0..(i % 4) as i32).map(Some));
            }
        }
        Arc::new(b.finish())
    }

    fn string_dictionary() -> ArrayRef {
        let mut b = StringDictionaryBuilder::<Int16Type>::new();
        for i in 0..N {
            if i % 7 == 0 {
                b.append_null();
            } else {
                b.append_value(format!("dict-{}", i % 50));
            }
        }
        Arc::new(b.finish())
    }

    #[test]
    fn test_primitive_and_null() {
        check(int32s());
        check(Arc::new(Int8Array::from_iter_values(
            (0..N).map(|i| i as i8),
        )));
        check(Arc::new(NullArray::new(N)));
    }

    #[test]
    fn test_decimal_temporal_interval() {
        check(Arc::new(
            Decimal128Array::from_iter_values((0..N).map(|i| i as i128))
                .with_precision_and_scale(20, 3)
                .unwrap(),
        ));
        check(Arc::new(
            TimestampNanosecondArray::from_iter_values((0..N).map(|i| i as i64))
                .with_timezone("+02:00"),
        ));
        check(Arc::new(Date32Array::from_iter_values(0..N as i32)));
        check(Arc::new(
            PrimitiveArray::<IntervalMonthDayNanoType>::from_iter_values(
                (0..N).map(|i| IntervalMonthDayNano::new(i as i32, 1, 2)),
            ),
        ));
        let array = minify(&TimestampNanosecondArray::from(vec![1]).with_timezone("UTC")).unwrap();
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            minify(&IntervalDayTimeArray::from(vec![None]))
                .unwrap()
                .data_type(),
            &DataType::Interval(IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_boolean() {
        let array = BooleanArray::new(
            BooleanBuffer::from_iter((0..N).map(|i| i % 3 == 0)),
            Some(nulls_every(5)),
        );
        check(Arc::new(array.clone()));
        // A bit offset that is not a multiple of 8
        let minified = minify(&array.slice(3, 10)).unwrap();
        assert_eq!(minified.as_boolean().values().offset(), 0);
        assert_eq!(minified.as_boolean().nulls().unwrap().offset(), 0);
    }

    #[test]
    fn test_nulls_without_nulls_are_dropped() {
        let array = Int32Array::new((0..N as i32).collect(), Some(nulls_every(100)));
        let minified = minify(&array.slice(1, 50)).unwrap();
        assert!(minified.nulls().is_none());
    }

    #[test]
    fn test_bytes() {
        check(strings());
        check(Arc::new(LargeStringArray::from_iter_values(
            (0..N).map(|i| format!("v{i}")),
        )));
        check(Arc::new(BinaryArray::from_iter_values(
            (0..N).map(|i| vec![i as u8; i % 5]),
        )));
        check(Arc::new(LargeBinaryArray::from_iter(
            (0..N).map(|i| (i % 2 == 0).then(|| vec![i as u8; 3])),
        )));
        check(Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                (0..N).map(|i| (i % 3 != 0).then_some([i as u8; 5])),
                5,
            )
            .unwrap(),
        ));

        let minified = minify(&strings().slice(10, 3)).unwrap();
        let minified = minified.as_string::<i32>();
        assert_eq!(minified.value_offsets()[0], 0);
        assert_eq!(minified.values().len(), "value-10value-11".len());
    }

    #[test]
    fn test_byte_views() {
        check(string_views());
        let inline_only = BinaryViewArray::from_iter_values((0..N).map(|i| vec![i as u8; i % 12]));
        check(Arc::new(inline_only));

        // A slice with only inline values keeps no data buffers
        let minified = minify(&string_views().slice(1, 1)).unwrap();
        assert!(minified.as_string_view().data_buffers().is_empty());

        let mut b = BinaryViewBuilder::new();
        b.append_value([7u8; 100]);
        b.append_value([8u8; 2]);
        let minified = minify(&b.finish().slice(1, 1)).unwrap();
        assert!(minified.as_binary_view().data_buffers().is_empty());
    }

    /// Builds byte views over `buffers` from `(buffer, offset, len)` triples.
    fn byte_views(buffers: Vec<Buffer>, views: &[(u32, u32, u32)]) -> BinaryViewArray {
        let views = views
            .iter()
            .map(|(b, o, l)| {
                let bytes = &buffers[*b as usize][*o as usize..(*o + *l) as usize];
                make_view(bytes, *b, *o)
            })
            .collect();
        BinaryViewArray::new(views, Arc::from(buffers), None)
    }

    #[test]
    fn test_byte_views_inline_values_drop_data_buffers() {
        // Values of exactly 12 bytes are inline, so the data buffer is unused
        let buffer = Buffer::from_vec(vec![b'x'; 100]);
        let array = byte_views(vec![buffer], &vec![(0, 0, 12); N]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        assert!(minified.as_binary_view().data_buffers().is_empty());
    }

    #[test]
    fn test_byte_views_copy_mostly_unused_buffer() {
        // One view references 13 of 500 bytes
        let array = byte_views(vec![Buffer::from_vec(vec![b'x'; 500])], &[(0, 7, 13)]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let buffers = minified.as_binary_view().data_buffers();
        assert_eq!(buffers.iter().map(|b| b.len()).sum::<usize>(), 13);
    }

    #[test]
    fn test_byte_views_copy_partly_used_buffers() {
        // Each buffer holds 50 unreferenced bytes. The referenced bytes of
        // both buffers move into one new buffer.
        let buffers = vec![
            Buffer::from_vec(vec![b'x'; 63]),
            Buffer::from_vec(vec![b'y'; 63]),
        ];
        let array = byte_views(buffers, &[(0, 0, 13), (1, 50, 13)]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let buffers = minified.as_binary_view().data_buffers();
        assert_eq!(buffers.len(), 1);
        assert_eq!(buffers[0].as_slice(), [[b'x'; 13], [b'y'; 13]].concat());
    }

    #[test]
    fn test_byte_views_reuse_fully_used_buffers() {
        let buffers = vec![
            Buffer::from_vec(vec![b'x'; 13]),
            Buffer::from_vec(vec![b'y'; 26]),
        ];
        let array = byte_views(buffers, &[(0, 0, 13), (1, 13, 13), (1, 0, 13)]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let (mut a, mut b) = (vec![], vec![]);
        pointers(&array.to_data(), &mut a);
        pointers(&minified.to_data(), &mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn test_byte_views_repeated_views_copy_bytes_once() {
        // `take` with repeated indices repeats views over a 1 MB buffer
        let mut builder = StringViewBuilder::new();
        for i in 0..10_000 {
            builder.append_value(format!("{i:0100}"));
        }
        let array = builder.finish();
        for (indices, distinct) in [
            (vec![0u32; 10_000], 1),
            ((0..10_000).map(|i| i % 2).collect(), 2),
            (vec![0u32; 5_000], 1),
        ] {
            let taken = take(&array, &UInt32Array::from(indices.clone()), None).unwrap();
            let minified = minify(&taken).unwrap();
            assert_eq!(minified.to_data(), taken.to_data());
            let minified = minified.as_string_view();
            let buffers: Vec<usize> = minified.data_buffers().iter().map(|b| b.len()).collect();
            assert_eq!(buffers, [100 * distinct]);
            assert_eq!(minified.views().inner().capacity(), 16 * indices.len());
        }
    }

    #[test]
    fn test_byte_views_null_rows_reference_nothing() {
        let mut builder = StringViewBuilder::new();
        for i in 0..10_000 {
            builder.append_value(format!("{i:0100}"));
        }
        let array = builder.finish();
        let mask = BooleanArray::from_iter((0..10_000).map(|i| Some(i != 0)));
        let array = crate::nullif::nullif(&array, &mask).unwrap();
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let minified = minified.as_string_view();
        let buffers: Vec<usize> = minified.data_buffers().iter().map(|b| b.len()).collect();
        assert_eq!(buffers, [100]);
        assert_eq!(minified.value(0), format!("{:0100}", 0));
        assert!(minified.views()[1..].iter().all(|v| *v == 0));
    }

    #[test]
    fn test_byte_views_drop_unreferenced_small_buffers() {
        // 10000 buffers of 60 bytes, and the slice references 13 bytes of one
        let buffers: Vec<Buffer> = (0..10_000)
            .map(|_| Buffer::from_vec(vec![b'x'; 60]))
            .collect();
        let views: Vec<(u32, u32, u32)> = (0..10_000).map(|b| (b, 0, 13)).collect();
        let array = byte_views(buffers, &views);
        let minified = minify(&array.slice(0, 1)).unwrap();
        let buffers: Vec<usize> = minified
            .as_binary_view()
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .collect();
        assert_eq!(buffers, [13]);
    }

    #[test]
    fn test_aliased_struct_columns() {
        let s: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..10_000).map(|i| format!("{i:0100}")),
        ));
        let field = |name| Arc::new(Field::new(name, DataType::Utf8, false));
        let before_bytes = allocated(&s.to_data());

        // Equal columns share one minified column
        let array = StructArray::from(vec![(field("x"), s.clone()), (field("y"), s.clone())])
            .slice(1, 9_999);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let columns = minified.as_struct().columns();
        assert!(Arc::ptr_eq(&columns[0], &columns[1]));
        // 999900 value bytes rounded up to 64, and 10000 offsets
        assert_eq!(allocated(&minified.to_data()), 999_936 + 40_000);

        // Different slices of the same buffer are not copied twice
        let array = StructArray::from(vec![
            (field("x"), s.slice(0, 9_000)),
            (field("y"), s.slice(1_000, 9_000)),
        ]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        assert_eq!(allocated(&minified.to_data()), before_bytes);
    }

    #[test]
    fn test_aliased_dictionary_values() {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..10_000).map(|i| format!("{i:0100}")),
        ));
        let keys =
            |skip: i32| Int32Array::from_iter_values((0..10_000).filter(move |k| *k != skip));
        let field = |name| {
            Arc::new(Field::new_dictionary(
                name,
                DataType::Int32,
                DataType::Utf8,
                false,
            ))
        };
        let array = StructArray::from(vec![
            (
                field("x"),
                Arc::new(DictionaryArray::new(keys(0), values.clone())) as ArrayRef,
            ),
            (
                field("y"),
                Arc::new(DictionaryArray::new(keys(1), values.clone())) as ArrayRef,
            ),
        ]);
        let before = allocated(&array.to_data());
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        assert_eq!(allocated(&minified.to_data()), before);
    }

    #[test]
    fn test_dense_union_table_spans_visible_offsets() {
        let n = 1_000_000;
        let child: ArrayRef = Arc::new(Int8Array::from(vec![7i8; n]));
        let fields = UnionFields::try_new([0], [Field::new("a", DataType::Int8, false)]).unwrap();
        let offsets: Vec<i32> = (0..n as i32).collect();
        let array = UnionArray::try_new(
            fields.clone(),
            vec![0i8; n].into(),
            Some(offsets.into()),
            vec![child],
        )
        .unwrap();
        let rows = dense_union_rows(&array, &fields, [6, 5, 6].into_iter());
        assert_eq!(rows.offsets, [1, 0, 1]);
        assert_eq!(rows.indices[0], [5, 6]);
        let minified = minify(&array.slice(5, 1)).unwrap();
        let minified = minified.as_union();
        assert_eq!(minified.child(0).as_primitive::<Int8Type>().values(), &[7]);
    }

    #[test]
    fn test_lists() {
        check(list_of_int32());
        let large = ListBuilder::new(StringBuilder::new());
        let mut large = large;
        for i in 0..N {
            large.append_value([Some(format!("x{i}")), None]);
        }
        let large = large.finish();
        let (field, offsets, values, nulls) = large.into_parts();
        let offsets = OffsetBuffer::new(offsets.iter().map(|o| *o as i64).collect());
        check(Arc::new(LargeListArray::new(field, offsets, values, nulls)));

        let minified = minify(&list_of_int32().slice(1, 2)).unwrap();
        // Rows 1 and 2 have 1 and 2 values
        assert_eq!(minified.as_list::<i32>().values().len(), 3);
    }

    #[test]
    fn test_map() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for i in 0..N {
            if i % 6 == 0 {
                b.append(false).unwrap();
                continue;
            }
            for j in 0..i % 3 {
                b.keys().append_value(format!("k{j}"));
                b.values().append_value(j as i32);
            }
            b.append(true).unwrap();
        }
        check(Arc::new(b.finish()));
    }

    /// Builds list views in reverse order that overlap and leave gaps.
    fn list_view<O: OffsetSizeTrait>() -> GenericListViewArray<O> {
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let values = strings();
        let offsets = (0..N).rev().map(|i| O::usize_as(i % (N - 5))).collect();
        let sizes = (0..N).map(|i| O::usize_as(i % 4)).collect();
        GenericListViewArray::new(field, offsets, sizes, values, Some(nulls_every(4)))
    }

    #[test]
    fn test_list_views() {
        check(Arc::new(list_view::<i32>()));
        check(Arc::new(list_view::<i64>()));

        let array = list_view::<i32>();
        let minified = minify(&array.slice(5, 3)).unwrap();
        let minified = minified.as_list_view::<i32>();
        // Rows 5, 6 and 7 overlap within child rows 192 to 194
        assert_eq!(minified.values().len(), 3);
        assert_eq!(minified.offsets().as_ref(), &[2, 1, 0]);
        assert_eq!(minified.sizes().as_ref(), &[1, 2, 3]);

        // The child ranges of views with a gap between them are copied back
        // to back, in child order
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let gaps = ListViewArray::new(
            field,
            vec![150, 0].into(),
            vec![2, 2].into(),
            strings(),
            None,
        );
        let minified = minify(&gaps).unwrap();
        let minified = minified.as_list_view::<i32>();
        assert_eq!(minified.values().len(), 4);
        assert_eq!(minified.offsets().as_ref(), &[2, 0]);
        assert_eq!(minified.to_data(), gaps.to_data());
    }

    #[test]
    fn test_fixed_size_list_of_struct() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let n = N * 2;
        let a: ArrayRef = Arc::new(Int32Array::from_iter_values(0..n as i32));
        let b: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("{i}")),
        ));
        let structs = StructArray::new(fields.clone(), vec![a, b], None);
        let field = Arc::new(Field::new_struct("item", fields, true));
        let array = FixedSizeListArray::new(field, 2, Arc::new(structs), Some(nulls_every(3)));
        check(Arc::new(array));
    }

    #[test]
    fn test_struct() {
        let fields = Fields::from(vec![
            Field::new("dict", string_dictionary().data_type().clone(), true),
            Field::new("list", list_of_int32().data_type().clone(), true),
            Field::new("view", DataType::Utf8View, true),
        ]);
        let array = StructArray::new(
            fields,
            vec![string_dictionary(), list_of_int32(), string_views()],
            Some(nulls_every(9)),
        );
        check(Arc::new(array));
        check(Arc::new(StructArray::new_empty_fields(N, None)));
    }

    fn union_fields() -> UnionFields {
        UnionFields::try_new(
            [0, 3],
            [
                Field::new("int", DataType::Int32, true),
                Field::new("list", list_of_int32().data_type().clone(), true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sparse_union() {
        let type_ids = (0..N).map(|i| if i % 3 == 0 { 3 } else { 0 }).collect();
        let array = UnionArray::try_new(
            union_fields(),
            type_ids,
            None,
            vec![int32s(), list_of_int32()],
        )
        .unwrap();
        check(Arc::new(array));
    }

    #[test]
    fn test_dense_union() {
        let type_ids: ScalarBuffer<i8> = (0..N).map(|i| if i % 3 == 0 { 3 } else { 0 }).collect();
        // Reference the children out of order and more than once
        let offsets = (0..N).map(|i| ((N - 1 - i) / 2) as i32).collect();
        let array = UnionArray::try_new(
            union_fields(),
            type_ids,
            Some(offsets),
            vec![int32s(), list_of_int32()],
        )
        .unwrap();
        check(Arc::new(array.clone()));

        let minified = minify(&array.slice(0, 4)).unwrap();
        let minified = minified.as_union();
        assert_eq!(minified.child(0).len(), 2);
        assert_eq!(minified.child(3).len(), 2);
        assert_eq!(minified.offsets().unwrap().as_ref(), &[1, 1, 0, 0]);
    }

    #[test]
    fn test_dense_union_child_order() {
        let fields = UnionFields::try_new([0], [Field::new("int", DataType::Int32, true)]).unwrap();
        let child = || Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;

        // Every child row is used, out of order. The child keeps its order,
        // so `minify` reuses the array.
        let array = UnionArray::try_new(
            fields.clone(),
            vec![0i8; 3].into(),
            Some(vec![2, 0, 1].into()),
            vec![child()],
        )
        .unwrap();
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let minified = minified.as_union();
        assert_eq!(
            minified.child(0).as_primitive::<Int32Type>().values(),
            &[10, 20, 30]
        );
        assert_eq!(minified.offsets().unwrap().as_ref(), &[2, 0, 1]);

        // The rows use a prefix of the child in order. The unused row goes away.
        let array = UnionArray::try_new(
            fields,
            vec![0i8; 2].into(),
            Some(vec![0, 1].into()),
            vec![child()],
        )
        .unwrap();
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let minified = minified.as_union();
        assert_eq!(
            minified.child(0).as_primitive::<Int32Type>().values(),
            &[10, 20]
        );
    }

    #[test]
    fn test_run_end_encoded() {
        let run_ends = Int32Array::from_iter_values((1..=N as i32 / 4).map(|i| i * 4));
        let values = strings().slice(0, N / 4);
        let array = RunArray::try_new(&run_ends, values.as_ref()).unwrap();
        check(Arc::new(array.clone()));

        let minified = minify(&array.slice(6, 5)).unwrap();
        let minified = minified.as_run::<Int32Type>();
        // Rows 6 to 10 cover runs 1 and 2
        assert_eq!(minified.values().len(), 2);
        assert_eq!(minified.run_ends().values(), &[2, 5]);

        let run_ends = Int16Array::from_iter_values((1..=N as i16 / 2).map(|i| i * 2));
        let values = string_dictionary().slice(0, N / 2);
        check(Arc::new(
            RunArray::try_new(&run_ends, values.as_ref()).unwrap(),
        ));
        let run_ends = Int64Array::from_iter_values(1..=N as i64);
        check(Arc::new(
            RunArray::try_new(&run_ends, int32s().as_ref()).unwrap(),
        ));
    }

    #[test]
    fn test_dictionary() {
        check(string_dictionary());

        let minified = minify(&string_dictionary().slice(1, 3)).unwrap();
        assert_eq!(minified.as_dictionary::<Int16Type>().values().len(), 3);

        // Dictionary of string views and of lists
        let views = string_views();
        let keys = Int8Array::from_iter((0..N).map(|i| (i % 11 != 0).then_some((i % 60) as i8)));
        check(Arc::new(
            DictionaryArray::<Int8Type>::try_new(keys, views).unwrap(),
        ));
        let keys = Int32Array::from_iter_values((0..N as i32).rev());
        check(Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, list_of_int32()).unwrap(),
        ));
    }

    #[test]
    fn test_list_of_dictionary() {
        let dict = string_dictionary();
        let field = Arc::new(Field::new_list_field(dict.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths((0..N / 4).map(|i| i % 5));
        let len = offsets[offsets.len() - 1] as usize;
        let list = ListArray::new(field, offsets, dict.slice(0, len), None);
        check(Arc::new(list));
    }

    #[test]
    fn test_sliced_children() {
        // Children that are themselves slices of larger arrays
        let child = list_of_int32().slice(20, N - 40);
        let field = Arc::new(Field::new_list_field(child.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths((0..(N - 40) / 2).map(|_| 2));
        check(Arc::new(ListArray::new(field, offsets, child, None)));
    }

    /// Returns the bytes `minified` keeps for the values of a byte array.
    fn byte_values_len(minified: &ArrayRef) -> usize {
        minified.as_string::<i32>().values().len()
    }

    /// Builds strings where null rows keep the non-empty range `"hidden"`.
    fn strings_with_null_ranges() -> StringArray {
        let valid = StringArray::from_iter_values((0..N).map(|_| "hidden"));
        let nulls = nulls_every(3);
        let (offsets, values, _) = valid.into_parts();
        StringArray::new(offsets, values, Some(nulls))
    }

    #[test]
    fn test_null_ranges_are_emptied() {
        let array = strings_with_null_ranges();
        assert_eq!(array.values().len(), 6 * N);
        let minified = minify(&array).unwrap();
        minified.to_data().validate_full().unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        // Rows 0, 3, 6, ... are null and keep no bytes
        let valid = N - N.div_ceil(3);
        assert_eq!(byte_values_len(&minified), 6 * valid);
        let s = minified.as_string::<i32>();
        assert!(
            (0..N)
                .filter(|i| s.is_null(*i))
                .all(|i| s.value_length(i) == 0)
        );
        check(Arc::new(array));

        let (offsets, values, nulls) = strings_with_null_ranges().into_parts();
        let large = LargeBinaryArray::new(
            OffsetBuffer::new(offsets.iter().map(|o| *o as i64).collect()),
            values,
            nulls,
        );
        let minified = minify(&large).unwrap();
        assert_eq!(minified.as_binary::<i64>().values().len(), 6 * valid);
        check(Arc::new(large));
    }

    #[test]
    fn test_no_nulls_stays_on_fast_path() {
        // Exact-size buffers, so the fast path has nothing to copy
        let offsets = OffsetBuffer::from_lengths((0..N).map(|_| 3));
        let array = StringArray::new(offsets, Buffer::from_vec(vec![b'a'; 3 * N]), None);
        let minified = minify(&array).unwrap();
        // The fast path reuses the minimal value and offset buffers
        let s = minified.as_string::<i32>();
        assert_eq!(s.values().as_ptr(), array.values().as_ptr());
        assert_eq!(
            s.offsets().inner().inner().as_ptr(),
            array.offsets().inner().inner().as_ptr()
        );

        // Null rows that already have empty ranges also stay on the fast path
        let with_nulls = minify(&strings()).unwrap();
        let minified = minify(&with_nulls).unwrap();
        let s = minified.as_string::<i32>();
        assert_eq!(
            s.values().as_ptr(),
            with_nulls.as_string::<i32>().values().as_ptr()
        );
    }

    /// Builds a list of strings with nulls at both levels. Null list rows and
    /// null strings keep non-empty ranges.
    fn nested_list_with_null_ranges() -> ListArray {
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let offsets = OffsetBuffer::from_lengths((0..N / 2).map(|_| 2));
        ListArray::new(
            field,
            offsets,
            Arc::new(strings_with_null_ranges()),
            Some(NullBuffer::from_iter((0..N / 2).map(|i| i % 4 != 1))),
        )
    }

    #[test]
    fn test_nested_null_ranges() {
        let list = nested_list_with_null_ranges();
        let minified = minify(&list).unwrap();
        minified.to_data().validate_full().unwrap();
        assert_eq!(minified.to_data(), list.to_data());
        let m = minified.as_list::<i32>();
        // One in four list rows is null, so the child keeps 3 of 4 pairs
        assert_eq!(m.values().len(), 2 * (N / 2 - N / 8));
        assert!(
            (0..m.len())
                .filter(|i| m.is_null(*i))
                .all(|i| m.value_length(i) == 0)
        );
        let child = m.values().as_string::<i32>();
        assert!(
            (0..child.len())
                .filter(|i| child.is_null(*i))
                .all(|i| child.value_length(i) == 0)
        );
        check(Arc::new(list.clone()));

        // Dictionary of list
        let keys = Int32Array::from_iter_values((0..N as i32 / 2).rev());
        check(Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(list.clone())).unwrap(),
        ));

        // Struct of list
        let field = Arc::new(Field::new("l", list.data_type().clone(), true));
        let structs = StructArray::new(
            Fields::from(vec![field]),
            vec![Arc::new(list) as ArrayRef],
            Some(NullBuffer::from_iter((0..N / 2).map(|i| i % 5 != 0))),
        );
        check(Arc::new(structs));
    }

    #[test]
    fn test_map_null_ranges() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for i in 0..N {
            b.keys().append_value(format!("k{i}"));
            b.values().append_value(i as i32);
            b.append(true).unwrap();
        }
        let (field, offsets, entries, _, ordered) = b.finish().into_parts();
        let nulls = NullBuffer::from_iter((0..N).map(|i| i % 2 == 0));
        let map = MapArray::new(field, offsets, entries, Some(nulls), ordered);
        let minified = minify(&map).unwrap();
        assert_eq!(minified.as_map().entries().len(), N / 2);
        check(Arc::new(map));
    }

    #[test]
    fn test_list_view_empty_view_past_child() {
        // Validation requires an empty valid view to point inside the child
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array = ListViewArray::new(field, vec![0, 3].into(), vec![1, 0].into(), values, None);
        let minified = minify(&array).unwrap();
        minified.to_data().validate_full().unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let minified = minified.as_list_view::<i32>();
        assert_eq!(minified.values().len(), 1);
        assert_eq!(minified.offsets().as_ref(), &[0, 0]);
    }

    #[test]
    fn test_byte_views_shared_bytes() {
        // Every view references the same bytes of one data buffer
        let long = b"a string longer than twelve bytes";
        let view = make_view(long, 0, 0);
        let array = BinaryViewArray::new(
            std::iter::repeat_n(view, N).collect(),
            Arc::from([Buffer::from_slice_ref(long)]),
            None,
        );
        let sliced = array.slice(0, N / 2);
        let before = sliced.get_buffer_memory_size();
        // Only the views buffer is oversized. `gc` would copy the bytes N / 2 times.
        let minified = minify(&sliced).unwrap();
        assert_eq!(minified.to_data(), sliced.to_data());
        let minified = minified.as_binary_view();
        assert_eq!(minified.views().inner().capacity(), 16 * N / 2);
        assert_eq!(
            minified.data_buffers()[0].as_ptr(),
            array.data_buffers()[0].as_ptr()
        );
        assert!(minified.get_buffer_memory_size() < before);
    }

    #[test]
    fn test_dense_union_shared_child_rows() {
        let array = shared_dense_union(N);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let minified = minified.as_union();
        assert_eq!(minified.child(0).as_primitive::<Int32Type>().values(), &[2]);
        assert!(minified.offsets().unwrap().iter().all(|o| *o == 0));
    }

    #[test]
    fn test_list_view_overlap_and_gap() {
        // Three views share a long string, and a gap separates them from the
        // fourth view
        let long = "x".repeat(1000);
        let values = Arc::new(StringArray::from(vec![long.as_str(), "a", "b", "c", "d"]));
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let array = ListViewArray::new(
            field,
            vec![0, 0, 0, 4].into(),
            vec![1, 1, 1, 1].into(),
            values,
            None,
        );
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        // The long string is not copied once per view
        assert!(minified.get_buffer_memory_size() <= array.get_buffer_memory_size());
        let minified = minified.as_list_view::<i32>();
        assert_eq!(minified.values().len(), 2);
        assert_eq!(minified.offsets().as_ref(), &[0, 0, 0, 1]);
    }

    /// Builds a dense union whose rows all reference child row 1.
    fn shared_dense_union(len: usize) -> UnionArray {
        let fields = UnionFields::try_new([0], [Field::new("int", DataType::Int32, true)]).unwrap();
        let child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        UnionArray::try_new(
            fields,
            vec![0i8; len].into(),
            Some(vec![1; len].into()),
            vec![child],
        )
        .unwrap()
    }

    #[test]
    fn test_copied_rows_keep_shared_child_rows() {
        // Copies of dense union rows keep sharing child rows: in a list with
        // null ranges, in list views with gaps, in a dense union child, and
        // in dictionary values with unused values
        let union = Arc::new(shared_dense_union(N));
        let field = Arc::new(Field::new_list_field(union.data_type().clone(), true));
        let list = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2, N / 2)),
            union.clone(),
            Some(nulls_every(2).slice(0, N / 2)),
        );
        let list_view = ListViewArray::new(
            field,
            (0..N as i32 / 2).map(|i| i * 2).collect(),
            vec![1; N / 2].into(),
            union.clone(),
            None,
        );
        let fields =
            UnionFields::try_new([0], [Field::new("u", union.data_type().clone(), true)]).unwrap();
        let nested = UnionArray::try_new(
            fields,
            vec![0i8; N / 2].into(),
            Some((0..N as i32 / 2).map(|i| i * 2).collect()),
            vec![union.clone()],
        )
        .unwrap();
        let dictionary = DictionaryArray::<Int16Type>::try_new(
            Int16Array::from_iter_values((0..N as i16 / 2).map(|i| i * 2)),
            union,
        )
        .unwrap();
        for array in [
            Arc::new(list) as ArrayRef,
            Arc::new(list_view),
            Arc::new(nested),
            Arc::new(dictionary),
        ] {
            let minified = minify(array.as_ref()).unwrap();
            minified.to_data().validate_full().unwrap();
            assert_eq!(minified.to_data(), array.to_data());
            let mut data = minified.to_data();
            while data.data_type() != &DataType::Int32 {
                data = data.child_data()[0].clone();
            }
            assert_eq!(data.len(), 1, "{}", array.data_type());
        }
    }

    /// Sums the rows of all `Int32` arrays in `data`.
    fn int32_rows(data: &ArrayData) -> usize {
        let own = match data.data_type() {
            DataType::Int32 => data.len(),
            _ => 0,
        };
        own + data.child_data().iter().map(int32_rows).sum::<usize>()
    }

    #[test]
    fn test_gathered_types_keep_shared_child_rows() {
        // Each array wraps a dense union whose rows share one child row, and
        // has null rows. A list with null ranges over each array makes
        // `minify` gather rows of the array.
        let n = 12;
        let union: ArrayRef = Arc::new(shared_dense_union(2 * n));
        let nulls = || Some(NullBuffer::from_iter((0..n).map(|i| i % 3 != 0)));
        let item = |a: &ArrayRef| Arc::new(Field::new_list_field(a.data_type().clone(), true));
        let one_each = || OffsetBuffer::from_lengths(std::iter::repeat_n(1, n));
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("keys", DataType::Int8, false),
                Field::new("values", union.data_type().clone(), true),
            ]),
            vec![
                Arc::new(Int8Array::from_iter_values(0..n as i8)),
                union.slice(0, n),
            ],
            None,
        );
        let map_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let sparse_fields =
            UnionFields::try_new([3], [Field::new("u", union.data_type().clone(), true)]).unwrap();
        let wrapped: Vec<ArrayRef> = vec![
            Arc::new(ListArray::new(
                item(&union),
                one_each(),
                union.slice(0, n),
                nulls(),
            )),
            Arc::new(LargeListArray::new(
                item(&union),
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, n)),
                union.slice(0, n),
                nulls(),
            )),
            Arc::new(MapArray::new(
                map_field,
                one_each(),
                entries,
                nulls(),
                false,
            )),
            Arc::new(FixedSizeListArray::new(
                item(&union),
                2,
                union.clone(),
                nulls(),
            )),
            Arc::new(StructArray::new(
                Fields::from(vec![Field::new("u", union.data_type().clone(), true)]),
                vec![union.slice(0, n)],
                nulls(),
            )),
            Arc::new(
                UnionArray::try_new(
                    sparse_fields,
                    vec![3i8; n].into(),
                    None,
                    vec![union.slice(0, n)],
                )
                .unwrap(),
            ),
            Arc::new(ListViewArray::new(
                item(&union),
                (0..n as i32).collect(),
                vec![1; n].into(),
                union.clone(),
                nulls(),
            )),
            Arc::new(
                RunArray::<Int16Type>::try_new(
                    &Int16Array::from_iter_values(1..=n as i16),
                    union.slice(0, n).as_ref(),
                )
                .unwrap(),
            ),
            Arc::new(
                DictionaryArray::<Int16Type>::try_new(
                    Int16Array::new((0..n as i16).collect(), nulls()),
                    union.clone(),
                )
                .unwrap(),
            ),
        ];
        for array in wrapped {
            let field = item(&array);
            // Every other row is null and keeps a non-empty range
            let list_nulls = NullBuffer::from_iter((0..n / 2).map(|i| i % 2 == 0));
            let lengths = OffsetBuffer::from_lengths(std::iter::repeat_n(2, n / 2));
            let list = ListArray::new(field, lengths, array, Some(list_nulls));
            let minified = minify(&list).unwrap();
            minified.to_data().validate_full().unwrap();
            assert_eq!(minified.to_data(), list.to_data(), "{}", list.data_type());
            assert_eq!(int32_rows(&minified.to_data()), 1, "{}", list.data_type());
        }
    }

    /// Rows of a dense union with `child` as its only child, one per index.
    fn dense_union_of(child: ArrayRef, indices: &[i32]) -> UnionArray {
        let fields =
            UnionFields::try_new([0], [Field::new("a", child.data_type().clone(), true)]).unwrap();
        let type_ids = vec![0_i8; indices.len()].into();
        UnionArray::try_new(fields, type_ids, Some(indices.to_vec().into()), vec![child]).unwrap()
    }

    /// Returns the run ends of the run-end encoded array in `data`.
    fn run_count(data: &ArrayData) -> usize {
        match data.data_type() {
            DataType::RunEndEncoded(_, _) => data.child_data()[0].len(),
            _ => run_count(&data.child_data()[0]),
        }
    }

    #[test]
    fn test_gathered_runs_are_not_merged() {
        // Runs 0 and 1 hold equal values. `minify` does not merge them.
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2, 3]),
            &Int32Array::from(vec![5, 5, 6]),
        )
        .unwrap();
        let direct = minify(&runs.slice(0, 2)).unwrap();
        assert_eq!(run_count(&direct.to_data()), 2);

        let union = dense_union_of(Arc::new(runs.clone()), &[0, 1]);
        let minified = minify(&union).unwrap();
        assert_eq!(minified.to_data(), union.to_data());
        assert_eq!(run_count(&minified.to_data()), 2);

        let keys = Int8Array::from(vec![0, 1]);
        let dict = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(runs)).unwrap();
        let minified = minify(&dict).unwrap();
        assert_eq!(minified.to_data(), dict.to_data());
        assert_eq!(run_count(&minified.to_data()), 2);
    }

    #[test]
    fn test_gathered_list_drops_bytes_of_null_rows() {
        let strings = StringViewArray::from_iter_values(["a long string, not inline", "b"]);
        let inner = dense_union_of(Arc::new(strings), &[0, 1]);
        // Row 0 is null and keeps the range of the long string
        let list = ListArray::new(
            Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(inner),
            Some(NullBuffer::from(vec![false, true])),
        );
        // `minify` gathers row 0 of the list, then empties its range
        let union = dense_union_of(Arc::new(list), &[0]);
        let minified = minify(&union).unwrap();
        assert_eq!(minified.to_data(), union.to_data());
        let data = minified.to_data();
        let views = &data.child_data()[0].child_data()[0].child_data()[0];
        assert_eq!(views.len(), 0);
        assert_eq!(views.buffers().iter().map(|b| b.len()).sum::<usize>(), 0);
    }

    #[test]
    fn test_gathered_list_keeps_runs() {
        let runs =
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2]), &Int32Array::from(vec![7]))
                .unwrap();
        // Rows 0 and 2 hold one value each of the same run. Their child
        // ranges are adjacent, but the rows are not.
        let list = ListArray::new(
            Arc::new(Field::new_list_field(runs.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 0, 1]),
            Arc::new(runs),
            None,
        );
        // `minify` gathers dictionary values 0 and 2, and their child rows
        // stay one run
        let keys = Int8Array::from(vec![0, 2]);
        let dict = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(list)).unwrap();
        let minified = minify(&dict).unwrap();
        assert_eq!(minified.to_data(), dict.to_data());
        assert_eq!(run_count(&minified.to_data()), 1);
    }

    #[test]
    fn test_gathered_ranges_in_one_run_copy_its_value_once() {
        // One run of 1000 rows. Every other list row is null and keeps its
        // range, so `minify` gathers 500 ranges of the same run.
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1000]),
            &StringArray::from(vec!["x".repeat(1000)]),
        )
        .unwrap();
        let list = ListArray::new(
            Arc::new(Field::new_list_field(runs.data_type().clone(), true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, 1000)),
            Arc::new(runs.clone()),
            Some(NullBuffer::from_iter((0..1000).map(|i| i % 2 == 0))),
        );
        let minified = minify(&list).unwrap();
        assert_eq!(minified.as_ref(), &list as &dyn Array);
        let data = minified.to_data();
        assert_eq!(run_count(&data), 1);
        // The result is compacted, not the input returned by the fallback
        assert_eq!(data.child_data()[0].len(), 500);

        // Dense union rows that reference every other row of the run, out of order
        let offsets: Vec<i32> = (0..500).rev().map(|i| 2 * i).collect();
        let union = dense_union_of(Arc::new(runs), &offsets);
        let minified = minify(&union).unwrap();
        assert_eq!(minified.to_data(), union.to_data());
        let data = minified.to_data();
        assert_eq!(run_count(&data), 1);
        assert_eq!(data.child_data()[0].len(), 500);
    }

    #[test]
    fn test_byte_view_offset_past_u32_end() {
        // `view_data` adds offsets and lengths as `usize`
        let view = ByteView::new(20, b"xxxx")
            .with_offset(u32::MAX - 10)
            .as_u128();
        // A valid array needs a buffer longer than `u32::MAX`. `view_data`
        // reads only the views, so a short buffer suffices here.
        let buffers: Arc<[Buffer]> = Arc::from([Buffer::from_vec(vec![b'x'; 20])]);
        // SAFETY: the test reads only the views, never the bytes they point to
        let array = unsafe { BinaryViewArray::new_unchecked(vec![view].into(), buffers, None) };
        let data = view_data(&array, None, 0..1);
        assert_eq!(
            data.merged,
            [(0, u32::MAX as usize - 10, u32::MAX as usize + 10)]
        );
        assert_eq!(data.referenced, [20]);
    }

    #[test]
    fn test_fallback_threshold() {
        assert!(!grew(100, 164, 1));
        assert!(grew(100, 165, 1));
        assert!(!grew(100, 292, 3));
        assert!(grew(100, 293, 3));
    }

    #[test]
    fn test_same_data_compares_nulls() {
        let values = ScalarBuffer::<i32>::from(vec![1, 2, 3, 4]);
        let nulls = NullBuffer::from(vec![true, false, true, true, false]);
        let a = Int32Array::new(values.clone(), Some(nulls.slice(0, 4))).to_data();
        let shifted = Int32Array::new(values.clone(), Some(nulls.slice(1, 4))).to_data();
        let other = NullBuffer::from(vec![true, false, true, true]);
        let copied = Int32Array::new(values.clone(), Some(other)).to_data();
        let none = Int32Array::new(values, None).to_data();
        assert!(same_data(&a, &a.clone()));
        assert!(!same_data(&a, &shifted));
        assert!(!same_data(&a, &copied));
        assert!(!same_data(&a, &none));

        // Columns that differ only in their nulls are not shared
        let field = |n| Arc::new(Field::new(n, DataType::Int32, true));
        let array = StructArray::from(vec![
            (field("x"), make_array(a) as ArrayRef),
            (field("y"), make_array(shifted) as ArrayRef),
        ]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let columns = minified.as_struct().columns();
        assert!(!Arc::ptr_eq(&columns[0], &columns[1]));
    }

    #[test]
    fn test_all_valid_nulls_are_dropped_without_copies() {
        // A null buffer without nulls takes the paths for arrays without nulls
        let all_valid = |n| Some(NullBuffer::new_valid(n));
        let strings = StringArray::new(
            OffsetBuffer::from_lengths([1, 2, 3, 4]),
            b"abcdefghij".to_vec().into(),
            all_valid(4),
        );
        let minified = minify(&strings.slice(1, 2)).unwrap();
        assert!(minified.nulls().is_none());
        // The rebased offsets and the reused value bytes
        assert_eq!(allocated_memory(&minified.to_data()).1, 2);

        // A list with a null range gathers a list view child with all-valid nulls
        let child = ListViewArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            vec![0, 1, 2].into(),
            vec![1, 1, 1].into(),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            all_valid(3),
        );
        let list = ListArray::new(
            Arc::new(Field::new_list_field(child.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1, 1]),
            Arc::new(child),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let minified = minify(&list).unwrap();
        assert_eq!(minified.to_data(), list.to_data());
        let child = minified.as_list::<i32>().values();
        assert_eq!(child.len(), 2);
        assert!(child.nulls().is_none());
        assert!(child.as_list_view::<i32>().values().nulls().is_none());
    }

    #[test]
    fn test_push_range() {
        let mut ranges = vec![];
        push_range(&mut ranges, 0..0);
        assert!(ranges.is_empty());
        push_range(&mut ranges, 2..4);
        push_range(&mut ranges, 4..6);
        push_range(&mut ranges, 6..6);
        push_range(&mut ranges, 7..8);
        assert_eq!(ranges, [2..6, 7..8]);
    }

    #[test]
    fn test_gathers_parts() {
        let list = |t| DataType::List(Arc::new(Field::new_list_field(t, true)));
        let list_view = DataType::ListView(Arc::new(Field::new_list_field(DataType::Int32, true)));
        assert!(!gathers_parts(&DataType::Utf8));
        assert!(!gathers_parts(&list(DataType::Utf8)));
        assert!(gathers_parts(&list_view));
        assert!(gathers_parts(&list(list_view)));
    }

    #[test]
    fn test_gather_ranges_copy_exact_byte_capacity() {
        // 128 bytes, so the gathered values need no rounding
        let strings = StringArray::from_iter_values((0..8).map(|i| format!("{i:032}")));
        let gathered = gather_ranges(&strings, &[0..2, 4..6]).unwrap();
        assert_eq!(gathered.len(), 4);
        let values = gathered.as_string::<i32>().values();
        assert_eq!(values.len(), 128);
        assert_eq!(values.capacity(), 128);
    }

    fn sizes_of(array: &dyn Array) -> Vec<u64> {
        let mut sizes = vec![0; array.len()];
        row_sizes(array, &mut sizes).unwrap();
        sizes
    }

    #[test]
    fn test_row_sizes_values() {
        let strings = StringArray::new(
            OffsetBuffer::from_lengths([2, 2, 3]),
            b"abcdefg".to_vec().into(),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        // Offset, bytes of valid rows, and 1 bitmap byte at row 0
        assert_eq!(sizes_of(&strings), [7, 4, 7]);
        assert_eq!(compacted_size(&strings).unwrap(), 4 * 4 + 5 + 1);

        let values = StringArray::from(vec!["x", "yy"]);
        let keys = Int8Array::from(vec![0, 0, 1]);
        let dict = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
        // Every row pays its key and its value
        assert_eq!(sizes_of(&dict), [6, 6, 7]);
        assert_eq!(compacted_size(&dict).unwrap(), 3 + 3 * 4 + 3);

        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3]),
            &Int64Array::from(vec![5, 6]),
        )
        .unwrap();
        // Every row of a run pays its run end and its value
        assert_eq!(sizes_of(&runs), [12, 12, 12]);
        assert_eq!(compacted_size(&runs).unwrap(), 2 * 4 + 2 * 8);

        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths([2, 1, 0]),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        // The null row 1 pays its offset but not its child row
        assert_eq!(sizes_of(&list), [4 + 8 + 1, 4, 4]);
        assert_eq!(compacted_size(&list).unwrap(), 4 * 4 + 8 + 1);
    }

    #[test]
    fn test_row_sizes_shared_and_edge_values() {
        // A null buffer without nulls costs nothing, as `minify` drops it
        let ints = Int32Array::new(vec![1, 2].into(), Some(NullBuffer::new_valid(2)));
        assert_eq!(sizes_of(&ints), [4, 4]);

        // Views of 12 bytes are inline, views of 13 bytes are not
        let views = StringViewArray::from_iter_values(["a".repeat(12), "b".repeat(13)]);
        assert_eq!(sizes_of(&views), [16, 16 + 13]);

        // Both rows pay for the shared child row
        let union = dense_union_of(Arc::new(Int64Array::from(vec![7, 8])), &[1, 1]);
        assert_eq!(sizes_of(&union), [1 + 4 + 8, 1 + 4 + 8]);
        assert_eq!(compacted_size(&union).unwrap(), 2 + 2 * 4 + 8);

        // Overlapping views pay for their child rows each. The null view
        // pays only its offset, its size and its bitmap byte.
        let list_views = ListViewArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            vec![0, 1, 2].into(),
            vec![2, 2, 1].into(),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Some(NullBuffer::from(vec![true, true, false])),
        );
        assert_eq!(sizes_of(&list_views), [8 + 8 + 1, 8 + 8, 8]);
        let minified = minify(&list_views).unwrap();
        let minified = minified.as_list_view::<i32>();
        // `minify` gives the null view an offset and a size of zero
        assert_eq!(minified.offsets().as_ref(), [0, 1, 0]);
        assert_eq!(minified.sizes().as_ref(), [2, 2, 0]);

        // A fixed size list of size zero has no child rows
        let empty = FixedSizeListArray::new_null(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            0,
            3,
        );
        assert_eq!(sizes_of(&empty), [1, 0, 0]);
        assert_eq!(compacted_size(&empty).unwrap(), 1);
    }

    #[test]
    fn test_row_sizes_adds_and_checks_len() {
        let array = Int32Array::from(vec![1, 2]);
        let mut sizes = vec![1, 2];
        row_sizes(&array, &mut sizes).unwrap();
        assert_eq!(sizes, [5, 6]);
        let err = row_sizes(&array, &mut [0; 3]).unwrap_err();
        assert!(err.to_string().contains("needs 2 entries, got 3"), "{err}");
    }

    #[test]
    fn test_split_by_size() {
        assert_eq!(split_by_size(&[], 8), Vec::<Range<usize>>::new());
        assert_eq!(split_by_size(&[9], 8), vec![0..1]);
        assert_eq!(split_by_size(&[0, 8, 0, 1], 8), vec![0..3, 3..4]);
    }

    #[test]
    fn test_split_by_size_saturates() {
        assert_eq!(split_by_size(&[u64::MAX, 1], u64::MAX), vec![0..1, 1..2]);
        assert_eq!(split_by_size(&[u64::MAX - 1, 2, 5], 10), vec![0..1, 1..3]);

        let mut sizes = [u64::MAX, 0, 0];
        row_sizes(&Int32Array::from(vec![1, 2, 3]), &mut sizes).unwrap();
        assert_eq!(sizes, [u64::MAX, 4, 4]);
    }

    #[test]
    fn test_sizes_views_over_many_small_buffers() {
        // `gc` and `concat` give one data buffer of 20 bytes per row
        let parts: Vec<StringViewArray> = (0..1000)
            .map(|i| StringViewArray::from(vec![format!("{i:020}")]).gc())
            .collect();
        let parts: Vec<&dyn Array> = parts.iter().map(|a| a as &dyn Array).collect();
        let array = crate::concat::concat(&parts).unwrap();
        assert_eq!(array.as_string_view().data_buffers().len(), 1000);

        let slice = array.slice(0, 1);
        assert_eq!(compacted_size(&slice).unwrap(), 36);
        assert_eq!(buffer_bytes(&minify(&slice).unwrap().to_data()), 36);
        assert_eq!(sizes_of(&slice), [36]);

        let sizes = sizes_of(&array);
        let ranges = split_by_size(&sizes, 360);
        assert_eq!(ranges.len(), 100);
        for r in ranges {
            assert_eq!(compacted_size(&array.slice(r.start, r.len())).unwrap(), 360);
        }
    }

    #[test]
    fn test_sizes_views_share_bytes() {
        // 100 views share 1000 bytes, and a second buffer is unused
        let shared = Buffer::from_vec(vec![b'x'; 1000]);
        let unused = Buffer::from_vec(vec![b'y'; 200_000]);
        let view = ByteView::new(1000, b"xxxx").as_u128();
        let array =
            StringViewArray::try_new(vec![view; 100].into(), vec![shared, unused], None).unwrap();
        assert_eq!(compacted_size(&array).unwrap(), 16 * 100 + 1000);
        assert_eq!(
            buffer_bytes(&minify(&array).unwrap().to_data()),
            16 * 100 + 1000
        );
        // Every row pays the shared bytes
        assert_eq!(sizes_of(&array), [16 + 1000; 100]);
    }

    #[test]
    fn test_sizes_view_null_rows() {
        let long = "x".repeat(44);
        let array = StringViewArray::from(vec![long.as_str(), long.as_str()]);
        let (views, buffers, _) = array.into_parts();
        let nulls = NullBuffer::from(vec![true, false]);
        let array = StringViewArray::try_new(views, buffers, Some(nulls)).unwrap();
        // Two views, the bytes of row 0 and 1 bitmap byte
        assert_eq!(compacted_size(&array).unwrap(), 2 * 16 + 44 + 1);
        assert_eq!(buffer_bytes(&minify(&array).unwrap().to_data()), 77);
        assert_eq!(sizes_of(&array), [16 + 44 + 1, 16]);
    }

    #[test]
    fn test_split_by_size_is_not_a_hard_cap() {
        // Each range of 2 rows sums to 16, but holds 3 offsets
        let array = StringArray::from(vec!["abcd"; 8]);
        let ranges = split_by_size(&sizes_of(&array), 16);
        assert_eq!(ranges, vec![0..2, 2..4, 4..6, 6..8]);
        for r in ranges {
            assert_eq!(compacted_size(&array.slice(r.start, r.len())).unwrap(), 20);
        }

        // A value that every row shares makes every range one row long
        let values = StringArray::from(vec!["x".repeat(1000), "small".to_string()]);
        let dict = DictionaryArray::new(Int32Array::from(vec![0; 10]), Arc::new(values));
        assert_eq!(compacted_size(&dict).unwrap(), 10 * 4 + 2 * 4 + 1000);
        let ranges = split_by_size(&sizes_of(&dict), 2000);
        assert_eq!(ranges.len(), 10);
    }

    /// Sums the buffer lengths of all buffers, including children.
    fn buffer_bytes(data: &ArrayData) -> usize {
        data.buffers().iter().map(|b| b.len()).sum::<usize>()
            + data.nulls().map_or(0, |n| n.buffer().len())
            + data.child_data().iter().map(buffer_bytes).sum::<usize>()
    }

    /// Generates random arrays of every type that `minify` supports. Arrays
    /// have random nulls, null rows with non-empty ranges, and random slices.
    struct Gen(rand::rngs::StdRng);

    impl Gen {
        fn new(seed: u64) -> Self {
            use rand::SeedableRng;
            Self(rand::rngs::StdRng::seed_from_u64(seed))
        }

        fn int(&mut self, max: usize) -> usize {
            use rand::RngExt;
            self.0.random_range(0..=max)
        }

        fn nulls(&mut self, len: usize) -> Option<NullBuffer> {
            use rand::RngExt;
            let p = [1.0, 0.9, 0.5, 0.0][self.int(3)];
            match self.int(2) {
                0 => None,
                _ => Some(NullBuffer::from_iter(
                    (0..len).map(|_| self.0.random_bool(p)),
                )),
            }
        }

        fn lengths(&mut self, len: usize, max: usize) -> OffsetBuffer<i32> {
            OffsetBuffer::from_lengths((0..len).map(|_| self.int(max)))
        }

        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| b'a' + self.int(25) as u8).collect()
        }

        /// Returns a random array of `len` rows.
        fn array(&mut self, depth: usize, len: usize) -> ArrayRef {
            let extra = [0, 1, 7, 30][self.int(3)];
            let before = self.int(extra);
            let array = self.unsliced(depth, len + extra);
            array.slice(before, len)
        }

        fn unsliced(&mut self, depth: usize, len: usize) -> ArrayRef {
            let kinds = if depth == 0 { 12 } else { 24 };
            let kind = match self.int(kinds - 1) {
                // Leaf kinds 21 to 23 follow the nested kinds
                k if depth == 0 && k >= 9 => k + 12,
                k => k,
            };
            match kind {
                0 => Arc::new(Int32Array::new((0..len as i32).collect(), self.nulls(len))),
                1 => Arc::new(BooleanArray::new(
                    BooleanBuffer::from_iter((0..len).map(|i| i % 3 == 0)),
                    self.nulls(len),
                )),
                2 => Arc::new(NullArray::new(len)),
                3 => {
                    let offsets = self.lengths(len, 6);
                    let values = self.bytes(offsets[len] as usize);
                    Arc::new(StringArray::new(offsets, values.into(), self.nulls(len)))
                }
                4 => {
                    let offsets: Vec<i64> =
                        self.lengths(len, 6).iter().map(|o| *o as i64).collect();
                    let values = self.bytes(offsets[len] as usize);
                    Arc::new(LargeBinaryArray::new(
                        OffsetBuffer::new(offsets.into()),
                        values.into(),
                        self.nulls(len),
                    ))
                }
                5 => Arc::new(
                    FixedSizeBinaryArray::try_new(3, self.bytes(3 * len).into(), self.nulls(len))
                        .unwrap(),
                ),
                6 | 7 => {
                    let strings: Vec<String> = (0..len)
                        .map(|_| {
                            let n = self.int(30);
                            String::from_utf8(self.bytes(n)).unwrap()
                        })
                        .collect();
                    let array = StringViewArray::from_iter_values(strings);
                    let (views, buffers, _) = array.into_parts();
                    let array = StringViewArray::new(views, buffers, self.nulls(len));
                    if self.int(1) == 0 {
                        Arc::new(array)
                    } else {
                        // Views out of order that share bytes
                        let indices = (0..len as u32).map(|_| self.int(len.max(1) - 1) as u32);
                        let indices = UInt32Array::from_iter_values(indices);
                        if len == 0 {
                            Arc::new(array)
                        } else {
                            take(&array, &indices, None).unwrap()
                        }
                    }
                }
                8 => Arc::new(Int64Array::new((0..len as i64).collect(), self.nulls(len))),
                9 | 10 => {
                    let offsets = self.lengths(len, 4);
                    let values = self.array(depth - 1, offsets[len] as usize);
                    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
                    let nulls = self.nulls(len);
                    if self.int(1) == 0 {
                        Arc::new(ListArray::new(field, offsets, values, nulls))
                    } else {
                        let offsets: Vec<i64> = offsets.iter().map(|o| *o as i64).collect();
                        let offsets = OffsetBuffer::new(offsets.into());
                        Arc::new(LargeListArray::new(field, offsets, values, nulls))
                    }
                }
                11 => {
                    let offsets = self.lengths(len, 3);
                    let n = offsets[len] as usize;
                    let values = self.array(depth - 1, n);
                    let fields = Fields::from(vec![
                        Field::new("keys", DataType::Int32, false),
                        Field::new("values", values.data_type().clone(), true),
                    ]);
                    let keys = Arc::new(Int32Array::from_iter_values(0..n as i32));
                    let entries = StructArray::new(fields.clone(), vec![keys, values], None);
                    let field = Arc::new(Field::new_struct("entries", fields, false));
                    Arc::new(
                        MapArray::try_new(field, offsets, entries, self.nulls(len), false).unwrap(),
                    )
                }
                12 | 13 => {
                    let n = self.int(2 * len);
                    let values = self.array(depth - 1, n);
                    let mut offsets = vec![];
                    let mut sizes = vec![];
                    for _ in 0..len {
                        let o = self.int(n);
                        offsets.push(o);
                        sizes.push(self.int((n - o).min(4)));
                    }
                    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
                    let nulls = self.nulls(len);
                    if self.int(1) == 0 {
                        let o = offsets.iter().map(|o| *o as i32).collect();
                        let s = sizes.iter().map(|s| *s as i32).collect();
                        Arc::new(ListViewArray::new(field, o, s, values, nulls))
                    } else {
                        let o = offsets.iter().map(|o| *o as i64).collect();
                        let s = sizes.iter().map(|s| *s as i64).collect();
                        Arc::new(LargeListViewArray::new(field, o, s, values, nulls))
                    }
                }
                14 => {
                    let values = self.array(depth - 1, 2 * len);
                    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
                    Arc::new(FixedSizeListArray::new(field, 2, values, self.nulls(len)))
                }
                15 => {
                    let a = self.array(depth - 1, len);
                    let b = self.array(depth - 1, len);
                    let fields = Fields::from(vec![
                        Field::new("a", a.data_type().clone(), true),
                        Field::new("b", b.data_type().clone(), true),
                    ]);
                    Arc::new(
                        StructArray::try_new_with_length(fields, vec![a, b], self.nulls(len), len)
                            .unwrap(),
                    )
                }
                16 | 17 => {
                    let dense = self.int(1) == 0;
                    let type_ids: Vec<i8> = (0..len).map(|_| self.int(1) as i8).collect();
                    let (children, offsets) = if dense {
                        let lens = [self.int(len).max(1), self.int(len).max(1)];
                        let offsets: Vec<i32> = type_ids
                            .iter()
                            .map(|t| self.int(lens[*t as usize] - 1) as i32)
                            .collect();
                        let children = vec![
                            self.array(depth - 1, lens[0]),
                            self.array(depth - 1, lens[1]),
                        ];
                        (children, Some(offsets.into()))
                    } else {
                        (
                            vec![self.array(depth - 1, len), self.array(depth - 1, len)],
                            None,
                        )
                    };
                    let fields = UnionFields::try_new(
                        [0, 1],
                        [
                            Field::new("a", children[0].data_type().clone(), true),
                            Field::new("b", children[1].data_type().clone(), true),
                        ],
                    )
                    .unwrap();
                    Arc::new(
                        UnionArray::try_new(fields, type_ids.into(), offsets, children).unwrap(),
                    )
                }
                18 => {
                    let mut ends = vec![];
                    let mut end = 0;
                    while end < len {
                        end = (end + 1 + self.int(3)).min(len);
                        ends.push(end as i32);
                    }
                    let values = self.array(depth - 1, ends.len());
                    Arc::new(
                        RunArray::<Int32Type>::try_new(&Int32Array::from(ends), values.as_ref())
                            .unwrap(),
                    )
                }
                21 => {
                    let offsets: Vec<i64> =
                        self.lengths(len, 6).iter().map(|o| *o as i64).collect();
                    let values = self.bytes(offsets[len] as usize);
                    let offsets = OffsetBuffer::new(offsets.into());
                    Arc::new(LargeStringArray::new(
                        offsets,
                        values.into(),
                        self.nulls(len),
                    ))
                }
                22 => {
                    let offsets = self.lengths(len, 6);
                    let values = self.bytes(offsets[len] as usize);
                    Arc::new(BinaryArray::new(offsets, values.into(), self.nulls(len)))
                }
                23 => {
                    let values: Vec<Vec<u8>> = (0..len)
                        .map(|_| {
                            let n = self.int(20);
                            self.bytes(n)
                        })
                        .collect();
                    let array = BinaryViewArray::from_iter_values(values);
                    let (views, buffers, _) = array.into_parts();
                    Arc::new(BinaryViewArray::new(views, buffers, self.nulls(len)))
                }
                _ => {
                    let n = self.int(len).max(1);
                    let values = loop {
                        let values = self.array(depth - 1, n);
                        if !matches!(
                            values.data_type(),
                            DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
                        ) {
                            break values;
                        }
                    };
                    let keys = Int16Array::new(
                        (0..len).map(|_| self.int(n - 1) as i16).collect(),
                        self.nulls(len),
                    );
                    Arc::new(DictionaryArray::<Int16Type>::try_new(keys, values).unwrap())
                }
            }
        }
    }

    /// Returns `true` if rows of `data_type` can share data, which
    /// `row_sizes` charges to every row that references it.
    fn shares_data(data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::Union(_, UnionMode::Dense)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Utf8View
            | DataType::BinaryView => true,
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => shares_data(f.data_type()),
            DataType::Struct(fields) => fields.iter().any(|f| shares_data(f.data_type())),
            DataType::Union(fields, _) => fields.iter().any(|(_, f)| shares_data(f.data_type())),
            _ => false,
        }
    }

    /// The error bound of `row_sizes` for an array that shares no data: one
    /// offset, one run and 2 bytes per bitmap per node, plus the bitmaps
    /// `minify` can drop and the bitmap bytes of non-contiguous child rows.
    fn row_size_slack(data: &ArrayData) -> usize {
        8 + 8
            + 2 * (2 + bit_util::ceil(data.len(), 8))
            + data.len()
            + data.child_data().iter().map(row_size_slack).sum::<usize>()
    }

    /// Checks `compacted_size` and `row_sizes` against `minify` on `array`
    /// and random slices of it.
    fn check_sizes(g: &mut Gen, array: &dyn Array) {
        let minified = minify(array).unwrap();
        let expected = buffer_bytes(&minified.to_data());
        assert_eq!(
            compacted_size(array).unwrap(),
            expected,
            "{:?}",
            array.data_type()
        );
        // `minify` of a minified array keeps the same buffers
        assert_eq!(compacted_size(minified.as_ref()).unwrap(), expected);

        let mut sizes = vec![0; array.len()];
        row_sizes(array, &mut sizes).unwrap();

        for _ in 0..4 {
            let a = g.int(array.len());
            let b = a + g.int(array.len() - a);
            let slice = array.slice(a, b - a);
            let c = compacted_size(&slice).unwrap();
            assert_eq!(c, buffer_bytes(&minify(&slice).unwrap().to_data()));
            let s = sizes[a..b].iter().sum::<u64>() as i64;
            let slack = row_size_slack(&slice.to_data()) as i64;
            let diff = s - c as i64;
            if shares_data(array.data_type()) {
                assert!(
                    diff >= -slack,
                    "{:?} {a}..{b}: {s} vs {c}",
                    array.data_type()
                );
            } else {
                assert!(
                    diff.abs() <= slack,
                    "{:?} {a}..{b}: {s} vs {c}",
                    array.data_type()
                );
            }
        }
    }

    /// The number of random arrays that `test_sizes_random` checks. Set
    /// `MINIFY_SIZES_ITERATIONS` for a longer run.
    fn iterations() -> u64 {
        std::env::var("MINIFY_SIZES_ITERATIONS").map_or(300, |v| v.parse().unwrap())
    }

    #[test]
    fn test_sizes_random() {
        let seed = std::env::var("MINIFY_SIZES_SEED").map_or(0, |v| v.parse().unwrap());
        for i in seed..seed + iterations() {
            let mut g = Gen::new(i);
            let depth = g.int(3);
            let len = g.int(40);
            let array = g.array(depth, len);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                check_sizes(&mut g, array.as_ref())
            }));
            if result.is_err() {
                panic!("seed {i} failed: {:?}", array.data_type());
            }
        }
    }

    /// Wraps an `Int32Array` or `Int64Array` but reports a time type with an
    /// invalid unit. Safe constructors reject such a type, so only a custom
    /// `Array` can reach the error paths.
    #[derive(Debug)]
    struct InvalidTime(ArrayRef, DataType);

    unsafe impl Array for InvalidTime {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn to_data(&self) -> ArrayData {
            self.0.to_data()
        }
        fn into_data(self) -> ArrayData {
            self.0.to_data()
        }
        fn data_type(&self) -> &DataType {
            &self.1
        }
        fn slice(&self, offset: usize, length: usize) -> ArrayRef {
            Arc::new(Self(self.0.slice(offset, length), self.1.clone()))
        }
        fn len(&self) -> usize {
            self.0.len()
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn offset(&self) -> usize {
            self.0.offset()
        }
        fn nulls(&self) -> Option<&NullBuffer> {
            self.0.nulls()
        }
        fn get_buffer_memory_size(&self) -> usize {
            self.0.get_buffer_memory_size()
        }
        fn get_array_memory_size(&self) -> usize {
            self.0.get_array_memory_size()
        }
    }

    #[test]
    fn sizes_reject_invalid_time_unit() {
        let int32: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let int64: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        for (values, data_type) in [
            (&int32, DataType::Time32(TimeUnit::Microsecond)),
            (&int32, DataType::Time32(TimeUnit::Nanosecond)),
            (&int64, DataType::Time64(TimeUnit::Second)),
            (&int64, DataType::Time64(TimeUnit::Millisecond)),
        ] {
            let expected = format!("Invalid argument error: invalid time unit in {data_type}");
            let array = InvalidTime(Arc::clone(values), data_type);
            assert_eq!(minify(&array).unwrap_err().to_string(), expected);
            assert_eq!(compacted_size(&array).unwrap_err().to_string(), expected);
            let mut sizes = [0; 3];
            assert_eq!(
                row_sizes(&array, &mut sizes).unwrap_err().to_string(),
                expected
            );
        }
    }

    #[test]
    fn test_sizes_of_gathered_ranges_in_one_run() {
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1000]),
            &StringArray::from(vec!["x".repeat(1000)]),
        )
        .unwrap();
        let list = ListArray::new(
            Arc::new(Field::new_list_field(runs.data_type().clone(), true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, 1000)),
            Arc::new(runs.clone()),
            Some(NullBuffer::from_iter((0..1000).map(|i| i % 2 == 0))),
        );
        // Offsets, bitmap, one run end, and one value of 1000 bytes
        let expected = 1001 * 4 + 125 + 4 + 2 * 4 + 1000;
        assert_eq!(compacted_size(&list).unwrap(), expected);
        assert_eq!(buffer_bytes(&minify(&list).unwrap().to_data()), expected);

        let offsets: Vec<i32> = (0..500).rev().map(|i| 2 * i).collect();
        let union = dense_union_of(Arc::new(runs), &offsets);
        // Type ids, offsets, one run end, and one value of 1000 bytes
        let expected = 500 * 5 + 4 + 2 * 4 + 1000;
        assert_eq!(compacted_size(&union).unwrap(), expected);
        assert_eq!(buffer_bytes(&minify(&union).unwrap().to_data()), expected);
    }

    #[test]
    fn test_sizes_of_slices_span_used_rows() {
        let values = Int64Array::from_iter_values(0..1_000_000);
        let keys = Int32Array::from_iter_values(0..1_000_000);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let slice = dict.slice(500_000, 2);
        assert_eq!(sizes_of(&slice), [12, 12]);
        assert_eq!(compacted_size(&slice).unwrap(), 24);

        let child: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000_000));
        let offsets: Vec<i32> = (0..1_000_000).collect();
        let union = dense_union_of(child, &offsets);
        assert_eq!(sizes_of(&union.slice(500_000, 2)), [13, 13]);
    }
}
