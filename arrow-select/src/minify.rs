// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`minify`]: Copy an array into buffers that hold only the data its rows reference
//!
//! Arrays often keep memory alive that their visible rows do not use:
//!
//! * A sliced array shares the buffers of the original, unsliced array.
//! * A sliced byte array shares the full value buffer.
//! * A view array can reference a small part of large data buffers.
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
//! # Null rows
//!
//! `minify` gives every null row of a byte array an empty value range. The
//! value buffer then holds only the data of valid rows.
//!
//! An O(rows) check over the offsets and the null bitmap finds out if any null
//! row has a non-empty range. If none has, `minify` copies or slices the
//! referenced range in one piece. Otherwise it copies the ranges of each run of
//! valid rows.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer, bit_util,
};
use arrow_data::MAX_INLINE_VIEW_LEN;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

/// Returns an array that is logically equal to `array` and whose buffers hold
/// only the data that its rows reference.
///
/// The result has an offset of zero.
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
/// `minify` returns `NotYetImplemented` for nested, union, dictionary and
/// run-end encoded types. It returns `InvalidArgumentError` for a time type
/// with an invalid unit, and the error of an array constructor that rejects
/// the minified parts.
pub fn minify(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
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
        t @ (DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _)) => Err(ArrowError::NotYetImplemented(format!(
            "minify does not support data type {t}"
        ))),
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

/// Appends `range` to `ranges`, and merges it with the last range if they
/// are adjacent. Skips an empty range.
fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    match ranges.last_mut() {
        Some(r) if r.end == range.start => r.end = range.end,
        _ if range.start < range.end => ranges.push(range),
        _ => {}
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

/// Returns the out-of-line bytes that `views` reference. A byte that several
/// views share counts once per view.
fn view_data_len(views: &[u128]) -> usize {
    views
        .iter()
        .map(|v| *v as u32)
        .filter(|len| *len > MAX_INLINE_VIEW_LEN)
        .map(|len| len as usize)
        .sum()
}

/// Returns `true` if `minify` keeps `buffers` of a view array whose views
/// reference `referenced` out-of-line bytes, and `false` if it runs `gc`.
///
/// Views can share bytes, so `referenced` can exceed the allocated bytes.
/// `gc` copies shared bytes once per view, so it runs only if the data
/// buffers hold more than the views reference.
fn keeps_view_buffers(buffers: &[Buffer], referenced: usize) -> bool {
    let allocated: usize = buffers.iter().map(|b| b.capacity()).sum();
    allocated <= referenced + 64 * buffers.len()
}

fn minify_byte_view<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
) -> GenericByteViewArray<T> {
    let referenced = view_data_len(array.views());
    let array = if keeps_view_buffers(array.data_buffers(), referenced) {
        array.clone()
    } else {
        array.gc()
    };
    let (views, buffers, nulls) = array.into_parts();
    // `gc` can return the views unchanged, for example when there are no data buffers
    let views = minify_scalar_buffer(&views);
    let nulls = minify_nulls(nulls.as_ref());
    // SAFETY: the views, data buffers and null bits stay the same
    unsafe { GenericByteViewArray::new_unchecked(views, buffers, nulls) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder, make_view};
    use arrow_array::types::IntervalMonthDayNanoType;
    use arrow_buffer::IntervalMonthDayNano;
    use arrow_data::ArrayData;
    use arrow_schema::{IntervalUnit, TimeUnit};

    const N: usize = 200;

    /// Sums the allocated bytes of all buffers, including children.
    fn allocated(data: &ArrayData) -> usize {
        data.buffers().iter().map(|b| b.capacity()).sum::<usize>()
            + data.nulls().map_or(0, |n| n.buffer().capacity())
            + data.child_data().iter().map(allocated).sum::<usize>()
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
    fn test_byte_views_reuse_buffers_within_slack() {
        // Each buffer holds 50 unreferenced bytes, which is within the slack
        // of 64 bytes per buffer
        let buffers = vec![
            Buffer::from_vec(vec![b'x'; 63]),
            Buffer::from_vec(vec![b'y'; 63]),
        ];
        let array = byte_views(buffers, &[(0, 0, 13), (1, 50, 13)]);
        let minified = minify(&array).unwrap();
        assert_eq!(minified.to_data(), array.to_data());
        let (mut a, mut b) = (vec![], vec![]);
        pointers(&array.to_data(), &mut a);
        pointers(&minified.to_data(), &mut b);
        assert_eq!(a, b);
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
}
