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

use crate::cast::*;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type,
    Decimal256Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    GenericBinaryArray, GenericStringArray, PrimitiveArray, StringViewArray,
};

/// Attempts to cast a `RunArray` with index type K into
/// `to_type` for supported types.
pub(crate) fn run_end_encoded_cast<K: RunEndIndexType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::RunEndEncoded(_, _) => {
            let run_array = array
                .as_any()
                .downcast_ref::<RunArray<K>>()
                .ok_or_else(|| ArrowError::CastError("Expected RunArray".to_string()))?;

            let values = run_array.values();

            match to_type {
                // Stay as RunEndEncoded, cast only the values
                DataType::RunEndEncoded(target_index_field, target_value_field) => {
                    let cast_values =
                        cast_with_options(values, target_value_field.data_type(), cast_options)?;

                    let run_ends_array = PrimitiveArray::<K>::from_iter_values(
                        run_array.run_ends().values().iter().copied(),
                    );
                    let cast_run_ends = cast_with_options(
                        &run_ends_array,
                        target_index_field.data_type(),
                        cast_options,
                    )?;
                    let new_run_array: ArrayRef = match target_index_field.data_type() {
                        DataType::Int16 => {
                            let re = cast_run_ends.as_primitive::<Int16Type>();
                            Arc::new(RunArray::<Int16Type>::try_new(re, cast_values.as_ref())?)
                        }
                        DataType::Int32 => {
                            let re = cast_run_ends.as_primitive::<Int32Type>();
                            Arc::new(RunArray::<Int32Type>::try_new(re, cast_values.as_ref())?)
                        }
                        DataType::Int64 => {
                            let re = cast_run_ends.as_primitive::<Int64Type>();
                            Arc::new(RunArray::<Int64Type>::try_new(re, cast_values.as_ref())?)
                        }
                        _ => {
                            return Err(ArrowError::CastError(
                                "Run-end type must be i16, i32, or i64".to_string(),
                            ));
                        }
                    };
                    Ok(Arc::new(new_run_array))
                }

                // Expand to logical form
                _ => {
                    let run_ends = run_array.run_ends().values().to_vec();
                    let mut indices = Vec::with_capacity(run_array.run_ends().len());
                    let mut physical_idx: usize = 0;
                    for logical_idx in 0..run_array.run_ends().len() {
                        // If the logical index is equal to the (next) run end, increment the physical index,
                        // since we are at the end of a run.
                        if logical_idx == run_ends[physical_idx].as_usize() {
                            physical_idx += 1;
                        }
                        indices.push(physical_idx as i32);
                    }

                    let taken = take(&values, &Int32Array::from_iter_values(indices), None)?;
                    if taken.data_type() != to_type {
                        cast_with_options(taken.as_ref(), to_type, cast_options)
                    } else {
                        Ok(taken)
                    }
                }
            }
        }

        _ => Err(ArrowError::CastError(format!(
            "Cannot cast array of type {:?} to RunEndEncodedArray",
            array.data_type()
        ))),
    }
}

/// Attempts to encode an array into a `RunArray` with index type K
/// and value type `value_type`
pub(crate) fn cast_to_run_end_encoded<K: RunEndIndexType>(
    array: &ArrayRef,
    value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let mut run_ends_builder = PrimitiveBuilder::<K>::new();

    // Cast the input array to the target value type if necessary
    let cast_array = if array.data_type() == value_type {
        array
    } else {
        &cast_with_options(array, value_type, cast_options)?
    };

    // Return early if the array to cast is empty
    if cast_array.is_empty() {
        let empty_run_ends = run_ends_builder.finish();
        let empty_values = make_array(ArrayData::new_empty(value_type));
        return Ok(Arc::new(RunArray::<K>::try_new(
            &empty_run_ends,
            empty_values.as_ref(),
        )?));
    }

    // REE arrays are handled by run_end_encoded_cast
    if let DataType::RunEndEncoded(_, _) = array.data_type() {
        return Err(ArrowError::CastError(
            "Source array is already a RunEndEncoded array, should have been handled by run_end_encoded_cast".to_string()
        ));
    }

    // Identify run boundaries by comparing consecutive values
    let (run_ends, values_indexes) = compute_run_boundaries(cast_array);

    // Build the run_ends array
    for run_end in run_ends {
        run_ends_builder.append_value(K::Native::from_usize(run_end).ok_or_else(|| {
            ArrowError::CastError(format!("Run end index out of range: {}", run_end))
        })?);
    }
    let run_ends_array = run_ends_builder.finish();
    // Build the values array by taking elements at the run start positions
    let indices = PrimitiveArray::<UInt32Type>::from_iter_values(
        values_indexes.iter().map(|&idx| idx as u32),
    );
    let values_array = take(&cast_array, &indices, None)?;

    // Create and return the RunArray
    let run_array = RunArray::<K>::try_new(&run_ends_array, values_array.as_ref())?;
    Ok(Arc::new(run_array))
}

fn compute_run_boundaries(array: &ArrayRef) -> (Vec<usize>, Vec<usize>) {
    if array.is_empty() {
        return (Vec::new(), Vec::new());
    }

    use arrow_schema::{DataType::*, IntervalUnit, TimeUnit};

    match array.data_type() {
        Null => (vec![array.len()], vec![0]),
        Boolean => runs_for_boolean(array.as_boolean()),
        Int8 => runs_for_primitive(array.as_primitive::<Int8Type>()),
        Int16 => runs_for_primitive(array.as_primitive::<Int16Type>()),
        Int32 => runs_for_primitive(array.as_primitive::<Int32Type>()),
        Int64 => runs_for_primitive(array.as_primitive::<Int64Type>()),
        UInt8 => runs_for_primitive(array.as_primitive::<UInt8Type>()),
        UInt16 => runs_for_primitive(array.as_primitive::<UInt16Type>()),
        UInt32 => runs_for_primitive(array.as_primitive::<UInt32Type>()),
        UInt64 => runs_for_primitive(array.as_primitive::<UInt64Type>()),
        Float16 => runs_for_primitive(array.as_primitive::<Float16Type>()),
        Float32 => runs_for_primitive(array.as_primitive::<Float32Type>()),
        Float64 => runs_for_primitive(array.as_primitive::<Float64Type>()),
        Date32 => runs_for_primitive(array.as_primitive::<Date32Type>()),
        Date64 => runs_for_primitive(array.as_primitive::<Date64Type>()),
        Time32(TimeUnit::Second) => runs_for_primitive(array.as_primitive::<Time32SecondType>()),
        Time32(TimeUnit::Millisecond) => {
            runs_for_primitive(array.as_primitive::<Time32MillisecondType>())
        }
        Time64(TimeUnit::Microsecond) => {
            runs_for_primitive(array.as_primitive::<Time64MicrosecondType>())
        }
        Time64(TimeUnit::Nanosecond) => {
            runs_for_primitive(array.as_primitive::<Time64NanosecondType>())
        }
        Duration(TimeUnit::Second) => {
            runs_for_primitive(array.as_primitive::<DurationSecondType>())
        }
        Duration(TimeUnit::Millisecond) => {
            runs_for_primitive(array.as_primitive::<DurationMillisecondType>())
        }
        Duration(TimeUnit::Microsecond) => {
            runs_for_primitive(array.as_primitive::<DurationMicrosecondType>())
        }
        Duration(TimeUnit::Nanosecond) => {
            runs_for_primitive(array.as_primitive::<DurationNanosecondType>())
        }
        Timestamp(TimeUnit::Second, _) => {
            runs_for_primitive(array.as_primitive::<TimestampSecondType>())
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            runs_for_primitive(array.as_primitive::<TimestampMillisecondType>())
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            runs_for_primitive(array.as_primitive::<TimestampMicrosecondType>())
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            runs_for_primitive(array.as_primitive::<TimestampNanosecondType>())
        }
        Interval(IntervalUnit::YearMonth) => {
            runs_for_primitive(array.as_primitive::<IntervalYearMonthType>())
        }
        Interval(IntervalUnit::DayTime) => {
            runs_for_primitive(array.as_primitive::<IntervalDayTimeType>())
        }
        Interval(IntervalUnit::MonthDayNano) => {
            runs_for_primitive(array.as_primitive::<IntervalMonthDayNanoType>())
        }
        Decimal128(_, _) => runs_for_primitive(array.as_primitive::<Decimal128Type>()),
        Decimal256(_, _) => runs_for_primitive(array.as_primitive::<Decimal256Type>()),
        Utf8 => runs_for_string_i32(array.as_string::<i32>()),
        LargeUtf8 => runs_for_string_i64(array.as_string::<i64>()),
        Utf8View => runs_for_string_view(array.as_string_view()),
        Binary => runs_for_binary_i32(array.as_binary::<i32>()),
        LargeBinary => runs_for_binary_i64(array.as_binary::<i64>()),
        BinaryView => runs_for_binary_view(array.as_binary_view()),
        FixedSizeBinary(_) => runs_for_fixed_size_binary(array.as_fixed_size_binary()),
        Dictionary(key_type, _) => match key_type.as_ref() {
            Int8 => runs_for_dictionary::<Int8Type>(array.as_dictionary()),
            Int16 => runs_for_dictionary::<Int16Type>(array.as_dictionary()),
            Int32 => runs_for_dictionary::<Int32Type>(array.as_dictionary()),
            Int64 => runs_for_dictionary::<Int64Type>(array.as_dictionary()),
            UInt8 => runs_for_dictionary::<UInt8Type>(array.as_dictionary()),
            UInt16 => runs_for_dictionary::<UInt16Type>(array.as_dictionary()),
            UInt32 => runs_for_dictionary::<UInt32Type>(array.as_dictionary()),
            UInt64 => runs_for_dictionary::<UInt64Type>(array.as_dictionary()),
            _ => runs_generic(array.as_ref()),
        },
        _ => runs_generic(array.as_ref()),
    }
}

fn runs_for_boolean(array: &BooleanArray) -> (Vec<usize>, Vec<usize>) {
    let len = array.len();
    if let Some(runs) = trivial_runs(len) {
        return runs;
    }

    let mut run_boundaries = Vec::with_capacity(len / 64 + 2);
    let mut current_valid = array.is_valid(0);
    let mut current_value = if current_valid { array.value(0) } else { false };

    for idx in 1..len {
        let valid = array.is_valid(idx);
        let mut boundary = false;
        if current_valid && valid {
            let value = array.value(idx);
            if value != current_value {
                current_value = value;
                boundary = true;
            }
        } else if current_valid != valid {
            boundary = true;
            if valid {
                current_value = array.value(idx);
            }
        }

        if boundary {
            ensure_capacity(&mut run_boundaries, len);
            run_boundaries.push(idx);
        }
        current_valid = valid;
    }

    finalize_runs(run_boundaries, len)
}

fn runs_for_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
) -> (Vec<usize>, Vec<usize>) {
    let len = array.len();
    if let Some(runs) = trivial_runs(len) {
        return runs;
    }

    let values = array.values();
    let mut run_boundaries = Vec::with_capacity(len / 64 + 2);

    if array.null_count() == 0 {
        let mut current = unsafe { *values.get_unchecked(0) };
        let mut idx = 1;
        while idx < len {
            let boundary = scan_run_end::<T>(values, current, idx);
            if boundary == len {
                break;
            }
            ensure_capacity(&mut run_boundaries, len);
            run_boundaries.push(boundary);
            current = unsafe { *values.get_unchecked(boundary) };
            idx = boundary + 1;
        }
        return finalize_runs(run_boundaries, len);
    }

    let nulls = array
        .nulls()
        .expect("null_count > 0 implies a null buffer is present");
    let mut current_valid = nulls.is_valid(0);
    let mut current_value = unsafe { *values.get_unchecked(0) };
    for idx in 1..len {
        let valid = nulls.is_valid(idx);
        let mut boundary = false;
        if current_valid && valid {
            let value = unsafe { *values.get_unchecked(idx) };
            if value != current_value {
                current_value = value;
                boundary = true;
            }
        } else if current_valid != valid {
            boundary = true;
            if valid {
                current_value = unsafe { *values.get_unchecked(idx) };
            }
        }
        if boundary {
            ensure_capacity(&mut run_boundaries, len);
            run_boundaries.push(idx);
        }
        current_valid = valid;
    }
    finalize_runs(run_boundaries, len)
}

fn runs_for_binary_i32(array: &GenericBinaryArray<i32>) -> (Vec<usize>, Vec<usize>) {
    let mut to_usize = |v: i32| v as usize;
    runs_for_binary_like(
        array.len(),
        array.null_count(),
        array.value_offsets(),
        array.value_data(),
        |idx| array.is_valid(idx),
        &mut to_usize,
    )
}

fn runs_for_binary_i64(array: &GenericBinaryArray<i64>) -> (Vec<usize>, Vec<usize>) {
    let mut to_usize = |v: i64| v as usize;
    runs_for_binary_like(
        array.len(),
        array.null_count(),
        array.value_offsets(),
        array.value_data(),
        |idx| array.is_valid(idx),
        &mut to_usize,
    )
}

fn runs_for_binary_like<T: Copy>(
    len: usize,
    null_count: usize,
    offsets: &[T],
    values: &[u8],
    mut is_valid: impl FnMut(usize) -> bool,
    to_usize: &mut impl FnMut(T) -> usize,
) -> (Vec<usize>, Vec<usize>) {
    if let Some(runs) = trivial_runs(len) {
        return runs;
    }

    let mut run_boundaries = Vec::with_capacity(len / 64 + 2);

    if null_count == 0 {
        let mut current_start = to_usize(offsets[0]);
        let mut current_end = to_usize(offsets[1]);
        for idx in 1..len {
            let start = to_usize(offsets[idx]);
            let end = to_usize(offsets[idx + 1]);
            if (end - start) != (current_end - current_start)
                || values[start..end] != values[current_start..current_end]
            {
                ensure_capacity(&mut run_boundaries, len);
                run_boundaries.push(idx);
                current_start = start;
                current_end = end;
            }
        }
    } else {
        let mut current_valid = is_valid(0);
        let mut current_range = (to_usize(offsets[0]), to_usize(offsets[1]));
        for idx in 1..len {
            let valid = is_valid(idx);
            let mut boundary = false;
            if current_valid && valid {
                let start = to_usize(offsets[idx]);
                let end = to_usize(offsets[idx + 1]);
                let (current_start, current_end) = current_range;
                if (end - start) != (current_end - current_start)
                    || values[start..end] != values[current_start..current_end]
                {
                    boundary = true;
                    current_range = (start, end);
                }
            } else if current_valid != valid {
                boundary = true;
                if valid {
                    current_range = (to_usize(offsets[idx]), to_usize(offsets[idx + 1]));
                }
            }
            if boundary {
                ensure_capacity(&mut run_boundaries, len);
                run_boundaries.push(idx);
            }
            current_valid = valid;
        }
    }

    finalize_runs(run_boundaries, len)
}

fn runs_for_string_i32(array: &GenericStringArray<i32>) -> (Vec<usize>, Vec<usize>) {
    let mut to_usize = |v: i32| v as usize;
    runs_for_binary_like(
        array.len(),
        array.null_count(),
        array.value_offsets(),
        array.value_data(),
        |idx| array.is_valid(idx),
        &mut to_usize,
    )
}

fn runs_for_string_i64(array: &GenericStringArray<i64>) -> (Vec<usize>, Vec<usize>) {
    let mut to_usize = |v: i64| v as usize;
    runs_for_binary_like(
        array.len(),
        array.null_count(),
        array.value_offsets(),
        array.value_data(),
        |idx| array.is_valid(idx),
        &mut to_usize,
    )
}

fn runs_for_string_view(array: &StringViewArray) -> (Vec<usize>, Vec<usize>) {
    runs_generic(array)
}

fn runs_for_binary_view(array: &BinaryViewArray) -> (Vec<usize>, Vec<usize>) {
    runs_generic(array)
}

fn runs_for_fixed_size_binary(array: &FixedSizeBinaryArray) -> (Vec<usize>, Vec<usize>) {
    let len = array.len();
    if let Some(runs) = trivial_runs(len) {
        return runs;
    }

    let width = array.value_length() as usize;
    let values = array.value_data();
    let mut run_boundaries = Vec::with_capacity(len / 64 + 2);
    if array.null_count() == 0 {
        let mut current_slice = &values[0..width];
        for idx in 1..len {
            let start = idx * width;
            let slice = &values[start..start + width];
            if slice != current_slice {
                ensure_capacity(&mut run_boundaries, len);
                run_boundaries.push(idx);
                current_slice = slice;
            }
        }
    } else {
        let nulls = array
            .nulls()
            .expect("null_count > 0 implies a null buffer is present");
        let mut current_valid = nulls.is_valid(0);
        let mut current_slice = &values[0..width];
        for idx in 1..len {
            let valid = nulls.is_valid(idx);
            let mut boundary = false;
            if current_valid && valid {
                let start = idx * width;
                let slice = &values[start..start + width];
                if slice != current_slice {
                    boundary = true;
                    current_slice = slice;
                }
            } else if current_valid != valid {
                boundary = true;
                if valid {
                    let start = idx * width;
                    current_slice = &values[start..start + width];
                }
            }
            if boundary {
                ensure_capacity(&mut run_boundaries, len);
                run_boundaries.push(idx);
            }
            current_valid = valid;
        }
    }

    finalize_runs(run_boundaries, len)
}

fn runs_for_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> (Vec<usize>, Vec<usize>) {
    runs_for_primitive(array.keys())
}

fn runs_generic(array: &dyn Array) -> (Vec<usize>, Vec<usize>) {
    let len = array.len();
    if let Some(runs) = trivial_runs(len) {
        return runs;
    }

    let mut run_boundaries = Vec::with_capacity(len / 64 + 2);
    let mut current_data = array.slice(0, 1).to_data();
    for idx in 1..len {
        let next_data = array.slice(idx, 1).to_data();
        if current_data != next_data {
            ensure_capacity(&mut run_boundaries, len);
            run_boundaries.push(idx);
            current_data = next_data;
        }
    }

    finalize_runs(run_boundaries, len)
}

fn trivial_runs(len: usize) -> Option<(Vec<usize>, Vec<usize>)> {
    match len {
        0 => Some((Vec::new(), Vec::new())),
        1 => Some((vec![1], vec![0])),
        _ => None,
    }
}

#[inline]
fn ensure_capacity(vec: &mut Vec<usize>, total_len: usize) {
    if vec.len() == vec.capacity() {
        let remaining = total_len.saturating_sub(vec.len());
        vec.reserve(remaining.max(1));
    }
}

fn finalize_runs(mut run_boundaries: Vec<usize>, len: usize) -> (Vec<usize>, Vec<usize>) {
    let mut values_indexes = Vec::with_capacity(run_boundaries.len() + 1);
    values_indexes.push(0);
    values_indexes.extend_from_slice(&run_boundaries);
    run_boundaries.push(len);
    (run_boundaries, values_indexes)
}

#[inline]
fn scan_run_end<T: ArrowPrimitiveType>(
    values: &[T::Native],
    current: T::Native,
    start: usize,
) -> usize {
    let element_size = std::mem::size_of::<T::Native>();
    if element_size <= 8 && 16 % element_size == 0 {
        let elements_per_chunk = 16 / element_size;
        return scan_run_end_chunk::<T>(values, current, start, elements_per_chunk, element_size);
    }
    scan_run_end_scalar::<T>(values, current, start)
}

#[inline]
fn scan_run_end_chunk<T: ArrowPrimitiveType>(
    values: &[T::Native],
    current: T::Native,
    start: usize,
    elements_per_chunk: usize,
    element_size: usize,
) -> usize {
    let len = values.len();
    let mut idx = start;
    if idx >= len {
        return len;
    }

    let mut pattern_bytes = [0u8; 16];
    unsafe {
        let value_bytes =
            std::slice::from_raw_parts(&current as *const T::Native as *const u8, element_size);
        for chunk in pattern_bytes.chunks_mut(element_size) {
            chunk.copy_from_slice(value_bytes);
        }
    }
    let pattern = u128::from_ne_bytes(pattern_bytes);

    while idx + elements_per_chunk <= len {
        let chunk = unsafe { (values.as_ptr().add(idx) as *const u128).read_unaligned() };
        if chunk != pattern {
            for offset in 0..elements_per_chunk {
                let value = unsafe { *values.get_unchecked(idx + offset) };
                if value != current {
                    return idx + offset;
                }
            }
            unreachable!("chunk mismatch without locating differing element");
        }
        idx += elements_per_chunk;
    }

    scan_run_end_scalar::<T>(values, current, idx)
}

#[inline]
fn scan_run_end_scalar<T: ArrowPrimitiveType>(
    values: &[T::Native],
    current: T::Native,
    mut idx: usize,
) -> usize {
    let len = values.len();
    while idx < len && unsafe { *values.get_unchecked(idx) } == current {
        idx += 1;
    }
    idx
}
