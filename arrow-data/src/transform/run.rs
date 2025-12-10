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

use super::{_MutableArrayData, ArrayData, Extend};
use arrow_buffer::{ArrowNativeType, Buffer, ToByteSlice};
use arrow_schema::DataType;
use num_traits::CheckedAdd;

/// Generic helper to get the last run end value from a run ends array
fn get_last_run_end<T: ArrowNativeType>(run_ends_data: &super::MutableArrayData) -> T {
    if run_ends_data.data.len == 0 {
        T::default()
    } else {
        let typed_slice: &[T] = run_ends_data.data.buffer1.typed_data();
        if typed_slice.len() >= run_ends_data.data.len {
            typed_slice[run_ends_data.data.len - 1]
        } else {
            T::default()
        }
    }
}

/// Extends the `MutableArrayData` with null values.
///
/// For RunEndEncoded, this adds nulls by extending the run_ends array
/// and values array appropriately.
pub fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    if len == 0 {
        return;
    }

    // For REE, we always need to add a value entry when adding a new run
    // The values array should have one entry per run, not per logical element
    mutable.child_data[1].extend_nulls(1);

    // Determine the run end type from the data type
    let run_end_type = if let DataType::RunEndEncoded(run_ends_field, _) = &mutable.data_type {
        run_ends_field.data_type()
    } else {
        panic!("extend_nulls called on non-RunEndEncoded array");
    };

    // Use a macro to handle all run end types generically
    macro_rules! extend_nulls_impl {
        ($run_end_type:ty) => {{
            let last_run_end = get_last_run_end::<$run_end_type>(&mutable.child_data[0]);
            let new_value = last_run_end
                .checked_add(<$run_end_type as ArrowNativeType>::usize_as(len))
                .expect("run end overflow");
            mutable.child_data[0]
                .data
                .buffer1
                .extend_from_slice(new_value.to_byte_slice());
        }};
    }

    // Apply the appropriate implementation based on run end type
    match run_end_type {
        DataType::Int16 => extend_nulls_impl!(i16),
        DataType::Int32 => extend_nulls_impl!(i32),
        DataType::Int64 => extend_nulls_impl!(i64),
        _ => panic!("Invalid run end type for RunEndEncoded array: {run_end_type}"),
    };

    mutable.child_data[0].data.len += 1;
}

/// Build run ends bytes and values range directly for batch processing
fn build_extend_arrays<T: ArrowNativeType + std::ops::Add<Output = T> + CheckedAdd>(
    buffer: &Buffer,
    length: usize,
    start: usize,
    len: usize,
    dest_last_run_end: T,
) -> (Vec<u8>, Option<(usize, usize)>) {
    let mut run_ends_bytes = Vec::new();
    let mut values_range: Option<(usize, usize)> = None;
    let end = start + len;
    let mut prev_end = 0;
    let mut current_run_end = dest_last_run_end;

    // Convert buffer to typed slice once
    let typed_slice: &[T] = buffer.typed_data();

    for i in 0..length {
        if i < typed_slice.len() {
            let run_end = typed_slice[i].to_usize().unwrap();

            if prev_end <= start && run_end > start {
                let start_offset = start - prev_end;
                let end_offset = if run_end >= end {
                    end - prev_end
                } else {
                    run_end - prev_end
                };
                current_run_end = current_run_end
                    .checked_add(&T::usize_as(end_offset - start_offset))
                    .expect("run end overflow");
                run_ends_bytes.extend_from_slice(current_run_end.to_byte_slice());

                // Start the range
                values_range = Some((i, i + 1));
            } else if prev_end >= start && run_end <= end {
                current_run_end = current_run_end
                    .checked_add(&T::usize_as(run_end - prev_end))
                    .expect("run end overflow");
                run_ends_bytes.extend_from_slice(current_run_end.to_byte_slice());

                // Extend the range
                values_range = Some((values_range.expect("Unreachable: values_range cannot be None when prev_end >= start && run_end <= end. \
                           If prev_end >= start and run_end > prev_end (required for valid runs), then run_end > start, \
                           which means the first condition (prev_end <= start && run_end > start) would have been true \
                           and already set values_range to Some.").0, i + 1));
            } else if prev_end < end && run_end >= end {
                current_run_end = current_run_end
                    .checked_add(&T::usize_as(end - prev_end))
                    .expect("run end overflow");
                run_ends_bytes.extend_from_slice(current_run_end.to_byte_slice());

                // Extend the range and break
                values_range = Some((values_range.expect("Unreachable: values_range cannot be None when prev_end < end && run_end >= end. \
                           Due to sequential processing and monotonic prev_end advancement, if we reach a run \
                           that spans beyond the slice end (run_end >= end), at least one previous condition \
                           must have matched first to set values_range. Either the first condition matched when \
                           the slice started (prev_end <= start && run_end > start), or the second condition \
                           matched for runs within the slice (prev_end >= start && run_end <= end).").0, i + 1));
                break;
            }

            prev_end = run_end;
            if prev_end >= end {
                break;
            }
        } else {
            break;
        }
    }
    (run_ends_bytes, values_range)
}

/// Process extends using batch operations
fn process_extends_batch<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    source_array_idx: usize,
    run_ends_bytes: Vec<u8>,
    values_range: Option<(usize, usize)>,
) {
    if run_ends_bytes.is_empty() {
        return;
    }

    // Batch extend the run_ends array with all bytes at once
    mutable.child_data[0]
        .data
        .buffer1
        .extend_from_slice(&run_ends_bytes);
    mutable.child_data[0].data.len += run_ends_bytes.len() / std::mem::size_of::<T>();

    // Batch extend the values array using the range
    let (start_idx, end_idx) =
        values_range.expect("values_range should be Some if run_ends_bytes is not empty");
    mutable.child_data[1].extend(source_array_idx, start_idx, end_idx);
}

/// Returns a function that extends the run encoded array.
///
/// It finds the physical indices in the source array that correspond to the logical range to copy, and adjusts the runs to the logical indices of the array to extend. The values are copied from the source array to the destination array verbatim.
pub fn build_extend(array: &ArrayData) -> Extend<'_> {
    Box::new(
        move |mutable: &mut _MutableArrayData, array_idx: usize, start: usize, len: usize| {
            if len == 0 {
                return;
            }

            // We need to analyze the source array's run structure
            let source_run_ends = &array.child_data()[0];
            let source_buffer = &source_run_ends.buffers()[0];

            // Get the run end type from the mutable array
            let dest_run_end_type =
                if let DataType::RunEndEncoded(run_ends_field, _) = &mutable.data_type {
                    run_ends_field.data_type()
                } else {
                    panic!("extend called on non-RunEndEncoded mutable array");
                };

            // Build run ends and values indices directly for batch processing
            macro_rules! build_and_process_impl {
                ($run_end_type:ty) => {{
                    let dest_last_run_end =
                        get_last_run_end::<$run_end_type>(&mutable.child_data[0]);
                    let (run_ends_bytes, values_range) = build_extend_arrays::<$run_end_type>(
                        source_buffer,
                        source_run_ends.len(),
                        start,
                        len,
                        dest_last_run_end,
                    );
                    process_extends_batch::<$run_end_type>(
                        mutable,
                        array_idx,
                        run_ends_bytes,
                        values_range,
                    );
                }};
            }

            match dest_run_end_type {
                DataType::Int16 => build_and_process_impl!(i16),
                DataType::Int32 => build_and_process_impl!(i32),
                DataType::Int64 => build_and_process_impl!(i64),
                _ => panic!("Invalid run end type for RunEndEncoded array: {dest_run_end_type}",),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::MutableArrayData;
    use crate::{ArrayData, ArrayDataBuilder};
    use arrow_buffer::Buffer;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn create_run_array_data(run_ends: Vec<i32>, values: ArrayData) -> ArrayData {
        let run_ends_field = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", values.data_type().clone(), true));
        let data_type = DataType::RunEndEncoded(run_ends_field, values_field);

        let last_run_end = if run_ends.is_empty() {
            0
        } else {
            run_ends[run_ends.len() - 1] as usize
        };

        let run_ends_buffer = Buffer::from_vec(run_ends);
        let run_ends_data = ArrayDataBuilder::new(DataType::Int32)
            .len(run_ends_buffer.len() / std::mem::size_of::<i32>())
            .add_buffer(run_ends_buffer)
            .build()
            .unwrap();

        ArrayDataBuilder::new(data_type)
            .len(last_run_end)
            .add_child_data(run_ends_data)
            .add_child_data(values)
            .build()
            .unwrap()
    }

    fn create_run_array_data_int16(run_ends: Vec<i16>, values: ArrayData) -> ArrayData {
        let run_ends_field = Arc::new(Field::new("run_ends", DataType::Int16, false));
        let values_field = Arc::new(Field::new("values", values.data_type().clone(), true));
        let data_type = DataType::RunEndEncoded(run_ends_field, values_field);

        let last_run_end = if run_ends.is_empty() {
            0
        } else {
            run_ends[run_ends.len() - 1] as usize
        };

        let run_ends_buffer = Buffer::from_vec(run_ends);
        let run_ends_data = ArrayDataBuilder::new(DataType::Int16)
            .len(run_ends_buffer.len() / std::mem::size_of::<i16>())
            .add_buffer(run_ends_buffer)
            .build()
            .unwrap();

        ArrayDataBuilder::new(data_type)
            .len(last_run_end)
            .add_child_data(run_ends_data)
            .add_child_data(values)
            .build()
            .unwrap()
    }

    fn create_run_array_data_int64(run_ends: Vec<i64>, values: ArrayData) -> ArrayData {
        let run_ends_field = Arc::new(Field::new("run_ends", DataType::Int64, false));
        let values_field = Arc::new(Field::new("values", values.data_type().clone(), true));
        let data_type = DataType::RunEndEncoded(run_ends_field, values_field);

        let last_run_end = if run_ends.is_empty() {
            0
        } else {
            run_ends[run_ends.len() - 1] as usize
        };

        let run_ends_buffer = Buffer::from_vec(run_ends);
        let run_ends_data = ArrayDataBuilder::new(DataType::Int64)
            .len(run_ends_buffer.len() / std::mem::size_of::<i64>())
            .add_buffer(run_ends_buffer)
            .build()
            .unwrap();

        ArrayDataBuilder::new(data_type)
            .len(last_run_end)
            .add_child_data(run_ends_data)
            .add_child_data(values)
            .build()
            .unwrap()
    }

    fn create_int32_array_data(values: Vec<i32>) -> ArrayData {
        let buffer = Buffer::from_vec(values);
        ArrayDataBuilder::new(DataType::Int32)
            .len(buffer.len() / std::mem::size_of::<i32>())
            .add_buffer(buffer)
            .build()
            .unwrap()
    }

    fn create_string_dict_array_data(values: Vec<&str>, dict_values: Vec<&str>) -> ArrayData {
        // Create dictionary values (strings)
        let dict_offsets: Vec<i32> = dict_values
            .iter()
            .scan(0i32, |acc, s| {
                let offset = *acc;
                *acc += s.len() as i32;
                Some(offset)
            })
            .chain(std::iter::once(
                dict_values.iter().map(|s| s.len()).sum::<usize>() as i32,
            ))
            .collect();

        let dict_data: Vec<u8> = dict_values.iter().flat_map(|s| s.bytes()).collect();

        let dict_array = ArrayDataBuilder::new(DataType::Utf8)
            .len(dict_values.len())
            .add_buffer(Buffer::from_vec(dict_offsets))
            .add_buffer(Buffer::from_vec(dict_data))
            .build()
            .unwrap();

        // Create keys array
        let keys: Vec<i32> = values
            .iter()
            .map(|v| dict_values.iter().position(|d| d == v).unwrap() as i32)
            .collect();

        // Create dictionary array
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        ArrayDataBuilder::new(dict_type)
            .len(values.len())
            .add_buffer(Buffer::from_vec(keys))
            .add_child_data(dict_array)
            .build()
            .unwrap()
    }

    #[test]
    fn test_extend_nulls_int32() {
        // Create values array with one value
        let values = create_int32_array_data(vec![42]);

        // Create REE array with Int32 run ends
        let ree_array = create_run_array_data(vec![5], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], true, 10);

        mutable.extend_nulls(3);
        mutable.extend(0, 0, 5);
        mutable.extend_nulls(3);

        // Verify the run ends were extended correctly
        let result = mutable.freeze();
        let run_ends_buffer = &result.child_data()[0].buffers()[0];
        let run_ends_slice = run_ends_buffer.as_slice();

        // Should have three run ends now
        assert_eq!(result.child_data()[0].len(), 3);
        let first_run_end = i32::from_ne_bytes(run_ends_slice[0..4].try_into().unwrap());
        let second_run_end = i32::from_ne_bytes(run_ends_slice[4..8].try_into().unwrap());
        let third_run_end = i32::from_ne_bytes(run_ends_slice[8..12].try_into().unwrap());
        assert_eq!(first_run_end, 3);
        assert_eq!(second_run_end, 8);
        assert_eq!(third_run_end, 11);

        // Verify the values array was extended correctly
        assert_eq!(result.child_data()[1].len(), 3); // Should match run ends length
        let values_buffer = &result.child_data()[1].buffers()[0];
        let values_slice = values_buffer.as_slice();

        // Check the values in the buffer
        let second_value = i32::from_ne_bytes(values_slice[4..8].try_into().unwrap());

        // Second value should be the original value from the source array
        assert_eq!(second_value, 42);

        // Verify the validity buffer shows the correct null pattern
        let values_array = &result.child_data()[1];
        // First value should be null
        assert!(values_array.is_null(0));
        // Second value should be valid
        assert!(values_array.is_valid(1));
        // Third value should be null
        assert!(values_array.is_null(2));
    }

    #[test]
    fn test_extend_nulls_int16() {
        // Create values array with one value
        let values = create_int32_array_data(vec![42]);

        // Create REE array with Int16 run ends
        let ree_array = create_run_array_data_int16(vec![5i16], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], true, 10);

        // First, we need to copy the existing data
        mutable.extend(0, 0, 5);

        // Then add nulls
        mutable.extend_nulls(3);

        // Verify the run ends were extended correctly
        let result = mutable.freeze();
        let run_ends_buffer = &result.child_data()[0].buffers()[0];
        let run_ends_slice = run_ends_buffer.as_slice();

        // Should have two run ends now: original 5 and new 8 (5 + 3)
        assert_eq!(result.child_data()[0].len(), 2);
        let first_run_end = i16::from_ne_bytes(run_ends_slice[0..2].try_into().unwrap());
        let second_run_end = i16::from_ne_bytes(run_ends_slice[2..4].try_into().unwrap());
        assert_eq!(first_run_end, 5);
        assert_eq!(second_run_end, 8);
    }

    #[test]
    fn test_extend_nulls_int64() {
        // Create values array with one value
        let values = create_int32_array_data(vec![42]);

        // Create REE array with Int64 run ends
        let ree_array = create_run_array_data_int64(vec![5i64], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], true, 10);

        // First, we need to copy the existing data
        mutable.extend(0, 0, 5);

        // Then add nulls
        mutable.extend_nulls(3);

        // Verify the run ends were extended correctly
        let result = mutable.freeze();
        let run_ends_buffer = &result.child_data()[0].buffers()[0];
        let run_ends_slice = run_ends_buffer.as_slice();

        // Should have two run ends now: original 5 and new 8 (5 + 3)
        assert_eq!(result.child_data()[0].len(), 2);
        let first_run_end = i64::from_ne_bytes(run_ends_slice[0..8].try_into().unwrap());
        let second_run_end = i64::from_ne_bytes(run_ends_slice[8..16].try_into().unwrap());
        assert_eq!(first_run_end, 5);
        assert_eq!(second_run_end, 8);
    }

    #[test]
    fn test_extend_int32() {
        // Create a simple REE array with Int32 run ends
        let values = create_int32_array_data(vec![10, 20]);

        // Array: [10, 10, 20, 20, 20] (run_ends = [2, 5])
        let ree_array = create_run_array_data(vec![2, 5], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], false, 10);

        // Extend the entire array
        mutable.extend(0, 0, 5);

        let result = mutable.freeze();

        // Should have extended correctly
        assert_eq!(result.len(), 5); // All 5 elements

        // Basic validation that we have the right structure
        assert!(!result.child_data()[0].is_empty()); // Should have at least one run
        assert_eq!(result.child_data()[0].len(), result.child_data()[1].len()); // run_ends and values should have same length
    }

    #[test]
    fn test_extend_empty() {
        let values = create_int32_array_data(vec![]);
        let ree_array = create_run_array_data(vec![], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], false, 10);
        mutable.extend(0, 0, 0);

        let result = mutable.freeze();
        assert_eq!(result.len(), 0);
        assert_eq!(result.child_data()[0].len(), 0);
    }

    #[test]
    fn test_build_extend_arrays_int16() {
        let buffer = Buffer::from_vec(vec![3i16, 5i16, 8i16]);
        let (run_ends_bytes, values_range) = build_extend_arrays::<i16>(&buffer, 3, 2, 4, 0i16);

        // Logical array: [A, A, A, B, B, C, C, C]
        // Requesting indices 2-6 should give us:
        // - Part of first run (index 2) -> length 1
        // - All of second run -> length 2
        // - Part of third run -> length 1
        // Total length = 4, so run ends should be [1, 3, 4]
        assert_eq!(run_ends_bytes.len(), 3 * std::mem::size_of::<i16>());
        assert_eq!(values_range, Some((0, 3)));

        // Verify the bytes represent [1i16, 3i16, 4i16]
        let expected_bytes = [1i16, 3i16, 4i16]
            .iter()
            .flat_map(|&val| val.to_ne_bytes())
            .collect::<Vec<u8>>();
        assert_eq!(run_ends_bytes, expected_bytes);
    }

    #[test]
    fn test_build_extend_arrays_int64() {
        let buffer = Buffer::from_vec(vec![3i64, 5i64, 8i64]);
        let (run_ends_bytes, values_range) = build_extend_arrays::<i64>(&buffer, 3, 2, 4, 0i64);

        // Same logic as above but with i64
        assert_eq!(run_ends_bytes.len(), 3 * std::mem::size_of::<i64>());
        assert_eq!(values_range, Some((0, 3)));

        // Verify the bytes represent [1i64, 3i64, 4i64]
        let expected_bytes = [1i64, 3i64, 4i64]
            .iter()
            .flat_map(|&val| val.to_ne_bytes())
            .collect::<Vec<u8>>();
        assert_eq!(run_ends_bytes, expected_bytes);
    }

    #[test]
    fn test_extend_string_dict() {
        // Create a dictionary array with string values: ["hello", "world"]
        let dict_values = vec!["hello", "world"];
        let values = create_string_dict_array_data(vec!["hello", "world"], dict_values);

        // Create REE array: [hello, hello, world, world, world] (run_ends = [2, 5])
        let ree_array = create_run_array_data(vec![2, 5], values);

        let mut mutable = MutableArrayData::new(vec![&ree_array], false, 10);

        // Extend the entire array
        mutable.extend(0, 0, 5);

        let result = mutable.freeze();

        // Should have extended correctly
        assert_eq!(result.len(), 5); // All 5 elements

        // Basic validation that we have the right structure
        assert!(!result.child_data()[0].is_empty()); // Should have at least one run
        assert_eq!(result.child_data()[0].len(), result.child_data()[1].len()); // run_ends and values should have same length

        // Should have 2 runs since we have 2 different values
        assert_eq!(result.child_data()[0].len(), 2);
        assert_eq!(result.child_data()[1].len(), 2);
    }

    #[test]
    #[should_panic(expected = "run end overflow")]
    fn test_extend_nulls_overflow_i16() {
        let values = create_int32_array_data(vec![42]);
        // Start with run end close to max to set up overflow condition
        let ree_array = create_run_array_data_int16(vec![5], values);
        let mut mutable = MutableArrayData::new(vec![&ree_array], true, 10);

        // Extend the original data first to initialize state
        mutable.extend(0, 0, 5_usize);

        // This should cause overflow: i16::MAX + 5 > i16::MAX
        mutable.extend_nulls(i16::MAX as usize);
    }

    #[test]
    #[should_panic(expected = "run end overflow")]
    fn test_extend_nulls_overflow_i32() {
        let values = create_int32_array_data(vec![42]);
        // Start with run end close to max to set up overflow condition
        let ree_array = create_run_array_data(vec![10], values);
        let mut mutable = MutableArrayData::new(vec![&ree_array], true, 10);

        // Extend the original data first to initialize state
        mutable.extend(0, 0, 10_usize);

        // This should cause overflow: (i32::MAX - 10) + 20 > i32::MAX
        mutable.extend_nulls(i32::MAX as usize);
    }

    #[test]
    #[should_panic(expected = "run end overflow")]
    fn test_build_extend_overflow_i16() {
        // Create a source array with small run that will cause overflow when added
        let values = create_int32_array_data(vec![10]);
        let source_array = create_run_array_data_int16(vec![20], values);

        // Create a destination array with run end close to max
        let dest_values = create_int32_array_data(vec![42]);
        let dest_array = create_run_array_data_int16(vec![i16::MAX - 5], dest_values);

        let mut mutable = MutableArrayData::new(vec![&source_array, &dest_array], false, 10);

        // First extend the destination array to set up state
        mutable.extend(1, 0, (i16::MAX - 5) as usize);

        // This should cause overflow: (i16::MAX - 5) + 20 > i16::MAX
        mutable.extend(0, 0, 20);
    }

    #[test]
    #[should_panic(expected = "run end overflow")]
    fn test_build_extend_overflow_i32() {
        // Create a source array with small run that will cause overflow when added
        let values = create_int32_array_data(vec![10]);
        let source_array = create_run_array_data(vec![100], values);

        // Create a destination array with run end close to max
        let dest_values = create_int32_array_data(vec![42]);
        let dest_array = create_run_array_data(vec![i32::MAX - 50], dest_values);

        let mut mutable = MutableArrayData::new(vec![&source_array, &dest_array], false, 10);

        // First extend the destination array to set up state
        mutable.extend(1, 0, (i32::MAX - 50) as usize);

        // This should cause overflow: (i32::MAX - 50) + 100 > i32::MAX
        mutable.extend(0, 0, 100);
    }
}
