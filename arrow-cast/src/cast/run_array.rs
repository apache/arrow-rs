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
use arrow_ord::partition::partition;

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

    // Partition the array to identify runs of consecutive equal values
    let partitions = partition(&[Arc::clone(cast_array)])?;
    let size = partitions.len();
    let mut run_ends = Vec::with_capacity(size);
    let mut values_indexes = Vec::with_capacity(size);
    let mut last_partition_end = 0;
    for partition in partitions.ranges() {
        values_indexes.push(last_partition_end);
        run_ends.push(partition.end);
        last_partition_end = partition.end;
    }

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
