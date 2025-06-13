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

use crate::{null_sentinel, RowConverter, Rows, SortField};
use arrow_array::types::RunEndIndexType;
use arrow_array::{Array, RunArray};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::{ArrowError, SortOptions};

/// Computes the lengths of each row for a RunEndEncodedArray
pub fn compute_lengths<R: RunEndIndexType>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &RunArray<R>,
) {
    // We don't need to use run_ends directly, just access through the array
    for (idx, length) in lengths.iter_mut().enumerate() {
        let physical_idx = array.get_physical_index(idx);
        let is_valid = array.values().is_valid(physical_idx);
        if is_valid {
            let row = rows.row(physical_idx);
            *length += row.data.len() + 1; // 1 for the validity byte
        } else {
            *length += 1; // just the null sentinel
        }
    }
}

/// Encodes the provided `RunEndEncodedArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded values
pub fn encode<R: RunEndIndexType>(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &RunArray<R>,
) {
    let run_ends = array.run_ends();
    let values = array.values();

    let mut logical_idx = 0;
    let mut offset_idx = 1; // Skip first offset

    // Iterate over each run
    for physical_idx in 0..run_ends.values().len() {
        let run_end = run_ends.values()[physical_idx].as_usize();
        let is_valid = values.is_valid(physical_idx);

        // Process all elements in this run
        while logical_idx < run_end && offset_idx < offsets.len() {
            let offset = &mut offsets[offset_idx];
            let out = &mut data[*offset..];

            if is_valid {
                let row = rows.row(physical_idx);
                out[0] = 1; // valid
                out[1..1 + row.data.len()].copy_from_slice(row.data);
                *offset += row.data.len() + 1;
            } else {
                out[0] = null_sentinel(opts);
                *offset += 1;
            }

            logical_idx += 1;
            offset_idx += 1;
        }

        // Break if we've processed all offsets
        if offset_idx >= offsets.len() {
            break;
        }
    }
}

/// Decodes a RunEndEncodedArray from `rows` with the provided `options`
///
/// # Safety
///
/// `rows` must contain valid data for the provided `converter`
pub unsafe fn decode<R: RunEndIndexType>(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
) -> Result<RunArray<R>, ArrowError> {
    let opts = field.options;

    // Track null values and collect row data to avoid borrow issues
    let mut valid_flags = Vec::with_capacity(rows.len());
    let mut row_data = Vec::with_capacity(rows.len());

    // First pass: collect valid flags and data for each row
    for row in rows.iter() {
        let is_valid = row[0] != null_sentinel(opts);
        valid_flags.push(is_valid);
        if is_valid {
            row_data.push(&row[1..]);
        } else {
            row_data.push(&[][..]);
        }
    }

    // Now build run ends and values
    let mut run_ends = Vec::new();
    let mut values_data = Vec::new();
    let mut current_value_idx = 0;
    let mut current_run_end = 0;

    for (idx, is_valid) in valid_flags.iter().enumerate() {
        current_run_end += 1;

        if idx == 0 {
            // Add the first row data
            values_data.push(row_data[idx]);
            run_ends.push(R::Native::usize_as(current_run_end));
            continue;
        }

        // Check if this row is different from the previous one
        let value_changed = if !is_valid {
            // Null value - check if previous was null
            !valid_flags[idx - 1]
        } else if !valid_flags[idx - 1] {
            // Previous was null, this is not
            true
        } else {
            // Both are valid, compare data
            let prev_data = row_data[idx - 1];
            let curr_data = row_data[idx];
            prev_data != curr_data
        };

        if value_changed {
            // Start a new run
            current_value_idx += 1;
            values_data.push(row_data[idx]);
            run_ends.push(R::Native::usize_as(current_run_end));
        } else {
            // Update the current run end
            run_ends[current_value_idx] = R::Native::usize_as(current_run_end);
        }
    }

    // Convert collected values to arrays
    let mut values_rows = values_data.clone();
    let values = converter.convert_raw(&mut values_rows, validate_utf8)?;

    // Create run ends array
    // Get the count of elements before we move the vector
    let element_count = run_ends.len();
    let buffer = Buffer::from_vec(run_ends);
    let run_ends_array = arrow_array::PrimitiveArray::<R>::new(
        arrow_buffer::ScalarBuffer::new(buffer, 0, element_count),
        None,
    );

    // Update rows to consume the data we've processed
    for i in 0..rows.len() {
        let row_len = if valid_flags[i] {
            1 + row_data[i].len()
        } else {
            1
        };
        rows[i] = &rows[i][row_len..];
    }

    // Create the RunEndEncodedArray
    RunArray::<R>::try_new(&run_ends_array, &values[0])
}
