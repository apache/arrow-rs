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

use crate::data::ArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::RunEndBuffer;
use arrow_schema::DataType;
use num_traits::ToPrimitive;

use super::equal_range;

/// Returns true if the two `RunEndEncoded` arrays are equal.
///
/// This provides a specialized implementation of equality for REE arrays that
/// handles differences in run-encoding by iterating through the logical range.
pub(super) fn run_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_index_type = match lhs.data_type() {
        DataType::RunEndEncoded(f, _) => f.data_type(),
        _ => unreachable!(),
    };

    match lhs_index_type {
        DataType::Int16 => run_equal_inner::<i16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int32 => run_equal_inner::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int64 => run_equal_inner::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        _ => unreachable!(),
    }
}

struct RunArrayData<'a, T: ArrowNativeType> {
    run_ends: RunEndBuffer<T>,
    values: &'a ArrayData,
}

impl<'a, T: ArrowNativeType + ToPrimitive> RunArrayData<'a, T> {
    fn new(data: &'a ArrayData, start: usize, len: usize) -> Self {
        debug_assert!(
            data.child_data().len() == 2,
            "RunEndEncoded arrays are guaranteed to have 2 children [run_ends, values]"
        );
        let run_ends_data = &data.child_data()[0];
        let raw_run_ends_buffer = &run_ends_data.buffers()[0];
        // SAFETY: we're reconstructing RunEndBuffer from a known valid RunArray
        let run_ends = unsafe {
            RunEndBuffer::<T>::new_unchecked(
                raw_run_ends_buffer.clone().into(),
                run_ends_data.offset() + data.offset() + start,
                len,
            )
        };

        let values = &data.child_data()[1];
        Self { run_ends, values }
    }

    fn run_end(&self, index: usize) -> usize {
        self.run_ends.values()[index].as_usize()
    }

    fn get_start_end_physical_indices(&self) -> (usize, usize) {
        let start = self.run_ends.get_start_physical_index();
        let end = self.run_ends.get_end_physical_index();
        (start, end)
    }
}

fn run_equal_inner<T: ArrowNativeType + ToPrimitive>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    if len == 0 {
        return true;
    }

    let l_array = RunArrayData::<T>::new(lhs, lhs_start, len);
    let r_array = RunArrayData::<T>::new(rhs, rhs_start, len);

    let (l_start_phys, l_end_phys) = l_array.get_start_end_physical_indices();
    let (r_start_phys, r_end_phys) = r_array.get_start_end_physical_indices();
    let l_runs = l_end_phys - l_start_phys + 1;
    let r_runs = r_end_phys - r_start_phys + 1;

    if l_runs == r_runs {
        // When the boundaries align perfectly, we don't need the complex stepping loop that calculates overlaps.
        // Instead, we can simply treat the underlying values arrays as if they were standard primitive arrays.
        let l_iter = l_array.run_ends.sliced_values();
        let r_iter = r_array.run_ends.sliced_values();
        let physical_match = l_iter.zip(r_iter).all(|(l_re, r_re)| l_re == r_re);

        if physical_match {
            // Both arrays are partitioned identically.
            // We can just verify if the physical values in those partitions match.
            return equal_range(
                l_array.values,
                r_array.values,
                l_start_phys,
                r_start_phys,
                l_runs,
            );
        }
    }

    let mut l_phys = l_start_phys;
    let mut r_phys = r_start_phys;
    let mut processed = 0;
    while processed < len {
        if !equal_range(l_array.values, r_array.values, l_phys, r_phys, 1) {
            return false;
        }

        let l_run_end = l_array.run_end(l_phys);
        let r_run_end = r_array.run_end(r_phys);

        //Calculate how many more logical elements are in the current run of the left and right array
        let l_remaining_in_run = l_run_end - (l_array.run_ends.offset() + processed);
        let r_remaining_in_run = r_run_end - (r_array.run_ends.offset() + processed);

        //Calculate how many elements are left to compare in the requested range
        let remaining_in_range = len - processed;

        //Find the smallest of these three to determine our step size
        //The goal is to move the logical cursor (processed) forward as far as possible without:
        //Crossing the boundary of a run in the left or right array (where the value might change).
        //Going past the total length we were asked to compare.
        let step = l_remaining_in_run
            .min(r_remaining_in_run)
            .min(remaining_in_range);
        processed += step;

        if l_array.run_ends.offset() + processed == l_run_end {
            l_phys += 1;
        }
        if r_array.run_ends.offset() + processed == r_run_end {
            r_phys += 1;
        }
    }

    true
}
