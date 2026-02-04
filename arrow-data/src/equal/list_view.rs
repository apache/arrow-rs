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

use crate::ArrayData;
use crate::data::count_nulls;
use crate::equal::equal_values;
use arrow_buffer::ArrowNativeType;
use num_integer::Integer;

pub(super) fn list_view_equal<T: ArrowNativeType + Integer>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let lhs_sizes = lhs.buffer::<T>(1);

    let rhs_offsets = rhs.buffer::<T>(0);
    let rhs_sizes = rhs.buffer::<T>(1);

    let lhs_data = &lhs.child_data()[0];
    let rhs_data = &rhs.child_data()[0];

    let lhs_null_count = count_nulls(lhs.nulls(), lhs_start, len);
    let rhs_null_count = count_nulls(rhs.nulls(), rhs_start, len);

    if lhs_null_count != rhs_null_count {
        return false;
    }

    if lhs_null_count == 0 {
        // non-null pathway: all sizes must be equal, and all values must be equal
        let lhs_range_sizes = &lhs_sizes[lhs_start..lhs_start + len];
        let rhs_range_sizes = &rhs_sizes[rhs_start..rhs_start + len];

        if lhs_range_sizes.len() != rhs_range_sizes.len() {
            return false;
        }

        if lhs_range_sizes != rhs_range_sizes {
            return false;
        }

        // Check values for equality
        let lhs_range_offsets = &lhs_offsets[lhs_start..lhs_start + len];
        let rhs_range_offsets = &rhs_offsets[rhs_start..rhs_start + len];

        if lhs_range_offsets.len() != rhs_range_offsets.len() {
            return false;
        }

        for ((&lhs_offset, &rhs_offset), &size) in lhs_range_offsets
            .iter()
            .zip(rhs_range_offsets)
            .zip(lhs_range_sizes)
        {
            let lhs_offset = lhs_offset.to_usize().unwrap();
            let rhs_offset = rhs_offset.to_usize().unwrap();
            let size = size.to_usize().unwrap();

            // Check if offsets are valid for the given range
            if !equal_values(lhs_data, rhs_data, lhs_offset, rhs_offset, size) {
                return false;
            }
        }
    } else {
        // Need to integrate validity check in the inner loop.
        // non-null pathway: all sizes must be equal, and all values must be equal
        let lhs_range_sizes = &lhs_sizes[lhs_start..lhs_start + len];
        let rhs_range_sizes = &rhs_sizes[rhs_start..rhs_start + len];

        let lhs_nulls = lhs.nulls().unwrap().slice(lhs_start, len);
        let rhs_nulls = rhs.nulls().unwrap().slice(rhs_start, len);

        // Sizes can differ if values are null
        if lhs_range_sizes.len() != rhs_range_sizes.len() {
            return false;
        }

        // Check values for equality, with null checking
        let lhs_range_offsets = &lhs_offsets[lhs_start..lhs_start + len];
        let rhs_range_offsets = &rhs_offsets[rhs_start..rhs_start + len];

        if lhs_range_offsets.len() != rhs_range_offsets.len() {
            return false;
        }

        for (index, ((&lhs_offset, &rhs_offset), &size)) in lhs_range_offsets
            .iter()
            .zip(rhs_range_offsets)
            .zip(lhs_range_sizes)
            .enumerate()
        {
            let lhs_is_null = lhs_nulls.is_null(index);
            let rhs_is_null = rhs_nulls.is_null(index);

            if lhs_is_null != rhs_is_null {
                return false;
            }

            let lhs_offset = lhs_offset.to_usize().unwrap();
            let rhs_offset = rhs_offset.to_usize().unwrap();
            let size = size.to_usize().unwrap();

            // Check if values match in the range
            if !lhs_is_null && !equal_values(lhs_data, rhs_data, lhs_offset, rhs_offset, size) {
                return false;
            }
        }
    }

    true
}
