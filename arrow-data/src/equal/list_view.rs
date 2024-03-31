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

use crate::data::count_nulls;
use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use num::Integer;

use super::equal_range;

pub(super) fn list_view_equal<T: ArrowNativeType + Integer>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);
    let lhs_sizes = lhs.buffer::<T>(1);
    let rhs_sizes = rhs.buffer::<T>(1);
    for i in 0..len {
        // compare offsets and sizes
        let lhs_pos = lhs_start + i;
        let rhs_pos = rhs_start + i;
        let lhs_offset_start = lhs_offsets[lhs_pos].to_usize().unwrap();
        let rhs_offset_start = rhs_offsets[rhs_pos].to_usize().unwrap();
        let lhs_size = lhs_sizes[lhs_pos].to_usize().unwrap();
        let rhs_size = rhs_sizes[rhs_pos].to_usize().unwrap();
        if lhs_size != rhs_size {
            return false;
        }
        // compare nulls
        let lhs_null_count = count_nulls(lhs.nulls(), lhs_offset_start, lhs_size);
        let rhs_null_count = count_nulls(rhs.nulls(), rhs_offset_start, lhs_size);
        if lhs_null_count != rhs_null_count {
            return false;
        }
        // compare values
        if !equal_range(
            &lhs.child_data()[0],
            &rhs.child_data()[0],
            lhs_offset_start,
            lhs_offset_start,
            lhs_size,
        ) {
            return false;
        }
    }
    true
}
