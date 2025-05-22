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
    let lhs_nulls = lhs.nulls();
    let rhs_nulls = rhs.nulls();
    for i in 0..len {
        let lhs_pos = lhs_start + i;
        let rhs_pos = rhs_start + i;

        // get offset and size
        let lhs_offset_start = lhs_offsets[lhs_pos].to_usize().unwrap();
        let rhs_offset_start = rhs_offsets[rhs_pos].to_usize().unwrap();
        let lhs_size = lhs_sizes[lhs_pos].to_usize().unwrap();
        let rhs_size = rhs_sizes[rhs_pos].to_usize().unwrap();

        if lhs_size != rhs_size {
            return false;
        }

        // check if null
        if let (Some(lhs_null), Some(rhs_null)) = (lhs_nulls, rhs_nulls) {
            if lhs_null.is_null(lhs_pos) != rhs_null.is_null(rhs_pos) {
                return false;
            }
            if lhs_null.is_null(lhs_pos) {
                continue;
            }
        }

        // compare values
        if !equal_range(
            &lhs.child_data()[0],
            &rhs.child_data()[0],
            lhs_offset_start,
            rhs_offset_start,
            lhs_size,
        ) {
            return false;
        }
    }
    true
}
