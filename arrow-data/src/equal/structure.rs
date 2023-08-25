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

use crate::data::{contains_nulls, ArrayData};

use super::equal_range;

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots.
fn equal_child_values(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    lhs.child_data()
        .iter()
        .zip(rhs.child_data())
        .all(|(lhs_values, rhs_values)| {
            equal_range(lhs_values, rhs_values, lhs_start, rhs_start, len)
        })
}

pub(super) fn struct_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        equal_child_values(lhs, rhs, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_nulls = lhs.nulls().unwrap();
        let rhs_nulls = rhs.nulls().unwrap();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            // if both struct and child had no null buffers,
            let lhs_is_null = lhs_nulls.is_null(lhs_pos);
            let rhs_is_null = rhs_nulls.is_null(rhs_pos);

            if lhs_is_null != rhs_is_null {
                return false;
            }

            lhs_is_null || equal_child_values(lhs, rhs, lhs_pos, rhs_pos, 1)
        })
    }
}
