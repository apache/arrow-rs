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

use super::equal_range;

/// The current implementation of comparison of run array does support partial comparison.
/// Comparing run encoded array based on logical indices (`lhs_start`, `rhs_start`) will
/// be time consuming as converting from logical index to physical index cannot be done
/// in constat time. The current comparison compares the underlying physical arrays.
pub(super) fn run_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    if lhs_start != 0
        || rhs_start != 0
        || (lhs.len() != len && rhs.len() != len)
        || lhs.offset() > 0
        || rhs.offset() > 0
    {
        unimplemented!("Partial comparison for run array not supported.")
    }

    if lhs.len() != rhs.len() {
        return false;
    }

    // This method does validation of the lhs array and rhs array required to do its
    // function. This method does not ensure the validity of lhs and rhs array as run array.
    if lhs.child_data().len() != 2 || rhs.child_data().len() != 2 {
        panic!(
            "Run array should have exactly two child arrays. lhs has {}, rhs has {}",
            lhs.child_data().len(),
            rhs.child_data().len()
        )
    }

    let lhs_run_ends_array = lhs.child_data().get(0).unwrap();
    let lhs_values_array = lhs.child_data().get(1).unwrap();

    let rhs_run_ends_array = rhs.child_data().get(0).unwrap();
    let rhs_values_array = rhs.child_data().get(1).unwrap();

    if lhs_run_ends_array.len() != rhs_run_ends_array.len() {
        return false;
    }

    if lhs_values_array.len() != rhs_values_array.len() {
        return false;
    }

    // check run ends array are equal. The length of the physical array
    // is used to validate the child arrays.
    let run_ends_equal = equal_range(
        lhs_run_ends_array,
        rhs_run_ends_array,
        lhs_start,
        rhs_start,
        lhs_run_ends_array.len(),
    );

    // if run ends array are not the same return early without validating
    // values array.
    if !run_ends_equal {
        return false;
    }

    // check values array are equal
    equal_range(
        lhs_values_array,
        rhs_values_array,
        lhs_start,
        rhs_start,
        rhs_values_array.len(),
    )
}
