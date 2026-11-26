// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::data::{ArrayData, contains_nulls};

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
    let lhs_start = lhs_start + lhs.offset();
    let rhs_start = rhs_start + rhs.offset();

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
