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

use crate::{array::ArrayData, datatypes::DataType, datatypes::UnionMode};

use super::equal_range;

fn equal_dense(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_type_ids: &[i8],
    rhs_type_ids: &[i8],
    lhs_offsets: &[i32],
    rhs_offsets: &[i32],
) -> bool {
    let offsets = lhs_offsets.iter().zip(rhs_offsets.iter());

    lhs_type_ids
        .iter()
        .zip(rhs_type_ids.iter())
        .zip(offsets)
        .all(|((l_type_id, r_type_id), (l_offset, r_offset))| {
            let lhs_values = &lhs.child_data()[*l_type_id as usize];
            let rhs_values = &rhs.child_data()[*r_type_id as usize];

            equal_range(
                lhs_values,
                rhs_values,
                *l_offset as usize,
                *r_offset as usize,
                1,
            )
        })
}

fn equal_sparse(
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

pub(super) fn union_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_type_ids = lhs.buffer::<i8>(0);
    let rhs_type_ids = rhs.buffer::<i8>(0);

    let lhs_type_id_range = &lhs_type_ids[lhs_start..lhs_start + len];
    let rhs_type_id_range = &rhs_type_ids[rhs_start..rhs_start + len];

    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Union(_, UnionMode::Dense), DataType::Union(_, UnionMode::Dense)) => {
            let lhs_offsets = lhs.buffer::<i32>(1);
            let rhs_offsets = rhs.buffer::<i32>(1);

            let lhs_offsets_range = &lhs_offsets[lhs_start..lhs_start + len];
            let rhs_offsets_range = &rhs_offsets[rhs_start..rhs_start + len];

            lhs_type_id_range == rhs_type_id_range
                && equal_dense(
                    lhs,
                    rhs,
                    lhs_type_id_range,
                    rhs_type_id_range,
                    lhs_offsets_range,
                    rhs_offsets_range,
                )
        }
        (
            DataType::Union(_, UnionMode::Sparse),
            DataType::Union(_, UnionMode::Sparse),
        ) => {
            lhs_type_id_range == rhs_type_id_range
                && equal_sparse(lhs, rhs, lhs_start, rhs_start, len)
        }
        _ => unimplemented!(
            "Logical equality not yet implemented between dense and sparse union arrays"
        ),
    }
}
