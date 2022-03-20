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

use crate::datatypes::Field;
use crate::{
    array::data::count_nulls, array::ArrayData, buffer::Buffer, datatypes::DataType,
    datatypes::UnionMode, util::bit_util::get_bit,
};
use std::any::Any;
use std::collections::HashSet;

use super::{equal_range, utils::child_logical_null_buffer};

// Checks if corresponding slots in two UnionArrays are same data types
fn equal_types(
    lhs_fields: &Vec<Field>,
    rhs_fields: &Vec<Field>,
    lhs_type_ids: &[i8],
    rhs_type_ids: &[i8],
) -> bool {
    let lhs_slots_types = lhs_type_ids
        .into_iter()
        .map(|type_id| lhs_fields.get(*type_id as usize).unwrap().data_type())
        .collect_vec();

    let rhs_slots_types = rhs_type_ids
        .into_iter()
        .map(|type_id| rhs_fields.get(*type_id as usize).unwrap().data_type())
        .collect_vec();

    lhs_slots_types
        .into_iter()
        .zip(rhs_slots_types.into_iter())
        .all(|(l, r)| l == r)
}

fn equal_dense(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_type_ids: &[i8],
    rhs_type_ids: &[i8],
    lhs_offsets: &[i32],
    rhs_offsets: &[i32],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let offsets = lhs_offsets.into_iter().zip(rhs_offsets.into_iter());


    lhs_type_ids.into_iter().zip(rhs_type_ids.into_iter()).zip(offsets).all(
        |((l_type_id, r_type_id), (l_offset, r_offset))| {
            let lhs_values = &lhs.child_data()[*l_type_id as usize];
            let rhs_values = &rhs.child_data()[*r_type_id as usize];

            let lhs_pos = lhs_start + *l_offset;
            let rhs_pos = rhs_start + *r_offset;

            // if both struct and child had no null buffers,
            let lhs_is_null = !get_bit(lhs_null_bytes, lhs_pos + lhs.offset());
            let rhs_is_null = !get_bit(rhs_null_bytes, rhs_pos + rhs.offset());

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_pos, rhs_pos, 1)
        },
    )
}

fn equal_sparse(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_type_ids: &[i8],
    rhs_type_ids: &[i8],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let mut visited_type_ids: HashSet<i8> = HashSet::new();

    lhs_type_ids.into_iter().zip(rhs_type_ids.into_iter()).all(
        |(l_type_id, r_type_id)| {
            if visited_type_ids.len() < lhs.child_data().len() {
                let lhs_values = &lhs.child_data()[*l_type_id as usize];
                let rhs_values = &rhs.child_data()[*r_type_id as usize];

                // merge the null data
                let lhs_merged_nulls =
                    child_logical_null_buffer(lhs, lhs_nulls, lhs_values);
                let rhs_merged_nulls =
                    child_logical_null_buffer(rhs, rhs_nulls, rhs_values);
                let comp_result = equal_range(
                    lhs_values,
                    rhs_values,
                    lhs_merged_nulls.as_ref(),
                    rhs_merged_nulls.as_ref(),
                    lhs_start,
                    rhs_start,
                    len,
                );

                visited_type_ids.insert(*l_type_id);

                comp_result
            } else {
                true
            }
        },
    )
}

pub(super) fn union_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_type_ids = lhs.buffer::<i8>(0);
    let rhs_type_ids = rhs.buffer::<i8>(0);

    let lhs_offsets = lhs.buffer::<i32>(1);
    let rhs_offsets = rhs.buffer::<i32>(1);

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    match (lhs.data_type(), rhs.data_type()) {
        (
            DataType::Union(lhs_fields, UnionMode::Dense),
            DataType::Union(rhs_fields, UnionMode::Dense),
        ) => {
            equal_types(lhs_fields, rhs_fields, lhs_type_ids, rhs_type_ids)
                && equal_dense(
                    lhs,
                    rhs,
                    lhs_nulls,
                    rhs_nulls,
                    lhs_type_ids,
                    rhs_type_ids,
                    lhs_offsets,
                    rhs_offsets
                    lhs_start,
                    rhs_start,
                    len,
                )
        }
        (
            DataType::Union(lhs_fields, UnionMode::Sparse),
            DataType::Union(rhs_fields, UnionMode::Sparse),
        ) => {
            equal_types(lhs_fields, rhs_fields, lhs_type_ids, rhs_type_ids)
                && equal_sparse(
                    lhs,
                    rhs,
                    lhs_nulls,
                    rhs_nulls,
                    lhs_type_ids,
                    rhs_type_ids,
                    lhs_start,
                    rhs_start,
                    len,
                )
        }
        (DataType::Union(lhs_fields, _), DataType::Union(rhs_fields, _)) => {
            equal_types(lhs_fields, rhs_fields, lhs_type_ids, rhs_type_ids)
        }
        _ => unreachable!(),
    }

    true
}
