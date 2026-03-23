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

use super::{_MutableArrayData, Extend};
use crate::ArrayData;
use arrow_schema::DataType;

pub(super) fn build_extend_sparse(array: &ArrayData) -> Extend<'_> {
    let type_ids = array.buffer::<i8>(0);

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            // extends type_ids
            mutable
                .buffer1
                .extend_from_slice(&type_ids[start..start + len]);

            mutable
                .child_data
                .iter_mut()
                .for_each(|child| child.extend(index, start, start + len))
        },
    )
}

pub(super) fn build_extend_dense(array: &ArrayData) -> Extend<'_> {
    let type_ids = array.buffer::<i8>(0);
    let offsets = array.buffer::<i32>(1);
    let arrow_schema::DataType::Union(src_fields, _) = array.data_type() else {
        unreachable!();
    };

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            // extends type_ids
            mutable
                .buffer1
                .extend_from_slice(&type_ids[start..start + len]);

            (start..start + len).for_each(|i| {
                let type_id = type_ids[i];
                let child_index = src_fields
                    .iter()
                    .position(|(r, _)| r == type_id)
                    .expect("invalid union type ID");
                let src_offset = offsets[i] as usize;
                let child_data = &mut mutable.child_data[child_index];
                let dst_offset = child_data.len();

                // Extend offsets
                mutable.buffer2.push(dst_offset as i32);
                mutable.child_data[child_index].extend(index, src_offset, src_offset + 1)
            })
        },
    )
}

pub(super) fn extend_nulls_dense(mutable: &mut _MutableArrayData, len: usize) {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let first_type_id = fields
        .iter()
        .next()
        .expect("union must have at least one field")
        .0;

    // Extend type_ids buffer
    mutable.buffer1.extend_from_slice(&vec![first_type_id; len]);

    // Dense: extend offsets pointing into the first child, then extend nulls in that child
    let child_offset = mutable.child_data[0].len();
    mutable
        .buffer2
        .extend((0..len).map(|i| (child_offset + i) as i32));
    mutable.child_data[0].extend_nulls(len);
}

pub(super) fn extend_nulls_sparse(mutable: &mut _MutableArrayData, len: usize) {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let first_type_id = fields
        .iter()
        .next()
        .expect("union must have at least one field")
        .0;

    // Extend type_ids buffer
    mutable.buffer1.extend_from_slice(&vec![first_type_id; len]);

    // Sparse: extend nulls in ALL children
    mutable
        .child_data
        .iter_mut()
        .for_each(|child| child.extend_nulls(len));
}
