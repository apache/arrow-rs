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

use super::{Extend, _MutableArrayData};
use crate::ArrayData;

pub(super) fn build_extend_sparse(array: &ArrayData) -> Extend {
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

pub(super) fn build_extend_dense(array: &ArrayData) -> Extend {
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

pub(super) fn extend_nulls_dense(_mutable: &mut _MutableArrayData, _len: usize) {
    panic!("cannot call extend_nulls on UnionArray as cannot infer type");
}

pub(super) fn extend_nulls_sparse(_mutable: &mut _MutableArrayData, _len: usize) {
    panic!("cannot call extend_nulls on UnionArray as cannot infer type");
}
