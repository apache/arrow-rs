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

use crate::array::ArrayData;

use super::{Extend, _MutableArrayData};

pub(super) fn build_extend_sparse(array: &ArrayData) -> Extend {
    let type_ids = array.buffer::<i8>(0);

    if array.null_count() == 0 {
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
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
    } else {
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                // extends type_ids
                mutable
                    .buffer1
                    .extend_from_slice(&type_ids[start..start + len]);

                (start..start + len).for_each(|i| {
                    if array.is_valid(i) {
                        mutable
                            .child_data
                            .iter_mut()
                            .for_each(|child| child.extend(index, i, i + 1))
                    } else {
                        mutable
                            .child_data
                            .iter_mut()
                            .for_each(|child| child.extend_nulls(1))
                    }
                })
            },
        )
    }
}

pub(super) fn build_extend_dense(array: &ArrayData) -> Extend {
    let type_ids = array.buffer::<i8>(0);
    let offsets = array.buffer::<i32>(1);

    if array.null_count() == 0 {
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                // extends type_ids
                mutable
                    .buffer1
                    .extend_from_slice(&type_ids[start..start + len]);
                // extends offsets
                mutable
                    .buffer2
                    .extend_from_slice(&offsets[start..start + len]);

                (start..start + len).for_each(|i| {
                    let type_id = type_ids[i] as usize;
                    let offset_start = offsets[start] as usize;

                    mutable.child_data[type_id].extend(
                        index,
                        offset_start,
                        offset_start + 1,
                    )
                })
            },
        )
    } else {
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                // extends type_ids
                mutable
                    .buffer1
                    .extend_from_slice(&type_ids[start..start + len]);
                // extends offsets
                mutable
                    .buffer2
                    .extend_from_slice(&offsets[start..start + len]);

                (start..start + len).for_each(|i| {
                    let type_id = type_ids[i] as usize;
                    let offset_start = offsets[start] as usize;

                    if array.is_valid(i) {
                        mutable.child_data[type_id].extend(
                            index,
                            offset_start,
                            offset_start + 1,
                        )
                    } else {
                        mutable.child_data[type_id].extend_nulls(1)
                    }
                })
            },
        )
    }
}

pub(super) fn extend_nulls_dense(mutable: &mut _MutableArrayData, len: usize) {
    let mut count: usize = 0;
    let num = len / mutable.child_data.len();
    mutable
        .child_data
        .iter_mut()
        .enumerate()
        .for_each(|(idx, child)| {
            let n = if count + num > len { len - count } else { num };
            count += n;
            mutable
                .buffer1
                .extend_from_slice(vec![idx as i8; n].as_slice());
            mutable
                .buffer2
                .extend_from_slice(vec![child.len() as i32; n].as_slice());
            child.extend_nulls(n)
        })
}

pub(super) fn extend_nulls_sparse(mutable: &mut _MutableArrayData, len: usize) {
    mutable
        .child_data
        .iter_mut()
        .for_each(|child| child.extend_nulls(len))
}
