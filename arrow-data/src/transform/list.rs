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

use super::{
    Extend, _MutableArrayData,
    utils::{extend_offsets, get_last_offset},
};
use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use num::{CheckedAdd, Integer};

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> Extend {
    let offsets = array.buffer::<T>(0);
    if array.null_count() == 0 {
        // fast case where we can copy regions without nullability checks
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                let offset_buffer = &mut mutable.buffer1;

                // this is safe due to how offset is built. See details on `get_last_offset`
                let last_offset: T = unsafe { get_last_offset(offset_buffer) };

                // offsets
                extend_offsets::<T>(
                    offset_buffer,
                    last_offset,
                    &offsets[start..start + len + 1],
                );

                mutable.child_data[0].extend(
                    index,
                    offsets[start].as_usize(),
                    offsets[start + len].as_usize(),
                )
            },
        )
    } else {
        // nulls present: append item by item, ignoring null entries
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                let offset_buffer = &mut mutable.buffer1;

                // this is safe due to how offset is built. See details on `get_last_offset`
                let mut last_offset: T = unsafe { get_last_offset(offset_buffer) };

                let delta_len = array.len() - array.null_count();
                offset_buffer.reserve(delta_len * std::mem::size_of::<T>());

                let child = &mut mutable.child_data[0];
                (start..start + len).for_each(|i| {
                    if array.is_valid(i) {
                        // compute the new offset
                        last_offset = last_offset + offsets[i + 1] - offsets[i];

                        // append value
                        child.extend(
                            index,
                            offsets[i].as_usize(),
                            offsets[i + 1].as_usize(),
                        );
                    }
                    // append offset
                    offset_buffer.push(last_offset);
                })
            },
        )
    }
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) {
    let offset_buffer = &mut mutable.buffer1;

    // this is safe due to how offset is built. See details on `get_last_offset`
    let last_offset: T = unsafe { get_last_offset(offset_buffer) };

    (0..len).for_each(|_| offset_buffer.push(last_offset))
}
