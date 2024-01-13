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
use arrow_buffer::{ArrowNativeType, MutableBuffer};
use num::traits::AsPrimitive;
use num::{CheckedAdd, Integer};

use super::{
    Extend, _MutableArrayData,
    utils::{extend_offsets, get_last_offset},
};

#[inline]
fn extend_offset_values<T: ArrowNativeType + AsPrimitive<usize>>(
    buffer: &mut MutableBuffer,
    offsets: &[T],
    values: &[u8],
    start: usize,
    len: usize,
) {
    let start_values = offsets[start].as_();
    let end_values = offsets[start + len].as_();
    let new_values = &values[start_values..end_values];
    buffer.extend_from_slice(new_values);
}

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd + AsPrimitive<usize>>(
    array: &ArrayData,
) -> Extend {
    let offsets = array.buffer::<T>(0);
    let values = array.buffers()[1].as_slice();
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;
            let values_buffer = &mut mutable.buffer2;

            // this is safe due to how offset is built. See details on `get_last_offset`
            let last_offset = unsafe { get_last_offset(offset_buffer) };

            extend_offsets::<T>(offset_buffer, last_offset, &offsets[start..start + len + 1]);
            // values
            extend_offset_values::<T>(values_buffer, offsets, values, start, len);
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(mutable: &mut _MutableArrayData, len: usize) {
    let offset_buffer = &mut mutable.buffer1;

    // this is safe due to how offset is built. See details on `get_last_offset`
    let last_offset: T = unsafe { get_last_offset(offset_buffer) };

    (0..len).for_each(|_| offset_buffer.push(last_offset))
}
