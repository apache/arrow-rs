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

use super::{Extend, _MutableArrayData, utils::resize_for_bits};
use crate::bit_mask::set_bits;
use crate::ArrayData;

pub(super) fn build_extend(array: &ArrayData) -> Extend {
    let values = array.buffers()[0].as_slice();
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let buffer = &mut mutable.buffer1;
            resize_for_bits(buffer, mutable.len + len);
            set_bits(
                buffer.as_slice_mut(),
                values,
                mutable.len,
                array.offset() + start,
                len,
            );
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    let buffer = &mut mutable.buffer1;
    resize_for_bits(buffer, mutable.len + len);
}
