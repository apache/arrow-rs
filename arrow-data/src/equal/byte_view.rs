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

use crate::{ArrayData, ByteView};

pub(super) fn byte_view_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_views = &lhs.buffer::<u128>(0)[lhs_start..lhs_start + len];
    let lhs_buffers = &lhs.buffers()[1..];
    let rhs_views = &rhs.buffer::<u128>(0)[rhs_start..rhs_start + len];
    let rhs_buffers = &rhs.buffers()[1..];

    for (idx, (l, r)) in lhs_views.iter().zip(rhs_views).enumerate() {
        // Only checking one null mask here because by the time the control flow reaches
        // this point, the equality of the two masks would have already been verified.
        if lhs.is_null(idx) {
            continue;
        }

        let l_len_prefix = *l as u64;
        let r_len_prefix = *r as u64;
        // short-circuit, check length and prefix
        if l_len_prefix != r_len_prefix {
            return false;
        }

        let len = l_len_prefix as u32;
        // for inline storage, only need check view
        if len <= 12 {
            if l != r {
                return false;
            }
            continue;
        }

        // check buffers
        let l_view = ByteView::from(*l);
        let r_view = ByteView::from(*r);

        let l_buffer = &lhs_buffers[l_view.buffer_index as usize];
        let r_buffer = &rhs_buffers[r_view.buffer_index as usize];

        // prefixes are already known to be equal; skip checking them
        let len = len as usize - 4;
        let l_offset = l_view.offset as usize + 4;
        let r_offset = r_view.offset as usize + 4;
        if l_buffer[l_offset..l_offset + len] != r_buffer[r_offset..r_offset + len] {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {}
