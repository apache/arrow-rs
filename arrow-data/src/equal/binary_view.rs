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

use crate::view::View;
use crate::ArrayData;

pub(super) fn binary_view_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_views = &lhs.buffer::<u128>(0)[lhs_start..lhs_start + len];
    let lhs_b = lhs.buffers();
    let rhs_views = &rhs.buffer::<u128>(0)[rhs_start..rhs_start + len];
    let rhs_b = rhs.buffers();

    for (idx, (l, r)) in lhs_views.iter().zip(rhs_views).enumerate() {
        // Only checking one null mask here because by the time the control flow reaches
        // this point, the equality of the two masks would have already been verified.
        if lhs.is_null(idx) {
            continue;
        }

        let l_len = *l as u32;
        let r_len = *r as u32;
        if l_len != r_len {
            return false;
        } else if l_len <= 12 {
            // Inline storage
            if l != r {
                return false;
            }
        } else {
            let l_view = View::from(*l);
            let r_view = View::from(*r);
            let l_b = &lhs_b[(l_view.buffer_index as usize) + 1];
            let r_b = &rhs_b[(r_view.buffer_index as usize) + 1];

            let l_o = l_view.offset as usize;
            let r_o = r_view.offset as usize;
            let len = l_len as usize;
            if l_b[l_o..l_o + len] != r_b[r_o..r_o + len] {
                return false;
            }
        }
    }
    true
}
