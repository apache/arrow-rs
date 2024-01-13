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

use crate::bit_iterator::BitSliceIterator;
use crate::contains_nulls;
use crate::data::ArrayData;
use crate::equal::primitive::NULL_SLICES_SELECTIVITY_THRESHOLD;
use arrow_schema::DataType;

use super::utils::equal_len;

pub(super) fn fixed_binary_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let size = match lhs.data_type() {
        DataType::FixedSizeBinary(i) => *i as usize,
        _ => unreachable!(),
    };

    let lhs_values = &lhs.buffers()[0].as_slice()[lhs.offset() * size..];
    let rhs_values = &rhs.buffers()[0].as_slice()[rhs.offset() * size..];

    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        equal_len(
            lhs_values,
            rhs_values,
            size * lhs_start,
            size * rhs_start,
            size * len,
        )
    } else {
        let selectivity_frac = lhs.null_count() as f64 / lhs.len() as f64;

        if selectivity_frac >= NULL_SLICES_SELECTIVITY_THRESHOLD {
            // get a ref of the null buffer bytes, to use in testing for nullness
            let lhs_nulls = lhs.nulls().unwrap();
            let rhs_nulls = rhs.nulls().unwrap();
            // with nulls, we need to compare item by item whenever it is not null
            (0..len).all(|i| {
                let lhs_pos = lhs_start + i;
                let rhs_pos = rhs_start + i;

                let lhs_is_null = lhs_nulls.is_null(lhs_pos);
                let rhs_is_null = rhs_nulls.is_null(rhs_pos);

                lhs_is_null
                    || (lhs_is_null == rhs_is_null)
                        && equal_len(
                            lhs_values,
                            rhs_values,
                            lhs_pos * size,
                            rhs_pos * size,
                            size, // 1 * size since we are comparing a single entry
                        )
            })
        } else {
            let lhs_nulls = lhs.nulls().unwrap();
            let lhs_slices_iter =
                BitSliceIterator::new(lhs_nulls.validity(), lhs_start + lhs_nulls.offset(), len);
            let rhs_nulls = rhs.nulls().unwrap();
            let rhs_slices_iter =
                BitSliceIterator::new(rhs_nulls.validity(), rhs_start + rhs_nulls.offset(), len);

            lhs_slices_iter
                .zip(rhs_slices_iter)
                .all(|((l_start, l_end), (r_start, r_end))| {
                    l_start == r_start
                        && l_end == r_end
                        && equal_len(
                            lhs_values,
                            rhs_values,
                            (lhs_start + l_start) * size,
                            (rhs_start + r_start) * size,
                            (l_end - l_start) * size,
                        )
                })
        }
    }
}
