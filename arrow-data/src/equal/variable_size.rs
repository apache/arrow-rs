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

use super::utils::equal_len;
use crate::data::{ArrayData, contains_nulls};
use crate::equal::list::lengths_equal;
use arrow_buffer::ArrowNativeType;
use num_integer::Integer;

fn offset_value_equal<T: ArrowNativeType + Integer>(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_offsets: &[T],
    rhs_offsets: &[T],
    lhs_pos: usize,
    rhs_pos: usize,
    len: usize,
) -> bool {
    let lhs_start = lhs_offsets[lhs_pos].as_usize();
    let rhs_start = rhs_offsets[rhs_pos].as_usize();
    let lhs_len = (lhs_offsets[lhs_pos + len] - lhs_offsets[lhs_pos])
        .to_usize()
        .unwrap();
    let rhs_len = (rhs_offsets[rhs_pos + len] - rhs_offsets[rhs_pos])
        .to_usize()
        .unwrap();

    if lhs_len == 0 && rhs_len == 0 {
        return true;
    }

    lhs_len == rhs_len && equal_len(lhs_values, rhs_values, lhs_start, rhs_start, lhs_len)
}

pub(super) fn variable_sized_equal<T: ArrowNativeType + Integer>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);

    // the offsets of the `ArrayData` are ignored as they are only applied to the offset buffer.
    let lhs_values = lhs.buffers()[1].as_slice();
    let rhs_values = rhs.buffers()[1].as_slice();

    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        let lhs_offsets_slice = &lhs_offsets[lhs_start..lhs_start + len + 1];
        let rhs_offsets_slice = &rhs_offsets[rhs_start..rhs_start + len + 1];
        lengths_equal(lhs_offsets_slice, rhs_offsets_slice)
            && offset_value_equal(
                lhs_values,
                rhs_values,
                lhs_offsets,
                rhs_offsets,
                lhs_start,
                rhs_start,
                len,
            )
    } else {
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            // the null bits can still be `None`, indicating that the value is valid.
            let lhs_is_null = lhs.nulls().map(|v| v.is_null(lhs_pos)).unwrap_or_default();
            let rhs_is_null = rhs.nulls().map(|v| v.is_null(rhs_pos)).unwrap_or_default();

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && offset_value_equal(
                        lhs_values,
                        rhs_values,
                        lhs_offsets,
                        rhs_offsets,
                        lhs_pos,
                        rhs_pos,
                        1,
                    )
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ArrayData;
    use crate::equal::variable_size::variable_sized_equal;
    use arrow_buffer::Buffer;
    use arrow_schema::DataType;

    #[test]
    fn test_variable_sized_equal_diff_offsets() {
        let a = ArrayData::builder(DataType::Utf8)
            .buffers(vec![
                Buffer::from_vec(vec![0_i32, 3, 6]),
                Buffer::from_slice_ref(b"foobar"),
            ])
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b01_u8])))
            .len(2)
            .build()
            .unwrap();
        let b = ArrayData::builder(DataType::Utf8)
            .buffers(vec![
                Buffer::from_vec(vec![0_i32, 2, 6]),
                Buffer::from_slice_ref(b"foobar"),
            ])
            .null_bit_buffer(Some(Buffer::from_slice_ref([0b01_u8])))
            .len(2)
            .build()
            .unwrap();

        assert!(!variable_sized_equal::<i32>(&a, &b, 0, 0, 2));
    }
}
