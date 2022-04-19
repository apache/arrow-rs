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

use crate::{
    array::ArrayData,
    array::{data::count_nulls, OffsetSizeTrait},
    buffer::Buffer,
    util::bit_util::get_bit,
};

use super::equal_range;

fn lengths_equal<T: OffsetSizeTrait>(lhs: &[T], rhs: &[T]) -> bool {
    // invariant from `base_equal`
    debug_assert_eq!(lhs.len(), rhs.len());

    if lhs.is_empty() {
        return true;
    }

    if lhs[0] == T::zero() && rhs[0] == T::zero() {
        return lhs == rhs;
    };

    // The expensive case, e.g.
    // [0, 2, 4, 6, 9] == [4, 6, 8, 10, 13]
    lhs.windows(2)
        .zip(rhs.windows(2))
        .all(|(lhs_offsets, rhs_offsets)| {
            // length of left == length of right
            (lhs_offsets[1] - lhs_offsets[0]) == (rhs_offsets[1] - rhs_offsets[0])
        })
}

pub(super) fn list_equal<T: OffsetSizeTrait>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);

    // There is an edge-case where a n-length list that has 0 children, results in panics.
    // For example; an array with offsets [0, 0, 0, 0, 0] has 4 slots, but will have
    // no valid children.
    // Under logical equality, the child null bitmap will be an empty buffer, as there are
    // no child values. This causes panics when trying to count set bits.
    //
    // We caught this by chance from an accidental test-case, but due to the nature of this
    // crash only occurring on list equality checks, we are adding a check here, instead of
    // on the buffer/bitmap utilities, as a length check would incur a penalty for almost all
    // other use-cases.
    //
    // The solution is to check the number of child values from offsets, and return `true` if
    // they = 0. Empty arrays are equal, so this is correct.
    //
    // It's unlikely that one would create a n-length list array with no values, where n > 0,
    // however, one is more likely to slice into a list array and get a region that has 0
    // child values.
    // The test that triggered this behaviour had [4, 4] as a slice of 1 value slot.
    let lhs_child_length = lhs_offsets.get(len).unwrap().to_usize().unwrap()
        - lhs_offsets.first().unwrap().to_usize().unwrap();
    let rhs_child_length = rhs_offsets.get(len).unwrap().to_usize().unwrap()
        - rhs_offsets.first().unwrap().to_usize().unwrap();

    if lhs_child_length == 0 && lhs_child_length == rhs_child_length {
        return true;
    }

    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count != rhs_null_count {
        return false;
    }

    if lhs_null_count == 0 && rhs_null_count == 0 {
        lengths_equal(
            &lhs_offsets[lhs_start..lhs_start + len],
            &rhs_offsets[rhs_start..rhs_start + len],
        ) && equal_range(
            lhs_values,
            rhs_values,
            lhs_values.null_buffer(),
            rhs_values.null_buffer(),
            lhs_offsets[lhs_start].to_usize().unwrap(),
            rhs_offsets[rhs_start].to_usize().unwrap(),
            (lhs_offsets[lhs_start + len] - lhs_offsets[lhs_start])
                .to_usize()
                .unwrap(),
        )
    } else {
        // get a ref of the parent null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs_nulls.unwrap().as_slice();
        let rhs_null_bytes = rhs_nulls.unwrap().as_slice();

        // with nulls, we need to compare item by item whenever it is not null
        // TODO: Could potentially compare runs of not NULL values
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !get_bit(lhs_null_bytes, lhs_pos + lhs.offset());
            let rhs_is_null = !get_bit(rhs_null_bytes, rhs_pos + rhs.offset());

            if lhs_is_null != rhs_is_null {
                return false;
            }

            let lhs_offset_start = lhs_offsets[lhs_pos].to_usize().unwrap();
            let lhs_offset_end = lhs_offsets[lhs_pos + 1].to_usize().unwrap();
            let rhs_offset_start = rhs_offsets[rhs_pos].to_usize().unwrap();
            let rhs_offset_end = rhs_offsets[rhs_pos + 1].to_usize().unwrap();

            let lhs_len = lhs_offset_end - lhs_offset_start;
            let rhs_len = rhs_offset_end - rhs_offset_start;

            lhs_is_null
                || (lhs_len == rhs_len
                    && equal_range(
                        lhs_values,
                        rhs_values,
                        lhs_values.null_buffer(),
                        rhs_values.null_buffer(),
                        lhs_offset_start,
                        rhs_offset_start,
                        lhs_len,
                    ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Int64Builder, ListBuilder};

    #[test]
    fn list_array_non_zero_nulls() {
        // Tests handling of list arrays with non-empty null ranges
        let mut builder = ListBuilder::new(Int64Builder::new(10));
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.values().append_value(3).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let array1 = builder.finish();

        let mut builder = ListBuilder::new(Int64Builder::new(10));
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.values().append_value(3).unwrap();
        builder.append(true).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.append(false).unwrap();
        let array2 = builder.finish();

        assert_eq!(array1, array2);
    }
}
