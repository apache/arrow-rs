use num::Integer;
use arrow_buffer::ArrowNativeType;
use crate::ArrayData;
use crate::data::count_nulls;

use super::equal_range;

pub(super) fn list_view_equal<T: ArrowNativeType + Integer>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);
    let lhs_sizes = lhs.buffer::<T>(1);
    let rhs_sizes = rhs.buffer::<T>(1);
    for i in 0..len {

        // compare offsets
        let lhs_pos = lhs_start + i;
        let rhs_pos = rhs_start + i;
        let lhs_offset_start = lhs_offsets[lhs_pos].to_usize().unwrap();
        let rhs_offset_start = rhs_offsets[rhs_pos].to_usize().unwrap();
        let lhs_len = lhs_sizes[lhs_pos].to_usize().unwrap();
        let rhs_len = rhs_sizes[rhs_pos].to_usize().unwrap();
        if lhs_len != rhs_len {
            return false;
        }

        // compare nulls
        let lhs_null_count = count_nulls(lhs.nulls(), lhs_offset_start, lhs_len);
        let rhs_null_count = count_nulls(rhs.nulls(), rhs_offset_start, rhs_len);
        if lhs_null_count != rhs_null_count {
            return false;
        }

        // compare values
        if !equal_range(
            &lhs.child_data()[0],
            &rhs.child_data()[0],
            lhs_offset_start,
            rhs_offset_start,
            lhs_len,
        ) {
            return false;
        }
    }
    true
}
