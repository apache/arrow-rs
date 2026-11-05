use crate::data::{ArrayData, contains_nulls};
use arrow_buffer::ArrowNativeType;

use super::equal_range;

pub(super) fn dictionary_equal<T: ArrowNativeType>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_keys = lhs.buffer::<T>(0);
    let rhs_keys = rhs.buffer::<T>(0);

    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];

    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            equal_range(
                lhs_values,
                rhs_values,
                lhs_keys[lhs_pos].to_usize().unwrap(),
                rhs_keys[rhs_pos].to_usize().unwrap(),
                1,
            )
        })
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_nulls = lhs.nulls().unwrap();
        let rhs_nulls = rhs.nulls().unwrap();
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = lhs_nulls.is_null(lhs_pos);
            let rhs_is_null = rhs_nulls.is_null(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_range(
                        lhs_values,
                        rhs_values,
                        lhs_keys[lhs_pos].to_usize().unwrap(),
                        rhs_keys[rhs_pos].to_usize().unwrap(),
                        1,
                    )
        })
    }
}
