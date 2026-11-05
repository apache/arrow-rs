use crate::data::{ArrayData, contains_nulls};
use arrow_schema::DataType;

use super::equal_range;

pub(super) fn fixed_list_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let size = match lhs.data_type() {
        DataType::FixedSizeList(_, i) => *i as usize,
        _ => unreachable!(),
    };

    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];

    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        equal_range(
            lhs_values,
            rhs_values,
            (lhs_start + lhs.offset()) * size,
            (rhs_start + rhs.offset()) * size,
            size * len,
        )
    } else {
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
                    && equal_range(
                        lhs_values,
                        rhs_values,
                        (lhs_pos + lhs.offset()) * size,
                        (rhs_pos + rhs.offset()) * size,
                        size, // 1 * size since we are comparing a single entry
                    )
        })
    }
}
