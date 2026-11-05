use crate::data::ArrayData;

#[inline]
pub(super) fn null_equal(
    _lhs: &ArrayData,
    _rhs: &ArrayData,
    _lhs_start: usize,
    _rhs_start: usize,
    _len: usize,
) -> bool {
    // a null buffer's range is always true, as every element is by definition equal (to null).
    // We only need to compare data_types
    true
}
