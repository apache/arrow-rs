use super::{_MutableArrayData, Extend};
use crate::ArrayData;

pub(super) fn build_extend(_: &ArrayData) -> Extend<'_> {
    Box::new(move |_, _, _, _| Ok(()))
}

pub(super) fn extend_nulls(
    _: &mut _MutableArrayData,
    _: usize,
) -> Result<(), arrow_schema::ArrowError> {
    Ok(())
}
