use crate::ArrayData;
use arrow_schema::{ArrowError, DataType};

use super::{_MutableArrayData, Extend};

pub(super) fn build_extend(array: &ArrayData) -> Extend<'_> {
    let size = match array.data_type() {
        DataType::FixedSizeList(_, i) => *i as usize,
        _ => unreachable!(),
    };
    let offset = array.offset();

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let start = start.checked_add(offset).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "fixed size list offset {offset} with start {start} overflows usize"
                ))
            })?;
            let end = start.checked_add(len).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "fixed size list start {start} with length {len} overflows usize"
                ))
            })?;
            let (child_start, child_end) = start
                .checked_mul(size)
                .zip(end.checked_mul(size))
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "fixed size list range {start}..{end} with size {size} overflows usize"
                    ))
                })?;
            for child in &mut mutable.child_data {
                child.try_extend(index, child_start, child_end)?;
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) -> Result<(), ArrowError> {
    let size = match mutable.data_type {
        DataType::FixedSizeList(_, i) => i as usize,
        _ => unreachable!(),
    };

    for child in &mut mutable.child_data {
        child.try_extend_nulls(len * size)?;
    }
    Ok(())
}
