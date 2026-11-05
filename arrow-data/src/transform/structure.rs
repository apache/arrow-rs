use super::{_MutableArrayData, Extend};
use crate::ArrayData;
use arrow_schema::{ArrowError, DataType};

pub(super) fn build_extend(array: &ArrayData) -> Extend<'_> {
    let offset = array.offset();
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let start = start.checked_add(offset).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "struct offset {offset} with start {start} overflows usize"
                ))
            })?;
            let end = start.checked_add(len).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "struct start {start} with length {len} overflows usize"
                ))
            })?;
            for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
                child
                    .try_extend(index, start, end)
                    .map_err(|e| wrap_column_error(e, col_idx, &mutable.data_type))?
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) -> Result<(), ArrowError> {
    for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
        child
            .try_extend_nulls(len)
            .map_err(|e| wrap_column_error(e, col_idx, &mutable.data_type))?;
    }
    Ok(())
}

fn wrap_column_error(e: ArrowError, col_idx: usize, data_type: &DataType) -> ArrowError {
    let name_ctx = if let DataType::Struct(fields) = data_type {
        fields
            .get(col_idx)
            .map(|f| format!(" (\"{}\")", f.name()))
            .unwrap_or_default()
    } else {
        String::new()
    };
    ArrowError::InvalidArgumentError(format!("struct column {col_idx}{name_ctx} failed: {e}"))
}
