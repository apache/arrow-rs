use std::{
    array::TryFromSliceError,
    ops::{Bound, RangeBounds},
};

use arrow_schema::ArrowError;

pub(crate) fn slice_from_slice<R>(bytes: &[u8], range: R) -> Result<&[u8], ArrowError>
where
    R: RangeBounds<usize>,
{
    // ----- translate RangeBounds → concrete `[start, end_exclusive)` -----
    let start = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };

    let end_exclusive = match range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1), // inclusive → exclusive
        Bound::Excluded(&e) => e,
        Bound::Unbounded => bytes.len(),
    };

    // ----- bounds check --------------------------------------------------
    if start > end_exclusive || end_exclusive > bytes.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Tried to extract {} bytes at offset {} from {}-byte buffer",
            end_exclusive.saturating_sub(start),
            start,
            bytes.len(),
        )));
    }

    // Safe: we just verified the range.
    Ok(&bytes[start..end_exclusive])
}
pub(crate) fn array_from_slice<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], ArrowError> {
    let bytes = slice_from_slice(bytes, offset..offset + N)?;
    bytes.try_into().map_err(map_try_from_slice_error)
}

/// To be used in `map_err` when unpacking an integer from a slice of bytes.
pub(crate) fn map_try_from_slice_error(e: TryFromSliceError) -> ArrowError {
    ArrowError::InvalidArgumentError(e.to_string())
}

pub(crate) fn non_empty_slice(slice: &[u8]) -> Result<&[u8], ArrowError> {
    if slice.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Received empty bytes".to_string(),
        ));
    } else {
        return Ok(slice);
    }
}

/// Constructs the error message for an invalid UTF-8 string.
pub(crate) fn invalid_utf8_err() -> ArrowError {
    ArrowError::InvalidArgumentError("invalid UTF-8 string".to_string())
}
