use std::{array::TryFromSliceError, ops::Range};

use arrow_schema::ArrowError;

#[inline]
pub(crate) fn slice_from_slice(bytes: &[u8], range: Range<usize>) -> Result<&[u8], ArrowError> {
    bytes.get(range.clone()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract {} bytes at offset {} from {}-byte buffer",
            range.end - range.start,
            range.start,
            bytes.len(),
        ))
    })
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
