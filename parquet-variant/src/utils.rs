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
use std::{array::TryFromSliceError, ops::Range, str};

use crate::VariantPathElement;
use arrow_schema::ArrowError;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::slice::SliceIndex;

/// Helper for reporting integer overflow errors in a consistent way.
pub(crate) fn overflow_error(msg: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Integer overflow computing {msg}"))
}

#[inline]
pub(crate) fn slice_from_slice<I: SliceIndex<[u8]> + Clone + Debug>(
    bytes: &[u8],
    index: I,
) -> Result<&I::Output, ArrowError> {
    bytes.get(index.clone()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {index:?} from {}-byte buffer",
            bytes.len(),
        ))
    })
}

/// Helper to safely slice bytes with offset calculations.
///
/// Equivalent to `slice_from_slice(bytes, (base_offset + range.start)..(base_offset + range.end))`
/// but using checked addition to prevent integer overflow panics on 32-bit systems.
#[inline]
pub(crate) fn slice_from_slice_at_offset(
    bytes: &[u8],
    base_offset: usize,
    range: Range<usize>,
) -> Result<&[u8], ArrowError> {
    let start_byte = base_offset
        .checked_add(range.start)
        .ok_or_else(|| overflow_error("slice start"))?;
    let end_byte = base_offset
        .checked_add(range.end)
        .ok_or_else(|| overflow_error("slice end"))?;
    slice_from_slice(bytes, start_byte..end_byte)
}

pub(crate) fn array_from_slice<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], ArrowError> {
    slice_from_slice_at_offset(bytes, offset, 0..N)?
        .try_into()
        .map_err(|e: TryFromSliceError| ArrowError::InvalidArgumentError(e.to_string()))
}

pub(crate) fn first_byte_from_slice(slice: &[u8]) -> Result<u8, ArrowError> {
    slice
        .first()
        .copied()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Helper to get a &str from a slice at the given offset and range, or an error if it contains invalid UTF-8 data.
#[inline]
pub(crate) fn string_from_slice(
    slice: &[u8],
    offset: usize,
    range: Range<usize>,
) -> Result<&str, ArrowError> {
    let offset_buffer = slice_from_slice_at_offset(slice, offset, range)?;

    //Use simdutf8 by default
    #[cfg(feature = "simdutf8")]
    {
        simdutf8::basic::from_utf8(offset_buffer).map_err(|_| {
            // Use simdutf8::compat to return details about the decoding error
            let e = simdutf8::compat::from_utf8(offset_buffer).unwrap_err();
            ArrowError::InvalidArgumentError(format!("encountered non UTF-8 data: {e}"))
        })
    }

    //Use std::str if simdutf8 is not enabled
    #[cfg(not(feature = "simdutf8"))]
    str::from_utf8(offset_buffer)
        .map_err(|_| ArrowError::InvalidArgumentError("invalid UTF-8 string".to_string()))
}

/// Performs a binary search over a range using a fallible key extraction function; a failed key
/// extraction immediately terminats the search.
///
/// This is similar to the standard library's `binary_search_by`, but generalized to ranges instead
/// of slices.
///
/// # Arguments
/// * `range` - The range to search in
/// * `target` - The target value to search for
/// * `key_extractor` - A function that extracts a comparable key from slice elements.
///   This function can fail and return None.
///
/// # Returns
/// * `Some(Ok(index))` - Element found at the given index
/// * `Some(Err(index))` - Element not found, but would be inserted at the given index
/// * `None` - Key extraction failed
pub(crate) fn try_binary_search_range_by<F>(
    range: Range<usize>,
    cmp: F,
) -> Option<Result<usize, usize>>
where
    F: Fn(usize) -> Option<Ordering>,
{
    let Range { mut start, mut end } = range;
    while start < end {
        let mid = start + (end - start) / 2;
        match cmp(mid)? {
            Ordering::Equal => return Some(Ok(mid)),
            Ordering::Greater => end = mid,
            Ordering::Less => start = mid + 1,
        }
    }

    Some(Err(start))
}

/// Verifies the expected size of type T, for a type that should only grow if absolutely necessary.
#[allow(unused)]
pub(crate) const fn expect_size_of<T>(expected: usize) {
    let size = std::mem::size_of::<T>();
    if size != expected {
        let _ = [""; 0][size];
    }
}

pub(crate) fn fits_precision<const N: u32>(n: impl Into<i64>) -> bool {
    n.into().unsigned_abs().leading_zeros() >= (i64::BITS - N)
}

// Helper fn to parse input segments like foo[0] or foo[0][0]
#[inline]
pub(crate) fn parse_path<'a>(segment: &'a str) -> Vec<VariantPathElement<'a>> {
    if segment.is_empty() {
        return Vec::new();
    }

    let mut path_elements = Vec::new();
    let mut base = segment;

    while let Some(stripped) = base.strip_suffix(']') {
        let Some(open_pos) = stripped.rfind('[') else {
            return vec![VariantPathElement::field(segment)];
        };

        let index_str = &stripped[open_pos + 1..];
        let Ok(index) = index_str.parse::<usize>() else {
            return vec![VariantPathElement::field(segment)];
        };

        path_elements.push(VariantPathElement::index(index));
        base = &stripped[..open_pos];
    }

    if !base.is_empty() {
        path_elements.push(VariantPathElement::field(base));
    }

    path_elements.reverse();
    path_elements
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_fits_precision() {
        assert!(fits_precision::<10>(1023));
        assert!(!fits_precision::<10>(1024));
        assert!(fits_precision::<10>(-1023));
        assert!(!fits_precision::<10>(-1024));
    }
}
