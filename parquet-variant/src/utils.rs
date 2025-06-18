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

use arrow_schema::ArrowError;

use std::fmt::Debug;
use std::slice::SliceIndex;

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

pub(crate) fn first_byte_from_slice(slice: &[u8]) -> Result<u8, ArrowError> {
    slice
        .first()
        .copied()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Helper to get a &str from a slice based on range, if it's valid or an error otherwise
pub(crate) fn string_from_slice(slice: &[u8], range: Range<usize>) -> Result<&str, ArrowError> {
    str::from_utf8(slice_from_slice(slice, range)?)
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
///   This function can fail and return an error.
///
/// # Returns
/// * `Ok(Ok(index))` - Element found at the given index
/// * `Ok(Err(index))` - Element not found, but would be inserted at the given index
/// * `Err(e)` - Key extraction failed with error `e`
pub(crate) fn try_binary_search_range_by<K, E, F>(
    range: Range<usize>,
    target: &K,
    mut key_extractor: F,
) -> Result<Result<usize, usize>, E>
where
    K: Ord,
    F: FnMut(usize) -> Result<K, E>,
{
    let Range { mut start, mut end } = range;

    while start < end {
        let mid = start + (end - start) / 2;
        let key = key_extractor(mid)?;

        match key.cmp(target) {
            std::cmp::Ordering::Equal => return Ok(Ok(mid)),
            std::cmp::Ordering::Greater => end = mid,
            std::cmp::Ordering::Less => start = mid + 1,
        }
    }

    Ok(Err(start))
}

/// Attempts to prove a fallible iterator is actually infallible in practice, by consuming every
/// element and returning the first error (if any).
pub(crate) fn validate_fallible_iterator<T, E>(
    mut it: impl Iterator<Item = Result<T, E>>,
) -> Result<(), E> {
    // NOTE: It should really be `let None = ...`, but the compiler can't prove that.
    let _ = it.find(Result::is_err).transpose()?;
    Ok(())
}
