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

pub(crate) fn first_byte_from_slice(slice: &[u8]) -> Result<&u8, ArrowError> {
    slice
        .get(0)
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Helper to get a &str from a slice based on range, if it's valid or an error otherwise
pub(crate) fn string_from_slice(slice: &[u8], range: Range<usize>) -> Result<&str, ArrowError> {
    str::from_utf8(slice_from_slice(slice, range)?)
        .map_err(|_| ArrowError::InvalidArgumentError("invalid UTF-8 string".to_string()))
}
