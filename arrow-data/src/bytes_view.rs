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

use arrow_buffer::Buffer;
use arrow_schema::ArrowError;

#[derive(Debug, Copy, Clone, Default)]
#[repr(C)]
pub struct BytesView {
    /// The length of the string/bytes.
    pub length: u32,
    /// First 4 bytes of string/bytes data.
    pub prefix: u32,
    /// The buffer index.
    pub buffer_index: u32,
    /// The offset into the buffer.
    pub offset: u32,
}

impl BytesView {
    #[inline(always)]
    pub fn as_u128(self) -> u128 {
        unsafe { std::mem::transmute(self) }
    }
}

impl From<u128> for BytesView {
    #[inline]
    fn from(value: u128) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<BytesView> for u128 {
    #[inline]
    fn from(value: BytesView) -> Self {
        value.as_u128()
    }
}

/// Validates the combination of `views` and `buffers` is a valid BinaryView
pub fn validate_binary_view(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |_, _| Ok(()))
}

/// Validates the combination of `views` and `buffers` is a valid StringView
pub fn validate_string_view(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |idx, b| {
        simdutf8::basic::from_utf8(b).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "Encountered non-UTF-8 data at index {idx}: {e}"
            ))
        })?;
        Ok(())
    })
}

fn validate_view_impl<F>(views: &[u128], buffers: &[Buffer], f: F) -> Result<(), ArrowError>
where
    F: Fn(usize, &[u8]) -> Result<(), ArrowError>,
{
    for (idx, v) in views.iter().enumerate() {
        let len = *v as u32;
        if len <= 12 {
            if len < 12 && (v >> (32 + len * 8)) != 0 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "View at index {idx} contained non-zero padding for string of length {len}",
                )));
            }
            f(idx, &v.to_le_bytes()[4..4 + len as usize])?;
        } else {
            let view = BytesView::from(*v);
            let data = buffers.get(view.buffer_index as usize).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid buffer index at {idx}: got index {} but only has {} buffers",
                    view.buffer_index,
                    buffers.len()
                ))
            })?;

            let start = view.offset as usize;
            let end = start + len as usize;
            let b = data.get(start..end).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid buffer slice at {idx}: got {start}..{end} but buffer {} has length {}",
                    view.buffer_index,
                    data.len()
                ))
            })?;

            if !b.starts_with(&view.prefix.to_le_bytes()) {
                return Err(ArrowError::InvalidArgumentError(
                    "Mismatch between embedded prefix and data".to_string(),
                ));
            }

            f(idx, b)?;
        }
    }
    Ok(())
}
