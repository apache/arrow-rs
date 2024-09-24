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

/// Helper to access views of [`GenericByteViewArray`] (`StringViewArray` and
/// `BinaryViewArray`) where the length is greater than 12 bytes.
///
/// See the documentation on [`GenericByteViewArray`] for more information on
/// the layout of the views.
///
/// [`GenericByteViewArray`]: https://docs.rs/arrow/latest/arrow/array/struct.GenericByteViewArray.html
#[derive(Debug, Copy, Clone, Default)]
#[repr(C)]
pub struct ByteView {
    /// The length of the string/bytes.
    pub length: u32,
    /// First 4 bytes of string/bytes data.
    pub prefix: u32,
    /// The buffer index.
    pub buffer_index: u32,
    /// The offset into the buffer.
    pub offset: u32,
}

impl ByteView {
    #[inline(always)]
    /// Convert `ByteView` to `u128` by concatenating the fields
    pub fn as_u128(self) -> u128 {
        (self.length as u128)
            | ((self.prefix as u128) << 32)
            | ((self.buffer_index as u128) << 64)
            | ((self.offset as u128) << 96)
    }
}

impl From<u128> for ByteView {
    #[inline]
    fn from(value: u128) -> Self {
        Self {
            length: value as u32,
            prefix: (value >> 32) as u32,
            buffer_index: (value >> 64) as u32,
            offset: (value >> 96) as u32,
        }
    }
}

impl From<ByteView> for u128 {
    #[inline]
    fn from(value: ByteView) -> Self {
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
        std::str::from_utf8(b).map_err(|e| {
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
            let view = ByteView::from(*v);
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
