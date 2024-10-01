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
/// See Also:
/// * [`GenericByteViewArray`] for more information on the layout of the views.
/// * [`validate_binary_view`] and [`validate_string_view`] to validate
///
/// # Example: Create a new u128 view
///
/// ```rust
/// # use arrow_data::ByteView;;
/// // Create a view for a string of length 20
/// // first four bytes are "Rust"
/// // stored in buffer 3
/// // at offset 42
/// let prefix = "Rust";
/// let view = ByteView::new(20, prefix.as_bytes())
///   .with_buffer_index(3)
///   .with_offset(42);
///
/// // create the final u128
/// let v = view.as_u128();
/// assert_eq!(v, 0x2a000000037473755200000014);
/// ```
///
/// # Example: decode a `u128` into its constituent fields
/// ```rust
/// # use arrow_data::ByteView;
/// // Convert a u128 to a ByteView
/// // See validate_{string,binary}_view functions to validate
/// let v = ByteView::from(0x2a000000037473755200000014);
///
/// assert_eq!(v.length, 20);
/// assert_eq!(v.prefix, 0x74737552);
/// assert_eq!(v.buffer_index, 3);
/// assert_eq!(v.offset, 42);
/// ```
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
    /// Construct a [`ByteView`] for data `length` of bytes with the specified prefix.
    ///
    /// See example on [`ByteView`] docs
    ///
    /// Notes:
    /// * the length should always be greater than 12 (Data less than 12
    ///   bytes is stored as an inline view)
    /// * buffer and offset are set to `0`
    ///
    /// # Panics
    /// If the prefix is not exactly 4 bytes
    #[inline]
    pub fn new(length: u32, prefix: &[u8]) -> Self {
        debug_assert!(length > 12);
        Self {
            length,
            prefix: u32::from_le_bytes(prefix.try_into().unwrap()),
            buffer_index: 0,
            offset: 0,
        }
    }

    /// Set the [`Self::buffer_index`] field
    #[inline]
    pub fn with_buffer_index(mut self, buffer_index: u32) -> Self {
        self.buffer_index = buffer_index;
        self
    }

    /// Set the [`Self::offset`] field
    #[inline]
    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

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
