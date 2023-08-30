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

//! View array utilities

use arrow_buffer::Buffer;
use arrow_schema::ArrowError;

/// The element layout of a view buffer
///
/// See [`DataType::Utf8View`](arrow_schema::DataType)
pub struct View {
    /// The length of the string
    pub length: u32,
    /// The first 4 bytes of string data
    pub prefix: u32,
    /// The buffer index
    pub buffer_index: u32,
    /// The offset into the buffer
    pub offset: u32,
}

impl From<u128> for View {
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

impl From<View> for u128 {
    #[inline]
    fn from(value: View) -> Self {
        (value.length as u128)
            | ((value.prefix as u128) << 32)
            | ((value.buffer_index as u128) << 64)
            | ((value.offset as u128) << 96)
    }
}

/// Validates the combination of `views` and `buffers` is a valid BinaryView
pub fn validate_binary_view(
    views: &[u128],
    buffers: &[Buffer],
) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |_, _| Ok(()))
}

/// Validates the combination of `views` and `buffers` is a valid StringView
pub fn validate_string_view(
    views: &[u128],
    buffers: &[Buffer],
) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |idx, b| {
        std::str::from_utf8(b).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "Encountered non-UTF-8 data at index {idx}: {e}"
            ))
        })?;
        Ok(())
    })
}

fn validate_view_impl<F>(
    views: &[u128],
    buffers: &[Buffer],
    f: F,
) -> Result<(), ArrowError>
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
            let view = View::from(*v);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate() {
        let buffers = vec![
            Buffer::from(b"helloworldbongo\xFFfoo"),
            Buffer::from(b"bar"),
        ];

        let test = |length: u32, prefix: u32, buffer_index: u32, offset: u32| {
            let view = View {
                length,
                prefix,
                buffer_index,
                offset,
            };
            validate_string_view(&[view.into()], &buffers)
        };

        let err = test(14, 0, 2, 0).unwrap_err().to_string();
        assert_eq!(err, "Invalid argument error: Invalid buffer index at 0: got index 2 but only has 2 buffers");

        let err = test(14, 0, 1, 3).unwrap_err().to_string();
        assert_eq!(err, "Invalid argument error: Invalid buffer slice at 0: got 3..17 but buffer 1 has length 3");

        let err = test(14, 0, 1, 3).unwrap_err().to_string();
        assert_eq!(err, "Invalid argument error: Invalid buffer slice at 0: got 3..17 but buffer 1 has length 3");

        let err = test(15, 0, 0, 0).unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Mismatch between embedded prefix and data"
        );

        let prefix = u32::from_le_bytes(*b"hell");
        test(15, prefix, 0, 0).unwrap();

        let prefix = u32::from_le_bytes(*b"ello");
        let err = test(15, prefix, 0, 1).unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 14"
        );

        let encoded = u128::from_le_bytes(*b"\x05\0\0\0hello\0\0\0\0\0\0\0");
        validate_string_view(&[encoded], &[]).unwrap();

        let encoded = u128::from_le_bytes(*b"\x05\0\0\0hello\0\0\0s\0\0\0");
        let err = validate_string_view(&[encoded], &[])
            .unwrap_err()
            .to_string();
        assert_eq!(err, "Invalid argument error: View at index 0 contained non-zero padding for string of length 5");

        let encoded = u128::from_le_bytes(*b"\x05\0\0\0he\xFFlo\0\0\0\0\0\0\0");
        let err = validate_string_view(&[encoded], &[])
            .unwrap_err()
            .to_string();
        assert_eq!(err, "Invalid argument error: Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 2");
    }
}
