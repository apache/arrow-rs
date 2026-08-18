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

//! Errors returned by `arrow-buffer`.

/// An offset, or an offset and a length, that is out of bounds.
///
/// Returned by the `try_` alternatives to the functions that panic when asked for
/// something outside the bounds of what they hold,
/// for instance [`Buffer::try_slice_with_length`](crate::Buffer::try_slice_with_length).
///
/// ```
/// # use arrow_buffer::Buffer;
/// let buffer = Buffer::from(&[0_u8, 1, 2]);
/// let err = buffer.try_slice_with_length(2, 4).unwrap_err();
/// assert_eq!(
///     err.to_string(),
///     "buffer out of bounds: offset 2 + length 4 exceeds length 3"
/// );
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutOfBoundsError {
    what: &'static str,
    offset: usize,
    len: Option<usize>,
    bounds: usize,
}

impl OutOfBoundsError {
    /// `what` names what was indexed, for instance `"buffer"`,
    /// and `bounds` is the length that `offset` is outside of.
    pub const fn new(what: &'static str, offset: usize, bounds: usize) -> Self {
        Self {
            what,
            offset,
            len: None,
            bounds,
        }
    }

    /// The length that was asked for, starting at the offset.
    pub const fn with_len(mut self, len: usize) -> Self {
        self.len = Some(len);
        self
    }

    /// What was indexed, for instance `"buffer"`.
    pub const fn what(&self) -> &'static str {
        self.what
    }

    /// The offset that was asked for.
    pub const fn requested_offset(&self) -> usize {
        self.offset
    }

    /// The length that was asked for, if any.
    pub const fn requested_len(&self) -> Option<usize> {
        self.len
    }

    /// The length that the request was outside of.
    pub const fn bounds(&self) -> usize {
        self.bounds
    }
}

impl std::fmt::Display for OutOfBoundsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            what,
            offset,
            len,
            bounds,
        } = self;
        write!(f, "{what} out of bounds: offset {offset}")?;
        if let Some(len) = len {
            write!(f, " + length {len}")?;
        }
        write!(f, " exceeds length {bounds}")
    }
}

impl std::error::Error for OutOfBoundsError {}
