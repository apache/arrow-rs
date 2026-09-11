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

/// An error returned by a fallible buffer operation.
///
/// This error aggregates the specific errors that can occur in `arrow-buffer`.
#[derive(Debug)]
pub enum BufferError {
    /// An arithmetic overflow.
    Overflow(OverflowError),
    /// An offset or range outside the buffer bounds.
    OutOfBounds(OutOfBoundsError),
    /// A buffer with insufficient alignment for a scalar type.
    Misaligned(AlignmentError),
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow(err) => err.fmt(f),
            Self::OutOfBounds(err) => err.fmt(f),
            Self::Misaligned(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for BufferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Overflow(err) => Some(err),
            Self::OutOfBounds(err) => Some(err),
            Self::Misaligned(err) => Some(err),
        }
    }
}

/// A buffer whose pointer is not aligned for a scalar type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlignmentError {
    alignment: usize,
    type_name: &'static str,
}

impl AlignmentError {
    /// Creates an error for a buffer that is not aligned for `T`.
    pub fn new<T>() -> Self {
        Self {
            alignment: std::mem::align_of::<T>(),
            type_name: std::any::type_name::<T>(),
        }
    }

    /// The required alignment, in bytes.
    pub const fn alignment(&self) -> usize {
        self.alignment
    }

    /// The scalar type that requires alignment.
    pub const fn type_name(&self) -> &'static str {
        self.type_name
    }
}

impl std::fmt::Display for AlignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            alignment,
            type_name,
        } = self;
        write!(
            f,
            "buffer is not aligned for {type_name} (requires {alignment}-byte alignment)"
        )
    }
}

impl std::error::Error for AlignmentError {}

impl From<AlignmentError> for BufferError {
    fn from(err: AlignmentError) -> Self {
        Self::Misaligned(err)
    }
}

/// An arithmetic overflow.
///
/// Returned by the `try_` alternatives to the functions that panic on overflow,
/// for instance [`OffsetBuffer::try_from_lengths`](crate::OffsetBuffer::try_from_lengths).
///
/// ```
/// # use arrow_buffer::OffsetBuffer;
/// // 32 bit offsets cannot describe more than 2 GiB of data:
/// let err = OffsetBuffer::<i32>::try_from_lengths([u32::MAX as usize]).unwrap_err();
/// assert_eq!(err.to_string(), "offset overflow: 4294967295 does not fit in i32");
///
/// // 64 bit offsets can:
/// assert!(OffsetBuffer::<i64>::try_from_lengths([u32::MAX as usize]).is_ok());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverflowError {
    what: &'static str,
    value: Option<usize>,
    type_name: &'static str,
}

impl OverflowError {
    /// `what` names what overflowed, for instance `"offset"`,
    /// and `T` is the type it did not fit in, for instance `i32`.
    pub fn new<T>(what: &'static str) -> Self {
        Self {
            what,
            value: None,
            type_name: std::any::type_name::<T>(),
        }
    }

    /// The value that did not fit.
    pub const fn with_value(mut self, value: usize) -> Self {
        self.value = Some(value);
        self
    }

    /// What overflowed, for instance `"offset"`.
    pub const fn what(&self) -> &'static str {
        self.what
    }

    /// The value that did not fit, if known.
    pub const fn value(&self) -> Option<usize> {
        self.value
    }

    /// The name of the type that the value did not fit in, for instance `i32`.
    pub const fn type_name(&self) -> &'static str {
        self.type_name
    }
}

impl std::fmt::Display for OverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            what,
            value,
            type_name,
        } = self;
        write!(f, "{what} overflow: ")?;
        match value {
            Some(value) => write!(f, "{value} does not fit in {type_name}"),
            None => write!(f, "does not fit in {type_name}"),
        }
    }
}

impl std::error::Error for OverflowError {}

impl From<OverflowError> for BufferError {
    fn from(err: OverflowError) -> Self {
        Self::Overflow(err)
    }
}

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

impl From<OutOfBoundsError> for BufferError {
    fn from(err: OutOfBoundsError) -> Self {
        Self::OutOfBounds(err)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn buffer_error_source() {
        let overflow = OverflowError::new::<i32>("offset");
        let err = BufferError::from(overflow);

        assert_eq!(err.to_string(), "offset overflow: does not fit in i32");
        assert!(
            err.source()
                .is_some_and(|source| source.is::<OverflowError>())
        );

        let out_of_bounds = OutOfBoundsError::new("buffer", 2, 1);
        let err = BufferError::from(out_of_bounds);

        assert_eq!(
            err.to_string(),
            "buffer out of bounds: offset 2 exceeds length 1"
        );
        assert!(
            err.source()
                .is_some_and(|source| source.is::<OutOfBoundsError>())
        );
    }
}
