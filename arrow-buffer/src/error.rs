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

/// An arithmetic overflow.
///
/// Returned by the `try_` alternatives to the functions that panic on overflow,
/// for instance [`OffsetBuffer::try_from_lengths`](crate::OffsetBuffer::try_from_lengths).
///
/// ```
/// # use arrow_buffer::OffsetBuffer;
/// // 32 bit offsets cannot describe more than 2 GiB of data:
/// let err = OffsetBuffer::<i32>::try_from_lengths([u32::MAX as usize]).unwrap_err();
/// assert_eq!(err.to_string(), "offset overflow");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverflowError {
    what: &'static str,
}

impl OverflowError {
    /// `what` names what overflowed, for instance `"offset"`.
    pub const fn new(what: &'static str) -> Self {
        Self { what }
    }

    /// What overflowed, for instance `"offset"`.
    pub const fn what(&self) -> &'static str {
        self.what
    }
}

impl std::fmt::Display for OverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { what } = self;
        write!(f, "{what} overflow")
    }
}

impl std::error::Error for OverflowError {}
