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

use std::ops::Deref;

use crate::{ArrowNativeType, OffsetBuffer, OverflowError};

/// Builder of [`OffsetBuffer`]
#[derive(Debug)]
pub struct OffsetBufferBuilder<O: ArrowNativeType> {
    offsets: Vec<O>,
    last_offset: usize,
}

impl<O: ArrowNativeType> OffsetBufferBuilder<O> {
    /// Create a new builder with space for `capacity + 1` offsets
    pub fn new(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::usize_as(0));
        Self {
            offsets,
            last_offset: 0,
        }
    }

    /// Push a slice of `length` bytes
    ///
    /// # Panics
    ///
    /// Panics if adding `length` would overflow `usize`.
    /// Use [`Self::try_push_length`] for a fallible version.
    #[inline]
    pub fn push_length(&mut self, length: usize) {
        self.try_push_length(length)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    /// Push a slice of `length` bytes
    ///
    /// # Errors
    ///
    /// Errors if adding `length` would overflow `usize`. The builder is left unchanged.
    #[inline]
    pub fn try_push_length(&mut self, length: usize) -> Result<(), OverflowError> {
        self.last_offset = self
            .last_offset
            .checked_add(length)
            .ok_or(OverflowError::new("usize"))?;
        self.offsets.push(O::usize_as(self.last_offset));
        Ok(())
    }

    /// Reserve space for at least `additional` further offsets
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    /// Takes the builder itself and returns an [`OffsetBuffer`]
    ///
    /// # Panics
    ///
    /// Panics if offsets overflow `O`. Use [`Self::try_finish`] for a fallible version.
    pub fn finish(self) -> OffsetBuffer<O> {
        self.try_finish().unwrap_or_else(|err| panic!("{err}"))
    }

    /// Takes the builder itself and returns an [`OffsetBuffer`]
    ///
    /// # Errors
    ///
    /// Errors if offsets overflow `O`, e.g. if they add up to more than `i32::MAX`
    /// for a `OffsetBufferBuilder<i32>`.
    pub fn try_finish(self) -> Result<OffsetBuffer<O>, OverflowError> {
        O::from_usize(self.last_offset).ok_or_else(|| {
            OverflowError::new("offset")
                .with_value(self.last_offset)
                .with_type::<O>()
        })?;
        Ok(unsafe { OffsetBuffer::new_unchecked(self.offsets.into()) })
    }

    /// Builds the [OffsetBuffer] without resetting the builder.
    ///
    /// # Panics
    ///
    /// Panics if offsets overflow `O`. Use [`Self::try_finish_cloned`] for a fallible version.
    pub fn finish_cloned(&self) -> OffsetBuffer<O> {
        self.try_finish_cloned()
            .unwrap_or_else(|err| panic!("{err}"))
    }

    /// Builds the [OffsetBuffer] without resetting the builder.
    ///
    /// # Errors
    ///
    /// Errors for the same reasons as [`Self::try_finish`].
    pub fn try_finish_cloned(&self) -> Result<OffsetBuffer<O>, OverflowError> {
        let cloned = Self {
            offsets: self.offsets.clone(),
            last_offset: self.last_offset,
        };
        cloned.try_finish()
    }
}

impl<O: ArrowNativeType> Deref for OffsetBufferBuilder<O> {
    type Target = [O];

    fn deref(&self) -> &Self::Target {
        self.offsets.as_ref()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn try_finish_overflow() {
        let mut builder = OffsetBufferBuilder::<i32>::new(2);
        builder.try_push_length(u32::MAX as usize).unwrap();
        let expected = "offset overflow: 4294967295 does not fit in i32";
        assert_eq!(
            builder.try_finish_cloned().unwrap_err().to_string(),
            expected
        );
        assert_eq!(builder.try_finish().unwrap_err().to_string(), expected);

        let mut builder = OffsetBufferBuilder::<i32>::new(2);
        builder.try_push_length(usize::MAX).unwrap();
        // The builder is unchanged by a failed push:
        assert_eq!(
            builder.try_push_length(1).unwrap_err().to_string(),
            "usize overflow"
        );
        assert_eq!(builder.len(), 2);
    }
    use crate::OffsetBufferBuilder;

    #[test]
    fn test_basic() {
        let mut builder = OffsetBufferBuilder::<i32>::new(5);
        assert_eq!(builder.len(), 1);
        assert_eq!(&*builder, &[0]);
        let finished = builder.finish_cloned();
        assert_eq!(finished.len(), 1);
        assert_eq!(&*finished, &[0]);

        builder.push_length(2);
        builder.push_length(6);
        builder.push_length(0);
        builder.push_length(13);

        let finished = builder.finish();
        assert_eq!(&*finished, &[0, 2, 8, 8, 21]);
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn test_usize_overflow() {
        let mut builder = OffsetBufferBuilder::<i32>::new(5);
        builder.push_length(1);
        builder.push_length(usize::MAX);
        builder.finish();
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn test_i32_overflow() {
        let mut builder = OffsetBufferBuilder::<i32>::new(5);
        builder.push_length(1);
        builder.push_length(i32::MAX as usize);
        builder.finish();
    }
}
