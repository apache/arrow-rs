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

use std::ops::{Add, Sub};

use crate::{ArrowNativeType, OffsetBuffer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetBufferBuilder<O: ArrowNativeType> {
    offsets: Vec<O>,
}

/// Builder of [`OffsetBuffer`]
impl<O: ArrowNativeType + Add<Output = O> + Sub<Output = O>> OffsetBufferBuilder<O> {
    /// Create a new builder containing only 1 zero offset.
    pub fn new(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::usize_as(0));
        unsafe { Self::new_unchecked(offsets) }
    }

    /// Create a new builder containing capacity number of zero offsets.
    pub fn new_zeroed(capacity: usize) -> Self {
        let offsets = vec![O::usize_as(0); capacity + 1];
        unsafe { Self::new_unchecked(offsets) }
    }

    /// Create from offsets.
    /// # Safety
    /// Caller guarantees that offsets are monotonically increasing values
    /// and the offsets vec is not empty.
    #[inline]
    pub unsafe fn new_unchecked(offsets: Vec<O>) -> Self {
        Self { offsets }
    }

    /// Try to create an [`OffsetBufferBuilder`] with a lengths iterator
    pub fn try_from_lengths(lengths: impl IntoIterator<Item = usize>) -> Result<Self, String> {
        let lengths_iter = lengths.into_iter();
        let size_hint = match lengths_iter.size_hint().1 {
            Some(h_bound) => h_bound,
            None => lengths_iter.size_hint().0,
        };
        let mut builder = OffsetBufferBuilder::new(size_hint);
        for len in lengths_iter {
            builder.try_push_length(len)?;
        }
        Ok(builder)
    }

    /// Try to safely push a length of usize type into builder
    #[inline]
    pub fn try_push_length(&mut self, length: usize) -> Result<(), String> {
        let last_offset = self.offsets.last().unwrap();
        let next_offset = O::from_usize(
            last_offset
                .as_usize()
                .checked_add(length)
                .ok_or("offset overflow")?,
        )
        .ok_or("offset overflow")?;
        self.offsets.push(next_offset);
        Ok(())
    }

    /// Push a length of usize type without overflow checking.
    /// # Safety
    /// This doesn't check offset overflow, the caller must ensure it.
    #[inline]
    pub unsafe fn push_length_unchecked(&mut self, length: usize) {
        let last_offset = self.offsets.last().unwrap();
        let next_offset = *last_offset + O::usize_as(length);
        self.offsets.push(next_offset);
    }

    /// Try to extend an IntoIterator<Item = O> of lengths
    pub fn try_extend<T: IntoIterator<Item = O>>(&mut self, lengths: T) -> Result<(), String> {
        let lengths_iter = lengths.into_iter();
        let size_hint = match lengths_iter.size_hint().1 {
            Some(h_bound) => h_bound,
            None => lengths_iter.size_hint().0,
        };
        self.reserve(size_hint);
        for len in lengths_iter {
            self.try_push_length(len.as_usize())?;
        }
        Ok(())
    }

    /// Takes the builder itself and returns an [`OffsetBuffer`]
    pub fn finish(self) -> OffsetBuffer<O> {
        unsafe { OffsetBuffer::new_unchecked(self.offsets.into()) }
    }

    /// Capacity of offsets
    pub fn capacity(&self) -> usize {
        self.offsets.capacity() - 1
    }

    /// Length of the Offsets
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Last offset
    #[inline]
    pub fn last(&self) -> O {
        *self.offsets.last().unwrap()
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.offsets.reserve_exact(additional);
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
    }
}

impl<O: ArrowNativeType + Add<Output = O> + Sub<Output = O>> Extend<O> for OffsetBufferBuilder<O> {
    fn extend<T: IntoIterator<Item = O>>(&mut self, lengths: T) {
        self.try_extend(lengths).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{OffsetBuffer, OffsetBufferBuilder};

    #[test]
    fn new_zeroed() -> Result<(), String> {
        let builder: OffsetBufferBuilder<i8> = OffsetBufferBuilder::new_zeroed(3);
        assert_eq!(builder.finish().to_vec(), vec![0, 0, 0, 0]);
        Ok(())
    }

    #[test]
    fn test_from() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let builder = OffsetBufferBuilder::<i32>::try_from_lengths(lengths)?;

        assert_eq!(builder.last(), 12);
        assert_eq!(builder.len(), 8);

        let offsets = builder.finish();
        assert_eq!(offsets.to_vec(), vec![0, 1, 3, 6, 6, 9, 11, 12]);
        Ok(())
    }

    #[test]
    fn test_push() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let mut builder = OffsetBufferBuilder::<i32>::try_from_lengths(lengths)?;
        builder.try_push_length(1usize)?;
        builder.try_push_length(2usize)?;
        unsafe {
            builder.push_length_unchecked(0usize);
        }
        let offsets: OffsetBuffer<i32> = builder.into();
        let expect_offsets = vec![0, 1, 3, 6, 6, 9, 11, 12, 13, 15, 15];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }

    #[test]
    fn test_try_push_unexpect() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder = OffsetBufferBuilder::<i8>::try_from_lengths(lengths)?;
        let len = 1 << 20;
        match builder.try_push_length(len) {
            Err(err) => {
                assert_eq!(format!("offset overflow"), err);
                Ok(())
            }
            Ok(_) => Err("builder.finish should return Err".to_string()),
        }
    }

    #[test]
    fn test_extend() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder = OffsetBufferBuilder::<i32>::try_from_lengths(lengths)?;

        let extend_lengths = vec![4, 4, 5, 5];
        builder.extend(extend_lengths);

        let offsets = builder.finish();
        let expect_offsets = vec![0, 1, 3, 6, 10, 14, 19, 24];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }
}
