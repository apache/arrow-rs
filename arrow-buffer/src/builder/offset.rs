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

use crate::{ArrowNativeType, OffsetBuffer, ScalarBuffer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsBuilder {
    offsets: Vec<usize>,
}

/// builder for [`OffsetBuffer`]
impl OffsetsBuilder {
    /// create a new builder containing only 1 zero offset
    pub fn new(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self::new_unchecked(offsets)
    }

    /// create a new builder containing capacity number of zero offsets
    pub fn new_zeroed(capacity: usize) -> Self {
        let offsets = vec![0usize; capacity + 1];
        Self::new_unchecked(offsets)
    }

    /// create from offsets
    /// caller guarantees that offsets are monotonically increasing values
    #[inline]
    pub fn new_unchecked(offsets: Vec<usize>) -> Self {
        Self { offsets }
    }

    /// try to push a [`ArrowNativeType`] length into the builder.
    /// it returns an Err if length cannot be safely cast to usize
    pub fn try_push_length(&mut self, length: impl ArrowNativeType) -> Result<(), String> {
        let usize_len = length
            .to_usize()
            .ok_or(format!("The length {length:?} cannot safely cast to usize").to_string())?;
        self.push_usize_length(usize_len);
        Ok(())
    }

    pub fn push_usize_length(&mut self, length: usize) {
        let last_offset = self.offsets.last().unwrap();
        let next_offset = last_offset + length;
        self.offsets.push(next_offset);
    }

    /// try to extend the builder with an Iterator of [`ArrowNativeType`] lengths
    /// returns an Err if any length cannot be safely cast to usize
    pub fn try_extend_from_lengths(
        &mut self,
        lengths: impl IntoIterator<Item = impl ArrowNativeType + std::fmt::Display>,
    ) -> Result<(), String> {
        let usize_lengths = lengths
            .into_iter()
            .map(|len| {
                len.to_usize()
                    .ok_or(format!("Cannot safely convert length {len} to usize"))
            })
            .collect::<Result<Vec<usize>, String>>()?;
        self.extend_from_usize_lengths(usize_lengths);
        Ok(())
    }

    /// extend with an Iterator of usize lengths
    pub fn extend_from_usize_lengths(&mut self, lengths: impl IntoIterator<Item = usize>) {
        let lengths_iter = lengths.into_iter();
        let size_hint = match lengths_iter.size_hint().1 {
            Some(h_bound) => h_bound,
            None => lengths_iter.size_hint().0,
        };
        self.offsets.reserve(size_hint);
        lengths_iter.for_each(|length| self.push_usize_length(length))
    }

    /// extend from another OffsetsBuilder
    /// it get a lengths iterator from another builder and extend the builder with the iter
    pub fn extend_from_builder(&mut self, offsets_builder: OffsetsBuilder) {
        let lengths = offsets_builder.lengths();
        self.extend_from_usize_lengths(lengths);
    }

    /// takes the builder itself and returns an [`OffsetBuffer`]
    pub fn finish<O: ArrowNativeType>(self) -> Result<OffsetBuffer<O>, String> {
        let scala_buf = ScalarBuffer::from(
            self.offsets
                .into_iter()
                .map(|offset| {
                    O::from_usize(offset).ok_or("Cannot safely convert offset to usize".to_string())
                })
                .collect::<Result<Vec<O>, String>>()?,
        );
        Ok(OffsetBuffer::new(scala_buf))
    }

    pub fn capacity(&self) -> usize {
        self.offsets.capacity()
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn last(&self) -> usize {
        *self.offsets.last().unwrap()
    }

    /// get an iterator of lengths from builder's underlying offsets
    pub fn lengths(&self) -> impl IntoIterator<Item = usize> {
        let mut lengths = self
            .offsets
            .iter()
            .scan(0usize, |prev, offset| {
                let ret = Some(*offset - *prev);
                *prev = *offset;
                ret
            })
            .collect::<Vec<usize>>();
        lengths.remove(0);
        lengths
    }
}

impl<IntoIter: IntoIterator<Item = usize>> From<IntoIter> for OffsetsBuilder {
    fn from(value: IntoIter) -> Self {
        let mut offsets_vec: Vec<usize> = value
            .into_iter()
            .scan(0usize, |prev, len| {
                *prev = len + *prev;
                Some(*prev)
            })
            .collect();
        offsets_vec.insert(0, 0);
        OffsetsBuilder::new_unchecked(offsets_vec)
    }
}

#[cfg(test)]
mod tests {
    use crate::{OffsetBuffer, OffsetsBuilder};

    #[test]
    fn new_zeroed() -> Result<(), String> {
        let builder = OffsetsBuilder::new_zeroed(3);
        assert_eq!(builder.finish::<i8>()?.to_vec(), vec![0, 0, 0, 0]);
        Ok(())
    }

    #[test]
    fn test_from() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let builder: OffsetsBuilder = lengths.into();

        assert_eq!(builder.last(), 12);
        assert_eq!(builder.len(), 8);

        let offsets = builder.finish::<i32>()?;
        assert_eq!(offsets.to_vec(), vec![0, 1, 3, 6, 6, 9, 11, 12]);
        Ok(())
    }

    #[test]
    fn test_push() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let mut builder: OffsetsBuilder = lengths.into();
        builder.push_usize_length(1);
        builder.try_push_length(2)?;
        builder.try_push_length(0i16)?;
        let offsets: OffsetBuffer<i32> = builder.try_into()?;
        let expect_offsets = vec![0, 1, 3, 6, 6, 9, 11, 12, 13, 15, 15];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }

    #[test]
    fn test_try_push_unexpect() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder: OffsetsBuilder = lengths.into();
        match builder.try_push_length(-1i32) {
            Err(err) => {
                assert_eq!("The length -1 cannot safely cast to usize", err);
                Ok(())
            }
            Ok(_) => return Err("builder.finish should return Err".to_string()),
        }
    }

    #[test]
    fn test_extend() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder: OffsetsBuilder = lengths.into();

        let extend_lengths = vec![4, 4];
        builder.extend_from_usize_lengths(extend_lengths);

        let extend_i32_lengths: Vec<i32> = vec![5, 5];
        builder.try_extend_from_lengths(extend_i32_lengths)?;

        let another_builder = vec![1, 2, 3].into();
        builder.extend_from_builder(another_builder);

        let offsets: OffsetBuffer<u32> = builder.finish()?;
        let expect_offsets = vec![0, 1, 3, 6, 10, 14, 19, 24, 25, 27, 30];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }
}
