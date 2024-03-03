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

use crate::{ArrowNativeType, OffsetBuffer, ScalarBuffer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsBuilder<O: ArrowNativeType> {
    offsets: Vec<O>,
}

/// builder for [`OffsetBuffer`]
impl<O: ArrowNativeType + Add<Output = O> + Sub<Output = O>> OffsetsBuilder<O> {
    /// create a new builder containing only 1 zero offset
    pub fn new(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::usize_as(0));
        Self::new_unchecked(offsets)
    }

    /// create a new builder containing capacity number of zero offsets
    pub fn new_zeroed(capacity: usize) -> Self {
        let offsets = vec![O::usize_as(0); capacity + 1];
        Self::new_unchecked(offsets)
    }

    /// create from offsets
    /// caller guarantees that offsets are monotonically increasing values
    #[inline]
    pub fn new_unchecked(offsets: Vec<O>) -> Self {
        Self { offsets }
    }

    /// push a length into the builder.
    #[inline]
    pub fn push_length(&mut self, length: O) {
        let last_offset = self.offsets.last().unwrap();
        let next_offset = *last_offset + length;
        self.offsets.push(next_offset);
    }

    #[inline]
    pub fn try_push_usize_length(&mut self, length: usize) -> Result<(), String> {
        self.push_length(O::from_usize(length).ok_or(format!(
            "Cannot safely convert usize length {length} to offset"
        ))?);
        Ok(())
    }

    /// extend the builder with an Iterator of lengths
    pub fn extend_from_lengths(&mut self, lengths: impl IntoIterator<Item = O>) {
        let lengths_iter = lengths.into_iter();
        let size_hint = match lengths_iter.size_hint().1 {
            Some(h_bound) => h_bound,
            None => lengths_iter.size_hint().0,
        };
        self.offsets.reserve(size_hint);
        lengths_iter.for_each(|length| self.push_length(length));
    }

    /// extend with an Iterator of usize lengths
    pub fn try_extend_from_usize_lengths(
        &mut self,
        lengths: impl IntoIterator<Item = usize>,
    ) -> Result<(), String> {
        self.extend_from_lengths(
            lengths
                .into_iter()
                .map(|u_len| {
                    O::from_usize(u_len).ok_or(format!(
                        "Cannot safely convert usize length {u_len} to offset"
                    ))
                })
                .collect::<Result<Vec<O>, String>>()?,
        );
        Ok(())
    }

    /// extend from another OffsetsBuilder
    /// it get a lengths iterator from another builder and extend the builder with the iter
    pub fn extend_from_builder(&mut self, offsets_builder: OffsetsBuilder<O>) {
        let lengths = offsets_builder.lengths();
        self.extend_from_lengths(lengths);
    }

    /// takes the builder itself and returns an [`OffsetBuffer`]
    pub fn finish(self) -> OffsetBuffer<O> {
        OffsetBuffer::new(ScalarBuffer::from(self.offsets))
    }

    pub fn capacity(&self) -> usize {
        self.offsets.capacity()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn last(&self) -> O {
        *self.offsets.last().unwrap()
    }

    /// get an iterator of lengths from builder's underlying offsets
    pub fn lengths(&self) -> impl IntoIterator<Item = O> {
        let mut lengths = self
            .offsets
            .iter()
            .scan(O::usize_as(0), |prev, offset| {
                let ret = Some(*offset - *prev);
                *prev = *offset;
                ret
            })
            .collect::<Vec<O>>();
        lengths.remove(0);
        lengths
    }
}

impl<IntoIter: IntoIterator<Item = O>, O: ArrowNativeType + Add<Output = O> + Sub<Output = O>>
    From<IntoIter> for OffsetsBuilder<O>
{
    fn from(value: IntoIter) -> Self {
        let mut offsets_vec: Vec<O> = value
            .into_iter()
            .scan(O::usize_as(0), |prev, len| {
                *prev = *prev + len;
                Some(*prev)
            })
            .collect();
        offsets_vec.insert(0, O::usize_as(0));
        OffsetsBuilder::new_unchecked(offsets_vec)
    }
}

#[cfg(test)]
mod tests {
    use crate::{OffsetBuffer, OffsetsBuilder};

    #[test]
    fn new_zeroed() -> Result<(), String> {
        let builder: OffsetsBuilder<i8> = OffsetsBuilder::new_zeroed(3);
        assert_eq!(builder.finish().to_vec(), vec![0, 0, 0, 0]);
        Ok(())
    }

    #[test]
    fn test_from() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let builder: OffsetsBuilder<i32> = lengths.into();

        assert_eq!(builder.last(), 12);
        assert_eq!(builder.len(), 8);

        let offsets = builder.finish();
        assert_eq!(offsets.to_vec(), vec![0, 1, 3, 6, 6, 9, 11, 12]);
        Ok(())
    }

    #[test]
    fn test_push() -> Result<(), String> {
        let lengths = vec![1, 2, 3, 0, 3, 2, 1];
        let mut builder: OffsetsBuilder<i32> = lengths.into();
        builder.try_push_usize_length(1)?;
        builder.push_length(2);
        builder.push_length(0);
        let offsets: OffsetBuffer<i32> = builder.into();
        let expect_offsets = vec![0, 1, 3, 6, 6, 9, 11, 12, 13, 15, 15];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }

    #[test]
    fn test_try_push_unexpect() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder: OffsetsBuilder<i8> = lengths.into();
        let len = 1 << 20;
        match builder.try_push_usize_length(1 << 20) {
            Err(err) => {
                assert_eq!(
                    format!("Cannot safely convert usize length {len} to offset"),
                    err
                );
                Ok(())
            }
            Ok(_) => return Err("builder.finish should return Err".to_string()),
        }
    }

    #[test]
    fn test_extend() -> Result<(), String> {
        let lengths = vec![1, 2, 3];
        let mut builder: OffsetsBuilder<i32> = lengths.into();

        let extend_lengths = vec![4, 4];
        builder.try_extend_from_usize_lengths(extend_lengths)?;

        let extend_i32_lengths: Vec<i32> = vec![5, 5];
        builder.extend_from_lengths(extend_i32_lengths);

        let another_builder = vec![1, 2, 3].into();
        builder.extend_from_builder(another_builder);

        let offsets = builder.finish();
        let expect_offsets = vec![0, 1, 3, 6, 10, 14, 19, 24, 25, 27, 30];
        assert_eq!(offsets.to_vec(), expect_offsets);
        Ok(())
    }
}
