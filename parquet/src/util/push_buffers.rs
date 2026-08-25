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

use crate::errors::ParquetError;
use crate::file::reader::{ChunkReader, Length};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::ops::Range;

pub(crate) type BufferId = u64;

/// A physically independent buffer and the end offset of the range it covers.
#[derive(Debug, Clone)]
struct BufferValue {
    /// End offset (exclusive) of the byte range this buffer covers.
    end: u64,
    /// The raw data.
    data: Bytes,
    /// Push order, used only to preserve the historical first-match behavior
    /// for overlapping physical buffers.
    insertion_id: BufferId,
}

/// Holds multiple non-contiguous, caller-provided buffers of file data.
///
/// This is the in-memory buffer used by the push-based Parquet decoders
/// (`ParquetPushDecoder` and `ParquetMetaDataPushDecoder`). It can be
/// constructed up front and handed to a builder so the decoder reuses bytes
/// that have already been fetched.
///
/// Features:
/// 1. Zero copy
/// 2. non contiguous ranges of bytes
///
/// # Non Coalescing
///
/// This buffer does not coalesce  (merging adjacent ranges of bytes into a
/// single range). Coalescing at this level would require copying the data but
/// the caller may already have the needed data in a single buffer which would
/// require no copying.
///
/// Thus, the implementation defers to the caller to coalesce subsequent requests
/// if desired.
#[derive(Debug, Clone, Default)]
pub struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The buffers of data that can be used to decode the Parquet file,
    /// grouped by range start. Multiple buffers with the same start are kept
    /// in push order so they are not silently overwritten.
    entries: BTreeMap<u64, Vec<BufferValue>>,
    /// Physical ranges in push order. This is consulted only when the fast
    /// range index identifies an entry that overlaps another entry.
    insertion_order: BTreeMap<u64, (u64, u64)>,
    /// Entries that overlap another physical entry. A local marker avoids
    /// turning every lookup into a scan after one historical overlap.
    overlapping: BTreeSet<u64>,
    next_insertion_id: BufferId,
}

impl Display for PushBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Buffers (offset: {}, file_len: {})",
            self.offset, self.file_len
        )?;
        writeln!(f, "Available Ranges (w/ offset):")?;
        for (&start, values) in &self.entries {
            for value in values {
                writeln!(
                    f,
                    "  {}..{} ({}..{}): {} bytes",
                    start,
                    value.end,
                    start + self.offset,
                    value.end + self.offset,
                    value.data.len(),
                )?;
            }
        }

        Ok(())
    }
}

impl PushBuffers {
    /// Create a new, empty `PushBuffers` for a file of the given length.
    ///
    /// Use [`PushBuffers::default`] when the file length is unknown or
    /// irrelevant (e.g. the push decoder, which tracks ranges by absolute
    /// offset and never consults `file_len`).
    pub fn new(file_len: u64) -> Self {
        Self {
            offset: 0,
            file_len,
            entries: BTreeMap::new(),
            insertion_order: BTreeMap::new(),
            overlapping: BTreeSet::new(),
            next_insertion_id: 0,
        }
    }

    /// Push all the ranges and buffers
    ///
    /// # Errors
    /// Returns an error if the number of ranges does not match the number of
    /// buffers, or if any buffer's length does not match its range (see
    /// [`Self::push_range`]).
    pub fn push_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        if ranges.len() != buffers.len() {
            return Err(general_err!(
                "Number of ranges ({}) must match number of buffers ({})",
                ranges.len(),
                buffers.len()
            ));
        }
        for (range, buffer) in ranges.into_iter().zip(buffers) {
            self.push_range(range, buffer)?;
        }
        Ok(())
    }

    /// Push a new range and its associated buffer
    ///
    /// # Errors
    /// Returns an error if the buffer's length does not match the range's
    /// length, e.g. when a truncated (short) read is pushed.
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) -> Result<(), ParquetError> {
        self.push_owned_range(range, buffer).map(|_| ())
    }

    /// Push a range and return the stable id of its physical entry.
    pub(crate) fn push_owned_range(
        &mut self,
        range: Range<u64>,
        buffer: Bytes,
    ) -> Result<BufferId, ParquetError> {
        if range.start > range.end {
            return Err(general_err!("Invalid range {}..{}", range.start, range.end));
        }
        let expected = range.end - range.start;
        if expected != buffer.len() as u64 {
            return Err(general_err!(
                "Buffer length ({}) does not match length ({}) of range {}..{}",
                buffer.len(),
                expected,
                range.start,
                range.end
            ));
        }

        let insertion_id = self.next_insertion_id;
        self.next_insertion_id = self
            .next_insertion_id
            .checked_add(1)
            .expect("PushBuffers insertion id exhausted");

        let has_overlap = range.start < range.end
            && (self.has_previous_overlap(range.start)
                || self
                    .entries
                    .range((
                        std::ops::Bound::Excluded(range.start),
                        std::ops::Bound::Excluded(range.end),
                    ))
                    .any(|(_, values)| values.iter().any(|value| value.end > range.start)));

        if has_overlap {
            // Overlaps are uncommon for decoder input. Once one is found,
            // mark the overlapping entries so lookup can preserve first-push
            // semantics without a permanent linear-search mode.
            for (&start, values) in self.entries.range(..range.end) {
                for value in values {
                    if start < range.end && value.end > range.start {
                        self.overlapping.insert(value.insertion_id);
                    }
                }
            }
            self.overlapping.insert(insertion_id);
        }

        self.entries
            .entry(range.start)
            .or_default()
            .push(BufferValue {
                end: range.end,
                data: buffer,
                insertion_id,
            });
        self.insertion_order
            .insert(insertion_id, (range.start, range.end));
        Ok(insertion_id)
    }

    /// Returns true if the Buffers contains data for the given range
    pub(crate) fn has_range(&self, range: &Range<u64>) -> bool {
        self.find_buffer(range).is_some()
    }

    /// Returns whether a previous non-empty entry overlaps `start`.
    ///
    /// The normal non-overlapping case stops after the immediate predecessor.
    /// If that predecessor is part of an overlap chain, walk backwards through
    /// that local chain so a long entry cannot be hidden by a shorter entry.
    fn has_previous_overlap(&self, start: u64) -> bool {
        for (&entry_start, values) in self.entries.range(..=start).rev() {
            if values.iter().any(|value| value.end > start) {
                return true;
            }
            if values.iter().any(|value| {
                value.end > entry_start && !self.overlapping.contains(&value.insertion_id)
            }) {
                return false;
            }
        }
        false
    }

    fn find_first_matching(&self, range: &Range<u64>) -> Option<(u64, &BufferValue)> {
        for (&insertion_id, &(start, end)) in &self.insertion_order {
            if start <= range.start
                && end >= range.end
                && let Some(value) = self.entries.get(&start).and_then(|values| {
                    values
                        .iter()
                        .find(|value| value.insertion_id == insertion_id)
                })
            {
                return Some((start, value));
            }
        }
        None
    }

    /// The predecessor lookup is O(log n) for the normal non-overlapping
    /// case. If the candidate is part of a local overlap, the insertion index
    /// is consulted to preserve the old first-match behavior.
    fn find_buffer(&self, range: &Range<u64>) -> Option<(u64, &BufferValue)> {
        if range.start > range.end {
            return None;
        }

        let (&start, values) = self.entries.range(..=range.start).next_back()?;
        if let Some(value) = values.iter().find(|value| value.end >= range.end) {
            if !self.overlapping.contains(&value.insertion_id) {
                return Some((start, value));
            }

            return self.find_first_matching(range).or(Some((start, value)));
        }

        if values
            .iter()
            .any(|value| self.overlapping.contains(&value.insertion_id))
            || values.iter().all(|value| value.end == start)
        {
            return self.find_first_matching(range);
        }

        None
    }

    /// return the file length of the Parquet file being read
    pub(crate) fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Specify a new offset
    fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Return the total of all physically retained buffered bytes.
    #[cfg(feature = "arrow")]
    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.entries
            .values()
            .flatten()
            .map(|buffer| buffer.data.len() as u64)
            .sum()
    }

    /// Remove independently owned physical entries by stable id.
    pub(crate) fn remove_ids(&mut self, ids: &[BufferId]) {
        let mut grouped_ids: BTreeMap<u64, BTreeSet<BufferId>> = BTreeMap::new();
        for &id in ids {
            let Some((start, _)) = self.insertion_order.remove(&id) else {
                continue;
            };

            let single_entry = self
                .entries
                .get(&start)
                .is_some_and(|values| values.len() == 1 && values[0].insertion_id == id);
            if single_entry {
                self.entries.remove(&start);
            } else {
                grouped_ids.entry(start).or_default().insert(id);
            }
            self.overlapping.remove(&id);
        }

        for (start, ids) in grouped_ids {
            let remove_start = if let Some(values) = self.entries.get_mut(&start) {
                values.retain(|value| !ids.contains(&value.insertion_id));
                values.is_empty()
            } else {
                false
            };
            if remove_start {
                self.entries.remove(&start);
            }
        }
    }

    /// Clear all buffered ranges and their corresponding data
    pub(crate) fn clear_all_ranges(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
        self.overlapping.clear();
    }

    /// Consume the buffer into its independently owned physical entries.
    ///
    /// This is used when a caller supplies a preexisting `PushBuffers` to the
    /// Arrow push decoder: those entries still need to pass through the
    /// decoder's retention admission step before they are used.
    #[cfg(feature = "arrow")]
    pub(crate) fn into_ranges(self) -> (u64, Vec<(Range<u64>, Bytes)>) {
        let Self {
            file_len, entries, ..
        } = self;
        let mut ranges = entries
            .into_iter()
            .flat_map(|(start, values)| {
                values
                    .into_iter()
                    .map(move |value| (value.insertion_id, start..value.end, value.data))
            })
            .collect::<Vec<_>>();
        ranges.sort_unstable_by_key(|(insertion_id, _, _)| *insertion_id);

        (
            file_len,
            ranges
                .into_iter()
                .map(|(_, range, data)| (range, data))
                .collect(),
        )
    }

    #[cfg(test)]
    pub(crate) fn num_buffers(&self) -> usize {
        self.entries.values().map(Vec::len).sum()
    }
}

impl Length for PushBuffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

/// less efficient implementation of Read for Buffers
impl std::io::Read for PushBuffers {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let requested_end = self.offset + buf.len() as u64;
        let range = self.offset..requested_end;
        let (start, data) = self.find_buffer(&range).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available in Buffers",
            )
        })?;
        let start_offset = (self.offset - start) as usize;
        let end_offset = start_offset + buf.len();
        buf.copy_from_slice(data.data.slice(start_offset..end_offset).as_ref());
        self.offset = requested_end;
        Ok(buf.len())
    }
}

impl ChunkReader for PushBuffers {
    type T = Self;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(self.clone().with_offset(self.offset + start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let requested_end = start + length as u64;
        let range = start..requested_end;
        let (buffer_start, data) = self
            .find_buffer(&range)
            .ok_or(ParquetError::NeedMoreDataRange(range))?;
        let start_offset = (start - buffer_start) as usize;
        Ok(data.data.slice(start_offset..start_offset + length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::reader::ChunkReader;

    fn push_bytes(buffers: &mut PushBuffers, range: Range<u64>, data: &'static [u8]) {
        assert_eq!(range.end - range.start, data.len() as u64);
        buffers.push_range(range, Bytes::from_static(data)).unwrap();
    }

    #[test]
    fn push_range_accepts_matching_length() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(10..14, Bytes::from_static(b"abcd"))
            .unwrap();
        assert!(buffers.has_range(&(10..14)));
    }

    #[test]
    fn push_range_rejects_short_buffer() {
        let mut buffers = PushBuffers::new(100);
        let err = buffers
            .push_range(10..20, Bytes::from_static(b"abcd"))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Buffer length (4) does not match length (10) of range 10..20"
        );
        assert!(!buffers.has_range(&(10..20)));
    }

    #[test]
    fn push_range_rejects_reversed_range() {
        let mut buffers = PushBuffers::new(100);
        let start = 20;
        let end = 10;
        let err = buffers.push_range(start..end, Bytes::new()).unwrap_err();
        assert_eq!(err.to_string(), "Parquet error: Invalid range 20..10");
    }

    #[test]
    fn push_ranges_rejects_mismatched_counts() {
        let mut buffers = PushBuffers::new(100);
        let err = buffers
            .push_ranges(vec![0..4, 4..8], vec![Bytes::from_static(b"abcd")])
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Number of ranges (2) must match number of buffers (1)"
        );
    }

    #[test]
    fn get_bytes_returns_exact_range() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 10..14, b"abcd");

        assert_eq!(&*buffers.get_bytes(10, 4).unwrap(), b"abcd");
        assert!(!buffers.has_range(&(9..14)));
        assert!(!buffers.has_range(&(10..15)));
    }

    #[test]
    fn get_bytes_returns_slice_from_larger_buffer() {
        let mut buffers = PushBuffers::new(100);
        let data = Bytes::from_static(b"0123456789");
        let pointer = data.as_ptr();
        buffers.push_range(10..20, data).unwrap();

        let result = buffers.get_bytes(13, 4).unwrap();
        assert_eq!(&*result, b"3456");
        assert_eq!(result.as_ptr(), unsafe { pointer.add(3) });
    }

    #[test]
    fn out_of_order_pushes_are_found() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 10..14, b"efgh");
        push_bytes(&mut buffers, 0..4, b"abcd");

        assert_eq!(&*buffers.get_bytes(0, 4).unwrap(), b"abcd");
        assert_eq!(&*buffers.get_bytes(10, 4).unwrap(), b"efgh");
    }

    #[test]
    fn multiple_small_non_overlapping_buffers_are_found() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 0..2, b"ab");
        push_bytes(&mut buffers, 4..6, b"ef");
        push_bytes(&mut buffers, 8..10, b"ij");

        for (start, expected) in [(0, &b"ab"[..]), (4, &b"ef"[..]), (8, &b"ij"[..])] {
            assert_eq!(&*buffers.get_bytes(start, 2).unwrap(), expected);
        }
        assert!(!buffers.has_range(&(2..4)));
    }

    #[test]
    fn overlapping_buffers_can_satisfy_a_range() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 0..10, b"0123456789");
        push_bytes(&mut buffers, 5..7, b"XY");

        assert!(buffers.has_range(&(5..7)));
        assert_eq!(&*buffers.get_bytes(5, 2).unwrap(), b"56");
    }

    #[test]
    fn nested_overlaps_keep_the_first_matching_buffer() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(0..100, Bytes::from(vec![b'A'; 100]))
            .unwrap();
        push_bytes(&mut buffers, 50..60, b"BBBBBBBBBB");
        push_bytes(&mut buffers, 70..80, b"CCCCCCCCCC");

        assert_eq!(&*buffers.get_bytes(70, 5).unwrap(), b"AAAAA");
    }

    #[test]
    fn repeated_ranges_are_not_overwritten() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 0..3, b"abc");
        push_bytes(&mut buffers, 0..3, b"xyz");

        assert_eq!(&*buffers.get_bytes(0, 3).unwrap(), b"abc");
    }

    #[test]
    fn same_start_different_end_ranges_are_preserved() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 0..5, b"abcde");
        push_bytes(&mut buffers, 0..2, b"xy");

        assert!(buffers.has_range(&(0..5)));
        assert_eq!(&*buffers.get_bytes(0, 2).unwrap(), b"ab");
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn zero_length_ranges_are_valid_and_have_no_buffered_bytes() {
        let mut buffers = PushBuffers::new(100);
        buffers.push_range(4..4, Bytes::new()).unwrap();

        assert!(buffers.has_range(&(4..4)));
        assert_eq!(buffers.buffered_bytes(), 0);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn remove_ids_removes_one_independent_entry() {
        let mut buffers = PushBuffers::new(100);
        let first_id = buffers
            .push_owned_range(0..4, Bytes::from_static(b"abcd"))
            .unwrap();
        push_bytes(&mut buffers, 4..8, b"efgh");

        buffers.remove_ids(&[first_id]);

        assert!(!buffers.has_range(&(0..4)));
        assert_eq!(&*buffers.get_bytes(4, 4).unwrap(), b"efgh");
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn remove_ids_removes_multiple_independent_entries() {
        let mut buffers = PushBuffers::new(100);
        let first_id = buffers
            .push_owned_range(0..2, Bytes::from_static(b"ab"))
            .unwrap();
        let middle_id = buffers
            .push_owned_range(2..4, Bytes::from_static(b"cd"))
            .unwrap();
        let last_id = buffers
            .push_owned_range(4..6, Bytes::from_static(b"ef"))
            .unwrap();

        buffers.remove_ids(&[first_id, last_id]);

        assert_eq!(&*buffers.get_bytes(2, 2).unwrap(), b"cd");
        assert!(!buffers.has_range(&(0..2)));
        assert!(!buffers.has_range(&(4..6)));
        buffers.remove_ids(&[middle_id]);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn remove_ids_supports_out_of_order_release() {
        let mut buffers = PushBuffers::new(100);
        let first_id = buffers
            .push_owned_range(0..4, Bytes::from_static(b"abcd"))
            .unwrap();
        let middle_id = buffers
            .push_owned_range(4..8, Bytes::from_static(b"efgh"))
            .unwrap();
        let last_id = buffers
            .push_owned_range(8..12, Bytes::from_static(b"ijkl"))
            .unwrap();

        buffers.remove_ids(&[last_id, first_id, middle_id]);

        assert_eq!(buffers.buffered_bytes(), 0);
        assert!(buffers.entries.is_empty());
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn removing_unrelated_ids_keeps_other_entries_readable() {
        let mut buffers = PushBuffers::new(100);
        push_bytes(&mut buffers, 0..10, b"0123456789");
        let removed_id = buffers
            .push_owned_range(20..24, Bytes::from_static(b"wxyz"))
            .unwrap();

        buffers.remove_ids(&[removed_id]);

        assert_eq!(&*buffers.get_bytes(3, 4).unwrap(), b"3456");
        assert!(!buffers.has_range(&(20..24)));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn removing_duplicate_ids_is_idempotent() {
        let mut buffers = PushBuffers::new(100);
        let first_id = buffers
            .push_owned_range(0..4, Bytes::from_static(b"abcd"))
            .unwrap();
        let second_id = buffers
            .push_owned_range(0..4, Bytes::from_static(b"WXYZ"))
            .unwrap();

        buffers.remove_ids(&[first_id, second_id]);
        assert_eq!(buffers.buffered_bytes(), 0);
        assert!(!buffers.has_range(&(0..4)));

        // Releasing the same independent range again is a no-op.
        buffers.remove_ids(&[first_id]);
        assert_eq!(buffers.buffered_bytes(), 0);
    }
}
