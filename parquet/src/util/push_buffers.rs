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
use std::fmt::Display;
use std::ops::Range;

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
///
/// # Ordering
///
/// The buffers are sorted by the start of their range. Thus, lookups use a
/// binary search, not a scan of all buffers. Pushed ranges can overlap.
#[derive(Debug, Clone, Default)]
pub struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The ranges of data that are available for decoding (not adjusted for
    /// offset), sorted by `start`
    ranges: Vec<Range<u64>>,
    /// The buffers of data that can be used to decode the Parquet file, in the
    /// same order as `ranges`
    buffers: Vec<Bytes>,
    /// The length of the longest range in `ranges`.
    ///
    /// This keeps lookups fast in the common case: buffers that do not
    /// overlap and have similar lengths. Pushed ranges can overlap, so a
    /// lookup cannot stop at the nearest buffer. `max_len` tells the lookup
    /// when no earlier buffer can reach the requested range, so it checks one
    /// or two buffers, not all of them. See [`Self::find`].
    max_len: u64,
    /// The sum of the lengths of `ranges`, kept up to date so that
    /// [`Self::buffered_bytes`] does not scan all ranges.
    buffered_bytes: u64,
    /// The number of empty ranges in `ranges`. [`Self::push_range`] accepts
    /// them, and [`Self::release_ranges`] removes them. The count tells
    /// `release_ranges` when it must look for them in all buffers.
    empty: usize,
}

impl Display for PushBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Buffers (offset: {}, file_len: {})",
            self.offset, self.file_len
        )?;
        writeln!(f, "Available Ranges (w/ offset):")?;
        for range in &self.ranges {
            writeln!(
                f,
                "  {}..{} ({}..{}): {} bytes",
                range.start,
                range.end,
                range.start + self.offset,
                range.end + self.offset,
                range.end - range.start
            )?;
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
            ranges: Vec::new(),
            buffers: Vec::new(),
            max_len: 0,
            buffered_bytes: 0,
            empty: 0,
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
        let expected = range.end.saturating_sub(range.start);
        if expected != buffer.len() as u64 {
            return Err(general_err!(
                "Buffer length ({}) does not match length ({}) of range {}..{}",
                buffer.len(),
                expected,
                range.start,
                range.end
            ));
        }
        // Insert after all buffers that start at or before `range.start`.
        // Thus, ranges pushed in file order go at the end.
        let idx = self.ranges.partition_point(|r| r.start <= range.start);
        self.max_len = self.max_len.max(expected);
        self.buffered_bytes += expected;
        self.empty += usize::from(expected == 0);
        self.ranges.insert(idx, range);
        self.buffers.insert(idx, buffer);
        Ok(())
    }

    /// Returns true if the Buffers contains data for the given range
    pub(crate) fn has_range(&self, range: &Range<u64>) -> bool {
        self.find(range.start, range.end).is_some()
    }

    /// Returns the index of a buffer that contains all bytes of `start..end`,
    /// if any.
    fn find(&self, start: u64, end: u64) -> Option<usize> {
        // Common case: the buffers do not overlap. Then only the last buffer
        // that starts at or before `start` can contain `start..end`:
        //
        //   buffers:  0..25    ├─────────┤
        //             25..50             ├─────────┤
        //             50..75                       ├─────────┤
        //             75..100                                ├─────────┤
        //   find:     55..70                         ├─────┤  only 50..75 can contain it
        //
        // But pushed buffers can overlap. Then a buffer that starts much
        // earlier can be the one that contains `start..end`:
        //
        //   buffers:  0..100   ├───────────────────────────────────────┤
        //             50..60                       ├───┤
        //             55..58                         ├┤
        //   find:     55..90                         ├─────────────┤  only 0..100 contains it
        //
        // Thus, scan back from the last buffer that starts at or before
        // `start`. Without a limit, a lookup that finds nothing scans all
        // earlier buffers. `max_len` gives the limit: a buffer that starts
        // more than `max_len` bytes before `end` ends before `end`, and so do
        // all buffers before it. Stop there.
        //
        // In the common case the scan stops after one or two buffers. It is
        // long only if a caller pushes one large buffer and then many small
        // buffers after its start.
        let candidates = self.ranges.partition_point(|r| r.start <= start);
        self.ranges[..candidates]
            .iter()
            .enumerate()
            .rev()
            .take_while(|(_, r)| r.start.saturating_add(self.max_len) >= end)
            .find(|(_, r)| r.end >= end)
            .map(|(idx, _)| idx)
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

    /// Return the total length of all buffered ranges.
    ///
    /// This counts the bytes that the decoder can still read, not the
    /// allocated memory. [`Self::release_ranges`] keeps the remaining parts of
    /// a buffer as slices of the pushed [`Bytes`], and the allocation of a
    /// pushed [`Bytes`] is freed only after all of its parts are released.
    #[cfg(feature = "arrow")]
    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Clear any range and corresponding buffer that is exactly in the ranges_to_clear
    ///
    /// A binary search finds the buffers that start at the start of each
    /// range to clear, as in [`Self::find`]. Thus, a call does not visit the
    /// other buffers.
    #[cfg(feature = "arrow")]
    pub(crate) fn clear_ranges(&mut self, ranges_to_clear: &[Range<u64>]) {
        // The indexes of the buffers to remove. The buffers are sorted by
        // start, so all buffers with the same start are adjacent.
        let mut remove = vec![];
        for clear in ranges_to_clear {
            let first = self.ranges.partition_point(|r| r.start < clear.start);
            let same_start = self.ranges[first..]
                .iter()
                .take_while(|r| r.start == clear.start);
            remove.extend(
                same_start
                    .enumerate()
                    .filter(|(_, r)| r.end == clear.end)
                    .map(|(idx, _)| first + idx),
            );
        }
        // Buffers with the same start are not sorted by end, and
        // `ranges_to_clear` can contain a range more than once.
        remove.sort_unstable();
        remove.dedup();
        let (Some(&first), Some(&last)) = (remove.first(), remove.last()) else {
            return;
        };
        // Move the kept buffers in `first..=last` to the start of that span,
        // in order, then remove the rest of the span. The buffers outside the
        // span are not visited.
        let mut removed_longest = false;
        let mut kept = first;
        let mut remove = remove.into_iter().peekable();
        for idx in first..=last {
            if remove.next_if_eq(&idx).is_some() {
                let len = self.ranges[idx].end.saturating_sub(self.ranges[idx].start);
                self.buffered_bytes -= len;
                self.empty -= usize::from(len == 0);
                removed_longest |= len == self.max_len;
            } else {
                self.ranges.swap(kept, idx);
                self.buffers.swap(kept, idx);
                kept += 1;
            }
        }
        self.ranges.drain(kept..=last);
        self.buffers.drain(kept..=last);
        // `max_len` can change only if a buffer of that length was removed.
        if removed_longest {
            self.update_max_len();
        }
    }

    /// Set `max_len` to the maximum length of the remaining ranges.
    ///
    /// A `max_len` that is too large is still correct, lookups only scan
    /// further. This update is for performance: a large buffer that was
    /// removed must not slow down later lookups.
    #[cfg(feature = "arrow")]
    fn update_max_len(&mut self) {
        self.max_len = self
            .ranges
            .iter()
            .map(|r| r.end.saturating_sub(r.start))
            .max()
            .unwrap_or(0);
    }

    /// Remove all buffered bytes in `ranges`.
    ///
    /// A buffer that overlaps a range is trimmed or split, and the parts
    /// outside the range are kept. The kept parts are zero-copy slices, which
    /// record the bytes that the decoder can still read. Thus, the allocator
    /// frees the memory of a pushed [`Bytes`] only after all of its parts are
    /// removed.
    ///
    /// The buffers stay sorted by start, which lookups require (see
    /// [`Self::find`]). If no buffer overlaps `ranges`, the buffers do not
    /// change.
    ///
    /// A binary search finds the buffers that can overlap `ranges`, as in
    /// [`Self::find`]. Thus, a call does not visit the other buffers.
    #[cfg(feature = "arrow")]
    pub(crate) fn release_ranges(&mut self, ranges: &[Range<u64>]) {
        let release = merge_ranges(ranges);
        if release.is_empty() {
            return;
        }
        let overlaps = |range: &Range<u64>| {
            !range.is_empty() && {
                let first = release.partition_point(|r| r.end <= range.start);
                release.get(first).is_some_and(|r| r.start < range.end)
            }
        };
        // Find the buffers that overlap a released range. The buffers are
        // sorted by start, and no buffer is longer than `max_len`. Thus, a
        // buffer can overlap `r` only if it starts before `r.end` and less
        // than `max_len` bytes before `r.start`:
        //
        //   buffers:  0..25    ├─────────┤
        //             25..50             ├─────────┤
        //             50..75                       ├─────────┤
        //             75..100                                ├─────────┤
        //   release:  55..70                         ├─────┤  max_len is 25: check 50..75 only
        let mut overlapping = vec![];
        let mut visited = 0;
        for r in &release {
            let max_len = self.max_len;
            let first = self
                .ranges
                .partition_point(|b| b.start.saturating_add(max_len) <= r.start);
            let end = self.ranges.partition_point(|b| b.start < r.end);
            // A buffer that an earlier released range visited is not visited
            // again, because `overlaps` checks all released ranges.
            overlapping
                .extend((first.max(visited)..end).filter(|&idx| overlaps(&self.ranges[idx])));
            visited = visited.max(end);
        }
        // Most calls release ranges that are no longer buffered. Return
        // before changing the buffers.
        let (Some(&first), Some(&last)) = (overlapping.first(), overlapping.last()) else {
            return;
        };
        // Trim the buffers that overlap a released range in place, and do not
        // clone or move the others. The parts after the first part of a split
        // buffer go to `split`. A buffer that is released entirely becomes
        // empty, and is removed below.
        let mut split = vec![];
        let mut emptied = false;
        let mut longest = false;
        for idx in overlapping {
            let (range, buffer) = (&mut self.ranges[idx], &mut self.buffers[idx]);
            let whole = range.clone();
            longest |= whole.end - whole.start == self.max_len;
            let offset = |pos: u64| (pos - whole.start) as usize;
            // The parts of `whole` between the released ranges.
            let first = release.partition_point(|r| r.end <= whole.start);
            let mut kept = 0;
            let mut first_part = None;
            let mut keep = |part: Range<u64>| {
                kept += part.end - part.start;
                if first_part.is_none() {
                    first_part = Some(part);
                } else {
                    let data = buffer.slice(offset(part.start)..offset(part.end));
                    split.push((part, data));
                }
            };
            let mut start = whole.start;
            for r in release[first..].iter().take_while(|r| r.start < whole.end) {
                if start < r.start {
                    keep(start..r.start);
                }
                start = r.end;
            }
            if start < whole.end {
                keep(start..whole.end);
            }
            self.buffered_bytes -= (whole.end - whole.start) - kept;
            match first_part {
                Some(part) => {
                    buffer.truncate(offset(part.end));
                    let _ = buffer.split_to(offset(part.start));
                    *range = part;
                }
                None => {
                    *buffer = Bytes::new();
                    *range = whole.start..whole.start;
                    emptied = true;
                }
            }
        }
        if emptied || self.empty > 0 {
            // Remove the released buffers. As before, this also removes empty
            // buffers that were pushed. The released buffers are all in
            // `first..=last`, so look at all buffers only if empty buffers
            // were pushed. The buffers after the removed ones still move down
            // (one `memmove` for each `drain`), which is much cheaper than a
            // visit of each buffer.
            let span = match self.empty {
                0 => first..last + 1,
                _ => 0..self.ranges.len(),
            };
            let mut kept = span.start;
            for idx in span.clone() {
                if !self.ranges[idx].is_empty() {
                    self.ranges.swap(kept, idx);
                    self.buffers.swap(kept, idx);
                    kept += 1;
                }
            }
            self.ranges.drain(kept..span.end);
            self.buffers.drain(kept..span.end);
            self.empty = 0;
        }
        // A trim moves the start of a buffer only past released bytes. Each
        // other buffer that starts in those bytes is trimmed past them too,
        // or is empty and removed. Thus, the buffers are still sorted, except
        // for the split parts, which are appended. Splits are rare, so sort
        // all buffers then. The sort is stable.
        if !split.is_empty() {
            for (range, buffer) in split {
                self.ranges.push(range);
                self.buffers.push(buffer);
            }
            let mut parts: Vec<_> = std::mem::take(&mut self.ranges)
                .into_iter()
                .zip(std::mem::take(&mut self.buffers))
                .collect();
            parts.sort_by_key(|(range, _)| range.start);
            (self.ranges, self.buffers) = parts.into_iter().unzip();
        }
        // `max_len` can change only if a buffer of that length was trimmed.
        // Only then pay for the pass over all buffers, which keeps `max_len`
        // exact (see `update_max_len`).
        if longest {
            self.update_max_len();
        }
    }

    /// Remove all buffered bytes outside `keep`.
    ///
    /// This calls [`Self::release_ranges`] with the complement of `keep`, so
    /// the same rules apply to the kept parts.
    #[cfg(feature = "arrow")]
    pub(crate) fn retain_ranges(&mut self, keep: &[Range<u64>]) {
        let mut release = vec![];
        let mut start = 0;
        for range in merge_ranges(keep) {
            release.push(start..range.start);
            start = range.end;
        }
        release.push(start..u64::MAX);
        self.release_ranges(&release);
    }

    /// Clear all buffered ranges and their corresponding data
    pub(crate) fn clear_all_ranges(&mut self) {
        self.ranges.clear();
        self.buffers.clear();
        self.max_len = 0;
        self.buffered_bytes = 0;
        self.empty = 0;
    }

    /// Panics if `ranges`, `buffers` and `max_len` do not agree, or if the
    /// buffers are not sorted.
    #[cfg(test)]
    #[track_caller]
    fn assert_invariants(&self) {
        assert_eq!(self.ranges.len(), self.buffers.len());
        assert!(
            self.ranges.is_sorted_by_key(|r| r.start),
            "not sorted: {:?}",
            self.ranges
        );
        for (range, buffer) in self.ranges.iter().zip(&self.buffers) {
            assert_eq!(range.end.saturating_sub(range.start), buffer.len() as u64);
        }
        let max_len = self
            .ranges
            .iter()
            .map(|r| r.end.saturating_sub(r.start))
            .max();
        assert_eq!(self.max_len, max_len.unwrap_or(0));
        let empty = self.ranges.iter().filter(|r| r.is_empty()).count();
        assert_eq!(self.empty, empty);
    }
}

/// Sort `ranges` by start, remove empty ranges, and merge ranges that
/// overlap or are adjacent.
#[cfg(feature = "arrow")]
fn merge_ranges(ranges: &[Range<u64>]) -> Vec<Range<u64>> {
    let mut sorted: Vec<Range<u64>> = ranges.iter().filter(|r| !r.is_empty()).cloned().collect();
    sorted.sort_unstable_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    merged
}

impl Length for PushBuffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

/// less efficient implementation of Read for Buffers
impl std::io::Read for PushBuffers {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Find the range that contains the start offset
        let found = self.find(self.offset, self.offset + buf.len() as u64);
        if let Some(idx) = found {
            // Found the range, figure out the starting offset in the buffer
            let start_offset = (self.offset - self.ranges[idx].start) as usize;
            let end_offset = start_offset + buf.len();
            buf.copy_from_slice(&self.buffers[idx][start_offset..end_offset]);
            // If we found the range, we can return the number of bytes read
            // advance our offset
            self.offset += buf.len() as u64;
            Ok(buf.len())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available in Buffers",
            ))
        }
    }
}

impl ChunkReader for PushBuffers {
    type T = Self;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(self.clone().with_offset(self.offset + start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        // find the range that contains the start offset
        if let Some(idx) = self.find(start, start + length as u64) {
            // Found the range, figure out the starting offset in the buffer
            let start_offset = (start - self.ranges[idx].start) as usize;
            return Ok(self.buffers[idx].slice(start_offset..start_offset + length));
        }
        // Signal that we need more data
        let requested_end = start + length as u64;
        Err(ParquetError::NeedMoreDataRange(start..requested_end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Release one range.
    #[cfg(feature = "arrow")]
    fn release(buffers: &mut PushBuffers, range: Range<u64>) {
        buffers.release_ranges(std::slice::from_ref(&range));
        buffers.assert_invariants();
    }

    /// The buffered ranges and their data.
    #[cfg(feature = "arrow")]
    fn buffered_parts(buffers: &PushBuffers) -> Vec<(Range<u64>, Bytes)> {
        let ranges = buffers.ranges.iter().cloned();
        ranges.zip(buffers.buffers.iter().cloned()).collect()
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn release_ranges_without_overlap_keeps_buffers() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(20..24, Bytes::from_static(b"abcd"))
            .unwrap();
        buffers
            .push_range(0..10, Bytes::from_static(b"0123456789"))
            .unwrap();

        release(&mut buffers, 90..95);
        release(&mut buffers, 10..20);
        assert_eq!(buffers.ranges, vec![0..10, 20..24]);
        assert_eq!(buffers.buffered_bytes(), 14);

        release(&mut buffers, 22..24);
        assert_eq!(buffers.ranges, vec![0..10, 20..22]);
        assert_eq!(buffers.buffered_bytes(), 12);

        buffers.clear_all_ranges();
        assert_eq!(buffers.buffered_bytes(), 0);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn release_ranges_trims_and_splits_buffers() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(0..10, Bytes::from_static(b"0123456789"))
            .unwrap();
        buffers
            .push_range(20..24, Bytes::from_static(b"abcd"))
            .unwrap();

        // Split the first buffer, and do not change the second buffer.
        release(&mut buffers, 3..5);
        assert_eq!(buffers.buffered_bytes(), 12);
        assert!(buffers.has_range(&(0..3)));
        assert!(buffers.has_range(&(5..10)));
        assert!(!buffers.has_range(&(3..4)));
        assert_eq!(
            buffers.get_bytes(5, 5).unwrap(),
            Bytes::from_static(b"56789")
        );
        assert!(buffers.has_range(&(20..24)));

        // One range that overlaps two buffers trims both of them.
        release(&mut buffers, 8..22);
        assert_eq!(buffers.buffered_bytes(), 3 + 3 + 2);
        assert_eq!(buffers.get_bytes(5, 3).unwrap(), Bytes::from_static(b"567"));
        assert_eq!(buffers.get_bytes(22, 2).unwrap(), Bytes::from_static(b"cd"));

        // Ranges in any order, adjacent or empty, and bytes that are not
        // buffered.
        buffers.release_ranges(&[50..60, 1..2, 0..1, 7..7]);
        assert_eq!(buffers.buffered_bytes(), 1 + 3 + 2);
        assert_eq!(buffers.get_bytes(2, 1).unwrap(), Bytes::from_static(b"2"));

        release(&mut buffers, 0..100);
        assert_eq!(buffers.buffered_bytes(), 0);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn release_ranges_removes_exact_and_overlapping_buffers() {
        let mut buffers = PushBuffers::new(100);
        for range in [10..20, 0..30, 10..15, 40..50] {
            let data = Bytes::from(vec![0u8; (range.end - range.start) as usize]);
            buffers.push_range(range, data).unwrap();
        }
        buffers.release_ranges(&[40..50, 10..15]);
        buffers.assert_invariants();
        assert_eq!(buffers.buffered_bytes(), 10 + 5 + 15);
        assert!(buffers.has_range(&(0..10)));
        assert!(buffers.has_range(&(15..30)));
        assert!(!buffers.has_range(&(10..15)));
        assert!(!buffers.has_range(&(40..50)));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn retain_ranges_keeps_only_the_given_bytes() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(0..10, Bytes::from_static(b"0123456789"))
            .unwrap();
        buffers
            .push_range(20..24, Bytes::from_static(b"abcd"))
            .unwrap();
        buffers
            .push_range(30..32, Bytes::from_static(b"xy"))
            .unwrap();
        // Ranges in any order, overlapping, and outside the buffers.
        buffers.retain_ranges(&[22..40, 2..4, 3..5, 8..9]);
        assert_eq!(buffers.buffered_bytes(), 3 + 1 + 2 + 2);
        assert_eq!(buffers.get_bytes(2, 3).unwrap(), Bytes::from_static(b"234"));
        assert_eq!(buffers.get_bytes(8, 1).unwrap(), Bytes::from_static(b"8"));
        assert!(!buffers.has_range(&(5..6)));
        assert_eq!(buffers.get_bytes(22, 2).unwrap(), Bytes::from_static(b"cd"));
        assert_eq!(buffers.get_bytes(30, 2).unwrap(), Bytes::from_static(b"xy"));
        buffers.retain_ranges(&[]);
        assert_eq!(buffers.buffered_bytes(), 0);
    }

    /// Lookups stay correct when the kept parts of overlapping buffers
    /// interleave.
    #[test]
    #[cfg(feature = "arrow")]
    fn retain_ranges_with_overlapping_buffers() {
        let mut buffers = PushBuffers::new(100);
        for range in [0..100, 3..70] {
            let data = Bytes::from(vec![0u8; (range.end - range.start) as usize]);
            buffers.push_range(range, data).unwrap();
        }
        buffers.retain_ranges(&[0..5, 50..60]);
        buffers.assert_invariants();
        assert_eq!(buffers.buffered_bytes(), 5 + 2 + 10 + 10);
        assert!(buffers.has_range(&(0..5)));
        assert!(buffers.has_range(&(50..60)));
        assert!(!buffers.has_range(&(5..50)));
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

    /// The bytes of a fake file: byte `i` is `i % 251`, so any slice of it is
    /// easy to build and to check.
    fn file_bytes(range: Range<u64>) -> Bytes {
        range.map(|i| (i % 251) as u8).collect::<Vec<u8>>().into()
    }

    fn push(buffers: &mut PushBuffers, range: Range<u64>) {
        buffers
            .push_range(range.clone(), file_bytes(range))
            .unwrap();
    }

    /// Checks the invariants, and that each buffer still has the bytes of its
    /// range.
    #[track_caller]
    fn assert_valid(buffers: &PushBuffers) {
        buffers.assert_invariants();
        for (range, buffer) in buffers.ranges.iter().zip(&buffers.buffers) {
            assert_eq!(*buffer, file_bytes(range.clone()));
        }
    }

    #[test]
    fn overlapping_pushes_find_the_containing_buffer() {
        let mut buffers = PushBuffers::new(1000);
        // Small buffers inside a large one start closer to most offsets.
        // The pushes are not in file order: `assert_valid` checks the sort.
        for range in [50..60, 0..100, 55..58, 10..20] {
            push(&mut buffers, range);
        }
        assert_valid(&buffers);
        // 55..90 starts in 50..60 and 55..58, but only 0..100 contains it.
        assert_eq!(buffers.get_bytes(55, 35).unwrap(), file_bytes(55..90));
        assert!(buffers.has_range(&(0..100)));
        assert!(buffers.has_range(&(99..100)));
        assert!(!buffers.has_range(&(99..101)));
        assert!(matches!(
            buffers.get_bytes(90, 20),
            Err(ParquetError::NeedMoreDataRange(r)) if r == (90..110)
        ));

        // `Read` finds the same buffer.
        let mut reader = buffers.get_read(56).unwrap();
        let mut out = [0u8; 30];
        std::io::Read::read_exact(&mut reader, &mut out).unwrap();
        assert_eq!(&out[..], &file_bytes(56..86)[..]);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn clear_ranges_drops_exact_matches_only() {
        let mut buffers = PushBuffers::new(1000);
        for range in [10..20, 0..30, 10..15, 40..50] {
            push(&mut buffers, range);
        }
        buffers.clear_ranges(&[40..50, 10..15, 5..30]);
        assert_valid(&buffers);
        assert_eq!(buffers.ranges, vec![0..30, 10..20]);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn clear_ranges_with_buffers_of_the_same_start() {
        let mut buffers = PushBuffers::new(1000);
        for range in [0..5, 10..20, 10..15, 10..20, 10..30, 10..12, 40..50] {
            push(&mut buffers, range);
        }
        // Buffers with the same start are not sorted by end. A range to clear
        // can match more than one buffer, and can be given more than once.
        buffers.clear_ranges(&[10..30, 10..20, 10..20, 60..70]);
        assert_valid(&buffers);
        assert_eq!(buffers.ranges, vec![0..5, 10..15, 10..12, 40..50]);
        assert_eq!(buffers.buffered_bytes(), 22);

        // No buffer matches: the buffers do not change.
        buffers.clear_ranges(&[10..20, 0..50]);
        assert_valid(&buffers);
        assert_eq!(buffers.ranges, vec![0..5, 10..15, 10..12, 40..50]);
    }

    /// Random pushes, clears and lookups, compared with a list that is
    /// scanned in full for each lookup.
    #[test]
    #[cfg(feature = "arrow")]
    fn fuzz_matches_a_linear_scan() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        fn random_range(rng: &mut StdRng) -> Range<u64> {
            let start = rng.random_range(0..500);
            let len = [0, 1, 5, 20, 100, 300][rng.random_range(0..6)];
            start..start + len
        }

        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut buffers = PushBuffers::new(1000);
            let mut model: Vec<Range<u64>> = vec![];
            for _ in 0..60 {
                match rng.random_range(0..10) {
                    0..4 => {
                        let range = random_range(&mut rng);
                        push(&mut buffers, range.clone());
                        model.push(range);
                    }
                    4..6 => {
                        // Clear some pushed ranges and some other ranges.
                        let mut clear: Vec<_> = (0..rng.random_range(1..4))
                            .map(|_| random_range(&mut rng))
                            .collect();
                        if !model.is_empty() {
                            clear.push(model[rng.random_range(0..model.len())].clone());
                        }
                        buffers.clear_ranges(&clear);
                        model.retain(|r| !clear.contains(r));
                    }
                    _ => {
                        let range = random_range(&mut rng);
                        let expected = model
                            .iter()
                            .any(|r| r.start <= range.start && r.end >= range.end);
                        assert_eq!(buffers.has_range(&range), expected, "seed {seed} {range:?}");
                        if expected {
                            let len = (range.end - range.start) as usize;
                            assert_eq!(
                                buffers.get_bytes(range.start, len).unwrap(),
                                file_bytes(range.clone())
                            );
                        }
                    }
                }
                assert_valid(&buffers);
                let mut actual = buffers.ranges.clone();
                actual.sort_by_key(|r| (r.start, r.end));
                model.sort_by_key(|r| (r.start, r.end));
                assert_eq!(actual, model, "seed {seed}");
            }
        }
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

    /// The result of `release_ranges`, computed by rebuilding all buffers.
    #[cfg(feature = "arrow")]
    fn release_by_rebuild(
        buffers: &PushBuffers,
        release: &[Range<u64>],
    ) -> Vec<(Range<u64>, Bytes)> {
        let release = merge_ranges(release);
        let mut parts = vec![];
        for (range, buffer) in &buffered_parts(buffers) {
            let mut start = range.start;
            let mut keep = |part: Range<u64>| {
                let offset = |pos: u64| (pos - range.start) as usize;
                parts.push((
                    part.clone(),
                    buffer.slice(offset(part.start)..offset(part.end)),
                ));
            };
            for r in release
                .iter()
                .filter(|r| r.start < range.end && r.end > range.start)
            {
                if start < r.start {
                    keep(start..r.start);
                }
                start = start.max(r.end);
            }
            if start < range.end {
                keep(start..range.end);
            }
        }
        parts.sort_by_key(|(range, _)| range.start);
        parts
    }

    #[test]
    #[cfg(feature = "arrow")]
    #[expect(clippy::reversed_empty_ranges)]
    fn release_ranges_splits_and_removes_empty_buffers() {
        let mut buffers = PushBuffers::new(100);
        buffers
            .push_range(0..20, Bytes::from_static(b"abcdefghijklmnopqrst"))
            .unwrap();
        // An empty and an inverted range, which `push_range` accepts.
        buffers.push_range(30..30, Bytes::new()).unwrap();
        buffers.push_range(10..5, Bytes::new()).unwrap();

        // Split one buffer into three parts.
        buffers.release_ranges(&[3..5, 8..10]);
        assert_eq!(buffers.ranges, vec![0..3, 5..8, 10..20]);
        assert_eq!(buffers.buffered_bytes(), 16);
        assert_eq!(buffers.get_bytes(5, 3).unwrap(), Bytes::from_static(b"fgh"));

        // Release a whole buffer.
        release(&mut buffers, 5..8);
        assert_eq!(buffers.ranges, vec![0..3, 10..20]);
        assert_eq!(buffers.buffered_bytes(), 13);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn fuzz_release_ranges_matches_rebuild() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};
        let data: Vec<u8> = (0..=255).collect();
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..2000 {
            let mut buffers = PushBuffers::new(256);
            for _ in 0..rng.random_range(0..6) {
                let start = rng.random_range(0..100u64);
                if rng.random_bool(0.1) {
                    // An inverted range, which `push_range` accepts.
                    let end = start.saturating_sub(rng.random_range(1..5u64));
                    buffers.push_range(start..end, Bytes::new()).unwrap();
                    continue;
                }
                let end = start + rng.random_range(0..20u64);
                let bytes = Bytes::copy_from_slice(&data[start as usize..end as usize]);
                buffers.push_range(start..end, bytes).unwrap();
            }
            for _ in 0..3 {
                let release: Vec<_> = (0..rng.random_range(0..4))
                    .map(|_| {
                        let start = rng.random_range(0..110u64);
                        start..start + rng.random_range(0..15u64)
                    })
                    .collect();
                let overlaps = buffers.ranges.iter().any(|range| {
                    !range.is_empty()
                        && release
                            .iter()
                            .any(|r| !r.is_empty() && r.start < range.end && r.end > range.start)
                });
                let before = buffered_parts(&buffers);
                let expected = release_by_rebuild(&buffers, &release);

                buffers.release_ranges(&release);
                buffers.assert_invariants();
                let actual = buffered_parts(&buffers);
                if overlaps {
                    // The order of buffers with the same start is not
                    // specified.
                    let by_range = |mut parts: Vec<(Range<u64>, Bytes)>| {
                        parts.sort_by_key(|(range, _)| (range.start, range.end));
                        parts
                    };
                    assert_eq!(
                        by_range(actual.clone()),
                        by_range(expected),
                        "release {release:?} of {before:?}"
                    );
                } else {
                    assert_eq!(actual, before, "release {release:?} of {before:?}");
                }
                for (range, buffer) in actual.iter().filter(|(range, _)| !range.is_empty()) {
                    assert_eq!(
                        &data[range.start as usize..range.end as usize],
                        buffer.as_ref()
                    );
                }
                let total: u64 = actual
                    .iter()
                    .map(|(r, _)| r.end.saturating_sub(r.start))
                    .sum();
                assert_eq!(buffers.buffered_bytes(), total);
            }
        }
    }
}
