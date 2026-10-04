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
#[derive(Debug, Clone, Default)]
pub struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The ranges of data that are available for decoding (not adjusted for offset)
    ranges: Vec<Range<u64>>,
    /// The buffers of data that can be used to decode the Parquet file
    buffers: Vec<Bytes>,
    /// The sum of the lengths of `ranges`, kept up to date so that
    /// [`Self::buffered_bytes`] does not scan all ranges.
    buffered_bytes: u64,
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
            buffered_bytes: 0,
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
        self.buffered_bytes += expected;
        self.ranges.push(range);
        self.buffers.push(buffer);
        Ok(())
    }

    /// Returns true if the Buffers contains data for the given range
    pub(crate) fn has_range(&self, range: &Range<u64>) -> bool {
        self.ranges
            .iter()
            .any(|r| r.start <= range.start && r.end >= range.end)
    }

    fn iter(&self) -> impl Iterator<Item = (&Range<u64>, &Bytes)> {
        self.ranges.iter().zip(self.buffers.iter())
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

    /// Return the total of all buffered ranges
    #[cfg(feature = "arrow")]
    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Clear any range and corresponding buffer that is exactly in the ranges_to_clear
    #[cfg(feature = "arrow")]
    pub(crate) fn clear_ranges(&mut self, ranges_to_clear: &[Range<u64>]) {
        let mut new_ranges = Vec::new();
        let mut new_buffers = Vec::new();

        for (range, buffer) in self.iter() {
            if !ranges_to_clear
                .iter()
                .any(|r| r.start == range.start && r.end == range.end)
            {
                new_ranges.push(range.clone());
                new_buffers.push(buffer.clone());
            }
        }
        self.buffered_bytes = new_ranges.iter().map(|r| r.end - r.start).sum();
        self.ranges = new_ranges;
        self.buffers = new_buffers;
    }

    /// Remove all buffered bytes in `ranges`, whatever the shape of the
    /// pushed buffers.
    ///
    /// A buffer that overlaps a range is trimmed or split, and the parts
    /// outside the range are kept. The kept parts are zero-copy slices. Thus,
    /// the allocator frees the memory of a pushed [`Bytes`] only after all of
    /// its parts are removed.
    ///
    /// If the buffers are sorted by start, they stay sorted. The order of
    /// buffers with the same start is not specified. If no buffer overlaps
    /// `ranges`, the buffers do not change.
    #[cfg(feature = "arrow")]
    pub(crate) fn release_ranges(&mut self, ranges: &[Range<u64>]) {
        let release = merge_ranges(ranges);
        if release.is_empty() {
            return;
        }
        // Most calls release ranges that are no longer buffered. Return
        // before reallocating the buffers.
        let overlaps = |range: &Range<u64>| {
            !range.is_empty() && {
                let first = release.partition_point(|r| r.end <= range.start);
                release.get(first).is_some_and(|r| r.start < range.end)
            }
        };
        if !self.ranges.iter().any(overlaps) {
            return;
        }
        // Trim the buffers that overlap a released range in place, and do not
        // clone or move the others. The parts after the first part of a split
        // buffer go to `split`. A buffer that is released entirely becomes
        // empty, and is removed below.
        let mut split = vec![];
        let mut emptied = false;
        for (range, buffer) in self.ranges.iter_mut().zip(self.buffers.iter_mut()) {
            if range.is_empty() {
                emptied = true;
                continue;
            }
            if !overlaps(range) {
                continue;
            }
            let whole = range.clone();
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
        if emptied {
            // Remove the released buffers. As before, this also removes empty
            // buffers that were pushed. `retain` visits each element exactly
            // once in the original order, so both calls keep the same indices.
            let mut keep = self.ranges.iter().map(|range| !range.is_empty());
            self.buffers.retain(|_| keep.next().unwrap());
            self.ranges.retain(|range| !range.is_empty());
        }
        for (range, buffer) in split {
            self.ranges.push(range);
            self.buffers.push(buffer);
        }
        // Split parts are appended, and if buffers overlap, the tail of a
        // trimmed buffer can start after the start of the next buffer. The
        // sort is stable.
        if !self.ranges.is_sorted_by_key(|range| range.start) {
            let mut parts: Vec<_> = std::mem::take(&mut self.ranges)
                .into_iter()
                .zip(std::mem::take(&mut self.buffers))
                .collect();
            parts.sort_by_key(|(range, _)| range.start);
            (self.ranges, self.buffers) = parts.into_iter().unzip();
        }
    }

    /// Remove all buffered bytes outside `keep`, whatever the shape of the
    /// pushed buffers.
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
        self.buffered_bytes = 0;
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
        let mut found = false;
        for (range, data) in self.iter() {
            if range.start <= self.offset && range.end >= self.offset + buf.len() as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (self.offset - range.start) as usize;
                let end_offset = start_offset + buf.len();
                let slice = data.slice(start_offset..end_offset);
                buf.copy_from_slice(slice.as_ref());
                found = true;
                break;
            }
        }
        if found {
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
        for (range, data) in self.iter() {
            if range.start <= start && range.end >= start + length as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (start - range.start) as usize;
                return Ok(data.slice(start_offset..start_offset + length));
            }
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
        assert_eq!(buffers.ranges, vec![20..24, 0..10]);
        assert_eq!(buffers.buffered_bytes(), 14);

        // A release that overlaps a buffer sorts the buffers.
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
        assert_eq!(buffers.ranges, vec![0..10, 15..20, 15..30]);
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

    /// Parts of overlapping buffers stay sorted by start.
    #[test]
    #[cfg(feature = "arrow")]
    fn retain_ranges_keeps_the_buffers_sorted() {
        let mut buffers = PushBuffers::new(100);
        for range in [0..100, 3..70] {
            let data = Bytes::from(vec![0u8; (range.end - range.start) as usize]);
            buffers.push_range(range, data).unwrap();
        }
        buffers.retain_ranges(&[0..5, 50..60]);
        assert_eq!(buffers.ranges, vec![0..5, 3..5, 50..60, 50..60]);
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
        for (range, buffer) in buffers.iter() {
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
    fn release_ranges_matches_rebuild() {
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
                let overlaps = buffers.iter().any(|(range, _)| {
                    !range.is_empty()
                        && release
                            .iter()
                            .any(|r| !r.is_empty() && r.start < range.end && r.end > range.start)
                });
                let before: Vec<_> = buffers
                    .iter()
                    .map(|(r, b)| (r.clone(), b.clone()))
                    .collect();
                let expected = release_by_rebuild(&buffers, &release);

                buffers.release_ranges(&release);
                let actual: Vec<_> = buffers
                    .iter()
                    .map(|(r, b)| (r.clone(), b.clone()))
                    .collect();
                if overlaps {
                    // The buffers are sorted by start. The order of buffers
                    // with the same start is not specified.
                    assert!(actual.is_sorted_by_key(|(range, _)| range.start));
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
