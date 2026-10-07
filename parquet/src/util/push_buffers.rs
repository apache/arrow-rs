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

    /// Return the total of all buffered ranges
    #[cfg(feature = "arrow")]
    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.end - r.start).sum()
    }

    /// Clear any range and corresponding buffer that is exactly in the ranges_to_clear
    #[cfg(feature = "arrow")]
    pub(crate) fn clear_ranges(&mut self, ranges_to_clear: &[Range<u64>]) {
        // Use `(start, end)` tuples because `Range` is not `Ord`
        let mut clear: Vec<(u64, u64)> = ranges_to_clear.iter().map(|r| (r.start, r.end)).collect();
        if clear.is_empty() {
            return;
        }
        clear.sort_unstable();
        let mut ranges = Vec::with_capacity(self.ranges.len());
        let mut buffers = Vec::with_capacity(self.buffers.len());
        for (range, buffer) in self.ranges.drain(..).zip(self.buffers.drain(..)) {
            if clear.binary_search(&(range.start, range.end)).is_err() {
                ranges.push(range);
                buffers.push(buffer);
            }
        }
        self.ranges = ranges;
        self.buffers = buffers;
        self.update_max_len();
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
            .map(|r| r.end - r.start)
            .max()
            .unwrap_or(0);
    }

    /// Clear all buffered ranges and their corresponding data
    pub(crate) fn clear_all_ranges(&mut self) {
        self.ranges.clear();
        self.buffers.clear();
        self.max_len = 0;
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
            assert_eq!(range.end - range.start, buffer.len() as u64);
        }
        let max_len = self.ranges.iter().map(|r| r.end - r.start).max();
        assert_eq!(self.max_len, max_len.unwrap_or(0));
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
    fn out_of_order_pushes_are_sorted() {
        let mut buffers = PushBuffers::new(1000);
        for range in [50..60, 10..20, 30..40, 0..5, 90..100, 10..15] {
            push(&mut buffers, range);
        }
        assert_valid(&buffers);
        assert_eq!(
            buffers.ranges,
            vec![0..5, 10..20, 10..15, 30..40, 50..60, 90..100]
        );
        assert!(buffers.has_range(&(12..18)));
        assert!(buffers.has_range(&(30..40)));
        assert!(!buffers.has_range(&(18..22)));
        assert!(!buffers.has_range(&(60..61)));
        assert_eq!(buffers.get_bytes(52, 4).unwrap(), file_bytes(52..56));
        assert!(matches!(
            buffers.get_bytes(40, 20),
            Err(ParquetError::NeedMoreDataRange(r)) if r == (40..60)
        ));
    }

    #[test]
    fn overlapping_pushes_find_the_containing_buffer() {
        let mut buffers = PushBuffers::new(1000);
        // Small buffers inside a large one start closer to most offsets.
        for range in [0..100, 10..20, 50..60, 55..58] {
            push(&mut buffers, range);
        }
        assert_valid(&buffers);
        // 55..90 starts in 50..60 and 55..58, but only 0..100 contains it.
        assert_eq!(buffers.get_bytes(55, 35).unwrap(), file_bytes(55..90));
        assert!(buffers.has_range(&(0..100)));
        assert!(buffers.has_range(&(99..100)));
        assert!(!buffers.has_range(&(99..101)));

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

    /// Random pushes, clears and lookups, compared with a list that is
    /// scanned in full for each lookup.
    #[test]
    #[cfg(feature = "arrow")]
    fn matches_a_linear_scan() {
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
}
