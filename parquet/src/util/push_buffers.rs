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
use crate::file::reader::Length;
use bytes::{Bytes, BytesMut};
use std::fmt::Display;
use std::ops::Range;

/// Holds multiple buffers of data
///
/// This is the in-memory buffer for the ParquetDecoder and ParquetMetadataDecoders
///
/// Features:
/// 1. Non-contiguous ranges of bytes
/// 2. Stitching: reads that span multiple contiguous physical buffers are
///    resolved transparently. When a single buffer covers the request, the
///    result is zero-copy ([`Bytes::slice`]). When multiple buffers must be
///    stitched, the data is copied into a new allocation.
///
/// # No Coalescing
///
/// This buffer does not coalesce (merging adjacent ranges of bytes into a ingle
/// range). The IO layer is free to push arbitrarily-sized buffers; they will be
/// stitched on read if needed. Coalescing is left to the IO layer because it
/// would require an extra copy here, and because the optimal coalescing
/// strategy depends on the workload and storage medium (e.g. spinning disk,
/// NVMe, blob storage,) context that only the IO layer has.
///
/// # No Speculative Prefetching
///
/// This layer does not prefetch data ahead of what the decoder requests.
/// The IO layer is free to push buffers at offsets not yet requested by
/// the decoder — they will be held and stitched into future reads — but
/// the decision of *whether* to prefetch, and *how much*, is left to the
/// IO layer. Like coalescing, prefetching strategy depends on the storage
/// medium and access pattern.
#[derive(Debug, Clone)]
pub(crate) struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The ranges of data that are available for decoding (not adjusted for offset)
    ranges: Vec<Range<u64>>,
    /// The buffers of data that can be used to decode the Parquet file
    buffers: Vec<Bytes>,
    /// High-water mark set by [`Self::release_through`].  After a release,
    /// no push, has_range, or read may target offsets below this value.
    #[cfg(feature = "arrow")]
    watermark: u64,
    /// Whether `ranges`/`buffers` are sorted by range start.
    /// Set to `false` on every `push_range`, restored lazily before reads.
    sorted: bool,
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
    /// Create a new Buffers instance with the given file length
    pub fn new(file_len: u64) -> Self {
        Self {
            offset: 0,
            file_len,
            ranges: Vec::new(),
            buffers: Vec::new(),
            #[cfg(feature = "arrow")]
            watermark: 0,
            sorted: true,
        }
    }

    /// Restore the sort invariant on `ranges`/`buffers`.
    ///
    /// Because IO completions are expected to generally arrive in-order,
    /// `push_range` appends without sorting. We instead delay sorting until
    /// conumption to amortize its cost, if necessary.
    ///
    /// This method must be called before any read-side operation that relies on
    /// binary search (`has_range`, `get_bytes`, `release_through`,
    /// `Read::read`). Callers that hold `&mut PushBuffers` should call this
    /// once before lending `&PushBuffers` to read-side code.
    pub fn ensure_sorted(&mut self) {
        if self.sorted {
            return;
        }

        // Insertion sort: zero-allocation and linear on nearly-sorted input
        // (IO completions typically arrive roughly in order).
        for i in 1..self.ranges.len() {
            let mut j = i;
            while j > 0 && self.ranges[j - 1].start > self.ranges[j].start {
                self.ranges.swap(j - 1, j);
                self.buffers.swap(j - 1, j);
                j -= 1;
            }
        }
        self.sorted = true;
    }

    /// Push a new range and its associated buffer.
    ///
    /// Ranges may be pushed in any order. The internal sort invariant is
    /// restored lazily before the next read-side operation.
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) {
        self.push_ranges(vec![range], vec![buffer]);
    }

    /// Push all the ranges and buffers.
    ///
    /// Ranges may be pushed in any order. The internal sort invariant is
    /// restored lazily before the next read-side operation.
    pub fn push_ranges(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        assert_eq!(
            ranges.len(),
            buffers.len(),
            "Number of ranges must match number of buffers"
        );
        self.ranges.reserve(ranges.len());
        self.buffers.reserve(buffers.len());
        for (range, buffer) in ranges.into_iter().zip(buffers.into_iter()) {
            #[cfg(feature = "arrow")]
            debug_assert!(
                range.start >= self.watermark,
                "push_range({:?}) below watermark {}",
                range,
                self.watermark,
            );
            assert_eq!(
                (range.end - range.start) as usize,
                buffer.len(),
                "Range length must match buffer length"
            );
            if self.sorted {
                if let Some(last) = self.ranges.last() {
                    if last.start > range.start {
                        self.sorted = false;
                    }
                }
            }
            self.ranges.push(range);
            self.buffers.push(buffer);
        }
    }

    /// Returns true if the Buffers contains data for the given range.
    ///
    /// This supports stitching: the range may span multiple contiguous physical
    /// buffers (e.g. fixed-size streaming parts that don't align with column
    /// chunk boundaries).
    pub fn has_range(&self, range: &Range<u64>) -> bool {
        assert!(
            self.sorted,
            "has_range called on unsorted PushBuffers — call ensure_sorted() first"
        );
        #[cfg(feature = "arrow")]
        debug_assert!(
            range.start >= self.watermark,
            "has_range({:?}) below watermark {}",
            range,
            self.watermark,
        );
        // Binary search for the last buffer with start <= range.start.
        let idx = self.ranges.partition_point(|r| r.start <= range.start);
        if idx == 0 {
            return false;
        }
        let mut covered = self.ranges[idx - 1].end;
        // Walk forward through contiguous buffers until we cover range.end.
        for r in &self.ranges[idx..] {
            if covered >= range.end {
                break;
            }
            if r.start > covered {
                return false;
            }
            covered = covered.max(r.end);
        }
        covered >= range.end
    }

    /// return the file length of the Parquet file being read
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Return the total of all buffered bytes.
    #[cfg(feature = "arrow")]
    pub fn buffered_bytes(&self) -> u64 {
        self.buffers.iter().map(|b| b.len() as u64).sum()
    }

    /// Release all buffered ranges and their corresponding data.
    pub fn release_all(&mut self) {
        self.ranges.clear();
        self.buffers.clear();
    }

    /// Release all physical buffers that end at or before `offset`.
    ///
    /// A buffer straddling the offset is trimmed: the portion before `offset`
    /// is dropped and the suffix is retained via [`Bytes::slice`] (zero-copy).
    ///
    /// This is useful for streaming scenarios where fixed-size data parts are
    /// pushed and memory should be freed incrementally as the decoder
    /// progresses through the file.
    #[cfg(feature = "arrow")]
    pub fn release_through(&mut self, offset: u64) {
        self.ensure_sorted();
        self.watermark = self.watermark.max(offset);

        // Find the first buffer whose end extends past the offset.
        let first_kept = self.ranges.partition_point(|r| r.end <= offset);

        // Drop all buffers entirely before the offset.
        self.ranges.drain(..first_kept);
        self.buffers.drain(..first_kept);

        // If the first remaining buffer straddles the offset, trim its prefix.
        if let Some(range) = self.ranges.first_mut() {
            if range.start < offset {
                let trim = (offset - range.start) as usize;
                self.buffers[0] = self.buffers[0].slice(trim..);
                range.start = offset;
            }
        }
    }
}

impl Length for PushBuffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

/// Implementation of Read for Buffers with stitching across adjacent buffers.
impl std::io::Read for PushBuffers {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.ensure_sorted();
        #[cfg(feature = "arrow")]
        debug_assert!(
            self.offset >= self.watermark,
            "Read::read at offset {} below watermark {}",
            self.offset,
            self.watermark,
        );
        let needed = buf.len() as u64;
        let end = self.offset + needed;

        // Binary search for the buffer containing self.offset.
        let idx = self.ranges.partition_point(|r| r.start <= self.offset);
        if idx == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available in Buffers",
            ));
        }

        let first = idx - 1;
        let mut written = 0usize;
        let mut cursor = self.offset;
        for (r, data) in self.ranges[first..]
            .iter()
            .zip(self.buffers[first..].iter())
        {
            if cursor >= end || r.start > cursor {
                break;
            }
            let buf_start = (cursor - r.start) as usize;
            let buf_end = ((end.min(r.end)) - r.start) as usize;
            let chunk = &data[buf_start..buf_end];
            buf[written..written + chunk.len()].copy_from_slice(chunk);
            written += chunk.len();
            cursor = r.end.max(cursor);
        }

        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available in Buffers",
            ));
        }
        self.offset += written as u64;
        Ok(written)
    }
}

impl PushBuffers {
    /// Sort (if needed) then look up bytes.
    pub fn get_bytes(&mut self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        self.ensure_sorted();
        #[cfg(feature = "arrow")]
        debug_assert!(
            start >= self.watermark,
            "get_bytes({start}, {length}) below watermark {}",
            self.watermark,
        );
        let end = start + length as u64;

        // Binary search for the last buffer with start <= `start`.
        let idx = self.ranges.partition_point(|r| r.start <= start);
        if idx == 0 {
            return Err(ParquetError::NeedMoreDataRange(start..end));
        }

        let first = idx - 1;
        let range = &self.ranges[first];

        // Fast path: single buffer covers the entire request (zero-copy).
        if range.end >= end {
            let off = (start - range.start) as usize;
            return Ok(self.buffers[first].slice(off..off + length));
        }

        // Slow path: stitch across multiple contiguous buffers.
        let mut buf = BytesMut::with_capacity(length);
        let mut cursor = start;
        for (r, data) in self.ranges[first..]
            .iter()
            .zip(self.buffers[first..].iter())
        {
            if cursor >= end {
                break;
            }
            if r.start > cursor {
                return Err(ParquetError::NeedMoreDataRange(start..end));
            }
            let buf_start = (cursor - r.start) as usize;
            let buf_end = ((end.min(r.end)) - r.start) as usize;
            buf.extend_from_slice(&data[buf_start..buf_end]);
            cursor = r.end.max(cursor);
        }
        if cursor < end {
            return Err(ParquetError::NeedMoreDataRange(start..end));
        }
        Ok(buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// Helper: create PushBuffers with the given (start, data) pairs.
    /// Calls `ensure_sorted()` so the result is ready for read-side methods.
    fn make_buffers(parts: &[(u64, &[u8])]) -> PushBuffers {
        let file_len = parts
            .iter()
            .map(|(s, d)| s + d.len() as u64)
            .max()
            .unwrap_or(0);
        let mut pb = PushBuffers::new(file_len);
        for &(start, data) in parts {
            let end = start + data.len() as u64;
            pb.push_range(start..end, Bytes::copy_from_slice(data));
        }
        pb.ensure_sorted();
        pb
    }

    // ---------------------------------------------------------------
    // has_range
    // ---------------------------------------------------------------

    #[test]
    fn has_range_single_buffer() {
        let pb = make_buffers(&[(0, &[1, 2, 3, 4, 5])]);
        assert!(pb.has_range(&(0..5)));
        assert!(pb.has_range(&(1..4)));
        assert!(!pb.has_range(&(0..6)));
    }

    #[test]
    fn has_range_two_adjacent_buffers() {
        let pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
        assert!(pb.has_range(&(0..6)));
        assert!(pb.has_range(&(2..5)));
    }

    #[test]
    fn has_range_three_adjacent_buffers() {
        let pb = make_buffers(&[(0, &[1, 2]), (2, &[3, 4]), (4, &[5, 6])]);
        assert!(pb.has_range(&(0..6)));
        assert!(pb.has_range(&(1..5)));
    }

    #[test]
    fn has_range_gap() {
        let pb = make_buffers(&[(0, &[1, 2]), (5, &[6, 7])]);
        assert!(!pb.has_range(&(0..7)));
        assert!(!pb.has_range(&(1..6)));
        assert!(pb.has_range(&(0..2)));
        assert!(pb.has_range(&(5..7)));
    }

    #[test]
    fn has_range_before_any_buffer() {
        let pb = make_buffers(&[(10, &[1, 2, 3])]);
        assert!(!pb.has_range(&(0..5)));
    }

    #[test]
    fn has_range_after_all_buffers() {
        let pb = make_buffers(&[(0, &[1, 2, 3])]);
        assert!(!pb.has_range(&(5..10)));
    }

    #[test]
    fn has_range_overlapping_buffers() {
        // Buffers overlap: [0..5) and [3..8)
        let pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        assert!(pb.has_range(&(0..8)));
        assert!(pb.has_range(&(2..7)));
    }

    // ---------------------------------------------------------------
    // get_bytes
    // ---------------------------------------------------------------

    #[test]
    fn get_bytes_single_buffer() {
        let mut pb = make_buffers(&[(0, &[10, 20, 30, 40, 50])]);
        let b = pb.get_bytes(1, 3).unwrap();
        assert_eq!(&*b, &[20, 30, 40]);
    }

    #[test]
    fn get_bytes_stitching_two_buffers() {
        let mut pb = make_buffers(&[(0, &[10, 20, 30]), (3, &[40, 50, 60])]);
        let b = pb.get_bytes(1, 4).unwrap();
        assert_eq!(&*b, &[20, 30, 40, 50]);
    }

    #[test]
    fn get_bytes_stitching_three_buffers() {
        let mut pb = make_buffers(&[(0, &[1, 2]), (2, &[3, 4]), (4, &[5, 6])]);
        let b = pb.get_bytes(0, 6).unwrap();
        assert_eq!(&*b, &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn get_bytes_gap_returns_error() {
        let mut pb = make_buffers(&[(0, &[1, 2]), (5, &[6, 7])]);
        let err = pb.get_bytes(0, 7).unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreDataRange(_)));
    }

    #[test]
    fn get_bytes_before_any_buffer() {
        let mut pb = make_buffers(&[(10, &[1, 2, 3])]);
        let err = pb.get_bytes(0, 5).unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreDataRange(_)));
    }

    #[test]
    fn get_bytes_extends_past_last_buffer() {
        let mut pb = make_buffers(&[(0, &[1, 2, 3])]);
        let err = pb.get_bytes(0, 10).unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreDataRange(_)));
    }

    #[test]
    fn get_bytes_overlapping_buffers() {
        // Buffers overlap: [0..5) and [3..8). The overlap region (bytes 3..5)
        // must not be duplicated in the output.
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let b = pb.get_bytes(0, 8).unwrap();
        assert_eq!(&*b, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn get_bytes_overlapping_buffers_interior() {
        // Request a sub-range that spans the overlap boundary.
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let b = pb.get_bytes(2, 5).unwrap();
        assert_eq!(&*b, &[3, 4, 5, 6, 7]);
    }

    // ---------------------------------------------------------------
    // Read impl
    // ---------------------------------------------------------------

    #[test]
    fn read_stitching_across_buffers() {
        let mut pb = make_buffers(&[(0, &[10, 20, 30]), (3, &[40, 50, 60])]);
        let mut buf = [0u8; 6];
        let n = pb.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(buf, [10, 20, 30, 40, 50, 60]);
    }

    #[test]
    fn read_sequential_across_buffers() {
        let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
        let mut buf = [0u8; 4];
        let n = pb.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(buf, [1, 2, 3, 4]);
        let mut buf2 = [0u8; 2];
        let n = pb.read(&mut buf2).unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf2, [5, 6]);
    }

    #[test]
    fn read_overlapping_buffers() {
        // Buffers overlap: [0..5) and [3..8). Read should produce the
        // correct sequence without duplicating the overlap region.
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let mut buf = [0u8; 8];
        let n = pb.read(&mut buf).unwrap();
        assert_eq!(n, 8);
        assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn read_overlapping_buffers_sequential() {
        // Two sequential reads that together span the overlap boundary.
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let mut buf1 = [0u8; 4];
        let n = pb.read(&mut buf1).unwrap();
        assert_eq!(n, 4);
        assert_eq!(buf1, [1, 2, 3, 4]);
        let mut buf2 = [0u8; 4];
        let n = pb.read(&mut buf2).unwrap();
        assert_eq!(n, 4);
        assert_eq!(buf2, [5, 6, 7, 8]);
    }

    // ---------------------------------------------------------------
    // Out-of-order push (lazy sort)
    // ---------------------------------------------------------------

    #[test]
    fn out_of_order_push_has_range() {
        // Push buffers in reverse order — simulates IO completing out of order.
        let mut pb = PushBuffers::new(6);
        pb.push_range(3..6, Bytes::from_static(&[4, 5, 6]));
        pb.push_range(0..3, Bytes::from_static(&[1, 2, 3]));
        pb.ensure_sorted();
        assert!(pb.has_range(&(0..6)));
        assert!(pb.has_range(&(1..5)));
    }

    #[test]
    fn out_of_order_push_get_bytes() {
        let mut pb = PushBuffers::new(9);
        pb.push_range(6..9, Bytes::from_static(&[7, 8, 9]));
        pb.push_range(0..3, Bytes::from_static(&[1, 2, 3]));
        pb.push_range(3..6, Bytes::from_static(&[4, 5, 6]));
        pb.ensure_sorted();
        let b = pb.get_bytes(0, 9).unwrap();
        assert_eq!(&*b, &[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn out_of_order_push_read() {
        let mut pb = PushBuffers::new(6);
        pb.push_range(3..6, Bytes::from_static(&[4, 5, 6]));
        pb.push_range(0..3, Bytes::from_static(&[1, 2, 3]));
        let mut buf = [0u8; 6];
        let n = pb.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(buf, [1, 2, 3, 4, 5, 6]);
    }

    // ---------------------------------------------------------------
    // release_through
    // ---------------------------------------------------------------

    #[cfg(feature = "arrow")]
    mod release_through_tests {
        use super::*;

        #[test]
        fn release_through_at_boundary() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            pb.release_through(3);
            assert_eq!(pb.ranges.len(), 1);
            assert_eq!(pb.ranges[0], 3..6);
            assert_eq!(&*pb.buffers[0], &[4, 5, 6]);
        }

        #[test]
        fn release_through_splits_straddling_buffer() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5])]);
            pb.release_through(3);
            assert_eq!(pb.ranges.len(), 1);
            assert_eq!(pb.ranges[0], 3..5);
            assert_eq!(&*pb.buffers[0], &[4, 5]);
        }

        #[test]
        fn release_through_before_all_buffers() {
            let mut pb = make_buffers(&[(10, &[1, 2, 3])]);
            pb.release_through(5);
            // Nothing to clear — buffer starts at 10.
            assert_eq!(pb.ranges.len(), 1);
            assert_eq!(pb.ranges[0], 10..13);
        }

        #[test]
        fn release_through_past_all_buffers() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            pb.release_through(100);
            assert!(pb.ranges.is_empty());
            assert!(pb.buffers.is_empty());
        }

        #[test]
        fn release_through_multiple_buffers() {
            let mut pb = make_buffers(&[(0, &[1, 2]), (2, &[3, 4]), (4, &[5, 6]), (6, &[7, 8])]);
            pb.release_through(5);
            assert_eq!(pb.ranges.len(), 2);
            assert_eq!(pb.ranges[0], 5..6);
            assert_eq!(&*pb.buffers[0], &[6]);
            assert_eq!(pb.ranges[1], 6..8);
        }

        #[test]
        fn release_through_idempotent() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5])]);
            pb.release_through(3);
            pb.release_through(3);
            assert_eq!(pb.ranges.len(), 1);
            assert_eq!(pb.ranges[0], 3..5);
            assert_eq!(&*pb.buffers[0], &[4, 5]);
        }

        #[test]
        fn release_through_zero_is_noop() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3])]);
            pb.release_through(0);
            assert_eq!(pb.ranges.len(), 1);
            assert_eq!(pb.ranges[0], 0..3);
        }

        #[test]
        fn buffered_bytes_after_release_through() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            assert_eq!(pb.buffered_bytes(), 6);
            pb.release_through(3);
            assert_eq!(pb.buffered_bytes(), 3);
        }
    }
}
