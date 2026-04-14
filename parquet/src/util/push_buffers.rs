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
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;

/// Value stored in the [`PushBuffers`] B-tree for each IO buffer.
/// The key is the buffer's start offset.
#[derive(Debug, Clone)]
struct BufferValue {
    /// End offset (exclusive) of the byte range this buffer covers.
    end: u64,
    /// The raw data.
    data: Bytes,
    /// Number of bytes within this buffer that have not yet been released.
    /// Initialized to `data.len()` and decremented by [`PushBuffers::release_range`].
    /// When this reaches zero the entry is dropped.
    live_bytes: u64,
}

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
/// This buffer does not coalesce (merging adjacent ranges of bytes into a single
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
///
/// # Release model
///
/// Callers release byte ranges via [`release_range`](Self::release_range).
/// Each buffer tracks a `live_bytes` counter that is decremented by the
/// overlap between the released range and the buffer's range. When the
/// counter reaches zero the buffer is dropped.
///
/// **Caller invariant:** each byte offset should be released at most once.
/// Violating this causes double-counting, which may drop a buffer
/// prematurely. This is safe but wasteful: the decoder's `NeedsData` retry loop
/// will re-request the data.
#[derive(Debug, Clone)]
pub(crate) struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// IO buffers keyed by their start offset. Each value stores the end
    /// offset, the data, and a live-byte counter for release tracking.
    entries: BTreeMap<u64, BufferValue>,
}

impl Display for PushBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Buffers (offset: {}, file_len: {})",
            self.offset, self.file_len
        )?;
        writeln!(f, "Available Ranges (w/ offset):")?;
        for (&start, entry) in &self.entries {
            writeln!(
                f,
                "  {}..{} ({}..{}): {} bytes ({} live)",
                start,
                entry.end,
                start + self.offset,
                entry.end + self.offset,
                entry.end - start,
                entry.live_bytes,
            )?;
        }

        Ok(())
    }
}

impl PushBuffers {
    /// Create a new Buffers instance with the given file length.
    pub fn new(file_len: u64) -> Self {
        Self {
            offset: 0,
            file_len,
            entries: BTreeMap::new(),
        }
    }

    /// Push a new range and its associated buffer.
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) {
        self.push_ranges(vec![range], vec![buffer]);
    }

    /// Push all the ranges and buffers.
    pub fn push_ranges(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        assert_eq!(
            ranges.len(),
            buffers.len(),
            "Number of ranges must match number of buffers"
        );
        for (range, buffer) in ranges.into_iter().zip(buffers.into_iter()) {
            assert_eq!(
                (range.end - range.start) as usize,
                buffer.len(),
                "Range length must match buffer length"
            );
            let live_bytes = buffer.len() as u64;
            self.entries.insert(
                range.start,
                BufferValue {
                    end: range.end,
                    data: buffer,
                    live_bytes,
                },
            );
        }
    }

    /// Returns true if the Buffers contains data for the given range.
    ///
    /// This supports stitching: the range may span multiple contiguous physical
    /// buffers (e.g. fixed-size streaming parts that don't align with column
    /// chunk boundaries).
    pub fn has_range(&self, range: &Range<u64>) -> bool {
        // Find the last buffer with start <= range.start.
        let (_, first) = match self.entries.range(..=range.start).next_back() {
            Some(entry) => entry,
            None => return false,
        };
        let mut covered = first.end;
        // Walk forward through contiguous buffers until we cover range.end.
        for (&entry_start, entry) in self.entries.range((range.start + 1)..) {
            if covered >= range.end {
                break;
            }
            if entry_start > covered {
                return false;
            }
            covered = covered.max(entry.end);
        }
        covered >= range.end
    }

    /// return the file length of the Parquet file being read
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Return the total of logically live (unreleased) buffered bytes.
    #[cfg(feature = "arrow")]
    pub fn buffered_bytes(&self) -> u64 {
        self.entries.values().map(|e| e.live_bytes).sum()
    }

    /// Release all buffered ranges and their corresponding data.
    #[cfg(feature = "arrow")]
    pub fn release_all(&mut self) {
        self.entries.clear();
    }

    /// Release the given byte range.
    ///
    /// For each buffer that overlaps `range`, the overlap is subtracted from
    /// the buffer's live-byte counter. When the counter reaches zero the
    /// buffer is dropped.
    ///
    /// Caller invariant: each byte offset should be released at most once.
    /// Double-releasing the same offset will over-decrement the counter, which
    /// may drop a buffer prematurely. This is safe (the decoder's `NeedsData`
    /// retry loop will re-request the data) but wasteful.
    #[cfg(feature = "arrow")]
    pub fn release_range(&mut self, range: Range<u64>) {
        if range.start >= range.end {
            return;
        }

        // Find the first entry that could overlap: the last entry with
        // start <= range.start (its data may extend into the range).
        let scan_start = self
            .entries
            .range(..=range.start)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(range.start);

        // Walk only the overlapping entries, collecting dead keys.
        let mut dead_keys = Vec::new();
        for (&start, value) in self.entries.range_mut(scan_start..range.end) {
            if value.end <= range.start {
                continue;
            }
            let overlap_start = start.max(range.start);
            let overlap_end = value.end.min(range.end);
            let overlap = overlap_end - overlap_start;
            value.live_bytes = value
                .live_bytes
                .checked_sub(overlap)
                .expect("release_range: overlap exceeds live_bytes — likely double-release");
            if value.live_bytes == 0 {
                dead_keys.push(start);
            }
        }
        for key in dead_keys {
            self.entries.remove(&key);
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
        let needed = buf.len() as u64;
        let end = self.offset + needed;

        // Find the last buffer with start <= self.offset.
        let (&first_start, _) =
            self.entries
                .range(..=self.offset)
                .next_back()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "No data available in Buffers",
                    )
                })?;

        let mut written = 0usize;
        let mut cursor = self.offset;
        for (&entry_start, entry) in self.entries.range(first_start..) {
            if cursor >= end || entry_start > cursor {
                break;
            }
            let buf_start = (cursor - entry_start) as usize;
            let buf_end = ((end.min(entry.end)) - entry_start) as usize;
            let chunk = &entry.data[buf_start..buf_end];
            buf[written..written + chunk.len()].copy_from_slice(chunk);
            written += chunk.len();
            cursor = entry.end.max(cursor);
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
    /// Look up bytes, returning a zero-copy slice when a single buffer
    /// covers the request, or stitching across contiguous buffers otherwise.
    pub fn get_bytes(&mut self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let end = start + length as u64;

        // Find the last buffer with start_offset <= `start`.
        let (&first_start, first) = self
            .entries
            .range(..=start)
            .next_back()
            .ok_or(ParquetError::NeedMoreDataRange(start..end))?;

        // Fast path: single buffer covers the entire request (zero-copy).
        if first.end >= end {
            let off = (start - first_start) as usize;
            return Ok(first.data.slice(off..off + length));
        }

        // Slow path: stitch across multiple contiguous buffers.
        let mut buf = BytesMut::with_capacity(length);
        let mut cursor = start;
        for (&entry_start, entry) in self.entries.range(first_start..) {
            if cursor >= end {
                break;
            }
            if entry_start > cursor {
                return Err(ParquetError::NeedMoreDataRange(start..end));
            }
            let buf_start = (cursor - entry_start) as usize;
            let buf_end = ((end.min(entry.end)) - entry_start) as usize;
            buf.extend_from_slice(&entry.data[buf_start..buf_end]);
            cursor = entry.end.max(cursor);
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
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let b = pb.get_bytes(0, 8).unwrap();
        assert_eq!(&*b, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn get_bytes_overlapping_buffers_interior() {
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
        let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (3, &[4, 5, 6, 7, 8])]);
        let mut buf = [0u8; 8];
        let n = pb.read(&mut buf).unwrap();
        assert_eq!(n, 8);
        assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn read_overlapping_buffers_sequential() {
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
    // Out-of-order push (BTreeMap keeps order automatically)
    // ---------------------------------------------------------------

    #[test]
    fn out_of_order_push_has_range() {
        let mut pb = PushBuffers::new(6);
        pb.push_range(3..6, Bytes::from_static(&[4, 5, 6]));
        pb.push_range(0..3, Bytes::from_static(&[1, 2, 3]));
        assert!(pb.has_range(&(0..6)));
        assert!(pb.has_range(&(1..5)));
    }

    #[test]
    fn out_of_order_push_get_bytes() {
        let mut pb = PushBuffers::new(9);
        pb.push_range(6..9, Bytes::from_static(&[7, 8, 9]));
        pb.push_range(0..3, Bytes::from_static(&[1, 2, 3]));
        pb.push_range(3..6, Bytes::from_static(&[4, 5, 6]));
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
    // release_range
    // ---------------------------------------------------------------

    #[cfg(feature = "arrow")]
    mod release_tests {
        use super::*;

        #[test]
        fn release_range_exact_buffer() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            pb.release_range(0..3);
            assert_eq!(pb.entries.len(), 1);
            assert!(pb.entries.contains_key(&3));
            assert_eq!(pb.buffered_bytes(), 3);
        }

        #[test]
        fn release_range_partial_overlap() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5]), (5, &[6, 7, 8, 9, 10])]);
            assert_eq!(pb.buffered_bytes(), 10);
            // Release middle section that spans both buffers.
            pb.release_range(3..7);
            // Both buffers still alive (partial release).
            assert_eq!(pb.entries.len(), 2);
            // First buffer: 5 - 2 = 3 live bytes
            assert_eq!(pb.entries[&0].live_bytes, 3);
            // Second buffer: 5 - 2 = 3 live bytes
            assert_eq!(pb.entries[&5].live_bytes, 3);
            assert_eq!(pb.buffered_bytes(), 6);
            // Data is still accessible (live_bytes is bookkeeping only).
            assert!(pb.has_range(&(0..10)));
        }

        #[test]
        fn release_range_drops_middle_buffer() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6]), (6, &[7, 8, 9])]);
            pb.release_range(3..6);
            assert_eq!(pb.entries.len(), 2);
            assert!(pb.entries.contains_key(&0));
            assert!(pb.entries.contains_key(&6));
        }

        #[test]
        fn release_range_no_overlap() {
            let mut pb = make_buffers(&[(10, &[1, 2, 3])]);
            pb.release_range(0..5);
            assert_eq!(pb.entries.len(), 1);
            assert_eq!(pb.buffered_bytes(), 3);
        }

        #[test]
        fn release_range_superset() {
            let mut pb = make_buffers(&[(2, &[1, 2, 3, 4, 5, 6])]);
            // Release range is larger than the buffer.
            pb.release_range(0..10);
            assert!(pb.entries.is_empty());
            assert_eq!(pb.buffered_bytes(), 0);
        }

        #[test]
        fn release_range_out_of_order() {
            // Simulate non-sequential row group access: release "later" range first.
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            pb.release_range(3..6);
            assert_eq!(pb.entries.len(), 1);
            assert!(pb.entries.contains_key(&0));
            pb.release_range(0..3);
            assert!(pb.entries.is_empty());
        }

        #[test]
        fn release_range_empty() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3])]);
            pb.release_range(5..5);
            assert_eq!(pb.entries.len(), 1);
            assert_eq!(pb.buffered_bytes(), 3);
        }

        #[test]
        fn release_range_incremental_on_single_buffer() {
            // One big buffer, released in chunks (like multiple row groups).
            let mut pb = make_buffers(&[(0, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]);
            assert_eq!(pb.buffered_bytes(), 10);

            pb.release_range(0..3);
            assert_eq!(pb.entries.len(), 1);
            assert_eq!(pb.buffered_bytes(), 7);

            pb.release_range(3..7);
            assert_eq!(pb.entries.len(), 1);
            assert_eq!(pb.buffered_bytes(), 3);

            pb.release_range(7..10);
            assert!(pb.entries.is_empty());
            assert_eq!(pb.buffered_bytes(), 0);
        }

        #[test]
        fn buffered_bytes_tracks_live() {
            let mut pb = make_buffers(&[(0, &[1, 2, 3]), (3, &[4, 5, 6])]);
            assert_eq!(pb.buffered_bytes(), 6);
            pb.release_range(0..6);
            assert_eq!(pb.buffered_bytes(), 0);
        }
    }
}
