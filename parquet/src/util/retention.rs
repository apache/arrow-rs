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

use bytes::Bytes;
use std::ops::Range;

use crate::file::metadata::ParquetMetaData;

/// A sorted, non-overlapping set of byte ranges that the decoder expects to
/// consume.
///
/// When attached to a `RowGroupReaderBuilder`, incoming buffers are filtered
/// against this set: only the portions that overlap a retained range are
/// stored. Everything else is silently discarded.
///
/// This prevents speculatively prefetched data for row groups the decoder will
/// never process from accumulating in memory.
#[derive(Debug, Clone)]
pub(crate) struct RetentionSet {
    /// Sorted, non-overlapping, merged ranges.
    ranges: Vec<Range<u64>>,
}

impl RetentionSet {
    /// Build a retention set from the column chunk byte ranges of the given
    /// row groups.
    ///
    /// All column chunks (regardless of projection) for each queued row group
    /// are included — this is a conservative superset of what the decoder will
    /// actually read.
    pub fn from_row_groups(metadata: &ParquetMetaData, row_groups: &[usize]) -> Self {
        let total_cols: usize = row_groups
            .iter()
            .map(|&rg| metadata.row_group(rg).columns().len())
            .sum();
        let mut ranges: Vec<Range<u64>> = Vec::with_capacity(total_cols);
        for &rg_idx in row_groups {
            let rg = metadata.row_group(rg_idx);
            for col in rg.columns() {
                let (start, len) = col.byte_range();
                ranges.push(start..start + len);
            }
        }
        ranges.sort_unstable_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(last) = merged.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }
        Self { ranges: merged }
    }

    /// Extend the retention set with additional ranges.
    ///
    /// This is called when the decoder explicitly requests ranges via
    /// `NeedsData`. Any range the decoder needs must be admitted on push,
    /// even if it wasn't in the original column-chunk-derived set.
    ///
    /// Empty ranges (`start >= end`) are skipped — they cannot be
    /// represented in the interval set. Zero-length pushes are handled
    /// separately by bypassing the filter in `push_data`.
    pub fn extend(&mut self, ranges: &[Range<u64>]) {
        let before = self.ranges.len();
        for range in ranges {
            if range.start >= range.end {
                continue;
            }
            let insert_at = self.ranges.partition_point(|r| r.start < range.start);
            self.ranges.insert(insert_at, range.clone());
        }
        if self.ranges.len() == before {
            return; // nothing added
        }
        // Re-merge in case new ranges overlap or abut existing ones.
        let mut merged: Vec<Range<u64>> = Vec::with_capacity(self.ranges.len());
        for range in self.ranges.drain(..) {
            if let Some(last) = merged.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }
        self.ranges = merged;
    }

    /// Filter incoming ranges and buffers, keeping only the portions that
    /// overlap the retention set.
    ///
    /// Each retained portion is a zero-copy [`Bytes::slice`] of the original
    /// buffer. Portions that fall entirely outside the retention set are
    /// dropped.
    pub fn filter(
        &self,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> (Vec<Range<u64>>, Vec<Bytes>) {
        let mut out_ranges = Vec::new();
        let mut out_buffers = Vec::new();

        for (range, buffer) in ranges.into_iter().zip(buffers) {
            // Zero-length ranges always pass through.
            if range.start >= range.end {
                out_ranges.push(range);
                out_buffers.push(buffer);
                continue;
            }

            // Find the first retention range that could overlap: the first
            // whose end is past range.start.
            let start_idx = self.ranges.partition_point(|r| r.end <= range.start);

            for ret in &self.ranges[start_idx..] {
                if ret.start >= range.end {
                    break;
                }
                let overlap_start = range.start.max(ret.start);
                let overlap_end = range.end.min(ret.end);
                let buf_offset = (overlap_start - range.start) as usize;
                let buf_len = (overlap_end - overlap_start) as usize;
                out_ranges.push(overlap_start..overlap_end);
                out_buffers.push(buffer.slice(buf_offset..buf_offset + buf_len));
            }
        }

        (out_ranges, out_buffers)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::single_range_in_vec_init)]
    use super::*;

    fn make_retention(ranges: &[Range<u64>]) -> RetentionSet {
        let mut sorted: Vec<Range<u64>> = ranges.to_vec();
        sorted.sort_unstable_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = Vec::new();
        for range in sorted {
            if let Some(last) = merged.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }
        RetentionSet { ranges: merged }
    }

    #[test]
    fn exact_match() {
        let ret = make_retention(&[10..20]);
        let buf = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let (ranges, buffers) = ret.filter(vec![10..20], vec![buf]);
        assert_eq!(ranges, vec![10..20]);
        assert_eq!(buffers.len(), 1);
        assert_eq!(&*buffers[0], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn no_overlap() {
        let ret = make_retention(&[10..20]);
        let buf = Bytes::from_static(&[1, 2, 3]);
        let (ranges, buffers) = ret.filter(vec![0..3], vec![buf]);
        assert!(ranges.is_empty());
        assert!(buffers.is_empty());
    }

    #[test]
    fn partial_overlap_left() {
        let ret = make_retention(&[10..20]);
        let buf = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        // Buffer covers 5..15, retention is 10..20 → keep 10..15
        let (ranges, buffers) = ret.filter(vec![5..15], vec![buf]);
        assert_eq!(ranges, vec![10..15]);
        assert_eq!(&*buffers[0], &[6, 7, 8, 9, 10]);
    }

    #[test]
    fn partial_overlap_right() {
        let ret = make_retention(&[10..20]);
        let buf = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        // Buffer covers 15..25, retention is 10..20 → keep 15..20
        let (ranges, buffers) = ret.filter(vec![15..25], vec![buf]);
        assert_eq!(ranges, vec![15..20]);
        assert_eq!(&*buffers[0], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn buffer_spans_gap_between_retention_ranges() {
        // Retention: [10..20) and [30..40). Buffer covers 5..45.
        let ret = make_retention(&[10..20, 30..40]);
        let data: Vec<u8> = (0..40).collect();
        let buf = Bytes::from(data);
        let (ranges, buffers) = ret.filter(vec![5..45], vec![buf]);
        assert_eq!(ranges, vec![10..20, 30..40]);
        assert_eq!(buffers.len(), 2);
        // First slice: bytes at offset 5..15 in the buffer (values 5..15)
        assert_eq!(&*buffers[0], &[5, 6, 7, 8, 9, 10, 11, 12, 13, 14]);
        // Second slice: bytes at offset 25..35 in the buffer (values 25..35)
        assert_eq!(&*buffers[1], &[25, 26, 27, 28, 29, 30, 31, 32, 33, 34]);
    }

    #[test]
    fn superset_buffer_trimmed() {
        let ret = make_retention(&[10..20]);
        let data: Vec<u8> = (0..50).collect();
        let buf = Bytes::from(data);
        let (ranges, buffers) = ret.filter(vec![0..50], vec![buf]);
        assert_eq!(ranges, vec![10..20]);
        assert_eq!(&*buffers[0], &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
    }

    #[test]
    fn empty_retention_discards_everything() {
        let ret = RetentionSet { ranges: Vec::new() };
        let buf = Bytes::from_static(&[1, 2, 3]);
        let (ranges, buffers) = ret.filter(vec![0..3], vec![buf]);
        assert!(ranges.is_empty());
        assert!(buffers.is_empty());
    }

    #[test]
    fn multiple_input_buffers() {
        let ret = make_retention(&[10..20, 30..40]);
        let buf1 = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let buf2 = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let buf3 = Bytes::from_static(&[1, 2, 3, 4, 5]);
        let (ranges, buffers) = ret.filter(vec![0..5, 10..15, 35..40], vec![buf1, buf2, buf3]);
        // First buffer: no overlap. Second: exact. Third: exact.
        assert_eq!(ranges, vec![10..15, 35..40]);
        assert_eq!(buffers.len(), 2);
    }

    #[test]
    fn zero_copy_slicing() {
        let ret = make_retention(&[10..20]);
        let data: Vec<u8> = (0..30).collect();
        let buf = Bytes::from(data);
        let original_ptr = buf.as_ptr();
        let (_, buffers) = ret.filter(vec![0..30], vec![buf]);
        // The output slice should point into the same allocation,
        // offset by 10 bytes.
        assert_eq!(buffers[0].as_ptr(), unsafe { original_ptr.add(10) },);
    }

    #[test]
    fn adjacent_retention_ranges_are_merged() {
        // Two abutting ranges should merge into one.
        let ret = make_retention(&[10..20, 20..30]);
        assert_eq!(ret.ranges, vec![10..30]);
        let data: Vec<u8> = (0..40).collect();
        let buf = Bytes::from(data);
        let (ranges, buffers) = ret.filter(vec![0..40], vec![buf]);
        // Should produce a single slice, not two.
        assert_eq!(ranges, vec![10..30]);
        assert_eq!(buffers.len(), 1);
    }
}
