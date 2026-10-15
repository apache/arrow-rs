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

//! Execution time iteration over a [`RowSelection`].
//!
//! A [`ReadPlan`](crate::arrow::arrow_reader::ReadPlan) resolves a
//! [`RowSelectionPolicy`] into a [`RowSelectionStrategy`] and builds the
//! matching [`RowSelectionCursor`], which keeps the per-reader position while
//! the selection itself stays immutable.

use super::{RowSelection, RowSelector};
use crate::errors::ParquetError;
use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use arrow_buffer::bit_chunk_iterator::BitChunks;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// Policy for picking a strategy to materialize [`RowSelection`] during execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowSelectionPolicy {
    /// Use a queue of [`RowSelector`] values
    Selectors,
    /// Use a boolean mask to materialize the selection
    Mask,
    /// Choose between [`Self::Mask`] and [`Self::Selectors`] based on selector density
    Auto {
        /// Average selector length below which masks are preferred
        threshold: usize,
    },
}

impl Default for RowSelectionPolicy {
    fn default() -> Self {
        Self::Auto { threshold: 32 }
    }
}

impl RowSelectionPolicy {
    /// Resolve this policy for a selection without changing its backing.
    pub(crate) fn resolve(self, selection: Option<&RowSelection>) -> RowSelectionStrategy {
        match self {
            Self::Selectors => RowSelectionStrategy::Selectors,
            Self::Mask => RowSelectionStrategy::Mask,
            Self::Auto { threshold } => selection
                .map(|selection| selection.auto_selection_strategy(threshold))
                .unwrap_or(RowSelectionStrategy::Selectors),
        }
    }

    /// Materialize a selection according to this policy, preserving its full
    /// length, including trailing skips needed for selection composition.
    pub(crate) fn apply(self, selection: RowSelection) -> RowSelection {
        match (
            self.resolve(Some(&selection)),
            selection.as_mask().is_some(),
        ) {
            (RowSelectionStrategy::Mask, true) | (RowSelectionStrategy::Selectors, false) => {
                selection
            }
            (RowSelectionStrategy::Mask, false) => selection.force_mask(),
            (RowSelectionStrategy::Selectors, true) => {
                RowSelection::from(Vec::<RowSelector>::from(selection))
            }
        }
    }
}

/// Fully resolved strategy for materializing [`RowSelection`] during execution.
///
/// This is determined by [`RowSelectionPolicy`], including selector density for
/// [`RowSelectionPolicy::Auto`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowSelectionStrategy {
    /// Use a queue of [`RowSelector`] values
    Selectors,
    /// Use a boolean mask to materialize the selection
    Mask,
}

impl RowSelectionStrategy {
    /// Build a cursor using this strategy. The selection must be trimmed of
    /// trailing skips before constructing a mask cursor.
    pub(crate) fn build_cursor(
        self,
        selection: RowSelection,
        loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
    ) -> RowSelectionCursor {
        match self {
            Self::Mask => RowSelectionCursor::new_mask_from_buffer(
                selection.into_boolean_buffer(),
                loaded_row_ranges,
            ),
            Self::Selectors => RowSelectionCursor::new_selectors(selection.into()),
        }
    }
}

/// Cursor for iterating a [`RowSelection`] during execution within a
/// [`ReadPlan`](crate::arrow::arrow_reader::ReadPlan).
///
/// This keeps per-reader state such as the current position and delegates the
/// actual storage strategy to the internal `RowSelectionInner`.
#[derive(Debug)]
pub enum RowSelectionCursor {
    /// Reading all rows
    All,
    /// Use a bitmask to back the selection (dense selections)
    Mask(MaskCursor),
    /// Use a queue of selectors to back the selection (sparse selections)
    Selectors(SelectorsCursor),
}

impl RowSelectionCursor {
    /// Create a [`MaskCursor`] cursor backed by an existing bitmask.
    pub(crate) fn new_mask_from_buffer(
        mask: BooleanBuffer,
        loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
    ) -> Self {
        debug_assert!(
            mask.is_empty() || mask.value(mask.len() - 1),
            "Mask selections must not end with a skip"
        );
        Self::Mask(MaskCursor {
            mask,
            position: 0,
            loaded_row_ranges,
        })
    }

    /// Create a [`RowSelectionCursor::Selectors`] from the provided selectors
    pub(crate) fn new_selectors(selectors: Vec<RowSelector>) -> Self {
        Self::Selectors(SelectorsCursor {
            selectors: selectors.into(),
            position: 0,
        })
    }

    /// Create a cursor that selects all rows
    pub(crate) fn new_all() -> Self {
        Self::All
    }
}

/// Cursor for iterating a selector-backed [`RowSelection`]
///
/// This is best for sparse selections where large contiguous
/// blocks of rows are selected or skipped.
#[derive(Debug)]
pub struct SelectorsCursor {
    selectors: VecDeque<RowSelector>,
    /// Current absolute offset into the selection
    position: usize,
}

impl SelectorsCursor {
    /// Returns `true` when no further rows remain
    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty()
    }

    /// Return the next [`RowSelector`]
    pub(crate) fn next_selector(&mut self) -> RowSelector {
        let selector = self.selectors.pop_front().unwrap();
        self.position += selector.row_count;
        selector
    }

    /// Return a selector to the front, rewinding the position
    pub(crate) fn return_selector(&mut self, selector: RowSelector) {
        self.position = self.position.saturating_sub(selector.row_count);
        self.selectors.push_front(selector);
    }
}

/// Cursor for iterating a mask-backed [`RowSelection`]
///
/// This is best for dense selections where there are many small skips
/// or selections. For example, selecting every other row.
///
/// When page pruning produces sparse column data, `loaded_row_ranges` limits
/// each decoded chunk to rows whose pages are loaded for every projected leaf.
/// For example, two projected columns can have different page boundaries:
///
/// ```text
/// Row ranges:       [0, 4) [4, 6) [6, 8) [8, 10) [10, 12)
/// Selection mask:   1000   00     00     00      01
/// Column A pages:   loaded | missing [4, 8) | loaded [8, 12)
/// Column B pages:   loaded [0, 6) | missing [6, 10) | loaded
/// LoadedRowRanges:  [0, 4)                         [10, 12)
/// ```
///
/// The first chunk decodes `[0, 1)` with mask `1`. The next chunk skips to row
/// 11 and decodes `[11, 12)` with mask `1`. When loaded ranges are present,
/// every returned chunk ends at a selected row and never includes trailing
/// unselected rows. [`ParquetRecordBatchReader`] still accumulates both chunks
/// and applies the combined mask `11` once.
///
/// [`ParquetRecordBatchReader`]: crate::arrow::arrow_reader::ParquetRecordBatchReader
#[derive(Debug)]
pub struct MaskCursor {
    mask: BooleanBuffer,
    /// Current absolute offset into the selection
    position: usize,
    /// Row ranges whose backing pages are loaded for every projected column.
    loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
}

impl MaskCursor {
    /// Returns `true` when no further rows remain
    pub fn is_empty(&self) -> bool {
        self.position >= self.mask.len()
    }

    /// Advance through the mask representation, producing the next chunk summary
    pub fn next_mask_chunk(&mut self, batch_size: usize) -> Option<MaskChunk> {
        if self.is_empty() {
            return None;
        }

        Some(self.next_mask_chunk_non_empty(batch_size))
    }

    /// Produces the next chunk for a non-empty, trailing-skip-free mask.
    fn next_mask_chunk_non_empty(&mut self, batch_size: usize) -> MaskChunk {
        debug_assert!(!self.is_empty());
        let mask_start = self.next_selected_row().unwrap_or(self.mask.len());
        debug_assert!(
            mask_start < self.mask.len(),
            "ReadPlan must remove trailing skips from Mask selections"
        );
        self.scan_chunk(mask_start, self.mask.len(), batch_size)
    }

    /// Find the first selected row without inspecting every skipped bit.
    fn next_selected_row(&self) -> Option<usize> {
        let chunks = BitChunks::new(
            self.mask.values(),
            self.mask.offset() + self.position,
            self.mask.len() - self.position,
        );
        chunks.iter_padded().enumerate().find_map(|(idx, word)| {
            (word != 0).then(|| self.position + idx * 64 + word.trailing_zeros() as usize)
        })
    }

    /// Count whole words until the batch is full or the loaded range ends.
    /// `BitChunks` normalizes words to little-endian and masks the remainder,
    /// so padding and bits in an unloaded page cannot contribute to the count.
    // Keep the scanning loop out of the reader's selection dispatch.
    #[inline(never)]
    fn scan_chunk(&mut self, mask_start: usize, end: usize, batch_size: usize) -> MaskChunk {
        let chunks = BitChunks::new(
            self.mask.values(),
            self.mask.offset() + mask_start,
            end - mask_start,
        );
        let mut selected_rows = 0;
        let mut chunk_end = mask_start;

        if batch_size > 0 {
            for (idx, mut word) in chunks.iter_padded().enumerate() {
                let count = word.count_ones() as usize;
                if count == 0 {
                    continue;
                }

                let remaining = batch_size - selected_rows;
                let last_bit = if count <= remaining {
                    selected_rows += count;
                    63 - word.leading_zeros() as usize
                } else {
                    // Only the final word needs individual set-bit selection.
                    for _ in 1..remaining {
                        word &= word - 1;
                    }
                    selected_rows = batch_size;
                    word.trailing_zeros() as usize
                };
                chunk_end = mask_start + idx * 64 + last_bit + 1;
                if selected_rows == batch_size {
                    break;
                }
            }
        }

        let initial_skip = mask_start - self.position;
        // Stop immediately after the last selected row. Unset bits at the
        // end of a loaded range belong to the next chunk's initial skip.
        self.position = chunk_end;
        MaskChunk {
            initial_skip,
            chunk_rows: chunk_end - mask_start,
            selected_rows,
            mask_start,
        }
    }

    /// Returns the next non-empty mask chunk without crossing an unloaded row range.
    /// When loaded ranges are present, every returned chunk ends immediately after a
    /// selected row and therefore never contains trailing unselected rows. Those rows
    /// remain for the next call's initial skip.
    ///
    /// The [`ReadPlan`](crate::arrow::arrow_reader::ReadPlan) removes trailing
    /// skips before constructing this cursor. Callers therefore only invoke
    /// this method for a non-empty mask that has another selected row.
    pub(crate) fn next_chunk(&mut self, batch_size: usize) -> Result<MaskChunk, ParquetError> {
        debug_assert!(batch_size > 0);
        debug_assert!(!self.is_empty());

        if self.loaded_row_ranges.is_none() {
            return Ok(self.next_mask_chunk_non_empty(batch_size));
        }

        let mask_start = self.next_selected_row().ok_or_else(|| {
            ParquetError::General(
                "Internal Error: Mask cursor reached the end without finding a selected row; \
                 ReadPlan must remove trailing skips"
                    .to_string(),
            )
        })?;

        let loaded_range_end = self
            .loaded_row_ranges
            .as_ref()
            .and_then(|ranges| ranges.end_containing(mask_start))
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Internal Error: selected row {mask_start} has no loaded page range"
                ))
            })?;

        Ok(self.scan_chunk(
            mask_start,
            loaded_range_end.min(self.mask.len()),
            batch_size,
        ))
    }

    /// Materialise the boolean values for a mask-backed chunk
    pub fn mask_values_for(&self, chunk: &MaskChunk) -> Result<BooleanArray, ParquetError> {
        if chunk.mask_start.saturating_add(chunk.chunk_rows) > self.mask.len() {
            return Err(ParquetError::General(
                "Internal Error: MaskChunk exceeds mask length".to_string(),
            ));
        }
        Ok(BooleanArray::from(
            self.mask.slice(chunk.mask_start, chunk.chunk_rows),
        ))
    }
}

/// Result of computing the next chunk to read when using a [`MaskCursor`]
#[derive(Debug)]
pub struct MaskChunk {
    /// Number of leading rows to skip before reaching selected rows
    pub initial_skip: usize,
    /// Total rows covered by this chunk (selected + skipped)
    pub chunk_rows: usize,
    /// Rows actually selected within the chunk
    pub selected_rows: usize,
    /// Starting offset within the mask where the chunk begins
    pub mask_start: usize,
}

/// Row ranges whose backing pages are loaded for every projected column.
#[derive(Clone, Debug)]
pub(crate) struct LoadedRowRanges(Vec<Range<usize>>);

impl LoadedRowRanges {
    pub(crate) fn from_selection(selection: RowSelection) -> Self {
        let selectors: Vec<RowSelector> = selection.into();
        let mut position = 0;
        let ranges = selectors
            .into_iter()
            .filter_map(|selector| {
                let start = position;
                position += selector.row_count;
                (!selector.skip).then_some(start..position)
            })
            .collect();
        Self(ranges)
    }

    fn end_containing(&self, row: usize) -> Option<usize> {
        let idx = self.0.partition_point(|range| range.end <= row);
        self.0
            .get(idx)
            .filter(|range| range.start <= row)
            .map(|range| range.end)
    }

    #[cfg(test)]
    pub(crate) fn ranges(&self) -> &[Range<usize>] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compare chunks with the selected row positions, independently of how
    /// the cursor scans the bitmap. A chunk may consume at most `batch_size`
    /// selected rows and may not leave the loaded range containing its first.
    fn assert_mask_chunks(mask: BooleanBuffer, loaded: Option<LoadedRowRanges>, batch_size: usize) {
        let selected: Vec<_> = (0..mask.len()).filter(|&row| mask.value(row)).collect();
        let loaded = loaded.map(Arc::new);
        let selection = RowSelection::from_boolean_buffer(mask).trim();
        let RowSelectionCursor::Mask(mut cursor) =
            RowSelectionStrategy::Mask.build_cursor(selection, loaded.clone())
        else {
            unreachable!()
        };
        let mut next = 0;
        let mut position = 0;
        while next < selected.len() {
            let first = selected[next];
            let end = loaded
                .as_ref()
                .map_or(usize::MAX, |ranges| ranges.end_containing(first).unwrap());
            let count = selected[next..]
                .iter()
                .take(batch_size)
                .take_while(|&&row| row < end)
                .count();
            let last = selected[next + count - 1];
            let chunk = cursor.next_chunk(batch_size).unwrap();
            assert_eq!(chunk.initial_skip, first - position);
            assert_eq!(chunk.mask_start, first);
            assert_eq!(chunk.chunk_rows, last + 1 - first);
            assert_eq!(chunk.selected_rows, count);
            assert_eq!(cursor.position, last + 1);
            let values = cursor.mask_values_for(&chunk).unwrap();
            assert_eq!(values.true_count(), count);
            assert!(values.value(0));
            assert!(values.value(values.len() - 1));
            next += count;
            position = last + 1;
        }
        assert!(cursor.is_empty());
        assert!(cursor.next_mask_chunk(batch_size).is_none());
    }

    #[test]
    fn test_mask_chunks_sparse_aligned_words() {
        // Distinct byte positions expose native-endian word interpretation
        // on big-endian platforms. Both selections must form one-row chunks.
        let mask = BooleanBuffer::from_iter((0..256).map(|row| row == 0 || row == 255));
        assert_mask_chunks(mask, None, 1);
    }

    #[test]
    fn test_mask_chunks_word_boundaries_and_offsets() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};

        for len in [0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 511, 2049] {
            for offset in [0, 1, 3, 7, 8, 31, 63, 64, 65] {
                for pattern in 0..7 {
                    let mut rng = StdRng::seed_from_u64(42);
                    let mask = BooleanBuffer::from_iter((0..offset + len + 64).map(|row| {
                        if !(offset..offset + len).contains(&row) {
                            // Set padding on both sides to catch reads outside
                            // the logical slice, including its final word.
                            return true;
                        }
                        let row = row - offset;
                        match pattern {
                            0 => false,
                            1 => true,
                            2 => row % 2 == 0,
                            3 => row % 129 == 128,
                            4 => rng.random_bool(0.1),
                            5 => rng.random_bool(0.5),
                            _ => rng.random_bool(0.9),
                        }
                    }))
                    .slice(offset, len);
                    for batch_size in [1, 2, 3, 63, 64, 65, 1024] {
                        assert_mask_chunks(mask.clone(), None, batch_size);
                    }
                }
            }
        }
    }

    #[test]
    fn test_mask_chunks_loaded_ranges_word_boundaries() {
        for page_rows in [1, 31, 63, 64, 65, 127, 257] {
            let len = page_rows * 7 + 13;
            let ranges: Vec<_> = (0..len)
                .step_by(page_rows)
                .enumerate()
                .filter(|(page, _)| page % 3 != 1)
                .map(|(_, start)| start..(start + page_rows).min(len))
                .collect();
            let loaded = LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
                ranges.iter().cloned(),
                len,
            ));
            for offset in [0, 1, 3, 7, 31, 63, 64, 65] {
                for pattern in 0..4 {
                    let mask = BooleanBuffer::from_iter((0..offset + len + 64).map(|row| {
                        if !(offset..offset + len).contains(&row) {
                            return true;
                        }
                        let row = row - offset;
                        ranges.iter().any(|range| range.contains(&row))
                            && match pattern {
                                0 => true,
                                1 => row % 2 == 0,
                                // Leave an unset tail in every loaded range.
                                2 => row % page_rows == 0,
                                // Select directly on either side of page and
                                // word boundaries, including the final page.
                                _ => {
                                    row % page_rows == page_rows - 1
                                        || row % 64 == 0
                                        || row % 64 == 63
                                }
                            }
                    }))
                    .slice(offset, len);
                    for batch_size in [1, 2, 3, 63, 64, 65, 1024] {
                        assert_mask_chunks(mask.clone(), Some(loaded.clone()), batch_size);
                    }
                }
            }
        }
    }

    #[test]
    fn test_mask_chunk_does_not_read_selected_rows_outside_loaded_range() {
        let mut cursor = MaskCursor {
            mask: BooleanBuffer::new_set(130),
            position: 0,
            loaded_row_ranges: Some(Arc::new(LoadedRowRanges(vec![0..65, 66..130]))),
        };
        let chunk = cursor.next_chunk(1024).unwrap();
        assert_eq!(chunk.chunk_rows, 65);
        assert_eq!(chunk.selected_rows, 65);
        assert_eq!(cursor.position, 65);
        let err = cursor.next_chunk(1024).unwrap_err();
        assert!(
            err.to_string()
                .contains("selected row 65 has no loaded page range")
        );
        assert_eq!(cursor.position, 65);
    }

    #[test]
    fn selection_respects_policy() {
        let mask = BooleanBuffer::from(vec![true, true, false, false, true, true, false, false]);
        for (policy, expect_mask) in [
            (RowSelectionPolicy::Mask, true),
            (RowSelectionPolicy::Selectors, false),
            (RowSelectionPolicy::Auto { threshold: 2 }, false),
            (RowSelectionPolicy::Auto { threshold: 3 }, true),
        ] {
            for selection in [
                RowSelection::from_boolean_buffer(mask.clone()),
                RowSelection::from(vec![
                    RowSelector::select(2),
                    RowSelector::skip(2),
                    RowSelector::select(2),
                    RowSelector::skip(2),
                ]),
            ] {
                let selection = policy.apply(selection);
                assert_eq!(selection.as_mask().is_some(), expect_mask, "{policy:?}");
                assert_eq!(selection.into_boolean_buffer(), mask);
            }
        }
    }

    #[test]
    fn adaptive_selection_uses_selectors_for_long_runs() {
        let mask = BooleanBuffer::from_iter((0..1_024).map(|idx| (256..768).contains(&idx)));
        let selection =
            RowSelectionPolicy::default().apply(RowSelection::from_boolean_buffer(mask));
        assert!(selection.as_mask().is_none());
    }

    #[test]
    fn adaptive_selection_keeps_fragmented_masks() {
        let mask = BooleanBuffer::from_iter((0..1_024).map(|idx| idx % 2 == 0));
        let selection =
            RowSelectionPolicy::default().apply(RowSelection::from_boolean_buffer(mask));
        assert!(selection.as_mask().is_some());
    }

    #[test]
    fn test_loaded_mask_chunk_stops_at_trimmed_mask_end() {
        let loaded = LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
            std::iter::once(0..5),
            10,
        ));
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionStrategy::Mask.build_cursor(
            RowSelection::from(vec![RowSelector::select(1)]),
            Some(loaded.into()),
        ) else {
            unreachable!()
        };

        let chunk = cursor.next_chunk(10).unwrap();
        assert_eq!(chunk.chunk_rows, 1);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_next_mask_chunk_until_cursor_is_empty() {
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionStrategy::Mask.build_cursor(
            RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(1),
            ]),
            None,
        ) else {
            unreachable!()
        };

        let first = cursor.next_mask_chunk(2).unwrap();
        assert_eq!(first.initial_skip, 2);
        assert_eq!(first.chunk_rows, 2);
        assert_eq!(first.selected_rows, 2);

        let second = cursor.next_mask_chunk(2).unwrap();
        assert_eq!(second.initial_skip, 1);
        assert_eq!(second.chunk_rows, 1);
        assert_eq!(second.selected_rows, 1);

        assert!(cursor.next_mask_chunk(2).is_none());
    }
}
