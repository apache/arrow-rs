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

use super::boolean::boolean_mask_from_selectors;
use super::{RowSelection, RowSelector};
use crate::errors::ParquetError;
use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
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
    /// Create a [`MaskCursor`] cursor backed by a bitmask, from an existing set of selectors
    pub(crate) fn new_mask_from_selectors(
        selectors: Vec<RowSelector>,
        loaded_row_ranges: Option<Arc<LoadedRowRanges>>,
    ) -> Self {
        debug_assert!(
            selectors
                .last()
                .map(|selector| !selector.skip)
                .unwrap_or(true),
            "Mask selectors must not end with a skip"
        );
        Self::Mask(MaskCursor {
            mask: boolean_mask_from_selectors(&selectors),
            position: 0,
            loaded_row_ranges,
        })
    }

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
/// The first chunk decodes `[0, 4)` with mask `1000`. The next chunk skips to
/// row 11 and decodes `[11, 12)` with mask `1`. The loaded ranges are decode
/// boundaries, not output batch boundaries: [`ParquetRecordBatchReader`]
/// accumulates both chunks and applies the combined mask `10001` once.
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

        let (initial_skip, chunk_rows, selected_rows, mask_start, end_position) = {
            let mask = &self.mask;
            let start_position = self.position;
            let mut cursor = start_position;
            let mut initial_skip = 0;

            while cursor < mask.len() && !mask.value(cursor) {
                initial_skip += 1;
                cursor += 1;
            }
            debug_assert!(
                cursor < mask.len(),
                "ReadPlan must remove trailing skips from Mask selections"
            );

            let mask_start = cursor;
            let mut chunk_rows = 0;
            let mut selected_rows = 0;

            // Advance until enough rows have been selected to satisfy the batch size,
            // or until the mask is exhausted. This mirrors the behaviour of the legacy
            // `RowSelector` queue-based iteration.
            while cursor < mask.len() && selected_rows < batch_size {
                chunk_rows += 1;
                if mask.value(cursor) {
                    selected_rows += 1;
                }
                cursor += 1;
            }

            (initial_skip, chunk_rows, selected_rows, mask_start, cursor)
        };

        self.position = end_position;

        MaskChunk {
            initial_skip,
            chunk_rows,
            selected_rows,
            mask_start,
        }
    }

    /// Returns the next non-empty mask chunk without crossing an unloaded row range.
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

        let start_position = self.position;
        let mut cursor = start_position;
        while cursor < self.mask.len() && !self.mask.value(cursor) {
            cursor += 1;
        }

        debug_assert!(
            cursor < self.mask.len(),
            "ReadPlan must remove trailing skips from Mask selections"
        );

        let loaded_range_end = self
            .loaded_row_ranges
            .as_ref()
            .and_then(|ranges| ranges.end_containing(cursor))
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Internal Error: selected row {cursor} has no loaded page range"
                ))
            })?;

        let mask_start = cursor;
        let mut selected_rows = 0;
        while cursor < loaded_range_end && cursor < self.mask.len() && selected_rows < batch_size {
            if self.mask.value(cursor) {
                selected_rows += 1;
            }
            cursor += 1;
        }

        self.position = cursor;
        Ok(MaskChunk {
            initial_skip: mask_start - start_position,
            chunk_rows: cursor - mask_start,
            selected_rows,
            mask_start,
        })
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

    #[test]
    fn test_loaded_mask_chunk_stops_at_trimmed_mask_end() {
        let loaded = LoadedRowRanges::from_selection(RowSelection::from_consecutive_ranges(
            std::iter::once(0..5),
            10,
        ));
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionCursor::new_mask_from_selectors(
            vec![RowSelector::select(1)],
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
        let RowSelectionCursor::Mask(mut cursor) = RowSelectionCursor::new_mask_from_selectors(
            vec![
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(1),
            ],
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
