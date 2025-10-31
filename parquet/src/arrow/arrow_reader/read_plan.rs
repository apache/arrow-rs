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

//! [`ReadPlan`] and [`ReadPlanBuilder`] for determining which rows to read
//! from a Parquet file

use crate::arrow::array_reader::ArrayReader;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, RowSelection, RowSelector,
};
use crate::errors::{ParquetError, Result};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::prep_null_mask_filter;
use std::collections::VecDeque;

/// Strategy for materialising [`RowSelection`] during execution.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RowSelectionStrategy {
    /// Automatically choose between mask- and selector-backed execution
    /// based on heuristics
    #[default]
    Auto,
    /// Always use a boolean mask to materialise the selection
    Mask,
    /// Always use a queue of [`RowSelector`] values
    Selectors,
}

/// A builder for [`ReadPlan`]
#[derive(Clone, Debug)]
pub struct ReadPlanBuilder {
    batch_size: usize,
    /// Current to apply, includes all filters
    selection: Option<RowSelection>,
    /// Strategy to use when materialising the row selection
    selection_strategy: RowSelectionStrategy,
}

impl ReadPlanBuilder {
    /// Create a `ReadPlanBuilder` with the given batch size
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            selection: None,
            selection_strategy: RowSelectionStrategy::Auto,
        }
    }

    /// Set the current selection to the given value
    pub fn with_selection(mut self, selection: Option<RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    /// Force a specific strategy when materialising the [`RowSelection`]
    pub fn with_selection_strategy(mut self, strategy: RowSelectionStrategy) -> Self {
        self.selection_strategy = strategy;
        self
    }

    /// Returns the current selection, if any
    pub fn selection(&self) -> Option<&RowSelection> {
        self.selection.as_ref()
    }

    /// Specifies the number of rows in the row group, before filtering is applied.
    ///
    /// Returns a [`LimitedReadPlanBuilder`] that can apply
    /// offset and limit.
    ///
    /// Call [`LimitedReadPlanBuilder::build_limited`] to apply the limits to this
    /// selection.
    pub(crate) fn limited(self, row_count: usize) -> LimitedReadPlanBuilder {
        LimitedReadPlanBuilder::new(self, row_count)
    }

    /// Returns true if the current plan selects any rows
    pub fn selects_any(&self) -> bool {
        self.selection
            .as_ref()
            .map(|s| s.selects_any())
            .unwrap_or(true)
    }

    /// Returns the number of rows selected, or `None` if all rows are selected.
    pub fn num_rows_selected(&self) -> Option<usize> {
        self.selection.as_ref().map(|s| s.row_count())
    }

    /// Evaluates an [`ArrowPredicate`], updating this plan's `selection`
    ///
    /// If the current `selection` is `Some`, the resulting [`RowSelection`]
    /// will be the conjunction of the existing selection and the rows selected
    /// by `predicate`.
    ///
    /// Note: pre-existing selections may come from evaluating a previous predicate
    /// or if the [`ParquetRecordBatchReader`] specified an explicit
    /// [`RowSelection`] in addition to one or more predicates.
    pub fn with_predicate(
        mut self,
        array_reader: Box<dyn ArrayReader>,
        predicate: &mut dyn ArrowPredicate,
    ) -> Result<Self> {
        let reader = ParquetRecordBatchReader::new(array_reader, self.clone().build());
        let mut filters = vec![];
        for maybe_batch in reader {
            let maybe_batch = maybe_batch?;
            let input_rows = maybe_batch.num_rows();
            let filter = predicate.evaluate(maybe_batch)?;
            // Since user supplied predicate, check error here to catch bugs quickly
            if filter.len() != input_rows {
                return Err(arrow_err!(
                    "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                    filter.len()
                ));
            }
            match filter.null_count() {
                0 => filters.push(filter),
                _ => filters.push(prep_null_mask_filter(&filter)),
            };
        }

        let raw = RowSelection::from_filters(&filters);
        self.selection = match self.selection.take() {
            Some(selection) => Some(selection.and_then(&raw)),
            None => Some(raw),
        };
        Ok(self)
    }

    /// Create a final `ReadPlan` the read plan for the scan
    pub fn build(mut self) -> ReadPlan {
        // If selection is empty, truncate
        if !self.selects_any() {
            self.selection = Some(RowSelection::from(vec![]));
        }
        let Self {
            batch_size,
            selection,
            selection_strategy,
        } = self;

        let selection = selection.map(|s| {
            let trimmed = s.trim();
            let selectors: Vec<RowSelector> = trimmed.into();
            RowSelectionCursor::new(selectors, selection_strategy)
        });

        ReadPlan {
            batch_size,
            selection,
        }
    }
}

/// Builder for [`ReadPlan`] that applies a limit and offset to the read plan
///
/// See [`ReadPlanBuilder::limited`] to create this builder.
pub(crate) struct LimitedReadPlanBuilder {
    /// The underlying builder
    inner: ReadPlanBuilder,
    /// Total number of rows in the row group before the selection, limit or
    /// offset are applied
    row_count: usize,
    /// The offset to apply, if any
    offset: Option<usize>,
    /// The limit to apply, if any
    limit: Option<usize>,
}

impl LimitedReadPlanBuilder {
    /// Create a new `LimitedReadPlanBuilder` from the existing builder and number of rows
    fn new(inner: ReadPlanBuilder, row_count: usize) -> Self {
        Self {
            inner,
            row_count,
            offset: None,
            limit: None,
        }
    }

    /// Set the offset to apply to the read plan
    pub(crate) fn with_offset(mut self, offset: Option<usize>) -> Self {
        self.offset = offset;
        self
    }

    /// Set the limit to apply to the read plan
    pub(crate) fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Apply offset and limit, updating the selection on the underlying builder
    /// and returning it.
    pub(crate) fn build_limited(self) -> ReadPlanBuilder {
        let Self {
            mut inner,
            row_count,
            offset,
            limit,
        } = self;

        // If the selection is empty, truncate
        if !inner.selects_any() {
            inner.selection = Some(RowSelection::from(vec![]));
        }

        // If an offset is defined, apply it to the `selection`
        if let Some(offset) = offset {
            inner.selection = Some(match row_count.checked_sub(offset) {
                None => RowSelection::from(vec![]),
                Some(remaining) => inner
                    .selection
                    .map(|selection| selection.offset(offset))
                    .unwrap_or_else(|| {
                        RowSelection::from(vec![
                            RowSelector::skip(offset),
                            RowSelector::select(remaining),
                        ])
                    }),
            });
        }

        // If a limit is defined, apply it to the final `selection`
        if let Some(limit) = limit {
            inner.selection = Some(
                inner
                    .selection
                    .map(|selection| selection.limit(limit))
                    .unwrap_or_else(|| {
                        RowSelection::from(vec![RowSelector::select(limit.min(row_count))])
                    }),
            );
        }

        inner
    }
}

/// A plan reading specific rows from a Parquet Row Group.
///
/// See [`ReadPlanBuilder`] to create `ReadPlan`s
#[derive(Debug)]
pub struct ReadPlan {
    /// The number of rows to read in each batch
    batch_size: usize,
    /// Row ranges to be selected from the data source
    selection: Option<RowSelectionCursor>,
}

impl ReadPlan {
    /// Returns a mutable reference to the selection, if any
    pub fn selection_mut(&mut self) -> Option<&mut RowSelectionCursor> {
        self.selection.as_mut()
    }

    /// Return the number of rows to read in each output batch
    #[inline(always)]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

/// Cursor for iterating a [`RowSelection`] during execution within a [`ReadPlan`].
///
/// This keeps per-reader state such as the current position and delegates the
/// actual storage strategy to [`RowSelectionBacking`].
#[derive(Debug)]
pub struct RowSelectionCursor {
    /// Backing storage describing how the selection is materialised
    storage: RowSelectionBacking,
    /// Current absolute offset into the selection
    position: usize,
}

/// Backing storage that powers [`RowSelectionCursor`].
///
/// The cursor either walks a boolean mask (dense representation) or a queue
/// of [`RowSelector`] ranges (sparse representation).
#[derive(Debug)]
enum RowSelectionBacking {
    Mask(BooleanBuffer),
    Selectors(VecDeque<RowSelector>),
}

/// Result of computing the next chunk to read when using a bitmap mask
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

impl RowSelectionCursor {
    /// Create a cursor, choosing an efficient backing representation
    fn new(selectors: Vec<RowSelector>, strategy: RowSelectionStrategy) -> Self {
        if matches!(strategy, RowSelectionStrategy::Selectors) {
            return Self {
                storage: RowSelectionBacking::Selectors(selectors.into()),
                position: 0,
            };
        }

        let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
        let selector_count = selectors.len();
        const AVG_SELECTOR_LEN_MASK_THRESHOLD: usize = 16;
        // Prefer a bitmap mask when the selectors are short on average, as the mask
        // (re)construction cost is amortized by a simpler execution path during reads.
        let use_mask = match strategy {
            RowSelectionStrategy::Mask => true,
            RowSelectionStrategy::Auto => {
                selector_count == 0
                    || total_rows < selector_count.saturating_mul(AVG_SELECTOR_LEN_MASK_THRESHOLD)
            }
            RowSelectionStrategy::Selectors => unreachable!(),
        };

        let storage = if use_mask {
            RowSelectionBacking::Mask(boolean_mask_from_selectors(&selectors))
        } else {
            RowSelectionBacking::Selectors(selectors.into())
        };

        Self {
            storage,
            position: 0,
        }
    }

    /// Returns `true` when no further rows remain
    pub fn is_empty(&self) -> bool {
        match &self.storage {
            RowSelectionBacking::Mask(mask) => self.position >= mask.len(),
            RowSelectionBacking::Selectors(selectors) => selectors.is_empty(),
        }
    }

    /// Current position within the overall selection
    pub fn position(&self) -> usize {
        self.position
    }

    /// Return the next [`RowSelector`] when using the sparse representation
    pub fn next_selector(&mut self) -> Option<RowSelector> {
        match &mut self.storage {
            RowSelectionBacking::Selectors(selectors) => {
                let selector = selectors.pop_front()?;
                self.position += selector.row_count;
                Some(selector)
            }
            RowSelectionBacking::Mask(_) => {
                unreachable!("next_selector called for mask-based RowSelectionCursor")
            }
        }
    }

    /// Return a selector to the front, rewinding the position (sparse-only)
    pub fn return_selector(&mut self, selector: RowSelector) {
        match &mut self.storage {
            RowSelectionBacking::Selectors(selectors) => {
                self.position = self.position.saturating_sub(selector.row_count);
                selectors.push_front(selector);
            }
            RowSelectionBacking::Mask(_) => {
                unreachable!("return_selector called for mask-based RowSelectionCursor")
            }
        }
    }

    /// Returns `true` if the cursor is backed by a boolean mask
    pub fn is_mask_backed(&self) -> bool {
        matches!(self.storage, RowSelectionBacking::Mask(_))
    }

    /// Advance through the mask representation, producing the next chunk summary
    pub fn next_mask_chunk(&mut self, batch_size: usize) -> Option<MaskChunk> {
        let (initial_skip, chunk_rows, selected_rows, mask_start, end_position) = {
            let mask = match &self.storage {
                RowSelectionBacking::Mask(mask) => mask,
                RowSelectionBacking::Selectors(_) => return None,
            };

            if self.position >= mask.len() {
                return None;
            }

            let start_position = self.position;
            let mut cursor = start_position;
            let mut initial_skip = 0;

            while cursor < mask.len() && !mask.value(cursor) {
                initial_skip += 1;
                cursor += 1;
            }

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

        Some(MaskChunk {
            initial_skip,
            chunk_rows,
            selected_rows,
            mask_start,
        })
    }

    /// Materialise the boolean values for a mask-backed chunk
    pub fn mask_values_for(&self, chunk: &MaskChunk) -> Option<BooleanArray> {
        match &self.storage {
            RowSelectionBacking::Mask(mask) => {
                if chunk.mask_start.saturating_add(chunk.chunk_rows) > mask.len() {
                    return None;
                }
                Some(BooleanArray::from(
                    mask.slice(chunk.mask_start, chunk.chunk_rows),
                ))
            }
            RowSelectionBacking::Selectors(_) => None,
        }
    }
}

fn boolean_mask_from_selectors(selectors: &[RowSelector]) -> BooleanBuffer {
    let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
    let mut builder = BooleanBufferBuilder::new(total_rows);
    for selector in selectors {
        builder.append_n(selector.row_count, !selector.skip);
    }
    builder.finish()
}
