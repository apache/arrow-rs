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

use crate::DecodeResult;
use crate::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection, RowSelector};
use crate::arrow::push_decoder::reader_builder::{
    RowBudget, RowGroupBuildResult, RowGroupReaderBuilder, RowGroupReaderBuilderParts,
};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// Whether the frontier walker should hand out a row group or carry on
/// scanning. Shared by [`RowGroupFrontier::next_readable_row_group`] and
/// [`RowGroupFrontier::peek_next_row_group`] so a single decision rule
/// drives both paths; this prevents the two walkers from drifting out of
/// sync.
#[derive(Debug)]
enum RowGroupAction {
    /// Hand this row group to the builder (read it).
    Read,
    /// Skip this row group entirely. Carry `budget_after` forward to the
    /// next iteration.
    Skip { budget_after: RowBudget },
}

/// Per-row-group decision shared by the read and peek paths.
///
/// Given the `selected_rows` already established from the row selection,
/// decide whether the row group must be read (predicates present, or the
/// budget admits at least one row) or skipped entirely (budget exhausted
/// for this row group). The single source of truth for this rule.
fn classify_row_group(
    has_predicates: bool,
    budget: RowBudget,
    selected_rows: usize,
) -> RowGroupAction {
    if has_predicates {
        // Predicates disable budget-based RG skipping for this RG; budget
        // still gates row emission inside the row group.
        return RowGroupAction::Read;
    }
    let rows_after_budget = budget.rows_after(selected_rows);
    if rows_after_budget != 0 {
        return RowGroupAction::Read;
    }
    RowGroupAction::Skip {
        budget_after: budget.advance(selected_rows, rows_after_budget),
    }
}

/// Borrowed cursor over a [`RowSelection`] that counts selected rows in
/// each row group's slice without mutating the selection.
///
/// Used by [`RowGroupFrontier::peek_next_row_group`] to walk per-row-group
/// selection slices without cloning the underlying selectors.
struct PeekSelectionCursor<'a> {
    iter: Box<dyn Iterator<Item = &'a RowSelector> + 'a>,
    /// The selector currently being consumed (only partly used).
    current: Option<&'a RowSelector>,
    /// Rows already consumed from `current`.
    consumed_in_current: usize,
}

impl<'a> PeekSelectionCursor<'a> {
    fn new(selection: &'a RowSelection) -> Self {
        Self {
            iter: Box::new(selection.iter()),
            current: None,
            consumed_in_current: 0,
        }
    }

    /// Consume the next `row_count` rows from the cursor and return the
    /// number of those rows that are selected (i.e. `!selector.skip`).
    /// Advances the cursor past the consumed range.
    fn take(&mut self, row_count: usize) -> usize {
        let mut selected = 0usize;
        let mut remaining = row_count;
        while remaining > 0 {
            if self.current.is_none() {
                self.current = self.iter.next();
                self.consumed_in_current = 0;
                if self.current.is_none() {
                    break;
                }
            }
            let selector = self.current.expect("current selector present");
            let available = selector.row_count.saturating_sub(self.consumed_in_current);
            let consume = available.min(remaining);
            if !selector.skip {
                selected += consume;
            }
            remaining -= consume;
            self.consumed_in_current += consume;
            if self.consumed_in_current >= selector.row_count {
                self.current = None;
            }
        }
        selected
    }
}

/// Work item handed from [`RowGroupFrontier`] to [`RowGroupReaderBuilder`].
#[derive(Debug)]
struct NextRowGroup {
    row_group_idx: usize,
    row_count: usize,
    /// This row group's slice of the global selection, or `None` when all rows
    /// are selected.
    selection: Option<RowSelection>,
    /// Budget snapshot to apply while decoding this row group.
    budget: RowBudget,
}

#[derive(Debug)]
struct RowGroupFrontier {
    /// Metadata used to resolve row counts for queued row groups.
    parquet_metadata: Arc<ParquetMetaData>,
    /// Row group indices not yet handed to the builder.
    row_groups: VecDeque<usize>,
    /// Cross-row-group cursor for the optional global row selection.
    selection: Option<RowSelection>,
    /// Offset/limit budget before the next readable row group is planned.
    budget: RowBudget,
    /// If predicates are present, row groups with selected rows must be read so
    /// the predicate can decide whether they are actually needed.
    has_predicates: bool,
}

impl RowGroupFrontier {
    fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
        budget: RowBudget,
        has_predicates: bool,
    ) -> Self {
        Self {
            parquet_metadata,
            row_groups: VecDeque::from(row_groups),
            selection,
            budget,
            has_predicates,
        }
    }

    fn row_group_num_rows(&self, row_group_idx: usize) -> Result<usize, ParquetError> {
        self.parquet_metadata
            .row_group(row_group_idx)
            .num_rows()
            .try_into()
            .map_err(|e| ParquetError::General(format!("Row count overflow: {e}")))
    }

    fn update_budget_after_row_group(&mut self, budget: RowBudget) {
        self.budget = budget;
    }

    /// True iff the frontier has nothing more to hand out (budget
    /// exhausted or selection drained). Centralized so peek and read
    /// agree on the early-exit condition.
    fn is_frontier_drained(&self) -> bool {
        self.budget.is_exhausted()
            || self
                .selection
                .as_ref()
                .is_some_and(|selection| selection.row_count() == 0)
    }

    /// Peek at the next row-group index `next_readable_row_group` would
    /// hand out, without mutating any state. Returns `None` if every
    /// remaining row group would be skipped under the current
    /// selection/budget, or if the queue is empty.
    ///
    /// Walks borrowed state via [`PeekSelectionCursor`] (no
    /// `RowSelection` clone) and routes its per-RG decision through
    /// [`classify_row_group`], the same helper used by
    /// [`Self::next_readable_row_group`]. The two paths therefore
    /// cannot diverge on a Read/Skip rule for the same input.
    fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        if self.is_frontier_drained() {
            return Ok(None);
        }

        let mut cursor = self.selection.as_ref().map(PeekSelectionCursor::new);
        let mut budget = self.budget;
        for &row_group_idx in &self.row_groups {
            let row_count = self.row_group_num_rows(row_group_idx)?;
            let selected_rows = match cursor.as_mut() {
                Some(cursor) => cursor.take(row_count),
                None => row_count,
            };
            if selected_rows == 0 {
                // Same selection-skip path as `next_readable_row_group`.
                continue;
            }
            match classify_row_group(self.has_predicates, budget, selected_rows) {
                RowGroupAction::Read => return Ok(Some(row_group_idx)),
                RowGroupAction::Skip { budget_after } => {
                    budget = budget_after;
                }
            }
        }
        Ok(None)
    }

    fn clear_remaining(&mut self) {
        self.selection = None;
        self.row_groups.clear();
    }

    /// Advance queued row groups until one should be handed to the builder.
    ///
    /// Per-row-group Read/Skip decisions go through [`classify_row_group`]
    /// (also used by [`Self::peek_next_row_group`]) so the two walkers
    /// stay in lock-step.
    fn next_readable_row_group(&mut self) -> Result<Option<NextRowGroup>, ParquetError> {
        loop {
            let Some(&row_group_idx) = self.row_groups.front() else {
                return Ok(None);
            };
            if self.is_frontier_drained() {
                self.clear_remaining();
                return Ok(None);
            }

            let row_count = self.row_group_num_rows(row_group_idx)?;
            let (selection, selected_rows) = match self.selection.as_mut() {
                Some(selection) => {
                    let selection = selection.split_off(row_count);
                    let selected_rows = selection.row_count();
                    if selected_rows == 0 {
                        self.row_groups.pop_front();
                        continue;
                    }

                    let selection = if selected_rows == row_count {
                        None
                    } else {
                        Some(selection)
                    };
                    (selection, selected_rows)
                }
                None => (None, row_count),
            };

            match classify_row_group(self.has_predicates, self.budget, selected_rows) {
                RowGroupAction::Read => {
                    let next_row_group = NextRowGroup {
                        row_group_idx,
                        row_count,
                        selection,
                        budget: self.budget,
                    };
                    self.row_groups.pop_front();
                    return Ok(Some(next_row_group));
                }
                RowGroupAction::Skip { budget_after } => {
                    self.row_groups.pop_front();
                    self.budget = budget_after;
                }
            }
        }
    }
}

/// State machine that tracks the remaining high level chunks (row groups) of
/// Parquet data left to read.
///
/// [`RowGroupFrontier`] owns cross-row-group scan state and selects the next
/// work item. [`RowGroupReaderBuilder`] owns decoding for the active row group.
#[derive(Debug)]
pub(crate) struct RemainingRowGroups {
    /// The arrow schema of the decoded output. Carried only so
    /// [`Self::into_parts`] can hand it to a rebuilt builder; unused while
    /// decoding.
    schema: SchemaRef,

    /// Cross-row-group scan state for queued work.
    frontier: RowGroupFrontier,

    /// State for building the reader for the current row group
    row_group_reader_builder: RowGroupReaderBuilder,
}

/// The state recovered from a [`RemainingRowGroups`] by
/// [`RemainingRowGroups::into_parts`], describing the row groups *not* yet
/// decoded so a builder reconstructed from it resumes where the decoder left off.
#[derive(Debug)]
pub(crate) struct RemainingRowGroupsParts {
    /// The arrow schema of the decoded output.
    pub schema: SchemaRef,
    /// The Parquet file metadata.
    pub metadata: Arc<ParquetMetaData>,
    /// Row groups not yet handed to the reader builder.
    pub row_groups: Vec<usize>,
    /// The not-yet-consumed slice of the global row selection.
    pub selection: Option<RowSelection>,
    /// Offset still to be skipped before the next readable row group.
    pub offset: Option<usize>,
    /// Output rows still permitted across the remaining row groups.
    pub limit: Option<usize>,
    /// Builder-configurable parts of the inner row-group reader builder.
    pub reader_builder: RowGroupReaderBuilderParts,
}

impl RemainingRowGroups {
    pub fn new(
        schema: SchemaRef,
        parquet_metadata: Arc<ParquetMetaData>,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
        budget: RowBudget,
        has_predicates: bool,
        row_group_reader_builder: RowGroupReaderBuilder,
    ) -> Self {
        Self {
            schema,
            frontier: RowGroupFrontier::new(
                parquet_metadata,
                row_groups,
                selection,
                budget,
                has_predicates,
            ),
            row_group_reader_builder,
        }
    }

    /// Decompose into [`RemainingRowGroupsParts`].
    ///
    /// Must be called at a row-group boundary (see
    /// [`Self::is_at_row_group_boundary`]). The inner reader builder's runtime
    /// decode state is discarded; its buffered bytes are carried through.
    pub(crate) fn into_parts(self) -> RemainingRowGroupsParts {
        let Self {
            schema,
            frontier,
            row_group_reader_builder,
        } = self;
        // `has_predicates` is recomputed by `build()` from the filter.
        let RowGroupFrontier {
            parquet_metadata,
            row_groups,
            selection,
            budget,
            has_predicates: _,
        } = frontier;
        RemainingRowGroupsParts {
            schema,
            metadata: parquet_metadata,
            row_groups: Vec::from(row_groups),
            selection,
            offset: budget.offset(),
            limit: budget.limit(),
            reader_builder: row_group_reader_builder.into_parts(),
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        self.row_group_reader_builder.push_data(ranges, buffers);
    }

    /// Return the total number of bytes buffered so far
    pub fn buffered_bytes(&self) -> u64 {
        self.row_group_reader_builder.buffered_bytes()
    }

    /// Clear any staged ranges currently buffered for future decode work
    pub fn clear_all_ranges(&mut self) {
        self.row_group_reader_builder.clear_all_ranges();
    }

    /// True iff the inner row-group reader is between row groups (state
    /// `Finished`). Forward to [`RowGroupReaderBuilder::is_finished`].
    pub fn is_at_row_group_boundary(&self) -> bool {
        self.row_group_reader_builder.is_finished()
    }

    /// Number of row groups remaining (not including the one currently
    /// being decoded).
    pub fn row_groups_remaining(&self) -> usize {
        self.frontier.row_groups.len()
    }

    /// Peek at the file-level row-group index that the next call to
    /// [`Self::try_next_reader`] will produce a reader for, after
    /// simulating the same skip logic [`Self::try_next_reader`] applies
    /// internally (row-selection emptiness + offset/limit budget). Does
    /// not mutate state.
    ///
    /// Returns `None` when the active row group is still being decoded,
    /// when no row groups remain, or when every remaining row group
    /// would be skipped under the current selection/budget.
    pub fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        if self.row_group_reader_builder.has_active_row_group() {
            return Ok(None);
        }
        self.frontier.peek_next_row_group()
    }

    /// returns [`ParquetRecordBatchReader`] suitable for reading the next
    /// group of rows from the Parquet data, or the list of data ranges still
    /// needed to proceed
    pub fn try_next_reader(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        loop {
            if !self.row_group_reader_builder.has_active_row_group() {
                // We are done with the previous row group, seek to the next one
                // from the frontier, if any.

                match self.frontier.next_readable_row_group()? {
                    Some(NextRowGroup {
                        row_group_idx,
                        row_count,
                        selection,
                        budget,
                    }) => {
                        self.row_group_reader_builder.next_row_group(
                            row_group_idx,
                            row_count,
                            selection,
                            budget,
                        )?;
                    }
                    None => return Ok(DecodeResult::Finished),
                }
            }

            match self.row_group_reader_builder.try_build()? {
                RowGroupBuildResult::Finished { remaining_budget } => {
                    self.frontier
                        .update_budget_after_row_group(remaining_budget);
                    // reader is done, proceed to the next row group
                }
                RowGroupBuildResult::NeedsData(ranges) => {
                    // need more data to proceed
                    return Ok(DecodeResult::NeedsData(ranges));
                }
                RowGroupBuildResult::Data {
                    batch_reader,
                    remaining_budget,
                } => {
                    self.frontier
                        .update_budget_after_row_group(remaining_budget);
                    // ready to read the row group
                    return Ok(DecodeResult::Data(batch_reader));
                }
            }
        }
    }
}
