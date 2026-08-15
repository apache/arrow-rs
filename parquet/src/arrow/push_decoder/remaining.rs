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
use crate::arrow::arrow_reader::{
    ParquetRecordBatchReader, RowGroupPlan, RowGroupSelection, RowSelection,
};
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

/// Plan for the next queued row group after row-selection slicing.
#[derive(Debug)]
enum QueuedRowGroupDecision {
    /// Hand this row group to the builder.
    Read(NextRowGroup),
    /// Skip this row group, and keep scanning with the updated budget.
    Skip { remaining_budget: RowBudget },
}

/// Work item handed from [`RowGroupFrontier`] to [`RowGroupReaderBuilder`].
#[derive(Debug)]
struct NextRowGroup {
    row_group_idx: usize,
    row_count: usize,
    /// This row group's selection, or `None` when all rows are selected.
    selection: Option<RowSelection>,
    /// Budget snapshot to apply while decoding this row group.
    budget: RowBudget,
}

#[derive(Debug, Clone)]
enum QueuedRowGroups {
    Global {
        row_groups: VecDeque<usize>,
        selection: Option<RowSelection>,
    },
    PerRowGroup(VecDeque<RowGroupSelection>),
}

impl QueuedRowGroups {
    fn front(&self) -> Option<usize> {
        match self {
            Self::Global { row_groups, .. } => row_groups.front().copied(),
            Self::PerRowGroup(row_groups) => row_groups
                .front()
                .map(|row_group| row_group.row_group_index),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Global { row_groups, .. } => row_groups.len(),
            Self::PerRowGroup(row_groups) => row_groups.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Global {
                row_groups,
                selection,
            } => {
                row_groups.clear();
                *selection = None;
            }
            Self::PerRowGroup(row_groups) => row_groups.clear(),
        }
    }
}

/// Number of rows in `row_group_idx`, or an error if the index is out of
/// bounds for the file.
fn row_group_num_rows(
    parquet_metadata: &ParquetMetaData,
    row_group_idx: usize,
) -> Result<usize, ParquetError> {
    parquet_metadata
        .row_groups()
        .get(row_group_idx)
        .ok_or_else(|| {
            ParquetError::General(format!(
                "Row group index {row_group_idx} out of bounds for file with {} row groups",
                parquet_metadata.num_row_groups()
            ))
        })?
        .num_rows()
        .try_into()
        .map_err(|e| ParquetError::General(format!("Row count overflow: {e}")))
}

#[derive(Debug, Clone)]
struct RowGroupFrontier {
    /// Metadata used to resolve row counts for queued row groups.
    parquet_metadata: Arc<ParquetMetaData>,
    /// Row groups not yet handed to the builder.
    queued: QueuedRowGroups,
    /// Offset/limit budget before the next readable row group is planned.
    budget: RowBudget,
    /// If predicates are present, row groups with selected rows must be read so
    /// the predicate can decide whether they are actually needed.
    has_predicates: bool,
}

impl RowGroupFrontier {
    fn new(
        parquet_metadata: Arc<ParquetMetaData>,
        row_group_plan: RowGroupPlan,
        budget: RowBudget,
        has_predicates: bool,
    ) -> Result<Self, ParquetError> {
        let queued = match row_group_plan {
            RowGroupPlan::Global {
                row_groups,
                selection,
            } => QueuedRowGroups::Global {
                row_groups: row_groups
                    .unwrap_or_else(|| (0..parquet_metadata.num_row_groups()).collect())
                    .into(),
                selection,
            },
            RowGroupPlan::PerRowGroup(row_groups) => {
                for row_group in &row_groups {
                    let row_count =
                        row_group_num_rows(&parquet_metadata, row_group.row_group_index)?;
                    if let Some(selection) = &row_group.selection {
                        let selection_rows = selection.row_count() + selection.skipped_row_count();
                        if selection_rows > row_count {
                            return Err(ParquetError::General(format!(
                                "Row selection for row group {} contains {selection_rows} rows, but the row group has {row_count}",
                                row_group.row_group_index
                            )));
                        }
                    }
                }
                QueuedRowGroups::PerRowGroup(row_groups.into())
            }
            RowGroupPlan::Conflicting => return Err(RowGroupPlan::conflict_error()),
        };

        Ok(Self {
            parquet_metadata,
            queued,
            budget,
            has_predicates,
        })
    }

    fn row_group_num_rows(&self, row_group_idx: usize) -> Result<usize, ParquetError> {
        row_group_num_rows(&self.parquet_metadata, row_group_idx)
    }

    fn update_budget_after_row_group(&mut self, budget: RowBudget) {
        self.budget = budget;
    }

    /// Peek at the next row-group index [`Self::next_readable_row_group`]
    /// would hand out, without mutating any state. Returns `None` if every
    /// remaining row group would be skipped under the current
    /// selection/budget, or if the queue is empty.
    ///
    /// Runs the real [`Self::next_readable_row_group`] advance logic on a
    /// throwaway clone of the frontier, so peek can never drift from the
    /// read path. The clone copies the queued row-group plan and selections;
    /// see
    /// [`RemainingRowGroups::peek_next_row_group`].
    fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        Ok(self
            .clone()
            .next_readable_row_group()?
            .map(|next_row_group| next_row_group.row_group_idx))
    }

    fn clear_remaining(&mut self) {
        self.queued.clear();
    }

    /// Plan whether a selected row group should be read or skipped.
    ///
    /// Selection-only skips are handled before this method is called. This
    /// method applies the remaining offset/limit budget and predicate
    /// conservatism.
    fn plan_selected_row_group(
        &self,
        next_row_group: NextRowGroup,
        selected_rows: usize,
    ) -> QueuedRowGroupDecision {
        if self.has_predicates {
            return QueuedRowGroupDecision::Read(next_row_group);
        }

        let rows_after_budget = self.budget.rows_after(selected_rows);
        if rows_after_budget != 0 {
            return QueuedRowGroupDecision::Read(next_row_group);
        }

        QueuedRowGroupDecision::Skip {
            remaining_budget: self.budget.advance(selected_rows, rows_after_budget),
        }
    }

    /// Advance queued row groups until one should be handed to the builder.
    fn next_readable_row_group(&mut self) -> Result<Option<NextRowGroup>, ParquetError> {
        loop {
            let Some(row_group_idx) = self.queued.front() else {
                return Ok(None);
            };
            if self.budget.is_exhausted()
                || matches!(
                    &self.queued,
                    QueuedRowGroups::Global {
                        selection: Some(selection),
                        ..
                    } if selection.row_count() == 0
                )
            {
                self.clear_remaining();
                return Ok(None);
            }

            let row_count = self.row_group_num_rows(row_group_idx)?;
            // Pop the front row group, resolving its selection to
            // row-group-local coordinates (splitting off the global cursor's
            // prefix, or taking the already-local selection as-is).
            let selection = match &mut self.queued {
                QueuedRowGroups::Global {
                    row_groups,
                    selection,
                } => {
                    row_groups.pop_front();
                    selection
                        .as_mut()
                        .map(|selection| selection.split_off(row_count))
                }
                QueuedRowGroups::PerRowGroup(row_groups) => {
                    row_groups
                        .pop_front()
                        .expect("front checked above")
                        .selection
                }
            };
            let (selection, selected_rows) = match selection {
                Some(selection) => {
                    let selected_rows = selection.row_count();
                    if selected_rows == 0 {
                        continue;
                    }
                    // An all-rows selection is equivalent to no selection
                    (
                        (selected_rows != row_count).then_some(selection),
                        selected_rows,
                    )
                }
                None => (None, row_count),
            };

            let next_row_group = NextRowGroup {
                row_group_idx,
                row_count,
                selection,
                budget: self.budget,
            };

            match self.plan_selected_row_group(next_row_group, selected_rows) {
                QueuedRowGroupDecision::Read(next_row_group) => {
                    return Ok(Some(next_row_group));
                }
                QueuedRowGroupDecision::Skip { remaining_budget } => {
                    self.budget = remaining_budget;
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
    /// Row groups and selections not yet handed to the reader builder.
    pub row_group_plan: RowGroupPlan,
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
        row_group_plan: RowGroupPlan,
        budget: RowBudget,
        has_predicates: bool,
        row_group_reader_builder: RowGroupReaderBuilder,
    ) -> Result<Self, ParquetError> {
        Ok(Self {
            schema,
            frontier: RowGroupFrontier::new(
                parquet_metadata,
                row_group_plan,
                budget,
                has_predicates,
            )?,
            row_group_reader_builder,
        })
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
            queued,
            budget,
            has_predicates: _,
        } = frontier;
        let row_group_plan = match queued {
            QueuedRowGroups::Global {
                row_groups,
                selection,
            } => RowGroupPlan::Global {
                row_groups: Some(Vec::from(row_groups)),
                selection,
            },
            QueuedRowGroups::PerRowGroup(row_groups) => {
                RowGroupPlan::PerRowGroup(Vec::from(row_groups))
            }
        };
        RemainingRowGroupsParts {
            schema,
            metadata: parquet_metadata,
            row_group_plan,
            offset: budget.offset(),
            limit: budget.limit(),
            reader_builder: row_group_reader_builder.into_parts(),
        }
    }

    /// Push new data buffers that can be used to satisfy pending requests
    pub fn push_data(
        &mut self,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        self.row_group_reader_builder.push_data(ranges, buffers)
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
        self.frontier.queued.len()
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
    ///
    /// Cost: one clone of the queued row-group plan and selections per call
    /// (the frontier is cloned so the real advance logic can run
    /// non-destructively). For callers that peek once per row-group boundary
    /// this is O(remaining row groups + selectors) per boundary.
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
